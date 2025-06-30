// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::fmt::Display;
use std::sync::Arc;

use api::v1::value::ValueData;
use api::v1::{
    greptime_request, ColumnDataType, ColumnSchema, Row, RowInsertRequest, RowInsertRequests, Rows,
    SemanticType,
};
use chrono::Utc;
use client::{Client, Database};
use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_PRIVATE_SCHEMA_NAME};
use common_meta::cluster::{NodeInfo, NodeInfoKey, Role as ClusterRole};
use common_meta::kv_backend::ResettableKvBackendRef;
use common_meta::rpc::store::RangeRequest;
use common_procedure::error::Error;
use common_procedure::ProcedureId;
use common_telemetry::{debug, error, info};
use snafu::ResultExt;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::task::JoinHandle;

use crate::error::{KvBackendSnafu, Result};
use crate::procedure::region_migration::manager::RegionMigrationProcedureTask;

const DEFAULT_REGION_MIGRATION_EVENTS_CHANNEL_SIZE: usize = 1024;
const DEFAULT_REGION_MIGRATION_EVENTS_TABLE_NAME: &str = "region_migration_events";

const REGION_MIGRATION_EVENTS_TABLE_PROCEDURE_ID_COLUMN_NAME: &str = "procedure_id";
const REGION_MIGRATION_EVENTS_TABLE_REGION_ID_COLUMN_NAME: &str = "region_id";
const REGION_MIGRATION_EVENTS_TABLE_FROM_DATANODE_ID_COLUMN_NAME: &str = "from_datanode_id";
const REGION_MIGRATION_EVENTS_TABLE_FROM_DATANODE_ADDR_COLUMN_NAME: &str = "from_datanode_addr";
const REGION_MIGRATION_EVENTS_TABLE_TO_DATANODE_ID_COLUMN_NAME: &str = "to_datanode_id";
const REGION_MIGRATION_EVENTS_TABLE_TO_DATANODE_ADDR_COLUMN_NAME: &str = "to_datanode_addr";
const REGION_MIGRATION_EVENTS_TABLE_TRIGGER_REASON_COLUMN_NAME: &str = "trigger_reason";
const REGION_MIGRATION_EVENTS_TABLE_TIMEOUT_COLUMN_NAME: &str = "timeout_ms";
const REGION_MIGRATION_EVENTS_TABLE_STATUS_COLUMN_NAME: &str = "status";
const REGION_MIGRATION_EVENTS_TABLE_TIMESTAMP_COLUMN_NAME: &str = "timestamp";

/// RegionMigrationEventRecorderRef is the reference of RegionMigrationEventRecorder.
pub type RegionMigrationEventRecorderRef = Arc<RegionMigrationEventRecorder>;

/// RegionMigrationEventRecorder is responsible for recording region migration events.
#[derive(Clone)]
pub struct RegionMigrationEventRecorder {
    tx: Sender<RegionMigrationEvent>,
    _handle: Arc<JoinHandle<()>>,
}

/// RegionMigrationEvent is the event of region migration.
#[derive(Debug)]
pub struct RegionMigrationEvent {
    task: RegionMigrationProcedureTask,
    procedure_id: ProcedureId,
    status: RegionMigrationStatus,
    timestamp: i64,
}

/// RegionMigrationStatus is the status of the whole region migration procedure.
#[derive(Debug)]
pub enum RegionMigrationStatus {
    // The manager creates the region migration task and prepares to submit to the procedure manager.
    Starting,
    // The region migration procedure is running.
    Running,
    // The region migration procedure is finished successfully.
    Finished,
    // The region migration procedure is failed.
    Failed(Error),
}

impl Display for RegionMigrationStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Starting => write!(f, "Starting"),
            Self::Running => write!(f, "Running"),
            Self::Finished => write!(f, "Finished"),
            Self::Failed(e) => write!(f, "Failed: {}", e),
        }
    }
}

impl RegionMigrationEventRecorder {
    pub fn new(in_memory_key: ResettableKvBackendRef) -> Self {
        let (tx, rx) = channel(DEFAULT_REGION_MIGRATION_EVENTS_CHANNEL_SIZE);

        let event_handler = RegionMigrationEventHandler { rx, in_memory_key };

        let handle = tokio::spawn(async move {
            event_handler.process_region_migration_event().await;
        });

        Self {
            tx,
            _handle: Arc::new(handle),
        }
    }

    pub fn record(
        &self,
        task: RegionMigrationProcedureTask,
        procedure_id: ProcedureId,
        status: RegionMigrationStatus,
    ) {
        let event = RegionMigrationEvent {
            task,
            procedure_id,
            status,
            timestamp: Utc::now().timestamp_millis(),
        };

        if let Err(e) = self.tx.try_send(event) {
            error!(e; "Failed to send region migration event");
        }
    }
}

struct RegionMigrationEventHandler {
    rx: Receiver<RegionMigrationEvent>,
    in_memory_key: ResettableKvBackendRef,
}

impl RegionMigrationEventHandler {
    async fn process_region_migration_event(mut self) {
        info!("Start the background handler to record region migration events.");
        while let Some(event) = self.rx.recv().await {
            debug!("Received region migration event: {:?}", event);

            let database_client = match self.build_database_client().await {
                Ok(client) => client,
                Err(e) => {
                    error!(e; "Failed to build database client");
                    continue;
                }
            };

            // TODO(zyy17): Batch the events to improve the performance.
            let row_inserts = self.build_row_inserts_request(event);

            debug!("Inserting region migration event: {:?}", row_inserts);

            if let Err(e) = database_client
                .handle(greptime_request::Request::RowInserts(row_inserts))
                .await
            {
                error!(e; "Failed to insert region migration event");
                continue;
            }
        }
    }

    async fn build_database_client(&self) -> Result<Database> {
        // Build a range request to get all available frontend addresses.
        let range_request = RangeRequest::new()
            .with_prefix(NodeInfoKey::key_prefix_with_role(ClusterRole::Frontend));
        let response = self
            .in_memory_key
            .range(range_request)
            .await
            .context(KvBackendSnafu)?;

        let mut urls = Vec::with_capacity(response.kvs.len());
        for kv in response.kvs {
            let node_info = NodeInfo::try_from(kv.value).context(KvBackendSnafu)?;
            urls.push(node_info.peer.addr);
        }

        debug!("Available frontend addresses: {:?}", urls);

        Ok(Database::new(
            DEFAULT_CATALOG_NAME,
            DEFAULT_PRIVATE_SCHEMA_NAME,
            Client::with_urls(urls),
        ))
    }

    fn build_row_inserts_request(&self, event: RegionMigrationEvent) -> RowInsertRequests {
        let insert = RowInsertRequest {
            table_name: DEFAULT_REGION_MIGRATION_EVENTS_TABLE_NAME.to_string(),
            rows: Some(Rows {
                schema: self.build_schema(),
                rows: vec![Row {
                    values: vec![
                        ValueData::StringValue(event.procedure_id.to_string()).into(),
                        ValueData::U64Value(event.task.region_id.as_u64()).into(),
                        ValueData::U64Value(event.task.from_peer.id).into(),
                        ValueData::StringValue(event.task.from_peer.addr.to_string()).into(),
                        ValueData::U64Value(event.task.to_peer.id).into(),
                        ValueData::StringValue(event.task.to_peer.addr.to_string()).into(),
                        ValueData::StringValue(event.task.trigger_reason.to_string()).into(),
                        ValueData::U64Value(event.task.timeout.as_millis() as u64).into(),
                        ValueData::StringValue(event.status.to_string()).into(),
                        ValueData::TimestampMillisecondValue(event.timestamp).into(),
                    ],
                }],
            }),
        };

        RowInsertRequests {
            inserts: vec![insert],
        }
    }

    fn build_schema(&self) -> Vec<ColumnSchema> {
        vec![
            ColumnSchema {
                column_name: REGION_MIGRATION_EVENTS_TABLE_PROCEDURE_ID_COLUMN_NAME.to_string(),
                datatype: ColumnDataType::String.into(),
                semantic_type: SemanticType::Field.into(),
                ..Default::default()
            },
            ColumnSchema {
                column_name: REGION_MIGRATION_EVENTS_TABLE_REGION_ID_COLUMN_NAME.to_string(),
                datatype: ColumnDataType::Uint64.into(),
                semantic_type: SemanticType::Field.into(),
                ..Default::default()
            },
            ColumnSchema {
                column_name: REGION_MIGRATION_EVENTS_TABLE_FROM_DATANODE_ID_COLUMN_NAME.to_string(),
                datatype: ColumnDataType::Uint64.into(),
                semantic_type: SemanticType::Field.into(),
                ..Default::default()
            },
            ColumnSchema {
                column_name: REGION_MIGRATION_EVENTS_TABLE_FROM_DATANODE_ADDR_COLUMN_NAME
                    .to_string(),
                datatype: ColumnDataType::String.into(),
                semantic_type: SemanticType::Field.into(),
                ..Default::default()
            },
            ColumnSchema {
                column_name: REGION_MIGRATION_EVENTS_TABLE_TO_DATANODE_ID_COLUMN_NAME.to_string(),
                datatype: ColumnDataType::Uint64.into(),
                semantic_type: SemanticType::Field.into(),
                ..Default::default()
            },
            ColumnSchema {
                column_name: REGION_MIGRATION_EVENTS_TABLE_TO_DATANODE_ADDR_COLUMN_NAME.to_string(),
                datatype: ColumnDataType::String.into(),
                semantic_type: SemanticType::Field.into(),
                ..Default::default()
            },
            ColumnSchema {
                column_name: REGION_MIGRATION_EVENTS_TABLE_TRIGGER_REASON_COLUMN_NAME.to_string(),
                datatype: ColumnDataType::String.into(),
                semantic_type: SemanticType::Field.into(),
                ..Default::default()
            },
            ColumnSchema {
                column_name: REGION_MIGRATION_EVENTS_TABLE_TIMEOUT_COLUMN_NAME.to_string(),
                datatype: ColumnDataType::Uint64.into(),
                semantic_type: SemanticType::Field.into(),
                ..Default::default()
            },
            ColumnSchema {
                column_name: REGION_MIGRATION_EVENTS_TABLE_STATUS_COLUMN_NAME.to_string(),
                datatype: ColumnDataType::String.into(),
                semantic_type: SemanticType::Field.into(),
                ..Default::default()
            },
            ColumnSchema {
                column_name: REGION_MIGRATION_EVENTS_TABLE_TIMESTAMP_COLUMN_NAME.to_string(),
                datatype: ColumnDataType::TimestampMillisecond.into(),
                semantic_type: SemanticType::Timestamp.into(),
                ..Default::default()
            },
        ]
    }
}
