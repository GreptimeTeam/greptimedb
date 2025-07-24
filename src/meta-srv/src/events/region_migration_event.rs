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

use std::any::Any;
use std::time::Duration;

use api::v1::value::ValueData;
use api::v1::{ColumnDataType, ColumnSchema, Row, SemanticType};
use common_event_recorder::error::{DeserializeEventSnafu, Result, SerializeEventSnafu};
use common_event_recorder::Event;
use common_procedure::event::ProcedureEvent;
use common_procedure::{ProcedureId, ProcedureState};
use common_time::timestamp::Timestamp;
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use store_api::storage::{RegionId, TableId};

use crate::procedure::region_migration::{RegionMigrationDataOwned, RegionMigrationTriggerReason};

pub const REGION_MIGRATION_EVENT_TYPE: &str = "region_migration";
pub const EVENTS_TABLE_PROCEDURE_ID_COLUMN_NAME: &str = "procedure_id";
pub const EVENTS_TABLE_REGION_ID_COLUMN_NAME: &str = "region_id";
pub const EVENTS_TABLE_TABLE_ID_COLUMN_NAME: &str = "table_id";
pub const EVENTS_TABLE_REGION_NUMBER_COLUMN_NAME: &str = "region_number";
pub const EVENTS_TABLE_REGION_MIGRATION_TRIGGER_REASON_COLUMN_NAME: &str =
    "region_migration_trigger_reason";
pub const EVENTS_TABLE_REGION_MIGRATION_STATUS_COLUMN_NAME: &str = "region_migration_status";
pub const EVENTS_TABLE_SRC_NODE_ID_COLUMN_NAME: &str = "region_migration_src_node_id";
pub const EVENTS_TABLE_SRC_PEER_ADDR_COLUMN_NAME: &str = "region_migration_src_peer_addr";
pub const EVENTS_TABLE_DST_NODE_ID_COLUMN_NAME: &str = "region_migration_dst_node_id";
pub const EVENTS_TABLE_DST_PEER_ADDR_COLUMN_NAME: &str = "region_migration_dst_peer_addr";

/// RegionMigrationEvent is the event of region migration.
#[derive(Debug, Serialize)]
pub(crate) struct RegionMigrationEvent {
    #[serde(skip)]
    procedure_id: ProcedureId,
    #[serde(skip)]
    region_id: RegionId,
    #[serde(skip)]
    table_id: TableId,
    #[serde(skip)]
    region_number: u32,
    #[serde(skip)]
    timestamp: Timestamp,
    #[serde(skip)]
    trigger_reason: RegionMigrationTriggerReason,
    #[serde(skip)]
    status: RegionMigrationStatus,
    #[serde(skip)]
    src_node_id: u64,
    #[serde(skip)]
    src_peer_addr: String,
    #[serde(skip)]
    dst_node_id: u64,
    #[serde(skip)]
    dst_peer_addr: String,

    // The following fields will be serialized as the json payload.
    timeout: Duration,
    error: Option<String>,
}

/// RegionMigrationStatus is the status of the whole region migration procedure.
#[derive(Debug, Serialize, Deserialize, strum::Display)]
#[strum(serialize_all = "PascalCase")]
pub enum RegionMigrationStatus {
    /// The region migration procedure is running.
    Running,
    /// The region migration procedure is finished successfully.
    Done,
    /// The region migration procedure is failed.
    Failed,
    /// The region migration procedure is retrying.
    Retrying,
}

impl Event for RegionMigrationEvent {
    fn event_type(&self) -> &str {
        REGION_MIGRATION_EVENT_TYPE
    }

    fn extra_schema(&self) -> Vec<ColumnSchema> {
        vec![
            ColumnSchema {
                column_name: EVENTS_TABLE_PROCEDURE_ID_COLUMN_NAME.to_string(),
                datatype: ColumnDataType::String.into(),
                semantic_type: SemanticType::Field.into(),
                ..Default::default()
            },
            ColumnSchema {
                column_name: EVENTS_TABLE_REGION_ID_COLUMN_NAME.to_string(),
                datatype: ColumnDataType::Uint64.into(),
                semantic_type: SemanticType::Field.into(),
                ..Default::default()
            },
            ColumnSchema {
                column_name: EVENTS_TABLE_TABLE_ID_COLUMN_NAME.to_string(),
                datatype: ColumnDataType::Uint32.into(),
                semantic_type: SemanticType::Field.into(),
                ..Default::default()
            },
            ColumnSchema {
                column_name: EVENTS_TABLE_REGION_NUMBER_COLUMN_NAME.to_string(),
                datatype: ColumnDataType::Uint32.into(),
                semantic_type: SemanticType::Field.into(),
                ..Default::default()
            },
            ColumnSchema {
                column_name: EVENTS_TABLE_REGION_MIGRATION_TRIGGER_REASON_COLUMN_NAME.to_string(),
                datatype: ColumnDataType::String.into(),
                semantic_type: SemanticType::Field.into(),
                ..Default::default()
            },
            ColumnSchema {
                column_name: EVENTS_TABLE_REGION_MIGRATION_STATUS_COLUMN_NAME.to_string(),
                datatype: ColumnDataType::String.into(),
                semantic_type: SemanticType::Field.into(),
                ..Default::default()
            },
            ColumnSchema {
                column_name: EVENTS_TABLE_SRC_NODE_ID_COLUMN_NAME.to_string(),
                datatype: ColumnDataType::Uint64.into(),
                semantic_type: SemanticType::Field.into(),
                ..Default::default()
            },
            ColumnSchema {
                column_name: EVENTS_TABLE_SRC_PEER_ADDR_COLUMN_NAME.to_string(),
                datatype: ColumnDataType::String.into(),
                semantic_type: SemanticType::Field.into(),
                ..Default::default()
            },
            ColumnSchema {
                column_name: EVENTS_TABLE_DST_NODE_ID_COLUMN_NAME.to_string(),
                datatype: ColumnDataType::Uint64.into(),
                semantic_type: SemanticType::Field.into(),
                ..Default::default()
            },
            ColumnSchema {
                column_name: EVENTS_TABLE_DST_PEER_ADDR_COLUMN_NAME.to_string(),
                datatype: ColumnDataType::String.into(),
                semantic_type: SemanticType::Field.into(),
                ..Default::default()
            },
        ]
    }

    fn extra_row(&self) -> Result<Row> {
        Ok(Row {
            values: vec![
                ValueData::StringValue(self.procedure_id.to_string()).into(),
                ValueData::U64Value(self.region_id.as_u64()).into(),
                ValueData::U32Value(self.table_id).into(),
                ValueData::U32Value(self.region_number).into(),
                ValueData::StringValue(self.trigger_reason.to_string()).into(),
                ValueData::StringValue(self.status.to_string()).into(),
                ValueData::U64Value(self.src_node_id).into(),
                ValueData::StringValue(self.src_peer_addr.to_string()).into(),
                ValueData::U64Value(self.dst_node_id).into(),
                ValueData::StringValue(self.dst_peer_addr.to_string()).into(),
            ],
        })
    }

    fn json_payload(&self) -> Result<String> {
        serde_json::to_string(self).context(SerializeEventSnafu)
    }

    fn timestamp(&self) -> Timestamp {
        self.timestamp
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl RegionMigrationEvent {
    pub fn new_from_procedure_event(event: &ProcedureEvent) -> Result<Option<Self>> {
        let region_migration_data =
            serde_json::from_str::<RegionMigrationDataOwned>(&event.procedure_dump_data)
                .context(DeserializeEventSnafu)?;

        let (status, error_message) = match &event.state {
            ProcedureState::Running => (RegionMigrationStatus::Running, None),
            ProcedureState::Done { .. } => (RegionMigrationStatus::Done, None),
            ProcedureState::Failed { error } => {
                (RegionMigrationStatus::Failed, Some(error.to_string()))
            }
            ProcedureState::Retrying { error } => {
                (RegionMigrationStatus::Retrying, Some(error.to_string()))
            }
            // Ignore other events.
            _ => return Ok(None),
        };

        Ok(Some(Self {
            procedure_id: event.procedure_id,
            region_id: region_migration_data.persistent_ctx.region_id,
            table_id: region_migration_data.persistent_ctx.region_id.table_id(),
            region_number: region_migration_data
                .persistent_ctx
                .region_id
                .region_number(),
            timestamp: event.timestamp,
            trigger_reason: region_migration_data.persistent_ctx.trigger_reason,
            status,
            src_node_id: region_migration_data.persistent_ctx.from_peer.id,
            src_peer_addr: region_migration_data.persistent_ctx.from_peer.addr,
            dst_node_id: region_migration_data.persistent_ctx.to_peer.id,
            dst_peer_addr: region_migration_data.persistent_ctx.to_peer.addr,
            timeout: region_migration_data.persistent_ctx.timeout,
            error: error_message,
        }))
    }
}
