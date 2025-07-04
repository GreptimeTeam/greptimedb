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
use std::fmt::Display;

use api::v1::value::ValueData;
use api::v1::{ColumnDataType, ColumnSchema, Row, RowInsertRequest, Rows, SemanticType};
use common_procedure::error::Error;
use common_procedure::ProcedureId;
use common_time::timestamp::{TimeUnit, Timestamp};

use crate::event_recorder::Event;
use crate::procedure::region_migration::manager::RegionMigrationProcedureTask;

pub const REGION_MIGRATION_EVENTS_TABLE_NAME: &str = "region_migration_events";

pub const REGION_MIGRATION_EVENTS_TABLE_PROCEDURE_ID_COLUMN_NAME: &str = "procedure_id";
pub const REGION_MIGRATION_EVENTS_TABLE_REGION_ID_COLUMN_NAME: &str = "region_id";
pub const REGION_MIGRATION_EVENTS_TABLE_TABLE_ID_COLUMN_NAME: &str = "table_id";
pub const REGION_MIGRATION_EVENTS_TABLE_REGION_NUMBER_COLUMN_NAME: &str = "region_number";
pub const REGION_MIGRATION_EVENTS_TABLE_FROM_DATANODE_ID_COLUMN_NAME: &str = "from_datanode_id";
pub const REGION_MIGRATION_EVENTS_TABLE_FROM_DATANODE_ADDR_COLUMN_NAME: &str = "from_datanode_addr";
pub const REGION_MIGRATION_EVENTS_TABLE_TO_DATANODE_ID_COLUMN_NAME: &str = "to_datanode_id";
pub const REGION_MIGRATION_EVENTS_TABLE_TO_DATANODE_ADDR_COLUMN_NAME: &str = "to_datanode_addr";
pub const REGION_MIGRATION_EVENTS_TABLE_TRIGGER_REASON_COLUMN_NAME: &str = "trigger_reason";
pub const REGION_MIGRATION_EVENTS_TABLE_TIMEOUT_COLUMN_NAME: &str = "timeout_ms";
pub const REGION_MIGRATION_EVENTS_TABLE_STATUS_COLUMN_NAME: &str = "status";
pub const REGION_MIGRATION_EVENTS_TABLE_TIMESTAMP_COLUMN_NAME: &str = "timestamp";

/// RegionMigrationEvent is the event of region migration.
#[derive(Debug)]
pub struct RegionMigrationEvent {
    pub(crate) task: RegionMigrationProcedureTask,
    pub(crate) procedure_id: ProcedureId,
    pub(crate) status: RegionMigrationStatus,
    pub(crate) timestamp: Timestamp,
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
    // The region migration procedure is failed with an error.
    Failed(Error),
}

impl Display for RegionMigrationStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Starting => write!(f, "Starting"),
            Self::Running => write!(f, "Running"),
            Self::Finished => write!(f, "Finished"),
            Self::Failed(e) => write!(f, "Failed: {:?}", e),
        }
    }
}

impl Event for RegionMigrationEvent {
    fn name(&self) -> &str {
        "region_migration_event"
    }

    fn to_row_insert(&self) -> RowInsertRequest {
        RowInsertRequest {
            table_name: self.table_name().to_string(),
            rows: Some(Rows {
                schema: self.schema(),
                rows: vec![self.to_row()],
            }),
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl RegionMigrationEvent {
    pub fn new(
        task: RegionMigrationProcedureTask,
        procedure_id: ProcedureId,
        status: RegionMigrationStatus,
    ) -> Self {
        Self {
            task,
            procedure_id,
            status,
            timestamp: Timestamp::current_time(TimeUnit::Nanosecond),
        }
    }

    fn table_name(&self) -> &str {
        REGION_MIGRATION_EVENTS_TABLE_NAME
    }

    fn schema(&self) -> Vec<ColumnSchema> {
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
                column_name: REGION_MIGRATION_EVENTS_TABLE_TABLE_ID_COLUMN_NAME.to_string(),
                datatype: ColumnDataType::Uint32.into(),
                semantic_type: SemanticType::Field.into(),
                ..Default::default()
            },
            ColumnSchema {
                column_name: REGION_MIGRATION_EVENTS_TABLE_REGION_NUMBER_COLUMN_NAME.to_string(),
                datatype: ColumnDataType::Uint32.into(),
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
                datatype: ColumnDataType::TimestampNanosecond.into(),
                semantic_type: SemanticType::Timestamp.into(),
                ..Default::default()
            },
        ]
    }

    fn to_row(&self) -> Row {
        Row {
            values: vec![
                ValueData::StringValue(self.procedure_id.to_string()).into(),
                ValueData::U64Value(self.task.region_id.as_u64()).into(),
                ValueData::U32Value(self.task.region_id.table_id()).into(),
                ValueData::U32Value(self.task.region_id.region_number()).into(),
                ValueData::U64Value(self.task.from_peer.id).into(),
                ValueData::StringValue(self.task.from_peer.addr.to_string()).into(),
                ValueData::U64Value(self.task.to_peer.id).into(),
                ValueData::StringValue(self.task.to_peer.addr.to_string()).into(),
                ValueData::StringValue(self.task.trigger_reason.to_string()).into(),
                ValueData::U64Value(self.task.timeout.as_millis() as u64).into(),
                ValueData::StringValue(self.status.to_string()).into(),
                ValueData::TimestampNanosecondValue(self.timestamp.value()).into(),
            ],
        }
    }
}
