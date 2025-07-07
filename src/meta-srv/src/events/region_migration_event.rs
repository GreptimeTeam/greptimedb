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
use common_event_recorder::error::{Result, SerializeEventSnafu};
use common_event_recorder::Event;
use common_meta::peer::Peer;
use common_procedure::error::Error;
use common_procedure::ProcedureId;
use common_time::timestamp::{TimeUnit, Timestamp};
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use store_api::storage::{RegionId, TableId};

use crate::procedure::region_migration::manager::RegionMigrationProcedureTask;
use crate::procedure::region_migration::RegionMigrationTriggerReason;

pub const REGION_MIGRATION_EVENT_TYPE: &str = "region_migration";
pub const EVENTS_TABLE_PROCEDURE_ID_COLUMN_NAME: &str = "procedure_id";
pub const EVENTS_TABLE_REGION_ID_COLUMN_NAME: &str = "region_id";
pub const EVENTS_TABLE_TABLE_ID_COLUMN_NAME: &str = "table_id";
pub const EVENTS_TABLE_REGION_NUMBER_COLUMN_NAME: &str = "region_number";
pub const EVENTS_TABLE_REGION_MIGRATION_TRIGGER_REASON_COLUMN_NAME: &str =
    "region_migration_trigger_reason";
pub const EVENTS_TABLE_REGION_MIGRATION_STATUS_COLUMN_NAME: &str = "region_migration_status";

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

    // The following fields will be serialized as the json payload.
    from_peer: Peer,
    to_peer: Peer,
    timeout: Duration,
    error: Option<String>,
}

/// RegionMigrationStatus is the status of the whole region migration procedure.
#[derive(Debug, Serialize, Deserialize, strum::Display)]
#[strum(serialize_all = "PascalCase")]
pub enum RegionMigrationStatus {
    // The manager creates the region migration task and prepares to submit to the procedure manager.
    Starting,
    // The region migration procedure is running.
    Running,
    // The region migration procedure is finished successfully.
    Finished,
    // The region migration procedure is failed.
    Failed,
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
    pub fn new(
        task: RegionMigrationProcedureTask,
        procedure_id: ProcedureId,
        status: RegionMigrationStatus,
        error: Option<Error>,
    ) -> Self {
        let table_id = task.region_id.table_id();
        let region_number = task.region_id.region_number();
        Self {
            procedure_id,
            region_id: task.region_id,
            table_id,
            region_number,
            from_peer: task.from_peer,
            to_peer: task.to_peer,
            timeout: task.timeout,
            trigger_reason: task.trigger_reason,
            status,
            timestamp: Timestamp::current_time(TimeUnit::Nanosecond),
            error: error.map(|e| e.to_string()),
        }
    }
}
