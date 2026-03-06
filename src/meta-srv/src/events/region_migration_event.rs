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
use common_event_recorder::Event;
use common_event_recorder::error::{Result, SerializeEventSnafu};
use serde::Serialize;
use snafu::ResultExt;
use store_api::storage::RegionId;

use crate::procedure::region_migration::{PersistentContext, RegionMigrationTriggerReason};

pub const REGION_MIGRATION_EVENT_TYPE: &str = "region_migration";
pub const EVENTS_TABLE_REGION_ID_COLUMN_NAME: &str = "region_id";
pub const EVENTS_TABLE_TABLE_ID_COLUMN_NAME: &str = "table_id";
pub const EVENTS_TABLE_REGION_NUMBER_COLUMN_NAME: &str = "region_number";
pub const EVENTS_TABLE_REGION_MIGRATION_TRIGGER_REASON_COLUMN_NAME: &str =
    "region_migration_trigger_reason";
pub const EVENTS_TABLE_SRC_NODE_ID_COLUMN_NAME: &str = "region_migration_src_node_id";
pub const EVENTS_TABLE_SRC_PEER_ADDR_COLUMN_NAME: &str = "region_migration_src_peer_addr";
pub const EVENTS_TABLE_DST_NODE_ID_COLUMN_NAME: &str = "region_migration_dst_node_id";
pub const EVENTS_TABLE_DST_PEER_ADDR_COLUMN_NAME: &str = "region_migration_dst_peer_addr";

/// RegionMigrationEvent is the event of region migration.
#[derive(Debug)]
pub(crate) struct RegionMigrationEvent {
    // The region ids of the region migration.
    region_ids: Vec<RegionId>,
    // The trigger reason of the region migration.
    trigger_reason: RegionMigrationTriggerReason,
    // The source node id of the region migration.
    src_node_id: u64,
    // The source peer address of the region migration.
    src_peer_addr: String,
    // The destination node id of the region migration.
    dst_node_id: u64,
    // The destination peer address of the region migration.
    dst_peer_addr: String,
    // The timeout of the region migration.
    timeout: Duration,
}

#[derive(Debug, Serialize)]
struct Payload {
    #[serde(with = "humantime_serde")]
    timeout: Duration,
}

impl RegionMigrationEvent {
    pub fn from_persistent_ctx(ctx: &PersistentContext) -> Self {
        Self {
            region_ids: ctx.region_ids.clone(),
            trigger_reason: ctx.trigger_reason,
            src_node_id: ctx.from_peer.id,
            src_peer_addr: ctx.from_peer.addr.clone(),
            dst_node_id: ctx.to_peer.id,
            dst_peer_addr: ctx.to_peer.addr.clone(),
            timeout: ctx.timeout,
        }
    }
}

impl Event for RegionMigrationEvent {
    fn event_type(&self) -> &str {
        REGION_MIGRATION_EVENT_TYPE
    }

    fn extra_schema(&self) -> Vec<ColumnSchema> {
        vec![
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

    fn extra_rows(&self) -> Result<Vec<Row>> {
        let mut extra_rows = Vec::with_capacity(self.region_ids.len());
        for region_id in &self.region_ids {
            extra_rows.push(Row {
                values: vec![
                    ValueData::U64Value(region_id.as_u64()).into(),
                    ValueData::U32Value(region_id.table_id()).into(),
                    ValueData::U32Value(region_id.region_number()).into(),
                    ValueData::StringValue(self.trigger_reason.to_string()).into(),
                    ValueData::U64Value(self.src_node_id).into(),
                    ValueData::StringValue(self.src_peer_addr.clone()).into(),
                    ValueData::U64Value(self.dst_node_id).into(),
                    ValueData::StringValue(self.dst_peer_addr.clone()).into(),
                ],
            });
        }

        Ok(extra_rows)
    }

    fn json_payload(&self) -> Result<serde_json::Value> {
        serde_json::to_value(Payload {
            timeout: self.timeout,
        })
        .context(SerializeEventSnafu)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
