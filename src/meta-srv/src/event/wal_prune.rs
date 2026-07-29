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

use api::v1::value::ValueData;
use api::v1::{ColumnSchema, Row};
use common_event_recorder::Event;
use common_event_recorder::error::{Result, SerializeEventSnafu};
use common_event_recorder::event_table::{
    PREVIOUS_PRUNED_ENTRY_ID_COLUMN, PRUNED_ENTRY_ID_COLUMN, TOPIC_NAME_COLUMN, column_schemas,
    nullable_value,
};
use serde::Serialize;
use snafu::ResultExt;
use store_api::logstore::EntryId;

/// Procedure event type for WAL pruning.
pub(crate) const WAL_PRUNE_EVENT_TYPE: &str = "wal_prune";

const PAYLOAD_VERSION: u8 = 1;

#[derive(Debug, Serialize)]
struct WalPrunePayload {
    version: u8,
    logical_delete: bool,
}

/// A sparse lifecycle event for a completed WAL prune operation.
#[derive(Debug)]
pub(crate) struct WalPruneEvent {
    topic_name: String,
    previous_pruned_entry_id: Option<EntryId>,
    pruned_entry_id: EntryId,
    payload: WalPrunePayload,
}

impl WalPruneEvent {
    /// Creates an event for a completed WAL prune operation.
    pub(crate) fn new(
        topic_name: &str,
        previous_pruned_entry_id: Option<EntryId>,
        pruned_entry_id: EntryId,
        logical_delete: bool,
    ) -> Self {
        Self {
            topic_name: topic_name.to_string(),
            previous_pruned_entry_id,
            pruned_entry_id,
            payload: WalPrunePayload {
                version: PAYLOAD_VERSION,
                logical_delete,
            },
        }
    }
}

impl Event for WalPruneEvent {
    fn event_type(&self) -> &str {
        WAL_PRUNE_EVENT_TYPE
    }

    fn json_payload(&self) -> Result<serde_json::Value> {
        serde_json::to_value(&self.payload).context(SerializeEventSnafu)
    }

    fn extra_schema(&self) -> Vec<ColumnSchema> {
        column_schemas([
            &TOPIC_NAME_COLUMN,
            &PREVIOUS_PRUNED_ENTRY_ID_COLUMN,
            &PRUNED_ENTRY_ID_COLUMN,
        ])
    }

    fn extra_rows(&self) -> Result<Vec<Row>> {
        Ok(vec![Row {
            values: vec![
                ValueData::StringValue(self.topic_name.clone()).into(),
                nullable_value(self.previous_pruned_entry_id.map(ValueData::U64Value)),
                ValueData::U64Value(self.pruned_entry_id).into(),
            ],
        }])
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use api::v1::Row;
    use common_event_recorder::Event;
    use common_event_recorder::event_table::{
        PREVIOUS_PRUNED_ENTRY_ID_COLUMN, PRUNED_ENTRY_ID_COLUMN, TOPIC_NAME_COLUMN, column_schemas,
    };
    use common_event_recorder::testing::assert_event_contract;

    use super::*;

    #[test]
    fn test_wal_prune_event_contract() {
        let event = WalPruneEvent::new("greptimedb_wal_topic_0", Some(100), 200, false);

        assert_event_contract(
            &event,
            WAL_PRUNE_EVENT_TYPE,
            &column_schemas([
                &TOPIC_NAME_COLUMN,
                &PREVIOUS_PRUNED_ENTRY_ID_COLUMN,
                &PRUNED_ENTRY_ID_COLUMN,
            ]),
            &[Row {
                values: vec![
                    ValueData::StringValue("greptimedb_wal_topic_0".to_string()).into(),
                    ValueData::U64Value(100).into(),
                    ValueData::U64Value(200).into(),
                ],
            }],
        );
        assert_eq!(
            event.json_payload().unwrap(),
            serde_json::json!({
                "version": 1,
                "logical_delete": false,
            })
        );

        let event = WalPruneEvent::new("greptimedb_wal_topic_1", None, 42, true);
        assert!(
            event.extra_rows().unwrap()[0].values[1]
                .value_data
                .is_none()
        );
        assert_eq!(
            event.json_payload().unwrap(),
            serde_json::json!({
                "version": 1,
                "logical_delete": true,
            })
        );
    }
}
