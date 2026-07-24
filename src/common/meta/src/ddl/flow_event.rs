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
use api::v1::{ColumnDataType, ColumnSchema, Row, SemanticType, Value};
use common_event_recorder::Event;
use common_event_recorder::error::{Result, SerializeEventSnafu};
use serde::Serialize;
use snafu::ResultExt;

pub(crate) const CREATE_FLOW_EVENT_TYPE: &str = "ddl_create_flow";
pub(crate) const DROP_FLOW_EVENT_TYPE: &str = "ddl_drop_flow";

pub(crate) const CATALOG_NAME_COLUMN: &str = "catalog_name";
pub(crate) const SCHEMA_NAME_COLUMN: &str = "schema_name";
pub(crate) const FLOW_NAME_COLUMN: &str = "flow_name";
pub(crate) const FLOW_ID_COLUMN: &str = "flow_id";

const PAYLOAD_VERSION: u8 = 1;

/// The bounded Create Flow intent allowed in a submitted event payload.
#[derive(Debug)]
pub(crate) struct CreateFlowEventIntent {
    pub(crate) or_replace: bool,
    pub(crate) create_if_not_exists: bool,
    pub(crate) expire_after: Option<i64>,
    pub(crate) eval_interval_secs: Option<i64>,
}

#[derive(Debug, Serialize)]
struct CreateFlowPayloadV1 {
    version: u8,
    or_replace: bool,
    create_if_not_exists: bool,
    expire_after: Option<i64>,
    eval_interval_secs: Option<i64>,
}

#[derive(Debug, Serialize)]
struct DropFlowPayloadV1 {
    version: u8,
    drop_if_exists: bool,
}

#[derive(Debug, Serialize)]
#[serde(untagged)]
enum FlowDdlPayload {
    Create(CreateFlowPayloadV1),
    Drop(DropFlowPayloadV1),
}

/// A Flow DDL event with a schema shared by every lifecycle trigger.
#[derive(Debug)]
pub(crate) struct FlowDdlEvent {
    event_type: &'static str,
    catalog_name: Option<String>,
    schema_name: Option<String>,
    flow_name: Option<String>,
    flow_id: Option<u32>,
    payload: Option<FlowDdlPayload>,
}

impl FlowDdlEvent {
    /// Builds the bounded event emitted when creating a Flow is submitted.
    pub(crate) fn create_submitted(
        catalog_name: &str,
        schema_name: &str,
        flow_name: &str,
        intent: CreateFlowEventIntent,
    ) -> Self {
        Self {
            event_type: CREATE_FLOW_EVENT_TYPE,
            catalog_name: Some(catalog_name.to_string()),
            schema_name: Some(schema_name.to_string()),
            flow_name: Some(flow_name.to_string()),
            flow_id: None,
            payload: Some(FlowDdlPayload::Create(CreateFlowPayloadV1 {
                version: PAYLOAD_VERSION,
                or_replace: intent.or_replace,
                create_if_not_exists: intent.create_if_not_exists,
                expire_after: intent.expire_after,
                eval_interval_secs: intent.eval_interval_secs,
            })),
        }
    }

    /// Builds the bounded event emitted when dropping a Flow is submitted.
    pub(crate) fn drop_submitted(
        catalog_name: &str,
        flow_name: &str,
        flow_id: u32,
        drop_if_exists: bool,
    ) -> Self {
        Self {
            event_type: DROP_FLOW_EVENT_TYPE,
            catalog_name: Some(catalog_name.to_string()),
            schema_name: None,
            flow_name: Some(flow_name.to_string()),
            flow_id: Some(flow_id),
            payload: Some(FlowDdlPayload::Drop(DropFlowPayloadV1 {
                version: PAYLOAD_VERSION,
                drop_if_exists,
            })),
        }
    }

    /// Builds a lightweight Create Flow lifecycle event.
    pub(crate) fn create_lifecycle() -> Self {
        Self::lifecycle(CREATE_FLOW_EVENT_TYPE)
    }

    /// Builds a successful Create Flow event containing only a resolved ID.
    pub(crate) fn create_succeeded(flow_id: Option<u32>) -> Self {
        Self {
            flow_id,
            ..Self::lifecycle(CREATE_FLOW_EVENT_TYPE)
        }
    }

    /// Builds a lightweight Drop Flow lifecycle event.
    pub(crate) fn drop_lifecycle() -> Self {
        Self::lifecycle(DROP_FLOW_EVENT_TYPE)
    }

    fn lifecycle(event_type: &'static str) -> Self {
        Self {
            event_type,
            catalog_name: None,
            schema_name: None,
            flow_name: None,
            flow_id: None,
            payload: None,
        }
    }
}

impl Event for FlowDdlEvent {
    fn event_type(&self) -> &str {
        self.event_type
    }

    fn json_payload(&self) -> Result<serde_json::Value> {
        match &self.payload {
            Some(payload) => serde_json::to_value(payload).context(SerializeEventSnafu),
            None => Ok(serde_json::Value::Null),
        }
    }

    fn extra_schema(&self) -> Vec<ColumnSchema> {
        [
            (CATALOG_NAME_COLUMN, ColumnDataType::String),
            (SCHEMA_NAME_COLUMN, ColumnDataType::String),
            (FLOW_NAME_COLUMN, ColumnDataType::String),
            (FLOW_ID_COLUMN, ColumnDataType::Uint32),
        ]
        .into_iter()
        .map(|(column_name, datatype)| ColumnSchema {
            column_name: column_name.to_string(),
            datatype: datatype.into(),
            semantic_type: SemanticType::Field.into(),
            ..Default::default()
        })
        .collect()
    }

    fn extra_rows(&self) -> Result<Vec<Row>> {
        Ok(vec![Row {
            values: vec![
                nullable_string(&self.catalog_name),
                nullable_string(&self.schema_name),
                nullable_string(&self.flow_name),
                nullable_u32(self.flow_id),
            ],
        }])
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

fn nullable_string(value: &Option<String>) -> Value {
    match value {
        Some(value) => ValueData::StringValue(value.clone()).into(),
        None => Value { value_data: None },
    }
}

fn nullable_u32(value: Option<u32>) -> Value {
    match value {
        Some(value) => ValueData::U32Value(value).into(),
        None => Value { value_data: None },
    }
}

#[cfg(test)]
mod tests {
    use common_event_recorder::Event;

    use super::*;

    #[test]
    fn flow_event_types_and_schema_are_stable() {
        let create = FlowDdlEvent::create_lifecycle();
        let drop = FlowDdlEvent::drop_lifecycle();

        assert_eq!(create.event_type(), CREATE_FLOW_EVENT_TYPE);
        assert_eq!(drop.event_type(), DROP_FLOW_EVENT_TYPE);
        assert_eq!(create.extra_schema(), drop.extra_schema());
        assert_eq!(
            create
                .extra_schema()
                .into_iter()
                .map(|column| (column.column_name, column.datatype, column.semantic_type))
                .collect::<Vec<_>>(),
            vec![
                (
                    CATALOG_NAME_COLUMN.to_string(),
                    ColumnDataType::String as i32,
                    SemanticType::Field as i32,
                ),
                (
                    SCHEMA_NAME_COLUMN.to_string(),
                    ColumnDataType::String as i32,
                    SemanticType::Field as i32,
                ),
                (
                    FLOW_NAME_COLUMN.to_string(),
                    ColumnDataType::String as i32,
                    SemanticType::Field as i32,
                ),
                (
                    FLOW_ID_COLUMN.to_string(),
                    ColumnDataType::Uint32 as i32,
                    SemanticType::Field as i32,
                ),
            ]
        );
    }

    #[test]
    fn submitted_payloads_are_bounded_and_versioned() {
        let create = FlowDdlEvent::create_submitted(
            "greptime",
            "public",
            "metrics",
            CreateFlowEventIntent {
                or_replace: true,
                create_if_not_exists: false,
                expire_after: Some(300),
                eval_interval_secs: Some(60),
            },
        );
        let drop = FlowDdlEvent::drop_submitted("greptime", "metrics", 42, true);

        assert_eq!(
            create.json_payload().unwrap(),
            serde_json::json!({
                "version": 1,
                "or_replace": true,
                "create_if_not_exists": false,
                "expire_after": 300,
                "eval_interval_secs": 60,
            })
        );
        assert_eq!(
            drop.json_payload().unwrap(),
            serde_json::json!({"version": 1, "drop_if_exists": true})
        );
    }

    #[test]
    fn lifecycle_rows_are_typed_null_except_create_success_id() {
        for event in [
            FlowDdlEvent::create_lifecycle(),
            FlowDdlEvent::create_succeeded(None),
            FlowDdlEvent::drop_lifecycle(),
        ] {
            assert_eq!(event.json_payload().unwrap(), serde_json::Value::Null);
            assert!(
                event.extra_rows().unwrap()[0]
                    .values
                    .iter()
                    .all(|value| value.value_data.is_none())
            );
        }

        let row = FlowDdlEvent::create_succeeded(Some(42))
            .extra_rows()
            .unwrap()
            .remove(0);
        assert!(
            row.values[..3]
                .iter()
                .all(|value| value.value_data.is_none())
        );
        assert_eq!(row.values[3].value_data, Some(ValueData::U32Value(42)));
    }
}
