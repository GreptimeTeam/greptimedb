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

pub(crate) const CREATE_VIEW_EVENT_TYPE: &str = "ddl_create_view";
pub(crate) const DROP_VIEW_EVENT_TYPE: &str = "ddl_drop_view";

pub(crate) const CATALOG_NAME_COLUMN: &str = "catalog_name";
pub(crate) const SCHEMA_NAME_COLUMN: &str = "schema_name";
pub(crate) const VIEW_NAME_COLUMN: &str = "view_name";
pub(crate) const VIEW_ID_COLUMN: &str = "view_id";

const PAYLOAD_VERSION: u8 = 1;

/// The bounded, versioned intent recorded when a view creation is submitted.
#[derive(Debug, Serialize)]
pub(crate) struct CreateViewPayloadV1 {
    version: u8,
    or_replace: bool,
    create_if_not_exists: bool,
    referenced_table_count: usize,
    column_count: usize,
}

impl CreateViewPayloadV1 {
    pub(crate) fn new(
        or_replace: bool,
        create_if_not_exists: bool,
        referenced_table_count: usize,
        column_count: usize,
    ) -> Self {
        Self {
            version: PAYLOAD_VERSION,
            or_replace,
            create_if_not_exists,
            referenced_table_count,
            column_count,
        }
    }
}

/// The bounded, versioned intent recorded when a view drop is submitted.
#[derive(Debug, Serialize)]
pub(crate) struct DropViewPayloadV1 {
    version: u8,
    drop_if_exists: bool,
}

impl DropViewPayloadV1 {
    pub(crate) fn new(drop_if_exists: bool) -> Self {
        Self {
            version: PAYLOAD_VERSION,
            drop_if_exists,
        }
    }
}

#[derive(Debug)]
pub(crate) struct ViewDdlEvent {
    event_type: &'static str,
    catalog_name: Option<String>,
    schema_name: Option<String>,
    view_name: Option<String>,
    view_id: Option<u32>,
    payload: Option<ViewDdlPayload>,
}

#[derive(Debug, Serialize)]
#[serde(untagged)]
enum ViewDdlPayload {
    Create(CreateViewPayloadV1),
    Drop(DropViewPayloadV1),
}

impl ViewDdlEvent {
    /// Builds the rich event emitted when creating a view is submitted.
    pub(crate) fn create_submitted(
        catalog_name: &str,
        schema_name: &str,
        view_name: &str,
        or_replace: bool,
        create_if_not_exists: bool,
        referenced_table_count: usize,
        column_count: usize,
    ) -> Self {
        Self::submitted(
            CREATE_VIEW_EVENT_TYPE,
            catalog_name,
            schema_name,
            view_name,
            None,
            ViewDdlPayload::Create(CreateViewPayloadV1::new(
                or_replace,
                create_if_not_exists,
                referenced_table_count,
                column_count,
            )),
        )
    }

    /// Builds the rich event emitted when dropping a view is submitted.
    pub(crate) fn drop_submitted(
        catalog_name: &str,
        schema_name: &str,
        view_name: &str,
        view_id: u32,
        drop_if_exists: bool,
    ) -> Self {
        Self::submitted(
            DROP_VIEW_EVENT_TYPE,
            catalog_name,
            schema_name,
            view_name,
            Some(view_id),
            ViewDdlPayload::Drop(DropViewPayloadV1::new(drop_if_exists)),
        )
    }

    /// Builds a lightweight create-view lifecycle event with no locator data.
    pub(crate) fn create_lifecycle() -> Self {
        Self::lifecycle(CREATE_VIEW_EVENT_TYPE)
    }

    /// Builds the successful create-view row that carries only the allocated id.
    pub(crate) fn create_succeeded(view_id: u32) -> Self {
        Self::succeeded(CREATE_VIEW_EVENT_TYPE, view_id)
    }

    /// Builds a lightweight drop-view lifecycle event with no locator data.
    pub(crate) fn drop_lifecycle() -> Self {
        Self::lifecycle(DROP_VIEW_EVENT_TYPE)
    }

    /// Builds the successful drop-view row, which carries no locator data.
    pub(crate) fn drop_succeeded() -> Self {
        Self::drop_lifecycle()
    }

    fn submitted(
        event_type: &'static str,
        catalog_name: &str,
        schema_name: &str,
        view_name: &str,
        view_id: Option<u32>,
        payload: ViewDdlPayload,
    ) -> Self {
        Self {
            event_type,
            catalog_name: Some(catalog_name.to_string()),
            schema_name: Some(schema_name.to_string()),
            view_name: Some(view_name.to_string()),
            view_id,
            payload: Some(payload),
        }
    }

    fn lifecycle(event_type: &'static str) -> Self {
        Self {
            event_type,
            catalog_name: None,
            schema_name: None,
            view_name: None,
            view_id: None,
            payload: None,
        }
    }

    fn succeeded(event_type: &'static str, view_id: u32) -> Self {
        Self {
            event_type,
            catalog_name: None,
            schema_name: None,
            view_name: None,
            view_id: Some(view_id),
            payload: None,
        }
    }
}

impl Event for ViewDdlEvent {
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
            (VIEW_NAME_COLUMN, ColumnDataType::String),
            (VIEW_ID_COLUMN, ColumnDataType::Uint32),
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
                nullable_string(&self.view_name),
                nullable_u32(self.view_id),
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
    use api::v1::value::ValueData;
    use common_event_recorder::Event;

    use super::*;

    #[test]
    fn create_submitted_event_contains_only_bounded_intent() {
        let event =
            ViewDdlEvent::create_submitted("greptime", "public", "v_metrics", true, false, 3, 5);

        assert_eq!(event.event_type(), CREATE_VIEW_EVENT_TYPE);
        assert_eq!(
            event.json_payload().unwrap(),
            serde_json::json!({
                "version": 1,
                "or_replace": true,
                "create_if_not_exists": false,
                "referenced_table_count": 3,
                "column_count": 5,
            })
        );
        assert!(!event.json_payload().unwrap().to_string().contains("SELECT"));

        let row = event.extra_rows().unwrap().remove(0);
        assert_eq!(
            row.values[..3],
            vec![
                ValueData::StringValue("greptime".to_string()).into(),
                ValueData::StringValue("public".to_string()).into(),
                ValueData::StringValue("v_metrics".to_string()).into(),
            ]
        );
        assert!(row.values[3].value_data.is_none());
    }

    #[test]
    fn drop_submitted_event_contains_only_typed_intent() {
        let event = ViewDdlEvent::drop_submitted("greptime", "public", "v_metrics", 42, true);

        assert_eq!(event.event_type(), DROP_VIEW_EVENT_TYPE);
        assert_eq!(
            event.json_payload().unwrap(),
            serde_json::json!({"version": 1, "drop_if_exists": true})
        );
    }

    #[test]
    fn lifecycle_rows_keep_fixed_schema_and_null_locators() {
        let submitted = ViewDdlEvent::create_submitted("c", "s", "v", false, false, 0, 0);
        let events = [
            ViewDdlEvent::create_lifecycle(),
            ViewDdlEvent::create_succeeded(7),
            ViewDdlEvent::drop_lifecycle(),
            ViewDdlEvent::drop_succeeded(),
        ];

        assert_eq!(submitted.extra_schema().len(), 4);
        assert!(
            submitted
                .extra_schema()
                .iter()
                .all(|column| column.semantic_type == SemanticType::Field as i32)
        );

        for event in events {
            assert_eq!(event.extra_schema(), submitted.extra_schema());
            assert_eq!(event.json_payload().unwrap(), serde_json::Value::Null);
        }

        let create_lifecycle = ViewDdlEvent::create_lifecycle()
            .extra_rows()
            .unwrap()
            .remove(0);
        assert!(
            create_lifecycle
                .values
                .iter()
                .all(|value| value.value_data.is_none())
        );

        let create_succeeded = ViewDdlEvent::create_succeeded(7)
            .extra_rows()
            .unwrap()
            .remove(0);
        assert!(
            create_succeeded.values[..3]
                .iter()
                .all(|value| value.value_data.is_none())
        );
        assert_eq!(
            create_succeeded.values[3].value_data,
            Some(ValueData::U32Value(7))
        );

        let drop_succeeded = ViewDdlEvent::drop_succeeded()
            .extra_rows()
            .unwrap()
            .remove(0);
        assert!(
            drop_succeeded
                .values
                .iter()
                .all(|value| value.value_data.is_none())
        );
    }
}
