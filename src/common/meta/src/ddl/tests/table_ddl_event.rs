// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use api::v1::value::ValueData;
use api::v1::{ColumnDataType, SemanticType, Value};
use common_event_recorder::Event;
use serde::Serialize;
use serde_json::json;

use crate::ddl::table_ddl_event::{
    EVENTS_TABLE_CATALOG_NAME_COLUMN_NAME, EVENTS_TABLE_PHYSICAL_TABLE_ID_COLUMN_NAME,
    EVENTS_TABLE_SCHEMA_NAME_COLUMN_NAME, EVENTS_TABLE_TABLE_ID_COLUMN_NAME,
    EVENTS_TABLE_TABLE_NAME_COLUMN_NAME, TABLE_DDL_PAYLOAD_VERSION, TableDdlEvent,
    TableDdlEventType, TableDdlLocator, versioned_table_ddl_payload,
    versioned_table_ddl_payload_or_error,
};

const ALL_EVENT_TYPES: [TableDdlEventType; 8] = [
    TableDdlEventType::CreateTable,
    TableDdlEventType::CreateLogicalTables,
    TableDdlEventType::AlterTable,
    TableDdlEventType::AlterLogicalTables,
    TableDdlEventType::DropTable,
    TableDdlEventType::UndropTable,
    TableDdlEventType::PurgeDroppedTable,
    TableDdlEventType::TruncateTable,
];

#[test]
fn table_ddl_event_types_have_fixed_field_schemas() {
    for event_type in ALL_EVENT_TYPES {
        let submitted = TableDdlEvent::submitted(
            event_type,
            TableDdlLocator::default(),
            json!({"version": 1}),
        );
        let lifecycle = TableDdlEvent::lifecycle(event_type);
        let schema = submitted.extra_schema();

        assert_eq!(schema, lifecycle.extra_schema());
        assert!(
            schema
                .iter()
                .all(|column| column.semantic_type == SemanticType::Field as i32)
        );
        assert_eq!(
            schema
                .iter()
                .map(|column| (column.column_name.as_str(), column.datatype))
                .collect::<Vec<_>>(),
            expected_schema(event_type)
        );
    }
}

#[test]
fn submitted_event_keeps_locator_and_full_versioned_payload() {
    let large_option = "x".repeat(128 * 1024);
    let payload = versioned_table_ddl_payload(json!({
        "kind": "create_table",
        "table_options": {"large": large_option},
    }))
    .unwrap();
    let event = TableDdlEvent::submitted(
        TableDdlEventType::CreateTable,
        TableDdlLocator::new("greptime", "public", "metrics").with_table_id(42),
        payload.clone(),
    );

    assert_eq!(event.json_payload().unwrap(), payload);
    assert_eq!(
        event.json_payload().unwrap()["version"],
        TABLE_DDL_PAYLOAD_VERSION
    );
    assert_eq!(
        event.json_payload().unwrap()["data"]["table_options"]["large"]
            .as_str()
            .unwrap()
            .len(),
        128 * 1024
    );
    assert_eq!(
        event.extra_rows().unwrap()[0].values,
        vec![
            ValueData::StringValue("greptime".to_string()).into(),
            ValueData::StringValue("public".to_string()).into(),
            ValueData::StringValue("metrics".to_string()).into(),
            ValueData::U32Value(42).into(),
        ]
    );
}

#[test]
fn submitted_event_keeps_locator_when_payload_serialization_fails() {
    struct Unserializable;

    impl Serialize for Unserializable {
        fn serialize<S>(&self, _serializer: S) -> Result<S::Ok, S::Error>
        where
            S: serde::Serializer,
        {
            Err(serde::ser::Error::custom("sensitive serializer detail"))
        }
    }

    let payload = versioned_table_ddl_payload_or_error(Unserializable);
    let event = TableDdlEvent::submitted(
        TableDdlEventType::CreateTable,
        TableDdlLocator::new("greptime", "public", "metrics"),
        payload,
    );

    assert_eq!(
        event.json_payload().unwrap(),
        json!({
            "version": TABLE_DDL_PAYLOAD_VERSION,
            "error": {"code": "payload_serialization_failed"}
        })
    );
    assert_eq!(
        event.extra_rows().unwrap()[0].values,
        vec![
            ValueData::StringValue("greptime".to_string()).into(),
            ValueData::StringValue("public".to_string()).into(),
            ValueData::StringValue("metrics".to_string()).into(),
            Value::default(),
        ]
    );
}

#[test]
fn lifecycle_event_has_one_null_row_and_null_payload() {
    let event = TableDdlEvent::lifecycle(TableDdlEventType::AlterLogicalTables);

    assert_eq!(event.json_payload().unwrap(), serde_json::Value::Null);
    assert_eq!(
        event.extra_rows().unwrap()[0].values,
        vec![Value::default(); 5]
    );
}

#[test]
fn create_success_events_only_include_allocated_ids() {
    let create_table = TableDdlEvent::create_table_succeeded(7);
    assert_eq!(
        create_table.json_payload().unwrap(),
        serde_json::Value::Null
    );
    assert_eq!(
        create_table.extra_rows().unwrap()[0].values,
        vec![
            Value::default(),
            Value::default(),
            Value::default(),
            ValueData::U32Value(7).into(),
        ]
    );

    let logical_tables = TableDdlEvent::create_logical_tables_succeeded([8, 9]);
    let rows = logical_tables.extra_rows().unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].values[3], ValueData::U32Value(8).into());
    assert_eq!(rows[1].values[3], ValueData::U32Value(9).into());
    assert_eq!(rows[0].values[4], Value::default());
    assert_eq!(rows[1].values[4], Value::default());
}

#[test]
fn logical_table_submission_emits_one_row_per_locator() {
    let locators = [
        TableDdlLocator::new("greptime", "public", "cpu")
            .with_table_id(10)
            .with_physical_table_id(1),
        TableDdlLocator::new("greptime", "public", "memory")
            .with_table_id(11)
            .with_physical_table_id(1),
    ];
    let event = TableDdlEvent::submitted_for_tables(
        TableDdlEventType::CreateLogicalTables,
        locators,
        versioned_table_ddl_payload(json!({"kind": "create_logical_tables"})).unwrap(),
    );

    let rows = event.extra_rows().unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(
        rows[0].values[2],
        ValueData::StringValue("cpu".to_string()).into()
    );
    assert_eq!(rows[0].values[4], ValueData::U32Value(1).into());
    assert_eq!(
        rows[1].values[2],
        ValueData::StringValue("memory".to_string()).into()
    );
    assert_eq!(rows[1].values[4], ValueData::U32Value(1).into());
}

fn expected_schema(event_type: TableDdlEventType) -> Vec<(&'static str, i32)> {
    let mut schema = vec![
        (
            EVENTS_TABLE_CATALOG_NAME_COLUMN_NAME,
            ColumnDataType::String as i32,
        ),
        (
            EVENTS_TABLE_SCHEMA_NAME_COLUMN_NAME,
            ColumnDataType::String as i32,
        ),
        (
            EVENTS_TABLE_TABLE_NAME_COLUMN_NAME,
            ColumnDataType::String as i32,
        ),
        (
            EVENTS_TABLE_TABLE_ID_COLUMN_NAME,
            ColumnDataType::Uint32 as i32,
        ),
    ];
    if matches!(
        event_type,
        TableDdlEventType::CreateLogicalTables | TableDdlEventType::AlterLogicalTables
    ) {
        schema.push((
            EVENTS_TABLE_PHYSICAL_TABLE_ID_COLUMN_NAME,
            ColumnDataType::Uint32 as i32,
        ));
    }
    schema
}
