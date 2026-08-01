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

use std::collections::HashMap;
use std::sync::Arc;

use api::v1::alter_database_expr::Kind as PbAlterDatabaseKind;
use api::v1::value::ValueData;
use api::v1::{AlterDatabaseExpr, Row, SetDatabaseOptions as PbSetDatabaseOptions, Value};
use common_event_recorder::Event;
use common_event_recorder::event_table::{
    CATALOG_NAME_COLUMN as EVENT_TABLE_CATALOG_NAME_COLUMN,
    PROCEDURE_ERROR_COLUMN as EVENT_TABLE_PROCEDURE_ERROR_COLUMN,
    PROCEDURE_ID_COLUMN as EVENT_TABLE_PROCEDURE_ID_COLUMN,
    PROCEDURE_STATE_COLUMN as EVENT_TABLE_PROCEDURE_STATE_COLUMN,
    PROCEDURE_TRIGGER_COLUMN as EVENT_TABLE_PROCEDURE_TRIGGER_COLUMN,
    SCHEMA_NAME_COLUMN as EVENT_TABLE_SCHEMA_NAME_COLUMN, jsonb_value,
};
use common_event_recorder::testing::assert_event_contract;
use common_procedure::{EventTrigger, ProcedureEvent, ProcedureId, ProcedureState};

use super::test_util::assert_event_filter;
use crate::ddl::alter_database::AlterDatabaseProcedure;
use crate::ddl::create_database::CreateDatabaseProcedure;
use crate::ddl::drop_database::DropDatabaseProcedure;
use crate::ddl::event::database::{
    ALTER_DATABASE_EVENT_TYPE, CREATE_DATABASE_EVENT_TYPE, DROP_DATABASE_EVENT_TYPE,
    DatabaseDdlEvent,
};
use crate::rpc::ddl::{
    AlterDatabaseKind, SetDatabaseOption, SetDatabaseOptions, UnsetDatabaseOption,
    UnsetDatabaseOptions,
};
use crate::test_util::{MockDatanodeManager, new_ddl_context};

#[test]
fn test_create_database_submitted_event_contract() {
    let options = HashMap::from([
        ("password".to_string(), "do-not-record".to_string()),
        ("compaction.type".to_string(), "twcs".to_string()),
    ]);
    let event = DatabaseDdlEvent::create_submitted("greptime", "metrics", true, &options);

    assert_event_locator(
        &event,
        CREATE_DATABASE_EVENT_TYPE,
        Some("greptime"),
        Some("metrics"),
    );
    assert_eq!(
        event.json_payload().unwrap(),
        serde_json::json!({
            "version": 1,
            "create_if_not_exists": true,
            "options": [
                {"key": "compaction.type", "value": "twcs"},
                {"key": "password", "value": "do-not-record"}
            ]
        })
    );
    assert!(
        !event
            .json_payload()
            .unwrap()
            .to_string()
            .contains("CreateMetadata")
    );
}

#[test]
fn test_alter_database_set_and_unset_event_contracts() {
    let set = DatabaseDdlEvent::alter_submitted(
        "greptime",
        "metrics",
        &AlterDatabaseKind::SetDatabaseOptions(SetDatabaseOptions(vec![
            SetDatabaseOption::Other("secret_token".to_string(), "hidden".to_string()),
            SetDatabaseOption::Ttl(std::time::Duration::from_secs(3600).into()),
        ])),
    );
    assert_event_locator(
        &set,
        ALTER_DATABASE_EVENT_TYPE,
        Some("greptime"),
        Some("metrics"),
    );
    assert_eq!(
        set.json_payload().unwrap(),
        serde_json::json!({
            "version": 1,
            "action": "set",
            "options": [
                {"key": "secret_token", "value": "hidden"},
                {"key": "ttl", "value": "1h"}
            ]
        })
    );

    let unset = DatabaseDdlEvent::alter_submitted(
        "greptime",
        "metrics",
        &AlterDatabaseKind::UnsetDatabaseOptions(UnsetDatabaseOptions(vec![
            UnsetDatabaseOption::Ttl,
            UnsetDatabaseOption::Other("compaction.type".to_string()),
        ])),
    );
    assert_event_locator(
        &unset,
        ALTER_DATABASE_EVENT_TYPE,
        Some("greptime"),
        Some("metrics"),
    );
    assert_eq!(
        unset.json_payload().unwrap(),
        serde_json::json!({
            "version": 1,
            "action": "unset",
            "options": ["ttl", "compaction.type"]
        })
    );
}

#[test]
fn test_drop_database_submitted_event_contract() {
    let event = DatabaseDdlEvent::drop_submitted("greptime", "metrics", true);

    assert_event_locator(
        &event,
        DROP_DATABASE_EVENT_TYPE,
        Some("greptime"),
        Some("metrics"),
    );
    assert_eq!(
        event.json_payload().unwrap(),
        serde_json::json!({"version": 1, "drop_if_exists": true})
    );
}

#[test]
fn test_database_lifecycle_events_have_fixed_schema_and_null_intent() {
    for (event, event_type) in [
        (
            DatabaseDdlEvent::create_lifecycle(),
            CREATE_DATABASE_EVENT_TYPE,
        ),
        (
            DatabaseDdlEvent::alter_lifecycle(),
            ALTER_DATABASE_EVENT_TYPE,
        ),
        (DatabaseDdlEvent::drop_lifecycle(), DROP_DATABASE_EVENT_TYPE),
    ] {
        assert_event_locator(&event, event_type, None, None);
        assert_eq!(event.json_payload().unwrap(), serde_json::Value::Null);
    }

    assert_eq!(
        DatabaseDdlEvent::create_lifecycle().extra_schema(),
        DatabaseDdlEvent::create_submitted("c", "s", false, &HashMap::new()).extra_schema()
    );
}

#[test]
fn test_database_events_preserve_procedure_envelope_contract() {
    let procedure_id = ProcedureId::parse_str("00000000-0000-0000-0000-000000000001").unwrap();
    let submitted = ProcedureEvent::new(
        procedure_id,
        Box::new(DatabaseDdlEvent::create_submitted(
            "greptime",
            "metrics",
            false,
            &HashMap::new(),
        )),
        ProcedureState::Running,
        EventTrigger::Submitted,
    );
    let lifecycle = ProcedureEvent::new(
        procedure_id,
        Box::new(DatabaseDdlEvent::create_lifecycle()),
        ProcedureState::Done { output: None },
        EventTrigger::Succeeded,
    );

    assert_procedure_event_contract(
        &submitted,
        CREATE_DATABASE_EVENT_TYPE,
        "Running",
        "Submitted",
        Some("greptime"),
        Some("metrics"),
    );
    assert_procedure_event_contract(
        &lifecycle,
        CREATE_DATABASE_EVENT_TYPE,
        "Done",
        "Succeeded",
        None,
        None,
    );
    assert_eq!(lifecycle.json_payload().unwrap(), serde_json::Value::Null);
}

#[test]
fn test_create_database_event_filter() {
    let procedure = CreateDatabaseProcedure::new(
        "greptime".to_string(),
        "metrics".to_string(),
        false,
        HashMap::new(),
        None,
        new_ddl_context(Arc::new(MockDatanodeManager::new(()))),
    );

    assert_event_filter(&procedure, CREATE_DATABASE_EVENT_TYPE);
}

#[test]
fn test_alter_database_event_filter() {
    let procedure = AlterDatabaseProcedure::new(
        crate::rpc::ddl::AlterDatabaseTask {
            alter_expr: AlterDatabaseExpr {
                catalog_name: "greptime".to_string(),
                schema_name: "metrics".to_string(),
                kind: Some(PbAlterDatabaseKind::SetDatabaseOptions(
                    PbSetDatabaseOptions {
                        set_database_options: vec![],
                    },
                )),
            },
        },
        new_ddl_context(Arc::new(MockDatanodeManager::new(()))),
    )
    .unwrap();

    assert_event_filter(&procedure, ALTER_DATABASE_EVENT_TYPE);
}

#[test]
fn test_drop_database_event_filter() {
    let procedure = DropDatabaseProcedure::new(
        "greptime".to_string(),
        "metrics".to_string(),
        false,
        new_ddl_context(Arc::new(MockDatanodeManager::new(()))),
    );

    assert_event_filter(&procedure, DROP_DATABASE_EVENT_TYPE);
}

fn assert_event_locator(
    event: &DatabaseDdlEvent,
    event_type: &str,
    catalog_name: Option<&str>,
    schema_name: Option<&str>,
) {
    assert_event_contract(
        event,
        event_type,
        &[
            EVENT_TABLE_CATALOG_NAME_COLUMN.column_schema(),
            EVENT_TABLE_SCHEMA_NAME_COLUMN.column_schema(),
        ],
        &[Row {
            values: vec![
                Value {
                    value_data: catalog_name.map(|value| ValueData::StringValue(value.to_string())),
                },
                Value {
                    value_data: schema_name.map(|value| ValueData::StringValue(value.to_string())),
                },
            ],
        }],
    );
}

fn assert_procedure_event_contract(
    event: &ProcedureEvent,
    event_type: &str,
    procedure_state: &str,
    procedure_trigger: &str,
    catalog_name: Option<&str>,
    schema_name: Option<&str>,
) {
    assert_event_contract(
        event,
        event_type,
        &[
            EVENT_TABLE_PROCEDURE_ID_COLUMN.column_schema(),
            EVENT_TABLE_PROCEDURE_STATE_COLUMN.column_schema(),
            EVENT_TABLE_PROCEDURE_ERROR_COLUMN.column_schema(),
            EVENT_TABLE_PROCEDURE_TRIGGER_COLUMN.column_schema(),
            EVENT_TABLE_CATALOG_NAME_COLUMN.column_schema(),
            EVENT_TABLE_SCHEMA_NAME_COLUMN.column_schema(),
        ],
        &[Row {
            values: vec![
                Value {
                    value_data: Some(ValueData::StringValue(
                        "00000000-0000-0000-0000-000000000001".to_string(),
                    )),
                },
                Value {
                    value_data: Some(ValueData::StringValue(procedure_state.to_string())),
                },
                Value {
                    value_data: Some(ValueData::StringValue(String::new())),
                },
                jsonb_value(&serde_json::json!({"type": procedure_trigger})),
                Value {
                    value_data: catalog_name.map(|value| ValueData::StringValue(value.to_string())),
                },
                Value {
                    value_data: schema_name.map(|value| ValueData::StringValue(value.to_string())),
                },
            ],
        }],
    );
}
