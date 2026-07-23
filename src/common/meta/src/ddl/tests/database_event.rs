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

use api::v1::Value;
use api::v1::value::ValueData;
use common_event_recorder::Event;

use crate::ddl::event::{
    ALTER_DATABASE_EVENT_TYPE, CATALOG_NAME_COLUMN, CREATE_DATABASE_EVENT_TYPE,
    DROP_DATABASE_EVENT_TYPE, DatabaseDdlEvent, SCHEMA_NAME_COLUMN,
};
use crate::rpc::ddl::{
    AlterDatabaseKind, SetDatabaseOption, SetDatabaseOptions, UnsetDatabaseOption,
    UnsetDatabaseOptions,
};

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
        assert_eq!(
            event.extra_rows().unwrap().remove(0).values,
            vec![Value { value_data: None }, Value { value_data: None }]
        );
    }

    assert_eq!(
        DatabaseDdlEvent::create_lifecycle().extra_schema(),
        DatabaseDdlEvent::create_submitted("c", "s", false, &HashMap::new()).extra_schema()
    );
}

fn assert_event_locator(
    event: &DatabaseDdlEvent,
    event_type: &str,
    catalog_name: Option<&str>,
    schema_name: Option<&str>,
) {
    assert_eq!(event.event_type(), event_type);
    let schema = event.extra_schema();
    assert_eq!(schema.len(), 2);
    assert_eq!(schema[0].column_name, CATALOG_NAME_COLUMN);
    assert_eq!(schema[1].column_name, SCHEMA_NAME_COLUMN);

    let row = event.extra_rows().unwrap().remove(0);
    assert_eq!(
        row.values[0].value_data,
        catalog_name.map(|value| ValueData::StringValue(value.to_string()))
    );
    assert_eq!(
        row.values[1].value_data,
        schema_name.map(|value| ValueData::StringValue(value.to_string()))
    );
}
