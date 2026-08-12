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

use std::sync::Arc;

use api::v1::value::ValueData;
use api::v1::{ColumnSchema, Row, Value};
use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
use common_event_recorder::Event;
use common_event_recorder::event_table::{
    ACTOR_COLUMN, CATALOG_NAME_COLUMN, EVENT_CONTEXT_COLUMN, FLOW_ID_COLUMN, FLOW_NAME_COLUMN,
    PROCEDURE_ERROR_COLUMN, PROCEDURE_ID_COLUMN, PROCEDURE_STATE_COLUMN, PROCEDURE_TRIGGER_COLUMN,
    jsonb_value,
};
use common_event_recorder::testing::assert_event_contract;
use common_procedure::{EventTrigger, ProcedureEvent, ProcedureId, ProcedureState};
use table::table_name::TableName;

use super::test_util::assert_event_filter;
use crate::ddl::create_flow::CreateFlowProcedure;
use crate::ddl::drop_flow::DropFlowProcedure;
use crate::ddl::event::flow::{
    CREATE_FLOW_EVENT_TYPE, CreateFlowEventIntent, DROP_FLOW_EVENT_TYPE, FlowDdlEvent,
};
use crate::ddl::test_util::flownode_handler::NaiveFlownodeHandler;
use crate::ddl::tests::create_flow::{test_create_flow_task, test_query_context};
use crate::ddl::tests::drop_flow::test_drop_flow_task;
use crate::test_util::{MockFlownodeManager, new_ddl_context};

#[test]
fn test_flow_submitted_event_contracts() {
    let create = FlowDdlEvent::create_submitted(
        "greptime",
        "metrics",
        CreateFlowEventIntent {
            or_replace: true,
            create_if_not_exists: false,
            expire_after: Some(300),
            eval_interval_secs: Some(60),
        },
    );
    assert_event_contract(
        &create,
        CREATE_FLOW_EVENT_TYPE,
        &flow_schema(),
        &[Row {
            values: vec![
                ValueData::StringValue("greptime".to_string()).into(),
                ValueData::StringValue("metrics".to_string()).into(),
                Value { value_data: None },
            ],
        }],
    );
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

    let drop = FlowDdlEvent::drop_submitted("greptime", "metrics", 42, true);
    assert_event_contract(
        &drop,
        DROP_FLOW_EVENT_TYPE,
        &flow_schema(),
        &[Row {
            values: vec![
                ValueData::StringValue("greptime".to_string()).into(),
                ValueData::StringValue("metrics".to_string()).into(),
                ValueData::U32Value(42).into(),
            ],
        }],
    );
    assert_eq!(
        drop.json_payload().unwrap(),
        serde_json::json!({"version": 1, "drop_if_exists": true})
    );
}

#[test]
fn test_flow_lifecycle_events_have_fixed_schema_and_null_intent() {
    for (event, event_type) in [
        (
            FlowDdlEvent::create_lifecycle("greptime", "metrics"),
            CREATE_FLOW_EVENT_TYPE,
        ),
        (
            FlowDdlEvent::create_succeeded("greptime", "metrics", None),
            CREATE_FLOW_EVENT_TYPE,
        ),
        (
            FlowDdlEvent::drop_lifecycle("greptime", "metrics", 42),
            DROP_FLOW_EVENT_TYPE,
        ),
    ] {
        assert_event_contract(
            &event,
            event_type,
            &flow_schema(),
            &[Row {
                values: vec![
                    ValueData::StringValue("greptime".to_string()).into(),
                    ValueData::StringValue("metrics".to_string()).into(),
                    if event_type == DROP_FLOW_EVENT_TYPE {
                        ValueData::U32Value(42).into()
                    } else {
                        Value { value_data: None }
                    },
                ],
            }],
        );
        assert_eq!(event.json_payload().unwrap(), serde_json::Value::Null);
    }

    let event = FlowDdlEvent::create_succeeded("greptime", "metrics", Some(42));
    assert_event_contract(
        &event,
        CREATE_FLOW_EVENT_TYPE,
        &flow_schema(),
        &[Row {
            values: vec![
                ValueData::StringValue("greptime".to_string()).into(),
                ValueData::StringValue("metrics".to_string()).into(),
                ValueData::U32Value(42).into(),
            ],
        }],
    );
}

#[test]
fn test_flow_events_preserve_procedure_envelope_contract() {
    let procedure_id = ProcedureId::parse_str("00000000-0000-0000-0000-000000000001").unwrap();
    let submitted = ProcedureEvent::new(
        procedure_id,
        Box::new(FlowDdlEvent::create_submitted(
            "greptime",
            "metrics",
            CreateFlowEventIntent {
                or_replace: false,
                create_if_not_exists: false,
                expire_after: None,
                eval_interval_secs: None,
            },
        )),
        ProcedureState::Running,
        EventTrigger::Submitted,
    );
    let succeeded = ProcedureEvent::new(
        procedure_id,
        Box::new(FlowDdlEvent::create_succeeded(
            "greptime",
            "metrics",
            Some(42),
        )),
        ProcedureState::Done { output: None },
        EventTrigger::Succeeded,
    );

    assert_procedure_event_contract(
        &submitted,
        CREATE_FLOW_EVENT_TYPE,
        "Running",
        "Submitted",
        FlowEventLocator {
            catalog_name: Some("greptime"),
            flow_name: Some("metrics"),
            flow_id: None,
        },
    );
    assert_procedure_event_contract(
        &succeeded,
        CREATE_FLOW_EVENT_TYPE,
        "Done",
        "Succeeded",
        FlowEventLocator {
            catalog_name: Some("greptime"),
            flow_name: Some("metrics"),
            flow_id: Some(42),
        },
    );
}

#[test]
fn test_create_flow_event_filter() {
    let procedure = CreateFlowProcedure::new(
        test_create_flow_task(
            "flow",
            vec![],
            TableName::new(DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, "sink"),
            false,
        ),
        test_query_context(),
        new_ddl_context(Arc::new(MockFlownodeManager::new(NaiveFlownodeHandler))),
    );
    assert_event_filter(&procedure, CREATE_FLOW_EVENT_TYPE);
}

#[test]
fn test_drop_flow_event_filter() {
    let procedure = DropFlowProcedure::new(
        test_drop_flow_task("flow", 42, false),
        new_ddl_context(Arc::new(MockFlownodeManager::new(NaiveFlownodeHandler))),
    );
    assert_event_filter(&procedure, DROP_FLOW_EVENT_TYPE);
}

fn flow_schema() -> Vec<ColumnSchema> {
    vec![
        CATALOG_NAME_COLUMN.column_schema(),
        FLOW_NAME_COLUMN.column_schema(),
        FLOW_ID_COLUMN.column_schema(),
    ]
}

struct FlowEventLocator<'a> {
    catalog_name: Option<&'a str>,
    flow_name: Option<&'a str>,
    flow_id: Option<u32>,
}

fn assert_procedure_event_contract(
    event: &ProcedureEvent,
    event_type: &str,
    state: &str,
    trigger: &str,
    locator: FlowEventLocator<'_>,
) {
    let mut schema = vec![
        ACTOR_COLUMN.column_schema(),
        PROCEDURE_ID_COLUMN.column_schema(),
        PROCEDURE_STATE_COLUMN.column_schema(),
        PROCEDURE_ERROR_COLUMN.column_schema(),
        PROCEDURE_TRIGGER_COLUMN.column_schema(),
    ];
    schema.extend(flow_schema());
    schema.push(EVENT_CONTEXT_COLUMN.column_schema());
    assert_event_contract(
        event,
        event_type,
        &schema,
        &[Row {
            values: vec![
                Value { value_data: None },
                ValueData::StringValue(event.procedure_id.to_string()).into(),
                ValueData::StringValue(state.to_string()).into(),
                ValueData::StringValue(String::new()).into(),
                jsonb_value(&serde_json::json!({"type": trigger})),
                optional_string(locator.catalog_name),
                optional_string(locator.flow_name),
                locator
                    .flow_id
                    .map(ValueData::U32Value)
                    .map(Into::into)
                    .unwrap_or(Value { value_data: None }),
                Value { value_data: None },
            ],
        }],
    );
}

fn optional_string(value: Option<&str>) -> Value {
    value
        .map(|value| ValueData::StringValue(value.to_string()).into())
        .unwrap_or(Value { value_data: None })
}
