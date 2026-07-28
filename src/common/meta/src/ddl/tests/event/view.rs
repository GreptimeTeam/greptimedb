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

use std::collections::HashSet;
use std::sync::Arc;

use api::v1::value::ValueData;
use api::v1::{ColumnSchema, Row, Value};
use common_event_recorder::event_table::{
    CATALOG_NAME_COLUMN, PROCEDURE_ERROR_COLUMN, PROCEDURE_ID_COLUMN, PROCEDURE_STATE_COLUMN,
    PROCEDURE_TRIGGER_COLUMN, SCHEMA_NAME_COLUMN, VIEW_ID_COLUMN, VIEW_NAME_COLUMN,
};
use common_event_recorder::testing::assert_event_contract;
use common_event_recorder::{Event, EventTypeFilter};
use common_procedure::{
    ChildSubmissionOutcome, EventContext, EventTrigger, Procedure, ProcedureEvent, ProcedureId,
    ProcedureState, RetryPhase,
};

use crate::ddl::create_view::CreateViewProcedure;
use crate::ddl::drop_view::DropViewProcedure;
use crate::ddl::event::view::{
    CREATE_VIEW_EVENT_TYPE, CreateViewEventIntent, DROP_VIEW_EVENT_TYPE, ViewDdlEvent,
};
use crate::ddl::tests::create_view::test_create_view_task;
use crate::ddl::tests::drop_view::new_drop_view_task;
use crate::test_util::{MockDatanodeManager, new_ddl_context};

fn view_schema() -> Vec<ColumnSchema> {
    vec![
        CATALOG_NAME_COLUMN.column_schema(),
        SCHEMA_NAME_COLUMN.column_schema(),
        VIEW_NAME_COLUMN.column_schema(),
        VIEW_ID_COLUMN.column_schema(),
    ]
}

fn drop_view_event(procedure: &DropViewProcedure, trigger: EventTrigger) -> Box<dyn Event> {
    let state = ProcedureState::Running;
    procedure
        .event(&EventContext {
            procedure_id: ProcedureId::random(),
            lifecycle_state: &state,
            trigger,
            event_type_filter: Arc::new(EventTypeFilter::All),
        })
        .unwrap()
}

#[test]
fn test_create_view_event_submitted() {
    let node_manager = Arc::new(MockDatanodeManager::new(()));
    let ddl_context = new_ddl_context(node_manager);
    let mut task = test_create_view_task("v_metrics");
    task.create_view.or_replace = true;
    task.create_view.create_if_not_exists = true;
    task.view_info.ident.table_id = 42;
    let procedure = CreateViewProcedure::new(task, ddl_context);
    let state = ProcedureState::Running;

    let event = procedure
        .event(&EventContext {
            procedure_id: ProcedureId::random(),
            lifecycle_state: &state,
            trigger: EventTrigger::Submitted,
            event_type_filter: Arc::new(EventTypeFilter::All),
        })
        .unwrap();

    assert_event_contract(
        event.as_ref(),
        CREATE_VIEW_EVENT_TYPE,
        &view_schema(),
        &[Row {
            values: vec![
                ValueData::StringValue("greptime".to_string()).into(),
                ValueData::StringValue("public".to_string()).into(),
                ValueData::StringValue("v_metrics".to_string()).into(),
                Default::default(),
            ],
        }],
    );
    assert_eq!(
        event.json_payload().unwrap(),
        serde_json::json!({
            "version": 1,
            "or_replace": true,
            "create_if_not_exists": true,
            "referenced_table_count": 2,
            "column_count": 1,
        })
    );
    let payload = event.json_payload().unwrap().to_string();
    assert!(!payload.contains("CREATE VIEW"));
    assert!(!payload.contains("SELECT"));
    assert!(!payload.contains("a_table"));
    assert!(!payload.contains("b_table"));
}

#[test]
fn test_create_view_event_lifecycle_rows_have_fixed_schema() {
    let node_manager = Arc::new(MockDatanodeManager::new(()));
    let ddl_context = new_ddl_context(node_manager);
    let mut task = test_create_view_task("v_metrics");
    task.view_info.ident.table_id = 42;
    let procedure = CreateViewProcedure::new(task, ddl_context);
    let state = ProcedureState::Running;
    let submitted = procedure
        .event(&EventContext {
            procedure_id: ProcedureId::random(),
            lifecycle_state: &state,
            trigger: EventTrigger::Submitted,
            event_type_filter: Arc::new(EventTypeFilter::All),
        })
        .unwrap();
    let expected_schema = submitted.extra_schema();
    let submitted_values = submitted.extra_rows().unwrap().remove(0).values;
    assert!(
        submitted_values[..3]
            .iter()
            .all(|value| value.value_data.is_some())
    );
    assert!(submitted_values[3].value_data.is_none());

    let lightweight_triggers = [
        EventTrigger::Recovered,
        EventTrigger::ChildSubmitted {
            procedure_id: ProcedureId::random(),
            outcome: ChildSubmissionOutcome::Accepted,
        },
        EventTrigger::Retrying {
            phase: RetryPhase::Execute,
            attempt: 1,
        },
        EventTrigger::RollingBack,
        EventTrigger::Failed,
        EventTrigger::Poisoned,
    ];

    for trigger in lightweight_triggers {
        let event = procedure
            .event(&EventContext {
                procedure_id: ProcedureId::random(),
                lifecycle_state: &state,
                trigger,
                event_type_filter: Arc::new(EventTypeFilter::All),
            })
            .unwrap();
        assert_eq!(event.extra_schema(), expected_schema);
        assert_eq!(event.json_payload().unwrap(), serde_json::Value::Null);
        assert!(
            event
                .extra_rows()
                .unwrap()
                .remove(0)
                .values
                .iter()
                .all(|value| value.value_data.is_none())
        );
    }

    let succeeded_state = ProcedureState::Done {
        output: Some(Arc::new(84_u32)),
    };
    let succeeded = procedure
        .event(&EventContext {
            procedure_id: ProcedureId::random(),
            lifecycle_state: &succeeded_state,
            trigger: EventTrigger::Succeeded,
            event_type_filter: Arc::new(EventTypeFilter::All),
        })
        .unwrap();
    assert_eq!(succeeded.extra_schema(), expected_schema);
    assert_eq!(succeeded.json_payload().unwrap(), serde_json::Value::Null);
    let values = succeeded.extra_rows().unwrap().remove(0).values;
    assert!(values[..3].iter().all(|value| value.value_data.is_none()));
    assert_eq!(values[3].value_data, Some(ValueData::U32Value(84)));
}

#[test]
fn test_create_view_event_succeeded_without_output_is_lifecycle() {
    let node_manager = Arc::new(MockDatanodeManager::new(()));
    let mut task = test_create_view_task("v_metrics");
    task.view_info.ident.table_id = 42;
    let procedure = CreateViewProcedure::new(task, new_ddl_context(node_manager));
    let state = ProcedureState::Done { output: None };

    let event = procedure
        .event(&EventContext {
            procedure_id: ProcedureId::random(),
            lifecycle_state: &state,
            trigger: EventTrigger::Succeeded,
            event_type_filter: Arc::new(EventTypeFilter::All),
        })
        .unwrap();

    assert_eq!(event.event_type(), CREATE_VIEW_EVENT_TYPE);
    assert_eq!(event.json_payload().unwrap(), serde_json::Value::Null);
    assert!(
        event
            .extra_rows()
            .unwrap()
            .remove(0)
            .values
            .iter()
            .all(|value| value.value_data.is_none())
    );
}

#[test]
fn test_create_view_event_succeeded_with_wrong_output_type_is_lifecycle() {
    let node_manager = Arc::new(MockDatanodeManager::new(()));
    let mut task = test_create_view_task("v_metrics");
    task.view_info.ident.table_id = 42;
    let procedure = CreateViewProcedure::new(task, new_ddl_context(node_manager));
    let state = ProcedureState::Done {
        output: Some(Arc::new("not a table id".to_string())),
    };

    let event = procedure
        .event(&EventContext {
            procedure_id: ProcedureId::random(),
            lifecycle_state: &state,
            trigger: EventTrigger::Succeeded,
            event_type_filter: Arc::new(EventTypeFilter::All),
        })
        .unwrap();

    assert_eq!(event.event_type(), CREATE_VIEW_EVENT_TYPE);
    assert_eq!(event.json_payload().unwrap(), serde_json::Value::Null);
    assert!(
        event
            .extra_rows()
            .unwrap()
            .remove(0)
            .values
            .iter()
            .all(|value| value.value_data.is_none())
    );
}

#[test]
fn test_drop_view_event_submission() {
    let node_manager = Arc::new(MockDatanodeManager::new(()));
    let procedure = DropViewProcedure::new(
        new_drop_view_task("view_name", 42, true),
        new_ddl_context(node_manager),
    );
    let event = drop_view_event(&procedure, EventTrigger::Submitted);

    assert_event_contract(
        event.as_ref(),
        DROP_VIEW_EVENT_TYPE,
        &view_schema(),
        &[Row {
            values: vec![
                ValueData::StringValue("greptime".to_string()).into(),
                ValueData::StringValue("public".to_string()).into(),
                ValueData::StringValue("view_name".to_string()).into(),
                ValueData::U32Value(42).into(),
            ],
        }],
    );
    assert_eq!(
        event.json_payload().unwrap(),
        serde_json::json!({"version": 1, "drop_if_exists": true})
    );
    assert!(
        !event
            .json_payload()
            .unwrap()
            .to_string()
            .contains("Prepare")
    );
    assert!(
        !event
            .json_payload()
            .unwrap()
            .to_string()
            .contains("view_name")
    );
}

#[test]
fn test_drop_view_event_lifecycle_rows_have_fixed_schema_and_nulls() {
    let node_manager = Arc::new(MockDatanodeManager::new(()));
    let procedure = DropViewProcedure::new(
        new_drop_view_task("view_name", 42, false),
        new_ddl_context(node_manager),
    );
    let submitted = drop_view_event(&procedure, EventTrigger::Submitted);
    let triggers = [
        EventTrigger::Recovered,
        EventTrigger::ChildSubmitted {
            procedure_id: ProcedureId::random(),
            outcome: ChildSubmissionOutcome::Accepted,
        },
        EventTrigger::Retrying {
            phase: RetryPhase::Execute,
            attempt: 1,
        },
        EventTrigger::RollingBack,
        EventTrigger::Succeeded,
        EventTrigger::Failed,
        EventTrigger::Poisoned,
    ];

    let submitted_schema = submitted.extra_schema();
    assert_eq!(submitted_schema, view_schema());

    for trigger in triggers {
        let event = drop_view_event(&procedure, trigger);
        assert_eq!(event.event_type(), DROP_VIEW_EVENT_TYPE);
        assert_eq!(event.extra_schema(), submitted_schema);
        assert_eq!(event.json_payload().unwrap(), serde_json::Value::Null);
        let row = event.extra_rows().unwrap().remove(0);
        assert!(row.values.iter().all(|value| value.value_data.is_none()));
    }
}

#[test]
fn test_create_view_event_filter() {
    let procedure = CreateViewProcedure::new(
        test_create_view_task("view_name"),
        new_ddl_context(Arc::new(MockDatanodeManager::new(()))),
    );

    assert_view_event_filter(&procedure, CREATE_VIEW_EVENT_TYPE);
}

#[test]
fn test_drop_view_event_filter() {
    let procedure = DropViewProcedure::new(
        new_drop_view_task("view_name", 42, false),
        new_ddl_context(Arc::new(MockDatanodeManager::new(()))),
    );

    assert_view_event_filter(&procedure, DROP_VIEW_EVENT_TYPE);
}

fn assert_view_event_filter(procedure: &dyn Procedure, event_type: &str) {
    let state = ProcedureState::Running;
    let event_context = |event_type_filter| EventContext {
        procedure_id: ProcedureId::random(),
        lifecycle_state: &state,
        trigger: EventTrigger::Submitted,
        event_type_filter: Arc::new(event_type_filter),
    };

    assert!(
        procedure
            .event(&event_context(EventTypeFilter::Only(HashSet::from([
                event_type.to_string()
            ]))))
            .is_some()
    );
    assert!(
        procedure
            .event(&event_context(EventTypeFilter::Only(HashSet::new())))
            .is_none()
    );
    assert!(
        procedure
            .event(&event_context(EventTypeFilter::Only(HashSet::from([
                "other_event".to_string(),
            ]))))
            .is_none()
    );
}

#[test]
fn test_view_event_procedure_envelope_contract() {
    let procedure_id = ProcedureId::parse_str("00000000-0000-0000-0000-000000000001").unwrap();
    let submitted = ProcedureEvent::new(
        procedure_id,
        Box::new(ViewDdlEvent::create_submitted(
            "greptime",
            "public",
            "view_name",
            CreateViewEventIntent {
                or_replace: false,
                create_if_not_exists: false,
                referenced_table_count: 1,
                column_count: 1,
            },
        )),
        ProcedureState::Running,
        EventTrigger::Submitted,
    );
    let succeeded = ProcedureEvent::new(
        procedure_id,
        Box::new(ViewDdlEvent::create_succeeded(42)),
        ProcedureState::Done { output: None },
        EventTrigger::Succeeded,
    );

    assert_procedure_event_contract(
        &submitted,
        CREATE_VIEW_EVENT_TYPE,
        "Running",
        "Submitted",
        ViewEventLocator {
            catalog_name: Some("greptime"),
            schema_name: Some("public"),
            view_name: Some("view_name"),
            view_id: None,
        },
    );
    assert_procedure_event_contract(
        &succeeded,
        CREATE_VIEW_EVENT_TYPE,
        "Done",
        "Succeeded",
        ViewEventLocator {
            catalog_name: None,
            schema_name: None,
            view_name: None,
            view_id: Some(42),
        },
    );
}

struct ViewEventLocator<'a> {
    catalog_name: Option<&'a str>,
    schema_name: Option<&'a str>,
    view_name: Option<&'a str>,
    view_id: Option<u32>,
}

fn assert_procedure_event_contract(
    event: &ProcedureEvent,
    event_type: &str,
    state: &str,
    trigger: &str,
    locator: ViewEventLocator<'_>,
) {
    let mut schema = vec![
        PROCEDURE_ID_COLUMN.column_schema(),
        PROCEDURE_STATE_COLUMN.column_schema(),
        PROCEDURE_ERROR_COLUMN.column_schema(),
        PROCEDURE_TRIGGER_COLUMN.column_schema(),
    ];
    schema.extend(view_schema());
    assert_event_contract(
        event,
        event_type,
        &schema,
        &[Row {
            values: vec![
                ValueData::StringValue(event.procedure_id.to_string()).into(),
                ValueData::StringValue(state.to_string()).into(),
                ValueData::StringValue(String::new()).into(),
                ValueData::StringValue(trigger.to_string()).into(),
                optional_string(locator.catalog_name),
                optional_string(locator.schema_name),
                optional_string(locator.view_name),
                locator
                    .view_id
                    .map(ValueData::U32Value)
                    .map(Into::into)
                    .unwrap_or(Value { value_data: None }),
            ],
        }],
    );
}

fn optional_string(value: Option<&str>) -> Value {
    value
        .map(|value| ValueData::StringValue(value.to_string()).into())
        .unwrap_or(Value { value_data: None })
}
