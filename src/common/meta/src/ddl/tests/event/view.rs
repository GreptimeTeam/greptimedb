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
use common_event_recorder::event_table::{
    CATALOG_NAME_COLUMN, PROCEDURE_ERROR_COLUMN, PROCEDURE_ID_COLUMN, PROCEDURE_STATE_COLUMN,
    PROCEDURE_TRIGGER_COLUMN, SCHEMA_NAME_COLUMN, VIEW_ID_COLUMN, VIEW_NAME_COLUMN,
};
use common_event_recorder::testing::assert_event_contract;
use common_event_recorder::{Event, EventTypeFilter};
use common_procedure::{
    ChildSubmissionOutcome, EventContext, EventTrigger, Output, Procedure, ProcedureEvent,
    ProcedureId, ProcedureState, RetryPhase,
};

use super::test_util::assert_event_filter;
use crate::ddl::create_view::CreateViewProcedure;
use crate::ddl::drop_view::DropViewProcedure;
use crate::ddl::event::view::{
    CREATE_VIEW_EVENT_TYPE, CreateViewEventIntent, DROP_VIEW_EVENT_TYPE, ViewDdlEvent,
};
use crate::ddl::tests::create_view::test_create_view_task;
use crate::ddl::tests::drop_view::new_drop_view_task;
use crate::test_util::{MockDatanodeManager, new_ddl_context};

#[test]
fn test_view_submitted_event_contracts() {
    let mut task = test_create_view_task("v_metrics");
    task.create_view.or_replace = true;
    task.create_view.create_if_not_exists = true;
    let create = CreateViewProcedure::new(task, test_context());
    let event = event_for(&create, EventTrigger::Submitted);

    assert_view_event_contract(
        event.as_ref(),
        CREATE_VIEW_EVENT_TYPE,
        ViewEventLocator {
            catalog_name: Some("greptime"),
            schema_name: Some("public"),
            view_name: Some("v_metrics"),
            view_id: None,
        },
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
    for omitted in ["CREATE VIEW", "SELECT", "a_table", "b_table"] {
        assert!(!payload.contains(omitted));
    }

    let drop = DropViewProcedure::new(new_drop_view_task("view_name", 42, true), test_context());
    let event = event_for(&drop, EventTrigger::Submitted);

    assert_view_event_contract(
        event.as_ref(),
        DROP_VIEW_EVENT_TYPE,
        ViewEventLocator {
            catalog_name: Some("greptime"),
            schema_name: Some("public"),
            view_name: Some("view_name"),
            view_id: Some(42),
        },
    );
    assert_eq!(
        event.json_payload().unwrap(),
        serde_json::json!({"version": 1, "drop_if_exists": true})
    );
    let payload = event.json_payload().unwrap().to_string();
    for omitted in ["Prepare", "view_name"] {
        assert!(!payload.contains(omitted));
    }
}

#[test]
fn test_view_lifecycle_event_contracts() {
    for (event, event_type) in [
        (ViewDdlEvent::create_lifecycle(), CREATE_VIEW_EVENT_TYPE),
        (ViewDdlEvent::drop_lifecycle(), DROP_VIEW_EVENT_TYPE),
    ] {
        assert_lightweight_event(&event, event_type);
    }

    let event = ViewDdlEvent::create_succeeded(84);
    assert_view_event_contract(
        &event,
        CREATE_VIEW_EVENT_TYPE,
        ViewEventLocator {
            view_id: Some(84),
            ..Default::default()
        },
    );
    assert_eq!(event.json_payload().unwrap(), serde_json::Value::Null);
}

#[test]
fn test_view_procedures_emit_lightweight_lifecycle_events() {
    let create = CreateViewProcedure::new(test_create_view_task("view_name"), test_context());
    let drop = DropViewProcedure::new(new_drop_view_task("view_name", 42, false), test_context());
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
        EventTrigger::Failed,
        EventTrigger::Poisoned,
    ];

    for (procedure, event_type) in [
        (&create as &dyn Procedure, CREATE_VIEW_EVENT_TYPE),
        (&drop as &dyn Procedure, DROP_VIEW_EVENT_TYPE),
    ] {
        for trigger in &triggers {
            let event = event_for(procedure, trigger.clone());
            assert_lightweight_event(event.as_ref(), event_type);
        }
    }

    let event = event_for(&drop, EventTrigger::Succeeded);
    assert_lightweight_event(event.as_ref(), DROP_VIEW_EVENT_TYPE);
}

#[test]
fn test_create_view_succeeded_output_mapping() {
    let procedure = CreateViewProcedure::new(test_create_view_task("view_name"), test_context());
    let state = ProcedureState::Done {
        output: Some(Arc::new(84_u32)),
    };
    let event = event_for_state(&procedure, EventTrigger::Succeeded, &state);

    assert_view_event_contract(
        event.as_ref(),
        CREATE_VIEW_EVENT_TYPE,
        ViewEventLocator {
            view_id: Some(84),
            ..Default::default()
        },
    );
    assert_eq!(event.json_payload().unwrap(), serde_json::Value::Null);

    let invalid_outputs: [Option<Output>; 2] = [None, Some(Arc::new("not a table id".to_string()))];
    for output in invalid_outputs {
        let state = ProcedureState::Done { output };
        let event = event_for_state(&procedure, EventTrigger::Succeeded, &state);
        assert_lightweight_event(event.as_ref(), CREATE_VIEW_EVENT_TYPE);
    }
}

#[test]
fn test_create_view_event_filter() {
    let procedure = CreateViewProcedure::new(test_create_view_task("view_name"), test_context());
    assert_event_filter(&procedure, CREATE_VIEW_EVENT_TYPE);
}

#[test]
fn test_drop_view_event_filter() {
    let procedure =
        DropViewProcedure::new(new_drop_view_task("view_name", 42, false), test_context());
    assert_event_filter(&procedure, DROP_VIEW_EVENT_TYPE);
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
            view_id: Some(42),
            ..Default::default()
        },
    );
}

#[derive(Default)]
struct ViewEventLocator<'a> {
    catalog_name: Option<&'a str>,
    schema_name: Option<&'a str>,
    view_name: Option<&'a str>,
    view_id: Option<u32>,
}

impl ViewEventLocator<'_> {
    fn values(&self) -> Vec<Value> {
        vec![
            optional_string(self.catalog_name),
            optional_string(self.schema_name),
            optional_string(self.view_name),
            self.view_id
                .map(ValueData::U32Value)
                .map(Into::into)
                .unwrap_or_default(),
        ]
    }
}

fn view_schema() -> Vec<ColumnSchema> {
    vec![
        CATALOG_NAME_COLUMN.column_schema(),
        SCHEMA_NAME_COLUMN.column_schema(),
        VIEW_NAME_COLUMN.column_schema(),
        VIEW_ID_COLUMN.column_schema(),
    ]
}

fn assert_view_event_contract(event: &dyn Event, event_type: &str, locator: ViewEventLocator<'_>) {
    assert_event_contract(
        event,
        event_type,
        &view_schema(),
        &[Row {
            values: locator.values(),
        }],
    );
}

fn assert_lightweight_event(event: &dyn Event, event_type: &str) {
    assert_view_event_contract(event, event_type, ViewEventLocator::default());
    assert_eq!(event.json_payload().unwrap(), serde_json::Value::Null);
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

    let mut values = vec![
        ValueData::StringValue(event.procedure_id.to_string()).into(),
        ValueData::StringValue(state.to_string()).into(),
        ValueData::StringValue(String::new()).into(),
        ValueData::StringValue(trigger.to_string()).into(),
    ];
    values.extend(locator.values());

    assert_event_contract(event, event_type, &schema, &[Row { values }]);
}

fn test_context() -> crate::ddl::DdlContext {
    new_ddl_context(Arc::new(MockDatanodeManager::new(())))
}

fn event_for(procedure: &dyn Procedure, trigger: EventTrigger) -> Box<dyn Event> {
    event_for_state(procedure, trigger, &ProcedureState::Running)
}

fn event_for_state(
    procedure: &dyn Procedure,
    trigger: EventTrigger,
    lifecycle_state: &ProcedureState,
) -> Box<dyn Event> {
    procedure
        .event(&EventContext {
            procedure_id: ProcedureId::random(),
            lifecycle_state,
            trigger,
            event_type_filter: Arc::new(EventTypeFilter::All),
        })
        .unwrap()
}

fn optional_string(value: Option<&str>) -> Value {
    value
        .map(|value| ValueData::StringValue(value.to_string()).into())
        .unwrap_or_default()
}
