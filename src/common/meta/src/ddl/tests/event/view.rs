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
use common_event_recorder::event_table::{CATALOG_NAME_COLUMN, SCHEMA_NAME_COLUMN};
use common_event_recorder::testing::assert_event_contract;
use common_event_recorder::{Event, EventTypeFilter};
use common_procedure::{
    ChildSubmissionOutcome, EventContext, EventTrigger, Procedure, ProcedureEvent, ProcedureId,
    ProcedureState, RetryPhase,
};

use crate::ddl::create_view::CreateViewProcedure;
use crate::ddl::drop_view::DropViewProcedure;
use crate::ddl::event::view::{CREATE_VIEW_EVENT_TYPE, DROP_VIEW_EVENT_TYPE};
use crate::ddl::tests::create_view::test_create_view_task;
use crate::ddl::tests::drop_view::new_drop_view_task;
use crate::test_util::{MockDatanodeManager, new_ddl_context};

fn view_event_schema() -> Vec<ColumnSchema> {
    common_event_recorder::event_table::column_schemas([
        &CATALOG_NAME_COLUMN,
        &SCHEMA_NAME_COLUMN,
        &common_event_recorder::event_table::VIEW_NAME_COLUMN,
        &common_event_recorder::event_table::VIEW_ID_COLUMN,
    ])
}

fn event_for(procedure: &DropViewProcedure, trigger: EventTrigger) -> Box<dyn Event> {
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
        &view_event_schema(),
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
    let event = event_for(&procedure, EventTrigger::Submitted);

    assert_event_contract(
        event.as_ref(),
        DROP_VIEW_EVENT_TYPE,
        &view_event_schema(),
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
    let submitted = event_for(&procedure, EventTrigger::Submitted);
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
    assert_eq!(submitted_schema, view_event_schema());

    for trigger in triggers {
        let event = event_for(&procedure, trigger);
        assert_eq!(event.event_type(), DROP_VIEW_EVENT_TYPE);
        assert_eq!(event.extra_schema(), submitted_schema);
        assert_eq!(event.json_payload().unwrap(), serde_json::Value::Null);
        let row = event.extra_rows().unwrap().remove(0);
        assert!(row.values.iter().all(|value| value.value_data.is_none()));
    }
}

#[test]
fn test_view_event_filter() {
    let context = new_ddl_context(Arc::new(MockDatanodeManager::new(())));
    let create = CreateViewProcedure::new(test_create_view_task("view_name"), context.clone());
    let drop = DropViewProcedure::new(new_drop_view_task("view_name", 42, false), context);
    let state = ProcedureState::Running;

    for (procedure, event_type) in [
        (&create as &dyn Procedure, CREATE_VIEW_EVENT_TYPE),
        (&drop as &dyn Procedure, DROP_VIEW_EVENT_TYPE),
    ] {
        let context = |event_type_filter| EventContext {
            procedure_id: ProcedureId::random(),
            lifecycle_state: &state,
            trigger: EventTrigger::Submitted,
            event_type_filter: Arc::new(event_type_filter),
        };

        assert!(
            procedure
                .event(&context(EventTypeFilter::Only(
                    std::collections::HashSet::from([event_type.to_string(),])
                )))
                .is_some()
        );
        assert!(
            procedure
                .event(&context(EventTypeFilter::Only(
                    std::collections::HashSet::from(["other_event".to_string(),])
                )))
                .is_none()
        );
    }
}

#[test]
fn test_view_event_procedure_envelope_contract() {
    let procedure_id = ProcedureId::parse_str("00000000-0000-0000-0000-000000000001").unwrap();
    let event = ProcedureEvent::new(
        procedure_id,
        Box::new(crate::ddl::event::view::ViewDdlEvent::create_submitted(
            "greptime",
            "public",
            "view_name",
            false,
            false,
            1,
            1,
        )),
        ProcedureState::Running,
        EventTrigger::Submitted,
    );

    let mut expected_schema = common_event_recorder::event_table::procedure_event_column_schemas();
    expected_schema.extend(view_event_schema());
    assert_event_contract(
        &event,
        CREATE_VIEW_EVENT_TYPE,
        &expected_schema,
        &[Row {
            values: vec![
                Value {
                    value_data: Some(ValueData::StringValue(procedure_id.to_string())),
                },
                Value {
                    value_data: Some(ValueData::StringValue("Running".to_string())),
                },
                Value {
                    value_data: Some(ValueData::StringValue(String::new())),
                },
                Value {
                    value_data: Some(ValueData::StringValue("Submitted".to_string())),
                },
                Value {
                    value_data: Some(ValueData::StringValue("greptime".to_string())),
                },
                Value {
                    value_data: Some(ValueData::StringValue("public".to_string())),
                },
                Value {
                    value_data: Some(ValueData::StringValue("view_name".to_string())),
                },
                Value { value_data: None },
            ],
        }],
    );
}
