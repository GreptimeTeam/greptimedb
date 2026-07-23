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

use api::v1::value::ValueData;
use common_error::ext::ErrorExt;
use common_error::status_code::StatusCode;
use common_event_recorder::Event;
use common_procedure::{
    ChildSubmissionOutcome, EventContext, EventTrigger, Procedure, ProcedureId, ProcedureState,
    RetryPhase,
};
use common_procedure_test::execute_procedure_until_done;
use store_api::storage::TableId;

use crate::ddl::drop_view::{DropViewProcedure, DropViewState};
use crate::ddl::test_util::create_table::test_create_table_task;
use crate::ddl::tests::create_view::{test_create_view_task, test_table_names};
use crate::ddl::view_event::{
    CATALOG_NAME_COLUMN, DROP_VIEW_EVENT_TYPE, SCHEMA_NAME_COLUMN, VIEW_ID_COLUMN, VIEW_NAME_COLUMN,
};
use crate::key::table_route::TableRouteValue;
use crate::rpc::ddl::DropViewTask;
use crate::test_util::{MockDatanodeManager, new_ddl_context};

fn new_drop_view_task(view: &str, view_id: TableId, drop_if_exists: bool) -> DropViewTask {
    DropViewTask {
        catalog: "greptime".to_string(),
        schema: "public".to_string(),
        view: view.to_string(),
        view_id,
        drop_if_exists,
    }
}

fn event_for(procedure: &DropViewProcedure, trigger: EventTrigger) -> Box<dyn Event> {
    let state = ProcedureState::Running;
    procedure
        .event(&EventContext {
            procedure_id: ProcedureId::random(),
            lifecycle_state: &state,
            trigger,
        })
        .unwrap()
}

#[test]
fn test_drop_view_event_submission() {
    let node_manager = Arc::new(MockDatanodeManager::new(()));
    let procedure = DropViewProcedure::new(
        new_drop_view_task("view_name", 42, true),
        new_ddl_context(node_manager),
    );
    let event = event_for(&procedure, EventTrigger::Submitted);

    assert_eq!(event.event_type(), DROP_VIEW_EVENT_TYPE);
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

    let row = event.extra_rows().unwrap().remove(0);
    assert_eq!(
        row.values,
        vec![
            ValueData::StringValue("greptime".to_string()).into(),
            ValueData::StringValue("public".to_string()).into(),
            ValueData::StringValue("view_name".to_string()).into(),
            ValueData::U32Value(42).into(),
        ]
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

    let expected_schema = vec![
        (CATALOG_NAME_COLUMN, api::v1::ColumnDataType::String),
        (SCHEMA_NAME_COLUMN, api::v1::ColumnDataType::String),
        (VIEW_NAME_COLUMN, api::v1::ColumnDataType::String),
        (VIEW_ID_COLUMN, api::v1::ColumnDataType::Uint32),
    ];
    let submitted_schema = submitted.extra_schema();
    assert_eq!(
        submitted_schema
            .iter()
            .map(|column| (column.column_name.as_str(), column.datatype))
            .collect::<Vec<_>>(),
        expected_schema
            .iter()
            .map(|(name, datatype)| (*name, i32::from(*datatype)))
            .collect::<Vec<_>>()
    );
    assert!(
        submitted_schema
            .iter()
            .all(|column| column.semantic_type == api::v1::SemanticType::Field as i32)
    );

    for trigger in triggers {
        let event = event_for(&procedure, trigger);
        assert_eq!(event.event_type(), DROP_VIEW_EVENT_TYPE);
        assert_eq!(event.extra_schema(), submitted_schema);
        assert_eq!(event.json_payload().unwrap(), serde_json::Value::Null);
        let row = event.extra_rows().unwrap().remove(0);
        assert!(row.values.iter().all(|value| value.value_data.is_none()));
    }
}

#[tokio::test]
async fn test_on_prepare_view_not_exists_err() {
    let node_manager = Arc::new(MockDatanodeManager::new(()));
    let ddl_context = new_ddl_context(node_manager);
    let view_id = 1024;
    let mut task = test_create_view_task("foo");
    task.view_info.ident.table_id = view_id;

    ddl_context
        .table_metadata_manager
        .create_view_metadata(
            task.view_info.clone(),
            task.create_view.logical_plan.clone(),
            test_table_names(),
            vec!["a".to_string()],
            vec!["number".to_string()],
            "the definition".to_string(),
        )
        .await
        .unwrap();

    let task = new_drop_view_task("bar", view_id, false);
    let mut procedure = DropViewProcedure::new(task, ddl_context);
    let err = procedure.on_prepare().await.unwrap_err();
    assert_eq!(err.status_code(), StatusCode::TableNotFound);
}

#[tokio::test]
async fn test_on_prepare_not_view_err() {
    let node_manager = Arc::new(MockDatanodeManager::new(()));
    let ddl_context = new_ddl_context(node_manager);
    let view_id = 1024;
    let view_name = "foo";
    let task = test_create_table_task(view_name, view_id);
    // Create a table, not a view.
    ddl_context
        .table_metadata_manager
        .create_table_metadata(
            task.table_info.clone(),
            TableRouteValue::physical(vec![]),
            HashMap::new(),
        )
        .await
        .unwrap();

    let task = new_drop_view_task(view_name, view_id, false);
    let mut procedure = DropViewProcedure::new(task, ddl_context);
    // It's not a view, expect error
    let err = procedure.on_prepare().await.unwrap_err();
    assert_eq!(err.status_code(), StatusCode::InvalidArguments);
}

#[tokio::test]
async fn test_on_prepare_success() {
    let node_manager = Arc::new(MockDatanodeManager::new(()));
    let ddl_context = new_ddl_context(node_manager);
    let view_id = 1024;
    let view_name = "foo";
    let mut task = test_create_view_task("foo");
    task.view_info.ident.table_id = view_id;

    ddl_context
        .table_metadata_manager
        .create_view_metadata(
            task.view_info.clone(),
            task.create_view.logical_plan.clone(),
            test_table_names(),
            vec!["a".to_string()],
            vec!["number".to_string()],
            "the definition".to_string(),
        )
        .await
        .unwrap();

    let task = new_drop_view_task("bar", view_id, true);
    // Drop if exists
    let mut procedure = DropViewProcedure::new(task, ddl_context.clone());
    procedure.on_prepare().await.unwrap();

    let task = new_drop_view_task(view_name, view_id, false);
    // Prepare success
    let mut procedure = DropViewProcedure::new(task, ddl_context);
    procedure.on_prepare().await.unwrap();
    assert_eq!(DropViewState::DeleteMetadata, procedure.state());
}

#[tokio::test]
async fn test_drop_view_success() {
    let node_manager = Arc::new(MockDatanodeManager::new(()));
    let ddl_context = new_ddl_context(node_manager);
    let view_id = 1024;
    let view_name = "foo";
    let mut task = test_create_view_task("foo");
    task.view_info.ident.table_id = view_id;

    ddl_context
        .table_metadata_manager
        .create_view_metadata(
            task.view_info.clone(),
            task.create_view.logical_plan.clone(),
            test_table_names(),
            vec!["a".to_string()],
            vec!["number".to_string()],
            "the definition".to_string(),
        )
        .await
        .unwrap();

    assert!(
        ddl_context
            .table_metadata_manager
            .view_info_manager()
            .get(view_id)
            .await
            .unwrap()
            .is_some()
    );

    let task = new_drop_view_task(view_name, view_id, false);
    // Prepare success
    let mut procedure = DropViewProcedure::new(task, ddl_context.clone());
    execute_procedure_until_done(&mut procedure).await;
    assert_eq!(DropViewState::InvalidateViewCache, procedure.state());

    // Assert view info is removed
    assert!(
        ddl_context
            .table_metadata_manager
            .view_info_manager()
            .get(view_id)
            .await
            .unwrap()
            .is_none()
    );

    // Drop again
    let task = new_drop_view_task(view_name, view_id, false);
    let mut procedure = DropViewProcedure::new(task, ddl_context);
    let err = procedure.on_prepare().await.unwrap_err();
    assert_eq!(err.status_code(), StatusCode::TableNotFound);
}
