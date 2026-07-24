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

use std::assert_matches;
use std::collections::HashMap;
use std::sync::Arc;

use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
use common_procedure::{
    ChildSubmissionOutcome, EventContext, EventTrigger, Procedure, ProcedureId, ProcedureState,
    RetryPhase,
};
use common_procedure_test::execute_procedure_until_done;
use table::table_name::TableName;

use crate::ddl::drop_flow::DropFlowProcedure;
use crate::ddl::test_util::create_table::test_create_table_task;
use crate::ddl::test_util::flownode_handler::NaiveFlownodeHandler;
use crate::ddl::tests::create_flow::{create_test_flow, create_test_pending_flow};
use crate::error;
use crate::key::table_route::TableRouteValue;
use crate::rpc::ddl::DropFlowTask;
use crate::test_util::{MockFlownodeManager, new_ddl_context};

fn test_drop_flow_task(flow_name: &str, flow_id: u32, drop_if_exists: bool) -> DropFlowTask {
    DropFlowTask {
        catalog_name: DEFAULT_CATALOG_NAME.to_string(),
        flow_name: flow_name.to_string(),
        flow_id,
        drop_if_exists,
    }
}

fn event_for(
    procedure: &DropFlowProcedure,
    trigger: EventTrigger,
) -> Box<dyn common_event_recorder::Event> {
    procedure
        .event(&EventContext {
            procedure_id: ProcedureId::random(),
            lifecycle_state: &ProcedureState::Running,
            trigger,
        })
        .unwrap()
}

#[test]
fn test_drop_flow_submitted_event_contains_only_typed_intent() {
    let node_manager = Arc::new(MockFlownodeManager::new(NaiveFlownodeHandler));
    let ddl_context = new_ddl_context(node_manager);
    let procedure =
        DropFlowProcedure::new(test_drop_flow_task("event_flow", 42, true), ddl_context);

    let event = event_for(&procedure, EventTrigger::Submitted);

    assert_eq!(event.event_type(), "ddl_drop_flow");
    assert_eq!(
        event.json_payload().unwrap(),
        serde_json::json!({"version": 1, "drop_if_exists": true})
    );
    assert_eq!(
        event.extra_rows().unwrap()[0].values,
        vec![
            api::v1::value::ValueData::StringValue(DEFAULT_CATALOG_NAME.to_string()).into(),
            api::v1::Value { value_data: None },
            api::v1::value::ValueData::StringValue("event_flow".to_string()).into(),
            api::v1::value::ValueData::U32Value(42).into(),
        ]
    );
}

#[test]
fn test_drop_flow_lifecycle_events_are_lightweight() {
    let node_manager = Arc::new(MockFlownodeManager::new(NaiveFlownodeHandler));
    let ddl_context = new_ddl_context(node_manager);
    let procedure =
        DropFlowProcedure::new(test_drop_flow_task("event_flow", 42, false), ddl_context);
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

    for trigger in triggers {
        let event = event_for(&procedure, trigger);
        assert_eq!(event.event_type(), "ddl_drop_flow");
        assert_eq!(event.extra_schema(), submitted.extra_schema());
        assert_eq!(event.json_payload().unwrap(), serde_json::Value::Null);
        assert!(
            event.extra_rows().unwrap()[0]
                .values
                .iter()
                .all(|value| value.value_data.is_none())
        );
    }
}

#[tokio::test]
async fn test_drop_flow_not_found() {
    let flow_id = 1024;
    let node_manager = Arc::new(MockFlownodeManager::new(NaiveFlownodeHandler));
    let ddl_context = new_ddl_context(node_manager);
    let task = test_drop_flow_task("my_flow", flow_id, false);
    let mut procedure = DropFlowProcedure::new(task, ddl_context);
    let err = procedure.on_prepare().await.unwrap_err();
    assert_matches!(err, error::Error::FlowNotFound { .. });
}

#[tokio::test]
async fn test_drop_flow() {
    // create a flow
    let table_id = 1024;
    let source_table_names = vec![TableName::new(
        DEFAULT_CATALOG_NAME,
        DEFAULT_SCHEMA_NAME,
        "my_source_table",
    )];
    let sink_table_name =
        TableName::new(DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, "my_sink_table");
    let node_manager = Arc::new(MockFlownodeManager::new(NaiveFlownodeHandler));
    let ddl_context = new_ddl_context(node_manager);

    let task = test_create_table_task("my_source_table", table_id);
    ddl_context
        .table_metadata_manager
        .create_table_metadata(
            task.table_info.clone(),
            TableRouteValue::physical(vec![]),
            HashMap::new(),
        )
        .await
        .unwrap();
    let flow_id =
        create_test_flow(&ddl_context, "my_flow", source_table_names, sink_table_name).await;
    // Drops the flows
    let task = test_drop_flow_task("my_flow", flow_id, false);
    let mut procedure = DropFlowProcedure::new(task, ddl_context.clone());
    execute_procedure_until_done(&mut procedure).await;

    // Drops if not exists
    let task = test_drop_flow_task("my_flow", flow_id, true);
    let mut procedure = DropFlowProcedure::new(task, ddl_context.clone());
    execute_procedure_until_done(&mut procedure).await;

    // Drops again
    let task = test_drop_flow_task("my_flow", flow_id, false);
    let mut procedure = DropFlowProcedure::new(task, ddl_context);
    let err = procedure.on_prepare().await.unwrap_err();
    assert_matches!(err, error::Error::FlowNotFound { .. });
}

#[tokio::test]
async fn test_drop_pending_flow_without_routes() {
    let source_table_name = TableName::new(
        DEFAULT_CATALOG_NAME,
        DEFAULT_SCHEMA_NAME,
        "drop_pending_missing_source_table",
    );
    let sink_table_name = TableName::new(
        DEFAULT_CATALOG_NAME,
        DEFAULT_SCHEMA_NAME,
        "drop_pending_sink_table",
    );
    let node_manager = Arc::new(MockFlownodeManager::new(NaiveFlownodeHandler));
    let ddl_context = new_ddl_context(node_manager);

    let flow_id = create_test_pending_flow(
        &ddl_context,
        "drop_pending_flow",
        vec![source_table_name],
        sink_table_name,
    )
    .await;
    let flow_info = ddl_context
        .flow_metadata_manager
        .flow_info_manager()
        .get(flow_id)
        .await
        .unwrap()
        .unwrap();
    assert!(flow_info.is_pending());
    assert!(flow_info.flownode_ids().is_empty());

    let task = test_drop_flow_task("drop_pending_flow", flow_id, false);
    let mut procedure = DropFlowProcedure::new(task, ddl_context.clone());
    execute_procedure_until_done(&mut procedure).await;

    let task = test_drop_flow_task("drop_pending_flow", flow_id, false);
    let mut procedure = DropFlowProcedure::new(task, ddl_context);
    let err = procedure.on_prepare().await.unwrap_err();
    assert_matches!(err, error::Error::FlowNotFound { .. });
}
