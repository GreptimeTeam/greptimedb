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

use std::assert_matches::assert_matches;
use std::collections::HashMap;
use std::sync::Arc;

use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
use common_procedure_test::execute_procedure_until_done;
use session::context::QueryContext as SessionQueryContext;
use table::table_name::TableName;

use crate::ddl::DdlContext;
use crate::ddl::create_flow::{CreateFlowData, CreateFlowProcedure, CreateFlowState, FlowType};
use crate::ddl::test_util::create_table::test_create_table_task;
use crate::ddl::test_util::flownode_handler::NaiveFlownodeHandler;
use crate::error;
use crate::key::FlowId;
use crate::key::table_route::TableRouteValue;
use crate::rpc::ddl::{CreateFlowTask, FlowQueryContext, QueryContext};
use crate::test_util::{MockFlownodeManager, new_ddl_context_with_flow};

pub(crate) fn test_create_flow_task(
    name: &str,
    source_table_names: Vec<TableName>,
    sink_table_name: TableName,
    create_if_not_exists: bool,
) -> CreateFlowTask {
    CreateFlowTask {
        catalog_name: DEFAULT_CATALOG_NAME.to_string(),
        flow_name: name.to_string(),
        source_table_names,
        sink_table_name,
        or_replace: false,
        create_if_not_exists,
        expire_after: Some(300),
        eval_interval_secs: None,
        comment: "".to_string(),
        sql: "select 1".to_string(),
        flow_options: Default::default(),
    }
}

#[tokio::test]
async fn test_create_flow_source_table_not_found() {
    let source_table_names = vec![TableName::new(
        DEFAULT_CATALOG_NAME,
        DEFAULT_SCHEMA_NAME,
        "my_table",
    )];
    let sink_table_name =
        TableName::new(DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, "my_sink_table");
    let task = test_create_flow_task("my_flow", source_table_names, sink_table_name, false);
    let node_manager = Arc::new(MockFlownodeManager::new(NaiveFlownodeHandler));
    let ddl_context = new_ddl_context_with_flow(node_manager);
    let query_ctx = SessionQueryContext::arc().into();
    let mut procedure = CreateFlowProcedure::new(task, query_ctx, ddl_context);
    let err = procedure.on_prepare().await.unwrap_err();
    assert_matches!(err, error::Error::TableNotFound { .. });
}

pub(crate) async fn create_test_flow(
    ddl_context: &DdlContext,
    flow_name: &str,
    source_table_names: Vec<TableName>,
    sink_table_name: TableName,
) -> FlowId {
    let task = test_create_flow_task(
        flow_name,
        source_table_names.clone(),
        sink_table_name.clone(),
        false,
    );
    let query_ctx = SessionQueryContext::arc().into();
    let mut procedure = CreateFlowProcedure::new(task.clone(), query_ctx, ddl_context.clone());
    let output = execute_procedure_until_done(&mut procedure).await.unwrap();
    let flow_id = output.downcast_ref::<FlowId>().unwrap();

    *flow_id
}

#[tokio::test]
async fn test_create_flow() {
    let table_id = 1024;
    let source_table_names = vec![TableName::new(
        DEFAULT_CATALOG_NAME,
        DEFAULT_SCHEMA_NAME,
        "my_source_table",
    )];
    let sink_table_name =
        TableName::new(DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, "my_sink_table");
    let node_manager = Arc::new(MockFlownodeManager::new(NaiveFlownodeHandler));
    let ddl_context = new_ddl_context_with_flow(node_manager);

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
    let flow_id = create_test_flow(
        &ddl_context,
        "my_flow",
        source_table_names.clone(),
        sink_table_name.clone(),
    )
    .await;
    assert_eq!(flow_id, 1024);

    // Creates if not exists
    let task = test_create_flow_task(
        "my_flow",
        source_table_names.clone(),
        sink_table_name.clone(),
        true,
    );
    let query_ctx = SessionQueryContext::arc().into();
    let mut procedure = CreateFlowProcedure::new(task.clone(), query_ctx, ddl_context.clone());
    let output = execute_procedure_until_done(&mut procedure).await.unwrap();
    let flow_id = output.downcast_ref::<FlowId>().unwrap();
    assert_eq!(*flow_id, 1024);

    // Creates again
    let task = test_create_flow_task("my_flow", source_table_names, sink_table_name, false);
    let query_ctx = SessionQueryContext::arc().into();
    let mut procedure = CreateFlowProcedure::new(task.clone(), query_ctx, ddl_context);
    let err = procedure.on_prepare().await.unwrap_err();
    assert_matches!(err, error::Error::FlowAlreadyExists { .. });
}

#[tokio::test]
async fn test_create_flow_same_source_and_sink_table() {
    let table_id = 1024;
    let table_name = TableName::new(DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, "same_table");

    // Use the same table for both source and sink
    let source_table_names = vec![table_name.clone()];
    let sink_table_name = table_name.clone();

    let node_manager = Arc::new(MockFlownodeManager::new(NaiveFlownodeHandler));
    let ddl_context = new_ddl_context_with_flow(node_manager);

    // Create the table first so it exists
    let task = test_create_table_task("same_table", table_id);
    ddl_context
        .table_metadata_manager
        .create_table_metadata(
            task.table_info.clone(),
            TableRouteValue::physical(vec![]),
            HashMap::new(),
        )
        .await
        .unwrap();

    // Try to create a flow with same source and sink table - should fail
    let task = test_create_flow_task("my_flow", source_table_names, sink_table_name, false);
    let query_ctx = SessionQueryContext::arc().into();
    let mut procedure = CreateFlowProcedure::new(task, query_ctx, ddl_context);
    let err = procedure.on_prepare().await.unwrap_err();
    assert_matches!(err, error::Error::Unsupported { .. });

    // Verify the error message contains information about the same table
    if let error::Error::Unsupported { operation, .. } = &err {
        assert!(operation.contains("source and sink table being the same"));
        assert!(operation.contains("same_table"));
    }
}

fn create_test_flow_task_for_serialization() -> CreateFlowTask {
    CreateFlowTask {
        catalog_name: "test_catalog".to_string(),
        flow_name: "test_flow".to_string(),
        source_table_names: vec![TableName::new("catalog", "schema", "source_table")],
        sink_table_name: TableName::new("catalog", "schema", "sink_table"),
        or_replace: false,
        create_if_not_exists: false,
        expire_after: None,
        eval_interval_secs: None,
        comment: "test comment".to_string(),
        sql: "SELECT * FROM source_table".to_string(),
        flow_options: HashMap::new(),
    }
}

#[test]
fn test_create_flow_data_serialization_backward_compatibility() {
    // Test that old serialized data with query_context can be deserialized
    let old_json = r#"{
        "state": "Prepare",
        "task": {
            "catalog_name": "test_catalog",
            "flow_name": "test_flow",
            "source_table_names": [{"catalog_name": "catalog", "schema_name": "schema", "table_name": "source"}],
            "sink_table_name": {"catalog_name": "catalog", "schema_name": "schema", "table_name": "sink"},
            "or_replace": false,
            "create_if_not_exists": false,
            "expire_after": null,
            "comment": "test",
            "sql": "SELECT * FROM source",
            "flow_options": {}
        },
        "flow_id": null,
        "peers": [],
        "source_table_ids": [],
        "query_context": {
            "current_catalog": "old_catalog",
            "current_schema": "old_schema", 
            "timezone": "UTC",
            "extensions": {},
            "channel": 0
        },
        "prev_flow_info_value": null,
        "did_replace": false,
        "flow_type": null
    }"#;

    let data: CreateFlowData = serde_json::from_str(old_json).unwrap();
    assert_eq!(data.flow_context.catalog, "old_catalog");
    assert_eq!(data.flow_context.schema, "old_schema");
    assert_eq!(data.flow_context.timezone, "UTC");
}

#[test]
fn test_create_flow_data_new_format_serialization() {
    // Test new format serialization/deserialization
    let flow_context = FlowQueryContext {
        catalog: "new_catalog".to_string(),
        schema: "new_schema".to_string(),
        timezone: "America/New_York".to_string(),
    };

    let data = CreateFlowData {
        state: CreateFlowState::Prepare,
        task: create_test_flow_task_for_serialization(),
        flow_id: None,
        peers: vec![],
        source_table_ids: vec![],
        flow_context,
        prev_flow_info_value: None,
        did_replace: false,
        flow_type: None,
    };

    let serialized = serde_json::to_string(&data).unwrap();
    let deserialized: CreateFlowData = serde_json::from_str(&serialized).unwrap();

    assert_eq!(data.flow_context, deserialized.flow_context);
    assert_eq!(deserialized.flow_context.catalog, "new_catalog");
    assert_eq!(deserialized.flow_context.schema, "new_schema");
    assert_eq!(deserialized.flow_context.timezone, "America/New_York");
}

#[test]
fn test_flow_query_context_conversion_from_query_context() {
    let query_context = QueryContext {
        current_catalog: "prod_catalog".to_string(),
        current_schema: "public".to_string(),
        timezone: "America/Los_Angeles".to_string(),
        extensions: [
            ("unused_key".to_string(), "unused_value".to_string()),
            ("another_key".to_string(), "another_value".to_string()),
        ]
        .into(),
        channel: 99,
    };

    let flow_context: FlowQueryContext = query_context.into();

    assert_eq!(flow_context.catalog, "prod_catalog");
    assert_eq!(flow_context.schema, "public");
    assert_eq!(flow_context.timezone, "America/Los_Angeles");
}

#[test]
fn test_flow_info_conversion_with_flow_context() {
    let flow_context = FlowQueryContext {
        catalog: "info_catalog".to_string(),
        schema: "info_schema".to_string(),
        timezone: "Europe/Berlin".to_string(),
    };

    let data = CreateFlowData {
        state: CreateFlowState::CreateMetadata,
        task: create_test_flow_task_for_serialization(),
        flow_id: Some(123),
        peers: vec![],
        source_table_ids: vec![456, 789],
        flow_context,
        prev_flow_info_value: None,
        did_replace: false,
        flow_type: Some(FlowType::Batching),
    };

    let (flow_info, _routes) = (&data).into();

    assert!(flow_info.query_context.is_some());
    let query_context = flow_info.query_context.unwrap();
    assert_eq!(query_context.current_catalog(), "info_catalog");
    assert_eq!(query_context.current_schema(), "info_schema");
    assert_eq!(query_context.timezone(), "Europe/Berlin");
    assert_eq!(query_context.channel(), 0);
    assert!(query_context.extensions().is_empty());
}

#[test]
fn test_mixed_serialization_format_support() {
    // Test that we can deserialize both old and new formats

    // Test new FlowQueryContext format
    let new_format = r#"{"catalog": "test", "schema": "test", "timezone": "UTC"}"#;
    let ctx_from_new: FlowQueryContext = serde_json::from_str(new_format).unwrap();
    assert_eq!(ctx_from_new.catalog, "test");
    assert_eq!(ctx_from_new.schema, "test");
    assert_eq!(ctx_from_new.timezone, "UTC");

    // Test old QueryContext format conversion
    let old_format = r#"{"current_catalog": "old_test", "current_schema": "old_schema", "timezone": "PST", "extensions": {}, "channel": 0}"#;
    let ctx_from_old: FlowQueryContext = serde_json::from_str(old_format).unwrap();
    assert_eq!(ctx_from_old.catalog, "old_test");
    assert_eq!(ctx_from_old.schema, "old_schema");
    assert_eq!(ctx_from_old.timezone, "PST");

    // Test that they can be compared
    let expected_new = FlowQueryContext {
        catalog: "test".to_string(),
        schema: "test".to_string(),
        timezone: "UTC".to_string(),
    };
    assert_eq!(ctx_from_new, expected_new);
}
