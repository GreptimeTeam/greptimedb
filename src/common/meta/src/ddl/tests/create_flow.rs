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
use session::context::QueryContext;
use table::table_name::TableName;

use crate::ddl::create_flow::CreateFlowProcedure;
use crate::ddl::test_util::create_table::test_create_table_task;
use crate::ddl::test_util::flownode_handler::NaiveFlownodeHandler;
use crate::ddl::DdlContext;
use crate::error;
use crate::key::table_route::TableRouteValue;
use crate::key::FlowId;
use crate::rpc::ddl::CreateFlowTask;
use crate::test_util::{new_ddl_context, MockFlownodeManager};

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
    let ddl_context = new_ddl_context(node_manager);
    let query_ctx = QueryContext::arc().into();
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
    let query_ctx = QueryContext::arc().into();
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
    let query_ctx = QueryContext::arc().into();
    let mut procedure = CreateFlowProcedure::new(task.clone(), query_ctx, ddl_context.clone());
    let output = execute_procedure_until_done(&mut procedure).await.unwrap();
    let flow_id = output.downcast_ref::<FlowId>().unwrap();
    assert_eq!(*flow_id, 1024);

    // Creates again
    let task = test_create_flow_task("my_flow", source_table_names, sink_table_name, false);
    let query_ctx = QueryContext::arc().into();
    let mut procedure = CreateFlowProcedure::new(task.clone(), query_ctx, ddl_context);
    let err = procedure.on_prepare().await.unwrap_err();
    assert_matches!(err, error::Error::FlowAlreadyExists { .. });
}
