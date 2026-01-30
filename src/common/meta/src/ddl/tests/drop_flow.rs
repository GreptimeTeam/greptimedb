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
use table::table_name::TableName;

use crate::ddl::drop_flow::DropFlowProcedure;
use crate::ddl::test_util::create_table::test_create_table_task;
use crate::ddl::test_util::flownode_handler::NaiveFlownodeHandler;
use crate::ddl::tests::create_flow::create_test_flow;
use crate::error;
use crate::key::table_route::TableRouteValue;
use crate::rpc::ddl::DropFlowTask;
use crate::test_util::{MockFlownodeManager, new_ddl_context_with_flow};

fn test_drop_flow_task(flow_name: &str, flow_id: u32, drop_if_exists: bool) -> DropFlowTask {
    DropFlowTask {
        catalog_name: DEFAULT_CATALOG_NAME.to_string(),
        flow_name: flow_name.to_string(),
        flow_id,
        drop_if_exists,
    }
}

#[tokio::test]
async fn test_drop_flow_not_found() {
    let flow_id = 1024;
    let node_manager = Arc::new(MockFlownodeManager::new(NaiveFlownodeHandler));
    let ddl_context = new_ddl_context_with_flow(node_manager);
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
