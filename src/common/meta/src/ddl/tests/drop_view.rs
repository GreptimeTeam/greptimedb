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

use common_error::ext::ErrorExt;
use common_error::status_code::StatusCode;
use common_procedure_test::execute_procedure_until_done;
use store_api::storage::TableId;

use crate::ddl::drop_view::{DropViewProcedure, DropViewState};
use crate::ddl::test_util::create_table::test_create_table_task;
use crate::ddl::tests::create_view::{test_create_view_task, test_table_names};
use crate::key::table_route::TableRouteValue;
use crate::rpc::ddl::DropViewTask;
use crate::test_util::{new_ddl_context, MockDatanodeManager};

fn new_drop_view_task(view: &str, view_id: TableId, drop_if_exists: bool) -> DropViewTask {
    DropViewTask {
        catalog: "greptime".to_string(),
        schema: "public".to_string(),
        view: view.to_string(),
        view_id,
        drop_if_exists,
    }
}

#[tokio::test]
async fn test_on_prepare_view_not_exists_err() {
    let node_manager = Arc::new(MockDatanodeManager::new(()));
    let ddl_context = new_ddl_context(node_manager);
    let cluster_id = 1;
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
    let mut procedure = DropViewProcedure::new(cluster_id, task, ddl_context);
    let err = procedure.on_prepare().await.unwrap_err();
    assert_eq!(err.status_code(), StatusCode::TableNotFound);
}

#[tokio::test]
async fn test_on_prepare_not_view_err() {
    let node_manager = Arc::new(MockDatanodeManager::new(()));
    let ddl_context = new_ddl_context(node_manager);
    let cluster_id = 1;
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
    let mut procedure = DropViewProcedure::new(cluster_id, task, ddl_context);
    // It's not a view, expect error
    let err = procedure.on_prepare().await.unwrap_err();
    assert_eq!(err.status_code(), StatusCode::InvalidArguments);
}

#[tokio::test]
async fn test_on_prepare_success() {
    let node_manager = Arc::new(MockDatanodeManager::new(()));
    let ddl_context = new_ddl_context(node_manager);
    let cluster_id = 1;
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
    let mut procedure = DropViewProcedure::new(cluster_id, task, ddl_context.clone());
    procedure.on_prepare().await.unwrap();

    let task = new_drop_view_task(view_name, view_id, false);
    // Prepare success
    let mut procedure = DropViewProcedure::new(cluster_id, task, ddl_context);
    procedure.on_prepare().await.unwrap();
    assert_eq!(DropViewState::DeleteMetadata, procedure.state());
}

#[tokio::test]
async fn test_drop_view_success() {
    let node_manager = Arc::new(MockDatanodeManager::new(()));
    let ddl_context = new_ddl_context(node_manager);
    let cluster_id = 1;
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

    assert!(ddl_context
        .table_metadata_manager
        .view_info_manager()
        .get(view_id)
        .await
        .unwrap()
        .is_some());

    let task = new_drop_view_task(view_name, view_id, false);
    // Prepare success
    let mut procedure = DropViewProcedure::new(cluster_id, task, ddl_context.clone());
    execute_procedure_until_done(&mut procedure).await;
    assert_eq!(DropViewState::InvalidateViewCache, procedure.state());

    // Assert view info is removed
    assert!(ddl_context
        .table_metadata_manager
        .view_info_manager()
        .get(view_id)
        .await
        .unwrap()
        .is_none());

    // Drop again
    let task = new_drop_view_task(view_name, view_id, false);
    let mut procedure = DropViewProcedure::new(cluster_id, task, ddl_context);
    let err = procedure.on_prepare().await.unwrap_err();
    assert_eq!(err.status_code(), StatusCode::TableNotFound);
}
