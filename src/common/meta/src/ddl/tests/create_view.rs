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
use std::collections::HashSet;
use std::sync::Arc;

use api::v1::{CreateViewExpr, TableName};
use common_error::ext::ErrorExt;
use common_error::status_code::StatusCode;
use common_procedure::{Context as ProcedureContext, Procedure, ProcedureId, Status};
use common_procedure_test::MockContextProvider;
use table::metadata;
use table::metadata::{RawTableInfo, RawTableMeta, TableType};

use crate::ddl::create_table::CreateTableProcedure;
use crate::ddl::create_view::CreateViewProcedure;
use crate::ddl::test_util::datanode_handler::NaiveDatanodeHandler;
use crate::ddl::tests::create_table::test_create_table_task;
use crate::error::Error;
use crate::rpc::ddl::CreateViewTask;
use crate::test_util::{MockDatanodeManager, new_ddl_context};

pub(crate) fn test_table_names() -> HashSet<table::table_name::TableName> {
    let mut set = HashSet::new();
    set.insert(table::table_name::TableName {
        catalog_name: "greptime".to_string(),
        schema_name: "public".to_string(),
        table_name: "a_table".to_string(),
    });
    set.insert(table::table_name::TableName {
        catalog_name: "greptime".to_string(),
        schema_name: "public".to_string(),
        table_name: "b_table".to_string(),
    });
    set
}

pub(crate) fn test_create_view_task(name: &str) -> CreateViewTask {
    let table_names = vec![
        TableName {
            catalog_name: "greptime".to_string(),
            schema_name: "public".to_string(),
            table_name: "a_table".to_string(),
        },
        TableName {
            catalog_name: "greptime".to_string(),
            schema_name: "public".to_string(),
            table_name: "b_table".to_string(),
        },
    ];

    let expr = CreateViewExpr {
        catalog_name: "greptime".to_string(),
        schema_name: "public".to_string(),
        view_name: name.to_string(),
        or_replace: false,
        create_if_not_exists: false,
        logical_plan: vec![1, 2, 3],
        table_names,
        columns: vec!["a".to_string()],
        plan_columns: vec!["number".to_string()],
        definition: "CREATE VIEW test AS SELECT * FROM numbers".to_string(),
    };

    let view_info = RawTableInfo {
        ident: metadata::TableIdent {
            table_id: 0,
            version: 0,
        },
        name: expr.view_name.clone(),
        desc: None,
        catalog_name: expr.catalog_name.clone(),
        schema_name: expr.schema_name.clone(),
        meta: RawTableMeta::default(),
        table_type: TableType::View,
    };

    CreateViewTask {
        create_view: expr,
        view_info,
    }
}

#[tokio::test]
async fn test_on_prepare_view_exists_err() {
    let node_manager = Arc::new(MockDatanodeManager::new(()));
    let ddl_context = new_ddl_context(node_manager);
    let task = test_create_view_task("foo");
    assert!(!task.create_view.create_if_not_exists);
    // Puts a value to table name key.
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
    let mut procedure = CreateViewProcedure::new(task, ddl_context);
    let err = procedure.on_prepare().await.unwrap_err();
    assert_matches!(err, Error::ViewAlreadyExists { .. });
    assert_eq!(err.status_code(), StatusCode::TableAlreadyExists);
}

#[tokio::test]
async fn test_on_prepare_with_create_if_view_exists() {
    let node_manager = Arc::new(MockDatanodeManager::new(()));
    let ddl_context = new_ddl_context(node_manager);
    let mut task = test_create_view_task("foo");
    task.create_view.create_if_not_exists = true;
    task.view_info.ident.table_id = 1024;
    // Puts a value to table name key.
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
    let mut procedure = CreateViewProcedure::new(task, ddl_context);
    let status = procedure.on_prepare().await.unwrap();
    assert_matches!(status, Status::Done { output: Some(..) });
    let table_id = *status.downcast_output_ref::<u32>().unwrap();
    assert_eq!(table_id, 1024);
}

#[tokio::test]
async fn test_on_prepare_without_create_if_table_exists() {
    let node_manager = Arc::new(MockDatanodeManager::new(()));
    let ddl_context = new_ddl_context(node_manager);
    let mut task = test_create_view_task("foo");
    task.create_view.create_if_not_exists = true;
    let mut procedure = CreateViewProcedure::new(task, ddl_context);
    let status = procedure.on_prepare().await.unwrap();
    assert_matches!(
        status,
        Status::Executing {
            persist: true,
            clean_poisons: false
        }
    );
    assert_eq!(procedure.view_id(), 1024);
}

#[tokio::test]
async fn test_on_create_metadata() {
    common_telemetry::init_default_ut_logging();
    let node_manager = Arc::new(MockDatanodeManager::new(NaiveDatanodeHandler));
    let ddl_context = new_ddl_context(node_manager);
    let task = test_create_view_task("foo");
    assert!(!task.create_view.create_if_not_exists);
    let mut procedure = CreateViewProcedure::new(task, ddl_context);
    procedure.on_prepare().await.unwrap();
    let ctx = ProcedureContext {
        procedure_id: ProcedureId::random(),
        provider: Arc::new(MockContextProvider::default()),
    };
    // Triggers procedure to create view metadata
    let status = procedure.execute(&ctx).await.unwrap();
    let view_id = status.downcast_output_ref::<u32>().unwrap();
    assert_eq!(*view_id, 1024);
}

#[tokio::test]
async fn test_replace_view_metadata() {
    common_telemetry::init_default_ut_logging();
    let node_manager = Arc::new(MockDatanodeManager::new(NaiveDatanodeHandler));
    let ddl_context = new_ddl_context(node_manager.clone());
    let task = test_create_view_task("foo");
    assert!(!task.create_view.create_if_not_exists);
    let mut procedure = CreateViewProcedure::new(task.clone(), ddl_context.clone());
    procedure.on_prepare().await.unwrap();
    let ctx = ProcedureContext {
        procedure_id: ProcedureId::random(),
        provider: Arc::new(MockContextProvider::default()),
    };
    // Triggers procedure to create view metadata
    let status = procedure.execute(&ctx).await.unwrap();
    let view_id = status.downcast_output_ref::<u32>().unwrap();
    assert_eq!(*view_id, 1024);

    let current_view_info = ddl_context
        .table_metadata_manager
        .view_info_manager()
        .get(*view_id)
        .await
        .unwrap()
        .unwrap();

    assert_eq!(current_view_info.view_info, vec![1, 2, 3]);

    // Create new task to replace the exists one.
    let mut task = test_create_view_task("foo");
    // The view already exists, prepare should fail
    {
        let mut procedure = CreateViewProcedure::new(task.clone(), ddl_context.clone());
        let err = procedure.on_prepare().await.unwrap_err();
        assert_matches!(err, Error::ViewAlreadyExists { .. });
        assert_eq!(err.status_code(), StatusCode::TableAlreadyExists);
    }

    // Set `or_replace` to be `true` and try again
    task.create_view.or_replace = true;
    task.create_view.logical_plan = vec![4, 5, 6];
    task.create_view.definition = "new_definition".to_string();

    let mut procedure = CreateViewProcedure::new(task, ddl_context.clone());
    procedure.on_prepare().await.unwrap();
    let ctx = ProcedureContext {
        procedure_id: ProcedureId::random(),
        provider: Arc::new(MockContextProvider::default()),
    };
    // Triggers procedure to replace view metadata, but the view_id is unchanged.
    let status = procedure.execute(&ctx).await.unwrap();
    let view_id = status.downcast_output_ref::<u32>().unwrap();
    assert_eq!(*view_id, 1024);

    let current_view_info = ddl_context
        .table_metadata_manager
        .view_info_manager()
        .get(*view_id)
        .await
        .unwrap()
        .unwrap();

    assert_eq!(current_view_info.view_info, vec![4, 5, 6]);
    assert_eq!(current_view_info.definition, "new_definition");
    assert_eq!(current_view_info.columns, vec!["a".to_string()]);
    assert_eq!(current_view_info.plan_columns, vec!["number".to_string()]);
}

#[tokio::test]
async fn test_replace_table() {
    common_telemetry::init_default_ut_logging();
    let node_manager = Arc::new(MockDatanodeManager::new(NaiveDatanodeHandler));
    let ddl_context = new_ddl_context(node_manager.clone());

    {
        // Create a `foo` table.
        let task = test_create_table_task("foo");
        let mut procedure = CreateTableProcedure::new(task, ddl_context.clone()).unwrap();
        procedure.on_prepare().await.unwrap();
        let ctx = ProcedureContext {
            procedure_id: ProcedureId::random(),
            provider: Arc::new(MockContextProvider::default()),
        };
        procedure.execute(&ctx).await.unwrap();
        procedure.execute(&ctx).await.unwrap();
    }

    // Try to replace a view named `foo` too.
    let mut task = test_create_view_task("foo");
    task.create_view.or_replace = true;
    let mut procedure = CreateViewProcedure::new(task.clone(), ddl_context.clone());
    let err = procedure.on_prepare().await.unwrap_err();
    assert_matches!(err, Error::TableAlreadyExists { .. });
    assert_eq!(err.status_code(), StatusCode::TableAlreadyExists);
}
