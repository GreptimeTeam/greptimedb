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

use api::v1::meta::Partition;
use api::v1::{ColumnDataType, SemanticType};
use common_error::ext::ErrorExt;
use common_error::status_code::StatusCode;
use common_procedure::{Context as ProcedureContext, Procedure, ProcedureId, Status};
use common_procedure_test::{
    execute_procedure_until, execute_procedure_until_done, MockContextProvider,
};
use store_api::storage::RegionId;

use crate::ddl::create_table::{CreateTableProcedure, CreateTableState};
use crate::ddl::test_util::columns::TestColumnDefBuilder;
use crate::ddl::test_util::create_table::{
    build_raw_table_info_from_expr, TestCreateTableExprBuilder,
};
use crate::ddl::test_util::datanode_handler::{
    NaiveDatanodeHandler, RetryErrorDatanodeHandler, UnexpectedErrorDatanodeHandler,
};
use crate::error::Error;
use crate::key::table_route::TableRouteValue;
use crate::kv_backend::memory::MemoryKvBackend;
use crate::rpc::ddl::CreateTableTask;
use crate::test_util::{new_ddl_context, new_ddl_context_with_kv_backend, MockDatanodeManager};

pub(crate) fn test_create_table_task(name: &str) -> CreateTableTask {
    let create_table = TestCreateTableExprBuilder::default()
        .column_defs([
            TestColumnDefBuilder::default()
                .name("ts")
                .data_type(ColumnDataType::TimestampMillisecond)
                .semantic_type(SemanticType::Timestamp)
                .build()
                .unwrap()
                .into(),
            TestColumnDefBuilder::default()
                .name("host")
                .data_type(ColumnDataType::String)
                .semantic_type(SemanticType::Tag)
                .build()
                .unwrap()
                .into(),
            TestColumnDefBuilder::default()
                .name("cpu")
                .data_type(ColumnDataType::Float64)
                .semantic_type(SemanticType::Field)
                .build()
                .unwrap()
                .into(),
        ])
        .time_index("ts")
        .primary_keys(["host".into()])
        .table_name(name)
        .build()
        .unwrap()
        .into();
    let table_info = build_raw_table_info_from_expr(&create_table);
    CreateTableTask {
        create_table,
        // Single region
        partitions: vec![Partition {
            column_list: vec![],
            value_list: vec![],
        }],
        table_info,
    }
}

#[tokio::test]
async fn test_on_prepare_table_exists_err() {
    let node_manager = Arc::new(MockDatanodeManager::new(()));
    let ddl_context = new_ddl_context(node_manager);
    let cluster_id = 1;
    let task = test_create_table_task("foo");
    assert!(!task.create_table.create_if_not_exists);
    // Puts a value to table name key.
    ddl_context
        .table_metadata_manager
        .create_table_metadata(
            task.table_info.clone(),
            TableRouteValue::physical(vec![]),
            HashMap::new(),
        )
        .await
        .unwrap();
    let mut procedure = CreateTableProcedure::new(cluster_id, task, ddl_context);
    let err = procedure.on_prepare().await.unwrap_err();
    assert_matches!(err, Error::TableAlreadyExists { .. });
    assert_eq!(err.status_code(), StatusCode::TableAlreadyExists);
}

#[tokio::test]
async fn test_on_prepare_with_create_if_table_exists() {
    let node_manager = Arc::new(MockDatanodeManager::new(()));
    let ddl_context = new_ddl_context(node_manager);
    let cluster_id = 1;
    let mut task = test_create_table_task("foo");
    task.create_table.create_if_not_exists = true;
    task.table_info.ident.table_id = 1024;
    // Puts a value to table name key.
    ddl_context
        .table_metadata_manager
        .create_table_metadata(
            task.table_info.clone(),
            TableRouteValue::physical(vec![]),
            HashMap::new(),
        )
        .await
        .unwrap();
    let mut procedure = CreateTableProcedure::new(cluster_id, task, ddl_context);
    let status = procedure.on_prepare().await.unwrap();
    assert_matches!(status, Status::Done { output: Some(..) });
    let table_id = *status.downcast_output_ref::<u32>().unwrap();
    assert_eq!(table_id, 1024);
}

#[tokio::test]
async fn test_on_prepare_without_create_if_table_exists() {
    let node_manager = Arc::new(MockDatanodeManager::new(()));
    let ddl_context = new_ddl_context(node_manager);
    let cluster_id = 1;
    let mut task = test_create_table_task("foo");
    task.create_table.create_if_not_exists = true;
    let mut procedure = CreateTableProcedure::new(cluster_id, task, ddl_context);
    let status = procedure.on_prepare().await.unwrap();
    assert_matches!(status, Status::Executing { persist: true });
    assert_eq!(procedure.table_id(), 1024);
}

#[tokio::test]
async fn test_on_prepare_with_no_partition_err() {
    let node_manager = Arc::new(MockDatanodeManager::new(()));
    let ddl_context = new_ddl_context(node_manager);
    let cluster_id = 1;
    let mut task = test_create_table_task("foo");
    task.partitions = vec![];
    task.create_table.create_if_not_exists = true;
    let mut procedure = CreateTableProcedure::new(cluster_id, task, ddl_context);
    let err = procedure.on_prepare().await.unwrap_err();
    assert_matches!(err, Error::Unexpected { .. });
    assert!(err
        .to_string()
        .contains("The number of partitions must be greater than 0"),);
}

#[tokio::test]
async fn test_on_datanode_create_regions_should_retry() {
    common_telemetry::init_default_ut_logging();
    let node_manager = Arc::new(MockDatanodeManager::new(RetryErrorDatanodeHandler));
    let ddl_context = new_ddl_context(node_manager);
    let cluster_id = 1;
    let task = test_create_table_task("foo");
    assert!(!task.create_table.create_if_not_exists);
    let mut procedure = CreateTableProcedure::new(cluster_id, task, ddl_context);
    procedure.on_prepare().await.unwrap();
    let ctx = ProcedureContext {
        procedure_id: ProcedureId::random(),
        provider: Arc::new(MockContextProvider::default()),
    };
    let error = procedure.execute(&ctx).await.unwrap_err();
    assert!(error.is_retry_later());
}

#[tokio::test]
async fn test_on_datanode_create_regions_should_not_retry() {
    common_telemetry::init_default_ut_logging();
    let node_manager = Arc::new(MockDatanodeManager::new(UnexpectedErrorDatanodeHandler));
    let ddl_context = new_ddl_context(node_manager);
    let cluster_id = 1;
    let task = test_create_table_task("foo");
    assert!(!task.create_table.create_if_not_exists);
    let mut procedure = CreateTableProcedure::new(cluster_id, task, ddl_context);
    procedure.on_prepare().await.unwrap();
    let ctx = ProcedureContext {
        procedure_id: ProcedureId::random(),
        provider: Arc::new(MockContextProvider::default()),
    };
    let error = procedure.execute(&ctx).await.unwrap_err();
    assert!(!error.is_retry_later());
}

#[tokio::test]
async fn test_on_create_metadata_error() {
    common_telemetry::init_default_ut_logging();
    let node_manager = Arc::new(MockDatanodeManager::new(NaiveDatanodeHandler));
    let ddl_context = new_ddl_context(node_manager);
    let cluster_id = 1;
    let task = test_create_table_task("foo");
    assert!(!task.create_table.create_if_not_exists);
    let mut procedure = CreateTableProcedure::new(cluster_id, task.clone(), ddl_context.clone());
    procedure.on_prepare().await.unwrap();
    let ctx = ProcedureContext {
        procedure_id: ProcedureId::random(),
        provider: Arc::new(MockContextProvider::default()),
    };
    procedure.execute(&ctx).await.unwrap();
    let mut task = task;
    // Creates table metadata(different with the task)
    task.table_info.ident.table_id = procedure.table_id();
    ddl_context
        .table_metadata_manager
        .create_table_metadata(
            task.table_info.clone(),
            TableRouteValue::physical(vec![]),
            HashMap::new(),
        )
        .await
        .unwrap();
    // Triggers procedure to create table metadata
    let error = procedure.execute(&ctx).await.unwrap_err();
    assert!(!error.is_retry_later());
}

#[tokio::test]
async fn test_on_create_metadata() {
    common_telemetry::init_default_ut_logging();
    let node_manager = Arc::new(MockDatanodeManager::new(NaiveDatanodeHandler));
    let ddl_context = new_ddl_context(node_manager);
    let cluster_id = 1;
    let task = test_create_table_task("foo");
    assert!(!task.create_table.create_if_not_exists);
    let mut procedure = CreateTableProcedure::new(cluster_id, task, ddl_context);
    procedure.on_prepare().await.unwrap();
    let ctx = ProcedureContext {
        procedure_id: ProcedureId::random(),
        provider: Arc::new(MockContextProvider::default()),
    };
    procedure.execute(&ctx).await.unwrap();
    // Triggers procedure to create table metadata
    let status = procedure.execute(&ctx).await.unwrap();
    let table_id = status.downcast_output_ref::<u32>().unwrap();
    assert_eq!(*table_id, 1024);
}

#[tokio::test]
async fn test_memory_region_keeper_guard_dropped_on_procedure_done() {
    let cluster_id = 1;

    let node_manager = Arc::new(MockDatanodeManager::new(NaiveDatanodeHandler));
    let kv_backend = Arc::new(MemoryKvBackend::new());
    let ddl_context = new_ddl_context_with_kv_backend(node_manager, kv_backend);

    let task = test_create_table_task("foo");
    let mut procedure = CreateTableProcedure::new(cluster_id, task, ddl_context.clone());

    execute_procedure_until(&mut procedure, |p| {
        p.creator.data.state == CreateTableState::CreateMetadata
    })
    .await;

    // Ensure that after running to the state `CreateMetadata`(just past `DatanodeCreateRegions`),
    // the opening regions should be recorded:
    let guards = &procedure.creator.opening_regions;
    assert_eq!(guards.len(), 1);
    let (datanode_id, region_id) = (0, RegionId::new(procedure.table_id(), 0));
    assert_eq!(guards[0].info(), (datanode_id, region_id));
    assert!(ddl_context
        .memory_region_keeper
        .contains(datanode_id, region_id));

    execute_procedure_until_done(&mut procedure).await;

    // Ensure that when run to the end, the opening regions should be cleared:
    let guards = &procedure.creator.opening_regions;
    assert!(guards.is_empty());
    assert!(!ddl_context
        .memory_region_keeper
        .contains(datanode_id, region_id));
}
