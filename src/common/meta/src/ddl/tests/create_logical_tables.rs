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
use api::v1::region::{QueryRequest, RegionRequest};
use api::v1::{ColumnDataType, SemanticType};
use common_error::ext::ErrorExt;
use common_error::status_code::StatusCode;
use common_procedure::{Context as ProcedureContext, Procedure, ProcedureId, Status};
use common_procedure_test::MockContextProvider;
use common_recordbatch::SendableRecordBatchStream;
use common_telemetry::debug;
use store_api::storage::RegionId;
use table::metadata::RawTableInfo;

use crate::ddl::create_logical_tables::CreateLogicalTablesProcedure;
use crate::ddl::test_util::create_table::build_raw_table_info_from_expr;
use crate::ddl::test_util::{TestColumnDefBuilder, TestCreateTableExprBuilder};
use crate::ddl::{DdlContext, TableMetadata, TableMetadataAllocatorContext};
use crate::error::{Error, Result};
use crate::key::table_route::TableRouteValue;
use crate::peer::Peer;
use crate::rpc::ddl::CreateTableTask;
use crate::test_util::{new_ddl_context, AffectedRows, MockDatanodeHandler, MockDatanodeManager};

// Note: this code may be duplicated with others.
// However, it's by design, ensures the tests are easy to be modified or added.
fn test_create_logical_table_task(name: &str) -> CreateTableTask {
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

// Note: this code may be duplicated with others.
// However, it's by design, ensures the tests are easy to be modified or added.
fn test_create_physical_table_task(name: &str) -> CreateTableTask {
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
                .name("value")
                .data_type(ColumnDataType::Float64)
                .semantic_type(SemanticType::Field)
                .build()
                .unwrap()
                .into(),
        ])
        .time_index("ts")
        .primary_keys(["value".into()])
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
async fn test_on_prepare_physical_table_not_found() {
    let datanode_manager = Arc::new(MockDatanodeManager::new(()));
    let ddl_context = new_ddl_context(datanode_manager);
    let cluster_id = 1;
    let tasks = vec![test_create_logical_table_task("foo")];
    let physical_table_id = 1024u32;
    let mut procedure =
        CreateLogicalTablesProcedure::new(cluster_id, tasks, physical_table_id, ddl_context);
    let err = procedure.on_prepare().await.unwrap_err();
    assert_matches!(err, Error::TableRouteNotFound { .. });
}

async fn create_physical_table_metadata(
    ddl_context: &DdlContext,
    table_info: RawTableInfo,
    table_route: TableRouteValue,
) {
    ddl_context
        .table_metadata_manager
        .create_table_metadata(table_info, table_route, HashMap::default())
        .await
        .unwrap();
}

#[tokio::test]
async fn test_on_prepare() {
    let datanode_manager = Arc::new(MockDatanodeManager::new(()));
    let ddl_context = new_ddl_context(datanode_manager);
    let cluster_id = 1;
    // Prepares physical table metadata.
    let mut create_physical_table_task = test_create_physical_table_task("phy_table");
    let TableMetadata {
        table_id,
        table_route,
        ..
    } = ddl_context
        .table_metadata_allocator
        .create(
            &TableMetadataAllocatorContext { cluster_id },
            &create_physical_table_task,
        )
        .await
        .unwrap();
    create_physical_table_task.set_table_id(table_id);
    create_physical_table_metadata(
        &ddl_context,
        create_physical_table_task.table_info.clone(),
        table_route,
    )
    .await;
    // The create logical table procedure.
    let tasks = vec![test_create_logical_table_task("foo")];
    let physical_table_id = table_id;
    let mut procedure =
        CreateLogicalTablesProcedure::new(cluster_id, tasks, physical_table_id, ddl_context);
    let status = procedure.on_prepare().await.unwrap();
    assert_matches!(status, Status::Executing { persist: true });
}

#[tokio::test]
async fn test_on_prepare_logical_table_exists_err() {
    let datanode_manager = Arc::new(MockDatanodeManager::new(()));
    let ddl_context = new_ddl_context(datanode_manager);
    let cluster_id = 1;
    // Prepares physical table metadata.
    let mut create_physical_table_task = test_create_physical_table_task("phy_table");
    let TableMetadata {
        table_id,
        table_route,
        ..
    } = ddl_context
        .table_metadata_allocator
        .create(
            &TableMetadataAllocatorContext { cluster_id },
            &create_physical_table_task,
        )
        .await
        .unwrap();
    create_physical_table_task.set_table_id(table_id);
    create_physical_table_metadata(
        &ddl_context,
        create_physical_table_task.table_info.clone(),
        table_route,
    )
    .await;
    // Creates the logical table metadata.
    let mut task = test_create_logical_table_task("foo");
    task.set_table_id(1025);
    ddl_context
        .table_metadata_manager
        .create_logic_tables_metadata(vec![(
            task.table_info.clone(),
            TableRouteValue::logical(1024, vec![RegionId::new(1025, 1)]),
        )])
        .await
        .unwrap();
    // The create logical table procedure.
    let physical_table_id = table_id;
    let mut procedure =
        CreateLogicalTablesProcedure::new(cluster_id, vec![task], physical_table_id, ddl_context);
    let err = procedure.on_prepare().await.unwrap_err();
    assert_matches!(err, Error::TableAlreadyExists { .. });
    assert_eq!(err.status_code(), StatusCode::TableAlreadyExists);
}

#[tokio::test]
async fn test_on_prepare_with_create_if_table_exists() {
    let datanode_manager = Arc::new(MockDatanodeManager::new(()));
    let ddl_context = new_ddl_context(datanode_manager);
    let cluster_id = 1;
    // Prepares physical table metadata.
    let mut create_physical_table_task = test_create_physical_table_task("phy_table");
    let TableMetadata {
        table_id,
        table_route,
        ..
    } = ddl_context
        .table_metadata_allocator
        .create(
            &TableMetadataAllocatorContext { cluster_id },
            &create_physical_table_task,
        )
        .await
        .unwrap();
    create_physical_table_task.set_table_id(table_id);
    create_physical_table_metadata(
        &ddl_context,
        create_physical_table_task.table_info.clone(),
        table_route,
    )
    .await;
    // Creates the logical table metadata.
    let mut task = test_create_logical_table_task("foo");
    task.set_table_id(8192);
    ddl_context
        .table_metadata_manager
        .create_logic_tables_metadata(vec![(
            task.table_info.clone(),
            TableRouteValue::logical(1024, vec![RegionId::new(8192, 1)]),
        )])
        .await
        .unwrap();
    // The create logical table procedure.
    let physical_table_id = table_id;
    // Sets `create_if_not_exists`
    task.create_table.create_if_not_exists = true;
    let mut procedure =
        CreateLogicalTablesProcedure::new(cluster_id, vec![task], physical_table_id, ddl_context);
    let status = procedure.on_prepare().await.unwrap();
    let output = status.downcast_output_ref::<Vec<u32>>().unwrap();
    assert_eq!(*output, vec![8192]);
}

#[tokio::test]
async fn test_on_prepare_part_logical_tables_exist() {
    let datanode_manager = Arc::new(MockDatanodeManager::new(()));
    let ddl_context = new_ddl_context(datanode_manager);
    let cluster_id = 1;
    // Prepares physical table metadata.
    let mut create_physical_table_task = test_create_physical_table_task("phy_table");
    let TableMetadata {
        table_id,
        table_route,
        ..
    } = ddl_context
        .table_metadata_allocator
        .create(
            &TableMetadataAllocatorContext { cluster_id },
            &create_physical_table_task,
        )
        .await
        .unwrap();
    create_physical_table_task.set_table_id(table_id);
    create_physical_table_metadata(
        &ddl_context,
        create_physical_table_task.table_info.clone(),
        table_route,
    )
    .await;
    // Creates the logical table metadata.
    let mut task = test_create_logical_table_task("exists");
    task.set_table_id(8192);
    ddl_context
        .table_metadata_manager
        .create_logic_tables_metadata(vec![(
            task.table_info.clone(),
            TableRouteValue::logical(1024, vec![RegionId::new(8192, 1)]),
        )])
        .await
        .unwrap();
    // The create logical table procedure.
    let physical_table_id = table_id;
    // Sets `create_if_not_exists`
    task.create_table.create_if_not_exists = true;
    let non_exist_task = test_create_logical_table_task("non_exists");
    let mut procedure = CreateLogicalTablesProcedure::new(
        cluster_id,
        vec![task, non_exist_task],
        physical_table_id,
        ddl_context,
    );
    let status = procedure.on_prepare().await.unwrap();
    assert_matches!(status, Status::Executing { persist: true });
}

#[derive(Clone)]
pub struct NaiveDatanodeHandler;

#[async_trait::async_trait]
impl MockDatanodeHandler for NaiveDatanodeHandler {
    async fn handle(&self, peer: &Peer, request: RegionRequest) -> Result<AffectedRows> {
        debug!("Returning Ok(0) for request: {request:?}, peer: {peer:?}");
        Ok(0)
    }

    async fn handle_query(
        &self,
        _peer: &Peer,
        _request: QueryRequest,
    ) -> Result<SendableRecordBatchStream> {
        unreachable!()
    }
}

#[tokio::test]
async fn test_on_create_metadata() {
    let datanode_manager = Arc::new(MockDatanodeManager::new(NaiveDatanodeHandler));
    let ddl_context = new_ddl_context(datanode_manager);
    let cluster_id = 1;
    // Prepares physical table metadata.
    let mut create_physical_table_task = test_create_physical_table_task("phy_table");
    let TableMetadata {
        table_id,
        table_route,
        ..
    } = ddl_context
        .table_metadata_allocator
        .create(
            &TableMetadataAllocatorContext { cluster_id },
            &create_physical_table_task,
        )
        .await
        .unwrap();
    create_physical_table_task.set_table_id(table_id);
    create_physical_table_metadata(
        &ddl_context,
        create_physical_table_task.table_info.clone(),
        table_route,
    )
    .await;
    // The create logical table procedure.
    let physical_table_id = table_id;
    // Creates the logical table metadata.
    let task = test_create_logical_table_task("foo");
    let yet_another_task = test_create_logical_table_task("bar");
    let mut procedure = CreateLogicalTablesProcedure::new(
        cluster_id,
        vec![task, yet_another_task],
        physical_table_id,
        ddl_context,
    );
    let status = procedure.on_prepare().await.unwrap();
    assert_matches!(status, Status::Executing { persist: true });
    let ctx = ProcedureContext {
        procedure_id: ProcedureId::random(),
        provider: Arc::new(MockContextProvider::default()),
    };
    procedure.execute(&ctx).await.unwrap();
    // Triggers procedure to create table metadata
    let status = procedure.execute(&ctx).await.unwrap();
    let table_ids = status.downcast_output_ref::<Vec<u32>>().unwrap();
    assert_eq!(*table_ids, vec![1025, 1026]);
}

#[tokio::test]
async fn test_on_create_metadata_part_logical_tables_exist() {
    let datanode_manager = Arc::new(MockDatanodeManager::new(NaiveDatanodeHandler));
    let ddl_context = new_ddl_context(datanode_manager);
    let cluster_id = 1;
    // Prepares physical table metadata.
    let mut create_physical_table_task = test_create_physical_table_task("phy_table");
    let TableMetadata {
        table_id,
        table_route,
        ..
    } = ddl_context
        .table_metadata_allocator
        .create(
            &TableMetadataAllocatorContext { cluster_id },
            &create_physical_table_task,
        )
        .await
        .unwrap();
    create_physical_table_task.set_table_id(table_id);
    create_physical_table_metadata(
        &ddl_context,
        create_physical_table_task.table_info.clone(),
        table_route,
    )
    .await;
    // Creates the logical table metadata.
    let mut task = test_create_logical_table_task("exists");
    task.set_table_id(8192);
    ddl_context
        .table_metadata_manager
        .create_logic_tables_metadata(vec![(
            task.table_info.clone(),
            TableRouteValue::logical(1024, vec![RegionId::new(8192, 1)]),
        )])
        .await
        .unwrap();
    // The create logical table procedure.
    let physical_table_id = table_id;
    // Sets `create_if_not_exists`
    task.create_table.create_if_not_exists = true;
    let non_exist_task = test_create_logical_table_task("non_exists");
    let mut procedure = CreateLogicalTablesProcedure::new(
        cluster_id,
        vec![task, non_exist_task],
        physical_table_id,
        ddl_context,
    );
    let status = procedure.on_prepare().await.unwrap();
    assert_matches!(status, Status::Executing { persist: true });
    let ctx = ProcedureContext {
        procedure_id: ProcedureId::random(),
        provider: Arc::new(MockContextProvider::default()),
    };
    procedure.execute(&ctx).await.unwrap();
    // Triggers procedure to create table metadata
    let status = procedure.execute(&ctx).await.unwrap();
    let table_ids = status.downcast_output_ref::<Vec<u32>>().unwrap();
    assert_eq!(*table_ids, vec![8192, 1025]);
}

#[tokio::test]
async fn test_on_create_metadata_err() {
    let datanode_manager = Arc::new(MockDatanodeManager::new(NaiveDatanodeHandler));
    let ddl_context = new_ddl_context(datanode_manager);
    let cluster_id = 1;
    // Prepares physical table metadata.
    let mut create_physical_table_task = test_create_physical_table_task("phy_table");
    let TableMetadata {
        table_id,
        table_route,
        ..
    } = ddl_context
        .table_metadata_allocator
        .create(
            &TableMetadataAllocatorContext { cluster_id },
            &create_physical_table_task,
        )
        .await
        .unwrap();
    create_physical_table_task.set_table_id(table_id);
    create_physical_table_metadata(
        &ddl_context,
        create_physical_table_task.table_info.clone(),
        table_route,
    )
    .await;
    // The create logical table procedure.
    let physical_table_id = table_id;
    // Creates the logical table metadata.
    let task = test_create_logical_table_task("foo");
    let yet_another_task = test_create_logical_table_task("bar");
    let mut procedure = CreateLogicalTablesProcedure::new(
        cluster_id,
        vec![task.clone(), yet_another_task],
        physical_table_id,
        ddl_context.clone(),
    );
    let status = procedure.on_prepare().await.unwrap();
    assert_matches!(status, Status::Executing { persist: true });
    let ctx = ProcedureContext {
        procedure_id: ProcedureId::random(),
        provider: Arc::new(MockContextProvider::default()),
    };
    procedure.execute(&ctx).await.unwrap();
    // Creates logical table metadata(different with the task)
    let mut task = task.clone();
    task.table_info.ident.table_id = 1025;
    ddl_context
        .table_metadata_manager
        .create_logic_tables_metadata(vec![(
            task.table_info,
            TableRouteValue::logical(512, vec![RegionId::new(1026, 1)]),
        )])
        .await
        .unwrap();
    // Triggers procedure to create table metadata
    let error = procedure.execute(&ctx).await.unwrap_err();
    assert!(!error.is_retry_later());
}
