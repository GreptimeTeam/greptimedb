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

use api::v1::region::{region_request, RegionRequest};
use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
use common_error::ext::ErrorExt;
use common_error::status_code::StatusCode;
use common_procedure::{Context as ProcedureContext, Procedure, ProcedureId};
use common_procedure_test::MockContextProvider;
use store_api::storage::RegionId;
use tokio::sync::mpsc;

use crate::ddl::create_logical_tables::CreateLogicalTablesProcedure;
use crate::ddl::drop_table::DropTableProcedure;
use crate::ddl::test_util::create_table::test_create_table_task;
use crate::ddl::test_util::datanode_handler::{DatanodeWatcher, NaiveDatanodeHandler};
use crate::ddl::test_util::{
    create_physical_table_metadata, test_create_logical_table_task, test_create_physical_table_task,
};
use crate::ddl::{TableMetadata, TableMetadataAllocatorContext};
use crate::key::table_route::TableRouteValue;
use crate::kv_backend::memory::MemoryKvBackend;
use crate::peer::Peer;
use crate::rpc::ddl::DropTableTask;
use crate::rpc::router::{Region, RegionRoute};
use crate::test_util::{new_ddl_context, new_ddl_context_with_kv_backend, MockDatanodeManager};

#[tokio::test]
async fn test_on_prepare_table_not_exists_err() {
    let datanode_manager = Arc::new(MockDatanodeManager::new(()));
    let ddl_context = new_ddl_context(datanode_manager);
    let cluster_id = 1;
    let table_name = "foo";
    let table_id = 1024;
    let task = test_create_table_task(table_name, table_id);
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

    let task = DropTableTask {
        catalog: DEFAULT_CATALOG_NAME.to_string(),
        schema: DEFAULT_SCHEMA_NAME.to_string(),
        table: "bar".to_string(),
        table_id,
        drop_if_exists: false,
    };

    let mut procedure = DropTableProcedure::new(cluster_id, task, ddl_context);
    let err = procedure.on_prepare().await.unwrap_err();
    assert_eq!(err.status_code(), StatusCode::TableNotFound);
}

#[tokio::test]
async fn test_on_prepare_table() {
    let datanode_manager = Arc::new(MockDatanodeManager::new(()));
    let ddl_context = new_ddl_context(datanode_manager);
    let cluster_id = 1;
    let table_name = "foo";
    let table_id = 1024;
    let task = test_create_table_task(table_name, table_id);
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

    let task = DropTableTask {
        catalog: DEFAULT_CATALOG_NAME.to_string(),
        schema: DEFAULT_SCHEMA_NAME.to_string(),
        table: "bar".to_string(),
        table_id,
        drop_if_exists: true,
    };

    // Drop if exists
    let mut procedure = DropTableProcedure::new(cluster_id, task, ddl_context.clone());
    procedure.on_prepare().await.unwrap();

    let task = DropTableTask {
        catalog: DEFAULT_CATALOG_NAME.to_string(),
        schema: DEFAULT_SCHEMA_NAME.to_string(),
        table: table_name.to_string(),
        table_id,
        drop_if_exists: false,
    };

    // Drop table
    let mut procedure = DropTableProcedure::new(cluster_id, task, ddl_context);
    procedure.on_prepare().await.unwrap();
}

#[tokio::test]
async fn test_on_datanode_drop_regions() {
    let (tx, mut rx) = mpsc::channel(8);
    let datanode_handler = DatanodeWatcher(tx);
    let datanode_manager = Arc::new(MockDatanodeManager::new(datanode_handler));
    let ddl_context = new_ddl_context(datanode_manager);
    let cluster_id = 1;
    let table_id = 1024;
    let table_name = "foo";
    let task = test_create_table_task(table_name, table_id);
    // Puts a value to table name key.
    ddl_context
        .table_metadata_manager
        .create_table_metadata(
            task.table_info.clone(),
            TableRouteValue::physical(vec![
                RegionRoute {
                    region: Region::new_test(RegionId::new(table_id, 1)),
                    leader_peer: Some(Peer::empty(1)),
                    follower_peers: vec![Peer::empty(5)],
                    leader_status: None,
                    leader_down_since: None,
                },
                RegionRoute {
                    region: Region::new_test(RegionId::new(table_id, 2)),
                    leader_peer: Some(Peer::empty(2)),
                    follower_peers: vec![Peer::empty(4)],
                    leader_status: None,
                    leader_down_since: None,
                },
                RegionRoute {
                    region: Region::new_test(RegionId::new(table_id, 3)),
                    leader_peer: Some(Peer::empty(3)),
                    follower_peers: vec![],
                    leader_status: None,
                    leader_down_since: None,
                },
            ]),
            HashMap::new(),
        )
        .await
        .unwrap();

    let task = DropTableTask {
        catalog: DEFAULT_CATALOG_NAME.to_string(),
        schema: DEFAULT_SCHEMA_NAME.to_string(),
        table: table_name.to_string(),
        table_id,
        drop_if_exists: false,
    };
    // Drop table
    let mut procedure = DropTableProcedure::new(cluster_id, task, ddl_context);
    procedure.on_prepare().await.unwrap();
    procedure.on_datanode_drop_regions().await.unwrap();

    let check = |peer: Peer,
                 request: RegionRequest,
                 expected_peer_id: u64,
                 expected_region_id: RegionId| {
        assert_eq!(peer.id, expected_peer_id);
        let Some(region_request::Body::Drop(req)) = request.body else {
            unreachable!();
        };
        assert_eq!(req.region_id, expected_region_id);
    };

    let mut results = Vec::new();
    for _ in 0..3 {
        let result = rx.try_recv().unwrap();
        results.push(result);
    }
    results.sort_unstable_by(|(a, _), (b, _)| a.id.cmp(&b.id));

    let (peer, request) = results.remove(0);
    check(peer, request, 1, RegionId::new(table_id, 1));
    let (peer, request) = results.remove(0);
    check(peer, request, 2, RegionId::new(table_id, 2));
    let (peer, request) = results.remove(0);
    check(peer, request, 3, RegionId::new(table_id, 3));
}

#[tokio::test]
async fn test_on_rollback() {
    let datanode_manager = Arc::new(MockDatanodeManager::new(NaiveDatanodeHandler));
    let kv_backend = Arc::new(MemoryKvBackend::new());
    let ddl_context = new_ddl_context_with_kv_backend(datanode_manager, kv_backend.clone());
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
        TableRouteValue::Physical(table_route),
    )
    .await;
    // The create logical table procedure.
    let physical_table_id = table_id;
    // Creates the logical table metadata.
    let task = test_create_logical_table_task("foo");
    let mut procedure = CreateLogicalTablesProcedure::new(
        cluster_id,
        vec![task],
        physical_table_id,
        ddl_context.clone(),
    );
    procedure.on_prepare().await.unwrap();
    let ctx = ProcedureContext {
        procedure_id: ProcedureId::random(),
        provider: Arc::new(MockContextProvider::default()),
    };
    procedure.execute(&ctx).await.unwrap();
    // Triggers procedure to create table metadata
    let status = procedure.execute(&ctx).await.unwrap();
    let table_ids = status.downcast_output_ref::<Vec<u32>>().unwrap();
    assert_eq!(*table_ids, vec![1025]);

    let expected_kvs = kv_backend.dump();
    // Drops the physical table
    {
        let task = DropTableTask {
            catalog: DEFAULT_CATALOG_NAME.to_string(),
            schema: DEFAULT_SCHEMA_NAME.to_string(),
            table: "phy_table".to_string(),
            table_id: physical_table_id,
            drop_if_exists: false,
        };
        let mut procedure = DropTableProcedure::new(cluster_id, task, ddl_context.clone());
        procedure.on_prepare().await.unwrap();
        procedure.on_remove_metadata().await.unwrap();
        let ctx = ProcedureContext {
            procedure_id: ProcedureId::random(),
            provider: Arc::new(MockContextProvider::default()),
        };
        procedure.rollback(&ctx).await.unwrap();
        // Rollback again
        procedure.rollback(&ctx).await.unwrap();
        let kvs = kv_backend.dump();
        assert_eq!(kvs, expected_kvs);
    }

    // Drops the logical table
    let task = DropTableTask {
        catalog: DEFAULT_CATALOG_NAME.to_string(),
        schema: DEFAULT_SCHEMA_NAME.to_string(),
        table: "foo".to_string(),
        table_id: table_ids[0],
        drop_if_exists: false,
    };
    let mut procedure = DropTableProcedure::new(cluster_id, task, ddl_context.clone());
    procedure.on_prepare().await.unwrap();
    procedure.on_remove_metadata().await.unwrap();
    let ctx = ProcedureContext {
        procedure_id: ProcedureId::random(),
        provider: Arc::new(MockContextProvider::default()),
    };
    procedure.rollback(&ctx).await.unwrap();
    // Rollback again
    procedure.rollback(&ctx).await.unwrap();
    let kvs = kv_backend.dump();
    assert_eq!(kvs, expected_kvs);
}
