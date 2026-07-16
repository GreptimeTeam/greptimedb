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
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use api::v1::region::{RegionRequest, region_request};
use async_trait::async_trait;
use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, FILE_ENGINE};
use common_error::ext::ErrorExt;
use common_error::status_code::StatusCode;
use common_procedure::{Procedure, StringKey};
use common_procedure_test::{
    execute_procedure_until, execute_procedure_until_done, new_test_procedure_context,
};
use store_api::region_engine::RegionRole;
use store_api::storage::RegionId;
use table::metadata::TableId;
use tokio::sync::{Mutex, mpsc};

use crate::ddl::create_logical_tables::CreateLogicalTablesProcedure;
use crate::ddl::create_table::CreateTableProcedure;
use crate::ddl::drop_table::{DropTableProcedure, DropTableState};
use crate::ddl::purge_dropped_table::PurgeDroppedTableProcedure;
use crate::ddl::test_util::create_table::test_create_table_task;
use crate::ddl::test_util::datanode_handler::{DatanodeWatcher, NaiveDatanodeHandler};
use crate::ddl::test_util::{
    create_logical_table, create_physical_table, create_physical_table_metadata,
    put_datanode_address, test_create_logical_table_task, test_create_physical_table_task,
};
use crate::ddl::undrop_table::UndropTableProcedure;
use crate::ddl::{DdlContext, DetectingRegion, RegionFailureDetectorController, TableMetadata};
use crate::error::Error;
use crate::key::MetadataKey;
use crate::key::table_name::TableNameKey;
use crate::key::table_route::TableRouteValue;
use crate::kv_backend::KvBackend;
use crate::kv_backend::memory::MemoryKvBackend;
use crate::peer::Peer;
use crate::rpc::ddl::{DropTableTask, PurgeDroppedTableTask, UndropTableTask};
use crate::rpc::router::{Region, RegionRoute};
use crate::rpc::store::{BatchDeleteRequest, PutRequest};
use crate::test_util::{MockDatanodeManager, new_ddl_context, new_ddl_context_with_kv_backend};

fn dropped_at_marker_key(table_id: TableId) -> String {
    format!("__tombstone/__dropped_at/{table_id}")
}

fn retention_expires_at_marker_key(table_id: TableId) -> String {
    format!("__tombstone/__retention_expires_at/{table_id}")
}

fn purging_marker_key(table_id: TableId) -> String {
    format!("__tombstone/__purging/{table_id}")
}

fn drop_generation_marker_key(table_id: TableId) -> String {
    format!("__tombstone/__drop_generation/{table_id}")
}

async fn create_dropped_table(
    context: &DdlContext,
    table_id: TableId,
    dropped_at: Option<i64>,
    retention_expires_at: Option<i64>,
) {
    let task = test_create_table_task("foo", table_id);
    let table_name = task.table_name();
    let table_route = TableRouteValue::physical(vec![]);
    context
        .table_metadata_manager
        .create_table_metadata(task.table_info, table_route.clone(), HashMap::new())
        .await
        .unwrap();
    context
        .table_metadata_manager
        .delete_table_metadata_with_retention(
            table_id,
            &table_name,
            &table_route,
            &HashMap::new(),
            dropped_at,
            retention_expires_at,
        )
        .await
        .unwrap();
}

async fn undrop_at_restore_metadata(
    context: &DdlContext,
    table_id: TableId,
) -> UndropTableProcedure {
    create_dropped_table(context, table_id, Some(1), Some(i64::MAX)).await;
    let mut undrop = UndropTableProcedure::new(new_undrop_table_task(table_id), context.clone());
    let procedure_context = new_test_procedure_context();
    undrop.execute(&procedure_context).await.unwrap();
    undrop.execute(&procedure_context).await.unwrap();
    undrop
}

fn legacy_undrop_snapshot(procedure: &UndropTableProcedure) -> String {
    let mut data: serde_json::Value = serde_json::from_str(&procedure.dump().unwrap()).unwrap();
    let data = data.as_object_mut().unwrap();
    for field in [
        "dropped_at",
        "retention_expires_at",
        "drop_generation",
        "tombstone_identity_loaded",
    ] {
        data.remove(field);
    }
    serde_json::to_string(data).unwrap()
}

#[test]
fn test_old_drop_table_json_defaults_to_hard_drop() {
    let mut runtime_context = new_ddl_context(Arc::new(MockDatanodeManager::new(())));
    runtime_context.soft_drop_enabled = true;
    let procedure = DropTableProcedure::new(
        new_drop_table_task("foo", 1024, false),
        runtime_context.clone(),
    );
    let mut old_data: serde_json::Value = serde_json::from_str(&procedure.dump().unwrap()).unwrap();
    old_data
        .as_object_mut()
        .unwrap()
        .remove("soft_drop_enabled");
    old_data.as_object_mut().unwrap().remove("dropped_at");

    let recovered = DropTableProcedure::from_json(&old_data.to_string(), runtime_context).unwrap();
    let recovered_data: serde_json::Value =
        serde_json::from_str(&recovered.dump().unwrap()).unwrap();

    assert_eq!(recovered_data["soft_drop_enabled"], false);
    assert_eq!(recovered_data["dropped_at"], serde_json::Value::Null);
}

#[test]
fn test_drop_table_lock_key_includes_table_name() {
    let context = new_ddl_context(Arc::new(MockDatanodeManager::new(())));
    let procedure = DropTableProcedure::new(new_drop_table_task("foo", 1024, false), context);

    let keys = procedure
        .lock_key()
        .keys_to_lock()
        .cloned()
        .collect::<Vec<_>>();
    assert!(
        keys.iter().any(|key| matches!(
            key,
            StringKey::Exclusive(key) if key == "__table_name_lock/greptime.public.foo"
        )),
        "drop lock keys should include the table name: {keys:?}"
    );
    assert!(
        keys.iter().any(|key| matches!(
            key,
            StringKey::Exclusive(key) if key == "__table_lock/1024"
        )),
        "drop lock keys should include the table id: {keys:?}"
    );
}

#[tokio::test]
async fn test_recovered_soft_drop_preserves_persisted_mode() {
    let (tx, mut rx) = mpsc::channel(8);
    let node_manager = Arc::new(MockDatanodeManager::new(DatanodeWatcher::new(tx)));
    let kv_backend = Arc::new(MemoryKvBackend::new());
    let mut soft_context =
        new_ddl_context_with_kv_backend(node_manager.clone(), kv_backend.clone());
    soft_context.soft_drop_enabled = true;
    let table_id = 1024;
    let task = test_create_table_task("foo", table_id);
    soft_context
        .table_metadata_manager
        .create_table_metadata(
            task.table_info,
            TableRouteValue::physical(vec![RegionRoute {
                region: Region::new_test(RegionId::new(table_id, 1)),
                leader_peer: Some(Peer::empty(1)),
                follower_peers: vec![],
                leader_state: None,
                leader_down_since: None,
                write_route_policy: None,
            }]),
            HashMap::new(),
        )
        .await
        .unwrap();
    let procedure = DropTableProcedure::new(
        new_drop_table_task("foo", table_id, false),
        soft_context.clone(),
    );

    let hard_context = new_ddl_context_with_kv_backend(node_manager, kv_backend);
    let mut recovered =
        DropTableProcedure::from_json(&procedure.dump().unwrap(), hard_context).unwrap();
    execute_procedure_until_done(&mut recovered).await;

    assert!(
        soft_context
            .table_metadata_manager
            .get_dropped_table_by_id(table_id)
            .await
            .unwrap()
            .is_some()
    );
    let (_, request) = rx.try_recv().unwrap();
    assert_matches!(request.body, Some(region_request::Body::Close(_)));
}

#[tokio::test]
async fn test_legacy_soft_drop_prepare_preserves_persisted_mode() {
    let node_manager = Arc::new(MockDatanodeManager::new(()));
    let mut context = new_ddl_context(node_manager);
    context.soft_drop_enabled = false;
    context.soft_drop_retention = Some(Duration::from_millis(100));
    let table_id = 1024;
    let task = test_create_table_task("foo", table_id);
    context
        .table_metadata_manager
        .create_table_metadata(
            task.table_info,
            TableRouteValue::physical(vec![]),
            HashMap::new(),
        )
        .await
        .unwrap();

    let mut persisted: serde_json::Value = serde_json::from_str(
        &DropTableProcedure::new(new_drop_table_task("foo", table_id, false), context.clone())
            .dump()
            .unwrap(),
    )
    .unwrap();
    persisted["soft_drop_enabled"] = true.into();
    persisted
        .as_object_mut()
        .unwrap()
        .remove("soft_drop_retention_millis");
    persisted
        .as_object_mut()
        .unwrap()
        .remove("retention_expires_at");

    let mut recovered =
        DropTableProcedure::from_json(&persisted.to_string(), context.clone()).unwrap();
    recovered.on_prepare().await.unwrap();

    assert!(recovered.data.soft_drop_enabled);
    assert_eq!(Some(100), recovered.data.soft_drop_retention_millis);
    assert!(recovered.data.dropped_at.is_some());
    assert!(recovered.data.retention_expires_at.is_some());
}

#[tokio::test]
async fn test_disabled_soft_drop_creates_hard_drop_procedure() {
    let node_manager = Arc::new(MockDatanodeManager::new(()));
    let mut context = new_ddl_context(node_manager);
    context.soft_drop_enabled = false;
    context.soft_drop_retention = Some(Duration::from_millis(100));

    let procedure = DropTableProcedure::new(new_drop_table_task("foo", 1024, false), context);

    assert!(!procedure.data.soft_drop_enabled);
    assert_eq!(None, procedure.data.dropped_at);
    assert_eq!(None, procedure.data.retention_expires_at);
}

#[tokio::test]
async fn test_recovered_hard_drop_ignores_soft_runtime_context() {
    let (tx, mut rx) = mpsc::channel(8);
    let node_manager = Arc::new(MockDatanodeManager::new(DatanodeWatcher::new(tx)));
    let kv_backend = Arc::new(MemoryKvBackend::new());
    let hard_context = new_ddl_context_with_kv_backend(node_manager.clone(), kv_backend.clone());
    let table_id = 1024;
    let task = test_create_table_task("foo", table_id);
    hard_context
        .table_metadata_manager
        .create_table_metadata(
            task.table_info,
            TableRouteValue::physical(vec![RegionRoute {
                region: Region::new_test(RegionId::new(table_id, 1)),
                leader_peer: Some(Peer::empty(1)),
                follower_peers: vec![],
                leader_state: None,
                leader_down_since: None,
                write_route_policy: None,
            }]),
            HashMap::new(),
        )
        .await
        .unwrap();
    let procedure = DropTableProcedure::new(
        new_drop_table_task("foo", table_id, false),
        hard_context.clone(),
    );

    let mut soft_context = new_ddl_context_with_kv_backend(node_manager, kv_backend);
    soft_context.soft_drop_enabled = true;
    let mut recovered =
        DropTableProcedure::from_json(&procedure.dump().unwrap(), soft_context).unwrap();
    execute_procedure_until_done(&mut recovered).await;

    assert!(
        hard_context
            .table_metadata_manager
            .get_dropped_table_by_id(table_id)
            .await
            .unwrap()
            .is_none()
    );
    let (_, request) = rx.try_recv().unwrap();
    assert_matches!(request.body, Some(region_request::Body::Drop(_)));
}

#[tokio::test]
async fn test_on_prepare_table_not_exists_err() {
    let node_manager = Arc::new(MockDatanodeManager::new(()));
    let mut ddl_context = new_ddl_context(node_manager);
    ddl_context.soft_drop_enabled = true;
    ddl_context.soft_drop_retention = Some(Duration::from_millis(100));
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

    let task = new_drop_table_task("bar", table_id, false);
    let mut procedure = DropTableProcedure::new(task, ddl_context);
    assert_eq!(procedure.data.dropped_at, None);
    assert_eq!(procedure.data.retention_expires_at, None);
    let err = procedure.on_prepare().await.unwrap_err();
    assert_eq!(err.status_code(), StatusCode::TableNotFound);
    assert_eq!(procedure.data.dropped_at, None);
    assert_eq!(procedure.data.retention_expires_at, None);
}

#[tokio::test]
async fn test_soft_drop_prepare_assigns_stable_deadline_and_recovers_it() {
    let node_manager = Arc::new(MockDatanodeManager::new(()));
    let mut ddl_context = new_ddl_context(node_manager);
    ddl_context.soft_drop_enabled = true;
    ddl_context.soft_drop_retention = Some(Duration::from_millis(100));
    let table_id = 1024;
    let task = test_create_table_task("foo", table_id);
    ddl_context
        .table_metadata_manager
        .create_table_metadata(
            task.table_info,
            TableRouteValue::physical(vec![]),
            HashMap::new(),
        )
        .await
        .unwrap();
    let mut procedure = DropTableProcedure::new(
        new_drop_table_task("foo", table_id, false),
        ddl_context.clone(),
    );

    procedure.on_prepare().await.unwrap();
    let dropped_at = procedure.data.dropped_at.unwrap();
    let deadline = procedure.data.retention_expires_at.unwrap();
    assert_eq!(deadline, dropped_at + 100);

    let mut changed_context = ddl_context;
    changed_context.soft_drop_retention = Some(Duration::from_secs(10));
    let recovered =
        DropTableProcedure::from_json(&procedure.dump().unwrap(), changed_context).unwrap();
    assert_eq!(recovered.data.dropped_at, Some(dropped_at));
    assert_eq!(recovered.data.retention_expires_at, Some(deadline));
}

#[tokio::test]
async fn test_soft_drop_prepare_rejects_deadline_overflow_before_metadata_delete() {
    let node_manager = Arc::new(MockDatanodeManager::new(()));
    let mut ddl_context = new_ddl_context(node_manager);
    ddl_context.soft_drop_enabled = true;
    ddl_context.soft_drop_retention = Some(Duration::from_millis(i64::MAX as u64));
    let table_id = 1024;
    let task = test_create_table_task("foo", table_id);
    ddl_context
        .table_metadata_manager
        .create_table_metadata(
            task.table_info,
            TableRouteValue::physical(vec![]),
            HashMap::new(),
        )
        .await
        .unwrap();
    let mut procedure = DropTableProcedure::new(
        new_drop_table_task("foo", table_id, false),
        ddl_context.clone(),
    );

    let error = procedure.on_prepare().await.unwrap_err();
    assert!(error.to_string().contains("retention deadline"), "{error}");
    assert!(
        ddl_context
            .table_metadata_manager
            .table_name_manager()
            .get(TableNameKey::new(
                DEFAULT_CATALOG_NAME,
                DEFAULT_SCHEMA_NAME,
                "foo",
            ))
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn test_on_prepare_table() {
    let node_manager = Arc::new(MockDatanodeManager::new(()));
    let ddl_context = new_ddl_context(node_manager);
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

    let task = new_drop_table_task("bar", table_id, true);
    // Drop if exists
    let mut procedure = DropTableProcedure::new(task, ddl_context.clone());
    procedure.on_prepare().await.unwrap();
    assert!(!procedure.rollback_supported());

    let task = new_drop_table_task(table_name, table_id, false);
    // Drop table
    let mut procedure = DropTableProcedure::new(task, ddl_context);
    procedure.on_prepare().await.unwrap();
}

#[tokio::test]
async fn test_on_datanode_drop_regions() {
    let (tx, mut rx) = mpsc::channel(8);
    let datanode_handler = DatanodeWatcher::new(tx);
    let node_manager = Arc::new(MockDatanodeManager::new(datanode_handler));
    let ddl_context = new_ddl_context(node_manager);
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
                    leader_state: None,
                    leader_down_since: None,
                    write_route_policy: None,
                },
                RegionRoute {
                    region: Region::new_test(RegionId::new(table_id, 2)),
                    leader_peer: Some(Peer::empty(2)),
                    follower_peers: vec![Peer::empty(4)],
                    leader_state: None,
                    leader_down_since: None,
                    write_route_policy: None,
                },
                RegionRoute {
                    region: Region::new_test(RegionId::new(table_id, 3)),
                    leader_peer: Some(Peer::empty(3)),
                    follower_peers: vec![],
                    leader_state: None,
                    leader_down_since: None,
                    write_route_policy: None,
                },
            ]),
            HashMap::new(),
        )
        .await
        .unwrap();

    let task = new_drop_table_task(table_name, table_id, false);
    // Drop table
    let mut procedure = DropTableProcedure::new(task, ddl_context);
    procedure.on_prepare().await.unwrap();
    procedure.on_datanode_drop_regions(false).await.unwrap();

    let check = |peer: Peer,
                 request: RegionRequest,
                 expected_peer_id: u64,
                 expected_region_id: RegionId,
                 follower: bool| {
        assert_eq!(peer.id, expected_peer_id);
        if follower {
            let Some(region_request::Body::Close(req)) = request.body else {
                unreachable!();
            };
            assert_eq!(req.region_id, expected_region_id);
        } else {
            let Some(region_request::Body::Drop(req)) = request.body else {
                unreachable!();
            };
            assert_eq!(req.region_id, expected_region_id);
        };
    };

    let mut results = Vec::new();
    for _ in 0..5 {
        let result = rx.try_recv().unwrap();
        results.push(result);
    }
    results.sort_unstable_by_key(|(a, _)| a.id);

    let (peer, request) = results.remove(0);
    check(peer, request, 1, RegionId::new(table_id, 1), false);
    let (peer, request) = results.remove(0);
    check(peer, request, 2, RegionId::new(table_id, 2), false);
    let (peer, request) = results.remove(0);
    check(peer, request, 3, RegionId::new(table_id, 3), false);
    let (peer, request) = results.remove(0);
    check(peer, request, 4, RegionId::new(table_id, 2), true);
    let (peer, request) = results.remove(0);
    check(peer, request, 5, RegionId::new(table_id, 1), true);
}

#[tokio::test]
async fn test_on_datanode_drop_regions_remaps_addresses_when_retrying() {
    let (tx, mut rx) = mpsc::channel(8);
    let datanode_handler = DatanodeWatcher::new(tx);
    let node_manager = Arc::new(MockDatanodeManager::new(datanode_handler));
    let ddl_context = new_ddl_context(node_manager);
    let table_id = 1024;
    let table_name = "foo";
    let task = test_create_table_task(table_name, table_id);
    ddl_context
        .table_metadata_manager
        .create_table_metadata(
            task.table_info.clone(),
            TableRouteValue::physical(vec![RegionRoute {
                region: Region::new_test(RegionId::new(table_id, 1)),
                leader_peer: Some(Peer::new(1, "old-leader")),
                follower_peers: vec![Peer::new(5, "old-follower")],
                leader_state: None,
                leader_down_since: None,
                write_route_policy: None,
            }]),
            HashMap::new(),
        )
        .await
        .unwrap();

    let task = new_drop_table_task(table_name, table_id, false);
    let mut procedure = DropTableProcedure::new(task, ddl_context.clone());
    procedure.on_prepare().await.unwrap();

    put_datanode_address(&ddl_context, 1, "new-leader").await;
    put_datanode_address(&ddl_context, 5, "new-follower").await;

    procedure.on_datanode_drop_regions(true).await.unwrap();

    let mut peers = Vec::new();
    for _ in 0..2 {
        peers.push(rx.try_recv().unwrap().0);
    }
    peers.sort_unstable_by_key(|p| p.id);
    assert_eq!(peers[0].addr, "new-leader");
    assert_eq!(peers[1].addr, "new-follower");
}

#[tokio::test]
async fn test_soft_drop_closes_regions_and_keeps_tombstone() {
    let (tx, mut rx) = mpsc::channel(8);
    let datanode_handler = DatanodeWatcher::new(tx);
    let node_manager = Arc::new(MockDatanodeManager::new(datanode_handler));
    let detector_controller = Arc::new(RecordingRegionFailureDetectorController::default());
    let mut ddl_context = new_ddl_context(node_manager);
    ddl_context.soft_drop_enabled = true;
    ddl_context.soft_drop_retention = Some(Duration::from_millis(100));
    ddl_context.region_failure_detector_controller = detector_controller.clone();
    let table_id = 1024;
    let table_name = "foo";
    let task = test_create_table_task(table_name, table_id);
    ddl_context
        .table_metadata_manager
        .create_table_metadata(
            task.table_info.clone(),
            TableRouteValue::physical(vec![
                RegionRoute {
                    region: Region::new_test(RegionId::new(table_id, 1)),
                    leader_peer: Some(Peer::empty(1)),
                    follower_peers: vec![Peer::empty(2)],
                    leader_state: None,
                    leader_down_since: None,
                    write_route_policy: None,
                },
                RegionRoute {
                    region: Region::new_test(RegionId::new(table_id, 2)),
                    leader_peer: Some(Peer::empty(2)),
                    follower_peers: vec![Peer::empty(1)],
                    leader_state: None,
                    leader_down_since: None,
                    write_route_policy: None,
                },
            ]),
            HashMap::new(),
        )
        .await
        .unwrap();

    let task = new_drop_table_task(table_name, table_id, false);
    let mut procedure = DropTableProcedure::new(task, ddl_context.clone());

    execute_procedure_until_done(&mut procedure).await;

    assert!(procedure.dropping_regions.is_empty());
    assert_eq!(ddl_context.memory_region_keeper.len(), 0);

    let mut requests = Vec::new();
    for _ in 0..4 {
        let (peer, request) = rx.try_recv().unwrap();
        let Some(region_request::Body::Close(req)) = request.body else {
            unreachable!();
        };
        requests.push((peer.id, req.region_id, req.flush_on_close));
    }
    requests.sort_unstable();
    assert_eq!(
        requests,
        vec![
            (1, RegionId::new(table_id, 1).as_u64(), true),
            (1, RegionId::new(table_id, 2).as_u64(), false),
            (2, RegionId::new(table_id, 1).as_u64(), false),
            (2, RegionId::new(table_id, 2).as_u64(), true),
        ]
    );
    assert!(rx.try_recv().is_err());

    let table_name = procedure.data.task.table_name();
    let live_table = ddl_context
        .table_metadata_manager
        .table_name_manager()
        .get(TableNameKey::from(&table_name))
        .await
        .unwrap();
    assert!(live_table.is_none());

    let dropped_at = procedure.data.dropped_at.unwrap();
    let dropped_table = ddl_context
        .table_metadata_manager
        .get_dropped_table(&table_name)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(dropped_table.dropped_at, Some(dropped_at));
    assert_eq!(dropped_table.retention_expires_at, Some(dropped_at + 100));
    let dropped_tables = ddl_context
        .table_metadata_manager
        .list_dropped_tables()
        .await
        .unwrap();
    assert_eq!(dropped_tables[0].dropped_at, Some(dropped_at));
    assert_eq!(
        dropped_tables[0].retention_expires_at,
        Some(dropped_at + 100)
    );

    assert!(
        ddl_context
            .table_metadata_manager
            .kv_backend()
            .get(retention_expires_at_marker_key(table_id).as_bytes(),)
            .await
            .unwrap()
            .is_some()
    );

    assert_eq!(
        detector_controller.deregistered().await,
        vec![
            (1, RegionId::new(table_id, 1)),
            (2, RegionId::new(table_id, 2))
        ]
    );
}

#[tokio::test]
async fn test_soft_drop_timestamp_is_stable_across_retry_and_recovery() {
    let node_manager = Arc::new(MockDatanodeManager::new(NaiveDatanodeHandler));
    let kv_backend = Arc::new(MemoryKvBackend::new());
    let mut ddl_context = new_ddl_context_with_kv_backend(node_manager, kv_backend.clone());
    ddl_context.soft_drop_enabled = true;
    let table_id = 1024;
    let task = test_create_table_task("foo", table_id);
    ddl_context
        .table_metadata_manager
        .create_table_metadata(
            task.table_info,
            TableRouteValue::physical(vec![]),
            HashMap::new(),
        )
        .await
        .unwrap();
    let procedure = DropTableProcedure::new(
        new_drop_table_task("foo", table_id, false),
        ddl_context.clone(),
    );
    assert_eq!(procedure.data.dropped_at, None);
    let mut procedure = procedure;
    procedure.on_prepare().await.unwrap();
    let dropped_at = procedure.data.dropped_at.unwrap();
    let persisted = procedure.dump().unwrap();
    let recovered = DropTableProcedure::from_json(&persisted, ddl_context.clone()).unwrap();
    assert_eq!(recovered.data.dropped_at, Some(dropped_at));
    let mut data: serde_json::Value = serde_json::from_str(&procedure.dump().unwrap()).unwrap();
    data["dropped_at"] = 1_234_567_890_i64.into();
    let mut recovered =
        DropTableProcedure::from_json(&data.to_string(), ddl_context.clone()).unwrap();

    recovered.on_delete_metadata().await.unwrap();
    recovered.on_delete_metadata().await.unwrap();
    let persisted = recovered.dump().unwrap();
    let mut recovered_again =
        DropTableProcedure::from_json(&persisted, ddl_context.clone()).unwrap();
    recovered_again.on_delete_metadata().await.unwrap();

    let marker = kv_backend
        .get(dropped_at_marker_key(table_id).as_bytes())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(marker.value, b"1234567890");
    let deadline_marker = kv_backend
        .get(retention_expires_at_marker_key(table_id).as_bytes())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        deadline_marker.value,
        recovered_again
            .data
            .retention_expires_at
            .unwrap()
            .to_string()
            .as_bytes()
    );
    assert_eq!(recovered_again.data.dropped_at, Some(1_234_567_890));
}

#[tokio::test]
async fn test_hard_drop_keeps_delete_tombstone_flow() {
    let node_manager = Arc::new(MockDatanodeManager::new(NaiveDatanodeHandler));
    let detector_controller = Arc::new(RecordingRegionFailureDetectorController::default());
    let mut ddl_context = new_ddl_context(node_manager);
    ddl_context.region_failure_detector_controller = detector_controller.clone();
    let table_id = 1024;
    let table_name = "foo";
    let task = test_create_table_task(table_name, table_id);
    ddl_context
        .table_metadata_manager
        .create_table_metadata(
            task.table_info.clone(),
            TableRouteValue::physical(vec![RegionRoute {
                region: Region::new_test(RegionId::new(table_id, 1)),
                leader_peer: Some(Peer::empty(1)),
                follower_peers: vec![],
                leader_state: None,
                leader_down_since: None,
                write_route_policy: None,
            }]),
            HashMap::new(),
        )
        .await
        .unwrap();

    let task = new_drop_table_task(table_name, table_id, false);
    let mut procedure = DropTableProcedure::new(task, ddl_context.clone());

    execute_procedure_until(&mut procedure, |p| {
        p.data.state == DropTableState::DeleteTombstone
    })
    .await;

    assert_eq!(procedure.data.state, DropTableState::DeleteTombstone);

    execute_procedure_until_done(&mut procedure).await;

    let dropped_table = ddl_context
        .table_metadata_manager
        .get_dropped_table(&procedure.data.task.table_name())
        .await
        .unwrap();
    assert!(dropped_table.is_none());
    assert_eq!(
        detector_controller.deregistered().await,
        vec![(1, RegionId::new(table_id, 1))]
    );
}

#[tokio::test]
async fn test_create_table_succeeds_while_tombstone_exists() {
    let node_manager = Arc::new(MockDatanodeManager::new(NaiveDatanodeHandler));
    let mut ddl_context = new_ddl_context(node_manager);
    ddl_context.soft_drop_enabled = true;
    let dropped_table_id = 1024;
    let table_name = "foo";
    let task = test_create_table_task(table_name, dropped_table_id);
    ddl_context
        .table_metadata_manager
        .create_table_metadata(
            task.table_info.clone(),
            TableRouteValue::physical(vec![]),
            HashMap::new(),
        )
        .await
        .unwrap();

    let drop_task = new_drop_table_task(table_name, dropped_table_id, false);
    let mut drop_procedure = DropTableProcedure::new(drop_task, ddl_context.clone());
    execute_procedure_until_done(&mut drop_procedure).await;

    let mut create_task = test_create_table_task(table_name, 1025);
    create_task.create_table.table_id = None;
    create_task.table_info.ident.table_id = 0;
    let mut create_procedure = CreateTableProcedure::new(create_task, ddl_context.clone()).unwrap();
    execute_procedure_until_done(&mut create_procedure).await;

    let live_table = ddl_context
        .table_metadata_manager
        .table_name_manager()
        .get(TableNameKey::new(
            DEFAULT_CATALOG_NAME,
            DEFAULT_SCHEMA_NAME,
            table_name,
        ))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(live_table.table_id(), create_procedure.table_id());

    let dropped_table = ddl_context
        .table_metadata_manager
        .get_dropped_table(&create_procedure.data.task.table_name())
        .await
        .unwrap();
    assert_eq!(dropped_table.unwrap().table_id, dropped_table_id);
}

#[tokio::test]
async fn test_hard_drop_recreated_table_fails_when_soft_tombstone_exists() {
    let node_manager = Arc::new(MockDatanodeManager::new(NaiveDatanodeHandler));
    let mut ddl_context = new_ddl_context(node_manager);
    ddl_context.soft_drop_enabled = true;
    let original_table_id = 1024;
    let recreated_table_id = 1025;
    let table_name = "foo";
    let task = test_create_table_task(table_name, original_table_id);
    ddl_context
        .table_metadata_manager
        .create_table_metadata(
            task.table_info.clone(),
            TableRouteValue::physical(vec![]),
            HashMap::new(),
        )
        .await
        .unwrap();

    let drop_task = new_drop_table_task(table_name, original_table_id, false);
    let mut drop_procedure = DropTableProcedure::new(drop_task, ddl_context.clone());
    execute_procedure_until_done(&mut drop_procedure).await;
    ddl_context.soft_drop_enabled = false;

    ddl_context
        .table_metadata_manager
        .create_table_metadata(
            test_create_table_task(table_name, recreated_table_id).table_info,
            TableRouteValue::physical(vec![]),
            HashMap::new(),
        )
        .await
        .unwrap();

    let mut procedure = DropTableProcedure::new(
        new_drop_table_task(table_name, recreated_table_id, false),
        ddl_context,
    );
    let err = procedure.on_prepare().await.unwrap_err();

    assert_matches!(err, Error::TableNameTombstoneConflict { .. });
}

#[tokio::test]
async fn test_hard_drop_recreated_table_ignores_previous_orphan_tombstone() {
    let node_manager = Arc::new(MockDatanodeManager::new(NaiveDatanodeHandler));
    let ddl_context = new_ddl_context(node_manager);
    let original_table_id = 1024;
    let recreated_table_id = 1025;
    let table_name = "foo";
    let task = test_create_table_task(table_name, original_table_id);
    ddl_context
        .table_metadata_manager
        .create_table_metadata(
            task.table_info.clone(),
            TableRouteValue::physical(vec![]),
            HashMap::new(),
        )
        .await
        .unwrap();

    let drop_task = new_drop_table_task(table_name, original_table_id, false);
    let mut drop_procedure = DropTableProcedure::new(drop_task, ddl_context.clone());
    execute_procedure_until(&mut drop_procedure, |p| {
        p.data.state == DropTableState::DeleteTombstone
    })
    .await;

    ddl_context
        .table_metadata_manager
        .create_table_metadata(
            test_create_table_task(table_name, recreated_table_id).table_info,
            TableRouteValue::physical(vec![]),
            HashMap::new(),
        )
        .await
        .unwrap();

    let mut procedure = DropTableProcedure::new(
        new_drop_table_task(table_name, recreated_table_id, false),
        ddl_context,
    );

    procedure.on_prepare().await.unwrap();
}

#[tokio::test]
async fn test_undrop_table_restores_metadata_and_reopens_regions() {
    let (tx, mut rx) = mpsc::channel(8);
    let datanode_handler = DatanodeWatcher::new(tx);
    let node_manager = Arc::new(MockDatanodeManager::new(datanode_handler));
    let detector_controller = Arc::new(RecordingRegionFailureDetectorController::default());
    let kv_backend = Arc::new(MemoryKvBackend::new());
    let mut ddl_context = new_ddl_context_with_kv_backend(node_manager, kv_backend.clone());
    ddl_context.soft_drop_enabled = true;
    ddl_context.region_failure_detector_controller = detector_controller.clone();
    let table_id = 1024;
    let table_name = "foo";
    let task = test_create_table_task(table_name, table_id);
    ddl_context
        .table_metadata_manager
        .create_table_metadata(
            task.table_info.clone(),
            TableRouteValue::physical(vec![RegionRoute {
                region: Region::new_test(RegionId::new(table_id, 1)),
                leader_peer: Some(Peer::empty(1)),
                follower_peers: vec![Peer::empty(2)],
                leader_state: None,
                leader_down_since: None,
                write_route_policy: None,
            }]),
            HashMap::new(),
        )
        .await
        .unwrap();

    let mut drop_procedure = DropTableProcedure::new(
        new_drop_table_task(table_name, table_id, false),
        ddl_context.clone(),
    );
    execute_procedure_until_done(&mut drop_procedure).await;
    assert!(
        ddl_context
            .table_metadata_manager
            .get_dropped_table_by_id(table_id)
            .await
            .unwrap()
            .unwrap()
            .dropped_at
            .is_some()
    );
    detector_controller.clear().await;

    while rx.try_recv().is_ok() {}

    let mut procedure =
        UndropTableProcedure::new(new_undrop_table_task(table_id), ddl_context.clone());
    execute_procedure_until_done(&mut procedure).await;

    let live_table = ddl_context
        .table_metadata_manager
        .table_name_manager()
        .get(TableNameKey::new(
            DEFAULT_CATALOG_NAME,
            DEFAULT_SCHEMA_NAME,
            table_name,
        ))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(live_table.table_id(), table_id);
    assert!(
        ddl_context
            .table_metadata_manager
            .get_dropped_table(&drop_procedure.data.task.table_name())
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        kv_backend
            .get(dropped_at_marker_key(table_id).as_bytes())
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        kv_backend
            .get(retention_expires_at_marker_key(table_id).as_bytes())
            .await
            .unwrap()
            .is_none()
    );

    let mut opened_regions = HashSet::new();
    for _ in 0..2 {
        let (peer, request) = rx.try_recv().unwrap();
        let Some(region_request::Body::Open(req)) = request.body else {
            unreachable!();
        };
        opened_regions.insert((peer.id, req.region_id));
    }
    assert_eq!(
        opened_regions,
        HashSet::from([
            (1, RegionId::new(table_id, 1).as_u64()),
            (2, RegionId::new(table_id, 1).as_u64()),
        ])
    );
    assert!(rx.try_recv().is_err());

    assert_eq!(
        detector_controller.registered().await,
        vec![(1, RegionId::new(table_id, 1))]
    );
}

#[tokio::test]
async fn test_undrop_table_opens_regions_before_restoring_live_metadata() {
    let (tx, mut rx) = mpsc::channel(8);
    let datanode_handler = DatanodeWatcher::new(tx);
    let node_manager = Arc::new(MockDatanodeManager::new(datanode_handler));
    let mut ddl_context = new_ddl_context(node_manager);
    ddl_context.soft_drop_enabled = true;
    let table_id = 1024;
    let table_name = "foo";
    let task = test_create_table_task(table_name, table_id);
    ddl_context
        .table_metadata_manager
        .create_table_metadata(
            task.table_info.clone(),
            TableRouteValue::physical(vec![RegionRoute {
                region: Region::new_test(RegionId::new(table_id, 1)),
                leader_peer: Some(Peer::empty(1)),
                follower_peers: vec![],
                leader_state: None,
                leader_down_since: None,
                write_route_policy: None,
            }]),
            HashMap::new(),
        )
        .await
        .unwrap();
    let mut drop_procedure = DropTableProcedure::new(
        new_drop_table_task(table_name, table_id, false),
        ddl_context.clone(),
    );
    execute_procedure_until_done(&mut drop_procedure).await;
    while rx.try_recv().is_ok() {}

    let mut procedure =
        UndropTableProcedure::new(new_undrop_table_task(table_id), ddl_context.clone());
    let ctx = new_test_procedure_context();
    procedure.execute(&ctx).await.unwrap();
    procedure.execute(&ctx).await.unwrap();

    let (_, request) = rx.try_recv().unwrap();
    assert_matches!(request.body, Some(region_request::Body::Open(_)));
    assert!(rx.try_recv().is_err());
    assert!(
        ddl_context
            .table_metadata_manager
            .table_name_manager()
            .get(TableNameKey::new(
                DEFAULT_CATALOG_NAME,
                DEFAULT_SCHEMA_NAME,
                table_name,
            ))
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn test_undrop_logical_table_skips_datanode_open() {
    let (tx, mut rx) = mpsc::channel(8);
    let datanode_handler = DatanodeWatcher::new(tx);
    let node_manager = Arc::new(MockDatanodeManager::new(datanode_handler));
    let mut ddl_context = new_ddl_context(node_manager);
    ddl_context.soft_drop_enabled = true;
    let physical_table_id = 1024;
    let logical_table_id = 1025;
    let table_name = "foo";
    ddl_context
        .table_metadata_manager
        .create_table_metadata(
            test_create_table_task("phy", physical_table_id).table_info,
            TableRouteValue::physical(vec![RegionRoute {
                region: Region::new_test(RegionId::new(physical_table_id, 1)),
                leader_peer: Some(Peer::empty(1)),
                follower_peers: vec![],
                leader_state: None,
                leader_down_since: None,
                write_route_policy: None,
            }]),
            HashMap::new(),
        )
        .await
        .unwrap();
    let task = test_create_table_task(table_name, logical_table_id);
    ddl_context
        .table_metadata_manager
        .create_table_metadata(
            task.table_info.clone(),
            TableRouteValue::logical(physical_table_id),
            HashMap::new(),
        )
        .await
        .unwrap();

    let mut drop_procedure = DropTableProcedure::new(
        new_drop_table_task(table_name, logical_table_id, false),
        ddl_context.clone(),
    );
    execute_procedure_until_done(&mut drop_procedure).await;

    while rx.try_recv().is_ok() {}

    let mut procedure =
        UndropTableProcedure::new(new_undrop_table_task(logical_table_id), ddl_context.clone());
    execute_procedure_until_done(&mut procedure).await;

    let live_table = ddl_context
        .table_metadata_manager
        .table_name_manager()
        .get(TableNameKey::new(
            DEFAULT_CATALOG_NAME,
            DEFAULT_SCHEMA_NAME,
            table_name,
        ))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(live_table.table_id(), logical_table_id);
    assert!(rx.try_recv().is_err());
}

#[tokio::test]
async fn test_soft_drop_metric_logical_table_fails() {
    let node_manager = Arc::new(MockDatanodeManager::new(NaiveDatanodeHandler));
    let mut ddl_context = new_ddl_context(node_manager);
    ddl_context.soft_drop_enabled = true;
    let physical_table_id = create_physical_table(&ddl_context, "phy").await;
    let logical_table_id =
        create_logical_table(ddl_context.clone(), physical_table_id, "foo").await;

    let mut procedure = DropTableProcedure::new(
        new_drop_table_task("foo", logical_table_id, false),
        ddl_context.clone(),
    );
    let err = procedure.on_prepare().await.unwrap_err();
    assert_eq!(err.status_code(), StatusCode::Unsupported);
    let persisted_soft_drop = procedure.dump().unwrap();
    ddl_context.soft_drop_enabled = false;
    let mut recovered =
        DropTableProcedure::from_json(&persisted_soft_drop, ddl_context.clone()).unwrap();
    let err = recovered.on_prepare().await.unwrap_err();
    assert_eq!(err.status_code(), StatusCode::Unsupported);
}

#[tokio::test]
async fn test_soft_drop_file_engine_table_fails() {
    let node_manager = Arc::new(MockDatanodeManager::new(NaiveDatanodeHandler));
    let mut ddl_context = new_ddl_context(node_manager);
    ddl_context.soft_drop_enabled = true;
    let table_id = 1024;
    let table_name = "foo";
    let mut task = test_create_table_task(table_name, table_id);
    task.table_info.meta.engine = FILE_ENGINE.to_string();
    ddl_context
        .table_metadata_manager
        .create_table_metadata(
            task.table_info.clone(),
            TableRouteValue::physical(vec![]),
            HashMap::new(),
        )
        .await
        .unwrap();

    let mut procedure = DropTableProcedure::new(
        new_drop_table_task(table_name, table_id, false),
        ddl_context.clone(),
    );
    let err = procedure.on_prepare().await.unwrap_err();
    assert_eq!(err.status_code(), StatusCode::Unsupported);
    let persisted_soft_drop = procedure.dump().unwrap();
    ddl_context.soft_drop_enabled = false;
    let mut recovered =
        DropTableProcedure::from_json(&persisted_soft_drop, ddl_context.clone()).unwrap();
    let err = recovered.on_prepare().await.unwrap_err();
    assert_eq!(err.status_code(), StatusCode::Unsupported);
}

#[tokio::test]
async fn test_undrop_metric_logical_table_fails() {
    let node_manager = Arc::new(MockDatanodeManager::new(NaiveDatanodeHandler));
    let ddl_context = new_ddl_context(node_manager);
    let physical_table_id = create_physical_table(&ddl_context, "phy").await;
    let logical_table_id =
        create_metric_logical_table_tombstone(&ddl_context, physical_table_id, "foo").await;

    let mut procedure =
        UndropTableProcedure::new(new_undrop_table_task(logical_table_id), ddl_context);
    let err = procedure.on_prepare().await.unwrap_err();

    assert_eq!(err.status_code(), StatusCode::Unsupported);
}

#[tokio::test]
async fn test_purge_metric_logical_table_fails() {
    let node_manager = Arc::new(MockDatanodeManager::new(NaiveDatanodeHandler));
    let ddl_context = new_ddl_context(node_manager);
    let physical_table_id = create_physical_table(&ddl_context, "phy").await;
    let logical_table_id =
        create_metric_logical_table_tombstone(&ddl_context, physical_table_id, "foo").await;

    let mut procedure = PurgeDroppedTableProcedure::new(
        new_purge_dropped_table_task(logical_table_id),
        ddl_context,
    );
    let err = procedure
        .execute(&new_test_procedure_context())
        .await
        .unwrap_err();

    assert_eq!(err.status_code(), StatusCode::Unsupported);
}

#[tokio::test]
async fn test_undrop_table_fails_when_live_name_exists() {
    let node_manager = Arc::new(MockDatanodeManager::new(NaiveDatanodeHandler));
    let mut ddl_context = new_ddl_context(node_manager);
    ddl_context.soft_drop_enabled = true;
    let dropped_table_id = 1024;
    let live_table_id = 1025;
    let table_name = "foo";
    let task = test_create_table_task(table_name, dropped_table_id);
    ddl_context
        .table_metadata_manager
        .create_table_metadata(
            task.table_info.clone(),
            TableRouteValue::physical(vec![]),
            HashMap::new(),
        )
        .await
        .unwrap();
    let mut drop_procedure = DropTableProcedure::new(
        new_drop_table_task(table_name, dropped_table_id, false),
        ddl_context.clone(),
    );
    execute_procedure_until_done(&mut drop_procedure).await;
    ddl_context
        .table_metadata_manager
        .create_table_metadata(
            test_create_table_task(table_name, live_table_id).table_info,
            TableRouteValue::physical(vec![]),
            HashMap::new(),
        )
        .await
        .unwrap();

    let mut procedure =
        UndropTableProcedure::new(new_undrop_table_task(dropped_table_id), ddl_context);
    let err = procedure.on_prepare().await.unwrap_err();

    assert_matches!(err, Error::TableAlreadyExists { .. });
}

#[tokio::test]
async fn test_undrop_table_fails_when_live_name_is_created_after_prepare() {
    let node_manager = Arc::new(MockDatanodeManager::new(NaiveDatanodeHandler));
    let mut ddl_context = new_ddl_context(node_manager);
    ddl_context.soft_drop_enabled = true;
    let dropped_table_id = 1024;
    let live_table_id = 1025;
    let table_name = "foo";
    let task = test_create_table_task(table_name, dropped_table_id);
    ddl_context
        .table_metadata_manager
        .create_table_metadata(
            task.table_info.clone(),
            TableRouteValue::physical(vec![]),
            HashMap::new(),
        )
        .await
        .unwrap();
    let mut drop_procedure = DropTableProcedure::new(
        new_drop_table_task(table_name, dropped_table_id, false),
        ddl_context.clone(),
    );
    execute_procedure_until_done(&mut drop_procedure).await;

    let mut procedure =
        UndropTableProcedure::new(new_undrop_table_task(dropped_table_id), ddl_context.clone());
    procedure.on_prepare().await.unwrap();
    ddl_context
        .table_metadata_manager
        .create_table_metadata(
            test_create_table_task(table_name, live_table_id).table_info,
            TableRouteValue::physical(vec![]),
            HashMap::new(),
        )
        .await
        .unwrap();

    let err = procedure
        .execute(&new_test_procedure_context())
        .await
        .unwrap_err();
    assert_eq!(err.status_code(), StatusCode::TableAlreadyExists);
    let live_table = ddl_context
        .table_metadata_manager
        .table_name_manager()
        .get(TableNameKey::new(
            DEFAULT_CATALOG_NAME,
            DEFAULT_SCHEMA_NAME,
            table_name,
        ))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(live_table.table_id(), live_table_id);
}

#[tokio::test]
async fn test_undrop_table_closes_opened_regions_when_restore_metadata_races_with_create() {
    let (tx, mut rx) = mpsc::channel(8);
    let datanode_handler = DatanodeWatcher::new(tx);
    let node_manager = Arc::new(MockDatanodeManager::new(datanode_handler));
    let detector_controller = Arc::new(RecordingRegionFailureDetectorController::default());
    let mut ddl_context = new_ddl_context(node_manager);
    ddl_context.soft_drop_enabled = true;
    ddl_context.region_failure_detector_controller = detector_controller.clone();
    let dropped_table_id = 1024;
    let live_table_id = 1025;
    let table_name = "foo";
    let task = test_create_table_task(table_name, dropped_table_id);
    ddl_context
        .table_metadata_manager
        .create_table_metadata(
            task.table_info.clone(),
            TableRouteValue::physical(vec![RegionRoute {
                region: Region::new_test(RegionId::new(dropped_table_id, 1)),
                leader_peer: Some(Peer::empty(1)),
                follower_peers: vec![Peer::empty(2)],
                leader_state: None,
                leader_down_since: None,
                write_route_policy: None,
            }]),
            HashMap::new(),
        )
        .await
        .unwrap();
    let mut drop_procedure = DropTableProcedure::new(
        new_drop_table_task(table_name, dropped_table_id, false),
        ddl_context.clone(),
    );
    execute_procedure_until_done(&mut drop_procedure).await;
    while rx.try_recv().is_ok() {}
    detector_controller.clear().await;

    let mut procedure =
        UndropTableProcedure::new(new_undrop_table_task(dropped_table_id), ddl_context.clone());
    let ctx = new_test_procedure_context();
    procedure.execute(&ctx).await.unwrap();
    procedure.execute(&ctx).await.unwrap();

    let mut opened_regions = HashSet::new();
    for _ in 0..2 {
        let (peer, request) = rx.try_recv().unwrap();
        let Some(region_request::Body::Open(req)) = request.body else {
            unreachable!();
        };
        opened_regions.insert((peer.id, req.region_id));
    }
    assert_eq!(
        opened_regions,
        HashSet::from([
            (1, RegionId::new(dropped_table_id, 1).as_u64()),
            (2, RegionId::new(dropped_table_id, 1).as_u64()),
        ])
    );
    assert_eq!(
        detector_controller.registered().await,
        vec![(1, RegionId::new(dropped_table_id, 1))]
    );

    ddl_context
        .table_metadata_manager
        .create_table_metadata(
            test_create_table_task(table_name, live_table_id).table_info,
            TableRouteValue::physical(vec![]),
            HashMap::new(),
        )
        .await
        .unwrap();

    let err = procedure.execute(&ctx).await.unwrap_err();
    assert_eq!(err.status_code(), StatusCode::TableAlreadyExists);

    let mut closed_regions = HashSet::new();
    for _ in 0..2 {
        let (peer, request) = rx.try_recv().unwrap();
        let Some(region_request::Body::Close(req)) = request.body else {
            unreachable!();
        };
        closed_regions.insert((peer.id, req.region_id));
    }
    assert_eq!(
        closed_regions,
        HashSet::from([
            (1, RegionId::new(dropped_table_id, 1).as_u64()),
            (2, RegionId::new(dropped_table_id, 1).as_u64()),
        ])
    );
    assert!(rx.try_recv().is_err());
    assert_eq!(
        detector_controller.deregistered().await,
        vec![(1, RegionId::new(dropped_table_id, 1))]
    );

    let live_table = ddl_context
        .table_metadata_manager
        .table_name_manager()
        .get(TableNameKey::new(
            DEFAULT_CATALOG_NAME,
            DEFAULT_SCHEMA_NAME,
            table_name,
        ))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(live_table.table_id(), live_table_id);
}

#[tokio::test]
async fn test_undrop_table_lock_key_includes_original_table_name_before_prepare() {
    let node_manager = Arc::new(MockDatanodeManager::new(NaiveDatanodeHandler));
    let mut ddl_context = new_ddl_context(node_manager);
    ddl_context.soft_drop_enabled = true;
    let table_id = 1024;
    let table_name = "foo";
    let task = test_create_table_task(table_name, table_id);
    ddl_context
        .table_metadata_manager
        .create_table_metadata(
            task.table_info.clone(),
            TableRouteValue::physical(vec![]),
            HashMap::new(),
        )
        .await
        .unwrap();
    let mut drop_procedure = DropTableProcedure::new(
        new_drop_table_task(table_name, table_id, false),
        ddl_context.clone(),
    );
    execute_procedure_until_done(&mut drop_procedure).await;

    let original_table_name = ddl_context
        .table_metadata_manager
        .get_dropped_table_by_id(table_id)
        .await
        .unwrap()
        .unwrap()
        .table_name;
    let procedure = UndropTableProcedure::new_with_original_table_name(
        new_undrop_table_task(table_id),
        ddl_context,
        Some(original_table_name),
    );

    let keys = procedure
        .lock_key()
        .keys_to_lock()
        .cloned()
        .collect::<Vec<_>>();
    assert!(
        keys.iter().any(|key| matches!(
            key,
            StringKey::Exclusive(key) if key == "__table_name_lock/greptime.public.foo"
        )),
        "undrop lock keys should include the original table name: {keys:?}"
    );
    assert!(
        keys.iter().any(|key| matches!(
            key,
            StringKey::Exclusive(key) if key == "__table_lock/1024"
        )),
        "undrop lock keys should include the dropped table id: {keys:?}"
    );
}

#[tokio::test]
async fn test_undrop_rejects_stale_id_after_name_tombstone_is_consumed() {
    let node_manager = Arc::new(MockDatanodeManager::new(NaiveDatanodeHandler));
    let ddl_context = new_ddl_context(node_manager);
    let original_table_id = 1024;
    let recreated_table_id = 1025;
    let table_name = "foo";
    let original_table_name = test_create_table_task(table_name, original_table_id).table_name();
    let table_route_value = TableRouteValue::physical(vec![]);
    ddl_context
        .table_metadata_manager
        .create_table_metadata(
            test_create_table_task(table_name, original_table_id).table_info,
            table_route_value.clone(),
            HashMap::new(),
        )
        .await
        .unwrap();
    ddl_context
        .table_metadata_manager
        .delete_table_metadata(
            original_table_id,
            &original_table_name,
            &table_route_value,
            &HashMap::new(),
            None,
        )
        .await
        .unwrap();
    ddl_context
        .table_metadata_manager
        .create_table_metadata(
            test_create_table_task(table_name, recreated_table_id).table_info,
            TableRouteValue::physical(vec![]),
            HashMap::new(),
        )
        .await
        .unwrap();

    let mut stale_undrop = UndropTableProcedure::new_with_original_table_name(
        new_undrop_table_task(original_table_id),
        ddl_context.clone(),
        Some(original_table_name.clone()),
    );
    let mut hard_drop = DropTableProcedure::new(
        new_drop_table_task(table_name, recreated_table_id, false),
        ddl_context.clone(),
    );
    execute_procedure_until_done(&mut hard_drop).await;
    assert!(
        ddl_context
            .table_metadata_manager
            .get_dropped_table(&original_table_name)
            .await
            .unwrap()
            .is_none()
    );

    let err = stale_undrop.on_prepare().await.unwrap_err();

    assert_eq!(err.status_code(), StatusCode::TableNotFound);
    assert!(
        ddl_context
            .table_metadata_manager
            .table_name_manager()
            .get(TableNameKey::from(&original_table_name))
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        ddl_context
            .table_metadata_manager
            .get_dropped_table_by_id(original_table_id)
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn test_undrop_table_replayed_restore_metadata_is_idempotent() {
    let node_manager = Arc::new(MockDatanodeManager::new(NaiveDatanodeHandler));
    let mut ddl_context = new_ddl_context(node_manager);
    ddl_context.soft_drop_enabled = true;
    let table_id = 1024;
    let table_name = "foo";
    let task = test_create_table_task(table_name, table_id);
    ddl_context
        .table_metadata_manager
        .create_table_metadata(
            task.table_info.clone(),
            TableRouteValue::physical(vec![]),
            HashMap::new(),
        )
        .await
        .unwrap();
    let mut drop_procedure = DropTableProcedure::new(
        new_drop_table_task(table_name, table_id, false),
        ddl_context.clone(),
    );
    execute_procedure_until_done(&mut drop_procedure).await;

    let mut procedure =
        UndropTableProcedure::new(new_undrop_table_task(table_id), ddl_context.clone());
    let ctx = new_test_procedure_context();
    procedure.execute(&ctx).await.unwrap();
    procedure.execute(&ctx).await.unwrap();
    let restore_metadata_data = procedure.dump().unwrap();
    procedure.execute(&ctx).await.unwrap();

    let mut replayed =
        UndropTableProcedure::from_json(&restore_metadata_data, ddl_context.clone()).unwrap();
    execute_procedure_until_done(&mut replayed).await;

    let live_table = ddl_context
        .table_metadata_manager
        .table_name_manager()
        .get(TableNameKey::new(
            DEFAULT_CATALOG_NAME,
            DEFAULT_SCHEMA_NAME,
            table_name,
        ))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(live_table.table_id(), table_id);
}

#[tokio::test]
async fn test_legacy_undrop_replays_restore_metadata() {
    for partial_restore in [false, true] {
        let node_manager = Arc::new(MockDatanodeManager::new(NaiveDatanodeHandler));
        let kv_backend = Arc::new(MemoryKvBackend::new());
        let mut context = new_ddl_context_with_kv_backend(node_manager, kv_backend.clone());
        context.soft_drop_enabled = true;
        let table_id = 1024;
        let mut procedure = undrop_at_restore_metadata(&context, table_id).await;
        let persisted = legacy_undrop_snapshot(&procedure);

        if partial_restore {
            let live_key =
                TableNameKey::new(DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, "foo").to_bytes();
            let tombstone_key = [b"__tombstone/".as_slice(), live_key.as_slice()].concat();
            let value = kv_backend.get(&tombstone_key).await.unwrap().unwrap().value;
            kv_backend
                .put(PutRequest::new().with_key(live_key).with_value(value))
                .await
                .unwrap();
            kv_backend
                .batch_delete(BatchDeleteRequest::new().with_keys(vec![tombstone_key]))
                .await
                .unwrap();
        } else {
            procedure
                .execute(&new_test_procedure_context())
                .await
                .unwrap();
        }

        let mut replayed = UndropTableProcedure::from_json(&persisted, context.clone()).unwrap();
        replayed.recover().unwrap();
        execute_procedure_until_done(&mut replayed).await;

        let (table_info, table_route) = context
            .table_metadata_manager
            .get_full_table_info(table_id)
            .await
            .unwrap();
        assert!(table_info.is_some(), "partial_restore={partial_restore}");
        assert!(table_route.is_some(), "partial_restore={partial_restore}");
    }
}

#[tokio::test]
async fn test_purge_dropped_table_cleans_regions_offline_and_deletes_tombstone() {
    let (tx, mut rx) = mpsc::channel(8);
    let datanode_handler = DatanodeWatcher::new(tx);
    let node_manager = Arc::new(MockDatanodeManager::new(datanode_handler));
    let detector_controller = Arc::new(RecordingRegionFailureDetectorController::default());
    let kv_backend = Arc::new(MemoryKvBackend::new());
    let mut ddl_context = new_ddl_context_with_kv_backend(node_manager, kv_backend.clone());
    ddl_context.soft_drop_enabled = true;
    ddl_context.region_failure_detector_controller = detector_controller.clone();
    let table_id = 1024;
    let table_name = "foo";
    let task = test_create_table_task(table_name, table_id);
    ddl_context
        .table_metadata_manager
        .create_table_metadata(
            task.table_info.clone(),
            TableRouteValue::physical(vec![RegionRoute {
                region: Region::new_test(RegionId::new(table_id, 1)),
                leader_peer: Some(Peer::empty(1)),
                follower_peers: vec![Peer::empty(2)],
                leader_state: None,
                leader_down_since: None,
                write_route_policy: None,
            }]),
            HashMap::new(),
        )
        .await
        .unwrap();
    let mut drop_procedure = DropTableProcedure::new(
        new_drop_table_task(table_name, table_id, false),
        ddl_context.clone(),
    );
    execute_procedure_until_done(&mut drop_procedure).await;
    while rx.try_recv().is_ok() {}
    detector_controller.clear().await;

    let mut procedure = PurgeDroppedTableProcedure::new(
        new_purge_dropped_table_task(table_id),
        ddl_context.clone(),
    );
    execute_procedure_until_done(&mut procedure).await;

    let mut requests = Vec::new();
    for _ in 0..1 {
        let (peer, request) = rx.try_recv().unwrap();
        requests.push((peer.id, request.body.unwrap()));
    }
    requests.sort_unstable_by_key(|(peer_id, _)| *peer_id);
    let region_request::Body::CleanUp(req) = &requests[0].1 else {
        unreachable!();
    };
    assert_eq!(requests[0].0, 1);
    assert_eq!(req.region_id, RegionId::new(table_id, 1).as_u64());
    assert!(rx.try_recv().is_err());
    assert!(
        ddl_context
            .table_metadata_manager
            .get_dropped_table(&drop_procedure.data.task.table_name())
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        kv_backend
            .get(dropped_at_marker_key(table_id).as_bytes())
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        kv_backend
            .get(retention_expires_at_marker_key(table_id).as_bytes())
            .await
            .unwrap()
            .is_none()
    );

    assert_eq!(
        detector_controller.deregistered().await,
        vec![(1, RegionId::new(table_id, 1))]
    );
}

#[tokio::test]
async fn test_automatic_purge_rechecks_unexpired_tombstone() {
    let node_manager = Arc::new(MockDatanodeManager::new(NaiveDatanodeHandler));
    let mut ddl_context = new_ddl_context(node_manager);
    ddl_context.soft_drop_enabled = true;
    let table_id = 1024;
    let table_name = test_create_table_task("foo", table_id).table_name();
    let table_route_value = TableRouteValue::physical(vec![]);
    ddl_context
        .table_metadata_manager
        .create_table_metadata(
            test_create_table_task("foo", table_id).table_info,
            table_route_value.clone(),
            HashMap::new(),
        )
        .await
        .unwrap();
    ddl_context
        .table_metadata_manager
        .delete_table_metadata_with_retention(
            table_id,
            &table_name,
            &table_route_value,
            &HashMap::new(),
            Some(1),
            Some(i64::MAX),
        )
        .await
        .unwrap();

    let procedure = PurgeDroppedTableProcedure::new(
        new_purge_dropped_table_task(table_id),
        ddl_context.clone(),
    );
    let mut data: serde_json::Value = serde_json::from_str(&procedure.dump().unwrap()).unwrap();
    data["check_expired"] = true.into();
    let mut procedure =
        PurgeDroppedTableProcedure::from_json(&data.to_string(), ddl_context.clone()).unwrap();
    assert_eq!(
        procedure.type_name(),
        PurgeDroppedTableProcedure::EXPIRED_TYPE_NAME
    );

    execute_procedure_until_done(&mut procedure).await;

    assert!(
        ddl_context
            .table_metadata_manager
            .get_dropped_table_by_id(table_id)
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn test_recovered_automatic_purge_rechecks_unexpired_tombstone() {
    let node_manager = Arc::new(MockDatanodeManager::new(NaiveDatanodeHandler));
    let mut ddl_context = new_ddl_context(node_manager);
    ddl_context.soft_drop_enabled = true;
    let table_id = 1024;
    let table_name = test_create_table_task("foo", table_id).table_name();
    let table_route_value = TableRouteValue::physical(vec![]);
    ddl_context
        .table_metadata_manager
        .create_table_metadata(
            test_create_table_task("foo", table_id).table_info,
            table_route_value.clone(),
            HashMap::new(),
        )
        .await
        .unwrap();
    ddl_context
        .table_metadata_manager
        .delete_table_metadata_with_retention(
            table_id,
            &table_name,
            &table_route_value,
            &HashMap::new(),
            Some(0),
            Some(0),
        )
        .await
        .unwrap();

    let procedure = PurgeDroppedTableProcedure::new(
        new_purge_dropped_table_task(table_id),
        ddl_context.clone(),
    );
    let mut data: serde_json::Value = serde_json::from_str(&procedure.dump().unwrap()).unwrap();
    data["check_expired"] = true.into();
    let mut procedure =
        PurgeDroppedTableProcedure::from_json(&data.to_string(), ddl_context.clone()).unwrap();
    procedure
        .execute(&new_test_procedure_context())
        .await
        .unwrap();
    let persisted = procedure.dump().unwrap();

    ddl_context
        .table_metadata_manager
        .restore_table_metadata(table_id, &table_name, &table_route_value, &HashMap::new())
        .await
        .unwrap();
    ddl_context
        .table_metadata_manager
        .delete_table_metadata_with_retention(
            table_id,
            &table_name,
            &table_route_value,
            &HashMap::new(),
            Some(1),
            Some(i64::MAX),
        )
        .await
        .unwrap();

    let mut recovered =
        PurgeDroppedTableProcedure::from_json(&persisted, ddl_context.clone()).unwrap();
    recovered.recover().unwrap();
    execute_procedure_until_done(&mut recovered).await;

    assert!(
        ddl_context
            .table_metadata_manager
            .get_dropped_table_by_id(table_id)
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn test_recovered_automatic_purge_obeys_runtime_disable() {
    let node_manager = Arc::new(MockDatanodeManager::new(NaiveDatanodeHandler));
    let ddl_context = new_ddl_context(node_manager);
    let table_id = 1024;
    let table_name = test_create_table_task("foo", table_id).table_name();
    let table_route_value = TableRouteValue::physical(vec![]);
    ddl_context
        .table_metadata_manager
        .create_table_metadata(
            test_create_table_task("foo", table_id).table_info,
            table_route_value.clone(),
            HashMap::new(),
        )
        .await
        .unwrap();
    ddl_context
        .table_metadata_manager
        .delete_table_metadata_with_retention(
            table_id,
            &table_name,
            &table_route_value,
            &HashMap::new(),
            Some(0),
            Some(0),
        )
        .await
        .unwrap();

    let procedure = PurgeDroppedTableProcedure::new_if_expired(
        new_purge_dropped_table_task(table_id),
        ddl_context.clone(),
    );
    let mut recovered =
        PurgeDroppedTableProcedure::from_json(&procedure.dump().unwrap(), ddl_context.clone())
            .unwrap();
    recovered.recover().unwrap();
    execute_procedure_until_done(&mut recovered).await;

    assert!(
        ddl_context
            .table_metadata_manager
            .get_dropped_table_by_id(table_id)
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn test_recovered_automatic_purge_finishes_after_cleanup_is_claimed() {
    let node_manager = Arc::new(MockDatanodeManager::new(NaiveDatanodeHandler));
    let kv_backend = Arc::new(MemoryKvBackend::new());
    let mut enabled_context =
        new_ddl_context_with_kv_backend(node_manager.clone(), kv_backend.clone());
    enabled_context.soft_drop_enabled = true;
    let table_id = 1024;
    create_dropped_table(&enabled_context, table_id, Some(0), Some(0)).await;

    let mut procedure = PurgeDroppedTableProcedure::new_if_expired(
        new_purge_dropped_table_task(table_id),
        enabled_context.clone(),
    );
    procedure
        .execute(&new_test_procedure_context())
        .await
        .unwrap();
    let persisted_before_cleanup = procedure.dump().unwrap();
    procedure
        .execute(&new_test_procedure_context())
        .await
        .unwrap();
    assert!(
        kv_backend
            .get(purging_marker_key(table_id).as_bytes())
            .await
            .unwrap()
            .is_some()
    );

    let disabled_context = new_ddl_context_with_kv_backend(node_manager, kv_backend.clone());
    let mut recovered =
        PurgeDroppedTableProcedure::from_json(&persisted_before_cleanup, disabled_context.clone())
            .unwrap();
    recovered.recover().unwrap();
    execute_procedure_until_done(&mut recovered).await;

    assert!(
        disabled_context
            .table_metadata_manager
            .get_dropped_table_by_id(table_id)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        kv_backend
            .get(purging_marker_key(table_id).as_bytes())
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn test_recovered_automatic_purge_ignores_foreign_generation_claim() {
    let node_manager = Arc::new(MockDatanodeManager::new(NaiveDatanodeHandler));
    let kv_backend = Arc::new(MemoryKvBackend::new());
    let mut ddl_context = new_ddl_context_with_kv_backend(node_manager.clone(), kv_backend.clone());
    ddl_context.soft_drop_enabled = true;
    let table_id = 1024;
    create_dropped_table(&ddl_context, table_id, Some(0), Some(0)).await;
    kv_backend
        .put(
            PutRequest::new()
                .with_key(drop_generation_marker_key(table_id))
                .with_value("generation-a"),
        )
        .await
        .unwrap();

    let mut procedure = PurgeDroppedTableProcedure::new_if_expired(
        new_purge_dropped_table_task(table_id),
        ddl_context.clone(),
    );
    procedure
        .execute(&new_test_procedure_context())
        .await
        .unwrap();
    let persisted = procedure.dump().unwrap();
    kv_backend
        .put(
            PutRequest::new()
                .with_key(purging_marker_key(table_id))
                .with_value("generation-b"),
        )
        .await
        .unwrap();

    let mut recovered =
        PurgeDroppedTableProcedure::from_json(&persisted, ddl_context.clone()).unwrap();
    execute_procedure_until_done(&mut recovered).await;

    assert!(
        ddl_context
            .table_metadata_manager
            .get_dropped_table_by_id(table_id)
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn test_undrop_rejects_table_claimed_for_purge() {
    let node_manager = Arc::new(MockDatanodeManager::new(NaiveDatanodeHandler));
    let kv_backend = Arc::new(MemoryKvBackend::new());
    let ddl_context = new_ddl_context_with_kv_backend(node_manager, kv_backend.clone());
    let table_id = 1024;
    create_dropped_table(&ddl_context, table_id, Some(0), Some(0)).await;
    kv_backend
        .put(PutRequest::new().with_key(purging_marker_key(table_id)))
        .await
        .unwrap();

    let mut procedure =
        UndropTableProcedure::new(new_undrop_table_task(table_id), ddl_context.clone());
    let error = procedure.on_prepare().await.unwrap_err();

    assert_eq!(StatusCode::TableNotFound, error.status_code());
    assert!(
        ddl_context
            .table_metadata_manager
            .get_dropped_table_by_id(table_id)
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn test_recovered_undrop_rejects_tombstone_deleted_by_purge() {
    let node_manager = Arc::new(MockDatanodeManager::new(NaiveDatanodeHandler));
    let ddl_context = new_ddl_context(node_manager);
    let table_id = 1024;
    create_dropped_table(&ddl_context, table_id, Some(1), Some(i64::MAX)).await;

    let mut undrop =
        UndropTableProcedure::new(new_undrop_table_task(table_id), ddl_context.clone());
    undrop.on_prepare().await.unwrap();
    let open_regions_state = undrop.dump().unwrap();
    undrop.execute(&new_test_procedure_context()).await.unwrap();
    let restore_metadata_state = undrop.dump().unwrap();

    let mut purge = PurgeDroppedTableProcedure::new(
        new_purge_dropped_table_task(table_id),
        ddl_context.clone(),
    );
    execute_procedure_until_done(&mut purge).await;

    for persisted in [open_regions_state, restore_metadata_state] {
        let mut recovered =
            UndropTableProcedure::from_json(&persisted, ddl_context.clone()).unwrap();
        let error = recovered
            .execute(&new_test_procedure_context())
            .await
            .unwrap_err();
        assert_eq!(StatusCode::TableNotFound, error.status_code());
    }
}

#[tokio::test]
async fn test_recovered_undrop_rejects_replacement_generation() {
    let node_manager = Arc::new(MockDatanodeManager::new(NaiveDatanodeHandler));
    let kv_backend = Arc::new(MemoryKvBackend::new());
    let ddl_context = new_ddl_context_with_kv_backend(node_manager, kv_backend.clone());
    let table_id = 1024;
    create_dropped_table(&ddl_context, table_id, Some(1), Some(i64::MAX)).await;
    kv_backend
        .put(
            PutRequest::new()
                .with_key(drop_generation_marker_key(table_id))
                .with_value("generation-a"),
        )
        .await
        .unwrap();

    let mut undrop =
        UndropTableProcedure::new(new_undrop_table_task(table_id), ddl_context.clone());
    undrop.on_prepare().await.unwrap();
    let persisted = undrop.dump().unwrap();
    kv_backend
        .put(
            PutRequest::new()
                .with_key(drop_generation_marker_key(table_id))
                .with_value("generation-b"),
        )
        .await
        .unwrap();

    let mut recovered = UndropTableProcedure::from_json(&persisted, ddl_context).unwrap();
    let error = recovered
        .execute(&new_test_procedure_context())
        .await
        .unwrap_err();
    assert_eq!(StatusCode::TableNotFound, error.status_code());
}

#[tokio::test]
async fn test_legacy_unmarked_tombstone_can_be_undropped_and_purged() {
    let node_manager = Arc::new(MockDatanodeManager::new(NaiveDatanodeHandler));
    let ddl_context = new_ddl_context(node_manager);
    let table_id = 1024;
    let table_name = test_create_table_task("foo", table_id).table_name();
    let table_route_value = TableRouteValue::physical(vec![]);
    ddl_context
        .table_metadata_manager
        .create_table_metadata(
            test_create_table_task("foo", table_id).table_info,
            table_route_value.clone(),
            HashMap::new(),
        )
        .await
        .unwrap();
    ddl_context
        .table_metadata_manager
        .delete_table_metadata(
            table_id,
            &table_name,
            &table_route_value,
            &HashMap::new(),
            None,
        )
        .await
        .unwrap();
    assert_eq!(
        ddl_context
            .table_metadata_manager
            .get_dropped_table_by_id(table_id)
            .await
            .unwrap()
            .unwrap()
            .dropped_at,
        None
    );
    let dropped_tables = ddl_context
        .table_metadata_manager
        .list_dropped_tables()
        .await
        .unwrap();
    assert_eq!(dropped_tables.len(), 1);
    assert_eq!(dropped_tables[0].dropped_at, None);

    let mut undrop =
        UndropTableProcedure::new(new_undrop_table_task(table_id), ddl_context.clone());
    execute_procedure_until_done(&mut undrop).await;

    ddl_context
        .table_metadata_manager
        .delete_table_metadata(
            table_id,
            &table_name,
            &table_route_value,
            &HashMap::new(),
            None,
        )
        .await
        .unwrap();
    let mut purge = PurgeDroppedTableProcedure::new(
        new_purge_dropped_table_task(table_id),
        ddl_context.clone(),
    );
    execute_procedure_until_done(&mut purge).await;
    assert!(
        ddl_context
            .table_metadata_manager
            .get_dropped_table_by_id(table_id)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn test_purge_dropped_table_by_id_selects_tombstone_when_live_table_exists() {
    let (tx, mut rx) = mpsc::channel(8);
    let datanode_handler = DatanodeWatcher::new(tx);
    let node_manager = Arc::new(MockDatanodeManager::new(datanode_handler));
    let mut ddl_context = new_ddl_context(node_manager);
    ddl_context.soft_drop_enabled = true;
    let dropped_table_id = 1024;
    let live_table_id = 1025;
    let table_name = "foo";
    ddl_context
        .table_metadata_manager
        .create_table_metadata(
            test_create_table_task(table_name, dropped_table_id).table_info,
            TableRouteValue::physical(vec![RegionRoute {
                region: Region::new_test(RegionId::new(dropped_table_id, 1)),
                leader_peer: Some(Peer::empty(1)),
                follower_peers: vec![],
                leader_state: None,
                leader_down_since: None,
                write_route_policy: None,
            }]),
            HashMap::new(),
        )
        .await
        .unwrap();
    let mut drop_procedure = DropTableProcedure::new(
        new_drop_table_task(table_name, dropped_table_id, false),
        ddl_context.clone(),
    );
    execute_procedure_until_done(&mut drop_procedure).await;
    ddl_context
        .table_metadata_manager
        .create_table_metadata(
            test_create_table_task(table_name, live_table_id).table_info,
            TableRouteValue::physical(vec![]),
            HashMap::new(),
        )
        .await
        .unwrap();
    while rx.try_recv().is_ok() {}

    let mut procedure = PurgeDroppedTableProcedure::new(
        new_purge_dropped_table_task(dropped_table_id),
        ddl_context.clone(),
    );
    execute_procedure_until_done(&mut procedure).await;

    let live_table = ddl_context
        .table_metadata_manager
        .table_name_manager()
        .get(TableNameKey::new(
            DEFAULT_CATALOG_NAME,
            DEFAULT_SCHEMA_NAME,
            table_name,
        ))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(live_table.table_id(), live_table_id);

    let (_, request) = rx.try_recv().unwrap();
    let Some(region_request::Body::CleanUp(req)) = request.body else {
        unreachable!();
    };
    assert_eq!(req.region_id, RegionId::new(dropped_table_id, 1).as_u64());
    assert!(rx.try_recv().is_err());
}

#[tokio::test]
async fn test_on_rollback() {
    let node_manager = Arc::new(MockDatanodeManager::new(NaiveDatanodeHandler));
    let kv_backend = Arc::new(MemoryKvBackend::new());
    let mut ddl_context = new_ddl_context_with_kv_backend(node_manager, kv_backend.clone());
    ddl_context.soft_drop_enabled = true;
    // Prepares physical table metadata.
    let mut create_physical_table_task = test_create_physical_table_task("phy_table");
    let TableMetadata {
        table_id,
        table_route,
        ..
    } = ddl_context
        .table_metadata_allocator
        .create(&create_physical_table_task)
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
    let mut procedure =
        CreateLogicalTablesProcedure::new(vec![task], physical_table_id, ddl_context.clone());
    procedure.on_prepare().await.unwrap();
    let ctx = new_test_procedure_context();
    procedure.execute(&ctx).await.unwrap();
    // Triggers procedure to create table metadata
    let status = procedure.execute(&ctx).await.unwrap();
    let table_ids = status.downcast_output_ref::<Vec<u32>>().unwrap();
    assert_eq!(*table_ids, vec![1025]);

    let expected_kvs = kv_backend.dump();
    // Drops the physical table
    {
        let task = new_drop_table_task("phy_table", physical_table_id, false);
        let mut procedure = DropTableProcedure::new(task, ddl_context.clone());
        procedure.on_prepare().await.unwrap();
        assert!(procedure.rollback_supported());
        procedure.on_delete_metadata().await.unwrap();
        assert!(
            kv_backend
                .get(dropped_at_marker_key(physical_table_id).as_bytes())
                .await
                .unwrap()
                .is_some()
        );
        assert!(
            kv_backend
                .get(retention_expires_at_marker_key(physical_table_id).as_bytes())
                .await
                .unwrap()
                .is_some()
        );
        assert!(procedure.rollback_supported());
        procedure.rollback(&ctx).await.unwrap();
        assert!(
            kv_backend
                .get(dropped_at_marker_key(physical_table_id).as_bytes())
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            kv_backend
                .get(retention_expires_at_marker_key(physical_table_id).as_bytes())
                .await
                .unwrap()
                .is_none()
        );
        // Rollback again
        assert!(procedure.rollback_supported());
        procedure.rollback(&ctx).await.unwrap();
        let kvs = kv_backend.dump();
        assert_eq!(kvs, expected_kvs);
    }

    // Drops the logical table
    ddl_context.soft_drop_enabled = false;
    let task = new_drop_table_task("foo", table_ids[0], false);
    let mut procedure = DropTableProcedure::new(task, ddl_context.clone());
    procedure.on_prepare().await.unwrap();
    assert!(!procedure.rollback_supported());
}

fn new_drop_table_task(table_name: &str, table_id: TableId, drop_if_exists: bool) -> DropTableTask {
    DropTableTask {
        catalog: DEFAULT_CATALOG_NAME.to_string(),
        schema: DEFAULT_SCHEMA_NAME.to_string(),
        table: table_name.to_string(),
        table_id,
        drop_if_exists,
    }
}

fn new_undrop_table_task(table_id: TableId) -> UndropTableTask {
    UndropTableTask { table_id }
}

fn new_purge_dropped_table_task(table_id: TableId) -> PurgeDroppedTableTask {
    PurgeDroppedTableTask { table_id }
}

async fn create_metric_logical_table_tombstone(
    ddl_context: &crate::ddl::DdlContext,
    physical_table_id: TableId,
    table_name: &str,
) -> TableId {
    let logical_table_id =
        create_logical_table(ddl_context.clone(), physical_table_id, table_name).await;
    let mut task = test_create_logical_table_task(table_name);
    task.set_table_id(logical_table_id);
    ddl_context
        .table_metadata_manager
        .delete_table_metadata(
            logical_table_id,
            &task.table_name(),
            &TableRouteValue::logical(physical_table_id),
            &HashMap::new(),
            None,
        )
        .await
        .unwrap();
    logical_table_id
}

#[derive(Default)]
struct RecordingRegionFailureDetectorController {
    registered: Mutex<Vec<DetectingRegion>>,
    deregistered: Mutex<Vec<DetectingRegion>>,
}

impl RecordingRegionFailureDetectorController {
    async fn registered(&self) -> Vec<DetectingRegion> {
        self.registered.lock().await.clone()
    }

    async fn deregistered(&self) -> Vec<DetectingRegion> {
        self.deregistered.lock().await.clone()
    }

    async fn clear(&self) {
        self.registered.lock().await.clear();
        self.deregistered.lock().await.clear();
    }
}

#[async_trait]
impl RegionFailureDetectorController for RecordingRegionFailureDetectorController {
    async fn register_failure_detectors(&self, detecting_regions: Vec<DetectingRegion>) {
        self.registered.lock().await.extend(detecting_regions);
    }

    async fn reset_failure_detectors(&self, detecting_regions: Vec<DetectingRegion>) {
        self.registered.lock().await.extend(detecting_regions);
    }

    async fn deregister_failure_detectors(&self, detecting_regions: Vec<DetectingRegion>) {
        self.deregistered.lock().await.extend(detecting_regions);
    }
}

#[tokio::test]
async fn test_memory_region_keeper_guard_dropped_on_procedure_done() {
    let node_manager = Arc::new(MockDatanodeManager::new(NaiveDatanodeHandler));
    let kv_backend = Arc::new(MemoryKvBackend::new());
    let ddl_context = new_ddl_context_with_kv_backend(node_manager, kv_backend);

    let physical_table_id = create_physical_table(&ddl_context, "t").await;
    let logical_table_id = create_logical_table(ddl_context.clone(), physical_table_id, "s").await;

    let inner_test = |task: DropTableTask| async {
        let mut procedure = DropTableProcedure::new(task, ddl_context.clone());
        execute_procedure_until(&mut procedure, |p| {
            p.data.state == DropTableState::InvalidateTableCache
        })
        .await;

        // Ensure that after running to the state `InvalidateTableCache`(just past `DeleteMetadata`),
        // the dropping regions should be recorded:
        let guards = &procedure.dropping_regions;
        assert_eq!(guards.len(), 1);
        let (datanode_id, region_id) = (0, RegionId::new(physical_table_id, 0));
        assert_eq!(guards[0].info(), (datanode_id, region_id));
        assert!(
            ddl_context
                .memory_region_keeper
                .contains(datanode_id, region_id)
        );
        let roles = ddl_context
            .memory_region_keeper
            .extract_operating_region_roles(datanode_id, &HashSet::from([region_id]));
        assert_eq!(roles.get(&region_id), Some(&RegionRole::Leader));

        execute_procedure_until_done(&mut procedure).await;

        // Ensure that when run to the end, the dropping regions should be cleared:
        let guards = &procedure.dropping_regions;
        assert!(guards.is_empty());
        assert!(
            !ddl_context
                .memory_region_keeper
                .contains(datanode_id, region_id)
        );
    };

    inner_test(new_drop_table_task("s", logical_table_id, false)).await;
    inner_test(new_drop_table_task("t", physical_table_id, false)).await;
}

#[tokio::test]
async fn test_from_json() {
    for (state, num_operating_regions, num_operating_regions_after_recovery) in [
        (DropTableState::DeleteMetadata, 0, 1),
        (DropTableState::InvalidateTableCache, 1, 1),
        (DropTableState::DatanodeDropRegions, 1, 1),
        (DropTableState::DeleteTombstone, 1, 0),
    ] {
        let node_manager = Arc::new(MockDatanodeManager::new(NaiveDatanodeHandler));
        let kv_backend = Arc::new(MemoryKvBackend::new());
        let ddl_context = new_ddl_context_with_kv_backend(node_manager, kv_backend);

        let physical_table_id = create_physical_table(&ddl_context, "t").await;
        let task = new_drop_table_task("t", physical_table_id, false);
        let mut procedure = DropTableProcedure::new(task, ddl_context.clone());
        execute_procedure_until(&mut procedure, |p| p.data.state == state).await;
        let data = procedure.dump().unwrap();
        assert_eq!(
            ddl_context.memory_region_keeper.len(),
            num_operating_regions
        );
        // Cleans up the keeper.
        ddl_context.memory_region_keeper.clear();
        let mut procedure = DropTableProcedure::from_json(&data, ddl_context.clone()).unwrap();
        procedure.recover().unwrap();
        assert_eq!(
            ddl_context.memory_region_keeper.len(),
            num_operating_regions_after_recovery
        );
        assert_eq!(
            procedure.dropping_regions.len(),
            num_operating_regions_after_recovery
        );
    }

    let num_operating_regions = 0;
    let num_operating_regions_after_recovery = 0;
    let node_manager = Arc::new(MockDatanodeManager::new(NaiveDatanodeHandler));
    let kv_backend = Arc::new(MemoryKvBackend::new());
    let ddl_context = new_ddl_context_with_kv_backend(node_manager, kv_backend);

    let physical_table_id = create_physical_table(&ddl_context, "t").await;
    let task = new_drop_table_task("t", physical_table_id, false);
    let mut procedure = DropTableProcedure::new(task, ddl_context.clone());
    execute_procedure_until_done(&mut procedure).await;
    let data = procedure.dump().unwrap();
    assert_eq!(
        ddl_context.memory_region_keeper.len(),
        num_operating_regions
    );
    // Cleans up the keeper.
    ddl_context.memory_region_keeper.clear();
    let mut procedure = DropTableProcedure::from_json(&data, ddl_context.clone()).unwrap();
    procedure.recover().unwrap();
    assert_eq!(
        ddl_context.memory_region_keeper.len(),
        num_operating_regions_after_recovery
    );
    assert_eq!(
        procedure.dropping_regions.len(),
        num_operating_regions_after_recovery
    );
}
