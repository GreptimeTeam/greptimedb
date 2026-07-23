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
use std::sync::Arc;

use api::region::RegionResponse;
use api::v1::Value;
use api::v1::meta::Peer;
use api::v1::region::sync_request::ManifestInfo;
use api::v1::region::{MetricManifestInfo, RegionRequest, SyncRequest, region_request};
use api::v1::value::ValueData;
use common_error::ext::ErrorExt;
use common_error::status_code::StatusCode;
use common_procedure::{
    Context as ProcedureContext, EventContext, EventTrigger, Procedure, ProcedureId,
    ProcedureState, Status,
};
use common_procedure_test::MockContextProvider;
use store_api::metadata::ColumnMetadata;
use store_api::metric_engine_consts::{ALTER_PHYSICAL_EXTENSION_KEY, MANIFEST_INFO_EXTENSION_KEY};
use store_api::region_engine::RegionManifestInfo;
use store_api::storage::RegionId;
use store_api::storage::consts::ReservedColumnId;
use tokio::sync::mpsc;

use crate::ddl::TableMetadata;
use crate::ddl::create_logical_tables::CreateLogicalTablesProcedure;
use crate::ddl::test_util::datanode_handler::{DatanodeWatcher, NaiveDatanodeHandler};
use crate::ddl::test_util::{
    assert_column_name, create_physical_table_metadata, get_raw_table_info, test_column_metadatas,
    test_create_logical_table_task, test_create_physical_table_task,
};
use crate::error::{Error, Result};
use crate::key::table_route::{PhysicalTableRouteValue, TableRouteValue};
use crate::rpc::router::{Region, RegionRoute};
use crate::test_util::{MockDatanodeManager, new_ddl_context};

fn make_creates_request_handler(
    column_metadatas: Vec<ColumnMetadata>,
) -> impl Fn(Peer, RegionRequest) -> Result<RegionResponse> {
    move |_peer, request| {
        let _ = _peer;
        if let region_request::Body::Creates(_) = request.body.unwrap() {
            let mut response = RegionResponse::new(0);
            // Default region id for physical table.
            let region_id = RegionId::new(1024, 1);
            response.extensions.insert(
                MANIFEST_INFO_EXTENSION_KEY.to_string(),
                RegionManifestInfo::encode_list(&[(
                    region_id,
                    RegionManifestInfo::metric(1, 0, 2, 0),
                )])
                .unwrap(),
            );
            response.extensions.insert(
                ALTER_PHYSICAL_EXTENSION_KEY.to_string(),
                ColumnMetadata::encode_list(&column_metadatas).unwrap(),
            );
            return Ok(response);
        }

        Ok(RegionResponse::new(0))
    }
}

fn assert_creates_request(
    peer: Peer,
    request: RegionRequest,
    expected_peer_id: u64,
    expected_region_ids: &[RegionId],
) {
    assert_eq!(peer.id, expected_peer_id,);
    let Some(region_request::Body::Creates(req)) = request.body else {
        unreachable!();
    };
    for (i, region_id) in expected_region_ids.iter().enumerate() {
        assert_eq!(
            req.requests[i].region_id,
            *region_id,
            "actual region id: {}",
            RegionId::from_u64(req.requests[i].region_id)
        );
    }
}

#[test]
fn test_submitted_event_has_one_rich_row_per_logical_table() {
    let node_manager = Arc::new(MockDatanodeManager::new(()));
    let ddl_context = new_ddl_context(node_manager);
    let mut foo = test_create_logical_table_task("foo");
    foo.set_table_id(7);
    let mut bar = test_create_logical_table_task("bar");
    bar.set_table_id(8);
    let expected_foo_task = serde_json::to_value(&foo).unwrap();
    let expected_bar_task = serde_json::to_value(&bar).unwrap();
    let procedure = CreateLogicalTablesProcedure::new(vec![foo, bar], 1024, ddl_context);
    let lifecycle_state = ProcedureState::Running;

    let event = procedure
        .event(&EventContext {
            procedure_id: ProcedureId::random(),
            lifecycle_state: &lifecycle_state,
            trigger: EventTrigger::Submitted,
        })
        .unwrap();

    assert_eq!(event.event_type(), "create_logical_tables");
    let rows = event.extra_rows().unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(
        rows[0].values,
        vec![
            ValueData::StringValue("greptime".to_string()).into(),
            ValueData::StringValue("public".to_string()).into(),
            ValueData::StringValue("foo".to_string()).into(),
            Value::default(),
            ValueData::U32Value(1024).into(),
        ]
    );
    assert_eq!(
        rows[1].values[2],
        ValueData::StringValue("bar".to_string()).into()
    );
    assert_eq!(rows[1].values[3], Value::default());
    assert_eq!(rows[1].values[4], ValueData::U32Value(1024).into());

    let payload = event.json_payload().unwrap();
    assert_eq!(payload["version"], 1);
    assert_eq!(payload["data"]["kind"], "create_logical_tables");
    assert_eq!(payload["data"]["physical_table_id"], 1024);
    let tables = payload["data"]["tables"].as_array().unwrap();
    assert_eq!(tables.len(), 2);
    assert_eq!(tables[0]["task"], expected_foo_task);
    assert_eq!(tables[1]["task"], expected_bar_task);
    assert!(tables[0]["table_info"].is_object());
}

#[test]
fn test_later_event_is_lightweight() {
    let node_manager = Arc::new(MockDatanodeManager::new(()));
    let ddl_context = new_ddl_context(node_manager);
    let procedure = CreateLogicalTablesProcedure::new(
        vec![test_create_logical_table_task("foo")],
        1024,
        ddl_context,
    );
    let lifecycle_state = ProcedureState::Running;

    let event = procedure
        .event(&EventContext {
            procedure_id: ProcedureId::random(),
            lifecycle_state: &lifecycle_state,
            trigger: EventTrigger::Recovered,
        })
        .unwrap();

    assert_eq!(event.event_type(), "create_logical_tables");
    assert_eq!(event.json_payload().unwrap(), serde_json::Value::Null);
    assert_eq!(
        event.extra_rows().unwrap()[0].values,
        vec![Value::default(); 5]
    );
}

#[test]
fn test_succeeded_event_has_one_id_only_row_per_logical_table() {
    let node_manager = Arc::new(MockDatanodeManager::new(()));
    let ddl_context = new_ddl_context(node_manager);
    let mut foo = test_create_logical_table_task("foo");
    foo.set_table_id(7);
    let mut bar = test_create_logical_table_task("bar");
    bar.set_table_id(8);
    let procedure = CreateLogicalTablesProcedure::new(vec![foo, bar], 1024, ddl_context);
    let lifecycle_state = ProcedureState::Done {
        output: Some(Arc::new(vec![1025_u32, 1026_u32])),
    };

    let event = procedure
        .event(&EventContext {
            procedure_id: ProcedureId::random(),
            lifecycle_state: &lifecycle_state,
            trigger: EventTrigger::Succeeded,
        })
        .unwrap();

    assert_eq!(event.json_payload().unwrap(), serde_json::Value::Null);
    let rows = event.extra_rows().unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(
        rows[0].values,
        vec![
            Value::default(),
            Value::default(),
            Value::default(),
            ValueData::U32Value(1025).into(),
            Value::default(),
        ]
    );
    assert_eq!(rows[1].values[3], ValueData::U32Value(1026).into());
    assert_eq!(rows[1].values[4], Value::default());
}

#[tokio::test]
async fn test_on_prepare_physical_table_not_found() {
    let node_manager = Arc::new(MockDatanodeManager::new(()));
    let ddl_context = new_ddl_context(node_manager);
    let tasks = vec![test_create_logical_table_task("foo")];
    let physical_table_id = 1024u32;
    let mut procedure = CreateLogicalTablesProcedure::new(tasks, physical_table_id, ddl_context);
    let err = procedure.on_prepare().await.unwrap_err();
    assert_matches!(err, Error::TableRouteNotFound { .. });
}

#[tokio::test]
async fn test_on_prepare() {
    let node_manager = Arc::new(MockDatanodeManager::new(()));
    let ddl_context = new_ddl_context(node_manager);
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
    let tasks = vec![test_create_logical_table_task("foo")];
    let physical_table_id = table_id;
    let mut procedure = CreateLogicalTablesProcedure::new(tasks, physical_table_id, ddl_context);
    let status = procedure.on_prepare().await.unwrap();
    assert_matches!(
        status,
        Status::Executing {
            persist: true,
            clean_poisons: false
        }
    );
}

#[tokio::test]
async fn test_on_prepare_logical_table_exists_err() {
    let node_manager = Arc::new(MockDatanodeManager::new(()));
    let ddl_context = new_ddl_context(node_manager);
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
    // Creates the logical table metadata.
    let mut task = test_create_logical_table_task("foo");
    task.set_table_id(1025);
    ddl_context
        .table_metadata_manager
        .create_logical_tables_metadata(vec![(
            task.table_info.clone(),
            TableRouteValue::logical(1024),
        )])
        .await
        .unwrap();
    // The create logical table procedure.
    let physical_table_id = table_id;
    let mut procedure =
        CreateLogicalTablesProcedure::new(vec![task], physical_table_id, ddl_context);
    let err = procedure.on_prepare().await.unwrap_err();
    assert_matches!(err, Error::TableAlreadyExists { .. });
    assert_eq!(err.status_code(), StatusCode::TableAlreadyExists);
}

#[tokio::test]
async fn test_on_prepare_with_create_if_table_exists() {
    let node_manager = Arc::new(MockDatanodeManager::new(()));
    let ddl_context = new_ddl_context(node_manager);
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
    // Creates the logical table metadata.
    let mut task = test_create_logical_table_task("foo");
    task.set_table_id(8192);
    ddl_context
        .table_metadata_manager
        .create_logical_tables_metadata(vec![(
            task.table_info.clone(),
            TableRouteValue::logical(1024),
        )])
        .await
        .unwrap();
    // The create logical table procedure.
    let physical_table_id = table_id;
    // Sets `create_if_not_exists`
    task.create_table.create_if_not_exists = true;
    let mut procedure =
        CreateLogicalTablesProcedure::new(vec![task], physical_table_id, ddl_context);
    let status = procedure.on_prepare().await.unwrap();
    let output = status.downcast_output_ref::<Vec<u32>>().unwrap();
    assert_eq!(*output, vec![8192]);
}

#[tokio::test]
async fn test_on_prepare_part_logical_tables_exist() {
    let node_manager = Arc::new(MockDatanodeManager::new(()));
    let ddl_context = new_ddl_context(node_manager);
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
    // Creates the logical table metadata.
    let mut task = test_create_logical_table_task("exists");
    task.set_table_id(8192);
    ddl_context
        .table_metadata_manager
        .create_logical_tables_metadata(vec![(
            task.table_info.clone(),
            TableRouteValue::logical(1024),
        )])
        .await
        .unwrap();
    // The create logical table procedure.
    let physical_table_id = table_id;
    // Sets `create_if_not_exists`
    task.create_table.create_if_not_exists = true;
    let non_exist_task = test_create_logical_table_task("non_exists");
    let mut procedure = CreateLogicalTablesProcedure::new(
        vec![task, non_exist_task],
        physical_table_id,
        ddl_context,
    );
    let status = procedure.on_prepare().await.unwrap();
    assert_matches!(
        status,
        Status::Executing {
            persist: true,
            clean_poisons: false
        }
    );
}

#[tokio::test]
async fn test_on_create_metadata() {
    common_telemetry::init_default_ut_logging();
    let (tx, mut rx) = mpsc::channel(8);
    let column_metadatas = test_column_metadatas(&["host", "cpu"]);
    let datanode_handler =
        DatanodeWatcher::new(tx).with_handler(make_creates_request_handler(column_metadatas));
    let node_manager = Arc::new(MockDatanodeManager::new(datanode_handler));
    let ddl_context = new_ddl_context(node_manager);
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
    let yet_another_task = test_create_logical_table_task("bar");
    let mut procedure = CreateLogicalTablesProcedure::new(
        vec![task, yet_another_task],
        physical_table_id,
        ddl_context.clone(),
    );
    let status = procedure.on_prepare().await.unwrap();
    assert_matches!(
        status,
        Status::Executing {
            persist: true,
            clean_poisons: false
        }
    );
    let ctx = ProcedureContext {
        procedure_id: ProcedureId::random(),
        provider: Arc::new(MockContextProvider::default()),
    };
    procedure.execute(&ctx).await.unwrap();
    // Triggers procedure to create table metadata
    let status = procedure.execute(&ctx).await.unwrap();
    let table_ids = status.downcast_output_ref::<Vec<u32>>().unwrap();
    assert_eq!(*table_ids, vec![1025, 1026]);

    let (peer, request) = rx.try_recv().unwrap();
    rx.try_recv().unwrap_err();
    assert_creates_request(
        peer,
        request,
        0,
        &[RegionId::new(1025, 0), RegionId::new(1026, 0)],
    );

    let table_info = get_raw_table_info(&ddl_context, table_id).await;
    assert_column_name(
        &table_info,
        &["ts", "value", "__table_id", "__tsid", "host", "cpu"],
    );
    assert_eq!(
        table_info.meta.column_ids,
        vec![
            0,
            1,
            ReservedColumnId::table_id(),
            ReservedColumnId::tsid(),
            2,
            3
        ]
    );
}

#[tokio::test]
async fn test_on_create_metadata_part_logical_tables_exist() {
    common_telemetry::init_default_ut_logging();
    let (tx, mut rx) = mpsc::channel(8);
    let column_metadatas = test_column_metadatas(&["host", "cpu"]);
    let datanode_handler =
        DatanodeWatcher::new(tx).with_handler(make_creates_request_handler(column_metadatas));
    let node_manager = Arc::new(MockDatanodeManager::new(datanode_handler));
    let ddl_context = new_ddl_context(node_manager);
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
    // Creates the logical table metadata.
    let mut task = test_create_logical_table_task("exists");
    task.set_table_id(8192);
    ddl_context
        .table_metadata_manager
        .create_logical_tables_metadata(vec![(
            task.table_info.clone(),
            TableRouteValue::logical(1024),
        )])
        .await
        .unwrap();
    // The create logical table procedure.
    let physical_table_id = table_id;
    // Sets `create_if_not_exists`
    task.create_table.create_if_not_exists = true;
    let non_exist_task = test_create_logical_table_task("non_exists");
    let mut procedure = CreateLogicalTablesProcedure::new(
        vec![task, non_exist_task],
        physical_table_id,
        ddl_context.clone(),
    );
    let status = procedure.on_prepare().await.unwrap();
    assert_matches!(
        status,
        Status::Executing {
            persist: true,
            clean_poisons: false
        }
    );
    let ctx = ProcedureContext {
        procedure_id: ProcedureId::random(),
        provider: Arc::new(MockContextProvider::default()),
    };
    procedure.execute(&ctx).await.unwrap();
    // Triggers procedure to create table metadata
    let status = procedure.execute(&ctx).await.unwrap();
    let table_ids = status.downcast_output_ref::<Vec<u32>>().unwrap();
    assert_eq!(*table_ids, vec![8192, 1025]);

    let (peer, request) = rx.try_recv().unwrap();
    rx.try_recv().unwrap_err();
    assert_creates_request(peer, request, 0, &[RegionId::new(1025, 0)]);

    let table_info = get_raw_table_info(&ddl_context, table_id).await;
    assert_column_name(
        &table_info,
        &["ts", "value", "__table_id", "__tsid", "host", "cpu"],
    );
    assert_eq!(
        table_info.meta.column_ids,
        vec![
            0,
            1,
            ReservedColumnId::table_id(),
            ReservedColumnId::tsid(),
            2,
            3
        ]
    );
}

#[tokio::test]
async fn test_on_create_metadata_err() {
    let node_manager = Arc::new(MockDatanodeManager::new(NaiveDatanodeHandler));
    let ddl_context = new_ddl_context(node_manager);
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
    let yet_another_task = test_create_logical_table_task("bar");
    let mut procedure = CreateLogicalTablesProcedure::new(
        vec![task.clone(), yet_another_task],
        physical_table_id,
        ddl_context.clone(),
    );
    let status = procedure.on_prepare().await.unwrap();
    assert_matches!(
        status,
        Status::Executing {
            persist: true,
            clean_poisons: false
        }
    );
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
        .create_logical_tables_metadata(vec![(task.table_info, TableRouteValue::logical(512))])
        .await
        .unwrap();
    // Triggers procedure to create table metadata
    let error = procedure.execute(&ctx).await.unwrap_err();
    assert!(!error.is_retry_later());
}

#[tokio::test]
async fn test_on_submit_create_request() {
    common_telemetry::init_default_ut_logging();
    let (tx, mut rx) = mpsc::channel(8);
    let column_metadatas = test_column_metadatas(&["host", "cpu"]);
    let handler =
        DatanodeWatcher::new(tx).with_handler(make_creates_request_handler(column_metadatas));
    let node_manager = Arc::new(MockDatanodeManager::new(handler));
    let ddl_context = new_ddl_context(node_manager);
    let mut create_physical_table_task = test_create_physical_table_task("phy_table");
    let table_id = 1024u32;
    let region_routes = vec![RegionRoute {
        region: Region::new_test(RegionId::new(table_id, 1)),
        leader_peer: Some(Peer::empty(1)),
        follower_peers: vec![Peer::empty(5)],
        leader_state: None,
        leader_down_since: None,
        write_route_policy: None,
    }];
    create_physical_table_task.set_table_id(table_id);
    create_physical_table_metadata(
        &ddl_context,
        create_physical_table_task.table_info.clone(),
        TableRouteValue::Physical(PhysicalTableRouteValue::new(region_routes)),
    )
    .await;
    let physical_table_id = table_id;
    let task = test_create_logical_table_task("foo");
    let yet_another_task = test_create_logical_table_task("bar");
    let mut procedure = CreateLogicalTablesProcedure::new(
        vec![task, yet_another_task],
        physical_table_id,
        ddl_context,
    );
    procedure.on_prepare().await.unwrap();
    procedure.on_datanode_create_regions().await.unwrap();
    let mut results = Vec::new();
    for _ in 0..2 {
        let result = rx.try_recv().unwrap();
        results.push(result);
    }
    rx.try_recv().unwrap_err();
    let (peer, request) = results.remove(0);
    assert_eq!(peer.id, 1);
    assert_matches!(request.body.unwrap(), region_request::Body::Creates(_));
    let (peer, request) = results.remove(0);
    assert_eq!(peer.id, 5);
    assert_matches!(
        request.body.unwrap(),
        region_request::Body::Sync(SyncRequest {
            manifest_info: Some(ManifestInfo::MetricManifestInfo(MetricManifestInfo {
                data_manifest_version: 1,
                metadata_manifest_version: 2,
                ..
            })),
            ..
        })
    );
}
