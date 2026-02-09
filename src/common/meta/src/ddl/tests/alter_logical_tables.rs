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
use std::sync::Arc;

use api::region::RegionResponse;
use api::v1::meta::Peer;
use api::v1::region::sync_request::ManifestInfo;
use api::v1::region::{MetricManifestInfo, RegionRequest, SyncRequest, region_request};
use api::v1::{ColumnDataType, SemanticType};
use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
use common_procedure::{Procedure, ProcedureId, Status};
use common_procedure_test::MockContextProvider;
use store_api::metadata::ColumnMetadata;
use store_api::metric_engine_consts::{ALTER_PHYSICAL_EXTENSION_KEY, MANIFEST_INFO_EXTENSION_KEY};
use store_api::region_engine::RegionManifestInfo;
use store_api::storage::RegionId;
use store_api::storage::consts::ReservedColumnId;
use tokio::sync::mpsc;

use crate::ddl::alter_logical_tables::AlterLogicalTablesProcedure;
use crate::ddl::test_util::alter_table::TestAlterTableExprBuilder;
use crate::ddl::test_util::columns::TestColumnDefBuilder;
use crate::ddl::test_util::datanode_handler::DatanodeWatcher;
use crate::ddl::test_util::{
    assert_column_name, create_logical_table, create_physical_table,
    create_physical_table_metadata, get_raw_table_info, test_column_metadatas,
    test_create_physical_table_task,
};
use crate::error::Error::{AlterLogicalTablesInvalidArguments, TableNotFound};
use crate::error::Result;
use crate::key::table_name::TableNameKey;
use crate::key::table_route::{PhysicalTableRouteValue, TableRouteValue};
use crate::rpc::ddl::AlterTableTask;
use crate::rpc::router::{Region, RegionRoute};
use crate::test_util::{MockDatanodeManager, new_ddl_context};

fn make_alter_logical_table_add_column_task(
    schema: Option<&str>,
    table: &str,
    add_columns: Vec<String>,
) -> AlterTableTask {
    let add_columns = add_columns
        .into_iter()
        .map(|name| {
            TestColumnDefBuilder::default()
                .name(name)
                .data_type(ColumnDataType::String)
                .is_nullable(true)
                .semantic_type(SemanticType::Tag)
                .comment("new column".to_string())
                .build()
                .unwrap()
                .into()
        })
        .collect::<Vec<_>>();
    let mut alter_table = TestAlterTableExprBuilder::default();
    if let Some(schema) = schema {
        alter_table.schema_name(schema.to_string());
    }
    let alter_table = alter_table
        .table_name(table.to_string())
        .add_columns(add_columns)
        .add_if_not_exists(true)
        .build()
        .unwrap();

    AlterTableTask {
        alter_table: alter_table.into(),
    }
}

fn make_alter_logical_table_rename_task(
    schema: &str,
    table: &str,
    new_table_name: &str,
) -> AlterTableTask {
    let alter_table = TestAlterTableExprBuilder::default()
        .schema_name(schema.to_string())
        .table_name(table.to_string())
        .new_table_name(new_table_name.to_string())
        .build()
        .unwrap();

    AlterTableTask {
        alter_table: alter_table.into(),
    }
}

fn make_alters_request_handler(
    column_metadatas: Vec<ColumnMetadata>,
) -> impl Fn(Peer, RegionRequest) -> Result<RegionResponse> {
    move |_peer: Peer, request: RegionRequest| {
        if let region_request::Body::Alters(_) = request.body.unwrap() {
            let mut response = RegionResponse::new(0);
            // Default region id for physical table.
            let region_id = RegionId::new(1000, 1);
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

fn assert_alters_request(
    peer: Peer,
    request: RegionRequest,
    expected_peer_id: u64,
    expected_region_ids: &[RegionId],
) {
    assert_eq!(peer.id, expected_peer_id,);
    let Some(region_request::Body::Alters(req)) = request.body else {
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

#[tokio::test]
async fn test_on_prepare_check_schema() {
    let node_manager = Arc::new(MockDatanodeManager::new(()));
    let ddl_context = new_ddl_context(node_manager);
    let tasks = vec![
        make_alter_logical_table_add_column_task(
            Some("schema1"),
            "table1",
            vec!["column1".to_string()],
        ),
        make_alter_logical_table_add_column_task(
            Some("schema2"),
            "table2",
            vec!["column2".to_string()],
        ),
    ];
    let physical_table_id = 1024u32;
    let mut procedure = AlterLogicalTablesProcedure::new(tasks, physical_table_id, ddl_context);
    let err = procedure.on_prepare().await.unwrap_err();
    assert_matches!(err, AlterLogicalTablesInvalidArguments { .. });
}

#[tokio::test]
async fn test_on_prepare_check_alter_kind() {
    let node_manager = Arc::new(MockDatanodeManager::new(()));
    let ddl_context = new_ddl_context(node_manager);
    let tasks = vec![make_alter_logical_table_rename_task(
        "schema1",
        "table1",
        "new_table1",
    )];
    let physical_table_id = 1024u32;
    let mut procedure = AlterLogicalTablesProcedure::new(tasks, physical_table_id, ddl_context);
    let err = procedure.on_prepare().await.unwrap_err();
    assert_matches!(err, AlterLogicalTablesInvalidArguments { .. });
}

#[tokio::test]
async fn test_on_prepare_different_physical_table() {
    let node_manager = Arc::new(MockDatanodeManager::new(()));
    let ddl_context = new_ddl_context(node_manager);

    let phy1_id = create_physical_table(&ddl_context, "phy1").await;
    create_logical_table(ddl_context.clone(), phy1_id, "table1").await;
    let phy2_id = create_physical_table(&ddl_context, "phy2").await;
    create_logical_table(ddl_context.clone(), phy2_id, "table2").await;

    let tasks = vec![
        make_alter_logical_table_add_column_task(None, "table1", vec!["column1".to_string()]),
        make_alter_logical_table_add_column_task(None, "table2", vec!["column2".to_string()]),
    ];

    let mut procedure = AlterLogicalTablesProcedure::new(tasks, phy1_id, ddl_context);
    let err = procedure.on_prepare().await.unwrap_err();
    assert_matches!(err, AlterLogicalTablesInvalidArguments { .. });
}

#[tokio::test]
async fn test_on_prepare_logical_table_not_exists() {
    let node_manager = Arc::new(MockDatanodeManager::new(()));
    let ddl_context = new_ddl_context(node_manager);

    // Creates physical table
    let phy_id = create_physical_table(&ddl_context, "phy").await;
    // Creates 3 logical tables
    create_logical_table(ddl_context.clone(), phy_id, "table1").await;

    let tasks = vec![
        make_alter_logical_table_add_column_task(None, "table1", vec!["column1".to_string()]),
        // table2 not exists
        make_alter_logical_table_add_column_task(None, "table2", vec!["column2".to_string()]),
    ];

    let mut procedure = AlterLogicalTablesProcedure::new(tasks, phy_id, ddl_context);
    let err = procedure.on_prepare().await.unwrap_err();
    assert_matches!(err, TableNotFound { .. });
}

#[tokio::test]
async fn test_on_prepare() {
    let node_manager = Arc::new(MockDatanodeManager::new(()));
    let ddl_context = new_ddl_context(node_manager);

    // Creates physical table
    let phy_id = create_physical_table(&ddl_context, "phy").await;
    // Creates 3 logical tables
    create_logical_table(ddl_context.clone(), phy_id, "table1").await;
    create_logical_table(ddl_context.clone(), phy_id, "table2").await;
    create_logical_table(ddl_context.clone(), phy_id, "table3").await;

    let tasks = vec![
        make_alter_logical_table_add_column_task(None, "table1", vec!["column1".to_string()]),
        make_alter_logical_table_add_column_task(None, "table2", vec!["column2".to_string()]),
        make_alter_logical_table_add_column_task(None, "table3", vec!["column3".to_string()]),
    ];

    let mut procedure = AlterLogicalTablesProcedure::new(tasks, phy_id, ddl_context);
    let result = procedure.on_prepare().await;
    assert_matches!(
        result,
        Ok(Status::Executing {
            persist: true,
            clean_poisons: false
        })
    );
}

#[tokio::test]
async fn test_on_update_metadata() {
    common_telemetry::init_default_ut_logging();
    let (tx, mut rx) = mpsc::channel(8);
    let test_column_metadatas = test_column_metadatas(&["new_col", "mew_col"]);
    let datanode_handler =
        DatanodeWatcher::new(tx).with_handler(make_alters_request_handler(test_column_metadatas));
    let node_manager = Arc::new(MockDatanodeManager::new(datanode_handler));
    let ddl_context = new_ddl_context(node_manager);

    // Creates physical table
    let phy_id = create_physical_table(&ddl_context, "phy").await;
    // Creates 3 logical tables
    let logical_table1_id = create_logical_table(ddl_context.clone(), phy_id, "table1").await;
    let logical_table2_id = create_logical_table(ddl_context.clone(), phy_id, "table2").await;
    let logical_table3_id = create_logical_table(ddl_context.clone(), phy_id, "table3").await;
    create_logical_table(ddl_context.clone(), phy_id, "table4").await;
    create_logical_table(ddl_context.clone(), phy_id, "table5").await;

    let tasks = vec![
        make_alter_logical_table_add_column_task(None, "table1", vec!["new_col".to_string()]),
        make_alter_logical_table_add_column_task(None, "table2", vec!["mew_col".to_string()]),
        make_alter_logical_table_add_column_task(None, "table3", vec!["new_col".to_string()]),
    ];

    let mut procedure = AlterLogicalTablesProcedure::new(tasks, phy_id, ddl_context.clone());
    let mut status = procedure.on_prepare().await.unwrap();
    assert_matches!(
        status,
        Status::Executing {
            persist: true,
            clean_poisons: false
        }
    );

    let ctx = common_procedure::Context {
        procedure_id: ProcedureId::random(),
        provider: Arc::new(MockContextProvider::default()),
    };
    // on_submit_alter_region_requests
    status = procedure.execute(&ctx).await.unwrap();
    assert_matches!(
        status,
        Status::Executing {
            persist: true,
            clean_poisons: false
        }
    );
    // on_update_metadata
    status = procedure.execute(&ctx).await.unwrap();
    assert_matches!(
        status,
        Status::Executing {
            persist: true,
            clean_poisons: false
        }
    );
    let (peer, request) = rx.try_recv().unwrap();
    rx.try_recv().unwrap_err();
    assert_alters_request(
        peer,
        request,
        0,
        &[
            RegionId::new(logical_table1_id, 0),
            RegionId::new(logical_table2_id, 0),
            RegionId::new(logical_table3_id, 0),
        ],
    );

    let table_info = get_raw_table_info(&ddl_context, phy_id).await;
    assert_column_name(
        &table_info,
        &["ts", "value", "__table_id", "__tsid", "new_col", "mew_col"],
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
async fn test_on_part_duplicate_alter_request() {
    common_telemetry::init_default_ut_logging();
    let (tx, mut rx) = mpsc::channel(8);
    let column_metadatas = test_column_metadatas(&["col_0"]);
    let handler =
        DatanodeWatcher::new(tx).with_handler(make_alters_request_handler(column_metadatas));
    let node_manager = Arc::new(MockDatanodeManager::new(handler));
    let mut ddl_context = new_ddl_context(node_manager);

    // Creates physical table
    let phy_id = create_physical_table(&ddl_context, "phy").await;
    // Creates 3 logical tables
    let logical_table1_id = create_logical_table(ddl_context.clone(), phy_id, "table1").await;
    let logical_table2_id = create_logical_table(ddl_context.clone(), phy_id, "table2").await;

    let tasks = vec![
        make_alter_logical_table_add_column_task(None, "table1", vec!["col_0".to_string()]),
        make_alter_logical_table_add_column_task(None, "table2", vec!["col_0".to_string()]),
    ];

    let mut procedure = AlterLogicalTablesProcedure::new(tasks, phy_id, ddl_context.clone());
    let mut status = procedure.on_prepare().await.unwrap();
    assert_matches!(
        status,
        Status::Executing {
            persist: true,
            clean_poisons: false
        }
    );

    let ctx = common_procedure::Context {
        procedure_id: ProcedureId::random(),
        provider: Arc::new(MockContextProvider::default()),
    };
    // on_submit_alter_region_requests
    status = procedure.execute(&ctx).await.unwrap();
    assert_matches!(
        status,
        Status::Executing {
            persist: true,
            clean_poisons: false
        }
    );
    // on_update_metadata
    status = procedure.execute(&ctx).await.unwrap();
    assert_matches!(
        status,
        Status::Executing {
            persist: true,
            clean_poisons: false
        }
    );
    let (peer, request) = rx.try_recv().unwrap();
    rx.try_recv().unwrap_err();
    assert_alters_request(
        peer,
        request,
        0,
        &[
            RegionId::new(logical_table1_id, 0),
            RegionId::new(logical_table2_id, 0),
        ],
    );

    let table_info = get_raw_table_info(&ddl_context, phy_id).await;
    assert_column_name(
        &table_info,
        &["ts", "value", "__table_id", "__tsid", "col_0"],
    );
    assert_eq!(
        table_info.meta.column_ids,
        vec![
            0,
            1,
            ReservedColumnId::table_id(),
            ReservedColumnId::tsid(),
            2
        ]
    );

    let (tx, mut rx) = mpsc::channel(8);
    let column_metadatas = test_column_metadatas(&["col_0", "new_col_1", "new_col_2"]);
    let handler =
        DatanodeWatcher::new(tx).with_handler(make_alters_request_handler(column_metadatas));
    let node_manager = Arc::new(MockDatanodeManager::new(handler));
    ddl_context.node_manager = node_manager;

    // re-alter
    let tasks = vec![
        make_alter_logical_table_add_column_task(
            None,
            "table1",
            vec!["col_0".to_string(), "new_col_1".to_string()],
        ),
        make_alter_logical_table_add_column_task(
            None,
            "table2",
            vec![
                "col_0".to_string(),
                "new_col_2".to_string(),
                "new_col_1".to_string(),
            ],
        ),
    ];

    let mut procedure = AlterLogicalTablesProcedure::new(tasks, phy_id, ddl_context.clone());
    let mut status = procedure.on_prepare().await.unwrap();
    assert_matches!(
        status,
        Status::Executing {
            persist: true,
            clean_poisons: false
        }
    );

    let ctx = common_procedure::Context {
        procedure_id: ProcedureId::random(),
        provider: Arc::new(MockContextProvider::default()),
    };
    // on_submit_alter_region_requests
    status = procedure.execute(&ctx).await.unwrap();
    assert_matches!(
        status,
        Status::Executing {
            persist: true,
            clean_poisons: false
        }
    );
    // on_update_metadata
    status = procedure.execute(&ctx).await.unwrap();
    assert_matches!(
        status,
        Status::Executing {
            persist: true,
            clean_poisons: false
        }
    );

    let (peer, request) = rx.try_recv().unwrap();
    rx.try_recv().unwrap_err();
    assert_alters_request(
        peer,
        request,
        0,
        &[
            RegionId::new(logical_table1_id, 0),
            RegionId::new(logical_table2_id, 0),
        ],
    );

    let table_info = get_raw_table_info(&ddl_context, phy_id).await;
    assert_column_name(
        &table_info,
        &[
            "ts",
            "value",
            "__table_id",
            "__tsid",
            "col_0",
            "new_col_1",
            "new_col_2",
        ],
    );
    assert_eq!(
        table_info.meta.column_ids,
        vec![
            0,
            1,
            ReservedColumnId::table_id(),
            ReservedColumnId::tsid(),
            2,
            3,
            4,
        ]
    );

    let table_name_keys = vec![
        TableNameKey::new(DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, "table1"),
        TableNameKey::new(DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, "table2"),
    ];
    let table_ids = ddl_context
        .table_metadata_manager
        .table_name_manager()
        .batch_get(table_name_keys)
        .await
        .unwrap()
        .into_iter()
        .map(|x| x.unwrap().table_id())
        .collect::<Vec<_>>();
    let tables = ddl_context
        .table_metadata_manager
        .table_info_manager()
        .batch_get(&table_ids)
        .await
        .unwrap();

    let table1 = tables.get(&table_ids[0]).unwrap();
    let table2 = tables.get(&table_ids[1]).unwrap();
    assert_eq!(table1.table_info.name, "table1");
    assert_eq!(table2.table_info.name, "table2");

    let table1_cols = table1
        .table_info
        .meta
        .schema
        .column_schemas()
        .iter()
        .map(|x| x.name.clone())
        .collect::<Vec<_>>();
    assert_eq!(
        table1_cols,
        vec![
            "col_0".to_string(),
            "cpu".to_string(),
            "host".to_string(),
            "new_col_1".to_string(),
            "ts".to_string()
        ]
    );

    let table2_cols = table2
        .table_info
        .meta
        .schema
        .column_schemas()
        .iter()
        .map(|x| x.name.clone())
        .collect::<Vec<_>>();
    assert_eq!(
        table2_cols,
        vec![
            "col_0".to_string(),
            "cpu".to_string(),
            "host".to_string(),
            "new_col_1".to_string(),
            "new_col_2".to_string(),
            "ts".to_string()
        ]
    );
}

#[tokio::test]
async fn test_on_submit_alter_region_request() {
    common_telemetry::init_default_ut_logging();
    let (tx, mut rx) = mpsc::channel(8);
    let column_metadatas = test_column_metadatas(&["new_col", "mew_col"]);
    let handler =
        DatanodeWatcher::new(tx).with_handler(make_alters_request_handler(column_metadatas));
    let node_manager = Arc::new(MockDatanodeManager::new(handler));
    let ddl_context = new_ddl_context(node_manager);

    let mut create_physical_table_task = test_create_physical_table_task("phy");
    let phy_id = 1000u32;
    let region_routes = vec![RegionRoute {
        region: Region::new_test(RegionId::new(phy_id, 1)),
        leader_peer: Some(Peer::empty(1)),
        follower_peers: vec![Peer::empty(5)],
        leader_state: None,
        leader_down_since: None,
        write_route_policy: None,
    }];
    create_physical_table_task.set_table_id(phy_id);
    create_physical_table_metadata(
        &ddl_context,
        create_physical_table_task.table_info.clone(),
        TableRouteValue::Physical(PhysicalTableRouteValue::new(region_routes)),
    )
    .await;
    create_logical_table(ddl_context.clone(), phy_id, "table1").await;
    create_logical_table(ddl_context.clone(), phy_id, "table2").await;

    let tasks = vec![
        make_alter_logical_table_add_column_task(None, "table1", vec!["new_col".to_string()]),
        make_alter_logical_table_add_column_task(None, "table2", vec!["mew_col".to_string()]),
    ];

    let mut procedure = AlterLogicalTablesProcedure::new(tasks, phy_id, ddl_context);
    procedure.on_prepare().await.unwrap();
    procedure.on_submit_alter_region_requests().await.unwrap();
    let mut results = Vec::new();
    for _ in 0..2 {
        let result = rx.try_recv().unwrap();
        results.push(result);
    }
    rx.try_recv().unwrap_err();
    let (peer, request) = results.remove(0);
    assert_eq!(peer.id, 1);
    assert_matches!(request.body.unwrap(), region_request::Body::Alters(_));
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
