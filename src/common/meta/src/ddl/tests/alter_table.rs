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

use api::v1::alter_table_expr::Kind;
use api::v1::region::{region_request, RegionRequest};
use api::v1::{
    AddColumn, AddColumns, AlterTableExpr, ColumnDataType, ColumnDef as PbColumnDef, DropColumn,
    DropColumns, SemanticType, SetTableOptions,
};
use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
use common_error::ext::ErrorExt;
use common_error::status_code::StatusCode;
use store_api::storage::RegionId;
use table::requests::TTL_KEY;
use tokio::sync::mpsc::{self};

use crate::ddl::alter_table::AlterTableProcedure;
use crate::ddl::test_util::alter_table::TestAlterTableExprBuilder;
use crate::ddl::test_util::create_table::test_create_table_task;
use crate::ddl::test_util::datanode_handler::{
    DatanodeWatcher, RequestOutdatedErrorDatanodeHandler,
};
use crate::key::datanode_table::DatanodeTableKey;
use crate::key::table_name::TableNameKey;
use crate::key::table_route::TableRouteValue;
use crate::peer::Peer;
use crate::rpc::ddl::AlterTableTask;
use crate::rpc::router::{Region, RegionRoute};
use crate::test_util::{new_ddl_context, MockDatanodeManager};

fn test_rename_alter_table_task(table_name: &str, new_table_name: &str) -> AlterTableTask {
    let builder = TestAlterTableExprBuilder::default()
        .table_name(table_name)
        .new_table_name(new_table_name)
        .build()
        .unwrap();

    AlterTableTask {
        alter_table: builder.into(),
    }
}

#[tokio::test]
async fn test_on_prepare_table_exists_err() {
    let node_manager = Arc::new(MockDatanodeManager::new(()));
    let ddl_context = new_ddl_context(node_manager);
    let cluster_id = 1;
    let task = test_create_table_task("foo", 1024);
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

    let task = test_rename_alter_table_task("non-exists", "foo");
    let mut procedure = AlterTableProcedure::new(cluster_id, 1024, task, ddl_context).unwrap();
    let err = procedure.on_prepare().await.unwrap_err();
    assert_matches!(err.status_code(), StatusCode::TableAlreadyExists);
}

#[tokio::test]
async fn test_on_prepare_table_not_exists_err() {
    let node_manager = Arc::new(MockDatanodeManager::new(()));
    let ddl_context = new_ddl_context(node_manager);
    let cluster_id = 1;
    let task = test_rename_alter_table_task("non-exists", "foo");
    let mut procedure = AlterTableProcedure::new(cluster_id, 1024, task, ddl_context).unwrap();
    let err = procedure.on_prepare().await.unwrap_err();
    assert_matches!(err.status_code(), StatusCode::TableNotFound);
}

#[tokio::test]
async fn test_on_submit_alter_request() {
    let (tx, mut rx) = mpsc::channel(8);
    let datanode_handler = DatanodeWatcher(tx);
    let node_manager = Arc::new(MockDatanodeManager::new(datanode_handler));
    let ddl_context = new_ddl_context(node_manager);
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
                    leader_state: None,
                    leader_down_since: None,
                },
                RegionRoute {
                    region: Region::new_test(RegionId::new(table_id, 2)),
                    leader_peer: Some(Peer::empty(2)),
                    follower_peers: vec![Peer::empty(4)],
                    leader_state: None,
                    leader_down_since: None,
                },
                RegionRoute {
                    region: Region::new_test(RegionId::new(table_id, 3)),
                    leader_peer: Some(Peer::empty(3)),
                    follower_peers: vec![],
                    leader_state: None,
                    leader_down_since: None,
                },
            ]),
            HashMap::new(),
        )
        .await
        .unwrap();

    let alter_table_task = AlterTableTask {
        alter_table: AlterTableExpr {
            catalog_name: DEFAULT_CATALOG_NAME.to_string(),
            schema_name: DEFAULT_SCHEMA_NAME.to_string(),
            table_name: table_name.to_string(),
            kind: Some(Kind::DropColumns(DropColumns {
                drop_columns: vec![DropColumn {
                    name: "my_field_column".to_string(),
                }],
            })),
        },
    };
    let mut procedure =
        AlterTableProcedure::new(cluster_id, table_id, alter_table_task, ddl_context).unwrap();
    procedure.on_prepare().await.unwrap();
    procedure.submit_alter_region_requests().await.unwrap();

    let check = |peer: Peer,
                 request: RegionRequest,
                 expected_peer_id: u64,
                 expected_region_id: RegionId| {
        assert_eq!(peer.id, expected_peer_id);
        let Some(region_request::Body::Alter(req)) = request.body else {
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
async fn test_on_submit_alter_request_with_outdated_request() {
    let node_manager = Arc::new(MockDatanodeManager::new(
        RequestOutdatedErrorDatanodeHandler,
    ));
    let ddl_context = new_ddl_context(node_manager);
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
                    leader_state: None,
                    leader_down_since: None,
                },
                RegionRoute {
                    region: Region::new_test(RegionId::new(table_id, 2)),
                    leader_peer: Some(Peer::empty(2)),
                    follower_peers: vec![Peer::empty(4)],
                    leader_state: None,
                    leader_down_since: None,
                },
                RegionRoute {
                    region: Region::new_test(RegionId::new(table_id, 3)),
                    leader_peer: Some(Peer::empty(3)),
                    follower_peers: vec![],
                    leader_state: None,
                    leader_down_since: None,
                },
            ]),
            HashMap::new(),
        )
        .await
        .unwrap();

    let alter_table_task = AlterTableTask {
        alter_table: AlterTableExpr {
            catalog_name: DEFAULT_CATALOG_NAME.to_string(),
            schema_name: DEFAULT_SCHEMA_NAME.to_string(),
            table_name: table_name.to_string(),
            kind: Some(Kind::DropColumns(DropColumns {
                drop_columns: vec![DropColumn {
                    name: "my_field_column".to_string(),
                }],
            })),
        },
    };
    let mut procedure =
        AlterTableProcedure::new(cluster_id, table_id, alter_table_task, ddl_context).unwrap();
    procedure.on_prepare().await.unwrap();
    procedure.submit_alter_region_requests().await.unwrap();
}

#[tokio::test]
async fn test_on_update_metadata_rename() {
    let node_manager = Arc::new(MockDatanodeManager::new(()));
    let ddl_context = new_ddl_context(node_manager);
    let cluster_id = 1;
    let table_name = "foo";
    let new_table_name = "bar";
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

    let task = test_rename_alter_table_task(table_name, new_table_name);
    let mut procedure =
        AlterTableProcedure::new(cluster_id, table_id, task, ddl_context.clone()).unwrap();
    procedure.on_prepare().await.unwrap();
    procedure.on_update_metadata().await.unwrap();

    let old_table_name_exists = ddl_context
        .table_metadata_manager
        .table_name_manager()
        .exists(TableNameKey::new(
            DEFAULT_CATALOG_NAME,
            DEFAULT_SCHEMA_NAME,
            table_name,
        ))
        .await
        .unwrap();
    assert!(!old_table_name_exists);
    let value = ddl_context
        .table_metadata_manager
        .table_name_manager()
        .get(TableNameKey::new(
            DEFAULT_CATALOG_NAME,
            DEFAULT_SCHEMA_NAME,
            new_table_name,
        ))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(value.table_id(), table_id);
}

#[tokio::test]
async fn test_on_update_metadata_add_columns() {
    let node_manager = Arc::new(MockDatanodeManager::new(()));
    let ddl_context = new_ddl_context(node_manager);
    let cluster_id = 1;
    let table_name = "foo";
    let table_id = 1024;
    let task = test_create_table_task(table_name, table_id);

    let region_id = RegionId::new(table_id, 0);
    let mock_table_routes = vec![RegionRoute {
        region: Region::new_test(region_id),
        leader_peer: Some(Peer::default()),
        follower_peers: vec![],
        leader_state: None,
        leader_down_since: None,
    }];
    // Puts a value to table name key.
    ddl_context
        .table_metadata_manager
        .create_table_metadata(
            task.table_info.clone(),
            TableRouteValue::physical(mock_table_routes),
            HashMap::new(),
        )
        .await
        .unwrap();

    let task = AlterTableTask {
        alter_table: AlterTableExpr {
            catalog_name: DEFAULT_CATALOG_NAME.to_string(),
            schema_name: DEFAULT_SCHEMA_NAME.to_string(),
            table_name: table_name.to_string(),
            kind: Some(Kind::AddColumns(AddColumns {
                add_columns: vec![AddColumn {
                    column_def: Some(PbColumnDef {
                        name: "my_tag3".to_string(),
                        data_type: ColumnDataType::String as i32,
                        semantic_type: SemanticType::Tag as i32,
                        is_nullable: true,
                        ..Default::default()
                    }),
                    location: None,
                }],
            })),
        },
    };
    let mut procedure =
        AlterTableProcedure::new(cluster_id, table_id, task, ddl_context.clone()).unwrap();
    procedure.on_prepare().await.unwrap();
    procedure.submit_alter_region_requests().await.unwrap();
    procedure.on_update_metadata().await.unwrap();

    let table_info = ddl_context
        .table_metadata_manager
        .table_info_manager()
        .get(table_id)
        .await
        .unwrap()
        .unwrap()
        .into_inner()
        .table_info;

    assert_eq!(
        table_info.meta.schema.column_schemas.len() as u32,
        table_info.meta.next_column_id
    );
}

#[tokio::test]
async fn test_on_update_table_options() {
    let node_manager = Arc::new(MockDatanodeManager::new(()));
    let ddl_context = new_ddl_context(node_manager);
    let cluster_id = 1;
    let table_name = "foo";
    let table_id = 1024;
    let task = test_create_table_task(table_name, table_id);

    let region_id = RegionId::new(table_id, 0);
    let mock_table_routes = vec![RegionRoute {
        region: Region::new_test(region_id),
        leader_peer: Some(Peer::default()),
        follower_peers: vec![],
        leader_state: None,
        leader_down_since: None,
    }];
    // Puts a value to table name key.
    ddl_context
        .table_metadata_manager
        .create_table_metadata(
            task.table_info.clone(),
            TableRouteValue::physical(mock_table_routes),
            HashMap::new(),
        )
        .await
        .unwrap();

    let task = AlterTableTask {
        alter_table: AlterTableExpr {
            catalog_name: DEFAULT_CATALOG_NAME.to_string(),
            schema_name: DEFAULT_SCHEMA_NAME.to_string(),
            table_name: table_name.to_string(),
            kind: Some(Kind::SetTableOptions(SetTableOptions {
                table_options: vec![api::v1::Option {
                    key: TTL_KEY.to_string(),
                    value: "1d".to_string(),
                }],
            })),
        },
    };
    let mut procedure =
        AlterTableProcedure::new(cluster_id, table_id, task, ddl_context.clone()).unwrap();
    procedure.on_prepare().await.unwrap();
    procedure.submit_alter_region_requests().await.unwrap();
    procedure.on_update_metadata().await.unwrap();

    let table_info = ddl_context
        .table_metadata_manager
        .table_info_manager()
        .get(table_id)
        .await
        .unwrap()
        .unwrap()
        .into_inner()
        .table_info;

    let datanode_key = DatanodeTableKey::new(0, table_id);
    let region_info = ddl_context
        .table_metadata_manager
        .datanode_table_manager()
        .get(&datanode_key)
        .await
        .unwrap()
        .unwrap()
        .region_info;

    assert_eq!(
        region_info.region_options,
        HashMap::from(&table_info.meta.options)
    );
}
