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

use api::v1::alter_expr::Kind;
use api::v1::meta::Partition;
use api::v1::region::{region_request, QueryRequest, RegionRequest};
use api::v1::{
    AddColumn, AddColumns, AlterExpr, ColumnDataType, ColumnDef as PbColumnDef, DropColumn,
    DropColumns, SemanticType,
};
use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
use common_error::ext::ErrorExt;
use common_error::status_code::StatusCode;
use common_recordbatch::SendableRecordBatchStream;
use common_telemetry::debug;
use store_api::storage::RegionId;
use table::metadata::TableId;
use tokio::sync::mpsc::{self};

use crate::datanode_manager::HandleResponse;
use crate::ddl::alter_table::AlterTableProcedure;
use crate::ddl::test_util::alter_table::TestAlterTableExprBuilder;
use crate::ddl::test_util::columns::TestColumnDefBuilder;
use crate::ddl::test_util::create_table::{
    build_raw_table_info_from_expr, TestCreateTableExprBuilder,
};
use crate::error::Result;
use crate::key::table_name::TableNameKey;
use crate::key::table_route::TableRouteValue;
use crate::peer::Peer;
use crate::rpc::ddl::{AlterTableTask, CreateTableTask};
use crate::rpc::router::{Region, RegionRoute};
use crate::test_util::{new_ddl_context, MockDatanodeHandler, MockDatanodeManager};

pub fn test_create_table_task(name: &str, table_id: TableId) -> CreateTableTask {
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
        .table_id(table_id)
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
    let datanode_manager = Arc::new(MockDatanodeManager::new(()));
    let ddl_context = new_ddl_context(datanode_manager);
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
    let datanode_manager = Arc::new(MockDatanodeManager::new(()));
    let ddl_context = new_ddl_context(datanode_manager);
    let cluster_id = 1;
    let task = test_rename_alter_table_task("non-exists", "foo");
    let mut procedure = AlterTableProcedure::new(cluster_id, 1024, task, ddl_context).unwrap();
    let err = procedure.on_prepare().await.unwrap_err();
    assert_matches!(err.status_code(), StatusCode::TableNotFound);
}

#[derive(Clone)]
pub struct DatanodeWatcher(mpsc::Sender<(Peer, RegionRequest)>);

#[async_trait::async_trait]
impl MockDatanodeHandler for DatanodeWatcher {
    async fn handle(&self, peer: &Peer, request: RegionRequest) -> Result<HandleResponse> {
        debug!("Returning Ok(0) for request: {request:?}, peer: {peer:?}");
        self.0.send((peer.clone(), request)).await.unwrap();
        Ok(HandleResponse::new(0))
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
async fn test_on_submit_alter_request() {
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

    let alter_table_task = AlterTableTask {
        alter_table: AlterExpr {
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
async fn test_on_update_metadata_rename() {
    let datanode_manager = Arc::new(MockDatanodeManager::new(()));
    let ddl_context = new_ddl_context(datanode_manager);
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

    let task = AlterTableTask {
        alter_table: AlterExpr {
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
