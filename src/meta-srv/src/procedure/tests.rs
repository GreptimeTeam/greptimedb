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

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use api::v1::region::region_request::{self, Body as PbRegionRequest};
use api::v1::region::{ColumnDef, CreateRequest as PbCreateRegionRequest};
use api::v1::{ColumnDataType, ColumnDef as PbColumnDef, CreateTableExpr, SemanticType};
use client::client_manager::DatanodeClients;
use common_catalog::consts::MITO2_ENGINE;
use common_meta::ddl::create_table::*;
use common_meta::ddl::drop_table::DropTableProcedure;
use common_meta::key::table_info::TableInfoValue;
use common_meta::key::table_route::TableRouteValue;
use common_meta::rpc::ddl::{CreateTableTask, DropTableTask};
use common_meta::rpc::router::find_leaders;
use common_procedure::Status;
use store_api::storage::RegionId;

use crate::procedure::utils::mock::EchoRegionServer;
use crate::procedure::utils::test_data;

fn create_table_procedure() -> CreateTableProcedure {
    let create_table_expr = CreateTableExpr {
        catalog_name: "my_catalog".to_string(),
        schema_name: "my_schema".to_string(),
        table_name: "my_table".to_string(),
        desc: "blabla".to_string(),
        column_defs: vec![
            PbColumnDef {
                name: "ts".to_string(),
                datatype: ColumnDataType::TimestampMillisecond as i32,
                is_nullable: false,
                default_constraint: vec![],
            },
            PbColumnDef {
                name: "my_tag1".to_string(),
                datatype: ColumnDataType::String as i32,
                is_nullable: true,
                default_constraint: vec![],
            },
            PbColumnDef {
                name: "my_tag2".to_string(),
                datatype: ColumnDataType::String as i32,
                is_nullable: true,
                default_constraint: vec![],
            },
            PbColumnDef {
                name: "my_field_column".to_string(),
                datatype: ColumnDataType::Int32 as i32,
                is_nullable: true,
                default_constraint: vec![],
            },
        ],
        time_index: "ts".to_string(),
        primary_keys: vec!["my_tag2".to_string(), "my_tag1".to_string()],
        create_if_not_exists: false,
        table_options: HashMap::new(),
        table_id: None,
        region_numbers: vec![1, 2, 3],
        engine: MITO2_ENGINE.to_string(),
    };

    CreateTableProcedure::new(
        1,
        CreateTableTask::new(create_table_expr, vec![], test_data::new_table_info()),
        test_data::new_region_routes(),
        test_data::new_ddl_context(),
    )
}

#[test]
fn test_create_region_request_template() {
    let procedure = create_table_procedure();

    let template = procedure.create_region_request_template().unwrap();

    let expected = PbCreateRegionRequest {
        region_id: 0,
        engine: MITO2_ENGINE.to_string(),
        column_defs: vec![
            ColumnDef {
                name: "ts".to_string(),
                column_id: 0,
                datatype: ColumnDataType::TimestampMillisecond as i32,
                is_nullable: false,
                default_constraint: vec![],
                semantic_type: SemanticType::Timestamp as i32,
            },
            ColumnDef {
                name: "my_tag1".to_string(),
                column_id: 1,
                datatype: ColumnDataType::String as i32,
                is_nullable: true,
                default_constraint: vec![],
                semantic_type: SemanticType::Tag as i32,
            },
            ColumnDef {
                name: "my_tag2".to_string(),
                column_id: 2,
                datatype: ColumnDataType::String as i32,
                is_nullable: true,
                default_constraint: vec![],
                semantic_type: SemanticType::Tag as i32,
            },
            ColumnDef {
                name: "my_field_column".to_string(),
                column_id: 3,
                datatype: ColumnDataType::Int32 as i32,
                is_nullable: true,
                default_constraint: vec![],
                semantic_type: SemanticType::Field as i32,
            },
        ],
        primary_key: vec![2, 1],
        create_if_not_exists: true,
        catalog: String::new(),
        schema: String::new(),
        options: HashMap::new(),
    };
    assert_eq!(template, expected);
}

#[tokio::test]
async fn test_on_datanode_create_regions() {
    let mut procedure = create_table_procedure();

    let (region_server, mut rx) = EchoRegionServer::new();

    let datanodes = find_leaders(&procedure.creator.data.region_routes);
    let datanode_clients = DatanodeClients::default();
    for peer in datanodes {
        let client = region_server.new_client(&peer);
        datanode_clients.insert_client(peer, client).await;
    }

    procedure.context.datanode_manager = Arc::new(datanode_clients);

    let expected_created_regions = Arc::new(Mutex::new(HashSet::from([
        RegionId::new(42, 1),
        RegionId::new(42, 2),
        RegionId::new(42, 3),
    ])));
    let handle = tokio::spawn({
        let expected_created_regions = expected_created_regions.clone();
        let mut max_recv = expected_created_regions.lock().unwrap().len();
        async move {
            while let Some(PbRegionRequest::Create(request)) = rx.recv().await {
                let region_id = RegionId::from_u64(request.region_id);

                expected_created_regions.lock().unwrap().remove(&region_id);

                max_recv -= 1;
                if max_recv == 0 {
                    break;
                }
            }
        }
    });

    let status = procedure.on_datanode_create_regions().await.unwrap();
    assert!(matches!(status, Status::Executing { persist: true }));
    assert!(matches!(
        procedure.creator.data.state,
        CreateTableState::CreateMetadata
    ));

    handle.await.unwrap();

    assert!(expected_created_regions.lock().unwrap().is_empty());
}

#[tokio::test]
async fn test_on_datanode_drop_regions() {
    let drop_table_task = DropTableTask {
        catalog: "my_catalog".to_string(),
        schema: "my_schema".to_string(),
        table: "my_table".to_string(),
        table_id: 42,
    };
    let mut procedure = DropTableProcedure::new(
        1,
        drop_table_task,
        TableRouteValue::new(test_data::new_region_routes()),
        TableInfoValue::new(test_data::new_table_info()),
        test_data::new_ddl_context(),
    );

    let (region_server, mut rx) = EchoRegionServer::new();

    let datanodes = find_leaders(&procedure.data.table_route_value.region_routes);
    let datanode_clients = DatanodeClients::default();
    for peer in datanodes {
        let client = region_server.new_client(&peer);
        datanode_clients.insert_client(peer, client).await;
    }

    procedure.context.datanode_manager = Arc::new(datanode_clients);

    let expected_dropped_regions = Arc::new(Mutex::new(HashSet::from([
        RegionId::new(42, 1),
        RegionId::new(42, 2),
        RegionId::new(42, 3),
    ])));
    let handle = tokio::spawn({
        let expected_dropped_regions = expected_dropped_regions.clone();
        let mut max_recv = expected_dropped_regions.lock().unwrap().len();
        async move {
            while let Some(region_request::Body::Drop(request)) = rx.recv().await {
                let region_id = RegionId::from_u64(request.region_id);

                expected_dropped_regions.lock().unwrap().remove(&region_id);

                max_recv -= 1;
                if max_recv == 0 {
                    break;
                }
            }
        }
    });

    let status = procedure.on_datanode_drop_regions().await.unwrap();
    assert!(matches!(status, Status::Done));

    handle.await.unwrap();

    assert!(expected_dropped_regions.lock().unwrap().is_empty());
}
