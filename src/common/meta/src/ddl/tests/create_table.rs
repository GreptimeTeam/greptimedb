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

use api::v1::region::{QueryRequest, RegionRequest};
use api::v1::{ColumnDataType, SemanticType};
use common_error::ext::ErrorExt;
use common_error::status_code::StatusCode;
use common_procedure::Status;
use common_recordbatch::SendableRecordBatchStream;

use crate::ddl::create_table::CreateTableProcedure;
use crate::ddl::test_util::create_table::build_raw_table_info_from_expr;
use crate::ddl::test_util::{TestColumnDefBuilder, TestCreateTableExprBuilder};
use crate::error::{Error, Result};
use crate::key::table_route::TableRouteValue;
use crate::peer::Peer;
use crate::rpc::ddl::CreateTableTask;
use crate::test_util::{new_ddl_context, AffectedRows, MockDatanodeHandler, MockDatanodeManager};

#[async_trait::async_trait]
impl MockDatanodeHandler for () {
    async fn handle(&self, _peer: &Peer, _request: RegionRequest) -> Result<AffectedRows> {
        unreachable!()
    }

    async fn handle_query(
        &self,
        _peer: &Peer,
        _request: QueryRequest,
    ) -> Result<SendableRecordBatchStream> {
        unreachable!()
    }
}

fn test_create_table_task(name: &str) -> CreateTableTask {
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
        partitions: vec![],
        table_info,
    }
}

#[tokio::test]
async fn test_on_prepare_table_exists_err() {
    let datanode_manager = Arc::new(MockDatanodeManager::new(()));
    let ddl_context = new_ddl_context(datanode_manager);
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
    let datanode_manager = Arc::new(MockDatanodeManager::new(()));
    let ddl_context = new_ddl_context(datanode_manager);
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
    let datanode_manager = Arc::new(MockDatanodeManager::new(()));
    let ddl_context = new_ddl_context(datanode_manager);
    let cluster_id = 1;
    let mut task = test_create_table_task("foo");
    task.create_table.create_if_not_exists = true;
    let mut procedure = CreateTableProcedure::new(cluster_id, task, ddl_context);
    let status = procedure.on_prepare().await.unwrap();
    assert_matches!(status, Status::Executing { persist: true });
    assert_eq!(procedure.table_id(), 1024);
}
