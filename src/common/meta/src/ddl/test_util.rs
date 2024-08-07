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

pub mod alter_table;
pub mod columns;
pub mod create_table;
pub mod datanode_handler;
pub mod flownode_handler;

use std::collections::HashMap;

use api::v1::meta::Partition;
use api::v1::{ColumnDataType, SemanticType};
use common_procedure::Status;
use store_api::metric_engine_consts::{LOGICAL_TABLE_METADATA_KEY, METRIC_ENGINE_NAME};
use table::metadata::{RawTableInfo, TableId};

use crate::ddl::create_logical_tables::CreateLogicalTablesProcedure;
use crate::ddl::test_util::columns::TestColumnDefBuilder;
use crate::ddl::test_util::create_table::{
    build_raw_table_info_from_expr, TestCreateTableExprBuilder,
};
use crate::ddl::{DdlContext, TableMetadata, TableMetadataAllocatorContext};
use crate::key::table_route::TableRouteValue;
use crate::rpc::ddl::CreateTableTask;
use crate::ClusterId;

pub async fn create_physical_table_metadata(
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

pub async fn create_physical_table(
    ddl_context: &DdlContext,
    cluster_id: ClusterId,
    name: &str,
) -> TableId {
    // Prepares physical table metadata.
    let mut create_physical_table_task = test_create_physical_table_task(name);
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
        ddl_context,
        create_physical_table_task.table_info.clone(),
        TableRouteValue::Physical(table_route),
    )
    .await;

    table_id
}

pub async fn create_logical_table(
    ddl_context: DdlContext,
    cluster_id: ClusterId,
    physical_table_id: TableId,
    table_name: &str,
) -> TableId {
    use std::assert_matches::assert_matches;

    let tasks = vec![test_create_logical_table_task(table_name)];
    let mut procedure =
        CreateLogicalTablesProcedure::new(cluster_id, tasks, physical_table_id, ddl_context);
    let status = procedure.on_prepare().await.unwrap();
    assert_matches!(status, Status::Executing { persist: true });
    let status = procedure.on_create_metadata().await.unwrap();
    assert_matches!(status, Status::Done { .. });

    let Status::Done {
        output: Some(output),
    } = status
    else {
        panic!("Unexpected status: {:?}", status);
    };
    output.downcast_ref::<Vec<u32>>().unwrap()[0]
}

pub fn test_create_logical_table_task(name: &str) -> CreateTableTask {
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
        .engine(METRIC_ENGINE_NAME)
        .table_options(HashMap::from([(
            LOGICAL_TABLE_METADATA_KEY.to_string(),
            "phy".to_string(),
        )]))
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

pub fn test_create_physical_table_task(name: &str) -> CreateTableTask {
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
        .engine(METRIC_ENGINE_NAME)
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
