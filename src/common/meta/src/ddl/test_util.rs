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
pub mod region_metadata;

use std::assert_matches::assert_matches;
use std::collections::HashMap;

use api::v1::meta::Partition;
use api::v1::{ColumnDataType, SemanticType};
use common_procedure::Status;
use datatypes::prelude::ConcreteDataType;
use datatypes::schema::ColumnSchema;
use store_api::metadata::ColumnMetadata;
use store_api::metric_engine_consts::{
    DATA_SCHEMA_TABLE_ID_COLUMN_NAME, DATA_SCHEMA_TSID_COLUMN_NAME, LOGICAL_TABLE_METADATA_KEY,
    METRIC_ENGINE_NAME,
};
use store_api::storage::consts::ReservedColumnId;
use table::metadata::{TableId, TableInfo};

use crate::ddl::create_logical_tables::CreateLogicalTablesProcedure;
use crate::ddl::test_util::columns::TestColumnDefBuilder;
use crate::ddl::test_util::create_table::{
    TestCreateTableExprBuilder, build_raw_table_info_from_expr,
};
use crate::ddl::{DdlContext, TableMetadata};
use crate::key::table_route::TableRouteValue;
use crate::rpc::ddl::CreateTableTask;

pub async fn create_physical_table_metadata(
    ddl_context: &DdlContext,
    table_info: TableInfo,
    table_route: TableRouteValue,
) {
    ddl_context
        .table_metadata_manager
        .create_table_metadata(table_info, table_route, HashMap::default())
        .await
        .unwrap();
}

pub async fn create_physical_table(ddl_context: &DdlContext, name: &str) -> TableId {
    // Prepares physical table metadata.
    let mut create_physical_table_task = test_create_physical_table_task(name);
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
        ddl_context,
        create_physical_table_task.table_info.clone(),
        TableRouteValue::Physical(table_route),
    )
    .await;

    table_id
}

pub async fn create_logical_table(
    ddl_context: DdlContext,
    physical_table_id: TableId,
    table_name: &str,
) -> TableId {
    let tasks = vec![test_create_logical_table_task(table_name)];
    let mut procedure = CreateLogicalTablesProcedure::new(tasks, physical_table_id, ddl_context);
    let status = procedure.on_prepare().await.unwrap();
    assert_matches!(
        status,
        Status::Executing {
            persist: true,
            clean_poisons: false
        }
    );
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
        partitions: vec![Partition::default()],
        table_info,
    }
}

/// Creates a physical table task with a single region.
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
        partitions: vec![Partition::default()],
        table_info,
    }
}

/// Creates a column metadata list with tag fields.
pub fn test_column_metadatas(tag_fields: &[&str]) -> Vec<ColumnMetadata> {
    let mut output = Vec::with_capacity(tag_fields.len() + 4);
    output.extend([
        ColumnMetadata {
            column_schema: ColumnSchema::new(
                "ts",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            ),
            semantic_type: SemanticType::Timestamp,
            column_id: 0,
        },
        ColumnMetadata {
            column_schema: ColumnSchema::new("value", ConcreteDataType::float64_datatype(), false),
            semantic_type: SemanticType::Field,
            column_id: 1,
        },
        ColumnMetadata {
            column_schema: ColumnSchema::new(
                DATA_SCHEMA_TABLE_ID_COLUMN_NAME,
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            ),
            semantic_type: SemanticType::Tag,
            column_id: ReservedColumnId::table_id(),
        },
        ColumnMetadata {
            column_schema: ColumnSchema::new(
                DATA_SCHEMA_TSID_COLUMN_NAME,
                ConcreteDataType::float64_datatype(),
                false,
            ),
            semantic_type: SemanticType::Tag,
            column_id: ReservedColumnId::tsid(),
        },
    ]);

    for (i, name) in tag_fields.iter().enumerate() {
        output.push(ColumnMetadata {
            column_schema: ColumnSchema::new(
                name.to_string(),
                ConcreteDataType::string_datatype(),
                true,
            ),
            semantic_type: SemanticType::Tag,
            column_id: (i + 2) as u32,
        });
    }

    output
}

/// Asserts the column names.
pub fn assert_column_name(table_info: &TableInfo, expected_column_names: &[&str]) {
    assert_eq!(
        table_info
            .meta
            .schema
            .column_schemas()
            .iter()
            .map(|c| c.name.clone())
            .collect::<Vec<_>>(),
        expected_column_names
    );
}

/// Asserts the column metadatas
pub fn assert_column_name_and_id(column_metadatas: &[ColumnMetadata], expected: &[(&str, u32)]) {
    assert_eq!(expected.len(), column_metadatas.len());
    for (name, id) in expected {
        let column_metadata = column_metadatas
            .iter()
            .find(|c| c.column_id == *id)
            .unwrap();
        assert_eq!(column_metadata.column_schema.name, *name);
    }
}

/// Gets the raw table info.
pub async fn get_raw_table_info(ddl_context: &DdlContext, table_id: TableId) -> TableInfo {
    ddl_context
        .table_metadata_manager
        .table_info_manager()
        .get(table_id)
        .await
        .unwrap()
        .unwrap()
        .into_inner()
        .table_info
}
