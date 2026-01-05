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

//! Mock test for adapter module
//! TODO(discord9): write mock test

use datatypes::schema::{ColumnSchema, SchemaBuilder};
use store_api::storage::ConcreteDataType;
use table::metadata::{TableInfo, TableInfoBuilder, TableMetaBuilder};

use super::*;

pub fn new_test_table_info_with_name<I: IntoIterator<Item = u32>>(
    table_id: TableId,
    table_name: &str,
    _region_numbers: I,
) -> TableInfo {
    let column_schemas = vec![
        ColumnSchema::new("number", ConcreteDataType::int32_datatype(), true),
        ColumnSchema::new(
            "ts",
            ConcreteDataType::timestamp_millisecond_datatype(),
            false,
        )
        .with_time_index(true),
    ];
    let schema = SchemaBuilder::try_from(column_schemas)
        .unwrap()
        .version(123)
        .build()
        .unwrap();

    let meta = TableMetaBuilder::empty()
        .schema(Arc::new(schema))
        .primary_key_indices(vec![0])
        .engine("engine")
        .next_column_id(3)
        .build()
        .unwrap();
    TableInfoBuilder::default()
        .table_id(table_id)
        .table_version(5)
        .name(table_name)
        .meta(meta)
        .build()
        .unwrap()
}

/// Create a mock harness for flow node manager
///
/// containing several default table info and schema
fn mock_harness_flow_node_manager() {}
