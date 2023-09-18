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

use datatypes::schema::SchemaRef;

use crate::metadata::{TableInfo, TableInfoBuilder, TableMetaBuilder, TableType, TableVersion};

pub fn test_table_info(
    table_id: u32,
    table_name: &str,
    schema_name: &str,
    catalog_name: &str,
    schema: SchemaRef,
) -> TableInfo {
    let meta = TableMetaBuilder::default()
        .schema(schema)
        .primary_key_indices(vec![])
        .value_indices(vec![])
        .engine("mito".to_string())
        .next_column_id(0)
        .options(Default::default())
        .created_on(Default::default())
        .region_numbers(vec![1])
        .build()
        .unwrap();

    TableInfoBuilder::default()
        .table_id(table_id)
        .table_version(0 as TableVersion)
        .name(table_name.to_string())
        .schema_name(schema_name.to_string())
        .catalog_name(catalog_name.to_string())
        .desc(None)
        .table_type(TableType::Base)
        .meta(meta)
        .build()
        .unwrap()
}
