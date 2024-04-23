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

use std::collections::HashSet;

use api::v1::SemanticType;
use store_api::metadata::ColumnMetadata;
use table::metadata::RawTableInfo;

/// Generate the new physical table info.
pub(crate) fn build_new_physical_table_info(
    mut raw_table_info: RawTableInfo,
    physical_columns: &[ColumnMetadata],
) -> RawTableInfo {
    let existing_columns = raw_table_info
        .meta
        .schema
        .column_schemas
        .iter()
        .map(|col| col.name.clone())
        .collect::<HashSet<_>>();
    let primary_key_indices = &mut raw_table_info.meta.primary_key_indices;
    let value_indices = &mut raw_table_info.meta.value_indices;
    value_indices.clear();
    let time_index = &mut raw_table_info.meta.schema.timestamp_index;
    let columns = &mut raw_table_info.meta.schema.column_schemas;
    columns.clear();

    for (idx, col) in physical_columns.iter().enumerate() {
        match col.semantic_type {
            SemanticType::Tag => {
                // push new primary key to the end.
                if !existing_columns.contains(&col.column_schema.name) {
                    primary_key_indices.push(idx);
                }
            }
            SemanticType::Field => value_indices.push(idx),
            SemanticType::Timestamp => *time_index = Some(idx),
        }

        columns.push(col.column_schema.clone());
    }

    if let Some(time_index) = *time_index {
        raw_table_info.meta.schema.column_schemas[time_index].set_time_index();
    }

    raw_table_info
}
