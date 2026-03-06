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
use std::sync::Arc;

use api::v1::SemanticType;
use common_telemetry::debug;
use common_telemetry::tracing::warn;
use datatypes::schema::Schema;
use store_api::metadata::ColumnMetadata;
use table::metadata::TableInfo;

/// Generate the new physical table info.
pub(crate) fn build_new_physical_table_info(
    mut table_info: TableInfo,
    physical_columns: &[ColumnMetadata],
) -> TableInfo {
    debug!(
        "building new physical table info for table: {}, table_id: {}",
        table_info.name, table_info.ident.table_id
    );
    let existing_columns = table_info
        .meta
        .schema
        .column_schemas()
        .iter()
        .map(|col| col.name.clone())
        .collect::<HashSet<_>>();
    let primary_key_indices = &mut table_info.meta.primary_key_indices;
    let value_indices = &mut table_info.meta.value_indices;
    value_indices.clear();
    let column_ids = &mut table_info.meta.column_ids;
    column_ids.clear();

    let mut columns = Vec::with_capacity(physical_columns.len());
    for (idx, col) in physical_columns.iter().enumerate() {
        match col.semantic_type {
            SemanticType::Tag => {
                // push new primary key to the end.
                if !existing_columns.contains(&col.column_schema.name) {
                    primary_key_indices.push(idx);
                }
            }
            SemanticType::Field => value_indices.push(idx),
            SemanticType::Timestamp => {
                value_indices.push(idx);
            }
        }

        columns.push(col.column_schema.clone());
        column_ids.push(col.column_id);
    }

    table_info.meta.schema = Arc::new(Schema::new_with_version(
        columns,
        table_info.meta.schema.version(),
    ));
    table_info
}

/// Updates the column IDs in the table info based on the provided column metadata.
///
/// This function validates that the column metadata matches the existing table schema
/// before updating the column ids. If the column metadata doesn't match the table schema,
/// the table info remains unchanged.
pub(crate) fn update_table_info_column_ids(
    table_info: &mut TableInfo,
    column_metadatas: &[ColumnMetadata],
) {
    let mut table_column_names = table_info
        .meta
        .schema
        .column_schemas()
        .iter()
        .map(|c| c.name.as_str())
        .collect::<Vec<_>>();
    table_column_names.sort_unstable();

    let mut column_names = column_metadatas
        .iter()
        .map(|c| c.column_schema.name.as_str())
        .collect::<Vec<_>>();
    column_names.sort_unstable();

    if table_column_names != column_names {
        warn!(
            "Column metadata doesn't match the table schema for table {}, table_id: {}, column in table: {:?}, column in metadata: {:?}",
            table_info.name, table_info.ident.table_id, table_column_names, column_names,
        );
        return;
    }

    let name_to_id = column_metadatas
        .iter()
        .map(|c| (c.column_schema.name.clone(), c.column_id))
        .collect::<HashMap<_, _>>();

    let schema = table_info.meta.schema.column_schemas();
    let mut column_ids = Vec::with_capacity(schema.len());
    for column_schema in schema {
        if let Some(id) = name_to_id.get(&column_schema.name) {
            column_ids.push(*id);
        }
    }

    table_info.meta.column_ids = column_ids;
}
