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
use datatypes::schema::{ColumnSchema, Schema};
use snafu::ensure;
use store_api::metadata::ColumnMetadata;
use table::metadata::TableInfo;

use crate::error::{MetadataCorruptionSnafu, Result};

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

/// Returns the stable IDs for `logical_columns` from the authoritative physical table.
///
/// The physical table must have a complete, unambiguous schema-to-ID mapping. Logical
/// columns may be a subset of physical columns, but every logical column must exist in
/// the physical table.
pub(crate) fn logical_column_ids_from_physical_table_info(
    physical_table_info: &TableInfo,
    logical_columns: &[ColumnSchema],
) -> Result<Vec<u32>> {
    let physical_columns = physical_table_info.meta.schema.column_schemas();
    let physical_column_ids = &physical_table_info.meta.column_ids;
    ensure!(
        physical_columns.len() == physical_column_ids.len(),
        MetadataCorruptionSnafu {
            err_msg: format!(
                "Physical table {} ({}) has {} columns but {} column IDs",
                physical_table_info.name,
                physical_table_info.ident.table_id,
                physical_columns.len(),
                physical_column_ids.len(),
            ),
        }
    );

    let mut ids_by_name = HashMap::with_capacity(physical_columns.len());
    let mut seen_ids = HashSet::with_capacity(physical_column_ids.len());
    for (column, column_id) in physical_columns.iter().zip(physical_column_ids) {
        ensure!(
            ids_by_name
                .insert(column.name.as_str(), *column_id)
                .is_none(),
            MetadataCorruptionSnafu {
                err_msg: format!(
                    "Physical table {} ({}) has duplicate column name {}",
                    physical_table_info.name, physical_table_info.ident.table_id, column.name,
                ),
            }
        );
        ensure!(
            seen_ids.insert(*column_id),
            MetadataCorruptionSnafu {
                err_msg: format!(
                    "Physical table {} ({}) has duplicate column ID {}",
                    physical_table_info.name, physical_table_info.ident.table_id, column_id,
                ),
            }
        );
    }

    logical_columns
        .iter()
        .map(|column| {
            ids_by_name
                .get(column.name.as_str())
                .copied()
                .ok_or_else(|| {
                    MetadataCorruptionSnafu {
                        err_msg: format!(
                            "Logical column {} is missing from physical table {} ({})",
                            column.name,
                            physical_table_info.name,
                            physical_table_info.ident.table_id,
                        ),
                    }
                    .build()
                })
        })
        .collect()
}

/// Restores incomplete logical column IDs from the authoritative physical table.
///
/// A complete persisted mapping is immutable: it must already match the physical mapping.
/// This prevents a corrupt complete mapping from being silently overwritten.
pub(crate) fn populate_logical_table_column_ids(
    physical_table_info: &TableInfo,
    logical_table_info: &mut TableInfo,
) -> Result<()> {
    let column_ids = logical_column_ids_from_physical_table_info(
        physical_table_info,
        logical_table_info.meta.schema.column_schemas(),
    )?;
    let existing_column_ids = &logical_table_info.meta.column_ids;

    if existing_column_ids.len() != column_ids.len() {
        logical_table_info.meta.column_ids = column_ids;
        return Ok(());
    }
    ensure!(
        *existing_column_ids == column_ids,
        MetadataCorruptionSnafu {
            err_msg: format!(
                "Logical table {} ({}) has a complete column ID mapping that conflicts with physical table {} ({})",
                logical_table_info.name,
                logical_table_info.ident.table_id,
                physical_table_info.name,
                physical_table_info.ident.table_id,
            ),
        }
    );

    Ok(())
}

/// Validates that every logical region column uses its physical table's stable ID.
pub(crate) fn validate_logical_column_metadata_ids(
    physical_table_info: &TableInfo,
    column_metadatas: &[ColumnMetadata],
) -> Result<()> {
    let logical_columns = column_metadatas
        .iter()
        .map(|column_metadata| column_metadata.column_schema.clone())
        .collect::<Vec<_>>();
    let expected_column_ids =
        logical_column_ids_from_physical_table_info(physical_table_info, &logical_columns)?;

    for (column_metadata, expected_column_id) in column_metadatas.iter().zip(expected_column_ids) {
        ensure!(
            column_metadata.column_id == expected_column_id,
            MetadataCorruptionSnafu {
                err_msg: format!(
                    "Logical region column {} has ID {}, but physical table {} ({}) has ID {}",
                    column_metadata.column_schema.name,
                    column_metadata.column_id,
                    physical_table_info.name,
                    physical_table_info.ident.table_id,
                    expected_column_id,
                ),
            }
        );
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datatypes::prelude::ConcreteDataType;
    use datatypes::schema::Schema;

    use super::*;
    use crate::ddl::test_util::{
        test_create_logical_table_task, test_create_physical_table_task, test_physical_table_info,
    };

    fn table_infos() -> (TableInfo, TableInfo) {
        let physical =
            test_physical_table_info(test_create_physical_table_task("physical").table_info);
        let logical = test_create_logical_table_task("logical").table_info;
        (physical, logical)
    }

    #[test]
    fn test_populate_logical_column_ids_replaces_incomplete_mapping() {
        let (physical, mut logical) = table_infos();
        let expected = logical_column_ids_from_physical_table_info(
            &physical,
            logical.meta.schema.column_schemas(),
        )
        .unwrap();
        logical.meta.column_ids = vec![999];

        populate_logical_table_column_ids(&physical, &mut logical).unwrap();

        assert_eq!(logical.meta.column_ids, expected);
    }

    #[test]
    fn test_populate_logical_column_ids_replaces_overlong_mapping() {
        let (physical, mut logical) = table_infos();
        let expected = logical_column_ids_from_physical_table_info(
            &physical,
            logical.meta.schema.column_schemas(),
        )
        .unwrap();
        logical.meta.column_ids = expected.iter().copied().chain([999]).collect();

        populate_logical_table_column_ids(&physical, &mut logical).unwrap();

        assert_eq!(logical.meta.column_ids, expected);
    }

    #[test]
    fn test_populate_logical_column_ids_keeps_complete_matching_mapping() {
        let (physical, mut logical) = table_infos();
        logical.meta.column_ids = logical_column_ids_from_physical_table_info(
            &physical,
            logical.meta.schema.column_schemas(),
        )
        .unwrap();
        let expected = logical.clone();

        populate_logical_table_column_ids(&physical, &mut logical).unwrap();

        assert_eq!(logical, expected);
    }

    #[test]
    fn test_populate_logical_column_ids_rejects_complete_conflict() {
        let (physical, mut logical) = table_infos();
        logical.meta.column_ids = logical_column_ids_from_physical_table_info(
            &physical,
            logical.meta.schema.column_schemas(),
        )
        .unwrap();
        logical.meta.column_ids[0] += 100;

        assert!(populate_logical_table_column_ids(&physical, &mut logical).is_err());
    }

    #[test]
    fn test_logical_column_ids_reject_incomplete_physical_ids() {
        let (mut physical, logical) = table_infos();
        physical.meta.column_ids.pop();

        assert!(
            logical_column_ids_from_physical_table_info(
                &physical,
                logical.meta.schema.column_schemas(),
            )
            .is_err()
        );
    }

    #[test]
    fn test_logical_column_ids_reject_duplicate_physical_id() {
        let (mut physical, logical) = table_infos();
        physical.meta.column_ids[1] = physical.meta.column_ids[0];
        assert!(
            logical_column_ids_from_physical_table_info(
                &physical,
                logical.meta.schema.column_schemas(),
            )
            .is_err()
        );
    }

    #[test]
    fn test_logical_column_ids_reject_missing_logical_column() {
        let (physical, mut logical) = table_infos();
        let mut columns = logical.meta.schema.column_schemas().to_vec();
        columns.push(ColumnSchema::new(
            "missing",
            ConcreteDataType::int64_datatype(),
            true,
        ));
        logical.meta.schema = Arc::new(Schema::new(columns));

        assert!(populate_logical_table_column_ids(&physical, &mut logical).is_err());
    }
}
