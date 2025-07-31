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
use std::fmt;

use api::v1::SemanticType;
use common_telemetry::warn;
use datatypes::schema::ColumnSchema;
use snafu::{ensure, OptionExt};
use store_api::metadata::{ColumnMetadata, RegionMetadata};
use store_api::storage::{RegionId, TableId};
use table::metadata::{RawTableInfo, RawTableMeta};
use table::table_name::TableName;
use table::table_reference::TableReference;

use crate::cache_invalidator::CacheInvalidatorRef;
use crate::error::{
    self, MismatchColumnIdSnafu, MissingColumnInColumnMetadataSnafu, Result, UnexpectedSnafu,
};
use crate::key::table_name::{TableNameKey, TableNameManager};
use crate::key::TableMetadataManagerRef;
use crate::node_manager::NodeManagerRef;

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct PartialRegionMetadata<'a> {
    pub(crate) column_metadatas: &'a [ColumnMetadata],
    pub(crate) primary_key: &'a [u32],
    pub(crate) table_id: TableId,
}

impl<'a> From<&'a RegionMetadata> for PartialRegionMetadata<'a> {
    fn from(region_metadata: &'a RegionMetadata) -> Self {
        Self {
            column_metadatas: &region_metadata.column_metadatas,
            primary_key: &region_metadata.primary_key,
            table_id: region_metadata.region_id.table_id(),
        }
    }
}

/// A display wrapper for [`ColumnMetadata`] that formats the column metadata in a more readable way.
struct ColumnMetadataDisplay<'a>(pub &'a ColumnMetadata);

impl<'a> fmt::Debug for ColumnMetadataDisplay<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let col = self.0;
        write!(
            f,
            "Column {{ name: {}, id: {}, semantic_type: {:?}, data_type: {:?} }}",
            col.column_schema.name, col.column_id, col.semantic_type, col.column_schema.data_type,
        )
    }
}

/// Checks if the column metadatas are consistent.
///
/// The column metadatas are consistent if:
/// - The column metadatas are the same.
/// - The primary key are the same.
/// - The table id of the region metadatas are the same.
///
/// ## Panic
/// Panic if region_metadatas is empty.
pub(crate) fn check_column_metadatas_consistent(
    region_metadatas: &[RegionMetadata],
) -> Option<Vec<ColumnMetadata>> {
    let is_column_metadata_consistent = region_metadatas
        .windows(2)
        .all(|w| PartialRegionMetadata::from(&w[0]) == PartialRegionMetadata::from(&w[1]));

    if !is_column_metadata_consistent {
        return None;
    }

    Some(region_metadatas[0].column_metadatas.clone())
}

/// Resolves column metadata inconsistencies among the given region metadatas
/// by using the column metadata from the metasrv as the source of truth.
///
/// All region metadatas whose column metadata differs from the given `column_metadatas`
/// will be marked for reconciliation.
///
/// Returns the region ids that need to be reconciled.
pub(crate) fn resolve_column_metadatas_with_metasrv(
    column_metadatas: &[ColumnMetadata],
    region_metadatas: &[RegionMetadata],
) -> Result<Vec<RegionId>> {
    let is_same_table = region_metadatas
        .windows(2)
        .all(|w| w[0].region_id.table_id() == w[1].region_id.table_id());

    ensure!(
        is_same_table,
        UnexpectedSnafu {
            err_msg: "Region metadatas are not from the same table"
        }
    );

    let mut regions_ids = vec![];
    for region_metadata in region_metadatas {
        if region_metadata.column_metadatas != column_metadatas {
            let is_invariant_preserved = check_column_metadata_invariants(
                column_metadatas,
                &region_metadata.column_metadatas,
            );
            ensure!(
                is_invariant_preserved,
                UnexpectedSnafu {
                    err_msg: format!(
                        "Column metadata invariants violated for region {}. Resolved column metadata: {:?}, region column metadata: {:?}",
                        region_metadata.region_id,
                        column_metadatas.iter().map(ColumnMetadataDisplay).collect::<Vec<_>>(),
                        region_metadata.column_metadatas.iter().map(ColumnMetadataDisplay).collect::<Vec<_>>(),
                    )
                }
            );
            regions_ids.push(region_metadata.region_id);
        }
    }
    Ok(regions_ids)
}

/// Resolves column metadata inconsistencies among the given region metadatas
/// by selecting the column metadata with the highest schema version.
///
/// This strategy assumes that at most two versions of column metadata may exist,
/// due to the poison mechanism, making the highest schema version a safe choice.
///
/// Returns the resolved column metadata and the region ids that need to be reconciled.
pub(crate) fn resolve_column_metadatas_with_latest(
    region_metadatas: &[RegionMetadata],
) -> Result<(Vec<ColumnMetadata>, Vec<RegionId>)> {
    let is_same_table = region_metadatas
        .windows(2)
        .all(|w| w[0].region_id.table_id() == w[1].region_id.table_id());

    ensure!(
        is_same_table,
        UnexpectedSnafu {
            err_msg: "Region metadatas are not from the same table"
        }
    );

    let latest_region_metadata = region_metadatas
        .iter()
        .max_by_key(|c| c.schema_version)
        .context(UnexpectedSnafu {
            err_msg: "All Region metadatas have the same schema version",
        })?;
    let latest_column_metadatas = PartialRegionMetadata::from(latest_region_metadata);

    let mut region_ids = vec![];
    for region_metadata in region_metadatas {
        if PartialRegionMetadata::from(region_metadata) != latest_column_metadatas {
            let is_invariant_preserved = check_column_metadata_invariants(
                &latest_region_metadata.column_metadatas,
                &region_metadata.column_metadatas,
            );
            ensure!(
                is_invariant_preserved,
                UnexpectedSnafu {
                    err_msg: format!(
                        "Column metadata invariants violated for region {}. Resolved column metadata: {:?}, region column metadata: {:?}",
                        region_metadata.region_id,
                        latest_column_metadatas.column_metadatas.iter().map(ColumnMetadataDisplay).collect::<Vec<_>>(),
                        region_metadata.column_metadatas.iter().map(ColumnMetadataDisplay).collect::<Vec<_>>()
                    )
                }
            );
            region_ids.push(region_metadata.region_id);
        }
    }

    // TODO(weny): verify the new column metadatas are acceptable for regions.
    Ok((latest_region_metadata.column_metadatas.clone(), region_ids))
}

/// Constructs a vector of [`ColumnMetadata`] from the provided table information.
///
/// This function maps each [`ColumnSchema`] to its corresponding [`ColumnMetadata`] by
/// determining the semantic type (Tag, Timestamp, or Field) and retrieving the column ID
/// from the `name_to_ids` mapping.
///
/// Returns an error if any column name is missing in the mapping.
pub(crate) fn build_column_metadata_from_table_info(
    column_schemas: &[ColumnSchema],
    primary_key_indexes: &[usize],
    name_to_ids: &HashMap<String, u32>,
) -> Result<Vec<ColumnMetadata>> {
    let primary_names = primary_key_indexes
        .iter()
        .map(|i| column_schemas[*i].name.as_str())
        .collect::<HashSet<_>>();

    column_schemas
        .iter()
        .map(|column_schema| {
            let column_id = *name_to_ids
                .get(column_schema.name.as_str())
                .with_context(|| UnexpectedSnafu {
                    err_msg: format!(
                        "Column name {} not found in name_to_ids",
                        column_schema.name
                    ),
                })?;

            let semantic_type = if primary_names.contains(&column_schema.name.as_str()) {
                SemanticType::Tag
            } else if column_schema.is_time_index() {
                SemanticType::Timestamp
            } else {
                SemanticType::Field
            };
            Ok(ColumnMetadata {
                column_schema: column_schema.clone(),
                semantic_type,
                column_id,
            })
        })
        .collect::<Result<Vec<_>>>()
}

/// Checks whether the schema invariants hold between the existing and new column metadata.
///
/// Invariants:
/// - Primary key (Tag) columns must exist in the new metadata, with identical name and ID.
/// - Timestamp column must remain exactly the same in name and ID.
pub(crate) fn check_column_metadata_invariants(
    new_column_metadatas: &[ColumnMetadata],
    column_metadatas: &[ColumnMetadata],
) -> bool {
    let new_primary_keys = new_column_metadatas
        .iter()
        .filter(|c| c.semantic_type == SemanticType::Tag)
        .map(|c| (c.column_schema.name.as_str(), c.column_id))
        .collect::<HashMap<_, _>>();

    let old_primary_keys = column_metadatas
        .iter()
        .filter(|c| c.semantic_type == SemanticType::Tag)
        .map(|c| (c.column_schema.name.as_str(), c.column_id));

    for (name, id) in old_primary_keys {
        if new_primary_keys.get(name) != Some(&id) {
            return false;
        }
    }

    let new_ts_column = new_column_metadatas
        .iter()
        .find(|c| c.semantic_type == SemanticType::Timestamp)
        .map(|c| (c.column_schema.name.as_str(), c.column_id));

    let old_ts_column = column_metadatas
        .iter()
        .find(|c| c.semantic_type == SemanticType::Timestamp)
        .map(|c| (c.column_schema.name.as_str(), c.column_id));

    new_ts_column == old_ts_column
}

/// Builds a [`RawTableMeta`] from the provided [`ColumnMetadata`]s.
///
/// Returns an error if:
/// - Any column is missing in the `name_to_ids`(if `name_to_ids` is provided).
/// - The column id in table metadata is not the same as the column id in the column metadata.(if `name_to_ids` is provided)
/// - The table index is missing in the column metadata.
/// - The primary key or partition key columns are missing in the column metadata.
///
/// TODO(weny): add tests
pub(crate) fn build_table_meta_from_column_metadatas(
    table_id: TableId,
    table_ref: TableReference,
    table_meta: &RawTableMeta,
    name_to_ids: Option<HashMap<String, u32>>,
    column_metadata: &[ColumnMetadata],
) -> Result<RawTableMeta> {
    let column_in_column_metadata = column_metadata
        .iter()
        .map(|c| (c.column_schema.name.as_str(), c))
        .collect::<HashMap<_, _>>();
    let primary_key_names = table_meta
        .primary_key_indices
        .iter()
        .map(|i| table_meta.schema.column_schemas[*i].name.as_str())
        .collect::<HashSet<_>>();
    let partition_key_names = table_meta
        .partition_key_indices
        .iter()
        .map(|i| table_meta.schema.column_schemas[*i].name.as_str())
        .collect::<HashSet<_>>();
    ensure!(
        column_metadata
            .iter()
            .any(|c| c.semantic_type == SemanticType::Timestamp),
        UnexpectedSnafu {
            err_msg: format!(
                "Missing table index in column metadata, table: {}, table_id: {}",
                table_ref, table_id
            ),
        }
    );

    if let Some(name_to_ids) = &name_to_ids {
        // Ensures all primary key and partition key exists in the column metadata.
        for column_name in primary_key_names.iter().chain(partition_key_names.iter()) {
            let column_in_column_metadata = column_in_column_metadata
                .get(column_name)
                .with_context(|| MissingColumnInColumnMetadataSnafu {
                    column_name: column_name.to_string(),
                    table_name: table_ref.to_string(),
                    table_id,
                })?;

            let column_id = *name_to_ids
                .get(*column_name)
                .with_context(|| UnexpectedSnafu {
                    err_msg: format!("column id not found in name_to_ids: {}", column_name),
                })?;
            ensure!(
                column_id == column_in_column_metadata.column_id,
                MismatchColumnIdSnafu {
                    column_name: column_name.to_string(),
                    column_id,
                    table_name: table_ref.to_string(),
                    table_id,
                }
            );
        }
    } else {
        warn!(
            "`name_to_ids` is not provided, table: {}, table_id: {}",
            table_ref, table_id
        );
    }

    let mut new_raw_table_meta = table_meta.clone();
    let primary_key_indices = &mut new_raw_table_meta.primary_key_indices;
    let partition_key_indices = &mut new_raw_table_meta.partition_key_indices;
    let value_indices = &mut new_raw_table_meta.value_indices;
    let time_index = &mut new_raw_table_meta.schema.timestamp_index;
    let columns = &mut new_raw_table_meta.schema.column_schemas;
    let column_ids = &mut new_raw_table_meta.column_ids;
    let next_column_id = &mut new_raw_table_meta.next_column_id;

    column_ids.clear();
    value_indices.clear();
    columns.clear();
    primary_key_indices.clear();
    partition_key_indices.clear();

    for (idx, col) in column_metadata.iter().enumerate() {
        if partition_key_names.contains(&col.column_schema.name.as_str()) {
            partition_key_indices.push(idx);
        }
        match col.semantic_type {
            SemanticType::Tag => {
                primary_key_indices.push(idx);
            }
            SemanticType::Field => {
                value_indices.push(idx);
            }
            SemanticType::Timestamp => {
                value_indices.push(idx);
                *time_index = Some(idx);
            }
        }

        columns.push(col.column_schema.clone());
        column_ids.push(col.column_id);
    }

    *next_column_id = column_ids
        .iter()
        .max()
        .map(|max| max + 1)
        .unwrap_or(*next_column_id)
        .max(*next_column_id);

    if let Some(time_index) = *time_index {
        new_raw_table_meta.schema.column_schemas[time_index].set_time_index();
    }

    Ok(new_raw_table_meta)
}

/// Validates the table id and name consistency.
///
/// It will check the table id and table name consistency.
/// If the table id and table name are not consistent, it will return an error.
pub(crate) async fn validate_table_id_and_name(
    table_name_manager: &TableNameManager,
    table_id: TableId,
    table_name: &TableName,
) -> Result<()> {
    let table_name_key = TableNameKey::new(
        &table_name.catalog_name,
        &table_name.schema_name,
        &table_name.table_name,
    );
    let table_name_value = table_name_manager
        .get(table_name_key)
        .await?
        .with_context(|| error::TableNotFoundSnafu {
            table_name: table_name.to_string(),
        })?;

    ensure!(
        table_name_value.table_id() == table_id,
        error::UnexpectedSnafu {
            err_msg: format!(
                "The table id mismatch for table: {}, expected {}, actual {}",
                table_name,
                table_id,
                table_name_value.table_id()
            ),
        }
    );

    Ok(())
}

/// Checks whether the column metadata invariants hold for the logical table.
///
/// Invariants:
/// - Primary key (Tag) columns must exist in the new metadata.
/// - Timestamp column must remain exactly the same in name and ID.
///
/// TODO(weny): add tests
pub(crate) fn check_column_metadatas_invariants_for_logical_table(
    column_metadatas: &[ColumnMetadata],
    table_info: &RawTableInfo,
) -> bool {
    let new_primary_keys = column_metadatas
        .iter()
        .filter(|c| c.semantic_type == SemanticType::Tag)
        .map(|c| c.column_schema.name.as_str())
        .collect::<HashSet<_>>();

    let old_primary_keys = table_info
        .meta
        .primary_key_indices
        .iter()
        .map(|i| table_info.meta.schema.column_schemas[*i].name.as_str());

    for name in old_primary_keys {
        if !new_primary_keys.contains(name) {
            return false;
        }
    }

    let old_timestamp_column_name = table_info
        .meta
        .schema
        .column_schemas
        .iter()
        .find(|c| c.is_time_index())
        .map(|c| c.name.as_str());

    let new_timestamp_column_name = column_metadatas
        .iter()
        .find(|c| c.semantic_type == SemanticType::Timestamp)
        .map(|c| c.column_schema.name.as_str());

    old_timestamp_column_name != new_timestamp_column_name
}

/// Returns true if the logical table info needs to be updated.
///
/// The logical table only support to add columns, so we can check the length of column metadatas
/// to determine whether the logical table info needs to be updated.
pub(crate) fn need_update_logical_table_info(
    table_info: &RawTableInfo,
    column_metadatas: &[ColumnMetadata],
) -> bool {
    table_info.meta.schema.column_schemas.len() != column_metadatas.len()
}

#[derive(Clone)]
pub struct Context {
    pub node_manager: NodeManagerRef,
    pub table_metadata_manager: TableMetadataManagerRef,
    pub cache_invalidator: CacheInvalidatorRef,
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;
    use std::collections::HashMap;
    use std::sync::Arc;

    use api::v1::SemanticType;
    use datatypes::prelude::ConcreteDataType;
    use datatypes::schema::{ColumnSchema, Schema, SchemaBuilder};
    use store_api::metadata::ColumnMetadata;
    use store_api::storage::RegionId;
    use table::metadata::{RawTableMeta, TableMetaBuilder};
    use table::table_reference::TableReference;

    use super::*;
    use crate::ddl::test_util::region_metadata::build_region_metadata;
    use crate::error::Error;
    use crate::reconciliation::utils::check_column_metadatas_consistent;

    fn new_test_schema() -> Schema {
        let column_schemas = vec![
            ColumnSchema::new("col1", ConcreteDataType::int32_datatype(), true),
            ColumnSchema::new(
                "ts",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            )
            .with_time_index(true),
            ColumnSchema::new("col2", ConcreteDataType::int32_datatype(), true),
        ];
        SchemaBuilder::try_from(column_schemas)
            .unwrap()
            .version(123)
            .build()
            .unwrap()
    }

    fn new_test_column_metadatas() -> Vec<ColumnMetadata> {
        vec![
            ColumnMetadata {
                column_schema: ColumnSchema::new("col1", ConcreteDataType::int32_datatype(), true),
                semantic_type: SemanticType::Tag,
                column_id: 0,
            },
            ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "ts",
                    ConcreteDataType::timestamp_millisecond_datatype(),
                    false,
                )
                .with_time_index(true),
                semantic_type: SemanticType::Timestamp,
                column_id: 1,
            },
            ColumnMetadata {
                column_schema: ColumnSchema::new("col2", ConcreteDataType::int32_datatype(), true),
                semantic_type: SemanticType::Field,
                column_id: 2,
            },
        ]
    }

    fn new_test_raw_table_info() -> RawTableMeta {
        let mut table_meta_builder = TableMetaBuilder::empty();
        let table_meta = table_meta_builder
            .schema(Arc::new(new_test_schema()))
            .primary_key_indices(vec![0])
            .partition_key_indices(vec![2])
            .next_column_id(4)
            .build()
            .unwrap();

        table_meta.into()
    }

    #[test]
    fn test_build_table_info_from_column_metadatas_identical() {
        let column_metadatas = new_test_column_metadatas();
        let table_id = 1;
        let table_ref = TableReference::full("test_catalog", "test_schema", "test_table");
        let mut table_meta = new_test_raw_table_info();
        table_meta.column_ids = vec![0, 1, 2];
        let name_to_ids = HashMap::from([
            ("col1".to_string(), 0),
            ("ts".to_string(), 1),
            ("col2".to_string(), 2),
        ]);

        let new_table_meta = build_table_meta_from_column_metadatas(
            table_id,
            table_ref,
            &table_meta,
            Some(name_to_ids),
            &column_metadatas,
        )
        .unwrap();
        assert_eq!(new_table_meta, table_meta);
    }

    #[test]
    fn test_build_table_info_from_column_metadatas() {
        let mut column_metadatas = new_test_column_metadatas();
        column_metadatas.push(ColumnMetadata {
            column_schema: ColumnSchema::new("col3", ConcreteDataType::string_datatype(), true),
            semantic_type: SemanticType::Tag,
            column_id: 3,
        });

        let table_id = 1;
        let table_ref = TableReference::full("test_catalog", "test_schema", "test_table");
        let table_meta = new_test_raw_table_info();
        let name_to_ids = HashMap::from([
            ("col1".to_string(), 0),
            ("ts".to_string(), 1),
            ("col2".to_string(), 2),
        ]);

        let new_table_meta = build_table_meta_from_column_metadatas(
            table_id,
            table_ref,
            &table_meta,
            Some(name_to_ids),
            &column_metadatas,
        )
        .unwrap();

        assert_eq!(new_table_meta.primary_key_indices, vec![0, 3]);
        assert_eq!(new_table_meta.partition_key_indices, vec![2]);
        assert_eq!(new_table_meta.value_indices, vec![1, 2]);
        assert_eq!(new_table_meta.schema.timestamp_index, Some(1));
        assert_eq!(new_table_meta.column_ids, vec![0, 1, 2, 3]);
        assert_eq!(new_table_meta.next_column_id, 4);
    }

    #[test]
    fn test_build_table_info_from_column_metadatas_with_incorrect_name_to_ids() {
        let column_metadatas = new_test_column_metadatas();
        let table_id = 1;
        let table_ref = TableReference::full("test_catalog", "test_schema", "test_table");
        let table_meta = new_test_raw_table_info();
        let name_to_ids = HashMap::from([
            ("col1".to_string(), 0),
            ("ts".to_string(), 1),
            // Change column id of col2 to 3.
            ("col2".to_string(), 3),
        ]);

        let err = build_table_meta_from_column_metadatas(
            table_id,
            table_ref,
            &table_meta,
            Some(name_to_ids),
            &column_metadatas,
        )
        .unwrap_err();

        assert_matches!(err, Error::MismatchColumnId { .. });
    }

    #[test]
    fn test_build_table_info_from_column_metadatas_with_missing_time_index() {
        let mut column_metadatas = new_test_column_metadatas();
        column_metadatas.retain(|c| c.semantic_type != SemanticType::Timestamp);
        let table_id = 1;
        let table_ref = TableReference::full("test_catalog", "test_schema", "test_table");
        let table_meta = new_test_raw_table_info();
        let name_to_ids = HashMap::from([
            ("col1".to_string(), 0),
            ("ts".to_string(), 1),
            ("col2".to_string(), 2),
        ]);

        let err = build_table_meta_from_column_metadatas(
            table_id,
            table_ref,
            &table_meta,
            Some(name_to_ids),
            &column_metadatas,
        )
        .unwrap_err();

        assert!(
            err.to_string()
                .contains("Missing table index in column metadata"),
            "err: {}",
            err
        );
    }

    #[test]
    fn test_build_table_info_from_column_metadatas_with_missing_column() {
        let mut column_metadatas = new_test_column_metadatas();
        // Remove primary key column.
        column_metadatas.retain(|c| c.column_id != 0);
        let table_id = 1;
        let table_ref = TableReference::full("test_catalog", "test_schema", "test_table");
        let table_meta = new_test_raw_table_info();
        let name_to_ids = HashMap::from([
            ("col1".to_string(), 0),
            ("ts".to_string(), 1),
            ("col2".to_string(), 2),
        ]);

        let err = build_table_meta_from_column_metadatas(
            table_id,
            table_ref,
            &table_meta,
            Some(name_to_ids.clone()),
            &column_metadatas,
        )
        .unwrap_err();
        assert_matches!(err, Error::MissingColumnInColumnMetadata { .. });

        let mut column_metadatas = new_test_column_metadatas();
        // Remove partition key column.
        column_metadatas.retain(|c| c.column_id != 2);

        let err = build_table_meta_from_column_metadatas(
            table_id,
            table_ref,
            &table_meta,
            Some(name_to_ids),
            &column_metadatas,
        )
        .unwrap_err();
        assert_matches!(err, Error::MissingColumnInColumnMetadata { .. });
    }

    #[test]
    fn test_check_column_metadatas_consistent() {
        let column_metadatas = new_test_column_metadatas();
        let region_metadata1 = build_region_metadata(RegionId::new(1024, 0), &column_metadatas);
        let region_metadata2 = build_region_metadata(RegionId::new(1024, 1), &column_metadatas);
        let result =
            check_column_metadatas_consistent(&[region_metadata1, region_metadata2]).unwrap();
        assert_eq!(result, column_metadatas);

        let region_metadata1 = build_region_metadata(RegionId::new(1025, 0), &column_metadatas);
        let region_metadata2 = build_region_metadata(RegionId::new(1024, 1), &column_metadatas);
        let result = check_column_metadatas_consistent(&[region_metadata1, region_metadata2]);
        assert!(result.is_none());
    }

    #[test]
    fn test_check_column_metadata_invariants() {
        let column_metadatas = new_test_column_metadatas();
        let mut new_column_metadatas = column_metadatas.clone();
        new_column_metadatas.push(ColumnMetadata {
            column_schema: ColumnSchema::new("col3", ConcreteDataType::int32_datatype(), true),
            semantic_type: SemanticType::Field,
            column_id: 3,
        });
        assert!(check_column_metadata_invariants(
            &new_column_metadatas,
            &column_metadatas
        ));
    }

    #[test]
    fn test_check_column_metadata_invariants_missing_primary_key_column_or_ts_column() {
        let column_metadatas = new_test_column_metadatas();
        let mut new_column_metadatas = column_metadatas.clone();
        new_column_metadatas.retain(|c| c.semantic_type != SemanticType::Timestamp);
        assert!(!check_column_metadata_invariants(
            &new_column_metadatas,
            &column_metadatas
        ));

        let column_metadatas = new_test_column_metadatas();
        let mut new_column_metadatas = column_metadatas.clone();
        new_column_metadatas.retain(|c| c.semantic_type != SemanticType::Tag);
        assert!(!check_column_metadata_invariants(
            &new_column_metadatas,
            &column_metadatas
        ));
    }

    #[test]
    fn test_check_column_metadata_invariants_mismatch_column_id() {
        let column_metadatas = new_test_column_metadatas();
        let mut new_column_metadatas = column_metadatas.clone();
        if let Some(col) = new_column_metadatas
            .iter_mut()
            .find(|c| c.semantic_type == SemanticType::Timestamp)
        {
            col.column_id = 100;
        }
        assert!(!check_column_metadata_invariants(
            &new_column_metadatas,
            &column_metadatas
        ));

        let column_metadatas = new_test_column_metadatas();
        let mut new_column_metadatas = column_metadatas.clone();
        if let Some(col) = new_column_metadatas
            .iter_mut()
            .find(|c| c.semantic_type == SemanticType::Tag)
        {
            col.column_id = 100;
        }
        assert!(!check_column_metadata_invariants(
            &new_column_metadatas,
            &column_metadatas
        ));
    }

    #[test]
    fn test_resolve_column_metadatas_with_use_metasrv_strategy() {
        let column_metadatas = new_test_column_metadatas();
        let region_metadata1 = build_region_metadata(RegionId::new(1024, 0), &column_metadatas);
        let mut metasrv_column_metadatas = region_metadata1.column_metadatas.clone();
        metasrv_column_metadatas.push(ColumnMetadata {
            column_schema: ColumnSchema::new("col3", ConcreteDataType::int32_datatype(), true),
            semantic_type: SemanticType::Field,
            column_id: 3,
        });
        let result =
            resolve_column_metadatas_with_metasrv(&metasrv_column_metadatas, &[region_metadata1])
                .unwrap();

        assert_eq!(result, vec![RegionId::new(1024, 0)]);
    }

    #[test]
    fn test_resolve_column_metadatas_with_use_latest_strategy() {
        let column_metadatas = new_test_column_metadatas();
        let region_metadata1 = build_region_metadata(RegionId::new(1024, 0), &column_metadatas);
        let mut new_column_metadatas = column_metadatas.clone();
        new_column_metadatas.push(ColumnMetadata {
            column_schema: ColumnSchema::new("col3", ConcreteDataType::int32_datatype(), true),
            semantic_type: SemanticType::Field,
            column_id: 3,
        });

        let mut region_metadata2 =
            build_region_metadata(RegionId::new(1024, 1), &new_column_metadatas);
        region_metadata2.schema_version = 2;

        let (resolved_column_metadatas, region_ids) =
            resolve_column_metadatas_with_latest(&[region_metadata1, region_metadata2]).unwrap();
        assert_eq!(region_ids, vec![RegionId::new(1024, 0)]);
        assert_eq!(resolved_column_metadatas, new_column_metadatas);
    }
}
