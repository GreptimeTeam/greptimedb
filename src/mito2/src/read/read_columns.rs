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

// TODO(fys): remove it until this module is used
#![allow(dead_code)]

use std::collections::{BTreeMap, HashSet};
use std::mem;

use datafusion_common::HashMap;
use datafusion_expr::utils::expr_to_columns;
use snafu::OptionExt;
use store_api::metadata::RegionMetadataRef;
use store_api::storage::{ColumnId, NestedPath, ProjectionInput};

use crate::error::{InvalidRequestSnafu, Result};
use crate::read::scan_region::PredicateGroup;

/// Logical columns to read from a region.
///
/// Read columns describe which logical columns and nested fields should be read
/// from storage. Each read column is identified by its [`ColumnId`],
/// which represents the root column in the storage schema.
///
/// Nested fields under the column are specified by [`NestedPath`] entries.
/// Each path includes the root column name as its first element.
///
/// For example, assume column id `9` corresponds to a root column named `j`
/// with nested fields:
///
/// ```text
/// j
/// ├── a
/// └── b
///     └── c
/// ```
///
/// The following SQL:
///
/// SELECT j.a, j.b.c FROM t
///
/// may produce read columns like:
///
/// ```text
/// ReadColumn {
///     column_id: 9,
///     nested_paths: [
///         ["j", "a"],
///         ["j", "b", "c"],
///     ]
/// }
/// ```
///
/// If `nested_paths` is empty, the whole column will be read.
#[derive(Debug, Default, Clone, PartialEq, Eq, Hash)]
pub struct ReadColumns {
    cols: Vec<ReadColumn>,
}

impl ReadColumns {
    pub fn from_deduped_column_ids<I>(column_ids: I) -> Self
    where
        I: IntoIterator<Item = ColumnId>,
    {
        let cols = column_ids
            .into_iter()
            .map(|col_id| ReadColumn::new(col_id, vec![]))
            .collect();
        ReadColumns { cols }
    }

    pub fn is_empty(&self) -> bool {
        self.cols.is_empty()
    }

    pub fn column_ids_iter(&self) -> impl Iterator<Item = ColumnId> + '_ {
        self.cols.iter().map(|column| column.column_id())
    }

    pub fn column_ids(&self) -> Vec<ColumnId> {
        self.column_ids_iter().collect()
    }

    pub fn columns(&self) -> &[ReadColumn] {
        &self.cols
    }

    pub fn estimated_size(&self) -> usize {
        self.cols.capacity() * mem::size_of::<ReadColumn>()
            + self
                .cols
                .iter()
                .map(ReadColumn::estimated_size)
                .sum::<usize>()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ReadColumn {
    column_id: ColumnId,
    /// Nested field paths under this column.
    /// Empty means reading the whole column.
    nested_paths: Vec<NestedPath>,
}

impl ReadColumn {
    pub fn new(column_id: ColumnId, nested_paths: Vec<NestedPath>) -> Self {
        Self {
            column_id,
            nested_paths,
        }
    }

    pub fn column_id(&self) -> ColumnId {
        self.column_id
    }

    pub fn nested_paths(&self) -> &[NestedPath] {
        &self.nested_paths
    }

    pub fn estimated_size(&self) -> usize {
        self.nested_paths.capacity() * mem::size_of::<NestedPath>()
            + self
                .nested_paths
                .iter()
                .map(|path| {
                    path.capacity() * mem::size_of::<String>()
                        + path.iter().map(|node| node.capacity()).sum::<usize>()
                })
                .sum::<usize>()
    }
}

pub fn merge(a: ReadColumns, b: ReadColumns) -> ReadColumns {
    let mut merged = BTreeMap::<ColumnId, Vec<NestedPath>>::new();

    for col in a.cols.into_iter().chain(b.cols) {
        if let Some(nested_paths) = merged.get_mut(&col.column_id) {
            if nested_paths.is_empty() || col.nested_paths.is_empty() {
                *nested_paths = vec![];
            } else {
                merge_nested_paths(nested_paths, col.nested_paths);
            }
            continue;
        }

        merged.insert(col.column_id, normalize_nested_paths(col.nested_paths));
    }

    ReadColumns {
        cols: merged
            .into_iter()
            .map(|(column_id, nested_paths)| ReadColumn {
                column_id,
                nested_paths,
            })
            .collect(),
    }
}

fn normalize_nested_paths(nested_paths: Vec<NestedPath>) -> Vec<NestedPath> {
    let mut normalized = Vec::with_capacity(nested_paths.len());
    merge_nested_paths(&mut normalized, nested_paths);
    normalized
}

fn merge_nested_paths(merged: &mut Vec<NestedPath>, incoming: Vec<NestedPath>) {
    for path in incoming {
        if merged
            .iter()
            .any(|existing| path.starts_with(existing.as_slice()))
        {
            continue;
        }

        merged.retain(|existing| !existing.starts_with(path.as_slice()));
        merged.push(path);
    }
}

/// Build [`ReadColumns`] from [`ProjectionInput`].
///
/// Note: If `projection.projection` is empty, this function still reads the
/// time index column so the scan can preserve row counts for empty-output
/// queries such as `SELECT COUNT(*)`.
pub fn read_columns_from_projection(
    projection: ProjectionInput,
    metadata: &RegionMetadataRef,
) -> Result<ReadColumns> {
    let root_indices = if projection.projection.is_empty() {
        vec![metadata.time_index_column_pos()]
    } else {
        projection.projection
    };

    let mut paths_by_col: HashMap<String, Vec<NestedPath>> =
        HashMap::with_capacity(projection.nested_paths.len());
    for path in projection.nested_paths {
        let Some((root_name, _)) = path.split_first() else {
            continue;
        };
        paths_by_col
            .entry(root_name.clone())
            .or_default()
            .push(path);
    }

    let mut read_cols = Vec::with_capacity(root_indices.len());
    let mut seen = HashSet::with_capacity(root_indices.len());
    for root_idx in root_indices {
        if !seen.insert(root_idx) {
            continue;
        }

        let col = metadata
            .column_metadatas
            .get(root_idx)
            .with_context(|| InvalidRequestSnafu {
                region_id: metadata.region_id,
                reason: format!("projection index {} is out of bound", root_idx),
            })?;
        let col_id = col.column_id;

        let nested_paths = paths_by_col
            .remove(&col.column_schema.name)
            .unwrap_or_default();

        read_cols.push(ReadColumn {
            column_id: col_id,
            nested_paths,
        });
    }

    Ok(ReadColumns { cols: read_cols })
}

/// Build [`ReadColumns`] from [`PredicateGroup`].
pub fn read_columns_from_predicate(
    predicate: &PredicateGroup,
    metadata: &RegionMetadataRef,
) -> ReadColumns {
    let mut root_names = HashSet::new();
    let mut columns = HashSet::new();

    if let Some(p) = predicate.predicate_without_region() {
        for expr in p.exprs() {
            columns.clear();
            if expr_to_columns(expr, &mut columns).is_err() {
                continue;
            }
            root_names.extend(columns.drain().map(|column| column.name));
        }
    }

    if let Some(expr) = predicate.region_partition_expr() {
        expr.collect_column_names(&mut root_names);
    }

    // TODO(fys): Parse nested paths from predicate expressions and attach them
    // to read columns instead of always reading the whole root column.
    let mut cols = Vec::with_capacity(root_names.len());
    for name in root_names {
        if let Some(column) = metadata.column_by_name(&name) {
            cols.push(ReadColumn::new(column.column_id, vec![]));
        }
    }

    ReadColumns { cols }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use api::v1::SemanticType;
    use datafusion_expr::{col, lit};
    use datatypes::prelude::ConcreteDataType;
    use datatypes::schema::ColumnSchema;
    use store_api::metadata::{ColumnMetadata, RegionMetadataBuilder};
    use store_api::storage::RegionId;

    use super::*;

    #[test]
    fn test_read_columns_from_empty_projection() {
        let metadata = new_test_metadata();

        let read_columns =
            read_columns_from_projection(ProjectionInput::default(), &metadata).unwrap();

        let expected = ReadColumns {
            cols: vec![ReadColumn::new(2, vec![])],
        };
        assert_eq!(expected, read_columns);

        let projection_input =
            ProjectionInput::new(vec![]).with_nested_paths(vec![vec!["1".to_string()]]);
        let read_columns = read_columns_from_projection(projection_input, &metadata).unwrap();

        let expected = ReadColumns {
            cols: vec![ReadColumn::new(2, vec![])],
        };
        assert_eq!(expected, read_columns);
    }

    #[test]
    fn test_read_columns_from_projection_with_nested_paths() {
        let metadata = new_test_metadata();
        let projection = ProjectionInput::new(vec![1, 0]).with_nested_paths(vec![
            nested_path(&["field_0", "a"]),
            nested_path(&["field_0", "b", "c"]),
        ]);

        let read_columns = read_columns_from_projection(projection, &metadata).unwrap();

        let expected = ReadColumns {
            cols: vec![
                ReadColumn::new(
                    3,
                    vec![
                        nested_path(&["field_0", "a"]),
                        nested_path(&["field_0", "b", "c"]),
                    ],
                ),
                ReadColumn::new(0, vec![]),
            ],
        };
        assert_eq!(expected, read_columns,);
    }

    #[test]
    fn test_read_columns_from_projection_dedups_duplicate_indices() {
        let metadata = new_test_metadata();
        let projection = ProjectionInput::new(vec![1, 1, 0]).with_nested_paths(vec![
            nested_path(&["field_0", "a"]),
            nested_path(&["field_0", "b", "c"]),
        ]);

        let read_columns = read_columns_from_projection(projection, &metadata).unwrap();

        let expected = ReadColumns {
            cols: vec![
                ReadColumn::new(
                    3,
                    vec![
                        nested_path(&["field_0", "a"]),
                        nested_path(&["field_0", "b", "c"]),
                    ],
                ),
                ReadColumn::new(0, vec![]),
            ],
        };
        assert_eq!(expected, read_columns);
    }

    #[test]
    fn test_read_columns_from_projection_out_of_bound() {
        let metadata = new_test_metadata();
        let projection = ProjectionInput::new(vec![3]);

        let err = read_columns_from_projection(projection, &metadata).unwrap_err();

        assert!(
            err.to_string()
                .contains("projection index 3 is out of bound")
        );
    }

    #[test]
    fn test_read_columns_from_predicate_reads_root_columns_only() {
        let metadata = new_test_metadata();
        let predicate = PredicateGroup::new(
            metadata.as_ref(),
            &[col("field_0").gt(lit(1)), col("tag_0").eq(lit("a"))],
        )
        .unwrap();

        let read_columns = read_columns_from_predicate(&predicate, &metadata);

        let mut actual_ids = read_columns.column_ids();
        actual_ids.sort_unstable();
        assert_eq!(vec![0, 3], actual_ids);
        assert!(
            read_columns
                .columns()
                .iter()
                .all(|col| col.nested_paths().is_empty())
        );
    }

    #[test]
    fn test_read_columns_from_predicate_empty() {
        let metadata = new_test_metadata();
        let predicate = PredicateGroup::new(metadata.as_ref(), &[]).unwrap();

        let read_columns = read_columns_from_predicate(&predicate, &metadata);

        assert!(read_columns.is_empty());
    }

    #[test]
    fn test_merge_read_cols_with_only_root() {
        let a = ReadColumns {
            cols: vec![ReadColumn::new(3, vec![]), ReadColumn::new(1, vec![])],
        };
        let b = ReadColumns {
            cols: vec![ReadColumn::new(2, vec![])],
        };

        let merged = merge(a, b);

        assert_eq!(
            merged,
            ReadColumns {
                cols: vec![
                    ReadColumn::new(1, vec![]),
                    ReadColumn::new(2, vec![]),
                    ReadColumn::new(3, vec![]),
                ],
            }
        );
    }

    #[test]
    fn test_merge_read_cols_with_nested_paths() {
        let a = ReadColumns {
            cols: vec![ReadColumn::new(1, vec![nested_path(&["j", "a"])])],
        };
        let b = ReadColumns {
            cols: vec![ReadColumn::new(
                1,
                vec![nested_path(&["j", "b"]), nested_path(&["j", "c"])],
            )],
        };

        let merged = merge(a, b);

        assert_eq!(
            merged,
            ReadColumns {
                cols: vec![ReadColumn::new(
                    1,
                    vec![
                        nested_path(&["j", "a"]),
                        nested_path(&["j", "b"]),
                        nested_path(&["j", "c"]),
                    ],
                )],
            }
        );
    }

    #[test]
    fn test_merge_read_cols_with_column_override() {
        let a = ReadColumns {
            cols: vec![
                ReadColumn::new(1, vec![nested_path(&["j", "a"])]),
                ReadColumn::new(2, vec![nested_path(&["k", "b"])]),
            ],
        };
        let b = ReadColumns {
            cols: vec![
                ReadColumn::new(1, vec![]),
                ReadColumn::new(2, vec![nested_path(&["k", "b", "c"])]),
            ],
        };

        let merged = merge(a, b);

        assert_eq!(
            merged,
            ReadColumns {
                cols: vec![
                    ReadColumn::new(1, vec![]),
                    ReadColumn::new(2, vec![nested_path(&["k", "b"])])
                ],
            }
        );
    }

    #[test]
    fn test_merge_read_cols_dedups_redundant_nested_paths() {
        let a = ReadColumns {
            cols: vec![ReadColumn::new(
                1,
                vec![
                    nested_path(&["j", "a", "b"]),
                    nested_path(&["j", "a"]),
                    nested_path(&["j", "a", "b", "c"]),
                ],
            )],
        };
        let b = ReadColumns {
            cols: vec![ReadColumn::new(1, vec![nested_path(&["j", "a"])])],
        };

        let merged = merge(a, b);

        assert_eq!(
            merged,
            ReadColumns {
                cols: vec![ReadColumn::new(1, vec![nested_path(&["j", "a"])])],
            }
        );
    }

    fn new_test_metadata() -> RegionMetadataRef {
        let mut builder = RegionMetadataBuilder::new(RegionId::new(1, 1));
        builder
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "tag_0".to_string(),
                    ConcreteDataType::string_datatype(),
                    true,
                ),
                semantic_type: SemanticType::Tag,
                column_id: 0,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "field_0".to_string(),
                    ConcreteDataType::string_datatype(),
                    true,
                ),
                semantic_type: SemanticType::Field,
                column_id: 3,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "ts".to_string(),
                    ConcreteDataType::timestamp_millisecond_datatype(),
                    false,
                ),
                semantic_type: SemanticType::Timestamp,
                column_id: 2,
            });
        builder.primary_key(vec![0]);
        Arc::new(builder.build().unwrap())
    }

    fn nested_path(parts: &[&str]) -> NestedPath {
        parts.iter().map(|part| (*part).to_string()).collect()
    }
}
