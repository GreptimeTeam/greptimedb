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

use parquet::arrow::ProjectionMask;
use parquet::schema::types::SchemaDescriptor;

/// A nested field access path inside one parquet root column.
pub type ParquetNestedPath = Vec<String>;

/// The parquet columns to read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParquetReadColumns {
    cols: Vec<ParquetReadColumn>,
    has_nested: bool,
}

impl ParquetReadColumns {
    /// Builds root-column projections from root indices that are already
    /// deduplicated.
    ///
    /// Note: this constructor does not check for duplicates.
    pub fn from_deduped_root_indices(root_indices: impl IntoIterator<Item = usize>) -> Self {
        let cols = root_indices
            .into_iter()
            .map(ParquetReadColumn::new)
            .collect();
        Self {
            cols,
            has_nested: false,
        }
    }

    pub fn columns(&self) -> &[ParquetReadColumn] {
        &self.cols
    }

    pub fn has_nested(&self) -> bool {
        self.has_nested
    }

    pub fn root_indices_iter(&self) -> impl Iterator<Item = usize> + '_ {
        self.cols.iter().map(|col| col.root_index)
    }
}

/// Read requirement for a single parquet root column.
///
/// `root_index` identifies the root column in the parquet schema.
///
/// If `nested_paths` is empty, the whole root column is read. Otherwise, only
/// leaves under the specified nested paths are read.
///
/// To construct a [`ParquetReadColumn`]:
/// - `ParquetReadColumn::new(0)` reads the whole root column at index `0`.
/// - `ParquetReadColumn::new(0).with_nested_paths(vec![vec!["j".into(), "b".into()]])`
///   reads only leaves under `j.b`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParquetReadColumn {
    /// Root field index in the parquet schema.
    root_index: usize,
    /// Nested paths to read under this root column.
    ///
    /// Each path includes the root column itself. For example, for a root
    /// column `j`, path `["j", "a", "b"]` refers to `j.a.b`.
    ///
    /// If empty, the whole root column is read.
    nested_paths: Vec<ParquetNestedPath>,
}

impl ParquetReadColumn {
    pub fn new(root_index: usize) -> Self {
        Self {
            root_index,
            nested_paths: vec![],
        }
    }

    pub fn with_nested_paths(self, nested_paths: Vec<ParquetNestedPath>) -> Self {
        Self {
            nested_paths,
            ..self
        }
    }

    pub fn root_index(&self) -> usize {
        self.root_index
    }

    pub fn nested_paths(&self) -> &[ParquetNestedPath] {
        &self.nested_paths
    }
}

#[derive(Clone)]
/// Projection plan built for a parquet file.
pub struct ProjectionMaskPlan {
    /// `mask` is the projection mask applied to the parquet reader.
    pub mask: ProjectionMask,
    pub projected_root_matches: Vec<bool>,
}

/// Builds a projection mask plan for reading a parquet file.
///
/// `parquet_read_cols` defines the requested root columns and optional nested
/// paths to read.
///
/// `parquet_schema_desc` is the schema descriptor of the current parquet file.
/// It is used to resolve requested nested paths to actual leaf column indices.
///
/// See [`ProjectionMaskPlan`] for the returned value.
///
/// For example, if the query requests `j.a` and `k`, but the current parquet
/// file only contains leaves under `j.b` and `k`, then the returned plan keeps
/// `k` in the projection mask and records `j` in `unmatched_roots`.
pub fn build_projection_mask(
    parquet_read_cols: &ParquetReadColumns,
    parquet_schema_desc: &SchemaDescriptor,
) -> ProjectionMaskPlan {
    if !parquet_read_cols.has_nested() {
        let mask =
            ProjectionMask::roots(parquet_schema_desc, parquet_read_cols.root_indices_iter());
        return ProjectionMaskPlan {
            mask,
            projected_root_matches: vec![true; parquet_read_cols.columns().len()],
        };
    }

    let (leaf_indices, projected_root_matches) =
        build_parquet_leaves_indices(parquet_schema_desc, parquet_read_cols);

    let mask = ProjectionMask::leaves(parquet_schema_desc, leaf_indices);
    ProjectionMaskPlan {
        mask,
        projected_root_matches,
    }
}

/// Builds parquet leaf-column indices for reading a parquet file.
///
/// Returns `(leaf_indices, projected_root_matches)`:
/// - `leaf_indices`: matched parquet leaf column indices
/// - `projected_root_matches`: whether each requested root column matches at
///   least one leaf in the current parquet schema.
fn build_parquet_leaves_indices(
    parquet_schema_desc: &SchemaDescriptor,
    projection: &ParquetReadColumns,
) -> (Vec<usize>, Vec<bool>) {
    let mut map = HashMap::with_capacity(projection.cols.len());
    for col in &projection.cols {
        map.insert(col.root_index, &col.nested_paths);
    }

    let mut leaf_indices = Vec::new();
    let mut matched_roots = HashSet::with_capacity(projection.cols.len());
    for (leaf_idx, leaf_col) in parquet_schema_desc.columns().iter().enumerate() {
        let root_idx = parquet_schema_desc.get_column_root_idx(leaf_idx);
        let Some(nested_paths) = map.get(&root_idx) else {
            continue;
        };
        if nested_paths.is_empty() {
            leaf_indices.push(leaf_idx);
            matched_roots.insert(root_idx);
            continue;
        }

        let leaf_path = leaf_col.path().parts();
        if nested_paths
            .iter()
            .any(|nested_path| leaf_path.starts_with(nested_path))
        {
            leaf_indices.push(leaf_idx);
            matched_roots.insert(root_idx);
        }
    }
    let projected_root_matches = projection
        .columns()
        .iter()
        .map(|col| matched_roots.contains(&col.root_index()))
        .collect();
    (leaf_indices, projected_root_matches)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use parquet::basic::Repetition;
    use parquet::schema::types::Type;

    use super::*;

    #[test]
    fn test_build_projection_mask_without_nested_paths() {
        let parquet_schema_desc = build_test_nested_parquet_schema();
        let projection = ParquetReadColumns::from_deduped_root_indices([0, 1]);

        let plan = build_projection_mask(&projection, &parquet_schema_desc);

        assert_eq!(vec![true, true], plan.projected_root_matches);
        assert_eq!(
            ProjectionMask::roots(&parquet_schema_desc, [0, 1]),
            plan.mask
        );
    }

    #[test]
    fn test_reads_whole_root() {
        let parquet_schema_desc = build_test_nested_parquet_schema();

        let projection = ParquetReadColumns {
            cols: vec![ParquetReadColumn {
                root_index: 0,
                nested_paths: vec![],
            }],
            has_nested: false,
        };

        let (leaf_indices, projected_root_matches) =
            build_parquet_leaves_indices(&parquet_schema_desc, &projection);
        assert_eq!(vec![0, 1, 2], leaf_indices);
        assert_eq!(vec![true], projected_root_matches);
    }

    #[test]
    fn test_filters_nested_paths() {
        let parquet_schema_desc = build_test_nested_parquet_schema();

        let projection = ParquetReadColumns {
            cols: vec![
                ParquetReadColumn {
                    root_index: 0,
                    nested_paths: vec![vec!["j".to_string(), "b".to_string()]],
                },
                ParquetReadColumn {
                    root_index: 1,
                    nested_paths: vec![],
                },
            ],
            has_nested: true,
        };

        let (leaf_indices, projected_root_matches) =
            build_parquet_leaves_indices(&parquet_schema_desc, &projection);
        assert_eq!(vec![1, 2, 3], leaf_indices);
        assert_eq!(vec![true, true], projected_root_matches);
    }

    #[test]
    fn test_reads_middle_level_path() {
        let parquet_schema_desc = build_test_nested_parquet_schema();

        let projection = ParquetReadColumns {
            cols: vec![ParquetReadColumn {
                root_index: 0,
                nested_paths: vec![vec!["j".to_string(), "b".to_string()]],
            }],
            has_nested: true,
        };

        let (leaf_indices, projected_root_matches) =
            build_parquet_leaves_indices(&parquet_schema_desc, &projection);
        assert_eq!(vec![1, 2], leaf_indices);
        assert_eq!(vec![true], projected_root_matches);
    }

    #[test]
    fn test_reads_leaf_level_path() {
        let parquet_schema_desc = build_test_nested_parquet_schema();

        let projection = ParquetReadColumns {
            cols: vec![ParquetReadColumn {
                root_index: 0,
                nested_paths: vec![vec!["j".to_string(), "b".to_string(), "c".to_string()]],
            }],
            has_nested: true,
        };

        let (leaf_indices, projected_root_matches) =
            build_parquet_leaves_indices(&parquet_schema_desc, &projection);
        assert_eq!(vec![1], leaf_indices);
        assert_eq!(vec![true], projected_root_matches);
    }

    #[test]
    fn test_build_projection_mask_with_unmatched_roots() {
        let parquet_schema_desc = build_test_nested_parquet_schema();

        let projection = ParquetReadColumns {
            cols: vec![
                ParquetReadColumn {
                    root_index: 0,
                    nested_paths: vec![vec!["j".to_string(), "missing".to_string()]],
                },
                ParquetReadColumn {
                    root_index: 1,
                    nested_paths: vec![],
                },
            ],
            has_nested: true,
        };

        let plan = build_projection_mask(&projection, &parquet_schema_desc);

        assert_eq!(vec![false, true], plan.projected_root_matches);
        assert_eq!(
            ProjectionMask::leaves(&parquet_schema_desc, vec![3]),
            plan.mask
        );
    }

    #[test]
    fn test_merges_mixed_paths() {
        let parquet_schema_desc = build_test_nested_parquet_schema();

        let projection = ParquetReadColumns {
            cols: vec![ParquetReadColumn {
                root_index: 0,
                nested_paths: vec![
                    vec!["j".to_string(), "a".to_string()],
                    vec!["j".to_string(), "b".to_string(), "d".to_string()],
                ],
            }],
            has_nested: true,
        };

        let (leaf_indices, projected_root_matches) =
            build_parquet_leaves_indices(&parquet_schema_desc, &projection);
        assert_eq!(vec![0, 2], leaf_indices);
        assert_eq!(vec![true], projected_root_matches);
    }

    // Test schema:
    // schema
    // |- j
    // |  |- a: INT64
    // |  `- b
    // |     |- c: INT64
    // |     `- d: INT64
    // `- k: INT64
    fn build_test_nested_parquet_schema() -> SchemaDescriptor {
        let leaf_a = Arc::new(
            Type::primitive_type_builder("a", parquet::basic::Type::INT64)
                .with_repetition(Repetition::REQUIRED)
                .build()
                .unwrap(),
        );
        let leaf_c = Arc::new(
            Type::primitive_type_builder("c", parquet::basic::Type::INT64)
                .with_repetition(Repetition::REQUIRED)
                .build()
                .unwrap(),
        );
        let leaf_d = Arc::new(
            Type::primitive_type_builder("d", parquet::basic::Type::INT64)
                .with_repetition(Repetition::REQUIRED)
                .build()
                .unwrap(),
        );
        let group_b = Arc::new(
            Type::group_type_builder("b")
                .with_repetition(Repetition::REQUIRED)
                .with_fields(vec![leaf_c, leaf_d])
                .build()
                .unwrap(),
        );
        let root_j = Arc::new(
            Type::group_type_builder("j")
                .with_repetition(Repetition::REQUIRED)
                .with_fields(vec![leaf_a, group_b])
                .build()
                .unwrap(),
        );
        let root_k = Arc::new(
            Type::primitive_type_builder("k", parquet::basic::Type::INT64)
                .with_repetition(Repetition::REQUIRED)
                .build()
                .unwrap(),
        );
        let schema = Arc::new(
            Type::group_type_builder("schema")
                .with_fields(vec![root_j, root_k])
                .build()
                .unwrap(),
        );

        SchemaDescriptor::new(schema)
    }
}
