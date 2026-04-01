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

use std::collections::HashMap;

use parquet::schema::types::SchemaDescriptor;

/// A nested field access path inside one parquet root column.
pub type ParquetNestedPath = Vec<String>;

/// The parquet columns to read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParquetReadColumns {
    cols: Vec<ParquetReadColumn>,
}

impl Default for ParquetReadColumns {
    fn default() -> Self {
        Self::new()
    }
}

impl ParquetReadColumns {
    pub fn new() -> ParquetReadColumns {
        ParquetReadColumns { cols: vec![] }
    }

    /// # Safety
    ///
    /// The caller must ensure `read_col.root_index()` is unique within `self`.
    pub unsafe fn push_cols(&mut self, read_col: ParquetReadColumn) {
        self.cols.push(read_col);
    }

    pub fn from_root_indices(root_indices: impl IntoIterator<Item = usize>) -> Self {
        let cols = root_indices
            .into_iter()
            .map(ParquetReadColumn::new)
            .collect();
        Self { cols }
    }

    pub fn columns(&self) -> &[ParquetReadColumn] {
        &self.cols
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

/// Builds parquet leaf-column indices from parquet read columns.
pub fn build_parquet_leaves_indices(
    parquet_schema_desc: &SchemaDescriptor,
    projection: &ParquetReadColumns,
) -> Vec<usize> {
    let mut map = HashMap::new();
    for col in &projection.cols {
        map.insert(col.root_index, &col.nested_paths);
    }

    let mut leaf_indices = Vec::new();
    for (leaf_idx, leaf_col) in parquet_schema_desc.columns().iter().enumerate() {
        let root_idx = parquet_schema_desc.get_column_root_idx(leaf_idx);
        let Some(nested_paths) = map.get(&root_idx) else {
            continue;
        };
        if nested_paths.is_empty() {
            leaf_indices.push(leaf_idx);
            continue;
        }

        let leaf_path = leaf_col.path().parts();
        if nested_paths
            .iter()
            .any(|nested_path| leaf_path.starts_with(nested_path))
        {
            leaf_indices.push(leaf_idx);
        }
    }
    leaf_indices
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use parquet::basic::Repetition;
    use parquet::schema::types::Type;

    use super::*;

    #[test]
    fn test_build_parquet_leaves_indices_reads_whole_root() {
        let leaf_a = Arc::new(
            Type::primitive_type_builder("a", parquet::basic::Type::INT64)
                .with_repetition(Repetition::REQUIRED)
                .build()
                .unwrap(),
        );
        let leaf_b = Arc::new(
            Type::primitive_type_builder("b", parquet::basic::Type::INT64)
                .with_repetition(Repetition::REQUIRED)
                .build()
                .unwrap(),
        );
        let root_j = Arc::new(
            Type::group_type_builder("j")
                .with_repetition(Repetition::REQUIRED)
                .with_fields(vec![leaf_a, leaf_b])
                .build()
                .unwrap(),
        );
        let schema = Arc::new(
            Type::group_type_builder("schema")
                .with_fields(vec![root_j])
                .build()
                .unwrap(),
        );
        let parquet_schema_desc = SchemaDescriptor::new(schema);

        let projection = ParquetReadColumns {
            cols: vec![ParquetReadColumn {
                root_index: 0,
                nested_paths: vec![],
            }],
        };

        assert_eq!(
            vec![0, 1],
            build_parquet_leaves_indices(&parquet_schema_desc, &projection)
        );
    }

    #[test]
    fn test_build_parquet_leaves_indices_filters_nested_paths() {
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
        let parquet_schema_desc = SchemaDescriptor::new(schema);

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
        };

        assert_eq!(
            vec![1, 2, 3],
            build_parquet_leaves_indices(&parquet_schema_desc, &projection)
        );
    }
}
