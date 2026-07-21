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
use parquet::basic::{ConvertedType, Type as PhysicalType};
use parquet::schema::types::{ColumnDescriptor, SchemaDescriptor};

/// A nested field access path inside one parquet root column.
pub type ParquetNestedPath = Vec<String>;

/// The parquet columns to read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParquetReadColumns {
    /// Root parquet column indices in the same order as `cols`.
    ///
    /// Most readers need these indices as a borrowed slice for Arrow schema
    /// projection or parquet root-column projection. Keeping them here avoids
    /// repeatedly collecting `cols.iter().map(|col| col.root_index)`.
    root_indices: Vec<usize>,
    cols: Vec<ParquetReadColumn>,
    has_nested: bool,
}

impl ParquetReadColumns {
    /// Builds parquet read columns from deduplicated, normalized input.
    ///
    /// `cols` must not contain duplicate root indices, and nested paths must
    /// already be merged. Empty `nested_paths` means reading the whole root column.
    ///
    /// This constructor does not validate or merge input.
    pub fn from_deduped(cols: Vec<ParquetReadColumn>) -> Self {
        let has_nested = cols.iter().any(|col| !col.nested_paths.is_empty());
        let root_indices = cols.iter().map(|col| col.root_index).collect();
        Self {
            root_indices,
            cols,
            has_nested,
        }
    }

    /// Builds root-column projections from root indices that are already
    /// deduplicated.
    ///
    /// Note: this constructor does not check for duplicates.
    pub fn from_deduped_root_indices(root_indices: impl IntoIterator<Item = usize>) -> Self {
        let root_indices = root_indices.into_iter().collect::<Vec<_>>();
        let cols = root_indices
            .iter()
            .copied()
            .map(ParquetReadColumn::new)
            .collect();
        Self {
            root_indices,
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
        self.root_indices.iter().copied()
    }

    /// Returns root parquet column indices.
    pub fn root_indices(&self) -> &[usize] {
        &self.root_indices
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

    /// Merges additional nested paths into this root column.
    pub fn merge_nested_paths(&mut self, nested_paths: Vec<ParquetNestedPath>) {
        let reads_whole_root = self.nested_paths.is_empty() || nested_paths.is_empty();
        if reads_whole_root {
            // Empty nested paths means reading the whole root column.
            self.nested_paths = vec![];
        } else {
            self.nested_paths.extend(nested_paths);
        }
    }

    pub fn root_index(&self) -> usize {
        self.root_index
    }

    pub fn nested_paths(&self) -> &[ParquetNestedPath] {
        &self.nested_paths
    }
}

/// Projection plan built for a parquet file.
#[derive(Clone)]
pub struct ProjectionMaskPlan {
    /// `mask` is the projection mask applied to the parquet reader.
    pub mask: ProjectionMask,
    /// A boolean mask in output schema order indicating whether each projected root
    /// has data read from the current parquet file.
    ///
    /// - `true`: parquet reads either requested nested data or a fallback parent
    ///   for this root.
    /// - `false`: this root is absent and must be synthesized later.
    ///
    /// The length of `projected_root_presence` is always equal to the
    /// number of fields in the output schema.
    pub projected_root_presence: Vec<bool>,
}

/// Builds a projection mask plan for reading a parquet file.
///
/// `parquet_read_cols` defines the requested root columns and optional
/// nested paths to read.
///
/// `parquet_schema_desc` is the schema descriptor of the current parquet
/// file. It is used to resolve requested nested paths to actual leaf
/// column indices.
///
/// See [`ProjectionMaskPlan`] for the returned value.
///
/// For example, if the query requests `j.a` and `k`, but the current
/// parquet file only contains leaves under `j.b` and `k`, then the
/// returned plan keeps `k` in the projection mask and marks `j` as
/// not present in the output, so it can be synthesized during
/// post-processing.
pub fn build_projection_plan(
    parquet_read_cols: &ParquetReadColumns,
    parquet_schema_desc: &SchemaDescriptor,
) -> ProjectionMaskPlan {
    if !parquet_read_cols.has_nested() {
        let mask =
            ProjectionMask::roots(parquet_schema_desc, parquet_read_cols.root_indices_iter());
        return ProjectionMaskPlan {
            mask,
            projected_root_presence: vec![true; parquet_read_cols.columns().len()],
        };
    }

    let (matched_leaves, matched_roots) =
        build_parquet_leaves_indices(parquet_schema_desc, parquet_read_cols);

    let projected_root_presence = parquet_read_cols
        .columns()
        .iter()
        .map(|col| matched_roots.contains(&col.root_index()))
        .collect();

    let mask = ProjectionMask::leaves(parquet_schema_desc, matched_leaves);
    ProjectionMaskPlan {
        mask,
        projected_root_presence,
    }
}

/// Builds parquet leaf-column indices for reading a parquet file.
///
/// Returns `(matched_leaves, matched_roots)`:
/// - `matched_leaves`: matched leaf-column indices in the current parquet file schema.
/// - `matched_roots`: root-field indices read from the current parquet file schema.
fn build_parquet_leaves_indices(
    parquet_schema_desc: &SchemaDescriptor,
    projection: &ParquetReadColumns,
) -> (Vec<usize>, HashSet<usize>) {
    let mut map = HashMap::with_capacity(projection.cols.len());
    for col in &projection.cols {
        map.insert(col.root_index, col);
    }

    let mut matched_leaves = HashSet::new();
    let mut matched_roots = HashSet::with_capacity(projection.cols.len());

    // Tracks whether each requested nested path matched by prefix without fallback.
    let mut prefix_matched = HashMap::<usize, Vec<bool>>::new();
    for col in &projection.cols {
        prefix_matched.insert(col.root_index, vec![false; col.nested_paths.len()]);
    }

    // First select root/nested prefix matches.
    for (leaf_idx, leaf_col) in parquet_schema_desc.columns().iter().enumerate() {
        let root_idx = parquet_schema_desc.get_column_root_idx(leaf_idx);
        let Some(col) = map.get(&root_idx) else {
            continue;
        };
        if col.nested_paths.is_empty() {
            matched_leaves.insert(leaf_idx);
            matched_roots.insert(root_idx);
            continue;
        }

        let leaf_path = leaf_col.path().parts();
        let mut matched = false;
        for (path_idx, _) in col
            .nested_paths
            .iter()
            .enumerate()
            .filter(|(_, nested_path)| leaf_path.starts_with(nested_path))
        {
            prefix_matched.get_mut(&root_idx).unwrap()[path_idx] = true;
            matched = true;
        }

        if matched {
            matched_leaves.insert(leaf_idx);
            matched_roots.insert(root_idx);
        }
    }

    // Then fallback prefix misses to their nearest variant parent.
    // TODO(fys): Gate fallback planning on the root being JSON2. A raw Binary
    // leaf is a JSONB variant only under a JSON2 root; plain struct Binary
    // children should not enter this fallback path.
    for col in &projection.cols {
        for (path_idx, nested_path) in col.nested_paths.iter().enumerate() {
            if prefix_matched[&col.root_index][path_idx] {
                continue;
            }

            let Some(leaf_idx) =
                find_nearest_variant_parent(parquet_schema_desc, col.root_index, nested_path)
            else {
                continue;
            };

            matched_leaves.insert(leaf_idx);
            matched_roots.insert(col.root_index);
        }
    }

    let mut matched_leaves = matched_leaves.into_iter().collect::<Vec<_>>();
    matched_leaves.sort_unstable();
    (matched_leaves, matched_roots)
}

fn find_nearest_variant_parent(
    parquet_schema_desc: &SchemaDescriptor,
    root_idx: usize,
    nested_path: &[String],
) -> Option<usize> {
    // TODO(fys): Build a variant path index if fallback lookup becomes hot.
    if nested_path.len() <= 1 {
        return None;
    }

    // JSON2 root columns are always structured fields. Fallback only applies to
    // variant leaves below the root.
    for parent_len in (2..nested_path.len()).rev() {
        let parent_path = &nested_path[..parent_len];
        for (leaf_idx, leaf_col) in parquet_schema_desc.columns().iter().enumerate() {
            if parquet_schema_desc.get_column_root_idx(leaf_idx) != root_idx {
                continue;
            }
            if leaf_col.path().parts() == parent_path && is_variant_leaf(leaf_col) {
                return Some(leaf_idx);
            }
        }
    }

    None
}

fn is_variant_leaf(leaf_col: &ColumnDescriptor) -> bool {
    // TODO(fys): Recognize JSON2 variant type from Arrow extension metadata
    // if the parquet Arrow schema preserves it for nested fields.
    matches!(
        leaf_col.physical_type(),
        PhysicalType::BYTE_ARRAY | PhysicalType::FIXED_LEN_BYTE_ARRAY
    ) && leaf_col.logical_type_ref().is_none()
        && leaf_col.converted_type() == ConvertedType::NONE
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use parquet::basic::{ConvertedType, LogicalType, Repetition};
    use parquet::schema::types::Type;

    use super::*;

    #[test]
    fn test_build_projection_mask_without_nested_paths() {
        let parquet_schema_desc = build_test_nested_parquet_schema();
        let projection = ParquetReadColumns::from_deduped_root_indices([0, 1]);

        let plan = build_projection_plan(&projection, &parquet_schema_desc);

        assert_eq!(vec![true, true], plan.projected_root_presence);
        assert_eq!(
            ProjectionMask::roots(&parquet_schema_desc, [0, 1]),
            plan.mask
        );
    }

    #[test]
    fn test_reads_whole_root() {
        let parquet_schema_desc = build_test_nested_parquet_schema();

        let projection = ParquetReadColumns::from_deduped(vec![ParquetReadColumn::new(0)]);

        let (matched_leaves, matched_roots) =
            build_parquet_leaves_indices(&parquet_schema_desc, &projection);
        assert_eq!(vec![0, 1, 2], matched_leaves);
        assert_eq!(HashSet::from([0]), matched_roots);
    }

    #[test]
    fn test_filters_nested_paths() {
        let parquet_schema_desc = build_test_nested_parquet_schema();

        let projection = ParquetReadColumns::from_deduped(vec![
            ParquetReadColumn::new(0)
                .with_nested_paths(vec![vec!["j".to_string(), "b".to_string()]]),
            ParquetReadColumn::new(1),
        ]);

        let (matched_leaves, matched_roots) =
            build_parquet_leaves_indices(&parquet_schema_desc, &projection);
        assert_eq!(vec![1, 2, 3], matched_leaves);
        assert_eq!(HashSet::from([0, 1]), matched_roots);
    }

    #[test]
    fn test_reads_middle_level_path() {
        let parquet_schema_desc = build_test_nested_parquet_schema();

        let projection = ParquetReadColumns::from_deduped(vec![
            ParquetReadColumn::new(0)
                .with_nested_paths(vec![vec!["j".to_string(), "b".to_string()]]),
        ]);

        let (matched_leaves, matched_roots) =
            build_parquet_leaves_indices(&parquet_schema_desc, &projection);
        assert_eq!(vec![1, 2], matched_leaves);
        assert_eq!(HashSet::from([0]), matched_roots);
    }

    #[test]
    fn test_parent_path_covers_redundant_child_path() {
        let parquet_schema_desc = build_test_nested_parquet_schema();
        let nested_paths = vec![
            vec!["j".to_string(), "b".to_string()],
            vec!["j".to_string(), "b".to_string(), "c".to_string()],
        ];

        let read_column = ParquetReadColumn::new(0).with_nested_paths(nested_paths);
        let projection = ParquetReadColumns::from_deduped(vec![read_column]);

        let (matched_leaves, matched_roots) =
            build_parquet_leaves_indices(&parquet_schema_desc, &projection);
        assert_eq!(vec![1, 2], matched_leaves);
        assert_eq!(HashSet::from([0]), matched_roots);
    }

    #[test]
    fn test_reads_leaf_level_path() {
        let parquet_schema_desc = build_test_nested_parquet_schema();

        let projection =
            ParquetReadColumns::from_deduped(vec![ParquetReadColumn::new(0).with_nested_paths(
                vec![vec!["j".to_string(), "b".to_string(), "c".to_string()]],
            )]);

        let (matched_leaves, matched_roots) =
            build_parquet_leaves_indices(&parquet_schema_desc, &projection);
        assert_eq!(vec![1], matched_leaves);
        assert_eq!(HashSet::from([0]), matched_roots);
    }

    #[test]
    fn test_build_projection_mask_with_unmatched_roots() {
        let parquet_schema_desc = build_test_nested_parquet_schema();

        let projection = ParquetReadColumns::from_deduped(vec![
            ParquetReadColumn::new(0)
                .with_nested_paths(vec![vec!["j".to_string(), "missing".to_string()]]),
            ParquetReadColumn::new(1),
        ]);

        let plan = build_projection_plan(&projection, &parquet_schema_desc);

        assert_eq!(vec![false, true], plan.projected_root_presence);
        assert_eq!(
            ProjectionMask::leaves(&parquet_schema_desc, vec![3]),
            plan.mask
        );
    }

    #[test]
    fn test_merges_mixed_paths() {
        let parquet_schema_desc = build_test_nested_parquet_schema();

        let projection =
            ParquetReadColumns::from_deduped(vec![ParquetReadColumn::new(0).with_nested_paths(
                vec![
                    vec!["j".to_string(), "a".to_string()],
                    vec!["j".to_string(), "b".to_string(), "d".to_string()],
                ],
            )]);

        let (matched_leaves, matched_roots) =
            build_parquet_leaves_indices(&parquet_schema_desc, &projection);
        assert_eq!(vec![0, 2], matched_leaves);
        assert_eq!(HashSet::from([0]), matched_roots);
    }

    #[test]
    fn test_merge_nested_paths_extends_paths() {
        let mut col = ParquetReadColumn::new(0)
            .with_nested_paths(vec![vec!["j".to_string(), "a".to_string()]]);

        col.merge_nested_paths(vec![vec!["j".to_string(), "b".to_string()]]);

        assert_eq!(
            &[
                vec!["j".to_string(), "a".to_string()],
                vec!["j".to_string(), "b".to_string()],
            ],
            col.nested_paths()
        );
    }

    #[test]
    fn test_merge_nested_paths_with_whole_root() {
        let mut col = ParquetReadColumn::new(0)
            .with_nested_paths(vec![vec!["j".to_string(), "a".to_string()]]);

        col.merge_nested_paths(vec![]);

        assert!(col.nested_paths().is_empty());
    }

    #[test]
    fn test_fallback_to_nearest_variant_parent() {
        let parquet_schema_desc = build_test_variant_parent_schema();
        let projection =
            ParquetReadColumns::from_deduped(vec![ParquetReadColumn::new(0).with_nested_paths(
                vec![vec!["j".to_string(), "a".to_string(), "x".to_string()]],
            )]);

        let plan = build_projection_plan(&projection, &parquet_schema_desc);

        assert_eq!(vec![true], plan.projected_root_presence);
        assert_eq!(
            ProjectionMask::leaves(&parquet_schema_desc, vec![0]),
            plan.mask
        );
    }

    #[test]
    fn test_prefix_match_prevents_variant_parent_fallback() {
        let parquet_schema_desc = build_test_variant_parent_schema();
        let projection =
            ParquetReadColumns::from_deduped(vec![ParquetReadColumn::new(0).with_nested_paths(
                vec![vec!["j".to_string(), "b".to_string(), "x".to_string()]],
            )]);

        let plan = build_projection_plan(&projection, &parquet_schema_desc);

        assert_eq!(vec![true], plan.projected_root_presence);
        assert_eq!(
            ProjectionMask::leaves(&parquet_schema_desc, vec![1]),
            plan.mask
        );
    }

    #[test]
    fn test_mixed_prefix_and_fallback_paths() {
        let parquet_schema_desc = build_test_variant_parent_schema();
        let nested_paths = vec![
            vec!["j".to_string(), "a".to_string(), "x".to_string()],
            vec!["j".to_string(), "b".to_string(), "x".to_string()],
        ];
        let projection = ParquetReadColumns::from_deduped(vec![
            ParquetReadColumn::new(0).with_nested_paths(nested_paths),
        ]);

        let plan = build_projection_plan(&projection, &parquet_schema_desc);

        assert_eq!(vec![true], plan.projected_root_presence);
        assert_eq!(
            ProjectionMask::leaves(&parquet_schema_desc, vec![0, 1]),
            plan.mask
        );
    }

    #[test]
    fn test_fallback_selects_multiple_variant_parents() {
        let parquet_schema_desc = build_test_two_variant_parents_schema();
        let nested_paths = vec![
            vec!["j".to_string(), "a".to_string(), "y".to_string()],
            vec!["j".to_string(), "b".to_string(), "d".to_string()],
            vec!["j".to_string(), "a".to_string(), "x".to_string()],
        ];
        let projection = ParquetReadColumns::from_deduped(vec![
            ParquetReadColumn::new(0).with_nested_paths(nested_paths),
        ]);

        let plan = build_projection_plan(&projection, &parquet_schema_desc);

        assert_eq!(vec![true], plan.projected_root_presence);
        assert_eq!(
            ProjectionMask::leaves(&parquet_schema_desc, vec![0, 1]),
            plan.mask
        );
    }

    #[test]
    fn test_nested_paths_fallback_to_variant_parent_by_default() {
        let parquet_schema_desc = build_test_variant_parent_schema();
        let projection =
            ParquetReadColumns::from_deduped(vec![ParquetReadColumn::new(0).with_nested_paths(
                vec![vec!["j".to_string(), "a".to_string(), "x".to_string()]],
            )]);

        let plan = build_projection_plan(&projection, &parquet_schema_desc);

        assert_eq!(vec![true], plan.projected_root_presence);
        assert_eq!(
            ProjectionMask::leaves(&parquet_schema_desc, vec![0]),
            plan.mask
        );
    }

    #[test]
    fn test_non_variant_parent_does_not_fallback() {
        let parquet_schema_desc = build_test_nested_parquet_schema();
        let projection =
            ParquetReadColumns::from_deduped(vec![ParquetReadColumn::new(0).with_nested_paths(
                vec![vec!["j".to_string(), "a".to_string(), "x".to_string()]],
            )]);

        let plan = build_projection_plan(&projection, &parquet_schema_desc);

        assert_eq!(vec![false], plan.projected_root_presence);
    }

    #[test]
    fn test_utf8_parent_does_not_fallback() {
        let parquet_schema_desc = build_test_utf8_parent_schema();
        let projection =
            ParquetReadColumns::from_deduped(vec![ParquetReadColumn::new(0).with_nested_paths(
                vec![vec!["j".to_string(), "a".to_string(), "x".to_string()]],
            )]);

        let plan = build_projection_plan(&projection, &parquet_schema_desc);

        assert_eq!(vec![false], plan.projected_root_presence);
    }

    #[test]
    fn test_root_variant_does_not_fallback() {
        let parquet_schema_desc = build_test_root_variant_schema();
        let projection = ParquetReadColumns::from_deduped(vec![
            ParquetReadColumn::new(0)
                .with_nested_paths(vec![vec!["j".to_string(), "a".to_string()]]),
        ]);

        let plan = build_projection_plan(&projection, &parquet_schema_desc);

        assert_eq!(vec![false], plan.projected_root_presence);
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

    // Test schema:
    // schema
    // `- j
    //    |- a: BYTE_ARRAY
    //    `- b
    //       `- x: INT64
    fn build_test_variant_parent_schema() -> SchemaDescriptor {
        let leaf_a = Arc::new(
            Type::primitive_type_builder("a", parquet::basic::Type::BYTE_ARRAY)
                .with_repetition(Repetition::REQUIRED)
                .build()
                .unwrap(),
        );
        let leaf_x = Arc::new(
            Type::primitive_type_builder("x", parquet::basic::Type::INT64)
                .with_repetition(Repetition::REQUIRED)
                .build()
                .unwrap(),
        );
        let group_b = Arc::new(
            Type::group_type_builder("b")
                .with_repetition(Repetition::REQUIRED)
                .with_fields(vec![leaf_x])
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
        let schema = Arc::new(
            Type::group_type_builder("schema")
                .with_fields(vec![root_j])
                .build()
                .unwrap(),
        );

        SchemaDescriptor::new(schema)
    }

    // Test schema:
    // schema
    // `- j
    //    |- a: BYTE_ARRAY
    //    `- b: BYTE_ARRAY
    fn build_test_two_variant_parents_schema() -> SchemaDescriptor {
        let leaf_a = Arc::new(
            Type::primitive_type_builder("a", parquet::basic::Type::BYTE_ARRAY)
                .with_repetition(Repetition::REQUIRED)
                .build()
                .unwrap(),
        );
        let leaf_b = Arc::new(
            Type::primitive_type_builder("b", parquet::basic::Type::BYTE_ARRAY)
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

        SchemaDescriptor::new(schema)
    }

    fn build_test_utf8_parent_schema() -> SchemaDescriptor {
        let leaf_a = Arc::new(
            Type::primitive_type_builder("a", parquet::basic::Type::BYTE_ARRAY)
                .with_repetition(Repetition::REQUIRED)
                .with_logical_type(Some(LogicalType::String))
                .with_converted_type(ConvertedType::UTF8)
                .build()
                .unwrap(),
        );
        let root_j = Arc::new(
            Type::group_type_builder("j")
                .with_repetition(Repetition::REQUIRED)
                .with_fields(vec![leaf_a])
                .build()
                .unwrap(),
        );
        let schema = Arc::new(
            Type::group_type_builder("schema")
                .with_fields(vec![root_j])
                .build()
                .unwrap(),
        );

        SchemaDescriptor::new(schema)
    }

    // Test schema:
    // schema
    // `- j: BYTE_ARRAY
    fn build_test_root_variant_schema() -> SchemaDescriptor {
        let root_j = Arc::new(
            Type::primitive_type_builder("j", parquet::basic::Type::BYTE_ARRAY)
                .with_repetition(Repetition::REQUIRED)
                .build()
                .unwrap(),
        );
        let schema = Arc::new(
            Type::group_type_builder("schema")
                .with_fields(vec![root_j])
                .build()
                .unwrap(),
        );

        SchemaDescriptor::new(schema)
    }
}
