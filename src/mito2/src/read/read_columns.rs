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

use std::mem;

use store_api::storage::{ColumnId, NestedPath};

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
    pub cols: Vec<ReadColumn>,
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
        self.cols.iter().map(|column| column.column_id)
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
    pub column_id: ColumnId,
    /// Nested field paths under this column.
    /// Empty means reading the whole column.
    pub nested_paths: Vec<NestedPath>,
}

impl ReadColumn {
    pub fn new(column_id: ColumnId, nested_paths: Vec<NestedPath>) -> Self {
        Self {
            column_id,
            nested_paths,
        }
    }

    pub fn nested_paths(&self) -> &[NestedPath] {
        &self.nested_paths
    }

    pub fn estimated_size(&self) -> usize {
        mem::size_of::<ColumnId>()
            + self.nested_paths.capacity() * mem::size_of::<NestedPath>()
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
