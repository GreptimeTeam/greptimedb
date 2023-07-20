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

//! Common structs and utilities for reading data.

use datatypes::vectors::VectorRef;

/// Storage internal representation of a batch of rows.
///
/// Now the structure of [Batch] is still unstable, all pub fields may be changed.
#[derive(Debug, Default, PartialEq, Eq, Clone)]
pub struct Batch {
    /// Rows organized in columnar format.
    ///
    /// Columns follow the same order convention of region schema:
    /// key, value, internal columns.
    pub columns: Vec<VectorRef>,
}

impl Batch {
    /// Create a new `Batch` from `columns`.
    ///
    /// # Panics
    /// Panics if vectors in `columns` have different length.
    pub fn new(columns: Vec<VectorRef>) -> Batch {
        Self::assert_columns(&columns);

        Batch { columns }
    }

    #[inline]
    pub fn num_columns(&self) -> usize {
        self.columns.len()
    }

    #[inline]
    pub fn num_rows(&self) -> usize {
        self.columns.get(0).map(|v| v.len()).unwrap_or(0)
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.num_rows() == 0
    }

    #[inline]
    pub fn columns(&self) -> &[VectorRef] {
        &self.columns
    }

    #[inline]
    pub fn column(&self, idx: usize) -> &VectorRef {
        &self.columns[idx]
    }

    /// Slice the batch, returning a new batch.
    ///
    /// # Panics
    /// Panics if `offset + length > self.num_rows()`.
    pub fn slice(&self, offset: usize, length: usize) -> Batch {
        let columns = self
            .columns
            .iter()
            .map(|v| v.slice(offset, length))
            .collect();
        Batch { columns }
    }

    fn assert_columns(columns: &[VectorRef]) {
        if columns.is_empty() {
            return;
        }

        let length = columns[0].len();
        assert!(columns.iter().all(|col| col.len() == length));
    }
}
