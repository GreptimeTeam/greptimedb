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

//! Statistics of parquet SSTs.

use std::collections::HashSet;

use datafusion::physical_optimizer::pruning::PruningStatistics;
use datafusion_common::Column;
use datatypes::arrow::array::ArrayRef;
use parquet::file::metadata::RowGroupMetaData;
use store_api::storage::ColumnId;

use crate::sst::parquet::format::ReadFormat;

/// Statistics for pruning row groups.
pub(crate) struct RowGroupPruningStats<'a> {
    /// Metadata of SST row groups.
    row_groups: &'a [RowGroupMetaData],
    /// Helper to read the SST.
    read_format: &'a ReadFormat,
    /// Projected column ids to read.
    ///
    /// We need column ids to distinguish different columns with the same name.
    /// e.g. Drops and then adds a column again.
    column_ids: HashSet<ColumnId>,
}

impl<'a> RowGroupPruningStats<'a> {
    /// Creates a new statistics to prune specific `row_groups`.
    pub(crate) fn new(
        row_groups: &'a [RowGroupMetaData],
        read_format: &'a ReadFormat,
        column_ids: HashSet<ColumnId>,
    ) -> Self {
        Self {
            row_groups,
            read_format,
            column_ids,
        }
    }

    /// Returns the column id of specific column name if we need to read it.
    fn column_id_to_prune(&self, name: &str) -> Option<ColumnId> {
        // Only use stats when the column to read has the same id as the column in the SST.
        self.read_format
            .metadata()
            .column_by_name(name)
            .and_then(|col| self.column_ids.get(&col.column_id).copied())
    }
}

impl<'a> PruningStatistics for RowGroupPruningStats<'a> {
    fn min_values(&self, column: &Column) -> Option<ArrayRef> {
        let column_id = self.column_id_to_prune(&column.name)?;
        self.read_format.min_values(self.row_groups, column_id)
    }

    fn max_values(&self, column: &Column) -> Option<ArrayRef> {
        let column_id = self.column_id_to_prune(&column.name)?;
        self.read_format.max_values(self.row_groups, column_id)
    }

    fn num_containers(&self) -> usize {
        self.row_groups.len()
    }

    fn null_counts(&self, column: &Column) -> Option<ArrayRef> {
        let column_id = self.column_id_to_prune(&column.name)?;
        self.read_format.null_counts(self.row_groups, column_id)
    }
}
