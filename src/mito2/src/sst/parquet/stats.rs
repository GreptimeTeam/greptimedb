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

use std::borrow::Borrow;
use std::collections::HashSet;

use datafusion::physical_optimizer::pruning::PruningStatistics;
use datafusion_common::{Column, ScalarValue};
use datatypes::arrow::array::{ArrayRef, BooleanArray};
use parquet::file::metadata::RowGroupMetaData;
use store_api::metadata::RegionMetadataRef;
use store_api::storage::ColumnId;

use crate::sst::parquet::format::ReadFormat;

/// Statistics for pruning row groups.
pub(crate) struct RowGroupPruningStats<'a, T> {
    /// Metadata of SST row groups.
    row_groups: &'a [T],
    /// Helper to read the SST.
    read_format: &'a ReadFormat,
    /// The metadata of the region.
    /// It contains the schema a query expects to read. If it is not None, we use it instead
    /// of the metadata in the SST to get the column id of a column as the SST may have
    /// different columns.
    expected_metadata: Option<RegionMetadataRef>,
}

impl<'a, T> RowGroupPruningStats<'a, T> {
    /// Creates a new statistics to prune specific `row_groups`.
    pub(crate) fn new(
        row_groups: &'a [T],
        read_format: &'a ReadFormat,
        expected_metadata: Option<RegionMetadataRef>,
    ) -> Self {
        Self {
            row_groups,
            read_format,
            expected_metadata,
        }
    }

    /// Returns the column id of specific column name if we need to read it.
    fn column_id_to_prune(&self, name: &str) -> Option<ColumnId> {
        let metadata = self
            .expected_metadata
            .as_ref()
            .unwrap_or_else(|| self.read_format.metadata());
        // Only use stats when the column to read has the same id as the column in the SST.
        metadata.column_by_name(name).map(|col| col.column_id)
    }
}

impl<T: Borrow<RowGroupMetaData>> PruningStatistics for RowGroupPruningStats<'_, T> {
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

    fn row_counts(&self, _column: &Column) -> Option<ArrayRef> {
        // TODO(LFC): Impl it.
        None
    }

    fn contained(&self, _column: &Column, _values: &HashSet<ScalarValue>) -> Option<BooleanArray> {
        // TODO(LFC): Impl it.
        None
    }
}
