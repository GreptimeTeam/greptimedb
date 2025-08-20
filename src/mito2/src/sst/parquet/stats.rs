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
use std::sync::Arc;

use datafusion_common::pruning::PruningStatistics;
use datafusion_common::{Column, ScalarValue};
use datatypes::arrow::array::{ArrayRef, BooleanArray, UInt64Array};
use parquet::file::metadata::RowGroupMetaData;
use store_api::metadata::RegionMetadataRef;
use store_api::storage::ColumnId;

use crate::sst::parquet::format::{ReadFormat, StatValues};

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
    /// Prefers the column id in the expected metadata if it exists.
    fn column_id_to_prune(&self, name: &str) -> Option<ColumnId> {
        let metadata = self
            .expected_metadata
            .as_ref()
            .unwrap_or_else(|| self.read_format.metadata());
        metadata.column_by_name(name).map(|col| col.column_id)
    }

    /// Returns the default value of all row groups for `column` according to the metadata.
    fn compat_default_value(&self, column: &str) -> Option<ArrayRef> {
        let metadata = self.expected_metadata.as_ref()?;
        let col_metadata = metadata.column_by_name(column)?;
        col_metadata
            .column_schema
            .create_default_vector(self.row_groups.len())
            .unwrap_or(None)
            .map(|vector| vector.to_arrow_array())
    }
}

impl<T: Borrow<RowGroupMetaData>> RowGroupPruningStats<'_, T> {
    /// Returns the null count of all row groups for `column` according to the metadata.
    fn compat_null_count(&self, column: &str) -> Option<ArrayRef> {
        let metadata = self.expected_metadata.as_ref()?;
        let col_metadata = metadata.column_by_name(column)?;
        let value = col_metadata
            .column_schema
            .create_default()
            .unwrap_or(None)?;
        let values = self.row_groups.iter().map(|meta| {
            if value.is_null() {
                u64::try_from(meta.borrow().num_rows()).ok()
            } else {
                Some(0)
            }
        });
        Some(Arc::new(UInt64Array::from_iter(values)))
    }
}

impl<T: Borrow<RowGroupMetaData>> PruningStatistics for RowGroupPruningStats<'_, T> {
    fn min_values(&self, column: &Column) -> Option<ArrayRef> {
        let column_id = self.column_id_to_prune(&column.name)?;
        match self.read_format.min_values(self.row_groups, column_id) {
            StatValues::Values(values) => Some(values),
            StatValues::NoColumn => self.compat_default_value(&column.name),
            StatValues::NoStats => None,
        }
    }

    fn max_values(&self, column: &Column) -> Option<ArrayRef> {
        let column_id = self.column_id_to_prune(&column.name)?;
        match self.read_format.max_values(self.row_groups, column_id) {
            StatValues::Values(values) => Some(values),
            StatValues::NoColumn => self.compat_default_value(&column.name),
            StatValues::NoStats => None,
        }
    }

    fn num_containers(&self) -> usize {
        self.row_groups.len()
    }

    fn null_counts(&self, column: &Column) -> Option<ArrayRef> {
        let column_id = self.column_id_to_prune(&column.name)?;
        match self.read_format.null_counts(self.row_groups, column_id) {
            StatValues::Values(values) => Some(values),
            StatValues::NoColumn => self.compat_null_count(&column.name),
            StatValues::NoStats => None,
        }
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
