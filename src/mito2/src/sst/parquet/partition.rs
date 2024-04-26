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

//! Structs and functions for reading partitions from a parquet file. A partition
//! is usually a row group in a parquet file.

use std::sync::Arc;

use common_recordbatch::filter::SimpleFilterEvaluator;
use parquet::arrow::arrow_reader::{ParquetRecordBatchReader, RowSelection};

use crate::error::Result;
use crate::sst::parquet::format::ReadFormat;
use crate::sst::parquet::reader::RowGroupReaderBuilder;

/// A partition of a parquet SST. Now it is a row group.
pub(crate) struct Partition {
    /// Shared context.
    context: PartitionContextRef,
    /// Index of the row group in the SST.
    row_group_idx: usize,
    /// Row selection for the row group. `None` means all rows.
    row_selection: Option<RowSelection>,
}

impl Partition {
    /// Creates a new partition.
    pub(crate) fn new(
        context: PartitionContextRef,
        row_group_idx: usize,
        row_selection: Option<RowSelection>,
    ) -> Self {
        Self {
            context,
            row_group_idx,
            row_selection,
        }
    }

    /// Returns a reader to read the partition.
    pub(crate) async fn reader(&self) -> Result<ParquetRecordBatchReader> {
        self.context
            .reader_builder
            .build(self.row_group_idx, self.row_selection.clone())
            .await
    }
}

/// Context shared by partitions of the same parquet SST.
pub(crate) struct PartitionContext {
    // Row group reader builder for the file.
    reader_builder: RowGroupReaderBuilder,
    /// Filters pushed down.
    filters: Vec<SimpleFilterEvaluator>,
    /// Helper to read the SST.
    format: ReadFormat,
}

pub(crate) type PartitionContextRef = Arc<PartitionContext>;

impl PartitionContext {
    /// Creates a new partition context.
    pub(crate) fn new(
        reader_builder: RowGroupReaderBuilder,
        filters: Vec<SimpleFilterEvaluator>,
        format: ReadFormat,
    ) -> Self {
        Self {
            reader_builder,
            filters,
            format,
        }
    }
}
