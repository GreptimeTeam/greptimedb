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

//! Cache key types for range scan outputs.

use std::mem;

use datatypes::arrow::record_batch::RecordBatch;
use datatypes::prelude::ConcreteDataType;
use store_api::storage::{
    ColumnId, FileId, RegionId, TimeSeriesDistribution, TimeSeriesRowSelector,
};

use crate::memtable::record_batch_estimated_size;
use crate::region::options::MergeMode;

/// Fingerprint of request-relevant scan options.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct ScanRequestFingerprint {
    pub(crate) read_column_ids: Vec<ColumnId>,
    pub(crate) read_column_types: Vec<Option<ConcreteDataType>>,
    pub(crate) filters: Vec<String>,
    pub(crate) time_filters: Vec<String>,
    pub(crate) series_row_selector: Option<TimeSeriesRowSelector>,
    pub(crate) distribution: Option<TimeSeriesDistribution>,
    pub(crate) append_mode: bool,
    pub(crate) filter_deleted: bool,
    pub(crate) merge_mode: MergeMode,
    pub(crate) partition_expr_version: u64,
}

/// Cache key for range scan outputs.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct RangeScanCacheKey {
    pub(crate) region_id: RegionId,
    /// Sorted (file_id, row_group_index) pairs that uniquely identify the covered data.
    pub(crate) row_groups: Vec<(FileId, i64)>,
    pub(crate) scan: ScanRequestFingerprint,
}

impl RangeScanCacheKey {
    pub(crate) fn estimated_size(&self) -> usize {
        mem::size_of::<Self>()
            + self.row_groups.capacity() * mem::size_of::<(FileId, i64)>()
            + self.scan.read_column_ids.capacity() * mem::size_of::<ColumnId>()
            + self.scan.read_column_types.capacity() * mem::size_of::<Option<ConcreteDataType>>()
            + self
                .scan
                .filters
                .iter()
                .map(|filter| filter.capacity())
                .sum::<usize>()
            + self
                .scan
                .time_filters
                .iter()
                .map(|filter| filter.capacity())
                .sum::<usize>()
    }
}

/// Cached result for one range scan.
pub(crate) struct RangeScanCacheValue {
    pub(crate) batches: Vec<RecordBatch>,
}

impl RangeScanCacheValue {
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn new(batches: Vec<RecordBatch>) -> Self {
        Self { batches }
    }

    pub(crate) fn estimated_size(&self) -> usize {
        mem::size_of::<Self>()
            + self.batches.capacity() * mem::size_of::<RecordBatch>()
            + self
                .batches
                .iter()
                .map(record_batch_estimated_size)
                .sum::<usize>()
    }
}
