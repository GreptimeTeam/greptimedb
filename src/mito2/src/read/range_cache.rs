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

//! Utilities for the partition range scan result cache.

use std::mem;
use std::sync::Arc;

use datatypes::arrow::record_batch::RecordBatch;
use datatypes::prelude::ConcreteDataType;
use store_api::storage::{
    ColumnId, FileId, RegionId, TimeSeriesDistribution, TimeSeriesRowSelector,
};

use crate::memtable::record_batch_estimated_size;
use crate::region::options::MergeMode;

/// Fingerprint of the scan request fields that affect partition range cache reuse.
///
/// It records a normalized view of the projected columns and filters, plus
/// scan options that can change the returned rows. Schema-dependent metadata
/// and the partition expression version are included so cached results are not
/// reused across incompatible schema or partitioning changes.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct ScanRequestFingerprint {
    /// Projection and filters without the time index and partition exprs.
    inner: Arc<SharedScanRequestFingerprint>,
    /// Filters with the time index column.
    time_filters: Option<Arc<Vec<String>>>,
    pub(crate) series_row_selector: Option<TimeSeriesRowSelector>,
    pub(crate) distribution: Option<TimeSeriesDistribution>,
    pub(crate) append_mode: bool,
    pub(crate) filter_deleted: bool,
    pub(crate) merge_mode: MergeMode,
    /// We keep the partition expr version to ensure we won't reuse the fingerprint after we change the partition expr.
    /// We store the version instead of the whole partition expr or partition expr filters.
    pub(crate) partition_expr_version: u64,
}

#[derive(Debug)]
pub(crate) struct ScanRequestFingerprintBuilder {
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

impl ScanRequestFingerprintBuilder {
    pub(crate) fn build(self) -> ScanRequestFingerprint {
        let Self {
            read_column_ids,
            read_column_types,
            filters,
            time_filters,
            series_row_selector,
            distribution,
            append_mode,
            filter_deleted,
            merge_mode,
            partition_expr_version,
        } = self;

        ScanRequestFingerprint {
            inner: Arc::new(SharedScanRequestFingerprint {
                read_column_ids,
                read_column_types,
                filters,
            }),
            time_filters: (!time_filters.is_empty()).then(|| Arc::new(time_filters)),
            series_row_selector,
            distribution,
            append_mode,
            filter_deleted,
            merge_mode,
            partition_expr_version,
        }
    }
}

/// Non-copiable struct of the fingerprint.
#[derive(Debug, PartialEq, Eq, Hash)]
struct SharedScanRequestFingerprint {
    /// Column ids of the projection.
    read_column_ids: Vec<ColumnId>,
    /// Column types of the projection.
    /// We keep this to ensure we won't reuse the fingerprint after a schema change.
    read_column_types: Vec<Option<ConcreteDataType>>,
    /// Filters without the time index column and region partition exprs.
    filters: Vec<String>,
}

impl ScanRequestFingerprint {
    #[cfg(test)]
    pub(crate) fn read_column_ids(&self) -> &[ColumnId] {
        &self.inner.read_column_ids
    }

    #[cfg(test)]
    pub(crate) fn read_column_types(&self) -> &[Option<ConcreteDataType>] {
        &self.inner.read_column_types
    }

    #[cfg(test)]
    pub(crate) fn filters(&self) -> &[String] {
        &self.inner.filters
    }

    pub(crate) fn time_filters(&self) -> &[String] {
        self.time_filters
            .as_deref()
            .map(Vec::as_slice)
            .unwrap_or(&[])
    }

    #[cfg(test)]
    pub(crate) fn without_time_filters(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            time_filters: None,
            series_row_selector: self.series_row_selector,
            distribution: self.distribution,
            append_mode: self.append_mode,
            filter_deleted: self.filter_deleted,
            merge_mode: self.merge_mode,
            partition_expr_version: self.partition_expr_version,
        }
    }

    pub(crate) fn estimated_size(&self) -> usize {
        mem::size_of::<SharedScanRequestFingerprint>()
            + self.inner.read_column_ids.capacity() * mem::size_of::<ColumnId>()
            + self.inner.read_column_types.capacity() * mem::size_of::<Option<ConcreteDataType>>()
            + self.inner.filters.capacity() * mem::size_of::<String>()
            + self
                .inner
                .filters
                .iter()
                .map(|filter| filter.capacity())
                .sum::<usize>()
            + self.time_filters.as_ref().map_or(0, |filters| {
                mem::size_of::<Vec<String>>()
                    + filters.capacity() * mem::size_of::<String>()
                    + filters
                        .iter()
                        .map(|filter| filter.capacity())
                        .sum::<usize>()
            })
    }
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
            + self.scan.estimated_size()
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

#[cfg(test)]
mod tests {
    use store_api::storage::{TimeSeriesDistribution, TimeSeriesRowSelector};

    use super::*;

    #[test]
    fn normalizes_and_clears_time_filters() {
        let normalized = ScanRequestFingerprintBuilder {
            read_column_ids: vec![1, 2],
            read_column_types: vec![None, None],
            filters: vec!["k0 = 'foo'".to_string()],
            time_filters: vec![],
            series_row_selector: None,
            distribution: None,
            append_mode: false,
            filter_deleted: true,
            merge_mode: MergeMode::LastRow,
            partition_expr_version: 0,
        }
        .build();

        assert!(normalized.time_filters().is_empty());

        let fingerprint = ScanRequestFingerprintBuilder {
            read_column_ids: vec![1, 2],
            read_column_types: vec![None, None],
            filters: vec!["k0 = 'foo'".to_string()],
            time_filters: vec!["ts >= 1000".to_string()],
            series_row_selector: Some(TimeSeriesRowSelector::LastRow),
            distribution: Some(TimeSeriesDistribution::PerSeries),
            append_mode: false,
            filter_deleted: true,
            merge_mode: MergeMode::LastRow,
            partition_expr_version: 7,
        }
        .build();

        let reset = fingerprint.without_time_filters();

        assert_eq!(reset.read_column_ids(), fingerprint.read_column_ids());
        assert_eq!(reset.read_column_types(), fingerprint.read_column_types());
        assert_eq!(reset.filters(), fingerprint.filters());
        assert!(reset.time_filters().is_empty());
        assert_eq!(reset.series_row_selector, fingerprint.series_row_selector);
        assert_eq!(reset.distribution, fingerprint.distribution);
        assert_eq!(reset.append_mode, fingerprint.append_mode);
        assert_eq!(reset.filter_deleted, fingerprint.filter_deleted);
        assert_eq!(reset.merge_mode, fingerprint.merge_mode);
        assert_eq!(
            reset.partition_expr_version,
            fingerprint.partition_expr_version
        );
    }
}
