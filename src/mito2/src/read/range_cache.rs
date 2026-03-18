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

use async_stream::try_stream;
use common_time::range::TimestampRange;
use datatypes::arrow::array::{Array, BinaryArray, DictionaryArray, UInt32Array};
use datatypes::arrow::datatypes::UInt32Type;
use datatypes::arrow::record_batch::RecordBatch;
use datatypes::prelude::ConcreteDataType;
use futures::TryStreamExt;
use store_api::region_engine::PartitionRange;
use store_api::storage::{ColumnId, FileId, RegionId, TimeSeriesRowSelector};

use crate::cache::CacheStrategy;
use crate::memtable::record_batch_estimated_size;
use crate::read::BoxedRecordBatchStream;
use crate::read::scan_region::StreamContext;
use crate::read::scan_util::PartitionMetrics;
use crate::region::options::MergeMode;
use crate::sst::file::FileTimeRange;
use crate::sst::parquet::flat_format::primary_key_column_index;

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
    series_row_selector: Option<TimeSeriesRowSelector>,
    append_mode: bool,
    filter_deleted: bool,
    merge_mode: MergeMode,
    /// We keep the partition expr version to ensure we won't reuse the fingerprint after we change the partition expr.
    /// We store the version instead of the whole partition expr or partition expr filters.
    partition_expr_version: u64,
}

#[derive(Debug)]
pub(crate) struct ScanRequestFingerprintBuilder {
    pub(crate) read_column_ids: Vec<ColumnId>,
    pub(crate) read_column_types: Vec<Option<ConcreteDataType>>,
    pub(crate) filters: Vec<String>,
    pub(crate) time_filters: Vec<String>,
    pub(crate) series_row_selector: Option<TimeSeriesRowSelector>,
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

    #[cfg(test)]
    pub(crate) fn time_filters(&self) -> &[String] {
        self.time_filters
            .as_deref()
            .map(Vec::as_slice)
            .unwrap_or(&[])
    }

    pub(crate) fn without_time_filters(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            time_filters: None,
            series_row_selector: self.series_row_selector,
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
    /// Sorted (file_id, row_group_index) pairs that uniquely identify the data this range covers.
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

/// Row groups and whether all sources are file-only for a partition range.
#[allow(dead_code)]
pub(crate) struct PartitionRangeRowGroups {
    /// Sorted (file_id, row_group_index) pairs.
    pub(crate) row_groups: Vec<(FileId, i64)>,
    pub(crate) only_file_sources: bool,
}

/// Collects (file_id, row_group_index) pairs from a partition range's row group indices.
#[allow(dead_code)]
pub(crate) fn collect_partition_range_row_groups(
    stream_ctx: &StreamContext,
    part_range: &PartitionRange,
) -> PartitionRangeRowGroups {
    let range_meta = &stream_ctx.ranges[part_range.identifier];
    let mut row_groups = Vec::new();
    let mut only_file_sources = true;

    for index in &range_meta.row_group_indices {
        if stream_ctx.is_file_range_index(*index) {
            let file_id = stream_ctx.input.file_from_index(*index).file_id().file_id();
            row_groups.push((file_id, index.row_group_index));
        } else {
            only_file_sources = false;
        }
    }

    row_groups.sort_unstable_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()).then(a.1.cmp(&b.1)));

    PartitionRangeRowGroups {
        row_groups,
        only_file_sources,
    }
}

/// Builds a cache key for the given partition range if it is eligible for caching.
#[allow(dead_code)]
pub(crate) fn build_range_cache_key(
    stream_ctx: &StreamContext,
    part_range: &PartitionRange,
) -> Option<RangeScanCacheKey> {
    let fingerprint = stream_ctx.scan_fingerprint.as_ref()?;

    // Dyn filters can change at runtime, so we can't cache when they're present.
    let has_dyn_filters = stream_ctx
        .input
        .predicate_group()
        .predicate_without_region()
        .is_some_and(|p| !p.dyn_filters().is_empty());
    if has_dyn_filters {
        return None;
    }

    let rg = collect_partition_range_row_groups(stream_ctx, part_range);
    if !rg.only_file_sources || rg.row_groups.is_empty() {
        return None;
    }

    let range_meta = &stream_ctx.ranges[part_range.identifier];
    let scan = if query_time_range_covers_partition_range(
        stream_ctx.input.time_range.as_ref(),
        range_meta.time_range,
    ) {
        fingerprint.without_time_filters()
    } else {
        fingerprint.clone()
    };

    Some(RangeScanCacheKey {
        region_id: stream_ctx.input.region_metadata().region_id,
        row_groups: rg.row_groups,
        scan,
    })
}

#[allow(dead_code)]
fn query_time_range_covers_partition_range(
    query_time_range: Option<&TimestampRange>,
    partition_time_range: FileTimeRange,
) -> bool {
    let Some(query_time_range) = query_time_range else {
        return true;
    };

    let (part_start, part_end) = partition_time_range;
    query_time_range.contains(&part_start) && query_time_range.contains(&part_end)
}

/// Returns a stream that replays cached record batches.
#[allow(dead_code)]
pub(crate) fn cached_flat_range_stream(value: Arc<RangeScanCacheValue>) -> BoxedRecordBatchStream {
    Box::pin(futures::stream::iter(
        value.batches.clone().into_iter().map(Ok),
    ))
}

/// Compacts the `__primary_key` dictionary column in a record batch by removing
/// unreferenced values. This reduces memory usage when caching batches whose
/// dictionary values array contains entries not referenced by any key.
///
/// Only compacts when the values array has more than 4 entries to avoid
/// unnecessary work for small dictionaries.
fn compact_pk_dictionary(batch: RecordBatch) -> RecordBatch {
    let pk_idx = primary_key_column_index(batch.num_columns());
    let pk_col = match batch
        .column(pk_idx)
        .as_any()
        .downcast_ref::<DictionaryArray<UInt32Type>>()
    {
        Some(dict) if dict.values().len() > 4 => dict,
        _ => return batch,
    };

    let old_values = pk_col
        .values()
        .as_any()
        .downcast_ref::<BinaryArray>()
        .expect("primary key dictionary values must be BinaryArray");
    let keys = pk_col.keys();

    // Single linear pass: since keys are sorted/grouped, we track when the key changes.
    let mut remap = vec![0u32; old_values.len()];
    let mut new_values: Vec<&[u8]> = Vec::new();
    let mut prev_key: Option<u32> = None;

    for key in keys.iter().flatten() {
        if prev_key != Some(key) {
            let new_index = new_values.len() as u32;
            new_values.push(old_values.value(key as usize));
            remap[key as usize] = new_index;
            prev_key = Some(key);
        }
    }

    if new_values.len() == old_values.len() {
        return batch; // all values are in use
    }

    let new_keys = UInt32Array::from_iter(keys.iter().map(|k| k.map(|v| remap[v as usize])));
    let new_values_array = BinaryArray::from_iter_values(new_values);
    let new_dict = DictionaryArray::new(new_keys, Arc::new(new_values_array));

    let mut columns: Vec<_> = batch.columns().to_vec();
    columns[pk_idx] = Arc::new(new_dict);
    RecordBatch::try_new(batch.schema(), columns).expect("schema should match after compaction")
}

/// Wraps a stream to cache its output for future range cache hits.
#[allow(dead_code)]
pub(crate) fn cache_flat_range_stream(
    mut stream: BoxedRecordBatchStream,
    cache_strategy: CacheStrategy,
    key: RangeScanCacheKey,
    part_metrics: PartitionMetrics,
) -> BoxedRecordBatchStream {
    Box::pin(try_stream! {
        let mut batches = Vec::new();
        while let Some(batch) = stream.try_next().await? {
            let batch = compact_pk_dictionary(batch);
            batches.push(batch.clone());
            yield batch;
        }

        if !batches.is_empty() {
            let value = Arc::new(RangeScanCacheValue::new(batches));

            part_metrics.inc_range_cache_size(key.estimated_size() + value.estimated_size());
            cache_strategy.put_range_result(key, value);
        } else {
            part_metrics.inc_range_cache_size(key.estimated_size());
            let value = Arc::new(RangeScanCacheValue::new(batches));
            cache_strategy.put_range_result(key, value);
        }
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Instant;

    use common_time::Timestamp;
    use common_time::range::TimestampRange;
    use common_time::timestamp::TimeUnit;
    use datafusion_expr::{Expr, col, lit};
    use smallvec::smallvec;
    use store_api::storage::FileId;

    use super::*;
    use crate::cache::CacheManager;
    use crate::read::projection::ProjectionMapper;
    use crate::read::range::{RangeMeta, RowGroupIndex, SourceIndex};
    use crate::read::scan_region::{PredicateGroup, ScanInput};
    use crate::test_util::memtable_util::metadata_with_primary_key;
    use crate::test_util::scheduler_util::SchedulerEnv;
    use crate::test_util::sst_util::sst_file_handle_with_file_id;

    fn test_cache_strategy() -> CacheStrategy {
        CacheStrategy::EnableAll(Arc::new(
            CacheManager::builder()
                .range_result_cache_size(1024)
                .build(),
        ))
    }

    async fn new_stream_context(
        filters: Vec<Expr>,
        query_time_range: Option<TimestampRange>,
        partition_time_range: FileTimeRange,
    ) -> (StreamContext, PartitionRange) {
        let env = SchedulerEnv::new().await;
        let metadata = Arc::new(metadata_with_primary_key(vec![0, 1], false));
        let mapper = ProjectionMapper::new(&metadata, [0, 2, 3].into_iter(), true).unwrap();
        let predicate = PredicateGroup::new(metadata.as_ref(), &filters).unwrap();
        let file_id = FileId::random();
        let file = sst_file_handle_with_file_id(
            file_id,
            partition_time_range.0.value(),
            partition_time_range.1.value(),
        );
        let input = ScanInput::new(env.access_layer.clone(), mapper)
            .with_predicate(predicate)
            .with_time_range(query_time_range)
            .with_files(vec![file])
            .with_cache(test_cache_strategy())
            .with_flat_format(true);
        let range_meta = RangeMeta {
            time_range: partition_time_range,
            indices: smallvec![SourceIndex {
                index: 0,
                num_row_groups: 1,
            }],
            row_group_indices: smallvec![RowGroupIndex {
                index: 0,
                row_group_index: 0,
            }],
            num_rows: 10,
        };
        let partition_range = range_meta.new_partition_range(0);
        let scan_fingerprint = crate::read::scan_region::build_scan_fingerprint(&input);
        let stream_ctx = StreamContext {
            input,
            ranges: vec![range_meta],
            scan_fingerprint,
            query_start: Instant::now(),
        };

        (stream_ctx, partition_range)
    }

    #[tokio::test]
    async fn strips_time_only_filters_when_query_covers_partition_range() {
        let (stream_ctx, part_range) = new_stream_context(
            vec![
                col("ts").gt_eq(lit(1000)),
                col("ts").lt(lit(2001)),
                col("k0").eq(lit("foo")),
            ],
            TimestampRange::with_unit(1000, 2002, TimeUnit::Millisecond),
            (
                Timestamp::new_millisecond(1000),
                Timestamp::new_millisecond(2000),
            ),
        )
        .await;

        let key = build_range_cache_key(&stream_ctx, &part_range).unwrap();

        // Time filters should be cleared when query covers partition range.
        assert!(key.scan.time_filters().is_empty());
        assert_eq!(
            key.scan.filters(),
            [col("k0").eq(lit("foo")).to_string()].as_slice()
        );
    }

    #[tokio::test]
    async fn preserves_time_filters_when_query_does_not_cover_partition_range() {
        let (stream_ctx, part_range) = new_stream_context(
            vec![col("ts").gt_eq(lit(1000)), col("k0").eq(lit("foo"))],
            TimestampRange::with_unit(1000, 1500, TimeUnit::Millisecond),
            (
                Timestamp::new_millisecond(1000),
                Timestamp::new_millisecond(2000),
            ),
        )
        .await;

        let key = build_range_cache_key(&stream_ctx, &part_range).unwrap();

        // Time filters should be preserved when query does not cover partition range.
        assert_eq!(
            key.scan.time_filters(),
            [col("ts").gt_eq(lit(1000)).to_string()].as_slice()
        );
        assert_eq!(
            key.scan.filters(),
            [col("k0").eq(lit("foo")).to_string()].as_slice()
        );
    }

    #[tokio::test]
    async fn strips_time_only_filters_when_query_has_no_time_range_limit() {
        let (stream_ctx, part_range) = new_stream_context(
            vec![col("ts").gt_eq(lit(1000)), col("k0").eq(lit("foo"))],
            None,
            (
                Timestamp::new_millisecond(1000),
                Timestamp::new_millisecond(2000),
            ),
        )
        .await;

        let key = build_range_cache_key(&stream_ctx, &part_range).unwrap();

        // Time filters should be cleared when query has no time range limit.
        assert!(key.scan.time_filters().is_empty());
        assert_eq!(
            key.scan.filters(),
            [col("k0").eq(lit("foo")).to_string()].as_slice()
        );
    }

    #[test]
    fn normalizes_and_clears_time_filters() {
        let normalized = ScanRequestFingerprintBuilder {
            read_column_ids: vec![1, 2],
            read_column_types: vec![None, None],
            filters: vec!["k0 = 'foo'".to_string()],
            time_filters: vec![],
            series_row_selector: None,
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
        assert_eq!(reset.append_mode, fingerprint.append_mode);
        assert_eq!(reset.filter_deleted, fingerprint.filter_deleted);
        assert_eq!(reset.merge_mode, fingerprint.merge_mode);
        assert_eq!(
            reset.partition_expr_version,
            fingerprint.partition_expr_version
        );
    }
}
