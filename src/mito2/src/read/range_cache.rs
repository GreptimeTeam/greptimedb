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
use datatypes::arrow::array::{Array, AsArray, DictionaryArray};
use datatypes::arrow::datatypes::UInt32Type;
use datatypes::arrow::record_batch::RecordBatch;
use datatypes::prelude::ConcreteDataType;
use futures::TryStreamExt;
use store_api::region_engine::PartitionRange;
use store_api::storage::{ColumnId, FileId, RegionId, TimeSeriesRowSelector};

use crate::cache::CacheStrategy;
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
    /// Precomputed size of all batches, accounting for shared dictionary values.
    estimated_batches_size: usize,
}

impl RangeScanCacheValue {
    pub(crate) fn new(batches: Vec<RecordBatch>, estimated_batches_size: usize) -> Self {
        Self {
            batches,
            estimated_batches_size,
        }
    }

    pub(crate) fn estimated_size(&self) -> usize {
        mem::size_of::<Self>()
            + self.batches.capacity() * mem::size_of::<RecordBatch>()
            + self.estimated_batches_size
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

/// Returns true if two primary key dictionary arrays share the same underlying
/// values buffers by pointer comparison.
///
/// The primary key column is always `DictionaryArray<UInt32Type>` with `Binary` values.
fn pk_values_ptr_eq(a: &DictionaryArray<UInt32Type>, b: &DictionaryArray<UInt32Type>) -> bool {
    let a = a.values().as_binary::<i32>();
    let b = b.values().as_binary::<i32>();
    let values_eq = a.values().ptr_eq(b.values()) && a.offsets().ptr_eq(b.offsets());
    match (a.nulls(), b.nulls()) {
        (Some(a), Some(b)) => values_eq && a.inner().ptr_eq(b.inner()),
        (None, None) => values_eq,
        _ => false,
    }
}

/// Buffers record batches for caching, tracking memory size while deduplicating
/// shared dictionary values across batches.
///
/// Uses the primary key column as a proxy to detect dictionary sharing: if the PK
/// column's dictionary values are pointer-equal across batches, we assume all
/// dictionary columns share their values and deduct the total dictionary values size.
struct CacheBatchBuffer {
    batches: Vec<RecordBatch>,
    /// Running total of batch memory.
    total_size: usize,
    /// The first batch's PK dictionary array, for pointer comparison.
    /// `None` if no dictionary PK column exists or no batch has been added yet.
    first_pk_dict: Option<DictionaryArray<UInt32Type>>,
    /// Sum of `get_array_memory_size()` of all dictionary value arrays from the first batch.
    total_dict_values_size: usize,
    /// Whether the PK dictionary is still shared across all batches seen so far.
    shared: bool,
}

impl CacheBatchBuffer {
    fn new() -> Self {
        Self {
            batches: Vec::new(),
            total_size: 0,
            first_pk_dict: None,
            total_dict_values_size: 0,
            shared: true,
        }
    }

    fn push(&mut self, batch: RecordBatch) {
        if self.batches.is_empty() {
            self.init_first_batch(&batch);
        } else {
            self.add_subsequent_batch(&batch);
        }
        self.batches.push(batch);
    }

    fn init_first_batch(&mut self, batch: &RecordBatch) {
        self.total_size += batch.get_array_memory_size();

        let pk_col_idx = primary_key_column_index(batch.num_columns());
        let mut total_dict_values_size = 0;
        for col_idx in 0..batch.num_columns() {
            let col = batch.column(col_idx);
            if let Some(dict) = col.as_any().downcast_ref::<DictionaryArray<UInt32Type>>() {
                total_dict_values_size += dict.values().get_array_memory_size();
                if col_idx == pk_col_idx {
                    self.first_pk_dict = Some(dict.clone());
                }
            }
        }
        self.total_dict_values_size = total_dict_values_size;
    }

    fn add_subsequent_batch(&mut self, batch: &RecordBatch) {
        let batch_size = batch.get_array_memory_size();

        if self.shared
            && let Some(first_pk_dict) = &self.first_pk_dict
        {
            let pk_col_idx = primary_key_column_index(batch.num_columns());
            let col = batch.column(pk_col_idx);
            if let Some(dict) = col.as_any().downcast_ref::<DictionaryArray<UInt32Type>>()
                && pk_values_ptr_eq(first_pk_dict, dict)
            {
                // PK dict is shared, deduct all dict values sizes.
                self.total_size += batch_size - self.total_dict_values_size;
                return;
            }
            // Dictionary diverged.
            self.shared = false;
        }

        self.total_size += batch_size;
    }

    fn estimated_batches_size(&self) -> usize {
        self.total_size
    }

    fn into_batches(self) -> Vec<RecordBatch> {
        self.batches
    }
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
        let mut buffer = CacheBatchBuffer::new();
        while let Some(batch) = stream.try_next().await? {
            buffer.push(batch.clone());
            yield batch;
        }

        let estimated_size = buffer.estimated_batches_size();
        let batches = buffer.into_batches();
        let value = Arc::new(RangeScanCacheValue::new(batches, estimated_size));
        part_metrics.inc_range_cache_size(key.estimated_size() + value.estimated_size());
        cache_strategy.put_range_result(key, value);
    })
}

/// Creates a `cache_flat_range_stream` with dummy internals for benchmarking.
///
/// This avoids exposing `RangeScanCacheKey`, `ScanRequestFingerprint`, and
/// `PartitionMetrics` publicly.
#[cfg(feature = "test")]
pub fn bench_cache_flat_range_stream(
    stream: BoxedRecordBatchStream,
    cache_size_bytes: u64,
    region_id: RegionId,
) -> BoxedRecordBatchStream {
    use std::time::Instant;

    use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;

    use crate::region::options::MergeMode;

    let cache_manager = Arc::new(
        crate::cache::CacheManager::builder()
            .range_result_cache_size(cache_size_bytes)
            .build(),
    );
    let cache_strategy = CacheStrategy::EnableAll(cache_manager);

    let fingerprint = ScanRequestFingerprintBuilder {
        read_column_ids: vec![],
        read_column_types: vec![],
        filters: vec![],
        time_filters: vec![],
        series_row_selector: None,
        append_mode: false,
        filter_deleted: false,
        merge_mode: MergeMode::LastRow,
        partition_expr_version: 0,
    }
    .build();

    let key = RangeScanCacheKey {
        region_id,
        row_groups: vec![],
        scan: fingerprint,
    };

    let metrics_set = ExecutionPlanMetricsSet::new();
    let part_metrics =
        PartitionMetrics::new(region_id, 0, "bench", Instant::now(), false, &metrics_set);

    cache_flat_range_stream(stream, cache_strategy, key, part_metrics)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Instant;

    use common_time::Timestamp;
    use common_time::range::TimestampRange;
    use common_time::timestamp::TimeUnit;
    use datafusion_common::ScalarValue;
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
        let mapper = ProjectionMapper::new(&metadata, [0, 2, 3].into_iter()).unwrap();
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
            .with_cache(test_cache_strategy());
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

    /// Helper to create a timestamp millisecond literal.
    fn ts_lit(val: i64) -> Expr {
        lit(ScalarValue::TimestampMillisecond(Some(val), None))
    }

    #[tokio::test]
    async fn strips_time_only_filters_when_query_covers_partition_range() {
        let (stream_ctx, part_range) = new_stream_context(
            vec![
                col("ts").gt_eq(ts_lit(1000)),
                col("ts").lt(ts_lit(2001)),
                col("ts").is_not_null(),
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

        // Range-reducible time filters should be cleared when query covers partition range.
        assert!(key.scan.time_filters().is_empty());
        // Non-range time predicates stay in filters.
        let mut expected_filters = [
            col("k0").eq(lit("foo")).to_string(),
            col("ts").is_not_null().to_string(),
        ];
        expected_filters.sort_unstable();
        assert_eq!(key.scan.filters(), expected_filters.as_slice());
    }

    #[tokio::test]
    async fn preserves_time_filters_when_query_does_not_cover_partition_range() {
        let (stream_ctx, part_range) = new_stream_context(
            vec![col("ts").gt_eq(ts_lit(1000)), col("k0").eq(lit("foo"))],
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
            [col("ts").gt_eq(ts_lit(1000)).to_string()].as_slice()
        );
        assert_eq!(
            key.scan.filters(),
            [col("k0").eq(lit("foo")).to_string()].as_slice()
        );
    }

    #[tokio::test]
    async fn strips_time_only_filters_when_query_has_no_time_range_limit() {
        let (stream_ctx, part_range) = new_stream_context(
            vec![
                col("ts").gt_eq(ts_lit(1000)),
                col("ts").is_not_null(),
                col("k0").eq(lit("foo")),
            ],
            None,
            (
                Timestamp::new_millisecond(1000),
                Timestamp::new_millisecond(2000),
            ),
        )
        .await;

        let key = build_range_cache_key(&stream_ctx, &part_range).unwrap();

        // Range-reducible time filters should be cleared when query has no time range limit.
        assert!(key.scan.time_filters().is_empty());
        // Non-range time predicates stay in filters.
        let mut expected_filters = [
            col("k0").eq(lit("foo")).to_string(),
            col("ts").is_not_null().to_string(),
        ];
        expected_filters.sort_unstable();
        assert_eq!(key.scan.filters(), expected_filters.as_slice());
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

    /// Creates a test schema with 5 columns where the primary key dictionary column
    /// is at index 2 (`num_columns - 3`), matching the flat format layout.
    ///
    /// Layout: `[field0: Int64, field1: Int64, pk: Dictionary<UInt32,Binary>, ts: Int64, seq: Int64]`
    fn dict_test_schema() -> Arc<datatypes::arrow::datatypes::Schema> {
        use datatypes::arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
        Arc::new(Schema::new(vec![
            Field::new("field0", ArrowDataType::Int64, false),
            Field::new("field1", ArrowDataType::Int64, false),
            Field::new(
                "pk",
                ArrowDataType::Dictionary(
                    Box::new(ArrowDataType::UInt32),
                    Box::new(ArrowDataType::Binary),
                ),
                false,
            ),
            Field::new("ts", ArrowDataType::Int64, false),
            Field::new("seq", ArrowDataType::Int64, false),
        ]))
    }

    /// Helper to create a record batch with a dictionary column at the primary key position.
    fn make_dict_batch(
        schema: Arc<datatypes::arrow::datatypes::Schema>,
        dict_values: &datatypes::arrow::array::BinaryArray,
        keys: &[u32],
        int_values: &[i64],
    ) -> RecordBatch {
        use datatypes::arrow::array::{Int64Array, UInt32Array};

        let key_array = UInt32Array::from(keys.to_vec());
        let dict_array: DictionaryArray<UInt32Type> =
            DictionaryArray::new(key_array, Arc::new(dict_values.clone()));
        let int_array = Int64Array::from(int_values.to_vec());
        let zeros = Int64Array::from(vec![0i64; int_values.len()]);
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(zeros.clone()),
                Arc::new(int_array),
                Arc::new(dict_array),
                Arc::new(zeros.clone()),
                Arc::new(zeros),
            ],
        )
        .unwrap()
    }

    /// Computes the total `get_array_memory_size()` of all dictionary value arrays in a batch.
    fn compute_total_dict_values_size(batch: &RecordBatch) -> usize {
        batch
            .columns()
            .iter()
            .filter_map(|col| {
                col.as_any()
                    .downcast_ref::<DictionaryArray<UInt32Type>>()
                    .map(|dict| dict.values().get_array_memory_size())
            })
            .sum()
    }

    #[test]
    fn cache_batch_buffer_empty() {
        let buffer = CacheBatchBuffer::new();
        assert_eq!(buffer.estimated_batches_size(), 0);
        assert!(buffer.into_batches().is_empty());
    }

    #[test]
    fn cache_batch_buffer_single_batch() {
        use datatypes::arrow::array::BinaryArray;

        let schema = dict_test_schema();
        let dict_values = BinaryArray::from_vec(vec![b"a", b"b", b"c"]);
        let batch = make_dict_batch(schema, &dict_values, &[0, 1, 2], &[10, 20, 30]);

        let full_size = batch.get_array_memory_size();

        let mut buffer = CacheBatchBuffer::new();
        buffer.push(batch);
        assert_eq!(buffer.estimated_batches_size(), full_size);
        assert_eq!(buffer.into_batches().len(), 1);
    }

    #[test]
    fn cache_batch_buffer_shared_dictionary() {
        use datatypes::arrow::array::BinaryArray;

        let schema = dict_test_schema();
        let dict_values = BinaryArray::from_vec(vec![b"alpha", b"beta", b"gamma"]);

        // Two batches sharing the same dictionary values array.
        let batch1 = make_dict_batch(schema.clone(), &dict_values, &[0, 1], &[10, 20]);
        let batch2 = make_dict_batch(schema, &dict_values, &[1, 2], &[30, 40]);

        let batch1_full = batch1.get_array_memory_size();
        let batch2_full = batch2.get_array_memory_size();

        // The total dictionary values size that should be deduplicated for the second batch.
        let dict_values_size = compute_total_dict_values_size(&batch2);

        let mut buffer = CacheBatchBuffer::new();
        buffer.push(batch1);
        buffer.push(batch2);

        // Second batch's dict values should not be counted again.
        assert_eq!(
            buffer.estimated_batches_size(),
            batch1_full + batch2_full - dict_values_size
        );
        assert_eq!(buffer.into_batches().len(), 2);
    }

    #[test]
    fn cache_batch_buffer_non_shared_dictionary() {
        use datatypes::arrow::array::BinaryArray;

        let schema = dict_test_schema();
        let dict_values1 = BinaryArray::from_vec(vec![b"a", b"b"]);
        let dict_values2 = BinaryArray::from_vec(vec![b"x", b"y"]);

        let batch1 = make_dict_batch(schema.clone(), &dict_values1, &[0, 1], &[10, 20]);
        let batch2 = make_dict_batch(schema, &dict_values2, &[0, 1], &[30, 40]);

        let batch1_full = batch1.get_array_memory_size();
        let batch2_full = batch2.get_array_memory_size();

        let mut buffer = CacheBatchBuffer::new();
        buffer.push(batch1);
        buffer.push(batch2);

        // Different dictionaries: full size for both.
        assert_eq!(buffer.estimated_batches_size(), batch1_full + batch2_full);
    }

    #[test]
    fn cache_batch_buffer_shared_then_diverged() {
        use datatypes::arrow::array::BinaryArray;

        let schema = dict_test_schema();
        let shared_values = BinaryArray::from_vec(vec![b"a", b"b", b"c"]);
        let different_values = BinaryArray::from_vec(vec![b"x", b"y"]);

        let batch1 = make_dict_batch(schema.clone(), &shared_values, &[0], &[1]);
        let batch2 = make_dict_batch(schema.clone(), &shared_values, &[1], &[2]);
        let batch3 = make_dict_batch(schema, &different_values, &[0], &[3]);

        let size1 = batch1.get_array_memory_size();
        let size2 = batch2.get_array_memory_size();
        let size3 = batch3.get_array_memory_size();

        let dict_values_size = compute_total_dict_values_size(&batch2);

        let mut buffer = CacheBatchBuffer::new();
        buffer.push(batch1);
        buffer.push(batch2);
        buffer.push(batch3);

        // batch2 shares dict with batch1 (dedup), batch3 does not (full size).
        assert_eq!(
            buffer.estimated_batches_size(),
            size1 + (size2 - dict_values_size) + size3
        );
    }
}
