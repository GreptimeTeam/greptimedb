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
use common_telemetry::warn;
use common_time::range::TimestampRange;
use datatypes::arrow::compute::concat_batches;
use datatypes::arrow::record_batch::RecordBatch;
use datatypes::prelude::ConcreteDataType;
use futures::TryStreamExt;
use snafu::ResultExt;
use store_api::region_engine::PartitionRange;
use store_api::storage::{ColumnId, FileId, RegionId, TimeSeriesRowSelector};
use tokio::sync::{mpsc, oneshot};

use crate::cache::CacheStrategy;
use crate::error::{ComputeArrowSnafu, Result, UnexpectedSnafu};
use crate::read::BoxedRecordBatchStream;
use crate::read::scan_region::StreamContext;
use crate::read::scan_util::PartitionMetrics;
use crate::region::options::MergeMode;
use crate::sst::file::FileTimeRange;
use crate::sst::parquet::DEFAULT_READ_BATCH_SIZE;

const RANGE_CACHE_COMPACT_THRESHOLD_BYTES: usize = 8 * 1024 * 1024;

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
#[derive(Debug)]
pub(crate) struct CachedBatchSlice {
    batch: RecordBatch,
    slice_lengths: Vec<usize>,
}

impl CachedBatchSlice {
    fn metadata_size(&self) -> usize {
        self.slice_lengths.capacity() * mem::size_of::<usize>()
    }
}

pub(crate) struct RangeScanCacheValue {
    cached_batches: Vec<CachedBatchSlice>,
    /// Precomputed size of all compacted batches.
    estimated_batches_size: usize,
}

impl RangeScanCacheValue {
    pub(crate) fn new(
        cached_batches: Vec<CachedBatchSlice>,
        estimated_batches_size: usize,
    ) -> Self {
        Self {
            cached_batches,
            estimated_batches_size,
        }
    }

    pub(crate) fn estimated_size(&self) -> usize {
        mem::size_of::<Self>()
            + self.cached_batches.capacity() * mem::size_of::<CachedBatchSlice>()
            + self
                .cached_batches
                .iter()
                .map(CachedBatchSlice::metadata_size)
                .sum::<usize>()
            + self.estimated_batches_size
    }
}

/// Row groups and whether all sources are file-only for a partition range.
pub(crate) struct PartitionRangeRowGroups {
    /// Sorted (file_id, row_group_index) pairs.
    pub(crate) row_groups: Vec<(FileId, i64)>,
    pub(crate) only_file_sources: bool,
}

/// Collects (file_id, row_group_index) pairs from a partition range's row group indices.
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
pub(crate) fn build_range_cache_key(
    stream_ctx: &StreamContext,
    part_range: &PartitionRange,
) -> Option<RangeScanCacheKey> {
    if !stream_ctx.input.cache_strategy.has_range_result_cache() {
        return None;
    }

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
pub(crate) fn cached_flat_range_stream(value: Arc<RangeScanCacheValue>) -> BoxedRecordBatchStream {
    Box::pin(try_stream! {
        for cached_batch in &value.cached_batches {
            let mut offset = 0;
            for &len in &cached_batch.slice_lengths {
                yield cached_batch.batch.slice(offset, len);
                offset += len;
            }
        }
    })
}

enum CacheConcatCommand {
    Compact(Vec<RecordBatch>),
    Finish {
        pending: Vec<RecordBatch>,
        key: RangeScanCacheKey,
        cache_strategy: CacheStrategy,
        part_metrics: PartitionMetrics,
        result_tx: Option<oneshot::Sender<Option<Arc<RangeScanCacheValue>>>>,
    },
}

#[derive(Default)]
struct CacheConcatState {
    cached_batches: Vec<CachedBatchSlice>,
    estimated_size: usize,
}

impl CacheConcatState {
    async fn compact(
        &mut self,
        batches: Vec<RecordBatch>,
        limiter: &crate::cache::RangeResultMemoryLimiter,
    ) -> Result<()> {
        if batches.is_empty() {
            return Ok(());
        }

        let input_size = batches
            .iter()
            .map(RecordBatch::get_array_memory_size)
            .sum::<usize>();
        let _permit = limiter.acquire(input_size).await.map_err(|_| {
            UnexpectedSnafu {
                reason: "range result memory limiter is unexpectedly closed",
            }
            .build()
        })?;

        let compacted = compact_record_batches(batches)?;
        self.estimated_size += compacted.batch.get_array_memory_size();
        self.cached_batches.push(compacted);
        Ok(())
    }

    fn finish(self) -> RangeScanCacheValue {
        RangeScanCacheValue::new(self.cached_batches, self.estimated_size)
    }
}

fn compact_record_batches(batches: Vec<RecordBatch>) -> Result<CachedBatchSlice> {
    debug_assert!(!batches.is_empty());

    let slice_lengths = batches.iter().map(RecordBatch::num_rows).collect();
    build_cached_batch_slice(batches, slice_lengths)
}

fn build_cached_batch_slice(
    batches: Vec<RecordBatch>,
    slice_lengths: Vec<usize>,
) -> Result<CachedBatchSlice> {
    let batch = if batches.len() == 1 {
        batches.into_iter().next().unwrap()
    } else {
        let schema = batches[0].schema();
        concat_batches(&schema, &batches).context(ComputeArrowSnafu)?
    };

    Ok(CachedBatchSlice {
        batch,
        slice_lengths,
    })
}

async fn run_cache_concat_task(
    mut rx: mpsc::UnboundedReceiver<CacheConcatCommand>,
    limiter: Arc<crate::cache::RangeResultMemoryLimiter>,
) {
    let mut state = CacheConcatState::default();

    while let Some(cmd) = rx.recv().await {
        match cmd {
            CacheConcatCommand::Compact(batches) => {
                if let Err(err) = state.compact(batches, &limiter).await {
                    warn!(err; "Failed to compact range cache batches");
                    return;
                }
            }
            CacheConcatCommand::Finish {
                pending,
                key,
                cache_strategy,
                part_metrics,
                result_tx,
            } => {
                let result = state
                    .compact(pending, &limiter)
                    .await
                    .map(|()| state.finish());
                if let Err(err) = &result {
                    warn!(err; "Failed to finalize range cache batches");
                }

                let value = result.ok().map(Arc::new);
                if let Some(value) = &value {
                    part_metrics
                        .inc_range_cache_size(key.estimated_size() + value.estimated_size());
                    cache_strategy.put_range_result(key, value.clone());
                }
                if let Some(tx) = result_tx {
                    let _ = tx.send(value);
                }
                return;
            }
        }
    }
}

struct CacheBatchBuffer {
    num_sources: usize,
    skip_threshold_bytes: usize,
    buffered_batches: Vec<RecordBatch>,
    buffered_rows: usize,
    buffered_size: usize,
    total_weight: usize,
    sender: Option<mpsc::UnboundedSender<CacheConcatCommand>>,
}

impl CacheBatchBuffer {
    fn new(cache_strategy: &CacheStrategy, num_sources: usize) -> Self {
        let sender = cache_strategy.range_result_memory_limiter().map(|limiter| {
            let (tx, rx) = mpsc::unbounded_channel();
            common_runtime::spawn_global(run_cache_concat_task(rx, limiter.clone()));
            tx
        });

        Self {
            num_sources,
            skip_threshold_bytes: cache_strategy.range_result_cache_size().unwrap_or(0),
            buffered_batches: Vec::new(),
            buffered_rows: 0,
            buffered_size: 0,
            total_weight: 0,
            sender,
        }
    }

    fn push(&mut self, batch: RecordBatch) -> Result<()> {
        if self.sender.is_none() {
            return Ok(());
        }

        let batch_size = batch.get_array_memory_size();
        self.total_weight += estimate_batch_weight(&batch, self.num_sources);
        if self.total_weight > self.skip_threshold_bytes {
            self.buffered_batches.clear();
            self.buffered_rows = 0;
            self.buffered_size = 0;
            self.sender = None;
            return Ok(());
        }

        self.buffered_rows += batch.num_rows();
        self.buffered_size += batch_size;
        self.buffered_batches.push(batch);

        if self.buffered_batches.len() > 1
            && (self.buffered_rows > DEFAULT_READ_BATCH_SIZE
                || self.buffered_size > RANGE_CACHE_COMPACT_THRESHOLD_BYTES)
        {
            self.notify_compact();
        }

        Ok(())
    }

    fn notify_compact(&mut self) {
        if self.buffered_batches.is_empty() || self.sender.is_none() {
            return;
        }

        let batches = mem::take(&mut self.buffered_batches);
        self.buffered_rows = 0;
        self.buffered_size = 0;

        let Some(sender) = &self.sender else {
            return;
        };
        if sender.send(CacheConcatCommand::Compact(batches)).is_err() {
            self.sender = None;
        }
    }

    fn finish(
        mut self,
        key: RangeScanCacheKey,
        cache_strategy: CacheStrategy,
        part_metrics: PartitionMetrics,
        result_tx: Option<oneshot::Sender<Option<Arc<RangeScanCacheValue>>>>,
    ) {
        let Some(sender) = self.sender.take() else {
            return;
        };

        if sender
            .send(CacheConcatCommand::Finish {
                pending: mem::take(&mut self.buffered_batches),
                key,
                cache_strategy,
                part_metrics,
                result_tx,
            })
            .is_err()
        {
            self.sender = None;
        }
    }
}

fn estimate_batch_weight(batch: &RecordBatch, num_sources: usize) -> usize {
    let num_rows = batch.num_rows().max(1);
    let memory_size = batch.get_array_memory_size();
    memory_size
        .saturating_mul(num_sources.max(1))
        .saturating_mul(num_rows)
        .div_ceil(DEFAULT_READ_BATCH_SIZE)
}

/// Wraps a stream to cache its output for future range cache hits.
pub(crate) fn cache_flat_range_stream(
    mut stream: BoxedRecordBatchStream,
    cache_strategy: CacheStrategy,
    key: RangeScanCacheKey,
    part_metrics: PartitionMetrics,
    num_sources: usize,
) -> BoxedRecordBatchStream {
    Box::pin(try_stream! {
        let mut buffer = CacheBatchBuffer::new(&cache_strategy, num_sources);
        while let Some(batch) = stream.try_next().await? {
            buffer.push(batch.clone())?;
            yield batch;
        }

        buffer.finish(key, cache_strategy, part_metrics, None);
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

    cache_flat_range_stream(stream, cache_strategy, key, part_metrics, 1)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Instant;

    use common_time::Timestamp;
    use common_time::range::TimestampRange;
    use common_time::timestamp::TimeUnit;
    use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
    use datafusion_common::ScalarValue;
    use datafusion_expr::{Expr, col, lit};
    use smallvec::smallvec;
    use store_api::storage::{FileId, RegionId};

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
                .range_result_cache_size(1024 * 1024)
                .build(),
        ))
    }

    fn test_cache_context(strategy: &CacheStrategy) -> (RangeScanCacheKey, PartitionMetrics) {
        let region_id = RegionId::new(1, 1);
        let key = RangeScanCacheKey {
            region_id,
            row_groups: vec![],
            scan: ScanRequestFingerprintBuilder {
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
            .build(),
        };

        let metrics_set = ExecutionPlanMetricsSet::new();
        let part_metrics =
            PartitionMetrics::new(region_id, 0, "test", Instant::now(), false, &metrics_set);

        assert!(strategy.get_range_result(&key).is_none());
        (key, part_metrics)
    }

    async fn finish_cache_batch_buffer(
        buffer: CacheBatchBuffer,
        key: RangeScanCacheKey,
        cache_strategy: CacheStrategy,
        part_metrics: PartitionMetrics,
    ) -> Option<Arc<RangeScanCacheValue>> {
        let (tx, rx) = oneshot::channel();
        buffer.finish(key, cache_strategy, part_metrics, Some(tx));
        rx.await.context(crate::error::RecvSnafu).ok().flatten()
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

    fn test_schema() -> Arc<datatypes::arrow::datatypes::Schema> {
        use datatypes::arrow::datatypes::{DataType as ArrowDataType, Field, Schema};

        Arc::new(Schema::new(vec![Field::new(
            "value",
            ArrowDataType::Int64,
            false,
        )]))
    }

    fn make_batch(values: &[i64]) -> RecordBatch {
        use datatypes::arrow::array::Int64Array;

        RecordBatch::try_new(
            test_schema(),
            vec![Arc::new(Int64Array::from(values.to_vec()))],
        )
        .unwrap()
    }

    fn make_large_binary_batch(rows: usize, bytes_per_row: usize) -> RecordBatch {
        use datatypes::arrow::array::BinaryArray;
        use datatypes::arrow::datatypes::{DataType as ArrowDataType, Field, Schema};

        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            ArrowDataType::Binary,
            false,
        )]));
        let payload = vec![b'x'; bytes_per_row];
        let values = (0..rows).map(|_| payload.as_slice()).collect::<Vec<_>>();

        RecordBatch::try_new(schema, vec![Arc::new(BinaryArray::from_vec(values))]).unwrap()
    }

    #[test]
    fn compact_record_batches_keeps_original_boundaries() {
        let batches = vec![make_batch(&[1, 2]), make_batch(&[3]), make_batch(&[4, 5])];

        let compacted = compact_record_batches(batches).unwrap();

        assert_eq!(compacted.batch.num_rows(), 5);
        assert_eq!(compacted.slice_lengths, vec![2, 1, 2]);
    }

    #[tokio::test]
    async fn cached_flat_range_stream_replays_original_batches() {
        let value = Arc::new(RangeScanCacheValue::new(
            vec![CachedBatchSlice {
                batch: make_batch(&[1, 2, 3]),
                slice_lengths: vec![2, 1],
            }],
            make_batch(&[1, 2, 3]).get_array_memory_size(),
        ));

        let replayed = cached_flat_range_stream(value)
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        assert_eq!(replayed.len(), 2);
        assert_eq!(replayed[0].num_rows(), 2);
        assert_eq!(replayed[1].num_rows(), 1);
    }

    #[tokio::test]
    async fn cache_batch_buffer_finishes_pending_batches() {
        let strategy = test_cache_strategy();
        let batch = make_batch(&[1, 2, 3]);
        let expected_size = batch.get_array_memory_size();
        let (key, part_metrics) = test_cache_context(&strategy);

        let mut buffer = CacheBatchBuffer::new(&strategy, 1);
        buffer.push(batch).unwrap();

        let value = finish_cache_batch_buffer(buffer, key.clone(), strategy.clone(), part_metrics)
            .await
            .unwrap();
        assert_eq!(value.cached_batches.len(), 1);
        assert_eq!(value.cached_batches[0].slice_lengths, vec![3]);
        assert_eq!(value.estimated_batches_size, expected_size);
        assert!(Arc::ptr_eq(
            &value,
            &strategy.get_range_result(&key).unwrap()
        ));
    }

    #[tokio::test]
    async fn cache_batch_buffer_compacts_when_rows_exceed_default_batch_size() {
        let strategy = test_cache_strategy();
        let batch = make_batch(&vec![1; DEFAULT_READ_BATCH_SIZE / 2 + 1]);
        let (key, part_metrics) = test_cache_context(&strategy);

        let mut buffer = CacheBatchBuffer::new(&strategy, 1);
        buffer.push(batch.clone()).unwrap();
        buffer.push(batch).unwrap();

        assert_eq!(buffer.buffered_rows, 0);
        assert!(buffer.buffered_batches.is_empty());

        let value = finish_cache_batch_buffer(buffer, key, strategy, part_metrics)
            .await
            .unwrap();
        assert_eq!(value.cached_batches.len(), 1);
        assert_eq!(
            value.cached_batches[0].slice_lengths,
            vec![
                DEFAULT_READ_BATCH_SIZE / 2 + 1,
                DEFAULT_READ_BATCH_SIZE / 2 + 1
            ]
        );
    }

    #[tokio::test]
    async fn cache_batch_buffer_compacts_when_buffered_size_exceeds_threshold() {
        let large_batch = make_large_binary_batch(DEFAULT_READ_BATCH_SIZE, 4096);
        let strategy = CacheStrategy::EnableAll(Arc::new(
            CacheManager::builder()
                .range_result_cache_size((large_batch.get_array_memory_size() * 3) as u64)
                .build(),
        ));
        let (key, part_metrics) = test_cache_context(&strategy);

        let mut buffer = CacheBatchBuffer::new(&strategy, 1);
        buffer.skip_threshold_bytes = usize::MAX;
        buffer.push(large_batch.clone()).unwrap();

        assert_eq!(buffer.buffered_rows, large_batch.num_rows());
        assert_eq!(buffer.buffered_batches.len(), 1);

        buffer.push(large_batch.clone()).unwrap();

        assert_eq!(buffer.buffered_rows, 0);
        assert!(buffer.buffered_batches.is_empty());

        let value = finish_cache_batch_buffer(buffer, key, strategy, part_metrics)
            .await
            .unwrap();
        assert_eq!(value.cached_batches.len(), 1);
        assert_eq!(
            value.cached_batches[0].slice_lengths,
            vec![large_batch.num_rows(), large_batch.num_rows()]
        );
    }

    #[test]
    fn estimate_batch_weight_uses_scale_and_num_sources() {
        let batch = make_batch(&[1, 2]);
        let expected = batch
            .get_array_memory_size()
            .saturating_mul(3)
            .saturating_mul(batch.num_rows())
            .div_ceil(DEFAULT_READ_BATCH_SIZE);

        assert_eq!(estimate_batch_weight(&batch, 3), expected);
    }

    #[tokio::test]
    async fn cache_batch_buffer_skips_cache_when_weight_exceeds_limit() {
        let strategy = test_cache_strategy();
        let (key, part_metrics) = test_cache_context(&strategy);
        let batch = make_batch(&[1]);
        let mut buffer = CacheBatchBuffer::new(&strategy, 1);
        buffer.skip_threshold_bytes = 0;

        buffer.push(batch).unwrap();

        assert!(buffer.sender.is_none());
        assert!(
            finish_cache_batch_buffer(buffer, key, strategy, part_metrics)
                .await
                .is_none()
        );
    }

    #[test]
    fn cache_batch_buffer_uses_configured_skip_threshold() {
        let strategy = test_cache_strategy();
        let buffer = CacheBatchBuffer::new(&strategy, 1);

        assert_eq!(
            buffer.skip_threshold_bytes,
            strategy.range_result_cache_size().unwrap()
        );
    }
}
