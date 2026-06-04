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
use common_time::Timestamp;
use common_time::range::TimestampRange;
use common_time::timestamp::TimeUnit;
use datafusion_expr::expr::Expr;
use datafusion_expr::{Between, BinaryExpr, Operator};
use datatypes::arrow::compute::concat_batches;
use datatypes::arrow::record_batch::RecordBatch;
use datatypes::prelude::ConcreteDataType;
use datatypes::value::scalar_value_to_timestamp;
use futures::TryStreamExt;
use snafu::ResultExt;
use store_api::region_engine::PartitionRange;
use store_api::storage::{FileId, RegionId, TimeSeriesRowSelector};
use table::predicate::is_string_timestamp_literal;
use tokio::sync::{mpsc, oneshot};

use crate::cache::CacheStrategy;
use crate::error::{ComputeArrowSnafu, Result};
use crate::read::BoxedRecordBatchStream;
use crate::read::read_columns::ReadColumns;
use crate::read::scan_region::StreamContext;
use crate::read::scan_util::PartitionMetrics;
use crate::region::options::MergeMode;
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
    pub(crate) read_columns: ReadColumns,
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
            read_columns,
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
                read_columns,
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
    /// Logical columns of the projection.
    read_columns: ReadColumns,
    /// Column types of the projection.
    /// We keep this to ensure we won't reuse the fingerprint after a schema change.
    read_column_types: Vec<Option<ConcreteDataType>>,
    /// Filters without the time index column and region partition exprs.
    filters: Vec<String>,
}

impl ScanRequestFingerprint {
    #[cfg(test)]
    pub(crate) fn read_columns(&self) -> &ReadColumns {
        &self.inner.read_columns
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
            + self.inner.read_columns.estimated_size()
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

/// Returns the timestamp range where all time-only predicates are guaranteed true.
///
/// Returns `Some(min_to_max)` for empty input (vacuously true everywhere).
/// Returns `None` if any expression contains an unsupported shape: `OR`, `NOT`,
/// `IN`, non-literal RHS, unsupported operator, column-name mismatch, an `=`
/// literal that cannot be represented exactly in the column unit, or overflow
/// during bound adjustment.
///
/// This is intentionally stricter than `extract_time_range_from_expr` in
/// `table::predicate`: lower bounds round up and upper bounds round down. If a
/// partition's file-time range is contained by the returned range, every row in
/// that partition satisfies the original time predicates.
///
/// `IsNull`/`IsNotNull` on the time index are not routed into `time_filters`
/// today. If that changes, handle them here before stripping time filters from
/// the cache key.
pub(crate) fn implied_time_range_from_exprs(
    ts_col_name: &str,
    ts_col_unit: TimeUnit,
    exprs: &[&Expr],
) -> Option<TimestampRange> {
    let mut acc = TimestampRange::min_to_max();
    for expr in exprs {
        let r = implied_time_range_from_expr(ts_col_name, ts_col_unit, expr)?;
        acc = acc.and(&r);
    }
    Some(acc)
}

fn implied_time_range_from_expr(
    ts_col_name: &str,
    ts_col_unit: TimeUnit,
    expr: &Expr,
) -> Option<TimestampRange> {
    match expr {
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => match op {
            Operator::And => {
                let l = implied_time_range_from_expr(ts_col_name, ts_col_unit, left)?;
                let r = implied_time_range_from_expr(ts_col_name, ts_col_unit, right)?;
                Some(l.and(&r))
            }
            Operator::Eq | Operator::Lt | Operator::LtEq | Operator::Gt | Operator::GtEq => {
                implied_from_cmp(ts_col_name, ts_col_unit, left, *op, right)
            }
            // `OR` would require a strict intersection over a union of half-planes
            // (not the loose-span union provided by `TimestampRange::or`), so we
            // refuse it. Any other operator is unsupported.
            _ => None,
        },
        Expr::Between(Between {
            expr,
            negated,
            low,
            high,
        }) => {
            if *negated {
                return None;
            }
            implied_from_between(ts_col_name, ts_col_unit, expr, low, high)
        }
        // Includes `IsNull`, `IsNotNull`, `Not`, `InList`, function calls, etc.
        _ => None,
    }
}

fn match_ts_column_literal<'a>(
    ts_col_name: &str,
    left: &'a Expr,
    right: &'a Expr,
) -> Option<(Timestamp, bool)> {
    let (col, scalar, reverse) = match (left, right) {
        (Expr::Column(c), Expr::Literal(s, _)) => (c, s, false),
        (Expr::Literal(s, _), Expr::Column(c)) => (c, s, true),
        _ => return None,
    };
    if col.name != ts_col_name {
        return None;
    }
    // Reject string literals: their conversion needs a timezone we do not have,
    // and the existing extractor in `table::predicate` rejects them too.
    if is_string_timestamp_literal(scalar) {
        return None;
    }
    scalar_value_to_timestamp(scalar, None).map(|t| (t, reverse))
}

fn implied_from_cmp(
    ts_col_name: &str,
    ts_col_unit: TimeUnit,
    left: &Expr,
    op: Operator,
    right: &Expr,
) -> Option<TimestampRange> {
    let (ts, reverse) = match_ts_column_literal(ts_col_name, left, right)?;
    // Normalize to "column OP literal".
    let op = if reverse {
        match op {
            Operator::Lt => Operator::Gt,
            Operator::LtEq => Operator::GtEq,
            Operator::Gt => Operator::Lt,
            Operator::GtEq => Operator::LtEq,
            Operator::Eq => Operator::Eq,
            _ => return None,
        }
    } else {
        op
    };

    match op {
        Operator::GtEq => {
            // ts >= L. Round the lower bound up in the column unit.
            let b = ts.convert_to_ceil(ts_col_unit)?;
            Some(TimestampRange::from_start(b))
        }
        Operator::Gt => {
            // ts > L. floor(L) + 1 is the tight lower bound in the column unit.
            let v = ts.convert_to(ts_col_unit)?.value().checked_add(1)?;
            Some(TimestampRange::from_start(Timestamp::new(v, ts_col_unit)))
        }
        Operator::LtEq => {
            // ts <= L. Round the upper bound down in the column unit.
            let b = ts.convert_to(ts_col_unit)?;
            Some(TimestampRange::until_end(b, true))
        }
        Operator::Lt => {
            // ts < L. `ts < ceil(L)` is the tight bound: equal to `ts < L` when
            // L is exactly representable, and `ts <= floor(L)` otherwise.
            let b = ts.convert_to_ceil(ts_col_unit)?;
            Some(TimestampRange::until_end(b, false))
        }
        Operator::Eq => {
            // ts = L. Only provable when L is exactly representable.
            let f = ts.convert_to(ts_col_unit)?;
            let c = ts.convert_to_ceil(ts_col_unit)?;
            if f.value() != c.value() {
                return None;
            }
            Some(TimestampRange::single(f))
        }
        _ => None,
    }
}

fn implied_from_between(
    ts_col_name: &str,
    ts_col_unit: TimeUnit,
    expr: &Expr,
    low: &Expr,
    high: &Expr,
) -> Option<TimestampRange> {
    let Expr::Column(c) = expr else {
        return None;
    };
    if c.name != ts_col_name {
        return None;
    }
    let (low_s, high_s) = match (low, high) {
        (Expr::Literal(l, _), Expr::Literal(h, _)) => (l, h),
        _ => return None,
    };
    if is_string_timestamp_literal(low_s) || is_string_timestamp_literal(high_s) {
        return None;
    }
    let low_ts = scalar_value_to_timestamp(low_s, None)?;
    let high_ts = scalar_value_to_timestamp(high_s, None)?;
    // BETWEEN low AND high is equivalent to ts >= low AND ts <= high.
    let lo = low_ts.convert_to_ceil(ts_col_unit)?;
    let hi = high_ts.convert_to(ts_col_unit)?;
    Some(TimestampRange::new_inclusive(Some(lo), Some(hi)))
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

    // If the implied range covers this partition's `FileTimeRange`, drop
    // time-only predicates from the cache key so that queries with different
    // but equally-covering time bounds share an entry. `None` means some
    // time-only predicate had an unsupported shape (e.g. `OR`), so we keep
    // them in the key.
    let range_meta = &stream_ctx.ranges[part_range.identifier];
    let (file_min, file_max) = range_meta.time_range;
    let covers = match &stream_ctx.scan_implied_time_range {
        // An empty implied range can never cover a non-empty file range, so
        // short-circuit. We also skip the unit asserts because
        // `TimestampRange::empty()` uses `Timestamp::default()` (millisecond),
        // which would falsely trip the asserts for non-ms time index columns.
        Some(implied) if !implied.is_empty() => {
            // The `contains` check is sound only when `file_min`/`file_max`
            // share the implied range's unit (the time index column's unit).
            // Mito stores time index values in that unit; assert to catch any
            // future drift.
            if let Some(ts) = implied.start().as_ref().or(implied.end().as_ref()) {
                assert_eq!(file_min.unit(), ts.unit());
                assert_eq!(file_max.unit(), ts.unit());
            }
            implied.contains(&file_min) && implied.contains(&file_max)
        }
        _ => false,
    };
    let scan = if covers {
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
        result_tx: Option<oneshot::Sender<Result<Arc<RangeScanCacheValue>>>>,
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
        let _permit = limiter.acquire(input_size).await?;

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
    skip_threshold_bytes: usize,
) {
    let mut state = CacheConcatState::default();

    while let Some(cmd) = rx.recv().await {
        match cmd {
            CacheConcatCommand::Compact(batches) => {
                if let Err(err) = state.compact(batches, &limiter).await {
                    warn!(err; "Failed to compact range cache batches");
                    return;
                }
                // Close the channel to stop further work as soon as the cached
                // size exceeds the configured cache budget.
                if state.estimated_size > skip_threshold_bytes {
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
                let compact_result = state
                    .compact(pending, &limiter)
                    .await
                    .map(|()| state.finish());
                let result = match compact_result {
                    Ok(v) => {
                        let value = Arc::new(v);
                        part_metrics
                            .inc_range_cache_size(key.estimated_size() + value.estimated_size());
                        cache_strategy.put_range_result(key, value.clone());

                        Ok(value)
                    }
                    Err(e) => {
                        warn!(e; "Failed to finalize range cache batches");

                        Err(e)
                    }
                };

                if let Some(tx) = result_tx {
                    let _ = tx.send(result);
                }

                break;
            }
        }
    }
}

struct CacheBatchBuffer {
    buffered_batches: Vec<RecordBatch>,
    buffered_rows: usize,
    buffered_size: usize,
    sender: Option<mpsc::UnboundedSender<CacheConcatCommand>>,
}

impl CacheBatchBuffer {
    fn new(cache_strategy: &CacheStrategy) -> Self {
        let sender = cache_strategy.range_result_memory_limiter().map(|limiter| {
            let skip_threshold_bytes = cache_strategy.range_result_cache_size().unwrap_or(0);
            let (tx, rx) = mpsc::unbounded_channel();
            common_runtime::spawn_datanode_query(run_cache_concat_task(
                rx,
                limiter.clone(),
                skip_threshold_bytes,
            ));
            tx
        });

        Self {
            buffered_batches: Vec::new(),
            buffered_rows: 0,
            buffered_size: 0,
            sender,
        }
    }

    fn push(&mut self, batch: RecordBatch) -> Result<()> {
        if self.sender.is_none() {
            return Ok(());
        }

        self.buffered_rows += batch.num_rows();
        self.buffered_size += batch.get_array_memory_size();
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
        result_tx: Option<oneshot::Sender<Result<Arc<RangeScanCacheValue>>>>,
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

/// Wraps a stream to cache its output for future range cache hits.
pub(crate) fn cache_flat_range_stream(
    mut stream: BoxedRecordBatchStream,
    cache_strategy: CacheStrategy,
    key: RangeScanCacheKey,
    part_metrics: PartitionMetrics,
) -> BoxedRecordBatchStream {
    Box::pin(try_stream! {
        let mut buffer = CacheBatchBuffer::new(&cache_strategy);
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
        read_columns: ReadColumns::from_deduped_column_ids(std::iter::empty()),
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
    use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
    use datafusion_common::ScalarValue;
    use datafusion_expr::{Expr, col, lit};
    use smallvec::smallvec;
    use store_api::storage::{FileId, RegionId};

    use super::*;
    use crate::cache::CacheManager;
    use crate::read::flat_projection::FlatProjectionMapper;
    use crate::read::range::{RangeMeta, RowGroupIndex, SourceIndex};
    use crate::read::scan_region::{PredicateGroup, ScanInput};
    use crate::sst::file::FileTimeRange;
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

    fn test_scan_fingerprint(
        filters: Vec<String>,
        time_filters: Vec<String>,
        series_row_selector: Option<TimeSeriesRowSelector>,
        filter_deleted: bool,
        partition_expr_version: u64,
    ) -> ScanRequestFingerprint {
        let read_columns = ReadColumns::from_deduped_column_ids([1, 2]);
        ScanRequestFingerprintBuilder {
            read_columns,
            read_column_types: vec![None, None],
            filters,
            time_filters,
            series_row_selector,
            append_mode: false,
            filter_deleted,
            merge_mode: MergeMode::LastRow,
            partition_expr_version,
        }
        .build()
    }

    fn test_cache_context(strategy: &CacheStrategy) -> (RangeScanCacheKey, PartitionMetrics) {
        let region_id = RegionId::new(1, 1);
        let key = RangeScanCacheKey {
            region_id,
            row_groups: vec![],
            scan: test_scan_fingerprint(vec![], vec![], None, false, 0),
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
    ) -> Result<Arc<RangeScanCacheValue>> {
        let (tx, rx) = oneshot::channel();
        common_telemetry::info!("finish start");
        buffer.finish(key, cache_strategy, part_metrics, Some(tx));
        common_telemetry::info!("finish end");
        rx.await.context(crate::error::RecvSnafu)?
    }

    async fn new_stream_context(
        filters: Vec<Expr>,
        query_time_range: Option<TimestampRange>,
        partition_time_range: FileTimeRange,
    ) -> (StreamContext, PartitionRange) {
        let env = SchedulerEnv::new().await;
        let metadata = Arc::new(metadata_with_primary_key(vec![0, 1], false));
        let mapper = FlatProjectionMapper::new(&metadata, [0, 2, 3]).unwrap();
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
        let (scan_fingerprint, scan_implied_time_range) =
            match crate::read::scan_region::build_scan_fingerprint(&input) {
                Some(b) => (Some(b.fingerprint), b.implied_time_range),
                None => (None, None),
            };
        let stream_ctx = StreamContext {
            input,
            ranges: vec![range_meta],
            scan_fingerprint,
            scan_implied_time_range,
            query_start: Instant::now(),
        };

        (stream_ctx, partition_range)
    }

    /// Helper to create a timestamp millisecond literal.
    fn ts_lit(val: i64) -> Expr {
        lit(ScalarValue::TimestampMillisecond(Some(val), None))
    }

    fn normalized_exprs(exprs: impl IntoIterator<Item = Expr>) -> Vec<String> {
        let mut exprs = exprs
            .into_iter()
            .map(|expr| expr.to_string())
            .collect::<Vec<_>>();
        exprs.sort_unstable();
        exprs
    }

    async fn assert_range_cache_filters(
        filters: Vec<Expr>,
        query_time_range: Option<TimestampRange>,
        partition_time_range: FileTimeRange,
        expected_filters: Vec<Expr>,
        expected_time_filters: Vec<Expr>,
    ) {
        let (stream_ctx, part_range) =
            new_stream_context(filters, query_time_range, partition_time_range).await;

        let key = build_range_cache_key(&stream_ctx, &part_range).unwrap();

        assert_eq!(
            key.scan.filters(),
            normalized_exprs(expected_filters).as_slice()
        );
        assert_eq!(
            key.scan.time_filters(),
            normalized_exprs(expected_time_filters).as_slice()
        );
    }

    #[tokio::test]
    async fn range_cache_time_filter_key_cases() {
        let partition = (
            Timestamp::new_millisecond(1000),
            Timestamp::new_millisecond(2000),
        );

        struct Case {
            filters: Vec<Expr>,
            query_time_range: Option<TimestampRange>,
            expected_filters: Vec<Expr>,
            expected_time_filters: Vec<Expr>,
        }

        // Time filters are stripped only when their implied range fully covers
        // the partition's file-time range. `is_not_null(ts)` stays in regular
        // filters because it is not routed into `time_filters`.
        for case in [
            Case {
                filters: vec![
                    col("ts").gt_eq(ts_lit(1000)),
                    col("ts").lt(ts_lit(2001)),
                    col("ts").is_not_null(),
                    col("k0").eq(lit("foo")),
                ],
                query_time_range: TimestampRange::with_unit(1000, 2002, TimeUnit::Millisecond),
                expected_filters: vec![col("k0").eq(lit("foo")), col("ts").is_not_null()],
                expected_time_filters: vec![],
            },
            Case {
                filters: vec![
                    col("ts").gt_eq(ts_lit(500)),
                    col("ts").lt(ts_lit(3000)),
                    col("k0").eq(lit("foo")),
                ],
                query_time_range: TimestampRange::with_unit(500, 3000, TimeUnit::Millisecond),
                expected_filters: vec![col("k0").eq(lit("foo"))],
                expected_time_filters: vec![],
            },
            Case {
                filters: vec![
                    col("ts").gt_eq(ts_lit(1000)),
                    col("ts").lt_eq(ts_lit(2000)),
                    col("k0").eq(lit("foo")),
                ],
                query_time_range: TimestampRange::with_unit(1000, 2001, TimeUnit::Millisecond),
                expected_filters: vec![col("k0").eq(lit("foo"))],
                expected_time_filters: vec![],
            },
            Case {
                filters: vec![
                    col("ts").between(ts_lit(1000), ts_lit(2000)),
                    col("k0").eq(lit("foo")),
                ],
                query_time_range: TimestampRange::with_unit(1000, 2001, TimeUnit::Millisecond),
                expected_filters: vec![col("k0").eq(lit("foo"))],
                expected_time_filters: vec![],
            },
            Case {
                filters: vec![col("ts").gt_eq(ts_lit(1200)), col("k0").eq(lit("foo"))],
                query_time_range: TimestampRange::with_unit(1200, 2001, TimeUnit::Millisecond),
                expected_filters: vec![col("k0").eq(lit("foo"))],
                expected_time_filters: vec![col("ts").gt_eq(ts_lit(1200))],
            },
            Case {
                filters: vec![
                    col("ts").gt_eq(ts_lit(1500)),
                    col("ts").is_not_null(),
                    col("k0").eq(lit("foo")),
                ],
                query_time_range: None,
                expected_filters: vec![col("k0").eq(lit("foo")), col("ts").is_not_null()],
                expected_time_filters: vec![col("ts").gt_eq(ts_lit(1500))],
            },
        ] {
            assert_range_cache_filters(
                case.filters,
                case.query_time_range,
                partition,
                case.expected_filters,
                case.expected_time_filters,
            )
            .await;
        }
    }

    #[tokio::test]
    async fn two_distinct_queries_share_cache_key_when_both_cover() {
        let partition_range = (
            Timestamp::new_millisecond(1000),
            Timestamp::new_millisecond(2000),
        );

        let (ctx_a, part_a) = new_stream_context(
            vec![
                col("ts").gt_eq(ts_lit(500)),
                col("ts").lt(ts_lit(3000)),
                col("k0").eq(lit("foo")),
            ],
            TimestampRange::with_unit(500, 3000, TimeUnit::Millisecond),
            partition_range,
        )
        .await;
        let (ctx_b, part_b) = new_stream_context(
            vec![
                col("ts").gt_eq(ts_lit(100)),
                col("ts").lt(ts_lit(5000)),
                col("k0").eq(lit("foo")),
            ],
            TimestampRange::with_unit(100, 5000, TimeUnit::Millisecond),
            partition_range,
        )
        .await;

        let key_a = build_range_cache_key(&ctx_a, &part_a).unwrap();
        let key_b = build_range_cache_key(&ctx_b, &part_b).unwrap();
        assert_eq!(key_a.scan, key_b.scan);
        assert!(key_a.scan.time_filters().is_empty());
    }

    #[tokio::test]
    async fn disables_optimization_on_or_clause() {
        let partition_range = (
            Timestamp::new_millisecond(1000),
            Timestamp::new_millisecond(2000),
        );

        let or_a = col("ts").gt_eq(ts_lit(1000)).or(col("ts").lt(ts_lit(500)));
        let or_b = col("ts").gt_eq(ts_lit(900)).or(col("ts").lt(ts_lit(400)));

        let (ctx_a, part_a) = new_stream_context(
            vec![or_a.clone(), col("k0").eq(lit("foo"))],
            None,
            partition_range,
        )
        .await;
        let (ctx_b, part_b) = new_stream_context(
            vec![or_b.clone(), col("k0").eq(lit("foo"))],
            None,
            partition_range,
        )
        .await;

        assert!(ctx_a.scan_implied_time_range.is_none());
        let key_a = build_range_cache_key(&ctx_a, &part_a).unwrap();
        let key_b = build_range_cache_key(&ctx_b, &part_b).unwrap();
        assert_ne!(key_a.scan, key_b.scan);
        assert_eq!(
            key_a.scan.time_filters(),
            normalized_exprs([or_a]).as_slice()
        );
    }

    #[tokio::test]
    async fn empty_implied_range_does_not_panic_on_non_ms_file_range() {
        // Contradictory time predicates make the implied range empty. The
        // empty range's sentinel timestamps use `Timestamp::default()` (ms),
        // so without the `is_empty()` short-circuit the unit asserts would
        // panic against a non-ms `range_meta.time_range`.
        let partition = (
            Timestamp::new_millisecond(1000),
            Timestamp::new_millisecond(2000),
        );

        let (mut ctx, part_range) = new_stream_context(
            vec![col("ts").gt_eq(ts_lit(1500)), col("k0").eq(lit("foo"))],
            TimestampRange::with_unit(1500, 3000, TimeUnit::Millisecond),
            partition,
        )
        .await;

        ctx.scan_implied_time_range = Some(TimestampRange::empty());
        ctx.ranges[0].time_range = (
            Timestamp::new(1_000_000_000, TimeUnit::Nanosecond),
            Timestamp::new(2_000_000_000, TimeUnit::Nanosecond),
        );

        let key = build_range_cache_key(&ctx, &part_range).unwrap();
        // Empty implied range cannot cover, so time filters stay in the key.
        assert!(!key.scan.time_filters().is_empty());
    }

    fn ms_ts(v: i64) -> Timestamp {
        Timestamp::new_millisecond(v)
    }

    fn implied_ms(expr: Expr) -> Option<TimestampRange> {
        implied_time_range_from_exprs("ts", TimeUnit::Millisecond, &[&expr])
    }

    #[test]
    fn implied_time_range_supported_exprs() {
        for (expr, expected) in [
            (
                col("ts").gt_eq(ts_lit(1000)),
                Some(TimestampRange::from_start(ms_ts(1000))),
            ),
            (
                col("ts").gt(ts_lit(1000)),
                Some(TimestampRange::from_start(ms_ts(1001))),
            ),
            (
                col("ts").lt_eq(ts_lit(2000)),
                Some(TimestampRange::until_end(ms_ts(2000), true)),
            ),
            (
                col("ts").lt(ts_lit(2000)),
                Some(TimestampRange::until_end(ms_ts(2000), false)),
            ),
            (
                col("ts").eq(ts_lit(1500)),
                Some(TimestampRange::single(ms_ts(1500))),
            ),
            (
                ts_lit(1000).lt_eq(col("ts")),
                Some(TimestampRange::from_start(ms_ts(1000))),
            ),
            (
                col("ts").between(ts_lit(1000), ts_lit(2000)),
                Some(TimestampRange::new_inclusive(
                    Some(ms_ts(1000)),
                    Some(ms_ts(2000)),
                )),
            ),
            (
                col("ts")
                    .gt_eq(ts_lit(1000))
                    .and(col("ts").lt(ts_lit(2000))),
                TimestampRange::with_unit(1000, 2000, TimeUnit::Millisecond),
            ),
            (
                col("ts")
                    .gt_eq(ts_lit(1000))
                    .and(col("ts").lt(ts_lit(5000)))
                    .and(col("ts").lt_eq(ts_lit(3000))),
                TimestampRange::with_unit(1000, 3001, TimeUnit::Millisecond),
            ),
        ] {
            assert_eq!(implied_ms(expr), expected);
        }

        assert_eq!(
            implied_time_range_from_exprs("ts", TimeUnit::Millisecond, &[]),
            Some(TimestampRange::min_to_max())
        );
    }

    #[test]
    fn implied_time_range_unsupported_exprs() {
        let not_between = Expr::Between(Between {
            expr: Box::new(col("ts")),
            negated: true,
            low: Box::new(ts_lit(1000)),
            high: Box::new(ts_lit(2000)),
        });

        for expr in [
            not_between,
            col("ts").gt_eq(ts_lit(1000)).or(col("ts").lt(ts_lit(500))),
            Expr::Not(Box::new(col("ts").gt_eq(ts_lit(1000)))),
            col("ts").in_list(vec![ts_lit(1000), ts_lit(2000)], false),
            col("ts").gt_eq(col("other")),
            col("other_ts").gt_eq(ts_lit(1000)),
        ] {
            assert!(implied_ms(expr).is_none());
        }
    }

    #[test]
    fn implied_time_range_unit_conversion() {
        let second_1 = lit(ScalarValue::TimestampSecond(Some(1), None));
        let ns_1500 = lit(ScalarValue::TimestampNanosecond(Some(1_500_000_000), None));
        let ns_1500_5 = lit(ScalarValue::TimestampNanosecond(Some(1_500_500_000), None));

        for (expr, expected) in [
            (
                col("ts").gt_eq(second_1.clone()),
                Some(TimestampRange::from_start(ms_ts(1000))),
            ),
            (
                col("ts").lt_eq(second_1),
                Some(TimestampRange::until_end(ms_ts(1000), true)),
            ),
            (
                col("ts").eq(ns_1500),
                Some(TimestampRange::single(ms_ts(1500))),
            ),
            (col("ts").eq(ns_1500_5.clone()), None),
            (
                col("ts").gt_eq(ns_1500_5.clone()),
                Some(TimestampRange::from_start(ms_ts(1501))),
            ),
            (
                col("ts").lt_eq(ns_1500_5.clone()),
                Some(TimestampRange::until_end(ms_ts(1500), true)),
            ),
            (
                col("ts").gt(ns_1500_5.clone()),
                Some(TimestampRange::from_start(ms_ts(1501))),
            ),
            (
                col("ts").lt(ns_1500_5),
                Some(TimestampRange::until_end(ms_ts(1501), false)),
            ),
        ] {
            assert_eq!(implied_ms(expr), expected);
        }
    }

    #[test]
    fn normalizes_and_clears_time_filters() {
        let normalized =
            test_scan_fingerprint(vec!["k0 = 'foo'".to_string()], vec![], None, true, 0);

        assert!(normalized.time_filters().is_empty());

        let fingerprint = test_scan_fingerprint(
            vec!["k0 = 'foo'".to_string()],
            vec!["ts >= 1000".to_string()],
            Some(TimeSeriesRowSelector::LastRow),
            true,
            7,
        );

        let reset = fingerprint.without_time_filters();

        assert_eq!(reset.read_columns(), fingerprint.read_columns());
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

        let mut buffer = CacheBatchBuffer::new(&strategy);
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

        let mut buffer = CacheBatchBuffer::new(&strategy);
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

        let mut buffer = CacheBatchBuffer::new(&strategy);
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

    #[tokio::test]
    async fn cache_batch_buffer_skips_cache_when_compacted_size_exceeds_limit() {
        let large_batch = make_large_binary_batch(DEFAULT_READ_BATCH_SIZE / 2 + 1, 4096);
        // Budget only fits two large batches.
        let budget = (large_batch.get_array_memory_size() as u64) * 2 + 1;
        let strategy = CacheStrategy::EnableAll(Arc::new(
            CacheManager::builder()
                .range_result_cache_size(budget)
                .build(),
        ));
        let (key, part_metrics) = test_cache_context(&strategy);

        let mut buffer = CacheBatchBuffer::new(&strategy);
        for _ in 0..4 {
            buffer.push(large_batch.clone()).unwrap();
        }
        assert!(
            finish_cache_batch_buffer(buffer, key.clone(), strategy.clone(), part_metrics)
                .await
                .is_err()
        );
        assert!(strategy.get_range_result(&key).is_none());
    }
}
