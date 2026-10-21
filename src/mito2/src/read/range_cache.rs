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

pub(crate) mod shared_range;

use std::future::Future;
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
#[cfg(any(test, feature = "test"))]
use futures::TryStreamExt;
use snafu::{IntoError, ResultExt};
use store_api::region_engine::PartitionRange;
use store_api::storage::{FileId, RegionId, SequenceRange, TimeSeriesRowSelector};
use table::predicate::is_string_timestamp_literal;

use crate::cache::CacheStrategy;
use crate::error::{ComputeArrowSnafu, Result};
use crate::read::BoxedRecordBatchStream;
use crate::read::pruner::PartitionPruner;
use crate::read::range_cache::shared_range::{RangeRead, SharedRange};
use crate::read::read_columns::ReadColumns;
use crate::read::scan_region::StreamContext;
use crate::read::scan_util::PartitionMetrics;
use crate::read::series_reader::SeriesRange;
use crate::region::options::MergeMode;
use crate::sst::parquet::DEFAULT_READ_BATCH_SIZE;
use crate::sst::parquet::file_range::PreFilterMode;

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
    /// Exact sequence range applied row-level on SST reads, or `None` for scans
    /// without row-level sequence filtering. Kept in the cache key so filtered
    /// range results are never reused for scans with a different (or no) range.
    sequence_range: Option<SequenceRange>,
    stage: RangeScanStage,
    /// File interpretation and snapshot guards, aligned with the key's row groups.
    file_reads: Option<Arc<Vec<FileReadFingerprint>>>,
    /// We keep the partition expr version to ensure we won't reuse the fingerprint after we change the partition expr.
    /// We store the version instead of the whole partition expr or partition expr filters.
    partition_expr_version: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum RangeScanStage {
    Data,
    CandidateSeries,
    SeriesData(SeriesRange),
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
    pub(crate) sequence_range: Option<SequenceRange>,
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
            sequence_range,
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
            sequence_range,
            stage: RangeScanStage::Data,
            file_reads: None,
            partition_expr_version,
        }
    }
}

/// Inputs affecting a file read's output.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct FileReadFingerprint {
    target_schema_version: u64,
    source_region: RegionId,
    file_sequence: Option<u64>,
    preserve_row_sequence: bool,
    snapshot_sequence: Option<u64>,
    pre_filter_mode: PreFilterMode,
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
            sequence_range: self.sequence_range,
            file_reads: self.file_reads.clone(),
            stage: self.stage,
            partition_expr_version: self.partition_expr_version,
        }
    }

    fn for_candidate_series(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            time_filters: self.time_filters.clone(),
            series_row_selector: self.series_row_selector,
            append_mode: self.append_mode,
            filter_deleted: self.filter_deleted,
            merge_mode: self.merge_mode,
            sequence_range: self.sequence_range,
            file_reads: self.file_reads.clone(),
            stage: RangeScanStage::CandidateSeries,
            partition_expr_version: self.partition_expr_version,
        }
    }

    fn for_series_data(&self, range: SeriesRange) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            time_filters: self.time_filters.clone(),
            series_row_selector: self.series_row_selector,
            append_mode: self.append_mode,
            filter_deleted: self.filter_deleted,
            merge_mode: self.merge_mode,
            sequence_range: self.sequence_range,
            file_reads: self.file_reads.clone(),
            stage: RangeScanStage::SeriesData(range),
            partition_expr_version: self.partition_expr_version,
        }
    }

    pub(crate) fn estimated_size(&self) -> usize {
        mem::size_of::<SharedScanRequestFingerprint>()
            + self.file_reads.as_ref().map_or(0, |files| {
                mem::size_of::<Vec<FileReadFingerprint>>()
                    + files.capacity() * mem::size_of::<FileReadFingerprint>()
            })
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
    /// Sorted file/row-group identities for merged results.
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
    estimated_batch_size: usize,
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
            estimated_batch_size: DEFAULT_READ_BATCH_SIZE,
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

/// Returns the timestamp range where all time-only predicates are guaranteed true.
///
/// Returns `None` for empty input because there is no time-filter implication
/// that cache-key normalization or prefilter postponement can use. It also
/// returns `None` if any expression contains an unsupported shape: `OR`, `NOT`,
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
    if exprs.is_empty() {
        return None;
    }

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
    partition_pruner: Option<&PartitionPruner>,
) -> Option<RangeScanCacheKey> {
    build_range_cache_key_inner(stream_ctx, part_range, None, partition_pruner)
}

/// Builds a cache key for a candidate-series partition-range result.
pub(crate) fn build_candidate_range_cache_key(
    stream_ctx: &StreamContext,
    part_range: &PartitionRange,
    partition_pruner: &PartitionPruner,
) -> Option<RangeScanCacheKey> {
    build_range_cache_key_inner(
        stream_ctx,
        part_range,
        Some(RangeScanStage::CandidateSeries),
        Some(partition_pruner),
    )
}

/// Builds a cache key for a two-phase series-data partition-range result.
pub(crate) fn build_series_range_cache_key(
    stream_ctx: &StreamContext,
    part_range: &PartitionRange,
    range: SeriesRange,
    partition_pruner: &PartitionPruner,
) -> Option<RangeScanCacheKey> {
    build_range_cache_key_inner(
        stream_ctx,
        part_range,
        Some(RangeScanStage::SeriesData(range)),
        Some(partition_pruner),
    )
}

fn build_range_cache_key_inner(
    stream_ctx: &StreamContext,
    part_range: &PartitionRange,
    stage: Option<RangeScanStage>,
    partition_pruner: Option<&PartitionPruner>,
) -> Option<RangeScanCacheKey> {
    if !stream_ctx.input.cache_strategy.has_range_result_cache() {
        return None;
    }

    let fingerprint = stream_ctx.input.scan_fingerprint()?;

    // Dyn filters can change at runtime, so we can't cache when they're present.
    let has_dyn_filters = stream_ctx
        .input
        .predicate_group()
        .predicate_without_region()
        .is_some_and(|p| !p.dyn_filters().is_empty());
    if has_dyn_filters {
        return None;
    }

    let range_meta = &stream_ctx.ranges[part_range.identifier];
    let mut files = Vec::with_capacity(range_meta.row_group_indices.len());
    for index in &range_meta.row_group_indices {
        if !stream_ctx.is_file_range_index(*index) {
            return None;
        }
        let handle = stream_ctx.input.file_from_index(*index);
        let file = handle.meta_ref();
        files.push((
            (handle.file_id().file_id(), index.row_group_index),
            FileReadFingerprint {
                target_schema_version: stream_ctx.input.region_metadata().schema_version,
                source_region: handle.region_id(),
                file_sequence: file.sequence.map(|sequence| sequence.get()),
                preserve_row_sequence: file.preserve_row_sequence,
                snapshot_sequence: stream_ctx.input.snapshot_sequence,
                pre_filter_mode: partition_pruner.map_or_else(
                    || stream_ctx.range_pre_filter_mode(part_range),
                    |pruner| pruner.pre_filter_mode(index.index - stream_ctx.input.num_memtables()),
                ),
            },
        ));
    }
    if files.is_empty() {
        return None;
    }
    files.sort_unstable_by(|a, b| {
        a.0.0
            .as_bytes()
            .cmp(b.0.0.as_bytes())
            .then(a.0.1.cmp(&b.0.1))
    });
    let (row_groups, file_reads): (Vec<_>, Vec<_>) = files.into_iter().unzip();

    // If the implied range covers this partition's `FileTimeRange`, drop
    // time-only predicates from the cache key so that queries with different
    // but equally-covering time bounds share an entry. `None` means there is no
    // analyzable time-only predicate or some predicate had an unsupported shape
    // (e.g. `OR`), so we keep the fingerprint unchanged. When `time_filters` is
    // already empty, cloning the fingerprint is equivalent to stripping them.
    let (file_min, file_max) = range_meta.time_range;
    let covers = match stream_ctx.input.implied_time_range() {
        // An empty implied range can never cover a non-empty file range, so
        // short-circuit.
        Some(implied) if !implied.is_empty() => {
            // `file_min`/`file_max` may use a different unit than the implied
            // range: after the time index unit was widened, pre-alter files
            // still carry the old unit while the implied range uses the
            // current column unit. `Timestamp` comparisons normalize units,
            // so `contains` compares instants and stays sound across units.
            implied.contains(&file_min) && implied.contains(&file_max)
        }
        _ => false,
    };
    let scan = if covers {
        fingerprint.without_time_filters()
    } else {
        fingerprint.clone()
    };
    let mut scan = match stage {
        Some(RangeScanStage::CandidateSeries) => scan.for_candidate_series(),
        Some(RangeScanStage::SeriesData(assignment)) => scan.for_series_data(assignment),
        Some(RangeScanStage::Data) | None => scan,
    };

    scan.file_reads = Some(Arc::new(file_reads));
    Some(RangeScanCacheKey {
        region_id: stream_ctx.input.region_metadata().region_id,
        row_groups,
        scan,
    })
}

/// Joins an active scan while retaining independently advancing stream cursors.
#[tracing::instrument(name = "range_result_lookup", skip_all)]
pub(crate) async fn get_or_load_range_result<F, Fut>(
    strategy: &CacheStrategy,
    key: RangeScanCacheKey,
    part_metrics: &PartitionMetrics,
    build: F,
) -> Result<(BoxedRecordBatchStream, usize)>
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = Result<(BoxedRecordBatchStream, usize)>>,
{
    get_or_load_range_result_in_scope(strategy, key, part_metrics, 0, build).await
}

/// Isolates active sources that reserve memory from a query-specific pool.
/// Pool identity affects active sharing only, never completed-result reuse.
pub(crate) async fn get_or_load_range_result_in_scope<F, Fut>(
    strategy: &CacheStrategy,
    key: RangeScanCacheKey,
    part_metrics: &PartitionMetrics,
    memory_pool_id: usize,
    build: F,
) -> Result<(BoxedRecordBatchStream, usize)>
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = Result<(BoxedRecordBatchStream, usize)>>,
{
    if let Some(value) = strategy.get_range_result(&key) {
        part_metrics.inc_range_cache_hit();
        crate::metrics::RANGE_CACHE_EVENTS
            .with_label_values(&["hit"])
            .inc();
        record_result_reuse(&value);
        let batch_size = value.estimated_batch_size;
        return Ok((cached_flat_range_stream(value), batch_size));
    }
    part_metrics.inc_range_cache_miss();
    crate::metrics::RANGE_CACHE_EVENTS
        .with_label_values(&["miss"])
        .inc();
    let (Some(cache), Some(limiter)) = (
        strategy.range_result_cache(),
        strategy.range_result_load_limiter(),
    ) else {
        return build().await;
    };
    let cache = cache.clone();
    let load = cache.get_or_create_range_load(&key, memory_pool_id);
    let mut build = Some(build);
    let mut owner_cursor = None;
    let (shared, batch_size) = load
        .result
        .get_or_try_init(|| async {
            // A previous producer may have published between lookup and registration.
            let (source, batch_size) = if let Some(value) = cache.get(&key) {
                let batch_size = value.estimated_batch_size;
                (cached_flat_range_stream(value), batch_size)
            } else {
                let builder = build.take().ok_or_else(|| {
                    crate::error::UnexpectedSnafu {
                        reason: "range scan builder already consumed",
                    }
                    .build()
                })?;
                let source = Box::pin(builder()).await?;
                crate::metrics::RANGE_CACHE_EVENTS
                    .with_label_values(&["built_sources"])
                    .inc();
                source
            };
            let shared = SharedRange::new(source, limiter.clone());
            owner_cursor = shared.subscribe().await;
            Ok::<_, crate::error::Error>((shared, batch_size))
        })
        .await?;
    let shared = shared.clone();
    let batch_size = *batch_size;
    let cursor = match owner_cursor {
        Some(cursor) => Some(cursor),
        None => {
            let cursor = shared.subscribe().await;
            if cursor.is_some() {
                crate::metrics::RANGE_CACHE_EVENTS
                    .with_label_values(&["reused_sources"])
                    .inc();
            }
            cursor
        }
    };
    let Some(mut cursor) = cursor else {
        // An oversized range has closed admission. This caller has not emitted
        // any rows, so its original ordinary stream remains a safe fallback.
        let builder = build.ok_or_else(|| {
            crate::error::UnexpectedSnafu {
                reason: "range scan owner lost its initial cursor",
            }
            .build()
        })?;
        return builder().await;
    };
    let strategy = strategy.clone();
    let metrics = part_metrics.clone();
    let stream = Box::pin(try_stream! {
        // Keep the weak registry entry alive until this reader finishes.
        let load = load;
        loop {
            match shared.read(&mut cursor).await {
                RangeRead::Batch(batch) => yield batch,
                RangeRead::Failed(error) => {
                    Err(crate::error::LoadRangeResultSnafu.into_error(error))?;
                }
                RangeRead::Finished => {
                    if let Some(batches) = shared.take_complete_batches().await {
                        // Finalization owns the registration until publication, so
                        // a new caller can still join during background compaction.
                        common_runtime::spawn_query(async move {
                            let _load = load;
                            let bytes = batches.iter().map(RecordBatch::get_array_memory_size).sum();
                            let batches = batches.into_iter().map(|batch| {
                                let rows = batch.num_rows();
                                CachedBatchSlice { batch, slice_lengths: vec![rows] }
                            }).collect();
                            let mut value = RangeScanCacheValue::new(batches, bytes);
                            if let Some(limiter) = strategy.range_result_memory_limiter() {
                                let mut compacted = CacheConcatState::default();
                                match compacted.compact_range(&value, limiter).await {
                                    Ok(()) => value = compacted.finish(),
                                    Err(error) => {
                                        warn!(error; "Failed to compact shared range result");
                                        return;
                                    }
                                }
                            }
                            value.estimated_batch_size = batch_size;
                            metrics.inc_range_cache_size(key.estimated_size() + value.estimated_size());
                            cache.insert(key, Arc::new(value));
                        });
                    }
                    break;
                }
            }
        }
    });
    Ok((stream, batch_size))
}

/// Reused output is separate from physical scan/decode counters.
fn record_result_reuse(value: &RangeScanCacheValue) {
    let rows: usize = value
        .cached_batches
        .iter()
        .map(|batch| batch.batch.num_rows())
        .sum();
    for (event, count) in [
        ("reused_sources", 1),
        ("reused_rows", rows),
        ("reused_batches", value.cached_batches.len()),
        ("reused_reference_bytes", value.estimated_batches_size),
    ] {
        crate::metrics::RANGE_CACHE_EVENTS
            .with_label_values(&[event])
            .inc_by(count as u64);
    }
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

    async fn compact_range(
        &mut self,
        value: &RangeScanCacheValue,
        limiter: &crate::cache::RangeResultMemoryLimiter,
    ) -> Result<()> {
        let mut pending = Vec::new();
        let mut rows = 0;
        let mut bytes = 0;
        for cached in &value.cached_batches {
            rows += cached.batch.num_rows();
            bytes += cached.batch.get_array_memory_size();
            pending.push(cached.batch.clone());
            if should_compact_batches(pending.len(), rows, bytes) {
                self.compact(mem::take(&mut pending), limiter).await?;
                rows = 0;
                bytes = 0;
            }
        }
        self.compact(pending, limiter).await
    }

    fn finish(self) -> RangeScanCacheValue {
        RangeScanCacheValue::new(self.cached_batches, self.estimated_size)
    }
}

fn should_compact_batches(batches: usize, rows: usize, bytes: usize) -> bool {
    batches > 1 && (rows > DEFAULT_READ_BATCH_SIZE || bytes > RANGE_CACHE_COMPACT_THRESHOLD_BYTES)
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

/// Benchmarks the production range-result loader with a fresh cache per call.
///
/// A zero capacity disables caching. A nonzero capacity measures cold construction,
/// including staging and consumption; background compaction may outlive the stream.
/// It does not measure warm reuse or concurrent load sharing.
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
        read_columns: ReadColumns::new(std::iter::empty()),
        read_column_types: vec![],
        filters: vec![],
        time_filters: vec![],
        series_row_selector: None,
        append_mode: false,
        filter_deleted: false,
        merge_mode: MergeMode::LastRow,
        sequence_range: None,
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

    Box::pin(try_stream! {
        let (mut stream, _) = get_or_load_range_result(
            &cache_strategy,
            key,
            &part_metrics,
            || async { Ok((stream, DEFAULT_READ_BATCH_SIZE)) },
        ).await?;
        while let Some(batch) = stream.try_next().await? {
            yield batch;
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
    use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
    use datafusion_common::ScalarValue;
    use datafusion_expr::{Expr, col, lit};
    use smallvec::smallvec;
    use store_api::storage::{FileId, RegionId};

    use super::*;
    use crate::cache::CacheManager;
    use crate::read::flat_projection::FlatProjectionMapper;
    use crate::read::pruner::Pruner;
    use crate::read::range::{RangeMeta, RowGroupIndex, SourceIndex};
    use crate::read::scan_region::{PredicateGroup, ScanInput, StreamContext};
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
        let read_columns = ReadColumns::new([1, 2]);
        ScanRequestFingerprintBuilder {
            read_columns,
            read_column_types: vec![None, None],
            filters,
            time_filters,
            series_row_selector,
            append_mode: false,
            filter_deleted,
            merge_mode: MergeMode::LastRow,
            sequence_range: None,
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
        let input = ScanInput::builder(env.access_layer.clone(), mapper)
            .with_predicate(predicate)
            .with_time_range(query_time_range)
            .with_files(vec![file])
            .with_cache(test_cache_strategy())
            .build();
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
        let stream_ctx = StreamContext {
            input,
            ranges: vec![range_meta],
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

        let key = build_range_cache_key(&stream_ctx, &part_range, None).unwrap();

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

        let key_a = build_range_cache_key(&ctx_a, &part_a, None).unwrap();
        let key_b = build_range_cache_key(&ctx_b, &part_b, None).unwrap();
        assert_eq!(key_a.scan, key_b.scan);
        assert!(key_a.scan.time_filters().is_empty());
    }

    #[tokio::test]
    async fn test_scan_stages_have_separate_cache_keys() {
        let partition_range = (
            Timestamp::new_millisecond(1000),
            Timestamp::new_millisecond(2000),
        );
        let (ctx, part_range) =
            new_stream_context(vec![col("k0").eq(lit("foo"))], None, partition_range).await;

        let ctx = Arc::new(ctx);
        let pruner = Arc::new(crate::read::pruner::Pruner::new(ctx.clone(), 0));
        let partition = PartitionPruner::new(pruner, std::slice::from_ref(&part_range));
        let data_key = build_range_cache_key(&ctx, &part_range, None).unwrap();
        let candidate_key = build_candidate_range_cache_key(&ctx, &part_range, &partition).unwrap();
        let range_0 = SeriesRange::new(0, 2).unwrap();
        let range_1 = SeriesRange::new(1, 2).unwrap();
        let series_key_0 =
            build_series_range_cache_key(&ctx, &part_range, range_0, &partition).unwrap();
        let series_key_1 =
            build_series_range_cache_key(&ctx, &part_range, range_1, &partition).unwrap();

        assert_ne!(data_key.scan, candidate_key.scan);
        assert_ne!(candidate_key.scan, series_key_0.scan);
        assert_eq!(data_key.row_groups, candidate_key.row_groups);
        assert_ne!(data_key.scan, series_key_0.scan);
        assert_ne!(series_key_0.scan, series_key_1.scan);
        assert_eq!(data_key.row_groups, series_key_0.row_groups);
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

        assert!(ctx_a.input.implied_time_range().is_none());
        let key_a = build_range_cache_key(&ctx_a, &part_a, None).unwrap();
        let key_b = build_range_cache_key(&ctx_b, &part_b, None).unwrap();
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
            vec![
                col("ts").gt_eq(ts_lit(1500)),
                col("ts").lt(ts_lit(1500)),
                col("k0").eq(lit("foo")),
            ],
            TimestampRange::with_unit(1500, 3000, TimeUnit::Millisecond),
            partition,
        )
        .await;

        assert!(ctx.input.implied_time_range().unwrap().is_empty());
        ctx.ranges[0].time_range = (
            Timestamp::new(1_000_000_000, TimeUnit::Nanosecond),
            Timestamp::new(2_000_000_000, TimeUnit::Nanosecond),
        );

        let key = build_range_cache_key(&ctx, &part_range, None).unwrap();
        // Empty implied range cannot cover, so time filters stay in the key.
        assert!(!key.scan.time_filters().is_empty());
    }

    #[tokio::test]
    async fn mixed_unit_file_range_is_compared_by_instant() {
        // After the time index unit was widened, pre-alter files keep the old
        // unit while `scan_implied_time_range` uses the current column unit.
        // The coverage decision must compare instants across units instead of
        // asserting unit equality (which panicked here for widened regions).
        let partition = (
            Timestamp::new_millisecond(1000),
            Timestamp::new_millisecond(2000),
        );

        // Covering: the implied range [0, 3000)ms contains the file range
        // [1s, 2s] expressed in microseconds; time filters are dropped from
        // the key.
        let (mut ctx, part_range) = new_stream_context(
            vec![
                col("ts").gt_eq(ts_lit(0)),
                col("ts").lt(ts_lit(3000)),
                col("k0").eq(lit("foo")),
            ],
            TimestampRange::with_unit(0, 3000, TimeUnit::Millisecond),
            partition,
        )
        .await;
        ctx.ranges[0].time_range = (
            Timestamp::new_microsecond(1_000_000),
            Timestamp::new_microsecond(2_000_000),
        );
        let key = build_range_cache_key(&ctx, &part_range, None).unwrap();
        assert!(key.scan.time_filters().is_empty());

        // Not covering: the implied range [1500, 3000)ms excludes the file's
        // 1s lower bound; time filters stay in the key.
        let (mut ctx, part_range) = new_stream_context(
            vec![
                col("ts").gt_eq(ts_lit(1500)),
                col("ts").lt(ts_lit(3000)),
                col("k0").eq(lit("foo")),
            ],
            TimestampRange::with_unit(1500, 3000, TimeUnit::Millisecond),
            partition,
        )
        .await;
        ctx.ranges[0].time_range = (
            Timestamp::new_microsecond(1_000_000),
            Timestamp::new_microsecond(2_000_000),
        );
        let key = build_range_cache_key(&ctx, &part_range, None).unwrap();
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
            None
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
    fn selector_after_merge_changes_fingerprint() {
        let ordinary = test_scan_fingerprint(
            vec!["k0 = 'foo'".to_string()],
            vec![],
            Some(TimeSeriesRowSelector::LastRow { after_merge: false }),
            true,
            0,
        );
        let after_merge = test_scan_fingerprint(
            vec!["k0 = 'foo'".to_string()],
            vec![],
            Some(TimeSeriesRowSelector::LastRow { after_merge: true }),
            true,
            0,
        );

        assert_ne!(ordinary, after_merge);
    }

    #[test]
    fn true_selector_after_merge_is_preserved_by_fingerprint_transforms() {
        let normalized =
            test_scan_fingerprint(vec!["k0 = 'foo'".to_string()], vec![], None, true, 0);

        assert!(normalized.time_filters().is_empty());

        let fingerprint = test_scan_fingerprint(
            vec!["k0 = 'foo'".to_string()],
            vec!["ts >= 1000".to_string()],
            Some(TimeSeriesRowSelector::LastRow { after_merge: true }),
            true,
            7,
        );

        let reset = fingerprint.without_time_filters();
        let candidate = fingerprint.for_candidate_series();
        let series_data = fingerprint.for_series_data(SeriesRange::new(0, 1).unwrap());

        assert_eq!(reset.read_columns(), fingerprint.read_columns());
        assert_eq!(reset.read_column_types(), fingerprint.read_column_types());
        assert_eq!(reset.filters(), fingerprint.filters());
        assert!(reset.time_filters().is_empty());
        assert_eq!(reset.series_row_selector, fingerprint.series_row_selector);
        assert_eq!(
            fingerprint.series_row_selector,
            Some(TimeSeriesRowSelector::LastRow { after_merge: true })
        );
        assert_eq!(
            candidate.series_row_selector,
            fingerprint.series_row_selector
        );
        assert_eq!(
            series_data.series_row_selector,
            fingerprint.series_row_selector
        );
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

    fn source_stream(batches: Vec<RecordBatch>) -> BoxedRecordBatchStream {
        Box::pin(futures::stream::iter(batches.into_iter().map(Ok)))
    }

    async fn wait_cached(
        strategy: &CacheStrategy,
        key: &RangeScanCacheKey,
    ) -> Arc<RangeScanCacheValue> {
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                if let Some(value) = strategy.get_range_result(key) {
                    return value;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("range result was not published")
    }

    #[tokio::test]
    async fn range_budget_fallback_releases_prefix_on_drop() {
        let strategy = CacheStrategy::EnableAll(Arc::new(
            CacheManager::builder()
                .range_result_cache_size(1024)
                .build(),
        ));
        let (key, metrics) = test_cache_context(&strategy);
        let (mut stream, size) =
            get_or_load_range_result(&strategy, key.clone(), &metrics, || async {
                Ok((source_stream(vec![make_batch(&[1]), make_batch(&[2])]), 7))
            })
            .await
            .unwrap();
        assert_eq!(size, 7);
        assert!(strategy.get_range_result(&key).is_none());
        let limiter = strategy.range_result_load_limiter().unwrap();
        assert_eq!(limiter.available_permits(), 1);
        assert_eq!(stream.try_next().await.unwrap().unwrap(), make_batch(&[1]));
        assert_eq!(limiter.available_permits(), 0);
        drop(stream);
        assert_eq!(limiter.available_permits(), 1);
    }

    #[tokio::test]
    async fn test_merged_key_covers_files_and_snapshot_independently_of_order() {
        let (context, range) = new_stream_context(
            vec![col("k0").eq(lit("a"))],
            None,
            (
                Timestamp::new_millisecond(0),
                Timestamp::new_millisecond(10),
            ),
        )
        .await;
        let mut context = Arc::new(context);
        let first = build_range_cache_key(&context, &range, None).unwrap();
        let file = sst_file_handle_with_file_id(FileId::random(), 0, 10);
        Arc::get_mut(&mut context).unwrap().input.files.push(file);
        Arc::get_mut(&mut context).unwrap().ranges[0]
            .indices
            .push(SourceIndex {
                index: 1,
                num_row_groups: 1,
            });
        Arc::get_mut(&mut context).unwrap().ranges[0]
            .row_group_indices
            .push(RowGroupIndex {
                index: 1,
                row_group_index: -1,
            });
        let whole = build_range_cache_key(&context, &range, None).unwrap();
        assert_eq!(whole.row_groups.len(), 2);
        assert!(
            whole
                .row_groups
                .iter()
                .any(|(_, row_group)| *row_group == -1)
        );
        assert_ne!(first, whole);
        Arc::get_mut(&mut context).unwrap().ranges[0]
            .row_group_indices
            .swap(0, 1);
        let reordered = build_range_cache_key(&context, &range, None).unwrap();
        assert_eq!(whole, reordered);
        Arc::get_mut(&mut context).unwrap().input.snapshot_sequence = Some(42);
        let snapshot = build_range_cache_key(&context, &range, None).unwrap();
        assert_ne!(snapshot, reordered);
        assert_eq!(snapshot.scan.stage, RangeScanStage::Data);
        let sources = snapshot.scan.file_reads.as_ref().unwrap();
        assert_eq!(sources.len(), 2);
        assert!(
            sources
                .iter()
                .all(|source| source.snapshot_sequence == Some(42))
        );
    }

    fn new_partition_pruner(
        context: &Arc<StreamContext>,
        range: &PartitionRange,
    ) -> Arc<PartitionPruner> {
        let pruner = Arc::new(Pruner::new(context.clone(), 1));
        pruner.add_partition_ranges(std::slice::from_ref(range));
        Arc::new(PartitionPruner::new(pruner, std::slice::from_ref(range)))
    }

    async fn sst_range_stream(
        context: &Arc<StreamContext>,
        range: &PartitionRange,
        metrics: &PartitionMetrics,
        partition: Arc<PartitionPruner>,
        semaphore: Option<Arc<tokio::sync::Semaphore>>,
    ) -> BoxedRecordBatchStream {
        crate::read::seq_scan::SeqScan::build_flat_partition_range_read(
            context,
            range,
            false,
            metrics,
            partition,
            semaphore.clone(),
            semaphore,
        )
        .await
        .unwrap()
        .0
    }

    fn u64_values(batches: &[RecordBatch], name: &str) -> Vec<Option<u64>> {
        batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column_by_name(name)
                    .unwrap()
                    .as_any()
                    .downcast_ref::<datatypes::arrow::array::UInt64Array>()
                    .unwrap()
                    .iter()
            })
            .collect()
    }

    async fn write_overlapping_sst_files(
        env: &SchedulerEnv,
    ) -> (
        store_api::metadata::RegionMetadataRef,
        Vec<crate::sst::file::FileHandle>,
    ) {
        use datatypes::arrow::array::UInt64Array;
        use parquet::arrow::ArrowWriter;
        use parquet::file::metadata::KeyValue;
        use parquet::file::properties::WriterProperties;
        use store_api::region_request::PathType;

        use crate::sst::file::{FileHandle, FileMeta};
        use crate::sst::parquet::PARQUET_METADATA_KEY;
        use crate::test_util::sst_util::{
            new_record_batch_with_custom_sequence, sst_region_metadata,
        };

        let metadata = Arc::new(sst_region_metadata());
        let mut files = Vec::new();
        for sequence in [1, 2] {
            let props = WriterProperties::builder()
                .set_key_value_metadata(Some(vec![KeyValue::new(
                    PARQUET_METADATA_KEY.to_string(),
                    metadata.to_json().unwrap(),
                )]))
                .build();
            let first = new_record_batch_with_custom_sequence(&["a", "x"], 0, 2, sequence);
            let mut bytes = Vec::new();
            let mut writer = ArrowWriter::try_new(&mut bytes, first.schema(), Some(props)).unwrap();
            for start in [0, 2] {
                let batch =
                    new_record_batch_with_custom_sequence(&["a", "x"], start, start + 2, sequence);
                let mut columns = batch.columns().to_vec();
                columns[2] = Arc::new(UInt64Array::from(vec![
                    sequence * 100 + start as u64,
                    sequence * 100 + start as u64 + 1,
                ]));
                writer
                    .write(&RecordBatch::try_new(batch.schema(), columns).unwrap())
                    .unwrap();
                writer.flush().unwrap();
            }
            writer.close().unwrap();
            let file = FileHandle::new(
                FileMeta {
                    region_id: metadata.region_id,
                    file_id: FileId::random(),
                    time_range: (Timestamp::new_millisecond(0), Timestamp::new_millisecond(3)),
                    file_size: bytes.len() as u64,
                    num_rows: 4,
                    num_row_groups: 2,
                    preserve_row_sequence: true,
                    ..Default::default()
                },
                crate::test_util::new_noop_file_purger(),
            );
            env.access_layer
                .object_store()
                .write(&file.file_path("", PathType::Bare), bytes)
                .await
                .unwrap();
            files.push(file);
        }
        (metadata, files)
    }

    #[tokio::test]
    async fn test_range_cache_keeps_cross_sst_dedup_and_task_membership() {
        use store_api::storage::consts::SEQUENCE_COLUMN_NAME;

        for capacity in [0, 1024, 1024 * 1024] {
            let env = SchedulerEnv::new().await;
            let (metadata, files) = write_overlapping_sst_files(&env).await;
            let strategy = CacheStrategy::EnableAll(Arc::new(
                CacheManager::builder()
                    .range_result_cache_size(capacity)
                    .build(),
            ));
            for (case, (max_sequence, last_row)) in [
                (1, false),
                (2, false),
                (1, false),
                (2, true),
                (2, false),
                (1, true),
            ]
            .into_iter()
            .enumerate()
            {
                let mapper =
                    FlatProjectionMapper::new(&metadata, 0..metadata.column_metadatas.len())
                        .unwrap();
                let input = ScanInput::builder(env.access_layer.clone(), mapper)
                    .with_files(files.clone())
                    .with_cache(strategy.clone())
                    .with_append_mode(false)
                    .with_predicate(
                        PredicateGroup::new(&metadata, &[col("tag_0").eq(lit("a"))]).unwrap(),
                    )
                    .with_batch_size(1)
                    .with_snapshot_sequence(Some(max_sequence))
                    .with_series_row_selector(
                        last_row.then_some(TimeSeriesRowSelector::LastRow { after_merge: true }),
                    )
                    .with_sequence_range(Some(SequenceRange::GtLtEq {
                        min: 0,
                        max: max_sequence,
                    }))
                    .build();
                let context = Arc::new(StreamContext::seq_scan_ctx(input));
                assert_eq!(context.ranges.len(), 1);
                assert_eq!(context.ranges[0].row_group_indices.len(), 2);
                let range = context.ranges[0].new_partition_range(0);
                let (_, metrics) = test_cache_context(&strategy);
                for run in 0..2 {
                    let partition = new_partition_pruner(&context, &range);
                    let semaphore = Arc::new(tokio::sync::Semaphore::new(1));
                    let batches = tokio::time::timeout(std::time::Duration::from_secs(5), async {
                        let mut stream = sst_range_stream(
                            &context,
                            &range,
                            &metrics,
                            partition,
                            Some(semaphore.clone()),
                        )
                        .await;
                        // The first scan at snapshot 2 has no completed cache entry.
                        if capacity > 0 && case == 1 && run == 0 {
                            assert!(build_range_cache_key(&context, &range, None).is_some());
                            let (_, follower_metrics) = test_cache_context(&strategy);
                            let follower = sst_range_stream(
                                &context,
                                &range,
                                &follower_metrics,
                                new_partition_pruner(&context, &range),
                                Some(semaphore.clone()),
                            )
                            .await;
                            let first = stream.try_next().await.unwrap().unwrap();
                            assert!(first.num_rows() < 4);
                            drop(stream);
                            let batches = follower.try_collect::<Vec<_>>().await.unwrap();
                            assert!(batches.len() > 1);
                            assert_eq!(batches.first().unwrap(), &first);
                            assert_eq!(semaphore.available_permits(), 1);
                            let owner: serde_json::Value =
                                serde_json::from_str(&format!("{metrics:?}")).unwrap();
                            let follower: serde_json::Value =
                                serde_json::from_str(&format!("{follower_metrics:?}")).unwrap();
                            assert!(owner["metrics"]["num_file_ranges"].as_u64().unwrap() > 0);
                            assert_eq!(follower["metrics"]["num_file_ranges"], 0);
                            batches
                        } else {
                            stream.try_collect::<Vec<_>>().await.unwrap()
                        }
                    })
                    .await
                    .unwrap();
                    assert!(
                        u64_values(&batches, SEQUENCE_COLUMN_NAME)
                            .iter()
                            .all(|sequence| *sequence == Some(max_sequence))
                    );
                    let values = u64_values(&batches, "field_0")
                        .into_iter()
                        .map(Option::unwrap)
                        .collect::<Vec<_>>();
                    let expected = if last_row {
                        vec![max_sequence * 100 + 3]
                    } else {
                        (0..4).map(|offset| max_sequence * 100 + offset).collect()
                    };
                    assert_eq!(values, expected);
                }
                if capacity > 1024 {
                    let key = build_range_cache_key(&context, &range, None).unwrap();
                    let value = wait_cached(&strategy, &key).await;
                    assert!(!value.cached_batches.is_empty());
                }
            }
        }
    }

    #[tokio::test]
    async fn failed_shared_stream_does_not_poison_a_later_request() {
        let strategy = test_cache_strategy();
        let (key, metrics) = test_cache_context(&strategy);
        let (mut failed, _) =
            get_or_load_range_result(&strategy, key.clone(), &metrics, || async {
                let source = futures::stream::iter(vec![
                    Ok(make_batch(&[7])),
                    Err(crate::error::UnexpectedSnafu {
                        reason: "transient source error",
                    }
                    .build()),
                ]);
                Ok((Box::pin(source) as BoxedRecordBatchStream, 3))
            })
            .await
            .unwrap();
        // Keep a second reader alive so the failed generation remains registered.
        let (mut existing, _) =
            get_or_load_range_result(&strategy, key.clone(), &metrics, || async {
                panic!("existing reader must share the source")
            })
            .await
            .unwrap();
        assert_eq!(failed.try_next().await.unwrap().unwrap(), make_batch(&[7]));
        assert!(
            failed
                .try_next()
                .await
                .unwrap_err()
                .to_string()
                .contains("transient source error")
        );
        let expected = make_batch(&[1, 2, 3]);
        let (mut retry, _) = get_or_load_range_result(&strategy, key, &metrics, || async {
            Ok((
                Box::pin(futures::stream::iter([Ok(expected.clone())])) as BoxedRecordBatchStream,
                3,
            ))
        })
        .await
        .unwrap();
        assert_eq!(retry.try_next().await.unwrap().unwrap(), expected);
        assert_eq!(
            existing.try_next().await.unwrap().unwrap(),
            make_batch(&[7])
        );
        assert!(
            existing
                .try_next()
                .await
                .unwrap_err()
                .to_string()
                .contains("transient source error")
        );
    }

    #[tokio::test]
    async fn shared_ranges_build_once_before_consumers_poll() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::time::Duration;

        for enabled in [false, true] {
            let strategy = CacheStrategy::EnableAll(Arc::new(
                CacheManager::builder()
                    .range_result_cache_size(if enabled { 1024 * 1024 } else { 0 })
                    .build(),
            ));
            let (key, metrics) = test_cache_context(&strategy);
            let builds = AtomicUsize::new(0);
            let gate = Arc::new(tokio::sync::Semaphore::new(0));
            let batch = make_batch(&[1, 2, 3]);
            let mut requests = (0..8)
                .map(|_| {
                    Box::pin(get_or_load_range_result(
                        &strategy,
                        key.clone(),
                        &metrics,
                        || async {
                            builds.fetch_add(1, Ordering::Relaxed);
                            let batch = batch.clone();
                            let gate = gate.clone();
                            let stream = Box::pin(try_stream! {
                                gate.acquire().await.unwrap().forget();
                                yield batch;
                            });
                            Ok((stream as BoxedRecordBatchStream, 3))
                        },
                    ))
                })
                .collect::<Vec<_>>();
            // Polling all builders to completion must not require consuming any
            // returned stream, matching the series distributor's construction order.
            let streams = tokio::time::timeout(
                Duration::from_secs(5),
                futures::future::join_all(&mut requests),
            )
            .await
            .unwrap();
            assert_eq!(builds.load(Ordering::Relaxed), if enabled { 1 } else { 8 });
            gate.add_permits(8);
            for stream in streams {
                let (stream, _) = stream.unwrap();
                assert_eq!(
                    stream.try_collect::<Vec<_>>().await.unwrap(),
                    vec![batch.clone()]
                );
            }
        }
    }

    #[tokio::test]
    async fn shared_range_compaction_releases_sliced_buffers_and_preserves_batches() {
        let strategy = test_cache_strategy();
        let (key, metrics) = test_cache_context(&strategy);
        // Small slices retain the original large allocation until concatenated.
        let source = make_large_binary_batch(128, 1024);
        let expected = vec![source.slice(1, 1), source.slice(10, 2)];
        let original_bytes = expected
            .iter()
            .map(RecordBatch::get_array_memory_size)
            .sum::<usize>();
        let input = expected.clone();
        let (stream, _) = get_or_load_range_result(&strategy, key.clone(), &metrics, || async {
            Ok((
                Box::pin(futures::stream::iter(input.into_iter().map(Ok)))
                    as BoxedRecordBatchStream,
                3,
            ))
        })
        .await
        .unwrap();
        assert_eq!(stream.try_collect::<Vec<_>>().await.unwrap(), expected);
        let cached = wait_cached(&strategy, &key).await;
        assert!(cached.estimated_batches_size < original_bytes / 4);
        assert_eq!(
            cached_flat_range_stream(cached)
                .try_collect::<Vec<_>>()
                .await
                .unwrap(),
            expected
        );
    }

    #[tokio::test]
    async fn oversized_shared_range_keeps_existing_readers_and_declines_new_readers() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let reads = Arc::new(AtomicUsize::new(0));
        let count = reads.clone();
        let strategy = CacheStrategy::EnableAll(Arc::new(
            CacheManager::builder()
                .range_result_cache_size(1024)
                .build(),
        ));
        let (key, metrics) = test_cache_context(&strategy);
        let expected = vec![make_batch(&[1]), make_batch(&[2, 3]), make_batch(&[4])];
        let input = expected.clone();
        let (mut owner, _) = get_or_load_range_result(&strategy, key.clone(), &metrics, || async {
            let source = futures::stream::iter(input.into_iter().map(move |batch| {
                count.fetch_add(1, Ordering::Relaxed);
                Ok(batch)
            }));
            Ok((Box::pin(source) as BoxedRecordBatchStream, 1))
        })
        .await
        .unwrap();
        assert_eq!(reads.load(Ordering::Relaxed), 0);
        let limiter = strategy.range_result_load_limiter().unwrap();
        assert_eq!(limiter.available_permits(), 1);
        assert!(strategy.get_range_result(&key).is_none());
        let (follower, _) = get_or_load_range_result(&strategy, key.clone(), &metrics, || async {
            panic!("existing follower must share")
        })
        .await
        .unwrap();
        assert_eq!(owner.try_next().await.unwrap().unwrap(), expected[0]);
        assert_eq!(owner.try_next().await.unwrap().unwrap(), expected[1]);
        let (late, _) = get_or_load_range_result(&strategy, key.clone(), &metrics, || async {
            Ok((source_stream(vec![make_batch(&[9])]), 1))
        })
        .await
        .unwrap();
        assert_eq!(
            late.try_collect::<Vec<_>>().await.unwrap(),
            vec![make_batch(&[9])]
        );
        assert_eq!(follower.try_collect::<Vec<_>>().await.unwrap(), expected);
        assert_eq!(
            owner.try_collect::<Vec<_>>().await.unwrap(),
            vec![make_batch(&[4])]
        );
        assert!(strategy.get_range_result(&key).is_none());
        assert_eq!(reads.load(Ordering::Relaxed), 3);
        assert_eq!(limiter.available_permits(), 1);
        let (retry, _) = get_or_load_range_result(&strategy, key.clone(), &metrics, || async {
            Ok((source_stream(vec![]), 1))
        })
        .await
        .unwrap();
        assert!(retry.try_collect::<Vec<_>>().await.unwrap().is_empty());
        wait_cached(&strategy, &key).await;
    }

    #[tokio::test]
    async fn dropping_reader_releases_unpolled_or_pending_source() {
        for polled in [false, true] {
            let strategy = test_cache_strategy();
            let (key, metrics) = test_cache_context(&strategy);
            let source_gate = Arc::new(tokio::sync::Semaphore::new(1));
            let permit = source_gate.clone().acquire_owned().await.unwrap();
            let (mut stream, _) =
                get_or_load_range_result(&strategy, key.clone(), &metrics, || async {
                    let source = Box::pin(try_stream! {
                        let _permit = permit;
                        std::future::pending::<()>().await;
                        yield make_batch(&[1]);
                    });
                    Ok((source as BoxedRecordBatchStream, 1))
                })
                .await
                .unwrap();
            assert_eq!(source_gate.available_permits(), 0);
            if polled {
                let next = stream.try_next();
                tokio::pin!(next);
                assert!(futures::poll!(&mut next).is_pending());
            }
            assert_eq!(source_gate.available_permits(), 0);
            drop(stream);
            assert_eq!(source_gate.available_permits(), 1);
            assert!(strategy.get_range_result(&key).is_none());
            let (retry, _) = get_or_load_range_result(&strategy, key, &metrics, || async {
                Ok((source_stream(vec![make_batch(&[2])]), 1))
            })
            .await
            .unwrap();
            assert_eq!(
                retry.try_collect::<Vec<_>>().await.unwrap(),
                vec![make_batch(&[2])]
            );
        }
    }

    #[tokio::test]
    async fn active_ranges_isolate_memory_pool_failures() {
        let strategy = test_cache_strategy();
        let (key, metrics) = test_cache_context(&strategy);
        let (failed, _) =
            get_or_load_range_result_in_scope(&strategy, key.clone(), &metrics, 1, || async {
                let error = crate::error::UnexpectedSnafu {
                    reason: "first memory pool exhausted",
                }
                .build();
                Ok((
                    Box::pin(futures::stream::iter(vec![Err(error)])) as BoxedRecordBatchStream,
                    1,
                ))
            })
            .await
            .unwrap();
        let (same_pool, _) =
            get_or_load_range_result_in_scope(&strategy, key.clone(), &metrics, 1, || async {
                panic!("same pool must share the active scan")
            })
            .await
            .unwrap();
        let (other_pool, _) =
            get_or_load_range_result_in_scope(&strategy, key.clone(), &metrics, 2, || async {
                Ok((source_stream(vec![make_batch(&[7])]), 1))
            })
            .await
            .unwrap();
        assert!(failed.try_collect::<Vec<_>>().await.is_err());
        assert!(same_pool.try_collect::<Vec<_>>().await.is_err());
        assert_eq!(
            other_pool.try_collect::<Vec<_>>().await.unwrap(),
            vec![make_batch(&[7])]
        );
        wait_cached(&strategy, &key).await;
        let (completed, _) =
            get_or_load_range_result_in_scope(&strategy, key, &metrics, 1, || async {
                panic!("completed results must remain reusable across pools")
            })
            .await
            .unwrap();
        assert_eq!(
            completed.try_collect::<Vec<_>>().await.unwrap(),
            vec![make_batch(&[7])]
        );
    }

    #[tokio::test]
    async fn cancelling_initial_reader_preserves_followers_and_releases_memory() {
        let strategy = test_cache_strategy();
        let (key, metrics) = test_cache_context(&strategy);
        let limiter = strategy.range_result_load_limiter().unwrap();
        let initial = limiter.available_permits();
        let gate = Arc::new(tokio::sync::Notify::new());
        let producer_gate = gate.clone();
        let (mut owner, _) = get_or_load_range_result(&strategy, key.clone(), &metrics, || async {
            let source = Box::pin(try_stream! {
                yield make_batch(&[1]);
                producer_gate.notified().await;
                yield make_batch(&[2]);
            });
            Ok((source as BoxedRecordBatchStream, 1))
        })
        .await
        .unwrap();
        let (mut follower, _) =
            get_or_load_range_result(&strategy, key.clone(), &metrics, || async {
                panic!("follower must share")
            })
            .await
            .unwrap();
        assert_eq!(owner.try_next().await.unwrap().unwrap(), make_batch(&[1]));
        drop(owner);
        assert_eq!(
            follower.try_next().await.unwrap().unwrap(),
            make_batch(&[1])
        );
        gate.notify_one();
        assert_eq!(
            follower.try_next().await.unwrap().unwrap(),
            make_batch(&[2])
        );
        // Stop before EOF: unfinished results must not be cached.
        drop(follower);
        assert_eq!(limiter.available_permits(), initial);
        assert!(strategy.get_range_result(&key).is_none());
    }

    #[tokio::test]
    async fn completed_stream_does_not_wait_for_cache_compaction() {
        let strategy = test_cache_strategy();
        let (key, metrics) = test_cache_context(&strategy);
        let limiter = strategy.range_result_memory_limiter().unwrap();
        let permit = limiter
            .acquire(limiter.available_permits() * limiter.permit_bytes())
            .await
            .unwrap();
        let expected = vec![make_batch(&[1]), make_batch(&[2])];
        let input = expected.clone();
        let (stream, _) = get_or_load_range_result(&strategy, key.clone(), &metrics, || async {
            Ok((source_stream(input), 1))
        })
        .await
        .unwrap();
        assert_eq!(stream.try_collect::<Vec<_>>().await.unwrap(), expected);
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while limiter.waited_acquires() == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("background compaction did not reach its memory wait");
        assert!(strategy.get_range_result(&key).is_none());
        let (joined, _) = get_or_load_range_result(&strategy, key.clone(), &metrics, || async {
            panic!("finalizing scan must remain discoverable")
        })
        .await
        .unwrap();
        assert_eq!(joined.try_collect::<Vec<_>>().await.unwrap(), expected);
        drop(permit);
        wait_cached(&strategy, &key).await;
    }

    #[tokio::test]
    async fn test_range_load_compacts_at_thresholds_and_keeps_tail() {
        let row_batch = make_batch(&vec![1; DEFAULT_READ_BATCH_SIZE / 2 + 1]);
        // Two batches exceed the byte threshold without reaching the row threshold.
        let byte_batch = make_large_binary_batch(1024, 4097);
        assert!(byte_batch.num_rows() * 2 <= DEFAULT_READ_BATCH_SIZE);
        assert!(byte_batch.get_array_memory_size() * 2 > RANGE_CACHE_COMPACT_THRESHOLD_BYTES);
        assert!(row_batch.get_array_memory_size() * 2 < RANGE_CACHE_COMPACT_THRESHOLD_BYTES);
        for batches in [
            vec![],
            vec![make_batch(&[1, 2]), make_batch(&[3]), make_batch(&[4, 5])],
            vec![make_batch(&[1, 2, 3])],
            vec![row_batch.clone(), row_batch.clone(), row_batch],
            vec![byte_batch.clone(), byte_batch.clone(), byte_batch],
        ] {
            let input_size = batches
                .iter()
                .map(RecordBatch::get_array_memory_size)
                .sum::<usize>();
            let strategy = CacheStrategy::EnableAll(Arc::new(
                CacheManager::builder()
                    .range_result_cache_size((input_size * 2).max(1024 * 1024) as u64)
                    .build(),
            ));
            let (key, metrics) = test_cache_context(&strategy);
            let input = batches.clone();
            let expected_batch_size = 7;
            let (stream, batch_size) =
                get_or_load_range_result(&strategy, key.clone(), &metrics, || async move {
                    Ok((source_stream(input), expected_batch_size))
                })
                .await
                .unwrap();
            assert_eq!(batch_size, expected_batch_size);
            assert_eq!(stream.try_collect::<Vec<_>>().await.unwrap(), batches);
            let value = wait_cached(&strategy, &key).await;
            let expected_lengths = if batches.is_empty() {
                vec![]
            } else if batches.iter().map(RecordBatch::num_rows).sum::<usize>() <= 5 {
                vec![batches.iter().map(RecordBatch::num_rows).collect()]
            } else {
                vec![
                    vec![batches[0].num_rows(), batches[1].num_rows()],
                    vec![batches[2].num_rows()],
                ]
            };
            assert_eq!(
                value
                    .cached_batches
                    .iter()
                    .map(|batch| batch.slice_lengths.clone())
                    .collect::<Vec<_>>(),
                expected_lengths
            );
            assert_eq!(
                value
                    .cached_batches
                    .iter()
                    .map(|slice| slice.batch.num_rows())
                    .sum::<usize>(),
                batches.iter().map(RecordBatch::num_rows).sum::<usize>()
            );
            assert_eq!(
                value.estimated_batches_size,
                value
                    .cached_batches
                    .iter()
                    .map(|batch| batch.batch.get_array_memory_size())
                    .sum::<usize>()
            );
            let (hit, batch_size) = get_or_load_range_result(&strategy, key, &metrics, || async {
                panic!("completed range must be reused without rebuilding");
            })
            .await
            .unwrap();
            assert_eq!(batch_size, expected_batch_size);
            assert_eq!(hit.try_collect::<Vec<_>>().await.unwrap(), batches);
        }
    }
}
