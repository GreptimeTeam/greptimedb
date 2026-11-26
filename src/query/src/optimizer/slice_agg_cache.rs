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

//! Caches partial aggregation states of a region scan per time slice.
//!
//! The rule rewrites a partial aggregation over a region scan bounded in time so that it
//! also groups by the time slice of each row; its output then splits into per-slice states.
//! A slice is served from the cache when the scanner reports the same
//! [TimeRangeFingerprint] for it as the cached entry, and is pruned from the scan. The
//! other slices are computed by the same scan and cached once every partition has
//! finished. All states therefore come from the data the scanner was built on. The final
//! aggregation merges states regardless of how rows were split, so the result does not
//! change.

use std::collections::{HashMap, HashSet};
use std::fmt::{self, Write as _};
use std::hash::Hash;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use arrow::array::{AsArray, Int64Array, UInt32Array};
use arrow::compute::{cast, take_record_batch};
use arrow::datatypes::{DataType, Int64Type, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use common_runtime::spawn_global;
use common_telemetry::warn;
use common_time::Timestamp;
use common_time::range::TimestampRange;
use common_time::timestamp::TimeUnit;
use datafusion::config::ConfigOptions;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::execution::TaskContext;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion::physical_expr::expressions::{
    BinaryExpr, Column, DynamicFilterPhysicalExpr, Literal,
};
use datafusion::physical_expr::{EquivalenceProperties, ScalarFunctionExpr};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode, PhysicalGroupBy};
use datafusion::physical_plan::coop::CooperativeExec;
use datafusion::physical_plan::filter::{FilterExec, FilterExecBuilder};
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricBuilder, MetricsSet};
use datafusion::physical_plan::projection::{ProjectionExec, ProjectionExpr};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    ColumnarValue, DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties,
    InputOrderMode, Partitioning, PlanProperties, SendableRecordBatchStream, collect,
    replace_children_if_necessary,
};
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion_common::{DataFusionError, Result, ScalarValue, internal_err};
use datafusion_expr::{Expr, Operator, Volatility};
use datafusion_physical_expr::PhysicalExpr;
use futures::StreamExt;
use moka::sync::Cache;
use prost::Message;
use sha2::{Digest, Sha256};
use store_api::region_engine::TimeRangeFingerprint;
use table::predicate::implied_time_range_from_exprs;
use table::table::scan::RegionScanExec;

use crate::metrics::{
    SLICE_AGG_CACHE_BYTES, SLICE_AGG_CACHE_SKIPPED_TOTAL, SLICE_AGG_CACHE_SLICES_TOTAL,
};
use crate::options::{QueryOptions, SliceAggCacheExtension};

const SLICE_SECONDS: i64 = 3600;
const MAX_SLICES: i64 = 10_000;
const SLICE_COLUMN: &str = "__slice_start";
const SLICE_TS_COLUMN: &str = "__slice_ts";

/// SHA-256 of everything that decides the partial states other than the slice bounds and
/// the data. Cache hits trust it without comparing the source, so it must resist
/// collisions; it also keeps keys a fixed size however large the plan is.
type PlanDigest = [u8; 32];

struct DigestWriter(Sha256);

impl fmt::Write for DigestWriter {
    fn write_str(&mut self, s: &str) -> fmt::Result {
        self.0.update(s.as_bytes());
        Ok(())
    }
}

#[derive(Clone, PartialEq, Eq, Hash)]
struct SliceKey {
    plan: PlanDigest,
    start: i64,
    end: i64,
}

struct SliceEntry {
    fingerprint: TimeRangeFingerprint,
    batches: Vec<RecordBatch>,
}

/// The caches of one query engine.
pub(crate) struct SliceCacheStore {
    entries: Cache<SliceKey, Arc<SliceEntry>>,
    /// Plans whose states exceeded `max_collect_bytes`, which are not rewritten until the
    /// idle timeout passes: they would pay for the extra grouping without being cached.
    oversized: Cache<PlanDigest, ()>,
    /// Plans seen once. A plan is rewritten only when it comes again, so a query that runs
    /// once does not pay for the extra slice grouping.
    seen: Cache<PlanDigest, ()>,
    /// A query stops collecting states for the cache once they exceed this size.
    max_collect_bytes: usize,
    /// The size of `entries` last added to [SLICE_AGG_CACHE_BYTES], which sums the
    /// caches of all engines in the process.
    reported_bytes: AtomicU64,
}

impl SliceCacheStore {
    fn new(options: &QueryOptions) -> Self {
        let idle_timeout = options.experimental_slice_agg_cache_idle_timeout;
        Self {
            entries: Cache::builder()
                .max_capacity(options.experimental_slice_agg_cache_size.as_bytes())
                .weigher(|_key: &SliceKey, entry: &Arc<SliceEntry>| {
                    let bytes = size_of::<SliceKey>()
                        + entry.fingerprint.len()
                        + entry
                            .batches
                            .iter()
                            .map(|b| b.get_array_memory_size())
                            .sum::<usize>();
                    bytes.try_into().unwrap_or(u32::MAX)
                })
                .time_to_idle(idle_timeout)
                .build(),
            oversized: Cache::builder()
                .max_capacity(1024)
                .time_to_live(idle_timeout)
                .build(),
            seen: Cache::builder()
                .max_capacity(10_000)
                .time_to_live(idle_timeout)
                .build(),
            max_collect_bytes: options
                .experimental_slice_agg_cache_max_collect_size
                .as_bytes() as usize,
            reported_bytes: AtomicU64::new(0),
        }
    }

    fn report_bytes(&self) {
        let bytes = self.entries.weighted_size();
        let last = self.reported_bytes.swap(bytes, Ordering::Relaxed);
        SLICE_AGG_CACHE_BYTES.add(bytes as i64 - last as i64);
    }
}

pub struct SliceAggCache {
    store: Arc<SliceCacheStore>,
}

impl SliceAggCache {
    pub fn new(options: &QueryOptions) -> Self {
        Self {
            store: Arc::new(SliceCacheStore::new(options)),
        }
    }
}

impl fmt::Debug for SliceAggCache {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SliceAggCache").finish()
    }
}

impl PhysicalOptimizerRule for SliceAggCache {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !slice_agg_cache_enabled(config) {
            return Ok(plan);
        }
        plan.transform_up(|node| {
            let Some(agg) = node.downcast_ref::<AggregateExec>() else {
                return Ok(Transformed::no(node));
            };
            match rewrite(&self.store, agg, &node, config)? {
                Some(exec) => Ok(Transformed::yes(exec)),
                None => Ok(Transformed::no(node)),
            }
        })
        .map(|t| t.data)
    }

    fn name(&self) -> &str {
        "SliceAggCache"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

pub(crate) fn slice_agg_cache_enabled(config: &ConfigOptions) -> bool {
    config
        .extensions
        .get::<SliceAggCacheExtension>()
        .is_some_and(|ext| ext.enabled)
}

/// Returns the time range to slice and the indexes of the request filters it replaces.
///
/// Only filters whose implied range is exact are used: every row in the returned range
/// satisfies them, so they can be left out of the cache key. Other filters on the time
/// index stay in the key.
pub(crate) fn slicing_range(
    filters: &[Expr],
    ts_column: &str,
    unit: TimeUnit,
) -> Option<(i64, i64, Vec<usize>)> {
    let mut range = TimestampRange::min_to_max();
    let mut implied = Vec::new();
    for (i, filter) in filters.iter().enumerate() {
        if let Some(r) = implied_time_range_from_exprs(ts_column, unit, &[filter]) {
            range = range.and(&r);
            implied.push(i);
        }
    }
    if implied.is_empty() || range.is_empty() {
        return None;
    }
    let (Some(start), Some(end)) = (range.start(), range.end()) else {
        return None;
    };
    Some((
        start.convert_to(unit)?.value(),
        end.convert_to(unit)?.value(),
        implied,
    ))
}

/// Returns the region scan below a chain of row-wise operators, if any.
fn find_scan(plan: &Arc<dyn ExecutionPlan>) -> Option<&RegionScanExec> {
    if let Some(scan) = plan.downcast_ref::<RegionScanExec>() {
        return Some(scan);
    }
    // A filter with a fetch keeps only some rows, and pruning cached slices would change
    // which ones.
    let row_wise = plan
        .downcast_ref::<FilterExec>()
        .is_some_and(|f| f.fetch().is_none())
        || plan.is::<ProjectionExec>()
        || plan.is::<CooperativeExec>();
    if row_wise {
        return find_scan(plan.children().first()?);
    }
    None
}

/// Rebuilds the chain above the scan so that it also outputs the scan column
/// `scan_column`, appending it where a projection drops it. Existing output columns keep
/// their positions. Returns the new chain and the position of the column in its output.
fn carry_scan_column(
    plan: &Arc<dyn ExecutionPlan>,
    scan_column: usize,
) -> Result<Option<(Arc<dyn ExecutionPlan>, usize)>> {
    if plan.is::<RegionScanExec>() {
        return Ok(Some((plan.clone(), scan_column)));
    }
    let Some(child) = plan.children().first().map(|c| (*c).clone()) else {
        return Ok(None);
    };
    let Some((new_child, index)) = carry_scan_column(&child, scan_column)? else {
        return Ok(None);
    };
    let unchanged = Arc::ptr_eq(&new_child, &child);

    if plan.is::<CooperativeExec>() {
        let plan = replace_children_if_necessary(plan.clone(), vec![new_child])?;
        return Ok(Some((plan, index)));
    }
    if let Some(filter) = plan.downcast_ref::<FilterExec>() {
        let Some(projection) = filter.projection() else {
            let plan = replace_children_if_necessary(plan.clone(), vec![new_child])?;
            return Ok(Some((plan, index)));
        };
        let existing = projection.iter().position(|&i| i == index);
        if unchanged && let Some(pos) = existing {
            return Ok(Some((plan.clone(), pos)));
        }
        let mut projection = projection.to_vec();
        let pos = existing.unwrap_or_else(|| {
            projection.push(index);
            projection.len() - 1
        });
        let filter = FilterExecBuilder::new(filter.predicate().clone(), new_child)
            .with_default_selectivity(filter.default_selectivity())
            .with_batch_size(filter.batch_size())
            .with_fetch(filter.fetch())
            .apply_projection(Some(projection))?
            .build()?;
        return Ok(Some((Arc::new(filter), pos)));
    }
    if let Some(projection) = plan.downcast_ref::<ProjectionExec>() {
        let existing = projection.expr().iter().position(|p| {
            p.expr
                .downcast_ref::<Column>()
                .is_some_and(|c| c.index() == index)
        });
        if unchanged && let Some(pos) = existing {
            return Ok(Some((plan.clone(), pos)));
        }
        let mut exprs = projection.expr().to_vec();
        let pos = match existing {
            Some(pos) => pos,
            None => {
                let name = new_child.schema().field(index).name().clone();
                exprs.push(ProjectionExpr {
                    expr: Arc::new(Column::new(&name, index)),
                    alias: SLICE_TS_COLUMN.to_string(),
                });
                exprs.len() - 1
            }
        };
        return Ok(Some((
            Arc::new(ProjectionExec::try_new(exprs, new_child)?),
            pos,
        )));
    }
    Ok(None)
}

fn rewrite(
    store: &Arc<SliceCacheStore>,
    agg: &AggregateExec,
    node: &Arc<dyn ExecutionPlan>,
    config: &ConfigOptions,
) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    let Some(scan) = find_scan(agg.input()) else {
        return Ok(None);
    };
    if *agg.mode() != AggregateMode::Partial {
        return skip("no_partial_stage");
    }
    if agg.group_expr().has_grouping_set()
        || agg.limit_options().is_some()
        || *agg.input_order_mode() != InputOrderMode::Linear
    {
        return skip("aggregate_shape");
    }
    let request = scan.request();
    // Dynamic filters change while the query runs. Requests bound to a sequence or a
    // snapshot read a different view than the one the scanner fingerprints. Metric engine
    // tables share a physical region, so any write to it would invalidate all of them.
    if scan.has_pushed_dyn_filters() {
        return skip("dynamic_filter");
    }
    if scan.reads_metric_region() {
        return skip("metric_engine");
    }
    if request.limit.is_some()
        || request.series_row_selector.is_some()
        || request.memtable_max_sequence.is_some()
        || request.memtable_min_sequence.is_some()
        || request.sst_min_sequence.is_some()
        || request.snapshot_on_scan
        || request.skip_sst_files
        || request.exact_sequence_range
    {
        return skip("scan_request");
    }
    let exprs = plan_exprs(agg);
    if has_non_immutable_expr(agg, &exprs) {
        return skip("not_immutable");
    }

    let metadata = scan.scanner_metadata();
    let ts = metadata.time_index_column();
    let ts_name = ts.column_schema.name.clone();
    let Some(ts_unit) = ts.column_schema.data_type.as_timestamp().map(|t| t.unit()) else {
        return skip("no_time_range");
    };
    let Some((start, end, implied)) = slicing_range(&request.filters, &ts_name, ts_unit) else {
        return skip("no_time_range");
    };
    let Some(width) = Timestamp::new_second(SLICE_SECONDS)
        .convert_to(ts_unit)
        .map(|t| t.value())
    else {
        return Ok(None);
    };
    // Rounding the earliest timestamps down to the slice width can underflow.
    if end
        .checked_sub(start)
        .is_none_or(|span| span / width > MAX_SLICES)
        || start.div_euclid(width).checked_mul(width).is_none()
    {
        return skip("too_many_slices");
    }
    let mut slices = Vec::new();
    let mut lo = start;
    while lo < end {
        let Some(next) = (lo.div_euclid(width) + 1).checked_mul(width) else {
            return skip("too_many_slices");
        };
        let hi = next.min(end);
        slices.push((lo, hi));
        lo = hi;
    }
    if slices.len() < 2 {
        return skip("single_slice");
    }

    let Some(identity) = plan_identity(agg, &exprs, scan, &implied, &ts_name, width, config)?
    else {
        return skip("unsupported_literal");
    };
    if store.oversized.contains_key(&identity) {
        return skip("oversized");
    }
    let Ok(scan_ts) = scan.schema().index_of(&ts_name) else {
        return skip("unsupported_plan");
    };
    let Some((input, input_ts)) = carry_scan_column(agg.input(), scan_ts)? else {
        return skip("unsupported_plan");
    };
    if store.seen.get(&identity).is_none() {
        store.seen.insert(identity, ());
        return skip("first_run");
    }
    let mut group_exprs = agg.group_expr().expr().to_vec();
    let index = group_exprs.len();
    group_exprs.push((
        Arc::new(SliceStartExpr {
            name: ts_name.clone(),
            index: input_ts,
            width,
        }),
        SLICE_COLUMN.to_string(),
    ));
    let partial = Arc::new(AggregateExec::try_new(
        AggregateMode::Partial,
        PhysicalGroupBy::new_single(group_exprs),
        agg.aggr_expr().to_vec(),
        agg.filter_expr().to_vec(),
        input.clone(),
        input.schema(),
    )?);
    let slice_column = SliceColumn { index, width };

    Ok(Some(Arc::new(SliceAggExec {
        input: partial,
        schema: node.schema(),
        // Cached states all come out of partition 0 ahead of computed ones, so the output
        // is neither partitioned nor ordered by any key.
        properties: Arc::new(PlanProperties::new(
            EquivalenceProperties::new(node.schema()),
            Partitioning::UnknownPartitioning(node.output_partitioning().partition_count()),
            node.pipeline_behavior(),
            node.boundedness(),
        )),
        slice_column,
        ts_name,
        ts_unit,
        width,
        slices,
        identity,
        store: store.clone(),
        state: Mutex::new(None),
        metrics: ExecutionPlanMetricsSet::new(),
    })))
}

fn skip(reason: &str) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    SLICE_AGG_CACHE_SKIPPED_TOTAL
        .with_label_values(&[reason])
        .inc();
    Ok(None)
}

/// The expressions the aggregation and the operators between it and the scan evaluate.
fn plan_exprs(agg: &AggregateExec) -> Vec<Arc<dyn PhysicalExpr>> {
    let mut exprs: Vec<Arc<dyn PhysicalExpr>> = agg
        .group_expr()
        .expr()
        .iter()
        .map(|(e, _)| e.clone())
        .collect();
    for aggr in agg.aggr_expr() {
        exprs.extend(aggr.expressions());
        exprs.extend(aggr.order_bys().iter().map(|s| s.expr.clone()));
    }
    exprs.extend(agg.filter_expr().iter().flatten().cloned());
    let mut node = agg.input().clone();
    loop {
        if let Some(filter) = node.downcast_ref::<FilterExec>() {
            exprs.push(filter.predicate().clone());
        } else if let Some(projection) = node.downcast_ref::<ProjectionExec>() {
            exprs.extend(projection.expr().iter().map(|p| p.expr.clone()));
        }
        let Some(child) = node.children().first().map(|c| (*c).clone()) else {
            break;
        };
        node = child;
    }
    exprs
}

/// Returns true if an expression of the plan can return different values in later
/// queries for the same input, which a cached state would not reflect. Calls that the
/// planner folded into literals, such as `now()`, are compared by value in the key.
fn has_non_immutable_expr(agg: &AggregateExec, exprs: &[Arc<dyn PhysicalExpr>]) -> bool {
    if agg
        .aggr_expr()
        .iter()
        .any(|aggr| aggr.fun().signature().volatility != Volatility::Immutable)
    {
        return true;
    }
    exprs.iter().any(|expr| {
        expr.exists(|e| {
            Ok(e.downcast_ref::<ScalarFunctionExpr>()
                .is_some_and(|f| f.fun().signature().volatility != Volatility::Immutable))
        })
        .unwrap_or(true)
    })
}

/// Describes everything that decides the partial states apart from the slice bounds and
/// the scanned data.
///
/// Debug output is used because the physical expressions implement neither a stable
/// serialization nor an equality that covers all their fields (`AggregateFunctionExpr`
/// ignores `DISTINCT` and the ordering). `ScalarFunctionExpr` leaves its config out of
/// Debug, so the session config is appended. Debug renders string and nested literals
/// ambiguously (`['a, b']` and `['a', 'b']` both print as `List([a, b])`), so literals
/// are appended in their protobuf encoding. Returns `None` if a literal has none.
fn plan_identity(
    agg: &AggregateExec,
    exprs: &[Arc<dyn PhysicalExpr>],
    scan: &RegionScanExec,
    implied: &[usize],
    ts_name: &str,
    width: i64,
    config: &ConfigOptions,
) -> Result<Option<PlanDigest>> {
    let request = scan.request();
    let mut s = DigestWriter(Sha256::new());
    let fmt_err = |e: fmt::Error| DataFusionError::External(Box::new(e));
    writeln!(
        s,
        "region={} ts={ts_name} width={width}",
        scan.scanner_metadata().region_id
    )
    .map_err(fmt_err)?;
    writeln!(
        s,
        "group={:?}\naggr={:?}\nfilter={:?}",
        agg.group_expr(),
        agg.aggr_expr(),
        agg.filter_expr()
    )
    .map_err(fmt_err)?;
    let mut node = agg.input().clone();
    loop {
        if let Some(filter) = node.downcast_ref::<FilterExec>() {
            writeln!(
                s,
                "filter={:?} {:?}",
                filter.predicate(),
                filter.projection()
            )
            .map_err(fmt_err)?;
        } else if let Some(projection) = node.downcast_ref::<ProjectionExec>() {
            writeln!(s, "projection={:?}", projection.expr()).map_err(fmt_err)?;
        } else if node.is::<CooperativeExec>() {
            writeln!(s, "coop").map_err(fmt_err)?;
        } else if node.is::<RegionScanExec>() {
            break;
        } else {
            return internal_err!(
                "unexpected operator {} below a sliced aggregate",
                node.name()
            );
        }
        let Some(child) = node.children().first().map(|c| (*c).clone()) else {
            break;
        };
        node = child;
    }
    for expr in exprs {
        let mut literals = Vec::new();
        expr.apply(|e| {
            if let Some(literal) = e.downcast_ref::<Literal>() {
                literals.push(literal.value().clone());
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
        if !write_literals(&mut s, &literals) {
            return Ok(None);
        }
    }
    let mut filters = Vec::new();
    for (i, filter) in request.filters.iter().enumerate() {
        if implied.contains(&i) {
            continue;
        }
        let mut literals = Vec::new();
        filter.apply(|e| {
            if let Expr::Literal(value, _) = e {
                literals.push(value.clone());
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
        let mut encoded = format!("{filter:?}");
        if !write_literals(&mut encoded, &literals) {
            return Ok(None);
        }
        filters.push(encoded);
    }
    filters.sort_unstable();
    let mut json_hints: Vec<String> = request
        .json_type_hint
        .iter()
        .map(|(k, v)| format!("{k}:{v:?}"))
        .collect();
    json_hints.sort_unstable();
    writeln!(
        s,
        "scan projection={:?} filters={filters:?} json={json_hints:?}",
        request.projection
    )
    .map_err(fmt_err)?;
    for field in scan.schema().fields() {
        writeln!(
            s,
            "field {}:{}:{}",
            field.name(),
            field.data_type(),
            field.is_nullable()
        )
        .map_err(fmt_err)?;
    }
    let mut entries: Vec<_> = config
        .entries()
        .into_iter()
        .map(|e| format!("{}={:?}", e.key, e.value))
        .collect();
    entries.sort_unstable();
    for entry in entries {
        writeln!(s, "{entry}").map_err(fmt_err)?;
    }
    Ok(Some(s.0.finalize().into()))
}

/// Appends the protobuf encoding of each literal, or returns false if one cannot be
/// encoded.
fn write_literals(s: &mut impl fmt::Write, literals: &[ScalarValue]) -> bool {
    for literal in literals {
        let Ok(encoded) = datafusion_proto::protobuf::ScalarValue::try_from(literal) else {
            return false;
        };
        let _ = s.write_str("\nliteral=");
        for byte in encoded.encode_to_vec() {
            let _ = write!(s, "{byte:02x}");
        }
    }
    true
}

/// The start of the slice a timestamp falls in, in the unit of the timestamp.
#[derive(Debug, PartialEq, Eq, Hash)]
struct SliceStartExpr {
    name: String,
    index: usize,
    width: i64,
}

impl fmt::Display for SliceStartExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "slice_start({}@{}, {})",
            self.name, self.index, self.width
        )
    }
}

impl PhysicalExpr for SliceStartExpr {
    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(DataType::Int64)
    }

    fn nullable(&self, input_schema: &Schema) -> Result<bool> {
        Ok(input_schema.field(self.index).is_nullable())
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let values = cast(batch.column(self.index), &DataType::Int64)?;
        let width = self.width;
        let starts: Int64Array = values
            .as_primitive::<Int64Type>()
            .unary(|v| v.div_euclid(width) * width);
        Ok(ColumnarValue::Array(Arc::new(starts)))
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(self)
    }

    fn fmt_sql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self}")
    }
}

/// A partial aggregation evaluated per time slice through the cache.
///
/// `input` is the original partial aggregation with an added [SliceStartExpr] group key,
/// which this operator removes, so its output schema is the original one.
pub struct SliceAggExec {
    input: Arc<dyn ExecutionPlan>,
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
    slice_column: SliceColumn,
    ts_name: String,
    ts_unit: TimeUnit,
    width: i64,
    slices: Vec<(i64, i64)>,
    identity: PlanDigest,
    store: Arc<SliceCacheStore>,
    /// Shared by the partitions of one execution.
    state: Mutex<Option<Arc<ExecState>>>,
    metrics: ExecutionPlanMetricsSet,
}

struct ExecState {
    identity: PlanDigest,
    store: Arc<SliceCacheStore>,
    /// Cached states, emitted by partition 0.
    hits: Mutex<Vec<RecordBatch>>,
    hit_slices: HashSet<i64>,
    /// Slices to cache once every partition has finished, by slice start.
    pending: HashMap<i64, (SliceKey, TimeRangeFingerprint)>,
    /// States handed over by the partitions that finished.
    collected: Mutex<Vec<Collected>>,
    collected_bytes: AtomicUsize,
    /// The states exceeded [SliceCacheStore::max_collect_bytes].
    overflow: AtomicBool,
    running: AtomicUsize,
    /// Nothing is cached: the query memory pool refused the states.
    failed: AtomicBool,
}

/// States of slices to cache, slice column included, collected by one partition.
struct Collected {
    batches: Vec<RecordBatch>,
    /// Holds the size of `batches` in the query memory pool until they are cached.
    reservation: MemoryReservation,
}

impl Collected {
    fn clear(&mut self) {
        self.batches.clear();
        self.reservation.free();
    }
}

/// The output column of the partial aggregation that gives the slice of each group.
#[derive(Clone, Copy)]
struct SliceColumn {
    index: usize,
    width: i64,
}

impl SliceColumn {
    fn starts(&self, batch: &RecordBatch) -> Result<Vec<i64>> {
        let values = cast(batch.column(self.index), &DataType::Int64)?;
        values
            .as_primitive::<Int64Type>()
            .iter()
            .map(|v| match v {
                Some(v) => Ok(v.div_euclid(self.width) * self.width),
                None => internal_err!("slice column of a row is null"),
            })
            .collect()
    }

    fn strip(&self, batch: &RecordBatch, schema: &SchemaRef) -> Result<RecordBatch> {
        let mut columns = batch.columns().to_vec();
        columns.remove(self.index);
        Ok(RecordBatch::try_new(schema.clone(), columns)?)
    }
}

impl ExecState {
    /// Splits a batch of `input` by slice, drops cached slices and the slice column, and
    /// keeps the states of slices to cache in `collected`.
    fn split(
        &self,
        batch: RecordBatch,
        slice_column: SliceColumn,
        schema: &SchemaRef,
        collected: &mut Collected,
    ) -> Result<Vec<RecordBatch>> {
        let mut rows: HashMap<i64, Vec<u32>> = HashMap::new();
        for (row, start) in slice_column.starts(&batch)?.into_iter().enumerate() {
            if !self.hit_slices.contains(&start) {
                rows.entry(start).or_default().push(row as u32);
            }
        }
        let mut outputs = Vec::with_capacity(rows.len());
        for (start, indices) in rows {
            let part = if indices.len() == batch.num_rows() {
                batch.clone()
            } else {
                take_record_batch(&batch, &UInt32Array::from(indices))?
            };
            if self.pending.contains_key(&start) {
                self.collect(part.clone(), collected);
            }
            outputs.push(slice_column.strip(&part, schema)?);
        }
        Ok(outputs)
    }

    /// Keeps a batch of states to cache. Collecting stops for the whole execution once
    /// the states exceed [SliceCacheStore::max_collect_bytes] or the memory pool refuses
    /// them; the query goes on either way.
    fn collect(&self, part: RecordBatch, collected: &mut Collected) {
        if self.overflow.load(Ordering::Relaxed) || self.failed.load(Ordering::Relaxed) {
            collected.clear();
            return;
        }
        let size = part.get_array_memory_size();
        if self.collected_bytes.fetch_add(size, Ordering::Relaxed) + size
            > self.store.max_collect_bytes
        {
            self.overflow.store(true, Ordering::Relaxed);
            collected.clear();
            return;
        }
        if collected.reservation.try_grow(size).is_err() {
            self.failed.store(true, Ordering::Relaxed);
            collected.clear();
            return;
        }
        collected.batches.push(part);
    }

    /// Called when a partition ran to its end. The last one hands the collected states to
    /// a background task that caches them, so the query does not wait for it; a failure
    /// there only costs later hits. A cancelled or failed partition never gets here, so
    /// `running` stays above zero and its execution caches nothing.
    fn finish_partition(
        self: &Arc<Self>,
        collected: Collected,
        partial: Arc<dyn ExecutionPlan>,
        slice_column: SliceColumn,
        schema: SchemaRef,
        context: Arc<TaskContext>,
    ) {
        if !collected.batches.is_empty() {
            self.collected.lock().unwrap().push(collected);
        }
        if self.running.fetch_sub(1, Ordering::SeqCst) != 1 || self.failed.load(Ordering::SeqCst) {
            return;
        }
        if self.overflow.load(Ordering::SeqCst) {
            self.store.oversized.insert(self.identity, ());
            return;
        }
        let mut batches = Vec::new();
        let mut reservations = Vec::new();
        for collected in std::mem::take(&mut *self.collected.lock().unwrap()) {
            batches.extend(collected.batches);
            reservations.push(collected.reservation);
        }
        let state = self.clone();
        let _handle = spawn_global(async move {
            if let Err(e) = state
                .cache_states(batches, &partial, slice_column, &schema, context)
                .await
            {
                warn!("Failed to cache slice aggregate states: {e}");
            }
            drop(reservations);
        });
    }

    /// Every partition emits its own states for a group, so the states are first merged
    /// by a `PartialReduce` aggregation over the slice-grouped partial aggregation.
    async fn cache_states(
        &self,
        batches: Vec<RecordBatch>,
        partial: &Arc<dyn ExecutionPlan>,
        slice_column: SliceColumn,
        schema: &SchemaRef,
        context: Arc<TaskContext>,
    ) -> Result<()> {
        let Some(partial) = partial.downcast_ref::<AggregateExec>() else {
            return internal_err!("SliceAggExec input is not an aggregation");
        };
        let mut by_slice: HashMap<i64, Vec<RecordBatch>> = HashMap::new();
        if !batches.is_empty() {
            let states = MemorySourceConfig::try_new_exec(&[batches], partial.schema(), None)?;
            let reduce = AggregateExec::try_new(
                AggregateMode::PartialReduce,
                partial.group_expr().as_final(),
                partial.aggr_expr().to_vec(),
                vec![None; partial.aggr_expr().len()],
                states,
                partial.input_schema(),
            )?;
            for batch in collect(Arc::new(reduce), context).await? {
                let mut rows: HashMap<i64, Vec<u32>> = HashMap::new();
                for (row, start) in slice_column.starts(&batch)?.into_iter().enumerate() {
                    rows.entry(start).or_default().push(row as u32);
                }
                for (start, indices) in rows {
                    let part = take_record_batch(&batch, &UInt32Array::from(indices))?;
                    by_slice
                        .entry(start)
                        .or_default()
                        .push(slice_column.strip(&part, schema)?);
                }
            }
        }
        for (start, (key, fingerprint)) in &self.pending {
            self.store.entries.insert(
                key.clone(),
                Arc::new(SliceEntry {
                    fingerprint: fingerprint.clone(),
                    batches: by_slice.remove(start).unwrap_or_default(),
                }),
            );
        }
        self.store.entries.run_pending_tasks();
        self.store.report_bytes();
        Ok(())
    }
}

impl SliceAggExec {
    fn timestamp_literal(&self, value: i64, data_type: &DataType) -> Arc<dyn PhysicalExpr> {
        let tz = match data_type {
            DataType::Timestamp(_, tz) => tz.clone(),
            _ => None,
        };
        let v = Some(value);
        Arc::new(Literal::new(match self.ts_unit {
            TimeUnit::Second => ScalarValue::TimestampSecond(v, tz),
            TimeUnit::Millisecond => ScalarValue::TimestampMillisecond(v, tz),
            TimeUnit::Microsecond => ScalarValue::TimestampMicrosecond(v, tz),
            TimeUnit::Nanosecond => ScalarValue::TimestampNanosecond(v, tz),
        }))
    }

    fn init(&self) -> Result<Arc<ExecState>> {
        let mut guard = self.state.lock().unwrap();
        if let Some(state) = guard.as_ref() {
            return Ok(state.clone());
        }
        let Some(scan) = find_scan(self.input.children()[0]) else {
            return internal_err!("SliceAggExec lost its region scan");
        };

        let mut hits = Vec::new();
        let mut hit_slices = HashSet::new();
        let mut pending = HashMap::new();
        let mut stale = 0;
        let mut misses: Vec<(i64, i64)> = Vec::new();
        for &(lo, hi) in &self.slices {
            let start = lo.div_euclid(self.width) * self.width;
            let key = SliceKey {
                plan: self.identity,
                start: lo,
                end: hi,
            };
            if let Some(fingerprint) = scan.time_range_fingerprint(
                Timestamp::new(lo, self.ts_unit),
                Timestamp::new(hi, self.ts_unit),
            ) {
                if let Some(entry) = self.store.entries.get(&key) {
                    if entry.fingerprint == fingerprint {
                        hits.extend(entry.batches.iter().cloned());
                        hit_slices.insert(start);
                        continue;
                    }
                    stale += 1;
                }
                pending.insert(start, (key, fingerprint));
            }
            match misses.last_mut() {
                Some(last) if last.1 == lo => last.1 = hi,
                _ => misses.push((lo, hi)),
            }
        }

        // Rows of cached slices are dropped after aggregation; this filter only lets the
        // scan skip them.
        if !hit_slices.is_empty() {
            let schema = scan.schema();
            let index = schema.index_of(&self.ts_name)?;
            let data_type = schema.field(index).data_type();
            let column: Arc<dyn PhysicalExpr> = Arc::new(Column::new(&self.ts_name, index));
            let predicate = misses
                .iter()
                .map(|&(lo, hi)| -> Arc<dyn PhysicalExpr> {
                    let ge = Arc::new(BinaryExpr::new(
                        column.clone(),
                        Operator::GtEq,
                        self.timestamp_literal(lo, data_type),
                    ));
                    let lt = Arc::new(BinaryExpr::new(
                        column.clone(),
                        Operator::Lt,
                        self.timestamp_literal(hi, data_type),
                    ));
                    Arc::new(BinaryExpr::new(ge, Operator::And, lt))
                })
                .reduce(|a, b| Arc::new(BinaryExpr::new(a, Operator::Or, b)))
                .unwrap_or_else(|| Arc::new(Literal::new(ScalarValue::Boolean(Some(false)))));
            let filter = Arc::new(DynamicFilterPhysicalExpr::new(
                vec![column],
                Arc::new(Literal::new(ScalarValue::Boolean(Some(true)))),
            ));
            filter.update(predicate)?;
            scan.add_dyn_filters_to_predicate(vec![filter]);
        }

        self.store.report_bytes();

        // `uncacheable` slices have no fingerprint, `stale` ones a cached entry whose
        // fingerprint differs, and `absent` ones no entry.
        let uncacheable = self.slices.len() - hit_slices.len() - pending.len();
        for (name, result, count) in [
            ("slice_hits", "hit", hit_slices.len()),
            ("slice_stale", "stale", stale),
            ("slice_absent", "absent", pending.len() - stale),
            ("slice_uncacheable", "uncacheable", uncacheable),
        ] {
            MetricBuilder::new(&self.metrics)
                .global_counter(name)
                .add(count);
            SLICE_AGG_CACHE_SLICES_TOTAL
                .with_label_values(&[result])
                .inc_by(count as u64);
        }

        let state = Arc::new(ExecState {
            identity: self.identity,
            store: self.store.clone(),
            hits: Mutex::new(hits),
            hit_slices,
            pending,
            collected: Mutex::new(Vec::new()),
            collected_bytes: AtomicUsize::new(0),
            overflow: AtomicBool::new(false),
            running: AtomicUsize::new(self.properties.partitioning.partition_count()),
            failed: AtomicBool::new(false),
        });
        *guard = Some(state.clone());
        Ok(state)
    }
}

impl fmt::Debug for SliceAggExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SliceAggExec")
            .field("slices", &self.slices.len())
            .finish()
    }
}

impl DisplayAs for SliceAggExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "SliceAggExec: slices={}, width={}",
            self.slices.len(),
            self.width
        )
    }
}

impl ExecutionPlan for SliceAggExec {
    fn name(&self) -> &str {
        "SliceAggExec"
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self {
            input: children[0].clone(),
            schema: self.schema.clone(),
            properties: self.properties.clone(),
            slice_column: self.slice_column,
            ts_name: self.ts_name.clone(),
            ts_unit: self.ts_unit,
            width: self.width,
            slices: self.slices.clone(),
            identity: self.identity,
            store: self.store.clone(),
            state: Mutex::new(None),
            metrics: ExecutionPlanMetricsSet::new(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let elapsed_compute = MetricBuilder::new(&self.metrics).elapsed_compute(partition);
        let timer = elapsed_compute.timer();
        let state = self.init()?;
        timer.done();
        let hits = if partition == 0 {
            std::mem::take(&mut *state.hits.lock().unwrap())
        } else {
            Vec::new()
        };
        let mut input = self.input.execute(partition, context.clone())?;
        let partial = self.input.clone();
        let slice_column = self.slice_column;
        let schema = self.schema.clone();
        let mut collected = Collected {
            batches: Vec::new(),
            reservation: MemoryConsumer::new(format!("SliceAggExec[{partition}]"))
                .register(context.memory_pool()),
        };
        let stream = async_stream::try_stream! {
            for batch in hits {
                yield batch;
            }
            while let Some(batch) = input.next().await {
                let timer = elapsed_compute.timer();
                let outputs = state.split(batch?, slice_column, &schema, &mut collected)?;
                timer.done();
                for output in outputs {
                    yield output;
                }
            }
            state.finish_partition(collected, partial, slice_column, schema, context);
        };
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema.clone(),
            stream,
        )))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::Field;
    use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryPool};

    use super::*;

    fn exec_state() -> ExecState {
        ExecState {
            identity: [0; 32],
            store: Arc::new(SliceCacheStore::new(&QueryOptions::default())),
            hits: Mutex::new(Vec::new()),
            hit_slices: HashSet::new(),
            pending: HashMap::new(),
            collected: Mutex::new(Vec::new()),
            collected_bytes: AtomicUsize::new(0),
            overflow: AtomicBool::new(false),
            running: AtomicUsize::new(1),
            failed: AtomicBool::new(false),
        }
    }

    fn batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1; 1024]))]).unwrap()
    }

    /// A memory pool that refuses the states stops caching for the execution without
    /// failing the query, and releases what was reserved.
    #[test]
    fn test_collect_gives_up_when_memory_pool_refuses() {
        let state = exec_state();
        let pool: Arc<dyn MemoryPool> =
            Arc::new(GreedyMemoryPool::new(batch().get_array_memory_size() + 1));
        let mut collected = Collected {
            batches: Vec::new(),
            reservation: MemoryConsumer::new("test").register(&pool),
        };
        state.collect(batch(), &mut collected);
        assert_eq!(1, collected.batches.len());
        assert!(!state.failed.load(Ordering::SeqCst));

        state.collect(batch(), &mut collected);
        assert!(state.failed.load(Ordering::SeqCst));
        assert!(collected.batches.is_empty());
        assert_eq!(0, pool.reserved());
    }
}
