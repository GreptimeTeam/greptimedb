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

//! Reuse of analyzed and optimized logical plans for PromQL range and instant
//! selectors over a single data source.
//!
//! Only the plan *shape* is retained. A cached template holds no table
//! provider, no snapshot, no execution state and no request-scoped UDF; every
//! hit rebinds the evaluation bounds, the generated time filters and the table
//! source of the current request before physical planning runs.

use std::any::Any;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, LazyLock};

use datafusion::arrow::datatypes::Fields;
use datafusion::datasource::empty::EmptyTable;
use datafusion::datasource::{DefaultTableSource, provider_as_source};
use datafusion::execution::SessionState;
use datafusion::functions_aggregate::average::avg_udaf;
use datafusion::functions_aggregate::count::count_udaf;
use datafusion::functions_aggregate::min_max::{max_udaf, min_udaf};
use datafusion::functions_aggregate::sum::sum_udaf;
use datafusion::functions_window::row_number::RowNumber;
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion_common::{DataFusionError, Result, ScalarValue};
use datafusion_expr::expr::WindowFunctionDefinition;
use datafusion_expr::{
    AggregateUDF, Expr, Extension, LogicalPlan, Operator, ScalarUDF, TableSource,
    UserDefinedLogicalNodeCore, WindowFrameBound, WindowUDF,
};
use datafusion_optimizer::{OptimizerContext, OptimizerRule};
use moka::future::Cache;
use promql::extension_plan::{InstantManipulate, RangeManipulate, SeriesDivide, SeriesNormalize};
use promql::functions::{
    AbsentOverTime, AvgOverTime, Changes, CountOverTime, Delta, Deriv, IDelta, Increase,
    LastOverTime, MaxOverTime, MinOverTime, PresentOverTime, Rate, Resets, StddevOverTime,
    StdvarOverTime, SumOverTime,
};
use session::context::QueryContext;
use store_api::metadata::{RegionMetadata, RegionMetadataRef};
use table::metadata::TableInfoRef;
use table::table::adapter::DfTableProviderAdapter;

use crate::dist_plan::MergeScanLogicalPlan;
use crate::dummy_catalog::DummyTableProvider;
use crate::metrics::PROMQL_PLAN_CACHE_ENTRIES;
use crate::optimizer::scan_hint::ScanHintRule;

#[cfg(test)]
mod tests;

/// Admission limits; exceeding one skips caching, it never rejects the query.
/// Nodes follow the query shape (measured 5 to 15), expressions and bytes grow
/// with table width by about 4 expressions and 1.2KiB per tag column, so a wide
/// table stops being cached at roughly 60 tag columns.
const MAX_NODES: usize = 64;
const MAX_EXPRESSIONS: usize = 256;
/// Estimated structural bytes, not a heap limit: 512 entries measured ~18MiB.
const MAX_BYTES: usize = 256 * 1024;

/// The float range functions a template may contain. Admitted occurrences are
/// replaced by these process-wide instances, so a template can never retain a
/// request's annotation collector or any other per-request closure.
///
/// This is the complete set of float range functions the PromQL planner builds
/// without an extra parameter, not a hand-picked subset: they all share one
/// safety argument, so leaving any of them out would be arbitrary. Functions
/// taking a parameter (`quantile_over_time`, `predict_linear`, ...) are out
/// until their literal handling is covered by a test.
///
/// Only the float implementations belong here. Their native-histogram
/// counterparts are built with `scalar_udf_with_collector` and carry a
/// per-request collector; they live in a separate `prom_native_histogram_*`
/// name space, so matching on name and signature cannot confuse the two.
static RANGE_FUNCTIONS: LazyLock<HashMap<String, Arc<ScalarUDF>>> = LazyLock::new(|| {
    [
        Arc::new(Rate::scalar_udf()),
        Arc::new(Increase::scalar_udf()),
        Arc::new(Delta::scalar_udf()),
        Arc::new(IDelta::<true>::scalar_udf()),
        Arc::new(IDelta::<false>::scalar_udf()),
        Arc::new(Resets::scalar_udf()),
        Arc::new(Changes::scalar_udf()),
        Arc::new(Deriv::scalar_udf()),
        Arc::new(AvgOverTime::scalar_udf()),
        Arc::new(MinOverTime::scalar_udf()),
        Arc::new(MaxOverTime::scalar_udf()),
        Arc::new(SumOverTime::scalar_udf()),
        Arc::new(CountOverTime::scalar_udf()),
        Arc::new(LastOverTime::scalar_udf()),
        Arc::new(AbsentOverTime::scalar_udf()),
        Arc::new(PresentOverTime::scalar_udf()),
        Arc::new(StddevOverTime::scalar_udf()),
        Arc::new(StdvarOverTime::scalar_udf()),
    ]
    .into_iter()
    .map(|func| (func.name().to_string(), func))
    .collect()
});

/// The only window function an admitted plan may contain.
static ROW_NUMBER: LazyLock<WindowUDF> = LazyLock::new(|| RowNumber::new().into());

/// The aggregate functions a template may contain, all of them DataFusion
/// process-wide instances that the PromQL planner calls directly. Unlike the
/// range functions these are kept as-is: there is no per-request variant to
/// canonicalize away.
static AGGREGATE_FUNCTIONS: LazyLock<HashMap<String, Arc<AggregateUDF>>> = LazyLock::new(|| {
    [sum_udaf(), avg_udaf(), max_udaf(), min_udaf(), count_udaf()]
        .into_iter()
        .map(|func| (func.name().to_string(), func))
        .collect()
});

fn canonical_range_function(func: &ScalarUDF) -> Option<Arc<ScalarUDF>> {
    RANGE_FUNCTIONS
        .get(func.name())
        .filter(|allowed| allowed.signature() == func.signature())
        .cloned()
}

fn is_allowed_aggregate_function(func: &Arc<AggregateUDF>) -> bool {
    // The name only selects the candidate; admission rests on full equality.
    AGGREGATE_FUNCTIONS
        .get(func.name())
        .is_some_and(|allowed| allowed == func)
}

/// Identity of the data the template was planned against.
///
/// `TableScan` equality ignores its source, so the metadata that planning
/// depends on has to be compared separately. A schema change, an alter or a
/// drop-and-recreate produces different metadata and therefore a different key.
#[derive(Clone)]
enum Dependency {
    Table(TableInfoRef),
    Region(RegionMetadataRef),
}

impl Dependency {
    fn from_source(source: &Arc<dyn TableSource>) -> Option<Self> {
        let source: &dyn Any = source.as_ref();
        let provider: &dyn Any = source
            .downcast_ref::<DefaultTableSource>()?
            .table_provider
            .as_ref();
        if let Some(table) = provider.downcast_ref::<DfTableProviderAdapter>() {
            Some(Self::Table(table.table().table_info()))
        } else {
            let region = provider.downcast_ref::<DummyTableProvider>()?;
            Some(Self::Region(region.region_metadata()))
        }
    }
}

/// Whether two regions plan the same.
///
/// Planning reads exactly three things out of the region metadata, and all
/// three come from `column_metadatas`: the Arrow schema, which is derived from
/// it, the semantic type behind `DummyTableProvider::schema`, and the one
/// behind its `supports_filters_pushdown`. `primary_key` and
/// `primary_key_encoding` are read on the scan path rather than the planning
/// path, but they describe the same table shape and cost nothing to compare,
/// so they are included.
///
/// `region_id`, `partition_expr` and its version are deliberately excluded. A
/// datanode holds many regions of one table; they receive a byte-identical
/// pushed-down plan and plan it identically, and the region a hit actually
/// reads through comes from the request's own table source, which `rebind`
/// installs. Sharing one template across them keeps the entry count
/// proportional to the number of query shapes rather than to the number of
/// regions, which is what the `size` option has to be dimensioned for.
fn regions_plan_alike(left: &RegionMetadata, right: &RegionMetadata) -> bool {
    left.schema_version == right.schema_version
        && left.primary_key_encoding == right.primary_key_encoding
        && left.primary_key == right.primary_key
        && left.column_metadatas == right.column_metadatas
}

impl PartialEq for Dependency {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            // Repeated requests normally observe the same metadata instance;
            // the deep comparison is the fallback, not the common path.
            (Self::Table(left), Self::Table(right)) => Arc::ptr_eq(left, right) || left == right,
            (Self::Region(left), Self::Region(right)) => {
                Arc::ptr_eq(left, right) || regions_plan_alike(left, right)
            }
            _ => false,
        }
    }
}

impl Eq for Dependency {}

impl std::fmt::Display for Dependency {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Table(info) => write!(f, "table {}", info.ident.table_id),
            Self::Region(metadata) => write!(f, "region {}", metadata.region_id),
        }
    }
}

impl Hash for Dependency {
    /// Hashes a subset of what equality compares, cheap enough to run on every
    /// lookup. It must not reach `region_id`: regions of one table share a
    /// template, so hashing their identity would put each of them in its own
    /// bucket and none of them would ever find the shared entry.
    fn hash<H: Hasher>(&self, state: &mut H) {
        std::mem::discriminant(self).hash(state);
        match self {
            Self::Table(info) => {
                info.ident.table_id.hash(state);
                info.ident.version.hash(state);
            }
            Self::Region(metadata) => {
                metadata.schema_version.hash(state);
                metadata.primary_key.hash(state);
                metadata.column_metadatas.len().hash(state);
            }
        }
    }
}

/// Everything the reused plan was derived from, besides the data itself.
#[derive(Clone, PartialEq, Eq)]
struct Key {
    /// Raw plan with all evaluation bounds and generated time filters shifted
    /// to a zero start, and with the table source detached.
    plan: LogicalPlan,
    dependency: Dependency,
    /// Session configuration, including DataFusion extension options.
    options: Vec<(String, Option<String>)>,
    /// Query context values that planning can observe.
    scope: Vec<(String, String)>,
}

impl Hash for Key {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.plan.hash(state);
        self.options.hash(state);
        self.scope.hash(state);
        self.dependency.hash(state);
    }
}

/// A request that may reuse or fill a template.
pub(crate) struct Candidate {
    key: Key,
    /// Request evaluation start in Unix milliseconds, before selector offset
    /// or lookback. Subtracted when normalizing and added back on cache hits.
    start: i64,
    source: Arc<dyn TableSource>,
}

/// Bounded store of PromQL logical plan templates.
pub(crate) struct PromqlPlanCache {
    entries: Cache<Key, Arc<LogicalPlan>>,
}

impl PromqlPlanCache {
    pub(crate) fn new(capacity: u64) -> Self {
        Self {
            entries: Cache::builder()
                .max_capacity(capacity)
                // Eviction and replacement are reported during housekeeping, so
                // the gauge can briefly read high; it is an occupancy signal,
                // not an exact count.
                .eviction_listener(|_, _, _| PROMQL_PLAN_CACHE_ENTRIES.dec())
                .build(),
        }
    }

    /// Describes the request if its plan is a supported single-source PromQL
    /// shape, otherwise returns `None` so the caller keeps the normal path.
    pub(crate) fn candidate(
        &self,
        plan: &LogicalPlan,
        state: &SessionState,
        ctx: &QueryContext,
    ) -> Option<Candidate> {
        let shape = Shape::inspect(plan)?;
        let source = shape.source?;
        let dependency = Dependency::from_source(&source)?;
        let start = shape.start?;
        let detached = provider_as_source(Arc::new(EmptyTable::new(source.schema())));
        let normalized = rebind(plan.clone(), start.checked_neg()?, &detached, false).ok()?;

        let mut options = state
            .config_options()
            .entries()
            .into_iter()
            .map(|entry| (entry.key, entry.value))
            .collect::<Vec<_>>();
        options.sort();

        let scope = ctx.plan_cache_scope();

        let size = options
            .iter()
            .map(|(key, value)| key.len() + value.as_ref().map_or(0, String::len))
            .sum::<usize>()
            + scope
                .iter()
                .map(|(key, value)| key.len() + value.len())
                .sum::<usize>();
        if size > MAX_BYTES {
            return None;
        }

        Some(Candidate {
            key: Key {
                plan: normalized,
                dependency,
                options,
                scope,
            },
            start,
            source,
        })
    }

    /// Returns the template rebound to this request, if one is cached.
    pub(crate) async fn get(&self, candidate: &Candidate) -> Result<Option<LogicalPlan>> {
        let Some(template) = self.entries.get(&candidate.key).await else {
            common_telemetry::debug!(
                "PromQL plan cache miss on {}, evaluation start {}",
                candidate.key.dependency,
                candidate.start
            );
            return Ok(None);
        };
        let plan = rebind(
            template.as_ref().clone(),
            candidate.start,
            &candidate.source,
            true,
        )?;
        common_telemetry::debug!(
            "PromQL plan cache hit on {}, evaluation start {}",
            candidate.key.dependency,
            candidate.start
        );
        Ok(Some(plan))
    }

    /// Stores the optimized plan as a template. Returns whether it was stored.
    pub(crate) async fn insert(&self, candidate: Candidate, optimized: &LogicalPlan) -> bool {
        let Some(shape) = Shape::inspect(optimized) else {
            return false;
        };
        let Some(source) = shape.source else {
            return false;
        };
        // Analysis and optimization must not have moved the evaluation bounds
        // or swapped the data the plan reads.
        if shape.start != Some(candidate.start)
            || Dependency::from_source(&source).as_ref() != Some(&candidate.key.dependency)
        {
            return false;
        }
        let Some(delta) = candidate.start.checked_neg() else {
            return false;
        };
        let detached = provider_as_source(Arc::new(EmptyTable::new(source.schema())));
        let Ok(template) = rebind(optimized.clone(), delta, &detached, false) else {
            return false;
        };
        common_telemetry::debug!(
            "Caching PromQL plan template for {}, evaluation start {}",
            candidate.key.dependency,
            candidate.start
        );
        self.entries.insert(candidate.key, Arc::new(template)).await;
        // After the insert, so a cancelled request cannot leave the gauge high.
        PROMQL_PLAN_CACHE_ENTRIES.inc();
        true
    }

    #[cfg(test)]
    async fn entry_count(&self) -> u64 {
        self.entries.run_pending_tasks().await;
        self.entries.entry_count()
    }
}

/// Admission check over a logical plan.
///
/// A plan is admitted only when every node, expression and literal it contains
/// is understood well enough to be rebound: one PromQL evaluation node over one
/// table source, and time literals that all belong to time-index predicates.
#[derive(Default)]
struct Shape {
    nodes: usize,
    expressions: usize,
    bytes: usize,
    ranges: usize,
    instants: usize,
    normalizers: usize,
    dividers: usize,
    timestamps: usize,
    start: Option<i64>,
    time_column: Option<String>,
    source: Option<Arc<dyn TableSource>>,
}

impl Shape {
    fn inspect(plan: &LogicalPlan) -> Option<Self> {
        let mut shape = Self::default();
        shape.visit(plan)?;
        // Rebinding can only bind one evaluation origin over one source with one
        // series split. A normalizer is mandatory for ranges and offset-only for
        // instants, and the time literals are the selector's scan bounds.
        (shape.ranges + shape.instants == 1
            && (shape.normalizers == 1 || (shape.instants == 1 && shape.normalizers == 0))
            && shape.dividers == 1
            && shape.timestamps >= 2
            && shape.source.is_some())
        .then_some(shape)
    }

    fn visit(&mut self, plan: &LogicalPlan) -> Option<()> {
        self.nodes += 1;
        if self.nodes > MAX_NODES {
            return None;
        }
        self.bytes += schema_size(plan.schema().fields(), plan.schema().metadata());
        if self.bytes > MAX_BYTES {
            return None;
        }
        match plan {
            LogicalPlan::TableScan(scan) => {
                if self.source.is_some() {
                    return None;
                }
                let source_schema = scan.source.schema();
                self.bytes += schema_size(source_schema.fields(), source_schema.metadata());
                self.bytes += scan.table_name.to_string().len();
                self.source = Some(scan.source.clone());
            }
            LogicalPlan::Projection(_)
            | LogicalPlan::Filter(_)
            | LogicalPlan::Aggregate(_)
            | LogicalPlan::Window(_)
            | LogicalPlan::Sort(_) => {}
            LogicalPlan::SubqueryAlias(alias) => {
                self.bytes += alias.alias.to_string().len();
            }
            LogicalPlan::Extension(ext) => {
                if let Some(range) = ext.node.as_any().downcast_ref::<RangeManipulate>() {
                    self.ranges += 1;
                    self.start = Some(range.time_bounds().0);
                    let Expr::Column(column) = range.expressions().first()?.clone() else {
                        return None;
                    };
                    self.time_column = Some(column.name);
                } else if let Some(instant) = ext.node.as_any().downcast_ref::<InstantManipulate>()
                {
                    self.instants += 1;
                    self.start = Some(instant.time_bounds().0);
                    let Expr::Column(column) = instant.expressions().first()?.clone() else {
                        return None;
                    };
                    self.time_column = Some(column.name);
                } else if ext.node.as_any().is::<SeriesNormalize>() {
                    self.normalizers += 1;
                } else if ext.node.as_any().is::<SeriesDivide>() {
                    self.dividers += 1;
                } else if let Some(remote) =
                    ext.node.as_any().downcast_ref::<MergeScanLogicalPlan>()
                {
                    // `MergeScanLogicalPlan` hides its input from `inputs()`.
                    self.visit(remote.input())?;
                } else {
                    return None;
                }
            }
            _ => return None,
        }
        // The evaluation node is visited before its input, so the time index
        // column is known by the time generated selector bounds are checked.
        for input in plan.inputs() {
            self.visit(input)?;
        }
        let carries_time_filter =
            matches!(plan, LogicalPlan::Filter(_) | LogicalPlan::TableScan(_));
        for expr in plan.expressions() {
            self.expression(&expr, carries_time_filter, false)?;
        }
        (self.bytes <= MAX_BYTES).then_some(())
    }

    /// `filter` marks expressions that may carry generated selector bounds;
    /// `timestamp` marks the operand of a comparison against the time index,
    /// which is the only position where a rebindable timestamp may appear.
    fn expression(&mut self, expr: &Expr, filter: bool, timestamp: bool) -> Option<()> {
        self.expressions += 1;
        if self.expressions > MAX_EXPRESSIONS {
            return None;
        }
        match expr {
            Expr::Literal(value, metadata) => {
                self.bytes += value.size();
                // The size estimate above does not cover literal metadata.
                if metadata.is_some() {
                    return None;
                }
                match value {
                    ScalarValue::TimestampMillisecond(Some(_), None) if timestamp => {
                        self.timestamps += 1;
                    }
                    ScalarValue::Null
                    | ScalarValue::Boolean(_)
                    | ScalarValue::Float64(_)
                    | ScalarValue::Float32(_)
                    | ScalarValue::Int64(_)
                    | ScalarValue::UInt64(_)
                    | ScalarValue::Int32(_)
                    | ScalarValue::UInt32(_)
                    | ScalarValue::Utf8(_)
                    | ScalarValue::LargeUtf8(_)
                    | ScalarValue::Utf8View(_) => {}
                    ScalarValue::Dictionary(key, value)
                        if key.as_ref() == &datafusion::arrow::datatypes::DataType::UInt32
                            && matches!(value.as_ref(), ScalarValue::Utf8(_)) => {}
                    // Includes every timestamp outside a time-index predicate:
                    // shifting one of those would change what the query asks.
                    _ => return None,
                }
            }
            Expr::Column(column) => {
                self.bytes += column.to_string().len();
            }
            Expr::Alias(alias) => {
                self.bytes += alias.name.len();
            }
            Expr::BinaryExpr(binary) => {
                let comparison = matches!(
                    binary.op,
                    Operator::Gt | Operator::GtEq | Operator::Lt | Operator::LtEq | Operator::Eq
                );
                let is_time = |expr: &Expr| matches!(expr, Expr::Column(column) if Some(&column.name) == self.time_column.as_ref());
                let left_is_bound = filter && comparison && is_time(&binary.right);
                let right_is_bound = filter && comparison && is_time(&binary.left);
                // The key is built before analysis and optimization, which can
                // fold a string, a cast or an arithmetic expression here into a
                // timestamp literal. `rebind` shifts every millisecond literal
                // it finds, so a bound the key never saw would move with the
                // request and silently change what the query asks for.
                if (left_is_bound && !is_bindable_timestamp(&binary.left))
                    || (right_is_bound && !is_bindable_timestamp(&binary.right))
                {
                    return None;
                }
                self.expression(&binary.left, filter, left_is_bound)?;
                self.expression(&binary.right, filter, right_is_bound)?;
                return (self.bytes <= MAX_BYTES).then_some(());
            }
            Expr::ScalarFunction(function) => {
                canonical_range_function(&function.func)?;
            }
            Expr::WindowFunction(window) => {
                // `row_number` is the only window function the PromQL planner
                // emits (the `topk`/`bottomk` rewrite). It is stateless, so the
                // template may keep the instance.
                let WindowFunctionDefinition::WindowUDF(udwf) = &window.fun else {
                    return None;
                };
                if udwf.name() != ROW_NUMBER.name() || udwf.signature() != ROW_NUMBER.signature() {
                    return None;
                }
                // `apply_children` does not descend into the frame, so its
                // bounds are neither checked below nor shifted by `rebind`. A
                // bound that moved with the request would therefore key a new
                // entry on every query instead of reusing one.
                let frame = &window.params.window_frame;
                if !stable_frame_bound(frame.start_bound.clone())
                    || !stable_frame_bound(frame.end_bound.clone())
                {
                    return None;
                }
            }
            Expr::AggregateFunction(function) => {
                // The PromQL planner builds these from DataFusion's process-wide
                // instances, so a template that keeps one holds no request state.
                if !is_allowed_aggregate_function(&function.func) {
                    return None;
                }
            }
            Expr::Cast(_)
            | Expr::IsNull(_)
            | Expr::IsNotNull(_)
            | Expr::Not(_)
            | Expr::Negative(_) => {}
            _ => return None,
        }
        let mut admitted = true;
        expr.apply_children(|child| {
            if self.expression(child, filter, false).is_none() {
                admitted = false;
                Ok(TreeNodeRecursion::Stop)
            } else {
                Ok(TreeNodeRecursion::Continue)
            }
        })
        .ok()?;
        (admitted && self.bytes <= MAX_BYTES).then_some(())
    }
}

/// Whether a time-index comparison already holds the literal `rebind` shifts,
/// rather than something that only becomes one later in planning.
fn is_bindable_timestamp(expr: &Expr) -> bool {
    matches!(
        expr,
        Expr::Literal(ScalarValue::TimestampMillisecond(Some(_), None), None)
    )
}

/// Whether a window frame bound is independent of the request's evaluation
/// bounds, so that the same query shape keys the same entry over time.
fn stable_frame_bound(bound: WindowFrameBound) -> bool {
    match bound {
        WindowFrameBound::CurrentRow => true,
        WindowFrameBound::Preceding(value) | WindowFrameBound::Following(value) => matches!(
            value,
            ScalarValue::Null
                | ScalarValue::UInt64(_)
                | ScalarValue::Int64(_)
                | ScalarValue::UInt32(_)
                | ScalarValue::Int32(_)
        ),
    }
}

fn schema_size(fields: &Fields, metadata: &HashMap<String, String>) -> usize {
    fields.iter().map(|field| field.size()).sum::<usize>()
        + metadata
            .iter()
            .map(|(key, value)| key.len() + value.len())
            .sum::<usize>()
}

fn shift(value: i64, delta: i64) -> Result<i64> {
    value
        .checked_add(delta)
        .ok_or_else(|| DataFusionError::Plan("PromQL plan template timestamp overflow".into()))
}

/// Moves every evaluation bound and generated time filter of `plan` by `delta`
/// and points its scan at `source`.
///
/// `apply_scan_hints` re-runs [`ScanHintRule`], whose output lives on the table
/// provider rather than in the plan: a template carries no provider, so the
/// hints have to be recomputed against the request's own one. It is the only
/// optimizer rule with that property that an admitted plan can reach, because
/// the JSON rules require scalar functions the admission check rejects.
fn rebind(
    plan: LogicalPlan,
    delta: i64,
    source: &Arc<dyn TableSource>,
    apply_scan_hints: bool,
) -> Result<LogicalPlan> {
    let plan = plan
        .transform_up(|plan| {
            let plan = match plan {
                LogicalPlan::TableScan(mut scan) => {
                    scan.source = source.clone();
                    LogicalPlan::TableScan(scan)
                }
                LogicalPlan::Extension(ext) => {
                    rebind_extension(ext, delta, source, apply_scan_hints)?
                }
                plan => plan,
            };
            let rebound = plan
                .map_expressions(|expr| {
                    expr.transform_up(|expr| match expr {
                        Expr::Literal(
                            ScalarValue::TimestampMillisecond(Some(value), None),
                            metadata,
                        ) => Ok(Transformed::yes(Expr::Literal(
                            ScalarValue::TimestampMillisecond(Some(shift(value, delta)?), None),
                            metadata,
                        ))),
                        Expr::ScalarFunction(mut function) => {
                            if let Some(canonical) = canonical_range_function(&function.func) {
                                function.func = canonical;
                            }
                            Ok(Transformed::yes(Expr::ScalarFunction(function)))
                        }
                        expr => Ok(Transformed::no(expr)),
                    })
                })?
                .data;
            Ok(Transformed::yes(rebound))
        })?
        .data;
    if apply_scan_hints {
        Ok(ScanHintRule.rewrite(plan, &OptimizerContext::new())?.data)
    } else {
        Ok(plan)
    }
}

fn rebind_extension(
    ext: Extension,
    delta: i64,
    source: &Arc<dyn TableSource>,
    apply_scan_hints: bool,
) -> Result<LogicalPlan> {
    if let Some(remote) = ext.node.as_any().downcast_ref::<MergeScanLogicalPlan>() {
        let mut rebound = MergeScanLogicalPlan::new(
            rebind(remote.input().clone(), delta, source, apply_scan_hints)?,
            remote.is_placeholder(),
            remote.partition_cols().clone(),
        )
        .with_output_schema(remote.schema().clone());
        // Producer ids are assigned by tree position, so they are stable for a
        // given plan shape and stay valid across requests.
        if let Some(id) = remote.remote_dyn_filter_producer_id() {
            rebound = rebound.with_remote_dyn_filter_producer_id(id);
        }
        Ok(rebound.into_logical_plan())
    } else if let Some(range) = ext.node.as_any().downcast_ref::<RangeManipulate>() {
        let (start, end) = range.time_bounds();
        Ok(LogicalPlan::Extension(Extension {
            node: Arc::new(range.with_time_bounds(shift(start, delta)?, shift(end, delta)?)),
        }))
    } else if let Some(instant) = ext.node.as_any().downcast_ref::<InstantManipulate>() {
        let (start, end) = instant.time_bounds();
        Ok(LogicalPlan::Extension(Extension {
            node: Arc::new(instant.with_time_bounds(shift(start, delta)?, shift(end, delta)?)),
        }))
    } else {
        Ok(LogicalPlan::Extension(ext))
    }
}
