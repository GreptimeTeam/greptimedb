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

//! The native fused-series route of the PromQL planner.
//!
//! [`PromPlanner::try_plan_native_fused`] offers a whole statement expression to the native
//! compiler and, when the compiler accepts it, replaces everything the existing path would plan
//! below the root with one [`FusedSeries`] node over the selector's own table scan. Declining is
//! the normal answer: every shape the compiler does not accept, and every query this route cannot
//! bind to a scan faithfully, keeps the plan the existing planner builds.
//!
//! What the node needs from here is three things the existing planner already resolves: the raw
//! series schema the tree's input was compiled against (the selector's selected field columns), the
//! statement's evaluation grid, and the logical tag names the output carries. The output schema is
//! built to the shape the existing unary pipeline produces — the series fields, then the tags, then
//! the time index — so a node planned here can be consumed by the same code that consumes a
//! `SeriesNormalize` output.

use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

use datafusion::arrow::datatypes::{DataType as ArrowDataType, Field, TimeUnit as ArrowTimeUnit};
use datafusion::common::{DFSchema, DFSchemaRef};
use datafusion::datasource::DefaultTableSource;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{Extension, LogicalPlan};
use datatypes::series::SeriesOrdering;
use datatypes::series::samples::{FieldSpec, SeriesSchema};
use promql::extension_plan::{FusedSeries, FusedSeriesBinding};
use promql_parser::parser::value::ValueType;
use promql_parser::parser::{
    EvalStmt, Expr as PromExpr, MatrixSelector, ParenExpr, VectorSelector,
};
use session::context::QueryContextRef;
use snafu::ResultExt;
use store_api::storage::TimeSeriesDistribution;

use crate::dummy_catalog::DummyTableProvider;
use crate::promql::error::{DataFusionPlanningSnafu, Result};
use crate::promql::native_compile::{compile, statement_grid};
use crate::promql::planner::PromPlanner;

/// The query-context key that turns the native fused-series route on for one query.
///
/// Named after the engine-wide option it mirrors ([`QueryOptions::experimental_native_promql`]),
/// and read exactly the way the other per-query override in this crate is read
/// ([`remote_dyn_filter_pushdown_enabled_from_extensions`]): a query-context extension, which a
/// client can set per request over the HTTP hints header or as a gRPC context key. The engine-wide
/// option is a query-engine setting that `QueryEngineState` consumes while it is built and does not
/// keep, so it is not readable from the planner's call site; see [`native_promql_requested`].
///
/// [`QueryOptions::experimental_native_promql`]: crate::options::QueryOptions::experimental_native_promql
/// [`remote_dyn_filter_pushdown_enabled_from_extensions`]:
///     crate::options::remote_dyn_filter_pushdown_enabled_from_extensions
pub(crate) const NATIVE_PROMQL_EXTENSION_KEY: &str = "query.experimental_native_promql";

/// Whether `query_ctx` asks for the native fused-series route.
///
/// The route is off unless the query asks for it, so a query that says nothing keeps the plan the
/// existing planner builds. The value is compared literally (`true` in any case) rather than through
/// a shared boolean parser: an unrecognized value leaves the route off, which is the same plan as
/// the flag never having been read, so a typo costs a slow query and never a wrong one.
pub(crate) fn native_promql_requested(query_ctx: &QueryContextRef) -> bool {
    query_ctx
        .extension(NATIVE_PROMQL_EXTENSION_KEY)
        .is_some_and(|value| value.eq_ignore_ascii_case("true"))
}

/// The ordering this route compiles its trees against.
///
/// The logical planner cannot read the ordering a scanner will declare — it is decided when the
/// region scanner is prepared, which happens during physical planning — so the route names the
/// strongest ordering it can assume instead of guessing: `KeyThenTime` orders each series' chunks
/// contiguously *and* orders the series by identity, which `Contiguous` (the weaker declaration a
/// two-phase legacy scan makes) also satisfies. Nothing downstream enforces the tree's declared
/// ordering against the frame's own declaration beyond rejecting `Interleaved`, so a scanner that
/// declares `Contiguous` still feeds a tree compiled here.
const NATIVE_ORDERING: SeriesOrdering = SeriesOrdering::KeyThenTime;

impl PromPlanner {
    /// Plans `prom_expr` as one [`FusedSeries`] node, or returns `None` to keep the existing plan.
    ///
    /// The route is offered a whole expression because the compiler folds a range function and the
    /// math functions over it into one tree: offering a sub-expression instead would compile a tree
    /// that computes something the reader did not write. Everything else about the node comes from
    /// the planner's resolved selector context: the table name, the time index column, the tag
    /// columns and (after the `__field__` matcher) the field columns, which are exactly the raw
    /// series schema the tree's input has to be compiled against.
    ///
    /// `None` is not a failure. It says this query cannot take the route *unchanged*, and the
    /// caller plans it with the existing planner as if this method did not exist. The two ways that
    /// happens are a shape the compiler refuses (`DataFusionError::NotImplemented`, which names
    /// what it found) and a query this route cannot bind faithfully — more than one selector, a
    /// label matcher the bare scan below the node would not apply, an `__field__` matcher that
    /// narrows the field set, a missing table, a native histogram field, or an unusable grid.
    pub(super) async fn try_plan_native_fused(
        &mut self,
        prom_expr: &PromExpr,
    ) -> Result<Option<LogicalPlan>> {
        if !self.native_fused {
            return Ok(None);
        }
        // Only the statement root is offered: the tree is compiled against the statement's grid,
        // and a subquery's expression is evaluated on its own, smaller one. A range-vector root is
        // the instant query's raw sample result, which the existing path reports as a matrix rather
        // than as one point per grid position.
        if prom_expr.value_type() == ValueType::Matrix {
            return Ok(None);
        }

        // A tree reads exactly one raw input, and the scan below the node is that one selector's,
        // so a shape that names no selector or more than one is not offered at all.
        let Some(selector) = single_selector(prom_expr) else {
            return Ok(None);
        };

        // Resolve the selector the way the existing planner does, and take what is left as the
        // rows it selects. The scan below the node is the selector's own scan, unfiltered, so a
        // matcher that survives preprocessing selects fewer rows here than the scan reads.
        let matchers = self.preprocess_label_matchers(&selector.matchers, &selector.name)?;
        if !matchers.matchers.is_empty() {
            return Ok(None);
        }
        // An `__field__` matcher narrows the field columns the existing planner builds its scan
        // with; folding every field of the table instead would evaluate a different window.
        if self.ctx.field_column_matcher.is_some() {
            return Ok(None);
        }
        // A metric without a table reads as an empty metric, which the existing planner plans.
        if self.setup_context().await?.is_some() {
            return Ok(None);
        }

        let Some(table_name) = self.ctx.table_name.clone() else {
            return Ok(None);
        };
        let Some(time_index) = self.ctx.time_index_column.clone() else {
            return Ok(None);
        };
        let fields = self.ctx.field_columns.clone();
        let tags = self.ctx.tag_columns.clone();
        // A series with no field has nothing to fold, and the tree's input would be an empty
        // schema, which no window can read.
        if fields.is_empty() {
            return Ok(None);
        }

        // The selector's own table scan, built by the same method the existing path uses: the
        // physical planner turns it into the region scan the node reads, and building a scan here
        // instead would let the two paths disagree about the columns they read.
        let table_ref = self.table_ref()?;
        let input = self.create_table_scan_plan(table_ref.clone()).await?;
        // The node reads the region scan itself, so the plan below it has to *be* that scan: a
        // filter, a projection or a `__tsid` rewrite in between produces a different physical
        // child, and the physical layer cannot reach through it. Declining here, while the logical
        // plan is still this method's to choose, is what keeps such a query on the existing path.
        let LogicalPlan::TableScan(_) = &input else {
            return Ok(None);
        };
        // A window over a native histogram yields a histogram, which the output schema below
        // cannot declare; the existing path evaluates those with its own kernels.
        if Self::field_columns_contain_native_histogram(input.schema(), &fields) {
            return Ok(None);
        }
        // A raw series frame is produced by the region's series scan, which a region scan only
        // prepares when the request asks for the per-series distribution. Without the hint the
        // region scans by row and has no raw frame to hand over, so the node would be planned with
        // an input that cannot feed it.
        let input = with_per_series_distribution(input);

        let Some(stmt) = self.native_statement(prom_expr) else {
            return Ok(None);
        };
        let Ok(grid) = statement_grid(&stmt) else {
            // The statement's grid is not one a tree can be evaluated on, so there is no grid to
            // address the node's output by.
            return Ok(None);
        };

        let series_schema = Arc::new(SeriesSchema {
            fields: fields
                .iter()
                .map(|name| FieldSpec {
                    name: Arc::from(name.as_str()),
                })
                .collect::<Vec<_>>()
                .into(),
            eval_grid: None,
        });
        let tree = match compile(&stmt, Arc::clone(&series_schema), NATIVE_ORDERING) {
            Ok(tree) => tree,
            // The compiler refusing a shape is this route declining it: the existing planner gets
            // the query exactly as it would have without this flag.
            Err(DataFusionError::NotImplemented(_)) => return Ok(None),
            Err(error) => return Err(error).context(DataFusionPlanningSnafu),
        };

        let binding = FusedSeriesBinding {
            series_schema,
            // The tree's own output is evaluated on the statement's grid, and the binding names
            // the same one, so a chunk's absolute positions are read on the grid it was computed
            // on.
            grid,
            metric_name: Some(Arc::from(table_name.as_str())),
            time_index: time_index.clone(),
            tags: tags.clone(),
        };
        let output_schema = fused_output_schema(&fields, &tags, &time_index)?;
        let node = FusedSeries::try_new(
            Arc::from(prom_expr.to_string()),
            tree,
            binding,
            input,
            output_schema,
        )
        .context(DataFusionPlanningSnafu)?;

        Ok(Some(LogicalPlan::Extension(Extension {
            node: Arc::new(node),
        })))
    }

    /// The statement being planned, rebuilt from the evaluation numbers the planner holds.
    ///
    /// The compiler reads the statement's grid, lookback and `@` anchors rather than the planner's
    /// mutable state. The route is only offered at the statement root, where `ctx.start` and
    /// `ctx.end` are the statement's own bounds: they are rewritten while an anchored window or a
    /// subquery is folded, so a nested expression would need the bounds it was actually entered
    /// with, which is why no nested expression is offered here.
    ///
    /// `None` means the bounds are not representable as a [`SystemTime`], which is not a grid a
    /// tree can be evaluated on either.
    fn native_statement(&self, prom_expr: &PromExpr) -> Option<EvalStmt> {
        let start = UNIX_EPOCH.checked_add(Duration::from_millis(self.ctx.stmt_start as u64))?;
        let end = UNIX_EPOCH.checked_add(Duration::from_millis(self.ctx.stmt_end as u64))?;
        Some(EvalStmt {
            expr: prom_expr.clone(),
            start,
            end,
            interval: Duration::from_millis(self.ctx.interval as u64),
            lookback_delta: Duration::from_millis(self.ctx.lookback_delta as u64),
        })
    }
}

/// The columns a [`FusedSeries`] node produces.
///
/// The shape is the one the existing unary pipeline consumes — the series fields, the tags, then
/// the time index — and each column is declared the way the node's own conversion reads it back:
/// the time column is a non-nullable millisecond timestamp with no timezone, because every
/// evaluated chunk is addressed on the statement's grid and carries one; a field is a nullable
/// `Float64`, because a grid point inside the statement's interval can have no sample; and a tag is
/// a nullable `Utf8`, because a range function drops the metric name and a series can be missing a
/// label entirely.
fn fused_output_schema(
    fields: &[String],
    tags: &[String],
    time_index: &str,
) -> Result<DFSchemaRef> {
    let mut columns = Vec::with_capacity(fields.len() + tags.len() + 1);
    for name in fields {
        columns.push((
            None,
            Arc::new(Field::new(name, ArrowDataType::Float64, true)),
        ));
    }
    for name in tags {
        columns.push((None, Arc::new(Field::new(name, ArrowDataType::Utf8, true))));
    }
    columns.push((
        None,
        Arc::new(Field::new(
            time_index,
            ArrowDataType::Timestamp(ArrowTimeUnit::Millisecond, None),
            false,
        )),
    ));

    Ok(Arc::new(
        DFSchema::new_with_metadata(columns, Default::default())
            .context(DataFusionPlanningSnafu)?,
    ))
}

/// The selector's own scan, asking the region for the per-series distribution.
///
/// The distribution is a hint on the *table scan*, which the region server reads when it builds
/// the region scan: `PerSeries` is what makes a region prepare its series scan, whose raw-series
/// entrypoint is the only one that packs a series' chunks into the frame this node reads. The
/// existing planner asks for the same hint indirectly, through a sort on `(series key, time)` that
/// [`ScanHintRule`](crate::optimizer::scan_hint::ScanHintRule) turns into this distribution; the
/// native node has no such sort below it, because it reads raw samples rather than sorted rows, so
/// the hint is attached here.
///
/// The provider is cloned for this use-site first, exactly as that rule does: a catalog provider can
/// be shared by several scans, and a hint attached to the shared one would change their requests
/// too. A table whose provider is not the region-server provider - a test fixture, or an engine
/// this route cannot read - is left untouched, and the physical route declines it there.
fn with_per_series_distribution(plan: LogicalPlan) -> LogicalPlan {
    let LogicalPlan::TableScan(mut scan) = plan else {
        return plan;
    };
    let Some(source) = scan.source.downcast_ref::<DefaultTableSource>() else {
        return LogicalPlan::TableScan(scan);
    };
    let Some(provider) = source.table_provider.downcast_ref::<DummyTableProvider>() else {
        return LogicalPlan::TableScan(scan);
    };
    let adapter = provider.clone_for_scan();
    adapter.with_distribution(TimeSeriesDistribution::PerSeries);
    scan.source = Arc::new(DefaultTableSource::new(Arc::new(adapter)));
    LogicalPlan::TableScan(scan)
}

/// The one selector `expr` folds, if it names exactly one.
///
/// The walk follows the grammar the native compiler accepts — a call, a range selector, a selector
/// and parentheses — and stops at every other shape, because a shape the compiler cannot compile
/// needs no selector resolved. Two distinct selectors are not one input, and a tree built over the
/// wrong one of them would evaluate a window the query did not write.
fn single_selector(expr: &PromExpr) -> Option<&VectorSelector> {
    let mut selectors = Vec::new();
    collect_selectors(expr, &mut selectors);
    match selectors.as_slice() {
        [only] => Some(only),
        _ => None,
    }
}

fn collect_selectors<'a>(expr: &'a PromExpr, out: &mut Vec<&'a VectorSelector>) {
    match expr {
        PromExpr::VectorSelector(selector) => out.push(selector),
        PromExpr::MatrixSelector(MatrixSelector { vs, .. }) => out.push(vs),
        PromExpr::Paren(ParenExpr { expr }) => collect_selectors(expr, out),
        PromExpr::Call(call) => {
            for arg in &call.args.args {
                collect_selectors(arg, out);
            }
        }
        _ => {}
    }
}
