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
//! built to the shape the existing unary pipeline produces — the time index, then the series
//! fields, then the tags — so a node planned here can be consumed by the same code that consumes a
//! `SeriesNormalize` output.

use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

use common_catalog::consts::{METRIC_ENGINE, MITO_ENGINE, MITO2_ENGINE};
use datafusion::arrow::datatypes::{DataType as ArrowDataType, Field, TimeUnit as ArrowTimeUnit};
use datafusion::common::{DFSchema, DFSchemaRef};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{Extension, LogicalPlan, LogicalPlanBuilder, TableScanBuilder};
use datafusion::prelude::Expr as DfExpr;
use datafusion_expr::Expr;
use datatypes::series::SeriesOrdering;
use datatypes::series::samples::{FieldSpec, SeriesSchema};
use promql::extension_plan::{FusedSeries, FusedSeriesBinding, FusedSeriesInput};
use promql::native::plan::{NativeExpr, reads_across_series};
use promql_parser::label::Matchers;
use promql_parser::parser::value::ValueType;
use promql_parser::parser::{
    EvalStmt, Expr as PromExpr, MatrixSelector, ParenExpr, VectorSelector,
};
use session::context::QueryContextRef;
use snafu::ResultExt;
use store_api::metric_engine_consts::{DATA_SCHEMA_TABLE_ID_COLUMN_NAME, METRIC_ENGINE_NAME};

use crate::parser::{EXPLAIN_NODE_NAME, EXPLAIN_VERBOSE_NODE_NAME};
use crate::promql::error::{DataFusionPlanningSnafu, Result};
use crate::promql::native_compile::{compile_selectors, statement_grid};
use crate::promql::planner::PromPlanner;

/// The query-context key that turns the native fused-series route on for one query.
///
/// Named after the engine-wide option it mirrors ([`QueryOptions::experimental_native_promql`]),
/// and read exactly the way the other per-query override in this crate is read
/// ([`remote_dyn_filter_pushdown_enabled_from_extensions`]): a query-context extension, which a
/// client can set per request over the HTTP hints header or as a gRPC context key. The engine-wide
/// option is applied while building query-engine state; this planner reads the per-query request
/// extension at the call site; see [`native_promql_requested`].
///
/// [`QueryOptions::experimental_native_promql`]: crate::options::QueryOptions::experimental_native_promql
/// [`remote_dyn_filter_pushdown_enabled_from_extensions`]:
///     crate::options::remote_dyn_filter_pushdown_enabled_from_extensions
pub const NATIVE_PROMQL_EXTENSION_KEY: &str = "query.experimental_native_promql";

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
/// Which ordering a raw-series scan declares is decided when the region scanner is prepared, during
/// physical planning, and the logical planner cannot read it: a legacy scan declares `KeyThenTime`
/// and a two-phase scan `Contiguous`. So the route names the weakest ordering both provide —
/// `Contiguous`, every series contiguous and the series themselves unordered — rather than
/// overclaiming the legacy scan's stronger `KeyThenTime`. A native evaluation only refuses
/// `Interleaved`, so a tree compiled here runs on either scanner.
const NATIVE_ORDERING: SeriesOrdering = SeriesOrdering::Contiguous;

impl PromPlanner {
    /// Plans `prom_expr` as one [`FusedSeries`] node, or returns `None` to keep the existing plan.
    ///
    /// The route is offered a whole expression because the compiler folds a range function and the
    /// math functions over it into one tree: offering a sub-expression instead would compile a tree
    /// that computes something the reader did not write. Everything else about the node comes from
    /// the planner's resolved selector context: the table name, the time index column, the tag
    /// columns and the field columns, which are exactly the raw series schema the tree's input
    /// has to be compiled against. Selectors with a `__field__` matcher keep legacy planning.
    ///
    /// `None` is not a failure. It says this query cannot take the route *unchanged*, and the
    /// caller plans it with the existing planner as if this method did not exist. The two ways that
    /// happens are a shape the compiler refuses (`DataFusionError::NotImplemented`, which names
    /// what it found) and a query this route cannot bind faithfully — unsupported aggregates or
    /// subqueries, a label matcher the bare scan below the node would not apply, a missing table, a
    /// native histogram field, or an unusable grid.
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

        let selectors = {
            let mut values = Vec::new();
            if !collect_selectors(prom_expr, &mut values) {
                return Ok(None);
            }
            values
        };
        if selectors.is_empty() {
            return Ok(None);
        }
        let Some(stmt) = self.native_statement(prom_expr) else {
            return Ok(None);
        };
        let Ok(grid) = statement_grid(&stmt) else {
            return Ok(None);
        };
        let original_context = self.ctx.clone();
        let preparation = async {
            let mut prepared: Vec<FusedSeriesInput> = Vec::new();
            for selector in &selectors {
                self.ctx = original_context.clone();
                let matchers =
                    match self.preprocess_label_matchers(&selector.matchers, &selector.name) {
                        Ok(m) => m,
                        Err(_) => return Ok(None),
                    };
                if self.setup_context().await?.is_some() {
                    return Ok(None);
                }
                let (Some(metric), Some(time)) = (
                    self.ctx.table_name.clone(),
                    self.ctx.time_index_column.clone(),
                ) else {
                    return Ok(None);
                };
                let tags = self.ctx.tag_columns.clone();
                let fields = self.ctx.field_columns.clone();
                // A `__field__` matcher narrows the field set in a projection the existing planner
                // builds, and the raw series scan below this node has no equivalent: it reports every
                // field the scan carries. Binding all of them would fold fields the query excluded, so
                // the route is declined until the field selector is reused here.
                if self.ctx.field_column_matcher.is_some() || fields.is_empty() {
                    return Ok(None);
                }
                // The selector's own table scan, built by the same method the existing path uses:
                // the physical planner turns it into the region scan the node reads, and building a
                // scan here instead would let the two paths disagree about the columns they read.
                let table_ref = self.table_ref()?;
                let mut input = self.create_table_scan_plan(table_ref).await?;
                let logical_scan = metric_scan_and_table_id(&input);
                let has_verified_metric_rewrite = logical_scan.is_some();
                let scan = match logical_scan {
                    Some((mut scan, predicate)) => {
                        // Preserve logical field order and the table restriction on the direct
                        // physical scan.
                        let source_schema = scan.source.schema();
                        let Some(mut ordered_projection) = fields
                            .iter()
                            .map(|field| source_schema.index_of(field).ok())
                            .collect::<Option<Vec<_>>>()
                        else {
                            return Ok(None);
                        };
                        let original_projection = scan.projection.as_ref();
                        let selected_indices = original_projection
                            .cloned()
                            .unwrap_or_else(|| (0..source_schema.fields().len()).collect());
                        for index in selected_indices {
                            let is_field = fields
                                .iter()
                                .any(|field| field == source_schema.field(index).name());
                            if !is_field && !ordered_projection.contains(&index) {
                                ordered_projection.push(index);
                            }
                        }
                        scan.filters.push(predicate);
                        scan = TableScanBuilder::from(scan)
                            .with_projection(Some(ordered_projection))
                            .build()
                            .context(DataFusionPlanningSnafu)?;
                        input = LogicalPlan::TableScan(scan.clone());
                        scan
                    }
                    None => match &input {
                        LogicalPlan::TableScan(scan) => scan.clone(),
                        _ => return Ok(None),
                    },
                };
                let table = self.table_from_source(&scan.source)?;
                let table_meta = &table.table_info().meta;
                // An engine without a raw-series scanner has no frame to give this node, and by the
                // time the physical planner asks, the logical plan is already this node: the refusal
                // there is a planning error, not a fallback. So the engine is checked here, where the
                // existing plan is still the answer. A file-engine table is the concrete case: its
                // scanner is a `SinglePartitionScanner`, which leaves `raw_series_schema` at the
                // default `None`. A metric-engine table is served only through the logical rewrite
                // recognised above, which is what makes its series the table's own.
                if !supports_native_raw_series(&table_meta.engine)
                    || (table_meta.engine == METRIC_ENGINE_NAME && !has_verified_metric_rewrite)
                {
                    return Ok(None);
                }
                // The region's raw-series preflight refuses these, and the physical layer turns that
                // refusal into an error, so decline this route here instead.
                let time_index_type = table_meta
                    .schema
                    .timestamp_column()
                    .map(|column| &column.data_type);
                if time_index_type
                    != Some(&datatypes::prelude::ConcreteDataType::timestamp_millisecond_datatype())
                    || table_meta
                        .options
                        .extra_options
                        .get(store_api::mito_engine_options::APPEND_MODE_KEY)
                        .is_some_and(|value| value.eq_ignore_ascii_case("true"))
                {
                    return Ok(None);
                }
                // FusedSeries evaluates each region independently, so partition keys must identify a
                // series.
                if !table
                    .table_info()
                    .meta
                    .partition_column_names()
                    .all(|column| tags.iter().any(|tag| tag == column))
                {
                    return Ok(None);
                }
                // A window over a native histogram yields a histogram, which the output schema below
                // cannot declare; the existing path evaluates those with its own kernels.
                if Self::field_columns_contain_native_histogram(input.schema(), &fields) {
                    return Ok(None);
                }
                // The region's raw series preflight serves a field only when it is numeric or a native
                // histogram, and the physical layer turns that refusal into a planning error because
                // the logical plan is already fixed by then. A table whose selected fields are, say, a
                // string and a boolean therefore has to be declined here, while the existing plan is
                // still the fallback - `stringsfield` over such a table answered normally before this
                // route existed.
                if !Self::field_columns_are_raw_series_servable(input.schema(), &fields) {
                    return Ok(None);
                }
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
                // The row filter is the selector's own: the time range is added after compiling,
                // because only the compiler knows whether this occurrence reads a window or one
                // instant.
                let mut filters = Self::matchers_to_expr(matchers, input.schema())?;
                if let LogicalPlan::TableScan(table_scan) = &mut input {
                    table_scan.filters.append(&mut filters);
                } else {
                    return Ok(None);
                }
                prepared.push(FusedSeriesInput {
                    series_schema,
                    metric_name: Some(Arc::from(metric)),
                    time_index: time,
                    tags,
                    input,
                });
            }
            Ok(Some(prepared))
        }
        .await;
        self.ctx = original_context;
        let Some(prepared) = preparation? else {
            return Ok(None);
        };
        if prepared
            .windows(2)
            .any(|w| w[0].time_index != w[1].time_index || w[0].tags != w[1].tags)
        {
            return Ok(None);
        }
        // The compiler binds one input per selector in the same pre-order `collect_selectors`
        // walks, so position i of both lists is the same occurrence. This is the guard that the
        // two walks agree: a mismatch is a compiler defect, not a query this route can plan.
        let identities: Vec<(Option<String>, Matchers, i64, Option<i64>)> = selectors
            .iter()
            .map(|selector| {
                let offset_ms = selector
                    .offset
                    .as_ref()
                    .map(|o| match o {
                        promql_parser::parser::Offset::Pos(d) => d.as_millis() as i64,
                        promql_parser::parser::Offset::Neg(d) => -(d.as_millis() as i64),
                    })
                    .unwrap_or_default();
                Ok((
                    selector.name.clone(),
                    selector.matchers.clone(),
                    offset_ms,
                    // The compiler records the raw anchor separately from the offset; compare
                    // that same canonical identity rather than the derived anchor-minus-offset.
                    self.at_ref_time(&selector.at, &None)?,
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        let bind_index = std::cell::Cell::new(0usize);
        let compiled = match compile_selectors(
            &stmt,
            &|req| {
                let i = bind_index.get();
                bind_index.set(i + 1);
                let (Some(prepared_input), Some(identity)) = (prepared.get(i), identities.get(i))
                else {
                    return Err(DataFusionError::Internal(
                        "a selector was bound that the planner did not prepare".into(),
                    ));
                };
                if *identity
                    != (
                        req.metric.clone(),
                        req.matchers.clone(),
                        req.offset_ms,
                        req.at_ms,
                    )
                {
                    return Err(DataFusionError::Internal(
                        "the compiler and the planner walk the selectors differently".into(),
                    ));
                }
                Ok(Arc::clone(&prepared_input.series_schema))
            },
            NATIVE_ORDERING,
        ) {
            Ok(v) => v,
            // The compiler refusing a shape is this route declining it.
            Err(DataFusionError::NotImplemented(_)) => return Ok(None),
            Err(e) => return Err(e).context(DataFusionPlanningSnafu),
        };
        // An aggregate or a vector operation (`sort`, `absent`, `topk`, `histogram_quantile`, ...)
        // reads the whole vector at each grid point rather than one series at a time, so it cannot
        // be evaluated by one executor per scan partition. Declining here is the only place that
        // still works: once the `FusedSeries` node exists the logical plan is fixed, and the
        // categorizer merely declining to push it down would leave a node the physical layer must
        // build anyway. A binary operation pairs one series with another and reads each of them
        // point-wise, so a tree rooted in one keeps the route: its operands are the independent
        // scans prepared above.
        //
        // TEMPORARY: the exception below is stated over the root alone, while the classifier it
        // narrows is a whole-tree walk. The two have to be reconciled with the per-operand
        // point-wise check the shared gate will own, so this narrowing is the first stage's anchor
        // rather than the shape to keep.
        if !matches!(compiled.tree.root().expr, NativeExpr::Binary { .. })
            && reads_across_series(compiled.tree.root())
        {
            return Ok(None);
        }
        if compiled.inputs.len() != prepared.len() {
            return Err(DataFusionError::Internal(
                "the compiled tree reads a different number of inputs than the planner prepared"
                    .into(),
            ))
            .context(DataFusionPlanningSnafu);
        }
        let mut inputs = Vec::with_capacity(prepared.len());
        for (index, (compiled_input, mut input)) in compiled.inputs.iter().zip(prepared).enumerate()
        {
            // The range this selector reads is the compiler's own: a window and an instant differ
            // in which side of the lookback or range their scan starts on, and an anchor can move
            // the whole range outside the statement's.
            let (start, end, inclusive) = compiled_input
                .requirement
                .raw_scan_bounds()
                .context(DataFusionPlanningSnafu)?;
            let time = input.time_index.clone();
            let mut scan = input.input;
            let filter = scan_time_filter(start, end, inclusive, scan.schema(), &time)?;
            if let (LogicalPlan::TableScan(table_scan), Some(filter)) = (&mut scan, filter) {
                table_scan.filters.push(filter);
            } else if !matches!(scan, LogicalPlan::TableScan(_)) {
                return Err(DataFusionError::Internal(format!(
                    "the input at {index} is not the table scan this route prepared"
                )))
                .context(DataFusionPlanningSnafu);
            }
            input.input = scan;
            inputs.push(input);
        }
        let root_fields = compiled
            .tree
            .root()
            .output
            .samples()
            .ok_or_else(|| DataFusionError::Internal("native tree root has no samples".into()))
            .context(DataFusionPlanningSnafu)?
            .fields
            .iter()
            .map(|f| f.name.to_string())
            .collect::<Vec<_>>();
        if root_fields.iter().any(|field| {
            !inputs.iter().any(|input| {
                input
                    .series_schema
                    .fields
                    .iter()
                    .any(|f| f.name.as_ref() == field)
            })
        }) {
            return Ok(None);
        }
        let time = inputs[0].time_index.clone();
        let tags = inputs[0].tags.clone();
        let output_schema = fused_output_schema(&root_fields, &tags, &time)?;
        let node = FusedSeries::try_new(
            Arc::from(prom_expr.to_string()),
            compiled.tree,
            FusedSeriesBinding { grid },
            inputs,
            output_schema,
        )
        .context(DataFusionPlanningSnafu)?;
        Ok(Some(LogicalPlan::Extension(Extension {
            node: Arc::new(node),
        })))
    }

    /// Offers an `EXPLAIN` statement's body to the native compiler, and wraps the plan it makes.
    ///
    /// `TQL EXPLAIN (…) <query>` is the statement whose root extension wraps the query, and the
    /// explanation a reader gets should be of the plan that query runs. Only the two plain
    /// explanations are handled: `ANALYZE`, which runs the plan and reports its metrics, and
    /// `EXPLAIN VERBOSE` are left to the existing path, so this adds nothing to what those report.
    pub(super) async fn try_plan_native_fused_explain(
        &mut self,
        prom_expr: &PromExpr,
    ) -> Result<Option<LogicalPlan>> {
        let PromExpr::Extension(extension) = prom_expr else {
            return Ok(None);
        };
        let explain = match extension.expr.name() {
            EXPLAIN_NODE_NAME => false,
            EXPLAIN_VERBOSE_NODE_NAME => true,
            _ => return Ok(None),
        };
        let children = extension.expr.children();
        let [body] = children else {
            return Ok(None);
        };
        let Some(plan) = self.try_plan_native_fused(body).await? else {
            return Ok(None);
        };
        Ok(Some(
            LogicalPlanBuilder::from(plan)
                .explain(explain, false)
                .context(DataFusionPlanningSnafu)?
                .build()
                .context(DataFusionPlanningSnafu)?,
        ))
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

/// The time-range filter a selector's scan reads, from the bounds its compiled requirement names.
///
/// The lower edge is exclusive unless the requirement says otherwise, which is how a window and a
/// positive lookback read; an inclusive edge is what a zero lookback instant selection has. The
/// bound is expressed in the scan's own timestamp unit, and a bound that unit cannot represent is
/// left out rather than widened: the other side still prunes, and the evaluator applies the exact
/// rule either way.
fn scan_time_filter(
    start: i64,
    end: i64,
    inclusive: bool,
    schema: &DFSchema,
    time_column: &str,
) -> Result<Option<DfExpr>> {
    use datafusion::scalar::ScalarValue;
    let Some(index) = schema.index_of_column_by_name(None, time_column) else {
        return Ok(None);
    };
    let unit = match &schema.field(index).data_type() {
        arrow::datatypes::DataType::Timestamp(unit, _) => *unit,
        _ => return Ok(None),
    };
    let native = |milliseconds: i128| match unit {
        arrow::datatypes::TimeUnit::Second => milliseconds.div_euclid(1_000),
        arrow::datatypes::TimeUnit::Millisecond => milliseconds,
        arrow::datatypes::TimeUnit::Microsecond => milliseconds * 1_000,
        arrow::datatypes::TimeUnit::Nanosecond => milliseconds * 1_000_000,
    };
    let literal = |milliseconds: i128| -> Option<DfExpr> {
        let value = i64::try_from(native(milliseconds)).ok()?;
        Some(DfExpr::Literal(
            match unit {
                arrow::datatypes::TimeUnit::Second => {
                    ScalarValue::TimestampSecond(Some(value), None)
                }
                arrow::datatypes::TimeUnit::Millisecond => {
                    ScalarValue::TimestampMillisecond(Some(value), None)
                }
                arrow::datatypes::TimeUnit::Microsecond => {
                    ScalarValue::TimestampMicrosecond(Some(value), None)
                }
                arrow::datatypes::TimeUnit::Nanosecond => {
                    ScalarValue::TimestampNanosecond(Some(value), None)
                }
            },
            None,
        ))
    };
    let column = DfExpr::Column(datafusion::common::Column::from_name(time_column));
    let lower = literal(i128::from(start)).map(|literal| {
        if inclusive {
            column.clone().gt_eq(literal)
        } else if unit == arrow::datatypes::TimeUnit::Millisecond {
            // An exclusive edge on a millisecond axis is the next representable instant, which is
            // how the existing selector filter states it.
            literal_or_next(start).map_or_else(
                || column.clone().gt(literal.clone()),
                |next| column.clone().gt_eq(next),
            )
        } else {
            column.clone().gt(literal)
        }
    });
    let upper = literal(i128::from(end)).map(|literal| column.clone().lt_eq(literal));
    Ok(match (lower, upper) {
        (Some(lower), Some(upper)) => Some(lower.and(upper)),
        (Some(one), None) | (None, Some(one)) => Some(one),
        (None, None) => None,
    })
}

/// The literal one millisecond after `start`, for an exclusive lower edge on a millisecond axis.
fn literal_or_next(start: i64) -> Option<DfExpr> {
    use datafusion::scalar::ScalarValue;
    let next = start.checked_add(1)?;
    Some(DfExpr::Literal(
        ScalarValue::TimestampMillisecond(Some(next), None),
        None,
    ))
}

/// The columns a [`FusedSeries`] node produces.
///
/// The shape is the one the existing unary pipeline consumes — the time index, then the series
/// fields, then the tags — and each column is declared the way the node's own conversion reads it
/// back:
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
    columns.push((
        None,
        Arc::new(Field::new(
            time_index,
            ArrowDataType::Timestamp(ArrowTimeUnit::Millisecond, None),
            false,
        )),
    ));
    for name in fields {
        columns.push((
            None,
            Arc::new(Field::new(name, ArrowDataType::Float64, true)),
        ));
    }
    for name in tags {
        columns.push((None, Arc::new(Field::new(name, ArrowDataType::Utf8, true))));
    }

    Ok(Arc::new(
        DFSchema::new_with_metadata(columns, Default::default())
            .context(DataFusionPlanningSnafu)?,
    ))
}

/// Every selector `expr` reads, in the order the compiler binds them.
///
/// The order is the compiler's own: it compiles a node's operands in the order they were written, so
/// this walk goes into the same children in the same order. The list is then matched position by
/// position against the compiled inputs, which is what makes a walk that visits a different set the
/// signal that the two disagree rather than a silent mismatch of selector to scan. `false` means a
/// selector appeared under an aggregate or subquery, whose semantics this route cannot execute;
/// the whole expression must fall back rather than binding only its remaining selectors.
fn collect_selectors<'a>(expr: &'a PromExpr, out: &mut Vec<&'a VectorSelector>) -> bool {
    match expr {
        PromExpr::VectorSelector(selector) => out.push(selector),
        PromExpr::MatrixSelector(MatrixSelector { vs, .. }) => out.push(vs),
        PromExpr::Paren(ParenExpr { expr }) => return collect_selectors(expr, out),
        // Left before right, because a non-commutative operator means what that order says.
        PromExpr::Binary(binary) => {
            return collect_selectors(&binary.lhs, out) && collect_selectors(&binary.rhs, out);
        }
        // The compiler folds a negation into a multiplication whose operand comes second, but the
        // selector is the only thing this walk collects, so the order inside one subtree does not
        // depend on where the constant sits.
        PromExpr::Unary(unary) => return collect_selectors(&unary.expr, out),
        PromExpr::Call(call) => {
            // A label rewrite adds a label the node's output schema cannot declare: that schema is
            // built from the table's own tags, so folding `label_replace`/`label_join` in would
            // silently drop the destination label the existing plan projects.
            if matches!(call.func.name, "label_replace" | "label_join") {
                return;
            }
            for arg in &call.args.args {
                if !collect_selectors(arg, out) {
                    return false;
                }
            }
        }
        // Aggregate and subquery plans are not executed by the native route. Decline the entire
        // expression, including when either is nested beneath a supported call or binary node.
        PromExpr::Aggregate(_) | PromExpr::Subquery(_) => return false,
        _ => {}
    }
    true
}

/// Whether `engine` can serve raw series frames to the fused-series node.
///
/// A whitelist avoids probing a region from logical planning; declining only costs the native route.
fn supports_native_raw_series(engine: &str) -> bool {
    matches!(engine, MITO_ENGINE | MITO2_ENGINE | METRIC_ENGINE)
}

/// Recognizes only the metric logical-table scan emitted by `create_table_scan_plan`.
fn metric_scan_and_table_id(
    plan: &LogicalPlan,
) -> Option<(datafusion::logical_expr::TableScan, Expr)> {
    let plan = match plan {
        LogicalPlan::Projection(projection) => projection.input.as_ref(),
        plan => plan,
    };
    let plan = match plan {
        LogicalPlan::SubqueryAlias(alias) => alias.input.as_ref(),
        plan => plan,
    };
    let LogicalPlan::Filter(filter) = plan else {
        return None;
    };
    let Expr::BinaryExpr(binary) = &filter.predicate else {
        return None;
    };
    if binary.op != datafusion::logical_expr::Operator::Eq
        || !matches!(binary.left.as_ref(), Expr::Column(column) if column.name == DATA_SCHEMA_TABLE_ID_COLUMN_NAME)
    {
        return None;
    }
    if !matches!(
        binary.right.as_ref(),
        Expr::Literal(datafusion::scalar::ScalarValue::UInt32(Some(_)), _)
    ) {
        return None;
    }
    let LogicalPlan::TableScan(scan) = filter.input.as_ref() else {
        return None;
    };
    Some((scan.clone(), filter.predicate.clone()))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use session::context::QueryContextBuilder;

    use super::{NATIVE_PROMQL_EXTENSION_KEY, native_promql_requested};

    #[test]
    fn native_promql_requested_is_false_when_extension_is_absent() {
        let query_ctx = Arc::new(QueryContextBuilder::default().build());

        assert!(!native_promql_requested(&query_ctx));
    }

    #[test]
    fn native_promql_requested_accepts_true_case_insensitively() {
        for value in ["true", "TRUE", "TrUe"] {
            let query_ctx = Arc::new(
                QueryContextBuilder::default()
                    .set_extension(NATIVE_PROMQL_EXTENSION_KEY.to_string(), value.to_string())
                    .build(),
            );

            assert!(native_promql_requested(&query_ctx), "value: {value}");
        }
    }

    #[test]
    fn native_promql_requested_rejects_false_and_unrecognized_values() {
        for value in ["false", "FALSE", "yes", "1", " true ", ""] {
            let query_ctx = Arc::new(
                QueryContextBuilder::default()
                    .set_extension(NATIVE_PROMQL_EXTENSION_KEY.to_string(), value.to_string())
                    .build(),
            );

            assert!(!native_promql_requested(&query_ctx), "value: {value:?}");
        }
    }
}
