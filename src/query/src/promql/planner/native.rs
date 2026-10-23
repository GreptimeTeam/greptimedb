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
use datafusion::logical_expr::{Extension, LogicalPlan, TableScanBuilder};
use datafusion_expr::{Expr, UserDefinedLogicalNode};
use datatypes::series::SeriesOrdering;
use datatypes::series::samples::{FieldSpec, SeriesSchema};
use promql::extension_plan::{FusedSeries, FusedSeriesBinding, VectorSelect};
use promql::native::plan::{BoundVectorOp, NativeExpr, NativeNode, reads_across_series};
use promql_parser::parser::value::ValueType;
use promql_parser::parser::{
    EvalStmt, Expr as PromExpr, MatrixSelector, ParenExpr, VectorSelector,
};
use session::context::QueryContextRef;
use snafu::ResultExt;
use store_api::metric_engine_consts::{DATA_SCHEMA_TABLE_ID_COLUMN_NAME, METRIC_ENGINE_NAME};

use crate::promql::error::{DataFusionPlanningSnafu, Result};
use crate::promql::native_compile::{compile, statement_grid};
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
    /// A statement whose root is a ranking over a tree that reads one series at a time is planned as
    /// a [`VectorSelect`] over such a prefix instead; see the whole-vector guard below.
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
    /// narrows the field set, a missing table, a native histogram field, an unusable grid, or a
    /// whole-vector root that is not a ranking over a per-series prefix of one `Float64` field.
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
        let mut input = self.create_table_scan_plan(table_ref.clone()).await?;
        let logical_scan = metric_scan_and_table_id(&input);
        let has_verified_metric_rewrite = logical_scan.is_some();
        let scan = match logical_scan {
            Some((mut scan, predicate)) => {
                // Preserve logical field order and the table restriction on the direct physical scan.
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
        // An engine without a raw-series scanner has no frame to give this node, and by the time
        // the physical planner asks, the logical plan is already this node: the refusal there is a
        // planning error, not a fallback. So the engine is checked here, where the existing plan is
        // still the answer. A file-engine table is the concrete case: its scanner is a
        // `SinglePartitionScanner`, which leaves `raw_series_schema` at the default `None`.
        if !supports_native_raw_series(&table_meta.engine)
            || (table_meta.engine == METRIC_ENGINE_NAME && !has_verified_metric_rewrite)
        {
            return Ok(None);
        }
        // The region's raw-series preflight refuses these, and the physical layer turns that refusal into an error, so decline this route here instead.
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
        // FusedSeries evaluates each region independently, so partition keys must identify a series.
        if !table
            .table_info()
            .meta
            .partition_column_names()
            .all(|column| tags.iter().any(|tag| tag == column))
        {
            return Ok(None);
        }
        // The legacy selector converts an offset the same way; sharing that conversion keeps the
        // two scan paths from disagreeing about an extreme duration.
        let offset = Self::offset_millis(&selector.offset);
        let (saved_start, saved_end, saved_range) = (self.ctx.start, self.ctx.end, self.ctx.range);
        self.ctx.range = selector_range_millis(prom_expr);
        // Keep the fused scan's bounds identical to the legacy selector scan, including @ and offset.
        let time_filter = (|| {
            let anchored = self.at_modifier_offset(&selector.at, &selector.offset)?;
            if anchored.is_some() {
                self.ctx.end = saved_start;
            }
            self.build_time_index_filter(anchored.unwrap_or(offset), input.schema())
        })();
        self.ctx.start = saved_start;
        self.ctx.end = saved_end;
        self.ctx.range = saved_range;
        let time_filter = time_filter?;
        if let Some(filter) = time_filter {
            let LogicalPlan::TableScan(scan) = &mut input else {
                unreachable!();
            };
            scan.filters.push(filter);
        }
        // A window over a native histogram yields a histogram, which the output schema below
        // cannot declare; the existing path evaluates those with its own kernels.
        if Self::field_columns_contain_native_histogram(input.schema(), &fields) {
            return Ok(None);
        }
        // The region's raw series preflight serves a field only when it is numeric or a native
        // histogram, and the physical layer turns that refusal into a planning error because the
        // logical plan is already fixed by then. A table whose selected fields are, say, a string
        // and a boolean therefore has to be declined here, while the existing plan is still the
        // fallback - `stringsfield` over such a table answered normally before this route existed.
        if !Self::field_columns_are_raw_series_servable(input.schema(), &fields) {
            return Ok(None);
        }
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
        // A vector operation (`sort`, `absent`, `topk`, `histogram_quantile`, ...) reads the whole
        // vector at each grid point rather than one series at a time, so it cannot be evaluated by
        // one executor per scan partition. Declining here is the only place that still works: once
        // the `FusedSeries` node exists the logical plan is fixed, and the categorizer merely
        // declining to push it down would leave a node the physical layer must build anyway.
        //
        // The one shape whose whole-vector part is only the outermost node — a ranking over a
        // tree that reads one series at a time — is still offered: each partition evaluates that
        // prefix on its own series, and the ranking over the gathered series needs the whole
        // vector. It is admitted only when the resolved input supplies exactly one `Float64`
        // field, because what a partition hands up names one value per grid position. Everything
        // else is declined here, before any node of this route exists, so the fallback is the plan
        // the existing planner would have built without any part of this one.
        //
        // The frames are numbered by the producer's local partition index, and every region starts
        // that numbering at zero, so a ranked statement is admitted only for a table whose DDL
        // leaves it one region: no partition expression. A partitioned table is declined even when
        // it happens to hold one region today, because the region list is not part of logical
        // planning; the per-series route above is unaffected.
        let ranked = reads_across_series(tree.root());
        if ranked
            && (!ranks_a_per_series_prefix(tree.root())
                || !single_float_field(input.schema(), &fields)
                || !table_meta.partition_key_indices.is_empty())
        {
            return Ok(None);
        }

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
        let node: Arc<dyn UserDefinedLogicalNode> = if ranked {
            // A ranking over a per-series prefix: the prefix leaves each partition as evaluated
            // frames, and the node above them ranks across the gathered series. A shape the node
            // refuses is this route declining it, exactly as the compiler's `NotImplemented` is.
            match VectorSelect::try_new(
                Arc::from(prom_expr.to_string()),
                tree,
                binding,
                input,
                output_schema,
            ) {
                Ok(node) => Arc::new(node),
                Err(DataFusionError::NotImplemented(_)) => return Ok(None),
                Err(error) => return Err(error).context(DataFusionPlanningSnafu),
            }
        } else {
            Arc::new(
                FusedSeries::try_new(
                    Arc::from(prom_expr.to_string()),
                    tree,
                    binding,
                    input,
                    output_schema,
                )
                .context(DataFusionPlanningSnafu)?,
            )
        };

        Ok(Some(LogicalPlan::Extension(Extension { node })))
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

/// The one selector `expr` folds, if it names exactly one.
///
/// The walk follows the grammar the native compiler accepts — a call, an aggregate, a range
/// selector, a selector and parentheses — and stops at every other shape, because a shape the
/// compiler cannot compile needs no selector resolved. Two distinct selectors are not one input, and a tree built over the
/// wrong one of them would evaluate a window the query did not write.
fn selector_range_millis(expr: &PromExpr) -> Option<i64> {
    match expr {
        PromExpr::MatrixSelector(matrix) => Some(matrix.range.as_millis() as i64),
        PromExpr::Paren(paren) => selector_range_millis(&paren.expr),
        PromExpr::Call(call) => call
            .args
            .args
            .iter()
            .find_map(|arg| selector_range_millis(arg)),
        // A ranking's range is the range of the vector it ranks: `topk(2, rate(x[5m]))` reads the
        // same five minutes `rate(x[5m])` does, and the scan has to cover it. The parameter is a
        // constant in every shape that reaches here, and one that carried a selector is not one
        // input to begin with.
        PromExpr::Aggregate(aggregate) => selector_range_millis(&aggregate.expr),
        _ => None,
    }
}

/// Whether `node` is one ranking over a tree that reads one series at a time.
///
/// `topk` and `bottomk` are the whole-vector operations that answer with their input's own series
/// and values, so they are the only ones that can sit above a per-series prefix. `limitk` and
/// `limit_ratio` pick whole series by an input order or a label hash, a `sort` answers with an
/// order only an instant result shows, `absent` reads no input at all and `count_values` writes a
/// label — those remain whole-vector evaluations the existing planner performs.
fn ranks_a_per_series_prefix(node: &NativeNode) -> bool {
    match &node.expr {
        NativeExpr::Vector {
            input,
            op: BoundVectorOp::TopK { .. },
        } => !reads_across_series(input),
        _ => false,
    }
}

/// Whether the resolved input supplies exactly one field and that field is `Float64`.
///
/// Counting is not enough: the consumer above the prefix carries one float value per grid
/// position, so a second field has nowhere to go, and a field of another type is a value the
/// frame cannot carry. The type is read off the resolved input the scan was built from, by the
/// field names the selector context resolved, rather than assumed from the logical column.
fn single_float_field(schema: &DFSchemaRef, fields: &[String]) -> bool {
    let [only] = fields else {
        return false;
    };
    PromPlanner::field_column_type(schema, only) == Some(&ArrowDataType::Float64)
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
        // An aggregate names its vector in its second argument, and the parameter a ranking takes
        // is an expression too: `topk(scalar(x), y)` reads two inputs, so it is counted here
        // rather than mistaken for one.
        PromExpr::Aggregate(aggregate) => {
            if let Some(param) = &aggregate.param {
                collect_selectors(param, out);
            }
            collect_selectors(&aggregate.expr, out);
        }
        _ => {}
    }
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
