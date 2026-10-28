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
//! below the root with one [`FusedSeries`] node over the selector's own table scan. A tree that
//! folds across series - an aggregate - is split instead: its per-series prefix runs in a
//! `FusedSeries` producer and an [`AggregateFinalize`] merges the state every partition exports,
//! because a region holds only part of a group. Declining is the normal answer: every shape the
//! compiler does not accept, and every query this route cannot bind to a scan faithfully, keeps the
//! plan the existing planner builds.
//!
//! What the node needs from here is three things the existing planner already resolves: the raw
//! series schema the tree's input was compiled against (the selector's selected field columns), the
//! statement's evaluation grid, and the logical tag names the output carries. The output schema is
//! built to the shape the existing unary pipeline produces — the time index, then the series
//! fields, then the tags — so a node planned here can be consumed by the same code that consumes a
//! `SeriesNormalize` output.
//!
//! A constant statement is planned with no scan: one node, no child, the time index and value.

use std::borrow::Cow;
use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

use common_catalog::consts::{METRIC_ENGINE, MITO_ENGINE, MITO2_ENGINE};
use common_query::prelude::OTLP_AGGREGATION_TEMPORALITY_LABEL;
use datafusion::arrow::datatypes::{DataType as ArrowDataType, Field, TimeUnit as ArrowTimeUnit};
use datafusion::common::{DFSchema, DFSchemaRef};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{Extension, LogicalPlan, LogicalPlanBuilder, TableScanBuilder};
use datafusion::prelude::Expr as DfExpr;
use datafusion_expr::Expr;
use datatypes::series::SeriesOrdering;
use datatypes::series::samples::{FieldSpec, SeriesSchema};
use promql::extension_plan::{AggregateFinalize, FusedSeries, FusedSeriesBinding, RawSeriesSource};
use promql::native::canonicalize::insert_native_merge_inside_current_fused;
use promql::native::plan::{
    NativeExpr, NativeNode, NativeProperties, NativeTree, needs_single_execution,
    reads_across_series,
};
use promql::vector_match::{Cardinality, MatchingLabels, SetOp};
use promql_parser::label::Matchers;
use promql_parser::parser::value::ValueType;
use promql_parser::parser::{
    BinModifier, EvalStmt, Expr as PromExpr, LabelModifier, MatrixSelector, ParenExpr,
    VectorMatchCardinality, VectorSelector, token,
};
use session::context::QueryContextRef;
use snafu::ResultExt;
use store_api::metric_engine_consts::{DATA_SCHEMA_TABLE_ID_COLUMN_NAME, METRIC_ENGINE_NAME};

use crate::promql::error::{DataFusionPlanningSnafu, Result};
use crate::promql::native_compile::{
    InfoInput, compile_selectors, compile_statement, constant_value, info_input, statement_grid,
};
use crate::promql::planner::{DEFAULT_FIELD_COLUMN, DEFAULT_TIME_INDEX_COLUMN, PromPlanner};
use crate::query_engine::QueryEngineState;

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

/// One prepared occurrence of a selector: its scan, raw-series layout and reporting columns.
type PreparedInput = (
    LogicalPlan,
    Arc<SeriesSchema>,
    Option<Arc<str>>,
    String,
    Vec<String>,
);

pub(crate) fn hybrid_binary_admitted(
    op: token::TokenType,
    modifier: &Option<BinModifier>,
    lhs: &PromExpr,
    rhs: &PromExpr,
) -> bool {
    let supported_op = matches!(
        op.id(),
        token::T_ADD
            | token::T_SUB
            | token::T_MUL
            | token::T_DIV
            | token::T_MOD
            | token::T_POW
            | token::T_ATAN2
    );
    let supported_matching = match modifier {
        None => true,
        Some(modifier) => {
            matches!(&modifier.card, VectorMatchCardinality::OneToOne)
                && (matches!(&modifier.matching, Some(LabelModifier::Include(labels)) if !labels.labels.is_empty())
                    || matches!(&modifier.matching, Some(LabelModifier::Exclude(labels)) if labels.labels.is_empty()))
        }
    };
    supported_op
        && supported_matching
        && lhs.value_type() == ValueType::Vector
        && rhs.value_type() == ValueType::Vector
        && admitted_selector(lhs).is_some()
        && admitted_selector(rhs).is_some()
}

impl PromPlanner {
    pub(super) async fn try_plan_native_operand(
        &mut self,
        expr: &PromExpr,
        query_engine_state: &QueryEngineState,
    ) -> Result<Option<(LogicalPlan, super::PromPlannerContext)>> {
        if admitted_selector(expr).is_none() {
            return Ok(None);
        }
        let saved = self.ctx.clone();
        let (plan, operand_ctx) = match self
            .try_plan_native_fused_with_context(expr, query_engine_state)
            .await?
        {
            Some((plan, Some(operand_ctx))) => (plan, operand_ctx),
            _ => {
                self.ctx = saved;
                return Ok(None);
            }
        };
        self.ctx = operand_ctx.clone();
        let table_ref = self.table_ref();
        self.ctx = saved;
        let table_ref = table_ref?;
        let plan = LogicalPlanBuilder::from(plan)
            .alias(table_ref)
            .context(DataFusionPlanningSnafu)?
            .build()
            .context(DataFusionPlanningSnafu)?;
        Ok(Some((plan, operand_ctx)))
    }

    /// Plans `prom_expr` as one native node, or returns `None` to keep the existing plan.
    ///
    /// `query_engine_state` is only read to build the *existing* plan of an aggregate query, whose
    /// column names and grouped labels are what the rows of a split aggregate are named after. That
    /// plan is built for its schema, its names are copied, and it is dropped: it is never executed
    /// and never stored, so the batch that answers the query is the native route's own.
    ///
    /// The route is offered a whole expression because the compiler folds a range function and the
    /// math functions over it into one tree: offering a sub-expression instead would compile a tree
    /// that computes something the reader did not write. Everything else about the node comes from
    /// the planner's resolved selector context: the table name, the time index column, the tag
    /// columns and the field columns, which are exactly the raw series schema the tree's input
    /// has to be compiled against. Selectors with a `__field__` matcher keep legacy planning.
    ///
    /// `None` is not a failure. It says this query cannot take the route unchanged, and the caller
    /// plans it with the existing planner as if this method did not exist. The aggregate split only
    /// admits shapes the split can carry; other vector-wide operations, subqueries, alternative
    /// matchers, and unbindable scans keep existing planning.
    pub(super) async fn try_plan_native_fused(
        &mut self,
        prom_expr: &PromExpr,
        query_engine_state: &QueryEngineState,
    ) -> Result<Option<LogicalPlan>> {
        Ok(self
            .try_plan_native_fused_with_context(prom_expr, query_engine_state)
            .await?
            .map(|(plan, _)| plan))
    }

    async fn try_plan_native_fused_with_context(
        &mut self,
        prom_expr: &PromExpr,
        query_engine_state: &QueryEngineState,
    ) -> Result<Option<(LogicalPlan, Option<super::PromPlannerContext>)>> {
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

        let Some(stmt) = self.native_statement(prom_expr) else {
            return Ok(None);
        };
        let mut selectors = Vec::new();
        if !collect_selectors(&stmt, prom_expr, &mut selectors) {
            return Ok(None);
        }
        // Preserve the original single-selector admission allowlist. Broader binary syntax is only
        // considered for a multi-occurrence root, where the compiled-tree proof below constrains it.
        if selectors.len() == 1 && !admitted_single_selector(prom_expr) {
            return Ok(None);
        }
        let Ok(grid) = statement_grid(&stmt) else {
            return Ok(None);
        };
        // No selector is not proof of a constant: `time()` and other data-free functions are
        // unsupported here, so the existing fold is asked before a zero-child node is bound.
        if selectors.is_empty() {
            if constant_value(prom_expr).is_none() {
                return Ok(None);
            }
            let tree = match compile_statement(&stmt) {
                Ok(tree) => tree,
                Err(DataFusionError::NotImplemented(_)) => return Ok(None),
                Err(error) => return Err(error).context(DataFusionPlanningSnafu),
            };
            // A constant has no scan: no metric, no tags, and the value under the name the existing
            // path gives a bare literal.
            let time_index = DEFAULT_TIME_INDEX_COLUMN.to_string();
            let value = DEFAULT_FIELD_COLUMN.to_string();
            let output_schema = fused_output_schema(&[value], &[], &time_index)?;
            let node = FusedSeries::try_new(
                Arc::from(prom_expr.to_string()),
                tree,
                FusedSeriesBinding {
                    grid,
                    time_index,
                    tags: Vec::new(),
                    metric_name: None,
                },
                Vec::new(),
                output_schema,
            )
            .context(DataFusionPlanningSnafu)?;
            return Ok(Some((
                LogicalPlan::Extension(Extension {
                    node: Arc::new(node),
                }),
                None,
            )));
        }
        let original_context = self.ctx.clone();
        let preparation = async {
            let mut prepared: Vec<PreparedInput> = Vec::new();
            let mut single_input_context = None;
            for selector in &selectors {
                self.ctx = original_context.clone();
                // These matchers are alternatives the one scan below the node cannot represent, so the
                // query keeps the plan the existing path builds for it.
                if !selector.matchers.or_matchers.is_empty() {
                    return Ok(None);
                }
                let matchers =
                    match self.preprocess_label_matchers(&selector.matchers, &selector.name) {
                        Ok(m) => m,
                        Err(_) => return Ok(None),
                    };
                if self.setup_context().await?.is_some() {
                    return Ok(None);
                }
                if selectors.len() == 1 {
                    single_input_context = Some(self.ctx.clone());
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
                // On a delta-temporality table a series' samples are increments, which the existing
                // planner sums over the window for `increase` and divides by the range for `rate`
                // ([`PromPlanner::select_delta_range_math`]), per series because the tag can differ
                // between them. A compiled tree computes both for a cumulative series, so any
                // `rate`/`increase` over such a table keeps the existing plan; a bare selector still
                // folds. The guard reads the whole statement, not this selector's subtree: one such
                // call anywhere admits a tree the existing plan would not build.
                if tags
                    .iter()
                    .any(|tag| tag == OTLP_AGGREGATION_TEMPORALITY_LABEL)
                    && calls_rate_or_increase(prom_expr)
                {
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
                    || (selectors.len() > 1
                        && !matches!(table_meta.engine.as_str(), MITO_ENGINE | MITO2_ENGINE))
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
                // Preserve the existing single-selector guard that its partition keys are tags.
                if selectors.len() == 1
                    && !table_meta
                        .partition_column_names()
                        .all(|column| tags.iter().any(|tag| tag == column))
                {
                    return Ok(None);
                }
                // A multi-scan plan has no placement over a table split into several regions: the
                // dist analyzer refuses a multi-child node there, so such a query keeps the
                // existing plan instead of failing.
                if selectors.len() > 1 && table_meta.partition_column_names().next().is_some() {
                    return Ok(None);
                }
                // The region's columnar series-index samples, one of the readers this scan may be
                // served by, store `Float64` fields only, and the logical plan cannot tell which
                // reader arrives. So every field has to be `Float64`: an integer, a `Float32`, a
                // decimal, a string or a native histogram field keeps the existing plan, which the
                // physical layer still serves.
                if !fields.iter().all(|field| {
                    Self::field_column_type(input.schema(), field) == Some(&ArrowDataType::Float64)
                }) {
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
                prepared.push((input, series_schema, Some(Arc::from(metric)), time, tags));
            }
            Ok(Some((prepared, single_input_context)))
        }
        .await;
        self.ctx = original_context;
        let Some((mut prepared, single_input_context)) = preparation? else {
            return Ok(None);
        };
        // Selectors over one table share one region, so the node runs there over every scan. When
        // they read more than one table, each scan answers from its own region and the node runs
        // above them; see `RawSeriesSource`.
        let cross_table = !same_table_scans(prepared.iter().map(|(scan, ..)| scan));
        // The first input names the output's time index and, on one table, binds every scan.
        let Some((_, series_schema, metric, time, tags)) = prepared.first() else {
            return Ok(None);
        };
        let (series_schema, metric, time, tags) = (
            Arc::clone(series_schema),
            metric.clone(),
            time.clone(),
            tags.clone(),
        );
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
                let (Some((_, schema, ..)), Some(identity)) = (prepared.get(i), identities.get(i))
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
                Ok(Arc::clone(schema))
            },
            NATIVE_ORDERING,
        ) {
            Ok(v) => v,
            // The compiler refusing a shape is this route declining it.
            Err(DataFusionError::NotImplemented(_)) => return Ok(None),
            Err(e) => return Err(e).context(DataFusionPlanningSnafu),
        };
        if compiled.inputs.len() != prepared.len() {
            return Err(DataFusionError::Internal(
                "the compiled tree reads a different number of inputs than the planner prepared"
                    .into(),
            ))
            .context(DataFusionPlanningSnafu);
        }
        for (index, (compiled_input, (scan, _, _, time, _))) in
            compiled.inputs.iter().zip(prepared.iter_mut()).enumerate()
        {
            if compiled_input.source_index != index {
                return Ok(None);
            }
            // The range this selector reads is the compiler's own: a window and an instant differ
            // in which side of the lookback or range their scan starts on, and an anchor can move
            // the whole range outside the statement's. The bound belongs to the scan, so it is
            // pushed down before two occurrences are compared: two selectors over different windows
            // or offsets read different rows even when nothing else about them differs.
            let (start, end, inclusive) = compiled_input
                .requirement
                .raw_scan_bounds()
                .context(DataFusionPlanningSnafu)?;
            let filter = scan_time_filter(start, end, inclusive, time);
            if let LogicalPlan::TableScan(table_scan) = &mut *scan {
                table_scan.filters.push(filter);
            } else {
                return Err(DataFusionError::Internal(format!(
                    "the child at {index} is not the table scan this route prepared"
                )))
                .context(DataFusionPlanningSnafu);
            }
        }
        let source_tags = prepared
            .iter()
            .map(|(_, _, _, _, tags)| tags.clone())
            .collect::<Vec<_>>();
        let children = prepared
            .into_iter()
            .map(|(scan, schema, metric, _, tags)| {
                if !cross_table {
                    return Ok(scan);
                }
                let Some(metric) = metric else {
                    return Err(DataFusionError::Internal(
                        "a prepared selector has no metric name".into(),
                    ));
                };
                let fields = schema
                    .fields
                    .iter()
                    .map(|field| field.name.to_string())
                    .collect();
                Ok(LogicalPlan::Extension(Extension {
                    node: Arc::new(RawSeriesSource::try_new(metric, tags, fields, scan)?),
                }))
            })
            .collect::<std::result::Result<Vec<_>, DataFusionError>>()
            .context(DataFusionPlanningSnafu)?;
        // Where the tree is placed is decided on the tree exactly as the compiler produced it, and
        // before anything is inserted into it. A merge reads across series, so a decision taken
        // afterwards would answer for the rewrite rather than for the query: the prefix of `sum(x)`
        // would sit over a merge, which the split's `admit_prefix` refuses, and `rate(x[5m])` would
        // report that it reads across series. What the insertion pass below does is therefore a
        // consequence of this placement and can never change which placement is taken.
        let Some(placement) = NativePlacement::of(
            &compiled.tree,
            selectors.len(),
            &series_schema.fields,
            &time,
        ) else {
            return Ok(None);
        };
        if let NativePlacement::SplitAggregate = placement {
            // A split inserts nothing. Its prefix is per-partition per-series work and its finalize
            // already merges the state every partition exports, so a canonical merge would both
            // break the prefix contract and repeat what the finalize does.
            let saved = self.ctx.clone();
            let planned = self
                .prom_root_expr_to_plan(prom_expr, query_engine_state)
                .await;
            let legacy_fields = self.ctx.field_columns.clone();
            let legacy_tags = self.ctx.tag_columns.clone();
            self.ctx = saved;
            drop(planned?);
            let output_schema = fused_output_schema(&legacy_fields, &legacy_tags, &time)?;
            let Some(rawinput) = children.into_iter().next() else {
                return Ok(None);
            };
            let node = match AggregateFinalize::try_new(
                Arc::from(prom_expr.to_string()),
                compiled.tree,
                FusedSeriesBinding {
                    grid,
                    time_index: time,
                    tags,
                    metric_name: metric,
                },
                rawinput,
                output_schema,
            ) {
                Ok(node) => node,
                Err(DataFusionError::NotImplemented(_)) => return Ok(None),
                Err(error) => return Err(error).context(DataFusionPlanningSnafu),
            };
            return Ok(Some((
                LogicalPlan::Extension(Extension {
                    node: Arc::new(node),
                }),
                single_input_context,
            )));
        }
        // What a merge may be inserted for is a property of the placement, so this is where the
        // pass runs and nowhere else. A single execution reads every input partition at once, so
        // the pieces of one label set arrive together and a consumer that needs the whole series
        // can be handed a merge. A per-partition execution reads one of them, so the tree is handed
        // over exactly as compiled: a merge there could only collect that partition's fragments and
        // would present them as a canonical series, which is worse than no merge at all.
        // Cross-partition aliasing therefore stays unsolved on a per-partition placement until a
        // repartition node makes more placements single-execution.
        let tree = if let NativePlacement::SingleExecution = placement {
            // What the route knows about each fused input: one scan partition's pieces of the
            // metric, read by the pass at the leaf the compiler bound to that source index.
            let boundary = vec![NativeProperties::source_pieces(); children.len()];
            insert_native_merge_inside_current_fused(&compiled.tree, &boundary)
                .context(DataFusionPlanningSnafu)?
        } else {
            compiled.tree
        };
        let root_fields = tree
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
            !series_schema
                .fields
                .iter()
                .any(|f| f.name.as_ref() == field)
        }) {
            return Ok(None);
        }
        let Some(output_tags) = fused_output_tags(tree.root(), &source_tags) else {
            return Ok(None);
        };
        // Under `or` the right table's tags join the left table's columns, and one of them can
        // share a name with the left side's field or time index. The output names each column
        // once, so such a union is left to the existing plan.
        let mut column_names = std::collections::HashSet::new();
        if !std::iter::once(&time)
            .chain(&root_fields)
            .chain(&output_tags)
            .all(|name| column_names.insert(name.as_str()))
        {
            return Ok(None);
        }
        let output_schema = fused_output_schema(&root_fields, &output_tags, &time)?;
        // Across tables the region scans are bound by their `RawSeriesSource` children.
        let (tags, metric) = if cross_table {
            (Vec::new(), None)
        } else {
            (tags, metric)
        };
        let node = FusedSeries::try_new(
            Arc::from(prom_expr.to_string()),
            tree,
            FusedSeriesBinding {
                grid,
                time_index: time,
                tags,
                metric_name: metric,
            },
            children,
            output_schema,
        )
        .context(DataFusionPlanningSnafu)?;
        Ok(Some((
            LogicalPlan::Extension(Extension {
                node: Arc::new(node),
            }),
            single_input_context,
        )))
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

/// The time-range filter a selector's scan reads, from the bounds its compiled requirement names.
///
/// The selector bounds are already checked against the millisecond timestamp schema during
/// admission; preserve the requirement's inclusive or exclusive lower edge exactly.
fn scan_time_filter(start: i64, end: i64, inclusive: bool, time_column: &str) -> DfExpr {
    let column = DfExpr::Column(datafusion::common::Column::from_name(time_column));
    let lower = DfExpr::Literal(
        datafusion::scalar::ScalarValue::TimestampMillisecond(Some(start), None),
        None,
    );
    let upper = DfExpr::Literal(
        datafusion::scalar::ScalarValue::TimestampMillisecond(Some(end), None),
        None,
    );
    let lower = if inclusive {
        column.clone().gt_eq(lower)
    } else {
        column.clone().gt(lower)
    };
    lower.and(column.lt_eq(upper))
}

/// Whether `root` is a supported one-to-one match, set operation or `info` over independent scan
/// occurrences, under any number of point-wise wrappers.
///
/// Any arithmetic or comparison operator qualifies: they pair series by the same matching rules,
/// and a comparison only changes which value a matched pair keeps.
///
/// A wrapper, a function of each value or an operation with a constant as in `100 * (1 - a / b)`,
/// maps each series of the match on its own, so the tree still reads every scan whole exactly once.
/// An operand may itself be such a match, as in `(a - b) / c`: the inner match is answered whole
/// before the outer one reads it.
pub(crate) fn independent_scan_pair(root: &NativeNode) -> bool {
    let (left, right, matching) = match &root.expr {
        NativeExpr::Binary {
            left,
            right,
            matching,
        } => match (&left.expr, &right.expr) {
            (NativeExpr::Scalar(_), NativeExpr::Scalar(_)) => return false,
            (NativeExpr::Scalar(_), _) => return independent_scan_pair(right),
            (_, NativeExpr::Scalar(_)) => return independent_scan_pair(left),
            _ => (left, right, matching),
        },
        NativeExpr::Map { input, .. } => return independent_scan_pair(input),
        NativeExpr::Set { left, right, .. } => {
            return scan_operand(left) && scan_operand(right);
        }
        NativeExpr::Info { base, info, .. } => {
            return pointwise_operand(base) && pointwise_operand(info);
        }
        _ => return false,
    };

    let supported_labels = match &matching.labels {
        MatchingLabels::On(names) => !names.is_empty(),
        MatchingLabels::Ignoring(names) => names.is_empty(),
    };
    matching.cardinality == Cardinality::OneToOne
        && supported_labels
        && scan_operand(left)
        && scan_operand(right)
}

fn scan_operand(node: &NativeNode) -> bool {
    pointwise_operand(node) || independent_scan_pair(node)
}

/// Whether every plan is a scan of one table. Filters, projections and bounds are
/// occurrence-specific and deliberately do not participate in this check.
pub(crate) fn same_table_scans<'a>(plans: impl IntoIterator<Item = &'a LogicalPlan>) -> bool {
    let mut table = None;
    plans.into_iter().all(|plan| {
        let LogicalPlan::TableScan(scan) = plan else {
            return false;
        };
        *table.get_or_insert(&scan.table_name) == &scan.table_name
    })
}

/// The label columns `node` reports, from the tags of the scan each input reads.
///
/// They follow Prometheus' result labels: a one-to-one match keeps the left side's labels, narrowed
/// by `on` or `ignoring`; `and` and `unless` keep the left side's series and `or` adds the right
/// side's; `info` adds the data labels of the info table; an operation with a constant or a function
/// of each value keeps its operand's. `None` for a node whose labels this walk does not derive - an
/// aggregate's groups, or a whole-vector operation - so a tree holding one is declined rather than
/// given its input's labels.
fn fused_output_tags(node: &NativeNode, source_tags: &[Vec<String>]) -> Option<Vec<String>> {
    match &node.expr {
        NativeExpr::Input(input) => source_tags.get(input.source_index).cloned(),
        NativeExpr::Scalar(_) => Some(Vec::new()),
        NativeExpr::Instant { input, .. }
        | NativeExpr::Window { input, .. }
        | NativeExpr::Map { input, .. }
        | NativeExpr::Timestamp { input } => fused_output_tags(input, source_tags),
        // A merge collects the pieces one canonical label set arrived in and writes no label of its
        // own, so the labels it hands over are the ones its input's series carried. The pieces it
        // did not read are not a label set the node invents either: what it emits spans fewer label
        // sets than it read, never more.
        NativeExpr::Merge { input } => fused_output_tags(input, source_tags),
        NativeExpr::Aggregate { .. } | NativeExpr::Vector { .. } => None,
        NativeExpr::Binary {
            left,
            right,
            matching,
        } => match (&left.expr, &right.expr) {
            (NativeExpr::Scalar(_), _) => fused_output_tags(right, source_tags),
            (_, NativeExpr::Scalar(_)) => fused_output_tags(left, source_tags),
            _ => {
                if matching.cardinality != Cardinality::OneToOne {
                    return None;
                }
                let kept = |tags: Vec<String>| -> Vec<String> {
                    match &matching.labels {
                        MatchingLabels::On(names) if !names.is_empty() => tags
                            .into_iter()
                            .filter(|tag| names.iter().any(|name| name.as_ref() == tag.as_str()))
                            .collect(),
                        MatchingLabels::Ignoring(names) => tags
                            .into_iter()
                            .filter(|tag| !names.iter().any(|name| name.as_ref() == tag.as_str()))
                            .collect(),
                        MatchingLabels::On(_) => tags,
                    }
                };
                let mut tags = kept(fused_output_tags(left, source_tags)?);
                // A filled left side takes an unmatched right series' matching labels, so those
                // results carry the right side's tags.
                if matching.fill.lhs.is_some() {
                    for tag in kept(fused_output_tags(right, source_tags)?) {
                        if !tags.contains(&tag) {
                            tags.push(tag);
                        }
                    }
                }
                Some(tags)
            }
        },
        NativeExpr::Set { left, right, op } => {
            let mut tags = fused_output_tags(left, source_tags)?;
            if op.op == SetOp::Or {
                for tag in fused_output_tags(right, source_tags)? {
                    if !tags.contains(&tag) {
                        tags.push(tag);
                    }
                }
            }
            Some(tags)
        }
        // A base series keeps its labels and takes the data labels of its info series: the ones the
        // selector names, or all of them when it names none.
        NativeExpr::Info {
            base,
            info,
            selector,
        } => {
            let mut tags = fused_output_tags(base, source_tags)?;
            for tag in fused_output_tags(info, source_tags)? {
                let copied = selector.data_labels.is_empty()
                    || selector
                        .data_labels
                        .iter()
                        .any(|label| label.as_ref() == tag.as_str());
                if copied && !tags.contains(&tag) {
                    tags.push(tag);
                }
            }
            Some(tags)
        }
    }
}

/// How the route places a tree on the scan partitions, decided on the tree as it was compiled.
///
/// The placement is what an inserted canonical merge means, so it is read off the compiled tree and
/// never off a rewritten one: [`reads_across_series`] and [`AggregateFinalize::can_split`] both
/// answer differently for a tree the insertion pass already put a merge into, and both answers would
/// then describe the rewrite instead of the query.
///
/// A scan keeps each whole series in one of its output partitions (`SeriesGather`'s contract), so a
/// tree that answers one series from one series' rows is answered correctly one partition at a time;
/// that is [`Self::PerPartition`]. A tree that folds across series needs more than one partition's
/// series, which it gets either by folding each partition's state in a finalize
/// ([`Self::SplitAggregate`]) or by seeing every partition in one execution
/// ([`Self::SingleExecution`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum NativePlacement {
    /// One execution per scan partition. Every series' rows are in the partition that holds it, so
    /// one execution sees each series whole.
    PerPartition,
    /// A per-partition per-series prefix, and a finalize that merges the state every partition
    /// exports.
    SplitAggregate,
    /// One execution over every input partition, so every partition's pieces arrive together. This
    /// is the one placement an inserted merge has anything to collect.
    SingleExecution,
}

impl NativePlacement {
    /// How `tree` is placed, or `None` when the route has no placement for it.
    ///
    /// `selectors` is how many occurrences the statement has. `fields` and `time_index` are the raw
    /// layout the aggregate split is offered its cut against.
    ///
    /// Per-series work is decided first: a tree that answers each series from its own rows is
    /// correct one partition at a time, and a two-occurrence pair reads each of its sides the same
    /// way. Of what is left, the aggregate split carries one cut over one raw scan child, and a
    /// one-selector tree the split refuses that nonetheless reads the whole vector at a point is a
    /// single execution.
    ///
    /// `None` is the route having no placement for the tree, and it is the same set the route
    /// declined before this decision was named: a two-occurrence statement outside the admitted pair
    /// is left to the existing planner, because no partition holds the whole of what it folds, and a
    /// one-selector tree that reads across series without needing a single execution is not a shape
    /// the compiler builds. Naming the placement does not widen what the route plans.
    fn of(
        tree: &NativeTree,
        selectors: usize,
        fields: &[FieldSpec],
        time_index: &str,
    ) -> Option<Self> {
        let root = tree.root();
        if !reads_across_series(root)
            || pointwise_operand(root)
            || (selectors == 2 && independent_scan_pair(root))
        {
            return Some(Self::PerPartition);
        }
        if selectors != 1 {
            return None;
        }
        if AggregateFinalize::can_split(tree, fields, time_index) {
            return Some(Self::SplitAggregate);
        }
        needs_single_execution(root).then_some(Self::SingleExecution)
    }
}

/// Whether `node` reads samples point-wise, so that one partitioned execution is one answer.
///
/// A node's output type is not checked here: [`NativeNode::validate`] already refuses a selection,
/// a map or a binary operation whose operands are not evaluated vectors, so every branch below is
/// one that produces values rather than raw samples.
pub(crate) fn pointwise_operand(node: &NativeNode) -> bool {
    match &node.expr {
        // A selection reads the raw samples below it; anything else below it is a value computed
        // from a whole series, not a scan.
        NativeExpr::Instant { input, .. } | NativeExpr::Window { input, .. } => {
            matches!(input.expr, NativeExpr::Input(_))
        }
        NativeExpr::Map { input, .. } | NativeExpr::Timestamp { input } => pointwise_operand(input),
        NativeExpr::Merge { .. } => false,
        // A constant on one side is broadcast over the vector side's positions. Two vectors are a
        // match between two series sets, which is the shape this route cannot split.
        NativeExpr::Binary {
            left,
            right,
            matching,
        } => {
            matching.cardinality == Cardinality::OneToOne
                && match (&left.expr, &right.expr) {
                    (NativeExpr::Scalar(_), NativeExpr::Scalar(_)) => false,
                    (NativeExpr::Scalar(_), _) => pointwise_operand(right),
                    (_, NativeExpr::Scalar(_)) => pointwise_operand(left),
                    _ => false,
                }
        }
        // An aggregate folds a group, a vector operation reads the whole vector, and a bare input or
        // scalar is not a per-series evaluation.
        NativeExpr::Input(_)
        | NativeExpr::Scalar(_)
        | NativeExpr::Aggregate { .. }
        | NativeExpr::Vector { .. }
        | NativeExpr::Set { .. }
        | NativeExpr::Info { .. } => false,
    }
}

/// Whether a single-selector expression belongs to the native route's existing selector allowlist
/// or is an aggregate expression handled by the aggregate split.
fn admitted_single_selector(expr: &PromExpr) -> bool {
    fn aggregate(expr: &PromExpr) -> bool {
        match expr {
            PromExpr::Aggregate(_aggregate) => true,
            PromExpr::Paren(paren) => aggregate(&paren.expr),
            PromExpr::Unary(unary) => aggregate(&unary.expr),
            PromExpr::Binary(binary) => aggregate(&binary.lhs) || aggregate(&binary.rhs),
            PromExpr::Call(call) => call.args.args.iter().any(|arg| aggregate(arg)),
            _ => false,
        }
    }

    admitted_selector(expr).is_some() || aggregate(expr)
}

/// The existing selector allowlist: a selector or supported unary calls over one selector.
fn admitted_selector(expr: &PromExpr) -> Option<&VectorSelector> {
    match expr {
        PromExpr::VectorSelector(selector) => Some(selector),
        PromExpr::MatrixSelector(MatrixSelector { vs, .. }) => Some(vs),
        PromExpr::Paren(ParenExpr { expr }) => admitted_selector(expr),
        PromExpr::Call(call)
            if matches!(
                call.func.name,
                "rate" | "increase" | "sum_over_time" | "abs" | "timestamp"
            ) && call.args.args.len() == 1 =>
        {
            admitted_selector(&call.args.args[0])
        }
        PromExpr::Unary(unary) => admitted_selector(&unary.expr),
        PromExpr::Binary(binary) if constant_value(&binary.lhs).is_some() => {
            admitted_selector(&binary.rhs)
        }
        PromExpr::Binary(binary) if constant_value(&binary.rhs).is_some() => {
            admitted_selector(&binary.lhs)
        }
        _ => None,
    }
}

/// The selectors `expr` reads, in the order the compiler binds them.
///
/// `info` reads its info series through a selector it does not write: [`info_input`] derives it, for
/// the compiler as for this walk, and the compiler binds it after the selectors of the base.
fn collect_selectors<'a>(
    stmt: &EvalStmt,
    expr: &'a PromExpr,
    out: &mut Vec<Cow<'a, VectorSelector>>,
) -> bool {
    match expr {
        PromExpr::VectorSelector(selector) => out.push(Cow::Borrowed(selector)),
        PromExpr::MatrixSelector(MatrixSelector { vs, .. }) => out.push(Cow::Borrowed(vs)),
        PromExpr::Paren(ParenExpr { expr }) => return collect_selectors(stmt, expr, out),
        PromExpr::Call(call)
            if matches!(
                call.func.name,
                "rate" | "increase" | "sum_over_time" | "abs" | "timestamp"
            ) && call.args.args.len() == 1 =>
        {
            return collect_selectors(stmt, &call.args.args[0], out);
        }
        PromExpr::Call(call) if call.func.name == "info" => {
            let Ok(InfoInput { selector, .. }) = info_input(stmt, &call.args.args) else {
                return false;
            };
            if !collect_selectors(stmt, &call.args.args[0], out) {
                return false;
            }
            out.push(Cow::Owned(selector));
        }
        PromExpr::Binary(binary) => {
            return collect_selectors(stmt, &binary.lhs, out)
                && collect_selectors(stmt, &binary.rhs, out);
        }
        // Literals are permitted only as compiler-recognized broadcasts in a bounded binary tree.
        PromExpr::NumberLiteral(_) => return true,
        PromExpr::Unary(unary) => return collect_selectors(stmt, &unary.expr, out),
        PromExpr::Call(call) => {
            if matches!(call.func.name, "label_replace" | "label_join") {
                return false;
            }
            for arg in &call.args.args {
                if !collect_selectors(stmt, arg, out) {
                    return false;
                }
            }
        }
        PromExpr::Aggregate(aggregate) => return collect_selectors(stmt, &aggregate.expr, out),
        PromExpr::Subquery(_) => return false,
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

/// Whether `expr` calls `rate`/`increase` anywhere in its admitted expression tree.
fn calls_rate_or_increase(expr: &PromExpr) -> bool {
    match expr {
        PromExpr::Call(call) => {
            matches!(call.func.name, "rate" | "increase")
                || call.args.args.iter().any(|arg| calls_rate_or_increase(arg))
        }
        PromExpr::Paren(ParenExpr { expr }) => calls_rate_or_increase(expr),
        PromExpr::Binary(binary) => {
            calls_rate_or_increase(&binary.lhs) || calls_rate_or_increase(&binary.rhs)
        }
        PromExpr::Unary(unary) => calls_rate_or_increase(&unary.expr),
        PromExpr::Aggregate(aggregate) => calls_rate_or_increase(&aggregate.expr),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use std::time::{Duration, UNIX_EPOCH};

    use datafusion::execution::memory_pool::{MemoryPool, UnboundedMemoryPool};
    use datatypes::arrow::array::Float64Array;
    use datatypes::arrow::buffer::{Buffer, ScalarBuffer};
    use datatypes::series::raw::{RawSeriesChunk, RawTimestamps};
    use datatypes::series::samples::{FieldSpec, SampleRef, Samples, SeriesFields, SeriesSchema};
    use datatypes::series::{Label, SeriesLabels, SeriesOrdering};
    use promql::extension_plan::AggregateFinalize;
    use promql::native::canonicalize::insert_native_merge_inside_current_fused;
    use promql::native::execute::{NativeExecutionState, NativeExecutor};
    use promql::native::plan::{
        NativeExpr, NativeNode, NativeProperties, NativeTree, needs_single_execution,
        reads_across_series,
    };
    use session::context::QueryContextBuilder;

    use super::{
        NATIVE_PROMQL_EXTENSION_KEY, NativePlacement, admitted_selector, admitted_single_selector,
        collect_selectors, fused_output_tags, hybrid_binary_admitted, independent_scan_pair,
        native_promql_requested, scan_time_filter,
    };

    /// The raw layout [`compiled_test_tree`] binds every selector to: one float field, no grid.
    fn raw_test_schema() -> Arc<SeriesSchema> {
        Arc::new(SeriesSchema {
            fields: Arc::from([FieldSpec { name: "v0".into() }]),
            eval_grid: None,
        })
    }

    fn compiled_test_tree(query: &str) -> NativeTree {
        let statement = promql_parser::parser::EvalStmt {
            expr: promql_parser::parser::parse(query).unwrap(),
            start: UNIX_EPOCH,
            end: UNIX_EPOCH + Duration::from_secs(10),
            interval: Duration::from_secs(5),
            lookback_delta: Duration::from_secs(5),
        };
        let schema = raw_test_schema();
        crate::promql::native_compile::compile_selectors(
            &statement,
            &|_| Ok(Arc::clone(&schema)),
            SeriesOrdering::KeyThenTime,
        )
        .unwrap()
        .tree
    }

    fn input_sources(root: &NativeNode) -> Vec<usize> {
        fn collect(node: &NativeNode, sources: &mut Vec<usize>) {
            match &node.expr {
                NativeExpr::Input(input) => sources.push(input.source_index),
                NativeExpr::Instant { input, .. }
                | NativeExpr::Window { input, .. }
                | NativeExpr::Merge { input }
                | NativeExpr::Map { input, .. }
                | NativeExpr::Timestamp { input } => collect(input, sources),
                NativeExpr::Binary { left, right, .. }
                | NativeExpr::Set { left, right, .. }
                | NativeExpr::Info {
                    base: left,
                    info: right,
                    ..
                } => {
                    collect(left, sources);
                    collect(right, sources);
                }
                NativeExpr::Scalar(_)
                | NativeExpr::Aggregate { .. }
                | NativeExpr::Vector { .. } => {}
            }
        }

        let mut sources = Vec::new();
        collect(root, &mut sources);
        sources
    }

    /// The selectors `query` names, in the order the compiler binds them.
    fn collected_selectors(query: &str) -> Vec<String> {
        let expr = promql_parser::parser::parse(query).expect("the query parses");
        let statement = promql_parser::parser::EvalStmt {
            expr: expr.clone(),
            start: UNIX_EPOCH,
            end: UNIX_EPOCH + Duration::from_secs(10),
            interval: Duration::from_secs(5),
            lookback_delta: Duration::from_secs(5),
        };
        let mut selectors = Vec::new();
        assert!(collect_selectors(&statement, &expr, &mut selectors));
        selectors
            .iter()
            .map(|selector| selector.name.clone().unwrap_or_default())
            .collect()
    }

    #[test]
    fn selector_walk_keeps_plain_selector_identity() {
        for (query, expected) in [
            ("timestamp((x @ 2 offset -1s))", vec!["x"]),
            ("abs(timestamp(x @ 2 offset -1s))", vec!["x"]),
            ("timestamp(x @ start() offset 1s)", vec!["x"]),
            ("timestamp(x @ end() offset -1s)", vec!["x"]),
            (
                "timestamp(x @ 2 offset 1s) + y @ 2 offset 1s",
                vec!["x", "y"],
            ),
        ] {
            assert_eq!(collected_selectors(query), expected, "{query}");
        }
    }

    #[test]
    fn collect_selectors_descends_into_aggregate_inputs() {
        for query in [
            "sum(x)",
            "sum by (job) (x)",
            "sum without (instance) (rate(x[5m]))",
        ] {
            assert_eq!(collected_selectors(query), vec!["x"], "{query}");
        }
    }

    /// The compiler retains both source indices for independent selector occurrences.
    #[test]
    fn independent_scan_pair_keeps_both_numbered_occurrences() {
        for query in [
            "x + on(host) x",
            "x - (x * 2)",
            "rate(x[5s]) + rate(x[10s] offset 2s)",
            "x == on(host) x",
            "x > bool x",
            "2 * (x != on(host) x)",
        ] {
            let tree = compiled_test_tree(query);
            assert_eq!(input_sources(tree.root()), vec![0, 1], "{query}");
            assert!(independent_scan_pair(tree.root()), "{query}");
        }
        assert!(!independent_scan_pair(
            compiled_test_tree("sum(x) + x").root()
        ));
        assert!(!independent_scan_pair(
            compiled_test_tree("x + on() x").root()
        ));
        // A wrapper does not admit a core the pair itself refuses.
        assert!(!independent_scan_pair(
            compiled_test_tree("2 * (x > on() x)").root()
        ));
    }

    /// A match whose operand is itself a match reads every scan whole; a nested operand that folds
    /// or matches many-to-one does not.
    #[test]
    fn independent_scan_pair_admits_nested_one_to_one_matches() {
        for query in [
            "x + y + z",
            "(x - y) / on(host) z",
            "x / on(host) y * on(host) z",
            "abs(x - y) + 2 * z",
            "(x and y) or z",
            "(x + y) unless on(host) z",
            "(x == on(host) y) + z",
            "x > bool y + z",
        ] {
            let tree = compiled_test_tree(query);
            assert_eq!(input_sources(tree.root()), vec![0, 1, 2], "{query}");
            assert!(independent_scan_pair(tree.root()), "{query}");
        }
        for query in [
            "x + y + sum(z)",
            "(x * on(host) group_left y) + z",
            "(x + on() y) + z",
            "(x > on() y) + z",
        ] {
            assert!(
                !independent_scan_pair(compiled_test_tree(query).root()),
                "{query}"
            );
        }
    }

    #[test]
    fn match_output_tags_follow_promql_vector_matching() {
        let tags = vec!["host".to_string(), "kind".to_string()];
        let sources = [tags.clone(), tags.clone()];
        let on = compiled_test_tree("x + on(host) x");
        assert_eq!(
            Some(vec!["host".to_string()]),
            fused_output_tags(on.root(), &sources)
        );
        let ignoring = compiled_test_tree("x + x");
        assert_eq!(
            Some(tags.clone()),
            fused_output_tags(ignoring.root(), &sources)
        );
        let empty_on = compiled_test_tree("x + on() x");
        assert_eq!(
            Some(tags.clone()),
            fused_output_tags(empty_on.root(), &sources)
        );

        // Two tables with different tags: the left side's labels pass through a wrapper and a
        // match, and `or` adds the labels only the right side carries.
        let sources = [
            vec!["host".to_string(), "kind".to_string()],
            vec!["host".to_string(), "zone".to_string()],
        ];
        let wrapped = compiled_test_tree("100 * (1 - x / on(host) y)");
        assert_eq!(
            Some(vec!["host".to_string()]),
            fused_output_tags(wrapped.root(), &sources)
        );
        let and = compiled_test_tree("x and on(host) y");
        assert_eq!(
            Some(sources[0].clone()),
            fused_output_tags(and.root(), &sources)
        );
        let or = compiled_test_tree("x or y");
        assert_eq!(
            Some(vec![
                "host".to_string(),
                "kind".to_string(),
                "zone".to_string()
            ]),
            fused_output_tags(or.root(), &sources)
        );
        // An unmatched right series answers with its own matching labels under a left fill.
        let fill = compiled_test_tree("x + fill_left(0) y");
        assert_eq!(
            Some(vec![
                "host".to_string(),
                "kind".to_string(),
                "zone".to_string()
            ]),
            fused_output_tags(fill.root(), &sources)
        );
        let fill_on = compiled_test_tree("x + on(host) fill(0) y");
        assert_eq!(
            Some(vec!["host".to_string()]),
            fused_output_tags(fill_on.root(), &sources)
        );
        // An outer match narrows the labels the inner one already narrowed.
        let sources = [
            vec!["host".to_string(), "kind".to_string(), "zone".to_string()],
            vec!["host".to_string(), "kind".to_string()],
            vec!["host".to_string(), "az".to_string()],
        ];
        for (query, expected) in [
            ("(x - y) / on(host) z", vec!["host"]),
            ("x + ignoring(zone) y + ignoring(kind) z", vec!["host"]),
            ("x / on(host, kind) y or z", vec!["host", "kind", "az"]),
        ] {
            assert_eq!(
                Some(expected.into_iter().map(String::from).collect::<Vec<_>>()),
                fused_output_tags(compiled_test_tree(query).root(), &sources),
                "{query}"
            );
        }
        // An aggregate's labels are its groups, which this walk does not derive.
        let aggregate = compiled_test_tree("sum by (host) (x)");
        assert_eq!(None, fused_output_tags(aggregate.root(), &sources));
    }

    /// What a caller knows about a fused input: this execution holds one scan partition's pieces of
    /// a metric. It is the only thing the planner can say about a scan, because whether one
    /// partition's series are whole label sets is decided when the region splits its ranges.
    fn source_pieces() -> NativeProperties {
        NativeProperties::source_pieces()
    }

    /// `query`'s tree with the canonical merges the insertion pass puts in it.
    ///
    /// The facts are the ones the planner holds for its scans, one per selector occurrence, so this
    /// is the tree the route hands over when its placement is a single execution - and, for the
    /// tests that show what the other placements avoid, the tree it would hand over had the pass run
    /// for every placement.
    fn passed_tree(query: &str, selectors: usize) -> NativeTree {
        insert_native_merge_inside_current_fused(
            &compiled_test_tree(query),
            &vec![source_pieces(); selectors],
        )
        .expect("the pass keeps the tree legal")
    }

    /// The placement the route gives `tree`, asked of the same function the planner asks.
    ///
    /// The raw field layout is the one [`compiled_test_tree`] binds, and `selectors` is how many
    /// occurrences the statement has.
    fn placement_of(tree: &NativeTree, selectors: usize) -> Option<NativePlacement> {
        NativePlacement::of(tree, selectors, &[FieldSpec { name: "v0".into() }], "ts")
    }

    /// What the route answers for `tree`, in the order
    /// [`PromPlanner::try_plan_native_fused_with_context`] applies it: the placement first, then
    /// whether the labels the fused node declares can be derived for the tree's root.
    ///
    /// `sources` holds the tags of each selector's scan, which the label walk reads.
    fn route_answer(tree: &NativeTree, selectors: usize, sources: &[Vec<String>]) -> &'static str {
        match placement_of(tree, selectors) {
            Some(NativePlacement::PerPartition) => "per-partition",
            Some(NativePlacement::SplitAggregate) => "split aggregate",
            // A single execution is handed to the fused node like any other tree, so the labels that
            // node declares have to be derivable for its root. Nothing has taught the walk to name
            // them for a whole-vector root, so a tree rooted at one is declined there.
            Some(NativePlacement::SingleExecution) => match fused_output_tags(tree.root(), sources)
            {
                Some(_) => "single execution",
                None => "legacy plan",
            },
            None => "legacy plan",
        }
    }

    /// The tree [`PromPlanner::try_plan_native_fused_with_context`] hands to the node it builds: the
    /// compiled tree, or - when the placement is a single execution - the one the insertion pass
    /// canonicalized.
    fn routed_tree(query: &str) -> NativeTree {
        let selectors = collected_selectors(query).len();
        let tree = compiled_test_tree(query);
        if placement_of(&tree, selectors) == Some(NativePlacement::SingleExecution) {
            return passed_tree(query, selectors);
        }
        tree
    }

    /// The labels a one-selector statement's scan reports, for the label walk.
    fn one_source_tags() -> Vec<Vec<String>> {
        vec![vec!["host".to_string()]]
    }

    /// An aggregate tree still takes the split route, because the placement is decided before the
    /// insertion pass runs.
    ///
    /// The two facts are one test on purpose: the placement of these shapes is a split only for the
    /// tree the compiler produced. A merge reads across series, and the split cuts at the innermost
    /// aggregate, so a pass that ran first would put a merge below the cut and its prefix would
    /// become `Merge(Window(..))`, which `admit_prefix` refuses with "a per-series prefix across a
    /// canonical merge, which reads the whole vector". The second half of each case is that tree, so
    /// the order is what the test holds.
    #[test]
    fn an_admitted_aggregate_still_splits_after_the_insertion_pass() {
        let fields = [FieldSpec { name: "v0".into() }];
        for query in ["sum(rate(x[5m]))", "sum(x)", "avg by (host) (rate(x[5m]))"] {
            // The placement is read off the compiled tree, so it is a split.
            let compiled = compiled_test_tree(query);
            assert_eq!(
                Some(NativePlacement::SplitAggregate),
                placement_of(&compiled, 1),
                "`{query}` no longer takes the split route:\n{compiled}"
            );
            assert_eq!(
                "split aggregate",
                route_answer(&compiled, 1, &one_source_tags()),
                "`{query}`"
            );
            // A split inserts nothing, so the finalize is handed the tree as compiled.
            let routed = routed_tree(query);
            assert_eq!(
                0,
                routed.to_string().matches("Merge").count(),
                "`{query}`: the split route was handed a merge:\n{routed}"
            );
            assert!(
                AggregateFinalize::can_split(&routed, &fields, "ts"),
                "`{query}`: the split route was handed a tree it cannot cut:\n{routed}"
            );

            // What the pass would have done to this tree is exactly what takes the split away: with
            // it, no placement is a split any more, so the route could not offer one.
            let passed = passed_tree(query, 1);
            assert!(
                !AggregateFinalize::can_split(&passed, &fields, "ts"),
                "`{query}`: the pass left the split intact, so this test no longer guards the order:\n{passed}"
            );
            assert_ne!(
                Some(NativePlacement::SplitAggregate),
                placement_of(&passed, 1),
                "`{query}`: a passed tree still reads as a split"
            );
        }
    }

    /// The per-series counterpart: a tree that answers each series from its own rows keeps the
    /// per-partition placement, and that placement inserts nothing.
    ///
    /// One execution reads one scan partition, and a scan keeps each series whole in one of them, so
    /// per-series work is already answered correctly there. A merge inserted on this placement could
    /// only collect the fragments of the partition it happens to run on - a partial merge presented
    /// as a canonical series - so the tree is handed over as compiled and cross-partition aliasing
    /// stays unsolved until a repartition node exists.
    #[test]
    fn the_insertion_pass_does_not_take_a_per_series_tree_off_the_route() {
        for query in ["rate(x[5m])", "x", "abs(x)", "x * 2"] {
            let compiled = compiled_test_tree(query);
            assert_eq!(
                Some(NativePlacement::PerPartition),
                placement_of(&compiled, 1),
                "`{query}` is not per-series work:\n{compiled}"
            );
            assert_eq!(
                "per-partition",
                route_answer(&compiled, 1, &one_source_tags()),
                "`{query}`"
            );
            // The placement inserts nothing, so what the node is handed is the compiled tree.
            let routed = routed_tree(query);
            assert_eq!(
                0,
                routed.to_string().matches("Merge").count(),
                "`{query}`: the per-partition route was handed a merge:\n{routed}"
            );
            assert!(
                !reads_across_series(routed.root()),
                "`{query}`: the per-partition route was handed a tree that reads across series:\n{routed}"
            );

            // And the pass would take this tree off the per-partition placement, which is the other
            // half of why the order matters: a merge reads across series.
            let passed = passed_tree(query, 1);
            assert_ne!(
                Some(NativePlacement::PerPartition),
                placement_of(&passed, 1),
                "`{query}`: a passed tree still reads as per-partition:\n{passed}"
            );
        }
    }

    /// A tree whose single execution reads every input partition is the one placement the pass
    /// inserts into, and no shape the route plans today gets that far.
    ///
    /// `topk`, `sort`, `absent` and an aggregate the split refuses all read the whole vector, so the
    /// route decides a single execution for them and runs the pass, which puts a merge below every
    /// consumer that needs a whole canonical series - here under the vector operation and again at
    /// the root. The tree stays a single execution holding the labels it held, so the placement and
    /// the shape survive the rewrite.
    ///
    /// The route then declines the tree anyway, and for a reason the pass cannot reach: the labels
    /// the fused node must declare are derived by [`fused_output_tags`], which names none for an
    /// aggregate or a vector root. So the merge is inserted into a tree that is then not built, which
    /// is why no plan the route produces today carries one.
    #[test]
    fn a_single_execution_tree_gains_a_merge_and_the_route_declines_it() {
        for query in ["topk(1, x)", "sort(x)", "absent(x)", "quantile(0.9, x)"] {
            let compiled = compiled_test_tree(query);
            assert_eq!(
                Some(NativePlacement::SingleExecution),
                placement_of(&compiled, 1),
                "`{query}` is not a single execution:\n{compiled}"
            );
            assert!(
                needs_single_execution(compiled.root()),
                "`{query}`: the placement and the tree disagree"
            );
            // The pass runs for this placement, and it is the only one it runs for.
            let routed = routed_tree(query);
            assert!(
                routed.to_string().matches("Merge").count() >= 2,
                "`{query}`: the single-execution tree gained no merge:\n{routed}"
            );
            assert_ne!(
                compiled.to_string(),
                routed.to_string(),
                "`{query}`: the route handed over the tree as compiled"
            );
            // The insertion leaves the placement and the labels alone.
            assert!(
                needs_single_execution(routed.root()),
                "`{query}`: the merge took the single execution away:\n{routed}"
            );
            assert_eq!(
                fused_output_tags(compiled.root(), &one_source_tags()),
                fused_output_tags(routed.root(), &one_source_tags()),
                "`{query}`: the pass changed the labels the node declares"
            );
            // And the route still leaves it to the existing planner, because the labels a root of
            // this shape reports are not the ones the fused node can declare.
            assert_eq!(
                "legacy plan",
                route_answer(&compiled, 1, &one_source_tags()),
                "`{query}`"
            );
            assert_eq!(
                "legacy plan",
                route_answer(&routed, 1, &one_source_tags()),
                "`{query}`: the insertion changed the route's answer:\n{routed}"
            );
        }
    }

    /// The rows a native tree answers when `pushes` are fed to its one input, as each output
    /// series' label set and the values it holds at each grid position.
    ///
    /// A series may arrive as one chunk or as several - a chunk says which series it carries and
    /// closes it - so rows are appended to the entry for their label set and a series split over two
    /// chunks reads as one. That is what makes the map comparable between a tree that answers
    /// fragments and one whose inserted merge collected them.
    fn native_rows(
        tree: &NativeTree,
        pushes: Vec<RawSeriesChunk>,
    ) -> BTreeMap<String, Vec<(usize, f64)>> {
        let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
        let state = NativeExecutionState::try_new(tree, Arc::clone(&pool))
            .expect("the runtime executes this tree");
        let ids = state.input_ids(0).collect::<Vec<_>>();
        assert_eq!(ids.len(), 1, "a one-selector tree has one runtime input");
        let mut executor = NativeExecutor::new(state);
        for chunk in pushes {
            executor
                .push_raw(ids[0], chunk)
                .expect("the chunk is pushed to the tree's input");
        }
        let chunks = executor.finish().expect("every series closed");
        assert_eq!(pool.reserved(), 0, "a finished execution reserves nothing");

        let mut rows: BTreeMap<String, Vec<(usize, f64)>> = BTreeMap::new();
        for chunk in &chunks {
            let metric = &chunk.series.metric;
            let mut parts = Vec::new();
            if let Some(name) = metric.name() {
                parts.push(format!("__name__=\"{name}\""));
            }
            for label in metric.tags() {
                parts.push(format!("{}=\"{}\"", label.name, label.value));
            }
            let field = chunk
                .series
                .fields
                .as_slice()
                .first()
                .expect("the result carries the one field it was bound to");
            let entry = rows.entry(format!("{{{}}}", parts.join(", "))).or_default();
            for (position, row) in chunk.series.time.positions() {
                match field.sample(row) {
                    SampleRef::Float(value) => entry.push((position, value)),
                    _ => panic!("the row at {row} holds no float"),
                }
            }
        }
        rows
    }

    /// One raw chunk of the series `x{host=<host>}`, holding `rows` at their source timestamps.
    ///
    /// `is_last` says this chunk closes the series, so one label set can be handed over as two
    /// fragments - which is what one scan partition's pieces of a series look like.
    fn raw_chunk_of(host: &str, key: &[u8], rows: &[(i64, f64)], is_last: bool) -> RawSeriesChunk {
        let timestamps = rows.iter().map(|&(time, _)| time).collect::<Vec<_>>();
        let values = rows.iter().map(|&(_, value)| value).collect::<Vec<_>>();
        RawSeriesChunk::try_new(
            &raw_test_schema(),
            Buffer::from_slice_ref(key),
            Some(
                SeriesLabels::try_new(
                    Some("x".into()),
                    vec![Label {
                        name: "host".into(),
                        value: host.into(),
                    }],
                )
                .expect("distinct label names"),
            ),
            RawTimestamps::try_new(ScalarBuffer::from(timestamps)).expect("increasing timestamps"),
            SeriesFields::try_from_vec(vec![Samples::from_float(Float64Array::from(values))])
                .expect("one field"),
            is_last,
        )
        .expect("the chunk is bound to the schema it is built against")
    }

    /// The input a single-execution test reads: `x{host="h1"}` in two fragments, and a second series
    /// `x{host="h2"}` that the `topk` below drops.
    ///
    /// `h1` holds 1, 2 and 3 on the three grid points and `h2` holds a smaller value on each of them,
    /// so `topk(1, x)` keeps the whole of `h1` - which is what makes that series' fragments show up
    /// in the answer rather than being lost.
    fn fragmented_single_execution_input() -> Vec<RawSeriesChunk> {
        vec![
            // One label set handed over as two pieces: the same encoded key, the first chunk not
            // closing the series and the second one closing it, which is what one scan partition's
            // pieces of a series look like.
            raw_chunk_of("h1", b"h1", &[(0, 1.0), (5_000, 2.0)], false),
            raw_chunk_of("h1", b"h1", &[(10_000, 3.0)], true),
            raw_chunk_of(
                "h2",
                b"h2",
                &[(0, 0.25), (5_000, 0.5), (10_000, 0.75)],
                true,
            ),
        ]
    }

    /// The one shape that carries an inserted merge answers the rows the unmerged tree answered.
    ///
    /// [`NativePlacement::SingleExecution`] is the only placement the insertion pass runs on, and
    /// `topk(1, x)` is that placement: one execution reads every scan partition, so the pieces of
    /// one label set arrive together and a merge can collect them. What it may not do is change the
    /// answer, so both trees are executed here over the same input - a series arriving as two
    /// fragments, which is what one partition's pieces of it look like - and the rows are compared.
    /// A merge collects pieces by canonical label set; the operation above it then reads one series
    /// where the unmerged tree read two, and the value at each grid position is the same.
    #[test]
    fn a_single_execution_merge_answers_the_rows_the_unmerged_tree_answered() {
        let query = "topk(1, x)";
        let compiled = compiled_test_tree(query);
        assert_eq!(
            Some(NativePlacement::SingleExecution),
            placement_of(&compiled, 1),
            "{compiled}"
        );
        let inserted = routed_tree(query);
        assert!(
            inserted.to_string().matches("Merge").count() > 0,
            "the single execution did not gain a merge:\n{inserted}"
        );

        let without = native_rows(&compiled, fragmented_single_execution_input());
        let with = native_rows(&inserted, fragmented_single_execution_input());
        assert_eq!(
            without, with,
            "`{query}`: the inserted merge answered different rows:\n{inserted}"
        );
        // The fixture's point: `topk(1, x)` keeps the whole of the one series it picks and drops the
        // other, so the answer is that series' three values.
        assert_eq!(
            vec![vec![(0, 1.0), (1, 2.0), (2, 3.0)]],
            with.into_values().collect::<Vec<_>>(),
            "`{query}`: the answer is not the collected series"
        );
    }

    /// The route's table for the shapes it answers today: the placement each one gets, and whether
    /// that placement inserts anything.
    ///
    /// This is the invariant the order exists for, stated one shape at a time. A merge may only be
    /// inserted where one execution sees every partition, so the shapes that reach a plan today -
    /// per-series work and the aggregate split - carry none, and the one shape that would carry one
    /// is not planned at all. A plan the route returns therefore runs exactly the tree the compiler
    /// produced, and the insertion pass is wired to a placement that no reachable shape takes.
    #[test]
    fn no_shape_the_route_plans_carries_an_inserted_merge() {
        let one = one_source_tags();
        let two = || {
            let mut sources = one.clone();
            sources.push(vec!["host".to_string(), "kind".to_string()]);
            sources
        };
        for (query, selectors, placement, sources) in [
            ("x", 1, NativePlacement::PerPartition, one.clone()),
            ("rate(x[5m])", 1, NativePlacement::PerPartition, one.clone()),
            ("abs(x)", 1, NativePlacement::PerPartition, one.clone()),
            ("x * 2", 1, NativePlacement::PerPartition, one.clone()),
            ("sum(x)", 1, NativePlacement::SplitAggregate, one.clone()),
            (
                "sum(rate(x[5m]))",
                1,
                NativePlacement::SplitAggregate,
                one.clone(),
            ),
            (
                "topk(1, x)",
                1,
                NativePlacement::SingleExecution,
                one.clone(),
            ),
            ("x + on(job) y", 2, NativePlacement::PerPartition, two()),
        ] {
            let compiled = compiled_test_tree(query);
            assert_eq!(
                Some(placement),
                placement_of(&compiled, selectors),
                "`{query}`: the placement changed\n{compiled}"
            );
            let routed = routed_tree(query);
            let merges = routed.to_string().matches("Merge").count();
            let answer = route_answer(&compiled, selectors, &sources);
            if placement == NativePlacement::SingleExecution {
                // The only placement that inserts, and it is not built.
                assert!(
                    merges > 0,
                    "`{query}`: a single execution gained no merge:\n{routed}"
                );
                assert_eq!(
                    "legacy plan", answer,
                    "`{query}`: a single execution is planned, so a plan can carry an inserted \
                     merge"
                );
            } else {
                assert_eq!(
                    0, merges,
                    "`{query}`: the {placement:?} placement inserted a merge:\n{routed}"
                );
                assert_ne!(
                    "legacy plan", answer,
                    "`{query}`: the route builds no plan, so the table row above is vacuous"
                );
            }
        }
    }

    /// The pass's own insertion, kept in view from the planner's side: the window still reads its
    /// raw input and what the pass adds above it is one merge, but a per-partition placement does not
    /// run the pass, so the route hands over the window.
    ///
    /// A merge writes no label of its own, so the shape the pass would produce declares the same tags
    /// the compiled tree declares - which is what lets the walk above stay a function of the tree.
    #[test]
    fn the_insertion_pass_wraps_a_per_series_tree_and_keeps_its_tags() {
        let query = "rate(x[5m])";
        let unpassed = compiled_test_tree(query);
        // The compiled shape: one window reading the scan directly.
        let NativeExpr::Window { input, .. } = &unpassed.root().expr else {
            panic!("the root is not a window: {unpassed}");
        };
        assert!(matches!(input.expr, NativeExpr::Input(_)), "{unpassed}");

        let passed = passed_tree(query, 1);
        let NativeExpr::Merge { input } = &passed.root().expr else {
            panic!("the pass left the window at the root: {passed}");
        };
        // One merge, at the root: the window merges the label sets it can make coincide itself, and
        // the pass has no reason to collect the raw samples a window folds one series at a time.
        assert_eq!(1, passed.to_string().matches("Merge").count(), "{passed}");
        let NativeExpr::Window { input, .. } = &input.expr else {
            panic!("the merge does not sit over the window: {passed}");
        };
        assert!(matches!(input.expr, NativeExpr::Input(_)), "{passed}");

        let sources = one_source_tags();
        assert_eq!(
            fused_output_tags(unpassed.root(), &sources),
            fused_output_tags(passed.root(), &sources),
            "`{query}`: the pass changed the labels the node declares"
        );
        // What the route hands over is the window, not the wrapped tree: a per-partition execution
        // would present one partition's fragments as a canonical series.
        assert_eq!(
            unpassed.to_string(),
            routed_tree(query).to_string(),
            "`{query}`: the route handed over a passed tree"
        );
    }

    /// A canonical merge collects the pieces one label set arrived in and writes no label of its
    /// own, so a tree the insertion pass wrapped in one reports the labels it reported before: the
    /// pass inserts merges, it does not rename the columns the node's schema declares.
    #[test]
    fn a_merge_forwards_its_input_tags() {
        let sources = [
            vec!["host".to_string(), "kind".to_string()],
            vec!["host".to_string(), "zone".to_string()],
        ];
        for (query, expected) in [
            ("x", Some(sources[0].clone())),
            ("rate(x[5m])", Some(sources[0].clone())),
            ("abs(x)", Some(sources[0].clone())),
            ("x * 2", Some(sources[0].clone())),
            // A match narrows the left side before a merge sees it, so the merge has to forward the
            // narrowed list and not the scan's.
            ("x + on(host) y", Some(vec!["host".to_string()])),
        ] {
            let tree = compiled_test_tree(query);
            assert_eq!(
                expected,
                fused_output_tags(tree.root(), &sources),
                "{query}"
            );
            let merged = merge_of(tree.root());
            assert_eq!(
                expected,
                fused_output_tags(&merged, &sources),
                "{query}: a merge changed the labels it hands over"
            );
            // A merge over a merge forwards through both, which is the shape the pass produces when
            // it runs over a tree that already held one.
            assert_eq!(
                expected,
                fused_output_tags(&merge_of(&merged), &sources),
                "{query}: a second merge changed the labels"
            );
        }
    }

    /// `node` wrapped in the merge the insertion pass inserts, as a legal node.
    fn merge_of(node: &NativeNode) -> NativeNode {
        NativeNode {
            expr: NativeExpr::Merge {
                input: Box::new(node.clone()),
            },
            output: node.output.clone(),
        }
    }

    #[test]
    fn single_selector_admission_accepts_literal_broadcasts_and_unary() {
        for query in ["x * 2", "2 - x", "(x * 2) + 3", "-x"] {
            let expression = promql_parser::parser::parse(query).unwrap();
            assert!(admitted_selector(&expression).is_some(), "{query}");
        }
        for query in ["time() + x", "scalar(x) + x", "x + y"] {
            let expression = promql_parser::parser::parse(query).unwrap();
            assert!(admitted_selector(&expression).is_none(), "{query}");
        }
        for query in [
            "x",
            "rate(x[5s])",
            "sum_over_time(x[5s])",
            "abs(x)",
            "timestamp(x)",
            "timestamp(abs(x))",
        ] {
            let expression = promql_parser::parser::parse(query).unwrap();
            assert!(admitted_selector(&expression).is_some(), "{query}");
        }
        for query in ["sum(x)", "sum by (job) (rate(x[5m]))"] {
            let expression = promql_parser::parser::parse(query).unwrap();
            assert!(admitted_single_selector(&expression), "{query}");
        }
    }

    fn hybrid_admission(query: &str) -> bool {
        let expression = promql_parser::parser::parse(query).unwrap();
        let promql_parser::parser::Expr::Binary(binary) = expression else {
            panic!("expected binary expression: {query}");
        };
        hybrid_binary_admitted(binary.op, &binary.modifier, &binary.lhs, &binary.rhs)
    }

    #[test]
    fn hybrid_binary_admission_is_narrow() {
        for query in ["A / B", "rate(A[1m]) / rate(B[1m])"] {
            assert!(hybrid_admission(query), "{query}");
        }
        assert!(hybrid_admission("A + on(job) B"));
        for query in [
            "A and B",
            "A + on() B",
            "A + on(job) group_left B",
            "sum(A) / B",
            "100 / B",
            "A / (B + C)",
        ] {
            assert!(!hybrid_admission(query), "{query}");
        }
    }

    #[test]
    fn millisecond_scan_bounds_preserve_open_closed_and_maximum_edges() {
        let inclusive = scan_time_filter(0, 10, true, "ts").to_string();
        assert!(inclusive.contains(">="), "{inclusive}");
        assert!(inclusive.contains("<="), "{inclusive}");

        let exclusive = scan_time_filter(0, 10, false, "ts").to_string();
        assert!(exclusive.contains("> "), "{exclusive}");
        assert!(!exclusive.contains(">="), "{exclusive}");
        assert!(exclusive.contains("<="), "{exclusive}");

        let maximum = scan_time_filter(i64::MAX, i64::MAX, false, "ts").to_string();
        assert!(maximum.contains("> "), "{maximum}");
        assert!(!maximum.contains(">="), "{maximum}");
        assert!(maximum.contains("<="), "{maximum}");
    }

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
