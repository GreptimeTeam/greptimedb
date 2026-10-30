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
//! A statement that reads no data - a constant, `time()`, `vector(time())` - is planned with no
//! scan: one node, no child, the time index and value.

use std::borrow::Cow;
use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

use common_catalog::consts::{METRIC_ENGINE, MITO_ENGINE, MITO2_ENGINE};
use common_query::prelude::OTLP_AGGREGATION_TEMPORALITY_LABEL;
use datafusion::arrow::datatypes::{DataType as ArrowDataType, Field, TimeUnit as ArrowTimeUnit};
use datafusion::common::{DFSchema, DFSchemaRef};
use datafusion::error::DataFusionError;
use datafusion::execution::memory_pool::UnboundedMemoryPool;
use datafusion::logical_expr::{Extension, LogicalPlan, LogicalPlanBuilder, TableScanBuilder};
use datafusion::prelude::Expr as DfExpr;
use datafusion_expr::Expr;
use datatypes::series::SeriesOrdering;
use datatypes::series::samples::{FieldSpec, SeriesSchema};
use datatypes::series::time::EvalGrid;
use promql::extension_plan::{
    AggregateFinalize, FusedOutputFormat, FusedSeries, FusedSeriesBinding, InputLocality,
    RawSeriesSource, input_locality,
};
use promql::native::execute::NativeExecutionState;
use promql::native::plan::{
    BoundAggregate, BoundInput, BoundVectorOp, Grouping, MapOp, NativeExpr, NativeNode, NativeTree,
    NativeType, reads_across_series,
};
use promql::vector_match::{Cardinality, MatchingLabels, SetOp};
use promql_parser::label::{METRIC_NAME, Matchers};
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
    /// `query_engine_state` is only read to build the *existing* plan of the same statement, for the
    /// column names the existing path gives it: they are what the rows of a split aggregate are named
    /// after, and what names the tags of a tree whose labels the walk below does not derive. That
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
        if selectors.is_empty() {
            let tree = match compile_statement(&stmt) {
                Ok(tree) => tree,
                Err(DataFusionError::NotImplemented(_)) => return Ok(None),
                Err(error) => return Err(error).context(DataFusionPlanningSnafu),
            };
            // A statement that reads no data has no scan: no metric, no tags, and the value under
            // the name the existing path gives a bare literal, or the one `vector()` binds. A tree
            // whose labels the walk cannot derive, such as `count_values("l", vector(1))`, would
            // lose the labels it generates, so it is left to the existing plan.
            if fused_output_tags(tree.root(), &[]).is_none_or(|tags| !tags.is_empty()) {
                return Ok(None);
            }
            let time_index = DEFAULT_TIME_INDEX_COLUMN.to_string();
            let values = match &tree.root().output {
                NativeType::Samples(schema) => schema
                    .fields
                    .iter()
                    .map(|field| field.name.to_string())
                    .collect(),
                _ => vec![DEFAULT_FIELD_COLUMN.to_string()],
            };
            let output_schema = fused_output_schema(&values, &[], &time_index)?;
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
            // The first metric logical-table id a selector's rewrite carried, and whether a later one
            // carried a different id. See the `cross_table` use below.
            let mut logical_scan_id: Option<u32> = None;
            let mut logical_ids_differ = false;
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
                // A metric logical table's id is part of the identity its own scan reads: two
                // logical tables served through one physical table prepare two scans of that one
                // table, and the ids are what says they are two metrics. A selector whose scan is not
                // a logical rewrite contributes no id, and a missing id on either side is not a
                // difference.
                if let Some((_, _, table_id)) = &logical_scan {
                    match logical_scan_id {
                        None => logical_scan_id = Some(*table_id),
                        Some(first) if first != *table_id => logical_ids_differ = true,
                        Some(_) => {}
                    }
                }
                let scan = match logical_scan {
                    Some((mut scan, predicate, _)) => {
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
                // default `None`. A metric-engine selector is admitted through its verified logical
                // rewrite, with no separate multi-selector exclusion: each input is a metric logical
                // table served through that rewrite, and the ids those rewrites carry are what tells
                // two different ones apart below. Two occurrences of one logical table stay two
                // scans of the one physical table, answered where that table's region runs them.
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
            Ok(Some((prepared, single_input_context, logical_ids_differ)))
        }
        .await;
        self.ctx = original_context;
        let Some((mut prepared, single_input_context, logical_ids_differ)) = preparation? else {
            return Ok(None);
        };
        // Selectors over one table share one region, so the node runs there over every scan. When
        // they read more than one table, each scan answers from its own region and the node runs
        // above them; see `RawSeriesSource`. A metric logical table's id is part of the identity it
        // reads: two ids are two metrics even when one physical table holds both, so two selectors
        // that differ only by that id still read different series and are bound as two inputs.
        let cross_table =
            !same_table_scans(prepared.iter().map(|(scan, ..)| scan)) || logical_ids_differ;
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
        // A segment carries one source's identity. Keeping each occurrence's scan and metadata
        // aligned here lets the cutting step name that source independently of selector count.
        let bare_scans = prepared
            .iter()
            .map(|(scan, ..)| scan.clone())
            .collect::<Vec<_>>();
        let per_source = prepared
            .iter()
            .map(|(_, _, metric, time_index, tags)| {
                (metric.clone(), time_index.clone(), tags.clone())
            })
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
        // Ordinary pointwise trees and trees of one-to-one matches, on one table or across
        // several, are answered per partition. A tree that folds across series - an aggregate - is
        // not, and one selector's fold is carried by the aggregate split below.
        let root = compiled.tree.root();
        let per_partition =
            !reads_across_series(root) || pointwise_operand(root) || independent_scan_pair(root);
        // The split is only ever a one-child plan: it exports each partition's leaf state and merges
        // them above the region. A tree over two metric logical tables of ONE physical table is not
        // split, because a node whose locality is `SinglePartition` normalizes every input to one
        // partition, so every group is folded in one place. Both of those inputs are `RawSeriesSource` children
        // (the two ids made `cross_table` true), each answering from its own region with frames that
        // carry their own labels, so the node is plannable above both. That plan needs no split to be
        // correct, so it falls through to the node below.
        //
        // A multi-selector tree whose inputs stay direct scans of one table is not admitted here: its
        // scans are not raw sources, so the categorizer refuses to place the node region-locally
        // (`Categorizer::check_extension_plan` is `NonCommutative` for a cross-series node over more
        // than one scan) and the physical planner cannot bind a merged plain scan under it either. So
        // such a tree keeps its one-selector-or-decline behaviour, unchanged.
        //
        // A selector whose scan carries no metric logical id - a Mito table's, for one - adds no
        // id, so one Mito aggregate beside one metric aggregate is not a multi-input tree:
        // `sum(m) + sum(mito)` keeps the existing plan. A second metric logical id does make the
        // tree multi-input, and the one execution above then reads the Mito input through its own
        // raw source as well.
        let multi_input = selectors.len() > 1 && logical_ids_differ;
        // A multi-input tree skips the split below, so the refusals the split makes for reasons that
        // are not about what the runtime computes have to be made here.
        //
        // The aggregate split keeps its prefix per-series, which is why it declines an operation
        // whose answer depends on the order its input arrives in. A multi-input node reads its
        // inputs' partitions in the order they became ready, so that refusal is repeated here.
        if multi_input && depends_on_input_order(root) {
            return Ok(None);
        }
        // The split names the columns its groups report from the existing planner. A grouping by the
        // metric name keeps a label no column of the existing plan can name, so the schema the node
        // declares would report one label set for groups the tree keeps apart.
        if multi_input && groups_by_metric_name(root) {
            return Ok(None);
        }
        // Catch construction-time refusals before choosing native execution: errors raised during
        // physical planning cannot fall back. This includes unsupported aggregates and `topk`,
        // `limitk` or CountValues groupings; it does not cover data-dependent errors raised while
        // executing the tree.
        if multi_input
            && NativeExecutionState::try_new(
                &compiled.tree,
                Arc::new(UnboundedMemoryPool::default()),
            )
            .is_err()
        {
            return Ok(None);
        }
        if !per_partition && !multi_input {
            // A tree whose fold can be split keeps that plan: it exports aggregate state rather
            // than whole samples, so it moves less between the two halves than a sample boundary
            // would. The general segmentation below is the fallback for the trees it declines,
            // which are the ones its per-series prefix cannot express.
            if selectors.len() == 1
                && AggregateFinalize::can_split(&compiled.tree, &series_schema.fields, &time)
            {
                let (legacy_fields, legacy_tags) = self
                    .legacy_output_columns(prom_expr, query_engine_state)
                    .await?;
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
            // The segmentation below is for a statement of one selector: it cuts that selector's
            // per-series prefix out from under a whole-vector node. A statement reading several
            // selectors has more than one such prefix and a matching relation between them, which
            // this route does not place yet, so it keeps the plan it had.
            if selectors.len() != 1 {
                return Ok(None);
            }
            let plan = Box::pin(segment_tree(
                self,
                prom_expr,
                query_engine_state,
                &compiled.tree,
                &children,
                &source_tags,
                &bare_scans,
                &per_source,
                &grid,
                &time,
                &tags,
                &metric,
            ))
            .await?;
            return match plan {
                Some(plan) => Ok(Some((plan, single_input_context))),
                None => Ok(None),
            };
        }
        let tree = compiled.tree;
        // The tags are asked for before the fields, because the walk answers for a tree the
        // compiler produced: a scalar root has no labels at all, and a sample root's are the ones
        // the walk derives or - where it derives none - the ones the existing planner names.
        let output_tags = match &tree.root().output {
            // A scalar has no labels, and its value is named as a bare literal's is.
            NativeType::Scalar(_) => Vec::new(),
            NativeType::Samples(_) => match fused_output_tags(tree.root(), &source_tags) {
                Some(output_tags) => output_tags,
                None => {
                    // The walk cannot name this tree's labels, so the columns the existing planner
                    // names for the same statement are the answer rather than a decline.
                    let (_, legacy_tags) = self
                        .legacy_output_columns(prom_expr, query_engine_state)
                        .await?;
                    // Only the tag columns are copied: the node names its field columns after the
                    // tree's own root schema, while the existing planner can name a computed column
                    // after its expression, so its field names are not this node's. A tag that
                    // collides with a field or the time index is still rejected by the uniqueness
                    // check below.
                    legacy_tags
                }
            },
            NativeType::AggregateState(_) => return Ok(None),
        };
        let root_fields = match &tree.root().output {
            // The fields are the compiled root's own: a raw field's name, or the one `vector(s)`
            // gives its result.
            NativeType::Samples(schema) => schema
                .fields
                .iter()
                .map(|f| f.name.to_string())
                .collect::<Vec<_>>(),
            // A scalar has no labels, and its value is named as a bare literal's is.
            NativeType::Scalar(_) => vec![DEFAULT_FIELD_COLUMN.to_string()],
            NativeType::AggregateState(_) => return Ok(None),
        };
        // A root field can share a name with a tag or the time index, and under `or` the right
        // table's tags join the left table's columns. The output names each column once, so such a
        // statement keeps the existing plan.
        let mut column_names = std::collections::HashSet::new();
        if !std::iter::once(&time)
            .chain(&root_fields)
            .chain(&output_tags)
            .all(|name| column_names.insert(name.as_str()))
        {
            return Ok(None);
        }
        let output_schema = fused_output_schema(&root_fields, &output_tags, &time)?;
        // Across tables the region scans are bound by their `RawSeriesSource` children, and this is
        // also the binding a node over more than one input needs: such a node reads frames that
        // already carry their own labels, so it binds neither tags nor one metric name of its own -
        // each child names the metric it reads. A multi-input plan whose children stay direct
        // scans reads one table through the one metric and tag set they share, and keeps this
        // binding, which is the same identity those scans carry.
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

    /// The field and tag columns the *existing* planner names for `prom_expr`.
    ///
    /// The names are read off the plan the existing path builds for the same statement, and that
    /// plan is then dropped: it is never executed and never stored, so the batch that answers the
    /// query is the native route's own. A statement the existing planner cannot plan fails here the
    /// same way it fails there, because the existing plan is the answer for this statement: an error
    /// planning it is the error the statement has, whichever route asked.
    ///
    /// Only the planner context is saved and restored: the table provider's resolved-table cache
    /// keeps the entries this speculative planning resolved, exactly as the aggregate-split branch,
    /// which planned the same way before this helper existed, always did.
    async fn legacy_output_columns(
        &mut self,
        prom_expr: &PromExpr,
        query_engine_state: &QueryEngineState,
    ) -> Result<(Vec<String>, Vec<String>)> {
        let saved = self.ctx.clone();
        let planned = self
            .prom_root_expr_to_plan(prom_expr, query_engine_state)
            .await;
        let legacy_fields = self.ctx.field_columns.clone();
        let legacy_tags = self.ctx.tag_columns.clone();
        self.ctx = saved;
        drop(planned?);
        Ok((legacy_fields, legacy_tags))
    }

    /// The columns a [`FusedSeries`] node over `tree` produces.
    ///
    /// The field names are the compiled root's own, and the tag names are the ones the tree
    /// derives. Where the tree cannot name them - a node this walk does not model - the existing
    /// planner's tag list is the answer rather than a decline, because the statement is still
    /// answerable and its tags are the ones that plan reports.
    ///
    /// `None` when the root is not a value the node can publish, or when a field would share a
    /// name with a tag or the time index: the output names each column once.
    async fn fused_root_schema(
        &mut self,
        prom_expr: &PromExpr,
        query_engine_state: &QueryEngineState,
        tree: &NativeTree,
        source_tags: &[Vec<String>],
        time: &str,
    ) -> Option<DFSchemaRef> {
        let output_tags = match &tree.root().output {
            NativeType::Scalar(_) => Vec::new(),
            NativeType::Samples(_) => match fused_output_tags(tree.root(), source_tags) {
                Some(output_tags) => output_tags,
                None => {
                    let (_, legacy_tags) = self
                        .legacy_output_columns(prom_expr, query_engine_state)
                        .await
                        .ok()?;
                    legacy_tags
                }
            },
            NativeType::AggregateState(_) => return None,
        };
        let root_fields = match &tree.root().output {
            NativeType::Samples(schema) => schema
                .fields
                .iter()
                .map(|field| field.name.to_string())
                .collect::<Vec<_>>(),
            NativeType::Scalar(_) => vec![DEFAULT_FIELD_COLUMN.to_string()],
            NativeType::AggregateState(_) => return None,
        };
        let mut column_names = std::collections::HashSet::new();
        let columns = std::iter::once(time)
            .chain(root_fields.iter().map(String::as_str))
            .chain(output_tags.iter().map(String::as_str));
        if !columns.clone().all(|name| column_names.insert(name)) {
            return None;
        }
        fused_output_schema(&root_fields, &output_tags, time).ok()
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

/// The subtrees a [`NativeNode`] reads, in the order the node evaluates them.
fn node_children(expr: &NativeExpr) -> Vec<&NativeNode> {
    match expr {
        NativeExpr::Input(_) | NativeExpr::Scalar(_) | NativeExpr::EvalTime { .. } => Vec::new(),
        NativeExpr::VectorOf { input }
        | NativeExpr::Instant { input, .. }
        | NativeExpr::Merge { input }
        | NativeExpr::Window { input, .. }
        | NativeExpr::Map { input, .. }
        | NativeExpr::Timestamp { input }
        | NativeExpr::Aggregate { input, .. }
        | NativeExpr::Vector { input, .. } => vec![input],
        NativeExpr::ParameterizedMap {
            input, parameters, ..
        } => std::iter::once(input.as_ref())
            .chain(parameters.iter())
            .collect(),
        NativeExpr::Binary { left, right, .. } | NativeExpr::Set { left, right, .. } => {
            vec![left, right]
        }
        NativeExpr::Info { base, info, .. } => vec![base, info],
    }
}

/// `expr` with its children replaced, in the order [`node_children`] reports them.
fn with_node_children(
    expr: &NativeExpr,
    mut children: std::vec::IntoIter<NativeNode>,
) -> NativeExpr {
    let mut next = || children.next().expect("one replacement per child");
    match expr {
        NativeExpr::Input(_) | NativeExpr::Scalar(_) | NativeExpr::EvalTime { .. } => expr.clone(),
        NativeExpr::VectorOf { .. } => NativeExpr::VectorOf {
            input: Box::new(next()),
        },
        NativeExpr::Instant { op, .. } => NativeExpr::Instant {
            input: Box::new(next()),
            op: op.clone(),
        },
        NativeExpr::Merge { .. } => NativeExpr::Merge {
            input: Box::new(next()),
        },
        NativeExpr::Window { op, .. } => NativeExpr::Window {
            input: Box::new(next()),
            op: op.clone(),
        },
        NativeExpr::Map { op, .. } => NativeExpr::Map {
            input: Box::new(next()),
            op: op.clone(),
        },
        NativeExpr::ParameterizedMap { op, parameters, .. } => NativeExpr::ParameterizedMap {
            input: Box::new(next()),
            op: *op,
            parameters: (0..parameters.len()).map(|_| next()).collect(),
        },
        NativeExpr::Timestamp { .. } => NativeExpr::Timestamp {
            input: Box::new(next()),
        },
        NativeExpr::Aggregate { op, .. } => NativeExpr::Aggregate {
            input: Box::new(next()),
            op: op.clone(),
        },
        NativeExpr::Vector { op, .. } => NativeExpr::Vector {
            input: Box::new(next()),
            op: op.clone(),
        },
        NativeExpr::Binary { matching, .. } => NativeExpr::Binary {
            left: Box::new(next()),
            right: Box::new(next()),
            matching: matching.clone(),
        },
        NativeExpr::Set { op, .. } => NativeExpr::Set {
            left: Box::new(next()),
            right: Box::new(next()),
            op: op.clone(),
        },
        NativeExpr::Info { selector, .. } => NativeExpr::Info {
            base: Box::new(next()),
            info: Box::new(next()),
            selector: selector.clone(),
        },
    }
}

/// Whether a segment can hand `node`'s values to another segment.
///
/// A boundary carries an evaluated series frame: values addressed on a grid, under the labels of
/// the series they belong to. A scalar is broadcast rather than carried, aggregate state is
/// internal, and raw samples are what a scan produces - none of them is a frame, so a subtree
/// producing one stays inside the segment that reads it.
fn exportable(node: &NativeNode) -> bool {
    node.output
        .samples()
        .is_some_and(|schema| schema.eval_grid.is_some())
}

/// Whether `node` contains a per-series prefix that a boundary could carry.
///
/// The cut is only ever between a node that must gather and a subtree that does not need to: that
/// subtree is the prefix the gather would otherwise swallow. Without one, a tree that reads across
/// series has nothing to hand over, and splitting it would only move the same work up a level.
fn carries_prefix(node: &NativeNode) -> bool {
    let expr = &node.expr;
    node_children(expr)
        .into_iter()
        .any(|child| cut_as_segment(child) || carries_prefix(child))
}

/// Whether `node` becomes a segment of its own under a parent that must gather.
///
/// The parent reads across series, so it answers one partition and gathers whatever it reads. A
/// subtree that answers each series on its own partitions does not need that gather, and keeping it
/// inside the parent would apply the gather to it too. Cutting it out leaves its work on however
/// many partitions its own input has.
///
/// Both ends of the comparison come from the classification the physical planner applies to the
/// tree it builds, so a cut here is a tree the physical planner would also have had to gather.
fn cut_as_segment(node: &NativeNode) -> bool {
    exportable(node) && !matches!(input_locality(node), InputLocality::SinglePartition)
}

/// One segment: its tree rewritten to read only its own inputs, the scans it reads, and the
/// subtrees cut out of it.
///
/// A segment has one index space of its own - the scans it reads first, in the order it first names
/// them, then the subtrees cut out of it - because `FusedSeries` requires its source indices to be
/// dense over its children.
struct Segment {
    node: NativeNode,
    /// Original scan indices, in the order this segment's indices name them.
    sources: Vec<usize>,
    /// The subtrees cut out of this segment, each still in the original index space.
    cut: Vec<NativeNode>,
}

/// Collects a segment's scans and cut subtrees, without descending into a cut.
fn collect_segment(node: &NativeNode, sources: &mut Vec<usize>, cut: &mut Vec<NativeNode>) {
    match &node.expr {
        NativeExpr::Input(input) => {
            if !sources.contains(&input.source_index) {
                sources.push(input.source_index);
            }
        }
        expr => {
            for child in node_children(expr) {
                if cut_as_segment(child) {
                    cut.push(child.clone());
                } else {
                    collect_segment(child, sources, cut);
                }
            }
        }
    }
}

/// Collects every scan input in a selected segment without looking for nested cuts.
fn collect_segment_inputs(node: &NativeNode, sources: &mut Vec<usize>) {
    match &node.expr {
        NativeExpr::Input(input) => {
            if !sources.contains(&input.source_index) {
                sources.push(input.source_index);
            }
        }
        expr => {
            for child in node_children(expr) {
                collect_segment_inputs(child, sources);
            }
        }
    }
}

/// Rewrites every input leaf in a selected segment to its index in the segment's own space.
fn rewrite_inner_segment(node: &NativeNode, sources: &[usize]) -> NativeNode {
    let expr = match &node.expr {
        NativeExpr::Input(input) => NativeExpr::Input(BoundInput {
            source_index: sources
                .iter()
                .position(|&source| source == input.source_index)
                .expect("the segment collected every scan it reads"),
            output: input.output.clone(),
            ordering: input.ordering,
        }),
        expr => with_node_children(
            expr,
            node_children(expr)
                .into_iter()
                .map(|child| rewrite_inner_segment(child, sources))
                .collect::<Vec<_>>()
                .into_iter(),
        ),
    };
    NativeNode {
        expr,
        output: node.output.clone(),
    }
}

/// Rewrites a segment's tree so every leaf names its index in the segment's own space.
fn rewrite_segment(
    node: &NativeNode,
    sources: &[usize],
    cut_base: usize,
    next_cut: &mut usize,
) -> NativeNode {
    let expr = match &node.expr {
        NativeExpr::Input(input) => NativeExpr::Input(BoundInput {
            source_index: sources
                .iter()
                .position(|&source| source == input.source_index)
                .expect("the segment collected every scan it reads"),
            output: input.output.clone(),
            ordering: input.ordering,
        }),
        expr => {
            let children = node_children(expr)
                .into_iter()
                .map(|child| {
                    if cut_as_segment(child) {
                        let index = cut_base + *next_cut;
                        *next_cut += 1;
                        NativeNode {
                            expr: NativeExpr::Input(BoundInput {
                                source_index: index,
                                output: child.output.clone(),
                                ordering: SeriesOrdering::Contiguous,
                            }),
                            output: child.output.clone(),
                        }
                    } else {
                        rewrite_segment(child, sources, cut_base, next_cut)
                    }
                })
                .collect::<Vec<_>>();
            with_node_children(expr, children.into_iter())
        }
    };
    NativeNode {
        expr,
        output: node.output.clone(),
    }
}

/// Splits `node` into one segment and the subtrees cut out of it.
fn cut_segments(node: &NativeNode) -> Segment {
    let mut sources = Vec::new();
    let mut cut = Vec::new();
    collect_segment(node, &mut sources, &mut cut);
    let cut_base = sources.len();
    let mut next_cut = 0;
    let rewritten = rewrite_segment(node, &sources, cut_base, &mut next_cut);
    Segment {
        node: rewritten,
        sources,
        cut,
    }
}

/// Builds one segment's plan from a subtree, binding its scan inputs without cutting again.
///
/// Returns the plan and the output columns the segment reports, which its parent names its own
/// columns by. `None` when any part cannot be built, so the statement keeps the plan it had.
#[allow(clippy::type_complexity)]
fn build_inner_segment(
    node: &NativeNode,
    bare_scans: &[LogicalPlan],
    per_source: &[(Option<Arc<str>>, String, Vec<String>)],
    grid: &Arc<EvalGrid>,
) -> Option<(LogicalPlan, Vec<String>)> {
    let mut sources = Vec::new();
    collect_segment_inputs(node, &mut sources);
    // A segment carries one source's identity; naming that source keeps cutting independent of
    // how many selectors the statement has. MatchingLocal can satisfy the cut predicate while
    // reading two sources, but one segment cannot carry both sources' metric, tags, and time index.
    let &[index] = sources.as_slice() else {
        return None;
    };
    let input = bare_scans.get(index)?.clone();
    let (metric, time_index, tags) = per_source.get(index)?;
    let inputs = vec![input];
    let tree = NativeTree::try_new(rewrite_inner_segment(node, &sources)).ok()?;
    let output_tags = fused_output_tags(tree.root(), std::slice::from_ref(tags))?;
    if NativeExecutionState::try_new(&tree, Arc::new(UnboundedMemoryPool::default())).is_err() {
        return None;
    }
    let frame = FusedSeries::series_frame_schema(&tree).ok()?;
    let frame = DFSchema::try_from(frame.as_ref().clone()).ok()?;
    let plan = FusedSeries::try_new_with_format(
        Arc::from("segment"),
        tree,
        FusedSeriesBinding {
            grid: Arc::clone(grid),
            time_index: time_index.clone(),
            tags: tags.clone(),
            metric_name: metric.clone(),
        },
        inputs,
        Arc::new(frame),
        FusedOutputFormat::SeriesFrame,
    )
    .ok()?;
    Some((
        LogicalPlan::Extension(Extension {
            node: Arc::new(plan),
        }),
        output_tags,
    ))
}

/// Plans `tree` as several segments, or `None` when it is one node already.
///
/// A tree that reads across series answers one partition and gathers whatever it reads. Its
/// per-series prefixes do not need that gather, so each becomes a segment of its own and keeps the
/// partitions it was scanned with. Nothing else is cut: a tree that already runs on every partition
/// has nothing to gain, and a tree that matches two vectors routes rather than gathers.
///
/// The cut is the physical planner's own classification of the same subtrees, so a plan this
/// produces is one the physical planner would otherwise have had to gather as a whole.
#[allow(clippy::too_many_arguments, clippy::type_complexity)]
async fn segment_tree(
    planner: &mut PromPlanner,
    prom_expr: &PromExpr,
    query_engine_state: &QueryEngineState,
    tree: &NativeTree,
    children: &[LogicalPlan],
    source_tags: &[Vec<String>],
    bare_scans: &[LogicalPlan],
    per_source: &[(Option<Arc<str>>, String, Vec<String>)],
    grid: &Arc<EvalGrid>,
    time_index: &str,
    tags: &[String],
    metric: &Option<Arc<str>>,
) -> Result<Option<LogicalPlan>> {
    if !matches!(input_locality(tree.root()), InputLocality::SinglePartition) {
        return Ok(None);
    }
    // A segment boundary has to carry something, and the only thing it can carry is a per-series
    // prefix: a subtree that answers each series on its own and can be recomputed. A tree with no
    // such subtree inside it has nothing to cut, and cutting anything else - an aggregate's own
    // state, a constant - is either impossible or a plain recomputation of the same work in one
    // more node. Cutting on locality alone would accept one here.
    if !carries_prefix(tree.root()) {
        return Ok(None);
    }
    // A statement the existing planner refuses is not one this route may answer. Its refusal is the
    // product's answer for that statement - an operation with no kernel, or one whose arguments it
    // rejects - and answering it here would turn an error into a value. Reading the existing plan's
    // columns is how the rest of this route inherits that refusal; this call is that same check for
    // a shape whose columns the derivation can name on its own.
    if planner
        .legacy_output_columns(prom_expr, query_engine_state)
        .await
        .is_err()
    {
        return Ok(None);
    }
    let segment = cut_segments(tree.root());
    if segment.cut.is_empty() {
        return Ok(None);
    }
    let mut inputs = Vec::with_capacity(segment.sources.len() + segment.cut.len());
    let mut child_tags = Vec::with_capacity(inputs.capacity());
    for &index in &segment.sources {
        let Some(child) = children.get(index) else {
            return Ok(None);
        };
        inputs.push(child.clone());
        child_tags.push(source_tags.get(index).cloned().unwrap_or_default());
    }
    for cut in &segment.cut {
        let Some((plan, cut_tags)) = build_inner_segment(cut, bare_scans, per_source, grid) else {
            return Ok(None);
        };
        inputs.push(plan);
        child_tags.push(cut_tags);
    }
    let Some(parent) = NativeTree::try_new(segment.node).ok() else {
        return Ok(None);
    };
    // Nothing below can fall back once physical planning starts, so every tree this route commits
    // to has to be one the runtime executes. A shape the runtime refuses - a grouping by a value
    // field, an operation with no kernel - therefore keeps the plan it had.
    if NativeExecutionState::try_new(&parent, Arc::new(UnboundedMemoryPool::default())).is_err() {
        return Ok(None);
    }
    let Some(output_schema) = planner
        .fused_root_schema(
            prom_expr,
            query_engine_state,
            &parent,
            &child_tags,
            time_index,
        )
        .await
    else {
        return Ok(None);
    };
    let Ok(plan) = FusedSeries::try_new(
        Arc::from(prom_expr.to_string()),
        parent,
        FusedSeriesBinding {
            grid: Arc::clone(grid),
            time_index: time_index.to_string(),
            tags: tags.to_vec(),
            metric_name: metric.clone(),
        },
        inputs,
        output_schema,
    ) else {
        return Ok(None);
    };
    Ok(Some(LogicalPlan::Extension(Extension {
        node: Arc::new(plan),
    })))
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
/// side's; `info` adds the data labels of the info table; an operation with a scalar or a function
/// of each value keeps its operand's; a scalar and `vector(s)` have none. `None` for a node whose
/// labels this walk does not derive - an aggregate's groups, a whole-vector operation, or a label
/// rewrite, which renames the labels below it - so a tree holding one reports no label set here.
/// `None` is not the query's answer: the caller asks the existing planner for the columns the node is
/// named after instead of declining the query.
fn fused_output_tags(node: &NativeNode, source_tags: &[Vec<String>]) -> Option<Vec<String>> {
    match &node.expr {
        NativeExpr::Input(input) => source_tags.get(input.source_index).cloned(),
        NativeExpr::Scalar(_) | NativeExpr::EvalTime { .. } | NativeExpr::VectorOf { .. } => {
            Some(Vec::new())
        }
        NativeExpr::Instant { input, .. }
        | NativeExpr::Window { input, .. }
        | NativeExpr::Timestamp { input } => fused_output_tags(input, source_tags),
        // A label rewrite changes the label set itself: `label_replace` adds its destination label
        // and an empty replacement removes it, so the input's labels are not the output's. A map of
        // math alone keeps them.
        NativeExpr::Map { input, op } => {
            if op.ops.iter().any(|op| matches!(op, MapOp::Label(_))) {
                return None;
            }
            fused_output_tags(input, source_tags)
        }
        // Its parameters are scalars, which carry no labels, so what the node reports are its
        // input's labels.
        NativeExpr::ParameterizedMap { input, .. } => fused_output_tags(input, source_tags),
        // An aggregate reports the grouping labels its input carries: `by(...)` keeps exactly the
        // ones it names, and `without(...)` keeps the rest and drops the metric name, as Prometheus
        // does.
        //
        // What this derives is the set of *output columns*, which is what the node's schema needs.
        // It is not the complete runtime label set: a chunk's labels also carry the metric name,
        // which is bound separately from these tag columns rather than being one of them. A
        // grouping by `__name__` keeps that name in the labels while contributing no column here,
        // and `sum by (__name__)` over one metric is still one column set.
        NativeExpr::Aggregate { input, op } => {
            let input_tags = fused_output_tags(input, source_tags)?;
            Some(match &op.group {
                Grouping::By(kept) => input_tags
                    .into_iter()
                    .filter(|tag| kept.iter().any(|name| name.as_ref() == tag.as_str()))
                    .collect(),
                Grouping::Without(dropped) => input_tags
                    .into_iter()
                    .filter(|tag| {
                        tag != METRIC_NAME
                            && !dropped.iter().any(|name| name.as_ref() == tag.as_str())
                    })
                    .collect(),
            })
        }
        // Selecting some of the input's series keeps each one's own labels; `count_values` writes a
        // new label and `histogram` folds bucket series into one histogram, so neither reports the
        // input's set.
        NativeExpr::Vector { input, op } => match op {
            BoundVectorOp::TopK { .. }
            | BoundVectorOp::LimitK { .. }
            | BoundVectorOp::LimitRatio { .. }
            | BoundVectorOp::Sort(_) => fused_output_tags(input, source_tags),
            // `absent` reports the labels its own matchers name, not its input's.
            BoundVectorOp::CountValues { .. }
            | BoundVectorOp::Histogram(_)
            | BoundVectorOp::Absent { .. } => None,
        },
        NativeExpr::Merge { .. } => None,
        NativeExpr::Binary {
            left,
            right,
            matching,
        } => match (&left.output, &right.output) {
            (NativeType::Scalar(_), _) => fused_output_tags(right, source_tags),
            (_, NativeType::Scalar(_)) => fused_output_tags(left, source_tags),
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

/// Whether `node` reads no data at all: a constant, the evaluation time, or an operation between
/// them. Every execution computes the same value for it.
fn data_free(node: &NativeNode) -> bool {
    match &node.expr {
        NativeExpr::Scalar(_) | NativeExpr::EvalTime { .. } => true,
        NativeExpr::Binary { left, right, .. } => data_free(left) && data_free(right),
        NativeExpr::VectorOf { input } => data_free(input),
        _ => false,
    }
}

/// Whether `node` holds an operation whose answer depends on the order its input arrives in.
///
/// `limitk` keeps the first `k` series of each group *in input order* ([`promql::select::limitk`]).
/// A node with more than one input reads its inputs' partitions through a gather that hands over
/// whichever partition became ready first, so the schedule would decide which series a query keeps
/// and the same data could answer differently between two runs. Prometheus defines the selection
/// over a series set, not over an arrival order.
///
/// Only this operation's *selection* depends on that order. `topk` breaks ties by label and
/// `limit_ratio` selects by a label hash, so a reordered gather changes neither. The instant
/// `sort`s keep tied series in the order they arrived, and a floating-point fold such as `stdvar`
/// can differ in its last bits, but neither changes which series a statement keeps. The aggregate
/// split declines `LimitK` for the same reason, and only a multi-input node repeats that refusal.
fn depends_on_input_order(node: &NativeNode) -> bool {
    match &node.expr {
        NativeExpr::Input(_)
        | NativeExpr::Scalar(_)
        | NativeExpr::EvalTime { .. }
        | NativeExpr::VectorOf { .. } => false,
        NativeExpr::Instant { input, .. }
        | NativeExpr::Merge { input }
        | NativeExpr::Window { input, .. }
        | NativeExpr::Map { input, .. }
        | NativeExpr::Timestamp { input }
        | NativeExpr::Aggregate { input, .. } => depends_on_input_order(input),
        NativeExpr::ParameterizedMap {
            input, parameters, ..
        } => depends_on_input_order(input) || parameters.iter().any(depends_on_input_order),
        NativeExpr::Vector { input, op } => {
            matches!(op, BoundVectorOp::LimitK { .. }) || depends_on_input_order(input)
        }
        NativeExpr::Binary { left, right, .. } | NativeExpr::Set { left, right, .. } => {
            depends_on_input_order(left) || depends_on_input_order(right)
        }
        NativeExpr::Info { base, info, .. } => {
            depends_on_input_order(base) || depends_on_input_order(info)
        }
    }
}

/// Whether a grouping in `node` keeps a label the existing planner's column list cannot name.
///
/// An aggregate's `by(...)` keeps the labels it lists that its input carries, and `__name__` is one
/// of them: the aggregate folds its groups by the metric name as by any other label. It is not a
/// column of any scan, though - it is the name the raw frame carries - so the existing planner's
/// `by(__name__)` finds no such column and reports no label at all. A schema taken from that list
/// would drop the metric name the native aggregate kept, and two groups differing only in their
/// metric would come out as two rows under one label set, which is a series identity lost rather
/// than an answer given.
///
/// `without(...)` needs no such check: both routes drop `__name__` there. Nor does a tree with one
/// selector: it reads one metric, so every group it folds carries the same name and naming that name
/// or dropping it cannot merge two groups. What the check is for is the tree that reads more than one
/// metric, which is the one a multi-input node is bound for.
fn groups_by_metric_name(node: &NativeNode) -> bool {
    let grouping_names_metric = |op: &BoundAggregate| matches!(&op.group, Grouping::By(names) if names.iter().any(|name| name.as_ref() == METRIC_NAME));
    match &node.expr {
        NativeExpr::Input(_)
        | NativeExpr::Scalar(_)
        | NativeExpr::EvalTime { .. }
        | NativeExpr::VectorOf { .. } => false,
        NativeExpr::Instant { input, .. }
        | NativeExpr::Merge { input }
        | NativeExpr::Window { input, .. }
        | NativeExpr::Map { input, .. }
        | NativeExpr::Timestamp { input } => groups_by_metric_name(input),
        NativeExpr::Aggregate { input, op } => {
            grouping_names_metric(op) || groups_by_metric_name(input)
        }
        NativeExpr::ParameterizedMap {
            input, parameters, ..
        } => groups_by_metric_name(input) || parameters.iter().any(groups_by_metric_name),
        NativeExpr::Vector { input, op } => {
            matches!(op, BoundVectorOp::CountValues { group: Grouping::By(names), .. }
                if names.iter().any(|name| name.as_ref() == METRIC_NAME))
                || groups_by_metric_name(input)
        }
        NativeExpr::Binary { left, right, .. } | NativeExpr::Set { left, right, .. } => {
            groups_by_metric_name(left) || groups_by_metric_name(right)
        }
        NativeExpr::Info { base, info, .. } => {
            groups_by_metric_name(base) || groups_by_metric_name(info)
        }
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
        // A parameter that reads no data has the same value in every execution.
        NativeExpr::ParameterizedMap {
            input, parameters, ..
        } => pointwise_operand(input) && parameters.iter().all(data_free),
        // A scalar that reads no data is broadcast over the vector side's positions. Two vectors
        // are a match between two series sets, which is the shape this route cannot split.
        NativeExpr::Binary {
            left,
            right,
            matching,
        } => {
            let scalar = |side: &NativeNode| matches!(side.output, NativeType::Scalar(_));
            matching.cardinality == Cardinality::OneToOne
                && match (scalar(left), scalar(right)) {
                    (true, false) => data_free(left) && pointwise_operand(right),
                    (false, true) => data_free(right) && pointwise_operand(left),
                    _ => false,
                }
        }
        // An aggregate folds a group, a vector operation reads the whole vector, and a bare input or
        // scalar is not a per-series evaluation.
        NativeExpr::Input(_)
        | NativeExpr::Scalar(_)
        | NativeExpr::EvalTime { .. }
        | NativeExpr::VectorOf { .. }
        | NativeExpr::Aggregate { .. }
        | NativeExpr::Vector { .. }
        | NativeExpr::Set { .. }
        | NativeExpr::Merge { .. }
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

    admitted_selector(expr).is_some() || aggregate(expr) || dynamic_scalar(expr)
}

/// Whether `expr` calls `time()`, `vector()` or a date function without an argument.
/// The compiled tree decides how such a statement runs.
fn dynamic_scalar(expr: &PromExpr) -> bool {
    match expr {
        PromExpr::Call(call) => {
            matches!(call.func.name, "time" | "vector")
                || (call.args.args.is_empty()
                    && matches!(
                        call.func.name,
                        "day_of_month"
                            | "day_of_week"
                            | "day_of_year"
                            | "days_in_month"
                            | "hour"
                            | "minute"
                            | "month"
                            | "year"
                    ))
                || call.args.args.iter().any(|arg| dynamic_scalar(arg))
        }
        PromExpr::Paren(paren) => dynamic_scalar(&paren.expr),
        PromExpr::Unary(unary) => dynamic_scalar(&unary.expr),
        PromExpr::Binary(binary) => dynamic_scalar(&binary.lhs) || dynamic_scalar(&binary.rhs),
        PromExpr::Aggregate(aggregate) => dynamic_scalar(&aggregate.expr),
        _ => false,
    }
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
            // A label rewrite is a point-wise map, so its selectors would be its argument's - but
            // the runtime refuses two series that a rewrite gives one label set
            // (`a_label_rewrite_relabels_series_and_refuses_a_shared_point`), where the existing
            // plan answers them. Admitting the shape would turn an answer into an error, so the
            // statement keeps the existing plan until that refusal has a fallback.
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

/// Recognizes only the metric logical-table scan emitted by `create_table_scan_plan`, and reports
/// the logical table's id together with the scan: the rewrite's `__table_id = <id>` restriction is
/// the logical table the rewrite stands for, so it is the identity a selector over it reads.
fn metric_scan_and_table_id(
    plan: &LogicalPlan,
) -> Option<(datafusion::logical_expr::TableScan, Expr, u32)> {
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
    let Expr::Literal(datafusion::scalar::ScalarValue::UInt32(Some(table_id)), _) =
        binary.right.as_ref()
    else {
        return None;
    };
    let LogicalPlan::TableScan(scan) = filter.input.as_ref() else {
        return None;
    };
    Some((scan.clone(), filter.predicate.clone(), *table_id))
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
    use std::sync::Arc;

    use std::time::{Duration, UNIX_EPOCH};

    use datatypes::series::SeriesOrdering;
    use datatypes::series::samples::{FieldSpec, SeriesSchema};
    use promql::native::plan::{NativeExpr, NativeNode, NativeTree};
    use session::context::QueryContextBuilder;

    use super::{
        NATIVE_PROMQL_EXTENSION_KEY, admitted_selector, admitted_single_selector,
        collect_selectors, fused_output_tags, hybrid_binary_admitted, independent_scan_pair,
        native_promql_requested, scan_time_filter,
    };

    fn compiled_test_tree(query: &str) -> NativeTree {
        let statement = promql_parser::parser::EvalStmt {
            expr: promql_parser::parser::parse(query).unwrap(),
            start: UNIX_EPOCH,
            end: UNIX_EPOCH + Duration::from_secs(10),
            interval: Duration::from_secs(5),
            lookback_delta: Duration::from_secs(5),
        };
        let schema = Arc::new(SeriesSchema {
            fields: Arc::from([FieldSpec { name: "v0".into() }]),
            eval_grid: None,
        });
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
                | NativeExpr::EvalTime { .. }
                | NativeExpr::VectorOf { .. }
                | NativeExpr::ParameterizedMap { .. }
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
        // An aggregate reports the grouping labels its input carries: `by` keeps exactly those,
        // and `without` keeps the rest and drops the metric name.
        // `x` is bound to the three tag columns below, so `without (instance)` keeps all three:
        // `instance` is not one of them, and the metric name is not a column here at all.
        for (query, expected) in [
            ("sum by (host) (x)", vec!["host"]),
            ("sum without (instance) (x)", vec!["host", "kind", "zone"]),
            ("sum(x)", Vec::new()),
        ] {
            let aggregate = compiled_test_tree(query);
            assert_eq!(
                Some(expected.into_iter().map(String::from).collect::<Vec<_>>()),
                fused_output_tags(aggregate.root(), &sources),
                "{query}"
            );
        }
        // Selecting some series keeps each one's own labels, whatever the grouping was.
        let topk = compiled_test_tree("topk(1, x)");
        assert_eq!(
            Some(sources[0].clone()),
            fused_output_tags(topk.root(), &sources),
            "topk keeps the labels of the series it selects"
        );
        // A label rewrite renames the label set below it, which this walk does not derive either.
        for query in [
            r#"label_replace(x, "dst", "$1", "src", "(.*)")"#,
            r#"label_join(x, "dst", "-", "src", "src")"#,
        ] {
            let rewrite = compiled_test_tree(query);
            assert_eq!(None, fused_output_tags(rewrite.root(), &sources), "{query}");
        }
    }

    /// A map that only computes values passes its input's labels through unchanged.
    #[test]
    fn match_output_tags_pass_through_pointwise_math() {
        let sources = [vec!["host".to_string(), "kind".to_string()]];
        let tree = compiled_test_tree("abs(x) * 2");
        assert_eq!(
            Some(vec!["host".to_string(), "kind".to_string()]),
            fused_output_tags(tree.root(), &sources)
        );
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
