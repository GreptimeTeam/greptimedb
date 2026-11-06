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
//! folds across series - an aggregate - is split instead: its per-series prefix and a `Partial`
//! aggregate run in a `FusedSeries` producer, a `SeriesRepartitionExec` routes that state, and a
//! second `FusedSeries` runs the `Final` aggregate over it, because a region holds only part of a
//! group. Declining is the normal answer: every shape the
//! compiler does not accept, and every query this route cannot bind to a scan faithfully, keeps the
//! plan the existing planner builds.
//!
//! What the node needs from here is the raw series schema the tree's input was compiled against
//! (the selector's selected field columns), the statement's evaluation grid, and output tag names
//! derived from the native tree. The output schema is built to the shape the existing unary pipeline
//! produces — the time index, then the series fields, then the tags — so a node planned here can be
//! consumed by the same code that consumes a `SeriesNormalize` output.
//!
//! A statement that reads no data - a constant, `time()`, `vector(time())` - is planned with no
//! scan: one node, no child, the time index and value.

use std::borrow::Cow;
use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

use common_catalog::consts::{METRIC_ENGINE, MITO_ENGINE, MITO2_ENGINE};
use common_query::native_histogram::native_histogram_value_type;
use common_query::prelude::OTLP_AGGREGATION_TEMPORALITY_LABEL;
use common_query::series_frame::raw_series_frame_schema_with_ordering;
use datafusion::arrow::datatypes::{DataType as ArrowDataType, Field, TimeUnit as ArrowTimeUnit};
use datafusion::common::{DFSchema, DFSchemaRef};
use datafusion::error::DataFusionError;
use datafusion::execution::memory_pool::UnboundedMemoryPool;
use datafusion::logical_expr::UserDefinedLogicalNode;
use datafusion::logical_expr::{
    EmptyRelation, Extension, LogicalPlan, LogicalPlanBuilder, TableScanBuilder, Union,
};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::Expr as DfExpr;
use datafusion_expr::Expr;
use datatypes::data_type::DataType as _;
use datatypes::series::SeriesOrdering;
use datatypes::series::samples::{FieldSpec, SeriesSchema};
use datatypes::series::time::EvalGrid;
use promql::extension_plan::{
    FieldKind, FusedOutputFormat, FusedSeries, FusedSeriesBinding, FusedSeriesExec, InputLocality,
    RawSeriesSource, input_locality, matching_operands_drop_key, tree_source_fields,
};
use promql::native::execute::NativeExecutionState;
use promql::native::plan::{
    BoundAggregate, BoundInput, BoundVectorOp, Grouping, InstantValue, MapOp, NativeExpr,
    NativeNode, NativeTree, NativeType, fused_output_tags, reads_across_series, uniform_field_kind,
};
use promql::vector_match::{Cardinality, MatchingLabels, SetOp};
use promql_parser::label::{METRIC_NAME, MatchOp, Matchers};
use promql_parser::parser::value::ValueType;
use promql_parser::parser::{
    BinModifier, EvalStmt, Expr as PromExpr, LabelModifier, MatrixSelector, ParenExpr,
    VectorMatchCardinality, VectorSelector, token,
};
use session::context::{MetricDiscoveryScope, QueryContextRef};
use snafu::ResultExt;
use store_api::metric_engine_consts::{DATA_SCHEMA_TABLE_ID_COLUMN_NAME, METRIC_ENGINE_NAME};
use store_api::region_engine::RawLabelSource;

use crate::promql::error::{DataFusionPlanningSnafu, Result};
use crate::promql::native_compile::{
    InfoInput, compile_selectors, compile_statement, info_input, statement_grid,
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

/// The analyzer rule an engine that plans distributed statements installs on its session; see
/// [`DistPlannerAnalyzer`](crate::dist_plan::analyzer::DistPlannerAnalyzer).
const DIST_PLANNER_RULE: &str = "DistPlannerAnalyzer";

/// Whether a plan built here is split into a frontend plan over regions reached over the wire.
///
/// This is the one condition under which every scan this route prepares has to be a
/// [`RawSeriesSource`]. The remote transport carries a region's half as Substrait bytes, which can
/// only encode that source, so over the wire the single fused node over raw sources is the only
/// shape admitted; in process a region accepts a `LogicalPlan` and the split and segmented shapes
/// keep running region-locally, so those stay and this returns `false`.
///
/// An engine that plans no distributed statement plans no region at all, and its plans are the
/// standalone ones: it installs the distributed planner as an analyzer rule exactly when it was
/// built to split statements, so the rule says whether this engine has a region to satisfy.
fn plans_over_remote_regions(query_engine_state: &QueryEngineState) -> bool {
    !query_engine_state.in_process_region_transport()
        && query_engine_state
            .session_state()
            .analyzer()
            .rules
            .iter()
            .any(|rule| rule.name() == DIST_PLANNER_RULE)
}

/// Whether this executable plan is the native raw PromQL topology, the one plan shape whose
/// region reads one query-wide body budget cannot deadlock.
///
/// That topology is a chain of [`FusedSeriesExec`] nodes over region reads: the split aggregate's
/// final and partial halves are one unary chain, and each fused node polls its input while that
/// input is being drained, so a region body holding a permit keeps making progress.
/// Admission stops there: a plan with any other multi-input node (a join, or the union a
/// discovered selector builds) keeps its per-scan bounds only and is not counted against the
/// query-wide target until its own topology is checked against the budget.
pub(crate) fn native_raw_plan_bounds_region_reads(plan: &Arc<dyn ExecutionPlan>) -> bool {
    fn walk(node: &dyn ExecutionPlan, fused: &mut usize) -> bool {
        let is_fused = node.is::<FusedSeriesExec>();
        if is_fused {
            *fused += 1;
        }
        if node.children().len() > 1 && !is_fused {
            return false;
        }
        node.children()
            .iter()
            .all(|child| walk(child.as_ref(), fused))
    }

    let mut fused = 0;
    walk(plan.as_ref(), &mut fused) && fused > 0
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

/// What one discovered metric contributes: the shared schema, its time column, the tags the union
/// reports, and the one metric name the single-source shape carries.
type DiscoveredSelectorMetadata = (Arc<SeriesSchema>, String, Vec<String>, Arc<str>);

/// One prepared occurrence of a selector: its scan, raw-series layout and reporting columns.
///
/// `partitioned` says whether the table behind that scan is split into regions along a partition
/// column. Nothing in the scan itself carries this: a partition is a placement fact about the
/// table, and the canonical route has to know it before it can claim a label-set answer covers the
/// whole query rather than one region's share of it.
type PreparedInput = (
    LogicalPlan,
    Arc<SeriesSchema>,
    Option<Arc<str>>,
    String,
    Vec<String>,
    // `partitioned`: whether this input's table is split into regions along a partition column.
    bool,
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
        && !contains_aggregate_expression(lhs)
        && !contains_aggregate_expression(rhs)
        && single_selector_operand(lhs)
        && single_selector_operand(rhs)
}

impl PromPlanner {
    pub(super) async fn try_plan_native_operand(
        &mut self,
        expr: &PromExpr,
        query_engine_state: &QueryEngineState,
    ) -> Result<Option<(LogicalPlan, super::PromPlannerContext)>> {
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
        // The scan context is only the input layout. Calls such as histogram_quantile can
        // remove labels while producing the fused node, so publish only columns actually in
        // that node's output schema to the binary matcher.
        let mut output_ctx = operand_ctx;
        let fields = plan.schema().fields();
        output_ctx.tag_columns = fields
            .iter()
            .filter(|field| field.data_type() == &ArrowDataType::Utf8)
            .map(|field| field.name().clone())
            .collect();
        output_ctx
            .field_columns
            .retain(|field| fields.iter().any(|column| column.name() == field));
        output_ctx.use_tsid = false;
        self.ctx = output_ctx.clone();
        let table_ref = self.table_ref();
        self.ctx = saved;
        let table_ref = table_ref?;
        let plan = LogicalPlanBuilder::from(plan)
            .alias(table_ref)
            .context(DataFusionPlanningSnafu)?
            .build()
            .context(DataFusionPlanningSnafu)?;
        Ok(Some((plan, output_ctx)))
    }

    /// Builds the scan of the one logical table the current context is bound to. The caller has
    /// already preprocessed this selector's matchers and set the context to its table.
    #[allow(clippy::too_many_arguments)]
    async fn prepare_one_scan(
        &mut self,
        matchers: Matchers,
        selector_count: usize,
        prom_expr: &PromExpr,
        logical_scan_id: &mut Option<u32>,
        logical_ids_differ: &mut bool,
        statement_kind: &mut Option<FieldKind>,
        self_contained: bool,
    ) -> Result<Option<PreparedInput>> {
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
        // This is a deliberate semantic safety gate, not a native-function allowlist. On a
        // delta-temporality table, samples are increments; the existing planner sums them
        // for `increase` and divides by the range for `rate` per series, while the native
        // compiled tree assumes cumulative samples. Keep any statement containing either
        // operation on the existing path for these tables. The check covers the whole
        // statement, not just this selector occurrence.
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
            match *logical_scan_id {
                None => *logical_scan_id = Some(*table_id),
                Some(first) if first != *table_id => *logical_ids_differ = true,
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
        // A table split along a partition column lives in more than one region. The canonical
        // boundary's router only collects the pieces its own region's producer holds, so the caller
        // needs this fact to tell a whole-query label-set answer from one region's share of it.
        let partitioned = table_meta.partition_column_names().next().is_some();
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
        // Preserve the existing single-selector guard that its partition keys are tags. A partition
        // column that is not a tag is the time index, so one series spans several regions; a bare
        // scan cannot reconcile the fragments, which is why this route kept the existing plan for
        // it. The raw source can, so the guard only holds the shapes that read a bare scan - the
        // ones a region can run in process.
        if selector_count == 1
            && !self_contained
            && !table_meta
                .partition_column_names()
                .all(|column| tags.iter().any(|tag| tag == column))
        {
            return Ok(None);
        }
        // This route declares one output column per field, so every field it binds carries
        // one kind: `Float64`, or the canonical native histogram struct. A field of any
        // other type - an integer, a `Float32`, a decimal, a string - keeps the existing
        // plan, and so does a statement whose fields disagree: one column cannot carry two
        // kinds.
        //
        // `Float64` fields are answered by the columnar series-index reader when a region
        // prepares one. A histogram field is not: the logical plan cannot tell which reader
        // a region will prepare, so such a selection falls back to the ordinary scan, whose
        // converter already turns the persisted struct into histogram samples.
        let Some(field_kind) = uniform_field_kind(
            fields
                .iter()
                .map(|field| Self::field_column_type(input.schema(), field)),
        ) else {
            return Ok(None);
        };
        if statement_kind.is_some_and(|kind| kind != field_kind) {
            return Ok(None);
        }
        *statement_kind = Some(field_kind);
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
        Ok(Some((
            input,
            series_schema,
            Some(Arc::from(metric)),
            time,
            tags,
            partitioned,
        )))
    }

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

    /// Plans `prom_expr` as one native node, or returns `None` to keep the existing plan.
    ///
    /// The compiler's bound tree and selector schemas determine the native output names; the
    /// existing planner is not consulted for capability or schema approval.
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
    async fn try_plan_native_fused_with_context(
        &mut self,
        prom_expr: &PromExpr,
        query_engine_state: &QueryEngineState,
    ) -> Result<Option<(LogicalPlan, Option<super::PromPlannerContext>)>> {
        if !self.native_fused {
            return Ok(None);
        }
        // Over the wire this route plans the whole statement as one fused node over raw sources:
        // each region answers the series it holds, and the node reconciles them before evaluating.
        // In process a region can also run a split or a segmented tree over its own scan, so those
        // shapes stay there. This is a planning choice, not a limit of what the node can encode.
        let self_contained = plans_over_remote_regions(query_engine_state);
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
        // The compiler decides which calls and shapes this route answers: a name it has no kernel
        // for comes back as `NotImplemented`, which is a decline below. A second list of names here
        // refused expressions the compiler does fold - a call under an aggregate was admitted while
        // the same call on its own was not - so there is one place that decides.
        let Ok(grid) = statement_grid(&stmt) else {
            return Ok(None);
        };
        if selectors.is_empty() {
            let tree = match compile_statement(&stmt) {
                Ok(tree) => tree,
                Err(DataFusionError::NotImplemented(_)) => return Ok(None),
                Err(error) => return Err(error).context(DataFusionPlanningSnafu),
            };
            // Bind only input tags: this data-free tree has no input, and its tags belong to its
            // output schema.
            let Some(output_tags) = native_output_tags(tree.root(), &[]) else {
                return Ok(None);
            };
            let Some(values) = native_root_fields(tree.root()) else {
                return Ok(None);
            };
            let time_index = if values
                .iter()
                .chain(&output_tags)
                .any(|name| name == DEFAULT_TIME_INDEX_COLUMN)
            {
                (0usize..)
                    .map(|index| format!("native_time_{index}"))
                    .find(|candidate| {
                        !values
                            .iter()
                            .chain(&output_tags)
                            .any(|name| name == candidate)
                    })
                    .expect("the time column candidate sequence is unbounded")
            } else {
                DEFAULT_TIME_INDEX_COLUMN.to_string()
            };
            // A data-free tree reads no input, so every field it answers is a float.
            let output_schema =
                fused_output_schema(&values, &output_tags, &time_index, FieldKind::Float)?;
            let node = FusedSeries::try_new(
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
            // The kind every bound field carries. A statement declares one column per field, so
            // selectors that disagree have no column to declare and keep the existing plan.
            let mut statement_kind: Option<FieldKind> = None;
            for selector in &selectors {
                self.ctx = original_context.clone();
                // These matchers are alternatives the one scan below the node cannot represent, so the
                // query keeps the plan the existing path builds for it.
                if !selector.matchers.or_matchers.is_empty() {
                    return Ok(None);
                }
                // A selector that names no metric is answered by enumerating the schema's logical
                // metric tables: one input per metric, each reading its own region. The tag walk
                // then sees every input's own labels, so each series keeps the identity the
                // expression itself cannot name.
                if discovers_metric_name(selector) {
                    let name_matchers = selector.matchers.find_matchers(METRIC_NAME);
                    let mut ordinary_matchers = selector.matchers.clone();
                    ordinary_matchers
                        .matchers
                        .retain(|matcher| matcher.name != METRIC_NAME);
                    // Name matchers constrain discovered table names rather than row columns.
                    // Supplying a temporary name lets preprocessing reset the context and apply
                    // schema/database and ordinary matchers without its expected NoMetricMatcher.
                    let matchers = match self
                        .preprocess_label_matchers(&ordinary_matchers, &Some(String::new()))
                    {
                        Ok(m) => m,
                        Err(_) => return Ok(None),
                    };
                    let effective_schema = self
                        .ctx
                        .schema_name
                        .clone()
                        .unwrap_or_else(|| self.table_provider.query_ctx().current_schema());
                    // Authorization is decided before anything is read: a table the caller may not
                    // see never becomes a source, so its name reaches neither the plan nor an error.
                    let scope = self.table_provider.query_ctx().metric_discovery_scope();
                    if matches!(scope, MetricDiscoveryScope::Unavailable) {
                        return Ok(None);
                    }
                    let mut metrics = self
                        .table_provider
                        .logical_metric_tables(&effective_schema)
                        .await
                        .map_err(|error| DataFusionError::External(Box::new(error)))
                        .context(DataFusionPlanningSnafu)?;
                    if let MetricDiscoveryScope::Allowed(allowed) = scope {
                        let catalog = self.table_provider.query_ctx().current_catalog();
                        // Borrowed keys, so a wide allowlist is one pass to build and one lookup
                        // per metric rather than a scan of the allowlist for each of them.
                        let permitted = allowed
                            .iter()
                            .map(|target| {
                                (
                                    target.catalog.as_str(),
                                    target.schema.as_str(),
                                    target.table.as_str(),
                                )
                            })
                            .collect::<std::collections::HashSet<_>>();
                        metrics.retain(|table| {
                            let info = table.table_info();
                            // Reject metadata that disagrees with where the table was enumerated.
                            info.catalog_name == catalog
                                && info.schema_name == effective_schema
                                && permitted.contains(&(
                                    info.catalog_name.as_str(),
                                    info.schema_name.as_str(),
                                    info.name.as_str(),
                                ))
                        });
                    }
                    metrics.retain(|table| {
                        name_matchers
                            .iter()
                            .all(|matcher| matcher.is_match(&table.table_info().name))
                    });
                    // A label a table does not carry reads as the empty string on every one of its
                    // rows, so a matcher that rejects the empty string cannot match anything there.
                    metrics.retain(|table| {
                        let schema = table.schema();
                        matchers.matchers.iter().all(|matcher| {
                            matcher.is_match("")
                                || schema.column_schema_by_name(&matcher.name).is_some()
                        })
                    });
                    metrics.sort_by_key(|table| table.table_info().name.clone());
                    if metrics.len() > MAX_DISCOVERED_METRICS {
                        return Err(DataFusionError::Plan(format!(
                            "the selector matches {} metric tables, over the {MAX_DISCOVERED_METRICS} one query may scan; narrow it",
                            metrics.len()
                        )))
                        .context(DataFusionPlanningSnafu);
                    }
                    if metrics.is_empty() {
                        // Nothing matches the selector, so the query still has an answer: the
                        // empty vector. Planning it here rather than declining is what keeps a
                        // nameless selector that matches nothing from reaching the existing
                        // planner, which has no table to bind and raises `TableNameNotFound`: no
                        // table matching is a result, not a failure to plan.
                        //
                        // The answer is an empty raw series frame, which is the one input the node
                        // and every operator above it already accept. A statement with another
                        // data input beside this selector keeps its existing plan: two inputs
                        // reach one node under one partition-count contract, and a real scan's
                        // partitions and an empty frame's single one cannot share it here.
                        if selectors.len() != 1 {
                            return Ok(None);
                        }
                        // A `__field__` matcher narrows the field set in a projection the existing
                        // planner builds, and this route declines it for a scan it can read; the
                        // same refusal keeps the empty input from claiming a field layout the
                        // matcher would have decided.
                        if self.ctx.field_column_matcher.is_some() {
                            return Ok(None);
                        }
                        prepared.push(empty_discovered_input(
                            self.ctx
                                .time_index_column
                                .clone()
                                .unwrap_or_else(|| DEFAULT_TIME_INDEX_COLUMN.to_string()),
                        )?);
                        continue;
                    }
                    // One compiler source per selector: the discovered scans become one union
                    // input, so the compiled tree still reads what the planner prepared.
                    let mut sources = Vec::with_capacity(metrics.len());
                    let mut selector_metadata: Option<DiscoveredSelectorMetadata> = None;
                    // The union reads every discovered metric's region, so one partitioned metric
                    // makes the whole input partitioned.
                    let mut discovered_partitioned = false;
                    for table in metrics {
                        self.ctx.table_name = Some(table.table_info().name.clone());
                        if self.setup_context().await?.is_some() {
                            return Ok(None);
                        }
                        let Some((scan, schema, metric, time, mut tags, partitioned)) = self
                            .prepare_one_scan(
                                matchers.clone(),
                                1,
                                prom_expr,
                                &mut logical_scan_id,
                                &mut logical_ids_differ,
                                &mut statement_kind,
                                self_contained,
                            )
                            .await?
                        else {
                            return Ok(None);
                        };
                        discovered_partitioned |= partitioned;
                        let Some(metric) = metric else {
                            return Ok(None);
                        };
                        // The expression names no metric, so each source reports its own: the tag is
                        // what tells two metrics with equal ordinary labels apart.
                        if !tags.iter().any(|tag| tag == METRIC_NAME) {
                            tags.push(METRIC_NAME.to_string());
                        }
                        let fields = schema
                            .fields
                            .iter()
                            .map(|field| field.name.to_string())
                            .collect();
                        sources.push(LogicalPlan::Extension(Extension {
                            node: Arc::new(
                                RawSeriesSource::try_new(
                                    metric.clone(),
                                    tags.clone(),
                                    fields,
                                    scan,
                                )
                                .context(DataFusionPlanningSnafu)?,
                            ),
                        }));
                        if let Some((first_schema, _, union_tags, _)) = &mut selector_metadata {
                            let first_fields = first_schema
                                .fields
                                .iter()
                                .map(|field| field.name.as_ref())
                                .collect::<Vec<_>>();
                            let metric_fields = schema
                                .fields
                                .iter()
                                .map(|field| field.name.as_ref())
                                .collect::<Vec<_>>();
                            if first_fields != metric_fields {
                                return Err(DataFusionError::Plan(format!(
                                    "discovered metric {} has an unsupported field layout; expected {:?}, found {:?}",
                                    table.table_info().name,
                                    first_fields,
                                    metric_fields,
                                )))
                                .context(DataFusionPlanningSnafu);
                            }
                            union_tags.extend(tags);
                            union_tags.sort();
                            union_tags.dedup();
                        } else {
                            selector_metadata = Some((schema, time, tags, metric));
                        }
                    }
                    let Some((schema, time, mut tags, single_metric)) = selector_metadata else {
                        return Ok(None);
                    };
                    tags.sort();
                    tags.dedup();
                    let (union, metric) = if sources.len() == 1 {
                        (sources.pop().unwrap(), Some(single_metric))
                    } else {
                        (
                            LogicalPlan::Union(
                                Union::try_new_with_loose_types(
                                    sources.into_iter().map(Arc::new).collect(),
                                )
                                .context(DataFusionPlanningSnafu)?,
                            ),
                            None,
                        )
                    };
                    prepared.push((union, schema, metric, time, tags, discovered_partitioned));
                    continue;
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
                let Some(one) = self
                    .prepare_one_scan(
                        matchers,
                        selectors.len(),
                        prom_expr,
                        &mut logical_scan_id,
                        &mut logical_ids_differ,
                        &mut statement_kind,
                        self_contained,
                    )
                    .await?
                else {
                    return Ok(None);
                };
                prepared.push(one);
            }
            Ok(Some((
                prepared,
                single_input_context,
                logical_ids_differ,
                statement_kind,
            )))
        }
        .await;
        self.ctx = original_context;
        let Some((mut prepared, single_input_context, logical_ids_differ, statement_kind)) =
            preparation?
        else {
            return Ok(None);
        };
        // Selectors over one table share one region, so the node runs there over every scan. When
        // they read more than one table, each scan answers from its own region and the node runs
        // above them; see `RawSeriesSource`. A metric logical table's id is part of the identity it
        // reads: two ids are two metrics even when one physical table holds both, so two selectors
        // that differ only by that id still read different series and are bound as two inputs.
        //
        // The typed empty input of a selector that matched no table reads no region, so it takes no
        // part in this: it is not a second table beside the others, and counting it as one would
        // ask the others' binding to name a metric it does not have.
        let cross_table = !same_table_scans(
            prepared
                .iter()
                .map(|(scan, ..)| scan)
                .filter(|scan| !is_empty_discovered_input(scan)),
        ) || logical_ids_differ;
        // A multi-selector tree over a partitioned table has no in-process placement whose matching
        // and uniqueness semantics equal the frontend route. One node over several scans of one
        // partitioned table reads neither the other scans nor the fragments another region holds,
        // and one node over several tables' sources matches every series those sources delivered,
        // where the frontend route copies the operands' matchers into their scans first and checks
        // uniqueness above the join. Keep the existing plan in process; the hybrid route lifts such
        // a tree one operand at a time. Over the wire every occurrence is already its own raw source
        // reconciled above the regions, so the route keeps those trees.
        if selectors.len() > 1 && !self_contained && prepared.iter().any(|input| input.5) {
            return Ok(None);
        }
        // The first input names the output's time index and, on one table, binds every scan.
        let Some((_, _, metric, time, tags, _)) = prepared.first() else {
            return Ok(None);
        };
        let (metric, time, tags) = (metric.clone(), time.clone(), tags.clone());
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
        for (index, (compiled_input, (scan, _, _, time, _, _))) in
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
            if let LogicalPlan::Extension(extension) = scan
                && extension.node.as_any().is::<RawSeriesSource>()
            {
                let mut inputs = extension
                    .node
                    .inputs()
                    .into_iter()
                    .cloned()
                    .collect::<Vec<_>>();
                let Some(LogicalPlan::TableScan(table_scan)) = inputs.first_mut() else {
                    return Err(DataFusionError::Internal(
                        "RawSeriesSource input is not a TableScan".into(),
                    ))
                    .context(DataFusionPlanningSnafu);
                };
                table_scan.filters.push(filter);
                let replacement = extension
                    .node
                    .with_exprs_and_inputs(vec![], inputs)
                    .context(DataFusionPlanningSnafu)?;
                *scan = LogicalPlan::Extension(Extension { node: replacement });
            } else if let LogicalPlan::Union(union) = scan {
                // A discovered selector's sources each read their own region, and the metric
                // engine checks that two logical metrics agree on their time column's unit, not on
                // its name. So each bound is built from the column that source actually carries.
                for child in &mut union.inputs {
                    let LogicalPlan::Extension(extension) = child.as_ref() else {
                        return Err(DataFusionError::Internal(format!(
                            "Union child at {index} is not a RawSeriesSource for time-bound pushdown"
                        )))
                        .context(DataFusionPlanningSnafu);
                    };
                    let source = extension
                        .node
                        .as_any()
                        .downcast_ref::<RawSeriesSource>()
                        .ok_or_else(|| {
                            DataFusionError::Internal(format!(
                                "Union child at {index} is not a RawSeriesSource for time-bound pushdown"
                            ))
                        })
                        .context(DataFusionPlanningSnafu)?;
                    let mut inputs = source.inputs().into_iter().cloned().collect::<Vec<_>>();
                    let Some(LogicalPlan::TableScan(table_scan)) = inputs.first_mut() else {
                        return Err(DataFusionError::Internal(format!(
                            "RawSeriesSource child at {index} is not backed by a TableScan"
                        )))
                        .context(DataFusionPlanningSnafu);
                    };
                    let source_time = table_scan
                        .projected_schema
                        .fields()
                        .iter()
                        .find(|field| {
                            matches!(
                                field.data_type(),
                                datafusion::arrow::datatypes::DataType::Timestamp(_, _)
                            )
                        })
                        .map(|field| field.name().clone());
                    let Some(source_time) = source_time else {
                        return Err(DataFusionError::Internal(format!(
                            "RawSeriesSource child at {index} has no timestamp column"
                        )))
                        .context(DataFusionPlanningSnafu);
                    };
                    let source_filter = scan_time_filter(start, end, inclusive, &source_time);
                    table_scan.filters.push(source_filter);
                    let replacement = extension
                        .node
                        .with_exprs_and_inputs(vec![], inputs)
                        .context(DataFusionPlanningSnafu)?;
                    *child = Arc::new(LogicalPlan::Extension(Extension { node: replacement }));
                }
            } else if let LogicalPlan::TableScan(table_scan) = scan {
                table_scan.filters.push(filter);
            } else if let LogicalPlan::EmptyRelation(_) = scan {
                // The typed empty input has no scan to constrain.
            } else {
                return Err(DataFusionError::Internal(format!(
                    "the child at {index} is not the table scan this route prepared"
                )))
                .context(DataFusionPlanningSnafu);
            }
        }
        let source_tags = prepared
            .iter()
            .map(|(_, _, _, _, tags, _)| tags.clone())
            .collect::<Vec<_>>();
        // Whether any input's table is split into regions along a partition column. The canonical
        // boundary is the only part of this route that claims a whole-query label-set answer, and a
        // region-local producer cannot see the pieces another region holds, so this fact decides
        // whether that boundary may be claimed at all.
        let any_partitioned = prepared.iter().any(|input| input.5);
        // A segment carries one source's identity. Keeping each occurrence's scan and metadata
        // aligned here lets the cutting step name that source independently of selector count.
        let bare_scans = prepared
            .iter()
            .map(|(scan, ..)| scan.clone())
            .collect::<Vec<_>>();
        let per_source = prepared
            .iter()
            .map(|(_, _, metric, time_index, tags, _)| {
                (metric.clone(), time_index.clone(), tags.clone())
            })
            .collect::<Vec<_>>();
        let children = prepared
            .into_iter()
            .map(|(scan, schema, metric, _, tags, _)| {
                // A discovered selector already carries its own sources: a single metric is one
                // `RawSeriesSource`, several are a `Union` of them, and each names the metric it
                // reads. Wrapping either again would nest a source in a source.
                let already_sourced = matches!(&scan, LogicalPlan::Union(_))
                    || matches!(
                        &scan,
                        LogicalPlan::Extension(extension)
                            if extension.node.as_any().is::<RawSeriesSource>()
                    );
                // A scan stays bare only where a region runs the node over it in process. Over the
                // wire every input is a raw source, whichever table it reads: the source is what the
                // transport carries, and it is also what reconciles the fragments of a series that
                // several regions hold.
                if (!cross_table && !self_contained) || already_sourced {
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
        // Admission and placement are separate: `independent_scan_pair` admits nested native
        // matches, while the physical locality classification decides whether their inputs need
        // gathering or segmentation.
        // A label rewrite can give two series one label set, which every consumer that answers a
        // whole label set has to see as one series. The pass puts a [`NativeExpr::Merge`] where that
        // matters and nowhere else, so a tree with no collapsing rewrite keeps every partition.
        let tree = match promql::native::canonicalize::merge_label_rewrites(
            &compiled.tree,
            &source_tags,
        ) {
            Ok(tree) => tree,
            Err(_) => return Ok(None),
        };
        let root = tree.root();
        // A canonical boundary routes each label set to one owner above its own producer, and a
        // region-local producer only sees the partitions of the region it runs in. A table split
        // along a partition column is planned region by region, so each region would answer a
        // canonical label set from the piece it holds while the consumer above has no collection of
        // the other regions' pieces. Without a proof that this query reads one region, decline the
        // route instead of claiming a global canonical answer; the existing planner reads every
        // region and reports what Prometheus reports. A tree with no canonical merge claims no such
        // answer, so it is untouched: a per-series query on a partitioned table keeps fusing.
        if any_partitioned && contains_canonical_merge(root) {
            return Ok(None);
        }
        // The kind the tree's root answers with, which every output column follows. It is not the
        // kind the inputs carry: `count_over_time`, `histogram_count` and `timestamp` reduce a
        // histogram to a float, while a selector, a histogram-answering range function and a label
        // rewrite pass it on.
        //
        // Only a histogram statement can answer a histogram column, so only then does the walk
        // have to name one; a float statement keeps the float schema it always had, and a shape
        // whose kind cannot be named keeps the existing plan rather than declaring a column the
        // first batch might not fill.
        let root_kind = match statement_kind {
            Some(FieldKind::Histogram) => {
                // A histogram answer standing on a canonical merge would run through the canonical
                // boundary carrying histogram frames, a pairing nothing exercises yet. The
                // follow-up that teaches the aggregate state to hold histograms owns it, and until
                // then the statement keeps the existing plan - its answer, not its semantics, is
                // out of this route's reach. Every merge counts, not only a root one: a merge below
                // a float-producing map is still a boundary that would carry histograms.
                if merge_answers_histogram(root, FieldKind::Histogram) {
                    return Ok(None);
                }
                match root.output_field_kind(FieldKind::Histogram) {
                    Some(root_kind) => root_kind,
                    None => return Ok(None),
                }
            }
            Some(FieldKind::Float) | None => FieldKind::Float,
        };
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
        // The aggregate split folds float state, and the multi-input node reads several scans, so
        // a histogram keeps the existing plan for both rather than running a tree whose output the
        // runtime could not finish. The kind entering each aggregate is what decides, not the
        // shape of the root: `sum(h) * 2` has a binary root and would otherwise hide the aggregate
        // under it.
        if let Some(kind @ FieldKind::Histogram) = statement_kind
            && (multi_input || aggregate_reads_histogram(root, kind))
        {
            return Ok(None);
        }
        // A multi-input tree skips the split below, so the refusals the split makes for reasons that
        // are not about what the runtime computes have to be made here.
        //
        // The aggregate split keeps its prefix per-series, which is why it declines an operation
        // whose answer depends on the order its input arrives in. A multi-input node reads its
        // inputs' partitions in the order they became ready, so that refusal is repeated here.
        if (multi_input || (self_contained && children.len() > 1)) && depends_on_input_order(root) {
            return Ok(None);
        }
        // Two sources that report groups by metric name need a column for that name: without one,
        // groups that differ only by metric would be reported as the same label set. A discovered
        // selector carries that column itself; several explicit selectors do not.
        if (multi_input || (self_contained && children.len() > 1)) && groups_by_metric_name(root) {
            return Ok(None);
        }
        // The native output schema can name only compiled value fields, the time index and the tag
        // columns a source declares. A discovered source declares `__name__` because the name is
        // what tells two metrics with equal labels apart, so a nameless selector's names survive a
        // grouping by `__name__` (#10106). A named selector's source does not declare it - the table
        // name is the metric name - so a tree reading more than one such source cannot report the
        // distinct groups and declines instead of claiming they are interchangeable.
        if multi_input && groups_by_metric_name(root) {
            return Ok(None);
        }
        // Catch construction-time refusals before choosing native execution: errors raised during
        // physical planning cannot fall back. This includes unsupported aggregates and `topk`,
        // `limitk` or CountValues groupings; it does not cover data-dependent errors raised while
        // executing the tree. A tree planned over the wire commits to the fused node without the
        // split or a segment above it, so it validates itself the same way before that choice.
        if (multi_input || self_contained)
            && NativeExecutionState::try_new(&tree, Arc::new(UnboundedMemoryPool::default()), None)
                .is_err()
        {
            return Ok(None);
        }
        // A tree that ends in a canonical merge answers one series per label set, and the pieces of
        // one set were spread over the scan's partitions by the rewrite that created them. Lowering
        // that boundary keeps the scan width: each partition merges the pieces it holds and exports
        // series frames, and a canonical router collects the pieces of one label set from every
        // partition. Declining here would hand the statement back to a path that answers it per
        // partition.
        if !multi_input
            && selectors.len() == 1
            && matches!(&tree.root().expr, NativeExpr::Merge { input } if pointwise_operand(input) && canonical_local_prefix(input) && !contains_global_consumer(input))
            && let Some(plan) = canonical_boundary_plan(
                &tree,
                &children,
                &source_tags,
                &grid,
                &time,
                &tags,
                &metric,
                root_kind,
            )
        {
            return Ok(Some((plan, single_input_context)));
        }
        let root = tree.root();
        let per_partition =
            !reads_across_series(root) || pointwise_operand(root) || independent_scan_pair(root);
        let locality = input_locality(root);
        let computed_key_case =
            matches!(locality, InputLocality::SinglePartition) && matching_operands_drop_key(root);
        let required_placement = !per_partition || computed_key_case;
        if required_placement {
            // A tree whose fold can be split keeps that plan: it exports aggregate state rather
            // than whole samples, so it moves less between the two halves than a sample boundary
            // would. The general segmentation below is the fallback for the trees it declines,
            // which are the ones its per-series prefix cannot express. The split is decided before
            // anything consumes a child, so a tree it declines keeps the plan it would have built
            // without this route. Its producer reads whatever input the statement prepared: over
            // the wire that is the raw series source, and the partial/final halves stay on the
            // frontend above the regions, so per-series work keeps the scan width.
            let split = if !multi_input && selectors.len() == 1 {
                FusedSeries::split_aggregate(&tree, &time).context(DataFusionPlanningSnafu)?
            } else {
                None
            };
            if let Some((partial, final_tree)) = split {
                // The split's output is the suffix's, and the state it exports is float: the gate
                // above already declines a histogram entering an aggregate, and this declines a
                // histogram answer whose aggregate sits elsewhere, because the merge folds float
                // state. The partial stage may introduce labels before aggregation, so the columns
                // come from the complete tree rather than from the suffix alone.
                if root_kind != FieldKind::Float {
                    return Ok(None);
                }
                let Some(output_schema) =
                    Self::fused_root_schema(&tree, &source_tags, &time, root_kind)
                else {
                    return Ok(None);
                };
                let Some(rawinput) = children.into_iter().next() else {
                    return Ok(None);
                };
                let node = plan_split_aggregate(
                    partial,
                    final_tree,
                    FusedSeriesBinding {
                        grid,
                        time_index: time.clone(),
                        tags,
                        metric_name: metric,
                    },
                    rawinput,
                    output_schema,
                )?;
                let plan = LogicalPlan::Extension(Extension {
                    node: Arc::new(node),
                });
                return Ok(Some((plan, single_input_context)));
            }
            // Preserve the established refusal for repeated same-table operands unless
            // segmentation is needed to preserve a computed matching key.
            if selectors.len() != 1 && !cross_table && !computed_key_case {
                return Ok(None);
            }
        }
        // A segment over the wire keeps the raw source as its input rather than a bare scan: the
        // segment's prefixes run on the frontend above the regions, so the raw series source is
        // what each of them reads.
        if required_placement || matches!(locality, InputLocality::SinglePartition) {
            let segment_metric = if cross_table { None } else { metric.clone() };
            let plan = Box::pin(segment_tree(
                &tree,
                &children,
                &source_tags,
                &bare_scans,
                &per_source,
                &grid,
                &time,
                &tags,
                &segment_metric,
                FusedOutputFormat::PlainRows,
                contains_canonical_merge(root),
                root_kind,
                self_contained,
            ))
            .await?;
            if let Some(plan) = plan {
                return Ok(Some((plan, single_input_context)));
            }
            // A raw scan alone does not prove a region contains all contributions required by a
            // cross-series consumer. Without a split or segmentation, keep that consumer out of
            // the region rather than treating its local gather as a global answer.
            if required_placement && !multi_input {
                return Ok(None);
            }
        }
        // Multi-input fused execution already reads every input in this execution.
        // The tags are asked for before the fields, because the walk answers for a tree the
        // compiler produced: a scalar root has no labels at all, and a sample root's are the ones
        // the walk derives. If it cannot derive them, this route declines.
        let output_tags = match &tree.root().output {
            // A scalar has no labels, and its value is named as a bare literal's is.
            NativeType::Scalar(_) => Vec::new(),
            NativeType::Samples(_) => match native_output_tags(tree.root(), &source_tags) {
                Some(output_tags) => output_tags,
                None => return Ok(None),
            },
            NativeType::AggregateState(_) => return Ok(None),
        };
        let Some(root_fields) = native_root_fields(tree.root()) else {
            return Ok(None);
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
        let output_schema = fused_output_schema(&root_fields, &output_tags, &time, root_kind)?;
        // Across tables the region scans are bound by their `RawSeriesSource` children, and this is
        // also the binding a node over more than one input needs: such a node reads frames that
        // already carry their own labels, so it binds neither tags nor one metric name of its own -
        // each child names the metric it reads. A multi-input plan whose children stay direct
        // scans reads one table through the one metric and tag set they share, and keeps this
        // binding, which is the same identity those scans carry. The same holds over the wire,
        // where every input is a raw source.
        let (tags, metric) = if cross_table || self_contained {
            (Vec::new(), None)
        } else {
            (tags, metric)
        };
        let node = FusedSeries::try_new(
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

    /// The columns a [`FusedSeries`] node over `tree` produces.
    ///
    /// Field and tag names are derived from the native tree. If this walk cannot derive a tag
    /// set, the native route declines rather than consulting the legacy planner.
    ///
    /// `None` when the root is not a value the node can publish, or when a field would share a
    /// name with a tag or the time index: the output names each column once.
    fn fused_root_schema(
        tree: &NativeTree,
        source_tags: &[Vec<String>],
        time: &str,
        root_kind: FieldKind,
    ) -> Option<DFSchemaRef> {
        let output_tags = match &tree.root().output {
            NativeType::Scalar(_) => Vec::new(),
            NativeType::Samples(_) => native_output_tags(tree.root(), source_tags)?,
            NativeType::AggregateState(_) => return None,
        };
        let root_fields = native_root_fields(tree.root())?;
        let mut column_names = std::collections::HashSet::new();
        let columns = std::iter::once(time)
            .chain(root_fields.iter().map(String::as_str))
            .chain(output_tags.iter().map(String::as_str));
        if !columns.clone().all(|name| column_names.insert(name)) {
            return None;
        }
        // The kind is the one the statement's own root answers, derived before segmentation cut
        // anything. It cannot be recomputed here: a cut prefix is replaced by an input leaf whose
        // samples are already evaluated, so walking this tree would read every such leaf as the
        // kind its scan carried. `count_over_time(h[5m]) + on (__name__) ...` answers floats, and
        // its replaced leaf carries floats, while the scan below was a histogram.
        fused_output_schema(&root_fields, &output_tags, time, root_kind).ok()
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

/// The typed empty input of a discovered selector that matched no metric table.
///
/// No table matched, so there is no scan and no metric name: the input is an empty raw series
/// frame, the one input a [`FusedSeries`] node and every operator above it already read. It is
/// bound to a series schema of one field and no grid because that is the layout this route compiles
/// a selector against, and a schema with no field is refused by the compiler for the shapes that
/// read one. No row of that field is ever read, so its name decides only what an operator that
/// answers on its own - `absent` - calls its value column.
///
/// The empty plan declares the frame a scan would declare, so the physical planner reads it as the
/// frame producer it is instead of looking for a region scan below the node. Its labels are
/// explicit, which is the mode a producer names each series in rather than a scan's storage key.
fn empty_discovered_input(time_index: String) -> Result<PreparedInput> {
    let schema = Arc::new(SeriesSchema {
        fields: Arc::from([FieldSpec {
            name: Arc::from(DEFAULT_FIELD_COLUMN),
        }]),
        eval_grid: None,
    });
    let frame =
        raw_series_frame_schema_with_ordering(&schema, &RawLabelSource::Explicit, NATIVE_ORDERING)
            .context(DataFusionPlanningSnafu)?;
    let input = LogicalPlan::EmptyRelation(EmptyRelation {
        produce_one_row: false,
        schema: Arc::new(
            DFSchema::try_from(frame.as_ref().clone()).context(DataFusionPlanningSnafu)?,
        ),
    });
    // No table stands behind this input, so no region splits it along a partition column.
    Ok((input, schema, None, time_index, Vec::new(), false))
}

/// Whether `plan` is the typed empty input [`empty_discovered_input`] builds.
///
/// It reads no region and holds no scan, so it is not one of the tables a node's binding is derived
/// from: two selectors are "of one table" only if the tables they scan are one.
fn is_empty_discovered_input(plan: &LogicalPlan) -> bool {
    matches!(plan, LogicalPlan::EmptyRelation(relation) if !relation.produce_one_row)
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

/// Whether `node` becomes a segment of its own under a parent that must gather.
///
/// Cut local prefixes, global consumers and canonical merges separately so the prefix keeps its
/// scan width, and every global consumer sees the canonical boundary below it. A pointwise wrapper
/// of a global consumer is cut as well, so it cannot hide that consumer from recursive lowering.
fn cut_as_segment(node: &NativeNode, canonical_segmentation: bool) -> bool {
    if !exportable(node)
        || matches!(node.expr, NativeExpr::Input(_))
        || (!canonical_segmentation
            && matches!(input_locality(node), InputLocality::SinglePartition))
    {
        return false;
    }
    if canonical_segmentation && (contains_global_consumer(node) || is_canonical_cut(node)) {
        return true;
    }
    // A segment builder needs an input, so data-free subtrees stay with their parent.
    let mut sources = Vec::new();
    collect_segment_inputs(node, &mut sources);
    !sources.is_empty()
}

/// Whether any canonical merge in `node` answers with histogram samples, given the kind its input
/// carries.
///
/// A merge collects the pieces of one label set and hands on the samples it was given, so its answer
/// kind is its child's. A histogram answer standing on a merge would travel through the canonical
/// boundary carrying histogram frames, a pairing no execution exercises yet: the planner keeps such
/// a statement on the existing plan until that pairing is covered, and it has to look at every
/// merge, not only a root one.
fn merge_answers_histogram(node: &NativeNode, input_kind: FieldKind) -> bool {
    (matches!(node.expr, NativeExpr::Merge { .. })
        && node.output_field_kind(input_kind) == Some(FieldKind::Histogram))
        || node_children(&node.expr)
            .into_iter()
            .any(|child| merge_answers_histogram(child, input_kind))
}

/// Whether the merge producer's entire prefix can run on each scan partition independently.
fn canonical_local_prefix(node: &NativeNode) -> bool {
    match &node.expr {
        NativeExpr::Input(_) => true,
        NativeExpr::Instant { input, .. } | NativeExpr::Window { input, .. } => {
            matches!(input.expr, NativeExpr::Input(_))
        }
        NativeExpr::Map { input, .. } | NativeExpr::Timestamp { input } => {
            canonical_local_prefix(input)
        }
        NativeExpr::ParameterizedMap {
            input, parameters, ..
        } => canonical_local_prefix(input) && parameters.iter().all(data_free),
        NativeExpr::Binary { left, right, .. } => {
            (data_free(left) && canonical_local_prefix(right))
                || (data_free(right) && canonical_local_prefix(left))
        }
        _ => false,
    }
}

fn contains_canonical_merge(node: &NativeNode) -> bool {
    matches!(node.expr, NativeExpr::Merge { .. })
        || node_children(&node.expr)
            .into_iter()
            .any(contains_canonical_merge)
}

fn contains_global_consumer(node: &NativeNode) -> bool {
    if is_global_consumer(node) {
        return true;
    }
    node_children(&node.expr)
        .into_iter()
        .any(contains_global_consumer)
}

/// Whole-vector consumers remain global, but can be cut out as evaluated segments so a pointwise
/// parent does not inherit their single-partition placement.
fn is_global_consumer(node: &NativeNode) -> bool {
    matches!(
        node.expr,
        NativeExpr::Aggregate { .. } | NativeExpr::Vector { .. }
    ) && matches!(input_locality(node), InputLocality::SinglePartition)
}

/// Whether a subtree can be cut without inheriting its parent's global gather.
///
/// A local prefix stays on its source partitions. A canonical merge becomes its own routed
/// boundary, and a parent reads the assembled frames rather than gathering the scan.
fn is_canonical_cut(node: &NativeNode) -> bool {
    matches!(node.expr, NativeExpr::Merge { .. })
        || !matches!(input_locality(node), InputLocality::SinglePartition)
        || canonical_local_prefix(node)
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
fn collect_segment(
    node: &NativeNode,
    sources: &mut Vec<usize>,
    cut: &mut Vec<NativeNode>,
    canonical_segmentation: bool,
) {
    match &node.expr {
        NativeExpr::Input(input) => {
            if !sources.contains(&input.source_index) {
                sources.push(input.source_index);
            }
        }
        expr => {
            for child in node_children(expr) {
                if cut_as_segment(child, canonical_segmentation) {
                    cut.push(child.clone());
                } else {
                    collect_segment(child, sources, cut, canonical_segmentation);
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
    canonical_segmentation: bool,
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
                    if cut_as_segment(child, canonical_segmentation) {
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
                        rewrite_segment(child, sources, cut_base, next_cut, canonical_segmentation)
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
fn cut_segments(node: &NativeNode, canonical_segmentation: bool) -> Segment {
    let mut sources = Vec::new();
    let mut cut = Vec::new();
    collect_segment(node, &mut sources, &mut cut, canonical_segmentation);
    let cut_base = sources.len();
    let mut next_cut = 0;
    let rewritten = rewrite_segment(
        node,
        &sources,
        cut_base,
        &mut next_cut,
        canonical_segmentation,
    );
    Segment {
        node: rewritten,
        sources,
        cut,
    }
}

/// Lowers a tree whose root is a canonical merge into a producer and a canonical router.
///
/// The tree says the statement answers one series per canonical label set; what it cannot say is
/// where the pieces of one set are, because a rewrite created them after the scan split the rows.
/// So the pieces have to meet before the answer is written, and there are only two honest ways to
/// make them meet: read every partition in one execution, or route each label set to one owner. The
/// first takes the caller's parallelism away for the whole query; this is the second.
///
/// Two stages come out of it, and each has one job:
///
/// - the **producer** runs the tree's root merge on every scan partition. Its merge collects the
///   pieces that partition holds - which is what makes a same-partition collision an error here,
///   before anything is routed - and it exports one series per label set as a series frame. It is
///   explicitly partition-local: the same node without that distinction would gather its inputs and
///   answer everything in one execution.
/// - the **router** is `SeriesRepartitionExec`'s canonical routing, which collects the pieces of one
///   label set from *every* input partition. It is the conservative router - a label set is
///   published only once every producer contributed it or ended - because a producer promises one
///   series per set *per partition*, not that each set lives in one partition. A second piece of one
///   set that holds a position another piece already holds is the same error, reported while that
///   label set is assembled, and each output of the router then holds whole label sets: one series
///   per set, complementary points joined.
///
/// `None` when the boundary cannot be built, so the caller keeps the plan it would have chosen.
#[allow(clippy::too_many_arguments)]
fn canonical_boundary_plan(
    tree: &NativeTree,
    children: &[LogicalPlan],
    source_tags: &[Vec<String>],
    grid: &Arc<EvalGrid>,
    time_index: &str,
    tags: &[String],
    metric: &Option<Arc<str>>,
    root_kind: FieldKind,
) -> Option<LogicalPlan> {
    let (plan, _) = canonical_boundary_stages(
        tree,
        children,
        source_tags,
        grid,
        time_index,
        tags,
        metric,
        FusedOutputFormat::PlainRows,
        root_kind,
    )?;
    Some(plan)
}

/// The stages of a canonical boundary over `tree`, and the columns its frames carry.
///
/// `format` says what the caller above reads: a statement root reads the boundary's rows, and
/// anything else reads the assembled frames as an ordinary evaluated input.
#[allow(clippy::too_many_arguments)]
fn canonical_boundary_stages(
    tree: &NativeTree,
    children: &[LogicalPlan],
    source_tags: &[Vec<String>],
    grid: &Arc<EvalGrid>,
    time_index: &str,
    tags: &[String],
    metric: &Option<Arc<str>>,
    format: FusedOutputFormat,
    root_kind: FieldKind,
) -> Option<(LogicalPlan, Vec<String>)> {
    let NativeExpr::Merge { input } = &tree.root().expr else {
        return None;
    };
    if !canonical_local_prefix(input) {
        return None;
    }
    let NativeType::Samples(schema) = &tree.root().output else {
        return None;
    };
    schema.eval_grid.as_ref()?;
    let fields = schema
        .fields
        .iter()
        .map(|field| field.name.to_string())
        .collect::<Vec<_>>();
    let output_tags = fused_output_tags(tree.root(), source_tags)?;
    let output_schema = fused_output_schema(&fields, &output_tags, time_index, root_kind).ok()?;

    // The producer: the statement's own tree, run per partition, exporting a frame.
    let producer = FusedSeries::try_new_canonical_producer(
        tree.clone(),
        FusedSeriesBinding {
            grid: Arc::clone(grid),
            time_index: time_index.to_string(),
            tags: tags.to_vec(),
            metric_name: metric.clone(),
        },
        children.to_vec(),
    )
    .ok()?;
    let producer = LogicalPlan::Extension(Extension {
        node: Arc::new(producer),
    });

    // What reads the router depends on where this boundary sits. A consumer still above it reads
    // whole label sets, and the router already merged the pieces of one label set into one series,
    // so that consumer is an ordinary evaluated input of its own and needs no merge. The statement's
    // root instead publishes rows, which is this boundary's own frame converted as usual.
    if format == FusedOutputFormat::SeriesFrame {
        return Some((producer, output_tags));
    }
    let evaluated = NativeType::Samples(Arc::clone(schema));
    let consumer_tree = NativeTree::try_new(NativeNode {
        expr: NativeExpr::Input(BoundInput {
            source_index: 0,
            output: evaluated.clone(),
            ordering: SeriesOrdering::Contiguous,
        }),
        output: evaluated,
    })
    .ok()?;
    // A frame names each series' own metric, so the binding names one only when a tag column does.
    let consumer_metric = if output_tags.iter().any(|tag| tag == METRIC_NAME) {
        metric.clone()
    } else {
        None
    };
    let consumer = FusedSeries::try_new_with_format(
        consumer_tree,
        FusedSeriesBinding {
            grid: Arc::clone(grid),
            time_index: time_index.to_string(),
            tags: output_tags.clone(),
            metric_name: consumer_metric,
        },
        vec![producer],
        output_schema,
        FusedOutputFormat::PlainRows,
    )
    .ok()?;
    Some((
        LogicalPlan::Extension(Extension {
            node: Arc::new(consumer),
        }),
        output_tags,
    ))
}

fn is_raw_series_source(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Extension(extension) => extension.node.as_any().is::<RawSeriesSource>(),
        _ => false,
    }
}

fn find_input_node(node: &NativeNode, source_index: usize) -> Option<&BoundInput> {
    match &node.expr {
        NativeExpr::Input(input) if input.source_index == source_index => Some(input),
        expr => node_children(expr)
            .into_iter()
            .find_map(|child| find_input_node(child, source_index)),
    }
}

#[allow(clippy::too_many_arguments, clippy::type_complexity)]
fn build_evaluated_segment(
    node: &NativeNode,
    children: &[LogicalPlan],
    source_tags: &[Vec<String>],
    source_ids: &[Option<usize>],
    per_source: &[(Option<Arc<str>>, String, Vec<String>)],
    grid: &Arc<EvalGrid>,
    time_index: &str,
    root_kind: FieldKind,
) -> Option<(LogicalPlan, Vec<String>)> {
    let mut sources = Vec::new();
    collect_segment_inputs(node, &mut sources);
    if sources.is_empty() {
        return None;
    }
    let mut inputs = Vec::with_capacity(sources.len());
    let mut local_tags = Vec::with_capacity(sources.len());
    let source_fields =
        tree_source_fields(&NativeTree::try_new(rewrite_inner_segment(node, &sources)).ok()?)
            .ok()?;
    for (local_index, &index) in sources.iter().enumerate() {
        let child = children.get(index)?.clone();
        let input = find_input_node(node, index)?;
        if input
            .output
            .samples()
            .is_some_and(|schema| schema.eval_grid.is_some())
            || is_raw_series_source(&child)
        {
            inputs.push(child);
            local_tags.push(source_tags.get(index)?.clone());
        } else {
            let original_index = *source_ids.get(index)?.as_ref()?;
            let (Some((Some(metric), _, tags)), Some(fields)) = (
                per_source.get(original_index),
                source_fields.get(local_index),
            ) else {
                return None;
            };
            inputs.push(LogicalPlan::Extension(Extension {
                node: Arc::new(
                    RawSeriesSource::try_new(
                        Arc::clone(metric),
                        tags.clone(),
                        fields.clone(),
                        child,
                    )
                    .ok()?,
                ),
            }));
            local_tags.push(tags.clone());
        }
    }
    let tree = NativeTree::try_new(rewrite_inner_segment(node, &sources)).ok()?;
    // A canonical merge is not a segment: it is the middle of a boundary whose two frame nodes and
    // router are what answer one series per label set across partitions, so it is built as that
    // boundary and read as a frame.
    if matches!(tree.root().expr, NativeExpr::Merge { .. }) {
        let (plan, tags) = canonical_boundary_stages(
            &tree,
            &inputs,
            &local_tags,
            grid,
            time_index,
            &[],
            &None,
            FusedOutputFormat::SeriesFrame,
            root_kind,
        )?;
        return Some((plan, tags));
    }
    let output_tags = native_output_tags(tree.root(), &local_tags)?;
    if NativeExecutionState::try_new(&tree, Arc::new(UnboundedMemoryPool::default()), None).is_err()
    {
        return None;
    }
    let frame = FusedSeries::series_frame_schema(&tree).ok()?;
    let frame = DFSchema::try_from(frame.as_ref().clone()).ok()?;
    let plan = FusedSeries::try_new_with_format(
        tree,
        FusedSeriesBinding {
            grid: Arc::clone(grid),
            time_index: time_index.to_string(),
            tags: output_tags.clone(),
            metric_name: None,
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
    if sources.is_empty() {
        return None;
    }
    let scans = sources
        .iter()
        .map(|&index| bare_scans.get(index).cloned())
        .collect::<Option<Vec<_>>>()?;
    let source_tags = sources
        .iter()
        .map(|&index| per_source.get(index).map(|(_, _, tags)| tags.clone()))
        .collect::<Option<Vec<_>>>()?;
    let tree = NativeTree::try_new(rewrite_inner_segment(node, &sources)).ok()?;
    let output_tags = native_output_tags(tree.root(), &source_tags)?;
    let (inputs, metric, time_index, tags) = if let [index] = sources.as_slice() {
        let (metric, time_index, tags) = per_source.get(*index)?;
        (scans, metric.clone(), time_index.clone(), tags.clone())
    } else {
        // A binding cannot describe different source identities. Give each child the existing
        // explicit-label source boundary used for separate metric tables.
        let source_fields = tree_source_fields(&tree).ok()?;
        let mut children = Vec::with_capacity(sources.len());
        for (local_index, &source_index) in sources.iter().enumerate() {
            let (Some((Some(metric), _, source_tags)), Some(fields), Some(scan)) = (
                per_source.get(source_index),
                source_fields.get(local_index),
                scans.get(local_index),
            ) else {
                return None;
            };
            children.push(LogicalPlan::Extension(Extension {
                node: Arc::new(
                    RawSeriesSource::try_new(
                        Arc::clone(metric),
                        source_tags.clone(),
                        fields.clone(),
                        scan.clone(),
                    )
                    .ok()?,
                ),
            }));
        }
        let time_index = per_source.get(*sources.first()?)?.1.clone();
        (children, None, time_index, output_tags.clone())
    };
    if NativeExecutionState::try_new(&tree, Arc::new(UnboundedMemoryPool::default()), None).is_err()
    {
        return None;
    }
    let frame = FusedSeries::series_frame_schema(&tree).ok()?;
    let frame = DFSchema::try_from(frame.as_ref().clone()).ok()?;
    let plan = FusedSeries::try_new_with_format(
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
/// partitions it was scanned with. Global consumers and canonical merges are cut in their original
/// order, and recursive segments publish frames for their parents to consume. Only the statement
/// root publishes rows.
#[allow(clippy::too_many_arguments, clippy::type_complexity)]
async fn segment_tree(
    tree: &NativeTree,
    children: &[LogicalPlan],
    source_tags: &[Vec<String>],
    bare_scans: &[LogicalPlan],
    per_source: &[(Option<Arc<str>>, String, Vec<String>)],
    grid: &Arc<EvalGrid>,
    time_index: &str,
    tags: &[String],
    metric: &Option<Arc<str>>,
    format: FusedOutputFormat,
    canonical_segmentation: bool,
    root_kind: FieldKind,
    remote_raw_boundary: bool,
) -> Result<Option<LogicalPlan>> {
    if !matches!(input_locality(tree.root()), InputLocality::SinglePartition) {
        return Ok(None);
    }
    // A segment boundary has to carry something, and the only thing it can carry is a per-series
    // prefix: a subtree that answers each series on its own and can be recomputed. A tree with no
    // such subtree inside it has nothing to cut, and cutting anything else - an aggregate's own
    // state, a constant - is either impossible or a plain recomputation of the same work in one
    // more node. Cutting on locality alone would accept one here.
    // A grouping by `__name__` keeps that label while the fused node's declared columns cannot
    // name it. With multiple sources, groups differing only by metric name would therefore be
    // reported under one label set. A single-source tree cannot merge distinct metric-name groups
    // this way, so this restriction is specific to multi-source segmentation.
    if children.len() > 1 && groups_by_metric_name(tree.root()) {
        return Ok(None);
    }
    let mut current_tree = tree.clone();
    let mut current_children = children.to_vec();
    let mut current_tags = source_tags.to_vec();
    let mut current_source_ids = (0..children.len()).map(Some).collect::<Vec<_>>();
    let mut first_iteration = true;
    let mut changed = false;
    loop {
        if !matches!(
            input_locality(current_tree.root()),
            InputLocality::SinglePartition
        ) {
            break;
        }
        let segment = cut_segments(current_tree.root(), canonical_segmentation);
        if segment.cut.is_empty() {
            break;
        }
        let mut inputs = Vec::with_capacity(segment.sources.len() + segment.cut.len());
        let mut child_tags = Vec::with_capacity(inputs.capacity());
        for &index in &segment.sources {
            let Some(child) = current_children.get(index) else {
                return Ok(None);
            };
            inputs.push(child.clone());
            child_tags.push(current_tags.get(index).cloned().unwrap_or_default());
        }
        let mut child_source_ids = segment
            .sources
            .iter()
            .map(|&index| current_source_ids.get(index).copied().flatten())
            .collect::<Vec<_>>();
        for cut in &segment.cut {
            // A cut may wrap a global consumer in a pointwise rewrite or a later merge. Segment
            // that subtree first so its inner canonical boundary stays below the consumer. A local
            // canonical merge instead goes directly to the boundary builder.
            let canonical = matches!(cut.expr, NativeExpr::Merge { .. });
            let built = if first_iteration && contains_global_consumer(cut) {
                let Some(cut_tree) = NativeTree::try_new(cut.clone()).ok() else {
                    return Ok(None);
                };
                let plan = Box::pin(segment_tree(
                    &cut_tree,
                    &current_children,
                    &current_tags,
                    bare_scans,
                    per_source,
                    grid,
                    time_index,
                    tags,
                    metric,
                    FusedOutputFormat::SeriesFrame,
                    canonical_segmentation,
                    root_kind,
                    remote_raw_boundary,
                ))
                .await?;
                match plan {
                    Some(plan) => {
                        let Some(cut_tags) = fused_output_tags(cut, &current_tags) else {
                            return Ok(None);
                        };
                        Some((plan, cut_tags))
                    }
                    None => None,
                }
            } else if first_iteration && !canonical && !remote_raw_boundary {
                build_inner_segment(cut, bare_scans, per_source, grid)
            } else {
                None
            };
            if built.is_none() && contains_global_consumer(cut) {
                return Ok(None);
            }
            let built = built.or_else(|| {
                build_evaluated_segment(
                    cut,
                    &current_children,
                    &current_tags,
                    &current_source_ids,
                    per_source,
                    grid,
                    time_index,
                    root_kind,
                )
            });
            let Some((plan, cut_tags)) = built else {
                return Ok(None);
            };
            inputs.push(plan);
            child_tags.push(cut_tags);
            child_source_ids.push(None);
        }
        let Some(parent) = NativeTree::try_new(segment.node).ok() else {
            return Ok(None);
        };
        first_iteration = false;
        current_tree = parent;
        current_children = inputs;
        current_tags = child_tags;
        current_source_ids = child_source_ids;
        changed = true;
    }
    if !changed {
        return Ok(None);
    }
    let parent = current_tree;
    // Segmentation is useful even when the resulting parent is still single-partition: each
    // exported per-series subtree remains a separate input and is bound to its own source.
    let mut inputs = current_children;
    let mut child_tags = current_tags;
    // A merge at this segment's root still needs its own boundary. An outer rewrite above a
    // recursively cut ranking runs after the ranking, then is canonicalized before its caller.
    if let NativeExpr::Merge { input } = &parent.root().expr {
        let source_fields = tree_source_fields(&parent).ok();
        for index in 0..inputs.len() {
            let Some(bound) = find_input_node(input, index) else {
                return Ok(None);
            };
            if bound
                .output
                .samples()
                .is_some_and(|schema| schema.eval_grid.is_none())
                && !is_raw_series_source(&inputs[index])
            {
                let Some(source_index) = current_source_ids.get(index).copied().flatten() else {
                    return Ok(None);
                };
                let (Some((Some(source_metric), _, source_tags)), Some(fields)) = (
                    per_source.get(source_index),
                    source_fields.as_ref().and_then(|fields| fields.get(index)),
                ) else {
                    return Ok(None);
                };
                let Ok(source) = RawSeriesSource::try_new(
                    Arc::clone(source_metric),
                    source_tags.clone(),
                    fields.clone(),
                    inputs[index].clone(),
                ) else {
                    return Ok(None);
                };
                inputs[index] = LogicalPlan::Extension(Extension {
                    node: Arc::new(source),
                });
                child_tags[index] = source_tags.clone();
            }
        }
        return Ok(canonical_boundary_stages(
            &parent,
            &inputs,
            &child_tags,
            grid,
            time_index,
            tags,
            metric,
            format,
            root_kind,
        )
        .map(|(plan, _)| plan));
    }
    // Nothing below can fall back once physical planning starts, so every tree this route commits
    // to has to be one the runtime executes. A shape the runtime refuses - a grouping by a value
    // field, an operation with no kernel - therefore keeps the plan it had.
    if NativeExecutionState::try_new(&parent, Arc::new(UnboundedMemoryPool::default()), None)
        .is_err()
    {
        return Ok(None);
    }
    let output_schema = match format {
        FusedOutputFormat::PlainRows => {
            let Some(schema) =
                PromPlanner::fused_root_schema(&parent, &child_tags, time_index, root_kind)
            else {
                return Ok(None);
            };
            schema
        }
        FusedOutputFormat::SeriesFrame => {
            let Ok(frame) = FusedSeries::series_frame_schema(&parent) else {
                return Ok(None);
            };
            let Ok(schema) = DFSchema::try_from(frame.as_ref().clone()) else {
                return Ok(None);
            };
            Arc::new(schema)
        }
    };
    // A recursively constructed segment still has its original source binding. Its parent may
    // combine another metric; giving the segment that parent's metric would relabel both inputs.
    // Direct scan segments keep their established builder and binding unchanged.
    let segment_binding = |source_ids: &[Option<usize>]| -> (Option<Arc<str>>, String) {
        let mut sources = source_ids.iter().flatten().copied();
        let Some(source) = sources.next() else {
            return (metric.clone(), time_index.to_string());
        };
        if sources.next().is_some() {
            return (None, time_index.to_string());
        }
        per_source.get(source).map_or_else(
            || (metric.clone(), time_index.to_string()),
            |(metric, time, _)| (metric.clone(), time.clone()),
        )
    };
    let (segment_metric, segment_time_index) = segment_binding(&current_source_ids);
    let Ok(plan) = FusedSeries::try_new_with_format(
        parent,
        FusedSeriesBinding {
            grid: Arc::clone(grid),
            time_index: segment_time_index,
            tags: tags.to_vec(),
            metric_name: segment_metric,
        },
        inputs,
        output_schema,
        format,
    ) else {
        return Ok(None);
    };
    Ok(Some(LogicalPlan::Extension(Extension {
        node: Arc::new(plan),
    })))
}

/// The native root's value columns, named from the compiled tree rather than legacy expressions.
fn native_root_fields(node: &NativeNode) -> Option<Vec<String>> {
    match &node.output {
        NativeType::Samples(schema) => Some(
            schema
                .fields
                .iter()
                .map(|field| field.name.to_string())
                .collect(),
        ),
        NativeType::Scalar(_) => Some(vec![DEFAULT_FIELD_COLUMN.to_string()]),
        NativeType::AggregateState(_) => None,
    }
}

/// The columns a [`FusedSeries`] node produces.
///
/// With `experimental_native_promql` enabled, a value column is named after its compiled root
/// field (for example, `field_0`), not legacy planner expression text. Legacy headers in
/// `tests/cases/**` are unaffected because the native route is opt-in and off by default.
///
/// The shape is the one the existing unary pipeline consumes — the time index, then the series
/// fields, then the tags — and each column is declared the way the node's own conversion reads it
/// back:
/// the time column is a non-nullable millisecond timestamp with no timezone, because every
/// evaluated chunk is addressed on the statement's grid and carries one; a field is a nullable
/// `Float64`, because a grid point inside the statement's interval can have no sample; and a tag is
/// a nullable `Utf8`, because a range function drops the metric name and a series can be missing a
/// label entirely.
/// The output schema of a fused node: its time index, one column per field, then its tags.
///
/// `field_kind` says what a field column carries. The physical conversion reads the same kind off
/// this schema and dispatches on it, so the schema and the batch it describes cannot disagree.
fn fused_output_schema(
    fields: &[String],
    tags: &[String],
    time_index: &str,
    field_kind: FieldKind,
) -> Result<DFSchemaRef> {
    let field_type = match field_kind {
        FieldKind::Float => ArrowDataType::Float64,
        FieldKind::Histogram => native_histogram_value_type().as_arrow_type(),
    };
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
        columns.push((None, Arc::new(Field::new(name, field_type.clone(), true))));
    }
    for name in tags {
        let mut field = Field::new(name, ArrowDataType::Utf8, true);
        // The runtime answers `__name__` from the metric name of the series it reports, so a tag
        // column of that name is the name of each row rather than one name for the whole result.
        // Declaring it keeps the response layer from naming every row after the statement.
        if name == METRIC_NAME {
            field = field.with_metadata(datatypes::schema::metric_name_field_metadata());
        }
        columns.push((None, Arc::new(field)));
    }

    Ok(Arc::new(
        DFSchema::new_with_metadata(columns, Default::default())
            .context(DataFusionPlanningSnafu)?,
    ))
}

/// Builds the two nodes a split native aggregate runs in.
///
/// The first is a [`FusedSeries`] over `rawinput` running the per-series prefix and a `Partial`
/// aggregate at its root, so each partition exports the state it folded. The second is a
/// [`FusedSeries`] over the first running the `Final` aggregate and everything the split moved
/// above it: it reads state where the first reads series, and answers the statement's rows. Between
/// them the physical planner puts a `SeriesRepartitionExec`, which is what makes every row of a
/// group reach one final partition.
///
/// The statement's output value columns must match the partial state's accumulator count; a
/// mismatch means the declared output shape does not match the state schema of the split tree.
fn plan_split_aggregate(
    partial: NativeTree,
    final_tree: NativeTree,
    binding: FusedSeriesBinding,
    rawinput: LogicalPlan,
    output_schema: DFSchemaRef,
) -> Result<FusedSeries> {
    let state_accumulators = match &partial.root().output {
        NativeType::AggregateState(state) => state.accumulators.len(),
        _ => {
            return Err(DataFusionError::Plan(
                "the partial tree does not produce aggregate state".into(),
            ))
            .context(DataFusionPlanningSnafu);
        }
    };
    let values = output_schema
        .fields()
        .iter()
        .filter(|field| field.data_type() == &ArrowDataType::Float64)
        .count();
    if values != state_accumulators {
        return Err(DataFusionError::Plan(format!(
            "the partial state has {state_accumulators} accumulator(s), but the output schema declares \
             {values} value column(s)"
        )))
        .context(DataFusionPlanningSnafu);
    }
    let partial = FusedSeries::try_new_state_export(partial, binding.clone(), vec![rawinput])
        .context(DataFusionPlanningSnafu)?;
    FusedSeries::try_new(
        final_tree,
        binding,
        vec![LogicalPlan::Extension(Extension {
            node: Arc::new(partial),
        })],
        output_schema,
    )
    .context(DataFusionPlanningSnafu)
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

/// Whether any aggregate in `node` consumes histogram samples, wherever it sits in the tree.
///
/// The walk asks each aggregate what kind enters it rather than what the tree's root answers,
/// because an operator above an aggregate hides it: `sum(h) * 2` has a binary root. Only a
/// histogram entering an aggregate is refused; a float-producing prefix such as
/// `count_over_time` over a histogram stays eligible.
fn aggregate_reads_histogram(node: &NativeNode, input_kind: FieldKind) -> bool {
    match &node.expr {
        // An aggregate folds float state, so it is refused unless the kind entering it is
        // provably a float. "Cannot name it" is not "not a histogram": a canonical merge
        // (`label_replace` collapsing a label) and a set operation both hand on the samples they
        // were given without the walk describing them, and reading that silence as float is what
        // admitted `sum(label_replace(h, ...))` to a native fold over histograms.
        NativeExpr::Aggregate { input, .. } => {
            !matches!(input.output_field_kind(input_kind), Some(FieldKind::Float))
                || aggregate_reads_histogram(input, input_kind)
        }
        NativeExpr::Input(_)
        | NativeExpr::Scalar(_)
        | NativeExpr::EvalTime { .. }
        | NativeExpr::VectorOf { .. } => false,
        NativeExpr::Instant { input, .. }
        | NativeExpr::Merge { input }
        | NativeExpr::Window { input, .. }
        | NativeExpr::Map { input, .. }
        | NativeExpr::Timestamp { input }
        | NativeExpr::Vector { input, .. } => aggregate_reads_histogram(input, input_kind),
        NativeExpr::ParameterizedMap {
            input, parameters, ..
        } => {
            aggregate_reads_histogram(input, input_kind)
                || parameters
                    .iter()
                    .any(|node| aggregate_reads_histogram(node, input_kind))
        }
        NativeExpr::Binary { left, right, .. } | NativeExpr::Set { left, right, .. } => {
            aggregate_reads_histogram(left, input_kind)
                || aggregate_reads_histogram(right, input_kind)
        }
        NativeExpr::Info { base, info, .. } => {
            aggregate_reads_histogram(base, input_kind)
                || aggregate_reads_histogram(info, input_kind)
        }
    }
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

/// The most metric tables one discovered selector will scan.
///
/// Every one of them costs a scan of the physical region behind it, and the union is as wide as
/// the schema, so a selector that matches everything is answered by narrowing it rather than by
/// planning one query that reads the whole schema at once.
const MAX_DISCOVERED_METRICS: usize = 1024;

/// Whether a selector names no single table and has to enumerate the schema's metric tables to
/// answer.
///
/// Exported so the authorization layer can ask the same question before it authorizes what the
/// planner will scan; the two answers have to agree. #10107 uses it from the frontend.
pub fn discovers_metric_name(selector: &VectorSelector) -> bool {
    let name_matchers = selector.matchers.find_matchers(METRIC_NAME);
    let has_non_equal_name_matcher = name_matchers
        .iter()
        .any(|matcher| !matches!(&matcher.op, MatchOp::Equal));
    let has_exact_name_matcher = name_matchers.len() == 1 && !has_non_equal_name_matcher;
    selector.name.is_none()
        && !has_exact_name_matcher
        && selector.matchers.or_matchers.is_empty()
        && (name_matchers.len() > 1
            || has_non_equal_name_matcher
            || selector.matchers.matchers.iter().any(|matcher| {
                !["__schema__", "__database__", "__field__", METRIC_NAME]
                    .contains(&matcher.name.as_str())
            }))
}

/// Whether a binary operation reports the samples of the side it keeps, name included.
fn keeps_metric_name(matching: &promql::vector_match::BinaryMatching) -> bool {
    use promql::vector_match::BinaryOp;
    matches!(
        matching.op,
        BinaryOp::Eq | BinaryOp::Ne | BinaryOp::Gt | BinaryOp::Lt | BinaryOp::Ge | BinaryOp::Le
    ) && !matching.return_bool
}

/// The tags a node reports after the metric name is dropped, which is what its runtime does to the
/// samples it reports.
fn without_metric_name(tags: Vec<String>) -> Vec<String> {
    tags.into_iter().filter(|tag| tag != METRIC_NAME).collect()
}

/// The label columns `node` reports, from the tags of the scan each input reads.
///
/// They follow Prometheus' result labels: a one-to-one match keeps the left side's labels, narrowed
/// by `on` or `ignoring`; `and` and `unless` keep the left side's series and `or` adds the right
/// side's; `info` adds the data labels of the info table; an operation with a scalar or a function
/// of each value keeps its operand's; a scalar and `vector(s)` have none. `None` for a node whose
/// labels this walk does not derive - a whole-vector operation or a label rewrite, which renames the
/// labels below it - so a tree holding one reports no label set here. Such a tree is declined rather
/// than borrowing output names from the legacy planner.
fn native_output_tags(node: &NativeNode, source_tags: &[Vec<String>]) -> Option<Vec<String>> {
    match &node.expr {
        NativeExpr::Aggregate { input, op } => {
            if matches!(&op.group, Grouping::By(names) if names.is_empty()) {
                return Some(Vec::new());
            }
            let tags = native_output_tags(input, source_tags)?;
            Some(match &op.group {
                Grouping::By(names) => tags
                    .into_iter()
                    .filter(|tag| names.iter().any(|name| name.as_ref() == tag))
                    .collect(),
                Grouping::Without(names) => tags
                    .into_iter()
                    .filter(|tag| {
                        tag != METRIC_NAME && !names.iter().any(|name| name.as_ref() == tag)
                    })
                    .collect(),
            })
        }
        NativeExpr::Input(input) => source_tags.get(input.source_index).cloned(),
        NativeExpr::Scalar(_) | NativeExpr::EvalTime { .. } | NativeExpr::VectorOf { .. } => {
            Some(Vec::new())
        }
        // These report a value of their own rather than the sample they read, and the runtime drops
        // the name for each of them. `source_tags` is the authority on what an input carries, so a
        // discovery input arrives carrying `__name__` and these arms are what decides it does not
        // survive.
        NativeExpr::Instant { input, op } => {
            let tags = native_output_tags(input, source_tags)?;
            Some(match op.value {
                InstantValue::Timestamp => without_metric_name(tags),
                InstantValue::Sample | InstantValue::InfoTimestamp => tags,
            })
        }
        NativeExpr::Window { input, op } => {
            let tags = native_output_tags(input, source_tags)?;
            Some(if op.function.drops_metric_name() {
                without_metric_name(tags)
            } else {
                tags
            })
        }
        NativeExpr::Timestamp { input } => {
            Some(without_metric_name(native_output_tags(input, source_tags)?))
        }
        // A label rewrite changes the label set itself: `label_replace` and `label_join` write their
        // destination label, so the input's labels are not the output's. A map of math alone keeps
        // them, minus the metric name its kernel drops.
        NativeExpr::Map { input, op } => {
            if op.ops.iter().any(|op| matches!(op, MapOp::Label(_))) {
                let mut tags = native_output_tags(input, source_tags)?;
                for operation in op.ops.iter() {
                    if let MapOp::Label(rewrite) = operation {
                        let destination = rewrite.destination();
                        if destination == "__name__" {
                            return None;
                        }
                        tags.retain(|tag| tag != destination);
                        tags.push(destination.to_string());
                    }
                }
                return Some(tags);
            }
            let tags = native_output_tags(input, source_tags)?;
            // A math kernel drops the name; a histogram kernel drops only the name, keeping the type
            // and unit.
            Some(
                if op
                    .ops
                    .iter()
                    .any(|op| matches!(op, MapOp::Math(_) | MapOp::Histogram(_)))
                {
                    without_metric_name(tags)
                } else {
                    tags
                },
            )
        }
        // Its parameters are scalars, which carry no labels, so what the node reports are its
        // input's labels.
        NativeExpr::ParameterizedMap { input, .. } => {
            Some(without_metric_name(native_output_tags(input, source_tags)?))
        }
        NativeExpr::Vector { input, op } => match op {
            BoundVectorOp::TopK { .. }
            | BoundVectorOp::LimitK { .. }
            | BoundVectorOp::LimitRatio { .. }
            | BoundVectorOp::Sort(_) => native_output_tags(input, source_tags),
            // Keep in sync with promql::grid_series::METADATA_LABELS, which is crate-private.
            BoundVectorOp::Histogram(_) => native_output_tags(input, source_tags).map(|tags| {
                tags.into_iter()
                    .filter(|tag| {
                        tag != "le" && tag != "__name__" && tag != "__type__" && tag != "__unit__"
                    })
                    .collect()
            }),
            BoundVectorOp::Absent { labels } => Some(
                labels
                    .tags()
                    .iter()
                    .map(|label| label.name.to_string())
                    .collect(),
            ),
            BoundVectorOp::CountValues { label, group } => {
                let mut tags = native_output_tags(input, source_tags)?;
                if !tags.contains(&label.to_string()) {
                    tags.push(label.to_string());
                }
                Some(match group {
                    Grouping::By(names) => {
                        let mut output = tags
                            .into_iter()
                            .filter(|tag| names.iter().any(|name| name.as_ref() == tag))
                            .collect::<Vec<_>>();
                        if !output.contains(&label.to_string()) {
                            output.push(label.to_string());
                        }
                        output
                    }
                    Grouping::Without(names) => tags
                        .into_iter()
                        .filter(|tag| {
                            tag != METRIC_NAME && !names.iter().any(|name| name.as_ref() == tag)
                        })
                        .collect(),
                })
            }
        },
        // The merge already assembled one series per canonical label set, so its labels are the
        // labels its input reported.
        NativeExpr::Merge { input } => native_output_tags(input, source_tags),
        // A filtering comparison reports the samples of the side it keeps, so the name survives it.
        // Every other binary operation computes new values, and `bool` reports the comparison's
        // outcome rather than either operand's samples, so both drop the name.
        NativeExpr::Binary {
            left,
            right,
            matching,
        } => {
            let keeps_name = keeps_metric_name(matching);
            let tags = match (&left.output, &right.output) {
                (NativeType::Scalar(_), _) => native_output_tags(right, source_tags)?,
                (_, NativeType::Scalar(_)) => native_output_tags(left, source_tags)?,
                _ => {
                    let kept = |tags: Vec<String>| -> Vec<String> {
                        match &matching.labels {
                            MatchingLabels::On(names) if !names.is_empty() => tags
                                .into_iter()
                                .filter(|tag| {
                                    names.iter().any(|name| name.as_ref() == tag.as_str())
                                })
                                .collect(),
                            MatchingLabels::Ignoring(names) => tags
                                .into_iter()
                                .filter(|tag| {
                                    !names.iter().any(|name| name.as_ref() == tag.as_str())
                                })
                                .collect(),
                            MatchingLabels::On(_) => tags,
                        }
                    };
                    let left_tags = native_output_tags(left, source_tags)?;
                    let right_tags = native_output_tags(right, source_tags)?;
                    // A group takes the many side's labels as they are, because its result keeps
                    // one series per series of that side: `group_left` reports the left side's
                    // labels, `group_right` the right side's. `group_left(...)`/`group_right(...)`
                    // add the labels the modifier names from the one side. A one-to-one match
                    // reports the left side's labels narrowed by `on` or `ignoring`.
                    let mut tags = match &matching.cardinality {
                        Cardinality::OneToOne => kept(left_tags.clone()),
                        Cardinality::ManyToOne(_) => left_tags.clone(),
                        Cardinality::OneToMany(_) => right_tags.clone(),
                    };
                    let (includes, include_tags) = match &matching.cardinality {
                        Cardinality::OneToOne => (&[][..], &[][..]),
                        Cardinality::ManyToOne(labels) => {
                            (labels.as_slice(), right_tags.as_slice())
                        }
                        Cardinality::OneToMany(labels) => (labels.as_slice(), left_tags.as_slice()),
                    };
                    for tag in include_tags {
                        if includes.iter().any(|label| label.as_ref() == tag.as_str())
                            && !tags.contains(tag)
                        {
                            tags.push(tag.clone());
                        }
                    }
                    // A filled left side takes an unmatched right series' matching labels, so
                    // those results carry the right side's tags.
                    if matching.fill.lhs.is_some() {
                        for tag in kept(native_output_tags(right, source_tags)?) {
                            if !tags.contains(&tag) {
                                tags.push(tag);
                            }
                        }
                    }
                    tags
                }
            };
            Some(if keeps_name {
                tags
            } else {
                without_metric_name(tags)
            })
        }
        NativeExpr::Set { left, right, op } => {
            let mut tags = native_output_tags(left, source_tags)?;
            if op.op == SetOp::Or {
                for tag in native_output_tags(right, source_tags)? {
                    if !tags.contains(&tag) {
                        tags.push(tag);
                    }
                }
                // `or` reports the series of both sides, and each row keeps the name of the side it
                // came from. The runtime already answers that name for every series it reports, so
                // the column it is read from is the one the tags name.
                if !tags.iter().any(|tag| tag == METRIC_NAME) {
                    tags.push(METRIC_NAME.to_string());
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
            let mut tags = native_output_tags(base, source_tags)?;
            for tag in native_output_tags(info, source_tags)? {
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
/// source: a discovered selector's source declares `__name__` per metric, and a named one folds one
/// metric whose name cannot merge two groups. What the check is for is the tree reading more than one
/// source, where only a discovered source can report a name at all.
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

/// Whether a hybrid operand contains an aggregate, which needs the split route rather than a join.
fn contains_aggregate_expression(expr: &PromExpr) -> bool {
    match expr {
        PromExpr::Aggregate(_) => true,
        PromExpr::Paren(paren) => contains_aggregate_expression(&paren.expr),
        PromExpr::Unary(unary) => contains_aggregate_expression(&unary.expr),
        PromExpr::Binary(binary) => {
            contains_aggregate_expression(&binary.lhs) || contains_aggregate_expression(&binary.rhs)
        }
        PromExpr::Call(call) => call
            .args
            .args
            .iter()
            .any(|arg| contains_aggregate_expression(arg)),
        _ => false,
    }
}

/// Whether this hybrid operand is a single-selector expression the native route can place.
fn single_selector_operand(expr: &PromExpr) -> bool {
    let stmt = EvalStmt {
        expr: expr.clone(),
        start: UNIX_EPOCH,
        end: UNIX_EPOCH,
        interval: Duration::from_millis(1),
        lookback_delta: Duration::from_millis(1),
    };
    let mut selectors = Vec::new();
    collect_selectors(&stmt, expr, &mut selectors) && selectors.len() == 1
}

/// The selectors `expr` reads, in the order the compiler binds them.
///
/// This is a structural walk, not a function capability list: calls are traversed and the compiler
/// decides whether their shapes have native kernels. `info` additionally reads a selector it does
/// not write; [`info_input`] derives that same selector here and the compiler binds it after the base.
fn collect_selectors<'a>(
    stmt: &EvalStmt,
    expr: &'a PromExpr,
    out: &mut Vec<Cow<'a, VectorSelector>>,
) -> bool {
    match expr {
        PromExpr::VectorSelector(selector) => out.push(Cow::Borrowed(selector)),
        PromExpr::MatrixSelector(MatrixSelector { vs, .. }) => out.push(Cow::Borrowed(vs)),
        PromExpr::Paren(ParenExpr { expr }) => return collect_selectors(stmt, expr, out),
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
        // Literals contribute no selector; the compiler decides which scalar forms can be bound.
        PromExpr::NumberLiteral(_) => return true,
        PromExpr::Unary(unary) => return collect_selectors(stmt, &unary.expr, out),
        PromExpr::Call(call) => {
            if matches!(call.func.name, "label_replace" | "label_join") {
                let arity_valid = if call.func.name == "label_replace" {
                    call.args.args.len() == 5
                } else {
                    call.args.args.len() >= 3
                };
                return arity_valid
                    && matches!(call.args.args.first(), Some(arg) if matches!(arg.value_type(), ValueType::Vector | ValueType::Matrix))
                    && collect_selectors(stmt, &call.args.args[0], out);
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
/// This is a scanner capability boundary: it avoids probing a region during logical planning, and
/// declining leaves the existing plan available.
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

/// Whether `expr` contains `rate`/`increase`, used only by the delta-temporality semantic safety
/// gate, not as a native-function capability list.
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

    use datafusion::physical_plan::ExecutionPlan;
    use datatypes::series::SeriesOrdering;
    use datatypes::series::samples::{FieldSpec, SeriesSchema};
    use promql::native::plan::{NativeExpr, NativeNode, NativeTree};
    use session::context::QueryContextBuilder;

    use super::{
        ArrowDataType, ArrowTimeUnit, FieldKind, METRIC_NAME, NATIVE_PROMQL_EXTENSION_KEY,
        NativeType, collect_selectors, cut_segments, fused_output_schema, hybrid_binary_admitted,
        independent_scan_pair, native_output_tags, native_promql_requested,
        native_raw_plan_bounds_region_reads, scan_time_filter,
    };

    #[test]
    fn native_raw_region_read_budget_needs_the_native_topology() {
        let schema = Arc::new(datafusion::arrow::datatypes::Schema::new(vec![
            datafusion::arrow::datatypes::Field::new(
                "ts",
                datafusion::arrow::datatypes::DataType::Int64,
                false,
            ),
        ]));
        let scan = || {
            Arc::new(datafusion::physical_plan::empty::EmptyExec::new(
                schema.clone(),
            )) as Arc<dyn ExecutionPlan>
        };

        // No `FusedSeriesExec`: nothing here reads regions the way the budget assumes.
        assert!(!native_raw_plan_bounds_region_reads(&scan()));
        // A union drains its inputs one after another, so a body budget could stall it.
        let union = datafusion::physical_plan::union::UnionExec::try_new(vec![scan(), scan()])
            .expect("two inputs");
        assert!(!native_raw_plan_bounds_region_reads(&union));
    }

    fn compiled_test_tree(query: &str) -> NativeTree {
        compiled_test_tree_with_fields(query, &["v0"])
    }

    fn compiled_test_tree_with_fields(query: &str, fields: &[&str]) -> NativeTree {
        let statement = promql_parser::parser::EvalStmt {
            expr: promql_parser::parser::parse(query).unwrap(),
            start: UNIX_EPOCH,
            end: UNIX_EPOCH + Duration::from_secs(10),
            interval: Duration::from_secs(5),
            lookback_delta: Duration::from_secs(5),
        };
        let schema = Arc::new(SeriesSchema {
            fields: fields
                .iter()
                .map(|name| FieldSpec {
                    name: (*name).into(),
                })
                .collect::<Vec<_>>()
                .into(),
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
            native_output_tags(on.root(), &sources)
        );
        let ignoring = compiled_test_tree("x + x");
        assert_eq!(
            Some(tags.clone()),
            native_output_tags(ignoring.root(), &sources)
        );
        let empty_on = compiled_test_tree("x + on() x");
        assert_eq!(
            Some(tags.clone()),
            native_output_tags(empty_on.root(), &sources)
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
            native_output_tags(wrapped.root(), &sources)
        );
        let and = compiled_test_tree("x and on(host) y");
        assert_eq!(
            Some(sources[0].clone()),
            native_output_tags(and.root(), &sources)
        );
        let or = compiled_test_tree("x or y");
        // `or` reports the series of both sides, so it reports each row's own name as well.
        assert_eq!(
            Some(vec![
                "host".to_string(),
                "kind".to_string(),
                "zone".to_string(),
                "__name__".to_string()
            ]),
            native_output_tags(or.root(), &sources)
        );
        // An unmatched right series answers with its own matching labels under a left fill.
        let fill = compiled_test_tree("x + fill_left(0) y");
        assert_eq!(
            Some(vec![
                "host".to_string(),
                "kind".to_string(),
                "zone".to_string()
            ]),
            native_output_tags(fill.root(), &sources)
        );
        let fill_on = compiled_test_tree("x + on(host) fill(0) y");
        assert_eq!(
            Some(vec!["host".to_string()]),
            native_output_tags(fill_on.root(), &sources)
        );
        // Group modifiers retain the many-side labels and copy declared labels from the one side.
        let group_sources = [
            vec![
                "cluster".into(),
                "namespace".into(),
                "pod".into(),
                "container".into(),
            ],
            vec![
                "cluster".into(),
                "namespace".into(),
                "pod".into(),
                "workload".into(),
                "workload_type".into(),
            ],
        ];
        let group_left = compiled_test_tree(
            "x * on(cluster, namespace, pod) group_left(workload, workload_type) y",
        );
        assert_eq!(
            Some(
                vec![
                    "cluster",
                    "namespace",
                    "pod",
                    "container",
                    "workload",
                    "workload_type"
                ]
                .into_iter()
                .map(String::from)
                .collect()
            ),
            native_output_tags(group_left.root(), &group_sources)
        );
        let group_right =
            compiled_test_tree("x * on(cluster, namespace, pod) group_right(container) y");
        assert_eq!(
            Some(
                vec![
                    "cluster",
                    "namespace",
                    "pod",
                    "workload",
                    "workload_type",
                    "container"
                ]
                .into_iter()
                .map(String::from)
                .collect()
            ),
            native_output_tags(group_right.root(), &group_sources)
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
            // `or` reports the series of both sides, so each row's own name comes with them.
            (
                "x / on(host, kind) y or z",
                vec!["host", "kind", "az", "__name__"],
            ),
        ] {
            assert_eq!(
                Some(expected.into_iter().map(String::from).collect::<Vec<_>>()),
                native_output_tags(compiled_test_tree(query).root(), &sources),
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
                native_output_tags(aggregate.root(), &sources),
                "{query}"
            );
        }
        // Whole-vector operators that retain series keep the input labels, while `absent` derives
        // its output names from the compiler-bound label set and `count_values` adds its value tag.
        let hostname_source = [vec!["hostname".to_string()]];
        for query in [
            "topk(10, sum by (hostname) (rate(x[5m])))",
            "bottomk(10, sum by (hostname) (rate(x[5m])))",
            "limitk(10, sum by (hostname) (rate(x[5m])))",
            "limit_ratio(0.5, sum by (hostname) (rate(x[5m])))",
            "sort(sum by (hostname) (rate(x[5m])))",
        ] {
            assert_eq!(
                Some(vec!["hostname".to_string()]),
                native_output_tags(compiled_test_tree(query).root(), &hostname_source),
                "{query}"
            );
        }
        let absent = compiled_test_tree("absent(x{host=\"a\", job=\"b\"})");
        assert_eq!(
            Some(vec!["host".to_string(), "job".to_string()]),
            native_output_tags(absent.root(), &sources)
        );
        let count_values =
            compiled_test_tree("count_values by (host) (\"sample\", sum by (host) (x))");
        assert_eq!(
            Some(vec!["host".to_string(), "sample".to_string()]),
            native_output_tags(count_values.root(), &sources)
        );
        // Selecting some series keeps each one's own labels, whatever the grouping was.
        let topk = compiled_test_tree("topk(1, x)");
        assert_eq!(
            Some(sources[0].clone()),
            native_output_tags(topk.root(), &sources),
            "topk keeps the labels of the series it selects"
        );
        for query in [
            r#"label_replace(x, "dst", "$1", "src", "(.*)")"#,
            r#"label_join(x, "dst", "-", "src", "src")"#,
        ] {
            let rewrite = compiled_test_tree(query);
            assert_eq!(
                Some(
                    vec!["host", "kind", "zone", "dst"]
                        .into_iter()
                        .map(String::from)
                        .collect()
                ),
                native_output_tags(rewrite.root(), &sources),
                "{query}"
            );
        }
    }

    #[test]
    fn recursive_segmentation_rewrites_nested_cuts_with_dense_inputs() {
        let tree = compiled_test_tree("sum(abs(rate(x[1m])))");
        let segment = cut_segments(tree.root(), true);
        assert!(!segment.cut.is_empty());
        assert_eq!(segment.cut.len(), 1);
        assert_eq!(vec![0], input_sources(&segment.cut[0]));
    }

    #[test]
    fn segmentation_leaves_a_data_free_only_candidate_unchanged() {
        for query in ["sum(vector(1))", "sum(abs(vector(1)))"] {
            let tree = compiled_test_tree(query);
            let segment = cut_segments(tree.root(), false);
            assert!(segment.sources.is_empty(), "{query}");
            assert!(segment.cut.is_empty(), "{query}");
            assert_eq!(
                tree.to_string(),
                NativeTree::try_new(segment.node).unwrap().to_string(),
                "{query}"
            );
        }
    }

    #[test]
    fn group_left_copies_included_labels_into_output_tags() {
        let sources = [
            vec!["pod".into(), "workload".into()],
            vec!["pod".into(), "container".into()],
        ];
        let tree = compiled_test_tree("x * on(pod) group_left(container) y");
        assert_eq!(
            Some(vec!["pod".into(), "workload".into(), "container".into()]),
            native_output_tags(tree.root(), &sources)
        );
    }

    #[test]
    fn output_tags_without_grouping_drops_metric_name() {
        let sources = [vec![
            "instance".to_string(),
            METRIC_NAME.to_string(),
            "job".to_string(),
        ]];

        let sum = compiled_test_tree("sum without (instance) (x)");
        assert_eq!(
            Some(vec!["job".to_string()]),
            native_output_tags(sum.root(), &sources)
        );

        let count_values = compiled_test_tree("count_values without (instance) (\"sample\", x)");
        assert_eq!(
            Some(vec!["job".to_string(), "sample".to_string()]),
            native_output_tags(count_values.root(), &sources)
        );
    }

    #[test]
    fn group_right_copies_included_labels_into_output_tags() {
        let sources = [
            vec!["pod".into(), "container".into()],
            vec!["pod".into(), "workload".into()],
        ];
        let tree = compiled_test_tree("x * on(pod) group_right(container) y");
        assert_eq!(
            Some(vec!["pod".into(), "workload".into(), "container".into()]),
            native_output_tags(tree.root(), &sources)
        );
    }

    #[test]
    fn numeric_aggregate_projection_keeps_grouping_tags_and_value_schema() {
        let sources = [vec![
            "host".to_string(),
            "kind".to_string(),
            "zone".to_string(),
        ]];
        for query in [
            "sum by (host, zone) ((x))",
            "min by (host, zone) ((x))",
            "max without (kind) ((x))",
        ] {
            let tree = compiled_test_tree(query);
            let NativeType::Samples(schema) = &tree.root().output else {
                panic!("{query} must produce evaluated samples");
            };
            assert_eq!(1, schema.fields.len(), "{query}");
            assert_eq!("v0", schema.fields[0].name.as_ref(), "{query}");
            let tags = native_output_tags(tree.root(), &sources).unwrap();
            assert_eq!(
                vec!["host", "zone"],
                tags.iter().map(String::as_str).collect::<Vec<_>>(),
                "{query}"
            );
        }

        let tree = compiled_test_tree_with_fields("sum by (zone, host) ((x))", &["v0", "v1"]);
        let NativeType::Samples(schema) = &tree.root().output else {
            panic!("multi-field aggregate must produce evaluated samples");
        };
        assert_eq!(
            vec!["v0", "v1"],
            schema
                .fields
                .iter()
                .map(|field| field.name.as_ref())
                .collect::<Vec<_>>()
        );
        let output = fused_output_schema(
            &["v0".to_string(), "v1".to_string()],
            &["zone".to_string(), "host".to_string()],
            "ts",
            FieldKind::Float,
        )
        .unwrap();
        assert_eq!(
            vec!["ts", "v0", "v1", "zone", "host"],
            output
                .fields()
                .iter()
                .map(|field| field.name().as_str())
                .collect::<Vec<_>>()
        );
        assert_eq!(
            ArrowDataType::Timestamp(ArrowTimeUnit::Millisecond, None),
            *output.field(0).data_type()
        );
        assert_eq!(ArrowDataType::Float64, *output.field(1).data_type());
        assert_eq!(ArrowDataType::Float64, *output.field(2).data_type());
        assert_eq!(ArrowDataType::Utf8, *output.field(3).data_type());
        assert_eq!(ArrowDataType::Utf8, *output.field(4).data_type());
    }

    #[test]
    fn histogram_quantile_output_tags_drop_bucket_and_metadata_labels() {
        let sources = [vec![
            "le".to_string(),
            "__name__".to_string(),
            "__type__".to_string(),
            "__unit__".to_string(),
            "job".to_string(),
        ]];
        let tree = compiled_test_tree("histogram_quantile(0.9, x)");
        assert_eq!(
            Some(vec!["job".to_string()]),
            native_output_tags(tree.root(), &sources)
        );
    }

    /// A map that only computes values passes its input's labels through unchanged.
    #[test]
    fn match_output_tags_pass_through_pointwise_math() {
        let sources = [vec!["host".to_string(), "kind".to_string()]];
        let tree = compiled_test_tree("abs(x) * 2");
        assert_eq!(
            Some(vec!["host".to_string(), "kind".to_string()]),
            native_output_tags(tree.root(), &sources)
        );
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
            "100 / B",
            "A / (B + C)",
            // A fold on either side: one shared output label set cannot describe both operands, and
            // the shapes that produced a `tag_0` planning error before the aggregate gate existed.
            "sum(A) / B",
            "A / sum(B)",
        ] {
            assert!(!hybrid_admission(query), "{query}");
        }

        for query in [
            "ceil(A) / B",
            "irate(A[5m]) / B",
            "sort(A) / B",
            "absent(A) / B",
            "-A / B",
        ] {
            assert!(hybrid_admission(query), "{query}");
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
