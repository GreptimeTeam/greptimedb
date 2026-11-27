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
//! [`PromPlanner::try_plan_native_fused`] offers a whole statement expression to the native compiler and,
//! when the compiler accepts it, replaces everything the existing path would plan below the root with one
//! [`FusedSeries`] node over the selector's own table scan. A tree that folds across series, such as an
//! aggregate, is split instead: its per-series prefix and a `Partial` aggregate run in a `FusedSeries`
//! producer, a `SeriesRepartitionExec` routes that state, and a second `FusedSeries` runs the `Final`
//! aggregate over it, because a region holds only part of a group. Refusing is the normal answer: every
//! shape the compiler does not accept, and every query this route cannot bind to a scan faithfully,
//! records the cause of its refusal, which the offer's caller reports - a query that asked for this route
//! is answered with that error, and a query that did not keeps the plan the existing planner builds.
//!
//! The node is built from the raw series schema the tree's input was compiled against, the statement's
//! evaluation grid, and output tag names derived from the native tree. The output schema has the shape the
//! existing unary pipeline produces -- the time index, then the series fields, then the tags -- so a node
//! planned here is consumed by the same code that consumes a `SeriesNormalize` output.
//!
//! A statement that reads no data - a constant, `time()`, `vector(time())` - is planned with no scan: one
//! node, no child, the time index and value.

use std::borrow::Cow;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use common_catalog::consts::{METRIC_ENGINE, MITO_ENGINE, MITO2_ENGINE};
use common_query::native_histogram::native_histogram_value_type;
use common_query::prelude::OTLP_AGGREGATION_TEMPORALITY_LABEL;
use common_query::series_frame::raw_series_frame_schema_with_ordering;
use common_telemetry::debug;
use datafusion::arrow::datatypes::DataType as ArrowDataType;
use datafusion::common::{DFSchema, DFSchemaRef};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::UserDefinedLogicalNode;
use datafusion::logical_expr::{EmptyRelation, Extension, LogicalPlan, TableScanBuilder, Union};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::Expr as DfExpr;
use datafusion_expr::Expr;
use datatypes::data_type::DataType as _;
use datatypes::series::SeriesOrdering;
use datatypes::series::samples::{FieldSpec, SeriesSchema};
use datatypes::series::time::EvalGrid;
use futures::future::BoxFuture;
use promql::extension_plan::{
    FieldKind, FusedOutputFormat, FusedSeries, FusedSeriesBinding, FusedSeriesExec, InputLocality,
    RawSeriesSource, input_locality, keys_survive_metadata_drop, matching_operands_drop_key,
};
use promql::native::plan::{
    AggregateKind, BoundAggregate, BoundInput, BoundVectorOp, NativeExpr, NativeNode, NativeTree,
    NativeType, aggregate_state_binding, binary_output_field_kind, fused_output_tags,
    native_tree_supported, uniform_field_kind,
};
use promql::native::segment::{
    self, NATIVE_ORDERING, canonical_boundary_stages, canonical_local_prefix,
    contains_canonical_merge, contains_global_consumer, region_local_segment, segment_tree,
    with_row_name_column,
};
// Kept reachable here because this crate reads them through this module's own path: the dist
// planner's commutativity check asks whether a fused node is per-series work, and this crate's
// planner and tests read a discovered input's shape.
pub(crate) use promql::native::segment::{
    independent_scan_pair, is_empty_discovered_input, pointwise_operand,
};
use promql_parser::label::{METRIC_NAME, MatchOp, Matcher, Matchers};
use promql_parser::parser::token;
use promql_parser::parser::value::ValueType;
use promql_parser::parser::{
    EvalStmt, Expr as PromExpr, MatrixSelector, ParenExpr, SubqueryExpr, VectorSelector,
};
use session::context::{MetricDiscoveryScope, QueryContextRef};
use snafu::{ResultExt, ensure};
use store_api::metric_engine_consts::{DATA_SCHEMA_TABLE_ID_COLUMN_NAME, METRIC_ENGINE_NAME};
use store_api::region_engine::RawLabelSource;

use crate::promql::error::{
    DataFusionPlanningSnafu, DuplicateOutputColumnsSnafu, Error, Result, UnsupportedExprSnafu,
    UnsupportedNativeShapeSnafu,
};
use crate::promql::native_compile::{
    InfoInput, bind_evaluated_binary, compile_selectors_with_grid, compile_statement_with_grid,
    info_input, statement_grid, subquery_child_grid,
};
use crate::promql::planner::{
    DEFAULT_FIELD_COLUMN, DEFAULT_TIME_INDEX_COLUMN, GrownStack, PromPlanner,
};
use crate::query_engine::QueryEngineState;

/// The query option that turns the native fused-series route on for one query.
///
/// Named after the engine-wide option it mirrors ([`QueryOptions::experimental_native_promql`]), it
/// is a query option like the others in this crate: a client states it per request (the HTTP hints
/// header, a gRPC context key) or per session (`SET query.experimental_native_promql = ...`), and
/// the engine-wide option is the default. The three resolve in that order where the query options
/// snapshot is applied, and the planner reads the resulting
/// [`NativePromqlExtension`](crate::options::NativePromqlExtension) rather than this key.
///
/// [`QueryOptions::experimental_native_promql`]: crate::options::QueryOptions::experimental_native_promql
pub const NATIVE_PROMQL_EXTENSION_KEY: &str = "query.experimental_native_promql";

/// Whether the query options of one query ask for the native fused-series route.
///
/// Read from the query options snapshot the query is planned with, so a per-request value and a
/// session `SET` arrive here alike, already normalized: this compares literally (`true` in any
/// case) rather than parsing again, because a value that is not a boolean never reaches the
/// snapshot.
pub(crate) fn native_promql_requested(query_ctx: &QueryContextRef) -> bool {
    query_ctx
        .extension(NATIVE_PROMQL_EXTENSION_KEY)
        .is_some_and(|value| value.eq_ignore_ascii_case("true"))
}

/// Whether one query asks for the native fused-series route, resolved the way planning resolves
/// it: a stated value wins (`request over session `SET``), otherwise the engine default answers.
///
/// This exists for the callers that have to act *before* a plan is built - the frontend bounds
/// metric discovery by the caller's grants only when this route will read it - and it resolves
/// from the same two sources planning reads: [`QueryContext::query_option_snapshot`] for what the
/// query states, and the engine-wide option for what it inherits. Nothing here re-derives the
/// option from a third place, so the two readers cannot disagree.
pub fn native_promql_resolved(
    query_ctx: &QueryContextRef,
    engine_default: bool,
) -> crate::error::Result<bool> {
    let snapshot = query_ctx.query_option_snapshot().map_err(|error| {
        crate::error::InvalidQueryContextExtensionSnafu {
            reason: error.to_string(),
        }
        .build()
    })?;
    Ok(
        match snapshot.extensions().get(NATIVE_PROMQL_EXTENSION_KEY) {
            Some(value) => value.eq_ignore_ascii_case("true"),
            None => engine_default,
        },
    )
}

/// Whether this executable plan is the native raw PromQL topology, the one plan shape whose
/// region reads one query-wide body budget cannot deadlock.
///
/// That topology is a chain of [`FusedSeriesExec`] nodes over region reads, and each fused node polls its
/// input while that input is being drained, so a region body holding a permit keeps making progress.
/// Admission stops there: a plan with any other multi-input node keeps its per-scan bounds only.
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

/// What one discovered metric contributes: the shared schema, its time column, the tags the union
/// reports, and the one metric name the single-source shape carries.
type DiscoveredSelectorMetadata = (Arc<SeriesSchema>, String, Vec<String>, Arc<str>);

/// One prepared occurrence of a selector: its scan, raw-series layout and reporting columns.
///
/// `partitioned` says whether the table behind that scan is split into regions along a partition column.
/// Nothing in the scan carries this: it is a placement fact about the table.
type PreparedInput = (
    LogicalPlan,
    Arc<SeriesSchema>,
    Option<Arc<str>>,
    String,
    Vec<String>,
    // `partitioned`: whether this input's table is split into regions along a partition column.
    bool,
    // `split_series`: whether that partition splits one series across regions, which a bare scan
    // cannot reconcile. A partition on tags keeps every series whole in one region.
    bool,
    // `keys_survive`: whether the resolved table keys its series by columns that always decode a
    // value, read from its own primary key through `keys_survive_metadata_drop`. It is the one fact
    // of the metadata drop a tree cannot answer about itself: the drop removes every empty label
    // value with the name, so a column holding `NULL` in one series and `''` in another leaves both
    // under one label set. The canonical pass does not collect for it, and
    // `UNPROVED_LABELSET_CHECK_NULLABLE_KEY` counts the statements it leaves unproved.
    bool,
);

/// A fully placed native operand and the evaluated layout its frame carries.
struct NativeFramePlan {
    plan: LogicalPlan,
    output: NativeType,
    tags: Vec<String>,
    kind: FieldKind,
    time: String,
    grid: Arc<EvalGrid>,
}

struct NativePlanned {
    kind: NativePlannedKind,
    unproved_keys: Option<&'static str>,
}

enum NativePlannedKind {
    Rows(LogicalPlan),
    Frame(NativeFramePlan),
}

fn native_planned(
    plan: LogicalPlan,
    metadata: Option<(NativeType, Vec<String>)>,
    kind: FieldKind,
    time: String,
    grid: Arc<EvalGrid>,
    unproved_keys: Option<&'static str>,
) -> NativePlanned {
    let kind = match metadata {
        Some((output, tags)) => NativePlannedKind::Frame(NativeFramePlan {
            plan,
            output,
            tags,
            kind,
            time,
            grid,
        }),
        None => NativePlannedKind::Rows(plan),
    };
    NativePlanned {
        kind,
        unproved_keys,
    }
}

/// The cause reported for a shape the route refuses without naming one.
const UNSUPPORTED_SHAPE_UNNAMED: &str = "cause_not_recorded";

/// The reason a statement's metadata drop is not proved: the scan that answers it keys its series by
/// a column that can hold `NULL` in one series and `''` in another, so the labels alone cannot prove
/// the drop tells two of its series apart. Nothing is collected for that pair, which is exactly what
/// this reports.
const UNPROVED_LABELSET_CHECK_NULLABLE_KEY: &str = "nullable_key";

/// The native route does not answer this shape, and the reason is the answer.
fn unsupported<T>(reason: &'static str) -> Result<T> {
    UnsupportedNativeShapeSnafu { reason }.fail()
}

/// Reports the shape the route does not answer, once, with the cause it named.
fn report_unsupported(error: &Error) {
    let Error::UnsupportedNativeShape { reason, .. } = error else {
        return;
    };
    debug!(
        "Native PromQL route does not answer this expression ({reason}); \
         the existing planner answers it"
    );
    crate::metrics::NATIVE_PROMQL_UNSUPPORTED_TOTAL
        .with_label_values(&[reason.as_str()])
        .inc();
}

impl PromPlanner {
    /// Builds the scan of the one logical table the current context is bound to. The caller has
    /// already preprocessed this selector's matchers and set the context to its table.
    #[allow(clippy::too_many_arguments)]
    async fn prepare_one_scan(
        &mut self,
        matchers: Matchers,
        under_rate_or_increase: bool,
        // The expression this statement asks for. Its shape, not the catalog type of a field, is what says
        // whether the existing plan reports a `Float64` for a numeric field the raw scan reads as one.
        statement_expr: &PromExpr,
        logical_scan_id: &mut Option<u32>,
        logical_ids_differ: &mut bool,
        statement_kind: &mut Option<FieldKind>,
    ) -> Result<PreparedInput> {
        let (Some(metric), Some(time)) = (
            self.ctx.table_name.clone(),
            self.ctx.time_index_column.clone(),
        ) else {
            return unsupported("selector_without_table_or_time_index");
        };
        let tags = self.ctx.tag_columns.clone();
        let fields = self.ctx.field_columns.clone();
        // A `__field__` matcher narrows the field set in a projection the existing planner builds, and the
        // raw series scan below this node has no equivalent: it reports every field the scan carries.
        if self.ctx.field_column_matcher.is_some() || fields.is_empty() {
            return unsupported("field_selector_without_raw_series_equivalent");
        }
        // Delta samples are increments: the existing planner sums them for `increase` and
        // divides by the range for `rate`, while the native tree assumes cumulative samples.
        // Refuse only when this selector is read by one of those calls.
        if tags
            .iter()
            .any(|tag| tag == OTLP_AGGREGATION_TEMPORALITY_LABEL)
            && under_rate_or_increase
        {
            return unsupported("delta_temporality_under_rate_or_increase");
        }
        // The selector's own table scan, built by the same method the existing path uses:
        // the physical planner turns it into the region scan the node reads, and building a
        // scan here instead would let the two paths disagree about the columns they read.
        let table_ref = self.table_ref()?;
        let mut input = self.create_table_scan_plan(table_ref).await?;
        let logical_scan = metric_scan_and_table_id(&input);
        let has_verified_metric_rewrite = logical_scan.is_some();
        // A metric logical table's id is part of the identity its own scan reads: two logical tables served
        // through one physical table prepare two scans of that one table, and the ids are what says they are
        // two metrics. A missing id on either side is not a difference.
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
                    return unsupported("field_missing_from_source_schema");
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
                _ => return unsupported("selector_plan_is_not_a_table_scan"),
            },
        };
        let table = self.table_from_source(&scan.source)?;
        let table_meta = &table.table_info().meta;
        // A table split along a partition column lives in more than one region. The canonical boundary's router
        // only collects the pieces its own producer holds, so the caller needs this fact to tell a whole-query
        // label-set answer from one region's share of it.
        let partitioned = table_meta.partition_column_names().next().is_some();
        // A kernel that drops the metadata labels drops every label whose value is empty with them,
        // so two stored series whose key columns hold `NULL` and `''` leave it as one identity. This
        // is the fact the drop's proof needs beside the labels - the same rule the physical
        // per-series scan reads to decide whether a node may stream - and the canonical pass
        // collects the drop when it does not hold.
        let keys_survive = keys_survive_metadata_drop(
            tags.iter().map(String::as_str),
            table_meta
                .primary_key_indices
                .iter()
                .map(|index| &table_meta.schema.column_schemas()[*index]),
        );
        // An engine without a raw-series scanner has no frame to give this node, and by the time the physical
        // planner asks, the logical plan is already this node: the refusal there is a planning error, not a
        // fallback. So the engine is checked here. A file-engine table is the concrete case, since its
        // `SinglePartitionScanner` leaves `raw_series_schema` at the default `None`. A metric-engine selector is
        // admitted through its verified logical rewrite.
        if !supports_native_raw_series(&table_meta.engine)
            || (table_meta.engine == METRIC_ENGINE_NAME && !has_verified_metric_rewrite)
        {
            return unsupported("engine_without_raw_series_scanner");
        }
        // The region's raw-series preflight refuses these, and the physical layer turns that
        // refusal into an error, so refuse this route here instead.
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
            return unsupported("unsupported_time_index_type_or_append_mode");
        }
        // A partition column that is not a tag splits one series across regions, and a bare scan cannot
        // reconcile the fragments; a raw source can, so such a selector is admitted and its input is bound per
        // source below.
        let split_series = !table_meta
            .partition_column_names()
            .all(|column| tags.iter().any(|tag| tag == column));
        // This route declares one output column per field, so every field it binds carries one kind:
        // `Float64`, or the canonical native histogram struct. A field of any other type - a decimal, a
        // string, a boolean - keeps the existing plan, and so does a statement whose fields disagree.
        //
        // Which kind a field carries is what the raw series scan hands the node, not the catalog type: the
        // scan casts every numeric field to `Float64`, so an integer or a `Float32` field it reads is a
        // float. The existing planner, on the other hand, publishes the field's own type for the shapes
        // that do not cast, so a float here is only the same answer where the statement's own shape says
        // the existing plan reports `Float64` too; [`read_field_type`] states that per field.
        //
        // A numeric field whose statement publishes the field's own type keeps the existing plan. Naming
        // that refusal here keeps it apart from a selection whose fields genuinely disagree, and it is the
        // refusal a reader can act on: the expression needs an operator that casts - an arithmetic one, a
        // range function, or an aggregate that answers a float.
        let mut keeps_its_own_type = false;
        let read_types = fields
            .iter()
            .map(|field| {
                Self::field_column_type(input.schema(), field).map(|data_type| {
                    keeps_its_own_type |= is_raw_numeric(data_type)
                        && !publishes_float_values(statement_expr, data_type);
                    read_field_type(data_type, statement_expr)
                })
            })
            .collect::<Vec<_>>();
        if keeps_its_own_type {
            // The existing plan publishes the field's own type for this shape - a bare selector copies the
            // column, `sum` keeps an integer's type, `count` builds an `Int64` - while a fused node
            // publishes `Float64` alone, so admitting it would change the answer's column type.
            return unsupported("field_keeps_its_own_type");
        }
        let Some(field_kind) =
            uniform_field_kind(read_types.iter().map(|data_type| data_type.as_ref()))
        else {
            return unsupported("mixed_field_kinds");
        };
        if statement_kind.is_some_and(|kind| kind != field_kind) {
            return unsupported("selectors_disagree_on_field_kind");
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
            return unsupported("selector_plan_is_not_a_table_scan");
        }
        Ok((
            input,
            series_schema,
            Some(Arc::from(metric)),
            time,
            tags,
            partitioned,
            split_series,
            keys_survive,
        ))
    }

    /// Offers `prom_expr` to the native route: the plan it answers, or the shape it does not answer.
    ///
    /// The caller offers only when the query asked for this route (see [`Self::native_fused`]). A
    /// shape the route does not answer is that error, and it is reported once (a log line and a
    /// counter).
    pub(super) async fn try_plan_native_fused(
        &mut self,
        prom_expr: &PromExpr,
        query_engine_state: &QueryEngineState,
    ) -> Result<LogicalPlan> {
        let planned = match self
            .try_plan_native(prom_expr, query_engine_state, FusedOutputFormat::PlainRows)
            .await
        {
            Ok(planned) => planned,
            Err(error @ Error::UnsupportedNativeShape { .. }) => {
                report_unsupported(&error);
                return Err(error);
            }
            Err(error) => return Err(error),
        };
        let Some(NativePlanned {
            kind: NativePlannedKind::Rows(plan),
            unproved_keys,
        }) = planned
        else {
            return unsupported(UNSUPPORTED_SHAPE_UNNAMED);
        };
        if let Some(reason) = unproved_keys {
            crate::metrics::DUPLICATE_LABELSET_CHECK_UNPROVED_TOTAL
                .with_label_values(&[reason])
                .inc();
        }
        Ok(plan)
    }

    /// The one native attempt at `prom_expr`, in the representation the caller needs from it.
    ///
    /// The whole expression is offered first: the compiler folds a range function and the math functions
    /// over it into one node. What it cannot bind that way - two selectors carrying different field kinds,
    /// or an operand that is itself a composition - is admitted as a composition of two frames instead.
    ///
    /// Both attempts write the planner's selector state, so this restores the context it was called with
    /// before returning whatever the attempt answered, which lets a refused operand leave nothing behind.
    ///
    /// `Box::pin` is here only because a composition and this attempt call each other, which an anonymous
    /// async future cannot be sized for.
    ///
    /// The boxed future is polled through [`GrownStack`], which is the one place this route's recursion
    /// grows its stack: a composition offers each operand here, so the descent alternates between this
    /// attempt and [`Self::try_plan_native_composed_binary`], and every one of those levels is charged to
    /// the stack of the level above it, below the reserve `PLANNER_STACK_RED_ZONE` names.
    fn try_plan_native<'a>(
        &'a mut self,
        prom_expr: &'a PromExpr,
        query_engine_state: &'a QueryEngineState,
        format: FusedOutputFormat,
    ) -> BoxFuture<'a, Result<Option<NativePlanned>>> {
        // An attempt descends the tree it was offered once per operator, and the composition below
        // offers each operand in turn, so the recursion grows its stack here as well as at the
        // planner's own descent: `GrownStack` grows it for the whole poll of this future, which
        // covers every nested attempt the poll reaches.
        Box::pin(GrownStack::new(async move {
            let saved = self.ctx.clone();
            let whole = Box::pin(self.try_plan_native_fused_with_context(
                prom_expr,
                query_engine_state,
                format,
            ))
            .await;
            self.ctx = saved.clone();
            let whole_unsupported = match whole {
                Ok(planned) => return Ok(Some(planned)),
                Err(error @ Error::UnsupportedNativeShape { .. }) => Some(error),
                Err(error) => return Err(error),
            };
            let composed = self
                .try_plan_native_composed_binary(prom_expr, query_engine_state, format)
                .await;
            self.ctx = saved;
            match composed {
                Ok(None) => match whole_unsupported {
                    Some(error) => Err(error),
                    None => Ok(None),
                },
                answer => answer,
            }
        }))
    }

    /// Combines two native operands without a legacy join.
    ///
    /// Each operand is planned the way the whole expression is and bound as one evaluated series frame.
    /// What admits the pair is the composition itself: the two grids must agree, and the matching, its
    /// cardinality, its fill values and the output column's kind must all be ones the kernels answer. A
    /// shape they refuse refuses with the cause it records, which the offer's caller reports.
    ///
    /// Only a binary expression composes, and only its own two operands are offered below.
    async fn try_plan_native_composed_binary(
        &mut self,
        expr: &PromExpr,
        query_engine_state: &QueryEngineState,
        format: FusedOutputFormat,
    ) -> Result<Option<NativePlanned>> {
        // Neither check refuses a shape: they say this second attempt does not apply here, so they
        // record no cause - the whole-expression attempt above already named the one that decided
        // the answer.
        if !self.native_fused
            || self.ctx.start != self.ctx.stmt_start
            || self.ctx.end != self.ctx.stmt_end
        {
            return Ok(None);
        }
        // Parentheses are not a shape of their own: the operand inside them is the one that
        // composes.
        let mut inner = expr;
        while let PromExpr::Paren(paren) = inner {
            inner = &paren.expr;
        }
        let PromExpr::Binary(binary) = inner else {
            return Ok(None);
        };
        // Each attempt restores context even when a scan or compiler fails, and the second operand
        // starts from the same context the first one did.
        let saved = self.ctx.clone();
        let left = self
            .try_plan_native(
                &binary.lhs,
                query_engine_state,
                FusedOutputFormat::SeriesFrame,
            )
            .await;
        self.ctx = saved.clone();
        let (left, left_unproved) = match left {
            Ok(Some(planned)) => match planned.kind {
                NativePlannedKind::Frame(frame) => (frame, planned.unproved_keys),
                _ => return unsupported("left_operand_is_not_a_frame"),
            },
            Ok(None) | Err(Error::UnsupportedNativeShape { .. }) => {
                return unsupported("left_operand_is_not_a_frame");
            }
            Err(error) => return Err(error),
        };
        let right = self
            .try_plan_native(
                &binary.rhs,
                query_engine_state,
                FusedOutputFormat::SeriesFrame,
            )
            .await;
        self.ctx = saved;
        let (right, right_unproved) = match right {
            Ok(Some(planned)) => match planned.kind {
                NativePlannedKind::Frame(frame) => (frame, planned.unproved_keys),
                _ => return unsupported("right_operand_is_not_a_frame"),
            },
            Ok(None) | Err(Error::UnsupportedNativeShape { .. }) => {
                return unsupported("right_operand_is_not_a_frame");
            }
            Err(error) => return Err(error),
        };
        if left.grid.spec() != right.grid.spec() {
            return unsupported("operand_grids_differ");
        }
        let NativeFramePlan {
            plan: left_plan,
            output: left_output,
            tags: left_tags,
            kind: left_kind,
            time: left_time,
            grid: left_grid,
        } = left;
        let NativeFramePlan {
            plan: right_plan,
            output: right_output,
            tags: right_tags,
            kind: right_kind,
            ..
        } = right;
        let source_tags = [left_tags, right_tags];
        let evaluated = |source_index, output: &NativeType| NativeNode {
            expr: NativeExpr::Input(BoundInput {
                source_index,
                output: output.clone(),
                ordering: SeriesOrdering::Contiguous,
            }),
            output: output.clone(),
        };
        let tree = match bind_evaluated_binary(
            binary,
            evaluated(0, &left_output),
            evaluated(1, &right_output),
        ) {
            Ok(tree) => tree,
            Err(DataFusionError::NotImplemented(_)) => {
                return unsupported("no_kernel_for_the_operator_pair");
            }
            Err(error) => return Err(error).context(DataFusionPlanningSnafu),
        };
        let NativeExpr::Binary { matching, .. } = &tree.root().expr else {
            return unsupported("bound_root_is_not_the_binary");
        };
        let Some(root_kind) = binary_output_field_kind(
            left_kind,
            right_kind,
            matching.op,
            matching.return_bool,
            matching.fill,
        ) else {
            return UnsupportedExprSnafu {
                name: "PromQL fill modifiers",
            }
            .fail();
        };
        if !native_tree_supported(&tree) {
            return unsupported("runtime_refuses_the_composed_tree");
        }
        // The expression names the value columns, and it is the label the node reports.
        let expression = expr.to_string();
        // What a consumer of this composition reads, and the layout the node publishes. The row
        // layout names one column per value; the frame layout is the evaluated frame this tree's
        // own root declares, which is what a frame consumer decodes.
        let metadata = match format {
            FusedOutputFormat::PlainRows => None,
            FusedOutputFormat::SeriesFrame => {
                let Some(tags) = fused_output_tags(tree.root(), &source_tags) else {
                    return unsupported("composition_has_no_output_labels");
                };
                Some((tree.root().output.clone(), tags))
            }
        };
        let output_schema = match format {
            FusedOutputFormat::PlainRows => {
                let Some(schema) = Self::fused_root_schema(
                    &expression,
                    &tree,
                    &source_tags,
                    &left_time,
                    root_kind,
                )?
                else {
                    return unsupported("composition_has_no_value_columns");
                };
                schema
            }
            FusedOutputFormat::SeriesFrame => {
                let frame = match FusedSeries::series_frame_schema(&tree) {
                    Ok(frame) => frame,
                    // A frame this composition cannot declare is a shape it does not answer.
                    Err(DataFusionError::Plan(_)) => {
                        return unsupported("composition_cannot_declare_a_frame");
                    }
                    Err(error) => return Err(error).context(DataFusionPlanningSnafu),
                };
                Arc::new(
                    DFSchema::try_from(frame.as_ref().clone()).context(DataFusionPlanningSnafu)?,
                )
            }
        };
        let Ok(node) = FusedSeries::try_new_with_format(
            Arc::from(expression.as_str()),
            tree,
            FusedSeriesBinding {
                grid: Arc::clone(&left_grid),
                time_index: left_time.clone(),
                tags: Vec::new(),
                metric_name: None,
            },
            vec![left_plan, right_plan],
            output_schema,
            format,
        ) else {
            return unsupported("fused_node_refuses_the_composition");
        };
        let plan = LogicalPlan::Extension(Extension {
            node: Arc::new(node),
        });
        Ok(Some(native_planned(
            plan,
            metadata,
            root_kind,
            left_time,
            left_grid,
            left_unproved.or(right_unproved),
        )))
    }

    /// Plans `prom_expr` as one native node, or answers why this shape is not one.
    ///
    /// The expression names the value columns; the compiler's bound tree and selector schemas
    /// determine their values. The existing planner is not consulted for capability or schema approval.
    ///
    /// The route is offered a whole expression because the compiler folds a range function and the math
    /// functions over it into one tree: offering a sub-expression would compile a tree that computes
    /// something the reader did not write. The node's table, time index, tag and field columns come from
    /// the planner's resolved selector context, which is the raw series schema the tree's input is
    /// compiled against. Selectors with a `__field__` matcher keep legacy planning.
    ///
    /// A shape this route does not answer is that error; the offer's caller reports it rather than
    /// planning the query some other way (see [`PromPlanner::try_plan_native_fused`]).
    async fn try_plan_native_fused_with_context(
        &mut self,
        prom_expr: &PromExpr,
        query_engine_state: &QueryEngineState,
        format: FusedOutputFormat,
    ) -> Result<NativePlanned> {
        if !self.native_fused {
            return unsupported("native_route_not_requested");
        }
        // The expression names the value columns of every rows layout this attempt publishes, and the
        // node's own label follows the same text. It is the whole statement's expression even when a
        // subquery root compiles its inner one, because that is the statement the reader wrote.
        let expression = prom_expr.to_string();
        // A range selector returns raw samples, which have no evaluation grid to bind. A subquery root
        // instead returns its inner expression on the subquery's child grid, and a range query has no grid
        // to bind a matrix root to.
        let subquery_root = root_subquery(prom_expr);
        if prom_expr.value_type() == ValueType::Matrix
            && (subquery_root.is_none() || self.ctx.start != self.ctx.end)
        {
            return unsupported("matrix_root_without_a_grid");
        }

        let Some(stmt) = self.native_statement(prom_expr) else {
            return unsupported("statement_has_no_evaluation_grid");
        };
        // The subquery root's inner expression is compiled on the child grid, with the statement left
        // untouched: `@ start()` and `@ end()` inside it resolve against the statement's bounds. A statement
        // whose grid cannot be built keeps the existing plan.
        let child_grid = if let Some(subquery) = subquery_root {
            let Ok(outer) = statement_grid(&stmt) else {
                return unsupported("subquery_outer_grid_unavailable");
            };
            let Some(child) =
                subquery_child_grid(&stmt, &outer, subquery).context(DataFusionPlanningSnafu)?
            else {
                return unsupported("subquery_has_no_child_grid");
            };
            Some(child.grid)
        } else {
            None
        };
        let effective_expr = match subquery_root {
            Some(subquery) => subquery.expr.as_ref(),
            None => &stmt.expr,
        };
        let mut selectors = Vec::new();
        // The walk starts outside any `rate` or `increase`, and records that per selector as it
        // descends: a selector is delta-temporality-sensitive only where one of those calls reads
        // it, which is what keeps `rate(a) / b` admissible when only `b` carries the marker.
        let mut set_operations = Vec::new();
        if !collect_selectors_with_set_operations(
            &stmt,
            effective_expr,
            false,
            &mut selectors,
            &mut set_operations,
        ) {
            return unsupported("selector_walk_refuses_the_statement");
        }
        // The compiler decides which calls and shapes this route answers: a name it has no kernel for comes
        // back as `NotImplemented`, which is a refusal below.
        let Ok(statement) = statement_grid(&stmt) else {
            return unsupported("statement_grid_unavailable");
        };
        // The one grid every lowering path below binds: a subquery root is compiled on its child
        // grid, and the rows it publishes carry that grid, so the split, the boundary and the
        // segment nodes have to publish on it too rather than on the statement's.
        let grid = child_grid.unwrap_or(statement);
        if selectors.is_empty() {
            let mut compile_stmt = stmt.clone();
            compile_stmt.expr = effective_expr.clone();
            let tree = match compile_statement_with_grid(&compile_stmt, Arc::clone(&grid)) {
                Ok(tree) => tree,
                Err(DataFusionError::NotImplemented(_)) => {
                    return unsupported("no_kernel_for_the_statement_shape");
                }
                Err(error) => return Err(error).context(DataFusionPlanningSnafu),
            };
            // A frame operand is an evaluated sample root: a composition matches series, and a
            // scalar root - `2`, `time()` - carries one value and no labels, so those keep the
            // existing plan rather than becoming a frame the matcher cannot pair.
            let evaluated = matches!(
                &tree.root().output,
                NativeType::Samples(schema) if schema.eval_grid.is_some()
            );
            if format == FusedOutputFormat::SeriesFrame && !evaluated {
                return unsupported("frame_operand_is_not_evaluated");
            }
            // Bind only input tags: this data-free tree has no input, and its tags belong to its
            // output schema.
            let Some(output_tags) = fused_output_tags(tree.root(), &[]) else {
                return unsupported("data_free_root_has_no_labels");
            };

            let Some(values) = native_root_fields(tree.root()) else {
                return unsupported("data_free_root_has_no_value_columns");
            };
            let value_names = expression_field_names(&expression, &values);
            let time_index = free_time_index(DEFAULT_TIME_INDEX_COLUMN, &value_names, &output_tags);
            // The metadata a consumer of a frame operand reads: the evaluated layout this tree's
            // root declares on the statement grid, and the labels a `vector(s)` reports.
            let metadata = match format {
                FusedOutputFormat::PlainRows => None,
                FusedOutputFormat::SeriesFrame => {
                    Some((tree.root().output.clone(), output_tags.clone()))
                }
            };
            // A data-free tree reads no input, so every field it answers is a float.
            let output_schema = match format {
                FusedOutputFormat::PlainRows => fused_output_schema(
                    &expression,
                    &values,
                    &with_row_name_column(tree.root(), output_tags.clone()),
                    &time_index,
                    FieldKind::Float,
                )?,
                FusedOutputFormat::SeriesFrame => {
                    let frame =
                        FusedSeries::series_frame_schema(&tree).context(DataFusionPlanningSnafu)?;
                    Arc::new(
                        DFSchema::try_from(frame.as_ref().clone())
                            .context(DataFusionPlanningSnafu)?,
                    )
                }
            };
            let node = FusedSeries::try_new_with_format(
                Arc::from(expression.as_str()),
                tree,
                FusedSeriesBinding {
                    grid: Arc::clone(&grid),
                    time_index: time_index.clone(),
                    tags: Vec::new(),
                    metric_name: None,
                },
                Vec::new(),
                output_schema,
                format,
            )
            .context(DataFusionPlanningSnafu)?;
            let plan = LogicalPlan::Extension(Extension {
                node: Arc::new(node),
            });
            return Ok(native_planned(
                plan,
                metadata,
                FieldKind::Float,
                time_index,
                grid,
                None,
            ));
        }
        let original_context = self.ctx.clone();
        let preparation = async {
            let mut prepared: Vec<PreparedInput> = Vec::new();
            // The first metric logical-table id a selector's rewrite carried, and whether a later one
            // carried a different id. See the `cross_table` use below.
            let mut logical_scan_id: Option<u32> = None;
            let mut logical_ids_differ = false;
            // The kind every bound field carries. A statement declares one column per field, so
            // selectors that disagree have no column to declare and keep the existing plan.
            let mut statement_kind: Option<FieldKind> = None;
            for (selector, under_rate_or_increase) in &selectors {
                self.ctx = original_context.clone();
                // These matchers are alternatives the one scan below the node cannot represent, so the
                // query keeps the plan the existing path builds for it.
                if !selector.matchers.or_matchers.is_empty() {
                    return unsupported("or_matchers_have_no_single_raw_scan");
                }
                // A selector that names no metric is answered by enumerating the schema's logical metric
                // tables: one input per metric, each reading its own region, so every series keeps the
                // identity the expression itself cannot name.
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
                        Err(_) => return unsupported("discovery_matchers_cannot_be_preprocessed"),
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
                        return unsupported("metric_discovery_unavailable");
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
                        // Nothing matches the selector, so the query still has an answer: the empty vector.
                        // Planning it here keeps a nameless selector that matches nothing from reaching the
                        // existing planner, which has no table to bind and raises `TableNameNotFound`.
                        //
                        // An empty raw series frame is an ordinary input to the node; multi-input placement
                        // and partition normalization are decided after compilation. A `__field__` matcher
                        // decides which fields that frame declares: it is the one place the field layout of
                        // this input is stated, and no table matched to state it otherwise. A matcher whose
                        // answer is a set over a table's field columns has none to read here, so that
                        // selection keeps the plan the existing planner would have built for it.
                        let Some(fields) =
                            empty_discovered_fields(self.ctx.field_column_matcher.as_deref())
                        else {
                            return unsupported("field_matcher_has_no_empty_layout");
                        };
                        prepared.push(empty_discovered_input(
                            self.ctx
                                .time_index_column
                                .clone()
                                .unwrap_or_else(|| DEFAULT_TIME_INDEX_COLUMN.to_string()),
                            empty_discovered_series_schema(fields),
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
                    let mut discovered_split_series = false;
                    // Every discovered metric's own key columns decide its series, so the union
                    // survives the drop only when all of them do - and a metric whose keys do not
                    // is enough to collect the whole input.
                    let mut discovered_keys_survive = true;
                    for table in metrics {
                        self.ctx.table_name = Some(table.table_info().name.clone());
                        if self.setup_context().await?.is_some() {
                            return unsupported("discovered_table_has_no_data");
                        }
                        let (
                            scan,
                            schema,
                            metric,
                            time,
                            mut tags,
                            partitioned,
                            split_series,
                            keys_survive,
                        ) = self
                            .prepare_one_scan(
                                matchers.clone(),
                                *under_rate_or_increase,
                                &stmt.expr,
                                &mut logical_scan_id,
                                &mut logical_ids_differ,
                                &mut statement_kind,
                            )
                            .await?;
                        discovered_partitioned |= partitioned;
                        discovered_split_series |= split_series;
                        discovered_keys_survive &= keys_survive;
                        // The union reads every discovered metric, so it is region-local only when
                        // every metric it reads is.
                        let Some(metric) = metric else {
                            return unsupported("discovered_selector_names_no_metric");
                        };
                        // The expression names no metric, so each source reports its own: the tag is
                        // what tells two metrics with equal ordinary labels apart, and it is what a
                        // grouping by the name groups on. It is part of the series' identity rather
                        // than of how a result is displayed, so every plan declares it.
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
                        return unsupported("discovered_selectors_disagree_on_their_layout");
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
                    prepared.push((
                        union,
                        schema,
                        metric,
                        time,
                        tags,
                        discovered_partitioned,
                        discovered_split_series,
                        discovered_keys_survive,
                    ));
                    continue;
                }
                let matchers =
                    match self.preprocess_label_matchers(&selector.matchers, &selector.name) {
                        Ok(m) => m,
                        Err(_) => return unsupported("selector_matchers_cannot_be_preprocessed"),
                    };
                if self.setup_context().await?.is_some() {
                    // The selector names a metric whose table does not exist, so the query still has an
                    // answer: the empty vector, planned from the same typed empty input a nameless selector
                    // that matched no table is. Planning it here keeps `TableNameNotFound` out of the
                    // existing planner, which has no table to bind.
                    //
                    // A `__field__` matcher does not refuse here, though one over a scan this route can
                    // read does: it narrows the field set of the table its selector reads, and this table has
                    // no field set to narrow, so every field selection over it is empty. The existing planner
                    // answers the same way, because a missing table is planned as `EmptyMetric` before the
                    // projection a `__field__` matcher builds (`setup_context` returns it, and
                    // `selector_to_series_normalize_plan` never runs). The empty input's own placeholder
                    // field is internal to the tree; no answer publishes it.
                    prepared.push(empty_discovered_input(
                        self.ctx
                            .time_index_column
                            .clone()
                            .unwrap_or_else(|| DEFAULT_TIME_INDEX_COLUMN.to_string()),
                        empty_discovered_series_schema(vec![DEFAULT_FIELD_COLUMN.to_string()]),
                    )?);
                    continue;
                }
                let one = self
                    .prepare_one_scan(
                        matchers,
                        *under_rate_or_increase,
                        &stmt.expr,
                        &mut logical_scan_id,
                        &mut logical_ids_differ,
                        &mut statement_kind,
                    )
                    .await?;
                prepared.push(one);
            }
            Ok((prepared, logical_ids_differ, statement_kind))
        }
        .await;
        self.ctx = original_context;
        let (mut prepared, logical_ids_differ, statement_kind) = preparation?;
        // `absent_over_time` reads its window through a fold that keeps the metric name, which the fused
        // node hands over in a frame's labels. Only the bare-scan arm of that node needs the binding's own
        // name: it asks a region scan for the series of one metric, and that is exactly the shape with a
        // name to ask for. A nameless selector that discovery expands into several metric tables never
        // reaches that arm, because its prepared input is the `Union` of one `RawSeriesSource` per
        // discovered metric, and each source names its own metric, so the node is handed explicit-label
        // frames instead. So the case refused here is a nameless input that is *not* sourced, which is the
        // one shape where a tree keeping `__name__` would meet a scan that has no name to bind.
        //
        // A selector that matched no table is not that shape either: its typed empty input holds no series.
        if reads_presence(prom_expr)
            && prepared.iter().any(|(scan, _, metric, ..)| {
                !is_empty_discovered_input(scan) && metric.is_none() && !is_sourced_plan(scan)
            })
        {
            return unsupported("presence_over_a_nameless_selector");
        }
        // Selectors over one table share one region, so the node runs there over every scan. When they read
        // more than one table, each scan answers from its own region and the node runs above them; see
        // `RawSeriesSource`. A metric logical table's id is part of the identity it reads: two ids are two
        // metrics even when one physical table holds both.
        //
        // The typed empty input of a selector that matched no table reads no region and counts as no
        // second table.
        let cross_table = !same_table_scans(
            prepared
                .iter()
                .map(|(scan, ..)| scan)
                .filter(|scan| !is_empty_discovered_input(scan)),
        ) || logical_ids_differ;
        // A multi-selector tree over a partitioned table has no region-local placement, but it does have a
        // frontend one - each occurrence reads the table through its own raw source and the node matches
        // above every region's contribution - so the tree is admitted on both routes.
        //
        // An empty selector has no table layout of its own: the set operator pairs its two sides' fields by
        // position, so an operand that reads no real input is laid out by the first real selector in the
        // other operand. That layout reaches every empty selector of the operand, nested set operations
        // included; a layout taken from the statement's first real input is not that sibling's and can
        // carry a different number of fields. With no such operand above it, an empty selector takes the
        // statement's first real layout, and the default layout of [`empty_discovered_input`] when every
        // selector is empty.
        let local_layouts = local_empty_layouts(&set_operations, &prepared);
        let statement_layout = prepared
            .iter()
            .find(|(scan, ..)| !is_empty_discovered_input(scan))
            .map(|(_, schema, ..)| Arc::clone(schema));
        for index in 0..prepared.len() {
            if !is_empty_discovered_input(&prepared[index].0) {
                continue;
            }
            let Some(schema) = local_layouts[index]
                .map(|sibling| Arc::clone(&prepared[sibling].1))
                .or_else(|| statement_layout.as_ref().map(Arc::clone))
            else {
                continue;
            };
            prepared[index].0 = empty_discovered_frame(&schema)?;
            prepared[index].1 = Arc::clone(&schema);
        }
        // A real input names the output's time index and, on one table, binds every scan.
        let named_by_a_real_input = prepared
            .iter()
            .any(|(scan, ..)| !is_empty_discovered_input(scan));
        let Some((_, _, metric, time, tags, _, _, _)) = prepared
            .iter()
            .find(|(scan, ..)| !is_empty_discovered_input(scan))
            .or_else(|| prepared.first())
        else {
            return unsupported("statement_prepared_no_readable_input");
        };
        let (metric, time, tags) = (metric.clone(), time.clone(), tags.clone());
        // The compiler binds one input per selector in the same pre-order `collect_selectors`
        // walks, so position i of both lists is the same occurrence. This is the guard that the
        // two walks agree: a mismatch is a compiler defect, not a query this route can plan.
        let identities: Vec<(Option<String>, Matchers, i64, Option<i64>)> = selectors
            .iter()
            .map(|(selector, _)| {
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
        let mut compile_stmt = stmt.clone();
        compile_stmt.expr = effective_expr.clone();
        let compiled = match compile_selectors_with_grid(
            &compile_stmt,
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
            Arc::clone(&grid),
            // The kind the prepared scans report for the statement's fields. An aggregate binds the
            // state its input's kind implies, so a `sum` over a native-histogram column has to be
            // compiled with that kind: bound as a float it would fold the payload as numbers. A
            // statement that reads no table reports `None` here, which is the data-free case.
            statement_kind,
        ) {
            Ok(v) => v,
            // The compiler refusing a shape is this route refusing it.
            Err(DataFusionError::NotImplemented(_)) => {
                return unsupported("no_kernel_for_the_statement_shape");
            }
            Err(e) => return Err(e).context(DataFusionPlanningSnafu),
        };
        if compiled.inputs.len() != prepared.len() {
            return Err(DataFusionError::Internal(
                "the compiled tree reads a different number of inputs than the planner prepared"
                    .into(),
            ))
            .context(DataFusionPlanningSnafu);
        }
        for (index, (compiled_input, (scan, _, _, time, _, _, _, _))) in
            compiled.inputs.iter().zip(prepared.iter_mut()).enumerate()
        {
            if compiled_input.source_index != index {
                return unsupported("compiled_sources_out_of_order");
            }
            // The range this selector reads is the compiler's own: a window and an instant differ in which
            // side of the lookback or range their scan starts on, and an anchor can move the whole range
            // outside the statement's. Two occurrences over different windows or offsets read different
            // rows even when nothing else about them differs.
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
        // The node reads more than one input when the tree has more than one selector occurrence: the
        // compiler binds one input per occurrence. Each is bound to its own source where the plan is split
        // over regions, or to one bare scan per occurrence where a region can run the node itself.
        let multi_input = selectors.len() > 1;
        // The tag columns each input declares, which is what the output layout is derived from.
        //
        // A tree over more than one input cannot be told apart by those columns alone: two occurrences
        // bound to two metrics agree on every one of them, so a grouping by the metric name would fold
        // both metrics into one group and publish two rows under one label set. `__name__` is not a
        // column of any scan - it is the name every series of a metric-bound scan carries - so a
        // statement reading several inputs declares it among each one's tags, exactly as a discovered
        // source does, and the grouping keeps it as a group label. A statement reading one input binds
        // one metric, whose name cannot tell two of its own groups apart; there the column is only
        // appended by [`with_row_name_column`].
        let source_tags = prepared
            .iter()
            .map(|(_, _, metric, _, tags, _, _, _)| {
                let mut tags = tags.clone();
                if multi_input && metric.is_some() && !tags.iter().any(|tag| tag == METRIC_NAME) {
                    tags.push(METRIC_NAME.to_string());
                }
                tags
            })
            .collect::<Vec<_>>();
        // The complete runtime label set of each input: a scan bound to one metric names every series it
        // hands over, so `__name__` is one of its labels even though it is not a tag column. Rewriting the
        // name of a named scan is therefore a rewrite of an existing label, and a discovered source
        // declares the name among its tags itself. Unlike [`source_tags`] this is the runtime answer for
        // every input, whatever the statement's shape.
        let runtime_source_tags = prepared
            .iter()
            .map(|(_, _, metric, _, tags, _, _, _)| {
                let mut tags = tags.clone();
                if metric.is_some() && !tags.iter().any(|tag| tag == METRIC_NAME) {
                    tags.push(METRIC_NAME.to_string());
                }
                tags
            })
            .collect::<Vec<_>>();
        // The other half of what the canonical pass judges a dropped label by: the one metric each
        // input is bound to. A scan of one table names every series it hands over, so `__name__` is
        // the same for all of them and dropping it tells none of them apart; a discovered selector
        // that expands into several metrics binds no single name, which is the fact that keeps
        // `rate` over it collected.
        let runtime_source_metrics = prepared
            .iter()
            .map(|(_, _, metric, _, _, _, _, _)| metric.clone())
            .collect::<Vec<_>>();
        // The pair the label-set check leaves outside itself, and the one no tree can answer about
        // itself: whether every input keys its series by columns that always decode a value. One
        // input that cannot say so - a nullable primary-key column, or a discovered selector whose
        // metrics disagree - can spell `NULL` in one series and `''` in another, which a metadata
        // drop normalizes into one label set. Nothing is collected for it any more, so the count
        // below is what keeps those statements observable.
        let keys_survive = prepared.iter().all(|input| input.7);
        // Whether any input's table is split into regions along a partition column. Only the canonical
        // boundary claims a whole-query label-set answer, and a region-local producer cannot see the pieces
        // another region holds.
        let any_partitioned = prepared.iter().any(|input| input.5);
        // Admission and placement are separate: `independent_scan_pair` admits nested native matches, while
        // the physical locality classification decides whether their inputs need gathering or segmentation.
        // The pass puts a [`NativeExpr::Merge`] where a label rewrite can give two series one label set, so
        // a tree with no collapsing rewrite keeps every partition.
        let tree = match promql::native::canonicalize::merge_label_rewrites(
            &compiled.tree,
            &runtime_source_tags,
            &runtime_source_metrics,
        ) {
            Ok(tree) => tree,
            Err(_) => return unsupported("label_rewrites_do_not_merge"),
        };
        // What the pair those collections used to be paid for can still hide: a node that drops the
        // metadata labels is proved by its labels, and the scan's key columns are the one fact they
        // cannot state - a primary-key column that can spell `NULL` in one series and `''` in
        // another. Counting the statements that fact leaves unproved is what keeps it observable.
        let unproved_keys = promql::native::plan::leaves_a_drop_to_the_storage_keys(
            &tree,
            &runtime_source_tags,
            &runtime_source_metrics,
            keys_survive,
        )
        .then_some(UNPROVED_LABELSET_CHECK_NULLABLE_KEY);
        let root = tree.root();
        // The placement facts the split and the segmentation below are chosen from, and the same
        // facts that say whether one node can run over the scans at all.
        let per_partition = region_local_segment(root);
        let locality = input_locality(root);
        let computed_key_case =
            matches!(locality, InputLocality::SinglePartition) && matching_operands_drop_key(root);
        let required_placement = !per_partition || computed_key_case;
        // A region runs one node over its own scans only when the table it reads is not split into
        // regions and the tree's root is per-partition work: a partitioned table's region reads
        // neither the other scans nor the fragments another region holds, and a whole-vector
        // consumer has no region-local placement. Such a tree runs above the regions, and then each
        // occurrence reads through its own raw source. An empty frame likewise requires sourced
        // scans: physical planning accepts either all frames or all bare scans, not a mixture.
        // With a distributed planner every read is planned above the regions, so each occurrence
        // reads through its own source.
        //
        // A canonical merge is the other shape that has to read through a source, and the reason
        // does not depend on how many occurrences the statement has: the boundary's producer runs
        // above the regions, so a bare region scan would give it neither a raw-series frame nor the
        // pieces another region holds - and physical planning refuses that node after the statement
        // has already committed to this route. Reading each occurrence through its own
        // `RawSeriesSource` puts the producer on the frontend instead, above every region, with its
        // canonical router above all of them; so a partitioned table whose tree holds a canonical
        // merge is planned that way whatever its selector count.
        let spans_regions = query_engine_state.with_dist_planner();
        let canonical_merge = contains_canonical_merge(root);
        let per_source_inputs = cross_table
            || spans_regions
            || (any_partitioned && canonical_merge)
            || (multi_input
                && (any_partitioned
                    || required_placement
                    || prepared
                        .iter()
                        .any(|(scan, ..)| is_empty_discovered_input(scan))));
        // A segment carries one source's identity. Keeping each occurrence's scan and metadata
        // aligned here lets the cutting step name that source independently of selector count.
        // Read the split-series fact before `prepared` is consumed by the child mapping below.
        let any_split_series = prepared.iter().any(|input| input.6);
        let bare_scans = prepared
            .iter()
            .map(|(scan, ..)| scan.clone())
            .collect::<Vec<_>>();
        let per_source = prepared
            .iter()
            .map(|(_, _, metric, time_index, tags, _, _, _)| {
                (metric.clone(), time_index.clone(), tags.clone())
            })
            .collect::<Vec<_>>();
        let children = prepared
            .into_iter()
            .map(|(scan, schema, metric, _, tags, _, split_series, _)| {
                // The typed empty frame of a selector that matched no table is already the input: it reads
                // no region and has no metric to bind, so it stays itself on both routes.
                if is_empty_discovered_input(&scan) {
                    return Ok(scan);
                }
                // A scan stays bare only where a region runs the node over it. A plan split over regions
                // reads every input through a raw source: the source names each region's half and
                // reconciles the fragments of a series several regions hold. A partition that splits one
                // series across regions has to be read through a raw source even where no split is
                // planned, since a bare scan would hand a region only its own fragment. A selector that
                // already carries its own sources stays as it is; see [`is_sourced_plan`].
                if (!per_source_inputs && !split_series) || is_sourced_plan(&scan) {
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
        // What a canonical boundary needs from its children is a raw-series frame, which is what
        // [`per_source_inputs`] decided above: a boundary over a partitioned table reads each
        // occurrence through its own `RawSeriesSource`, so its producer runs on the frontend above
        // every region with its canonical router above all of them, and each label set is collected
        // from every region's contribution - a collision across regions is reported as Prometheus
        // reports it.
        //
        // No shape is left to refuse here: a canonical boundary over a partitioned table reads every
        // occurrence through its own raw source, which is the frame physical planning converts, so
        // the producer runs on the frontend with its canonical router above every region's
        // contribution. A region could not route one label set on its own, and it is never asked to.
        //
        // A tree with no canonical merge claims no label-set answer, so it is untouched:
        // a per-series query on a partitioned table keeps fusing over the bare scans it read before.
        //
        // The kind the tree's root answers with, which every output column follows. It is not the kind the
        // inputs carry: `count_over_time`, `histogram_count` and `timestamp` reduce a histogram to a
        // float, while a selector, a histogram-answering range function and a label rewrite pass it on.
        // A canonical merge hands on the samples it was given, so an answer kind crosses it unchanged:
        // a histogram answer keeps its frames over the boundary, and the router merges them with the
        // same kernel it merges floats with.
        // A shape whose kind cannot be named keeps the existing plan rather than declaring a column the
        // first batch might not fill.
        let root_kind = match statement_kind {
            Some(FieldKind::Histogram) => {
                match root.output_field_kind(Some(FieldKind::Histogram)) {
                    Some(root_kind) => root_kind,
                    None => return unsupported("root_declares_no_field_kind"),
                }
            }
            Some(FieldKind::Float) | None => FieldKind::Float,
        };
        let frame_metadata = if format == FusedOutputFormat::SeriesFrame {
            let NativeType::Samples(schema) = &root.output else {
                return unsupported("frame_operand_is_not_samples");
            };
            if schema.eval_grid.is_none() {
                return unsupported("frame_operand_has_no_grid");
            }
            // A segment publishes frames, so it reports no name column of its own.
            let Some(output_tags) = fused_output_tags(root, &source_tags) else {
                return unsupported("root_has_no_output_labels");
            };
            Some((root.output.clone(), output_tags))
        } else {
            None
        };
        // A tree over more than one input needs no split: each input already answers from its own source,
        // and the one node above them folds every partition in one execution. The aggregate split folds
        // float state, and a node reading several sources runs over frames it reconciled rather than over
        // its own scans, so a histogram keeps the existing plan for both. The kind entering each
        // aggregate is what decides, not the shape of the root: `sum(h) * 2` has a binary root.
        if let Some(kind @ FieldKind::Histogram) = statement_kind
            && ((multi_input && per_source_inputs) || aggregate_reads_histogram(root, kind))
        {
            return unsupported("histogram_aggregate_needs_state_this_route_lacks");
        }
        // The split keeps its prefix per-series, which is why it refuses an operation whose
        // answer depends on the order its input arrives in. A node over more than one input reads
        // its inputs' partitions in the order they became ready, so that refusal holds there too.
        if multi_input && depends_on_input_order(root) {
            return unsupported("answer_depends_on_input_order");
        }
        // The native output schema can name only compiled value fields, the time index and the tag
        // columns a source declares, and `source_tags` declares the metric name of every occurrence of
        // a statement reading more than one input - the label a grouping by `__name__` keeps, so that
        // the groups two metrics form stay apart. A discovered source declares `__name__` because the
        // name tells two metrics with equal labels apart (#10106); a named source needs the column for
        // the same reason, so it is declared rather than refused. A statement reading one input binds
        // one metric, whose grouping by that name cannot separate two rows it names alike; there the
        // column is the appended one, and no shape needs refusing.
        //
        // Catch the shape refusals before choosing native execution: errors raised during physical
        // planning cannot fall back. This includes unsupported aggregates and `topk`, `limitk` or
        // CountValues groupings; it does not cover data-dependent errors raised while executing the
        // tree. A plan split over regions commits to the fused node without the split or a segment
        // above it, so it checks itself the same way before that choice.
        if (multi_input || per_source_inputs) && !native_tree_supported(&tree) {
            return unsupported("runtime_refuses_the_tree");
        }
        // A tree that ends in a canonical merge answers one series per label set, and the pieces of one
        // set were spread over the scan's partitions by the rewrite that created them. Lowering that
        // boundary keeps the scan width: each partition merges the pieces it holds and a canonical
        // router collects a label set from every partition.
        if selectors.len() == 1
            && matches!(&tree.root().expr, NativeExpr::Merge { input } if pointwise_operand(input) && canonical_local_prefix(input) && !contains_global_consumer(input))
            && let Some((plan, _)) = canonical_boundary_stages(
                Arc::from(expression.as_str()),
                &tree,
                &children,
                &source_tags,
                &grid,
                &time,
                &tags,
                &metric,
                format,
                root_kind,
                fused_output_tags,
                expression_field_names,
            )
        {
            return Ok(native_planned(
                plan,
                frame_metadata,
                root_kind,
                time,
                grid,
                unproved_keys,
            ));
        }
        if required_placement {
            // A tree whose fold can be split keeps that plan: it exports aggregate state rather than
            // whole samples. The general segmentation below is the fallback for the trees it refuses,
            // and the split is decided before anything consumes a child. Its producer reads whatever
            // input the statement prepared.
            let split = if selectors.len() == 1 {
                FusedSeries::split_aggregate(&tree, &time).context(DataFusionPlanningSnafu)?
            } else {
                None
            };
            if let Some((partial, final_tree)) = split {
                // The split's output is the suffix's, so a histogram answer is only carried when the
                // cut aggregate's exported state is a histogram's too: `sum`/`avg` over native
                // histograms merge their own state across the boundary. A histogram answer whose
                // aggregate sits elsewhere - above the cut, or built by a function of a float -
                // still keeps the existing plan, because no state of this route carries it. The
                // partial stage may introduce labels before aggregation, so the columns come from
                // the complete tree rather than from the suffix alone.
                let histogram_state = split_exports_histogram_state(&partial);
                if root_kind != FieldKind::Float
                    && !(root_kind == FieldKind::Histogram && histogram_state)
                {
                    return unsupported("split_aggregate_folds_float_state_only");
                }
                // The row-value layout is what the split is validated against whichever format the
                // caller asked for: it names one column per value the partial state accumulates,
                // while the encoded frame carries those values inside its own layout.
                let Some(value_schema) =
                    Self::fused_root_schema(&expression, &tree, &source_tags, &time, root_kind)?
                else {
                    return unsupported("root_has_no_value_columns");
                };
                // A frame operand also needs the evaluated layout its consumers decode, which is
                // the metadata derived above: a request that could not name one keeps the existing
                // plan rather than publishing rows to a frame consumer.
                if format == FusedOutputFormat::SeriesFrame && frame_metadata.is_none() {
                    return unsupported("frame_operand_needs_an_evaluated_layout");
                }
                let Some(rawinput) = children.into_iter().next() else {
                    return unsupported("split_aggregate_has_no_input");
                };
                let publish_grid = Arc::clone(&grid);
                let node = plan_split_aggregate(
                    Arc::from(expression.as_str()),
                    partial,
                    final_tree,
                    FusedSeriesBinding {
                        grid: publish_grid,
                        time_index: time.clone(),
                        tags,
                        metric_name: metric,
                    },
                    rawinput,
                    value_schema,
                    format,
                )?;
                let plan = LogicalPlan::Extension(Extension {
                    node: Arc::new(node),
                });
                return Ok(native_planned(
                    plan,
                    frame_metadata,
                    root_kind,
                    time,
                    grid,
                    unproved_keys,
                ));
            }
            // A tree that folds native histograms anywhere and whose split refused keeps the
            // existing plan instead of being segmented. A segment reads the frames a boundary above
            // it assembles - the canonical merge's collected pieces, or a region-local producer's
            // frames - and this route has no execution that folds a histogram state out of those:
            // every histogram fold it runs today reads its own scan. Until one is covered, the
            // statement keeps the plan that already answers it rather than publishing a column
            // nothing here can fill.
            if binds_histogram_state_anywhere(root) {
                return unsupported("histogram_state_above_a_segment_boundary");
            }
        }
        // The tags are asked for before the fields, because the walk answers for a tree the
        // compiler produced: a scalar root has no labels at all, and a sample root's are the ones
        // the walk derives. If it cannot derive them, this route refuses.
        let output_tags = match &tree.root().output {
            // A scalar has no labels, and its value is named as a bare literal's is.
            NativeType::Scalar(_) => Vec::new(),
            NativeType::Samples(_) => match fused_output_tags(tree.root(), &source_tags) {
                Some(output_tags) => with_row_name_column(tree.root(), output_tags),
                None => return unsupported("root_has_no_output_labels"),
            },
            NativeType::AggregateState(_) => {
                return unsupported("aggregate_state_is_not_a_statement_answer");
            }
        };
        let Some(root_fields) = native_root_fields(tree.root()) else {
            return unsupported("root_has_no_value_columns");
        };
        // A statement whose every input matched no table has no column of its own to name its grid
        // after: the empty input carries the synthetic `time` column the existing planner names an
        // empty metric's grid with (`setup_context_for_empty_metric`), and nothing supersedes it. A
        // statement named after that column - the metric `time` - would then publish its value and
        // its grid under one name, so the grid takes the first free `native_time_N`, exactly as the
        // data-free route above resolves the same collision. A value named after the row-name column
        // is not a collision this route can name its way out of: the name column is the series'
        // identity rather than a label of its own, so that statement keeps the existing plan, which
        // answers the empty vector for a table it has none of.
        let time = if named_by_a_real_input {
            time
        } else {
            let value_names = expression_field_names(&expression, &root_fields);
            if value_names
                .iter()
                .any(|name| output_tags.iter().any(|tag| tag == name))
            {
                return unsupported(UNSUPPORTED_SHAPE_UNNAMED);
            }
            free_time_index(&time, &value_names, &output_tags)
        };

        // A segment of a plan split over regions keeps the raw source as its input rather than a
        // bare scan: the segment's prefixes run on the frontend above the regions, so the raw series
        // source is what each of them reads.
        if required_placement || matches!(locality, InputLocality::SinglePartition) {
            let segment_metric = if cross_table { None } else { metric.clone() };
            let plan = segment_tree(
                Arc::from(expression.as_str()),
                &tree,
                &children,
                &source_tags,
                &bare_scans,
                &per_source,
                &grid,
                &time,
                &tags,
                &segment_metric,
                format,
                contains_canonical_merge(root),
                root_kind,
                // A cut must keep the source whenever a region's own read cannot hold what the cut
                // evaluates: a per-region prefix would evaluate a window over one region's fragment, so
                // `rate` never sees the samples the other region holds. That is a partition on a column
                // that is not a tag, and a partitioned table whose occurrences are source-backed.
                any_split_series || (per_source_inputs && any_partitioned),
                fused_output_tags,
                expression_field_names,
                PromPlanner::segmented_root_schema,
            );
            if let Some(plan) = plan {
                return Ok(native_planned(
                    plan,
                    frame_metadata,
                    root_kind,
                    time,
                    grid,
                    unproved_keys,
                ));
            }
            // One input with no split or segment above it leaves a raw scan doing the work of a
            // cross-series consumer, and a single scan does not prove its region holds every
            // contribution the consumer needs. Keep that consumer on the existing plan; a tree over more
            // than one input is not this shape.
            if required_placement && !multi_input {
                return unsupported("cross_series_consumer_over_one_scan");
            }
        }
        let output_schema = match format {
            FusedOutputFormat::PlainRows => {
                fused_output_schema(&expression, &root_fields, &output_tags, &time, root_kind)?
            }
            FusedOutputFormat::SeriesFrame => {
                let frame =
                    FusedSeries::series_frame_schema(&tree).context(DataFusionPlanningSnafu)?;
                Arc::new(
                    DFSchema::try_from(frame.as_ref().clone()).context(DataFusionPlanningSnafu)?,
                )
            }
        };
        // The children a node reads bind it: a `RawSeriesSource` child carries the frames and names the
        // metric it reads, so a node over them binds neither tags nor one metric name of its own. A node
        // whose children stay direct scans of one table reads that table through the one metric and tag
        // set they share.
        let (tags, metric) = if per_source_inputs {
            (Vec::new(), None)
        } else {
            (tags, metric)
        };
        // Every lowering path above publishes on the same grid the tree was compiled on, so the
        // binding is that grid rather than a per-shape choice.
        let publish_grid = Arc::clone(&grid);
        let node = FusedSeries::try_new_with_format(
            Arc::from(expression.as_str()),
            tree,
            FusedSeriesBinding {
                grid: publish_grid,
                // The node below names the same output column, so the binding takes a copy.
                time_index: time.clone(),
                tags,
                metric_name: metric,
            },
            children,
            output_schema,
            format,
        )
        .context(DataFusionPlanningSnafu)?;
        let plan = LogicalPlan::Extension(Extension {
            node: Arc::new(node),
        });
        Ok(native_planned(
            plan,
            frame_metadata,
            root_kind,
            time,
            grid,
            unproved_keys,
        ))
    }

    /// The columns a [`FusedSeries`] node over `tree` produces.
    ///
    /// Fields and tags are derived from the native tree; the expression names the output fields.
    /// If this walk cannot derive a tag set, the native route refuses rather than consulting the
    /// legacy planner.
    ///
    /// `Ok(None)` when the root is not a value the node can publish. A schema the output cannot
    /// declare is an error rather than a refusal: duplicate names violate the expression-named
    /// native output contract, so construction reports the collision instead of publishing a
    /// layout the reader cannot address.
    fn fused_root_schema(
        expression: &str,
        tree: &NativeTree,
        source_tags: &[Vec<String>],
        time: &str,
        root_kind: FieldKind,
    ) -> Result<Option<DFSchemaRef>> {
        let output_tags = match &tree.root().output {
            NativeType::Scalar(_) => Vec::new(),
            NativeType::Samples(_) => {
                let Some(tags) = fused_output_tags(tree.root(), source_tags) else {
                    return Ok(None);
                };
                with_row_name_column(tree.root(), tags)
            }
            NativeType::AggregateState(_) => return Ok(None),
        };
        let Some(root_fields) = native_root_fields(tree.root()) else {
            return Ok(None);
        };
        // The kind is the one the statement's own root answers, derived before segmentation cut
        // anything. It cannot be recomputed here: a cut prefix is replaced by an input leaf whose
        // samples are already evaluated, so walking this tree would read every such leaf as the
        // kind its scan carried. `count_over_time(h[5m]) + on (__name__) ...` answers floats, and
        // its replaced leaf carries floats, while the scan below was a histogram.
        fused_output_schema(expression, &root_fields, &output_tags, time, root_kind).map(Some)
    }

    /// The rows layout a segmented statement root publishes, under the segment builders' callback.
    ///
    /// A segment cannot carry this crate's error type, so a layout it cannot declare is a refusal
    /// here: the route keeps the plan the existing planner would have built for the same statement.
    fn segmented_root_schema(
        expression: &str,
        tree: &NativeTree,
        source_tags: &[Vec<String>],
        time: &str,
        root_kind: FieldKind,
    ) -> Option<DFSchemaRef> {
        Self::fused_root_schema(expression, tree, source_tags, time, root_kind)
            .ok()
            .flatten()
    }

    /// The statement being planned, rebuilt from the evaluation numbers the planner holds.
    ///
    /// The compiler reads the statement's grid, lookback and `@` anchors rather than the planner's mutable
    /// state. A nested binary is offered only while `ctx.start` and `ctx.end` are still the statement's
    /// bounds; anchored windows and subqueries can rewrite those bounds. `None` means the bounds are not
    /// representable as a [`SystemTime`].
    fn native_statement(&self, prom_expr: &PromExpr) -> Option<EvalStmt> {
        let start = epoch_millis(self.ctx.stmt_start)?;
        let end = epoch_millis(self.ctx.stmt_end)?;
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
/// No table matched, so there is no scan and no metric name: the input is an empty raw series frame, the
/// one input a [`FusedSeries`] node already reads. `schema` is the layout this route compiles a selector
/// against - one field per field the selection names, and no grid; the planner re-lays it out in the
/// schema of the set operation it takes part in. No row is ever read, so a surviving name decides only
/// what an operator answering on its own - `absent` - calls its value column.
///
/// The empty plan declares the frame a scan would declare, so physical planning reads it as the frame
/// producer it is, with explicit labels rather than a scan's storage key.
fn empty_discovered_input(time_index: String, schema: Arc<SeriesSchema>) -> Result<PreparedInput> {
    let input = empty_discovered_frame(&schema)?;
    // No table stands behind this input, so no region splits it along a partition column - and no
    // metadata states what its keys would survive, which is the answer that collects.
    Ok((
        input,
        schema,
        None,
        time_index,
        Vec::new(),
        false,
        false,
        false,
    ))
}

fn empty_discovered_frame(schema: &SeriesSchema) -> Result<LogicalPlan> {
    let frame =
        raw_series_frame_schema_with_ordering(schema, &RawLabelSource::Explicit, NATIVE_ORDERING)
            .context(DataFusionPlanningSnafu)?;
    Ok(LogicalPlan::EmptyRelation(EmptyRelation {
        produce_one_row: false,
        schema: Arc::new(
            DFSchema::try_from(frame.as_ref().clone()).context(DataFusionPlanningSnafu)?,
        ),
    }))
}

/// The raw series schema of the typed empty input [`empty_discovered_input`] builds, declaring `fields`.
fn empty_discovered_series_schema(fields: Vec<String>) -> Arc<SeriesSchema> {
    Arc::new(SeriesSchema {
        fields: fields
            .iter()
            .map(|name| FieldSpec {
                name: Arc::from(name.as_str()),
            })
            .collect::<Vec<_>>()
            .into(),
        eval_grid: None,
    })
}

/// The value fields an input that reads no table declares.
///
/// A selection keeps the field columns its `__field__` matcher names, and no metric table matched this
/// selector, so the only field names this input can be given are the ones the matcher states itself: an
/// `=` names one field. The selector that carries no such matcher gets the one default field, which is
/// the name this route gives the single value column it reads a scan's field into.
///
/// `None` is a matcher whose answer is a set over the fields a table carries - `!=`, `=~` and `!~` keep
/// or drop columns by a rule over every one of them - because an input that reads no table has none to
/// apply that rule to. Such a selection keeps the plan the existing planner would have built for it
/// rather than claiming a layout nothing decided.
fn empty_discovered_fields(matchers: Option<&[Matcher]>) -> Option<Vec<String>> {
    let Some(matchers) = matchers else {
        return Some(vec![DEFAULT_FIELD_COLUMN.to_string()]);
    };
    let mut fields = Vec::with_capacity(matchers.len());
    for matcher in matchers {
        if matcher.op != MatchOp::Equal {
            return None;
        }
        if !fields.iter().any(|field| field == &matcher.value) {
            fields.push(matcher.value.clone());
        }
    }
    Some(fields)
}

/// Whether `expr` asks for `absent_over_time`, whose window is read through a fold that keeps the
/// metric name.
fn reads_presence(expr: &PromExpr) -> bool {
    #[derive(Default)]
    struct Presence(bool);

    impl promql_parser::util::ExprVisitor for Presence {
        type Error = std::convert::Infallible;

        fn pre_visit(&mut self, expr: &PromExpr) -> std::result::Result<bool, Self::Error> {
            if matches!(expr, PromExpr::Call(call) if call.func.name == "absent_over_time") {
                self.0 = true;
            }
            Ok(true)
        }
    }

    let mut presence = Presence::default();
    let _ = promql_parser::util::walk_expr(&mut presence, expr);
    presence.0
}

/// Whether a prepared selector already carries its own raw sources, and so hands the fused node
/// explicit-label frames rather than a bare region scan.
///
/// A discovered selector is that shape: a single metric is one [`RawSeriesSource`], several are a
/// `Union` of them, and each names the metric it reads. Wrapping either again would nest a source in
/// a source.
fn is_sourced_plan(plan: &LogicalPlan) -> bool {
    matches!(plan, LogicalPlan::Union(_))
        || matches!(
            plan,
            LogicalPlan::Extension(extension)
                if extension.node.as_any().is::<RawSeriesSource>()
        )
}

/// The moment `millis` milliseconds from the Unix epoch, on whichever side of it that lands.
///
/// A pre-epoch statement reaches the planner as a negative millisecond. `None` means the bound is
/// outside what a [`SystemTime`] represents.
fn epoch_millis(millis: i64) -> Option<SystemTime> {
    if millis.is_negative() {
        UNIX_EPOCH.checked_sub(Duration::from_millis(millis.unsigned_abs()))
    } else {
        UNIX_EPOCH.checked_add(Duration::from_millis(millis as u64))
    }
}

/// Finds a subquery at the statement root, treating parentheses as transparent.
fn root_subquery(expr: &PromExpr) -> Option<&SubqueryExpr> {
    match expr {
        PromExpr::Subquery(subquery) => Some(subquery),
        PromExpr::Paren(paren) => root_subquery(&paren.expr),
        _ => None,
    }
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

/// The names an expression gives the value columns of `fields`.
///
/// One value is named after the whole expression; several append their compiled field names to it,
/// because one name cannot stand for more than one column. Only the published layout is named this
/// way: the fields inside the native tree keep the names the compiler gave them.
fn expression_field_names(expression: &str, fields: &[String]) -> Vec<String> {
    fields
        .iter()
        .map(|field| {
            if fields.len() == 1 {
                expression.to_string()
            } else {
                format!("{expression}.{field}")
            }
        })
        .collect()
}

/// The column a rows layout publishes its grid under, moved off any column it would collide with.
///
/// A label or value named `time` is the one collision the default time column can have: the value
/// columns are named after the expression and the tags are the labels a series reports, so nothing
/// but the grid itself is renamed. The time column is this route's own synthetic name, so a layout
/// that already holds one publishes its grid under the first free `native_time_N` instead of
/// dropping or renaming the column it collides with.
///
/// Two callers ask this: a data-free tree, whose grid is the only column it could collide with, and
/// a statement whose every input matched no table, whose grid is named after the synthetic column
/// the existing planner names an empty metric after (`setup_context_for_empty_metric`). A statement
/// that reads a table keeps that table's own time column, which is the name its scan declares.
fn free_time_index(time_index: &str, value_names: &[String], tags: &[String]) -> String {
    let taken = |candidate: &str| value_names.iter().chain(tags).any(|name| name == candidate);
    if !taken(time_index) {
        return time_index.to_string();
    }
    (0usize..)
        .map(|index| format!("native_time_{index}"))
        .find(|candidate| !taken(candidate))
        .expect("the time column candidate sequence is unbounded")
}

/// The columns a [`FusedSeries`] node produces: its time index, one column per value, then its tags.
///
/// A single value is named after the expression, and multiple values append their compiled field
/// names to it, so a value is never published under a name the reader cannot address. The names
/// inside the native tree are unchanged.
///
/// The layout itself is [`segment::fused_output_schema`]'s, which is where a segment asks for the
/// same columns; this is that answer under this crate's error type, which is what the planner's own
/// call sites report.
fn fused_output_schema(
    expression: &str,
    fields: &[String],
    tags: &[String],
    time_index: &str,
    field_kind: FieldKind,
) -> Result<DFSchemaRef> {
    let value_names = expression_field_names(expression, fields);
    // A name appears twice when a value named after its expression - a bare selector is named after
    // its metric - lands on the table's own time index or on one of its tags, or when two values
    // take one name. `DFSchema` rejects that, but reports only that building the plan failed, which
    // names no column; the collision is detected here so the error names the columns that collide.
    let mut names = Vec::with_capacity(value_names.len() + tags.len() + 1);
    names.push(time_index.to_string());
    names.extend(value_names.iter().cloned());
    names.extend(tags.iter().cloned());
    let mut occurrences: std::collections::HashMap<&str, usize> =
        std::collections::HashMap::with_capacity(names.len());
    for name in &names {
        *occurrences.entry(name.as_str()).or_default() += 1;
    }
    let mut reported = std::collections::HashSet::new();
    let duplicates = names
        .iter()
        .filter(|name| occurrences[name.as_str()] > 1 && reported.insert(name.as_str()))
        .map(String::as_str)
        .collect::<Vec<_>>();
    ensure!(
        duplicates.is_empty(),
        DuplicateOutputColumnsSnafu {
            names: duplicates.join(", ")
        }
    );
    segment::fused_output_schema(&value_names, tags, time_index, field_kind)
        .context(DataFusionPlanningSnafu)
}

/// Builds the two nodes a split native aggregate runs in.
///
/// The first is a [`FusedSeries`] over `rawinput` running the per-series prefix and a `Partial`
/// aggregate at its root, so each partition exports the state it folded. The second runs the `Final`
/// aggregate and everything the split moved above it: it reads state where the first reads series.
/// Between them the physical planner puts a `SeriesRepartitionExec`, which is what makes every row of
/// a group reach one final partition.
///
/// `format` is the representation the caller needs from the second node: the statement's row table,
/// or the evaluated series frame another native node reads as an operand.
///
/// `value_schema` is the statement's row-value layout; its value columns must match the partial
/// state's accumulator count.
fn plan_split_aggregate(
    promql: Arc<str>,
    partial: NativeTree,
    final_tree: NativeTree,
    binding: FusedSeriesBinding,
    rawinput: LogicalPlan,
    value_schema: DFSchemaRef,
    format: FusedOutputFormat,
) -> Result<FusedSeries> {
    // The kind a value column carries is the state's own: a numeric fold publishes `Float64`, a
    // native-histogram fold publishes the histogram value type. Reading it here keeps the layout and
    // the schema it is checked against in one place.
    let (state_accumulators, value_type) = match &partial.root().output {
        NativeType::AggregateState(state) => (
            state.accumulators.len(),
            if state.kind.is_histogram() {
                native_histogram_value_type().as_arrow_type()
            } else {
                ArrowDataType::Float64
            },
        ),
        _ => {
            return Err(DataFusionError::Plan(format!(
                "the partial tree for `{promql}` does not produce aggregate state"
            )))
            .context(DataFusionPlanningSnafu);
        }
    };
    let values = value_schema
        .fields()
        .iter()
        .filter(|field| field.data_type() == &value_type)
        .count();
    if values != state_accumulators {
        return Err(DataFusionError::Plan(format!(
            "the partial state for `{promql}` has {state_accumulators} accumulator(s), but the output schema declares \
             {values} value column(s)"
        )))
        .context(DataFusionPlanningSnafu);
    }
    // The partial tree publishes aggregate state on the cut aggregate's grid, which is not
    // necessarily the statement's when the suffix re-grids the values - a subquery above the cut
    // produces on its own grid. The final tree's binding is the one the caller derived for it.
    let partial_binding = FusedSeriesBinding {
        grid: match &partial.root().output {
            NativeType::AggregateState(state) => Arc::clone(&state.grid),
            _ => Arc::clone(&binding.grid),
        },
        ..binding.clone()
    };
    let partial = FusedSeries::try_new_state_export(
        Arc::clone(&promql),
        partial,
        partial_binding,
        vec![rawinput],
    )
    .context(DataFusionPlanningSnafu)?;
    let output_schema = match format {
        FusedOutputFormat::PlainRows => value_schema,
        // The final node's output is the frame its own evaluated root declares, so it is derived
        // from the tree it runs rather than from the row layout.
        FusedOutputFormat::SeriesFrame => {
            let frame =
                FusedSeries::series_frame_schema(&final_tree).context(DataFusionPlanningSnafu)?;
            Arc::new(DFSchema::try_from(frame.as_ref().clone()).context(DataFusionPlanningSnafu)?)
        }
    };
    FusedSeries::try_new_with_format(
        promql,
        final_tree,
        binding,
        vec![LogicalPlan::Extension(Extension {
            node: Arc::new(partial),
        })],
        output_schema,
        format,
    )
    .context(DataFusionPlanningSnafu)
}

/// The time-range filter a selector's scan reads, from the bounds its compiled requirement names.
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

/// Whether any aggregate in `node` consumes histogram samples, wherever it sits in the tree.
///
/// The walk asks each aggregate what kind enters it rather than what the tree's root answers,
/// because an operator above an aggregate hides it: `sum(h) * 2` has a binary root. Only a
/// histogram entering an aggregate that would fold its payload is refused; a float-producing prefix
/// such as `count_over_time` over a histogram stays eligible, and so does an aggregate that skips the
/// sample instead of folding it.
fn aggregate_reads_histogram(node: &NativeNode, input_kind: FieldKind) -> bool {
    // `count` and `group` count a histogram as one present sample, and
    // `min`/`max`/`stddev`/`stdvar`/`quantile` skip it the way Prometheus does -- the legacy
    // planner maps all five through one branch (`create_native_histogram_aggregate_expr`), which
    // drops the samples and answers a Float NULL plus an info annotation, and the native fold does
    // the same thing at run time. Only `sum` and `avg` fold the payload, and an aggregate that
    // folds it runs here over a histogram when it binds the histogram state the kernel folds and
    // merges. `binds_histogram_state` is that binding, read from the same table the compiler and
    // the split bind from, so the gate and the runtime cannot disagree about which kinds fold.
    //
    // Everything else that is not provably a float entering it is still refused: "cannot name it"
    // is not "not a histogram", and a fold that read such a payload as a float would answer
    // something else. A canonical merge (`label_replace` collapsing a label) and a set operation
    // both hand on the samples they were given without the walk describing them, so they are what
    // this check still catches.
    node.any(&|node| {
        matches!(
            &node.expr,
            NativeExpr::Aggregate { input, op }
                if matches!(op.kind, AggregateKind::Sum | AggregateKind::Avg)
                    && !matches!(
                        input.output_field_kind(Some(input_kind)),
                        Some(FieldKind::Float)
                    )
                    && !binds_histogram_state(op)
        )
    })
}

/// Whether this aggregate's state carries a native histogram.
///
/// It is the same table the compiler and the split bind from, so the gates and the runtime agree on
/// which aggregates fold a payload: `sum` and `avg` over histograms do, and every other aggregate
/// answers a float whatever it reads.
fn binds_histogram_state(op: &BoundAggregate) -> bool {
    aggregate_state_binding(op.kind, op.field_kind).is_some_and(|(kind, _)| kind.is_histogram())
}

/// Whether any aggregate in `node` binds a state that carries a native histogram.
///
/// Unlike [`aggregate_reads_histogram`] this asks what each aggregate *keeps*, not what enters it:
/// a tree that folds a histogram anywhere in it needs the split (or nothing at all), because only
/// the split's state carries that fold across a partition boundary.
fn binds_histogram_state_anywhere(node: &NativeNode) -> bool {
    node.any(&|node| {
        matches!(
            &node.expr,
            NativeExpr::Aggregate { op, .. } if binds_histogram_state(op)
        )
    })
}

/// Whether the state a split tree exports is a native histogram's.
///
/// The partial tree's root is the cut aggregate bound as `Partial`, so its own binding is what says
/// which state crosses the boundary; reading it there is what keeps the gate and the runtime from
/// disagreeing about the layout the suffix will merge.
fn split_exports_histogram_state(partial: &NativeTree) -> bool {
    match &partial.root().expr {
        NativeExpr::Aggregate { op, .. } => binds_histogram_state(op),
        _ => false,
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
/// A selector that matches everything is answered by narrowing it rather than by planning one query
/// that reads the whole schema at once.
const MAX_DISCOVERED_METRICS: usize = 1024;

/// Whether a selector names no single table and has to enumerate the schema's metric tables.
///
/// Exported so the authorization layer can ask the same question before it authorizes what the
/// planner will scan; the two answers have to agree.
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

/// Whether `node` holds an operation whose answer depends on the order its input arrives in.
///
/// `limitk` keeps the first `k` series of each group *in input order* ([`promql::select::limitk`]),
/// and a node with more than one input reads them through a gather that hands over whichever
/// partition became ready first, so the schedule would decide which series a query keeps. Only this
/// operation's selection depends on that order: `topk` breaks ties by label and `limit_ratio`
/// selects by a label hash.
fn depends_on_input_order(node: &NativeNode) -> bool {
    match &node.expr {
        NativeExpr::Input(_) | NativeExpr::Scalar(_) | NativeExpr::EvalTime { .. } => false,
        NativeExpr::Instant { input, .. }
        | NativeExpr::Merge { input }
        | NativeExpr::Window { input, .. }
        | NativeExpr::Map { input, .. }
        | NativeExpr::Timestamp { input }
        | NativeExpr::Aggregate { input, .. }
        // `vector()` and `scalar()` answer one series or one value from their input, and since
        // `scalar()` exists their input can hold an order-dependent selection:
        // `vector(scalar(limitk(1, a)))` keeps whichever `a` series the gather handed over first.
        | NativeExpr::VectorOf { input }
        | NativeExpr::ScalarOf { input } => depends_on_input_order(input),
        NativeExpr::ParameterizedMap {
            input, parameters, ..
        } => depends_on_input_order(input) || parameters.iter().any(depends_on_input_order),
        NativeExpr::ParameterizedTopK {
            input, parameter, ..
        } => depends_on_input_order(input) || depends_on_input_order(parameter),
        // The fold's own order is its input's, and the argument it reads per point can hold an
        // order-dependent selection of its own (`quantile(scalar(limitk(1, a)), b)`).
        NativeExpr::ParameterizedAggregate {
            input, parameter, ..
        } => depends_on_input_order(input) || depends_on_input_order(parameter),
        NativeExpr::ParameterizedHistogram {
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

/// One side of a set operation, as the slice of the pre-order selector list it reads.
///
/// The slice is the side's whole subtree, so it also says whether the side reads a table at all.
#[derive(Clone, Copy, Debug)]
struct SetOperand {
    start: usize,
    end: usize,
}

/// One set operation in the statement and the two operands it compares.
#[derive(Clone, Copy, Debug)]
struct SetOperation {
    lhs: SetOperand,
    rhs: SetOperand,
}

/// [`collect_selectors`], also recording the set operations it passes.
///
/// The operands are recorded as index ranges of `out`, which is the pre-order the compiler binds its inputs in.
fn collect_selectors_with_set_operations<'a>(
    stmt: &EvalStmt,
    expr: &'a PromExpr,
    under_rate_or_increase: bool,
    out: &mut Vec<(Cow<'a, VectorSelector>, bool)>,
    set_operations: &mut Vec<SetOperation>,
) -> bool {
    match expr {
        PromExpr::VectorSelector(selector) => {
            out.push((Cow::Borrowed(selector), under_rate_or_increase))
        }
        PromExpr::MatrixSelector(MatrixSelector { vs, .. }) => {
            out.push((Cow::Borrowed(vs), under_rate_or_increase))
        }
        PromExpr::Paren(ParenExpr { expr }) => {
            return collect_selectors_with_set_operations(
                stmt,
                expr,
                under_rate_or_increase,
                out,
                set_operations,
            );
        }
        PromExpr::Call(call) if call.func.name == "info" => {
            let Ok(InfoInput { selector, .. }) = info_input(stmt, &call.args.args) else {
                return false;
            };
            if !collect_selectors_with_set_operations(
                stmt,
                &call.args.args[0],
                under_rate_or_increase,
                out,
                set_operations,
            ) {
                return false;
            }
            out.push((Cow::Owned(selector), under_rate_or_increase));
        }
        PromExpr::Binary(binary) => {
            let lhs_start = out.len();
            if !collect_selectors_with_set_operations(
                stmt,
                &binary.lhs,
                under_rate_or_increase,
                out,
                set_operations,
            ) {
                return false;
            }
            let lhs_end = out.len();
            if !collect_selectors_with_set_operations(
                stmt,
                &binary.rhs,
                under_rate_or_increase,
                out,
                set_operations,
            ) {
                return false;
            }
            if binary.op.is_set_operator() {
                set_operations.push(SetOperation {
                    lhs: SetOperand {
                        start: lhs_start,
                        end: lhs_end,
                    },
                    rhs: SetOperand {
                        start: lhs_end,
                        end: out.len(),
                    },
                });
            }
            return true;
        }
        // Literals contribute no selector; the compiler decides which scalar forms can be bound.
        PromExpr::NumberLiteral(_) => return true,
        PromExpr::Unary(unary) => {
            return collect_selectors_with_set_operations(
                stmt,
                &unary.expr,
                under_rate_or_increase,
                out,
                set_operations,
            );
        }
        // `histogram_quantile` and `histogram_fraction` take their vector last, and the compiler
        // compiles that vector before the arguments in front of it, so the walk reads it first: the
        // scan prepared at a position has to be the one the compiler binds there.
        PromExpr::Call(call)
            if matches!(call.func.name, "histogram_quantile" | "histogram_fraction") =>
        {
            let Some((vector, parameters)) = call.args.args.split_last() else {
                return false;
            };
            return collect_selectors_with_set_operations(
                stmt,
                vector,
                under_rate_or_increase,
                out,
                set_operations,
            ) && parameters.iter().all(|parameter| {
                collect_selectors_with_set_operations(
                    stmt,
                    parameter,
                    under_rate_or_increase,
                    out,
                    set_operations,
                )
            });
        }
        PromExpr::Call(call) => {
            if matches!(call.func.name, "label_replace" | "label_join") {
                let arity_valid = if call.func.name == "label_replace" {
                    call.args.args.len() == 5
                } else {
                    call.args.args.len() >= 3
                };
                return arity_valid
                    && matches!(call.args.args.first(), Some(arg) if matches!(arg.value_type(), ValueType::Vector | ValueType::Matrix))
                    && collect_selectors_with_set_operations(
                        stmt,
                        &call.args.args[0],
                        under_rate_or_increase,
                        out,
                        set_operations,
                    );
            }
            for arg in &call.args.args {
                if !collect_selectors_with_set_operations(
                    stmt,
                    arg,
                    under_rate_or_increase || matches!(call.func.name, "rate" | "increase"),
                    out,
                    set_operations,
                ) {
                    return false;
                }
            }
        }
        // The compiler compiles an aggregate's input and then its parameter (`topk`/`bottomk` take an expression
        // where the others take a number or a label), so a parameter that reads a selector is a second source
        // bound after the input. Walking both keeps the prepared scan and the bound input at the same position.
        PromExpr::Aggregate(aggregate) => {
            return collect_selectors_with_set_operations(
                stmt,
                &aggregate.expr,
                under_rate_or_increase,
                out,
                set_operations,
            ) && match &aggregate.param {
                Some(param) => collect_selectors_with_set_operations(
                    stmt,
                    param,
                    under_rate_or_increase,
                    out,
                    set_operations,
                ),
                None => true,
            };
        }
        // A subquery evaluates its expression on its own grid; its selectors are prepared like any
        // other, each with the scan bounds `raw_scan_bounds` derives from its own range and step.
        PromExpr::Subquery(subquery) => {
            // Keep the set operations inside the subquery in the same accumulator: a zero-match
            // selector there is laid out by its own set operand, not by the statement's layout.
            return collect_selectors_with_set_operations(
                stmt,
                &subquery.expr,
                under_rate_or_increase,
                out,
                set_operations,
            );
        }
        _ => {}
    }
    true
}

/// The index of the real input each zero-match input is laid out by, in `prepared` order.
///
/// A set operation pairs its two sides' fields by position, so an operand that reads no real input is laid out
/// by the first real selector in the other operand, and that layout reaches every zero-match selector of the
/// operand, nested set operations included. An empty input with no such operand above it has no entry here.
fn local_empty_layouts(
    set_operations: &[SetOperation],
    prepared: &[PreparedInput],
) -> Vec<Option<usize>> {
    let mut layouts = vec![None; prepared.len()];
    let real = |index: usize| {
        prepared
            .get(index)
            .is_some_and(|(scan, ..)| !is_empty_discovered_input(scan))
    };
    // The operations are recorded inside out, so an inner one decides before the operation above it
    // reads the subtree it holds. An operand that reads no real input holds no real input inside it
    // either, so an operation that writes a layout here is the only one that reaches those indices.
    for operation in set_operations {
        for (empty, sibling) in [
            (operation.lhs, operation.rhs),
            (operation.rhs, operation.lhs),
        ] {
            // A side that reads a table lays its own selectors out: only a side that reads none
            // needs the layout of the comparison it takes part in.
            if (empty.start..empty.end).any(&real) {
                continue;
            }
            let Some(source) = (sibling.start..sibling.end).find(|index| real(*index)) else {
                continue;
            };
            for index in empty.start..empty.end {
                let Some(layout) = layouts.get_mut(index) else {
                    continue;
                };
                if layout.is_none() {
                    *layout = Some(source);
                }
            }
        }
    }
    layouts
}

/// Whether `engine` can serve raw series frames to the fused-series node.
///
/// A scanner capability boundary: it avoids probing a region during logical planning.
fn supports_native_raw_series(engine: &str) -> bool {
    matches!(engine, MITO_ENGINE | MITO2_ENGINE | METRIC_ENGINE)
}

/// The type the raw series scan hands one field of `statement` to the fused node as.
///
/// The scan casts a numeric field to `Float64` and converts a native histogram struct into histogram
/// samples; nothing else has a raw representation, so every other type stays what it is here and is
/// refused by the field-kind guard that reads this. A `Float64` field is a float under either route, so
/// only the remaining numeric types depend on the statement: they may be planned as the float the scan
/// reads only where the existing plan publishes a `Float64` for that field as well, which is
/// [`publishes_float_values`]. A decimal is numeric to Arrow but has no float representation under
/// either route, so it keeps the existing plan.
fn read_field_type(data_type: &ArrowDataType, statement: &PromExpr) -> ArrowDataType {
    if is_raw_numeric(data_type) && publishes_float_values(statement, data_type) {
        ArrowDataType::Float64
    } else {
        data_type.clone()
    }
}

/// Whether a field of this type is one the raw series scan casts to `Float64`.
///
/// Arrow counts the decimals as numeric, while neither route reads one as a float, so they are left out
/// here: a decimal field keeps the existing plan instead of reaching a frame that cannot hold it.
fn is_raw_numeric(data_type: &ArrowDataType) -> bool {
    matches!(
        data_type,
        ArrowDataType::Int8
            | ArrowDataType::Int16
            | ArrowDataType::Int32
            | ArrowDataType::Int64
            | ArrowDataType::UInt8
            | ArrowDataType::UInt16
            | ArrowDataType::UInt32
            | ArrowDataType::UInt64
            | ArrowDataType::Float16
            | ArrowDataType::Float32
    )
}

/// The range functions, each of which answers a `Float64` whatever its window holds.
///
/// Every one of them is a UDF whose value window is a `Float64` range and whose return type is
/// `Float64` (`promql::functions`), so the existing plan casts the window's values before folding them:
/// `rate(integer_field[5m])` publishes `Float64`. The functions are named rather than matched by prefix
/// so that a call this list does not state - `abs`, `ceil` and the rest of the math functions, which
/// keep an integer's own type - keeps the existing plan.
const FLOAT_ANSWERING_RANGE_FUNCTIONS: &[&str] = &[
    "avg_over_time",
    "changes",
    "count_over_time",
    "delta",
    "deriv",
    "double_exponential_smoothing",
    "holt_winters",
    "idelta",
    "increase",
    "irate",
    "last_over_time",
    "max_over_time",
    "min_over_time",
    "predict_linear",
    "present_over_time",
    "quantile_over_time",
    "rate",
    "resets",
    "stddev_over_time",
    "stdvar_over_time",
    "sum_over_time",
];

/// The type the existing planner publishes for the values of one expression over one field.
///
/// The fused node declares one `Float64` value column per field, so a numeric field the raw scan reads as
/// a float may only enter the route where the existing plan publishes a float for it too. That is a
/// property of the shape of the expression and, for `sum`, of the type it folds - which is why this is a
/// class rather than a boolean: an integer `sum` reports its argument's own type, and a `Float32` one is
/// widened to `Float64`, whatever the shape below it publishes.
#[derive(Clone, Copy, PartialEq, Eq)]
enum PublishedValues {
    /// A `Float64`, which is the one kind the fused node declares.
    Float64,
    /// A narrower float, `Float16` or `Float32`. `sum` folds one into a `Float64`; everything else
    /// publishes it as it is.
    NarrowFloat,
    /// An integer, `Int8` through `Int64` and `UInt8` through `UInt64`. `sum` keeps it, `count` builds
    /// one, and the field itself is one under a bare selector.
    Integer,
    /// A type or a shape this rule does not model: a string, a decimal, a native histogram, or a call
    /// whose answer is not written down here. Nothing is admitted on it.
    Other,
}

/// Whether the existing planner publishes a `Float64` for `field_type` under `statement`.
///
/// Refusing is what keeps the two routes' columns identical, so a shape this rule does not prove is a
/// refusal rather than a guess. The shapes it proves, and the existing planner's site that decides each:
///
/// - parentheses and subqueries are not shapes of their own: they publish what their inner expression
///   publishes,
/// - arithmetic binary operators are built by `PromPlanner::prom_token_to_binary_expr_builder`, which
///   wraps both operands in `cast_float` - but the cast has to be the first thing that reaches the
///   samples for the answer to stay the same, which is [`folds_before_cast`]: `<float fold> * 1` is
///   admitted and `sum(integer_field) * 1` is not,
/// - `min`/`max` fold through `common_function::aggrs::extremum::Extremum`, whose `return_type` is
///   `Float64`, and `avg`, `stddev`, `stdvar`, `quantile` and `group` through UDAFs that answer `Float64`
///   for every numeric input (`PromPlanner::create_numeric_aggregate_expr`),
/// - `sum` follows DataFusion's `sum`, which answers `Float64` for a float argument and the argument's
///   own type for an integer one,
/// - the range functions answer `Float64`: each is a UDF whose value window is a `Float64` range
///   ([`FLOAT_ANSWERING_RANGE_FUNCTIONS`]).
///
/// What is not proved, and why it keeps the existing plan:
///
/// - a bare selector copies the value column's own type (`InstantManipulate::calculate_output_schema`),
/// - `count` and `count_values` fold through `count`, which answers `Int64` for every input,
/// - the math functions keep the type they are given (`abs(integer_field)` is an integer) and so do
///   `label_replace`/`label_join`, `ceil`, `floor`, `round`, `clamp*` and `sort*`,
/// - `-x` negates a column of its own type, and a comparison without `bool` filters on one,
/// - a set operation hands on the values it was given, and `topk`/`bottomk`/`limitk` keep the samples
///   they pick.
fn publishes_float_values(statement: &PromExpr, field_type: &ArrowDataType) -> bool {
    published_values(statement, field_type) == PublishedValues::Float64
}

/// The class [`publishes_float_values`] reads: what the existing plan publishes for `statement`.
fn published_values(statement: &PromExpr, field_type: &ArrowDataType) -> PublishedValues {
    match statement {
        // Neither parentheses nor a subquery is a shape of its own: the values a subquery publishes are
        // those of the expression it ranges over.
        PromExpr::Paren(ParenExpr { expr }) => published_values(expr, field_type),
        PromExpr::Subquery(SubqueryExpr { expr, .. }) => published_values(expr, field_type),
        // A selector publishes the value column itself, which is the field's own type.
        PromExpr::VectorSelector(_) | PromExpr::MatrixSelector(_) => field_values(field_type),
        // Every arithmetic operator is built by the planner's `cast_float` builder, which casts both
        // operands: what it publishes is a `Float64` whatever the fields hold. A comparison publishes a
        // float only when it is written with `bool`, and a set operation hands on the values it was given,
        // so neither is a shape this rule models.
        PromExpr::Binary(binary) => {
            let arithmetic = matches!(
                binary.op.id(),
                token::T_ADD
                    | token::T_SUB
                    | token::T_MUL
                    | token::T_DIV
                    | token::T_MOD
                    | token::T_POW
                    | token::T_ATAN2
            );
            // The cast is the operator's own, so it reaches whatever an operand answers. An operand that
            // folds raw samples into a non-float answer hands over a value the existing plan computed
            // exactly, which the raw series scan cannot; such a shape keeps the existing plan.
            if !arithmetic
                || folds_before_cast(&binary.lhs, field_type)
                || folds_before_cast(&binary.rhs, field_type)
            {
                PublishedValues::Other
            } else {
                PublishedValues::Float64
            }
        }
        PromExpr::Aggregate(aggregate) => match aggregate.op.id() {
            // `min` and `max` fold through `Extremum`, and `avg`, `stddev`, `stdvar`, `quantile` and
            // `group` fold through UDAFs whose answer is a `Float64` for every numeric input.
            token::T_MIN
            | token::T_MAX
            | token::T_AVG
            | token::T_STDDEV
            | token::T_STDVAR
            | token::T_QUANTILE
            | token::T_GROUP => PublishedValues::Float64,
            // DataFusion's `sum` answers `Float64` for a float argument and the argument's own type for an
            // integer one, so what it publishes is what it folds.
            token::T_SUM => match published_values(&aggregate.expr, field_type) {
                PublishedValues::Float64 | PublishedValues::NarrowFloat => PublishedValues::Float64,
                // An integer sum keeps the integer it folds, and a type this rule does not model stays
                // one.
                PublishedValues::Integer => PublishedValues::Integer,
                PublishedValues::Other => PublishedValues::Other,
            },
            // `count` and `count_values` fold through `count`, which answers `Int64` for every input.
            token::T_COUNT | token::T_COUNT_VALUES => PublishedValues::Integer,
            // `topk`, `bottomk`, `limitk` and `limit_ratio` report some of the samples they were given, so
            // they publish what their input publishes.
            token::T_TOPK | token::T_BOTTOMK | token::T_LIMITK | token::T_LIMIT_RATIO => {
                published_values(&aggregate.expr, field_type)
            }
            _ => PublishedValues::Other,
        },
        // The range functions answer floats; every other call - the math functions, the label functions,
        // `timestamp`, `vector`, `scalar` - publishes the type it is given or has no field at all.
        PromExpr::Call(call) => {
            if FLOAT_ANSWERING_RANGE_FUNCTIONS.contains(&call.func.name) {
                PublishedValues::Float64
            } else {
                PublishedValues::Other
            }
        }
        _ => PublishedValues::Other,
    }
}

/// Whether `statement` folds raw samples into an answer that is not a float, which is what makes the
/// cast above it late.
///
/// An arithmetic operator's `cast_float` makes its column a `Float64`, but a column type is not an
/// answer: where an operand *folds* the samples before the cast reaches them, the existing plan folds
/// exactly - `sum` of an `Int64` column adds integers and casts the sum - while the raw series scan
/// casts every sample and only then folds. Two integers above `2^53` that cancel answer `1` before the
/// cast and `0` after it, so `sum(integer_field) * 1` keeps the existing plan. `sum(rate(m[5m]))` is
/// what that bound admits: the fold it applies is already a float fold, which is
/// [`publishes_float_values`] of the aggregate itself.
///
/// A bare selector, a literal and a call hand over one sample at a time, so the cast reaches the same
/// sample the scan casts and no fold happens in between.
fn folds_before_cast(statement: &PromExpr, field_type: &ArrowDataType) -> bool {
    match statement {
        PromExpr::Paren(ParenExpr { expr }) => folds_before_cast(expr, field_type),
        PromExpr::Subquery(SubqueryExpr { expr, .. }) => folds_before_cast(expr, field_type),
        PromExpr::Binary(binary) => {
            folds_before_cast(&binary.lhs, field_type) || folds_before_cast(&binary.rhs, field_type)
        }
        PromExpr::Unary(unary) => folds_before_cast(&unary.expr, field_type),
        // A fold answers what it folds: an integer `sum` keeps the integer it added, `count` builds an
        // `Int64`, and the ranking aggregates report the samples they were given. None of them is a
        // `Float64`, so the cast above one of them is the late cast this refuses.
        PromExpr::Aggregate(_) => !publishes_float_values(statement, field_type),
        // A selector, a literal and a call reach one sample at a time; the cast reaches that same sample.
        _ => false,
    }
}

/// The class of a value column of this type: what a selector over a field of it publishes.
fn field_values(field_type: &ArrowDataType) -> PublishedValues {
    match field_type {
        ArrowDataType::Float64 => PublishedValues::Float64,
        ArrowDataType::Float16 | ArrowDataType::Float32 => PublishedValues::NarrowFloat,
        ArrowDataType::Int8
        | ArrowDataType::Int16
        | ArrowDataType::Int32
        | ArrowDataType::Int64
        | ArrowDataType::UInt8
        | ArrowDataType::UInt16
        | ArrowDataType::UInt32
        | ArrowDataType::UInt64 => PublishedValues::Integer,
        _ => PublishedValues::Other,
    }
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use std::time::{Duration, UNIX_EPOCH};

    use datafusion::arrow::datatypes::TimeUnit as ArrowTimeUnit;
    use datafusion::physical_plan::ExecutionPlan;
    use datatypes::series::SeriesOrdering;
    use datatypes::series::samples::{FieldSpec, SeriesSchema};
    use promql::native::plan::{
        NativeExpr, NativeNode, NativeTree, fused_output_tags, reads_across_series,
    };
    use promql::native::segment::{cut_segments, region_local_segment};
    use session::context::QueryContextBuilder;

    use super::{
        ArrowDataType, FieldKind, METRIC_NAME, NATIVE_PROMQL_EXTENSION_KEY, NativeType,
        PromPlanner, aggregate_reads_histogram, collect_selectors_with_set_operations,
        fused_output_schema, independent_scan_pair, native_promql_requested,
        native_promql_resolved, native_raw_plan_bounds_region_reads, scan_time_filter,
    };

    /// The switch resolves from the two sources planning reads: what the query states (a request
    /// value over the session `SET` it inherits), and the engine default when it states nothing.
    /// The frontend reads the same resolver before a plan exists, so a query that never asks for
    /// the route is not bounded for discovery and one that does is.
    #[test]
    fn the_route_switch_resolves_over_the_engine_default() {
        const KEY: &str = "query.experimental_native_promql";
        for (engine_default, session, request, resolved) in [
            // Nothing is stated, so the engine default answers.
            (false, None, None, false),
            (true, None, None, true),
            // A session `SET` states it, and a request value outranks it in both directions.
            (false, Some("true"), None, true),
            (true, Some("false"), None, false),
            (false, Some("false"), Some("true"), true),
            (true, Some("true"), Some("false"), false),
            // The value is compared literally, as planning compares it.
            (false, None, Some("TRUE"), true),
        ] {
            let mut ctx = QueryContextBuilder::default().build();
            if let Some(value) = session {
                ctx.configuration_parameter()
                    .set_query_option(KEY, value)
                    .unwrap();
            }
            if let Some(value) = request {
                ctx.set_extension(KEY.to_string(), value.to_string());
            }
            assert_eq!(
                resolved,
                native_promql_resolved(&Arc::new(ctx), engine_default).unwrap(),
                "engine={engine_default}, session={session:?}, request={request:?}"
            );
        }
    }

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
            Some(FieldKind::Float),
        )
        .unwrap()
        .tree
    }

    #[test]
    fn aggregate_histogram_walk_descends_through_scalar_vector_wrappers() {
        let tree = compiled_test_tree("scalar(vector(scalar(sum(some_metric))))");
        assert_eq!(input_sources(tree.root()), vec![0]);
        assert!(aggregate_reads_histogram(tree.root(), FieldKind::Histogram));
        assert!(!aggregate_reads_histogram(tree.root(), FieldKind::Float));
    }

    fn input_sources(root: &NativeNode) -> Vec<usize> {
        root.inputs()
            .into_iter()
            .map(|input| input.source_index)
            .collect()
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
        assert!(collect_selectors_with_set_operations(
            &statement,
            &expr,
            false,
            &mut selectors,
            &mut Vec::new()
        ));
        selectors
            .iter()
            .map(|(selector, _)| selector.name.clone().unwrap_or_default())
            .collect()
    }

    /// The metric names the compiler binds selectors to, in the order it binds them.
    fn compiled_selector_order(query: &str) -> Vec<String> {
        let statement = promql_parser::parser::EvalStmt {
            expr: promql_parser::parser::parse(query).expect("the query parses"),
            start: UNIX_EPOCH,
            end: UNIX_EPOCH + Duration::from_secs(10),
            interval: Duration::from_secs(5),
            lookback_delta: Duration::from_secs(5),
        };
        let schema = Arc::new(SeriesSchema {
            fields: Arc::from([FieldSpec { name: "v0".into() }]),
            eval_grid: None,
        });
        let bound = std::cell::RefCell::new(Vec::new());
        crate::promql::native_compile::compile_selectors(
            &statement,
            &|requirement| {
                bound
                    .borrow_mut()
                    .push(requirement.metric.clone().unwrap_or_default());
                Ok(Arc::clone(&schema))
            },
            SeriesOrdering::KeyThenTime,
            Some(FieldKind::Float),
        )
        .expect("the compiler binds every selector of this query");
        bound.into_inner()
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
        // A parameter is an expression of its own, and the compiler compiles the input before it, so
        // a parameter that reads a selector is a second source bound after the input. The order here
        // is that binding order, which is the one the planner prepares its scans in.
        for (query, expected) in [
            ("topk(scalar(a), b)", vec!["b", "a"]),
            ("bottomk(scalar(a), b)", vec!["b", "a"]),
            // A literal parameter and a label parameter contribute no selector at all.
            ("topk(3, b)", vec!["b"]),
            ("count_values(\"x\", b)", vec!["b"]),
            (
                "histogram_quantile(scalar(a), sum by (le) (rate(h[5m])))",
                vec!["h", "a"],
            ),
            (
                "histogram_fraction(scalar(a), scalar(b), sum by (le) (rate(h[5m])))",
                vec!["h", "a", "b"],
            ),
        ] {
            assert_eq!(collected_selectors(query), expected, "{query}");
            // The walk's contract is the compiler's binding order, so the two are compared rather
            // than kept in step by hand.
            assert_eq!(
                collected_selectors(query),
                compiled_selector_order(query),
                "{query}: the walk and the compiler must bind the same selectors in the same order"
            );
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
            "x + on() x",
            "2 * (x > on() x)",
            "x + ignoring(host) x",
            "x * on(host) group_left x",
            "x * on(host) group_right x",
        ] {
            let tree = compiled_test_tree(query);
            assert_eq!(input_sources(tree.root()), vec![0, 1], "{query}");
            assert!(independent_scan_pair(tree.root()), "{query}");
        }
        assert!(!independent_scan_pair(
            compiled_test_tree("sum(x) + x").root()
        ));
    }

    #[test]
    fn independent_scan_pair_admits_nested_matches() {
        for query in [
            "x + y + z",
            "(x - y) / on(host) z",
            "x / on(host) y * on(host) z",
            "abs(x - y) + 2 * z",
            "(x and y) or z",
            "(x + y) unless on(host) z",
            "(x == on(host) y) + z",
            "x > bool y + z",
            "(x * on(host) group_left y) + z",
            "(x * on(host) group_right y) + z",
            "(x + on() y) + z",
            "(x > on() y) + z",
        ] {
            let tree = compiled_test_tree(query);
            assert_eq!(input_sources(tree.root()), vec![0, 1, 2], "{query}");
            assert!(independent_scan_pair(tree.root()), "{query}");
        }
        assert!(!independent_scan_pair(
            compiled_test_tree("x + y + sum(z)").root()
        ));
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
        assert_eq!(Some(vec![]), fused_output_tags(empty_on.root(), &sources));

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
        // `or` reports the series of both sides, so it reports each row's own name as well.
        assert_eq!(
            Some(vec![
                "host".to_string(),
                "kind".to_string(),
                "zone".to_string(),
                "__name__".to_string()
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
            fused_output_tags(group_left.root(), &group_sources)
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
            fused_output_tags(group_right.root(), &group_sources)
        );
        for query in [
            "x * on() group_left(workload) y",
            "x * on() group_right(container) y",
        ] {
            let tree = compiled_test_tree(query);
            let expected = if query.contains("group_left") {
                vec!["cluster", "namespace", "pod", "container", "workload"]
            } else {
                vec![
                    "cluster",
                    "namespace",
                    "pod",
                    "workload",
                    "workload_type",
                    "container",
                ]
            };
            let expected = Some(expected.into_iter().map(String::from).collect());
            assert_eq!(expected, fused_output_tags(tree.root(), &group_sources));
        }
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
                fused_output_tags(compiled_test_tree(query).root(), &sources),
                "{query}"
            );
        }
        // An aggregate reports the grouping labels its input carries: `by` keeps exactly those, and `without`
        // keeps the rest and drops the metric name. `x` is bound to the three tag columns below, so
        // `without (instance)` keeps all three: `instance` is not one of them.
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
                fused_output_tags(compiled_test_tree(query).root(), &hostname_source),
                "{query}"
            );
        }
        let absent = compiled_test_tree("absent(x{host=\"a\", job=\"b\"})");
        assert_eq!(
            Some(vec!["host".to_string(), "job".to_string()]),
            fused_output_tags(absent.root(), &sources)
        );
        // A per-point `k` keeps the input's labels too: it selects the same series the bound form
        // selects, so it reports the same set. Its parameter reads a source of its own, so this tree
        // binds two.
        assert_eq!(
            Some(vec!["hostname".to_string()]),
            fused_output_tags(
                compiled_test_tree("topk(scalar(x), sum by (hostname) (rate(x[5m])))").root(),
                &[vec!["hostname".to_string()], vec!["hostname".to_string()]],
            ),
            "a per-point k reports its input's labels"
        );
        let count_values =
            compiled_test_tree("count_values by (host) (\"sample\", sum by (host) (x))");
        assert_eq!(
            Some(vec!["host".to_string(), "sample".to_string()]),
            fused_output_tags(count_values.root(), &sources)
        );
        // A destination already present in the grouping is reported once, not duplicated.
        let count_values = compiled_test_tree("count_values by (host) (\"host\", x)");
        assert_eq!(
            Some(vec!["host".to_string()]),
            fused_output_tags(count_values.root(), &sources)
        );
        // Selecting some series keeps each one's own labels, whatever the grouping was.
        let topk = compiled_test_tree("topk(1, x)");
        assert_eq!(
            Some(sources[0].clone()),
            fused_output_tags(topk.root(), &sources),
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
                fused_output_tags(rewrite.root(), &sources),
                "{query}"
            );
        }
    }

    /// Pins the placement flip the name-preserving window unification caused. The inner window of a
    /// subquery over an evaluated selector now answers `Some(true)`, so the outer window does not
    /// read across series and both are region-local. While the inner window answered `Some(false)`
    /// the outer one crossed series and was placed on the frontend above the `MergeScan`; a
    /// regression in that unification shows up here as a frontend placement.
    #[test]
    fn nested_name_keeping_windows_are_region_local() {
        let tree = compiled_test_tree("max_over_time(last_over_time(some_metric[5m:1m])[10m:1m])");
        assert!(
            !reads_across_series(tree.root()),
            "nested name-keeping windows answer one series per label set"
        );
        assert!(
            region_local_segment(tree.root()),
            "the dist analyzer must place the nested windows region-side"
        );
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
            fused_output_tags(tree.root(), &sources)
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
            fused_output_tags(sum.root(), &sources)
        );

        let count_values = compiled_test_tree("count_values without (instance) (\"sample\", x)");
        assert_eq!(
            Some(vec!["job".to_string(), "sample".to_string()]),
            fused_output_tags(count_values.root(), &sources)
        );

        let count_values =
            compiled_test_tree("count_values without (sample, instance) (\"sample\", x)");
        assert_eq!(
            Some(vec!["job".to_string()]),
            fused_output_tags(count_values.root(), &sources)
        );
    }

    /// A rows layout declares the per-row name column, and only a rows layout does.
    ///
    /// The walk is the authority on the name's values - it keeps it exactly where the operator keeps
    /// it - so the column it is read from has to exist whether the walk kept the name or dropped it.
    #[test]
    fn rows_layouts_declare_the_name_column() {
        let one = || vec![vec!["host".to_string()]];
        let two = || vec![vec!["host".to_string()], vec!["host".to_string()]];
        for (query, sources) in [
            ("x", one()),
            ("abs(x)", one()),
            ("rate(x[5m])", one()),
            ("sum(x)", one()),
            // A set operation binds one source per occurrence.
            ("x or y", two()),
        ] {
            let tree = compiled_test_tree(query);
            let schema =
                PromPlanner::fused_root_schema(query, &tree, &sources, "ts", FieldKind::Float)
                    .unwrap()
                    .unwrap_or_else(|| panic!("{query}: a samples root declares a rows layout"));
            let names = schema
                .fields()
                .iter()
                .map(|field| field.name().as_str())
                .collect::<Vec<_>>();
            assert_eq!(Some(&METRIC_NAME), names.last(), "{query}: {names:?}");
            assert_eq!(
                1,
                names.iter().filter(|name| **name == METRIC_NAME).count(),
                "{query}: one name column: {names:?}"
            );
            let name = schema.field_with_unqualified_name(METRIC_NAME).unwrap();
            assert!(
                datatypes::schema::is_metric_name_field(name),
                "{query}: the column is the layout's name column"
            );
            assert_eq!(ArrowDataType::Utf8, *name.data_type(), "{query}");
        }
        let scalar = compiled_test_tree("1 + 2");
        let schema = PromPlanner::fused_root_schema("1 + 2", &scalar, &[], "ts", FieldKind::Float)
            .unwrap()
            .unwrap();
        assert_eq!(
            // A scalar root declares no name column, and its one value is named after the
            // expression, as every other rows layout names its values.
            vec!["ts", "1 + 2"],
            schema
                .fields()
                .iter()
                .map(|field| field.name().as_str())
                .collect::<Vec<_>>()
        );
    }

    /// A label set that already names the metric keeps the one column it has.
    #[test]
    fn a_present_name_tag_is_not_declared_twice() {
        let sources = [vec!["host".to_string(), METRIC_NAME.to_string()]];
        let tree = compiled_test_tree("x");
        let schema = PromPlanner::fused_root_schema("x", &tree, &sources, "ts", FieldKind::Float)
            .unwrap()
            .unwrap();
        assert_eq!(
            vec!["ts", "x", "host", METRIC_NAME],
            schema
                .fields()
                .iter()
                .map(|field| field.name().as_str())
                .collect::<Vec<_>>()
        );
    }

    /// The value columns are named after the expression, so a tree whose own field is named
    /// `__name__` still publishes that value under the expression's name - the name column is the
    /// layout's own. A time index of that name collides with it, and the collision is reported
    /// rather than published as a layout the reader cannot address.
    #[test]
    fn a_time_index_named_after_the_metric_name_is_reported() {
        let sources = [vec!["host".to_string()]];
        // The field is renamed by the expression, so it no longer stands where the name column is.
        let tree = compiled_test_tree_with_fields("x", &[METRIC_NAME]);
        let schema = PromPlanner::fused_root_schema("x", &tree, &sources, "ts", FieldKind::Float)
            .unwrap()
            .unwrap_or_else(|| panic!("the value is named after the expression"));
        assert_eq!(
            vec!["ts", "x", "host", METRIC_NAME],
            schema
                .fields()
                .iter()
                .map(|field| field.name().as_str())
                .collect::<Vec<_>>()
        );
        // A time index of that name collides with the row-name column, and the error names it.
        let tree = compiled_test_tree("x");
        let error =
            PromPlanner::fused_root_schema("x", &tree, &sources, METRIC_NAME, FieldKind::Float)
                .expect_err("a time index named after the metric collides with the name column");
        let message = error.to_string().to_lowercase();
        assert!(message.contains("duplicate"), "{message}");
        assert!(message.contains(METRIC_NAME), "{message}");
    }

    /// A `__name__` rewrite publishes a name column, canonicalizes the complete runtime label set,
    /// and leaves the name dropped wherever a kernel computes a value of its own.
    #[test]
    fn name_rewrite_publishes_a_column_and_is_canonicalized() {
        let columns = [vec!["host".to_string(), "kind".to_string()]];
        let runtime = [vec![
            "host".to_string(),
            "kind".to_string(),
            METRIC_NAME.to_string(),
        ]];
        let tree =
            compiled_test_tree(r#"label_replace(x, "__name__", "same", "__name__", "(.*)")"#);
        assert_eq!(
            Some(vec!["host".into(), "kind".into(), METRIC_NAME.into()]),
            fused_output_tags(tree.root(), &columns),
            "a written name is a column the output publishes"
        );
        let collected = promql::native::canonicalize::merge_label_rewrites(&tree, &runtime, &[])
            .expect("the pass produces a legal tree");
        assert!(
            matches!(collected.root().expr, NativeExpr::Merge { .. }),
            "a name rewrite writes an existing runtime label: {collected}"
        );
        assert_eq!(
            fused_output_tags(tree.root(), &columns),
            fused_output_tags(collected.root(), &columns),
            "collecting the pieces does not change the columns"
        );

        // A delete-only rewrite over a scan whose columns carry no name publishes none.
        let deleted = compiled_test_tree(r#"label_replace(x, "__name__", "", "__name__", "(.*)")"#);
        assert_eq!(
            Some(vec!["host".into(), "kind".into()]),
            fused_output_tags(deleted.root(), &columns)
        );

        // A kernel that computes a value of its own drops the name a rewrite below it wrote.
        for query in [
            r#"abs(label_replace(x, "__name__", "same", "__name__", "(.*)"))"#,
            r#"histogram_count(label_replace(x, "__name__", "same", "__name__", "(.*)"))"#,
            r#"timestamp(label_replace(x, "__name__", "same", "__name__", "(.*)"))"#,
            r#"sum(label_replace(x, "__name__", "same", "__name__", "(.*)"))"#,
        ] {
            let tree = compiled_test_tree(query);
            let tags = fused_output_tags(tree.root(), &columns)
                .unwrap_or_else(|| panic!("{query} derives its columns"));
            assert!(
                !tags.iter().any(|tag| tag == METRIC_NAME),
                "{query}: {tags:?}"
            );
        }
    }

    /// A name write is collected even above a kernel that dropped the name: the scan below it can
    /// still hold several metrics whose ordinary labels are equal, so writing one name can fold them
    /// into one identity. The output still publishes the written name.
    #[test]
    fn a_name_rewrite_above_a_dropped_name_is_collected() {
        let runtime = [vec![
            "host".to_string(),
            "kind".to_string(),
            METRIC_NAME.to_string(),
        ]];
        let tree =
            compiled_test_tree(r#"label_replace(abs(x), "__name__", "same", "missing", "(.*)")"#);
        let collected = promql::native::canonicalize::merge_label_rewrites(&tree, &runtime, &[])
            .expect("the pass produces a legal tree");
        assert!(
            matches!(collected.root().expr, NativeExpr::Merge { .. }),
            "the merge sits above the name rewrite: {collected}"
        );
        assert_eq!(
            2,
            collected.to_string().matches("Merge").count(),
            "the name rewrite and the name drop below it are each collected: {collected}"
        );
        // The rewrite writes a name over a dropped one, so the output publishes a name column.
        assert_eq!(
            Some(vec!["host".into(), "kind".into(), METRIC_NAME.into()]),
            fused_output_tags(collected.root(), &[vec!["host".into(), "kind".into()]])
        );

        // A fresh ordinary destination above the same kernel is still uncollected: no scan can carry
        // an ordinary label it does not declare.
        let fresh = compiled_test_tree(r#"label_replace(abs(x), "dst", "$1", "host", "(.*)")"#);
        let collected = promql::native::canonicalize::merge_label_rewrites(&fresh, &runtime, &[])
            .expect("the pass produces a legal tree");
        assert_eq!(
            1,
            collected.to_string().matches("Merge").count(),
            "the fresh ordinary destination adds no collection of its own, and `abs` keeps the one \
             its name drop needs: {collected}"
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
            fused_output_tags(tree.root(), &sources)
        );
    }

    /// `group_left(__name__)` copies the one side's name onto an arithmetic result: the operation
    /// drops the name before it copies an included label, so the copied name is a column. A one side
    /// without a name cannot provide one, so the column leaves the set.
    #[test]
    fn an_included_metric_name_is_an_output_column() {
        let sources = [
            vec!["host".to_string()],
            vec!["host".to_string(), METRIC_NAME.to_string()],
        ];
        let included = compiled_test_tree(
            r#"x * on(host) group_left(__name__) label_replace(y, "__name__", "renamed", "__name__", "(.*)")"#,
        );
        assert_eq!(
            Some(vec!["host".to_string(), METRIC_NAME.to_string()]),
            fused_output_tags(included.root(), &sources)
        );
        let unnamed = compiled_test_tree(r#"x * on(host) group_left(__name__) y"#);
        assert_eq!(
            Some(vec!["host".to_string()]),
            fused_output_tags(
                unnamed.root(),
                &[vec!["host".to_string()], vec!["host".to_string()]]
            )
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
            let tags = fused_output_tags(tree.root(), &sources).unwrap();
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
        let expression = "sum by (zone, host) ((x))";
        let output = fused_output_schema(
            expression,
            &["v0".to_string(), "v1".to_string()],
            &["zone".to_string(), "host".to_string()],
            "ts",
            FieldKind::Float,
        )
        .unwrap();
        assert_eq!(
            vec![
                "ts".to_string(),
                format!("{expression}.v0"),
                format!("{expression}.v1"),
                "zone".to_string(),
                "host".to_string(),
            ],
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
            fused_output_tags(tree.root(), &sources)
        );
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
    fn native_promql_requested_rejects_false_values() {
        for value in ["false", "FALSE", "False"] {
            let query_ctx = Arc::new(
                QueryContextBuilder::default()
                    .set_extension(NATIVE_PROMQL_EXTENSION_KEY.to_string(), value.to_string())
                    .build(),
            );

            assert!(!native_promql_requested(&query_ctx), "value: {value:?}");
        }
    }
}
