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
//! [`PromPlanner::try_plan_native_fused`] offers the whole statement to the native compiler and,
//! when it is accepted, plans it as [`FusedSeries`] nodes over the selectors' table scans.
//! For splittable aggregates, a `Partial` producer sends state through `SeriesRepartitionExec`
//! to a `Final` consumer. Aggregates without mergeable state, such as `quantile`, can still be
//! planned over segments. A statement the native route does not answer reports the shape it
//! refuses rather than being planned some other way.
//!
//! The row output schema is the time index, then the value fields, then the tags: the same shape a
//! `SeriesNormalize` output has. A statement that reads no data is planned as one node without a
//! scan.

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
// Re-exported for the dist planner's commutativity check, `crate::planner` and tests.
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
    InfoInput, Substitution, bind_evaluated_binary, compile_selectors_with_grid,
    compile_selectors_with_grid_and_substitution, compile_statement_with_grid, info_input,
    pointwise_vector_argument, statement_grid, subquery_child_grid,
};
use crate::promql::planner::{
    DEFAULT_FIELD_COLUMN, DEFAULT_TIME_INDEX_COLUMN, GrownStack, PromPlanner,
};
use crate::query_engine::QueryEngineState;

/// The query option that turns the native fused-series route on for one query.
///
/// Set per request (HTTP hints header, gRPC context key) or per session
/// (`SET query.experimental_native_promql = ...`); the engine-wide
/// [`QueryOptions::experimental_native_promql`] is the default. The planner reads the resolved
/// [`NativePromqlExtension`](crate::options::NativePromqlExtension), not this key.
///
/// [`QueryOptions::experimental_native_promql`]: crate::options::QueryOptions::experimental_native_promql
pub const NATIVE_PROMQL_EXTENSION_KEY: &str = "query.experimental_native_promql";

/// Whether the query options of one query ask for the native fused-series route.
///
/// The snapshot only holds validated booleans, so a case-insensitive `true` comparison suffices.
pub(crate) fn native_promql_requested(query_ctx: &QueryContextRef) -> bool {
    query_ctx
        .extension(NATIVE_PROMQL_EXTENSION_KEY)
        .is_some_and(|value| value.eq_ignore_ascii_case("true"))
}

/// Whether one query asks for the native fused-series route: the value in
/// [`QueryContext::query_option_snapshot`] if stated, otherwise `engine_default`.
///
/// For callers that decide before planning, e.g. the frontend scoping metric discovery.
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

/// Whether `plan` is a native raw PromQL plan whose region reads can share one query-wide body
/// budget without deadlock.
///
/// That holds for a chain of [`FusedSeriesExec`] nodes: each drains its inputs while polling, so a
/// region body holding a permit keeps progressing. Any other multi-input node disqualifies the
/// plan.
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

/// One prepared occurrence of a selector.
type PreparedInput = (
    // scan
    LogicalPlan,
    // raw-series layout
    Arc<SeriesSchema>,
    // metric name
    Option<Arc<str>>,
    // time index column
    String,
    // tag columns
    Vec<String>,
    // `partitioned`: the table is split into regions along a partition column.
    bool,
    // `split_series`: a partition column is not a tag, so one series can span regions.
    bool,
    // `keys_survive`: every primary-key column always decodes a value (no `NULL` vs `''`
    // ambiguity after a metadata drop); see `keys_survive_metadata_drop`.
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

/// A metadata drop is unproved because a key column can hold `NULL` in one series and `''` in
/// another, which the drop merges into one label set.
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
        // Decides whether the existing plan reports a numeric field as `Float64`.
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
        // The raw series scan has no equivalent of the `__field__` projection.
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
        // Built by the existing path's method so both paths read the same columns.
        let table_ref = self.table_ref()?;
        let mut input = self.create_table_scan_plan(table_ref).await?;
        let logical_scan = metric_scan_and_table_id(&input);
        let has_verified_metric_rewrite = logical_scan.is_some();
        // Two logical tables over one physical table scan the same table; only their ids tell the
        // metrics apart.
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
        let partitioned = table_meta.partition_column_names().next().is_some();
        // When false, a metadata drop over this table is unproved and only counted
        // (`UNPROVED_LABELSET_CHECK_NULLABLE_KEY`); nothing is collected for it.
        let keys_survive = keys_survive_metadata_drop(
            tags.iter().map(String::as_str),
            table_meta
                .primary_key_indices
                .iter()
                .map(|index| &table_meta.schema.column_schemas()[*index]),
        );
        // Checked here because a physical-planning refusal is an error, not a fallback. A
        // file-engine table has no raw-series scanner; a metric-engine selector needs its verified
        // logical rewrite.
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
        // Admitted: such inputs are read through a raw source, which reconciles the fragments.
        let split_series = !table_meta
            .partition_column_names()
            .all(|column| tags.iter().any(|tag| tag == column));
        // Every field must read as one kind: `Float64` or the native histogram struct. The raw scan
        // casts numeric fields to `Float64`, but the existing plan keeps a numeric field's own type
        // for shapes that do not cast (a bare selector, `sum` of integers, `count`); those are
        // refused so the output column type does not change.
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
        // The time filter is added after compiling, once the occurrence's window is known.
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

    /// One native attempt at `prom_expr` in the requested output format.
    ///
    /// Offers the whole expression first, then a binary composition of two frames. Restores the
    /// planner context afterwards, so a refused attempt leaves nothing behind.
    ///
    /// Boxed because it recurses through [`Self::try_plan_native_composed_binary`], and polled
    /// through [`GrownStack`], which grows the stack for that recursion.
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
    /// Each operand of a binary expression is planned as an evaluated series frame. Refuses when
    /// the grids differ or the kernels cannot answer the matching or output kind.
    async fn try_plan_native_composed_binary(
        &mut self,
        expr: &PromExpr,
        query_engine_state: &QueryEngineState,
        format: FusedOutputFormat,
    ) -> Result<Option<NativePlanned>> {
        // No refusal recorded: the whole-expression attempt already named the cause.
        if !self.native_fused
            || self.ctx.start != self.ctx.stmt_start
            || self.ctx.end != self.ctx.stmt_end
        {
            return Ok(None);
        }
        // The statement whose own AST the wrappers and the binary are read from. A compile below
        // substitutes the composed node for one node of this AST, so the node it names must be the
        // node that compile walks.
        let Some(stmt) = self.native_statement(expr) else {
            return Ok(None);
        };
        // A function this compiler binds pointwise over its one vector - `ceil(x + y)` - applies to
        // the composition's answer, so the binary is composed and the functions above it are
        // compiled once more over that answer.
        let Some((wrapped, wrappers)) = wrapped_binary(&stmt.expr) else {
            return Ok(None);
        };
        let PromExpr::Binary(binary) = wrapped else {
            return Ok(None);
        };
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
        let Some(binary_kind) = binary_output_field_kind(
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
        // The functions above the binary apply to the answer it decides, so they are compiled once
        // more with that answer in the binary's place.
        let (tree, root_kind) = if wrappers == 0 {
            (tree, binary_kind)
        } else {
            let substitution = Substitution {
                target: wrapped,
                node: tree.root().clone(),
            };
            let compiled = match compile_selectors_with_grid_and_substitution(
                &stmt,
                // Every function above the binary reads the vector it wraps and nothing else, so a
                // selector of its own is a shape this composition does not answer.
                &|requirement| {
                    Err(DataFusionError::NotImplemented(format!(
                        "the selector `{}` beside the composed operands of `{expr}`",
                        requirement.metric.clone().unwrap_or_default()
                    )))
                },
                NATIVE_ORDERING,
                Arc::clone(&left_grid),
                Some(binary_kind),
                &substitution,
            ) {
                Ok(compiled) => compiled.tree,
                Err(DataFusionError::NotImplemented(_)) => {
                    return unsupported("wrapped_binary_is_not_pointwise");
                }
                Err(error) => return Err(error).context(DataFusionPlanningSnafu),
            };
            let Some(root_kind) = compiled.root().output_field_kind(Some(binary_kind)) else {
                return unsupported("wrapped_binary_has_no_output_kind");
            };
            (compiled, root_kind)
        };
        if !native_tree_supported(&tree) {
            return unsupported("runtime_refuses_the_composed_tree");
        }
        let expression = expr.to_string();
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
    /// The expression text names the value columns. Table, time index, tag and field columns come
    /// from the planner's resolved selector context.
    async fn try_plan_native_fused_with_context(
        &mut self,
        prom_expr: &PromExpr,
        query_engine_state: &QueryEngineState,
        format: FusedOutputFormat,
    ) -> Result<NativePlanned> {
        if !self.native_fused {
            return unsupported("native_route_not_requested");
        }
        // The whole expression, even when a subquery root compiles only its inner one.
        let expression = prom_expr.to_string();
        // A matrix root is planned only as an instant-query subquery, on its child grid.
        let subquery_root = root_subquery(prom_expr);
        if prom_expr.value_type() == ValueType::Matrix
            && (subquery_root.is_none() || self.ctx.start != self.ctx.end)
        {
            return unsupported("matrix_root_without_a_grid");
        }

        let Some(stmt) = self.native_statement(prom_expr) else {
            return unsupported("statement_has_no_evaluation_grid");
        };
        // `@ start()` and `@ end()` inside the subquery still resolve against the statement's
        // bounds.
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
        // Each selector records whether `rate` or `increase` reads it, so `rate(a) / b` stays
        // admissible when only `b` has delta temporality.
        let mut selectors = Vec::new();
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
        let Ok(statement) = statement_grid(&stmt) else {
            return unsupported("statement_grid_unavailable");
        };
        // Every lowering below publishes on this grid, the child grid for a subquery root.
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
            // A scalar root has no labels to match, so it cannot be a frame operand.
            let evaluated = matches!(
                &tree.root().output,
                NativeType::Samples(schema) if schema.eval_grid.is_some()
            );
            if format == FusedOutputFormat::SeriesFrame && !evaluated {
                return unsupported("frame_operand_is_not_evaluated");
            }
            let Some(output_tags) = fused_output_tags(tree.root(), &[]) else {
                return unsupported("data_free_root_has_no_labels");
            };

            let Some(values) = native_root_fields(tree.root()) else {
                return unsupported("data_free_root_has_no_value_columns");
            };
            let value_names = expression_field_names(&expression, &values);
            let time_index = free_time_index(DEFAULT_TIME_INDEX_COLUMN, &value_names, &output_tags);
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
            let mut logical_scan_id: Option<u32> = None;
            let mut logical_ids_differ = false;
            // Selectors must agree on one field kind.
            let mut statement_kind: Option<FieldKind> = None;
            for (selector, under_rate_or_increase) in &selectors {
                self.ctx = original_context.clone();
                if !selector.matchers.or_matchers.is_empty() {
                    return unsupported("or_matchers_have_no_single_raw_scan");
                }
                // A nameless selector enumerates the schema's logical metric tables, one scan each,
                // unioned into one input.
                if discovers_metric_name(selector) {
                    let name_matchers = selector.matchers.find_matchers(METRIC_NAME);
                    let mut ordinary_matchers = selector.matchers.clone();
                    ordinary_matchers
                        .matchers
                        .retain(|matcher| matcher.name != METRIC_NAME);
                    // Name matchers filter table names, not rows. The placeholder name keeps
                    // preprocessing from failing with `NoMetricMatcher`.
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
                    // A table the caller may not see never becomes a source, so its name reaches
                    // neither the plan nor an error.
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
                        // Planned as an empty input: the existing planner would raise
                        // `TableNameNotFound`. Its fields come from the `__field__` matcher; a
                        // matcher that needs table columns to resolve is refused.
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
                    let mut sources = Vec::with_capacity(metrics.len());
                    let mut selector_metadata: Option<DiscoveredSelectorMetadata> = None;
                    let mut discovered_partitioned = false;
                    let mut discovered_split_series = false;
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
                        let Some(metric) = metric else {
                            return unsupported("discovered_selector_names_no_metric");
                        };
                        // `__name__` is part of a discovered series' identity: it separates metrics
                        // with equal labels.
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
                    // A missing table answers the empty vector, as the existing planner's
                    // `EmptyMetric` does, so a `__field__` matcher does not refuse here. The
                    // placeholder field is never published.
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
        // `absent_over_time` keeps `__name__`, which a bare scan takes from the binding's metric. A
        // nameless input that is neither sourced nor empty has no name to bind.
        if reads_presence(prom_expr)
            && prepared.iter().any(|(scan, _, metric, ..)| {
                !is_empty_discovered_input(scan) && metric.is_none() && !is_sourced_plan(scan)
            })
        {
            return unsupported("presence_over_a_nameless_selector");
        }
        // Scans over more than one table (or more than one metric logical table id) are read
        // through their own sources and the node runs above the regions. Empty inputs read no
        // table.
        let cross_table = !same_table_scans(
            prepared
                .iter()
                .map(|(scan, ..)| scan)
                .filter(|scan| !is_empty_discovered_input(scan)),
        ) || logical_ids_differ;
        // An empty selector takes its field layout from the first real selector of the other
        // operand of its enclosing set operation, since set operators pair fields by position.
        // Otherwise it takes the statement's first real layout, or keeps the default when every
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
        // The compiler binds selectors in the same pre-order as
        // `collect_selectors_with_set_operations`; a mismatch is an internal error.
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
                    // The raw anchor, not anchor minus offset, as the compiler records it.
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
            // An aggregate's state depends on it: a `sum` over histograms is not a float sum.
            statement_kind,
        ) {
            Ok(v) => v,
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
                // Discovered metrics may name their time columns differently, so each source is
                // bounded on its own column.
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
            } else {
                return Err(DataFusionError::Internal(format!(
                    "the child at {index} is not the table scan this route prepared"
                )))
                .context(DataFusionPlanningSnafu);
            }
        }
        let multi_input = selectors.len() > 1;
        // The tags each input declares for the output layout. With several inputs, `__name__` is
        // declared so a grouping keeps two metrics apart; a single input only gets it appended by
        // `with_row_name_column`.
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
        // Each input's runtime label set, `__name__` included for a metric-bound scan.
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
        // The one metric each input is bound to, `None` for a multi-metric discovered selector. The
        // canonical pass uses it to decide whether dropping `__name__` can merge series.
        let runtime_source_metrics = prepared
            .iter()
            .map(|(_, _, metric, _, _, _, _, _)| metric.clone())
            .collect::<Vec<_>>();
        let keys_survive = prepared.iter().all(|input| input.7);
        let any_partitioned = prepared.iter().any(|input| input.5);
        // Inserts a `NativeExpr::Merge` where a label rewrite can give two series one label set.
        let tree = match promql::native::canonicalize::merge_label_rewrites(
            &compiled.tree,
            &runtime_source_tags,
            &runtime_source_metrics,
        ) {
            Ok(tree) => tree,
            Err(_) => return unsupported("label_rewrites_do_not_merge"),
        };
        // A metadata drop over keys that can hold `NULL` or `''` is not collected, only counted.
        let unproved_keys = promql::native::plan::leaves_a_drop_to_the_storage_keys(
            &tree,
            &runtime_source_tags,
            &runtime_source_metrics,
            keys_survive,
        )
        .then_some(UNPROVED_LABELSET_CHECK_NULLABLE_KEY);
        let root = tree.root();
        let per_partition = region_local_segment(root);
        let locality = input_locality(root);
        let computed_key_case =
            matches!(locality, InputLocality::SinglePartition) && matching_operands_drop_key(root);
        let required_placement = !per_partition || computed_key_case;
        // Each occurrence reads through its own `RawSeriesSource`, with the node above the regions,
        // when the scans span tables or the planner is distributed. With several occurrences, a
        // partitioned table, work that is not per-partition, or an empty frame does the same
        // (physical planning accepts all frames or all bare scans, not a mix). A canonical merge
        // over a partitioned table needs every region's pieces, so it forces sources for one
        // selector too.
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
                if is_empty_discovered_input(&scan) {
                    return Ok(scan);
                }
                // A split series always needs a raw source to reconcile its fragments, even when
                // the plan is not split over regions.
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
        // The root's kind, which every output column follows; it can differ from the inputs'
        // (`count_over_time(h[5m])` is a float).
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
            // A frame carries no name column of its own.
            let Some(output_tags) = fused_output_tags(root, &source_tags) else {
                return unsupported("root_has_no_output_labels");
            };
            Some((root.output.clone(), output_tags))
        } else {
            None
        };
        // Neither the aggregate split nor a node over several sources can fold histogram state. The
        // kind entering each aggregate decides, not the root's shape: `sum(h) * 2` has a binary
        // root.
        if let Some(kind @ FieldKind::Histogram) = statement_kind
            && ((multi_input && per_source_inputs) || aggregate_reads_histogram(root, kind))
        {
            return unsupported("histogram_aggregate_needs_state_this_route_lacks");
        }
        // A node over several inputs reads their partitions in arrival order.
        if multi_input && depends_on_input_order(root) {
            return unsupported("answer_depends_on_input_order");
        }
        // Physical planning cannot fall back, so runtime shape refusals are checked here for plans
        // that commit to one fused node.
        if (multi_input || per_source_inputs) && !native_tree_supported(&tree) {
            return unsupported("runtime_refuses_the_tree");
        }
        // A root canonical merge keeps the scan width: each partition merges its pieces and a
        // canonical router collects each label set across partitions.
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
            // Prefer the aggregate split, which exports state rather than samples; segmentation
            // below is the fallback.
            let split = if selectors.len() == 1 {
                FusedSeries::split_aggregate(&tree, &time).context(DataFusionPlanningSnafu)?
            } else {
                None
            };
            if let Some((partial, final_tree)) = split {
                // A histogram answer is admitted only when the cut aggregate exports histogram
                // state (`sum`/`avg` over native histograms).
                let histogram_state = split_exports_histogram_state(&partial);
                if root_kind != FieldKind::Float
                    && !(root_kind == FieldKind::Histogram && histogram_state)
                {
                    return unsupported("split_aggregate_folds_float_state_only");
                }
                // Built from the complete tree, since the partial stage may add labels. The split
                // is validated against the row layout in either format.
                let Some(value_schema) =
                    Self::fused_root_schema(&expression, &tree, &source_tags, &time, root_kind)?
                else {
                    return unsupported("root_has_no_value_columns");
                };
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
            // A segment reads assembled frames, and no histogram fold here runs over those; every
            // histogram fold reads its own scan.
            if binds_histogram_state_anywhere(root) {
                return unsupported("histogram_state_above_a_segment_boundary");
            }
        }
        let output_tags = match &tree.root().output {
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
        // With only empty inputs, the time index is the synthetic `time` column
        // (`setup_context_for_empty_metric`); a value of the same name moves it to the first free
        // `native_time_N`. A value colliding with a tag has no free name to take.
        let time = if named_by_a_real_input {
            time
        } else {
            let value_names = expression_field_names(&expression, &root_fields);
            if value_names
                .iter()
                .any(|name| output_tags.iter().any(|tag| tag == name))
            {
                // No table exists, so no row ever carries this layout, and the statement answers
                // the empty vector whichever column it would name: `__name__` names both the metric
                // - and so the value column - and the row-name column every samples layout
                // publishes, and neither can move. The empty input a selector over a missing table
                // is planned as is that answer, so the statement is answered by it rather than
                // refused. A statement that reads a table keeps the refusal: there the layout is a
                // row's.
                if format == FusedOutputFormat::PlainRows
                    && children.iter().all(is_empty_discovered_input)
                    && let Some(empty) = children.first()
                {
                    return Ok(NativePlanned {
                        kind: NativePlannedKind::Rows(empty.clone()),
                        unproved_keys,
                    });
                }
                return unsupported(UNSUPPORTED_SHAPE_UNNAMED);
            }
            free_time_index(&time, &value_names, &output_tags)
        };

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
                // Cuts keep the raw source when a region holds only a fragment of a series, so a
                // window never evaluates over one region's share.
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
            // One scan does not prove its region holds every contribution a cross-series consumer
            // needs.
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
        // `RawSeriesSource` children carry their own metric and labels.
        let (tags, metric) = if per_source_inputs {
            (Vec::new(), None)
        } else {
            (tags, metric)
        };
        let publish_grid = Arc::clone(&grid);
        let node = FusedSeries::try_new_with_format(
            Arc::from(expression.as_str()),
            tree,
            FusedSeriesBinding {
                grid: publish_grid,
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
    /// `Ok(None)` when the root is not a value the node can publish or its tags cannot be derived.
    /// Duplicate column names are an error, not a refusal.
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
        // `root_kind` comes from the uncut tree: after segmentation an input leaf no longer shows
        // that `count_over_time(h[5m])` turned histograms into floats.
        fused_output_schema(expression, &root_fields, &output_tags, time, root_kind).map(Some)
    }

    /// The rows layout a segmented statement root publishes, under the segment builders' callback.
    ///
    /// Segments cannot carry this crate's error type, so an undeclarable layout is a refusal.
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
    /// `None` when the bounds are not representable as a [`SystemTime`].
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
/// An empty raw-series frame with explicit labels, no metric name, and `schema`'s fields. The
/// planner may later re-lay it out to match a set-operation sibling.
fn empty_discovered_input(time_index: String, schema: Arc<SeriesSchema>) -> Result<PreparedInput> {
    let input = empty_discovered_frame(&schema)?;
    // No table, so not partitioned, and no key metadata proves `keys_survive`.
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

/// The raw series schema of an [`empty_discovered_input`] declaring `fields`.
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
/// The fields named by `=` `__field__` matchers, or the default field without a matcher. `None` for
/// `!=`, `=~` or `!~`, which need a table's columns to resolve.
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

/// Whether a prepared selector is already a discovered [`RawSeriesSource`] or a `Union` of them,
/// which must not be wrapped in another source.
fn is_sourced_plan(plan: &LogicalPlan) -> bool {
    matches!(plan, LogicalPlan::Union(_))
        || matches!(
            plan,
            LogicalPlan::Extension(extension)
                if extension.node.as_any().is::<RawSeriesSource>()
        )
}

/// `millis` from the Unix epoch, negative included; `None` when [`SystemTime`] cannot represent it.
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

/// The binary a chain of pointwise functions wraps, and how many functions wrap it.
///
/// Only the calls this compiler binds as one map over the vector they read - a math function, a
/// native histogram function, a label function, `round`/`clamp` with a parameter that is read per
/// point, or `timestamp` - are looked through: each answers its one vector point by point, so the
/// tree compiled over the answer of a composed binary is the tree the whole expression means. A
/// call that reads its vector some other way - an aggregate folds a group, a vector operation
/// reorders or drops series, a window rewinds time - stops the walk, and the statement is offered
/// to the whole-expression compile alone.
fn wrapped_binary(expr: &PromExpr) -> Option<(&PromExpr, usize)> {
    let mut inner = expr;
    let mut wrappers = 0;
    loop {
        inner = match inner {
            PromExpr::Paren(paren) => &paren.expr,
            PromExpr::Binary(_) => return Some((inner, wrappers)),
            PromExpr::Call(call) => {
                wrappers += 1;
                pointwise_vector_argument(call)?
            }
            _ => return None,
        };
    }
}

/// The native root's value field names.
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
/// One value is named after the expression; several are named `<expression>.<field>`.
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
/// Used only for synthetic time columns (data-free trees and statements over empty inputs); on a
/// collision the grid moves to the first free `native_time_N`.
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

/// The columns a [`FusedSeries`] node produces: its time index, one column per value, then its
/// tags.
///
/// Values are named by [`expression_field_names`]; the layout is
/// [`segment::fused_output_schema`]'s. Errors with the colliding names when a column name repeats.
fn fused_output_schema(
    expression: &str,
    fields: &[String],
    tags: &[String],
    time_index: &str,
    field_kind: FieldKind,
) -> Result<DFSchemaRef> {
    let value_names = expression_field_names(expression, fields);
    // Detected here because `DFSchema`'s own error does not name the columns.
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
/// A state-exporting [`FusedSeries`] runs the per-series prefix and the `Partial` aggregate over
/// `rawinput`; the returned node runs the `Final` aggregate and the suffix in `format`. The
/// physical planner puts a `SeriesRepartitionExec` between them. `value_schema`'s value columns
/// must match the partial state's accumulator count.
fn plan_split_aggregate(
    promql: Arc<str>,
    partial: NativeTree,
    final_tree: NativeTree,
    binding: FusedSeriesBinding,
    rawinput: LogicalPlan,
    value_schema: DFSchemaRef,
    format: FusedOutputFormat,
) -> Result<FusedSeries> {
    let state_accumulators = match &partial.root().output {
        NativeType::AggregateState(state) => state.accumulators.len(),
        _ => {
            return Err(DataFusionError::Plan(format!(
                "the partial tree for `{promql}` does not produce aggregate state"
            )))
            .context(DataFusionPlanningSnafu);
        }
    };
    // Value columns are all floats or all histograms, which need not match the state's kind.
    let histogram_type = native_histogram_value_type().as_arrow_type();
    let values = value_schema
        .fields()
        .iter()
        .filter(|field| {
            field.data_type() == &ArrowDataType::Float64 || field.data_type() == &histogram_type
        })
        .count();
    if values != state_accumulators {
        return Err(DataFusionError::Plan(format!(
            "the partial state for `{promql}` has {state_accumulators} accumulator(s), but the output schema declares \
             {values} value column(s)"
        )))
        .context(DataFusionPlanningSnafu);
    }
    // The cut aggregate's grid differs from the statement's when a subquery above the cut re-grids.
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
/// Only `sum` and `avg` fold a histogram payload; they are admitted when they bind histogram state.
/// An input whose kind cannot be named (e.g. through a canonical merge or a set operation) counts
/// as a histogram.
fn aggregate_reads_histogram(node: &NativeNode, input_kind: FieldKind) -> bool {
    // `count` and `group` count a histogram as one sample; `min`, `max`, `stddev`, `stdvar` and
    // `quantile` skip it as Prometheus does (legacy: `create_native_histogram_aggregate_expr`).
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

/// Whether this aggregate's state carries a native histogram, per [`aggregate_state_binding`], the
/// table the compiler and the split bind from.
fn binds_histogram_state(op: &BoundAggregate) -> bool {
    aggregate_state_binding(op.kind, op.field_kind).is_some_and(|(kind, _)| kind.is_histogram())
}

/// Whether any aggregate in `node` binds a state that carries a native histogram.
///
/// Only the aggregate split carries histogram state across a partition boundary.
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
/// Read from the partial tree's root, the cut aggregate.
fn split_exports_histogram_state(partial: &NativeTree) -> bool {
    match &partial.root().expr {
        NativeExpr::Aggregate { op, .. } => binds_histogram_state(op),
        _ => false,
    }
}

/// Whether every plan is a scan of the same table, ignoring filters, projections and bounds.
pub(crate) fn same_table_scans<'a>(plans: impl IntoIterator<Item = &'a LogicalPlan>) -> bool {
    let mut table = None;
    plans.into_iter().all(|plan| {
        let LogicalPlan::TableScan(scan) = plan else {
            return false;
        };
        *table.get_or_insert(&scan.table_name) == &scan.table_name
    })
}

/// The most metric tables one discovered selector will scan; more is a planning error.
const MAX_DISCOVERED_METRICS: usize = 1024;

/// Whether a selector names no single table and has to enumerate the schema's metric tables.
///
/// Public so the authorization layer gives the same answer as the planner.
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
/// Only `limitk`, which keeps the first `k` series in input order ([`promql::select::limitk`]).
/// `topk` breaks ties by label and `limit_ratio` selects by label hash.
fn depends_on_input_order(node: &NativeNode) -> bool {
    match &node.expr {
        NativeExpr::Input(_) | NativeExpr::Scalar(_) | NativeExpr::EvalTime { .. } => false,
        NativeExpr::Instant { input, .. }
        | NativeExpr::Merge { input }
        | NativeExpr::Window { input, .. }
        | NativeExpr::Map { input, .. }
        | NativeExpr::Timestamp { input }
        | NativeExpr::Aggregate { input, .. }
        // e.g. `vector(scalar(limitk(1, a)))`.
        | NativeExpr::VectorOf { input }
        | NativeExpr::ScalarOf { input } => depends_on_input_order(input),
        NativeExpr::ParameterizedMap {
            input, parameters, ..
        } => depends_on_input_order(input) || parameters.iter().any(depends_on_input_order),
        NativeExpr::ParameterizedTopK {
            input, parameter, ..
        } => depends_on_input_order(input) || depends_on_input_order(parameter),
        // e.g. `quantile(scalar(limitk(1, a)), b)`.
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

/// One side of a set operation, as its range in the pre-order selector list.
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

/// Collects selectors into `out` in the order the compiler binds them, each with whether `rate` or
/// `increase` reads it, and records set operations as ranges of `out`. `false` refuses the
/// statement.
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
        // The compiler binds the vector (last argument) before the parameters.
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
        // The compiler binds an aggregate's input before its parameter.
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
        PromExpr::Subquery(subquery) => {
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
/// An operand without a real input takes the layout of the first real selector in the other
/// operand, since set operators pair fields by position. `None` for an empty input with no such
/// operand.
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
    // Operations are recorded inside out, so the innermost one decides first.
    for operation in set_operations {
        for (empty, sibling) in [
            (operation.lhs, operation.rhs),
            (operation.rhs, operation.lhs),
        ] {
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
/// Decided by engine name so logical planning does not probe a region.
fn supports_native_raw_series(engine: &str) -> bool {
    matches!(engine, MITO_ENGINE | MITO2_ENGINE | METRIC_ENGINE)
}

/// The type the raw series scan hands one field of `statement` to the fused node as.
///
/// A numeric field reads as `Float64` only where the existing plan also publishes `Float64`
/// ([`publishes_float_values`]); any other type is returned unchanged for the field-kind guard.
fn read_field_type(data_type: &ArrowDataType, statement: &PromExpr) -> ArrowDataType {
    if is_raw_numeric(data_type) && publishes_float_values(statement, data_type) {
        ArrowDataType::Float64
    } else {
        data_type.clone()
    }
}

/// Whether a field of this type is one the raw series scan casts to `Float64`.
///
/// Decimals are excluded: neither route reads them as floats.
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

/// The range functions whose existing-plan UDF (`promql::functions`) returns `Float64` for any
/// numeric window, so `rate(integer_field[5m])` publishes `Float64`.
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

/// The type class the existing planner publishes for one expression over one field. Not a boolean
/// because `sum` keeps an integer's type but widens `Float32` to `Float64`.
#[derive(Clone, Copy, PartialEq, Eq)]
enum PublishedValues {
    Float64,
    /// `Float16` or `Float32`.
    NarrowFloat,
    /// Any signed or unsigned integer.
    Integer,
    /// A type or shape not modeled here; never admitted.
    Other,
}

/// Whether the existing planner publishes a `Float64` for `field_type` under `statement`.
///
/// Anything not proved is `false`, which refuses it. Proved, with the existing planner's deciding
/// site:
///
/// - arithmetic: `PromPlanner::prom_token_to_binary_expr_builder` wraps operands in `cast_float`,
///   unless an operand folds before the cast ([`folds_before_cast`]),
/// - `min`/`max` (`Extremum`) and `avg`, `stddev`, `stdvar`, `quantile`, `group`
///   (`PromPlanner::create_numeric_aggregate_expr`) return `Float64`,
/// - `sum` returns `Float64` for float input and the input type for integers,
/// - [`FLOAT_ANSWERING_RANGE_FUNCTIONS`].
///
/// Parentheses, subqueries and the selecting aggregates publish their input's class; a bare
/// selector publishes the field's own type. Not proved: `count`/`count_values` (`Int64`), math and
/// label functions, `-x`, comparisons and set operations.
fn publishes_float_values(statement: &PromExpr, field_type: &ArrowDataType) -> bool {
    published_values(statement, field_type) == PublishedValues::Float64
}

/// The class [`publishes_float_values`] reads: what the existing plan publishes for `statement`.
fn published_values(statement: &PromExpr, field_type: &ArrowDataType) -> PublishedValues {
    match statement {
        PromExpr::Paren(ParenExpr { expr }) => published_values(expr, field_type),
        PromExpr::Subquery(SubqueryExpr { expr, .. }) => published_values(expr, field_type),
        PromExpr::VectorSelector(_) | PromExpr::MatrixSelector(_) => field_values(field_type),
        // Comparisons and set operations are not modeled.
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
            token::T_MIN
            | token::T_MAX
            | token::T_AVG
            | token::T_STDDEV
            | token::T_STDVAR
            | token::T_QUANTILE
            | token::T_GROUP => PublishedValues::Float64,
            token::T_SUM => match published_values(&aggregate.expr, field_type) {
                PublishedValues::Float64 | PublishedValues::NarrowFloat => PublishedValues::Float64,
                PublishedValues::Integer => PublishedValues::Integer,
                PublishedValues::Other => PublishedValues::Other,
            },
            token::T_COUNT | token::T_COUNT_VALUES => PublishedValues::Integer,
            token::T_TOPK | token::T_BOTTOMK | token::T_LIMITK | token::T_LIMIT_RATIO => {
                published_values(&aggregate.expr, field_type)
            }
            _ => PublishedValues::Other,
        },
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

/// Whether `statement` folds samples into a non-float before an arithmetic `cast_float` reaches
/// them.
///
/// The existing plan then folds exactly (`sum` of `Int64` adds integers) while the raw scan casts
/// first; integers above `2^53` can answer differently, so `sum(integer_field) * 1` is refused.
fn folds_before_cast(statement: &PromExpr, field_type: &ArrowDataType) -> bool {
    match statement {
        PromExpr::Paren(ParenExpr { expr }) => folds_before_cast(expr, field_type),
        PromExpr::Subquery(SubqueryExpr { expr, .. }) => folds_before_cast(expr, field_type),
        PromExpr::Binary(binary) => {
            folds_before_cast(&binary.lhs, field_type) || folds_before_cast(&binary.rhs, field_type)
        }
        PromExpr::Unary(unary) => folds_before_cast(&unary.expr, field_type),
        PromExpr::Aggregate(_) => !publishes_float_values(statement, field_type),
        // Selectors, literals and calls hand over single samples; nothing folds before the cast.
        _ => false,
    }
}

/// What a bare selector over a field of this type publishes.
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

/// Recognizes the metric logical-table scan emitted by `create_table_scan_plan` and returns the
/// physical scan, its `__table_id = <id>` predicate, and the id.
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
        // A parameter is an expression of its own, and the compiler compiles the input before it,
        // so a parameter that reads a selector is a second source bound after the input. The order
        // here is that binding order, which is the one the planner prepares its scans in.
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
        // An aggregate reports the grouping labels its input carries: `by` keeps exactly those, and
        // `without` keeps the rest and drops the metric name. `x` is bound to the three tag columns
        // below, so `without (instance)` keeps all three: `instance` is not one of them.
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
        // selects, so it reports the same set. Its parameter reads a source of its own, so this
        // tree binds two.
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
    /// The walk is the authority on the name's values - it keeps it exactly where the operator
    /// keeps it - so the column it is read from has to exist whether the walk kept the name or
    /// dropped it.
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
    /// still hold several metrics whose ordinary labels are equal, so writing one name can fold
    /// them into one identity. The output still publishes the written name.
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

        // A fresh ordinary destination above the same kernel is still uncollected: no scan can
        // carry an ordinary label it does not declare.
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
    /// drops the name before it copies an included label, so the copied name is a column. A one
    /// side without a name cannot provide one, so the column leaves the set.
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
