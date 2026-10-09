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

mod at_modifier;
mod function_plans;
mod island;
mod matching_filters;

mod set_operator;

use std::collections::{BTreeSet, HashSet, VecDeque};
use std::sync::Arc;
use std::time::UNIX_EPOCH;

use arrow::datatypes::IntervalDayTime;
use async_recursion::async_recursion;
use catalog::table_source::DfTableSourceProvider;
use common_error::ext::ErrorExt;
use common_error::status_code::StatusCode;
use common_function::function::FunctionContext;
use common_query::native_histogram::native_histogram_value_type;
use common_query::prelude::{
    GREPTIME_TEMPORALITY_DELTA, OTLP_AGGREGATION_TEMPORALITY_LABEL, greptime_native_histogram,
    greptime_value,
};
use common_query::promql_annotations::PromqlAnnotationCollector;
use datafusion::common::DFSchemaRef;
use datafusion::datasource::DefaultTableSource;
use datafusion::functions_aggregate::average::avg_udaf;
use datafusion::functions_aggregate::count::count_udaf;
use datafusion::functions_aggregate::expr_fn::first_value;
use datafusion::functions_aggregate::min_max::{max_udaf, min_udaf};
use datafusion::functions_aggregate::stddev::stddev_pop_udaf;
use datafusion::functions_aggregate::sum::sum_udaf;
use datafusion::functions_aggregate::variance::var_pop_udaf;
use datafusion::functions_window::row_number::RowNumber;
use datafusion::logical_expr::expr::{Alias, ScalarFunction, WindowFunction};
use datafusion::logical_expr::expr_rewriter::normalize_cols;
use datafusion::logical_expr::{
    BinaryExpr, Cast, EmptyRelation, Extension, LogicalPlan, LogicalPlanBuilder, Operator,
    ScalarUDF as ScalarUdfDef, WindowFrame, WindowFunctionDefinition, scalar_subquery,
};
use datafusion::prelude as df_prelude;
use datafusion::prelude::{Column, Expr as DfExpr, JoinType};
use datafusion::scalar::ScalarValue;
use datafusion_common::{DFSchema, NullEquality, TableReference};
use datafusion_expr::expr::WindowFunctionParams;
use datafusion_expr::expr_fn::when;
use datafusion_expr::utils::{conjunction, disjunction};
use datafusion_expr::{ExprSchemable, Literal, SortExpr, TableSource, ident, lit};
use datafusion_functions::core::coalesce;
use datatypes::arrow::datatypes::{DataType as ArrowDataType, TimeUnit as ArrowTimeUnit};
use datatypes::data_type::{ConcreteDataType, DataType as GreptimeDataType};
use itertools::Itertools;
use once_cell::sync::Lazy;
use promql::extension_plan::{
    EmptyMetric, InstantManipulate, Millisecond, RangeManipulate, SeriesDivide, SeriesNormalize,
    build_special_time_expr,
};
use promql::functions::{
    AbsentOverTime, AvgOverTime, Changes, CountOverTime, Delta, Deriv, DoubleExponentialSmoothing,
    IDelta, Increase, LastOverTime, MatchGroupViolation, MaxOverTime, MinOverTime, MixedRange,
    NativeHistogramAbsentOverTime, NativeHistogramAdd, NativeHistogramAggAvg,
    NativeHistogramAggSum, NativeHistogramAvg, NativeHistogramAvgOverTime, NativeHistogramChanges,
    NativeHistogramCount, NativeHistogramCountOverTime, NativeHistogramDelta,
    NativeHistogramDivScalar, NativeHistogramDrop, NativeHistogramEq, NativeHistogramIDelta,
    NativeHistogramIRate, NativeHistogramIncrease, NativeHistogramLastOverTime,
    NativeHistogramMulScalar, NativeHistogramNeg, NativeHistogramNotEq,
    NativeHistogramPresentOverTime, NativeHistogramRate, NativeHistogramResets,
    NativeHistogramScalarMul, NativeHistogramStddev, NativeHistogramStdvar, NativeHistogramSub,
    NativeHistogramSum, NativeHistogramSumOverTime, NativeHistogramToString, PredictLinear,
    PresentOverTime, PromqlFloatToString, QuantileOverTime, Rate, Resets, Round, StddevOverTime,
    StdvarOverTime, SumOverTime, UniqueMatchGroup, quantile_udaf,
};
use promql_parser::label::{METRIC_NAME, MatchOp, Matcher, Matchers};
use promql_parser::parser::token::TokenType;
use promql_parser::parser::value::ValueType;
use promql_parser::parser::{
    AggregateExpr, BinModifier, BinaryExpr as PromBinaryExpr, Call, EvalStmt, Expr as PromExpr,
    Function, LabelModifier, MatrixSelector, NumberLiteral, Offset, ParenExpr, StringLiteral,
    SubqueryExpr, UnaryExpr, VectorMatchCardinality, VectorSelector, token,
};
use regex::{self, Regex};
use snafu::{OptionExt, ResultExt, ensure};
use store_api::metric_engine_consts::{
    DATA_SCHEMA_TABLE_ID_COLUMN_NAME, DATA_SCHEMA_TSID_COLUMN_NAME, LOGICAL_TABLE_METADATA_KEY,
    METRIC_ENGINE_NAME, is_metric_engine_internal_column,
};
use table::table::adapter::DfTableProviderAdapter;

use crate::parser::{
    ALIAS_NODE_NAME, ANALYZE_NODE_NAME, ANALYZE_VERBOSE_NODE_NAME, AliasExpr, EXPLAIN_NODE_NAME,
    EXPLAIN_VERBOSE_NODE_NAME,
};
use crate::promql::error::{
    CatalogSnafu, ColumnNotFoundSnafu, DataFusionPlanningSnafu, ExpectRangeSelectorSnafu,
    FunctionInvalidArgumentSnafu, InvalidDestinationLabelNameSnafu, InvalidRegularExpressionSnafu,
    InvalidTimeRangeSnafu, MultiFieldsNotSupportedSnafu, MultipleMetricMatchersSnafu,
    MultipleVectorSnafu, NoMetricMatcherSnafu, Result, SubqueryTimestampOutOfRangeSnafu,
    TableNameNotFoundSnafu, TimeIndexNotFoundSnafu, UnexpectedPlanExprSnafu, UnexpectedTokenSnafu,
    UnknownTableSnafu, UnsupportedExprSnafu, UnsupportedMatcherOpSnafu, ValueNotFoundSnafu,
    ZeroRangeSelectorSnafu,
};
use crate::query_engine::QueryEngineState;

/// `time()` function in PromQL.
const SPECIAL_TIME_FUNCTION: &str = "time";
/// `scalar()` function in PromQL.
const SCALAR_FUNCTION: &str = "scalar";
/// `absent()` function in PromQL
const SPECIAL_ABSENT_FUNCTION: &str = "absent";
/// `histogram_quantile` function in PromQL
const SPECIAL_HISTOGRAM_QUANTILE: &str = "histogram_quantile";
/// `histogram_fraction` function in PromQL
const SPECIAL_HISTOGRAM_FRACTION: &str = "histogram_fraction";
/// `vector` function in PromQL
const SPECIAL_VECTOR_FUNCTION: &str = "vector";

/// Error text for a runtime `scalar(...)` parameter of a range query: one value
/// cannot be broadcast to the rows of every evaluation step.
const DYNAMIC_SCALAR_UNSUPPORTED_IN_RANGE_QUERY: &str =
    "dynamic scalar parameter not supported in range query";
/// Error text for a runtime `scalar(...)` parameter of a function that reads
/// its parameter while planning, e.g. `histogram_quantile`, `topk` and
/// `bottomk`.
const DYNAMIC_SCALAR_UNSUPPORTED_FOR_FUNCTION: &str =
    "dynamic scalar parameter not supported for this function";
/// Functions whose scalar parameter may be a runtime `scalar(...)` expression.
///
/// The parameter of these functions only becomes an argument of an ordinary
/// scalar function call, so the value computed by the parameter's subquery can
/// be broadcast to every row the function is applied to.
const DYNAMIC_SCALAR_FUNCTIONS: [&str; 3] = ["clamp_min", "clamp_max", "quantile_over_time"];

/// `le` column for conventional histogram.
const LE_COLUMN_NAME: &str = "le";

/// Static regex for validating label names according to Prometheus specification.
/// Label names must match the regex: [a-zA-Z_][a-zA-Z0-9_]*
static LABEL_NAME_REGEX: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"^[a-zA-Z_][a-zA-Z0-9_]*$").unwrap());

const DEFAULT_TIME_INDEX_COLUMN: &str = "time";

/// default value column name for empty metric
const DEFAULT_FIELD_COLUMN: &str = "value";

/// Special modifier to project field columns under multi-field mode
const FIELD_COLUMN_MATCHER: &str = "__field__";

/// Special modifier for cross schema query
const SCHEMA_COLUMN_MATCHER: &str = "__schema__";
const DB_COLUMN_MATCHER: &str = "__database__";

const OR_FLOAT_FIELD_PREFIX: &str = "__promql_or_float_";
const OR_HISTOGRAM_FIELD_PREFIX: &str = "__promql_or_histogram_";
const TIMESTAMP_VALUE_PREFIX: &str = "__promql_timestamp_value_";
/// Per-match-group row count the vector matching cardinality check is built on.
const MATCH_GROUP_COUNT_COLUMN: &str = "__promql_match_group_count";

/// Threshold for scatter scan mode
const MAX_SCATTER_POINTS: i64 = 400;

/// Interval 1 hour in millisecond
const INTERVAL_1H: i64 = 60 * 60 * 1000;

#[derive(Default, Debug, Clone)]
struct PromPlannerContext {
    // query parameters
    start: Millisecond,
    end: Millisecond,
    interval: Millisecond,
    lookback_delta: Millisecond,
    /// Evaluation range of the whole statement, which `@ start()` and `@ end()` refer to.
    ///
    /// Unlike [`Self::start`] and [`Self::end`], these are never rewritten while planning, so a
    /// selector inside a subquery still resolves `@ start()` / `@ end()` against the statement.
    stmt_start: Millisecond,
    stmt_end: Millisecond,

    // planner states
    table_name: Option<String>,
    time_index_column: Option<String>,
    field_columns: Vec<String>,
    tag_columns: Vec<String>,
    /// `by(...)` labels of the aggregation that produced this operand that are not series tags of
    /// its input, i.e. value fields (or a label an inner aggregation already reported as one).
    ///
    /// An aggregation reports every `by(...)` label it finds in its input schema among its tag
    /// columns, and a value field named there is a group key of the aggregate rather than a
    /// property of a series: its value varies between the samples of one series. Lowering a
    /// matcher on it into the scan would change which samples are selected (#9242), so
    /// [`matching_filters`] refuses to propagate such a matcher.
    aggregation_field_labels: Vec<String>,
    /// Use metric engine internal series identifier column (`__tsid`) as series key.
    ///
    /// This is enabled only when the underlying scan can provide `__tsid` (`UInt64`). The planner
    /// uses it internally (e.g. as the series key for [`SeriesDivide`]) and strips it from the
    /// final output.
    use_tsid: bool,
    /// The matcher for field columns `__field__`.
    field_column_matcher: Option<Vec<Matcher>>,
    /// The matcher for selectors (normal matchers).
    selector_matcher: Vec<Matcher>,
    schema_name: Option<String>,
    /// The range in millisecond of range selector. None if there is no range selector.
    range: Option<Millisecond>,
    /// The offset in milliseconds the window of the last planned range selector is folded with,
    /// or `None` when no range selector has been planned since the last read.
    ///
    /// [`Self::start`] and the sample timestamps are compared on the shifted timeline: a range
    /// payload carries `sample_timestamp + offset`, while the time index column of a folded row
    /// stays the evaluation timestamp of its step. A function that reads it right after its input
    /// plan is built, like `predict_linear`, consumes it instead of reading the state, so the
    /// offset cannot leak from one input to another; see
    /// [`PromPlanner::create_function_expr`].
    range_fold_offset: Option<Millisecond>,
}

/// Result labels a vector-vector binary operation derives from its matching modifier, projected
/// from the operand each label belongs to.
#[derive(Debug)]
struct BinaryResultLabels {
    exprs: Vec<DfExpr>,
    names: Vec<String>,
    aggregation_field_labels: Vec<String>,
    /// `__tsid` column of the operand the labels come from, when that operand contributes its
    /// whole tag set and the column still identifies the result series.
    tsid: Option<DfExpr>,
}

impl BinaryResultLabels {
    fn apply(&self, ctx: &mut PromPlannerContext) {
        ctx.tag_columns = self.names.clone();
        ctx.aggregation_field_labels = self.aggregation_field_labels.clone();
        ctx.use_tsid = self.tsid.is_some();
    }

    /// `__tsid` is projected from whichever operand the labels come from, so it carries that
    /// operand's qualifier. Re-qualify it as the result's own, which is what the context names
    /// and what the enclosing expression looks the column up by.
    fn tsid_projection(&self, table_ref: Option<TableReference>) -> Option<DfExpr> {
        self.tsid
            .clone()
            .map(|tsid| tsid.alias_qualified(table_ref, DATA_SCHEMA_TSID_COLUMN_NAME))
    }
}

impl PromPlannerContext {
    fn from_eval_stmt(stmt: &EvalStmt) -> Self {
        let start = stmt.start.duration_since(UNIX_EPOCH).unwrap().as_millis() as Millisecond;
        let end = stmt.end.duration_since(UNIX_EPOCH).unwrap().as_millis() as Millisecond;
        Self {
            start,
            end,
            interval: stmt.interval.as_millis() as _,
            lookback_delta: stmt.lookback_delta.as_millis() as _,
            stmt_start: start,
            stmt_end: end,
            ..Default::default()
        }
    }

    /// Reset all planner states
    fn reset(&mut self) {
        self.table_name = None;
        self.time_index_column = None;
        self.field_columns = vec![];
        self.tag_columns = vec![];
        self.use_tsid = false;
        self.field_column_matcher = None;
        self.selector_matcher.clear();
        self.schema_name = None;
        self.range = None;
        self.range_fold_offset = None;
    }

    /// Reset table name and schema to empty
    fn reset_table_name_and_schema(&mut self) {
        self.table_name = Some(String::new());
        self.schema_name = None;
        self.use_tsid = false;
    }

    /// Check if `le` is present in tag columns
    fn has_le_tag(&self) -> bool {
        self.tag_columns.iter().any(|c| c.eq(&LE_COLUMN_NAME))
    }
}

pub struct PromPlanner {
    table_provider: DfTableSourceProvider,
    ctx: PromPlannerContext,
    /// Optional collector passed to native histogram UDFs.
    promql_annotations: Option<PromqlAnnotationCollector>,
}

type BinaryFieldPair<'a> = (&'a String, &'a String);

impl PromPlanner {
    pub async fn stmt_to_plan(
        table_provider: DfTableSourceProvider,
        stmt: &EvalStmt,
        query_engine_state: &QueryEngineState,
    ) -> Result<LogicalPlan> {
        Self::stmt_to_plan_with_annotations(table_provider, stmt, query_engine_state, None).await
    }

    /// Plans a PromQL statement and passes the optional collector to histogram UDFs.
    pub async fn stmt_to_plan_with_annotations(
        table_provider: DfTableSourceProvider,
        stmt: &EvalStmt,
        query_engine_state: &QueryEngineState,
        promql_annotations: Option<PromqlAnnotationCollector>,
    ) -> Result<LogicalPlan> {
        let mut planner = Self {
            table_provider,
            ctx: PromPlannerContext::from_eval_stmt(stmt),
            promql_annotations,
        };

        let plan = planner
            .prom_expr_to_plan(&stmt.expr, query_engine_state)
            .await?;

        // Never leak internal series identifier to output.
        planner.strip_tsid_column(plan)
    }

    pub async fn prom_expr_to_plan(
        &mut self,
        prom_expr: &PromExpr,
        query_engine_state: &QueryEngineState,
    ) -> Result<LogicalPlan> {
        self.prom_expr_to_plan_inner(prom_expr, false, query_engine_state)
            .await
    }

    /**
    Converts a PromQL expression to a logical plan.

    NOTE:
        The `timestamp_fn` indicates whether the PromQL `timestamp()` function is being evaluated in the current context.
        If `true`, the planner generates a logical plan that projects the timestamp (time index) column
        as the value column for each input row, implementing the PromQL `timestamp()` function semantics.
        If `false`, the planner generates the standard logical plan for the given PromQL expression.
    */
    #[async_recursion]
    async fn prom_expr_to_plan_inner(
        &mut self,
        prom_expr: &PromExpr,
        timestamp_fn: bool,
        query_engine_state: &QueryEngineState,
    ) -> Result<LogicalPlan> {
        // An anchored range call is step-invariant: evaluate it once, at the start of the
        // evaluation, and report its result at every step; see
        // [`Self::promote_anchored_range_call`].
        if let Some(plan) = self
            .promote_anchored_range_call(prom_expr, timestamp_fn, query_engine_state)
            .await?
        {
            return Ok(plan);
        }

        let res = match prom_expr {
            PromExpr::Aggregate(expr) => {
                self.prom_aggr_expr_to_plan(query_engine_state, expr)
                    .await?
            }
            PromExpr::Unary(expr) => {
                self.prom_unary_expr_to_plan(query_engine_state, expr)
                    .await?
            }
            PromExpr::Binary(expr) => {
                self.prom_binary_expr_to_plan(query_engine_state, expr)
                    .await?
            }
            PromExpr::Paren(ParenExpr { expr }) => {
                self.prom_expr_to_plan_inner(expr, timestamp_fn, query_engine_state)
                    .await?
            }
            PromExpr::Subquery(expr) => {
                self.prom_subquery_expr_to_plan(query_engine_state, expr)
                    .await?
            }
            PromExpr::NumberLiteral(lit) => self.prom_number_lit_to_plan(lit)?,
            PromExpr::StringLiteral(lit) => self.prom_string_lit_to_plan(lit)?,
            PromExpr::VectorSelector(selector) => {
                self.prom_vector_selector_to_plan(selector, timestamp_fn)
                    .await?
            }
            PromExpr::MatrixSelector(selector) => {
                self.prom_matrix_selector_to_plan(selector).await?
            }
            PromExpr::Call(expr) => {
                self.prom_call_expr_to_plan(query_engine_state, expr)
                    .await?
            }
            PromExpr::Extension(expr) => {
                self.prom_ext_expr_to_plan(query_engine_state, expr).await?
            }
        };

        Ok(res)
    }

    async fn prom_subquery_expr_to_plan(
        &mut self,
        query_engine_state: &QueryEngineState,
        subquery_expr: &SubqueryExpr,
    ) -> Result<LogicalPlan> {
        let SubqueryExpr {
            expr,
            range,
            step,
            offset,
            ..
        } = subquery_expr;

        // Shift the child window back; `RangeManipulate` restores the outer timeline.
        let offset_ms = match offset {
            Some(Offset::Pos(duration)) => duration.as_millis() as Millisecond,
            Some(Offset::Neg(duration)) => -(duration.as_millis() as Millisecond),
            None => 0,
        };

        let current_interval = self.ctx.interval;
        if let Some(step) = step {
            self.ctx.interval = step.as_millis() as _;
        }
        ensure!(self.ctx.interval > 0, ZeroRangeSelectorSnafu);
        let current_start = self.ctx.start;
        let current_end = self.ctx.end;
        // Reject an invalid parent window rather than treating it as empty.
        ensure!(
            current_start <= current_end,
            InvalidTimeRangeSnafu {
                start: current_start,
                end: current_end,
            }
        );
        // An empty child needs only its schema; plan it over the valid caller window.
        let child_window = Self::subquery_child_window(
            current_start,
            current_end,
            current_interval,
            offset_ms,
            range.as_millis() as Millisecond,
            self.ctx.interval,
        )?;
        if let Some((child_start, child_end)) = child_window {
            self.ctx.start = child_start;
            self.ctx.end = child_end;
        }
        let input = self.prom_expr_to_plan(expr, query_engine_state).await?;
        self.ctx.interval = current_interval;
        self.ctx.start = current_start;
        self.ctx.end = current_end;
        // Return no rows with the child schema so enclosing fallbacks still work.
        let input = match child_window {
            Some(_) => input,
            None => LogicalPlan::EmptyRelation(EmptyRelation {
                produce_one_row: false,
                schema: input.schema().clone(),
            }),
        };

        ensure!(!range.is_zero(), ZeroRangeSelectorSnafu);
        let range_ms = range.as_millis() as _;
        self.ctx.range = Some(range_ms);

        let time_index_column =
            self.ctx
                .time_index_column
                .clone()
                .with_context(|| TimeIndexNotFoundSnafu {
                    table: self.ctx.table_name.clone().unwrap_or_default(),
                })?;

        // `RangeManipulate` assumes each input batch holds exactly one series
        // (it takes tag column values from row 0 and applies them to every
        // output row). The inner expression may emit batches that mix series,
        // so sort by series key + time index and split into per-series batches
        // with a `SeriesDivide` first.
        let input_schema = input.schema();
        let input_has_tsid = input_schema.fields().iter().any(|field| {
            field.name() == DATA_SCHEMA_TSID_COLUMN_NAME
                && field.data_type() == &ArrowDataType::UInt64
        });
        let (series_key_columns, mut sort_exprs) = if input_has_tsid {
            (
                vec![DATA_SCHEMA_TSID_COLUMN_NAME.to_string()],
                vec![
                    DfExpr::Column(Column::from_name(DATA_SCHEMA_TSID_COLUMN_NAME))
                        .sort(true, true),
                ],
            )
        } else {
            // Only use tag columns that survive in the inner plan's schema —
            // `ctx.tag_columns` can drift from the actual output.
            let key_columns: Vec<String> = self
                .ctx
                .tag_columns
                .iter()
                .filter(|name| input_schema.has_column_with_unqualified_name(name))
                .cloned()
                .collect();
            let sort = key_columns
                .iter()
                .map(|name| DfExpr::Column(Column::from_name(name)).sort(true, true))
                .collect::<Vec<_>>();
            (key_columns, sort)
        };
        sort_exprs.push(DfExpr::Column(Column::from_name(&time_index_column)).sort(true, true));

        let sort_plan = LogicalPlanBuilder::from(input)
            .sort(sort_exprs)
            .context(DataFusionPlanningSnafu)?
            .build()
            .context(DataFusionPlanningSnafu)?;
        let divide_plan = LogicalPlan::Extension(Extension {
            node: Arc::new(SeriesDivide::new(
                series_key_columns.clone(),
                time_index_column.clone(),
                sort_plan,
            )),
        });

        // `RangeManipulate` has no offset in its protobuf message; decoding recovers it from the
        // `SeriesNormalize` directly below. Stale markers are not filtered: the input is computed.
        let divide_plan = if offset_ms != 0 {
            LogicalPlan::Extension(Extension {
                node: Arc::new(SeriesNormalize::new(
                    offset_ms,
                    time_index_column.clone(),
                    false,
                    series_key_columns,
                    divide_plan,
                )),
            })
        } else {
            divide_plan
        };

        let manipulate = RangeManipulate::new(
            self.ctx.start,
            self.ctx.end,
            self.ctx.interval,
            offset_ms,
            range_ms,
            time_index_column,
            self.ctx.field_columns.clone(),
            divide_plan,
        )
        .context(DataFusionPlanningSnafu)?;
        // The payload timestamps are shifted by the subquery offset; see
        // [`Self::create_range_eval_ts_expr`].
        self.ctx.range_fold_offset = Some(offset_ms);

        Ok(LogicalPlan::Extension(Extension {
            node: Arc::new(manipulate),
        }))
    }

    async fn prom_aggr_expr_to_plan(
        &mut self,
        query_engine_state: &QueryEngineState,
        aggr_expr: &AggregateExpr,
    ) -> Result<LogicalPlan> {
        let AggregateExpr {
            op,
            expr,
            modifier,
            param,
        } = aggr_expr;

        let input = self.prom_expr_to_plan(expr, query_engine_state).await?;
        let input_has_tsid = input.schema().fields().iter().any(|field| {
            field.name() == DATA_SCHEMA_TSID_COLUMN_NAME
                && field.data_type() == &ArrowDataType::UInt64
        });

        match (*op).id() {
            token::T_TOPK | token::T_BOTTOMK => {
                self.prom_topk_bottomk_to_plan(aggr_expr, input).await
            }
            _ => {
                // When `__tsid` is available, tag columns may have been pruned from the input plan.
                // For `keep_tsid` decision we should compare against the full row-key label set,
                // otherwise we may incorrectly treat label-reducing aggregates as preserving labels.
                let input_tag_columns = if input_has_tsid {
                    self.collect_row_key_tag_columns_from_plan(&input)?
                        .into_iter()
                        .collect::<Vec<_>>()
                } else {
                    self.ctx.tag_columns.clone()
                };
                // calculate columns to group by
                // Need to append time index column into group by columns
                let mut group_exprs = self.agg_modifier_to_col(input.schema(), modifier, true)?;
                let mixed_sample_columns =
                    Self::alternative_sample_columns(input.schema(), &self.ctx.field_columns)
                        .map(|(float, histogram)| (float.to_string(), histogram.to_string()));
                // Aggregates over native histogram inputs may drop every sample in a group
                // (e.g. `min` over histogram-only samples, or `sum` over histograms with
                // incompatible schemas) and leave a NULL-valued group row behind. Compute this
                // before `create_aggregate_exprs` mutates `ctx.field_columns`.
                let preserve_any_value = mixed_sample_columns.is_some();
                let has_native_histogram = preserve_any_value
                    || self.all_field_columns_are_native_histograms(input.schema());
                // convert op and value columns to aggregate exprs
                let (mut aggr_exprs, prev_field_exprs) =
                    self.create_aggregate_exprs(*op, param, &input)?;
                let prev_field_exprs =
                    normalize_cols(prev_field_exprs, &input).context(DataFusionPlanningSnafu)?;

                let keep_tsid = op.id() != token::T_COUNT_VALUES
                    && input_has_tsid
                    && input_tag_columns.iter().collect::<HashSet<_>>()
                        == self.ctx.tag_columns.iter().collect::<HashSet<_>>();

                if keep_tsid {
                    aggr_exprs.push(
                        first_value(
                            DfExpr::Column(Column::from_name(DATA_SCHEMA_TSID_COLUMN_NAME)),
                            vec![],
                        )
                        .alias(DATA_SCHEMA_TSID_COLUMN_NAME),
                    );
                }
                self.ctx.use_tsid = keep_tsid;

                // create plan
                let builder = LogicalPlanBuilder::from(input);
                let builder = if op.id() == token::T_COUNT_VALUES {
                    let label = Self::get_param_value_as_str(*op, param)?;
                    // `count_values` must be grouped by fields,
                    // and project the fields to the new label.
                    //
                    // The generated label is a real label (column) of the output, so it must be
                    // registered in `ctx.tag_columns` below. Otherwise enclosing expressions
                    // rebuild their projections from `ctx.tag_columns` and silently drop it.
                    //
                    // PromQL sets the generated label *before* the grouping key is built, so an
                    // input label with the same name is overwritten by the sample value and must
                    // not remain a grouping key either: samples are grouped by the generated
                    // label only. Dropping it from the projected tag columns is required as well:
                    // projecting both would emit two columns with the same name (rejected as an
                    // ambiguous reference).
                    self.ctx.tag_columns.retain(|tag| tag != label);
                    group_exprs.retain(
                        |expr| !matches!(expr, DfExpr::Column(column) if column.name == label),
                    );
                    // The tag columns projected below are unqualified `Column` references, so they
                    // end up qualified with whatever qualifier they carry in the input plan. Give
                    // the generated label the same qualifier, otherwise qualified references to it
                    // (e.g. from an enclosing binary expression or a vector join) fail to resolve.
                    let label_qualifier = self
                        .ctx
                        .time_index_column
                        .as_deref()
                        .and_then(|time_index| {
                            builder
                                .schema()
                                .qualified_field_with_unqualified_name(time_index)
                                .ok()
                        })
                        .and_then(|(qualifier, _)| qualifier.cloned());
                    // The generated label carries the *sample value*, so it must be materialized
                    // as a string using Prometheus' format
                    // (`strconv.FormatFloat(value, 'f', -1, 64)`: shortest decimal form without
                    // an exponent, `1.0` becomes "1"). Labels are inferred from string columns
                    // when a result is converted into the Prometheus HTTP API JSON format, so a
                    // numeric label column would be mistaken for the sample value of the series.
                    let count_value_exprs = prev_field_exprs
                        .iter()
                        .map(|expr| {
                            let value = match expr {
                                DfExpr::Column(column) => {
                                    let value = DfExpr::Column(column.clone());
                                    // `prom_float_to_string` formats exactly like Prometheus,
                                    // while arrow's `Float64 -> Utf8` cast would render `1.0`.
                                    let value = if matches!(
                                        builder.schema().field_with_unqualified_name(&column.name),
                                        Ok(field) if field.data_type() == &ArrowDataType::Float64
                                    ) {
                                        value
                                    } else {
                                        DfExpr::Cast(Cast::new(
                                            Box::new(value),
                                            ArrowDataType::Float64,
                                        ))
                                    };
                                    DfExpr::ScalarFunction(ScalarFunction {
                                        func: Arc::new(PromqlFloatToString::scalar_udf()),
                                        args: vec![value],
                                    })
                                }
                                // The value is already formatted (e.g. a native histogram is
                                // converted to its string form by the aggregation), and the
                                // aggregate output names it by its schema name.
                                _ => DfExpr::Column(Column::from_name(
                                    expr.schema_name().to_string(),
                                )),
                            };
                            DfExpr::Alias(Alias::new(value, label_qualifier.clone(), label))
                        })
                        .collect::<Vec<_>>();
                    let aggregate_group_exprs = group_exprs
                        .iter()
                        .cloned()
                        .chain(prev_field_exprs.clone())
                        .collect::<Vec<_>>();
                    group_exprs.push(ident(label));
                    let project_fields = self
                        .create_field_column_exprs()?
                        .into_iter()
                        .chain(self.create_tag_column_exprs()?)
                        .chain(Some(self.create_time_index_column_expr()?))
                        .chain(count_value_exprs);

                    let builder = builder
                        .aggregate(aggregate_group_exprs, aggr_exprs)
                        .context(DataFusionPlanningSnafu)?
                        .project(project_fields)
                        .context(DataFusionPlanningSnafu)?;
                    // The label only exists in the output schema from here on.
                    self.ctx.tag_columns.push(label.to_string());
                    builder
                } else {
                    builder
                        .aggregate(group_exprs.clone(), aggr_exprs)
                        .context(DataFusionPlanningSnafu)?
                };

                let builder = if let Some((float, histogram)) = mixed_sample_columns {
                    let builder = match op.id() {
                        token::T_SUM | token::T_AVG => builder
                            .filter(self.mixed_aggregate_filter_expr(*op, &float, &histogram)?)
                            .context(DataFusionPlanningSnafu)?,
                        token::T_MIN
                        | token::T_MAX
                        | token::T_STDDEV
                        | token::T_STDVAR
                        | token::T_QUANTILE => builder
                            .filter(self.mixed_ignored_histogram_filter_expr(*op, &histogram)?)
                            .context(DataFusionPlanningSnafu)?,
                        _ => builder,
                    };

                    match op.id() {
                        token::T_SUM
                        | token::T_AVG
                        | token::T_MIN
                        | token::T_MAX
                        | token::T_STDDEV
                        | token::T_STDVAR
                        | token::T_QUANTILE => {
                            let project_fields = self
                                .create_field_column_exprs()?
                                .into_iter()
                                .chain(self.create_tag_column_exprs()?)
                                .chain(self.ctx.use_tsid.then_some(DfExpr::Column(
                                    Column::from_name(DATA_SCHEMA_TSID_COLUMN_NAME),
                                )))
                                .chain(Some(self.create_time_index_column_expr()?));
                            builder
                                .project(project_fields)
                                .context(DataFusionPlanningSnafu)?
                        }
                        _ => builder,
                    }
                } else {
                    builder
                };

                // Drop group rows whose every aggregated sample was discarded (NULL), so that
                // e.g. `group(min(native_histogram))` doesn't resurrect groups Prometheus
                // considers unseen. For alternative float/histogram fields keep the row if any
                // field survived.
                let builder = if has_native_histogram {
                    builder
                        .filter(self.create_empty_values_filter_expr(preserve_any_value)?)
                        .context(DataFusionPlanningSnafu)?
                } else {
                    builder
                };

                let sort_expr = group_exprs.into_iter().map(|expr| expr.sort(true, false));

                builder
                    .sort(sort_expr)
                    .context(DataFusionPlanningSnafu)?
                    .build()
                    .context(DataFusionPlanningSnafu)
            }
        }
    }

    /// Create logical plan for PromQL topk and bottomk expr.
    async fn prom_topk_bottomk_to_plan(
        &mut self,
        aggr_expr: &AggregateExpr,
        input: LogicalPlan,
    ) -> Result<LogicalPlan> {
        let AggregateExpr {
            op,
            param,
            modifier,
            ..
        } = aggr_expr;

        let input_has_tsid = input.schema().fields().iter().any(|field| {
            field.name() == DATA_SCHEMA_TSID_COLUMN_NAME
                && field.data_type() == &ArrowDataType::UInt64
        });
        self.ctx.use_tsid = input_has_tsid;

        let group_exprs = self.agg_modifier_to_col(input.schema(), modifier, false)?;

        let mut input = input;
        if let Some((float_column, histogram_column)) =
            Self::alternative_sample_columns(input.schema(), &self.ctx.field_columns)
                .map(|(float, histogram)| (float.to_string(), histogram.to_string()))
        {
            let drop_histogram = DfExpr::ScalarFunction(ScalarFunction {
                func: Arc::new(NativeHistogramDrop::bool_false_udf(
                    format!(
                        "{}: dropped native histogram samples because this aggregation is not supported for native histograms",
                        op
                    ),
                    self.promql_annotations.clone(),
                )),
                args: vec![ident(&histogram_column)],
            });
            let keep_float = when(ident(&histogram_column).is_not_null(), drop_histogram)
                .otherwise(ident(&float_column).is_not_null())
                .context(DataFusionPlanningSnafu)?;
            input = LogicalPlanBuilder::from(input)
                .filter(keep_float)
                .context(DataFusionPlanningSnafu)?
                .build()
                .context(DataFusionPlanningSnafu)?;
            self.ctx.field_columns = vec![float_column];
        }

        if self.all_field_columns_are_native_histograms(input.schema()) {
            let promql_annotations = self.promql_annotations.clone();
            let input = self.projection_for_each_field_column(input, |col| {
                Ok(DfExpr::ScalarFunction(ScalarFunction {
                    func: Arc::new(NativeHistogramDrop::float_null_udf(
                        format!(
                            "{}: dropped native histogram samples because this aggregation is not supported for native histograms",
                            op
                        ),
                        promql_annotations.clone(),
                    )),
                    args: vec![DfExpr::Column(Column::from_name(col))],
                }))
            })?;
            return LogicalPlanBuilder::from(input)
                .filter(self.create_empty_values_filter_expr(false)?)
                .context(DataFusionPlanningSnafu)?
                .build()
                .context(DataFusionPlanningSnafu);
        }

        let val = Self::get_param_as_literal_expr(
            param.as_deref(),
            Some(*op),
            Some(ArrowDataType::Float64),
        )?;

        // convert op and value columns to window exprs.
        let window_exprs = self.create_window_exprs(*op, group_exprs.clone(), &input)?;

        let rank_columns: Vec<_> = window_exprs
            .iter()
            .map(|expr| expr.schema_name().to_string())
            .collect();

        // Create ranks filter with `Operator::Or`.
        // Safety: at least one rank column
        let filter: DfExpr = rank_columns
            .iter()
            .fold(None, |expr, rank| {
                let predicate = DfExpr::BinaryExpr(BinaryExpr {
                    left: Box::new(ident(rank)),
                    op: Operator::LtEq,
                    right: Box::new(val.clone()),
                });

                match expr {
                    None => Some(predicate),
                    Some(expr) => Some(DfExpr::BinaryExpr(BinaryExpr {
                        left: Box::new(expr),
                        op: Operator::Or,
                        right: Box::new(predicate),
                    })),
                }
            })
            .unwrap();

        let rank_columns: Vec<_> = rank_columns.into_iter().map(ident).collect();

        let mut new_group_exprs = group_exprs.clone();
        // Order by ranks
        new_group_exprs.extend(rank_columns);

        let group_sort_expr = new_group_exprs
            .into_iter()
            .map(|expr| expr.sort(true, false));

        let project_fields = self
            .create_field_column_exprs()?
            .into_iter()
            .chain(self.create_tag_column_exprs()?)
            .chain(
                self.ctx
                    .use_tsid
                    .then_some(DfExpr::Column(Column::from_name(
                        DATA_SCHEMA_TSID_COLUMN_NAME,
                    ))),
            )
            .chain(Some(self.create_time_index_column_expr()?));

        LogicalPlanBuilder::from(input)
            .window(window_exprs)
            .context(DataFusionPlanningSnafu)?
            .filter(filter)
            .context(DataFusionPlanningSnafu)?
            .sort(group_sort_expr)
            .context(DataFusionPlanningSnafu)?
            .project(project_fields)
            .context(DataFusionPlanningSnafu)?
            .build()
            .context(DataFusionPlanningSnafu)
    }

    async fn prom_unary_expr_to_plan(
        &mut self,
        query_engine_state: &QueryEngineState,
        unary_expr: &UnaryExpr,
    ) -> Result<LogicalPlan> {
        let UnaryExpr { expr } = unary_expr;
        // Unary Expr in PromQL implys the `-` operator
        let input = self.prom_expr_to_plan(expr, query_engine_state).await?;
        self.negate_field_columns(input)
    }

    fn negate_field_columns(&mut self, input: LogicalPlan) -> Result<LogicalPlan> {
        let input_schema = input.schema().clone();
        self.projection_for_each_field_column(input, |col| {
            if Self::field_column_is_native_histogram(&input_schema, col) {
                Ok(DfExpr::ScalarFunction(ScalarFunction {
                    func: Arc::new(NativeHistogramNeg::scalar_udf()),
                    args: vec![DfExpr::Column(Column::from_name(col))],
                }))
            } else {
                Ok(DfExpr::Negative(Box::new(DfExpr::Column(
                    Column::from_name(col),
                ))))
            }
        })
    }

    async fn prom_binary_expr_to_plan(
        &mut self,
        query_engine_state: &QueryEngineState,
        binary_expr: &PromBinaryExpr,
    ) -> Result<LogicalPlan> {
        // promql-parser accepts fill modifiers, but Greptime does not implement the
        // required outer joins and missing-value substitution. Reject them before the
        // binary-island fast path so they cannot silently behave like normal inner joins.
        if let Some(modifier) = &binary_expr.modifier {
            ensure!(
                modifier.fill_values.lhs.is_none() && modifier.fill_values.rhs.is_none(),
                UnsupportedExprSnafu {
                    name: "PromQL fill modifiers"
                }
            );
        }

        if let Some(plan) = self.try_plan_binary_island(binary_expr).await? {
            return Ok(plan);
        }

        let PromBinaryExpr {
            lhs,
            rhs,
            op,
            modifier,
        } = binary_expr;

        // if set to true, comparison operator will return 0/1 (for true/false) instead of
        // filter on the result column
        let should_return_bool = if let Some(m) = modifier {
            m.return_bool
        } else {
            false
        };
        let is_comparison_op = Self::is_token_a_comparison_op(*op);

        // we should build a filter plan here if the op is comparison op and need not
        // to return 0/1. Otherwise, we should build a projection plan
        match (
            Self::try_build_literal_expr(lhs),
            Self::try_build_literal_expr(rhs),
        ) {
            (Some(lhs), Some(rhs)) => {
                self.ctx.time_index_column = Some(DEFAULT_TIME_INDEX_COLUMN.to_string());
                self.ctx.field_columns = vec![DEFAULT_FIELD_COLUMN.to_string()];
                self.ctx.reset_table_name_and_schema();
                let field_expr_builder = Self::prom_token_to_binary_expr_builder(*op)?;
                let mut field_expr = field_expr_builder(lhs, rhs)?;

                if is_comparison_op && should_return_bool {
                    field_expr =
                        DfExpr::Cast(Cast::new(Box::new(field_expr), ArrowDataType::Float64));
                }

                Ok(LogicalPlan::Extension(Extension {
                    node: Arc::new(
                        EmptyMetric::new(
                            self.ctx.start,
                            self.ctx.end,
                            self.ctx.interval,
                            SPECIAL_TIME_FUNCTION.to_string(),
                            DEFAULT_FIELD_COLUMN.to_string(),
                            Some(field_expr),
                        )
                        .context(DataFusionPlanningSnafu)?,
                    ),
                }))
            }
            // lhs is a literal, rhs is a column
            (Some(mut expr), None) => {
                let input = self.prom_expr_to_plan(rhs, query_engine_state).await?;
                // Arithmetic against a literal preserves the series labels, so
                // `aggregation_field_labels` passes through with `tag_columns` unchanged.
                // check if the literal is a special time expr
                if let Some(time_expr) = self.try_build_special_time_expr_with_context(lhs) {
                    expr = time_expr
                }
                let input_schema = input.schema().clone();
                let preserve_any_value = Self::field_columns_are_alternative_samples(
                    &input_schema,
                    &self.ctx.field_columns,
                );
                let has_native_histogram = Self::field_columns_contain_native_histogram(
                    &input_schema,
                    &self.ctx.field_columns,
                );
                let retain_field_columns = self
                    .ctx
                    .field_columns
                    .iter()
                    .map(|col| {
                        Self::binary_result_is_histogram(
                            *op,
                            false,
                            Self::field_column_is_native_histogram(&input_schema, col),
                        )
                        .is_some()
                    })
                    .collect();
                let promql_annotations = self.promql_annotations.clone();
                let bin_expr_builder = |col: &String| {
                    let binary_expr_builder = Self::prom_token_to_binary_expr_builder(*op)?;
                    let rhs_is_histogram =
                        Self::field_column_is_native_histogram(&input_schema, col);
                    let rhs = DfExpr::Column(Column::from_name(col));
                    let mut binary_expr = match Self::native_histogram_binary_expr(
                        *op,
                        expr.clone(),
                        false,
                        rhs.clone(),
                        rhs_is_histogram,
                        is_comparison_op && !should_return_bool,
                        promql_annotations.clone(),
                    )? {
                        Some(expr) => expr,
                        None => binary_expr_builder(expr.clone(), rhs)?,
                    };

                    if is_comparison_op && should_return_bool {
                        binary_expr =
                            DfExpr::Cast(Cast::new(Box::new(binary_expr), ArrowDataType::Float64));
                    }
                    Ok(binary_expr)
                };
                if is_comparison_op && !should_return_bool {
                    self.filter_on_field_column(input, bin_expr_builder)
                } else {
                    let projected =
                        self.projection_for_each_field_column(input, bin_expr_builder)?;
                    self.filter_binary_projection(
                        projected,
                        has_native_histogram,
                        preserve_any_value,
                        retain_field_columns,
                    )
                }
            }
            // lhs is a column, rhs is a literal
            (None, Some(mut expr)) => {
                let input = self.prom_expr_to_plan(lhs, query_engine_state).await?;
                // check if the literal is a special time expr
                if let Some(time_expr) = self.try_build_special_time_expr_with_context(rhs) {
                    expr = time_expr
                }
                let input_schema = input.schema().clone();
                let preserve_any_value = Self::field_columns_are_alternative_samples(
                    &input_schema,
                    &self.ctx.field_columns,
                );
                let has_native_histogram = Self::field_columns_contain_native_histogram(
                    &input_schema,
                    &self.ctx.field_columns,
                );
                let retain_field_columns = self
                    .ctx
                    .field_columns
                    .iter()
                    .map(|col| {
                        Self::binary_result_is_histogram(
                            *op,
                            Self::field_column_is_native_histogram(&input_schema, col),
                            false,
                        )
                        .is_some()
                    })
                    .collect();
                let promql_annotations = self.promql_annotations.clone();
                let bin_expr_builder = |col: &String| {
                    let binary_expr_builder = Self::prom_token_to_binary_expr_builder(*op)?;
                    let lhs_is_histogram =
                        Self::field_column_is_native_histogram(&input_schema, col);
                    let lhs = DfExpr::Column(Column::from_name(col));
                    let mut binary_expr = match Self::native_histogram_binary_expr(
                        *op,
                        lhs.clone(),
                        lhs_is_histogram,
                        expr.clone(),
                        false,
                        is_comparison_op && !should_return_bool,
                        promql_annotations.clone(),
                    )? {
                        Some(expr) => expr,
                        None => binary_expr_builder(lhs, expr.clone())?,
                    };

                    if is_comparison_op && should_return_bool {
                        binary_expr =
                            DfExpr::Cast(Cast::new(Box::new(binary_expr), ArrowDataType::Float64));
                    }
                    Ok(binary_expr)
                };
                if is_comparison_op && !should_return_bool {
                    self.filter_on_field_column(input, bin_expr_builder)
                } else {
                    let projected =
                        self.projection_for_each_field_column(input, bin_expr_builder)?;
                    self.filter_binary_projection(
                        projected,
                        has_native_histogram,
                        preserve_any_value,
                        retain_field_columns,
                    )
                }
            }
            // both are columns. join them on time index
            (None, None) => {
                let mut left_input = self.prom_expr_to_plan(lhs, query_engine_state).await?;
                let left_field_columns = self.ctx.field_columns.clone();
                let left_time_index_column = self.ctx.time_index_column.clone();
                let mut left_table_ref = self
                    .table_ref()
                    .unwrap_or_else(|_| TableReference::bare(""));
                let mut left_context = self.ctx.clone();

                let mut right_input = self.prom_expr_to_plan(rhs, query_engine_state).await?;
                let right_field_columns = self.ctx.field_columns.clone();
                let right_time_index_column = self.ctx.time_index_column.clone();
                let mut right_table_ref = self
                    .table_ref()
                    .unwrap_or_else(|_| TableReference::bare(""));
                let mut right_context = self.ctx.clone();

                // Both operands are planned first because only the planned contexts tell a tag
                // from a value field. The rewrite merely adds matchers to a selector, so the
                // table reference, time index and field columns captured above stay valid.
                if let Some(rewritten) = matching_filters::propagate(
                    binary_expr,
                    &left_context.tag_columns,
                    &right_context.tag_columns,
                    &left_context.aggregation_field_labels,
                    &right_context.aggregation_field_labels,
                ) {
                    // A copied matcher belongs to the scan, not to the operand's identity:
                    // `absent()` turns `selector_matcher` into the labels it reports, so the
                    // re-planned context keeps the matchers the operand was written with.
                    if rewritten.lhs.as_ref() != lhs.as_ref() {
                        let selectors = std::mem::take(&mut left_context.selector_matcher);
                        left_input = self
                            .prom_expr_to_plan(&rewritten.lhs, query_engine_state)
                            .await?;
                        left_context = self.ctx.clone();
                        left_context.selector_matcher = selectors;
                    }
                    if rewritten.rhs.as_ref() != rhs.as_ref() {
                        let selectors = std::mem::take(&mut right_context.selector_matcher);
                        right_input = self
                            .prom_expr_to_plan(&rewritten.rhs, query_engine_state)
                            .await?;
                        right_context = self.ctx.clone();
                        right_context.selector_matcher = selectors;
                    }
                    // The code below reads `self.ctx` as the right operand's context.
                    self.ctx = right_context.clone();
                }

                let left_is_empty_metric = Self::is_empty_metric(&left_input);
                let right_is_empty_metric = Self::is_empty_metric(&right_input);

                // TODO(ruihang): avoid join if left and right are the same table

                // set op has "special" join semantics
                if Self::is_token_a_set_op(*op) {
                    return self.set_op_on_non_field_columns(
                        left_input,
                        right_input,
                        left_context,
                        right_context,
                        *op,
                        modifier,
                    );
                }

                let has_native_histogram = Self::field_columns_contain_native_histogram(
                    left_input.schema(),
                    &left_field_columns,
                ) || Self::field_columns_contain_native_histogram(
                    right_input.schema(),
                    &right_field_columns,
                );

                // normal join
                if left_table_ref == right_table_ref {
                    // rename table references to avoid ambiguity
                    left_table_ref = TableReference::bare("lhs");
                    right_table_ref = TableReference::bare("rhs");
                    // `self.ctx` have ctx in right plan, if right plan have no tag,
                    // we use left plan ctx as the ctx for subsequent calculations,
                    // to avoid case like `host + scalar(...)`
                    // we need preserve tag column on `host` table in subsequent projection,
                    // which only show in left plan ctx.
                    if self.ctx.tag_columns.is_empty() {
                        self.ctx = left_context.clone();
                        self.ctx.table_name = Some("lhs".to_string());
                    } else {
                        self.ctx.table_name = Some("rhs".to_string());
                    }
                } else if right_is_empty_metric && !left_is_empty_metric {
                    self.ctx = left_context.clone();
                }
                // Computed scalars reach this join path instead of the literal projection paths.
                // Broadcast them for arithmetic in the same way as literal scalars.
                let broadcast_scalar = !is_comparison_op;
                let (field_groups, invalid_field_pairs) = Self::align_binary_field_columns(
                    left_input.schema(),
                    right_input.schema(),
                    &left_field_columns,
                    &right_field_columns,
                    *op,
                    broadcast_scalar && lhs.value_type() == ValueType::Scalar,
                    broadcast_scalar && rhs.value_type() == ValueType::Scalar,
                );
                let left_aligned_field_columns = field_groups
                    .iter()
                    .flat_map(|(_, pairs)| {
                        pairs
                            .iter()
                            .map(|(left_col_name, _)| (*left_col_name).clone())
                    })
                    .collect::<Vec<_>>();
                let right_aligned_field_columns = field_groups
                    .iter()
                    .flat_map(|(_, pairs)| {
                        pairs
                            .iter()
                            .map(|(_, right_col_name)| (*right_col_name).clone())
                    })
                    .collect::<Vec<_>>();
                // Regular multi-field vectors combine their shared prefix. Alternative
                // float/histogram lanes instead align by valid PromQL sample combinations.
                self.ctx.field_columns = field_groups
                    .iter()
                    .map(|(output, _)| output.clone())
                    .collect();
                let mut field_groups = field_groups.into_iter();
                // `vector()` uses EmptyMetric and keeps GreptimeDB's timestamp broadcast.
                let has_empty_metric_operand = left_is_empty_metric || right_is_empty_metric;

                let only_join_time_index = lhs.value_type() == ValueType::Scalar
                    || rhs.value_type() == ValueType::Scalar
                    || has_empty_metric_operand
                    || ((left_context.tag_columns.is_empty()
                        || right_context.tag_columns.is_empty())
                        && !left_context
                            .tag_columns
                            .iter()
                            .chain(&right_context.tag_columns)
                            .any(|tag| tag == OTLP_AGGREGATION_TEMPORALITY_LABEL));
                let join_plan = self.join_on_non_field_columns(
                    left_input,
                    right_input,
                    left_table_ref.clone(),
                    right_table_ref.clone(),
                    left_time_index_column,
                    right_time_index_column,
                    only_join_time_index,
                    modifier,
                    &left_context,
                    &right_context,
                )?;
                let join_plan_schema = join_plan.schema().clone();
                // The matching modifier derives the result labels instead of the operands keeping
                // their own tag set. An operand that is broadcast rather than matched (a scalar,
                // or a vector without tags) keeps the other side's labels as before.
                let result_labels = if only_join_time_index {
                    None
                } else {
                    Self::binary_result_labels(&left_context, &right_context, modifier)
                        .map(|labels| {
                            Self::binary_result_label_projection(
                                &join_plan_schema,
                                &left_table_ref,
                                &right_table_ref,
                                &left_context,
                                &right_context,
                                labels,
                            )
                        })
                        .transpose()?
                };
                if let Some(labels) = &result_labels {
                    labels.apply(&mut self.ctx);
                }
                let promql_annotations = self.promql_annotations.clone();
                // These predicates always pass; they only evaluate otherwise-discarded pairs
                // while collecting annotations.
                let invalid_pair_predicates = invalid_field_pairs
                    .into_iter()
                    .filter(|_| promql_annotations.is_some())
                    .map(|(left_col_name, right_col_name)| {
                        let left_field = join_plan_schema
                            .qualified_field_with_name(Some(&left_table_ref), left_col_name)
                            .context(DataFusionPlanningSnafu)?;
                        let right_field = join_plan_schema
                            .qualified_field_with_name(Some(&right_table_ref), right_col_name)
                            .context(DataFusionPlanningSnafu)?;
                        let left_is_histogram =
                            left_field.1.data_type() == &Self::native_histogram_arrow_type();
                        let right_is_histogram =
                            right_field.1.data_type() == &Self::native_histogram_arrow_type();
                        let drop_expr = Self::native_histogram_binary_expr(
                            *op,
                            DfExpr::Column(left_field.into()),
                            left_is_histogram,
                            DfExpr::Column(right_field.into()),
                            right_is_histogram,
                            true,
                            promql_annotations.clone(),
                        )?
                        .with_context(|| UnexpectedPlanExprSnafu {
                            desc: "invalid native histogram pair produced no drop expression",
                        })?;
                        Ok(DfExpr::Not(Box::new(drop_expr)))
                    })
                    .collect::<Result<Vec<_>>>()?;
                let join_plan = if let Some(predicate) = conjunction(invalid_pair_predicates) {
                    LogicalPlanBuilder::from(join_plan)
                        .filter(predicate)
                        .context(DataFusionPlanningSnafu)?
                        .build()
                        .context(DataFusionPlanningSnafu)?
                } else {
                    join_plan
                };

                let bin_expr_builder = |_: &String| {
                    let (_, field_pairs) =
                        field_groups
                            .next()
                            .with_context(|| UnexpectedPlanExprSnafu {
                                desc: "missing binary field group",
                            })?;
                    let binary_exprs = field_pairs
                        .into_iter()
                        .map(|(left_col_name, right_col_name)| {
                            let left_field = join_plan_schema
                                .qualified_field_with_name(Some(&left_table_ref), left_col_name)
                                .context(DataFusionPlanningSnafu)?;
                            let right_field = join_plan_schema
                                .qualified_field_with_name(Some(&right_table_ref), right_col_name)
                                .context(DataFusionPlanningSnafu)?;
                            let left_is_histogram =
                                left_field.1.data_type() == &Self::native_histogram_arrow_type();
                            let right_is_histogram =
                                right_field.1.data_type() == &Self::native_histogram_arrow_type();
                            let left_col = left_field.into();
                            let right_col = right_field.into();

                            let binary_expr_builder = Self::prom_token_to_binary_expr_builder(*op)?;
                            let lhs = DfExpr::Column(left_col);
                            let rhs = DfExpr::Column(right_col);
                            let mut binary_expr = match Self::native_histogram_binary_expr(
                                *op,
                                lhs.clone(),
                                left_is_histogram,
                                rhs.clone(),
                                right_is_histogram,
                                is_comparison_op && !should_return_bool,
                                promql_annotations.clone(),
                            )? {
                                Some(expr) => expr,
                                None => binary_expr_builder(lhs, rhs)?,
                            };
                            if is_comparison_op && should_return_bool {
                                binary_expr = DfExpr::Cast(Cast::new(
                                    Box::new(binary_expr),
                                    ArrowDataType::Float64,
                                ));
                            }
                            Ok(binary_expr)
                        })
                        .collect::<Result<Vec<_>>>()?;
                    if let [binary_expr] = binary_exprs.as_slice() {
                        Ok(binary_expr.clone())
                    } else {
                        Ok(DfExpr::ScalarFunction(ScalarFunction {
                            func: coalesce(),
                            args: binary_exprs,
                        }))
                    }
                };
                if is_comparison_op && !should_return_bool {
                    // PromQL comparison operators without `bool` are filters:
                    //   - keep the instant-vector side sample values
                    //   - drop samples where the comparison is false
                    //
                    // So we filter on the join result and then project only the side that should
                    // be preserved according to PromQL semantics.
                    let filtered = self.filter_on_field_column(join_plan, bin_expr_builder)?;
                    let (project_table_ref, mut project_context, project_field_columns) =
                        match (lhs.value_type(), rhs.value_type()) {
                            (ValueType::Scalar, ValueType::Vector) => (
                                &right_table_ref,
                                right_context.clone(),
                                right_aligned_field_columns,
                            ),
                            _ => (
                                &left_table_ref,
                                left_context.clone(),
                                left_aligned_field_columns,
                            ),
                        };
                    project_context.field_columns = project_field_columns;
                    self.project_binary_join_side(
                        filtered,
                        project_table_ref,
                        &project_context,
                        result_labels.as_ref(),
                    )
                } else {
                    let projected = self.projection_for_each_field_column_with_labels(
                        join_plan,
                        result_labels.as_ref(),
                        bin_expr_builder,
                    )?;
                    let preserve_any_value = Self::field_columns_are_alternative_samples(
                        projected.schema(),
                        &self.ctx.field_columns,
                    );
                    let retain_field_columns = vec![true; self.ctx.field_columns.len()];
                    self.filter_binary_projection(
                        projected,
                        has_native_histogram,
                        preserve_any_value,
                        retain_field_columns,
                    )
                }
            }
        }
    }

    fn filter_binary_projection(
        &mut self,
        input: LogicalPlan,
        has_native_histogram: bool,
        preserve_any_value: bool,
        retain_field_columns: Vec<bool>,
    ) -> Result<LogicalPlan> {
        if !has_native_histogram {
            return Ok(input);
        }

        ensure!(
            retain_field_columns.len() == self.ctx.field_columns.len(),
            UnexpectedPlanExprSnafu {
                desc: "binary output field count changed unexpectedly",
            }
        );

        let filtered = LogicalPlanBuilder::from(input)
            .filter(self.create_empty_values_filter_expr(preserve_any_value)?)
            .context(DataFusionPlanningSnafu)?
            .build()
            .context(DataFusionPlanningSnafu)?;
        if retain_field_columns.iter().all(|retain| *retain) {
            return Ok(filtered);
        }

        let retained = self
            .ctx
            .field_columns
            .iter()
            .zip(retain_field_columns)
            .filter(|(_, retain)| *retain)
            .map(|(field, _)| field.clone())
            .collect::<Vec<_>>();
        if retained.is_empty() {
            return Ok(filtered);
        }
        self.ctx.field_columns = retained;

        let mut output_columns = self
            .ctx
            .field_columns
            .iter()
            .chain(&self.ctx.tag_columns)
            .cloned()
            .collect::<HashSet<_>>();
        output_columns.extend(self.ctx.time_index_column.iter().cloned());
        if self.ctx.use_tsid {
            output_columns.insert(DATA_SCHEMA_TSID_COLUMN_NAME.to_string());
        }
        let project_exprs = filtered
            .schema()
            .iter()
            .filter(|(_, field)| output_columns.contains(field.name()))
            .map(|(qualifier, field)| {
                DfExpr::Column(Column::new(qualifier.cloned(), field.name().clone()))
            })
            .collect::<Vec<_>>();
        LogicalPlanBuilder::from(filtered)
            .project(project_exprs)
            .context(DataFusionPlanningSnafu)?
            .build()
            .context(DataFusionPlanningSnafu)
    }

    fn project_binary_join_side(
        &mut self,
        input: LogicalPlan,
        table_ref: &TableReference,
        context: &PromPlannerContext,
        result_labels: Option<&BinaryResultLabels>,
    ) -> Result<LogicalPlan> {
        let schema = input.schema();

        let mut project_exprs =
            Vec::with_capacity(context.tag_columns.len() + context.field_columns.len() + 2);

        // Project time index from the chosen side.
        if let Some(time_index_column) = &context.time_index_column {
            let time_index_col = schema
                .qualified_field_with_name(Some(table_ref), time_index_column)
                .context(DataFusionPlanningSnafu)?
                .into();
            project_exprs.push(DfExpr::Column(time_index_col));
        }

        // Project field columns from the chosen side.
        for field_column in &context.field_columns {
            let field_col = schema
                .qualified_field_with_name(Some(table_ref), field_column)
                .context(DataFusionPlanningSnafu)?
                .into();
            project_exprs.push(DfExpr::Column(field_col));
        }

        // Project tag columns: the labels the matching modifier derived, or the chosen side's.
        match result_labels {
            Some(labels) => project_exprs.extend(labels.exprs.iter().cloned()),
            None => {
                for tag_column in &context.tag_columns {
                    let tag_col = schema
                        .qualified_field_with_name(Some(table_ref), tag_column)
                        .context(DataFusionPlanningSnafu)?
                        .into();
                    project_exprs.push(DfExpr::Column(tag_col));
                }
            }
        }

        // Preserve `__tsid` if present, so it can still be used internally downstream. It's
        // stripped from the final output anyway.
        let tsid_col = match result_labels {
            Some(labels) => labels.tsid_projection(Some(table_ref.clone())),
            None => Self::optional_tsid_projection(schema, Some(table_ref), context.use_tsid),
        };
        if let Some(tsid_col) = tsid_col {
            project_exprs.push(tsid_col);
        }

        let plan = LogicalPlanBuilder::from(input)
            .project(project_exprs)
            .context(DataFusionPlanningSnafu)?
            .build()
            .context(DataFusionPlanningSnafu)?;

        // Update context to reflect the projected schema. Don't keep a table qualifier since
        // the result is a derived expression.
        self.ctx = context.clone();
        self.ctx.table_name = None;
        self.ctx.schema_name = None;
        if let Some(labels) = result_labels {
            labels.apply(&mut self.ctx);
        }

        Ok(plan)
    }

    fn prom_number_lit_to_plan(&mut self, number_literal: &NumberLiteral) -> Result<LogicalPlan> {
        let NumberLiteral { val } = number_literal;
        self.ctx.time_index_column = Some(DEFAULT_TIME_INDEX_COLUMN.to_string());
        self.ctx.field_columns = vec![DEFAULT_FIELD_COLUMN.to_string()];
        self.ctx.reset_table_name_and_schema();
        let literal_expr = df_prelude::lit(*val);

        let plan = LogicalPlan::Extension(Extension {
            node: Arc::new(
                EmptyMetric::new(
                    self.ctx.start,
                    self.ctx.end,
                    self.ctx.interval,
                    SPECIAL_TIME_FUNCTION.to_string(),
                    DEFAULT_FIELD_COLUMN.to_string(),
                    Some(literal_expr),
                )
                .context(DataFusionPlanningSnafu)?,
            ),
        });
        Ok(plan)
    }

    fn prom_string_lit_to_plan(&mut self, string_literal: &StringLiteral) -> Result<LogicalPlan> {
        let StringLiteral { val } = string_literal;
        self.ctx.time_index_column = Some(DEFAULT_TIME_INDEX_COLUMN.to_string());
        self.ctx.field_columns = vec![DEFAULT_FIELD_COLUMN.to_string()];
        self.ctx.reset_table_name_and_schema();
        let literal_expr = df_prelude::lit(val.clone());

        let plan = LogicalPlan::Extension(Extension {
            node: Arc::new(
                EmptyMetric::new(
                    self.ctx.start,
                    self.ctx.end,
                    self.ctx.interval,
                    SPECIAL_TIME_FUNCTION.to_string(),
                    DEFAULT_FIELD_COLUMN.to_string(),
                    Some(literal_expr),
                )
                .context(DataFusionPlanningSnafu)?,
            ),
        });
        Ok(plan)
    }

    /// The offset of a selector in milliseconds. A positive offset selects samples from an earlier
    /// time and moves them forward into the evaluation timeline.
    fn offset_millis(offset: &Option<Offset>) -> Millisecond {
        match offset {
            Some(Offset::Pos(duration)) => duration.as_millis() as Millisecond,
            Some(Offset::Neg(duration)) => -(duration.as_millis() as Millisecond),
            None => 0,
        }
    }

    /// Returns the first absolute step multiple strictly after `start - offset - range`
    /// through the last parent evaluation instant minus `offset`. Requires a positive step.
    /// Returns `None` for an empty window; an unrepresentable nonempty bound is an error.
    fn subquery_child_window(
        start: Millisecond,
        end: Millisecond,
        interval: Millisecond,
        offset: Millisecond,
        range: Millisecond,
        step: Millisecond,
    ) -> Result<Option<(Millisecond, Millisecond)>> {
        let window_start = i128::from(start) - i128::from(offset) - i128::from(range);
        let grid_start = (window_start.div_euclid(i128::from(step)) + 1) * i128::from(step);
        let grid_end = if interval > 0 {
            let steps = (i128::from(end) - i128::from(start)) / i128::from(interval);
            i128::from(start) + steps * i128::from(interval)
        } else {
            i128::from(end)
        } - i128::from(offset);
        // Check emptiness before converting bounds to i64.
        if grid_start > grid_end {
            return Ok(None);
        }
        let bound = |name: &str, value: i128| {
            i64::try_from(value).map_err(|_| {
                SubqueryTimestampOutOfRangeSnafu {
                    timestamp: format!(
                        "subquery child window {name} = {value} ms (start = {start} ms, \
                         end = {end} ms, interval = {interval} ms, offset = {offset} ms, \
                         range = {range} ms, step = {step} ms)"
                    ),
                }
                .build()
            })
        };
        Ok(Some((
            bound("grid start", grid_start)?,
            bound("grid end", grid_end)?,
        )))
    }

    /// The columns that identify one series, which is the series key expected by the PromQL plan
    /// nodes that hold exactly one series per input batch.
    fn series_key_columns(&self) -> Vec<String> {
        if self.ctx.use_tsid {
            vec![DATA_SCHEMA_TSID_COLUMN_NAME.to_string()]
        } else {
            self.ctx.tag_columns.clone()
        }
    }

    /// Keep replay and series division on the same effective keys when a call rewrites labels.
    fn series_key_columns_for_schema(&self, schema: &DFSchemaRef) -> Vec<String> {
        let has_tsid = schema.fields().iter().any(|field| {
            field.name() == DATA_SCHEMA_TSID_COLUMN_NAME
                && field.data_type() == &ArrowDataType::UInt64
        });
        if has_tsid {
            vec![DATA_SCHEMA_TSID_COLUMN_NAME.to_string()]
        } else {
            self.ctx
                .tag_columns
                .iter()
                .filter(|name| schema.has_column_with_unqualified_name(name))
                .cloned()
                .collect()
        }
    }

    async fn prom_vector_selector_to_plan(
        &mut self,
        vector_selector: &VectorSelector,
        timestamp_fn: bool,
    ) -> Result<LogicalPlan> {
        let VectorSelector {
            name,
            offset,
            matchers,
            at,
        } = vector_selector;
        let matchers = self.preprocess_label_matchers(matchers, name)?;
        if let Some(empty_plan) = self.setup_context().await? {
            return Ok(empty_plan);
        }
        let offset_ms = Self::offset_millis(offset);
        // `@` anchors the sample window at a fixed timestamp: the selector selects its samples
        // around the anchor once, instead of following the outer evaluation grid. See
        // [`Self::at_modifier_offset`].
        let at_offset = self.at_modifier_offset(at, offset)?;
        let grid_start = self.ctx.start;
        let grid_end = self.ctx.end;
        let normalize = match at_offset {
            Some(at_offset) => {
                // Select the anchored samples at the start of the evaluation, with the offset that
                // re-anchors the selector.
                // The planner is single-use (one `EvalStmt` produces one plan), so an error
                // below aborts the whole planning and `ctx.end` needs no restore-on-error.
                self.ctx.end = grid_start;
                let plan = self
                    .selector_to_series_normalize_plan(at_offset, matchers, false)
                    .await?;
                self.ctx.end = grid_end;
                plan
            }
            None => {
                self.selector_to_series_normalize_plan(offset_ms, matchers, false)
                    .await?
            }
        };
        let time_index_column =
            self.ctx
                .time_index_column
                .clone()
                .with_context(|| TimeIndexNotFoundSnafu {
                    table: self.ctx.table_name.clone().unwrap_or_default(),
                })?;

        let (normalize, timestamp_value_column) = if timestamp_fn {
            // Keep the original sample for stale-marker detection while carrying
            // its timestamp through InstantManipulate in a private value column.
            let occupied = normalize
                .schema()
                .fields()
                .iter()
                .map(|field| field.name().as_str())
                .collect::<HashSet<_>>();
            let mut timestamp_value_column = TIMESTAMP_VALUE_PREFIX.to_string();
            while occupied.contains(timestamp_value_column.as_str()) {
                timestamp_value_column.push('_');
            }
            let mut project_exprs = normalize
                .schema()
                .iter()
                .map(|(qualifier, field)| {
                    DfExpr::Column(Column::new(qualifier.cloned(), field.name().clone()))
                })
                .collect::<Vec<_>>();
            // The time index still holds the raw sample timestamp here, which is what
            // `timestamp()` reports regardless of `offset` and `@`.
            let sample_time = Self::timestamp_seconds_expr(&time_index_column, normalize.schema())?;
            project_exprs.push(sample_time.alias(&timestamp_value_column));
            let normalize = LogicalPlanBuilder::from(normalize)
                .project(project_exprs)
                .context(DataFusionPlanningSnafu)?
                .build()
                .context(DataFusionPlanningSnafu)?;
            (normalize, Some(timestamp_value_column))
        } else {
            (normalize, None)
        };

        let field_column = self.ctx.field_columns.first().cloned();
        let series_key_columns = self.series_key_columns();
        let manipulate = match at_offset {
            Some(at_offset) => {
                // Select the anchored sample once, then report it at every step of the outer
                // grid. The samples keep their native anchor-time timestamps, so the manipulate
                // must shift them onto the evaluation timeline with the rewritten offset.
                let anchored = InstantManipulate::new(
                    grid_start,
                    grid_start,
                    self.ctx.lookback_delta,
                    self.ctx.interval,
                    at_offset,
                    time_index_column.clone(),
                    series_key_columns,
                    field_column,
                    normalize,
                );
                self.replay_over_grid(
                    LogicalPlan::Extension(Extension {
                        node: Arc::new(anchored),
                    }),
                    grid_start,
                    grid_end,
                    time_index_column,
                )
            }
            None => LogicalPlan::Extension(Extension {
                node: Arc::new(InstantManipulate::new(
                    grid_start,
                    grid_end,
                    self.ctx.lookback_delta,
                    self.ctx.interval,
                    offset_ms,
                    time_index_column,
                    series_key_columns,
                    field_column,
                    normalize,
                )),
            }),
        };
        if let Some(timestamp_value_column) = timestamp_value_column {
            self.create_timestamp_func_plan(manipulate, ident(timestamp_value_column))
        } else {
            Ok(manipulate)
        }
    }

    /// Converts the timestamp column `column` into PromQL seconds, truncated to milliseconds.
    fn timestamp_seconds_expr(column: &str, schema: &DFSchema) -> Result<DfExpr> {
        let column = DfExpr::Column(Column::from_name(column));
        let ArrowDataType::Timestamp(_, timezone) =
            column.get_type(schema).context(DataFusionPlanningSnafu)?
        else {
            unreachable!("time index is a timestamp")
        };
        let millis = column
            .cast_to(
                &ArrowDataType::Timestamp(ArrowTimeUnit::Millisecond, timezone),
                schema,
            )
            .context(DataFusionPlanningSnafu)?
            .cast_to(&ArrowDataType::Int64, schema)
            .context(DataFusionPlanningSnafu)?
            .cast_to(&ArrowDataType::Float64, schema)
            .context(DataFusionPlanningSnafu)?;
        Ok(DfExpr::BinaryExpr(BinaryExpr {
            left: Box::new(millis),
            op: Operator::Divide,
            right: Box::new(lit(1000.0)),
        }))
    }

    /// Builds a projection plan for the PromQL `timestamp()` function, which reports
    /// `timestamp_value` as the value of each row, along with the original tag and time index
    /// columns.
    ///
    /// Updates the planner context's field columns to the timestamp column name.
    fn create_timestamp_func_plan(
        &mut self,
        input: LogicalPlan,
        timestamp_value: DfExpr,
    ) -> Result<LogicalPlan> {
        // A row whose fields are all NULL holds no sample, so it must not get a timestamp. The
        // check reads the input fields, which the projection below replaces.
        let has_sample = self.create_empty_values_filter_expr(true)?;
        let time_expr = timestamp_value.alias(DEFAULT_FIELD_COLUMN);
        self.ctx.field_columns = vec![time_expr.schema_name().to_string()];
        let mut project_exprs = Vec::with_capacity(self.ctx.tag_columns.len() + 2);
        project_exprs.push(self.create_time_index_column_expr()?);
        project_exprs.push(time_expr);
        project_exprs.extend(self.create_tag_column_exprs()?);

        LogicalPlanBuilder::from(input)
            .filter(has_sample)
            .context(DataFusionPlanningSnafu)?
            .project(project_exprs)
            .context(DataFusionPlanningSnafu)?
            .build()
            .context(DataFusionPlanningSnafu)
    }

    async fn prom_matrix_selector_to_plan(
        &mut self,
        matrix_selector: &MatrixSelector,
    ) -> Result<LogicalPlan> {
        let MatrixSelector { vs, range } = matrix_selector;
        let VectorSelector {
            name,
            offset,
            matchers,
            at,
        } = vs;
        let matchers = self.preprocess_label_matchers(matchers, name)?;
        ensure!(!range.is_zero(), ZeroRangeSelectorSnafu);
        let range_ms = range.as_millis() as _;
        self.ctx.range = Some(range_ms);
        let offset_ms = Self::offset_millis(offset);

        // `@` anchors the range selector's window at a fixed timestamp, so the same window is fed
        // to the enclosing function at every evaluation step. See [`Self::at_modifier_offset`].
        let at_offset = self.at_modifier_offset(at, offset)?;
        let grid_start = self.ctx.start;
        let grid_end = self.ctx.end;

        // Some functions like rate may require special fields in the RangeManipulate plan
        // so we can't skip RangeManipulate.
        let (normalize, at_offset) = match self.setup_context().await? {
            // An empty metric does not contain any sample, so anchoring cannot change the result.
            // The manipulate below folds the empty input with `offset_ms`, and the recorded fold
            // offset agrees with it instead of with the anchor the window cannot use.
            Some(empty_plan) => {
                self.ctx.range_fold_offset = Some(offset_ms);
                (empty_plan, None)
            }
            None => {
                let normalize = match at_offset {
                    Some(at_offset) => {
                        // Fold the anchored window once, at the start of the evaluation.
                        // Single-use planner: an error below aborts planning, so `ctx.end`
                        // needs no restore-on-error.
                        self.ctx.end = grid_start;
                        let plan = self
                            .selector_to_series_normalize_plan(at_offset, matchers, true)
                            .await?;
                        self.ctx.end = grid_end;
                        plan
                    }
                    None => {
                        self.selector_to_series_normalize_plan(offset_ms, matchers, true)
                            .await?
                    }
                };
                // Samples are shifted onto the evaluation timeline with the very same offset while
                // the window is folded. Record it so that the function above the selector can
                // recover the evaluation instant of the folded window; see
                // [`Self::create_range_eval_ts_expr`].
                self.ctx.range_fold_offset = Some(at_offset.unwrap_or(offset_ms));
                (normalize, at_offset)
            }
        };
        let time_index_column = self
            .ctx
            .time_index_column
            .clone()
            .expect("time index should be set in `setup_context`");
        let manipulate = match at_offset {
            Some(at_offset) => {
                // Fold the anchored window once, then report it at every step of the outer
                // grid. The samples keep their native anchor-time timestamps, so the manipulate
                // must shift them onto the evaluation timeline with the rewritten offset.
                let anchored = RangeManipulate::new(
                    grid_start,
                    grid_start,
                    self.ctx.interval,
                    at_offset,
                    // TODO(ruihang): convert via Timestamp datatypes to support different time units
                    range_ms,
                    time_index_column.clone(),
                    self.ctx.field_columns.clone(),
                    normalize,
                )
                .context(DataFusionPlanningSnafu)?;
                self.replay_over_grid(
                    LogicalPlan::Extension(Extension {
                        node: Arc::new(anchored),
                    }),
                    grid_start,
                    grid_end,
                    time_index_column,
                )
            }
            None => {
                let manipulate = RangeManipulate::new(
                    grid_start,
                    grid_end,
                    self.ctx.interval,
                    offset_ms,
                    // TODO(ruihang): convert via Timestamp datatypes to support different time units
                    range_ms,
                    time_index_column,
                    self.ctx.field_columns.clone(),
                    normalize,
                )
                .context(DataFusionPlanningSnafu)?;

                LogicalPlan::Extension(Extension {
                    node: Arc::new(manipulate),
                })
            }
        };

        Ok(manipulate)
    }

    async fn prom_call_expr_to_plan(
        &mut self,
        query_engine_state: &QueryEngineState,
        call_expr: &Call,
    ) -> Result<LogicalPlan> {
        let Call { func, args } = call_expr;
        // some special functions that are not expression but a plan
        match func.name {
            SPECIAL_HISTOGRAM_QUANTILE | SPECIAL_HISTOGRAM_FRACTION => {
                // The histogram helpers keep their scalar parameters as
                // compile-time constants in `HistogramFold`, so a runtime
                // `scalar(...)` parameter is rejected here. The last argument is
                // the input vector, not a parameter.
                let params = &args.args[..args.args.len().saturating_sub(1)];
                for param in params {
                    Self::ensure_static_scalar_param(func.name, param.as_ref())?;
                }
                return self
                    .create_histogram_plan(func.name, args, query_engine_state)
                    .await;
            }
            SPECIAL_VECTOR_FUNCTION => return self.create_vector_plan(args).await,
            SCALAR_FUNCTION => return self.create_scalar_plan(args, query_engine_state).await,
            SPECIAL_ABSENT_FUNCTION => {
                return self.create_absent_plan(args, query_engine_state).await;
            }
            _ => {}
        }

        // transform function arguments
        let args = self.create_function_args(&args.args)?;
        // A `scalar(...)` parameter is only known when the query runs, so it is
        // turned into an expression reading the parameter's own subquery here.
        let literals = self
            .build_scalar_params(func.name, &args.literals, query_engine_state)
            .await?;
        // Only a vector selector keeps the timestamps of its samples. Any other expression
        // produces samples at the evaluation time, which is what `timestamp()` reports for it.
        let mut timestamp_arg = args.input.as_ref();
        while let Some(PromExpr::Paren(ParenExpr { expr })) = timestamp_arg {
            timestamp_arg = Some(expr);
        }
        let timestamp_of_selector =
            func.name == "timestamp" && matches!(timestamp_arg, Some(PromExpr::VectorSelector(_)));
        let input = if let Some(prom_expr) = &args.input {
            let input = self
                .prom_expr_to_plan_inner(prom_expr, timestamp_of_selector, query_engine_state)
                .await?;
            if func.name == "timestamp" && !timestamp_of_selector {
                let time_index_column =
                    self.ctx
                        .time_index_column
                        .clone()
                        .with_context(|| TimeIndexNotFoundSnafu {
                            table: self.ctx.table_name.clone().unwrap_or_default(),
                        })?;
                let eval_time = Self::timestamp_seconds_expr(&time_index_column, input.schema())?;
                self.create_timestamp_func_plan(input, eval_time)?
            } else {
                input
            }
        } else {
            self.ctx.time_index_column = Some(SPECIAL_TIME_FUNCTION.to_string());
            self.ctx.reset_table_name_and_schema();
            self.ctx.tag_columns = vec![];
            self.ctx.aggregation_field_labels.clear();
            self.ctx.field_columns = vec![DEFAULT_FIELD_COLUMN.to_string()];
            LogicalPlan::Extension(Extension {
                node: Arc::new(
                    EmptyMetric::new(
                        self.ctx.start,
                        self.ctx.end,
                        self.ctx.interval,
                        SPECIAL_TIME_FUNCTION.to_string(),
                        DEFAULT_FIELD_COLUMN.to_string(),
                        None,
                    )
                    .context(DataFusionPlanningSnafu)?,
                ),
            })
        };
        // The input plan records the fold offset of the range selector it is built from. Take it
        // here, so that the offset of one input cannot leak into another call, and pass it to
        // `create_function_expr`: the function that reads it (`predict_linear`) then depends on an
        // argument instead of on planner state written by the selector below it.
        let range_fold_offset = self.ctx.range_fold_offset.take();
        let input_tag_columns = self.ctx.tag_columns.clone();
        let (mut func_exprs, new_tags) = self.create_function_expr(
            func,
            literals.clone(),
            input.schema(),
            query_engine_state,
            range_fold_offset,
        )?;
        func_exprs.insert(0, self.create_time_index_column_expr()?);
        func_exprs.extend_from_slice(&self.create_tag_column_exprs()?);
        if let Some(tsid_col) =
            Self::optional_tsid_projection(input.schema(), None, self.ctx.use_tsid)
        {
            func_exprs.push(tsid_col);
        }

        // A row survives as long as one field column produced a sample, and the fields without
        // one stay NULL, which is the shape a selector already emits. Requiring every field to
        // be non-NULL would drop one field's samples because another field has none in the same
        // window — the reason alternative float/histogram columns already needed this form. A
        // single field column reduces to the same predicate either way.
        let builder = LogicalPlanBuilder::from(input)
            .project(func_exprs)
            .context(DataFusionPlanningSnafu)?
            .filter(self.create_empty_values_filter_expr(true)?)
            .context(DataFusionPlanningSnafu)?;

        let builder = match func.name {
            "sort" => builder
                .sort(self.create_field_columns_sort_exprs(true))
                .context(DataFusionPlanningSnafu)?,
            "sort_desc" => builder
                .sort(self.create_field_columns_sort_exprs(false))
                .context(DataFusionPlanningSnafu)?,
            "sort_by_label" => builder
                .sort(Self::create_sort_exprs_by_tags(func.name, literals, true)?)
                .context(DataFusionPlanningSnafu)?,
            "sort_by_label_desc" => builder
                .sort(Self::create_sort_exprs_by_tags(func.name, literals, false)?)
                .context(DataFusionPlanningSnafu)?,

            _ => builder,
        };

        // Rewriting a label the input series already have can map several of them onto the same
        // label set, which PromQL rejects. A new label keeps the series distinct.
        let may_duplicate_label_sets = matches!(func.name, "label_join" | "label_replace")
            && new_tags.iter().any(|tag| input_tag_columns.contains(tag));

        // Update context tags after building plan
        // We can't push them before planning, because they won't exist until projection.
        for tag in new_tags {
            self.ctx.tag_columns.push(tag);
        }

        let mut plan = builder.build().context(DataFusionPlanningSnafu)?;
        if may_duplicate_label_sets {
            let labels = self.ctx.tag_columns.clone();
            plan = Self::assert_unique_match_group(
                plan,
                labels
                    .iter()
                    .map(|label| DfExpr::Column(Column::from_name(label)))
                    .collect(),
                labels,
                self.create_time_index_column_expr()?,
                MatchGroupViolation::DuplicateLabelSet,
            )?;
        }
        common_telemetry::debug!("Created PromQL function plan: {plan:?} for {call_expr:?}");

        Ok(plan)
    }

    async fn prom_ext_expr_to_plan(
        &mut self,
        query_engine_state: &QueryEngineState,
        ext_expr: &promql_parser::parser::ast::Extension,
    ) -> Result<LogicalPlan> {
        // let promql_parser::parser::ast::Extension { expr } = ext_expr;
        let expr = &ext_expr.expr;
        let children = expr.children();
        let plan = self
            .prom_expr_to_plan(&children[0], query_engine_state)
            .await?;
        // Wrapper for the explanation/analyze of the existing plan
        // https://docs.rs/datafusion-expr/latest/datafusion_expr/logical_plan/builder/struct.LogicalPlanBuilder.html#method.explain
        // if `analyze` is true, runs the actual plan and produces
        // information about metrics during run.
        // if `verbose` is true, prints out additional details when VERBOSE keyword is specified
        match expr.name() {
            ANALYZE_NODE_NAME => LogicalPlanBuilder::from(plan)
                .explain(false, true)
                .unwrap()
                .build()
                .context(DataFusionPlanningSnafu),
            ANALYZE_VERBOSE_NODE_NAME => LogicalPlanBuilder::from(plan)
                .explain(true, true)
                .unwrap()
                .build()
                .context(DataFusionPlanningSnafu),
            EXPLAIN_NODE_NAME => LogicalPlanBuilder::from(plan)
                .explain(false, false)
                .unwrap()
                .build()
                .context(DataFusionPlanningSnafu),
            EXPLAIN_VERBOSE_NODE_NAME => LogicalPlanBuilder::from(plan)
                .explain(true, false)
                .unwrap()
                .build()
                .context(DataFusionPlanningSnafu),
            ALIAS_NODE_NAME => {
                let alias = expr
                    .as_any()
                    .downcast_ref::<AliasExpr>()
                    .context(UnexpectedPlanExprSnafu {
                        desc: "Expected AliasExpr",
                    })?
                    .alias
                    .clone();
                self.apply_alias(plan, alias)
            }
            _ => LogicalPlanBuilder::empty(true)
                .build()
                .context(DataFusionPlanningSnafu),
        }
    }

    /// Extract metric name from `__name__` matcher and set it into [PromPlannerContext].
    /// Returns a new [Matchers] that doesn't contain metric name matcher.
    ///
    /// Each call to this function means new selector is started. Thus, the context will be reset
    /// at first.
    ///
    /// Name rule:
    /// - if `name` is some, then the matchers MUST NOT contain `__name__` matcher.
    /// - if `name` is none, then the matchers MAY contain NONE OR MULTIPLE `__name__` matchers.
    #[allow(clippy::mutable_key_type)]
    fn preprocess_label_matchers(
        &mut self,
        label_matchers: &Matchers,
        name: &Option<String>,
    ) -> Result<Matchers> {
        self.ctx.reset();

        let metric_name;
        if let Some(name) = name.clone() {
            metric_name = Some(name);
            ensure!(
                label_matchers.find_matchers(METRIC_NAME).is_empty(),
                MultipleMetricMatchersSnafu
            );
        } else {
            let mut matches = label_matchers.find_matchers(METRIC_NAME);
            ensure!(!matches.is_empty(), NoMetricMatcherSnafu);
            ensure!(matches.len() == 1, MultipleMetricMatchersSnafu);
            ensure!(
                matches[0].op == MatchOp::Equal,
                UnsupportedMatcherOpSnafu {
                    matcher_op: matches[0].op.to_string(),
                    matcher: METRIC_NAME
                }
            );
            metric_name = matches.pop().map(|m| m.value);
        }

        self.ctx.table_name = metric_name;

        // Deduplicate in place instead of through a `HashSet`: the scan filter is built in
        // this order, and a hashed order makes the plan of one query vary between runs.
        let mut matchers: Vec<Matcher> = Vec::with_capacity(label_matchers.matchers.len());
        for matcher in &label_matchers.matchers {
            // TODO(ruihang): support other metric match ops
            if matcher.name == FIELD_COLUMN_MATCHER {
                self.ctx
                    .field_column_matcher
                    .get_or_insert_default()
                    .push(matcher.clone());
            } else if matcher.name == SCHEMA_COLUMN_MATCHER || matcher.name == DB_COLUMN_MATCHER {
                ensure!(
                    matcher.op == MatchOp::Equal,
                    UnsupportedMatcherOpSnafu {
                        matcher: matcher.name.clone(),
                        matcher_op: matcher.op.to_string(),
                    }
                );
                self.ctx.schema_name = Some(matcher.value.clone());
            } else if matcher.name != METRIC_NAME {
                self.ctx.selector_matcher.push(matcher.clone());
                if !matchers.contains(matcher) {
                    matchers.push(matcher.clone());
                }
            }
        }

        Ok(Matchers::new(matchers))
    }

    async fn selector_to_series_normalize_plan(
        &mut self,
        offset_duration: Millisecond,
        label_matchers: Matchers,
        is_range_selector: bool,
    ) -> Result<LogicalPlan> {
        // make table scan plan
        let table_ref = self.table_ref()?;
        let mut table_scan = self.create_table_scan_plan(table_ref.clone()).await?;
        let table_schema = table_scan.schema();

        // make filter exprs
        let mut scan_filters = Self::matchers_to_expr(label_matchers.clone(), table_schema)?;
        if let Some(time_index_filter) =
            self.build_time_index_filter(offset_duration, table_schema)?
        {
            scan_filters.push(time_index_filter);
        }
        if let Some(filter) = conjunction(scan_filters) {
            table_scan = LogicalPlanBuilder::from(table_scan)
                .filter(filter)
                .context(DataFusionPlanningSnafu)?
                .build()
                .context(DataFusionPlanningSnafu)?;
        }

        // make a projection plan if there is any `__field__` matcher
        if let Some(field_matchers) = &self.ctx.field_column_matcher {
            let col_set = self.ctx.field_columns.iter().collect::<HashSet<_>>();
            // opt-in set
            let mut result_set = HashSet::new();
            // opt-out set
            let mut reverse_set = HashSet::new();
            for matcher in field_matchers {
                match &matcher.op {
                    MatchOp::Equal => {
                        if col_set.contains(&matcher.value) {
                            let _ = result_set.insert(matcher.value.clone());
                        } else {
                            return Err(ColumnNotFoundSnafu {
                                col: matcher.value.clone(),
                            }
                            .build());
                        }
                    }
                    MatchOp::NotEqual => {
                        if col_set.contains(&matcher.value) {
                            let _ = reverse_set.insert(matcher.value.clone());
                        } else {
                            return Err(ColumnNotFoundSnafu {
                                col: matcher.value.clone(),
                            }
                            .build());
                        }
                    }
                    MatchOp::Re(regex) => {
                        for col in &self.ctx.field_columns {
                            if regex.is_match(col) {
                                let _ = result_set.insert(col.clone());
                            }
                        }
                    }
                    MatchOp::NotRe(regex) => {
                        for col in &self.ctx.field_columns {
                            if regex.is_match(col) {
                                let _ = reverse_set.insert(col.clone());
                            }
                        }
                    }
                }
            }
            // merge two set
            if result_set.is_empty() {
                result_set = col_set.into_iter().cloned().collect();
            }
            for col in reverse_set {
                let _ = result_set.remove(&col);
            }

            // mask the field columns in context using computed result set
            self.ctx.field_columns = self
                .ctx
                .field_columns
                .drain(..)
                .filter(|col| result_set.contains(col))
                .collect();

            let exprs = result_set
                .into_iter()
                .map(|col| DfExpr::Column(Column::new_unqualified(col)))
                .chain(self.create_tag_column_exprs()?)
                .chain(
                    self.ctx
                        .use_tsid
                        .then_some(DfExpr::Column(Column::new_unqualified(
                            DATA_SCHEMA_TSID_COLUMN_NAME,
                        ))),
                )
                .chain(Some(self.create_time_index_column_expr()?))
                .collect::<Vec<_>>();

            // reuse this variable for simplicity
            table_scan = LogicalPlanBuilder::from(table_scan)
                .project(exprs)
                .context(DataFusionPlanningSnafu)?
                .build()
                .context(DataFusionPlanningSnafu)?;
        }

        // make sort plan
        let series_key_columns = self.series_key_columns();

        let sort_exprs = if self.ctx.use_tsid {
            vec![
                DfExpr::Column(Column::from_name(DATA_SCHEMA_TSID_COLUMN_NAME)).sort(true, true),
                self.create_time_index_column_expr()?.sort(true, true),
            ]
        } else {
            self.create_tag_and_time_index_column_sort_exprs()?
        };

        let sort_plan = LogicalPlanBuilder::from(table_scan)
            .sort(sort_exprs)
            .context(DataFusionPlanningSnafu)?
            .build()
            .context(DataFusionPlanningSnafu)?;

        // make divide plan
        let time_index_column =
            self.ctx
                .time_index_column
                .clone()
                .with_context(|| TimeIndexNotFoundSnafu {
                    table: table_ref.to_string(),
                })?;
        let divide_plan = LogicalPlan::Extension(Extension {
            node: Arc::new(SeriesDivide::new(
                series_key_columns.clone(),
                time_index_column,
                sort_plan,
            )),
        });

        // make series_normalize plan
        if !is_range_selector && offset_duration == 0 {
            return Ok(divide_plan);
        }
        let series_normalize = SeriesNormalize::new(
            offset_duration,
            self.ctx
                .time_index_column
                .clone()
                .with_context(|| TimeIndexNotFoundSnafu {
                    table: table_ref.to_quoted_string(),
                })?,
            is_range_selector,
            series_key_columns,
            divide_plan,
        );
        let logical_plan = LogicalPlan::Extension(Extension {
            node: Arc::new(series_normalize),
        });

        Ok(logical_plan)
    }

    /// Convert [LabelModifier] to [Column] exprs for aggregation.
    /// Timestamp column and tag columns will be included.
    ///
    /// # Side effect
    ///
    /// This method will also change the tag columns in ctx if `update_ctx` is true.
    fn agg_modifier_to_col(
        &mut self,
        input_schema: &DFSchemaRef,
        modifier: &Option<LabelModifier>,
        update_ctx: bool,
    ) -> Result<Vec<DfExpr>> {
        match modifier {
            None => {
                if update_ctx {
                    self.ctx.tag_columns.clear();
                    self.ctx.aggregation_field_labels.clear();
                }
                Ok(vec![self.create_time_index_column_expr()?])
            }
            Some(LabelModifier::Include(labels)) => {
                if update_ctx {
                    // A `by(...)` label can name a value field of the input instead of a tag. The
                    // aggregate still reports it among its tag columns below, but unlike a tag it
                    // is a group key rather than a property of a series: its value varies between
                    // the samples of one series, so a matcher on it must stay above sample
                    // selection (#9242). Record it, before the tag columns are overwritten.
                    self.ctx.aggregation_field_labels = labels
                        .labels
                        .iter()
                        .filter(|label| {
                            self.ctx.field_columns.contains(label)
                                || !self.ctx.tag_columns.contains(label)
                                || self.ctx.aggregation_field_labels.contains(label)
                        })
                        .cloned()
                        .collect();
                    self.ctx.tag_columns.clear();
                }
                let mut exprs = Vec::with_capacity(labels.labels.len());
                for label in &labels.labels {
                    if is_metric_engine_internal_column(label) {
                        continue;
                    }
                    // nonexistence label will be ignored
                    if let Some(column_name) = Self::find_case_sensitive_column(input_schema, label)
                    {
                        exprs.push(DfExpr::Column(Column::from_name(column_name.clone())));

                        if update_ctx {
                            // update the tag columns in context
                            self.ctx.tag_columns.push(column_name);
                        }
                    }
                }
                // add timestamp column
                exprs.push(self.create_time_index_column_expr()?);

                Ok(exprs)
            }
            Some(LabelModifier::Exclude(labels)) => {
                let mut all_fields = input_schema
                    .fields()
                    .iter()
                    .map(|f| f.name())
                    .collect::<BTreeSet<_>>();

                // Exclude metric engine internal columns (not PromQL labels) from the implicit
                // "without" label set.
                all_fields.retain(|col| !is_metric_engine_internal_column(col.as_str()));

                // remove "without"-ed fields
                // nonexistence label will be ignored
                for label in &labels.labels {
                    let _ = all_fields.remove(label);
                }

                // remove time index and value fields
                if let Some(time_index) = &self.ctx.time_index_column {
                    let _ = all_fields.remove(time_index);
                }
                for value in &self.ctx.field_columns {
                    let _ = all_fields.remove(value);
                }

                if update_ctx {
                    // change the tag columns in context
                    self.ctx.tag_columns = all_fields.iter().map(|col| (*col).clone()).collect();
                    // `without(...)` drops the value fields of the input from its grouping labels,
                    // so a label stays a grouping label in name only if it survives in the input
                    // schema (e.g. an inner aggregation that grouped by it).
                    self.ctx
                        .aggregation_field_labels
                        .retain(|label| all_fields.iter().any(|col| *col == label));
                }

                // collect remaining fields and convert to col expr
                let mut exprs = all_fields
                    .into_iter()
                    .map(|c| DfExpr::Column(Column::from_name(c)))
                    .collect::<Vec<_>>();

                // add timestamp column
                exprs.push(self.create_time_index_column_expr()?);

                Ok(exprs)
            }
        }
    }

    // TODO(ruihang): ignore `MetricNameLabel` (`__name__`) matcher
    pub fn matchers_to_expr(
        label_matchers: Matchers,
        table_schema: &DFSchemaRef,
    ) -> Result<Vec<DfExpr>> {
        let mut exprs = Vec::with_capacity(label_matchers.matchers.len());
        for matcher in label_matchers.matchers {
            if matcher.name == SCHEMA_COLUMN_MATCHER
                || matcher.name == DB_COLUMN_MATCHER
                || matcher.name == FIELD_COLUMN_MATCHER
            {
                continue;
            }

            let accepts_empty = matcher.is_match("");
            let column_name = Self::find_case_sensitive_column(table_schema, matcher.name.as_str());
            // Prometheus reads a label a series does not carry as the empty
            // string. A row can miss a label two ways: the table has no column
            // for it, or the column exists but is NULL on that row — the latter
            // is the norm for logical metrics sharing a physical table, which
            // holds the union of their label columns.
            let mut null_label = None;
            let col = if let Some(column_name) = column_name {
                let column = DfExpr::Column(Column::from_name(&column_name));
                let field = table_schema
                    .index_of_column_by_name(None, &column_name)
                    .map(|index| table_schema.field(index));
                if accepts_empty
                    && field.is_some_and(|field| {
                        field.is_nullable()
                            && Self::string_value_data_type(field.data_type()).is_some()
                    })
                {
                    null_label = Some(column.clone().is_null());
                }
                column
            } else {
                DfExpr::Literal(ScalarValue::Utf8(Some(String::new())), None)
            };
            let lit = DfExpr::Literal(ScalarValue::Utf8(Some(matcher.value)), None);
            let expr = match matcher.op {
                MatchOp::Equal => col.eq(lit),
                MatchOp::NotEqual => col.not_eq(lit),
                MatchOp::Re(re) => {
                    // TODO(ruihang): a more programmatic way to handle this in datafusion

                    // This is a hack to handle `.+` and `.*`, and is not strictly correct
                    // `.` doesn't match newline (`\n`). Given this is in PromQL context,
                    // most of the time it's fine.
                    if re.as_str() == "^(?:.*)$" {
                        continue;
                    }
                    if re.as_str() == "^(?:.+)$" {
                        col.not_eq(DfExpr::Literal(
                            ScalarValue::Utf8(Some(String::new())),
                            None,
                        ))
                    } else {
                        DfExpr::BinaryExpr(BinaryExpr {
                            left: Box::new(col),
                            op: Operator::RegexMatch,
                            right: Box::new(DfExpr::Literal(
                                ScalarValue::Utf8(Some(re.as_str().to_string())),
                                None,
                            )),
                        })
                    }
                }
                MatchOp::NotRe(re) => {
                    if re.as_str() == "^(?:.*)$" {
                        DfExpr::Literal(ScalarValue::Boolean(Some(false)), None)
                    } else if re.as_str() == "^(?:.+)$" {
                        col.eq(DfExpr::Literal(
                            ScalarValue::Utf8(Some(String::new())),
                            None,
                        ))
                    } else {
                        DfExpr::BinaryExpr(BinaryExpr {
                            left: Box::new(col),
                            op: Operator::RegexNotMatch,
                            right: Box::new(DfExpr::Literal(
                                ScalarValue::Utf8(Some(re.as_str().to_string())),
                                None,
                            )),
                        })
                    }
                }
            };
            let expr = match null_label {
                Some(null_label) => null_label.or(expr),
                None => expr,
            };
            exprs.push(expr);
        }

        Ok(exprs)
    }

    fn find_case_sensitive_column(schema: &DFSchemaRef, column: &str) -> Option<String> {
        if is_metric_engine_internal_column(column) {
            return None;
        }
        schema
            .fields()
            .iter()
            .find(|field| field.name() == column)
            .map(|field| field.name().clone())
    }

    fn table_from_source(&self, source: &Arc<dyn TableSource>) -> Result<table::TableRef> {
        Ok(source
            .downcast_ref::<DefaultTableSource>()
            .context(UnknownTableSnafu)?
            .table_provider
            .downcast_ref::<DfTableProviderAdapter>()
            .context(UnknownTableSnafu)?
            .table())
    }

    fn table_ref(&self) -> Result<TableReference> {
        let table_name = self
            .ctx
            .table_name
            .clone()
            .context(TableNameNotFoundSnafu)?;

        // set schema name if `__schema__` is given
        let table_ref = if let Some(schema_name) = &self.ctx.schema_name {
            TableReference::partial(schema_name.as_str(), table_name.as_str())
        } else {
            TableReference::bare(table_name.as_str())
        };

        Ok(table_ref)
    }

    fn build_time_index_filter(
        &self,
        offset_duration: i64,
        schema: &DFSchemaRef,
    ) -> Result<Option<DfExpr>> {
        let start = self.ctx.start;
        let end = self.ctx.end;
        if end < start {
            return InvalidTimeRangeSnafu { start, end }.fail();
        }
        let time_index_expr = self.create_time_index_column_expr()?;
        let time_index_name = self.ctx.time_index_column.as_ref().unwrap();
        let unit = schema
            .index_of_column_by_name(None, time_index_name)
            .and_then(|index| match schema.field(index).data_type() {
                ArrowDataType::Timestamp(unit, _) => Some(*unit),
                _ => None,
            })
            .unwrap_or(ArrowTimeUnit::Millisecond);
        let native_value = |milliseconds: i128| match unit {
            ArrowTimeUnit::Second => milliseconds.div_euclid(1_000),
            ArrowTimeUnit::Millisecond => milliseconds,
            ArrowTimeUnit::Microsecond => milliseconds * 1_000,
            ArrowTimeUnit::Nanosecond => milliseconds * 1_000_000,
        };
        let scalar = |milliseconds: i128| -> Option<ScalarValue> {
            let value = i64::try_from(native_value(milliseconds)).ok()?;
            Some(match unit {
                ArrowTimeUnit::Second => ScalarValue::TimestampSecond(Some(value), None),
                ArrowTimeUnit::Millisecond => ScalarValue::TimestampMillisecond(Some(value), None),
                ArrowTimeUnit::Microsecond => ScalarValue::TimestampMicrosecond(Some(value), None),
                ArrowTimeUnit::Nanosecond => ScalarValue::TimestampNanosecond(Some(value), None),
            })
        };
        let window = self.ctx.range.unwrap_or(self.ctx.lookback_delta);
        let filter = |lower_ms: i128, upper_ms: i128| {
            let lower_value = native_value(lower_ms);
            let upper_value = native_value(upper_ms);
            if lower_value > i128::from(i64::MAX) || upper_value < i128::from(i64::MIN) {
                return Some(lit(false));
            }
            let lower_filter = (lower_value >= i128::from(i64::MIN)).then(|| {
                let lower = DfExpr::Literal(scalar(lower_ms).unwrap(), None);
                if window == 0 {
                    time_index_expr.clone().gt_eq(lower)
                } else if unit == ArrowTimeUnit::Millisecond
                    && let Some(inclusive_lower) = lower_ms
                        .checked_add(1)
                        .and_then(|lower| i64::try_from(lower).ok())
                        .and_then(|lower| scalar(i128::from(lower)))
                {
                    time_index_expr
                        .clone()
                        .gt_eq(DfExpr::Literal(inclusive_lower, None))
                } else {
                    time_index_expr.clone().gt(lower)
                }
            });
            let upper_filter = (upper_value <= i128::from(i64::MAX)).then(|| {
                time_index_expr
                    .clone()
                    .lt_eq(DfExpr::Literal(scalar(upper_ms).unwrap(), None))
            });

            // An underflowing lower bound must not discard a representable upper
            // bound: without it, LastRow could retain a future row and discard the
            // older eligible sample before the manipulator can check its time.
            match (lower_filter, upper_filter) {
                (Some(lower), Some(upper)) => Some(lower.and(upper)),
                (Some(filter), None) | (None, Some(filter)) => Some(filter),
                (None, None) => None,
            }
        };
        let bounds = |timestamp: i64| {
            let upper = i128::from(timestamp) - i128::from(offset_duration);
            (upper - i128::from(window), upper)
        };
        let num_points = (end as i128 - start as i128) / self.ctx.interval as i128;
        if num_points > MAX_SCATTER_POINTS as i128 || self.ctx.interval <= INTERVAL_1H {
            let (lower, _) = bounds(start);
            let (_, upper) = bounds(end);
            return Ok(filter(lower, upper));
        }
        let mut filters = Vec::new();
        for timestamp in (start..=end).step_by(self.ctx.interval as usize) {
            let (lower, upper) = bounds(timestamp);
            let Some(filter) = filter(lower, upper) else {
                // A point whose native bounds cannot be represented may cover the whole native
                // time domain, so its disjunct cannot be omitted.
                return Ok(None);
            };
            filters.push(filter);
        }
        Ok(filters.into_iter().reduce(DfExpr::or))
    }

    /// Create a table scan plan and a filter plan with given filter.
    ///
    /// # Panic
    /// If the filter is empty
    async fn create_table_scan_plan(&mut self, table_ref: TableReference) -> Result<LogicalPlan> {
        let provider = self
            .table_provider
            .resolve_table(table_ref.clone())
            .await
            .context(CatalogSnafu)?;

        let logical_table = self.table_from_source(&provider)?;

        // Try to rewrite the table scan to physical table scan if possible.
        let mut maybe_phy_table_ref = table_ref.clone();
        let mut scan_provider = provider;
        let mut table_id_filter: Option<u32> = None;

        // If it's a metric engine logical table, scan its physical table directly and filter by
        // `__table_id = logical_table_id` to get access to internal columns like `__tsid`.
        if logical_table.table_info().meta.engine == METRIC_ENGINE_NAME
            && let Some(physical_table_name) = logical_table
                .table_info()
                .meta
                .options
                .extra_options
                .get(LOGICAL_TABLE_METADATA_KEY)
        {
            let physical_table_ref = if let Some(schema_name) = &self.ctx.schema_name {
                TableReference::partial(schema_name.as_str(), physical_table_name.as_str())
            } else {
                TableReference::bare(physical_table_name.as_str())
            };

            let physical_provider = match self
                .table_provider
                .resolve_table(physical_table_ref.clone())
                .await
            {
                Ok(provider) => provider,
                Err(e) if e.status_code() == StatusCode::TableNotFound => {
                    // Fall back to scanning the logical table. It still works, but without
                    // `__tsid` optimization.
                    scan_provider.clone()
                }
                Err(e) => return Err(e).context(CatalogSnafu),
            };

            if !Arc::ptr_eq(&physical_provider, &scan_provider) {
                // Only rewrite when internal columns exist in physical schema.
                let physical_table = self.table_from_source(&physical_provider)?;

                let has_table_id = physical_table
                    .schema()
                    .column_schema_by_name(DATA_SCHEMA_TABLE_ID_COLUMN_NAME)
                    .is_some();
                let has_tsid = physical_table
                    .schema()
                    .column_schema_by_name(DATA_SCHEMA_TSID_COLUMN_NAME)
                    .is_some_and(|col| matches!(col.data_type, ConcreteDataType::UInt64(_)));

                if has_table_id && has_tsid {
                    scan_provider = physical_provider;
                    maybe_phy_table_ref = physical_table_ref;
                    table_id_filter = Some(logical_table.table_info().ident.table_id);
                }
            }
        }

        let scan_table = self.table_from_source(&scan_provider)?;

        let use_tsid = table_id_filter.is_some()
            && scan_table
                .schema()
                .column_schema_by_name(DATA_SCHEMA_TSID_COLUMN_NAME)
                .is_some_and(|col| matches!(col.data_type, ConcreteDataType::UInt64(_)));
        self.ctx.use_tsid = use_tsid;

        let all_table_tags = self.ctx.tag_columns.clone();

        let scan_tag_columns = if use_tsid {
            let mut scan_tags = self.ctx.tag_columns.clone();
            for matcher in &self.ctx.selector_matcher {
                if is_metric_engine_internal_column(&matcher.name) {
                    continue;
                }
                if all_table_tags.iter().any(|tag| tag == &matcher.name) {
                    scan_tags.push(matcher.name.clone());
                }
            }
            scan_tags.sort_unstable();
            scan_tags.dedup();
            scan_tags
        } else {
            self.ctx.tag_columns.clone()
        };

        let time_index_data_type = scan_table
            .schema()
            .timestamp_column()
            .with_context(|| TimeIndexNotFoundSnafu {
                table: maybe_phy_table_ref.to_quoted_string(),
            })?
            .data_type
            .clone();
        let is_time_index_second =
            time_index_data_type == ConcreteDataType::timestamp_second_datatype();

        let scan_projection = if table_id_filter.is_some() {
            let mut required_columns = HashSet::new();
            required_columns.insert(DATA_SCHEMA_TABLE_ID_COLUMN_NAME.to_string());
            required_columns.insert(self.ctx.time_index_column.clone().with_context(|| {
                TimeIndexNotFoundSnafu {
                    table: maybe_phy_table_ref.to_quoted_string(),
                }
            })?);
            for col in &scan_tag_columns {
                required_columns.insert(col.clone());
            }
            for col in &self.ctx.field_columns {
                required_columns.insert(col.clone());
            }
            if use_tsid {
                required_columns.insert(DATA_SCHEMA_TSID_COLUMN_NAME.to_string());
            }

            let arrow_schema = scan_table.schema().arrow_schema().clone();
            Some(
                arrow_schema
                    .fields()
                    .iter()
                    .enumerate()
                    .filter(|(_, field)| required_columns.contains(field.name().as_str()))
                    .map(|(idx, _)| idx)
                    .collect::<Vec<_>>(),
            )
        } else {
            None
        };

        let mut scan_plan =
            LogicalPlanBuilder::scan(maybe_phy_table_ref.clone(), scan_provider, scan_projection)
                .context(DataFusionPlanningSnafu)?
                .build()
                .context(DataFusionPlanningSnafu)?;

        if let Some(table_id) = table_id_filter {
            scan_plan = LogicalPlanBuilder::from(scan_plan)
                .filter(
                    DfExpr::Column(Column::from_name(DATA_SCHEMA_TABLE_ID_COLUMN_NAME))
                        .eq(lit(table_id)),
                )
                .context(DataFusionPlanningSnafu)?
                .alias(table_ref.clone()) // rename the relation back to logical table's name after filtering
                .context(DataFusionPlanningSnafu)?
                .build()
                .context(DataFusionPlanningSnafu)?;
        }

        if is_time_index_second {
            // Promote seconds so millisecond offsets remain exact; retain finer precision.
            // Later manipulators compare native sample ticks, while PromQL evaluation and
            // emitted timestamps remain millisecond-based, so this projection must not
            // silently truncate a finer-grained time index.
            let expr: Vec<_> = self
                .create_field_column_exprs()?
                .into_iter()
                .chain(
                    scan_tag_columns
                        .iter()
                        .map(|tag| DfExpr::Column(Column::from_name(tag))),
                )
                .chain(self.ctx.use_tsid.then_some(DfExpr::Column(Column::new(
                    Some(table_ref.clone()),
                    DATA_SCHEMA_TSID_COLUMN_NAME.to_string(),
                ))))
                .chain(Some(DfExpr::Alias(Alias {
                    expr: Box::new(DfExpr::Cast(Cast::new(
                        Box::new(self.create_time_index_column_expr()?),
                        ArrowDataType::Timestamp(ArrowTimeUnit::Millisecond, None),
                    ))),
                    relation: Some(table_ref.clone()),
                    name: self
                        .ctx
                        .time_index_column
                        .as_ref()
                        .with_context(|| TimeIndexNotFoundSnafu {
                            table: table_ref.to_quoted_string(),
                        })?
                        .clone(),
                    metadata: None,
                })))
                .collect::<Vec<_>>();
            scan_plan = LogicalPlanBuilder::from(scan_plan)
                .project(expr)
                .context(DataFusionPlanningSnafu)?
                .build()
                .context(DataFusionPlanningSnafu)?;
        } else if table_id_filter.is_some()
            || time_index_data_type == ConcreteDataType::timestamp_microsecond_datatype()
            || time_index_data_type == ConcreteDataType::timestamp_nanosecond_datatype()
        {
            // Drop the internal `__table_id` column after filtering and preserve PromQL's
            // field/tag/timestamp column order for native microsecond/nanosecond timestamps.
            // Keeping the original time column also lets the existing ordering hints
            // use PerSeries scans without a cast, repartition, and sort. This benefits
            // multi-evaluation selectors too; only a single evaluation can use LastRow.
            let project_exprs = self
                .create_field_column_exprs()?
                .into_iter()
                .chain(
                    scan_tag_columns
                        .iter()
                        .map(|tag| DfExpr::Column(Column::from_name(tag))),
                )
                .chain(
                    self.ctx
                        .use_tsid
                        .then_some(DfExpr::Column(Column::from_name(
                            DATA_SCHEMA_TSID_COLUMN_NAME,
                        ))),
                )
                .chain(Some(self.create_time_index_column_expr()?))
                .collect::<Vec<_>>();

            scan_plan = LogicalPlanBuilder::from(scan_plan)
                .project(project_exprs)
                .context(DataFusionPlanningSnafu)?
                .build()
                .context(DataFusionPlanningSnafu)?;
        }

        let result = LogicalPlanBuilder::from(scan_plan)
            .build()
            .context(DataFusionPlanningSnafu)?;
        Ok(result)
    }

    fn collect_row_key_tag_columns_from_plan(
        &self,
        plan: &LogicalPlan,
    ) -> Result<BTreeSet<String>> {
        fn walk(
            planner: &PromPlanner,
            plan: &LogicalPlan,
            out: &mut BTreeSet<String>,
        ) -> Result<()> {
            // Derived PromQL plans may contain non-Greptime scans without row-key metadata.
            if let LogicalPlan::TableScan(scan) = plan
                && let Ok(table) = planner.table_from_source(&scan.source)
            {
                for col in table.table_info().meta.row_key_column_names() {
                    if col != DATA_SCHEMA_TABLE_ID_COLUMN_NAME
                        && col != DATA_SCHEMA_TSID_COLUMN_NAME
                        && !is_metric_engine_internal_column(col)
                    {
                        out.insert(col.clone());
                    }
                }
            }

            for input in plan.inputs() {
                walk(planner, input, out)?;
            }
            Ok(())
        }

        let mut out = BTreeSet::new();
        walk(self, plan, &mut out)?;
        Ok(out)
    }

    /// Setup [PromPlannerContext]'s state fields.
    ///
    /// Returns a logical plan for an empty metric.
    async fn setup_context(&mut self) -> Result<Option<LogicalPlan>> {
        let table_ref = self.table_ref()?;
        let source = match self.table_provider.resolve_table(table_ref.clone()).await {
            Err(e) if e.status_code() == StatusCode::TableNotFound => {
                let plan = self.setup_context_for_empty_metric()?;
                return Ok(Some(plan));
            }
            res => res.context(CatalogSnafu)?,
        };
        let table = self.table_from_source(&source)?;

        // set time index column name
        let time_index = table
            .schema()
            .timestamp_column()
            .with_context(|| TimeIndexNotFoundSnafu {
                table: table_ref.to_quoted_string(),
            })?
            .name
            .clone();
        self.ctx.time_index_column = Some(time_index);

        // set values columns
        let values = table
            .table_info()
            .meta
            .field_column_names()
            .cloned()
            .collect();
        self.ctx.field_columns = values;

        // set primary key (tag) columns
        let tags = table
            .table_info()
            .meta
            .row_key_column_names()
            .filter(|col| {
                // remove metric engine's internal columns
                col != &DATA_SCHEMA_TABLE_ID_COLUMN_NAME && col != &DATA_SCHEMA_TSID_COLUMN_NAME
            })
            .cloned()
            .collect();
        self.ctx.tag_columns = tags;
        // The operand is a plain selector: its tag columns are the table's.
        self.ctx.aggregation_field_labels.clear();

        self.ctx.use_tsid = false;

        Ok(None)
    }

    /// Setup [PromPlannerContext]'s state fields for a non existent table
    /// without any rows.
    fn setup_context_for_empty_metric(&mut self) -> Result<LogicalPlan> {
        self.ctx.time_index_column = Some(SPECIAL_TIME_FUNCTION.to_string());
        self.ctx.reset_table_name_and_schema();
        self.ctx.tag_columns = vec![];
        self.ctx.aggregation_field_labels.clear();
        self.ctx.field_columns = vec![DEFAULT_FIELD_COLUMN.to_string()];
        self.ctx.use_tsid = false;

        // The table doesn't have any data, so we set start to 0 and end to -1.
        let plan = LogicalPlan::Extension(Extension {
            node: Arc::new(
                EmptyMetric::new(
                    0,
                    -1,
                    self.ctx.interval,
                    SPECIAL_TIME_FUNCTION.to_string(),
                    DEFAULT_FIELD_COLUMN.to_string(),
                    Some(lit(0.0f64)),
                )
                .context(DataFusionPlanningSnafu)?,
            ),
        });
        Ok(plan)
    }

    // TODO(ruihang): insert column expr
    fn create_function_args(&self, args: &[Box<PromExpr>]) -> Result<FunctionArgs> {
        let mut result = FunctionArgs::default();

        for arg in args {
            // First try to parse as literal expression (including binary expressions like 100.0 + 3.0)
            if let Some(expr) = Self::try_build_literal_expr(arg) {
                result.literals.push(ScalarParam::Static(expr));
            } else if Self::is_scalar_call(arg) {
                // A `scalar(...)` call is a runtime scalar parameter rather than
                // a second vector input.
                result
                    .literals
                    .push(ScalarParam::Dynamic(arg.as_ref().clone()));
            } else {
                // If not a literal, treat as vector input
                match arg.as_ref() {
                    PromExpr::Subquery(_)
                    | PromExpr::VectorSelector(_)
                    | PromExpr::MatrixSelector(_)
                    | PromExpr::Extension(_)
                    | PromExpr::Aggregate(_)
                    | PromExpr::Paren(_)
                    | PromExpr::Call(_)
                    | PromExpr::Binary(_)
                    | PromExpr::Unary(_) => {
                        if result.input.replace(*arg.clone()).is_some() {
                            MultipleVectorSnafu { expr: *arg.clone() }.fail()?;
                        }
                    }

                    _ => {
                        let param = Self::get_param_as_scalar_expr(Some(arg.as_ref()), None, None)?;
                        result.literals.push(param);
                    }
                }
            }
        }

        Ok(result)
    }

    /// Returns true if `expr` is a `scalar(...)` call, i.e. a scalar value that
    /// is only known when the query runs.
    fn is_scalar_call(expr: &PromExpr) -> bool {
        matches!(expr, PromExpr::Call(Call { func, .. }) if func.name == SCALAR_FUNCTION)
    }

    /// Rejects a runtime `scalar(...)` parameter of a function whose parameter
    /// has to be known while planning.
    fn ensure_static_scalar_param(func_name: &str, param: &PromExpr) -> Result<()> {
        ensure!(
            !Self::is_scalar_call(param),
            FunctionInvalidArgumentSnafu {
                fn_name: format!("{func_name}: {DYNAMIC_SCALAR_UNSUPPORTED_FOR_FUNCTION}"),
            }
        );

        Ok(())
    }

    /// Builds the expressions of the scalar parameters of a call.
    ///
    /// A [`ScalarParam::Dynamic`] parameter (`scalar(...)`) is evaluated when
    /// the query runs, so it is only accepted for the functions in
    /// [`DYNAMIC_SCALAR_FUNCTIONS`] and only for an instant query, where the
    /// single computed value can be broadcast to the rows of the input.
    async fn build_scalar_params(
        &mut self,
        func_name: &str,
        params: &[ScalarParam],
        query_engine_state: &QueryEngineState,
    ) -> Result<Vec<DfExpr>> {
        let mut exprs = Vec::with_capacity(params.len());

        for param in params {
            match param {
                ScalarParam::Static(expr) => exprs.push(expr.clone()),
                ScalarParam::Dynamic(param_expr) => {
                    // A dynamic parameter is computed once for every evaluation
                    // timestamp, so it can only be broadcast to the input's rows
                    // when the query has a single evaluation timestamp.
                    ensure!(
                        self.ctx.start == self.ctx.end,
                        FunctionInvalidArgumentSnafu {
                            fn_name: format!(
                                "{func_name}: {DYNAMIC_SCALAR_UNSUPPORTED_IN_RANGE_QUERY}"
                            ),
                        }
                    );
                    ensure!(
                        DYNAMIC_SCALAR_FUNCTIONS.contains(&func_name),
                        FunctionInvalidArgumentSnafu {
                            fn_name: format!(
                                "{func_name}: {DYNAMIC_SCALAR_UNSUPPORTED_FOR_FUNCTION}"
                            ),
                        }
                    );
                    exprs.push(
                        self.build_dynamic_scalar_subquery(
                            func_name,
                            param_expr,
                            query_engine_state,
                        )
                        .await?,
                    );
                }
            }
        }

        Ok(exprs)
    }

    /// Plans a runtime `scalar(...)` parameter as a subquery and returns the
    /// expression reading its value column as a scalar.
    ///
    /// The subquery is planned on its own, so the planner state of the call the
    /// parameter belongs to is restored before returning: that call still plans
    /// its own input from it.
    async fn build_dynamic_scalar_subquery(
        &mut self,
        func_name: &str,
        param_expr: &PromExpr,
        query_engine_state: &QueryEngineState,
    ) -> Result<DfExpr> {
        let outer_ctx = self.ctx.clone();
        let param_plan = self.prom_expr_to_plan(param_expr, query_engine_state).await;
        // The value column of the parameter's plan, e.g. `scalar(<field>)` of
        // the `ScalarCalculate` plan that plans `scalar(...)`.
        let value_column = self.ctx.field_columns.first().cloned();
        self.ctx = outer_ctx;
        let param_plan = param_plan?;
        let value_column = value_column.with_context(|| FunctionInvalidArgumentSnafu {
            fn_name: format!("{func_name}: scalar parameter without a value column"),
        })?;

        // A scalar subquery must produce a single column, so only the value
        // column of the parameter's plan is kept.
        let subquery = LogicalPlanBuilder::from(param_plan)
            .project(vec![DfExpr::Column(Column::from_name(&value_column))])
            .context(DataFusionPlanningSnafu)?
            .build()
            .context(DataFusionPlanningSnafu)?;

        Ok(scalar_subquery(Arc::new(subquery)))
    }

    fn create_mixed_range_function_exprs(
        &mut self,
        func: &Function,
        mut other_input_exprs: VecDeque<DfExpr>,
        float_field: &str,
        histogram_field: &str,
        input_schema: &DFSchemaRef,
        range_fold_offset: Option<Millisecond>,
    ) -> Result<Option<Vec<DfExpr>>> {
        let returns_histogram = matches!(
            func.name,
            "rate"
                | "increase"
                | "delta"
                | "idelta"
                | "irate"
                | "avg_over_time"
                | "sum_over_time"
                | "last_over_time"
        );
        if !returns_histogram
            && !matches!(
                func.name,
                "changes"
                    | "resets"
                    | "deriv"
                    | "min_over_time"
                    | "max_over_time"
                    | "count_over_time"
                    | "absent_over_time"
                    | "present_over_time"
                    | "stddev_over_time"
                    | "stdvar_over_time"
                    | "quantile_over_time"
                    | "predict_linear"
                    | "double_exponential_smoothing"
                    | "holt_winters"
            )
        {
            return Ok(None);
        }

        if func.name == "predict_linear" {
            other_input_exprs[0] = DfExpr::Cast(Cast::new(
                Box::new(other_input_exprs[0].clone()),
                ArrowDataType::Int64,
            ));
            // Same evaluation instant as in the non-mixed path; it follows the other inputs so the
            // float UDF below is called as `predict_linear(ts_range, value_range, t, eval_ts)`.
            // The regression is only defined for a folded window, so a missing fold offset is an
            // error instead of a default.
            other_input_exprs.push_back(self.create_range_eval_ts_expr(
                range_fold_offset.context(ExpectRangeSelectorSnafu)?,
                input_schema,
            )?);
        }

        let timestamp_range = DfExpr::Column(Column::from_name(
            RangeManipulate::build_timestamp_range_name(
                self.ctx.time_index_column.as_ref().unwrap(),
            ),
        ));
        let float_range = DfExpr::Column(Column::from_name(float_field));
        let histogram_range = DfExpr::Column(Column::from_name(histogram_field));
        let mut args = Vec::with_capacity(other_input_exprs.len() + 6);
        args.push(lit(func.name));
        args.push(timestamp_range.clone());
        args.push(float_range.clone());
        args.push(histogram_range.clone());
        args.extend(other_input_exprs);
        if matches!(func.name, "rate" | "increase" | "delta") {
            args.push(self.create_time_index_column_expr()?);
            args.push(lit(self.ctx.range.context(ExpectRangeSelectorSnafu)?));
        }

        let mut float_expr = DfExpr::ScalarFunction(ScalarFunction {
            func: Arc::new(MixedRange::float_udf(self.promql_annotations.clone())),
            args: args.clone(),
        });
        if matches!(func.name, "rate" | "increase") {
            let raw_delta_function = if func.name == "rate" {
                "raw_delta_rate"
            } else {
                "raw_delta_increase"
            };
            let delta_sum = DfExpr::ScalarFunction(ScalarFunction {
                func: Arc::new(MixedRange::float_udf(self.promql_annotations.clone())),
                args: vec![
                    lit(raw_delta_function),
                    timestamp_range,
                    float_range,
                    histogram_range,
                ],
            });
            float_expr = self.select_delta_range_math(
                func.name,
                input_schema,
                self.ctx.range.context(ExpectRangeSelectorSnafu)?,
                delta_sum,
                float_expr,
            )?;
        }
        let exprs = if returns_histogram {
            self.ctx.field_columns = vec![float_field.to_string(), histogram_field.to_string()];
            vec![
                float_expr.alias(float_field),
                DfExpr::ScalarFunction(ScalarFunction {
                    func: Arc::new(MixedRange::histogram_udf(self.promql_annotations.clone())),
                    args,
                })
                .alias(histogram_field),
            ]
        } else {
            let display_name = if func.name == "predict_linear" {
                // The evaluation instant is the private last argument of the mixed float UDF
                // call; keep it out of the output column name like on the non-mixed path.
                Self::name_without_last_arg(&float_expr)
            } else {
                float_expr.schema_name().to_string()
            };
            self.ctx.field_columns = vec![display_name.clone()];
            vec![float_expr.alias(display_name)]
        };
        Ok(Some(exprs))
    }

    /// Creates function expressions for projection and returns the expressions and new tags.
    ///
    /// # Side Effects
    ///
    /// This method will update [PromPlannerContext]'s fields and tags if needed.
    fn create_function_expr(
        &mut self,
        func: &Function,
        other_input_exprs: Vec<DfExpr>,
        input_schema: &DFSchemaRef,
        query_engine_state: &QueryEngineState,
        range_fold_offset: Option<Millisecond>,
    ) -> Result<(Vec<DfExpr>, Vec<String>)> {
        // TODO(ruihang): check function args list
        let mut other_input_exprs: VecDeque<DfExpr> = other_input_exprs.into();
        if let Some((float_field, histogram_field)) =
            Self::alternative_sample_range_columns(input_schema, &self.ctx.field_columns)
                .map(|(float, histogram)| (float.to_string(), histogram.to_string()))
            && let Some(exprs) = self.create_mixed_range_function_exprs(
                func,
                other_input_exprs.clone(),
                &float_field,
                &histogram_field,
                input_schema,
                range_fold_offset,
            )?
        {
            return Ok((exprs, vec![]));
        }
        let alternative_samples =
            Self::field_columns_are_alternative_samples(input_schema, &self.ctx.field_columns);
        let all_field_columns_are_native_histogram_ranges =
            self.all_field_columns_are_native_histogram_ranges(input_schema);

        // TODO(ruihang): set this according to in-param list
        let field_column_pos = 0;
        let mut exprs = Vec::with_capacity(self.ctx.field_columns.len());
        // New labels after executing the function, e.g. `label_replace` etc.
        let mut new_tags = vec![];
        let promql_annotations = self.promql_annotations.clone();
        let native_histogram_drop_udf = |name: &str| {
            Arc::new(NativeHistogramDrop::float_null_udf(
                format!(
                    "{name}: dropped native histogram samples because this function is not supported for native histograms"
                ),
                promql_annotations.clone(),
            ))
        };
        let scalar_func = match func.name {
            "increase" => {
                if all_field_columns_are_native_histogram_ranges {
                    ScalarFunc::ExtrapolateUdf(
                        Arc::new(NativeHistogramIncrease::scalar_udf_with_collector(
                            self.promql_annotations.clone(),
                        )),
                        self.ctx.range.context(ExpectRangeSelectorSnafu)?,
                    )
                } else {
                    ScalarFunc::ExtrapolateUdf(
                        Arc::new(Increase::scalar_udf()),
                        self.ctx.range.context(ExpectRangeSelectorSnafu)?,
                    )
                }
            }
            "rate" => {
                if all_field_columns_are_native_histogram_ranges {
                    ScalarFunc::ExtrapolateUdf(
                        Arc::new(NativeHistogramRate::scalar_udf_with_collector(
                            self.promql_annotations.clone(),
                        )),
                        self.ctx.range.context(ExpectRangeSelectorSnafu)?,
                    )
                } else {
                    ScalarFunc::ExtrapolateUdf(
                        Arc::new(Rate::scalar_udf()),
                        self.ctx.range.context(ExpectRangeSelectorSnafu)?,
                    )
                }
            }
            "delta" => {
                if all_field_columns_are_native_histogram_ranges {
                    ScalarFunc::ExtrapolateUdf(
                        Arc::new(NativeHistogramDelta::scalar_udf_with_collector(
                            self.promql_annotations.clone(),
                        )),
                        self.ctx.range.context(ExpectRangeSelectorSnafu)?,
                    )
                } else {
                    ScalarFunc::ExtrapolateUdf(
                        Arc::new(Delta::scalar_udf()),
                        self.ctx.range.context(ExpectRangeSelectorSnafu)?,
                    )
                }
            }
            "idelta" => {
                if all_field_columns_are_native_histogram_ranges {
                    ScalarFunc::Udf(Arc::new(NativeHistogramIDelta::scalar_udf_with_collector(
                        self.promql_annotations.clone(),
                    )))
                } else {
                    ScalarFunc::Udf(Arc::new(IDelta::<false>::scalar_udf()))
                }
            }
            "irate" => {
                if all_field_columns_are_native_histogram_ranges {
                    ScalarFunc::Udf(Arc::new(NativeHistogramIRate::scalar_udf_with_collector(
                        self.promql_annotations.clone(),
                    )))
                } else {
                    ScalarFunc::Udf(Arc::new(IDelta::<true>::scalar_udf()))
                }
            }
            "resets" => {
                if all_field_columns_are_native_histogram_ranges {
                    ScalarFunc::Udf(Arc::new(NativeHistogramResets::scalar_udf()))
                } else {
                    ScalarFunc::Udf(Arc::new(Resets::scalar_udf()))
                }
            }
            "changes" => {
                if all_field_columns_are_native_histogram_ranges {
                    ScalarFunc::Udf(Arc::new(NativeHistogramChanges::scalar_udf()))
                } else {
                    ScalarFunc::Udf(Arc::new(Changes::scalar_udf()))
                }
            }
            "deriv" => {
                if all_field_columns_are_native_histogram_ranges {
                    ScalarFunc::Udf(native_histogram_drop_udf(func.name))
                } else {
                    ScalarFunc::Udf(Arc::new(Deriv::scalar_udf()))
                }
            }
            "avg_over_time" => {
                if all_field_columns_are_native_histogram_ranges {
                    ScalarFunc::Udf(Arc::new(
                        NativeHistogramAvgOverTime::scalar_udf_with_collector(
                            self.promql_annotations.clone(),
                        ),
                    ))
                } else {
                    ScalarFunc::Udf(Arc::new(AvgOverTime::scalar_udf()))
                }
            }
            "min_over_time" => {
                if all_field_columns_are_native_histogram_ranges {
                    ScalarFunc::Udf(native_histogram_drop_udf(func.name))
                } else {
                    ScalarFunc::Udf(Arc::new(MinOverTime::scalar_udf()))
                }
            }
            "max_over_time" => {
                if all_field_columns_are_native_histogram_ranges {
                    ScalarFunc::Udf(native_histogram_drop_udf(func.name))
                } else {
                    ScalarFunc::Udf(Arc::new(MaxOverTime::scalar_udf()))
                }
            }
            "sum_over_time" => {
                if all_field_columns_are_native_histogram_ranges {
                    ScalarFunc::Udf(Arc::new(
                        NativeHistogramSumOverTime::scalar_udf_with_collector(
                            self.promql_annotations.clone(),
                        ),
                    ))
                } else {
                    ScalarFunc::Udf(Arc::new(SumOverTime::scalar_udf()))
                }
            }
            "count_over_time" => {
                if all_field_columns_are_native_histogram_ranges {
                    ScalarFunc::Udf(Arc::new(NativeHistogramCountOverTime::scalar_udf()))
                } else {
                    ScalarFunc::Udf(Arc::new(CountOverTime::scalar_udf()))
                }
            }
            "last_over_time" => {
                if all_field_columns_are_native_histogram_ranges {
                    ScalarFunc::Udf(Arc::new(NativeHistogramLastOverTime::scalar_udf()))
                } else {
                    ScalarFunc::Udf(Arc::new(LastOverTime::scalar_udf()))
                }
            }
            "absent_over_time" => {
                if all_field_columns_are_native_histogram_ranges {
                    ScalarFunc::Udf(Arc::new(NativeHistogramAbsentOverTime::scalar_udf()))
                } else {
                    ScalarFunc::Udf(Arc::new(AbsentOverTime::scalar_udf()))
                }
            }
            "present_over_time" => {
                if all_field_columns_are_native_histogram_ranges {
                    ScalarFunc::Udf(Arc::new(NativeHistogramPresentOverTime::scalar_udf()))
                } else {
                    ScalarFunc::Udf(Arc::new(PresentOverTime::scalar_udf()))
                }
            }
            "stddev_over_time" => {
                if all_field_columns_are_native_histogram_ranges {
                    ScalarFunc::Udf(native_histogram_drop_udf(func.name))
                } else {
                    ScalarFunc::Udf(Arc::new(StddevOverTime::scalar_udf()))
                }
            }
            "stdvar_over_time" => {
                if all_field_columns_are_native_histogram_ranges {
                    ScalarFunc::Udf(native_histogram_drop_udf(func.name))
                } else {
                    ScalarFunc::Udf(Arc::new(StdvarOverTime::scalar_udf()))
                }
            }
            "quantile_over_time" => {
                if all_field_columns_are_native_histogram_ranges {
                    ScalarFunc::Udf(native_histogram_drop_udf(func.name))
                } else {
                    ScalarFunc::Udf(Arc::new(QuantileOverTime::scalar_udf()))
                }
            }
            "predict_linear" => {
                if all_field_columns_are_native_histogram_ranges {
                    ScalarFunc::Udf(native_histogram_drop_udf(func.name))
                } else {
                    other_input_exprs[0] = DfExpr::Cast(Cast::new(
                        Box::new(other_input_exprs[0].clone()),
                        ArrowDataType::Int64,
                    ));
                    // The prediction starts at the evaluation instant of the step, which the
                    // window's fold offset recovers from the row's time index; see
                    // [`Self::create_range_eval_ts_expr`]. It is appended last, so the UDF is
                    // called as `predict_linear(ts_range, value_range, t, eval_ts)`. The
                    // regression is only defined for a folded window, so a missing fold offset
                    // is an error instead of a default.
                    other_input_exprs.push_back(self.create_range_eval_ts_expr(
                        range_fold_offset.context(ExpectRangeSelectorSnafu)?,
                        input_schema,
                    )?);
                    ScalarFunc::Udf(Arc::new(PredictLinear::scalar_udf()))
                }
            }
            "double_exponential_smoothing" | "holt_winters" => {
                if all_field_columns_are_native_histogram_ranges {
                    ScalarFunc::Udf(native_histogram_drop_udf(func.name))
                } else {
                    ScalarFunc::Udf(Arc::new(DoubleExponentialSmoothing::scalar_udf()))
                }
            }
            "histogram_count" => {
                ScalarFunc::NativeHistogramUdf(Arc::new(NativeHistogramCount::scalar_udf()))
            }
            "histogram_sum" => {
                ScalarFunc::NativeHistogramUdf(Arc::new(NativeHistogramSum::scalar_udf()))
            }
            "histogram_avg" => {
                ScalarFunc::NativeHistogramUdf(Arc::new(NativeHistogramAvg::scalar_udf()))
            }
            "histogram_stddev" => {
                ScalarFunc::NativeHistogramUdf(Arc::new(NativeHistogramStddev::scalar_udf()))
            }
            "histogram_stdvar" => {
                ScalarFunc::NativeHistogramUdf(Arc::new(NativeHistogramStdvar::scalar_udf()))
            }
            "time" => {
                exprs.push(build_special_time_expr(
                    self.ctx.time_index_column.as_ref().unwrap(),
                ));
                ScalarFunc::GeneratedExpr
            }
            "minute" => {
                // date_part('minute', time_index)
                let expr = self.date_part_on_time_index("minute")?;
                exprs.push(expr);
                ScalarFunc::GeneratedExpr
            }
            "hour" => {
                // date_part('hour', time_index)
                let expr = self.date_part_on_time_index("hour")?;
                exprs.push(expr);
                ScalarFunc::GeneratedExpr
            }
            "month" => {
                // date_part('month', time_index)
                let expr = self.date_part_on_time_index("month")?;
                exprs.push(expr);
                ScalarFunc::GeneratedExpr
            }
            "year" => {
                // date_part('year', time_index)
                let expr = self.date_part_on_time_index("year")?;
                exprs.push(expr);
                ScalarFunc::GeneratedExpr
            }
            "day_of_month" => {
                // date_part('day', time_index)
                let expr = self.date_part_on_time_index("day")?;
                exprs.push(expr);
                ScalarFunc::GeneratedExpr
            }
            "day_of_week" => {
                // date_part('dow', time_index)
                let expr = self.date_part_on_time_index("dow")?;
                exprs.push(expr);
                ScalarFunc::GeneratedExpr
            }
            "day_of_year" => {
                // date_part('doy', time_index)
                let expr = self.date_part_on_time_index("doy")?;
                exprs.push(expr);
                ScalarFunc::GeneratedExpr
            }
            "days_in_month" => {
                // date_part(
                //     'days',
                //     (date_trunc('month', <TIME INDEX>::date) + interval '1 month - 1 day')
                // );
                let day_lit_expr = "day".lit();
                let month_lit_expr = "month".lit();
                let interval_1month_lit_expr =
                    DfExpr::Literal(ScalarValue::IntervalYearMonth(Some(1)), None);
                let interval_1day_lit_expr = DfExpr::Literal(
                    ScalarValue::IntervalDayTime(Some(IntervalDayTime::new(1, 0))),
                    None,
                );
                let the_1month_minus_1day_expr = DfExpr::BinaryExpr(BinaryExpr {
                    left: Box::new(interval_1month_lit_expr),
                    op: Operator::Minus,
                    right: Box::new(interval_1day_lit_expr),
                });
                let date_trunc_expr = DfExpr::ScalarFunction(ScalarFunction {
                    func: datafusion_functions::datetime::date_trunc(),
                    args: vec![month_lit_expr, self.create_time_index_column_expr()?],
                });
                let date_trunc_plus_interval_expr = DfExpr::BinaryExpr(BinaryExpr {
                    left: Box::new(date_trunc_expr),
                    op: Operator::Plus,
                    right: Box::new(the_1month_minus_1day_expr),
                });
                let date_part_expr = DfExpr::ScalarFunction(ScalarFunction {
                    func: datafusion_functions::datetime::date_part(),
                    args: vec![day_lit_expr, date_trunc_plus_interval_expr],
                });

                exprs.push(date_part_expr);
                ScalarFunc::GeneratedExpr
            }

            "label_join" => {
                self.ctx.use_tsid = false;
                let (concat_expr, dst_label) = Self::build_concat_labels_expr(
                    &mut other_input_exprs,
                    &self.ctx,
                    input_schema,
                    query_engine_state,
                )?;

                // Reserve the current field columns except the `dst_label`.
                for value in &self.ctx.field_columns {
                    if *value != dst_label {
                        let expr = DfExpr::Column(Column::from_name(value));
                        exprs.push(expr);
                    }
                }

                // Remove it from tag columns if exists to avoid duplicated column names
                self.ctx.tag_columns.retain(|tag| *tag != dst_label);
                new_tags.push(dst_label);
                // Add the new label expr to evaluate
                exprs.push(concat_expr);

                ScalarFunc::GeneratedExpr
            }
            "label_replace" => {
                self.ctx.use_tsid = false;
                if let Some((replace_expr, dst_label)) =
                    self.build_regexp_replace_label_expr(&mut other_input_exprs, input_schema)?
                {
                    // Reserve the current field columns except the `dst_label`.
                    for value in &self.ctx.field_columns {
                        if *value != dst_label {
                            let expr = DfExpr::Column(Column::from_name(value));
                            exprs.push(expr);
                        }
                    }

                    // Remove it from tag columns if exists to avoid duplicated column names
                    self.ctx.tag_columns.retain(|tag| *tag != dst_label);
                    new_tags.push(dst_label);
                    // Add the new label expr to evaluate
                    exprs.push(replace_expr);
                } else {
                    // Keep the current field columns
                    for value in &self.ctx.field_columns {
                        let expr = DfExpr::Column(Column::from_name(value));
                        exprs.push(expr);
                    }
                }

                ScalarFunc::GeneratedExpr
            }
            "sort" | "sort_desc" => {
                // Value sorting silently ignores native histogram samples.
                for value in &self.ctx.field_columns {
                    if !Self::field_column_is_native_histogram(input_schema, value) {
                        exprs.push(DfExpr::Column(Column::from_name(value)));
                    }
                }
                // Keep a nullable float field so the normal empty-value filter produces an
                // empty vector when the input contains only histograms.
                if exprs.is_empty() {
                    exprs.push(DfExpr::Literal(ScalarValue::Float64(None), None));
                }

                ScalarFunc::GeneratedExpr
            }
            "sort_by_label" | "sort_by_label_desc" | "timestamp" => {
                // These functions are not expression but a part of plan,
                // they are processed by `prom_call_expr_to_plan`.
                for value in &self.ctx.field_columns {
                    let expr = DfExpr::Column(Column::from_name(value));
                    exprs.push(expr);
                }

                ScalarFunc::GeneratedExpr
            }
            "round" if self.all_field_columns_are_native_histograms(input_schema) => {
                if other_input_exprs.is_empty() {
                    other_input_exprs.push_front(0.0f64.lit());
                }
                ScalarFunc::DataFusionUdf(native_histogram_drop_udf(func.name))
            }
            "round" => {
                if other_input_exprs.is_empty() {
                    other_input_exprs.push_front(0.0f64.lit());
                }
                ScalarFunc::DataFusionUdf(Arc::new(Round::scalar_udf()))
            }
            "rad" | "deg" | "sgn" if self.all_field_columns_are_native_histograms(input_schema) => {
                ScalarFunc::DataFusionUdf(native_histogram_drop_udf(func.name))
            }
            "rad" => ScalarFunc::DataFusionBuiltin(datafusion::functions::math::radians()),
            "deg" => ScalarFunc::DataFusionBuiltin(datafusion::functions::math::degrees()),
            "sgn" => ScalarFunc::DataFusionBuiltin(datafusion::functions::math::signum()),
            "pi" => {
                // pi functions doesn't accepts any arguments, needs special processing
                let fn_expr = DfExpr::ScalarFunction(ScalarFunction {
                    func: datafusion::functions::math::pi(),
                    args: vec![],
                });
                exprs.push(fn_expr);

                ScalarFunc::GeneratedExpr
            }
            _ => {
                if let Some(f) = query_engine_state
                    .session_state()
                    .scalar_functions()
                    .get(func.name)
                {
                    if self.all_field_columns_are_native_histograms(input_schema) {
                        ScalarFunc::DataFusionUdf(native_histogram_drop_udf(func.name))
                    } else {
                        ScalarFunc::DataFusionBuiltin(f.clone())
                    }
                } else if let Some(factory) = query_engine_state.scalar_function(func.name) {
                    if self.all_field_columns_are_native_histograms(input_schema) {
                        ScalarFunc::DataFusionUdf(native_histogram_drop_udf(func.name))
                    } else {
                        let func_state = query_engine_state.function_state();
                        let query_ctx = self.table_provider.query_ctx();

                        ScalarFunc::DataFusionUdf(Arc::new(factory.provide(FunctionContext {
                            state: func_state,
                            query_ctx: query_ctx.clone(),
                        })))
                    }
                } else if let Some(f) = datafusion_functions::math::functions()
                    .iter()
                    .find(|f| f.name() == func.name)
                {
                    if self.all_field_columns_are_native_histograms(input_schema) {
                        ScalarFunc::DataFusionUdf(native_histogram_drop_udf(func.name))
                    } else {
                        ScalarFunc::DataFusionUdf(f.clone())
                    }
                } else {
                    return UnsupportedExprSnafu {
                        name: func.name.to_string(),
                    }
                    .fail();
                }
            }
        };

        for value in &self.ctx.field_columns {
            let col_expr = DfExpr::Column(Column::from_name(value));
            let value_is_histogram = Self::field_column_is_native_histogram(input_schema, value);

            match scalar_func.clone() {
                ScalarFunc::DataFusionBuiltin(func) => {
                    if alternative_samples && value_is_histogram {
                        continue;
                    }
                    other_input_exprs.insert(field_column_pos, col_expr);
                    let fn_expr = DfExpr::ScalarFunction(ScalarFunction {
                        func,
                        args: other_input_exprs.clone().into(),
                    });
                    exprs.push(fn_expr);
                    let _ = other_input_exprs.remove(field_column_pos);
                }
                ScalarFunc::DataFusionUdf(func) => {
                    if alternative_samples && value_is_histogram {
                        continue;
                    }
                    let args = itertools::chain!(
                        other_input_exprs.iter().take(field_column_pos).cloned(),
                        std::iter::once(col_expr),
                        other_input_exprs.iter().skip(field_column_pos).cloned()
                    )
                    .collect_vec();
                    exprs.push(DfExpr::ScalarFunction(ScalarFunction { func, args }))
                }
                ScalarFunc::NativeHistogramUdf(func) => {
                    if value_is_histogram {
                        let args = itertools::chain!(
                            other_input_exprs.iter().take(field_column_pos).cloned(),
                            std::iter::once(col_expr),
                            other_input_exprs.iter().skip(field_column_pos).cloned()
                        )
                        .collect_vec();
                        exprs.push(DfExpr::ScalarFunction(ScalarFunction { func, args }));
                    } else if !alternative_samples {
                        exprs.push(
                            DfExpr::Literal(ScalarValue::Float64(None), None).alias(format!(
                                "{}_{}",
                                func.name(),
                                value
                            )),
                        );
                    }
                }
                ScalarFunc::Udf(func) => {
                    let ts_range_expr = DfExpr::Column(Column::from_name(
                        RangeManipulate::build_timestamp_range_name(
                            self.ctx.time_index_column.as_ref().unwrap(),
                        ),
                    ));
                    other_input_exprs.insert(field_column_pos, ts_range_expr);
                    other_input_exprs.insert(field_column_pos + 1, col_expr);
                    let fn_expr = DfExpr::ScalarFunction(ScalarFunction {
                        func,
                        args: other_input_exprs.clone().into(),
                    });
                    exprs.push(fn_expr);
                    let _ = other_input_exprs.remove(field_column_pos + 1);
                    let _ = other_input_exprs.remove(field_column_pos);
                }
                ScalarFunc::ExtrapolateUdf(udf, range_length) => {
                    let ts_range_expr = DfExpr::Column(Column::from_name(
                        RangeManipulate::build_timestamp_range_name(
                            self.ctx.time_index_column.as_ref().unwrap(),
                        ),
                    ));
                    other_input_exprs.insert(field_column_pos, ts_range_expr.clone());
                    other_input_exprs.insert(field_column_pos + 1, col_expr.clone());
                    other_input_exprs
                        .insert(field_column_pos + 2, self.create_time_index_column_expr()?);
                    other_input_exprs.push_back(lit(range_length));
                    let fn_expr = DfExpr::ScalarFunction(ScalarFunction {
                        func: udf,
                        args: other_input_exprs.clone().into(),
                    });
                    let fn_expr = if matches!(func.name, "rate" | "increase")
                        && !all_field_columns_are_native_histogram_ranges
                    {
                        let delta_sum = DfExpr::ScalarFunction(ScalarFunction {
                            func: Arc::new(SumOverTime::scalar_udf()),
                            args: vec![ts_range_expr, col_expr],
                        });
                        self.select_delta_range_math(
                            func.name,
                            input_schema,
                            range_length,
                            delta_sum,
                            fn_expr,
                        )?
                    } else {
                        fn_expr
                    };
                    exprs.push(fn_expr);
                    let _ = other_input_exprs.pop_back();
                    let _ = other_input_exprs.remove(field_column_pos + 2);
                    let _ = other_input_exprs.remove(field_column_pos + 1);
                    let _ = other_input_exprs.remove(field_column_pos);
                }
                ScalarFunc::GeneratedExpr => {}
            }
        }

        // Update value columns' name, and alias them to remove qualifiers
        // For label functions such as `label_join`, `label_replace`, etc.,
        // we keep the fields unchanged.
        if !matches!(func.name, "label_join" | "label_replace") {
            let mut new_field_columns = Vec::with_capacity(exprs.len());

            exprs = exprs
                .into_iter()
                .map(|expr| {
                    // `predict_linear` appends its private evaluation instant as the last argument
                    // of the UDF call; the output column is named after the call without it, so
                    // the injected expression stays out of the user-visible schema. The
                    // native-histogram drop UDF takes no such argument.
                    let display_name = if func.name == "predict_linear"
                        && !all_field_columns_are_native_histogram_ranges
                    {
                        Self::name_without_last_arg(&expr)
                    } else {
                        expr.schema_name().to_string()
                    };
                    new_field_columns.push(display_name.clone());
                    Ok(expr.alias(display_name))
                })
                .collect::<std::result::Result<Vec<_>, _>>()
                .context(DataFusionPlanningSnafu)?;

            self.ctx.field_columns = new_field_columns;
        }

        Ok((exprs, new_tags))
    }

    fn select_delta_range_math(
        &self,
        function: &str,
        input_schema: &DFSchemaRef,
        range_length: Millisecond,
        delta_sum: DfExpr,
        cumulative: DfExpr,
    ) -> Result<DfExpr> {
        let marker_is_delta = if self
            .ctx
            .tag_columns
            .iter()
            .any(|tag| tag == OTLP_AGGREGATION_TEMPORALITY_LABEL)
        {
            Self::field_column_type(input_schema, OTLP_AGGREGATION_TEMPORALITY_LABEL)
                .filter(|data_type| Self::string_value_data_type(data_type).is_some())
                .map(|_| {
                    DfExpr::Column(Column::from_name(OTLP_AGGREGATION_TEMPORALITY_LABEL))
                        .eq(lit(GREPTIME_TEMPORALITY_DELTA))
                })
        } else {
            None
        };
        let Some(marker_is_delta) = marker_is_delta else {
            return Ok(cumulative);
        };

        let delta = if function == "rate" {
            DfExpr::BinaryExpr(BinaryExpr {
                left: Box::new(delta_sum),
                op: Operator::Divide,
                right: Box::new(lit(range_length as f64 / 1000.0)),
            })
        } else {
            delta_sum
        };
        let display_name = cumulative.schema_name().to_string();
        when(marker_is_delta, delta)
            .otherwise(cumulative)
            .context(DataFusionPlanningSnafu)
            .map(|expr| expr.alias(display_name))
    }

    /// Validate label name according to Prometheus specification.
    /// Label names must match the regex: [a-zA-Z_][a-zA-Z0-9_]*
    /// Additionally, label names starting with double underscores are reserved for internal use.
    fn validate_label_name(label_name: &str) -> Result<()> {
        // Check if label name starts with double underscores (reserved)
        if label_name.starts_with("__") {
            return InvalidDestinationLabelNameSnafu { label_name }.fail();
        }
        // Check if label name matches the required pattern
        if !LABEL_NAME_REGEX.is_match(label_name) {
            return InvalidDestinationLabelNameSnafu { label_name }.fail();
        }

        Ok(())
    }

    /// Build expr for `label_replace` function
    fn build_regexp_replace_label_expr(
        &self,
        other_input_exprs: &mut VecDeque<DfExpr>,
        input_schema: &DFSchemaRef,
    ) -> Result<Option<(DfExpr, String)>> {
        // label_replace(vector, dst_label, replacement, src_label, regex)
        let dst_label = match other_input_exprs.pop_front() {
            Some(DfExpr::Literal(ScalarValue::Utf8(Some(d)), _)) => d,
            other => UnexpectedPlanExprSnafu {
                desc: format!("expected dst_label string literal, but found {:?}", other),
            }
            .fail()?,
        };

        // Validate the destination label name
        Self::validate_label_name(&dst_label)?;
        let replacement = match other_input_exprs.pop_front() {
            Some(DfExpr::Literal(ScalarValue::Utf8(Some(r)), _)) => r,
            other => UnexpectedPlanExprSnafu {
                desc: format!("expected replacement string literal, but found {:?}", other),
            }
            .fail()?,
        };
        let src_label = match other_input_exprs.pop_front() {
            Some(DfExpr::Literal(ScalarValue::Utf8(Some(s)), None)) => s,
            other => UnexpectedPlanExprSnafu {
                desc: format!("expected src_label string literal, but found {:?}", other),
            }
            .fail()?,
        };

        let regex = match other_input_exprs.pop_front() {
            Some(DfExpr::Literal(ScalarValue::Utf8(Some(r)), None)) => r,
            other => UnexpectedPlanExprSnafu {
                desc: format!("expected regex string literal, but found {:?}", other),
            }
            .fail()?,
        };

        // Like Prometheus, match the whole source value. A series whose source value matches gets
        // `dst_label` set to the expanded replacement; any other series is left unchanged.
        // doc: https://prometheus.io/docs/prometheus/latest/querying/functions/#label_replace
        let anchored = format!("^(?s:{regex})$");
        let compiled = regex::Regex::new(&anchored)
            .map_err(|_| InvalidRegularExpressionSnafu { regex }.build())?;
        let dst_exists = self.ctx.tag_columns.contains(&dst_label);

        // A missing source label reads as the empty string for every series, so the result is
        // the same for all of them and is decided here.
        if !self.ctx.tag_columns.contains(&src_label) {
            let Some(captures) = compiled.captures("") else {
                return Ok(None);
            };
            let mut value = String::new();
            captures.expand(&replacement, &mut value);
            if value.is_empty() {
                // Setting a label to the empty string removes it, which is a no-op for a new
                // label.
                if !dst_exists {
                    return Ok(None);
                }
                return Ok(Some((
                    lit(ScalarValue::Utf8(None)).alias(&dst_label),
                    dst_label,
                )));
            }
            return Ok(Some((lit(value).alias(&dst_label), dst_label)));
        }

        let src = Self::label_value_expr(&src_label, input_schema)?;
        let matched = DfExpr::ScalarFunction(ScalarFunction {
            func: datafusion_functions::regex::regexp_like(),
            args: vec![src.clone(), lit(anchored.clone())],
        });
        let replaced = Self::empty_label_to_null(DfExpr::ScalarFunction(ScalarFunction {
            func: datafusion_functions::regex::regexp_replace(),
            args: vec![src, lit(anchored), lit(replacement)],
        }));
        let unchanged = if dst_exists {
            DfExpr::Column(Column::from_name(&dst_label))
                .cast_to(&ArrowDataType::Utf8, input_schema)
                .context(DataFusionPlanningSnafu)?
        } else {
            lit(ScalarValue::Utf8(None))
        };
        let replace_expr = when(matched, replaced)
            .otherwise(unchanged)
            .context(DataFusionPlanningSnafu)?;

        Ok(Some((replace_expr.alias(&dst_label), dst_label)))
    }

    /// Build expr for `label_join` function
    fn build_concat_labels_expr(
        other_input_exprs: &mut VecDeque<DfExpr>,
        ctx: &PromPlannerContext,
        input_schema: &DFSchemaRef,
        query_engine_state: &QueryEngineState,
    ) -> Result<(DfExpr, String)> {
        // label_join(vector, dst_label, separator, src_label_1, src_label_2, ...)

        let dst_label = match other_input_exprs.pop_front() {
            Some(DfExpr::Literal(ScalarValue::Utf8(Some(d)), _)) => d,
            other => UnexpectedPlanExprSnafu {
                desc: format!("expected dst_label string literal, but found {:?}", other),
            }
            .fail()?,
        };
        let separator = match other_input_exprs.pop_front() {
            Some(DfExpr::Literal(ScalarValue::Utf8(Some(d)), _)) => d,
            other => UnexpectedPlanExprSnafu {
                desc: format!("expected separator string literal, but found {:?}", other),
            }
            .fail()?,
        };

        // Create a set of available columns (tag columns + field columns + time index column)
        let available_columns: HashSet<&str> = ctx
            .tag_columns
            .iter()
            .chain(ctx.field_columns.iter())
            .chain(ctx.time_index_column.as_ref())
            .map(|s| s.as_str())
            .collect();

        let src_labels = other_input_exprs
            .iter()
            .map(|expr| {
                // `concat_ws` skips NULL arguments together with their separator, while an
                // absent label joins as the empty string.
                match expr {
                    DfExpr::Literal(ScalarValue::Utf8(Some(label)), None) => {
                        if label.is_empty() {
                            FunctionInvalidArgumentSnafu {
                                fn_name: "label_join",
                            }
                            .fail()
                        } else if available_columns.contains(label.as_str()) {
                            Self::label_value_expr(label, input_schema)
                        } else {
                            Ok(lit(""))
                        }
                    }
                    other => UnexpectedPlanExprSnafu {
                        desc: format!(
                            "expected source label string literal, but found {:?}",
                            other
                        ),
                    }
                    .fail(),
                }
            })
            .collect::<Result<Vec<_>>>()?;
        // Joining no labels yields the empty string, i.e. removes `dst_label`.
        if src_labels.is_empty() {
            return Ok((lit(ScalarValue::Utf8(None)).alias(&dst_label), dst_label));
        }

        let session_state = query_engine_state.session_state();
        let func = session_state
            .scalar_functions()
            .get("concat_ws")
            .context(UnsupportedExprSnafu { name: "concat_ws" })?;

        // concat_ws(separator, src_label_1, src_label_2, ...) as dst_label
        let mut args = Vec::with_capacity(1 + src_labels.len());
        args.push(DfExpr::Literal(ScalarValue::Utf8(Some(separator)), None));
        args.extend(src_labels);

        Ok((
            Self::empty_label_to_null(DfExpr::ScalarFunction(ScalarFunction {
                func: func.clone(),
                args,
            }))
            .alias(&dst_label),
            dst_label,
        ))
    }

    /// The value of `label` as a string, where NULL (the series has no such label) reads as the
    /// empty string, as in PromQL.
    fn label_value_expr(label: &str, input_schema: &DFSchemaRef) -> Result<DfExpr> {
        let value = DfExpr::Column(Column::from_name(label))
            .cast_to(&ArrowDataType::Utf8, input_schema)
            .context(DataFusionPlanningSnafu)?;
        Ok(DfExpr::ScalarFunction(ScalarFunction {
            func: coalesce(),
            args: vec![value, lit("")],
        }))
    }

    /// An empty label value means the label is absent in PromQL. Label functions represent it
    /// as NULL, like a series that never had the label, so that both compare equal when label
    /// sets are matched and neither is reported as a label.
    fn empty_label_to_null(value: DfExpr) -> DfExpr {
        DfExpr::ScalarFunction(ScalarFunction {
            func: datafusion_functions::core::nullif(),
            args: vec![value, lit("")],
        })
    }

    fn create_time_index_column_expr(&self) -> Result<DfExpr> {
        Ok(DfExpr::Column(Column::from_name(
            self.ctx
                .time_index_column
                .clone()
                .with_context(|| TimeIndexNotFoundSnafu { table: "unknown" })?,
        )))
    }

    /// Builds the evaluation instant the window of the last planned range selector is folded for,
    /// as a `Timestamp(Millisecond)` expression.
    ///
    /// The timestamp payload of a folded window is shifted onto the evaluation timeline by the
    /// offset the window is folded with (`fold_offset`), while the time index column of a folded
    /// row keeps the evaluation timestamp of its step. Adding the offset back yields the
    /// evaluation instant on the payload timeline, which is where the regression of
    /// `predict_linear` is centered: neither a plain window (which may end before the step, and
    /// is additionally shifted by `offset` on the payload timeline) nor an `@`-anchored one
    /// (whose end is the anchor, while the payload is shifted by `at_offset`) ends at the step it
    /// is evaluated at.
    ///
    /// The sum is computed on the millisecond representation and cast back, so that the result
    /// keeps the `Timestamp(Millisecond)` type the range functions declare for it.
    fn create_range_eval_ts_expr(
        &self,
        fold_offset: Millisecond,
        input_schema: &DFSchemaRef,
    ) -> Result<DfExpr> {
        let eval_ts = self
            .create_time_index_column_expr()?
            .cast_to(
                &ArrowDataType::Timestamp(ArrowTimeUnit::Millisecond, None),
                input_schema,
            )
            .context(DataFusionPlanningSnafu)?
            .cast_to(&ArrowDataType::Int64, input_schema)
            .context(DataFusionPlanningSnafu)?;
        DfExpr::BinaryExpr(BinaryExpr {
            left: Box::new(eval_ts),
            op: Operator::Plus,
            right: Box::new(lit(fold_offset)),
        })
        .cast_to(
            &ArrowDataType::Timestamp(ArrowTimeUnit::Millisecond, None),
            input_schema,
        )
        .context(DataFusionPlanningSnafu)
    }

    /// The name of `expr` without its last argument.
    ///
    /// A `predict_linear` call appends the evaluation instant of its window as a private last
    /// argument ([`Self::create_range_eval_ts_expr`]). Naming the output column after the call
    /// the user wrote, without that argument, keeps the injected expression out of the
    /// user-visible schema; the expression itself keeps every argument it needs.
    fn name_without_last_arg(expr: &DfExpr) -> String {
        if let DfExpr::ScalarFunction(ScalarFunction { func, args }) = expr
            && let Some((_, visible_args)) = args.split_last()
        {
            let visible = ScalarFunction {
                func: func.clone(),
                args: visible_args.to_vec(),
            };
            return DfExpr::ScalarFunction(visible).schema_name().to_string();
        }

        expr.schema_name().to_string()
    }

    fn create_tag_column_exprs(&self) -> Result<Vec<DfExpr>> {
        let mut result = Vec::with_capacity(self.ctx.tag_columns.len());
        for tag in &self.ctx.tag_columns {
            let expr = DfExpr::Column(Column::from_name(tag));
            result.push(expr);
        }
        Ok(result)
    }

    fn create_field_column_exprs(&self) -> Result<Vec<DfExpr>> {
        let mut result = Vec::with_capacity(self.ctx.field_columns.len());
        for field in &self.ctx.field_columns {
            let expr = DfExpr::Column(Column::from_name(field));
            result.push(expr);
        }
        Ok(result)
    }

    fn create_tag_and_time_index_column_sort_exprs(&self) -> Result<Vec<SortExpr>> {
        let mut result = self
            .ctx
            .tag_columns
            .iter()
            .map(|col| DfExpr::Column(Column::from_name(col)).sort(true, true))
            .collect::<Vec<_>>();
        result.push(self.create_time_index_column_expr()?.sort(true, true));
        Ok(result)
    }

    fn create_field_columns_sort_exprs(&self, asc: bool) -> Vec<SortExpr> {
        self.ctx
            .field_columns
            .iter()
            .map(|col| DfExpr::Column(Column::from_name(col)).sort(asc, true))
            .collect::<Vec<_>>()
    }

    fn create_sort_exprs_by_tags(
        func: &str,
        tags: Vec<DfExpr>,
        asc: bool,
    ) -> Result<Vec<SortExpr>> {
        ensure!(
            !tags.is_empty(),
            FunctionInvalidArgumentSnafu { fn_name: func }
        );

        tags.iter()
            .map(|col| match col {
                DfExpr::Literal(ScalarValue::Utf8(Some(label)), _) => {
                    Ok(DfExpr::Column(Column::from_name(label)).sort(asc, false))
                }
                other => UnexpectedPlanExprSnafu {
                    desc: format!("expected label string literal, but found {:?}", other),
                }
                .fail(),
            })
            .collect::<Result<Vec<_>>>()
    }

    fn create_empty_values_filter_expr(&self, preserve_any_value: bool) -> Result<DfExpr> {
        let mut exprs = Vec::with_capacity(self.ctx.field_columns.len());
        for value in &self.ctx.field_columns {
            let expr = DfExpr::Column(Column::from_name(value)).is_not_null();
            exprs.push(expr);
        }

        // This error context should be computed lazily: the planner may set `ctx.table_name` to
        // `None` for derived expressions (e.g. after projecting the LHS of a vector-vector
        // comparison filter). Eagerly calling `table_ref()?` here can turn a valid plan into
        // a `TableNameNotFound` error even when predicate construction succeeds.
        let predicate = if preserve_any_value {
            disjunction(exprs)
        } else {
            conjunction(exprs)
        };
        predicate.with_context(|| ValueNotFoundSnafu {
            table: self
                .table_ref()
                .map(|t| t.to_quoted_string())
                .unwrap_or_else(|_| "unknown".to_string()),
        })
    }

    /// Creates a set of DataFusion `DfExpr::AggregateFunction` expressions for each value column using the specified aggregate function.
    ///
    /// # Side Effects
    ///
    /// This method modifies the value columns in the context by replacing them with the new columns
    /// created by the aggregate function application.
    ///
    /// # Returns
    ///
    /// Returns a tuple of `(aggregate_expressions, previous_field_expressions)` where:
    /// - `aggregate_expressions`: Expressions that apply the aggregate function to the original fields
    /// - `previous_field_expressions`: Field expressions naming the pre-aggregation values. This is
    ///   non-empty only when the operation is `count_values`, which groups by the sample value and
    ///   projects it as the generated label, so these expressions are passed through the same
    ///   formatting as that label (`prom_float_to_string`).
    ///
    fn create_aggregate_exprs(
        &mut self,
        op: TokenType,
        param: &Option<Box<PromExpr>>,
        input_plan: &LogicalPlan,
    ) -> Result<(Vec<DfExpr>, Vec<DfExpr>)> {
        let mixed_sample_columns =
            Self::alternative_sample_columns(input_plan.schema(), &self.ctx.field_columns)
                .map(|(float, histogram)| (float.to_string(), histogram.to_string()));
        let is_group_agg = op.id() == token::T_GROUP;
        if is_group_agg && mixed_sample_columns.is_none() {
            ensure!(
                self.ctx.field_columns.len() == 1,
                MultiFieldsNotSupportedSnafu {
                    operator: "group()"
                }
            );
        }

        if let Some((float, histogram)) = mixed_sample_columns {
            return self.create_mixed_aggregate_exprs(op, param, &float, &histogram);
        }

        if self.all_field_columns_are_native_histograms(input_plan.schema()) {
            return self.create_native_histogram_aggregate_exprs(op, input_plan);
        }

        // perform aggregate operation to each value column
        let exprs = self
            .ctx
            .field_columns
            .iter()
            .map(|col| {
                Self::create_numeric_aggregate_expr(
                    op,
                    param,
                    DfExpr::Column(Column::from_name(col)),
                )
            })
            .collect::<Result<Vec<_>>>()?;

        // if the aggregator is `count_values`, it must be grouped by current fields.
        //
        // The grouping key is the *formatted* sample value, i.e. the same expression that
        // produces the generated label below: PromQL groups by the value, and the label is
        // that value in Prometheus' textual form (`strconv.FormatFloat(value, 'f', -1, 64)`),
        // so grouping by the raw value would split samples that render to one label into
        // several groups, each emitting the same label set for one timestamp.
        let prev_field_exprs = if op.id() == token::T_COUNT_VALUES {
            ensure!(
                self.ctx.field_columns.len() == 1,
                UnsupportedExprSnafu {
                    name: "count_values on multi-value input"
                }
            );

            self.ctx
                .field_columns
                .iter()
                .map(|col| {
                    let value = DfExpr::Column(Column::from_name(col));
                    // Normalize non `Float64` inputs the same way the label projection does,
                    // so both sides agree on the formatted value: `prom_float_to_string`
                    // formats exactly like Prometheus, while arrow's `Float64 -> Utf8` cast
                    // would render `1.0`.
                    let value = if Self::field_column_type(input_plan.schema(), col)
                        == Some(&ArrowDataType::Float64)
                    {
                        value
                    } else {
                        DfExpr::Cast(Cast::new(Box::new(value), ArrowDataType::Float64))
                    };
                    DfExpr::ScalarFunction(ScalarFunction {
                        func: Arc::new(PromqlFloatToString::scalar_udf()),
                        args: vec![value],
                    })
                })
                .collect()
        } else {
            vec![]
        };

        // update value column name according to the aggregators,
        let mut new_field_columns = Vec::with_capacity(self.ctx.field_columns.len());

        let normalized_exprs =
            normalize_cols(exprs.iter().cloned(), input_plan).context(DataFusionPlanningSnafu)?;
        for expr in normalized_exprs {
            new_field_columns.push(expr.schema_name().to_string());
        }
        self.ctx.field_columns = new_field_columns;

        Ok((exprs, prev_field_exprs))
    }

    fn create_numeric_aggregate_expr(
        op: TokenType,
        param: &Option<Box<PromExpr>>,
        input: DfExpr,
    ) -> Result<DfExpr> {
        let expr = match op.id() {
            token::T_SUM => sum_udaf().call(vec![input]),
            token::T_QUANTILE => {
                let q = Self::get_param_as_literal_expr(
                    param.as_deref(),
                    Some(op),
                    Some(ArrowDataType::Float64),
                )?;
                quantile_udaf().call(vec![q, input])
            }
            token::T_AVG => avg_udaf().call(vec![input]),
            token::T_COUNT_VALUES | token::T_COUNT => count_udaf().call(vec![input]),
            token::T_MIN => min_udaf().call(vec![input]),
            token::T_MAX => max_udaf().call(vec![input]),
            // PromQL's `group()` aggregator produces 1 for each group.
            // Use `max(1.0)` (per-group) to match semantics and output type (Float64).
            token::T_GROUP => max_udaf().call(vec![lit(1_f64)]),
            token::T_STDDEV => stddev_pop_udaf().call(vec![input]),
            token::T_STDVAR => var_pop_udaf().call(vec![input]),
            token::T_TOPK | token::T_BOTTOMK => {
                return UnsupportedExprSnafu {
                    name: format!("{op:?}"),
                }
                .fail();
            }
            _ => return UnexpectedTokenSnafu { token: op }.fail(),
        };
        Ok(expr)
    }

    fn create_mixed_aggregate_exprs(
        &mut self,
        op: TokenType,
        param: &Option<Box<PromExpr>>,
        float_column: &str,
        histogram_column: &str,
    ) -> Result<(Vec<DfExpr>, Vec<DfExpr>)> {
        let float_input = DfExpr::Column(Column::from_name(float_column));
        let histogram_input = DfExpr::Column(Column::from_name(histogram_column));
        let float_count = count_udaf().call(vec![float_input.clone()]);
        let histogram_count = count_udaf().call(vec![histogram_input.clone()]);
        let mixed_sample_value = || {
            DfExpr::ScalarFunction(ScalarFunction {
                func: coalesce(),
                args: vec![
                    DfExpr::ScalarFunction(ScalarFunction {
                        func: Arc::new(PromqlFloatToString::scalar_udf()),
                        args: vec![float_input.clone()],
                    }),
                    DfExpr::ScalarFunction(ScalarFunction {
                        func: Arc::new(NativeHistogramToString::scalar_udf()),
                        args: vec![histogram_input.clone()],
                    }),
                ],
            })
        };

        let (exprs, prev_field_exprs, field_columns) = match op.id() {
            token::T_SUM | token::T_AVG => (
                vec![
                    Self::create_numeric_aggregate_expr(op, param, float_input)?
                        .alias(float_column),
                    self.create_native_histogram_aggregate_expr(op, histogram_column)?,
                    float_count.alias(Self::mixed_sample_count_name(float_column)),
                    histogram_count.alias(Self::mixed_sample_count_name(histogram_column)),
                ],
                vec![],
                vec![float_column.to_string(), histogram_column.to_string()],
            ),
            token::T_COUNT => {
                let present = when(
                    float_input
                        .clone()
                        .is_not_null()
                        .or(histogram_input.clone().is_not_null()),
                    lit(1_i64),
                )
                .otherwise(lit(ScalarValue::Int64(None)))
                .context(DataFusionPlanningSnafu)?;
                (
                    vec![count_udaf().call(vec![present]).alias(float_column)],
                    vec![],
                    vec![float_column.to_string()],
                )
            }
            token::T_GROUP => (
                vec![max_udaf().call(vec![lit(1_f64)]).alias(float_column)],
                vec![],
                vec![float_column.to_string()],
            ),
            token::T_COUNT_VALUES => {
                let value = mixed_sample_value();
                (
                    vec![count_udaf().call(vec![value.clone()]).alias(float_column)],
                    vec![value],
                    vec![float_column.to_string()],
                )
            }
            token::T_MIN | token::T_MAX | token::T_STDDEV | token::T_STDVAR | token::T_QUANTILE => {
                (
                    vec![
                        Self::create_numeric_aggregate_expr(op, param, float_input)?
                            .alias(float_column),
                        histogram_count.alias(Self::mixed_sample_count_name(histogram_column)),
                    ],
                    vec![],
                    vec![float_column.to_string()],
                )
            }
            token::T_TOPK | token::T_BOTTOMK => {
                return UnsupportedExprSnafu {
                    name: format!("{op:?}"),
                }
                .fail();
            }
            _ => return UnexpectedTokenSnafu { token: op }.fail(),
        };

        self.ctx.field_columns = field_columns;
        Ok((exprs, prev_field_exprs))
    }

    fn mixed_sample_count_column(column: &str) -> DfExpr {
        DfExpr::Column(Column::from_name(Self::mixed_sample_count_name(column)))
    }

    fn mixed_sample_count_name(column: &str) -> String {
        format!("__promql_sample_count({column})")
    }

    fn mixed_aggregate_filter_expr(
        &self,
        op: TokenType,
        float_column: &str,
        histogram_column: &str,
    ) -> Result<DfExpr> {
        let float_count = Self::mixed_sample_count_column(float_column);
        let histogram_count = Self::mixed_sample_count_column(histogram_column);
        let mixed = float_count
            .clone()
            .gt(lit(0_i64))
            .and(histogram_count.clone().gt(lit(0_i64)));
        let drop_mixed = DfExpr::ScalarFunction(ScalarFunction {
            func: Arc::new(NativeHistogramDrop::warning_bool_false_udf(
                format!(
                    "{op}: dropped aggregation result containing both float and native histogram samples"
                ),
                self.promql_annotations.clone(),
            )),
            args: vec![float_count, histogram_count],
        });

        when(mixed, drop_mixed)
            .otherwise(lit(true))
            .context(DataFusionPlanningSnafu)
    }

    fn mixed_ignored_histogram_filter_expr(
        &self,
        op: TokenType,
        histogram_column: &str,
    ) -> Result<DfExpr> {
        let histogram_count = Self::mixed_sample_count_column(histogram_column);
        let has_histograms = histogram_count.clone().gt(lit(0_i64));
        let record_info = DfExpr::ScalarFunction(ScalarFunction {
            func: Arc::new(NativeHistogramDrop::bool_true_udf(
                format!(
                    "{op}: dropped native histogram samples because this aggregation is not supported for native histograms"
                ),
                self.promql_annotations.clone(),
            )),
            args: vec![histogram_count],
        });

        when(has_histograms, record_info)
            .otherwise(lit(true))
            .context(DataFusionPlanningSnafu)
    }

    fn create_native_histogram_aggregate_expr(
        &self,
        op: TokenType,
        column: &str,
    ) -> Result<DfExpr> {
        let input = DfExpr::Column(Column::from_name(column));
        let expr = match op.id() {
            token::T_SUM => Arc::new(NativeHistogramAggSum::aggregate_udf_with_collector(
                self.promql_annotations.clone(),
            ))
            .call(vec![input])
            .alias(column),
            token::T_AVG => Arc::new(NativeHistogramAggAvg::aggregate_udf_with_collector(
                self.promql_annotations.clone(),
            ))
            .call(vec![input])
            .alias(column),
            token::T_COUNT_VALUES | token::T_COUNT => {
                count_udaf().call(vec![input]).alias(column)
            }
            token::T_GROUP => max_udaf().call(vec![lit(1_f64)]).alias(column),
            token::T_MIN
            | token::T_MAX
            | token::T_STDDEV
            | token::T_STDVAR
            | token::T_QUANTILE
            | token::T_TOPK
            | token::T_BOTTOMK => sum_udaf()
                .call(vec![DfExpr::ScalarFunction(ScalarFunction {
                    func: Arc::new(NativeHistogramDrop::float_null_udf(
                        format!(
                            "{op}: dropped native histogram samples because this aggregation is not supported for native histograms"
                        ),
                        self.promql_annotations.clone(),
                    )),
                    args: vec![input],
                })])
                .alias(column),
            _ => return UnexpectedTokenSnafu { token: op }.fail(),
        };
        Ok(expr)
    }

    fn create_native_histogram_aggregate_exprs(
        &mut self,
        op: TokenType,
        input_plan: &LogicalPlan,
    ) -> Result<(Vec<DfExpr>, Vec<DfExpr>)> {
        let prev_field_exprs = if op.id() == token::T_COUNT_VALUES {
            ensure!(
                self.ctx.field_columns.len() == 1,
                UnsupportedExprSnafu {
                    name: "count_values on multi-value input"
                }
            );
            self.ctx
                .field_columns
                .iter()
                .map(|col| {
                    DfExpr::ScalarFunction(ScalarFunction {
                        func: Arc::new(NativeHistogramToString::scalar_udf()),
                        args: vec![DfExpr::Column(Column::from_name(col))],
                    })
                })
                .collect::<Vec<_>>()
        } else {
            vec![]
        };

        let exprs = self
            .ctx
            .field_columns
            .iter()
            .map(|col| self.create_native_histogram_aggregate_expr(op, col))
            .collect::<Result<Vec<_>>>()?;

        let normalized_exprs =
            normalize_cols(exprs.iter().cloned(), input_plan).context(DataFusionPlanningSnafu)?;
        self.ctx.field_columns = normalized_exprs
            .into_iter()
            .map(|expr| expr.schema_name().to_string())
            .collect();

        Ok((exprs, prev_field_exprs))
    }

    fn get_param_value_as_str(op: TokenType, param: &Option<Box<PromExpr>>) -> Result<&str> {
        let param = param
            .as_deref()
            .with_context(|| FunctionInvalidArgumentSnafu {
                fn_name: op.to_string(),
            })?;
        let PromExpr::StringLiteral(StringLiteral { val }) = param else {
            return FunctionInvalidArgumentSnafu {
                fn_name: op.to_string(),
            }
            .fail();
        };

        Ok(val)
    }

    /// Returns the given parameter as a scalar expression that is known while
    /// planning.
    ///
    /// A runtime `scalar(...)` parameter is rejected: use
    /// [`Self::get_param_as_scalar_expr`] and
    /// [`Self::build_scalar_params`] for the functions that accept one.
    fn get_param_as_literal_expr(
        param: Option<&PromExpr>,
        op: Option<TokenType>,
        expected_type: Option<ArrowDataType>,
    ) -> Result<DfExpr> {
        match Self::get_param_as_scalar_expr(param, op, expected_type)? {
            ScalarParam::Static(expr) => Ok(expr),
            ScalarParam::Dynamic(_) => FunctionInvalidArgumentSnafu {
                fn_name: format!(
                    "{}: {DYNAMIC_SCALAR_UNSUPPORTED_FOR_FUNCTION}",
                    Self::function_argument_name(op)
                ),
            }
            .fail(),
        }
    }

    /// Returns the given parameter as a scalar expression, which is either a
    /// compile-time literal or a runtime `scalar(...)` expression.
    fn get_param_as_scalar_expr(
        param: Option<&PromExpr>,
        op: Option<TokenType>,
        expected_type: Option<ArrowDataType>,
    ) -> Result<ScalarParam> {
        let prom_param = param.with_context(|| FunctionInvalidArgumentSnafu {
            fn_name: Self::function_argument_name(op),
        })?;

        let Some(expr) = Self::try_build_literal_expr(prom_param) else {
            ensure!(
                Self::is_scalar_call(prom_param),
                FunctionInvalidArgumentSnafu {
                    fn_name: Self::function_argument_name(op),
                }
            );

            return Ok(ScalarParam::Dynamic(prom_param.clone()));
        };

        // check if the type is expected
        if let Some(expected_type) = expected_type {
            // literal should not have reference to column
            let expr_type = expr
                .get_type(&DFSchema::empty())
                .context(DataFusionPlanningSnafu)?;
            if expected_type != expr_type {
                return FunctionInvalidArgumentSnafu {
                    fn_name: format!("expected {expected_type:?}, but found {expr_type:?}"),
                }
                .fail();
            }
        }

        Ok(ScalarParam::Static(expr))
    }

    /// The name of a function argument used in errors raised while reading the
    /// argument.
    fn function_argument_name(op: Option<TokenType>) -> String {
        op.map(|op| op.to_string())
            .unwrap_or_else(|| "unknown".to_string())
    }

    /// Create [DfExpr::WindowFunction] expr for each value column with given window function.
    ///
    fn create_window_exprs(
        &mut self,
        op: TokenType,
        group_exprs: Vec<DfExpr>,
        input_plan: &LogicalPlan,
    ) -> Result<Vec<DfExpr>> {
        ensure!(
            self.ctx.field_columns.len() == 1,
            UnsupportedExprSnafu {
                name: "topk or bottomk on multi-value input"
            }
        );

        assert!(matches!(op.id(), token::T_TOPK | token::T_BOTTOMK));

        let asc = matches!(op.id(), token::T_BOTTOMK);

        let tag_sort_exprs = self
            .create_tag_column_exprs()?
            .into_iter()
            .map(|expr| expr.sort(asc, true));

        // perform window operation to each value column
        let exprs: Vec<DfExpr> = self
            .ctx
            .field_columns
            .iter()
            .map(|col| {
                let mut sort_exprs = Vec::with_capacity(self.ctx.tag_columns.len() + 1);
                // Order by value in the specific order
                sort_exprs.push(DfExpr::Column(Column::from_name(col)).sort(asc, true));
                // Then tags if the values are equal,
                // Try to ensure the relative stability of the output results.
                sort_exprs.extend(tag_sort_exprs.clone());

                DfExpr::WindowFunction(Box::new(WindowFunction {
                    fun: WindowFunctionDefinition::WindowUDF(Arc::new(RowNumber::new().into())),
                    params: WindowFunctionParams {
                        args: vec![],
                        partition_by: group_exprs.clone(),
                        order_by: sort_exprs,
                        window_frame: WindowFrame::new(Some(true)),
                        null_treatment: None,
                        distinct: false,
                        filter: None,
                    },
                }))
            })
            .collect();

        let normalized_exprs =
            normalize_cols(exprs.iter().cloned(), input_plan).context(DataFusionPlanningSnafu)?;
        Ok(normalized_exprs)
    }

    /// Try to build a DataFusion Literal Expression from PromQL Expr, return
    /// `None` if the input is not a literal expression.
    fn try_build_literal_expr(expr: &PromExpr) -> Option<DfExpr> {
        match expr {
            PromExpr::NumberLiteral(NumberLiteral { val }) => Some(val.lit()),
            PromExpr::StringLiteral(StringLiteral { val }) => Some(val.lit()),
            PromExpr::VectorSelector(_)
            | PromExpr::MatrixSelector(_)
            | PromExpr::Extension(_)
            | PromExpr::Aggregate(_)
            | PromExpr::Subquery(_) => None,
            PromExpr::Call(Call { func, .. }) => {
                if func.name == SPECIAL_TIME_FUNCTION {
                    // For time() function, don't treat it as a literal
                    // Let it be handled as a regular function call
                    None
                } else {
                    None
                }
            }
            PromExpr::Paren(ParenExpr { expr }) => Self::try_build_literal_expr(expr),
            PromExpr::Unary(UnaryExpr { expr, .. }) => Some(DfExpr::Negative(Box::new(
                Self::try_build_literal_expr(expr)?,
            ))),
            PromExpr::Binary(PromBinaryExpr {
                lhs,
                rhs,
                op,
                modifier,
            }) => {
                let lhs = Self::try_build_literal_expr(lhs)?;
                let rhs = Self::try_build_literal_expr(rhs)?;
                let is_comparison_op = Self::is_token_a_comparison_op(*op);
                let expr_builder = Self::prom_token_to_binary_expr_builder(*op).ok()?;
                let expr = expr_builder(lhs, rhs).ok()?;

                let should_return_bool = if let Some(m) = modifier {
                    m.return_bool
                } else {
                    false
                };
                if is_comparison_op && should_return_bool {
                    Some(DfExpr::Cast(Cast::new(
                        Box::new(expr),
                        ArrowDataType::Float64,
                    )))
                } else {
                    Some(expr)
                }
            }
        }
    }

    fn try_build_special_time_expr_with_context(&self, expr: &PromExpr) -> Option<DfExpr> {
        match expr {
            PromExpr::Call(Call { func, .. }) => {
                if func.name == SPECIAL_TIME_FUNCTION
                    && let Some(time_index_col) = self.ctx.time_index_column.as_ref()
                {
                    Some(build_special_time_expr(time_index_col))
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    fn native_histogram_binary_expr(
        token: TokenType,
        lhs: DfExpr,
        lhs_is_histogram: bool,
        rhs: DfExpr,
        rhs_is_histogram: bool,
        filter_context: bool,
        promql_annotations: Option<PromqlAnnotationCollector>,
    ) -> Result<Option<DfExpr>> {
        if !lhs_is_histogram && !rhs_is_histogram {
            return Ok(None);
        }

        let scalar_fn = |func: ScalarUdfDef, args| {
            DfExpr::ScalarFunction(ScalarFunction {
                func: Arc::new(func),
                args,
            })
        };
        let invalid_expr = || {
            let message = format!(
                "{}: dropped native histogram samples because this binary operation is not supported for native histograms",
                token
            );
            let func = if filter_context {
                NativeHistogramDrop::bool_false_udf(message, promql_annotations.clone())
            } else {
                NativeHistogramDrop::float_null_udf(message, promql_annotations.clone())
            };
            let args = vec![lhs.clone(), rhs.clone()];
            scalar_fn(func, args)
        };

        let expr = match (token.id(), lhs_is_histogram, rhs_is_histogram) {
            (token::T_ADD, true, true) => scalar_fn(
                NativeHistogramAdd::scalar_udf_with_collector(promql_annotations.clone()),
                vec![lhs, rhs],
            ),
            (token::T_SUB, true, true) => scalar_fn(
                NativeHistogramSub::scalar_udf_with_collector(promql_annotations.clone()),
                vec![lhs, rhs],
            ),
            (token::T_MUL, true, false) => {
                scalar_fn(NativeHistogramMulScalar::scalar_udf(), vec![lhs, rhs])
            }
            (token::T_MUL, false, true) => {
                scalar_fn(NativeHistogramScalarMul::scalar_udf(), vec![lhs, rhs])
            }
            (token::T_DIV, true, false) => {
                scalar_fn(NativeHistogramDivScalar::scalar_udf(), vec![lhs, rhs])
            }
            (token::T_EQLC, true, true) => {
                scalar_fn(NativeHistogramEq::scalar_udf(), vec![lhs, rhs])
            }
            (token::T_NEQ, true, true) => {
                scalar_fn(NativeHistogramNotEq::scalar_udf(), vec![lhs, rhs])
            }
            _ => invalid_expr(),
        };

        Ok(Some(expr))
    }

    /// Return a lambda to build binary expression from token.
    /// Because some binary operator are function in DataFusion like `atan2` or `^`.
    #[allow(clippy::type_complexity)]
    fn prom_token_to_binary_expr_builder(
        token: TokenType,
    ) -> Result<Box<dyn Fn(DfExpr, DfExpr) -> Result<DfExpr>>> {
        let cast_float = |expr| {
            if matches!(
                &expr,
                DfExpr::Cast(Cast { field, .. }) if field.data_type() == &ArrowDataType::Float64
            ) || matches!(&expr, DfExpr::Literal(ScalarValue::Float64(_), _))
            {
                expr
            } else {
                DfExpr::Cast(Cast::new(Box::new(expr), ArrowDataType::Float64))
            }
        };
        match token.id() {
            token::T_ADD => Ok(Box::new(move |lhs, rhs| {
                Ok(cast_float(lhs) + cast_float(rhs))
            })),
            token::T_SUB => Ok(Box::new(move |lhs, rhs| {
                Ok(cast_float(lhs) - cast_float(rhs))
            })),
            token::T_MUL => Ok(Box::new(move |lhs, rhs| {
                Ok(cast_float(lhs) * cast_float(rhs))
            })),
            token::T_DIV => Ok(Box::new(move |lhs, rhs| {
                Ok(cast_float(lhs) / cast_float(rhs))
            })),
            token::T_MOD => Ok(Box::new(move |lhs: DfExpr, rhs| {
                Ok(cast_float(lhs) % cast_float(rhs))
            })),
            token::T_EQLC => Ok(Box::new(|lhs, rhs| Ok(lhs.eq(rhs)))),
            token::T_NEQ => Ok(Box::new(|lhs, rhs| Ok(lhs.not_eq(rhs)))),
            token::T_GTR => Ok(Box::new(|lhs, rhs| Ok(lhs.gt(rhs)))),
            token::T_LSS => Ok(Box::new(|lhs, rhs| Ok(lhs.lt(rhs)))),
            token::T_GTE => Ok(Box::new(|lhs, rhs| Ok(lhs.gt_eq(rhs)))),
            token::T_LTE => Ok(Box::new(|lhs, rhs| Ok(lhs.lt_eq(rhs)))),
            token::T_POW => Ok(Box::new(move |lhs, rhs| {
                Ok(DfExpr::ScalarFunction(ScalarFunction {
                    func: datafusion_functions::math::power(),
                    args: vec![cast_float(lhs), cast_float(rhs)],
                }))
            })),
            token::T_ATAN2 => Ok(Box::new(move |lhs, rhs| {
                Ok(DfExpr::ScalarFunction(ScalarFunction {
                    func: datafusion_functions::math::atan2(),
                    args: vec![cast_float(lhs), cast_float(rhs)],
                }))
            })),
            _ => UnexpectedTokenSnafu { token }.fail(),
        }
    }

    /// Check if the given op is a [comparison operator](https://prometheus.io/docs/prometheus/latest/querying/operators/#comparison-binary-operators).
    fn is_token_a_comparison_op(token: TokenType) -> bool {
        matches!(
            token.id(),
            token::T_EQLC
                | token::T_NEQ
                | token::T_GTR
                | token::T_LSS
                | token::T_GTE
                | token::T_LTE
        )
    }

    /// Check if the given op is a set operator (UNION, INTERSECT and EXCEPT in SQL).
    fn is_token_a_set_op(token: TokenType) -> bool {
        matches!(
            token.id(),
            token::T_LAND // INTERSECT
                | token::T_LOR // UNION
                | token::T_LUNLESS // EXCEPT
        )
    }

    fn align_binary_field_columns<'a>(
        left_schema: &DFSchemaRef,
        right_schema: &DFSchemaRef,
        left_field_columns: &'a [String],
        right_field_columns: &'a [String],
        op: TokenType,
        left_is_scalar: bool,
        right_is_scalar: bool,
    ) -> (
        Vec<(String, Vec<BinaryFieldPair<'a>>)>,
        Vec<BinaryFieldPair<'a>>,
    ) {
        // Mixed vectors store mutually exclusive float and histogram samples in two columns.
        // Retain each valid sample combination and group expressions by their output lane.
        let left_alternative = Self::alternative_sample_columns(left_schema, left_field_columns);
        let right_alternative = Self::alternative_sample_columns(right_schema, right_field_columns);
        let alternative_alignment = match (left_alternative, right_alternative) {
            (Some(output_names), Some(_)) => Some((
                output_names,
                left_field_columns
                    .iter()
                    .flat_map(|left| right_field_columns.iter().map(move |right| (left, right)))
                    .collect::<Vec<_>>(),
            )),
            (Some(output_names), None) if right_field_columns.len() == 1 => Some((
                output_names,
                left_field_columns
                    .iter()
                    .map(|left| (left, &right_field_columns[0]))
                    .collect::<Vec<_>>(),
            )),
            (None, Some(output_names)) if left_field_columns.len() == 1 => Some((
                output_names,
                right_field_columns
                    .iter()
                    .map(|right| (&left_field_columns[0], right))
                    .collect::<Vec<_>>(),
            )),
            _ => None,
        };
        let mut invalid_pairs = Vec::new();
        if let Some(((float_output, histogram_output), field_pairs)) = alternative_alignment {
            let mut float_pairs = Vec::new();
            let mut histogram_pairs = Vec::new();
            for (left, right) in field_pairs {
                let left_is_histogram = Self::field_column_is_native_histogram(left_schema, left);
                let right_is_histogram =
                    Self::field_column_is_native_histogram(right_schema, right);
                match Self::binary_result_is_histogram(op, left_is_histogram, right_is_histogram) {
                    Some(false) => float_pairs.push((left, right)),
                    Some(true) => histogram_pairs.push((left, right)),
                    None => invalid_pairs.push((left, right)),
                }
            }
            if !float_pairs.is_empty() || !histogram_pairs.is_empty() {
                return (
                    [
                        (!float_pairs.is_empty()).then(|| (float_output.to_string(), float_pairs)),
                        (!histogram_pairs.is_empty())
                            .then(|| (histogram_output.to_string(), histogram_pairs)),
                    ]
                    .into_iter()
                    .flatten()
                    .collect(),
                    invalid_pairs,
                );
            }
        }

        if left_is_scalar && !right_is_scalar && left_field_columns.len() == 1 {
            return (
                right_field_columns
                    .iter()
                    .map(|right| (right.clone(), vec![(&left_field_columns[0], right)]))
                    .collect(),
                invalid_pairs,
            );
        }
        if right_is_scalar && !left_is_scalar && right_field_columns.len() == 1 {
            return (
                left_field_columns
                    .iter()
                    .map(|left| (left.clone(), vec![(left, &right_field_columns[0])]))
                    .collect(),
                invalid_pairs,
            );
        }

        (
            left_field_columns
                .iter()
                .zip(right_field_columns.iter())
                .map(|(left, right)| (left.clone(), vec![(left, right)]))
                .collect(),
            invalid_pairs,
        )
    }

    fn binary_result_is_histogram(
        token: TokenType,
        lhs_is_histogram: bool,
        rhs_is_histogram: bool,
    ) -> Option<bool> {
        match (token.id(), lhs_is_histogram, rhs_is_histogram) {
            (_, false, false) => Some(false),
            (token::T_ADD | token::T_SUB, true, true)
            | (token::T_MUL, true, false)
            | (token::T_MUL, false, true)
            | (token::T_DIV, true, false) => Some(true),
            (token::T_EQLC | token::T_NEQ, true, true) => Some(false),
            _ => None,
        }
    }

    fn plan_has_tsid_column(plan: &LogicalPlan) -> bool {
        plan.schema()
            .fields()
            .iter()
            .any(|field| field.name() == DATA_SCHEMA_TSID_COLUMN_NAME)
    }

    fn is_empty_metric(plan: &LogicalPlan) -> bool {
        matches!(plan, LogicalPlan::Extension(Extension { node }) if node.as_any().is::<EmptyMetric>())
    }

    fn native_histogram_arrow_type() -> ArrowDataType {
        native_histogram_value_type().as_arrow_type()
    }

    fn field_column_type<'a>(
        schema: &'a DFSchemaRef,
        field_column: &str,
    ) -> Option<&'a ArrowDataType> {
        schema
            .index_of_column_by_name(None, field_column)
            .map(|idx| schema.field(idx).data_type())
    }

    fn field_column_is_native_histogram(schema: &DFSchemaRef, field_column: &str) -> bool {
        Self::field_column_type(schema, field_column)
            .is_some_and(|data_type| data_type == &Self::native_histogram_arrow_type())
    }

    fn field_columns_contain_native_histogram(
        schema: &DFSchemaRef,
        field_columns: &[String],
    ) -> bool {
        field_columns
            .iter()
            .any(|field| Self::field_column_is_native_histogram(schema, field))
    }

    fn field_column_is_float_range(schema: &DFSchemaRef, field_column: &str) -> bool {
        Self::field_column_type(schema, field_column).is_some_and(|data_type| {
            matches!(
                data_type,
                ArrowDataType::Dictionary(key_type, value_type)
                    if key_type.as_ref() == &ArrowDataType::Int64
                        && value_type.as_ref() == &ArrowDataType::Float64
            )
        })
    }

    fn field_columns_are_alternative_samples(
        schema: &DFSchemaRef,
        field_columns: &[String],
    ) -> bool {
        Self::alternative_sample_columns(schema, field_columns).is_some()
    }

    fn alternative_sample_columns<'a>(
        schema: &DFSchemaRef,
        field_columns: &'a [String],
    ) -> Option<(&'a str, &'a str)> {
        if field_columns.len() != 2 {
            return None;
        }

        let canonical_float = field_columns.iter().find(|field| {
            field.as_str() == greptime_value()
                && (Self::field_column_type(schema, field) == Some(&ArrowDataType::Float64)
                    || Self::field_column_is_float_range(schema, field))
        });
        let canonical_histogram = field_columns.iter().find(|field| {
            field.as_str() == greptime_native_histogram()
                && (Self::field_column_is_native_histogram(schema, field)
                    || Self::field_column_is_native_histogram_range(schema, field))
        });
        if let (Some(float), Some(histogram)) = (canonical_float, canonical_histogram) {
            return Some((float, histogram));
        }

        let float = field_columns.iter().find(|field| {
            field.starts_with(OR_FLOAT_FIELD_PREFIX)
                && (Self::field_column_type(schema, field) == Some(&ArrowDataType::Float64)
                    || Self::field_column_is_float_range(schema, field))
        })?;
        let histogram = field_columns.iter().find(|field| {
            field.starts_with(OR_HISTOGRAM_FIELD_PREFIX)
                && (Self::field_column_is_native_histogram(schema, field)
                    || Self::field_column_is_native_histogram_range(schema, field))
        })?;
        Some((float, histogram))
    }

    fn alternative_sample_range_columns<'a>(
        schema: &DFSchemaRef,
        field_columns: &'a [String],
    ) -> Option<(&'a str, &'a str)> {
        Self::alternative_sample_columns(schema, field_columns).filter(|(float, histogram)| {
            Self::field_column_is_float_range(schema, float)
                && Self::field_column_is_native_histogram_range(schema, histogram)
        })
    }

    fn field_column_is_native_histogram_range(schema: &DFSchemaRef, field_column: &str) -> bool {
        Self::field_column_type(schema, field_column).is_some_and(|data_type| {
            matches!(
                data_type,
                ArrowDataType::Dictionary(key_type, value_type)
                    if key_type.as_ref() == &ArrowDataType::Int64
                        && value_type.as_ref() == &Self::native_histogram_arrow_type()
            )
        })
    }

    fn all_field_columns_are_native_histograms(&self, schema: &DFSchemaRef) -> bool {
        !self.ctx.field_columns.is_empty()
            && self
                .ctx
                .field_columns
                .iter()
                .all(|field| Self::field_column_is_native_histogram(schema, field))
    }

    fn all_field_columns_are_native_histogram_ranges(&self, schema: &DFSchemaRef) -> bool {
        !self.ctx.field_columns.is_empty()
            && self
                .ctx
                .field_columns
                .iter()
                .all(|field| Self::field_column_is_native_histogram_range(schema, field))
    }

    fn optional_tsid_projection(
        schema: &DFSchemaRef,
        table_ref: Option<&TableReference>,
        keep_tsid: bool,
    ) -> Option<DfExpr> {
        keep_tsid.then_some(()).and_then(|_| {
            schema
                .qualified_field_with_name(table_ref, DATA_SCHEMA_TSID_COLUMN_NAME)
                .ok()
                .map(|field| DfExpr::Column(field.into()))
        })
    }

    fn binary_join_key_columns(
        &self,
        left_schema: &DFSchemaRef,
        right_schema: &DFSchemaRef,
        left_context: &PromPlannerContext,
        right_context: &PromPlannerContext,
        only_join_time_index: bool,
        modifier: &Option<BinModifier>,
    ) -> Result<(BTreeSet<String>, BTreeSet<String>, bool)> {
        let has_tsid = |schema: &DFSchemaRef| {
            schema
                .fields()
                .iter()
                .any(|field| field.name() == DATA_SCHEMA_TSID_COLUMN_NAME)
        };
        let use_tsid_join = !only_join_time_index
            && self.binary_modifier_preserves_tsid_join_key(left_context, right_context, modifier)
            && left_context.use_tsid
            && right_context.use_tsid
            && has_tsid(left_schema)
            && has_tsid(right_schema);

        let (mut left_tag_columns, mut right_tag_columns) = if use_tsid_join {
            (
                BTreeSet::from([DATA_SCHEMA_TSID_COLUMN_NAME.to_string()]),
                BTreeSet::from([DATA_SCHEMA_TSID_COLUMN_NAME.to_string()]),
            )
        } else {
            if only_join_time_index {
                (BTreeSet::new(), BTreeSet::new())
            } else {
                (
                    left_context
                        .tag_columns
                        .iter()
                        .cloned()
                        .collect::<BTreeSet<_>>(),
                    right_context
                        .tag_columns
                        .iter()
                        .cloned()
                        .collect::<BTreeSet<_>>(),
                )
            }
        };

        if !use_tsid_join
            && let Some(modifier) = modifier
            && let Some(matching) = &modifier.matching
        {
            match matching {
                LabelModifier::Include(on) => {
                    let mask = on.labels.iter().cloned().collect::<BTreeSet<_>>();
                    left_tag_columns = left_tag_columns.intersection(&mask).cloned().collect();
                    right_tag_columns = right_tag_columns.intersection(&mask).cloned().collect();
                }
                LabelModifier::Exclude(ignoring) => {
                    for label in &ignoring.labels {
                        let _ = left_tag_columns.remove(label);
                        let _ = right_tag_columns.remove(label);
                    }
                }
            }
        }

        let force_empty_join =
            !use_tsid_join && !only_join_time_index && left_tag_columns != right_tag_columns;
        if force_empty_join {
            let common_tag_columns = left_tag_columns
                .intersection(&right_tag_columns)
                .cloned()
                .collect::<BTreeSet<_>>();
            left_tag_columns = common_tag_columns.clone();
            right_tag_columns = common_tag_columns;
        }

        Ok((left_tag_columns, right_tag_columns, force_empty_join))
    }

    /// Result labels of a vector-vector binary operation, following Prometheus `resultMetric`:
    /// `on(...)` keeps only the matching labels, `ignoring(...)` drops them, and a group modifier
    /// keeps the "many" side's labels plus the `group_x(...)` labels taken from the "one" side.
    ///
    /// The flag of each entry tells which operand the label is projected from. `None` means the
    /// operation keeps a whole operand tag set, which the default projection already does.
    fn binary_result_labels(
        left_context: &PromPlannerContext,
        right_context: &PromPlannerContext,
        modifier: &Option<BinModifier>,
    ) -> Option<Vec<(bool, String)>> {
        let modifier = modifier.as_ref()?;
        let (many_is_left, include) = match &modifier.card {
            VectorMatchCardinality::OneToOne => (true, None),
            VectorMatchCardinality::ManyToOne(labels) => (true, Some(labels)),
            VectorMatchCardinality::OneToMany(labels) => (false, Some(labels)),
            // Set operators keep their operands' labels and don't reach this path.
            VectorMatchCardinality::ManyToMany => return None,
        };

        let Some(include) = include else {
            let matching = modifier.matching.as_ref()?;
            let reduced = |keep: bool, labels: &BTreeSet<&String>| {
                left_context
                    .tag_columns
                    .iter()
                    .filter(|tag| labels.contains(tag) == keep)
                    .map(|tag| (true, tag.clone()))
                    .collect()
            };
            return Some(match matching {
                LabelModifier::Include(on) => reduced(true, &on.labels.iter().collect()),
                LabelModifier::Exclude(ignoring) => {
                    reduced(false, &ignoring.labels.iter().collect())
                }
            });
        };

        let (many_context, one_context) = if many_is_left {
            (left_context, right_context)
        } else {
            (right_context, left_context)
        };
        let include = include.labels.iter().collect::<BTreeSet<_>>();
        // An included label the "one" side doesn't carry is deleted from the result, so it is
        // dropped from the "many" side as well.
        let mut labels = many_context
            .tag_columns
            .iter()
            .filter(|tag| !include.contains(tag))
            .map(|tag| (many_is_left, tag.clone()))
            .collect::<Vec<_>>();
        labels.extend(
            include
                .into_iter()
                .filter(|label| one_context.tag_columns.contains(label))
                .map(|label| (!many_is_left, label.clone())),
        );
        Some(labels)
    }

    /// Resolve [`Self::binary_result_labels`] against the join output.
    fn binary_result_label_projection(
        schema: &DFSchemaRef,
        left_table_ref: &TableReference,
        right_table_ref: &TableReference,
        left_context: &PromPlannerContext,
        right_context: &PromPlannerContext,
        labels: Vec<(bool, String)>,
    ) -> Result<BinaryResultLabels> {
        let mut exprs = Vec::with_capacity(labels.len());
        let mut names = Vec::with_capacity(labels.len());
        let mut aggregation_field_labels = Vec::new();
        let mut sources = HashSet::new();
        for (from_left, label) in labels {
            let (table_ref, context) = if from_left {
                (left_table_ref, left_context)
            } else {
                (right_table_ref, right_context)
            };
            let field = schema
                .qualified_field_with_name(Some(table_ref), &label)
                .context(DataFusionPlanningSnafu)?;
            exprs.push(DfExpr::Column(field.into()));
            if context.aggregation_field_labels.contains(&label) {
                aggregation_field_labels.push(label.clone());
            }
            sources.insert(from_left);
            names.push(label);
        }

        // One operand contributing every one of its tags as the whole result label set keeps the
        // one-to-one correspondence between its `__tsid` and a result series: no other operand
        // value reaches the labels, and the matching gives each of its rows a single partner.
        let source_context = match sources.into_iter().collect::<Vec<_>>().as_slice() {
            [true] => Some((left_table_ref, left_context)),
            [false] => Some((right_table_ref, right_context)),
            _ => None,
        };
        let tsid = source_context
            .filter(|(_, context)| {
                context.use_tsid
                    && context.tag_columns.len() == names.len()
                    && context.tag_columns.iter().all(|tag| names.contains(tag))
            })
            .and_then(|(table_ref, _)| {
                Self::optional_tsid_projection(schema, Some(table_ref), true)
            });

        Ok(BinaryResultLabels {
            exprs,
            names,
            aggregation_field_labels,
            tsid,
        })
    }

    /// Whether the result of a binary operation can hold two series with the same labels, which
    /// only [`Self::binary_result_labels`] can introduce: one-to-one matching on a subset of the
    /// tags, or a group modifier that overwrites a label of the "many" side.
    fn binary_result_labels_may_repeat(
        left_context: &PromPlannerContext,
        right_context: &PromPlannerContext,
        modifier: &Option<BinModifier>,
    ) -> bool {
        let Some(modifier) = modifier else {
            return false;
        };
        match &modifier.card {
            VectorMatchCardinality::OneToOne => match &modifier.matching {
                None => false,
                Some(LabelModifier::Include(on)) => {
                    let on = on.labels.iter().collect::<BTreeSet<_>>();
                    !left_context.tag_columns.iter().all(|tag| on.contains(tag))
                }
                Some(LabelModifier::Exclude(ignoring)) => ignoring
                    .labels
                    .iter()
                    .any(|label| left_context.tag_columns.contains(label)),
            },
            VectorMatchCardinality::ManyToOne(include) => include
                .labels
                .iter()
                .any(|label| left_context.tag_columns.contains(label)),
            VectorMatchCardinality::OneToMany(include) => include
                .labels
                .iter()
                .any(|label| right_context.tag_columns.contains(label)),
            VectorMatchCardinality::ManyToMany => false,
        }
    }

    /// Wrap `plan` in a check that fails the query when a match group holds more than one row at
    /// a timestamp. `group_exprs` are the label columns of the group, resolved against `plan`.
    fn assert_unique_match_group(
        plan: LogicalPlan,
        group_exprs: Vec<DfExpr>,
        group_labels: Vec<String>,
        time_index_expr: DfExpr,
        violation: MatchGroupViolation,
    ) -> Result<LogicalPlan> {
        let mut partition_by = group_exprs.clone();
        partition_by.push(time_index_expr);
        // A label may carry the generated name, and the count column has to stay unambiguous
        // against every column the operand already has.
        let occupied_column_names = plan
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect::<HashSet<_>>();
        let mut next_suffix = 0;
        let count_column = loop {
            let name = match next_suffix {
                0 => MATCH_GROUP_COUNT_COLUMN.to_string(),
                suffix => format!("{MATCH_GROUP_COUNT_COLUMN}_{suffix}"),
            };
            next_suffix += 1;
            if !occupied_column_names.contains(name.as_str()) {
                break name;
            }
        };
        let count = DfExpr::WindowFunction(Box::new(WindowFunction {
            fun: WindowFunctionDefinition::AggregateUDF(count_udaf()),
            params: WindowFunctionParams {
                args: vec![lit(1i64)],
                partition_by,
                order_by: vec![],
                window_frame: WindowFrame::new(None),
                null_treatment: None,
                distinct: false,
                filter: None,
            },
        }))
        .alias(count_column.as_str());

        let output_exprs = plan
            .schema()
            .iter()
            .map(|(qualifier, field)| DfExpr::Column(Column::new(qualifier.cloned(), field.name())))
            .collect::<Vec<_>>();
        let assert_expr = DfExpr::ScalarFunction(ScalarFunction {
            func: Arc::new(UniqueMatchGroup::scalar_udf(group_labels, violation)),
            args: std::iter::once(ident(count_column.as_str()))
                .chain(group_exprs)
                .collect(),
        });

        LogicalPlanBuilder::from(plan)
            .window(vec![count])
            .context(DataFusionPlanningSnafu)?
            .filter(assert_expr)
            .context(DataFusionPlanningSnafu)?
            .project(output_exprs)
            .context(DataFusionPlanningSnafu)?
            .build()
            .context(DataFusionPlanningSnafu)
    }

    fn binary_modifier_preserves_tsid_join_key(
        &self,
        left_context: &PromPlannerContext,
        right_context: &PromPlannerContext,
        modifier: &Option<BinModifier>,
    ) -> bool {
        let Some(modifier) = modifier else {
            return true;
        };

        if !matches!(modifier.card, VectorMatchCardinality::OneToOne) {
            return false;
        }

        match &modifier.matching {
            None => true,
            Some(LabelModifier::Exclude(ignoring)) => ignoring.labels.iter().all(|label| {
                !left_context.tag_columns.contains(label)
                    && !right_context.tag_columns.contains(label)
            }),
            Some(LabelModifier::Include(on)) => {
                let on_labels = on.labels.iter().cloned().collect::<BTreeSet<_>>();
                let left_labels = left_context
                    .tag_columns
                    .iter()
                    .cloned()
                    .collect::<BTreeSet<_>>();
                let right_labels = right_context
                    .tag_columns
                    .iter()
                    .cloned()
                    .collect::<BTreeSet<_>>();

                on_labels == left_labels && on_labels == right_labels
            }
        }
    }

    /// Build a inner join on time index column and tag columns to concat two logical plans.
    /// When `only_join_time_index == true` we only join on the time index, because these two plan may not have the same tag columns
    #[allow(clippy::too_many_arguments)]
    fn join_on_non_field_columns(
        &self,
        left: LogicalPlan,
        right: LogicalPlan,
        left_table_ref: TableReference,
        right_table_ref: TableReference,
        left_time_index_column: Option<String>,
        right_time_index_column: Option<String>,
        only_join_time_index: bool,
        modifier: &Option<BinModifier>,
        left_context: &PromPlannerContext,
        right_context: &PromPlannerContext,
    ) -> Result<LogicalPlan> {
        let (mut left_tag_columns, mut right_tag_columns, mut force_empty_join) = self
            .binary_join_key_columns(
                left.schema(),
                right.schema(),
                left_context,
                right_context,
                only_join_time_index,
                modifier,
            )?;
        let use_tsid_join = !only_join_time_index
            && !force_empty_join
            && left_tag_columns == BTreeSet::from([DATA_SCHEMA_TSID_COLUMN_NAME.to_string()])
            && right_tag_columns == BTreeSet::from([DATA_SCHEMA_TSID_COLUMN_NAME.to_string()]);
        let (left, right, left_matched_tags, right_matched_tags) = if !only_join_time_index
            && !use_tsid_join
            && Self::only_temporality_match_label_mismatches(left_context, right_context, modifier)
        {
            let mut aligned_left_context = left_context.clone();
            let mut aligned_right_context = right_context.clone();
            let (left, right, _) = Self::align_temporality_match_column(
                left,
                right,
                &mut aligned_left_context,
                &mut aligned_right_context,
            )?;
            (left_tag_columns, right_tag_columns, force_empty_join) = self
                .binary_join_key_columns(
                    left.schema(),
                    right.schema(),
                    &aligned_left_context,
                    &aligned_right_context,
                    false,
                    modifier,
                )?;
            (
                left,
                right,
                aligned_left_context.tag_columns,
                aligned_right_context.tag_columns,
            )
        } else {
            (
                left,
                right,
                left_context.tag_columns.clone(),
                right_context.tag_columns.clone(),
            )
        };

        // A join key that covers the whole tag set of a side already makes that side's match
        // groups unique, and so does a join on `__tsid`. Only the reduced keys need the check.
        let checked_matching = !only_join_time_index
            && !force_empty_join
            && !use_tsid_join
            && !matches!(
                modifier.as_ref().map(|modifier| &modifier.card),
                Some(VectorMatchCardinality::ManyToMany)
            );
        // The right operand is the "one" side of the matching, unless `group_right` swaps them.
        let one_side_is_left = matches!(
            modifier.as_ref().map(|modifier| &modifier.card),
            Some(VectorMatchCardinality::OneToMany(_))
        );
        let (left, right) = if !checked_matching {
            (left, right)
        } else if one_side_is_left {
            (
                Self::assert_unique_one_side(
                    left,
                    &left_tag_columns,
                    &left_matched_tags,
                    left_time_index_column.as_deref(),
                    true,
                )?,
                right,
            )
        } else {
            (
                left,
                Self::assert_unique_one_side(
                    right,
                    &right_tag_columns,
                    &right_matched_tags,
                    right_time_index_column.as_deref(),
                    false,
                )?,
            )
        };

        // push time index column if it exists
        if let (Some(left_time_index_column), Some(right_time_index_column)) = (
            left_time_index_column.clone(),
            right_time_index_column.clone(),
        ) {
            left_tag_columns.insert(left_time_index_column);
            right_tag_columns.insert(right_time_index_column);
        }

        let right = LogicalPlanBuilder::from(right)
            .alias(right_table_ref.clone())
            .context(DataFusionPlanningSnafu)?
            .build()
            .context(DataFusionPlanningSnafu)?;

        // Inner Join on time index column to concat two operator
        let join_plan = LogicalPlanBuilder::from(left)
            .alias(left_table_ref.clone())
            .context(DataFusionPlanningSnafu)?
            .join_detailed(
                right,
                JoinType::Inner,
                (
                    left_tag_columns
                        .into_iter()
                        .map(Column::from_name)
                        .collect::<Vec<_>>(),
                    right_tag_columns
                        .into_iter()
                        .map(Column::from_name)
                        .collect::<Vec<_>>(),
                ),
                force_empty_join.then_some(lit(false)),
                NullEquality::NullEqualsNull,
            )
            .context(DataFusionPlanningSnafu)?
            .build()
            .context(DataFusionPlanningSnafu)?;

        // The result labels no longer identify a series on their own: the "many" side can hold
        // several series per result label set, which PromQL cannot represent.
        let labels = checked_matching
            .then(|| Self::binary_result_labels(left_context, right_context, modifier))
            .flatten()
            .filter(|_| {
                Self::binary_result_labels_may_repeat(left_context, right_context, modifier)
            });
        let Some(labels) = labels else {
            return Ok(join_plan);
        };
        let schema = join_plan.schema().clone();
        let group_exprs = labels
            .iter()
            .map(|(from_left, label)| {
                let table_ref = if *from_left {
                    &left_table_ref
                } else {
                    &right_table_ref
                };
                schema
                    .qualified_field_with_name(Some(table_ref), label)
                    .map(|field| DfExpr::Column(field.into()))
                    .context(DataFusionPlanningSnafu)
            })
            .collect::<Result<Vec<_>>>()?;
        let time_index_expr = left_time_index_column
            .as_deref()
            .map(|column| {
                schema
                    .qualified_field_with_name(Some(&left_table_ref), column)
                    .map(|field| DfExpr::Column(field.into()))
                    .context(DataFusionPlanningSnafu)
            })
            .transpose()?
            .with_context(|| UnexpectedPlanExprSnafu {
                desc: "vector matching on a plan without a time index",
            })?;
        let violation = if matches!(
            modifier.as_ref().map(|modifier| &modifier.card),
            Some(VectorMatchCardinality::OneToOne)
        ) {
            MatchGroupViolation::ImplicitManyToOne
        } else {
            MatchGroupViolation::AmbiguousGroupLabels
        };
        Self::assert_unique_match_group(
            join_plan,
            group_exprs,
            labels.into_iter().map(|(_, label)| label).collect(),
            time_index_expr,
            violation,
        )
    }

    /// Guard the side of a vector matching that must hold one series per match group.
    fn assert_unique_one_side(
        plan: LogicalPlan,
        join_keys: &BTreeSet<String>,
        tag_columns: &[String],
        time_index_column: Option<&str>,
        one_side_is_left: bool,
    ) -> Result<LogicalPlan> {
        let Some(time_index_column) = time_index_column else {
            return Ok(plan);
        };
        if tag_columns.iter().all(|tag| join_keys.contains(tag)) {
            return Ok(plan);
        }

        let group_labels = join_keys.iter().cloned().collect::<Vec<_>>();
        let group_exprs = group_labels
            .iter()
            .map(|label| DfExpr::Column(Column::from_name(label)))
            .collect();
        Self::assert_unique_match_group(
            plan,
            group_exprs,
            group_labels,
            DfExpr::Column(Column::from_name(time_index_column)),
            MatchGroupViolation::DuplicateOnOneSide { one_side_is_left },
        )
    }

    fn selected_binary_match_labels(
        left_context: &PromPlannerContext,
        right_context: &PromPlannerContext,
        modifier: &Option<BinModifier>,
    ) -> BTreeSet<String> {
        let mut labels = left_context
            .tag_columns
            .iter()
            .chain(&right_context.tag_columns)
            .cloned()
            .collect::<BTreeSet<_>>();
        if let Some(matching) = modifier
            .as_ref()
            .and_then(|modifier| modifier.matching.as_ref())
        {
            match matching {
                LabelModifier::Include(on) => {
                    labels = on
                        .labels
                        .iter()
                        .filter(|label| {
                            left_context.tag_columns.contains(label)
                                || right_context.tag_columns.contains(label)
                        })
                        .cloned()
                        .collect();
                }
                LabelModifier::Exclude(ignoring) => {
                    for label in &ignoring.labels {
                        labels.remove(label);
                    }
                }
            }
        }
        labels
    }

    fn only_temporality_match_label_mismatches(
        left_context: &PromPlannerContext,
        right_context: &PromPlannerContext,
        modifier: &Option<BinModifier>,
    ) -> bool {
        let mut mismatches =
            Self::selected_binary_match_labels(left_context, right_context, modifier)
                .into_iter()
                .filter(|label| {
                    left_context.tag_columns.contains(label)
                        != right_context.tag_columns.contains(label)
                });
        matches!(
            (mismatches.next(), mismatches.next()),
            (Some(label), None) if label == OTLP_AGGREGATION_TEMPORALITY_LABEL
        )
    }

    fn align_temporality_match_column(
        mut left: LogicalPlan,
        mut right: LogicalPlan,
        left_context: &mut PromPlannerContext,
        right_context: &mut PromPlannerContext,
    ) -> Result<(LogicalPlan, LogicalPlan, bool)> {
        let marker = OTLP_AGGREGATION_TEMPORALITY_LABEL;
        let left_has_marker = left_context.tag_columns.iter().any(|tag| tag == marker);
        let (data_type, value_type, add_to_left) = {
            let (present, add_to_left) = if left_has_marker {
                (&left, false)
            } else {
                (&right, true)
            };
            let data_type = present
                .schema()
                .fields()
                .iter()
                .find(|field| field.name() == marker)
                .map(|field| field.data_type().clone())
                .with_context(|| ColumnNotFoundSnafu {
                    col: marker.to_string(),
                })?;
            let value_type = Self::string_value_data_type(&data_type)
                .cloned()
                .with_context(|| UnexpectedPlanExprSnafu {
                    desc: format!("temporality match label {marker} must be a string"),
                })?;
            (data_type, value_type, add_to_left)
        };
        let null = Self::string_scalar_value(&value_type, None).with_context(|| {
            UnexpectedPlanExprSnafu {
                desc: format!("temporality match label {marker} must be a string"),
            }
        })?;
        let add_marker = |plan: LogicalPlan| {
            let visible = plan
                .schema()
                .iter()
                .map(|(qualifier, field)| {
                    DfExpr::Column(Column::new(qualifier.cloned(), field.name().clone()))
                })
                .collect::<Vec<_>>();
            LogicalPlanBuilder::from(plan)
                .project(
                    visible
                        .into_iter()
                        .chain([DfExpr::Literal(null, None).alias(marker)]),
                )
                .context(DataFusionPlanningSnafu)?
                .build()
                .context(DataFusionPlanningSnafu)
        };
        if data_type != value_type {
            let present = if add_to_left { &mut right } else { &mut left };
            let visible = present
                .schema()
                .iter()
                .map(|(qualifier, field)| {
                    let column =
                        DfExpr::Column(Column::new(qualifier.cloned(), field.name().clone()));
                    if field.name() == marker {
                        DfExpr::Cast(Cast::new(Box::new(column), value_type.clone()))
                            .alias_qualified(qualifier.cloned(), field.name().clone())
                    } else {
                        column
                    }
                })
                .collect::<Vec<_>>();
            *present = LogicalPlanBuilder::from(present.clone())
                .project(visible)
                .context(DataFusionPlanningSnafu)?
                .build()
                .context(DataFusionPlanningSnafu)?;
        }
        if add_to_left {
            left = add_marker(left)?;
            left_context.tag_columns.push(marker.to_string());
        } else {
            right = add_marker(right)?;
            right_context.tag_columns.push(marker.to_string());
        }
        Ok((left, right, add_to_left))
    }

    fn normalized_match_key_expr(
        label: &str,
        field: Option<(Option<TableReference>, ArrowDataType)>,
        value_type: &ArrowDataType,
        internal_name: &str,
    ) -> DfExpr {
        let empty = Self::string_scalar_value(value_type, Some(String::new()))
            .expect("match label value type is a string");
        let expr = if let Some((qualifier, data_type)) = field {
            let column = DfExpr::Column(Column::new(qualifier, label));
            let column = if &data_type == value_type {
                column
            } else {
                DfExpr::Cast(Cast::new(Box::new(column), value_type.clone()))
            };
            DfExpr::ScalarFunction(ScalarFunction {
                func: coalesce(),
                args: vec![column, DfExpr::Literal(empty, None)],
            })
        } else {
            DfExpr::Literal(empty, None)
        };
        expr.alias(internal_name)
    }

    fn is_zero_row_empty_relation(plan: &LogicalPlan) -> bool {
        // `produce_one_row` is used for input-free plans that still emit one row;
        // only the false case is a statically proven empty vector.
        matches!(plan, LogicalPlan::EmptyRelation(relation) if !relation.produce_one_row)
    }

    fn string_value_data_type(data_type: &ArrowDataType) -> Option<&ArrowDataType> {
        match data_type {
            data_type if data_type.is_string() => Some(data_type),
            ArrowDataType::Dictionary(_, value_type) if value_type.is_string() => Some(value_type),
            _ => None,
        }
    }

    fn string_scalar_value(
        data_type: &ArrowDataType,
        value: Option<String>,
    ) -> Option<ScalarValue> {
        match data_type {
            ArrowDataType::Utf8 => Some(ScalarValue::Utf8(value)),
            ArrowDataType::LargeUtf8 => Some(ScalarValue::LargeUtf8(value)),
            ArrowDataType::Utf8View => Some(ScalarValue::Utf8View(value)),
            ArrowDataType::Dictionary(key_type, value_type) => Some(ScalarValue::Dictionary(
                key_type.clone(),
                Box::new(Self::string_scalar_value(value_type, value)?),
            )),
            _ => None,
        }
    }

    fn common_label_data_type(
        left: Option<&ArrowDataType>,
        right: Option<&ArrowDataType>,
    ) -> Option<ArrowDataType> {
        match (left, right) {
            (Some(left), Some(right)) if left == right => {
                Self::string_value_data_type(left).map(|_| left.clone())
            }
            (Some(left), Some(right)) => {
                let left_value_type = Self::string_value_data_type(left)?;
                let right_value_type = Self::string_value_data_type(right)?;
                // DataFusion projections can decode dictionaries, but do not encode plain strings
                // as dictionaries. Preserve the encoding only when both inputs already share it.
                match (left_value_type, right_value_type) {
                    (left, right) if left == right => Some(left.clone()),
                    (ArrowDataType::LargeUtf8, _) | (_, ArrowDataType::LargeUtf8) => {
                        Some(ArrowDataType::LargeUtf8)
                    }
                    (ArrowDataType::Utf8View, ArrowDataType::Utf8View) => {
                        Some(ArrowDataType::Utf8View)
                    }
                    _ => Some(ArrowDataType::Utf8),
                }
            }
            (Some(data_type), None) | (None, Some(data_type)) => {
                Self::string_value_data_type(data_type).cloned()
            }
            (None, None) => Some(ArrowDataType::Utf8),
        }
    }

    /// Build a projection that project and perform operation expr for every value columns.
    /// Non-value columns (tag and timestamp) will be preserved in the projection.
    ///
    /// # Side effect
    ///
    /// This function will update the value columns in the context. Those new column names
    /// don't contains qualifier.
    fn projection_for_each_field_column<F>(
        &mut self,
        input: LogicalPlan,
        name_to_expr: F,
    ) -> Result<LogicalPlan>
    where
        F: FnMut(&String) -> Result<DfExpr>,
    {
        self.projection_for_each_field_column_with_labels(input, None, name_to_expr)
    }

    /// Like [`Self::projection_for_each_field_column`], but projects `result_labels` instead of
    /// the context tag columns when a binary operation derived its own result label set.
    fn projection_for_each_field_column_with_labels<F>(
        &mut self,
        input: LogicalPlan,
        result_labels: Option<&BinaryResultLabels>,
        name_to_expr: F,
    ) -> Result<LogicalPlan>
    where
        F: FnMut(&String) -> Result<DfExpr>,
    {
        // Keep the generated float/histogram lane names while an element-wise operation
        // preserves both sample types, so downstream operators still recognize the pair.
        let preserve_field_names =
            Self::field_columns_are_alternative_samples(input.schema(), &self.ctx.field_columns);
        let table_ref = self.ctx.table_name.clone().map(TableReference::bare);
        // Derived labels can be unqualified even when the context still names the source table.
        let input_schema = input.schema().clone();
        let lookup = |col: &String| {
            input_schema
                .qualified_field_with_name(table_ref.as_ref(), col)
                .or_else(|_| input_schema.qualified_field_with_unqualified_name(col))
                .map(|field| DfExpr::Column(field.into()))
                .context(DataFusionPlanningSnafu)
        };
        let tag_columns_iter = match result_labels {
            Some(labels) => labels.exprs.iter().cloned().map(Ok).collect::<Vec<_>>(),
            None => self.ctx.tag_columns.iter().map(lookup).collect::<Vec<_>>(),
        };
        let non_field_columns_iter = tag_columns_iter
            .into_iter()
            .chain(self.ctx.time_index_column.iter().map(lookup));
        let tsid_iter = match result_labels {
            Some(labels) => labels.tsid_projection(table_ref.clone()),
            None => Self::optional_tsid_projection(
                input.schema(),
                table_ref.as_ref(),
                self.ctx.use_tsid,
            ),
        }
        .into_iter()
        .map(Ok);

        // build computation exprs
        let result_field_columns = self
            .ctx
            .field_columns
            .iter()
            .map(name_to_expr)
            .collect::<Result<Vec<_>>>()?;

        // alias the computation exprs to remove qualifier
        if !preserve_field_names {
            self.ctx.field_columns = result_field_columns
                .iter()
                .map(|expr| expr.schema_name().to_string())
                .collect();
        }
        let field_columns_iter = result_field_columns
            .into_iter()
            .zip(self.ctx.field_columns.iter())
            .map(|(expr, name)| Ok(DfExpr::Alias(Alias::new(expr, None::<String>, name))));

        // chain non-field columns (unchanged) and field columns (applied computation then alias)
        let project_fields = non_field_columns_iter
            .chain(tsid_iter)
            .chain(field_columns_iter)
            .collect::<Result<Vec<_>>>()?;

        LogicalPlanBuilder::from(input)
            .project(project_fields)
            .context(DataFusionPlanningSnafu)?
            .build()
            .context(DataFusionPlanningSnafu)
    }

    /// Build a filter plan on one value column or a float/histogram alternative pair.
    fn filter_on_field_column<F>(&self, input: LogicalPlan, name_to_expr: F) -> Result<LogicalPlan>
    where
        F: FnMut(&String) -> Result<DfExpr>,
    {
        ensure!(
            self.ctx.field_columns.len() == 1
                || Self::field_columns_are_alternative_samples(
                    input.schema(),
                    &self.ctx.field_columns,
                ),
            UnsupportedExprSnafu {
                name: "filter on multi-value input"
            }
        );

        let field_column_filters = self
            .ctx
            .field_columns
            .iter()
            .map(name_to_expr)
            .collect::<Result<Vec<_>>>()?;
        let field_column_filter =
            disjunction(field_column_filters).context(UnsupportedExprSnafu {
                name: "filter on empty input",
            })?;

        LogicalPlanBuilder::from(input)
            .filter(field_column_filter)
            .context(DataFusionPlanningSnafu)?
            .build()
            .context(DataFusionPlanningSnafu)
    }

    /// Generate an expr like `date_part("hour", <TIME_INDEX>)`. Caller should ensure the
    /// time index column in context is set
    fn date_part_on_time_index(&self, date_part: &str) -> Result<DfExpr> {
        let input_expr = datafusion::logical_expr::ident(
            self.ctx
                .time_index_column
                .as_ref()
                // table name doesn't matters here
                .with_context(|| TimeIndexNotFoundSnafu {
                    table: "<doesn't matter>",
                })?,
        );
        let fn_expr = DfExpr::ScalarFunction(ScalarFunction {
            func: datafusion_functions::datetime::date_part(),
            args: vec![date_part.lit(), input_expr],
        });
        Ok(fn_expr)
    }

    fn strip_tsid_column(&self, plan: LogicalPlan) -> Result<LogicalPlan> {
        let schema = plan.schema();
        if !schema
            .fields()
            .iter()
            .any(|field| field.name() == DATA_SCHEMA_TSID_COLUMN_NAME)
        {
            return Ok(plan);
        }

        // Preserve column qualifiers so downstream plan nodes can keep referencing
        // the columns by their original qualified names.
        let project_exprs = schema
            .iter()
            .filter(|(_, field)| field.name() != DATA_SCHEMA_TSID_COLUMN_NAME)
            .map(|(qualifier, field)| {
                DfExpr::Column(Column::new(qualifier.cloned(), field.name().clone()))
            })
            .collect::<Vec<_>>();

        LogicalPlanBuilder::from(plan)
            .project(project_exprs)
            .context(DataFusionPlanningSnafu)?
            .build()
            .context(DataFusionPlanningSnafu)
    }

    /// Apply an alias to the query result by adding a projection with the alias name
    fn apply_alias(&mut self, plan: LogicalPlan, alias_name: String) -> Result<LogicalPlan> {
        let fields_expr = self.create_field_column_exprs()?;

        // TODO(dennis): how to support multi-value aliasing?
        ensure!(
            fields_expr.len() == 1,
            UnsupportedExprSnafu {
                name: "alias on multi-value result"
            }
        );

        let project_fields = fields_expr
            .into_iter()
            .map(|expr| expr.alias(&alias_name))
            .chain(self.create_tag_column_exprs()?)
            .chain(Some(self.create_time_index_column_expr()?));

        LogicalPlanBuilder::from(plan)
            .project(project_fields)
            .context(DataFusionPlanningSnafu)?
            .build()
            .context(DataFusionPlanningSnafu)
    }
}

#[derive(Default, Debug)]
struct FunctionArgs {
    input: Option<PromExpr>,
    literals: Vec<ScalarParam>,
}

/// A scalar parameter of a PromQL function call.
#[derive(Debug, Clone)]
enum ScalarParam {
    /// A parameter whose value is known while planning, e.g. the `2` of
    /// `clamp_min(some_metric, 2)`.
    Static(DfExpr),
    /// A parameter that is only computed when the query runs, i.e. a
    /// `scalar(...)` call such as the `scalar(vector(3))` of
    /// `clamp_min(some_metric, scalar(vector(3)))`.
    ///
    /// It is planned as a scalar subquery by
    /// [`PromPlanner::build_scalar_params`], which also rejects the functions
    /// and query shapes that cannot broadcast a runtime parameter.
    Dynamic(PromExpr),
}

/// Represents different types of scalar functions supported in PromQL expressions.
/// Each variant defines how the function should be processed and what arguments it expects.
#[derive(Debug, Clone)]
enum ScalarFunc {
    /// DataFusion's registered(including built-in) scalar functions (e.g., abs, sqrt, round, clamp).
    /// These are passed through directly to DataFusion's execution engine.
    /// Processing: Simple argument insertion at the specified position.
    DataFusionBuiltin(Arc<ScalarUdfDef>),
    /// User-defined functions registered in DataFusion's function registry.
    /// Similar to DataFusionBuiltin but for custom functions not built into DataFusion.
    /// Processing: Direct pass-through with argument positioning.
    DataFusionUdf(Arc<ScalarUdfDef>),
    /// Native histogram helper UDFs. Non-histogram inputs are projected as NULL
    /// so the normal PromQL empty-value filter drops them.
    NativeHistogramUdf(Arc<ScalarUdfDef>),
    /// PromQL-specific functions that operate on time series data with temporal context.
    /// These functions require both timestamp ranges and values to perform calculations.
    /// Processing: Automatically injects timestamp_range and value columns as first arguments.
    /// Examples: idelta, irate, resets, changes, deriv, *_over_time function
    Udf(Arc<ScalarUdfDef>),
    /// PromQL functions requiring extrapolation calculations with explicit range information.
    /// These functions need to know the time range length to perform rate calculations.
    /// The second field contains the range length in milliseconds.
    /// Processing: Injects timestamp_range, value, time_index columns and appends range_length.
    /// Examples: increase, rate, delta
    // TODO(ruihang): maybe merge with Udf later
    ExtrapolateUdf(Arc<ScalarUdfDef>, i64),
    /// Functions that generate expressions directly without external UDF calls.
    /// The expression is constructed during function matching and requires no additional processing.
    /// Examples: time(), minute(), hour(), month(), year() and other date/time extractors
    GeneratedExpr,
}

#[cfg(test)]
mod test;
