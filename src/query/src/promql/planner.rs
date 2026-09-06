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

use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};
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
    BinaryExpr, Cast, Extension, LogicalPlan, LogicalPlanBuilder, Operator,
    ScalarUDF as ScalarUdfDef, WindowFrame, WindowFunctionDefinition,
};
use datafusion::optimizer::simplify_expressions::ExprSimplifier;
use datafusion::prelude as df_prelude;
use datafusion::prelude::{Column, Expr as DfExpr, JoinType};
use datafusion::scalar::ScalarValue;
use datafusion::sql::TableReference;
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRewriter};
use datafusion_common::{DFSchema, NullEquality};
use datafusion_expr::expr::WindowFunctionParams;
use datafusion_expr::expr_fn::when;
use datafusion_expr::simplify::SimplifyContext;
use datafusion_expr::utils::{conjunction, disjunction};
use datafusion_expr::{
    ExprSchemable, Literal, Projection, SortExpr, TableScan, TableSource, col, lit,
};
use datafusion_functions::core::coalesce;
use datatypes::arrow::datatypes::{DataType as ArrowDataType, TimeUnit as ArrowTimeUnit};
use datatypes::data_type::{ConcreteDataType, DataType as GreptimeDataType};
use itertools::Itertools;
use once_cell::sync::Lazy;
use promql::extension_plan::{
    Absent, EmptyMetric, HistogramFold, HistogramFoldOperation, InstantManipulate, Millisecond,
    RangeManipulate, ScalarCalculate, SeriesDivide, SeriesNormalize, UnionDistinctOn,
    build_special_time_expr,
};
use promql::functions::{
    AbsentOverTime, AvgOverTime, Changes, CountOverTime, Delta, Deriv, DoubleExponentialSmoothing,
    IDelta, Increase, LastOverTime, MaxOverTime, MinOverTime, MixedRange,
    NativeHistogramAbsentOverTime, NativeHistogramAdd, NativeHistogramAggAvg,
    NativeHistogramAggSum, NativeHistogramAvg, NativeHistogramAvgOverTime, NativeHistogramChanges,
    NativeHistogramCount, NativeHistogramCountOverTime, NativeHistogramDelta,
    NativeHistogramDivScalar, NativeHistogramDrop, NativeHistogramEq, NativeHistogramFraction,
    NativeHistogramIDelta, NativeHistogramIRate, NativeHistogramIncrease,
    NativeHistogramLastOverTime, NativeHistogramMulScalar, NativeHistogramNeg,
    NativeHistogramNotEq, NativeHistogramPresentOverTime, NativeHistogramQuantile,
    NativeHistogramRate, NativeHistogramResets, NativeHistogramScalarMul, NativeHistogramStddev,
    NativeHistogramStdvar, NativeHistogramSub, NativeHistogramSum, NativeHistogramSumOverTime,
    NativeHistogramToString, PredictLinear, PresentOverTime, PromqlFloatToString, QuantileOverTime,
    Rate, Resets, Round, StddevOverTime, StdvarOverTime, SumOverTime, quantile_udaf,
};
use promql_parser::label::{METRIC_NAME, MatchOp, Matcher, Matchers};
use promql_parser::parser::token::TokenType;
use promql_parser::parser::value::ValueType;
use promql_parser::parser::{
    AggregateExpr, BinModifier, BinaryExpr as PromBinaryExpr, Call, EvalStmt, Expr as PromExpr,
    Function, FunctionArgs as PromFunctionArgs, LabelModifier, MatrixSelector, NumberLiteral,
    Offset, ParenExpr, StringLiteral, SubqueryExpr, UnaryExpr, VectorMatchCardinality,
    VectorSelector, token,
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
    CatalogSnafu, ColumnNotFoundSnafu, CombineTableColumnMismatchSnafu, DataFusionPlanningSnafu,
    ExpectRangeSelectorSnafu, FunctionInvalidArgumentSnafu, InvalidDestinationLabelNameSnafu,
    InvalidRegularExpressionSnafu, InvalidTimeRangeSnafu, MultiFieldsNotSupportedSnafu,
    MultipleMetricMatchersSnafu, MultipleVectorSnafu, NoMetricMatcherSnafu, PromqlPlanNodeSnafu,
    Result, SameLabelSetSnafu, TableNameNotFoundSnafu, TimeIndexNotFoundSnafu,
    UnexpectedPlanExprSnafu, UnexpectedTokenSnafu, UnknownTableSnafu, UnsupportedExprSnafu,
    UnsupportedMatcherOpSnafu, UnsupportedVectorMatchSnafu, ValueNotFoundSnafu,
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

/// Prefix for generated binary island leaf aliases.
const BINARY_ISLAND_LEAF_ALIAS_PREFIX: &str = "__prom_v";
const OR_FLOAT_FIELD_PREFIX: &str = "__promql_or_float_";
const OR_HISTOGRAM_FIELD_PREFIX: &str = "__promql_or_histogram_";
const TIMESTAMP_VALUE_PREFIX: &str = "__promql_timestamp_value_";

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

    // planner states
    table_name: Option<String>,
    time_index_column: Option<String>,
    field_columns: Vec<String>,
    tag_columns: Vec<String>,
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
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct VectorLeafKey {
    metric_name: String,
    matchers: Vec<(String, String, String)>,
    or_matchers: Vec<Vec<(String, String, String)>>,
    offset_ms: i128,
    at: String,
}

#[derive(Debug, Clone)]
struct IslandLeaf {
    selector: VectorSelector,
    display_table: String,
}

#[derive(Debug, Clone)]
enum IslandExpr {
    VectorLeaf(usize),
    Scalar(DfExpr),
    Unary {
        input: Box<IslandExpr>,
    },
    Binary {
        op: TokenType,
        lhs: Box<IslandExpr>,
        rhs: Box<IslandExpr>,
    },
}

impl IslandExpr {
    fn try_new(expr: &PromExpr, env: &mut IslandCollectEnv) -> Option<Self> {
        if let Some(expr) = PromPlanner::try_build_literal_expr(expr) {
            return Some(Self::Scalar(expr));
        }

        match expr {
            PromExpr::Paren(ParenExpr { expr }) => Self::try_new(expr, env),
            PromExpr::VectorSelector(selector) => {
                let leaf = env.intern_leaf(selector)?;
                Some(Self::VectorLeaf(leaf))
            }
            PromExpr::Unary(UnaryExpr { expr }) => {
                let input = Self::try_new(expr, env)?;
                Some(Self::Unary {
                    input: Box::new(input),
                })
            }
            PromExpr::Binary(PromBinaryExpr {
                lhs,
                rhs,
                op,
                modifier,
            }) if matches!(
                op.id(),
                token::T_ADD
                    | token::T_SUB
                    | token::T_MUL
                    | token::T_DIV
                    | token::T_MOD
                    | token::T_POW
                    | token::T_ATAN2
            ) && modifier.as_ref().is_none_or(|modifier| {
                !modifier.return_bool
                    && modifier.matching.is_none()
                    && matches!(modifier.card, VectorMatchCardinality::OneToOne)
                    && modifier.fill_values.lhs.is_none()
                    && modifier.fill_values.rhs.is_none()
            }) =>
            {
                let lhs = Self::try_new(lhs, env)?;
                let rhs = Self::try_new(rhs, env)?;
                Some(Self::Binary {
                    op: *op,
                    lhs: Box::new(lhs),
                    rhs: Box::new(rhs),
                })
            }
            _ => None,
        }
    }
}

#[derive(Debug, Default)]
struct IslandCollectEnv {
    leaf_by_key: HashMap<VectorLeafKey, usize>,
    leaves: Vec<IslandLeaf>,
    vector_occurrences: usize,
}

#[derive(Debug)]
struct PlannedIslandLeaf {
    plan: LogicalPlan,
    ctx: PromPlannerContext,
    alias: TableReference,
    display_table: String,
}

#[derive(Debug)]
struct IslandFieldExprs {
    exprs: Vec<DfExpr>,
    names: Vec<String>,
    scalar: bool,
}

impl VectorLeafKey {
    fn from_selector(selector: &VectorSelector) -> Option<Self> {
        let mut metric_name = selector.name.clone();
        let mut matchers = Vec::with_capacity(selector.matchers.matchers.len());
        let matcher_key = |matcher: &Matcher| {
            (
                matcher.name.clone(),
                matcher.op.to_string(),
                matcher.value.clone(),
            )
        };

        for matcher in &selector.matchers.matchers {
            if matcher.name == METRIC_NAME {
                if matcher.op != MatchOp::Equal || metric_name.is_some() {
                    return None;
                }
                metric_name = Some(matcher.value.clone());
            } else {
                matchers.push(matcher_key(matcher));
            }
        }
        matchers.sort();

        let mut or_matchers = selector
            .matchers
            .or_matchers
            .iter()
            .map(|group| {
                let mut group = group.iter().map(matcher_key).collect::<Vec<_>>();
                group.sort();
                group
            })
            .collect::<Vec<_>>();
        or_matchers.sort();

        Some(Self {
            metric_name: metric_name?,
            matchers,
            or_matchers,
            offset_ms: match &selector.offset {
                Some(Offset::Pos(duration)) => duration.as_millis() as i128,
                Some(Offset::Neg(duration)) => -(duration.as_millis() as i128),
                None => 0,
            },
            at: format!("{:?}", selector.at),
        })
    }
}

impl IslandCollectEnv {
    fn intern_leaf(&mut self, selector: &VectorSelector) -> Option<usize> {
        self.vector_occurrences += 1;
        let key = VectorLeafKey::from_selector(selector)?;
        if let Some(id) = self.leaf_by_key.get(&key) {
            return Some(*id);
        }

        let id = self.leaves.len();
        self.leaves.push(IslandLeaf {
            selector: selector.clone(),
            display_table: key.metric_name.clone(),
        });
        self.leaf_by_key.insert(key, id);
        Some(id)
    }
}

impl PromPlannerContext {
    fn from_eval_stmt(stmt: &EvalStmt) -> Self {
        Self {
            start: stmt.start.duration_since(UNIX_EPOCH).unwrap().as_millis() as _,
            end: stmt.end.duration_since(UNIX_EPOCH).unwrap().as_millis() as _,
            interval: stmt.interval.as_millis() as _,
            lookback_delta: stmt.lookback_delta.as_millis() as _,
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
            expr, range, step, ..
        } = subquery_expr;

        let current_interval = self.ctx.interval;
        if let Some(step) = step {
            self.ctx.interval = step.as_millis() as _;
        }
        let current_start = self.ctx.start;
        self.ctx.start -= range.as_millis() as i64 - self.ctx.interval;
        let input = self.prom_expr_to_plan(expr, query_engine_state).await?;
        self.ctx.interval = current_interval;
        self.ctx.start = current_start;

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
                series_key_columns,
                time_index_column.clone(),
                sort_plan,
            )),
        });

        let manipulate = RangeManipulate::new(
            self.ctx.start,
            self.ctx.end,
            self.ctx.interval,
            range_ms,
            time_index_column,
            self.ctx.field_columns.clone(),
            divide_plan,
        )
        .context(DataFusionPlanningSnafu)?;

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

        let mut input = self.prom_expr_to_plan(expr, query_engine_state).await?;
        let input_has_tsid = input.schema().fields().iter().any(|field| {
            field.name() == DATA_SCHEMA_TSID_COLUMN_NAME
                && field.data_type() == &ArrowDataType::UInt64
        });

        // `__tsid` based scan projection may prune tag columns. Ensure tags referenced in
        // aggregation modifiers (`by`/`without`) are available before planning group keys.
        let required_group_tags = match modifier {
            None => BTreeSet::new(),
            Some(LabelModifier::Include(labels)) => labels
                .labels
                .iter()
                .filter(|label| !is_metric_engine_internal_column(label.as_str()))
                .cloned()
                .collect(),
            Some(LabelModifier::Exclude(labels)) => {
                let mut all_tags = self.collect_row_key_tag_columns_from_plan(&input)?;
                for label in &labels.labels {
                    let _ = all_tags.remove(label);
                }
                all_tags
            }
        };

        if !required_group_tags.is_empty()
            && required_group_tags
                .iter()
                .any(|tag| Self::find_case_sensitive_column(input.schema(), tag.as_str()).is_none())
        {
            input = self.ensure_tag_columns_available(input, &required_group_tags)?;
            self.refresh_tag_columns_from_schema(input.schema());
        }

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
                    let count_value_exprs = prev_field_exprs.iter().map(|expr| {
                        match expr {
                            DfExpr::Column(column) => DfExpr::Column(column.clone()),
                            _ => DfExpr::Column(Column::from_name(expr.schema_name().to_string())),
                        }
                        .alias(label)
                    });
                    let aggregate_group_exprs = group_exprs
                        .iter()
                        .cloned()
                        .chain(prev_field_exprs.clone())
                        .collect::<Vec<_>>();
                    group_exprs.push(col(label));
                    let project_fields = self
                        .create_field_column_exprs()?
                        .into_iter()
                        .chain(self.create_tag_column_exprs()?)
                        .chain(Some(self.create_time_index_column_expr()?))
                        .chain(count_value_exprs);

                    builder
                        .aggregate(aggregate_group_exprs, aggr_exprs)
                        .context(DataFusionPlanningSnafu)?
                        .project(project_fields)
                        .context(DataFusionPlanningSnafu)?
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
                args: vec![col(&histogram_column)],
            });
            let keep_float = when(col(&histogram_column).is_not_null(), drop_histogram)
                .otherwise(col(&float_column).is_not_null())
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
                    left: Box::new(col(rank)),
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

        let rank_columns: Vec<_> = rank_columns.into_iter().map(col).collect();

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
                    args: vec![DfExpr::Column(col.into())],
                }))
            } else {
                Ok(DfExpr::Negative(Box::new(DfExpr::Column(col.into()))))
            }
        })
    }

    async fn try_plan_binary_island(
        &mut self,
        binary_expr: &PromBinaryExpr,
    ) -> Result<Option<LogicalPlan>> {
        let original_ctx = self.ctx.clone();
        let mut collect_env = IslandCollectEnv::default();
        let Some(island_expr) =
            IslandExpr::try_new(&PromExpr::Binary(binary_expr.clone()), &mut collect_env)
        else {
            return Ok(None);
        };

        if collect_env.leaves.is_empty()
            || collect_env.vector_occurrences <= collect_env.leaves.len()
        {
            return Ok(None);
        }

        let mut planned_leaves = Vec::with_capacity(collect_env.leaves.len());
        for (idx, leaf) in collect_env.leaves.iter().enumerate() {
            let plan = self
                .prom_vector_selector_to_plan(&leaf.selector, false)
                .await?;
            let ctx = self.ctx.clone();
            let alias = TableReference::bare(format!("{BINARY_ISLAND_LEAF_ALIAS_PREFIX}{idx}"));
            let plan = LogicalPlanBuilder::from(plan)
                .alias(alias.clone())
                .context(DataFusionPlanningSnafu)?
                .build()
                .context(DataFusionPlanningSnafu)?;
            planned_leaves.push(PlannedIslandLeaf {
                plan,
                ctx,
                alias,
                display_table: leaf.display_table.clone(),
            });
        }

        if planned_leaves.iter().any(|leaf| {
            Self::field_columns_contain_native_histogram(
                leaf.plan.schema(),
                &leaf.ctx.field_columns,
            )
        }) {
            self.ctx = original_ctx;
            return Ok(None);
        }

        if !Self::binary_island_join_contexts_supported(&planned_leaves) {
            self.ctx = original_ctx;
            return Ok(None);
        }

        let mut input = planned_leaves[0].plan.clone();
        for right_idx in 1..planned_leaves.len() {
            input = self.join_binary_island_leaf(
                input,
                &planned_leaves[0],
                &planned_leaves[right_idx],
            )?;
        }

        let field_exprs =
            Self::build_binary_island_field_exprs(&island_expr, &planned_leaves, input.schema())?;
        if field_exprs.scalar || field_exprs.exprs.is_empty() {
            self.ctx = original_ctx;
            return Ok(None);
        }

        let plan = self.project_binary_island(
            input,
            &planned_leaves[0].alias,
            &planned_leaves[0].ctx,
            field_exprs,
        )?;
        Ok(Some(plan))
    }

    fn binary_island_join_contexts_supported(leaves: &[PlannedIslandLeaf]) -> bool {
        if leaves
            .iter()
            .any(|leaf| leaf.ctx.time_index_column.is_none())
        {
            return false;
        }

        if leaves.len() <= 1 {
            return true;
        }

        let first_tags = leaves[0].ctx.tag_columns.iter().collect::<BTreeSet<_>>();

        leaves.iter().skip(1).all(|leaf| {
            (Self::plan_has_tsid_column(&leaves[0].plan) && Self::plan_has_tsid_column(&leaf.plan))
                || leaf.ctx.tag_columns.iter().collect::<BTreeSet<_>>() == first_tags
        })
    }

    fn join_binary_island_leaf(
        &self,
        left: LogicalPlan,
        first_leaf: &PlannedIslandLeaf,
        right_leaf: &PlannedIslandLeaf,
    ) -> Result<LogicalPlan> {
        let only_join_time_index = (first_leaf.ctx.tag_columns.is_empty()
            || right_leaf.ctx.tag_columns.is_empty())
            && !first_leaf
                .ctx
                .tag_columns
                .iter()
                .chain(&right_leaf.ctx.tag_columns)
                .any(|tag| tag == OTLP_AGGREGATION_TEMPORALITY_LABEL);
        let (mut left_keys, mut right_keys, force_empty_join) = self.binary_join_key_columns(
            left.schema(),
            right_leaf.plan.schema(),
            &first_leaf.ctx,
            &right_leaf.ctx,
            only_join_time_index,
            &None,
        )?;

        if let (Some(left_time_index_column), Some(right_time_index_column)) = (
            first_leaf.ctx.time_index_column.clone(),
            right_leaf.ctx.time_index_column.clone(),
        ) {
            left_keys.insert(left_time_index_column);
            right_keys.insert(right_time_index_column);
        }

        LogicalPlanBuilder::from(left)
            .join_detailed(
                right_leaf.plan.clone(),
                JoinType::Inner,
                (
                    left_keys
                        .into_iter()
                        .map(|name| Column::new(Some(first_leaf.alias.clone()), name))
                        .collect::<Vec<_>>(),
                    right_keys
                        .into_iter()
                        .map(|name| Column::new(Some(right_leaf.alias.clone()), name))
                        .collect::<Vec<_>>(),
                ),
                force_empty_join.then_some(lit(false)),
                NullEquality::NullEqualsNull,
            )
            .context(DataFusionPlanningSnafu)?
            .build()
            .context(DataFusionPlanningSnafu)
    }

    fn build_binary_island_field_exprs(
        expr: &IslandExpr,
        leaves: &[PlannedIslandLeaf],
        schema: &DFSchemaRef,
    ) -> Result<IslandFieldExprs> {
        match expr {
            IslandExpr::VectorLeaf(id) => {
                let leaf = &leaves[*id];
                let exprs = leaf
                    .ctx
                    .field_columns
                    .iter()
                    .map(|field| {
                        schema
                            .qualified_field_with_name(Some(&leaf.alias), field)
                            .context(DataFusionPlanningSnafu)
                            .map(|field| DfExpr::Column(field.into()))
                    })
                    .collect::<Result<Vec<_>>>()?;
                let names = leaf
                    .ctx
                    .field_columns
                    .iter()
                    .map(|field| format!("{}.{}", leaf.display_table, field))
                    .collect();
                Ok(IslandFieldExprs {
                    exprs,
                    names,
                    scalar: false,
                })
            }
            IslandExpr::Scalar(expr) => Ok(IslandFieldExprs {
                exprs: vec![expr.clone()],
                names: vec![expr.schema_name().to_string()],
                scalar: true,
            }),
            IslandExpr::Unary { input } => {
                let input = Self::build_binary_island_field_exprs(input, leaves, schema)?;
                let mut exprs = Vec::with_capacity(input.exprs.len());
                let mut names = Vec::with_capacity(input.names.len());
                for (expr, name) in input.exprs.into_iter().zip(input.names) {
                    exprs.push(DfExpr::Negative(Box::new(expr)));
                    names.push(format!("-{name}"));
                }
                Ok(IslandFieldExprs {
                    exprs,
                    names,
                    scalar: input.scalar,
                })
            }
            IslandExpr::Binary { op, lhs, rhs } => {
                let same_leaf = match (&**lhs, &**rhs) {
                    (IslandExpr::VectorLeaf(left), IslandExpr::VectorLeaf(right))
                        if left == right =>
                    {
                        Some(*left)
                    }
                    _ => None,
                };
                let lhs = Self::build_binary_island_field_exprs(lhs, leaves, schema)?;
                let rhs = Self::build_binary_island_field_exprs(rhs, leaves, schema)?;
                let expr_builder = Self::prom_token_to_binary_expr_builder(*op)?;
                let scalar = lhs.scalar && rhs.scalar;
                let op = op.to_string();

                let (exprs, names) = match (lhs.scalar, rhs.scalar) {
                    (true, true) => {
                        let expr = expr_builder(lhs.exprs[0].clone(), rhs.exprs[0].clone())?;
                        let name = format!("{} {op} {}", lhs.names[0], rhs.names[0]);
                        (vec![expr], vec![name])
                    }
                    (true, false) => {
                        let mut exprs = Vec::with_capacity(rhs.exprs.len());
                        let mut names = Vec::with_capacity(rhs.names.len());
                        for (rhs_expr, rhs_name) in rhs.exprs.into_iter().zip(rhs.names) {
                            exprs.push(expr_builder(lhs.exprs[0].clone(), rhs_expr)?);
                            names.push(format!("{} {op} {rhs_name}", lhs.names[0]));
                        }
                        (exprs, names)
                    }
                    (false, true) => {
                        let mut exprs = Vec::with_capacity(lhs.exprs.len());
                        let mut names = Vec::with_capacity(lhs.names.len());
                        for (lhs_expr, lhs_name) in lhs.exprs.into_iter().zip(lhs.names) {
                            exprs.push(expr_builder(lhs_expr, rhs.exprs[0].clone())?);
                            names.push(format!("{lhs_name} {op} {}", rhs.names[0]));
                        }
                        (exprs, names)
                    }
                    (false, false) => {
                        let mut exprs = Vec::new();
                        let mut names = Vec::new();
                        for (idx, ((lhs_expr, rhs_expr), (mut lhs_name, mut rhs_name))) in lhs
                            .exprs
                            .into_iter()
                            .zip(rhs.exprs)
                            .zip(lhs.names.into_iter().zip(rhs.names))
                            .enumerate()
                        {
                            if let Some(leaf) = same_leaf {
                                let field = leaves[leaf]
                                    .ctx
                                    .field_columns
                                    .get(idx)
                                    .cloned()
                                    .unwrap_or_else(|| lhs_name.clone());
                                lhs_name = format!("lhs.{field}");
                                rhs_name = format!("rhs.{field}");
                            }
                            exprs.push(expr_builder(lhs_expr, rhs_expr)?);
                            names.push(format!("{lhs_name} {op} {rhs_name}"));
                        }
                        (exprs, names)
                    }
                };

                Ok(IslandFieldExprs {
                    exprs,
                    names,
                    scalar,
                })
            }
        }
    }

    fn project_binary_island(
        &mut self,
        input: LogicalPlan,
        base_alias: &TableReference,
        base_ctx: &PromPlannerContext,
        field_exprs: IslandFieldExprs,
    ) -> Result<LogicalPlan> {
        self.ctx = base_ctx.clone();

        let schema = input.schema();
        let non_field_exprs = base_ctx
            .tag_columns
            .iter()
            .chain(base_ctx.time_index_column.iter())
            .map(|column| {
                schema
                    .qualified_field_with_name(Some(base_alias), column)
                    .context(DataFusionPlanningSnafu)
                    .map(|field| DfExpr::Column(field.into()))
            });
        let tsid_expr = Self::optional_tsid_projection(schema, Some(base_alias), base_ctx.use_tsid)
            .into_iter()
            .map(Ok);

        self.ctx.field_columns = field_exprs.names;
        let field_exprs = field_exprs
            .exprs
            .into_iter()
            .zip(self.ctx.field_columns.iter())
            .map(|(expr, name)| Ok(DfExpr::Alias(Alias::new(expr, None::<String>, name))));

        let project_exprs = non_field_exprs
            .chain(tsid_expr)
            .chain(field_exprs)
            .collect::<Result<Vec<_>>>()?;

        let plan = LogicalPlanBuilder::from(input)
            .project(project_exprs)
            .context(DataFusionPlanningSnafu)?
            .build()
            .context(DataFusionPlanningSnafu)?;

        self.ctx.table_name = None;
        self.ctx.schema_name = None;

        Ok(plan)
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
                    field_expr = DfExpr::Cast(Cast {
                        expr: Box::new(field_expr),
                        data_type: ArrowDataType::Float64,
                    });
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
                    let rhs = DfExpr::Column(col.into());
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
                        binary_expr = DfExpr::Cast(Cast {
                            expr: Box::new(binary_expr),
                            data_type: ArrowDataType::Float64,
                        });
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
                    let lhs = DfExpr::Column(col.into());
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
                        binary_expr = DfExpr::Cast(Cast {
                            expr: Box::new(binary_expr),
                            data_type: ArrowDataType::Float64,
                        });
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
                let left_input = self.prom_expr_to_plan(lhs, query_engine_state).await?;
                let left_field_columns = self.ctx.field_columns.clone();
                let left_time_index_column = self.ctx.time_index_column.clone();
                let mut left_table_ref = self
                    .table_ref()
                    .unwrap_or_else(|_| TableReference::bare(""));
                let left_context = self.ctx.clone();

                let right_input = self.prom_expr_to_plan(rhs, query_engine_state).await?;
                let right_field_columns = self.ctx.field_columns.clone();
                let right_time_index_column = self.ctx.time_index_column.clone();
                let mut right_table_ref = self
                    .table_ref()
                    .unwrap_or_else(|_| TableReference::bare(""));
                let right_context = self.ctx.clone();
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

                let join_plan = self.join_on_non_field_columns(
                    left_input,
                    right_input,
                    left_table_ref.clone(),
                    right_table_ref.clone(),
                    left_time_index_column,
                    right_time_index_column,
                    lhs.value_type() == ValueType::Scalar
                        || rhs.value_type() == ValueType::Scalar
                        || has_empty_metric_operand
                        || ((left_context.tag_columns.is_empty()
                            || right_context.tag_columns.is_empty())
                            && !left_context
                                .tag_columns
                                .iter()
                                .chain(&right_context.tag_columns)
                                .any(|tag| tag == OTLP_AGGREGATION_TEMPORALITY_LABEL)),
                    modifier,
                    &left_context,
                    &right_context,
                )?;
                let join_plan_schema = join_plan.schema().clone();
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
                                binary_expr = DfExpr::Cast(Cast {
                                    expr: Box::new(binary_expr),
                                    data_type: ArrowDataType::Float64,
                                });
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
                    self.project_binary_join_side(filtered, project_table_ref, &project_context)
                } else {
                    let projected =
                        self.projection_for_each_field_column(join_plan, bin_expr_builder)?;
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

        // Project tag columns from the chosen side.
        for tag_column in &context.tag_columns {
            let tag_col = schema
                .qualified_field_with_name(Some(table_ref), tag_column)
                .context(DataFusionPlanningSnafu)?
                .into();
            project_exprs.push(DfExpr::Column(tag_col));
        }

        // Preserve `__tsid` if present, so it can still be used internally downstream. It's
        // stripped from the final output anyway.
        if let Some(tsid_col) =
            Self::optional_tsid_projection(schema, Some(table_ref), context.use_tsid)
        {
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

    async fn prom_vector_selector_to_plan(
        &mut self,
        vector_selector: &VectorSelector,
        timestamp_fn: bool,
    ) -> Result<LogicalPlan> {
        let VectorSelector {
            name,
            offset,
            matchers,
            at: _,
        } = vector_selector;
        let matchers = self.preprocess_label_matchers(matchers, name)?;
        if let Some(empty_plan) = self.setup_context().await? {
            return Ok(empty_plan);
        }
        let normalize = self
            .selector_to_series_normalize_plan(offset, matchers, false)
            .await?;
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
            project_exprs
                .push(build_special_time_expr(&time_index_column).alias(&timestamp_value_column));
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
        let manipulate = InstantManipulate::new(
            self.ctx.start,
            self.ctx.end,
            self.ctx.lookback_delta,
            self.ctx.interval,
            time_index_column,
            if self.ctx.use_tsid {
                vec![DATA_SCHEMA_TSID_COLUMN_NAME.to_string()]
            } else {
                self.ctx.tag_columns.clone()
            },
            field_column,
            normalize,
        );
        let manipulate = LogicalPlan::Extension(Extension {
            node: Arc::new(manipulate),
        });
        if let Some(timestamp_value_column) = timestamp_value_column {
            self.create_timestamp_func_plan(manipulate, &timestamp_value_column)
        } else {
            Ok(manipulate)
        }
    }

    /// Builds a projection plan for the PromQL `timestamp()` function.
    /// Projects the time index column as the value column for each row.
    ///
    /// # Arguments
    /// * `input` - Input [`LogicalPlan`] after instant-vector selection.
    /// * `timestamp_value_column` - Private column containing each selected sample's timestamp.
    ///
    /// # Returns
    /// Returns a [`Result<LogicalPlan>`] where the resulting logical plan projects the timestamp
    /// column as the value column, along with the original tag and time index columns.
    ///
    /// # Timestamp vs. Time Function
    ///
    /// - **Timestamp Function (`timestamp()`)**: In PromQL, the `timestamp()` function returns the
    ///   timestamp (time index) of each sample as the value column.
    ///
    /// - **Time Function (`time()`)**: The `time()` function returns the evaluation time of the query
    ///   as a scalar value.
    ///
    /// # Side Effects
    /// Updates the planner context's field columns to the timestamp column name.
    ///
    fn create_timestamp_func_plan(
        &mut self,
        input: LogicalPlan,
        timestamp_value_column: &str,
    ) -> Result<LogicalPlan> {
        let time_expr = col(timestamp_value_column).alias(DEFAULT_FIELD_COLUMN);
        self.ctx.field_columns = vec![time_expr.schema_name().to_string()];
        let mut project_exprs = Vec::with_capacity(self.ctx.tag_columns.len() + 2);
        project_exprs.push(self.create_time_index_column_expr()?);
        project_exprs.push(time_expr);
        project_exprs.extend(self.create_tag_column_exprs()?);

        LogicalPlanBuilder::from(input)
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
            ..
        } = vs;
        let matchers = self.preprocess_label_matchers(matchers, name)?;
        ensure!(!range.is_zero(), ZeroRangeSelectorSnafu);
        let range_ms = range.as_millis() as _;
        self.ctx.range = Some(range_ms);

        // Some functions like rate may require special fields in the RangeManipulate plan
        // so we can't skip RangeManipulate.
        let normalize = match self.setup_context().await? {
            Some(empty_plan) => empty_plan,
            None => {
                self.selector_to_series_normalize_plan(offset, matchers, true)
                    .await?
            }
        };
        let manipulate = RangeManipulate::new(
            self.ctx.start,
            self.ctx.end,
            self.ctx.interval,
            // TODO(ruihang): convert via Timestamp datatypes to support different time units
            range_ms,
            self.ctx
                .time_index_column
                .clone()
                .expect("time index should be set in `setup_context`"),
            self.ctx.field_columns.clone(),
            normalize,
        )
        .context(DataFusionPlanningSnafu)?;

        Ok(LogicalPlan::Extension(Extension {
            node: Arc::new(manipulate),
        }))
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
        let input = if let Some(prom_expr) = &args.input {
            self.prom_expr_to_plan_inner(prom_expr, func.name == "timestamp", query_engine_state)
                .await?
        } else {
            self.ctx.time_index_column = Some(SPECIAL_TIME_FUNCTION.to_string());
            self.ctx.reset_table_name_and_schema();
            self.ctx.tag_columns = vec![];
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
        let preserve_any_value =
            Self::field_columns_are_alternative_samples(input.schema(), &self.ctx.field_columns);
        let (mut func_exprs, new_tags) = self.create_function_expr(
            func,
            args.literals.clone(),
            input.schema(),
            query_engine_state,
        )?;
        func_exprs.insert(0, self.create_time_index_column_expr()?);
        func_exprs.extend_from_slice(&self.create_tag_column_exprs()?);
        if let Some(tsid_col) =
            Self::optional_tsid_projection(input.schema(), None, self.ctx.use_tsid)
        {
            func_exprs.push(tsid_col);
        }

        let builder = LogicalPlanBuilder::from(input)
            .project(func_exprs)
            .context(DataFusionPlanningSnafu)?
            .filter(self.create_empty_values_filter_expr(preserve_any_value)?)
            .context(DataFusionPlanningSnafu)?;

        let builder = match func.name {
            "sort" => builder
                .sort(self.create_field_columns_sort_exprs(true))
                .context(DataFusionPlanningSnafu)?,
            "sort_desc" => builder
                .sort(self.create_field_columns_sort_exprs(false))
                .context(DataFusionPlanningSnafu)?,
            "sort_by_label" => builder
                .sort(Self::create_sort_exprs_by_tags(
                    func.name,
                    args.literals,
                    true,
                )?)
                .context(DataFusionPlanningSnafu)?,
            "sort_by_label_desc" => builder
                .sort(Self::create_sort_exprs_by_tags(
                    func.name,
                    args.literals,
                    false,
                )?)
                .context(DataFusionPlanningSnafu)?,

            _ => builder,
        };

        // Update context tags after building plan
        // We can't push them before planning, because they won't exist until projection.
        for tag in new_tags {
            self.ctx.tag_columns.push(tag);
        }

        let plan = builder.build().context(DataFusionPlanningSnafu)?;
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

        let mut matchers = HashSet::new();
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
                let _ = matchers.insert(matcher.clone());
            }
        }

        Ok(Matchers::new(matchers.into_iter().collect()))
    }

    async fn selector_to_series_normalize_plan(
        &mut self,
        offset: &Option<Offset>,
        label_matchers: Matchers,
        is_range_selector: bool,
    ) -> Result<LogicalPlan> {
        // make table scan plan
        let table_ref = self.table_ref()?;
        let mut table_scan = self.create_table_scan_plan(table_ref.clone()).await?;
        let table_schema = table_scan.schema();

        // make filter exprs
        let offset_duration = match offset {
            Some(Offset::Pos(duration)) => duration.as_millis() as Millisecond,
            Some(Offset::Neg(duration)) => -(duration.as_millis() as Millisecond),
            None => 0,
        };
        let mut scan_filters = Self::matchers_to_expr(label_matchers.clone(), table_schema)?;
        if let Some(time_index_filter) = self.build_time_index_filter(offset_duration)? {
            scan_filters.push(time_index_filter);
        }
        table_scan = LogicalPlanBuilder::from(table_scan)
            .filter(conjunction(scan_filters).unwrap()) // Safety: `scan_filters` is not empty.
            .context(DataFusionPlanningSnafu)?
            .build()
            .context(DataFusionPlanningSnafu)?;

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
        let series_key_columns = if self.ctx.use_tsid {
            vec![DATA_SCHEMA_TSID_COLUMN_NAME.to_string()]
        } else {
            self.ctx.tag_columns.clone()
        };

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
                }
                Ok(vec![self.create_time_index_column_expr()?])
            }
            Some(LabelModifier::Include(labels)) => {
                if update_ctx {
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
                }

                // collect remaining fields and convert to col expr
                let mut exprs = all_fields
                    .into_iter()
                    .map(|c| DfExpr::Column(Column::from(c)))
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
            let col = if let Some(column_name) = column_name {
                let column = DfExpr::Column(Column::from_name(&column_name));
                let field = table_schema
                    .index_of_column_by_name(None, &column_name)
                    .map(|index| table_schema.field(index));
                if accepts_empty
                    && column_name == OTLP_AGGREGATION_TEMPORALITY_LABEL
                    && let Some(data_type) = field
                        .filter(|field| {
                            field.is_nullable()
                                && Self::string_value_data_type(field.data_type()).is_some()
                        })
                        .map(|field| field.data_type())
                {
                    let empty = Self::string_scalar_value(data_type, Some(String::new()))
                        .expect("nullable label has a string type");
                    DfExpr::ScalarFunction(ScalarFunction {
                        func: coalesce(),
                        args: vec![column, DfExpr::Literal(empty, None)],
                    })
                } else {
                    column
                }
            } else {
                DfExpr::Literal(ScalarValue::Utf8(Some(String::new())), None)
                    .alias(matcher.name.clone())
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
            .as_any()
            .downcast_ref::<DefaultTableSource>()
            .context(UnknownTableSnafu)?
            .table_provider
            .as_any()
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

    fn build_time_index_filter(&self, offset_duration: i64) -> Result<Option<DfExpr>> {
        let start = self.ctx.start;
        let end = self.ctx.end;
        if end < start {
            return InvalidTimeRangeSnafu { start, end }.fail();
        }
        let lookback_delta = self.ctx.lookback_delta;
        let range = self.ctx.range.unwrap_or_default();
        let interval = self.ctx.interval;
        let time_index_expr = self.create_time_index_column_expr()?;
        let num_points = (end - start) / interval;

        // Prometheus semantics:
        // - Instant selector lookback: (eval_ts - lookback_delta, eval_ts]
        // - Range selector:           (eval_ts - range, eval_ts]
        //
        // So samples positioned exactly at the lower boundary must be excluded. We align the scan
        // lower bound with Prometheus by shifting it forward by 1ms (millisecond granularity),
        // while still using a `>=` filter.
        let selector_window = if range == 0 { lookback_delta } else { range };
        let lower_exclusive_adjustment = if selector_window > 0 { 1 } else { 0 };

        // Scan a continuous time range
        if (end - start) / interval > MAX_SCATTER_POINTS || interval <= INTERVAL_1H {
            let single_time_range = time_index_expr
                .clone()
                .gt_eq(DfExpr::Literal(
                    ScalarValue::TimestampMillisecond(
                        Some(
                            self.ctx.start - offset_duration - selector_window
                                + lower_exclusive_adjustment,
                        ),
                        None,
                    ),
                    None,
                ))
                .and(time_index_expr.lt_eq(DfExpr::Literal(
                    ScalarValue::TimestampMillisecond(Some(self.ctx.end - offset_duration), None),
                    None,
                )));
            return Ok(Some(single_time_range));
        }

        // Otherwise scan scatter ranges separately
        let mut filters = Vec::with_capacity(num_points as usize + 1);
        for timestamp in (start..=end).step_by(interval as usize) {
            filters.push(
                time_index_expr
                    .clone()
                    .gt_eq(DfExpr::Literal(
                        ScalarValue::TimestampMillisecond(
                            Some(
                                timestamp - offset_duration - selector_window
                                    + lower_exclusive_adjustment,
                            ),
                            None,
                        ),
                        None,
                    ))
                    .and(time_index_expr.clone().lt_eq(DfExpr::Literal(
                        ScalarValue::TimestampMillisecond(Some(timestamp - offset_duration), None),
                        None,
                    ))),
            )
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

        let is_time_index_ms = scan_table
            .schema()
            .timestamp_column()
            .with_context(|| TimeIndexNotFoundSnafu {
                table: maybe_phy_table_ref.to_quoted_string(),
            })?
            .data_type
            == ConcreteDataType::timestamp_millisecond_datatype();

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

        if !is_time_index_ms {
            // cast to ms if time_index not in Millisecond precision
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
                    expr: Box::new(DfExpr::Cast(Cast {
                        expr: Box::new(self.create_time_index_column_expr()?),
                        data_type: ArrowDataType::Timestamp(ArrowTimeUnit::Millisecond, None),
                    })),
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
        } else if table_id_filter.is_some() {
            // Drop the internal `__table_id` column after filtering.
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

    fn ensure_tag_columns_available(
        &self,
        plan: LogicalPlan,
        required_tags: &BTreeSet<String>,
    ) -> Result<LogicalPlan> {
        if required_tags.is_empty() {
            return Ok(plan);
        }

        struct Rewriter {
            required_tags: BTreeSet<String>,
        }

        impl TreeNodeRewriter for Rewriter {
            type Node = LogicalPlan;

            fn f_up(
                &mut self,
                node: Self::Node,
            ) -> datafusion_common::Result<Transformed<Self::Node>> {
                match node {
                    LogicalPlan::TableScan(scan) => {
                        let schema = scan.source.schema();
                        let mut projection = match scan.projection.clone() {
                            Some(p) => p,
                            None => {
                                // Scanning all columns already covers required tags.
                                return Ok(Transformed::no(LogicalPlan::TableScan(scan)));
                            }
                        };

                        let mut changed = false;
                        for tag in &self.required_tags {
                            if let Some((idx, _)) = schema
                                .fields()
                                .iter()
                                .enumerate()
                                .find(|(_, field)| field.name() == tag)
                                && !projection.contains(&idx)
                            {
                                projection.push(idx);
                                changed = true;
                            }
                        }

                        if !changed {
                            return Ok(Transformed::no(LogicalPlan::TableScan(scan)));
                        }

                        projection.sort_unstable();
                        projection.dedup();

                        let new_scan = TableScan::try_new(
                            scan.table_name.clone(),
                            scan.source.clone(),
                            Some(projection),
                            scan.filters,
                            scan.fetch,
                        )?;
                        Ok(Transformed::yes(LogicalPlan::TableScan(new_scan)))
                    }
                    LogicalPlan::Projection(proj) => {
                        let input_schema = proj.input.schema();

                        let existing = proj
                            .schema
                            .fields()
                            .iter()
                            .map(|f| f.name().as_str())
                            .collect::<HashSet<_>>();

                        let mut expr = proj.expr.clone();
                        let mut has_changed = false;
                        for tag in &self.required_tags {
                            if existing.contains(tag.as_str()) {
                                continue;
                            }

                            if let Some(idx) = input_schema.index_of_column_by_name(None, tag) {
                                expr.push(DfExpr::Column(Column::from(
                                    input_schema.qualified_field(idx),
                                )));
                                has_changed = true;
                            }
                        }

                        if !has_changed {
                            return Ok(Transformed::no(LogicalPlan::Projection(proj)));
                        }

                        let new_proj = Projection::try_new(expr, proj.input)?;
                        Ok(Transformed::yes(LogicalPlan::Projection(new_proj)))
                    }
                    other => Ok(Transformed::no(other)),
                }
            }
        }

        let mut rewriter = Rewriter {
            required_tags: required_tags.clone(),
        };
        let rewritten = plan
            .rewrite(&mut rewriter)
            .context(DataFusionPlanningSnafu)?;
        Ok(rewritten.data)
    }

    fn refresh_tag_columns_from_schema(&mut self, schema: &DFSchemaRef) {
        let time_index = self.ctx.time_index_column.as_deref();
        let field_columns = self.ctx.field_columns.iter().collect::<HashSet<_>>();

        let mut tags = schema
            .fields()
            .iter()
            .map(|f| f.name())
            .filter(|name| Some(name.as_str()) != time_index)
            .filter(|name| !field_columns.contains(name))
            .filter(|name| !is_metric_engine_internal_column(name))
            .cloned()
            .collect::<Vec<_>>();
        tags.sort_unstable();
        tags.dedup();
        self.ctx.tag_columns = tags;
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

        self.ctx.use_tsid = false;

        Ok(None)
    }

    /// Setup [PromPlannerContext]'s state fields for a non existent table
    /// without any rows.
    fn setup_context_for_empty_metric(&mut self) -> Result<LogicalPlan> {
        self.ctx.time_index_column = Some(SPECIAL_TIME_FUNCTION.to_string());
        self.ctx.reset_table_name_and_schema();
        self.ctx.tag_columns = vec![];
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
                result.literals.push(expr);
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
                        let expr = Self::get_param_as_literal_expr(Some(arg.as_ref()), None, None)?;
                        result.literals.push(expr);
                    }
                }
            }
        }

        Ok(result)
    }

    fn create_mixed_range_function_exprs(
        &mut self,
        func: &Function,
        mut other_input_exprs: VecDeque<DfExpr>,
        float_field: &str,
        histogram_field: &str,
        input_schema: &DFSchemaRef,
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
            other_input_exprs[0] = DfExpr::Cast(Cast {
                expr: Box::new(other_input_exprs[0].clone()),
                data_type: ArrowDataType::Int64,
            });
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
            let display_name = float_expr.schema_name().to_string();
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
                    other_input_exprs[0] = DfExpr::Cast(Cast {
                        expr: Box::new(other_input_exprs[0].clone()),
                        data_type: ArrowDataType::Int64,
                    });
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
                if let Some((replace_expr, dst_label)) = self
                    .build_regexp_replace_label_expr(&mut other_input_exprs, query_engine_state)?
                {
                    // Reserve the current field columns except the `dst_label`.
                    for value in &self.ctx.field_columns {
                        if *value != dst_label {
                            let expr = DfExpr::Column(Column::from_name(value));
                            exprs.push(expr);
                        }
                    }

                    ensure!(
                        !self.ctx.tag_columns.contains(&dst_label),
                        SameLabelSetSnafu
                    );
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
                    let display_name = expr.schema_name().to_string();
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
        query_engine_state: &QueryEngineState,
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

        // Validate the regex before using it
        // doc: https://prometheus.io/docs/prometheus/latest/querying/functions/#label_replace
        regex::Regex::new(&regex).map_err(|_| {
            InvalidRegularExpressionSnafu {
                regex: regex.clone(),
            }
            .build()
        })?;

        // If the src_label exists and regex is empty, keep everything unchanged.
        if self.ctx.tag_columns.contains(&src_label) && regex.is_empty() {
            return Ok(None);
        }

        // If the src_label doesn't exists, and
        if !self.ctx.tag_columns.contains(&src_label) {
            if replacement.is_empty() {
                // the replacement is empty, keep everything unchanged.
                return Ok(None);
            } else {
                // the replacement is not empty, always adds dst_label with replacement value.
                return Ok(Some((
                    // alias literal `replacement` as dst_label
                    lit(replacement).alias(&dst_label),
                    dst_label,
                )));
            }
        }

        // Preprocess the regex:
        // https://github.com/prometheus/prometheus/blob/d902abc50d6652ba8fe9a81ff8e5cce936114eba/promql/functions.go#L1575C32-L1575C37
        let regex = format!("^(?s:{regex})$");

        let session_state = query_engine_state.session_state();
        let func = session_state
            .scalar_functions()
            .get("regexp_replace")
            .context(UnsupportedExprSnafu {
                name: "regexp_replace",
            })?;

        // regexp_replace(src_label, regex, replacement)
        let args = vec![
            if src_label.is_empty() {
                DfExpr::Literal(ScalarValue::Utf8(Some(String::new())), None)
            } else {
                DfExpr::Column(Column::from_name(src_label))
            },
            DfExpr::Literal(ScalarValue::Utf8(Some(regex)), None),
            DfExpr::Literal(ScalarValue::Utf8(Some(replacement)), None),
        ];

        Ok(Some((
            DfExpr::ScalarFunction(ScalarFunction {
                func: func.clone(),
                args,
            })
            .alias(&dst_label),
            dst_label,
        )))
    }

    /// Build expr for `label_join` function
    fn build_concat_labels_expr(
        other_input_exprs: &mut VecDeque<DfExpr>,
        ctx: &PromPlannerContext,
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
                // Cast source label into column or null literal
                match expr {
                    DfExpr::Literal(ScalarValue::Utf8(Some(label)), None) => {
                        if label.is_empty() {
                            Ok(DfExpr::Literal(ScalarValue::Null, None))
                        } else if available_columns.contains(label.as_str()) {
                            // Label exists in the table schema
                            Ok(DfExpr::Column(Column::from_name(label)))
                        } else {
                            // Label doesn't exist, treat as empty string (null)
                            Ok(DfExpr::Literal(ScalarValue::Null, None))
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
        ensure!(
            !src_labels.is_empty(),
            FunctionInvalidArgumentSnafu {
                fn_name: "label_join"
            }
        );

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
            DfExpr::ScalarFunction(ScalarFunction {
                func: func.clone(),
                args,
            })
            .alias(&dst_label),
            dst_label,
        ))
    }

    fn create_time_index_column_expr(&self) -> Result<DfExpr> {
        Ok(DfExpr::Column(Column::from_name(
            self.ctx
                .time_index_column
                .clone()
                .with_context(|| TimeIndexNotFoundSnafu { table: "unknown" })?,
        )))
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
    /// - `previous_field_expressions`: Original field expressions before aggregation. This is non-empty
    ///   only when the operation is `count_values`, as this operation requires preserving the original
    ///   values for grouping.
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
        let prev_field_exprs = if op.id() == token::T_COUNT_VALUES {
            let prev_field_exprs: Vec<_> = self
                .ctx
                .field_columns
                .iter()
                .map(|col| DfExpr::Column(Column::from_name(col)))
                .collect();

            ensure!(
                self.ctx.field_columns.len() == 1,
                UnsupportedExprSnafu {
                    name: "count_values on multi-value input"
                }
            );

            prev_field_exprs
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

    fn get_param_as_literal_expr(
        param: Option<&PromExpr>,
        op: Option<TokenType>,
        expected_type: Option<ArrowDataType>,
    ) -> Result<DfExpr> {
        let prom_param = param.with_context(|| {
            if let Some(op) = op {
                FunctionInvalidArgumentSnafu {
                    fn_name: op.to_string(),
                }
            } else {
                FunctionInvalidArgumentSnafu {
                    fn_name: "unknown".to_string(),
                }
            }
        })?;

        let expr = Self::try_build_literal_expr(prom_param).with_context(|| {
            if let Some(op) = op {
                FunctionInvalidArgumentSnafu {
                    fn_name: op.to_string(),
                }
            } else {
                FunctionInvalidArgumentSnafu {
                    fn_name: "unknown".to_string(),
                }
            }
        })?;

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

        Ok(expr)
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
                sort_exprs.push(DfExpr::Column(Column::from(col)).sort(asc, true));
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

    /// Create a classic, native, or mixed histogram helper plan.
    async fn create_histogram_plan(
        &mut self,
        function_name: &str,
        args: &PromFunctionArgs,
        query_engine_state: &QueryEngineState,
    ) -> Result<LogicalPlan> {
        let float_literal = |param: &PromExpr| -> Result<f64> {
            let value = (|| {
                let expr = Self::get_param_as_literal_expr(
                    Some(param),
                    None,
                    Some(ArrowDataType::Float64),
                )
                .ok()?;
                let simplifier = ExprSimplifier::new(SimplifyContext::default());
                let expr = simplifier.coerce(expr, &DFSchema::empty()).ok()?;
                let DfExpr::Literal(value, _) = simplifier.simplify(expr).ok()? else {
                    return None;
                };
                let ScalarValue::Float64(Some(value)) =
                    value.cast_to(&ArrowDataType::Float64).ok()?
                else {
                    return None;
                };
                Some(value)
            })()
            .with_context(|| FunctionInvalidArgumentSnafu {
                fn_name: function_name.to_string(),
            })?;
            Ok(value)
        };
        let (function, input) = match (function_name, args.args.as_slice()) {
            (SPECIAL_HISTOGRAM_QUANTILE, [quantile, input]) => (
                HistogramFoldOperation::Quantile(float_literal(quantile)?.into()),
                input.as_ref().clone(),
            ),
            (SPECIAL_HISTOGRAM_FRACTION, [lower, upper, input]) => (
                HistogramFoldOperation::Fraction {
                    lower: float_literal(lower)?.into(),
                    upper: float_literal(upper)?.into(),
                },
                input.as_ref().clone(),
            ),
            _ => {
                return FunctionInvalidArgumentSnafu {
                    fn_name: function_name.to_string(),
                }
                .fail();
            }
        };

        let input_plan = self.prom_expr_to_plan(&input, query_engine_state).await?;
        // Histogram helpers fold buckets across `le`, so `__tsid` (which includes `le`) is not a
        // stable series identifier anymore. HistogramFold must not treat it as a label column.
        let input_plan = self.strip_tsid_column(input_plan)?;
        self.ctx.use_tsid = false;

        if let Some((float_field, histogram_field)) =
            Self::alternative_sample_columns(input_plan.schema(), &self.ctx.field_columns)
                .map(|(float, histogram)| (float.to_string(), histogram.to_string()))
        {
            if self.ctx.has_le_tag() {
                return self.create_mixed_histogram_plan(
                    function,
                    input_plan,
                    float_field,
                    histogram_field,
                );
            }
            self.ctx.field_columns = vec![histogram_field];
        }
        if self.all_field_columns_are_native_histograms(input_plan.schema()) {
            return self.create_native_histogram_plan(function, input_plan);
        }

        if !self.ctx.has_le_tag() {
            // Return empty result instead of error when 'le' column is not found
            // This handles the case when histogram metrics don't exist
            return Ok(LogicalPlan::EmptyRelation(
                datafusion::logical_expr::EmptyRelation {
                    produce_one_row: false,
                    schema: input_plan.schema().clone(),
                },
            ));
        }
        let time_index_column =
            self.ctx
                .time_index_column
                .clone()
                .with_context(|| TimeIndexNotFoundSnafu {
                    table: self.ctx.table_name.clone().unwrap_or_default(),
                })?;
        // FIXME(ruihang): support multi fields
        let field_column = self
            .ctx
            .field_columns
            .first()
            .with_context(|| FunctionInvalidArgumentSnafu {
                fn_name: function.function_name().to_string(),
            })?
            .clone();
        // remove le column from tag columns
        self.ctx.tag_columns.retain(|col| col != LE_COLUMN_NAME);

        let fold = HistogramFold::new_with_operation(
            LE_COLUMN_NAME.to_string(),
            field_column,
            time_index_column,
            function,
            None,
            input_plan,
        )
        .context(DataFusionPlanningSnafu)?;
        Ok(LogicalPlan::Extension(Extension {
            node: Arc::new(fold),
        }))
    }

    fn create_native_histogram_expr(
        &self,
        function: HistogramFoldOperation,
        field_column: &str,
    ) -> DfExpr {
        let field = DfExpr::Column(Column::from_name(field_column));
        let (func, args) = match function {
            HistogramFoldOperation::Quantile(quantile) => (
                Arc::new(NativeHistogramQuantile::scalar_udf_with_collector(
                    self.promql_annotations.clone(),
                )),
                vec![field, lit(f64::from(quantile))],
            ),
            HistogramFoldOperation::Fraction { lower, upper } => (
                Arc::new(NativeHistogramFraction::scalar_udf_with_collector(
                    self.promql_annotations.clone(),
                )),
                vec![field, lit(f64::from(lower)), lit(f64::from(upper))],
            ),
        };
        DfExpr::ScalarFunction(ScalarFunction { func, args })
    }

    fn create_native_histogram_plan(
        &mut self,
        function: HistogramFoldOperation,
        input_plan: LogicalPlan,
    ) -> Result<LogicalPlan> {
        ensure!(
            self.ctx.field_columns.len() == 1,
            MultiFieldsNotSupportedSnafu {
                operator: function.function_name()
            },
        );

        let field_column = self.ctx.field_columns[0].clone();
        let function_expr = self.create_native_histogram_expr(function, &field_column);
        let display_name = function_expr.schema_name().to_string();
        self.ctx.field_columns = vec![display_name.clone()];

        let project_exprs = std::iter::once(self.create_time_index_column_expr()?)
            .chain(std::iter::once(function_expr.alias(display_name)))
            .chain(self.create_tag_column_exprs()?)
            .collect::<Vec<_>>();

        LogicalPlanBuilder::from(input_plan)
            .project(project_exprs)
            .context(DataFusionPlanningSnafu)?
            .filter(self.create_empty_values_filter_expr(false)?)
            .context(DataFusionPlanningSnafu)?
            .build()
            .context(DataFusionPlanningSnafu)
    }

    fn create_mixed_histogram_plan(
        &mut self,
        function: HistogramFoldOperation,
        input_plan: LogicalPlan,
        float_field: String,
        histogram_field: String,
    ) -> Result<LogicalPlan> {
        let time_index_column =
            self.ctx
                .time_index_column
                .clone()
                .with_context(|| TimeIndexNotFoundSnafu {
                    table: self.ctx.table_name.clone().unwrap_or_default(),
                })?;
        let tag_columns = self.ctx.tag_columns.clone();
        let folded = HistogramFold::new_with_operation(
            LE_COLUMN_NAME.to_string(),
            float_field.clone(),
            time_index_column.clone(),
            function,
            Some(histogram_field.clone()),
            input_plan,
        )
        .context(DataFusionPlanningSnafu)?;
        let record_collision = DfExpr::ScalarFunction(ScalarFunction {
            func: Arc::new(NativeHistogramDrop::warning_bool_false_udf(
                "vector contains a mix of classic and native histograms".to_string(),
                self.promql_annotations.clone(),
            )),
            args: vec![col(&float_field), col(&histogram_field)],
        });
        let keep = when(
            col(&float_field)
                .is_not_null()
                .and(col(&histogram_field).is_not_null()),
            record_collision,
        )
        .otherwise(lit(true))
        .context(DataFusionPlanningSnafu)?;

        let native_expr = self.create_native_histogram_expr(function, &histogram_field);
        let output_field = native_expr.schema_name().to_string();
        let value = DfExpr::ScalarFunction(ScalarFunction {
            func: coalesce(),
            args: vec![col(&float_field), native_expr],
        });
        self.ctx.field_columns = vec![output_field.clone()];
        LogicalPlanBuilder::from(LogicalPlan::Extension(Extension {
            node: Arc::new(folded),
        }))
        .filter(keep)
        .context(DataFusionPlanningSnafu)?
        .project(
            std::iter::once(col(&time_index_column))
                .chain(std::iter::once(value.alias(output_field)))
                .chain(tag_columns.iter().map(col)),
        )
        .context(DataFusionPlanningSnafu)?
        .build()
        .context(DataFusionPlanningSnafu)
    }

    /// Create a [SPECIAL_VECTOR_FUNCTION] plan
    async fn create_vector_plan(&mut self, args: &PromFunctionArgs) -> Result<LogicalPlan> {
        if args.args.len() != 1 {
            return FunctionInvalidArgumentSnafu {
                fn_name: SPECIAL_VECTOR_FUNCTION.to_string(),
            }
            .fail();
        }
        let lit = Self::get_param_as_literal_expr(Some(args.args[0].as_ref()), None, None)?;

        // reuse `SPECIAL_TIME_FUNCTION` as name of time index column
        self.ctx.time_index_column = Some(SPECIAL_TIME_FUNCTION.to_string());
        self.ctx.reset_table_name_and_schema();
        self.ctx.tag_columns = vec![];
        self.ctx.field_columns = vec![greptime_value().to_string()];
        Ok(LogicalPlan::Extension(Extension {
            node: Arc::new(
                EmptyMetric::new(
                    self.ctx.start,
                    self.ctx.end,
                    self.ctx.interval,
                    SPECIAL_TIME_FUNCTION.to_string(),
                    greptime_value().to_string(),
                    Some(lit),
                )
                .context(DataFusionPlanningSnafu)?,
            ),
        }))
    }

    /// Create a [SCALAR_FUNCTION] plan
    async fn create_scalar_plan(
        &mut self,
        args: &PromFunctionArgs,
        query_engine_state: &QueryEngineState,
    ) -> Result<LogicalPlan> {
        ensure!(
            args.len() == 1,
            FunctionInvalidArgumentSnafu {
                fn_name: SCALAR_FUNCTION
            }
        );
        let input = self
            .prom_expr_to_plan(&args.args[0], query_engine_state)
            .await?;
        let input_schema = input.schema().clone();
        let alternative_samples =
            Self::field_columns_are_alternative_samples(&input_schema, &self.ctx.field_columns);
        let histogram_fields = self
            .ctx
            .field_columns
            .iter()
            .filter(|field| Self::field_column_is_native_histogram(&input_schema, field))
            .count();
        ensure!(
            self.ctx.field_columns.len() == 1 || alternative_samples,
            MultiFieldsNotSupportedSnafu {
                operator: SCALAR_FUNCTION
            },
        );
        let scalar_field = self
            .ctx
            .field_columns
            .iter()
            .find(|field| !Self::field_column_is_native_histogram(&input_schema, field))
            .or_else(|| self.ctx.field_columns.first())
            .cloned()
            .with_context(|| FunctionInvalidArgumentSnafu {
                fn_name: SCALAR_FUNCTION,
            })?;
        let input = if histogram_fields == self.ctx.field_columns.len() {
            // scalar() ignores histogram samples. An empty input makes ScalarCalculate emit NaN
            // for every evaluation timestamp without attempting a Struct-to-Float64 cast.
            LogicalPlanBuilder::from(input)
                .filter(lit(false))
                .context(DataFusionPlanningSnafu)?
                .build()
                .context(DataFusionPlanningSnafu)?
        } else if histogram_fields > 0 {
            // A mixed vector contributes only its float samples to scalar().
            LogicalPlanBuilder::from(input)
                .filter(DfExpr::Column(Column::from_name(&scalar_field)).is_not_null())
                .context(DataFusionPlanningSnafu)?
                .build()
                .context(DataFusionPlanningSnafu)?
        } else {
            input
        };
        let scalar_plan = LogicalPlan::Extension(Extension {
            node: Arc::new(
                ScalarCalculate::new(
                    self.ctx.start,
                    self.ctx.end,
                    self.ctx.interval,
                    input,
                    self.ctx.time_index_column.as_ref().unwrap(),
                    &self.ctx.tag_columns,
                    &scalar_field,
                    self.ctx.table_name.as_deref(),
                )
                .context(PromqlPlanNodeSnafu)?,
            ),
        });
        // scalar plan have no tag columns
        self.ctx.tag_columns.clear();
        self.ctx.field_columns.clear();
        self.ctx
            .field_columns
            .push(scalar_plan.schema().field(1).name().clone());
        Ok(scalar_plan)
    }

    /// Create a [SPECIAL_ABSENT_FUNCTION] plan
    async fn create_absent_plan(
        &mut self,
        args: &PromFunctionArgs,
        query_engine_state: &QueryEngineState,
    ) -> Result<LogicalPlan> {
        if args.args.len() != 1 {
            return FunctionInvalidArgumentSnafu {
                fn_name: SPECIAL_ABSENT_FUNCTION.to_string(),
            }
            .fail();
        }
        let input = self
            .prom_expr_to_plan(&args.args[0], query_engine_state)
            .await?;

        let time_index_expr = self.create_time_index_column_expr()?;
        let first_field_expr =
            self.create_field_column_exprs()?
                .pop()
                .with_context(|| ValueNotFoundSnafu {
                    table: self.ctx.table_name.clone().unwrap_or_default(),
                })?;
        let first_value_expr = first_value(first_field_expr, vec![]);

        let ordered_aggregated_input = LogicalPlanBuilder::from(input)
            .aggregate(
                vec![time_index_expr.clone()],
                vec![first_value_expr.clone()],
            )
            .context(DataFusionPlanningSnafu)?
            .sort(vec![time_index_expr.sort(true, false)])
            .context(DataFusionPlanningSnafu)?
            .build()
            .context(DataFusionPlanningSnafu)?;

        let fake_labels = self
            .ctx
            .selector_matcher
            .iter()
            .filter_map(|matcher| match matcher.op {
                MatchOp::Equal => Some((matcher.name.clone(), matcher.value.clone())),
                _ => None,
            })
            .collect::<Vec<_>>();

        // Create the absent plan
        let absent_plan = LogicalPlan::Extension(Extension {
            node: Arc::new(
                Absent::try_new(
                    self.ctx.start,
                    self.ctx.end,
                    self.ctx.interval,
                    self.ctx.time_index_column.as_ref().unwrap().clone(),
                    self.ctx.field_columns[0].clone(),
                    fake_labels,
                    ordered_aggregated_input,
                )
                .context(DataFusionPlanningSnafu)?,
            ),
        });

        Ok(absent_plan)
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
                    Some(DfExpr::Cast(Cast {
                        expr: Box::new(expr),
                        data_type: ArrowDataType::Float64,
                    }))
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
                DfExpr::Cast(Cast {
                    data_type: ArrowDataType::Float64,
                    ..
                })
            ) || matches!(&expr, DfExpr::Literal(ScalarValue::Float64(_), _))
            {
                expr
            } else {
                DfExpr::Cast(Cast {
                    expr: Box::new(expr),
                    data_type: ArrowDataType::Float64,
                })
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
        let (left, right) = if !only_join_time_index
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
            (left, right)
        } else {
            (left, right)
        };

        // push time index column if it exists
        if let (Some(left_time_index_column), Some(right_time_index_column)) =
            (left_time_index_column, right_time_index_column)
        {
            left_tag_columns.insert(left_time_index_column);
            right_tag_columns.insert(right_time_index_column);
        }

        let right = LogicalPlanBuilder::from(right)
            .alias(right_table_ref)
            .context(DataFusionPlanningSnafu)?
            .build()
            .context(DataFusionPlanningSnafu)?;

        // Inner Join on time index column to concat two operator
        LogicalPlanBuilder::from(left)
            .alias(left_table_ref)
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
            .context(DataFusionPlanningSnafu)
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
        let null = Self::string_scalar_value(&data_type, None).with_context(|| {
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
                DfExpr::Cast(Cast {
                    expr: Box::new(column),
                    data_type: value_type.clone(),
                })
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

    /// Build a set operator (AND/OR/UNLESS)
    fn set_op_on_non_field_columns(
        &mut self,
        mut left: LogicalPlan,
        mut right: LogicalPlan,
        left_context: PromPlannerContext,
        right_context: PromPlannerContext,
        op: TokenType,
        modifier: &Option<BinModifier>,
    ) -> Result<LogicalPlan> {
        let left_tag_col_set = left_context
            .tag_columns
            .iter()
            .cloned()
            .collect::<HashSet<_>>();
        let right_tag_col_set = right_context
            .tag_columns
            .iter()
            .cloned()
            .collect::<HashSet<_>>();

        if matches!(op.id(), token::T_LOR) {
            return self.or_operator(
                left,
                right,
                left_tag_col_set,
                right_tag_col_set,
                left_context,
                right_context,
                modifier,
            );
        }

        if let Some(modifier) = modifier {
            ensure!(
                matches!(
                    modifier.card,
                    VectorMatchCardinality::OneToOne | VectorMatchCardinality::ManyToMany
                ),
                UnsupportedVectorMatchSnafu {
                    name: modifier.card.clone(),
                },
            );
        }

        let output_context = left_context.clone();
        let visible_left_schema = left.schema().clone();
        let mut left_context = left_context;
        let mut right_context = right_context;
        let added_marker_to_left = if Self::only_temporality_match_label_mismatches(
            &left_context,
            &right_context,
            modifier,
        ) {
            let aligned = Self::align_temporality_match_column(
                left,
                right,
                &mut left_context,
                &mut right_context,
            )?;
            left = aligned.0;
            right = aligned.1;
            aligned.2
        } else {
            false
        };

        let mut left_tag_col_set = left_context
            .tag_columns
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>();
        let mut right_tag_col_set = right_context
            .tag_columns
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>();
        if let Some(matching) = modifier
            .as_ref()
            .and_then(|modifier| modifier.matching.as_ref())
        {
            match matching {
                LabelModifier::Include(on) => {
                    let mask = on.labels.iter().cloned().collect::<BTreeSet<_>>();
                    left_tag_col_set = left_tag_col_set.intersection(&mask).cloned().collect();
                    right_tag_col_set = right_tag_col_set.intersection(&mask).cloned().collect();
                }
                LabelModifier::Exclude(ignoring) => {
                    for label in &ignoring.labels {
                        let _ = left_tag_col_set.remove(label);
                        let _ = right_tag_col_set.remove(label);
                    }
                }
            }
        }
        ensure!(
            left_tag_col_set == right_tag_col_set,
            CombineTableColumnMismatchSnafu {
                left: left_tag_col_set.iter().cloned().collect::<Vec<_>>(),
                right: right_tag_col_set.iter().cloned().collect::<Vec<_>>(),
            }
        );

        let left_time_index = left_context.time_index_column.clone().unwrap();
        let right_time_index = right_context.time_index_column.clone().unwrap();

        // alias right time index column if necessary
        if left_context.time_index_column != right_context.time_index_column {
            let right_project_exprs = right
                .schema()
                .fields()
                .iter()
                .map(|field| {
                    if field.name() == &right_time_index {
                        DfExpr::Column(Column::from_name(&right_time_index)).alias(&left_time_index)
                    } else {
                        DfExpr::Column(Column::from_name(field.name()))
                    }
                })
                .collect::<Vec<_>>();

            right = LogicalPlanBuilder::from(right)
                .project(right_project_exprs)
                .context(DataFusionPlanningSnafu)?
                .build()
                .context(DataFusionPlanningSnafu)?;
        }

        let join_keys = left_tag_col_set
            .into_iter()
            .chain([left_time_index])
            .collect::<Vec<_>>();

        ensure!(
            left_context.field_columns.len() == 1
                || Self::field_columns_are_alternative_samples(
                    left.schema(),
                    &left_context.field_columns,
                ),
            MultiFieldsNotSupportedSnafu {
                operator: "AND/UNLESS operator"
            }
        );
        // Generate join plan.
        // All set operations in PromQL are "distinct"
        let result = match op.id() {
            token::T_LAND => LogicalPlanBuilder::from(left)
                .distinct()
                .context(DataFusionPlanningSnafu)?
                .join_detailed(
                    right,
                    JoinType::LeftSemi,
                    (join_keys.clone(), join_keys),
                    None,
                    NullEquality::NullEqualsNull,
                )
                .context(DataFusionPlanningSnafu)?
                .build()
                .context(DataFusionPlanningSnafu),
            token::T_LUNLESS => LogicalPlanBuilder::from(left)
                .distinct()
                .context(DataFusionPlanningSnafu)?
                .join_detailed(
                    right,
                    JoinType::LeftAnti,
                    (join_keys.clone(), join_keys),
                    None,
                    NullEquality::NullEqualsNull,
                )
                .context(DataFusionPlanningSnafu)?
                .build()
                .context(DataFusionPlanningSnafu),
            token::T_LOR => {
                // OR is handled at the beginning of this function, as it cannot
                // be expressed using JOIN like AND and UNLESS.
                unreachable!()
            }
            _ => UnexpectedTokenSnafu { token: op }.fail(),
        }?;
        let result = if added_marker_to_left {
            LogicalPlanBuilder::from(result)
                .project(visible_left_schema.iter().map(|(qualifier, field)| {
                    DfExpr::Column(Column::new(qualifier.cloned(), field.name().clone()))
                }))
                .context(DataFusionPlanningSnafu)?
                .build()
                .context(DataFusionPlanningSnafu)?
        } else {
            result
        };

        // AND/UNLESS preserve the complete left operand schema and metadata.
        self.ctx = output_context;
        Ok(result)
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

    // TODO(ruihang): change function name
    #[allow(clippy::too_many_arguments)]
    fn or_operator(
        &mut self,
        left: LogicalPlan,
        right: LogicalPlan,
        left_tag_cols_set: HashSet<String>,
        right_tag_cols_set: HashSet<String>,
        left_context: PromPlannerContext,
        right_context: PromPlannerContext,
        modifier: &Option<BinModifier>,
    ) -> Result<LogicalPlan> {
        let left_is_empty = Self::is_zero_row_empty_relation(&left);
        let right_is_empty = Self::is_zero_row_empty_relation(&right);
        match (left_is_empty, right_is_empty) {
            (true, false) => {
                self.ctx = right_context;
                return Ok(right);
            }
            (false, true) => {
                self.ctx = left_context;
                return Ok(left);
            }
            (true, true) => {
                self.ctx = left_context;
                return Ok(left);
            }
            (false, false) => {}
        }

        ensure!(
            !left.schema().fields().is_empty() && !right.schema().fields().is_empty(),
            UnexpectedPlanExprSnafu {
                desc: "OR operator input has zero columns",
            }
        );
        let left_has_alternative_samples =
            Self::field_columns_are_alternative_samples(left.schema(), &left_context.field_columns);
        let right_has_alternative_samples = Self::field_columns_are_alternative_samples(
            right.schema(),
            &right_context.field_columns,
        );
        ensure!(
            left_context.field_columns.len() == 1 || left_has_alternative_samples,
            MultiFieldsNotSupportedSnafu {
                operator: "OR operator"
            }
        );
        ensure!(
            right_context.field_columns.len() == 1 || right_has_alternative_samples,
            MultiFieldsNotSupportedSnafu {
                operator: "OR operator"
            }
        );

        // prepare hash sets
        let all_tags = left_tag_cols_set
            .union(&right_tag_cols_set)
            .cloned()
            .collect::<HashSet<_>>();
        let left_qualifier = left.schema().qualified_field(0).0.cloned();
        let right_qualifier = right.schema().qualified_field(0).0.cloned();
        let left_qualifier_string = left_qualifier
            .as_ref()
            .map(|l| l.to_string())
            .unwrap_or_default();
        let right_qualifier_string = right_qualifier
            .as_ref()
            .map(|r| r.to_string())
            .unwrap_or_default();
        let left_time_index_column =
            left_context
                .time_index_column
                .clone()
                .with_context(|| TimeIndexNotFoundSnafu {
                    table: left_qualifier_string.clone(),
                })?;
        let right_time_index_column =
            right_context
                .time_index_column
                .clone()
                .with_context(|| TimeIndexNotFoundSnafu {
                    table: right_qualifier_string.clone(),
                })?;
        let native_histogram_type = Self::native_histogram_arrow_type();
        let is_numeric = |data_type: &ArrowDataType| {
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
                    | ArrowDataType::Float32
                    | ArrowDataType::Float64
            )
        };
        let left_fields = left_context
            .field_columns
            .iter()
            .map(|name| {
                left.schema()
                    .iter()
                    .find(|(_, field)| field.name() == name)
                    .map(|(qualifier, field)| {
                        (name.clone(), qualifier.cloned(), field.data_type().clone())
                    })
                    .with_context(|| ColumnNotFoundSnafu { col: name.clone() })
            })
            .collect::<Result<Vec<_>>>()?;
        let right_fields = right_context
            .field_columns
            .iter()
            .map(|name| {
                right
                    .schema()
                    .iter()
                    .find(|(_, field)| field.name() == name)
                    .map(|(qualifier, field)| {
                        (name.clone(), qualifier.cloned(), field.data_type().clone())
                    })
                    .with_context(|| ColumnNotFoundSnafu { col: name.clone() })
            })
            .collect::<Result<Vec<_>>>()?;
        let left_field = &left_fields[0];
        let right_field = &right_fields[0];
        let left_field_col = &left_field.0;
        let right_field_col = &right_field.0;
        let fields_are_samples = |fields: &[(String, Option<TableReference>, ArrowDataType)]| {
            fields.iter().all(|(_, _, data_type)| {
                is_numeric(data_type) || data_type == &native_histogram_type
            })
        };
        let mixed_sample_types = if left_has_alternative_samples || right_has_alternative_samples {
            if !fields_are_samples(&left_fields) || !fields_are_samples(&right_fields) {
                return UnexpectedPlanExprSnafu {
                    desc: format!(
                        "OR value fields have incompatible types: {:?} and {:?}",
                        left_fields
                            .iter()
                            .map(|(_, _, data_type)| data_type)
                            .collect::<Vec<_>>(),
                        right_fields
                            .iter()
                            .map(|(_, _, data_type)| data_type)
                            .collect::<Vec<_>>()
                    ),
                }
                .fail();
            }
            true
        } else {
            (left_field.2 == native_histogram_type && is_numeric(&right_field.2))
                || (right_field.2 == native_histogram_type && is_numeric(&left_field.2))
        };
        let target_field_type = if mixed_sample_types {
            // Mixed vectors use the existing response representation: one nullable float column
            // and one nullable native-histogram column.
            ArrowDataType::Float64
        } else if left_field.2 == right_field.2 {
            left_field.2.clone()
        } else if is_numeric(&left_field.2) && is_numeric(&right_field.2) {
            ArrowDataType::Float64
        } else {
            return UnexpectedPlanExprSnafu {
                desc: format!(
                    "OR value fields have incompatible types: {:?} and {:?}",
                    left_field.2, right_field.2
                ),
            }
            .fail();
        };
        let (mixed_float_field_col, mixed_histogram_field_col) = if mixed_sample_types {
            let mut reserved_names = left
                .schema()
                .fields()
                .iter()
                .chain(right.schema().fields().iter())
                .map(|field| field.name().clone())
                .collect::<HashSet<_>>();
            for (name, _, _) in left_fields.iter().chain(&right_fields) {
                reserved_names.remove(name);
            }
            reserved_names.extend(all_tags.iter().cloned());
            let unique_name = |prefix: &str, reserved_names: &mut HashSet<String>| {
                let mut index = 0;
                loop {
                    let name = format!("{prefix}{index}");
                    index += 1;
                    if reserved_names.insert(name.clone()) {
                        break name;
                    }
                }
            };
            let float_field = unique_name(OR_FLOAT_FIELD_PREFIX, &mut reserved_names);
            let histogram_field = unique_name(OR_HISTOGRAM_FIELD_PREFIX, &mut reserved_names);
            (float_field, histogram_field)
        } else {
            (left_field_col.clone(), String::new())
        };
        let left_tag_types = left_tag_cols_set
            .iter()
            .map(|label| {
                left.schema()
                    .fields()
                    .iter()
                    .find(|field| field.name() == label)
                    .map(|field| (label.clone(), field.data_type().clone()))
                    .with_context(|| ColumnNotFoundSnafu { col: label.clone() })
            })
            .collect::<Result<HashMap<_, _>>>()?;
        let right_tag_types = right_tag_cols_set
            .iter()
            .map(|label| {
                right
                    .schema()
                    .fields()
                    .iter()
                    .find(|field| field.name() == label)
                    .map(|field| (label.clone(), field.data_type().clone()))
                    .with_context(|| ColumnNotFoundSnafu { col: label.clone() })
            })
            .collect::<Result<HashMap<_, _>>>()?;
        let mut target_tag_types = HashMap::with_capacity(all_tags.len());
        for label in &all_tags {
            let Some(data_type) =
                Self::common_label_data_type(left_tag_types.get(label), right_tag_types.get(label))
            else {
                return UnexpectedPlanExprSnafu {
                    desc: format!(
                        "OR label {label} has incompatible types: {:?} and {:?}",
                        left_tag_types.get(label),
                        right_tag_types.get(label)
                    ),
                }
                .fail();
            };
            target_tag_types.insert(label.clone(), data_type);
        }
        let left_has_tsid = left
            .schema()
            .fields()
            .iter()
            .any(|field| field.name() == DATA_SCHEMA_TSID_COLUMN_NAME);
        let right_has_tsid = right
            .schema()
            .fields()
            .iter()
            .any(|field| field.name() == DATA_SCHEMA_TSID_COLUMN_NAME);

        // step 0: fill all columns in output schema
        let mut all_columns_set = left
            .schema()
            .fields()
            .iter()
            .chain(right.schema().fields().iter())
            .map(|field| field.name().clone())
            .collect::<HashSet<_>>();
        // Keep `__tsid` only when both sides contain it, otherwise it may break schema alignment
        // (e.g. `unknown_metric or some_metric`).
        if !(left_has_tsid && right_has_tsid) {
            all_columns_set.remove(DATA_SCHEMA_TSID_COLUMN_NAME);
        }
        // remove time index column
        all_columns_set.remove(&left_time_index_column);
        all_columns_set.remove(&right_time_index_column);
        if mixed_sample_types {
            for (name, _, _) in left_fields.iter().chain(&right_fields) {
                all_columns_set.remove(name);
            }
            all_columns_set.extend(all_tags.iter().cloned());
            all_columns_set.insert(mixed_float_field_col.clone());
            all_columns_set.insert(mixed_histogram_field_col.clone());
        } else if left_field_col != right_field_col {
            // remove field column in the right
            all_columns_set.remove(right_field_col);
        }
        let mut all_columns = all_columns_set.into_iter().collect::<Vec<_>>();
        // sort to ensure the generated schema is not volatile
        all_columns.sort_unstable();
        // use left time index column name as the result time index column name
        all_columns.insert(0, left_time_index_column.clone());
        let mut occupied_column_names = left
            .schema()
            .fields()
            .iter()
            .chain(right.schema().fields().iter())
            .map(|field| field.name().clone())
            .collect::<HashSet<_>>();

        // step 1: align schema using project, fill non-exist columns with null
        let aligned_label_expr = |col: &String, source_types: &HashMap<String, ArrowDataType>| {
            let target_type = &target_tag_types[col];
            if let Some(source_type) = source_types.get(col) {
                let expr = DfExpr::Column(Column::new(None::<String>, col));
                if source_type == target_type {
                    expr
                } else {
                    DfExpr::Cast(Cast {
                        expr: Box::new(expr),
                        data_type: target_type.clone(),
                    })
                    .alias(col.clone())
                }
            } else {
                DfExpr::Literal(
                    Self::string_scalar_value(target_type, None)
                        .expect("target label type is a string"),
                    None,
                )
                .alias(col.clone())
            }
        };
        let null_histogram =
            ScalarValue::try_new_null(&native_histogram_type).context(DataFusionPlanningSnafu)?;
        let mixed_value_expr = |fields: &[(String, Option<TableReference>, ArrowDataType)],
                                output_col: &String| {
            if output_col == &mixed_float_field_col {
                if let Some((name, qualifier, data_type)) = fields
                    .iter()
                    .find(|(_, _, data_type)| is_numeric(data_type))
                {
                    let expr = DfExpr::Column(Column::new(qualifier.clone(), name));
                    if data_type == &ArrowDataType::Float64 {
                        expr.alias(output_col)
                    } else {
                        DfExpr::Cast(Cast {
                            expr: Box::new(expr),
                            data_type: ArrowDataType::Float64,
                        })
                        .alias(output_col)
                    }
                } else {
                    DfExpr::Literal(ScalarValue::Float64(None), None).alias(output_col)
                }
            } else {
                fields
                    .iter()
                    .find(|(_, _, data_type)| data_type == &native_histogram_type)
                    .map(|(name, qualifier, _)| {
                        DfExpr::Column(Column::new(qualifier.clone(), name)).alias(output_col)
                    })
                    .unwrap_or_else(|| {
                        DfExpr::Literal(null_histogram.clone(), None).alias(output_col)
                    })
            }
        };
        let left_proj_exprs = all_columns.iter().map(|col| {
            if mixed_sample_types
                && (col == &mixed_float_field_col || col == &mixed_histogram_field_col)
            {
                mixed_value_expr(&left_fields, col)
            } else if !mixed_sample_types
                && col == left_field_col
                && left_field.2 != target_field_type
            {
                DfExpr::Cast(Cast {
                    expr: Box::new(DfExpr::Column(Column::new(
                        left_field.1.clone(),
                        left_field_col,
                    ))),
                    data_type: target_field_type.clone(),
                })
                .alias(left_field_col.clone())
            } else if target_tag_types.contains_key(col) {
                aligned_label_expr(col, &left_tag_types)
            } else {
                DfExpr::Column(Column::new(None::<String>, col))
            }
        });
        let right_time_index_expr = DfExpr::Column(Column::new(
            right_qualifier.clone(),
            right_time_index_column,
        ))
        .alias(left_time_index_column.clone());
        // The field column in right side may not have qualifier (it may be removed by join operation),
        // so we need to find it from the schema.
        // `skip（1)` to skip the time index column
        let right_proj_exprs_without_time_index = all_columns.iter().skip(1).map(|col| {
            // expr
            if mixed_sample_types
                && (col == &mixed_float_field_col || col == &mixed_histogram_field_col)
            {
                mixed_value_expr(&right_fields, col)
            } else if !mixed_sample_types && col == left_field_col {
                let expr = DfExpr::Column(Column::new(right_field.1.clone(), right_field_col));
                if right_field.2 != target_field_type {
                    DfExpr::Cast(Cast {
                        expr: Box::new(expr),
                        data_type: target_field_type.clone(),
                    })
                    .alias(left_field_col.clone())
                } else if left_field_col != right_field_col {
                    expr.alias(left_field_col.clone())
                } else {
                    expr
                }
            } else if target_tag_types.contains_key(col) {
                aligned_label_expr(col, &right_tag_types)
            } else {
                DfExpr::Column(Column::new(None::<String>, col))
            }
        });
        let right_proj_exprs = [right_time_index_expr]
            .into_iter()
            .chain(right_proj_exprs_without_time_index);

        let left_projected = LogicalPlanBuilder::from(left)
            .project(left_proj_exprs)
            .context(DataFusionPlanningSnafu)?
            .alias(left_qualifier_string.clone())
            .context(DataFusionPlanningSnafu)?
            .build()
            .context(DataFusionPlanningSnafu)?;
        let right_projected = LogicalPlanBuilder::from(right)
            .project(right_proj_exprs)
            .context(DataFusionPlanningSnafu)?
            .alias(right_qualifier_string.clone())
            .context(DataFusionPlanningSnafu)?
            .build()
            .context(DataFusionPlanningSnafu)?;

        // step 2: compute match columns
        let mut match_columns = if let Some(modifier) = modifier
            && let Some(matching) = &modifier.matching
        {
            match matching {
                // keeps columns mentioned in `on`
                LabelModifier::Include(on) => on.labels.clone(),
                // removes columns memtioned in `ignoring`
                LabelModifier::Exclude(ignoring) => {
                    let ignoring = ignoring.labels.iter().cloned().collect::<HashSet<_>>();
                    all_tags.difference(&ignoring).cloned().collect()
                }
            }
        } else {
            all_tags.iter().cloned().collect()
        };
        // sort to ensure the generated plan is not volatile
        match_columns.sort_unstable();
        match_columns.dedup();
        occupied_column_names.extend(
            left_projected
                .schema()
                .fields()
                .iter()
                .chain(right_projected.schema().fields().iter())
                .map(|field| field.name().clone()),
        );

        let visible_schema = left_projected.schema().clone();
        let visible_left_exprs = left_projected
            .schema()
            .iter()
            .map(|(qualifier, field)| {
                DfExpr::Column(Column::new(qualifier.cloned(), field.name().clone()))
            })
            .collect::<Vec<_>>();
        let visible_right_exprs = right_projected
            .schema()
            .iter()
            .map(|(qualifier, field)| {
                DfExpr::Column(Column::new(qualifier.cloned(), field.name().clone()))
            })
            .collect::<Vec<_>>();
        let mut left_match_exprs = Vec::with_capacity(match_columns.len());
        let mut right_match_exprs = Vec::with_capacity(match_columns.len());
        let mut next_internal_column = 0;

        for label in &match_columns {
            let left_field = if left_tag_cols_set.contains(label) {
                Some(
                    left_projected
                        .schema()
                        .iter()
                        .find(|(_, field)| field.name() == label)
                        .map(|(qualifier, field)| (qualifier.cloned(), field.data_type().clone()))
                        .with_context(|| ColumnNotFoundSnafu { col: label.clone() })?,
                )
            } else {
                None
            };
            let right_field = if right_tag_cols_set.contains(label) {
                Some(
                    right_projected
                        .schema()
                        .iter()
                        .find(|(_, field)| field.name() == label)
                        .map(|(qualifier, field)| (qualifier.cloned(), field.data_type().clone()))
                        .with_context(|| ColumnNotFoundSnafu { col: label.clone() })?,
                )
            } else {
                None
            };
            let data_type = match (left_field.as_ref(), right_field.as_ref()) {
                (Some((_, left_type)), Some((_, right_type))) if left_type == right_type => {
                    left_type.clone()
                }
                (Some((_, left_type)), Some((_, right_type))) => {
                    return UnexpectedPlanExprSnafu {
                        desc: format!(
                            "OR match label {label} has incompatible types: {left_type:?} and {right_type:?}"
                        ),
                    }
                    .fail();
                }
                (Some((_, data_type)), None) | (None, Some((_, data_type))) => data_type.clone(),
                (None, None) => ArrowDataType::Utf8,
            };
            let Some(value_type) = Self::string_value_data_type(&data_type).cloned() else {
                return UnexpectedPlanExprSnafu {
                    desc: format!("OR match label {label} must be a string"),
                }
                .fail();
            };
            let internal_name = loop {
                let name = format!("__promql_or_match_{next_internal_column}");
                next_internal_column += 1;
                if occupied_column_names.insert(name.clone()) {
                    break name;
                }
            };
            left_match_exprs.push(Self::normalized_match_key_expr(
                label,
                left_field,
                &value_type,
                &internal_name,
            ));
            right_match_exprs.push(Self::normalized_match_key_expr(
                label,
                right_field,
                &value_type,
                &internal_name,
            ));
        }

        let left_augmented = LogicalPlanBuilder::from(left_projected)
            .project(visible_left_exprs.into_iter().chain(left_match_exprs))
            .context(DataFusionPlanningSnafu)?
            .build()
            .context(DataFusionPlanningSnafu)?;
        let right_augmented = LogicalPlanBuilder::from(right_projected)
            .project(visible_right_exprs.into_iter().chain(right_match_exprs))
            .context(DataFusionPlanningSnafu)?
            .build()
            .context(DataFusionPlanningSnafu)?;

        // step 3: build `UnionDistinctOn` with normalized internal match keys.
        let visible_field_count = visible_schema.fields().len();
        let compare_key_indices =
            (visible_field_count..visible_field_count + match_columns.len()).collect::<Vec<_>>();
        let (time_qualifier, _) = visible_schema
            .iter()
            .find(|(_, field)| field.name() == &left_time_index_column)
            .with_context(|| TimeIndexNotFoundSnafu {
                table: left_qualifier_string.clone(),
            })?;
        let ts_col_idx = left_augmented
            .schema()
            .iter()
            .position(|(qualifier, field)| {
                qualifier == time_qualifier && field.name() == &left_time_index_column
            })
            .with_context(|| TimeIndexNotFoundSnafu {
                table: left_qualifier_string.clone(),
            })?;
        let union_distinct_on = UnionDistinctOn::try_new(
            left_augmented,
            right_augmented,
            compare_key_indices,
            ts_col_idx,
        )
        .context(DataFusionPlanningSnafu)?;
        let augmented_result = LogicalPlan::Extension(Extension {
            node: Arc::new(union_distinct_on),
        });
        let result = LogicalPlanBuilder::from(augmented_result)
            .project(visible_schema.iter().map(|(qualifier, field)| {
                DfExpr::Column(Column::new(qualifier.cloned(), field.name().clone()))
            }))
            .context(DataFusionPlanningSnafu)?
            .build()
            .context(DataFusionPlanningSnafu)?;

        // step 4: update context
        let output_field_col = left_field_col.clone();
        let mut output_context = left_context;
        let mut visible_tags = all_tags.into_iter().collect::<Vec<_>>();
        visible_tags.sort_unstable();
        output_context.time_index_column = Some(left_time_index_column);
        output_context.tag_columns = visible_tags;
        output_context.field_columns = if mixed_sample_types {
            vec![mixed_float_field_col, mixed_histogram_field_col]
        } else {
            vec![output_field_col]
        };
        output_context.use_tsid = left_has_tsid && right_has_tsid;
        self.ctx = output_context;

        Ok(result)
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
        // Keep the generated float/histogram lane names while an element-wise operation
        // preserves both sample types, so downstream operators still recognize the pair.
        let preserve_field_names =
            Self::field_columns_are_alternative_samples(input.schema(), &self.ctx.field_columns);
        let table_ref = self.ctx.table_name.clone().map(TableReference::bare);
        // Derived labels can be unqualified even when the context still names the source table.
        let input_schema = input.schema().clone();
        let non_field_columns_iter = self
            .ctx
            .tag_columns
            .iter()
            .chain(self.ctx.time_index_column.iter())
            .map(|col| {
                input_schema
                    .qualified_field_with_name(table_ref.as_ref(), col)
                    .or_else(|_| input_schema.qualified_field_with_unqualified_name(col))
                    .map(|field| DfExpr::Column(field.into()))
                    .context(DataFusionPlanningSnafu)
            });
        let tsid_iter =
            Self::optional_tsid_projection(input.schema(), table_ref.as_ref(), self.ctx.use_tsid)
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
        let input_expr = datafusion::logical_expr::col(
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
    literals: Vec<DfExpr>,
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
mod test {
    use std::time::{Duration, UNIX_EPOCH};

    use catalog::RegisterTableRequest;
    use catalog::memory::{MemoryCatalogManager, new_memory_catalog_manager};
    use common_base::Plugins;
    use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
    use common_query::native_histogram::{
        CUSTOM_BUCKETS_SCHEMA, CounterResetHint, NativeHistogram, build_histogram_array,
    };
    use common_query::prelude::{greptime_native_histogram, greptime_timestamp, greptime_value};
    use common_query::prometheus::PROMETHEUS_STALE_NAN_BITS;
    use common_query::test_util::DummyDecoder;
    use common_recordbatch::RecordBatch as GreptimeRecordBatch;
    use datafusion::arrow::array::{
        Array, Float64Array, Int64Array, StringArray, TimestampMillisecondArray,
    };
    use datafusion::arrow::datatypes::{Field, Schema as ArrowSchema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::catalog::{CatalogProvider, MemoryCatalogProvider, MemorySchemaProvider};
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::datasource::source::DataSourceExec;
    use datafusion::datasource::{MemTable, provider_as_source};
    use datafusion::execution::context::SessionContext;
    use datafusion::logical_expr::Extension;
    use datatypes::prelude::ConcreteDataType;
    use datatypes::schema::{ColumnSchema, Schema};
    use promql_parser::label::Labels;
    use promql_parser::parser;
    use session::context::QueryContext;
    use substrait::{DFLogicalSubstraitConvertor, SubstraitPlan};
    use table::Table;
    use table::metadata::{FilterPushDownType, TableInfoBuilder, TableMetaBuilder};
    use table::test_util::{EmptyTable, MemTable as GreptimeMemTable};

    use super::*;
    use crate::QueryEngineContext;
    use crate::options::QueryOptions;
    use crate::parser::QueryLanguageParser;
    use crate::query_engine::DefaultSerializer;

    mod delta;

    fn find_instant_manipulate(plan: &LogicalPlan) -> Option<&InstantManipulate> {
        if let LogicalPlan::Extension(Extension { node }) = plan
            && let Some(instant_manipulate) = node.as_any().downcast_ref::<InstantManipulate>()
        {
            return Some(instant_manipulate);
        }

        plan.inputs().into_iter().find_map(find_instant_manipulate)
    }

    fn build_query_engine_state() -> QueryEngineState {
        QueryEngineState::new(
            new_memory_catalog_manager().unwrap(),
            None,
            None,
            None,
            None,
            None,
            false,
            Plugins::default(),
            QueryOptions::default(),
        )
    }

    #[test]
    fn common_label_type_preserves_only_shared_dictionary_encoding() {
        let dictionary = ArrowDataType::Dictionary(
            Box::new(ArrowDataType::UInt32),
            Box::new(ArrowDataType::Utf8),
        );
        let other_dictionary = ArrowDataType::Dictionary(
            Box::new(ArrowDataType::Int32),
            Box::new(ArrowDataType::Utf8),
        );

        assert_eq!(
            Some(dictionary.clone()),
            PromPlanner::common_label_data_type(Some(&dictionary), Some(&dictionary))
        );
        assert_eq!(
            Some(ArrowDataType::Utf8),
            PromPlanner::common_label_data_type(Some(&dictionary), Some(&ArrowDataType::Utf8))
        );
        assert_eq!(
            Some(ArrowDataType::Utf8),
            PromPlanner::common_label_data_type(Some(&dictionary), Some(&other_dictionary))
        );
        assert_eq!(
            Some(ArrowDataType::Utf8),
            PromPlanner::common_label_data_type(Some(&dictionary), None)
        );
    }

    async fn build_optimized_promql_plan(
        table_provider: DfTableSourceProvider,
        eval_stmt: &EvalStmt,
    ) -> LogicalPlan {
        let state = build_query_engine_state();
        let raw_plan = PromPlanner::stmt_to_plan(table_provider, eval_stmt, &state)
            .await
            .unwrap();
        let context = QueryEngineContext::new(state.session_state(), QueryContext::arc());
        state
            .optimize_by_extension_rules(raw_plan, &context)
            .unwrap()
    }

    async fn build_optimized_tsid_plan(
        query: &str,
        num_tag: usize,
        num_field: usize,
        end_secs: u64,
        lookback_secs: u64,
    ) -> String {
        let eval_stmt = EvalStmt {
            expr: parser::parse(query).unwrap(),
            start: UNIX_EPOCH,
            end: UNIX_EPOCH
                .checked_add(Duration::from_secs(end_secs))
                .unwrap(),
            interval: Duration::from_secs(5),
            lookback_delta: Duration::from_secs(lookback_secs),
        };
        let table_provider = build_test_table_provider_with_tsid(
            &[(DEFAULT_SCHEMA_NAME.to_string(), "some_metric".to_string())],
            num_tag,
            num_field,
        )
        .await;

        build_optimized_promql_plan(table_provider, &eval_stmt)
            .await
            .display_indent_schema()
            .to_string()
    }

    async fn assert_nested_count_rewrite_applies(query: &str, expected_outer_agg: &str) {
        let plan_str = build_optimized_tsid_plan(query, 2, 1, 100_000, 1).await;

        assert!(plan_str.contains("PromSeriesDivide: tags=[\"__tsid\"]"));
        assert!(plan_str.contains("Projection: some_metric.timestamp, some_metric.tag_0"));
        assert!(plan_str.contains("Distinct:"));
        assert!(plan_str.contains(expected_outer_agg), "{plan_str}");
        assert!(!plan_str.contains("PromSeriesDivide: tags=[\"tag_0\"]"));
    }

    async fn assert_nested_count_rewrite_missing(query: &str, num_tag: usize, lookback_secs: u64) {
        let plan_str = build_optimized_tsid_plan(query, num_tag, 1, 100_000, lookback_secs).await;
        assert!(!plan_str.contains("Distinct:"), "{plan_str}");
    }

    fn build_eval_stmt(expr: &str) -> EvalStmt {
        EvalStmt {
            expr: parser::parse(expr).unwrap(),
            start: UNIX_EPOCH,
            end: UNIX_EPOCH
                .checked_add(Duration::from_secs(100_000))
                .unwrap(),
            interval: Duration::from_secs(5),
            lookback_delta: Duration::from_secs(1),
        }
    }

    enum DirectOrValue {
        Float64(f64),
        Int64(i64),
        NativeHistogram(NativeHistogram),
        Utf8(&'static str),
    }

    impl DirectOrValue {
        fn data_type(&self) -> ArrowDataType {
            match self {
                Self::Float64(_) => ArrowDataType::Float64,
                Self::Int64(_) => ArrowDataType::Int64,
                Self::NativeHistogram(_) => native_histogram_value_type().as_arrow_type(),
                Self::Utf8(_) => ArrowDataType::Utf8,
            }
        }
        fn array(&self) -> Arc<dyn Array> {
            match self {
                Self::Float64(v) => Arc::new(Float64Array::from(vec![*v])),
                Self::Int64(v) => Arc::new(Int64Array::from(vec![*v])),
                Self::NativeHistogram(v) => build_histogram_array(&[Some(v.clone())]),
                Self::Utf8(v) => Arc::new(StringArray::from(vec![*v])),
            }
        }
    }

    fn direct_or_histogram() -> NativeHistogram {
        NativeHistogram {
            schema: 0,
            zero_threshold: 0.0,
            sum: 1.0,
            reset_hint: CounterResetHint::Unknown,
            start_timestamp: None,
            custom_values: vec![],
            positive_spans: vec![],
            negative_spans: vec![],
            count: 1.0,
            zero_count: 1.0,
            positive_buckets: vec![],
            negative_buckets: vec![],
        }
    }

    fn operator_metric_table(
        name: &str,
        table_id: u32,
        tag: &str,
        le: Option<&str>,
        value: DirectOrValue,
    ) -> table::TableRef {
        let value_type = match &value {
            DirectOrValue::Float64(_) => ConcreteDataType::float64_datatype(),
            DirectOrValue::Int64(_) => ConcreteDataType::int64_datatype(),
            DirectOrValue::NativeHistogram(_) => native_histogram_value_type().clone(),
            DirectOrValue::Utf8(_) => ConcreteDataType::string_datatype(),
        };
        let tag_count = 1 + usize::from(le.is_some());
        let mut columns = vec![ColumnSchema::new(
            "tag".to_string(),
            ConcreteDataType::string_datatype(),
            false,
        )];
        if le.is_some() {
            columns.push(ColumnSchema::new(
                LE_COLUMN_NAME.to_string(),
                ConcreteDataType::string_datatype(),
                false,
            ));
        }
        columns.extend([
            ColumnSchema::new(
                "ts".to_string(),
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            )
            .with_time_index(true),
            ColumnSchema::new("v".to_string(), value_type, true),
        ]);
        let schema = Arc::new(Schema::new(columns));
        let mut arrays = vec![Arc::new(StringArray::from(vec![tag])) as Arc<dyn Array>];
        if let Some(le) = le {
            arrays.push(Arc::new(StringArray::from(vec![le])));
        }
        arrays.extend([
            Arc::new(TimestampMillisecondArray::from(vec![1_000])) as Arc<dyn Array>,
            value.array(),
        ]);
        let batch = RecordBatch::try_new(schema.arrow_schema().clone(), arrays).unwrap();
        let backing = GreptimeMemTable::new_with_catalog(
            name,
            GreptimeRecordBatch::from_df_record_batch(schema.clone(), batch),
            table_id,
            DEFAULT_CATALOG_NAME.to_string(),
            DEFAULT_SCHEMA_NAME.to_string(),
        );
        let value_index = tag_count + 1;
        let meta = TableMetaBuilder::empty()
            .schema(schema)
            .primary_key_indices((0..tag_count).collect())
            .value_indices(vec![value_index])
            .next_column_id((value_index + 1) as u32)
            .build()
            .unwrap();
        let info = Arc::new(
            TableInfoBuilder::default()
                .table_id(table_id)
                .name(name)
                .meta(meta)
                .build()
                .unwrap(),
        );
        Arc::new(Table::new(
            info,
            FilterPushDownType::Unsupported,
            backing.data_source(),
        ))
    }

    fn operator_table_provider() -> DfTableSourceProvider {
        let catalog = MemoryCatalogManager::with_default_setup();
        let tables = [
            operator_metric_table("lf", 2_001, "a", None, DirectOrValue::Float64(2.0)),
            operator_metric_table(
                "lh",
                2_002,
                "b",
                None,
                DirectOrValue::NativeHistogram(direct_or_histogram()),
            ),
            operator_metric_table("rf", 2_003, "b", None, DirectOrValue::Float64(3.0)),
            operator_metric_table(
                "rh",
                2_004,
                "a",
                None,
                DirectOrValue::NativeHistogram(direct_or_histogram()),
            ),
            operator_metric_table("fallback", 2_005, "c", None, DirectOrValue::Float64(7.0)),
            operator_metric_table(
                "bad_classic",
                2_006,
                "d",
                Some("broken"),
                DirectOrValue::Float64(1.0),
            ),
            operator_metric_table(
                "bad_native",
                2_007,
                "d",
                None,
                DirectOrValue::NativeHistogram(direct_or_histogram()),
            ),
        ];
        for table in tables {
            let info = table.table_info();
            catalog
                .register_table_sync(RegisterTableRequest {
                    catalog: DEFAULT_CATALOG_NAME.to_string(),
                    schema: DEFAULT_SCHEMA_NAME.to_string(),
                    table_name: info.name.clone(),
                    table_id: info.ident.table_id,
                    table,
                })
                .unwrap();
        }
        DfTableSourceProvider::new(
            catalog,
            false,
            QueryContext::arc(),
            DummyDecoder::arc(),
            false,
        )
    }

    fn operator_eval_stmt(expr: &str) -> EvalStmt {
        let time = UNIX_EPOCH.checked_add(Duration::from_secs(1)).unwrap();
        EvalStmt {
            expr: parser::parse(expr).unwrap(),
            start: time,
            end: time,
            interval: Duration::from_secs(1),
            lookback_delta: Duration::from_secs(5),
        }
    }

    struct DirectOrSource {
        name: &'static str,
        empty: bool,
        timestamp: i64,
        tags: Vec<(&'static str, Option<&'static str>)>,
        value: DirectOrValue,
    }

    fn source(
        name: &'static str,
        empty: bool,
        timestamp: i64,
        tags: Vec<(&'static str, Option<&'static str>)>,
        value: DirectOrValue,
    ) -> DirectOrSource {
        DirectOrSource {
            name,
            empty,
            timestamp,
            tags,
            value,
        }
    }

    fn tagged_source(
        name: &'static str,
        empty: bool,
        tag: (&'static str, Option<&'static str>),
        value: DirectOrValue,
    ) -> DirectOrSource {
        source(name, empty, 1, vec![("job", Some("job")), tag], value)
    }

    fn job_source(name: &'static str, value: DirectOrValue) -> DirectOrSource {
        source(name, true, 1, vec![("job", Some("job"))], value)
    }

    fn table(source: &DirectOrSource) -> Arc<MemTable> {
        let mut fields = vec![Field::new(
            "ts",
            ArrowDataType::Timestamp(ArrowTimeUnit::Millisecond, None),
            false,
        )];
        fields.extend(
            source
                .tags
                .iter()
                .map(|(name, _)| Field::new(*name, ArrowDataType::Utf8, true)),
        );
        fields.push(Field::new("v", source.value.data_type(), true));
        let schema = Arc::new(ArrowSchema::new(fields));
        let partitions = if source.empty {
            vec![vec![]]
        } else {
            let mut columns: Vec<Arc<dyn Array>> =
                vec![Arc::new(TimestampMillisecondArray::from(vec![
                    source.timestamp,
                ]))];
            columns.extend(
                source
                    .tags
                    .iter()
                    .map(|(_, value)| Arc::new(StringArray::from(vec![*value])) as Arc<dyn Array>),
            );
            columns.push(source.value.array());
            vec![vec![RecordBatch::try_new(schema.clone(), columns).unwrap()]]
        };
        Arc::new(MemTable::try_new(schema, partitions).unwrap())
    }

    fn scan(source: &DirectOrSource) -> LogicalPlan {
        LogicalPlanBuilder::scan(source.name, provider_as_source(table(source)), None)
            .unwrap()
            .build()
            .unwrap()
    }

    fn direct_or_context(qualifier: &str, tags: &[&str], field: &str) -> PromPlannerContext {
        PromPlannerContext {
            table_name: Some(qualifier.to_string()),
            time_index_column: Some("ts".to_string()),
            field_columns: vec![field.to_string()],
            tag_columns: tags.iter().map(|tag| (*tag).to_string()).collect(),
            ..Default::default()
        }
    }

    fn or_modifier(expr: &str) -> Option<BinModifier> {
        let PromExpr::Binary(expr) = parser::parse(expr).unwrap() else {
            unreachable!()
        };
        expr.modifier
    }

    async fn plan_direct_or(
        left: LogicalPlan,
        right: LogicalPlan,
        left_context: PromPlannerContext,
        right_context: PromPlannerContext,
        modifier: &Option<BinModifier>,
    ) -> LogicalPlan {
        let table_provider = build_test_table_provider_with_fields(
            &[(DEFAULT_SCHEMA_NAME.to_string(), "dummy".to_string())],
            &[],
        )
        .await;
        let mut planner = PromPlanner {
            table_provider,
            ctx: PromPlannerContext::default(),
            promql_annotations: None,
        };
        planner
            .or_operator(
                left,
                right,
                left_context.tag_columns.iter().cloned().collect(),
                right_context.tag_columns.iter().cloned().collect(),
                left_context,
                right_context,
                modifier,
            )
            .unwrap()
    }

    async fn execute(
        plan: LogicalPlan,
        state: &QueryEngineState,
    ) -> (LogicalPlan, Vec<RecordBatch>) {
        let context = QueryEngineContext::new(state.session_state(), QueryContext::arc());
        let optimized = state.optimize_by_extension_rules(plan, &context).unwrap();
        let physical = state
            .session_state()
            .create_physical_plan(&optimized)
            .await
            .unwrap();
        let batches =
            datafusion::physical_plan::collect(physical, state.session_state().task_ctx())
                .await
                .unwrap();
        (optimized, batches)
    }

    async fn run(
        left: &DirectOrSource,
        right: &DirectOrSource,
        left_context: PromPlannerContext,
        right_context: PromPlannerContext,
        modifier: &Option<BinModifier>,
    ) -> (LogicalPlan, Vec<RecordBatch>) {
        let plan = plan_direct_or(
            scan(left),
            scan(right),
            left_context,
            right_context,
            modifier,
        )
        .await;
        execute(plan, &build_query_engine_state()).await
    }

    async fn mixed_direct_or(histogram_on_left: bool) -> (PromPlanner, LogicalPlan) {
        let sample = |histogram: bool| {
            if histogram {
                DirectOrValue::NativeHistogram(direct_or_histogram())
            } else {
                DirectOrValue::Float64(1.25)
            }
        };
        let left = tagged_source(
            "lhs",
            false,
            (
                "k",
                Some(if histogram_on_left {
                    "histogram"
                } else {
                    "float"
                }),
            ),
            sample(histogram_on_left),
        );
        let right = tagged_source(
            "rhs",
            false,
            (
                "k",
                Some(if histogram_on_left {
                    "float"
                } else {
                    "histogram"
                }),
            ),
            sample(!histogram_on_left),
        );
        let table_provider = build_test_table_provider_with_fields(
            &[(DEFAULT_SCHEMA_NAME.to_string(), "dummy".to_string())],
            &[],
        )
        .await;
        let mut planner = PromPlanner {
            table_provider,
            ctx: PromPlannerContext::default(),
            promql_annotations: None,
        };
        let left_context = direct_or_context("lhs", &["job", "k"], "v");
        let right_context = direct_or_context("rhs", &["job", "k"], "v");
        let plan = planner
            .or_operator(
                scan(&left),
                scan(&right),
                left_context.tag_columns.iter().cloned().collect(),
                right_context.tag_columns.iter().cloned().collect(),
                left_context,
                right_context,
                &or_modifier("lhs or on(k) rhs"),
            )
            .unwrap();
        (planner, plan)
    }

    async fn mixed_aggregate_input(histograms: Vec<NativeHistogram>) -> (PromPlanner, LogicalPlan) {
        let float_field = format!("{OR_FLOAT_FIELD_PREFIX}0");
        let histogram_field = format!("{OR_HISTOGRAM_FIELD_PREFIX}0");
        let row_count = histograms.len() + 1;
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new(
                "ts",
                ArrowDataType::Timestamp(ArrowTimeUnit::Millisecond, None),
                false,
            ),
            Field::new("k", ArrowDataType::Utf8, false),
            Field::new(&float_field, ArrowDataType::Float64, true),
            Field::new(
                &histogram_field,
                native_histogram_value_type().as_arrow_type(),
                true,
            ),
        ]));
        let mut histogram_values = Vec::with_capacity(row_count);
        histogram_values.push(None);
        histogram_values.extend(histograms.into_iter().map(Some));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(TimestampMillisecondArray::from(vec![1; row_count])),
                Arc::new(StringArray::from_iter_values(
                    (0..row_count).map(|row| format!("kind_{row}")),
                )),
                Arc::new(Float64Array::from_iter(
                    (0..row_count).map(|row| (row == 0).then_some(1.25)),
                )),
                build_histogram_array(&histogram_values),
            ],
        )
        .unwrap();
        let table = Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap());
        let plan = LogicalPlanBuilder::scan("mixed", provider_as_source(table), None)
            .unwrap()
            .build()
            .unwrap();
        let table_provider = build_test_table_provider_with_fields(
            &[(DEFAULT_SCHEMA_NAME.to_string(), "dummy".to_string())],
            &[],
        )
        .await;
        let planner = PromPlanner {
            table_provider,
            ctx: PromPlannerContext {
                table_name: Some("mixed".to_string()),
                time_index_column: Some("ts".to_string()),
                field_columns: vec![float_field, histogram_field],
                tag_columns: vec!["k".to_string()],
                ..Default::default()
            },
            promql_annotations: None,
        };
        (planner, plan)
    }

    fn assert_no_internal_or_keys(schema: &DFSchema) {
        assert!(
            schema
                .fields()
                .iter()
                .all(|field| !field.name().starts_with("__promql_or_match_")),
            "{schema:?}"
        );
    }

    fn values(batches: &[RecordBatch], column: &str) -> Vec<f64> {
        batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column_by_name(column)
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap()
                    .iter()
                    .flatten()
            })
            .collect()
    }

    fn numeric_values(batches: &[RecordBatch], column: &str) -> Vec<f64> {
        batches
            .iter()
            .flat_map(|batch| {
                let values = datafusion::arrow::compute::cast(
                    batch.column_by_name(column).unwrap(),
                    &ArrowDataType::Float64,
                )
                .unwrap();
                values
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap()
                    .iter()
                    .flatten()
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    fn histograms(batches: &[RecordBatch], column: &str) -> Vec<NativeHistogram> {
        batches
            .iter()
            .flat_map(|batch| {
                let values = batch
                    .column_by_name(column)
                    .unwrap()
                    .as_any()
                    .downcast_ref::<datafusion::arrow::array::StructArray>()
                    .unwrap();
                (0..values.len()).filter_map(|row| {
                    common_query::native_histogram::read_histogram(values, row).unwrap()
                })
            })
            .collect()
    }

    fn rows(batches: &[RecordBatch]) -> Vec<(f64, Option<String>)> {
        let mut rows = batches
            .iter()
            .flat_map(|batch| {
                let values = batch
                    .column_by_name("v")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap();
                let labels = batch
                    .column_by_name("k")
                    .map(|column| column.as_any().downcast_ref::<StringArray>().unwrap());
                (0..batch.num_rows()).map(move |i| {
                    (
                        values.value(i),
                        labels.and_then(|labels| {
                            (!labels.is_null(i)).then(|| labels.value(i).to_string())
                        }),
                    )
                })
            })
            .collect::<Vec<_>>();
        rows.sort_by(|left, right| left.0.total_cmp(&right.0));
        rows
    }

    fn matrix_source(
        name: &'static str,
        k: Option<Option<&'static str>>,
        timestamp: i64,
        value: f64,
    ) -> DirectOrSource {
        let mut tags = vec![("job", Some("job"))];
        if let Some(k) = k {
            tags.push(("k", k));
        }
        source(name, false, timestamp, tags, DirectOrValue::Float64(value))
    }

    fn matrix_context(name: &str, k: Option<Option<&str>>) -> PromPlannerContext {
        direct_or_context(
            name,
            if k.is_some() { &["job", "k"] } else { &["job"] },
            "v",
        )
    }

    async fn build_missing_le_or_normal_metric_table_provider() -> DfTableSourceProvider {
        build_test_table_provider_with_fields(
            &[
                (
                    DEFAULT_SCHEMA_NAME.to_string(),
                    "non_existent_histogram_bucket".to_string(),
                ),
                (DEFAULT_SCHEMA_NAME.to_string(), "normal_metric".to_string()),
            ],
            &["pod", "instance"],
        )
        .await
    }

    fn assert_normal_metric_schema(plan: &LogicalPlan) {
        let fields = plan.schema().fields();
        assert_eq!(fields.len(), 4, "{fields:?}");
        assert!(
            fields.iter().any(|field| field.name() == "pod"),
            "{fields:?}"
        );
        assert!(
            fields.iter().any(|field| field.name() == "instance"),
            "{fields:?}"
        );
        assert!(
            fields
                .iter()
                .any(|field| field.name() == greptime_timestamp()),
            "{fields:?}"
        );
        assert!(
            fields.iter().any(|field| {
                field.name() == greptime_value() && field.data_type() == &ArrowDataType::Float64
            }),
            "{fields:?}"
        );
    }

    async fn build_test_table_provider_with_distinct_tags(
        table_tags: &[(&str, &[&str])],
    ) -> DfTableSourceProvider {
        let catalog_list = MemoryCatalogManager::with_default_setup();
        for (table_name, tags) in table_tags {
            let mut columns = tags
                .iter()
                .map(|tag| {
                    ColumnSchema::new(
                        (*tag).to_string(),
                        ConcreteDataType::string_datatype(),
                        false,
                    )
                })
                .collect::<Vec<_>>();
            columns.push(
                ColumnSchema::new(
                    greptime_timestamp().to_string(),
                    ConcreteDataType::timestamp_millisecond_datatype(),
                    false,
                )
                .with_time_index(true),
            );
            columns.push(ColumnSchema::new(
                greptime_value().to_string(),
                ConcreteDataType::float64_datatype(),
                true,
            ));
            let table_meta = TableMetaBuilder::empty()
                .schema(Arc::new(Schema::new(columns)))
                .primary_key_indices((0..tags.len()).collect())
                .next_column_id(1024)
                .build()
                .unwrap();
            let table_info = TableInfoBuilder::default()
                .name((*table_name).to_string())
                .meta(table_meta)
                .build()
                .unwrap();

            assert!(
                catalog_list
                    .register_table_sync(RegisterTableRequest {
                        catalog: DEFAULT_CATALOG_NAME.to_string(),
                        schema: DEFAULT_SCHEMA_NAME.to_string(),
                        table_name: (*table_name).to_string(),
                        table_id: 1024,
                        table: EmptyTable::from_table_info(&table_info),
                    })
                    .is_ok()
            );
        }

        DfTableSourceProvider::new(
            catalog_list,
            false,
            QueryContext::arc(),
            DummyDecoder::arc(),
            false,
        )
    }

    fn contains_histogram_fold(plan: &LogicalPlan) -> bool {
        matches!(plan, LogicalPlan::Extension(Extension { node }) if node.as_any().is::<HistogramFold>())
            || plan.inputs().into_iter().any(contains_histogram_fold)
    }

    async fn build_set_op_context_table_provider() -> DfTableSourceProvider {
        build_test_table_provider_with_distinct_tags(&[
            ("bucket_metric", &["job", "le"]),
            ("normal_metric", &["job"]),
            ("fallback_metric", &["instance"]),
        ])
        .await
    }

    async fn build_or_context_table_provider() -> DfTableSourceProvider {
        build_test_table_provider_with_distinct_tags(&[
            ("normal_metric", &["job"]),
            ("other_metric", &["instance"]),
            ("non_hist_metric", &["instance"]),
        ])
        .await
    }

    async fn optimize_and_create_physical_plan(
        state: &QueryEngineState,
        plan: LogicalPlan,
    ) -> (
        LogicalPlan,
        Arc<dyn datafusion::physical_plan::ExecutionPlan>,
    ) {
        let context = QueryEngineContext::new(state.session_state(), QueryContext::arc());
        let optimized = state.optimize_by_extension_rules(plan, &context).unwrap();
        let physical = state
            .session_state()
            .create_physical_plan(&optimized)
            .await
            .unwrap();
        (optimized, physical)
    }

    async fn build_test_table_provider(
        table_name_tuples: &[(String, String)],
        num_tag: usize,
        num_field: usize,
    ) -> DfTableSourceProvider {
        let catalog_list = MemoryCatalogManager::with_default_setup();
        for (schema_name, table_name) in table_name_tuples {
            let mut columns = vec![];
            for i in 0..num_tag {
                columns.push(ColumnSchema::new(
                    format!("tag_{i}"),
                    ConcreteDataType::string_datatype(),
                    false,
                ));
            }
            columns.push(
                ColumnSchema::new(
                    "timestamp".to_string(),
                    ConcreteDataType::timestamp_millisecond_datatype(),
                    false,
                )
                .with_time_index(true),
            );
            for i in 0..num_field {
                columns.push(ColumnSchema::new(
                    format!("field_{i}"),
                    ConcreteDataType::float64_datatype(),
                    true,
                ));
            }
            let schema = Arc::new(Schema::new(columns));
            let table_meta = TableMetaBuilder::empty()
                .schema(schema)
                .primary_key_indices((0..num_tag).collect())
                .value_indices((num_tag + 1..num_tag + 1 + num_field).collect())
                .next_column_id(1024)
                .build()
                .unwrap();
            let table_info = TableInfoBuilder::default()
                .name(table_name.clone())
                .meta(table_meta)
                .build()
                .unwrap();
            let table = EmptyTable::from_table_info(&table_info);

            assert!(
                catalog_list
                    .register_table_sync(RegisterTableRequest {
                        catalog: DEFAULT_CATALOG_NAME.to_string(),
                        schema: schema_name.clone(),
                        table_name: table_name.clone(),
                        table_id: 1024,
                        table,
                    })
                    .is_ok()
            );
        }

        DfTableSourceProvider::new(
            catalog_list,
            false,
            QueryContext::arc(),
            DummyDecoder::arc(),
            false,
        )
    }

    async fn build_test_native_histogram_table_provider(table_name: &str) -> DfTableSourceProvider {
        build_test_native_histogram_table_provider_with_marker(table_name, false).await
    }

    async fn build_test_native_histogram_table_provider_with_marker(
        table_name: &str,
        temporality_marker: bool,
    ) -> DfTableSourceProvider {
        let catalog_list = MemoryCatalogManager::with_default_setup();
        let mut columns = vec![
            ColumnSchema::new(
                "tag_0".to_string(),
                ConcreteDataType::string_datatype(),
                false,
            ),
            ColumnSchema::new(
                LE_COLUMN_NAME.to_string(),
                ConcreteDataType::string_datatype(),
                true,
            ),
        ];
        if temporality_marker {
            columns.push(ColumnSchema::new(
                OTLP_AGGREGATION_TEMPORALITY_LABEL.to_string(),
                ConcreteDataType::string_datatype(),
                true,
            ));
        }
        let tag_count = columns.len();
        columns.extend([
            ColumnSchema::new(
                "timestamp".to_string(),
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            )
            .with_time_index(true),
            ColumnSchema::new(
                greptime_native_histogram().to_string(),
                native_histogram_value_type().clone(),
                true,
            ),
        ]);
        let schema = Arc::new(Schema::new(columns));
        let table_meta = TableMetaBuilder::empty()
            .schema(schema)
            .primary_key_indices((0..tag_count).collect())
            .value_indices(vec![tag_count + 1])
            .next_column_id(1024)
            .build()
            .unwrap();
        let table_info = TableInfoBuilder::default()
            .name(table_name)
            .meta(table_meta)
            .build()
            .unwrap();
        let table = EmptyTable::from_table_info(&table_info);

        assert!(
            catalog_list
                .register_table_sync(RegisterTableRequest {
                    catalog: DEFAULT_CATALOG_NAME.to_string(),
                    schema: DEFAULT_SCHEMA_NAME.to_string(),
                    table_name: table_name.to_string(),
                    table_id: 1024,
                    table,
                })
                .is_ok()
        );

        DfTableSourceProvider::new(
            catalog_list,
            false,
            QueryContext::arc(),
            DummyDecoder::arc(),
            false,
        )
    }

    async fn build_test_multi_histogram_table_provider(table_name: &str) -> DfTableSourceProvider {
        let catalog_list = MemoryCatalogManager::with_default_setup();
        let columns = vec![
            ColumnSchema::new(
                "tag_0".to_string(),
                ConcreteDataType::string_datatype(),
                false,
            ),
            ColumnSchema::new(
                "timestamp".to_string(),
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            )
            .with_time_index(true),
            ColumnSchema::new(
                greptime_native_histogram().to_string(),
                native_histogram_value_type().clone(),
                true,
            ),
            ColumnSchema::new(
                "native_histogram_2".to_string(),
                native_histogram_value_type().clone(),
                true,
            ),
        ];
        let schema = Arc::new(Schema::new(columns));
        let table_meta = TableMetaBuilder::empty()
            .schema(schema)
            .primary_key_indices(vec![0])
            .value_indices(vec![2, 3])
            .next_column_id(1024)
            .build()
            .unwrap();
        let table_info = TableInfoBuilder::default()
            .name(table_name)
            .meta(table_meta)
            .build()
            .unwrap();
        let table = EmptyTable::from_table_info(&table_info);

        assert!(
            catalog_list
                .register_table_sync(RegisterTableRequest {
                    catalog: DEFAULT_CATALOG_NAME.to_string(),
                    schema: DEFAULT_SCHEMA_NAME.to_string(),
                    table_name: table_name.to_string(),
                    table_id: 1024,
                    table,
                })
                .is_ok()
        );

        DfTableSourceProvider::new(
            catalog_list,
            false,
            QueryContext::arc(),
            DummyDecoder::arc(),
            false,
        )
    }

    async fn build_test_mixed_native_histogram_table_provider(
        table_name: &str,
    ) -> DfTableSourceProvider {
        build_test_mixed_native_histogram_table_provider_with_marker(table_name, false).await
    }

    async fn build_test_mixed_native_histogram_table_provider_with_marker(
        table_name: &str,
        temporality_marker: bool,
    ) -> DfTableSourceProvider {
        let catalog_list = MemoryCatalogManager::with_default_setup();
        let mut columns = vec![ColumnSchema::new(
            "tag_0".to_string(),
            ConcreteDataType::string_datatype(),
            false,
        )];
        if temporality_marker {
            columns.push(ColumnSchema::new(
                OTLP_AGGREGATION_TEMPORALITY_LABEL.to_string(),
                ConcreteDataType::string_datatype(),
                true,
            ));
        }
        let tag_count = columns.len();
        columns.extend([
            ColumnSchema::new(
                "timestamp".to_string(),
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            )
            .with_time_index(true),
            ColumnSchema::new(
                greptime_native_histogram().to_string(),
                native_histogram_value_type().clone(),
                true,
            ),
            ColumnSchema::new(
                greptime_value().to_string(),
                ConcreteDataType::float64_datatype(),
                true,
            ),
        ]);
        let schema = Arc::new(Schema::new(columns));
        let table_meta = TableMetaBuilder::empty()
            .schema(schema.clone())
            .primary_key_indices((0..tag_count).collect())
            .value_indices(vec![tag_count + 1, tag_count + 2])
            .next_column_id(1024)
            .build()
            .unwrap();
        let table_info = Arc::new(
            TableInfoBuilder::default()
                .name(table_name)
                .meta(table_meta)
                .build()
                .unwrap(),
        );
        let mut arrays: Vec<Arc<dyn Array>> =
            vec![Arc::new(StringArray::from(vec!["float", "histogram"]))];
        if temporality_marker {
            arrays.push(Arc::new(StringArray::from(vec![
                Some(GREPTIME_TEMPORALITY_DELTA),
                Some(GREPTIME_TEMPORALITY_DELTA),
            ])));
        }
        arrays.extend([
            Arc::new(TimestampMillisecondArray::from(vec![1_000, 1_000])) as Arc<dyn Array>,
            build_histogram_array(&[None, Some(direct_or_histogram())]),
            Arc::new(Float64Array::from(vec![Some(2.0), None])),
        ]);
        let batch = RecordBatch::try_new(schema.arrow_schema().clone(), arrays).unwrap();
        let backing = GreptimeMemTable::new_with_catalog(
            table_name,
            GreptimeRecordBatch::from_df_record_batch(schema, batch),
            1024,
            DEFAULT_CATALOG_NAME.to_string(),
            DEFAULT_SCHEMA_NAME.to_string(),
        );
        let table = Arc::new(Table::new(
            table_info,
            FilterPushDownType::Unsupported,
            backing.data_source(),
        ));

        assert!(
            catalog_list
                .register_table_sync(RegisterTableRequest {
                    catalog: DEFAULT_CATALOG_NAME.to_string(),
                    schema: DEFAULT_SCHEMA_NAME.to_string(),
                    table_name: table_name.to_string(),
                    table_id: 1024,
                    table,
                })
                .is_ok()
        );

        DfTableSourceProvider::new(
            catalog_list,
            false,
            QueryContext::arc(),
            DummyDecoder::arc(),
            false,
        )
    }

    fn classic_and_native_histogram_table_provider(
        native_tag: &str,
        native_le: Option<&str>,
        native_histogram: NativeHistogram,
    ) -> DfTableSourceProvider {
        let table_name = "mixed_histogram";
        let catalog = MemoryCatalogManager::with_default_setup();
        let schema = Arc::new(Schema::new(vec![
            ColumnSchema::new(
                "tag".to_string(),
                ConcreteDataType::string_datatype(),
                false,
            ),
            ColumnSchema::new(
                LE_COLUMN_NAME.to_string(),
                ConcreteDataType::string_datatype(),
                true,
            ),
            ColumnSchema::new(
                "timestamp".to_string(),
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            )
            .with_time_index(true),
            ColumnSchema::new(
                greptime_native_histogram().to_string(),
                native_histogram_value_type().clone(),
                true,
            ),
            ColumnSchema::new(
                greptime_value().to_string(),
                ConcreteDataType::float64_datatype(),
                true,
            ),
        ]));
        let table_meta = TableMetaBuilder::empty()
            .schema(schema.clone())
            .primary_key_indices(vec![0, 1])
            .value_indices(vec![3, 4])
            .next_column_id(5)
            .build()
            .unwrap();
        let table_info = Arc::new(
            TableInfoBuilder::default()
                .name(table_name)
                .meta(table_meta)
                .build()
                .unwrap(),
        );
        let batch = RecordBatch::try_new(
            schema.arrow_schema().clone(),
            vec![
                Arc::new(StringArray::from(vec![
                    "classic", "classic", native_tag, "classic", "classic", native_tag,
                ])),
                Arc::new(StringArray::from(vec![
                    Some("1"),
                    Some("+Inf"),
                    native_le,
                    Some("1"),
                    Some("+Inf"),
                    native_le,
                ])),
                Arc::new(TimestampMillisecondArray::from(vec![
                    1_000, 1_000, 1_000, 2_000, 2_000, 2_000,
                ])),
                build_histogram_array(&[
                    None,
                    None,
                    Some(native_histogram.clone()),
                    None,
                    None,
                    Some(native_histogram),
                ]),
                Arc::new(Float64Array::from(vec![
                    Some(2.0),
                    Some(4.0),
                    None,
                    Some(2.0),
                    Some(4.0),
                    None,
                ])),
            ],
        )
        .unwrap();
        let backing = GreptimeMemTable::new_with_catalog(
            table_name,
            GreptimeRecordBatch::from_df_record_batch(schema, batch),
            2_200,
            DEFAULT_CATALOG_NAME.to_string(),
            DEFAULT_SCHEMA_NAME.to_string(),
        );
        let table = Arc::new(Table::new(
            table_info,
            FilterPushDownType::Unsupported,
            backing.data_source(),
        ));
        catalog
            .register_table_sync(RegisterTableRequest {
                catalog: DEFAULT_CATALOG_NAME.to_string(),
                schema: DEFAULT_SCHEMA_NAME.to_string(),
                table_name: table_name.to_string(),
                table_id: 2_200,
                table,
            })
            .unwrap();

        DfTableSourceProvider::new(
            catalog,
            false,
            QueryContext::arc(),
            DummyDecoder::arc(),
            false,
        )
    }

    async fn build_test_table_provider_with_tsid(
        table_name_tuples: &[(String, String)],
        num_tag: usize,
        num_field: usize,
    ) -> DfTableSourceProvider {
        let table_specs = table_name_tuples
            .iter()
            .map(|(schema_name, table_name)| ((schema_name.clone(), table_name.clone()), num_field))
            .collect::<Vec<_>>();
        build_test_table_provider_with_tsid_fields(&table_specs, num_tag).await
    }

    async fn build_test_table_provider_with_tsid_fields(
        table_specs: &[((String, String), usize)],
        num_tag: usize,
    ) -> DfTableSourceProvider {
        let table_specs = table_specs
            .iter()
            .map(|(table_name_tuple, num_field)| (table_name_tuple.clone(), num_tag, *num_field))
            .collect::<Vec<_>>();
        build_test_table_provider_with_tsid_tag_fields(&table_specs).await
    }

    async fn build_test_table_provider_with_tsid_tag_fields(
        table_specs: &[((String, String), usize, usize)],
    ) -> DfTableSourceProvider {
        let catalog_list = MemoryCatalogManager::with_default_setup();

        let physical_table_name = "phy";
        let physical_table_id = 999u32;
        let physical_num_tag = table_specs
            .iter()
            .map(|(_, num_tag, _)| *num_tag)
            .max()
            .unwrap_or(0);
        let physical_num_field = table_specs
            .iter()
            .map(|(_, _, num_field)| *num_field)
            .max()
            .unwrap_or(0);

        // Register a metric engine physical table with internal columns.
        {
            let mut columns = vec![
                ColumnSchema::new(
                    DATA_SCHEMA_TABLE_ID_COLUMN_NAME.to_string(),
                    ConcreteDataType::uint32_datatype(),
                    false,
                ),
                ColumnSchema::new(
                    DATA_SCHEMA_TSID_COLUMN_NAME.to_string(),
                    ConcreteDataType::uint64_datatype(),
                    false,
                ),
            ];
            for i in 0..physical_num_tag {
                columns.push(ColumnSchema::new(
                    format!("tag_{i}"),
                    ConcreteDataType::string_datatype(),
                    false,
                ));
            }
            columns.push(
                ColumnSchema::new(
                    "timestamp".to_string(),
                    ConcreteDataType::timestamp_millisecond_datatype(),
                    false,
                )
                .with_time_index(true),
            );
            for i in 0..physical_num_field {
                columns.push(ColumnSchema::new(
                    format!("field_{i}"),
                    ConcreteDataType::float64_datatype(),
                    true,
                ));
            }

            let schema = Arc::new(Schema::new(columns));
            let primary_key_indices = (0..(2 + physical_num_tag)).collect::<Vec<_>>();
            let table_meta = TableMetaBuilder::empty()
                .schema(schema)
                .primary_key_indices(primary_key_indices)
                .value_indices(
                    (2 + physical_num_tag..2 + physical_num_tag + 1 + physical_num_field).collect(),
                )
                .engine(METRIC_ENGINE_NAME.to_string())
                .next_column_id(1024)
                .build()
                .unwrap();
            let table_info = TableInfoBuilder::default()
                .table_id(physical_table_id)
                .name(physical_table_name)
                .meta(table_meta)
                .build()
                .unwrap();
            let table = EmptyTable::from_table_info(&table_info);

            assert!(
                catalog_list
                    .register_table_sync(RegisterTableRequest {
                        catalog: DEFAULT_CATALOG_NAME.to_string(),
                        schema: DEFAULT_SCHEMA_NAME.to_string(),
                        table_name: physical_table_name.to_string(),
                        table_id: physical_table_id,
                        table,
                    })
                    .is_ok()
            );
        }

        // Register metric engine logical tables without `__tsid`, referencing the physical table.
        for (idx, ((schema_name, table_name), num_tag, num_field)) in table_specs.iter().enumerate()
        {
            let mut columns = vec![];
            for i in 0..*num_tag {
                columns.push(ColumnSchema::new(
                    format!("tag_{i}"),
                    ConcreteDataType::string_datatype(),
                    false,
                ));
            }
            columns.push(
                ColumnSchema::new(
                    "timestamp".to_string(),
                    ConcreteDataType::timestamp_millisecond_datatype(),
                    false,
                )
                .with_time_index(true),
            );
            for i in 0..*num_field {
                columns.push(ColumnSchema::new(
                    format!("field_{i}"),
                    ConcreteDataType::float64_datatype(),
                    true,
                ));
            }

            let schema = Arc::new(Schema::new(columns));
            let mut options = table::requests::TableOptions::default();
            options.extra_options.insert(
                LOGICAL_TABLE_METADATA_KEY.to_string(),
                physical_table_name.to_string(),
            );
            let table_id = 1024u32 + idx as u32;
            let table_meta = TableMetaBuilder::empty()
                .schema(schema)
                .primary_key_indices((0..*num_tag).collect())
                .value_indices((*num_tag + 1..*num_tag + 1 + *num_field).collect())
                .engine(METRIC_ENGINE_NAME.to_string())
                .options(options)
                .next_column_id(1024)
                .build()
                .unwrap();
            let table_info = TableInfoBuilder::default()
                .table_id(table_id)
                .name(table_name.clone())
                .meta(table_meta)
                .build()
                .unwrap();
            let table = EmptyTable::from_table_info(&table_info);

            assert!(
                catalog_list
                    .register_table_sync(RegisterTableRequest {
                        catalog: DEFAULT_CATALOG_NAME.to_string(),
                        schema: schema_name.clone(),
                        table_name: table_name.clone(),
                        table_id,
                        table,
                    })
                    .is_ok()
            );
        }

        DfTableSourceProvider::new(
            catalog_list,
            false,
            QueryContext::arc(),
            DummyDecoder::arc(),
            false,
        )
    }

    async fn build_test_table_provider_with_fields(
        table_name_tuples: &[(String, String)],
        tags: &[&str],
    ) -> DfTableSourceProvider {
        let catalog_list = MemoryCatalogManager::with_default_setup();
        for (schema_name, table_name) in table_name_tuples {
            let mut columns = vec![];
            let num_tag = tags.len();
            for tag in tags {
                columns.push(ColumnSchema::new(
                    tag.to_string(),
                    ConcreteDataType::string_datatype(),
                    false,
                ));
            }
            columns.push(
                ColumnSchema::new(
                    greptime_timestamp().to_string(),
                    ConcreteDataType::timestamp_millisecond_datatype(),
                    false,
                )
                .with_time_index(true),
            );
            columns.push(ColumnSchema::new(
                greptime_value().to_string(),
                ConcreteDataType::float64_datatype(),
                true,
            ));
            let schema = Arc::new(Schema::new(columns));
            let table_meta = TableMetaBuilder::empty()
                .schema(schema)
                .primary_key_indices((0..num_tag).collect())
                .next_column_id(1024)
                .build()
                .unwrap();
            let table_info = TableInfoBuilder::default()
                .name(table_name.clone())
                .meta(table_meta)
                .build()
                .unwrap();
            let table = EmptyTable::from_table_info(&table_info);

            assert!(
                catalog_list
                    .register_table_sync(RegisterTableRequest {
                        catalog: DEFAULT_CATALOG_NAME.to_string(),
                        schema: schema_name.clone(),
                        table_name: table_name.clone(),
                        table_id: 1024,
                        table,
                    })
                    .is_ok()
            );
        }

        DfTableSourceProvider::new(
            catalog_list,
            false,
            QueryContext::arc(),
            DummyDecoder::arc(),
            false,
        )
    }

    // {
    //     input: `abs(some_metric{foo!="bar"})`,
    //     expected: &Call{
    //         Func: MustGetFunction("abs"),
    //         Args: Expressions{
    //             &VectorSelector{
    //                 Name: "some_metric",
    //                 LabelMatchers: []*labels.Matcher{
    //                     MustLabelMatcher(labels.MatchNotEqual, "foo", "bar"),
    //                     MustLabelMatcher(labels.MatchEqual, model.MetricNameLabel, "some_metric"),
    //                 },
    //             },
    //         },
    //     },
    // },
    async fn do_single_instant_function_call(fn_name: &'static str, plan_name: &str) {
        let prom_expr =
            parser::parse(&format!("{fn_name}(some_metric{{tag_0!=\"bar\"}})")).unwrap();
        let eval_stmt = EvalStmt {
            expr: prom_expr,
            start: UNIX_EPOCH,
            end: UNIX_EPOCH
                .checked_add(Duration::from_secs(100_000))
                .unwrap(),
            interval: Duration::from_secs(5),
            lookback_delta: Duration::from_secs(1),
        };

        let table_provider = build_test_table_provider(
            &[(DEFAULT_SCHEMA_NAME.to_string(), "some_metric".to_string())],
            1,
            1,
        )
        .await;
        let plan =
            PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &build_query_engine_state())
                .await
                .unwrap();

        let expected = String::from(
            "Filter: TEMPLATE(field_0) IS NOT NULL [timestamp:Timestamp(ms), TEMPLATE(field_0):Float64;N, tag_0:Utf8]\
            \n  Projection: some_metric.timestamp, TEMPLATE(some_metric.field_0) AS TEMPLATE(field_0), some_metric.tag_0 [timestamp:Timestamp(ms), TEMPLATE(field_0):Float64;N, tag_0:Utf8]\
            \n    PromInstantManipulate: range=[0..100000000], lookback=[1000], interval=[5000], time index=[timestamp] [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n      PromSeriesDivide: tags=[\"tag_0\"] [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n        Sort: some_metric.tag_0 ASC NULLS FIRST, some_metric.timestamp ASC NULLS FIRST [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
	            \n          Filter: some_metric.tag_0 != Utf8(\"bar\") AND some_metric.timestamp >= TimestampMillisecond(-999, None) AND some_metric.timestamp <= TimestampMillisecond(100000000, None) [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n            TableScan: some_metric [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]"
        ).replace("TEMPLATE", plan_name);

        assert_eq!(plan.display_indent_schema().to_string(), expected);
    }

    #[tokio::test]
    async fn single_abs() {
        do_single_instant_function_call("abs", "abs").await;
    }

    #[tokio::test]
    #[should_panic]
    async fn single_absent() {
        do_single_instant_function_call("absent", "").await;
    }

    #[tokio::test]
    async fn single_ceil() {
        do_single_instant_function_call("ceil", "ceil").await;
    }

    #[tokio::test]
    async fn single_exp() {
        do_single_instant_function_call("exp", "exp").await;
    }

    #[tokio::test]
    async fn single_ln() {
        do_single_instant_function_call("ln", "ln").await;
    }

    #[tokio::test]
    async fn single_log2() {
        do_single_instant_function_call("log2", "log2").await;
    }

    #[tokio::test]
    async fn single_log10() {
        do_single_instant_function_call("log10", "log10").await;
    }

    #[tokio::test]
    #[should_panic]
    async fn single_scalar() {
        do_single_instant_function_call("scalar", "").await;
    }

    #[tokio::test]
    #[should_panic]
    async fn single_sgn() {
        do_single_instant_function_call("sgn", "").await;
    }

    #[tokio::test]
    #[should_panic]
    async fn single_sort() {
        do_single_instant_function_call("sort", "").await;
    }

    #[tokio::test]
    #[should_panic]
    async fn single_sort_desc() {
        do_single_instant_function_call("sort_desc", "").await;
    }

    #[tokio::test]
    async fn single_sqrt() {
        do_single_instant_function_call("sqrt", "sqrt").await;
    }

    #[tokio::test]
    async fn single_timestamp_plan_preserves_source_value() {
        let eval_stmt = build_eval_stmt(r#"timestamp(some_metric{tag_0!="bar"})"#);
        let table_provider = build_test_table_provider(
            &[(DEFAULT_SCHEMA_NAME.to_string(), "some_metric".to_string())],
            1,
            1,
        )
        .await;
        let plan =
            PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &build_query_engine_state())
                .await
                .unwrap();

        let expected = String::from(
            "Filter: value IS NOT NULL [timestamp:Timestamp(ms), value:Float64, tag_0:Utf8]\
            \n  Projection: some_metric.timestamp, value AS value, some_metric.tag_0 [timestamp:Timestamp(ms), value:Float64, tag_0:Utf8]\
            \n    Projection: some_metric.timestamp, __promql_timestamp_value_ AS value, some_metric.tag_0 [timestamp:Timestamp(ms), value:Float64, tag_0:Utf8]\
            \n      PromInstantManipulate: range=[0..100000000], lookback=[1000], interval=[5000], time index=[timestamp] [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N, __promql_timestamp_value_:Float64]\
            \n        Projection: some_metric.tag_0, some_metric.timestamp, some_metric.field_0, CAST(CAST(some_metric.timestamp AS Int64) AS Float64) / Float64(1000) AS __promql_timestamp_value_ [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N, __promql_timestamp_value_:Float64]\
            \n          PromSeriesDivide: tags=[\"tag_0\"] [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n            Sort: some_metric.tag_0 ASC NULLS FIRST, some_metric.timestamp ASC NULLS FIRST [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n              Filter: some_metric.tag_0 != Utf8(\"bar\") AND some_metric.timestamp >= TimestampMillisecond(-999, None) AND some_metric.timestamp <= TimestampMillisecond(100000000, None) [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n                TableScan: some_metric [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]",
        );

        assert_eq!(plan.display_indent_schema().to_string(), expected);
    }

    #[tokio::test]
    async fn single_acos() {
        do_single_instant_function_call("acos", "acos").await;
    }

    #[tokio::test]
    #[should_panic]
    async fn single_acosh() {
        do_single_instant_function_call("acosh", "").await;
    }

    #[tokio::test]
    async fn single_asin() {
        do_single_instant_function_call("asin", "asin").await;
    }

    #[tokio::test]
    #[should_panic]
    async fn single_asinh() {
        do_single_instant_function_call("asinh", "").await;
    }

    #[tokio::test]
    async fn single_atan() {
        do_single_instant_function_call("atan", "atan").await;
    }

    #[tokio::test]
    #[should_panic]
    async fn single_atanh() {
        do_single_instant_function_call("atanh", "").await;
    }

    #[tokio::test]
    async fn single_cos() {
        do_single_instant_function_call("cos", "cos").await;
    }

    #[tokio::test]
    #[should_panic]
    async fn single_cosh() {
        do_single_instant_function_call("cosh", "").await;
    }

    #[tokio::test]
    async fn single_sin() {
        do_single_instant_function_call("sin", "sin").await;
    }

    #[tokio::test]
    #[should_panic]
    async fn single_sinh() {
        do_single_instant_function_call("sinh", "").await;
    }

    #[tokio::test]
    async fn single_tan() {
        do_single_instant_function_call("tan", "tan").await;
    }

    #[tokio::test]
    #[should_panic]
    async fn single_tanh() {
        do_single_instant_function_call("tanh", "").await;
    }

    #[tokio::test]
    #[should_panic]
    async fn single_deg() {
        do_single_instant_function_call("deg", "").await;
    }

    #[tokio::test]
    #[should_panic]
    async fn single_rad() {
        do_single_instant_function_call("rad", "").await;
    }

    // {
    //     input: "avg by (foo)(some_metric)",
    //     expected: &AggregateExpr{
    //         Op: AVG,
    //         Expr: &VectorSelector{
    //             Name: "some_metric",
    //             LabelMatchers: []*labels.Matcher{
    //                 MustLabelMatcher(labels.MatchEqual, model.MetricNameLabel, "some_metric"),
    //             },
    //             PosRange: PositionRange{
    //                 Start: 13,
    //                 End:   24,
    //             },
    //         },
    //         Grouping: []string{"foo"},
    //         PosRange: PositionRange{
    //             Start: 0,
    //             End:   25,
    //         },
    //     },
    // },
    async fn do_aggregate_expr_plan(fn_name: &str, plan_name: &str) {
        let prom_expr = parser::parse(&format!(
            "{fn_name} by (tag_1)(some_metric{{tag_0!=\"bar\"}})",
        ))
        .unwrap();
        let mut eval_stmt = EvalStmt {
            expr: prom_expr,
            start: UNIX_EPOCH,
            end: UNIX_EPOCH
                .checked_add(Duration::from_secs(100_000))
                .unwrap(),
            interval: Duration::from_secs(5),
            lookback_delta: Duration::from_secs(1),
        };

        // test group by
        let table_provider = build_test_table_provider(
            &[(DEFAULT_SCHEMA_NAME.to_string(), "some_metric".to_string())],
            2,
            2,
        )
        .await;
        let plan =
            PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &build_query_engine_state())
                .await
                .unwrap();
        let expected_no_without = String::from(
            "Sort: some_metric.tag_1 ASC NULLS LAST, some_metric.timestamp ASC NULLS LAST [tag_1:Utf8, timestamp:Timestamp(ms), TEMPLATE(some_metric.field_0):Float64;N, TEMPLATE(some_metric.field_1):Float64;N]\
            \n  Aggregate: groupBy=[[some_metric.tag_1, some_metric.timestamp]], aggr=[[TEMPLATE(some_metric.field_0), TEMPLATE(some_metric.field_1)]] [tag_1:Utf8, timestamp:Timestamp(ms), TEMPLATE(some_metric.field_0):Float64;N, TEMPLATE(some_metric.field_1):Float64;N]\
            \n    PromInstantManipulate: range=[0..100000000], lookback=[1000], interval=[5000], time index=[timestamp] [tag_0:Utf8, tag_1:Utf8, timestamp:Timestamp(ms), field_0:Float64;N, field_1:Float64;N]\
            \n      PromSeriesDivide: tags=[\"tag_0\", \"tag_1\"] [tag_0:Utf8, tag_1:Utf8, timestamp:Timestamp(ms), field_0:Float64;N, field_1:Float64;N]\
            \n        Sort: some_metric.tag_0 ASC NULLS FIRST, some_metric.tag_1 ASC NULLS FIRST, some_metric.timestamp ASC NULLS FIRST [tag_0:Utf8, tag_1:Utf8, timestamp:Timestamp(ms), field_0:Float64;N, field_1:Float64;N]\
            \n          Filter: some_metric.tag_0 != Utf8(\"bar\") AND some_metric.timestamp >= TimestampMillisecond(-999, None) AND some_metric.timestamp <= TimestampMillisecond(100000000, None) [tag_0:Utf8, tag_1:Utf8, timestamp:Timestamp(ms), field_0:Float64;N, field_1:Float64;N]\
            \n            TableScan: some_metric [tag_0:Utf8, tag_1:Utf8, timestamp:Timestamp(ms), field_0:Float64;N, field_1:Float64;N]"
        ).replace("TEMPLATE", plan_name);
        assert_eq!(
            plan.display_indent_schema().to_string(),
            expected_no_without
        );

        // test group without
        if let PromExpr::Aggregate(AggregateExpr { modifier, .. }) = &mut eval_stmt.expr {
            *modifier = Some(LabelModifier::Exclude(Labels {
                labels: vec![String::from("tag_1")].into_iter().collect(),
            }));
        }
        let table_provider = build_test_table_provider(
            &[(DEFAULT_SCHEMA_NAME.to_string(), "some_metric".to_string())],
            2,
            2,
        )
        .await;
        let plan =
            PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &build_query_engine_state())
                .await
                .unwrap();
        let expected_without = String::from(
            "Sort: some_metric.tag_0 ASC NULLS LAST, some_metric.timestamp ASC NULLS LAST [tag_0:Utf8, timestamp:Timestamp(ms), TEMPLATE(some_metric.field_0):Float64;N, TEMPLATE(some_metric.field_1):Float64;N]\
            \n  Aggregate: groupBy=[[some_metric.tag_0, some_metric.timestamp]], aggr=[[TEMPLATE(some_metric.field_0), TEMPLATE(some_metric.field_1)]] [tag_0:Utf8, timestamp:Timestamp(ms), TEMPLATE(some_metric.field_0):Float64;N, TEMPLATE(some_metric.field_1):Float64;N]\
            \n    PromInstantManipulate: range=[0..100000000], lookback=[1000], interval=[5000], time index=[timestamp] [tag_0:Utf8, tag_1:Utf8, timestamp:Timestamp(ms), field_0:Float64;N, field_1:Float64;N]\
            \n      PromSeriesDivide: tags=[\"tag_0\", \"tag_1\"] [tag_0:Utf8, tag_1:Utf8, timestamp:Timestamp(ms), field_0:Float64;N, field_1:Float64;N]\
            \n        Sort: some_metric.tag_0 ASC NULLS FIRST, some_metric.tag_1 ASC NULLS FIRST, some_metric.timestamp ASC NULLS FIRST [tag_0:Utf8, tag_1:Utf8, timestamp:Timestamp(ms), field_0:Float64;N, field_1:Float64;N]\
            \n          Filter: some_metric.tag_0 != Utf8(\"bar\") AND some_metric.timestamp >= TimestampMillisecond(-999, None) AND some_metric.timestamp <= TimestampMillisecond(100000000, None) [tag_0:Utf8, tag_1:Utf8, timestamp:Timestamp(ms), field_0:Float64;N, field_1:Float64;N]\
            \n            TableScan: some_metric [tag_0:Utf8, tag_1:Utf8, timestamp:Timestamp(ms), field_0:Float64;N, field_1:Float64;N]"
        ).replace("TEMPLATE", plan_name);
        assert_eq!(plan.display_indent_schema().to_string(), expected_without);
    }

    #[tokio::test]
    async fn aggregate_sum() {
        do_aggregate_expr_plan("sum", "sum").await;
    }

    #[tokio::test]
    async fn tsid_is_used_for_series_divide_when_available() {
        let prom_expr = parser::parse("some_metric").unwrap();
        let eval_stmt = EvalStmt {
            expr: prom_expr,
            start: UNIX_EPOCH,
            end: UNIX_EPOCH
                .checked_add(Duration::from_secs(100_000))
                .unwrap(),
            interval: Duration::from_secs(5),
            lookback_delta: Duration::from_secs(1),
        };

        let table_provider = build_test_table_provider_with_tsid(
            &[(DEFAULT_SCHEMA_NAME.to_string(), "some_metric".to_string())],
            1,
            1,
        )
        .await;
        let plan =
            PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &build_query_engine_state())
                .await
                .unwrap();

        let plan_str = plan.display_indent_schema().to_string();
        assert!(plan_str.contains("PromSeriesDivide: tags=[\"__tsid\"]"));
        assert!(plan_str.contains("__tsid ASC NULLS FIRST"));
        assert!(
            !plan
                .schema()
                .fields()
                .iter()
                .any(|field| field.name() == DATA_SCHEMA_TSID_COLUMN_NAME)
        );

        let manipulate = find_instant_manipulate(&plan).unwrap();
        let exec = manipulate.to_execution_plan(Arc::new(DataSourceExec::new(Arc::new(
            MemorySourceConfig::try_new(&[], Arc::new(ArrowSchema::empty()), None).unwrap(),
        ))));
        assert!(format!("{exec:?}").contains("reuse_tsid_column: true"));
    }

    #[tokio::test]
    async fn default_binary_join_uses_tsid_when_available() {
        let eval_stmt = build_eval_stmt("some_metric / some_alt_metric");

        let table_provider = build_test_table_provider_with_tsid(
            &[
                (DEFAULT_SCHEMA_NAME.to_string(), "some_metric".to_string()),
                (
                    DEFAULT_SCHEMA_NAME.to_string(),
                    "some_alt_metric".to_string(),
                ),
            ],
            1,
            1,
        )
        .await;
        let plan =
            PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &build_query_engine_state())
                .await
                .unwrap();

        let plan_str = plan.display_indent_schema().to_string();
        assert!(
            plan_str.contains("some_metric.__tsid = some_alt_metric.__tsid"),
            "{plan_str}"
        );
        assert!(
            !plan_str.contains("some_metric.tag_0 = some_alt_metric.tag_0"),
            "{plan_str}"
        );
    }

    #[tokio::test]
    async fn reject_binary_fill_modifiers() {
        let state = build_query_engine_state();

        for query in [
            "some_metric + fill(0) some_alt_metric",
            "some_metric + fill_left(0) some_alt_metric",
            "some_metric + fill_right(0) some_alt_metric",
            "(some_metric + fill(0) some_alt_metric) + some_metric",
        ] {
            let eval_stmt = build_eval_stmt(query);
            let table_provider = build_test_table_provider(&[], 0, 0).await;
            let err = PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &state)
                .await
                .unwrap_err();

            assert!(
                matches!(
                    &err,
                    crate::promql::error::Error::UnsupportedExpr { name, .. }
                        if name == "PromQL fill modifiers"
                ),
                "{err}"
            );
        }
    }

    #[tokio::test]
    async fn timestamp_binary_join_falls_back_when_tsid_is_projected_out() {
        for query in [
            "timestamp(some_metric) / some_metric",
            "some_metric / timestamp(some_metric)",
        ] {
            let eval_stmt = build_eval_stmt(query);

            let table_provider = build_test_table_provider_with_tsid(
                &[(DEFAULT_SCHEMA_NAME.to_string(), "some_metric".to_string())],
                1,
                1,
            )
            .await;
            let plan =
                PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &build_query_engine_state())
                    .await
                    .unwrap();

            let plan_str = plan.display_indent_schema().to_string();
            assert!(!plan_str.contains("__tsid ="), "{query}: {plan_str}");
            assert!(
                plan_str.contains("lhs.tag_0 = rhs.tag_0"),
                "{query}: {plan_str}"
            );
            assert!(
                !plan
                    .schema()
                    .fields()
                    .iter()
                    .any(|field| field.name() == DATA_SCHEMA_TSID_COLUMN_NAME),
                "{query}: {plan_str}"
            );
        }
    }

    #[tokio::test]
    async fn timestamp_binary_join_rejects_default_matching_on_mismatched_labels() {
        let eval_stmt = build_eval_stmt("timestamp(left_host_job) / right_by_job");

        let table_provider = build_test_table_provider_with_tsid_tag_fields(&[
            (
                (DEFAULT_SCHEMA_NAME.to_string(), "left_host_job".to_string()),
                2,
                1,
            ),
            (
                (DEFAULT_SCHEMA_NAME.to_string(), "right_by_job".to_string()),
                1,
                1,
            ),
        ])
        .await;
        let plan =
            PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &build_query_engine_state())
                .await
                .unwrap();
        let plan_str = plan.display_indent_schema().to_string();

        assert!(
            plan_str.contains("Boolean(false)") || plan_str.contains("false"),
            "{plan_str}"
        );
    }

    #[tokio::test]
    async fn tsid_is_preserved_for_nested_default_binary_joins() {
        let eval_stmt = build_eval_stmt("(some_metric - some_alt_metric) / some_third_metric");

        let table_provider = build_test_table_provider_with_tsid(
            &[
                (DEFAULT_SCHEMA_NAME.to_string(), "some_metric".to_string()),
                (
                    DEFAULT_SCHEMA_NAME.to_string(),
                    "some_alt_metric".to_string(),
                ),
                (
                    DEFAULT_SCHEMA_NAME.to_string(),
                    "some_third_metric".to_string(),
                ),
            ],
            1,
            1,
        )
        .await;
        let plan =
            PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &build_query_engine_state())
                .await
                .unwrap();

        let plan_str = plan.display_indent_schema().to_string();
        assert_eq!(plan_str.matches("__tsid =").count(), 2, "{plan_str}");
        assert!(!plan_str.contains("tag_0 ="), "{plan_str}");
    }

    #[tokio::test]
    async fn repeated_tsid_binary_operand_reuses_leaf_plan() {
        let eval_stmt = build_eval_stmt("((some_metric - some_alt_metric) / some_metric) * 100");

        let table_provider = build_test_table_provider_with_tsid(
            &[
                (DEFAULT_SCHEMA_NAME.to_string(), "some_metric".to_string()),
                (
                    DEFAULT_SCHEMA_NAME.to_string(),
                    "some_alt_metric".to_string(),
                ),
            ],
            1,
            1,
        )
        .await;
        let plan =
            PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &build_query_engine_state())
                .await
                .unwrap();

        let plan_str = plan.display_indent_schema().to_string();
        assert_eq!(plan_str.matches("__tsid =").count(), 1, "{plan_str}");
        assert_eq!(
            plan_str
                .matches("Filter: phy.__table_id = UInt32(1024)")
                .count(),
            1,
            "{plan_str}"
        );
        assert_eq!(
            plan_str.matches("PromInstantManipulate").count(),
            2,
            "{plan_str}"
        );
        assert!(!plan_str.contains("tag_0 ="), "{plan_str}");
    }

    #[tokio::test]
    async fn repeated_tsid_binary_operand_reuses_shorter_field_side() {
        let eval_stmt =
            build_eval_stmt("((two_field_metric - one_field_metric) / one_field_metric) * 100");

        let table_provider = build_test_table_provider_with_tsid_fields(
            &[
                (
                    (
                        DEFAULT_SCHEMA_NAME.to_string(),
                        "two_field_metric".to_string(),
                    ),
                    2,
                ),
                (
                    (
                        DEFAULT_SCHEMA_NAME.to_string(),
                        "one_field_metric".to_string(),
                    ),
                    1,
                ),
            ],
            1,
        )
        .await;
        let plan =
            PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &build_query_engine_state())
                .await
                .unwrap();

        let field_names = plan
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect::<Vec<_>>();
        let value_columns = field_names
            .iter()
            .filter(|name| {
                *name != "tag_0" && *name != "timestamp" && *name != DATA_SCHEMA_TSID_COLUMN_NAME
            })
            .count();
        assert_eq!(value_columns, 1, "{field_names:?}");
        let plan_str = plan.display_indent_schema().to_string();
        assert_eq!(plan_str.matches("__tsid =").count(), 1, "{plan_str}");
        assert_eq!(
            plan_str
                .matches("Filter: phy.__table_id = UInt32(1025)")
                .count(),
            1,
            "{plan_str}"
        );
        assert!(!plan_str.contains("tag_0 ="), "{plan_str}");
    }

    #[tokio::test]
    async fn binary_island_reuses_self_operand_without_join() {
        let eval_stmt = build_eval_stmt("some_metric / some_metric");

        let table_provider = build_test_table_provider_with_tsid(
            &[(DEFAULT_SCHEMA_NAME.to_string(), "some_metric".to_string())],
            1,
            1,
        )
        .await;
        let plan =
            PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &build_query_engine_state())
                .await
                .unwrap();

        let plan_str = plan.display_indent_schema().to_string();
        assert_eq!(plan_str.matches("__tsid =").count(), 0, "{plan_str}");
        assert_eq!(
            plan_str
                .matches("Filter: phy.__table_id = UInt32(1024)")
                .count(),
            1,
            "{plan_str}"
        );
        assert_eq!(
            plan_str.matches("PromInstantManipulate").count(),
            1,
            "{plan_str}"
        );
    }

    #[tokio::test]
    async fn binary_island_reuses_leaf_across_two_branches() {
        let eval_stmt =
            build_eval_stmt("(some_metric + some_alt_metric) / (some_metric + third_metric)");

        let table_provider = build_test_table_provider_with_tsid(
            &[
                (DEFAULT_SCHEMA_NAME.to_string(), "some_metric".to_string()),
                (
                    DEFAULT_SCHEMA_NAME.to_string(),
                    "some_alt_metric".to_string(),
                ),
                (DEFAULT_SCHEMA_NAME.to_string(), "third_metric".to_string()),
            ],
            1,
            1,
        )
        .await;
        let plan =
            PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &build_query_engine_state())
                .await
                .unwrap();

        let plan_str = plan.display_indent_schema().to_string();
        assert_eq!(plan_str.matches("__tsid =").count(), 2, "{plan_str}");
        assert_eq!(
            plan_str
                .matches("Filter: phy.__table_id = UInt32(1024)")
                .count(),
            1,
            "{plan_str}"
        );
        assert_eq!(
            plan_str.matches("PromInstantManipulate").count(),
            3,
            "{plan_str}"
        );
    }

    #[tokio::test]
    async fn binary_island_generated_alias_avoids_user_column_names() {
        let eval_stmt = build_eval_stmt("(some_metric + some_alt_metric) / some_metric");

        let table_provider = build_test_table_provider_with_fields(
            &[
                (DEFAULT_SCHEMA_NAME.to_string(), "some_metric".to_string()),
                (
                    DEFAULT_SCHEMA_NAME.to_string(),
                    "some_alt_metric".to_string(),
                ),
            ],
            &["prom_v0", "__prom_v0"],
        )
        .await;
        let plan =
            PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &build_query_engine_state())
                .await
                .unwrap();

        let field_names = plan.schema().field_names();
        assert!(field_names.iter().any(|name| name.ends_with(".prom_v0")));
        assert!(field_names.iter().any(|name| name.ends_with(".__prom_v0")));

        let plan_str = plan.display_indent_schema().to_string();
        assert!(plan_str.contains("SubqueryAlias: __prom_v0"), "{plan_str}");
        assert_eq!(
            plan_str.matches("PromInstantManipulate").count(),
            2,
            "{plan_str}"
        );
    }

    #[tokio::test]
    async fn binary_island_clears_qualifier_for_nested_unary_projection() {
        let eval_stmt = build_eval_stmt("-((some_metric + some_alt_metric) / some_metric)");

        let table_provider = build_test_table_provider_with_tsid(
            &[
                (DEFAULT_SCHEMA_NAME.to_string(), "some_metric".to_string()),
                (
                    DEFAULT_SCHEMA_NAME.to_string(),
                    "some_alt_metric".to_string(),
                ),
            ],
            1,
            1,
        )
        .await;
        let plan =
            PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &build_query_engine_state())
                .await
                .unwrap();

        let plan_str = plan.display_indent_schema().to_string();
        assert_eq!(plan_str.matches("__tsid =").count(), 1, "{plan_str}");
        assert_eq!(
            plan_str.matches("PromInstantManipulate").count(),
            2,
            "{plan_str}"
        );
    }

    #[tokio::test]
    async fn binary_island_keeps_distinct_matcher_leaves() {
        let eval_stmt = build_eval_stmt(
            "(some_metric{tag_0=\"foo\"} + some_alt_metric) / some_metric{tag_0=\"bar\"}",
        );

        let table_provider = build_test_table_provider_with_tsid(
            &[
                (DEFAULT_SCHEMA_NAME.to_string(), "some_metric".to_string()),
                (
                    DEFAULT_SCHEMA_NAME.to_string(),
                    "some_alt_metric".to_string(),
                ),
            ],
            1,
            1,
        )
        .await;
        let plan =
            PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &build_query_engine_state())
                .await
                .unwrap();

        let plan_str = plan.display_indent_schema().to_string();
        assert_eq!(plan_str.matches("__tsid =").count(), 2, "{plan_str}");
        assert_eq!(
            plan_str.matches("PromInstantManipulate").count(),
            3,
            "{plan_str}"
        );
    }

    #[tokio::test]
    async fn binary_island_keeps_offset_leaves_distinct() {
        let eval_stmt = build_eval_stmt("(some_metric offset 5m + some_alt_metric) / some_metric");

        let table_provider = build_test_table_provider_with_tsid(
            &[
                (DEFAULT_SCHEMA_NAME.to_string(), "some_metric".to_string()),
                (
                    DEFAULT_SCHEMA_NAME.to_string(),
                    "some_alt_metric".to_string(),
                ),
            ],
            1,
            1,
        )
        .await;
        let plan =
            PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &build_query_engine_state())
                .await
                .unwrap();

        let plan_str = plan.display_indent_schema().to_string();
        assert_eq!(plan_str.matches("__tsid =").count(), 2, "{plan_str}");
        assert_eq!(
            plan_str.matches("PromInstantManipulate").count(),
            3,
            "{plan_str}"
        );
    }

    #[tokio::test]
    async fn binary_island_falls_back_for_group_modifier() {
        let eval_stmt = build_eval_stmt(
            "(some_metric + ignoring(tag_0) group_left some_alt_metric) / some_metric",
        );

        let table_provider = build_test_table_provider_with_tsid(
            &[
                (DEFAULT_SCHEMA_NAME.to_string(), "some_metric".to_string()),
                (
                    DEFAULT_SCHEMA_NAME.to_string(),
                    "some_alt_metric".to_string(),
                ),
            ],
            1,
            1,
        )
        .await;
        let plan =
            PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &build_query_engine_state())
                .await
                .unwrap();

        let plan_str = plan.display_indent_schema().to_string();
        assert_eq!(
            plan_str.matches("PromInstantManipulate").count(),
            3,
            "{plan_str}"
        );
    }

    #[tokio::test]
    async fn binary_island_falls_back_for_comparison_filter() {
        let eval_stmt = build_eval_stmt("(some_metric > some_alt_metric) / some_metric");

        let table_provider = build_test_table_provider_with_tsid(
            &[
                (DEFAULT_SCHEMA_NAME.to_string(), "some_metric".to_string()),
                (
                    DEFAULT_SCHEMA_NAME.to_string(),
                    "some_alt_metric".to_string(),
                ),
            ],
            1,
            1,
        )
        .await;
        let plan =
            PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &build_query_engine_state())
                .await
                .unwrap();

        let plan_str = plan.display_indent_schema().to_string();
        assert_eq!(plan_str.matches("__tsid =").count(), 2, "{plan_str}");
        assert_eq!(
            plan_str.matches("PromInstantManipulate").count(),
            3,
            "{plan_str}"
        );
    }

    #[tokio::test]
    async fn tsid_binary_join_uses_shorter_field_side() {
        let eval_stmt = build_eval_stmt("one_field_metric / two_field_metric");

        let table_provider = build_test_table_provider_with_tsid_fields(
            &[
                (
                    (
                        DEFAULT_SCHEMA_NAME.to_string(),
                        "one_field_metric".to_string(),
                    ),
                    1,
                ),
                (
                    (
                        DEFAULT_SCHEMA_NAME.to_string(),
                        "two_field_metric".to_string(),
                    ),
                    2,
                ),
            ],
            1,
        )
        .await;
        let plan =
            PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &build_query_engine_state())
                .await
                .unwrap();

        let field_names = plan
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect::<Vec<_>>();
        let value_columns = field_names
            .iter()
            .filter(|name| {
                *name != "tag_0" && *name != "timestamp" && *name != DATA_SCHEMA_TSID_COLUMN_NAME
            })
            .count();
        assert_eq!(value_columns, 1, "{field_names:?}");
    }

    #[tokio::test]
    async fn comparison_binary_join_uses_shorter_field_side() {
        let eval_stmt = build_eval_stmt("two_field_metric > one_field_metric");

        let table_provider = build_test_table_provider_with_tsid_fields(
            &[
                (
                    (
                        DEFAULT_SCHEMA_NAME.to_string(),
                        "two_field_metric".to_string(),
                    ),
                    2,
                ),
                (
                    (
                        DEFAULT_SCHEMA_NAME.to_string(),
                        "one_field_metric".to_string(),
                    ),
                    1,
                ),
            ],
            1,
        )
        .await;
        let plan =
            PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &build_query_engine_state())
                .await
                .unwrap();

        let field_names = plan
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect::<Vec<_>>();
        assert!(
            field_names.iter().any(|name| name == "field_0"),
            "{field_names:?}"
        );
        assert!(
            !field_names.iter().any(|name| name == "field_1"),
            "{field_names:?}"
        );
    }

    #[tokio::test]
    async fn label_matching_modifier_disables_tsid_binary_join() {
        let eval_stmt = build_eval_stmt("some_metric / ignoring(tag_0) some_alt_metric");

        let table_provider = build_test_table_provider_with_tsid(
            &[
                (DEFAULT_SCHEMA_NAME.to_string(), "some_metric".to_string()),
                (
                    DEFAULT_SCHEMA_NAME.to_string(),
                    "some_alt_metric".to_string(),
                ),
            ],
            2,
            1,
        )
        .await;
        let plan =
            PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &build_query_engine_state())
                .await
                .unwrap();

        let plan_str = plan.display_indent_schema().to_string();
        assert!(!plan_str.contains("__tsid ="), "{plan_str}");
        assert!(
            plan_str.contains("some_metric.tag_1 = some_alt_metric.tag_1"),
            "{plan_str}"
        );
    }

    #[tokio::test]
    async fn ignoring_absent_label_keeps_tsid_binary_join() {
        let eval_stmt = build_eval_stmt("some_metric / ignoring(missing) some_alt_metric");

        let table_provider = build_test_table_provider_with_tsid(
            &[
                (DEFAULT_SCHEMA_NAME.to_string(), "some_metric".to_string()),
                (
                    DEFAULT_SCHEMA_NAME.to_string(),
                    "some_alt_metric".to_string(),
                ),
            ],
            2,
            1,
        )
        .await;
        let plan =
            PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &build_query_engine_state())
                .await
                .unwrap();

        let plan_str = plan.display_indent_schema().to_string();
        assert!(
            plan_str.contains("some_metric.__tsid = some_alt_metric.__tsid"),
            "{plan_str}"
        );
        assert!(!plan_str.contains("tag_0 ="), "{plan_str}");
        assert!(!plan_str.contains("tag_1 ="), "{plan_str}");
    }

    #[tokio::test]
    async fn range_function_keeps_tsid_for_absent_ignoring_binary_join() {
        let eval_stmt =
            build_eval_stmt("rate(some_metric[5m]) / ignoring(missing) some_alt_metric");

        let table_provider = build_test_table_provider_with_tsid(
            &[
                (DEFAULT_SCHEMA_NAME.to_string(), "some_metric".to_string()),
                (
                    DEFAULT_SCHEMA_NAME.to_string(),
                    "some_alt_metric".to_string(),
                ),
            ],
            2,
            1,
        )
        .await;
        let plan =
            PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &build_query_engine_state())
                .await
                .unwrap();

        let plan_str = plan.display_indent_schema().to_string();
        assert!(
            plan_str.contains("some_metric.__tsid = some_alt_metric.__tsid"),
            "{plan_str}"
        );
        assert!(!plan_str.contains("tag_0 ="), "{plan_str}");
        assert!(!plan_str.contains("tag_1 ="), "{plan_str}");
    }

    #[tokio::test]
    async fn on_full_label_set_keeps_tsid_binary_join() {
        let eval_stmt = build_eval_stmt("some_metric / on(tag_0, tag_1) some_alt_metric");

        let table_provider = build_test_table_provider_with_tsid(
            &[
                (DEFAULT_SCHEMA_NAME.to_string(), "some_metric".to_string()),
                (
                    DEFAULT_SCHEMA_NAME.to_string(),
                    "some_alt_metric".to_string(),
                ),
            ],
            2,
            1,
        )
        .await;
        let plan =
            PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &build_query_engine_state())
                .await
                .unwrap();

        let plan_str = plan.display_indent_schema().to_string();
        assert!(
            plan_str.contains("some_metric.__tsid = some_alt_metric.__tsid"),
            "{plan_str}"
        );
        assert!(!plan_str.contains("tag_0 ="), "{plan_str}");
        assert!(!plan_str.contains("tag_1 ="), "{plan_str}");
    }

    #[tokio::test]
    async fn on_partial_label_set_disables_tsid_binary_join() {
        let eval_stmt = build_eval_stmt("some_metric / on(tag_0) some_alt_metric");

        let table_provider = build_test_table_provider_with_tsid(
            &[
                (DEFAULT_SCHEMA_NAME.to_string(), "some_metric".to_string()),
                (
                    DEFAULT_SCHEMA_NAME.to_string(),
                    "some_alt_metric".to_string(),
                ),
            ],
            2,
            1,
        )
        .await;
        let plan =
            PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &build_query_engine_state())
                .await
                .unwrap();

        let plan_str = plan.display_indent_schema().to_string();
        assert!(!plan_str.contains("__tsid ="), "{plan_str}");
        assert!(
            plan_str.contains("some_metric.tag_0 = some_alt_metric.tag_0"),
            "{plan_str}"
        );
        assert!(!plan_str.contains("tag_1 ="), "{plan_str}");
    }

    #[tokio::test]
    async fn on_label_set_must_cover_both_sides_to_use_tsid_binary_join() {
        let eval_stmt = build_eval_stmt("some_metric / on(tag_0) some_alt_metric");

        let table_provider = build_test_table_provider_with_tsid_tag_fields(&[
            (
                (DEFAULT_SCHEMA_NAME.to_string(), "some_metric".to_string()),
                2,
                1,
            ),
            (
                (
                    DEFAULT_SCHEMA_NAME.to_string(),
                    "some_alt_metric".to_string(),
                ),
                1,
                1,
            ),
        ])
        .await;
        let plan =
            PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &build_query_engine_state())
                .await
                .unwrap();

        let plan_str = plan.display_indent_schema().to_string();
        assert!(!plan_str.contains("__tsid ="), "{plan_str}");
        assert!(
            plan_str.contains("some_metric.tag_0 = some_alt_metric.tag_0"),
            "{plan_str}"
        );
        assert!(!plan_str.contains("tag_1 ="), "{plan_str}");
    }

    #[tokio::test]
    async fn comparison_binary_join_uses_tsid_and_keeps_it_in_filtered_result() {
        let eval_stmt = build_eval_stmt("some_metric > some_alt_metric");

        let table_provider = build_test_table_provider_with_tsid(
            &[
                (DEFAULT_SCHEMA_NAME.to_string(), "some_metric".to_string()),
                (
                    DEFAULT_SCHEMA_NAME.to_string(),
                    "some_alt_metric".to_string(),
                ),
            ],
            2,
            1,
        )
        .await;
        let mut planner = PromPlanner {
            table_provider,
            ctx: PromPlannerContext::from_eval_stmt(&eval_stmt),
            promql_annotations: None,
        };
        let plan = planner
            .prom_expr_to_plan(&eval_stmt.expr, &build_query_engine_state())
            .await
            .unwrap();

        let plan_str = plan.display_indent_schema().to_string();
        assert!(
            plan_str.contains("some_metric.__tsid = some_alt_metric.__tsid"),
            "{plan_str}"
        );
        assert!(
            plan.schema()
                .fields()
                .iter()
                .any(|field| field.name() == DATA_SCHEMA_TSID_COLUMN_NAME),
            "{plan_str}"
        );
        assert!(planner.ctx.use_tsid, "{plan_str}");
    }

    #[tokio::test]
    async fn comparison_bool_binary_join_uses_tsid_when_available() {
        let eval_stmt = build_eval_stmt("some_metric > bool some_alt_metric");

        let table_provider = build_test_table_provider_with_tsid(
            &[
                (DEFAULT_SCHEMA_NAME.to_string(), "some_metric".to_string()),
                (
                    DEFAULT_SCHEMA_NAME.to_string(),
                    "some_alt_metric".to_string(),
                ),
            ],
            2,
            1,
        )
        .await;
        let plan =
            PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &build_query_engine_state())
                .await
                .unwrap();

        let plan_str = plan.display_indent_schema().to_string();
        assert!(
            plan_str.contains("some_metric.__tsid = some_alt_metric.__tsid"),
            "{plan_str}"
        );
        assert!(!plan_str.contains("tag_0 ="), "{plan_str}");
        assert!(!plan_str.contains("tag_1 ="), "{plan_str}");
    }

    #[tokio::test]
    async fn scalar_count_count_range_keeps_full_window() {
        let plan_str = build_optimized_tsid_plan(
            "scalar(count(count(some_metric) by (tag_0)))",
            1,
            1,
            100_000,
            1,
        )
        .await;
        assert!(plan_str.contains("ScalarCalculate: tags=[]"));
        assert!(plan_str.contains("PromInstantManipulate: range=[0..100000000]"));
        assert!(!plan_str.contains("PromInstantManipulate: range=[99999000..99999000]"));
    }

    #[tokio::test]
    async fn scalar_count_count_rewrite_applies_inside_binary_expr_for_tsid_input() {
        let plan_str = build_optimized_tsid_plan(
            "sum(irate(some_metric[1h])) / scalar(count(count(some_metric) by (tag_0)))",
            2,
            1,
            10,
            300,
        )
        .await;
        assert!(plan_str.contains("Distinct:"), "{plan_str}");
    }

    #[tokio::test]
    async fn nested_count_rewrite_keeps_full_series_key_with_tsid_input() {
        assert_nested_count_rewrite_applies(
            "count(count(some_metric) by (tag_0))",
            "Aggregate: groupBy=[[some_metric.timestamp]], aggr=[[count(Int64(1)) AS count(count(some_metric.field_0))]]"
        )
        .await;
    }

    #[tokio::test]
    async fn nested_sum_count_rewrite_keeps_full_series_key_with_tsid_input() {
        assert_nested_count_rewrite_applies(
            "count(sum(some_metric) by (tag_0))",
            "Aggregate: groupBy=[[some_metric.timestamp]], aggr=[[count(Int64(1)) AS count(sum(some_metric.field_0))]]"
        )
        .await;
    }

    #[tokio::test]
    async fn nested_supported_inner_aggs_rewrite_apply_for_tsid_input() {
        for (query, expected_outer_agg) in [
            (
                "count(avg(some_metric) by (tag_0))",
                "Aggregate: groupBy=[[some_metric.timestamp]], aggr=[[count(Int64(1)) AS count(avg(some_metric.field_0))]]",
            ),
            (
                "count(min(some_metric) by (tag_0))",
                "Aggregate: groupBy=[[some_metric.timestamp]], aggr=[[count(Int64(1)) AS count(min(some_metric.field_0))]]",
            ),
            (
                "count(max(some_metric) by (tag_0))",
                "Aggregate: groupBy=[[some_metric.timestamp]], aggr=[[count(Int64(1)) AS count(max(some_metric.field_0))]]",
            ),
            (
                "count(stddev(some_metric) by (tag_0))",
                "Aggregate: groupBy=[[some_metric.timestamp]], aggr=[[count(Int64(1)) AS count(stddev_pop(some_metric.field_0))]]",
            ),
            (
                "count(stdvar(some_metric) by (tag_0))",
                "Aggregate: groupBy=[[some_metric.timestamp]], aggr=[[count(Int64(1)) AS count(var_pop(some_metric.field_0))]]",
            ),
        ] {
            assert_nested_count_rewrite_applies(query, expected_outer_agg).await;
        }
    }

    #[tokio::test]
    async fn nested_non_count_inner_aggs_rewrite_filter_null_values_for_tsid_input() {
        let count_plan =
            build_optimized_tsid_plan("count(count(some_metric) by (tag_0))", 2, 1, 100_000, 1)
                .await;
        assert!(
            !count_plan.contains("some_metric.field_0 IS NOT NULL"),
            "{count_plan}"
        );

        for query in [
            "count(sum(some_metric) by (tag_0))",
            "count(avg(some_metric) by (tag_0))",
            "count(min(some_metric) by (tag_0))",
            "count(max(some_metric) by (tag_0))",
            "count(stddev(some_metric) by (tag_0))",
            "count(stdvar(some_metric) by (tag_0))",
        ] {
            let plan_str = build_optimized_tsid_plan(query, 2, 1, 100_000, 1).await;
            assert!(
                plan_str.contains("Filter: some_metric.field_0 IS NOT NULL"),
                "{query}: {plan_str}"
            );
        }
    }

    #[tokio::test]
    async fn nested_unsupported_or_non_direct_inner_aggs_do_not_rewrite() {
        assert_nested_count_rewrite_missing("count(group(some_metric) by (tag_0))", 2, 1).await;
        assert_nested_count_rewrite_missing(
            "count(sum(irate(some_metric[1h])) by (tag_0))",
            2,
            300,
        )
        .await;
    }

    #[tokio::test]
    async fn physical_table_name_is_not_leaked_in_plan() {
        let prom_expr = parser::parse("some_metric").unwrap();
        let eval_stmt = EvalStmt {
            expr: prom_expr,
            start: UNIX_EPOCH,
            end: UNIX_EPOCH
                .checked_add(Duration::from_secs(100_000))
                .unwrap(),
            interval: Duration::from_secs(5),
            lookback_delta: Duration::from_secs(1),
        };

        let table_provider = build_test_table_provider_with_tsid(
            &[(DEFAULT_SCHEMA_NAME.to_string(), "some_metric".to_string())],
            1,
            1,
        )
        .await;
        let plan =
            PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &build_query_engine_state())
                .await
                .unwrap();

        let plan_str = plan.display_indent_schema().to_string();
        assert!(plan_str.contains("TableScan: phy"), "{plan}");
        assert!(plan_str.contains("SubqueryAlias: some_metric"));
        assert!(plan_str.contains("Filter: phy.__table_id = UInt32(1024)"));
        assert!(!plan_str.contains("TableScan: some_metric"));
    }

    #[tokio::test]
    async fn sum_without_does_not_group_by_tsid() {
        let prom_expr = parser::parse("sum without (tag_0) (some_metric)").unwrap();
        let eval_stmt = EvalStmt {
            expr: prom_expr,
            start: UNIX_EPOCH,
            end: UNIX_EPOCH
                .checked_add(Duration::from_secs(100_000))
                .unwrap(),
            interval: Duration::from_secs(5),
            lookback_delta: Duration::from_secs(1),
        };

        let table_provider = build_test_table_provider_with_tsid(
            &[(DEFAULT_SCHEMA_NAME.to_string(), "some_metric".to_string())],
            1,
            1,
        )
        .await;
        let plan =
            PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &build_query_engine_state())
                .await
                .unwrap();

        let plan_str = plan.display_indent_schema().to_string();
        assert!(plan_str.contains("PromSeriesDivide: tags=[\"__tsid\"]"));

        let aggr_line = plan_str
            .lines()
            .find(|line| line.contains("Aggregate: groupBy="))
            .unwrap();
        assert!(!aggr_line.contains(DATA_SCHEMA_TSID_COLUMN_NAME));
    }

    #[tokio::test]
    async fn topk_without_does_not_partition_by_tsid() {
        let prom_expr = parser::parse("topk without (tag_0) (1, some_metric)").unwrap();
        let eval_stmt = EvalStmt {
            expr: prom_expr,
            start: UNIX_EPOCH,
            end: UNIX_EPOCH
                .checked_add(Duration::from_secs(100_000))
                .unwrap(),
            interval: Duration::from_secs(5),
            lookback_delta: Duration::from_secs(1),
        };

        let table_provider = build_test_table_provider_with_tsid(
            &[(DEFAULT_SCHEMA_NAME.to_string(), "some_metric".to_string())],
            1,
            1,
        )
        .await;
        let plan =
            PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &build_query_engine_state())
                .await
                .unwrap();

        let plan_str = plan.display_indent_schema().to_string();
        assert!(plan_str.contains("PromSeriesDivide: tags=[\"__tsid\"]"));

        let window_line = plan_str
            .lines()
            .find(|line| line.contains("WindowAggr: windowExpr=[[row_number()"))
            .unwrap();
        let partition_by = window_line
            .split("PARTITION BY [")
            .nth(1)
            .and_then(|s| s.split("] ORDER BY").next())
            .unwrap();
        assert!(!partition_by.contains(DATA_SCHEMA_TSID_COLUMN_NAME));
    }

    #[tokio::test]
    async fn sum_by_does_not_group_by_tsid() {
        let prom_expr = parser::parse("sum by (__tsid) (some_metric)").unwrap();
        let eval_stmt = EvalStmt {
            expr: prom_expr,
            start: UNIX_EPOCH,
            end: UNIX_EPOCH
                .checked_add(Duration::from_secs(100_000))
                .unwrap(),
            interval: Duration::from_secs(5),
            lookback_delta: Duration::from_secs(1),
        };

        let table_provider = build_test_table_provider_with_tsid(
            &[(DEFAULT_SCHEMA_NAME.to_string(), "some_metric".to_string())],
            1,
            1,
        )
        .await;
        let plan =
            PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &build_query_engine_state())
                .await
                .unwrap();

        let plan_str = plan.display_indent_schema().to_string();
        assert!(plan_str.contains("PromSeriesDivide: tags=[\"__tsid\"]"));

        let aggr_line = plan_str
            .lines()
            .find(|line| line.contains("Aggregate: groupBy="))
            .unwrap();
        assert!(!aggr_line.contains(DATA_SCHEMA_TSID_COLUMN_NAME));
    }

    #[tokio::test]
    async fn aggregate_over_binary_time_function_expr() {
        for op in ["sum", "min", "max", "avg"] {
            let prom_expr = parser::parse(&format!(
                "{op} by (tag_0, tag_1, tag_2) (time() - some_metric)"
            ))
            .unwrap();
            let eval_stmt = EvalStmt {
                expr: prom_expr,
                start: UNIX_EPOCH,
                end: UNIX_EPOCH
                    .checked_add(Duration::from_secs(100_000))
                    .unwrap(),
                interval: Duration::from_secs(5),
                lookback_delta: Duration::from_secs(1),
            };

            let table_provider = build_test_table_provider_with_tsid(
                &[(DEFAULT_SCHEMA_NAME.to_string(), "some_metric".to_string())],
                3,
                1,
            )
            .await;
            let plan =
                PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &build_query_engine_state())
                    .await
                    .unwrap();

            let plan_str = plan.display_indent_schema().to_string();
            let aggr_line = plan_str
                .lines()
                .find(|line| line.contains("Aggregate: groupBy="))
                .unwrap();
            assert!(aggr_line.contains(op), "{plan_str}");
            assert!(aggr_line.contains("first_value"), "{plan_str}");
            assert!(
                !plan
                    .schema()
                    .fields()
                    .iter()
                    .any(|field| { field.name() == DATA_SCHEMA_TSID_COLUMN_NAME })
            );
        }
    }

    #[tokio::test]
    async fn topk_by_does_not_partition_by_tsid() {
        let prom_expr = parser::parse("topk by (__tsid) (1, some_metric)").unwrap();
        let eval_stmt = EvalStmt {
            expr: prom_expr,
            start: UNIX_EPOCH,
            end: UNIX_EPOCH
                .checked_add(Duration::from_secs(100_000))
                .unwrap(),
            interval: Duration::from_secs(5),
            lookback_delta: Duration::from_secs(1),
        };

        let table_provider = build_test_table_provider_with_tsid(
            &[(DEFAULT_SCHEMA_NAME.to_string(), "some_metric".to_string())],
            1,
            1,
        )
        .await;
        let plan =
            PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &build_query_engine_state())
                .await
                .unwrap();

        let plan_str = plan.display_indent_schema().to_string();
        assert!(plan_str.contains("PromSeriesDivide: tags=[\"__tsid\"]"));

        let window_line = plan_str
            .lines()
            .find(|line| line.contains("WindowAggr: windowExpr=[[row_number()"))
            .unwrap();
        let partition_by = window_line
            .split("PARTITION BY [")
            .nth(1)
            .and_then(|s| s.split("] ORDER BY").next())
            .unwrap();
        assert!(!partition_by.contains(DATA_SCHEMA_TSID_COLUMN_NAME));
    }

    #[tokio::test]
    async fn selector_matcher_on_tsid_does_not_use_internal_column() {
        let prom_expr = parser::parse(r#"some_metric{__tsid="123"}"#).unwrap();
        let eval_stmt = EvalStmt {
            expr: prom_expr,
            start: UNIX_EPOCH,
            end: UNIX_EPOCH
                .checked_add(Duration::from_secs(100_000))
                .unwrap(),
            interval: Duration::from_secs(5),
            lookback_delta: Duration::from_secs(1),
        };

        let table_provider = build_test_table_provider_with_tsid(
            &[(DEFAULT_SCHEMA_NAME.to_string(), "some_metric".to_string())],
            1,
            1,
        )
        .await;
        let plan =
            PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &build_query_engine_state())
                .await
                .unwrap();

        fn collect_filter_cols(plan: &LogicalPlan, out: &mut HashSet<Column>) {
            if let LogicalPlan::Filter(filter) = plan {
                datafusion_expr::utils::expr_to_columns(&filter.predicate, out).unwrap();
            }
            for input in plan.inputs() {
                collect_filter_cols(input, out);
            }
        }

        let mut filter_cols = HashSet::new();
        collect_filter_cols(&plan, &mut filter_cols);
        assert!(
            !filter_cols
                .iter()
                .any(|c| c.name == DATA_SCHEMA_TSID_COLUMN_NAME)
        );
    }

    #[tokio::test]
    async fn tsid_is_not_used_when_physical_table_is_missing() {
        let prom_expr = parser::parse("some_metric").unwrap();
        let eval_stmt = EvalStmt {
            expr: prom_expr,
            start: UNIX_EPOCH,
            end: UNIX_EPOCH
                .checked_add(Duration::from_secs(100_000))
                .unwrap(),
            interval: Duration::from_secs(5),
            lookback_delta: Duration::from_secs(1),
        };

        let catalog_list = MemoryCatalogManager::with_default_setup();

        // Register a metric engine logical table referencing a missing physical table.
        let mut columns = vec![ColumnSchema::new(
            "tag_0".to_string(),
            ConcreteDataType::string_datatype(),
            false,
        )];
        columns.push(
            ColumnSchema::new(
                "timestamp".to_string(),
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            )
            .with_time_index(true),
        );
        columns.push(ColumnSchema::new(
            "field_0".to_string(),
            ConcreteDataType::float64_datatype(),
            true,
        ));
        let schema = Arc::new(Schema::new(columns));
        let mut options = table::requests::TableOptions::default();
        options
            .extra_options
            .insert(LOGICAL_TABLE_METADATA_KEY.to_string(), "phy".to_string());
        let table_meta = TableMetaBuilder::empty()
            .schema(schema)
            .primary_key_indices(vec![0])
            .value_indices(vec![2])
            .engine(METRIC_ENGINE_NAME.to_string())
            .options(options)
            .next_column_id(1024)
            .build()
            .unwrap();
        let table_info = TableInfoBuilder::default()
            .table_id(1024)
            .name("some_metric")
            .meta(table_meta)
            .build()
            .unwrap();
        let table = EmptyTable::from_table_info(&table_info);
        catalog_list
            .register_table_sync(RegisterTableRequest {
                catalog: DEFAULT_CATALOG_NAME.to_string(),
                schema: DEFAULT_SCHEMA_NAME.to_string(),
                table_name: "some_metric".to_string(),
                table_id: 1024,
                table,
            })
            .unwrap();

        let table_provider = DfTableSourceProvider::new(
            catalog_list,
            false,
            QueryContext::arc(),
            DummyDecoder::arc(),
            false,
        );

        let plan =
            PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &build_query_engine_state())
                .await
                .unwrap();

        let plan_str = plan.display_indent_schema().to_string();
        assert!(plan_str.contains("PromSeriesDivide: tags=[\"tag_0\"]"));
        assert!(!plan_str.contains("PromSeriesDivide: tags=[\"__tsid\"]"));
    }

    #[tokio::test]
    async fn tsid_is_carried_only_when_aggregate_preserves_label_set() {
        let prom_expr = parser::parse("sum by (tag_0) (some_metric)").unwrap();
        let eval_stmt = EvalStmt {
            expr: prom_expr,
            start: UNIX_EPOCH,
            end: UNIX_EPOCH
                .checked_add(Duration::from_secs(100_000))
                .unwrap(),
            interval: Duration::from_secs(5),
            lookback_delta: Duration::from_secs(1),
        };

        let table_provider = build_test_table_provider_with_tsid(
            &[(DEFAULT_SCHEMA_NAME.to_string(), "some_metric".to_string())],
            1,
            1,
        )
        .await;
        let plan =
            PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &build_query_engine_state())
                .await
                .unwrap();

        let plan_str = plan.display_indent_schema().to_string();
        assert!(plan_str.contains("first_value") && plan_str.contains("__tsid"));
        assert!(
            !plan
                .schema()
                .fields()
                .iter()
                .any(|field| field.name() == DATA_SCHEMA_TSID_COLUMN_NAME)
        );

        // Merging aggregate: label set is reduced, tsid should not be carried.
        let prom_expr = parser::parse("sum(some_metric)").unwrap();
        let eval_stmt = EvalStmt {
            expr: prom_expr,
            start: UNIX_EPOCH,
            end: UNIX_EPOCH
                .checked_add(Duration::from_secs(100_000))
                .unwrap(),
            interval: Duration::from_secs(5),
            lookback_delta: Duration::from_secs(1),
        };
        let table_provider = build_test_table_provider_with_tsid(
            &[(DEFAULT_SCHEMA_NAME.to_string(), "some_metric".to_string())],
            1,
            1,
        )
        .await;
        let plan =
            PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &build_query_engine_state())
                .await
                .unwrap();
        let plan_str = plan.display_indent_schema().to_string();
        assert!(!plan_str.contains("first_value"));
    }

    #[tokio::test]
    async fn or_operator_with_unknown_metric_does_not_require_tsid() {
        let prom_expr = parser::parse("unknown_metric or some_metric").unwrap();
        let eval_stmt = EvalStmt {
            expr: prom_expr,
            start: UNIX_EPOCH,
            end: UNIX_EPOCH
                .checked_add(Duration::from_secs(100_000))
                .unwrap(),
            interval: Duration::from_secs(5),
            lookback_delta: Duration::from_secs(1),
        };

        let table_provider = build_test_table_provider_with_tsid(
            &[(DEFAULT_SCHEMA_NAME.to_string(), "some_metric".to_string())],
            1,
            1,
        )
        .await;

        let plan =
            PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &build_query_engine_state())
                .await
                .unwrap();

        assert!(
            !plan
                .schema()
                .fields()
                .iter()
                .any(|field| field.name() == DATA_SCHEMA_TSID_COLUMN_NAME)
        );
    }

    #[tokio::test]
    async fn aggregate_avg() {
        do_aggregate_expr_plan("avg", "avg").await;
    }

    #[tokio::test]
    #[should_panic] // output type doesn't match
    async fn aggregate_count() {
        do_aggregate_expr_plan("count", "count").await;
    }

    #[tokio::test]
    async fn aggregate_min() {
        do_aggregate_expr_plan("min", "min").await;
    }

    #[tokio::test]
    async fn aggregate_max() {
        do_aggregate_expr_plan("max", "max").await;
    }

    #[tokio::test]
    async fn aggregate_group() {
        // Regression test for `group()` aggregator.
        // PromQL: sum(group by (cluster)(kubernetes_build_info{service="kubernetes",job="apiserver"}))
        // should be plannable, and `group()` should produce constant 1 for each group.
        let prom_expr = parser::parse(
            "sum(group by (cluster)(kubernetes_build_info{service=\"kubernetes\",job=\"apiserver\"}))",
        )
        .unwrap();
        let eval_stmt = EvalStmt {
            expr: prom_expr,
            start: UNIX_EPOCH,
            end: UNIX_EPOCH
                .checked_add(Duration::from_secs(100_000))
                .unwrap(),
            interval: Duration::from_secs(5),
            lookback_delta: Duration::from_secs(1),
        };

        let table_provider = build_test_table_provider_with_fields(
            &[(
                DEFAULT_SCHEMA_NAME.to_string(),
                "kubernetes_build_info".to_string(),
            )],
            &["cluster", "service", "job"],
        )
        .await;
        let plan =
            PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &build_query_engine_state())
                .await
                .unwrap();

        let plan_str = plan.display_indent_schema().to_string();
        assert!(plan_str.contains("max(Float64(1"));
    }

    #[tokio::test]
    async fn aggregate_stddev() {
        do_aggregate_expr_plan("stddev", "stddev_pop").await;
    }

    #[tokio::test]
    async fn aggregate_stdvar() {
        do_aggregate_expr_plan("stdvar", "var_pop").await;
    }

    // TODO(ruihang): add range fn tests once exprs are ready.

    // {
    //     input: "some_metric{tag_0="foo"} + some_metric{tag_0="bar"}",
    //     expected: &BinaryExpr{
    //         Op: ADD,
    //         LHS: &VectorSelector{
    //             Name: "a",
    //             LabelMatchers: []*labels.Matcher{
    //                     MustLabelMatcher(labels.MatchEqual, "tag_0", "foo"),
    //                     MustLabelMatcher(labels.MatchEqual, model.MetricNameLabel, "some_metric"),
    //             },
    //         },
    //         RHS: &VectorSelector{
    //             Name: "sum",
    //             LabelMatchers: []*labels.Matcher{
    //                     MustLabelMatcher(labels.MatchxEqual, "tag_0", "bar"),
    //                     MustLabelMatcher(labels.MatchEqual, model.MetricNameLabel, "some_metric"),
    //             },
    //         },
    //         VectorMatching: &VectorMatching{},
    //     },
    // },
    #[tokio::test]
    async fn binary_op_column_column() {
        let prom_expr =
            parser::parse(r#"some_metric{tag_0="foo"} + some_metric{tag_0="bar"}"#).unwrap();
        let eval_stmt = EvalStmt {
            expr: prom_expr,
            start: UNIX_EPOCH,
            end: UNIX_EPOCH
                .checked_add(Duration::from_secs(100_000))
                .unwrap(),
            interval: Duration::from_secs(5),
            lookback_delta: Duration::from_secs(1),
        };

        let table_provider = build_test_table_provider(
            &[(DEFAULT_SCHEMA_NAME.to_string(), "some_metric".to_string())],
            1,
            1,
        )
        .await;
        let plan =
            PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &build_query_engine_state())
                .await
                .unwrap();

        let expected = String::from(
            "Projection: rhs.tag_0, rhs.timestamp, CAST(lhs.field_0 AS Float64) + CAST(rhs.field_0 AS Float64) AS lhs.field_0 + rhs.field_0 [tag_0:Utf8, timestamp:Timestamp(ms), lhs.field_0 + rhs.field_0:Float64;N]\
            \n  Inner Join: lhs.tag_0 = rhs.tag_0, lhs.timestamp = rhs.timestamp [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N, tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n    SubqueryAlias: lhs [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n      PromInstantManipulate: range=[0..100000000], lookback=[1000], interval=[5000], time index=[timestamp] [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n        PromSeriesDivide: tags=[\"tag_0\"] [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n          Sort: some_metric.tag_0 ASC NULLS FIRST, some_metric.timestamp ASC NULLS FIRST [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n            Filter: some_metric.tag_0 = Utf8(\"foo\") AND some_metric.timestamp >= TimestampMillisecond(-999, None) AND some_metric.timestamp <= TimestampMillisecond(100000000, None) [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n              TableScan: some_metric [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n    SubqueryAlias: rhs [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n      PromInstantManipulate: range=[0..100000000], lookback=[1000], interval=[5000], time index=[timestamp] [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n        PromSeriesDivide: tags=[\"tag_0\"] [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n          Sort: some_metric.tag_0 ASC NULLS FIRST, some_metric.timestamp ASC NULLS FIRST [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n            Filter: some_metric.tag_0 = Utf8(\"bar\") AND some_metric.timestamp >= TimestampMillisecond(-999, None) AND some_metric.timestamp <= TimestampMillisecond(100000000, None) [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n              TableScan: some_metric [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]",
        );

        assert_eq!(plan.display_indent_schema().to_string(), expected);
    }

    async fn indie_query_plan_compare<T: AsRef<str>>(query: &str, expected: T) {
        let prom_expr = parser::parse(query).unwrap();
        let eval_stmt = EvalStmt {
            expr: prom_expr,
            start: UNIX_EPOCH,
            end: UNIX_EPOCH
                .checked_add(Duration::from_secs(100_000))
                .unwrap(),
            interval: Duration::from_secs(5),
            lookback_delta: Duration::from_secs(1),
        };

        let table_provider = build_test_table_provider(
            &[
                (DEFAULT_SCHEMA_NAME.to_string(), "some_metric".to_string()),
                (
                    "greptime_private".to_string(),
                    "some_alt_metric".to_string(),
                ),
            ],
            1,
            1,
        )
        .await;
        let plan =
            PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &build_query_engine_state())
                .await
                .unwrap();

        assert_eq!(plan.display_indent_schema().to_string(), expected.as_ref());
    }

    #[tokio::test]
    async fn binary_op_literal_column() {
        let query = r#"1 + some_metric{tag_0="bar"}"#;
        let expected = String::from(
            "Projection: some_metric.tag_0, some_metric.timestamp, Float64(1) + CAST(some_metric.field_0 AS Float64) AS Float64(1) + field_0 [tag_0:Utf8, timestamp:Timestamp(ms), Float64(1) + field_0:Float64;N]\
            \n  PromInstantManipulate: range=[0..100000000], lookback=[1000], interval=[5000], time index=[timestamp] [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n    PromSeriesDivide: tags=[\"tag_0\"] [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n      Sort: some_metric.tag_0 ASC NULLS FIRST, some_metric.timestamp ASC NULLS FIRST [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n        Filter: some_metric.tag_0 = Utf8(\"bar\") AND some_metric.timestamp >= TimestampMillisecond(-999, None) AND some_metric.timestamp <= TimestampMillisecond(100000000, None) [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n          TableScan: some_metric [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]",
        );

        indie_query_plan_compare(query, expected).await;
    }

    #[tokio::test]
    async fn binary_op_literal_literal() {
        let query = r#"1 + 1"#;
        let expected = r#"EmptyMetric: range=[0..100000000], interval=[5000] [time:Timestamp(ms), value:Float64;N]
  TableScan: dummy [time:Timestamp(ms), value:Float64;N]"#;
        indie_query_plan_compare(query, expected).await;
    }

    #[tokio::test]
    async fn simple_bool_grammar() {
        let query = "some_metric != bool 1.2345";
        let expected = String::from(
            "Projection: some_metric.tag_0, some_metric.timestamp, CAST(some_metric.field_0 != Float64(1.2345) AS Float64) AS field_0 != Float64(1.2345) [tag_0:Utf8, timestamp:Timestamp(ms), field_0 != Float64(1.2345):Float64;N]\
            \n  PromInstantManipulate: range=[0..100000000], lookback=[1000], interval=[5000], time index=[timestamp] [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n    PromSeriesDivide: tags=[\"tag_0\"] [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n      Sort: some_metric.tag_0 ASC NULLS FIRST, some_metric.timestamp ASC NULLS FIRST [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n        Filter: some_metric.timestamp >= TimestampMillisecond(-999, None) AND some_metric.timestamp <= TimestampMillisecond(100000000, None) [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n          TableScan: some_metric [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]",
        );

        indie_query_plan_compare(query, expected).await;
    }

    #[tokio::test]
    async fn bool_with_additional_arithmetic() {
        let query = "some_metric + (1 == bool 2)";
        let expected = String::from(
            "Projection: some_metric.tag_0, some_metric.timestamp, CAST(some_metric.field_0 AS Float64) + CAST(Float64(1) = Float64(2) AS Float64) AS field_0 + Float64(1) = Float64(2) [tag_0:Utf8, timestamp:Timestamp(ms), field_0 + Float64(1) = Float64(2):Float64;N]\
            \n  PromInstantManipulate: range=[0..100000000], lookback=[1000], interval=[5000], time index=[timestamp] [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n    PromSeriesDivide: tags=[\"tag_0\"] [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n      Sort: some_metric.tag_0 ASC NULLS FIRST, some_metric.timestamp ASC NULLS FIRST [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n        Filter: some_metric.timestamp >= TimestampMillisecond(-999, None) AND some_metric.timestamp <= TimestampMillisecond(100000000, None) [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n          TableScan: some_metric [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]",
        );

        indie_query_plan_compare(query, expected).await;
    }

    #[tokio::test]
    async fn simple_unary() {
        let query = "-some_metric";
        let expected = String::from(
            "Projection: some_metric.tag_0, some_metric.timestamp, (- some_metric.field_0) AS (- field_0) [tag_0:Utf8, timestamp:Timestamp(ms), (- field_0):Float64;N]\
            \n  PromInstantManipulate: range=[0..100000000], lookback=[1000], interval=[5000], time index=[timestamp] [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n    PromSeriesDivide: tags=[\"tag_0\"] [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n      Sort: some_metric.tag_0 ASC NULLS FIRST, some_metric.timestamp ASC NULLS FIRST [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n        Filter: some_metric.timestamp >= TimestampMillisecond(-999, None) AND some_metric.timestamp <= TimestampMillisecond(100000000, None) [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n          TableScan: some_metric [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]",
        );

        indie_query_plan_compare(query, expected).await;
    }

    #[tokio::test]
    async fn increase_aggr() {
        let query = "increase(some_metric[5m])";
        let expected = String::from(
            "Filter: prom_increase(timestamp_range,field_0,timestamp,Int64(300000)) IS NOT NULL [timestamp:Timestamp(ms), prom_increase(timestamp_range,field_0,timestamp,Int64(300000)):Float64;N, tag_0:Utf8]\
            \n  Projection: some_metric.timestamp, prom_increase(timestamp_range, field_0, some_metric.timestamp, Int64(300000)) AS prom_increase(timestamp_range,field_0,timestamp,Int64(300000)), some_metric.tag_0 [timestamp:Timestamp(ms), prom_increase(timestamp_range,field_0,timestamp,Int64(300000)):Float64;N, tag_0:Utf8]\
            \n    PromRangeManipulate: req range=[0..100000000], interval=[5000], eval range=[300000], time index=[timestamp], values=[\"field_0\"] [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Dictionary(Int64, Float64);N, timestamp_range:Dictionary(Int64, Timestamp(ms))]\
            \n      PromSeriesNormalize: offset=[0], time index=[timestamp], filter NaN: [true] [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n        PromSeriesDivide: tags=[\"tag_0\"] [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n          Sort: some_metric.tag_0 ASC NULLS FIRST, some_metric.timestamp ASC NULLS FIRST [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n            Filter: some_metric.timestamp >= TimestampMillisecond(-299999, None) AND some_metric.timestamp <= TimestampMillisecond(100000000, None) [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n              TableScan: some_metric [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]",
        );

        indie_query_plan_compare(query, expected).await;
    }

    async fn native_histogram_plan(query: &str) -> String {
        let table_provider = build_test_native_histogram_table_provider("some_metric").await;
        let plan = PromPlanner::stmt_to_plan(
            table_provider,
            &build_eval_stmt(query),
            &build_query_engine_state(),
        )
        .await
        .unwrap();
        plan.display_indent_schema().to_string()
    }

    #[tokio::test]
    async fn native_histogram_count_uses_native_udf() {
        let plan = native_histogram_plan("histogram_count(some_metric)").await;

        assert!(plan.contains("prom_native_histogram_count"), "{plan}");
        assert!(!plan.contains("HistogramFold:"), "{plan}");
    }

    #[tokio::test]
    async fn timestamp_filters_native_histogram_stale_marker_before_projection() {
        let mut stale = direct_or_histogram();
        stale.sum = f64::from_bits(PROMETHEUS_STALE_NAN_BITS);
        let table = operator_metric_table(
            "stale_histogram",
            2_100,
            "a",
            None,
            DirectOrValue::NativeHistogram(stale),
        );
        let catalog = MemoryCatalogManager::with_default_setup();
        catalog
            .register_table_sync(RegisterTableRequest {
                catalog: DEFAULT_CATALOG_NAME.to_string(),
                schema: DEFAULT_SCHEMA_NAME.to_string(),
                table_name: "stale_histogram".to_string(),
                table_id: 2_100,
                table,
            })
            .unwrap();
        let provider = DfTableSourceProvider::new(
            catalog,
            false,
            QueryContext::arc(),
            DummyDecoder::arc(),
            false,
        );
        let state = build_query_engine_state();
        let plan = PromPlanner::stmt_to_plan(
            provider,
            &operator_eval_stmt("timestamp(stale_histogram)"),
            &state,
        )
        .await
        .unwrap();
        let plan_text = plan.display_indent_schema().to_string();
        assert!(plan_text.contains(TIMESTAMP_VALUE_PREFIX), "{plan_text}");

        let (_, batches) = execute(plan, &state).await;
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 0);
    }

    #[tokio::test]
    async fn timestamp_filters_stale_marker_from_mixed_sample_companion() {
        let histograms = build_histogram_array(&[None]);
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new(
                "timestamp",
                ArrowDataType::Timestamp(ArrowTimeUnit::Millisecond, None),
                false,
            ),
            Field::new(
                greptime_native_histogram(),
                histograms.data_type().clone(),
                true,
            ),
            Field::new(greptime_value(), ArrowDataType::Float64, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(TimestampMillisecondArray::from(vec![1_000])),
                histograms,
                Arc::new(Float64Array::from(vec![f64::from_bits(
                    PROMETHEUS_STALE_NAN_BITS,
                )])),
            ],
        )
        .unwrap();
        let table = Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap());
        let input = LogicalPlanBuilder::scan("mixed", provider_as_source(table), None)
            .unwrap()
            .build()
            .unwrap();
        let input = LogicalPlan::Extension(Extension {
            node: Arc::new(SeriesDivide::new(
                Vec::new(),
                "timestamp".to_string(),
                input,
            )),
        });
        let input = LogicalPlan::Extension(Extension {
            node: Arc::new(InstantManipulate::new(
                1_000,
                1_000,
                5_000,
                1_000,
                "timestamp".to_string(),
                Vec::new(),
                Some(greptime_native_histogram().to_string()),
                input,
            )),
        });
        // Match timestamp()'s parent projection, which otherwise prunes the companion lane.
        let plan = LogicalPlanBuilder::from(input)
            .project([col("timestamp")])
            .unwrap()
            .build()
            .unwrap();

        let (_, batches) = execute(plan, &build_query_engine_state()).await;
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 0);
    }

    #[tokio::test]
    async fn native_histogram_rate_can_feed_count() {
        let plan = native_histogram_plan("histogram_count(rate(some_metric[5m]))").await;

        assert!(plan.contains("prom_native_histogram_rate"), "{plan}");
        assert!(plan.contains("prom_native_histogram_count"), "{plan}");
    }

    #[tokio::test]
    async fn native_histogram_quantile_skips_classic_fold() {
        let plan = native_histogram_plan("histogram_quantile(0.9, some_metric)").await;

        assert!(plan.contains("prom_native_histogram_quantile"), "{plan}");
        assert!(!plan.contains("HistogramFold:"), "{plan}");
        assert!(plan.contains("some_metric.le"), "{plan}");
        // The phi literal is threaded into the native quantile UDF as its second argument.
        assert!(plan.contains("Float64(0.9)"), "{plan}");
        // The empty-values filter drops NULL quantile results so the output is empty
        // when all native histogram samples are dropped.
        assert!(plan.contains("IS NOT NULL"), "{plan}");
    }

    #[tokio::test]
    async fn mixed_native_histogram_quantile_uses_histogram_field() {
        let table_provider = build_test_mixed_native_histogram_table_provider("some_metric").await;
        let plan = PromPlanner::stmt_to_plan(
            table_provider,
            &build_eval_stmt("histogram_quantile(0.9, some_metric)"),
            &build_query_engine_state(),
        )
        .await
        .unwrap()
        .display_indent_schema()
        .to_string();

        assert!(
            plan.contains("prom_native_histogram_quantile(greptime_native_histogram"),
            "{plan}"
        );
        assert!(!plan.contains("EmptyRelation"), "{plan}");
    }

    #[tokio::test]
    async fn mixed_histogram_helpers_execute_classic_and_native_samples() {
        let state = build_query_engine_state();
        for (query, expected) in [
            (
                "histogram_quantile(0.5, mixed_histogram)",
                vec![("classic", 1.0), ("native", 0.0)],
            ),
            (
                "histogram_fraction(-Inf, +Inf, mixed_histogram)",
                vec![("classic", 1.0), ("native", 1.0)],
            ),
        ] {
            let plan = PromPlanner::stmt_to_plan(
                classic_and_native_histogram_table_provider("native", None, direct_or_histogram()),
                &operator_eval_stmt(query),
                &state,
            )
            .await
            .unwrap();
            let plan_text = plan.display_indent_schema().to_string();
            assert!(plan_text.contains("HistogramFold:"), "{plan_text}");
            assert!(plan_text.contains("prom_native_histogram_"), "{plan_text}");
            let value_field = plan
                .schema()
                .fields()
                .iter()
                .find(|field| field.data_type() == &ArrowDataType::Float64)
                .unwrap()
                .name()
                .clone();

            let (_, batches) = execute(plan, &state).await;
            let mut actual = batches
                .iter()
                .flat_map(|batch| {
                    let tags = batch
                        .column_by_name("tag")
                        .unwrap()
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap();
                    let values = batch
                        .column_by_name(&value_field)
                        .unwrap()
                        .as_any()
                        .downcast_ref::<Float64Array>()
                        .unwrap();
                    (0..batch.num_rows()).map(|row| (tags.value(row), values.value(row)))
                })
                .collect::<Vec<_>>();
            actual.sort_by_key(|(tag, _)| *tag);
            assert_eq!(actual, expected, "{query}");
        }
    }

    #[tokio::test]
    async fn mixed_histogram_helpers_report_annotations() {
        let state = build_query_engine_state();
        let mut native_histogram = direct_or_histogram();
        native_histogram.count = 2.0;
        native_histogram.sum = f64::NAN;
        for (native_tag, expected_rows, expected_warnings, expected_infos) in [
            (
                "classic",
                0,
                vec!["vector contains a mix of classic and native histograms"],
                vec![],
            ),
            (
                "native",
                2,
                vec![],
                vec!["input to histogram_quantile has NaN observations, result is skewed higher"],
            ),
        ] {
            let collector = PromqlAnnotationCollector::default();
            let plan = PromPlanner::stmt_to_plan_with_annotations(
                classic_and_native_histogram_table_provider(
                    native_tag,
                    None,
                    native_histogram.clone(),
                ),
                &operator_eval_stmt("histogram_quantile(0.5, mixed_histogram)"),
                &state,
                Some(collector.clone()),
            )
            .await
            .unwrap();

            let (_, batches) = execute(plan, &state).await;
            assert_eq!(
                batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
                expected_rows
            );
            let mut warnings = vec![];
            let mut infos = vec![];
            collector.append_to(&mut warnings, &mut infos);
            assert_eq!(warnings, expected_warnings);
            assert_eq!(infos, expected_infos);
        }
    }

    #[tokio::test]
    async fn mixed_histogram_helper_preserves_native_le_and_scans_once() {
        let state = build_query_engine_state();
        let mut stmt = operator_eval_stmt("histogram_quantile(0.5, mixed_histogram)");
        stmt.end = UNIX_EPOCH.checked_add(Duration::from_secs(2)).unwrap();
        let plan = PromPlanner::stmt_to_plan(
            classic_and_native_histogram_table_provider(
                "classic",
                Some("native"),
                direct_or_histogram(),
            ),
            &stmt,
            &state,
        )
        .await
        .unwrap();
        let plan_text = plan.display_indent_schema().to_string();
        assert_eq!(
            plan_text.matches("TableScan: mixed_histogram").count(),
            1,
            "{plan_text}"
        );

        let value_field = plan
            .schema()
            .fields()
            .iter()
            .find(|field| field.data_type() == &ArrowDataType::Float64)
            .unwrap()
            .name()
            .clone();
        let (_, batches) = execute(plan, &state).await;
        let mut actual = batches
            .iter()
            .flat_map(|batch| {
                let le = batch
                    .column_by_name(LE_COLUMN_NAME)
                    .unwrap()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                let timestamps = batch
                    .column_by_name("timestamp")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<TimestampMillisecondArray>()
                    .unwrap();
                let values = batch
                    .column_by_name(&value_field)
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap();
                (0..batch.num_rows()).map(|row| {
                    (
                        timestamps.value(row),
                        (!le.is_null(row)).then(|| le.value(row).to_string()),
                        values.value(row),
                    )
                })
            })
            .collect::<Vec<_>>();
        actual.sort_by(|lhs, rhs| (lhs.0, &lhs.1).cmp(&(rhs.0, &rhs.1)));
        assert_eq!(
            actual,
            vec![
                (1_000, None, 1.0),
                (1_000, Some("native".to_string()), 0.0),
                (2_000, None, 1.0),
                (2_000, Some("native".to_string()), 0.0),
            ]
        );
    }

    #[tokio::test]
    async fn nested_histogram_helpers_ignore_unparsable_bucket_labels() {
        let state = build_query_engine_state();
        for native_le in [None, Some("native")] {
            for query in [
                "histogram_quantile(0.5, histogram_quantile(0.5, mixed_histogram))",
                "histogram_fraction(-Inf, +Inf, histogram_fraction(-Inf, +Inf, mixed_histogram))",
            ] {
                let plan = PromPlanner::stmt_to_plan(
                    classic_and_native_histogram_table_provider(
                        "native",
                        native_le,
                        direct_or_histogram(),
                    ),
                    &operator_eval_stmt(query),
                    &state,
                )
                .await
                .unwrap();

                let (_, batches) = execute(plan, &state).await;
                assert_eq!(
                    batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
                    0,
                    "native_le={native_le:?}, query={query}"
                );
            }
        }
    }

    #[tokio::test]
    async fn native_histogram_quantile_rejects_multi_field_input() {
        let table_provider = build_test_multi_histogram_table_provider("some_metric").await;
        let result = PromPlanner::stmt_to_plan(
            table_provider,
            &build_eval_stmt("histogram_quantile(0.9, some_metric)"),
            &build_query_engine_state(),
        )
        .await;

        let err = result.expect_err("histogram_quantile on two native histogram fields must fail");
        assert!(
            err.to_string()
                .contains("Multi fields calculation is not supported in histogram_quantile"),
            "{err}"
        );
    }

    #[tokio::test]
    async fn native_histogram_topk_uses_drop_udf() {
        let plan = native_histogram_plan("topk(1, some_metric)").await;

        assert!(plan.contains("prom_native_histogram_drop_float"), "{plan}");
        assert!(
            plan.contains("Filter: prom_native_histogram_drop_float")
                && plan.contains("IS NOT NULL"),
            "{plan}"
        );
    }

    #[tokio::test]
    async fn mixed_or_topk_bottomk_ignore_native_histograms() {
        for op in ["topk", "bottomk"] {
            let collector = PromqlAnnotationCollector::default();
            let state = build_query_engine_state();
            let plan = PromPlanner::stmt_to_plan_with_annotations(
                operator_table_provider(),
                &operator_eval_stmt(&format!("{op}(1, lf or on(tag) lh)")),
                &state,
                Some(collector.clone()),
            )
            .await
            .unwrap();
            let float_field = plan
                .schema()
                .fields()
                .iter()
                .find(|field| field.data_type() == &ArrowDataType::Float64)
                .unwrap()
                .name()
                .clone();
            assert!(
                plan.schema()
                    .fields()
                    .iter()
                    .all(|field| field.data_type() != &PromPlanner::native_histogram_arrow_type()),
                "{plan:?}"
            );

            let (_, batches) = execute(plan, &state).await;
            assert_eq!(values(&batches, &float_field), vec![2.0], "{op}");
            let mut warnings = vec![];
            let mut infos = vec![];
            collector.append_to(&mut warnings, &mut infos);
            assert!(warnings.is_empty());
            assert_eq!(
                infos,
                vec![format!(
                    "{op}: dropped native histogram samples because this aggregation is not supported for native histograms"
                )]
            );
        }
    }

    #[tokio::test]
    async fn native_histogram_scalar_is_ignored_before_scalar_calculate() {
        let plan = native_histogram_plan("scalar(some_metric)").await;

        assert!(plan.contains("ScalarCalculate"), "{plan}");
        assert!(plan.contains("Filter: Boolean(false)"), "{plan}");
        assert!(!plan.contains("prom_native_histogram_drop"), "{plan}");
    }

    #[tokio::test]
    async fn native_histogram_value_sort_is_empty_but_label_sort_preserves_samples() {
        for function in ["sort", "sort_desc"] {
            let plan = native_histogram_plan(&format!("{function}(some_metric)")).await;

            assert!(plan.contains("Float64(NULL) IS NOT NULL"), "{plan}");
            assert!(
                !plan.contains(&format!("Sort: {}", greptime_native_histogram())),
                "{plan}"
            );
            assert!(!plan.contains("prom_native_histogram_drop"), "{plan}");
        }

        for (function, direction) in [("sort_by_label", "ASC"), ("sort_by_label_desc", "DESC")] {
            let plan = native_histogram_plan(&format!("{function}(some_metric, \"tag_0\")")).await;

            assert!(plan.contains(&format!("tag_0 {direction}")), "{plan}");
            assert!(plan.contains(greptime_native_histogram()), "{plan}");
            assert!(!plan.contains("Float64(NULL) IS NOT NULL"), "{plan}");
        }
    }

    #[tokio::test]
    async fn unsupported_native_histogram_functions_use_drop_udf() {
        for query in [
            "deriv(some_metric[5m])",
            "min_over_time(some_metric[5m])",
            "quantile_over_time(0.9, some_metric[5m])",
            "predict_linear(some_metric[5m], 60)",
            "round(some_metric)",
            "abs(some_metric)",
        ] {
            let plan = native_histogram_plan(query).await;

            assert!(
                plan.contains("prom_native_histogram_drop_float"),
                "{query}\n{plan}"
            );
        }
    }

    #[tokio::test]
    async fn native_histogram_absent_over_time_uses_native_udf() {
        let plan = native_histogram_plan("absent_over_time(some_metric[5m])").await;

        assert!(
            plan.contains("prom_native_histogram_absent_over_time"),
            "{plan}"
        );
    }

    #[tokio::test]
    async fn native_histogram_all_function_arms_route_correctly() {
        // Every native-histogram match arm in `create_function_expr` must route to the
        // expected UDF when all field columns are native histograms. `holt_winters` shares
        // the `double_exponential_smoothing` arm but is not registered in the promql
        // parser (0.10), so it cannot be exercised through a query string.
        let cases = [
            // Range functions routed to native histogram UDFs.
            (
                "increase(some_metric[5m])",
                "prom_native_histogram_increase",
            ),
            ("rate(some_metric[5m])", "prom_native_histogram_rate"),
            ("delta(some_metric[5m])", "prom_native_histogram_delta"),
            ("idelta(some_metric[5m])", "prom_native_histogram_idelta"),
            ("irate(some_metric[5m])", "prom_native_histogram_irate"),
            ("resets(some_metric[5m])", "prom_native_histogram_resets"),
            ("changes(some_metric[5m])", "prom_native_histogram_changes"),
            (
                "avg_over_time(some_metric[5m])",
                "prom_native_histogram_avg_over_time",
            ),
            (
                "sum_over_time(some_metric[5m])",
                "prom_native_histogram_sum_over_time",
            ),
            (
                "count_over_time(some_metric[5m])",
                "prom_native_histogram_count_over_time",
            ),
            (
                "last_over_time(some_metric[5m])",
                "prom_native_histogram_last_over_time",
            ),
            (
                "present_over_time(some_metric[5m])",
                "prom_native_histogram_present_over_time",
            ),
            // Unsupported functions dropped with the float-null UDF.
            ("deriv(some_metric[5m])", "prom_native_histogram_drop_float"),
            (
                "min_over_time(some_metric[5m])",
                "prom_native_histogram_drop_float",
            ),
            (
                "max_over_time(some_metric[5m])",
                "prom_native_histogram_drop_float",
            ),
            (
                "stddev_over_time(some_metric[5m])",
                "prom_native_histogram_drop_float",
            ),
            (
                "stdvar_over_time(some_metric[5m])",
                "prom_native_histogram_drop_float",
            ),
            (
                "quantile_over_time(0.9, some_metric[5m])",
                "prom_native_histogram_drop_float",
            ),
            (
                "predict_linear(some_metric[5m], 60)",
                "prom_native_histogram_drop_float",
            ),
            (
                "double_exponential_smoothing(some_metric[5m], 0.5, 0.5)",
                "prom_native_histogram_drop_float",
            ),
            ("round(some_metric)", "prom_native_histogram_drop_float"),
            ("rad(some_metric)", "prom_native_histogram_drop_float"),
            ("deg(some_metric)", "prom_native_histogram_drop_float"),
            ("sgn(some_metric)", "prom_native_histogram_drop_float"),
            // Instant helper functions routed to native histogram UDFs.
            (
                "histogram_count(some_metric)",
                "prom_native_histogram_count",
            ),
            ("histogram_sum(some_metric)", "prom_native_histogram_sum"),
            ("histogram_avg(some_metric)", "prom_native_histogram_avg"),
            (
                "histogram_stddev(some_metric)",
                "prom_native_histogram_stddev",
            ),
            (
                "histogram_stdvar(some_metric)",
                "prom_native_histogram_stdvar",
            ),
            (
                "histogram_fraction(-2 + 1, 2 / 2, some_metric)",
                "prom_native_histogram_fraction",
            ),
        ];

        for (query, expected_udf) in cases {
            let plan = native_histogram_plan(query).await;
            assert!(plan.contains(expected_udf), "{query}\n{plan}");
            if query.starts_with("histogram_fraction") {
                assert!(plan.contains("Float64(-1)"), "{query}\n{plan}");
            }
        }
    }

    #[tokio::test]
    async fn mixed_native_histogram_ranges_use_coordinated_udfs() {
        let dual_output = [
            "increase(some_metric[5m])",
            "rate(some_metric[5m])",
            "delta(some_metric[5m])",
            "idelta(some_metric[5m])",
            "irate(some_metric[5m])",
            "avg_over_time(some_metric[5m])",
            "sum_over_time(some_metric[5m])",
            "last_over_time(some_metric[5m])",
        ];
        let float_output = [
            "resets(some_metric[5m])",
            "changes(some_metric[5m])",
            "deriv(some_metric[5m])",
            "min_over_time(some_metric[5m])",
            "max_over_time(some_metric[5m])",
            "count_over_time(some_metric[5m])",
            "absent_over_time(some_metric[5m])",
            "present_over_time(some_metric[5m])",
            "stddev_over_time(some_metric[5m])",
            "stdvar_over_time(some_metric[5m])",
            "quantile_over_time(0.9, some_metric[5m])",
            "predict_linear(some_metric[5m], 60)",
            "double_exponential_smoothing(some_metric[5m], 0.5, 0.5)",
        ];

        for query in dual_output.iter().chain(float_output.iter()) {
            let plan = PromPlanner::stmt_to_plan(
                build_test_mixed_native_histogram_table_provider("some_metric").await,
                &build_eval_stmt(query),
                &build_query_engine_state(),
            )
            .await
            .unwrap()
            .display_indent_schema()
            .to_string();
            assert!(plan.contains("prom_mixed_range_float"), "{query}\n{plan}");
            assert_eq!(
                plan.contains("prom_mixed_range_histogram"),
                dual_output.contains(query),
                "{query}\n{plan}"
            );
        }

        let plan = PromPlanner::stmt_to_plan(
            build_test_mixed_native_histogram_table_provider("some_metric").await,
            &build_eval_stmt("sum_over_time(rate(some_metric[5m])[10m:1m])"),
            &build_query_engine_state(),
        )
        .await
        .unwrap()
        .display_indent_schema()
        .to_string();
        let expected = r#"Filter: greptime_value IS NOT NULL OR greptime_native_histogram IS NOT NULL [timestamp:Timestamp(ms), greptime_value:Float64;N, greptime_native_histogram:Struct("schema": Int32, "zero_threshold": Float64, "sum": Float64, "reset_hint": Int32, "start_timestamp": Timestamp(ms), "custom_values": List(Float64), "positive_span_offsets": List(Int32), "positive_span_lengths": List(Int32), "negative_span_offsets": List(Int32), "negative_span_lengths": List(Int32), "count_i64": Int64, "zero_count_i64": Int64, "positive_buckets_i64": List(Int64), "negative_buckets_i64": List(Int64), "count_f64": Float64, "zero_count_f64": Float64, "positive_buckets_f64": List(Float64), "negative_buckets_f64": List(Float64));N, tag_0:Utf8]
  Projection: some_metric.timestamp, prom_mixed_range_float(Utf8("sum_over_time"), timestamp_range, greptime_value, greptime_native_histogram) AS greptime_value, prom_mixed_range_histogram(Utf8("sum_over_time"), timestamp_range, greptime_value, greptime_native_histogram) AS greptime_native_histogram, some_metric.tag_0 [timestamp:Timestamp(ms), greptime_value:Float64;N, greptime_native_histogram:Struct("schema": Int32, "zero_threshold": Float64, "sum": Float64, "reset_hint": Int32, "start_timestamp": Timestamp(ms), "custom_values": List(Float64), "positive_span_offsets": List(Int32), "positive_span_lengths": List(Int32), "negative_span_offsets": List(Int32), "negative_span_lengths": List(Int32), "count_i64": Int64, "zero_count_i64": Int64, "positive_buckets_i64": List(Int64), "negative_buckets_i64": List(Int64), "count_f64": Float64, "zero_count_f64": Float64, "positive_buckets_f64": List(Float64), "negative_buckets_f64": List(Float64));N, tag_0:Utf8]
    PromRangeManipulate: req range=[0..100000000], interval=[5000], eval range=[600000], time index=[timestamp], values=["greptime_value", "greptime_native_histogram"] [timestamp:Timestamp(ms), greptime_value:Dictionary(Int64, Float64);N, greptime_native_histogram:Dictionary(Int64, Struct("schema": Int32, "zero_threshold": Float64, "sum": Float64, "reset_hint": Int32, "start_timestamp": Timestamp(ms), "custom_values": List(Float64), "positive_span_offsets": List(Int32), "positive_span_lengths": List(Int32), "negative_span_offsets": List(Int32), "negative_span_lengths": List(Int32), "count_i64": Int64, "zero_count_i64": Int64, "positive_buckets_i64": List(Int64), "negative_buckets_i64": List(Int64), "count_f64": Float64, "zero_count_f64": Float64, "positive_buckets_f64": List(Float64), "negative_buckets_f64": List(Float64)));N, tag_0:Utf8, timestamp_range:Dictionary(Int64, Timestamp(ms))]
      PromSeriesDivide: tags=["tag_0"] [timestamp:Timestamp(ms), greptime_value:Float64;N, greptime_native_histogram:Struct("schema": Int32, "zero_threshold": Float64, "sum": Float64, "reset_hint": Int32, "start_timestamp": Timestamp(ms), "custom_values": List(Float64), "positive_span_offsets": List(Int32), "positive_span_lengths": List(Int32), "negative_span_offsets": List(Int32), "negative_span_lengths": List(Int32), "count_i64": Int64, "zero_count_i64": Int64, "positive_buckets_i64": List(Int64), "negative_buckets_i64": List(Int64), "count_f64": Float64, "zero_count_f64": Float64, "positive_buckets_f64": List(Float64), "negative_buckets_f64": List(Float64));N, tag_0:Utf8]
        Sort: some_metric.tag_0 ASC NULLS FIRST, some_metric.timestamp ASC NULLS FIRST [timestamp:Timestamp(ms), greptime_value:Float64;N, greptime_native_histogram:Struct("schema": Int32, "zero_threshold": Float64, "sum": Float64, "reset_hint": Int32, "start_timestamp": Timestamp(ms), "custom_values": List(Float64), "positive_span_offsets": List(Int32), "positive_span_lengths": List(Int32), "negative_span_offsets": List(Int32), "negative_span_lengths": List(Int32), "count_i64": Int64, "zero_count_i64": Int64, "positive_buckets_i64": List(Int64), "negative_buckets_i64": List(Int64), "count_f64": Float64, "zero_count_f64": Float64, "positive_buckets_f64": List(Float64), "negative_buckets_f64": List(Float64));N, tag_0:Utf8]
          Filter: greptime_value IS NOT NULL OR greptime_native_histogram IS NOT NULL [timestamp:Timestamp(ms), greptime_value:Float64;N, greptime_native_histogram:Struct("schema": Int32, "zero_threshold": Float64, "sum": Float64, "reset_hint": Int32, "start_timestamp": Timestamp(ms), "custom_values": List(Float64), "positive_span_offsets": List(Int32), "positive_span_lengths": List(Int32), "negative_span_offsets": List(Int32), "negative_span_lengths": List(Int32), "count_i64": Int64, "zero_count_i64": Int64, "positive_buckets_i64": List(Int64), "negative_buckets_i64": List(Int64), "count_f64": Float64, "zero_count_f64": Float64, "positive_buckets_f64": List(Float64), "negative_buckets_f64": List(Float64));N, tag_0:Utf8]
            Projection: some_metric.timestamp, prom_mixed_range_float(Utf8("rate"), timestamp_range, greptime_value, greptime_native_histogram, some_metric.timestamp, Int64(300000)) AS greptime_value, prom_mixed_range_histogram(Utf8("rate"), timestamp_range, greptime_value, greptime_native_histogram, some_metric.timestamp, Int64(300000)) AS greptime_native_histogram, some_metric.tag_0 [timestamp:Timestamp(ms), greptime_value:Float64;N, greptime_native_histogram:Struct("schema": Int32, "zero_threshold": Float64, "sum": Float64, "reset_hint": Int32, "start_timestamp": Timestamp(ms), "custom_values": List(Float64), "positive_span_offsets": List(Int32), "positive_span_lengths": List(Int32), "negative_span_offsets": List(Int32), "negative_span_lengths": List(Int32), "count_i64": Int64, "zero_count_i64": Int64, "positive_buckets_i64": List(Int64), "negative_buckets_i64": List(Int64), "count_f64": Float64, "zero_count_f64": Float64, "positive_buckets_f64": List(Float64), "negative_buckets_f64": List(Float64));N, tag_0:Utf8]
              PromRangeManipulate: req range=[-540000..100000000], interval=[60000], eval range=[300000], time index=[timestamp], values=["greptime_native_histogram", "greptime_value"] [tag_0:Utf8, timestamp:Timestamp(ms), greptime_native_histogram:Dictionary(Int64, Struct("schema": Int32, "zero_threshold": Float64, "sum": Float64, "reset_hint": Int32, "start_timestamp": Timestamp(ms), "custom_values": List(Float64), "positive_span_offsets": List(Int32), "positive_span_lengths": List(Int32), "negative_span_offsets": List(Int32), "negative_span_lengths": List(Int32), "count_i64": Int64, "zero_count_i64": Int64, "positive_buckets_i64": List(Int64), "negative_buckets_i64": List(Int64), "count_f64": Float64, "zero_count_f64": Float64, "positive_buckets_f64": List(Float64), "negative_buckets_f64": List(Float64)));N, greptime_value:Dictionary(Int64, Float64);N, timestamp_range:Dictionary(Int64, Timestamp(ms))]
                PromSeriesNormalize: offset=[0], time index=[timestamp], filter NaN: [true] [tag_0:Utf8, timestamp:Timestamp(ms), greptime_native_histogram:Struct("schema": Int32, "zero_threshold": Float64, "sum": Float64, "reset_hint": Int32, "start_timestamp": Timestamp(ms), "custom_values": List(Float64), "positive_span_offsets": List(Int32), "positive_span_lengths": List(Int32), "negative_span_offsets": List(Int32), "negative_span_lengths": List(Int32), "count_i64": Int64, "zero_count_i64": Int64, "positive_buckets_i64": List(Int64), "negative_buckets_i64": List(Int64), "count_f64": Float64, "zero_count_f64": Float64, "positive_buckets_f64": List(Float64), "negative_buckets_f64": List(Float64));N, greptime_value:Float64;N]
                  PromSeriesDivide: tags=["tag_0"] [tag_0:Utf8, timestamp:Timestamp(ms), greptime_native_histogram:Struct("schema": Int32, "zero_threshold": Float64, "sum": Float64, "reset_hint": Int32, "start_timestamp": Timestamp(ms), "custom_values": List(Float64), "positive_span_offsets": List(Int32), "positive_span_lengths": List(Int32), "negative_span_offsets": List(Int32), "negative_span_lengths": List(Int32), "count_i64": Int64, "zero_count_i64": Int64, "positive_buckets_i64": List(Int64), "negative_buckets_i64": List(Int64), "count_f64": Float64, "zero_count_f64": Float64, "positive_buckets_f64": List(Float64), "negative_buckets_f64": List(Float64));N, greptime_value:Float64;N]
                    Sort: some_metric.tag_0 ASC NULLS FIRST, some_metric.timestamp ASC NULLS FIRST [tag_0:Utf8, timestamp:Timestamp(ms), greptime_native_histogram:Struct("schema": Int32, "zero_threshold": Float64, "sum": Float64, "reset_hint": Int32, "start_timestamp": Timestamp(ms), "custom_values": List(Float64), "positive_span_offsets": List(Int32), "positive_span_lengths": List(Int32), "negative_span_offsets": List(Int32), "negative_span_lengths": List(Int32), "count_i64": Int64, "zero_count_i64": Int64, "positive_buckets_i64": List(Int64), "negative_buckets_i64": List(Int64), "count_f64": Float64, "zero_count_f64": Float64, "positive_buckets_f64": List(Float64), "negative_buckets_f64": List(Float64));N, greptime_value:Float64;N]
                      Filter: some_metric.timestamp >= TimestampMillisecond(-839999, None) AND some_metric.timestamp <= TimestampMillisecond(100000000, None) [tag_0:Utf8, timestamp:Timestamp(ms), greptime_native_histogram:Struct("schema": Int32, "zero_threshold": Float64, "sum": Float64, "reset_hint": Int32, "start_timestamp": Timestamp(ms), "custom_values": List(Float64), "positive_span_offsets": List(Int32), "positive_span_lengths": List(Int32), "negative_span_offsets": List(Int32), "negative_span_lengths": List(Int32), "count_i64": Int64, "zero_count_i64": Int64, "positive_buckets_i64": List(Int64), "negative_buckets_i64": List(Int64), "count_f64": Float64, "zero_count_f64": Float64, "positive_buckets_f64": List(Float64), "negative_buckets_f64": List(Float64));N, greptime_value:Float64;N]
                        TableScan: some_metric [tag_0:Utf8, timestamp:Timestamp(ms), greptime_native_histogram:Struct("schema": Int32, "zero_threshold": Float64, "sum": Float64, "reset_hint": Int32, "start_timestamp": Timestamp(ms), "custom_values": List(Float64), "positive_span_offsets": List(Int32), "positive_span_lengths": List(Int32), "negative_span_offsets": List(Int32), "negative_span_lengths": List(Int32), "count_i64": Int64, "zero_count_i64": Int64, "positive_buckets_i64": List(Int64), "negative_buckets_i64": List(Int64), "count_f64": Float64, "zero_count_f64": Float64, "positive_buckets_f64": List(Float64), "negative_buckets_f64": List(Float64));N, greptime_value:Float64;N]"#;
        assert_eq!(plan, expected);
    }

    #[tokio::test]
    async fn mixed_native_histogram_rate_executes_real_ranges() {
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new(
                "timestamp",
                ArrowDataType::Timestamp(ArrowTimeUnit::Millisecond, None),
                false,
            ),
            Field::new(greptime_value(), ArrowDataType::Float64, true),
            Field::new(
                greptime_native_histogram(),
                native_histogram_value_type().as_arrow_type(),
                true,
            ),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(TimestampMillisecondArray::from(vec![1000, 2000, 3000])),
                Arc::new(Float64Array::from(vec![Some(1.0), None, Some(3.0)])),
                build_histogram_array(&[None, Some(direct_or_histogram()), None]),
            ],
        )
        .unwrap();
        let table = Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap());
        let input = LogicalPlanBuilder::scan("mixed", provider_as_source(table), None)
            .unwrap()
            .build()
            .unwrap();
        let collector = PromqlAnnotationCollector::default();
        let mut planner = PromPlanner {
            table_provider: build_test_table_provider_with_fields(
                &[(DEFAULT_SCHEMA_NAME.to_string(), "dummy".to_string())],
                &[],
            )
            .await,
            ctx: PromPlannerContext {
                start: 3000,
                end: 3000,
                interval: 1000,
                range: Some(3000),
                time_index_column: Some("timestamp".to_string()),
                field_columns: vec![
                    greptime_native_histogram().to_string(),
                    greptime_value().to_string(),
                ],
                ..Default::default()
            },
            promql_annotations: Some(collector.clone()),
        };
        let input = LogicalPlan::Extension(Extension {
            node: Arc::new(
                RangeManipulate::new(
                    3000,
                    3000,
                    1000,
                    3000,
                    "timestamp".to_string(),
                    planner.ctx.field_columns.clone(),
                    input,
                )
                .unwrap(),
            ),
        });
        let PromExpr::Call(call) = parser::parse("rate(mixed[3s])").unwrap() else {
            unreachable!()
        };
        let preserve_any_value = PromPlanner::field_columns_are_alternative_samples(
            input.schema(),
            &planner.ctx.field_columns,
        );
        let state = build_query_engine_state();
        let (mut exprs, _) = planner
            .create_function_expr(&call.func, vec![], input.schema(), &state)
            .unwrap();
        exprs.insert(0, planner.create_time_index_column_expr().unwrap());
        let plan = LogicalPlanBuilder::from(input)
            .project(exprs)
            .unwrap()
            .filter(
                planner
                    .create_empty_values_filter_expr(preserve_any_value)
                    .unwrap(),
            )
            .unwrap()
            .build()
            .unwrap();
        let (_, batches) = execute(plan, &state).await;
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 0);
        let mut warnings = Vec::new();
        collector.append_to(&mut warnings, &mut Vec::new());
        assert!(
            warnings
                .iter()
                .any(|warning| warning.contains("mix of float and native histogram"))
        );
    }

    #[tokio::test]
    async fn native_histogram_mixed_field_table_behaves() {
        // Exercise function planning after float and histogram samples have already been
        // represented as alternative nullable fields. Histogram functions must select the
        // histogram field without adding a NULL float field that would reject every row.
        let table_provider = build_test_mixed_native_histogram_table_provider("some_metric").await;
        let plan = PromPlanner::stmt_to_plan(
            table_provider,
            &build_eval_stmt("histogram_count(some_metric)"),
            &build_query_engine_state(),
        )
        .await
        .unwrap();
        let plan_str = plan.display_indent_schema().to_string();
        assert!(
            plan_str.contains("prom_native_histogram_count"),
            "{plan_str}"
        );
        assert!(!plan_str.contains("Float64(NULL)"), "{plan_str}");
        assert!(
            plan_str.contains("prom_native_histogram_count(greptime_native_histogram) IS NOT NULL"),
            "{plan_str}"
        );

        // Value sorting keeps the float column and never sorts by the histogram column.
        let table_provider = build_test_mixed_native_histogram_table_provider("some_metric").await;
        let plan = PromPlanner::stmt_to_plan(
            table_provider,
            &build_eval_stmt("sort(some_metric)"),
            &build_query_engine_state(),
        )
        .await
        .unwrap();
        let plan_str = plan.display_indent_schema().to_string();
        assert!(
            plan_str.contains("greptime_value ASC NULLS FIRST"),
            "{plan_str}"
        );
        assert!(
            !plan_str.contains("greptime_native_histogram ASC"),
            "{plan_str}"
        );

        // scalar() ignores histogram samples and evaluates only the float field.
        let table_provider = build_test_mixed_native_histogram_table_provider("some_metric").await;
        let plan = PromPlanner::stmt_to_plan(
            table_provider,
            &build_eval_stmt("scalar(some_metric)"),
            &build_query_engine_state(),
        )
        .await
        .unwrap();
        let plan_str = plan.display_indent_schema().to_string();
        assert!(plan_str.contains("ScalarCalculate"), "{plan_str}");
        assert!(
            plan_str.contains("greptime_value IS NOT NULL"),
            "{plan_str}"
        );

        // Functions that preserve both alternative fields keep rows with either sample type.
        let table_provider = build_test_mixed_native_histogram_table_provider("some_metric").await;
        let plan = PromPlanner::stmt_to_plan(
            table_provider,
            &build_eval_stmt(r#"label_replace(some_metric, "copied", "$1", "tag_0", "(.*)")"#),
            &build_query_engine_state(),
        )
        .await
        .unwrap();
        let plan_str = plan.display_indent_schema().to_string();
        let filter = plan_str.lines().next().unwrap();
        assert!(
            filter.starts_with("Filter: ")
                && filter.contains("greptime_native_histogram IS NOT NULL")
                && filter.contains(" OR ")
                && filter.contains("greptime_value IS NOT NULL"),
            "{plan_str}"
        );
    }

    #[tokio::test]
    async fn less_filter_on_value() {
        let query = "some_metric < 1.2345";
        let expected = String::from(
            "Filter: some_metric.field_0 < Float64(1.2345) [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n  PromInstantManipulate: range=[0..100000000], lookback=[1000], interval=[5000], time index=[timestamp] [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n    PromSeriesDivide: tags=[\"tag_0\"] [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n      Sort: some_metric.tag_0 ASC NULLS FIRST, some_metric.timestamp ASC NULLS FIRST [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n        Filter: some_metric.timestamp >= TimestampMillisecond(-999, None) AND some_metric.timestamp <= TimestampMillisecond(100000000, None) [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n          TableScan: some_metric [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]",
        );

        indie_query_plan_compare(query, expected).await;
    }

    #[tokio::test]
    async fn count_over_time() {
        let query = "count_over_time(some_metric[5m])";
        let expected = String::from(
            "Filter: prom_count_over_time(timestamp_range,field_0) IS NOT NULL [timestamp:Timestamp(ms), prom_count_over_time(timestamp_range,field_0):Float64;N, tag_0:Utf8]\
            \n  Projection: some_metric.timestamp, prom_count_over_time(timestamp_range, field_0) AS prom_count_over_time(timestamp_range,field_0), some_metric.tag_0 [timestamp:Timestamp(ms), prom_count_over_time(timestamp_range,field_0):Float64;N, tag_0:Utf8]\
            \n    PromRangeManipulate: req range=[0..100000000], interval=[5000], eval range=[300000], time index=[timestamp], values=[\"field_0\"] [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Dictionary(Int64, Float64);N, timestamp_range:Dictionary(Int64, Timestamp(ms))]\
            \n      PromSeriesNormalize: offset=[0], time index=[timestamp], filter NaN: [true] [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n        PromSeriesDivide: tags=[\"tag_0\"] [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n          Sort: some_metric.tag_0 ASC NULLS FIRST, some_metric.timestamp ASC NULLS FIRST [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n            Filter: some_metric.timestamp >= TimestampMillisecond(-299999, None) AND some_metric.timestamp <= TimestampMillisecond(100000000, None) [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n              TableScan: some_metric [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]",
        );

        indie_query_plan_compare(query, expected).await;
    }

    /// The outer `PromRangeManipulate` from a subquery must be preceded by
    /// `Sort` + `PromSeriesDivide`.
    #[tokio::test]
    async fn count_over_time_subquery() {
        let query = "count_over_time(some_metric[10m:1m])";
        let expected = String::from(
            "Filter: prom_count_over_time(timestamp_range,field_0) IS NOT NULL [timestamp:Timestamp(ms), prom_count_over_time(timestamp_range,field_0):Float64;N, tag_0:Utf8]\
            \n  Projection: some_metric.timestamp, prom_count_over_time(timestamp_range, field_0) AS prom_count_over_time(timestamp_range,field_0), some_metric.tag_0 [timestamp:Timestamp(ms), prom_count_over_time(timestamp_range,field_0):Float64;N, tag_0:Utf8]\
            \n    PromRangeManipulate: req range=[0..100000000], interval=[5000], eval range=[600000], time index=[timestamp], values=[\"field_0\"] [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Dictionary(Int64, Float64);N, timestamp_range:Dictionary(Int64, Timestamp(ms))]\
            \n      PromSeriesDivide: tags=[\"tag_0\"] [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n        Sort: some_metric.tag_0 ASC NULLS FIRST, some_metric.timestamp ASC NULLS FIRST [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n          PromInstantManipulate: range=[-540000..100000000], lookback=[1000], interval=[60000], time index=[timestamp] [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n            PromSeriesDivide: tags=[\"tag_0\"] [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n              Sort: some_metric.tag_0 ASC NULLS FIRST, some_metric.timestamp ASC NULLS FIRST [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n                Filter: some_metric.timestamp >= TimestampMillisecond(-540999, None) AND some_metric.timestamp <= TimestampMillisecond(100000000, None) [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n                  TableScan: some_metric [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]",
        );
        indie_query_plan_compare(query, expected).await;
    }

    #[tokio::test]
    async fn test_hash_join() {
        let mut eval_stmt = EvalStmt {
            expr: PromExpr::NumberLiteral(NumberLiteral { val: 1.0 }),
            start: UNIX_EPOCH,
            end: UNIX_EPOCH
                .checked_add(Duration::from_secs(100_000))
                .unwrap(),
            interval: Duration::from_secs(5),
            lookback_delta: Duration::from_secs(1),
        };

        let case = r#"http_server_requests_seconds_sum{uri="/accounts/login"} / ignoring(kubernetes_pod_name,kubernetes_namespace) http_server_requests_seconds_count{uri="/accounts/login"}"#;

        let prom_expr = parser::parse(case).unwrap();
        eval_stmt.expr = prom_expr;
        let table_provider = build_test_table_provider_with_fields(
            &[
                (
                    DEFAULT_SCHEMA_NAME.to_string(),
                    "http_server_requests_seconds_sum".to_string(),
                ),
                (
                    DEFAULT_SCHEMA_NAME.to_string(),
                    "http_server_requests_seconds_count".to_string(),
                ),
            ],
            &["uri", "kubernetes_namespace", "kubernetes_pod_name"],
        )
        .await;
        // Should be ok
        let plan =
            PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &build_query_engine_state())
                .await
                .unwrap();
        let expected = "Projection: http_server_requests_seconds_count.uri, http_server_requests_seconds_count.kubernetes_namespace, http_server_requests_seconds_count.kubernetes_pod_name, http_server_requests_seconds_count.greptime_timestamp, CAST(http_server_requests_seconds_sum.greptime_value AS Float64) / CAST(http_server_requests_seconds_count.greptime_value AS Float64) AS http_server_requests_seconds_sum.greptime_value / http_server_requests_seconds_count.greptime_value\
            \n  Inner Join: http_server_requests_seconds_sum.greptime_timestamp = http_server_requests_seconds_count.greptime_timestamp, http_server_requests_seconds_sum.uri = http_server_requests_seconds_count.uri\
            \n    SubqueryAlias: http_server_requests_seconds_sum\
            \n      PromInstantManipulate: range=[0..100000000], lookback=[1000], interval=[5000], time index=[greptime_timestamp]\
            \n        PromSeriesDivide: tags=[\"uri\", \"kubernetes_namespace\", \"kubernetes_pod_name\"]\
            \n          Sort: http_server_requests_seconds_sum.uri ASC NULLS FIRST, http_server_requests_seconds_sum.kubernetes_namespace ASC NULLS FIRST, http_server_requests_seconds_sum.kubernetes_pod_name ASC NULLS FIRST, http_server_requests_seconds_sum.greptime_timestamp ASC NULLS FIRST\
            \n            Filter: http_server_requests_seconds_sum.uri = Utf8(\"/accounts/login\") AND http_server_requests_seconds_sum.greptime_timestamp >= TimestampMillisecond(-999, None) AND http_server_requests_seconds_sum.greptime_timestamp <= TimestampMillisecond(100000000, None)\
            \n              TableScan: http_server_requests_seconds_sum\
            \n    SubqueryAlias: http_server_requests_seconds_count\
            \n      PromInstantManipulate: range=[0..100000000], lookback=[1000], interval=[5000], time index=[greptime_timestamp]\
            \n        PromSeriesDivide: tags=[\"uri\", \"kubernetes_namespace\", \"kubernetes_pod_name\"]\
            \n          Sort: http_server_requests_seconds_count.uri ASC NULLS FIRST, http_server_requests_seconds_count.kubernetes_namespace ASC NULLS FIRST, http_server_requests_seconds_count.kubernetes_pod_name ASC NULLS FIRST, http_server_requests_seconds_count.greptime_timestamp ASC NULLS FIRST\
            \n            Filter: http_server_requests_seconds_count.uri = Utf8(\"/accounts/login\") AND http_server_requests_seconds_count.greptime_timestamp >= TimestampMillisecond(-999, None) AND http_server_requests_seconds_count.greptime_timestamp <= TimestampMillisecond(100000000, None)\
            \n              TableScan: http_server_requests_seconds_count";
        assert_eq!(plan.to_string(), expected);
    }

    #[tokio::test]
    async fn test_nested_histogram_quantile() {
        let mut eval_stmt = EvalStmt {
            expr: PromExpr::NumberLiteral(NumberLiteral { val: 1.0 }),
            start: UNIX_EPOCH,
            end: UNIX_EPOCH
                .checked_add(Duration::from_secs(100_000))
                .unwrap(),
            interval: Duration::from_secs(5),
            lookback_delta: Duration::from_secs(1),
        };

        let case = r#"label_replace(histogram_quantile(0.99, sum by(pod, le, path, code) (rate(greptime_servers_grpc_requests_elapsed_bucket{container="frontend"}[1m0s]))), "pod_new", "$1", "pod", "greptimedb-frontend-[0-9a-z]*-(.*)")"#;

        let prom_expr = parser::parse(case).unwrap();
        eval_stmt.expr = prom_expr;
        let table_provider = build_test_table_provider_with_fields(
            &[(
                DEFAULT_SCHEMA_NAME.to_string(),
                "greptime_servers_grpc_requests_elapsed_bucket".to_string(),
            )],
            &["pod", "le", "path", "code", "container"],
        )
        .await;
        // Should be ok
        let _ = PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &build_query_engine_state())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_histogram_quantile_binary_op() {
        let mut eval_stmt = EvalStmt {
            expr: PromExpr::NumberLiteral(NumberLiteral { val: 1.0 }),
            start: UNIX_EPOCH,
            end: UNIX_EPOCH
                .checked_add(Duration::from_secs(100_000))
                .unwrap(),
            interval: Duration::from_secs(5),
            lookback_delta: Duration::from_secs(1),
        };

        // Arithmetic applied to a histogram_quantile() result. Regression for #8144:
        // HistogramFold used to drop the input column qualifiers, so the binary-op
        // projection failed to resolve the qualified tag column.
        let case = r#"histogram_quantile(0.5, sum by (le, pod) (rate(http_request_duration_seconds_bucket[5m]))) + 0"#;

        let prom_expr = parser::parse(case).unwrap();
        eval_stmt.expr = prom_expr;
        let table_provider = build_test_table_provider_with_fields(
            &[(
                DEFAULT_SCHEMA_NAME.to_string(),
                "http_request_duration_seconds_bucket".to_string(),
            )],
            &["pod", "le"],
        )
        .await;
        // Should plan without a "No field named ..." error.
        let _ = PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &build_query_engine_state())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_parse_and_operator() {
        let mut eval_stmt = EvalStmt {
            expr: PromExpr::NumberLiteral(NumberLiteral { val: 1.0 }),
            start: UNIX_EPOCH,
            end: UNIX_EPOCH
                .checked_add(Duration::from_secs(100_000))
                .unwrap(),
            interval: Duration::from_secs(5),
            lookback_delta: Duration::from_secs(1),
        };

        let cases = [
            r#"count (max by (persistentvolumeclaim,namespace) (kubelet_volume_stats_used_bytes{namespace=~".+"} ) and (max by (persistentvolumeclaim,namespace) (kubelet_volume_stats_used_bytes{namespace=~".+"} )) / (max by (persistentvolumeclaim,namespace) (kubelet_volume_stats_capacity_bytes{namespace=~".+"} )) >= (80 / 100)) or vector (0)"#,
            r#"count (max by (persistentvolumeclaim,namespace) (kubelet_volume_stats_used_bytes{namespace=~".+"} ) unless (max by (persistentvolumeclaim,namespace) (kubelet_volume_stats_used_bytes{namespace=~".+"} )) / (max by (persistentvolumeclaim,namespace) (kubelet_volume_stats_capacity_bytes{namespace=~".+"} )) >= (80 / 100)) or vector (0)"#,
        ];

        for case in cases {
            let prom_expr = parser::parse(case).unwrap();
            eval_stmt.expr = prom_expr;
            let table_provider = build_test_table_provider_with_fields(
                &[
                    (
                        DEFAULT_SCHEMA_NAME.to_string(),
                        "kubelet_volume_stats_used_bytes".to_string(),
                    ),
                    (
                        DEFAULT_SCHEMA_NAME.to_string(),
                        "kubelet_volume_stats_capacity_bytes".to_string(),
                    ),
                ],
                &["namespace", "persistentvolumeclaim"],
            )
            .await;
            // Should be ok
            let _ =
                PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &build_query_engine_state())
                    .await
                    .unwrap();
        }
    }

    #[tokio::test]
    async fn test_nested_binary_op() {
        let mut eval_stmt = EvalStmt {
            expr: PromExpr::NumberLiteral(NumberLiteral { val: 1.0 }),
            start: UNIX_EPOCH,
            end: UNIX_EPOCH
                .checked_add(Duration::from_secs(100_000))
                .unwrap(),
            interval: Duration::from_secs(5),
            lookback_delta: Duration::from_secs(1),
        };

        let case = r#"sum(rate(nginx_ingress_controller_requests{job=~".*"}[2m])) -
        (
            sum(rate(nginx_ingress_controller_requests{namespace=~".*"}[2m]))
            or
            vector(0)
        )"#;

        let prom_expr = parser::parse(case).unwrap();
        eval_stmt.expr = prom_expr;
        let table_provider = build_test_table_provider_with_fields(
            &[(
                DEFAULT_SCHEMA_NAME.to_string(),
                "nginx_ingress_controller_requests".to_string(),
            )],
            &["namespace", "job"],
        )
        .await;
        // Should be ok
        let _ = PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &build_query_engine_state())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_parse_or_operator() {
        let mut eval_stmt = EvalStmt {
            expr: PromExpr::NumberLiteral(NumberLiteral { val: 1.0 }),
            start: UNIX_EPOCH,
            end: UNIX_EPOCH
                .checked_add(Duration::from_secs(100_000))
                .unwrap(),
            interval: Duration::from_secs(5),
            lookback_delta: Duration::from_secs(1),
        };

        let case = r#"
        sum(rate(sysstat{tenant_name=~"tenant1",cluster_name=~"cluster1"}[120s])) by (cluster_name,tenant_name) /
        (sum(sysstat{tenant_name=~"tenant1",cluster_name=~"cluster1"}) by (cluster_name,tenant_name) * 100)
            or
        200 * sum(sysstat{tenant_name=~"tenant1",cluster_name=~"cluster1"}) by (cluster_name,tenant_name) /
        sum(sysstat{tenant_name=~"tenant1",cluster_name=~"cluster1"}) by (cluster_name,tenant_name)"#;

        let table_provider = build_test_table_provider_with_fields(
            &[(DEFAULT_SCHEMA_NAME.to_string(), "sysstat".to_string())],
            &["tenant_name", "cluster_name"],
        )
        .await;
        eval_stmt.expr = parser::parse(case).unwrap();
        let _ = PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &build_query_engine_state())
            .await
            .unwrap();

        let case = r#"sum(delta(sysstat{tenant_name=~"sys",cluster_name=~"cluster1"}[2m])/120) by (cluster_name,tenant_name) /
            (sum(delta(sysstat{tenant_name=~"sys",cluster_name=~"cluster1"}[2m])/120) by (cluster_name,tenant_name) *1000) +
            sum(delta(sysstat{tenant_name=~"sys",cluster_name=~"cluster1"}[2m])/120) by (cluster_name,tenant_name) /
            (sum(delta(sysstat{tenant_name=~"sys",cluster_name=~"cluster1"}[2m])/120) by (cluster_name,tenant_name) *1000) >= 0
            or
            sum(delta(sysstat{tenant_name=~"sys",cluster_name=~"cluster1"}[2m])/120) by (cluster_name,tenant_name) /
            (sum(delta(sysstat{tenant_name=~"sys",cluster_name=~"cluster1"}[2m])/120) by (cluster_name,tenant_name) *1000) >= 0
            or
            sum(delta(sysstat{tenant_name=~"sys",cluster_name=~"cluster1"}[2m])/120) by (cluster_name,tenant_name) /
            (sum(delta(sysstat{tenant_name=~"sys",cluster_name=~"cluster1"}[2m])/120) by (cluster_name,tenant_name) *1000) >= 0"#;
        let table_provider = build_test_table_provider_with_fields(
            &[(DEFAULT_SCHEMA_NAME.to_string(), "sysstat".to_string())],
            &["tenant_name", "cluster_name"],
        )
        .await;
        eval_stmt.expr = parser::parse(case).unwrap();
        let _ = PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &build_query_engine_state())
            .await
            .unwrap();

        let case = r#"(sum(background_waitevent_cnt{tenant_name=~"sys",cluster_name=~"cluster1"}) by (cluster_name,tenant_name) +
            sum(foreground_waitevent_cnt{tenant_name=~"sys",cluster_name=~"cluster1"}) by (cluster_name,tenant_name)) or
            (sum(background_waitevent_cnt{tenant_name=~"sys",cluster_name=~"cluster1"}) by (cluster_name,tenant_name)) or
            (sum(foreground_waitevent_cnt{tenant_name=~"sys",cluster_name=~"cluster1"}) by (cluster_name,tenant_name))"#;
        let table_provider = build_test_table_provider_with_fields(
            &[
                (
                    DEFAULT_SCHEMA_NAME.to_string(),
                    "background_waitevent_cnt".to_string(),
                ),
                (
                    DEFAULT_SCHEMA_NAME.to_string(),
                    "foreground_waitevent_cnt".to_string(),
                ),
            ],
            &["tenant_name", "cluster_name"],
        )
        .await;
        eval_stmt.expr = parser::parse(case).unwrap();
        let _ = PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &build_query_engine_state())
            .await
            .unwrap();

        let case = r#"avg(node_load1{cluster_name=~"cluster1"}) by (cluster_name,host_name) or max(container_cpu_load_average_10s{cluster_name=~"cluster1"}) by (cluster_name,host_name) * 100 / max(container_spec_cpu_quota{cluster_name=~"cluster1"}) by (cluster_name,host_name)"#;
        let table_provider = build_test_table_provider_with_fields(
            &[
                (DEFAULT_SCHEMA_NAME.to_string(), "node_load1".to_string()),
                (
                    DEFAULT_SCHEMA_NAME.to_string(),
                    "container_cpu_load_average_10s".to_string(),
                ),
                (
                    DEFAULT_SCHEMA_NAME.to_string(),
                    "container_spec_cpu_quota".to_string(),
                ),
            ],
            &["cluster_name", "host_name"],
        )
        .await;
        eval_stmt.expr = parser::parse(case).unwrap();
        let _ = PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &build_query_engine_state())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn value_matcher() {
        // template
        let mut eval_stmt = EvalStmt {
            expr: PromExpr::NumberLiteral(NumberLiteral { val: 1.0 }),
            start: UNIX_EPOCH,
            end: UNIX_EPOCH
                .checked_add(Duration::from_secs(100_000))
                .unwrap(),
            interval: Duration::from_secs(5),
            lookback_delta: Duration::from_secs(1),
        };

        let cases = [
            // single equal matcher
            (
                r#"some_metric{__field__="field_1"}"#,
                vec![
                    "some_metric.field_1",
                    "some_metric.tag_0",
                    "some_metric.tag_1",
                    "some_metric.tag_2",
                    "some_metric.timestamp",
                ],
            ),
            // two equal matchers
            (
                r#"some_metric{__field__="field_1", __field__="field_0"}"#,
                vec![
                    "some_metric.field_0",
                    "some_metric.field_1",
                    "some_metric.tag_0",
                    "some_metric.tag_1",
                    "some_metric.tag_2",
                    "some_metric.timestamp",
                ],
            ),
            // single not_eq matcher
            (
                r#"some_metric{__field__!="field_1"}"#,
                vec![
                    "some_metric.field_0",
                    "some_metric.field_2",
                    "some_metric.tag_0",
                    "some_metric.tag_1",
                    "some_metric.tag_2",
                    "some_metric.timestamp",
                ],
            ),
            // two not_eq matchers
            (
                r#"some_metric{__field__!="field_1", __field__!="field_2"}"#,
                vec![
                    "some_metric.field_0",
                    "some_metric.tag_0",
                    "some_metric.tag_1",
                    "some_metric.tag_2",
                    "some_metric.timestamp",
                ],
            ),
            // equal and not_eq matchers (no conflict)
            (
                r#"some_metric{__field__="field_1", __field__!="field_0"}"#,
                vec![
                    "some_metric.field_1",
                    "some_metric.tag_0",
                    "some_metric.tag_1",
                    "some_metric.tag_2",
                    "some_metric.timestamp",
                ],
            ),
            // equal and not_eq matchers (conflict)
            (
                r#"some_metric{__field__="field_2", __field__!="field_2"}"#,
                vec![
                    "some_metric.tag_0",
                    "some_metric.tag_1",
                    "some_metric.tag_2",
                    "some_metric.timestamp",
                ],
            ),
            // single regex eq matcher
            (
                r#"some_metric{__field__=~"field_1|field_2"}"#,
                vec![
                    "some_metric.field_1",
                    "some_metric.field_2",
                    "some_metric.tag_0",
                    "some_metric.tag_1",
                    "some_metric.tag_2",
                    "some_metric.timestamp",
                ],
            ),
            // single regex not_eq matcher
            (
                r#"some_metric{__field__!~"field_1|field_2"}"#,
                vec![
                    "some_metric.field_0",
                    "some_metric.tag_0",
                    "some_metric.tag_1",
                    "some_metric.tag_2",
                    "some_metric.timestamp",
                ],
            ),
        ];

        for case in cases {
            let prom_expr = parser::parse(case.0).unwrap();
            eval_stmt.expr = prom_expr;
            let table_provider = build_test_table_provider(
                &[(DEFAULT_SCHEMA_NAME.to_string(), "some_metric".to_string())],
                3,
                3,
            )
            .await;
            let plan =
                PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &build_query_engine_state())
                    .await
                    .unwrap();
            let mut fields = plan.schema().field_names();
            let mut expected = case.1.into_iter().map(String::from).collect::<Vec<_>>();
            fields.sort();
            expected.sort();
            assert_eq!(fields, expected, "case: {:?}", case.0);
        }

        let bad_cases = [
            r#"some_metric{__field__="nonexistent"}"#,
            r#"some_metric{__field__!="nonexistent"}"#,
        ];

        for case in bad_cases {
            let prom_expr = parser::parse(case).unwrap();
            eval_stmt.expr = prom_expr;
            let table_provider = build_test_table_provider(
                &[(DEFAULT_SCHEMA_NAME.to_string(), "some_metric".to_string())],
                3,
                3,
            )
            .await;
            let plan =
                PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &build_query_engine_state())
                    .await;
            assert!(plan.is_err(), "case: {:?}", case);
        }
    }

    #[tokio::test]
    async fn custom_schema() {
        let query = "some_alt_metric{__schema__=\"greptime_private\"}";
        let expected = String::from(
            "PromInstantManipulate: range=[0..100000000], lookback=[1000], interval=[5000], time index=[timestamp] [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n  PromSeriesDivide: tags=[\"tag_0\"] [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n    Sort: greptime_private.some_alt_metric.tag_0 ASC NULLS FIRST, greptime_private.some_alt_metric.timestamp ASC NULLS FIRST [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n      Filter: greptime_private.some_alt_metric.timestamp >= TimestampMillisecond(-999, None) AND greptime_private.some_alt_metric.timestamp <= TimestampMillisecond(100000000, None) [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n        TableScan: greptime_private.some_alt_metric [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]",
        );

        indie_query_plan_compare(query, expected).await;

        let query = "some_alt_metric{__database__=\"greptime_private\"}";
        let expected = String::from(
            "PromInstantManipulate: range=[0..100000000], lookback=[1000], interval=[5000], time index=[timestamp] [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n  PromSeriesDivide: tags=[\"tag_0\"] [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n    Sort: greptime_private.some_alt_metric.tag_0 ASC NULLS FIRST, greptime_private.some_alt_metric.timestamp ASC NULLS FIRST [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n      Filter: greptime_private.some_alt_metric.timestamp >= TimestampMillisecond(-999, None) AND greptime_private.some_alt_metric.timestamp <= TimestampMillisecond(100000000, None) [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n        TableScan: greptime_private.some_alt_metric [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]",
        );

        indie_query_plan_compare(query, expected).await;

        let query = "some_alt_metric{__schema__=\"greptime_private\"} / some_metric";
        let expected = String::from(
            "Projection: some_metric.tag_0, some_metric.timestamp, CAST(greptime_private.some_alt_metric.field_0 AS Float64) / CAST(some_metric.field_0 AS Float64) AS greptime_private.some_alt_metric.field_0 / some_metric.field_0 [tag_0:Utf8, timestamp:Timestamp(ms), greptime_private.some_alt_metric.field_0 / some_metric.field_0:Float64;N]\
            \n  Inner Join: greptime_private.some_alt_metric.tag_0 = some_metric.tag_0, greptime_private.some_alt_metric.timestamp = some_metric.timestamp [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N, tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n    SubqueryAlias: greptime_private.some_alt_metric [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n      PromInstantManipulate: range=[0..100000000], lookback=[1000], interval=[5000], time index=[timestamp] [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n        PromSeriesDivide: tags=[\"tag_0\"] [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n          Sort: greptime_private.some_alt_metric.tag_0 ASC NULLS FIRST, greptime_private.some_alt_metric.timestamp ASC NULLS FIRST [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n            Filter: greptime_private.some_alt_metric.timestamp >= TimestampMillisecond(-999, None) AND greptime_private.some_alt_metric.timestamp <= TimestampMillisecond(100000000, None) [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n              TableScan: greptime_private.some_alt_metric [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n    SubqueryAlias: some_metric [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n      PromInstantManipulate: range=[0..100000000], lookback=[1000], interval=[5000], time index=[timestamp] [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n        PromSeriesDivide: tags=[\"tag_0\"] [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n          Sort: some_metric.tag_0 ASC NULLS FIRST, some_metric.timestamp ASC NULLS FIRST [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n            Filter: some_metric.timestamp >= TimestampMillisecond(-999, None) AND some_metric.timestamp <= TimestampMillisecond(100000000, None) [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]\
            \n              TableScan: some_metric [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]",
        );

        indie_query_plan_compare(query, expected).await;
    }

    #[tokio::test]
    async fn only_equals_is_supported_for_special_matcher() {
        let queries = &[
            "some_alt_metric{__schema__!=\"greptime_private\"}",
            "some_alt_metric{__schema__=~\"lalala\"}",
            "some_alt_metric{__database__!=\"greptime_private\"}",
            "some_alt_metric{__database__=~\"lalala\"}",
        ];

        for query in queries {
            let prom_expr = parser::parse(query).unwrap();
            let eval_stmt = EvalStmt {
                expr: prom_expr,
                start: UNIX_EPOCH,
                end: UNIX_EPOCH
                    .checked_add(Duration::from_secs(100_000))
                    .unwrap(),
                interval: Duration::from_secs(5),
                lookback_delta: Duration::from_secs(1),
            };

            let table_provider = build_test_table_provider(
                &[
                    (DEFAULT_SCHEMA_NAME.to_string(), "some_metric".to_string()),
                    (
                        "greptime_private".to_string(),
                        "some_alt_metric".to_string(),
                    ),
                ],
                1,
                1,
            )
            .await;

            let plan =
                PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &build_query_engine_state())
                    .await;
            assert!(plan.is_err(), "query: {:?}", query);
        }
    }

    #[tokio::test]
    async fn test_non_ms_precision() {
        let catalog_list = MemoryCatalogManager::with_default_setup();
        let columns = vec![
            ColumnSchema::new(
                "tag".to_string(),
                ConcreteDataType::string_datatype(),
                false,
            ),
            ColumnSchema::new(
                "timestamp".to_string(),
                ConcreteDataType::timestamp_nanosecond_datatype(),
                false,
            )
            .with_time_index(true),
            ColumnSchema::new(
                "field".to_string(),
                ConcreteDataType::float64_datatype(),
                true,
            ),
        ];
        let schema = Arc::new(Schema::new(columns));
        let table_meta = TableMetaBuilder::empty()
            .schema(schema)
            .primary_key_indices(vec![0])
            .value_indices(vec![2])
            .next_column_id(1024)
            .build()
            .unwrap();
        let table_info = TableInfoBuilder::default()
            .name("metrics".to_string())
            .meta(table_meta)
            .build()
            .unwrap();
        let table = EmptyTable::from_table_info(&table_info);
        assert!(
            catalog_list
                .register_table_sync(RegisterTableRequest {
                    catalog: DEFAULT_CATALOG_NAME.to_string(),
                    schema: DEFAULT_SCHEMA_NAME.to_string(),
                    table_name: "metrics".to_string(),
                    table_id: 1024,
                    table,
                })
                .is_ok()
        );

        let plan = PromPlanner::stmt_to_plan(
            DfTableSourceProvider::new(
                catalog_list.clone(),
                false,
                QueryContext::arc(),
                DummyDecoder::arc(),
                true,
            ),
            &EvalStmt {
                expr: parser::parse("metrics{tag = \"1\"}").unwrap(),
                start: UNIX_EPOCH,
                end: UNIX_EPOCH
                    .checked_add(Duration::from_secs(100_000))
                    .unwrap(),
                interval: Duration::from_secs(5),
                lookback_delta: Duration::from_secs(1),
            },
            &build_query_engine_state(),
        )
        .await
        .unwrap();
        assert_eq!(
            plan.display_indent_schema().to_string(),
            "PromInstantManipulate: range=[0..100000000], lookback=[1000], interval=[5000], time index=[timestamp] [field:Float64;N, tag:Utf8, timestamp:Timestamp(ms)]\
            \n  PromSeriesDivide: tags=[\"tag\"] [field:Float64;N, tag:Utf8, timestamp:Timestamp(ms)]\
            \n    Sort: metrics.tag ASC NULLS FIRST, metrics.timestamp ASC NULLS FIRST [field:Float64;N, tag:Utf8, timestamp:Timestamp(ms)]\
            \n      Filter: metrics.tag = Utf8(\"1\") AND metrics.timestamp >= TimestampMillisecond(-999, None) AND metrics.timestamp <= TimestampMillisecond(100000000, None) [field:Float64;N, tag:Utf8, timestamp:Timestamp(ms)]\
            \n        Projection: metrics.field, metrics.tag, CAST(metrics.timestamp AS Timestamp(ms)) AS timestamp [field:Float64;N, tag:Utf8, timestamp:Timestamp(ms)]\
            \n          TableScan: metrics [tag:Utf8, timestamp:Timestamp(ns), field:Float64;N]"
        );
        let plan = PromPlanner::stmt_to_plan(
            DfTableSourceProvider::new(
                catalog_list.clone(),
                false,
                QueryContext::arc(),
                DummyDecoder::arc(),
                true,
            ),
            &EvalStmt {
                expr: parser::parse("avg_over_time(metrics{tag = \"1\"}[5s])").unwrap(),
                start: UNIX_EPOCH,
                end: UNIX_EPOCH
                    .checked_add(Duration::from_secs(100_000))
                    .unwrap(),
                interval: Duration::from_secs(5),
                lookback_delta: Duration::from_secs(1),
            },
            &build_query_engine_state(),
        )
        .await
        .unwrap();
        assert_eq!(
            plan.display_indent_schema().to_string(),
            "Filter: prom_avg_over_time(timestamp_range,field) IS NOT NULL [timestamp:Timestamp(ms), prom_avg_over_time(timestamp_range,field):Float64;N, tag:Utf8]\
            \n  Projection: metrics.timestamp, prom_avg_over_time(timestamp_range, field) AS prom_avg_over_time(timestamp_range,field), metrics.tag [timestamp:Timestamp(ms), prom_avg_over_time(timestamp_range,field):Float64;N, tag:Utf8]\
            \n    PromRangeManipulate: req range=[0..100000000], interval=[5000], eval range=[5000], time index=[timestamp], values=[\"field\"] [field:Dictionary(Int64, Float64);N, tag:Utf8, timestamp:Timestamp(ms), timestamp_range:Dictionary(Int64, Timestamp(ms))]\
            \n      PromSeriesNormalize: offset=[0], time index=[timestamp], filter NaN: [true] [field:Float64;N, tag:Utf8, timestamp:Timestamp(ms)]\
            \n        PromSeriesDivide: tags=[\"tag\"] [field:Float64;N, tag:Utf8, timestamp:Timestamp(ms)]\
            \n          Sort: metrics.tag ASC NULLS FIRST, metrics.timestamp ASC NULLS FIRST [field:Float64;N, tag:Utf8, timestamp:Timestamp(ms)]\
            \n            Filter: metrics.tag = Utf8(\"1\") AND metrics.timestamp >= TimestampMillisecond(-4999, None) AND metrics.timestamp <= TimestampMillisecond(100000000, None) [field:Float64;N, tag:Utf8, timestamp:Timestamp(ms)]\
            \n              Projection: metrics.field, metrics.tag, CAST(metrics.timestamp AS Timestamp(ms)) AS timestamp [field:Float64;N, tag:Utf8, timestamp:Timestamp(ms)]\
            \n                TableScan: metrics [tag:Utf8, timestamp:Timestamp(ns), field:Float64;N]"
        );
    }

    #[tokio::test]
    async fn test_nonexistent_label() {
        // template
        let mut eval_stmt = EvalStmt {
            expr: PromExpr::NumberLiteral(NumberLiteral { val: 1.0 }),
            start: UNIX_EPOCH,
            end: UNIX_EPOCH
                .checked_add(Duration::from_secs(100_000))
                .unwrap(),
            interval: Duration::from_secs(5),
            lookback_delta: Duration::from_secs(1),
        };

        let case = r#"some_metric{nonexistent="hi"}"#;
        let prom_expr = parser::parse(case).unwrap();
        eval_stmt.expr = prom_expr;
        let table_provider = build_test_table_provider(
            &[(DEFAULT_SCHEMA_NAME.to_string(), "some_metric".to_string())],
            3,
            3,
        )
        .await;
        // Should be ok
        let _ = PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &build_query_engine_state())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_label_join() {
        let prom_expr = parser::parse(
            "label_join(up{tag_0='api-server'}, 'foo', ',', 'tag_1', 'tag_2', 'tag_3')",
        )
        .unwrap();
        let eval_stmt = EvalStmt {
            expr: prom_expr,
            start: UNIX_EPOCH,
            end: UNIX_EPOCH
                .checked_add(Duration::from_secs(100_000))
                .unwrap(),
            interval: Duration::from_secs(5),
            lookback_delta: Duration::from_secs(1),
        };

        let table_provider =
            build_test_table_provider(&[(DEFAULT_SCHEMA_NAME.to_string(), "up".to_string())], 4, 1)
                .await;
        let plan =
            PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &build_query_engine_state())
                .await
                .unwrap();

        let expected = r#"
Filter: up.field_0 IS NOT NULL [timestamp:Timestamp(ms), field_0:Float64;N, foo:Utf8;N, tag_0:Utf8, tag_1:Utf8, tag_2:Utf8, tag_3:Utf8]
  Projection: up.timestamp, up.field_0, concat_ws(Utf8(","), up.tag_1, up.tag_2, up.tag_3) AS foo, up.tag_0, up.tag_1, up.tag_2, up.tag_3 [timestamp:Timestamp(ms), field_0:Float64;N, foo:Utf8;N, tag_0:Utf8, tag_1:Utf8, tag_2:Utf8, tag_3:Utf8]
    PromInstantManipulate: range=[0..100000000], lookback=[1000], interval=[5000], time index=[timestamp] [tag_0:Utf8, tag_1:Utf8, tag_2:Utf8, tag_3:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]
      PromSeriesDivide: tags=["tag_0", "tag_1", "tag_2", "tag_3"] [tag_0:Utf8, tag_1:Utf8, tag_2:Utf8, tag_3:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]
        Sort: up.tag_0 ASC NULLS FIRST, up.tag_1 ASC NULLS FIRST, up.tag_2 ASC NULLS FIRST, up.tag_3 ASC NULLS FIRST, up.timestamp ASC NULLS FIRST [tag_0:Utf8, tag_1:Utf8, tag_2:Utf8, tag_3:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]
          Filter: up.tag_0 = Utf8("api-server") AND up.timestamp >= TimestampMillisecond(-999, None) AND up.timestamp <= TimestampMillisecond(100000000, None) [tag_0:Utf8, tag_1:Utf8, tag_2:Utf8, tag_3:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]
            TableScan: up [tag_0:Utf8, tag_1:Utf8, tag_2:Utf8, tag_3:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]"#;

        let ret = plan.display_indent_schema().to_string();
        assert_eq!(format!("\n{ret}"), expected, "\n{}", ret);
    }

    #[tokio::test]
    async fn test_label_replace() {
        let prom_expr = parser::parse(
            "label_replace(up{tag_0=\"a:c\"}, \"foo\", \"$1\", \"tag_0\", \"(.*):.*\")",
        )
        .unwrap();
        let eval_stmt = EvalStmt {
            expr: prom_expr,
            start: UNIX_EPOCH,
            end: UNIX_EPOCH
                .checked_add(Duration::from_secs(100_000))
                .unwrap(),
            interval: Duration::from_secs(5),
            lookback_delta: Duration::from_secs(1),
        };

        let table_provider =
            build_test_table_provider(&[(DEFAULT_SCHEMA_NAME.to_string(), "up".to_string())], 1, 1)
                .await;
        let plan =
            PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &build_query_engine_state())
                .await
                .unwrap();

        let expected = r#"
Filter: up.field_0 IS NOT NULL [timestamp:Timestamp(ms), field_0:Float64;N, foo:Utf8;N, tag_0:Utf8]
  Projection: up.timestamp, up.field_0, regexp_replace(up.tag_0, Utf8("^(?s:(.*):.*)$"), Utf8("$1")) AS foo, up.tag_0 [timestamp:Timestamp(ms), field_0:Float64;N, foo:Utf8;N, tag_0:Utf8]
    PromInstantManipulate: range=[0..100000000], lookback=[1000], interval=[5000], time index=[timestamp] [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]
      PromSeriesDivide: tags=["tag_0"] [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]
        Sort: up.tag_0 ASC NULLS FIRST, up.timestamp ASC NULLS FIRST [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]
          Filter: up.tag_0 = Utf8("a:c") AND up.timestamp >= TimestampMillisecond(-999, None) AND up.timestamp <= TimestampMillisecond(100000000, None) [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]
            TableScan: up [tag_0:Utf8, timestamp:Timestamp(ms), field_0:Float64;N]"#;

        let ret = plan.display_indent_schema().to_string();
        assert_eq!(format!("\n{ret}"), expected, "\n{}", ret);
    }

    #[tokio::test]
    async fn label_replace_aggregation_queries_plan_successfully() {
        let aggregate =
            r#"sum by (foo) (label_replace(some_metric, "foo", "$1", "tag_0", "(.*)"))"#;
        let queries = [
            aggregate.to_string(),
            format!("{aggregate} <= 10"),
            format!("{aggregate} * 0.8"),
            format!("0.8 * {aggregate}"),
            format!("{aggregate} <= {aggregate} * 0.8"),
        ];
        let state = build_query_engine_state();
        let mut failures = Vec::new();

        for query in queries {
            let table_provider = build_test_table_provider(
                &[(DEFAULT_SCHEMA_NAME.to_string(), "some_metric".to_string())],
                1,
                1,
            )
            .await;
            if let Err(error) =
                PromPlanner::stmt_to_plan(table_provider, &build_eval_stmt(&query), &state).await
            {
                failures.push(format!("{query}: {error:?}"));
            }
        }

        assert!(failures.is_empty(), "{}", failures.join("\n"));
    }

    #[tokio::test]
    async fn test_matchers_to_expr() {
        let mut eval_stmt = EvalStmt {
            expr: PromExpr::NumberLiteral(NumberLiteral { val: 1.0 }),
            start: UNIX_EPOCH,
            end: UNIX_EPOCH
                .checked_add(Duration::from_secs(100_000))
                .unwrap(),
            interval: Duration::from_secs(5),
            lookback_delta: Duration::from_secs(1),
        };
        let case =
            r#"sum(prometheus_tsdb_head_series{tag_1=~"(10.0.160.237:8080|10.0.160.237:9090)"})"#;

        let prom_expr = parser::parse(case).unwrap();
        eval_stmt.expr = prom_expr;
        let table_provider = build_test_table_provider(
            &[(
                DEFAULT_SCHEMA_NAME.to_string(),
                "prometheus_tsdb_head_series".to_string(),
            )],
            3,
            3,
        )
        .await;
        let plan =
            PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &build_query_engine_state())
                .await
                .unwrap();
        let expected = "Sort: prometheus_tsdb_head_series.timestamp ASC NULLS LAST [timestamp:Timestamp(ms), sum(prometheus_tsdb_head_series.field_0):Float64;N, sum(prometheus_tsdb_head_series.field_1):Float64;N, sum(prometheus_tsdb_head_series.field_2):Float64;N]\
        \n  Aggregate: groupBy=[[prometheus_tsdb_head_series.timestamp]], aggr=[[sum(prometheus_tsdb_head_series.field_0), sum(prometheus_tsdb_head_series.field_1), sum(prometheus_tsdb_head_series.field_2)]] [timestamp:Timestamp(ms), sum(prometheus_tsdb_head_series.field_0):Float64;N, sum(prometheus_tsdb_head_series.field_1):Float64;N, sum(prometheus_tsdb_head_series.field_2):Float64;N]\
        \n    PromInstantManipulate: range=[0..100000000], lookback=[1000], interval=[5000], time index=[timestamp] [tag_0:Utf8, tag_1:Utf8, tag_2:Utf8, timestamp:Timestamp(ms), field_0:Float64;N, field_1:Float64;N, field_2:Float64;N]\
        \n      PromSeriesDivide: tags=[\"tag_0\", \"tag_1\", \"tag_2\"] [tag_0:Utf8, tag_1:Utf8, tag_2:Utf8, timestamp:Timestamp(ms), field_0:Float64;N, field_1:Float64;N, field_2:Float64;N]\
        \n        Sort: prometheus_tsdb_head_series.tag_0 ASC NULLS FIRST, prometheus_tsdb_head_series.tag_1 ASC NULLS FIRST, prometheus_tsdb_head_series.tag_2 ASC NULLS FIRST, prometheus_tsdb_head_series.timestamp ASC NULLS FIRST [tag_0:Utf8, tag_1:Utf8, tag_2:Utf8, timestamp:Timestamp(ms), field_0:Float64;N, field_1:Float64;N, field_2:Float64;N]\
        \n          Filter: prometheus_tsdb_head_series.tag_1 ~ Utf8(\"^(?:(10.0.160.237:8080|10.0.160.237:9090))$\") AND prometheus_tsdb_head_series.timestamp >= TimestampMillisecond(-999, None) AND prometheus_tsdb_head_series.timestamp <= TimestampMillisecond(100000000, None) [tag_0:Utf8, tag_1:Utf8, tag_2:Utf8, timestamp:Timestamp(ms), field_0:Float64;N, field_1:Float64;N, field_2:Float64;N]\
        \n            TableScan: prometheus_tsdb_head_series [tag_0:Utf8, tag_1:Utf8, tag_2:Utf8, timestamp:Timestamp(ms), field_0:Float64;N, field_1:Float64;N, field_2:Float64;N]";
        assert_eq!(plan.display_indent_schema().to_string(), expected);
    }

    #[tokio::test]
    async fn test_topk_expr() {
        let mut eval_stmt = EvalStmt {
            expr: PromExpr::NumberLiteral(NumberLiteral { val: 1.0 }),
            start: UNIX_EPOCH,
            end: UNIX_EPOCH
                .checked_add(Duration::from_secs(100_000))
                .unwrap(),
            interval: Duration::from_secs(5),
            lookback_delta: Duration::from_secs(1),
        };
        let case = r#"topk(10, sum(prometheus_tsdb_head_series{ip=~"(10.0.160.237:8080|10.0.160.237:9090)"}) by (ip))"#;

        let prom_expr = parser::parse(case).unwrap();
        eval_stmt.expr = prom_expr;
        let table_provider = build_test_table_provider_with_fields(
            &[
                (
                    DEFAULT_SCHEMA_NAME.to_string(),
                    "prometheus_tsdb_head_series".to_string(),
                ),
                (
                    DEFAULT_SCHEMA_NAME.to_string(),
                    "http_server_requests_seconds_count".to_string(),
                ),
            ],
            &["ip"],
        )
        .await;

        let plan =
            PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &build_query_engine_state())
                .await
                .unwrap();
        let expected = "Projection: sum(prometheus_tsdb_head_series.greptime_value), prometheus_tsdb_head_series.ip, prometheus_tsdb_head_series.greptime_timestamp [sum(prometheus_tsdb_head_series.greptime_value):Float64;N, ip:Utf8, greptime_timestamp:Timestamp(ms)]\
        \n  Sort: prometheus_tsdb_head_series.greptime_timestamp ASC NULLS LAST, row_number() PARTITION BY [prometheus_tsdb_head_series.greptime_timestamp] ORDER BY [sum(prometheus_tsdb_head_series.greptime_value) DESC NULLS FIRST, prometheus_tsdb_head_series.ip DESC NULLS FIRST] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ASC NULLS LAST [ip:Utf8, greptime_timestamp:Timestamp(ms), sum(prometheus_tsdb_head_series.greptime_value):Float64;N, row_number() PARTITION BY [prometheus_tsdb_head_series.greptime_timestamp] ORDER BY [sum(prometheus_tsdb_head_series.greptime_value) DESC NULLS FIRST, prometheus_tsdb_head_series.ip DESC NULLS FIRST] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW:UInt64]\
        \n    Filter: row_number() PARTITION BY [prometheus_tsdb_head_series.greptime_timestamp] ORDER BY [sum(prometheus_tsdb_head_series.greptime_value) DESC NULLS FIRST, prometheus_tsdb_head_series.ip DESC NULLS FIRST] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW <= Float64(10) [ip:Utf8, greptime_timestamp:Timestamp(ms), sum(prometheus_tsdb_head_series.greptime_value):Float64;N, row_number() PARTITION BY [prometheus_tsdb_head_series.greptime_timestamp] ORDER BY [sum(prometheus_tsdb_head_series.greptime_value) DESC NULLS FIRST, prometheus_tsdb_head_series.ip DESC NULLS FIRST] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW:UInt64]\
        \n      WindowAggr: windowExpr=[[row_number() PARTITION BY [prometheus_tsdb_head_series.greptime_timestamp] ORDER BY [sum(prometheus_tsdb_head_series.greptime_value) DESC NULLS FIRST, prometheus_tsdb_head_series.ip DESC NULLS FIRST] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW]] [ip:Utf8, greptime_timestamp:Timestamp(ms), sum(prometheus_tsdb_head_series.greptime_value):Float64;N, row_number() PARTITION BY [prometheus_tsdb_head_series.greptime_timestamp] ORDER BY [sum(prometheus_tsdb_head_series.greptime_value) DESC NULLS FIRST, prometheus_tsdb_head_series.ip DESC NULLS FIRST] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW:UInt64]\
        \n        Sort: prometheus_tsdb_head_series.ip ASC NULLS LAST, prometheus_tsdb_head_series.greptime_timestamp ASC NULLS LAST [ip:Utf8, greptime_timestamp:Timestamp(ms), sum(prometheus_tsdb_head_series.greptime_value):Float64;N]\
        \n          Aggregate: groupBy=[[prometheus_tsdb_head_series.ip, prometheus_tsdb_head_series.greptime_timestamp]], aggr=[[sum(prometheus_tsdb_head_series.greptime_value)]] [ip:Utf8, greptime_timestamp:Timestamp(ms), sum(prometheus_tsdb_head_series.greptime_value):Float64;N]\
        \n            PromInstantManipulate: range=[0..100000000], lookback=[1000], interval=[5000], time index=[greptime_timestamp] [ip:Utf8, greptime_timestamp:Timestamp(ms), greptime_value:Float64;N]\
        \n              PromSeriesDivide: tags=[\"ip\"] [ip:Utf8, greptime_timestamp:Timestamp(ms), greptime_value:Float64;N]\
        \n                Sort: prometheus_tsdb_head_series.ip ASC NULLS FIRST, prometheus_tsdb_head_series.greptime_timestamp ASC NULLS FIRST [ip:Utf8, greptime_timestamp:Timestamp(ms), greptime_value:Float64;N]\
        \n                  Filter: prometheus_tsdb_head_series.ip ~ Utf8(\"^(?:(10.0.160.237:8080|10.0.160.237:9090))$\") AND prometheus_tsdb_head_series.greptime_timestamp >= TimestampMillisecond(-999, None) AND prometheus_tsdb_head_series.greptime_timestamp <= TimestampMillisecond(100000000, None) [ip:Utf8, greptime_timestamp:Timestamp(ms), greptime_value:Float64;N]\
        \n                    TableScan: prometheus_tsdb_head_series [ip:Utf8, greptime_timestamp:Timestamp(ms), greptime_value:Float64;N]";

        assert_eq!(plan.display_indent_schema().to_string(), expected);
    }

    #[tokio::test]
    async fn test_count_values_expr() {
        let mut eval_stmt = EvalStmt {
            expr: PromExpr::NumberLiteral(NumberLiteral { val: 1.0 }),
            start: UNIX_EPOCH,
            end: UNIX_EPOCH
                .checked_add(Duration::from_secs(100_000))
                .unwrap(),
            interval: Duration::from_secs(5),
            lookback_delta: Duration::from_secs(1),
        };
        let case = r#"count_values('series', prometheus_tsdb_head_series{ip=~"(10.0.160.237:8080|10.0.160.237:9090)"}) by (ip)"#;

        let prom_expr = parser::parse(case).unwrap();
        eval_stmt.expr = prom_expr;
        let table_provider = build_test_table_provider_with_fields(
            &[
                (
                    DEFAULT_SCHEMA_NAME.to_string(),
                    "prometheus_tsdb_head_series".to_string(),
                ),
                (
                    DEFAULT_SCHEMA_NAME.to_string(),
                    "http_server_requests_seconds_count".to_string(),
                ),
            ],
            &["ip"],
        )
        .await;

        let plan =
            PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &build_query_engine_state())
                .await
                .unwrap();
        let expected = "Sort: prometheus_tsdb_head_series.ip ASC NULLS LAST, prometheus_tsdb_head_series.greptime_timestamp ASC NULLS LAST, series ASC NULLS LAST [count(prometheus_tsdb_head_series.greptime_value):Int64, ip:Utf8, greptime_timestamp:Timestamp(ms), series:Float64;N]\
        \n  Projection: count(prometheus_tsdb_head_series.greptime_value), prometheus_tsdb_head_series.ip, prometheus_tsdb_head_series.greptime_timestamp, prometheus_tsdb_head_series.greptime_value AS series [count(prometheus_tsdb_head_series.greptime_value):Int64, ip:Utf8, greptime_timestamp:Timestamp(ms), series:Float64;N]\
        \n    Aggregate: groupBy=[[prometheus_tsdb_head_series.ip, prometheus_tsdb_head_series.greptime_timestamp, prometheus_tsdb_head_series.greptime_value]], aggr=[[count(prometheus_tsdb_head_series.greptime_value)]] [ip:Utf8, greptime_timestamp:Timestamp(ms), greptime_value:Float64;N, count(prometheus_tsdb_head_series.greptime_value):Int64]\
        \n      PromInstantManipulate: range=[0..100000000], lookback=[1000], interval=[5000], time index=[greptime_timestamp] [ip:Utf8, greptime_timestamp:Timestamp(ms), greptime_value:Float64;N]\
        \n        PromSeriesDivide: tags=[\"ip\"] [ip:Utf8, greptime_timestamp:Timestamp(ms), greptime_value:Float64;N]\
        \n          Sort: prometheus_tsdb_head_series.ip ASC NULLS FIRST, prometheus_tsdb_head_series.greptime_timestamp ASC NULLS FIRST [ip:Utf8, greptime_timestamp:Timestamp(ms), greptime_value:Float64;N]\
        \n            Filter: prometheus_tsdb_head_series.ip ~ Utf8(\"^(?:(10.0.160.237:8080|10.0.160.237:9090))$\") AND prometheus_tsdb_head_series.greptime_timestamp >= TimestampMillisecond(-999, None) AND prometheus_tsdb_head_series.greptime_timestamp <= TimestampMillisecond(100000000, None) [ip:Utf8, greptime_timestamp:Timestamp(ms), greptime_value:Float64;N]\
        \n              TableScan: prometheus_tsdb_head_series [ip:Utf8, greptime_timestamp:Timestamp(ms), greptime_value:Float64;N]";

        assert_eq!(plan.display_indent_schema().to_string(), expected);
    }

    #[tokio::test]
    async fn test_value_alias() {
        let mut eval_stmt = EvalStmt {
            expr: PromExpr::NumberLiteral(NumberLiteral { val: 1.0 }),
            start: UNIX_EPOCH,
            end: UNIX_EPOCH
                .checked_add(Duration::from_secs(100_000))
                .unwrap(),
            interval: Duration::from_secs(5),
            lookback_delta: Duration::from_secs(1),
        };
        let case = r#"count_values('series', prometheus_tsdb_head_series{ip=~"(10.0.160.237:8080|10.0.160.237:9090)"}) by (ip)"#;

        let prom_expr = parser::parse(case).unwrap();
        eval_stmt.expr = prom_expr;
        eval_stmt = QueryLanguageParser::apply_alias_extension(eval_stmt, "my_series");
        let table_provider = build_test_table_provider_with_fields(
            &[
                (
                    DEFAULT_SCHEMA_NAME.to_string(),
                    "prometheus_tsdb_head_series".to_string(),
                ),
                (
                    DEFAULT_SCHEMA_NAME.to_string(),
                    "http_server_requests_seconds_count".to_string(),
                ),
            ],
            &["ip"],
        )
        .await;

        let plan =
            PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &build_query_engine_state())
                .await
                .unwrap();
        let expected = r#"
Projection: count(prometheus_tsdb_head_series.greptime_value) AS my_series, prometheus_tsdb_head_series.ip, prometheus_tsdb_head_series.greptime_timestamp [my_series:Int64, ip:Utf8, greptime_timestamp:Timestamp(ms)]
  Sort: prometheus_tsdb_head_series.ip ASC NULLS LAST, prometheus_tsdb_head_series.greptime_timestamp ASC NULLS LAST, series ASC NULLS LAST [count(prometheus_tsdb_head_series.greptime_value):Int64, ip:Utf8, greptime_timestamp:Timestamp(ms), series:Float64;N]
    Projection: count(prometheus_tsdb_head_series.greptime_value), prometheus_tsdb_head_series.ip, prometheus_tsdb_head_series.greptime_timestamp, prometheus_tsdb_head_series.greptime_value AS series [count(prometheus_tsdb_head_series.greptime_value):Int64, ip:Utf8, greptime_timestamp:Timestamp(ms), series:Float64;N]
      Aggregate: groupBy=[[prometheus_tsdb_head_series.ip, prometheus_tsdb_head_series.greptime_timestamp, prometheus_tsdb_head_series.greptime_value]], aggr=[[count(prometheus_tsdb_head_series.greptime_value)]] [ip:Utf8, greptime_timestamp:Timestamp(ms), greptime_value:Float64;N, count(prometheus_tsdb_head_series.greptime_value):Int64]
        PromInstantManipulate: range=[0..100000000], lookback=[1000], interval=[5000], time index=[greptime_timestamp] [ip:Utf8, greptime_timestamp:Timestamp(ms), greptime_value:Float64;N]
          PromSeriesDivide: tags=["ip"] [ip:Utf8, greptime_timestamp:Timestamp(ms), greptime_value:Float64;N]
            Sort: prometheus_tsdb_head_series.ip ASC NULLS FIRST, prometheus_tsdb_head_series.greptime_timestamp ASC NULLS FIRST [ip:Utf8, greptime_timestamp:Timestamp(ms), greptime_value:Float64;N]
              Filter: prometheus_tsdb_head_series.ip ~ Utf8("^(?:(10.0.160.237:8080|10.0.160.237:9090))$") AND prometheus_tsdb_head_series.greptime_timestamp >= TimestampMillisecond(-999, None) AND prometheus_tsdb_head_series.greptime_timestamp <= TimestampMillisecond(100000000, None) [ip:Utf8, greptime_timestamp:Timestamp(ms), greptime_value:Float64;N]
                TableScan: prometheus_tsdb_head_series [ip:Utf8, greptime_timestamp:Timestamp(ms), greptime_value:Float64;N]"#;
        assert_eq!(format!("\n{}", plan.display_indent_schema()), expected);
    }

    #[tokio::test]
    async fn test_quantile_expr() {
        let mut eval_stmt = EvalStmt {
            expr: PromExpr::NumberLiteral(NumberLiteral { val: 1.0 }),
            start: UNIX_EPOCH,
            end: UNIX_EPOCH
                .checked_add(Duration::from_secs(100_000))
                .unwrap(),
            interval: Duration::from_secs(5),
            lookback_delta: Duration::from_secs(1),
        };
        let case = r#"quantile(0.3, sum(prometheus_tsdb_head_series{ip=~"(10.0.160.237:8080|10.0.160.237:9090)"}) by (ip))"#;

        let prom_expr = parser::parse(case).unwrap();
        eval_stmt.expr = prom_expr;
        let table_provider = build_test_table_provider_with_fields(
            &[
                (
                    DEFAULT_SCHEMA_NAME.to_string(),
                    "prometheus_tsdb_head_series".to_string(),
                ),
                (
                    DEFAULT_SCHEMA_NAME.to_string(),
                    "http_server_requests_seconds_count".to_string(),
                ),
            ],
            &["ip"],
        )
        .await;

        let plan =
            PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &build_query_engine_state())
                .await
                .unwrap();
        let expected = "Sort: prometheus_tsdb_head_series.greptime_timestamp ASC NULLS LAST [greptime_timestamp:Timestamp(ms), quantile(Float64(0.3),sum(prometheus_tsdb_head_series.greptime_value)):Float64;N]\
        \n  Aggregate: groupBy=[[prometheus_tsdb_head_series.greptime_timestamp]], aggr=[[quantile(Float64(0.3), sum(prometheus_tsdb_head_series.greptime_value))]] [greptime_timestamp:Timestamp(ms), quantile(Float64(0.3),sum(prometheus_tsdb_head_series.greptime_value)):Float64;N]\
        \n    Sort: prometheus_tsdb_head_series.ip ASC NULLS LAST, prometheus_tsdb_head_series.greptime_timestamp ASC NULLS LAST [ip:Utf8, greptime_timestamp:Timestamp(ms), sum(prometheus_tsdb_head_series.greptime_value):Float64;N]\
        \n      Aggregate: groupBy=[[prometheus_tsdb_head_series.ip, prometheus_tsdb_head_series.greptime_timestamp]], aggr=[[sum(prometheus_tsdb_head_series.greptime_value)]] [ip:Utf8, greptime_timestamp:Timestamp(ms), sum(prometheus_tsdb_head_series.greptime_value):Float64;N]\
        \n        PromInstantManipulate: range=[0..100000000], lookback=[1000], interval=[5000], time index=[greptime_timestamp] [ip:Utf8, greptime_timestamp:Timestamp(ms), greptime_value:Float64;N]\
        \n          PromSeriesDivide: tags=[\"ip\"] [ip:Utf8, greptime_timestamp:Timestamp(ms), greptime_value:Float64;N]\
        \n            Sort: prometheus_tsdb_head_series.ip ASC NULLS FIRST, prometheus_tsdb_head_series.greptime_timestamp ASC NULLS FIRST [ip:Utf8, greptime_timestamp:Timestamp(ms), greptime_value:Float64;N]\
        \n              Filter: prometheus_tsdb_head_series.ip ~ Utf8(\"^(?:(10.0.160.237:8080|10.0.160.237:9090))$\") AND prometheus_tsdb_head_series.greptime_timestamp >= TimestampMillisecond(-999, None) AND prometheus_tsdb_head_series.greptime_timestamp <= TimestampMillisecond(100000000, None) [ip:Utf8, greptime_timestamp:Timestamp(ms), greptime_value:Float64;N]\
        \n                TableScan: prometheus_tsdb_head_series [ip:Utf8, greptime_timestamp:Timestamp(ms), greptime_value:Float64;N]";

        assert_eq!(plan.display_indent_schema().to_string(), expected);
    }

    #[tokio::test]
    async fn test_or_not_exists_table_label() {
        let state = build_query_engine_state();
        let provider = build_test_table_provider_with_fields(
            &[(DEFAULT_SCHEMA_NAME.to_string(), "normal_metric".to_string())],
            &["job"],
        )
        .await;
        let raw = PromPlanner::stmt_to_plan(
            provider,
            &build_eval_stmt(r#"missing_metric or on(absent_label) normal_metric"#),
            &state,
        )
        .await
        .unwrap();
        assert!(
            raw.display_indent_schema()
                .to_string()
                .contains("__promql_or_match_0@")
        );
        let (optimized, batches) = execute(raw, &state).await;
        assert_no_internal_or_keys(optimized.schema());
        assert!(batches.iter().all(|batch| {
            batch
                .schema()
                .fields()
                .iter()
                .all(|field| !field.name().starts_with("__promql_or_match_"))
        }));
    }

    #[tokio::test]
    async fn test_histogram_quantile_missing_le_column() {
        let mut eval_stmt = EvalStmt {
            expr: PromExpr::NumberLiteral(NumberLiteral { val: 1.0 }),
            start: UNIX_EPOCH,
            end: UNIX_EPOCH
                .checked_add(Duration::from_secs(100_000))
                .unwrap(),
            interval: Duration::from_secs(5),
            lookback_delta: Duration::from_secs(1),
        };

        // Test case: histogram_quantile with a table that doesn't have 'le' column
        let case = r#"histogram_quantile(0.99, sum by(pod,instance,le) (rate(non_existent_histogram_bucket{instance=~"xxx"}[1m])))"#;

        let prom_expr = parser::parse(case).unwrap();
        eval_stmt.expr = prom_expr;

        // Create a table provider with a table that doesn't have 'le' column
        let table_provider = build_test_table_provider_with_fields(
            &[(
                DEFAULT_SCHEMA_NAME.to_string(),
                "non_existent_histogram_bucket".to_string(),
            )],
            &["pod", "instance"], // Note: no 'le' column
        )
        .await;

        // Should return empty result instead of error
        let result =
            PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &build_query_engine_state())
                .await;

        // This should succeed now (returning empty result) instead of failing with "Cannot find column le"
        assert!(
            result.is_ok(),
            "Expected successful plan creation with empty result, but got error: {:?}",
            result.err()
        );

        // Verify that the result is an EmptyRelation
        let plan = result.unwrap();
        match plan {
            LogicalPlan::EmptyRelation(_) => {
                // This is what we expect
            }
            _ => panic!("Expected EmptyRelation, but got: {:?}", plan),
        }
    }

    #[tokio::test]
    async fn test_direct_or_normalizes_missing_match_labels() {
        type Case<'a> = (
            Option<Option<&'a str>>,
            Option<Option<&'a str>>,
            i64,
            i64,
            &'a [(f64, Option<&'a str>)],
        );

        let modifier = or_modifier("lhs or on(k) rhs");
        #[rustfmt::skip]
        let cases: &[Case<'_>] = &[
            (None, None, 1, 1, &[(1.0, None)]),
            (None, Some(Some("")), 1, 1, &[(1.0, None)]),
            (Some(Some("")), None, 1, 1, &[(1.0, Some(""))]),
            (None, Some(Some("r")), 1, 1, &[(1.0, None), (2.0, Some("r"))]),
            (Some(Some("l")), None, 1, 1, &[(1.0, Some("l")), (2.0, None)]),
            (Some(None), Some(Some("")), 1, 1, &[(1.0, None)]),
            (Some(None), Some(Some("r")), 1, 1, &[(1.0, None), (2.0, Some("r"))]),
            (Some(Some("same")), Some(Some("same")), 1, 2, &[(1.0, Some("same")), (2.0, Some("same"))]),
        ];
        for &(left, right, left_ts, right_ts, expected) in cases {
            let (optimized, batches) = run(
                &matrix_source("lhs", left, left_ts, 1.0),
                &matrix_source("rhs", right, right_ts, 2.0),
                matrix_context("lhs", left),
                matrix_context("rhs", right),
                &modifier,
            )
            .await;
            assert_no_internal_or_keys(optimized.schema());
            assert_eq!(
                rows(&batches),
                expected
                    .iter()
                    .map(|(value, label)| (*value, label.map(str::to_string)))
                    .collect::<Vec<_>>()
            );
        }
    }

    #[tokio::test]
    async fn test_direct_or_match_modifiers() {
        for (modifier, left, right, expected) in [
            (None, "left", "right", 2),
            (or_modifier("lhs or on(k) rhs"), "same", "same", 1),
            (or_modifier("lhs or on() rhs"), "left", "right", 1),
            (or_modifier("lhs or ignoring(k) rhs"), "left", "right", 1),
        ] {
            let (_, batches) = run(
                &matrix_source("lhs", Some(Some(left)), 1, 1.0),
                &matrix_source("rhs", Some(Some(right)), 1, 2.0),
                direct_or_context("lhs", &["job", "k"], "v"),
                direct_or_context("rhs", &["job", "k"], "v"),
                &modifier,
            )
            .await;
            assert_eq!(
                batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
                expected
            );
        }
    }

    #[tokio::test]
    async fn test_direct_or_nested_projection_uses_left_context() {
        let left = matrix_source("lhs", Some(Some("k")), 1, 1.0);
        let right = matrix_source("rhs", Some(Some("k")), 1, 2.0);
        let raw = plan_direct_or(
            scan(&left),
            scan(&right),
            direct_or_context("lhs", &["job", "k"], "v"),
            direct_or_context("rhs", &["job", "k"], "v"),
            &or_modifier("lhs or on(k) rhs"),
        )
        .await;
        assert!(raw.schema().iter().any(|(qualifier, field)| {
            qualifier.as_ref().is_some_and(|q| q.to_string() == "lhs") && field.name() == "v"
        }));
        let nested = LogicalPlanBuilder::from(raw)
            .project(vec![
                DfExpr::BinaryExpr(BinaryExpr {
                    left: Box::new(DfExpr::Column(Column::new(
                        Some(TableReference::bare("lhs")),
                        "v",
                    ))),
                    op: Operator::Plus,
                    right: Box::new(lit(1.0)),
                })
                .alias("v_plus"),
            ])
            .unwrap()
            .build()
            .unwrap();
        let (_, batches) = execute(nested, &build_query_engine_state()).await;
        assert_eq!(values(&batches, "v_plus"), vec![2.0]);
    }

    #[tokio::test]
    async fn test_direct_or_skips_user_internal_key_name() {
        const USER_TAG: &str = "__promql_or_match_0";
        let left = tagged_source(
            "lhs",
            false,
            (USER_TAG, Some("left")),
            DirectOrValue::Float64(1.0),
        );
        let right = tagged_source(
            "rhs",
            false,
            (USER_TAG, Some("right")),
            DirectOrValue::Float64(2.0),
        );
        let raw = plan_direct_or(
            scan(&left),
            scan(&right),
            direct_or_context("lhs", &["job", USER_TAG], "v"),
            direct_or_context("rhs", &["job", USER_TAG], "v"),
            &or_modifier("lhs or on(missing_label) rhs"),
        )
        .await;
        assert!(
            raw.display_indent_schema()
                .to_string()
                .contains("__promql_or_match_1@")
        );
        let (_, batches) = execute(raw, &build_query_engine_state()).await;
        assert!(
            batches
                .iter()
                .all(|batch| batch.column_by_name(USER_TAG).is_some())
        );
    }

    #[tokio::test]
    async fn test_direct_or_substrait_round_trip_with_normalized_key() {
        let state = build_query_engine_state();
        let ctx = SessionContext::new_with_state(state.session_state());
        let catalog = Arc::new(MemoryCatalogProvider::new());
        catalog
            .register_schema("public", Arc::new(MemorySchemaProvider::new()))
            .unwrap();
        ctx.register_catalog("datafusion", catalog);
        let left = matrix_source("lhs", Some(Some("")), 1, 1.0);
        let right = matrix_source("rhs", None, 1, 2.0);
        ctx.register_table(
            TableReference::full("datafusion", "public", "lhs"),
            table(&left),
        )
        .unwrap();
        ctx.register_table(
            TableReference::full("datafusion", "public", "rhs"),
            table(&right),
        )
        .unwrap();
        let raw = plan_direct_or(
            ctx.table("datafusion.public.lhs")
                .await
                .unwrap()
                .into_unoptimized_plan(),
            ctx.table("datafusion.public.rhs")
                .await
                .unwrap()
                .into_unoptimized_plan(),
            direct_or_context("lhs", &["job", "k"], "v"),
            direct_or_context("rhs", &["job"], "v"),
            &or_modifier("lhs or on(k) rhs"),
        )
        .await;
        let decoded = DFLogicalSubstraitConvertor
            .decode(
                DFLogicalSubstraitConvertor
                    .encode(&raw, DefaultSerializer)
                    .unwrap(),
                ctx.state(),
            )
            .await
            .unwrap();
        let (optimized, batches) = execute(decoded, &state).await;
        assert_no_internal_or_keys(optimized.schema());
        assert!(batches.iter().all(|batch| {
            batch
                .schema()
                .fields()
                .iter()
                .all(|field| !field.name().starts_with("__promql_or_match_"))
        }));
        assert_eq!(values(&batches, "v"), vec![1.0]);
    }

    #[tokio::test]
    async fn test_direct_or_numeric_value_types() {
        let left = tagged_source("lhs", true, ("k", Some("lhs")), DirectOrValue::Int64(0));
        let right = tagged_source(
            "rhs",
            false,
            ("k", Some("rhs")),
            DirectOrValue::Float64(0.5),
        );
        let (optimized, batches) = run(
            &left,
            &right,
            direct_or_context("lhs", &["job", "k"], "v"),
            direct_or_context("rhs", &["job", "k"], "v"),
            &or_modifier("lhs or on(k) rhs"),
        )
        .await;
        assert_eq!(
            optimized
                .schema()
                .field_with_name(None, "v")
                .unwrap()
                .data_type(),
            &ArrowDataType::Float64
        );
        assert_eq!(values(&batches, "v"), vec![0.5]);
        let provider = build_test_table_provider_with_fields(
            &[(DEFAULT_SCHEMA_NAME.to_string(), "dummy".to_string())],
            &[],
        )
        .await;
        let mut planner = PromPlanner {
            table_provider: provider,
            ctx: PromPlannerContext::default(),
            promql_annotations: None,
        };
        let left_context = direct_or_context("lhs", &["job"], "v");
        let right_context = direct_or_context("rhs", &["job"], "v");
        let error = planner
            .or_operator(
                scan(&job_source("lhs", DirectOrValue::Utf8("x"))),
                scan(&job_source("rhs", DirectOrValue::Float64(1.0))),
                left_context.tag_columns.iter().cloned().collect(),
                right_context.tag_columns.iter().cloned().collect(),
                left_context,
                right_context,
                &or_modifier("lhs or on() rhs"),
            )
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("OR value fields have incompatible types")
        );
    }

    #[tokio::test]
    async fn test_or_with_histogram_quantile_missing_le_column() {
        let case = r#"histogram_quantile(0.99, non_existent_histogram_bucket) or normal_metric"#;
        let eval_stmt = build_eval_stmt(case);
        let table_provider = build_missing_le_or_normal_metric_table_provider().await;

        let plan =
            PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &build_query_engine_state())
                .await
                .unwrap();
        assert_normal_metric_schema(&plan);
    }

    #[tokio::test]
    async fn test_or_with_right_empty_histogram_restores_left_context() {
        let eval_stmt = build_eval_stmt(
            r#"abs(sum by(instance) (normal_metric) or histogram_quantile(0.99, sum by(pod) (non_existent_histogram_bucket)))"#,
        );
        let table_provider = build_missing_le_or_normal_metric_table_provider().await;

        PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &build_query_engine_state())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_or_with_both_empty_histograms() {
        let eval_stmt = build_eval_stmt(
            r#"histogram_quantile(0.99, sum by(pod) (left_histogram_bucket)) or histogram_quantile(0.99, sum by(instance) (right_histogram_bucket))"#,
        );
        let table_provider = build_test_table_provider_with_fields(
            &[
                (
                    DEFAULT_SCHEMA_NAME.to_string(),
                    "left_histogram_bucket".to_string(),
                ),
                (
                    DEFAULT_SCHEMA_NAME.to_string(),
                    "right_histogram_bucket".to_string(),
                ),
            ],
            &["pod", "instance"],
        )
        .await;

        let plan =
            PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &build_query_engine_state())
                .await
                .unwrap();
        match plan {
            LogicalPlan::EmptyRelation(relation) => {
                assert!(!relation.produce_one_row);
                assert!(!relation.schema.fields().is_empty());
                assert!(
                    relation
                        .schema
                        .fields()
                        .iter()
                        .any(|field| field.data_type() == &ArrowDataType::Float64)
                );
                assert!(
                    relation
                        .schema
                        .fields()
                        .iter()
                        .any(|field| field.name() == "pod")
                );
                assert!(
                    !relation
                        .schema
                        .fields()
                        .iter()
                        .any(|field| field.name() == "instance")
                );
            }
            _ => panic!("Expected EmptyRelation, but got: {plan:?}"),
        }
    }

    #[tokio::test]
    async fn test_nested_or_with_both_empty_histograms() {
        for case in [
            r#"abs(histogram_quantile(0.99, left_histogram_bucket) or histogram_quantile(0.99, right_histogram_bucket))"#,
            r#"(histogram_quantile(0.99, left_histogram_bucket) or histogram_quantile(0.99, right_histogram_bucket)) + 1"#,
        ] {
            let eval_stmt = build_eval_stmt(case);
            let table_provider = build_test_table_provider_with_fields(
                &[
                    (
                        DEFAULT_SCHEMA_NAME.to_string(),
                        "left_histogram_bucket".to_string(),
                    ),
                    (
                        DEFAULT_SCHEMA_NAME.to_string(),
                        "right_histogram_bucket".to_string(),
                    ),
                ],
                &["pod", "instance"],
            )
            .await;

            PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &build_query_engine_state())
                .await
                .unwrap();
        }
    }

    #[tokio::test]
    async fn test_or_with_empty_histogram_modifiers() {
        for case in [
            r#"histogram_quantile(0.99, non_existent_histogram_bucket) or on(pod) normal_metric"#,
            r#"normal_metric or ignoring(instance) histogram_quantile(0.99, non_existent_histogram_bucket)"#,
        ] {
            let eval_stmt = build_eval_stmt(case);
            let table_provider = build_missing_le_or_normal_metric_table_provider().await;

            let plan =
                PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &build_query_engine_state())
                    .await
                    .unwrap();
            assert_normal_metric_schema(&plan);
        }
    }

    #[tokio::test]
    async fn test_unless_preserves_left_context_for_histogram() {
        let eval_stmt = build_eval_stmt(
            r#"histogram_quantile(0.99, bucket_metric unless on(job) normal_metric) or fallback_metric"#,
        );
        let state = build_query_engine_state();
        let plan = PromPlanner::stmt_to_plan(
            build_set_op_context_table_provider().await,
            &eval_stmt,
            &state,
        )
        .await
        .unwrap();
        assert!(contains_histogram_fold(&plan), "{plan:?}");
        let (optimized, physical) = optimize_and_create_physical_plan(&state, plan).await;
        assert!(contains_histogram_fold(&optimized), "{optimized:?}");
        let batches =
            datafusion::physical_plan::collect(physical, state.session_state().task_ctx())
                .await
                .unwrap();
        assert!(batches.iter().all(|batch| batch.num_rows() == 0));
    }

    #[tokio::test]
    async fn test_and_preserves_left_context_for_histogram() {
        let eval_stmt = build_eval_stmt(
            r#"histogram_quantile(0.99, bucket_metric and on(job) normal_metric) or fallback_metric"#,
        );
        let plan = PromPlanner::stmt_to_plan(
            build_set_op_context_table_provider().await,
            &eval_stmt,
            &build_query_engine_state(),
        )
        .await
        .unwrap();
        assert!(contains_histogram_fold(&plan), "{plan:?}");
    }

    #[tokio::test]
    async fn test_and_preserves_left_context_when_le_is_missing() {
        let eval_stmt =
            build_eval_stmt(r#"histogram_quantile(0.99, normal_metric and on(job) bucket_metric)"#);
        let plan = PromPlanner::stmt_to_plan(
            build_set_op_context_table_provider().await,
            &eval_stmt,
            &build_query_engine_state(),
        )
        .await
        .unwrap();
        assert!(matches!(&plan, LogicalPlan::EmptyRelation(_)), "{plan:?}");
        assert!(!plan.schema().fields().is_empty());
        assert!(!contains_histogram_fold(&plan), "{plan:?}");
    }

    #[tokio::test]
    async fn test_or_context_uses_left_qualified_output() {
        let case = r#"(normal_metric or other_metric) + 1"#;
        let eval_stmt = build_eval_stmt(case);
        let state = build_query_engine_state();
        let plan =
            PromPlanner::stmt_to_plan(build_or_context_table_provider().await, &eval_stmt, &state)
                .await
                .unwrap();
        assert!(
            plan.schema()
                .fields()
                .iter()
                .any(|field| field.data_type() == &ArrowDataType::Float64),
            "{plan:?}"
        );
        let (_optimized, _physical) = optimize_and_create_physical_plan(&state, plan).await;
    }

    #[tokio::test]
    async fn test_or_context_uses_left_qualified_empty_histogram_output() {
        let case = r#"(abs(histogram_quantile(0.99, non_hist_metric)) or normal_metric) + 1"#;
        let eval_stmt = build_eval_stmt(case);
        let plan = PromPlanner::stmt_to_plan(
            build_or_context_table_provider().await,
            &eval_stmt,
            &build_query_engine_state(),
        )
        .await
        .unwrap();
        assert!(
            plan.schema()
                .fields()
                .iter()
                .any(|field| field.data_type() == &ArrowDataType::Float64),
            "{plan:?}"
        );
    }

    #[tokio::test]
    async fn test_direct_or_preserves_float_and_native_histogram_samples() {
        for histogram_on_left in [false, true] {
            let (planner, plan) = mixed_direct_or(histogram_on_left).await;

            let float_field = &planner.ctx.field_columns[0];
            let histogram_field = &planner.ctx.field_columns[1];
            assert!(float_field.starts_with(OR_FLOAT_FIELD_PREFIX));
            assert!(histogram_field.starts_with(OR_HISTOGRAM_FIELD_PREFIX));
            assert_eq!(
                plan.schema()
                    .field_with_name(None, float_field)
                    .unwrap()
                    .data_type(),
                &ArrowDataType::Float64
            );
            assert_eq!(
                plan.schema()
                    .field_with_name(None, histogram_field)
                    .unwrap()
                    .data_type(),
                &native_histogram_value_type().as_arrow_type()
            );

            let (optimized, batches) = execute(plan, &build_query_engine_state()).await;
            assert_no_internal_or_keys(optimized.schema());
            let mut sample_kinds = batches
                .iter()
                .flat_map(|batch| {
                    let values = batch.column_by_name(float_field).unwrap();
                    let histograms = batch.column_by_name(histogram_field).unwrap();
                    (0..batch.num_rows())
                        .map(|row| (values.is_valid(row), histograms.is_valid(row)))
                })
                .collect::<Vec<_>>();
            sample_kinds.sort_unstable();
            assert_eq!(sample_kinds, vec![(false, true), (true, false)]);
        }
    }

    #[tokio::test]
    async fn malformed_classic_bucket_does_not_drop_native_histogram() {
        let state = build_query_engine_state();
        let collector = PromqlAnnotationCollector::default();
        let plan = PromPlanner::stmt_to_plan_with_annotations(
            operator_table_provider(),
            &operator_eval_stmt("histogram_quantile(0.5, bad_classic or bad_native)"),
            &state,
            Some(collector.clone()),
        )
        .await
        .unwrap();
        let value_field = plan
            .schema()
            .fields()
            .iter()
            .find(|field| field.data_type() == &ArrowDataType::Float64)
            .unwrap()
            .name()
            .clone();

        let (_, batches) = execute(plan, &state).await;
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
        assert_eq!(values(&batches, &value_field), vec![0.0]);
        let mut warnings = vec![];
        let mut infos = vec![];
        collector.append_to(&mut warnings, &mut infos);
        assert!(warnings.is_empty());
        assert!(infos.is_empty());
    }

    #[tokio::test]
    async fn test_mixed_binary_operator_aligns_both_alternative_inputs() {
        let state = build_query_engine_state();
        let plan = PromPlanner::stmt_to_plan(
            operator_table_provider(),
            &operator_eval_stmt("(lf or on(tag) lh) * on(tag) (rf or on(tag) rh)"),
            &state,
        )
        .await
        .unwrap();
        let plan_text = plan.display_indent_schema().to_string();
        assert!(
            plan_text.contains("prom_native_histogram_mul_scalar"),
            "{plan_text}"
        );
        assert!(
            plan_text.contains("prom_native_histogram_scalar_mul"),
            "{plan_text}"
        );
        let float_field = plan
            .schema()
            .fields()
            .iter()
            .find(|field| field.name().starts_with(OR_FLOAT_FIELD_PREFIX))
            .unwrap()
            .name()
            .clone();
        let histogram_field = plan
            .schema()
            .fields()
            .iter()
            .find(|field| field.name().starts_with(OR_HISTOGRAM_FIELD_PREFIX))
            .unwrap()
            .name()
            .clone();

        let (_, batches) = execute(plan, &state).await;
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
        assert!(values(&batches, &float_field).is_empty());
        let mut sums = histograms(&batches, &histogram_field)
            .into_iter()
            .map(|histogram| histogram.sum)
            .collect::<Vec<_>>();
        sums.sort_by(f64::total_cmp);
        assert_eq!(sums, vec![2.0, 3.0]);
    }

    #[tokio::test]
    async fn test_mixed_binary_operator_reports_only_dropped_samples() {
        for (query, expected_rows, expected_infos) in [
            ("(lf or on(tag) lh) + on(tag) (rf or on(tag) rh)", 0, 1),
            ("(lf or on(tag) lh) + on(tag) (lf or on(tag) lh)", 2, 0),
            ("(lf or on(tag) lh) % on(tag) lh", 0, 1),
        ] {
            let state = build_query_engine_state();
            let annotations = PromqlAnnotationCollector::default();
            let plan = PromPlanner::stmt_to_plan_with_annotations(
                operator_table_provider(),
                &operator_eval_stmt(query),
                &state,
                Some(annotations.clone()),
            )
            .await
            .unwrap();

            let (_, batches) = execute(plan, &state).await;
            assert_eq!(
                batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
                expected_rows,
                "{query}"
            );
            let mut warnings = vec![];
            let mut infos = vec![];
            annotations.append_to(&mut warnings, &mut infos);
            assert!(warnings.is_empty(), "{query}: {warnings:?}");
            assert_eq!(infos.len(), expected_infos, "{query}: {infos:?}");
        }
    }

    #[tokio::test]
    async fn test_histogram_only_min_drops_empty_aggregate_group() {
        // `min` over native-histogram-only input drops every sample in the group, so the
        // NULL-valued aggregate row must be filtered out. Otherwise an outer expression
        // like `group()` resurrects the group Prometheus considers unseen.
        let state = build_query_engine_state();
        for query in ["min(lh)", "group(min(lh))"] {
            let plan = PromPlanner::stmt_to_plan(
                operator_table_provider(),
                &operator_eval_stmt(query),
                &state,
            )
            .await
            .unwrap();
            let (_, batches) = execute(plan, &state).await;
            assert_eq!(
                batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
                0,
                "{query}"
            );
        }
    }

    #[tokio::test]
    async fn test_mixed_min_drops_histogram_only_group() {
        // With alternative float/histogram fields, `min by (tag)` keeps float-only groups
        // (tag=a from `lf`) and drops histogram-only groups (tag=b from `lh`) instead of
        // emitting a NULL-valued row for them.
        let state = build_query_engine_state();
        let plan = PromPlanner::stmt_to_plan(
            operator_table_provider(),
            &operator_eval_stmt("min by (tag) (lf or on(tag) lh)"),
            &state,
        )
        .await
        .unwrap();
        let float_field = plan
            .schema()
            .fields()
            .iter()
            .find(|field| field.data_type() == &ArrowDataType::Float64)
            .unwrap()
            .name()
            .clone();
        let (_, batches) = execute(plan, &state).await;
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
        assert_eq!(values(&batches, &float_field), vec![2.0]);
    }

    #[tokio::test]
    async fn test_mixed_or_can_feed_another_or() {
        let state = build_query_engine_state();
        let plan = PromPlanner::stmt_to_plan(
            operator_table_provider(),
            &operator_eval_stmt("lf or on(tag) lh or on(tag) fallback"),
            &state,
        )
        .await
        .unwrap();
        let float_field = plan
            .schema()
            .fields()
            .iter()
            .find(|field| field.name().starts_with(OR_FLOAT_FIELD_PREFIX))
            .unwrap()
            .name()
            .clone();
        let histogram_field = plan
            .schema()
            .fields()
            .iter()
            .find(|field| field.name().starts_with(OR_HISTOGRAM_FIELD_PREFIX))
            .unwrap()
            .name()
            .clone();

        let (_, batches) = execute(plan, &state).await;
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 3);
        let mut float_values = values(&batches, &float_field);
        float_values.sort_by(f64::total_cmp);
        assert_eq!(float_values, vec![2.0, 7.0]);
        assert_eq!(histograms(&batches, &histogram_field).len(), 1);
    }

    #[tokio::test]
    async fn test_mixed_fields_align_with_single_float_vector() {
        let (planner, mixed) = mixed_direct_or(false).await;
        let scale = tagged_source(
            "scale",
            false,
            ("k", Some("float")),
            DirectOrValue::Float64(2.0),
        );
        let scale = scan(&scale);
        let scale_fields = vec!["v".to_string()];
        let PromExpr::Binary(binary) = parser::parse("lhs * rhs").unwrap() else {
            unreachable!()
        };

        let (groups, invalid_pairs) = PromPlanner::align_binary_field_columns(
            mixed.schema(),
            scale.schema(),
            &planner.ctx.field_columns,
            &scale_fields,
            binary.op,
            false,
            false,
        );
        assert!(invalid_pairs.is_empty());
        assert_eq!(
            groups
                .iter()
                .map(|(output, _)| output.clone())
                .collect::<Vec<_>>(),
            planner.ctx.field_columns
        );
        assert_eq!(groups.len(), 2);
        assert!(
            groups
                .iter()
                .flat_map(|(_, pairs)| pairs)
                .all(|(_, right)| *right == &scale_fields[0])
        );

        let (groups, invalid_pairs) = PromPlanner::align_binary_field_columns(
            scale.schema(),
            mixed.schema(),
            &scale_fields,
            &planner.ctx.field_columns,
            binary.op,
            false,
            false,
        );
        assert!(invalid_pairs.is_empty());
        assert_eq!(
            groups
                .iter()
                .map(|(output, _)| output.clone())
                .collect::<Vec<_>>(),
            planner.ctx.field_columns
        );
        assert_eq!(groups.len(), 2);
        assert!(
            groups
                .iter()
                .flat_map(|(_, pairs)| pairs)
                .all(|(left, _)| *left == &scale_fields[0])
        );
    }

    #[tokio::test]
    async fn test_non_bool_comparison_filters_mixed_sample_lanes() {
        let (planner, input) = mixed_direct_or(false).await;
        let input_schema = input.schema().clone();
        let plan = planner
            .filter_on_field_column(input, |field| {
                if PromPlanner::field_column_is_native_histogram(&input_schema, field) {
                    Ok(lit(false))
                } else {
                    Ok(col(field).gt(lit(0.0)))
                }
            })
            .unwrap();
        let float_field = planner.ctx.field_columns[0].clone();

        let (_, batches) = execute(plan, &build_query_engine_state()).await;
        assert_eq!(values(&batches, &float_field), vec![1.25]);
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
    }

    #[tokio::test]
    async fn test_mixed_left_and_unless_preserve_sample_lanes() {
        for (expression, expected_sample_kind) in [
            ("lhs and on(k) mask", (false, true)),
            ("lhs unless on(k) mask", (true, false)),
        ] {
            let (mut planner, left) = mixed_direct_or(false).await;
            let left_context = planner.ctx.clone();
            let float_field = left_context.field_columns[0].clone();
            let histogram_field = left_context.field_columns[1].clone();
            let mask = tagged_source(
                "mask",
                false,
                ("k", Some("histogram")),
                DirectOrValue::Float64(1.0),
            );
            let PromExpr::Binary(binary) = parser::parse(expression).unwrap() else {
                unreachable!()
            };
            let plan = planner
                .set_op_on_non_field_columns(
                    left,
                    scan(&mask),
                    left_context,
                    direct_or_context("mask", &["job", "k"], "v"),
                    binary.op,
                    &binary.modifier,
                )
                .unwrap();

            let (_, batches) = execute(plan, &build_query_engine_state()).await;
            let sample_kinds = batches
                .iter()
                .flat_map(|batch| {
                    let floats = batch.column_by_name(&float_field).unwrap();
                    let histograms = batch.column_by_name(&histogram_field).unwrap();
                    (0..batch.num_rows())
                        .map(|row| (floats.is_valid(row), histograms.is_valid(row)))
                })
                .collect::<Vec<_>>();
            assert_eq!(sample_kinds, vec![expected_sample_kind], "{expression}");
        }
    }

    #[tokio::test]
    async fn test_mixed_fields_arithmetic_broadcasts_computed_scalar() {
        let plan = PromPlanner::stmt_to_plan(
            build_test_mixed_native_histogram_table_provider("some_metric").await,
            &build_eval_stmt("some_metric * scalar(vector(2))"),
            &build_query_engine_state(),
        )
        .await
        .unwrap();
        let schema = plan.schema();
        assert_eq!(
            schema
                .field_with_unqualified_name(greptime_value())
                .unwrap()
                .data_type(),
            &ArrowDataType::Float64
        );
        assert_eq!(
            schema
                .field_with_unqualified_name(greptime_native_histogram())
                .unwrap()
                .data_type(),
            &native_histogram_value_type().as_arrow_type()
        );
        assert!(
            plan.display_indent_schema()
                .to_string()
                .contains("prom_native_histogram_mul_scalar"),
            "{plan:?}"
        );
    }

    #[tokio::test]
    async fn test_unsupported_histogram_binary_does_not_block_or_fallback() {
        let state = build_query_engine_state();
        let plan = PromPlanner::stmt_to_plan(
            operator_table_provider(),
            &operator_eval_stmt("((lf or on(tag) lh) % 2) or on(tag) lh"),
            &state,
        )
        .await
        .unwrap();
        let float_field = plan
            .schema()
            .fields()
            .iter()
            .find(|field| field.data_type() == &ArrowDataType::Float64)
            .unwrap()
            .name()
            .clone();
        let histogram_field = plan
            .schema()
            .fields()
            .iter()
            .find(|field| field.data_type() == &native_histogram_value_type().as_arrow_type())
            .unwrap()
            .name()
            .clone();

        let (_, batches) = execute(plan, &state).await;
        assert_eq!(values(&batches, &float_field), vec![0.0]);
        assert_eq!(histograms(&batches, &histogram_field).len(), 1);
    }

    #[tokio::test]
    async fn test_unary_negates_mixed_float_and_native_histogram_samples() {
        for histogram_on_left in [false, true] {
            let (mut planner, input) = mixed_direct_or(histogram_on_left).await;
            let plan = planner.negate_field_columns(input).unwrap();
            assert!(PromPlanner::field_columns_are_alternative_samples(
                plan.schema(),
                &planner.ctx.field_columns
            ));
            let float_field = planner
                .ctx
                .field_columns
                .iter()
                .find(|field| field.starts_with(OR_FLOAT_FIELD_PREFIX))
                .unwrap();
            let histogram_field = planner
                .ctx
                .field_columns
                .iter()
                .find(|field| field.starts_with(OR_HISTOGRAM_FIELD_PREFIX))
                .unwrap();

            let (_, batches) = execute(plan, &build_query_engine_state()).await;
            assert_eq!(values(&batches, float_field), vec![-1.25]);
            let histogram = batches
                .iter()
                .find_map(|batch| {
                    let values = batch
                        .column_by_name(histogram_field)
                        .unwrap()
                        .as_any()
                        .downcast_ref::<datafusion::arrow::array::StructArray>()
                        .unwrap();
                    (0..values.len()).find_map(|row| {
                        common_query::native_histogram::read_histogram(values, row).unwrap()
                    })
                })
                .unwrap();
            assert_eq!(histogram.count, -1.0);
            assert_eq!(histogram.sum, -1.0);
            assert_eq!(histogram.reset_hint, CounterResetHint::Gauge);
        }
    }

    #[tokio::test]
    async fn test_native_histogram_sum_and_avg_execute_real_batches() {
        for op_name in ["sum", "avg"] {
            for incompatible in [false, true] {
                let mut second = direct_or_histogram();
                if incompatible {
                    second.schema = CUSTOM_BUCKETS_SCHEMA;
                    second.custom_values = vec![1.0];
                }
                let collector = PromqlAnnotationCollector::default();
                let (mut planner, input) =
                    mixed_aggregate_input(vec![direct_or_histogram(), second]).await;
                planner.promql_annotations = Some(collector.clone());
                let histogram_column = planner.ctx.field_columns[1].clone();
                planner.ctx.field_columns = vec![histogram_column.clone()];
                let input = LogicalPlanBuilder::from(input)
                    .project([col("ts"), col(&histogram_column)])
                    .unwrap()
                    .build()
                    .unwrap();
                let PromExpr::Aggregate(AggregateExpr { op, param, .. }) =
                    parser::parse(&format!("{op_name}(mixed)")).unwrap()
                else {
                    unreachable!()
                };
                let (aggregate_exprs, _) =
                    planner.create_aggregate_exprs(op, &param, &input).unwrap();
                let plan = LogicalPlanBuilder::from(input)
                    .aggregate(vec![col("ts")], aggregate_exprs)
                    .unwrap()
                    .filter(planner.create_empty_values_filter_expr(false).unwrap())
                    .unwrap()
                    .build()
                    .unwrap();

                let (_, batches) = execute(plan, &build_query_engine_state()).await;
                let mut warnings = vec![];
                let mut infos = vec![];
                collector.append_to(&mut warnings, &mut infos);
                assert!(infos.is_empty());
                if incompatible {
                    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 0);
                    assert!(warnings.iter().any(|warning| {
                        warning
                            == &format!(
                                "prom_native_histogram_agg_{op_name}: dropped native histogram aggregate with incompatible schemas"
                            )
                    }));
                } else {
                    let histograms = histograms(&batches, &histogram_column);
                    assert_eq!(histograms.len(), 1);
                    let expected = if op_name == "sum" { 2.0 } else { 1.0 };
                    assert_eq!(histograms[0].count, expected);
                    assert_eq!(histograms[0].sum, expected);
                    assert!(warnings.is_empty());
                }
            }
        }
    }

    #[tokio::test]
    async fn test_canonical_mixed_count_group_and_count_values_execute() {
        let state = build_query_engine_state();
        for (query, expected) in [
            ("count(some_metric)", vec![2.0]),
            ("group(some_metric)", vec![1.0]),
            (r#"count_values("sample", some_metric)"#, vec![1.0, 1.0]),
        ] {
            let plan = PromPlanner::stmt_to_plan(
                build_test_mixed_native_histogram_table_provider("some_metric").await,
                &operator_eval_stmt(query),
                &state,
            )
            .await
            .unwrap();
            assert!(
                plan.schema()
                    .fields()
                    .iter()
                    .all(|field| !field.name().starts_with("__promql_sample_count")),
                "{query}: {plan:?}"
            );
            let value_fields = plan
                .schema()
                .fields()
                .iter()
                .filter(|field| {
                    matches!(
                        field.data_type(),
                        ArrowDataType::Float64 | ArrowDataType::Int64 | ArrowDataType::UInt64
                    ) || field.data_type() == &native_histogram_value_type().as_arrow_type()
                })
                .collect::<Vec<_>>();
            assert_eq!(value_fields.len(), 1, "{query}: {plan:?}");
            assert_ne!(
                value_fields[0].data_type(),
                &native_histogram_value_type().as_arrow_type(),
                "{query}: {plan:?}"
            );
            let value_column = value_fields[0].name().clone();

            let (_, batches) = execute(plan, &state).await;
            let mut actual = numeric_values(&batches, &value_column);
            actual.sort_by(f64::total_cmp);
            assert_eq!(actual, expected, "{query}");

            if query.starts_with("count_values") {
                let mut sample_labels = batches
                    .iter()
                    .flat_map(|batch| {
                        batch
                            .column_by_name("sample")
                            .unwrap()
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .unwrap()
                            .iter()
                            .flatten()
                            .map(str::to_string)
                    })
                    .collect::<Vec<_>>();
                sample_labels.sort();
                let mut expected_labels =
                    vec!["2".to_string(), direct_or_histogram().promql_string()];
                expected_labels.sort();
                assert_eq!(sample_labels, expected_labels);
            }
        }
    }

    #[tokio::test]
    async fn test_mixed_or_sum_aggregates_each_sample_type() {
        let PromExpr::Aggregate(AggregateExpr { op, param, .. }) =
            parser::parse("sum(lhs)").unwrap()
        else {
            unreachable!()
        };

        let collector = PromqlAnnotationCollector::default();
        let (mut planner, input) = mixed_direct_or(false).await;
        planner.promql_annotations = Some(collector.clone());
        let float_column = planner.ctx.field_columns[0].clone();
        let histogram_column = planner.ctx.field_columns[1].clone();
        let (aggregate_exprs, _) = planner.create_aggregate_exprs(op, &param, &input).unwrap();
        let plan = LogicalPlanBuilder::from(input)
            .aggregate(vec![col("ts"), col("k")], aggregate_exprs)
            .unwrap()
            .filter(
                planner
                    .mixed_aggregate_filter_expr(op, &float_column, &histogram_column)
                    .unwrap(),
            )
            .unwrap()
            .project([
                col(&float_column),
                col(&histogram_column),
                col("ts"),
                col("k"),
            ])
            .unwrap()
            .build()
            .unwrap();

        let (_, batches) = execute(plan, &build_query_engine_state()).await;
        assert_eq!(values(&batches, &float_column), vec![1.25]);
        let histogram = batches
            .iter()
            .find_map(|batch| {
                let values = batch
                    .column_by_name(&histogram_column)?
                    .as_any()
                    .downcast_ref::<datafusion::arrow::array::StructArray>()?;
                (0..values.len()).find_map(|row| {
                    common_query::native_histogram::read_histogram(values, row).unwrap()
                })
            })
            .unwrap();
        assert_eq!(histogram.count, 1.0);
        let mut warnings = vec![];
        let mut infos = vec![];
        collector.append_to(&mut warnings, &mut infos);
        assert!(warnings.is_empty());

        let collector = PromqlAnnotationCollector::default();
        let (mut planner, input) = mixed_direct_or(false).await;
        planner.promql_annotations = Some(collector.clone());
        let float_column = planner.ctx.field_columns[0].clone();
        let histogram_column = planner.ctx.field_columns[1].clone();
        let (aggregate_exprs, _) = planner.create_aggregate_exprs(op, &param, &input).unwrap();
        let plan = LogicalPlanBuilder::from(input)
            .aggregate(vec![col("ts")], aggregate_exprs)
            .unwrap()
            .filter(
                planner
                    .mixed_aggregate_filter_expr(op, &float_column, &histogram_column)
                    .unwrap(),
            )
            .unwrap()
            .project([col(&float_column), col(&histogram_column), col("ts")])
            .unwrap()
            .build()
            .unwrap();

        let (_, batches) = execute(plan, &build_query_engine_state()).await;
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 0);
        let mut warnings = vec![];
        let mut infos = vec![];
        collector.append_to(&mut warnings, &mut infos);
        assert_eq!(
            warnings,
            vec![
                "sum: dropped aggregation result containing both float and native histogram samples"
            ]
        );
    }

    #[tokio::test]
    async fn test_mixed_or_sum_drops_incompatible_mixed_group() {
        let PromExpr::Aggregate(AggregateExpr { op, param, .. }) =
            parser::parse("sum(lhs)").unwrap()
        else {
            unreachable!()
        };
        let mut custom = direct_or_histogram();
        custom.schema = CUSTOM_BUCKETS_SCHEMA;
        custom.custom_values = vec![1.0];
        let collector = PromqlAnnotationCollector::default();
        let (mut planner, input) = mixed_aggregate_input(vec![direct_or_histogram(), custom]).await;
        planner.promql_annotations = Some(collector.clone());
        let float_column = planner.ctx.field_columns[0].clone();
        let histogram_column = planner.ctx.field_columns[1].clone();
        let (aggregate_exprs, _) = planner.create_aggregate_exprs(op, &param, &input).unwrap();
        let plan = LogicalPlanBuilder::from(input)
            .aggregate(vec![col("ts")], aggregate_exprs)
            .unwrap()
            .filter(
                planner
                    .mixed_aggregate_filter_expr(op, &float_column, &histogram_column)
                    .unwrap(),
            )
            .unwrap()
            .project([col(&float_column), col(&histogram_column), col("ts")])
            .unwrap()
            .build()
            .unwrap();

        let (_, batches) = execute(plan, &build_query_engine_state()).await;
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 0);
        let mut warnings = vec![];
        let mut infos = vec![];
        collector.append_to(&mut warnings, &mut infos);
        assert!(warnings.iter().any(|warning| {
            warning
                == "sum: dropped aggregation result containing both float and native histogram samples"
        }));
    }

    #[tokio::test]
    async fn test_mixed_or_min_records_only_present_histograms() {
        let PromExpr::Aggregate(AggregateExpr { op, param, .. }) =
            parser::parse("min(lhs)").unwrap()
        else {
            unreachable!()
        };
        let expected_info = "min: dropped native histogram samples because this aggregation is not supported for native histograms";

        for (histograms, expected_infos) in [
            (vec![], vec![]),
            (vec![direct_or_histogram()], vec![expected_info]),
        ] {
            let collector = PromqlAnnotationCollector::default();
            let (mut planner, input) = mixed_aggregate_input(histograms).await;
            planner.promql_annotations = Some(collector.clone());
            let float_column = planner.ctx.field_columns[0].clone();
            let histogram_column = planner.ctx.field_columns[1].clone();
            let (aggregate_exprs, _) = planner.create_aggregate_exprs(op, &param, &input).unwrap();
            let plan = LogicalPlanBuilder::from(input)
                .aggregate(vec![col("ts")], aggregate_exprs)
                .unwrap()
                .filter(
                    planner
                        .mixed_ignored_histogram_filter_expr(op, &histogram_column)
                        .unwrap(),
                )
                .unwrap()
                .project([col(&float_column), col("ts")])
                .unwrap()
                .build()
                .unwrap();

            let (_, batches) = execute(plan, &build_query_engine_state()).await;
            assert_eq!(values(&batches, &float_column), vec![1.25]);
            let mut warnings = vec![];
            let mut infos = vec![];
            collector.append_to(&mut warnings, &mut infos);
            assert!(warnings.is_empty());
            assert_eq!(infos, expected_infos);
        }
    }

    #[tokio::test]
    async fn test_mixed_or_value_aliases_do_not_replace_labels() {
        let left = source(
            "lhs",
            false,
            1,
            vec![("job", Some("job")), ("k", Some("float"))],
            DirectOrValue::Float64(1.0),
        );
        let right = source(
            "rhs",
            false,
            1,
            vec![
                ("job", Some("job")),
                ("k", Some("histogram")),
                (greptime_value(), Some("value-label")),
            ],
            DirectOrValue::NativeHistogram(direct_or_histogram()),
        );
        let table_provider = build_test_table_provider_with_fields(
            &[(DEFAULT_SCHEMA_NAME.to_string(), "dummy".to_string())],
            &[],
        )
        .await;
        let mut planner = PromPlanner {
            table_provider,
            ctx: PromPlannerContext::default(),
            promql_annotations: None,
        };
        let left = LogicalPlanBuilder::from(scan(&left))
            .project(vec![
                col("ts"),
                col("job"),
                col("k"),
                col("v").alias(greptime_value()),
            ])
            .unwrap()
            .build()
            .unwrap();
        let left_context = direct_or_context("lhs", &["job", "k"], greptime_value());
        let right_context = direct_or_context("rhs", &["job", "k", greptime_value()], "v");
        let plan = planner
            .or_operator(
                left,
                scan(&right),
                left_context.tag_columns.iter().cloned().collect(),
                right_context.tag_columns.iter().cloned().collect(),
                left_context,
                right_context,
                &or_modifier("lhs or on(k) rhs"),
            )
            .unwrap();

        assert_eq!(
            plan.schema()
                .field_with_name(None, greptime_value())
                .unwrap()
                .data_type(),
            &ArrowDataType::Utf8
        );
        assert!(
            planner
                .ctx
                .field_columns
                .iter()
                .all(|field| { field != greptime_value() && field != greptime_native_histogram() })
        );
        assert!(PromPlanner::field_columns_are_alternative_samples(
            plan.schema(),
            &planner.ctx.field_columns
        ));
        let (_, batches) = execute(plan, &build_query_engine_state()).await;
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
        let labels = batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column_by_name(greptime_value())
                    .unwrap()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .iter()
                    .flatten()
            })
            .collect::<Vec<_>>();
        assert_eq!(labels, vec!["value-label"]);
    }

    #[tokio::test]
    async fn test_mixed_or_routes_float_histogram_and_label_functions() {
        for (function, expected) in [("abs", 1.25), ("round", 1.0), ("histogram_count", 1.0)] {
            let (mut planner, input) = mixed_direct_or(false).await;
            let preserve_any_value = PromPlanner::field_columns_are_alternative_samples(
                input.schema(),
                &planner.ctx.field_columns,
            );
            let PromExpr::Call(call) = parser::parse(&format!("{function}(lhs)")).unwrap() else {
                unreachable!()
            };
            let state = build_query_engine_state();
            let (mut exprs, _) = planner
                .create_function_expr(&call.func, vec![], input.schema(), &state)
                .unwrap();
            exprs.insert(0, planner.create_time_index_column_expr().unwrap());
            exprs.extend(planner.create_tag_column_exprs().unwrap());
            let plan = LogicalPlanBuilder::from(input)
                .project(exprs)
                .unwrap()
                .filter(
                    planner
                        .create_empty_values_filter_expr(preserve_any_value)
                        .unwrap(),
                )
                .unwrap()
                .build()
                .unwrap();
            let (_, batches) = execute(plan, &state).await;
            let values = batches
                .iter()
                .flat_map(|batch| {
                    batch
                        .schema()
                        .fields()
                        .iter()
                        .position(|field| field.data_type() == &ArrowDataType::Float64)
                        .map(|index| {
                            batch
                                .column(index)
                                .as_any()
                                .downcast_ref::<Float64Array>()
                                .unwrap()
                                .iter()
                                .flatten()
                        })
                        .into_iter()
                        .flatten()
                })
                .collect::<Vec<_>>();
            assert_eq!(values, vec![expected], "{function}");
        }

        let (mut planner, input) = mixed_direct_or(false).await;
        let preserve_any_value = PromPlanner::field_columns_are_alternative_samples(
            input.schema(),
            &planner.ctx.field_columns,
        );
        let PromExpr::Call(call) =
            parser::parse(r#"label_replace(lhs, "copy", "$1", "k", "(.*)")"#).unwrap()
        else {
            unreachable!()
        };
        let args = planner.create_function_args(&call.args.args).unwrap();
        let state = build_query_engine_state();
        let (mut exprs, _) = planner
            .create_function_expr(&call.func, args.literals, input.schema(), &state)
            .unwrap();
        exprs.insert(0, planner.create_time_index_column_expr().unwrap());
        exprs.extend(planner.create_tag_column_exprs().unwrap());
        let plan = LogicalPlanBuilder::from(input)
            .project(exprs)
            .unwrap()
            .filter(
                planner
                    .create_empty_values_filter_expr(preserve_any_value)
                    .unwrap(),
            )
            .unwrap()
            .build()
            .unwrap();
        let (_, batches) = execute(plan, &state).await;
        let sample_count = batches.iter().map(RecordBatch::num_rows).sum::<usize>();
        assert_eq!(sample_count, 2);
    }
}
