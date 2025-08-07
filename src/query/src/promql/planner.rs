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

use std::collections::{BTreeSet, HashSet, VecDeque};
use std::sync::Arc;
use std::time::UNIX_EPOCH;

use arrow::datatypes::IntervalDayTime;
use async_recursion::async_recursion;
use catalog::table_source::DfTableSourceProvider;
use common_error::ext::ErrorExt;
use common_error::status_code::StatusCode;
use common_function::function::FunctionContext;
use common_query::prelude::GREPTIME_VALUE;
use datafusion::common::DFSchemaRef;
use datafusion::datasource::DefaultTableSource;
use datafusion::functions_aggregate::average::avg_udaf;
use datafusion::functions_aggregate::count::count_udaf;
use datafusion::functions_aggregate::expr_fn::first_value;
use datafusion::functions_aggregate::grouping::grouping_udaf;
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
use datafusion::prelude as df_prelude;
use datafusion::prelude::{Column, Expr as DfExpr, JoinType};
use datafusion::scalar::ScalarValue;
use datafusion::sql::TableReference;
use datafusion_common::{DFSchema, NullEquality};
use datafusion_expr::expr::WindowFunctionParams;
use datafusion_expr::utils::conjunction;
use datafusion_expr::{col, lit, ExprSchemable, Literal, SortExpr};
use datatypes::arrow::datatypes::{DataType as ArrowDataType, TimeUnit as ArrowTimeUnit};
use datatypes::data_type::ConcreteDataType;
use itertools::Itertools;
use promql::extension_plan::{
    build_special_time_expr, Absent, EmptyMetric, HistogramFold, InstantManipulate, Millisecond,
    RangeManipulate, ScalarCalculate, SeriesDivide, SeriesNormalize, UnionDistinctOn,
};
use promql::functions::{
    quantile_udaf, AbsentOverTime, AvgOverTime, Changes, CountOverTime, Delta, Deriv, HoltWinters,
    IDelta, Increase, LastOverTime, MaxOverTime, MinOverTime, PredictLinear, PresentOverTime,
    QuantileOverTime, Rate, Resets, Round, StddevOverTime, StdvarOverTime, SumOverTime,
};
use promql_parser::label::{MatchOp, Matcher, Matchers, METRIC_NAME};
use promql_parser::parser::token::TokenType;
use promql_parser::parser::{
    token, AggregateExpr, BinModifier, BinaryExpr as PromBinaryExpr, Call, EvalStmt,
    Expr as PromExpr, Function, FunctionArgs as PromFunctionArgs, LabelModifier, MatrixSelector,
    NumberLiteral, Offset, ParenExpr, StringLiteral, SubqueryExpr, UnaryExpr,
    VectorMatchCardinality, VectorSelector,
};
use snafu::{ensure, OptionExt, ResultExt};
use store_api::metric_engine_consts::{
    DATA_SCHEMA_TABLE_ID_COLUMN_NAME, DATA_SCHEMA_TSID_COLUMN_NAME,
};
use table::table::adapter::DfTableProviderAdapter;

use crate::promql::error::{
    CatalogSnafu, ColumnNotFoundSnafu, CombineTableColumnMismatchSnafu, DataFusionPlanningSnafu,
    ExpectRangeSelectorSnafu, FunctionInvalidArgumentSnafu, InvalidTimeRangeSnafu,
    MultiFieldsNotSupportedSnafu, MultipleMetricMatchersSnafu, MultipleVectorSnafu,
    NoMetricMatcherSnafu, PromqlPlanNodeSnafu, Result, SameLabelSetSnafu, TableNameNotFoundSnafu,
    TimeIndexNotFoundSnafu, UnexpectedPlanExprSnafu, UnexpectedTokenSnafu, UnknownTableSnafu,
    UnsupportedExprSnafu, UnsupportedMatcherOpSnafu, UnsupportedVectorMatchSnafu,
    ValueNotFoundSnafu, ZeroRangeSelectorSnafu,
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
/// `vector` function in PromQL
const SPECIAL_VECTOR_FUNCTION: &str = "vector";
/// `le` column for conventional histogram.
const LE_COLUMN_NAME: &str = "le";

const DEFAULT_TIME_INDEX_COLUMN: &str = "time";

/// default value column name for empty metric
const DEFAULT_FIELD_COLUMN: &str = "value";

/// Special modifier to project field columns under multi-field mode
const FIELD_COLUMN_MATCHER: &str = "__field__";

/// Special modifier for cross schema query
const SCHEMA_COLUMN_MATCHER: &str = "__schema__";
const DB_COLUMN_MATCHER: &str = "__database__";

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
    /// The matcher for field columns `__field__`.
    field_column_matcher: Option<Vec<Matcher>>,
    /// The matcher for selectors (normal matchers).
    selector_matcher: Vec<Matcher>,
    schema_name: Option<String>,
    /// The range in millisecond of range selector. None if there is no range selector.
    range: Option<Millisecond>,
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
        self.field_column_matcher = None;
        self.selector_matcher.clear();
        self.schema_name = None;
        self.range = None;
    }

    /// Reset table name and schema to empty
    fn reset_table_name_and_schema(&mut self) {
        self.table_name = Some(String::new());
        self.schema_name = None;
    }

    /// Check if `le` is present in tag columns
    fn has_le_tag(&self) -> bool {
        self.tag_columns.iter().any(|c| c.eq(&LE_COLUMN_NAME))
    }
}

pub struct PromPlanner {
    table_provider: DfTableSourceProvider,
    ctx: PromPlannerContext,
}

/// Unescapes the value of the matcher
pub fn normalize_matcher(mut matcher: Matcher) -> Matcher {
    if let Ok(unescaped_value) = unescaper::unescape(&matcher.value) {
        matcher.value = unescaped_value;
    }
    matcher
}

impl PromPlanner {
    pub async fn stmt_to_plan(
        table_provider: DfTableSourceProvider,
        stmt: &EvalStmt,
        query_engine_state: &QueryEngineState,
    ) -> Result<LogicalPlan> {
        let mut planner = Self {
            table_provider,
            ctx: PromPlannerContext::from_eval_stmt(stmt),
        };

        planner
            .prom_expr_to_plan(&stmt.expr, query_engine_state)
            .await
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

        let manipulate = RangeManipulate::new(
            self.ctx.start,
            self.ctx.end,
            self.ctx.interval,
            range_ms,
            self.ctx
                .time_index_column
                .clone()
                .expect("time index should be set in `setup_context`"),
            self.ctx.field_columns.clone(),
            input,
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

        let input = self.prom_expr_to_plan(expr, query_engine_state).await?;

        match (*op).id() {
            token::T_TOPK | token::T_BOTTOMK => {
                self.prom_topk_bottomk_to_plan(aggr_expr, input).await
            }
            _ => {
                // calculate columns to group by
                // Need to append time index column into group by columns
                let mut group_exprs = self.agg_modifier_to_col(input.schema(), modifier, true)?;
                // convert op and value columns to aggregate exprs
                let (aggr_exprs, prev_field_exprs) =
                    self.create_aggregate_exprs(*op, param, &input)?;

                // create plan
                let builder = LogicalPlanBuilder::from(input);
                let builder = if op.id() == token::T_COUNT_VALUES {
                    let label = Self::get_param_value_as_str(*op, param)?;
                    // `count_values` must be grouped by fields,
                    // and project the fields to the new label.
                    group_exprs.extend(prev_field_exprs.clone());
                    let project_fields = self
                        .create_field_column_exprs()?
                        .into_iter()
                        .chain(self.create_tag_column_exprs()?)
                        .chain(Some(self.create_time_index_column_expr()?))
                        .chain(prev_field_exprs.into_iter().map(|expr| expr.alias(label)));

                    builder
                        .aggregate(group_exprs.clone(), aggr_exprs)
                        .context(DataFusionPlanningSnafu)?
                        .project(project_fields)
                        .context(DataFusionPlanningSnafu)?
                } else {
                    builder
                        .aggregate(group_exprs.clone(), aggr_exprs)
                        .context(DataFusionPlanningSnafu)?
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

        let group_exprs = self.agg_modifier_to_col(input.schema(), modifier, false)?;

        let val = Self::get_param_as_literal_expr(param, Some(*op), Some(ArrowDataType::Float64))?;

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
        self.projection_for_each_field_column(input, |col| {
            Ok(DfExpr::Negative(Box::new(DfExpr::Column(col.into()))))
        })
    }

    async fn prom_binary_expr_to_plan(
        &mut self,
        query_engine_state: &QueryEngineState,
        binary_expr: &PromBinaryExpr,
    ) -> Result<LogicalPlan> {
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
                if let Some(time_expr) = Self::try_build_special_time_expr(
                    lhs,
                    self.ctx.time_index_column.as_ref().unwrap(),
                ) {
                    expr = time_expr
                }
                let bin_expr_builder = |col: &String| {
                    let binary_expr_builder = Self::prom_token_to_binary_expr_builder(*op)?;
                    let mut binary_expr =
                        binary_expr_builder(expr.clone(), DfExpr::Column(col.into()))?;

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
                    self.projection_for_each_field_column(input, bin_expr_builder)
                }
            }
            // lhs is a column, rhs is a literal
            (None, Some(mut expr)) => {
                let input = self.prom_expr_to_plan(lhs, query_engine_state).await?;
                // check if the literal is a special time expr
                if let Some(time_expr) = Self::try_build_special_time_expr(
                    rhs,
                    self.ctx.time_index_column.as_ref().unwrap(),
                ) {
                    expr = time_expr
                }
                let bin_expr_builder = |col: &String| {
                    let binary_expr_builder = Self::prom_token_to_binary_expr_builder(*op)?;
                    let mut binary_expr =
                        binary_expr_builder(DfExpr::Column(col.into()), expr.clone())?;

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
                    self.projection_for_each_field_column(input, bin_expr_builder)
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
                }
                let mut field_columns = left_field_columns.iter().zip(right_field_columns.iter());

                let join_plan = self.join_on_non_field_columns(
                    left_input,
                    right_input,
                    left_table_ref.clone(),
                    right_table_ref.clone(),
                    left_time_index_column,
                    right_time_index_column,
                    // if left plan or right plan tag is empty, means case like `scalar(...) + host` or `host + scalar(...)`
                    // under this case we only join on time index
                    left_context.tag_columns.is_empty() || right_context.tag_columns.is_empty(),
                    modifier,
                )?;
                let join_plan_schema = join_plan.schema().clone();

                let bin_expr_builder = |_: &String| {
                    let (left_col_name, right_col_name) = field_columns.next().unwrap();
                    let left_col = join_plan_schema
                        .qualified_field_with_name(Some(&left_table_ref), left_col_name)
                        .context(DataFusionPlanningSnafu)?
                        .into();
                    let right_col = join_plan_schema
                        .qualified_field_with_name(Some(&right_table_ref), right_col_name)
                        .context(DataFusionPlanningSnafu)?
                        .into();

                    let binary_expr_builder = Self::prom_token_to_binary_expr_builder(*op)?;
                    let mut binary_expr =
                        binary_expr_builder(DfExpr::Column(left_col), DfExpr::Column(right_col))?;
                    if is_comparison_op && should_return_bool {
                        binary_expr = DfExpr::Cast(Cast {
                            expr: Box::new(binary_expr),
                            data_type: ArrowDataType::Float64,
                        });
                    }
                    Ok(binary_expr)
                };
                if is_comparison_op && !should_return_bool {
                    self.filter_on_field_column(join_plan, bin_expr_builder)
                } else {
                    self.projection_for_each_field_column(join_plan, bin_expr_builder)
                }
            }
        }
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
        let literal_expr = df_prelude::lit(val.to_string());

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

        let normalize = if timestamp_fn {
            // If evaluating the PromQL `timestamp()` function, project the time index column as the value column
            // before wrapping with [`InstantManipulate`], so the output matches PromQL's `timestamp()` semantics.
            self.create_timestamp_func_plan(normalize)?
        } else {
            normalize
        };

        let manipulate = InstantManipulate::new(
            self.ctx.start,
            self.ctx.end,
            self.ctx.lookback_delta,
            self.ctx.interval,
            self.ctx
                .time_index_column
                .clone()
                .expect("time index should be set in `setup_context`"),
            self.ctx.field_columns.first().cloned(),
            normalize,
        );
        Ok(LogicalPlan::Extension(Extension {
            node: Arc::new(manipulate),
        }))
    }

    /// Builds a projection plan for the PromQL `timestamp()` function.
    /// Projects the time index column as the value column for each row.
    ///
    /// # Arguments
    /// * `normalize` - Input [`LogicalPlan`] for the normalized series.
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
    fn create_timestamp_func_plan(&mut self, normalize: LogicalPlan) -> Result<LogicalPlan> {
        let time_expr = build_special_time_expr(self.ctx.time_index_column.as_ref().unwrap())
            .alias(DEFAULT_FIELD_COLUMN);
        self.ctx.field_columns = vec![time_expr.schema_name().to_string()];
        let mut project_exprs = Vec::with_capacity(self.ctx.tag_columns.len() + 2);
        project_exprs.push(self.create_time_index_column_expr()?);
        project_exprs.push(time_expr);
        project_exprs.extend(self.create_tag_column_exprs()?);

        LogicalPlanBuilder::from(normalize)
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
            SPECIAL_HISTOGRAM_QUANTILE => {
                return self.create_histogram_plan(args, query_engine_state).await
            }
            SPECIAL_VECTOR_FUNCTION => return self.create_vector_plan(args).await,
            SCALAR_FUNCTION => return self.create_scalar_plan(args, query_engine_state).await,
            SPECIAL_ABSENT_FUNCTION => {
                return self.create_absent_plan(args, query_engine_state).await
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
        let (mut func_exprs, new_tags) =
            self.create_function_expr(func, args.literals.clone(), query_engine_state)?;
        func_exprs.insert(0, self.create_time_index_column_expr()?);
        func_exprs.extend_from_slice(&self.create_tag_column_exprs()?);

        let builder = LogicalPlanBuilder::from(input)
            .project(func_exprs)
            .context(DataFusionPlanningSnafu)?
            .filter(self.create_empty_values_filter_expr()?)
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
            "ANALYZE" => LogicalPlanBuilder::from(plan)
                .explain(false, true)
                .unwrap()
                .build()
                .context(DataFusionPlanningSnafu),
            "ANALYZE VERBOSE" => LogicalPlanBuilder::from(plan)
                .explain(true, true)
                .unwrap()
                .build()
                .context(DataFusionPlanningSnafu),
            "EXPLAIN" => LogicalPlanBuilder::from(plan)
                .explain(false, false)
                .unwrap()
                .build()
                .context(DataFusionPlanningSnafu),
            "EXPLAIN VERBOSE" => LogicalPlanBuilder::from(plan)
                .explain(true, false)
                .unwrap()
                .build()
                .context(DataFusionPlanningSnafu),
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
                        matcher: matcher.name.to_string(),
                        matcher_op: matcher.op.to_string(),
                    }
                );
                self.ctx.schema_name = Some(matcher.value.clone());
            } else if matcher.name != METRIC_NAME {
                self.ctx.selector_matcher.push(matcher.clone());
                let _ = matchers.insert(matcher.clone());
            }
        }

        Ok(Matchers::new(
            matchers.into_iter().map(normalize_matcher).collect(),
        ))
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
        let sort_plan = LogicalPlanBuilder::from(table_scan)
            .sort(self.create_tag_and_time_index_column_sort_exprs()?)
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
                self.ctx.tag_columns.clone(),
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
            self.ctx.tag_columns.clone(),
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
                    // nonexistence label will be ignored
                    if let Ok(field) = input_schema.field_with_unqualified_name(label) {
                        exprs.push(DfExpr::Column(Column::from(field.name())));

                        if update_ctx {
                            // update the tag columns in context
                            self.ctx.tag_columns.push(label.clone());
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

            let col = if table_schema
                .field_with_unqualified_name(&matcher.name)
                .is_err()
            {
                DfExpr::Literal(ScalarValue::Utf8(Some(String::new())), None).alias(matcher.name)
            } else {
                DfExpr::Column(Column::from_name(matcher.name))
            };
            let lit = DfExpr::Literal(ScalarValue::Utf8(Some(matcher.value)), None);
            let expr = match matcher.op {
                MatchOp::Equal => col.eq(lit),
                MatchOp::NotEqual => col.not_eq(lit),
                MatchOp::Re(re) => {
                    // TODO(ruihang): a more programmatic way to handle this in datafusion
                    if re.as_str() == ".*" {
                        continue;
                    }
                    DfExpr::BinaryExpr(BinaryExpr {
                        left: Box::new(col),
                        op: Operator::RegexMatch,
                        right: Box::new(re.as_str().lit()),
                    })
                }
                MatchOp::NotRe(re) => DfExpr::BinaryExpr(BinaryExpr {
                    left: Box::new(col),
                    op: Operator::RegexNotMatch,
                    right: Box::new(re.as_str().lit()),
                }),
            };
            exprs.push(expr);
        }

        Ok(exprs)
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

        // Scan a continuous time range
        if (end - start) / interval > MAX_SCATTER_POINTS || interval <= INTERVAL_1H {
            let single_time_range = time_index_expr
                .clone()
                .gt_eq(DfExpr::Literal(
                    ScalarValue::TimestampMillisecond(
                        Some(self.ctx.start - offset_duration - self.ctx.lookback_delta - range),
                        None,
                    ),
                    None,
                ))
                .and(time_index_expr.lt_eq(DfExpr::Literal(
                    ScalarValue::TimestampMillisecond(
                        Some(self.ctx.end - offset_duration + self.ctx.lookback_delta),
                        None,
                    ),
                    None,
                )));
            return Ok(Some(single_time_range));
        }

        // Otherwise scan scatter ranges separately
        let mut filters = Vec::with_capacity(num_points as usize);
        for timestamp in (start..end).step_by(interval as usize) {
            filters.push(
                time_index_expr
                    .clone()
                    .gt_eq(DfExpr::Literal(
                        ScalarValue::TimestampMillisecond(
                            Some(timestamp - offset_duration - lookback_delta - range),
                            None,
                        ),
                        None,
                    ))
                    .and(time_index_expr.clone().lt_eq(DfExpr::Literal(
                        ScalarValue::TimestampMillisecond(
                            Some(timestamp - offset_duration + lookback_delta),
                            None,
                        ),
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

        let is_time_index_ms = provider
            .as_any()
            .downcast_ref::<DefaultTableSource>()
            .context(UnknownTableSnafu)?
            .table_provider
            .as_any()
            .downcast_ref::<DfTableProviderAdapter>()
            .context(UnknownTableSnafu)?
            .table()
            .schema()
            .timestamp_column()
            .with_context(|| TimeIndexNotFoundSnafu {
                table: table_ref.to_quoted_string(),
            })?
            .data_type
            == ConcreteDataType::timestamp_millisecond_datatype();

        let mut scan_plan = LogicalPlanBuilder::scan(table_ref.clone(), provider, None)
            .context(DataFusionPlanningSnafu)?
            .build()
            .context(DataFusionPlanningSnafu)?;

        if !is_time_index_ms {
            // cast to ms if time_index not in Millisecond precision
            let expr: Vec<_> = self
                .ctx
                .field_columns
                .iter()
                .map(|col| DfExpr::Column(Column::new(Some(table_ref.clone()), col.clone())))
                .chain(self.create_tag_column_exprs()?)
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
        }

        let result = LogicalPlanBuilder::from(scan_plan)
            .build()
            .context(DataFusionPlanningSnafu)?;
        Ok(result)
    }

    /// Setup [PromPlannerContext]'s state fields.
    ///
    /// Returns a logical plan for an empty metric.
    async fn setup_context(&mut self) -> Result<Option<LogicalPlan>> {
        let table_ref = self.table_ref()?;
        let table = match self.table_provider.resolve_table(table_ref.clone()).await {
            Err(e) if e.status_code() == StatusCode::TableNotFound => {
                let plan = self.setup_context_for_empty_metric()?;
                return Ok(Some(plan));
            }
            res => res.context(CatalogSnafu)?,
        };
        let table = table
            .as_any()
            .downcast_ref::<DefaultTableSource>()
            .context(UnknownTableSnafu)?
            .table_provider
            .as_any()
            .downcast_ref::<DfTableProviderAdapter>()
            .context(UnknownTableSnafu)?
            .table();

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

        Ok(None)
    }

    /// Setup [PromPlannerContext]'s state fields for a non existent table
    /// without any rows.
    fn setup_context_for_empty_metric(&mut self) -> Result<LogicalPlan> {
        self.ctx.time_index_column = Some(SPECIAL_TIME_FUNCTION.to_string());
        self.ctx.reset_table_name_and_schema();
        self.ctx.tag_columns = vec![];
        self.ctx.field_columns = vec![DEFAULT_FIELD_COLUMN.to_string()];

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
            match *arg.clone() {
                PromExpr::Subquery(_)
                | PromExpr::VectorSelector(_)
                | PromExpr::MatrixSelector(_)
                | PromExpr::Extension(_)
                | PromExpr::Aggregate(_)
                | PromExpr::Paren(_)
                | PromExpr::Call(_) => {
                    if result.input.replace(*arg.clone()).is_some() {
                        MultipleVectorSnafu { expr: *arg.clone() }.fail()?;
                    }
                }

                _ => {
                    let expr =
                        Self::get_param_as_literal_expr(&Some(Box::new(*arg.clone())), None, None)?;
                    result.literals.push(expr);
                }
            }
        }

        Ok(result)
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
        query_engine_state: &QueryEngineState,
    ) -> Result<(Vec<DfExpr>, Vec<String>)> {
        // TODO(ruihang): check function args list
        let mut other_input_exprs: VecDeque<DfExpr> = other_input_exprs.into();

        // TODO(ruihang): set this according to in-param list
        let field_column_pos = 0;
        let mut exprs = Vec::with_capacity(self.ctx.field_columns.len());
        // New labels after executing the function, e.g. `label_replace` etc.
        let mut new_tags = vec![];
        let scalar_func = match func.name {
            "increase" => ScalarFunc::ExtrapolateUdf(
                Arc::new(Increase::scalar_udf()),
                self.ctx.range.context(ExpectRangeSelectorSnafu)?,
            ),
            "rate" => ScalarFunc::ExtrapolateUdf(
                Arc::new(Rate::scalar_udf()),
                self.ctx.range.context(ExpectRangeSelectorSnafu)?,
            ),
            "delta" => ScalarFunc::ExtrapolateUdf(
                Arc::new(Delta::scalar_udf()),
                self.ctx.range.context(ExpectRangeSelectorSnafu)?,
            ),
            "idelta" => ScalarFunc::Udf(Arc::new(IDelta::<false>::scalar_udf())),
            "irate" => ScalarFunc::Udf(Arc::new(IDelta::<true>::scalar_udf())),
            "resets" => ScalarFunc::Udf(Arc::new(Resets::scalar_udf())),
            "changes" => ScalarFunc::Udf(Arc::new(Changes::scalar_udf())),
            "deriv" => ScalarFunc::Udf(Arc::new(Deriv::scalar_udf())),
            "avg_over_time" => ScalarFunc::Udf(Arc::new(AvgOverTime::scalar_udf())),
            "min_over_time" => ScalarFunc::Udf(Arc::new(MinOverTime::scalar_udf())),
            "max_over_time" => ScalarFunc::Udf(Arc::new(MaxOverTime::scalar_udf())),
            "sum_over_time" => ScalarFunc::Udf(Arc::new(SumOverTime::scalar_udf())),
            "count_over_time" => ScalarFunc::Udf(Arc::new(CountOverTime::scalar_udf())),
            "last_over_time" => ScalarFunc::Udf(Arc::new(LastOverTime::scalar_udf())),
            "absent_over_time" => ScalarFunc::Udf(Arc::new(AbsentOverTime::scalar_udf())),
            "present_over_time" => ScalarFunc::Udf(Arc::new(PresentOverTime::scalar_udf())),
            "stddev_over_time" => ScalarFunc::Udf(Arc::new(StddevOverTime::scalar_udf())),
            "stdvar_over_time" => ScalarFunc::Udf(Arc::new(StdvarOverTime::scalar_udf())),
            "quantile_over_time" => ScalarFunc::Udf(Arc::new(QuantileOverTime::scalar_udf())),
            "predict_linear" => {
                other_input_exprs[0] = DfExpr::Cast(Cast {
                    expr: Box::new(other_input_exprs[0].clone()),
                    data_type: ArrowDataType::Int64,
                });
                ScalarFunc::Udf(Arc::new(PredictLinear::scalar_udf()))
            }
            "holt_winters" => ScalarFunc::Udf(Arc::new(HoltWinters::scalar_udf())),
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
                let (concat_expr, dst_label) =
                    Self::build_concat_labels_expr(&mut other_input_exprs, query_engine_state)?;

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
            "sort" | "sort_desc" | "sort_by_label" | "sort_by_label_desc" | "timestamp" => {
                // These functions are not expression but a part of plan,
                // they are processed by `prom_call_expr_to_plan`.
                for value in &self.ctx.field_columns {
                    let expr = DfExpr::Column(Column::from_name(value));
                    exprs.push(expr);
                }

                ScalarFunc::GeneratedExpr
            }
            "round" => {
                if other_input_exprs.is_empty() {
                    other_input_exprs.push_front(0.0f64.lit());
                }
                ScalarFunc::DataFusionUdf(Arc::new(Round::scalar_udf()))
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
                    ScalarFunc::DataFusionBuiltin(f.clone())
                } else if let Some(factory) = query_engine_state.scalar_function(func.name) {
                    let func_state = query_engine_state.function_state();
                    let query_ctx = self.table_provider.query_ctx();

                    ScalarFunc::DataFusionUdf(Arc::new(factory.provide(FunctionContext {
                        state: func_state,
                        query_ctx: query_ctx.clone(),
                    })))
                } else if let Some(f) = datafusion_functions::math::functions()
                    .iter()
                    .find(|f| f.name() == func.name)
                {
                    ScalarFunc::DataFusionUdf(f.clone())
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

            match scalar_func.clone() {
                ScalarFunc::DataFusionBuiltin(func) => {
                    other_input_exprs.insert(field_column_pos, col_expr);
                    let fn_expr = DfExpr::ScalarFunction(ScalarFunction {
                        func,
                        args: other_input_exprs.clone().into(),
                    });
                    exprs.push(fn_expr);
                    let _ = other_input_exprs.remove(field_column_pos);
                }
                ScalarFunc::DataFusionUdf(func) => {
                    let args = itertools::chain!(
                        other_input_exprs.iter().take(field_column_pos).cloned(),
                        std::iter::once(col_expr),
                        other_input_exprs.iter().skip(field_column_pos).cloned()
                    )
                    .collect_vec();
                    exprs.push(DfExpr::ScalarFunction(ScalarFunction { func, args }))
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
                ScalarFunc::ExtrapolateUdf(func, range_length) => {
                    let ts_range_expr = DfExpr::Column(Column::from_name(
                        RangeManipulate::build_timestamp_range_name(
                            self.ctx.time_index_column.as_ref().unwrap(),
                        ),
                    ));
                    other_input_exprs.insert(field_column_pos, ts_range_expr);
                    other_input_exprs.insert(field_column_pos + 1, col_expr);
                    other_input_exprs
                        .insert(field_column_pos + 2, self.create_time_index_column_expr()?);
                    other_input_exprs.push_back(lit(range_length));
                    let fn_expr = DfExpr::ScalarFunction(ScalarFunction {
                        func,
                        args: other_input_exprs.clone().into(),
                    });
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
        let src_labels = other_input_exprs
            .clone()
            .into_iter()
            .map(|expr| {
                // Cast source label into column
                match expr {
                    DfExpr::Literal(ScalarValue::Utf8(Some(label)), None) => {
                        if label.is_empty() {
                            Ok(DfExpr::Literal(ScalarValue::Null, None))
                        } else {
                            Ok(DfExpr::Column(Column::from_name(label)))
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

    fn create_empty_values_filter_expr(&self) -> Result<DfExpr> {
        let mut exprs = Vec::with_capacity(self.ctx.field_columns.len());
        for value in &self.ctx.field_columns {
            let expr = DfExpr::Column(Column::from_name(value)).is_not_null();
            exprs.push(expr);
        }

        conjunction(exprs).context(ValueNotFoundSnafu {
            table: self.table_ref()?.to_quoted_string(),
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
        let mut non_col_args = Vec::new();
        let aggr = match op.id() {
            token::T_SUM => sum_udaf(),
            token::T_QUANTILE => {
                let q =
                    Self::get_param_as_literal_expr(param, Some(op), Some(ArrowDataType::Float64))?;
                non_col_args.push(q);
                quantile_udaf()
            }
            token::T_AVG => avg_udaf(),
            token::T_COUNT_VALUES | token::T_COUNT => count_udaf(),
            token::T_MIN => min_udaf(),
            token::T_MAX => max_udaf(),
            token::T_GROUP => grouping_udaf(),
            token::T_STDDEV => stddev_pop_udaf(),
            token::T_STDVAR => var_pop_udaf(),
            token::T_TOPK | token::T_BOTTOMK => UnsupportedExprSnafu {
                name: format!("{op:?}"),
            }
            .fail()?,
            _ => UnexpectedTokenSnafu { token: op }.fail()?,
        };

        // perform aggregate operation to each value column
        let exprs: Vec<DfExpr> = self
            .ctx
            .field_columns
            .iter()
            .map(|col| {
                non_col_args.push(DfExpr::Column(Column::from_name(col)));
                let expr = aggr.call(non_col_args.clone());
                non_col_args.pop();
                expr
            })
            .collect::<Vec<_>>();

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
        param: &Option<Box<PromExpr>>,
        op: Option<TokenType>,
        expected_type: Option<ArrowDataType>,
    ) -> Result<DfExpr> {
        let prom_param = param.as_deref().with_context(|| {
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
                    },
                }))
            })
            .collect();

        let normalized_exprs =
            normalize_cols(exprs.iter().cloned(), input_plan).context(DataFusionPlanningSnafu)?;
        Ok(normalized_exprs)
    }

    /// Try to build a [f64] from [PromExpr].
    #[deprecated(
        note = "use `Self::get_param_as_literal_expr` instead. This is only for `create_histogram_plan`"
    )]
    fn try_build_float_literal(expr: &PromExpr) -> Option<f64> {
        match expr {
            PromExpr::NumberLiteral(NumberLiteral { val }) => Some(*val),
            PromExpr::Paren(ParenExpr { expr }) => Self::try_build_float_literal(expr),
            PromExpr::Unary(UnaryExpr { expr, .. }) => {
                Self::try_build_float_literal(expr).map(|f| -f)
            }
            PromExpr::StringLiteral(_)
            | PromExpr::Binary(_)
            | PromExpr::VectorSelector(_)
            | PromExpr::MatrixSelector(_)
            | PromExpr::Call(_)
            | PromExpr::Extension(_)
            | PromExpr::Aggregate(_)
            | PromExpr::Subquery(_) => None,
        }
    }

    /// Create a [SPECIAL_HISTOGRAM_QUANTILE] plan.
    async fn create_histogram_plan(
        &mut self,
        args: &PromFunctionArgs,
        query_engine_state: &QueryEngineState,
    ) -> Result<LogicalPlan> {
        if args.args.len() != 2 {
            return FunctionInvalidArgumentSnafu {
                fn_name: SPECIAL_HISTOGRAM_QUANTILE.to_string(),
            }
            .fail();
        }
        #[allow(deprecated)]
        let phi = Self::try_build_float_literal(&args.args[0]).with_context(|| {
            FunctionInvalidArgumentSnafu {
                fn_name: SPECIAL_HISTOGRAM_QUANTILE.to_string(),
            }
        })?;

        let input = args.args[1].as_ref().clone();
        let input_plan = self.prom_expr_to_plan(&input, query_engine_state).await?;

        if !self.ctx.has_le_tag() {
            // Return empty result instead of error when 'le' column is not found
            // This handles the case when histogram metrics don't exist
            return Ok(LogicalPlan::EmptyRelation(
                datafusion::logical_expr::EmptyRelation {
                    produce_one_row: false,
                    schema: Arc::new(DFSchema::empty()),
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
                fn_name: SPECIAL_HISTOGRAM_QUANTILE.to_string(),
            })?
            .clone();
        // remove le column from tag columns
        self.ctx.tag_columns.retain(|col| col != LE_COLUMN_NAME);

        Ok(LogicalPlan::Extension(Extension {
            node: Arc::new(
                HistogramFold::new(
                    LE_COLUMN_NAME.to_string(),
                    field_column,
                    time_index_column,
                    phi,
                    input_plan,
                )
                .context(DataFusionPlanningSnafu)?,
            ),
        }))
    }

    /// Create a [SPECIAL_VECTOR_FUNCTION] plan
    async fn create_vector_plan(&mut self, args: &PromFunctionArgs) -> Result<LogicalPlan> {
        if args.args.len() != 1 {
            return FunctionInvalidArgumentSnafu {
                fn_name: SPECIAL_VECTOR_FUNCTION.to_string(),
            }
            .fail();
        }
        let lit = Self::get_param_as_literal_expr(&Some(args.args[0].clone()), None, None)?;

        // reuse `SPECIAL_TIME_FUNCTION` as name of time index column
        self.ctx.time_index_column = Some(SPECIAL_TIME_FUNCTION.to_string());
        self.ctx.reset_table_name_and_schema();
        self.ctx.tag_columns = vec![];
        self.ctx.field_columns = vec![GREPTIME_VALUE.to_string()];
        Ok(LogicalPlan::Extension(Extension {
            node: Arc::new(
                EmptyMetric::new(
                    self.ctx.start,
                    self.ctx.end,
                    self.ctx.interval,
                    SPECIAL_TIME_FUNCTION.to_string(),
                    GREPTIME_VALUE.to_string(),
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
        ensure!(
            self.ctx.field_columns.len() == 1,
            MultiFieldsNotSupportedSnafu {
                operator: SCALAR_FUNCTION
            },
        );
        let scalar_plan = LogicalPlan::Extension(Extension {
            node: Arc::new(
                ScalarCalculate::new(
                    self.ctx.start,
                    self.ctx.end,
                    self.ctx.interval,
                    input,
                    self.ctx.time_index_column.as_ref().unwrap(),
                    &self.ctx.tag_columns,
                    &self.ctx.field_columns[0],
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
                    Some(build_special_time_expr(SPECIAL_TIME_FUNCTION))
                } else {
                    None
                }
            }
            PromExpr::Paren(ParenExpr { expr }) => Self::try_build_literal_expr(expr),
            // TODO(ruihang): support Unary operator
            PromExpr::Unary(UnaryExpr { expr, .. }) => Self::try_build_literal_expr(expr),
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

    fn try_build_special_time_expr(expr: &PromExpr, time_index_col: &str) -> Option<DfExpr> {
        match expr {
            PromExpr::Call(Call { func, .. }) => {
                if func.name == SPECIAL_TIME_FUNCTION {
                    Some(build_special_time_expr(time_index_col))
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    /// Return a lambda to build binary expression from token.
    /// Because some binary operator are function in DataFusion like `atan2` or `^`.
    #[allow(clippy::type_complexity)]
    fn prom_token_to_binary_expr_builder(
        token: TokenType,
    ) -> Result<Box<dyn Fn(DfExpr, DfExpr) -> Result<DfExpr>>> {
        match token.id() {
            token::T_ADD => Ok(Box::new(|lhs, rhs| Ok(lhs + rhs))),
            token::T_SUB => Ok(Box::new(|lhs, rhs| Ok(lhs - rhs))),
            token::T_MUL => Ok(Box::new(|lhs, rhs| Ok(lhs * rhs))),
            token::T_DIV => Ok(Box::new(|lhs, rhs| Ok(lhs / rhs))),
            token::T_MOD => Ok(Box::new(|lhs: DfExpr, rhs| Ok(lhs % rhs))),
            token::T_EQLC => Ok(Box::new(|lhs, rhs| Ok(lhs.eq(rhs)))),
            token::T_NEQ => Ok(Box::new(|lhs, rhs| Ok(lhs.not_eq(rhs)))),
            token::T_GTR => Ok(Box::new(|lhs, rhs| Ok(lhs.gt(rhs)))),
            token::T_LSS => Ok(Box::new(|lhs, rhs| Ok(lhs.lt(rhs)))),
            token::T_GTE => Ok(Box::new(|lhs, rhs| Ok(lhs.gt_eq(rhs)))),
            token::T_LTE => Ok(Box::new(|lhs, rhs| Ok(lhs.lt_eq(rhs)))),
            token::T_POW => Ok(Box::new(|lhs, rhs| {
                Ok(DfExpr::ScalarFunction(ScalarFunction {
                    func: datafusion_functions::math::power(),
                    args: vec![lhs, rhs],
                }))
            })),
            token::T_ATAN2 => Ok(Box::new(|lhs, rhs| {
                Ok(DfExpr::ScalarFunction(ScalarFunction {
                    func: datafusion_functions::math::atan2(),
                    args: vec![lhs, rhs],
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
    ) -> Result<LogicalPlan> {
        let mut left_tag_columns = if only_join_time_index {
            BTreeSet::new()
        } else {
            self.ctx
                .tag_columns
                .iter()
                .cloned()
                .collect::<BTreeSet<_>>()
        };
        let mut right_tag_columns = left_tag_columns.clone();

        // apply modifier
        if let Some(modifier) = modifier {
            // apply label modifier
            if let Some(matching) = &modifier.matching {
                match matching {
                    // keeps columns mentioned in `on`
                    LabelModifier::Include(on) => {
                        let mask = on.labels.iter().cloned().collect::<BTreeSet<_>>();
                        left_tag_columns = left_tag_columns.intersection(&mask).cloned().collect();
                        right_tag_columns =
                            right_tag_columns.intersection(&mask).cloned().collect();
                    }
                    // removes columns memtioned in `ignoring`
                    LabelModifier::Exclude(ignoring) => {
                        // doesn't check existence of label
                        for label in &ignoring.labels {
                            let _ = left_tag_columns.remove(label);
                            let _ = right_tag_columns.remove(label);
                        }
                    }
                }
            }
        }

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
                None,
                NullEquality::NullEqualsNull,
            )
            .context(DataFusionPlanningSnafu)?
            .build()
            .context(DataFusionPlanningSnafu)
    }

    /// Build a set operator (AND/OR/UNLESS)
    fn set_op_on_non_field_columns(
        &mut self,
        left: LogicalPlan,
        mut right: LogicalPlan,
        left_context: PromPlannerContext,
        right_context: PromPlannerContext,
        op: TokenType,
        modifier: &Option<BinModifier>,
    ) -> Result<LogicalPlan> {
        let mut left_tag_col_set = left_context
            .tag_columns
            .iter()
            .cloned()
            .collect::<HashSet<_>>();
        let mut right_tag_col_set = right_context
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

        // apply modifier
        if let Some(modifier) = modifier {
            // one-to-many and many-to-one are not supported
            ensure!(
                matches!(
                    modifier.card,
                    VectorMatchCardinality::OneToOne | VectorMatchCardinality::ManyToMany
                ),
                UnsupportedVectorMatchSnafu {
                    name: modifier.card.clone(),
                },
            );
            // apply label modifier
            if let Some(matching) = &modifier.matching {
                match matching {
                    // keeps columns mentioned in `on`
                    LabelModifier::Include(on) => {
                        let mask = on.labels.iter().cloned().collect::<HashSet<_>>();
                        left_tag_col_set = left_tag_col_set.intersection(&mask).cloned().collect();
                        right_tag_col_set =
                            right_tag_col_set.intersection(&mask).cloned().collect();
                    }
                    // removes columns memtioned in `ignoring`
                    LabelModifier::Exclude(ignoring) => {
                        // doesn't check existence of label
                        for label in &ignoring.labels {
                            let _ = left_tag_col_set.remove(label);
                            let _ = right_tag_col_set.remove(label);
                        }
                    }
                }
            }
        }
        // ensure two sides have the same tag columns
        if !matches!(op.id(), token::T_LOR) {
            ensure!(
                left_tag_col_set == right_tag_col_set,
                CombineTableColumnMismatchSnafu {
                    left: left_tag_col_set.into_iter().collect::<Vec<_>>(),
                    right: right_tag_col_set.into_iter().collect::<Vec<_>>(),
                }
            )
        };
        let left_time_index = left_context.time_index_column.clone().unwrap();
        let right_time_index = right_context.time_index_column.clone().unwrap();
        let join_keys = left_tag_col_set
            .iter()
            .cloned()
            .chain([left_time_index.clone()])
            .collect::<Vec<_>>();
        self.ctx.time_index_column = Some(left_time_index.clone());

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

        ensure!(
            left_context.field_columns.len() == 1,
            MultiFieldsNotSupportedSnafu {
                operator: "AND operator"
            }
        );
        // Update the field column in context.
        // The AND/UNLESS operator only keep the field column in left input.
        let left_field_col = left_context.field_columns.first().unwrap();
        self.ctx.field_columns = vec![left_field_col.clone()];

        // Generate join plan.
        // All set operations in PromQL are "distinct"
        match op.id() {
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
        // checks
        ensure!(
            left_context.field_columns.len() == right_context.field_columns.len(),
            CombineTableColumnMismatchSnafu {
                left: left_context.field_columns.clone(),
                right: right_context.field_columns.clone()
            }
        );
        ensure!(
            left_context.field_columns.len() == 1,
            MultiFieldsNotSupportedSnafu {
                operator: "OR operator"
            }
        );

        // prepare hash sets
        let all_tags = left_tag_cols_set
            .union(&right_tag_cols_set)
            .cloned()
            .collect::<HashSet<_>>();
        let tags_not_in_left = all_tags
            .difference(&left_tag_cols_set)
            .cloned()
            .collect::<Vec<_>>();
        let tags_not_in_right = all_tags
            .difference(&right_tag_cols_set)
            .cloned()
            .collect::<Vec<_>>();
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
        // Take the name of first field column. The length is checked above.
        let left_field_col = left_context.field_columns.first().unwrap();
        let right_field_col = right_context.field_columns.first().unwrap();

        // step 0: fill all columns in output schema
        let mut all_columns_set = left
            .schema()
            .fields()
            .iter()
            .chain(right.schema().fields().iter())
            .map(|field| field.name().clone())
            .collect::<HashSet<_>>();
        // remove time index column
        all_columns_set.remove(&left_time_index_column);
        all_columns_set.remove(&right_time_index_column);
        // remove field column in the right
        if left_field_col != right_field_col {
            all_columns_set.remove(right_field_col);
        }
        let mut all_columns = all_columns_set.into_iter().collect::<Vec<_>>();
        // sort to ensure the generated schema is not volatile
        all_columns.sort_unstable();
        // use left time index column name as the result time index column name
        all_columns.insert(0, left_time_index_column.clone());

        // step 1: align schema using project, fill non-exist columns with null
        let left_proj_exprs = all_columns.iter().map(|col| {
            if tags_not_in_left.contains(col) {
                DfExpr::Literal(ScalarValue::Utf8(None), None).alias(col.to_string())
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
        let right_qualifier_for_field = right
            .schema()
            .iter()
            .find(|(_, f)| f.name() == right_field_col)
            .map(|(q, _)| q)
            .context(ColumnNotFoundSnafu {
                col: right_field_col.to_string(),
            })?
            .cloned();

        // `skip（1)` to skip the time index column
        let right_proj_exprs_without_time_index = all_columns.iter().skip(1).map(|col| {
            // expr
            if col == left_field_col && left_field_col != right_field_col {
                // qualify field in right side if necessary to handle different field name
                DfExpr::Column(Column::new(
                    right_qualifier_for_field.clone(),
                    right_field_col,
                ))
            } else if tags_not_in_right.contains(col) {
                DfExpr::Literal(ScalarValue::Utf8(None), None).alias(col.to_string())
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
        // step 3: build `UnionDistinctOn` plan
        let schema = left_projected.schema().clone();
        let union_distinct_on = UnionDistinctOn::new(
            left_projected,
            right_projected,
            match_columns,
            left_time_index_column.clone(),
            schema,
        );
        let result = LogicalPlan::Extension(Extension {
            node: Arc::new(union_distinct_on),
        });

        // step 4: update context
        self.ctx.time_index_column = Some(left_time_index_column);
        self.ctx.tag_columns = all_tags.into_iter().collect();
        self.ctx.field_columns = vec![left_field_col.to_string()];

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
        let non_field_columns_iter = self
            .ctx
            .tag_columns
            .iter()
            .chain(self.ctx.time_index_column.iter())
            .map(|col| {
                Ok(DfExpr::Column(Column::new(
                    self.ctx.table_name.clone().map(TableReference::bare),
                    col,
                )))
            });

        // build computation exprs
        let result_field_columns = self
            .ctx
            .field_columns
            .iter()
            .map(name_to_expr)
            .collect::<Result<Vec<_>>>()?;

        // alias the computation exprs to remove qualifier
        self.ctx.field_columns = result_field_columns
            .iter()
            .map(|expr| expr.schema_name().to_string())
            .collect();
        let field_columns_iter = result_field_columns
            .into_iter()
            .zip(self.ctx.field_columns.iter())
            .map(|(expr, name)| Ok(DfExpr::Alias(Alias::new(expr, None::<String>, name))));

        // chain non-field columns (unchanged) and field columns (applied computation then alias)
        let project_fields = non_field_columns_iter
            .chain(field_columns_iter)
            .collect::<Result<Vec<_>>>()?;

        LogicalPlanBuilder::from(input)
            .project(project_fields)
            .context(DataFusionPlanningSnafu)?
            .build()
            .context(DataFusionPlanningSnafu)
    }

    /// Build a filter plan that filter on value column. Notice that only one value column
    /// is expected.
    fn filter_on_field_column<F>(
        &self,
        input: LogicalPlan,
        mut name_to_expr: F,
    ) -> Result<LogicalPlan>
    where
        F: FnMut(&String) -> Result<DfExpr>,
    {
        ensure!(
            self.ctx.field_columns.len() == 1,
            UnsupportedExprSnafu {
                name: "filter on multi-value input"
            }
        );

        let field_column_filter = name_to_expr(&self.ctx.field_columns[0])?;

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

    use catalog::memory::{new_memory_catalog_manager, MemoryCatalogManager};
    use catalog::RegisterTableRequest;
    use common_base::Plugins;
    use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
    use common_query::test_util::DummyDecoder;
    use datatypes::prelude::ConcreteDataType;
    use datatypes::schema::{ColumnSchema, Schema};
    use promql_parser::label::Labels;
    use promql_parser::parser;
    use session::context::QueryContext;
    use table::metadata::{TableInfoBuilder, TableMetaBuilder};
    use table::test_util::EmptyTable;

    use super::*;
    use crate::options::QueryOptions;

    fn build_query_engine_state() -> QueryEngineState {
        QueryEngineState::new(
            new_memory_catalog_manager().unwrap(),
            None,  // region_query_handler
            None,  // table_mutation_handler
            None,  // procedure_service_handler
            None,  // flow_service_handler
            false, // with_dist_planner
            Plugins::default(),
            QueryOptions::default(),
        )
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
                .name(table_name.to_string())
                .meta(table_meta)
                .build()
                .unwrap();
            let table = EmptyTable::from_table_info(&table_info);

            assert!(catalog_list
                .register_table_sync(RegisterTableRequest {
                    catalog: DEFAULT_CATALOG_NAME.to_string(),
                    schema: schema_name.to_string(),
                    table_name: table_name.to_string(),
                    table_id: 1024,
                    table,
                })
                .is_ok());
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
                    "greptime_timestamp".to_string(),
                    ConcreteDataType::timestamp_millisecond_datatype(),
                    false,
                )
                .with_time_index(true),
            );
            columns.push(ColumnSchema::new(
                "greptime_value".to_string(),
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
                .name(table_name.to_string())
                .meta(table_meta)
                .build()
                .unwrap();
            let table = EmptyTable::from_table_info(&table_info);

            assert!(catalog_list
                .register_table_sync(RegisterTableRequest {
                    catalog: DEFAULT_CATALOG_NAME.to_string(),
                    schema: schema_name.to_string(),
                    table_name: table_name.to_string(),
                    table_id: 1024,
                    table,
                })
                .is_ok());
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
            "Filter: TEMPLATE(field_0) IS NOT NULL [timestamp:Timestamp(Millisecond, None), TEMPLATE(field_0):Float64;N, tag_0:Utf8]\
            \n  Projection: some_metric.timestamp, TEMPLATE(some_metric.field_0) AS TEMPLATE(field_0), some_metric.tag_0 [timestamp:Timestamp(Millisecond, None), TEMPLATE(field_0):Float64;N, tag_0:Utf8]\
            \n    PromInstantManipulate: range=[0..100000000], lookback=[1000], interval=[5000], time index=[timestamp] [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]\
            \n      PromSeriesDivide: tags=[\"tag_0\"] [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]\
            \n        Sort: some_metric.tag_0 ASC NULLS FIRST, some_metric.timestamp ASC NULLS FIRST [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]\
            \n          Filter: some_metric.tag_0 != Utf8(\"bar\") AND some_metric.timestamp >= TimestampMillisecond(-1000, None) AND some_metric.timestamp <= TimestampMillisecond(100001000, None) [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]\
            \n            TableScan: some_metric [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]"
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
    #[should_panic]
    async fn single_timestamp() {
        do_single_instant_function_call("timestamp", "").await;
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
            "Sort: some_metric.tag_1 ASC NULLS LAST, some_metric.timestamp ASC NULLS LAST [tag_1:Utf8, timestamp:Timestamp(Millisecond, None), TEMPLATE(some_metric.field_0):Float64;N, TEMPLATE(some_metric.field_1):Float64;N]\
            \n  Aggregate: groupBy=[[some_metric.tag_1, some_metric.timestamp]], aggr=[[TEMPLATE(some_metric.field_0), TEMPLATE(some_metric.field_1)]] [tag_1:Utf8, timestamp:Timestamp(Millisecond, None), TEMPLATE(some_metric.field_0):Float64;N, TEMPLATE(some_metric.field_1):Float64;N]\
            \n    PromInstantManipulate: range=[0..100000000], lookback=[1000], interval=[5000], time index=[timestamp] [tag_0:Utf8, tag_1:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N, field_1:Float64;N]\
            \n      PromSeriesDivide: tags=[\"tag_0\", \"tag_1\"] [tag_0:Utf8, tag_1:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N, field_1:Float64;N]\
            \n        Sort: some_metric.tag_0 ASC NULLS FIRST, some_metric.tag_1 ASC NULLS FIRST, some_metric.timestamp ASC NULLS FIRST [tag_0:Utf8, tag_1:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N, field_1:Float64;N]\
            \n          Filter: some_metric.tag_0 != Utf8(\"bar\") AND some_metric.timestamp >= TimestampMillisecond(-1000, None) AND some_metric.timestamp <= TimestampMillisecond(100001000, None) [tag_0:Utf8, tag_1:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N, field_1:Float64;N]\
            \n            TableScan: some_metric [tag_0:Utf8, tag_1:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N, field_1:Float64;N]"
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
            "Sort: some_metric.tag_0 ASC NULLS LAST, some_metric.timestamp ASC NULLS LAST [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), TEMPLATE(some_metric.field_0):Float64;N, TEMPLATE(some_metric.field_1):Float64;N]\
            \n  Aggregate: groupBy=[[some_metric.tag_0, some_metric.timestamp]], aggr=[[TEMPLATE(some_metric.field_0), TEMPLATE(some_metric.field_1)]] [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), TEMPLATE(some_metric.field_0):Float64;N, TEMPLATE(some_metric.field_1):Float64;N]\
            \n    PromInstantManipulate: range=[0..100000000], lookback=[1000], interval=[5000], time index=[timestamp] [tag_0:Utf8, tag_1:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N, field_1:Float64;N]\
            \n      PromSeriesDivide: tags=[\"tag_0\", \"tag_1\"] [tag_0:Utf8, tag_1:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N, field_1:Float64;N]\
            \n        Sort: some_metric.tag_0 ASC NULLS FIRST, some_metric.tag_1 ASC NULLS FIRST, some_metric.timestamp ASC NULLS FIRST [tag_0:Utf8, tag_1:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N, field_1:Float64;N]\
            \n          Filter: some_metric.tag_0 != Utf8(\"bar\") AND some_metric.timestamp >= TimestampMillisecond(-1000, None) AND some_metric.timestamp <= TimestampMillisecond(100001000, None) [tag_0:Utf8, tag_1:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N, field_1:Float64;N]\
            \n            TableScan: some_metric [tag_0:Utf8, tag_1:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N, field_1:Float64;N]"
        ).replace("TEMPLATE", plan_name);
        assert_eq!(plan.display_indent_schema().to_string(), expected_without);
    }

    #[tokio::test]
    async fn aggregate_sum() {
        do_aggregate_expr_plan("sum", "sum").await;
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
    #[should_panic] // output type doesn't match
    async fn aggregate_group() {
        do_aggregate_expr_plan("grouping", "GROUPING").await;
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

        let  expected = String::from(
            "Projection: rhs.tag_0, rhs.timestamp, lhs.field_0 + rhs.field_0 AS lhs.field_0 + rhs.field_0 [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), lhs.field_0 + rhs.field_0:Float64;N]\
            \n  Inner Join: lhs.tag_0 = rhs.tag_0, lhs.timestamp = rhs.timestamp [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N, tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]\
            \n    SubqueryAlias: lhs [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]\
            \n      PromInstantManipulate: range=[0..100000000], lookback=[1000], interval=[5000], time index=[timestamp] [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]\
            \n        PromSeriesDivide: tags=[\"tag_0\"] [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]\
            \n          Sort: some_metric.tag_0 ASC NULLS FIRST, some_metric.timestamp ASC NULLS FIRST [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]\
            \n            Filter: some_metric.tag_0 = Utf8(\"foo\") AND some_metric.timestamp >= TimestampMillisecond(-1000, None) AND some_metric.timestamp <= TimestampMillisecond(100001000, None) [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]\
            \n              TableScan: some_metric [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]\
            \n    SubqueryAlias: rhs [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]\
            \n      PromInstantManipulate: range=[0..100000000], lookback=[1000], interval=[5000], time index=[timestamp] [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]\
            \n        PromSeriesDivide: tags=[\"tag_0\"] [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]\
            \n          Sort: some_metric.tag_0 ASC NULLS FIRST, some_metric.timestamp ASC NULLS FIRST [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]\
            \n            Filter: some_metric.tag_0 = Utf8(\"bar\") AND some_metric.timestamp >= TimestampMillisecond(-1000, None) AND some_metric.timestamp <= TimestampMillisecond(100001000, None) [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]\
            \n              TableScan: some_metric [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]"
        );

        assert_eq!(plan.display_indent_schema().to_string(), expected);
    }

    async fn indie_query_plan_compare(query: &str, expected: String) {
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

        assert_eq!(plan.display_indent_schema().to_string(), expected);
    }

    #[tokio::test]
    async fn binary_op_literal_column() {
        let query = r#"1 + some_metric{tag_0="bar"}"#;
        let expected = String::from(
            "Projection: some_metric.tag_0, some_metric.timestamp, Float64(1) + some_metric.field_0 AS Float64(1) + field_0 [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), Float64(1) + field_0:Float64;N]\
            \n  PromInstantManipulate: range=[0..100000000], lookback=[1000], interval=[5000], time index=[timestamp] [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]\
            \n    PromSeriesDivide: tags=[\"tag_0\"] [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]\
            \n      Sort: some_metric.tag_0 ASC NULLS FIRST, some_metric.timestamp ASC NULLS FIRST [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]\
            \n        Filter: some_metric.tag_0 = Utf8(\"bar\") AND some_metric.timestamp >= TimestampMillisecond(-1000, None) AND some_metric.timestamp <= TimestampMillisecond(100001000, None) [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]\
            \n          TableScan: some_metric [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]"
        );

        indie_query_plan_compare(query, expected).await;
    }

    #[tokio::test]
    async fn binary_op_literal_literal() {
        let query = r#"1 + 1"#;
        let expected = String::from("EmptyMetric: range=[0..100000000], interval=[5000] [time:Timestamp(Millisecond, None), value:Float64;N]");

        indie_query_plan_compare(query, expected).await;
    }

    #[tokio::test]
    async fn simple_bool_grammar() {
        let query = "some_metric != bool 1.2345";
        let expected = String::from(
            "Projection: some_metric.tag_0, some_metric.timestamp, CAST(some_metric.field_0 != Float64(1.2345) AS Float64) AS field_0 != Float64(1.2345) [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0 != Float64(1.2345):Float64;N]\
            \n  PromInstantManipulate: range=[0..100000000], lookback=[1000], interval=[5000], time index=[timestamp] [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]\
            \n    PromSeriesDivide: tags=[\"tag_0\"] [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]\
            \n      Sort: some_metric.tag_0 ASC NULLS FIRST, some_metric.timestamp ASC NULLS FIRST [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]\
            \n        Filter: some_metric.timestamp >= TimestampMillisecond(-1000, None) AND some_metric.timestamp <= TimestampMillisecond(100001000, None) [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]\
            \n          TableScan: some_metric [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]"
        );

        indie_query_plan_compare(query, expected).await;
    }

    #[tokio::test]
    async fn bool_with_additional_arithmetic() {
        let query = "some_metric + (1 == bool 2)";
        let expected = String::from(
            "Projection: some_metric.tag_0, some_metric.timestamp, some_metric.field_0 + CAST(Float64(1) = Float64(2) AS Float64) AS field_0 + Float64(1) = Float64(2) [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0 + Float64(1) = Float64(2):Float64;N]\
            \n  PromInstantManipulate: range=[0..100000000], lookback=[1000], interval=[5000], time index=[timestamp] [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]\
            \n    PromSeriesDivide: tags=[\"tag_0\"] [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]\
            \n      Sort: some_metric.tag_0 ASC NULLS FIRST, some_metric.timestamp ASC NULLS FIRST [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]\
            \n        Filter: some_metric.timestamp >= TimestampMillisecond(-1000, None) AND some_metric.timestamp <= TimestampMillisecond(100001000, None) [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]\
            \n          TableScan: some_metric [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]"
        );

        indie_query_plan_compare(query, expected).await;
    }

    #[tokio::test]
    async fn simple_unary() {
        let query = "-some_metric";
        let expected = String::from(
            "Projection: some_metric.tag_0, some_metric.timestamp, (- some_metric.field_0) AS (- field_0) [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), (- field_0):Float64;N]\
            \n  PromInstantManipulate: range=[0..100000000], lookback=[1000], interval=[5000], time index=[timestamp] [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]\
            \n    PromSeriesDivide: tags=[\"tag_0\"] [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]\
            \n      Sort: some_metric.tag_0 ASC NULLS FIRST, some_metric.timestamp ASC NULLS FIRST [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]\
            \n        Filter: some_metric.timestamp >= TimestampMillisecond(-1000, None) AND some_metric.timestamp <= TimestampMillisecond(100001000, None) [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]\
            \n          TableScan: some_metric [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]"
        );

        indie_query_plan_compare(query, expected).await;
    }

    #[tokio::test]
    async fn increase_aggr() {
        let query = "increase(some_metric[5m])";
        let expected = String::from(
            "Filter: prom_increase(timestamp_range,field_0,timestamp,Int64(300000)) IS NOT NULL [timestamp:Timestamp(Millisecond, None), prom_increase(timestamp_range,field_0,timestamp,Int64(300000)):Float64;N, tag_0:Utf8]\
            \n  Projection: some_metric.timestamp, prom_increase(timestamp_range, field_0, some_metric.timestamp, Int64(300000)) AS prom_increase(timestamp_range,field_0,timestamp,Int64(300000)), some_metric.tag_0 [timestamp:Timestamp(Millisecond, None), prom_increase(timestamp_range,field_0,timestamp,Int64(300000)):Float64;N, tag_0:Utf8]\
            \n    PromRangeManipulate: req range=[0..100000000], interval=[5000], eval range=[300000], time index=[timestamp], values=[\"field_0\"] [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Dictionary(Int64, Float64);N, timestamp_range:Dictionary(Int64, Timestamp(Millisecond, None))]\
            \n      PromSeriesNormalize: offset=[0], time index=[timestamp], filter NaN: [true] [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]\
            \n        PromSeriesDivide: tags=[\"tag_0\"] [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]\
            \n          Sort: some_metric.tag_0 ASC NULLS FIRST, some_metric.timestamp ASC NULLS FIRST [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]\
            \n            Filter: some_metric.timestamp >= TimestampMillisecond(-301000, None) AND some_metric.timestamp <= TimestampMillisecond(100001000, None) [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]\
            \n              TableScan: some_metric [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]"
        );

        indie_query_plan_compare(query, expected).await;
    }

    #[tokio::test]
    async fn less_filter_on_value() {
        let query = "some_metric < 1.2345";
        let expected = String::from(
            "Filter: some_metric.field_0 < Float64(1.2345) [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]\
            \n  PromInstantManipulate: range=[0..100000000], lookback=[1000], interval=[5000], time index=[timestamp] [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]\
            \n    PromSeriesDivide: tags=[\"tag_0\"] [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]\
            \n      Sort: some_metric.tag_0 ASC NULLS FIRST, some_metric.timestamp ASC NULLS FIRST [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]\
            \n        Filter: some_metric.timestamp >= TimestampMillisecond(-1000, None) AND some_metric.timestamp <= TimestampMillisecond(100001000, None) [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]\
            \n          TableScan: some_metric [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]"
        );

        indie_query_plan_compare(query, expected).await;
    }

    #[tokio::test]
    async fn count_over_time() {
        let query = "count_over_time(some_metric[5m])";
        let expected = String::from(
            "Filter: prom_count_over_time(timestamp_range,field_0) IS NOT NULL [timestamp:Timestamp(Millisecond, None), prom_count_over_time(timestamp_range,field_0):Float64;N, tag_0:Utf8]\
            \n  Projection: some_metric.timestamp, prom_count_over_time(timestamp_range, field_0) AS prom_count_over_time(timestamp_range,field_0), some_metric.tag_0 [timestamp:Timestamp(Millisecond, None), prom_count_over_time(timestamp_range,field_0):Float64;N, tag_0:Utf8]\
            \n    PromRangeManipulate: req range=[0..100000000], interval=[5000], eval range=[300000], time index=[timestamp], values=[\"field_0\"] [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Dictionary(Int64, Float64);N, timestamp_range:Dictionary(Int64, Timestamp(Millisecond, None))]\
            \n      PromSeriesNormalize: offset=[0], time index=[timestamp], filter NaN: [true] [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]\
            \n        PromSeriesDivide: tags=[\"tag_0\"] [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]\
            \n          Sort: some_metric.tag_0 ASC NULLS FIRST, some_metric.timestamp ASC NULLS FIRST [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]\
            \n            Filter: some_metric.timestamp >= TimestampMillisecond(-301000, None) AND some_metric.timestamp <= TimestampMillisecond(100001000, None) [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]\
            \n              TableScan: some_metric [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]"
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
        let expected = "Projection: http_server_requests_seconds_count.uri, http_server_requests_seconds_count.kubernetes_namespace, http_server_requests_seconds_count.kubernetes_pod_name, http_server_requests_seconds_count.greptime_timestamp, http_server_requests_seconds_sum.greptime_value / http_server_requests_seconds_count.greptime_value AS http_server_requests_seconds_sum.greptime_value / http_server_requests_seconds_count.greptime_value\
            \n  Inner Join: http_server_requests_seconds_sum.greptime_timestamp = http_server_requests_seconds_count.greptime_timestamp, http_server_requests_seconds_sum.uri = http_server_requests_seconds_count.uri\
            \n    SubqueryAlias: http_server_requests_seconds_sum\
            \n      PromInstantManipulate: range=[0..100000000], lookback=[1000], interval=[5000], time index=[greptime_timestamp]\
            \n        PromSeriesDivide: tags=[\"uri\", \"kubernetes_namespace\", \"kubernetes_pod_name\"]\
            \n          Sort: http_server_requests_seconds_sum.uri ASC NULLS FIRST, http_server_requests_seconds_sum.kubernetes_namespace ASC NULLS FIRST, http_server_requests_seconds_sum.kubernetes_pod_name ASC NULLS FIRST, http_server_requests_seconds_sum.greptime_timestamp ASC NULLS FIRST\
            \n            Filter: http_server_requests_seconds_sum.uri = Utf8(\"/accounts/login\") AND http_server_requests_seconds_sum.greptime_timestamp >= TimestampMillisecond(-1000, None) AND http_server_requests_seconds_sum.greptime_timestamp <= TimestampMillisecond(100001000, None)\
            \n              TableScan: http_server_requests_seconds_sum\
            \n    SubqueryAlias: http_server_requests_seconds_count\
            \n      PromInstantManipulate: range=[0..100000000], lookback=[1000], interval=[5000], time index=[greptime_timestamp]\
            \n        PromSeriesDivide: tags=[\"uri\", \"kubernetes_namespace\", \"kubernetes_pod_name\"]\
            \n          Sort: http_server_requests_seconds_count.uri ASC NULLS FIRST, http_server_requests_seconds_count.kubernetes_namespace ASC NULLS FIRST, http_server_requests_seconds_count.kubernetes_pod_name ASC NULLS FIRST, http_server_requests_seconds_count.greptime_timestamp ASC NULLS FIRST\
            \n            Filter: http_server_requests_seconds_count.uri = Utf8(\"/accounts/login\") AND http_server_requests_seconds_count.greptime_timestamp >= TimestampMillisecond(-1000, None) AND http_server_requests_seconds_count.greptime_timestamp <= TimestampMillisecond(100001000, None)\
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
            "PromInstantManipulate: range=[0..100000000], lookback=[1000], interval=[5000], time index=[timestamp] [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]\
            \n  PromSeriesDivide: tags=[\"tag_0\"] [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]\
            \n    Sort: greptime_private.some_alt_metric.tag_0 ASC NULLS FIRST, greptime_private.some_alt_metric.timestamp ASC NULLS FIRST [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]\
            \n      Filter: greptime_private.some_alt_metric.timestamp >= TimestampMillisecond(-1000, None) AND greptime_private.some_alt_metric.timestamp <= TimestampMillisecond(100001000, None) [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]\
            \n        TableScan: greptime_private.some_alt_metric [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]"
        );

        indie_query_plan_compare(query, expected).await;

        let query = "some_alt_metric{__database__=\"greptime_private\"}";
        let expected = String::from(
            "PromInstantManipulate: range=[0..100000000], lookback=[1000], interval=[5000], time index=[timestamp] [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]\
            \n  PromSeriesDivide: tags=[\"tag_0\"] [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]\
            \n    Sort: greptime_private.some_alt_metric.tag_0 ASC NULLS FIRST, greptime_private.some_alt_metric.timestamp ASC NULLS FIRST [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]\
            \n      Filter: greptime_private.some_alt_metric.timestamp >= TimestampMillisecond(-1000, None) AND greptime_private.some_alt_metric.timestamp <= TimestampMillisecond(100001000, None) [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]\
            \n        TableScan: greptime_private.some_alt_metric [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]"
        );

        indie_query_plan_compare(query, expected).await;

        let query = "some_alt_metric{__schema__=\"greptime_private\"} / some_metric";
        let expected = String::from("Projection: some_metric.tag_0, some_metric.timestamp, greptime_private.some_alt_metric.field_0 / some_metric.field_0 AS greptime_private.some_alt_metric.field_0 / some_metric.field_0 [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), greptime_private.some_alt_metric.field_0 / some_metric.field_0:Float64;N]\
        \n  Inner Join: greptime_private.some_alt_metric.tag_0 = some_metric.tag_0, greptime_private.some_alt_metric.timestamp = some_metric.timestamp [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N, tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]\
        \n    SubqueryAlias: greptime_private.some_alt_metric [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]\
        \n      PromInstantManipulate: range=[0..100000000], lookback=[1000], interval=[5000], time index=[timestamp] [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]\
        \n        PromSeriesDivide: tags=[\"tag_0\"] [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]\
        \n          Sort: greptime_private.some_alt_metric.tag_0 ASC NULLS FIRST, greptime_private.some_alt_metric.timestamp ASC NULLS FIRST [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]\
        \n            Filter: greptime_private.some_alt_metric.timestamp >= TimestampMillisecond(-1000, None) AND greptime_private.some_alt_metric.timestamp <= TimestampMillisecond(100001000, None) [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]\
        \n              TableScan: greptime_private.some_alt_metric [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]\
        \n    SubqueryAlias: some_metric [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]\
        \n      PromInstantManipulate: range=[0..100000000], lookback=[1000], interval=[5000], time index=[timestamp] [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]\
        \n        PromSeriesDivide: tags=[\"tag_0\"] [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]\
        \n          Sort: some_metric.tag_0 ASC NULLS FIRST, some_metric.timestamp ASC NULLS FIRST [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]\
        \n            Filter: some_metric.timestamp >= TimestampMillisecond(-1000, None) AND some_metric.timestamp <= TimestampMillisecond(100001000, None) [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]\
        \n              TableScan: some_metric [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]");

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
        assert!(catalog_list
            .register_table_sync(RegisterTableRequest {
                catalog: DEFAULT_CATALOG_NAME.to_string(),
                schema: DEFAULT_SCHEMA_NAME.to_string(),
                table_name: "metrics".to_string(),
                table_id: 1024,
                table,
            })
            .is_ok());

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
        assert_eq!(plan.display_indent_schema().to_string(),
        "PromInstantManipulate: range=[0..100000000], lookback=[1000], interval=[5000], time index=[timestamp] [field:Float64;N, tag:Utf8, timestamp:Timestamp(Millisecond, None)]\
        \n  PromSeriesDivide: tags=[\"tag\"] [field:Float64;N, tag:Utf8, timestamp:Timestamp(Millisecond, None)]\
        \n    Sort: metrics.tag ASC NULLS FIRST, metrics.timestamp ASC NULLS FIRST [field:Float64;N, tag:Utf8, timestamp:Timestamp(Millisecond, None)]\
        \n      Filter: metrics.tag = Utf8(\"1\") AND metrics.timestamp >= TimestampMillisecond(-1000, None) AND metrics.timestamp <= TimestampMillisecond(100001000, None) [field:Float64;N, tag:Utf8, timestamp:Timestamp(Millisecond, None)]\
        \n        Projection: metrics.field, metrics.tag, CAST(metrics.timestamp AS Timestamp(Millisecond, None)) AS timestamp [field:Float64;N, tag:Utf8, timestamp:Timestamp(Millisecond, None)]\
        \n          TableScan: metrics [tag:Utf8, timestamp:Timestamp(Nanosecond, None), field:Float64;N]"
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
        assert_eq!(plan.display_indent_schema().to_string(),
        "Filter: prom_avg_over_time(timestamp_range,field) IS NOT NULL [timestamp:Timestamp(Millisecond, None), prom_avg_over_time(timestamp_range,field):Float64;N, tag:Utf8]\
        \n  Projection: metrics.timestamp, prom_avg_over_time(timestamp_range, field) AS prom_avg_over_time(timestamp_range,field), metrics.tag [timestamp:Timestamp(Millisecond, None), prom_avg_over_time(timestamp_range,field):Float64;N, tag:Utf8]\
        \n    PromRangeManipulate: req range=[0..100000000], interval=[5000], eval range=[5000], time index=[timestamp], values=[\"field\"] [field:Dictionary(Int64, Float64);N, tag:Utf8, timestamp:Timestamp(Millisecond, None), timestamp_range:Dictionary(Int64, Timestamp(Millisecond, None))]\
        \n      PromSeriesNormalize: offset=[0], time index=[timestamp], filter NaN: [true] [field:Float64;N, tag:Utf8, timestamp:Timestamp(Millisecond, None)]\
        \n        PromSeriesDivide: tags=[\"tag\"] [field:Float64;N, tag:Utf8, timestamp:Timestamp(Millisecond, None)]\
        \n          Sort: metrics.tag ASC NULLS FIRST, metrics.timestamp ASC NULLS FIRST [field:Float64;N, tag:Utf8, timestamp:Timestamp(Millisecond, None)]\
        \n            Filter: metrics.tag = Utf8(\"1\") AND metrics.timestamp >= TimestampMillisecond(-6000, None) AND metrics.timestamp <= TimestampMillisecond(100001000, None) [field:Float64;N, tag:Utf8, timestamp:Timestamp(Millisecond, None)]\
        \n              Projection: metrics.field, metrics.tag, CAST(metrics.timestamp AS Timestamp(Millisecond, None)) AS timestamp [field:Float64;N, tag:Utf8, timestamp:Timestamp(Millisecond, None)]\
        \n                TableScan: metrics [tag:Utf8, timestamp:Timestamp(Nanosecond, None), field:Float64;N]"
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
Filter: up.field_0 IS NOT NULL [timestamp:Timestamp(Millisecond, None), field_0:Float64;N, foo:Utf8;N, tag_0:Utf8, tag_1:Utf8, tag_2:Utf8, tag_3:Utf8]
  Projection: up.timestamp, up.field_0, concat_ws(Utf8(","), up.tag_1, up.tag_2, up.tag_3) AS foo, up.tag_0, up.tag_1, up.tag_2, up.tag_3 [timestamp:Timestamp(Millisecond, None), field_0:Float64;N, foo:Utf8;N, tag_0:Utf8, tag_1:Utf8, tag_2:Utf8, tag_3:Utf8]
    PromInstantManipulate: range=[0..100000000], lookback=[1000], interval=[5000], time index=[timestamp] [tag_0:Utf8, tag_1:Utf8, tag_2:Utf8, tag_3:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]
      PromSeriesDivide: tags=["tag_0", "tag_1", "tag_2", "tag_3"] [tag_0:Utf8, tag_1:Utf8, tag_2:Utf8, tag_3:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]
        Sort: up.tag_0 ASC NULLS FIRST, up.tag_1 ASC NULLS FIRST, up.tag_2 ASC NULLS FIRST, up.tag_3 ASC NULLS FIRST, up.timestamp ASC NULLS FIRST [tag_0:Utf8, tag_1:Utf8, tag_2:Utf8, tag_3:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]
          Filter: up.tag_0 = Utf8("api-server") AND up.timestamp >= TimestampMillisecond(-1000, None) AND up.timestamp <= TimestampMillisecond(100001000, None) [tag_0:Utf8, tag_1:Utf8, tag_2:Utf8, tag_3:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]
            TableScan: up [tag_0:Utf8, tag_1:Utf8, tag_2:Utf8, tag_3:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]"#;

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
Filter: up.field_0 IS NOT NULL [timestamp:Timestamp(Millisecond, None), field_0:Float64;N, foo:Utf8;N, tag_0:Utf8]
  Projection: up.timestamp, up.field_0, regexp_replace(up.tag_0, Utf8("^(?s:(.*):.*)$"), Utf8("$1")) AS foo, up.tag_0 [timestamp:Timestamp(Millisecond, None), field_0:Float64;N, foo:Utf8;N, tag_0:Utf8]
    PromInstantManipulate: range=[0..100000000], lookback=[1000], interval=[5000], time index=[timestamp] [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]
      PromSeriesDivide: tags=["tag_0"] [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]
        Sort: up.tag_0 ASC NULLS FIRST, up.timestamp ASC NULLS FIRST [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]
          Filter: up.tag_0 = Utf8("a:c") AND up.timestamp >= TimestampMillisecond(-1000, None) AND up.timestamp <= TimestampMillisecond(100001000, None) [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]
            TableScan: up [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]"#;

        let ret = plan.display_indent_schema().to_string();
        assert_eq!(format!("\n{ret}"), expected, "\n{}", ret);
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
        let expected = "Sort: prometheus_tsdb_head_series.timestamp ASC NULLS LAST [timestamp:Timestamp(Millisecond, None), sum(prometheus_tsdb_head_series.field_0):Float64;N, sum(prometheus_tsdb_head_series.field_1):Float64;N, sum(prometheus_tsdb_head_series.field_2):Float64;N]\
        \n  Aggregate: groupBy=[[prometheus_tsdb_head_series.timestamp]], aggr=[[sum(prometheus_tsdb_head_series.field_0), sum(prometheus_tsdb_head_series.field_1), sum(prometheus_tsdb_head_series.field_2)]] [timestamp:Timestamp(Millisecond, None), sum(prometheus_tsdb_head_series.field_0):Float64;N, sum(prometheus_tsdb_head_series.field_1):Float64;N, sum(prometheus_tsdb_head_series.field_2):Float64;N]\
        \n    PromInstantManipulate: range=[0..100000000], lookback=[1000], interval=[5000], time index=[timestamp] [tag_0:Utf8, tag_1:Utf8, tag_2:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N, field_1:Float64;N, field_2:Float64;N]\
        \n      PromSeriesDivide: tags=[\"tag_0\", \"tag_1\", \"tag_2\"] [tag_0:Utf8, tag_1:Utf8, tag_2:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N, field_1:Float64;N, field_2:Float64;N]\
        \n        Sort: prometheus_tsdb_head_series.tag_0 ASC NULLS FIRST, prometheus_tsdb_head_series.tag_1 ASC NULLS FIRST, prometheus_tsdb_head_series.tag_2 ASC NULLS FIRST, prometheus_tsdb_head_series.timestamp ASC NULLS FIRST [tag_0:Utf8, tag_1:Utf8, tag_2:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N, field_1:Float64;N, field_2:Float64;N]\
        \n          Filter: prometheus_tsdb_head_series.tag_1 ~ Utf8(\"^(?:(10.0.160.237:8080|10.0.160.237:9090))$\") AND prometheus_tsdb_head_series.timestamp >= TimestampMillisecond(-1000, None) AND prometheus_tsdb_head_series.timestamp <= TimestampMillisecond(100001000, None) [tag_0:Utf8, tag_1:Utf8, tag_2:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N, field_1:Float64;N, field_2:Float64;N]\
        \n            TableScan: prometheus_tsdb_head_series [tag_0:Utf8, tag_1:Utf8, tag_2:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N, field_1:Float64;N, field_2:Float64;N]";
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
        let expected = "Projection: sum(prometheus_tsdb_head_series.greptime_value), prometheus_tsdb_head_series.ip, prometheus_tsdb_head_series.greptime_timestamp [sum(prometheus_tsdb_head_series.greptime_value):Float64;N, ip:Utf8, greptime_timestamp:Timestamp(Millisecond, None)]\
        \n  Sort: prometheus_tsdb_head_series.greptime_timestamp ASC NULLS LAST, row_number() PARTITION BY [prometheus_tsdb_head_series.greptime_timestamp] ORDER BY [sum(prometheus_tsdb_head_series.greptime_value) DESC NULLS FIRST, prometheus_tsdb_head_series.ip DESC NULLS FIRST] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ASC NULLS LAST [ip:Utf8, greptime_timestamp:Timestamp(Millisecond, None), sum(prometheus_tsdb_head_series.greptime_value):Float64;N, row_number() PARTITION BY [prometheus_tsdb_head_series.greptime_timestamp] ORDER BY [sum(prometheus_tsdb_head_series.greptime_value) DESC NULLS FIRST, prometheus_tsdb_head_series.ip DESC NULLS FIRST] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW:UInt64]\
        \n    Filter: row_number() PARTITION BY [prometheus_tsdb_head_series.greptime_timestamp] ORDER BY [sum(prometheus_tsdb_head_series.greptime_value) DESC NULLS FIRST, prometheus_tsdb_head_series.ip DESC NULLS FIRST] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW <= Float64(10) [ip:Utf8, greptime_timestamp:Timestamp(Millisecond, None), sum(prometheus_tsdb_head_series.greptime_value):Float64;N, row_number() PARTITION BY [prometheus_tsdb_head_series.greptime_timestamp] ORDER BY [sum(prometheus_tsdb_head_series.greptime_value) DESC NULLS FIRST, prometheus_tsdb_head_series.ip DESC NULLS FIRST] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW:UInt64]\
        \n      WindowAggr: windowExpr=[[row_number() PARTITION BY [prometheus_tsdb_head_series.greptime_timestamp] ORDER BY [sum(prometheus_tsdb_head_series.greptime_value) DESC NULLS FIRST, prometheus_tsdb_head_series.ip DESC NULLS FIRST] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW]] [ip:Utf8, greptime_timestamp:Timestamp(Millisecond, None), sum(prometheus_tsdb_head_series.greptime_value):Float64;N, row_number() PARTITION BY [prometheus_tsdb_head_series.greptime_timestamp] ORDER BY [sum(prometheus_tsdb_head_series.greptime_value) DESC NULLS FIRST, prometheus_tsdb_head_series.ip DESC NULLS FIRST] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW:UInt64]\
        \n        Sort: prometheus_tsdb_head_series.ip ASC NULLS LAST, prometheus_tsdb_head_series.greptime_timestamp ASC NULLS LAST [ip:Utf8, greptime_timestamp:Timestamp(Millisecond, None), sum(prometheus_tsdb_head_series.greptime_value):Float64;N]\
        \n          Aggregate: groupBy=[[prometheus_tsdb_head_series.ip, prometheus_tsdb_head_series.greptime_timestamp]], aggr=[[sum(prometheus_tsdb_head_series.greptime_value)]] [ip:Utf8, greptime_timestamp:Timestamp(Millisecond, None), sum(prometheus_tsdb_head_series.greptime_value):Float64;N]\
        \n            PromInstantManipulate: range=[0..100000000], lookback=[1000], interval=[5000], time index=[greptime_timestamp] [ip:Utf8, greptime_timestamp:Timestamp(Millisecond, None), greptime_value:Float64;N]\
        \n              PromSeriesDivide: tags=[\"ip\"] [ip:Utf8, greptime_timestamp:Timestamp(Millisecond, None), greptime_value:Float64;N]\
        \n                Sort: prometheus_tsdb_head_series.ip ASC NULLS FIRST, prometheus_tsdb_head_series.greptime_timestamp ASC NULLS FIRST [ip:Utf8, greptime_timestamp:Timestamp(Millisecond, None), greptime_value:Float64;N]\
        \n                  Filter: prometheus_tsdb_head_series.ip ~ Utf8(\"^(?:(10.0.160.237:8080|10.0.160.237:9090))$\") AND prometheus_tsdb_head_series.greptime_timestamp >= TimestampMillisecond(-1000, None) AND prometheus_tsdb_head_series.greptime_timestamp <= TimestampMillisecond(100001000, None) [ip:Utf8, greptime_timestamp:Timestamp(Millisecond, None), greptime_value:Float64;N]\
        \n                    TableScan: prometheus_tsdb_head_series [ip:Utf8, greptime_timestamp:Timestamp(Millisecond, None), greptime_value:Float64;N]";

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
        let expected = "Projection: count(prometheus_tsdb_head_series.greptime_value), prometheus_tsdb_head_series.ip, prometheus_tsdb_head_series.greptime_timestamp, series [count(prometheus_tsdb_head_series.greptime_value):Int64, ip:Utf8, greptime_timestamp:Timestamp(Millisecond, None), series:Float64;N]\
        \n  Sort: prometheus_tsdb_head_series.ip ASC NULLS LAST, prometheus_tsdb_head_series.greptime_timestamp ASC NULLS LAST, prometheus_tsdb_head_series.greptime_value ASC NULLS LAST [count(prometheus_tsdb_head_series.greptime_value):Int64, ip:Utf8, greptime_timestamp:Timestamp(Millisecond, None), series:Float64;N, greptime_value:Float64;N]\
        \n    Projection: count(prometheus_tsdb_head_series.greptime_value), prometheus_tsdb_head_series.ip, prometheus_tsdb_head_series.greptime_timestamp, prometheus_tsdb_head_series.greptime_value AS series, prometheus_tsdb_head_series.greptime_value [count(prometheus_tsdb_head_series.greptime_value):Int64, ip:Utf8, greptime_timestamp:Timestamp(Millisecond, None), series:Float64;N, greptime_value:Float64;N]\
        \n      Aggregate: groupBy=[[prometheus_tsdb_head_series.ip, prometheus_tsdb_head_series.greptime_timestamp, prometheus_tsdb_head_series.greptime_value]], aggr=[[count(prometheus_tsdb_head_series.greptime_value)]] [ip:Utf8, greptime_timestamp:Timestamp(Millisecond, None), greptime_value:Float64;N, count(prometheus_tsdb_head_series.greptime_value):Int64]\
        \n        PromInstantManipulate: range=[0..100000000], lookback=[1000], interval=[5000], time index=[greptime_timestamp] [ip:Utf8, greptime_timestamp:Timestamp(Millisecond, None), greptime_value:Float64;N]\
        \n          PromSeriesDivide: tags=[\"ip\"] [ip:Utf8, greptime_timestamp:Timestamp(Millisecond, None), greptime_value:Float64;N]\
        \n            Sort: prometheus_tsdb_head_series.ip ASC NULLS FIRST, prometheus_tsdb_head_series.greptime_timestamp ASC NULLS FIRST [ip:Utf8, greptime_timestamp:Timestamp(Millisecond, None), greptime_value:Float64;N]\
        \n              Filter: prometheus_tsdb_head_series.ip ~ Utf8(\"^(?:(10.0.160.237:8080|10.0.160.237:9090))$\") AND prometheus_tsdb_head_series.greptime_timestamp >= TimestampMillisecond(-1000, None) AND prometheus_tsdb_head_series.greptime_timestamp <= TimestampMillisecond(100001000, None) [ip:Utf8, greptime_timestamp:Timestamp(Millisecond, None), greptime_value:Float64;N]\
        \n                TableScan: prometheus_tsdb_head_series [ip:Utf8, greptime_timestamp:Timestamp(Millisecond, None), greptime_value:Float64;N]";

        assert_eq!(plan.display_indent_schema().to_string(), expected);
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
        let expected = "Sort: prometheus_tsdb_head_series.greptime_timestamp ASC NULLS LAST [greptime_timestamp:Timestamp(Millisecond, None), quantile(Float64(0.3),sum(prometheus_tsdb_head_series.greptime_value)):Float64;N]\
        \n  Aggregate: groupBy=[[prometheus_tsdb_head_series.greptime_timestamp]], aggr=[[quantile(Float64(0.3), sum(prometheus_tsdb_head_series.greptime_value))]] [greptime_timestamp:Timestamp(Millisecond, None), quantile(Float64(0.3),sum(prometheus_tsdb_head_series.greptime_value)):Float64;N]\
        \n    Sort: prometheus_tsdb_head_series.ip ASC NULLS LAST, prometheus_tsdb_head_series.greptime_timestamp ASC NULLS LAST [ip:Utf8, greptime_timestamp:Timestamp(Millisecond, None), sum(prometheus_tsdb_head_series.greptime_value):Float64;N]\
        \n      Aggregate: groupBy=[[prometheus_tsdb_head_series.ip, prometheus_tsdb_head_series.greptime_timestamp]], aggr=[[sum(prometheus_tsdb_head_series.greptime_value)]] [ip:Utf8, greptime_timestamp:Timestamp(Millisecond, None), sum(prometheus_tsdb_head_series.greptime_value):Float64;N]\
        \n        PromInstantManipulate: range=[0..100000000], lookback=[1000], interval=[5000], time index=[greptime_timestamp] [ip:Utf8, greptime_timestamp:Timestamp(Millisecond, None), greptime_value:Float64;N]\
        \n          PromSeriesDivide: tags=[\"ip\"] [ip:Utf8, greptime_timestamp:Timestamp(Millisecond, None), greptime_value:Float64;N]\
        \n            Sort: prometheus_tsdb_head_series.ip ASC NULLS FIRST, prometheus_tsdb_head_series.greptime_timestamp ASC NULLS FIRST [ip:Utf8, greptime_timestamp:Timestamp(Millisecond, None), greptime_value:Float64;N]\
        \n              Filter: prometheus_tsdb_head_series.ip ~ Utf8(\"^(?:(10.0.160.237:8080|10.0.160.237:9090))$\") AND prometheus_tsdb_head_series.greptime_timestamp >= TimestampMillisecond(-1000, None) AND prometheus_tsdb_head_series.greptime_timestamp <= TimestampMillisecond(100001000, None) [ip:Utf8, greptime_timestamp:Timestamp(Millisecond, None), greptime_value:Float64;N]\
        \n                TableScan: prometheus_tsdb_head_series [ip:Utf8, greptime_timestamp:Timestamp(Millisecond, None), greptime_value:Float64;N]";

        assert_eq!(plan.display_indent_schema().to_string(), expected);
    }

    #[tokio::test]
    async fn test_or_not_exists_table_label() {
        let mut eval_stmt = EvalStmt {
            expr: PromExpr::NumberLiteral(NumberLiteral { val: 1.0 }),
            start: UNIX_EPOCH,
            end: UNIX_EPOCH
                .checked_add(Duration::from_secs(100_000))
                .unwrap(),
            interval: Duration::from_secs(5),
            lookback_delta: Duration::from_secs(1),
        };
        let case = r#"sum by (job, tag0, tag2) (metric_exists) or sum by (job, tag0, tag2) (metric_not_exists)"#;

        let prom_expr = parser::parse(case).unwrap();
        eval_stmt.expr = prom_expr;
        let table_provider = build_test_table_provider_with_fields(
            &[(DEFAULT_SCHEMA_NAME.to_string(), "metric_exists".to_string())],
            &["job"],
        )
        .await;

        let plan =
            PromPlanner::stmt_to_plan(table_provider, &eval_stmt, &build_query_engine_state())
                .await
                .unwrap();
        let expected = "UnionDistinctOn: on col=[[\"job\"]], ts_col=[greptime_timestamp] [greptime_timestamp:Timestamp(Millisecond, None), job:Utf8, sum(metric_exists.greptime_value):Float64;N]\
        \n  SubqueryAlias: metric_exists [greptime_timestamp:Timestamp(Millisecond, None), job:Utf8, sum(metric_exists.greptime_value):Float64;N]\
        \n    Projection: metric_exists.greptime_timestamp, metric_exists.job, sum(metric_exists.greptime_value) [greptime_timestamp:Timestamp(Millisecond, None), job:Utf8, sum(metric_exists.greptime_value):Float64;N]\
        \n      Sort: metric_exists.job ASC NULLS LAST, metric_exists.greptime_timestamp ASC NULLS LAST [job:Utf8, greptime_timestamp:Timestamp(Millisecond, None), sum(metric_exists.greptime_value):Float64;N]\
        \n        Aggregate: groupBy=[[metric_exists.job, metric_exists.greptime_timestamp]], aggr=[[sum(metric_exists.greptime_value)]] [job:Utf8, greptime_timestamp:Timestamp(Millisecond, None), sum(metric_exists.greptime_value):Float64;N]\
        \n          PromInstantManipulate: range=[0..100000000], lookback=[1000], interval=[5000], time index=[greptime_timestamp] [job:Utf8, greptime_timestamp:Timestamp(Millisecond, None), greptime_value:Float64;N]\
        \n            PromSeriesDivide: tags=[\"job\"] [job:Utf8, greptime_timestamp:Timestamp(Millisecond, None), greptime_value:Float64;N]\
        \n              Sort: metric_exists.job ASC NULLS FIRST, metric_exists.greptime_timestamp ASC NULLS FIRST [job:Utf8, greptime_timestamp:Timestamp(Millisecond, None), greptime_value:Float64;N]\
        \n                Filter: metric_exists.greptime_timestamp >= TimestampMillisecond(-1000, None) AND metric_exists.greptime_timestamp <= TimestampMillisecond(100001000, None) [job:Utf8, greptime_timestamp:Timestamp(Millisecond, None), greptime_value:Float64;N]\
        \n                  TableScan: metric_exists [job:Utf8, greptime_timestamp:Timestamp(Millisecond, None), greptime_value:Float64;N]\
        \n  SubqueryAlias:  [greptime_timestamp:Timestamp(Millisecond, None), job:Utf8;N, sum(.value):Float64;N]\
        \n    Projection: .time AS greptime_timestamp, Utf8(NULL) AS job, sum(.value) [greptime_timestamp:Timestamp(Millisecond, None), job:Utf8;N, sum(.value):Float64;N]\
        \n      Sort: .time ASC NULLS LAST [time:Timestamp(Millisecond, None), sum(.value):Float64;N]\
        \n        Aggregate: groupBy=[[.time]], aggr=[[sum(.value)]] [time:Timestamp(Millisecond, None), sum(.value):Float64;N]\
        \n          EmptyMetric: range=[0..-1], interval=[5000] [time:Timestamp(Millisecond, None), value:Float64;N]";

        assert_eq!(plan.display_indent_schema().to_string(), expected);
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
}
