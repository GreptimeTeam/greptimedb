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
use std::str::FromStr;
use std::sync::Arc;
use std::time::UNIX_EPOCH;

use async_recursion::async_recursion;
use catalog::table_source::DfTableSourceProvider;
use datafusion::common::{DFSchemaRef, OwnedTableReference, Result as DfResult};
use datafusion::datasource::DefaultTableSource;
use datafusion::logical_expr::expr::{AggregateFunction, Alias, ScalarFunction, ScalarUDF};
use datafusion::logical_expr::expr_rewriter::normalize_cols;
use datafusion::logical_expr::{
    AggregateFunction as AggregateFunctionEnum, BinaryExpr, BuiltinScalarFunction, Cast, Extension,
    LogicalPlan, LogicalPlanBuilder, Operator, ScalarUDF as ScalarUdfDef,
};
use datafusion::optimizer::utils;
use datafusion::prelude as df_prelude;
use datafusion::prelude::{Column, Expr as DfExpr, JoinType};
use datafusion::scalar::ScalarValue;
use datafusion::sql::TableReference;
use datatypes::arrow::datatypes::DataType as ArrowDataType;
use promql_parser::label::{MatchOp, Matcher, Matchers, METRIC_NAME};
use promql_parser::parser::{
    token, AggregateExpr, BinModifier, BinaryExpr as PromBinaryExpr, Call, EvalStmt,
    Expr as PromExpr, Function, LabelModifier, MatrixSelector, NumberLiteral, Offset, ParenExpr,
    StringLiteral, SubqueryExpr, TokenType, UnaryExpr, VectorMatchCardinality, VectorSelector,
};
use snafu::{ensure, OptionExt, ResultExt};
use table::table::adapter::DfTableProviderAdapter;

use crate::error::{
    CatalogSnafu, ColumnNotFoundSnafu, CombineTableColumnMismatchSnafu, DataFusionPlanningSnafu,
    ExpectRangeSelectorSnafu, FunctionInvalidArgumentSnafu, MultipleMetricMatchersSnafu,
    MultipleVectorSnafu, NoMetricMatcherSnafu, Result, TableNameNotFoundSnafu,
    TimeIndexNotFoundSnafu, UnexpectedPlanExprSnafu, UnexpectedTokenSnafu, UnknownTableSnafu,
    UnsupportedExprSnafu, UnsupportedVectorMatchSnafu, ValueNotFoundSnafu, ZeroRangeSelectorSnafu,
};
use crate::extension_plan::{
    build_special_time_expr, EmptyMetric, HistogramFold, InstantManipulate, Millisecond,
    RangeManipulate, SeriesDivide, SeriesNormalize, UnionDistinctOn,
};
use crate::functions::{
    AbsentOverTime, AvgOverTime, Changes, CountOverTime, Delta, Deriv, HoltWinters, IDelta,
    Increase, LastOverTime, MaxOverTime, MinOverTime, PredictLinear, PresentOverTime,
    QuantileOverTime, Rate, Resets, StddevOverTime, StdvarOverTime, SumOverTime,
};

/// `time()` function in PromQL.
const SPECIAL_TIME_FUNCTION: &str = "time";
/// `histogram_quantile` function in PromQL
const SPECIAL_HISTOGRAM_QUANTILE: &str = "histogram_quantile";
/// `le` column for conventional histogram.
const LE_COLUMN_NAME: &str = "le";

const DEFAULT_TIME_INDEX_COLUMN: &str = "time";

/// default value column name for empty metric
const DEFAULT_FIELD_COLUMN: &str = "value";

/// Special modifier to project field columns under multi-field mode
const FIELD_COLUMN_MATCHER: &str = "__field__";

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
    field_column_matcher: Option<Vec<Matcher>>,
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
        self.range = None;
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

impl PromPlanner {
    pub async fn stmt_to_plan(
        table_provider: DfTableSourceProvider,
        stmt: EvalStmt,
    ) -> Result<LogicalPlan> {
        let mut planner = Self {
            table_provider,
            ctx: PromPlannerContext::from_eval_stmt(&stmt),
        };

        planner.prom_expr_to_plan(stmt.expr).await
    }

    #[async_recursion]
    pub async fn prom_expr_to_plan(&mut self, prom_expr: PromExpr) -> Result<LogicalPlan> {
        let res = match &prom_expr {
            PromExpr::Aggregate(AggregateExpr {
                op,
                expr,
                // TODO(ruihang): support param
                param: _param,
                modifier,
            }) => {
                let input = self.prom_expr_to_plan(*expr.clone()).await?;

                // calculate columns to group by
                // Need to append time index column into group by columns
                let group_exprs = self.agg_modifier_to_col(input.schema(), modifier)?;

                // convert op and value columns to aggregate exprs
                let aggr_exprs = self.create_aggregate_exprs(*op, &input)?;

                // create plan
                let group_sort_expr = group_exprs
                    .clone()
                    .into_iter()
                    .map(|expr| expr.sort(true, false));
                LogicalPlanBuilder::from(input)
                    .aggregate(group_exprs, aggr_exprs)
                    .context(DataFusionPlanningSnafu)?
                    .sort(group_sort_expr)
                    .context(DataFusionPlanningSnafu)?
                    .build()
                    .context(DataFusionPlanningSnafu)?
            }
            PromExpr::Unary(UnaryExpr { expr }) => {
                // Unary Expr in PromQL implys the `-` operator
                let input = self.prom_expr_to_plan(*expr.clone()).await?;
                self.projection_for_each_field_column(input, |col| {
                    Ok(DfExpr::Negative(Box::new(DfExpr::Column(col.into()))))
                })?
            }
            PromExpr::Binary(PromBinaryExpr {
                lhs,
                rhs,
                op,
                modifier,
            }) => {
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
                        self.ctx.table_name = Some(String::new());
                        let field_expr_builder = Self::prom_token_to_binary_expr_builder(*op)?;
                        let mut field_expr = field_expr_builder(lhs, rhs)?;

                        if is_comparison_op && should_return_bool {
                            field_expr = DfExpr::Cast(Cast {
                                expr: Box::new(field_expr),
                                data_type: ArrowDataType::Float64,
                            });
                        }

                        LogicalPlan::Extension(Extension {
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
                        })
                    }
                    // lhs is a literal, rhs is a column
                    (Some(mut expr), None) => {
                        let input = self.prom_expr_to_plan(*rhs.clone()).await?;
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
                            self.filter_on_field_column(input, bin_expr_builder)?
                        } else {
                            self.projection_for_each_field_column(input, bin_expr_builder)?
                        }
                    }
                    // lhs is a column, rhs is a literal
                    (None, Some(mut expr)) => {
                        let input = self.prom_expr_to_plan(*lhs.clone()).await?;
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
                            self.filter_on_field_column(input, bin_expr_builder)?
                        } else {
                            self.projection_for_each_field_column(input, bin_expr_builder)?
                        }
                    }
                    // both are columns. join them on time index
                    (None, None) => {
                        let left_input = self.prom_expr_to_plan(*lhs.clone()).await?;
                        let left_field_columns = self.ctx.field_columns.clone();
                        let left_table_ref: OwnedTableReference =
                            self.ctx.table_name.clone().unwrap_or_default().into();
                        let left_tag_cols = self.ctx.tag_columns.clone();

                        let right_input = self.prom_expr_to_plan(*rhs.clone()).await?;
                        let right_field_columns = self.ctx.field_columns.clone();
                        let right_table_ref: OwnedTableReference =
                            self.ctx.table_name.clone().unwrap_or_default().into();
                        let right_tag_cols = self.ctx.tag_columns.clone();

                        // TODO(ruihang): avoid join if left and right are the same table

                        // set op has "special" join semantics
                        if Self::is_token_a_set_op(*op) {
                            return self.set_op_on_non_field_columns(
                                left_input,
                                right_input,
                                left_tag_cols,
                                right_tag_cols,
                                *op,
                                modifier,
                            );
                        }

                        // normal join
                        let mut field_columns =
                            left_field_columns.iter().zip(right_field_columns.iter());
                        let join_plan = self.join_on_non_field_columns(
                            left_input,
                            right_input,
                            left_table_ref.clone(),
                            right_table_ref.clone(),
                        )?;
                        let join_plan_schema = join_plan.schema().clone();

                        let bin_expr_builder = |_: &String| {
                            let (left_col_name, right_col_name) = field_columns.next().unwrap();
                            let left_col = join_plan_schema
                                .field_with_name(Some(&left_table_ref), left_col_name)
                                .context(DataFusionPlanningSnafu)?
                                .qualified_column();
                            let right_col = join_plan_schema
                                .field_with_name(Some(&right_table_ref), right_col_name)
                                .context(DataFusionPlanningSnafu)?
                                .qualified_column();

                            let binary_expr_builder = Self::prom_token_to_binary_expr_builder(*op)?;
                            let mut binary_expr = binary_expr_builder(
                                DfExpr::Column(left_col),
                                DfExpr::Column(right_col),
                            )?;
                            if is_comparison_op && should_return_bool {
                                binary_expr = DfExpr::Cast(Cast {
                                    expr: Box::new(binary_expr),
                                    data_type: ArrowDataType::Float64,
                                });
                            }
                            Ok(binary_expr)
                        };
                        if is_comparison_op && !should_return_bool {
                            self.filter_on_field_column(join_plan, bin_expr_builder)?
                        } else {
                            self.projection_for_each_field_column(join_plan, bin_expr_builder)?
                        }
                    }
                }
            }
            PromExpr::Paren(ParenExpr { expr }) => self.prom_expr_to_plan(*expr.clone()).await?,
            PromExpr::Subquery(SubqueryExpr { .. }) => UnsupportedExprSnafu {
                name: "Prom Subquery",
            }
            .fail()?,
            PromExpr::NumberLiteral(NumberLiteral { val }) => {
                self.ctx.time_index_column = Some(DEFAULT_TIME_INDEX_COLUMN.to_string());
                self.ctx.field_columns = vec![DEFAULT_FIELD_COLUMN.to_string()];
                self.ctx.table_name = Some(String::new());
                let literal_expr = df_prelude::lit(*val);

                LogicalPlan::Extension(Extension {
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
                })
            }
            PromExpr::StringLiteral(StringLiteral { val }) => {
                self.ctx.time_index_column = Some(DEFAULT_TIME_INDEX_COLUMN.to_string());
                self.ctx.field_columns = vec![DEFAULT_FIELD_COLUMN.to_string()];
                self.ctx.table_name = Some(String::new());
                let literal_expr = df_prelude::lit(val.to_string());

                LogicalPlan::Extension(Extension {
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
                })
            }
            PromExpr::VectorSelector(VectorSelector {
                name,
                offset,
                matchers,
                at: _,
            }) => {
                let matchers = self.preprocess_label_matchers(matchers, name)?;
                self.setup_context().await?;
                let normalize = self
                    .selector_to_series_normalize_plan(offset, matchers, false)
                    .await?;
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
                LogicalPlan::Extension(Extension {
                    node: Arc::new(manipulate),
                })
            }
            PromExpr::MatrixSelector(MatrixSelector { vs, range }) => {
                let VectorSelector {
                    name,
                    offset,
                    matchers,
                    ..
                } = vs;
                let matchers = self.preprocess_label_matchers(matchers, name)?;
                self.setup_context().await?;

                ensure!(!range.is_zero(), ZeroRangeSelectorSnafu);
                let range_ms = range.as_millis() as _;
                self.ctx.range = Some(range_ms);

                let normalize = self
                    .selector_to_series_normalize_plan(offset, matchers, true)
                    .await?;
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

                LogicalPlan::Extension(Extension {
                    node: Arc::new(manipulate),
                })
            }
            PromExpr::Call(Call { func, args }) => {
                if func.name == SPECIAL_HISTOGRAM_QUANTILE {
                    if args.args.len() != 2 {
                        return FunctionInvalidArgumentSnafu {
                            fn_name: SPECIAL_HISTOGRAM_QUANTILE.to_string(),
                        }
                        .fail();
                    }
                    let phi = Self::try_build_float_literal(&args.args[0]).with_context(|| {
                        FunctionInvalidArgumentSnafu {
                            fn_name: SPECIAL_HISTOGRAM_QUANTILE.to_string(),
                        }
                    })?;
                    let input = args.args[1].as_ref().clone();
                    let input_plan = self.prom_expr_to_plan(input).await?;

                    if !self.ctx.has_le_tag() {
                        return ColumnNotFoundSnafu {
                            col: LE_COLUMN_NAME.to_string(),
                        }
                        .fail();
                    }
                    let time_index_column =
                        self.ctx.time_index_column.clone().with_context(|| {
                            TimeIndexNotFoundSnafu {
                                table: self.ctx.table_name.clone().unwrap_or_default(),
                            }
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

                    return Ok(LogicalPlan::Extension(Extension {
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
                    }));
                }

                let args = self.create_function_args(&args.args)?;
                let input = if let Some(prom_expr) = args.input {
                    self.prom_expr_to_plan(prom_expr).await?
                } else {
                    self.ctx.time_index_column = Some(SPECIAL_TIME_FUNCTION.to_string());
                    self.ctx.table_name = Some(String::new());
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
                let mut func_exprs = self.create_function_expr(func, args.literals)?;
                func_exprs.insert(0, self.create_time_index_column_expr()?);
                func_exprs.extend_from_slice(&self.create_tag_column_exprs()?);

                LogicalPlanBuilder::from(input)
                    .project(func_exprs)
                    .context(DataFusionPlanningSnafu)?
                    .filter(self.create_empty_values_filter_expr()?)
                    .context(DataFusionPlanningSnafu)?
                    .build()
                    .context(DataFusionPlanningSnafu)?
            }
            PromExpr::Extension(promql_parser::parser::ast::Extension { expr }) => {
                let children = expr.children();
                let plan = self.prom_expr_to_plan(children[0].clone()).await?;
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
                        .context(DataFusionPlanningSnafu)?,
                    "EXPLAIN" => LogicalPlanBuilder::from(plan)
                        .explain(false, false)
                        .unwrap()
                        .build()
                        .context(DataFusionPlanningSnafu)?,
                    _ => LogicalPlanBuilder::empty(true)
                        .build()
                        .context(DataFusionPlanningSnafu)?,
                }
            }
        };
        Ok(res)
    }

    /// Extract metric name from `__name__` matcher and set it into [PromPlannerContext].
    /// Returns a new [Matchers] that doesn't contains metric name matcher.
    ///
    /// Each call to this function means new selector is started. Thus the context will be reset
    /// at first.
    ///
    /// Name rule:
    /// - if `name` is some, then the matchers MUST NOT contains `__name__` matcher.
    /// - if `name` is none, then the matchers MAY contains NONE OR MULTIPLE `__name__` matchers.
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
                label_matchers.find_matcher(METRIC_NAME).is_none(),
                MultipleMetricMatchersSnafu
            );
        } else {
            metric_name = Some(
                label_matchers
                    .find_matcher(METRIC_NAME)
                    .context(NoMetricMatcherSnafu)?,
            );
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
            } else {
                let _ = matchers.insert(matcher.clone());
            }
        }
        let matchers = matchers.into_iter().collect();
        Ok(Matchers { matchers })
    }

    async fn selector_to_series_normalize_plan(
        &mut self,
        offset: &Option<Offset>,
        label_matchers: Matchers,
        is_range_selector: bool,
    ) -> Result<LogicalPlan> {
        let table_name = self.ctx.table_name.clone().unwrap();

        // make filter exprs
        let offset_duration = match offset {
            Some(Offset::Pos(duration)) => duration.as_millis() as Millisecond,
            Some(Offset::Neg(duration)) => -(duration.as_millis() as Millisecond),
            None => 0,
        };
        let range_ms = self.ctx.range.unwrap_or_default();
        let mut scan_filters = self.matchers_to_expr(label_matchers.clone())?;
        scan_filters.push(self.create_time_index_column_expr()?.gt_eq(DfExpr::Literal(
            ScalarValue::TimestampMillisecond(
                Some(self.ctx.start - offset_duration - self.ctx.lookback_delta - range_ms),
                None,
            ),
        )));
        scan_filters.push(self.create_time_index_column_expr()?.lt_eq(DfExpr::Literal(
            ScalarValue::TimestampMillisecond(
                Some(self.ctx.end - offset_duration + self.ctx.lookback_delta),
                None,
            ),
        )));

        // make table scan with filter exprs
        let mut table_scan = self
            .create_table_scan_plan(&table_name, scan_filters.clone())
            .await?;

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
                .map(|col| DfExpr::Column(col.into()))
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

        // make filter and sort plan
        let mut plan_builder = LogicalPlanBuilder::from(table_scan);
        let accurate_filters = self.matchers_to_expr(label_matchers)?;
        if !accurate_filters.is_empty() {
            plan_builder = plan_builder
                .filter(utils::conjunction(accurate_filters).unwrap())
                .context(DataFusionPlanningSnafu)?;
        }
        let sort_plan = plan_builder
            .sort(self.create_tag_and_time_index_column_sort_exprs()?)
            .context(DataFusionPlanningSnafu)?
            .build()
            .context(DataFusionPlanningSnafu)?;

        // make divide plan
        let divide_plan = LogicalPlan::Extension(Extension {
            node: Arc::new(SeriesDivide::new(self.ctx.tag_columns.clone(), sort_plan)),
        });

        // make series_normalize plan
        let series_normalize = SeriesNormalize::new(
            offset_duration,
            self.ctx
                .time_index_column
                .clone()
                .with_context(|| TimeIndexNotFoundSnafu { table: table_name })?,
            is_range_selector,
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
    /// This method will also change the tag columns in ctx.
    fn agg_modifier_to_col(
        &mut self,
        input_schema: &DFSchemaRef,
        modifier: &Option<LabelModifier>,
    ) -> Result<Vec<DfExpr>> {
        match modifier {
            None => {
                self.ctx.tag_columns = vec![];
                Ok(vec![self.create_time_index_column_expr()?])
            }
            Some(LabelModifier::Include(labels)) => {
                let mut exprs = Vec::with_capacity(labels.labels.len());
                for label in &labels.labels {
                    // nonexistence label will be ignored
                    if let Ok(field) = input_schema.field_with_unqualified_name(label) {
                        exprs.push(DfExpr::Column(Column::from(field.name())));
                    }
                }

                // change the tag columns in context
                self.ctx.tag_columns = labels.labels.clone();

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

                // change the tag columns in context
                self.ctx.tag_columns = all_fields.iter().map(|col| (*col).clone()).collect();

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
    fn matchers_to_expr(&self, label_matchers: Matchers) -> Result<Vec<DfExpr>> {
        let mut exprs = Vec::with_capacity(label_matchers.matchers.len());
        for matcher in label_matchers.matchers {
            let col = DfExpr::Column(Column::from_name(matcher.name));
            let lit = DfExpr::Literal(ScalarValue::Utf8(Some(matcher.value)));
            let expr = match matcher.op {
                MatchOp::Equal => col.eq(lit),
                MatchOp::NotEqual => col.not_eq(lit),
                MatchOp::Re(_) => DfExpr::BinaryExpr(BinaryExpr {
                    left: Box::new(col),
                    op: Operator::RegexMatch,
                    right: Box::new(lit),
                }),
                MatchOp::NotRe(_) => DfExpr::BinaryExpr(BinaryExpr {
                    left: Box::new(col),
                    op: Operator::RegexNotMatch,
                    right: Box::new(lit),
                }),
            };
            exprs.push(expr);
        }

        Ok(exprs)
    }

    async fn create_table_scan_plan(
        &mut self,
        table_name: &str,
        filter: Vec<DfExpr>,
    ) -> Result<LogicalPlan> {
        let table_ref = OwnedTableReference::bare(table_name.to_string());
        let provider = self
            .table_provider
            .resolve_table(table_ref.clone())
            .await
            .context(CatalogSnafu)?;
        let result = LogicalPlanBuilder::scan_with_filters(table_ref, provider, None, filter)
            .context(DataFusionPlanningSnafu)?
            .build()
            .context(DataFusionPlanningSnafu)?;
        Ok(result)
    }

    /// Setup [PromPlannerContext]'s state fields.
    async fn setup_context(&mut self) -> Result<()> {
        let table_name = self
            .ctx
            .table_name
            .clone()
            .context(TableNameNotFoundSnafu)?;
        let table = self
            .table_provider
            .resolve_table(TableReference::bare(&table_name))
            .await
            .context(CatalogSnafu)?
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
            .with_context(|| TimeIndexNotFoundSnafu { table: table_name })?
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
            .cloned()
            .collect();
        self.ctx.tag_columns = tags;

        Ok(())
    }

    // TODO(ruihang): insert column expr
    fn create_function_args(&self, args: &[Box<PromExpr>]) -> Result<FunctionArgs> {
        let mut result = FunctionArgs::default();

        for arg in args {
            match *arg.clone() {
                PromExpr::Aggregate(_)
                | PromExpr::Unary(_)
                | PromExpr::Binary(_)
                | PromExpr::Paren(_)
                | PromExpr::Subquery(_)
                | PromExpr::VectorSelector(_)
                | PromExpr::MatrixSelector(_)
                | PromExpr::Extension(_)
                | PromExpr::Call(_) => {
                    if result.input.replace(*arg.clone()).is_some() {
                        MultipleVectorSnafu { expr: *arg.clone() }.fail()?;
                    }
                }

                PromExpr::NumberLiteral(NumberLiteral { val, .. }) => {
                    let scalar_value = ScalarValue::Float64(Some(val));
                    result.literals.push(DfExpr::Literal(scalar_value));
                }
                PromExpr::StringLiteral(StringLiteral { val, .. }) => {
                    let scalar_value = ScalarValue::Utf8(Some(val));
                    result.literals.push(DfExpr::Literal(scalar_value));
                }
            }
        }

        Ok(result)
    }

    /// # Side Effects
    ///
    /// This method will update [PromPlannerContext]'s value fields.
    fn create_function_expr(
        &mut self,
        func: &Function,
        other_input_exprs: Vec<DfExpr>,
    ) -> Result<Vec<DfExpr>> {
        // TODO(ruihang): check function args list
        let mut other_input_exprs: VecDeque<DfExpr> = other_input_exprs.into();

        // TODO(ruihang): set this according to in-param list
        let field_column_pos = 0;
        let mut exprs = Vec::with_capacity(self.ctx.field_columns.len());
        let scalar_func = match func.name {
            "increase" => ScalarFunc::ExtrapolateUdf(Increase::scalar_udf(
                self.ctx.range.context(ExpectRangeSelectorSnafu)?,
            )),
            "rate" => ScalarFunc::ExtrapolateUdf(Rate::scalar_udf(
                self.ctx.range.context(ExpectRangeSelectorSnafu)?,
            )),
            "delta" => ScalarFunc::ExtrapolateUdf(Delta::scalar_udf(
                self.ctx.range.context(ExpectRangeSelectorSnafu)?,
            )),
            "idelta" => ScalarFunc::Udf(IDelta::<false>::scalar_udf()),
            "irate" => ScalarFunc::Udf(IDelta::<true>::scalar_udf()),
            "resets" => ScalarFunc::Udf(Resets::scalar_udf()),
            "changes" => ScalarFunc::Udf(Changes::scalar_udf()),
            "deriv" => ScalarFunc::Udf(Deriv::scalar_udf()),
            "avg_over_time" => ScalarFunc::Udf(AvgOverTime::scalar_udf()),
            "min_over_time" => ScalarFunc::Udf(MinOverTime::scalar_udf()),
            "max_over_time" => ScalarFunc::Udf(MaxOverTime::scalar_udf()),
            "sum_over_time" => ScalarFunc::Udf(SumOverTime::scalar_udf()),
            "count_over_time" => ScalarFunc::Udf(CountOverTime::scalar_udf()),
            "last_over_time" => ScalarFunc::Udf(LastOverTime::scalar_udf()),
            "absent_over_time" => ScalarFunc::Udf(AbsentOverTime::scalar_udf()),
            "present_over_time" => ScalarFunc::Udf(PresentOverTime::scalar_udf()),
            "stddev_over_time" => ScalarFunc::Udf(StddevOverTime::scalar_udf()),
            "stdvar_over_time" => ScalarFunc::Udf(StdvarOverTime::scalar_udf()),
            "quantile_over_time" => {
                let quantile_expr = match other_input_exprs.pop_front() {
                    Some(DfExpr::Literal(ScalarValue::Float64(Some(quantile)))) => quantile,
                    other => UnexpectedPlanExprSnafu {
                        desc: format!("expect f64 literal as quantile, but found {:?}", other),
                    }
                    .fail()?,
                };
                ScalarFunc::Udf(QuantileOverTime::scalar_udf(quantile_expr))
            }
            "predict_linear" => {
                let t_expr = match other_input_exprs.pop_front() {
                    Some(DfExpr::Literal(ScalarValue::Float64(Some(t)))) => t as i64,
                    Some(DfExpr::Literal(ScalarValue::Int64(Some(t)))) => t,
                    other => UnexpectedPlanExprSnafu {
                        desc: format!("expect i64 literal as t, but found {:?}", other),
                    }
                    .fail()?,
                };
                ScalarFunc::Udf(PredictLinear::scalar_udf(t_expr))
            }
            "holt_winters" => {
                let sf_exp = match other_input_exprs.pop_front() {
                    Some(DfExpr::Literal(ScalarValue::Float64(Some(sf)))) => sf,
                    other => UnexpectedPlanExprSnafu {
                        desc: format!(
                            "expect f64 literal as smoothing factor, but found {:?}",
                            other
                        ),
                    }
                    .fail()?,
                };
                let tf_exp = match other_input_exprs.pop_front() {
                    Some(DfExpr::Literal(ScalarValue::Float64(Some(tf)))) => tf,
                    other => UnexpectedPlanExprSnafu {
                        desc: format!("expect f64 literal as trend factor, but found {:?}", other),
                    }
                    .fail()?,
                };
                ScalarFunc::Udf(HoltWinters::scalar_udf(sf_exp, tf_exp))
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
                let day_lit_expr = DfExpr::Literal(ScalarValue::Utf8(Some("day".to_string())));
                let month_lit_expr = DfExpr::Literal(ScalarValue::Utf8(Some("month".to_string())));
                let interval_1month_lit_expr =
                    DfExpr::Literal(ScalarValue::IntervalYearMonth(Some(1)));
                let interval_1day_lit_expr =
                    DfExpr::Literal(ScalarValue::IntervalDayTime(Some(1 << 32)));
                let the_1month_minus_1day_expr = DfExpr::BinaryExpr(BinaryExpr {
                    left: Box::new(interval_1month_lit_expr),
                    op: Operator::Minus,
                    right: Box::new(interval_1day_lit_expr),
                });
                let date_trunc_expr = DfExpr::ScalarFunction(ScalarFunction {
                    fun: BuiltinScalarFunction::DateTrunc,
                    args: vec![month_lit_expr, self.create_time_index_column_expr()?],
                });
                let date_trunc_plus_interval_expr = DfExpr::BinaryExpr(BinaryExpr {
                    left: Box::new(date_trunc_expr),
                    op: Operator::Plus,
                    right: Box::new(the_1month_minus_1day_expr),
                });
                let date_part_expr = DfExpr::ScalarFunction(ScalarFunction {
                    fun: BuiltinScalarFunction::DatePart,
                    args: vec![day_lit_expr, date_trunc_plus_interval_expr],
                });

                exprs.push(date_part_expr);
                ScalarFunc::GeneratedExpr
            }
            _ => ScalarFunc::DataFusionBuiltin(
                BuiltinScalarFunction::from_str(func.name).map_err(|_| {
                    UnsupportedExprSnafu {
                        name: func.name.to_string(),
                    }
                    .build()
                })?,
            ),
        };

        for value in &self.ctx.field_columns {
            let col_expr = DfExpr::Column(Column::from_name(value));

            match scalar_func.clone() {
                ScalarFunc::DataFusionBuiltin(fun) => {
                    other_input_exprs.insert(field_column_pos, col_expr);
                    let fn_expr = DfExpr::ScalarFunction(ScalarFunction {
                        fun,
                        args: other_input_exprs.clone().into(),
                    });
                    exprs.push(fn_expr);
                    let _ = other_input_exprs.remove(field_column_pos);
                }
                ScalarFunc::Udf(fun) => {
                    let ts_range_expr = DfExpr::Column(Column::from_name(
                        RangeManipulate::build_timestamp_range_name(
                            self.ctx.time_index_column.as_ref().unwrap(),
                        ),
                    ));
                    other_input_exprs.insert(field_column_pos, ts_range_expr);
                    other_input_exprs.insert(field_column_pos + 1, col_expr);
                    let fn_expr = DfExpr::ScalarUDF(ScalarUDF {
                        fun: Arc::new(fun),
                        args: other_input_exprs.clone().into(),
                    });
                    exprs.push(fn_expr);
                    let _ = other_input_exprs.remove(field_column_pos + 1);
                    let _ = other_input_exprs.remove(field_column_pos);
                }
                ScalarFunc::ExtrapolateUdf(fun) => {
                    let ts_range_expr = DfExpr::Column(Column::from_name(
                        RangeManipulate::build_timestamp_range_name(
                            self.ctx.time_index_column.as_ref().unwrap(),
                        ),
                    ));
                    other_input_exprs.insert(field_column_pos, ts_range_expr);
                    other_input_exprs.insert(field_column_pos + 1, col_expr);
                    other_input_exprs
                        .insert(field_column_pos + 2, self.create_time_index_column_expr()?);
                    let fn_expr = DfExpr::ScalarUDF(ScalarUDF {
                        fun: Arc::new(fun),
                        args: other_input_exprs.clone().into(),
                    });
                    exprs.push(fn_expr);
                    let _ = other_input_exprs.remove(field_column_pos + 2);
                    let _ = other_input_exprs.remove(field_column_pos + 1);
                    let _ = other_input_exprs.remove(field_column_pos);
                }
                ScalarFunc::GeneratedExpr => {}
            }
        }

        // update value columns' name, and alias them to remove qualifiers
        let mut new_field_columns = Vec::with_capacity(exprs.len());
        exprs = exprs
            .into_iter()
            .map(|expr| {
                let display_name = expr.display_name()?;
                new_field_columns.push(display_name.clone());
                Ok(expr.alias(display_name))
            })
            .collect::<std::result::Result<Vec<_>, _>>()
            .context(DataFusionPlanningSnafu)?;
        self.ctx.field_columns = new_field_columns;

        Ok(exprs)
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

    fn create_tag_and_time_index_column_sort_exprs(&self) -> Result<Vec<DfExpr>> {
        let mut result = self
            .ctx
            .tag_columns
            .iter()
            .map(|col| DfExpr::Column(Column::from_name(col)).sort(false, false))
            .collect::<Vec<_>>();
        result.push(self.create_time_index_column_expr()?.sort(false, false));
        Ok(result)
    }

    fn create_empty_values_filter_expr(&self) -> Result<DfExpr> {
        let mut exprs = Vec::with_capacity(self.ctx.field_columns.len());
        for value in &self.ctx.field_columns {
            let expr = DfExpr::Column(Column::from_name(value)).is_not_null();
            exprs.push(expr);
        }

        utils::conjunction(exprs).context(ValueNotFoundSnafu {
            table: self.ctx.table_name.clone().unwrap(),
        })
    }

    /// Create [DfExpr::AggregateFunction] expr for each value column with given aggregate function.
    ///
    /// # Side effect
    ///
    /// This method will update value columns in context to the new value columns created by
    /// aggregate function.
    fn create_aggregate_exprs(
        &mut self,
        op: TokenType,
        input_plan: &LogicalPlan,
    ) -> Result<Vec<DfExpr>> {
        let aggr = match op.id() {
            token::T_SUM => AggregateFunctionEnum::Sum,
            token::T_AVG => AggregateFunctionEnum::Avg,
            token::T_COUNT => AggregateFunctionEnum::Count,
            token::T_MIN => AggregateFunctionEnum::Min,
            token::T_MAX => AggregateFunctionEnum::Max,
            token::T_GROUP => AggregateFunctionEnum::Grouping,
            token::T_STDDEV => AggregateFunctionEnum::StddevPop,
            token::T_STDVAR => AggregateFunctionEnum::VariancePop,
            token::T_TOPK | token::T_BOTTOMK | token::T_COUNT_VALUES | token::T_QUANTILE => {
                UnsupportedExprSnafu {
                    name: format!("{op:?}"),
                }
                .fail()?
            }
            _ => UnexpectedTokenSnafu { token: op }.fail()?,
        };

        // perform aggregate operation to each value column
        let exprs: Vec<DfExpr> = self
            .ctx
            .field_columns
            .iter()
            .map(|col| {
                DfExpr::AggregateFunction(AggregateFunction {
                    fun: aggr.clone(),
                    args: vec![DfExpr::Column(Column::from_name(col))],
                    distinct: false,
                    filter: None,
                    order_by: None,
                })
            })
            .collect();

        // update value column name according to the aggregators
        let mut new_field_columns = Vec::with_capacity(self.ctx.field_columns.len());
        let normalized_exprs =
            normalize_cols(exprs.iter().cloned(), input_plan).context(DataFusionPlanningSnafu)?;
        for expr in normalized_exprs {
            new_field_columns.push(expr.display_name().context(DataFusionPlanningSnafu)?);
        }
        self.ctx.field_columns = new_field_columns;

        Ok(exprs)
    }

    /// Try to build a DataFusion Literal Expression from PromQL Expr, return
    /// `None` if the input is not a literal expression.
    fn try_build_literal_expr(expr: &PromExpr) -> Option<DfExpr> {
        match expr {
            PromExpr::NumberLiteral(NumberLiteral { val }) => {
                let scalar_value = ScalarValue::Float64(Some(*val));
                Some(DfExpr::Literal(scalar_value))
            }
            PromExpr::StringLiteral(StringLiteral { val }) => {
                let scalar_value = ScalarValue::Utf8(Some(val.to_string()));
                Some(DfExpr::Literal(scalar_value))
            }
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

    /// Try to build a [f64] from [PromExpr].
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
                    fun: BuiltinScalarFunction::Power,
                    args: vec![lhs, rhs],
                }))
            })),
            token::T_ATAN2 => Ok(Box::new(|lhs, rhs| {
                Ok(DfExpr::ScalarFunction(ScalarFunction {
                    fun: BuiltinScalarFunction::Atan2,
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
    fn join_on_non_field_columns(
        &self,
        left: LogicalPlan,
        right: LogicalPlan,
        left_table_ref: OwnedTableReference,
        right_table_ref: OwnedTableReference,
    ) -> Result<LogicalPlan> {
        let mut tag_columns = self
            .ctx
            .tag_columns
            .iter()
            .map(Column::from_name)
            .collect::<Vec<_>>();

        // push time index column if it exist
        if let Some(time_index_column) = &self.ctx.time_index_column {
            tag_columns.push(Column::from_name(time_index_column));
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
            .join(
                right,
                JoinType::Inner,
                (tag_columns.clone(), tag_columns),
                None,
            )
            .context(DataFusionPlanningSnafu)?
            .build()
            .context(DataFusionPlanningSnafu)
    }

    /// Build a set operator (AND/OR/UNLESS)
    fn set_op_on_non_field_columns(
        &self,
        left: LogicalPlan,
        right: LogicalPlan,
        left_tag_cols: Vec<String>,
        right_tag_cols: Vec<String>,
        op: TokenType,
        modifier: &Option<BinModifier>,
    ) -> Result<LogicalPlan> {
        let mut left_tag_col_set = left_tag_cols.into_iter().collect::<HashSet<_>>();
        let mut right_tag_col_set = right_tag_cols.into_iter().collect::<HashSet<_>>();

        if matches!(op.id(), token::T_LOR) {
            return self.or_operator(left, right, left_tag_col_set, right_tag_col_set, modifier);
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
        let join_keys = left_tag_col_set
            .iter()
            .cloned()
            .chain([self.ctx.time_index_column.clone().unwrap()])
            .collect::<Vec<_>>();

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
                    true,
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
                    true,
                )
                .context(DataFusionPlanningSnafu)?
                .build()
                .context(DataFusionPlanningSnafu),
            token::T_LOR => {
                self.or_operator(left, right, left_tag_col_set, right_tag_col_set, modifier)
            }
            _ => UnexpectedTokenSnafu { token: op }.fail(),
        }
    }

    // TODO(ruihang): change function name
    fn or_operator(
        &self,
        left: LogicalPlan,
        right: LogicalPlan,
        left_tag_cols_set: HashSet<String>,
        right_tag_cols_set: HashSet<String>,
        modifier: &Option<BinModifier>,
    ) -> Result<LogicalPlan> {
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
        let left_qualifier = left.schema().field(0).qualifier().cloned();
        let right_qualifier = right.schema().field(0).qualifier().cloned();
        let left_qualifier_string = left_qualifier
            .as_ref()
            .map(|l| l.to_string())
            .unwrap_or_default();
        let right_qualifier_string = right_qualifier
            .as_ref()
            .map(|r| r.to_string())
            .unwrap_or_default();

        // step 0: fill all columns in output schema
        let all_columns_set = left
            .schema()
            .fields()
            .iter()
            .chain(right.schema().fields().iter())
            .map(|field| field.name().clone())
            .collect::<HashSet<_>>();
        let mut all_columns = all_columns_set.into_iter().collect::<Vec<_>>();
        // sort to ensure the generated schema is not volatile
        all_columns.sort_unstable();

        // step 1: align schema using project, fill non-exist columns with null
        let left_proj_exprs = all_columns.iter().map(|col| {
            if tags_not_in_left.contains(col) {
                DfExpr::Literal(ScalarValue::Utf8(None)).alias(col.to_string())
            } else {
                DfExpr::Column(Column::new(left_qualifier.clone(), col))
            }
        });
        let right_proj_exprs = all_columns.iter().map(|col| {
            if tags_not_in_right.contains(col) {
                DfExpr::Literal(ScalarValue::Utf8(None)).alias(col.to_string())
            } else {
                DfExpr::Column(Column::new(right_qualifier.clone(), col))
            }
        });

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
            self.ctx.time_index_column.clone().unwrap(),
            schema,
        );
        let result = LogicalPlan::Extension(Extension {
            node: Arc::new(union_distinct_on),
        });

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
                    self.ctx.table_name.clone(),
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
            .map(|expr| expr.display_name())
            .collect::<DfResult<Vec<_>>>()
            .context(DataFusionPlanningSnafu)?;
        let field_columns_iter = result_field_columns
            .into_iter()
            .zip(self.ctx.field_columns.iter())
            .map(|(expr, name)| Ok(DfExpr::Alias(Alias::new(expr, name.to_string()))));

        // chain non-value columns (unchanged) and value columns (applied computation then alias)
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
        let lit_expr = DfExpr::Literal(ScalarValue::Utf8(Some(date_part.to_string())));
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
            fun: BuiltinScalarFunction::DatePart,
            args: vec![lit_expr, input_expr],
        });
        Ok(fn_expr)
    }
}

#[derive(Default, Debug)]
struct FunctionArgs {
    input: Option<PromExpr>,
    literals: Vec<DfExpr>,
}

#[derive(Debug, Clone)]
enum ScalarFunc {
    DataFusionBuiltin(BuiltinScalarFunction),
    Udf(ScalarUdfDef),
    // todo(ruihang): maybe merge with Udf later
    /// UDF that require extra information like range length to be evaluated.
    ExtrapolateUdf(ScalarUdfDef),
    /// Func that doesn't require input, like `time()`.
    GeneratedExpr,
}

#[cfg(test)]
mod test {
    use std::time::{Duration, UNIX_EPOCH};

    use catalog::memory::MemoryCatalogManager;
    use catalog::RegisterTableRequest;
    use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
    use datatypes::prelude::ConcreteDataType;
    use datatypes::schema::{ColumnSchema, Schema};
    use promql_parser::label::Labels;
    use promql_parser::parser;
    use session::context::QueryContext;
    use table::metadata::{TableInfoBuilder, TableMetaBuilder};
    use table::test_util::EmptyTable;

    use super::*;

    async fn build_test_table_provider(
        table_name: String,
        num_tag: usize,
        num_field: usize,
    ) -> DfTableSourceProvider {
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
        let table_meta = TableMetaBuilder::default()
            .schema(schema)
            .primary_key_indices((0..num_tag).collect())
            .value_indices((num_tag + 1..num_tag + 1 + num_field).collect())
            .next_column_id(1024)
            .build()
            .unwrap();
        let table_info = TableInfoBuilder::default()
            .name(&table_name)
            .meta(table_meta)
            .build()
            .unwrap();
        let table = EmptyTable::from_table_info(&table_info);
        let catalog_list = MemoryCatalogManager::with_default_setup();
        assert!(catalog_list
            .register_table_sync(RegisterTableRequest {
                catalog: DEFAULT_CATALOG_NAME.to_string(),
                schema: DEFAULT_SCHEMA_NAME.to_string(),
                table_name,
                table_id: 1024,
                table,
            })
            .is_ok());
        DfTableSourceProvider::new(catalog_list, false, QueryContext::arc().as_ref())
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

        let table_provider = build_test_table_provider("some_metric".to_string(), 1, 1).await;
        let plan = PromPlanner::stmt_to_plan(table_provider, eval_stmt)
            .await
            .unwrap();

        let expected = String::from(
            "Filter: TEMPLATE(field_0) IS NOT NULL [timestamp:Timestamp(Millisecond, None), TEMPLATE(field_0):Float64;N, tag_0:Utf8]\
            \n  Projection: some_metric.timestamp, TEMPLATE(some_metric.field_0) AS TEMPLATE(field_0), some_metric.tag_0 [timestamp:Timestamp(Millisecond, None), TEMPLATE(field_0):Float64;N, tag_0:Utf8]\
            \n    PromInstantManipulate: range=[0..100000000], lookback=[1000], interval=[5000], time index=[timestamp] [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]\
            \n      PromSeriesNormalize: offset=[0], time index=[timestamp], filter NaN: [false] [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]\
            \n        PromSeriesDivide: tags=[\"tag_0\"] [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]\
            \n          Sort: some_metric.tag_0 DESC NULLS LAST, some_metric.timestamp DESC NULLS LAST [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]\
            \n            Filter: some_metric.tag_0 != Utf8(\"bar\") [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]\
            \n              TableScan: some_metric, unsupported_filters=[tag_0 != Utf8(\"bar\"), timestamp >= TimestampMillisecond(-1000, None), timestamp <= TimestampMillisecond(100001000, None)] [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]"
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
        let table_provider = build_test_table_provider("some_metric".to_string(), 2, 2).await;
        let plan = PromPlanner::stmt_to_plan(table_provider, eval_stmt.clone())
            .await
            .unwrap();
        let expected_no_without = String::from(
            "Sort: some_metric.tag_1 ASC NULLS LAST, some_metric.timestamp ASC NULLS LAST [tag_1:Utf8, timestamp:Timestamp(Millisecond, None), TEMPLATE(some_metric.field_0):Float64;N, TEMPLATE(some_metric.field_1):Float64;N]\
            \n  Aggregate: groupBy=[[some_metric.tag_1, some_metric.timestamp]], aggr=[[TEMPLATE(some_metric.field_0), TEMPLATE(some_metric.field_1)]] [tag_1:Utf8, timestamp:Timestamp(Millisecond, None), TEMPLATE(some_metric.field_0):Float64;N, TEMPLATE(some_metric.field_1):Float64;N]\
            \n    PromInstantManipulate: range=[0..100000000], lookback=[1000], interval=[5000], time index=[timestamp] [tag_0:Utf8, tag_1:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N, field_1:Float64;N]\
            \n      PromSeriesNormalize: offset=[0], time index=[timestamp], filter NaN: [false] [tag_0:Utf8, tag_1:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N, field_1:Float64;N]\
            \n        PromSeriesDivide: tags=[\"tag_0\", \"tag_1\"] [tag_0:Utf8, tag_1:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N, field_1:Float64;N]\
            \n          Sort: some_metric.tag_0 DESC NULLS LAST, some_metric.tag_1 DESC NULLS LAST, some_metric.timestamp DESC NULLS LAST [tag_0:Utf8, tag_1:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N, field_1:Float64;N]\
            \n            Filter: some_metric.tag_0 != Utf8(\"bar\") [tag_0:Utf8, tag_1:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N, field_1:Float64;N]\
            \n              TableScan: some_metric, unsupported_filters=[tag_0 != Utf8(\"bar\"), timestamp >= TimestampMillisecond(-1000, None), timestamp <= TimestampMillisecond(100001000, None)] [tag_0:Utf8, tag_1:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N, field_1:Float64;N]"
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
        let table_provider = build_test_table_provider("some_metric".to_string(), 2, 2).await;
        let plan = PromPlanner::stmt_to_plan(table_provider, eval_stmt)
            .await
            .unwrap();
        let expected_without = String::from(
            "Sort: some_metric.tag_0 ASC NULLS LAST, some_metric.timestamp ASC NULLS LAST [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), TEMPLATE(some_metric.field_0):Float64;N, TEMPLATE(some_metric.field_1):Float64;N]\
            \n  Aggregate: groupBy=[[some_metric.tag_0, some_metric.timestamp]], aggr=[[TEMPLATE(some_metric.field_0), TEMPLATE(some_metric.field_1)]] [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), TEMPLATE(some_metric.field_0):Float64;N, TEMPLATE(some_metric.field_1):Float64;N]\
            \n    PromInstantManipulate: range=[0..100000000], lookback=[1000], interval=[5000], time index=[timestamp] [tag_0:Utf8, tag_1:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N, field_1:Float64;N]\
            \n      PromSeriesNormalize: offset=[0], time index=[timestamp], filter NaN: [false] [tag_0:Utf8, tag_1:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N, field_1:Float64;N]\
            \n        PromSeriesDivide: tags=[\"tag_0\", \"tag_1\"] [tag_0:Utf8, tag_1:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N, field_1:Float64;N]\
            \n          Sort: some_metric.tag_0 DESC NULLS LAST, some_metric.tag_1 DESC NULLS LAST, some_metric.timestamp DESC NULLS LAST [tag_0:Utf8, tag_1:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N, field_1:Float64;N]\
            \n            Filter: some_metric.tag_0 != Utf8(\"bar\") [tag_0:Utf8, tag_1:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N, field_1:Float64;N]\
            \n              TableScan: some_metric, unsupported_filters=[tag_0 != Utf8(\"bar\"), timestamp >= TimestampMillisecond(-1000, None), timestamp <= TimestampMillisecond(100001000, None)] [tag_0:Utf8, tag_1:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N, field_1:Float64;N]"
        ).replace("TEMPLATE", plan_name);
        assert_eq!(plan.display_indent_schema().to_string(), expected_without);
    }

    #[tokio::test]
    async fn aggregate_sum() {
        do_aggregate_expr_plan("sum", "SUM").await;
    }

    #[tokio::test]
    async fn aggregate_avg() {
        do_aggregate_expr_plan("avg", "AVG").await;
    }

    #[tokio::test]
    #[should_panic] // output type doesn't match
    async fn aggregate_count() {
        do_aggregate_expr_plan("count", "COUNT").await;
    }

    #[tokio::test]
    async fn aggregate_min() {
        do_aggregate_expr_plan("min", "MIN").await;
    }

    #[tokio::test]
    async fn aggregate_max() {
        do_aggregate_expr_plan("max", "MAX").await;
    }

    #[tokio::test]
    #[should_panic] // output type doesn't match
    async fn aggregate_group() {
        do_aggregate_expr_plan("grouping", "GROUPING").await;
    }

    #[tokio::test]
    async fn aggregate_stddev() {
        do_aggregate_expr_plan("stddev", "STDDEV_POP").await;
    }

    #[tokio::test]
    async fn aggregate_stdvar() {
        do_aggregate_expr_plan("stdvar", "VARIANCE_POP").await;
    }

    #[tokio::test]
    #[should_panic]
    async fn aggregate_top_k() {
        do_aggregate_expr_plan("topk", "").await;
    }

    #[tokio::test]
    #[should_panic]
    async fn aggregate_bottom_k() {
        do_aggregate_expr_plan("bottomk", "").await;
    }

    #[tokio::test]
    #[should_panic]
    async fn aggregate_count_values() {
        do_aggregate_expr_plan("count_values", "").await;
    }

    #[tokio::test]
    #[should_panic]
    async fn aggregate_quantile() {
        do_aggregate_expr_plan("quantile", "").await;
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

        let table_provider = build_test_table_provider("some_metric".to_string(), 1, 1).await;
        let plan = PromPlanner::stmt_to_plan(table_provider, eval_stmt)
            .await
            .unwrap();

        let  expected = String::from(
            "Projection: some_metric.tag_0, some_metric.timestamp, some_metric.field_0 + some_metric.field_0 AS some_metric.field_0 + some_metric.field_0 [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), some_metric.field_0 + some_metric.field_0:Float64;N]\
            \n  Inner Join: some_metric.tag_0 = some_metric.tag_0, some_metric.timestamp = some_metric.timestamp [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N, tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]\
            \n    SubqueryAlias: some_metric [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]\
            \n      PromInstantManipulate: range=[0..100000000], lookback=[1000], interval=[5000], time index=[timestamp] [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]\
            \n        PromSeriesNormalize: offset=[0], time index=[timestamp], filter NaN: [false] [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]\
            \n          PromSeriesDivide: tags=[\"tag_0\"] [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]\
            \n            Sort: some_metric.tag_0 DESC NULLS LAST, some_metric.timestamp DESC NULLS LAST [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]\
            \n              Filter: some_metric.tag_0 = Utf8(\"foo\") [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]\
            \n                TableScan: some_metric, unsupported_filters=[tag_0 = Utf8(\"foo\"), timestamp >= TimestampMillisecond(-1000, None), timestamp <= TimestampMillisecond(100001000, None)] [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]\
            \n    SubqueryAlias: some_metric [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]\
            \n      PromInstantManipulate: range=[0..100000000], lookback=[1000], interval=[5000], time index=[timestamp] [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]\
            \n        PromSeriesNormalize: offset=[0], time index=[timestamp], filter NaN: [false] [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]\
            \n          PromSeriesDivide: tags=[\"tag_0\"] [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]\
            \n            Sort: some_metric.tag_0 DESC NULLS LAST, some_metric.timestamp DESC NULLS LAST [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]\
            \n              Filter: some_metric.tag_0 = Utf8(\"bar\") [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]\
            \n                TableScan: some_metric, unsupported_filters=[tag_0 = Utf8(\"bar\"), timestamp >= TimestampMillisecond(-1000, None), timestamp <= TimestampMillisecond(100001000, None)] [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]"
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

        let table_provider = build_test_table_provider("some_metric".to_string(), 1, 1).await;
        let plan = PromPlanner::stmt_to_plan(table_provider, eval_stmt)
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
            \n    PromSeriesNormalize: offset=[0], time index=[timestamp], filter NaN: [false] [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]\
            \n      PromSeriesDivide: tags=[\"tag_0\"] [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]\
            \n        Sort: some_metric.tag_0 DESC NULLS LAST, some_metric.timestamp DESC NULLS LAST [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]\
            \n          Filter: some_metric.tag_0 = Utf8(\"bar\") [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]\
            \n            TableScan: some_metric, unsupported_filters=[tag_0 = Utf8(\"bar\"), timestamp >= TimestampMillisecond(-1000, None), timestamp <= TimestampMillisecond(100001000, None)] [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]"
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
            \n    PromSeriesNormalize: offset=[0], time index=[timestamp], filter NaN: [false] [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]\
            \n      PromSeriesDivide: tags=[\"tag_0\"] [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]\
            \n        Sort: some_metric.tag_0 DESC NULLS LAST, some_metric.timestamp DESC NULLS LAST [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]\
            \n          TableScan: some_metric, unsupported_filters=[timestamp >= TimestampMillisecond(-1000, None), timestamp <= TimestampMillisecond(100001000, None)] [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]"
        );

        indie_query_plan_compare(query, expected).await;
    }

    #[tokio::test]
    async fn bool_with_additional_arithmetic() {
        let query = "some_metric + (1 == bool 2)";
        let expected = String::from(
            "Projection: some_metric.tag_0, some_metric.timestamp, some_metric.field_0 + CAST(Float64(1) = Float64(2) AS Float64) AS field_0 + Float64(1) = Float64(2) [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0 + Float64(1) = Float64(2):Float64;N]\
            \n  PromInstantManipulate: range=[0..100000000], lookback=[1000], interval=[5000], time index=[timestamp] [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]\
            \n    PromSeriesNormalize: offset=[0], time index=[timestamp], filter NaN: [false] [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]\
            \n      PromSeriesDivide: tags=[\"tag_0\"] [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]\
            \n        Sort: some_metric.tag_0 DESC NULLS LAST, some_metric.timestamp DESC NULLS LAST [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]\
            \n          TableScan: some_metric, unsupported_filters=[timestamp >= TimestampMillisecond(-1000, None), timestamp <= TimestampMillisecond(100001000, None)] [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]"
        );

        indie_query_plan_compare(query, expected).await;
    }

    #[tokio::test]
    async fn simple_unary() {
        let query = "-some_metric";
        let expected = String::from(
            "Projection: some_metric.tag_0, some_metric.timestamp, (- some_metric.field_0) AS (- field_0) [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), (- field_0):Float64;N]\
            \n  PromInstantManipulate: range=[0..100000000], lookback=[1000], interval=[5000], time index=[timestamp] [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]\
            \n    PromSeriesNormalize: offset=[0], time index=[timestamp], filter NaN: [false] [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]\
            \n      PromSeriesDivide: tags=[\"tag_0\"] [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]\
            \n        Sort: some_metric.tag_0 DESC NULLS LAST, some_metric.timestamp DESC NULLS LAST [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]\
            \n          TableScan: some_metric, unsupported_filters=[timestamp >= TimestampMillisecond(-1000, None), timestamp <= TimestampMillisecond(100001000, None)] [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]"
        );

        indie_query_plan_compare(query, expected).await;
    }

    #[tokio::test]
    async fn increase_aggr() {
        let query = "increase(some_metric[5m])";
        let expected = String::from(
            "Filter: prom_increase(timestamp_range,field_0,timestamp) IS NOT NULL [timestamp:Timestamp(Millisecond, None), prom_increase(timestamp_range,field_0,timestamp):Float64;N, tag_0:Utf8]\
            \n  Projection: some_metric.timestamp, prom_increase(timestamp_range, field_0, some_metric.timestamp) AS prom_increase(timestamp_range,field_0,timestamp), some_metric.tag_0 [timestamp:Timestamp(Millisecond, None), prom_increase(timestamp_range,field_0,timestamp):Float64;N, tag_0:Utf8]\
            \n    PromRangeManipulate: req range=[0..100000000], interval=[5000], eval range=[300000], time index=[timestamp], values=[\"field_0\"] [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Dictionary(Int64, Float64);N, timestamp_range:Dictionary(Int64, Timestamp(Millisecond, None))]\
            \n      PromSeriesNormalize: offset=[0], time index=[timestamp], filter NaN: [true] [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]\
            \n        PromSeriesDivide: tags=[\"tag_0\"] [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]\
            \n          Sort: some_metric.tag_0 DESC NULLS LAST, some_metric.timestamp DESC NULLS LAST [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]\
            \n            TableScan: some_metric, unsupported_filters=[timestamp >= TimestampMillisecond(-301000, None), timestamp <= TimestampMillisecond(100001000, None)] [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]"
        );

        indie_query_plan_compare(query, expected).await;
    }

    #[tokio::test]
    async fn less_filter_on_value() {
        let query = "some_metric < 1.2345";
        let expected = String::from(
            "Filter: some_metric.field_0 < Float64(1.2345) [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]\
            \n  PromInstantManipulate: range=[0..100000000], lookback=[1000], interval=[5000], time index=[timestamp] [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]\
            \n    PromSeriesNormalize: offset=[0], time index=[timestamp], filter NaN: [false] [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]\
            \n      PromSeriesDivide: tags=[\"tag_0\"] [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]\
            \n        Sort: some_metric.tag_0 DESC NULLS LAST, some_metric.timestamp DESC NULLS LAST [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]\
            \n          TableScan: some_metric, unsupported_filters=[timestamp >= TimestampMillisecond(-1000, None), timestamp <= TimestampMillisecond(100001000, None)] [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]"
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
            \n          Sort: some_metric.tag_0 DESC NULLS LAST, some_metric.timestamp DESC NULLS LAST [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]\
            \n            TableScan: some_metric, unsupported_filters=[timestamp >= TimestampMillisecond(-301000, None), timestamp <= TimestampMillisecond(100001000, None)] [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]"
        );

        indie_query_plan_compare(query, expected).await;
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
            let table_provider = build_test_table_provider("some_metric".to_string(), 3, 3).await;
            let plan = PromPlanner::stmt_to_plan(table_provider, eval_stmt.clone())
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
            let table_provider = build_test_table_provider("some_metric".to_string(), 3, 3).await;
            let plan = PromPlanner::stmt_to_plan(table_provider, eval_stmt.clone()).await;
            assert!(plan.is_err(), "case: {:?}", case);
        }
    }
}
