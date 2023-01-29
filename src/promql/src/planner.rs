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

use std::collections::HashSet;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

use datafusion::datasource::DefaultTableSource;
use datafusion::logical_expr::expr::AggregateFunction;
use datafusion::logical_expr::{
    AggregateFunction as AggregateFunctionEnum, BinaryExpr, BuiltinScalarFunction, Extension,
    Filter, LogicalPlan, LogicalPlanBuilder, Operator,
};
use datafusion::optimizer::utils;
use datafusion::prelude::{Column, Expr as DfExpr, JoinType};
use datafusion::scalar::ScalarValue;
use datafusion::sql::planner::ContextProvider;
use datafusion::sql::TableReference;
use promql_parser::label::{MatchOp, Matchers, METRIC_NAME};
use promql_parser::parser::{
    token, AggregateExpr, BinaryExpr as PromBinaryExpr, Call, EvalStmt, Expr as PromExpr, Function,
    MatrixSelector, NumberLiteral, ParenExpr, StringLiteral, SubqueryExpr, TokenType, UnaryExpr,
    VectorSelector,
};
use snafu::{OptionExt, ResultExt};
use table::table::adapter::DfTableProviderAdapter;

use crate::error::{
    DataFusionPlanningSnafu, ExpectExprSnafu, LabelNotFoundSnafu, MultipleVectorSnafu, Result,
    TableNameNotFoundSnafu, TableNotFoundSnafu, TimeIndexNotFoundSnafu, UnexpectedPlanExprSnafu,
    UnexpectedTokenSnafu, UnknownTableSnafu, UnsupportedExprSnafu, ValueNotFoundSnafu,
};
use crate::extension_plan::{InstantManipulate, Millisecond, RangeManipulate, SeriesNormalize};

const LEFT_PLAN_JOIN_ALIAS: &str = "lhs";

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
    value_columns: Vec<String>,
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
}

pub struct PromPlanner<S: ContextProvider> {
    schema_provider: S,
    ctx: PromPlannerContext,
}

impl<S: ContextProvider> PromPlanner<S> {
    pub fn stmt_to_plan(stmt: EvalStmt, schema_provider: S) -> Result<LogicalPlan> {
        let mut planner = Self {
            schema_provider,
            ctx: PromPlannerContext::from_eval_stmt(&stmt),
        };
        planner.prom_expr_to_plan(stmt.expr)
    }

    pub fn prom_expr_to_plan(&mut self, prom_expr: PromExpr) -> Result<LogicalPlan> {
        let res = match &prom_expr {
            PromExpr::Aggregate(AggregateExpr {
                op,
                expr,
                // TODO(ruihang): support param
                param: _param,
                grouping,
                without,
            }) => {
                let input = self.prom_expr_to_plan(*expr.clone())?;

                // calculate columns to group by
                let schema = input.schema();
                let group_columns_indices = grouping
                    .iter()
                    .map(|label| {
                        schema
                            .index_of_column_by_name(None, label)
                            .with_context(|_| LabelNotFoundSnafu {
                                table: self.ctx.table_name.clone().unwrap(),
                            })
                    })
                    .collect::<Result<HashSet<_>>>()?;
                let value_names = self.ctx.value_columns.iter().collect::<HashSet<_>>();
                let group_exprs = schema
                    .fields()
                    .iter()
                    .enumerate()
                    .filter_map(|(i, field)| {
                        if *without != group_columns_indices.contains(&i)
                            && Some(field.name()) != self.ctx.time_index_column.as_ref()
                            && !value_names.contains(&field.name())
                        {
                            Some(DfExpr::Column(Column::from(field.name())))
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<_>>();

                // convert op and value columns to aggregate exprs
                let aggr_exprs = self.create_aggregate_exprs(*op)?;

                // create plan
                LogicalPlanBuilder::from(input)
                    .aggregate(group_exprs, aggr_exprs)
                    .context(DataFusionPlanningSnafu)?
                    .build()
                    .context(DataFusionPlanningSnafu)?
            }
            PromExpr::Unary(UnaryExpr { .. }) => UnsupportedExprSnafu {
                name: "Prom Unary Expr",
            }
            .fail()?,
            PromExpr::Binary(PromBinaryExpr { lhs, rhs, op, .. }) => {
                match (
                    Self::try_build_literal_expr(lhs),
                    Self::try_build_literal_expr(rhs),
                ) {
                    // TODO(ruihang): handle literal-only expressions
                    (Some(_lhs), Some(_rhs)) => UnsupportedExprSnafu {
                        name: "Literal-only expression",
                    }
                    .fail()?,
                    // lhs is a literal, rhs is a column
                    (Some(expr), None) => {
                        let input = self.prom_expr_to_plan(*rhs.clone())?;
                        self.projection_for_each_value_column(input, |col| {
                            Ok(DfExpr::BinaryExpr(BinaryExpr {
                                left: Box::new(expr.clone()),
                                op: Self::prom_token_to_binary_op(*op)?,
                                right: Box::new(DfExpr::Column(col.into())),
                            }))
                        })?
                    }
                    // lhs is a column, rhs is a literal
                    (None, Some(expr)) => {
                        let input = self.prom_expr_to_plan(*lhs.clone())?;
                        self.projection_for_each_value_column(input, |col| {
                            Ok(DfExpr::BinaryExpr(BinaryExpr {
                                left: Box::new(DfExpr::Column(col.into())),
                                op: Self::prom_token_to_binary_op(*op)?,
                                right: Box::new(expr.clone()),
                            }))
                        })?
                    }
                    // both are columns. join them on time index
                    (None, None) => {
                        let left_input = self.prom_expr_to_plan(*lhs.clone())?;
                        let right_input = self.prom_expr_to_plan(*rhs.clone())?;
                        let join_plan = self.join_on_time_index(left_input, right_input)?;
                        self.projection_for_each_value_column(join_plan, |col| {
                            Ok(DfExpr::BinaryExpr(BinaryExpr {
                                left: Box::new(DfExpr::Column(Column::new(
                                    Some(LEFT_PLAN_JOIN_ALIAS),
                                    col,
                                ))),
                                op: Self::prom_token_to_binary_op(*op)?,
                                right: Box::new(DfExpr::Column(Column::new(
                                    self.ctx.table_name.as_ref(),
                                    col,
                                ))),
                            }))
                        })?
                    }
                }
            }
            PromExpr::Paren(ParenExpr { .. }) => UnsupportedExprSnafu {
                name: "Prom Paren Expr",
            }
            .fail()?,
            PromExpr::Subquery(SubqueryExpr { .. }) => UnsupportedExprSnafu {
                name: "Prom Subquery",
            }
            .fail()?,
            PromExpr::NumberLiteral(NumberLiteral { .. }) => UnsupportedExprSnafu {
                name: "Prom Number Literal",
            }
            .fail()?,
            PromExpr::StringLiteral(StringLiteral { .. }) => UnsupportedExprSnafu {
                name: "Prom String Literal",
            }
            .fail()?,
            PromExpr::VectorSelector(VectorSelector {
                name: _,
                offset,
                start_or_end: _,
                label_matchers,
            }) => {
                let matchers = self.preprocess_label_matchers(label_matchers)?;
                self.setup_context()?;
                let normalize = self.selector_to_series_normalize_plan(*offset, matchers)?;
                let manipulate = InstantManipulate::new(
                    self.ctx.start,
                    self.ctx.end,
                    self.ctx.lookback_delta,
                    self.ctx.interval,
                    self.ctx
                        .time_index_column
                        .clone()
                        .expect("time index should be set in `setup_context`"),
                    normalize,
                );
                LogicalPlan::Extension(Extension {
                    node: Arc::new(manipulate),
                })
            }
            PromExpr::MatrixSelector(MatrixSelector {
                vector_selector,
                range,
            }) => {
                let normalize = match &**vector_selector {
                    PromExpr::VectorSelector(VectorSelector {
                        name: _,
                        offset,
                        start_or_end: _,
                        label_matchers,
                    })=> {
                        let matchers = self.preprocess_label_matchers(label_matchers)?;
                        self.setup_context()?;
                        self.selector_to_series_normalize_plan(*offset, matchers)?
                    }
                    _ => UnexpectedPlanExprSnafu {
                        desc: format!(
                            "MatrixSelector must contains a VectorSelector, but found {vector_selector:?}",
                        ),
                    }
                    .fail()?,
                };
                let manipulate = RangeManipulate::new(
                    self.ctx.start,
                    self.ctx.end,
                    self.ctx.interval,
                    // TODO(ruihang): convert via Timestamp datatypes to support different time units
                    range.as_millis() as _,
                    self.ctx
                        .time_index_column
                        .clone()
                        .expect("time index should be set in `setup_context`"),
                    self.ctx.value_columns.clone(),
                    normalize,
                )
                .context(DataFusionPlanningSnafu)?;

                LogicalPlan::Extension(Extension {
                    node: Arc::new(manipulate),
                })
            }
            PromExpr::Call(Call { func, args }) => {
                let args = self.create_function_args(args)?;
                let input =
                    self.prom_expr_to_plan(args.input.with_context(|| ExpectExprSnafu {
                        expr: prom_expr.clone(),
                    })?)?;
                let mut func_exprs = self.create_function_expr(func, args.literals)?;
                func_exprs.insert(0, self.create_time_index_column_expr()?);
                LogicalPlanBuilder::from(input)
                    .project(func_exprs)
                    .context(DataFusionPlanningSnafu)?
                    .filter(self.create_empty_values_filter_expr()?)
                    .context(DataFusionPlanningSnafu)?
                    .build()
                    .context(DataFusionPlanningSnafu)?
            }
        };
        Ok(res)
    }

    /// Extract metric name from `__name__` matcher and set it into [PromPlannerContext].
    /// Returns a new [Matchers] that doesn't contains metric name matcher.
    fn preprocess_label_matchers(&mut self, label_matchers: &Matchers) -> Result<Matchers> {
        let mut matchers = Vec::with_capacity(label_matchers.matchers.len());
        for matcher in &label_matchers.matchers {
            // TODO(ruihang): support other metric match ops
            if matcher.name == METRIC_NAME && matches!(matcher.op, MatchOp::Equal) {
                self.ctx.table_name = Some(matcher.value.clone());
            } else {
                matchers.push(matcher.clone());
            }
        }

        Ok(Matchers { matchers })
    }

    fn selector_to_series_normalize_plan(
        &self,
        offset: Option<Duration>,
        label_matchers: Matchers,
    ) -> Result<LogicalPlan> {
        let table_name = self.ctx.table_name.clone().unwrap();

        // make filter exprs
        let mut filters = self.matchers_to_expr(label_matchers)?;
        filters.push(self.create_time_index_column_expr()?.gt_eq(DfExpr::Literal(
            ScalarValue::TimestampMillisecond(Some(self.ctx.start), None),
        )));
        filters.push(self.create_time_index_column_expr()?.lt_eq(DfExpr::Literal(
            ScalarValue::TimestampMillisecond(Some(self.ctx.end), None),
        )));

        // make table scan with filter exprs
        let table_scan = self.create_table_scan_plan(&table_name, filters.clone())?;

        // make filter plan
        let filter_plan = LogicalPlan::Filter(
            Filter::try_new(
                // safety: at least there are two exprs that filter timestamp column.
                utils::conjunction(filters.into_iter()).unwrap(),
                Arc::new(table_scan),
            )
            .context(DataFusionPlanningSnafu)?,
        );

        // make series_normalize plan
        let offset = offset.unwrap_or_default();
        let series_normalize = SeriesNormalize::new(
            offset,
            self.ctx
                .time_index_column
                .clone()
                .with_context(|| TimeIndexNotFoundSnafu { table: table_name })?,
            filter_plan,
        );
        let logical_plan = LogicalPlan::Extension(Extension {
            node: Arc::new(series_normalize),
        });
        Ok(logical_plan)
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

    fn create_table_scan_plan(&self, table_name: &str, filter: Vec<DfExpr>) -> Result<LogicalPlan> {
        let table_ref = TableReference::Bare { table: table_name };
        let provider = self
            .schema_provider
            .get_table_provider(table_ref)
            .context(TableNotFoundSnafu { table: table_name })?;
        let result = LogicalPlanBuilder::scan_with_filters(table_name, provider, None, filter)
            .context(DataFusionPlanningSnafu)?
            .build()
            .context(DataFusionPlanningSnafu)?;
        Ok(result)
    }

    /// Setup [PromPlannerContext]'s state fields.
    fn setup_context(&mut self) -> Result<()> {
        let table_name = self
            .ctx
            .table_name
            .clone()
            .context(TableNameNotFoundSnafu)?;
        let table = self
            .schema_provider
            .get_table_provider(TableReference::Bare { table: &table_name })
            .context(DataFusionPlanningSnafu)?
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

        // set values column
        let values = table
            .table_info()
            .meta
            .value_column_names()
            .cloned()
            .collect();

        self.ctx.value_columns = values;

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

    fn create_function_expr(
        &self,
        func: &Function,
        mut other_input_exprs: Vec<DfExpr>,
    ) -> Result<Vec<DfExpr>> {
        // TODO(ruihang): check function args list

        // TODO(ruihang): set this according to in-param list
        let value_column_pos = 0;
        let scalar_func = BuiltinScalarFunction::from_str(func.name).map_err(|_| {
            UnsupportedExprSnafu {
                name: func.name.to_string(),
            }
            .build()
        })?;

        // TODO(ruihang): handle those functions doesn't require input
        let mut exprs = Vec::with_capacity(self.ctx.value_columns.len());
        for value in &self.ctx.value_columns {
            let col_expr = DfExpr::Column(Column::from_name(value));
            other_input_exprs.insert(value_column_pos, col_expr);
            let fn_expr = DfExpr::ScalarFunction {
                fun: scalar_func.clone(),
                args: other_input_exprs.clone(),
            };
            exprs.push(fn_expr);
            other_input_exprs.remove(value_column_pos);
        }

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

    fn create_empty_values_filter_expr(&self) -> Result<DfExpr> {
        let mut exprs = Vec::with_capacity(self.ctx.value_columns.len());
        for value in &self.ctx.value_columns {
            let expr = DfExpr::Column(Column::from_name(value)).is_not_null();
            exprs.push(expr);
        }

        utils::conjunction(exprs.into_iter()).context(ValueNotFoundSnafu {
            table: self.ctx.table_name.clone().unwrap(),
        })
    }

    fn create_aggregate_exprs(&self, op: TokenType) -> Result<Vec<DfExpr>> {
        let aggr = match op {
            token::T_SUM => AggregateFunctionEnum::Sum,
            token::T_AVG => AggregateFunctionEnum::Avg,
            token::T_COUNT => AggregateFunctionEnum::Count,
            token::T_MIN => AggregateFunctionEnum::Min,
            token::T_MAX => AggregateFunctionEnum::Max,
            token::T_GROUP => AggregateFunctionEnum::Grouping,
            token::T_STDDEV => AggregateFunctionEnum::Stddev,
            token::T_STDVAR => AggregateFunctionEnum::Variance,
            token::T_TOPK | token::T_BOTTOMK | token::T_COUNT_VALUES | token::T_QUANTILE => {
                UnsupportedExprSnafu {
                    name: op.to_string(),
                }
                .fail()?
            }
            _ => UnexpectedTokenSnafu { token: op }.fail()?,
        };

        let exprs = self
            .ctx
            .value_columns
            .iter()
            .map(|col| {
                DfExpr::AggregateFunction(AggregateFunction {
                    fun: aggr.clone(),
                    args: vec![DfExpr::Column(Column::from_name(col))],
                    distinct: false,
                    filter: None,
                })
            })
            .collect();
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
            | PromExpr::Call(_)
            | PromExpr::Aggregate(_)
            | PromExpr::Subquery(_) => None,
            PromExpr::Paren(ParenExpr { expr }) => Self::try_build_literal_expr(expr),
            // TODO(ruihang): support Unary operator
            PromExpr::Unary(UnaryExpr { expr, .. }) => Self::try_build_literal_expr(expr),
            PromExpr::Binary(PromBinaryExpr { lhs, rhs, op, .. }) => {
                let lhs = Self::try_build_literal_expr(lhs)?;
                let rhs = Self::try_build_literal_expr(rhs)?;
                let op = Self::prom_token_to_binary_op(*op).ok()?;
                Some(DfExpr::BinaryExpr(BinaryExpr {
                    left: Box::new(lhs),
                    op,
                    right: Box::new(rhs),
                }))
            }
        }
    }

    fn prom_token_to_binary_op(token: TokenType) -> Result<Operator> {
        match token {
            token::T_ADD => Ok(Operator::Plus),
            token::T_SUB => Ok(Operator::Minus),
            token::T_MUL => Ok(Operator::Multiply),
            token::T_DIV => Ok(Operator::Divide),
            token::T_MOD => Ok(Operator::Modulo),
            token::T_EQLC => Ok(Operator::Eq),
            token::T_NEQ => Ok(Operator::NotEq),
            token::T_GTR => Ok(Operator::Gt),
            token::T_LSS => Ok(Operator::Lt),
            token::T_GTE => Ok(Operator::GtEq),
            token::T_LTE => Ok(Operator::LtEq),
            // TODO(ruihang): support these two operators
            // token::T_POW => Ok(Operator::Power),
            // token::T_ATAN2 => Ok(Operator::Atan2),
            _ => UnexpectedTokenSnafu { token }.fail(),
        }
    }

    /// Build a inner join on time index column to concat two logical plans.
    /// The left plan will be alised as [`LEFT_PLAN_JOIN_ALIAS`].
    fn join_on_time_index(&self, left: LogicalPlan, right: LogicalPlan) -> Result<LogicalPlan> {
        let time_index_column = Column::from_name(
            self.ctx
                .time_index_column
                .clone()
                .context(TimeIndexNotFoundSnafu { table: "unknown" })?,
        );
        // Inner Join on time index column to concat two operator
        LogicalPlanBuilder::from(left)
            .alias(LEFT_PLAN_JOIN_ALIAS)
            .context(DataFusionPlanningSnafu)?
            .join(
                right,
                JoinType::Inner,
                (vec![time_index_column.clone()], vec![time_index_column]),
                None,
            )
            .context(DataFusionPlanningSnafu)?
            .build()
            .context(DataFusionPlanningSnafu)
    }

    // Build a projection that project and perform operation expr for every value columns.
    fn projection_for_each_value_column<F>(
        &self,
        input: LogicalPlan,
        name_to_expr: F,
    ) -> Result<LogicalPlan>
    where
        F: Fn(&String) -> Result<DfExpr>,
    {
        let value_columns = self
            .ctx
            .value_columns
            .iter()
            .map(name_to_expr)
            .collect::<Result<Vec<_>>>()?;
        LogicalPlanBuilder::from(input)
            .project(value_columns)
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

#[cfg(test)]
mod test {
    use std::time::UNIX_EPOCH;

    use catalog::local::MemoryCatalogManager;
    use catalog::{CatalogManager, RegisterTableRequest};
    use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
    use datatypes::prelude::ConcreteDataType;
    use datatypes::schema::{ColumnSchema, Schema};
    use promql_parser::label::Matcher;
    use promql_parser::parser::ValueType;
    use query::query_engine::QueryEngineState;
    use query::DfContextProviderAdapter;
    use session::context::QueryContext;
    use table::metadata::{TableInfoBuilder, TableMetaBuilder};
    use table::test_util::EmptyTable;

    use super::*;

    async fn build_test_context_provider(
        table_name: String,
        num_tag: usize,
        num_field: usize,
    ) -> DfContextProviderAdapter {
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
        let table = Arc::new(EmptyTable::from_table_info(&table_info));
        let catalog_list = Arc::new(MemoryCatalogManager::default());
        catalog_list
            .register_table(RegisterTableRequest {
                catalog: DEFAULT_CATALOG_NAME.to_string(),
                schema: DEFAULT_SCHEMA_NAME.to_string(),
                table_name,
                table_id: 1024,
                table,
            })
            .await
            .unwrap();

        let query_engine_state = QueryEngineState::new(catalog_list);
        let query_context = QueryContext::new();
        DfContextProviderAdapter::new(query_engine_state, query_context.into())
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
        let prom_expr = PromExpr::Call(Call {
            func: Function {
                name: fn_name,
                arg_types: vec![ValueType::Vector],
                variadic: false,
                return_type: ValueType::Vector,
            },
            args: vec![Box::new(PromExpr::VectorSelector(VectorSelector {
                name: Some("some_metric".to_owned()),
                offset: None,
                start_or_end: None,
                label_matchers: Matchers {
                    matchers: vec![
                        Matcher {
                            op: MatchOp::NotEqual,
                            name: "tag_0".to_string(),
                            value: "bar".to_string(),
                        },
                        Matcher {
                            op: MatchOp::Equal,
                            name: METRIC_NAME.to_string(),
                            value: "some_metric".to_string(),
                        },
                    ],
                },
            }))],
        });
        let eval_stmt = EvalStmt {
            expr: prom_expr,
            start: UNIX_EPOCH,
            end: UNIX_EPOCH
                .checked_add(Duration::from_secs(100_000))
                .unwrap(),
            interval: Duration::from_secs(5),
            lookback_delta: Duration::from_secs(1),
        };

        let context_provider = build_test_context_provider("some_metric".to_string(), 1, 1).await;
        let plan = PromPlanner::stmt_to_plan(eval_stmt, context_provider).unwrap();

        let  expected = String::from(
            "Filter: some_metric.field_0 IS NOT NULL [timestamp:Timestamp(Millisecond, None), TEMPLATE(some_metric.field_0):Float64;N]\
            \n  Projection: some_metric.timestamp, TEMPLATE(some_metric.field_0) [timestamp:Timestamp(Millisecond, None), TEMPLATE(some_metric.field_0):Float64;N]\
            \n    PromInstantManipulate: range=[0..100000000], lookback=[1000], interval=[5000], time index=[timestamp] [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]\
            \n      PromSeriesNormalize: offset=[0], time index=[timestamp] [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]\
            \n        Filter: tag_0 != Utf8(\"bar\") AND timestamp >= TimestampMillisecond(0, None) AND timestamp <= TimestampMillisecond(100000000, None) [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]\
            \n          TableScan: some_metric, unsupported_filters=[tag_0 != Utf8(\"bar\"), timestamp >= TimestampMillisecond(0, None), timestamp <= TimestampMillisecond(100000000, None)] [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]",
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
    async fn do_aggregate_expr_plan(op: TokenType, name: &str) {
        let prom_expr = PromExpr::Aggregate(AggregateExpr {
            op,
            expr: Box::new(PromExpr::VectorSelector(VectorSelector {
                name: Some("some_metric".to_owned()),
                offset: None,
                start_or_end: None,
                label_matchers: Matchers {
                    matchers: vec![
                        Matcher {
                            op: MatchOp::NotEqual,
                            name: "tag_0".to_string(),
                            value: "bar".to_string(),
                        },
                        Matcher {
                            op: MatchOp::Equal,
                            name: METRIC_NAME.to_string(),
                            value: "some_metric".to_string(),
                        },
                    ],
                },
            })),
            param: Box::new(PromExpr::empty_vector_selector()),
            grouping: vec![String::from("tag_1")],
            without: false,
        });
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
        let context_provider = build_test_context_provider("some_metric".to_string(), 2, 2).await;
        let plan = PromPlanner::stmt_to_plan(eval_stmt.clone(), context_provider).unwrap();
        let  expected_no_without = String::from(
            "Aggregate: groupBy=[[some_metric.tag_1]], aggr=[[TEMPLATE(some_metric.field_0), TEMPLATE(some_metric.field_1)]] [tag_1:Utf8, TEMPLATE(some_metric.field_0):Float64;N, TEMPLATE(some_metric.field_1):Float64;N]\
            \n  PromInstantManipulate: range=[0..100000000], lookback=[1000], interval=[5000], time index=[timestamp] [tag_0:Utf8, tag_1:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N, field_1:Float64;N]\
            \n    PromSeriesNormalize: offset=[0], time index=[timestamp] [tag_0:Utf8, tag_1:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N, field_1:Float64;N]\
            \n      Filter: tag_0 != Utf8(\"bar\") AND timestamp >= TimestampMillisecond(0, None) AND timestamp <= TimestampMillisecond(100000000, None) [tag_0:Utf8, tag_1:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N, field_1:Float64;N]\
            \n        TableScan: some_metric, unsupported_filters=[tag_0 != Utf8(\"bar\"), timestamp >= TimestampMillisecond(0, None), timestamp <= TimestampMillisecond(100000000, None)] [tag_0:Utf8, tag_1:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N, field_1:Float64;N]")
            .replace("TEMPLATE", name);
        assert_eq!(
            plan.display_indent_schema().to_string(),
            expected_no_without
        );

        // test group without
        if let PromExpr::Aggregate(AggregateExpr { without, .. }) = &mut eval_stmt.expr {
            *without = true;
        }
        let context_provider = build_test_context_provider("some_metric".to_string(), 2, 2).await;
        let plan = PromPlanner::stmt_to_plan(eval_stmt, context_provider).unwrap();
        let  expected_without = String::from(
            "Aggregate: groupBy=[[some_metric.tag_0]], aggr=[[TEMPLATE(some_metric.field_0), TEMPLATE(some_metric.field_1)]] [tag_0:Utf8, TEMPLATE(some_metric.field_0):Float64;N, TEMPLATE(some_metric.field_1):Float64;N]\
            \n  PromInstantManipulate: range=[0..100000000], lookback=[1000], interval=[5000], time index=[timestamp] [tag_0:Utf8, tag_1:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N, field_1:Float64;N]\
            \n    PromSeriesNormalize: offset=[0], time index=[timestamp] [tag_0:Utf8, tag_1:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N, field_1:Float64;N]\
            \n      Filter: tag_0 != Utf8(\"bar\") AND timestamp >= TimestampMillisecond(0, None) AND timestamp <= TimestampMillisecond(100000000, None) [tag_0:Utf8, tag_1:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N, field_1:Float64;N]\
            \n        TableScan: some_metric, unsupported_filters=[tag_0 != Utf8(\"bar\"), timestamp >= TimestampMillisecond(0, None), timestamp <= TimestampMillisecond(100000000, None)] [tag_0:Utf8, tag_1:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N, field_1:Float64;N]")
            .replace("TEMPLATE", name);
        assert_eq!(plan.display_indent_schema().to_string(), expected_without);
    }

    #[tokio::test]
    async fn aggregate_sum() {
        do_aggregate_expr_plan(token::T_SUM, "SUM").await;
    }

    #[tokio::test]
    async fn aggregate_avg() {
        do_aggregate_expr_plan(token::T_AVG, "AVG").await;
    }

    #[tokio::test]
    #[should_panic] // output type doesn't match
    async fn aggregate_count() {
        do_aggregate_expr_plan(token::T_COUNT, "COUNT").await;
    }

    #[tokio::test]
    async fn aggregate_min() {
        do_aggregate_expr_plan(token::T_MIN, "MIN").await;
    }

    #[tokio::test]
    async fn aggregate_max() {
        do_aggregate_expr_plan(token::T_MAX, "MAX").await;
    }

    #[tokio::test]
    #[should_panic] // output type doesn't match
    async fn aggregate_group() {
        do_aggregate_expr_plan(token::T_GROUP, "GROUPING").await;
    }

    #[tokio::test]
    async fn aggregate_stddev() {
        do_aggregate_expr_plan(token::T_STDDEV, "STDDEV").await;
    }

    #[tokio::test]
    #[should_panic] // schema doesn't match
    async fn aggregate_stdvar() {
        do_aggregate_expr_plan(token::T_STDVAR, "STDVAR").await;
    }

    #[tokio::test]
    #[should_panic]
    async fn aggregate_top_k() {
        do_aggregate_expr_plan(token::T_TOPK, "").await;
    }

    #[tokio::test]
    #[should_panic]
    async fn aggregate_bottom_k() {
        do_aggregate_expr_plan(token::T_BOTTOMK, "").await;
    }

    #[tokio::test]
    #[should_panic]
    async fn aggregate_count_values() {
        do_aggregate_expr_plan(token::T_COUNT_VALUES, "").await;
    }

    #[tokio::test]
    #[should_panic]
    async fn aggregate_quantile() {
        do_aggregate_expr_plan(token::T_QUANTILE, "").await;
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
        let prom_expr = PromExpr::Binary(PromBinaryExpr {
            lhs: Box::new(PromExpr::VectorSelector(VectorSelector {
                name: Some("some_metric".to_owned()),
                offset: None,
                start_or_end: None,
                label_matchers: Matchers {
                    matchers: vec![
                        Matcher {
                            op: MatchOp::Equal,
                            name: "tag_0".to_string(),
                            value: "foo".to_string(),
                        },
                        Matcher {
                            op: MatchOp::Equal,
                            name: METRIC_NAME.to_string(),
                            value: "some_metric".to_string(),
                        },
                    ],
                },
            })),
            op: token::T_ADD,
            rhs: Box::new(PromExpr::VectorSelector(VectorSelector {
                name: Some("some_metric".to_owned()),
                offset: None,
                start_or_end: None,
                label_matchers: Matchers {
                    matchers: vec![
                        Matcher {
                            op: MatchOp::Equal,
                            name: "tag_0".to_string(),
                            value: "bar".to_string(),
                        },
                        Matcher {
                            op: MatchOp::Equal,
                            name: METRIC_NAME.to_string(),
                            value: "some_metric".to_string(),
                        },
                    ],
                },
            })),
            matching: None,
            return_bool: false,
        });

        let eval_stmt = EvalStmt {
            expr: prom_expr,
            start: UNIX_EPOCH,
            end: UNIX_EPOCH
                .checked_add(Duration::from_secs(100_000))
                .unwrap(),
            interval: Duration::from_secs(5),
            lookback_delta: Duration::from_secs(1),
        };

        let context_provider = build_test_context_provider("some_metric".to_string(), 1, 1).await;
        let plan = PromPlanner::stmt_to_plan(eval_stmt, context_provider).unwrap();

        let  expected = String::from(
            "Projection: lhs.field_0 + some_metric.field_0 [lhs.field_0 + some_metric.field_0:Float64;N]\
            \n  Inner Join: lhs.timestamp = some_metric.timestamp [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N, tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]\
            \n    SubqueryAlias: lhs [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]\
            \n      PromInstantManipulate: range=[0..100000000], lookback=[1000], interval=[5000], time index=[timestamp] [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]\
            \n        PromSeriesNormalize: offset=[0], time index=[timestamp] [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]\
            \n          Filter: tag_0 = Utf8(\"foo\") AND timestamp >= TimestampMillisecond(0, None) AND timestamp <= TimestampMillisecond(100000000, None) [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]\
            \n            TableScan: some_metric, unsupported_filters=[tag_0 = Utf8(\"foo\"), timestamp >= TimestampMillisecond(0, None), timestamp <= TimestampMillisecond(100000000, None)] [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]\
            \n    PromInstantManipulate: range=[0..100000000], lookback=[1000], interval=[5000], time index=[timestamp] [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]\
            \n      PromSeriesNormalize: offset=[0], time index=[timestamp] [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]\
            \n        Filter: tag_0 = Utf8(\"bar\") AND timestamp >= TimestampMillisecond(0, None) AND timestamp <= TimestampMillisecond(100000000, None) [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]\
            \n          TableScan: some_metric, unsupported_filters=[tag_0 = Utf8(\"bar\"), timestamp >= TimestampMillisecond(0, None), timestamp <= TimestampMillisecond(100000000, None)] [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]"
        );

        assert_eq!(plan.display_indent_schema().to_string(), expected);
    }

    #[tokio::test]
    async fn binary_op_literal_column() {
        let prom_expr = PromExpr::Binary(PromBinaryExpr {
            lhs: Box::new(PromExpr::NumberLiteral(NumberLiteral { val: 1.0 })),
            op: token::T_ADD,
            rhs: Box::new(PromExpr::VectorSelector(VectorSelector {
                name: Some("some_metric".to_owned()),
                offset: None,
                start_or_end: None,
                label_matchers: Matchers {
                    matchers: vec![
                        Matcher {
                            op: MatchOp::Equal,
                            name: "tag_0".to_string(),
                            value: "bar".to_string(),
                        },
                        Matcher {
                            op: MatchOp::Equal,
                            name: METRIC_NAME.to_string(),
                            value: "some_metric".to_string(),
                        },
                    ],
                },
            })),
            matching: None,
            return_bool: false,
        });

        let eval_stmt = EvalStmt {
            expr: prom_expr,
            start: UNIX_EPOCH,
            end: UNIX_EPOCH
                .checked_add(Duration::from_secs(100_000))
                .unwrap(),
            interval: Duration::from_secs(5),
            lookback_delta: Duration::from_secs(1),
        };

        let context_provider = build_test_context_provider("some_metric".to_string(), 1, 1).await;
        let plan = PromPlanner::stmt_to_plan(eval_stmt, context_provider).unwrap();

        let  expected = String::from(
            "Projection: Float64(1) + some_metric.field_0 [Float64(1) + some_metric.field_0:Float64;N]\
            \n  PromInstantManipulate: range=[0..100000000], lookback=[1000], interval=[5000], time index=[timestamp] [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]\
            \n    PromSeriesNormalize: offset=[0], time index=[timestamp] [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]\
            \n      Filter: tag_0 = Utf8(\"bar\") AND timestamp >= TimestampMillisecond(0, None) AND timestamp <= TimestampMillisecond(100000000, None) [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]\
            \n        TableScan: some_metric, unsupported_filters=[tag_0 = Utf8(\"bar\"), timestamp >= TimestampMillisecond(0, None), timestamp <= TimestampMillisecond(100000000, None)] [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]"
        );

        assert_eq!(plan.display_indent_schema().to_string(), expected);
    }

    #[tokio::test]
    async fn binary_op_literal_literal() {
        let prom_expr = PromExpr::Binary(PromBinaryExpr {
            lhs: Box::new(PromExpr::NumberLiteral(NumberLiteral { val: 1.0 })),
            op: token::T_ADD,
            rhs: Box::new(PromExpr::NumberLiteral(NumberLiteral { val: 1.0 })),
            matching: None,
            return_bool: false,
        });

        let eval_stmt = EvalStmt {
            expr: prom_expr,
            start: UNIX_EPOCH,
            end: UNIX_EPOCH
                .checked_add(Duration::from_secs(100_000))
                .unwrap(),
            interval: Duration::from_secs(5),
            lookback_delta: Duration::from_secs(1),
        };

        let context_provider = build_test_context_provider("some_metric".to_string(), 1, 1).await;
        let plan_result = PromPlanner::stmt_to_plan(eval_stmt, context_provider);
        assert!(plan_result.is_err());
    }
}
