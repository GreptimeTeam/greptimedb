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

use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

use datafusion::datasource::DefaultTableSource;
use datafusion::logical_expr::{
    BinaryExpr, BuiltinScalarFunction, Extension, Filter, LogicalPlan, LogicalPlanBuilder, Operator,
};
use datafusion::optimizer::utils;
use datafusion::prelude::{Column, Expr as DfExpr};
use datafusion::scalar::ScalarValue;
use datafusion::sql::planner::ContextProvider;
use datafusion::sql::TableReference;
use promql_parser::label::{MatchOp, Matchers, METRIC_NAME};
use promql_parser::parser::{EvalStmt, Expr as PromExpr, Function};
use snafu::{OptionExt, ResultExt};
use table::table::adapter::DfTableProviderAdapter;

use crate::error::{
    DataFusionPlanningSnafu, ExpectExprSnafu, MultipleVectorSnafu, Result, TableNameNotFoundSnafu,
    TableNotFoundSnafu, TimeIndexNotFoundSnafu, UnknownTableSnafu, UnsupportedExprSnafu,
    ValueNotFoundSnafu,
};
use crate::extension_plan::{InstantManipulate, Millisecond, SeriesNormalize};

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
            PromExpr::AggregateExpr { .. } => UnsupportedExprSnafu {
                name: "Prom Aggregate",
            }
            .fail()?,
            PromExpr::UnaryExpr { .. } => UnsupportedExprSnafu {
                name: "Prom Unary Expr",
            }
            .fail()?,
            PromExpr::BinaryExpr { lhs, rhs, .. } => {
                let _left_input = self.prom_expr_to_plan(*lhs.clone())?;
                let _right_input = self.prom_expr_to_plan(*rhs.clone())?;

                UnsupportedExprSnafu {
                    name: "Prom Binary Expr",
                }
                .fail()?
            }
            PromExpr::ParenExpr { .. } => UnsupportedExprSnafu {
                name: "Prom Paren Expr",
            }
            .fail()?,
            PromExpr::SubqueryExpr { .. } => UnsupportedExprSnafu {
                name: "Prom Subquery",
            }
            .fail()?,
            PromExpr::NumberLiteral { .. } => UnsupportedExprSnafu {
                name: "Prom Number Literal",
            }
            .fail()?,
            PromExpr::StringLiteral { .. } => UnsupportedExprSnafu {
                name: "Prom String Literal",
            }
            .fail()?,
            PromExpr::VectorSelector {
                name: _,
                offset,
                start_or_end: _,
                label_matchers,
            } => {
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
            PromExpr::MatrixSelector { .. } => UnsupportedExprSnafu {
                name: "Prom Matrix Selector",
            }
            .fail()?,
            PromExpr::Call { func, args } => {
                let args = self.create_function_args(args)?;
                let input =
                    self.prom_expr_to_plan(args.input.with_context(|| ExpectExprSnafu {
                        expr: prom_expr.clone(),
                    })?)?;
                let mut func_exprs = self.create_function_expr(func, args.literals)?;
                println!("{func_exprs:?}");
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

        println!(
            "value columns: {:?}",
            table
                .table_info()
                .meta
                .value_column_names()
                .collect::<Vec<_>>()
        );

        self.ctx.value_columns = values;

        Ok(())
    }

    // TODO(ruihang): insert column expr
    fn create_function_args(&self, args: &[Box<PromExpr>]) -> Result<FunctionArgs> {
        let mut result = FunctionArgs::default();

        for arg in args {
            match *arg.clone() {
                PromExpr::AggregateExpr { .. }
                | PromExpr::UnaryExpr { .. }
                | PromExpr::BinaryExpr { .. }
                | PromExpr::ParenExpr { .. }
                | PromExpr::SubqueryExpr { .. }
                | PromExpr::VectorSelector { .. }
                | PromExpr::MatrixSelector { .. }
                | PromExpr::Call { .. } => {
                    if result.input.replace(*arg.clone()).is_some() {
                        MultipleVectorSnafu { expr: *arg.clone() }.fail()?;
                    }
                }

                PromExpr::NumberLiteral { val, .. } => {
                    let scalar_value = ScalarValue::Float64(Some(val));
                    result.literals.push(DfExpr::Literal(scalar_value));
                }
                PromExpr::StringLiteral { val, .. } => {
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

        println!("other_input_exprs: {:?}", other_input_exprs);
        println!("value_columns: {:?}", self.ctx.value_columns);

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
    // 	input: `abs(some_metric{foo!="bar"})`,
    // 	expected: &Call{
    // 		Func: MustGetFunction("abs"),
    // 		Args: Expressions{
    // 			&VectorSelector{
    // 				Name: "some_metric",
    // 				LabelMatchers: []*labels.Matcher{
    // 					MustLabelMatcher(labels.MatchNotEqual, "foo", "bar"),
    // 					MustLabelMatcher(labels.MatchEqual, model.MetricNameLabel, "some_metric"),
    // 				},
    // 			},
    // 		},
    // 	},
    // },
    async fn do_single_instant_function_call(fn_name: &'static str, plan_name: &str) {
        let prom_expr = PromExpr::Call {
            func: Function {
                name: fn_name,
                arg_types: vec![ValueType::Vector],
                variadic: false,
                return_type: ValueType::Vector,
            },
            args: vec![Box::new(PromExpr::VectorSelector {
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
            })],
        };
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
            "Projection: some_metric.timestamp, TEMPLATE(some_metric.field_0) [timestamp:Timestamp(Millisecond, None), TEMPLATE(some_metric.field_0):Float64;N]\
            \n  PromInstantManipulate: range=[0..100000000], lookback=[1000], interval=[5000], time index=[timestamp] [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]\
            \n    PromSeriesNormalize: offset=[0], time index=[timestamp] [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]\
            \n      TableScan: some_metric, unsupported_filters=[tag_0 != Utf8(\"bar\")] [tag_0:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N]",
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
}
