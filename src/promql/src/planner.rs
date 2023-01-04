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
    BinaryExpr, BuiltinScalarFunction, Extension, LogicalPlan, LogicalPlanBuilder, Operator,
};
use datafusion::prelude::{Column, Expr as DfExpr};
use datafusion::scalar::ScalarValue;
use datafusion::sql::planner::ContextProvider;
use datafusion::sql::TableReference;
use datatypes::schema::SchemaRef;
use promql_parser::label::{MatchOp, Matchers};
use promql_parser::parser::{EvalStmt, Expr as PromExpr, Function};
use snafu::{OptionExt, ResultExt};
use table::table::adapter::DfTableProviderAdapter;

use crate::error::{
    DataFusionSnafu, ExpectExprSnafu, MultipleVectorSnafu, NoTimeIndexSnafu, Result,
    UnknownTableSnafu, UnsupportedExprSnafu,
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
            PromExpr::AggregateExpr {
                op,
                expr,
                param,
                grouping,
                without,
            } => todo!(),
            PromExpr::UnaryExpr { op, expr } => todo!(),
            PromExpr::BinaryExpr {
                op,
                lhs,
                rhs,
                matching,
                return_bool,
            } => {
                let left_input = self.prom_expr_to_plan(*lhs.clone())?;
                let right_input = self.prom_expr_to_plan(*rhs.clone())?;

                todo!()
            }
            PromExpr::ParenExpr { expr } => todo!(),
            PromExpr::SubqueryExpr {
                expr,
                range,
                offset,
                timestamp,
                start_or_end,
                step,
            } => todo!(),
            PromExpr::NumberLiteral { val, span } => todo!(),
            PromExpr::StringLiteral { val, span } => todo!(),
            PromExpr::VectorSelector {
                name,
                offset,
                start_or_end: _,
                label_matchers,
            } => {
                // This `name` should not be optional
                let name = name.as_ref().unwrap().clone();
                self.setup_context(&name)?;
                let normalize = self.selector_to_series_normalize_plan(
                    &name,
                    offset.clone(),
                    label_matchers.clone(),
                )?;
                let manipulate = InstantManipulate::new(
                    self.ctx.start,
                    self.ctx.end,
                    self.ctx.lookback_delta,
                    self.ctx.interval,
                    self.ctx
                        .time_index_column
                        .clone()
                        .context(NoTimeIndexSnafu)?,
                    normalize,
                );
                LogicalPlan::Extension(Extension {
                    node: Arc::new(manipulate),
                })
            }
            PromExpr::MatrixSelector {
                vector_selector,
                range,
            } => todo!(),
            PromExpr::Call { func, args } => {
                let args = self.create_function_args(args)?;
                let input =
                    self.prom_expr_to_plan(args.input.with_context(|| ExpectExprSnafu {
                        expr: prom_expr.clone(),
                    })?)?;
                let mut func_exprs = self.create_function_expr(func, args.literals)?;
                func_exprs.insert(0, self.create_time_index_column_expr()?);
                LogicalPlanBuilder::from(input)
                    .project(func_exprs)
                    .context(DataFusionSnafu)?
                    .build()
                    .context(DataFusionSnafu)?
            }
        };
        Ok(res)
    }

    fn selector_to_series_normalize_plan(
        &self,
        table_name: &str,
        offset: Option<Duration>,
        label_matchers: Matchers,
    ) -> Result<LogicalPlan> {
        // TODO(ruihang): add time range filter
        let filter = self.matchers_to_expr(label_matchers)?;
        let table_scan = self.create_relation(&table_name, filter)?;
        let offset = offset.unwrap_or_default();

        let series_normalize = SeriesNormalize::new(
            offset,
            self.ctx
                .time_index_column
                .clone()
                .context(NoTimeIndexSnafu)?,
            table_scan,
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
            let col = DfExpr::Column(Column::new(None::<String>, matcher.name));
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

    fn create_relation(&self, table_name: &str, filter: Vec<DfExpr>) -> Result<LogicalPlan> {
        let table_ref = TableReference::Bare { table: table_name };
        let provider = self
            .schema_provider
            .get_table_provider(table_ref)
            .context(DataFusionSnafu)?;
        let result = LogicalPlanBuilder::scan_with_filters(table_name, provider, None, filter)
            .context(DataFusionSnafu)?
            .build()
            .context(DataFusionSnafu)?;
        Ok(result)
    }

    /// Setup [PromPlannerContext]'s state fields.
    fn setup_context(&mut self, table_name: &str) -> Result<()> {
        let table = self
            .schema_provider
            .get_table_provider(TableReference::Bare { table: table_name })
            .context(DataFusionSnafu)?
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
            .context(NoTimeIndexSnafu)?
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

    /// Get [SchemaRef] of GreptimeDB (rather than DataFusion's).
    fn get_schema(&self, table_name: &str) -> Result<SchemaRef> {
        let table_ref = TableReference::Bare { table: table_name };
        let table = self
            .schema_provider
            .get_table_provider(table_ref)
            .context(DataFusionSnafu)?
            .as_any()
            .downcast_ref::<DefaultTableSource>()
            .context(UnknownTableSnafu)?
            .table_provider
            .as_any()
            .downcast_ref::<DfTableProviderAdapter>()
            .context(UnknownTableSnafu)?
            .table();

        Ok(table.schema())
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
        let vector_pos = 0;
        let scalar_func = BuiltinScalarFunction::from_str(func.name).map_err(|_| {
            UnsupportedExprSnafu {
                name: func.name.to_string(),
            }
            .build()
        })?;

        // TODO(ruihang): handle those functions doesn't require input
        let mut exprs = Vec::with_capacity(self.ctx.value_columns.len());
        for value in &self.ctx.value_columns {
            let col_expr = DfExpr::Column(Column::new(None::<String>, value));
            other_input_exprs.insert(vector_pos, col_expr);
            let fn_expr = DfExpr::ScalarFunction {
                fun: scalar_func.clone(),
                args: other_input_exprs.clone(),
            };
            exprs.push(fn_expr);
            other_input_exprs.remove(vector_pos);
        }

        Ok(exprs)
    }

    fn create_time_index_column_expr(&self) -> Result<DfExpr> {
        Ok(DfExpr::Column(Column::new(
            None::<String>,
            self.ctx
                .time_index_column
                .clone()
                .context(NoTimeIndexSnafu)?,
        )))
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
                format!("timestamp"),
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
        let context_provider =
            DfContextProviderAdapter::new(query_engine_state, query_context.into());

        context_provider
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
    async fn do_single_instant_function_call(func: Function, op_name: &str) {
        let prom_expr = PromExpr::Call {
            func,
            args: vec![Box::new(PromExpr::VectorSelector {
                name: Some("some_metric".to_owned()),
                offset: None,
                start_or_end: None,
                label_matchers: Matchers {
                    matchers: vec![Matcher {
                        op: MatchOp::NotEqual,
                        name: "tag_0".to_string(),
                        value: "bar".to_string(),
                    }],
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
        ).replace("TEMPLATE", op_name);

        assert_eq!(plan.display_indent_schema().to_string(), expected);
    }

    #[tokio::test]
    async fn single_abs() {
        let func = Function {
            name: "abs",
            arg_types: vec![ValueType::Vector],
            variadic: false,
            return_type: ValueType::Vector,
        };
        do_single_instant_function_call(func, "abs").await;
    }

    #[tokio::test]
    #[should_panic] // absent is not supported.
    async fn single_absent() {
        let func = Function {
            name: "absent",
            arg_types: vec![ValueType::Vector],
            variadic: false,
            return_type: ValueType::Vector,
        };
        do_single_instant_function_call(func, "").await;
    }

    #[tokio::test]
    async fn single_ceil() {
        let func = Function {
            name: "ceil",
            arg_types: vec![ValueType::Vector],
            variadic: false,
            return_type: ValueType::Vector,
        };
        do_single_instant_function_call(func, "ceil").await;
    }

    #[tokio::test]
    async fn single_exp() {
        let func = Function {
            name: "exp",
            arg_types: vec![ValueType::Vector],
            variadic: false,
            return_type: ValueType::Vector,
        };
        do_single_instant_function_call(func, "exp").await;
    }

    #[tokio::test]
    async fn single_ln() {
        let func = Function {
            name: "ln",
            arg_types: vec![ValueType::Vector],
            variadic: false,
            return_type: ValueType::Vector,
        };
        do_single_instant_function_call(func, "ln").await;
    }

    #[tokio::test]
    async fn single_log2() {
        let func = Function {
            name: "log2",
            arg_types: vec![ValueType::Vector],
            variadic: false,
            return_type: ValueType::Vector,
        };
        do_single_instant_function_call(func, "log2").await;
    }

    #[tokio::test]
    async fn single_log10() {
        let func = Function {
            name: "log10",
            arg_types: vec![ValueType::Vector],
            variadic: false,
            return_type: ValueType::Vector,
        };
        do_single_instant_function_call(func, "log10").await;
    }

    #[tokio::test]
    #[should_panic]
    async fn single_scalar() {
        let func = Function {
            name: "scalar",
            arg_types: vec![ValueType::Vector],
            variadic: false,
            return_type: ValueType::Vector,
        };
        do_single_instant_function_call(func, "").await;
    }

    #[tokio::test]
    #[should_panic]
    async fn single_sgn() {
        let func = Function {
            name: "sgn",
            arg_types: vec![ValueType::Vector],
            variadic: false,
            return_type: ValueType::Vector,
        };
        do_single_instant_function_call(func, "").await;
    }

    #[tokio::test]
    #[should_panic]
    async fn single_sort() {
        let func = Function {
            name: "sort",
            arg_types: vec![ValueType::Vector],
            variadic: false,
            return_type: ValueType::Vector,
        };
        do_single_instant_function_call(func, "").await;
    }

    #[tokio::test]
    #[should_panic]
    async fn single_sort_desc() {
        let func = Function {
            name: "sort_desc",
            arg_types: vec![ValueType::Vector],
            variadic: false,
            return_type: ValueType::Vector,
        };
        do_single_instant_function_call(func, "").await;
    }

    #[tokio::test]
    async fn single_sqrt() {
        let func = Function {
            name: "sqrt",
            arg_types: vec![ValueType::Vector],
            variadic: false,
            return_type: ValueType::Vector,
        };
        do_single_instant_function_call(func, "sqrt").await;
    }

    #[tokio::test]
    #[should_panic]
    async fn single_timestamp() {
        let func = Function {
            name: "timestamp",
            arg_types: vec![ValueType::Vector],
            variadic: false,
            return_type: ValueType::Vector,
        };
        do_single_instant_function_call(func, "").await;
    }

    #[tokio::test]
    async fn single_acos() {
        let func = Function {
            name: "acos",
            arg_types: vec![ValueType::Vector],
            variadic: false,
            return_type: ValueType::Vector,
        };
        do_single_instant_function_call(func, "acos").await;
    }

    #[tokio::test]
    #[should_panic]
    async fn single_acosh() {
        let func = Function {
            name: "acosh",
            arg_types: vec![ValueType::Vector],
            variadic: false,
            return_type: ValueType::Vector,
        };
        do_single_instant_function_call(func, "acosh").await;
    }

    #[tokio::test]
    async fn single_asin() {
        let func = Function {
            name: "asin",
            arg_types: vec![ValueType::Vector],
            variadic: false,
            return_type: ValueType::Vector,
        };
        do_single_instant_function_call(func, "asin").await;
    }

    #[tokio::test]
    #[should_panic]
    async fn single_asinh() {
        let func = Function {
            name: "asinh",
            arg_types: vec![ValueType::Vector],
            variadic: false,
            return_type: ValueType::Vector,
        };
        do_single_instant_function_call(func, "asinh").await;
    }

    #[tokio::test]
    async fn single_atan() {
        let func = Function {
            name: "atan",
            arg_types: vec![ValueType::Vector],
            variadic: false,
            return_type: ValueType::Vector,
        };
        do_single_instant_function_call(func, "atan").await;
    }

    #[tokio::test]
    #[should_panic]
    async fn single_atanh() {
        let func = Function {
            name: "atanh",
            arg_types: vec![ValueType::Vector],
            variadic: false,
            return_type: ValueType::Vector,
        };
        do_single_instant_function_call(func, "atanh").await;
    }

    #[tokio::test]
    async fn single_cos() {
        let func = Function {
            name: "cos",
            arg_types: vec![ValueType::Vector],
            variadic: false,
            return_type: ValueType::Vector,
        };
        do_single_instant_function_call(func, "cos").await;
    }

    #[tokio::test]
    #[should_panic]
    async fn single_cosh() {
        let func = Function {
            name: "cosh",
            arg_types: vec![ValueType::Vector],
            variadic: false,
            return_type: ValueType::Vector,
        };
        do_single_instant_function_call(func, "cosh").await;
    }

    #[tokio::test]
    async fn single_sin() {
        let func = Function {
            name: "sin",
            arg_types: vec![ValueType::Vector],
            variadic: false,
            return_type: ValueType::Vector,
        };
        do_single_instant_function_call(func, "sin").await;
    }

    #[tokio::test]
    #[should_panic]
    async fn single_sinh() {
        let func = Function {
            name: "sinh",
            arg_types: vec![ValueType::Vector],
            variadic: false,
            return_type: ValueType::Vector,
        };
        do_single_instant_function_call(func, "sinh").await;
    }

    #[tokio::test]
    async fn single_tan() {
        let func = Function {
            name: "tan",
            arg_types: vec![ValueType::Vector],
            variadic: false,
            return_type: ValueType::Vector,
        };
        do_single_instant_function_call(func, "tan").await;
    }

    #[tokio::test]
    #[should_panic]
    async fn single_tanh() {
        let func = Function {
            name: "tanh",
            arg_types: vec![ValueType::Vector],
            variadic: false,
            return_type: ValueType::Vector,
        };
        do_single_instant_function_call(func, "tanh").await;
    }

    #[tokio::test]
    #[should_panic]
    async fn single_deg() {
        let func = Function {
            name: "deg",
            arg_types: vec![ValueType::Vector],
            variadic: false,
            return_type: ValueType::Vector,
        };
        do_single_instant_function_call(func, "").await;
    }

    #[tokio::test]
    #[should_panic]
    async fn single_rad() {
        let func = Function {
            name: "rad",
            arg_types: vec![ValueType::Vector],
            variadic: false,
            return_type: ValueType::Vector,
        };
        do_single_instant_function_call(func, "").await;
    }
}
