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
use std::time::Duration;

use async_recursion::async_recursion;
use catalog::table_source::DfTableSourceProvider;
use datafusion::datasource::DefaultTableSource;
use datafusion::prelude::Column;
use datafusion::scalar::ScalarValue;
use datafusion_common::tree_node::{TreeNode, TreeNodeRewriter, VisitRecursion};
use datafusion_common::{DFSchema, DataFusionError, Result as DFResult};
use datafusion_expr::expr::{AggregateFunction, AggregateUDF, ScalarUDF};
use datafusion_expr::{
    AggregateFunction as AggregateFn, Expr, Extension, LogicalPlan, LogicalPlanBuilder, Projection,
};
use datafusion_sql::planner::ContextProvider;
use datatypes::prelude::ConcreteDataType;
use promql_parser::util::parse_duration;
use snafu::{OptionExt, ResultExt};
use table::table::adapter::DfTableProviderAdapter;

use super::plan::{RangeFn, RangeSelect};
use crate::error::{
    CatalogSnafu, DataFusionSnafu, Result, TimeIndexNotFoundSnafu, UnknownTableSnafu,
};
use crate::DfContextProviderAdapter;

pub struct RangeRewriter<'a> {
    align: Duration,
    by: Vec<Expr>,
    range_fn: Vec<RangeFn>,
    context_provider: &'a DfContextProviderAdapter,
}

impl<'a> RangeRewriter<'a> {
    pub fn gen_range_expr(&self, func_name: &str, args: Vec<Expr>) -> DFResult<Expr> {
        match AggregateFn::from_str(func_name) {
            Ok(agg_fn) => Ok(Expr::AggregateFunction(AggregateFunction::new(
                agg_fn, args, false, None, None,
            ))),
            Err(_) => match self.context_provider.get_aggregate_meta(func_name) {
                Some(agg_udf) => Ok(Expr::AggregateUDF(AggregateUDF::new(
                    agg_udf, args, None, None,
                ))),
                None => Err(DataFusionError::Internal(format!(
                    "{} is not a Aggregate function or a Aggregate UDF",
                    func_name
                ))),
            },
        }
    }
}

impl<'a> TreeNodeRewriter for RangeRewriter<'a> {
    type N = Expr;

    fn mutate(&mut self, node: Expr) -> DFResult<Expr> {
        if let Expr::ScalarUDF(func) = &node {
            if func.fun.name == "range_fn" {
                let func_name = match &func.args[0] {
                    Expr::Literal(ScalarValue::Utf8(Some(agg_str))) => agg_str,
                    _ => "",
                };
                let mut index = 1;
                let mut args = vec![];
                let mut by = vec![];
                let mut have_args = false;
                let mut range_fn = RangeFn {
                    expr: Expr::Wildcard,
                    range: Duration::default(),
                    fill: String::new(),
                };
                while index < func.args.len() {
                    match &func.args[index] {
                        Expr::Column(_) => {
                            if !have_args {
                                args.push(func.args[index].clone());
                            } else {
                                by.push(func.args[index].clone());
                            }
                        }
                        Expr::Literal(ScalarValue::Utf8(Some(str))) => {
                            have_args = true;
                            if range_fn.range == Duration::default() {
                                range_fn.range = parse_duration(str.as_str())
                                    .map_err(DataFusionError::Internal)?;
                                index += 1;
                                continue;
                            }
                            if range_fn.fill.is_empty() {
                                range_fn.fill = str.clone();
                                index += 1;
                                continue;
                            }
                            let align =
                                parse_duration(str.as_str()).map_err(DataFusionError::Internal)?;
                            if self.align != Duration::default() && self.align != align {
                                return Err(DataFusionError::Internal(
                                    "Inconsistent align given in Range Function Rewrite".into(),
                                ));
                            } else {
                                self.align = align
                            }
                        }
                        _ => {
                            return Err(DataFusionError::Internal(
                                "Unexpected param in range_fn args".into(),
                            ))
                        }
                    }
                    index += 1
                }
                if !self.by.is_empty() && self.by != by {
                    return Err(DataFusionError::Internal(
                        "Inconsistent by given in Range Function Rewrite".into(),
                    ));
                } else {
                    self.by = by.clone();
                }
                range_fn.expr = self.gen_range_expr(func_name, args)?;
                let alias = Expr::Column(Column::from_name(range_fn.expr.display_name()?));
                self.range_fn.push(range_fn);
                return Ok(alias);
            }
        }
        Ok(node)
    }
}

pub struct RangePlanRewriter {
    table_provider: DfTableSourceProvider,
    context_provider: DfContextProviderAdapter,
}

impl RangePlanRewriter {
    pub fn new(
        table_provider: DfTableSourceProvider,
        context_provider: DfContextProviderAdapter,
    ) -> Self {
        Self {
            table_provider,
            context_provider,
        }
    }

    pub async fn rewrite(&mut self, plan: LogicalPlan) -> Result<LogicalPlan> {
        match self.rewrite_logical_plan(&plan).await? {
            Some(new_plan) => Ok(new_plan),
            None => Ok(plan),
        }
    }

    #[async_recursion]
    async fn rewrite_logical_plan(&mut self, plan: &LogicalPlan) -> Result<Option<LogicalPlan>> {
        let inputs = plan.inputs();
        let mut new_inputs = Vec::with_capacity(inputs.len());
        for input in &inputs {
            new_inputs.push(self.rewrite_logical_plan(input).await?)
        }
        match plan {
            LogicalPlan::Projection(Projection { expr, input, .. })
                if have_range_in_exprs(expr) =>
            {
                let input = if let Some(new_input) = new_inputs[0].take() {
                    Arc::new(new_input)
                } else {
                    input.clone()
                };
                let (time_index, default_by) = self.get_index_by(input.schema().clone()).await?;
                let mut range_rewriter = RangeRewriter {
                    align: Duration::default(),
                    by: vec![],
                    range_fn: vec![],
                    context_provider: &self.context_provider,
                };
                let new_expr = expr
                    .iter()
                    .map(|expr| expr.clone().rewrite(&mut range_rewriter))
                    .collect::<DFResult<Vec<_>>>()
                    .context(DataFusionSnafu)?;
                if range_rewriter.by.is_empty() {
                    range_rewriter.by = default_by;
                }
                let range_plan = RangeSelect::try_new(
                    input.clone(),
                    range_rewriter.range_fn,
                    range_rewriter.align,
                    time_index,
                    range_rewriter.by,
                )?;
                let project_plan = LogicalPlanBuilder::from(LogicalPlan::Extension(Extension {
                    node: Arc::new(range_plan),
                }))
                .project(new_expr)
                .context(DataFusionSnafu)?
                .build()
                .context(DataFusionSnafu)?;
                Ok(Some(project_plan))
            }
            _ => {
                if new_inputs.iter().any(|x| x.is_some()) {
                    let inputs: Vec<LogicalPlan> = new_inputs
                        .into_iter()
                        .zip(inputs.into_iter())
                        .map(|(x, y)| match x {
                            Some(plan) => plan,
                            None => y.clone(),
                        })
                        .collect();
                    Ok(Some(
                        plan.with_new_inputs(&inputs).context(DataFusionSnafu)?,
                    ))
                } else {
                    Ok(None)
                }
            }
        }
    }

    async fn get_index_by(&mut self, schema: Arc<DFSchema>) -> Result<(Expr, Vec<Expr>)> {
        let mut time_index_expr = Expr::Wildcard;
        let mut default_by = vec![];
        for field in schema.fields() {
            if let Some(table_ref) = field.qualifier() {
                let table = self
                    .table_provider
                    .resolve_table(table_ref.clone())
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
                let schema = table.schema();
                let time_index_column =
                    schema
                        .timestamp_column()
                        .with_context(|| TimeIndexNotFoundSnafu {
                            table: table_ref.to_string(),
                        })?;
                // assert time_index's datatype is timestamp
                if let ConcreteDataType::Timestamp(datatypes::types::TimestampType::Millisecond(
                    _,
                )) = time_index_column.data_type
                {
                    default_by = table
                        .table_info()
                        .meta
                        .row_key_column_names()
                        .map(|key| Expr::Column(Column::new(Some(table_ref.clone()), key)))
                        .collect();
                    time_index_expr = Expr::Column(Column::new(
                        Some(table_ref.clone()),
                        time_index_column.name.clone(),
                    ));
                }
            }
        }
        if time_index_expr == Expr::Wildcard {
            TimeIndexNotFoundSnafu {
                table: schema.to_string(),
            }
            .fail()
        } else {
            Ok((time_index_expr, default_by))
        }
    }
}

fn have_range_in_exprs(exprs: &Vec<Expr>) -> bool {
    let mut have = false;
    for expr in exprs {
        let _ = expr.apply(&mut |expr| {
            if let Expr::ScalarUDF(ScalarUDF { fun, .. }) = expr {
                if fun.name == "range_fn" {
                    have = true;
                    return Ok(VisitRecursion::Stop);
                }
            }
            Ok(VisitRecursion::Continue)
        });
        if have {
            break;
        }
    }
    have
}

#[cfg(test)]
mod test {

    use catalog::local::MemoryCatalogManager;
    use catalog::{CatalogManager, RegisterTableRequest};
    use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
    use datatypes::prelude::ConcreteDataType;
    use datatypes::schema::{ColumnSchema, Schema};
    use session::context::QueryContext;
    use table::metadata::{TableInfoBuilder, TableMetaBuilder};
    use table::test_util::EmptyTable;

    use super::*;
    use crate::parser::QueryLanguageParser;
    use crate::plan::LogicalPlan as GreptimeLogicalPlan;
    use crate::{QueryEngineFactory, QueryEngineRef};

    async fn create_test_engine() -> QueryEngineRef {
        let table_name = "test".to_string();
        let mut columns = vec![];
        for i in 0..5 {
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
        for i in 0..5 {
            columns.push(ColumnSchema::new(
                format!("field_{i}"),
                ConcreteDataType::float64_datatype(),
                true,
            ));
        }
        let schema = Arc::new(Schema::new(columns));
        let table_meta = TableMetaBuilder::default()
            .schema(schema)
            .primary_key_indices((0..5).collect())
            .value_indices((6..11).collect())
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
        assert!(catalog_list
            .register_table(RegisterTableRequest {
                catalog: DEFAULT_CATALOG_NAME.to_string(),
                schema: DEFAULT_SCHEMA_NAME.to_string(),
                table_name,
                table_id: 1024,
                table,
            })
            .await
            .is_ok());
        QueryEngineFactory::new(catalog_list, false).query_engine()
    }

    async fn query_plan_compare(sql: &str, expected: String) {
        let stmt = QueryLanguageParser::parse_sql(sql).unwrap();
        let engine = create_test_engine().await;
        let GreptimeLogicalPlan::DfPlan(plan) = engine
            .planner()
            .plan(stmt, QueryContext::arc())
            .await
            .unwrap();
        assert_eq!(plan.display_indent_schema().to_string(), expected);
    }

    #[tokio::test]
    async fn range_calculation() {
        let query = r#"SELECT (avg(field_0)+sum(field_1))/4 RANGE '5m' FROM test ALIGN '1h' by (tag_0,tag_1) FILL NULL;"#;
        let expected = String::from(
            "Projection: (AVG(test.field_0) + SUM(test.field_1)) / Int64(4) [AVG(test.field_0) + SUM(test.field_1) / Int64(4):Float64;N]\
            \n  RangeSelect: range_exprs=[RangeFn { expr:AVG(test.field_0) range:300s fill:NULL }, RangeFn { expr:SUM(test.field_1) range:300s fill:NULL }], algin=3600s time_index=timestamp [AVG(test.field_0):Float64;N, SUM(test.field_1):Float64;N, timestamp:Timestamp(Millisecond, None), tag_0:Utf8, tag_1:Utf8]\
            \n    TableScan: test [tag_0:Utf8, tag_1:Utf8, tag_2:Utf8, tag_3:Utf8, tag_4:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N, field_1:Float64;N, field_2:Float64;N, field_3:Float64;N, field_4:Float64;N]"
        );
        query_plan_compare(query, expected).await;
    }

    #[tokio::test]
    async fn range_as_sub_query() {
        let query = r#"SELECT foo + 1 from (SELECT (avg(field_0)+sum(field_1))/4 RANGE '5m' as foo FROM test ALIGN '1h' by (tag_0,tag_1) FILL NULL) where foo > 1;"#;
        let expected = String::from(
            "Projection: foo + Int64(1) [foo + Int64(1):Float64;N]\
            \n  Filter: foo > Int64(1) [foo:Float64;N]\
            \n    Projection: (AVG(test.field_0) + SUM(test.field_1)) / Int64(4) AS foo [foo:Float64;N]\
            \n      RangeSelect: range_exprs=[RangeFn { expr:AVG(test.field_0) range:300s fill:NULL }, RangeFn { expr:SUM(test.field_1) range:300s fill:NULL }], algin=3600s time_index=timestamp [AVG(test.field_0):Float64;N, SUM(test.field_1):Float64;N, timestamp:Timestamp(Millisecond, None), tag_0:Utf8, tag_1:Utf8]\
            \n        TableScan: test [tag_0:Utf8, tag_1:Utf8, tag_2:Utf8, tag_3:Utf8, tag_4:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N, field_1:Float64;N, field_2:Float64;N, field_3:Float64;N, field_4:Float64;N]"
        );
        query_plan_compare(query, expected).await;
    }

    #[tokio::test]
    async fn range_from_nest_query() {
        let query = r#"SELECT (avg(a)+sum(b))/4 RANGE '5m' FROM (SELECT field_0 as a, field_1 as b, tag_0 as c, tag_1 as d, timestamp from test where field_0 > 1.0) ALIGN '1h' by (c, d) FILL NULL;"#;
        let expected = String::from(
            "Projection: (AVG(a) + SUM(b)) / Int64(4) [AVG(a) + SUM(b) / Int64(4):Float64;N]\
            \n  RangeSelect: range_exprs=[RangeFn { expr:AVG(a) range:300s fill:NULL }, RangeFn { expr:SUM(b) range:300s fill:NULL }], algin=3600s time_index=timestamp [AVG(a):Float64;N, SUM(b):Float64;N, timestamp:Timestamp(Millisecond, None), c:Utf8, d:Utf8]\
            \n    Projection: test.field_0 AS a, test.field_1 AS b, test.tag_0 AS c, test.tag_1 AS d, test.timestamp [a:Float64;N, b:Float64;N, c:Utf8, d:Utf8, timestamp:Timestamp(Millisecond, None)]\
            \n      Filter: test.field_0 > Float64(1) [tag_0:Utf8, tag_1:Utf8, tag_2:Utf8, tag_3:Utf8, tag_4:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N, field_1:Float64;N, field_2:Float64;N, field_3:Float64;N, field_4:Float64;N]\
            \n        TableScan: test [tag_0:Utf8, tag_1:Utf8, tag_2:Utf8, tag_3:Utf8, tag_4:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N, field_1:Float64;N, field_2:Float64;N, field_3:Float64;N, field_4:Float64;N]"
        );
        query_plan_compare(query, expected).await;
    }
}
