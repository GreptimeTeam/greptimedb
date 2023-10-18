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

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use arrow_schema::DataType;
use async_recursion::async_recursion;
use catalog::table_source::DfTableSourceProvider;
use datafusion::datasource::DefaultTableSource;
use datafusion::prelude::Column;
use datafusion::scalar::ScalarValue;
use datafusion_common::tree_node::{TreeNode, TreeNodeRewriter, VisitRecursion};
use datafusion_common::{DFSchema, DataFusionError, Result as DFResult};
use datafusion_expr::expr::ScalarUDF;
use datafusion_expr::{
    Aggregate, Expr, ExprSchemable, Extension, LogicalPlan, LogicalPlanBuilder, Projection,
};
use datatypes::prelude::ConcreteDataType;
use promql_parser::util::parse_duration;
use snafu::{OptionExt, ResultExt};
use table::table::adapter::DfTableProviderAdapter;

use super::plan::Fill;
use crate::error::{
    CatalogSnafu, DataFusionSnafu, RangeQuerySnafu, Result, TimeIndexNotFoundSnafu,
    UnknownTableSnafu,
};
use crate::range_select::plan::{RangeFn, RangeSelect};

/// `RangeExprRewriter` will recursively search certain `Expr`, find all `range_fn` scalar udf contained in `Expr`,
/// and collect the information required by the RangeSelect query,
/// and finally modify the `range_fn` scalar udf to an ordinary column field.
pub struct RangeExprRewriter<'a> {
    input_plan: &'a Arc<LogicalPlan>,
    align: Duration,
    by: Vec<Expr>,
    /// Use `BTreeSet` to avoid in case like `avg(a) RANGE '5m' + avg(a) RANGE '5m'`, duplicate range expr `avg(a) RANGE '5m'` be calculate twice
    range_fn: BTreeSet<RangeFn>,
    sub_aggr: &'a Aggregate,
}

#[inline]
fn dispose_parse_error(expr: Option<&Expr>) -> DataFusionError {
    DataFusionError::Plan(
        expr.map(|x| {
            format!(
                "Illegal argument `{}` in range select query",
                x.display_name().unwrap_or_default()
            )
        })
        .unwrap_or("Missing argument in range select query".into()),
    )
}

impl<'a> RangeExprRewriter<'a> {
    pub fn get_range_expr(&self, args: &[Expr], i: usize) -> DFResult<Expr> {
        match args.get(i) {
            Some(Expr::Column(column)) => {
                let index = self.sub_aggr.schema.index_of_column(column)?;
                let len = self.sub_aggr.group_expr.len();
                self.sub_aggr
                    .aggr_expr
                    .get(index - len)
                    .cloned()
                    .ok_or(DataFusionError::Plan(
                        "Range expr not found in underlying Aggregate Plan".into(),
                    ))
            }
            other => Err(dispose_parse_error(other)),
        }
    }
}

fn parse_str_expr(args: &[Expr], i: usize) -> DFResult<&str> {
    match args.get(i) {
        Some(Expr::Literal(ScalarValue::Utf8(Some(str)))) => Ok(str.as_str()),
        other => Err(dispose_parse_error(other)),
    }
}

fn parse_expr_list(args: &[Expr], start: usize, len: usize) -> DFResult<Vec<Expr>> {
    let mut outs = Vec::with_capacity(len);
    for i in start..start + len {
        outs.push(match &args.get(i) {
            Some(
                Expr::Column(_)
                | Expr::Literal(_)
                | Expr::BinaryExpr(_)
                | Expr::ScalarFunction(_)
                | Expr::ScalarUDF(_),
            ) => args[i].clone(),
            other => {
                return Err(dispose_parse_error(*other));
            }
        });
    }
    Ok(outs)
}

impl<'a> TreeNodeRewriter for RangeExprRewriter<'a> {
    type N = Expr;

    fn mutate(&mut self, node: Expr) -> DFResult<Expr> {
        if let Expr::ScalarUDF(func) = &node {
            if func.fun.name == "range_fn" {
                // `range_fn(func, range, fill, byc, [byv], align)`
                // `[byv]` are variadic arguments, byc indicate the length of arguments
                let range_expr = self.get_range_expr(&func.args, 0)?;
                let range_str = parse_str_expr(&func.args, 1)?;
                let byc = str::parse::<usize>(parse_str_expr(&func.args, 3)?)
                    .map_err(|e| DataFusionError::Plan(e.to_string()))?;
                let by = parse_expr_list(&func.args, 4, byc)?;
                let align = parse_duration(parse_str_expr(&func.args, byc + 4)?)
                    .map_err(DataFusionError::Plan)?;
                let mut data_type = range_expr.get_type(self.input_plan.schema())?;
                let mut need_cast = false;
                let fill = Fill::try_from_str(parse_str_expr(&func.args, 2)?, &data_type)?;
                if matches!(fill, Fill::Linear) && data_type.is_integer() {
                    data_type = DataType::Float64;
                    need_cast = true;
                }
                if !self.by.is_empty() && self.by != by {
                    return Err(DataFusionError::Plan(
                        "Inconsistent by given in Range Function Rewrite".into(),
                    ));
                } else {
                    self.by = by;
                }
                if self.align != Duration::default() && self.align != align {
                    return Err(DataFusionError::Plan(
                        "Inconsistent align given in Range Function Rewrite".into(),
                    ));
                } else {
                    self.align = align;
                }
                let range_fn = RangeFn {
                    name: format!(
                        "{} RANGE {} FILL {}",
                        range_expr.display_name()?,
                        range_str,
                        fill
                    ),
                    data_type,
                    expr: range_expr,
                    range: parse_duration(range_str).map_err(DataFusionError::Plan)?,
                    fill,
                    need_cast,
                };
                let alias = Expr::Column(Column::from_name(range_fn.name.clone()));
                self.range_fn.insert(range_fn);
                return Ok(alias);
            }
        }
        Ok(node)
    }
}

/// In order to implement RangeSelect query like `avg(field_0) RANGE '5m' FILL NULL`,
/// All RangeSelect query items are converted into udf scalar function in sql parse stage, with format like `range_fn(avg(field_0), .....)`.
/// `range_fn` contains all the parameters we need to execute RangeSelect.
/// In order to correctly execute the query process of range select, we need to modify the query plan generated by datafusion.
/// We need to recursively find the entire LogicalPlan, and find all `range_fn` scalar udf contained in the project plan,
/// collecting info we need to generate RangeSelect Query LogicalPlan and rewrite th original LogicalPlan.
pub struct RangePlanRewriter {
    table_provider: DfTableSourceProvider,
}

impl RangePlanRewriter {
    pub fn new(table_provider: DfTableSourceProvider) -> Self {
        Self { table_provider }
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
                let (aggr_plan, input) = if let LogicalPlan::Aggregate(aggr) = input.as_ref() {
                    // Expr like `rate(max(a) RANGE '6m') RANGE '6m'` have legal syntax but illegal semantic.
                    if have_range_in_exprs(&aggr.aggr_expr) {
                        return RangeQuerySnafu {
                            msg: "Nest Range Query is not allowed",
                        }
                        .fail();
                    }
                    (aggr, aggr.input.clone())
                } else {
                    return RangeQuerySnafu {
                        msg: "Window functions is not allowed in Range Query",
                    }
                    .fail();
                };
                let (time_index, default_by) = self.get_index_by(input.schema()).await?;
                let mut range_rewriter = RangeExprRewriter {
                    input_plan: &input,
                    align: Duration::default(),
                    by: vec![],
                    range_fn: BTreeSet::new(),
                    sub_aggr: aggr_plan,
                };
                let new_expr = expr
                    .iter()
                    .map(|expr| expr.clone().rewrite(&mut range_rewriter))
                    .collect::<DFResult<Vec<_>>>()
                    .context(DataFusionSnafu)?;
                if range_rewriter.by.is_empty() {
                    range_rewriter.by = default_by;
                }
                let range_select = RangeSelect::try_new(
                    input.clone(),
                    range_rewriter.range_fn.into_iter().collect(),
                    range_rewriter.align,
                    time_index,
                    range_rewriter.by,
                    &new_expr,
                )?;
                let no_additional_project = range_select.schema_project.is_some();
                let range_plan = LogicalPlan::Extension(Extension {
                    node: Arc::new(range_select),
                });
                if no_additional_project {
                    Ok(Some(range_plan))
                } else {
                    let project_plan = LogicalPlanBuilder::from(range_plan)
                        .project(new_expr)
                        .context(DataFusionSnafu)?
                        .build()
                        .context(DataFusionSnafu)?;
                    Ok(Some(project_plan))
                }
            }
            _ => {
                if new_inputs.iter().any(|x| x.is_some()) {
                    let inputs: Vec<LogicalPlan> = new_inputs
                        .into_iter()
                        .zip(inputs)
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

    /// this function use to find the time_index column and row columns from input schema,
    /// return `(time_index, [row_columns])` to the rewriter.
    /// If the user does not explicitly use the `by` keyword to indicate time series,
    /// `[row_columns]` will be use as default time series
    async fn get_index_by(&mut self, schema: &Arc<DFSchema>) -> Result<(Expr, Vec<Expr>)> {
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
                if let ConcreteDataType::Timestamp(_) = time_index_column.data_type {
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

fn have_range_in_exprs(exprs: &[Expr]) -> bool {
    exprs.iter().any(|expr| {
        let mut find_range = false;
        let _ = expr.apply(&mut |expr| {
            if let Expr::ScalarUDF(ScalarUDF { fun, .. }) = expr {
                if fun.name == "range_fn" {
                    find_range = true;
                    return Ok(VisitRecursion::Stop);
                }
            }
            Ok(VisitRecursion::Continue)
        });
        find_range
    })
}

#[cfg(test)]
mod test {

    use std::error::Error;

    use catalog::memory::MemoryCatalogManager;
    use catalog::RegisterTableRequest;
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
        QueryEngineFactory::new(catalog_list, None, None, false).query_engine()
    }

    async fn do_query(sql: &str) -> Result<crate::plan::LogicalPlan> {
        let stmt = QueryLanguageParser::parse_sql(sql).unwrap();
        let engine = create_test_engine().await;
        engine.planner().plan(stmt, QueryContext::arc()).await
    }

    async fn query_plan_compare(sql: &str, expected: String) {
        let GreptimeLogicalPlan::DfPlan(plan) = do_query(sql).await.unwrap();
        assert_eq!(plan.display_indent_schema().to_string(), expected);
    }

    #[tokio::test]
    async fn range_no_project() {
        let query = r#"SELECT timestamp, tag_0, tag_1, avg(field_0 + field_1) RANGE '5m' FROM test ALIGN '1h' by (tag_0,tag_1);"#;
        let expected = String::from(
            "RangeSelect: range_exprs=[AVG(test.field_0 + test.field_1) RANGE 5m FILL NULL], align=3600s time_index=timestamp [timestamp:Timestamp(Millisecond, None), tag_0:Utf8, tag_1:Utf8, AVG(test.field_0 + test.field_1) RANGE 5m FILL NULL:Float64;N]\
            \n  TableScan: test [tag_0:Utf8, tag_1:Utf8, tag_2:Utf8, tag_3:Utf8, tag_4:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N, field_1:Float64;N, field_2:Float64;N, field_3:Float64;N, field_4:Float64;N]"
        );
        query_plan_compare(query, expected).await;
    }

    #[tokio::test]
    async fn range_expr_calculation() {
        let query = r#"SELECT (avg(field_0 + field_1)/4) RANGE '5m' FROM test ALIGN '1h' by (tag_0,tag_1);"#;
        let expected = String::from(
            "Projection: AVG(test.field_0 + test.field_1) RANGE 5m FILL NULL / Int64(4) [AVG(test.field_0 + test.field_1) RANGE 5m FILL NULL / Int64(4):Float64;N]\
            \n  RangeSelect: range_exprs=[AVG(test.field_0 + test.field_1) RANGE 5m FILL NULL], align=3600s time_index=timestamp [AVG(test.field_0 + test.field_1) RANGE 5m FILL NULL:Float64;N, timestamp:Timestamp(Millisecond, None), tag_0:Utf8, tag_1:Utf8]\
            \n    TableScan: test [tag_0:Utf8, tag_1:Utf8, tag_2:Utf8, tag_3:Utf8, tag_4:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N, field_1:Float64;N, field_2:Float64;N, field_3:Float64;N, field_4:Float64;N]"
        );
        query_plan_compare(query, expected).await;
    }

    #[tokio::test]
    async fn range_multi_args() {
        let query =
            r#"SELECT (covar(field_0 + field_1, field_1)/4) RANGE '5m' FROM test ALIGN '1h';"#;
        let expected = String::from(
            "Projection: COVARIANCE(test.field_0 + test.field_1,test.field_1) RANGE 5m FILL NULL / Int64(4) [COVARIANCE(test.field_0 + test.field_1,test.field_1) RANGE 5m FILL NULL / Int64(4):Float64;N]\
            \n  RangeSelect: range_exprs=[COVARIANCE(test.field_0 + test.field_1,test.field_1) RANGE 5m FILL NULL], align=3600s time_index=timestamp [COVARIANCE(test.field_0 + test.field_1,test.field_1) RANGE 5m FILL NULL:Float64;N, timestamp:Timestamp(Millisecond, None), tag_0:Utf8, tag_1:Utf8, tag_2:Utf8, tag_3:Utf8, tag_4:Utf8]\
            \n    TableScan: test [tag_0:Utf8, tag_1:Utf8, tag_2:Utf8, tag_3:Utf8, tag_4:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N, field_1:Float64;N, field_2:Float64;N, field_3:Float64;N, field_4:Float64;N]"
        );
        query_plan_compare(query, expected).await;
    }

    #[tokio::test]
    async fn range_calculation() {
        let query = r#"SELECT ((avg(field_0)+sum(field_1))/4) RANGE '5m' FROM test ALIGN '1h' by (tag_0,tag_1) FILL NULL;"#;
        let expected = String::from(
            "Projection: (AVG(test.field_0) RANGE 5m FILL NULL + SUM(test.field_1) RANGE 5m FILL NULL) / Int64(4) [AVG(test.field_0) RANGE 5m FILL NULL + SUM(test.field_1) RANGE 5m FILL NULL / Int64(4):Float64;N]\
            \n  RangeSelect: range_exprs=[AVG(test.field_0) RANGE 5m FILL NULL, SUM(test.field_1) RANGE 5m FILL NULL], align=3600s time_index=timestamp [AVG(test.field_0) RANGE 5m FILL NULL:Float64;N, SUM(test.field_1) RANGE 5m FILL NULL:Float64;N, timestamp:Timestamp(Millisecond, None), tag_0:Utf8, tag_1:Utf8]\
            \n    TableScan: test [tag_0:Utf8, tag_1:Utf8, tag_2:Utf8, tag_3:Utf8, tag_4:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N, field_1:Float64;N, field_2:Float64;N, field_3:Float64;N, field_4:Float64;N]"
        );
        query_plan_compare(query, expected).await;
    }

    #[tokio::test]
    async fn range_as_sub_query() {
        let query = r#"SELECT foo + 1 from (SELECT ((avg(field_0)+sum(field_1))/4) RANGE '5m' as foo FROM test ALIGN '1h' by (tag_0,tag_1) FILL NULL) where foo > 1;"#;
        let expected = String::from(
            "Projection: foo + Int64(1) [foo + Int64(1):Float64;N]\
            \n  Filter: foo > Int64(1) [foo:Float64;N]\
            \n    Projection: (AVG(test.field_0) RANGE 5m FILL NULL + SUM(test.field_1) RANGE 5m FILL NULL) / Int64(4) AS foo [foo:Float64;N]\
            \n      RangeSelect: range_exprs=[AVG(test.field_0) RANGE 5m FILL NULL, SUM(test.field_1) RANGE 5m FILL NULL], align=3600s time_index=timestamp [AVG(test.field_0) RANGE 5m FILL NULL:Float64;N, SUM(test.field_1) RANGE 5m FILL NULL:Float64;N, timestamp:Timestamp(Millisecond, None), tag_0:Utf8, tag_1:Utf8]\
            \n        TableScan: test [tag_0:Utf8, tag_1:Utf8, tag_2:Utf8, tag_3:Utf8, tag_4:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N, field_1:Float64;N, field_2:Float64;N, field_3:Float64;N, field_4:Float64;N]"
        );
        query_plan_compare(query, expected).await;
    }

    #[tokio::test]
    async fn range_from_nest_query() {
        let query = r#"SELECT ((avg(a)+sum(b))/4) RANGE '5m' FROM (SELECT field_0 as a, field_1 as b, tag_0 as c, tag_1 as d, timestamp from test where field_0 > 1.0) ALIGN '1h' by (c, d) FILL NULL;"#;
        let expected = String::from(
            "Projection: (AVG(a) RANGE 5m FILL NULL + SUM(b) RANGE 5m FILL NULL) / Int64(4) [AVG(a) RANGE 5m FILL NULL + SUM(b) RANGE 5m FILL NULL / Int64(4):Float64;N]\
            \n  RangeSelect: range_exprs=[AVG(a) RANGE 5m FILL NULL, SUM(b) RANGE 5m FILL NULL], align=3600s time_index=timestamp [AVG(a) RANGE 5m FILL NULL:Float64;N, SUM(b) RANGE 5m FILL NULL:Float64;N, timestamp:Timestamp(Millisecond, None), c:Utf8, d:Utf8]\
            \n    Projection: test.field_0 AS a, test.field_1 AS b, test.tag_0 AS c, test.tag_1 AS d, test.timestamp [a:Float64;N, b:Float64;N, c:Utf8, d:Utf8, timestamp:Timestamp(Millisecond, None)]\
            \n      Filter: test.field_0 > Float64(1) [tag_0:Utf8, tag_1:Utf8, tag_2:Utf8, tag_3:Utf8, tag_4:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N, field_1:Float64;N, field_2:Float64;N, field_3:Float64;N, field_4:Float64;N]\
            \n        TableScan: test [tag_0:Utf8, tag_1:Utf8, tag_2:Utf8, tag_3:Utf8, tag_4:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N, field_1:Float64;N, field_2:Float64;N, field_3:Float64;N, field_4:Float64;N]"
         );
        query_plan_compare(query, expected).await;
    }

    #[tokio::test]
    async fn range_in_expr() {
        let query = r#"SELECT sin(avg(field_0 + field_1) RANGE '5m' + 1) FROM test ALIGN '1h' by (tag_0,tag_1);"#;
        let expected = String::from(
            "Projection: sin(AVG(test.field_0 + test.field_1) RANGE 5m FILL NULL + Int64(1)) [sin(AVG(test.field_0 + test.field_1) RANGE 5m FILL NULL + Int64(1)):Float64;N]\
            \n  RangeSelect: range_exprs=[AVG(test.field_0 + test.field_1) RANGE 5m FILL NULL], align=3600s time_index=timestamp [AVG(test.field_0 + test.field_1) RANGE 5m FILL NULL:Float64;N, timestamp:Timestamp(Millisecond, None), tag_0:Utf8, tag_1:Utf8]\
            \n    TableScan: test [tag_0:Utf8, tag_1:Utf8, tag_2:Utf8, tag_3:Utf8, tag_4:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N, field_1:Float64;N, field_2:Float64;N, field_3:Float64;N, field_4:Float64;N]"
        );
        query_plan_compare(query, expected).await;
    }

    #[tokio::test]
    async fn duplicate_range_expr() {
        let query = r#"SELECT avg(field_0) RANGE '5m' FILL 6.0 + avg(field_0) RANGE '5m' FILL 6.0 FROM test ALIGN '1h' by (tag_0,tag_1);"#;
        let expected = String::from(
            "Projection: AVG(test.field_0) RANGE 5m FILL 6 + AVG(test.field_0) RANGE 5m FILL 6 [AVG(test.field_0) RANGE 5m FILL 6 + AVG(test.field_0) RANGE 5m FILL 6:Float64]\
            \n  RangeSelect: range_exprs=[AVG(test.field_0) RANGE 5m FILL 6], align=3600s time_index=timestamp [AVG(test.field_0) RANGE 5m FILL 6:Float64, timestamp:Timestamp(Millisecond, None), tag_0:Utf8, tag_1:Utf8]\
            \n    TableScan: test [tag_0:Utf8, tag_1:Utf8, tag_2:Utf8, tag_3:Utf8, tag_4:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N, field_1:Float64;N, field_2:Float64;N, field_3:Float64;N, field_4:Float64;N]"
        );
        query_plan_compare(query, expected).await;
    }

    #[tokio::test]
    async fn deep_nest_range_expr() {
        let query = r#"SELECT round(sin(avg(field_0 + field_1) RANGE '5m' + 1)) FROM test ALIGN '1h' by (tag_0,tag_1);"#;
        let expected = String::from(
            "Projection: round(sin(AVG(test.field_0 + test.field_1) RANGE 5m FILL NULL + Int64(1))) [round(sin(AVG(test.field_0 + test.field_1) RANGE 5m FILL NULL + Int64(1))):Float64;N]\
            \n  RangeSelect: range_exprs=[AVG(test.field_0 + test.field_1) RANGE 5m FILL NULL], align=3600s time_index=timestamp [AVG(test.field_0 + test.field_1) RANGE 5m FILL NULL:Float64;N, timestamp:Timestamp(Millisecond, None), tag_0:Utf8, tag_1:Utf8]\
            \n    TableScan: test [tag_0:Utf8, tag_1:Utf8, tag_2:Utf8, tag_3:Utf8, tag_4:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N, field_1:Float64;N, field_2:Float64;N, field_3:Float64;N, field_4:Float64;N]"
        );
        query_plan_compare(query, expected).await;
    }

    #[tokio::test]
    async fn complex_range_expr() {
        let query = r#"SELECT gcd(CAST(max(field_0 + 1) Range '5m' FILL NULL AS Int64), CAST(tag_0 AS Int64)) + round(max(field_2+1) Range '6m' FILL NULL + 1) + max(field_2+3) Range '10m' FILL NULL * CAST(tag_1 AS Float64) + 1 FROM test ALIGN '1h' by (tag_0, tag_1);"#;
        let expected = String::from(
            "Projection: gcd(CAST(MAX(test.field_0 + Int64(1)) RANGE 5m FILL NULL AS Int64), CAST(test.tag_0 AS Int64)) + round(MAX(test.field_2 + Int64(1)) RANGE 6m FILL NULL + Int64(1)) + MAX(test.field_2 + Int64(3)) RANGE 10m FILL NULL * CAST(test.tag_1 AS Float64) + Int64(1) [gcd(MAX(test.field_0 + Int64(1)) RANGE 5m FILL NULL,test.tag_0) + round(MAX(test.field_2 + Int64(1)) RANGE 6m FILL NULL + Int64(1)) + MAX(test.field_2 + Int64(3)) RANGE 10m FILL NULL * test.tag_1 + Int64(1):Float64;N]\
            \n  RangeSelect: range_exprs=[MAX(test.field_0 + Int64(1)) RANGE 5m FILL NULL, MAX(test.field_2 + Int64(1)) RANGE 6m FILL NULL, MAX(test.field_2 + Int64(3)) RANGE 10m FILL NULL], align=3600s time_index=timestamp [MAX(test.field_0 + Int64(1)) RANGE 5m FILL NULL:Float64;N, MAX(test.field_2 + Int64(1)) RANGE 6m FILL NULL:Float64;N, MAX(test.field_2 + Int64(3)) RANGE 10m FILL NULL:Float64;N, timestamp:Timestamp(Millisecond, None), tag_0:Utf8, tag_1:Utf8]\
            \n    TableScan: test [tag_0:Utf8, tag_1:Utf8, tag_2:Utf8, tag_3:Utf8, tag_4:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N, field_1:Float64;N, field_2:Float64;N, field_3:Float64;N, field_4:Float64;N]"
        );
        query_plan_compare(query, expected).await;
    }

    #[tokio::test]
    async fn range_linear_on_integer() {
        let query = r#"SELECT min(CAST(field_0 AS Int64) + CAST(field_1 AS Int64)) RANGE '5m' FILL LINEAR FROM test ALIGN '1h' by (tag_0,tag_1);"#;
        let expected = String::from(
            "RangeSelect: range_exprs=[MIN(test.field_0 + test.field_1) RANGE 5m FILL LINEAR], align=3600s time_index=timestamp [MIN(test.field_0 + test.field_1) RANGE 5m FILL LINEAR:Float64;N]\
            \n  TableScan: test [tag_0:Utf8, tag_1:Utf8, tag_2:Utf8, tag_3:Utf8, tag_4:Utf8, timestamp:Timestamp(Millisecond, None), field_0:Float64;N, field_1:Float64;N, field_2:Float64;N, field_3:Float64;N, field_4:Float64;N]"
        );
        query_plan_compare(query, expected).await;
    }

    #[tokio::test]
    async fn range_nest_range_err() {
        let query = r#"SELECT sum(avg(field_0 + field_1) RANGE '5m' + 1) RANGE '5m' + 1 FROM test ALIGN '1h' by (tag_0,tag_1);"#;
        assert_eq!(
            do_query(query).await.unwrap_err().to_string(),
            "Range Query: Nest Range Query is not allowed"
        )
    }

    #[tokio::test]
    /// Start directly from the rewritten SQL and check whether the error reported by the range expression rewriting is as expected.
    /// the right argument is `range_fn(avg(field_0), '5m', 'NULL', '0', '1h')`
    async fn range_argument_err_1() {
        let query = r#"SELECT range_fn('5m', avg(field_0), 'NULL', '1', tag_0, '1h') FROM test group by tag_0;"#;
        let error = do_query(query)
            .await
            .unwrap_err()
            .source()
            .unwrap()
            .to_string();
        assert_eq!(
            error,
            "Error during planning: Illegal argument `Utf8(\"5m\")` in range select query"
        )
    }

    #[tokio::test]
    async fn range_argument_err_2() {
        let query = r#"SELECT range_fn(avg(field_0), 5, 'NULL', '1', tag_0, '1h') FROM test group by tag_0;"#;
        let error = do_query(query)
            .await
            .unwrap_err()
            .source()
            .unwrap()
            .to_string();
        assert_eq!(
            error,
            "Error during planning: Illegal argument `Int64(5)` in range select query"
        )
    }
}
