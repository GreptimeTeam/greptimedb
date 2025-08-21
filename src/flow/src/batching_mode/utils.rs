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

//! some utils for helping with batching mode

use std::collections::{BTreeSet, HashSet};
use std::sync::Arc;

use catalog::CatalogManagerRef;
use common_error::ext::BoxedError;
use common_telemetry::debug;
use datafusion::error::Result as DfResult;
use datafusion::logical_expr::Expr;
use datafusion::sql::unparser::Unparser;
use datafusion_common::tree_node::{
    Transformed, TreeNodeRecursion, TreeNodeRewriter, TreeNodeVisitor,
};
use datafusion_common::{DFSchema, DataFusionError, ScalarValue};
use datafusion_expr::{Distinct, LogicalPlan, Projection};
use datatypes::schema::SchemaRef;
use query::parser::{PromQuery, QueryLanguageParser, QueryStatement, DEFAULT_LOOKBACK_STRING};
use query::QueryEngineRef;
use session::context::QueryContextRef;
use snafu::{ensure, OptionExt, ResultExt};
use sql::parser::{ParseOptions, ParserContext};
use sql::statements::statement::Statement;
use sql::statements::tql::Tql;
use table::TableRef;

use crate::adapter::AUTO_CREATED_PLACEHOLDER_TS_COL;
use crate::df_optimizer::apply_df_optimizer;
use crate::error::{DatafusionSnafu, ExternalSnafu, InvalidQuerySnafu, TableNotFoundSnafu};
use crate::{Error, TableName};

pub async fn get_table_info_df_schema(
    catalog_mr: CatalogManagerRef,
    table_name: TableName,
) -> Result<(TableRef, Arc<DFSchema>), Error> {
    let full_table_name = table_name.clone().join(".");
    let table = catalog_mr
        .table(&table_name[0], &table_name[1], &table_name[2], None)
        .await
        .map_err(BoxedError::new)
        .context(ExternalSnafu)?
        .context(TableNotFoundSnafu {
            name: &full_table_name,
        })?;
    let table_info = table.table_info();

    let schema = table_info.meta.schema.clone();

    let df_schema: Arc<DFSchema> = Arc::new(
        schema
            .arrow_schema()
            .clone()
            .try_into()
            .with_context(|_| DatafusionSnafu {
                context: format!(
                    "Failed to convert arrow schema to datafusion schema, arrow_schema={:?}",
                    schema.arrow_schema()
                ),
            })?,
    );
    Ok((table, df_schema))
}

/// Convert sql to datafusion logical plan
/// Also support TQL (but only Eval not Explain or Analyze)
pub async fn sql_to_df_plan(
    query_ctx: QueryContextRef,
    engine: QueryEngineRef,
    sql: &str,
    optimize: bool,
) -> Result<LogicalPlan, Error> {
    let stmts =
        ParserContext::create_with_dialect(sql, query_ctx.sql_dialect(), ParseOptions::default())
            .map_err(BoxedError::new)
            .context(ExternalSnafu)?;

    ensure!(
        stmts.len() == 1,
        InvalidQuerySnafu {
            reason: format!("Expect only one statement, found {}", stmts.len())
        }
    );
    let stmt = &stmts[0];
    let query_stmt = match stmt {
        Statement::Tql(tql) => match tql {
            Tql::Eval(eval) => {
                let eval = eval.clone();
                let promql = PromQuery {
                    start: eval.start,
                    end: eval.end,
                    step: eval.step,
                    query: eval.query,
                    lookback: eval
                        .lookback
                        .unwrap_or_else(|| DEFAULT_LOOKBACK_STRING.to_string()),
                };

                QueryLanguageParser::parse_promql(&promql, &query_ctx)
                    .map_err(BoxedError::new)
                    .context(ExternalSnafu)?
            }
            _ => InvalidQuerySnafu {
                reason: format!("TQL statement {tql:?} is not supported, expect only TQL EVAL"),
            }
            .fail()?,
        },
        _ => QueryStatement::Sql(stmt.clone()),
    };
    let plan = engine
        .planner()
        .plan(&query_stmt, query_ctx)
        .await
        .map_err(BoxedError::new)
        .context(ExternalSnafu)?;

    let plan = if optimize {
        apply_df_optimizer(plan).await?
    } else {
        plan
    };
    Ok(plan)
}

pub fn df_plan_to_sql(plan: &LogicalPlan) -> Result<String, Error> {
    /// A dialect that forces identifiers to be quoted when have uppercase
    struct ForceQuoteIdentifiers;
    impl datafusion::sql::unparser::dialect::Dialect for ForceQuoteIdentifiers {
        fn identifier_quote_style(&self, identifier: &str) -> Option<char> {
            if identifier.to_lowercase() != identifier {
                Some('`')
            } else {
                None
            }
        }
    }
    let unparser = Unparser::new(&ForceQuoteIdentifiers);
    // first make all column qualified
    let sql = unparser
        .plan_to_sql(plan)
        .with_context(|_e| DatafusionSnafu {
            context: format!("Failed to unparse logical plan {plan:?}"),
        })?;
    Ok(sql.to_string())
}

/// Helper to find the innermost group by expr in schema, return None if no group by expr
#[derive(Debug, Clone, Default)]
pub struct FindGroupByFinalName {
    group_exprs: Option<HashSet<datafusion_expr::Expr>>,
}

impl FindGroupByFinalName {
    pub fn get_group_expr_names(&self) -> Option<HashSet<String>> {
        self.group_exprs
            .as_ref()
            .map(|exprs| exprs.iter().map(|expr| expr.qualified_name().1).collect())
    }
}

impl TreeNodeVisitor<'_> for FindGroupByFinalName {
    type Node = LogicalPlan;

    fn f_down(&mut self, node: &Self::Node) -> datafusion_common::Result<TreeNodeRecursion> {
        if let LogicalPlan::Aggregate(aggregate) = node {
            self.group_exprs = Some(aggregate.group_expr.iter().cloned().collect());
            debug!(
                "FindGroupByFinalName: Get Group by exprs from Aggregate: {:?}",
                self.group_exprs
            );
        } else if let LogicalPlan::Distinct(distinct) = node {
            debug!("FindGroupByFinalName: Distinct: {}", node);
            match distinct {
                Distinct::All(input) => {
                    if let LogicalPlan::TableScan(table_scan) = &**input {
                        // get column from field_qualifier, projection and projected_schema:
                        let len = table_scan.projected_schema.fields().len();
                        let columns = (0..len)
                            .map(|f| {
                                let (qualifier, field) =
                                    table_scan.projected_schema.qualified_field(f);
                                datafusion_common::Column::new(qualifier.cloned(), field.name())
                            })
                            .map(datafusion_expr::Expr::Column);
                        self.group_exprs = Some(columns.collect());
                    } else {
                        self.group_exprs = Some(input.expressions().iter().cloned().collect())
                    }
                }
                Distinct::On(distinct_on) => {
                    self.group_exprs = Some(distinct_on.on_expr.iter().cloned().collect())
                }
            }
            debug!(
                "FindGroupByFinalName: Get Group by exprs from Distinct: {:?}",
                self.group_exprs
            );
        }

        Ok(TreeNodeRecursion::Continue)
    }

    /// deal with projection when going up with group exprs
    fn f_up(&mut self, node: &Self::Node) -> datafusion_common::Result<TreeNodeRecursion> {
        if let LogicalPlan::Projection(projection) = node {
            for expr in &projection.expr {
                let Some(group_exprs) = &mut self.group_exprs else {
                    return Ok(TreeNodeRecursion::Continue);
                };
                if let datafusion_expr::Expr::Alias(alias) = expr {
                    // if a alias exist, replace with the new alias
                    let mut new_group_exprs = group_exprs.clone();
                    for group_expr in group_exprs.iter() {
                        if group_expr.name_for_alias()? == alias.expr.name_for_alias()? {
                            new_group_exprs.remove(group_expr);
                            new_group_exprs.insert(expr.clone());
                            break;
                        }
                    }
                    *group_exprs = new_group_exprs;
                }
            }
        }
        debug!("Aliased group by exprs: {:?}", self.group_exprs);
        Ok(TreeNodeRecursion::Continue)
    }
}

/// Add to the final select columns like `update_at`
/// (which doesn't necessary need to have exact name just need to be a extra timestamp column)
/// and `__ts_placeholder`(this column need to have exact this name and be a timestamp)
/// with values like `now()` and `0`
///
/// it also give existing columns alias to column in sink table if needed
#[derive(Debug)]
pub struct AddAutoColumnRewriter {
    pub schema: SchemaRef,
    pub is_rewritten: bool,
}

impl AddAutoColumnRewriter {
    pub fn new(schema: SchemaRef) -> Self {
        Self {
            schema,
            is_rewritten: false,
        }
    }
}

impl TreeNodeRewriter for AddAutoColumnRewriter {
    type Node = LogicalPlan;
    fn f_down(&mut self, mut node: Self::Node) -> DfResult<Transformed<Self::Node>> {
        if self.is_rewritten {
            return Ok(Transformed::no(node));
        }

        // if is distinct all, wrap it in a projection
        if let LogicalPlan::Distinct(Distinct::All(_)) = &node {
            let mut exprs = vec![];

            for field in node.schema().fields().iter() {
                exprs.push(Expr::Column(datafusion::common::Column::new_unqualified(
                    field.name(),
                )));
            }

            let projection =
                LogicalPlan::Projection(Projection::try_new(exprs, Arc::new(node.clone()))?);

            node = projection;
        }
        // handle table_scan by wrap it in a projection
        else if let LogicalPlan::TableScan(table_scan) = node {
            let mut exprs = vec![];

            for field in table_scan.projected_schema.fields().iter() {
                exprs.push(Expr::Column(datafusion::common::Column::new(
                    Some(table_scan.table_name.clone()),
                    field.name(),
                )));
            }

            let projection = LogicalPlan::Projection(Projection::try_new(
                exprs,
                Arc::new(LogicalPlan::TableScan(table_scan)),
            )?);

            node = projection;
        }

        // only do rewrite if found the outermost projection
        let mut exprs = if let LogicalPlan::Projection(project) = &node {
            project.expr.clone()
        } else {
            return Ok(Transformed::no(node));
        };

        let all_names = self
            .schema
            .column_schemas()
            .iter()
            .map(|c| c.name.clone())
            .collect::<BTreeSet<_>>();
        // first match by position
        for (idx, expr) in exprs.iter_mut().enumerate() {
            if !all_names.contains(&expr.qualified_name().1) {
                if let Some(col_name) = self
                    .schema
                    .column_schemas()
                    .get(idx)
                    .map(|c| c.name.clone())
                {
                    // if the data type mismatched, later check_execute will error out
                    // hence no need to check it here, beside, optimize pass might be able to cast it
                    // so checking here is not necessary
                    *expr = expr.clone().alias(col_name);
                }
            }
        }

        // add columns if have different column count
        let query_col_cnt = exprs.len();
        let table_col_cnt = self.schema.column_schemas().len();
        debug!("query_col_cnt={query_col_cnt}, table_col_cnt={table_col_cnt}");

        let placeholder_ts_expr =
            datafusion::logical_expr::lit(ScalarValue::TimestampMillisecond(Some(0), None))
                .alias(AUTO_CREATED_PLACEHOLDER_TS_COL);

        if query_col_cnt == table_col_cnt {
            // still need to add alias, see below
        } else if query_col_cnt + 1 == table_col_cnt {
            let last_col_schema = self.schema.column_schemas().last().unwrap();

            // if time index column is auto created add it
            if last_col_schema.name == AUTO_CREATED_PLACEHOLDER_TS_COL
                && self.schema.timestamp_index() == Some(table_col_cnt - 1)
            {
                exprs.push(placeholder_ts_expr);
            } else if last_col_schema.data_type.is_timestamp() {
                // is the update at column
                exprs.push(datafusion::prelude::now().alias(&last_col_schema.name));
            } else {
                // helpful error message
                return Err(DataFusionError::Plan(format!(
                    "Expect the last column in table to be timestamp column, found column {} with type {:?}",
                    last_col_schema.name,
                    last_col_schema.data_type
                )));
            }
        } else if query_col_cnt + 2 == table_col_cnt {
            let mut col_iter = self.schema.column_schemas().iter().rev();
            let last_col_schema = col_iter.next().unwrap();
            let second_last_col_schema = col_iter.next().unwrap();
            if second_last_col_schema.data_type.is_timestamp() {
                exprs.push(datafusion::prelude::now().alias(&second_last_col_schema.name));
            } else {
                return Err(DataFusionError::Plan(format!(
                    "Expect the second last column in the table to be timestamp column, found column {} with type {:?}",
                    second_last_col_schema.name,
                    second_last_col_schema.data_type
                )));
            }

            if last_col_schema.name == AUTO_CREATED_PLACEHOLDER_TS_COL
                && self.schema.timestamp_index() == Some(table_col_cnt - 1)
            {
                exprs.push(placeholder_ts_expr);
            } else {
                return Err(DataFusionError::Plan(format!(
                    "Expect timestamp column {}, found {:?}",
                    AUTO_CREATED_PLACEHOLDER_TS_COL, last_col_schema
                )));
            }
        } else {
            return Err(DataFusionError::Plan(format!(
                    "Expect table have 0,1 or 2 columns more than query columns, found {} query columns {:?}, {} table columns {:?}",
                    query_col_cnt, exprs, table_col_cnt, self.schema.column_schemas()
                )));
        }

        self.is_rewritten = true;
        let new_plan = node.with_new_exprs(exprs, node.inputs().into_iter().cloned().collect())?;
        Ok(Transformed::yes(new_plan))
    }

    /// We might add new columns, so we need to recompute the schema
    fn f_up(&mut self, node: Self::Node) -> DfResult<Transformed<Self::Node>> {
        node.recompute_schema().map(Transformed::yes)
    }
}

/// Find out the `Filter` Node corresponding to innermost(deepest) `WHERE` and add a new filter expr to it
#[derive(Debug)]
pub struct AddFilterRewriter {
    extra_filter: Expr,
    is_rewritten: bool,
}

impl AddFilterRewriter {
    pub fn new(filter: Expr) -> Self {
        Self {
            extra_filter: filter,
            is_rewritten: false,
        }
    }
}

impl TreeNodeRewriter for AddFilterRewriter {
    type Node = LogicalPlan;
    fn f_up(&mut self, node: Self::Node) -> DfResult<Transformed<Self::Node>> {
        if self.is_rewritten {
            return Ok(Transformed::no(node));
        }
        match node {
            LogicalPlan::Filter(mut filter) => {
                filter.predicate = filter.predicate.and(self.extra_filter.clone());
                self.is_rewritten = true;
                Ok(Transformed::yes(LogicalPlan::Filter(filter)))
            }
            LogicalPlan::TableScan(_) => {
                // add a new filter
                let filter =
                    datafusion_expr::Filter::try_new(self.extra_filter.clone(), Arc::new(node))?;
                self.is_rewritten = true;
                Ok(Transformed::yes(LogicalPlan::Filter(filter)))
            }
            _ => Ok(Transformed::no(node)),
        }
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use datafusion_common::tree_node::TreeNode as _;
    use datatypes::prelude::ConcreteDataType;
    use datatypes::schema::{ColumnSchema, Schema};
    use pretty_assertions::assert_eq;
    use query::query_engine::DefaultSerializer;
    use session::context::QueryContext;
    use substrait::{DFLogicalSubstraitConvertor, SubstraitPlan};

    use super::*;
    use crate::test_utils::create_test_query_engine;

    /// test if uppercase are handled correctly(with quote)
    #[tokio::test]
    async fn test_sql_plan_convert() {
        let query_engine = create_test_query_engine();
        let ctx = QueryContext::arc();
        let old = r#"SELECT "NUMBER" FROM "UPPERCASE_NUMBERS_WITH_TS""#;
        let new = sql_to_df_plan(ctx.clone(), query_engine.clone(), old, false)
            .await
            .unwrap();
        let new_sql = df_plan_to_sql(&new).unwrap();

        assert_eq!(
            r#"SELECT `UPPERCASE_NUMBERS_WITH_TS`.`NUMBER` FROM `UPPERCASE_NUMBERS_WITH_TS`"#,
            new_sql
        );
    }

    #[tokio::test]
    async fn test_add_filter() {
        let testcases = vec![
            (
                "SELECT number FROM numbers_with_ts GROUP BY number",
                "SELECT numbers_with_ts.number FROM numbers_with_ts WHERE (number > 4) GROUP BY numbers_with_ts.number"
            ),

            (
                "SELECT number FROM numbers_with_ts WHERE number < 2 OR number >10",
                "SELECT numbers_with_ts.number FROM numbers_with_ts WHERE ((numbers_with_ts.number < 2) OR (numbers_with_ts.number > 10)) AND (number > 4)"
            ),

            (
                "SELECT date_bin('5 minutes', ts) as time_window FROM numbers_with_ts GROUP BY time_window",
                "SELECT date_bin('5 minutes', numbers_with_ts.ts) AS time_window FROM numbers_with_ts WHERE (number > 4) GROUP BY date_bin('5 minutes', numbers_with_ts.ts)"
            ),

            // subquery
            (
                "SELECT number, time_window FROM (SELECT number, date_bin('5 minutes', ts) as time_window FROM numbers_with_ts GROUP BY time_window, number);", 
                "SELECT numbers_with_ts.number, time_window FROM (SELECT numbers_with_ts.number, date_bin('5 minutes', numbers_with_ts.ts) AS time_window FROM numbers_with_ts WHERE (number > 4) GROUP BY date_bin('5 minutes', numbers_with_ts.ts), numbers_with_ts.number)"
            ),

            // complex subquery without alias
            (
                "SELECT sum(number), number, date_bin('5 minutes', ts) as time_window, bucket_name FROM (SELECT number, ts, case when number < 5 THEN 'bucket_0_5' when number >= 5 THEN 'bucket_5_inf' END as bucket_name FROM numbers_with_ts) GROUP BY number, time_window, bucket_name;",
                "SELECT sum(numbers_with_ts.number), numbers_with_ts.number, date_bin('5 minutes', numbers_with_ts.ts) AS time_window, bucket_name FROM (SELECT numbers_with_ts.number, numbers_with_ts.ts, CASE WHEN (numbers_with_ts.number < 5) THEN 'bucket_0_5' WHEN (numbers_with_ts.number >= 5) THEN 'bucket_5_inf' END AS bucket_name FROM numbers_with_ts WHERE (number > 4)) GROUP BY numbers_with_ts.number, date_bin('5 minutes', numbers_with_ts.ts), bucket_name"
            ),

            // complex subquery alias
            (
                "SELECT sum(number), number, date_bin('5 minutes', ts) as time_window, bucket_name FROM (SELECT number, ts, case when number < 5 THEN 'bucket_0_5' when number >= 5 THEN 'bucket_5_inf' END as bucket_name FROM numbers_with_ts) as cte WHERE number > 1 GROUP BY number, time_window, bucket_name;",
                "SELECT sum(cte.number), cte.number, date_bin('5 minutes', cte.ts) AS time_window, cte.bucket_name FROM (SELECT numbers_with_ts.number, numbers_with_ts.ts, CASE WHEN (numbers_with_ts.number < 5) THEN 'bucket_0_5' WHEN (numbers_with_ts.number >= 5) THEN 'bucket_5_inf' END AS bucket_name FROM numbers_with_ts WHERE (number > 4)) AS cte WHERE (cte.number > 1) GROUP BY cte.number, date_bin('5 minutes', cte.ts), cte.bucket_name"
            )
        ];
        use datafusion_expr::{col, lit};
        let query_engine = create_test_query_engine();
        let ctx = QueryContext::arc();

        for (before, after) in testcases {
            let sql = before;
            let plan = sql_to_df_plan(ctx.clone(), query_engine.clone(), sql, false)
                .await
                .unwrap();

            let mut add_filter = AddFilterRewriter::new(col("number").gt(lit(4u32)));
            let plan = plan.rewrite(&mut add_filter).unwrap().data;
            let new_sql = df_plan_to_sql(&plan).unwrap();
            assert_eq!(after, new_sql);
        }
    }

    #[tokio::test]
    async fn test_add_auto_column_rewriter() {
        let testcases = vec![
            // add update_at
            (
                "SELECT number FROM numbers_with_ts",
                Ok("SELECT numbers_with_ts.number, now() AS ts FROM numbers_with_ts"),
                vec![
                    ColumnSchema::new("number", ConcreteDataType::int32_datatype(), true),
                    ColumnSchema::new(
                        "ts",
                        ConcreteDataType::timestamp_millisecond_datatype(),
                        false,
                    )
                    .with_time_index(true),
                ],
            ),
            // add ts placeholder
            (
                "SELECT number FROM numbers_with_ts",
                Ok("SELECT numbers_with_ts.number, CAST('1970-01-01 00:00:00' AS TIMESTAMP) AS __ts_placeholder FROM numbers_with_ts"),
                vec![
                    ColumnSchema::new("number", ConcreteDataType::int32_datatype(), true),
                    ColumnSchema::new(
                        AUTO_CREATED_PLACEHOLDER_TS_COL,
                        ConcreteDataType::timestamp_millisecond_datatype(),
                        false,
                    )
                    .with_time_index(true),
                ],
            ),
            // no modify
            (
                "SELECT number, ts FROM numbers_with_ts",
                Ok("SELECT numbers_with_ts.number, numbers_with_ts.ts FROM numbers_with_ts"),
                vec![
                    ColumnSchema::new("number", ConcreteDataType::int32_datatype(), true),
                    ColumnSchema::new(
                        "ts",
                        ConcreteDataType::timestamp_millisecond_datatype(),
                        false,
                    )
                    .with_time_index(true),
                ],
            ),
            // add update_at and ts placeholder
            (
                "SELECT number FROM numbers_with_ts",
                Ok("SELECT numbers_with_ts.number, now() AS update_at, CAST('1970-01-01 00:00:00' AS TIMESTAMP) AS __ts_placeholder FROM numbers_with_ts"),
                vec![
                    ColumnSchema::new("number", ConcreteDataType::int32_datatype(), true),
                    ColumnSchema::new(
                        "update_at",
                        ConcreteDataType::timestamp_millisecond_datatype(),
                        false,
                    ),
                    ColumnSchema::new(
                        AUTO_CREATED_PLACEHOLDER_TS_COL,
                        ConcreteDataType::timestamp_millisecond_datatype(),
                        false,
                    )
                    .with_time_index(true),
                ],
            ),
            // add ts placeholder
            (
                "SELECT number, ts FROM numbers_with_ts",
                Ok("SELECT numbers_with_ts.number, numbers_with_ts.ts AS update_at, CAST('1970-01-01 00:00:00' AS TIMESTAMP) AS __ts_placeholder FROM numbers_with_ts"),
                vec![
                    ColumnSchema::new("number", ConcreteDataType::int32_datatype(), true),
                    ColumnSchema::new(
                        "update_at",
                        ConcreteDataType::timestamp_millisecond_datatype(),
                        false,
                    ),
                    ColumnSchema::new(
                        AUTO_CREATED_PLACEHOLDER_TS_COL,
                        ConcreteDataType::timestamp_millisecond_datatype(),
                        false,
                    )
                    .with_time_index(true),
                ],
            ),
            // add update_at after time index column
            (
                "SELECT number, ts FROM numbers_with_ts",
                Ok("SELECT numbers_with_ts.number, numbers_with_ts.ts, now() AS update_atat FROM numbers_with_ts"),
                vec![
                    ColumnSchema::new("number", ConcreteDataType::int32_datatype(), true),
                    ColumnSchema::new(
                        "ts",
                        ConcreteDataType::timestamp_millisecond_datatype(),
                        false,
                    )
                    .with_time_index(true),
                    ColumnSchema::new(
                        // name is irrelevant for update_at column
                        "update_atat",
                        ConcreteDataType::timestamp_millisecond_datatype(),
                        false,
                    ),
                ],
            ),
            // error datatype mismatch
            (
                "SELECT number, ts FROM numbers_with_ts",
                Err("Expect the last column in table to be timestamp column, found column atat with type Int8"),
                vec![
                    ColumnSchema::new("number", ConcreteDataType::int32_datatype(), true),
                    ColumnSchema::new(
                        "ts",
                        ConcreteDataType::timestamp_millisecond_datatype(),
                        false,
                    )
                    .with_time_index(true),
                    ColumnSchema::new(
                        // name is irrelevant for update_at column
                        "atat",
                        ConcreteDataType::int8_datatype(),
                        false,
                    ),
                ],
            ),
            // error datatype mismatch on second last column
            (
                "SELECT number FROM numbers_with_ts",
                Err("Expect the second last column in the table to be timestamp column, found column ts with type Int8"),
                vec![
                    ColumnSchema::new("number", ConcreteDataType::int32_datatype(), true),
                    ColumnSchema::new(
                        "ts",
                        ConcreteDataType::int8_datatype(),
                        false,
                    ),
                    ColumnSchema::new(
                        // name is irrelevant for update_at column
                        "atat",
                        ConcreteDataType::timestamp_millisecond_datatype(),
                        false,
                    )
                    .with_time_index(true),
                ],
            ),
        ];

        let query_engine = create_test_query_engine();
        let ctx = QueryContext::arc();
        for (before, after, column_schemas) in testcases {
            let schema = Arc::new(Schema::new(column_schemas));
            let mut add_auto_column_rewriter = AddAutoColumnRewriter::new(schema);

            let plan = sql_to_df_plan(ctx.clone(), query_engine.clone(), before, false)
                .await
                .unwrap();
            let new_sql = (|| {
                let plan = plan
                    .rewrite(&mut add_auto_column_rewriter)
                    .map_err(|e| e.to_string())?
                    .data;
                df_plan_to_sql(&plan).map_err(|e| e.to_string())
            })();
            match (after, new_sql.clone()) {
                (Ok(after), Ok(new_sql)) => assert_eq!(after, new_sql),
                (Err(expected), Err(real_err_msg)) => assert!(
                    real_err_msg.contains(expected),
                    "expected: {expected}, real: {real_err_msg}"
                ),
                _ => panic!("expected: {:?}, real: {:?}", after, new_sql),
            }
        }
    }

    #[tokio::test]
    async fn test_find_group_by_exprs() {
        let testcases = vec![
            (
                "SELECT arrow_cast(date_bin(INTERVAL '1 MINS', numbers_with_ts.ts), 'Timestamp(Second, None)') AS ts FROM numbers_with_ts GROUP BY ts;", 
                vec!["ts"]
            ),
            (
                "SELECT number FROM numbers_with_ts GROUP BY number",
                vec!["number"]
            ),
            (
                "SELECT date_bin('5 minutes', ts) as time_window FROM numbers_with_ts GROUP BY time_window",
                vec!["time_window"]
            ),
             // subquery
            (
                "SELECT number, time_window FROM (SELECT number, date_bin('5 minutes', ts) as time_window FROM numbers_with_ts GROUP BY time_window, number);", 
                vec!["time_window", "number"]
            ),
            // complex subquery without alias
            (
                "SELECT sum(number), number, date_bin('5 minutes', ts) as time_window, bucket_name FROM (SELECT number, ts, case when number < 5 THEN 'bucket_0_5' when number >= 5 THEN 'bucket_5_inf' END as bucket_name FROM numbers_with_ts) GROUP BY number, time_window, bucket_name;",
                vec!["number", "time_window", "bucket_name"]
            ),
            // complex subquery alias
            (
                "SELECT sum(number), number, date_bin('5 minutes', ts) as time_window, bucket_name FROM (SELECT number, ts, case when number < 5 THEN 'bucket_0_5' when number >= 5 THEN 'bucket_5_inf' END as bucket_name FROM numbers_with_ts) as cte GROUP BY number, time_window, bucket_name;",
                vec!["number", "time_window", "bucket_name"]
            )
        ];

        let query_engine = create_test_query_engine();
        let ctx = QueryContext::arc();
        for (sql, expected) in testcases {
            // need to be unoptimize for better readiability
            let plan = sql_to_df_plan(ctx.clone(), query_engine.clone(), sql, false)
                .await
                .unwrap();
            let mut group_by_exprs = FindGroupByFinalName::default();
            plan.visit(&mut group_by_exprs).unwrap();
            let expected: HashSet<String> = expected.into_iter().map(|s| s.to_string()).collect();
            assert_eq!(
                expected,
                group_by_exprs.get_group_expr_names().unwrap_or_default()
            );
        }
    }

    #[tokio::test]
    async fn test_null_cast() {
        let query_engine = create_test_query_engine();
        let ctx = QueryContext::arc();
        let sql = "SELECT NULL::DOUBLE FROM numbers_with_ts";
        let plan = sql_to_df_plan(ctx, query_engine.clone(), sql, false)
            .await
            .unwrap();

        let _sub_plan = DFLogicalSubstraitConvertor {}
            .encode(&plan, DefaultSerializer)
            .unwrap();
    }
}
