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

//! some utils for helping with recording rule

use std::collections::HashSet;
use std::sync::Arc;

use common_error::ext::BoxedError;
use common_telemetry::{debug, info};
use datafusion::error::Result as DfResult;
use datafusion::logical_expr::Expr;
use datafusion::sql::unparser::Unparser;
use datafusion_common::tree_node::{
    Transformed, TreeNodeRecursion, TreeNodeRewriter, TreeNodeVisitor,
};
use datafusion_common::DataFusionError;
use datafusion_expr::{Distinct, LogicalPlan};
use datatypes::schema::RawSchema;
use query::parser::QueryLanguageParser;
use query::QueryEngineRef;
use session::context::QueryContextRef;
use snafu::ResultExt;

use crate::adapter::AUTO_CREATED_PLACEHOLDER_TS_COL;
use crate::df_optimizer::apply_df_optimizer;
use crate::error::{DatafusionSnafu, ExternalSnafu};
use crate::Error;

/// Convert sql to datafusion logical plan
pub async fn sql_to_df_plan(
    query_ctx: QueryContextRef,
    engine: QueryEngineRef,
    sql: &str,
    optimize: bool,
) -> Result<LogicalPlan, Error> {
    let stmt = QueryLanguageParser::parse_sql(sql, &query_ctx)
        .map_err(BoxedError::new)
        .context(ExternalSnafu)?;
    let plan = engine
        .planner()
        .plan(&stmt, query_ctx)
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
    /// A dialect that forces all identifiers to be quoted
    struct ForceQuoteIdentifiers;
    impl datafusion::sql::unparser::dialect::Dialect for ForceQuoteIdentifiers {
        fn identifier_quote_style(&self, identifier: &str) -> Option<char> {
            if identifier.to_lowercase() != identifier {
                Some('"')
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
            debug!("Group by exprs: {:?}", self.group_exprs);
        } else if let LogicalPlan::Distinct(distinct) = node {
            debug!("Distinct: {:#?}", distinct);
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
            debug!("Group by exprs: {:?}", self.group_exprs);
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

/// Add to the final select columns like `update_at` and `__ts_placeholder` with values like `now()` and `0`
#[derive(Debug)]
pub struct AddAutoColumnRewriter {
    pub schema: RawSchema,
    pub is_rewritten: bool,
}

impl AddAutoColumnRewriter {
    pub fn new(schema: RawSchema) -> Self {
        Self {
            schema,
            is_rewritten: false,
        }
    }
}

impl TreeNodeRewriter for AddAutoColumnRewriter {
    type Node = LogicalPlan;
    fn f_down(&mut self, node: Self::Node) -> DfResult<Transformed<Self::Node>> {
        if self.is_rewritten {
            return Ok(Transformed::no(node));
        }

        // if is distinct all, go one level down
        if let LogicalPlan::Distinct(Distinct::All(_)) = node {
            return Ok(Transformed::no(node));
        }

        // FIXME(discord9): just read plan.expr and do stuffs
        let mut exprs = node.expressions();

        // add columns if have different column count
        let query_col_cnt = exprs.len();
        let table_col_cnt = self.schema.column_schemas.len();
        info!("query_col_cnt={query_col_cnt}, table_col_cnt={table_col_cnt}");
        if query_col_cnt == table_col_cnt {
            self.is_rewritten = true;
            return Ok(Transformed::no(node));
        } else if query_col_cnt + 1 == table_col_cnt {
            let last_col_schema = self.schema.column_schemas.last().unwrap();

            // if time index column is auto created add it
            if last_col_schema.name == AUTO_CREATED_PLACEHOLDER_TS_COL
                && self.schema.timestamp_index == Some(table_col_cnt - 1)
            {
                exprs.push(datafusion::logical_expr::lit(0));
            } else if last_col_schema.data_type.is_timestamp() {
                // is the update at column
                exprs.push(datafusion::prelude::now());
            } else {
                // helpful error message
                return Err(DataFusionError::Plan(format!(
                    "Expect timestamp column, found {:?}",
                    last_col_schema.data_type
                )));
            }
        } else if query_col_cnt + 2 == table_col_cnt {
            let mut col_iter = self.schema.column_schemas.iter().rev();
            let last_col_schema = col_iter.next().unwrap();
            let second_last_col_schema = col_iter.next().unwrap();
            if second_last_col_schema.data_type.is_timestamp() {
                exprs.push(datafusion::prelude::now());
            } else {
                return Err(DataFusionError::Plan(format!(
                    "Expect timestamp column, found {:?} at column {:?}",
                    second_last_col_schema.data_type, second_last_col_schema.name
                )));
            }

            if last_col_schema.name == AUTO_CREATED_PLACEHOLDER_TS_COL
                && self.schema.timestamp_index == Some(table_col_cnt - 1)
            {
                exprs.push(datafusion::logical_expr::lit(0));
            } else {
                return Err(DataFusionError::Plan(format!(
                    "Expect timestamp column {}, found {:?}",
                    AUTO_CREATED_PLACEHOLDER_TS_COL, last_col_schema
                )));
            }
        } else {
            return Err(DataFusionError::Plan(format!(
                    "Expect table have 0,1 or 2 columns more than query columns, found {} query columns {:?}, {} table columns {:?}",
                    query_col_cnt, node.expressions(), table_col_cnt, self.schema.column_schemas
                )));
        }

        self.is_rewritten = true;
        let new_plan = node.with_new_exprs(exprs, node.inputs().into_iter().cloned().collect())?;
        Ok(Transformed::yes(new_plan))
    }
}

// TODO(discord9): a method to found out the precise time window

/// Find out the `Filter` Node corresponding to outermost `WHERE` and add a new filter expr to it
#[derive(Debug)]
pub struct AddFilterRewriter {
    extra_filter: Expr,
    is_rewritten: bool,
}

impl AddFilterRewriter {
    fn new(filter: Expr) -> Self {
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
            LogicalPlan::Filter(mut filter) if !filter.having => {
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
    use datafusion_common::tree_node::TreeNode as _;
    use datatypes::prelude::ConcreteDataType;
    use datatypes::schema::ColumnSchema;
    use pretty_assertions::assert_eq;
    use session::context::QueryContext;

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
            r#"SELECT "UPPERCASE_NUMBERS_WITH_TS"."NUMBER" FROM "UPPERCASE_NUMBERS_WITH_TS""#,
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
                "SELECT sum(number), number, date_bin('5 minutes', ts) as time_window, bucket_name FROM (SELECT number, ts, case when number < 5 THEN 'bucket_0_5' when number >= 5 THEN 'bucket_5_inf' END as bucket_name FROM numbers_with_ts) as cte GROUP BY number, time_window, bucket_name;",
                "SELECT sum(cte.number), cte.number, date_bin('5 minutes', cte.ts) AS time_window, cte.bucket_name FROM (SELECT numbers_with_ts.number, numbers_with_ts.ts, CASE WHEN (numbers_with_ts.number < 5) THEN 'bucket_0_5' WHEN (numbers_with_ts.number >= 5) THEN 'bucket_5_inf' END AS bucket_name FROM numbers_with_ts WHERE (number > 4)) AS cte GROUP BY cte.number, date_bin('5 minutes', cte.ts), cte.bucket_name"
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
                Ok("SELECT numbers_with_ts.number, now() FROM numbers_with_ts"),
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
                Ok("SELECT numbers_with_ts.number, 0 FROM numbers_with_ts"),
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
                Ok("SELECT numbers_with_ts.number, now(), 0 FROM numbers_with_ts"),
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
                Ok("SELECT numbers_with_ts.number, numbers_with_ts.ts, 0 FROM numbers_with_ts"),
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
                Ok("SELECT numbers_with_ts.number, numbers_with_ts.ts, now() FROM numbers_with_ts"),
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
                Err("Expect timestamp"),
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
        ];

        let query_engine = create_test_query_engine();
        let ctx = QueryContext::arc();
        for (before, after, column_schemas) in testcases {
            let raw_schema = RawSchema::new(column_schemas);
            let mut add_auto_column_rewriter = AddAutoColumnRewriter::new(raw_schema);

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
}
