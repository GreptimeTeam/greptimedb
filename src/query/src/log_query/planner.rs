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

use catalog::table_source::DfTableSourceProvider;
use common_function::utils::escape_like_pattern;
use datafusion::datasource::DefaultTableSource;
use datafusion::execution::SessionState;
use datafusion_common::{DFSchema, ScalarValue};
use datafusion_expr::utils::conjunction;
use datafusion_expr::{col, lit, Expr, LogicalPlan, LogicalPlanBuilder};
use datafusion_sql::TableReference;
use datatypes::schema::Schema;
use log_query::{ColumnFilters, LogExpr, LogQuery, TimeFilter};
use snafu::{OptionExt, ResultExt};
use table::table::adapter::DfTableProviderAdapter;

use crate::log_query::error::{
    CatalogSnafu, DataFusionPlanningSnafu, Result, TimeIndexNotFoundSnafu, UnexpectedLogExprSnafu,
    UnimplementedSnafu, UnknownAggregateFunctionSnafu, UnknownScalarFunctionSnafu,
    UnknownTableSnafu,
};

const DEFAULT_LIMIT: usize = 1000;

pub struct LogQueryPlanner {
    table_provider: DfTableSourceProvider,
    session_state: SessionState,
}

impl LogQueryPlanner {
    pub fn new(table_provider: DfTableSourceProvider, session_state: SessionState) -> Self {
        Self {
            table_provider,
            session_state,
        }
    }

    pub async fn query_to_plan(&mut self, query: LogQuery) -> Result<LogicalPlan> {
        // Resolve table
        let table_ref: TableReference = query.table.table_ref().into();
        let table_source = self
            .table_provider
            .resolve_table(table_ref.clone())
            .await
            .context(CatalogSnafu)?;
        let schema = table_source
            .as_any()
            .downcast_ref::<DefaultTableSource>()
            .context(UnknownTableSnafu)?
            .table_provider
            .as_any()
            .downcast_ref::<DfTableProviderAdapter>()
            .context(UnknownTableSnafu)?
            .table()
            .schema();

        // Build the initial scan plan
        let mut plan_builder = LogicalPlanBuilder::scan(table_ref, table_source, None)
            .context(DataFusionPlanningSnafu)?;

        // Collect filter expressions
        let mut filters = Vec::new();

        // Time filter
        filters.push(self.build_time_filter(&query.time_filter, &schema)?);

        // Column filters
        for column_filter in &query.filters {
            if let Some(expr) = self.build_column_filter(column_filter)? {
                filters.push(expr);
            }
        }

        // Apply filters
        if !filters.is_empty() {
            let filter_expr = filters.into_iter().reduce(|a, b| a.and(b)).unwrap();
            plan_builder = plan_builder
                .filter(filter_expr)
                .context(DataFusionPlanningSnafu)?;
        }

        // Apply projections
        if !query.columns.is_empty() {
            let projected_columns = query.columns.iter().map(col).collect::<Vec<_>>();
            plan_builder = plan_builder
                .project(projected_columns)
                .context(DataFusionPlanningSnafu)?;
        }

        // Apply limit
        plan_builder = plan_builder
            .limit(
                query.limit.skip.unwrap_or(0),
                Some(query.limit.fetch.unwrap_or(DEFAULT_LIMIT)),
            )
            .context(DataFusionPlanningSnafu)?;

        // Apply log expressions
        for expr in &query.exprs {
            plan_builder = self.process_log_expr(plan_builder, expr)?;
        }

        // Build the final plan
        let plan = plan_builder.build().context(DataFusionPlanningSnafu)?;

        Ok(plan)
    }

    fn build_time_filter(&self, time_filter: &TimeFilter, schema: &Schema) -> Result<Expr> {
        let timestamp_col = schema
            .timestamp_column()
            .with_context(|| TimeIndexNotFoundSnafu {})?
            .name
            .clone();

        let start_time = ScalarValue::Utf8(time_filter.start.clone());
        let end_time = ScalarValue::Utf8(
            time_filter
                .end
                .clone()
                .or(Some("9999-12-31T23:59:59Z".to_string())),
        );
        let expr = col(timestamp_col.clone())
            .gt_eq(lit(start_time))
            .and(col(timestamp_col).lt_eq(lit(end_time)));

        Ok(expr)
    }

    /// Returns filter expressions
    fn build_column_filter(&self, column_filter: &ColumnFilters) -> Result<Option<Expr>> {
        if column_filter.filters.is_empty() {
            return Ok(None);
        }

        self.build_content_filters(&column_filter.column_name, &column_filter.filters)
    }

    /// Builds filter expressions from content filters for a specific column
    fn build_content_filters(
        &self,
        column_name: &str,
        filters: &[log_query::ContentFilter],
    ) -> Result<Option<Expr>> {
        if filters.is_empty() {
            return Ok(None);
        }

        let exprs = filters
            .iter()
            .map(|filter| self.build_content_filter(column_name, filter))
            .try_collect::<Vec<_>>()?;

        Ok(conjunction(exprs))
    }

    /// Builds a single content filter expression
    #[allow(clippy::only_used_in_recursion)]
    fn build_content_filter(
        &self,
        column_name: &str,
        filter: &log_query::ContentFilter,
    ) -> Result<Expr> {
        match filter {
            log_query::ContentFilter::Exact(pattern) => {
                Ok(col(column_name)
                    .like(lit(ScalarValue::Utf8(Some(escape_like_pattern(pattern))))))
            }
            log_query::ContentFilter::Prefix(pattern) => Ok(col(column_name).like(lit(
                ScalarValue::Utf8(Some(format!("{}%", escape_like_pattern(pattern)))),
            ))),
            log_query::ContentFilter::Postfix(pattern) => Ok(col(column_name).like(lit(
                ScalarValue::Utf8(Some(format!("%{}", escape_like_pattern(pattern)))),
            ))),
            log_query::ContentFilter::Contains(pattern) => Ok(col(column_name).like(lit(
                ScalarValue::Utf8(Some(format!("%{}%", escape_like_pattern(pattern)))),
            ))),
            log_query::ContentFilter::Regex(..) => Err::<Expr, _>(
                UnimplementedSnafu {
                    feature: "regex filter",
                }
                .build(),
            ),
            log_query::ContentFilter::Exist => Ok(col(column_name).is_not_null()),
            log_query::ContentFilter::Between {
                start,
                end,
                start_inclusive,
                end_inclusive,
            } => {
                let left = if *start_inclusive {
                    Expr::gt_eq
                } else {
                    Expr::gt
                };
                let right = if *end_inclusive {
                    Expr::lt_eq
                } else {
                    Expr::lt
                };
                Ok(left(
                    col(column_name),
                    lit(ScalarValue::Utf8(Some(escape_like_pattern(start)))),
                )
                .and(right(
                    col(column_name),
                    lit(ScalarValue::Utf8(Some(escape_like_pattern(end)))),
                )))
            }
            log_query::ContentFilter::GreatThan { value, inclusive } => {
                let expr = if *inclusive { Expr::gt_eq } else { Expr::gt };
                Ok(expr(
                    col(column_name),
                    lit(ScalarValue::Utf8(Some(escape_like_pattern(value)))),
                ))
            }
            log_query::ContentFilter::LessThan { value, inclusive } => {
                let expr = if *inclusive { Expr::lt_eq } else { Expr::lt };
                Ok(expr(
                    col(column_name),
                    lit(ScalarValue::Utf8(Some(escape_like_pattern(value)))),
                ))
            }
            log_query::ContentFilter::In(values) => {
                let list = values
                    .iter()
                    .map(|value| lit(ScalarValue::Utf8(Some(escape_like_pattern(value)))))
                    .collect();
                Ok(col(column_name).in_list(list, false))
            }
            log_query::ContentFilter::Compound(filters, op) => {
                let exprs = filters
                    .iter()
                    .map(|filter| self.build_content_filter(column_name, filter))
                    .try_collect::<Vec<_>>()?;

                match op {
                    log_query::BinaryOperator::And => Ok(conjunction(exprs).unwrap()),
                    log_query::BinaryOperator::Or => {
                        // Build a disjunction (OR) of expressions
                        Ok(exprs.into_iter().reduce(|a, b| a.or(b)).unwrap())
                    }
                }
            }
        }
    }

    fn build_aggr_func(
        &self,
        schema: &DFSchema,
        fn_name: &str,
        args: &[LogExpr],
        by: &[LogExpr],
    ) -> Result<(Expr, Vec<Expr>)> {
        let aggr_fn = self
            .session_state
            .aggregate_functions()
            .get(fn_name)
            .context(UnknownAggregateFunctionSnafu {
                name: fn_name.to_string(),
            })?;
        let args = args
            .iter()
            .map(|expr| self.log_expr_to_column_expr(expr, schema))
            .try_collect::<Vec<_>>()?;
        let group_exprs = by
            .iter()
            .map(|expr| self.log_expr_to_column_expr(expr, schema))
            .try_collect::<Vec<_>>()?;
        let aggr_expr = aggr_fn.call(args);

        Ok((aggr_expr, group_exprs))
    }

    /// Converts a log expression to a column expression.
    ///
    /// A column expression here can be a column identifier, a positional identifier, or a literal.
    /// They don't rely on the context of the query or other columns.
    fn log_expr_to_column_expr(&self, expr: &LogExpr, schema: &DFSchema) -> Result<Expr> {
        match expr {
            LogExpr::NamedIdent(name) => Ok(col(name)),
            LogExpr::PositionalIdent(index) => Ok(col(schema.field(*index).name())),
            LogExpr::Literal(literal) => Ok(lit(ScalarValue::Utf8(Some(literal.clone())))),
            _ => UnexpectedLogExprSnafu {
                expr: expr.clone(),
                expected: "named identifier, positional identifier, or literal",
            }
            .fail(),
        }
    }

    fn build_scalar_func(&self, schema: &DFSchema, name: &str, args: &[LogExpr]) -> Result<Expr> {
        let args = args
            .iter()
            .map(|expr| self.log_expr_to_column_expr(expr, schema))
            .try_collect::<Vec<_>>()?;
        let func = self.session_state.scalar_functions().get(name).context(
            UnknownScalarFunctionSnafu {
                name: name.to_string(),
            },
        )?;
        let expr = func.call(args);

        Ok(expr)
    }

    /// Process LogExpr recursively.
    ///
    /// Return the [`LogicalPlanBuilder`] after modification and the resulting expression's names.
    fn process_log_expr(
        &self,
        plan_builder: LogicalPlanBuilder,
        expr: &LogExpr,
    ) -> Result<LogicalPlanBuilder> {
        let mut plan_builder = plan_builder;

        match expr {
            LogExpr::AggrFunc {
                name,
                args,
                by,
                range: _range,
                alias,
            } => {
                let schema = plan_builder.schema();
                let (mut aggr_expr, group_exprs) = self.build_aggr_func(schema, name, args, by)?;
                if let Some(alias) = alias {
                    aggr_expr = aggr_expr.alias(alias);
                }

                plan_builder = plan_builder
                    .aggregate(group_exprs, [aggr_expr.clone()])
                    .context(DataFusionPlanningSnafu)?;
            }
            LogExpr::Filter { expr, filter } => {
                let schema = plan_builder.schema();
                let expr = self.log_expr_to_column_expr(expr, schema)?;

                let col_name = expr.schema_name().to_string();
                let filter_expr = self.build_content_filter(&col_name, filter)?;
                plan_builder = plan_builder
                    .filter(filter_expr)
                    .context(DataFusionPlanningSnafu)?;
            }
            LogExpr::ScalarFunc { name, args, alias } => {
                let schema = plan_builder.schema();
                let mut expr = self.build_scalar_func(schema, name, args)?;
                if let Some(alias) = alias {
                    expr = expr.alias(alias);
                }
                plan_builder = plan_builder
                    .project([expr.clone()])
                    .context(DataFusionPlanningSnafu)?;
            }
            LogExpr::NamedIdent(_) | LogExpr::PositionalIdent(_) => {
                // nothing to do, return empty vec.
            }
            LogExpr::Alias { expr, alias } => {
                let expr = self.log_expr_to_column_expr(expr, plan_builder.schema())?;
                let aliased_expr = expr.alias(alias);
                plan_builder = plan_builder
                    .project([aliased_expr.clone()])
                    .context(DataFusionPlanningSnafu)?;
            }
            _ => {
                UnimplementedSnafu {
                    feature: "log expression",
                }
                .fail()?;
            }
        }
        Ok(plan_builder)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use catalog::memory::MemoryCatalogManager;
    use catalog::RegisterTableRequest;
    use common_catalog::consts::DEFAULT_CATALOG_NAME;
    use common_query::test_util::DummyDecoder;
    use datafusion::execution::SessionStateBuilder;
    use datatypes::prelude::ConcreteDataType;
    use datatypes::schema::{ColumnSchema, SchemaRef};
    use log_query::{BinaryOperator, ContentFilter, Context, Limit};
    use session::context::QueryContext;
    use table::metadata::{TableInfoBuilder, TableMetaBuilder};
    use table::table_name::TableName;
    use table::test_util::EmptyTable;

    use super::*;

    fn mock_schema() -> SchemaRef {
        let columns = vec![
            ColumnSchema::new(
                "message".to_string(),
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
                "host".to_string(),
                ConcreteDataType::string_datatype(),
                true,
            ),
        ];

        Arc::new(Schema::new(columns))
    }

    /// Registers table under `greptime`, with `message` and `timestamp` and `host` columns.
    async fn build_test_table_provider(
        table_name_tuples: &[(String, String)],
    ) -> DfTableSourceProvider {
        let catalog_list = MemoryCatalogManager::with_default_setup();
        for (schema_name, table_name) in table_name_tuples {
            let schema = mock_schema();
            let table_meta = TableMetaBuilder::default()
                .schema(schema)
                .primary_key_indices(vec![2])
                .value_indices(vec![0])
                .next_column_id(1024)
                .build()
                .unwrap();
            let table_info = TableInfoBuilder::default()
                .name(table_name.to_string())
                .meta(table_meta)
                .build()
                .unwrap();
            let table = EmptyTable::from_table_info(&table_info);

            catalog_list
                .register_table_sync(RegisterTableRequest {
                    catalog: DEFAULT_CATALOG_NAME.to_string(),
                    schema: schema_name.to_string(),
                    table_name: table_name.to_string(),
                    table_id: 1024,
                    table,
                })
                .unwrap();
        }

        DfTableSourceProvider::new(
            catalog_list,
            false,
            QueryContext::arc(),
            DummyDecoder::arc(),
            false,
        )
    }

    #[tokio::test]
    async fn test_query_to_plan() {
        let table_provider =
            build_test_table_provider(&[("public".to_string(), "test_table".to_string())]).await;
        let session_state = SessionStateBuilder::new().with_default_features().build();
        let mut planner = LogQueryPlanner::new(table_provider, session_state);

        let log_query = LogQuery {
            table: TableName::new(DEFAULT_CATALOG_NAME, "public", "test_table"),
            time_filter: TimeFilter {
                start: Some("2021-01-01T00:00:00Z".to_string()),
                end: Some("2021-01-02T00:00:00Z".to_string()),
                span: None,
            },
            filters: vec![ColumnFilters {
                column_name: "message".to_string(),
                filters: vec![ContentFilter::Contains("error".to_string())],
            }],
            limit: Limit {
                skip: None,
                fetch: Some(100),
            },
            context: Context::None,
            columns: vec![],
            exprs: vec![],
        };

        let plan = planner.query_to_plan(log_query).await.unwrap();
        let expected = "Limit: skip=0, fetch=100 [message:Utf8, timestamp:Timestamp(Millisecond, None), host:Utf8;N]\
\n  Filter: greptime.public.test_table.timestamp >= Utf8(\"2021-01-01T00:00:00Z\") AND greptime.public.test_table.timestamp <= Utf8(\"2021-01-02T00:00:00Z\") AND greptime.public.test_table.message LIKE Utf8(\"%error%\") [message:Utf8, timestamp:Timestamp(Millisecond, None), host:Utf8;N]\
\n    TableScan: greptime.public.test_table [message:Utf8, timestamp:Timestamp(Millisecond, None), host:Utf8;N]";

        assert_eq!(plan.display_indent_schema().to_string(), expected);
    }

    #[tokio::test]
    async fn test_build_time_filter() {
        let table_provider =
            build_test_table_provider(&[("public".to_string(), "test_table".to_string())]).await;
        let session_state = SessionStateBuilder::new().with_default_features().build();
        let planner = LogQueryPlanner::new(table_provider, session_state);

        let time_filter = TimeFilter {
            start: Some("2021-01-01T00:00:00Z".to_string()),
            end: Some("2021-01-02T00:00:00Z".to_string()),
            span: None,
        };

        let expr = planner
            .build_time_filter(&time_filter, &mock_schema())
            .unwrap();

        let expected_expr = col("timestamp")
            .gt_eq(lit(ScalarValue::Utf8(Some(
                "2021-01-01T00:00:00Z".to_string(),
            ))))
            .and(col("timestamp").lt_eq(lit(ScalarValue::Utf8(Some(
                "2021-01-02T00:00:00Z".to_string(),
            )))));

        assert_eq!(format!("{:?}", expr), format!("{:?}", expected_expr));
    }

    #[tokio::test]
    async fn test_build_time_filter_without_end() {
        let table_provider =
            build_test_table_provider(&[("public".to_string(), "test_table".to_string())]).await;
        let session_state = SessionStateBuilder::new().with_default_features().build();
        let planner = LogQueryPlanner::new(table_provider, session_state);

        let time_filter = TimeFilter {
            start: Some("2021-01-01T00:00:00Z".to_string()),
            end: None,
            span: None,
        };

        let expr = planner
            .build_time_filter(&time_filter, &mock_schema())
            .unwrap();

        let expected_expr = col("timestamp")
            .gt_eq(lit(ScalarValue::Utf8(Some(
                "2021-01-01T00:00:00Z".to_string(),
            ))))
            .and(col("timestamp").lt_eq(lit(ScalarValue::Utf8(Some(
                "9999-12-31T23:59:59Z".to_string(),
            )))));

        assert_eq!(format!("{:?}", expr), format!("{:?}", expected_expr));
    }

    #[tokio::test]
    async fn test_build_column_filter() {
        let table_provider =
            build_test_table_provider(&[("public".to_string(), "test_table".to_string())]).await;
        let session_state = SessionStateBuilder::new().with_default_features().build();
        let planner = LogQueryPlanner::new(table_provider, session_state);

        let column_filter = ColumnFilters {
            column_name: "message".to_string(),
            filters: vec![
                ContentFilter::Contains("error".to_string()),
                ContentFilter::Prefix("WARN".to_string()),
            ],
        };

        let expr_option = planner.build_column_filter(&column_filter).unwrap();
        assert!(expr_option.is_some());

        let expr = expr_option.unwrap();

        let expected_expr = col("message")
            .like(lit(ScalarValue::Utf8(Some("%error%".to_string()))))
            .and(col("message").like(lit(ScalarValue::Utf8(Some("WARN%".to_string())))));

        assert_eq!(format!("{:?}", expr), format!("{:?}", expected_expr));
    }

    #[tokio::test]
    async fn test_query_to_plan_with_only_skip() {
        let table_provider =
            build_test_table_provider(&[("public".to_string(), "test_table".to_string())]).await;
        let session_state = SessionStateBuilder::new().with_default_features().build();
        let mut planner = LogQueryPlanner::new(table_provider, session_state);

        let log_query = LogQuery {
            table: TableName::new(DEFAULT_CATALOG_NAME, "public", "test_table"),
            time_filter: TimeFilter {
                start: Some("2021-01-01T00:00:00Z".to_string()),
                end: Some("2021-01-02T00:00:00Z".to_string()),
                span: None,
            },
            filters: vec![ColumnFilters {
                column_name: "message".to_string(),
                filters: vec![ContentFilter::Contains("error".to_string())],
            }],
            limit: Limit {
                skip: Some(10),
                fetch: None,
            },
            context: Context::None,
            columns: vec![],
            exprs: vec![],
        };

        let plan = planner.query_to_plan(log_query).await.unwrap();
        let expected = "Limit: skip=10, fetch=1000 [message:Utf8, timestamp:Timestamp(Millisecond, None), host:Utf8;N]\
\n  Filter: greptime.public.test_table.timestamp >= Utf8(\"2021-01-01T00:00:00Z\") AND greptime.public.test_table.timestamp <= Utf8(\"2021-01-02T00:00:00Z\") AND greptime.public.test_table.message LIKE Utf8(\"%error%\") [message:Utf8, timestamp:Timestamp(Millisecond, None), host:Utf8;N]\
\n    TableScan: greptime.public.test_table [message:Utf8, timestamp:Timestamp(Millisecond, None), host:Utf8;N]";

        assert_eq!(plan.display_indent_schema().to_string(), expected);
    }

    #[tokio::test]
    async fn test_query_to_plan_without_limit() {
        let table_provider =
            build_test_table_provider(&[("public".to_string(), "test_table".to_string())]).await;
        let session_state = SessionStateBuilder::new().with_default_features().build();
        let mut planner = LogQueryPlanner::new(table_provider, session_state);

        let log_query = LogQuery {
            table: TableName::new(DEFAULT_CATALOG_NAME, "public", "test_table"),
            time_filter: TimeFilter {
                start: Some("2021-01-01T00:00:00Z".to_string()),
                end: Some("2021-01-02T00:00:00Z".to_string()),
                span: None,
            },
            filters: vec![ColumnFilters {
                column_name: "message".to_string(),
                filters: vec![ContentFilter::Contains("error".to_string())],
            }],
            limit: Limit {
                skip: None,
                fetch: None,
            },
            context: Context::None,
            columns: vec![],
            exprs: vec![],
        };

        let plan = planner.query_to_plan(log_query).await.unwrap();
        let expected = "Limit: skip=0, fetch=1000 [message:Utf8, timestamp:Timestamp(Millisecond, None), host:Utf8;N]\
\n  Filter: greptime.public.test_table.timestamp >= Utf8(\"2021-01-01T00:00:00Z\") AND greptime.public.test_table.timestamp <= Utf8(\"2021-01-02T00:00:00Z\") AND greptime.public.test_table.message LIKE Utf8(\"%error%\") [message:Utf8, timestamp:Timestamp(Millisecond, None), host:Utf8;N]\
\n    TableScan: greptime.public.test_table [message:Utf8, timestamp:Timestamp(Millisecond, None), host:Utf8;N]";

        assert_eq!(plan.display_indent_schema().to_string(), expected);
    }

    #[test]
    fn test_escape_pattern() {
        assert_eq!(escape_like_pattern("test"), "test");
        assert_eq!(escape_like_pattern("te%st"), "te\\%st");
        assert_eq!(escape_like_pattern("te_st"), "te\\_st");
        assert_eq!(escape_like_pattern("te\\st"), "te\\\\st");
    }

    #[tokio::test]
    async fn test_query_to_plan_with_aggr_func() {
        let table_provider =
            build_test_table_provider(&[("public".to_string(), "test_table".to_string())]).await;
        let session_state = SessionStateBuilder::new().with_default_features().build();
        let mut planner = LogQueryPlanner::new(table_provider, session_state);

        let log_query = LogQuery {
            table: TableName::new(DEFAULT_CATALOG_NAME, "public", "test_table"),
            time_filter: TimeFilter {
                start: Some("2021-01-01T00:00:00Z".to_string()),
                end: Some("2021-01-02T00:00:00Z".to_string()),
                span: None,
            },
            filters: vec![],
            limit: Limit {
                skip: None,
                fetch: Some(100),
            },
            context: Context::None,
            columns: vec![],
            exprs: vec![LogExpr::AggrFunc {
                name: "count".to_string(),
                args: vec![LogExpr::NamedIdent("message".to_string())],
                by: vec![LogExpr::NamedIdent("host".to_string())],
                range: None,
                alias: Some("count_result".to_string()),
            }],
        };

        let plan = planner.query_to_plan(log_query).await.unwrap();
        let expected = "Aggregate: groupBy=[[greptime.public.test_table.host]], aggr=[[count(greptime.public.test_table.message) AS count_result]] [host:Utf8;N, count_result:Int64]\
\n  Limit: skip=0, fetch=100 [message:Utf8, timestamp:Timestamp(Millisecond, None), host:Utf8;N]\
\n    Filter: greptime.public.test_table.timestamp >= Utf8(\"2021-01-01T00:00:00Z\") AND greptime.public.test_table.timestamp <= Utf8(\"2021-01-02T00:00:00Z\") [message:Utf8, timestamp:Timestamp(Millisecond, None), host:Utf8;N]\
\n      TableScan: greptime.public.test_table [message:Utf8, timestamp:Timestamp(Millisecond, None), host:Utf8;N]";

        assert_eq!(plan.display_indent_schema().to_string(), expected);
    }

    #[tokio::test]
    async fn test_query_to_plan_with_scalar_func() {
        let table_provider =
            build_test_table_provider(&[("public".to_string(), "test_table".to_string())]).await;
        let session_state = SessionStateBuilder::new().with_default_features().build();
        let mut planner = LogQueryPlanner::new(table_provider, session_state);

        let log_query = LogQuery {
            table: TableName::new(DEFAULT_CATALOG_NAME, "public", "test_table"),
            time_filter: TimeFilter {
                start: Some("2021-01-01T00:00:00Z".to_string()),
                end: Some("2021-01-02T00:00:00Z".to_string()),
                span: None,
            },
            filters: vec![],
            limit: Limit {
                skip: None,
                fetch: Some(100),
            },
            context: Context::None,
            columns: vec![],
            exprs: vec![LogExpr::ScalarFunc {
                name: "date_trunc".to_string(),
                args: vec![
                    LogExpr::NamedIdent("timestamp".to_string()),
                    LogExpr::Literal("day".to_string()),
                ],
                alias: Some("time_bucket".to_string()),
            }],
        };

        let plan = planner.query_to_plan(log_query).await.unwrap();
        let expected = "Projection: date_trunc(greptime.public.test_table.timestamp, Utf8(\"day\")) AS time_bucket [time_bucket:Timestamp(Nanosecond, None);N]\
        \n  Limit: skip=0, fetch=100 [message:Utf8, timestamp:Timestamp(Millisecond, None), host:Utf8;N]\
        \n    Filter: greptime.public.test_table.timestamp >= Utf8(\"2021-01-01T00:00:00Z\") AND greptime.public.test_table.timestamp <= Utf8(\"2021-01-02T00:00:00Z\") [message:Utf8, timestamp:Timestamp(Millisecond, None), host:Utf8;N]\
        \n      TableScan: greptime.public.test_table [message:Utf8, timestamp:Timestamp(Millisecond, None), host:Utf8;N]";

        assert_eq!(plan.display_indent_schema().to_string(), expected);
    }

    #[tokio::test]
    async fn test_build_column_filter_between() {
        let table_provider =
            build_test_table_provider(&[("public".to_string(), "test_table".to_string())]).await;
        let session_state = SessionStateBuilder::new().with_default_features().build();
        let planner = LogQueryPlanner::new(table_provider, session_state);

        let column_filter = ColumnFilters {
            column_name: "message".to_string(),
            filters: vec![ContentFilter::Between {
                start: "a".to_string(),
                end: "z".to_string(),
                start_inclusive: true,
                end_inclusive: false,
            }],
        };

        let expr_option = planner.build_column_filter(&column_filter).unwrap();
        assert!(expr_option.is_some());

        let expr = expr_option.unwrap();
        let expected_expr = col("message")
            .gt_eq(lit(ScalarValue::Utf8(Some("a".to_string()))))
            .and(col("message").lt(lit(ScalarValue::Utf8(Some("z".to_string())))));

        assert_eq!(format!("{:?}", expr), format!("{:?}", expected_expr));
    }

    #[tokio::test]
    async fn test_query_to_plan_with_date_histogram() {
        let table_provider =
            build_test_table_provider(&[("public".to_string(), "test_table".to_string())]).await;
        let session_state = SessionStateBuilder::new().with_default_features().build();
        let mut planner = LogQueryPlanner::new(table_provider, session_state);

        let log_query = LogQuery {
            table: TableName::new(DEFAULT_CATALOG_NAME, "public", "test_table"),
            time_filter: TimeFilter {
                start: Some("2021-01-01T00:00:00Z".to_string()),
                end: Some("2021-01-02T00:00:00Z".to_string()),
                span: None,
            },
            filters: vec![],
            limit: Limit {
                skip: Some(0),
                fetch: None,
            },
            context: Context::None,
            columns: vec![],
            exprs: vec![
                LogExpr::ScalarFunc {
                    name: "date_bin".to_string(),
                    args: vec![
                        LogExpr::Literal("30 seconds".to_string()),
                        LogExpr::NamedIdent("timestamp".to_string()),
                    ],
                    alias: Some("2__date_histogram__time_bucket".to_string()),
                },
                LogExpr::AggrFunc {
                    name: "count".to_string(),
                    args: vec![LogExpr::PositionalIdent(0)],
                    by: vec![LogExpr::NamedIdent(
                        "2__date_histogram__time_bucket".to_string(),
                    )],
                    range: None,
                    alias: Some("count_result".to_string()),
                },
            ],
        };

        let plan = planner.query_to_plan(log_query).await.unwrap();
        let expected = "Aggregate: groupBy=[[2__date_histogram__time_bucket]], aggr=[[count(2__date_histogram__time_bucket) AS count_result]] [2__date_histogram__time_bucket:Timestamp(Nanosecond, None);N, count_result:Int64]\
\n  Projection: date_bin(Utf8(\"30 seconds\"), greptime.public.test_table.timestamp) AS 2__date_histogram__time_bucket [2__date_histogram__time_bucket:Timestamp(Nanosecond, None);N]\
\n    Limit: skip=0, fetch=1000 [message:Utf8, timestamp:Timestamp(Millisecond, None), host:Utf8;N]\
\n      Filter: greptime.public.test_table.timestamp >= Utf8(\"2021-01-01T00:00:00Z\") AND greptime.public.test_table.timestamp <= Utf8(\"2021-01-02T00:00:00Z\") [message:Utf8, timestamp:Timestamp(Millisecond, None), host:Utf8;N]\
\n        TableScan: greptime.public.test_table [message:Utf8, timestamp:Timestamp(Millisecond, None), host:Utf8;N]";

        assert_eq!(plan.display_indent_schema().to_string(), expected);
    }

    #[tokio::test]
    async fn test_build_compound_filter() {
        let table_provider =
            build_test_table_provider(&[("public".to_string(), "test_table".to_string())]).await;
        let session_state = SessionStateBuilder::new().with_default_features().build();
        let planner = LogQueryPlanner::new(table_provider, session_state);

        // Test AND compound
        let filter = ContentFilter::Compound(
            vec![
                ContentFilter::Contains("error".to_string()),
                ContentFilter::Prefix("WARN".to_string()),
            ],
            BinaryOperator::And,
        );
        let expr = planner.build_content_filter("message", &filter).unwrap();

        let expected_expr = col("message")
            .like(lit(ScalarValue::Utf8(Some("%error%".to_string()))))
            .and(col("message").like(lit(ScalarValue::Utf8(Some("WARN%".to_string())))));

        assert_eq!(format!("{:?}", expr), format!("{:?}", expected_expr));

        // Test OR compound
        let filter = ContentFilter::Compound(
            vec![
                ContentFilter::Contains("error".to_string()),
                ContentFilter::Prefix("WARN".to_string()),
            ],
            BinaryOperator::Or,
        );
        let expr = planner.build_content_filter("message", &filter).unwrap();

        let expected_expr = col("message")
            .like(lit(ScalarValue::Utf8(Some("%error%".to_string()))))
            .or(col("message").like(lit(ScalarValue::Utf8(Some("WARN%".to_string())))));

        assert_eq!(format!("{:?}", expr), format!("{:?}", expected_expr));

        // Test nested compound
        let filter = ContentFilter::Compound(
            vec![
                ContentFilter::Contains("error".to_string()),
                ContentFilter::Compound(
                    vec![
                        ContentFilter::Prefix("WARN".to_string()),
                        ContentFilter::Exact("DEBUG".to_string()),
                    ],
                    BinaryOperator::Or,
                ),
            ],
            BinaryOperator::And,
        );
        let expr = planner.build_content_filter("message", &filter).unwrap();

        let expected_nested = col("message")
            .like(lit(ScalarValue::Utf8(Some("WARN%".to_string()))))
            .or(col("message").like(lit(ScalarValue::Utf8(Some("DEBUG".to_string())))));
        let expected_expr = col("message")
            .like(lit(ScalarValue::Utf8(Some("%error%".to_string()))))
            .and(expected_nested);

        assert_eq!(format!("{:?}", expr), format!("{:?}", expected_expr));
    }

    #[tokio::test]
    async fn test_build_great_than_filter() {
        let table_provider =
            build_test_table_provider(&[("public".to_string(), "test_table".to_string())]).await;
        let session_state = SessionStateBuilder::new().with_default_features().build();
        let planner = LogQueryPlanner::new(table_provider, session_state);

        // Test GreatThan with inclusive=true
        let column_filter = ColumnFilters {
            column_name: "message".to_string(),
            filters: vec![ContentFilter::GreatThan {
                value: "error".to_string(),
                inclusive: true,
            }],
        };

        let expr_option = planner.build_column_filter(&column_filter).unwrap();
        assert!(expr_option.is_some());

        let expr = expr_option.unwrap();
        let expected_expr = col("message").gt_eq(lit(ScalarValue::Utf8(Some("error".to_string()))));

        assert_eq!(format!("{:?}", expr), format!("{:?}", expected_expr));

        // Test GreatThan with inclusive=false
        let column_filter = ColumnFilters {
            column_name: "message".to_string(),
            filters: vec![ContentFilter::GreatThan {
                value: "error".to_string(),
                inclusive: false,
            }],
        };

        let expr_option = planner.build_column_filter(&column_filter).unwrap();
        assert!(expr_option.is_some());

        let expr = expr_option.unwrap();
        let expected_expr = col("message").gt(lit(ScalarValue::Utf8(Some("error".to_string()))));

        assert_eq!(format!("{:?}", expr), format!("{:?}", expected_expr));
    }

    #[tokio::test]
    async fn test_build_less_than_filter() {
        let table_provider =
            build_test_table_provider(&[("public".to_string(), "test_table".to_string())]).await;
        let session_state = SessionStateBuilder::new().with_default_features().build();
        let planner = LogQueryPlanner::new(table_provider, session_state);

        // Test LessThan with inclusive=true
        let column_filter = ColumnFilters {
            column_name: "message".to_string(),
            filters: vec![ContentFilter::LessThan {
                value: "error".to_string(),
                inclusive: true,
            }],
        };

        let expr_option = planner.build_column_filter(&column_filter).unwrap();
        assert!(expr_option.is_some());

        let expr = expr_option.unwrap();
        let expected_expr = col("message").lt_eq(lit(ScalarValue::Utf8(Some("error".to_string()))));

        assert_eq!(format!("{:?}", expr), format!("{:?}", expected_expr));

        // Test LessThan with inclusive=false
        let column_filter = ColumnFilters {
            column_name: "message".to_string(),
            filters: vec![ContentFilter::LessThan {
                value: "error".to_string(),
                inclusive: false,
            }],
        };

        let expr_option = planner.build_column_filter(&column_filter).unwrap();
        assert!(expr_option.is_some());

        let expr = expr_option.unwrap();
        let expected_expr = col("message").lt(lit(ScalarValue::Utf8(Some("error".to_string()))));

        assert_eq!(format!("{:?}", expr), format!("{:?}", expected_expr));
    }

    #[tokio::test]
    async fn test_build_in_filter() {
        let table_provider =
            build_test_table_provider(&[("public".to_string(), "test_table".to_string())]).await;
        let session_state = SessionStateBuilder::new().with_default_features().build();
        let planner = LogQueryPlanner::new(table_provider, session_state);

        // Test In filter with multiple values
        let column_filter = ColumnFilters {
            column_name: "message".to_string(),
            filters: vec![ContentFilter::In(vec![
                "error".to_string(),
                "warning".to_string(),
                "info".to_string(),
            ])],
        };

        let expr_option = planner.build_column_filter(&column_filter).unwrap();
        assert!(expr_option.is_some());

        let expr = expr_option.unwrap();
        let expected_expr = col("message").in_list(
            vec![
                lit(ScalarValue::Utf8(Some("error".to_string()))),
                lit(ScalarValue::Utf8(Some("warning".to_string()))),
                lit(ScalarValue::Utf8(Some("info".to_string()))),
            ],
            false,
        );

        assert_eq!(format!("{:?}", expr), format!("{:?}", expected_expr));
    }
}
