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
use datafusion_common::ScalarValue;
use datafusion_expr::utils::conjunction;
use datafusion_expr::{col, lit, Expr, LogicalPlan, LogicalPlanBuilder};
use datafusion_sql::TableReference;
use datatypes::schema::Schema;
use log_query::{ColumnFilters, LogQuery, TimeFilter};
use snafu::{OptionExt, ResultExt};
use table::table::adapter::DfTableProviderAdapter;

use crate::log_query::error::{
    CatalogSnafu, DataFusionPlanningSnafu, Result, TimeIndexNotFoundSnafu, UnimplementedSnafu,
    UnknownTableSnafu,
};

const DEFAULT_LIMIT: usize = 1000;

pub struct LogQueryPlanner {
    table_provider: DfTableSourceProvider,
}

impl LogQueryPlanner {
    pub fn new(table_provider: DfTableSourceProvider) -> Self {
        Self { table_provider }
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

        // Column filters and projections
        let mut projected_columns = Vec::new();
        for column_filter in &query.columns {
            if let Some(expr) = self.build_column_filter(column_filter)? {
                filters.push(expr);
            }
            projected_columns.push(col(&column_filter.column_name));
        }

        // Apply filters
        if !filters.is_empty() {
            let filter_expr = filters.into_iter().reduce(|a, b| a.and(b)).unwrap();
            plan_builder = plan_builder
                .filter(filter_expr)
                .context(DataFusionPlanningSnafu)?;
        }

        // Apply projections
        plan_builder = plan_builder
            .project(projected_columns)
            .context(DataFusionPlanningSnafu)?;

        // Apply limit
        plan_builder = plan_builder
            .limit(
                query.limit.skip.unwrap_or(0),
                Some(query.limit.fetch.unwrap_or(DEFAULT_LIMIT)),
            )
            .context(DataFusionPlanningSnafu)?;

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

        let exprs = column_filter
            .filters
            .iter()
            .map(|filter| match filter {
                log_query::ContentFilter::Exact(pattern) => Ok(col(&column_filter.column_name)
                    .like(lit(ScalarValue::Utf8(Some(escape_like_pattern(pattern)))))),
                log_query::ContentFilter::Prefix(pattern) => Ok(col(&column_filter.column_name)
                    .like(lit(ScalarValue::Utf8(Some(format!(
                        "{}%",
                        escape_like_pattern(pattern)
                    )))))),
                log_query::ContentFilter::Postfix(pattern) => Ok(col(&column_filter.column_name)
                    .like(lit(ScalarValue::Utf8(Some(format!(
                        "%{}",
                        escape_like_pattern(pattern)
                    )))))),
                log_query::ContentFilter::Contains(pattern) => Ok(col(&column_filter.column_name)
                    .like(lit(ScalarValue::Utf8(Some(format!(
                        "%{}%",
                        escape_like_pattern(pattern)
                    )))))),
                log_query::ContentFilter::Regex(..) => Err::<Expr, _>(
                    UnimplementedSnafu {
                        feature: "regex filter",
                    }
                    .build(),
                ),
                log_query::ContentFilter::Compound(..) => Err::<Expr, _>(
                    UnimplementedSnafu {
                        feature: "compound filter",
                    }
                    .build(),
                ),
            })
            .try_collect::<Vec<_>>()?;

        Ok(conjunction(exprs))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use catalog::memory::MemoryCatalogManager;
    use catalog::RegisterTableRequest;
    use common_catalog::consts::DEFAULT_CATALOG_NAME;
    use common_query::test_util::DummyDecoder;
    use datatypes::prelude::ConcreteDataType;
    use datatypes::schema::{ColumnSchema, SchemaRef};
    use log_query::{ContentFilter, Context, Limit};
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
        let mut planner = LogQueryPlanner::new(table_provider);

        let log_query = LogQuery {
            table: TableName::new(DEFAULT_CATALOG_NAME, "public", "test_table"),
            time_filter: TimeFilter {
                start: Some("2021-01-01T00:00:00Z".to_string()),
                end: Some("2021-01-02T00:00:00Z".to_string()),
                span: None,
            },
            columns: vec![ColumnFilters {
                column_name: "message".to_string(),
                filters: vec![ContentFilter::Contains("error".to_string())],
            }],
            limit: Limit {
                skip: None,
                fetch: Some(100),
            },
            context: Context::None,
        };

        let plan = planner.query_to_plan(log_query).await.unwrap();
        let expected = "Limit: skip=0, fetch=100 [message:Utf8]\
\n  Projection: greptime.public.test_table.message [message:Utf8]\
\n    Filter: greptime.public.test_table.timestamp >= Utf8(\"2021-01-01T00:00:00Z\") AND greptime.public.test_table.timestamp <= Utf8(\"2021-01-02T00:00:00Z\") AND greptime.public.test_table.message LIKE Utf8(\"%error%\") [message:Utf8, timestamp:Timestamp(Millisecond, None), host:Utf8;N]\
\n      TableScan: greptime.public.test_table [message:Utf8, timestamp:Timestamp(Millisecond, None), host:Utf8;N]";

        assert_eq!(plan.display_indent_schema().to_string(), expected);
    }

    #[tokio::test]
    async fn test_build_time_filter() {
        let table_provider =
            build_test_table_provider(&[("public".to_string(), "test_table".to_string())]).await;
        let planner = LogQueryPlanner::new(table_provider);

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
        let planner = LogQueryPlanner::new(table_provider);

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
        let planner = LogQueryPlanner::new(table_provider);

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
        let mut planner = LogQueryPlanner::new(table_provider);

        let log_query = LogQuery {
            table: TableName::new(DEFAULT_CATALOG_NAME, "public", "test_table"),
            time_filter: TimeFilter {
                start: Some("2021-01-01T00:00:00Z".to_string()),
                end: Some("2021-01-02T00:00:00Z".to_string()),
                span: None,
            },
            columns: vec![ColumnFilters {
                column_name: "message".to_string(),
                filters: vec![ContentFilter::Contains("error".to_string())],
            }],
            limit: Limit {
                skip: Some(10),
                fetch: None,
            },
            context: Context::None,
        };

        let plan = planner.query_to_plan(log_query).await.unwrap();
        let expected = "Limit: skip=10, fetch=1000 [message:Utf8]\
\n  Projection: greptime.public.test_table.message [message:Utf8]\
\n    Filter: greptime.public.test_table.timestamp >= Utf8(\"2021-01-01T00:00:00Z\") AND greptime.public.test_table.timestamp <= Utf8(\"2021-01-02T00:00:00Z\") AND greptime.public.test_table.message LIKE Utf8(\"%error%\") [message:Utf8, timestamp:Timestamp(Millisecond, None), host:Utf8;N]\
\n      TableScan: greptime.public.test_table [message:Utf8, timestamp:Timestamp(Millisecond, None), host:Utf8;N]";

        assert_eq!(plan.display_indent_schema().to_string(), expected);
    }

    #[tokio::test]
    async fn test_query_to_plan_without_limit() {
        let table_provider =
            build_test_table_provider(&[("public".to_string(), "test_table".to_string())]).await;
        let mut planner = LogQueryPlanner::new(table_provider);

        let log_query = LogQuery {
            table: TableName::new(DEFAULT_CATALOG_NAME, "public", "test_table"),
            time_filter: TimeFilter {
                start: Some("2021-01-01T00:00:00Z".to_string()),
                end: Some("2021-01-02T00:00:00Z".to_string()),
                span: None,
            },
            columns: vec![ColumnFilters {
                column_name: "message".to_string(),
                filters: vec![ContentFilter::Contains("error".to_string())],
            }],
            limit: Limit {
                skip: None,
                fetch: None,
            },
            context: Context::None,
        };

        let plan = planner.query_to_plan(log_query).await.unwrap();
        let expected = "Limit: skip=0, fetch=1000 [message:Utf8]\
\n  Projection: greptime.public.test_table.message [message:Utf8]\
\n    Filter: greptime.public.test_table.timestamp >= Utf8(\"2021-01-01T00:00:00Z\") AND greptime.public.test_table.timestamp <= Utf8(\"2021-01-02T00:00:00Z\") AND greptime.public.test_table.message LIKE Utf8(\"%error%\") [message:Utf8, timestamp:Timestamp(Millisecond, None), host:Utf8;N]\
\n      TableScan: greptime.public.test_table [message:Utf8, timestamp:Timestamp(Millisecond, None), host:Utf8;N]";

        assert_eq!(plan.display_indent_schema().to_string(), expected);
    }

    #[test]
    fn test_escape_pattern() {
        assert_eq!(escape_like_pattern("test"), "test");
        assert_eq!(escape_like_pattern("te%st"), "te\\%st");
        assert_eq!(escape_like_pattern("te_st"), "te\\_st");
        assert_eq!(escape_like_pattern("te\\st"), "te\\\\st");
    }
}
