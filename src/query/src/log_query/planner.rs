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
            .limit(0, query.limit.or(Some(DEFAULT_LIMIT)))
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

        let expr = col(timestamp_col.clone())
            .gt_eq(lit(ScalarValue::Utf8(time_filter.start.clone())))
            .and(col(timestamp_col).lt_eq(lit(ScalarValue::Utf8(time_filter.end.clone()))));

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
                    .like(lit(ScalarValue::Utf8(Some(escape_pattern(pattern)))))),
                log_query::ContentFilter::Prefix(pattern) => Ok(col(&column_filter.column_name)
                    .like(lit(ScalarValue::Utf8(Some(format!(
                        "{}%",
                        escape_pattern(pattern)
                    )))))),
                log_query::ContentFilter::Postfix(pattern) => Ok(col(&column_filter.column_name)
                    .like(lit(ScalarValue::Utf8(Some(format!(
                        "%{}",
                        escape_pattern(pattern)
                    )))))),
                log_query::ContentFilter::Contains(pattern) => Ok(col(&column_filter.column_name)
                    .like(lit(ScalarValue::Utf8(Some(format!(
                        "%{}%",
                        escape_pattern(pattern)
                    )))))),
                log_query::ContentFilter::Regex(..) => {
                    return Err::<Expr, _>(
                        UnimplementedSnafu {
                            feature: format!("regex filter"),
                        }
                        .build(),
                    );
                }
                log_query::ContentFilter::Compound(..) => {
                    return Err::<Expr, _>(
                        UnimplementedSnafu {
                            feature: format!("compound filter"),
                        }
                        .build(),
                    );
                }
            })
            .try_collect::<Vec<_>>()?;

        Ok(conjunction(exprs))
    }
}

fn escape_pattern(pattern: &str) -> String {
    pattern
        .chars()
        .flat_map(|c| match c {
            '\\' | '%' | '_' => vec!['\\', c],
            _ => vec![c],
        })
        .collect::<String>()
}
