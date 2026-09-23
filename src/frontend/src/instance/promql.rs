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
use std::sync::Arc;
use std::time::SystemTime;

use auth::PermissionTableTarget;
use catalog::information_schema::TABLES;
use client::OutputData;
use common_catalog::consts::INFORMATION_SCHEMA_NAME;
use common_catalog::format_full_table_name;
use common_recordbatch::{RecordBatch, util};
use common_telemetry::tracing;
use datafusion_expr::LogicalPlan;
use datatypes::arrow::array::{Array, UInt32Array};
use futures::StreamExt;
use promql_parser::label::{Matcher, Matchers};
use query::promql;
use query::promql::planner::PromPlanner;
use servers::prometheus;
use session::context::QueryContextRef;
use snafu::{OptionExt, ResultExt};
use store_api::metric_engine_consts::DATA_SCHEMA_TABLE_ID_COLUMN_NAME;
use store_api::storage::TableId;
use table::TableRef;

use crate::error::{
    CatalogSnafu, CollectRecordbatchSnafu, ExecLogicalPlanSnafu,
    PrometheusLabelValuesQueryPlanSnafu, PrometheusMetricNamesQueryPlanSnafu, ReadTableSnafu,
    Result, TableNotFoundSnafu, TableSnafu, UnexpectedColumnTypeSnafu,
};
use crate::instance::Instance;

/// Strips the output sort a PromQL plan ends with, keeping the plan schema intact.
///
/// Only the sort the caller would observe is removed: recursion stops at any other
/// node, so ordering consumed by windows, limits or PromQL extension nodes stays.
pub(super) fn remove_output_sort(plan: LogicalPlan) -> LogicalPlan {
    match plan {
        LogicalPlan::Sort(sort) if sort.fetch.is_none() => Arc::unwrap_or_clone(sort.input),
        LogicalPlan::Projection(mut projection) => {
            projection.input = Arc::new(remove_output_sort(Arc::unwrap_or_clone(projection.input)));
            LogicalPlan::Projection(projection)
        }
        plan => plan,
    }
}

impl Instance {
    /// Handles metric names query request, returns the names.
    ///
    /// Returns every metric table name matching `matchers` and never truncates the candidate set.
    /// The callers filter these names by table permission first and only then reject a query whose
    /// *authorized* set exceeds [`servers::prometheus::MAX_METRICS_NUM`], so truncating here would
    /// hide candidates behind the names the caller cannot read.
    #[tracing::instrument(skip_all)]
    pub(crate) async fn handle_query_metric_names(
        &self,
        matchers: Vec<Matcher>,
        schema: &str,
        ctx: &QueryContextRef,
    ) -> Result<Vec<String>> {
        let _timer = crate::metrics::PROMQL_QUERY_METRICS_ELAPSED
            .with_label_values(&[ctx.get_db_string().as_str()])
            .start_timer();

        let table = self
            .catalog_manager
            .table(
                ctx.current_catalog(),
                INFORMATION_SCHEMA_NAME,
                TABLES,
                Some(ctx),
            )
            .await
            .context(CatalogSnafu)?
            .with_context(|| TableNotFoundSnafu {
                table_name: "greptime.information_schema.tables",
            })?;

        let dataframe = self
            .query_engine
            .read_table(table)
            .with_context(|_| ReadTableSnafu {
                table_name: "greptime.information_schema.tables",
            })?;

        let logical_plan =
            prometheus::metric_name_matchers_to_plan(dataframe, matchers, schema, ctx)
                .context(PrometheusMetricNamesQueryPlanSnafu)?;

        let results = self
            .query_engine
            .execute(logical_plan, ctx.clone())
            .await
            .context(ExecLogicalPlanSnafu)?;

        let batches = match results.data {
            OutputData::Stream(stream) => util::collect(stream)
                .await
                .context(CollectRecordbatchSnafu)?,
            OutputData::RecordBatches(rbs) => rbs.take(),
            _ => unreachable!("should not happen"),
        };

        let mut results = Vec::with_capacity(batches.iter().map(|b| b.num_rows()).sum());

        for batch in batches {
            // Only one column the results, ensured by `prometheus::metric_name_matchers_to_plan`.
            batch
                .iter_column_as_string(0)
                .flatten()
                .for_each(|x| results.push(x))
        }

        Ok(results)
    }

    /// Handles a metric names query constrained by matchers on ordinary labels.
    ///
    /// [`Instance::handle_query_metric_names`] answers from table metadata, which
    /// cannot resolve a label matcher: whether a metric carries `pod="abc"` is a
    /// property of its data. The metric engine multiplexes the logical tables of
    /// one physical table into a region holding the union of their label columns
    /// alongside `__table_id`, so a single distinct scan per physical table
    /// resolves the matchers for all of its logical tables at once.
    ///
    /// Only metric engine tables are covered. Tables on other engines share no
    /// column space to scan, and a scan per table does not scale to the table
    /// counts this API is expected to answer over.
    #[tracing::instrument(skip_all)]
    pub(crate) async fn handle_query_metric_names_by_labels(
        &self,
        matchers: Vec<Matcher>,
        schema: &str,
        start: SystemTime,
        end: SystemTime,
        ctx: &QueryContextRef,
    ) -> Result<Vec<String>> {
        let _timer = crate::metrics::PROMQL_QUERY_METRICS_ELAPSED
            .with_label_values(&[ctx.get_db_string().as_str()])
            .start_timer();

        let catalog = ctx.current_catalog();
        let mut table_ids = HashSet::new();
        for physical in self.physical_metric_tables(catalog, schema, ctx).await? {
            table_ids.extend(
                self.scan_matching_table_ids(physical, &matchers, start, end, ctx)
                    .await?,
            );
        }

        // Batch-resolve only the ids the scan produced. An id dropped between the
        // scan and here simply has no entry.
        let table_ids = table_ids.into_iter().collect::<Vec<_>>();
        let mut names = self
            .catalog_manager
            .tables_by_ids(catalog, schema, &table_ids)
            .await
            .context(CatalogSnafu)?
            .into_iter()
            .map(|table| table.table_info().name.clone())
            .collect::<Vec<_>>();
        names.sort_unstable();
        Ok(names)
    }

    /// Returns the metric engine physical tables of a schema.
    ///
    /// Their data regions carry the union of their logical tables' label columns,
    /// so scanning these covers every metric of the schema.
    async fn physical_metric_tables(
        &self,
        catalog: &str,
        schema: &str,
        ctx: &QueryContextRef,
    ) -> Result<Vec<TableRef>> {
        let mut tables = self.catalog_manager.tables(catalog, schema, Some(ctx));
        let mut physical_tables = Vec::new();

        while let Some(table) = tables.next().await {
            let table = table.context(CatalogSnafu)?;
            if table.table_info().is_physical_table() {
                physical_tables.push(table);
            }
        }

        Ok(physical_tables)
    }

    /// Scans `table` for the distinct values of `column` that match `matchers`
    /// within the time range.
    ///
    /// Callers look the table up and decode the batches; the plan between is the
    /// same whether the projected column is a label or `__table_id`.
    async fn scan_distinct_column(
        &self,
        table: TableRef,
        matchers: Vec<Matcher>,
        column: String,
        start: SystemTime,
        end: SystemTime,
        ctx: &QueryContextRef,
    ) -> Result<Vec<RecordBatch>> {
        let dataframe = self
            .query_engine
            .read_table(table.clone())
            .with_context(|_| ReadTableSnafu {
                table_name: table.table_info().full_table_name(),
            })?;

        let scan_plan = dataframe.into_unoptimized_plan();
        let conditions = PromPlanner::matchers_to_expr(Matchers::new(matchers), scan_plan.schema())
            .context(PrometheusLabelValuesQueryPlanSnafu)?;
        let logical_plan = promql::label_values::rewrite_label_values_query(
            table, scan_plan, conditions, column, start, end,
        )
        .context(PrometheusLabelValuesQueryPlanSnafu)?;

        let results = self
            .query_engine
            .execute(logical_plan, ctx.clone())
            .await
            .context(ExecLogicalPlanSnafu)?;

        match results.data {
            OutputData::Stream(stream) => {
                util::collect(stream).await.context(CollectRecordbatchSnafu)
            }
            OutputData::RecordBatches(rbs) => Ok(rbs.take()),
            _ => unreachable!("should not happen"),
        }
    }

    /// Returns the `__table_id`s of `physical` carrying a row that matches every
    /// matcher within the time range.
    async fn scan_matching_table_ids(
        &self,
        physical: TableRef,
        matchers: &[Matcher],
        start: SystemTime,
        end: SystemTime,
        ctx: &QueryContextRef,
    ) -> Result<Vec<TableId>> {
        // `__table_id` attributes a row to its logical table, and a physical
        // table that never took a column from one does not expose it. Skipping
        // such a table can only miss a metric carrying no labels at all.
        if physical
            .schema()
            .column_schema_by_name(DATA_SCHEMA_TABLE_ID_COLUMN_NAME)
            .is_none()
        {
            return Ok(Vec::new());
        }

        let table_name = physical.table_info().full_table_name();
        let batches = self
            .scan_distinct_column(
                physical,
                matchers.to_vec(),
                DATA_SCHEMA_TABLE_ID_COLUMN_NAME.to_string(),
                start,
                end,
                ctx,
            )
            .await?;

        let mut table_ids = Vec::new();
        for batch in batches {
            // Only one column in results, ensured by `rewrite_label_values_query`.
            let column = batch.column(0);
            let ids = column
                .as_any()
                .downcast_ref::<UInt32Array>()
                .with_context(|| UnexpectedColumnTypeSnafu {
                    table_name: table_name.clone(),
                    column: DATA_SCHEMA_TABLE_ID_COLUMN_NAME,
                    data_type: column.data_type().to_string(),
                })?;
            table_ids.extend(ids.iter().flatten());
        }

        Ok(table_ids)
    }

    /// Handles label values query request, returns the values.
    #[tracing::instrument(skip_all)]
    pub(crate) async fn handle_query_label_values(
        &self,
        target: PermissionTableTarget,
        label_name: String,
        matchers: Vec<Matcher>,
        start: SystemTime,
        end: SystemTime,
        ctx: &QueryContextRef,
    ) -> Result<Vec<String>> {
        let full_table_name =
            format_full_table_name(&target.catalog, &target.schema, &target.table);
        let table = self
            .catalog_manager
            .table(&target.catalog, &target.schema, &target.table, Some(ctx))
            .await
            .context(CatalogSnafu)?
            .with_context(|| TableNotFoundSnafu {
                table_name: full_table_name.clone(),
            })?;

        // Check label column existence before building the query plan so a missing label can be
        // reported as `TableColumnNotFound` and handled like Prometheus expects.
        if table.schema().column_schema_by_name(&label_name).is_none() {
            return table::error::ColumnNotExistsSnafu {
                column_name: label_name,
                table_name: full_table_name,
            }
            .fail()
            .context(TableSnafu);
        }

        let batches = self
            .scan_distinct_column(table, matchers, label_name, start, end, ctx)
            .await?;

        let mut results = Vec::with_capacity(batches.iter().map(|b| b.num_rows()).sum());
        for batch in batches {
            // Only one column in results, ensured by `prometheus::label_values_matchers_to_plan`.
            batch
                .iter_column_as_string(0)
                .flatten()
                .for_each(|x| results.push(x))
        }

        Ok(results)
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::Int64Array;
    use datafusion::prelude::{SessionConfig, SessionContext};
    use datafusion_expr::{LogicalPlanBuilder, Sort, col, lit};

    use super::*;

    #[tokio::test]
    async fn remove_output_sort_keeps_schema_and_row_selection() {
        let input =
            LogicalPlanBuilder::values(vec![vec![lit(3_i64)], vec![lit(1_i64)], vec![lit(2_i64)]])
                .unwrap()
                .build()
                .unwrap();
        let sort = Sort {
            expr: vec![col("column1").sort(true, false)],
            input: Arc::new(input),
            fetch: None,
        };
        let projected = LogicalPlanBuilder::from(LogicalPlan::Sort(sort.clone()))
            .project(vec![col("column1").alias("sample")])
            .unwrap()
            .project(vec![col("sample")])
            .unwrap()
            .build()
            .unwrap();
        // The root sort is removed, but the sort feeding the limit selects the rows.
        let limited = LogicalPlanBuilder::from(LogicalPlan::Sort(sort.clone()))
            .limit(0, Some(2))
            .unwrap()
            .sort(vec![col("column1").sort(false, false)])
            .unwrap()
            .build()
            .unwrap();
        let fetched = LogicalPlan::Sort(Sort {
            fetch: Some(2),
            ..sort.clone()
        });

        let context =
            SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
        for (name, plan, expected) in [
            ("root", LogicalPlan::Sort(sort), vec![3, 1, 2]),
            ("projection", projected, vec![3, 1, 2]),
            ("sort below limit", limited, vec![1, 2]),
            ("fetch", fetched, vec![1, 2]),
        ] {
            let schema = plan.schema().clone();
            let plan = remove_output_sort(plan);
            assert_eq!(plan.schema(), &schema, "{name}");
            let output = context
                .execute_logical_plan(plan)
                .await
                .unwrap()
                .collect()
                .await
                .unwrap();
            let values = output
                .iter()
                .flat_map(|batch| {
                    batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .values()
                        .iter()
                        .copied()
                })
                .collect::<Vec<_>>();
            assert_eq!(values, expected, "{name}");
        }
    }
}
