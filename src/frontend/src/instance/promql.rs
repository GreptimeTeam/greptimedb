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

use std::sync::Arc;
use std::time::SystemTime;

use auth::PermissionTableTarget;
use catalog::information_schema::TABLES;
use client::OutputData;
use common_catalog::consts::INFORMATION_SCHEMA_NAME;
use common_catalog::format_full_table_name;
use common_recordbatch::util;
use common_telemetry::tracing;
use datafusion_expr::LogicalPlan;
use promql_parser::label::{Matcher, Matchers};
use query::promql;
use query::promql::planner::PromPlanner;
use servers::prometheus;
use session::context::QueryContextRef;
use snafu::{OptionExt, ResultExt};

use crate::error::{
    CatalogSnafu, CollectRecordbatchSnafu, ExecLogicalPlanSnafu,
    PrometheusLabelValuesQueryPlanSnafu, PrometheusMetricNamesQueryPlanSnafu, ReadTableSnafu,
    Result, TableNotFoundSnafu, TableSnafu,
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

        let dataframe = self
            .query_engine
            .read_table(table.clone())
            .with_context(|_| ReadTableSnafu {
                table_name: full_table_name,
            })?;

        let scan_plan = dataframe.into_unoptimized_plan();
        let filter_conditions =
            PromPlanner::matchers_to_expr(Matchers::new(matchers), scan_plan.schema())
                .context(PrometheusLabelValuesQueryPlanSnafu)?;
        let logical_plan = promql::label_values::rewrite_label_values_query(
            table,
            scan_plan,
            filter_conditions,
            label_name,
            start,
            end,
        )
        .context(PrometheusLabelValuesQueryPlanSnafu)?;

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
