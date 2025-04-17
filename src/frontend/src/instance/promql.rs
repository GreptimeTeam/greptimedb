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

use std::time::SystemTime;

use catalog::information_schema::TABLES;
use client::OutputData;
use common_catalog::consts::INFORMATION_SCHEMA_NAME;
use common_catalog::format_full_table_name;
use common_recordbatch::util;
use common_telemetry::tracing;
use datatypes::prelude::Value;
use promql_parser::label::{Matcher, Matchers};
use query::promql;
use query::promql::planner::PromPlanner;
use servers::prometheus;
use session::context::QueryContextRef;
use snafu::{OptionExt, ResultExt};

use crate::error::{
    CatalogSnafu, CollectRecordbatchSnafu, ExecLogicalPlanSnafu,
    PrometheusLabelValuesQueryPlanSnafu, PrometheusMetricNamesQueryPlanSnafu, ReadTableSnafu,
    Result, TableNotFoundSnafu,
};
use crate::instance::Instance;

impl Instance {
    /// Handles metric names query request, returns the names.
    #[tracing::instrument(skip_all)]
    pub(crate) async fn handle_query_metric_names(
        &self,
        matchers: Vec<Matcher>,
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

        let logical_plan = prometheus::metric_name_matchers_to_plan(dataframe, matchers, ctx)
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
            let names = batch.column(0);

            for i in 0..names.len() {
                let Value::String(name) = names.get(i) else {
                    unreachable!();
                };

                results.push(name.into_string());
            }
        }

        Ok(results)
    }

    /// Handles label values query request, returns the values.
    #[tracing::instrument(skip_all)]
    pub(crate) async fn handle_query_label_values(
        &self,
        metric: String,
        label_name: String,
        matchers: Vec<Matcher>,
        start: SystemTime,
        end: SystemTime,
        ctx: &QueryContextRef,
    ) -> Result<Vec<String>> {
        let table_schema = ctx.current_schema();
        let table = self
            .catalog_manager
            .table(ctx.current_catalog(), &table_schema, &metric, Some(ctx))
            .await
            .context(CatalogSnafu)?
            .with_context(|| TableNotFoundSnafu {
                table_name: format_full_table_name(ctx.current_catalog(), &table_schema, &metric),
            })?;

        let dataframe = self
            .query_engine
            .read_table(table.clone())
            .with_context(|_| ReadTableSnafu {
                table_name: format_full_table_name(ctx.current_catalog(), &table_schema, &metric),
            })?;

        let scan_plan = dataframe.into_logical_plan();
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
            let names = batch.column(0);

            for i in 0..names.len() {
                results.push(names.get(i).to_string());
            }
        }

        Ok(results)
    }
}
