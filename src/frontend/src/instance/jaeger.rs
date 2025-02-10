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

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use catalog::CatalogManagerRef;
use common_function::function::{Function, FunctionRef};
use common_function::scalars::json::json_get::{
    JsonGetBool, JsonGetFloat, JsonGetInt, JsonGetString,
};
use common_function::scalars::udf::create_udf;
use common_function::state::FunctionState;
use common_query::Output;
use common_recordbatch::adapter::RecordBatchStreamAdapter;
use datafusion::dataframe::DataFrame;
use datafusion::execution::context::SessionContext;
use datafusion::execution::SessionStateBuilder;
use datafusion_expr::{col, lit, lit_timestamp_nano, Expr};
use query::QueryEngineRef;
use serde_json::Value as JsonValue;
use servers::error::{
    CatalogSnafu, CollectRecordbatchSnafu, DataFusionSnafu, Result as ServerResult,
    TableNotFoundSnafu,
};
use servers::http::jaeger::QueryTraceParams;
use servers::otlp::trace::TRACE_TABLE_NAME;
use servers::query_handler::JaegerQueryHandler;
use session::context::QueryContextRef;
use snafu::{OptionExt, ResultExt};
use table::table::adapter::DfTableProviderAdapter;

use super::Instance;

const SERVICE_NAME_COLUMN: &str = "service_name";
const TRACE_ID_COLUMN: &str = "trace_id";
const TIMESTAMP_COLUMN: &str = "timestamp";
const DURATION_NANO_COLUMN: &str = "duration_nano";
const SPAN_ID_COLUMN: &str = "span_id";
const SPAN_NAME_COLUMN: &str = "span_name";
const SPAN_KIND_COLUMN: &str = "span_kind";
const SPAN_ATTRIBUTES_COLUMN: &str = "span_attributes";

const DEFAULT_LIMIT: usize = 100;

#[async_trait]
impl JaegerQueryHandler for Instance {
    async fn get_services(&self, ctx: QueryContextRef) -> ServerResult<Output> {
        // It's equivalent to `SELECT DISTINCT(service_name) FROM {db}.{trace_table}`.
        Ok(query_trace_table(
            ctx,
            self.catalog_manager(),
            self.query_engine(),
            vec![col(SERVICE_NAME_COLUMN)],
            vec![],
            Some(DEFAULT_LIMIT),
            None,
            true,
        )
        .await?)
    }

    async fn get_operations(
        &self,
        ctx: QueryContextRef,
        service_name: &str,
        span_kind: Option<&str>,
    ) -> ServerResult<Output> {
        let mut filters = vec![col(SERVICE_NAME_COLUMN).eq(lit(service_name))];

        if let Some(span_kind) = span_kind {
            filters.push(
                col(SPAN_KIND_COLUMN).eq(lit(format!("SPAN_KIND_{}", span_kind.to_uppercase()))),
            );
        }

        // It's equivalent to `SELECT span_name, span_kind FROM {db}.{trace_table} WHERE service_name = '{service_name}'`.
        Ok(query_trace_table(
            ctx,
            self.catalog_manager(),
            self.query_engine(),
            vec![
                col(SPAN_NAME_COLUMN),
                col(SPAN_KIND_COLUMN),
                col(SERVICE_NAME_COLUMN),
            ],
            filters,
            Some(DEFAULT_LIMIT),
            None,
            false,
        )
        .await?)
    }

    async fn get_trace(&self, ctx: QueryContextRef, trace_id: &str) -> ServerResult<Output> {
        // It's equivalent to `SELECT trace_id, timestamp, duration_nano, service_name, span_name, span_id, span_attributes FROM {db}.{trace_table} WHERE trace_id = '{trace_id}'`.
        let selects = vec![
            col(TRACE_ID_COLUMN),
            col(TIMESTAMP_COLUMN),
            col(DURATION_NANO_COLUMN),
            col(SERVICE_NAME_COLUMN),
            col(SPAN_NAME_COLUMN),
            col(SPAN_ID_COLUMN),
            col(SPAN_ATTRIBUTES_COLUMN),
        ];

        let filters = vec![col(TRACE_ID_COLUMN).eq(lit(trace_id))];

        Ok(query_trace_table(
            ctx,
            self.catalog_manager(),
            self.query_engine(),
            selects,
            filters,
            Some(DEFAULT_LIMIT),
            None,
            false,
        )
        .await?)
    }

    async fn find_traces(
        &self,
        ctx: QueryContextRef,
        query_params: QueryTraceParams,
    ) -> ServerResult<Output> {
        let selects = vec![
            col(TRACE_ID_COLUMN),
            col(TIMESTAMP_COLUMN),
            col(DURATION_NANO_COLUMN),
            col(SERVICE_NAME_COLUMN),
            col(SPAN_NAME_COLUMN),
            col(SPAN_ID_COLUMN),
            col(SPAN_ATTRIBUTES_COLUMN),
        ];

        let mut filters = vec![];

        if let Some(operation_name) = query_params.operation_name {
            filters.push(col(SPAN_NAME_COLUMN).eq(lit(operation_name)));
        }

        if let Some(start_time) = query_params.start_time {
            filters.push(col(TIMESTAMP_COLUMN).gt_eq(lit_timestamp_nano(start_time)));
        }

        if let Some(end_time) = query_params.end_time {
            filters.push(col(TIMESTAMP_COLUMN).lt_eq(lit_timestamp_nano(end_time)));
        }

        if let Some(min_duration) = query_params.min_duration {
            filters.push(col(DURATION_NANO_COLUMN).gt_eq(lit(min_duration)));
        }

        if let Some(max_duration) = query_params.max_duration {
            filters.push(col(DURATION_NANO_COLUMN).lt_eq(lit(max_duration)));
        }

        Ok(query_trace_table(
            ctx,
            self.catalog_manager(),
            self.query_engine(),
            selects,
            filters,
            Some(DEFAULT_LIMIT),
            query_params.tags,
            false,
        )
        .await?)
    }
}

#[allow(clippy::too_many_arguments)]
async fn query_trace_table(
    ctx: QueryContextRef,
    catalog_manager: &CatalogManagerRef,
    query_engine: &QueryEngineRef,
    selects: Vec<Expr>,
    filters: Vec<Expr>,
    limit: Option<usize>,
    tags: Option<HashMap<String, JsonValue>>,
    distinct: bool,
) -> ServerResult<Output> {
    let db = ctx.get_db_string();
    let table = catalog_manager
        .table(ctx.current_catalog(), &db, TRACE_TABLE_NAME, Some(&ctx))
        .await
        .context(CatalogSnafu)?
        .with_context(|| TableNotFoundSnafu {
            table: TRACE_TABLE_NAME,
            catalog: ctx.current_catalog().to_string(),
            schema: db.to_string(),
        })?;

    let df_context = create_df_context(query_engine, ctx.clone())?;

    let dataframe = df_context
        .read_table(Arc::new(DfTableProviderAdapter::new(table)))
        .context(DataFusionSnafu)?;

    let dataframe = dataframe.select(selects).context(DataFusionSnafu)?;

    // Apply all filters
    let dataframe = filters
        .into_iter()
        .chain(tags.map_or(Ok(vec![]), |t| tags_filters(&dataframe, t))?)
        .try_fold(dataframe, |df, expr| {
            df.filter(expr).context(DataFusionSnafu)
        })?;

    // Apply the distinct if needed.
    let dataframe = if distinct {
        dataframe.distinct().context(DataFusionSnafu)?
    } else {
        dataframe
    };

    // Apply the limit if needed.
    let dataframe = if let Some(limit) = limit {
        dataframe.limit(0, Some(limit)).context(DataFusionSnafu)?
    } else {
        dataframe
    };

    // Execute the query and collect the result.
    let stream = dataframe.execute_stream().await.context(DataFusionSnafu)?;

    let output = Output::new_with_stream(Box::pin(
        RecordBatchStreamAdapter::try_new(stream).context(CollectRecordbatchSnafu)?,
    ));

    Ok(output)
}

// To decouple from the global session context, it will create a new session context and register the necessary UDFs.
fn create_df_context(
    query_engine: &QueryEngineRef,
    ctx: QueryContextRef,
) -> ServerResult<SessionContext> {
    let df_context = SessionContext::new_with_state(
        SessionStateBuilder::new_from_existing(query_engine.engine_state().session_state()).build(),
    );

    let udfs: Vec<FunctionRef> = vec![
        Arc::new(JsonGetInt),
        Arc::new(JsonGetFloat),
        Arc::new(JsonGetBool),
        Arc::new(JsonGetString),
    ];

    for udf in udfs {
        df_context
            .register_udf(create_udf(udf, ctx.clone(), Arc::new(FunctionState::default())).into());
    }

    Ok(df_context)
}

fn tags_filters(
    dataframe: &DataFrame,
    tags: HashMap<String, JsonValue>,
) -> ServerResult<Vec<Expr>> {
    let mut filters = vec![];

    // NOTE: The key of the tags may contain `.`, for example: `http.status_code`, so we need to use `["http.status_code"]` in json path to access the value.
    for (key, value) in tags.iter() {
        if let JsonValue::String(value) = value {
            filters.push(
                dataframe
                    .registry()
                    .udf(JsonGetString {}.name())
                    .context(DataFusionSnafu)?
                    .call(vec![
                        col(SPAN_ATTRIBUTES_COLUMN),
                        lit(format!("[\"{}\"]", key)),
                    ])
                    .eq(lit(value)),
            );
        }
        if let JsonValue::Number(value) = value {
            if value.is_i64() {
                filters.push(
                    dataframe
                        .registry()
                        .udf(JsonGetInt {}.name())
                        .context(DataFusionSnafu)?
                        .call(vec![
                            col(SPAN_ATTRIBUTES_COLUMN),
                            lit(format!("[\"{}\"]", key)),
                        ])
                        .eq(lit(value.as_i64().unwrap())),
                );
            }
            if value.is_f64() {
                filters.push(
                    dataframe
                        .registry()
                        .udf(JsonGetFloat {}.name())
                        .context(DataFusionSnafu)?
                        .call(vec![
                            col(SPAN_ATTRIBUTES_COLUMN),
                            lit(format!("[\"{}\"]", key)),
                        ])
                        .eq(lit(value.as_f64().unwrap())),
                );
            }
        }
        if let JsonValue::Bool(value) = value {
            filters.push(
                dataframe
                    .registry()
                    .udf(JsonGetBool {}.name())
                    .context(DataFusionSnafu)?
                    .call(vec![
                        col(SPAN_ATTRIBUTES_COLUMN),
                        lit(format!("[\"{}\"]", key)),
                    ])
                    .eq(lit(*value)),
            );
        }
    }

    Ok(filters)
}
