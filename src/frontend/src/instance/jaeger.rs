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

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;
use catalog::CatalogManagerRef;
use common_catalog::consts::{
    TRACE_TABLE_NAME, trace_operations_table_name, trace_services_table_name,
};
use common_function::function::FunctionRef;
use common_function::scalars::json::json_get::{
    JsonGetBool, JsonGetFloat, JsonGetInt, JsonGetString,
};
use common_function::scalars::udf::create_udf;
use common_query::{Output, OutputData};
use common_recordbatch::adapter::RecordBatchStreamAdapter;
use common_recordbatch::util;
use common_telemetry::warn;
use datafusion::dataframe::DataFrame;
use datafusion::execution::SessionStateBuilder;
use datafusion::execution::context::SessionContext;
use datafusion_expr::select_expr::SelectExpr;
use datafusion_expr::{Expr, SortExpr, col, lit, lit_timestamp_nano, wildcard};
use datatypes::value::ValueRef;
use query::QueryEngineRef;
use serde_json::Value as JsonValue;
use servers::error::{
    CatalogSnafu, CollectRecordbatchSnafu, DataFusionSnafu, Result as ServerResult,
    TableNotFoundSnafu,
};
use servers::http::jaeger::{JAEGER_QUERY_TABLE_NAME_KEY, QueryTraceParams};
use servers::otlp::trace::{
    DURATION_NANO_COLUMN, KEY_OTEL_STATUS_ERROR_KEY, SERVICE_NAME_COLUMN, SPAN_ATTRIBUTES_COLUMN,
    SPAN_KIND_COLUMN, SPAN_KIND_PREFIX, SPAN_NAME_COLUMN, SPAN_STATUS_CODE, SPAN_STATUS_ERROR,
    TIMESTAMP_COLUMN, TRACE_ID_COLUMN,
};
use servers::query_handler::JaegerQueryHandler;
use session::context::QueryContextRef;
use snafu::{OptionExt, ResultExt};
use table::requests::{TABLE_DATA_MODEL, TABLE_DATA_MODEL_TRACE_V1};
use table::table::adapter::DfTableProviderAdapter;

use crate::instance::Instance;

const DEFAULT_LIMIT: usize = 2000;

#[async_trait]
impl JaegerQueryHandler for Instance {
    async fn get_services(&self, ctx: QueryContextRef) -> ServerResult<Output> {
        // It's equivalent to `SELECT DISTINCT(service_name) FROM {db}.{trace_table}`.
        Ok(query_trace_table(
            ctx,
            self.catalog_manager(),
            self.query_engine(),
            vec![SelectExpr::from(col(SERVICE_NAME_COLUMN))],
            vec![],
            vec![],
            None,
            None,
            vec![col(SERVICE_NAME_COLUMN)],
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
            filters.push(col(SPAN_KIND_COLUMN).eq(lit(format!(
                "{}{}",
                SPAN_KIND_PREFIX,
                span_kind.to_uppercase()
            ))));
        }

        // It's equivalent to the following SQL query:
        //
        // ```
        // SELECT DISTINCT span_name, span_kind
        // FROM
        //   {db}.{trace_table}
        // WHERE
        //   service_name = '{service_name}' AND
        //   span_kind = '{span_kind}'
        // ORDER BY
        //   span_name ASC
        // ```.
        Ok(query_trace_table(
            ctx,
            self.catalog_manager(),
            self.query_engine(),
            vec![
                SelectExpr::from(col(SPAN_NAME_COLUMN)),
                SelectExpr::from(col(SPAN_KIND_COLUMN)),
                SelectExpr::from(col(SERVICE_NAME_COLUMN)),
                SelectExpr::from(col(TIMESTAMP_COLUMN)),
            ],
            filters,
            vec![col(SPAN_NAME_COLUMN).sort(true, false)], // Sort by span_name in ascending order.
            Some(DEFAULT_LIMIT),
            None,
            vec![col(SPAN_NAME_COLUMN), col(SPAN_KIND_COLUMN)],
        )
        .await?)
    }

    async fn get_trace(
        &self,
        ctx: QueryContextRef,
        trace_id: &str,
        start_time: Option<i64>,
        end_time: Option<i64>,
    ) -> ServerResult<Output> {
        // It's equivalent to the following SQL query:
        //
        // ```
        // SELECT
        //   *
        // FROM
        //   {db}.{trace_table}
        // WHERE
        //   trace_id = '{trace_id}' AND
        //   timestamp >= {start_time} AND
        //   timestamp <= {end_time}
        // ORDER BY
        //   timestamp DESC
        // ```.
        let selects = vec![wildcard()];

        let mut filters = vec![col(TRACE_ID_COLUMN).eq(lit(trace_id))];

        if let Some(start_time) = start_time {
            filters.push(col(TIMESTAMP_COLUMN).gt_eq(lit_timestamp_nano(start_time)));
        }

        if let Some(end_time) = end_time {
            filters.push(col(TIMESTAMP_COLUMN).lt_eq(lit_timestamp_nano(end_time)));
        }

        Ok(query_trace_table(
            ctx,
            self.catalog_manager(),
            self.query_engine(),
            selects,
            filters,
            vec![col(TIMESTAMP_COLUMN).sort(false, false)], // Sort by timestamp in descending order.
            Some(DEFAULT_LIMIT),
            None,
            vec![],
        )
        .await?)
    }

    async fn find_traces(
        &self,
        ctx: QueryContextRef,
        query_params: QueryTraceParams,
    ) -> ServerResult<Output> {
        let mut filters = vec![];

        // `service_name` is already validated in `from_jaeger_query_params()`, so no additional check needed here.
        filters.push(col(SERVICE_NAME_COLUMN).eq(lit(query_params.service_name)));

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

        // Get all distinct trace ids that match the filters.
        // It's equivalent to the following SQL query:
        //
        // ```
        // SELECT DISTINCT trace_id
        // FROM
        //   {db}.{trace_table}
        // WHERE
        //   service_name = '{service_name}' AND
        //   operation_name = '{operation_name}' AND
        //   timestamp >= {start_time} AND
        //   timestamp <= {end_time} AND
        //   duration >= {min_duration} AND
        //   duration <= {max_duration}
        // LIMIT {limit}
        // ```.
        let output = query_trace_table(
            ctx.clone(),
            self.catalog_manager(),
            self.query_engine(),
            vec![wildcard()],
            filters,
            vec![],
            Some(query_params.limit.unwrap_or(DEFAULT_LIMIT)),
            query_params.tags,
            vec![col(TRACE_ID_COLUMN)],
        )
        .await?;

        // Get all traces that match the trace ids from the previous query.
        // It's equivalent to the following SQL query:
        //
        // ```
        // SELECT *
        // FROM
        //   {db}.{trace_table}
        // WHERE
        //   trace_id IN ({trace_ids}) AND
        //   timestamp >= {start_time} AND
        //   timestamp <= {end_time}
        // ```
        let mut filters = vec![
            col(TRACE_ID_COLUMN).in_list(
                trace_ids_from_output(output)
                    .await?
                    .iter()
                    .map(lit)
                    .collect::<Vec<Expr>>(),
                false,
            ),
        ];

        if let Some(start_time) = query_params.start_time {
            filters.push(col(TIMESTAMP_COLUMN).gt_eq(lit_timestamp_nano(start_time)));
        }

        if let Some(end_time) = query_params.end_time {
            filters.push(col(TIMESTAMP_COLUMN).lt_eq(lit_timestamp_nano(end_time)));
        }

        Ok(query_trace_table(
            ctx,
            self.catalog_manager(),
            self.query_engine(),
            vec![wildcard()],
            filters,
            vec![],
            None,
            None,
            vec![],
        )
        .await?)
    }
}

#[allow(clippy::too_many_arguments)]
async fn query_trace_table(
    ctx: QueryContextRef,
    catalog_manager: &CatalogManagerRef,
    query_engine: &QueryEngineRef,
    selects: Vec<SelectExpr>,
    filters: Vec<Expr>,
    sorts: Vec<SortExpr>,
    limit: Option<usize>,
    tags: Option<HashMap<String, JsonValue>>,
    distincts: Vec<Expr>,
) -> ServerResult<Output> {
    let trace_table_name = ctx
        .extension(JAEGER_QUERY_TABLE_NAME_KEY)
        .unwrap_or(TRACE_TABLE_NAME);

    // If only select services, use the trace services table.
    // If querying operations (distinct by span_name and span_kind), use the trace operations table.
    let table_name = {
        if match selects.as_slice() {
            [SelectExpr::Expression(x)] => x == &col(SERVICE_NAME_COLUMN),
            _ => false,
        } {
            &trace_services_table_name(trace_table_name)
        } else if !distincts.is_empty()
            && distincts.contains(&col(SPAN_NAME_COLUMN))
            && distincts.contains(&col(SPAN_KIND_COLUMN))
        {
            &trace_operations_table_name(trace_table_name)
        } else {
            trace_table_name
        }
    };

    let table = catalog_manager
        .table(
            ctx.current_catalog(),
            &ctx.current_schema(),
            table_name,
            Some(&ctx),
        )
        .await
        .context(CatalogSnafu)?
        .with_context(|| TableNotFoundSnafu {
            table: table_name,
            catalog: ctx.current_catalog(),
            schema: ctx.current_schema(),
        })?;

    let is_data_model_v1 = table
        .clone()
        .table_info()
        .meta
        .options
        .extra_options
        .get(TABLE_DATA_MODEL)
        .map(|s| s.as_str())
        == Some(TABLE_DATA_MODEL_TRACE_V1);

    // collect to set
    let col_names = table
        .table_info()
        .meta
        .field_column_names()
        .cloned()
        .collect::<HashSet<String>>();

    let df_context = create_df_context(query_engine)?;

    let dataframe = df_context
        .read_table(Arc::new(DfTableProviderAdapter::new(table)))
        .context(DataFusionSnafu)?;

    let dataframe = dataframe.select(selects).context(DataFusionSnafu)?;

    // Apply all filters.
    let dataframe = filters
        .into_iter()
        .chain(tags.map_or(Ok(vec![]), |t| {
            tags_filters(&dataframe, t, is_data_model_v1, &col_names)
        })?)
        .try_fold(dataframe, |df, expr| {
            df.filter(expr).context(DataFusionSnafu)
        })?;

    // Apply the distinct if needed.
    let dataframe = if !distincts.is_empty() {
        dataframe
            .distinct_on(distincts.clone(), distincts, None)
            .context(DataFusionSnafu)?
    } else {
        dataframe
    };

    // Apply the sorts if needed.
    let dataframe = if !sorts.is_empty() {
        dataframe.sort(sorts).context(DataFusionSnafu)?
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

// The current implementation registers UDFs during the planning stage, which makes it difficult
// to utilize them through DataFrame APIs. To address this limitation, we create a new session
// context and register the required UDFs, allowing them to be decoupled from the global context.
// TODO(zyy17): Is it possible or necessary to reuse the existing session context?
fn create_df_context(query_engine: &QueryEngineRef) -> ServerResult<SessionContext> {
    let df_context = SessionContext::new_with_state(
        SessionStateBuilder::new_from_existing(query_engine.engine_state().session_state()).build(),
    );

    // The following JSON UDFs will be used for tags filters on v0 data model.
    let udfs: Vec<FunctionRef> = vec![
        Arc::new(JsonGetInt::default()),
        Arc::new(JsonGetFloat::default()),
        Arc::new(JsonGetBool::default()),
        Arc::new(JsonGetString::default()),
    ];

    for udf in udfs {
        df_context.register_udf(create_udf(udf));
    }

    Ok(df_context)
}

fn json_tag_filters(
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
                    .udf(JsonGetString::NAME)
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
                        .udf(JsonGetInt::NAME)
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
                        .udf(JsonGetFloat::NAME)
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
                    .udf(JsonGetBool::NAME)
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

fn flatten_tag_filters(
    tags: HashMap<String, JsonValue>,
    col_names: &HashSet<String>,
) -> ServerResult<Vec<Expr>> {
    let filters = tags
        .into_iter()
        .filter_map(|(key, value)| {
            if key == KEY_OTEL_STATUS_ERROR_KEY && value == JsonValue::Bool(true) {
                return Some(col(SPAN_STATUS_CODE).eq(lit(SPAN_STATUS_ERROR)));
            }

            // TODO(shuiyisong): add more precise mapping from key to col name
            let span_key = format!("\"span_attributes.{}\"", key);
            let resource_key = format!("\"resource_attributes.{}\"", key);
            match value {
                JsonValue::String(value) => {
                    if col_names.contains(&span_key) {
                        return Some(col(span_key).eq(lit(value)));
                    }
                    if col_names.contains(&resource_key) {
                        return Some(col(resource_key).eq(lit(value)));
                    }
                    warn!("tag key {} not found in table columns", key);
                    None
                }
                JsonValue::Number(value) => {
                    if value.is_f64() {
                        // safe to unwrap as checked previously
                        let value = value.as_f64().unwrap();
                        if col_names.contains(&span_key) {
                            return Some(col(span_key).eq(lit(value)));
                        }
                        if col_names.contains(&resource_key) {
                            return Some(col(resource_key).eq(lit(value)));
                        }
                        warn!("tag key {} not found in table columns", key);
                        None
                    } else {
                        let value = value.as_i64().unwrap();
                        if col_names.contains(&span_key) {
                            return Some(col(span_key).eq(lit(value)));
                        }
                        if col_names.contains(&resource_key) {
                            return Some(col(resource_key).eq(lit(value)));
                        }
                        warn!("tag key {} not found in table columns", key);
                        None
                    }
                }
                JsonValue::Bool(value) => {
                    if col_names.contains(&span_key) {
                        return Some(col(span_key).eq(lit(value)));
                    }
                    if col_names.contains(&resource_key) {
                        return Some(col(resource_key).eq(lit(value)));
                    }
                    warn!("tag key {} not found in table columns", key);
                    None
                }
                JsonValue::Null => {
                    if col_names.contains(&span_key) {
                        return Some(col(span_key).is_null());
                    }
                    if col_names.contains(&resource_key) {
                        return Some(col(resource_key).is_null());
                    }
                    warn!("tag key {} not found in table columns", key);
                    None
                }
                // not supported at the moment
                JsonValue::Array(_value) => None,
                JsonValue::Object(_value) => None,
            }
        })
        .collect();
    Ok(filters)
}

fn tags_filters(
    dataframe: &DataFrame,
    tags: HashMap<String, JsonValue>,
    is_data_model_v1: bool,
    col_names: &HashSet<String>,
) -> ServerResult<Vec<Expr>> {
    if is_data_model_v1 {
        flatten_tag_filters(tags, col_names)
    } else {
        json_tag_filters(dataframe, tags)
    }
}

// Get trace ids from the output in recordbatches.
async fn trace_ids_from_output(output: Output) -> ServerResult<Vec<String>> {
    if let OutputData::Stream(stream) = output.data {
        let schema = stream.schema().clone();
        let recordbatches = util::collect(stream)
            .await
            .context(CollectRecordbatchSnafu)?;

        // Only contains `trace_id` column in string type.
        if !recordbatches.is_empty()
            && schema.num_columns() == 1
            && schema.contains_column(TRACE_ID_COLUMN)
        {
            let mut trace_ids = vec![];
            for recordbatch in recordbatches {
                for col in recordbatch.columns().iter() {
                    for row_idx in 0..recordbatch.num_rows() {
                        if let ValueRef::String(value) = col.get_ref(row_idx) {
                            trace_ids.push(value.to_string());
                        }
                    }
                }
            }

            return Ok(trace_ids);
        }
    }

    Ok(vec![])
}
