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
use std::env;
use std::time::Instant;

use aide::transform::TransformOperation;
use axum::extract::{Json, Query, State};
use axum::response::{IntoResponse, Response};
use axum::{Extension, Form};
use common_error::ext::ErrorExt;
use common_error::status_code::StatusCode;
use common_query::Output;
use common_recordbatch::util;
use query::parser::PromQuery;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use session::context::QueryContextRef;

use crate::http::csv_result::CsvResponse;
use crate::http::error_result::ErrorResponse;
use crate::http::greptime_result_v1::GreptimedbV1Response;
use crate::http::influxdb_result_v1::InfluxdbV1Response;
use crate::http::{
    ApiState, Epoch, GreptimeOptionsConfigState, GreptimeQueryOutput, HttpRecordsOutput,
    HttpResponse, ResponseFormat,
};
use crate::metrics_handler::MetricsHandler;
use crate::query_handler::sql::ServerSqlQueryHandlerRef;

#[derive(Debug, Default, Serialize, Deserialize, JsonSchema)]
pub struct SqlQuery {
    pub db: Option<String>,
    pub sql: Option<String>,
    // (Optional) result format: [`greptimedb_v1`, `influxdb_v1`, `csv`],
    // the default value is `greptimedb_v1`
    pub format: Option<String>,
    // Returns epoch timestamps with the specified precision.
    // Both u and µ indicate microseconds.
    // epoch = [ns,u,µ,ms,s],
    //
    // TODO(jeremy): currently, only InfluxDB result format is supported,
    // and all columns of the `Timestamp` type will be converted to their
    // specified time precision. Maybe greptimedb format can support this
    // param too.
    pub epoch: Option<String>,
}

/// Handler to execute sql
#[axum_macros::debug_handler]
pub async fn sql(
    State(state): State<ApiState>,
    Query(query_params): Query<SqlQuery>,
    Extension(query_ctx): Extension<QueryContextRef>,
    Form(form_params): Form<SqlQuery>,
) -> HttpResponse {
    let sql_handler = &state.sql_handler;

    let start = Instant::now();
    let sql = query_params.sql.or(form_params.sql);
    let db = query_ctx.get_db_string();
    let format = query_params
        .format
        .or(form_params.format)
        .map(|s| s.to_lowercase())
        .map(|s| ResponseFormat::parse(s.as_str()).unwrap_or(ResponseFormat::GreptimedbV1))
        .unwrap_or(ResponseFormat::GreptimedbV1);
    let epoch = query_params
        .epoch
        .or(form_params.epoch)
        .map(|s| s.to_lowercase())
        .map(|s| Epoch::parse(s.as_str()).unwrap_or(Epoch::Millisecond));

    let _timer = crate::metrics::METRIC_HTTP_SQL_ELAPSED
        .with_label_values(&[db.as_str()])
        .start_timer();

    let result = if let Some(sql) = &sql {
        if let Some((status, msg)) = validate_schema(sql_handler.clone(), query_ctx.clone()).await {
            Err((status, msg))
        } else {
            Ok(sql_handler.do_query(sql, query_ctx).await)
        }
    } else {
        Err((
            StatusCode::InvalidArguments,
            "sql parameter is required.".to_string(),
        ))
    };

    let outputs = match result {
        Err((status, msg)) => {
            return HttpResponse::Error(
                ErrorResponse::from_error_message(format, status, msg)
                    .with_execution_time(start.elapsed().as_millis() as u64),
            );
        }
        Ok(outputs) => outputs,
    };

    let resp = match format {
        ResponseFormat::Csv => CsvResponse::from_output(outputs).await,
        ResponseFormat::GreptimedbV1 => GreptimedbV1Response::from_output(outputs).await,
        ResponseFormat::InfluxdbV1 => InfluxdbV1Response::from_output(outputs, epoch).await,
    };

    resp.with_execution_time(start.elapsed().as_millis() as u64)
}

/// Create a response from query result
pub async fn from_output(
    ty: ResponseFormat,
    outputs: Vec<crate::error::Result<Output>>,
) -> Result<Vec<GreptimeQueryOutput>, ErrorResponse> {
    // TODO(sunng87): this api response structure cannot represent error well.
    //  It hides successful execution results from error response
    let mut results = Vec::with_capacity(outputs.len());
    for out in outputs {
        match out {
            Ok(Output::AffectedRows(rows)) => {
                results.push(GreptimeQueryOutput::AffectedRows(rows));
            }
            Ok(Output::Stream(stream)) => {
                // TODO(sunng87): streaming response
                match util::collect(stream).await {
                    Ok(rows) => match HttpRecordsOutput::try_from(rows) {
                        Ok(rows) => {
                            results.push(GreptimeQueryOutput::Records(rows));
                        }
                        Err(err) => {
                            return Err(ErrorResponse::from_error(ty, err));
                        }
                    },
                    Err(err) => {
                        return Err(ErrorResponse::from_error(ty, err));
                    }
                }
            }
            Ok(Output::RecordBatches(rbs)) => match HttpRecordsOutput::try_from(rbs.take()) {
                Ok(rows) => {
                    results.push(GreptimeQueryOutput::Records(rows));
                }
                Err(err) => {
                    return Err(ErrorResponse::from_error(ty, err));
                }
            },
            Err(err) => {
                return Err(ErrorResponse::from_error(ty, err));
            }
        }
    }

    Ok(results)
}

#[derive(Debug, Default, Serialize, Deserialize, JsonSchema)]
pub struct PromqlQuery {
    pub query: String,
    pub start: String,
    pub end: String,
    pub step: String,
    pub db: Option<String>,
}

impl From<PromqlQuery> for PromQuery {
    fn from(query: PromqlQuery) -> Self {
        PromQuery {
            query: query.query,
            start: query.start,
            end: query.end,
            step: query.step,
        }
    }
}

/// Handler to execute promql
#[axum_macros::debug_handler]
pub async fn promql(
    State(state): State<ApiState>,
    Query(params): Query<PromqlQuery>,
    Extension(query_ctx): Extension<QueryContextRef>,
) -> Response {
    let sql_handler = &state.sql_handler;
    let exec_start = Instant::now();
    let db = query_ctx.get_db_string();
    let _timer = crate::metrics::METRIC_HTTP_PROMQL_ELAPSED
        .with_label_values(&[db.as_str()])
        .start_timer();

    let resp = if let Some((status, msg)) =
        validate_schema(sql_handler.clone(), query_ctx.clone()).await
    {
        let resp = ErrorResponse::from_error_message(ResponseFormat::GreptimedbV1, status, msg);
        HttpResponse::Error(resp)
    } else {
        let prom_query = params.into();
        let outputs = sql_handler.do_promql_query(&prom_query, query_ctx).await;
        GreptimedbV1Response::from_output(outputs).await
    };

    resp.with_execution_time(exec_start.elapsed().as_millis() as u64)
        .into_response()
}

pub(crate) fn sql_docs(op: TransformOperation) -> TransformOperation {
    op.response::<200, Json<HttpResponse>>()
}

/// Handler to export metrics
#[axum_macros::debug_handler]
pub async fn metrics(
    State(state): State<MetricsHandler>,
    Query(_params): Query<HashMap<String, String>>,
) -> String {
    // A default ProcessCollector is registered automatically in prometheus.
    // We do not need to explicitly collect process-related data.
    // But ProcessCollector only support on linux.

    #[cfg(not(windows))]
    if let Some(c) = crate::metrics::jemalloc::JEMALLOC_COLLECTOR.as_ref() {
        if let Err(e) = c.update() {
            common_telemetry::error!(e; "Failed to update jemalloc metrics");
        }
    }
    state.render()
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct HealthQuery {}

#[derive(Debug, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct HealthResponse {}

/// Handler to export healthy check
///
/// Currently simply return status "200 OK" (default) with an empty json payload "{}"
#[axum_macros::debug_handler]
pub async fn health(Query(_params): Query<HealthQuery>) -> Json<HealthResponse> {
    Json(HealthResponse {})
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct StatusResponse<'a> {
    pub source_time: &'a str,
    pub commit: &'a str,
    pub branch: &'a str,
    pub rustc_version: &'a str,
    pub hostname: String,
    pub version: &'a str,
}

/// Handler to expose information info about runtime, build, etc.
#[axum_macros::debug_handler]
pub async fn status() -> Json<StatusResponse<'static>> {
    let hostname = hostname::get()
        .map(|s| s.to_string_lossy().to_string())
        .unwrap_or_else(|_| "unknown".to_string());
    Json(StatusResponse {
        source_time: env!("SOURCE_TIMESTAMP"),
        commit: env!("GIT_COMMIT"),
        branch: env!("GIT_BRANCH"),
        rustc_version: env!("RUSTC_VERSION"),
        hostname,
        version: env!("CARGO_PKG_VERSION"),
    })
}

/// Handler to expose configuration information info about runtime, build, etc.
#[axum_macros::debug_handler]
pub async fn config(State(state): State<GreptimeOptionsConfigState>) -> Response {
    (axum::http::StatusCode::OK, state.greptime_config_options).into_response()
}

async fn validate_schema(
    sql_handler: ServerSqlQueryHandlerRef,
    query_ctx: QueryContextRef,
) -> Option<(StatusCode, String)> {
    match sql_handler
        .is_valid_schema(query_ctx.current_catalog(), query_ctx.current_schema())
        .await
    {
        Ok(true) => None,
        Ok(false) => Some((
            StatusCode::DatabaseNotFound,
            format!("Database not found: {}", query_ctx.get_db_string()),
        )),
        Err(e) => Some((
            StatusCode::Internal,
            format!(
                "Error checking database: {}, {}",
                query_ctx.get_db_string(),
                e.output_msg(),
            ),
        )),
    }
}
