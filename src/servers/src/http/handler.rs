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
use query::parser::PromQuery;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use session::context::QueryContextRef;

use crate::http::{ApiState, GreptimeOptionsConfigState, JsonResponse};
use crate::metrics_handler::MetricsHandler;
use crate::query_handler::sql::ServerSqlQueryHandlerRef;

#[derive(Debug, Default, Serialize, Deserialize, JsonSchema)]
pub struct SqlQuery {
    pub db: Option<String>,
    pub sql: Option<String>,
}

/// Handler to execute sql
#[axum_macros::debug_handler]
pub async fn sql(
    State(state): State<ApiState>,
    Query(query_params): Query<SqlQuery>,
    Extension(query_ctx): Extension<QueryContextRef>,
    Form(form_params): Form<SqlQuery>,
) -> Json<JsonResponse> {
    let sql_handler = &state.sql_handler;

    let start = Instant::now();
    let sql = query_params.sql.or(form_params.sql);
    let db = query_ctx.get_db_string();
    let _timer = crate::metrics::METRIC_HTTP_SQL_ELAPSED
        .with_label_values(&[db.as_str()])
        .start_timer();

    let resp = if let Some(sql) = &sql {
        if let Some(resp) = validate_schema(sql_handler.clone(), query_ctx.clone()).await {
            return Json(resp);
        }

        JsonResponse::from_output(sql_handler.do_query(sql, query_ctx).await).await
    } else {
        JsonResponse::with_error_message(
            "sql parameter is required.".to_string(),
            StatusCode::InvalidArguments,
        )
    };

    Json(resp.with_execution_time(start.elapsed().as_millis()))
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
) -> Json<JsonResponse> {
    let sql_handler = &state.sql_handler;
    let exec_start = Instant::now();
    let db = query_ctx.get_db_string();
    let _timer = crate::metrics::METRIC_HTTP_PROMQL_ELAPSED
        .with_label_values(&[db.as_str()])
        .start_timer();

    if let Some(resp) = validate_schema(sql_handler.clone(), query_ctx.clone()).await {
        return Json(resp);
    }

    let prom_query = params.into();
    let resp =
        JsonResponse::from_output(sql_handler.do_promql_query(&prom_query, query_ctx).await).await;

    Json(resp.with_execution_time(exec_start.elapsed().as_millis()))
}

pub(crate) fn sql_docs(op: TransformOperation) -> TransformOperation {
    op.response::<200, Json<JsonResponse>>()
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
) -> Option<JsonResponse> {
    match sql_handler
        .is_valid_schema(query_ctx.current_catalog(), query_ctx.current_schema())
        .await
    {
        Ok(false) => Some(JsonResponse::with_error_message(
            format!("Database not found: {}", query_ctx.get_db_string()),
            StatusCode::DatabaseNotFound,
        )),
        Err(e) => Some(JsonResponse::with_error_message(
            format!(
                "Error checking database: {}, {}",
                query_ctx.get_db_string(),
                e.output_msg(),
            ),
            StatusCode::Internal,
        )),
        _ => None,
    }
}
