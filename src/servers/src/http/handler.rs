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
use std::time::{Duration, Instant};

use axum::extract::rejection::FormRejection;
use axum::extract::{Json, Query, State};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::{Extension, Form};
use common_catalog::parse_catalog_and_schema_from_db_string;
use common_error::ext::ErrorExt;
use common_error::status_code::StatusCode;
use common_plugins::GREPTIME_EXEC_WRITE_COST;
use common_query::{Output, OutputData};
use common_recordbatch::{RecordBatch, SendableRecordBatchStream, util};
use common_telemetry::tracing;
use datafusion::physical_plan::ExecutionPlan;
use datatypes::schema::SchemaRef;
use futures::StreamExt;
use query::parser::{DEFAULT_LOOKBACK_STRING, PromQuery};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use session::context::{Channel, QueryContext, QueryContextRef};
use snafu::ResultExt;
use sql::dialect::GreptimeDbDialect;
use sql::parser::{ParseOptions, ParserContext};
use sql::statements::statement::Statement;

use crate::error::{FailedToParseQuerySnafu, InvalidQuerySnafu, Result};
use crate::http::header::collect_plan_metrics;
use crate::http::result::arrow_result::ArrowResponse;
use crate::http::result::csv_result::CsvResponse;
use crate::http::result::error_result::ErrorResponse;
use crate::http::result::greptime_result_v1::GreptimedbV1Response;
use crate::http::result::influxdb_result_v1::InfluxdbV1Response;
use crate::http::result::json_result::JsonResponse;
use crate::http::result::null_result::NullResponse;
use crate::http::result::table_result::TableResponse;
use crate::http::{
    ApiState, Epoch, GreptimeOptionsConfigState, GreptimeQueryOutput, HttpRecordsOutput,
    HttpResponse, ResponseFormat,
};
use crate::metrics_handler::MetricsHandler;
use crate::query_handler::sql::ServerSqlQueryHandlerRef;

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct SqlQuery {
    pub db: Option<String>,
    pub sql: Option<String>,
    // (Optional) result format: [`greptimedb_v1`, `influxdb_v1`, `csv`,
    // `arrow`],
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
    pub limit: Option<usize>,
    // For arrow output
    pub compression: Option<String>,
    pub snapshot_interval_ms: Option<u64>,
}

const DEFAULT_ANALYZE_SNAPSHOT_INTERVAL_MS: u64 = 5000;
const MIN_ANALYZE_SNAPSHOT_INTERVAL_MS: u64 = 1000;
const MAX_ANALYZE_SNAPSHOT_INTERVAL_MS: u64 = 60000;

#[derive(Serialize)]
struct AnalyzeStreamPayload {
    seq: u64,
    state: &'static str,
    partial: bool,
    elapsed_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    metrics: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    output: Option<GreptimeQueryOutput>,
    #[serde(skip_serializing_if = "Option::is_none")]
    reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    code: Option<u32>,
}

struct AnalyzeStreamState {
    stream: SendableRecordBatchStream,
    schema: SchemaRef,
    plan: Option<Arc<dyn ExecutionPlan>>,
    batches: Vec<RecordBatch>,
    seq: u64,
    start: Instant,
    requested_interval_ms: u64,
    current_interval_ms: u64,
    done: bool,
}

/// Handler to execute sql
#[axum_macros::debug_handler]
#[tracing::instrument(skip_all, fields(protocol = "http", request_type = "sql"))]
pub async fn sql(
    State(state): State<ApiState>,
    Query(query_params): Query<SqlQuery>,
    Extension(mut query_ctx): Extension<QueryContext>,
    Form(form_params): Form<SqlQuery>,
) -> HttpResponse {
    let start = Instant::now();
    let sql_handler = &state.sql_handler;
    if let Some(db) = &query_params.db.or(form_params.db) {
        let (catalog, schema) = parse_catalog_and_schema_from_db_string(db);
        query_ctx.set_current_catalog(&catalog);
        query_ctx.set_current_schema(&schema);
    }
    let db = query_ctx.get_db_string();

    query_ctx.set_channel(Channel::HttpSql);
    let query_ctx = Arc::new(query_ctx);

    let _timer = crate::metrics::METRIC_HTTP_SQL_ELAPSED
        .with_label_values(&[db.as_str()])
        .start_timer();

    let sql = query_params.sql.or(form_params.sql);
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

    let result = if let Some(sql) = &sql {
        if let Some((status, msg)) = validate_schema(sql_handler.clone(), query_ctx.clone()).await {
            Err((status, msg))
        } else {
            Ok(sql_handler.do_query(sql, query_ctx.clone()).await)
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
                ErrorResponse::from_error_message(status, msg)
                    .with_execution_time(start.elapsed().as_millis() as u64),
            );
        }
        Ok(outputs) => outputs,
    };

    let mut resp = match format {
        ResponseFormat::Arrow => {
            ArrowResponse::from_output(outputs, query_params.compression).await
        }
        ResponseFormat::Csv(with_names, with_types) => {
            CsvResponse::from_output(outputs, with_names, with_types).await
        }
        ResponseFormat::Table => TableResponse::from_output(outputs).await,
        ResponseFormat::GreptimedbV1 => GreptimedbV1Response::from_output(outputs).await,
        ResponseFormat::InfluxdbV1 => InfluxdbV1Response::from_output(outputs, epoch).await,
        ResponseFormat::Json => JsonResponse::from_output(outputs).await,
        ResponseFormat::Null => NullResponse::from_output(outputs).await,
    };

    if let Some(limit) = query_params.limit {
        resp = resp.with_limit(limit);
    }
    resp.with_execution_time(start.elapsed().as_millis() as u64)
}

/// Handler to stream partial `EXPLAIN ANALYZE VERBOSE` metrics as SSE.
#[axum_macros::debug_handler]
#[tracing::instrument(
    skip_all,
    fields(protocol = "http", request_type = "sql_analyze_stream")
)]
pub async fn sql_analyze_stream(
    State(state): State<ApiState>,
    Query(query_params): Query<SqlQuery>,
    Extension(mut query_ctx): Extension<QueryContext>,
    form_params: std::result::Result<Form<SqlQuery>, FormRejection>,
) -> Response {
    let start = Instant::now();
    let form_params = match form_params {
        Ok(Form(params)) => params,
        Err(err) => {
            if err.status() != axum::http::StatusCode::UNSUPPORTED_MEDIA_TYPE {
                return ErrorResponse::from_error_message(
                    StatusCode::InvalidArguments,
                    err.body_text(),
                )
                .with_execution_time(start.elapsed().as_millis() as u64)
                .into_response();
            }
            SqlQuery::default()
        }
    };
    let sql_handler = &state.sql_handler;
    if let Some(db) = &query_params.db.or(form_params.db) {
        let (catalog, schema) = parse_catalog_and_schema_from_db_string(db);
        query_ctx.set_current_catalog(&catalog);
        query_ctx.set_current_schema(&schema);
    }
    query_ctx.set_channel(Channel::HttpSql);
    let query_ctx = Arc::new(query_ctx);

    let Some(sql) = query_params.sql.or(form_params.sql) else {
        return ErrorResponse::from_error_message(
            StatusCode::InvalidArguments,
            "sql parameter is required.".to_string(),
        )
        .with_execution_time(start.elapsed().as_millis() as u64)
        .into_response();
    };
    if let Some((status, msg)) = validate_schema(sql_handler.clone(), query_ctx.clone()).await {
        return ErrorResponse::from_error_message(status, msg)
            .with_execution_time(start.elapsed().as_millis() as u64)
            .into_response();
    }

    let interval_ms = query_params
        .snapshot_interval_ms
        .or(form_params.snapshot_interval_ms)
        .unwrap_or(DEFAULT_ANALYZE_SNAPSHOT_INTERVAL_MS)
        .clamp(
            MIN_ANALYZE_SNAPSHOT_INTERVAL_MS,
            MAX_ANALYZE_SNAPSHOT_INTERVAL_MS,
        );

    let output = match state
        .sql_handler
        .do_analyze_stream_query(&sql, query_ctx.clone())
        .await
    {
        Ok(output) => output,
        Err(err) => {
            return ErrorResponse::from_error(err)
                .with_execution_time(start.elapsed().as_millis() as u64)
                .into_response();
        }
    };

    let plan = output.meta.plan.clone();
    let OutputData::Stream(stream) = output.data else {
        return ErrorResponse::from_error_message(
            StatusCode::InvalidArguments,
            "analyze stream query must return a stream".to_string(),
        )
        .with_execution_time(start.elapsed().as_millis() as u64)
        .into_response();
    };
    let schema = stream.schema();

    let sse_stream = futures::stream::unfold(
        AnalyzeStreamState {
            stream,
            schema,
            plan,
            batches: Vec::new(),
            seq: 0,
            start,
            requested_interval_ms: interval_ms,
            current_interval_ms: interval_ms,
            done: false,
        },
        |mut state| async move {
            if state.done {
                return None;
            }
            let tick = tokio::time::sleep(Duration::from_millis(state.current_interval_ms));
            tokio::pin!(tick);
            loop {
                tokio::select! {
                    item = state.stream.next() => {
                        match item {
                            Some(Ok(batch)) => state.batches.push(batch),
                            Some(Err(err)) => {
                                let status = err.status_code();
                                let event_name = if status == StatusCode::Cancelled { "canceled" } else { "error" };
                                let (payload, _) = make_analyze_payload(AnalyzePayloadArgs {
                                    seq: state.seq,
                                    state: event_name,
                                    partial: false,
                                    start: state.start,
                                    plan: state.plan.as_ref(),
                                    output: None,
                                    reason: Some(err.output_msg()),
                                    code: Some(status as u32),
                                });
                                state.seq += 1;
                                state.done = true;
                                return Some((Ok::<Event, std::convert::Infallible>(Event::default().event(event_name).data(payload)), state));
                            }
                            None => {
                                let batches = std::mem::take(&mut state.batches);
                                let output = HttpRecordsOutput::try_new(state.schema.clone(), batches)
                                    .map(GreptimeQueryOutput::Records);
                                let (event_name, payload) = make_final_analyze_event(
                                    output.map_err(|err| (err.output_msg(), err.status_code() as u32)),
                                    state.seq,
                                    state.start,
                                    state.plan.as_ref(),
                                );
                                state.seq += 1;
                                state.done = true;
                                return Some((Ok::<Event, std::convert::Infallible>(Event::default().event(event_name).data(payload)), state));
                            }
                        }
                    }
                    _ = &mut tick => {
                        if state.plan.is_some() {
                            let (payload, payload_bytes) = make_analyze_payload(AnalyzePayloadArgs {
                                seq: state.seq,
                                state: "metrics",
                                partial: true,
                                start: state.start,
                                plan: state.plan.as_ref(),
                                output: None,
                                reason: None,
                                code: None,
                            });
                            state.current_interval_ms = adaptive_interval_ms(payload_bytes, state.requested_interval_ms);
                            state.seq += 1;
                            return Some((Ok::<Event, std::convert::Infallible>(Event::default().event("metrics").data(payload)), state));
                        }
                        tick.as_mut().reset(tokio::time::Instant::now() + Duration::from_millis(state.current_interval_ms));
                    }
                }
            }
        },
    );

    Sse::new(sse_stream)
        .keep_alive(KeepAlive::new().interval(Duration::from_secs(15)))
        .into_response()
}

fn adaptive_interval_ms(payload_bytes: usize, requested_ms: u64) -> u64 {
    if payload_bytes >= 10 * 1024 * 1024 {
        requested_ms.max(30_000)
    } else if payload_bytes >= 1024 * 1024 {
        requested_ms.max(10_000)
    } else {
        requested_ms
    }
}

fn make_final_analyze_event(
    output: std::result::Result<GreptimeQueryOutput, (String, u32)>,
    seq: u64,
    start: Instant,
    plan: Option<&Arc<dyn ExecutionPlan>>,
) -> (&'static str, String) {
    match output {
        Ok(output) => (
            "final",
            make_analyze_payload(AnalyzePayloadArgs {
                seq,
                state: "final",
                partial: false,
                start,
                plan,
                output: Some(output),
                reason: None,
                code: None,
            })
            .0,
        ),
        Err((reason, code)) => (
            "error",
            make_analyze_payload(AnalyzePayloadArgs {
                seq,
                state: "error",
                partial: false,
                start,
                plan,
                output: None,
                reason: Some(reason),
                code: Some(code),
            })
            .0,
        ),
    }
}

struct AnalyzePayloadArgs<'a> {
    seq: u64,
    state: &'static str,
    partial: bool,
    start: Instant,
    plan: Option<&'a Arc<dyn ExecutionPlan>>,
    output: Option<GreptimeQueryOutput>,
    reason: Option<String>,
    code: Option<u32>,
}

fn make_analyze_payload(args: AnalyzePayloadArgs<'_>) -> (String, usize) {
    let AnalyzePayloadArgs {
        seq,
        state,
        partial,
        start,
        plan,
        output,
        reason,
        code,
    } = args;
    let metrics = plan.and_then(|plan| query::analyze_plan_metrics_to_json_value(plan, true).ok());
    let payload = AnalyzeStreamPayload {
        seq,
        state,
        partial,
        elapsed_ms: start.elapsed().as_millis() as u64,
        metrics,
        output,
        reason,
        code,
    };
    let payload_string = serde_json::to_string(&payload).unwrap_or_else(|e| {
        serde_json::json!({
            "seq": seq,
            "state": "error",
            "partial": false,
            "reason": format!("Failed to serialize SSE payload: {e}"),
        })
        .to_string()
    });
    let payload_bytes = payload_string.len();
    (payload_string, payload_bytes)
}

/// Handler to parse sql
#[axum_macros::debug_handler]
#[tracing::instrument(skip_all, fields(protocol = "http", request_type = "sql"))]
pub async fn sql_parse(
    Query(query_params): Query<SqlQuery>,
    Form(form_params): Form<SqlQuery>,
) -> Result<Json<Vec<Statement>>> {
    let Some(sql) = query_params.sql.or(form_params.sql) else {
        return InvalidQuerySnafu {
            reason: "sql parameter is required.",
        }
        .fail();
    };

    let stmts =
        ParserContext::create_with_dialect(&sql, &GreptimeDbDialect {}, ParseOptions::default())
            .context(FailedToParseQuerySnafu)?;

    Ok(stmts.into())
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SqlFormatResponse {
    pub formatted: String,
}

/// Handler to format sql string
#[axum_macros::debug_handler]
#[tracing::instrument(skip_all, fields(protocol = "http", request_type = "sql_format"))]
pub async fn sql_format(
    Query(query_params): Query<SqlQuery>,
    Form(form_params): Form<SqlQuery>,
) -> axum::response::Response {
    let Some(sql) = query_params.sql.or(form_params.sql) else {
        let resp = ErrorResponse::from_error_message(
            StatusCode::InvalidArguments,
            "sql parameter is required.".to_string(),
        );
        return HttpResponse::Error(resp).into_response();
    };

    // Parse using GreptimeDB dialect then reconstruct statements via Display
    let stmts = match ParserContext::create_with_dialect(
        &sql,
        &GreptimeDbDialect {},
        ParseOptions::default(),
    ) {
        Ok(v) => v,
        Err(e) => return HttpResponse::Error(ErrorResponse::from_error(e)).into_response(),
    };

    let mut parts: Vec<String> = Vec::with_capacity(stmts.len());
    for stmt in stmts {
        let mut s = format!("{stmt}");
        if !s.trim_end().ends_with(';') {
            s.push(';');
        }
        parts.push(s);
    }

    let formatted = parts.join("\n");
    Json(SqlFormatResponse { formatted }).into_response()
}

/// Create a response from query result
pub async fn from_output(
    outputs: Vec<crate::error::Result<Output>>,
) -> std::result::Result<(Vec<GreptimeQueryOutput>, HashMap<String, Value>), ErrorResponse> {
    // TODO(sunng87): this api response structure cannot represent error well.
    //  It hides successful execution results from error response
    let mut results = Vec::with_capacity(outputs.len());
    let mut merge_map = HashMap::new();

    for out in outputs {
        match out {
            Ok(o) => match o.data {
                OutputData::AffectedRows(rows) => {
                    results.push(GreptimeQueryOutput::AffectedRows(rows));
                    if o.meta.cost > 0 {
                        merge_map.insert(GREPTIME_EXEC_WRITE_COST.to_string(), o.meta.cost as u64);
                    }
                }
                OutputData::Stream(stream) => {
                    let schema = stream.schema().clone();
                    // TODO(sunng87): streaming response
                    let mut http_record_output = match util::collect(stream).await {
                        Ok(rows) => match HttpRecordsOutput::try_new(schema, rows) {
                            Ok(rows) => rows,
                            Err(err) => {
                                return Err(ErrorResponse::from_error(err));
                            }
                        },
                        Err(err) => {
                            return Err(ErrorResponse::from_error(err));
                        }
                    };
                    if let Some(physical_plan) = o.meta.plan {
                        let mut result_map = HashMap::new();

                        let mut tmp = vec![&mut merge_map, &mut result_map];
                        collect_plan_metrics(&physical_plan, &mut tmp);
                        let re = result_map
                            .into_iter()
                            .map(|(k, v)| (k, Value::from(v)))
                            .collect::<HashMap<String, Value>>();
                        http_record_output.metrics.extend(re);
                    }
                    results.push(GreptimeQueryOutput::Records(http_record_output))
                }
                OutputData::RecordBatches(rbs) => {
                    match HttpRecordsOutput::try_new(rbs.schema(), rbs.take()) {
                        Ok(rows) => {
                            results.push(GreptimeQueryOutput::Records(rows));
                        }
                        Err(err) => {
                            return Err(ErrorResponse::from_error(err));
                        }
                    }
                }
            },

            Err(err) => {
                return Err(ErrorResponse::from_error(err));
            }
        }
    }

    let merge_map = merge_map
        .into_iter()
        .map(|(k, v)| (k, Value::from(v)))
        .collect();

    Ok((results, merge_map))
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct PromqlQuery {
    pub query: String,
    pub start: String,
    pub end: String,
    pub step: String,
    pub lookback: Option<String>,
    pub db: Option<String>,
    // (Optional) result format: [`greptimedb_v1`, `influxdb_v1`, `csv`,
    // `arrow`],
    // the default value is `greptimedb_v1`
    pub format: Option<String>,
    // For arrow output
    pub compression: Option<String>,
    // Returns epoch timestamps with the specified precision.
    // Both u and µ indicate microseconds.
    // epoch = [ns,u,µ,ms,s],
    //
    // For influx output only
    //
    // TODO(jeremy): currently, only InfluxDB result format is supported,
    // and all columns of the `Timestamp` type will be converted to their
    // specified time precision. Maybe greptimedb format can support this
    // param too.
    pub epoch: Option<String>,
}

impl From<PromqlQuery> for PromQuery {
    fn from(query: PromqlQuery) -> Self {
        PromQuery {
            query: query.query,
            start: query.start,
            end: query.end,
            step: query.step,
            lookback: query
                .lookback
                .unwrap_or_else(|| DEFAULT_LOOKBACK_STRING.to_string()),
            // TODO(dennis): support alias from http params or parse from query.query
            alias: None,
        }
    }
}

/// Handler to execute promql
#[axum_macros::debug_handler]
#[tracing::instrument(skip_all, fields(protocol = "http", request_type = "promql"))]
pub async fn promql(
    State(state): State<ApiState>,
    Query(params): Query<PromqlQuery>,
    Extension(mut query_ctx): Extension<QueryContext>,
) -> Response {
    let sql_handler = &state.sql_handler;
    let exec_start = Instant::now();
    let db = query_ctx.get_db_string();

    query_ctx.set_channel(Channel::Promql);
    let query_ctx = Arc::new(query_ctx);

    let _timer = crate::metrics::METRIC_HTTP_PROMQL_ELAPSED
        .with_label_values(&[db.as_str()])
        .start_timer();

    let resp = if let Some((status, msg)) =
        validate_schema(sql_handler.clone(), query_ctx.clone()).await
    {
        let resp = ErrorResponse::from_error_message(status, msg);
        HttpResponse::Error(resp)
    } else {
        let format = params
            .format
            .as_ref()
            .map(|s| s.to_lowercase())
            .map(|s| ResponseFormat::parse(s.as_str()).unwrap_or(ResponseFormat::GreptimedbV1))
            .unwrap_or(ResponseFormat::GreptimedbV1);
        let epoch = params
            .epoch
            .as_ref()
            .map(|s| s.to_lowercase())
            .map(|s| Epoch::parse(s.as_str()).unwrap_or(Epoch::Millisecond));
        let compression = params.compression.clone();

        let prom_query = params.into();
        let outputs = sql_handler.do_promql_query(&prom_query, query_ctx).await;

        match format {
            ResponseFormat::Arrow => ArrowResponse::from_output(outputs, compression).await,
            ResponseFormat::Csv(with_names, with_types) => {
                CsvResponse::from_output(outputs, with_names, with_types).await
            }
            ResponseFormat::Table => TableResponse::from_output(outputs).await,
            ResponseFormat::GreptimedbV1 => GreptimedbV1Response::from_output(outputs).await,
            ResponseFormat::InfluxdbV1 => InfluxdbV1Response::from_output(outputs, epoch).await,
            ResponseFormat::Json => JsonResponse::from_output(outputs).await,
            ResponseFormat::Null => NullResponse::from_output(outputs).await,
        }
    };

    resp.with_execution_time(exec_start.elapsed().as_millis() as u64)
        .into_response()
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
    if let Some(c) = crate::metrics::jemalloc::JEMALLOC_COLLECTOR.as_ref()
        && let Err(e) = c.update()
    {
        common_telemetry::error!(e; "Failed to update jemalloc metrics");
    }
    state.render()
}

#[derive(Debug, Serialize, Deserialize)]
pub struct HealthQuery {}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct HealthResponse {}

/// Handler to export healthy check
///
/// Currently simply return status "200 OK" (default) with an empty json payload "{}"
#[axum_macros::debug_handler]
pub async fn health(Query(_params): Query<HealthQuery>) -> Json<HealthResponse> {
    Json(HealthResponse {})
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct StatusResponse<'a> {
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
    let build_info = common_version::build_info();
    Json(StatusResponse {
        commit: build_info.commit,
        branch: build_info.branch,
        rustc_version: build_info.rustc,
        hostname,
        version: build_info.version,
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
        .is_valid_schema(query_ctx.current_catalog(), &query_ctx.current_schema())
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

pub async fn index() -> axum::response::Html<String> {
    let name = common_version::product_name();
    let version = common_version::version();
    axum::response::Html(format!(
        r#"<!DOCTYPE html>
<html>
<head><title>{name}</title></head>
<body>
<h1>{name}</h1>
<p>Version: {version}</p>
<ul>
<li><a href="/dashboard">Dashboard UI</a></li>
<li><a href="/v1/health">Health</a> (JSON)</li>
<li><a href="/status">Status</a> (JSON)</li>
<li><a href="/metrics">Metrics</a> (For Prometheus Scrape)</li>
<li><a href="/config">Config</a> (TXT)</li>
</ul>
</body>
</html>"#,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_final_analyze_event_uses_error_event_for_conversion_error() {
        let (event_name, payload) = make_final_analyze_event(
            Err((
                "conversion failed".to_string(),
                StatusCode::InvalidArguments as u32,
            )),
            7,
            Instant::now(),
            None,
        );

        assert_eq!(event_name, "error");
        let value: Value = serde_json::from_str(&payload).unwrap();
        assert_eq!(value["state"], "error");
        assert_eq!(value["reason"], "conversion failed");
    }
}
