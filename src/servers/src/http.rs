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

pub mod authorize;
pub mod handler;
pub mod header;
pub mod influxdb;
pub mod mem_prof;
pub mod opentsdb;
pub mod otlp;
pub mod pprof;
pub mod prom_store;
pub mod prometheus;
pub mod script;

#[cfg(feature = "dashboard")]
mod dashboard;
pub mod influxdb_result_v1;

use std::fmt::Display;
use std::net::SocketAddr;
use std::time::{Duration, Instant};

use aide::axum::{routing as apirouting, ApiRouter, IntoApiResponse};
use aide::openapi::{Info, OpenApi, Server as OpenAPIServer};
use async_trait::async_trait;
use auth::UserProviderRef;
use axum::error_handling::HandleErrorLayer;
use axum::extract::{DefaultBodyLimit, MatchedPath};
use axum::http::Request;
use axum::middleware::{self, Next};
use axum::response::{Html, IntoResponse, Json};
use axum::{routing, BoxError, Extension, Router};
use common_base::readable_size::ReadableSize;
use common_base::Plugins;
use common_error::ext::ErrorExt;
use common_error::status_code::StatusCode;
use common_query::Output;
use common_recordbatch::{util, RecordBatch};
use common_telemetry::logging::{debug, error, info};
use common_time::timestamp::TimeUnit;
use common_time::Timestamp;
use datatypes::data_type::DataType;
use futures::FutureExt;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use snafu::{ensure, ResultExt};
use tokio::sync::oneshot::{self, Sender};
use tokio::sync::Mutex;
use tower::timeout::TimeoutLayer;
use tower::ServiceBuilder;
use tower_http::trace::TraceLayer;

use self::authorize::AuthState;
use crate::configurator::ConfiguratorRef;
use crate::error::{AlreadyStartedSnafu, Error, Result, StartHttpSnafu, ToJsonSnafu};
use crate::http::influxdb::{influxdb_health, influxdb_ping, influxdb_write_v1, influxdb_write_v2};
use crate::http::influxdb_result_v1::InfluxdbV1Response;
use crate::http::prometheus::{
    format_query, instant_query, label_values_query, labels_query, range_query, series_query,
};
use crate::metrics::{
    HTTP_TRACK_METRICS, METRIC_HTTP_REQUESTS_ELAPSED, METRIC_HTTP_REQUESTS_TOTAL,
};
use crate::metrics_handler::MetricsHandler;
use crate::prometheus_handler::PrometheusHandlerRef;
use crate::query_handler::grpc::ServerGrpcQueryHandlerRef;
use crate::query_handler::sql::ServerSqlQueryHandlerRef;
use crate::query_handler::{
    InfluxdbLineProtocolHandlerRef, OpenTelemetryProtocolHandlerRef, OpentsdbProtocolHandlerRef,
    PromStoreProtocolHandlerRef, ScriptHandlerRef,
};
use crate::server::Server;

pub const HTTP_API_VERSION: &str = "v1";
pub const HTTP_API_PREFIX: &str = "/v1/";
/// Default http body limit (64M).
const DEFAULT_BODY_LIMIT: ReadableSize = ReadableSize::mb(64);

// TODO(fys): This is a temporary workaround, it will be improved later
pub static PUBLIC_APIS: [&str; 2] = ["/v1/influxdb/ping", "/v1/influxdb/health"];

#[derive(Default)]
pub struct HttpServer {
    sql_handler: Option<ServerSqlQueryHandlerRef>,
    grpc_handler: Option<ServerGrpcQueryHandlerRef>,
    options: HttpOptions,
    influxdb_handler: Option<InfluxdbLineProtocolHandlerRef>,
    opentsdb_handler: Option<OpentsdbProtocolHandlerRef>,
    prom_handler: Option<PromStoreProtocolHandlerRef>,
    prometheus_handler: Option<PrometheusHandlerRef>,
    otlp_handler: Option<OpenTelemetryProtocolHandlerRef>,
    script_handler: Option<ScriptHandlerRef>,
    shutdown_tx: Mutex<Option<Sender<()>>>,
    user_provider: Option<UserProviderRef>,
    metrics_handler: Option<MetricsHandler>,
    greptime_config_options: Option<String>,
    plugins: Plugins,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct HttpOptions {
    pub addr: String,

    #[serde(with = "humantime_serde")]
    pub timeout: Duration,

    #[serde(skip)]
    pub disable_dashboard: bool,

    pub body_limit: ReadableSize,
}

impl Default for HttpOptions {
    fn default() -> Self {
        Self {
            addr: "127.0.0.1:4000".to_string(),
            timeout: Duration::from_secs(30),
            disable_dashboard: false,
            body_limit: DEFAULT_BODY_LIMIT,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, Eq, PartialEq)]
pub struct ColumnSchema {
    name: String,
    data_type: String,
}

impl ColumnSchema {
    pub fn new(name: String, data_type: String) -> ColumnSchema {
        ColumnSchema { name, data_type }
    }
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, Eq, PartialEq)]
pub struct Schema {
    column_schemas: Vec<ColumnSchema>,
}

impl Schema {
    pub fn new(columns: Vec<ColumnSchema>) -> Schema {
        Schema {
            column_schemas: columns,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, Eq, PartialEq)]
pub struct HttpRecordsOutput {
    schema: Option<Schema>,
    rows: Vec<Vec<Value>>,
}

impl HttpRecordsOutput {
    pub fn num_rows(&self) -> usize {
        self.rows.len()
    }

    pub fn num_cols(&self) -> usize {
        self.schema
            .as_ref()
            .map(|x| x.column_schemas.len())
            .unwrap_or(0)
    }

    pub fn schema(&self) -> Option<&Schema> {
        self.schema.as_ref()
    }

    pub fn rows(&self) -> &Vec<Vec<Value>> {
        &self.rows
    }
}

impl TryFrom<Vec<RecordBatch>> for HttpRecordsOutput {
    type Error = Error;

    // TODO(sunng87): use schema from recordstreams when #366 fixed
    fn try_from(
        recordbatches: Vec<RecordBatch>,
    ) -> std::result::Result<HttpRecordsOutput, Self::Error> {
        if recordbatches.is_empty() {
            Ok(HttpRecordsOutput {
                schema: None,
                rows: vec![],
            })
        } else {
            // safety ensured by previous empty check
            let first = &recordbatches[0];
            let schema = Schema {
                column_schemas: first
                    .schema
                    .column_schemas()
                    .iter()
                    .map(|cs| ColumnSchema {
                        name: cs.name.clone(),
                        data_type: cs.data_type.name(),
                    })
                    .collect(),
            };

            let mut rows =
                Vec::with_capacity(recordbatches.iter().map(|r| r.num_rows()).sum::<usize>());

            for recordbatch in recordbatches {
                for row in recordbatch.rows() {
                    let value_row = row
                        .into_iter()
                        .map(Value::try_from)
                        .collect::<std::result::Result<Vec<Value>, _>>()
                        .context(ToJsonSnafu)?;

                    rows.push(value_row);
                }
            }

            Ok(HttpRecordsOutput {
                schema: Some(schema),
                rows,
            })
        }
    }
}

#[derive(Serialize, Deserialize, Debug, JsonSchema, Eq, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum JsonOutput {
    AffectedRows(usize),
    Records(HttpRecordsOutput),
}

#[derive(Serialize, Deserialize, Debug, JsonSchema)]
pub struct GreptimedbV1Response {
    code: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    output: Vec<JsonOutput>,
    #[serde(skip_serializing_if = "Option::is_none")]
    execution_time_ms: Option<u64>,
}

impl GreptimedbV1Response {
    pub fn with_error(error: impl ErrorExt) -> Self {
        let code = error.status_code();
        if code.should_log_error() {
            error!(error; "Failed to handle HTTP request");
        } else {
            debug!("Failed to handle HTTP request, err: {:?}", error);
        }

        GreptimedbV1Response {
            error: Some(error.output_msg()),
            code: code as u32,
            output: vec![],
            execution_time_ms: None,
        }
    }

    fn with_error_message(err_msg: String, error_code: StatusCode) -> Self {
        GreptimedbV1Response {
            error: Some(err_msg),
            code: error_code as u32,
            output: vec![],
            execution_time_ms: None,
        }
    }

    fn with_output(output: Vec<JsonOutput>) -> Self {
        GreptimedbV1Response {
            error: None,
            code: StatusCode::Success as u32,
            output,
            execution_time_ms: None,
        }
    }

    fn with_execution_time(&mut self, execution_time: u64) {
        self.execution_time_ms = Some(execution_time);
    }

    /// Create a json response from query result
    pub async fn from_output(outputs: Vec<Result<Output>>) -> Self {
        // TODO(sunng87): this api response structure cannot represent error
        // well. It hides successful execution results from error response
        let mut results = Vec::with_capacity(outputs.len());
        for out in outputs {
            match out {
                Ok(Output::AffectedRows(rows)) => {
                    results.push(JsonOutput::AffectedRows(rows));
                }
                Ok(Output::Stream(stream)) => {
                    // TODO(sunng87): streaming response
                    match util::collect(stream).await {
                        Ok(rows) => match HttpRecordsOutput::try_from(rows) {
                            Ok(rows) => {
                                results.push(JsonOutput::Records(rows));
                            }
                            Err(err) => {
                                return Self::with_error(err);
                            }
                        },

                        Err(e) => {
                            return Self::with_error(e);
                        }
                    }
                }
                Ok(Output::RecordBatches(rbs)) => match HttpRecordsOutput::try_from(rbs.take()) {
                    Ok(rows) => {
                        results.push(JsonOutput::Records(rows));
                    }
                    Err(err) => {
                        return Self::with_error(err);
                    }
                },
                Err(e) => {
                    return Self::with_error(e);
                }
            }
        }
        Self::with_output(results)
    }

    pub fn code(&self) -> u32 {
        self.code
    }

    pub fn success(&self) -> bool {
        self.code == (StatusCode::Success as u32)
    }

    pub fn error(&self) -> Option<&String> {
        self.error.as_ref()
    }

    pub fn output(&self) -> &[JsonOutput] {
        &self.output
    }

    pub fn execution_time_ms(&self) -> Option<u64> {
        self.execution_time_ms
    }
}

/// It allows the results of SQL queries to be presented in different formats.
/// Currently, `greptimedb_v1` and `influxdb_v1` are supported.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResponseFormat {
    GreptimedbV1,
    InfluxdbV1,
}

impl ResponseFormat {
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "greptimedb_v1" => Some(ResponseFormat::GreptimedbV1),
            "influxdb_v1" => Some(ResponseFormat::InfluxdbV1),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Epoch {
    Nanosecond,
    Microsecond,
    Millisecond,
    Second,
}

impl Epoch {
    pub fn parse(s: &str) -> Option<Epoch> {
        // Both u and µ indicate microseconds.
        // epoch = [ns,u,µ,ms,s],
        // For details, see the Influxdb documents.
        // https://docs.influxdata.com/influxdb/v1/tools/api/#query-string-parameters-1
        match s {
            "ns" => Some(Epoch::Nanosecond),
            "u" | "µ" => Some(Epoch::Microsecond),
            "ms" => Some(Epoch::Millisecond),
            "s" => Some(Epoch::Second),
            _ => None, // just returns None for other cases
        }
    }

    pub fn convert_timestamp(&self, ts: Timestamp) -> Option<Timestamp> {
        match self {
            Epoch::Nanosecond => ts.convert_to(TimeUnit::Nanosecond),
            Epoch::Microsecond => ts.convert_to(TimeUnit::Microsecond),
            Epoch::Millisecond => ts.convert_to(TimeUnit::Millisecond),
            Epoch::Second => ts.convert_to(TimeUnit::Second),
        }
    }
}

impl Display for Epoch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Epoch::Nanosecond => write!(f, "Epoch::Nanosecond"),
            Epoch::Microsecond => write!(f, "Epoch::Microsecond"),
            Epoch::Millisecond => write!(f, "Epoch::Millisecond"),
            Epoch::Second => write!(f, "Epoch::Second"),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, JsonSchema)]
#[serde(tag = "type")]
pub enum JsonResponse {
    GreptimedbV1(GreptimedbV1Response),
    InfluxdbV1(InfluxdbV1Response),
}

impl From<GreptimedbV1Response> for JsonResponse {
    fn from(value: GreptimedbV1Response) -> Self {
        JsonResponse::GreptimedbV1(value)
    }
}

impl From<InfluxdbV1Response> for JsonResponse {
    fn from(value: InfluxdbV1Response) -> Self {
        JsonResponse::InfluxdbV1(value)
    }
}

impl JsonResponse {
    pub fn with_error(error: impl ErrorExt, response_format: ResponseFormat) -> Self {
        match response_format {
            ResponseFormat::GreptimedbV1 => GreptimedbV1Response::with_error(error).into(),
            ResponseFormat::InfluxdbV1 => InfluxdbV1Response::with_error(error).into(),
        }
    }

    pub fn with_error_message(
        err_msg: String,
        error_code: StatusCode,
        response_format: ResponseFormat,
    ) -> Self {
        match response_format {
            ResponseFormat::GreptimedbV1 => {
                GreptimedbV1Response::with_error_message(err_msg, error_code).into()
            }
            ResponseFormat::InfluxdbV1 => InfluxdbV1Response::with_error_message(err_msg).into(),
        }
    }
    pub async fn from_output(
        outputs: Vec<Result<Output>>,
        response_format: ResponseFormat,
        epoch: Option<Epoch>,
    ) -> Self {
        match response_format {
            ResponseFormat::GreptimedbV1 => GreptimedbV1Response::from_output(outputs).await.into(),
            ResponseFormat::InfluxdbV1 => {
                InfluxdbV1Response::from_output(outputs, epoch).await.into()
            }
        }
    }

    fn with_execution_time(mut self, execution_time: u128) -> Self {
        match &mut self {
            JsonResponse::GreptimedbV1(resp) => {
                resp.with_execution_time(execution_time as u64);
            }
            JsonResponse::InfluxdbV1(resp) => {
                resp.with_execution_time(execution_time as u64);
            }
        }
        self
    }
}

async fn serve_api(Extension(api): Extension<OpenApi>) -> impl IntoApiResponse {
    Json(api)
}

async fn serve_docs() -> Html<String> {
    Html(include_str!("http/redoc.html").to_owned())
}

#[derive(Clone)]
pub struct ApiState {
    pub sql_handler: ServerSqlQueryHandlerRef,
    pub script_handler: Option<ScriptHandlerRef>,
}

#[derive(Clone)]
pub struct GreptimeOptionsConfigState {
    pub greptime_config_options: String,
}

#[derive(Default)]
pub struct HttpServerBuilder {
    inner: HttpServer,
}

impl HttpServerBuilder {
    pub fn new(options: HttpOptions) -> Self {
        Self {
            inner: HttpServer {
                sql_handler: None,
                grpc_handler: None,
                options,
                opentsdb_handler: None,
                influxdb_handler: None,
                prom_handler: None,
                prometheus_handler: None,
                otlp_handler: None,
                user_provider: None,
                script_handler: None,
                metrics_handler: None,
                shutdown_tx: Mutex::new(None),
                greptime_config_options: None,
                plugins: Default::default(),
            },
        }
    }

    pub fn with_sql_handler(&mut self, handler: ServerSqlQueryHandlerRef) -> &mut Self {
        let _ = self.inner.sql_handler.get_or_insert(handler);
        self
    }

    pub fn with_grpc_handler(&mut self, handler: ServerGrpcQueryHandlerRef) -> &mut Self {
        let _ = self.inner.grpc_handler.get_or_insert(handler);
        self
    }

    pub fn with_opentsdb_handler(&mut self, handler: OpentsdbProtocolHandlerRef) -> &mut Self {
        let _ = self.inner.opentsdb_handler.get_or_insert(handler);
        self
    }

    pub fn with_script_handler(&mut self, handler: ScriptHandlerRef) -> &mut Self {
        let _ = self.inner.script_handler.get_or_insert(handler);
        self
    }

    pub fn with_influxdb_handler(&mut self, handler: InfluxdbLineProtocolHandlerRef) -> &mut Self {
        let _ = self.inner.influxdb_handler.get_or_insert(handler);
        self
    }

    pub fn with_prom_handler(&mut self, handler: PromStoreProtocolHandlerRef) -> &mut Self {
        let _ = self.inner.prom_handler.get_or_insert(handler);
        self
    }

    pub fn with_prometheus_handler(&mut self, handler: PrometheusHandlerRef) -> &mut Self {
        let _ = self.inner.prometheus_handler.get_or_insert(handler);
        self
    }

    pub fn with_otlp_handler(&mut self, handler: OpenTelemetryProtocolHandlerRef) -> &mut Self {
        let _ = self.inner.otlp_handler.get_or_insert(handler);
        self
    }

    pub fn with_user_provider(&mut self, user_provider: UserProviderRef) -> &mut Self {
        let _ = self.inner.user_provider.get_or_insert(user_provider);
        self
    }

    pub fn with_metrics_handler(&mut self, handler: MetricsHandler) -> &mut Self {
        let _ = self.inner.metrics_handler.get_or_insert(handler);
        self
    }

    pub fn with_plugins(&mut self, plugins: Plugins) -> &mut Self {
        self.inner.plugins = plugins;
        self
    }

    pub fn with_greptime_config_options(&mut self, opts: String) -> &mut Self {
        self.inner.greptime_config_options = Some(opts);
        self
    }

    pub fn build(&mut self) -> HttpServer {
        std::mem::take(self).inner
    }
}

impl HttpServer {
    pub fn make_app(&self) -> Router {
        let mut api = OpenApi {
            info: Info {
                title: "GreptimeDB HTTP API".to_string(),
                description: Some("HTTP APIs to interact with GreptimeDB".to_string()),
                version: HTTP_API_VERSION.to_string(),
                ..Info::default()
            },
            servers: vec![OpenAPIServer {
                url: format!("/{HTTP_API_VERSION}"),
                ..OpenAPIServer::default()
            }],
            ..OpenApi::default()
        };

        let mut router = Router::new();

        if let Some(sql_handler) = self.sql_handler.clone() {
            let sql_router = self
                .route_sql(ApiState {
                    sql_handler,
                    script_handler: self.script_handler.clone(),
                })
                .finish_api(&mut api)
                .layer(Extension(api.clone()));
            router = router.nest(&format!("/{HTTP_API_VERSION}"), sql_router);
        }

        if let Some(opentsdb_handler) = self.opentsdb_handler.clone() {
            router = router.nest(
                &format!("/{HTTP_API_VERSION}/opentsdb"),
                self.route_opentsdb(opentsdb_handler),
            );
        }

        if let Some(influxdb_handler) = self.influxdb_handler.clone() {
            router = router.nest(
                &format!("/{HTTP_API_VERSION}/influxdb"),
                self.route_influxdb(influxdb_handler),
            );
        }

        if let Some(prom_handler) = self.prom_handler.clone() {
            router = router.nest(
                &format!("/{HTTP_API_VERSION}/prometheus"),
                self.route_prom(prom_handler),
            );
        }

        if let Some(prometheus_handler) = self.prometheus_handler.clone() {
            router = router.nest(
                &format!("/{HTTP_API_VERSION}/prometheus/api/v1"),
                self.route_prometheus(prometheus_handler),
            );
        }

        if let Some(otlp_handler) = self.otlp_handler.clone() {
            router = router.nest(
                &format!("/{HTTP_API_VERSION}/otlp"),
                self.route_otlp(otlp_handler),
            );
        }

        if let Some(metrics_handler) = self.metrics_handler {
            router = router.nest("", self.route_metrics(metrics_handler));
        }

        router = router.route(
            "/health",
            routing::get(handler::health).post(handler::health),
        );

        let config_router = self
            .route_config(GreptimeOptionsConfigState {
                greptime_config_options: self
                    .greptime_config_options
                    .clone()
                    .unwrap_or("".to_string()),
            })
            .finish_api(&mut api);

        router = router.nest("", config_router);

        router = router.route("/status", routing::get(handler::status));

        #[cfg(feature = "dashboard")]
        {
            if !self.options.disable_dashboard {
                info!("Enable dashboard service at '/dashboard'");
                router = router.nest("/dashboard", dashboard::dashboard());

                // "/dashboard" and "/dashboard/" are two different paths in Axum.
                // We cannot nest "/dashboard/", because we already mapping "/dashboard/*x" while nesting "/dashboard".
                // So we explicitly route "/dashboard/" here.
                router = router.route(
                    "/dashboard/",
                    routing::get(dashboard::static_handler).post(dashboard::static_handler),
                );
            }
        }

        // Add a layer to collect HTTP metrics for axum.
        router = router.route_layer(middleware::from_fn(track_metrics));

        router
    }

    pub fn build(&self, router: Router) -> Router {
        router
            // middlewares
            .layer(
                ServiceBuilder::new()
                    .layer(HandleErrorLayer::new(handle_error))
                    .layer(TraceLayer::new_for_http())
                    .layer(TimeoutLayer::new(self.options.timeout))
                    .layer(DefaultBodyLimit::max(
                        self.options
                            .body_limit
                            .0
                            .try_into()
                            .unwrap_or_else(|_| DEFAULT_BODY_LIMIT.as_bytes() as usize),
                    ))
                    // auth layer
                    .layer(middleware::from_fn_with_state(
                        AuthState::new(self.user_provider.clone()),
                        authorize::check_http_auth,
                    )),
            )
            // Handlers for debug, we don't expect a timeout.
            .nest(
                &format!("/{HTTP_API_VERSION}/prof"),
                Router::new()
                    .route(
                        "/cpu",
                        routing::get(pprof::pprof_handler).post(pprof::pprof_handler),
                    )
                    .route(
                        "/mem",
                        routing::get(mem_prof::mem_prof_handler).post(mem_prof::mem_prof_handler),
                    ),
            )
    }

    fn route_metrics<S>(&self, metrics_handler: MetricsHandler) -> Router<S> {
        Router::new()
            .route("/metrics", routing::get(handler::metrics))
            .with_state(metrics_handler)
    }

    fn route_sql<S>(&self, api_state: ApiState) -> ApiRouter<S> {
        ApiRouter::new()
            .api_route(
                "/sql",
                apirouting::get_with(handler::sql, handler::sql_docs)
                    .post_with(handler::sql, handler::sql_docs),
            )
            .api_route(
                "/promql",
                apirouting::get_with(handler::promql, handler::sql_docs)
                    .post_with(handler::promql, handler::sql_docs),
            )
            .api_route("/scripts", apirouting::post(script::scripts))
            .api_route("/run-script", apirouting::post(script::run_script))
            .route("/private/api.json", apirouting::get(serve_api))
            .route("/private/docs", apirouting::get(serve_docs))
            .with_state(api_state)
    }

    fn route_prometheus<S>(&self, prometheus_handler: PrometheusHandlerRef) -> Router<S> {
        Router::new()
            .route(
                "/format_query",
                routing::post(format_query).get(format_query),
            )
            .route("/query", routing::post(instant_query).get(instant_query))
            .route("/query_range", routing::post(range_query).get(range_query))
            .route("/labels", routing::post(labels_query).get(labels_query))
            .route("/series", routing::post(series_query).get(series_query))
            .route(
                "/label/:label_name/values",
                routing::get(label_values_query),
            )
            .with_state(prometheus_handler)
    }

    fn route_prom<S>(&self, prom_handler: PromStoreProtocolHandlerRef) -> Router<S> {
        Router::new()
            .route("/write", routing::post(prom_store::remote_write))
            .route("/read", routing::post(prom_store::remote_read))
            .with_state(prom_handler)
    }

    fn route_influxdb<S>(&self, influxdb_handler: InfluxdbLineProtocolHandlerRef) -> Router<S> {
        Router::new()
            .route("/write", routing::post(influxdb_write_v1))
            .route("/api/v2/write", routing::post(influxdb_write_v2))
            .route("/ping", routing::get(influxdb_ping))
            .route("/health", routing::get(influxdb_health))
            .with_state(influxdb_handler)
    }

    fn route_opentsdb<S>(&self, opentsdb_handler: OpentsdbProtocolHandlerRef) -> Router<S> {
        Router::new()
            .route("/api/put", routing::post(opentsdb::put))
            .with_state(opentsdb_handler)
    }

    fn route_otlp<S>(&self, otlp_handler: OpenTelemetryProtocolHandlerRef) -> Router<S> {
        Router::new()
            .route("/v1/metrics", routing::post(otlp::metrics))
            .route("/v1/traces", routing::post(otlp::traces))
            .with_state(otlp_handler)
    }

    fn route_config<S>(&self, state: GreptimeOptionsConfigState) -> ApiRouter<S> {
        ApiRouter::new()
            .route("/config", apirouting::get(handler::config))
            .with_state(state)
    }
}

/// A middleware to record metrics for HTTP.
// Based on https://github.com/tokio-rs/axum/blob/axum-v0.6.16/examples/prometheus-metrics/src/main.rs
pub(crate) async fn track_metrics<B>(req: Request<B>, next: Next<B>) -> impl IntoResponse {
    let _timer = HTTP_TRACK_METRICS
        .with_label_values(&["value"])
        .start_timer();
    let start = Instant::now();
    let path = if let Some(matched_path) = req.extensions().get::<MatchedPath>() {
        matched_path.as_str().to_owned()
    } else {
        req.uri().path().to_owned()
    };
    let method = req.method().clone();

    let response = next.run(req).await;

    let latency = start.elapsed().as_secs_f64();
    let status = response.status().as_u16().to_string();
    let method_str = method.to_string();

    let labels = [method_str.as_str(), path.as_str(), status.as_str()];
    METRIC_HTTP_REQUESTS_TOTAL.with_label_values(&labels).inc();
    METRIC_HTTP_REQUESTS_ELAPSED
        .with_label_values(&labels)
        .observe(latency);

    response
}

pub const HTTP_SERVER: &str = "HTTP_SERVER";

#[async_trait]
impl Server for HttpServer {
    async fn shutdown(&self) -> Result<()> {
        let mut shutdown_tx = self.shutdown_tx.lock().await;
        if let Some(tx) = shutdown_tx.take() {
            if tx.send(()).is_err() {
                info!("Receiver dropped, the HTTP server has already existed");
            }
        }
        info!("Shutdown HTTP server");

        Ok(())
    }

    async fn start(&self, listening: SocketAddr) -> Result<SocketAddr> {
        let (tx, rx) = oneshot::channel();
        let server = {
            let mut shutdown_tx = self.shutdown_tx.lock().await;
            ensure!(
                shutdown_tx.is_none(),
                AlreadyStartedSnafu { server: "HTTP" }
            );

            let mut app = self.make_app();
            if let Some(configurator) = self.plugins.get::<ConfiguratorRef>() {
                app = configurator.config_http(app);
            }
            let app = self.build(app);
            let server = axum::Server::bind(&listening)
                .tcp_nodelay(true)
                .serve(app.into_make_service());

            *shutdown_tx = Some(tx);

            server
        };
        let listening = server.local_addr();
        info!("HTTP server is bound to {}", listening);

        let graceful = server.with_graceful_shutdown(rx.map(drop));
        graceful.await.context(StartHttpSnafu)?;

        Ok(listening)
    }

    fn name(&self) -> &str {
        HTTP_SERVER
    }
}

/// handle error middleware
async fn handle_error(err: BoxError) -> Json<JsonResponse> {
    error!(err; "Unhandled internal error");

    Json(JsonResponse::with_error_message(
        format!("Unhandled internal error: {err}"),
        StatusCode::Unexpected,
        ResponseFormat::GreptimedbV1,
    ))
}

#[cfg(test)]
mod test {
    use std::future::pending;
    use std::sync::Arc;

    use api::v1::greptime_request::Request;
    use axum::handler::Handler;
    use axum::http::StatusCode;
    use axum::routing::get;
    use axum_test_helper::TestClient;
    use common_recordbatch::RecordBatches;
    use datatypes::prelude::*;
    use datatypes::schema::{ColumnSchema, Schema};
    use datatypes::vectors::{StringVector, UInt32Vector};
    use query::parser::PromQuery;
    use query::plan::LogicalPlan;
    use query::query_engine::DescribeResult;
    use session::context::QueryContextRef;
    use tokio::sync::mpsc;

    use super::*;
    use crate::error::Error;
    use crate::query_handler::grpc::{GrpcQueryHandler, ServerGrpcQueryHandlerAdapter};
    use crate::query_handler::sql::{ServerSqlQueryHandlerAdapter, SqlQueryHandler};

    struct DummyInstance {
        _tx: mpsc::Sender<(String, Vec<u8>)>,
    }

    #[async_trait]
    impl GrpcQueryHandler for DummyInstance {
        type Error = Error;

        async fn do_query(
            &self,
            _query: Request,
            _ctx: QueryContextRef,
        ) -> std::result::Result<Output, Self::Error> {
            unimplemented!()
        }
    }

    #[async_trait]
    impl SqlQueryHandler for DummyInstance {
        type Error = Error;

        async fn do_query(&self, _: &str, _: QueryContextRef) -> Vec<Result<Output>> {
            unimplemented!()
        }

        async fn do_promql_query(
            &self,
            _: &PromQuery,
            _: QueryContextRef,
        ) -> Vec<std::result::Result<Output, Self::Error>> {
            unimplemented!()
        }

        async fn do_exec_plan(
            &self,
            _plan: LogicalPlan,
            _query_ctx: QueryContextRef,
        ) -> std::result::Result<Output, Self::Error> {
            unimplemented!()
        }

        async fn do_describe(
            &self,
            _stmt: sql::statements::statement::Statement,
            _query_ctx: QueryContextRef,
        ) -> Result<Option<DescribeResult>> {
            unimplemented!()
        }

        async fn is_valid_schema(&self, _catalog: &str, _schema: &str) -> Result<bool> {
            Ok(true)
        }
    }

    fn timeout() -> TimeoutLayer {
        TimeoutLayer::new(Duration::from_millis(10))
    }

    async fn forever() {
        pending().await
    }

    fn make_test_app(tx: mpsc::Sender<(String, Vec<u8>)>) -> Router {
        let instance = Arc::new(DummyInstance { _tx: tx });
        let sql_instance = ServerSqlQueryHandlerAdapter::arc(instance.clone());
        let grpc_instance = ServerGrpcQueryHandlerAdapter::arc(instance);
        let server = HttpServerBuilder::new(HttpOptions::default())
            .with_sql_handler(sql_instance)
            .with_grpc_handler(grpc_instance)
            .build();
        server.build(server.make_app()).route(
            "/test/timeout",
            get(forever.layer(
                ServiceBuilder::new()
                    .layer(HandleErrorLayer::new(|_: BoxError| async {
                        StatusCode::REQUEST_TIMEOUT
                    }))
                    .layer(timeout()),
            )),
        )
    }

    #[test]
    fn test_http_options_default() {
        let default = HttpOptions::default();
        assert_eq!("127.0.0.1:4000".to_string(), default.addr);
        assert_eq!(Duration::from_secs(30), default.timeout)
    }

    #[tokio::test]
    async fn test_http_server_request_timeout() {
        let (tx, _rx) = mpsc::channel(100);
        let app = make_test_app(tx);
        let client = TestClient::new(app);
        let res = client.get("/test/timeout").send().await;
        assert_eq!(res.status(), StatusCode::REQUEST_TIMEOUT);
    }

    #[tokio::test]
    async fn test_recordbatches_conversion() {
        let column_schemas = vec![
            ColumnSchema::new("numbers", ConcreteDataType::uint32_datatype(), false),
            ColumnSchema::new("strings", ConcreteDataType::string_datatype(), true),
        ];
        let schema = Arc::new(Schema::new(column_schemas));
        let columns: Vec<VectorRef> = vec![
            Arc::new(UInt32Vector::from_slice(vec![1, 2, 3, 4])),
            Arc::new(StringVector::from(vec![
                None,
                Some("hello"),
                Some("greptime"),
                None,
            ])),
        ];
        let recordbatch = RecordBatch::new(schema.clone(), columns).unwrap();

        for format in [ResponseFormat::GreptimedbV1, ResponseFormat::InfluxdbV1] {
            let recordbatches =
                RecordBatches::try_new(schema.clone(), vec![recordbatch.clone()]).unwrap();
            let json_resp = JsonResponse::from_output(
                vec![Ok(Output::RecordBatches(recordbatches))],
                format,
                None,
            )
            .await;

            match json_resp {
                JsonResponse::GreptimedbV1(json_resp) => {
                    let json_output = &json_resp.output[0];
                    if let JsonOutput::Records(r) = json_output {
                        assert_eq!(r.num_rows(), 4);
                        assert_eq!(r.num_cols(), 2);
                        let schema = r.schema.as_ref().unwrap();
                        assert_eq!(schema.column_schemas[0].name, "numbers");
                        assert_eq!(schema.column_schemas[0].data_type, "UInt32");
                        assert_eq!(r.rows[0][0], serde_json::Value::from(1));
                        assert_eq!(r.rows[0][1], serde_json::Value::Null);
                    } else {
                        panic!("invalid output type");
                    }
                }
                JsonResponse::InfluxdbV1(json_resp) => {
                    let json_output = &json_resp.results()[0];
                    assert_eq!(json_output.num_rows(), 4);
                    assert_eq!(json_output.num_cols(), 2);
                    assert_eq!(json_output.series[0].columns.clone()[0], "numbers");
                    assert_eq!(
                        json_output.series[0].values[0][0],
                        serde_json::Value::from(1)
                    );
                    assert_eq!(json_output.series[0].values[0][1], serde_json::Value::Null);
                }
            }
        }
    }
}
