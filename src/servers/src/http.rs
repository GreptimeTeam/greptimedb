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
use std::convert::Infallible;
use std::fmt::Display;
use std::net::SocketAddr;
use std::sync::Mutex as StdMutex;
use std::time::Duration;

use async_trait::async_trait;
use auth::UserProviderRef;
use axum::extract::{DefaultBodyLimit, Request};
use axum::http::StatusCode as HttpStatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::Route;
use axum::serve::ListenerExt;
use axum::{Router, middleware, routing};
use common_base::Plugins;
use common_base::readable_size::ReadableSize;
use common_recordbatch::RecordBatch;
use common_telemetry::{debug, error, info};
use common_time::Timestamp;
use common_time::timestamp::TimeUnit;
use datatypes::data_type::DataType;
use datatypes::schema::SchemaRef;
use event::{LogState, LogValidatorRef};
use futures::FutureExt;
use http::{HeaderValue, Method};
use prost::DecodeError;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use snafu::{ResultExt, ensure};
use tokio::sync::Mutex;
use tokio::sync::oneshot::{self, Sender};
use tonic::codegen::Service;
use tower::{Layer, ServiceBuilder};
use tower_http::compression::CompressionLayer;
use tower_http::cors::{AllowOrigin, Any, CorsLayer};
use tower_http::decompression::RequestDecompressionLayer;
use tower_http::trace::TraceLayer;

use self::authorize::AuthState;
use self::result::table_result::TableResponse;
use crate::configurator::HttpConfiguratorRef;
use crate::elasticsearch;
use crate::error::{
    AddressBindSnafu, AlreadyStartedSnafu, Error, InternalIoSnafu, InvalidHeaderValueSnafu,
    OtherSnafu, Result,
};
use crate::http::influxdb::{influxdb_health, influxdb_ping, influxdb_write_v1, influxdb_write_v2};
use crate::http::otlp::OtlpState;
use crate::http::prom_store::PromStoreState;
use crate::http::prometheus::{
    build_info_query, format_query, instant_query, label_values_query, labels_query, parse_query,
    range_query, series_query,
};
use crate::http::result::arrow_result::ArrowResponse;
use crate::http::result::csv_result::CsvResponse;
use crate::http::result::error_result::ErrorResponse;
use crate::http::result::greptime_result_v1::GreptimedbV1Response;
use crate::http::result::influxdb_result_v1::InfluxdbV1Response;
use crate::http::result::json_result::JsonResponse;
use crate::http::result::null_result::NullResponse;
use crate::interceptor::LogIngestInterceptorRef;
use crate::metrics::http_metrics_layer;
use crate::metrics_handler::MetricsHandler;
use crate::prometheus_handler::PrometheusHandlerRef;
use crate::query_handler::sql::ServerSqlQueryHandlerRef;
use crate::query_handler::{
    InfluxdbLineProtocolHandlerRef, JaegerQueryHandlerRef, LogQueryHandlerRef,
    OpenTelemetryProtocolHandlerRef, OpentsdbProtocolHandlerRef, PipelineHandlerRef,
    PromStoreProtocolHandlerRef,
};
use crate::request_memory_limiter::ServerMemoryLimiter;
use crate::server::Server;

pub mod authorize;
#[cfg(feature = "dashboard")]
mod dashboard;
pub mod dyn_log;
pub mod dyn_trace;
pub mod event;
pub mod extractor;
pub mod handler;
pub mod header;
pub mod influxdb;
pub mod jaeger;
pub mod logs;
pub mod loki;
pub mod mem_prof;
mod memory_limit;
pub mod opentsdb;
pub mod otlp;
pub mod pprof;
pub mod prom_store;
pub mod prometheus;
pub mod result;
mod timeout;
pub mod utils;

use result::HttpOutputWriter;
pub(crate) use timeout::DynamicTimeoutLayer;

mod client_ip;
mod hints;
mod read_preference;
#[cfg(any(test, feature = "testing"))]
pub mod test_helpers;

pub const HTTP_API_VERSION: &str = "v1";
pub const HTTP_API_PREFIX: &str = "/v1/";
/// Default http body limit (64M).
const DEFAULT_BODY_LIMIT: ReadableSize = ReadableSize::mb(64);

/// Authorization header
pub const AUTHORIZATION_HEADER: &str = "x-greptime-auth";

// TODO(fys): This is a temporary workaround, it will be improved later
pub static PUBLIC_APIS: [&str; 3] = ["/v1/influxdb/ping", "/v1/influxdb/health", "/v1/health"];

#[derive(Default)]
pub struct HttpServer {
    router: StdMutex<Router>,
    shutdown_tx: Mutex<Option<Sender<()>>>,
    user_provider: Option<UserProviderRef>,
    memory_limiter: ServerMemoryLimiter,

    // plugins
    plugins: Plugins,

    // server configs
    options: HttpOptions,
    bind_addr: Option<SocketAddr>,
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

    /// Validation mode while decoding Prometheus remote write requests.
    pub prom_validation_mode: PromValidationMode,

    pub cors_allowed_origins: Vec<String>,

    pub enable_cors: bool,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PromValidationMode {
    /// Force UTF8 validation
    Strict,
    /// Allow lossy UTF8 strings
    Lossy,
    /// Do not validate UTF8 strings.
    Unchecked,
}

impl PromValidationMode {
    /// Decodes provided bytes to [String] with optional UTF-8 validation.
    pub fn decode_string(&self, bytes: &[u8]) -> std::result::Result<String, DecodeError> {
        let result = match self {
            PromValidationMode::Strict => match String::from_utf8(bytes.to_vec()) {
                Ok(s) => s,
                Err(e) => {
                    debug!("Invalid UTF-8 string value: {:?}, error: {:?}", bytes, e);
                    return Err(DecodeError::new("invalid utf-8"));
                }
            },
            PromValidationMode::Lossy => String::from_utf8_lossy(bytes).to_string(),
            PromValidationMode::Unchecked => unsafe { String::from_utf8_unchecked(bytes.to_vec()) },
        };
        Ok(result)
    }
}

impl Default for HttpOptions {
    fn default() -> Self {
        Self {
            addr: "127.0.0.1:4000".to_string(),
            timeout: Duration::from_secs(0),
            disable_dashboard: false,
            body_limit: DEFAULT_BODY_LIMIT,
            cors_allowed_origins: Vec::new(),
            enable_cors: true,
            prom_validation_mode: PromValidationMode::Strict,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ColumnSchema {
    name: String,
    data_type: String,
}

impl ColumnSchema {
    pub fn new(name: String, data_type: String) -> ColumnSchema {
        ColumnSchema { name, data_type }
    }
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct OutputSchema {
    column_schemas: Vec<ColumnSchema>,
}

impl OutputSchema {
    pub fn new(columns: Vec<ColumnSchema>) -> OutputSchema {
        OutputSchema {
            column_schemas: columns,
        }
    }
}

impl From<SchemaRef> for OutputSchema {
    fn from(schema: SchemaRef) -> OutputSchema {
        OutputSchema {
            column_schemas: schema
                .column_schemas()
                .iter()
                .map(|cs| ColumnSchema {
                    name: cs.name.clone(),
                    data_type: cs.data_type.name(),
                })
                .collect(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct HttpRecordsOutput {
    schema: OutputSchema,
    rows: Vec<Vec<Value>>,
    // total_rows is equal to rows.len() in most cases,
    // the Dashboard query result may be truncated, so we need to return the total_rows.
    #[serde(default)]
    total_rows: usize,

    // plan level execution metrics
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    #[serde(default)]
    metrics: HashMap<String, Value>,
}

impl HttpRecordsOutput {
    pub fn num_rows(&self) -> usize {
        self.rows.len()
    }

    pub fn num_cols(&self) -> usize {
        self.schema.column_schemas.len()
    }

    pub fn schema(&self) -> &OutputSchema {
        &self.schema
    }

    pub fn rows(&self) -> &Vec<Vec<Value>> {
        &self.rows
    }
}

impl HttpRecordsOutput {
    pub fn try_new(
        schema: SchemaRef,
        recordbatches: Vec<RecordBatch>,
    ) -> std::result::Result<HttpRecordsOutput, Error> {
        if recordbatches.is_empty() {
            Ok(HttpRecordsOutput {
                schema: OutputSchema::from(schema),
                rows: vec![],
                total_rows: 0,
                metrics: Default::default(),
            })
        } else {
            let num_rows = recordbatches.iter().map(|r| r.num_rows()).sum::<usize>();
            let mut rows = Vec::with_capacity(num_rows);

            for recordbatch in recordbatches {
                let mut writer = HttpOutputWriter::new(schema.num_columns(), None);
                writer.write(recordbatch, &mut rows)?;
            }

            Ok(HttpRecordsOutput {
                schema: OutputSchema::from(schema),
                total_rows: rows.len(),
                rows,
                metrics: Default::default(),
            })
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Eq, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum GreptimeQueryOutput {
    AffectedRows(usize),
    Records(HttpRecordsOutput),
}

/// It allows the results of SQL queries to be presented in different formats.
#[derive(Default, Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResponseFormat {
    Arrow,
    // (with_names, with_types)
    Csv(bool, bool),
    Table,
    #[default]
    GreptimedbV1,
    InfluxdbV1,
    Json,
    Null,
}

impl ResponseFormat {
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "arrow" => Some(ResponseFormat::Arrow),
            "csv" => Some(ResponseFormat::Csv(false, false)),
            "csvwithnames" => Some(ResponseFormat::Csv(true, false)),
            "csvwithnamesandtypes" => Some(ResponseFormat::Csv(true, true)),
            "table" => Some(ResponseFormat::Table),
            "greptimedb_v1" => Some(ResponseFormat::GreptimedbV1),
            "influxdb_v1" => Some(ResponseFormat::InfluxdbV1),
            "json" => Some(ResponseFormat::Json),
            "null" => Some(ResponseFormat::Null),
            _ => None,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            ResponseFormat::Arrow => "arrow",
            ResponseFormat::Csv(_, _) => "csv",
            ResponseFormat::Table => "table",
            ResponseFormat::GreptimedbV1 => "greptimedb_v1",
            ResponseFormat::InfluxdbV1 => "influxdb_v1",
            ResponseFormat::Json => "json",
            ResponseFormat::Null => "null",
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

#[derive(Serialize, Deserialize, Debug)]
pub enum HttpResponse {
    Arrow(ArrowResponse),
    Csv(CsvResponse),
    Table(TableResponse),
    Error(ErrorResponse),
    GreptimedbV1(GreptimedbV1Response),
    InfluxdbV1(InfluxdbV1Response),
    Json(JsonResponse),
    Null(NullResponse),
}

impl HttpResponse {
    pub fn with_execution_time(self, execution_time: u64) -> Self {
        match self {
            HttpResponse::Arrow(resp) => resp.with_execution_time(execution_time).into(),
            HttpResponse::Csv(resp) => resp.with_execution_time(execution_time).into(),
            HttpResponse::Table(resp) => resp.with_execution_time(execution_time).into(),
            HttpResponse::GreptimedbV1(resp) => resp.with_execution_time(execution_time).into(),
            HttpResponse::InfluxdbV1(resp) => resp.with_execution_time(execution_time).into(),
            HttpResponse::Json(resp) => resp.with_execution_time(execution_time).into(),
            HttpResponse::Null(resp) => resp.with_execution_time(execution_time).into(),
            HttpResponse::Error(resp) => resp.with_execution_time(execution_time).into(),
        }
    }

    pub fn with_limit(self, limit: usize) -> Self {
        match self {
            HttpResponse::Csv(resp) => resp.with_limit(limit).into(),
            HttpResponse::Table(resp) => resp.with_limit(limit).into(),
            HttpResponse::GreptimedbV1(resp) => resp.with_limit(limit).into(),
            HttpResponse::Json(resp) => resp.with_limit(limit).into(),
            _ => self,
        }
    }
}

pub fn process_with_limit(
    mut outputs: Vec<GreptimeQueryOutput>,
    limit: usize,
) -> Vec<GreptimeQueryOutput> {
    outputs
        .drain(..)
        .map(|data| match data {
            GreptimeQueryOutput::Records(mut records) => {
                if records.rows.len() > limit {
                    records.rows.truncate(limit);
                    records.total_rows = limit;
                }
                GreptimeQueryOutput::Records(records)
            }
            _ => data,
        })
        .collect()
}

impl IntoResponse for HttpResponse {
    fn into_response(self) -> Response {
        match self {
            HttpResponse::Arrow(resp) => resp.into_response(),
            HttpResponse::Csv(resp) => resp.into_response(),
            HttpResponse::Table(resp) => resp.into_response(),
            HttpResponse::GreptimedbV1(resp) => resp.into_response(),
            HttpResponse::InfluxdbV1(resp) => resp.into_response(),
            HttpResponse::Json(resp) => resp.into_response(),
            HttpResponse::Null(resp) => resp.into_response(),
            HttpResponse::Error(resp) => resp.into_response(),
        }
    }
}

impl From<ArrowResponse> for HttpResponse {
    fn from(value: ArrowResponse) -> Self {
        HttpResponse::Arrow(value)
    }
}

impl From<CsvResponse> for HttpResponse {
    fn from(value: CsvResponse) -> Self {
        HttpResponse::Csv(value)
    }
}

impl From<TableResponse> for HttpResponse {
    fn from(value: TableResponse) -> Self {
        HttpResponse::Table(value)
    }
}

impl From<ErrorResponse> for HttpResponse {
    fn from(value: ErrorResponse) -> Self {
        HttpResponse::Error(value)
    }
}

impl From<GreptimedbV1Response> for HttpResponse {
    fn from(value: GreptimedbV1Response) -> Self {
        HttpResponse::GreptimedbV1(value)
    }
}

impl From<InfluxdbV1Response> for HttpResponse {
    fn from(value: InfluxdbV1Response) -> Self {
        HttpResponse::InfluxdbV1(value)
    }
}

impl From<JsonResponse> for HttpResponse {
    fn from(value: JsonResponse) -> Self {
        HttpResponse::Json(value)
    }
}

impl From<NullResponse> for HttpResponse {
    fn from(value: NullResponse) -> Self {
        HttpResponse::Null(value)
    }
}

#[derive(Clone)]
pub struct ApiState {
    pub sql_handler: ServerSqlQueryHandlerRef,
}

#[derive(Clone)]
pub struct GreptimeOptionsConfigState {
    pub greptime_config_options: String,
}

pub struct HttpServerBuilder {
    options: HttpOptions,
    plugins: Plugins,
    user_provider: Option<UserProviderRef>,
    router: Router,
    memory_limiter: ServerMemoryLimiter,
}

impl HttpServerBuilder {
    pub fn new(options: HttpOptions) -> Self {
        Self {
            options,
            plugins: Plugins::default(),
            user_provider: None,
            router: Router::new(),
            memory_limiter: ServerMemoryLimiter::default(),
        }
    }

    /// Set a global memory limiter for all server protocols.
    pub fn with_memory_limiter(mut self, limiter: ServerMemoryLimiter) -> Self {
        self.memory_limiter = limiter;
        self
    }

    pub fn with_sql_handler(self, sql_handler: ServerSqlQueryHandlerRef) -> Self {
        let sql_router = HttpServer::route_sql(ApiState { sql_handler });

        Self {
            router: self
                .router
                .nest(&format!("/{HTTP_API_VERSION}"), sql_router),
            ..self
        }
    }

    pub fn with_logs_handler(self, logs_handler: LogQueryHandlerRef) -> Self {
        let logs_router = HttpServer::route_logs(logs_handler);

        Self {
            router: self
                .router
                .nest(&format!("/{HTTP_API_VERSION}"), logs_router),
            ..self
        }
    }

    pub fn with_opentsdb_handler(self, handler: OpentsdbProtocolHandlerRef) -> Self {
        Self {
            router: self.router.nest(
                &format!("/{HTTP_API_VERSION}/opentsdb"),
                HttpServer::route_opentsdb(handler),
            ),
            ..self
        }
    }

    pub fn with_influxdb_handler(self, handler: InfluxdbLineProtocolHandlerRef) -> Self {
        Self {
            router: self.router.nest(
                &format!("/{HTTP_API_VERSION}/influxdb"),
                HttpServer::route_influxdb(handler),
            ),
            ..self
        }
    }

    pub fn with_prom_handler(
        self,
        handler: PromStoreProtocolHandlerRef,
        pipeline_handler: Option<PipelineHandlerRef>,
        prom_store_with_metric_engine: bool,
        prom_validation_mode: PromValidationMode,
    ) -> Self {
        let state = PromStoreState {
            prom_store_handler: handler,
            pipeline_handler,
            prom_store_with_metric_engine,
            prom_validation_mode,
        };

        Self {
            router: self.router.nest(
                &format!("/{HTTP_API_VERSION}/prometheus"),
                HttpServer::route_prom(state),
            ),
            ..self
        }
    }

    pub fn with_prometheus_handler(self, handler: PrometheusHandlerRef) -> Self {
        Self {
            router: self.router.nest(
                &format!("/{HTTP_API_VERSION}/prometheus/api/v1"),
                HttpServer::route_prometheus(handler),
            ),
            ..self
        }
    }

    pub fn with_otlp_handler(
        self,
        handler: OpenTelemetryProtocolHandlerRef,
        with_metric_engine: bool,
    ) -> Self {
        Self {
            router: self.router.nest(
                &format!("/{HTTP_API_VERSION}/otlp"),
                HttpServer::route_otlp(handler, with_metric_engine),
            ),
            ..self
        }
    }

    pub fn with_user_provider(self, user_provider: UserProviderRef) -> Self {
        Self {
            user_provider: Some(user_provider),
            ..self
        }
    }

    pub fn with_metrics_handler(self, handler: MetricsHandler) -> Self {
        Self {
            router: self.router.merge(HttpServer::route_metrics(handler)),
            ..self
        }
    }

    pub fn with_log_ingest_handler(
        self,
        handler: PipelineHandlerRef,
        validator: Option<LogValidatorRef>,
        ingest_interceptor: Option<LogIngestInterceptorRef<Error>>,
    ) -> Self {
        let log_state = LogState {
            log_handler: handler,
            log_validator: validator,
            ingest_interceptor,
        };

        let router = self.router.nest(
            &format!("/{HTTP_API_VERSION}"),
            HttpServer::route_pipelines(log_state.clone()),
        );
        // deprecated since v0.11.0. Use `/logs` and `/pipelines` instead.
        let router = router.nest(
            &format!("/{HTTP_API_VERSION}/events"),
            #[allow(deprecated)]
            HttpServer::route_log_deprecated(log_state.clone()),
        );

        let router = router.nest(
            &format!("/{HTTP_API_VERSION}/loki"),
            HttpServer::route_loki(log_state.clone()),
        );

        let router = router.nest(
            &format!("/{HTTP_API_VERSION}/elasticsearch"),
            HttpServer::route_elasticsearch(log_state.clone()),
        );

        let router = router.nest(
            &format!("/{HTTP_API_VERSION}/elasticsearch/"),
            Router::new()
                .route("/", routing::get(elasticsearch::handle_get_version))
                .with_state(log_state),
        );

        Self { router, ..self }
    }

    pub fn with_plugins(self, plugins: Plugins) -> Self {
        Self { plugins, ..self }
    }

    pub fn with_greptime_config_options(self, opts: String) -> Self {
        let config_router = HttpServer::route_config(GreptimeOptionsConfigState {
            greptime_config_options: opts,
        });

        Self {
            router: self.router.merge(config_router),
            ..self
        }
    }

    pub fn with_jaeger_handler(self, handler: JaegerQueryHandlerRef) -> Self {
        Self {
            router: self.router.nest(
                &format!("/{HTTP_API_VERSION}/jaeger"),
                HttpServer::route_jaeger(handler),
            ),
            ..self
        }
    }

    pub fn with_extra_router(self, router: Router) -> Self {
        Self {
            router: self.router.merge(router),
            ..self
        }
    }

    pub fn add_layer<L>(self, layer: L) -> Self
    where
        L: Layer<Route> + Clone + Send + Sync + 'static,
        L::Service: Service<Request> + Clone + Send + Sync + 'static,
        <L::Service as Service<Request>>::Response: IntoResponse + 'static,
        <L::Service as Service<Request>>::Error: Into<Infallible> + 'static,
        <L::Service as Service<Request>>::Future: Send + 'static,
    {
        Self {
            router: self.router.layer(layer),
            ..self
        }
    }

    pub fn build(self) -> HttpServer {
        HttpServer {
            options: self.options,
            user_provider: self.user_provider,
            shutdown_tx: Mutex::new(None),
            plugins: self.plugins,
            router: StdMutex::new(self.router),
            bind_addr: None,
            memory_limiter: self.memory_limiter,
        }
    }
}

impl HttpServer {
    /// Gets the router and adds necessary root routes (health, status, dashboard).
    pub fn make_app(&self) -> Router {
        let mut router = {
            let router = self.router.lock().unwrap();
            router.clone()
        };

        router = router
            .route(
                "/health",
                routing::get(handler::health).post(handler::health),
            )
            .route(
                &format!("/{HTTP_API_VERSION}/health"),
                routing::get(handler::health).post(handler::health),
            )
            .route(
                "/ready",
                routing::get(handler::health).post(handler::health),
            );

        router = router.route("/status", routing::get(handler::status));

        #[cfg(feature = "dashboard")]
        {
            if !self.options.disable_dashboard {
                info!("Enable dashboard service at '/dashboard'");
                // redirect /dashboard to /dashboard/
                router = router.route(
                    "/dashboard",
                    routing::get(|uri: axum::http::uri::Uri| async move {
                        let path = uri.path();
                        let query = uri.query().map(|q| format!("?{}", q)).unwrap_or_default();

                        let new_uri = format!("{}/{}", path, query);
                        axum::response::Redirect::permanent(&new_uri)
                    }),
                );

                // "/dashboard" and "/dashboard/" are two different paths in Axum.
                // We cannot nest "/dashboard/", because we already mapping "/dashboard/{*x}" while nesting "/dashboard".
                // So we explicitly route "/dashboard/" here.
                router = router
                    .route(
                        "/dashboard/",
                        routing::get(dashboard::static_handler).post(dashboard::static_handler),
                    )
                    .route(
                        "/dashboard/{*x}",
                        routing::get(dashboard::static_handler).post(dashboard::static_handler),
                    );
            }
        }

        // Add a layer to collect HTTP metrics for axum.
        router = router.route_layer(middleware::from_fn(http_metrics_layer));

        router
    }

    /// Attaches middlewares and debug routes to the router.
    /// Callers should call this method after [HttpServer::make_app()].
    pub fn build(&self, router: Router) -> Result<Router> {
        let timeout_layer = if self.options.timeout != Duration::default() {
            Some(ServiceBuilder::new().layer(DynamicTimeoutLayer::new(self.options.timeout)))
        } else {
            info!("HTTP server timeout is disabled");
            None
        };
        let body_limit_layer = if self.options.body_limit != ReadableSize(0) {
            Some(
                ServiceBuilder::new()
                    .layer(DefaultBodyLimit::max(self.options.body_limit.0 as usize)),
            )
        } else {
            info!("HTTP server body limit is disabled");
            None
        };
        let cors_layer = if self.options.enable_cors {
            Some(
                CorsLayer::new()
                    .allow_methods([
                        Method::GET,
                        Method::POST,
                        Method::PUT,
                        Method::DELETE,
                        Method::HEAD,
                    ])
                    .allow_origin(if self.options.cors_allowed_origins.is_empty() {
                        AllowOrigin::from(Any)
                    } else {
                        AllowOrigin::from(
                            self.options
                                .cors_allowed_origins
                                .iter()
                                .map(|s| {
                                    HeaderValue::from_str(s.as_str())
                                        .context(InvalidHeaderValueSnafu)
                                })
                                .collect::<Result<Vec<HeaderValue>>>()?,
                        )
                    })
                    .allow_headers(Any),
            )
        } else {
            info!("HTTP server cross-origin is disabled");
            None
        };

        Ok(router
            // middlewares
            .layer(
                ServiceBuilder::new()
                    // disable on failure tracing. because printing out isn't very helpful,
                    // and we have impl IntoResponse for Error. It will print out more detailed error messages
                    .layer(TraceLayer::new_for_http().on_failure(()))
                    .option_layer(cors_layer)
                    .option_layer(timeout_layer)
                    .option_layer(body_limit_layer)
                    // memory limit layer - must be before body is consumed
                    .layer(middleware::from_fn_with_state(
                        self.memory_limiter.clone(),
                        memory_limit::memory_limit_middleware,
                    ))
                    // auth layer
                    .layer(middleware::from_fn_with_state(
                        AuthState::new(self.user_provider.clone()),
                        authorize::check_http_auth,
                    ))
                    .layer(middleware::from_fn(hints::extract_hints))
                    .layer(middleware::from_fn(client_ip::log_error_with_client_ip))
                    .layer(middleware::from_fn(
                        read_preference::extract_read_preference,
                    )),
            )
            // Handlers for debug, we don't expect a timeout.
            .nest(
                "/debug",
                Router::new()
                    // handler for changing log level dynamically
                    .route("/log_level", routing::post(dyn_log::dyn_log_handler))
                    .route("/enable_trace", routing::post(dyn_trace::dyn_trace_handler))
                    .nest(
                        "/prof",
                        Router::new()
                            .route("/cpu", routing::post(pprof::pprof_handler))
                            .route("/mem", routing::post(mem_prof::mem_prof_handler))
                            .route(
                                "/mem/activate",
                                routing::post(mem_prof::activate_heap_prof_handler),
                            )
                            .route(
                                "/mem/deactivate",
                                routing::post(mem_prof::deactivate_heap_prof_handler),
                            )
                            .route(
                                "/mem/status",
                                routing::get(mem_prof::heap_prof_status_handler),
                            ) // jemalloc gdump flag status and toggle
                            .route(
                                "/mem/gdump",
                                routing::get(mem_prof::gdump_status_handler)
                                    .post(mem_prof::gdump_toggle_handler),
                            ),
                    ),
            ))
    }

    fn route_metrics<S>(metrics_handler: MetricsHandler) -> Router<S> {
        Router::new()
            .route("/metrics", routing::get(handler::metrics))
            .with_state(metrics_handler)
    }

    fn route_loki<S>(log_state: LogState) -> Router<S> {
        Router::new()
            .route("/api/v1/push", routing::post(loki::loki_ingest))
            .layer(
                ServiceBuilder::new()
                    .layer(RequestDecompressionLayer::new().pass_through_unaccepted(true)),
            )
            .with_state(log_state)
    }

    fn route_elasticsearch<S>(log_state: LogState) -> Router<S> {
        Router::new()
            // Return fake responsefor HEAD '/' request.
            .route(
                "/",
                routing::head((HttpStatusCode::OK, elasticsearch::elasticsearch_headers())),
            )
            // Return fake response for Elasticsearch version request.
            .route("/", routing::get(elasticsearch::handle_get_version))
            // Return fake response for Elasticsearch license request.
            .route("/_license", routing::get(elasticsearch::handle_get_license))
            .route("/_bulk", routing::post(elasticsearch::handle_bulk_api))
            .route(
                "/{index}/_bulk",
                routing::post(elasticsearch::handle_bulk_api_with_index),
            )
            // Return fake response for Elasticsearch ilm request.
            .route(
                "/_ilm/policy/{*path}",
                routing::any((
                    HttpStatusCode::OK,
                    elasticsearch::elasticsearch_headers(),
                    axum::Json(serde_json::json!({})),
                )),
            )
            // Return fake response for Elasticsearch index template request.
            .route(
                "/_index_template/{*path}",
                routing::any((
                    HttpStatusCode::OK,
                    elasticsearch::elasticsearch_headers(),
                    axum::Json(serde_json::json!({})),
                )),
            )
            // Return fake response for Elasticsearch ingest pipeline request.
            // See: https://www.elastic.co/guide/en/elasticsearch/reference/8.8/put-pipeline-api.html.
            .route(
                "/_ingest/{*path}",
                routing::any((
                    HttpStatusCode::OK,
                    elasticsearch::elasticsearch_headers(),
                    axum::Json(serde_json::json!({})),
                )),
            )
            // Return fake response for Elasticsearch nodes discovery request.
            // See: https://www.elastic.co/guide/en/elasticsearch/reference/8.8/cluster.html.
            .route(
                "/_nodes/{*path}",
                routing::any((
                    HttpStatusCode::OK,
                    elasticsearch::elasticsearch_headers(),
                    axum::Json(serde_json::json!({})),
                )),
            )
            // Return fake response for Logstash APIs requests.
            // See: https://www.elastic.co/guide/en/elasticsearch/reference/8.8/logstash-apis.html
            .route(
                "/logstash/{*path}",
                routing::any((
                    HttpStatusCode::OK,
                    elasticsearch::elasticsearch_headers(),
                    axum::Json(serde_json::json!({})),
                )),
            )
            .route(
                "/_logstash/{*path}",
                routing::any((
                    HttpStatusCode::OK,
                    elasticsearch::elasticsearch_headers(),
                    axum::Json(serde_json::json!({})),
                )),
            )
            .layer(ServiceBuilder::new().layer(RequestDecompressionLayer::new()))
            .with_state(log_state)
    }

    #[deprecated(since = "0.11.0", note = "Use `route_pipelines()` instead.")]
    fn route_log_deprecated<S>(log_state: LogState) -> Router<S> {
        Router::new()
            .route("/logs", routing::post(event::log_ingester))
            .route(
                "/pipelines/{pipeline_name}",
                routing::get(event::query_pipeline),
            )
            .route(
                "/pipelines/{pipeline_name}",
                routing::post(event::add_pipeline),
            )
            .route(
                "/pipelines/{pipeline_name}",
                routing::delete(event::delete_pipeline),
            )
            .route("/pipelines/dryrun", routing::post(event::pipeline_dryrun))
            .layer(
                ServiceBuilder::new()
                    .layer(RequestDecompressionLayer::new().pass_through_unaccepted(true)),
            )
            .with_state(log_state)
    }

    fn route_pipelines<S>(log_state: LogState) -> Router<S> {
        Router::new()
            .route("/ingest", routing::post(event::log_ingester))
            .route(
                "/pipelines/{pipeline_name}",
                routing::get(event::query_pipeline),
            )
            .route(
                "/pipelines/{pipeline_name}/ddl",
                routing::get(event::query_pipeline_ddl),
            )
            .route(
                "/pipelines/{pipeline_name}",
                routing::post(event::add_pipeline),
            )
            .route(
                "/pipelines/{pipeline_name}",
                routing::delete(event::delete_pipeline),
            )
            .route("/pipelines/_dryrun", routing::post(event::pipeline_dryrun))
            .layer(
                ServiceBuilder::new()
                    .layer(RequestDecompressionLayer::new().pass_through_unaccepted(true)),
            )
            .with_state(log_state)
    }

    fn route_sql<S>(api_state: ApiState) -> Router<S> {
        Router::new()
            .route("/sql", routing::get(handler::sql).post(handler::sql))
            .route(
                "/sql/parse",
                routing::get(handler::sql_parse).post(handler::sql_parse),
            )
            .route(
                "/sql/format",
                routing::get(handler::sql_format).post(handler::sql_format),
            )
            .route(
                "/promql",
                routing::get(handler::promql).post(handler::promql),
            )
            .with_state(api_state)
    }

    fn route_logs<S>(log_handler: LogQueryHandlerRef) -> Router<S> {
        Router::new()
            .route("/logs", routing::get(logs::logs).post(logs::logs))
            .with_state(log_handler)
    }

    /// Route Prometheus [HTTP API].
    ///
    /// [HTTP API]: https://prometheus.io/docs/prometheus/latest/querying/api/
    pub fn route_prometheus<S>(prometheus_handler: PrometheusHandlerRef) -> Router<S> {
        Router::new()
            .route(
                "/format_query",
                routing::post(format_query).get(format_query),
            )
            .route("/status/buildinfo", routing::get(build_info_query))
            .route("/query", routing::post(instant_query).get(instant_query))
            .route("/query_range", routing::post(range_query).get(range_query))
            .route("/labels", routing::post(labels_query).get(labels_query))
            .route("/series", routing::post(series_query).get(series_query))
            .route("/parse_query", routing::post(parse_query).get(parse_query))
            .route(
                "/label/{label_name}/values",
                routing::get(label_values_query),
            )
            .layer(ServiceBuilder::new().layer(CompressionLayer::new()))
            .with_state(prometheus_handler)
    }

    /// Route Prometheus remote [read] and [write] API. In other places the related modules are
    /// called `prom_store`.
    ///
    /// [read]: https://prometheus.io/docs/prometheus/latest/querying/remote_read_api/
    /// [write]: https://prometheus.io/docs/concepts/remote_write_spec/
    fn route_prom<S>(state: PromStoreState) -> Router<S> {
        Router::new()
            .route("/read", routing::post(prom_store::remote_read))
            .route("/write", routing::post(prom_store::remote_write))
            .with_state(state)
    }

    fn route_influxdb<S>(influxdb_handler: InfluxdbLineProtocolHandlerRef) -> Router<S> {
        Router::new()
            .route("/write", routing::post(influxdb_write_v1))
            .route("/api/v2/write", routing::post(influxdb_write_v2))
            .layer(
                ServiceBuilder::new()
                    .layer(RequestDecompressionLayer::new().pass_through_unaccepted(true)),
            )
            .route("/ping", routing::get(influxdb_ping))
            .route("/health", routing::get(influxdb_health))
            .with_state(influxdb_handler)
    }

    fn route_opentsdb<S>(opentsdb_handler: OpentsdbProtocolHandlerRef) -> Router<S> {
        Router::new()
            .route("/api/put", routing::post(opentsdb::put))
            .with_state(opentsdb_handler)
    }

    fn route_otlp<S>(
        otlp_handler: OpenTelemetryProtocolHandlerRef,
        with_metric_engine: bool,
    ) -> Router<S> {
        Router::new()
            .route("/v1/metrics", routing::post(otlp::metrics))
            .route("/v1/traces", routing::post(otlp::traces))
            .route("/v1/logs", routing::post(otlp::logs))
            .layer(
                ServiceBuilder::new()
                    .layer(RequestDecompressionLayer::new().pass_through_unaccepted(true)),
            )
            .with_state(OtlpState {
                with_metric_engine,
                handler: otlp_handler,
            })
    }

    fn route_config<S>(state: GreptimeOptionsConfigState) -> Router<S> {
        Router::new()
            .route("/config", routing::get(handler::config))
            .with_state(state)
    }

    fn route_jaeger<S>(handler: JaegerQueryHandlerRef) -> Router<S> {
        Router::new()
            .route("/api/services", routing::get(jaeger::handle_get_services))
            .route(
                "/api/services/{service_name}/operations",
                routing::get(jaeger::handle_get_operations_by_service),
            )
            .route(
                "/api/operations",
                routing::get(jaeger::handle_get_operations),
            )
            .route("/api/traces", routing::get(jaeger::handle_find_traces))
            .route(
                "/api/traces/{trace_id}",
                routing::get(jaeger::handle_get_trace),
            )
            .with_state(handler)
    }
}

pub const HTTP_SERVER: &str = "HTTP_SERVER";

#[async_trait]
impl Server for HttpServer {
    async fn shutdown(&self) -> Result<()> {
        let mut shutdown_tx = self.shutdown_tx.lock().await;
        if let Some(tx) = shutdown_tx.take()
            && tx.send(()).is_err()
        {
            info!("Receiver dropped, the HTTP server has already exited");
        }
        info!("Shutdown HTTP server");

        Ok(())
    }

    async fn start(&mut self, listening: SocketAddr) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        let serve = {
            let mut shutdown_tx = self.shutdown_tx.lock().await;
            ensure!(
                shutdown_tx.is_none(),
                AlreadyStartedSnafu { server: "HTTP" }
            );

            let mut app = self.make_app();
            if let Some(configurator) = self.plugins.get::<HttpConfiguratorRef<()>>() {
                app = configurator
                    .configure_http(app, ())
                    .await
                    .context(OtherSnafu)?;
            }
            let app = self.build(app)?;
            let listener = tokio::net::TcpListener::bind(listening)
                .await
                .context(AddressBindSnafu { addr: listening })?
                .tap_io(|tcp_stream| {
                    if let Err(e) = tcp_stream.set_nodelay(true) {
                        error!(e; "Failed to set TCP_NODELAY on incoming connection");
                    }
                });
            let serve = axum::serve(
                listener,
                app.into_make_service_with_connect_info::<SocketAddr>(),
            );

            // FIXME(yingwen): Support keepalive.
            // See:
            // - https://github.com/tokio-rs/axum/discussions/2939
            // - https://stackoverflow.com/questions/73069718/how-do-i-keep-alive-tokiotcpstream-in-rust
            // let server = axum::Server::try_bind(&listening)
            //     .with_context(|_| AddressBindSnafu { addr: listening })?
            //     .tcp_nodelay(true)
            //     // Enable TCP keepalive to close the dangling established connections.
            //     // It's configured to let the keepalive probes first send after the connection sits
            //     // idle for 59 minutes, and then send every 10 seconds for 6 times.
            //     // So the connection will be closed after roughly 1 hour.
            //     .tcp_keepalive(Some(Duration::from_secs(59 * 60)))
            //     .tcp_keepalive_interval(Some(Duration::from_secs(10)))
            //     .tcp_keepalive_retries(Some(6))
            //     .serve(app.into_make_service());

            *shutdown_tx = Some(tx);

            serve
        };
        let listening = serve.local_addr().context(InternalIoSnafu)?;
        info!("HTTP server is bound to {}", listening);

        common_runtime::spawn_global(async move {
            if let Err(e) = serve
                .with_graceful_shutdown(rx.map(drop))
                .await
                .context(InternalIoSnafu)
            {
                error!(e; "Failed to shutdown http server");
            }
        });

        self.bind_addr = Some(listening);
        Ok(())
    }

    fn name(&self) -> &str {
        HTTP_SERVER
    }

    fn bind_addr(&self) -> Option<SocketAddr> {
        self.bind_addr
    }
}

#[cfg(test)]
mod test {
    use std::future::pending;
    use std::io::Cursor;
    use std::sync::Arc;

    use arrow_ipc::reader::FileReader;
    use arrow_schema::DataType;
    use axum::handler::Handler;
    use axum::http::StatusCode;
    use axum::routing::get;
    use common_query::Output;
    use common_recordbatch::RecordBatches;
    use datafusion_expr::LogicalPlan;
    use datatypes::prelude::*;
    use datatypes::schema::{ColumnSchema, Schema};
    use datatypes::vectors::{StringVector, UInt32Vector};
    use header::constants::GREPTIME_DB_HEADER_TIMEOUT;
    use query::parser::PromQuery;
    use query::query_engine::DescribeResult;
    use session::context::QueryContextRef;
    use sql::statements::statement::Statement;
    use tokio::sync::mpsc;
    use tokio::time::Instant;

    use super::*;
    use crate::error::Error;
    use crate::http::test_helpers::TestClient;
    use crate::query_handler::sql::{ServerSqlQueryHandlerAdapter, SqlQueryHandler};

    struct DummyInstance {
        _tx: mpsc::Sender<(String, Vec<u8>)>,
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
            _stmt: Option<Statement>,
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

    fn timeout() -> DynamicTimeoutLayer {
        DynamicTimeoutLayer::new(Duration::from_millis(10))
    }

    async fn forever() {
        pending().await
    }

    fn make_test_app(tx: mpsc::Sender<(String, Vec<u8>)>) -> Router {
        make_test_app_custom(tx, HttpOptions::default())
    }

    fn make_test_app_custom(tx: mpsc::Sender<(String, Vec<u8>)>, options: HttpOptions) -> Router {
        let instance = Arc::new(DummyInstance { _tx: tx });
        let sql_instance = ServerSqlQueryHandlerAdapter::arc(instance.clone());
        let server = HttpServerBuilder::new(options)
            .with_sql_handler(sql_instance)
            .build();
        server.build(server.make_app()).unwrap().route(
            "/test/timeout",
            get(forever.layer(ServiceBuilder::new().layer(timeout()))),
        )
    }

    #[tokio::test]
    pub async fn test_cors() {
        // cors is on by default
        let (tx, _rx) = mpsc::channel(100);
        let app = make_test_app(tx);
        let client = TestClient::new(app).await;

        let res = client.get("/health").send().await;

        assert_eq!(res.status(), StatusCode::OK);
        assert_eq!(
            res.headers()
                .get(http::header::ACCESS_CONTROL_ALLOW_ORIGIN)
                .expect("expect cors header origin"),
            "*"
        );

        let res = client.get("/v1/health").send().await;

        assert_eq!(res.status(), StatusCode::OK);
        assert_eq!(
            res.headers()
                .get(http::header::ACCESS_CONTROL_ALLOW_ORIGIN)
                .expect("expect cors header origin"),
            "*"
        );

        let res = client
            .options("/health")
            .header("Access-Control-Request-Headers", "x-greptime-auth")
            .header("Access-Control-Request-Method", "DELETE")
            .header("Origin", "https://example.com")
            .send()
            .await;
        assert_eq!(res.status(), StatusCode::OK);
        assert_eq!(
            res.headers()
                .get(http::header::ACCESS_CONTROL_ALLOW_ORIGIN)
                .expect("expect cors header origin"),
            "*"
        );
        assert_eq!(
            res.headers()
                .get(http::header::ACCESS_CONTROL_ALLOW_HEADERS)
                .expect("expect cors header headers"),
            "*"
        );
        assert_eq!(
            res.headers()
                .get(http::header::ACCESS_CONTROL_ALLOW_METHODS)
                .expect("expect cors header methods"),
            "GET,POST,PUT,DELETE,HEAD"
        );
    }

    #[tokio::test]
    pub async fn test_cors_custom_origins() {
        // cors is on by default
        let (tx, _rx) = mpsc::channel(100);
        let origin = "https://example.com";

        let options = HttpOptions {
            cors_allowed_origins: vec![origin.to_string()],
            ..Default::default()
        };

        let app = make_test_app_custom(tx, options);
        let client = TestClient::new(app).await;

        let res = client.get("/health").header("Origin", origin).send().await;

        assert_eq!(res.status(), StatusCode::OK);
        assert_eq!(
            res.headers()
                .get(http::header::ACCESS_CONTROL_ALLOW_ORIGIN)
                .expect("expect cors header origin"),
            origin
        );

        let res = client
            .get("/health")
            .header("Origin", "https://notallowed.com")
            .send()
            .await;

        assert_eq!(res.status(), StatusCode::OK);
        assert!(
            !res.headers()
                .contains_key(http::header::ACCESS_CONTROL_ALLOW_ORIGIN)
        );
    }

    #[tokio::test]
    pub async fn test_cors_disabled() {
        // cors is on by default
        let (tx, _rx) = mpsc::channel(100);

        let options = HttpOptions {
            enable_cors: false,
            ..Default::default()
        };

        let app = make_test_app_custom(tx, options);
        let client = TestClient::new(app).await;

        let res = client.get("/health").send().await;

        assert_eq!(res.status(), StatusCode::OK);
        assert!(
            !res.headers()
                .contains_key(http::header::ACCESS_CONTROL_ALLOW_ORIGIN)
        );
    }

    #[test]
    fn test_http_options_default() {
        let default = HttpOptions::default();
        assert_eq!("127.0.0.1:4000".to_string(), default.addr);
        assert_eq!(Duration::from_secs(0), default.timeout)
    }

    #[tokio::test]
    async fn test_http_server_request_timeout() {
        common_telemetry::init_default_ut_logging();

        let (tx, _rx) = mpsc::channel(100);
        let app = make_test_app(tx);
        let client = TestClient::new(app).await;
        let res = client.get("/test/timeout").send().await;
        assert_eq!(res.status(), StatusCode::REQUEST_TIMEOUT);

        let now = Instant::now();
        let res = client
            .get("/test/timeout")
            .header(GREPTIME_DB_HEADER_TIMEOUT, "20ms")
            .send()
            .await;
        assert_eq!(res.status(), StatusCode::REQUEST_TIMEOUT);
        let elapsed = now.elapsed();
        assert!(elapsed > Duration::from_millis(15));

        tokio::time::timeout(
            Duration::from_millis(15),
            client
                .get("/test/timeout")
                .header(GREPTIME_DB_HEADER_TIMEOUT, "0s")
                .send(),
        )
        .await
        .unwrap_err();

        tokio::time::timeout(
            Duration::from_millis(15),
            client
                .get("/test/timeout")
                .header(
                    GREPTIME_DB_HEADER_TIMEOUT,
                    humantime::format_duration(Duration::default()).to_string(),
                )
                .send(),
        )
        .await
        .unwrap_err();
    }

    #[tokio::test]
    async fn test_schema_for_empty_response() {
        let column_schemas = vec![
            ColumnSchema::new("numbers", ConcreteDataType::uint32_datatype(), false),
            ColumnSchema::new("strings", ConcreteDataType::string_datatype(), true),
        ];
        let schema = Arc::new(Schema::new(column_schemas));

        let recordbatches = RecordBatches::try_new(schema.clone(), vec![]).unwrap();
        let outputs = vec![Ok(Output::new_with_record_batches(recordbatches))];

        let json_resp = GreptimedbV1Response::from_output(outputs).await;
        if let HttpResponse::GreptimedbV1(json_resp) = json_resp {
            let json_output = &json_resp.output[0];
            if let GreptimeQueryOutput::Records(r) = json_output {
                assert_eq!(r.num_rows(), 0);
                assert_eq!(r.num_cols(), 2);
                assert_eq!(r.schema.column_schemas[0].name, "numbers");
                assert_eq!(r.schema.column_schemas[0].data_type, "UInt32");
            } else {
                panic!("invalid output type");
            }
        } else {
            panic!("invalid format")
        }
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

        for format in [
            ResponseFormat::GreptimedbV1,
            ResponseFormat::InfluxdbV1,
            ResponseFormat::Csv(true, true),
            ResponseFormat::Table,
            ResponseFormat::Arrow,
            ResponseFormat::Json,
            ResponseFormat::Null,
        ] {
            let recordbatches =
                RecordBatches::try_new(schema.clone(), vec![recordbatch.clone()]).unwrap();
            let outputs = vec![Ok(Output::new_with_record_batches(recordbatches))];
            let json_resp = match format {
                ResponseFormat::Arrow => ArrowResponse::from_output(outputs, None).await,
                ResponseFormat::Csv(with_names, with_types) => {
                    CsvResponse::from_output(outputs, with_names, with_types).await
                }
                ResponseFormat::Table => TableResponse::from_output(outputs).await,
                ResponseFormat::GreptimedbV1 => GreptimedbV1Response::from_output(outputs).await,
                ResponseFormat::InfluxdbV1 => InfluxdbV1Response::from_output(outputs, None).await,
                ResponseFormat::Json => JsonResponse::from_output(outputs).await,
                ResponseFormat::Null => NullResponse::from_output(outputs).await,
            };

            match json_resp {
                HttpResponse::GreptimedbV1(resp) => {
                    let json_output = &resp.output[0];
                    if let GreptimeQueryOutput::Records(r) = json_output {
                        assert_eq!(r.num_rows(), 4);
                        assert_eq!(r.num_cols(), 2);
                        assert_eq!(r.schema.column_schemas[0].name, "numbers");
                        assert_eq!(r.schema.column_schemas[0].data_type, "UInt32");
                        assert_eq!(r.rows[0][0], serde_json::Value::from(1));
                        assert_eq!(r.rows[0][1], serde_json::Value::Null);
                    } else {
                        panic!("invalid output type");
                    }
                }
                HttpResponse::InfluxdbV1(resp) => {
                    let json_output = &resp.results()[0];
                    assert_eq!(json_output.num_rows(), 4);
                    assert_eq!(json_output.num_cols(), 2);
                    assert_eq!(json_output.series[0].columns.clone()[0], "numbers");
                    assert_eq!(
                        json_output.series[0].values[0][0],
                        serde_json::Value::from(1)
                    );
                    assert_eq!(json_output.series[0].values[0][1], serde_json::Value::Null);
                }
                HttpResponse::Csv(resp) => {
                    let output = &resp.output()[0];
                    if let GreptimeQueryOutput::Records(r) = output {
                        assert_eq!(r.num_rows(), 4);
                        assert_eq!(r.num_cols(), 2);
                        assert_eq!(r.schema.column_schemas[0].name, "numbers");
                        assert_eq!(r.schema.column_schemas[0].data_type, "UInt32");
                        assert_eq!(r.rows[0][0], serde_json::Value::from(1));
                        assert_eq!(r.rows[0][1], serde_json::Value::Null);
                    } else {
                        panic!("invalid output type");
                    }
                }

                HttpResponse::Table(resp) => {
                    let output = &resp.output()[0];
                    if let GreptimeQueryOutput::Records(r) = output {
                        assert_eq!(r.num_rows(), 4);
                        assert_eq!(r.num_cols(), 2);
                        assert_eq!(r.schema.column_schemas[0].name, "numbers");
                        assert_eq!(r.schema.column_schemas[0].data_type, "UInt32");
                        assert_eq!(r.rows[0][0], serde_json::Value::from(1));
                        assert_eq!(r.rows[0][1], serde_json::Value::Null);
                    } else {
                        panic!("invalid output type");
                    }
                }

                HttpResponse::Arrow(resp) => {
                    let output = resp.data;
                    let mut reader =
                        FileReader::try_new(Cursor::new(output), None).expect("Arrow reader error");
                    let schema = reader.schema();
                    assert_eq!(schema.fields[0].name(), "numbers");
                    assert_eq!(schema.fields[0].data_type(), &DataType::UInt32);
                    assert_eq!(schema.fields[1].name(), "strings");
                    assert_eq!(schema.fields[1].data_type(), &DataType::Utf8);

                    let rb = reader.next().unwrap().expect("read record batch failed");
                    assert_eq!(rb.num_columns(), 2);
                    assert_eq!(rb.num_rows(), 4);
                }

                HttpResponse::Json(resp) => {
                    let output = &resp.output()[0];
                    if let GreptimeQueryOutput::Records(r) = output {
                        assert_eq!(r.num_rows(), 4);
                        assert_eq!(r.num_cols(), 2);
                        assert_eq!(r.schema.column_schemas[0].name, "numbers");
                        assert_eq!(r.schema.column_schemas[0].data_type, "UInt32");
                        assert_eq!(r.rows[0][0], serde_json::Value::from(1));
                        assert_eq!(r.rows[0][1], serde_json::Value::Null);
                    } else {
                        panic!("invalid output type");
                    }
                }

                HttpResponse::Null(resp) => {
                    assert_eq!(resp.rows(), 4);
                }

                HttpResponse::Error(err) => unreachable!("{err:?}"),
            }
        }
    }

    #[test]
    fn test_response_format_misc() {
        assert_eq!(ResponseFormat::default(), ResponseFormat::GreptimedbV1);
        assert_eq!(ResponseFormat::parse("arrow"), Some(ResponseFormat::Arrow));
        assert_eq!(
            ResponseFormat::parse("csv"),
            Some(ResponseFormat::Csv(false, false))
        );
        assert_eq!(
            ResponseFormat::parse("csvwithnames"),
            Some(ResponseFormat::Csv(true, false))
        );
        assert_eq!(
            ResponseFormat::parse("csvwithnamesandtypes"),
            Some(ResponseFormat::Csv(true, true))
        );
        assert_eq!(ResponseFormat::parse("table"), Some(ResponseFormat::Table));
        assert_eq!(
            ResponseFormat::parse("greptimedb_v1"),
            Some(ResponseFormat::GreptimedbV1)
        );
        assert_eq!(
            ResponseFormat::parse("influxdb_v1"),
            Some(ResponseFormat::InfluxdbV1)
        );
        assert_eq!(ResponseFormat::parse("json"), Some(ResponseFormat::Json));
        assert_eq!(ResponseFormat::parse("null"), Some(ResponseFormat::Null));

        // invalid formats
        assert_eq!(ResponseFormat::parse("invalid"), None);
        assert_eq!(ResponseFormat::parse(""), None);
        assert_eq!(ResponseFormat::parse("CSV"), None); // Case sensitive

        // as str
        assert_eq!(ResponseFormat::Arrow.as_str(), "arrow");
        assert_eq!(ResponseFormat::Csv(false, false).as_str(), "csv");
        assert_eq!(ResponseFormat::Csv(true, true).as_str(), "csv");
        assert_eq!(ResponseFormat::Table.as_str(), "table");
        assert_eq!(ResponseFormat::GreptimedbV1.as_str(), "greptimedb_v1");
        assert_eq!(ResponseFormat::InfluxdbV1.as_str(), "influxdb_v1");
        assert_eq!(ResponseFormat::Json.as_str(), "json");
        assert_eq!(ResponseFormat::Null.as_str(), "null");
        assert_eq!(ResponseFormat::default().as_str(), "greptimedb_v1");
    }
}
