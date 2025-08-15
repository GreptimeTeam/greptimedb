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

#[cfg(not(windows))]
pub(crate) mod jemalloc;

use std::task::{Context, Poll};
use std::time::Instant;

use axum::extract::{MatchedPath, Request};
use axum::middleware::Next;
use axum::response::IntoResponse;
use lazy_static::lazy_static;
use prometheus::{
    register_histogram, register_histogram_vec, register_int_counter, register_int_counter_vec,
    register_int_gauge, Histogram, HistogramVec, IntCounter, IntCounterVec, IntGauge,
};
use session::context::QueryContext;
use tonic::body::Body;
use tower::{Layer, Service};

pub(crate) const METRIC_DB_LABEL: &str = "db";
pub(crate) const METRIC_CODE_LABEL: &str = "code";
pub(crate) const METRIC_TYPE_LABEL: &str = "type";
pub(crate) const METRIC_PROTOCOL_LABEL: &str = "protocol";
pub(crate) const METRIC_ERROR_COUNTER_LABEL_MYSQL: &str = "mysql";
pub(crate) const METRIC_MYSQL_SUBPROTOCOL_LABEL: &str = "subprotocol";
pub(crate) const METRIC_MYSQL_BINQUERY: &str = "binquery";
pub(crate) const METRIC_MYSQL_TEXTQUERY: &str = "textquery";
pub(crate) const METRIC_POSTGRES_SUBPROTOCOL_LABEL: &str = "subprotocol";
pub(crate) const METRIC_POSTGRES_SIMPLE_QUERY: &str = "simple";
pub(crate) const METRIC_POSTGRES_EXTENDED_QUERY: &str = "extended";
pub(crate) const METRIC_METHOD_LABEL: &str = "method";
pub(crate) const METRIC_PATH_LABEL: &str = "path";
pub(crate) const METRIC_RESULT_LABEL: &str = "result";

pub(crate) const METRIC_SUCCESS_VALUE: &str = "success";
pub(crate) const METRIC_FAILURE_VALUE: &str = "failure";

lazy_static! {

    pub static ref HTTP_REQUEST_COUNTER: IntCounterVec = register_int_counter_vec!(
        "greptime_servers_http_request_counter",
        "servers http request counter",
        &[METRIC_METHOD_LABEL, METRIC_PATH_LABEL, METRIC_CODE_LABEL, METRIC_DB_LABEL]
    ).unwrap();

    pub static ref METRIC_ERROR_COUNTER: IntCounterVec = register_int_counter_vec!(
        "greptime_servers_error",
        "servers error",
        &[METRIC_PROTOCOL_LABEL]
    )
    .unwrap();
    /// Http SQL query duration per database.
    pub static ref METRIC_HTTP_SQL_ELAPSED: HistogramVec = register_histogram_vec!(
        "greptime_servers_http_sql_elapsed",
        "servers http sql elapsed",
        &[METRIC_DB_LABEL],
        vec![0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0, 60.0, 300.0]
    )
    .unwrap();
    /// Http pql query duration per database.
    pub static ref METRIC_HTTP_PROMQL_ELAPSED: HistogramVec = register_histogram_vec!(
        "greptime_servers_http_promql_elapsed",
        "servers http promql elapsed",
        &[METRIC_DB_LABEL],
        vec![0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0, 60.0, 300.0]
    )
    .unwrap();
    /// Http logs query duration per database.
    pub static ref METRIC_HTTP_LOGS_ELAPSED: HistogramVec = register_histogram_vec!(
        "greptime_servers_http_logs_elapsed",
        "servers http logs elapsed",
        &[METRIC_DB_LABEL],
        vec![0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0, 60.0, 300.0]
    )
    .unwrap();
    pub static ref METRIC_AUTH_FAILURE: IntCounterVec = register_int_counter_vec!(
        "greptime_servers_auth_failure_count",
        "servers auth failure count",
        &[METRIC_CODE_LABEL]
    )
    .unwrap();
    /// Http influxdb write duration per database.
    pub static ref METRIC_HTTP_INFLUXDB_WRITE_ELAPSED: HistogramVec = register_histogram_vec!(
        "greptime_servers_http_influxdb_write_elapsed",
        "servers http influxdb write elapsed",
        &[METRIC_DB_LABEL],
        vec![0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0, 60.0, 300.0]
    )
    .unwrap();
    /// Http prometheus write duration per database.
    pub static ref METRIC_HTTP_PROM_STORE_WRITE_ELAPSED: HistogramVec = register_histogram_vec!(
        "greptime_servers_http_prometheus_write_elapsed",
        "servers http prometheus write elapsed",
        &[METRIC_DB_LABEL],
        vec![0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0, 60.0, 300.0]
    )
    .unwrap();
    /// Prometheus remote write codec duration.
    pub static ref METRIC_HTTP_PROM_STORE_CODEC_ELAPSED: HistogramVec = register_histogram_vec!(
        "greptime_servers_http_prometheus_codec_elapsed",
        "servers http prometheus request codec duration",
        &["type"],
    )
    .unwrap();
    /// Decode duration of prometheus write request.
    pub static ref METRIC_HTTP_PROM_STORE_DECODE_ELAPSED: Histogram = METRIC_HTTP_PROM_STORE_CODEC_ELAPSED
        .with_label_values(&["decode"]);
    /// Duration to convert prometheus write request to gRPC request.
    pub static ref METRIC_HTTP_PROM_STORE_CONVERT_ELAPSED: Histogram = METRIC_HTTP_PROM_STORE_CODEC_ELAPSED
        .with_label_values(&["convert"]);
        /// The samples count of Prometheus remote write.
    pub static ref PROM_STORE_REMOTE_WRITE_SAMPLES: IntCounterVec = register_int_counter_vec!(
        "greptime_servers_prometheus_remote_write_samples",
        "frontend prometheus remote write samples",
        &[METRIC_DB_LABEL]
    )
    .unwrap();
    /// Http prometheus read duration per database.
    pub static ref METRIC_HTTP_PROM_STORE_READ_ELAPSED: HistogramVec = register_histogram_vec!(
        "greptime_servers_http_prometheus_read_elapsed",
        "servers http prometheus read elapsed",
        &[METRIC_DB_LABEL]
    )
    .unwrap();
    /// Http prometheus endpoint query duration per database.
    pub static ref METRIC_HTTP_PROMETHEUS_PROMQL_ELAPSED: HistogramVec = register_histogram_vec!(
        "greptime_servers_http_prometheus_promql_elapsed",
        "servers http prometheus promql elapsed",
        &[METRIC_DB_LABEL, METRIC_METHOD_LABEL]
    )
    .unwrap();
    pub static ref METRIC_HTTP_OPENTELEMETRY_METRICS_ELAPSED: HistogramVec =
        register_histogram_vec!(
            "greptime_servers_http_otlp_metrics_elapsed",
            "servers_http_otlp_metrics_elapsed",
            &[METRIC_DB_LABEL]
        )
        .unwrap();
    pub static ref METRIC_HTTP_OPENTELEMETRY_TRACES_ELAPSED: HistogramVec =
        register_histogram_vec!(
            "greptime_servers_http_otlp_traces_elapsed",
            "servers http otlp traces elapsed",
            &[METRIC_DB_LABEL]
        )
        .unwrap();
    pub static ref METRIC_HTTP_OPENTELEMETRY_LOGS_ELAPSED: HistogramVec =
    register_histogram_vec!(
        "greptime_servers_http_otlp_logs_elapsed",
        "servers http otlp logs elapsed",
        &[METRIC_DB_LABEL]
    )
    .unwrap();
    pub static ref METRIC_HTTP_LOGS_INGESTION_COUNTER: IntCounterVec = register_int_counter_vec!(
        "greptime_servers_http_logs_ingestion_counter",
        "servers http logs ingestion counter",
        &[METRIC_DB_LABEL]
    )
    .unwrap();
    pub static ref METRIC_HTTP_LOGS_INGESTION_ELAPSED: HistogramVec =
        register_histogram_vec!(
            "greptime_servers_http_logs_ingestion_elapsed",
            "servers http logs ingestion elapsed",
            &[METRIC_DB_LABEL, METRIC_RESULT_LABEL]
        )
        .unwrap();

    /// Count of logs ingested into Loki.
    pub static ref METRIC_LOKI_LOGS_INGESTION_COUNTER: IntCounterVec = register_int_counter_vec!(
        "greptime_servers_loki_logs_ingestion_counter",
        "servers loki logs ingestion counter",
        &[METRIC_DB_LABEL]
    )
    .unwrap();
    pub static ref METRIC_LOKI_LOGS_INGESTION_ELAPSED: HistogramVec =
        register_histogram_vec!(
            "greptime_servers_loki_logs_ingestion_elapsed",
            "servers loki logs ingestion elapsed",
            &[METRIC_DB_LABEL, METRIC_RESULT_LABEL]
        )
        .unwrap();
    pub static ref METRIC_ELASTICSEARCH_LOGS_INGESTION_ELAPSED: HistogramVec =
        register_histogram_vec!(
            "greptime_servers_elasticsearch_logs_ingestion_elapsed",
            "servers elasticsearch logs ingestion elapsed",
            &[METRIC_DB_LABEL]
        )
        .unwrap();

    /// Count of documents ingested into Elasticsearch logs.
    pub static ref METRIC_ELASTICSEARCH_LOGS_DOCS_COUNT: IntCounterVec = register_int_counter_vec!(
        "greptime_servers_elasticsearch_logs_docs_count",
        "servers elasticsearch ingest logs docs count",
        &[METRIC_DB_LABEL]
    )
    .unwrap();

    pub static ref METRIC_HTTP_LOGS_TRANSFORM_ELAPSED: HistogramVec =
        register_histogram_vec!(
            "greptime_servers_http_logs_transform_elapsed",
            "servers http logs transform elapsed",
            &[METRIC_DB_LABEL, METRIC_RESULT_LABEL]
        )
        .unwrap();
    pub static ref METRIC_MYSQL_CONNECTIONS: IntGauge = register_int_gauge!(
        "greptime_servers_mysql_connection_count",
        "servers mysql connection count"
    )
    .unwrap();
    pub static ref METRIC_MYSQL_QUERY_TIMER: HistogramVec = register_histogram_vec!(
        "greptime_servers_mysql_query_elapsed",
        "servers mysql query elapsed",
        &[METRIC_MYSQL_SUBPROTOCOL_LABEL, METRIC_DB_LABEL],
        vec![0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0, 60.0, 300.0]
    )
    .unwrap();
    pub static ref METRIC_MYSQL_PREPARED_COUNT: IntCounterVec = register_int_counter_vec!(
        "greptime_servers_mysql_prepared_count",
        "servers mysql prepared count",
        &[METRIC_DB_LABEL]
    )
    .unwrap();
    pub static ref METRIC_POSTGRES_CONNECTIONS: IntGauge = register_int_gauge!(
        "greptime_servers_postgres_connection_count",
        "servers postgres connection count"
    )
    .unwrap();
    pub static ref METRIC_POSTGRES_QUERY_TIMER: HistogramVec = register_histogram_vec!(
        "greptime_servers_postgres_query_elapsed",
        "servers postgres query elapsed",
        &[METRIC_POSTGRES_SUBPROTOCOL_LABEL, METRIC_DB_LABEL],
        vec![0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0, 60.0, 300.0]
    )
    .unwrap();
    pub static ref METRIC_POSTGRES_PREPARED_COUNT: IntCounter = register_int_counter!(
        "greptime_servers_postgres_prepared_count",
        "servers postgres prepared count"
    )
    .unwrap();
    pub static ref METRIC_SERVER_GRPC_DB_REQUEST_TIMER: HistogramVec = register_histogram_vec!(
        "greptime_servers_grpc_db_request_elapsed",
        "servers grpc db request elapsed",
        &[METRIC_DB_LABEL, METRIC_TYPE_LABEL, METRIC_CODE_LABEL]
    )
    .unwrap();
    pub static ref METRIC_SERVER_GRPC_PROM_REQUEST_TIMER: HistogramVec = register_histogram_vec!(
        "greptime_servers_grpc_prom_request_elapsed",
        "servers grpc prom request elapsed",
        &[METRIC_DB_LABEL],
        vec![0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0, 60.0, 300.0]
    )
    .unwrap();
    pub static ref METRIC_HTTP_REQUESTS_TOTAL: IntCounterVec = register_int_counter_vec!(
        "greptime_servers_http_requests_total",
        "servers http requests total",
        &[METRIC_METHOD_LABEL, METRIC_PATH_LABEL, METRIC_CODE_LABEL, METRIC_DB_LABEL]
    )
    .unwrap();
    pub static ref METRIC_HTTP_REQUESTS_ELAPSED: HistogramVec = register_histogram_vec!(
        "greptime_servers_http_requests_elapsed",
        "servers http requests elapsed",
        &[METRIC_METHOD_LABEL, METRIC_PATH_LABEL, METRIC_CODE_LABEL, METRIC_DB_LABEL],
        vec![0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0, 60.0, 300.0]
    )
    .unwrap();
    pub static ref METRIC_GRPC_REQUESTS_TOTAL: IntCounterVec = register_int_counter_vec!(
        "greptime_servers_grpc_requests_total",
        "servers grpc requests total",
        &[METRIC_PATH_LABEL, METRIC_CODE_LABEL]
    )
    .unwrap();
    pub static ref METRIC_GRPC_REQUESTS_ELAPSED: HistogramVec = register_histogram_vec!(
        "greptime_servers_grpc_requests_elapsed",
        "servers grpc requests elapsed",
        &[METRIC_PATH_LABEL, METRIC_CODE_LABEL],
        vec![0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0, 60.0, 300.0]
    )
    .unwrap();
    pub static ref METRIC_JAEGER_QUERY_ELAPSED: HistogramVec = register_histogram_vec!(
        "greptime_servers_jaeger_query_elapsed",
        "servers jaeger query elapsed",
        &[METRIC_DB_LABEL, METRIC_PATH_LABEL]
    ).unwrap();

    pub static ref GRPC_BULK_INSERT_ELAPSED: Histogram = register_histogram!(
        "greptime_servers_bulk_insert_elapsed",
        "servers handle bulk insert elapsed",
    ).unwrap();
}

// Based on https://github.com/hyperium/tonic/blob/master/examples/src/tower/server.rs
// See https://github.com/hyperium/tonic/issues/242
/// A metrics middleware.
#[derive(Debug, Clone, Default)]
pub(crate) struct MetricsMiddlewareLayer;

impl<S> Layer<S> for MetricsMiddlewareLayer {
    type Service = MetricsMiddleware<S>;

    fn layer(&self, service: S) -> Self::Service {
        MetricsMiddleware { inner: service }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct MetricsMiddleware<S> {
    inner: S,
}

impl<S> Service<http::Request<Body>> for MetricsMiddleware<S>
where
    S: Service<http::Request<Body>, Response = http::Response<Body>> + Clone + Send + 'static,
    S::Future: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = futures::future::BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: http::Request<Body>) -> Self::Future {
        // This is necessary because tonic internally uses `tower::buffer::Buffer`.
        // See https://github.com/tower-rs/tower/issues/547#issuecomment-767629149
        // for details on why this is necessary
        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);

        Box::pin(async move {
            let start = Instant::now();
            let path = req.uri().path().to_string();

            // Do extra async work here...
            let response = inner.call(req).await?;

            let latency = start.elapsed().as_secs_f64();
            let status = response.status().as_u16().to_string();

            let labels = [path.as_str(), status.as_str()];
            METRIC_GRPC_REQUESTS_TOTAL.with_label_values(&labels).inc();
            METRIC_GRPC_REQUESTS_ELAPSED
                .with_label_values(&labels)
                .observe(latency);

            Ok(response)
        })
    }
}

/// A middleware to record metrics for HTTP.
// Based on https://github.com/tokio-rs/axum/blob/axum-v0.6.16/examples/prometheus-metrics/src/main.rs
pub(crate) async fn http_metrics_layer(req: Request, next: Next) -> impl IntoResponse {
    let start = Instant::now();
    let path = if let Some(matched_path) = req.extensions().get::<MatchedPath>() {
        matched_path.as_str().to_string()
    } else {
        req.uri().path().to_string()
    };
    let method = req.method().clone();

    let db = req
        .extensions()
        .get::<QueryContext>()
        .map(|ctx| ctx.get_db_string())
        .unwrap_or_else(|| "unknown".to_string());

    let response = next.run(req).await;

    let latency = start.elapsed().as_secs_f64();
    let status = response.status();
    let status = status.as_str();
    let method_str = method.as_str();

    let labels = [method_str, &path, status, db.as_str()];
    METRIC_HTTP_REQUESTS_TOTAL.with_label_values(&labels).inc();
    METRIC_HTTP_REQUESTS_ELAPSED
        .with_label_values(&labels)
        .observe(latency);

    response
}
