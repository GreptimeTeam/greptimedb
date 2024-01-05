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

use hyper::Body;
use lazy_static::lazy_static;
use prometheus::{
    register_histogram, register_histogram_vec, register_int_counter, register_int_counter_vec,
    register_int_gauge, Histogram, HistogramVec, IntCounter, IntCounterVec, IntGauge,
};
use tonic::body::BoxBody;
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

lazy_static! {
    pub static ref METRIC_ERROR_COUNTER: IntCounterVec = register_int_counter_vec!(
        "greptime_servers_error",
        "servers error",
        &[METRIC_PROTOCOL_LABEL]
    )
    .unwrap();
    pub static ref METRIC_HTTP_SQL_ELAPSED: HistogramVec = register_histogram_vec!(
        "greptime_servers_http_sql_elapsed",
        "servers http sql elapsed",
        &[METRIC_DB_LABEL]
    )
    .unwrap();
    pub static ref METRIC_HTTP_PROMQL_ELAPSED: HistogramVec = register_histogram_vec!(
        "greptime_servers_http_promql_elapsed",
        "servers http promql elapsed",
        &[METRIC_DB_LABEL]
    )
    .unwrap();
    pub static ref METRIC_AUTH_FAILURE: IntCounterVec = register_int_counter_vec!(
        "greptime_servers_auth_failure_count",
        "servers auth failure count",
        &[METRIC_CODE_LABEL]
    )
    .unwrap();
    pub static ref METRIC_HTTP_INFLUXDB_WRITE_ELAPSED: HistogramVec = register_histogram_vec!(
        "greptime_servers_http_influxdb_write_elapsed",
        "servers http influxdb write elapsed",
        &[METRIC_DB_LABEL]
    )
    .unwrap();
    pub static ref METRIC_HTTP_PROM_STORE_WRITE_ELAPSED: HistogramVec = register_histogram_vec!(
        "greptime_servers_http_prometheus_write_elapsed",
        "servers http prometheus write elapsed",
        &[METRIC_DB_LABEL]
    )
    .unwrap();
    pub static ref METRIC_HTTP_PROM_STORE_READ_ELAPSED: HistogramVec = register_histogram_vec!(
        "greptime_servers_http_prometheus_read_elapsed",
        "servers http prometheus read elapsed",
        &[METRIC_DB_LABEL]
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
    pub static ref METRIC_TCP_OPENTSDB_LINE_WRITE_ELAPSED: Histogram = register_histogram!(
        "greptime_servers_opentsdb_line_write_elapsed",
        "servers opentsdb line write elapsed"
    )
    .unwrap();
    pub static ref METRIC_HTTP_PROMQL_FORMAT_QUERY_ELAPSED: Histogram = register_histogram!(
        "greptime_servers_http_promql_format_query_elapsed",
        "servers http promql format query elapsed"
    )
    .unwrap();
    pub static ref METRIC_HTTP_PROMQL_INSTANT_QUERY_ELAPSED: Histogram = register_histogram!(
        "greptime_servers_http_promql_instant_query_elapsed",
        "servers http promql instant query elapsed"
    )
    .unwrap();
    pub static ref METRIC_HTTP_PROMQL_RANGE_QUERY_ELAPSED: Histogram = register_histogram!(
        "greptime_servers_http_promql_range_query_elapsed",
        "servers http promql range query elapsed"
    )
    .unwrap();
    pub static ref METRIC_HTTP_PROMQL_LABEL_QUERY_ELAPSED: Histogram = register_histogram!(
        "greptime_servers_http_promql_label_query_elapsed",
        "servers http promql label query elapsed"
    )
    .unwrap();
    pub static ref METRIC_HTTP_PROMQL_SERIES_QUERY_ELAPSED: Histogram = register_histogram!(
        "greptime_servers_http_promql_series_query_elapsed",
        "servers http promql series query elapsed"
    )
    .unwrap();
    pub static ref METRIC_HTTP_PROMQL_LABEL_VALUE_QUERY_ELAPSED: Histogram = register_histogram!(
        "greptime_servers_http_promql_label_value_query_elapsed",
        "servers http promql label value query elapsed"
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
        &[METRIC_MYSQL_SUBPROTOCOL_LABEL, METRIC_DB_LABEL]
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
        &[METRIC_POSTGRES_SUBPROTOCOL_LABEL, METRIC_DB_LABEL]
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
        &[METRIC_DB_LABEL]
    )
    .unwrap();
    pub static ref METRIC_HTTP_REQUESTS_TOTAL: IntCounterVec = register_int_counter_vec!(
        "greptime_servers_http_requests_total",
        "servers http requests total",
        &[METRIC_METHOD_LABEL, METRIC_PATH_LABEL, METRIC_CODE_LABEL]
    )
    .unwrap();
    pub static ref METRIC_HTTP_REQUESTS_ELAPSED: HistogramVec = register_histogram_vec!(
        "greptime_servers_http_requests_elapsed",
        "servers http requests elapsed",
        &[METRIC_METHOD_LABEL, METRIC_PATH_LABEL, METRIC_CODE_LABEL]
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
        &[METRIC_PATH_LABEL, METRIC_CODE_LABEL]
    )
    .unwrap();
    pub static ref HTTP_TRACK_METRICS: HistogramVec = register_histogram_vec!(
        "greptime_http_track_metrics",
        "http track metrics",
        &["tag"]
    )
    .unwrap();
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

impl<S> Service<hyper::Request<Body>> for MetricsMiddleware<S>
where
    S: Service<hyper::Request<Body>, Response = hyper::Response<BoxBody>> + Clone + Send + 'static,
    S::Future: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = futures::future::BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: hyper::Request<Body>) -> Self::Future {
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
