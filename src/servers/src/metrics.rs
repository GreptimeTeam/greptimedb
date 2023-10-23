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
use tonic::body::BoxBody;
use tower::{Layer, Service};

pub(crate) const METRIC_DB_LABEL: &str = "db";
pub(crate) const METRIC_CODE_LABEL: &str = "code";
pub(crate) const METRIC_TYPE_LABEL: &str = "type";
pub(crate) const METRIC_PROTOCOL_LABEL: &str = "protocol";

pub(crate) const METRIC_ERROR_COUNTER: &str = "servers.error";
pub(crate) const METRIC_ERROR_COUNTER_LABEL_MYSQL: &str = "mysql";

pub(crate) const METRIC_HTTP_SQL_ELAPSED: &str = "servers.http_sql_elapsed";
pub(crate) const METRIC_HTTP_PROMQL_ELAPSED: &str = "servers.http_promql_elapsed";
pub(crate) const METRIC_AUTH_FAILURE: &str = "servers.auth_failure_count";
pub(crate) const METRIC_HTTP_INFLUXDB_WRITE_ELAPSED: &str = "servers.http_influxdb_write_elapsed";
pub(crate) const METRIC_HTTP_PROM_STORE_WRITE_ELAPSED: &str =
    "servers.http_prometheus_write_elapsed";
pub(crate) const METRIC_HTTP_PROM_STORE_READ_ELAPSED: &str = "servers.http_prometheus_read_elapsed";
pub(crate) const METRIC_HTTP_OPENTELEMETRY_METRICS_ELAPSED: &str =
    "servers.http_otlp_metrics_elapsed";
pub(crate) const METRIC_HTTP_OPENTELEMETRY_TRACES_ELAPSED: &str =
    "servers.http_otlp_traces_elapsed";
pub(crate) const METRIC_TCP_OPENTSDB_LINE_WRITE_ELAPSED: &str =
    "servers.opentsdb_line_write_elapsed";
pub(crate) const METRIC_HTTP_PROMQL_INSTANT_QUERY_ELAPSED: &str =
    "servers.http_promql_instant_query_elapsed";
pub(crate) const METRIC_HTTP_PROMQL_RANGE_QUERY_ELAPSED: &str =
    "servers.http_promql_range_query_elapsed";
pub(crate) const METRIC_HTTP_PROMQL_LABEL_QUERY_ELAPSED: &str =
    "servers.http_promql_label_query_elapsed";
pub(crate) const METRIC_HTTP_PROMQL_SERIES_QUERY_ELAPSED: &str =
    "servers.http_promql_series_query_elapsed";
pub(crate) const METRIC_HTTP_PROMQL_LABEL_VALUE_QUERY_ELAPSED: &str =
    "servers.http_promql_label_value_query_elapsed";

pub(crate) const METRIC_MYSQL_CONNECTIONS: &str = "servers.mysql_connection_count";
pub(crate) const METRIC_MYSQL_QUERY_TIMER: &str = "servers.mysql_query_elapsed";
pub(crate) const METRIC_MYSQL_SUBPROTOCOL_LABEL: &str = "subprotocol";
pub(crate) const METRIC_MYSQL_BINQUERY: &str = "binquery";
pub(crate) const METRIC_MYSQL_TEXTQUERY: &str = "textquery";
pub(crate) const METRIC_MYSQL_PREPARED_COUNT: &str = "servers.mysql_prepared_count";

pub(crate) const METRIC_POSTGRES_CONNECTIONS: &str = "servers.postgres_connection_count";
pub(crate) const METRIC_POSTGRES_QUERY_TIMER: &str = "servers.postgres_query_elapsed";
pub(crate) const METRIC_POSTGRES_SUBPROTOCOL_LABEL: &str = "subprotocol";
pub(crate) const METRIC_POSTGRES_SIMPLE_QUERY: &str = "simple";
pub(crate) const METRIC_POSTGRES_EXTENDED_QUERY: &str = "extended";
pub(crate) const METRIC_POSTGRES_PREPARED_COUNT: &str = "servers.postgres_prepared_count";

pub(crate) const METRIC_SERVER_GRPC_DB_REQUEST_TIMER: &str = "servers.grpc.db_request_elapsed";
pub(crate) const METRIC_SERVER_GRPC_PROM_REQUEST_TIMER: &str = "servers.grpc.prom_request_elapsed";

pub(crate) const METRIC_HTTP_REQUESTS_TOTAL: &str = "servers.http_requests_total";
pub(crate) const METRIC_HTTP_REQUESTS_ELAPSED: &str = "servers.http_requests_elapsed";
pub(crate) const METRIC_GRPC_REQUESTS_TOTAL: &str = "servers.grpc_requests_total";
pub(crate) const METRIC_GRPC_REQUESTS_ELAPSED: &str = "servers.grpc_requests_elapsed";
pub(crate) const METRIC_METHOD_LABEL: &str = "method";
pub(crate) const METRIC_PATH_LABEL: &str = "path";

/// Prometheus style process metrics collector.
#[cfg(feature = "metrics-process")]
pub(crate) static PROCESS_COLLECTOR: once_cell::sync::Lazy<metrics_process::Collector> =
    once_cell::sync::Lazy::new(|| {
        let collector = metrics_process::Collector::default();
        collector.describe();
        collector
    });

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

            let labels = [(METRIC_PATH_LABEL, path), (METRIC_CODE_LABEL, status)];
            metrics::increment_counter!(METRIC_GRPC_REQUESTS_TOTAL, &labels);
            metrics::histogram!(METRIC_GRPC_REQUESTS_ELAPSED, latency, &labels);

            Ok(response)
        })
    }
}
