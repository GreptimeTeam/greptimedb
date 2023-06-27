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

use std::task::{Context, Poll};
use std::time::Instant;

use common_telemetry::error;
use hyper::Body;
use metrics::gauge;
use metrics_process::Collector;
use once_cell::sync::Lazy;
use snafu::ResultExt;
use tikv_jemalloc_ctl::stats::{allocated_mib, resident_mib};
use tikv_jemalloc_ctl::{epoch, epoch_mib, stats};
use tonic::body::BoxBody;
use tower::{Layer, Service};

use crate::error;
use crate::error::UpdateJemallocMetricsSnafu;

pub(crate) const METRIC_DB_LABEL: &str = "db";
pub(crate) const METRIC_CODE_LABEL: &str = "code";
pub(crate) const METRIC_TYPE_LABEL: &str = "type";

pub(crate) const METRIC_HTTP_SQL_ELAPSED: &str = "servers.http_sql_elapsed";
pub(crate) const METRIC_HTTP_PROMQL_ELAPSED: &str = "servers.http_promql_elapsed";
pub(crate) const METRIC_AUTH_FAILURE: &str = "servers.auth_failure_count";
pub(crate) const METRIC_HTTP_INFLUXDB_WRITE_ELAPSED: &str = "servers.http_influxdb_write_elapsed";
pub(crate) const METRIC_HTTP_PROMETHEUS_WRITE_ELAPSED: &str =
    "servers.http_prometheus_write_elapsed";
pub(crate) const METRIC_HTTP_PROMETHEUS_READ_ELAPSED: &str = "servers.http_prometheus_read_elapsed";
pub(crate) const METRIC_TCP_OPENTSDB_LINE_WRITE_ELAPSED: &str =
    "servers.opentsdb_line_write_elapsed";

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
pub(crate) const METRIC_STATUS_LABEL: &str = "status";
pub(crate) const METRIC_JEMALLOC_RESIDENT: &str = "sys.jemalloc.resident";
pub(crate) const METRIC_JEMALLOC_ALLOCATED: &str = "sys.jemalloc.allocated";

/// Prometheus style process metrics collector.
pub(crate) static PROCESS_COLLECTOR: Lazy<Collector> = Lazy::new(|| {
    let collector = Collector::default();
    // Describe collector.
    collector.describe();
    collector
});

pub(crate) static JEMALLOC_COLLECTOR: Lazy<Option<JemallocCollector>> = Lazy::new(|| {
    let collector = JemallocCollector::try_new()
        .map_err(|e| {
            error!(e; "Failed to retrieve jemalloc metrics");
            e
        })
        .ok();
    collector.map(|c| {
        if let Err(e) = c.update() {
            error!(e; "Failed to update jemalloc metrics");
        };
        c
    })
});

pub(crate) struct JemallocCollector {
    epoch: epoch_mib,
    allocated: allocated_mib,
    resident: resident_mib,
}

impl JemallocCollector {
    pub(crate) fn try_new() -> error::Result<Self> {
        let e = epoch::mib().context(UpdateJemallocMetricsSnafu)?;
        let allocated = stats::allocated::mib().context(UpdateJemallocMetricsSnafu)?;
        let resident = stats::resident::mib().context(UpdateJemallocMetricsSnafu)?;
        Ok(Self {
            epoch: e,
            allocated,
            resident,
        })
    }

    pub(crate) fn update(&self) -> error::Result<()> {
        let _ = self.epoch.advance().context(UpdateJemallocMetricsSnafu)?;
        let allocated = self.allocated.read().context(UpdateJemallocMetricsSnafu)?;
        let resident = self.resident.read().context(UpdateJemallocMetricsSnafu)?;
        gauge!(METRIC_JEMALLOC_ALLOCATED, allocated as f64);
        gauge!(METRIC_JEMALLOC_RESIDENT, resident as f64);
        Ok(())
    }
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

            let labels = [(METRIC_PATH_LABEL, path), (METRIC_STATUS_LABEL, status)];
            metrics::increment_counter!(METRIC_GRPC_REQUESTS_TOTAL, &labels);
            metrics::histogram!(METRIC_GRPC_REQUESTS_ELAPSED, latency, &labels);

            Ok(response)
        })
    }
}
