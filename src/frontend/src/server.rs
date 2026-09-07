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

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use auth::UserProviderRef;
use axum::extract::{Request, State};
use axum::middleware::Next;
use axum::response::IntoResponse;
use common_base::Plugins;
use common_config::Configurable;
use common_telemetry::{info, warn};
use meta_client::MetaClientOptions;
use servers::error::Error as ServerError;
use servers::grpc::builder::GrpcServerBuilder;
use servers::grpc::flight::FlightCraftRef;
use servers::grpc::frontend_grpc_handler::FrontendGrpcHandler;
use servers::grpc::greptime_handler::GreptimeRequestHandler;
use servers::grpc::{GrpcOptions, GrpcServer};
use servers::http::event::LogValidatorRef;
use servers::http::result::error_result::ErrorResponse;
use servers::http::utils::router::RouterConfigurator;
use servers::http::{HttpOptions, HttpServer, HttpServerBuilder};
use servers::interceptor::LogIngestInterceptorRef;
use servers::metrics_handler::MetricsHandler;
use servers::mysql::server::{MysqlServer, MysqlSpawnConfig, MysqlSpawnRef};
use servers::otel_arrow::OtelArrowServiceHandler;
use servers::pending_rows_batcher::{
    MetricRowBatcherRef, PendingRowsBatchMode, PendingRowsBatcher, PendingRowsBatcherOptions,
    pending_rows_batch_sync_enabled,
};
use servers::postgres::PostgresServer;
use servers::request_memory_limiter::ServerMemoryLimiter;
use servers::server::{Server, ServerHandlers};
use servers::tls::{ReloadableTlsServerConfig, maybe_watch_server_tls_config};
use snafu::ResultExt;
use tonic::Status;

use crate::error::{self, Result, StartServerSnafu, TomlFormatSnafu};
use crate::frontend::FrontendOptions;
use crate::instance::Instance;

pub struct Services<T>
where
    T: Into<FrontendOptions> + Configurable + Clone,
{
    opts: T,
    instance: Arc<Instance>,
    grpc_server_builder: Option<GrpcServerBuilder>,
    http_server_builder: Option<HttpServerBuilder>,
    plugins: Plugins,
    flight_handler: Option<FlightCraftRef>,
    internal_flight_handler: Option<FlightCraftRef>,
    metric_batching: ResolvedMetricBatching,
    pending_rows_batcher: Option<Arc<PendingRowsBatcher>>,
    #[cfg(any(test, feature = "testing"))]
    metric_batcher_wiring: MetricBatcherWiring,
    pub server_memory_limiter: ServerMemoryLimiter,
}

#[cfg(any(test, feature = "testing"))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MetricBatchConsumer {
    PrometheusHttp,
    OtlpHttp,
    ExternalOtelArrow,
    InternalOtelArrow,
}

#[cfg(any(test, feature = "testing"))]
type MetricBatcherWiring =
    Arc<std::sync::Mutex<Vec<(MetricBatchConsumer, Option<MetricRowBatcherRef>)>>>;

struct ResolvedMetricBatching {
    options: Option<PendingRowsBatcherOptions>,
    batch_mode: PendingRowsBatchMode,
    with_metric_engine: bool,
    prometheus_enabled: bool,
    otlp_enabled: bool,
    http_batch_consumer_enabled: bool,
}

/// Metric HTTP consumers registered outside OSS [`Services`] route assembly.
///
/// This input only describes route ownership. Runtime [`FrontendOptions`] remain
/// authoritative for Metric Engine, batching mode, tuning, and protocol behavior.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ExternalMetricHttpConsumers {
    /// An external Prometheus HTTP route consumes the shared metric batcher.
    pub prometheus: bool,
    /// An external OTLP HTTP route consumes the shared metric batcher.
    pub otlp: bool,
}

impl ResolvedMetricBatching {
    fn batcher_enabled(&self) -> bool {
        self.prometheus_enabled || self.otlp_enabled
    }
}

fn resolve_metric_batching(
    opts: &FrontendOptions,
    external_http_consumers: ExternalMetricHttpConsumers,
    batch_mode: PendingRowsBatchMode,
) -> ResolvedMetricBatching {
    let prom = &opts.prom_store;
    let options = prom.pending_rows_batcher_options();
    let valid_options = options.is_some();
    let prometheus_enabled = (prom.enable || external_http_consumers.prometheus)
        && prom.with_metric_engine
        && valid_options;
    let otlp_enabled = opts.otlp.enable_metrics_batching
        && prom.with_metric_engine
        && batch_mode.is_synchronous()
        && valid_options;
    let http_batch_consumer_enabled =
        prometheus_enabled || ((opts.otlp.enable || external_http_consumers.otlp) && otlp_enabled);

    ResolvedMetricBatching {
        options,
        batch_mode,
        with_metric_engine: prom.with_metric_engine,
        prometheus_enabled,
        otlp_enabled,
        http_batch_consumer_enabled,
    }
}

impl<T> Services<T>
where
    T: Into<FrontendOptions> + Configurable + Clone,
{
    pub fn new(opts: T, instance: Arc<Instance>, plugins: Plugins) -> Self {
        Self::new_with_external_metric_http_consumers(
            opts,
            ExternalMetricHttpConsumers::default(),
            instance,
            plugins,
        )
    }

    /// Creates services that share metric batching with externally owned HTTP routes.
    pub fn new_with_external_metric_http_consumers(
        opts: T,
        external_http_consumers: ExternalMetricHttpConsumers,
        instance: Arc<Instance>,
        plugins: Plugins,
    ) -> Self {
        let batch_mode = if pending_rows_batch_sync_enabled() {
            PendingRowsBatchMode::Synchronous
        } else {
            PendingRowsBatchMode::Asynchronous
        };
        let metric_batching =
            resolve_metric_batching(&opts.clone().into(), external_http_consumers, batch_mode);
        Self::new_with_metric_batching(opts, instance, plugins, metric_batching)
    }

    #[cfg(test)]
    fn new_with_synchronous_metric_batching(
        opts: T,
        instance: Arc<Instance>,
        plugins: Plugins,
    ) -> Self {
        let metric_batching = resolve_metric_batching(
            &opts.clone().into(),
            ExternalMetricHttpConsumers::default(),
            PendingRowsBatchMode::Synchronous,
        );
        Self::new_with_metric_batching(opts, instance, plugins, metric_batching)
    }

    fn new_with_metric_batching(
        opts: T,
        instance: Arc<Instance>,
        plugins: Plugins,
        metric_batching: ResolvedMetricBatching,
    ) -> Self {
        let feopts = opts.clone().into();
        let pending_rows_batcher = metric_batching
            .options
            .as_ref()
            .filter(|_| metric_batching.batcher_enabled())
            .map(|options| {
                PendingRowsBatcher::new(
                    instance.partition_manager().clone(),
                    instance.node_manager().clone(),
                    instance.catalog_manager().clone(),
                    instance.table_flownode_set_cache().clone(),
                    metric_batching.with_metric_engine,
                    instance.clone(),
                    options.clone(),
                    metric_batching.batch_mode,
                )
            });
        // Create server request memory limiter for all server protocols
        let server_memory_limiter = ServerMemoryLimiter::new(
            feopts.max_in_flight_write_bytes.as_bytes(),
            feopts.write_bytes_exhausted_policy,
        );

        Self {
            opts,
            instance,
            grpc_server_builder: None,
            http_server_builder: None,
            plugins,
            flight_handler: None,
            internal_flight_handler: None,
            metric_batching,
            pending_rows_batcher,
            #[cfg(any(test, feature = "testing"))]
            metric_batcher_wiring: Default::default(),
            server_memory_limiter,
        }
    }

    pub fn grpc_server_builder(
        &self,
        opts: &GrpcOptions,
        request_memory_limiter: ServerMemoryLimiter,
    ) -> Result<GrpcServerBuilder> {
        let builder = GrpcServerBuilder::new(opts.as_config(), common_runtime::global_runtime())
            .with_memory_limiter(request_memory_limiter)
            .with_tls_config(opts.tls.clone())
            .context(error::InvalidTlsConfigSnafu)?;
        Ok(builder)
    }

    pub fn http_server_builder(
        &self,
        opts: &FrontendOptions,
        request_memory_limiter: ServerMemoryLimiter,
    ) -> HttpServerBuilder {
        let mut builder =
            HttpServerBuilder::new(effective_http_options(&opts.http, &self.metric_batching))
                .with_memory_limiter(request_memory_limiter)
                .with_sql_handler(self.instance.clone());

        let validator = self.plugins.get::<LogValidatorRef>();
        let ingest_interceptor = self.plugins.get::<LogIngestInterceptorRef<ServerError>>();
        builder =
            builder.with_log_ingest_handler(self.instance.clone(), validator, ingest_interceptor);
        builder = builder.with_logs_handler(self.instance.clone());

        if let Some(user_provider) = self.plugins.get::<UserProviderRef>() {
            builder = builder.with_user_provider(user_provider);
        }

        if opts.opentsdb.enable {
            builder = builder.with_opentsdb_handler(self.instance.clone());
        }

        if opts.influxdb.enable {
            builder = builder.with_influxdb_handler(self.instance.clone());
        }

        if opts.prom_store.enable {
            let metric_row_batcher =
                self.metric_row_batcher(self.metric_batching.prometheus_enabled);
            #[cfg(any(test, feature = "testing"))]
            self.record_metric_batcher(MetricBatchConsumer::PrometheusHttp, &metric_row_batcher);
            builder = builder
                .with_prom_handler(
                    self.instance.clone(),
                    Some(self.instance.clone()),
                    opts.prom_store.with_metric_engine,
                    opts.prom_store.prom_validation_mode,
                    opts.prom_store
                        .experimental_enable_prometheus_native_histogram,
                    metric_row_batcher,
                )
                .with_prometheus_handler(self.instance.clone());
        }

        if opts.otlp.enable {
            let metric_row_batcher = self.metric_row_batcher(self.metric_batching.otlp_enabled);
            #[cfg(any(test, feature = "testing"))]
            self.record_metric_batcher(MetricBatchConsumer::OtlpHttp, &metric_row_batcher);
            builder = builder.with_otlp_handler(
                self.instance.clone(),
                opts.prom_store.with_metric_engine,
                opts.otlp.experimental_enable_exponential_histogram,
                metric_row_batcher,
            );
        }

        if opts.jaeger.enable {
            builder = builder.with_jaeger_handler(self.instance.clone());
        }

        builder = builder.with_dashboard_handler(self.instance.clone());

        if let Some(configurator) = self.plugins.get::<RouterConfigurator>() {
            info!("Adding extra router from plugins");
            builder = builder.with_extra_router(configurator.router());
        }

        builder.add_layer(axum::middleware::from_fn_with_state(
            self.instance.clone(),
            async move |State(state): State<Arc<Instance>>, request: Request, next: Next| {
                if state.is_suspended() {
                    return ErrorResponse::from_error(servers::error::SuspendedSnafu.build())
                        .into_response();
                }
                next.run(request).await
            },
        ))
    }

    pub fn with_grpc_server_builder(self, builder: GrpcServerBuilder) -> Self {
        Self {
            grpc_server_builder: Some(builder),
            ..self
        }
    }

    pub fn with_http_server_builder(self, builder: HttpServerBuilder) -> Self {
        Self {
            http_server_builder: Some(builder),
            ..self
        }
    }

    pub fn with_flight_handler(self, flight_handler: FlightCraftRef) -> Self {
        Self {
            flight_handler: Some(flight_handler),
            ..self
        }
    }

    pub fn with_internal_flight_handler(self, flight_handler: FlightCraftRef) -> Self {
        Self {
            internal_flight_handler: Some(flight_handler),
            ..self
        }
    }

    fn metric_row_batcher(&self, enabled: bool) -> Option<MetricRowBatcherRef> {
        if !enabled {
            return None;
        }

        self.pending_rows_batcher
            .as_ref()
            .map(|batcher| batcher.clone() as MetricRowBatcherRef)
    }

    /// Returns the shared batcher when Prometheus pending-row batching is enabled.
    pub fn prometheus_metric_row_batcher(&self) -> Option<MetricRowBatcherRef> {
        self.metric_row_batcher(self.metric_batching.prometheus_enabled)
    }

    /// Returns the shared batcher when OTLP metric batching is enabled.
    pub fn otlp_metric_row_batcher(&self) -> Option<MetricRowBatcherRef> {
        self.metric_row_batcher(self.metric_batching.otlp_enabled)
    }

    #[cfg(any(test, feature = "testing"))]
    fn record_metric_batcher(
        &self,
        consumer: MetricBatchConsumer,
        batcher: &Option<MetricRowBatcherRef>,
    ) {
        self.metric_batcher_wiring
            .lock()
            .unwrap()
            .push((consumer, batcher.clone()));
    }

    /// Reports whether the production HTTP assembly configured an OTLP metric batcher.
    #[cfg(feature = "testing")]
    pub fn otlp_http_metric_batcher_configured(&self) -> Option<bool> {
        self.metric_batcher_wiring
            .lock()
            .unwrap()
            .iter()
            .rev()
            .find(|(consumer, _)| *consumer == MetricBatchConsumer::OtlpHttp)
            .map(|(_, batcher)| batcher.is_some())
    }

    fn build_grpc_server(
        &mut self,
        grpc: &GrpcOptions,
        meta_client: &Option<MetaClientOptions>,
        name: Option<String>,
        external: bool,
        request_memory_limiter: ServerMemoryLimiter,
    ) -> Result<GrpcServer> {
        let builder = if let Some(builder) = self.grpc_server_builder.take() {
            builder
        } else {
            self.grpc_server_builder(grpc, request_memory_limiter)?
        };

        let user_provider = if external {
            self.plugins.get::<UserProviderRef>()
        } else {
            // skip authentication for internal grpc port
            None
        };

        // Determine whether it is Standalone or Distributed mode based on whether the meta client is configured.
        let runtime = if meta_client.is_none() {
            Some(builder.runtime().clone())
        } else {
            None
        };

        let greptime_request_handler = GreptimeRequestHandler::new(
            self.instance.clone(),
            user_provider.clone(),
            runtime,
            grpc.flight_compression,
        );

        let default_flight_handler = Arc::new(greptime_request_handler.clone()) as FlightCraftRef;
        let flight_handler = if external {
            self.flight_handler
                .clone()
                .unwrap_or(default_flight_handler)
        } else {
            self.internal_flight_handler
                .clone()
                .unwrap_or(default_flight_handler)
        };

        let metric_row_batcher = self.metric_row_batcher(self.metric_batching.otlp_enabled);
        #[cfg(any(test, feature = "testing"))]
        self.record_metric_batcher(
            if external {
                MetricBatchConsumer::ExternalOtelArrow
            } else {
                MetricBatchConsumer::InternalOtelArrow
            },
            &metric_row_batcher,
        );

        let grpc_server = builder
            .name(name)
            .database_handler(greptime_request_handler.clone())
            .prometheus_handler(self.instance.clone(), user_provider.clone())
            .otel_arrow_handler(OtelArrowServiceHandler::new(
                self.instance.clone(),
                user_provider.clone(),
                self.metric_batching.with_metric_engine,
                metric_row_batcher,
            ))
            .flight_handler(flight_handler)
            .add_layer(axum::middleware::from_fn_with_state(
                self.instance.clone(),
                async move |State(state): State<Arc<Instance>>, request: Request, next: Next| {
                    if state.is_suspended() {
                        let status = Status::from(servers::error::SuspendedSnafu.build());
                        return status.into_http();
                    }
                    next.run(request).await
                },
            ));

        let grpc_server = if !external {
            let frontend_grpc_handler =
                FrontendGrpcHandler::new(self.instance.process_manager().clone());
            grpc_server.frontend_grpc_handler(frontend_grpc_handler)
        } else {
            grpc_server
        }
        .build();

        Ok(grpc_server)
    }

    fn build_http_server(
        &mut self,
        opts: &FrontendOptions,
        toml: String,
        request_memory_limiter: ServerMemoryLimiter,
    ) -> Result<(HttpServer, Option<HttpServer>)> {
        let builder = if let Some(builder) = self.http_server_builder.take() {
            builder
        } else {
            self.http_server_builder(opts, request_memory_limiter)
        };

        // The API server is configured entirely under `[http]` (`enable_api_server`,
        // `api_server_host`, `api_server_port`) and shares every other `[http]`
        // option with the main server.
        let (internal, api) = builder
            .with_metrics_handler(MetricsHandler)
            .with_greptime_config_options(toml)
            .build_servers();
        Ok((internal, api))
    }

    pub fn build(mut self) -> Result<ServerHandlers> {
        let opts = self.opts.clone();
        let instance = self.instance.clone();

        let toml = opts.to_toml().context(TomlFormatSnafu)?;
        let opts: FrontendOptions = opts.into();

        let handlers = ServerHandlers::default();

        let user_provider = self.plugins.get::<UserProviderRef>();

        {
            // Always init GRPC server
            let grpc_addr = parse_addr(&opts.grpc.bind_addr)?;
            let grpc_server = self.build_grpc_server(
                &opts.grpc,
                &opts.meta_client,
                None,
                true,
                self.server_memory_limiter.clone(),
            )?;
            handlers.insert((Box::new(grpc_server), grpc_addr));
        }

        if let Some(internal_grpc) = &opts.internal_grpc {
            // Always init Internal GRPC server
            let grpc_addr = parse_addr(&internal_grpc.bind_addr)?;
            let grpc_server = self.build_grpc_server(
                internal_grpc,
                &opts.meta_client,
                Some("INTERNAL_GRPC_SERVER".to_string()),
                false,
                self.server_memory_limiter.clone(),
            )?;
            handlers.insert((Box::new(grpc_server), grpc_addr));
        }

        {
            // Always init the internal/full HTTP server (v1 + internal interfaces)
            // and, when enabled, the dedicated HTTP API server (v1 + dashboard only).
            let http_options = &opts.http;
            let http_addr = parse_addr(&http_options.addr)?;
            let (http_server, http_api_server) =
                self.build_http_server(&opts, toml, self.server_memory_limiter.clone())?;
            handlers.insert((Box::new(http_server), http_addr));

            if let Some(http_api_server) = http_api_server {
                let http_api_addr = parse_addr(&http_options.api_server_addr)?;
                info!("HTTP API server is enabled at {}", http_api_addr);
                handlers.insert((Box::new(http_api_server), http_api_addr));
            }
        }

        if opts.mysql.enable {
            // Init MySQL server
            let opts = &opts.mysql;
            let mysql_addr = parse_addr(&opts.addr)?;

            let tls_server_config = Arc::new(
                ReloadableTlsServerConfig::try_new(opts.tls.clone()).context(StartServerSnafu)?,
            );

            // will not watch if watch is disabled in tls option
            maybe_watch_server_tls_config(tls_server_config.clone()).context(StartServerSnafu)?;

            let mysql_server = MysqlServer::create_server(
                common_runtime::global_runtime(),
                Arc::new(MysqlSpawnRef::new(instance.clone(), user_provider.clone())),
                Arc::new(MysqlSpawnConfig::new(
                    opts.tls.should_force_tls(),
                    tls_server_config,
                    opts.keep_alive.as_secs(),
                    opts.reject_no_database.unwrap_or(false),
                    opts.prepared_stmt_cache_size,
                )),
                Some(instance.process_manager().clone()),
            );
            handlers.insert((mysql_server, mysql_addr));
        }

        if opts.postgres.enable {
            // Init PosgresSQL Server
            let opts = &opts.postgres;
            let pg_addr = parse_addr(&opts.addr)?;

            let tls_server_config = Arc::new(
                ReloadableTlsServerConfig::try_new(opts.tls.clone()).context(StartServerSnafu)?,
            );

            maybe_watch_server_tls_config(tls_server_config.clone()).context(StartServerSnafu)?;

            let pg_server = Box::new(PostgresServer::new(
                instance.clone(),
                opts.tls.should_force_tls(),
                tls_server_config,
                opts.keep_alive.as_secs(),
                common_runtime::global_runtime(),
                user_provider.clone(),
                Some(self.instance.process_manager().clone()),
            )) as Box<dyn Server>;

            handlers.insert((pg_server, pg_addr));
        }

        Ok(handlers)
    }
}

fn effective_http_options(
    configured_http: &HttpOptions,
    metric_batching: &ResolvedMetricBatching,
) -> HttpOptions {
    let mut http = configured_http.clone();
    let flush_interval = metric_batching
        .options
        .as_ref()
        .map(PendingRowsBatcherOptions::flush_interval)
        .unwrap_or_default();
    let fallback_timeout = flush_interval.saturating_add(Duration::from_secs(1));
    // In asynchronous batch mode submissions return right after enqueue and
    // no request waits for a pending-row flush, so the timeout must not be
    // raised either.
    if !metric_batching.batch_mode.is_synchronous()
        || !metric_batching.http_batch_consumer_enabled
        || http.timeout.is_zero()
        || http.timeout > fallback_timeout
    {
        return http;
    }

    let configured_timeout = http.timeout;
    http.timeout = fallback_timeout;
    warn!(
        ?configured_timeout,
        ?flush_interval,
        ?fallback_timeout,
        "HTTP request timeout is not longer than the pending-row timeout fallback; using the fallback"
    );
    http
}

fn parse_addr(addr: &str) -> Result<SocketAddr> {
    addr.parse().context(error::ParseAddrSnafu { addr })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use api::v1::HealthCheckRequest;
    use api::v1::health_check_client::HealthCheckClient;
    use api::v1::meta::Role;
    use arrow_flight::{FlightData, PutResult, Ticket};
    use async_trait::async_trait;
    use auth::{UserProviderRef, static_user_provider_from_option};
    use client::{Client, Database};
    use meta_client::client::MetaClientBuilder;
    use servers::grpc::GRPC_SERVER;
    use servers::grpc::flight::{FlightCraft, FlightCraftRef, TonicStream};
    use tonic::{Code, Request, Response, Status, Streaming};

    use super::*;
    use crate::instance::builder::FrontendBuilder;

    struct CountingFlightCraft {
        inner: FlightCraftRef,
        do_get_calls: AtomicUsize,
        do_put_calls: AtomicUsize,
    }

    #[async_trait]
    impl FlightCraft for CountingFlightCraft {
        async fn do_get(
            &self,
            request: Request<Ticket>,
        ) -> std::result::Result<Response<TonicStream<FlightData>>, Status> {
            self.do_get_calls.fetch_add(1, Ordering::SeqCst);
            self.inner.do_get(request).await
        }

        async fn do_put(
            &self,
            request: Request<Streaming<FlightData>>,
        ) -> std::result::Result<Response<TonicStream<PutResult>>, Status> {
            self.do_put_calls.fetch_add(1, Ordering::SeqCst);
            self.inner.do_put(request).await
        }
    }

    fn resolved_http_options(
        opts: &FrontendOptions,
        batch_mode: PendingRowsBatchMode,
    ) -> HttpOptions {
        let metric_batching =
            resolve_metric_batching(opts, ExternalMetricHttpConsumers::default(), batch_mode);
        effective_http_options(&opts.http, &metric_batching)
    }

    #[test]
    fn test_effective_http_timeout_for_pending_rows() {
        let cases = [
            ("disabled timeout", 0, 5000, true, true, 0),
            ("disabled prom store", 1000, 5000, false, true, 1000),
            ("disabled metric engine", 1000, 5000, true, false, 1000),
            ("disabled batching", 1000, 0, true, true, 1000),
            ("timeout below flush interval", 4000, 5000, true, true, 6000),
            (
                "timeout equals flush interval",
                5000,
                5000,
                true,
                true,
                6000,
            ),
            ("timeout below fallback", 5500, 5000, true, true, 6000),
            ("timeout equals fallback", 6000, 5000, true, true, 6000),
            ("timeout above fallback", 7000, 5000, true, true, 7000),
        ];

        for (name, timeout, flush_interval, enable, with_metric_engine, expected) in cases {
            let mut opts = FrontendOptions::default();
            opts.http.timeout = Duration::from_millis(timeout);
            opts.prom_store.pending_rows_flush_interval = Duration::from_millis(flush_interval);
            opts.prom_store.enable = enable;
            opts.prom_store.with_metric_engine = with_metric_engine;

            assert_eq!(
                Duration::from_millis(expected),
                resolved_http_options(&opts, PendingRowsBatchMode::Synchronous).timeout,
                "{name}"
            );
        }
    }

    #[test]
    fn test_effective_http_timeout_skips_fallback_in_async_batch_mode() {
        // With `PENDING_ROWS_BATCH_SYNC=false`, submissions return right after
        // enqueue and no request waits for a pending-row flush, so the
        // timeout must not be raised.
        let mut opts = FrontendOptions::default();
        opts.http.timeout = Duration::from_millis(1000);
        opts.prom_store.pending_rows_flush_interval = Duration::from_millis(5000);

        assert_eq!(
            Duration::from_millis(1000),
            resolved_http_options(&opts, PendingRowsBatchMode::Asynchronous).timeout,
        );
        assert_eq!(
            Duration::from_millis(6000),
            resolved_http_options(&opts, PendingRowsBatchMode::Synchronous).timeout,
        );
    }

    #[test]
    fn test_effective_http_timeout_skips_fallback_when_batcher_disabled() {
        // Invalid tuning cannot produce a resolved batcher, so no request can
        // wait for a pending-row flush and the timeout must not be raised.
        type KnobMutator = fn(&mut FrontendOptions);
        let cases: [(&str, KnobMutator); 4] = [
            ("zero max_batch_rows", |opts| {
                opts.prom_store.max_batch_rows = 0
            }),
            ("zero max_concurrent_flushes", |opts| {
                opts.prom_store.max_concurrent_flushes = 0
            }),
            ("zero worker_channel_capacity", |opts| {
                opts.prom_store.worker_channel_capacity = 0
            }),
            ("zero max_inflight_requests", |opts| {
                opts.prom_store.max_inflight_requests = 0
            }),
        ];

        for (name, disable_batcher) in cases {
            let mut opts = FrontendOptions::default();
            opts.http.timeout = Duration::from_millis(1000);
            opts.prom_store.pending_rows_flush_interval = Duration::from_millis(5000);
            disable_batcher(&mut opts);

            assert_eq!(
                Duration::from_millis(1000),
                resolved_http_options(&opts, PendingRowsBatchMode::Synchronous).timeout,
                "{name}"
            );
        }
    }

    #[test]
    fn test_metric_batching_enablement_matrix() {
        type OptionsMutator = fn(&mut FrontendOptions);
        let cases: [(&str, OptionsMutator, bool, bool, bool, PendingRowsBatchMode); 8] = [
            (
                "prometheus only",
                |_| {},
                true,
                false,
                true,
                PendingRowsBatchMode::Synchronous,
            ),
            (
                "otlp only with HTTP route disabled",
                |opts| {
                    opts.prom_store.enable = false;
                    opts.otlp.enable = false;
                    opts.otlp.enable_metrics_batching = true;
                },
                false,
                true,
                true,
                PendingRowsBatchMode::Synchronous,
            ),
            (
                "both consumers",
                |opts| opts.otlp.enable_metrics_batching = true,
                true,
                true,
                true,
                PendingRowsBatchMode::Synchronous,
            ),
            (
                "both routes disabled",
                |opts| {
                    opts.prom_store.enable = false;
                    opts.otlp.enable = false;
                },
                false,
                false,
                false,
                PendingRowsBatchMode::Synchronous,
            ),
            (
                "metric engine disabled",
                |opts| {
                    opts.prom_store.with_metric_engine = false;
                    opts.otlp.enable_metrics_batching = true;
                },
                false,
                false,
                false,
                PendingRowsBatchMode::Synchronous,
            ),
            (
                "asynchronous mode preserves prometheus only",
                |opts| opts.otlp.enable_metrics_batching = true,
                true,
                false,
                true,
                PendingRowsBatchMode::Asynchronous,
            ),
            (
                "prometheus disabled does not disable otlp",
                |opts| {
                    opts.prom_store.enable = false;
                    opts.otlp.enable_metrics_batching = true;
                },
                false,
                true,
                true,
                PendingRowsBatchMode::Synchronous,
            ),
            (
                "otlp batching disabled does not disable prometheus",
                |opts| opts.otlp.enable_metrics_batching = false,
                true,
                false,
                true,
                PendingRowsBatchMode::Synchronous,
            ),
        ];

        for (name, mutate, expected_prom, expected_otlp, expected_batcher, batch_mode) in cases {
            let mut opts = FrontendOptions::default();
            opts.prom_store.pending_rows_flush_interval = Duration::from_secs(5);
            mutate(&mut opts);

            let resolved =
                resolve_metric_batching(&opts, ExternalMetricHttpConsumers::default(), batch_mode);
            assert_eq!(expected_prom, resolved.prometheus_enabled, "{name}");
            assert_eq!(expected_otlp, resolved.otlp_enabled, "{name}");
            assert_eq!(batch_mode, resolved.batch_mode, "{name}");
            assert_eq!(expected_batcher, resolved.batcher_enabled(), "{name}");
            assert_eq!(
                Duration::from_secs(5),
                resolved.options.as_ref().unwrap().flush_interval(),
                "{name}"
            );
        }
    }

    #[test]
    fn test_metric_batching_snapshot_remains_authoritative_after_source_options_change() {
        let mut opts = FrontendOptions::default();
        opts.http.timeout = Duration::from_secs(1);
        opts.prom_store.pending_rows_flush_interval = Duration::from_secs(5);
        let resolved = resolve_metric_batching(
            &opts,
            ExternalMetricHttpConsumers::default(),
            PendingRowsBatchMode::Synchronous,
        );

        opts.prom_store.pending_rows_flush_interval = Duration::from_secs(30);
        opts.prom_store.max_batch_rows = 0;

        assert_eq!(
            Duration::from_secs(6),
            effective_http_options(&opts.http, &resolved).timeout
        );
        assert!(resolved.options.is_some());
        assert!(resolved.batcher_enabled());
    }

    #[test]
    fn test_external_http_consumers_cannot_override_runtime_batching_configuration() {
        type OptionsMutator = fn(&mut FrontendOptions);
        struct TestCase {
            name: &'static str,
            external_http_consumers: ExternalMetricHttpConsumers,
            batch_mode: PendingRowsBatchMode,
            mutate: OptionsMutator,
            expected_prometheus: bool,
            expected_otlp: bool,
            expected_flush_interval: Option<Duration>,
            expected_timeout: Duration,
        }

        let cases = [
            TestCase {
                name: "external Prometheus uses runtime tuning",
                external_http_consumers: ExternalMetricHttpConsumers {
                    prometheus: true,
                    otlp: false,
                },
                batch_mode: PendingRowsBatchMode::Synchronous,
                mutate: |_| {},
                expected_prometheus: true,
                expected_otlp: false,
                expected_flush_interval: Some(Duration::from_secs(5)),
                expected_timeout: Duration::from_secs(6),
            },
            TestCase {
                name: "external OTLP uses runtime tuning and batching flag",
                external_http_consumers: ExternalMetricHttpConsumers {
                    prometheus: false,
                    otlp: true,
                },
                batch_mode: PendingRowsBatchMode::Synchronous,
                mutate: |opts| opts.otlp.enable_metrics_batching = true,
                expected_prometheus: false,
                expected_otlp: true,
                expected_flush_interval: Some(Duration::from_secs(5)),
                expected_timeout: Duration::from_secs(6),
            },
            TestCase {
                name: "external OTLP cannot enable runtime-disabled batching",
                external_http_consumers: ExternalMetricHttpConsumers {
                    prometheus: false,
                    otlp: true,
                },
                batch_mode: PendingRowsBatchMode::Synchronous,
                mutate: |_| {},
                expected_prometheus: false,
                expected_otlp: false,
                expected_flush_interval: Some(Duration::from_secs(5)),
                expected_timeout: Duration::from_secs(1),
            },
            TestCase {
                name: "external OTLP cannot replace invalid runtime tuning",
                external_http_consumers: ExternalMetricHttpConsumers {
                    prometheus: false,
                    otlp: true,
                },
                batch_mode: PendingRowsBatchMode::Synchronous,
                mutate: |opts| {
                    opts.otlp.enable_metrics_batching = true;
                    opts.prom_store.max_batch_rows = 0;
                },
                expected_prometheus: false,
                expected_otlp: false,
                expected_flush_interval: None,
                expected_timeout: Duration::from_secs(1),
            },
            TestCase {
                name: "external OTLP cannot override asynchronous runtime mode",
                external_http_consumers: ExternalMetricHttpConsumers {
                    prometheus: false,
                    otlp: true,
                },
                batch_mode: PendingRowsBatchMode::Asynchronous,
                mutate: |opts| opts.otlp.enable_metrics_batching = true,
                expected_prometheus: false,
                expected_otlp: false,
                expected_flush_interval: Some(Duration::from_secs(5)),
                expected_timeout: Duration::from_secs(1),
            },
        ];

        for case in cases {
            let mut opts = FrontendOptions::default();
            opts.http.timeout = Duration::from_secs(1);
            opts.prom_store.enable = false;
            opts.prom_store.pending_rows_flush_interval = Duration::from_secs(5);
            opts.otlp.enable = false;
            (case.mutate)(&mut opts);

            let resolved =
                resolve_metric_batching(&opts, case.external_http_consumers, case.batch_mode);

            assert_eq!(
                case.expected_prometheus, resolved.prometheus_enabled,
                "{}",
                case.name
            );
            assert_eq!(case.expected_otlp, resolved.otlp_enabled, "{}", case.name);
            assert_eq!(case.batch_mode, resolved.batch_mode, "{}", case.name);
            assert_eq!(
                case.expected_flush_interval,
                resolved
                    .options
                    .as_ref()
                    .map(|options| options.flush_interval()),
                "{}",
                case.name
            );
            assert_eq!(
                case.expected_timeout,
                effective_http_options(&opts.http, &resolved).timeout,
                "{}",
                case.name
            );
        }
    }

    #[test]
    fn test_metric_batching_invalid_options_disable_all_consumers() {
        type KnobMutator = fn(&mut FrontendOptions);
        let cases: [(&str, KnobMutator); 5] = [
            ("zero flush interval", |opts| {
                opts.prom_store.pending_rows_flush_interval = Duration::ZERO
            }),
            ("zero max batch rows", |opts| {
                opts.prom_store.max_batch_rows = 0
            }),
            ("zero max concurrent flushes", |opts| {
                opts.prom_store.max_concurrent_flushes = 0
            }),
            ("zero worker channel capacity", |opts| {
                opts.prom_store.worker_channel_capacity = 0
            }),
            ("zero max inflight requests", |opts| {
                opts.prom_store.max_inflight_requests = 0
            }),
        ];

        for (name, disable_batcher) in cases {
            let mut opts = FrontendOptions::default();
            opts.prom_store.pending_rows_flush_interval = Duration::from_secs(5);
            opts.otlp.enable_metrics_batching = true;
            disable_batcher(&mut opts);

            let resolved = resolve_metric_batching(
                &opts,
                ExternalMetricHttpConsumers::default(),
                PendingRowsBatchMode::Synchronous,
            );
            assert!(!resolved.prometheus_enabled, "{name}");
            assert!(!resolved.otlp_enabled, "{name}");
            assert!(resolved.options.is_none(), "{name}");
        }
    }

    #[test]
    fn test_effective_http_timeout_for_enabled_sync_consumers() {
        let cases = [
            ("prometheus", true, false, true, true, 6000),
            ("otlp HTTP", false, true, true, true, 6000),
            ("otlp Arrow only", false, true, false, true, 1000),
            ("asynchronous prometheus", true, false, true, false, 1000),
            (
                "otlp fallback in asynchronous mode",
                false,
                true,
                true,
                false,
                1000,
            ),
        ];

        for (name, prom_enable, otlp_batching, otlp_http, batch_sync, expected) in cases {
            let mut opts = FrontendOptions::default();
            opts.http.timeout = Duration::from_millis(1000);
            opts.prom_store.enable = prom_enable;
            opts.prom_store.pending_rows_flush_interval = Duration::from_millis(5000);
            opts.otlp.enable = otlp_http;
            opts.otlp.enable_metrics_batching = otlp_batching;

            assert_eq!(
                Duration::from_millis(expected),
                resolved_http_options(
                    &opts,
                    if batch_sync {
                        PendingRowsBatchMode::Synchronous
                    } else {
                        PendingRowsBatchMode::Asynchronous
                    },
                )
                .timeout,
                "{name}"
            );
        }
    }

    #[test]
    fn test_enterprise_otlp_consumer_extends_timeout_with_oss_route_disabled() {
        let mut opts = FrontendOptions::default();
        opts.http.timeout = Duration::from_secs(1);
        opts.prom_store.enable = false;
        opts.prom_store.pending_rows_flush_interval = Duration::from_secs(5);
        opts.otlp.enable = false;
        opts.otlp.enable_metrics_batching = true;
        let resolved = resolve_metric_batching(
            &opts,
            ExternalMetricHttpConsumers {
                prometheus: false,
                otlp: true,
            },
            PendingRowsBatchMode::Synchronous,
        );

        assert_eq!(
            Duration::from_secs(6),
            effective_http_options(&opts.http, &resolved).timeout
        );
        assert!(!opts.otlp.enable);
        assert!(resolved.otlp_enabled);
        assert!(resolved.http_batch_consumer_enabled);
    }

    #[tokio::test]
    async fn test_database_sql_authentication_differs_between_public_and_internal_grpc() {
        let options = FrontendOptions {
            http: HttpOptions {
                addr: "127.0.0.1:0".to_string(),
                ..Default::default()
            },
            grpc: GrpcOptions::default().with_bind_addr("127.0.0.1:0"),
            internal_grpc: Some(GrpcOptions::default().with_bind_addr("127.0.0.1:0")),
            mysql: crate::service_config::MysqlOptions {
                enable: false,
                ..Default::default()
            },
            postgres: crate::service_config::PostgresOptions {
                enable: false,
                ..Default::default()
            },
            ..Default::default()
        };
        let meta_client = Arc::new(
            MetaClientBuilder::new(0, Role::Frontend)
                .enable_procedure()
                .build(),
        );
        let instance = Arc::new(
            FrontendBuilder::new_test(&options, meta_client)
                .try_build()
                .await
                .unwrap(),
        );
        let plugins = Plugins::new();
        let provider =
            static_user_provider_from_option("static_user_provider:cmd:greptime=greptime").unwrap();
        plugins.insert::<UserProviderRef>(Arc::new(provider));
        let public_flight_handler = Arc::new(GreptimeRequestHandler::new(
            instance.clone(),
            plugins.get::<UserProviderRef>(),
            None,
            options.grpc.flight_compression,
        )) as FlightCraftRef;
        let internal_flight_handler = Arc::new(CountingFlightCraft {
            inner: Arc::new(GreptimeRequestHandler::new(
                instance.clone(),
                None,
                None,
                options.grpc.flight_compression,
            )),
            do_get_calls: AtomicUsize::new(0),
            do_put_calls: AtomicUsize::new(0),
        });
        let internal_flight_handler_ref = internal_flight_handler.clone() as FlightCraftRef;
        let mut services = Services::new(options, instance, plugins)
            .with_flight_handler(public_flight_handler)
            .with_internal_flight_handler(internal_flight_handler_ref)
            .build()
            .unwrap();

        services.start_all().await.unwrap();
        let public_addr = services.addr(GRPC_SERVER).unwrap();
        let internal_addr = services.addr("INTERNAL_GRPC_SERVER").unwrap();
        let public_database = Database::new(
            "greptime",
            "public",
            Client::with_urls([public_addr.to_string()]),
        );
        let internal_database = Database::new(
            "greptime",
            "public",
            Client::with_urls([internal_addr.to_string()]),
        );

        let internal_result = internal_database.sql("SELECT 1").await;
        let put_result = internal_database
            .do_put(Box::pin(futures::stream::empty()))
            .await;
        let public_result = public_database.sql("SELECT 1").await;

        services.shutdown_all().await.unwrap();

        assert!(internal_result.is_ok());
        assert!(put_result.is_ok());
        assert_eq!(
            1,
            internal_flight_handler.do_get_calls.load(Ordering::SeqCst)
        );
        assert_eq!(
            1,
            internal_flight_handler.do_put_calls.load(Ordering::SeqCst)
        );
        assert_eq!(
            Some(Code::Unauthenticated),
            public_result
                .as_ref()
                .err()
                .and_then(|err| err.tonic_code())
        );
    }

    #[tokio::test]
    async fn test_services_builder_health_check_is_reachable() {
        // Arrange
        let options = FrontendOptions {
            http: HttpOptions {
                addr: "127.0.0.1:0".to_string(),
                ..Default::default()
            },
            grpc: GrpcOptions::default().with_bind_addr("127.0.0.1:0"),
            mysql: crate::service_config::MysqlOptions {
                enable: false,
                ..Default::default()
            },
            postgres: crate::service_config::PostgresOptions {
                enable: false,
                ..Default::default()
            },
            ..Default::default()
        };
        let meta_client = Arc::new(
            MetaClientBuilder::new(0, Role::Frontend)
                .enable_procedure()
                .build(),
        );
        let instance = Arc::new(
            FrontendBuilder::new_test(&options, meta_client)
                .try_build()
                .await
                .unwrap(),
        );
        let mut services = Services::new(options, instance, Default::default())
            .build()
            .unwrap();

        // Act
        services.start_all().await.unwrap();
        let addr = services.addr(GRPC_SERVER).unwrap();
        let health_check = HealthCheckClient::connect(format!("http://{addr}"))
            .await
            .unwrap()
            .health_check(HealthCheckRequest {})
            .await;
        services.shutdown_all().await.unwrap();

        // Assert
        assert!(health_check.is_ok());
    }

    #[tokio::test]
    async fn test_services_wires_same_metric_batcher_to_all_enabled_consumers() {
        let options = FrontendOptions {
            grpc: GrpcOptions::default().with_bind_addr("127.0.0.1:0"),
            internal_grpc: Some(GrpcOptions::default().with_bind_addr("127.0.0.1:0")),
            prom_store: crate::service_config::PromStoreOptions {
                pending_rows_flush_interval: Duration::from_secs(1),
                ..Default::default()
            },
            otlp: crate::service_config::OtlpOptions {
                enable_metrics_batching: true,
                ..Default::default()
            },
            ..Default::default()
        };
        let meta_client = Arc::new(
            MetaClientBuilder::new(0, Role::Frontend)
                .enable_procedure()
                .build(),
        );
        let instance = Arc::new(
            FrontendBuilder::new_test(&options, meta_client)
                .try_build()
                .await
                .unwrap(),
        );
        let services =
            Services::new_with_synchronous_metric_batching(options, instance, Default::default());
        let wiring = services.metric_batcher_wiring.clone();

        let _handlers = services.build().unwrap();

        let wiring = wiring.lock().unwrap();
        assert_eq!(4, wiring.len());
        let consumers = [
            MetricBatchConsumer::PrometheusHttp,
            MetricBatchConsumer::OtlpHttp,
            MetricBatchConsumer::ExternalOtelArrow,
            MetricBatchConsumer::InternalOtelArrow,
        ];
        let first = wiring
            .iter()
            .find(|(consumer, _)| *consumer == consumers[0])
            .and_then(|(_, batcher)| batcher.as_ref())
            .unwrap();
        for consumer in consumers {
            let batcher = wiring
                .iter()
                .find(|(wired_consumer, _)| *wired_consumer == consumer)
                .and_then(|(_, batcher)| batcher.as_ref())
                .unwrap();
            assert!(Arc::ptr_eq(first, batcher), "{consumer:?}");
        }
    }
}
