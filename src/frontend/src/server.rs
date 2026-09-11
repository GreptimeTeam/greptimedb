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
use servers::batcher::logical_table::{
    LogicalTablePendingRowsBatcher, pending_rows_batch_sync_enabled,
};
use servers::error::Error as ServerError;
use servers::grpc::builder::GrpcServerBuilder;
use servers::grpc::flight::FlightCraftRef;
use servers::grpc::frontend_grpc_handler::FrontendGrpcHandler;
use servers::grpc::greptime_handler::GreptimeRequestHandler;
use servers::grpc::{GrpcOptions, GrpcServer};
use servers::http::event::LogValidatorRef;
use servers::http::result::error_result::ErrorResponse;
use servers::http::utils::router::RouterConfigurator;
use servers::http::{BatchingProtocol, HttpOptions, HttpServer, HttpServerBuilder};
use servers::interceptor::LogIngestInterceptorRef;
use servers::metrics_handler::MetricsHandler;
use servers::mysql::server::{MysqlServer, MysqlSpawnConfig, MysqlSpawnRef};
use servers::otel_arrow::OtelArrowServiceHandler;
use servers::postgres::PostgresServer;
use servers::request_memory_limiter::ServerMemoryLimiter;
use servers::server::{Server, ServerHandlers};
use servers::tls::{ReloadableTlsServerConfig, maybe_watch_server_tls_config};
use snafu::ResultExt;
use tonic::Status;

use crate::error::{self, Result, StartServerSnafu, TomlFormatSnafu};
use crate::frontend::FrontendOptions;
use crate::instance::Instance;
use crate::service_config::PromStoreOptions;

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
    pub server_memory_limiter: ServerMemoryLimiter,
}

impl<T> Services<T>
where
    T: Into<FrontendOptions> + Configurable + Clone,
{
    pub fn new(opts: T, instance: Arc<Instance>, plugins: Plugins) -> Self {
        let feopts = opts.clone().into();
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
        let mut builder = HttpServerBuilder::new(effective_http_options(opts))
            .with_batching_protocols(opts.experimental_pending_rows_batcher.protocols.clone())
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

        let prom_store = effective_prom_store_options(opts);
        if prom_store.enable {
            let pending_rows_batcher = if prom_store.with_metric_engine {
                LogicalTablePendingRowsBatcher::try_new(
                    self.instance.partition_manager().clone(),
                    self.instance.node_manager().clone(),
                    self.instance.catalog_manager().clone(),
                    self.instance.table_flownode_set_cache().clone(),
                    prom_store.with_metric_engine,
                    self.instance.clone(),
                    prom_store.pending_rows_flush_interval,
                    prom_store.max_batch_rows,
                    prom_store.max_concurrent_flushes,
                    prom_store.worker_channel_capacity,
                    prom_store.max_inflight_requests,
                    prom_store.flow_notification_queue_capacity,
                )
            } else {
                None
            };
            builder = builder
                .with_prom_handler(
                    self.instance.clone(),
                    Some(self.instance.clone()),
                    opts.prom_store.with_metric_engine,
                    opts.prom_store.prom_validation_mode,
                    opts.prom_store
                        .experimental_enable_prometheus_native_histogram,
                    pending_rows_batcher,
                )
                .with_prometheus_handler(self.instance.clone());
        }

        if opts.otlp.enable {
            builder = builder.with_otlp_handler(
                self.instance.clone(),
                opts.prom_store.with_metric_engine,
                opts.otlp.experimental_enable_exponential_histogram,
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

        let grpc_server = builder
            .name(name)
            .database_handler(greptime_request_handler.clone())
            .prometheus_handler(self.instance.clone(), user_provider.clone())
            .otel_arrow_handler(OtelArrowServiceHandler::new(
                self.instance.clone(),
                user_provider.clone(),
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

/// Selected shared controls override legacy Prom batching knobs, not protocol behavior.
fn effective_prom_store_options(opts: &FrontendOptions) -> PromStoreOptions {
    let mut prom_store = opts.prom_store.clone();
    let shared = &opts.experimental_pending_rows_batcher;
    if shared.protocols.contains(&BatchingProtocol::Prom) && shared.pending_rows_batching_enabled()
    {
        prom_store.pending_rows_flush_interval = shared.pending_rows_flush_interval;
        prom_store.max_batch_rows = shared.max_batch_rows;
        prom_store.max_concurrent_flushes = shared.max_concurrent_flushes;
        prom_store.worker_channel_capacity = shared.worker_channel_capacity;
        prom_store.max_inflight_requests = shared.max_inflight_requests;
        prom_store.flow_notification_queue_capacity = shared.flow_notification_queue_capacity;
    }
    prom_store
}

fn effective_http_options(opts: &FrontendOptions) -> HttpOptions {
    effective_http_options_with_sync(opts, pending_rows_batch_sync_enabled())
}

fn effective_http_options_with_sync(opts: &FrontendOptions, batch_sync: bool) -> HttpOptions {
    let mut http = opts.http.clone();
    let prom_store = effective_prom_store_options(opts);
    let shared = &opts.experimental_pending_rows_batcher;
    // Ordinary-table batching always waits for its flush, independently of the
    // dedicated Prom batcher's asynchronous acknowledgement mode.
    let common_enabled = shared.pending_rows_batching_enabled()
        && shared.protocols.iter().any(|protocol| {
            *protocol != BatchingProtocol::Prom
                || (prom_store.enable && !prom_store.with_metric_engine)
        });
    let common_interval = common_enabled.then_some(shared.pending_rows_flush_interval);
    let prom_interval = (prom_store.pending_rows_batching_enabled() && batch_sync)
        .then_some(prom_store.pending_rows_flush_interval);
    let Some(flush_interval) = common_interval.into_iter().chain(prom_interval).max() else {
        return http;
    };
    let fallback_timeout = flush_interval.saturating_add(Duration::from_secs(1));
    if http.timeout.is_zero() || http.timeout > fallback_timeout {
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
    use std::num::NonZeroUsize;
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

    use crate::instance::builder::FrontendBuilder;
    use crate::server::*;

    #[test]
    fn test_effective_prom_batching_controls() {
        // Only an enabled shared Prom selection replaces the legacy controls.
        for (selected, shared_enabled, metric_engine, prom_enabled) in [
            (true, true, true, true),
            (false, true, true, true),
            (true, false, true, true),
            (true, true, false, true),
            (true, true, true, false),
        ] {
            let mut opts = FrontendOptions::default();
            opts.http.timeout = Duration::from_millis(1);
            opts.prom_store.pending_rows_flush_interval = Duration::from_secs(2);
            opts.prom_store.with_metric_engine = metric_engine;
            opts.prom_store.enable = prom_enabled;
            opts.prom_store
                .experimental_enable_prometheus_native_histogram = true;
            let shared = &mut opts.experimental_pending_rows_batcher;
            shared.protocols = vec![if selected {
                BatchingProtocol::Prom
            } else {
                BatchingProtocol::Influxdb
            }];
            shared.pending_rows_flush_interval = if shared_enabled {
                Duration::from_secs(5)
            } else {
                Duration::ZERO
            };
            shared.max_batch_rows = 7;
            shared.max_concurrent_flushes = 3;
            shared.worker_channel_capacity = 11;
            shared.max_inflight_requests = 13;
            shared.flow_notification_queue_capacity = NonZeroUsize::new(17).unwrap();

            let mut expected = opts.prom_store.clone();
            if selected && shared_enabled {
                expected.pending_rows_flush_interval = shared.pending_rows_flush_interval;
                expected.max_batch_rows = shared.max_batch_rows;
                expected.max_concurrent_flushes = shared.max_concurrent_flushes;
                expected.worker_channel_capacity = shared.worker_channel_capacity;
                expected.max_inflight_requests = shared.max_inflight_requests;
                expected.flow_notification_queue_capacity = shared.flow_notification_queue_capacity;
            }
            let actual = effective_prom_store_options(&opts);
            assert_eq!(actual, expected);
            assert_eq!(
                actual.pending_rows_batching_enabled(),
                metric_engine && prom_enabled
            );
        }
    }

    #[test]
    fn test_http_timeout_covers_synchronous_batchers() {
        // Shared ordinary writes remain synchronous even when Prom is asynchronous.
        for (
            protocols,
            metric_engine,
            batch_sync,
            shared_secs,
            legacy_secs,
            timeout_secs,
            expected_secs,
        ) in [
            (vec![BatchingProtocol::Prom], false, false, 5, 2, 1, 6),
            (vec![BatchingProtocol::Prom], true, false, 5, 2, 1, 1),
            (vec![BatchingProtocol::Prom], true, true, 5, 2, 1, 6),
            (vec![BatchingProtocol::Influxdb], true, false, 5, 2, 1, 6),
            (vec![BatchingProtocol::Influxdb], true, true, 5, 8, 1, 9),
            (vec![BatchingProtocol::Influxdb], true, true, 8, 5, 1, 9),
            (vec![BatchingProtocol::Influxdb], true, false, 5, 2, 0, 0),
            (vec![BatchingProtocol::Influxdb], true, false, 5, 2, 10, 10),
            (vec![BatchingProtocol::Prom], false, false, 0, 2, 1, 1),
            (vec![], true, false, 5, 2, 1, 1),
            (vec![], true, true, 5, 2, 1, 3),
        ] {
            let mut opts = FrontendOptions::default();
            opts.http.timeout = Duration::from_secs(timeout_secs);
            opts.prom_store.with_metric_engine = metric_engine;
            opts.prom_store.pending_rows_flush_interval = Duration::from_secs(legacy_secs);
            opts.experimental_pending_rows_batcher.protocols = protocols;
            opts.experimental_pending_rows_batcher
                .pending_rows_flush_interval = Duration::from_secs(shared_secs);
            assert_eq!(
                effective_http_options_with_sync(&opts, batch_sync).timeout,
                Duration::from_secs(expected_secs)
            );
        }
    }

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
                effective_http_options_with_sync(&opts, true).timeout,
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
            effective_http_options_with_sync(&opts, false).timeout,
        );
        assert_eq!(
            Duration::from_millis(6000),
            effective_http_options_with_sync(&opts, true).timeout,
        );
    }

    #[test]
    fn test_effective_http_timeout_skips_fallback_when_batcher_disabled() {
        // Mirrors the conditions under which `LogicalTablePendingRowsBatcher::try_new`
        // returns `None`; in these cases no request can wait for a pending-row
        // flush, so the timeout must not be raised.
        type KnobMutator = fn(&mut FrontendOptions);
        let cases: [(&str, KnobMutator); 9] = [
            ("oversized flush concurrency", |opts| {
                opts.prom_store.max_concurrent_flushes = usize::MAX
            }),
            ("oversized worker channel", |opts| {
                opts.prom_store.worker_channel_capacity = usize::MAX
            }),
            ("oversized inflight limit", |opts| {
                opts.prom_store.max_inflight_requests = usize::MAX
            }),
            ("oversized flow queue", |opts| {
                opts.prom_store.flow_notification_queue_capacity =
                    std::num::NonZeroUsize::new(usize::MAX).unwrap()
            }),
            ("unrepresentable deadline", |opts| {
                opts.prom_store.pending_rows_flush_interval = Duration::MAX
            }),
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
                effective_http_options_with_sync(&opts, true).timeout,
                "{name}"
            );
        }
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
}
