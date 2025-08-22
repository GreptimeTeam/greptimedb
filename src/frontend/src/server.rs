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

use auth::UserProviderRef;
use common_base::Plugins;
use common_config::Configurable;
use meta_client::MetaClientOptions;
use servers::error::Error as ServerError;
use servers::grpc::builder::GrpcServerBuilder;
use servers::grpc::frontend_grpc_handler::FrontendGrpcHandler;
use servers::grpc::greptime_handler::GreptimeRequestHandler;
use servers::grpc::{GrpcOptions, GrpcServer};
use servers::http::event::LogValidatorRef;
use servers::http::{HttpServer, HttpServerBuilder};
use servers::interceptor::LogIngestInterceptorRef;
use servers::metrics_handler::MetricsHandler;
use servers::mysql::server::{MysqlServer, MysqlSpawnConfig, MysqlSpawnRef};
use servers::otel_arrow::OtelArrowServiceHandler;
use servers::postgres::PostgresServer;
use servers::query_handler::grpc::ServerGrpcQueryHandlerAdapter;
use servers::query_handler::sql::ServerSqlQueryHandlerAdapter;
use servers::server::{Server, ServerHandlers};
use servers::tls::{maybe_watch_tls_config, ReloadableTlsServerConfig};
use snafu::ResultExt;

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
}

impl<T> Services<T>
where
    T: Into<FrontendOptions> + Configurable + Clone,
{
    pub fn new(opts: T, instance: Arc<Instance>, plugins: Plugins) -> Self {
        Self {
            opts,
            instance,
            grpc_server_builder: None,
            http_server_builder: None,
            plugins,
        }
    }

    pub fn grpc_server_builder(&self, opts: &GrpcOptions) -> Result<GrpcServerBuilder> {
        let builder = GrpcServerBuilder::new(opts.as_config(), common_runtime::global_runtime())
            .with_tls_config(opts.tls.clone())
            .context(error::InvalidTlsConfigSnafu)?;
        Ok(builder)
    }

    pub fn http_server_builder(&self, opts: &FrontendOptions) -> HttpServerBuilder {
        let mut builder = HttpServerBuilder::new(opts.http.clone())
            .with_sql_handler(ServerSqlQueryHandlerAdapter::arc(self.instance.clone()));

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
            builder = builder
                .with_prom_handler(
                    self.instance.clone(),
                    Some(self.instance.clone()),
                    opts.prom_store.with_metric_engine,
                    opts.http.prom_validation_mode,
                )
                .with_prometheus_handler(self.instance.clone());
        }

        if opts.otlp.enable {
            builder = builder
                .with_otlp_handler(self.instance.clone(), opts.prom_store.with_metric_engine);
        }

        if opts.jaeger.enable {
            builder = builder.with_jaeger_handler(self.instance.clone());
        }

        builder
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

    fn build_grpc_server(
        &mut self,
        grpc: &GrpcOptions,
        meta_client: &Option<MetaClientOptions>,
        name: Option<String>,
        external: bool,
    ) -> Result<GrpcServer> {
        let builder = if let Some(builder) = self.grpc_server_builder.take() {
            builder
        } else {
            self.grpc_server_builder(grpc)?
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
            ServerGrpcQueryHandlerAdapter::arc(self.instance.clone()),
            user_provider.clone(),
            runtime,
            grpc.flight_compression,
        );

        let grpc_server = builder
            .name(name)
            .database_handler(greptime_request_handler.clone())
            .prometheus_handler(self.instance.clone(), user_provider.clone())
            .otel_arrow_handler(OtelArrowServiceHandler::new(self.instance.clone()))
            .flight_handler(Arc::new(greptime_request_handler));

        let grpc_server = if external {
            let frontend_grpc_handler =
                FrontendGrpcHandler::new(self.instance.process_manager().clone());
            grpc_server.frontend_grpc_handler(frontend_grpc_handler)
        } else {
            grpc_server
        }
        .build();

        Ok(grpc_server)
    }

    fn build_http_server(&mut self, opts: &FrontendOptions, toml: String) -> Result<HttpServer> {
        let builder = if let Some(builder) = self.http_server_builder.take() {
            builder
        } else {
            self.http_server_builder(opts)
        };

        let http_server = builder
            .with_metrics_handler(MetricsHandler)
            .with_plugins(self.plugins.clone())
            .with_greptime_config_options(toml)
            .build();
        Ok(http_server)
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
            let grpc_server = self.build_grpc_server(&opts.grpc, &opts.meta_client, None, true)?;
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
            )?;
            handlers.insert((Box::new(grpc_server), grpc_addr));
        }

        {
            // Always init HTTP server
            let http_options = &opts.http;
            let http_addr = parse_addr(&http_options.addr)?;
            let http_server = self.build_http_server(&opts, toml)?;
            handlers.insert((Box::new(http_server), http_addr));
        }

        if opts.mysql.enable {
            // Init MySQL server
            let opts = &opts.mysql;
            let mysql_addr = parse_addr(&opts.addr)?;

            let tls_server_config = Arc::new(
                ReloadableTlsServerConfig::try_new(opts.tls.clone()).context(StartServerSnafu)?,
            );

            // will not watch if watch is disabled in tls option
            maybe_watch_tls_config(tls_server_config.clone()).context(StartServerSnafu)?;

            let mysql_server = MysqlServer::create_server(
                common_runtime::global_runtime(),
                Arc::new(MysqlSpawnRef::new(
                    ServerSqlQueryHandlerAdapter::arc(instance.clone()),
                    user_provider.clone(),
                )),
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

            maybe_watch_tls_config(tls_server_config.clone()).context(StartServerSnafu)?;

            let pg_server = Box::new(PostgresServer::new(
                ServerSqlQueryHandlerAdapter::arc(instance.clone()),
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

fn parse_addr(addr: &str) -> Result<SocketAddr> {
    addr.parse().context(error::ParseAddrSnafu { addr })
}
