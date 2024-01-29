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
use common_runtime::Builder as RuntimeBuilder;
use servers::error::InternalIoSnafu;
use servers::grpc::builder::GrpcServerBuilder;
use servers::grpc::greptime_handler::GreptimeRequestHandler;
use servers::grpc::{GrpcServer, GrpcServerConfig};
use servers::http::{HttpServer, HttpServerBuilder};
use servers::metrics_handler::MetricsHandler;
use servers::mysql::server::{MysqlServer, MysqlSpawnConfig, MysqlSpawnRef};
use servers::opentsdb::OpentsdbServer;
use servers::postgres::PostgresServer;
use servers::query_handler::grpc::ServerGrpcQueryHandlerAdapter;
use servers::query_handler::sql::ServerSqlQueryHandlerAdapter;
use servers::server::{Server, ServerHandler, ServerHandlers};
use snafu::ResultExt;

use crate::error::{self, Result, StartServerSnafu};
use crate::frontend::{FrontendOptions, TomlSerializable};
use crate::instance::FrontendInstance;
use crate::service_config::GrpcOptions;

pub struct Services<T, U>
where
    T: Into<FrontendOptions> + TomlSerializable + Clone,
    U: FrontendInstance,
{
    opts: T,
    instance: Arc<U>,
    grpc_server_builder: Option<GrpcServerBuilder>,
    http_server_builder: Option<HttpServerBuilder>,
    plugins: Plugins,
}

impl<T, U> Services<T, U>
where
    T: Into<FrontendOptions> + TomlSerializable + Clone,
    U: FrontendInstance,
{
    pub fn new(opts: T, instance: Arc<U>, plugins: Plugins) -> Self {
        Self {
            opts,
            instance,
            grpc_server_builder: None,
            http_server_builder: None,
            plugins,
        }
    }

    pub fn grpc_server_builder(&self, opts: &GrpcOptions) -> Result<GrpcServerBuilder> {
        let grpc_runtime = Arc::new(
            RuntimeBuilder::default()
                .worker_threads(opts.runtime_size)
                .thread_name("grpc-handlers")
                .build()
                .context(error::RuntimeResourceSnafu)?,
        );

        let grpc_config = GrpcServerConfig {
            max_recv_message_size: opts.max_recv_message_size.as_bytes() as usize,
            max_send_message_size: opts.max_send_message_size.as_bytes() as usize,
        };

        Ok(GrpcServerBuilder::new(grpc_config, grpc_runtime))
    }

    pub fn http_server_builder(&self, opts: &FrontendOptions) -> HttpServerBuilder {
        let mut builder = HttpServerBuilder::new(opts.http.clone()).with_sql_handler(
            ServerSqlQueryHandlerAdapter::arc(self.instance.clone()),
            Some(self.instance.clone()),
        );

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
                .with_prom_handler(self.instance.clone(), opts.prom_store.with_metric_engine)
                .with_prometheus_handler(self.instance.clone());
        }

        if opts.otlp.enable {
            builder = builder.with_otlp_handler(self.instance.clone());
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

    fn build_grpc_server(&mut self, opts: &FrontendOptions) -> Result<GrpcServer> {
        let builder = if let Some(builder) = self.grpc_server_builder.take() {
            builder
        } else {
            self.grpc_server_builder(&opts.grpc)?
        };

        let user_provider = self.plugins.get::<UserProviderRef>();

        let greptime_request_handler = GreptimeRequestHandler::new(
            ServerGrpcQueryHandlerAdapter::arc(self.instance.clone()),
            user_provider.clone(),
            builder.runtime().clone(),
        );

        let grpc_server = builder
            .database_handler(greptime_request_handler.clone())
            .prometheus_handler(self.instance.clone(), user_provider.clone())
            .otlp_handler(self.instance.clone(), user_provider)
            .flight_handler(Arc::new(greptime_request_handler))
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

        let toml = opts.to_toml()?;
        let opts: FrontendOptions = opts.into();

        let mut result = Vec::<ServerHandler>::new();

        let user_provider = self.plugins.get::<UserProviderRef>();

        {
            // Always init GRPC server
            let grpc_addr = parse_addr(&opts.grpc.addr)?;
            let grpc_server = self.build_grpc_server(&opts)?;
            result.push((Box::new(grpc_server), grpc_addr));
        }

        {
            // Always init HTTP server
            let http_options = &opts.http;
            let http_addr = parse_addr(&http_options.addr)?;
            let http_server = self.build_http_server(&opts, toml)?;
            result.push((Box::new(http_server), http_addr));
        }

        if opts.mysql.enable {
            // Init MySQL server
            let opts = &opts.mysql;
            let mysql_addr = parse_addr(&opts.addr)?;

            let mysql_io_runtime = Arc::new(
                RuntimeBuilder::default()
                    .worker_threads(opts.runtime_size)
                    .thread_name("mysql-io-handlers")
                    .build()
                    .context(error::RuntimeResourceSnafu)?,
            );
            let mysql_server = MysqlServer::create_server(
                mysql_io_runtime,
                Arc::new(MysqlSpawnRef::new(
                    ServerSqlQueryHandlerAdapter::arc(instance.clone()),
                    user_provider.clone(),
                )),
                Arc::new(MysqlSpawnConfig::new(
                    opts.tls.should_force_tls(),
                    opts.tls
                        .setup()
                        .context(InternalIoSnafu)
                        .context(StartServerSnafu)?
                        .map(Arc::new),
                    opts.reject_no_database.unwrap_or(false),
                )),
            );
            result.push((mysql_server, mysql_addr));
        }

        if opts.postgres.enable {
            // Init PosgresSQL Server
            let opts = &opts.postgres;
            let pg_addr = parse_addr(&opts.addr)?;

            let pg_io_runtime = Arc::new(
                RuntimeBuilder::default()
                    .worker_threads(opts.runtime_size)
                    .thread_name("pg-io-handlers")
                    .build()
                    .context(error::RuntimeResourceSnafu)?,
            );

            let pg_server = Box::new(PostgresServer::new(
                ServerSqlQueryHandlerAdapter::arc(instance.clone()),
                opts.tls.clone(),
                pg_io_runtime,
                user_provider.clone(),
            )) as Box<dyn Server>;

            result.push((pg_server, pg_addr));
        }

        if opts.opentsdb.enable {
            // Init OpenTSDB server
            let opts = &opts.opentsdb;
            let addr = parse_addr(&opts.addr)?;

            let io_runtime = Arc::new(
                RuntimeBuilder::default()
                    .worker_threads(opts.runtime_size)
                    .thread_name("opentsdb-io-handlers")
                    .build()
                    .context(error::RuntimeResourceSnafu)?,
            );

            let server = OpentsdbServer::create_server(instance.clone(), io_runtime);

            result.push((server, addr));
        }

        Ok(result
            .into_iter()
            .map(|(server, addr)| (server.name().to_string(), (server, addr)))
            .collect())
    }
}

fn parse_addr(addr: &str) -> Result<SocketAddr> {
    addr.parse().context(error::ParseAddrSnafu { addr })
}
