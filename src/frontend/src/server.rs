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
use std::net::SocketAddr;
use std::sync::Arc;

use auth::UserProviderRef;
use common_base::Plugins;
use common_runtime::Builder as RuntimeBuilder;
use common_telemetry::info;
use servers::configurator::ConfiguratorRef;
use servers::error::Error::InternalIo;
use servers::grpc::GrpcServer;
use servers::http::HttpServerBuilder;
use servers::metrics_handler::MetricsHandler;
use servers::mysql::server::{MysqlServer, MysqlSpawnConfig, MysqlSpawnRef};
use servers::opentsdb::OpentsdbServer;
use servers::postgres::PostgresServer;
use servers::prometheus::PrometheusServer;
use servers::query_handler::grpc::ServerGrpcQueryHandlerAdaptor;
use servers::query_handler::sql::ServerSqlQueryHandlerAdaptor;
use servers::server::Server;
use snafu::ResultExt;

use crate::error::Error::StartServer;
use crate::error::{self, Result};
use crate::frontend::FrontendOptions;
use crate::instance::FrontendInstance;
use crate::service_config::{InfluxdbOptions, OtlpOptions, PromStoreOptions};

pub(crate) struct Services;

pub type ServerHandlers = HashMap<String, ServerHandler>;

pub type ServerHandler = (Box<dyn Server>, SocketAddr);

impl Services {
    pub(crate) async fn build<T>(
        opts: &FrontendOptions,
        instance: Arc<T>,
        plugins: Arc<Plugins>,
    ) -> Result<ServerHandlers>
    where
        T: FrontendInstance,
    {
        let mut result = Vec::<ServerHandler>::with_capacity(plugins.len());
        let user_provider = plugins.get::<UserProviderRef>();

        if let Some(opts) = &opts.grpc_options {
            let grpc_addr = parse_addr(&opts.addr)?;

            let grpc_runtime = Arc::new(
                RuntimeBuilder::default()
                    .worker_threads(opts.runtime_size)
                    .thread_name("grpc-handlers")
                    .build()
                    .context(error::RuntimeResourceSnafu)?,
            );

            let grpc_server = GrpcServer::new(
                ServerGrpcQueryHandlerAdaptor::arc(instance.clone()),
                Some(instance.clone()),
                user_provider.clone(),
                grpc_runtime,
            );

            result.push((Box::new(grpc_server), grpc_addr));
        };

        if let Some(opts) = &opts.mysql_options {
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
                    ServerSqlQueryHandlerAdaptor::arc(instance.clone()),
                    user_provider.clone(),
                )),
                Arc::new(MysqlSpawnConfig::new(
                    opts.tls.should_force_tls(),
                    opts.tls
                        .setup()
                        .map_err(|e| StartServer {
                            source: InternalIo { source: e },
                        })?
                        .map(Arc::new),
                    opts.reject_no_database.unwrap_or(false),
                )),
            );
            result.push((mysql_server, mysql_addr));
        }

        if let Some(opts) = &opts.postgres_options {
            let pg_addr = parse_addr(&opts.addr)?;

            let pg_io_runtime = Arc::new(
                RuntimeBuilder::default()
                    .worker_threads(opts.runtime_size)
                    .thread_name("pg-io-handlers")
                    .build()
                    .context(error::RuntimeResourceSnafu)?,
            );

            let pg_server = Box::new(PostgresServer::new(
                ServerSqlQueryHandlerAdaptor::arc(instance.clone()),
                opts.tls.clone(),
                pg_io_runtime,
                user_provider.clone(),
            )) as Box<dyn Server>;

            result.push((pg_server, pg_addr));
        }

        let mut set_opentsdb_handler = false;

        if let Some(opts) = &opts.opentsdb_options {
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
            set_opentsdb_handler = true;
        }

        if let Some(http_options) = &opts.http_options {
            let http_addr = parse_addr(&http_options.addr)?;

            let mut http_server_builder = HttpServerBuilder::new(http_options.clone());
            let _ = http_server_builder
                .with_sql_handler(ServerSqlQueryHandlerAdaptor::arc(instance.clone()))
                .with_grpc_handler(ServerGrpcQueryHandlerAdaptor::arc(instance.clone()));

            if let Some(user_provider) = user_provider.clone() {
                let _ = http_server_builder.with_user_provider(user_provider);
            }

            if set_opentsdb_handler {
                let _ = http_server_builder.with_opentsdb_handler(instance.clone());
            }
            if matches!(
                opts.influxdb_options,
                Some(InfluxdbOptions { enable: true })
            ) {
                let _ = http_server_builder.with_influxdb_handler(instance.clone());
            }

            if matches!(
                opts.prom_store_options,
                Some(PromStoreOptions { enable: true })
            ) {
                let _ = http_server_builder.with_prom_handler(instance.clone());
            }

            if matches!(opts.otlp_options, Some(OtlpOptions { enable: true })) {
                let _ = http_server_builder.with_otlp_handler(instance.clone());
            }

            let http_server = http_server_builder
                .with_metrics_handler(MetricsHandler)
                .with_script_handler(instance.clone())
                .with_configurator(plugins.get::<ConfiguratorRef>())
                .with_greptime_config_options(opts.to_toml_string())
                .build();
            result.push((Box::new(http_server), http_addr));
        }

        if let Some(prometheus_options) = &opts.prometheus_options {
            let prom_addr = parse_addr(&prometheus_options.addr)?;

            let mut prom_server = PrometheusServer::create_server(instance);
            if let Some(user_provider) = user_provider {
                prom_server.set_user_provider(user_provider);
            }

            result.push((prom_server, prom_addr));
        };

        Ok(result
            .into_iter()
            .map(|(server, addr)| (server.name().to_string(), (server, addr)))
            .collect())
    }
}

fn parse_addr(addr: &str) -> Result<SocketAddr> {
    addr.parse().context(error::ParseAddrSnafu { addr })
}

pub async fn start_server(
    server_and_addr: &(Box<dyn Server>, SocketAddr),
) -> servers::error::Result<Option<SocketAddr>> {
    let (server, addr) = server_and_addr;
    info!("Starting {} at {}", server.name(), addr);
    server.start(*addr).await.map(Some)
}
