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
use servers::grpc::{GrpcServer, GrpcServerConfig};
use servers::http::HttpServerBuilder;
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

pub(crate) struct Services;

impl Services {
    pub(crate) async fn build<T, U>(
        opts: T,
        instance: Arc<U>,
        plugins: Plugins,
    ) -> Result<ServerHandlers>
    where
        T: Into<FrontendOptions> + TomlSerializable,
        U: FrontendInstance,
    {
        let toml = opts.to_toml()?;
        let opts: FrontendOptions = opts.into();
        let mut result = Vec::<ServerHandler>::with_capacity(plugins.len());
        let user_provider = plugins.get::<UserProviderRef>();

        {
            // Always init GRPC server
            let opts = &opts.grpc;
            let grpc_addr = parse_addr(&opts.addr)?;

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
            let grpc_server = GrpcServer::new(
                Some(grpc_config),
                Some(ServerGrpcQueryHandlerAdapter::arc(instance.clone())),
                Some(instance.clone()),
                None,
                None,
                user_provider.clone(),
                grpc_runtime,
            );

            result.push((Box::new(grpc_server), grpc_addr));
        }

        {
            // Always init HTTP server
            let http_options = &opts.http;
            let http_addr = parse_addr(&http_options.addr)?;

            let mut http_server_builder = HttpServerBuilder::new(http_options.clone());
            let _ = http_server_builder
                .with_sql_handler(ServerSqlQueryHandlerAdapter::arc(instance.clone()))
                .with_grpc_handler(ServerGrpcQueryHandlerAdapter::arc(instance.clone()));

            if let Some(user_provider) = user_provider.clone() {
                let _ = http_server_builder.with_user_provider(user_provider);
            }

            if opts.opentsdb.enable {
                let _ = http_server_builder.with_opentsdb_handler(instance.clone());
            }

            if opts.influxdb.enable {
                let _ = http_server_builder.with_influxdb_handler(instance.clone());
            }

            if opts.prom_store.enable {
                let _ = http_server_builder
                    .with_prom_handler(instance.clone())
                    .with_prometheus_handler(instance.clone());
            }

            if opts.otlp.enable {
                let _ = http_server_builder.with_otlp_handler(instance.clone());
            }

            let http_server = http_server_builder
                .with_metrics_handler(MetricsHandler)
                .with_script_handler(instance.clone())
                .with_plugins(plugins)
                .with_greptime_config_options(toml)
                .build();
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
