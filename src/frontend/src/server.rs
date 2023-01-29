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

use common_runtime::Builder as RuntimeBuilder;
use common_telemetry::info;
use servers::auth::UserProviderRef;
use servers::error::Error::InternalIo;
use servers::grpc::GrpcServer;
use servers::http::HttpServer;
use servers::mysql::server::{MysqlServer, MysqlSpawnConfig, MysqlSpawnRef};
use servers::opentsdb::OpentsdbServer;
use servers::postgres::PostgresServer;
use servers::query_handler::grpc::ServerGrpcQueryHandlerAdaptor;
use servers::query_handler::sql::ServerSqlQueryHandlerAdaptor;
use servers::server::Server;
use snafu::ResultExt;
use tokio::try_join;

use crate::error::Error::StartServer;
use crate::error::{self, Result};
use crate::frontend::FrontendOptions;
use crate::influxdb::InfluxdbOptions;
use crate::instance::FrontendInstance;
use crate::prometheus::PrometheusOptions;
use crate::Plugins;

pub(crate) struct Services;

impl Services {
    pub(crate) async fn start<T>(
        opts: &FrontendOptions,
        instance: Arc<T>,
        plugins: Arc<Plugins>,
    ) -> Result<()>
    where
        T: FrontendInstance,
    {
        info!("Starting frontend servers");
        let user_provider = plugins.get::<UserProviderRef>().cloned();

        let grpc_server_and_addr = if let Some(opts) = &opts.grpc_options {
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
                grpc_runtime,
            );

            Some((Box::new(grpc_server) as _, grpc_addr))
        } else {
            None
        };

        let mysql_server_and_addr = if let Some(opts) = &opts.mysql_options {
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

            Some((mysql_server, mysql_addr))
        } else {
            None
        };

        let postgres_server_and_addr = if let Some(opts) = &opts.postgres_options {
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

            Some((pg_server, pg_addr))
        } else {
            None
        };

        let opentsdb_server_and_addr = if let Some(opts) = &opts.opentsdb_options {
            let addr = parse_addr(&opts.addr)?;

            let io_runtime = Arc::new(
                RuntimeBuilder::default()
                    .worker_threads(opts.runtime_size)
                    .thread_name("opentsdb-io-handlers")
                    .build()
                    .context(error::RuntimeResourceSnafu)?,
            );

            let server = OpentsdbServer::create_server(instance.clone(), io_runtime);

            Some((server, addr))
        } else {
            None
        };

        let http_server_and_addr = if let Some(http_options) = &opts.http_options {
            let http_addr = parse_addr(&http_options.addr)?;

            let mut http_server = HttpServer::new(
                ServerSqlQueryHandlerAdaptor::arc(instance.clone()),
                http_options.clone(),
            );
            if let Some(user_provider) = user_provider {
                http_server.set_user_provider(user_provider);
            }

            if opentsdb_server_and_addr.is_some() {
                http_server.set_opentsdb_handler(instance.clone());
            }
            if matches!(
                opts.influxdb_options,
                Some(InfluxdbOptions { enable: true })
            ) {
                http_server.set_influxdb_handler(instance.clone());
            }

            if matches!(
                opts.prometheus_options,
                Some(PrometheusOptions { enable: true })
            ) {
                http_server.set_prom_handler(instance.clone());
            }
            http_server.set_script_handler(instance.clone());

            Some((Box::new(http_server) as _, http_addr))
        } else {
            None
        };

        try_join!(
            start_server(http_server_and_addr),
            start_server(grpc_server_and_addr),
            start_server(mysql_server_and_addr),
            start_server(postgres_server_and_addr),
            start_server(opentsdb_server_and_addr)
        )
        .context(error::StartServerSnafu)?;
        Ok(())
    }
}

fn parse_addr(addr: &str) -> Result<SocketAddr> {
    addr.parse().context(error::ParseAddrSnafu { addr })
}

async fn start_server(
    server_and_addr: Option<(Box<dyn Server>, SocketAddr)>,
) -> servers::error::Result<Option<SocketAddr>> {
    if let Some((server, addr)) = server_and_addr {
        info!("Starting server at {}", addr);
        server.start(addr).await.map(Some)
    } else {
        Ok(None)
    }
}
