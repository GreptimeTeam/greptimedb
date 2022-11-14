use std::net::SocketAddr;
use std::sync::Arc;

use common_runtime::Builder as RuntimeBuilder;
use common_telemetry::info;
use servers::grpc::GrpcServer;
use servers::http::HttpServer;
use servers::mysql::server::MysqlServer;
use servers::opentsdb::OpentsdbServer;
use servers::postgres::PostgresServer;
use servers::server::Server;
use snafu::ResultExt;
use tokio::try_join;

use crate::error::{self, Result};
use crate::frontend::FrontendOptions;
use crate::influxdb::InfluxdbOptions;
use crate::instance::FrontendInstance;
use crate::prometheus::PrometheusOptions;

pub(crate) struct Services;

impl Services {
    pub(crate) async fn start<T>(opts: &FrontendOptions, instance: Arc<T>) -> Result<()>
    where
        T: FrontendInstance,
    {
        info!("Starting frontend servers");
        let grpc_server_and_addr = if let Some(opts) = &opts.grpc_options {
            let grpc_addr = parse_addr(&opts.addr)?;

            let grpc_runtime = Arc::new(
                RuntimeBuilder::default()
                    .worker_threads(opts.runtime_size)
                    .thread_name("grpc-handlers")
                    .build()
                    .context(error::RuntimeResourceSnafu)?,
            );

            let grpc_server = GrpcServer::new(instance.clone(), instance.clone(), grpc_runtime);

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

            let mysql_server = MysqlServer::create_server(instance.clone(), mysql_io_runtime);

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

            let pg_server =
                Box::new(PostgresServer::new(instance.clone(), pg_io_runtime)) as Box<dyn Server>;

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

        let http_server_and_addr = if let Some(http_addr) = &opts.http_addr {
            let http_addr = parse_addr(http_addr)?;

            let mut http_server = HttpServer::new(instance.clone());
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
