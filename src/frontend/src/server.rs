use std::net::SocketAddr;
use std::sync::Arc;

use common_runtime::Builder as RuntimeBuilder;
use servers::grpc::GrpcServer;
use servers::http::HttpServer;
use servers::mysql::server::MysqlServer;
#[cfg(feature = "postgres")]
use servers::postgres::PostgresServer;
use servers::server::Server;
use snafu::ResultExt;
use tokio::try_join;

use crate::error::{self, Result};
use crate::frontend::FrontendOptions;
use crate::instance::InstanceRef;

pub(crate) struct Services;

impl Services {
    pub(crate) async fn start(opts: &FrontendOptions, instance: InstanceRef) -> Result<()> {
        let http_server_and_addr = if let Some(http_addr) = &opts.http_addr {
            let http_addr = parse_addr(http_addr)?;

            let http_server = HttpServer::new(instance.clone());

            Some((Box::new(http_server) as _, http_addr))
        } else {
            None
        };

        let grpc_server_and_addr = if let Some(grpc_addr) = &opts.grpc_addr {
            let grpc_addr = parse_addr(grpc_addr)?;

            let grpc_server = GrpcServer::new(instance.clone(), instance.clone());

            Some((Box::new(grpc_server) as _, grpc_addr))
        } else {
            None
        };

        let mysql_server_and_addr = if let Some(mysql_addr) = &opts.mysql_addr {
            let mysql_addr = parse_addr(mysql_addr)?;

            let mysql_io_runtime = Arc::new(
                RuntimeBuilder::default()
                    .worker_threads(opts.mysql_runtime_size as usize)
                    .thread_name("mysql-io-handlers")
                    .build()
                    .context(error::RuntimeResourceSnafu)?,
            );

            let mysql_server = MysqlServer::create_server(instance.clone(), mysql_io_runtime);

            Some((mysql_server, mysql_addr))
        } else {
            None
        };

        #[cfg(feature = "postgres")]
        let postgres_server_and_addr = if let Some(pg_addr) = &opts.postgres_addr {
            let pg_addr = parse_addr(pg_addr)?;

            let pg_io_runtime = Arc::new(
                RuntimeBuilder::default()
                    .worker_threads(opts.postgres_runtime_size as usize)
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

        try_join!(
            start_server(http_server_and_addr),
            start_server(grpc_server_and_addr),
            start_server(mysql_server_and_addr),
            #[cfg(feature = "postgres")]
            start_server(postgres_server_and_addr),
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
    if let Some((mut server, addr)) = server_and_addr {
        server.start(addr).await.map(Some)
    } else {
        Ok(None)
    }
}
