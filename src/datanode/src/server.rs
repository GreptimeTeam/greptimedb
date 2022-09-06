pub mod grpc;

use std::net::SocketAddr;
use std::sync::Arc;

use common_runtime::Builder as RuntimeBuilder;
use servers::grpc::GrpcServer;
use servers::http::HttpServer;
use servers::mysql::server::MysqlServer;
use servers::postgres::PostgresServer;
use servers::server::Server;
use snafu::ResultExt;
use tokio::try_join;

use crate::datanode::DatanodeOptions;
use crate::error::{self, Result};
use crate::instance::InstanceRef;

/// All rpc services.
pub struct Services {
    http_server: HttpServer,
    grpc_server: GrpcServer,
    mysql_server: Box<dyn Server>,
    postgres_server: Box<dyn Server>,
}

impl Services {
    pub fn try_new(instance: InstanceRef, opts: &DatanodeOptions) -> Result<Self> {
        let mysql_io_runtime = Arc::new(
            RuntimeBuilder::default()
                .worker_threads(opts.mysql_runtime_size as usize)
                .thread_name("mysql-io-handlers")
                .build()
                .context(error::RuntimeResourceSnafu)?,
        );
        let postgres_io_runtime = Arc::new(
            RuntimeBuilder::default()
                .worker_threads(opts.postgres_runtime_size as usize)
                .thread_name("postgres-io-handlers")
                .build()
                .context(error::RuntimeResourceSnafu)?,
        );
        Ok(Self {
            http_server: HttpServer::new(instance.clone()),
            grpc_server: GrpcServer::new(instance.clone(), instance.clone()),
            mysql_server: MysqlServer::create_server(instance.clone(), mysql_io_runtime),
            postgres_server: Box::new(PostgresServer::new(instance.clone(), postgres_io_runtime)),
        })
    }

    // TODO(LFC): make servers started on demand (not starting mysql if no needed, for example)
    pub async fn start(&mut self, opts: &DatanodeOptions) -> Result<()> {
        let http_addr = &opts.http_addr;
        let http_addr: SocketAddr = http_addr
            .parse()
            .context(error::ParseAddrSnafu { addr: http_addr })?;

        let grpc_addr = &opts.rpc_addr;
        let grpc_addr: SocketAddr = grpc_addr
            .parse()
            .context(error::ParseAddrSnafu { addr: grpc_addr })?;

        let mysql_addr = &opts.mysql_addr;
        let mysql_addr: SocketAddr = mysql_addr
            .parse()
            .context(error::ParseAddrSnafu { addr: mysql_addr })?;

        let postgres_addr = &opts.postgres_addr;
        let postgres_addr: SocketAddr = postgres_addr.parse().context(error::ParseAddrSnafu {
            addr: postgres_addr,
        })?;

        try_join!(
            self.http_server.start(http_addr),
            self.grpc_server.start(grpc_addr),
            self.mysql_server.start(mysql_addr),
            self.postgres_server.start(postgres_addr),
        )
        .context(error::StartServerSnafu)?;
        Ok(())
    }
}
