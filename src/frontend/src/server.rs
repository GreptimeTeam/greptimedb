use std::net::SocketAddr;
use std::sync::Arc;

use common_runtime::Builder as RuntimeBuilder;
use servers::grpc::GrpcServer;
use servers::http::HttpServer;
use servers::mysql::server::MysqlServer;
use servers::server::Server;
use snafu::ResultExt;
use tokio::try_join;

use crate::error::{self, Result};
use crate::frontend::FrontendOptions;
use crate::instance::InstanceRef;

pub(crate) struct Services {
    instance: InstanceRef,
}

impl Services {
    pub(crate) fn new(instance: InstanceRef) -> Self {
        Self { instance }
    }

    pub(crate) async fn start(&mut self, opts: &FrontendOptions) -> Result<()> {
        let http_server_and_addr = if let Some(http_addr) = &opts.http_addr {
            let http_addr: SocketAddr = http_addr
                .parse()
                .context(error::ParseAddrSnafu { addr: http_addr })?;

            let http_server = HttpServer::new(self.instance.clone());

            Some((Box::new(http_server) as _, http_addr))
        } else {
            None
        };

        let grpc_server_and_addr = if let Some(grpc_addr) = &opts.grpc_addr {
            let grpc_addr: SocketAddr = grpc_addr
                .parse()
                .context(error::ParseAddrSnafu { addr: grpc_addr })?;

            let grpc_server = GrpcServer::new(self.instance.clone(), self.instance.clone());

            Some((Box::new(grpc_server) as _, grpc_addr))
        } else {
            None
        };

        let mysql_server_and_addr = if let Some(mysql_addr) = &opts.mysql_addr {
            let mysql_addr: SocketAddr = mysql_addr
                .parse()
                .context(error::ParseAddrSnafu { addr: mysql_addr })?;

            let mysql_io_runtime = Arc::new(
                RuntimeBuilder::default()
                    .worker_threads(opts.mysql_runtime_size as usize)
                    .thread_name("mysql-io-handlers")
                    .build()
                    .context(error::RuntimeResourceSnafu)?,
            );

            let mysql_server = MysqlServer::create_server(self.instance.clone(), mysql_io_runtime);

            Some((mysql_server, mysql_addr))
        } else {
            None
        };

        try_join!(
            start_server(http_server_and_addr),
            start_server(grpc_server_and_addr),
            start_server(mysql_server_and_addr)
        )
        .context(error::StartServerSnafu)?;
        Ok(())
    }
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
