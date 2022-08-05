mod handler;
pub mod insert;
mod select;
mod server;

use common_telemetry::logging::info;
use snafu::ResultExt;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;

use crate::{
    error::{Result, StartGrpcSnafu, TcpBindSnafu},
    instance::InstanceRef,
    server::grpc::{handler::BatchHandler, server::Server},
};

pub struct GrpcServer {
    handler: BatchHandler,
}

impl GrpcServer {
    pub fn new(instance: InstanceRef) -> Self {
        Self {
            handler: BatchHandler::new(instance),
        }
    }

    pub async fn start(&self, addr: &str) -> Result<()> {
        let listener = TcpListener::bind(addr)
            .await
            .context(TcpBindSnafu { addr })?;
        let addr = listener.local_addr().context(TcpBindSnafu { addr })?;
        info!("The gRPC server is running at {}", addr);

        let svc = Server::new(self.handler.clone()).into_service();
        let _ = tonic::transport::Server::builder()
            .add_service(svc)
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .context(StartGrpcSnafu)?;
        Ok(())
    }
}
