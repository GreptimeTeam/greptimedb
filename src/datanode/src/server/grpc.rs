use common_telemetry::logging::info;
use snafu::ResultExt;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;

use crate::{
    error::{Result, StartGrpcSnafu, TcpBindSnafu},
    instance::InstanceRef,
    server::grpc::{processors::BatchProcessor, server::Server},
};

mod processors;
mod server;

pub struct GrpcServer {
    processor: BatchProcessor,
}

impl GrpcServer {
    pub fn new(instance: InstanceRef) -> Self {
        Self {
            processor: BatchProcessor::new(instance),
        }
    }

    pub async fn start(&self, addr: &str) -> Result<()> {
        let listener = TcpListener::bind(addr)
            .await
            .context(TcpBindSnafu { addr })?;
        let addr = listener.local_addr().context(TcpBindSnafu { addr })?;
        info!("The gRPC server is running at {}", addr);

        let svc = Server::new(self.processor.clone()).into_service();
        let _ = tonic::transport::Server::builder()
            .add_service(svc)
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .context(StartGrpcSnafu)?;
        Ok(())
    }
}
