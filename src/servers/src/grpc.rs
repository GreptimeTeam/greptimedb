pub mod handler;

use std::net::SocketAddr;

use api::v1::{greptime_server, BatchRequest, BatchResponse};
use async_trait::async_trait;
use common_telemetry::logging::info;
use snafu::ResultExt;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{Request, Response, Status};

use crate::error::{Result, StartGrpcSnafu, TcpBindSnafu};
use crate::grpc::handler::BatchHandler;
use crate::query_handler::GrpcQueryHandlerRef;
use crate::server::Server;

pub struct GrpcServer {
    query_handler: GrpcQueryHandlerRef,
}

impl GrpcServer {
    pub fn new(query_handler: GrpcQueryHandlerRef) -> Self {
        Self { query_handler }
    }

    pub fn create_service(&self) -> greptime_server::GreptimeServer<GrpcService> {
        let service = GrpcService {
            handler: BatchHandler::new(self.query_handler.clone()),
        };
        greptime_server::GreptimeServer::new(service)
    }
}

pub struct GrpcService {
    handler: BatchHandler,
}

#[tonic::async_trait]
impl greptime_server::Greptime for GrpcService {
    async fn batch(
        &self,
        req: Request<BatchRequest>,
    ) -> std::result::Result<Response<BatchResponse>, Status> {
        let req = req.into_inner();
        let res = self.handler.batch(req).await?;
        Ok(Response::new(res))
    }
}

#[async_trait]
impl Server for GrpcServer {
    async fn shutdown(&mut self) -> Result<()> {
        // TODO(LFC): shutdown grpc server
        unimplemented!()
    }

    async fn start(&mut self, addr: SocketAddr) -> Result<SocketAddr> {
        let listener = TcpListener::bind(addr)
            .await
            .context(TcpBindSnafu { addr })?;
        let addr = listener.local_addr().context(TcpBindSnafu { addr })?;
        info!("GRPC server is bound to {}", addr);

        tonic::transport::Server::builder()
            .add_service(self.create_service())
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .context(StartGrpcSnafu)?;
        Ok(addr)
    }
}
