pub mod handler;

use std::net::SocketAddr;

use api::v1::{greptime_server, BatchRequest, BatchResponse};
use async_trait::async_trait;
use common_telemetry::logging::info;
use futures::FutureExt;
use snafu::ensure;
use snafu::ResultExt;
use tokio::net::TcpListener;
use tokio::sync::oneshot::{self, Sender};
use tokio::sync::Mutex;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{Request, Response, Status};

use crate::error::{AlreadyStartedSnafu, Result, StartGrpcSnafu, TcpBindSnafu};
use crate::grpc::handler::BatchHandler;
use crate::query_handler::{GrpcAdminHandlerRef, GrpcQueryHandlerRef};
use crate::server::Server;

pub struct GrpcServer {
    query_handler: GrpcQueryHandlerRef,
    admin_handler: GrpcAdminHandlerRef,
    shutdown_tx: Mutex<Option<Sender<()>>>,
}

impl GrpcServer {
    pub fn new(query_handler: GrpcQueryHandlerRef, admin_handler: GrpcAdminHandlerRef) -> Self {
        Self {
            query_handler,
            admin_handler,
            shutdown_tx: Mutex::new(None),
        }
    }

    pub fn create_service(&self) -> greptime_server::GreptimeServer<GrpcService> {
        let service = GrpcService {
            handler: BatchHandler::new(self.query_handler.clone(), self.admin_handler.clone()),
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
    async fn shutdown(&self) -> Result<()> {
        let mut shutdown_tx = self.shutdown_tx.lock().await;
        if let Some(tx) = shutdown_tx.take() {
            if tx.send(()).is_err() {
                info!("Receiver dropped, the grpc server has already existed");
            }
        }
        info!("Shutdown grpc server");

        Ok(())
    }

    async fn start(&self, addr: SocketAddr) -> Result<SocketAddr> {
        let (tx, rx) = oneshot::channel();
        let (listener, addr) = {
            let mut shutdown_tx = self.shutdown_tx.lock().await;
            ensure!(shutdown_tx.is_none(), AlreadyStartedSnafu);

            let listener = TcpListener::bind(addr)
                .await
                .context(TcpBindSnafu { addr })?;
            let addr = listener.local_addr().context(TcpBindSnafu { addr })?;
            info!("GRPC server is bound to {}", addr);

            *shutdown_tx = Some(tx);

            (listener, addr)
        };

        // Would block to serve requests.
        tonic::transport::Server::builder()
            .add_service(self.create_service())
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), rx.map(drop))
            .await
            .context(StartGrpcSnafu)?;

        Ok(addr)
    }
}
