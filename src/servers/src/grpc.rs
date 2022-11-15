// Copyright 2022 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

pub mod handler;

use std::net::SocketAddr;
use std::sync::Arc;

use api::v1::{greptime_server, BatchRequest, BatchResponse};
use async_trait::async_trait;
use common_runtime::Runtime;
use common_telemetry::logging::info;
use futures::FutureExt;
use snafu::{ensure, ResultExt};
use tokio::net::TcpListener;
use tokio::sync::oneshot::{self, Sender};
use tokio::sync::Mutex;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{Request, Response, Status};

use crate::error::{self, AlreadyStartedSnafu, Result, StartGrpcSnafu, TcpBindSnafu};
use crate::grpc::handler::BatchHandler;
use crate::query_handler::{GrpcAdminHandlerRef, GrpcQueryHandlerRef};
use crate::server::Server;

pub struct GrpcServer {
    query_handler: GrpcQueryHandlerRef,
    admin_handler: GrpcAdminHandlerRef,
    shutdown_tx: Mutex<Option<Sender<()>>>,
    runtime: Arc<Runtime>,
}

impl GrpcServer {
    pub fn new(
        query_handler: GrpcQueryHandlerRef,
        admin_handler: GrpcAdminHandlerRef,
        runtime: Arc<Runtime>,
    ) -> Self {
        Self {
            query_handler,
            admin_handler,
            shutdown_tx: Mutex::new(None),
            runtime,
        }
    }

    pub fn create_service(&self) -> greptime_server::GreptimeServer<GrpcService> {
        let service = GrpcService {
            handler: BatchHandler::new(
                self.query_handler.clone(),
                self.admin_handler.clone(),
                self.runtime.clone(),
            ),
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
            ensure!(
                shutdown_tx.is_none(),
                AlreadyStartedSnafu { server: "gRPC" }
            );

            let listener = TcpListener::bind(addr)
                .await
                .context(TcpBindSnafu { addr })?;
            let addr = listener.local_addr().context(TcpBindSnafu { addr })?;
            info!("GRPC server is bound to {}", addr);

            *shutdown_tx = Some(tx);

            (listener, addr)
        };

        let reflection_service = tonic_reflection::server::Builder::configure()
            .register_encoded_file_descriptor_set(api::v1::GREPTIME_FD_SET)
            .with_service_name("greptime.v1.Greptime")
            .build()
            .context(error::GrpcReflectionServiceSnafu)?;

        // Would block to serve requests.
        tonic::transport::Server::builder()
            .add_service(self.create_service())
            .add_service(reflection_service)
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), rx.map(drop))
            .await
            .context(StartGrpcSnafu)?;

        Ok(addr)
    }
}
