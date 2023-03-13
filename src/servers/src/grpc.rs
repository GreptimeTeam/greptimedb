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

pub mod flight;

use std::net::SocketAddr;
use std::sync::Arc;

use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use async_trait::async_trait;
use common_runtime::Runtime;
use common_telemetry::logging::info;
use futures::FutureExt;
use snafu::{ensure, ResultExt};
use tokio::net::TcpListener;
use tokio::sync::oneshot::{self, Sender};
use tokio::sync::Mutex;
use tokio_stream::wrappers::TcpListenerStream;

use crate::auth::UserProviderRef;
use crate::error::{AlreadyStartedSnafu, Result, StartGrpcSnafu, TcpBindSnafu};
use crate::grpc::flight::FlightHandler;
use crate::query_handler::grpc::ServerGrpcQueryHandlerRef;
use crate::server::Server;

pub struct GrpcServer {
    query_handler: ServerGrpcQueryHandlerRef,
    user_provider: Option<UserProviderRef>,
    shutdown_tx: Mutex<Option<Sender<()>>>,
    runtime: Arc<Runtime>,
}

impl GrpcServer {
    pub fn new(
        query_handler: ServerGrpcQueryHandlerRef,
        user_provider: Option<UserProviderRef>,
        runtime: Arc<Runtime>,
    ) -> Self {
        Self {
            query_handler,
            user_provider,
            shutdown_tx: Mutex::new(None),
            runtime,
        }
    }

    pub fn create_service(&self) -> FlightServiceServer<impl FlightService> {
        let service = FlightHandler::new(
            self.query_handler.clone(),
            self.user_provider.clone(),
            self.runtime.clone(),
        );
        FlightServiceServer::new(service)
    }
}

pub const GRPC_SERVER: &str = "GRPC_SERVER";

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
            info!("gRPC server is bound to {}", addr);

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

    fn name(&self) -> &str {
        GRPC_SERVER
    }
}
