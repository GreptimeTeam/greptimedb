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

mod authorize;
pub mod builder;
mod database;
pub mod flight;
pub mod greptime_handler;
mod otlp;
pub mod prom_query_gateway;
pub mod region_server;

use std::net::SocketAddr;

use api::v1::health_check_server::{HealthCheck, HealthCheckServer};
use api::v1::{HealthCheckRequest, HealthCheckResponse};
use async_trait::async_trait;
use common_grpc::channel_manager::{
    DEFAULT_MAX_GRPC_RECV_MESSAGE_SIZE, DEFAULT_MAX_GRPC_SEND_MESSAGE_SIZE,
};
use common_telemetry::logging::info;
use common_telemetry::{error, warn};
use futures::FutureExt;
use snafu::{ensure, OptionExt, ResultExt};
use tokio::net::TcpListener;
use tokio::sync::oneshot::{self, Receiver, Sender};
use tokio::sync::Mutex;
use tonic::transport::server::{Routes, TcpIncoming};
use tonic::{Request, Response, Status};
use tonic_reflection::server::{ServerReflection, ServerReflectionServer};

use crate::error::{
    AlreadyStartedSnafu, InternalSnafu, Result, StartGrpcSnafu, TcpBindSnafu, TcpIncomingSnafu,
};
use crate::server::Server;

type TonicResult<T> = std::result::Result<T, Status>;

pub struct GrpcServer {
    // states
    shutdown_tx: Mutex<Option<Sender<()>>>,
    /// gRPC serving state receiver. Only present if the gRPC server is started.
    /// Used to wait for the server to stop, performing the old blocking fashion.
    serve_state: Mutex<Option<Receiver<Result<()>>>>,
    // handlers
    routes: Mutex<Option<Routes>>,
}

/// Grpc Server configuration
#[derive(Debug, Clone)]
pub struct GrpcServerConfig {
    // Max gRPC receiving(decoding) message size
    pub max_recv_message_size: usize,
    // Max gRPC sending(encoding) message size
    pub max_send_message_size: usize,
}

impl Default for GrpcServerConfig {
    fn default() -> Self {
        Self {
            max_recv_message_size: DEFAULT_MAX_GRPC_RECV_MESSAGE_SIZE.as_bytes() as usize,
            max_send_message_size: DEFAULT_MAX_GRPC_SEND_MESSAGE_SIZE.as_bytes() as usize,
        }
    }
}

impl GrpcServer {
    pub fn create_healthcheck_service(&self) -> HealthCheckServer<impl HealthCheck> {
        HealthCheckServer::new(HealthCheckHandler)
    }

    pub fn create_reflection_service(&self) -> ServerReflectionServer<impl ServerReflection> {
        tonic_reflection::server::Builder::configure()
            .register_encoded_file_descriptor_set(api::v1::GREPTIME_GRPC_DESC)
            .with_service_name("greptime.v1.GreptimeDatabase")
            .with_service_name("greptime.v1.HealthCheck")
            .with_service_name("greptime.v1.RegionServer")
            .build()
            .unwrap()
    }

    pub async fn wait_for_serve(&self) -> Result<()> {
        let mut serve_state = self.serve_state.lock().await;
        let rx = serve_state.take().context(InternalSnafu {
            err_msg: "gRPC serving state is unknown, maybe the server is not started, \
                      or we have already waited for the serve result before.",
        })?;
        let Ok(result) = rx.await else {
            warn!("Background gRPC serving task is quited before we can receive the serve result.");
            return Ok(());
        };
        if let Err(e) = result {
            error!(e; "GRPC serve error");
        }
        Ok(())
    }
}

pub struct HealthCheckHandler;

#[async_trait]
impl HealthCheck for HealthCheckHandler {
    async fn health_check(
        &self,
        _req: Request<HealthCheckRequest>,
    ) -> TonicResult<Response<HealthCheckResponse>> {
        Ok(Response::new(HealthCheckResponse {}))
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
        let routes = {
            let mut routes = self.routes.lock().await;
            let Some(routes) = routes.take() else {
                return AlreadyStartedSnafu {
                    server: self.name(),
                }
                .fail();
            };
            routes
        };

        let (tx, rx) = oneshot::channel();
        let (incoming, addr) = {
            let mut shutdown_tx = self.shutdown_tx.lock().await;
            ensure!(
                shutdown_tx.is_none(),
                AlreadyStartedSnafu { server: "gRPC" }
            );

            let listener = TcpListener::bind(addr)
                .await
                .context(TcpBindSnafu { addr })?;
            let addr = listener.local_addr().context(TcpBindSnafu { addr })?;
            let incoming =
                TcpIncoming::from_listener(listener, true, None).context(TcpIncomingSnafu)?;
            info!("gRPC server is bound to {}", addr);

            *shutdown_tx = Some(tx);

            (incoming, addr)
        };

        let builder = tonic::transport::Server::builder()
            .tls_config()
            .add_routes(routes)
            .add_service(self.create_healthcheck_service())
            .add_service(self.create_reflection_service());

        let (serve_state_tx, serve_state_rx) = oneshot::channel();
        let mut serve_state = self.serve_state.lock().await;
        *serve_state = Some(serve_state_rx);

        let _handle = common_runtime::spawn_bg(async move {
            let result = builder
                .serve_with_incoming_shutdown(incoming, rx.map(drop))
                .await
                .context(StartGrpcSnafu);
            serve_state_tx.send(result)
        });
        Ok(addr)
    }

    fn name(&self) -> &str {
        GRPC_SERVER
    }
}
