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

mod database;
pub mod flight;
pub mod handler;
pub mod prom_query_gateway;

use std::net::SocketAddr;
use std::sync::Arc;

use api::v1::greptime_database_server::{GreptimeDatabase, GreptimeDatabaseServer};
use api::v1::health_check_server::{HealthCheck, HealthCheckServer};
use api::v1::prometheus_gateway_server::{PrometheusGateway, PrometheusGatewayServer};
use api::v1::{HealthCheckRequest, HealthCheckResponse};
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use async_trait::async_trait;
use auth::UserProviderRef;
use common_runtime::Runtime;
use common_telemetry::logging::info;
use common_telemetry::{error, warn};
use futures::FutureExt;
use snafu::{ensure, OptionExt, ResultExt};
use tokio::net::TcpListener;
use tokio::sync::oneshot::{self, Receiver, Sender};
use tokio::sync::Mutex;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{Request, Response, Status};

use self::prom_query_gateway::PrometheusGatewayService;
use crate::error::{
    AlreadyStartedSnafu, GrpcReflectionServiceSnafu, InternalSnafu, Result, StartGrpcSnafu,
    TcpBindSnafu,
};
use crate::grpc::database::DatabaseService;
use crate::grpc::flight::FlightHandler;
use crate::grpc::handler::GreptimeRequestHandler;
use crate::prometheus::PrometheusHandlerRef;
use crate::query_handler::grpc::ServerGrpcQueryHandlerRef;
use crate::server::Server;

type TonicResult<T> = std::result::Result<T, Status>;

pub struct GrpcServer {
    shutdown_tx: Mutex<Option<Sender<()>>>,
    request_handler: Arc<GreptimeRequestHandler>,
    user_provider: Option<UserProviderRef>,
    /// Handler for Prometheus-compatible PromQL queries. Only present for frontend server.
    prometheus_handler: Option<PrometheusHandlerRef>,

    /// gRPC serving state receiver. Only present if the gRPC server is started.
    /// Used to wait for the server to stop, performing the old blocking fashion.
    serve_state: Mutex<Option<Receiver<Result<()>>>>,
}

impl GrpcServer {
    pub fn new(
        query_handler: ServerGrpcQueryHandlerRef,
        prometheus_handler: Option<PrometheusHandlerRef>,
        user_provider: Option<UserProviderRef>,
        runtime: Arc<Runtime>,
    ) -> Self {
        let request_handler = Arc::new(GreptimeRequestHandler::new(
            query_handler,
            user_provider.clone(),
            runtime,
        ));
        Self {
            shutdown_tx: Mutex::new(None),
            request_handler,
            user_provider,
            prometheus_handler,
            serve_state: Mutex::new(None),
        }
    }

    pub fn create_flight_service(&self) -> FlightServiceServer<impl FlightService> {
        FlightServiceServer::new(FlightHandler::new(self.request_handler.clone()))
    }

    pub fn create_database_service(&self) -> GreptimeDatabaseServer<impl GreptimeDatabase> {
        GreptimeDatabaseServer::new(DatabaseService::new(self.request_handler.clone()))
    }

    pub fn create_healthcheck_service(&self) -> HealthCheckServer<impl HealthCheck> {
        HealthCheckServer::new(HealthCheckHandler)
    }

    pub fn create_prom_query_gateway_service(
        &self,
        handler: PrometheusHandlerRef,
    ) -> PrometheusGatewayServer<impl PrometheusGateway> {
        PrometheusGatewayServer::new(PrometheusGatewayService::new(
            handler,
            self.user_provider.clone(),
        ))
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

        let reflection_service = tonic_reflection::server::Builder::configure()
            .register_encoded_file_descriptor_set(api::v1::GREPTIME_GRPC_DESC)
            .with_service_name("greptime.v1.GreptimeDatabase")
            .with_service_name("greptime.v1.HealthCheck")
            .build()
            .context(GrpcReflectionServiceSnafu)?;

        let mut builder = tonic::transport::Server::builder()
            .add_service(self.create_flight_service())
            .add_service(self.create_database_service())
            .add_service(self.create_healthcheck_service());
        if let Some(prometheus_handler) = &self.prometheus_handler {
            builder = builder
                .add_service(self.create_prom_query_gateway_service(prometheus_handler.clone()))
        }
        let builder = builder.add_service(reflection_service);

        let (serve_state_tx, serve_state_rx) = oneshot::channel();
        let mut serve_state = self.serve_state.lock().await;
        *serve_state = Some(serve_state_rx);

        let _handle = common_runtime::spawn_bg(async move {
            let result = builder
                .serve_with_incoming_shutdown(TcpListenerStream::new(listener), rx.map(drop))
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
