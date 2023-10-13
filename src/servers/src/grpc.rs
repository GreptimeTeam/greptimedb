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
pub mod greptime_handler;
pub mod prom_query_gateway;
pub mod region_server;

use std::net::SocketAddr;
use std::sync::Arc;

use api::v1::greptime_database_server::GreptimeDatabaseServer;
use api::v1::health_check_server::{HealthCheck, HealthCheckServer};
use api::v1::prometheus_gateway_server::{PrometheusGateway, PrometheusGatewayServer};
#[cfg(feature = "testing")]
use api::v1::region::region_server::Region;
use api::v1::region::region_server::RegionServer;
use api::v1::{HealthCheckRequest, HealthCheckResponse};
#[cfg(feature = "testing")]
use arrow_flight::flight_service_server::FlightService;
use arrow_flight::flight_service_server::FlightServiceServer;
use async_trait::async_trait;
use auth::UserProviderRef;
use common_grpc::channel_manager::{
    DEFAULT_MAX_GRPC_RECV_MESSAGE_SIZE, DEFAULT_MAX_GRPC_SEND_MESSAGE_SIZE,
};
use common_runtime::Runtime;
use common_telemetry::logging::info;
use common_telemetry::{error, warn};
use futures::FutureExt;
use snafu::{ensure, OptionExt, ResultExt};
use tokio::net::TcpListener;
use tokio::sync::oneshot::{self, Receiver, Sender};
use tokio::sync::Mutex;
use tonic::transport::server::TcpIncoming;
use tonic::{Request, Response, Status};
use tonic_reflection::server::{ServerReflection, ServerReflectionServer};

use self::flight::{FlightCraftRef, FlightCraftWrapper};
use self::prom_query_gateway::PrometheusGatewayService;
use self::region_server::{RegionServerHandlerRef, RegionServerRequestHandler};
use crate::error::{
    AlreadyStartedSnafu, InternalSnafu, Result, StartGrpcSnafu, TcpBindSnafu, TcpIncomingSnafu,
};
use crate::grpc::database::DatabaseService;
use crate::grpc::greptime_handler::GreptimeRequestHandler;
use crate::prometheus_handler::PrometheusHandlerRef;
use crate::query_handler::grpc::ServerGrpcQueryHandlerRef;
use crate::server::Server;

type TonicResult<T> = std::result::Result<T, Status>;

pub struct GrpcServer {
    config: GrpcServerConfig,
    // states
    shutdown_tx: Mutex<Option<Sender<()>>>,
    user_provider: Option<UserProviderRef>,

    /// gRPC serving state receiver. Only present if the gRPC server is started.
    /// Used to wait for the server to stop, performing the old blocking fashion.
    serve_state: Mutex<Option<Receiver<Result<()>>>>,

    // handlers
    /// Handler for [DatabaseService] service.
    database_handler: Option<GreptimeRequestHandler>,
    /// Handler for Prometheus-compatible PromQL queries ([PrometheusGateway]). Only present for frontend server.
    prometheus_handler: Option<PrometheusHandlerRef>,
    /// Handler for [FlightService](arrow_flight::flight_service_server::FlightService).
    flight_handler: Option<FlightCraftRef>,
    /// Handler for [RegionServer].
    region_server_handler: Option<RegionServerRequestHandler>,
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
    pub fn new(
        config: Option<GrpcServerConfig>,
        query_handler: Option<ServerGrpcQueryHandlerRef>,
        prometheus_handler: Option<PrometheusHandlerRef>,
        flight_handler: Option<FlightCraftRef>,
        region_server_handler: Option<RegionServerHandlerRef>,
        user_provider: Option<UserProviderRef>,
        runtime: Arc<Runtime>,
    ) -> Self {
        let database_handler = query_handler.map(|handler| {
            GreptimeRequestHandler::new(handler, user_provider.clone(), runtime.clone())
        });
        let region_server_handler = region_server_handler
            .map(|handler| RegionServerRequestHandler::new(handler, runtime.clone()));
        Self {
            config: config.unwrap_or_default(),
            shutdown_tx: Mutex::new(None),
            user_provider,
            serve_state: Mutex::new(None),
            database_handler,
            prometheus_handler,
            flight_handler,
            region_server_handler,
        }
    }

    #[cfg(feature = "testing")]
    pub fn create_flight_service(&self) -> FlightServiceServer<impl FlightService> {
        FlightServiceServer::new(FlightCraftWrapper(self.flight_handler.clone().unwrap()))
    }

    #[cfg(feature = "testing")]
    pub fn create_region_service(&self) -> RegionServer<impl Region> {
        RegionServer::new(self.region_server_handler.clone().unwrap())
    }

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
        let max_recv_message_size = self.config.max_recv_message_size;
        let max_send_message_size = self.config.max_send_message_size;
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

        let mut builder = tonic::transport::Server::builder()
            .add_service(self.create_healthcheck_service())
            .add_service(self.create_reflection_service());
        if let Some(database_handler) = &self.database_handler {
            builder = builder.add_service(
                GreptimeDatabaseServer::new(DatabaseService::new(database_handler.clone()))
                    .max_decoding_message_size(max_recv_message_size)
                    .max_encoding_message_size(max_send_message_size),
            )
        }
        if let Some(prometheus_handler) = &self.prometheus_handler {
            builder = builder
                .add_service(self.create_prom_query_gateway_service(prometheus_handler.clone()))
        }
        if let Some(flight_handler) = &self.flight_handler {
            builder = builder.add_service(
                FlightServiceServer::new(FlightCraftWrapper(flight_handler.clone()))
                    .max_decoding_message_size(max_recv_message_size)
                    .max_encoding_message_size(max_send_message_size),
            )
        } else {
            // TODO(ruihang): this is a temporary workaround before region server is ready.
            builder = builder.add_service(
                FlightServiceServer::new(FlightCraftWrapper(
                    self.database_handler.clone().unwrap(),
                ))
                .max_decoding_message_size(max_recv_message_size)
                .max_encoding_message_size(max_send_message_size),
            )
        }
        if let Some(region_server_handler) = &self.region_server_handler {
            builder = builder.add_service(
                RegionServer::new(region_server_handler.clone())
                    .max_decoding_message_size(max_recv_message_size)
                    .max_encoding_message_size(max_send_message_size),
            );
        }

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
