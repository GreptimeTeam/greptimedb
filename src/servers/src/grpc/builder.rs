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

use std::sync::Arc;

use api::v1::greptime_database_server::GreptimeDatabaseServer;
use api::v1::prometheus_gateway_server::PrometheusGatewayServer;
use api::v1::region::region_server::RegionServer;
use arrow_flight::flight_service_server::FlightServiceServer;
use auth::UserProviderRef;
use common_runtime::Runtime;
use opentelemetry_proto::tonic::collector::metrics::v1::metrics_service_server::MetricsServiceServer;
use opentelemetry_proto::tonic::collector::trace::v1::trace_service_server::TraceServiceServer;
use tokio::sync::Mutex;
use tonic::transport::server::RoutesBuilder;
use tower::ServiceBuilder;

use super::flight::{FlightCraftRef, FlightCraftWrapper};
use super::region_server::{RegionServerHandlerRef, RegionServerRequestHandler};
use super::{GrpcServer, GrpcServerConfig};
use crate::grpc::authorize::AuthMiddlewareLayer;
use crate::grpc::database::DatabaseService;
use crate::grpc::greptime_handler::GreptimeRequestHandler;
use crate::grpc::otlp::OtlpService;
use crate::grpc::prom_query_gateway::PrometheusGatewayService;
use crate::prometheus_handler::PrometheusHandlerRef;
use crate::query_handler::OpenTelemetryProtocolHandlerRef;

macro_rules! add_service {
    ($builder: ident, $service: expr) => {
        $builder.routes_builder.add_service(
            $service
                .max_decoding_message_size($builder.config.max_recv_message_size)
                .max_encoding_message_size($builder.config.max_send_message_size),
        )
    };
}

pub struct GrpcServerBuilder {
    config: GrpcServerConfig,
    runtime: Arc<Runtime>,
    routes_builder: RoutesBuilder,
}

impl GrpcServerBuilder {
    pub fn new(config: GrpcServerConfig, runtime: Arc<Runtime>) -> Self {
        Self {
            config,
            runtime,
            routes_builder: RoutesBuilder::default(),
        }
    }

    pub fn runtime(&self) -> &Arc<Runtime> {
        &self.runtime
    }

    /// Add handler for [DatabaseService] service.
    pub fn database_handler(mut self, database_handler: GreptimeRequestHandler) -> Self {
        add_service!(
            self,
            GreptimeDatabaseServer::new(DatabaseService::new(database_handler))
        );
        self
    }

    /// Add handler for Prometheus-compatible PromQL queries ([PrometheusGateway]).
    pub fn prometheus_handler(
        mut self,
        prometheus_handler: PrometheusHandlerRef,
        user_provider: Option<UserProviderRef>,
    ) -> Self {
        add_service!(
            self,
            PrometheusGatewayServer::new(PrometheusGatewayService::new(
                prometheus_handler,
                user_provider,
            ))
        );
        self
    }

    /// Add handler for [FlightService](arrow_flight::flight_service_server::FlightService).
    pub fn flight_handler(mut self, flight_handler: FlightCraftRef) -> Self {
        add_service!(
            self,
            FlightServiceServer::new(FlightCraftWrapper(flight_handler.clone()))
        );
        self
    }

    /// Add handler for [RegionServer].
    pub fn region_server_handler(mut self, region_server_handler: RegionServerHandlerRef) -> Self {
        let handler = RegionServerRequestHandler::new(region_server_handler, self.runtime.clone());
        add_service!(self, RegionServer::new(handler));
        self
    }

    /// Add handler for OpenTelemetry Protocol (OTLP) requests.
    pub fn otlp_handler(
        mut self,
        otlp_handler: OpenTelemetryProtocolHandlerRef,
        user_provider: Option<UserProviderRef>,
    ) -> Self {
        let trace_server = ServiceBuilder::new()
            .layer(AuthMiddlewareLayer::with(user_provider.clone()))
            .service(TraceServiceServer::new(OtlpService::new(
                otlp_handler.clone(),
            )));
        self.routes_builder.add_service(trace_server);

        let metrics_server = ServiceBuilder::new()
            .layer(AuthMiddlewareLayer::with(user_provider))
            .service(MetricsServiceServer::new(OtlpService::new(otlp_handler)));
        self.routes_builder.add_service(metrics_server);

        self
    }

    pub fn routes_builder_mut(&mut self) -> &mut RoutesBuilder {
        &mut self.routes_builder
    }

    pub fn build(self) -> GrpcServer {
        GrpcServer {
            routes: Mutex::new(Some(self.routes_builder.routes())),
            shutdown_tx: Mutex::new(None),
            serve_state: Mutex::new(None),
        }
    }
}
