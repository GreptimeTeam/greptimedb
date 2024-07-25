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

use api::v1::greptime_database_server::GreptimeDatabaseServer;
use api::v1::prometheus_gateway_server::PrometheusGatewayServer;
use api::v1::region::region_server::RegionServer;
use arrow_flight::flight_service_server::FlightServiceServer;
use auth::UserProviderRef;
use common_grpc::error::{Error, InvalidConfigFilePathSnafu, Result};
use common_runtime::Runtime;
use opentelemetry_proto::tonic::collector::metrics::v1::metrics_service_server::MetricsServiceServer;
use opentelemetry_proto::tonic::collector::trace::v1::trace_service_server::TraceServiceServer;
use snafu::ResultExt;
use tokio::sync::Mutex;
use tonic::codec::CompressionEncoding;
use tonic::transport::server::RoutesBuilder;
use tonic::transport::{Identity, ServerTlsConfig};
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
use crate::tls::TlsOption;

/// Add a gRPC service (`service`) to a `builder`([RoutesBuilder]).
/// This macro will automatically add some gRPC properties to the service.
#[macro_export]
macro_rules! add_service {
    ($builder: ident, $service: expr) => {
        let max_recv_message_size = $builder.config().max_recv_message_size;
        let max_send_message_size = $builder.config().max_send_message_size;

        use tonic::codec::CompressionEncoding;
        let service_builder = $service
            .max_decoding_message_size(max_recv_message_size)
            .max_encoding_message_size(max_send_message_size)
            .accept_compressed(CompressionEncoding::Gzip)
            .accept_compressed(CompressionEncoding::Zstd)
            .send_compressed(CompressionEncoding::Gzip)
            .send_compressed(CompressionEncoding::Zstd);

        $builder.routes_builder_mut().add_service(service_builder);
    };
}

pub struct GrpcServerBuilder {
    config: GrpcServerConfig,
    runtime: Runtime,
    routes_builder: RoutesBuilder,
    tls_config: Option<ServerTlsConfig>,
}

impl GrpcServerBuilder {
    pub fn new(config: GrpcServerConfig, runtime: Runtime) -> Self {
        Self {
            config,
            runtime,
            routes_builder: RoutesBuilder::default(),
            tls_config: None,
        }
    }

    pub fn config(&self) -> &GrpcServerConfig {
        &self.config
    }

    pub fn runtime(&self) -> &Runtime {
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
        let tracing_service = TraceServiceServer::new(OtlpService::new(otlp_handler.clone()))
            .accept_compressed(CompressionEncoding::Gzip)
            .accept_compressed(CompressionEncoding::Zstd)
            .send_compressed(CompressionEncoding::Gzip)
            .send_compressed(CompressionEncoding::Zstd);

        let trace_server = ServiceBuilder::new()
            .layer(AuthMiddlewareLayer::with(user_provider.clone()))
            .service(tracing_service);
        self.routes_builder.add_service(trace_server);

        let metrics_service = MetricsServiceServer::new(OtlpService::new(otlp_handler))
            .accept_compressed(CompressionEncoding::Gzip)
            .accept_compressed(CompressionEncoding::Zstd)
            .send_compressed(CompressionEncoding::Gzip)
            .send_compressed(CompressionEncoding::Zstd);

        let metrics_server = ServiceBuilder::new()
            .layer(AuthMiddlewareLayer::with(user_provider))
            .service(metrics_service);
        self.routes_builder.add_service(metrics_server);

        self
    }

    pub fn routes_builder_mut(&mut self) -> &mut RoutesBuilder {
        &mut self.routes_builder
    }

    pub fn with_tls_config(mut self, tls_option: TlsOption) -> Result<Self> {
        // tonic does not support watching for tls config changes
        // so we don't support it either for now
        if tls_option.watch {
            return Err(Error::NotSupported {
                feat: "Certificates watch and reloading for gRPC is not supported at the moment"
                    .to_string(),
            });
        }
        self.tls_config = if tls_option.should_force_tls() {
            let cert = std::fs::read_to_string(tls_option.cert_path)
                .context(InvalidConfigFilePathSnafu)?;
            let key =
                std::fs::read_to_string(tls_option.key_path).context(InvalidConfigFilePathSnafu)?;
            let identity = Identity::from_pem(cert, key);
            Some(ServerTlsConfig::new().identity(identity))
        } else {
            None
        };
        Ok(self)
    }

    pub fn build(self) -> GrpcServer {
        GrpcServer {
            routes: Mutex::new(Some(self.routes_builder.routes())),
            shutdown_tx: Mutex::new(None),
            serve_state: Mutex::new(None),
            tls_config: self.tls_config,
        }
    }
}
