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

use auth::UserProviderRef;
use common_runtime::Runtime;
use tokio::sync::Mutex;

use super::flight::FlightCraftRef;
use super::region_server::{RegionServerHandlerRef, RegionServerRequestHandler};
use super::{GrpcServer, GrpcServerConfig};
use crate::grpc::greptime_handler::GreptimeRequestHandler;
use crate::prometheus_handler::PrometheusHandlerRef;
use crate::query_handler::grpc::ServerGrpcQueryHandlerRef;
use crate::query_handler::OpenTelemetryProtocolHandlerRef;

pub struct GrpcServerBuilder {
    config: Option<GrpcServerConfig>,
    query_handler: Option<ServerGrpcQueryHandlerRef>,
    prometheus_handler: Option<PrometheusHandlerRef>,
    flight_handler: Option<FlightCraftRef>,
    region_server_handler: Option<RegionServerHandlerRef>,
    otlp_handler: Option<OpenTelemetryProtocolHandlerRef>,
    user_provider: Option<UserProviderRef>,
    runtime: Arc<Runtime>,
}

impl GrpcServerBuilder {
    pub fn new(runtime: Arc<Runtime>) -> Self {
        Self {
            config: None,
            query_handler: None,
            prometheus_handler: None,
            flight_handler: None,
            region_server_handler: None,
            otlp_handler: None,
            user_provider: None,
            runtime,
        }
    }

    pub fn config(mut self, config: GrpcServerConfig) -> Self {
        self.config = Some(config);
        self
    }

    pub fn option_config(mut self, config: Option<GrpcServerConfig>) -> Self {
        self.config = config;
        self
    }

    pub fn query_handler(mut self, query_handler: ServerGrpcQueryHandlerRef) -> Self {
        self.query_handler = Some(query_handler);
        self
    }

    pub fn prometheus_handler(mut self, prometheus_handler: PrometheusHandlerRef) -> Self {
        self.prometheus_handler = Some(prometheus_handler);
        self
    }

    pub fn flight_handler(mut self, flight_handler: FlightCraftRef) -> Self {
        self.flight_handler = Some(flight_handler);
        self
    }

    pub fn region_server_handler(mut self, region_server_handler: RegionServerHandlerRef) -> Self {
        self.region_server_handler = Some(region_server_handler);
        self
    }

    pub fn otlp_handler(mut self, otlp_handler: OpenTelemetryProtocolHandlerRef) -> Self {
        self.otlp_handler = Some(otlp_handler);
        self
    }

    pub fn user_provider(mut self, user_provider: Option<UserProviderRef>) -> Self {
        self.user_provider = user_provider;
        self
    }

    pub fn build(self) -> GrpcServer {
        let config = self.config.unwrap_or_default();

        let user_provider = self.user_provider.as_ref();

        let runtime = self.runtime;

        let database_handler = self.query_handler.map(|handler| {
            GreptimeRequestHandler::new(handler, user_provider.cloned(), runtime.clone())
        });

        let region_server_handler = self
            .region_server_handler
            .map(|handler| RegionServerRequestHandler::new(handler, runtime));

        GrpcServer {
            config,
            prometheus_handler: self.prometheus_handler,
            flight_handler: self.flight_handler,
            region_server_handler,
            database_handler,
            otlp_handler: self.otlp_handler,
            user_provider: self.user_provider,
            shutdown_tx: Mutex::new(None),
            serve_state: Mutex::new(None),
        }
    }
}
