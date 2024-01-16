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

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use servers::grpc::builder::GrpcServerBuilder;
use servers::grpc::{GrpcServer, GrpcServerConfig};
use servers::http::HttpServerBuilder;
use servers::metrics_handler::MetricsHandler;
use servers::server::{ServerHandler, ServerHandlers};
use snafu::ResultExt;

use crate::config::DatanodeOptions;
use crate::error::{ParseAddrSnafu, Result};
use crate::region_server::RegionServer;

const DATANODE_GRPC_SERVICE_NAME: &str = "DATANODE_GRPC_SERVICE";
const DATANODE_HTTP_SERVICE_NAME: &str = "DATANODE_HTTP_SERVICE";

pub struct DatanodeServiceBuilder<'a> {
    opts: &'a DatanodeOptions,
    grpc_server: Option<GrpcServer>,
    enable_http_service: bool,
}

impl<'a> DatanodeServiceBuilder<'a> {
    pub fn new(opts: &'a DatanodeOptions) -> Self {
        Self {
            opts,
            grpc_server: None,
            enable_http_service: false,
        }
    }

    pub fn with_grpc_server(self, grpc_server: GrpcServer) -> Self {
        Self {
            grpc_server: Some(grpc_server),
            ..self
        }
    }

    pub fn with_default_grpc_server(mut self, region_server: &RegionServer) -> Self {
        let grpc_server = Self::grpc_server_builder(self.opts, region_server).build();
        self.grpc_server = Some(grpc_server);
        self
    }

    pub fn enable_http_service(self) -> Self {
        Self {
            enable_http_service: true,
            ..self
        }
    }

    pub fn build(mut self) -> Result<ServerHandlers> {
        let mut services = HashMap::new();

        if let Some(grpc_server) = self.grpc_server.take() {
            let addr: SocketAddr = self.opts.rpc_addr.parse().context(ParseAddrSnafu {
                addr: &self.opts.rpc_addr,
            })?;
            let handler: ServerHandler = (Box::new(grpc_server), addr);
            services.insert(DATANODE_GRPC_SERVICE_NAME.to_string(), handler);
        }

        if self.enable_http_service {
            let http_server = HttpServerBuilder::new(self.opts.http.clone())
                .with_metrics_handler(MetricsHandler)
                .with_greptime_config_options(self.opts.to_toml_string())
                .build();
            let addr: SocketAddr = self.opts.http.addr.parse().context(ParseAddrSnafu {
                addr: &self.opts.http.addr,
            })?;
            let handler: ServerHandler = (Box::new(http_server), addr);
            services.insert(DATANODE_HTTP_SERVICE_NAME.to_string(), handler);
        }

        Ok(services)
    }

    pub fn grpc_server_builder(
        opts: &DatanodeOptions,
        region_server: &RegionServer,
    ) -> GrpcServerBuilder {
        let config = GrpcServerConfig {
            max_recv_message_size: opts.rpc_max_recv_message_size.as_bytes() as usize,
            max_send_message_size: opts.rpc_max_send_message_size.as_bytes() as usize,
        };

        GrpcServerBuilder::new(config, region_server.runtime())
            .flight_handler(Arc::new(region_server.clone()))
            .region_server_handler(Arc::new(region_server.clone()))
    }
}
