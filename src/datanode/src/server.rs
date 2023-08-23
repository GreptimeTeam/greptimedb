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

use std::default::Default;
use std::net::SocketAddr;
use std::sync::Arc;

use common_runtime::Builder as RuntimeBuilder;
use futures::future;
use servers::grpc::GrpcServer;
use servers::http::{HttpServer, HttpServerBuilder};
use servers::metrics_handler::MetricsHandler;
use servers::query_handler::grpc::ServerGrpcQueryHandlerAdaptor;
use servers::server::Server;
use snafu::ResultExt;

use crate::datanode::DatanodeOptions;
use crate::error::{
    ParseAddrSnafu, Result, RuntimeResourceSnafu, ShutdownServerSnafu, StartServerSnafu,
    WaitForGrpcServingSnafu,
};
use crate::instance::InstanceRef;
use crate::region_server::RegionServer;

pub mod grpc;

/// All rpc services.
pub struct Services {
    grpc_server: GrpcServer,
    http_server: HttpServer,
}

impl Services {
    pub async fn try_new(instance: InstanceRef, opts: &DatanodeOptions) -> Result<Self> {
        // TODO(ruihang): remove database service once region server is ready.
        let enable_region_server = option_env!("ENABLE_REGION_SERVER").is_some();

        let grpc_runtime = Arc::new(
            RuntimeBuilder::default()
                .worker_threads(opts.rpc_runtime_size)
                .thread_name("grpc-io-handlers")
                .build()
                .context(RuntimeResourceSnafu)?,
        );

        let region_server = RegionServer::new(instance.query_engine());
        let flight_handler = if enable_region_server {
            Some(Arc::new(region_server.clone()) as _)
        } else {
            None
        };
        let region_server_handler = if enable_region_server {
            Some(Arc::new(region_server.clone()) as _)
        } else {
            None
        };

        Ok(Self {
            grpc_server: GrpcServer::new(
                ServerGrpcQueryHandlerAdaptor::arc(instance),
                None,
                flight_handler,
                region_server_handler,
                None,
                grpc_runtime,
            ),
            http_server: HttpServerBuilder::new(opts.http_opts.clone())
                .with_metrics_handler(MetricsHandler)
                .with_greptime_config_options(opts.to_toml_string())
                .build(),
        })
    }

    pub async fn start(&mut self, opts: &DatanodeOptions) -> Result<()> {
        let grpc_addr: SocketAddr = opts.rpc_addr.parse().context(ParseAddrSnafu {
            addr: &opts.rpc_addr,
        })?;
        let http_addr = opts.http_opts.addr.parse().context(ParseAddrSnafu {
            addr: &opts.http_opts.addr,
        })?;
        let grpc = self.grpc_server.start(grpc_addr);
        let http = self.http_server.start(http_addr);
        let _ = future::try_join_all(vec![grpc, http])
            .await
            .context(StartServerSnafu)?;

        self.grpc_server
            .wait_for_serve()
            .await
            .context(WaitForGrpcServingSnafu)?;
        Ok(())
    }

    pub async fn shutdown(&self) -> Result<()> {
        self.grpc_server
            .shutdown()
            .await
            .context(ShutdownServerSnafu)?;
        self.http_server
            .shutdown()
            .await
            .context(ShutdownServerSnafu)?;
        Ok(())
    }
}
