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

use std::net::SocketAddr;
use std::sync::Arc;

use futures::future;
use servers::grpc::GrpcServer;
use servers::http::{HttpServer, HttpServerBuilder};
use servers::metrics_handler::MetricsHandler;
use servers::server::Server;
use snafu::ResultExt;

use crate::datanode::DatanodeOptions;
use crate::error::{
    ParseAddrSnafu, Result, ShutdownServerSnafu, StartServerSnafu, WaitForGrpcServingSnafu,
};
use crate::region_server::RegionServer;

/// All rpc services.
pub struct Services {
    grpc_server: GrpcServer,
    http_server: HttpServer,
}

impl Services {
    pub async fn try_new(region_server: RegionServer, opts: &DatanodeOptions) -> Result<Self> {
        let flight_handler = Some(Arc::new(region_server.clone()) as _);
        let region_server_handler = Some(Arc::new(region_server.clone()) as _);
        let runtime = region_server.runtime();

        Ok(Self {
            grpc_server: GrpcServer::new(
                None,
                None,
                flight_handler,
                region_server_handler,
                None,
                runtime,
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
