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
use common_telemetry::info;
use servers::grpc::GrpcServer;
use servers::http::{HttpOptions, HttpServer};
use servers::metrics_handler::MetricsHandler;
use servers::query_handler::grpc::ServerGrpcQueryHandlerAdaptor;
use servers::server::Server;
use servers::Mode;
use snafu::ResultExt;
use tokio::select;

use crate::datanode::DatanodeOptions;
use crate::error::{
    ParseAddrSnafu, Result, RuntimeResourceSnafu, ShutdownServerSnafu, StartServerSnafu,
};
use crate::instance::InstanceRef;

pub mod grpc;

/// All rpc services.
pub struct Services {
    grpc_server: GrpcServer,
    http_server: Option<HttpServer>,
}

impl Services {
    pub async fn try_new(instance: InstanceRef, opts: &DatanodeOptions) -> Result<Self> {
        let grpc_runtime = Arc::new(
            RuntimeBuilder::default()
                .worker_threads(opts.rpc_runtime_size)
                .thread_name("grpc-io-handlers")
                .build()
                .context(RuntimeResourceSnafu)?,
        );

        Ok(Self {
            grpc_server: GrpcServer::new(
                ServerGrpcQueryHandlerAdaptor::arc(instance),
                None,
                grpc_runtime,
            ),
            http_server: match opts.mode {
                Mode::Standalone => Some(HttpServer::new_with_metrics_handler(
                    MetricsHandler,
                    HttpOptions::default(),
                )),
                Mode::Distributed => None,
            },
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
        if let Some(ref http_server) = self.http_server {
            let http = http_server.start(http_addr);
            select!(
                v = grpc => v.context(StartServerSnafu)?,
                v= http => v.context(StartServerSnafu)?,);
        } else {
            grpc.await.context(StartServerSnafu)?;
        }

        Ok(())
    }

    pub async fn shutdown(&self) -> Result<()> {
        self.grpc_server
            .shutdown()
            .await
            .context(ShutdownServerSnafu)?;
        if let Some(ref http_server) = self.http_server {
            http_server.shutdown().await.context(ShutdownServerSnafu)?;
        }
        Ok(())
    }
}
