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
use servers::grpc::GrpcServer;
use servers::query_handler::grpc::ServerGrpcQueryHandlerAdaptor;
use servers::server::Server;
use snafu::ResultExt;

use crate::datanode::DatanodeOptions;
use crate::error::Error::StartServer;
use crate::error::{
    ParseAddrSnafu, Result, RuntimeResourceSnafu, ShutdownServerSnafu, StartServerSnafu,
};
use crate::instance::InstanceRef;

pub mod grpc;

/// All rpc services.
pub struct Services {
    grpc_server: GrpcServer,
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
        })
    }

    pub async fn start(&mut self, opts: &DatanodeOptions) -> Result<()> {
        let grpc_addr: SocketAddr = opts.rpc_addr.parse().context(ParseAddrSnafu {
            addr: &opts.rpc_addr,
        })?;
        self.grpc_server
            .start(grpc_addr)
            .await
            .context(StartServerSnafu)?;
        Ok(())
    }

    pub async fn shutdown(&self) -> Result<()> {
        let mut res = vec![self.grpc_server.shutdown()];
        if let Some(mysql_server) = &self.mysql_server {
            res.push(mysql_server.shutdown());
        }

        futures::future::try_join_all(res)
            .await
            .context(ShutdownServerSnafu)?;

        Ok(())
    }
}
