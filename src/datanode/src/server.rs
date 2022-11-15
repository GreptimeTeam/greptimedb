// Copyright 2022 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
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
use servers::mysql::server::MysqlServer;
use servers::server::Server;
use snafu::ResultExt;
use tokio::try_join;

use crate::datanode::DatanodeOptions;
use crate::error::{ParseAddrSnafu, Result, RuntimeResourceSnafu, StartServerSnafu};
use crate::instance::InstanceRef;

pub mod grpc;

/// All rpc services.
pub struct Services {
    grpc_server: GrpcServer,
    mysql_server: Box<dyn Server>,
}

impl Services {
    pub async fn try_new(instance: InstanceRef, opts: &DatanodeOptions) -> Result<Self> {
        let grpc_runtime = Arc::new(
            RuntimeBuilder::default()
                .worker_threads(opts.rpc_runtime_size as usize)
                .thread_name("grpc-io-handlers")
                .build()
                .context(RuntimeResourceSnafu)?,
        );

        let mysql_io_runtime = Arc::new(
            RuntimeBuilder::default()
                .worker_threads(opts.mysql_runtime_size as usize)
                .thread_name("mysql-io-handlers")
                .build()
                .context(RuntimeResourceSnafu)?,
        );

        Ok(Self {
            grpc_server: GrpcServer::new(instance.clone(), instance.clone(), grpc_runtime),
            mysql_server: MysqlServer::create_server(instance, mysql_io_runtime),
        })
    }

    pub async fn start(&mut self, opts: &DatanodeOptions) -> Result<()> {
        let grpc_addr: SocketAddr = opts.rpc_addr.parse().context(ParseAddrSnafu {
            addr: &opts.rpc_addr,
        })?;
        let mysql_addr = &opts.mysql_addr;
        let mysql_addr: SocketAddr = mysql_addr
            .parse()
            .context(ParseAddrSnafu { addr: mysql_addr })?;
        try_join!(
            self.grpc_server.start(grpc_addr),
            self.mysql_server.start(mysql_addr),
        )
        .context(StartServerSnafu)?;
        Ok(())
    }
}
