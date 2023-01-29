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
use common_telemetry::tracing::log::info;
use servers::error::Error::InternalIo;
use servers::grpc::GrpcServer;
use servers::mysql::server::{MysqlServer, MysqlSpawnConfig, MysqlSpawnRef};
use servers::query_handler::grpc::ServerGrpcQueryHandlerAdaptor;
use servers::query_handler::sql::ServerSqlQueryHandlerAdaptor;
use servers::server::Server;
use servers::tls::TlsOption;
use servers::Mode;
use snafu::ResultExt;

use crate::datanode::DatanodeOptions;
use crate::error::Error::StartServer;
use crate::error::{ParseAddrSnafu, Result, RuntimeResourceSnafu, StartServerSnafu};
use crate::instance::InstanceRef;

pub mod grpc;

/// All rpc services.
pub struct Services {
    grpc_server: GrpcServer,
    mysql_server: Option<Box<dyn Server>>,
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

        let mysql_server = match opts.mode {
            Mode::Standalone => {
                info!("Disable MySQL server on datanode when running in standalone mode");
                None
            }
            Mode::Distributed => {
                let mysql_io_runtime = Arc::new(
                    RuntimeBuilder::default()
                        .worker_threads(opts.mysql_runtime_size)
                        .thread_name("mysql-io-handlers")
                        .build()
                        .context(RuntimeResourceSnafu)?,
                );
                let tls = TlsOption::default();
                // default tls config returns None
                // but try to think a better way to do this
                Some(MysqlServer::create_server(
                    mysql_io_runtime,
                    Arc::new(MysqlSpawnRef::new(
                        ServerSqlQueryHandlerAdaptor::arc(instance.clone()),
                        None,
                    )),
                    Arc::new(MysqlSpawnConfig::new(
                        tls.should_force_tls(),
                        tls.setup()
                            .map_err(|e| StartServer {
                                source: InternalIo { source: e },
                            })?
                            .map(Arc::new),
                        false,
                    )),
                ))
            }
        };

        Ok(Self {
            grpc_server: GrpcServer::new(
                ServerGrpcQueryHandlerAdaptor::arc(instance),
                grpc_runtime,
            ),
            mysql_server,
        })
    }

    pub async fn start(&mut self, opts: &DatanodeOptions) -> Result<()> {
        let grpc_addr: SocketAddr = opts.rpc_addr.parse().context(ParseAddrSnafu {
            addr: &opts.rpc_addr,
        })?;

        let mut res = vec![self.grpc_server.start(grpc_addr)];
        if let Some(mysql_server) = &self.mysql_server {
            let mysql_addr = &opts.mysql_addr;
            let mysql_addr: SocketAddr = mysql_addr
                .parse()
                .context(ParseAddrSnafu { addr: mysql_addr })?;
            res.push(mysql_server.start(mysql_addr));
        };

        futures::future::try_join_all(res)
            .await
            .context(StartServerSnafu)?;
        Ok(())
    }
}
