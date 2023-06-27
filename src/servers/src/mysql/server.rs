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

use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;

use async_trait::async_trait;
use common_runtime::Runtime;
use common_telemetry::logging::{info, warn};
use futures::StreamExt;
use metrics::{decrement_gauge, increment_gauge};
use opensrv_mysql::{
    plain_run_with_options, secure_run_with_options, AsyncMysqlIntermediary, IntermediaryOptions,
};
use tokio;
use tokio::io::BufWriter;
use tokio::net::TcpStream;
use tokio_rustls::rustls::ServerConfig;

use crate::auth::UserProviderRef;
use crate::error::{Error, Result};
use crate::mysql::handler::MysqlInstanceShim;
use crate::query_handler::sql::ServerSqlQueryHandlerRef;
use crate::server::{AbortableStream, BaseTcpServer, Server};

// Default size of ResultSet write buffer: 100KB
const DEFAULT_RESULT_SET_WRITE_BUFFER_SIZE: usize = 100 * 1024;

/// [`MysqlSpawnRef`] stores arc refs
/// that should be passed to new [`MysqlInstanceShim`]s.
pub struct MysqlSpawnRef {
    query_handler: ServerSqlQueryHandlerRef,
    user_provider: Option<UserProviderRef>,
}

impl MysqlSpawnRef {
    pub fn new(
        query_handler: ServerSqlQueryHandlerRef,
        user_provider: Option<UserProviderRef>,
    ) -> MysqlSpawnRef {
        MysqlSpawnRef {
            query_handler,
            user_provider,
        }
    }

    fn query_handler(&self) -> ServerSqlQueryHandlerRef {
        self.query_handler.clone()
    }
    fn user_provider(&self) -> Option<UserProviderRef> {
        self.user_provider.clone()
    }
}

/// [`MysqlSpawnConfig`] stores config values
/// which are used to initialize [`MysqlInstanceShim`]s.
pub struct MysqlSpawnConfig {
    // tls config
    force_tls: bool,
    tls: Option<Arc<ServerConfig>>,
    // other shim config
    reject_no_database: bool,
}

impl MysqlSpawnConfig {
    pub fn new(
        force_tls: bool,
        tls: Option<Arc<ServerConfig>>,
        reject_no_database: bool,
    ) -> MysqlSpawnConfig {
        MysqlSpawnConfig {
            force_tls,
            tls,
            reject_no_database,
        }
    }

    fn tls(&self) -> Option<Arc<ServerConfig>> {
        self.tls.clone()
    }
}

impl From<&MysqlSpawnConfig> for IntermediaryOptions {
    fn from(value: &MysqlSpawnConfig) -> Self {
        IntermediaryOptions {
            reject_connection_on_dbname_absence: value.reject_no_database,
            ..Default::default()
        }
    }
}

pub struct MysqlServer {
    base_server: BaseTcpServer,
    spawn_ref: Arc<MysqlSpawnRef>,
    spawn_config: Arc<MysqlSpawnConfig>,
}

impl MysqlServer {
    pub fn create_server(
        io_runtime: Arc<Runtime>,
        spawn_ref: Arc<MysqlSpawnRef>,
        spawn_config: Arc<MysqlSpawnConfig>,
    ) -> Box<dyn Server> {
        Box::new(MysqlServer {
            base_server: BaseTcpServer::create_server("MySQL", io_runtime),
            spawn_ref,
            spawn_config,
        })
    }

    fn accept(
        &self,
        io_runtime: Arc<Runtime>,
        stream: AbortableStream,
    ) -> impl Future<Output = ()> {
        let spawn_ref = self.spawn_ref.clone();
        let spawn_config = self.spawn_config.clone();

        stream.for_each(move |tcp_stream| {
            let io_runtime = io_runtime.clone();
            let spawn_ref = spawn_ref.clone();
            let spawn_config = spawn_config.clone();

            async move {
                match tcp_stream {
                    Err(error) => warn!("Broken pipe: {}", error), // IoError doesn't impl ErrorExt.
                    Ok(io_stream) => {
                        if let Err(error) =
                            Self::handle(io_stream, io_runtime, spawn_ref, spawn_config).await
                        {
                            warn!("Unexpected error when handling TcpStream {}", error);
                        };
                    }
                };
            }
        })
    }

    async fn handle(
        stream: TcpStream,
        io_runtime: Arc<Runtime>,
        spawn_ref: Arc<MysqlSpawnRef>,
        spawn_config: Arc<MysqlSpawnConfig>,
    ) -> Result<()> {
        info!("MySQL connection coming from: {}", stream.peer_addr()?);
        let _handle = io_runtime.spawn(async move {
            increment_gauge!(crate::metrics::METRIC_MYSQL_CONNECTIONS, 1.0);
            if let Err(e)  = Self::do_handle(stream, spawn_ref, spawn_config).await {
                // TODO(LFC): Write this error to client as well, in MySQL text protocol.
                // Looks like we have to expose opensrv-mysql's `PacketWriter`?
                warn!("Internal error occurred during query exec, server actively close the channel to let client try next time: {}.", e)
            }
            decrement_gauge!(crate::metrics::METRIC_MYSQL_CONNECTIONS, 1.0);
        });

        Ok(())
    }

    async fn do_handle(
        stream: TcpStream,
        spawn_ref: Arc<MysqlSpawnRef>,
        spawn_config: Arc<MysqlSpawnConfig>,
    ) -> Result<()> {
        let mut shim = MysqlInstanceShim::create(
            spawn_ref.query_handler(),
            spawn_ref.user_provider(),
            stream.peer_addr()?,
        );
        let (mut r, w) = stream.into_split();
        let mut w = BufWriter::with_capacity(DEFAULT_RESULT_SET_WRITE_BUFFER_SIZE, w);

        let ops = spawn_config.as_ref().into();

        let (client_tls, init_params) =
            AsyncMysqlIntermediary::init_before_ssl(&mut shim, &mut r, &mut w, &spawn_config.tls())
                .await?;

        if spawn_config.force_tls && !client_tls {
            return Err(Error::TlsRequired {
                server: "mysql".to_owned(),
            });
        }

        match spawn_config.tls() {
            Some(tls_conf) if client_tls => {
                secure_run_with_options(shim, w, ops, tls_conf, init_params).await
            }
            _ => plain_run_with_options(shim, w, ops, init_params).await,
        }
    }
}

pub const MYSQL_SERVER: &str = "MYSQL_SERVER";

#[async_trait]
impl Server for MysqlServer {
    async fn shutdown(&self) -> Result<()> {
        self.base_server.shutdown().await
    }

    async fn start(&self, listening: SocketAddr) -> Result<SocketAddr> {
        let (stream, addr) = self.base_server.bind(listening).await?;
        let io_runtime = self.base_server.io_runtime();

        let join_handle = tokio::spawn(self.accept(io_runtime, stream));
        self.base_server.start_with(join_handle).await?;
        Ok(addr)
    }

    fn name(&self) -> &str {
        MYSQL_SERVER
    }
}
