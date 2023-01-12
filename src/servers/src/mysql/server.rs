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
use common_telemetry::logging::{error, info};
use futures::StreamExt;
use opensrv_mysql::{
    plain_run_with_options, secure_run_with_options, AsyncMysqlIntermediary, IntermediaryOptions,
};
use tokio;
use tokio::io::BufWriter;
use tokio::net::TcpStream;
use tokio_rustls::rustls::ServerConfig;

use crate::auth::{SchemaValidatorRef, UserProviderRef};
use crate::error::{Error, Result};
use crate::mysql::handler::MysqlInstanceShim;
use crate::query_handler::sql::ServerSqlQueryHandlerRef;
use crate::server::{AbortableStream, BaseTcpServer, Server};
use crate::tls::TlsOption;

// Default size of ResultSet write buffer: 100KB
const DEFAULT_RESULT_SET_WRITE_BUFFER_SIZE: usize = 100 * 1024;

struct MysqlRuntimeOption {
    query_handler: ServerSqlQueryHandlerRef,
    tls_conf: Option<Arc<ServerConfig>>,
    force_tls: bool,
    user_provider: Option<UserProviderRef>,
    schema_validator: Option<SchemaValidatorRef>,
}

type MysqlRuntimeOptionRef = Arc<MysqlRuntimeOption>;

pub struct MysqlServer {
    base_server: BaseTcpServer,
    query_handler: ServerSqlQueryHandlerRef,
    tls: TlsOption,
    user_provider: Option<UserProviderRef>,
    schema_validator: Option<SchemaValidatorRef>,
}

impl MysqlServer {
    pub fn create_server(
        query_handler: ServerSqlQueryHandlerRef,
        io_runtime: Arc<Runtime>,
        tls: TlsOption,
        user_provider: Option<UserProviderRef>,
        schema_validator: Option<SchemaValidatorRef>,
    ) -> Box<dyn Server> {
        Box::new(MysqlServer {
            base_server: BaseTcpServer::create_server("MySQL", io_runtime),
            query_handler,
            tls,
            user_provider,
            schema_validator,
        })
    }

    fn accept(
        &self,
        io_runtime: Arc<Runtime>,
        stream: AbortableStream,
        tls_conf: Option<Arc<ServerConfig>>,
    ) -> impl Future<Output = ()> {
        let query_handler = self.query_handler.clone();
        let user_provider = self.user_provider.clone();
        let schema_validator = self.schema_validator.clone();

        let force_tls = self.tls.should_force_tls();

        stream.for_each(move |tcp_stream| {
            let io_runtime = io_runtime.clone();
            let query_handler = query_handler.clone();
            let user_provider = user_provider.clone();
            let schema_validator = schema_validator.clone();
            let tls_conf = tls_conf.clone();

            let mysql_runtime_option = Arc::new(MysqlRuntimeOption {
                query_handler,
                tls_conf,
                force_tls,
                user_provider,
                schema_validator,
            });

            async move {
                match tcp_stream {
                    Err(error) => error!("Broken pipe: {}", error), // IoError doesn't impl ErrorExt.
                    Ok(io_stream) => {
                        if let Err(error) =
                            Self::handle(io_stream, io_runtime, mysql_runtime_option).await
                        {
                            error!(error; "Unexpected error when handling TcpStream");
                        };
                    }
                };
            }
        })
    }

    async fn handle(
        stream: TcpStream,
        io_runtime: Arc<Runtime>,
        runtime_opts: MysqlRuntimeOptionRef,
    ) -> Result<()> {
        info!("MySQL connection coming from: {}", stream.peer_addr()?);
        io_runtime .spawn(async move {
            // TODO(LFC): Use `output_stream` to write large MySQL ResultSet to client.
            if let Err(e)  = Self::do_handle(stream, runtime_opts).await {
                // TODO(LFC): Write this error to client as well, in MySQL text protocol.
                // Looks like we have to expose opensrv-mysql's `PacketWriter`?
                error!(e; "Internal error occurred during query exec, server actively close the channel to let client try next time.")
            }
        });

        Ok(())
    }

    async fn do_handle(stream: TcpStream, runtime_opts: MysqlRuntimeOptionRef) -> Result<()> {
        let mut shim = MysqlInstanceShim::create(
            runtime_opts.query_handler.clone(),
            stream.peer_addr()?,
            runtime_opts.user_provider.clone(),
            runtime_opts.schema_validator.clone(),
        );
        let (mut r, w) = stream.into_split();
        let mut w = BufWriter::with_capacity(DEFAULT_RESULT_SET_WRITE_BUFFER_SIZE, w);
        let ops = IntermediaryOptions::default();

        let (client_tls, init_params) = AsyncMysqlIntermediary::init_before_ssl(
            &mut shim,
            &mut r,
            &mut w,
            &runtime_opts.tls_conf,
        )
        .await?;

        if runtime_opts.force_tls && !client_tls {
            return Err(Error::TlsRequired {
                server: "mysql".to_owned(),
            });
        }

        match runtime_opts.tls_conf.clone() {
            Some(tls_conf) if client_tls => {
                secure_run_with_options(shim, w, ops, tls_conf, init_params).await
            }
            _ => plain_run_with_options(shim, w, ops, init_params).await,
        }
    }
}

#[async_trait]
impl Server for MysqlServer {
    async fn shutdown(&self) -> Result<()> {
        self.base_server.shutdown().await
    }

    async fn start(&self, listening: SocketAddr) -> Result<SocketAddr> {
        let (stream, addr) = self.base_server.bind(listening).await?;

        let io_runtime = self.base_server.io_runtime();

        let tls_conf = self.tls.setup()?.map(Arc::new);

        let join_handle = tokio::spawn(self.accept(io_runtime, stream, tls_conf));
        self.base_server.start_with(join_handle).await?;
        Ok(addr)
    }
}
