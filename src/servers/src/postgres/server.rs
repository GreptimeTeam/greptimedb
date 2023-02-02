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
use common_telemetry::logging::error;
use common_telemetry::{debug, warn};
use futures::StreamExt;
use pgwire::tokio::process_socket;
use tokio;
use tokio_rustls::TlsAcceptor;

use super::{MakePostgresServerHandler, MakePostgresServerHandlerBuilder};
use crate::auth::UserProviderRef;
use crate::error::Result;
use crate::query_handler::sql::ServerSqlQueryHandlerRef;
use crate::server::{AbortableStream, BaseTcpServer, Server};
use crate::tls::TlsOption;

pub struct PostgresServer {
    base_server: BaseTcpServer,
    make_handler: Arc<MakePostgresServerHandler>,
    tls: TlsOption,
}

impl PostgresServer {
    /// Creates a new Postgres server with provided query_handler and async runtime
    pub fn new(
        query_handler: ServerSqlQueryHandlerRef,
        tls: TlsOption,
        io_runtime: Arc<Runtime>,
        user_provider: Option<UserProviderRef>,
    ) -> PostgresServer {
        let make_handler = Arc::new(
            MakePostgresServerHandlerBuilder::default()
                .query_handler(query_handler.clone())
                .user_provider(user_provider.clone())
                .force_tls(tls.should_force_tls())
                .build()
                .unwrap(),
        );
        PostgresServer {
            base_server: BaseTcpServer::create_server("Postgres", io_runtime),
            make_handler,
            tls,
        }
    }

    fn accept(
        &self,
        io_runtime: Arc<Runtime>,
        accepting_stream: AbortableStream,
        tls_acceptor: Option<Arc<TlsAcceptor>>,
    ) -> impl Future<Output = ()> {
        let handler = self.make_handler.clone();
        accepting_stream.for_each(move |tcp_stream| {
            let io_runtime = io_runtime.clone();
            let tls_acceptor = tls_acceptor.clone();
            let handler = handler.clone();

            async move {
                match tcp_stream {
                    Err(error) => error!("Broken pipe: {}", error), // IoError doesn't impl ErrorExt.
                    Ok(io_stream) => {
                        match io_stream.peer_addr() {
                            Ok(addr) => debug!("PostgreSQL client coming from {}", addr),
                            Err(e) => warn!("Failed to get PostgreSQL client addr, err: {}", e),
                        }

                        io_runtime.spawn(process_socket(
                            io_stream,
                            tls_acceptor.clone(),
                            handler.clone(),
                            handler.clone(),
                            handler,
                        ));
                    }
                };
            }
        })
    }
}

#[async_trait]
impl Server for PostgresServer {
    async fn shutdown(&self) -> Result<()> {
        self.base_server.shutdown().await
    }

    async fn start(&self, listening: SocketAddr) -> Result<SocketAddr> {
        let (stream, addr) = self.base_server.bind(listening).await?;

        debug!("Starting PostgreSQL with TLS option: {:?}", self.tls);
        let tls_acceptor = self
            .tls
            .setup()?
            .map(|server_conf| Arc::new(TlsAcceptor::from(Arc::new(server_conf))));

        let io_runtime = self.base_server.io_runtime();
        let join_handle = tokio::spawn(self.accept(io_runtime, stream, tls_acceptor));

        self.base_server.start_with(join_handle).await?;
        Ok(addr)
    }
}
