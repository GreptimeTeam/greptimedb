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

use std::any::Any;
use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;

use ::auth::UserProviderRef;
use async_trait::async_trait;
use catalog::process_manager::ProcessManagerRef;
use common_runtime::Runtime;
use common_runtime::runtime::RuntimeTrait;
use common_telemetry::{debug, warn};
use futures::StreamExt;
use pgwire::tokio::process_socket;
use tokio_rustls::TlsAcceptor;

use crate::error::Result;
use crate::postgres::{MakePostgresServerHandler, MakePostgresServerHandlerBuilder};
use crate::query_handler::CopyInHandlerRef;
use crate::query_handler::InfluxdbLineProtocolHandlerRef;
use crate::query_handler::OpenTelemetryProtocolHandlerRef;
use crate::query_handler::sql::ServerSqlQueryHandlerRef;
use crate::server::{AbortableStream, BaseTcpServer, Server};
use crate::tls::ReloadableTlsServerConfig;

pub struct PostgresServer {
    base_server: BaseTcpServer,
    make_handler: Arc<MakePostgresServerHandler>,
    tls_server_config: Arc<ReloadableTlsServerConfig>,
    keep_alive_secs: u64,
    batching_enabled: bool,
    influxdb_handler: Option<InfluxdbLineProtocolHandlerRef>,
    otlp_metrics_handler: Option<OpenTelemetryProtocolHandlerRef>,
    bind_addr: Option<SocketAddr>,
    process_manager: Option<ProcessManagerRef>,
}

impl PostgresServer {
    /// Creates a new Postgres server with provided query_handler and async runtime.
    ///
    /// `query_handler` and `copy_in_handler` are expected to be handles to
    /// the same frontend instance (the SQL and COPY FROM STDIN facets of
    /// one object); they are separate parameters only because they are
    /// separate traits.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        query_handler: ServerSqlQueryHandlerRef,
        copy_in_handler: CopyInHandlerRef,
        force_tls: bool,
        tls_server_config: Arc<ReloadableTlsServerConfig>,
        keep_alive_secs: u64,
        io_runtime: Runtime,
        user_provider: Option<UserProviderRef>,
        process_manager: Option<ProcessManagerRef>,
    ) -> PostgresServer {
        let make_handler = Arc::new(
            MakePostgresServerHandlerBuilder::default()
                .query_handler(query_handler.clone())
                .copy_in_handler(copy_in_handler)
                .user_provider(user_provider.clone())
                .force_tls(force_tls)
                .build()
                .unwrap(),
        );
        PostgresServer {
            base_server: BaseTcpServer::create_server("Postgres", io_runtime),
            make_handler,
            tls_server_config,
            keep_alive_secs,
            batching_enabled: false,
            influxdb_handler: None,
            otlp_metrics_handler: None,
            bind_addr: None,
            process_manager,
        }
    }

    /// Enables ordinary-table batching for connections accepted by this server.
    pub fn with_batching_enabled(mut self, enabled: bool) -> Self {
        self.batching_enabled = enabled;
        self
    }

    /// Wires the influxdb line-protocol handler used by table-less
    /// `COPY FROM STDIN WITH (FORMAT influxdb)` flushes — the same
    /// handler the HTTP influxdb write endpoints use, so a flushed batch
    /// runs through the identical pipeline.
    pub fn with_influxdb_handler(mut self, handler: InfluxdbLineProtocolHandlerRef) -> Self {
        self.influxdb_handler = Some(handler);
        self
    }

    /// Wires the OTLP metrics handler used by table-less
    /// `COPY FROM STDIN WITH (FORMAT otlp)` — the same handler the
    /// OTLP/HTTP endpoint uses, so the payload runs through the
    /// identical pipeline.
    pub fn with_otlp_metrics_handler(mut self, handler: OpenTelemetryProtocolHandlerRef) -> Self {
        self.otlp_metrics_handler = Some(handler);
        self
    }

    fn accept(
        &self,
        io_runtime: Runtime,
        accepting_stream: AbortableStream,
    ) -> impl Future<Output = ()> + use<> {
        // Assemble the per-connection handler directly, including any
        // `with_*` wiring applied after construction (the influxdb
        // line-protocol handler). Constructing the struct — instead of
        // going through the builder — keeps this infallible: no
        // `.build().unwrap()` on the hot accept path.
        let make_handler = &self.make_handler;
        let handler_maker = Arc::new(MakePostgresServerHandler {
            query_handler: make_handler.query_handler.clone(),
            copy_in_handler: make_handler.copy_in_handler.clone(),
            influxdb_handler: self.influxdb_handler.clone(),
            otlp_metrics_handler: self.otlp_metrics_handler.clone(),
            user_provider: make_handler.user_provider.clone(),
            param_provider: make_handler.param_provider.clone(),
            force_tls: make_handler.force_tls,
        });
        let batching_enabled = self.batching_enabled;
        let tls_server_config = self.tls_server_config.clone();
        let process_manager = self.process_manager.clone();
        accepting_stream.for_each(move |tcp_stream| {
            let io_runtime = io_runtime.clone();
            let tls_acceptor = tls_server_config.get_config().map(TlsAcceptor::from);
            let handler_maker = handler_maker.clone();
            let process_id = process_manager.as_ref().map(|p| p.next_id()).unwrap_or(0);

            async move {
                match tcp_stream {
                    Err(error) => debug!("Broken pipe: {}", error), // IoError doesn't impl ErrorExt.
                    Ok(io_stream) => {
                        let addr = match io_stream.peer_addr() {
                            Ok(addr) => {
                                debug!("PostgreSQL client coming from {}", addr);
                                Some(addr)
                            }
                            Err(e) => {
                                warn!(e; "Failed to get PostgreSQL client addr");
                                None
                            }
                        };

                        let _handle = io_runtime.spawn(async move {
                            crate::metrics::METRIC_POSTGRES_CONNECTIONS.inc();
                            let pg_handler =
                                Arc::new(handler_maker.make(addr, process_id, batching_enabled));
                            let r =
                                process_socket(io_stream, tls_acceptor.clone(), pg_handler).await;
                            crate::metrics::METRIC_POSTGRES_CONNECTIONS.dec();
                            r
                        });
                    }
                };
            }
        })
    }
}

pub const POSTGRES_SERVER: &str = "POSTGRES_SERVER";

#[async_trait]
impl Server for PostgresServer {
    async fn shutdown(&self) -> Result<()> {
        self.base_server.shutdown().await
    }

    async fn start(&mut self, listening: SocketAddr) -> Result<()> {
        let (stream, addr) = self
            .base_server
            .bind(listening, self.keep_alive_secs)
            .await?;

        let io_runtime = self.base_server.io_runtime();
        let join_handle = common_runtime::spawn_global(self.accept(io_runtime, stream));

        self.base_server.start_with(join_handle).await?;

        self.bind_addr = Some(addr);
        Ok(())
    }

    fn name(&self) -> &str {
        POSTGRES_SERVER
    }

    fn bind_addr(&self) -> Option<SocketAddr> {
        self.bind_addr
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
