use std::collections::HashMap;
use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;

use async_trait::async_trait;
use common_runtime::Runtime;
use common_telemetry::logging::error;
use futures::StreamExt;
use pgwire::api::auth::{self, ServerParameterProvider, StartupHandler};
use pgwire::api::ClientInfo;
use pgwire::error::PgWireResult;
use pgwire::messages::PgWireFrontendMessage;
use pgwire::tokio::process_socket;
use tokio;

use crate::error::Result;
use crate::postgres::handler::PostgresServerHandler;
use crate::query_handler::SqlQueryHandlerRef;
use crate::server::{AbortableStream, BaseTcpServer, Server};

struct SimpleStartupHandler;

#[async_trait]
impl StartupHandler for SimpleStartupHandler {
    async fn on_startup<C>(
        &self,
        client: &mut C,
        message: &pgwire::messages::PgWireFrontendMessage,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + futures::Sink<pgwire::messages::PgWireBackendMessage> + Unpin + Send,
        C::Error: std::fmt::Debug,
        pgwire::error::PgWireError:
            From<<C as futures::Sink<pgwire::messages::PgWireBackendMessage>>::Error>,
    {
        if let PgWireFrontendMessage::Startup(ref startup) = message {
            auth::save_startup_parameters_to_metadata(client, startup);
            auth::finish_authentication(client, &GreptimeDBStartupParameters::new()).await;
        }

        Ok(())
    }
}

struct GreptimeDBStartupParameters {
    version: &'static str,
}

impl GreptimeDBStartupParameters {
    fn new() -> GreptimeDBStartupParameters {
        GreptimeDBStartupParameters {
            version: env!("CARGO_PKG_VERSION"),
        }
    }
}

impl ServerParameterProvider for GreptimeDBStartupParameters {
    fn server_parameters<C>(&self, _client: &C) -> Option<HashMap<String, String>>
    where
        C: ClientInfo,
    {
        let mut params = HashMap::new();
        params.insert("server_version".to_owned(), self.version.to_owned());

        Some(params)
    }
}

pub struct PostgresServer {
    base_server: BaseTcpServer,
    auth_handler: Arc<SimpleStartupHandler>,
    query_handler: Arc<PostgresServerHandler>,
}

impl PostgresServer {
    /// Creates a new Postgres server with provided query_handler and async runtime
    pub fn new(query_handler: SqlQueryHandlerRef, io_runtime: Arc<Runtime>) -> PostgresServer {
        let postgres_handler = Arc::new(PostgresServerHandler::new(query_handler));
        let startup_handler = Arc::new(SimpleStartupHandler);
        PostgresServer {
            base_server: BaseTcpServer::create_server("Postgres", io_runtime),
            auth_handler: startup_handler,
            query_handler: postgres_handler,
        }
    }

    fn accept(
        &self,
        io_runtime: Arc<Runtime>,
        accepting_stream: AbortableStream,
    ) -> impl Future<Output = ()> {
        let auth_handler = self.auth_handler.clone();
        let query_handler = self.query_handler.clone();

        accepting_stream.for_each(move |tcp_stream| {
            let io_runtime = io_runtime.clone();
            let auth_handler = auth_handler.clone();
            let query_handler = query_handler.clone();

            async move {
                match tcp_stream {
                    Err(error) => error!("Broken pipe: {}", error), // IoError doesn't impl ErrorExt.
                    Ok(io_stream) => {
                        io_runtime.spawn(async move {
                            process_socket(
                                io_stream,
                                auth_handler.clone(),
                                query_handler.clone(),
                                query_handler.clone(),
                            )
                            .await;
                        });
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

        let io_runtime = self.base_server.io_runtime();
        let join_handle = tokio::spawn(self.accept(io_runtime, stream));
        self.base_server.start_with(join_handle).await?;
        Ok(addr)
    }
}
