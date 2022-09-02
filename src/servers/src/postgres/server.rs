use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;

use async_trait::async_trait;
use common_runtime::Runtime;
use common_telemetry::logging::{error, info};
use futures::future::AbortHandle;
use futures::future::AbortRegistration;
use futures::future::Abortable;
use futures::StreamExt;
use pgwire::api::auth::noop::NoopStartupHandler;
use pgwire::tokio::process_socket;
use snafu::prelude::*;
use tokio;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::TcpListenerStream;

use crate::error::{self, Result};
use crate::postgres::handler::PostgresServerHandler;
use crate::query_handler::SqlQueryHandlerRef;
use crate::server::Server;

pub struct PostgresServer {
    // See MySQL module for usage of these types
    abort_handle: AbortHandle,
    abort_registration: Option<AbortRegistration>,

    // A handle holding the TCP accepting task.
    join_handle: Option<JoinHandle<()>>,

    startup_handler: Arc<NoopStartupHandler>,
    query_handler: Arc<PostgresServerHandler>,
    io_runtime: Arc<Runtime>,
}

impl PostgresServer {
    /// Creates a new Postgres server with provided query_handler and async runtime
    pub fn new(query_handler: SqlQueryHandlerRef, io_runtime: Arc<Runtime>) -> PostgresServer {
        let (abort_handle, registration) = AbortHandle::new_pair();
        let postgres_handler = Arc::new(PostgresServerHandler::new(query_handler));
        let startup_handler = Arc::new(NoopStartupHandler);
        PostgresServer {
            abort_handle,
            abort_registration: Some(registration),
            join_handle: None,

            startup_handler,
            query_handler: postgres_handler,

            io_runtime,
        }
    }

    async fn bind(addr: SocketAddr) -> Result<(TcpListenerStream, SocketAddr)> {
        let listener = tokio::net::TcpListener::bind(addr)
            .await
            .context(error::TokioIoSnafu {
                err_msg: format!("Failed to bind addr {}", addr),
            })?;
        // get actually bond addr in case input addr use port 0
        let addr = listener.local_addr()?;
        info!("Postgres server is bound to {}", addr);
        Ok((TcpListenerStream::new(listener), addr))
    }

    fn accept(&self, accepting_stream: Abortable<TcpListenerStream>) -> impl Future<Output = ()> {
        let io_runtime = self.io_runtime.clone();
        let auth_handler = self.startup_handler.clone();
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
    async fn shutdown(&mut self) -> Result<()> {
        match self.join_handle.take() {
            Some(join_handle) => {
                self.abort_handle.abort();

                if let Err(error) = join_handle.await {
                    // Couldn't use `error!(e; xxx)` as JoinError doesn't implement ErrorExt.
                    error!(
                        "Unexpected error during shutdown Postgres server, error: {}",
                        error
                    );
                } else {
                    info!("Postgres server is shutdown.")
                }
                Ok(())
            }
            None => error::InternalSnafu {
                err_msg: "Postgres server is not started.",
            }
            .fail()?,
        }
    }

    async fn start(&mut self, listening: SocketAddr) -> Result<SocketAddr> {
        match self.abort_registration.take() {
            Some(registration) => {
                let (stream, listener) = Self::bind(listening).await?;
                let stream = Abortable::new(stream, registration);
                self.join_handle = Some(tokio::spawn(self.accept(stream)));
                Ok(listener)
            }
            None => error::InternalSnafu {
                err_msg: "Postgres server has been started.",
            }
            .fail()?,
        }
    }
}
