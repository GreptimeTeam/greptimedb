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
use opensrv_mysql::AsyncMysqlIntermediary;
use snafu::prelude::*;
use tokio;
use tokio::net::TcpStream;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::TcpListenerStream;

use crate::error::{self, Result};
use crate::mysql::handler::MysqlInstanceShim;
use crate::query_handler::SqlQueryHandlerRef;
use crate::server::Server;

pub struct MysqlServer {
    // `abort_handle` and `abort_registration` are used in pairs in shutting down MySQL server.
    // They work like sender and receiver for aborting stream. When the server is shutting down,
    // calling `abort_handle.abort()` will "notify" `abort_registration` to stop emitting new
    // elements in the stream.
    abort_handle: AbortHandle,
    abort_registration: Option<AbortRegistration>,

    // A handle holding the TCP accepting task.
    join_handle: Option<JoinHandle<()>>,

    query_handler: SqlQueryHandlerRef,
    io_runtime: Arc<Runtime>,
}

impl MysqlServer {
    /// Creates a new MySQL server with provided [MysqlInstance] and [Runtime].
    pub fn create_server(
        query_handler: SqlQueryHandlerRef,
        io_runtime: Arc<Runtime>,
    ) -> Box<dyn Server> {
        let (abort_handle, registration) = AbortHandle::new_pair();
        Box::new(MysqlServer {
            abort_handle,
            abort_registration: Some(registration),
            join_handle: None,
            query_handler,
            io_runtime,
        })
    }

    async fn bind(addr: SocketAddr) -> Result<(TcpListenerStream, SocketAddr)> {
        let listener = tokio::net::TcpListener::bind(addr)
            .await
            .context(error::TokioIoSnafu {
                err_msg: format!("Failed to bind addr {}", addr),
            })?;
        // get actually bond addr in case input addr use port 0
        let addr = listener.local_addr()?;
        info!("MySQL server is bound to {}", addr);
        Ok((TcpListenerStream::new(listener), addr))
    }

    fn accept(&self, accepting_stream: Abortable<TcpListenerStream>) -> impl Future<Output = ()> {
        let io_runtime = self.io_runtime.clone();
        let query_handler = self.query_handler.clone();
        accepting_stream.for_each(move |tcp_stream| {
            let io_runtime = io_runtime.clone();
            let query_handler = query_handler.clone();
            async move {
                match tcp_stream {
                    Err(error) => error!("Broken pipe: {}", error),
                    Ok(io_stream) => {
                        if let Err(error) = Self::handle(io_stream, io_runtime, query_handler) {
                            error!("Unexpected error when handling TcpStream: {:?}", error);
                        };
                    }
                };
            }
        })
    }

    pub fn handle(
        stream: TcpStream,
        io_runtime: Arc<Runtime>,
        query_handler: SqlQueryHandlerRef,
    ) -> Result<()> {
        info!("MySQL connection coming from: {}", stream.peer_addr()?);
        let shim = MysqlInstanceShim::create(query_handler);
        // TODO(LFC): Relate "handler" with MySQL session; also deal with panics there.
        let _handler = io_runtime.spawn(AsyncMysqlIntermediary::run_on(shim, stream));
        Ok(())
    }
}

#[async_trait]
impl Server for MysqlServer {
    async fn shutdown(&mut self) -> Result<()> {
        match self.join_handle.take() {
            Some(join_handle) => {
                self.abort_handle.abort();

                if let Err(error) = join_handle.await {
                    error!("Unexpected error during shutdown MySQL server: {}", error);
                } else {
                    info!("MySQL server is shutdown.")
                }
                Ok(())
            }
            None => error::InternalSnafu {
                err_msg: "MySQL server is not started.",
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
                err_msg: "MySQL server has been started.",
            }
            .fail()?,
        }
    }
}
