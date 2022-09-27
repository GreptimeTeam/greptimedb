pub mod codec;
pub mod connection;
mod handler;

use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;

use async_trait::async_trait;
use common_runtime::Runtime;
use common_telemetry::logging::error;
use futures::StreamExt;
use tokio::sync::broadcast;

use crate::error::Result;
use crate::opentsdb::connection::Connection;
use crate::opentsdb::handler::Handler;
use crate::query_handler::OpentsdbProtocolHandlerRef;
use crate::server::{AbortableStream, BaseTcpServer, Server};
use crate::shutdown::Shutdown;

pub struct OpentsdbServer {
    base_server: BaseTcpServer,
    query_handler: OpentsdbProtocolHandlerRef,

    /// Broadcasts a shutdown signal to all active connections.
    ///
    /// When a connection task is spawned, it is passed a broadcast receiver handle. We can send
    /// a `()` value via `notify_shutdown` or just drop `notify_shutdown`, then each active
    /// connection receives it, reaches a safe terminal state, and completes the task.
    notify_shutdown: Option<broadcast::Sender<()>>,
}

impl OpentsdbServer {
    pub fn create_server(
        query_handler: OpentsdbProtocolHandlerRef,
        io_runtime: Arc<Runtime>,
    ) -> Box<dyn Server> {
        // When the provided `shutdown` future completes, we must send a shutdown
        // message to all active connections. We use a broadcast channel for this
        // purpose. The call below ignores the receiver of the broadcast pair, and when
        // a receiver is needed, the subscribe() method on the sender is used to create
        // one.
        let (notify_shutdown, _) = broadcast::channel(1);

        Box::new(OpentsdbServer {
            base_server: BaseTcpServer::create_server("OpenTSDB", io_runtime),
            query_handler,
            notify_shutdown: Some(notify_shutdown),
        })
    }

    fn accept(
        &self,
        io_runtime: Arc<Runtime>,
        stream: AbortableStream,
    ) -> impl Future<Output = ()> {
        let query_handler = self.query_handler.clone();
        let notify_shutdown = self
            .notify_shutdown
            .clone()
            .expect("`notify_shutdown` must be present when accepting connection!");
        stream.for_each(move |stream| {
            let io_runtime = io_runtime.clone();
            let query_handler = query_handler.clone();
            let shutdown = Shutdown::new(notify_shutdown.subscribe());
            async move {
                match stream {
                    Ok(stream) => {
                        let connection = Connection::new(stream);
                        let mut handler = Handler::new(query_handler, connection, shutdown);

                        let _ = io_runtime.spawn(async move {
                            if let Err(e) = handler.run().await {
                                error!(e; "Unexpected error when handling OpenTSDB connection");
                            }
                        });
                    }
                    Err(error) => error!("Broken pipe: {}", error), // IoError doesn't impl ErrorExt.
                };
            }
        })
    }
}

#[async_trait]
impl Server for OpentsdbServer {
    async fn shutdown(&mut self) -> Result<()> {
        self.base_server.shutdown().await?;
        drop(self.notify_shutdown.take());
        Ok(())
    }

    async fn start(&mut self, listening: SocketAddr) -> Result<SocketAddr> {
        let (stream, addr) = self.base_server.bind(listening).await?;

        let io_runtime = self.base_server.io_runtime();
        let join_handle = tokio::spawn(self.accept(io_runtime, stream));
        self.base_server.start_with(join_handle)?;
        Ok(addr)
    }
}
