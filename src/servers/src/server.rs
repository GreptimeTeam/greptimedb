use std::net::SocketAddr;
use std::sync::Arc;

use async_trait::async_trait;
use common_runtime::Runtime;
use common_telemetry::logging::{error, info};
use futures::future::AbortRegistration;
use futures::future::{AbortHandle, Abortable};
use snafu::ResultExt;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::TcpListenerStream;

use crate::error::{self, Result};

pub(crate) type AbortableStream = Abortable<TcpListenerStream>;

#[async_trait]
pub trait Server: Send {
    async fn shutdown(&mut self) -> Result<()>;
    async fn start(&mut self, listening: SocketAddr) -> Result<SocketAddr>;
}

pub(crate) struct BaseTcpServer {
    name: String,

    // `abort_handle` and `abort_registration` are used in pairs in shutting down the server.
    // They work like sender and receiver for aborting stream. When the server is shutting down,
    // calling `abort_handle.abort()` will "notify" `abort_registration` to stop emitting new
    // elements in the stream.
    abort_handle: AbortHandle,
    abort_registration: Option<AbortRegistration>,

    // A handle holding the TCP accepting task.
    join_handle: Option<JoinHandle<()>>,

    io_runtime: Arc<Runtime>,
}

impl BaseTcpServer {
    pub(crate) fn create_server(name: impl Into<String>, io_runtime: Arc<Runtime>) -> Self {
        let (abort_handle, registration) = AbortHandle::new_pair();
        Self {
            name: name.into(),
            abort_handle,
            abort_registration: Some(registration),
            join_handle: None,
            io_runtime,
        }
    }

    pub(crate) async fn shutdown(&mut self) -> Result<()> {
        match self.join_handle.take() {
            Some(join_handle) => {
                self.abort_handle.abort();

                if let Err(error) = join_handle.await {
                    // Couldn't use `error!(e; xxx)` because JoinError doesn't implement ErrorExt.
                    error!(
                        "Unexpected error during shutdown {} server, error: {}",
                        &self.name, error
                    );
                } else {
                    info!("{} server is shutdown.", &self.name);
                }
                Ok(())
            }
            None => error::InternalSnafu {
                err_msg: format!("{} server is not started.", &self.name),
            }
            .fail()?,
        }
    }

    pub(crate) async fn bind(
        &mut self,
        addr: SocketAddr,
    ) -> Result<(Abortable<TcpListenerStream>, SocketAddr)> {
        match self.abort_registration.take() {
            Some(registration) => {
                let listener =
                    tokio::net::TcpListener::bind(addr)
                        .await
                        .context(error::TokioIoSnafu {
                            err_msg: format!("Failed to bind addr {}", addr),
                        })?;
                // get actually bond addr in case input addr use port 0
                let addr = listener.local_addr()?;
                info!("{} server started at {}", &self.name, addr);

                let stream = TcpListenerStream::new(listener);
                let stream = Abortable::new(stream, registration);
                Ok((stream, addr))
            }
            None => error::InternalSnafu {
                err_msg: format!("{} server has been started.", &self.name),
            }
            .fail()?,
        }
    }

    pub(crate) fn start_with(&mut self, join_handle: JoinHandle<()>) -> Result<()> {
        if self.join_handle.is_some() {
            return error::InternalSnafu {
                err_msg: format!("{} server has been started.", &self.name),
            }
            .fail();
        }
        let _ = self.join_handle.insert(join_handle);
        Ok(())
    }

    pub(crate) fn io_runtime(&self) -> Arc<Runtime> {
        self.io_runtime.clone()
    }
}
