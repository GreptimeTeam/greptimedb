use std::net::SocketAddr;
use std::sync::Arc;

use async_trait::async_trait;
use common_runtime::Runtime;
use common_telemetry::logging::{error, info};
use futures::future::{AbortHandle, AbortRegistration, Abortable};
use snafu::ResultExt;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::TcpListenerStream;

use crate::error::{self, Result};

pub(crate) type AbortableStream = Abortable<TcpListenerStream>;

#[async_trait]
pub trait Server: Send {
    /// Shutdown the server gracefully.
    async fn shutdown(&self) -> Result<()>;

    /// Starts the server and binds on `listening`.
    ///
    /// Caller should ensure `start()` is only invoked once.
    async fn start(&self, listening: SocketAddr) -> Result<SocketAddr>;
}

struct AccpetTask {
    // `abort_handle` and `abort_registration` are used in pairs in shutting down the server.
    // They work like sender and receiver for aborting stream. When the server is shutting down,
    // calling `abort_handle.abort()` will "notify" `abort_registration` to stop emitting new
    // elements in the stream.
    abort_handle: AbortHandle,
    abort_registration: Option<AbortRegistration>,

    // A handle holding the TCP accepting task.
    join_handle: Option<JoinHandle<()>>,
}

impl AccpetTask {
    async fn shutdown(&mut self, name: &str) -> Result<()> {
        match self.join_handle.take() {
            Some(join_handle) => {
                self.abort_handle.abort();

                if let Err(error) = join_handle.await {
                    // Couldn't use `error!(e; xxx)` because JoinError doesn't implement ErrorExt.
                    error!(
                        "Unexpected error during shutdown {} server, error: {}",
                        name, error
                    );
                } else {
                    info!("{} server is shutdown.", name);
                }
                Ok(())
            }
            None => error::InternalSnafu {
                err_msg: format!("{} server is not started.", name),
            }
            .fail()?,
        }
    }

    async fn bind(
        &mut self,
        addr: SocketAddr,
        name: &str,
    ) -> Result<(Abortable<TcpListenerStream>, SocketAddr)> {
        match self.abort_registration.take() {
            Some(registration) => {
                let listener =
                    tokio::net::TcpListener::bind(addr)
                        .await
                        .context(error::TokioIoSnafu {
                            err_msg: format!("{} failed to bind addr {}", name, addr),
                        })?;
                // get actually bond addr in case input addr use port 0
                let addr = listener.local_addr()?;
                info!("{} server started at {}", name, addr);

                let stream = TcpListenerStream::new(listener);
                let stream = Abortable::new(stream, registration);
                Ok((stream, addr))
            }
            None => error::InternalSnafu {
                err_msg: format!("{} server has been started.", name),
            }
            .fail()?,
        }
    }

    fn start_with(&mut self, join_handle: JoinHandle<()>, name: &str) -> Result<()> {
        if self.join_handle.is_some() {
            return error::InternalSnafu {
                err_msg: format!("{} server has been started.", name),
            }
            .fail();
        }
        let _ = self.join_handle.insert(join_handle);

        Ok(())
    }
}

pub(crate) struct BaseTcpServer {
    name: String,
    accept_task: Mutex<AccpetTask>,
    io_runtime: Arc<Runtime>,
}

impl BaseTcpServer {
    pub(crate) fn create_server(name: impl Into<String>, io_runtime: Arc<Runtime>) -> Self {
        let (abort_handle, registration) = AbortHandle::new_pair();
        Self {
            name: name.into(),
            accept_task: Mutex::new(AccpetTask {
                abort_handle,
                abort_registration: Some(registration),
                join_handle: None,
            }),
            io_runtime,
        }
    }

    pub(crate) async fn shutdown(&self) -> Result<()> {
        let mut task = self.accept_task.lock().await;
        task.shutdown(&self.name).await
    }

    pub(crate) async fn bind(
        &self,
        addr: SocketAddr,
    ) -> Result<(Abortable<TcpListenerStream>, SocketAddr)> {
        let mut task = self.accept_task.lock().await;
        task.bind(addr, &self.name).await
    }

    pub(crate) async fn start_with(&self, join_handle: JoinHandle<()>) -> Result<()> {
        let mut task = self.accept_task.lock().await;
        task.start_with(join_handle, &self.name)
    }

    pub(crate) fn io_runtime(&self) -> Arc<Runtime> {
        self.io_runtime.clone()
    }
}
