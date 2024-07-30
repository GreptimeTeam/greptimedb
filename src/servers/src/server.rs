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

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use async_trait::async_trait;
use common_runtime::Runtime;
use common_telemetry::{error, info};
use futures::future::{try_join_all, AbortHandle, AbortRegistration, Abortable};
use snafu::{ensure, ResultExt};
use tokio::sync::{Mutex, RwLock};
use tokio::task::JoinHandle;
use tokio_stream::wrappers::TcpListenerStream;

use crate::error::{self, Result};

pub(crate) type AbortableStream = Abortable<TcpListenerStream>;

pub type ServerHandler = (Box<dyn Server>, SocketAddr);

/// [ServerHandlers] is used to manage the lifecycle of all the services like http or grpc in the GreptimeDB server.
#[derive(Clone, Default)]
pub struct ServerHandlers {
    handlers: Arc<RwLock<HashMap<String, ServerHandler>>>,
}

impl ServerHandlers {
    pub fn new() -> Self {
        Self {
            handlers: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn insert(&self, handler: ServerHandler) {
        let mut handlers = self.handlers.write().await;
        handlers.insert(handler.0.name().to_string(), handler);
    }

    /// Finds the __actual__ bound address of the service by its name.
    ///
    /// This is useful in testing. We can configure the service to bind to port 0 first, then start
    /// the server to get the real bound port number. This way we avoid doing careful assignment of
    /// the port number to the service in the test.
    ///
    /// Note that the address is guaranteed to be correct only after the `start_all` method is
    /// successfully invoked. Otherwise you may find the address to be what you configured before.
    pub async fn addr(&self, name: &str) -> Option<SocketAddr> {
        let handlers = self.handlers.read().await;
        handlers.get(name).map(|x| x.1)
    }

    /// Starts all the managed services. It will block until all the services are started.
    /// And it will set the actual bound address to the service.
    pub async fn start_all(&self) -> Result<()> {
        let mut handlers = self.handlers.write().await;
        try_join_all(handlers.values_mut().map(|(server, addr)| async move {
            let bind_addr = server.start(*addr).await?;
            *addr = bind_addr;
            info!("Service {} is started at {}", server.name(), bind_addr);
            Ok::<(), error::Error>(())
        }))
        .await?;
        Ok(())
    }

    /// Shutdown all the managed services. It will block until all the services are shutdown.
    pub async fn shutdown_all(&self) -> Result<()> {
        // Even though the `shutdown` method in server does not require mut self, we still acquire
        // write lock to pair with `start_all` method.
        let handlers = self.handlers.write().await;
        try_join_all(handlers.values().map(|(server, _)| async move {
            server.shutdown().await?;
            info!("Service {} is shutdown!", server.name());
            Ok::<(), error::Error>(())
        }))
        .await?;
        Ok(())
    }
}

#[async_trait]
pub trait Server: Send + Sync {
    /// Shutdown the server gracefully.
    async fn shutdown(&self) -> Result<()>;

    /// Starts the server and binds on `listening`.
    ///
    /// Caller should ensure `start()` is only invoked once.
    async fn start(&self, listening: SocketAddr) -> Result<SocketAddr>;

    fn name(&self) -> &str;
}

struct AcceptTask {
    // `abort_handle` and `abort_registration` are used in pairs in shutting down the server.
    // They work like sender and receiver for aborting stream. When the server is shutting down,
    // calling `abort_handle.abort()` will "notify" `abort_registration` to stop emitting new
    // elements in the stream.
    abort_handle: AbortHandle,
    abort_registration: Option<AbortRegistration>,

    // A handle holding the TCP accepting task.
    join_handle: Option<JoinHandle<()>>,
}

impl AcceptTask {
    async fn shutdown(&mut self, name: &str) -> Result<()> {
        match self.join_handle.take() {
            Some(join_handle) => {
                self.abort_handle.abort();

                if let Err(error) = join_handle.await {
                    // Couldn't use `error!(e; xxx)` because JoinError doesn't implement ErrorExt.
                    error!(
                        "Unexpected error during shutdown {} server, error: {:?}",
                        name, error
                    );
                } else {
                    info!("{name} server is shutdown.");
                }
                Ok(())
            }
            None => error::InternalSnafu {
                err_msg: format!("{name} server is not started."),
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
                            err_msg: format!("{name} failed to bind addr {addr}"),
                        })?;
                // get actually bond addr in case input addr use port 0
                let addr = listener.local_addr()?;
                info!("{name} server started at {addr}");

                let stream = TcpListenerStream::new(listener);
                let stream = Abortable::new(stream, registration);
                Ok((stream, addr))
            }
            None => error::InternalSnafu {
                err_msg: format!("{name} server has been started."),
            }
            .fail()?,
        }
    }

    fn start_with(&mut self, join_handle: JoinHandle<()>, name: &str) -> Result<()> {
        ensure!(
            self.join_handle.is_none(),
            error::InternalSnafu {
                err_msg: format!("{name} server has been started."),
            }
        );
        let _handle = self.join_handle.get_or_insert(join_handle);
        Ok(())
    }
}

pub(crate) struct BaseTcpServer {
    name: String,
    accept_task: Mutex<AcceptTask>,
    io_runtime: Runtime,
}

impl BaseTcpServer {
    pub(crate) fn create_server(name: impl Into<String>, io_runtime: Runtime) -> Self {
        let (abort_handle, registration) = AbortHandle::new_pair();
        Self {
            name: name.into(),
            accept_task: Mutex::new(AcceptTask {
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

    pub(crate) fn io_runtime(&self) -> Runtime {
        self.io_runtime.clone()
    }
}
