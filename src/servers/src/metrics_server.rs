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

use std::default::Default;
use std::net::SocketAddr;

use async_trait::async_trait;
use common_telemetry::info;
use common_telemetry::metric::try_handle;
use hyper::server::Server;
use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Response, StatusCode};
use snafu::{ensure, ResultExt};
use tokio::sync::oneshot::{self, Sender};
use tokio::sync::Mutex;

use crate::error::{AlreadyStartedSnafu, HyperSnafu, Result};
use crate::server::Server as ServerTrait;

pub const METRIC_SERVER: &str = "METRIC_SERVER";

/// a server that serves metrics
/// only start when datanode starts in distributed mode
#[derive(Default)]
pub struct MetricsServer {
    shutdown_tx: Mutex<Option<Sender<()>>>,
}

#[async_trait]
impl ServerTrait for MetricsServer {
    async fn start(&self, listening: SocketAddr) -> Result<SocketAddr> {
        self.start(listening).await
    }
    async fn shutdown(&self) -> Result<()> {
        let mut shutdown_tx = self.shutdown_tx.lock().await;
        if let Some(tx) = shutdown_tx.take() {
            if tx.send(()).is_err() {
                info!("Receiver dropped, the metrics server has already existed");
            }
        }
        info!("Shutdown metrics server");

        Ok(())
    }

    fn name(&self) -> &str {
        METRIC_SERVER
    }
}

impl MetricsServer {
    pub fn new() -> Self {
        Self {
            shutdown_tx: Mutex::new(None),
        }
    }
    pub async fn start(&self, listening: SocketAddr) -> Result<SocketAddr> {
        let (tx, rx) = oneshot::channel();
        let server = {
            let mut shutdown_tx = self.shutdown_tx.lock().await;
            ensure!(
                shutdown_tx.is_none(),
                AlreadyStartedSnafu {
                    server: METRIC_SERVER
                }
            );
            *shutdown_tx = Some(tx);
            info!("metrics server bound to {}", &listening);
            Server::try_bind(&listening).context(HyperSnafu)?
        };
        async move {
            let make_svc = make_service_fn(move |_| async move {
                Ok::<_, hyper::Error>(service_fn(move |request| async move {
                    let handle = try_handle().unwrap();
                    if request.uri().path() == "/metrics" && request.method() == hyper::Method::GET
                    {
                        let output = handle.render();
                        Ok::<_, hyper::Error>(Response::new(Body::from(output)))
                    } else {
                        Ok::<_, hyper::Error>(
                            Response::builder()
                                .status(StatusCode::NOT_FOUND)
                                .body(Body::empty())
                                .expect("static response is valid"),
                        )
                    }
                }))
            });
            let server = server.serve(make_svc);
            server
                .with_graceful_shutdown(async {
                    rx.await.ok();
                })
                .await
        }
        .await
        .context(HyperSnafu)?;

        Ok(listening)
    }
}
