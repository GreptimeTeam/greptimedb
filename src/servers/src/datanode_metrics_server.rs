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
use hyper::server::conn::AddrStream;
use hyper::server::Server;
use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Response, StatusCode};
use snafu::ResultExt;

use crate::error::{HyperSnafu, Result};
use crate::server::Server as ServerTrait;

pub const METRIC_SERVER: &str = "METRIC_SERVER";

/// a server that serves metrics
/// only start when datanode starts in distributed mode
#[derive(Clone, Default)]
pub struct DatanodeMetricsServer {
    allowed_addresses: Option<Vec<SocketAddr>>,
}

#[async_trait]
impl ServerTrait for DatanodeMetricsServer {
    async fn start(&self, listening: SocketAddr) -> Result<SocketAddr> {
        self.start(listening).await
    }
    async fn shutdown(&self) -> Result<()> {
        Ok(())
    }

    fn name(&self) -> &str {
        METRIC_SERVER
    }
}

impl DatanodeMetricsServer {
    pub fn new() -> Self {
        Self {
            ..Default::default()
        }
    }
    pub async fn start(&self, listening: SocketAddr) -> Result<SocketAddr> {
        let server = Server::try_bind(&listening).context(HyperSnafu)?;
        info!("metrics server bound to {}", &listening);
        let _ = async move {
            let make_svc = make_service_fn(move |socket: &AddrStream| {
                let remote_addr = socket.remote_addr().ip();

                // If the allowlist is empty, the request is allowed.  Otherwise, it must
                // match one of the entries in the allowlist or it will be denied.
                let is_allowed = self.allowed_addresses.as_ref().map_or(true, |addresses| {
                    addresses.iter().any(|address| address.ip() == remote_addr)
                });

                async move {
                    Ok::<_, hyper::Error>(service_fn(move |request| async move {
                        if is_allowed {
                            let handle = try_handle().unwrap();
                            if request.uri().path() == "/metrics" {
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
                        } else {
                            Ok::<_, hyper::Error>(
                                Response::builder()
                                    .status(StatusCode::FORBIDDEN)
                                    .body(Body::empty())
                                    .expect("static response is valid"),
                            )
                        }
                    }))
                }
            });
            server.serve(make_svc).await
        }
        .await;

        Ok(listening)
    }
}
