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
