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

use std::sync::Arc;

use api::v1::region::cancel_compaction_response::Result as CancelCompactionResult;
use api::v1::region::region_server::Region as RegionServer;
use api::v1::region::{
    CancelCompactionRequest, CancelCompactionResponse, RegionRequest, RegionResponse,
    region_request,
};
use api::v1::{ResponseHeader, Status as ResponseStatus};
use async_trait::async_trait;
use common_error::ext::ErrorExt;
use common_error::status_code::StatusCode;
use common_runtime::Runtime;
use common_runtime::runtime::RuntimeTrait;
use common_telemetry::tracing::info_span;
use common_telemetry::tracing_context::{FutureExt, TracingContext};
use common_telemetry::{debug, error, warn};
use snafu::{OptionExt, ResultExt};
use tonic::{Request, Response, Status};

use crate::error::{InvalidQuerySnafu, JoinTaskSnafu, Result};
use crate::grpc::{TonicResult, cancellation};

#[async_trait]
pub trait RegionServerHandler: Send + Sync {
    async fn handle(&self, request: region_request::Body) -> Result<RegionResponse>;

    /// Requests cooperative cancellation of one identified compaction cycle.
    async fn cancel_compaction(
        &self,
        _request: CancelCompactionRequest,
    ) -> Result<CancelCompactionResponse> {
        Ok(CancelCompactionResponse {
            header: Some(ResponseHeader {
                status: Some(ResponseStatus {
                    status_code: StatusCode::Success as u32,
                    ..Default::default()
                }),
            }),
            result: CancelCompactionResult::Unsupported as i32,
        })
    }

    async fn handle_region_request(&self, request: RegionRequest) -> Result<RegionResponse> {
        let body = request.body.context(InvalidQuerySnafu {
            reason: "Expecting non-empty region request body.",
        })?;
        self.handle(body).await
    }
}

pub type RegionServerHandlerRef = Arc<dyn RegionServerHandler>;

#[derive(Clone)]
pub struct RegionServerRequestHandler {
    handler: Arc<dyn RegionServerHandler>,
    runtime: Runtime,
}

impl RegionServerRequestHandler {
    pub fn new(handler: Arc<dyn RegionServerHandler>, runtime: Runtime) -> Self {
        Self { handler, runtime }
    }

    async fn cancel_compaction(
        &self,
        request: CancelCompactionRequest,
    ) -> Result<CancelCompactionResponse> {
        let tracing_context = TracingContext::from_w3c(
            &request
                .header
                .as_ref()
                .context(InvalidQuerySnafu {
                    reason: "Expecting non-empty region request header.",
                })?
                .tracing_context,
        );
        let handler = self.handler.clone();
        // Keeps cancellation requests alive if the caller disconnects, as with Handle.
        let handle = self.runtime.spawn(async move {
            handler
                .cancel_compaction(request)
                .trace(
                    tracing_context
                        .attach(info_span!("RegionServerRequestHandler::cancel_compaction")),
                )
                .await
                .map_err(|e| {
                    if e.status_code().should_log_error() {
                        error!(e; "Failed to cancel compaction");
                    } else {
                        debug!("Failed to cancel compaction, err: {}", e);
                    }
                    e
                })
        });
        handle.await.context(JoinTaskSnafu)?
    }

    async fn handle(&self, request: RegionRequest) -> Result<RegionResponse> {
        let tracing_context = TracingContext::from_w3c(
            &request
                .header
                .as_ref()
                .context(InvalidQuerySnafu {
                    reason: "Expecting non-empty region request header.",
                })?
                .tracing_context,
        );
        let handler = self.handler.clone();

        // Executes requests in another runtime to
        // 1. prevent the execution from being cancelled unexpected by Tonic runtime;
        //   - Refer to our blog for the rational behind it:
        //     https://www.greptime.com/blogs/2023-01-12-hidden-control-flow.html
        //   - Obtaining a `JoinHandle` to get the panic message (if there's any).
        //     From its docs, `JoinHandle` is cancel safe. The task keeps running even it's handle been dropped.
        // 2. avoid the handler blocks the gRPC runtime incidentally.
        let handle = self.runtime.spawn(async move {
            handler
                .handle_region_request(request)
                .trace(tracing_context.attach(info_span!("RegionServerRequestHandler::handle")))
                .await
                .map_err(|e| {
                    if e.status_code().should_log_error() {
                        error!(e; "Failed to handle request");
                    } else {
                        // Currently, we still print a debug log.
                        debug!("Failed to handle request, err: {}", e);
                    }
                    e
                })
        });

        handle.await.context(JoinTaskSnafu)?
    }
}

#[async_trait]
impl RegionServer for RegionServerRequestHandler {
    async fn cancel_compaction(
        &self,
        request: Request<CancelCompactionRequest>,
    ) -> TonicResult<Response<CancelCompactionResponse>> {
        let remote_addr = request.remote_addr();
        let self_cloned = self.clone();
        let request_future = async move {
            let response = self_cloned.cancel_compaction(request.into_inner()).await?;
            Ok(Response::new(response))
        };
        let cancellation_future = async move {
            warn!(
                "Compaction cancellation request from {:?} cancelled by client",
                remote_addr
            );
            Err(Status::cancelled(
                "Compaction cancellation request cancelled by client",
            ))
        };
        cancellation::with_cancellation_handler(request_future, cancellation_future).await
    }

    async fn handle(
        &self,
        request: Request<RegionRequest>,
    ) -> TonicResult<Response<RegionResponse>> {
        let remote_addr = request.remote_addr();
        let self_cloned = self.clone();
        let request_future = async move {
            let request = request.into_inner();
            let response = self_cloned.handle(request).await?;

            Ok(Response::new(response))
        };

        let cancellation_future = async move {
            warn!("Region request from {:?} cancelled by client", remote_addr);
            // If this future is executed it means the request future was dropped,
            // so it doesn't actually matter what is returned here
            Err(Status::cancelled("Region request cancelled by client"))
        };
        cancellation::with_cancellation_handler(request_future, cancellation_future).await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use std::time::Duration;

    use api::v1::region::RegionRequestHeader;
    use api::v1::region::region_client::RegionClient;
    use api::v1::region::region_server::RegionServer as RegionService;
    use tokio::net::TcpListener;
    use tokio_stream::wrappers::TcpListenerStream;

    use super::*;

    #[derive(Default)]
    struct RecordingHandler {
        requests: Mutex<Vec<CancelCompactionRequest>>,
    }

    #[async_trait]
    impl RegionServerHandler for RecordingHandler {
        async fn handle(&self, _request: region_request::Body) -> Result<RegionResponse> {
            panic!("cancellation must use its independent RPC handler")
        }

        async fn cancel_compaction(
            &self,
            request: CancelCompactionRequest,
        ) -> Result<CancelCompactionResponse> {
            self.requests.lock().unwrap().push(request);
            Ok(CancelCompactionResponse {
                header: Some(ResponseHeader {
                    status: Some(ResponseStatus {
                        status_code: StatusCode::Success as u32,
                        ..Default::default()
                    }),
                }),
                result: CancelCompactionResult::AlreadyCancelling as i32,
            })
        }
    }

    #[tokio::test]
    async fn test_cancel_compaction_forwards_through_region_handler() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let handler = Arc::new(RecordingHandler::default());
        let bridge =
            RegionServerRequestHandler::new(handler.clone(), common_runtime::global_runtime());
        let (shutdown, stopped) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(RegionService::new(bridge))
                .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                    let _ = stopped.await;
                })
                .await
                .unwrap();
        });
        tokio::time::timeout(Duration::from_secs(10), async {
            let mut client = RegionClient::connect(format!("http://{addr}"))
                .await
                .unwrap();
            let error = client
                .cancel_compaction(CancelCompactionRequest {
                    header: None,
                    region_id: 42,
                    task_id: "cycle-id".into(),
                })
                .await
                .unwrap_err();
            assert_eq!(error.code(), tonic::Code::InvalidArgument);
            assert!(handler.requests.lock().unwrap().is_empty());

            let request = CancelCompactionRequest {
                header: Some(RegionRequestHeader {
                    dbname: "greptime-public".into(),
                    tracing_context: TracingContext::from_current_span().to_w3c(),
                    ..Default::default()
                }),
                region_id: 42,
                task_id: "cycle-id".into(),
            };
            let response = client
                .cancel_compaction(request.clone())
                .await
                .unwrap()
                .into_inner();
            assert_eq!(
                response.result,
                CancelCompactionResult::AlreadyCancelling as i32
            );
            assert_eq!(
                response.header.unwrap().status.unwrap().status_code,
                StatusCode::Success as u32
            );
            assert_eq!(*handler.requests.lock().unwrap(), vec![request]);
        })
        .await
        .expect("cancellation RPC did not complete");
        shutdown.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(10), server)
            .await
            .unwrap()
            .unwrap();
    }
}
