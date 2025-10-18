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

use api::v1::greptime_database_server::GreptimeDatabase;
use api::v1::greptime_response::Response as RawResponse;
use api::v1::{AffectedRows, GreptimeRequest, GreptimeResponse, ResponseHeader};
use async_trait::async_trait;
use common_error::status_code::StatusCode;
use common_query::OutputData;
use common_telemetry::{debug, warn};
use futures::StreamExt;
use prost::Message;
use tonic::{Request, Response, Status, Streaming};

use crate::grpc::greptime_handler::GreptimeRequestHandler;
use crate::grpc::{TonicResult, cancellation};
use crate::hint_headers;
use crate::metrics::{METRIC_GRPC_MEMORY_USAGE_BYTES, METRIC_GRPC_REQUESTS_REJECTED_TOTAL};
use crate::request_limiter::RequestMemoryLimiter;

pub(crate) struct DatabaseService {
    handler: GreptimeRequestHandler,
}

impl DatabaseService {
    pub(crate) fn new(handler: GreptimeRequestHandler) -> Self {
        Self { handler }
    }
}

#[async_trait]
impl GreptimeDatabase for DatabaseService {
    async fn handle(
        &self,
        request: Request<GreptimeRequest>,
    ) -> TonicResult<Response<GreptimeResponse>> {
        let remote_addr = request.remote_addr();
        let hints = hint_headers::extract_hints(request.metadata());
        debug!(
            "GreptimeDatabase::Handle: request from {:?} with hints: {:?}",
            remote_addr, hints
        );

        let _guard = request
            .extensions()
            .get::<RequestMemoryLimiter>()
            .filter(|limiter| limiter.is_enabled())
            .and_then(|limiter| {
                let message_size = request.get_ref().encoded_len();
                limiter
                    .try_acquire(message_size)
                    .map(|guard| {
                        guard.inspect(|g| {
                            METRIC_GRPC_MEMORY_USAGE_BYTES.set(g.current_usage() as i64);
                        })
                    })
                    .inspect_err(|_| {
                        METRIC_GRPC_REQUESTS_REJECTED_TOTAL.inc();
                    })
                    .transpose()
            })
            .transpose()?;

        let handler = self.handler.clone();
        let request_future = async move {
            let request = request.into_inner();
            let output = handler.handle_request(request, hints).await?;
            let message = match output.data {
                OutputData::AffectedRows(rows) => GreptimeResponse {
                    header: Some(ResponseHeader {
                        status: Some(api::v1::Status {
                            status_code: StatusCode::Success as _,
                            ..Default::default()
                        }),
                    }),
                    response: Some(RawResponse::AffectedRows(AffectedRows { value: rows as _ })),
                },
                OutputData::Stream(_) | OutputData::RecordBatches(_) => {
                    return Err(Status::unimplemented("GreptimeDatabase::Handle for query"));
                }
            };

            Ok(Response::new(message))
        };

        let cancellation_future = async move {
            warn!(
                "GreptimeDatabase::Handle: request from {:?} cancelled by client",
                remote_addr
            );
            // If this future is executed it means the request future was dropped,
            // so it doesn't actually matter what is returned here
            Err(Status::cancelled(
                "GreptimeDatabase::Handle: request cancelled by client",
            ))
        };
        cancellation::with_cancellation_handler(request_future, cancellation_future).await
    }

    async fn handle_requests(
        &self,
        request: Request<Streaming<GreptimeRequest>>,
    ) -> Result<Response<GreptimeResponse>, Status> {
        let remote_addr = request.remote_addr();
        let hints = hint_headers::extract_hints(request.metadata());
        debug!(
            "GreptimeDatabase::HandleRequests: request from {:?} with hints: {:?}",
            remote_addr, hints
        );

        let limiter = request.extensions().get::<RequestMemoryLimiter>().cloned();

        let handler = self.handler.clone();
        let request_future = async move {
            let mut affected_rows = 0;

            let mut stream = request.into_inner();
            while let Some(request) = stream.next().await {
                let request = request?;

                let _guard = limiter
                    .as_ref()
                    .filter(|limiter| limiter.is_enabled())
                    .and_then(|limiter| {
                        let message_size = request.encoded_len();
                        limiter
                            .try_acquire(message_size)
                            .map(|guard| {
                                guard.inspect(|g| {
                                    METRIC_GRPC_MEMORY_USAGE_BYTES.set(g.current_usage() as i64);
                                })
                            })
                            .inspect_err(|_| {
                                METRIC_GRPC_REQUESTS_REJECTED_TOTAL.inc();
                            })
                            .transpose()
                    })
                    .transpose()?;
                let output = handler.handle_request(request, hints.clone()).await?;
                match output.data {
                    OutputData::AffectedRows(rows) => affected_rows += rows,
                    OutputData::Stream(_) | OutputData::RecordBatches(_) => {
                        return Err(Status::unimplemented(
                            "GreptimeDatabase::HandleRequests for query",
                        ));
                    }
                }
            }
            let message = GreptimeResponse {
                header: Some(ResponseHeader {
                    status: Some(api::v1::Status {
                        status_code: StatusCode::Success as _,
                        ..Default::default()
                    }),
                }),
                response: Some(RawResponse::AffectedRows(AffectedRows {
                    value: affected_rows as u32,
                })),
            };

            Ok(Response::new(message))
        };

        let cancellation_future = async move {
            warn!(
                "GreptimeDatabase::HandleRequests: request from {:?} cancelled by client",
                remote_addr
            );
            // If this future is executed it means the request future was dropped,
            // so it doesn't actually matter what is returned here
            Err(Status::cancelled(
                "GreptimeDatabase::HandleRequests: request cancelled by client",
            ))
        };
        cancellation::with_cancellation_handler(request_future, cancellation_future).await
    }
}
