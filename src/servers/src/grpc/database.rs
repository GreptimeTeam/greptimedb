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
use session::context::Channel;
use tonic::{Request, Response, Status, Streaming};

use crate::grpc::greptime_handler::GreptimeRequestHandler;
use crate::grpc::memory_limit::PreDecodeMemoryReservation;
use crate::grpc::{TonicResult, cancellation};
use crate::hint_headers;
use crate::request_memory_limiter::ServerMemoryLimiter;

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
        let channel = request
            .extensions()
            .get::<Channel>()
            .copied()
            .unwrap_or(Channel::Grpc);
        debug!(
            "GreptimeDatabase::Handle: request from {:?} with hints: {:?}",
            remote_addr, hints
        );

        // Retain the pre-decode reservation for the whole request: the
        // extension holding the guard would be dropped when the request is
        // consumed below, but the post-decode charge is skipped while it is
        // active.
        let _pre_reservation = request
            .extensions()
            .get::<PreDecodeMemoryReservation>()
            .cloned();
        let _guard = if _pre_reservation.is_some() {
            // Compressed requests already reserved the worst-case decoded
            // size before tonic decompressed the message; skip the exact
            // post-decode charge to avoid double accounting.
            None
        } else if let Some(limiter) = request.extensions().get::<ServerMemoryLimiter>() {
            let message_size = request.get_ref().encoded_len() as u64;
            Some(limiter.acquire(message_size).await?)
        } else {
            None
        };

        let handler = self.handler.clone();
        let request_future = async move {
            let request = request.into_inner();
            let output = handler.handle_request(request, hints, channel).await?;
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
        let channel = request
            .extensions()
            .get::<Channel>()
            .copied()
            .unwrap_or(Channel::Grpc);
        debug!(
            "GreptimeDatabase::HandleRequests: request from {:?} with hints: {:?}",
            remote_addr, hints
        );

        let limiter = request.extensions().get::<ServerMemoryLimiter>().cloned();
        // For compressed streams the whole stream's decoding memory was
        // reserved before tonic started decompressing; messages are decoded
        // one at a time, so the reservation covers each of them. The
        // reservation is retained below for the whole stream: the extension
        // holding the guard would otherwise be dropped when the request is
        // consumed, while per-message charges stay skipped.
        let reservation = request
            .extensions()
            .get::<PreDecodeMemoryReservation>()
            .cloned();
        let pre_reserved = reservation.is_some();

        let handler = self.handler.clone();
        let request_future = async move {
            let mut affected_rows = 0;

            // Hold the pre-decode reservation until the stream is exhausted.
            let _reservation = reservation;
            let mut stream = request.into_inner();
            while let Some(request) = stream.next().await {
                let request = request?;

                let _guard = if pre_reserved {
                    None
                } else if let Some(limiter_ref) = &limiter {
                    let message_size = request.encoded_len() as u64;
                    Some(limiter_ref.acquire(message_size).await?)
                } else {
                    None
                };
                let output = handler
                    .handle_request(request, hints.clone(), channel)
                    .await?;
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
