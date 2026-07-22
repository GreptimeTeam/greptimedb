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
use crate::request_memory_limiter::ServerMemoryLimiter;

static AFFECTED_ROWS_OUT_OF_RANGE_MESSAGE: &str =
    "affected rows exceed the maximum value supported by the gRPC response";

fn affected_rows_response(affected_rows: usize) -> Result<RawResponse, Status> {
    let value = u32::try_from(affected_rows)
        .map_err(|_| Status::out_of_range(AFFECTED_ROWS_OUT_OF_RANGE_MESSAGE))?;

    Ok(RawResponse::AffectedRows(AffectedRows { value }))
}

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

        let _guard = if let Some(limiter) = request.extensions().get::<ServerMemoryLimiter>() {
            let message_size = request.get_ref().encoded_len() as u64;
            Some(limiter.acquire(message_size).await?)
        } else {
            None
        };

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
                    response: Some(affected_rows_response(rows)?),
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

        let limiter = request.extensions().get::<ServerMemoryLimiter>().cloned();

        let handler = self.handler.clone();
        let request_future = async move {
            let mut affected_rows: usize = 0;

            let mut stream = request.into_inner();
            while let Some(request) = stream.next().await {
                let request = request?;

                let _guard = if let Some(limiter_ref) = &limiter {
                    let message_size = request.encoded_len() as u64;
                    Some(limiter_ref.acquire(message_size).await?)
                } else {
                    None
                };
                let output = handler.handle_request(request, hints.clone()).await?;
                match output.data {
                    OutputData::AffectedRows(rows) => {
                        affected_rows = affected_rows.checked_add(rows).ok_or_else(|| {
                            Status::out_of_range(AFFECTED_ROWS_OUT_OF_RANGE_MESSAGE)
                        })?
                    }
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
                response: Some(affected_rows_response(affected_rows)?),
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

#[cfg(all(test, target_pointer_width = "64"))]
mod qx_079_tests {
    use std::collections::VecDeque;
    use std::pin::Pin;
    use std::sync::{Arc, Mutex};

    use api::v1::greptime_database_client::GreptimeDatabaseClient;
    use api::v1::greptime_database_server::GreptimeDatabaseServer;
    use api::v1::greptime_request::Request as DatabaseRequest;
    use api::v1::greptime_response::Response as RawResponse;
    use api::v1::{DdlRequest, GreptimeRequest, GreptimeResponse};
    use async_trait::async_trait;
    use common_grpc::flight::do_put::DoPutResponse;
    use common_query::Output;
    use futures::Stream;
    use session::context::QueryContextRef;
    use tokio::net::TcpListener;
    use tokio::task::JoinHandle;
    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::Code;
    use tonic::transport::Channel;

    use super::{AFFECTED_ROWS_OUT_OF_RANGE_MESSAGE, DatabaseService};
    use crate::grpc::FlightCompression;
    use crate::grpc::flight::PutRecordBatchRequestStream;
    use crate::grpc::greptime_handler::GreptimeRequestHandler;
    use crate::query_handler::grpc::GrpcQueryHandler;

    struct AffectedRowsHandler {
        outputs: Mutex<VecDeque<usize>>,
    }

    impl AffectedRowsHandler {
        fn new(outputs: impl IntoIterator<Item = usize>) -> Self {
            Self {
                outputs: Mutex::new(outputs.into_iter().collect()),
            }
        }
    }

    #[async_trait]
    impl GrpcQueryHandler for AffectedRowsHandler {
        async fn do_query(
            &self,
            _query: DatabaseRequest,
            _ctx: QueryContextRef,
        ) -> crate::error::Result<Output> {
            let rows = self
                .outputs
                .lock()
                .expect("lock QX-079 affected rows handler")
                .pop_front()
                .expect("QX-079 handler received more requests than configured");
            Ok(Output::new_with_affected_rows(rows))
        }

        fn handle_put_record_batch_stream(
            &self,
            _stream: PutRecordBatchRequestStream,
            _ctx: QueryContextRef,
        ) -> Pin<Box<dyn Stream<Item = crate::error::Result<DoPutResponse>> + Send>> {
            Box::pin(futures::stream::empty())
        }
    }

    fn database_request() -> GreptimeRequest {
        GreptimeRequest {
            header: None,
            request: Some(DatabaseRequest::Ddl(DdlRequest::default())),
        }
    }

    fn affected_rows(response: GreptimeResponse) -> u32 {
        match response.response {
            Some(RawResponse::AffectedRows(rows)) => rows.value,
            None => panic!("QX-079 database response is missing affected rows"),
        }
    }

    async fn start_database_server(
        outputs: impl IntoIterator<Item = usize>,
    ) -> (GreptimeDatabaseClient<Channel>, JoinHandle<()>) {
        let query_handler = Arc::new(AffectedRowsHandler::new(outputs));
        let request_handler =
            GreptimeRequestHandler::new(query_handler, None, None, FlightCompression::None);
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind QX-079 database server");
        let addr = listener
            .local_addr()
            .expect("get QX-079 database server address");
        let server = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(GreptimeDatabaseServer::new(DatabaseService::new(
                    request_handler,
                )))
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .expect("serve QX-079 database server");
        });
        let client = GreptimeDatabaseClient::connect(format!("http://{addr}"))
            .await
            .expect("connect QX-079 database client");

        (client, server)
    }

    #[tokio::test]
    async fn qx_079_unary_u32_max_round_trips_exactly() {
        let (mut client, server) = start_database_server([u32::MAX as usize]).await;

        let result = client.handle(database_request()).await;
        server.abort();

        let response = result.expect("QX-079 unary u32::MAX request succeeds");
        assert_eq!(affected_rows(response.into_inner()), u32::MAX);
    }

    #[tokio::test]
    async fn qx_079_unary_above_u32_max_is_out_of_range() {
        let (mut client, server) = start_database_server([(u32::MAX as usize) + 1]).await;

        let result = client.handle(database_request()).await;
        server.abort();

        let status = result.expect_err("QX-079 unary affected rows above u32::MAX must fail");
        assert_eq!(status.code(), Code::OutOfRange);
        assert_eq!(status.message(), AFFECTED_ROWS_OUT_OF_RANGE_MESSAGE);
    }

    #[tokio::test]
    async fn qx_079_client_stream_u32_control_sums_exactly() {
        let (mut client, server) = start_database_server([7, 9]).await;

        let result = client
            .handle_requests(tokio_stream::iter([database_request(), database_request()]))
            .await;
        server.abort();

        let response = result.expect("QX-079 client-stream u32 control succeeds");
        assert_eq!(affected_rows(response.into_inner()), 16);
    }

    #[tokio::test]
    async fn qx_079_client_stream_u32_max_round_trips_exactly() {
        let (mut client, server) = start_database_server([(u32::MAX as usize) - 1, 1]).await;

        let result = client
            .handle_requests(tokio_stream::iter([database_request(), database_request()]))
            .await;
        server.abort();

        let response = result.expect("QX-079 client-stream u32::MAX request succeeds");
        assert_eq!(affected_rows(response.into_inner()), u32::MAX);
    }

    #[tokio::test]
    async fn qx_079_client_stream_above_u32_max_is_out_of_range() {
        let (mut client, server) = start_database_server([3_000_000_000, 2_000_000_000]).await;

        let result = client
            .handle_requests(tokio_stream::iter([database_request(), database_request()]))
            .await;
        server.abort();

        let status =
            result.expect_err("QX-079 client-stream affected rows above u32::MAX must fail");
        assert_eq!(status.code(), Code::OutOfRange);
        assert_eq!(status.message(), AFFECTED_ROWS_OUT_OF_RANGE_MESSAGE);
    }

    #[tokio::test]
    async fn qx_079_client_stream_usize_overflow_is_out_of_range() {
        let (mut client, server) = start_database_server([usize::MAX, 1]).await;

        let result = client
            .handle_requests(tokio_stream::iter([database_request(), database_request()]))
            .await;
        server.abort();

        let status = result.expect_err("QX-079 client-stream usize overflow must fail");
        assert_eq!(status.code(), Code::OutOfRange);
        assert_eq!(status.message(), AFFECTED_ROWS_OUT_OF_RANGE_MESSAGE);
    }
}
