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

mod codec;

use std::borrow::Borrow;
use std::sync::Arc;

use api::region::RegionResponse;
use api::v1::ResponseHeader;
use api::v1::region::{
    RegionRequest, RegionRequestHeader, RemoteDynFilterRequest, RemoteDynFilterUnregister,
    RemoteDynFilterUpdate, region_request, remote_dyn_filter_request,
};
use arc_swap::ArcSwapOption;
use arrow_flight::Ticket;
use async_stream::stream;
use async_trait::async_trait;
use common_error::ext::BoxedError;
use common_error::status_code::StatusCode;
use common_grpc::flight::{FlightDecoder, FlightMessage};
use common_meta::error::{self as meta_error, Result as MetaResult};
use common_meta::node_manager::Datanode;
use common_query::request::QueryRequest;
use common_recordbatch::error::ExternalSnafu;
use common_recordbatch::{RecordBatch, RecordBatchStreamWrapper, SendableRecordBatchStream};
use common_telemetry::error;
use common_telemetry::tracing::Span;
use common_telemetry::tracing_context::TracingContext;
use prost::Message;
use query::query_engine::DefaultSerializer;
use snafu::{OptionExt, ResultExt, location};
use substrait::{DFLogicalSubstraitConvertor, SubstraitPlan};
use tokio_stream::StreamExt;

use crate::error::{
    self, ConvertFlightDataSnafu, FlightGetSnafu, IllegalDatabaseResponseSnafu,
    IllegalFlightMessagesSnafu, MissingFieldSnafu, Result, ServerSnafu,
};
use crate::region::codec::RegionCodec;
use crate::{Client, Error, metrics};

/// Sends region operations through a configured gRPC client.
#[derive(Debug)]
pub struct RegionRequester {
    client: Client,
    send_compression: bool,
    accept_compression: bool,
}

#[async_trait]
impl Datanode for RegionRequester {
    async fn handle_request(&self, request: Arc<RegionRequest>) -> MetaResult<RegionResponse> {
        self.handle_inner(request).await.map_err(Into::into)
    }

    async fn handle_query(&self, request: QueryRequest) -> MetaResult<SendableRecordBatchStream> {
        let plan = DFLogicalSubstraitConvertor
            .encode(&request.plan, DefaultSerializer)
            .map_err(BoxedError::new)
            .context(meta_error::ExternalSnafu)?
            .to_vec();
        let request = api::v1::region::QueryRequest {
            header: request.header,
            region_id: request.region_id.as_u64(),
            plan,
        };

        let ticket = Ticket {
            ticket: request.encode_to_vec().into(),
        };
        self.do_get_inner(ticket)
            .await
            .map_err(BoxedError::new)
            .context(meta_error::ExternalSnafu)
    }
}

impl RegionRequester {
    pub fn new(client: Client, send_compression: bool, accept_compression: bool) -> Self {
        Self {
            client,
            send_compression,
            accept_compression,
        }
    }

    pub async fn do_get_inner(&self, ticket: Ticket) -> Result<SendableRecordBatchStream> {
        let mut flight_client = self
            .client
            .make_flight_client(self.send_compression, self.accept_compression)?;
        let response = flight_client
            .mut_inner()
            .do_get(ticket)
            .await
            .or_else(|e| {
                let tonic_code = e.code();
                let e: error::Error = e.into();
                error!(
                    e; "Failed to do Flight get, addr: {}, code: {}",
                    flight_client.addr(),
                    tonic_code
                );
                Err(BoxedError::new(e)).with_context(|_| FlightGetSnafu {
                    addr: flight_client.addr().to_string(),
                    tonic_code,
                })
            })?;

        let flight_data_stream = response.into_inner();
        let mut decoder = FlightDecoder::default();

        let mut flight_message_stream = flight_data_stream.map(move |flight_data| {
            flight_data
                .map_err(Error::from)
                .and_then(|data| decoder.try_decode(&data).context(ConvertFlightDataSnafu))?
                .context(IllegalFlightMessagesSnafu {
                    reason: "none message",
                })
        });

        let Some(first_flight_message) = flight_message_stream.next().await else {
            return IllegalFlightMessagesSnafu {
                reason: "Expect the response not to be empty",
            }
            .fail();
        };
        let FlightMessage::Schema(schema) = first_flight_message? else {
            return IllegalFlightMessagesSnafu {
                reason: "Expect schema to be the first flight message",
            }
            .fail();
        };

        let metrics = Arc::new(ArcSwapOption::from(None));
        let metrics_ref = metrics.clone();

        let tracing_context = TracingContext::from_current_span();

        let schema = Arc::new(
            datatypes::schema::Schema::try_from(schema).context(error::ConvertSchemaSnafu)?,
        );
        let schema_cloned = schema.clone();
        let stream = Box::pin(stream!({
            let _span = tracing_context.attach(common_telemetry::tracing::info_span!(
                "poll_flight_data_stream"
            ));

            let mut buffered_message: Option<FlightMessage> = None;
            let mut stream_ended = false;

            while !stream_ended {
                // get the next message from the buffered message or read from the flight message stream
                let flight_message_item = if let Some(msg) = buffered_message.take() {
                    Some(Ok(msg))
                } else {
                    flight_message_stream.next().await
                };

                let flight_message = match flight_message_item {
                    Some(Ok(message)) => message,
                    Some(Err(e)) => {
                        yield Err(BoxedError::new(e)).context(ExternalSnafu);
                        break;
                    }
                    None => break,
                };

                match flight_message {
                    FlightMessage::RecordBatch(record_batch) => {
                        let result_to_yield =
                            RecordBatch::from_df_record_batch(schema_cloned.clone(), record_batch);

                        // get the next message from the stream. normally it should be a metrics message.
                        if let Some(next_flight_message_result) = flight_message_stream.next().await
                        {
                            match next_flight_message_result {
                                Ok(FlightMessage::Metrics(s)) => {
                                    let m = serde_json::from_str(&s).ok().map(Arc::new);
                                    metrics_ref.swap(m);
                                }
                                Ok(FlightMessage::RecordBatch(rb)) => {
                                    // for some reason it's not a metrics message, so we need to buffer this record batch
                                    // and yield it in the next iteration.
                                    buffered_message = Some(FlightMessage::RecordBatch(rb));
                                }
                                Ok(_) => {
                                    yield IllegalFlightMessagesSnafu {
                                        reason: "A RecordBatch message can only be succeeded by a Metrics message or another RecordBatch message"
                                    }
                                    .fail()
                                    .map_err(BoxedError::new)
                                    .context(ExternalSnafu);
                                    break;
                                }
                                Err(e) => {
                                    yield Err(BoxedError::new(e)).context(ExternalSnafu);
                                    break;
                                }
                            }
                        } else {
                            // the stream has ended
                            stream_ended = true;
                        }

                        yield Ok(result_to_yield);
                    }
                    FlightMessage::Metrics(s) => {
                        // just a branch in case of some metrics message comes after other things.
                        let m = serde_json::from_str(&s).ok().map(Arc::new);
                        metrics_ref.swap(m);
                        break;
                    }
                    _ => {
                        yield IllegalFlightMessagesSnafu {
                            reason: "A Schema message must be succeeded exclusively by a set of RecordBatch messages"
                        }
                        .fail()
                        .map_err(BoxedError::new)
                        .context(ExternalSnafu);
                        break;
                    }
                }
            }
        }));
        let record_batch_stream = RecordBatchStreamWrapper {
            schema,
            stream,
            output_ordering: None,
            metrics,
            span: Span::current(),
        };
        Ok(Box::pin(record_batch_stream))
    }

    /// Sends an owned or shared request without deep-cloning its protobuf payload.
    pub async fn handle<R>(&self, request: R) -> Result<RegionResponse>
    where
        R: Borrow<RegionRequest> + Send + Sync + 'static,
    {
        self.handle_inner(request).await
    }

    /// Sends a remote dynamic filter update to the region service.
    pub async fn handle_remote_dyn_filter_update(
        &self,
        query_id: impl Into<String>,
        update: RemoteDynFilterUpdate,
    ) -> Result<RegionResponse> {
        self.handle_inner(build_remote_dyn_filter_update_request(query_id, update))
            .await
    }

    /// Unregisters a remote dynamic filter in the region service.
    pub async fn handle_remote_dyn_filter_unregister(
        &self,
        query_id: impl Into<String>,
        unregister: RemoteDynFilterUnregister,
    ) -> Result<RegionResponse> {
        self.handle_inner(build_remote_dyn_filter_unregister_request(
            query_id, unregister,
        ))
        .await
    }

    async fn handle_inner<R>(&self, request: R) -> Result<RegionResponse>
    where
        R: Borrow<RegionRequest> + Send + Sync + 'static,
    {
        let request_body = request
            .borrow()
            .body
            .as_ref()
            .context(MissingFieldSnafu { field: "body" })?;
        let is_insert = matches!(
            request_body,
            region_request::Body::Inserts(_) | region_request::Body::BulkInsert(_)
        );
        let request_type = request_body.as_ref().to_string();
        let _timer = metrics::METRIC_REGION_REQUEST_GRPC
            .with_label_values(&[request_type.as_str()])
            .start_timer();

        let (addr, mut client) = self.client.raw_region_client()?;
        if is_insert {
            if self.send_compression {
                client = client.send_compressed(tonic::codec::CompressionEncoding::Zstd);
            }
            if self.accept_compression {
                client = client.accept_compressed(tonic::codec::CompressionEncoding::Zstd);
            }
        }

        let response = async {
            client
                .ready()
                .await
                .map_err(|e| tonic::Status::unknown(format!("Service was not ready: {e}")))?;
            let mut request = tonic::Request::new(request);
            request.extensions_mut().insert(tonic::GrpcMethod::new(
                "greptime.v1.region.Region",
                "Handle",
            ));
            client
                .unary(
                    request,
                    tonic::codegen::http::uri::PathAndQuery::from_static(
                        "/greptime.v1.region.Region/Handle",
                    ),
                    RegionCodec::default(),
                )
                .await
        }
        .await
        .map_err(|status| map_region_error(status, addr))?
        .into_inner();

        check_response_header(&response.header)?;

        Ok(RegionResponse::from_region_response(response))
    }
}

fn map_region_error(status: tonic::Status, addr: String) -> error::Error {
    error::Error::RegionServer {
        addr,
        code: status.code(),
        source: BoxedError::new(error::Error::from(status)),
        location: location!(),
    }
}

pub fn build_remote_dyn_filter_update_request(
    query_id: impl Into<String>,
    update: RemoteDynFilterUpdate,
) -> RegionRequest {
    build_remote_dyn_filter_request(
        query_id.into(),
        remote_dyn_filter_request::Action::Update(update),
    )
}

pub fn build_remote_dyn_filter_unregister_request(
    query_id: impl Into<String>,
    unregister: RemoteDynFilterUnregister,
) -> RegionRequest {
    build_remote_dyn_filter_request(
        query_id.into(),
        remote_dyn_filter_request::Action::Unregister(unregister),
    )
}

fn build_remote_dyn_filter_request(
    query_id: String,
    action: remote_dyn_filter_request::Action,
) -> RegionRequest {
    RegionRequest {
        header: Some(RegionRequestHeader {
            tracing_context: TracingContext::from_current_span().to_w3c(),
            ..Default::default()
        }),
        body: Some(region_request::Body::RemoteDynFilter(
            RemoteDynFilterRequest {
                query_id,
                action: Some(action),
            },
        )),
    }
}

pub fn check_response_header(header: &Option<ResponseHeader>) -> Result<()> {
    let status = header
        .as_ref()
        .and_then(|header| header.status.as_ref())
        .context(IllegalDatabaseResponseSnafu {
            err_msg: "either response header or status is missing",
        })?;

    if StatusCode::is_success(status.status_code) {
        Ok(())
    } else {
        let code =
            StatusCode::from_u32(status.status_code).context(IllegalDatabaseResponseSnafu {
                err_msg: format!("unknown server status: {:?}", status),
            })?;
        ServerSnafu {
            code,
            msg: status.err_msg.clone(),
        }
        .fail()
    }
}

#[cfg(test)]
#[allow(deprecated)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::Mutex;

    use api::v1::region::region_server::{Region, RegionServer};
    use api::v1::region::{
        BulkInsertRequest, InsertRequest, InsertRequests, RegionResponse as PbRegionResponse,
        RemoteDynFilterUnregister, RemoteDynFilterUpdate, region_request,
        remote_dyn_filter_request,
    };
    use api::v1::value::ValueData;
    use api::v1::{
        ColumnDataType, ColumnSchema as PbColumnSchema, Row, Rows, Status as PbStatus, Value,
    };
    use common_error::ext::{ErrorExt, RetryHint};
    use common_grpc::channel_manager::{ChannelConfig, ChannelManager};
    use tokio::net::TcpListener;
    use tokio::task::JoinHandle;
    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::codec::CompressionEncoding;
    use tonic::{Request, Response, Status};

    use super::*;
    use crate::Error;

    #[derive(Clone, Default)]
    struct RecordingRegionService {
        requests: Arc<Mutex<Vec<(RegionRequest, bool, bool)>>>,
        responses: Arc<Mutex<VecDeque<std::result::Result<PbRegionResponse, Status>>>>,
    }

    #[tonic::async_trait]
    impl Region for RecordingRegionService {
        async fn handle(
            &self,
            request: Request<RegionRequest>,
        ) -> std::result::Result<Response<PbRegionResponse>, Status> {
            let metadata = request.metadata();
            let sends_zstd = metadata.get("grpc-encoding").is_some_and(|v| v == "zstd");
            let accepts_zstd = metadata
                .get("grpc-accept-encoding")
                .and_then(|v| v.to_str().ok())
                .is_some_and(|v| v.split(',').any(|encoding| encoding == "zstd"));
            self.requests
                .lock()
                .unwrap()
                .push((request.into_inner(), sends_zstd, accepts_zstd));
            self.responses
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or_else(|| Ok(build_response(StatusCode::Success)))
                .map(Response::new)
        }
    }

    struct RegionTestServer {
        addr: String,
        service: RecordingRegionService,
        task: JoinHandle<()>,
    }

    impl RegionTestServer {
        async fn start() -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap().to_string();
            let service = RecordingRegionService::default();
            let server_service = service.clone();
            let task = tokio::spawn(async move {
                tonic::transport::Server::builder()
                    .add_service(
                        RegionServer::new(server_service)
                            .accept_compressed(CompressionEncoding::Zstd)
                            .send_compressed(CompressionEncoding::Zstd),
                    )
                    .serve_with_incoming(TcpListenerStream::new(listener))
                    .await
                    .unwrap();
            });
            Self {
                addr,
                service,
                task,
            }
        }

        fn create_requester(
            &self,
            config: ChannelConfig,
            send: bool,
            accept: bool,
        ) -> RegionRequester {
            RegionRequester::new(
                Client::with_manager_and_urls(
                    ChannelManager::with_config(config, None),
                    [self.addr.clone()],
                ),
                send,
                accept,
            )
        }
    }

    impl Drop for RegionTestServer {
        fn drop(&mut self) {
            self.task.abort();
        }
    }

    fn build_response(code: StatusCode) -> PbRegionResponse {
        PbRegionResponse {
            header: Some(ResponseHeader {
                status: Some(PbStatus {
                    status_code: code as u32,
                    ..Default::default()
                }),
            }),
            affected_rows: 1,
            ..Default::default()
        }
    }

    fn build_insert_request() -> RegionRequest {
        RegionRequest {
            header: Some(RegionRequestHeader {
                dbname: "shared-request-test".to_string(),
                ..Default::default()
            }),
            body: Some(region_request::Body::Inserts(InsertRequests {
                requests: vec![InsertRequest {
                    region_id: 42,
                    rows: Some(Rows {
                        schema: vec![
                            PbColumnSchema {
                                column_name: "message".to_string(),
                                datatype: ColumnDataType::String as i32,
                                ..Default::default()
                            },
                            PbColumnSchema {
                                column_name: "payload".to_string(),
                                datatype: ColumnDataType::Binary as i32,
                                ..Default::default()
                            },
                        ],
                        rows: vec![Row {
                            values: vec![
                                Value {
                                    value_data: Some(ValueData::StringValue(
                                        "hello 世界".to_string(),
                                    )),
                                },
                                Value {
                                    value_data: Some(ValueData::BinaryValue(vec![0, 1, 127, 255])),
                                },
                            ],
                        }],
                    }),
                    ..Default::default()
                }],
            })),
        }
    }

    #[tokio::test]
    async fn test_owned_and_shared_requests_preserve_payload_and_compression() {
        let server = RegionTestServer::start().await;
        let shared = Arc::new(build_insert_request());
        for (send, accept) in [(false, false), (true, false), (false, true), (true, true)] {
            let requester = server.create_requester(ChannelConfig::default(), send, accept);
            let channel =
                tonic::transport::Endpoint::from_shared(format!("http://{}", server.addr))
                    .unwrap()
                    .connect()
                    .await
                    .unwrap();
            let mut baseline = api::v1::region::region_client::RegionClient::new(channel);
            if send {
                baseline = baseline.send_compressed(CompressionEncoding::Zstd);
            }
            if accept {
                baseline = baseline.accept_compressed(CompressionEncoding::Zstd);
            }
            assert_eq!(
                baseline
                    .handle(build_insert_request())
                    .await
                    .unwrap()
                    .into_inner()
                    .affected_rows,
                1
            );
            assert_eq!(
                requester
                    .handle(build_insert_request())
                    .await
                    .unwrap()
                    .affected_rows,
                1
            );
            assert_eq!(
                requester
                    .handle(Arc::clone(&shared))
                    .await
                    .unwrap()
                    .affected_rows,
                1
            );
        }
        let requests = server.service.requests.lock().unwrap();
        assert_eq!(requests.len(), 12);
        for ((request, sends, accepts), (send, accept)) in requests.iter().zip(
            [(false, false), (true, false), (false, true), (true, true)]
                .into_iter()
                .flat_map(|settings| [settings, settings, settings]),
        ) {
            assert_eq!(request, shared.as_ref());
            assert_eq!((*sends, *accepts), (send, accept));
        }
    }

    #[tokio::test]
    async fn test_shared_request_can_be_resent_after_transport_error() {
        let server = RegionTestServer::start().await;
        let mut status = Status::unavailable("migrating");
        status.metadata_mut().insert(
            common_error::GREPTIME_DB_HEADER_ERROR_RETRY_HINT,
            RetryHint::Retryable.as_str().parse().unwrap(),
        );
        server
            .service
            .responses
            .lock()
            .unwrap()
            .push_back(Err(status));
        let requester = server.create_requester(ChannelConfig::default(), false, false);
        let shared = Arc::new(build_insert_request());
        let error = requester.handle(Arc::clone(&shared)).await.unwrap_err();
        assert_eq!(error.retry_hint(), RetryHint::Retryable);
        assert!(
            matches!(error, Error::RegionServer { addr, code: tonic::Code::Unavailable, .. } if addr == server.addr)
        );
        requester.handle(Arc::clone(&shared)).await.unwrap();
        let requests = server.service.requests.lock().unwrap();
        assert_eq!(requests.len(), 2);
        assert_eq!(&requests[0].0, shared.as_ref());
        assert_eq!(&requests[1].0, shared.as_ref());
    }

    #[test]
    fn test_retry_hint_wire_compatibility() {
        for (name, wire_hint, expected_hint) in [
            ("legacy_missing", None, RetryHint::NonRetryable),
            ("legacy_retryable", Some("retryable"), RetryHint::Retryable),
            (
                "legacy_non_retryable",
                Some("non_retryable"),
                RetryHint::NonRetryable,
            ),
            (
                "route_refresh",
                Some("retry_after_route_refresh"),
                RetryHint::RetryAfterRouteRefresh,
            ),
            (
                "unknown_future_hint",
                Some("future_retry_hint"),
                RetryHint::NonRetryable,
            ),
        ] {
            let mut status = Status::failed_precondition("region unavailable");
            status.metadata_mut().insert(
                common_error::GREPTIME_DB_HEADER_ERROR_CODE,
                (StatusCode::RegionNotReady as u32)
                    .to_string()
                    .parse()
                    .unwrap(),
            );
            if let Some(hint) = wire_hint {
                status.metadata_mut().insert(
                    common_error::GREPTIME_DB_HEADER_ERROR_RETRY_HINT,
                    hint.parse().unwrap(),
                );
            }
            let error: Error = status.into();
            assert_eq!(error.retry_hint(), expected_hint, "{name}");
            assert_eq!(error.status_code(), StatusCode::RegionNotReady, "{name}");
            assert_eq!(
                error.tonic_code(),
                Some(tonic::Code::FailedPrecondition),
                "{name}"
            );
        }
    }

    #[tokio::test]
    async fn test_transport_error_parity_and_datanode_mapping() {
        let server = RegionTestServer::start().await;
        let requester = server.create_requester(ChannelConfig::default(), false, false);
        for hint in [
            RetryHint::Retryable,
            RetryHint::RetryAfterRouteRefresh,
            RetryHint::NonRetryable,
        ] {
            let mut status = Status::failed_precondition("region unavailable");
            status.metadata_mut().insert(
                common_error::GREPTIME_DB_HEADER_ERROR_RETRY_HINT,
                hint.as_str().parse().unwrap(),
            );
            status.metadata_mut().insert(
                common_error::GREPTIME_DB_HEADER_ERROR_CODE,
                (StatusCode::RegionNotFound as u32)
                    .to_string()
                    .parse()
                    .unwrap(),
            );
            server
                .service
                .responses
                .lock()
                .unwrap()
                .extend((0..4).map(|_| Err(status.clone())));
            let mut baseline = api::v1::region::region_client::RegionClient::connect(format!(
                "http://{}",
                server.addr
            ))
            .await
            .unwrap();
            let baseline: Error = baseline
                .handle(build_insert_request())
                .await
                .unwrap_err()
                .into();
            assert_eq!(baseline.status_code(), StatusCode::RegionNotFound);
            assert_eq!(baseline.retry_hint(), hint);
            assert_eq!(baseline.tonic_code(), Some(tonic::Code::FailedPrecondition));
            let owned = requester.handle(build_insert_request()).await.unwrap_err();
            let shared = requester
                .handle(Arc::new(build_insert_request()))
                .await
                .unwrap_err();
            for error in [owned, shared] {
                assert_eq!(error.status_code(), baseline.status_code());
                assert_eq!(error.retry_hint(), baseline.retry_hint());
                assert_eq!(error.tonic_code(), baseline.tonic_code());
            }
            let datanode: &dyn Datanode = &requester;
            let error = datanode.handle(build_insert_request()).await.unwrap_err();
            assert_eq!(error.status_code(), StatusCode::RegionNotFound);
            assert_eq!(error.retry_hint(), hint);
            assert!(matches!(error, meta_error::Error::External { .. }));
        }
    }

    #[tokio::test]
    async fn test_datanode_transport_retry_hints() {
        let server = RegionTestServer::start().await;
        let requester = server.create_requester(ChannelConfig::default(), false, false);
        let datanode: &dyn Datanode = &requester;
        let statuses = [
            tonic::Code::Unavailable,
            tonic::Code::Cancelled,
            tonic::Code::DeadlineExceeded,
            tonic::Code::FailedPrecondition,
        ]
        .into_iter()
        .flat_map(|code| {
            [
                None,
                Some(RetryHint::NonRetryable),
                Some(RetryHint::Retryable),
                Some(RetryHint::RetryAfterRouteRefresh),
            ]
            .into_iter()
            .map(move |hint| (code, hint))
        });
        for (code, hint) in statuses {
            for body in [
                region_request::Body::Inserts(Default::default()),
                region_request::Body::Deletes(Default::default()),
                region_request::Body::BulkInsert(Default::default()),
                region_request::Body::Create(Default::default()),
                region_request::Body::RemoteDynFilter(Default::default()),
            ] {
                let request_type = body.as_ref().to_string();
                let mut status = Status::new(code, "request failed");
                // Metadata status is deliberately different from the transport status.
                status.metadata_mut().insert(
                    common_error::GREPTIME_DB_HEADER_ERROR_CODE,
                    (StatusCode::RegionNotReady as u32)
                        .to_string()
                        .parse()
                        .unwrap(),
                );
                if let Some(hint) = hint {
                    status.metadata_mut().insert(
                        common_error::GREPTIME_DB_HEADER_ERROR_RETRY_HINT,
                        hint.as_str().parse().unwrap(),
                    );
                }
                let original = Error::from(status.clone());
                let expected = if original.should_retry() && !original.retry_hint().is_retryable() {
                    RetryHint::Retryable
                } else {
                    original.retry_hint()
                };
                server
                    .service
                    .responses
                    .lock()
                    .unwrap()
                    .extend([Err(status.clone()), Err(status)]);
                let request = RegionRequest {
                    header: None,
                    body: Some(body),
                };
                let shared = Arc::new(request);
                let shared_error = datanode.handle(Arc::clone(&shared)).await.unwrap_err();
                assert_eq!(Arc::strong_count(&shared), 1);
                let request = Arc::try_unwrap(shared).unwrap();
                let owned_error = datanode.handle(request).await.unwrap_err();
                for error in [owned_error, shared_error] {
                    assert_eq!(
                        error.retry_hint(),
                        expected,
                        "{code:?}, {hint:?}, {request_type}"
                    );
                    assert_eq!(error.retry_hint().is_retryable(), original.should_retry());
                    assert_eq!(error.status_code(), StatusCode::RegionNotReady);
                    let meta_error::Error::External { source, .. } = error else {
                        panic!("expected an external error");
                    };
                    assert_eq!(source.retry_hint(), expected);
                    let Error::RegionServer {
                        source,
                        code: actual_code,
                        ..
                    } = source.as_any().downcast_ref::<Error>().unwrap()
                    else {
                        panic!("expected a region server error");
                    };
                    assert_eq!(*actual_code, code);
                    assert_eq!(source.retry_hint(), original.retry_hint());
                    assert_eq!(source.to_string(), original.to_string());
                }
            }
        }
    }

    #[tokio::test]
    async fn test_shared_request_preserves_response_header_errors() {
        let server = RegionTestServer::start().await;
        server.service.responses.lock().unwrap().extend([
            Ok(PbRegionResponse::default()),
            Ok(build_response(StatusCode::RegionNotFound)),
        ]);
        let requester = server.create_requester(ChannelConfig::default(), false, false);
        let shared = Arc::new(build_insert_request());
        assert!(matches!(
            requester.handle(Arc::clone(&shared)).await.unwrap_err(),
            Error::IllegalDatabaseResponse { .. }
        ));
        assert!(matches!(
            requester.handle(shared).await.unwrap_err(),
            Error::Server {
                code: StatusCode::RegionNotFound,
                ..
            }
        ));
    }

    #[tokio::test]
    async fn test_shared_request_respects_configured_message_limits() {
        let server = RegionTestServer::start().await;
        let shared = Arc::new(build_insert_request());
        for send_limit in [true, false] {
            let mut config = ChannelConfig::default();
            if send_limit {
                config.max_send_message_size = "1B".parse().unwrap();
            } else {
                config.max_recv_message_size = "1B".parse().unwrap();
            }
            let mut baseline = api::v1::region::region_client::RegionClient::connect(format!(
                "http://{}",
                server.addr
            ))
            .await
            .unwrap()
            .max_encoding_message_size(config.max_send_message_size.as_bytes() as usize)
            .max_decoding_message_size(config.max_recv_message_size.as_bytes() as usize);
            let baseline: Error = baseline
                .handle(build_insert_request())
                .await
                .unwrap_err()
                .into();
            if !send_limit {
                assert_eq!(
                    baseline.tonic_code(),
                    Some(tonic::Code::OutOfRange),
                    "{baseline:?}"
                );
            }
            let requester = server.create_requester(config, false, false);
            let error = requester.handle(Arc::clone(&shared)).await.unwrap_err();
            assert_eq!(
                error.tonic_code(),
                baseline.tonic_code(),
                "shared: {error:?}; generated: {baseline:?}"
            );
            assert_eq!(
                error.status_code(),
                baseline.status_code(),
                "shared: {error:?}; generated: {baseline:?}"
            );
            let Error::RegionServer { source, .. } = &error else {
                panic!("expected a region server error: {error:?}");
            };
            assert_eq!(
                source.retry_hint(),
                baseline.retry_hint(),
                "shared: {error:?}; generated: {baseline:?}"
            );
            assert_eq!(
                server.service.requests.lock().unwrap().len(),
                if send_limit { 0 } else { 2 }
            );
        }
        let requester = server.create_requester(ChannelConfig::default(), false, false);
        requester.handle(shared).await.unwrap();
    }

    #[tokio::test]
    async fn test_inserts_and_bulk_insert_use_transport_compression() {
        let server = RegionTestServer::start().await;
        let requester = server.create_requester(ChannelConfig::default(), true, true);
        let send_only_requester = server.create_requester(ChannelConfig::default(), true, false);
        let accept_only_requester = server.create_requester(ChannelConfig::default(), false, true);
        let disabled_requester = server.create_requester(ChannelConfig::default(), false, false);
        let inserts = || RegionRequest {
            body: Some(region_request::Body::Inserts(Default::default())),
            ..Default::default()
        };
        let bulk_insert = || RegionRequest {
            body: Some(region_request::Body::BulkInsert(
                BulkInsertRequest::default(),
            )),
            ..Default::default()
        };

        requester.handle(inserts()).await.unwrap();
        send_only_requester.handle(inserts()).await.unwrap();
        accept_only_requester.handle(inserts()).await.unwrap();
        requester.handle(bulk_insert()).await.unwrap();
        disabled_requester.handle(bulk_insert()).await.unwrap();
        requester
            .handle(build_remote_dyn_filter_unregister_request(
                "query-1",
                RemoteDynFilterUnregister {
                    filter_id: "filter-1".to_string(),
                },
            ))
            .await
            .unwrap();

        assert_eq!(
            vec![
                (true, true),
                (true, false),
                (false, true),
                (true, true),
                (false, false),
                (false, false)
            ],
            server
                .service
                .requests
                .lock()
                .unwrap()
                .iter()
                .map(|(_, send, accept)| (*send, *accept))
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_check_response_header() {
        let result = check_response_header(&None);
        assert!(matches!(
            result.unwrap_err(),
            Error::IllegalDatabaseResponse { .. }
        ));

        let result = check_response_header(&Some(ResponseHeader { status: None }));
        assert!(matches!(
            result.unwrap_err(),
            Error::IllegalDatabaseResponse { .. }
        ));

        let result = check_response_header(&Some(ResponseHeader {
            status: Some(PbStatus {
                status_code: StatusCode::Success as u32,
                err_msg: String::default(),
            }),
        }));
        assert!(result.is_ok());

        let result = check_response_header(&Some(ResponseHeader {
            status: Some(PbStatus {
                status_code: u32::MAX,
                err_msg: String::default(),
            }),
        }));
        assert!(matches!(
            result.unwrap_err(),
            Error::IllegalDatabaseResponse { .. }
        ));

        let result = check_response_header(&Some(ResponseHeader {
            status: Some(PbStatus {
                status_code: StatusCode::Internal as u32,
                err_msg: "blabla".to_string(),
            }),
        }));
        let Error::Server { code, msg, .. } = result.unwrap_err() else {
            unreachable!()
        };
        assert_eq!(code, StatusCode::Internal);
        assert_eq!(msg, "blabla");
    }

    #[test]
    fn test_build_remote_dyn_filter_request_sets_header_and_body() {
        let request = build_remote_dyn_filter_update_request(
            "query-1",
            RemoteDynFilterUpdate {
                filter_id: "filter-1".to_string(),
                payload: vec![1, 2, 3],
                generation: 7,
                is_complete: false,
            },
        );

        request.header.expect("remote dyn filter header must exist");

        let body = request.body.expect("remote dyn filter body must exist");
        let region_request::Body::RemoteDynFilter(remote_request) = body else {
            panic!("expected remote dyn filter request body");
        };

        assert_eq!(remote_request.query_id, "query-1");
        assert!(matches!(
            remote_request.action,
            Some(remote_dyn_filter_request::Action::Update(_))
        ));
    }

    #[test]
    fn test_build_remote_dyn_filter_unregister_request_sets_header_and_body() {
        let request = build_remote_dyn_filter_unregister_request(
            "query-1",
            RemoteDynFilterUnregister {
                filter_id: "filter-9".to_string(),
            },
        );

        request.header.expect("remote dyn filter header must exist");

        let body = request.body.expect("remote dyn filter body must exist");
        let region_request::Body::RemoteDynFilter(remote_request) = body else {
            panic!("expected remote dyn filter request body");
        };

        assert_eq!(remote_request.query_id, "query-1");
        assert!(matches!(
            remote_request.action,
            Some(remote_dyn_filter_request::Action::Unregister(_))
        ));
    }
}
