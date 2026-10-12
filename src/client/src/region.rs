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
use std::time::Duration;

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
use common_error::ext::{BoxedError, ErrorExt};
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
use futures_util::Stream;
use prost::Message;
use query::query_engine::DefaultSerializer;
use snafu::{OptionExt, ResultExt, location};
use substrait::{DFLogicalSubstraitConvertor, SubstraitPlan};
use tokio_stream::StreamExt;
use tonic::codec::CompressionEncoding;

use crate::error::{
    self, FlightGetSnafu, IllegalDatabaseResponseSnafu, IllegalFlightMessagesSnafu,
    MissingFieldSnafu, Result, ServerSnafu,
};
use crate::flight::{FlightMessageReader, decode_flight_data};
use crate::region::codec::RegionCodec;
use crate::{Client, metrics};

const FLIGHT_DO_GET_TIMEOUT: Duration = Duration::from_secs(10);

/// Sends region operations through a configured gRPC client.
#[derive(Debug)]
pub struct RegionRequester {
    client: Client,
    send_compression: bool,
    accept_compression: bool,
}

#[async_trait]
impl Datanode for RegionRequester {
    async fn handle(&self, request: RegionRequest) -> MetaResult<RegionResponse> {
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
        // Limit Flight DoGet response time without limiting query stream execution.
        let addr = flight_client.addr().to_string();
        let mut request = tonic::Request::new(ticket);
        request.set_timeout(FLIGHT_DO_GET_TIMEOUT);
        let response = flight_client
            .mut_inner()
            .do_get(request)
            .await
            .or_else(|e| {
                let tonic_code = e.code();
                let e: error::Error = e.into();
                error!(
                    e; "Failed to do Flight get, addr: {}, code: {}",
                    addr,
                    tonic_code
                );
                Err(BoxedError::new(e)).with_context(|_| FlightGetSnafu {
                    addr: addr.clone(),
                    tonic_code,
                })
            })?;

        let flight_data_stream = response.into_inner();
        let mut decoder = FlightDecoder::default();

        let flight_message_stream = flight_data_stream
            .filter_map(move |flight_data| decode_flight_data(&mut decoder, flight_data));

        recordbatches_from_flight_message_stream(addr, flight_message_stream).await
    }

    /// Sends an owned or shared request without deep-cloning its protobuf payload.
    ///
    /// Accepts both [`RegionRequest`] and [`Arc<RegionRequest>`]. Callers can retain a
    /// shared request for another attempt; this method does not retry or determine
    /// whether replaying the request is safe.
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
                client = client.send_compressed(CompressionEncoding::Zstd);
            }
            if self.accept_compression {
                client = client.accept_compressed(CompressionEncoding::Zstd);
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
        .map_err(|e| {
            let code = e.code();
            // Uses `Error::RegionServer` instead of `Error::Server`
            error::Error::RegionServer {
                addr,
                code,
                source: BoxedError::new(error::Error::from(e)),
                location: location!(),
            }
        })?
        .into_inner();

        check_response_header(&response.header)?;

        Ok(RegionResponse::from_region_response(response))
    }
}

async fn recordbatches_from_flight_message_stream<S>(
    addr: String,
    flight_message_stream: S,
) -> Result<SendableRecordBatchStream>
where
    S: Stream<Item = Result<FlightMessage>> + Send + Unpin + 'static,
{
    let mut reader = FlightMessageReader::new(addr.clone(), flight_message_stream);
    let FlightMessage::Schema(schema) = reader
        .read_first()
        .await
        .map_err(|error| flight_stream_error(reader.remote_addr(), error))?
    else {
        return IllegalFlightMessagesSnafu {
            reason: "Expect schema to be the first flight message",
        }
        .fail()
        .map_err(|error| flight_stream_error(reader.remote_addr(), error));
    };

    let metrics = Arc::new(ArcSwapOption::from(None));
    let metrics_ref = metrics.clone();

    let tracing_context = TracingContext::from_current_span();

    let schema =
        Arc::new(datatypes::schema::Schema::try_from(schema).context(error::ConvertSchemaSnafu)?);
    let schema_cloned = schema.clone();
    let stream_addr = addr;
    let stream = Box::pin(stream!({
        let _span = tracing_context.attach(common_telemetry::tracing::info_span!(
            "poll_flight_data_stream"
        ));

        loop {
            let flight_message = match reader.read_next().await {
                Ok(Some(message)) => message,
                Ok(None) => break,
                Err(error) => {
                    yield Err(BoxedError::new(flight_stream_error(&stream_addr, error)))
                        .context(ExternalSnafu);
                    break;
                }
            };

            match flight_message {
                FlightMessage::RecordBatch(record_batch) => {
                    // Deliver each batch immediately. In particular, do not
                    // wait for a possible following Metrics message; it is
                    // consumed on the next poll of this stream.
                    yield Ok(RecordBatch::from_df_record_batch(
                        schema_cloned.clone(),
                        record_batch,
                    ));
                }
                FlightMessage::Metrics(s) => {
                    // Metrics may arrive before the next RecordBatch.
                    match serde_json::from_str(&s) {
                        Ok(metrics) => {
                            metrics_ref.swap(Some(Arc::new(metrics)));
                        }
                        Err(error) => {
                            common_telemetry::warn!(
                                "Failed to decode region Flight metrics: {}",
                                error
                            );
                        }
                    }
                    continue;
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

fn flight_stream_error(addr: &str, error: error::Error) -> error::Error {
    let tonic_code = error.tonic_code().unwrap_or(tonic::Code::Unknown);
    if error.status_code().should_log_error() {
        error!(
            error; "Failed to receive Flight data, addr: {}, code: {}",
            addr,
            tonic_code
        );
    }

    error::Error::FlightGet {
        addr: addr.to_string(),
        tonic_code,
        source: BoxedError::new(error),
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
    use common_error::ext::RetryHint;
    use common_grpc::channel_manager::{ChannelConfig, ChannelManager};
    use common_recordbatch::adapter::RecordBatchMetrics;
    use datatypes::arrow::array::Int32Array;
    use datatypes::prelude::{ConcreteDataType, VectorRef};
    use datatypes::schema::{ColumnSchema, Schema};
    use datatypes::vectors::Int32Vector;
    use futures_util::stream;
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
            matches!(error, error::Error::RegionServer { addr, code: tonic::Code::Unavailable, .. } if addr == server.addr)
        );
        requester.handle(Arc::clone(&shared)).await.unwrap();
        let requests = server.service.requests.lock().unwrap();
        assert_eq!(requests.len(), 2);
        assert_eq!(&requests[0].0, shared.as_ref());
        assert_eq!(&requests[1].0, shared.as_ref());
    }

    #[test]
    fn test_retry_hint_wire_compatibility() {
        #[derive(serde::Deserialize)]
        struct Fixture {
            name: String,
            wire_hint: Option<String>,
            expected_hint: String,
        }

        let fixtures: Vec<Fixture> = serde_json::from_str(include_str!(
            "../../../tests/compatibility/wire/write_retry/retry_hints.json"
        ))
        .unwrap();
        assert_eq!(fixtures.len(), 5);
        for fixture in fixtures {
            let mut status = Status::failed_precondition("region unavailable");
            status.metadata_mut().insert(
                common_error::GREPTIME_DB_HEADER_ERROR_CODE,
                (StatusCode::RegionNotReady as u32)
                    .to_string()
                    .parse()
                    .unwrap(),
            );
            if let Some(hint) = fixture.wire_hint {
                status.metadata_mut().insert(
                    common_error::GREPTIME_DB_HEADER_ERROR_RETRY_HINT,
                    hint.parse().unwrap(),
                );
            }
            let error: error::Error = status.into();
            assert_eq!(
                error.retry_hint().as_str(),
                fixture.expected_hint,
                "{}",
                fixture.name
            );
            assert_eq!(
                error.status_code(),
                StatusCode::RegionNotReady,
                "{}",
                fixture.name
            );
            assert_eq!(
                error.tonic_code(),
                Some(tonic::Code::FailedPrecondition),
                "{}",
                fixture.name
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
            let baseline: error::Error = baseline
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
            let error = Datanode::handle(&requester, build_insert_request())
                .await
                .unwrap_err();
            assert_eq!(error.status_code(), StatusCode::RegionNotFound);
            assert_eq!(error.retry_hint(), hint);
            match hint {
                RetryHint::Retryable | RetryHint::RetryAfterRouteRefresh => assert!(matches!(
                    error,
                    meta_error::Error::RetryLater {
                        clean_poisons: false,
                        ..
                    }
                )),
                RetryHint::NonRetryable => {
                    assert!(matches!(error, meta_error::Error::External { .. }))
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
            error::Error::IllegalDatabaseResponse { .. }
        ));
        assert!(matches!(
            requester.handle(shared).await.unwrap_err(),
            error::Error::Server {
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
            let baseline: error::Error = baseline
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
            assert_eq!(
                error.retry_hint(),
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
    fn test_flight_stream_error_preserves_peer_address() {
        let error = flight_stream_error(
            "127.0.0.1:4001",
            tonic::Status::unavailable("datanode unavailable").into(),
        );

        assert!(matches!(
            error,
            error::Error::FlightGet {
                addr,
                tonic_code: tonic::Code::Unavailable,
                ..
            } if addr == "127.0.0.1:4001"
        ));
    }

    #[tokio::test]
    async fn test_empty_flight_stream_preserves_peer_address() {
        let Err(error) = recordbatches_from_flight_message_stream(
            "127.0.0.1:4001".to_string(),
            stream::empty::<Result<FlightMessage>>(),
        )
        .await
        else {
            panic!("expected empty Flight stream to fail");
        };

        assert!(matches!(
            error,
            error::Error::FlightGet {
                addr,
                tonic_code: tonic::Code::Unknown,
                ..
            } if addr == "127.0.0.1:4001"
        ));
    }

    fn test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![ColumnSchema::new(
            "v",
            ConcreteDataType::int32_datatype(),
            false,
        )]))
    }

    fn test_metrics_json() -> String {
        serde_json::to_string(&RecordBatchMetrics {
            elapsed_compute: 7,
            ..Default::default()
        })
        .unwrap()
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

    #[tokio::test]
    async fn test_record_batch_stream_continues_after_pre_batch_metrics() {
        let schema = test_schema();
        let batch = RecordBatch::new(
            schema.clone(),
            vec![Arc::new(Int32Vector::from_slice([1])) as VectorRef],
        )
        .unwrap();

        let mut recordbatches = recordbatches_from_flight_message_stream(
            "test-peer".to_string(),
            stream::iter(vec![
                Ok(FlightMessage::Schema(schema.arrow_schema().clone())),
                Ok(FlightMessage::Metrics(test_metrics_json())),
                Ok(FlightMessage::RecordBatch(batch.into_df_record_batch())),
            ]),
        )
        .await
        .unwrap();

        let batch = recordbatches.next().await.unwrap().unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert!(recordbatches.next().await.is_none());

        let metrics = recordbatches.metrics().unwrap();
        assert_eq!(metrics.elapsed_compute, 7);
    }

    #[tokio::test]
    async fn test_record_batch_is_yielded_without_waiting_for_next_message() {
        let schema = test_schema();
        let batch = RecordBatch::new(
            schema.clone(),
            vec![Arc::new(Int32Vector::from_slice([1])) as VectorRef],
        )
        .unwrap();

        let messages = stream::iter(vec![
            Ok(FlightMessage::Schema(schema.arrow_schema().clone())),
            Ok(FlightMessage::RecordBatch(batch.into_df_record_batch())),
        ])
        .chain(stream::pending::<Result<FlightMessage>>());
        let mut recordbatches =
            recordbatches_from_flight_message_stream("test-peer".to_string(), messages)
                .await
                .unwrap();

        let batch = tokio::time::timeout(Duration::from_secs(1), recordbatches.next())
            .await
            .expect("the first batch must not wait for lookahead")
            .unwrap()
            .unwrap();
        assert_eq!(batch.num_rows(), 1);
    }

    #[tokio::test]
    async fn test_malformed_region_metrics_are_non_fatal() {
        let schema = test_schema();
        let batch = RecordBatch::new(
            schema.clone(),
            vec![Arc::new(Int32Vector::from_slice([1])) as VectorRef],
        )
        .unwrap();
        let mut recordbatches = recordbatches_from_flight_message_stream(
            "test-peer".to_string(),
            stream::iter(vec![
                Ok(FlightMessage::Schema(schema.arrow_schema().clone())),
                Ok(FlightMessage::Metrics("{not-json}".to_string())),
                Ok(FlightMessage::RecordBatch(batch.into_df_record_batch())),
            ]),
        )
        .await
        .unwrap();

        assert_eq!(recordbatches.next().await.unwrap().unwrap().num_rows(), 1);
        assert!(recordbatches.next().await.is_none());
    }

    #[tokio::test]
    async fn test_record_batch_stream_preserves_following_record_batch() {
        let schema = test_schema();
        let first_batch = RecordBatch::new(
            schema.clone(),
            vec![Arc::new(Int32Vector::from_slice([1])) as VectorRef],
        )
        .unwrap();
        let second_batch = RecordBatch::new(
            schema.clone(),
            vec![Arc::new(Int32Vector::from_slice([2])) as VectorRef],
        )
        .unwrap();

        let mut recordbatches = recordbatches_from_flight_message_stream(
            "test-peer".to_string(),
            stream::iter(vec![
                Ok(FlightMessage::Schema(schema.arrow_schema().clone())),
                Ok(FlightMessage::RecordBatch(
                    first_batch.into_df_record_batch(),
                )),
                Ok(FlightMessage::RecordBatch(
                    second_batch.into_df_record_batch(),
                )),
            ]),
        )
        .await
        .unwrap();

        let first_batch = recordbatches.next().await.unwrap().unwrap();
        let second_batch = recordbatches.next().await.unwrap().unwrap();
        assert_eq!(
            first_batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0),
            1
        );
        assert_eq!(
            second_batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0),
            2
        );
        assert!(recordbatches.next().await.is_none());
    }

    #[tokio::test]
    async fn test_record_batch_stream_captures_final_metrics_after_record_batch() {
        let schema = test_schema();
        let batch = RecordBatch::new(
            schema.clone(),
            vec![Arc::new(Int32Vector::from_slice([1])) as VectorRef],
        )
        .unwrap();
        let final_metrics = serde_json::to_string(&RecordBatchMetrics {
            elapsed_compute: 99,
            ..Default::default()
        })
        .unwrap();
        let mut recordbatches = recordbatches_from_flight_message_stream(
            "test-peer".to_string(),
            stream::iter(vec![
                Ok(FlightMessage::Schema(schema.arrow_schema().clone())),
                Ok(FlightMessage::RecordBatch(batch.into_df_record_batch())),
                Ok(FlightMessage::Metrics(final_metrics)),
            ]),
        )
        .await
        .unwrap();

        assert_eq!(recordbatches.next().await.unwrap().unwrap().num_rows(), 1);
        assert!(recordbatches.next().await.is_none());
        assert_eq!(recordbatches.metrics().unwrap().elapsed_compute, 99);
    }

    #[tokio::test]
    async fn test_record_batch_stream_exposes_error_after_pre_batch_metrics() {
        let schema = test_schema();
        let mut recordbatches = recordbatches_from_flight_message_stream(
            "test-peer".to_string(),
            stream::iter(vec![
                Ok(FlightMessage::Schema(schema.arrow_schema().clone())),
                Ok(FlightMessage::Metrics(test_metrics_json())),
                Err(Error::from(Status::internal("boom after metrics"))),
            ]),
        )
        .await
        .unwrap();

        let err = recordbatches.next().await.unwrap().unwrap_err();
        assert_eq!("External error", err.to_string());
        assert!(
            format!("{err:?}").contains("boom after metrics"),
            "unexpected error: {err:?}"
        );
        assert!(recordbatches.next().await.is_none());
    }
}
