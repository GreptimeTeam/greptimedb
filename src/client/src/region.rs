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
#[cfg(test)]
mod named_raw_series_tests;

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
use arrow_flight::{Action, Ticket};
use async_stream::stream;
use async_trait::async_trait;
use common_error::ext::{BoxedError, ErrorExt};
use common_error::status_code::StatusCode;
use common_grpc::flight::{FlightDecoder, FlightMessage};
use common_meta::error::{self as meta_error, Result as MetaResult};
use common_meta::node_manager::Datanode;
use common_query::request::{
    PREPARE_RAW_SERIES_ACTION, PreparedNamedRawSeries, QueryRequest, RawSeriesRequest,
    decode_prepare_raw_series_response,
};
use common_recordbatch::error::ExternalSnafu;
use common_recordbatch::{RecordBatch, RecordBatchStreamWrapper, SendableRecordBatchStream};
use common_telemetry::error;
use common_telemetry::tracing::Span;
use common_telemetry::tracing_context::TracingContext;
use futures_util::Stream;
use prost::Message;
use query::query_engine::DefaultSerializer;
use snafu::{OptionExt, ResultExt, location};
use store_api::region_engine::SendableRawSeriesStream;
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

    /// Non-consumingly checks whether a named raw-series preflight is supported.
    ///
    /// The request is forwarded unmodified: the plan travels as the already-encoded
    /// Substrait plan inside the existing region query wire shape, exactly like
    /// [`Datanode::handle_query`]. The peer owns the admission policy and answers
    /// with the approved binding and ordering, or with one of the two frozen
    /// unsupported shapes.
    ///
    /// Only a peer that rejects the action call itself with a bare `Unimplemented`
    /// status -- no error-code metadata -- is reported as unsupported without
    /// answering the contract. A peer that implements the action answers with a
    /// single explicit `null` body and a clean end of stream; that is the
    /// protocol-level unsupported answer, decoded by
    /// [`decode_prepare_raw_series_response`].
    ///
    /// Every other failure is an error with its status and peer address preserved,
    /// including an `Unimplemented` status that carries error metadata, a response
    /// body that does not decode, a response that is not exactly one result, and a
    /// response that does not end before the preflight budget elapses.
    async fn prepare_named_raw_series_query(
        &self,
        request: QueryRequest,
    ) -> MetaResult<Option<PreparedNamedRawSeries>> {
        self.prepare_named_raw_series_query_with_timeout(request, FLIGHT_DO_GET_TIMEOUT)
            .await
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

    /// Sends the preflight action and reads its single answer under one deadline.
    ///
    /// The whole interaction -- the action call, the single-result read, and the read
    /// that must observe the end of the response -- runs inside one `timeout`. The
    /// per-request timeout header is kept for the peer, but a gRPC deadline only
    /// bounds the response headers, so the body reads need their own bound: without
    /// it a peer could hold the response open forever after its first result.
    ///
    /// The deadline is a helper parameter only so tests can exercise it without
    /// waiting for the production budget; production callers always pass
    /// [`FLIGHT_DO_GET_TIMEOUT`].
    async fn prepare_named_raw_series_query_with_timeout(
        &self,
        request: QueryRequest,
        timeout: Duration,
    ) -> MetaResult<Option<PreparedNamedRawSeries>> {
        let plan = DFLogicalSubstraitConvertor
            .encode(&request.plan, DefaultSerializer)
            .map_err(BoxedError::new)
            .context(meta_error::ExternalSnafu)?
            .to_vec();
        let body = api::v1::region::QueryRequest {
            header: request.header,
            region_id: request.region_id.as_u64(),
            plan,
        }
        .encode_to_vec()
        .into();

        let mut flight_client = self
            .client
            .make_flight_client(self.send_compression, self.accept_compression)?;
        let addr = flight_client.addr().to_string();
        let mut request = tonic::Request::new(Action {
            r#type: PREPARE_RAW_SERIES_ACTION.to_string(),
            body,
        });
        request.set_timeout(timeout);

        let result = tokio::time::timeout(timeout, async {
            let response = match flight_client.mut_inner().do_action(request).await {
                Ok(response) => response,
                // Only a peer that rejects the action call itself answers as unsupported.
                Err(status) if is_unsupported_preflight(&status) => return Ok(None),
                Err(status) => return Err(flight_action_error(&addr, status)),
            };

            read_prepare_raw_series_response(&addr, response.into_inner()).await
        })
        .await
        // An elapsed deadline is a real error: it must never be read as unsupported.
        .unwrap_or_else(|_| {
            Err(flight_action_error(
                &addr,
                tonic::Status::deadline_exceeded(format!(
                    "{} did not answer within {timeout:?}",
                    PREPARE_RAW_SERIES_ACTION
                )),
            ))
        });

        result.map_err(Into::into)
    }

    /// Sends an explicit trusted internal raw-series request and decodes its Flight frames.
    ///
    /// The binding is an expectation supplied by the already-authorized internal planner; it is
    /// not evidence of provenance or an independent authorization grant.
    pub async fn handle_raw_series_query(
        &self,
        request: QueryRequest,
        binding: RawSeriesRequest,
    ) -> MetaResult<SendableRawSeriesStream> {
        Datanode::handle_raw_series_query(self, request, binding).await
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

/// Returns true only for a bare `Unimplemented` answer to the preflight action.
///
/// A peer that does not implement the action rejects the action call itself with a
/// bare `Unimplemented` status, and that is the only answer reported as unsupported
/// without a contract body. An `Unimplemented` status that carries error-code
/// metadata is a real failure of a peer that does implement the action, and so is
/// any error delivered after the response started, including an error delivered as
/// the first response stream item. A body that carries `supported: null` is not
/// classified here: it is the protocol's own unsupported answer and is decoded by
/// [`decode_prepare_raw_series_response`] after a clean end of stream.
fn is_unsupported_preflight(status: &tonic::Status) -> bool {
    status.code() == tonic::Code::Unimplemented
        && !status
            .metadata()
            .contains_key(common_error::GREPTIME_DB_HEADER_ERROR_CODE)
}

/// Logs a preflight transport failure and maps it with the shared region-server mapping.
fn flight_action_error(addr: &str, status: tonic::Status) -> error::Error {
    let tonic_code = status.code();
    let failure = map_region_error(status, addr.to_string());
    error!(
        failure; "Failed to do Flight action {}, addr: {}, code: {}",
        PREPARE_RAW_SERIES_ACTION, addr, tonic_code
    );
    failure
}

/// Reads the preflight answer, requiring exactly one result and a clean end of stream.
///
/// The end of the response is validated before the body is decoded, and at most two
/// stream items are read, so a missing result, an extra result, a body that does not
/// decode, or a stream error can never be reported as "unsupported" or "supported".
/// In particular an explicit `null` body is only the unsupported answer when it is
/// the single result and the stream then ended cleanly.
async fn read_prepare_raw_series_response(
    addr: &str,
    mut response: tonic::codec::Streaming<arrow_flight::Result>,
) -> Result<Option<PreparedNamedRawSeries>> {
    let Some(first) = response.next().await else {
        return IllegalFlightMessagesSnafu {
            reason: format!("{} response carries no result", PREPARE_RAW_SERIES_ACTION),
        }
        .fail();
    };
    let first = first.map_err(|status| flight_action_error(addr, status))?;

    match response.next().await {
        None => {}
        Some(Ok(_)) => {
            return IllegalFlightMessagesSnafu {
                reason: format!(
                    "{} response carries more than one result",
                    PREPARE_RAW_SERIES_ACTION
                ),
            }
            .fail();
        }
        Some(Err(status)) => return Err(flight_action_error(addr, status)),
    }

    match decode_prepare_raw_series_response(&first.body) {
        Ok(prepared) => Ok(prepared),
        Err(error) => IllegalFlightMessagesSnafu {
            reason: format!("{} response is invalid: {error}", PREPARE_RAW_SERIES_ACTION),
        }
        .fail(),
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
        ColumnDataType, ColumnSchema as PbColumnSchema, QueryContext as ApiQueryContext, Row, Rows,
        Status as PbStatus, Value,
    };
    use common_error::ext::RetryHint;
    use common_grpc::channel_manager::{ChannelConfig, ChannelManager};
    use common_query::request::RAW_SERIES_REQUEST_EXTENSION_KEY;
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

    fn raw_series_fixture() -> RawSeriesRequest {
        RawSeriesRequest::from_extension_value(include_str!(
            "../../../tests/compatibility/wire/raw_series_request/v1_request.json"
        ))
        .unwrap()
    }

    #[tokio::test]
    async fn raw_query_rejects_existing_marker_before_transport() {
        let requester = RegionRequester::new(Client::new(), false, false);
        let mut context = ApiQueryContext::default();
        context.extensions.insert(
            RAW_SERIES_REQUEST_EXTENSION_KEY.to_string(),
            "already-set".to_string(),
        );
        let request = QueryRequest {
            header: Some(RegionRequestHeader {
                query_context: Some(context),
                ..Default::default()
            }),
            region_id: raw_series_fixture().physical_metadata.region_id,
            plan: Default::default(),
        };
        let error = requester
            .handle_raw_series_query(request, raw_series_fixture())
            .await
            .err()
            .expect("conflicting marker should be rejected before transport");
        assert_eq!(error.status_code(), StatusCode::InvalidArguments);
    }

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
