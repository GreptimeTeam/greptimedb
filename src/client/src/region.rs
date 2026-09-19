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
use common_query::request::{
    QUERY_INTERNAL_STAGE_EXTENSION_KEY, QueryRequest, mark_query_internal_stage,
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
use substrait::{DFLogicalSubstraitConvertor, SubstraitPlan};
use tokio_stream::StreamExt;
use tonic::codec::CompressionEncoding;

use crate::error::{
    self, FlightGetSnafu, IllegalDatabaseResponseSnafu, IllegalFlightMessagesSnafu,
    MissingFieldSnafu, Result, ServerSnafu,
};
use crate::flight::{FlightMessageKind, FlightMessageReader, decode_flight_data};
use crate::{Client, metrics};

const FLIGHT_DO_GET_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug)]
pub struct RegionRequester {
    client: Client,
    send_compression: bool,
    accept_compression: bool,
}

#[async_trait]
impl Datanode for RegionRequester {
    async fn handle(&self, request: RegionRequest) -> MetaResult<RegionResponse> {
        self.handle_inner(request).await.map_err(|err| {
            if err.should_retry() {
                meta_error::Error::RetryLater {
                    source: BoxedError::new(err),
                    clean_poisons: false,
                }
            } else {
                meta_error::Error::External {
                    source: BoxedError::new(err),
                    location: location!(),
                }
            }
        })
    }

    async fn handle_query(&self, request: QueryRequest) -> MetaResult<SendableRecordBatchStream> {
        let plan = DFLogicalSubstraitConvertor
            .encode(&request.plan, DefaultSerializer)
            .map_err(BoxedError::new)
            .context(meta_error::ExternalSnafu)?
            .to_vec();
        let request = api::v1::region::QueryRequest {
            header: encode_region_request_header(request.header, request.internal),
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

/// Encodes the header of a region query, carrying [`QueryRequest::internal`] to the peer datanode.
///
/// The wire format of a region query (`api::v1::region::QueryRequest`) has no field for the marker,
/// so an internal request (an execution stage of the plan the sender received) is marked with the
/// [`QUERY_INTERNAL_STAGE_EXTENSION_KEY`] extension of the query context of the header, which the
/// peer reads back in `RegionServer::handle_remote_read_inner`. Without it the peer can't tell an
/// inner stage of another query from an independent query and takes a concurrency permit for it,
/// which makes the outer query wait for the inner stage while the inner stage waits for the permit
/// held by the outer query.
///
/// A header-less request keeps no header unless it is internal. An internal request that has no
/// header is encoded with a header that carries the marker only, so the peer builds the query
/// context of the request from that header, i.e. without a remote query id (the peer generates a
/// fresh query context, with an id, only for a request that has no header at all). That only
/// disables the remote dynamic filter cleanup of the request.
fn encode_region_request_header(
    header: Option<RegionRequestHeader>,
    internal: bool,
) -> Option<RegionRequestHeader> {
    let mut header = match header {
        Some(header) => header,
        // A request that has no header is an independent query: it has no marker to carry.
        None if !internal => return None,
        None => RegionRequestHeader::default(),
    };
    if internal {
        // Creates the query context if the caller didn't set one: the peer honors the marker only
        // in the query context of the header.
        mark_query_internal_stage(
            &mut header
                .query_context
                .get_or_insert_with(api::v1::QueryContext::default)
                .extensions,
        );
    } else if let Some(query_context) = header.query_context.as_mut() {
        // [`QueryRequest::internal`] owns the marker: a header that carries it while the request
        // claims to be an independent query must not make the peer skip its concurrency limiter.
        query_context
            .extensions
            .remove(QUERY_INTERNAL_STAGE_EXTENSION_KEY);
    }
    Some(header)
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

    async fn handle_inner(&self, request: RegionRequest) -> Result<RegionResponse> {
        let request_body = request
            .body
            .as_ref()
            .with_context(|| MissingFieldSnafu { field: "body" })?;
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

        let response = client
            .handle(request)
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

    pub async fn handle(&self, request: RegionRequest) -> Result<RegionResponse> {
        self.handle_inner(request).await
    }

    pub async fn handle_remote_dyn_filter_update(
        &self,
        query_id: impl Into<String>,
        update: RemoteDynFilterUpdate,
    ) -> Result<RegionResponse> {
        self.handle_inner(build_remote_dyn_filter_update_request(query_id, update))
            .await
    }

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

        let mut stream_ended = false;

        while !stream_ended {
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
                    let result_to_yield =
                        RecordBatch::from_df_record_batch(schema_cloned.clone(), record_batch);

                    // Metrics follow a batch so MergeScan can observe them before yielding it.
                    match reader.peek_next_message_kind().await {
                        Ok(Some(FlightMessageKind::Metrics)) => {
                            let metrics_message = match reader.read_next().await {
                                Ok(Some(FlightMessage::Metrics(metrics))) => metrics,
                                Ok(Some(_) | None) => {
                                    yield IllegalFlightMessagesSnafu {
                                        reason: "Flight stream changed after peek",
                                    }
                                    .fail()
                                    .map_err(BoxedError::new)
                                    .context(ExternalSnafu);
                                    break;
                                }
                                Err(error) => {
                                    yield Err(BoxedError::new(flight_stream_error(
                                        &stream_addr,
                                        error,
                                    )))
                                    .context(ExternalSnafu);
                                    break;
                                }
                            };
                            let metrics = serde_json::from_str(&metrics_message).ok().map(Arc::new);
                            metrics_ref.swap(metrics);
                        }
                        Ok(Some(FlightMessageKind::RecordBatch)) => {}
                        Ok(Some(FlightMessageKind::Schema | FlightMessageKind::AffectedRows)) => {
                            yield IllegalFlightMessagesSnafu {
                                reason: "A RecordBatch message can only be succeeded by a Metrics message or another RecordBatch message"
                            }
                            .fail()
                            .map_err(BoxedError::new)
                            .context(ExternalSnafu);
                            break;
                        }
                        Ok(None) => stream_ended = true,
                        Err(error) => {
                            yield Err(BoxedError::new(flight_stream_error(&stream_addr, error)))
                                .context(ExternalSnafu);
                            break;
                        }
                    }

                    yield Ok(result_to_yield);
                }
                FlightMessage::Metrics(s) => {
                    // Metrics may arrive before the next RecordBatch.
                    let m = serde_json::from_str(&s).ok().map(Arc::new);
                    metrics_ref.swap(m);
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
mod test {
    use api::v1::Status as PbStatus;
    use api::v1::region::region_server::{Region, RegionServer};
    use api::v1::region::{
        BulkInsertRequest, RegionResponse as PbRegionResponse, RemoteDynFilterUnregister,
        RemoteDynFilterUpdate, region_request, remote_dyn_filter_request,
    };
    use common_recordbatch::adapter::RecordBatchMetrics;
    use datatypes::arrow::array::Int32Array;
    use datatypes::prelude::{ConcreteDataType, VectorRef};
    use datatypes::schema::{ColumnSchema, Schema};
    use datatypes::vectors::Int32Vector;
    use futures_util::stream;
    use tokio::net::TcpListener;
    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::codec::CompressionEncoding;
    use tonic::{Request, Response, Status};

    use super::*;
    use crate::Error::{self, IllegalDatabaseResponse, Server};

    #[derive(Clone)]
    struct CompressionRecordingRegionService {
        zstd_headers: Arc<std::sync::Mutex<Vec<(bool, bool)>>>,
    }

    #[tonic::async_trait]
    impl Region for CompressionRecordingRegionService {
        async fn handle(
            &self,
            request: Request<RegionRequest>,
        ) -> std::result::Result<Response<PbRegionResponse>, Status> {
            let metadata = request.metadata();
            let sends_zstd = metadata
                .get("grpc-encoding")
                .and_then(|v| v.to_str().ok())
                .is_some_and(|value| value == "zstd");
            let accepts_zstd = metadata
                .get("grpc-accept-encoding")
                .and_then(|value| value.to_str().ok())
                .is_some_and(|value| value.split(',').any(|encoding| encoding == "zstd"));
            self.zstd_headers
                .lock()
                .unwrap()
                .push((sends_zstd, accepts_zstd));

            Ok(Response::new(PbRegionResponse {
                header: Some(ResponseHeader {
                    status: Some(PbStatus {
                        status_code: StatusCode::Success as u32,
                        ..Default::default()
                    }),
                }),
                ..Default::default()
            }))
        }
    }

    #[tokio::test]
    async fn test_inserts_and_bulk_insert_use_transport_compression() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let zstd_headers = Arc::new(std::sync::Mutex::new(Vec::new()));
        let service = CompressionRecordingRegionService {
            zstd_headers: zstd_headers.clone(),
        };
        let server = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(
                    RegionServer::new(service)
                        .accept_compressed(CompressionEncoding::Zstd)
                        .send_compressed(CompressionEncoding::Zstd),
                )
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .unwrap();
        });
        let client = Client::with_urls([addr.to_string()]);
        let requester = RegionRequester::new(client.clone(), true, true);
        let send_only_requester = RegionRequester::new(client.clone(), true, false);
        let accept_only_requester = RegionRequester::new(client.clone(), false, true);
        let disabled_requester = RegionRequester::new(client, false, false);
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
            *zstd_headers.lock().unwrap()
        );
        server.abort();
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
            IllegalDatabaseResponse { .. }
        ));

        let result = check_response_header(&Some(ResponseHeader { status: None }));
        assert!(matches!(
            result.unwrap_err(),
            IllegalDatabaseResponse { .. }
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
            IllegalDatabaseResponse { .. }
        ));

        let result = check_response_header(&Some(ResponseHeader {
            status: Some(PbStatus {
                status_code: StatusCode::Internal as u32,
                err_msg: "blabla".to_string(),
            }),
        }));
        let Server { code, msg, .. } = result.unwrap_err() else {
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
    async fn test_record_batch_stream_updates_following_metrics_before_yielding_batch() {
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
                Ok(FlightMessage::RecordBatch(batch.into_df_record_batch())),
                Ok(FlightMessage::Metrics(test_metrics_json())),
            ]),
        )
        .await
        .unwrap();

        let batch = recordbatches.next().await.unwrap().unwrap();
        assert_eq!(batch.num_rows(), 1);

        let metrics = recordbatches.metrics().unwrap();
        assert_eq!(metrics.elapsed_compute, 7);
        assert!(recordbatches.next().await.is_none());
    }

    #[tokio::test]
    async fn test_record_batch_stream_preserves_peeked_record_batch() {
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

#[cfg(test)]
mod internal_stage_test {
    use std::collections::HashMap;

    use common_query::request::{QUERY_INTERNAL_STAGE_EXTENSION_VALUE, is_internal_stage};

    use super::*;

    /// Reads the marker of a header the way the receiver does: `RegionServer::do_get` builds the
    /// query context of the request from the header, `session::context::QueryContext` copies the
    /// extensions of that context verbatim.
    fn extensions_of(header: &RegionRequestHeader) -> HashMap<String, String> {
        header
            .query_context
            .as_ref()
            .map(|ctx| ctx.extensions.clone())
            .unwrap_or_default()
    }

    #[test]
    fn test_encode_region_request_header_marks_internal_request() {
        // An internal request carries the marker even when its caller set no header at all.
        let header = encode_region_request_header(None, true).unwrap();

        assert!(is_internal_stage(&extensions_of(&header)));
    }

    #[test]
    fn test_encode_region_request_header_keeps_context_of_internal_request() {
        let header = encode_region_request_header(
            Some(RegionRequestHeader {
                dbname: "greptime".to_string(),
                query_context: Some(api::v1::QueryContext {
                    current_schema: "public".to_string(),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            true,
        )
        .unwrap();

        assert_eq!(header.dbname, "greptime");
        assert_eq!(
            header
                .query_context
                .as_ref()
                .map(|ctx| ctx.current_schema.as_str()),
            Some("public")
        );
        assert!(is_internal_stage(&extensions_of(&header)));
    }

    #[test]
    fn test_encode_region_request_header_does_not_mark_independent_request() {
        // An independent request with no header keeps no header at all.
        assert!(encode_region_request_header(None, false).is_none());

        // A marker that a caller left in the header of an independent request is dropped: the
        // marker is owned by `QueryRequest::internal`.
        let header = encode_region_request_header(
            Some(RegionRequestHeader {
                query_context: Some(api::v1::QueryContext {
                    extensions: HashMap::from([(
                        QUERY_INTERNAL_STAGE_EXTENSION_KEY.to_string(),
                        QUERY_INTERNAL_STAGE_EXTENSION_VALUE.to_string(),
                    )]),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            false,
        )
        .unwrap();

        assert!(!is_internal_stage(&extensions_of(&header)));
    }

    #[test]
    fn test_internal_marker_survives_the_wire_format() {
        let request = api::v1::region::QueryRequest {
            header: encode_region_request_header(
                Some(RegionRequestHeader {
                    query_context: Some(api::v1::QueryContext {
                        current_schema: "public".to_string(),
                        extensions: HashMap::from([(
                            "remote_query_id".to_string(),
                            "1".to_string(),
                        )]),
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
                true,
            ),
            region_id: 1,
            plan: Vec::new(),
        };
        let bytes = request.encode_to_vec();

        // The receiver decodes the ticket and builds the query context of the request from the
        // header of the decoded request.
        let decoded = api::v1::region::QueryRequest::decode(bytes.as_slice()).unwrap();
        let extensions = extensions_of(decoded.header.as_ref().unwrap());

        assert!(is_internal_stage(&extensions));
        assert_eq!(
            extensions.get("remote_query_id").map(String::as_str),
            Some("1")
        );
    }
}
