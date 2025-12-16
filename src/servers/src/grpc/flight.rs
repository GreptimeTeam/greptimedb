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

mod stream;

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use api::v1::GreptimeRequest;
use arrow_flight::flight_service_server::FlightService;
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
};
use async_trait::async_trait;
use bytes;
use bytes::Bytes;
use common_grpc::flight::do_put::{DoPutMetadata, DoPutResponse};
use common_grpc::flight::{FlightDecoder, FlightEncoder, FlightMessage};
use common_query::{Output, OutputData};
use common_recordbatch::DfRecordBatch;
use common_telemetry::debug;
use common_telemetry::tracing::info_span;
use common_telemetry::tracing_context::{FutureExt, TracingContext};
use datatypes::arrow::datatypes::SchemaRef;
use futures::{Stream, future, ready};
use futures_util::{StreamExt, TryStreamExt};
use prost::Message;
use session::context::{QueryContext, QueryContextRef};
use snafu::{ResultExt, ensure};
use table::table_name::TableName;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};

use crate::error::{InvalidParameterSnafu, Result, ToJsonSnafu};
pub use crate::grpc::flight::stream::FlightRecordBatchStream;
use crate::grpc::greptime_handler::{GreptimeRequestHandler, get_request_type};
use crate::grpc::{FlightCompression, TonicResult, context_auth};
use crate::metrics::{METRIC_GRPC_MEMORY_USAGE_BYTES, METRIC_GRPC_REQUESTS_REJECTED_TOTAL};
use crate::request_limiter::{RequestMemoryGuard, RequestMemoryLimiter};
use crate::{error, hint_headers};

pub type TonicStream<T> = Pin<Box<dyn Stream<Item = TonicResult<T>> + Send + 'static>>;

/// A subset of [FlightService]
#[async_trait]
pub trait FlightCraft: Send + Sync + 'static {
    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> TonicResult<Response<TonicStream<FlightData>>>;

    async fn do_put(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> TonicResult<Response<TonicStream<PutResult>>> {
        let _ = request;
        Err(Status::unimplemented("Not yet implemented"))
    }
}

pub type FlightCraftRef = Arc<dyn FlightCraft>;

pub struct FlightCraftWrapper<T: FlightCraft>(pub T);

impl<T: FlightCraft> From<T> for FlightCraftWrapper<T> {
    fn from(t: T) -> Self {
        Self(t)
    }
}

#[async_trait]
impl FlightCraft for FlightCraftRef {
    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> TonicResult<Response<TonicStream<FlightData>>> {
        (**self).do_get(request).await
    }

    async fn do_put(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> TonicResult<Response<TonicStream<PutResult>>> {
        self.as_ref().do_put(request).await
    }
}

#[async_trait]
impl<T: FlightCraft> FlightService for FlightCraftWrapper<T> {
    type HandshakeStream = TonicStream<HandshakeResponse>;

    async fn handshake(
        &self,
        _: Request<Streaming<HandshakeRequest>>,
    ) -> TonicResult<Response<Self::HandshakeStream>> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    type ListFlightsStream = TonicStream<FlightInfo>;

    async fn list_flights(
        &self,
        _: Request<Criteria>,
    ) -> TonicResult<Response<Self::ListFlightsStream>> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    async fn get_flight_info(
        &self,
        _: Request<FlightDescriptor>,
    ) -> TonicResult<Response<FlightInfo>> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    async fn poll_flight_info(
        &self,
        _: Request<FlightDescriptor>,
    ) -> TonicResult<Response<PollInfo>> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    async fn get_schema(
        &self,
        _: Request<FlightDescriptor>,
    ) -> TonicResult<Response<SchemaResult>> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    type DoGetStream = TonicStream<FlightData>;

    async fn do_get(&self, request: Request<Ticket>) -> TonicResult<Response<Self::DoGetStream>> {
        self.0.do_get(request).await
    }

    type DoPutStream = TonicStream<PutResult>;

    async fn do_put(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> TonicResult<Response<Self::DoPutStream>> {
        self.0.do_put(request).await
    }

    type DoExchangeStream = TonicStream<FlightData>;

    async fn do_exchange(
        &self,
        _: Request<Streaming<FlightData>>,
    ) -> TonicResult<Response<Self::DoExchangeStream>> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    type DoActionStream = TonicStream<arrow_flight::Result>;

    async fn do_action(&self, _: Request<Action>) -> TonicResult<Response<Self::DoActionStream>> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    type ListActionsStream = TonicStream<ActionType>;

    async fn list_actions(
        &self,
        _: Request<Empty>,
    ) -> TonicResult<Response<Self::ListActionsStream>> {
        Err(Status::unimplemented("Not yet implemented"))
    }
}

#[async_trait]
impl FlightCraft for GreptimeRequestHandler {
    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> TonicResult<Response<TonicStream<FlightData>>> {
        let hints = hint_headers::extract_hints(request.metadata());

        let ticket = request.into_inner().ticket;
        let request =
            GreptimeRequest::decode(ticket.as_ref()).context(error::InvalidFlightTicketSnafu)?;

        // The Grpc protocol pass query by Flight. It needs to be wrapped under a span, in order to record stream
        let span = info_span!(
            "GreptimeRequestHandler::do_get",
            protocol = "grpc",
            request_type = get_request_type(&request)
        );
        let flight_compression = self.flight_compression;
        async {
            let output = self.handle_request(request, hints).await?;
            let stream = to_flight_data_stream(
                output,
                TracingContext::from_current_span(),
                flight_compression,
                QueryContext::arc(),
            );
            Ok(Response::new(stream))
        }
        .trace(span)
        .await
    }

    async fn do_put(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> TonicResult<Response<TonicStream<PutResult>>> {
        let (headers, extensions, stream) = request.into_parts();

        let limiter = extensions.get::<RequestMemoryLimiter>().cloned();

        let query_ctx = context_auth::create_query_context_from_grpc_metadata(&headers)?;
        context_auth::check_auth(self.user_provider.clone(), &headers, query_ctx.clone()).await?;

        const MAX_PENDING_RESPONSES: usize = 32;
        let (tx, rx) = mpsc::channel::<TonicResult<DoPutResponse>>(MAX_PENDING_RESPONSES);

        let stream = PutRecordBatchRequestStream::new(
            stream,
            query_ctx.current_catalog().to_string(),
            query_ctx.current_schema(),
            limiter,
        )
        .await?;
        // Ack immediately when stream is created successfully (in Init state)
        let _ = tx.send(Ok(DoPutResponse::new(0, 0, 0.0))).await;
        self.put_record_batches(stream, tx, query_ctx).await;

        let response = ReceiverStream::new(rx)
            .and_then(|response| {
                future::ready({
                    serde_json::to_vec(&response)
                        .context(ToJsonSnafu)
                        .map(|x| PutResult {
                            app_metadata: Bytes::from(x),
                        })
                        .map_err(Into::into)
                })
            })
            .boxed();
        Ok(Response::new(response))
    }
}

pub struct PutRecordBatchRequest {
    pub table_name: TableName,
    pub request_id: i64,
    pub record_batch: DfRecordBatch,
    pub schema_bytes: Bytes,
    pub flight_data: FlightData,
    pub(crate) _guard: Option<RequestMemoryGuard>,
}

impl PutRecordBatchRequest {
    fn try_new(
        table_name: TableName,
        record_batch: DfRecordBatch,
        request_id: i64,
        schema_bytes: Bytes,
        flight_data: FlightData,
        limiter: Option<&RequestMemoryLimiter>,
    ) -> Result<Self> {
        let memory_usage = flight_data.data_body.len()
            + flight_data.app_metadata.len()
            + flight_data.data_header.len();

        let _guard = limiter
            .filter(|limiter| limiter.is_enabled())
            .map(|limiter| {
                limiter
                    .try_acquire(memory_usage)
                    .map(|guard| {
                        guard.inspect(|g| {
                            METRIC_GRPC_MEMORY_USAGE_BYTES.set(g.current_usage() as i64);
                        })
                    })
                    .inspect_err(|_| {
                        METRIC_GRPC_REQUESTS_REJECTED_TOTAL.inc();
                    })
            })
            .transpose()?
            .flatten();

        Ok(Self {
            table_name,
            request_id,
            record_batch,
            schema_bytes,
            flight_data,
            _guard,
        })
    }
}

pub struct PutRecordBatchRequestStream {
    flight_data_stream: Streaming<FlightData>,
    catalog: String,
    schema_name: String,
    limiter: Option<RequestMemoryLimiter>,
    state: StreamState,
}

enum StreamState {
    Init,
    Ready {
        table_name: TableName,
        schema: SchemaRef,
        schema_bytes: Bytes,
        decoder: FlightDecoder,
    },
}

impl PutRecordBatchRequestStream {
    /// Creates a new `PutRecordBatchRequestStream` in Init state.
    /// The stream will transition to Ready state when it receives the schema message.
    pub async fn new(
        flight_data_stream: Streaming<FlightData>,
        catalog: String,
        schema: String,
        limiter: Option<RequestMemoryLimiter>,
    ) -> TonicResult<Self> {
        Ok(Self {
            flight_data_stream,
            catalog,
            schema_name: schema,
            limiter,
            state: StreamState::Init,
        })
    }

    /// Returns the table name extracted from the flight descriptor.
    /// Returns None if the stream is still in Init state.
    pub fn table_name(&self) -> Option<&TableName> {
        match &self.state {
            StreamState::Init => None,
            StreamState::Ready { table_name, .. } => Some(table_name),
        }
    }

    /// Returns the Arrow schema decoded from the first flight message.
    /// Returns None if the stream is still in Init state.
    pub fn schema(&self) -> Option<&SchemaRef> {
        match &self.state {
            StreamState::Init => None,
            StreamState::Ready { schema, .. } => Some(schema),
        }
    }

    /// Returns the raw schema bytes in IPC format.
    /// Returns None if the stream is still in Init state.
    pub fn schema_bytes(&self) -> Option<&Bytes> {
        match &self.state {
            StreamState::Init => None,
            StreamState::Ready { schema_bytes, .. } => Some(schema_bytes),
        }
    }

    fn extract_table_name(mut descriptor: FlightDescriptor) -> Result<String> {
        ensure!(
            descriptor.r#type == arrow_flight::flight_descriptor::DescriptorType::Path as i32,
            InvalidParameterSnafu {
                reason: "expect FlightDescriptor::type == 'Path' only",
            }
        );
        ensure!(
            descriptor.path.len() == 1,
            InvalidParameterSnafu {
                reason: "expect FlightDescriptor::path has only one table name",
            }
        );
        Ok(descriptor.path.remove(0))
    }
}

impl Stream for PutRecordBatchRequestStream {
    type Item = TonicResult<PutRecordBatchRequest>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            let poll = ready!(self.flight_data_stream.poll_next_unpin(cx));

            match poll {
                Some(Ok(flight_data)) => {
                    // Clone limiter once to avoid borrowing issues
                    let limiter = self.limiter.clone();

                    match &mut self.state {
                        StreamState::Init => {
                            // First message - expecting schema
                            let flight_descriptor = match flight_data.flight_descriptor.as_ref() {
                                Some(descriptor) => descriptor.clone(),
                                None => {
                                    return Poll::Ready(Some(Err(Status::failed_precondition(
                                        "table to put is not found in flight descriptor",
                                    ))));
                                }
                            };

                            let table_name_str = match Self::extract_table_name(flight_descriptor) {
                                Ok(name) => name,
                                Err(e) => {
                                    return Poll::Ready(Some(Err(Status::invalid_argument(
                                        e.to_string(),
                                    ))));
                                }
                            };
                            let table_name = TableName::new(
                                self.catalog.clone(),
                                self.schema_name.clone(),
                                table_name_str,
                            );

                            // Decode the schema
                            let mut decoder = FlightDecoder::default();
                            let schema_message = decoder.try_decode(&flight_data).map_err(|e| {
                                Status::invalid_argument(format!("Failed to decode schema: {}", e))
                            })?;

                            match schema_message {
                                Some(FlightMessage::Schema(schema)) => {
                                    let schema_bytes = decoder.schema_bytes().ok_or_else(|| {
                                        Status::internal(
                                            "decoder should have schema bytes after decoding schema",
                                        )
                                    })?;

                                    // Transition to Ready state with all necessary data
                                    self.state = StreamState::Ready {
                                        table_name,
                                        schema,
                                        schema_bytes,
                                        decoder,
                                    };
                                    // Continue to next iteration to process RecordBatch messages
                                    continue;
                                }
                                _ => {
                                    return Poll::Ready(Some(Err(Status::failed_precondition(
                                        "first message must be a Schema message",
                                    ))));
                                }
                            }
                        }
                        StreamState::Ready {
                            table_name,
                            schema: _,
                            schema_bytes,
                            decoder,
                        } => {
                            // Extract request_id and body_size from FlightData before decoding
                            let request_id = if !flight_data.app_metadata.is_empty() {
                                serde_json::from_slice::<DoPutMetadata>(&flight_data.app_metadata)
                                    .map(|meta| meta.request_id())
                                    .unwrap_or_default()
                            } else {
                                0
                            };

                            // Decode FlightData to RecordBatch
                            match decoder.try_decode(&flight_data) {
                                Ok(Some(FlightMessage::RecordBatch(record_batch))) => {
                                    let table_name = table_name.clone();
                                    let schema_bytes = schema_bytes.clone();
                                    return Poll::Ready(Some(
                                        PutRecordBatchRequest::try_new(
                                            table_name,
                                            record_batch,
                                            request_id,
                                            schema_bytes,
                                            flight_data,
                                            limiter.as_ref(),
                                        )
                                        .map_err(|e| Status::invalid_argument(e.to_string())),
                                    ));
                                }
                                Ok(Some(other)) => {
                                    debug!("Unexpected flight message: {:?}", other);
                                    return Poll::Ready(Some(Err(Status::invalid_argument(
                                        "Expected RecordBatch message, got other message type",
                                    ))));
                                }
                                Ok(None) => {
                                    // Dictionary batch - processed internally by decoder, continue polling
                                    continue;
                                }
                                Err(e) => {
                                    return Poll::Ready(Some(Err(Status::invalid_argument(
                                        format!("Failed to decode RecordBatch: {}", e),
                                    ))));
                                }
                            }
                        }
                    }
                }
                Some(Err(e)) => {
                    return Poll::Ready(Some(Err(e)));
                }
                None => {
                    return Poll::Ready(None);
                }
            }
        }
    }
}

fn to_flight_data_stream(
    output: Output,
    tracing_context: TracingContext,
    flight_compression: FlightCompression,
    query_ctx: QueryContextRef,
) -> TonicStream<FlightData> {
    match output.data {
        OutputData::Stream(stream) => {
            let stream = FlightRecordBatchStream::new(
                stream,
                tracing_context,
                flight_compression,
                query_ctx,
            );
            Box::pin(stream) as _
        }
        OutputData::RecordBatches(x) => {
            let stream = FlightRecordBatchStream::new(
                x.as_stream(),
                tracing_context,
                flight_compression,
                query_ctx,
            );
            Box::pin(stream) as _
        }
        OutputData::AffectedRows(rows) => {
            let stream = tokio_stream::iter(
                FlightEncoder::default()
                    .encode(FlightMessage::AffectedRows(rows))
                    .into_iter()
                    .map(Ok),
            );
            Box::pin(stream) as _
        }
    }
}
