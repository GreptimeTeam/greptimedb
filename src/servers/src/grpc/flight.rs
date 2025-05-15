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
use bytes::Bytes;
use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
use common_catalog::parse_catalog_and_schema_from_db_string;
use common_grpc::flight::do_put::{DoPutMetadata, DoPutResponse};
use common_grpc::flight::{FlightEncoder, FlightMessage};
use common_query::{Output, OutputData};
use common_telemetry::tracing::info_span;
use common_telemetry::tracing_context::{FutureExt, TracingContext};
use futures::{future, ready, Stream};
use futures_util::{StreamExt, TryStreamExt};
use prost::Message;
use snafu::{ensure, ResultExt};
use table::table_name::TableName;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};

use crate::error::{InvalidParameterSnafu, ParseJsonSnafu, Result, ToJsonSnafu};
pub use crate::grpc::flight::stream::FlightRecordBatchStream;
use crate::grpc::greptime_handler::{get_request_type, GreptimeRequestHandler};
use crate::grpc::TonicResult;
use crate::http::header::constants::GREPTIME_DB_HEADER_NAME;
use crate::http::AUTHORIZATION_HEADER;
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
        async {
            let output = self.handle_request(request, hints).await?;
            let stream = to_flight_data_stream(output, TracingContext::from_current_span());
            Ok(Response::new(stream))
        }
        .trace(span)
        .await
    }

    async fn do_put(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> TonicResult<Response<TonicStream<PutResult>>> {
        let (headers, _, stream) = request.into_parts();

        let header = |key: &str| -> TonicResult<Option<&str>> {
            let Some(v) = headers.get(key) else {
                return Ok(None);
            };
            let Ok(v) = std::str::from_utf8(v.as_bytes()) else {
                return Err(InvalidParameterSnafu {
                    reason: "expect valid UTF-8 value",
                }
                .build()
                .into());
            };
            Ok(Some(v))
        };

        let username_and_password = header(AUTHORIZATION_HEADER)?;
        let db = header(GREPTIME_DB_HEADER_NAME)?;
        if !self.validate_auth(username_and_password, db).await? {
            return Err(Status::unauthenticated("auth failed"));
        }

        const MAX_PENDING_RESPONSES: usize = 32;
        let (tx, rx) = mpsc::channel::<TonicResult<DoPutResponse>>(MAX_PENDING_RESPONSES);

        let stream = PutRecordBatchRequestStream {
            flight_data_stream: stream,
            state: PutRecordBatchRequestStreamState::Init(db.map(ToString::to_string)),
        };
        self.put_record_batches(stream, tx).await;

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

pub(crate) struct PutRecordBatchRequest {
    pub(crate) table_name: TableName,
    pub(crate) request_id: i64,
    pub(crate) data: FlightData,
}

impl PutRecordBatchRequest {
    fn try_new(table_name: TableName, flight_data: FlightData) -> Result<Self> {
        let request_id = if !flight_data.app_metadata.is_empty() {
            let metadata: DoPutMetadata =
                serde_json::from_slice(&flight_data.app_metadata).context(ParseJsonSnafu)?;
            metadata.request_id()
        } else {
            0
        };
        Ok(Self {
            table_name,
            request_id,
            data: flight_data,
        })
    }
}

pub(crate) struct PutRecordBatchRequestStream {
    flight_data_stream: Streaming<FlightData>,
    state: PutRecordBatchRequestStreamState,
}

enum PutRecordBatchRequestStreamState {
    Init(Option<String>),
    Started(TableName),
}

impl Stream for PutRecordBatchRequestStream {
    type Item = TonicResult<PutRecordBatchRequest>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
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

        let poll = ready!(self.flight_data_stream.poll_next_unpin(cx));

        let result = match &mut self.state {
            PutRecordBatchRequestStreamState::Init(db) => match poll {
                Some(Ok(mut flight_data)) => {
                    let flight_descriptor = flight_data.flight_descriptor.take();
                    let result = if let Some(descriptor) = flight_descriptor {
                        let table_name = extract_table_name(descriptor).map(|x| {
                            let (catalog, schema) = if let Some(db) = db {
                                parse_catalog_and_schema_from_db_string(db)
                            } else {
                                (
                                    DEFAULT_CATALOG_NAME.to_string(),
                                    DEFAULT_SCHEMA_NAME.to_string(),
                                )
                            };
                            TableName::new(catalog, schema, x)
                        });
                        let table_name = match table_name {
                            Ok(table_name) => table_name,
                            Err(e) => return Poll::Ready(Some(Err(e.into()))),
                        };

                        let request =
                            PutRecordBatchRequest::try_new(table_name.clone(), flight_data);
                        let request = match request {
                            Ok(request) => request,
                            Err(e) => return Poll::Ready(Some(Err(e.into()))),
                        };

                        self.state = PutRecordBatchRequestStreamState::Started(table_name);

                        Ok(request)
                    } else {
                        Err(Status::failed_precondition(
                            "table to put is not found in flight descriptor",
                        ))
                    };
                    Some(result)
                }
                Some(Err(e)) => Some(Err(e)),
                None => None,
            },
            PutRecordBatchRequestStreamState::Started(table_name) => poll.map(|x| {
                x.and_then(|flight_data| {
                    PutRecordBatchRequest::try_new(table_name.clone(), flight_data)
                        .map_err(Into::into)
                })
            }),
        };
        Poll::Ready(result)
    }
}

fn to_flight_data_stream(
    output: Output,
    tracing_context: TracingContext,
) -> TonicStream<FlightData> {
    match output.data {
        OutputData::Stream(stream) => {
            let stream = FlightRecordBatchStream::new(stream, tracing_context);
            Box::pin(stream) as _
        }
        OutputData::RecordBatches(x) => {
            let stream = FlightRecordBatchStream::new(x.as_stream(), tracing_context);
            Box::pin(stream) as _
        }
        OutputData::AffectedRows(rows) => {
            let stream = tokio_stream::once(Ok(
                FlightEncoder::default().encode(FlightMessage::AffectedRows(rows))
            ));
            Box::pin(stream) as _
        }
    }
}
