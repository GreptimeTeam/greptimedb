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

use api::v1::{GreptimeRequest, GreptimeResponse};
use arrow_flight::flight_service_server::FlightService;
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
};
use async_trait::async_trait;
use common_grpc::flight::{FlightEncoder, FlightMessage};
use common_query::{Output, OutputData};
use common_telemetry::tracing::info_span;
use common_telemetry::tracing_context::{FutureExt, TracingContext};
use futures::{future, Stream};
use futures_util::{StreamExt, TryStreamExt};
use prost::Message;
use snafu::{ensure, OptionExt, ResultExt};
use table::table_name::TableName;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};

use crate::error;
use crate::error::{
    InvalidAuthHeaderInvalidUtf8ValueSnafu, InvalidParameterSnafu, NotFoundAuthHeaderSnafu,
};
pub use crate::grpc::flight::stream::FlightRecordBatchStream;
use crate::grpc::greptime_handler::{get_request_type, GreptimeRequestHandler};
use crate::grpc::TonicResult;
use crate::http::header::constants::GREPTIME_DB_HEADER_NAME;
use crate::http::AUTHORIZATION_HEADER;
use crate::query_handler::grpc::RawRecordBatch;

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
            let output = self.handle_request(request, Default::default()).await?;
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

        let header = |key: &str| -> TonicResult<String> {
            let v = headers.get(key).context(NotFoundAuthHeaderSnafu)?;
            Ok(String::from_utf8(v.as_bytes().to_vec())
                .context(InvalidAuthHeaderInvalidUtf8ValueSnafu)?)
        };

        let username_and_password = header(AUTHORIZATION_HEADER)?;
        let schema = header(GREPTIME_DB_HEADER_NAME)?;
        if !self.validate_auth(username_and_password, schema).await? {
            return Err(Status::unauthenticated("auth failed"));
        }

        const MAX_PENDING_RESPONSES: usize = 32;
        let (tx, rx) = mpsc::channel::<TonicResult<GreptimeResponse>>(MAX_PENDING_RESPONSES);

        let stream: PutRecordBatchRequestStream = stream
            .and_then(|x| future::ready(PutRecordBatchRequest::try_from(x).map_err(Into::into)))
            .boxed();
        self.put_record_batches(stream, tx).await;

        let response = ReceiverStream::new(rx)
            .map_ok(|x| PutResult {
                app_metadata: bytes::Bytes::from(x.encode_to_vec()),
            })
            .boxed();
        Ok(Response::new(response))
    }
}

pub(crate) type PutRecordBatchRequestStream =
    Pin<Box<dyn Stream<Item = TonicResult<PutRecordBatchRequest>> + Send>>;

pub(crate) enum PutRecordBatchRequest {
    First((TableName, RawRecordBatch)),
    Rest(RawRecordBatch),
}

impl TryFrom<FlightData> for PutRecordBatchRequest {
    type Error = error::Error;

    fn try_from(value: FlightData) -> Result<Self, Self::Error> {
        let FlightData {
            flight_descriptor,
            data_body,
            ..
        } = value;
        if let Some(descriptor) = flight_descriptor {
            ensure!(
                descriptor.r#type == arrow_flight::flight_descriptor::DescriptorType::Path as i32,
                InvalidParameterSnafu {
                    reason: "expect FlightDescriptor::type == 'Path' only",
                }
            );
            let table_name: TableName = descriptor.path.try_into().map_err(|e| {
                InvalidParameterSnafu {
                    reason: format!("{e}"),
                }
                .build()
            })?;
            Ok(Self::First((table_name, data_body)))
        } else {
            Ok(Self::Rest(data_body))
        }
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
