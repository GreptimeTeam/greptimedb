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

use api::v1::GreptimeRequest;
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
use futures::Stream;
use prost::Message;
use snafu::ResultExt;
use tonic::{Request, Response, Status, Streaming};

use crate::error;
pub use crate::grpc::flight::stream::FlightRecordBatchStream;
use crate::grpc::greptime_handler::{get_request_type, GreptimeRequestHandler};
use crate::grpc::TonicResult;

pub type TonicStream<T> = Pin<Box<dyn Stream<Item = TonicResult<T>> + Send + Sync + 'static>>;

/// A subset of [FlightService]
#[async_trait]
pub trait FlightCraft: Send + Sync + 'static {
    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> TonicResult<Response<TonicStream<FlightData>>>;
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
        _: Request<Streaming<FlightData>>,
    ) -> TonicResult<Response<Self::DoPutStream>> {
        Err(Status::unimplemented("Not yet implemented"))
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
            let stream: Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send + Sync>> =
                to_flight_data_stream(output, TracingContext::from_current_span());
            Ok(Response::new(stream))
        }
        .trace(span)
        .await
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
