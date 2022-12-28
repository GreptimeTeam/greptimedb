// Copyright 2022 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

mod stream;

use std::pin::Pin;

use api::v1::object_expr::Expr;
use api::v1::query_request::Query;
use api::v1::ObjectExpr;
use arrow_flight::flight_service_server::FlightService;
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PutResult, SchemaResult, Ticket,
};
use async_trait::async_trait;
use common_query::Output;
use futures::Stream;
use prost::Message;
use session::context::QueryContext;
use snafu::{OptionExt, ResultExt};
use tonic::{Request, Response, Streaming};

use crate::error::{self, Result};
use crate::instance::flight::stream::GetStream;
use crate::instance::Instance;

type TonicResult<T> = std::result::Result<T, tonic::Status>;
type TonicStream<T> = Pin<Box<dyn Stream<Item = TonicResult<T>> + Send + Sync + 'static>>;

#[async_trait]
impl FlightService for Instance {
    type HandshakeStream = TonicStream<HandshakeResponse>;

    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> TonicResult<Response<Self::HandshakeStream>> {
        Err(tonic::Status::unimplemented("Not yet implemented"))
    }

    type ListFlightsStream = TonicStream<FlightInfo>;

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> TonicResult<Response<Self::ListFlightsStream>> {
        Err(tonic::Status::unimplemented("Not yet implemented"))
    }
    async fn get_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> TonicResult<Response<FlightInfo>> {
        Err(tonic::Status::unimplemented("Not yet implemented"))
    }
    async fn get_schema(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> TonicResult<Response<SchemaResult>> {
        Err(tonic::Status::unimplemented("Not yet implemented"))
    }

    type DoGetStream = TonicStream<FlightData>;

    async fn do_get(&self, request: Request<Ticket>) -> TonicResult<Response<Self::DoGetStream>> {
        let ticket = request.into_inner().ticket;
        let expr = ObjectExpr::decode(ticket.as_slice())
            .context(error::InvalidFlightTicketSnafu)?
            .expr
            .context(error::MissingRequiredFieldSnafu { name: "expr" })?;
        match expr {
            Expr::Query(query_request) => {
                let query = query_request
                    .query
                    .context(error::MissingRequiredFieldSnafu { name: "expr" })?;
                let stream = self.handle_query(query).await?;
                Ok(Response::new(Box::pin(stream) as TonicStream<FlightData>))
            }
            // TODO(LFC): Implement Insertion Flight interface.
            Expr::Insert(_) => Err(tonic::Status::unimplemented("Not yet implemented")),
        }
    }

    type DoPutStream = TonicStream<PutResult>;

    async fn do_put(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> TonicResult<Response<Self::DoPutStream>> {
        Err(tonic::Status::unimplemented("Not yet implemented"))
    }

    type DoExchangeStream = TonicStream<FlightData>;

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> TonicResult<Response<Self::DoExchangeStream>> {
        Err(tonic::Status::unimplemented("Not yet implemented"))
    }

    type DoActionStream = TonicStream<arrow_flight::Result>;

    async fn do_action(
        &self,
        _request: Request<Action>,
    ) -> TonicResult<Response<Self::DoActionStream>> {
        Err(tonic::Status::unimplemented("Not yet implemented"))
    }

    type ListActionsStream = TonicStream<ActionType>;

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> TonicResult<Response<Self::ListActionsStream>> {
        Err(tonic::Status::unimplemented("Not yet implemented"))
    }
}

impl Instance {
    async fn handle_query(&self, query: Query) -> Result<GetStream> {
        let output = match query {
            Query::Sql(sql) => {
                let stmt = self
                    .query_engine
                    .sql_to_statement(&sql)
                    .context(error::ExecuteSqlSnafu)?;
                self.execute_stmt(stmt, QueryContext::arc()).await?
            }
            Query::LogicalPlan(plan) => self.execute_logical(plan).await?,
        };

        let recordbatch_stream = match output {
            Output::Stream(stream) => stream,
            Output::RecordBatches(x) => x.as_stream(),
            Output::AffectedRows(_) => {
                unreachable!("SELECT should not have returned affected rows!")
            }
        };
        Ok(GetStream::new(recordbatch_stream))
    }
}
