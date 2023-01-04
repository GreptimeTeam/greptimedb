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

use std::pin::Pin;

use api::v1::ddl_request::Expr as DdlExpr;
use api::v1::object_expr::Request as GrpcRequest;
use api::v1::ObjectExpr;
use arrow_flight::flight_service_server::FlightService;
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PutResult, SchemaResult, Ticket,
};
use async_trait::async_trait;
use datanode::instance::flight::to_flight_data_stream;
use futures::Stream;
use prost::Message;
use snafu::{OptionExt, ResultExt};
use tonic::{Request, Response, Status, Streaming};

use crate::error::{IncompleteGrpcResultSnafu, InvalidFlightTicketSnafu};
use crate::instance::distributed::DistInstance;

type TonicResult<T> = Result<T, Status>;
type TonicStream<T> = Pin<Box<dyn Stream<Item = TonicResult<T>> + Send + Sync + 'static>>;

#[async_trait]
impl FlightService for DistInstance {
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

    async fn get_schema(
        &self,
        _: Request<FlightDescriptor>,
    ) -> TonicResult<Response<SchemaResult>> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    type DoGetStream = TonicStream<FlightData>;

    async fn do_get(&self, request: Request<Ticket>) -> TonicResult<Response<Self::DoGetStream>> {
        let ticket = request.into_inner().ticket;
        let request = ObjectExpr::decode(ticket.as_slice())
            .context(InvalidFlightTicketSnafu)?
            .request
            .context(IncompleteGrpcResultSnafu {
                err_msg: "Missing 'request' in ObjectExpr",
            })?;
        let output = match request {
            GrpcRequest::Insert(request) => self.handle_dist_insert(request).await?,
            GrpcRequest::Query(_) => {
                unreachable!("Query should have been handled directly in Frontend Instance!")
            }
            GrpcRequest::Ddl(request) => {
                let expr = request.expr.context(IncompleteGrpcResultSnafu {
                    err_msg: "Missing 'expr' in DDL request",
                })?;
                match expr {
                    DdlExpr::CreateDatabase(expr) => self.handle_create_database(expr).await?,
                    DdlExpr::CreateTable(mut expr) => {
                        // TODO(LFC): Support creating distributed table through GRPC interface.
                        // Currently only SQL supports it; how to design the fields in CreateTableExpr?
                        self.create_table(&mut expr, None).await?
                    }
                    DdlExpr::Alter(expr) => self.handle_alter_table(expr).await?,
                    DdlExpr::DropTable(_) => {
                        // TODO(LFC): Implement distributed drop table.
                        // Seems the whole "drop table through GRPC interface" feature is not implemented?
                        return Err(Status::unimplemented("Not yet implemented"));
                    }
                }
            }
        };
        let stream = to_flight_data_stream(output);
        Ok(Response::new(stream))
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
