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

use api::v1::object_expr::Request as GrpcRequest;
use api::v1::{ObjectExpr, ObjectResult};
use arrow_flight::flight_service_server::FlightService;
use arrow_flight::Ticket;
use async_trait::async_trait;
use common_error::prelude::BoxedError;
use common_grpc::flight;
use prost::Message;
use servers::error as server_error;
use servers::query_handler::GrpcQueryHandler;
use snafu::{OptionExt, ResultExt};
use tonic::Request;

use crate::error::{FlightGetSnafu, InvalidFlightDataSnafu, Result};
use crate::instance::Instance;

impl Instance {
    async fn boarding(&self, ticket: Request<Ticket>) -> Result<ObjectResult> {
        let response = self.do_get(ticket).await.context(FlightGetSnafu)?;
        flight::flight_data_to_object_result(response)
            .await
            .context(InvalidFlightDataSnafu)
    }
}

#[async_trait]
impl GrpcQueryHandler for Instance {
    async fn do_query(&self, query: ObjectExpr) -> server_error::Result<ObjectResult> {
        let request = query
            .clone()
            .request
            .context(server_error::InvalidQuerySnafu {
                reason: "empty expr",
            })?;
        match request {
            // TODO(LFC): Unify to "boarding" when do_get supports DDL requests.
            GrpcRequest::Ddl(_) => {
                GrpcQueryHandler::do_query(&*self.grpc_query_handler, query).await
            }
            _ => {
                let ticket = Request::new(Ticket {
                    ticket: query.encode_to_vec(),
                });
                // TODO(LFC): Temporarily use old GRPC interface here, will get rid of them near the end of Arrow Flight adoption.
                self.boarding(ticket)
                    .await
                    .map_err(BoxedError::new)
                    .with_context(|_| servers::error::ExecuteQuerySnafu {
                        query: format!("{query:?}"),
                    })
            }
        }
    }
}
