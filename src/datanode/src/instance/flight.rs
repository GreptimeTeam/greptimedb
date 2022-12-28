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
use api::v1::{FlightAppMeta, ObjectExpr};
use arrow_flight::flight_service_server::FlightService;
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, IpcMessage, PutResult, SchemaResult, Ticket,
};
use async_trait::async_trait;
use common_query::Output;
use datatypes::arrow;
use flatbuffers::FlatBufferBuilder;
use futures::Stream;
use prost::Message;
use session::context::QueryContext;
use snafu::{OptionExt, ResultExt};
use tonic::{Request, Response, Streaming};

use crate::error::{self, Result};
use crate::instance::flight::stream::FlightRecordBatchStream;
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
                Ok(Response::new(stream))
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
    async fn handle_query(&self, query: Query) -> Result<TonicStream<FlightData>> {
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
        Ok(match output {
            Output::Stream(stream) => {
                let stream = FlightRecordBatchStream::new(stream);
                Box::pin(stream) as _
            }
            Output::RecordBatches(x) => {
                let stream = FlightRecordBatchStream::new(x.as_stream());
                Box::pin(stream) as _
            }
            Output::AffectedRows(rows) => {
                let stream = async_stream::stream! {
                    let app_meta = FlightAppMeta {
                        affected_rows: rows as _,
                    }.encode_to_vec();
                    yield Ok(FlightData::new(None, IpcMessage(build_none_flight_msg()), app_meta, vec![]))
                };
                Box::pin(stream) as _
            }
        })
    }
}

fn build_none_flight_msg() -> Vec<u8> {
    let mut builder = FlatBufferBuilder::new();

    let mut message = arrow::ipc::MessageBuilder::new(&mut builder);
    message.add_version(arrow::ipc::MetadataVersion::V5);
    message.add_header_type(arrow::ipc::MessageHeader::NONE);
    message.add_bodyLength(0);

    let data = message.finish();
    builder.finish(data, None);

    builder.finished_data().to_vec()
}

#[cfg(test)]
mod test {
    use api::v1::{object_result, FlightDataRaw, QueryRequest};
    use common_grpc::flight;
    use common_grpc::flight::FlightMessage;
    use datatypes::prelude::*;

    use super::*;
    use crate::tests::test_util::{self, MockInstance};

    #[tokio::test(flavor = "multi_thread")]
    async fn test_handle_query() {
        let instance = MockInstance::new("test_handle_query").await;
        test_util::create_test_table(
            &instance,
            ConcreteDataType::timestamp_millisecond_datatype(),
        )
        .await
        .unwrap();

        let ticket = Request::new(Ticket {
            ticket: ObjectExpr {
                header: None,
                expr: Some(Expr::Query(QueryRequest {
                    query: Some(Query::Sql(
                        "INSERT INTO demo(host, cpu, memory, ts) VALUES \
                            ('host1', 66.6, 1024, 1672201025000),\
                            ('host2', 88.8, 333.3, 1672201026000)"
                            .to_string(),
                    )),
                })),
            }
            .encode_to_vec(),
        });

        let response = instance.inner().do_get(ticket).await.unwrap();
        let result = flight::flight_data_to_object_result(response)
            .await
            .unwrap();
        let result = result.result.unwrap();
        assert!(matches!(result, object_result::Result::FlightData(_)));

        let object_result::Result::FlightData(FlightDataRaw { raw_data }) = result else { unreachable!() };
        let mut messages = flight::raw_flight_data_to_message(raw_data).unwrap();
        assert_eq!(messages.len(), 1);

        let message = messages.remove(0);
        assert!(matches!(message, FlightMessage::AffectedRows(_)));
        let FlightMessage::AffectedRows(affected_rows) = message else { unreachable!() };
        assert_eq!(affected_rows, 2);

        let ticket = Request::new(Ticket {
            ticket: ObjectExpr {
                header: None,
                expr: Some(Expr::Query(QueryRequest {
                    query: Some(Query::Sql(
                        "SELECT ts, host, cpu, memory FROM demo".to_string(),
                    )),
                })),
            }
            .encode_to_vec(),
        });

        let response = instance.inner().do_get(ticket).await.unwrap();
        let result = flight::flight_data_to_object_result(response)
            .await
            .unwrap();
        let result = result.result.unwrap();
        assert!(matches!(result, object_result::Result::FlightData(_)));

        let object_result::Result::FlightData(FlightDataRaw { raw_data }) = result else { unreachable!() };
        let messages = flight::raw_flight_data_to_message(raw_data).unwrap();
        assert_eq!(messages.len(), 2);

        let recordbatch = flight::flight_messages_to_recordbatches(messages).unwrap();
        let expected = "\
+---------------------+-------+------+--------+
| ts                  | host  | cpu  | memory |
+---------------------+-------+------+--------+
| 2022-12-28T04:17:05 | host1 | 66.6 | 1024   |
| 2022-12-28T04:17:06 | host2 | 88.8 | 333.3  |
+---------------------+-------+------+--------+";
        let actual = recordbatch.pretty_print().unwrap();
        assert_eq!(actual, expected);
    }
}
