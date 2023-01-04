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

use api::v1::object_expr::Request as GrpcRequest;
use api::v1::query_request::Query;
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
use session::context::QueryContext;
use snafu::{ensure, OptionExt, ResultExt};
use tonic::{Request, Response, Status, Streaming};

use crate::error::{IncompleteGrpcResultSnafu, InvalidFlightTicketSnafu, InvalidSqlSnafu};
use crate::instance::{parse_stmt, Instance};

type TonicResult<T> = Result<T, Status>;
type TonicStream<T> = Pin<Box<dyn Stream<Item = TonicResult<T>> + Send + Sync + 'static>>;

#[async_trait]
impl FlightService for Instance {
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
            GrpcRequest::Insert(request) => self.handle_insert(request).await?,
            GrpcRequest::Query(query_request) => {
                let query = query_request.query.context(IncompleteGrpcResultSnafu {
                    err_msg: "Missing 'query' in ObjectExpr::Request",
                })?;
                match query {
                    Query::Sql(sql) => {
                        let mut stmt = parse_stmt(&sql)?;
                        ensure!(
                            stmt.len() == 1,
                            InvalidSqlSnafu {
                                err_msg: "expect only one statement in SQL query string through GRPC interface"
                            }
                        );
                        let stmt = stmt.remove(0);

                        self.query_statement(stmt, QueryContext::arc()).await?
                    }
                    Query::LogicalPlan(_) => {
                        return Err(Status::unimplemented("Not yet implemented"))
                    }
                }
            }
            GrpcRequest::Ddl(_request) => {
                // TODO(LFC): Implement it.
                unimplemented!()
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

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use api::v1::column::{SemanticType, Values};
    use api::v1::{Column, ColumnDataType, InsertRequest, QueryRequest};
    use client::RpcOutput;
    use common_grpc::flight;

    use super::*;
    use crate::tests;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_distributed_insert_and_query() {
        common_telemetry::init_default_ut_logging();

        let instance =
            tests::create_distributed_instance("test_distributed_insert_and_query").await;

        test_insert_and_query(&instance.frontend).await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_standalone_insert_and_query() {
        common_telemetry::init_default_ut_logging();

        let (instance, _) =
            tests::create_standalone_instance("test_standalone_insert_and_query").await;

        test_insert_and_query(&instance).await
    }

    async fn test_insert_and_query(instance: &Arc<Instance>) {
        let ticket = Request::new(Ticket {
            ticket: ObjectExpr {
                request: Some(GrpcRequest::Query(QueryRequest {
                    query: Some(Query::Sql(
                        "CREATE TABLE my_table (a INT, ts TIMESTAMP, TIME INDEX (ts))".to_string(),
                    )),
                })),
            }
            .encode_to_vec(),
        });
        let output = boarding(instance, ticket).await;
        assert!(matches!(output, RpcOutput::AffectedRows(0)));

        let insert = InsertRequest {
            schema_name: "public".to_string(),
            table_name: "my_table".to_string(),
            columns: vec![
                Column {
                    column_name: "a".to_string(),
                    values: Some(Values {
                        i32_values: vec![1, 3],
                        ..Default::default()
                    }),
                    null_mask: vec![2],
                    semantic_type: SemanticType::Field as i32,
                    datatype: ColumnDataType::Int32 as i32,
                },
                Column {
                    column_name: "ts".to_string(),
                    values: Some(Values {
                        ts_millisecond_values: vec![1672557972000, 1672557973000, 1672557974000],
                        ..Default::default()
                    }),
                    semantic_type: SemanticType::Timestamp as i32,
                    datatype: ColumnDataType::TimestampMillisecond as i32,
                    ..Default::default()
                },
            ],
            row_count: 3,
            ..Default::default()
        };

        let ticket = Request::new(Ticket {
            ticket: ObjectExpr {
                request: Some(GrpcRequest::Insert(insert)),
            }
            .encode_to_vec(),
        });

        // Test inserting to exist table.
        let output = boarding(instance, ticket).await;
        assert!(matches!(output, RpcOutput::AffectedRows(3)));

        let ticket = Request::new(Ticket {
            ticket: ObjectExpr {
                request: Some(GrpcRequest::Query(QueryRequest {
                    query: Some(Query::Sql("SELECT ts, a FROM my_table".to_string())),
                })),
            }
            .encode_to_vec(),
        });

        let output = boarding(instance, ticket).await;
        let RpcOutput::RecordBatches(recordbatches) = output else { unreachable!() };
        let expected = "\
+---------------------+---+
| ts                  | a |
+---------------------+---+
| 2023-01-01T07:26:12 | 1 |
| 2023-01-01T07:26:13 |   |
| 2023-01-01T07:26:14 | 3 |
+---------------------+---+";
        assert_eq!(recordbatches.pretty_print().unwrap(), expected);

        let insert = InsertRequest {
            schema_name: "public".to_string(),
            table_name: "auto_created_table".to_string(),
            columns: vec![
                Column {
                    column_name: "a".to_string(),
                    values: Some(Values {
                        i32_values: vec![4, 6],
                        ..Default::default()
                    }),
                    null_mask: vec![2],
                    semantic_type: SemanticType::Field as i32,
                    datatype: ColumnDataType::Int32 as i32,
                },
                Column {
                    column_name: "ts".to_string(),
                    values: Some(Values {
                        ts_millisecond_values: vec![1672557975000, 1672557976000, 1672557977000],
                        ..Default::default()
                    }),
                    semantic_type: SemanticType::Timestamp as i32,
                    datatype: ColumnDataType::TimestampMillisecond as i32,
                    ..Default::default()
                },
            ],
            row_count: 3,
            ..Default::default()
        };

        let ticket = Request::new(Ticket {
            ticket: ObjectExpr {
                request: Some(GrpcRequest::Insert(insert)),
            }
            .encode_to_vec(),
        });

        // Test auto create not existed table upon insertion.
        let output = boarding(instance, ticket).await;
        assert!(matches!(output, RpcOutput::AffectedRows(3)));

        let insert = InsertRequest {
            schema_name: "public".to_string(),
            table_name: "auto_created_table".to_string(),
            columns: vec![
                Column {
                    column_name: "b".to_string(),
                    values: Some(Values {
                        string_values: vec!["x".to_string(), "z".to_string()],
                        ..Default::default()
                    }),
                    null_mask: vec![2],
                    semantic_type: SemanticType::Field as i32,
                    datatype: ColumnDataType::String as i32,
                },
                Column {
                    column_name: "ts".to_string(),
                    values: Some(Values {
                        ts_millisecond_values: vec![1672557978000, 1672557979000, 1672557980000],
                        ..Default::default()
                    }),
                    semantic_type: SemanticType::Timestamp as i32,
                    datatype: ColumnDataType::TimestampMillisecond as i32,
                    ..Default::default()
                },
            ],
            row_count: 3,
            ..Default::default()
        };

        let ticket = Request::new(Ticket {
            ticket: ObjectExpr {
                request: Some(GrpcRequest::Insert(insert)),
            }
            .encode_to_vec(),
        });

        // Test auto add not existed column upon insertion.
        let output = boarding(instance, ticket).await;
        assert!(matches!(output, RpcOutput::AffectedRows(3)));

        let ticket = Request::new(Ticket {
            ticket: ObjectExpr {
                request: Some(GrpcRequest::Query(QueryRequest {
                    query: Some(Query::Sql(
                        "SELECT ts, a, b FROM auto_created_table".to_string(),
                    )),
                })),
            }
            .encode_to_vec(),
        });

        let output = boarding(instance, ticket).await;
        let RpcOutput::RecordBatches(recordbatches) = output else { unreachable!() };
        let expected = "\
+---------------------+---+---+
| ts                  | a | b |
+---------------------+---+---+
| 2023-01-01T07:26:15 | 4 |   |
| 2023-01-01T07:26:16 |   |   |
| 2023-01-01T07:26:17 | 6 |   |
| 2023-01-01T07:26:18 |   | x |
| 2023-01-01T07:26:19 |   |   |
| 2023-01-01T07:26:20 |   | z |
+---------------------+---+---+";
        assert_eq!(recordbatches.pretty_print().unwrap(), expected);
    }

    async fn boarding(instance: &Arc<Instance>, ticket: Request<Ticket>) -> RpcOutput {
        let response = instance.do_get(ticket).await.unwrap();
        let result = flight::flight_data_to_object_result(response)
            .await
            .unwrap();
        result.try_into().unwrap()
    }
}
