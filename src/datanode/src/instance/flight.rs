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

use api::v1::ddl_request::Expr as DdlExpr;
use api::v1::object_expr::Request as GrpcRequest;
use api::v1::query_request::Query;
use api::v1::{DdlRequest, InsertRequest, ObjectExpr};
use arrow_flight::flight_service_server::FlightService;
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PutResult, SchemaResult, Ticket,
};
use async_trait::async_trait;
use common_catalog::consts::DEFAULT_CATALOG_NAME;
use common_grpc::flight::{FlightEncoder, FlightMessage};
use common_query::Output;
use futures::Stream;
use prost::Message;
use session::context::QueryContext;
use snafu::{OptionExt, ResultExt};
use tonic::{Request, Response, Streaming};

use crate::error::{
    CatalogSnafu, ExecuteSqlSnafu, InsertDataSnafu, InsertSnafu, InvalidFlightTicketSnafu,
    MissingRequiredFieldSnafu, Result, TableNotFoundSnafu,
};
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
        let request = ObjectExpr::decode(ticket.as_slice())
            .context(InvalidFlightTicketSnafu)?
            .request
            .context(MissingRequiredFieldSnafu { name: "request" })?;
        let output = match request {
            GrpcRequest::Insert(request) => self.handle_insert(request).await?,
            GrpcRequest::Query(query_request) => {
                let query = query_request
                    .query
                    .context(MissingRequiredFieldSnafu { name: "query" })?;
                self.handle_query(query).await?
            }
            GrpcRequest::Ddl(request) => self.handle_ddl(request).await?,
        };
        let stream = to_flight_data_stream(output);
        Ok(Response::new(stream))
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
    async fn handle_query(&self, query: Query) -> Result<Output> {
        Ok(match query {
            Query::Sql(sql) => {
                let stmt = self
                    .query_engine
                    .sql_to_statement(&sql)
                    .context(ExecuteSqlSnafu)?;
                self.execute_stmt(stmt, QueryContext::arc()).await?
            }
            Query::LogicalPlan(plan) => self.execute_logical(plan).await?,
        })
    }

    pub async fn handle_insert(&self, request: InsertRequest) -> Result<Output> {
        let table_name = &request.table_name.clone();
        // TODO(LFC): InsertRequest should carry catalog name, too.
        let table = self
            .catalog_manager
            .table(DEFAULT_CATALOG_NAME, &request.schema_name, table_name)
            .context(CatalogSnafu)?
            .context(TableNotFoundSnafu { table_name })?;

        let request = common_grpc_expr::insert::to_table_insert_request(request, table.schema())
            .context(InsertDataSnafu)?;

        let affected_rows = table
            .insert(request)
            .await
            .context(InsertSnafu { table_name })?;
        Ok(Output::AffectedRows(affected_rows))
    }

    async fn handle_ddl(&self, request: DdlRequest) -> Result<Output> {
        let expr = request
            .expr
            .context(MissingRequiredFieldSnafu { name: "expr" })?;
        match expr {
            DdlExpr::CreateTable(expr) => self.handle_create(expr).await,
            DdlExpr::Alter(expr) => self.handle_alter(expr).await,
            DdlExpr::CreateDatabase(expr) => self.handle_create_database(expr).await,
            DdlExpr::DropTable(expr) => self.handle_drop_table(expr).await,
        }
    }
}

fn to_flight_data_stream(output: Output) -> TonicStream<FlightData> {
    match output {
        Output::Stream(stream) => {
            let stream = FlightRecordBatchStream::new(stream);
            Box::pin(stream) as _
        }
        Output::RecordBatches(x) => {
            let stream = FlightRecordBatchStream::new(x.as_stream());
            Box::pin(stream) as _
        }
        Output::AffectedRows(rows) => {
            let stream = tokio_stream::once(Ok(
                FlightEncoder::default().encode(FlightMessage::AffectedRows(rows))
            ));
            Box::pin(stream) as _
        }
    }
}

#[cfg(test)]
mod test {
    use api::v1::column::{SemanticType, Values};
    use api::v1::{
        alter_expr, AddColumn, AddColumns, AlterExpr, Column, ColumnDataType, ColumnDef,
        CreateDatabaseExpr, CreateTableExpr, QueryRequest,
    };
    use client::RpcOutput;
    use common_grpc::flight;
    use common_recordbatch::RecordBatches;
    use datatypes::prelude::*;

    use super::*;
    use crate::tests::test_util::{self, MockInstance};

    async fn boarding(instance: &MockInstance, ticket: Request<Ticket>) -> RpcOutput {
        let response = instance.inner().do_get(ticket).await.unwrap();
        let result = flight::flight_data_to_object_result(response)
            .await
            .unwrap();
        result.try_into().unwrap()
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_handle_ddl() {
        let instance = MockInstance::new("test_handle_ddl").await;

        let ticket = Request::new(Ticket {
            ticket: ObjectExpr {
                request: Some(GrpcRequest::Ddl(DdlRequest {
                    expr: Some(DdlExpr::CreateDatabase(CreateDatabaseExpr {
                        database_name: "my_database".to_string(),
                    })),
                })),
            }
            .encode_to_vec(),
        });

        let output = boarding(&instance, ticket).await;
        assert!(matches!(output, RpcOutput::AffectedRows(1)));

        let ticket = Request::new(Ticket {
            ticket: ObjectExpr {
                request: Some(GrpcRequest::Ddl(DdlRequest {
                    expr: Some(DdlExpr::CreateTable(CreateTableExpr {
                        catalog_name: "greptime".to_string(),
                        schema_name: "my_database".to_string(),
                        table_name: "my_table".to_string(),
                        desc: "blabla".to_string(),
                        column_defs: vec![
                            ColumnDef {
                                name: "a".to_string(),
                                datatype: ColumnDataType::String as i32,
                                is_nullable: true,
                                default_constraint: vec![],
                            },
                            ColumnDef {
                                name: "ts".to_string(),
                                datatype: ColumnDataType::TimestampMillisecond as i32,
                                is_nullable: false,
                                default_constraint: vec![],
                            },
                        ],
                        time_index: "ts".to_string(),
                        ..Default::default()
                    })),
                })),
            }
            .encode_to_vec(),
        });

        let output = boarding(&instance, ticket).await;
        assert!(matches!(output, RpcOutput::AffectedRows(1)));

        let ticket = Request::new(Ticket {
            ticket: ObjectExpr {
                request: Some(GrpcRequest::Ddl(DdlRequest {
                    expr: Some(DdlExpr::Alter(AlterExpr {
                        catalog_name: "greptime".to_string(),
                        schema_name: "my_database".to_string(),
                        table_name: "my_table".to_string(),
                        kind: Some(alter_expr::Kind::AddColumns(AddColumns {
                            add_columns: vec![AddColumn {
                                column_def: Some(ColumnDef {
                                    name: "b".to_string(),
                                    datatype: ColumnDataType::Int32 as i32,
                                    is_nullable: true,
                                    default_constraint: vec![],
                                }),
                                is_key: true,
                            }],
                        })),
                    })),
                })),
            }
            .encode_to_vec(),
        });

        let output = boarding(&instance, ticket).await;
        assert!(matches!(output, RpcOutput::AffectedRows(0)));

        let output = instance
            .inner()
            .execute_sql(
                "INSERT INTO my_database.my_table (a, b, ts) VALUES ('s', 1, 1672384140000)",
                QueryContext::arc(),
            )
            .await
            .unwrap();
        assert!(matches!(output, Output::AffectedRows(1)));

        let output = instance
            .inner()
            .execute_sql(
                "SELECT ts, a, b FROM my_database.my_table",
                QueryContext::arc(),
            )
            .await
            .unwrap();
        let Output::Stream(stream) = output else { unreachable!() };
        let recordbatches = RecordBatches::try_collect(stream).await.unwrap();
        let expected = "\
+---------------------+---+---+
| ts                  | a | b |
+---------------------+---+---+
| 2022-12-30T07:09:00 | s | 1 |
+---------------------+---+---+";
        assert_eq!(recordbatches.pretty_print().unwrap(), expected);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_handle_insert() {
        let instance = MockInstance::new("test_handle_insert").await;
        test_util::create_test_table(
            &instance,
            ConcreteDataType::timestamp_millisecond_datatype(),
        )
        .await
        .unwrap();

        let insert = InsertRequest {
            schema_name: "public".to_string(),
            table_name: "demo".to_string(),
            columns: vec![
                Column {
                    column_name: "host".to_string(),
                    values: Some(Values {
                        string_values: vec![
                            "host1".to_string(),
                            "host2".to_string(),
                            "host3".to_string(),
                        ],
                        ..Default::default()
                    }),
                    semantic_type: SemanticType::Tag as i32,
                    datatype: ColumnDataType::String as i32,
                    ..Default::default()
                },
                Column {
                    column_name: "cpu".to_string(),
                    values: Some(Values {
                        f64_values: vec![1.0, 3.0],
                        ..Default::default()
                    }),
                    null_mask: vec![2],
                    semantic_type: SemanticType::Field as i32,
                    datatype: ColumnDataType::Float64 as i32,
                },
                Column {
                    column_name: "ts".to_string(),
                    values: Some(Values {
                        ts_millisecond_values: vec![1672384140000, 1672384141000, 1672384142000],
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

        let output = boarding(&instance, ticket).await;
        assert!(matches!(output, RpcOutput::AffectedRows(3)));

        let output = instance
            .inner()
            .execute_sql("SELECT ts, host, cpu FROM demo", QueryContext::arc())
            .await
            .unwrap();
        let Output::Stream(stream) = output else { unreachable!() };
        let recordbatches = RecordBatches::try_collect(stream).await.unwrap();
        let expected = "\
+---------------------+-------+-----+
| ts                  | host  | cpu |
+---------------------+-------+-----+
| 2022-12-30T07:09:00 | host1 | 1   |
| 2022-12-30T07:09:01 | host2 |     |
| 2022-12-30T07:09:02 | host3 | 3   |
+---------------------+-------+-----+";
        assert_eq!(recordbatches.pretty_print().unwrap(), expected);
    }

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
                request: Some(GrpcRequest::Query(QueryRequest {
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

        let output = boarding(&instance, ticket).await;
        assert!(matches!(output, RpcOutput::AffectedRows(2)));

        let ticket = Request::new(Ticket {
            ticket: ObjectExpr {
                request: Some(GrpcRequest::Query(QueryRequest {
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
        let raw_data = result.flight_data;
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
