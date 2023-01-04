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
use client::RpcOutput;
use datanode::instance::flight::to_flight_data_stream;
use futures::Stream;
use prost::Message;
use servers::query_handler::GrpcQueryHandler;
use session::context::QueryContext;
use snafu::{ensure, OptionExt, ResultExt};
use tonic::{Request, Response, Status, Streaming};

use crate::error::{
    IncompleteGrpcResultSnafu, InvalidFlightTicketSnafu, InvalidObjectResultSnafu, InvalidSqlSnafu,
    InvokeGrpcServerSnafu,
};
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
            GrpcRequest::Ddl(request) => {
                let query = ObjectExpr {
                    request: Some(GrpcRequest::Ddl(request)),
                };
                let result = GrpcQueryHandler::do_query(&*self.grpc_query_handler, query)
                    .await
                    .context(InvokeGrpcServerSnafu)?;
                let result: RpcOutput = result.try_into().context(InvalidObjectResultSnafu)?;
                result.into()
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
    use std::collections::HashMap;
    use std::sync::Arc;

    use api::v1::column::{SemanticType, Values};
    use api::v1::ddl_request::Expr as DdlExpr;
    use api::v1::{
        alter_expr, AddColumn, AddColumns, AlterExpr, Column, ColumnDataType, ColumnDef,
        CreateDatabaseExpr, CreateTableExpr, DdlRequest, InsertRequest, QueryRequest,
    };
    use catalog::helper::{TableGlobalKey, TableGlobalValue};
    use client::RpcOutput;
    use common_grpc::flight;
    use common_query::Output;
    use common_recordbatch::RecordBatches;

    use super::*;
    use crate::table::DistTable;
    use crate::tests;
    use crate::tests::MockDistributedInstance;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_distributed_handle_ddl_request() {
        let instance =
            tests::create_distributed_instance("test_distributed_handle_ddl_request").await;
        let frontend = &instance.frontend;

        test_handle_ddl_request(frontend).await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_standalone_handle_ddl_request() {
        let standalone =
            tests::create_standalone_instance("test_standalone_handle_ddl_request").await;
        let instance = &standalone.instance;

        test_handle_ddl_request(instance).await
    }

    async fn test_handle_ddl_request(instance: &Arc<Instance>) {
        let ticket = Request::new(Ticket {
            ticket: ObjectExpr {
                request: Some(GrpcRequest::Ddl(DdlRequest {
                    expr: Some(DdlExpr::CreateDatabase(CreateDatabaseExpr {
                        database_name: "database_created_through_grpc".to_string(),
                    })),
                })),
            }
            .encode_to_vec(),
        });
        let output = boarding(instance, ticket).await;
        assert!(matches!(output, RpcOutput::AffectedRows(1)));

        let ticket = Request::new(Ticket {
            ticket: ObjectExpr {
                request: Some(GrpcRequest::Ddl(DdlRequest {
                    expr: Some(DdlExpr::CreateTable(CreateTableExpr {
                        catalog_name: "greptime".to_string(),
                        schema_name: "database_created_through_grpc".to_string(),
                        table_name: "table_created_through_grpc".to_string(),
                        column_defs: vec![
                            ColumnDef {
                                name: "a".to_string(),
                                datatype: ColumnDataType::String as _,
                                is_nullable: true,
                                default_constraint: vec![],
                            },
                            ColumnDef {
                                name: "ts".to_string(),
                                datatype: ColumnDataType::TimestampMillisecond as _,
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
        let output = boarding(instance, ticket).await;
        assert!(matches!(output, RpcOutput::AffectedRows(0)));

        let ticket = Request::new(Ticket {
            ticket: ObjectExpr {
                request: Some(GrpcRequest::Ddl(DdlRequest {
                    expr: Some(DdlExpr::Alter(AlterExpr {
                        catalog_name: "greptime".to_string(),
                        schema_name: "database_created_through_grpc".to_string(),
                        table_name: "table_created_through_grpc".to_string(),
                        kind: Some(alter_expr::Kind::AddColumns(AddColumns {
                            add_columns: vec![AddColumn {
                                column_def: Some(ColumnDef {
                                    name: "b".to_string(),
                                    datatype: ColumnDataType::Int32 as _,
                                    is_nullable: true,
                                    default_constraint: vec![],
                                }),
                                is_key: false,
                            }],
                        })),
                    })),
                })),
            }
            .encode_to_vec(),
        });
        let output = boarding(instance, ticket).await;
        assert!(matches!(output, RpcOutput::AffectedRows(0)));

        let ticket = Request::new(Ticket {
            ticket: ObjectExpr {
                request: Some(GrpcRequest::Query(QueryRequest {
                    query: Some(Query::Sql("INSERT INTO database_created_through_grpc.table_created_through_grpc (a, b, ts) VALUES ('s', 1, 1672816466000)".to_string()))
                }))
            }.encode_to_vec()
        });
        let output = boarding(instance, ticket).await;
        assert!(matches!(output, RpcOutput::AffectedRows(1)));

        let ticket = Request::new(Ticket {
            ticket: ObjectExpr {
                request: Some(GrpcRequest::Query(QueryRequest {
                    query: Some(Query::Sql("SELECT ts, a, b FROM database_created_through_grpc.table_created_through_grpc".to_string()))
                }))
            }.encode_to_vec()
        });
        let output = boarding(instance, ticket).await;
        let RpcOutput::RecordBatches(recordbatches) = output else { unreachable!() };
        let expected = "\
+---------------------+---+---+
| ts                  | a | b |
+---------------------+---+---+
| 2023-01-04T07:14:26 | s | 1 |
+---------------------+---+---+";
        assert_eq!(recordbatches.pretty_print().unwrap(), expected);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_distributed_insert_and_query() {
        common_telemetry::init_default_ut_logging();

        let instance =
            tests::create_distributed_instance("test_distributed_insert_and_query").await;
        let frontend = &instance.frontend;

        let table_name = "my_dist_table";
        let sql = format!(
            r"
CREATE TABLE {table_name} (
    a INT, 
    ts TIMESTAMP, 
    TIME INDEX (ts)
) PARTITION BY RANGE COLUMNS(a) (
    PARTITION r0 VALUES LESS THAN (10),
    PARTITION r1 VALUES LESS THAN (20),
    PARTITION r2 VALUES LESS THAN (50),
    PARTITION r3 VALUES LESS THAN (MAXVALUE),
)"
        );
        create_table(frontend, sql).await;

        test_insert_and_query_on_existing_table(frontend, table_name).await;

        verify_data_distribution(
            &instance,
            table_name,
            HashMap::from([
                (
                    0u32,
                    "\
+---------------------+---+
| ts                  | a |
+---------------------+---+
| 2023-01-01T07:26:12 | 1 |
| 2023-01-01T07:26:14 |   |
+---------------------+---+",
                ),
                (
                    1u32,
                    "\
+---------------------+----+
| ts                  | a  |
+---------------------+----+
| 2023-01-01T07:26:13 | 11 |
+---------------------+----+",
                ),
                (
                    2u32,
                    "\
+---------------------+----+
| ts                  | a  |
+---------------------+----+
| 2023-01-01T07:26:15 | 20 |
| 2023-01-01T07:26:16 | 22 |
+---------------------+----+",
                ),
                (
                    3u32,
                    "\
+---------------------+----+
| ts                  | a  |
+---------------------+----+
| 2023-01-01T07:26:17 | 50 |
| 2023-01-01T07:26:18 | 55 |
| 2023-01-01T07:26:19 | 99 |
+---------------------+----+",
                ),
            ]),
        )
        .await;

        test_insert_and_query_on_auto_created_table(frontend).await;

        // Auto created table has only one region.
        verify_data_distribution(
            &instance,
            "auto_created_table",
            HashMap::from([(
                0u32,
                "\
+---------------------+---+
| ts                  | a |
+---------------------+---+
| 2023-01-01T07:26:15 | 4 |
| 2023-01-01T07:26:16 |   |
| 2023-01-01T07:26:17 | 6 |
| 2023-01-01T07:26:18 |   |
| 2023-01-01T07:26:19 |   |
| 2023-01-01T07:26:20 |   |
+---------------------+---+",
            )]),
        )
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_standalone_insert_and_query() {
        common_telemetry::init_default_ut_logging();

        let standalone =
            tests::create_standalone_instance("test_standalone_insert_and_query").await;
        let instance = &standalone.instance;

        let table_name = "my_table";
        let sql = format!("CREATE TABLE {table_name} (a INT, ts TIMESTAMP, TIME INDEX (ts))");
        create_table(instance, sql).await;

        test_insert_and_query_on_existing_table(instance, table_name).await;

        test_insert_and_query_on_auto_created_table(instance).await
    }

    async fn create_table(frontend: &Arc<Instance>, sql: String) {
        let ticket = Request::new(Ticket {
            ticket: ObjectExpr {
                request: Some(GrpcRequest::Query(QueryRequest {
                    query: Some(Query::Sql(sql)),
                })),
            }
            .encode_to_vec(),
        });
        let output = boarding(frontend, ticket).await;
        assert!(matches!(output, RpcOutput::AffectedRows(0)));
    }

    async fn test_insert_and_query_on_existing_table(instance: &Arc<Instance>, table_name: &str) {
        let insert = InsertRequest {
            schema_name: "public".to_string(),
            table_name: table_name.to_string(),
            columns: vec![
                Column {
                    column_name: "a".to_string(),
                    values: Some(Values {
                        i32_values: vec![1, 11, 20, 22, 50, 55, 99],
                        ..Default::default()
                    }),
                    null_mask: vec![4],
                    semantic_type: SemanticType::Field as i32,
                    datatype: ColumnDataType::Int32 as i32,
                },
                Column {
                    column_name: "ts".to_string(),
                    values: Some(Values {
                        ts_millisecond_values: vec![
                            1672557972000,
                            1672557973000,
                            1672557974000,
                            1672557975000,
                            1672557976000,
                            1672557977000,
                            1672557978000,
                            1672557979000,
                        ],
                        ..Default::default()
                    }),
                    semantic_type: SemanticType::Timestamp as i32,
                    datatype: ColumnDataType::TimestampMillisecond as i32,
                    ..Default::default()
                },
            ],
            row_count: 8,
            ..Default::default()
        };

        let ticket = Request::new(Ticket {
            ticket: ObjectExpr {
                request: Some(GrpcRequest::Insert(insert)),
            }
            .encode_to_vec(),
        });

        let output = boarding(instance, ticket).await;
        assert!(matches!(output, RpcOutput::AffectedRows(8)));

        let ticket = Request::new(Ticket {
            ticket: ObjectExpr {
                request: Some(GrpcRequest::Query(QueryRequest {
                    query: Some(Query::Sql(format!(
                        "SELECT ts, a FROM {table_name} ORDER BY ts"
                    ))),
                })),
            }
            .encode_to_vec(),
        });

        let output = boarding(instance, ticket).await;
        let RpcOutput::RecordBatches(recordbatches) = output else { unreachable!() };
        let expected = "\
+---------------------+----+
| ts                  | a  |
+---------------------+----+
| 2023-01-01T07:26:12 | 1  |
| 2023-01-01T07:26:13 | 11 |
| 2023-01-01T07:26:14 |    |
| 2023-01-01T07:26:15 | 20 |
| 2023-01-01T07:26:16 | 22 |
| 2023-01-01T07:26:17 | 50 |
| 2023-01-01T07:26:18 | 55 |
| 2023-01-01T07:26:19 | 99 |
+---------------------+----+";
        assert_eq!(recordbatches.pretty_print().unwrap(), expected);
    }

    async fn verify_data_distribution(
        instance: &MockDistributedInstance,
        table_name: &str,
        expected_distribution: HashMap<u32, &str>,
    ) {
        let table = instance
            .frontend
            .catalog_manager()
            .table("greptime", "public", table_name)
            .unwrap()
            .unwrap();
        let table = table.as_any().downcast_ref::<DistTable>().unwrap();

        let TableGlobalValue { regions_id_map, .. } = table
            .table_global_value(&TableGlobalKey {
                catalog_name: "greptime".to_string(),
                schema_name: "public".to_string(),
                table_name: table_name.to_string(),
            })
            .await
            .unwrap()
            .unwrap();
        let region_to_dn_map = regions_id_map
            .iter()
            .map(|(k, v)| (v[0], *k))
            .collect::<HashMap<u32, u64>>();
        assert_eq!(region_to_dn_map.len(), expected_distribution.len());

        for (region, dn) in region_to_dn_map.iter() {
            let dn = instance.datanodes.get(dn).unwrap();
            let output = dn
                .execute_sql(
                    &format!("SELECT ts, a FROM {table_name} ORDER BY ts"),
                    QueryContext::arc(),
                )
                .await
                .unwrap();
            let Output::Stream(stream) = output else { unreachable!() };
            let recordbatches = RecordBatches::try_collect(stream).await.unwrap();
            let actual = recordbatches.pretty_print().unwrap();

            let expected = expected_distribution.get(region).unwrap();
            assert_eq!(&actual, expected);
        }
    }

    async fn test_insert_and_query_on_auto_created_table(instance: &Arc<Instance>) {
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
