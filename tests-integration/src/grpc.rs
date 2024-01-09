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

#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use api::v1::column::Values;
    use api::v1::ddl_request::Expr as DdlExpr;
    use api::v1::greptime_request::Request;
    use api::v1::query_request::Query;
    use api::v1::region::QueryRequest as RegionQueryRequest;
    use api::v1::{
        alter_expr, AddColumn, AddColumns, AlterExpr, Column, ColumnDataType, ColumnDef,
        CreateDatabaseExpr, CreateTableExpr, DdlRequest, DeleteRequest, DeleteRequests,
        DropTableExpr, InsertRequest, InsertRequests, QueryRequest, SemanticType,
    };
    use common_catalog::consts::MITO_ENGINE;
    use common_meta::rpc::router::region_distribution;
    use common_query::Output;
    use common_recordbatch::RecordBatches;
    use frontend::instance::Instance;
    use query::parser::QueryLanguageParser;
    use query::plan::LogicalPlan;
    use servers::query_handler::grpc::GrpcQueryHandler;
    use session::context::QueryContext;
    use store_api::storage::RegionId;
    use substrait::{DFLogicalSubstraitConvertor, SubstraitPlan};

    use crate::standalone::GreptimeDbStandaloneBuilder;
    use crate::tests;
    use crate::tests::MockDistributedInstance;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_distributed_handle_ddl_request() {
        common_telemetry::init_default_ut_logging();
        let instance =
            tests::create_distributed_instance("test_distributed_handle_ddl_request").await;

        test_handle_ddl_request(instance.frontend().as_ref()).await;

        verify_table_is_dropped(&instance).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_standalone_handle_ddl_request() {
        let standalone = GreptimeDbStandaloneBuilder::new("test_standalone_handle_ddl_request")
            .build()
            .await;
        let instance = &standalone.instance;

        test_handle_ddl_request(instance.as_ref()).await;
    }

    async fn query(instance: &Instance, request: Request) -> Output {
        GrpcQueryHandler::do_query(instance, request, QueryContext::arc())
            .await
            .unwrap()
    }

    async fn test_handle_ddl_request(instance: &Instance) {
        let request = Request::Ddl(DdlRequest {
            expr: Some(DdlExpr::CreateDatabase(CreateDatabaseExpr {
                database_name: "database_created_through_grpc".to_string(),
                create_if_not_exists: true,
                options: Default::default(),
            })),
        });
        let output = query(instance, request).await;
        assert!(matches!(output, Output::AffectedRows(1)));

        let request = Request::Ddl(DdlRequest {
            expr: Some(DdlExpr::CreateTable(CreateTableExpr {
                catalog_name: "greptime".to_string(),
                schema_name: "database_created_through_grpc".to_string(),
                table_name: "table_created_through_grpc".to_string(),
                column_defs: vec![
                    ColumnDef {
                        name: "a".to_string(),
                        data_type: ColumnDataType::String as _,
                        is_nullable: true,
                        default_constraint: vec![],
                        semantic_type: SemanticType::Field as i32,
                        ..Default::default()
                    },
                    ColumnDef {
                        name: "ts".to_string(),
                        data_type: ColumnDataType::TimestampMillisecond as _,
                        is_nullable: false,
                        default_constraint: vec![],
                        semantic_type: SemanticType::Timestamp as i32,
                        ..Default::default()
                    },
                ],
                time_index: "ts".to_string(),
                engine: MITO_ENGINE.to_string(),
                ..Default::default()
            })),
        });
        let output = query(instance, request).await;
        assert!(matches!(output, Output::AffectedRows(0)));

        let request = Request::Ddl(DdlRequest {
            expr: Some(DdlExpr::Alter(AlterExpr {
                catalog_name: "greptime".to_string(),
                schema_name: "database_created_through_grpc".to_string(),
                table_name: "table_created_through_grpc".to_string(),
                kind: Some(alter_expr::Kind::AddColumns(AddColumns {
                    add_columns: vec![AddColumn {
                        column_def: Some(ColumnDef {
                            name: "b".to_string(),
                            data_type: ColumnDataType::Int32 as _,
                            is_nullable: true,
                            default_constraint: vec![],
                            semantic_type: SemanticType::Field as i32,
                            ..Default::default()
                        }),
                        location: None,
                    }],
                })),
            })),
        });
        let output = query(instance, request).await;
        assert!(matches!(output, Output::AffectedRows(0)));

        let request = Request::Query(QueryRequest {
            query: Some(Query::Sql("INSERT INTO database_created_through_grpc.table_created_through_grpc (a, b, ts) VALUES ('s', 1, 1672816466000)".to_string()))
        });
        let output = query(instance, request).await;
        assert!(matches!(output, Output::AffectedRows(1)));

        let request = Request::Query(QueryRequest {
            query: Some(Query::Sql(
                "SELECT ts, a, b FROM database_created_through_grpc.table_created_through_grpc"
                    .to_string(),
            )),
        });
        let output = query(instance, request).await;
        let Output::Stream(stream) = output else {
            unreachable!()
        };
        let recordbatches = RecordBatches::try_collect(stream).await.unwrap();
        let expected = "\
+---------------------+---+---+
| ts                  | a | b |
+---------------------+---+---+
| 2023-01-04T07:14:26 | s | 1 |
+---------------------+---+---+";
        assert_eq!(recordbatches.pretty_print().unwrap(), expected);

        let request = Request::Ddl(DdlRequest {
            expr: Some(DdlExpr::DropTable(DropTableExpr {
                catalog_name: "greptime".to_string(),
                schema_name: "database_created_through_grpc".to_string(),
                table_name: "table_created_through_grpc".to_string(),
                ..Default::default()
            })),
        });
        let output = query(instance, request).await;
        assert!(matches!(output, Output::AffectedRows(0)));
    }

    async fn verify_table_is_dropped(instance: &MockDistributedInstance) {
        assert!(instance
            .frontend()
            .catalog_manager()
            .table(
                "greptime",
                "database_created_through_grpc",
                "table_created_through_grpc"
            )
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_distributed_insert_delete_and_query() {
        common_telemetry::init_default_ut_logging();

        let instance =
            tests::create_distributed_instance("test_distributed_insert_delete_and_query").await;
        let frontend = instance.frontend();
        let frontend = frontend.as_ref();

        let table_name = "my_dist_table";
        let sql = format!(
            r"
CREATE TABLE {table_name} (
    a INT,
    b STRING,
    ts TIMESTAMP,
    TIME INDEX (ts),
    PRIMARY KEY (a, b)
) PARTITION BY RANGE COLUMNS(a) (
    PARTITION r0 VALUES LESS THAN (10),
    PARTITION r1 VALUES LESS THAN (20),
    PARTITION r2 VALUES LESS THAN (50),
    PARTITION r3 VALUES LESS THAN (MAXVALUE),
)"
        );
        create_table(frontend, sql).await;

        test_insert_delete_and_query_on_existing_table(frontend, table_name).await;

        verify_data_distribution(
            &instance,
            table_name,
            HashMap::from([
                (
                    0u32,
                    "\
+---------------------+---+-------------------+
| ts                  | a | b                 |
+---------------------+---+-------------------+
| 2023-01-01T07:26:12 | 1 | ts: 1672557972000 |
| 2023-01-01T07:26:15 | 4 | ts: 1672557975000 |
| 2023-01-01T07:26:16 | 5 | ts: 1672557976000 |
| 2023-01-01T07:26:17 |   | ts: 1672557977000 |
+---------------------+---+-------------------+",
                ),
                (
                    1u32,
                    "\
+---------------------+----+-------------------+
| ts                  | a  | b                 |
+---------------------+----+-------------------+
| 2023-01-01T07:26:18 | 11 | ts: 1672557978000 |
+---------------------+----+-------------------+",
                ),
                (
                    2u32,
                    "\
+---------------------+----+-------------------+
| ts                  | a  | b                 |
+---------------------+----+-------------------+
| 2023-01-01T07:26:20 | 20 | ts: 1672557980000 |
| 2023-01-01T07:26:21 | 21 | ts: 1672557981000 |
| 2023-01-01T07:26:23 | 23 | ts: 1672557983000 |
+---------------------+----+-------------------+",
                ),
                (
                    3u32,
                    "\
+---------------------+----+-------------------+
| ts                  | a  | b                 |
+---------------------+----+-------------------+
| 2023-01-01T07:26:24 | 50 | ts: 1672557984000 |
| 2023-01-01T07:26:25 | 51 | ts: 1672557985000 |
+---------------------+----+-------------------+",
                ),
            ]),
        )
        .await;

        test_insert_delete_and_query_on_auto_created_table(frontend).await;

        // Auto created table has only one region.
        verify_data_distribution(
            &instance,
            "auto_created_table",
            HashMap::from([(
                0u32,
                "\
+---------------------+---+---+
| ts                  | a | b |
+---------------------+---+---+
| 2023-01-01T07:26:16 |   |   |
| 2023-01-01T07:26:17 | 6 |   |
| 2023-01-01T07:26:18 |   | x |
| 2023-01-01T07:26:20 |   | z |
+---------------------+---+---+",
            )]),
        )
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_standalone_insert_and_query() {
        let standalone = GreptimeDbStandaloneBuilder::new("test_standalone_insert_and_query")
            .build()
            .await;
        let instance = &standalone.instance;

        let table_name = "my_table";
        let sql = format!("CREATE TABLE {table_name} (a INT, b STRING, ts TIMESTAMP, TIME INDEX (ts), PRIMARY KEY (a, b))");
        create_table(instance, sql).await;

        test_insert_delete_and_query_on_existing_table(instance, table_name).await;

        test_insert_delete_and_query_on_auto_created_table(instance).await
    }

    async fn create_table(frontend: &Instance, sql: String) {
        let request = Request::Query(QueryRequest {
            query: Some(Query::Sql(sql)),
        });
        let output = query(frontend, request).await;
        assert!(matches!(output, Output::AffectedRows(0)));
    }

    async fn test_insert_delete_and_query_on_existing_table(instance: &Instance, table_name: &str) {
        let timestamp_millisecond_values = vec![
            1672557972000,
            1672557973000,
            1672557974000,
            1672557975000,
            1672557976000,
            1672557977000,
            1672557978000,
            1672557979000,
            1672557980000,
            1672557981000,
            1672557982000,
            1672557983000,
            1672557984000,
            1672557985000,
            1672557986000,
            1672557987000,
        ];
        let insert = InsertRequest {
            table_name: table_name.to_string(),
            columns: vec![
                Column {
                    column_name: "a".to_string(),
                    values: Some(Values {
                        i32_values: vec![1, 2, 3, 4, 5, 11, 12, 20, 21, 22, 23, 50, 51, 52, 53],
                        ..Default::default()
                    }),
                    null_mask: vec![32, 0],
                    semantic_type: SemanticType::Tag as i32,
                    datatype: ColumnDataType::Int32 as i32,
                    ..Default::default()
                },
                Column {
                    column_name: "b".to_string(),
                    values: Some(Values {
                        string_values: timestamp_millisecond_values
                            .iter()
                            .map(|x| format!("ts: {x}"))
                            .collect(),
                        ..Default::default()
                    }),
                    semantic_type: SemanticType::Tag as i32,
                    datatype: ColumnDataType::String as i32,
                    ..Default::default()
                },
                Column {
                    column_name: "ts".to_string(),
                    values: Some(Values {
                        timestamp_millisecond_values,
                        ..Default::default()
                    }),
                    semantic_type: SemanticType::Timestamp as i32,
                    datatype: ColumnDataType::TimestampMillisecond as i32,
                    ..Default::default()
                },
            ],
            row_count: 16,
        };
        let output = query(
            instance,
            Request::Inserts(InsertRequests {
                inserts: vec![insert],
            }),
        )
        .await;
        assert!(matches!(output, Output::AffectedRows(16)));

        let request = Request::Query(QueryRequest {
            query: Some(Query::Sql(format!(
                "SELECT ts, a, b FROM {table_name} ORDER BY ts"
            ))),
        });
        let output = query(instance, request.clone()).await;
        let Output::Stream(stream) = output else {
            unreachable!()
        };
        let recordbatches = RecordBatches::try_collect(stream).await.unwrap();
        let expected = "\
+---------------------+----+-------------------+
| ts                  | a  | b                 |
+---------------------+----+-------------------+
| 2023-01-01T07:26:12 | 1  | ts: 1672557972000 |
| 2023-01-01T07:26:13 | 2  | ts: 1672557973000 |
| 2023-01-01T07:26:14 | 3  | ts: 1672557974000 |
| 2023-01-01T07:26:15 | 4  | ts: 1672557975000 |
| 2023-01-01T07:26:16 | 5  | ts: 1672557976000 |
| 2023-01-01T07:26:17 |    | ts: 1672557977000 |
| 2023-01-01T07:26:18 | 11 | ts: 1672557978000 |
| 2023-01-01T07:26:19 | 12 | ts: 1672557979000 |
| 2023-01-01T07:26:20 | 20 | ts: 1672557980000 |
| 2023-01-01T07:26:21 | 21 | ts: 1672557981000 |
| 2023-01-01T07:26:22 | 22 | ts: 1672557982000 |
| 2023-01-01T07:26:23 | 23 | ts: 1672557983000 |
| 2023-01-01T07:26:24 | 50 | ts: 1672557984000 |
| 2023-01-01T07:26:25 | 51 | ts: 1672557985000 |
| 2023-01-01T07:26:26 | 52 | ts: 1672557986000 |
| 2023-01-01T07:26:27 | 53 | ts: 1672557987000 |
+---------------------+----+-------------------+";
        assert_eq!(recordbatches.pretty_print().unwrap(), expected);

        let new_grpc_delete_request = |a, b, ts, row_count| DeleteRequest {
            table_name: table_name.to_string(),
            key_columns: vec![
                Column {
                    column_name: "a".to_string(),
                    semantic_type: SemanticType::Tag as i32,
                    values: Some(Values {
                        i32_values: a,
                        ..Default::default()
                    }),
                    datatype: ColumnDataType::Int32 as i32,
                    ..Default::default()
                },
                Column {
                    column_name: "b".to_string(),
                    semantic_type: SemanticType::Tag as i32,
                    values: Some(Values {
                        string_values: b,
                        ..Default::default()
                    }),
                    datatype: ColumnDataType::String as i32,
                    ..Default::default()
                },
                Column {
                    column_name: "ts".to_string(),
                    semantic_type: SemanticType::Timestamp as i32,
                    values: Some(Values {
                        timestamp_millisecond_values: ts,
                        ..Default::default()
                    }),
                    datatype: ColumnDataType::TimestampMillisecond as i32,
                    ..Default::default()
                },
            ],
            row_count,
        };
        let delete1 = new_grpc_delete_request(
            vec![2, 12, 22, 52],
            vec![
                "ts: 1672557973000".to_string(),
                "ts: 1672557979000".to_string(),
                "ts: 1672557982000".to_string(),
                "ts: 1672557986000".to_string(),
            ],
            vec![1672557973000, 1672557979000, 1672557982000, 1672557986000],
            4,
        );
        let delete2 = new_grpc_delete_request(
            vec![3, 53],
            vec![
                "ts: 1672557974000".to_string(),
                "ts: 1672557987000".to_string(),
            ],
            vec![1672557974000, 1672557987000],
            2,
        );
        let output = query(
            instance,
            Request::Deletes(DeleteRequests {
                deletes: vec![delete1, delete2],
            }),
        )
        .await;
        assert!(matches!(output, Output::AffectedRows(6)));

        let output = query(instance, request).await;
        let Output::Stream(stream) = output else {
            unreachable!()
        };
        let recordbatches = RecordBatches::try_collect(stream).await.unwrap();
        let expected = "\
+---------------------+----+-------------------+
| ts                  | a  | b                 |
+---------------------+----+-------------------+
| 2023-01-01T07:26:12 | 1  | ts: 1672557972000 |
| 2023-01-01T07:26:15 | 4  | ts: 1672557975000 |
| 2023-01-01T07:26:16 | 5  | ts: 1672557976000 |
| 2023-01-01T07:26:17 |    | ts: 1672557977000 |
| 2023-01-01T07:26:18 | 11 | ts: 1672557978000 |
| 2023-01-01T07:26:20 | 20 | ts: 1672557980000 |
| 2023-01-01T07:26:21 | 21 | ts: 1672557981000 |
| 2023-01-01T07:26:23 | 23 | ts: 1672557983000 |
| 2023-01-01T07:26:24 | 50 | ts: 1672557984000 |
| 2023-01-01T07:26:25 | 51 | ts: 1672557985000 |
+---------------------+----+-------------------+";
        assert_eq!(recordbatches.pretty_print().unwrap(), expected);
    }

    async fn verify_data_distribution(
        instance: &MockDistributedInstance,
        table_name: &str,
        expected_distribution: HashMap<u32, &str>,
    ) {
        let table = instance
            .frontend()
            .catalog_manager()
            .table("greptime", "public", table_name)
            .await
            .unwrap()
            .unwrap();
        let table_id = table.table_info().ident.table_id;
        let table_route_value = instance
            .table_metadata_manager()
            .table_route_manager()
            .get(table_id)
            .await
            .unwrap()
            .unwrap()
            .into_inner();

        let region_to_dn_map = region_distribution(
            table_route_value
                .region_routes()
                .expect("physical table route"),
        )
        .iter()
        .map(|(k, v)| (v[0], *k))
        .collect::<HashMap<u32, u64>>();
        assert!(region_to_dn_map.len() <= instance.datanodes().len());

        let stmt = QueryLanguageParser::parse_sql(&format!(
            "SELECT ts, a, b FROM {table_name} ORDER BY ts"
        ))
        .unwrap();
        let LogicalPlan::DfPlan(plan) = instance
            .frontend()
            .statement_executor()
            .plan(stmt, QueryContext::arc())
            .await
            .unwrap();
        let plan = DFLogicalSubstraitConvertor.encode(&plan).unwrap();

        for (region, dn) in region_to_dn_map.iter() {
            let region_server = instance.datanodes().get(dn).unwrap().region_server();

            let region_id = RegionId::new(table_id, *region);

            let stream = region_server
                .handle_read(RegionQueryRequest {
                    region_id: region_id.as_u64(),
                    plan: plan.to_vec(),
                    ..Default::default()
                })
                .await
                .unwrap();

            let recordbatches = RecordBatches::try_collect(stream).await.unwrap();
            let actual = recordbatches.pretty_print().unwrap();

            let expected = expected_distribution.get(region).unwrap();
            assert_eq!(&actual, expected);
        }
    }

    async fn test_insert_delete_and_query_on_auto_created_table(instance: &Instance) {
        let insert = InsertRequest {
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
                    ..Default::default()
                },
                Column {
                    column_name: "ts".to_string(),
                    values: Some(Values {
                        timestamp_millisecond_values: vec![
                            1672557975000,
                            1672557976000,
                            1672557977000,
                        ],
                        ..Default::default()
                    }),
                    semantic_type: SemanticType::Timestamp as i32,
                    datatype: ColumnDataType::TimestampMillisecond as i32,
                    ..Default::default()
                },
            ],
            row_count: 3,
        };

        // Test auto create not existed table upon insertion.
        let request = Request::Inserts(InsertRequests {
            inserts: vec![insert],
        });
        let output = query(instance, request).await;
        assert!(matches!(output, Output::AffectedRows(3)));

        let insert = InsertRequest {
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
                    ..Default::default()
                },
                Column {
                    column_name: "ts".to_string(),
                    values: Some(Values {
                        timestamp_millisecond_values: vec![
                            1672557978000,
                            1672557979000,
                            1672557980000,
                        ],
                        ..Default::default()
                    }),
                    semantic_type: SemanticType::Timestamp as i32,
                    datatype: ColumnDataType::TimestampMillisecond as i32,
                    ..Default::default()
                },
            ],
            row_count: 3,
        };

        // Test auto add not existed column upon insertion.
        let request = Request::Inserts(InsertRequests {
            inserts: vec![insert],
        });
        let output = query(instance, request).await;
        assert!(matches!(output, Output::AffectedRows(3)));

        let request = Request::Query(QueryRequest {
            query: Some(Query::Sql(
                "SELECT ts, a, b FROM auto_created_table".to_string(),
            )),
        });
        let output = query(instance, request.clone()).await;
        let Output::Stream(stream) = output else {
            unreachable!()
        };
        let recordbatches = RecordBatches::try_collect(stream).await.unwrap();
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

        let delete = DeleteRequest {
            table_name: "auto_created_table".to_string(),
            key_columns: vec![Column {
                column_name: "ts".to_string(),
                values: Some(Values {
                    timestamp_millisecond_values: vec![1672557975000, 1672557979000],
                    ..Default::default()
                }),
                semantic_type: SemanticType::Timestamp as i32,
                datatype: ColumnDataType::TimestampMillisecond as i32,
                ..Default::default()
            }],
            row_count: 2,
        };

        let output = query(
            instance,
            Request::Deletes(DeleteRequests {
                deletes: vec![delete],
            }),
        )
        .await;
        assert!(matches!(output, Output::AffectedRows(2)));

        let output = query(instance, request).await;
        let Output::Stream(stream) = output else {
            unreachable!()
        };
        let recordbatches = RecordBatches::try_collect(stream).await.unwrap();
        let expected = "\
+---------------------+---+---+
| ts                  | a | b |
+---------------------+---+---+
| 2023-01-01T07:26:16 |   |   |
| 2023-01-01T07:26:17 | 6 |   |
| 2023-01-01T07:26:18 |   | x |
| 2023-01-01T07:26:20 |   | z |
+---------------------+---+---+";
        assert_eq!(recordbatches.pretty_print().unwrap(), expected);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_promql_query() {
        let standalone = GreptimeDbStandaloneBuilder::new("test_standalone_promql_query")
            .build()
            .await;
        let instance = &standalone.instance;

        let table_name = "my_table";
        let sql = format!("CREATE TABLE {table_name} (h string, a double, ts TIMESTAMP, TIME INDEX (ts), PRIMARY KEY(h))");
        create_table(instance, sql).await;

        let insert = InsertRequest {
            table_name: table_name.to_string(),
            columns: vec![
                Column {
                    column_name: "h".to_string(),
                    values: Some(Values {
                        string_values: vec![
                            "t".to_string(),
                            "t".to_string(),
                            "t".to_string(),
                            "t".to_string(),
                            "t".to_string(),
                            "t".to_string(),
                            "t".to_string(),
                            "t".to_string(),
                        ],
                        ..Default::default()
                    }),
                    semantic_type: SemanticType::Tag as i32,
                    datatype: ColumnDataType::String as i32,
                    ..Default::default()
                },
                Column {
                    column_name: "a".to_string(),
                    values: Some(Values {
                        f64_values: vec![1f64, 11f64, 20f64, 22f64, 50f64, 55f64, 99f64],
                        ..Default::default()
                    }),
                    null_mask: vec![4],
                    semantic_type: SemanticType::Field as i32,
                    datatype: ColumnDataType::Float64 as i32,
                    ..Default::default()
                },
                Column {
                    column_name: "ts".to_string(),
                    values: Some(Values {
                        timestamp_millisecond_values: vec![
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
        };

        let request = Request::Inserts(InsertRequests {
            inserts: vec![insert],
        });
        let output = query(instance, request).await;
        assert!(matches!(output, Output::AffectedRows(8)));

        let request = Request::Query(QueryRequest {
            query: Some(Query::PromRangeQuery(api::v1::PromRangeQuery {
                query: "my_table".to_owned(),
                start: "1672557973".to_owned(),
                end: "1672557978".to_owned(),
                step: "1s".to_owned(),
            })),
        });
        let output = query(instance, request).await;
        let Output::Stream(stream) = output else {
            unreachable!()
        };
        let recordbatches = RecordBatches::try_collect(stream).await.unwrap();
        let expected = "\
+---+------+---------------------+
| h | a    | ts                  |
+---+------+---------------------+
| t | 11.0 | 2023-01-01T07:26:13 |
| t |      | 2023-01-01T07:26:14 |
| t | 20.0 | 2023-01-01T07:26:15 |
| t | 22.0 | 2023-01-01T07:26:16 |
| t | 50.0 | 2023-01-01T07:26:17 |
| t | 55.0 | 2023-01-01T07:26:18 |
+---+------+---------------------+";
        assert_eq!(recordbatches.pretty_print().unwrap(), expected);
    }
}
