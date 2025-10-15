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

mod flight;
mod network;

use api::v1::QueryRequest;
use api::v1::greptime_request::Request;
use api::v1::query_request::Query;
use common_query::OutputData;
use common_recordbatch::RecordBatches;
use frontend::instance::Instance;
use servers::query_handler::grpc::GrpcQueryHandler;
use session::context::QueryContext;

#[allow(unused)]
async fn query_and_expect(instance: &Instance, sql: &str, expected: &str) {
    let request = Request::Query(QueryRequest {
        query: Some(Query::Sql(sql.to_string())),
    });
    let output = GrpcQueryHandler::do_query(instance, request, QueryContext::arc())
        .await
        .unwrap();
    let OutputData::Stream(stream) = output.data else {
        unreachable!()
    };
    let recordbatches = RecordBatches::try_collect(stream).await.unwrap();
    let actual = recordbatches.pretty_print().unwrap();
    assert_eq!(actual, expected, "actual: {}", actual);
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;
    use std::sync::Arc;

    use api::v1::column::Values;
    use api::v1::column_data_type_extension::TypeExt;
    use api::v1::ddl_request::Expr as DdlExpr;
    use api::v1::greptime_request::Request;
    use api::v1::query_request::Query;
    use api::v1::region::QueryRequest as RegionQueryRequest;
    use api::v1::{
        AddColumn, AddColumns, AlterTableExpr, Column, ColumnDataType, ColumnDataTypeExtension,
        ColumnDef, CreateDatabaseExpr, CreateTableExpr, DdlRequest, DeleteRequest, DeleteRequests,
        DropTableExpr, InsertRequest, InsertRequests, QueryRequest, SemanticType,
        VectorTypeExtension, alter_table_expr,
    };
    use client::OutputData;
    use common_catalog::consts::MITO_ENGINE;
    use common_meta::rpc::router::region_distribution;
    use common_query::Output;
    use common_recordbatch::RecordBatches;
    use common_test_util::recordbatch::check_output_stream;
    use frontend::instance::Instance;
    use query::parser::QueryLanguageParser;
    use query::query_engine::DefaultSerializer;
    use rstest::rstest;
    use rstest_reuse::apply;
    use servers::query_handler::grpc::GrpcQueryHandler;
    use session::context::{QueryContext, QueryContextBuilder};
    use store_api::mito_engine_options::TWCS_TIME_WINDOW;
    use store_api::storage::RegionId;
    use substrait::{DFLogicalSubstraitConvertor, SubstraitPlan};

    use super::*;
    use crate::standalone::GreptimeDbStandaloneBuilder;
    use crate::tests;
    use crate::tests::MockDistributedInstance;
    use crate::tests::test_util::{
        MockInstance, both_instances_cases, distributed, execute_sql, standalone,
    };

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
        let instance = standalone.fe_instance();

        test_handle_ddl_request(instance.as_ref()).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_distributed_handle_multi_ddl_request() {
        common_telemetry::init_default_ut_logging();
        let instance =
            tests::create_distributed_instance("test_distributed_handle_multi_ddl_request").await;

        test_handle_multi_ddl_request(instance.frontend().as_ref()).await;

        verify_table_is_dropped(&instance).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_standalone_handle_multi_ddl_request() {
        let standalone =
            GreptimeDbStandaloneBuilder::new("test_standalone_handle_multi_ddl_request")
                .build()
                .await;
        let instance = standalone.fe_instance();

        test_handle_multi_ddl_request(instance.as_ref()).await;
    }

    async fn query(instance: &Instance, request: Request) -> Output {
        GrpcQueryHandler::do_query(instance, request, QueryContext::arc())
            .await
            .unwrap()
    }

    async fn test_handle_multi_ddl_request(instance: &Instance) {
        let request = Request::Ddl(DdlRequest {
            expr: Some(DdlExpr::CreateDatabase(CreateDatabaseExpr {
                catalog_name: "greptime".to_string(),
                schema_name: "database_created_through_grpc".to_string(),
                create_if_not_exists: true,
                options: Default::default(),
            })),
        });
        let output = query(instance, request).await;
        assert!(matches!(output.data, OutputData::AffectedRows(1)));

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
        assert!(matches!(output.data, OutputData::AffectedRows(0)));

        let request = Request::Ddl(DdlRequest {
            expr: Some(DdlExpr::AlterTable(AlterTableExpr {
                catalog_name: "greptime".to_string(),
                schema_name: "database_created_through_grpc".to_string(),
                table_name: "table_created_through_grpc".to_string(),
                kind: Some(alter_table_expr::Kind::AddColumns(AddColumns {
                    add_columns: vec![
                        AddColumn {
                            column_def: Some(ColumnDef {
                                name: "b".to_string(),
                                data_type: ColumnDataType::Int32 as _,
                                is_nullable: true,
                                default_constraint: vec![],
                                semantic_type: SemanticType::Field as i32,
                                ..Default::default()
                            }),
                            location: None,
                            add_if_not_exists: true,
                        },
                        AddColumn {
                            column_def: Some(ColumnDef {
                                name: "a".to_string(),
                                data_type: ColumnDataType::String as _,
                                is_nullable: true,
                                default_constraint: vec![],
                                semantic_type: SemanticType::Field as i32,
                                ..Default::default()
                            }),
                            location: None,
                            add_if_not_exists: true,
                        },
                    ],
                })),
            })),
        });
        let output = query(instance, request).await;
        assert!(matches!(output.data, OutputData::AffectedRows(0)));

        let request = Request::Ddl(DdlRequest {
            expr: Some(DdlExpr::AlterTable(AlterTableExpr {
                catalog_name: "greptime".to_string(),
                schema_name: "database_created_through_grpc".to_string(),
                table_name: "table_created_through_grpc".to_string(),
                kind: Some(alter_table_expr::Kind::AddColumns(AddColumns {
                    add_columns: vec![
                        AddColumn {
                            column_def: Some(ColumnDef {
                                name: "c".to_string(),
                                data_type: ColumnDataType::Int32 as _,
                                is_nullable: true,
                                default_constraint: vec![],
                                semantic_type: SemanticType::Field as i32,
                                ..Default::default()
                            }),
                            location: None,
                            add_if_not_exists: true,
                        },
                        AddColumn {
                            column_def: Some(ColumnDef {
                                name: "d".to_string(),
                                data_type: ColumnDataType::Int32 as _,
                                is_nullable: true,
                                default_constraint: vec![],
                                semantic_type: SemanticType::Field as i32,
                                ..Default::default()
                            }),
                            location: None,
                            add_if_not_exists: true,
                        },
                    ],
                })),
            })),
        });
        let output = query(instance, request).await;
        assert!(matches!(output.data, OutputData::AffectedRows(0)));

        let request = Request::Query(QueryRequest {
            query: Some(Query::Sql("INSERT INTO database_created_through_grpc.table_created_through_grpc (a, b, c, d, ts) VALUES ('s', 1, 1, 1, 1672816466000)".to_string()))
        });
        let output = query(instance, request).await;
        assert!(matches!(output.data, OutputData::AffectedRows(1)));

        let sql = "SELECT ts, a, b FROM database_created_through_grpc.table_created_through_grpc";
        let expected = "\
+---------------------+---+---+
| ts                  | a | b |
+---------------------+---+---+
| 2023-01-04T07:14:26 | s | 1 |
+---------------------+---+---+";
        query_and_expect(instance, sql, expected).await;

        let request = Request::Ddl(DdlRequest {
            expr: Some(DdlExpr::DropTable(DropTableExpr {
                catalog_name: "greptime".to_string(),
                schema_name: "database_created_through_grpc".to_string(),
                table_name: "table_created_through_grpc".to_string(),
                ..Default::default()
            })),
        });
        let output = query(instance, request).await;
        assert!(matches!(output.data, OutputData::AffectedRows(0)));
    }

    async fn test_handle_ddl_request(instance: &Instance) {
        let request = Request::Ddl(DdlRequest {
            expr: Some(DdlExpr::CreateDatabase(CreateDatabaseExpr {
                catalog_name: "greptime".to_string(),
                schema_name: "database_created_through_grpc".to_string(),
                create_if_not_exists: true,
                options: Default::default(),
            })),
        });
        let output = query(instance, request).await;
        assert!(matches!(output.data, OutputData::AffectedRows(1)));

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
        assert!(matches!(output.data, OutputData::AffectedRows(0)));

        let request = Request::Ddl(DdlRequest {
            expr: Some(DdlExpr::AlterTable(AlterTableExpr {
                catalog_name: "greptime".to_string(),
                schema_name: "database_created_through_grpc".to_string(),
                table_name: "table_created_through_grpc".to_string(),
                kind: Some(alter_table_expr::Kind::AddColumns(AddColumns {
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
                        add_if_not_exists: false,
                    }],
                })),
            })),
        });
        let output = query(instance, request).await;
        assert!(matches!(output.data, OutputData::AffectedRows(0)));

        let request = Request::Query(QueryRequest {
            query: Some(Query::Sql("INSERT INTO database_created_through_grpc.table_created_through_grpc (a, b, ts) VALUES ('s', 1, 1672816466000)".to_string()))
        });
        let output = query(instance, request).await;
        assert!(matches!(output.data, OutputData::AffectedRows(1)));

        let sql = "SELECT ts, a, b FROM database_created_through_grpc.table_created_through_grpc";
        let expected = "\
+---------------------+---+---+
| ts                  | a | b |
+---------------------+---+---+
| 2023-01-04T07:14:26 | s | 1 |
+---------------------+---+---+";
        query_and_expect(instance, sql, expected).await;

        let request = Request::Ddl(DdlRequest {
            expr: Some(DdlExpr::DropTable(DropTableExpr {
                catalog_name: "greptime".to_string(),
                schema_name: "database_created_through_grpc".to_string(),
                table_name: "table_created_through_grpc".to_string(),
                ..Default::default()
            })),
        });
        let output = query(instance, request).await;
        assert!(matches!(output.data, OutputData::AffectedRows(0)));
    }

    async fn verify_table_is_dropped(instance: &MockDistributedInstance) {
        assert!(
            instance
                .frontend()
                .catalog_manager()
                .table(
                    "greptime",
                    "database_created_through_grpc",
                    "table_created_through_grpc",
                    None,
                )
                .await
                .unwrap()
                .is_none()
        );
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
    c JSON,
    d VECTOR(3),
    ts TIMESTAMP,
    TIME INDEX (ts),
    PRIMARY KEY (a, b, c)
) PARTITION ON COLUMNS(a) (
    a < 10,
    a >= 10 AND a < 20,
    a >= 20 AND a < 50,
    a >= 50
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
        common_telemetry::init_default_ut_logging();
        let standalone = GreptimeDbStandaloneBuilder::new("test_standalone_insert_and_query")
            .build()
            .await;
        let instance = standalone.fe_instance();

        let table_name = "my_table";
        let sql = format!(
            "CREATE TABLE {table_name} (a INT, b STRING, c JSON, ts TIMESTAMP, TIME INDEX (ts), PRIMARY KEY (a, b, c))"
        );
        create_table(instance, sql).await;

        test_insert_delete_and_query_on_existing_table(instance, table_name).await;

        test_insert_delete_and_query_on_auto_created_table(instance).await
    }

    async fn create_table(frontend: &Instance, sql: String) {
        let request = Request::Query(QueryRequest {
            query: Some(Query::Sql(sql)),
        });
        let output = query(frontend, request).await;
        assert!(matches!(output.data, OutputData::AffectedRows(0)));
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
        let json_strings = vec![
            r#"{ "id": 1, "name": "Alice", "age": 30, "active": true }"#.to_string(),
            r#"{ "id": 2, "name": "Bob", "balance": 1234.56, "active": false }"#.to_string(),
            r#"{ "id": 3, "tags": ["rust", "testing", "json"], "age": 28 }"#.to_string(),
            r#"{ "id": 4, "metadata": { "created_at": "2024-10-30T12:00:00Z", "status": "inactive" } }"#.to_string(),
            r#"{ "id": 5, "name": null, "phone": "+1234567890" }"#.to_string(),
            r#"{ "id": 6, "height": 5.9, "weight": 72.5, "active": true }"#.to_string(),
            r#"{ "id": 7, "languages": ["English", "Spanish"], "age": 29 }"#.to_string(),
            r#"{ "id": 8, "contact": { "email": "hank@example.com", "phone": "+0987654321" } }"#.to_string(),
            r#"{ "id": 9, "preferences": { "notifications": true, "theme": "dark" } }"#.to_string(),
            r#"{ "id": 10, "scores": [88, 92, 76], "active": false }"#.to_string(),
            r#"{ "id": 11, "birthday": "1996-07-20", "location": { "city": "New York", "zip": "10001" } }"#.to_string(),
            r#"{ "id": 12, "subscription": { "type": "premium", "expires": "2025-01-01" } }"#.to_string(),
            r#"{ "id": 13, "settings": { "volume": 0.8, "brightness": 0.6 }, "active": true }"#.to_string(),
            r#"{ "id": 14, "notes": ["first note", "second note"], "priority": 1 }"#.to_string(),
            r#"{ "id": 15, "transactions": [{ "amount": 500, "date": "2024-01-01" }, { "amount": -200, "date": "2024-02-01" }] }"#.to_string(),
            r#"{ "id": 16, "transactions": [{ "amount": 500, "date": "2024-01-01" }] }"#.to_string(),
        ];
        let vector_values = [
            [1.0f32, 2.0, 3.0],
            [4.0, 5.0, 6.0],
            [7.0, 8.0, 9.0],
            [10.0, 11.0, 12.0],
            [13.0, 14.0, 15.0],
            [16.0, 17.0, 18.0],
            [19.0, 20.0, 21.0],
            [22.0, 23.0, 24.0],
            [25.0, 26.0, 27.0],
            [28.0, 29.0, 30.0],
            [31.0, 32.0, 33.0],
            [34.0, 35.0, 36.0],
            [37.0, 38.0, 39.0],
            [40.0, 41.0, 42.0],
            [43.0, 44.0, 45.0],
            [46.0, 47.0, 48.0],
        ]
        .iter()
        .map(|x| x.iter().flat_map(|&f| f.to_le_bytes()).collect::<Vec<u8>>())
        .collect::<Vec<_>>();

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
                    column_name: "c".to_string(),
                    values: Some(Values {
                        string_values: json_strings,
                        ..Default::default()
                    }),
                    semantic_type: SemanticType::Tag as i32,
                    datatype: ColumnDataType::Json as i32,
                    ..Default::default()
                },
                Column {
                    column_name: "d".to_string(),
                    values: Some(Values {
                        binary_values: vector_values.clone(),
                        ..Default::default()
                    }),
                    semantic_type: SemanticType::Field as i32,
                    datatype: ColumnDataType::Vector as i32,
                    datatype_extension: Some(ColumnDataTypeExtension {
                        type_ext: Some(TypeExt::VectorType(VectorTypeExtension { dim: 3 })),
                    }),
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
        assert!(matches!(output.data, OutputData::AffectedRows(16)));

        let request = Request::Query(QueryRequest {
            query: Some(Query::Sql(format!(
                "SELECT ts, a, b, json_to_string(c) as c, d FROM {table_name} ORDER BY ts"
            ))),
        });
        let output = query(instance, request.clone()).await;
        let OutputData::Stream(stream) = output.data else {
            unreachable!()
        };
        let recordbatches = RecordBatches::try_collect(stream).await.unwrap();
        let expected = r#"+---------------------+----+-------------------+---------------------------------------------------------------------------------------------------+--------------------------+
| ts                  | a  | b                 | c                                                                                                 | d                        |
+---------------------+----+-------------------+---------------------------------------------------------------------------------------------------+--------------------------+
| 2023-01-01T07:26:12 | 1  | ts: 1672557972000 | {"active":true,"age":30,"id":1,"name":"Alice"}                                                    | 0000803f0000004000004040 |
| 2023-01-01T07:26:13 | 2  | ts: 1672557973000 | {"active":false,"balance":1234.56,"id":2,"name":"Bob"}                                            | 000080400000a0400000c040 |
| 2023-01-01T07:26:14 | 3  | ts: 1672557974000 | {"age":28,"id":3,"tags":["rust","testing","json"]}                                                | 0000e0400000004100001041 |
| 2023-01-01T07:26:15 | 4  | ts: 1672557975000 | {"id":4,"metadata":{"created_at":"2024-10-30T12:00:00Z","status":"inactive"}}                     | 000020410000304100004041 |
| 2023-01-01T07:26:16 | 5  | ts: 1672557976000 | {"id":5,"name":null,"phone":"+1234567890"}                                                        | 000050410000604100007041 |
| 2023-01-01T07:26:17 |    | ts: 1672557977000 | {"active":true,"height":5.9,"id":6,"weight":72.5}                                                 | 000080410000884100009041 |
| 2023-01-01T07:26:18 | 11 | ts: 1672557978000 | {"age":29,"id":7,"languages":["English","Spanish"]}                                               | 000098410000a0410000a841 |
| 2023-01-01T07:26:19 | 12 | ts: 1672557979000 | {"contact":{"email":"hank@example.com","phone":"+0987654321"},"id":8}                             | 0000b0410000b8410000c041 |
| 2023-01-01T07:26:20 | 20 | ts: 1672557980000 | {"id":9,"preferences":{"notifications":true,"theme":"dark"}}                                      | 0000c8410000d0410000d841 |
| 2023-01-01T07:26:21 | 21 | ts: 1672557981000 | {"active":false,"id":10,"scores":[88,92,76]}                                                      | 0000e0410000e8410000f041 |
| 2023-01-01T07:26:22 | 22 | ts: 1672557982000 | {"birthday":"1996-07-20","id":11,"location":{"city":"New York","zip":"10001"}}                    | 0000f8410000004200000442 |
| 2023-01-01T07:26:23 | 23 | ts: 1672557983000 | {"id":12,"subscription":{"expires":"2025-01-01","type":"premium"}}                                | 0000084200000c4200001042 |
| 2023-01-01T07:26:24 | 50 | ts: 1672557984000 | {"active":true,"id":13,"settings":{"brightness":0.6,"volume":0.8}}                                | 000014420000184200001c42 |
| 2023-01-01T07:26:25 | 51 | ts: 1672557985000 | {"id":14,"notes":["first note","second note"],"priority":1}                                       | 000020420000244200002842 |
| 2023-01-01T07:26:26 | 52 | ts: 1672557986000 | {"id":15,"transactions":[{"amount":500,"date":"2024-01-01"},{"amount":-200,"date":"2024-02-01"}]} | 00002c420000304200003442 |
| 2023-01-01T07:26:27 | 53 | ts: 1672557987000 | {"id":16,"transactions":[{"amount":500,"date":"2024-01-01"}]}                                     | 0000384200003c4200004042 |
+---------------------+----+-------------------+---------------------------------------------------------------------------------------------------+--------------------------+"#;
        similar_asserts::assert_eq!(recordbatches.pretty_print().unwrap(), expected);

        // Checks if the encoded vector values are as expected.
        let hex_repr_of_vector_values = vector_values.iter().map(hex::encode).collect::<Vec<_>>();
        assert_eq!(
            hex_repr_of_vector_values,
            vec![
                "0000803f0000004000004040",
                "000080400000a0400000c040",
                "0000e0400000004100001041",
                "000020410000304100004041",
                "000050410000604100007041",
                "000080410000884100009041",
                "000098410000a0410000a841",
                "0000b0410000b8410000c041",
                "0000c8410000d0410000d841",
                "0000e0410000e8410000f041",
                "0000f8410000004200000442",
                "0000084200000c4200001042",
                "000014420000184200001c42",
                "000020420000244200002842",
                "00002c420000304200003442",
                "0000384200003c4200004042",
            ]
        );

        let new_grpc_delete_request = |a, b, c, d, ts, row_count| DeleteRequest {
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
                    column_name: "c".to_string(),
                    values: Some(Values {
                        string_values: c,
                        ..Default::default()
                    }),
                    semantic_type: SemanticType::Tag as i32,
                    datatype: ColumnDataType::Json as i32,
                    ..Default::default()
                },
                Column {
                    column_name: "d".to_string(),
                    values: Some(Values {
                        binary_values: d,
                        ..Default::default()
                    }),
                    semantic_type: SemanticType::Field as i32,
                    datatype: ColumnDataType::Vector as i32,
                    datatype_extension: Some(ColumnDataTypeExtension {
                        type_ext: Some(TypeExt::VectorType(VectorTypeExtension { dim: 3 })),
                    }),
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
            vec![
                r#"{ "id": 2, "name": "Bob", "balance": 1234.56, "active": false }"#.to_string(),
                r#"{ "id": 8, "contact": { "email": "hank@example.com", "phone": "+0987654321" } }"#.to_string(),
                r#"{ "id": 11, "birthday": "1996-07-20", "location": { "city": "New York", "zip": "10001" } }"#.to_string(),
                r#"{ "id": 15, "transactions": [{ "amount": 500, "date": "2024-01-01" }, { "amount": -200, "date": "2024-02-01" }] }"#.to_string(),
            ],
            vec![
                [4.0f32, 5.0, 6.0].iter().flat_map(|f| f.to_le_bytes()).collect::<Vec<u8>>(),
                [22.0f32, 23.0, 24.0].iter().flat_map(|f| f.to_le_bytes()).collect::<Vec<u8>>(),
                [31.0f32, 32.0, 33.0].iter().flat_map(|f| f.to_le_bytes()).collect::<Vec<u8>>(),
                [43.0f32, 44.0, 45.0].iter().flat_map(|f| f.to_le_bytes()).collect::<Vec<u8>>(),
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
            vec![
                r#"{ "id": 3, "tags": ["rust", "testing", "json"], "age": 28 }"#.to_string(),
                r#"{ "id": 16, "transactions": [{ "amount": 500, "date": "2024-01-01" }] }"#
                    .to_string(),
            ],
            vec![
                [7.0f32, 8.0, 9.0]
                    .iter()
                    .flat_map(|f| f.to_le_bytes())
                    .collect::<Vec<u8>>(),
                [46.0f32, 47.0, 48.0]
                    .iter()
                    .flat_map(|f| f.to_le_bytes())
                    .collect::<Vec<u8>>(),
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
        assert!(matches!(output.data, OutputData::AffectedRows(6)));

        let output = query(instance, request).await;
        let OutputData::Stream(stream) = output.data else {
            unreachable!()
        };
        let recordbatches = RecordBatches::try_collect(stream).await.unwrap();
        let expected = r#"+---------------------+----+-------------------+-------------------------------------------------------------------------------+--------------------------+
| ts                  | a  | b                 | c                                                                             | d                        |
+---------------------+----+-------------------+-------------------------------------------------------------------------------+--------------------------+
| 2023-01-01T07:26:12 | 1  | ts: 1672557972000 | {"active":true,"age":30,"id":1,"name":"Alice"}                                | 0000803f0000004000004040 |
| 2023-01-01T07:26:15 | 4  | ts: 1672557975000 | {"id":4,"metadata":{"created_at":"2024-10-30T12:00:00Z","status":"inactive"}} | 000020410000304100004041 |
| 2023-01-01T07:26:16 | 5  | ts: 1672557976000 | {"id":5,"name":null,"phone":"+1234567890"}                                    | 000050410000604100007041 |
| 2023-01-01T07:26:17 |    | ts: 1672557977000 | {"active":true,"height":5.9,"id":6,"weight":72.5}                             | 000080410000884100009041 |
| 2023-01-01T07:26:18 | 11 | ts: 1672557978000 | {"age":29,"id":7,"languages":["English","Spanish"]}                           | 000098410000a0410000a841 |
| 2023-01-01T07:26:20 | 20 | ts: 1672557980000 | {"id":9,"preferences":{"notifications":true,"theme":"dark"}}                  | 0000c8410000d0410000d841 |
| 2023-01-01T07:26:21 | 21 | ts: 1672557981000 | {"active":false,"id":10,"scores":[88,92,76]}                                  | 0000e0410000e8410000f041 |
| 2023-01-01T07:26:23 | 23 | ts: 1672557983000 | {"id":12,"subscription":{"expires":"2025-01-01","type":"premium"}}            | 0000084200000c4200001042 |
| 2023-01-01T07:26:24 | 50 | ts: 1672557984000 | {"active":true,"id":13,"settings":{"brightness":0.6,"volume":0.8}}            | 000014420000184200001c42 |
| 2023-01-01T07:26:25 | 51 | ts: 1672557985000 | {"id":14,"notes":["first note","second note"],"priority":1}                   | 000020420000244200002842 |
+---------------------+----+-------------------+-------------------------------------------------------------------------------+--------------------------+"#;
        similar_asserts::assert_eq!(recordbatches.pretty_print().unwrap(), expected);
    }

    async fn verify_data_distribution(
        instance: &MockDistributedInstance,
        table_name: &str,
        expected_distribution: HashMap<u32, &str>,
    ) {
        let table = instance
            .frontend()
            .catalog_manager()
            .table("greptime", "public", table_name, None)
            .await
            .unwrap()
            .unwrap();
        let table_id = table.table_info().ident.table_id;
        let table_route_value = instance
            .table_metadata_manager()
            .table_route_manager()
            .table_route_storage()
            .get(table_id)
            .await
            .unwrap()
            .unwrap();

        let region_to_dn_map = region_distribution(
            table_route_value
                .region_routes()
                .expect("physical table route"),
        )
        .iter()
        .map(|(k, v)| (v.leader_regions[0], *k))
        .collect::<HashMap<u32, u64>>();
        assert!(region_to_dn_map.len() <= instance.datanodes().len());

        let stmt = QueryLanguageParser::parse_sql(
            &format!("SELECT ts, a, b FROM {table_name} ORDER BY ts"),
            &QueryContext::arc(),
        )
        .unwrap();
        let plan = instance
            .frontend()
            .statement_executor()
            .plan(&stmt, QueryContext::arc())
            .await
            .unwrap();
        let plan = DFLogicalSubstraitConvertor
            .encode(&plan, DefaultSerializer)
            .unwrap();

        for (region, dn) in region_to_dn_map.iter() {
            let region_server = instance.datanodes().get(dn).unwrap().region_server();

            let region_id = RegionId::new(table_id, *region);

            let stream = region_server
                .handle_remote_read(
                    RegionQueryRequest {
                        region_id: region_id.as_u64(),
                        plan: plan.to_vec(),
                        ..Default::default()
                    },
                    QueryContext::arc(),
                )
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
                    column_name: "c".to_string(),
                    values: Some(Values {
                        string_values: vec![
                            r#"{ "id": 1, "name": "Alice", "age": 30, "active": true }"#
                                .to_string(),
                            r#"{ "id": 2, "name": "Bob", "balance": 1234.56, "active": false }"#
                                .to_string(),
                        ],
                        ..Default::default()
                    }),
                    null_mask: vec![2],
                    semantic_type: SemanticType::Field as i32,
                    datatype: ColumnDataType::Json as i32,
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
        assert!(matches!(output.data, OutputData::AffectedRows(3)));

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
        assert!(matches!(output.data, OutputData::AffectedRows(3)));

        let request = Request::Query(QueryRequest {
            query: Some(Query::Sql(
                "SELECT ts, a, b, json_to_string(c) as c FROM auto_created_table order by ts"
                    .to_string(),
            )),
        });
        let output = query(instance, request.clone()).await;
        let OutputData::Stream(stream) = output.data else {
            unreachable!()
        };
        let recordbatches = RecordBatches::try_collect(stream).await.unwrap();
        let expected = r#"+---------------------+---+---+--------------------------------------------------------+
| ts                  | a | b | c                                                      |
+---------------------+---+---+--------------------------------------------------------+
| 2023-01-01T07:26:15 | 4 |   | {"active":true,"age":30,"id":1,"name":"Alice"}         |
| 2023-01-01T07:26:16 |   |   |                                                        |
| 2023-01-01T07:26:17 | 6 |   | {"active":false,"balance":1234.56,"id":2,"name":"Bob"} |
| 2023-01-01T07:26:18 |   | x |                                                        |
| 2023-01-01T07:26:19 |   |   |                                                        |
| 2023-01-01T07:26:20 |   | z |                                                        |
+---------------------+---+---+--------------------------------------------------------+"#;
        similar_asserts::assert_eq!(recordbatches.pretty_print().unwrap(), expected);

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
        assert!(matches!(output.data, OutputData::AffectedRows(2)));

        let output = query(instance, request).await;
        let OutputData::Stream(stream) = output.data else {
            unreachable!()
        };
        let recordbatches = RecordBatches::try_collect(stream).await.unwrap();
        let expected = r#"+---------------------+---+---+--------------------------------------------------------+
| ts                  | a | b | c                                                      |
+---------------------+---+---+--------------------------------------------------------+
| 2023-01-01T07:26:16 |   |   |                                                        |
| 2023-01-01T07:26:17 | 6 |   | {"active":false,"balance":1234.56,"id":2,"name":"Bob"} |
| 2023-01-01T07:26:18 |   | x |                                                        |
| 2023-01-01T07:26:20 |   | z |                                                        |
+---------------------+---+---+--------------------------------------------------------+"#;
        similar_asserts::assert_eq!(recordbatches.pretty_print().unwrap(), expected);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_promql_query() {
        let standalone = GreptimeDbStandaloneBuilder::new("test_standalone_promql_query")
            .build()
            .await;
        let instance = standalone.fe_instance();

        let table_name = "my_table";
        let sql = format!(
            "CREATE TABLE {table_name} (h string, a double, ts TIMESTAMP, TIME INDEX (ts), PRIMARY KEY(h))"
        );
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
        assert!(matches!(output.data, OutputData::AffectedRows(8)));

        let request = Request::Query(QueryRequest {
            query: Some(Query::PromRangeQuery(api::v1::PromRangeQuery {
                query: "my_table".to_owned(),
                start: "1672557973".to_owned(),
                end: "1672557978".to_owned(),
                step: "1s".to_owned(),
                lookback: "5m".to_string(),
            })),
        });
        let output = query(instance, request).await;
        let OutputData::Stream(stream) = output.data else {
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

    #[apply(both_instances_cases)]
    async fn test_extra_external_table_options(instance: Arc<dyn MockInstance>) {
        common_telemetry::init_default_ut_logging();
        let frontend = instance.frontend();
        let instance = frontend.as_ref();

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
                    column_name: "c".to_string(),
                    values: Some(Values {
                        string_values: vec![
                            r#"{ "id": 1, "name": "Alice", "age": 30, "active": true }"#
                                .to_string(),
                            r#"{ "id": 2, "name": "Bob", "balance": 1234.56, "active": false }"#
                                .to_string(),
                        ],
                        ..Default::default()
                    }),
                    null_mask: vec![2],
                    semantic_type: SemanticType::Field as i32,
                    datatype: ColumnDataType::Json as i32,
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
        let request = Request::Inserts(InsertRequests {
            inserts: vec![insert],
        });

        let ctx = Arc::new(
            QueryContextBuilder::default()
                .set_extension(TWCS_TIME_WINDOW.to_string(), "1d".to_string())
                .build(),
        );
        let output = GrpcQueryHandler::do_query(instance, request, ctx)
            .await
            .unwrap();
        assert!(matches!(output.data, OutputData::AffectedRows(3)));

        let output = execute_sql(&frontend, "show create table auto_created_table").await;

        let expected = r#"+--------------------+---------------------------------------------------+
| Table              | Create Table                                      |
+--------------------+---------------------------------------------------+
| auto_created_table | CREATE TABLE IF NOT EXISTS "auto_created_table" ( |
|                    |   "a" INT NULL,                                   |
|                    |   "c" JSON NULL,                                  |
|                    |   "ts" TIMESTAMP(3) NOT NULL,                     |
|                    |   TIME INDEX ("ts")                               |
|                    | )                                                 |
|                    |                                                   |
|                    | ENGINE=mito                                       |
|                    | WITH(                                             |
|                    |   'comment' = 'Created on insertion',             |
|                    |   'compaction.twcs.time_window' = '1d',           |
|                    |   'compaction.type' = 'twcs'                      |
|                    | )                                                 |
+--------------------+---------------------------------------------------+"#;
        check_output_stream(output.data, expected).await;
    }
}
