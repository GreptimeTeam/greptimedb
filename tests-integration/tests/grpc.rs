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

use api::v1::alter_expr::Kind;
use api::v1::promql_request::Promql;
use api::v1::{
    column, AddColumn, AddColumns, AlterExpr, Basic, Column, ColumnDataType, ColumnDef,
    CreateTableExpr, InsertRequest, InsertRequests, PromInstantQuery, PromRangeQuery,
    PromqlRequest, RequestHeader, SemanticType,
};
use auth::user_provider_from_option;
use client::{Client, Database, OutputData, DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
use common_catalog::consts::MITO_ENGINE;
use common_grpc::channel_manager::ClientTlsOption;
use common_query::Output;
use common_recordbatch::RecordBatches;
use common_runtime::Runtime;
use common_test_util::find_workspace_path;
use servers::grpc::builder::GrpcServerBuilder;
use servers::grpc::GrpcServerConfig;
use servers::http::prometheus::{
    PromData, PromQueryResult, PromSeriesMatrix, PromSeriesVector, PrometheusJsonResponse,
    PrometheusResponse,
};
use servers::server::Server;
use servers::tls::{TlsMode, TlsOption};
use tests_integration::test_util::{
    setup_grpc_server, setup_grpc_server_with, setup_grpc_server_with_user_provider, StorageType,
};

#[macro_export]
macro_rules! grpc_test {
    ($service:ident, $($(#[$meta:meta])* $test:ident),*,) => {
        paste::item! {
            mod [<integration_grpc_ $service:lower _test>] {
                $(
                    #[tokio::test(flavor = "multi_thread")]
                    $(
                        #[$meta]
                    )*
                    async fn [< $test >]() {
                        let store_type = tests_integration::test_util::StorageType::$service;
                        if store_type.test_on() {
                            let _ = $crate::grpc::$test(store_type).await;
                        }

                    }
                )*
            }
        }
    };
}

#[macro_export]
macro_rules! grpc_tests {
    ($($service:ident),*) => {
        $(
            grpc_test!(
                $service,

                test_invalid_dbname,
                test_auto_create_table,
                test_auto_create_table_with_hints,
                test_insert_and_select,
                test_dbname,
                test_grpc_message_size_ok,
                test_grpc_zstd_compression,
                test_grpc_message_size_limit_recv,
                test_grpc_message_size_limit_send,
                test_grpc_auth,
                test_health_check,
                test_prom_gateway_query,
                test_grpc_timezone,
                test_grpc_tls_config,
            );
        )*
    };
}

pub async fn test_invalid_dbname(store_type: StorageType) {
    let (addr, mut guard, fe_grpc_server) =
        setup_grpc_server(store_type, "auto_create_table").await;

    let grpc_client = Client::with_urls(vec![addr]);
    let db = Database::new_with_dbname("tom", grpc_client);

    let (expected_host_col, expected_cpu_col, expected_mem_col, expected_ts_col) = expect_data();
    let request = InsertRequest {
        table_name: "demo".to_string(),
        columns: vec![
            expected_host_col.clone(),
            expected_cpu_col.clone(),
            expected_mem_col.clone(),
            expected_ts_col.clone(),
        ],
        row_count: 4,
    };
    let result = db
        .insert(InsertRequests {
            inserts: vec![request],
        })
        .await;
    assert!(result.is_err());

    let _ = fe_grpc_server.shutdown().await;
    guard.remove_all().await;
}

pub async fn test_dbname(store_type: StorageType) {
    let (addr, mut guard, fe_grpc_server) =
        setup_grpc_server(store_type, "auto_create_table").await;

    let grpc_client = Client::with_urls(vec![addr]);
    let db = Database::new_with_dbname(
        format!("{}-{}", DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME),
        grpc_client,
    );
    insert_and_assert(&db).await;
    let _ = fe_grpc_server.shutdown().await;
    guard.remove_all().await;
}

pub async fn test_grpc_message_size_ok(store_type: StorageType) {
    let config = GrpcServerConfig {
        max_recv_message_size: 1024,
        max_send_message_size: 1024,
        ..Default::default()
    };
    let (addr, mut guard, fe_grpc_server) =
        setup_grpc_server_with(store_type, "auto_create_table", None, Some(config)).await;

    let grpc_client = Client::with_urls(vec![addr]);
    let db = Database::new_with_dbname(
        format!("{}-{}", DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME),
        grpc_client,
    );
    db.sql("show tables;").await.unwrap();
    let _ = fe_grpc_server.shutdown().await;
    guard.remove_all().await;
}

pub async fn test_grpc_zstd_compression(store_type: StorageType) {
    // server and client both support gzip
    let config = GrpcServerConfig {
        max_recv_message_size: 1024,
        max_send_message_size: 1024,
        ..Default::default()
    };
    let (addr, mut guard, fe_grpc_server) =
        setup_grpc_server_with(store_type, "auto_create_table", None, Some(config)).await;

    let grpc_client = Client::with_urls(vec![addr]);
    let db = Database::new_with_dbname(
        format!("{}-{}", DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME),
        grpc_client,
    );
    db.sql("show tables;").await.unwrap();
    let _ = fe_grpc_server.shutdown().await;
    guard.remove_all().await;
}

pub async fn test_grpc_message_size_limit_send(store_type: StorageType) {
    let config = GrpcServerConfig {
        max_recv_message_size: 1024,
        max_send_message_size: 50,
        ..Default::default()
    };
    let (addr, mut guard, fe_grpc_server) =
        setup_grpc_server_with(store_type, "auto_create_table", None, Some(config)).await;

    let grpc_client = Client::with_urls(vec![addr]);
    let db = Database::new_with_dbname(
        format!("{}-{}", DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME),
        grpc_client,
    );
    let err_msg = db.sql("show tables;").await.unwrap_err().to_string();
    assert!(err_msg.contains("message length too large"), "{}", err_msg);
    let _ = fe_grpc_server.shutdown().await;
    guard.remove_all().await;
}

pub async fn test_grpc_message_size_limit_recv(store_type: StorageType) {
    let config = GrpcServerConfig {
        max_recv_message_size: 10,
        max_send_message_size: 1024,
        ..Default::default()
    };
    let (addr, mut guard, fe_grpc_server) =
        setup_grpc_server_with(store_type, "auto_create_table", None, Some(config)).await;

    let grpc_client = Client::with_urls(vec![addr]);
    let db = Database::new_with_dbname(
        format!("{}-{}", DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME),
        grpc_client,
    );
    let err_msg = db.sql("show tables;").await.unwrap_err().to_string();
    assert!(
        err_msg.contains("Operation was attempted past the valid range"),
        "{}",
        err_msg
    );
    let _ = fe_grpc_server.shutdown().await;
    guard.remove_all().await;
}

pub async fn test_grpc_auth(store_type: StorageType) {
    let user_provider = user_provider_from_option(
        &"static_user_provider:cmd:greptime_user=greptime_pwd".to_string(),
    )
    .unwrap();
    let (addr, mut guard, fe_grpc_server) =
        setup_grpc_server_with_user_provider(store_type, "auto_create_table", Some(user_provider))
            .await;

    let grpc_client = Client::with_urls(vec![addr]);
    let mut db = Database::new_with_dbname(
        format!("{}-{}", DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME),
        grpc_client,
    );

    // 1. test without auth
    let re = db.sql("show tables;").await;
    assert!(re.is_err());
    assert!(matches!(
        re,
        Err(client::Error::FlightGet {
            tonic_code: tonic::Code::Unauthenticated,
            ..
        })
    ));

    // 2. test wrong auth
    db.set_auth(api::v1::auth_header::AuthScheme::Basic(Basic {
        username: "greptime_user".to_string(),
        password: "wrong_pwd".to_string(),
    }));
    let re = db.sql("show tables;").await;
    assert!(re.is_err());
    assert!(matches!(
        re,
        Err(client::Error::FlightGet {
            tonic_code: tonic::Code::Unauthenticated,
            ..
        })
    ));

    // 3. test right auth
    db.set_auth(api::v1::auth_header::AuthScheme::Basic(Basic {
        username: "greptime_user".to_string(),
        password: "greptime_pwd".to_string(),
    }));
    let re = db.sql("show tables;").await;
    assert!(re.is_ok());

    let _ = fe_grpc_server.shutdown().await;
    guard.remove_all().await;
}

pub async fn test_auto_create_table(store_type: StorageType) {
    let (addr, mut guard, fe_grpc_server) =
        setup_grpc_server(store_type, "auto_create_table").await;

    let grpc_client = Client::with_urls(vec![addr]);
    let db = Database::new(DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, grpc_client);
    insert_and_assert(&db).await;
    let _ = fe_grpc_server.shutdown().await;
    guard.remove_all().await;
}

pub async fn test_auto_create_table_with_hints(store_type: StorageType) {
    let (addr, mut guard, fe_grpc_server) =
        setup_grpc_server(store_type, "auto_create_table_with_hints").await;

    let grpc_client = Client::with_urls(vec![addr]);
    let db = Database::new(DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, grpc_client);
    insert_with_hints_and_assert(&db).await;
    let _ = fe_grpc_server.shutdown().await;
    guard.remove_all().await;
}

fn expect_data() -> (Column, Column, Column, Column) {
    // testing data:
    let expected_host_col = Column {
        column_name: "host".to_string(),
        values: Some(column::Values {
            string_values: vec!["host1", "host2", "host3", "host4"]
                .into_iter()
                .map(|s| s.to_string())
                .collect(),
            ..Default::default()
        }),
        semantic_type: SemanticType::Tag as i32,
        datatype: ColumnDataType::String as i32,
        ..Default::default()
    };
    let expected_cpu_col = Column {
        column_name: "cpu".to_string(),
        values: Some(column::Values {
            f64_values: vec![0.31, 0.41, 0.2],
            ..Default::default()
        }),
        null_mask: vec![2],
        semantic_type: SemanticType::Field as i32,
        datatype: ColumnDataType::Float64 as i32,
        ..Default::default()
    };
    let expected_mem_col = Column {
        column_name: "memory".to_string(),
        values: Some(column::Values {
            f64_values: vec![0.1, 0.2, 0.3],
            ..Default::default()
        }),
        null_mask: vec![4],
        semantic_type: SemanticType::Field as i32,
        datatype: ColumnDataType::Float64 as i32,
        ..Default::default()
    };
    let expected_ts_col = Column {
        column_name: "ts".to_string(),
        values: Some(column::Values {
            timestamp_millisecond_values: vec![100, 101, 102, 103],
            ..Default::default()
        }),
        semantic_type: SemanticType::Timestamp as i32,
        datatype: ColumnDataType::TimestampMillisecond as i32,
        ..Default::default()
    };

    (
        expected_host_col,
        expected_cpu_col,
        expected_mem_col,
        expected_ts_col,
    )
}

pub async fn test_insert_and_select(store_type: StorageType) {
    common_telemetry::init_default_ut_logging();
    let (addr, mut guard, fe_grpc_server) =
        setup_grpc_server(store_type, "insert_and_select").await;

    let grpc_client = Client::with_urls(vec![addr]);
    let db = Database::new(DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, grpc_client);

    // create
    let expr = testing_create_expr();
    let result = db.create(expr).await.unwrap();
    assert!(matches!(result.data, OutputData::AffectedRows(0)));

    //alter
    let add_column = ColumnDef {
        name: "test_column".to_string(),
        data_type: ColumnDataType::Int64.into(),
        is_nullable: true,
        default_constraint: vec![],
        semantic_type: SemanticType::Field as i32,
        ..Default::default()
    };
    let kind = Kind::AddColumns(AddColumns {
        add_columns: vec![AddColumn {
            column_def: Some(add_column),
            location: None,
        }],
    });
    let expr = AlterExpr {
        catalog_name: DEFAULT_CATALOG_NAME.to_string(),
        schema_name: DEFAULT_SCHEMA_NAME.to_string(),
        table_name: "demo".to_string(),
        kind: Some(kind),
    };
    let result = db.alter(expr).await.unwrap();
    assert!(matches!(result.data, OutputData::AffectedRows(0)));

    // insert
    insert_and_assert(&db).await;

    let _ = fe_grpc_server.shutdown().await;
    guard.remove_all().await;
}

async fn insert_with_hints_and_assert(db: &Database) {
    // testing data:
    let (expected_host_col, expected_cpu_col, expected_mem_col, expected_ts_col) = expect_data();

    let request = InsertRequest {
        table_name: "demo".to_string(),
        columns: vec![
            expected_host_col.clone(),
            expected_cpu_col.clone(),
            expected_mem_col.clone(),
            expected_ts_col.clone(),
        ],
        row_count: 4,
    };
    let result = db
        .insert_with_hints(
            InsertRequests {
                inserts: vec![request],
            },
            &[("append_mode", "true")],
        )
        .await;
    assert_eq!(result.unwrap(), 4);

    // show table
    let output = db.sql("SHOW CREATE TABLE demo;").await.unwrap();

    let record_batches = match output.data {
        OutputData::RecordBatches(record_batches) => record_batches,
        OutputData::Stream(stream) => RecordBatches::try_collect(stream).await.unwrap(),
        OutputData::AffectedRows(_) => unreachable!(),
    };

    let pretty = record_batches.pretty_print().unwrap();
    let expected = "\
+-------+-------------------------------------+
| Table | Create Table                        |
+-------+-------------------------------------+
| demo  | CREATE TABLE IF NOT EXISTS \"demo\" ( |
|       |   \"host\" STRING NULL,               |
|       |   \"cpu\" DOUBLE NULL,                |
|       |   \"memory\" DOUBLE NULL,             |
|       |   \"ts\" TIMESTAMP(3) NOT NULL,       |
|       |   TIME INDEX (\"ts\"),                |
|       |   PRIMARY KEY (\"host\")              |
|       | )                                   |
|       |                                     |
|       | ENGINE=mito                         |
|       | WITH(                               |
|       |   append_mode = 'true'              |
|       | )                                   |
+-------+-------------------------------------+\
";
    assert_eq!(pretty, expected);
}

async fn insert_and_assert(db: &Database) {
    // testing data:
    let (expected_host_col, expected_cpu_col, expected_mem_col, expected_ts_col) = expect_data();

    let request = InsertRequest {
        table_name: "demo".to_string(),
        columns: vec![
            expected_host_col.clone(),
            expected_cpu_col.clone(),
            expected_mem_col.clone(),
            expected_ts_col.clone(),
        ],
        row_count: 4,
    };
    let result = db
        .insert(InsertRequests {
            inserts: vec![request],
        })
        .await;
    assert_eq!(result.unwrap(), 4);

    let result = db
        .sql(
            "INSERT INTO demo(host, cpu, memory, ts) VALUES \
            ('host5', 66.6, 1024, 1672201027000),\
            ('host6', 88.8, 333.3, 1672201028000)",
        )
        .await
        .unwrap();
    assert!(matches!(result.data, OutputData::AffectedRows(2)));

    // select
    let output = db
        .sql("SELECT host, cpu, memory, ts FROM demo")
        .await
        .unwrap();

    let record_batches = match output.data {
        OutputData::RecordBatches(record_batches) => record_batches,
        OutputData::Stream(stream) => RecordBatches::try_collect(stream).await.unwrap(),
        OutputData::AffectedRows(_) => unreachable!(),
    };

    let pretty = record_batches.pretty_print().unwrap();
    let expected = "\
+-------+------+--------+-------------------------+
| host  | cpu  | memory | ts                      |
+-------+------+--------+-------------------------+
| host1 | 0.31 | 0.1    | 1970-01-01T00:00:00.100 |
| host2 |      | 0.2    | 1970-01-01T00:00:00.101 |
| host3 | 0.41 |        | 1970-01-01T00:00:00.102 |
| host4 | 0.2  | 0.3    | 1970-01-01T00:00:00.103 |
| host5 | 66.6 | 1024.0 | 2022-12-28T04:17:07     |
| host6 | 88.8 | 333.3  | 2022-12-28T04:17:08     |
+-------+------+--------+-------------------------+\
";
    assert_eq!(pretty, expected);
}

fn testing_create_expr() -> CreateTableExpr {
    let column_defs = vec![
        ColumnDef {
            name: "host".to_string(),
            data_type: ColumnDataType::String as i32,
            is_nullable: false,
            default_constraint: vec![],
            semantic_type: SemanticType::Tag as i32,
            ..Default::default()
        },
        ColumnDef {
            name: "cpu".to_string(),
            data_type: ColumnDataType::Float64 as i32,
            is_nullable: true,
            default_constraint: vec![],
            semantic_type: SemanticType::Field as i32,
            ..Default::default()
        },
        ColumnDef {
            name: "memory".to_string(),
            data_type: ColumnDataType::Float64 as i32,
            is_nullable: true,
            default_constraint: vec![],
            semantic_type: SemanticType::Field as i32,
            ..Default::default()
        },
        ColumnDef {
            name: "ts".to_string(),
            data_type: ColumnDataType::TimestampMillisecond as i32, // timestamp
            is_nullable: false,
            default_constraint: vec![],
            semantic_type: SemanticType::Timestamp as i32,
            ..Default::default()
        },
    ];
    CreateTableExpr {
        catalog_name: "greptime".to_string(),
        schema_name: "public".to_string(),
        table_name: "demo".to_string(),
        desc: "blabla little magic fairy".to_string(),
        column_defs,
        time_index: "ts".to_string(),
        primary_keys: vec!["host".to_string()],
        create_if_not_exists: true,
        table_options: Default::default(),
        table_id: None,
        engine: MITO_ENGINE.to_string(),
    }
}

pub async fn test_health_check(store_type: StorageType) {
    let (addr, mut guard, fe_grpc_server) =
        setup_grpc_server(store_type, "auto_create_table").await;

    let grpc_client = Client::with_urls(vec![addr]);
    grpc_client.health_check().await.unwrap();

    let _ = fe_grpc_server.shutdown().await;
    guard.remove_all().await;
}

pub async fn test_prom_gateway_query(store_type: StorageType) {
    common_telemetry::init_default_ut_logging();

    // prepare connection
    let (addr, mut guard, fe_grpc_server) = setup_grpc_server(store_type, "prom_gateway").await;
    let grpc_client = Client::with_urls(vec![addr]);
    let db = Database::new(
        DEFAULT_CATALOG_NAME,
        DEFAULT_SCHEMA_NAME,
        grpc_client.clone(),
    );
    let mut gateway_client = grpc_client.make_prometheus_gateway_client().unwrap();

    // create table and insert data
    assert!(matches!(
        db.sql("CREATE TABLE test(i DOUBLE, j TIMESTAMP TIME INDEX, k STRING PRIMARY KEY);")
            .await
            .unwrap()
            .data,
        OutputData::AffectedRows(0)
    ));
    assert!(matches!(
        db.sql(r#"INSERT INTO test VALUES (1, 1, "a"), (1, 1, "b"), (2, 2, "a");"#)
            .await
            .unwrap()
            .data,
        OutputData::AffectedRows(3)
    ));

    // Instant query using prometheus gateway service
    let header = RequestHeader {
        dbname: "public".to_string(),
        ..Default::default()
    };
    let instant_query = PromInstantQuery {
        query: "test".to_string(),
        time: "5".to_string(),
        lookback: "5m".to_string(),
    };
    let instant_query_request = PromqlRequest {
        header: Some(header.clone()),
        promql: Some(Promql::InstantQuery(instant_query)),
    };
    let json_bytes = gateway_client
        .handle(instant_query_request)
        .await
        .unwrap()
        .into_inner()
        .body;
    let instant_query_result =
        serde_json::from_slice::<PrometheusJsonResponse>(&json_bytes).unwrap();
    let expected = PrometheusJsonResponse {
        status: "success".to_string(),
        data: PrometheusResponse::PromData(PromData {
            result_type: "vector".to_string(),
            result: PromQueryResult::Vector(vec![
                PromSeriesVector {
                    metric: [
                        ("k".to_string(), "a".to_string()),
                        ("__name__".to_string(), "test".to_string()),
                    ]
                    .into_iter()
                    .collect(),
                    value: Some((5.0, "2".to_string())),
                },
                PromSeriesVector {
                    metric: [
                        ("__name__".to_string(), "test".to_string()),
                        ("k".to_string(), "b".to_string()),
                    ]
                    .into_iter()
                    .collect(),
                    value: Some((5.0, "1".to_string())),
                },
            ]),
        }),
        error: None,
        error_type: None,
        warnings: None,
        resp_metrics: Default::default(),
        status_code: None,
    };
    assert_eq!(instant_query_result, expected);

    // Range query using prometheus gateway service
    let range_query = PromRangeQuery {
        query: "test".to_string(),
        start: "0".to_string(),
        end: "10".to_string(),
        step: "5s".to_string(),
        lookback: "5m".to_string(),
    };
    let range_query_request: PromqlRequest = PromqlRequest {
        header: Some(header.clone()),
        promql: Some(Promql::RangeQuery(range_query)),
    };
    let json_bytes = gateway_client
        .handle(range_query_request)
        .await
        .unwrap()
        .into_inner()
        .body;
    let range_query_result = serde_json::from_slice::<PrometheusJsonResponse>(&json_bytes).unwrap();
    let expected = PrometheusJsonResponse {
        status: "success".to_string(),
        data: PrometheusResponse::PromData(PromData {
            result_type: "matrix".to_string(),
            result: PromQueryResult::Matrix(vec![
                PromSeriesMatrix {
                    metric: [
                        ("__name__".to_string(), "test".to_string()),
                        ("k".to_string(), "a".to_string()),
                    ]
                    .into_iter()
                    .collect(),
                    values: vec![(5.0, "2".to_string()), (10.0, "2".to_string())],
                },
                PromSeriesMatrix {
                    metric: [
                        ("__name__".to_string(), "test".to_string()),
                        ("k".to_string(), "b".to_string()),
                    ]
                    .into_iter()
                    .collect(),
                    values: vec![(5.0, "1".to_string()), (10.0, "1".to_string())],
                },
            ]),
        }),
        error: None,
        error_type: None,
        warnings: None,
        resp_metrics: Default::default(),
        status_code: None,
    };
    assert_eq!(range_query_result, expected);

    // query nonexistent data
    let range_query = PromRangeQuery {
        query: "test".to_string(),
        start: "1000000000".to_string(),
        end: "1000001000".to_string(),
        step: "5s".to_string(),
        lookback: "5m".to_string(),
    };
    let range_query_request: PromqlRequest = PromqlRequest {
        header: Some(header),
        promql: Some(Promql::RangeQuery(range_query)),
    };
    let json_bytes = gateway_client
        .handle(range_query_request)
        .await
        .unwrap()
        .into_inner()
        .body;
    let range_query_result = serde_json::from_slice::<PrometheusJsonResponse>(&json_bytes).unwrap();
    let expected = PrometheusJsonResponse {
        status: "success".to_string(),
        data: PrometheusResponse::PromData(PromData {
            result_type: "matrix".to_string(),
            result: PromQueryResult::Matrix(vec![]),
        }),
        error: None,
        error_type: None,
        warnings: None,
        resp_metrics: Default::default(),
        status_code: None,
    };
    assert_eq!(range_query_result, expected);

    // clean up
    let _ = fe_grpc_server.shutdown().await;
    guard.remove_all().await;
}

pub async fn test_grpc_timezone(store_type: StorageType) {
    let config = GrpcServerConfig {
        max_recv_message_size: 1024,
        max_send_message_size: 1024,
        ..Default::default()
    };
    let (addr, mut guard, fe_grpc_server) =
        setup_grpc_server_with(store_type, "auto_create_table", None, Some(config)).await;

    let grpc_client = Client::with_urls(vec![addr]);
    let mut db = Database::new_with_dbname(
        format!("{}-{}", DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME),
        grpc_client,
    );
    db.set_timezone("Asia/Shanghai");
    let sys1 = to_batch(db.sql("show variables system_time_zone;").await.unwrap()).await;
    let user1 = to_batch(db.sql("show variables time_zone;").await.unwrap()).await;
    db.set_timezone("");
    let sys2 = to_batch(db.sql("show variables system_time_zone;").await.unwrap()).await;
    let user2 = to_batch(db.sql("show variables time_zone;").await.unwrap()).await;
    assert_eq!(sys1, sys2);
    assert_eq!(
        sys2,
        "\
+------------------+
| SYSTEM_TIME_ZONE |
+------------------+
| UTC              |
+------------------+"
    );
    assert_eq!(
        user1,
        "\
+---------------+
| TIME_ZONE     |
+---------------+
| Asia/Shanghai |
+---------------+"
    );
    assert_eq!(
        user2,
        "\
+-----------+
| TIME_ZONE |
+-----------+
| UTC       |
+-----------+"
    );
    let _ = fe_grpc_server.shutdown().await;
    guard.remove_all().await;
}

async fn to_batch(output: Output) -> String {
    match output.data {
        OutputData::RecordBatches(batch) => batch,
        OutputData::Stream(stream) => RecordBatches::try_collect(stream).await.unwrap(),
        OutputData::AffectedRows(_) => unreachable!(),
    }
    .pretty_print()
    .unwrap()
}

pub async fn test_grpc_tls_config(store_type: StorageType) {
    let comm_dir = find_workspace_path("/src/common/grpc/tests/tls");
    let ca_path = comm_dir.join("ca.pem").to_str().unwrap().to_string();
    let server_cert_path = comm_dir.join("server.pem").to_str().unwrap().to_string();
    let server_key_path = comm_dir.join("server.key").to_str().unwrap().to_string();
    let client_cert_path = comm_dir.join("client.pem").to_str().unwrap().to_string();
    let client_key_path = comm_dir.join("client.key").to_str().unwrap().to_string();
    let client_corrupted = comm_dir.join("corrupted").to_str().unwrap().to_string();

    let tls = TlsOption::new(
        Some(TlsMode::Require),
        Some(server_cert_path),
        Some(server_key_path),
    );
    let config = GrpcServerConfig {
        max_recv_message_size: 1024,
        max_send_message_size: 1024,
        tls,
    };
    let (addr, mut guard, fe_grpc_server) =
        setup_grpc_server_with(store_type, "tls_create_table", None, Some(config)).await;

    let mut client_tls = ClientTlsOption {
        server_ca_cert_path: ca_path,
        client_cert_path,
        client_key_path,
    };
    {
        let grpc_client =
            Client::with_tls_and_urls(vec![addr.clone()], client_tls.clone()).unwrap();
        let db = Database::new_with_dbname(
            format!("{}-{}", DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME),
            grpc_client,
        );
        db.sql("show tables;").await.unwrap();
    }
    // test corrupted client key
    {
        client_tls.client_key_path = client_corrupted;
        let grpc_client = Client::with_tls_and_urls(vec![addr], client_tls.clone()).unwrap();
        let db = Database::new_with_dbname(
            format!("{}-{}", DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME),
            grpc_client,
        );
        let re = db.sql("show tables;").await;
        assert!(re.is_err());
    }
    // test grpc unsupported tls watch
    {
        let tls = TlsOption {
            watch: true,
            ..Default::default()
        };
        let config = GrpcServerConfig {
            max_recv_message_size: 1024,
            max_send_message_size: 1024,
            tls,
        };
        let runtime = Runtime::builder().build().unwrap();
        let grpc_builder =
            GrpcServerBuilder::new(config.clone(), runtime).with_tls_config(config.tls);
        assert!(grpc_builder.is_err());
    }

    let _ = fe_grpc_server.shutdown().await;
    guard.remove_all().await;
}
