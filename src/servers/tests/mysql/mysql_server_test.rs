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

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use common_recordbatch::RecordBatch;
use common_runtime::Builder as RuntimeBuilder;
use datatypes::schema::Schema;
use mysql_async::prelude::*;
use rand::rngs::StdRng;
use rand::Rng;
use servers::error::Result;
use servers::mysql::server::MysqlServer;
use servers::server::Server;
use table::test_util::MemTable;

use crate::create_testing_sql_query_handler;
use crate::mysql::{all_datatype_testing_data, MysqlTextRow, TestingData};

fn create_mysql_server(table: MemTable) -> Result<Box<dyn Server>> {
    let query_handler = create_testing_sql_query_handler(table);
    let io_runtime = Arc::new(
        RuntimeBuilder::default()
            .worker_threads(4)
            .thread_name("mysql-io-handlers")
            .build()
            .unwrap(),
    );
    Ok(MysqlServer::create_server(
        query_handler,
        io_runtime,
        Default::default(),
    ))
}

#[tokio::test]
async fn test_start_mysql_server() -> Result<()> {
    let table = MemTable::default_numbers_table();

    let mysql_server = create_mysql_server(table)?;
    let listening = "127.0.0.1:0".parse::<SocketAddr>().unwrap();
    let result = mysql_server.start(listening).await;
    assert!(result.is_ok());

    let result = mysql_server.start(listening).await;
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("MySQL server has been started."));
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_shutdown_mysql_server() -> Result<()> {
    common_telemetry::init_default_ut_logging();

    let table = MemTable::default_numbers_table();

    let mysql_server = create_mysql_server(table)?;
    let result = mysql_server.shutdown().await;
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("MySQL server is not started."));

    let listening = "127.0.0.1:0".parse::<SocketAddr>().unwrap();
    let server_addr = mysql_server.start(listening).await.unwrap();
    let server_port = server_addr.port();

    let mut join_handles = vec![];
    for index in 0..2 {
        join_handles.push(tokio::spawn(async move {
            for _ in 0..1000 {
                match create_connection(server_port, index == 1).await {
                    Ok(mut connection) => {
                        let result: u32 = connection
                            .query_first("SELECT uint32s FROM numbers LIMIT 1")
                            .await
                            .unwrap()
                            .unwrap();
                        assert_eq!(result, 0);
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                    Err(e) => return Err(e),
                }
            }
            Ok(())
        }))
    }

    tokio::time::sleep(Duration::from_millis(100)).await;
    let result = mysql_server.shutdown().await;
    assert!(result.is_ok());

    for handle in join_handles.iter_mut() {
        let result = handle.await.unwrap();
        assert!(result.is_err());
        let error = result.unwrap_err().to_string();
        assert!(error.contains("Connection refused") || error.contains("Connection reset by peer"));
    }
    Ok(())
}

#[tokio::test]
async fn test_query_all_datatypes() -> Result<()> {
    common_telemetry::init_default_ut_logging();

    let TestingData {
        column_schemas,
        mysql_columns_def,
        columns,
        mysql_text_output_rows,
    } = all_datatype_testing_data();
    let schema = Arc::new(Schema::new(column_schemas.clone()));
    let recordbatch = RecordBatch::new(schema, columns).unwrap();
    let table = MemTable::new("all_datatypes", recordbatch);

    let mysql_server = create_mysql_server(table)?;
    let listening = "127.0.0.1:0".parse::<SocketAddr>().unwrap();
    let server_addr = mysql_server.start(listening).await.unwrap();

    let mut connection = create_connection(server_addr.port(), false).await.unwrap();
    let mut result = connection
        .query_iter("SELECT * FROM all_datatypes LIMIT 3")
        .await
        .unwrap();
    let columns = result.columns().unwrap();
    assert_eq!(column_schemas.len(), columns.len());

    for (i, column) in columns.iter().enumerate() {
        assert_eq!(mysql_columns_def[i], column.column_type());
        assert_eq!(column_schemas[i].name, column.name_str());
    }

    let rows = result.collect::<MysqlTextRow>().await.unwrap();
    assert_eq!(3, rows.len());
    for (expected, actual) in mysql_text_output_rows.iter().take(3).zip(rows.iter()) {
        assert_eq!(expected, &actual.values);
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_query_concurrently() -> Result<()> {
    common_telemetry::init_default_ut_logging();

    let table = MemTable::default_numbers_table();

    let mysql_server = create_mysql_server(table)?;
    let listening = "127.0.0.1:0".parse::<SocketAddr>().unwrap();
    let server_addr = mysql_server.start(listening).await.unwrap();
    let server_port = server_addr.port();

    let threads = 4;
    let expect_executed_queries_per_worker = 1000;
    let mut join_handles = vec![];
    for index in 0..threads {
        join_handles.push(tokio::spawn(async move {
            let mut rand: StdRng = rand::SeedableRng::from_entropy();

            let mut connection = create_connection(server_port, index % 2 == 0)
                .await
                .unwrap();
            for _ in 0..expect_executed_queries_per_worker {
                let expected: u32 = rand.gen_range(0..100);
                let result: u32 = connection
                    .query_first(format!(
                        "SELECT uint32s FROM numbers WHERE uint32s = {}",
                        expected
                    ))
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(result, expected);

                let should_recreate_conn = expected == 1;
                if should_recreate_conn {
                    connection = create_connection(server_port, index % 2 == 0)
                        .await
                        .unwrap();
                }
            }
            expect_executed_queries_per_worker
        }))
    }
    let mut total_pending_queries = threads * expect_executed_queries_per_worker;
    for handle in join_handles.iter_mut() {
        total_pending_queries -= handle.await.unwrap();
    }
    assert_eq!(0, total_pending_queries);
    Ok(())
}

async fn create_connection(port: u16, with_pwd: bool) -> mysql_async::Result<mysql_async::Conn> {
    let mut opts = mysql_async::OptsBuilder::default()
        .ip_or_hostname("127.0.0.1")
        .tcp_port(port)
        .prefer_socket(false)
        .wait_timeout(Some(1000));

    if with_pwd {
        opts = opts.pass(Some("default_pwd".to_string()));
    }

    mysql_async::Conn::new(opts).await
}
