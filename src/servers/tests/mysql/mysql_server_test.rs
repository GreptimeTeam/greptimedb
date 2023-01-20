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

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use common_catalog::consts::DEFAULT_SCHEMA_NAME;
use common_recordbatch::RecordBatch;
use common_runtime::Builder as RuntimeBuilder;
use datatypes::schema::Schema;
use mysql_async::prelude::*;
use mysql_async::SslOpts;
use rand::rngs::StdRng;
use rand::Rng;
use servers::error::Result;
use servers::mysql::server::MysqlServer;
use servers::server::Server;
use servers::tls::TlsOption;
use table::test_util::MemTable;

use crate::auth::{DatabaseAuthInfo, MockUserProvider};
use crate::create_testing_sql_query_handler;
use crate::mysql::{all_datatype_testing_data, MysqlTextRow, TestingData};

fn create_mysql_server(
    table: MemTable,
    tls: TlsOption,
    auth_info: Option<DatabaseAuthInfo>,
) -> Result<Box<dyn Server>> {
    let query_handler = create_testing_sql_query_handler(table);
    let io_runtime = Arc::new(
        RuntimeBuilder::default()
            .worker_threads(4)
            .thread_name("mysql-io-handlers")
            .build()
            .unwrap(),
    );

    let mut provider = MockUserProvider::default();
    if let Some(auth_info) = auth_info {
        provider.set_authorization_info(auth_info);
    }

    Ok(MysqlServer::create_server(
        query_handler,
        io_runtime,
        tls,
        Some(Arc::new(provider)),
    ))
}

#[tokio::test]
async fn test_start_mysql_server() -> Result<()> {
    let table = MemTable::default_numbers_table();

    let mysql_server = create_mysql_server(table, Default::default(), None)?;
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

#[tokio::test]
async fn test_schema_validation() -> Result<()> {
    async fn generate_server(auth_info: DatabaseAuthInfo<'_>) -> Result<(Box<dyn Server>, u16)> {
        let table = MemTable::default_numbers_table();
        let mysql_server = create_mysql_server(table, Default::default(), Some(auth_info))?;
        let listening = "127.0.0.1:0".parse::<SocketAddr>().unwrap();
        let server_addr = mysql_server.start(listening).await.unwrap();
        Ok((mysql_server, server_addr.port()))
    }

    common_telemetry::init_default_ut_logging();
    let (mysql_server, server_port) = generate_server(DatabaseAuthInfo {
        catalog: "greptime",
        schema: "public",
        username: "greptime",
    })
    .await?;

    //TODO(shuiyisong): mysql conn without dbname rejection is not implemented yet, add test later.

    let pass = create_connection(server_port, Some("public"), false).await;
    assert!(pass.is_ok());
    let result = mysql_server.shutdown().await;
    assert!(result.is_ok());

    // change to another username
    let (mysql_server, server_port) = generate_server(DatabaseAuthInfo {
        catalog: "greptime",
        schema: "public",
        username: "no_access_user",
    })
    .await?;

    let fail = create_connection(server_port, Some("public"), false).await;
    assert!(fail.is_err());
    let result = mysql_server.shutdown().await;
    assert!(result.is_ok());

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_shutdown_mysql_server() -> Result<()> {
    common_telemetry::init_default_ut_logging();

    let table = MemTable::default_numbers_table();

    let mysql_server = create_mysql_server(table, Default::default(), None)?;
    let result = mysql_server.shutdown().await;
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("MySQL server is not started."));

    let listening = "127.0.0.1:0".parse::<SocketAddr>().unwrap();
    let server_addr = mysql_server.start(listening).await.unwrap();
    let server_port = server_addr.port();

    let mut join_handles = vec![];
    for _ in 0..2 {
        join_handles.push(tokio::spawn(async move {
            for _ in 0..1000 {
                match create_connection(server_port, None, false).await {
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

    let server_tls = TlsOption::default();
    let client_tls = false;

    do_test_query_all_datatypes(server_tls, client_tls).await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_server_prefer_secure_client_plain() -> Result<()> {
    do_test_query_all_datatypes_with_secure_server(servers::tls::TlsMode::Prefer, false, false)
        .await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_server_prefer_secure_client_plain_with_pkcs8_priv_key() -> Result<()> {
    do_test_query_all_datatypes_with_secure_server(servers::tls::TlsMode::Prefer, false, true)
        .await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_server_require_secure_client_secure() -> Result<()> {
    do_test_query_all_datatypes_with_secure_server(servers::tls::TlsMode::Require, true, false)
        .await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_server_require_secure_client_secure_with_pkcs8_priv_key() -> Result<()> {
    do_test_query_all_datatypes_with_secure_server(servers::tls::TlsMode::Require, true, true)
        .await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_server_required_secure_client_plain() -> Result<()> {
    let server_tls = TlsOption {
        mode: servers::tls::TlsMode::Require,
        cert_path: "tests/ssl/server.crt".to_owned(),
        key_path: "tests/ssl/server-rsa.key".to_owned(),
    };

    let client_tls = false;

    #[allow(unused)]
    let TestingData {
        column_schemas,
        mysql_columns_def,
        columns,
        mysql_text_output_rows,
    } = all_datatype_testing_data();
    let schema = Arc::new(Schema::new(column_schemas.clone()));
    let recordbatch = RecordBatch::new(schema, columns).unwrap();
    let table = MemTable::new("all_datatypes", recordbatch);

    let mysql_server = create_mysql_server(table, server_tls, None)?;

    let listening = "127.0.0.1:0".parse::<SocketAddr>().unwrap();
    let server_addr = mysql_server.start(listening).await.unwrap();

    let r = create_connection(server_addr.port(), None, client_tls).await;
    assert!(r.is_err());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_server_required_secure_client_plain_with_pkcs8_priv_key() -> Result<()> {
    let server_tls = TlsOption {
        mode: servers::tls::TlsMode::Require,
        cert_path: "tests/ssl/server.crt".to_owned(),
        key_path: "tests/ssl/server-pkcs8.key".to_owned(),
    };

    let client_tls = false;

    #[allow(unused)]
    let TestingData {
        column_schemas,
        mysql_columns_def,
        columns,
        mysql_text_output_rows,
    } = all_datatype_testing_data();
    let schema = Arc::new(Schema::new(column_schemas.clone()));
    let recordbatch = RecordBatch::new(schema, columns).unwrap();
    let table = MemTable::new("all_datatypes", recordbatch);

    let mysql_server = create_mysql_server(table, server_tls, None)?;

    let listening = "127.0.0.1:0".parse::<SocketAddr>().unwrap();
    let server_addr = mysql_server.start(listening).await.unwrap();

    let r = create_connection(server_addr.port(), None, client_tls).await;
    assert!(r.is_err());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_db_name() -> Result<()> {
    let server_tls = TlsOption::default();
    let client_tls = false;

    #[allow(unused)]
    let TestingData {
        column_schemas,
        mysql_columns_def,
        columns,
        mysql_text_output_rows,
    } = all_datatype_testing_data();
    let schema = Arc::new(Schema::new(column_schemas.clone()));
    let recordbatch = RecordBatch::new(schema, columns).unwrap();
    let table = MemTable::new("all_datatypes", recordbatch);

    let mysql_server = create_mysql_server(table, server_tls, None)?;

    let listening = "127.0.0.1:0".parse::<SocketAddr>().unwrap();
    let server_addr = mysql_server.start(listening).await.unwrap();

    let r = create_connection(server_addr.port(), None, client_tls).await;
    assert!(r.is_ok());

    let r = create_connection(server_addr.port(), Some(DEFAULT_SCHEMA_NAME), client_tls).await;
    assert!(r.is_ok());

    let r = create_connection(server_addr.port(), Some("tomcat"), client_tls).await;
    assert!(r.is_err());
    Ok(())
}

async fn do_test_query_all_datatypes(server_tls: TlsOption, client_tls: bool) -> Result<()> {
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

    let mysql_server = create_mysql_server(table, server_tls, None)?;

    let listening = "127.0.0.1:0".parse::<SocketAddr>().unwrap();
    let server_addr = mysql_server.start(listening).await.unwrap();

    let mut connection = create_connection(server_addr.port(), None, client_tls)
        .await
        .unwrap();

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

    let mysql_server = create_mysql_server(table, Default::default(), None)?;
    let listening = "127.0.0.1:0".parse::<SocketAddr>().unwrap();
    let server_addr = mysql_server.start(listening).await.unwrap();
    let server_port = server_addr.port();

    let threads = 4;
    let expect_executed_queries_per_worker = 1000;
    let mut join_handles = vec![];
    for _ in 0..threads {
        join_handles.push(tokio::spawn(async move {
            let mut rand: StdRng = rand::SeedableRng::from_entropy();

            let mut connection = create_connection(server_port, None, false).await.unwrap();
            for _ in 0..expect_executed_queries_per_worker {
                let expected: u32 = rand.gen_range(0..100);
                let result: u32 = connection
                    .query_first(format!(
                        "SELECT uint32s FROM numbers WHERE uint32s = {expected}"
                    ))
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(result, expected);

                let should_recreate_conn = expected == 1;
                if should_recreate_conn {
                    connection = create_connection(server_port, None, false).await.unwrap();
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

async fn create_connection(
    port: u16,
    db_name: Option<&str>,
    ssl: bool,
) -> mysql_async::Result<mysql_async::Conn> {
    let mut opts = mysql_async::OptsBuilder::default()
        .ip_or_hostname("127.0.0.1")
        .tcp_port(port)
        .prefer_socket(false)
        .wait_timeout(Some(1000))
        .db_name(db_name.or(Some(DEFAULT_SCHEMA_NAME)))
        .user(Some("greptime".to_string()))
        .pass(Some("greptime".to_string()));

    if ssl {
        let ssl_opts = SslOpts::default()
            .with_danger_skip_domain_validation(true)
            .with_danger_accept_invalid_certs(true);
        opts = opts.ssl_opts(ssl_opts)
    }

    mysql_async::Conn::new(opts).await
}

async fn do_test_query_all_datatypes_with_secure_server(
    server_tls_mode: servers::tls::TlsMode,
    client_tls: bool,
    is_pkcs8_priv_key: bool,
) -> Result<()> {
    let server_tls = TlsOption {
        mode: server_tls_mode,
        cert_path: "tests/ssl/server.crt".to_owned(),
        key_path: {
            if is_pkcs8_priv_key {
                "tests/ssl/server-pkcs8.key".to_owned()
            } else {
                "tests/ssl/server-rsa.key".to_owned()
            }
        },
    };

    do_test_query_all_datatypes(server_tls, client_tls).await
}
