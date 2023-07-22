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
use datatypes::prelude::VectorRef;
use datatypes::schema::{ColumnSchema, Schema};
use datatypes::value::Value;
use mysql_async::prelude::*;
use mysql_async::{Conn, Row, SslOpts};
use rand::rngs::StdRng;
use rand::Rng;
use servers::error::Result;
use servers::mysql::server::{MysqlServer, MysqlSpawnConfig, MysqlSpawnRef};
use servers::server::Server;
use servers::tls::TlsOption;
use table::test_util::MemTable;

use crate::auth::{DatabaseAuthInfo, MockUserProvider};
use crate::create_testing_sql_query_handler;
use crate::mysql::{all_datatype_testing_data, MysqlTextRow, TestingData};

#[derive(Default)]
struct MysqlOpts<'a> {
    tls: TlsOption,
    auth_info: Option<DatabaseAuthInfo<'a>>,
    reject_no_database: bool,
}

fn create_mysql_server(table: MemTable, opts: MysqlOpts<'_>) -> Result<Box<dyn Server>> {
    let query_handler = create_testing_sql_query_handler(table);
    let io_runtime = Arc::new(
        RuntimeBuilder::default()
            .worker_threads(4)
            .thread_name("mysql-io-handlers")
            .build()
            .unwrap(),
    );

    let mut provider = MockUserProvider::default();
    if let Some(auth_info) = opts.auth_info {
        provider.set_authorization_info(auth_info);
    }

    Ok(MysqlServer::create_server(
        io_runtime,
        Arc::new(MysqlSpawnRef::new(query_handler, Some(Arc::new(provider)))),
        Arc::new(MysqlSpawnConfig::new(
            opts.tls.should_force_tls(),
            opts.tls.setup()?.map(Arc::new),
            opts.reject_no_database,
        )),
    ))
}

#[tokio::test]
async fn test_start_mysql_server() -> Result<()> {
    let table = MemTable::default_numbers_table();

    let mysql_server = create_mysql_server(table, Default::default())?;
    let listening = "127.0.0.1:0".parse::<SocketAddr>().unwrap();
    let result = mysql_server.start(listening).await;
    let _ = result.unwrap();

    let result = mysql_server.start(listening).await;
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("MySQL server has been started."));
    Ok(())
}

#[tokio::test]
async fn test_reject_no_database() -> Result<()> {
    common_telemetry::init_default_ut_logging();
    let table = MemTable::default_numbers_table();
    let mysql_server = create_mysql_server(
        table,
        MysqlOpts {
            reject_no_database: true,
            ..Default::default()
        },
    )?;
    let listening = "127.0.0.1:0".parse::<SocketAddr>().unwrap();
    let server_addr = mysql_server.start(listening).await.unwrap();
    let server_port = server_addr.port();

    let fail = create_connection(server_port, None, false).await;
    assert!(fail.is_err());
    let _ = create_connection(server_port, Some("public"), false)
        .await
        .unwrap();
    mysql_server.shutdown().await.unwrap();

    Ok(())
}

#[tokio::test]
async fn test_schema_validation() -> Result<()> {
    async fn generate_server(auth_info: DatabaseAuthInfo<'_>) -> Result<(Box<dyn Server>, u16)> {
        let table = MemTable::default_numbers_table();
        let mysql_server = create_mysql_server(
            table,
            MysqlOpts {
                auth_info: Some(auth_info),
                ..Default::default()
            },
        )?;
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

    let _ = create_connection_default_db_name(server_port, false)
        .await
        .unwrap();
    mysql_server.shutdown().await.unwrap();

    // change to another username
    let (mysql_server, server_port) = generate_server(DatabaseAuthInfo {
        catalog: "greptime",
        schema: "public",
        username: "no_access_user",
    })
    .await?;

    let fail = create_connection_default_db_name(server_port, false).await;
    assert!(fail.is_err());
    mysql_server.shutdown().await.unwrap();

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_shutdown_mysql_server() -> Result<()> {
    common_telemetry::init_default_ut_logging();

    let table = MemTable::default_numbers_table();

    let mysql_server = create_mysql_server(table, Default::default())?;
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
                match create_connection_default_db_name(server_port, false).await {
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
    mysql_server.shutdown().await.unwrap();

    for handle in join_handles.iter_mut() {
        let result = handle.await.unwrap();
        assert!(result.is_err());
        assert!(result.unwrap_err().is_fatal());
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

    let mysql_server = create_mysql_server(
        table,
        MysqlOpts {
            tls: server_tls,
            ..Default::default()
        },
    )?;

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

    let mysql_server = create_mysql_server(
        table,
        MysqlOpts {
            tls: server_tls,
            ..Default::default()
        },
    )?;

    let listening = "127.0.0.1:0".parse::<SocketAddr>().unwrap();
    let server_addr = mysql_server.start(listening).await.unwrap();

    let r = create_connection_default_db_name(server_addr.port(), client_tls).await;
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

    let mysql_server = create_mysql_server(
        table,
        MysqlOpts {
            tls: server_tls,
            ..Default::default()
        },
    )?;

    let listening = "127.0.0.1:0".parse::<SocketAddr>().unwrap();
    let server_addr = mysql_server.start(listening).await.unwrap();

    // None actually uses default database name
    let r = create_connection_default_db_name(server_addr.port(), client_tls).await;
    let _ = r.unwrap();

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

    let mysql_server = create_mysql_server(
        table,
        MysqlOpts {
            tls: server_tls,
            ..Default::default()
        },
    )?;

    let listening = "127.0.0.1:0".parse::<SocketAddr>().unwrap();
    let server_addr = mysql_server.start(listening).await.unwrap();

    let mut connection = create_connection_default_db_name(server_addr.port(), client_tls)
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

    let mysql_server = create_mysql_server(table, Default::default())?;
    let listening = "127.0.0.1:0".parse::<SocketAddr>().unwrap();
    let server_addr = mysql_server.start(listening).await.unwrap();
    let server_port = server_addr.port();

    let threads = 4;
    let expect_executed_queries_per_worker = 1000;
    let mut join_handles = vec![];
    for _ in 0..threads {
        join_handles.push(tokio::spawn(async move {
            let mut rand: StdRng = rand::SeedableRng::from_entropy();

            let mut connection = create_connection_default_db_name(server_port, false)
                .await
                .unwrap();
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
                    connection = create_connection_default_db_name(server_port, false)
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

#[ignore = "https://github.com/GreptimeTeam/greptimedb/issues/1385"]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_query_prepared() -> Result<()> {
    common_telemetry::init_default_ut_logging();
    let TestingData {
        column_schemas,
        mysql_columns_def: _,
        columns,
        mysql_text_output_rows: _,
    } = all_datatype_testing_data();
    let schema = Arc::new(Schema::new(column_schemas.clone()));
    let recordbatch = RecordBatch::new(schema, columns.clone()).unwrap();
    let table = MemTable::new("all_datatypes", recordbatch);

    let mysql_server = create_mysql_server(
        table,
        MysqlOpts {
            ..Default::default()
        },
    )?;

    let listening = "127.0.0.1:0".parse::<SocketAddr>().unwrap();
    let server_addr = mysql_server.start(listening).await.unwrap();

    let mut connection = create_connection_default_db_name(server_addr.port(), false)
        .await
        .unwrap();

    test_prepare_all_type(column_schemas, columns, &mut connection).await;

    Ok(())
}

async fn test_prepare_all_type(
    column_schemas: Vec<ColumnSchema>,
    columns: Vec<VectorRef>,
    connection: &mut Conn,
) {
    let mut column_index = 0;
    let mut stmt_id = 1;
    for schema in column_schemas {
        let query = format!(
            "SELECT {} FROM all_datatypes WHERE {} = ?",
            schema.name, schema.name
        );
        let statement = connection.prep(query).await;
        let statement = statement.unwrap();
        assert_eq!(stmt_id, statement.id());
        stmt_id += 1;

        let vector_ref = columns.get(column_index).unwrap();
        for vector_index in 0..vector_ref.len() {
            let v = vector_ref.get(vector_index);
            let v = if let Some(v) = prepare_convert_type(v) {
                v
            } else {
                continue;
            };

            let output: std::result::Result<Vec<Row>, mysql_async::Error> =
                connection.exec(statement.clone(), vec![v]).await;

            let rows = output.unwrap();
            assert!(!rows.is_empty());
        }
        column_index += 1;
    }
}

fn prepare_convert_type(item: Value) -> Option<mysql_async::Value> {
    let v = match item {
        Value::UInt8(u) => mysql_async::Value::UInt(u as u64),
        Value::UInt16(u) => mysql_async::Value::UInt(u as u64),
        Value::UInt32(u) => mysql_async::Value::UInt(u as u64),
        Value::UInt64(u) => mysql_async::Value::UInt(u),
        Value::Int8(i) => mysql_async::Value::Int(i as i64),
        Value::Int16(i) => mysql_async::Value::Int(i as i64),
        Value::Int32(i) => mysql_async::Value::Int(i as i64),
        Value::Int64(i) => mysql_async::Value::Int(i),
        Value::Float32(f) => mysql_async::Value::Float(f.into()),
        Value::Float64(f) => mysql_async::Value::Double(f.into()),
        Value::String(s) => mysql_async::Value::Bytes(s.as_utf8().as_bytes().to_vec()),
        Value::Binary(b) => mysql_async::Value::Bytes(b.to_vec()),
        _ => return None,
    };
    Some(v)
}

async fn create_connection_default_db_name(
    port: u16,
    ssl: bool,
) -> mysql_async::Result<mysql_async::Conn> {
    create_connection(port, Some(DEFAULT_SCHEMA_NAME), ssl).await
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
        .user(Some("greptime".to_string()))
        .pass(Some("greptime".to_string()));

    if let Some(db_name) = db_name {
        opts = opts.db_name(Some(db_name.to_string()));
    }

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
