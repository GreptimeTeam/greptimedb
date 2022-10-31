use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use common_runtime::Builder as RuntimeBuilder;
use rand::rngs::StdRng;
use rand::Rng;
use servers::error::Result;
use servers::postgres::PostgresServer;
use servers::server::Server;
use table::test_util::MemTable;
use tokio_postgres::{Client, Error as PgError, NoTls, SimpleQueryMessage};

use crate::create_testing_sql_query_handler;

fn create_postgres_server(table: MemTable) -> Result<Box<dyn Server>> {
    let query_handler = create_testing_sql_query_handler(table);
    let io_runtime = Arc::new(
        RuntimeBuilder::default()
            .worker_threads(4)
            .thread_name("postgres-io-handlers")
            .build()
            .unwrap(),
    );
    Ok(Box::new(PostgresServer::new(query_handler, io_runtime)))
}

#[tokio::test]
pub async fn test_start_postgres_server() -> Result<()> {
    let table = MemTable::default_numbers_table();

    let mut pg_server = create_postgres_server(table)?;
    let listening = "127.0.0.1:0".parse::<SocketAddr>().unwrap();
    let result = pg_server.start(listening).await;
    assert!(result.is_ok());

    let result = pg_server.start(listening).await;
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("Postgres server has been started."));
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_shutdown_pg_server() -> Result<()> {
    common_telemetry::init_default_ut_logging();

    let table = MemTable::default_numbers_table();

    let mut postgres_server = create_postgres_server(table)?;
    let result = postgres_server.shutdown().await;
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("Postgres server is not started."));

    let listening = "127.0.0.1:5432".parse::<SocketAddr>().unwrap();
    let server_addr = postgres_server.start(listening).await.unwrap();
    let server_port = server_addr.port();

    let mut join_handles = vec![];
    for _ in 0..2 {
        join_handles.push(tokio::spawn(async move {
            for _ in 0..1000 {
                match create_connection(server_port).await {
                    Ok(connection) => {
                        match connection
                            .simple_query("SELECT uint32s FROM numbers LIMIT 1")
                            .await
                        {
                            Ok(rows) => {
                                let result_text = unwrap_results(&rows)[0];
                                let result: i32 = result_text.parse().unwrap();
                                assert_eq!(result, 0);
                                tokio::time::sleep(Duration::from_millis(10)).await;
                            }
                            Err(e) => {
                                return Err(e);
                            }
                        }
                    }
                    Err(e) => {
                        return Err(e);
                    }
                }
            }
            Ok(())
        }))
    }

    tokio::time::sleep(Duration::from_millis(100)).await;
    let result = postgres_server.shutdown().await;
    assert!(result.is_ok());

    for handle in join_handles.iter_mut() {
        let result = handle.await.unwrap();
        assert!(result.is_err());
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_query_pg_concurrently() -> Result<()> {
    common_telemetry::init_default_ut_logging();

    let table = MemTable::default_numbers_table();

    let mut pg_server = create_postgres_server(table)?;
    let listening = "127.0.0.1:0".parse::<SocketAddr>().unwrap();
    let server_addr = pg_server.start(listening).await.unwrap();
    let server_port = server_addr.port();

    let threads = 4;
    let expect_executed_queries_per_worker = 300;
    let mut join_handles = vec![];
    for _i in 0..threads {
        join_handles.push(tokio::spawn(async move {
            let mut rand: StdRng = rand::SeedableRng::from_entropy();

            let mut client = create_connection(server_port).await.unwrap();

            for _k in 0..expect_executed_queries_per_worker {
                let expected: u32 = rand.gen_range(0..100);
                let result: u32 = unwrap_results(
                    client
                        .simple_query(&format!(
                            "SELECT uint32s FROM numbers WHERE uint32s = {}",
                            expected
                        ))
                        .await
                        .unwrap()
                        .as_ref(),
                )[0]
                .parse()
                .unwrap();
                assert_eq!(result, expected);

                // 1/100 chance to reconnect
                let should_recreate_conn = expected == 1;
                if should_recreate_conn {
                    client = create_connection(server_port).await.unwrap();
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

async fn create_connection(port: u16) -> std::result::Result<Client, PgError> {
    let url = format!("host=127.0.0.1 port={} connect_timeout=2", port);
    let (client, conn) = tokio_postgres::connect(&url, NoTls).await?;
    tokio::spawn(conn);
    Ok(client)
}

fn resolve_result(resp: &SimpleQueryMessage, col_index: usize) -> Option<&str> {
    match resp {
        &SimpleQueryMessage::Row(ref r) => r.get(col_index),
        _ => None,
    }
}

fn unwrap_results(resp: &[SimpleQueryMessage]) -> Vec<&str> {
    resp.iter().filter_map(|m| resolve_result(m, 0)).collect()
}
