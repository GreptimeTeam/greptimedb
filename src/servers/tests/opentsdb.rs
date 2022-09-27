use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use common_runtime::Builder as RuntimeBuilder;
use rand::rngs::StdRng;
use rand::Rng;
use servers::error::{self as server_error, Result};
use servers::opentsdb::codec::DataPoint;
use servers::opentsdb::connection::Connection;
use servers::opentsdb::OpentsdbServer;
use servers::query_handler::OpentsdbProtocolHandler;
use servers::server::Server;
use tokio::net::TcpStream;
use tokio::sync::mpsc;

struct DummyOpentsdbInstance {
    tx: mpsc::Sender<i32>,
}

#[async_trait]
impl OpentsdbProtocolHandler for DummyOpentsdbInstance {
    async fn exec(&self, data_point: &DataPoint) -> Result<()> {
        let metric = data_point.metric();
        if metric == "should_failed" {
            return server_error::InternalSnafu {
                err_msg: "expected",
            }
            .fail();
        }
        let i = metric.parse::<i32>().unwrap();
        let _ = self.tx.send(i * i).await;
        Ok(())
    }
}

fn create_opentsdb_server(tx: mpsc::Sender<i32>) -> Result<Box<dyn Server>> {
    let query_handler = Arc::new(DummyOpentsdbInstance { tx });
    let io_runtime = Arc::new(
        RuntimeBuilder::default()
            .worker_threads(2)
            .thread_name("opentsdb-io-handlers")
            .build()
            .unwrap(),
    );
    Ok(OpentsdbServer::create_server(query_handler, io_runtime))
}

#[tokio::test]
async fn test_start_opentsdb_server() -> Result<()> {
    let (tx, _) = mpsc::channel(100);
    let mut server = create_opentsdb_server(tx)?;
    let listening = "127.0.0.1:0".parse::<SocketAddr>().unwrap();
    let result = server.start(listening).await;
    assert!(result.is_ok());

    let result = server.start(listening).await;
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("OpenTSDB server has been started."));
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_shutdown_opentsdb_server() -> Result<()> {
    let (tx, _) = mpsc::channel(100);
    let mut server = create_opentsdb_server(tx)?;
    let result = server.shutdown().await;
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("OpenTSDB server is not started."));

    let listening = "127.0.0.1:0".parse::<SocketAddr>().unwrap();
    let addr = server.start(listening).await?;

    let mut join_handles = vec![];
    for _ in 0..2 {
        join_handles.push(tokio::spawn(async move {
            for i in 0..1000 {
                let stream = TcpStream::connect(addr).await;
                match stream {
                    Ok(stream) => {
                        let mut connection = Connection::new(stream);
                        let result = connection.write_line(format!("put {} 1 1", i)).await;
                        if let Err(e) = result {
                            return Err(e.to_string());
                        }
                    }
                    Err(e) => return Err(e.to_string()),
                }
            }
            Ok(())
        }))
    }

    tokio::time::sleep(Duration::from_millis(10)).await;
    let result = server.shutdown().await;
    assert!(result.is_ok());

    for handle in join_handles.iter_mut() {
        let result = handle.await.unwrap();
        assert!(result.is_err());
        let error = result.unwrap_err();
        assert!(error.contains("Connection refused") || error.contains("Connection reset by peer"));
    }
    Ok(())
}

#[tokio::test]
async fn test_query() -> Result<()> {
    let (tx, mut rx) = mpsc::channel(10);
    let mut server = create_opentsdb_server(tx)?;
    let listening = "127.0.0.1:0".parse::<SocketAddr>().unwrap();
    let addr = server.start(listening).await?;

    let stream = TcpStream::connect(addr).await.unwrap();
    let mut connection = Connection::new(stream);
    connection.write_line("put 100 1 1".to_string()).await?;
    assert_eq!(rx.recv().await.unwrap(), 10000);

    connection
        .write_line("foo illegal put line".to_string())
        .await
        .unwrap();
    let result = connection.read_line().await?;
    assert_eq!(
        result,
        Some("Invalid query: unknown command foo.".to_string())
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_query_concurrently() -> Result<()> {
    let threads = 4;
    let expect_executed_queries_per_worker = 1000;
    let (tx, mut rx) = mpsc::channel(threads * expect_executed_queries_per_worker);

    let mut server = create_opentsdb_server(tx)?;
    let listening = "127.0.0.1:0".parse::<SocketAddr>().unwrap();
    let addr = server.start(listening).await?;

    let mut join_handles = vec![];
    for _ in 0..threads {
        join_handles.push(tokio::spawn(async move {
            let mut rand: StdRng = rand::SeedableRng::from_entropy();

            let stream = TcpStream::connect(addr).await.unwrap();
            let mut connection = Connection::new(stream);
            for i in 0..expect_executed_queries_per_worker {
                connection
                    .write_line(format!("put {} 1 1", i))
                    .await
                    .unwrap();

                let should_recreate_conn = rand.gen_range(0..100) == 1;
                if should_recreate_conn {
                    let stream = TcpStream::connect(addr).await.unwrap();
                    connection = Connection::new(stream);
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

    let mut expected_result: i32 = (threads
        * (0..expect_executed_queries_per_worker)
            .map(|i| i * i)
            .sum::<usize>()) as i32;
    while let Some(i) = rx.recv().await {
        expected_result -= i;
        if expected_result == 0 {
            break;
        }
    }
    Ok(())
}
