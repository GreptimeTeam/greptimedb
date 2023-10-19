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
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use common_runtime::Builder as RuntimeBuilder;
use rand::rngs::StdRng;
use rand::Rng;
use servers::error::{self as server_error, Error, Result};
use servers::opentsdb::codec::DataPoint;
use servers::opentsdb::connection::Connection;
use servers::opentsdb::OpentsdbServer;
use servers::query_handler::OpentsdbProtocolHandler;
use servers::server::Server;
use session::context::QueryContextRef;
use tokio::net::TcpStream;
use tokio::sync::{mpsc, Notify};

struct DummyOpentsdbInstance {
    tx: mpsc::Sender<i32>,
}

#[async_trait]
impl OpentsdbProtocolHandler for DummyOpentsdbInstance {
    async fn exec(&self, data_points: Vec<DataPoint>, _ctx: QueryContextRef) -> Result<usize> {
        let metric = data_points.first().unwrap().metric();
        if metric == "should_failed" {
            return server_error::InternalSnafu {
                err_msg: "expected",
            }
            .fail();
        }
        let i = metric.parse::<i32>().unwrap();
        let _ = self.tx.send(i * i).await;
        Ok(data_points.len())
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
    let server = create_opentsdb_server(tx)?;
    let listening = "127.0.0.1:0".parse::<SocketAddr>().unwrap();
    let result = server.start(listening).await;
    let _ = result.unwrap();

    let result = server.start(listening).await;
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("OpenTSDB server has been started."));
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_shutdown_opentsdb_server_concurrently() -> Result<()> {
    let (tx, _) = mpsc::channel(100);
    let server = create_opentsdb_server(tx)?;
    let result = server.shutdown().await;
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("OpenTSDB server is not started."));

    let listening = "127.0.0.1:0".parse::<SocketAddr>().unwrap();
    let addr = server.start(listening).await?;

    let notify = Arc::new(Notify::new());
    let notify_in_task = notify.clone();
    let stop = Arc::new(AtomicBool::new(false));
    let stop_task = stop.clone();
    let stop_timeout = Duration::from_secs(5 * 60);
    let join_handle = tokio::spawn(async move {
        let mut i = 1;
        let mut stop_time = None;

        loop {
            let stream = TcpStream::connect(addr).await;
            match stream {
                Ok(stream) => {
                    let mut connection = Connection::new(stream);
                    let result = connection.write_line(format!("put {i} 1 1")).await;
                    i += 1;

                    if i > 4 {
                        // Ensure the server has been started.
                        notify_in_task.notify_one();
                    }

                    if let Err(e) = result {
                        match e {
                            Error::InternalIo { .. } => return,
                            _ => panic!("Not IO error, err is {e}"),
                        }
                    }

                    if stop.load(Ordering::Relaxed) {
                        let du_since_stop = stop_time.get_or_insert_with(Instant::now).elapsed();
                        if du_since_stop > stop_timeout {
                            // Avoid hang on test.
                            panic!("Stop timeout");
                        }
                    }
                }
                Err(_) => return,
            }
        }
    });

    notify.notified().await;

    server.shutdown().await.unwrap();

    stop_task.store(true, Ordering::Relaxed);
    join_handle.await.unwrap();

    Ok(())
}

#[tokio::test]
async fn test_opentsdb_connection_shutdown() -> Result<()> {
    let (tx, _) = mpsc::channel(100);
    let server = create_opentsdb_server(tx)?;
    let result = server.shutdown().await;
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("OpenTSDB server is not started."));

    let listening = "127.0.0.1:0".parse::<SocketAddr>().unwrap();
    let addr = server.start(listening).await?;

    let stream = TcpStream::connect(addr).await.unwrap();
    let mut connection = Connection::new(stream);
    connection
        .write_line("put 1 1 1".to_string())
        .await
        .unwrap();

    server.shutdown().await.unwrap();

    let shutdown_time = Instant::now();
    let timeout = Duration::from_secs(5 * 60);
    let mut i = 2;
    loop {
        // The connection may not be unwritable after shutdown immediately.
        let result = connection.write_line(format!("put {i} 1 1")).await;
        i += 1;
        if result.is_err() {
            if let Err(e) = result {
                match e {
                    Error::InternalIo { .. } => break,
                    _ => panic!("Not IO error, err is {e}"),
                }
            }
        }
        if shutdown_time.elapsed() > timeout {
            panic!("Shutdown timeout");
        }
    }

    Ok(())
}

#[tokio::test]
async fn test_opentsdb_connect_after_shutdown() -> Result<()> {
    let (tx, _) = mpsc::channel(100);
    let server = create_opentsdb_server(tx)?;
    let result = server.shutdown().await;
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("OpenTSDB server is not started."));

    let listening = "127.0.0.1:0".parse::<SocketAddr>().unwrap();
    let addr = server.start(listening).await?;

    server.shutdown().await.unwrap();

    assert!(TcpStream::connect(addr).await.is_err());

    Ok(())
}

#[tokio::test]
async fn test_query() -> Result<()> {
    let (tx, mut rx) = mpsc::channel(10);
    let server = create_opentsdb_server(tx)?;
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

    let server = create_opentsdb_server(tx)?;
    let listening = "127.0.0.1:0".parse::<SocketAddr>().unwrap();
    let addr = server.start(listening).await?;

    let mut join_handles = vec![];
    for _ in 0..threads {
        join_handles.push(tokio::spawn(async move {
            let mut rand: StdRng = rand::SeedableRng::from_entropy();

            let stream = TcpStream::connect(addr).await.unwrap();
            let mut connection = Connection::new(stream);
            for i in 0..expect_executed_queries_per_worker {
                connection.write_line(format!("put {i} 1 1")).await.unwrap();

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
