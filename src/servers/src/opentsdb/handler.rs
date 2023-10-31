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

//! Modified from Tokio's mini-redis example.

use common_error::ext::ErrorExt;
use session::context::QueryContextBuilder;
use tokio::io::{AsyncRead, AsyncWrite};

use crate::error::Result;
use crate::opentsdb::codec::DataPoint;
use crate::opentsdb::connection::Connection;
use crate::query_handler::OpentsdbProtocolHandlerRef;
use crate::shutdown::Shutdown;

/// Per-connection handler. Reads requests from `connection` and applies the OpenTSDB metric to
/// [OpentsdbProtocolHandlerRef].
pub(crate) struct Handler<S: AsyncWrite + AsyncRead + Unpin> {
    query_handler: OpentsdbProtocolHandlerRef,

    /// The TCP connection decorated with OpenTSDB line protocol encoder / decoder implemented
    /// using a buffered `TcpStream`.
    ///
    /// When TCP listener receives an inbound connection, the `TcpStream` is passed to
    /// `Connection::new`, which initializes the associated buffers. The byte level protocol
    /// parsing details is encapsulated in `Connection`.
    connection: Connection<S>,

    /// Listen for shutdown notifications.
    ///
    /// A wrapper around the `broadcast::Receiver` paired with the sender in TCP connections
    /// listener. The connection handler processes requests from the connection until the peer
    /// disconnects **or** a shutdown notification is received from `shutdown`. In the latter case,
    /// any in-flight work being processed for the peer is continued until it reaches a safe state,
    /// at which point the connection is terminated. (Graceful shutdown.)
    shutdown: Shutdown,
}

impl<S: AsyncWrite + AsyncRead + Unpin> Handler<S> {
    pub(crate) fn new(
        query_handler: OpentsdbProtocolHandlerRef,
        connection: Connection<S>,
        shutdown: Shutdown,
    ) -> Self {
        Self {
            query_handler,
            connection,
            shutdown,
        }
    }

    pub(crate) async fn run(&mut self) -> Result<()> {
        // TODO(shuiyisong): figure out how to auth in tcp connection.
        let ctx = QueryContextBuilder::default().build();
        while !self.shutdown.is_shutdown() {
            // While reading a request, also listen for the shutdown signal.
            let maybe_line = tokio::select! {
                line = self.connection.read_line() => line?,
                _ = self.shutdown.recv() => {
                    // If a shutdown signal is received, return from `run`.
                    // This will result in the task terminating.
                    return Ok(());
                }
            };

            // If `None` is returned from `read_line()` then the peer closed the socket. There is
            // no further work to do and the task can be terminated.
            let line = match maybe_line {
                Some(line) => line,
                None => return Ok(()),
            };

            // Close connection upon receiving "quit" line. With actual OpenTSDB, telnet just won't
            // quit, the connection to OpenTSDB server can be closed only via terminating telnet
            // session manually, for example, close the terminal window. That is a little annoying,
            // so I added "quit" command to the line protocol, to make telnet client able to quit
            // gracefully.
            if line.trim().eq_ignore_ascii_case("quit") {
                return Ok(());
            }

            match DataPoint::try_create(&line) {
                Ok(data_point) => {
                    let _timer =
                        crate::metrics::METRIC_TCP_OPENTSDB_LINE_WRITE_ELAPSED.start_timer();
                    let result = self.query_handler.exec(vec![data_point], ctx.clone()).await;
                    if let Err(e) = result {
                        self.connection.write_line(e.output_msg()).await?;
                    }
                }
                Err(e) => {
                    self.connection.write_line(e.output_msg()).await?;
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::sync::Arc;

    use async_trait::async_trait;
    use session::context::QueryContextRef;
    use tokio::net::{TcpListener, TcpStream};
    use tokio::sync::{broadcast, mpsc};

    use super::*;
    use crate::error;
    use crate::query_handler::OpentsdbProtocolHandler;

    struct DummyQueryHandler {
        tx: mpsc::Sender<String>,
    }

    #[async_trait]
    impl OpentsdbProtocolHandler for DummyQueryHandler {
        async fn exec(&self, data_points: Vec<DataPoint>, _ctx: QueryContextRef) -> Result<usize> {
            let metric = data_points.first().unwrap().metric();
            if metric == "should_failed" {
                return error::InternalSnafu {
                    err_msg: "expected",
                }
                .fail();
            }
            self.tx.send(metric.to_string()).await.unwrap();
            Ok(data_points.len())
        }
    }

    #[tokio::test]
    async fn test_run() {
        let (tx, mut rx) = mpsc::channel(100);

        let query_handler = Arc::new(DummyQueryHandler { tx });
        let (notify_shutdown, _) = broadcast::channel(1);
        let addr = start_server(query_handler, notify_shutdown).await;

        let stream = TcpStream::connect(addr).await.unwrap();
        let mut client = Connection::new(stream);

        client
            .write_line("put my_metric_1 1000 1.0 host=web01".to_string())
            .await
            .unwrap();
        assert_eq!(rx.recv().await.unwrap(), "my_metric_1");

        client
            .write_line("put my_metric_2 1000 1.0 host=web01".to_string())
            .await
            .unwrap();
        assert_eq!(rx.recv().await.unwrap(), "my_metric_2");

        client
            .write_line("put should_failed 1000 1.0 host=web01".to_string())
            .await
            .unwrap();
        let resp = client.read_line().await.unwrap();
        assert_eq!(resp, Some("Internal error: 1003".to_string()));

        client.write_line("get".to_string()).await.unwrap();
        let resp = client.read_line().await.unwrap();
        assert_eq!(
            resp,
            Some("Invalid query: unknown command get.".to_string())
        );
    }

    async fn start_server(
        query_handler: OpentsdbProtocolHandlerRef,
        notify_shutdown: broadcast::Sender<()>,
    ) -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let _handle = common_runtime::spawn_read(async move {
            loop {
                let (stream, _) = listener.accept().await.unwrap();

                let query_handler = query_handler.clone();
                let connection = Connection::new(stream);
                let shutdown = Shutdown::new(notify_shutdown.subscribe());
                let _handle = common_runtime::spawn_read(async move {
                    Handler::new(query_handler, connection, shutdown)
                        .run()
                        .await
                });
            }
        });
        addr
    }
}
