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

use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;

use async_trait::async_trait;
use common_runtime::Runtime;
use common_telemetry::logging::{error, info};
use futures::StreamExt;
use opensrv_mysql::AsyncMysqlIntermediary;
use tokio;
use tokio::io::BufWriter;
use tokio::net::TcpStream;

use crate::error::Result;
use crate::mysql::handler::MysqlInstanceShim;
use crate::query_handler::SqlQueryHandlerRef;
use crate::server::{AbortableStream, BaseTcpServer, Server};

// Default size of ResultSet write buffer: 100KB
const DEFAULT_RESULT_SET_WRITE_BUFFER_SIZE: usize = 100 * 1024;

pub struct MysqlServer {
    base_server: BaseTcpServer,
    query_handler: SqlQueryHandlerRef,
}

impl MysqlServer {
    pub fn create_server(
        query_handler: SqlQueryHandlerRef,
        io_runtime: Arc<Runtime>,
    ) -> Box<dyn Server> {
        Box::new(MysqlServer {
            base_server: BaseTcpServer::create_server("MySQL", io_runtime),
            query_handler,
        })
    }

    fn accept(
        &self,
        io_runtime: Arc<Runtime>,
        stream: AbortableStream,
    ) -> impl Future<Output = ()> {
        let query_handler = self.query_handler.clone();
        stream.for_each(move |tcp_stream| {
            let io_runtime = io_runtime.clone();
            let query_handler = query_handler.clone();
            async move {
                match tcp_stream {
                    Err(error) => error!("Broken pipe: {}", error), // IoError doesn't impl ErrorExt.
                    Ok(io_stream) => {
                        if let Err(error) = Self::handle(io_stream, io_runtime, query_handler).await
                        {
                            error!(error; "Unexpected error when handling TcpStream");
                        };
                    }
                };
            }
        })
    }

    async fn handle(
        stream: TcpStream,
        io_runtime: Arc<Runtime>,
        query_handler: SqlQueryHandlerRef,
    ) -> Result<()> {
        info!("MySQL connection coming from: {}", stream.peer_addr()?);
        let shim = MysqlInstanceShim::create(query_handler, stream.peer_addr()?.to_string());

        let (r, w) = stream.into_split();
        let w = BufWriter::with_capacity(DEFAULT_RESULT_SET_WRITE_BUFFER_SIZE, w);
        // TODO(LFC): Use `output_stream` to write large MySQL ResultSet to client.
        let spawn_result = io_runtime
            .spawn(AsyncMysqlIntermediary::run_on(shim, r, w))
            .await;
        match spawn_result {
            Ok(run_result) => {
                if let Err(e) = run_result {
                    // TODO(LFC): Write this error and the below one to client as well, in MySQL text protocol.
                    // Looks like we have to expose opensrv-mysql's `PacketWriter`?
                    error!(e; "Internal error occurred during query exec, server actively close the channel to let client try next time.")
                }
            }
            Err(e) => error!("IO runtime cannot execute task, error: {}", e),
        }
        Ok(())
    }
}

#[async_trait]
impl Server for MysqlServer {
    async fn shutdown(&self) -> Result<()> {
        self.base_server.shutdown().await
    }

    async fn start(&self, listening: SocketAddr) -> Result<SocketAddr> {
        let (stream, addr) = self.base_server.bind(listening).await?;

        let io_runtime = self.base_server.io_runtime();
        let join_handle = tokio::spawn(self.accept(io_runtime, stream));
        self.base_server.start_with(join_handle).await?;
        Ok(addr)
    }
}
