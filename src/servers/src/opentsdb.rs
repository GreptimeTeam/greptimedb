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

pub mod codec;
pub mod connection;
mod handler;

use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;

use api::v1::RowInsertRequests;
use async_trait::async_trait;
use common_runtime::Runtime;
use common_telemetry::logging::error;
use futures::StreamExt;
use tokio::sync::broadcast;

use self::codec::DataPoint;
use crate::error::Result;
use crate::opentsdb::connection::Connection;
use crate::opentsdb::handler::Handler;
use crate::prom_store::{FIELD_COLUMN_NAME, TIMESTAMP_COLUMN_NAME};
use crate::query_handler::OpentsdbProtocolHandlerRef;
use crate::row_writer::{self, MultiTableData};
use crate::server::{AbortableStream, BaseTcpServer, Server};
use crate::shutdown::Shutdown;

pub struct OpentsdbServer {
    base_server: BaseTcpServer,
    query_handler: OpentsdbProtocolHandlerRef,

    /// Broadcasts a shutdown signal to all active connections.
    ///
    /// When a connection task is spawned, it is passed a broadcast receiver handle. We can send
    /// a `()` value via `notify_shutdown` or just drop `notify_shutdown`, then each active
    /// connection receives it, reaches a safe terminal state, and completes the task.
    notify_shutdown: Option<broadcast::Sender<()>>,
}

impl OpentsdbServer {
    pub fn create_server(
        query_handler: OpentsdbProtocolHandlerRef,
        io_runtime: Arc<Runtime>,
    ) -> Box<dyn Server> {
        // When the provided `shutdown` future completes, we must send a shutdown
        // message to all active connections. We use a broadcast channel for this
        // purpose. The call below ignores the receiver of the broadcast pair, and when
        // a receiver is needed, the subscribe() method on the sender is used to create
        // one.
        let (notify_shutdown, _) = broadcast::channel(1);

        Box::new(OpentsdbServer {
            base_server: BaseTcpServer::create_server("OpenTSDB", io_runtime),
            query_handler,
            notify_shutdown: Some(notify_shutdown),
        })
    }

    fn accept(
        &self,
        io_runtime: Arc<Runtime>,
        stream: AbortableStream,
    ) -> impl Future<Output = ()> {
        let query_handler = self.query_handler.clone();
        let notify_shutdown = self
            .notify_shutdown
            .clone()
            .expect("`notify_shutdown` must be present when accepting connection!");
        stream.for_each(move |stream| {
            let io_runtime = io_runtime.clone();
            let query_handler = query_handler.clone();
            let shutdown = Shutdown::new(notify_shutdown.subscribe());
            async move {
                match stream {
                    Ok(stream) => {
                        if let Err(e) = stream.set_nodelay(true) {
                            error!(e; "Failed to set TCP nodelay");
                        }
                        let connection = Connection::new(stream);
                        let mut handler = Handler::new(query_handler, connection, shutdown);

                        let _handle = io_runtime.spawn(async move {
                            if let Err(e) = handler.run().await {
                                error!(e; "Unexpected error when handling OpenTSDB connection");
                            }
                        });
                    }
                    Err(error) => error!("Broken pipe: {}", error), // IoError doesn't impl ErrorExt.
                };
            }
        })
    }
}

pub const OPENTSDB_SERVER: &str = "OPENTSDB_SERVER";

#[async_trait]
impl Server for OpentsdbServer {
    async fn shutdown(&self) -> Result<()> {
        if let Some(tx) = &self.notify_shutdown {
            // Err of broadcast sender does not mean that future calls to send will fail, so
            // its return value is ignored here.
            let _ = tx.send(());
        }
        self.base_server.shutdown().await?;
        Ok(())
    }

    async fn start(&self, listening: SocketAddr) -> Result<SocketAddr> {
        let (stream, addr) = self.base_server.bind(listening).await?;

        let io_runtime = self.base_server.io_runtime();
        let join_handle = common_runtime::spawn_read(self.accept(io_runtime, stream));
        self.base_server.start_with(join_handle).await?;
        Ok(addr)
    }
    fn name(&self) -> &str {
        OPENTSDB_SERVER
    }
}

pub fn data_point_to_grpc_row_insert_requests(
    data_points: Vec<DataPoint>,
) -> Result<(RowInsertRequests, usize)> {
    let mut multi_table_data = MultiTableData::new();

    for mut data_point in data_points {
        let tags: Vec<(String, String)> = std::mem::take(data_point.tags_mut());
        let table_name = data_point.metric();
        let value = data_point.value();
        let timestamp = data_point.ts_millis();
        // length of tags + 2 extra columns for greptime_timestamp and the value
        let num_columns = tags.len() + 2;

        let table_data = multi_table_data.get_or_default_table_data(table_name, num_columns, 1);
        let mut one_row = table_data.alloc_one_row();

        // tags
        row_writer::write_tags(table_data, tags.into_iter(), &mut one_row)?;

        // value
        row_writer::write_f64(table_data, FIELD_COLUMN_NAME, value, &mut one_row)?;
        // timestamp
        row_writer::write_ts_millis(
            table_data,
            TIMESTAMP_COLUMN_NAME,
            Some(timestamp),
            &mut one_row,
        )?;

        table_data.add_row(one_row);
    }

    Ok(multi_table_data.into_row_insert_requests())
}
