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

//! Streams rows from a remote MySQL table into a bounded channel.
//!
//! `mysql_async::QueryResult` borrows its `Conn`, so it is not `'static`. The
//! connection is therefore owned by a dedicated spawned task; decoded rows are
//! batched and sent over a bounded mpsc channel. The receiver is wrapped into a
//! [`RecordBatchStreamWrapper`], whose downstream drop cancels the task (the
//! `send` fails, the task disconnects and exits).

use common_error::ext::BoxedError;
use common_recordbatch::error::{Error as RecordBatchError, Result as RecordBatchResult};
use common_recordbatch::{RecordBatch, RecordBatchStreamWrapper, SendableRecordBatchStream};
use datatypes::prelude::{MutableVector, Value};
use datatypes::schema::{ColumnSchema, SchemaRef};
use mysql_async::prelude::Queryable;
use snafu::{GenerateImplicitData, Location, ResultExt};
use tokio::sync::mpsc;

use crate::connection::box_foreign_error;
use crate::convert;
use crate::error::{DecodeRowSnafu, Error};

/// Flush a batch once either threshold is reached.
const ROWS_PER_BATCH: usize = 8192;
const BYTES_PER_BATCH: usize = 8 * 1024 * 1024;
/// Bounded channel capacity: a few batches of backpressure (≈32 MiB).
const CHANNEL_CAPACITY: usize = 4;

/// Spawns the connection task and returns a stream of record batches.
pub(crate) fn create_stream(
    opts: mysql_async::Opts,
    connection: String,
    remote_table: String,
    sql: String,
    schema: SchemaRef,
    column_schemas: Vec<ColumnSchema>,
    scan_to_remote: Vec<Option<usize>>,
) -> SendableRecordBatchStream {
    let (tx, rx) = mpsc::channel::<RecordBatchResult<RecordBatch>>(CHANNEL_CAPACITY);

    let wrapper_schema = schema.clone();
    tokio::spawn(async move {
        stream_task(
            opts,
            connection,
            remote_table,
            sql,
            schema,
            column_schemas,
            scan_to_remote,
            tx,
        )
        .await;
    });

    Box::pin(RecordBatchStreamWrapper::new(
        wrapper_schema,
        tokio_stream::wrappers::ReceiverStream::new(rx),
    ))
}

/// Maps a relational [`Error`] into the recordbatch error the stream carries.
fn external(error: Error) -> RecordBatchError {
    RecordBatchError::External {
        source: BoxedError::new(error),
        location: Location::generate(),
    }
}

/// Owns a single connection for the lifetime of this task. On any error or
/// cancellation, the connection is disconnected and the task exits.
async fn stream_task(
    opts: mysql_async::Opts,
    connection: String,
    remote_table: String,
    sql: String,
    schema: SchemaRef,
    column_schemas: Vec<ColumnSchema>,
    scan_to_remote: Vec<Option<usize>>,
    tx: mpsc::Sender<RecordBatchResult<RecordBatch>>,
) {
    let mut conn = match mysql_async::Conn::new(opts).await {
        Ok(conn) => conn,
        Err(e) => {
            let _ = tx
                .send(Err(external(Error::ConnectRemote {
                    connection: table::requests::redact_connection(&connection),
                    source: box_foreign_error(e, &connection),
                    location: Location::generate(),
                })))
                .await;
            return;
        }
    };

    // `QueryResult` borrows `conn`; its inherent `next()` yields
    // `Result<Option<Row>>` (no `StreamExt` / pinning needed).
    let mut result = match conn.exec_iter(sql.as_str(), ()).await {
        Ok(result) => result,
        Err(e) => {
            let _ = tx
                .send(Err(external(Error::FetchRows {
                    remote_table: remote_table.clone(),
                    source: box_foreign_error(e, &connection),
                    location: Location::generate(),
                })))
                .await;
            let _ = conn.disconnect().await;
            return;
        }
    };

    let mut builder = BatchBuilder::new(schema, column_schemas, scan_to_remote);

    loop {
        let row = match result.next().await {
            Ok(Some(row)) => row,
            Ok(None) => break,
            Err(e) => {
                let _ = tx
                    .send(Err(external(Error::FetchRows {
                        remote_table: remote_table.clone(),
                        source: box_foreign_error(e, &connection),
                        location: Location::generate(),
                    })))
                    .await;
                drop(result);
                let _ = conn.disconnect().await;
                return;
            }
        };

        match builder.push_row(&row) {
            Ok(Some(batch)) => {
                if tx.send(Ok(batch)).await.is_err() {
                    // Downstream dropped the stream: cancel by disconnecting.
                    drop(result);
                    let _ = conn.disconnect().await;
                    return;
                }
            }
            Ok(None) => {}
            Err(e) => {
                let _ = tx.send(Err(e)).await;
                drop(result);
                let _ = conn.disconnect().await;
                return;
            }
        }
    }

    // Flush the final partial batch (if any), then close the channel normally.
    match builder.finish() {
        Ok(Some(batch)) => {
            let _ = tx.send(Ok(batch)).await;
        }
        Ok(None) => {}
        Err(e) => {
            let _ = tx.send(Err(e)).await;
        }
    }
    drop(result);
    let _ = conn.disconnect().await;
}

/// Accumulates decoded rows into columnar mutable vectors and flushes them into
/// [`RecordBatch`]es. Synthetic columns (mapped to `None`) are filled from their
/// default constraint at flush time, mirroring the file engine's
/// `FileToScanRegionStream`.
struct BatchBuilder {
    schema: SchemaRef,
    column_schemas: Vec<ColumnSchema>,
    /// `Some(idx)` = take remote column `idx`; `None` = synthetic column.
    scan_to_remote: Vec<Option<usize>>,
    /// Per-column mutable vector; `None` for synthetic columns (filled at flush).
    mutable_vectors: Vec<Option<Box<dyn MutableVector>>>,
    num_rows: usize,
    num_bytes: usize,
}

impl BatchBuilder {
    fn new(
        schema: SchemaRef,
        column_schemas: Vec<ColumnSchema>,
        scan_to_remote: Vec<Option<usize>>,
    ) -> Self {
        let mutable_vectors = column_schemas
            .iter()
            .zip(&scan_to_remote)
            .map(|(col, remote)| {
                remote
                    .is_some()
                    .then(|| col.create_mutable_vector(ROWS_PER_BATCH))
            })
            .collect();
        Self {
            schema,
            column_schemas,
            scan_to_remote,
            mutable_vectors,
            num_rows: 0,
            num_bytes: 0,
        }
    }

    /// Decodes one driver row and appends it. Returns `Some(batch)` when a
    /// threshold is reached and the batch should be emitted.
    fn push_row(&mut self, row: &mysql_async::Row) -> RecordBatchResult<Option<RecordBatch>> {
        for (i, column_schema) in self.column_schemas.iter().enumerate() {
            let Some(remote_idx) = self.scan_to_remote[i] else {
                // Synthetic column: filled at flush time.
                continue;
            };
            let vector = self.mutable_vectors[i].as_mut().unwrap();
            match row.as_ref(remote_idx) {
                // `Row::as_ref` returns `None` for SQL NULL; a `Value::NULL`
                // variant (which result rows do not produce) also maps to null.
                None | Some(mysql_async::Value::NULL) => vector.push_null(),
                Some(value) => {
                    let decoded = convert::decode_value(value, &column_schema.data_type, &column_schema.name)
                        .map_err(external)?;
                    let value_ref = decoded.as_value_ref();
                    vector
                        .try_push_value_ref(&value_ref)
                        .map_err(|e| {
                            external(DecodeRowSnafu {
                                column: column_schema.name.clone(),
                                reason: e.to_string(),
                            }
                            .build())
                        })?;
                    self.num_bytes += estimated_size(&decoded);
                }
            }
        }

        self.num_rows += 1;
        if self.num_rows >= ROWS_PER_BATCH || self.num_bytes >= BYTES_PER_BATCH {
            return Ok(Some(self.flush()?));
        }
        Ok(None)
    }

    /// Flushes the accumulated rows into a record batch and resets the buffers.
    fn flush(&mut self) -> RecordBatchResult<RecordBatch> {
        let mut columns = Vec::with_capacity(self.column_schemas.len());
        for (i, column_schema) in self.column_schemas.iter().enumerate() {
            let vector = match self.mutable_vectors[i].as_mut() {
                Some(vector) => vector.to_vector(),
                None => column_schema
                    .create_default_vector(self.num_rows)
                    .context(common_recordbatch::error::DataTypesSnafu)?
                    .ok_or_else(|| {
                        common_recordbatch::error::CreateRecordBatchesSnafu {
                            reason: format!(
                                "synthetic column `{}` has no default constraint",
                                column_schema.name
                            ),
                        }
                        .build()
                    })?,
            };
            columns.push(vector);
        }

        let batch = RecordBatch::new(self.schema.clone(), columns)?;

        // Reset buffers for the next batch.
        for (i, remote) in self.scan_to_remote.iter().enumerate() {
            if remote.is_some() {
                self.mutable_vectors[i] =
                    Some(self.column_schemas[i].create_mutable_vector(ROWS_PER_BATCH));
            }
        }
        self.num_rows = 0;
        self.num_bytes = 0;

        Ok(batch)
    }

    /// Flushes a final non-empty partial batch.
    fn finish(&mut self) -> RecordBatchResult<Option<RecordBatch>> {
        if self.num_rows == 0 {
            return Ok(None);
        }
        Ok(Some(self.flush()?))
    }
}

/// A coarse per-value byte estimate for the batch-size threshold. String and
/// binary use their actual byte length; everything else counts a fixed slot.
fn estimated_size(value: &Value) -> usize {
    match value {
        Value::String(s) => s.as_utf8().len(),
        Value::Binary(b) => b.len(),
        _ => std::mem::size_of::<Value>(),
    }
}
