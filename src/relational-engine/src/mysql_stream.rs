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
use datatypes::prelude::{ConcreteDataType, MutableVector, Value};
use datatypes::schema::{ColumnSchema, SchemaRef};
use mysql_async::prelude::Queryable;
use snafu::{GenerateImplicitData, Location, ResultExt};
use tokio::sync::mpsc;

use crate::connection::{box_foreign_error, box_timeout_error, CONNECT_TIMEOUT, QUERY_TIMEOUT};
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

fn validate_remote_columns(
    columns: &[mysql_async::Column],
    column_schemas: &[ColumnSchema],
    scan_to_remote: &[Option<usize>],
) -> crate::error::Result<()> {
    for (i, remote) in scan_to_remote.iter().enumerate() {
        let Some(idx) = remote else { continue };
        let column_schema = &column_schemas[i];
        let column = columns.get(*idx).ok_or_else(|| {
            DecodeRowSnafu {
                column: column_schema.name.clone(),
                reason: format!("remote result is missing column index {idx}"),
            }
            .build()
        })?;
        convert::validate_column(column, &column_schema.data_type, &column_schema.name)?;
    }
    Ok(())
}

/// Owns a single connection for the lifetime of this task. On any error or
/// cancellation, the connection is disconnected and the task exits.
///
/// Cancellation is not yet complete: `disconnect()` drains the pending result
/// before sending `COM_QUIT` (mysql_async 0.37), and the connect/exec/read
/// phases do not observe `tx` closing until a batch send fails. A dropped
/// consumer with a large remaining result can therefore still hold the
/// connection while it is drained. Full cancellation is tracked in #9767.
async fn stream_task(
    opts: mysql_async::Opts,
    connection: String,
    remote_table: String,
    sql: String,
    schema: SchemaRef,
    scan_to_remote: Vec<Option<usize>>,
    tx: mpsc::Sender<RecordBatchResult<RecordBatch>>,
) {
    let column_schemas = schema.column_schemas().to_vec();
    let mut conn = match tokio::time::timeout(CONNECT_TIMEOUT, mysql_async::Conn::new(opts)).await {
        Ok(Ok(conn)) => conn,
        Ok(Err(e)) => {
            let _ = tx
                .send(Err(external(Error::ConnectRemote {
                    connection: table::requests::redact_connection(&connection),
                    source: box_foreign_error(e, &connection),
                    location: Location::generate(),
                })))
                .await;
            return;
        }
        Err(_) => {
            let _ = tx
                .send(Err(external(Error::ConnectRemote {
                    connection: table::requests::redact_connection(&connection),
                    source: box_timeout_error("connecting to the remote database", CONNECT_TIMEOUT),
                    location: Location::generate(),
                })))
                .await;
            return;
        }
    };

    if let Err(e) = conn
        .exec_drop("SET SESSION time_zone = '+00:00'", ())
        .await
    {
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

    // `QueryResult` borrows `conn`; its inherent `next()` yields
    // `Result<Option<Row>>` (no `StreamExt` / pinning needed).
    let mut result = match tokio::time::timeout(QUERY_TIMEOUT, conn.exec_iter(sql.as_str(), ())).await
    {
        Ok(Ok(result)) => result,
        Ok(Err(e)) => {
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
        Err(_) => {
            let _ = tx
                .send(Err(external(Error::FetchRows {
                    remote_table: remote_table.clone(),
                    source: box_timeout_error("querying the remote database", QUERY_TIMEOUT),
                    location: Location::generate(),
                })))
                .await;
            let _ = conn.disconnect().await;
            return;
        }
    };

    let validation = validate_remote_columns(result.columns_ref(), &column_schemas, &scan_to_remote);
    if let Err(e) = validation {
        let _ = tx.send(Err(external(e))).await;
        drop(result);
        let _ = conn.disconnect().await;
        return;
    }

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
                // `as_ref` returns `None` only for an out-of-bounds or
                // already-consumed index; a SQL NULL is `Some(Value::NULL)`.
                // A missing slot is a bug in our column mapping, so surface it
                // as a decode error instead of silently emitting a null.
                None => {
                    return Err(external(
                        DecodeRowSnafu {
                            column: column_schema.name.clone(),
                            reason: format!("remote row is missing column index {remote_idx}"),
                        }
                        .build(),
                    ));
                }
                Some(mysql_async::Value::NULL) => {
                    // A declared NOT NULL column receiving a SQL NULL must fail
                    // here, with the column name, rather than at Arrow batch
                    // construction time.
                    if !column_schema.is_nullable() {
                        return Err(external(
                            DecodeRowSnafu {
                                column: column_schema.name.clone(),
                                reason: "non-nullable column received NULL from the remote table"
                                    .to_string(),
                            }
                            .build(),
                        ));
                    }
                    self.num_bytes += estimated_size(&column_schema.data_type, None);
                    vector.push_null();
                }
                Some(value) => {
                    let decoded = convert::decode_value(value, &column_schema.data_type, &column_schema.name)
                        .map_err(external)?;
                    self.num_bytes += estimated_size(&column_schema.data_type, Some(&decoded));
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

        let batch = if columns.is_empty() {
            RecordBatch::new_with_count(self.schema.clone(), self.num_rows)?
        } else {
            RecordBatch::new(self.schema.clone(), columns)?
        };

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

fn estimated_size(data_type: &ConcreteDataType, value: Option<&Value>) -> usize {
    const VALIDITY_BIT: usize = 1;
    match fixed_width(data_type) {
        Some(width) => width + VALIDITY_BIT,
        None => match value {
            Some(Value::String(s)) => s.as_utf8().len() + VALIDITY_BIT,
            Some(Value::Binary(b)) => b.len() + VALIDITY_BIT,
            _ => VALIDITY_BIT,
        },
    }
}

fn fixed_width(data_type: &ConcreteDataType) -> Option<usize> {
    use ConcreteDataType::{
        Boolean, Date, Decimal128, Float32, Float64, Int16, Int32, Int64, Int8, Timestamp, UInt16,
        UInt32, UInt64, UInt8,
    };
    match data_type {
        Boolean(_) | Int8(_) | UInt8(_) => Some(1),
        Int16(_) | UInt16(_) => Some(2),
        Int32(_) | UInt32(_) | Float32(_) | Date(_) => Some(4),
        Int64(_) | UInt64(_) | Float64(_) | Timestamp(_) => Some(8),
        Decimal128(_) => Some(16),
        _ => None,
    }
}
