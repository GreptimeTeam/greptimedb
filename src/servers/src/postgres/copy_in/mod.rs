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

//! PostgreSQL `COPY ... FROM STDIN` (copy-in) support.
//!
//! The wire handshake is driven by pgwire: returning [`Response::CopyIn`]
//! from a query handler makes the server send a `CopyInResponse` and enter
//! the copy-in sub-protocol; subsequent `CopyData` messages from the client
//! are dispatched to [`CopyHandler`], whose implementation below parses
//! the data stream incrementally and flushes it into the query handler as
//! [`RowInsertRequests`] batches.
//!
//! # Formats
//!
//! The supported formats are the explicit [`codec::CopyFormat`] enum —
//! `csv` (table-bound, incremental row parsing) and `influxdb`
//! (table-less, one bounded whole-payload request). Each format states
//! its own target rule and ingestion shape in the match arms of the
//! copy lifecycle below; adding a format is a new variant plus its
//! arms. Builders resolve a `FORMAT` name (and its aliases) through
//! [`REGISTERED_FORMATS`]. See the `codec` module docs.

mod arrow;
mod codec;
mod csv;
mod influxdb;
mod jsonl;
mod options;
mod otlp;
mod parse;
mod prom_write;

use std::fmt::Debug;
use std::sync::Arc;

use crate::influxdb::InfluxdbRequest;
use crate::query_handler::PipelineHandlerRef;
use api::helper::ColumnDataTypeWrapper;
use api::v1::column_def::options_from_column_schema;
use api::v1::{
    Row as GrpcRow, RowInsertRequest, RowInsertRequests, Rows as GrpcRows, SemanticType,
};
use async_trait::async_trait;
use codec::{CopyFormat, RecordParser, TableBoundCodec};
use common_catalog::consts::{TRACE_TABLE_NAME, TRACE_TABLE_NAME_SESSION_KEY};
use common_grpc::precision::Precision;
use common_query::OutputData;
use common_telemetry::{info, warn};
use datafusion::sql::sqlparser::ast::{CopyTarget, Statement as SqlParserStatement};
use datatypes::data_type::DataType;
use datatypes::schema::{ColumnSchema, SchemaRef};
use futures::{Sink, SinkExt, stream};
use influxdb::InfluxdbOptions;
use otel_arrow_rust::proto::opentelemetry::collector::metrics::v1::ExportMetricsServiceRequest;
use otlp::{OtlpOptions, OtlpSignal};
use pgwire::api::copy::CopyHandler;
use pgwire::api::results::{CopyResponse, Response, Tag};
use pgwire::api::{ClientInfo, PgWireConnectionState};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::PgWireBackendMessage;
use pgwire::messages::copy::{CopyData, CopyDone, CopyFail};
use prom_write::{PromWriteOptions, PromWriteVersion};
use prost::Message;
use session::context::QueryContextRef;
use table::metadata::TableInfo;

use crate::metrics::METRIC_POSTGRES_COPY_IN_ROWS;
use crate::postgres::PostgresServerHandlerInner;
use crate::postgres::types::PgErrorCode;
use crate::postgres::utils::convert_err;
use crate::request_memory_limiter::ServerMemoryLimiter;

/// Number of parsed rows buffered before flushing a batch to the handler.
///
/// Each flush runs the full insert pipeline (catalog lookup, on-demand
/// schema check, partition split, region write) and, without the
/// pending-rows batcher, waits for it — so larger batches mean fewer
/// round trips at the cost of linearly more memory per connection
/// (decoder batch + rows buffer). The constant doubles as the arrow
/// decoder's batch size. High-rate COPY users should enable the
/// pending-rows batcher (`global-table-batch` / `BatcherOptions.protocols`
/// containing `Postgres`), which turns flushes into enqueues and makes
/// the per-batch fixed costs negligible.
pub(crate) const COPY_IN_FLUSH_ROWS: usize = 32768;

/// Upper bound of a table-less COPY FROM STDIN payload (influxdb line
/// protocol, OTLP). One COPY statement is one bounded ingestion request
/// (the whole payload is buffered like one HTTP request body); a larger
/// import is multiple COPY statements.
pub(crate) const COPY_IN_MAX_PAYLOAD_BYTES: usize = 64 * 1024 * 1024;

/// Builds a format from the format-neutral option set.
type FormatBuilder = fn(options::CopyOptionSet) -> PgWireResult<CopyFormat>;

/// The registered COPY FROM STDIN formats: accepted `FORMAT` name to
/// builder. Unimplemented PostgreSQL text/binary formats are registered
/// to report a clear "not supported yet" instead of an unknown-format error.
const REGISTERED_FORMATS: &[(&str, FormatBuilder)] = &[
    ("csv", csv::build_csv_codec),
    ("jsonl", jsonl::build_jsonl_codec),
    ("json", jsonl::build_jsonl_codec),
    ("arrow", arrow::build_arrow_codec),
    ("influxdb", influxdb::build_influxdb_codec),
    ("influxdb_line", influxdb::build_influxdb_codec),
    ("otlp", otlp::build_otlp_codec),
    ("otlp_metrics", otlp::build_otlp_codec),
    ("promwrite", prom_write::build_prom_write_codec),
    ("text", build_unimplemented_text_codec),
    ("txt", build_unimplemented_text_codec),
    ("binary", build_unimplemented_binary_codec),
];

fn build_unimplemented_text_codec(_options: options::CopyOptionSet) -> PgWireResult<CopyFormat> {
    Err(unsupported(
        "the text format is not supported yet; use WITH (FORMAT csv)",
    ))
}

fn build_unimplemented_binary_codec(_options: options::CopyOptionSet) -> PgWireResult<CopyFormat> {
    Err(unsupported(
        "COPY FROM STDIN WITH (FORMAT binary) is not supported yet",
    ))
}

/// Resolves the format from the parsed options.
fn resolve_format(options: &mut options::CopyOptionSet) -> PgWireResult<CopyFormat> {
    let name = options.take_format()?.ok_or_else(|| {
        unsupported(
            "COPY FROM STDIN requires an explicit format in this version; use WITH (FORMAT csv)",
        )
    })?;
    let builder = REGISTERED_FORMATS
        .iter()
        .find(|(registered, _)| *registered == name)
        .map(|(_, builder)| *builder)
        .ok_or_else(|| bad_copy_data(format!("unknown COPY format \"{}\"", name)))?;
    builder(std::mem::take(options))
}

/// The parsed `COPY tbl [(columns)] FROM STDIN [(options)]` statement.
#[derive(Debug, Clone)]
pub(crate) struct CopyFromStdin {
    /// Table name parts as written (`t`, `schema.t`, `catalog.schema.t`);
    /// quoted names keep their case.
    table: Vec<String>,
    /// Requested column list; empty means all columns in table order.
    columns: Vec<String>,
    /// The resolved format with its validated options.
    format: CopyFormat,
}

fn copy_in_error(code: PgErrorCode, message: impl Into<String>) -> PgWireError {
    PgWireError::UserError(Box::new(code.to_err_info(message.into())))
}

pub(super) fn unsupported(message: impl Into<String>) -> PgWireError {
    copy_in_error(PgErrorCode::Ec0A000, message)
}

fn bad_copy_data(message: impl Into<String>) -> PgWireError {
    copy_in_error(PgErrorCode::Ec22P04, message)
}

fn invalid_text_repr(message: impl Into<String>) -> PgWireError {
    copy_in_error(PgErrorCode::Ec22P02, message)
}

pub(crate) fn copy_in_must_run_alone_error() -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_string(),
        PgErrorCode::Ec0A000.code(),
        "COPY FROM STDIN must be executed alone".to_string(),
    )))
}

/// Parses a leading `COPY ... FROM STDIN` statement from SQL text.
///
/// The statement is parsed textually — not through sqlparser — because the
/// `WITH (...)` option list is open: formats may accept options beyond
/// sqlparser's COPY grammar, and unknown options must reach the format
/// codec instead of failing statement parsing. Returns `Ok(None)` when
/// the string does not lead with a copy-in statement.
pub(crate) fn copy_from_stdin_from_sql(sql: &str) -> PgWireResult<Option<CopyFromStdin>> {
    let raw = match parse::scan_copy_from_stdin(sql)? {
        parse::CopyInScan::NotCopyIn => return Ok(None),
        parse::CopyInScan::CopyIn(raw) => raw,
    };
    let mut option_set = raw.options;
    let format = resolve_format(&mut option_set)?;
    Ok(Some(CopyFromStdin {
        table: raw.table,
        columns: raw.columns,
        format,
    }))
}

/// Whether an already-parsed statement is a `COPY ... FROM STDIN`; used to
/// reject copy-in statements trailing other statements, where the textual
/// scanner (which only sees statement-leading COPYs) does not see them.
pub(crate) fn is_copy_from_stdin(statement: &SqlParserStatement) -> bool {
    matches!(
        statement,
        SqlParserStatement::Copy {
            to: false,
            target: CopyTarget::Stdin,
            ..
        }
    )
}

/// Resolves table name parts against the query context defaults.
fn resolve_table_name(
    query_ctx: &QueryContextRef,
    parts: &[String],
) -> PgWireResult<(String, String, String)> {
    match parts {
        [table] => Ok((
            query_ctx.current_catalog().to_string(),
            query_ctx.current_schema(),
            table.clone(),
        )),
        [schema, table] => Ok((
            query_ctx.current_catalog().to_string(),
            schema.clone(),
            table.clone(),
        )),
        [catalog, schema, table] => Ok((catalog.clone(), schema.clone(), table.clone())),
        _ => Err(unsupported(format!(
            "invalid table name for COPY: {}",
            parts.join(".")
        ))),
    }
}

/// Per-connection state of an in-progress COPY FROM STDIN.
///
/// The payload shape follows the codec: table-bound formats (csv) parse
/// records into rows against the targeted table's columns; table-less
/// formats (influxdb line protocol) split the stream into complete
/// lines that each route to their own measurement, and flush through
/// the handler's line-protocol pipeline — the same one the HTTP write
/// endpoint uses.
pub(crate) struct CopyInState {
    query_ctx: QueryContextRef,
    payload: Payload,
    /// Reservations against the shared request-memory limiter for the
    /// bytes buffered by a table-less payload, acquired per CopyData
    /// chunk as it arrives and held until the buffer is flushed or the
    /// copy ends — the receive-stage counterpart of HTTP's
    /// body-accounting middleware, so concurrent unfinished COPY
    /// requests cannot retain unaccounted memory.
    memory_guards: Vec<
        common_memory_manager::MemoryGuard<crate::request_memory_metrics::RequestMemoryMetrics>,
    >,
    /// Payload bytes already covered by limiter grants, in the
    /// limiter's KiB grant granularity. Chunks acquire only the
    /// uncovered delta (`buffered_total - granted`), so rounding adds at
    /// most one KiB for the whole stream instead of one per chunk.
    granted_bytes: u64,
    /// The resolved codec's name, for logging.
    codec_name: &'static str,
    /// The wire format code of the resolved codec.
    codec_format_code: i8,
    rows_written: u64,
}

/// The receive-stage reservation for one buffered payload, handed to
/// the flush that consumes the payload: the guards must outlive the
/// payload's bytes (they may coexist with the decode buffer or wait on
/// decode quota), and `charged_bytes` is the payload's pre-charged
/// reservation to hand to decode-side fits checks.
pub(crate) struct BufferedReservation {
    /// Held only for its Drop (the reservation release); never read.
    #[allow(dead_code)]
    guards: Vec<
        common_memory_manager::MemoryGuard<crate::request_memory_metrics::RequestMemoryMetrics>,
    >,
    granted_bytes: u64,
}

impl BufferedReservation {
    /// The payload bytes already charged at the receive stage; decode
    /// paths take this as their pre-held reservation.
    pub(crate) fn charged_bytes(&self) -> u64 {
        self.granted_bytes
    }
}

enum Payload {
    /// Table-bound Arrow IPC batches, written without protobuf row conversion.
    Arrow(Box<arrow::ArrowCopyState>),
    /// Table-bound rows: parsed against `grpc_schema` and flushed to
    /// `table_name` via `copy_in_insert`. The schema is cloned into each
    /// flushed batch: `Rows.schema` is an owned `Vec` in the proto
    /// request, so one copy per request is inherent.
    Table {
        grpc_schema: Vec<api::v1::ColumnSchema>,
        table_name: String,
        parser: Box<dyn RecordParser + Send>,
        /// Parsed rows not yet flushed to the handler.
        rows: Vec<GrpcRow>,
        memory: TableCopyMemory,
    },
    /// The whole buffered influxdb line-protocol payload with its
    /// request precision. One COPY statement is one bounded ingestion
    /// request, buffered until `CopyDone` — exactly like one HTTP write
    /// body — and record boundaries belong to the line-protocol parser,
    /// never to the COPY layer.
    Influxdb {
        data: Vec<u8>,
        precision: Option<Precision>,
    },
    /// The whole buffered OTLP payload with its validated options,
    /// decoded and completed as one request at `CopyDone` through the
    /// matching OTLP pipeline.
    Otlp { data: Vec<u8>, options: OtlpOptions },
    /// The whole buffered Prometheus Remote Write body with its
    /// validated options, completed as one request at `CopyDone`
    /// through the shared remote-write ingestion core.
    PromWrite {
        data: Vec<u8>,
        options: PromWriteOptions,
    },
}

/// Estimated working-set reservation for a row-based COPY stream.
///
/// Arrow retains decoder capacity across flushes, so the reservation follows the
/// largest batch seen, not the cumulative import size. It stays held through
/// inserts and until the decoder is dropped (including errors and disconnects).
/// This is admission accounting, not an exact bound on allocator/RSS usage.
struct TableCopyMemory {
    limiter: ServerMemoryLimiter,
    guards: Vec<
        common_memory_manager::MemoryGuard<crate::request_memory_metrics::RequestMemoryMetrics>,
    >,
    granted_bytes: u64,
    fixed_bytes: u64,
    batch_bytes: u64,
}

impl TableCopyMemory {
    // Covers variable-sized decoder/tape buffers, capacity growth, Arrow arrays
    // and protobuf strings coexisting during conversion. Fixed per-cell storage
    // is charged separately so sparse JSON and empty CSV fields are not free.
    const INPUT_EXPANSION: u64 = 16;
    // Bound speculative admission and CPU work before checking for a full batch.
    const DECODE_BYTES: usize = 8 * 1024;

    async fn new(limiter: ServerMemoryLimiter, columns: usize) -> PgWireResult<Self> {
        // Two copies of fixed cell/row storage plus Arrow validity/offset buffers
        // and CSV's field offsets/initial data allocation. Reserve before creating
        // the decoder, including for streams that never send a complete record.
        let cell_bytes = std::mem::size_of::<api::v1::Value>() + 32;
        let fixed_bytes = columns
            .checked_mul(cell_bytes)
            .and_then(|bytes| bytes.checked_add(std::mem::size_of::<GrpcRow>()))
            .and_then(|bytes| bytes.checked_mul(COPY_IN_FLUSH_ROWS * 2))
            .and_then(|bytes| bytes.checked_add(1024))
            .ok_or_else(|| bad_copy_data("COPY decode memory estimate overflow"))?
            as u64;
        let mut memory = Self {
            limiter,
            guards: Vec::new(),
            granted_bytes: 0,
            fixed_bytes,
            batch_bytes: 0,
        };
        memory.reserve_input(0, 0).await?;
        Ok(memory)
    }

    async fn reserve_input(&mut self, bytes: usize, wire_bytes: u64) -> PgWireResult<()> {
        let required = self
            .batch_bytes
            .checked_add(bytes as u64)
            .and_then(|bytes| bytes.checked_mul(Self::INPUT_EXPANSION))
            .and_then(|bytes| bytes.checked_add(self.fixed_bytes))
            .ok_or_else(|| bad_copy_data("COPY decode memory estimate overflow"))?;
        let uncovered = required.saturating_sub(self.granted_bytes);
        if uncovered != 0 {
            // Unlimited managers do not round grants. Keep tiny CopyData
            // fragments from adding one guard per byte even in that mode.
            let uncovered = uncovered.max(1024);
            let held = self
                .granted_bytes
                .checked_add(wire_bytes)
                .ok_or_else(|| bad_copy_data("COPY decode memory estimate overflow"))?;
            self.limiter
                .check_request_fits(uncovered, held)
                .map_err(convert_err)?;
            let guard = self.limiter.acquire(uncovered).await.map_err(|error| {
                convert_err(crate::prom_remote_write::map_memory_acquire_timeout(error))
            })?;
            self.granted_bytes += guard.granted_bytes();
            self.guards.push(guard);
        }
        Ok(())
    }
}

impl CopyInState {
    async fn new(
        codec: &dyn TableBoundCodec,
        columns: Vec<String>,
        table_info: Arc<TableInfo>,
        query_ctx: QueryContextRef,
        table_name: String,
        memory_limiter: ServerMemoryLimiter,
    ) -> PgWireResult<Self> {
        let schema = table_info.meta.schema.clone();
        let timestamp_index = schema.timestamp_index();

        let columns = resolve_columns(codec, columns, &schema, &table_info)?;
        if let Some(ts_index) = timestamp_index {
            let ts_column = &schema.column_schemas()[ts_index];
            if !columns.iter().any(|column| column.name == ts_column.name) {
                return Err(unsupported(format!(
                    "COPY FROM STDIN requires the time index column \"{}\" in the column list",
                    ts_column.name
                )));
            }
        }
        let memory = TableCopyMemory::new(memory_limiter, columns.len()).await?;
        let parser = codec.create_parser(&columns, query_ctx.timezone().to_string().as_str())?;

        let grpc_schema = columns
            .iter()
            .map(|column| build_grpc_column_schema(column, &table_info))
            .collect::<PgWireResult<Vec<_>>>()?;

        Ok(Self {
            query_ctx,
            payload: Payload::Table {
                grpc_schema,
                table_name,
                parser,
                rows: Vec::new(),
                memory,
            },
            memory_guards: Vec::new(),
            granted_bytes: 0,
            codec_name: "csv",
            codec_format_code: 0,
            rows_written: 0,
        })
    }

    /// State for the influxdb format: the whole payload is buffered and
    /// completed once at `CopyDone` through the line-protocol pipeline.
    fn new_influxdb(options: &InfluxdbOptions, query_ctx: QueryContextRef) -> Self {
        Self {
            query_ctx,
            payload: Payload::Influxdb {
                data: Vec::new(),
                precision: options.precision,
            },
            memory_guards: Vec::new(),
            granted_bytes: 0,
            codec_name: "influxdb",
            codec_format_code: 0,
            rows_written: 0,
        }
    }

    /// State for the OTLP format: the whole binary payload is buffered
    /// and completed once at `CopyDone` through the matching OTLP
    /// pipeline. Binary: the advertised wire format code is 1.
    fn new_otlp(options: OtlpOptions, query_ctx: QueryContextRef) -> Self {
        Self {
            query_ctx,
            payload: Payload::Otlp {
                data: Vec::new(),
                options,
            },
            memory_guards: Vec::new(),
            granted_bytes: 0,
            codec_name: "otlp",
            codec_format_code: 1,
            rows_written: 0,
        }
    }

    /// State for the promwrite format: the whole request body is
    /// buffered and completed once at `CopyDone` through the shared
    /// remote-write ingestion core. Binary: the advertised wire format
    /// code is 1.
    fn new_prom_write(options: &PromWriteOptions, query_ctx: QueryContextRef) -> Self {
        Self {
            query_ctx,
            payload: Payload::PromWrite {
                data: Vec::new(),
                options: options.clone(),
            },
            memory_guards: Vec::new(),
            granted_bytes: 0,
            codec_name: "promwrite",
            codec_format_code: 1,
            rows_written: 0,
        }
    }

    /// The wire format code advertised to the client.
    pub(crate) fn format_code(&self) -> i8 {
        self.codec_format_code
    }

    /// Whether the payload buffers raw CopyData bytes until `CopyDone`
    /// (the table-less formats); such bytes are charged against the
    /// shared request-memory limiter as they arrive.
    fn buffers_payload(&self) -> bool {
        matches!(
            self.payload,
            Payload::Influxdb { .. } | Payload::Otlp { .. } | Payload::PromWrite { .. }
        )
    }

    /// The payload bytes currently buffered (charged or not).
    fn buffered_bytes(&self) -> u64 {
        match &self.payload {
            Payload::Influxdb { data, .. }
            | Payload::Otlp { data, .. }
            | Payload::PromWrite { data, .. } => data.len() as u64,
            Payload::Table { .. } | Payload::Arrow(_) => 0,
        }
    }

    /// The payload bytes already covered by grants (KiB-aligned).
    fn granted_bytes(&self) -> u64 {
        self.granted_bytes
    }

    /// Retains a new reservation and records the granted coverage: the
    /// limiter grants in KiB granularity, so coverage rounds the
    /// buffered total up to the next KiB — subsequent chunks inside
    /// that headroom need no acquisition.
    fn charge_chunk(
        &mut self,
        guard: common_memory_manager::MemoryGuard<
            crate::request_memory_metrics::RequestMemoryMetrics,
        >,
        buffered_total: u64,
    ) {
        self.memory_guards.push(guard);
        self.granted_bytes = buffered_total.div_ceil(1024) * 1024;
    }

    /// Detaches the buffered-payload reservation so a flush can hold it
    /// alongside the payload until the payload's memory is actually
    /// consumed; see [`Self::memory_guards`].
    fn take_reservation(&mut self) -> BufferedReservation {
        BufferedReservation {
            guards: std::mem::take(&mut self.memory_guards),
            granted_bytes: std::mem::take(&mut self.granted_bytes),
        }
    }

    /// Feeds a `CopyData` chunk into the parser and buffers parsed rows
    /// (csv) or payload bytes (influxdb).
    fn feed(&mut self, data: &[u8]) -> PgWireResult<()> {
        match &mut self.payload {
            Payload::Table { .. } => return Err(bad_copy_data("Table requires accounted decode")),
            // Arrow's asynchronous feed writes each complete batch immediately.
            Payload::Arrow(_) => return Err(bad_copy_data("Arrow requires asynchronous feed")),
            Payload::Influxdb { data: buffered, .. }
            | Payload::Otlp { data: buffered, .. }
            | Payload::PromWrite { data: buffered, .. } => {
                if buffered.len() + data.len() > COPY_IN_MAX_PAYLOAD_BYTES {
                    return Err(bad_copy_data(format!(
                        "COPY FROM STDIN payload exceeds the {} limit; split the import into multiple COPY statements",
                        COPY_IN_MAX_PAYLOAD_BYTES
                    )));
                }
                buffered.extend_from_slice(data);
            }
        }
        Ok(())
    }

    /// Parses the buffered tail at `CopyDone`; a final record without a
    /// trailing newline is accepted.
    fn finish(&mut self) -> PgWireResult<()> {
        match &mut self.payload {
            Payload::Table { parser, rows, .. } => rows.extend(parser.finish()?),
            // Finalized asynchronously by on_copy_done.
            Payload::Arrow(_) => {}
            // The whole payload is one request body; nothing to finalize.
            Payload::Influxdb { .. } | Payload::Otlp { .. } | Payload::PromWrite { .. } => {}
        }
        Ok(())
    }

    fn should_flush(&self) -> bool {
        match &self.payload {
            Payload::Table { rows, .. } => rows.len() >= COPY_IN_FLUSH_ROWS,
            Payload::Arrow(_) => false,
            // Completed once at CopyDone, not mid-stream.
            Payload::Influxdb { .. } | Payload::Otlp { .. } | Payload::PromWrite { .. } => false,
        }
    }

    /// Takes the buffered rows as an insert request, if any.
    fn take_requests(&mut self) -> Option<RowInsertRequests> {
        let Payload::Table {
            grpc_schema,
            table_name,
            rows,
            ..
        } = &mut self.payload
        else {
            return None;
        };
        if rows.is_empty() {
            return None;
        }
        let rows = GrpcRows {
            schema: grpc_schema.clone(),
            rows: std::mem::take(rows),
        };
        Some(RowInsertRequests {
            inserts: vec![RowInsertRequest {
                table_name: table_name.clone(),
                rows: Some(rows),
            }],
        })
    }

    /// Takes the buffered OTLP payload and its options, if any.
    fn take_otlp(&mut self) -> Option<(OtlpOptions, Vec<u8>, BufferedReservation)> {
        let Payload::Otlp { options, data } = &mut self.payload else {
            return None;
        };
        if data.is_empty() {
            return None;
        }
        Some((
            options.clone(),
            std::mem::take(data),
            self.take_reservation(),
        ))
    }

    /// Takes the buffered remote-write body and its options, if any.
    fn take_prom_write(&mut self) -> Option<(PromWriteOptions, Vec<u8>, BufferedReservation)> {
        let Payload::PromWrite { options, data } = &mut self.payload else {
            return None;
        };
        if data.is_empty() {
            return None;
        }
        Some((
            options.clone(),
            std::mem::take(data),
            self.take_reservation(),
        ))
    }

    /// Takes the buffered payload as one line-protocol request body, if
    /// any — the same bytes an HTTP write of the same records would
    /// carry.
    fn take_lines(&mut self) -> PgWireResult<Option<(InfluxdbRequest, BufferedReservation)>> {
        let Payload::Influxdb { data, precision } = &mut self.payload else {
            return Ok(None);
        };
        if data.is_empty() {
            return Ok(None);
        }
        let request = InfluxdbRequest {
            precision: *precision,
            lines: String::from_utf8(std::mem::take(data)).map_err(|e| {
                bad_copy_data(format!("COPY FROM STDIN data is not valid UTF-8: {e}"))
            })?,
        };
        Ok(Some((request, self.take_reservation())))
    }
}

fn resolve_columns(
    codec: &dyn TableBoundCodec,
    columns: Vec<String>,
    schema: &SchemaRef,
    table_info: &Arc<TableInfo>,
) -> PgWireResult<Vec<ColumnSchema>> {
    let table_name = &table_info.name;
    let selected: Vec<ColumnSchema> = if columns.is_empty() {
        schema.column_schemas().to_vec()
    } else {
        let mut selected = Vec::with_capacity(columns.len());
        for name in &columns {
            let column = schema
                .column_schema_by_name(name)
                .ok_or_else(|| {
                    copy_in_error(
                        PgErrorCode::Ec42703,
                        format!(
                            "column \"{}\" of relation \"{}\" does not exist",
                            name, table_name
                        ),
                    )
                })?
                .clone();
            if selected.iter().any(|c: &ColumnSchema| &c.name == name) {
                return Err(bad_copy_data(format!(
                    "column \"{}\" specified more than once",
                    name
                )));
            }
            selected.push(column);
        }
        selected
    };

    for column in &selected {
        codec.check_column_type(&column.data_type)?;
    }

    Ok(selected)
}

/// Derives a column's semantic type the same way the flow adapter does
/// (`flow::adapter::util::column_schemas_to_proto`): the time-index flag
/// for timestamps, primary-key membership for tags.
fn semantic_type_of(table_info: &TableInfo, column: &ColumnSchema) -> PgWireResult<SemanticType> {
    let meta = &table_info.meta;
    if column.is_time_index() {
        Ok(SemanticType::Timestamp)
    } else if meta
        .primary_key_indices
        .iter()
        .any(|&index| meta.schema.column_schemas()[index].name == column.name)
    {
        Ok(SemanticType::Tag)
    } else {
        Ok(SemanticType::Field)
    }
}

fn build_grpc_column_schema(
    column: &ColumnSchema,
    table_info: &Arc<TableInfo>,
) -> PgWireResult<api::v1::ColumnSchema> {
    let (datatype, datatype_extension) = ColumnDataTypeWrapper::try_from(column.data_type.clone())
        .map_err(|_| {
            unsupported(format!(
                "column type \"{}\" is not supported for COPY FROM STDIN yet",
                column.data_type.name()
            ))
        })?
        .to_parts();
    Ok(api::v1::ColumnSchema {
        column_name: column.name.clone(),
        datatype: datatype as i32,
        semantic_type: semantic_type_of(table_info, column)? as i32,
        datatype_extension,
        options: options_from_column_schema(column),
    })
}

// ---------------------------------------------------------------------------
// pgwire handlers
// ---------------------------------------------------------------------------

impl PostgresServerHandlerInner {
    /// Prepares a COPY FROM STDIN: resolves the target table, validates
    /// columns and stashes the per-connection copy state.
    pub(crate) async fn begin_copy_in(&self, stmt: CopyFromStdin) -> PgWireResult<Response> {
        // The batching-aware context routes copy-in flushes through the
        // pending-rows batcher like SQL INSERT when it is enabled for
        // this connection.
        let query_ctx = self.new_query_context();

        // Each format states its own target rule and ingestion shape
        // explicitly.
        let state = match &stmt.format {
            // Row-based table-bound formats parse records against the
            // target columns and insert them in batches.
            CopyFormat::Csv(codec) => self.table_bound_state(&stmt, codec).await?,
            CopyFormat::Jsonl(codec) => self.table_bound_state(&stmt, codec).await?,
            CopyFormat::Arrow(options) => {
                if stmt.table.is_empty() {
                    return Err(unsupported("format arrow requires a target table"));
                }
                if !stmt.columns.is_empty() {
                    return Err(unsupported(
                        "format arrow uses IPC field names; omit the COPY column list",
                    ));
                }
                let (catalog, schema, table) = resolve_table_name(&query_ctx, &stmt.table)?;
                // Validate against the original context, including COPY's
                // cross-catalog policy, before changing the target scope.
                if self
                    .copy_in_handler
                    .copy_in_table(&catalog, &schema, &table, query_ctx.clone())
                    .await
                    .map_err(convert_err)?
                    .is_none()
                {
                    return Err(copy_in_error(
                        PgErrorCode::Ec42P01,
                        format!("relation \"{}\" does not exist", stmt.table.join(".")),
                    ));
                }
                let query_ctx =
                    options.query_context(&adjust_query_context(query_ctx, &catalog, &schema));
                let arrow = arrow::ArrowCopyState::new(
                    table,
                    &self.copy_in_handler,
                    query_ctx.clone(),
                    self.memory_limiter.clone(),
                )
                .await?;
                CopyInState {
                    query_ctx,
                    payload: Payload::Arrow(Box::new(arrow)),
                    memory_guards: Vec::new(),
                    granted_bytes: 0,
                    codec_name: "arrow",
                    codec_format_code: 1,
                    rows_written: 0,
                }
            }
            // influxdb forbids a target table — each line names its own
            // measurement — and completes through the line-protocol
            // pipeline the HTTP write endpoints use; the server must be
            // wired for it.
            CopyFormat::Influxdb(options) => {
                if !stmt.table.is_empty() {
                    return Err(unsupported(format!(
                        "format influxdb routes each record to its own table; \"COPY {} FROM STDIN\" must not name one",
                        stmt.table.join(".")
                    )));
                }
                if self.influxdb_handler.is_none() {
                    return Err(unsupported(
                        "this server is not wired for the influxdb line protocol format",
                    ));
                }
                CopyInState::new_influxdb(options, query_ctx)
            }
            // OTLP metrics route to their own tables. Logs and traces
            // accept a destination without requiring it to exist yet.
            CopyFormat::Otlp(options) => {
                if !stmt.columns.is_empty() {
                    return Err(unsupported("format otlp does not support a column list"));
                }
                if self.otlp_handler.is_none() {
                    return Err(unsupported("this server is not wired for the otlp format"));
                }
                let (options, target_ctx) =
                    resolve_otlp_target(options, &stmt.table, query_ctx.clone())?;
                if target_ctx.current_catalog() != query_ctx.current_catalog()
                    || target_ctx.current_schema() != query_ctx.current_schema()
                {
                    // The same cross-catalog restriction SQL INSERT and
                    // the csv format enforce — must run against the
                    // ORIGINAL session context, before the user-provider
                    // scope check and before any data is transferred.
                    self.copy_in_handler
                        .copy_in_validate_scope(
                            target_ctx.current_catalog(),
                            &target_ctx.current_schema(),
                            query_ctx.clone(),
                        )
                        .await
                        .map_err(convert_err)?;
                    if let Some(source) = &self.auth_source {
                        source
                            .authorize_database(
                                target_ctx.current_catalog(),
                                &target_ctx.current_schema(),
                                &query_ctx.current_user(),
                            )
                            .await?;
                    }
                }
                CopyInState::new_otlp(options, target_ctx)
            }
            // promwrite forbids a target table too — the body's series
            // carry their own metric names — and completes through the
            // shared remote-write ingestion core; the server must be
            // wired.
            CopyFormat::PromWrite(options) => {
                if !stmt.table.is_empty() {
                    return Err(unsupported(format!(
                        "format promwrite routes each series to its own metric table; \"COPY {} FROM STDIN\" must not name one",
                        stmt.table.join(".")
                    )));
                }
                if self.prom_write_state.is_none() {
                    return Err(unsupported(
                        "this server is not wired for the promwrite format",
                    ));
                }
                CopyInState::new_prom_write(options, query_ctx)
            }
        };
        let num_columns = match &state.payload {
            Payload::Table { grpc_schema, .. } => grpc_schema.len(),
            // The IPC schema arrives after the COPY handshake.
            Payload::Arrow(_) => 0,
            // No fixed column set: the format routes per record.
            Payload::Influxdb { .. } | Payload::Otlp { .. } | Payload::PromWrite { .. } => 0,
        };
        let format_code = state.format_code();

        *self.copy_in_state.lock().await = Some(state);

        Ok(Response::CopyIn(CopyResponse::new(
            format_code,
            num_columns,
            stream::empty(),
        )))
    }

    /// Resolves the target table and builds the table-bound copy state
    /// shared by csv and jsonl.
    async fn table_bound_state(
        &self,
        stmt: &CopyFromStdin,
        codec: &dyn TableBoundCodec,
    ) -> PgWireResult<CopyInState> {
        let query_ctx = self.new_query_context();
        if stmt.table.is_empty() {
            return Err(unsupported(format!(
                "format {} requires a table: COPY tbl [(cols)] FROM STDIN WITH (FORMAT {})",
                stmt.format.name(),
                stmt.format.name()
            )));
        }
        let (catalog, schema, table) = resolve_table_name(&query_ctx, &stmt.table)?;

        let Some(table_info) = self
            .copy_in_handler
            .copy_in_table(&catalog, &schema, &table, query_ctx.clone())
            .await
            .map_err(convert_err)?
        else {
            return Err(copy_in_error(
                PgErrorCode::Ec42P01,
                format!("relation \"{}\" does not exist", stmt.table.join(".")),
            ));
        };

        // The insert path resolves the bare table name against the query
        // context, so point the context at the resolved catalog and schema
        // when the COPY target is qualified.
        let query_ctx = adjust_query_context(query_ctx, &catalog, &schema);

        CopyInState::new(
            codec,
            stmt.columns.clone(),
            table_info,
            query_ctx,
            table,
            self.memory_limiter.clone(),
        )
        .await
    }

    /// Flushes the buffered payload of an in-progress copy. Returns the
    /// number of rows written by the flushed batch.
    async fn flush_copy_in(&self, state: &mut CopyInState) -> PgWireResult<u64> {
        let query_ctx = state.query_ctx.clone();
        // The buffered influxdb payload is completed through the
        // line-protocol pipeline — the same handler the HTTP write
        // endpoints use, so conversion, permissions, interceptors,
        // timestamp alignment and inserts are identical to an HTTP
        // write of the same bytes. The context stays this connection's
        // postgres context.
        if let Some((options, bytes, reservation)) = state.take_otlp() {
            let Some(otlp_handler) = &self.otlp_handler else {
                return Err(unsupported("this server is not wired for the otlp format"));
            };
            // The reservation outlives the payload: it is dropped when
            // the payload's bytes are consumed below.
            let rows = self
                .flush_otlp(
                    otlp_handler,
                    options,
                    bytes,
                    reservation.charged_bytes(),
                    query_ctx,
                )
                .await?;
            drop(reservation);
            return Ok(rows);
        }
        if let Some((options, bytes, reservation)) = state.take_prom_write() {
            let Some(prom_write_state) = &self.prom_write_state else {
                return Err(unsupported(
                    "this server is not wired for the promwrite format",
                ));
            };
            return self
                .flush_prom_write(prom_write_state, options, bytes, reservation, query_ctx)
                .await;
        }
        if let Some((request, reservation)) = state.take_lines()? {
            let Some(influxdb_handler) = &self.influxdb_handler else {
                return Err(unsupported(
                    "this server is not wired for the influxdb line protocol format",
                ));
            };
            let output = influxdb_handler
                .exec(request, query_ctx)
                .await
                .map_err(convert_err)?;
            drop(reservation);
            return match output.data {
                OutputData::AffectedRows(rows) => Ok(rows as u64),
                OutputData::RecordBatches(_) | OutputData::Stream(_) => Ok(0),
            };
        }
        let Some(requests) = state.take_requests() else {
            return Ok(0);
        };
        let output = self
            .copy_in_handler
            .copy_in_insert(requests, query_ctx)
            .await
            .map_err(convert_err)?;
        match output.data {
            OutputData::AffectedRows(rows) => Ok(rows as u64),
            OutputData::RecordBatches(_) | OutputData::Stream(_) => {
                warn!("unexpected output from COPY FROM STDIN insert");
                Ok(0)
            }
        }
    }

    /// Completes an OTLP copy through the pipeline the matching
    /// OTLP/HTTP endpoint uses — the payload is decoded with the same
    /// protobuf types and handed to the same handler methods with the
    /// same context preparation, so both ingestion routes are
    /// identical. The context stays this connection's postgres context
    /// apart from the signal-specific preparation below.
    async fn flush_otlp(
        &self,
        otlp_handler: &crate::query_handler::OpenTelemetryProtocolHandlerRef,
        options: OtlpOptions,
        bytes: Vec<u8>,
        body_reserved_bytes: u64,
        query_ctx: QueryContextRef,
    ) -> PgWireResult<u64> {
        use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
        use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
        use pipeline::{GreptimePipelineParams, PipelineWay, SelectInfo};
        use session::protocol_ctx::{MetricType, OtlpMetricCtx, ProtocolCtx};

        // The HTTP routes derive the pipeline handler from the same
        // protocol handler via trait upcasting; so does the copy.
        let pipeline_handler: PipelineHandlerRef = otlp_handler.clone();
        match options.signal {
            OtlpSignal::Metrics {
                promote_all_resource_attrs,
                resource_attrs,
                promote_scope_attrs,
                translation_strategy,
            } => {
                // Same context preparation as the HTTP metrics route.
                let mut ctx = (*query_ctx).clone();
                ctx.set_protocol_ctx(ProtocolCtx::OtlpMetric(OtlpMetricCtx {
                    promote_all_resource_attrs,
                    resource_attrs,
                    promote_scope_attrs,
                    with_metric_engine: self.otlp_with_metric_engine,
                    // Set by the frontend from its config.
                    resource_info: false,
                    metric_type: MetricType::Init,
                    metric_translation_strategy: translation_strategy,
                }));
                let request = ExportMetricsServiceRequest::decode(bytes.as_slice())
                    .map_err(|e| bad_copy_data(format!("invalid OTLP payload: {e}")))?;
                let outcome = otlp_handler
                    .metrics(
                        request,
                        Arc::new(ctx),
                        Some((self.memory_limiter.clone(), body_reserved_bytes)),
                    )
                    .await
                    .map_err(convert_err)?;
                // Same failure semantics as the HTTP route: a payload
                // whose points were all rejected is an error, not a
                // successful `COPY 0`.
                if outcome.accepted_data_points == 0 && outcome.rejected_data_points > 0 {
                    return Err(all_rejected_error(
                        "data points",
                        outcome.rejected_data_points,
                        outcome.error_message.as_deref(),
                    ));
                }
                // Rows are the accepted data points.
                Ok(outcome.accepted_data_points.max(0) as u64)
            }
            OtlpSignal::Traces {
                table_name,
                pipeline_name,
                pipeline_version,
                pipeline_params,
            } => {
                let table_name = table_name.unwrap_or_else(|| TRACE_TABLE_NAME.to_string());
                // Same context preparation as the HTTP traces route.
                let mut ctx = (*query_ctx).clone();
                ctx.set_extension(TRACE_TABLE_NAME_SESSION_KEY, &table_name);
                let pipeline = PipelineWay::from_name_and_default(
                    pipeline_name.as_deref(),
                    pipeline_version.as_deref(),
                    None,
                )
                .map_err(convert_err)?;
                let request = ExportTraceServiceRequest::decode(bytes.as_slice())
                    .map_err(|e| bad_copy_data(format!("invalid OTLP payload: {e}")))?;
                let outcome = otlp_handler
                    .traces(
                        pipeline_handler,
                        request,
                        pipeline,
                        GreptimePipelineParams::from_params(pipeline_params.as_deref()),
                        table_name,
                        Arc::new(ctx),
                    )
                    .await
                    .map_err(convert_err)?;
                // Same failure semantics as the HTTP route.
                if outcome.accepted_spans == 0 && outcome.rejected_spans > 0 {
                    return Err(all_rejected_error(
                        "spans",
                        outcome.rejected_spans as i64,
                        outcome.error_message.as_deref(),
                    ));
                }
                // Rows are the accepted spans.
                Ok(outcome.accepted_spans as u64)
            }
            OtlpSignal::Logs {
                table_name,
                pipeline_name,
                pipeline_version,
                pipeline_params,
                extract_keys,
            } => {
                let table_name = table_name.unwrap_or_else(|| "opentelemetry_logs".to_string());
                // Same select semantics as the HTTP extract-keys header.
                let select_info = match extract_keys.as_deref() {
                    Some(keys) if !keys.is_empty() => SelectInfo::from(keys.to_string()),
                    _ => Default::default(),
                };
                let pipeline = PipelineWay::from_name_and_default(
                    pipeline_name.as_deref(),
                    pipeline_version.as_deref(),
                    Some(PipelineWay::OtlpLogDirect(Box::new(select_info))),
                )
                .map_err(convert_err)?;
                let request = ExportLogsServiceRequest::decode(bytes.as_slice())
                    .map_err(|e| bad_copy_data(format!("invalid OTLP payload: {e}")))?;
                let outputs = otlp_handler
                    .logs(
                        pipeline_handler,
                        request,
                        pipeline,
                        GreptimePipelineParams::from_params(pipeline_params.as_deref()),
                        table_name,
                        query_ctx,
                    )
                    .await
                    .map_err(convert_err)?;
                // Rows are the affected rows across the outputs.
                Ok(outputs
                    .iter()
                    .map(|o| match &o.data {
                        OutputData::AffectedRows(rows) => *rows as u64,
                        _ => 0,
                    })
                    .sum())
            }
        }
    }

    /// Completes a promwrite copy through the shared remote-write
    /// ingestion core: the same decode, validation, conversion and
    /// write paths the `/v1/prometheus/write` endpoint runs, with the
    /// same context extensions, under the connection's regular postgres
    /// query context. No parsing or conversion lives at the copy layer.
    async fn flush_prom_write(
        &self,
        prom_write_state: &crate::http::prom_store::PromStoreState,
        options: PromWriteOptions,
        bytes: Vec<u8>,
        reservation: BufferedReservation,
        query_ctx: QueryContextRef,
    ) -> PgWireResult<u64> {
        use crate::http::prom_store::{
            ingest_remote_write_v1, ingest_remote_write_v2, stamp_remote_write_extensions,
        };
        use crate::prom_remote_write::{REMOTE_WRITE_V1_VERSION, REMOTE_WRITE_V2_VERSION};

        let version_str = match options.version {
            PromWriteVersion::V1 => REMOTE_WRITE_V1_VERSION,
            PromWriteVersion::V2 => REMOTE_WRITE_V2_VERSION,
        };
        // Same context extensions the HTTP endpoint stamps (physical
        // table, semantic identity); the channel stays this
        // connection's postgres channel.
        // Capture pipeline presence BEFORE the options are moved: the
        // v2 core rejects pipelines combined with exemplars, exactly
        // like the HTTP route, so the same payload+options cannot be
        // silently ingested without the pipeline over COPY.
        let has_pipeline = options.pipeline_name.is_some();
        let mut ctx = (*query_ctx).clone();
        stamp_remote_write_extensions(&mut ctx, options.physical_table.as_deref(), version_str);
        let pipeline_info = crate::http::extractor::PipelineInfo {
            pipeline_name: options.pipeline_name,
            pipeline_version: options.pipeline_version,
            pipeline_params: Default::default(),
        };
        let body = axum::body::Bytes::from(bytes);
        // The db labels the write-progress counters. The body's receive
        // reservation stays held for the whole ingestion (the guards in
        // `reservation` drop when it does), and its charged bytes ride
        // along as the decode-side pre-held reservation — the same
        // contract HTTP's body accounting passes downstream.
        let body_reserved_bytes = reservation.charged_bytes();
        let db = query_ctx.get_db_string();
        match options.version {
            PromWriteVersion::V1 => {
                let result = ingest_remote_write_v1(
                    prom_write_state,
                    &db,
                    pipeline_info,
                    options.zstd,
                    Arc::new(ctx),
                    body,
                    body_reserved_bytes,
                )
                .await
                .map_err(|error| convert_err(error.error))?;
                Ok(result.written.iter().sum())
            }
            PromWriteVersion::V2 => {
                let result = ingest_remote_write_v2(
                    prom_write_state,
                    &db,
                    has_pipeline,
                    options.zstd,
                    ctx,
                    body,
                    body_reserved_bytes,
                )
                .await
                .map_err(|error| convert_err(error.error))?;
                Ok(result.written.iter().sum())
            }
        }
    }
}

/// The error for an OTLP payload whose points/spans were all rejected,
/// carrying the pipeline's reason when it provided one.
fn all_rejected_error(kind: &str, rejected: i64, reason: Option<&str>) -> PgWireError {
    bad_copy_data(match reason {
        Some(reason) => format!("OTLP ingestion rejected all {rejected} {kind}: {reason}"),
        None => format!("OTLP ingestion rejected all {rejected} {kind}"),
    })
}

/// Resolves the SQL target's scope and supplies a fallback destination
/// for logs/traces. The explicit WITH table_name option takes precedence
/// over the SQL target's name, but does not override its catalog/schema.
fn resolve_otlp_target(
    options: &OtlpOptions,
    parts: &[String],
    query_ctx: QueryContextRef,
) -> PgWireResult<(OtlpOptions, QueryContextRef)> {
    let mut options = options.clone();
    if parts.is_empty() {
        return Ok((options, query_ctx));
    }
    let table_name = match &mut options.signal {
        OtlpSignal::Metrics { .. } => {
            return Err(unsupported(format!(
                "format otlp routes each metric to its own table; \"COPY {} FROM STDIN\" must not name one",
                parts.join(".")
            )));
        }
        OtlpSignal::Traces { table_name, .. } | OtlpSignal::Logs { table_name, .. } => table_name,
    };
    let (catalog, schema, table) = resolve_table_name(&query_ctx, parts)?;
    table_name.get_or_insert(table);
    Ok((options, adjust_query_context(query_ctx, &catalog, &schema)))
}

fn adjust_query_context(
    query_ctx: QueryContextRef,
    catalog: &str,
    schema: &str,
) -> QueryContextRef {
    if query_ctx.current_catalog() == catalog && query_ctx.current_schema() == schema {
        return query_ctx;
    }
    let mut forked = query_ctx.fork();
    forked.set_current_catalog(catalog);
    forked.set_current_schema(schema);
    Arc::new(forked)
}

#[async_trait]
impl CopyHandler for PostgresServerHandlerInner {
    async fn on_copy_data<C>(&self, _client: &mut C, copy_data: CopyData) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let mut guard = self.copy_in_state.lock().await;
        let Some(mut state) = guard.take() else {
            return Err(copy_in_error(
                PgErrorCode::Ec08P01,
                "CopyData message received outside COPY FROM STDIN",
            ));
        };
        // The state is owned here: any parse, conversion or insert error
        // below aborts the whole copy and drops the buffered data —
        // otherwise a pooled connection would retain failed-import
        // buffers until its next COPY. On success it is put back.
        //
        // Buffered table-less payloads charge their bytes against the
        // shared request-memory limiter as they arrive (the
        // receive-stage counterpart of HTTP's body accounting); the
        // reservation is held until the buffer is flushed or the state
        // is dropped.
        if let Payload::Arrow(arrow) = &mut state.payload {
            let rows = arrow.feed(&copy_data.data).await?;
            state.rows_written += rows;
            METRIC_POSTGRES_COPY_IN_ROWS.inc_by(rows);
        } else if state.buffers_payload() {
            let chunk_bytes = u64::try_from(copy_data.data.len())
                .map_err(|_| bad_copy_data("chunk too large"))?;
            // Charge only this chunk's uncovered delta against the
            // shared budget: the limiter rounds each acquisition up to a
            // KiB, so per-chunk full charges would accumulate rounding
            // overhead (64 x 16-byte chunks would retain 64 KiB of
            // permits for 1 KiB of payload). The fits check covers only
            // THIS request's own holdings — cross-request contention is
            // left to `acquire`, which applies the configured
            // Wait/Reject policy instead of failing fast.
            let buffered_after = state.buffered_bytes() + chunk_bytes;
            // Granted coverage is KiB-aligned, so it can exceed the
            // buffered bytes: chunks inside the rounding headroom are
            // already covered.
            let uncovered = buffered_after.saturating_sub(state.granted_bytes());
            if uncovered > 0 {
                self.memory_limiter
                    .check_request_fits(uncovered, state.granted_bytes())
                    .map_err(convert_err)?;
                let memory_guard = self.memory_limiter.acquire(uncovered).await.map_err(|e| {
                    convert_err(crate::prom_remote_write::map_memory_acquire_timeout(e))
                })?;
                state.feed(&copy_data.data)?;
                state.charge_chunk(memory_guard, buffered_after);
            } else {
                // Fully covered by the previous acquisitions' rounding
                // headroom.
                state.feed(&copy_data.data)?;
            }
        } else {
            let Payload::Table { memory, .. } = &state.payload else {
                return Err(bad_copy_data("expected table COPY payload"));
            };
            // pgwire has already received this frame. Keep its entire body
            // charged while decoding slices or awaiting an insert; unconsumed
            // bytes of a large frame remain live throughout the loop.
            self.memory_limiter
                .check_request_fits(copy_data.data.len() as u64, memory.granted_bytes)
                .map_err(convert_err)?;
            let wire_guard = self
                .memory_limiter
                .acquire(copy_data.data.len() as u64)
                .await
                .map_err(|error| {
                    convert_err(crate::prom_remote_write::map_memory_acquire_timeout(error))
                })?;
            let mut data = copy_data.data.as_ref();
            while !data.is_empty() {
                let Payload::Table {
                    parser,
                    rows,
                    memory,
                    ..
                } = &mut state.payload
                else {
                    return Err(bad_copy_data("expected table COPY payload"));
                };
                let input = &data[..data.len().min(TableCopyMemory::DECODE_BYTES)];
                memory
                    .reserve_input(input.len(), wire_guard.granted_bytes())
                    .await?;
                let consumed = parser.decode(input)?;
                memory.batch_bytes += consumed as u64;
                data = &data[consumed..];
                if parser.is_full() {
                    *rows = parser.flush()?;
                    // The guards remain held: Arrow retains its buffer capacity.
                    memory.batch_bytes = 0;
                    let written = self.flush_copy_in(&mut state).await?;
                    state.rows_written += written;
                    METRIC_POSTGRES_COPY_IN_ROWS.inc_by(written);
                } else if consumed == 0 {
                    return Err(bad_copy_data("COPY decoder made no progress"));
                }
            }
        }
        if state.should_flush() {
            let rows = self.flush_copy_in(&mut state).await?;
            state.rows_written += rows;
            METRIC_POSTGRES_COPY_IN_ROWS.inc_by(rows);
        }
        *guard = Some(state);
        Ok(())
    }

    async fn on_copy_done<C>(&self, client: &mut C, _done: CopyDone) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let mut guard = self.copy_in_state.lock().await;
        // Take ownership up front: even finalization failures (parse
        // errors in the trailing record, insert errors) drop the state
        // instead of retaining it on a long-lived connection.
        let Some(mut state) = guard.take() else {
            return Err(copy_in_error(
                PgErrorCode::Ec08P01,
                "CopyDone message received outside COPY FROM STDIN",
            ));
        };
        let rows = if let Payload::Arrow(arrow) = &mut state.payload {
            arrow.finish().await?
        } else {
            state.finish()?;
            self.flush_copy_in(&mut state).await?
        };
        state.rows_written += rows;
        METRIC_POSTGRES_COPY_IN_ROWS.inc_by(rows);

        let rows_written = state.rows_written;
        let codec_name = state.codec_name;
        // Release decoder capacity and its reservation before acknowledging COPY.
        // A slow client reading the response must not keep ingestion memory live.
        drop(state);
        info!(
            "PostgreSQL COPY FROM STDIN (format {}) finished: {} rows written",
            codec_name, rows_written
        );
        client
            .send(PgWireBackendMessage::CommandComplete(
                Tag::new("COPY").with_rows(rows_written as usize).into(),
            ))
            .await?;

        // After a successful extended-protocol copy-in, pgwire leaves the
        // connection in `CopyInProgress(true)` and swallows the client's
        // terminating `Sync` message, so `ReadyForQuery` would never be
        // sent and the connection would deadlock. Route the state to
        // `AwaitingSync` so the `Sync` is dispatched to `on_sync`, which
        // completes the round trip.
        if let PgWireConnectionState::CopyInProgress(true) = client.state() {
            client.set_state(PgWireConnectionState::AwaitingSync);
        }
        Ok(())
    }

    async fn on_copy_fail<C>(&self, _client: &mut C, fail: CopyFail) -> PgWireError
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        // Drop the buffered state; partial data of a failed copy is not
        // written.
        self.copy_in_state.lock().await.take();
        copy_in_error(
            PgErrorCode::Ec57014,
            format!("COPY FROM STDIN mode terminated by user: {}", fail.message),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_table_copy_memory_tracks_peak_not_total() {
        let limiter = ServerMemoryLimiter::new(
            64 * 1024 * 1024,
            common_memory_manager::OnExhaustedPolicy::Fail,
        );
        let mut memory = TableCopyMemory::new(limiter.clone(), 2).await.unwrap();
        let fixed = limiter.used_bytes();
        assert!(fixed > 0);
        for _ in 0..100 {
            // Tiny chunks share the rounding headroom; resetting the current
            // batch reuses the high-water reservation, including after a flush.
            for _ in 0..1024 {
                memory.reserve_input(1, 0).await.unwrap();
                memory.batch_bytes += 1;
            }
            memory.batch_bytes = 0;
            assert_eq!(
                limiter.used_bytes(),
                fixed + 1024 * TableCopyMemory::INPUT_EXPANSION
            );
        }
        assert!(memory.guards.len() <= 17);
        drop(memory);
        assert_eq!(limiter.used_bytes(), 0);
    }

    #[tokio::test]
    async fn test_table_copy_memory_unlimited_tiny_chunks() {
        let limiter = ServerMemoryLimiter::default();
        let mut memory = TableCopyMemory::new(limiter.clone(), 2).await.unwrap();
        for _ in 0..1024 {
            memory.reserve_input(1, 0).await.unwrap();
            memory.batch_bytes += 1;
        }
        assert!(memory.guards.len() <= 17);
        drop(memory);
        assert_eq!(limiter.used_bytes(), 0);
    }

    #[tokio::test]
    async fn test_table_copy_memory_includes_held_wire_bytes() {
        let limiter = ServerMemoryLimiter::new(
            32 * 1024 * 1024,
            common_memory_manager::OnExhaustedPolicy::default(),
        );
        let mut memory = TableCopyMemory::new(limiter.clone(), 2).await.unwrap();
        let wire = limiter
            .acquire(limiter.limit_bytes() - memory.granted_bytes - 1024)
            .await
            .unwrap();
        // It cannot fit alongside its OWN frame. Fail immediately, not after
        // waiting for permits that only this request can release.
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            memory.reserve_input(8192, wire.granted_bytes()),
        )
        .await
        .unwrap();
        assert!(result.is_err());
        drop(memory);
        drop(wire);
        assert_eq!(limiter.used_bytes(), 0);
    }

    #[tokio::test]
    async fn test_table_copy_memory_waits_for_other_request() {
        let probe = TableCopyMemory::new(ServerMemoryLimiter::default(), 2)
            .await
            .unwrap();
        let limiter = ServerMemoryLimiter::new(
            probe.granted_bytes,
            common_memory_manager::OnExhaustedPolicy::default(),
        );
        let first = TableCopyMemory::new(limiter.clone(), 2).await.unwrap();
        let second = TableCopyMemory::new(limiter.clone(), 2);
        tokio::pin!(second);
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(50), &mut second)
                .await
                .is_err()
        );
        drop(first);
        let second = tokio::time::timeout(std::time::Duration::from_secs(1), second)
            .await
            .unwrap()
            .unwrap();
        drop(second);
        assert_eq!(limiter.used_bytes(), 0);
    }

    #[test]
    fn test_resolve_otlp_target() {
        for signal in ["logs", "traces"] {
            for (target, catalog, schema, expected_table) in [
                ("", "greptime", "public", None),
                ("my_table", "greptime", "public", Some("my_table")),
                ("other.my_table", "greptime", "other", Some("my_table")),
                ("cat.other.my_table", "cat", "other", Some("my_table")),
                (
                    "\"Other\".\"MyTable\"",
                    "greptime",
                    "Other",
                    Some("MyTable"),
                ),
            ] {
                for override_name in [None, Some("overridden")] {
                    let override_option = override_name
                        .map(|name| format!(", table_name '{name}'"))
                        .unwrap_or_default();
                    let stmt = copy_from_stdin_from_sql(&format!(
                        "COPY {target} FROM STDIN WITH (FORMAT otlp, signal '{signal}'{override_option})"
                    ))
                    .unwrap()
                    .unwrap();
                    let CopyFormat::Otlp(options) = stmt.format else {
                        panic!("expected OTLP");
                    };
                    let original_ctx = session::context::QueryContext::arc();
                    let (resolved, ctx) =
                        resolve_otlp_target(&options, &stmt.table, original_ctx.clone()).unwrap();
                    let table_name = match resolved.signal {
                        OtlpSignal::Logs { table_name, .. }
                        | OtlpSignal::Traces { table_name, .. } => table_name,
                        _ => panic!("expected logs or traces"),
                    };
                    assert_eq!(table_name.as_deref(), override_name.or(expected_table));
                    assert_eq!(ctx.current_catalog(), catalog);
                    assert_eq!(ctx.current_schema(), schema);
                    // A qualified target must not change the session context.
                    assert_eq!(original_ctx.current_catalog(), "greptime");
                    assert_eq!(original_ctx.current_schema(), "public");
                }
            }
        }
    }

    #[test]
    fn test_resolve_otlp_metrics_target_rejected() {
        for signal in ["", ", signal 'metrics'"] {
            let stmt = copy_from_stdin_from_sql(&format!(
                "COPY my_metrics FROM STDIN WITH (FORMAT otlp{signal})"
            ))
            .unwrap()
            .unwrap();
            let CopyFormat::Otlp(options) = stmt.format else {
                panic!("expected OTLP");
            };
            let ctx = session::context::QueryContext::arc();
            let err = resolve_otlp_target(&options, &stmt.table, ctx).unwrap_err();
            assert!(err.to_string().contains("must not name one"), "{err}");
        }
    }

    #[test]
    fn test_parse_copy_from_stdin_requires_format() {
        let err = copy_from_stdin_from_sql("COPY t FROM STDIN").unwrap_err();
        assert!(
            err.to_string()
                .contains("requires an explicit format in this version"),
            "{err}"
        );

        let copy = copy_from_stdin_from_sql("COPY t FROM STDIN WITH (FORMAT csv)")
            .unwrap()
            .unwrap();
        assert_eq!(copy.table, vec!["t"]);
        assert!(copy.columns.is_empty());
        assert!(matches!(copy.format, CopyFormat::Csv(_)));
        // Both formats are textual: format code 0.
    }

    #[test]
    fn test_parse_copy_from_stdin_csv() {
        let copy = copy_from_stdin_from_sql(
            "COPY t (a, b) FROM STDIN WITH (FORMAT csv, HEADER, DELIMITER ';')",
        )
        .unwrap()
        .unwrap();
        assert_eq!(copy.columns, vec!["a", "b"]);
        assert!(matches!(copy.format, CopyFormat::Csv(_)));
    }

    #[test]
    fn test_parse_copy_from_stdin_text_unsupported() {
        for sql in [
            "COPY t FROM STDIN WITH (FORMAT text)",
            "COPY t FROM STDIN WITH (FORMAT txt)",
        ] {
            let err = copy_from_stdin_from_sql(sql).unwrap_err();
            assert!(
                err.to_string().contains("text format is not supported yet"),
                "{err}"
            );
        }
    }

    #[test]
    fn test_parse_copy_from_stdin_binary_unsupported() {
        let err = copy_from_stdin_from_sql("COPY t FROM STDIN WITH (FORMAT binary)").unwrap_err();
        assert!(err.to_string().contains("not supported"));
    }

    #[test]
    fn test_parse_copy_from_stdin_unknown_format() {
        let err = copy_from_stdin_from_sql("COPY t FROM STDIN WITH (FORMAT parquet)").unwrap_err();
        assert!(err.to_string().contains("unknown COPY format"));
    }

    #[test]
    fn test_parse_copy_from_stdin_not_copy_in() {
        assert!(
            copy_from_stdin_from_sql("COPY (SELECT 1) TO STDOUT")
                .unwrap()
                .is_none()
        );
        assert!(
            copy_from_stdin_from_sql("COPY t TO '/tmp/x.csv'")
                .unwrap()
                .is_none()
        );
        // GreptimeDB's own file-based COPY FROM.
        assert!(
            copy_from_stdin_from_sql("COPY t FROM '/tmp/x.csv' WITH (FORMAT csv)")
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn test_redundant_options() {
        let err = copy_from_stdin_from_sql("COPY t FROM STDIN WITH (FORMAT csv, FORMAT text)")
            .unwrap_err();
        assert!(err.to_string().contains("redundant FORMAT"));

        let err = copy_from_stdin_from_sql("COPY t FROM STDIN WITH (DELIMITER ',', DELIMITER ';')")
            .unwrap_err();
        assert!(err.to_string().contains("redundant DELIMITER"));
    }

    #[test]
    fn test_format_specific_options_rejected() {
        // The open WITH section parses anything, but the format rejects
        // the options it does not understand.
        let err =
            copy_from_stdin_from_sql("COPY t FROM STDIN WITH (FORMAT csv, PRECISION 'ns', FREEZE)")
                .unwrap_err();
        assert!(
            err.to_string().contains("not supported by format csv"),
            "{err}"
        );
    }

    #[test]
    fn test_multi_statement_rejected() {
        let err = copy_from_stdin_from_sql("COPY t FROM STDIN; SELECT 1").unwrap_err();
        assert!(err.to_string().contains("alone"));
        // Trailing separators and quoted semicolons are fine.
        assert!(
            copy_from_stdin_from_sql("COPY t FROM STDIN WITH (FORMAT csv);")
                .unwrap()
                .is_some()
        );
        assert!(
            copy_from_stdin_from_sql("COPY t FROM STDIN WITH (FORMAT csv, DELIMITER ';')")
                .unwrap()
                .is_some()
        );
    }
}
