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

use std::fmt::Debug;
use std::sync::Arc;

use api::helper::{ColumnDataTypeWrapper, to_grpc_value};
use api::v1::column_def::options_from_column_schema;
use api::v1::{
    Row as GrpcRow, RowInsertRequest, RowInsertRequests, Rows as GrpcRows, SemanticType,
};
use async_trait::async_trait;
use common_query::OutputData;
use common_telemetry::info;
use common_time::Timestamp;
use datafusion::sql::sqlparser::ast::{
    CopyOption, CopySource, CopyTarget, ObjectName, Statement as SqlParserStatement,
};
use datatypes::data_type::DataType;
use datatypes::prelude::ConcreteDataType;
use datatypes::schema::{ColumnSchema, SchemaRef};
use datatypes::value::Value as GtValue;
use futures::{Sink, SinkExt, stream};
use pgwire::api::copy::CopyHandler;
use pgwire::api::results::{CopyResponse, Response, Tag};
use pgwire::api::{ClientInfo, PgWireConnectionState};
use pgwire::error::{PgWireError, PgWireResult};
use pgwire::messages::PgWireBackendMessage;
use pgwire::messages::copy::{CopyData, CopyDone, CopyFail};
use session::context::QueryContextRef;
use sql::ast::ObjectNamePartExt;
use table::metadata::TableInfo;

use crate::metrics::METRIC_POSTGRES_COPY_IN_ROWS;
use crate::postgres::PostgresServerHandlerInner;
use crate::postgres::types::PgErrorCode;
use crate::postgres::utils::convert_err;

/// Number of parsed rows buffered before flushing a batch to the handler.
pub(crate) const COPY_IN_FLUSH_ROWS: usize = 8192;

/// The parsed `COPY tbl [(columns)] FROM STDIN [(options)]` statement.
#[derive(Debug, Clone)]
pub(crate) struct CopyFromStdin {
    pub(crate) table: ObjectName,
    /// Requested column list; empty means all columns in table order.
    pub(crate) columns: Vec<String>,
    pub(crate) format: CopyInFormat,
}

/// Supported COPY FROM STDIN data formats.
#[derive(Debug, Clone)]
pub(crate) enum CopyInFormat {
    /// Delimiter-separated text format with backslash escapes (`\N` for
    /// NULL). This is the format of PostgreSQL's plain text COPY.
    Text { delimiter: u8, null: Vec<u8> },
    /// CSV format.
    Csv {
        delimiter: u8,
        quote: u8,
        escape: u8,
        null: Vec<u8>,
        header: bool,
    },
}

fn copy_in_error(code: PgErrorCode, message: impl Into<String>) -> PgWireError {
    PgWireError::UserError(Box::new(code.to_err_info(message.into())))
}

fn unsupported(message: impl Into<String>) -> PgWireError {
    copy_in_error(PgErrorCode::Ec0A000, message)
}

fn bad_copy_data(message: impl Into<String>) -> PgWireError {
    copy_in_error(PgErrorCode::Ec22P04, message)
}

fn invalid_text_repr(message: impl Into<String>) -> PgWireError {
    copy_in_error(PgErrorCode::Ec22P02, message)
}

/// Detects and parses a `COPY ... FROM STDIN` statement.
///
/// Returns `Ok(None)` when the statement is not a copy-in (for example a
/// `COPY ... TO STDOUT`), `Ok(Some(..))` on success and an error when the
/// statement is a copy-in with unsupported or invalid options.
pub(crate) fn parse_copy_from_stdin(
    statement: &SqlParserStatement,
) -> PgWireResult<Option<CopyFromStdin>> {
    let SqlParserStatement::Copy {
        source,
        to,
        target,
        options,
        ..
    } = statement
    else {
        return Ok(None);
    };

    if *to || !matches!(target, CopyTarget::Stdin) {
        return Ok(None);
    }

    let (table, columns) = match source {
        CopySource::Table {
            table_name,
            columns,
        } => (table_name.clone(), columns.clone()),
        CopySource::Query(_) => {
            return Err(unsupported("COPY FROM STDIN requires a table as target"));
        }
    };

    let mut format: Option<String> = None;
    let mut delimiter: Option<u8> = None;
    let mut quote: Option<u8> = None;
    let mut escape: Option<u8> = None;
    let mut null: Option<Vec<u8>> = None;
    let mut header = false;
    for option in options {
        match option {
            CopyOption::Format(name) => {
                if format.replace(name.value.to_lowercase()).is_some() {
                    return Err(bad_copy_data("conflicting or redundant FORMAT option"));
                }
            }
            CopyOption::Delimiter(c) => {
                let d = single_byte(*c, "DELIMITER")?;
                if delimiter.replace(d).is_some() {
                    return Err(bad_copy_data("conflicting or redundant DELIMITER option"));
                }
            }
            CopyOption::Quote(c) => quote = Some(single_byte(*c, "QUOTE")?),
            CopyOption::Escape(c) => escape = Some(single_byte(*c, "ESCAPE")?),
            CopyOption::Null(s) => null = Some(s.clone().into_bytes()),
            CopyOption::Header(h) => header = *h,
            CopyOption::Freeze(_)
            | CopyOption::Encoding(_)
            | CopyOption::ForceQuote(_)
            | CopyOption::ForceNotNull(_)
            | CopyOption::ForceNull(_) => {
                return Err(unsupported(format!(
                    "COPY option {} is not supported",
                    option
                )));
            }
        }
    }

    let format = match format.as_deref() {
        None | Some("text") | Some("txt") => {
            if quote.is_some() || escape.is_some() {
                return Err(unsupported("QUOTE/ESCAPE are only allowed in CSV format"));
            }
            CopyInFormat::Text {
                delimiter: delimiter.unwrap_or(b'\t'),
                null: null.unwrap_or_else(|| b"\\N".to_vec()),
            }
        }
        Some("csv") => {
            let delimiter = delimiter.unwrap_or(b',');
            let quote = quote.unwrap_or(b'"');
            if delimiter == quote {
                return Err(bad_copy_data("DELIMITER must not be equal to QUOTE"));
            }
            CopyInFormat::Csv {
                delimiter,
                quote,
                // In PostgreSQL the default escape character for CSV is the
                // quote character itself (doubled quotes inside quoted fields).
                escape: escape.unwrap_or(quote),
                null: null.unwrap_or_default(),
                header,
            }
        }
        Some("binary") => {
            return Err(unsupported(
                "COPY FROM STDIN WITH (FORMAT binary) is not supported yet",
            ));
        }
        Some(unknown) => {
            return Err(bad_copy_data(format!(
                "unknown COPY format \"{}\"",
                unknown
            )));
        }
    };

    Ok(Some(CopyFromStdin {
        table,
        columns: columns.iter().map(|c| c.value.clone()).collect(),
        format,
    }))
}

fn single_byte(c: char, option: &str) -> PgWireResult<u8> {
    if c.is_ascii() && !c.is_ascii_control() {
        Ok(c as u8)
    } else {
        Err(bad_copy_data(format!(
            "{} must be a single one-byte non-newline character",
            option
        )))
    }
}

/// Resolves an [`ObjectName`] against the query context defaults.
fn resolve_table_name(
    query_ctx: &QueryContextRef,
    table: &ObjectName,
) -> PgWireResult<(String, String, String)> {
    let parts = &table.0;
    match parts.len() {
        1 => Ok((
            query_ctx.current_catalog().to_string(),
            query_ctx.current_schema(),
            parts[0].to_string_unquoted(),
        )),
        2 => Ok((
            query_ctx.current_catalog().to_string(),
            parts[0].to_string_unquoted(),
            parts[1].to_string_unquoted(),
        )),
        3 => Ok((
            parts[0].to_string_unquoted(),
            parts[1].to_string_unquoted(),
            parts[2].to_string_unquoted(),
        )),
        _ => Err(unsupported(format!(
            "invalid table name for COPY: {}",
            table
        ))),
    }
}

/// Returns true when the SQL string contains more than one statement, that
/// is, any token other than statement separators appears after the first
/// `;`. sqlparser's COPY grammar swallows statements trailing a COPY, so a
/// plain statement count cannot detect this.
pub(crate) fn has_multiple_statements(sql: &str) -> bool {
    use datafusion::sql::sqlparser::dialect::GenericDialect;
    use datafusion::sql::sqlparser::tokenizer::{Token, Tokenizer};

    let Ok(tokens) = Tokenizer::new(&GenericDialect {}, sql).tokenize() else {
        return false;
    };
    let mut seen_semicolon = false;
    for token in tokens {
        match token {
            Token::SemiColon => seen_semicolon = true,
            _ if seen_semicolon => return true,
            _ => {}
        }
    }
    false
}

/// Per-connection state of an in-progress COPY FROM STDIN.
pub(crate) struct CopyInState {
    query_ctx: QueryContextRef,
    /// Targeted columns in COPY field order.
    columns: Vec<ColumnSchema>,
    /// The gRPC column schemas matching `columns`, reused for every batch.
    grpc_schema: Vec<api::v1::ColumnSchema>,
    table_name: String,
    parser: Box<dyn RecordParser + Send>,
    header_to_skip: bool,
    /// Parsed rows not yet flushed to the handler.
    rows: Vec<GrpcRow>,
    rows_written: u64,
}

impl CopyInState {
    pub(crate) fn new(
        format: CopyInFormat,
        columns: Vec<String>,
        table_info: Arc<TableInfo>,
        query_ctx: QueryContextRef,
        table_name: String,
    ) -> PgWireResult<Self> {
        let schema = table_info.meta.schema.clone();
        let timestamp_index = schema.timestamp_index();

        let (columns, parser) = resolve_columns(format, columns, &schema, &table_info)?;
        if let Some(ts_index) = timestamp_index {
            let ts_column = &schema.column_schemas()[ts_index];
            if !columns.iter().any(|column| column.name == ts_column.name) {
                return Err(unsupported(format!(
                    "COPY FROM STDIN requires the time index column \"{}\" in the column list",
                    ts_column.name
                )));
            }
        }
        let header_to_skip = parser.skips_header();

        let grpc_schema = columns
            .iter()
            .map(|column| build_grpc_column_schema(column, &table_info))
            .collect::<PgWireResult<Vec<_>>>()?;

        Ok(Self {
            query_ctx,
            grpc_schema,
            columns,
            table_name,
            parser,
            header_to_skip,
            rows: Vec::new(),
            rows_written: 0,
        })
    }

    fn handle_record(&mut self, record: Record) -> PgWireResult<()> {
        if self.header_to_skip {
            self.header_to_skip = false;
            return Ok(());
        }

        let expected = self.columns.len();
        if record.len() < expected {
            let missing = &self.columns[record.len()];
            return Err(bad_copy_data(format!(
                "missing data for column \"{}\"",
                missing.name
            )));
        } else if record.len() > expected {
            return Err(bad_copy_data("extra data after last expected column"));
        }

        let mut values = Vec::with_capacity(expected);
        for (field, column) in record.into_iter().zip(&self.columns) {
            values.push(convert_field(column, field)?);
        }
        self.rows.push(GrpcRow { values });
        Ok(())
    }

    /// Feeds a `CopyData` chunk into the parser and buffers parsed rows.
    fn feed(&mut self, data: &[u8]) -> PgWireResult<()> {
        for record in self.parser.feed(data)? {
            self.handle_record(record)?;
        }
        Ok(())
    }

    /// Parses the buffered tail at `CopyDone`; a final record without a
    /// trailing newline is accepted.
    fn finish(&mut self) -> PgWireResult<()> {
        for record in self.parser.finish()? {
            self.handle_record(record)?;
        }
        Ok(())
    }

    fn should_flush(&self) -> bool {
        self.rows.len() >= COPY_IN_FLUSH_ROWS
    }

    /// Takes the buffered rows as an insert request, if any.
    fn take_requests(&mut self) -> Option<RowInsertRequests> {
        if self.rows.is_empty() {
            return None;
        }
        let rows = GrpcRows {
            schema: self.grpc_schema.clone(),
            rows: std::mem::take(&mut self.rows),
        };
        Some(RowInsertRequests {
            inserts: vec![RowInsertRequest {
                table_name: self.table_name.clone(),
                rows: Some(rows),
            }],
        })
    }
}

fn resolve_columns(
    format: CopyInFormat,
    columns: Vec<String>,
    schema: &SchemaRef,
    table_info: &Arc<TableInfo>,
) -> PgWireResult<(Vec<ColumnSchema>, Box<dyn RecordParser + Send>)> {
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
        check_column_type(&column.data_type)?;
    }

    let parser: Box<dyn RecordParser + Send> = match format {
        CopyInFormat::Text { delimiter, null } => Box::new(TextRecordParser {
            delimiter,
            null,
            pending: Vec::new(),
        }),
        CopyInFormat::Csv {
            delimiter,
            quote,
            escape,
            null,
            header,
        } => Box::new(CsvRecordParser::new(
            delimiter, quote, escape, &null, header,
        )),
    };

    Ok((selected, parser))
}

/// Columns whose text representation can be parsed by this module.
fn check_column_type(data_type: &ConcreteDataType) -> PgWireResult<()> {
    let supported = matches!(
        data_type,
        ConcreteDataType::Boolean(_)
            | ConcreteDataType::Int8(_)
            | ConcreteDataType::Int16(_)
            | ConcreteDataType::Int32(_)
            | ConcreteDataType::Int64(_)
            | ConcreteDataType::UInt8(_)
            | ConcreteDataType::UInt16(_)
            | ConcreteDataType::UInt32(_)
            | ConcreteDataType::UInt64(_)
            | ConcreteDataType::Float32(_)
            | ConcreteDataType::Float64(_)
            | ConcreteDataType::String(_)
            | ConcreteDataType::Binary(_)
            | ConcreteDataType::Date(_)
            | ConcreteDataType::Timestamp(_)
            | ConcreteDataType::Decimal128(_)
            | ConcreteDataType::Json(_)
    );
    if !supported {
        return Err(unsupported(format!(
            "column type \"{}\" is not supported for COPY FROM STDIN yet",
            data_type.name()
        )));
    }
    Ok(())
}

fn semantic_type_of(table_info: &TableInfo, column: &ColumnSchema) -> PgWireResult<SemanticType> {
    let meta = &table_info.meta;
    let schema = &meta.schema;
    let index = schema.column_index_by_name(&column.name).ok_or_else(|| {
        copy_in_error(
            PgErrorCode::Ec42703,
            format!("column \"{}\" not found in table schema", column.name),
        )
    })?;
    if Some(index) == schema.timestamp_index() {
        Ok(SemanticType::Timestamp)
    } else if meta.primary_key_indices.contains(&index) {
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

/// Converts one parsed field into a gRPC value following the column type.
fn convert_field(column: &ColumnSchema, field: Field) -> PgWireResult<api::v1::Value> {
    let value = match field {
        Field::Null => GtValue::Null,
        Field::Value(bytes) => convert_non_null_field(&column.data_type, &bytes)?,
    };
    Ok(to_grpc_value(value))
}

fn convert_non_null_field(data_type: &ConcreteDataType, bytes: &[u8]) -> PgWireResult<GtValue> {
    match data_type {
        ConcreteDataType::String(_) => Ok(GtValue::from(utf8(bytes)?.to_string())),
        ConcreteDataType::Binary(_) => Ok(GtValue::Binary(decode_bytea(bytes)?.into())),
        ConcreteDataType::Json(_) => {
            let json: serde_json::Value =
                serde_json::from_str(utf8(bytes)?).map_err(|e| invalid_text_repr(e.to_string()))?;
            Ok(GtValue::Json(Box::new(json.into())))
        }
        ConcreteDataType::Timestamp(t) => {
            // Mirrors the SQL INSERT string-to-timestamp conversion: parse
            // then convert to the column's unit; a bare integer is an epoch
            // value in the column's unit.
            let s = utf8(bytes)?;
            if let Ok(ts) = Timestamp::from_str(s, None) {
                ts.convert_to(t.unit())
                    .map(GtValue::Timestamp)
                    .ok_or_else(|| invalid_text_repr(format!("timestamp out of range: {s}")))
            } else if let Ok(n) = s.parse::<i64>() {
                Ok(GtValue::Timestamp(Timestamp::new(n, t.unit())))
            } else {
                Err(invalid_text_repr(format!(
                    "invalid input syntax for type {}: \"{}\"",
                    data_type.name(),
                    s
                )))
            }
        }
        _ => {
            let parsed = data_type
                .try_cast(GtValue::from(utf8(bytes)?))
                .ok_or_else(|| {
                    invalid_text_repr(format!(
                        "invalid input syntax for type {}: \"{}\"",
                        data_type.name(),
                        String::from_utf8_lossy(bytes)
                    ))
                })?;
            Ok(parsed)
        }
    }
}

fn utf8(bytes: &[u8]) -> PgWireResult<&str> {
    std::str::from_utf8(bytes)
        .map_err(|_| invalid_text_repr("invalid byte sequence for encoding UTF8"))
}

/// PostgreSQL sends `bytea` in text/CSV mode as a hex string prefixed
/// with `\x`; fall back to the raw bytes otherwise.
fn decode_bytea(bytes: &[u8]) -> PgWireResult<Vec<u8>> {
    if let Some(hex) = bytes.strip_prefix(b"\\x") {
        let hex = utf8(hex)?;
        decode_hex(hex).map_err(|_| invalid_text_repr("invalid hexadecimal bytea value"))
    } else {
        Ok(bytes.to_vec())
    }
}

fn decode_hex(s: &str) -> std::result::Result<Vec<u8>, ()> {
    let bytes = s.as_bytes();
    if !bytes.len().is_multiple_of(2) {
        return Err(());
    }
    let mut out = Vec::with_capacity(bytes.len() / 2);
    for pair in bytes.chunks_exact(2) {
        let hi = (pair[0] as char).to_digit(16).ok_or(())?;
        let lo = (pair[1] as char).to_digit(16).ok_or(())?;
        out.push((hi * 16 + lo) as u8);
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// Record parsers
// ---------------------------------------------------------------------------

/// One parsed field of a record.
enum Field {
    Null,
    Value(Vec<u8>),
}

type Record = Vec<Field>;

trait RecordParser: Send {
    /// Feeds a chunk of `CopyData` bytes and returns all records completed
    /// by this chunk. Records may span chunk boundaries.
    fn feed(&mut self, data: &[u8]) -> PgWireResult<Vec<Record>>;
    /// Parses the buffered tail as a final record; returns no record when
    /// the stream ended exactly at a record boundary.
    fn finish(&mut self) -> PgWireResult<Vec<Record>>;
    fn skips_header(&self) -> bool {
        false
    }
}

/// Parser for the text format: one record per line, fields separated by an
/// unescaped delimiter, special characters escaped with backslashes.
struct TextRecordParser {
    delimiter: u8,
    null: Vec<u8>,
    pending: Vec<u8>,
}

impl RecordParser for TextRecordParser {
    fn feed(&mut self, data: &[u8]) -> PgWireResult<Vec<Record>> {
        self.pending.extend_from_slice(data);
        let mut records = Vec::new();
        while let Some(pos) = self.pending.iter().position(|&b| b == b'\n') {
            let line: Vec<u8> = self.pending.drain(..=pos).collect();
            // The drained line always ends with the `\n` separator.
            let line = &line[..line.len() - 1];
            records.push(parse_text_line(line, self.delimiter, &self.null)?);
        }
        Ok(records)
    }

    fn finish(&mut self) -> PgWireResult<Vec<Record>> {
        if self.pending.is_empty() {
            return Ok(Vec::new());
        }
        let line = std::mem::take(&mut self.pending);
        Ok(vec![parse_text_line(&line, self.delimiter, &self.null)?])
    }
}

fn parse_text_line(line: &[u8], delimiter: u8, null: &[u8]) -> PgWireResult<Record> {
    split_raw_fields(line, delimiter)
        .into_iter()
        .map(|raw| {
            if raw == null {
                Ok(Field::Null)
            } else {
                unescape_text(raw).map(Field::Value)
            }
        })
        .collect()
}

/// Splits a text-format line on unescaped delimiters.
fn split_raw_fields(line: &[u8], delimiter: u8) -> Vec<&[u8]> {
    let mut fields = Vec::new();
    let mut start = 0;
    let mut i = 0;
    while i < line.len() {
        match line[i] {
            b'\\' => i += 2, // an escape always consumes the next byte
            b if b == delimiter => {
                fields.push(&line[start..i]);
                start = i + 1;
                i += 1;
            }
            _ => i += 1,
        }
    }
    fields.push(&line[start..]);
    fields
}

fn unescape_text(raw: &[u8]) -> PgWireResult<Vec<u8>> {
    let mut out = Vec::with_capacity(raw.len());
    let mut i = 0;
    while i < raw.len() {
        let b = raw[i];
        if b != b'\\' {
            out.push(b);
            i += 1;
            continue;
        }
        let Some(&next) = raw.get(i + 1) else {
            return Err(bad_copy_data("end-of-line backslash in COPY data"));
        };
        match next {
            b'\\' => out.push(b'\\'),
            b'n' => out.push(b'\n'),
            b'r' => out.push(b'\r'),
            b't' => out.push(b'\t'),
            b'b' => out.push(0x08),
            b'f' => out.push(0x0C),
            b'v' => out.push(0x0B),
            b'0'..=b'7' => {
                let mut value = 0u32;
                let mut j = i + 1;
                let octal_end = (j + 3).min(raw.len());
                while j < octal_end && (b'0'..=b'7').contains(&raw[j]) {
                    value = value * 8 + (raw[j] - b'0') as u32;
                    j += 1;
                }
                if value > 255 {
                    return Err(bad_copy_data("octal value out of range in COPY data"));
                }
                out.push(value as u8);
                i = j;
                continue;
            }
            other => {
                return Err(bad_copy_data(format!(
                    "invalid escape sequence '\\{}' in COPY data",
                    other as char
                )));
            }
        }
        i += 2;
    }
    Ok(out)
}

/// Incremental parser for the CSV format, keeping state across `CopyData`
/// chunk boundaries (quoted fields may contain newlines, and `\r\n` pairs
/// may split across chunks).
struct CsvRecordParser {
    delimiter: u8,
    quote: u8,
    escape: u8,
    null: Vec<u8>,
    state: CsvState,
    /// Completed fields of the current record.
    fields: Vec<Field>,
    /// Field currently being parsed.
    current: Vec<u8>,
    current_quoted: bool,
    /// A byte deferred until the next chunk because its meaning depends on
    /// its successor (`\r` of a possible `\r\n`, or an escape character).
    carry: Option<u8>,
    header: bool,
}

impl CsvRecordParser {
    fn new(delimiter: u8, quote: u8, escape: u8, null: &[u8], header: bool) -> Self {
        Self {
            delimiter,
            quote,
            escape,
            null: null.to_vec(),
            state: CsvState::FieldStart,
            fields: Vec::new(),
            current: Vec::new(),
            current_quoted: false,
            carry: None,
            header,
        }
    }

    /// Treats `b` as a data byte in the current state; used for a carried
    /// `\r` at end-of-stream that turns out to be literal data.
    fn push_data(&mut self, b: u8) {
        if self.state == CsvState::FieldStart {
            self.state = CsvState::InUnquoted;
        }
        self.current.push(b);
    }
}

#[derive(PartialEq, Eq)]
enum CsvState {
    FieldStart,
    InUnquoted,
    InQuoted,
    QuoteInQuoted,
}

impl RecordParser for CsvRecordParser {
    fn skips_header(&self) -> bool {
        self.header
    }

    fn feed(&mut self, data: &[u8]) -> PgWireResult<Vec<Record>> {
        let mut records = Vec::new();
        let mut buf;
        let data: &[u8] = if let Some(b) = self.carry.take() {
            buf = Vec::with_capacity(data.len() + 1);
            buf.push(b);
            buf.extend_from_slice(data);
            &buf
        } else {
            data
        };

        let mut idx = 0;
        while idx < data.len() {
            let b = data[idx];
            let next = data.get(idx + 1).copied();
            match self.state {
                CsvState::FieldStart => {
                    if b == self.quote {
                        self.state = CsvState::InQuoted;
                        self.current_quoted = true;
                        idx += 1;
                    } else if b == self.delimiter {
                        self.end_field();
                        idx += 1;
                    } else if is_record_end(b, next) {
                        self.state = CsvState::FieldStart;
                        records.push(self.end_record());
                        idx += record_end_len(b);
                    } else if is_incomplete_record_end(b, next) {
                        self.carry = Some(b);
                        break;
                    } else {
                        self.current.push(b);
                        self.state = CsvState::InUnquoted;
                        idx += 1;
                    }
                }
                CsvState::InUnquoted => {
                    if b == self.delimiter {
                        self.end_field();
                        self.state = CsvState::FieldStart;
                        idx += 1;
                    } else if is_record_end(b, next) {
                        self.state = CsvState::FieldStart;
                        records.push(self.end_record());
                        idx += record_end_len(b);
                    } else if is_incomplete_record_end(b, next) {
                        self.carry = Some(b);
                        break;
                    } else {
                        self.current.push(b);
                        idx += 1;
                    }
                }
                CsvState::InQuoted => {
                    if self.escape != self.quote && b == self.escape {
                        match next {
                            Some(n) => {
                                self.current.push(n);
                                idx += 2;
                            }
                            None => {
                                self.carry = Some(b);
                                break;
                            }
                        }
                    } else if b == self.quote {
                        self.state = CsvState::QuoteInQuoted;
                        idx += 1;
                    } else {
                        self.current.push(b);
                        idx += 1;
                    }
                }
                CsvState::QuoteInQuoted => {
                    if b == self.quote && self.escape == self.quote {
                        self.current.push(self.quote);
                        self.state = CsvState::InQuoted;
                        idx += 1;
                    } else if b == self.delimiter {
                        self.end_field();
                        self.state = CsvState::FieldStart;
                        idx += 1;
                    } else if is_record_end(b, next) {
                        self.state = CsvState::FieldStart;
                        records.push(self.end_record());
                        idx += record_end_len(b);
                    } else if is_incomplete_record_end(b, next) {
                        self.carry = Some(b);
                        break;
                    } else {
                        return Err(bad_copy_data(
                            "found unexpected data after a closing quote in CSV field",
                        ));
                    }
                }
            }
        }
        Ok(records)
    }

    fn finish(&mut self) -> PgWireResult<Vec<Record>> {
        if let Some(b) = self.carry.take() {
            // The deferred byte has no successor: a `\r` or escape character
            // at end of stream is literal data.
            match self.state {
                CsvState::QuoteInQuoted => {
                    return Err(bad_copy_data(
                        "found unexpected data after a closing quote in CSV field",
                    ));
                }
                CsvState::InQuoted | CsvState::InUnquoted | CsvState::FieldStart => {
                    self.push_data(b);
                }
            }
        }
        if matches!(self.state, CsvState::InQuoted) {
            return Err(bad_copy_data("unterminated CSV quoted field"));
        }
        // A field is pending unless the stream ended exactly at a record
        // boundary. A trailing delimiter also implies one (empty) field.
        let has_pending_field = !self.fields.is_empty()
            || !self.current.is_empty()
            || self.current_quoted
            || matches!(self.state, CsvState::InUnquoted | CsvState::QuoteInQuoted);
        if has_pending_field {
            self.end_field();
        }
        if !self.fields.is_empty() {
            return Ok(vec![std::mem::take(&mut self.fields)]);
        }
        Ok(Vec::new())
    }
}

/// `b` starts a record terminator (`\n` or the `\r` of a `\r\n` pair).
fn is_record_end(b: u8, next: Option<u8>) -> bool {
    b == b'\n' || (b == b'\r' && next == Some(b'\n'))
}

/// `b` may start a record terminator but its successor is unknown.
fn is_incomplete_record_end(b: u8, next: Option<u8>) -> bool {
    b == b'\r' && next.is_none()
}

fn record_end_len(b: u8) -> usize {
    if b == b'\r' { 2 } else { 1 }
}

impl CsvRecordParser {
    fn end_field(&mut self) {
        let value = std::mem::take(&mut self.current);
        let quoted = self.current_quoted;
        self.current_quoted = false;
        if !quoted && value == self.null {
            self.fields.push(Field::Null);
        } else {
            self.fields.push(Field::Value(value));
        }
    }

    fn end_record(&mut self) -> Record {
        self.end_field();
        std::mem::take(&mut self.fields)
    }
}

// ---------------------------------------------------------------------------
// pgwire handlers
// ---------------------------------------------------------------------------

impl PostgresServerHandlerInner {
    /// Prepares a COPY FROM STDIN: resolves the target table, validates
    /// columns and stashes the per-connection copy state.
    pub(crate) async fn begin_copy_in(&self, stmt: CopyFromStdin) -> PgWireResult<Response> {
        let query_ctx = self.session.new_query_context();
        let (catalog, schema, table) = resolve_table_name(&query_ctx, &stmt.table)?;

        let Some(table_info) = self
            .copy_in_handler
            .copy_in_table(&catalog, &schema, &table, query_ctx.clone())
            .await
            .map_err(convert_err)?
        else {
            return Err(copy_in_error(
                PgErrorCode::Ec42P01,
                format!("relation \"{}\" does not exist", stmt.table),
            ));
        };

        // The insert path resolves the bare table name against the query
        // context, so point the context at the resolved catalog and schema
        // when the COPY target is qualified.
        let query_ctx = adjust_query_context(query_ctx, &catalog, &schema);

        let state = CopyInState::new(stmt.format, stmt.columns, table_info, query_ctx, table)?;
        let num_columns = state.columns.len();

        *self.copy_in_state.lock().await = Some(state);

        Ok(Response::CopyIn(CopyResponse::new(
            0, // text
            num_columns,
            stream::empty(),
        )))
    }

    /// Flushes buffered rows of an in-progress copy. Returns the number of
    /// rows written by the flushed batch.
    async fn flush_copy_in(&self, state: &mut CopyInState) -> PgWireResult<u64> {
        let Some(requests) = state.take_requests() else {
            return Ok(0);
        };
        let query_ctx = state.query_ctx.clone();
        let output = self
            .copy_in_handler
            .copy_in_insert(requests, query_ctx)
            .await
            .map_err(convert_err)?;
        match output.data {
            OutputData::AffectedRows(rows) => Ok(rows as u64),
            _ => Ok(0),
        }
    }
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
        let Some(state) = guard.as_mut() else {
            return Err(copy_in_error(
                PgErrorCode::Ec08P01,
                "CopyData message received outside COPY FROM STDIN",
            ));
        };
        state.feed(&copy_data.data)?;
        if state.should_flush() {
            let rows = self.flush_copy_in(state).await?;
            state.rows_written += rows;
            METRIC_POSTGRES_COPY_IN_ROWS.inc_by(rows);
        }
        Ok(())
    }

    async fn on_copy_done<C>(&self, client: &mut C, _done: CopyDone) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let mut guard = self.copy_in_state.lock().await;
        let Some(state) = guard.as_mut() else {
            return Err(copy_in_error(
                PgErrorCode::Ec08P01,
                "CopyDone message received outside COPY FROM STDIN",
            ));
        };
        state.finish()?;
        let rows = self.flush_copy_in(state).await?;
        state.rows_written += rows;
        METRIC_POSTGRES_COPY_IN_ROWS.inc_by(rows);

        let rows_written = state.rows_written;
        info!(
            "PostgreSQL COPY FROM STDIN finished: {} rows written",
            rows_written
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
    use datafusion_pg_catalog::sql::PostgresCompatibilityParser;

    use super::*;

    fn parse(sql: &str) -> SqlParserStatement {
        let parser = PostgresCompatibilityParser::new();
        let statements = parser.parse(sql).unwrap();
        statements.into_iter().next().unwrap()
    }

    #[test]
    fn test_parse_copy_from_stdin_default() {
        let stmt = parse("COPY t FROM STDIN");
        let copy = parse_copy_from_stdin(&stmt).unwrap().unwrap();
        assert_eq!(copy.table.to_string(), "t");
        assert!(copy.columns.is_empty());
        assert!(matches!(copy.format, CopyInFormat::Text { .. }));
    }

    #[test]
    fn test_parse_copy_from_stdin_csv() {
        let stmt = parse("COPY t (a, b) FROM STDIN WITH (FORMAT csv, HEADER, DELIMITER ';')");
        let copy = parse_copy_from_stdin(&stmt).unwrap().unwrap();
        assert_eq!(copy.columns, vec!["a", "b"]);
        match copy.format {
            CopyInFormat::Csv {
                delimiter, header, ..
            } => {
                assert_eq!(delimiter, b';');
                assert!(header);
            }
            other => panic!("expected csv, got {:?}", other),
        }
    }

    #[test]
    fn test_parse_copy_from_stdin_null_string() {
        let stmt = parse("COPY t FROM STDIN WITH (NULL 'nil', DELIMITER '|')");
        let copy = parse_copy_from_stdin(&stmt).unwrap().unwrap();
        match copy.format {
            CopyInFormat::Text { delimiter, null } => {
                assert_eq!(delimiter, b'|');
                assert_eq!(null, b"nil".to_vec());
            }
            other => panic!("expected text, got {:?}", other),
        }
    }

    #[test]
    fn test_parse_copy_from_stdin_binary_unsupported() {
        let stmt = parse("COPY t FROM STDIN WITH (FORMAT binary)");
        let err = parse_copy_from_stdin(&stmt).unwrap_err();
        assert!(err.to_string().contains("not supported"));
    }

    #[test]
    fn test_parse_copy_from_stdin_not_copy_in() {
        let stmt = parse("COPY (SELECT 1) TO STDOUT");
        assert!(parse_copy_from_stdin(&stmt).unwrap().is_none());

        let stmt = parse("COPY t TO '/tmp/x.csv'");
        assert!(parse_copy_from_stdin(&stmt).unwrap().is_none());
    }

    fn text_parser() -> TextRecordParser {
        TextRecordParser {
            delimiter: b'\t',
            null: b"\\N".to_vec(),
            pending: Vec::new(),
        }
    }

    #[test]
    fn test_text_parse_records() {
        let mut parser = text_parser();
        let records = parser.feed(b"1\thello\t\\N\n2\two\\trld\t3\n").unwrap();
        assert_eq!(records.len(), 2);
        assert!(matches!(&records[0][0], Field::Value(v) if v == b"1"));
        assert!(matches!(&records[0][2], Field::Null));
        assert!(matches!(&records[1][1], Field::Value(v) if v == b"wo\trld"));
    }

    #[test]
    fn test_text_parse_chunk_boundaries() {
        let mut parser = text_parser();
        assert!(parser.feed(b"ab").unwrap().is_empty());
        assert!(parser.feed(b"c\td").unwrap().is_empty());
        let records = parser.feed(b"e\n").unwrap();
        assert_eq!(records.len(), 1);
        assert!(matches!(&records[0][0], Field::Value(v) if v == b"abc"));
        assert!(matches!(&records[0][1], Field::Value(v) if v == b"de"));
    }

    #[test]
    fn test_text_trailing_record_without_newline() {
        let mut parser = text_parser();
        parser.feed(b"x\ty").unwrap();
        let records = parser.finish().unwrap();
        assert_eq!(records.len(), 1);
        assert!(matches!(&records[0][0], Field::Value(v) if v == b"x"));
        assert!(matches!(&records[0][1], Field::Value(v) if v == b"y"));
    }

    #[test]
    fn test_text_octal_and_errors() {
        let mut parser = text_parser();
        let records = parser.feed(b"\\101\\102\n").unwrap();
        assert!(matches!(&records[0][0], Field::Value(v) if v == b"AB"));

        let mut parser = text_parser();
        let err = match parser.feed(b"a\\z\n") {
            Ok(_) => panic!("expected parse error"),
            Err(e) => e,
        };
        assert!(err.to_string().contains("invalid escape sequence"));

        let mut parser = text_parser();
        parser.feed(b"trailing\\").unwrap();
        let err = match parser.finish() {
            Ok(_) => panic!("expected parse error"),
            Err(e) => e,
        };
        assert!(err.to_string().contains("end-of-line backslash"));
    }

    fn csv_parser() -> CsvRecordParser {
        CsvRecordParser::new(b',', b'"', b'"', b"", false)
    }

    #[test]
    fn test_csv_parse_records() {
        let mut parser = csv_parser();
        let records = parser
            .feed(b"a,b,\"multi\nline\",plain\"quote\n1,,x\n")
            .unwrap();
        assert_eq!(records.len(), 2);
        assert!(matches!(&records[0][0], Field::Value(v) if v == b"a"));
        assert!(matches!(&records[0][2], Field::Value(v) if v == b"multi\nline"));
        // Unquoted empty field is NULL; an unquoted quote is plain data.
        assert!(matches!(&records[1][1], Field::Null));
        assert!(matches!(&records[0][3], Field::Value(v) if v == b"plain\"quote"));
    }

    #[test]
    fn test_csv_escaped_quotes() {
        let mut parser = csv_parser();
        let records = parser.feed(b"\"say \"\"hi\"\"\"\n").unwrap();
        assert!(matches!(&records[0][0], Field::Value(v) if v == b"say \"hi\""));
    }

    #[test]
    fn test_csv_chunk_boundaries() {
        let mut parser = csv_parser();
        // Quoted field containing a newline, then a CRLF record separator
        // split across chunks.
        assert!(parser.feed(b"\"ab").unwrap().is_empty());
        assert!(parser.feed(b"c\nd\",x").unwrap().is_empty());
        let records = parser.feed(b"\r\nnext\r\n").unwrap();
        assert_eq!(records.len(), 2);
        assert!(matches!(&records[0][0], Field::Value(v) if v == b"abc\nd"));
        assert!(matches!(&records[1][0], Field::Value(v) if v == b"next"));
    }

    #[test]
    fn test_csv_lone_cr_is_data() {
        let mut parser = csv_parser();
        let records = parser.feed(b"a\rb\nc\r").unwrap();
        assert_eq!(records.len(), 1);
        assert!(matches!(&records[0][0], Field::Value(v) if v == b"a\rb"));
        // Trailing lone `\r` becomes data of a final unterminated record.
        let records = parser.finish().unwrap();
        assert_eq!(records.len(), 1);
        assert!(matches!(&records[0][0], Field::Value(v) if v == b"c\r"));
    }

    #[test]
    fn test_csv_unterminated_quote() {
        let mut parser = csv_parser();
        parser.feed(b"\"abc").unwrap();
        let err = match parser.finish() {
            Ok(_) => panic!("expected parse error"),
            Err(e) => e,
        };
        assert!(err.to_string().contains("unterminated"));
    }

    #[test]
    fn test_csv_custom_null() {
        let mut parser = CsvRecordParser::new(b',', b'"', b'"', b"NULL", false);
        let records = parser.feed(b"NULL,\"NULL\"\n").unwrap();
        assert!(matches!(&records[0][0], Field::Null));
        assert!(matches!(&records[0][1], Field::Value(v) if v == b"NULL"));
    }

    #[test]
    fn test_csv_custom_escape() {
        let mut parser = CsvRecordParser::new(b',', b'"', b'\\', b"", false);
        let records = parser.feed(b"\"a\\\"b\",x\n").unwrap();
        assert!(matches!(&records[0][0], Field::Value(v) if v == b"a\"b"));
        assert!(matches!(&records[0][1], Field::Value(v) if v == b"x"));
    }

    #[test]
    fn test_csv_stray_data_after_quote() {
        let mut parser = csv_parser();
        let err = match parser.feed(b"\"ab\"c\n") {
            Ok(_) => panic!("expected parse error"),
            Err(e) => e,
        };
        assert!(err.to_string().contains("unexpected data"));
    }

    #[test]
    fn test_csv_finish_at_record_boundary() {
        let mut parser = csv_parser();
        parser.feed(b"a,b\n").unwrap();
        assert!(parser.finish().unwrap().is_empty());
    }

    #[test]
    fn test_csv_trailing_delimiter() {
        let mut parser = csv_parser();
        parser.feed(b"a,").unwrap();
        let records = parser.finish().unwrap();
        assert_eq!(records.len(), 1);
        assert!(matches!(&records[0][0], Field::Value(v) if v == b"a"));
        assert!(matches!(&records[0][1], Field::Null));
    }

    #[test]
    fn test_decode_bytea() {
        assert_eq!(decode_bytea(b"\\x616263").unwrap(), b"abc");
        assert_eq!(decode_bytea(b"plain").unwrap(), b"plain");
        assert!(decode_bytea(b"\\xzz").is_err());
        assert!(decode_bytea(b"\\x6").is_err());
    }
}
