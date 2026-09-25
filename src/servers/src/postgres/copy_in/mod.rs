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
//! # Format framework
//!
//! Each COPY data format is implemented as a [`codec::CopyInCodec`]: a
//! stateless strategy that validates its options at statement-parse time,
//! describes the column types it can ingest, creates the stateful record
//! parser for one stream, and converts parsed fields into typed values.
//! Formats register a builder under every accepted `FORMAT` name in
//! [`REGISTERED_FORMATS`]; see the `codec` module docs for how to add one.

mod codec;
mod csv;
mod options;
mod text;

use std::fmt::Debug;
use std::sync::Arc;

use api::helper::ColumnDataTypeWrapper;
use api::v1::column_def::options_from_column_schema;
use api::v1::{
    Row as GrpcRow, RowInsertRequest, RowInsertRequests, Rows as GrpcRows, SemanticType,
};
use async_trait::async_trait;
use codec::{CopyInCodec, RecordParser};
use common_query::OutputData;
use common_telemetry::info;
use datafusion::sql::sqlparser::ast::{
    CopySource, CopyTarget, ObjectName, Statement as SqlParserStatement,
};
use datatypes::data_type::DataType;
use datatypes::schema::{ColumnSchema, SchemaRef};
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

/// The default format when `WITH (FORMAT ...)` is absent.
const DEFAULT_FORMAT: &str = "text";

/// Builds a codec from the format-neutral option set.
type CodecBuilder = fn(options::CopyOptionSet) -> PgWireResult<Arc<dyn CopyInCodec>>;

/// The registered COPY FROM STDIN formats: accepted `FORMAT` name to
/// builder. A format may register aliases (like `txt` for `text`).
const REGISTERED_FORMATS: &[(&str, CodecBuilder)] = &[
    ("text", text::build_text_codec),
    ("txt", text::build_text_codec),
    ("csv", csv::build_csv_codec),
    // Registered so `FORMAT binary` reports a clear "not supported yet"
    // instead of an unknown-format error; replace with a real codec when
    // implemented.
    ("binary", build_unimplemented_binary_codec),
];

fn build_unimplemented_binary_codec(
    _options: options::CopyOptionSet,
) -> PgWireResult<Arc<dyn CopyInCodec>> {
    Err(unsupported(
        "COPY FROM STDIN WITH (FORMAT binary) is not supported yet",
    ))
}

/// Resolves the format codec from the parsed options.
fn resolve_codec(options: &mut options::CopyOptionSet) -> PgWireResult<Arc<dyn CopyInCodec>> {
    let name = options
        .format
        .take()
        .unwrap_or_else(|| DEFAULT_FORMAT.to_string());
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
    table: ObjectName,
    /// Requested column list; empty means all columns in table order.
    columns: Vec<String>,
    /// The resolved format codec, validated and ready to run.
    codec: Arc<dyn CopyInCodec>,
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

    let mut option_set = options::parse_copy_options(options)?;
    let codec = resolve_codec(&mut option_set)?;

    Ok(Some(CopyFromStdin {
        table,
        columns: columns.iter().map(|c| c.value.clone()).collect(),
        codec,
    }))
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
    codec: Arc<dyn CopyInCodec>,
    parser: Box<dyn RecordParser + Send>,
    header_to_skip: bool,
    /// Parsed rows not yet flushed to the handler.
    rows: Vec<GrpcRow>,
    rows_written: u64,
}

impl CopyInState {
    fn new(
        codec: Arc<dyn CopyInCodec>,
        columns: Vec<String>,
        table_info: Arc<TableInfo>,
        query_ctx: QueryContextRef,
        table_name: String,
    ) -> PgWireResult<Self> {
        let schema = table_info.meta.schema.clone();
        let timestamp_index = schema.timestamp_index();

        let columns = resolve_columns(codec.as_ref(), columns, &schema, &table_info)?;
        if let Some(ts_index) = timestamp_index {
            let ts_column = &schema.column_schemas()[ts_index];
            if !columns.iter().any(|column| column.name == ts_column.name) {
                return Err(unsupported(format!(
                    "COPY FROM STDIN requires the time index column \"{}\" in the column list",
                    ts_column.name
                )));
            }
        }
        let parser = codec.create_parser();
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
            codec,
            parser,
            header_to_skip,
            rows: Vec::new(),
            rows_written: 0,
        })
    }

    /// The wire format code advertised to the client.
    pub(crate) fn format_code(&self) -> i8 {
        self.codec.format_code()
    }

    fn handle_record(&mut self, record: codec::Record) -> PgWireResult<()> {
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
            values.push(self.codec.convert_field(column, field)?);
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
    codec: &dyn CopyInCodec,
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

// ---------------------------------------------------------------------------
// pgwire handlers
// ---------------------------------------------------------------------------

impl PostgresServerHandlerInner {
    /// Prepares a COPY FROM STDIN: resolves the target table, validates
    /// columns and stashes the per-connection copy state.
    pub(crate) async fn begin_copy_in(&self, stmt: CopyFromStdin) -> PgWireResult<Response> {
        // The batching-aware context routes copy-in flushes through the
        // pending-rows batcher like SQL INSERTs when it is enabled for
        // this connection.
        let query_ctx = self.new_query_context();
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

        let state = CopyInState::new(stmt.codec, stmt.columns, table_info, query_ctx, table)?;
        let num_columns = state.columns.len();
        let format_code = state.format_code();

        *self.copy_in_state.lock().await = Some(state);

        Ok(Response::CopyIn(CopyResponse::new(
            format_code,
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
            "PostgreSQL COPY FROM STDIN (format {}) finished: {} rows written",
            state.codec.name(),
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
        assert_eq!(copy.codec.name(), "text");
        assert_eq!(copy.codec.format_code(), 0);
    }

    #[test]
    fn test_parse_copy_from_stdin_csv() {
        let stmt = parse("COPY t (a, b) FROM STDIN WITH (FORMAT csv, HEADER, DELIMITER ';')");
        let copy = parse_copy_from_stdin(&stmt).unwrap().unwrap();
        assert_eq!(copy.columns, vec!["a", "b"]);
        assert_eq!(copy.codec.name(), "csv");
    }

    #[test]
    fn test_parse_copy_from_stdin_txt_alias() {
        let stmt = parse("COPY t FROM STDIN WITH (FORMAT txt)");
        let copy = parse_copy_from_stdin(&stmt).unwrap().unwrap();
        assert_eq!(copy.codec.name(), "text");
    }

    #[test]
    fn test_parse_copy_from_stdin_binary_unsupported() {
        let stmt = parse("COPY t FROM STDIN WITH (FORMAT binary)");
        let err = parse_copy_from_stdin(&stmt).unwrap_err();
        assert!(err.to_string().contains("not supported"));
    }

    #[test]
    fn test_parse_copy_from_stdin_unknown_format() {
        let stmt = parse("COPY t FROM STDIN WITH (FORMAT parquet)");
        let err = parse_copy_from_stdin(&stmt).unwrap_err();
        assert!(err.to_string().contains("unknown COPY format"));
    }

    #[test]
    fn test_parse_copy_from_stdin_not_copy_in() {
        let stmt = parse("COPY (SELECT 1) TO STDOUT");
        assert!(parse_copy_from_stdin(&stmt).unwrap().is_none());

        let stmt = parse("COPY t TO '/tmp/x.csv'");
        assert!(parse_copy_from_stdin(&stmt).unwrap().is_none());
    }

    #[test]
    fn test_redundant_options() {
        let stmt = parse("COPY t FROM STDIN WITH (FORMAT csv, FORMAT text)");
        let err = parse_copy_from_stdin(&stmt).unwrap_err();
        assert!(err.to_string().contains("redundant FORMAT"));

        let stmt = parse("COPY t FROM STDIN WITH (DELIMITER ',', DELIMITER ';')");
        let err = parse_copy_from_stdin(&stmt).unwrap_err();
        assert!(err.to_string().contains("redundant DELIMITER"));
    }

    #[test]
    fn test_has_multiple_statements() {
        assert!(!has_multiple_statements("COPY t FROM STDIN"));
        assert!(!has_multiple_statements("COPY t FROM STDIN;"));
        assert!(has_multiple_statements("COPY t FROM STDIN; SELECT 1"));
        assert!(has_multiple_statements("SELECT 1; COPY t FROM STDIN"));
        // Semicolons inside string literals are not separators.
        assert!(!has_multiple_statements(
            "COPY t FROM STDIN WITH (NULL ';')"
        ));
    }
}
