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

//! The COPY FROM STDIN codec framework.
//!
//! A *codec* binds one COPY data format to its implementation. Codecs are
//! stateless strategies: they validate the options they accept at
//! statement-parse time, describe which column types they can ingest, and
//! create the stateful [`RecordParser`] per stream.
//!
//! Formats are not limited to PostgreSQL's own — GreptimeDB-specific
//! formats (a line protocol, a JSON-lines dialect, ...) register exactly
//! like `csv` does, and may define their own options through the open
//! `WITH (...)` clause.
//!
//! # Parsing and conversion
//!
//! A [`RecordParser`] consumes raw `CopyData` bytes and emits complete,
//! insertable rows: one [`ParsedRow`] per record, in the order of the
//! targeted columns. Parsers validate record shape (field counts) and
//! convert field values themselves — `csv` delegates both splitting and
//! typed conversion to arrow-csv's incremental decoder (the engine
//! behind `COPY ... FROM 'file.csv'`) and turns decoded batches into
//! rows with the shared `api::helper::vectors_to_rows`. A future format
//! that carries values as text (e.g. a line protocol) would reintroduce
//! shared field-conversion helpers here; see the removed `text` codec in
//! git history for the shape they had.
//!
//! # Options
//!
//! The `WITH (...)` clause is open: every `KEY value` entry the client
//! writes is collected into a [`CopyOptionSet`] (see `options`) regardless
//! of whether any format understands it — the statement is parsed
//! textually (see `parse`) precisely so options beyond sqlparser's COPY
//! grammar can reach the codecs. A codec builder takes the options it
//! understands (`take_string`, `take_single_byte`, `take_flag`, or raw
//! `take_value` for codec-specific shapes) and rejects the rest with
//! `reject_leftovers`, so a future format like
//! `COPY t FROM STDIN WITH (FORMAT influxdb_line, precision 'ns')` only
//! needs its own codec and registry entry.
//!
//! To add a format:
//!
//! 1. implement [`CopyInCodec`] for a config struct holding the format's
//!    validated options (see `csv::CsvCodec` for the example),
//! 2. write a builder `fn(CopyOptionSet) -> PgWireResult<Arc<dyn
//!    CopyInCodec>>` that applies defaults and rejects options the format
//!    does not accept,
//! 3. register the builder under every accepted `FORMAT` name (including
//!    aliases) in `REGISTERED_FORMATS` in the parent module.

use datatypes::prelude::ConcreteDataType;
use datatypes::schema::ColumnSchema;
use pgwire::error::PgWireResult;

/// A parsed, insertable row: one gRPC value per targeted column, in COPY
/// field order. Every parser emits these directly, whatever mechanism it
/// used to produce them.
pub(super) type ParsedRow = api::v1::Row;

/// Incremental parser of one COPY FROM STDIN stream.
///
/// The wire protocol delivers the payload as an arbitrary sequence of
/// `CopyData` chunks; implementations must keep state across [`feed`]
/// calls because records (and even single fields) may span chunk
/// boundaries. [`finish`] is called at `CopyDone`; a final record without
/// a trailing record separator is accepted.
pub(super) trait RecordParser: Send {
    /// Feeds a chunk of `CopyData` bytes and returns all complete rows.
    fn feed(&mut self, data: &[u8]) -> PgWireResult<Vec<ParsedRow>>;
    /// Parses the buffered tail at `CopyDone`; returns no row when the
    /// stream ended exactly at a record boundary.
    fn finish(&mut self) -> PgWireResult<Vec<ParsedRow>>;
}

/// A COPY FROM STDIN data format implementation.
///
/// Implementations are shared per statement (hence `Send + Sync`) and must
/// be stateless; per-stream state belongs to the [`RecordParser`] they
/// create. Every method is tailored to the format: `check_column_type`
/// declares the types the format can decode (csv is bounded by what
/// arrow-csv supports).
pub(super) trait CopyInCodec: Send + Sync + std::fmt::Debug {
    /// The canonical format name, used in error messages.
    fn name(&self) -> &'static str;
    /// The format code advertised in the `CopyInResponse` header: 0 when
    /// the client sends textual data (the default), 1 for binary.
    fn format_code(&self) -> i8 {
        0
    }
    /// Validates that a column of `data_type` can be ingested by this
    /// format.
    fn check_column_type(&self, data_type: &ConcreteDataType) -> PgWireResult<()>;
    /// Creates the stateful parser for one copy stream over the targeted
    /// columns (in COPY field order).
    ///
    /// `timezone` is the session timezone (an IANA name or a UTC offset):
    /// formats that parse offset-free timestamp strings from text should
    /// interpret them in this zone, matching SQL INSERT.
    fn create_parser(
        &self,
        columns: &[ColumnSchema],
        timezone: &str,
    ) -> PgWireResult<Box<dyn RecordParser + Send>>;
}
