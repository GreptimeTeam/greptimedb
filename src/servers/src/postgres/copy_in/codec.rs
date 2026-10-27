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

//! The COPY FROM STDIN formats.
//!
//! [`CopyFormat`] is the closed, explicit set of supported formats — a
//! small enum over the real implementations rather than a generic
//! format framework. Each variant carries its validated options and
//! its rules are stated where they apply: a variant's match arms in
//! the copy lifecycle (see the parent module) decide whether a target
//! table is required, how the `CopyData` bytes are consumed, and which
//! existing ingestion pipeline completes the copy. Adding a format is
//! a normal extension: a new variant plus its arms.
//!
//! # Parsing and conversion
//!
//! Table-bound formats parse incrementally through a [`RecordParser`]:
//! it consumes raw `CopyData` bytes and emits complete, insertable
//! rows — one [`ParsedRow`] per record, in the order of the targeted
//! columns. `csv` delegates splitting and typed conversion to
//! arrow-csv's incremental decoder (the engine behind
//! `COPY ... FROM 'file.csv'`) and turns decoded batches into rows
//! with the shared `api::helper::vectors_to_rows`.
//!
//! Formats that delegate to an existing ingestion pipeline (the
//! influxdb line protocol) do not parse here at all: the whole payload
//! is buffered as one bounded request body and completed through that
//! pipeline, so record boundaries and values stay with the pipeline's
//! own parser.
//!
//! # Options
//!
//! The `WITH (...)` clause is open: every `KEY value` entry the client
//! writes is collected into a [`CopyOptionSet`] (see `options`)
//! regardless of whether any format understands it — the statement is
//! parsed textually (see `parse`) precisely so options beyond
//! sqlparser's COPY grammar can reach the formats. A format builder
//! takes the options it understands (`take_string`,
//! `take_single_byte`, `take_flag`, or raw `take_value` for
//! format-specific shapes) and rejects the rest with
//! `reject_leftovers`.

use pgwire::error::PgWireResult;

use crate::postgres::copy_in::csv::CsvCodec;
use crate::postgres::copy_in::influxdb::InfluxdbOptions;

/// A resolved COPY FROM STDIN format with its validated options.
#[derive(Debug, Clone)]
pub(super) enum CopyFormat {
    /// Table-bound CSV: records are parsed incrementally against the
    /// targeted table's columns and inserted in batches.
    Csv(CsvCodec),
    /// Table-less influxdb line protocol: the whole payload is one
    /// bounded request body, completed through the line-protocol
    /// pipeline the HTTP write endpoints use (one COPY statement is one
    /// ingestion request; streaming ingest is not supported).
    Influxdb(InfluxdbOptions),
    /// Table-less, **binary** OTLP metrics: one
    /// `ExportMetricsServiceRequest` protobuf message, buffered whole
    /// and completed through the OTLP metrics pipeline the OTLP/HTTP
    /// endpoint uses. Advertises wire format code 1.
    Otlp,
}

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
