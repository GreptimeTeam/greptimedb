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
//! A *codec* binds one COPY data format (text, CSV, binary, ...) to its
//! implementation. Codecs are stateless strategies: they validate the
//! options they accept at statement-parse time, describe which column
//! types they can ingest, and create the stateful [`RecordParser`] per
//! stream.
//!
//! # Parsing and conversion
//!
//! A [`RecordParser`] consumes raw `CopyData` bytes and emits complete,
//! insertable rows: one [`ParsedRow`] (a vector of gRPC values) per
//! record, in the order of the targeted columns. Parsers validate record
//! shape (field counts) and convert field values themselves — the `text`
//! family does so with the shared text-representation helpers in this
//! module, while the `csv` codec delegates both splitting and typed
//! conversion to arrow-csv's incremental decoder, the same engine used
//! by `COPY ... FROM 'file.csv'`.
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
//!    validated options (see `text::TextCodec` for the smallest example),
//! 2. write a builder `fn(CopyOptionSet) -> PgWireResult<Arc<dyn
//!    CopyInCodec>>` that applies defaults and rejects options the format
//!    does not accept,
//! 3. register the builder under every accepted `FORMAT` name (including
//!    aliases) in `REGISTERED_FORMATS` in the parent module.

use api::helper::to_grpc_value;
use common_time::Timestamp;
use datatypes::data_type::DataType;
use datatypes::prelude::ConcreteDataType;
use datatypes::schema::ColumnSchema;
use datatypes::value::Value as GtValue;
use pgwire::error::PgWireResult;

use super::{invalid_text_repr, unsupported};

/// A parsed, insertable row: one gRPC value per targeted column, in COPY
/// field order. Parsers emit these directly — the `text` family converts
/// with the helpers below, `csv` converts arrow batches with the shared
/// `api::helper::vectors_to_rows`.
pub(super) type ParsedRow = api::v1::Row;

/// One parsed field of a text-family record.
pub(super) enum Field {
    Null,
    Value(Vec<u8>),
}

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
/// create. The default `check_column_type` accepts the types whose
/// PostgreSQL text representation the shared helpers below can parse; a
/// codec with its own typed decoding (like `csv`) overrides it.
pub(super) trait CopyInCodec: Send + Sync + std::fmt::Debug {
    /// The canonical format name, used in error messages.
    fn name(&self) -> &'static str;
    /// The format code advertised in the `CopyInResponse` header:
    /// 0 for text, 1 for binary.
    fn format_code(&self) -> i8 {
        0
    }
    /// Validates that a column of `data_type` can be ingested by this
    /// format.
    fn check_column_type(&self, data_type: &ConcreteDataType) -> PgWireResult<()> {
        check_text_representable_type(data_type)
    }
    /// Creates the stateful parser for one copy stream over the targeted
    /// columns (in COPY field order).
    fn create_parser(&self, columns: &[ColumnSchema])
    -> PgWireResult<Box<dyn RecordParser + Send>>;
}

/// The default [`CopyInCodec::check_column_type`] for text-family
/// formats: column types whose PostgreSQL text representation the
/// helpers below can parse.
fn check_text_representable_type(data_type: &ConcreteDataType) -> PgWireResult<()> {
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

/// Converts one text-format field into a gRPC value following the column
/// type; shared by the text-family codecs.
pub(super) fn convert_text_field(
    column: &ColumnSchema,
    field: Field,
) -> PgWireResult<api::v1::Value> {
    let value = match field {
        Field::Null => GtValue::Null,
        Field::Value(bytes) => convert_text_value(&column.data_type, &bytes)?,
    };
    Ok(to_grpc_value(value))
}

fn convert_text_value(data_type: &ConcreteDataType, bytes: &[u8]) -> PgWireResult<GtValue> {
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
    let (pairs, _) = bytes.as_chunks::<2>();
    let mut out = Vec::with_capacity(pairs.len());
    for &[hi, lo] in pairs {
        let hi = (hi as char).to_digit(16).ok_or(())?;
        let lo = (lo as char).to_digit(16).ok_or(())?;
        out.push((hi * 16 + lo) as u8);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_check_text_representable_type() {
        assert!(check_text_representable_type(&ConcreteDataType::int32_datatype()).is_ok());
        assert!(
            check_text_representable_type(&ConcreteDataType::timestamp_millisecond_datatype())
                .is_ok()
        );
        let err = check_text_representable_type(&ConcreteDataType::time_second_datatype());
        assert!(err.unwrap_err().to_string().contains("not supported"));
    }

    #[test]
    fn test_decode_bytea() {
        assert_eq!(decode_bytea(b"\\x616263").unwrap(), b"abc");
        assert_eq!(decode_bytea(b"plain").unwrap(), b"plain");
        assert!(decode_bytea(b"\\xzz").is_err());
        assert!(decode_bytea(b"\\x6").is_err());
    }
}
