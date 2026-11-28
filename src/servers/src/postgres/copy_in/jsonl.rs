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

//! The `jsonl` format: newline-delimited JSON objects, one per record.
//!
//! Parsing and typed conversion both reuse arrow-json's incremental
//! [`Decoder`](arrow::json::reader::Decoder) — the engine behind
//! GreptimeDB's file-based `COPY ... FROM 'file.json'` (see
//! `common-datasource`'s `json` file format) — so both COPY routes
//! accept identical data. The decoder splits records (objects may span
//! chunk boundaries; pretty-printed multi-line objects and concatenated
//! `{...}{...}` streams are accepted too) and converts fields against
//! the targeted columns' schema in one pass; a record is one JSON
//! object whose keys address the columns **by name** (the COPY column
//! list fixes the target order, as with csv).
//!
//! What the engine actually accepts, so the rules match the behavior
//! rather than a strict reading of the column types:
//!
//! - Numeric columns take JSON numbers **or numeric strings**
//!   (`"val": 1.5` and `"val": "1.5"` both decode); booleans must be
//!   JSON booleans (`"b": "true"` is rejected).
//! - Timestamps are date-time strings (RFC 3339; offset-free ones parse
//!   in the session timezone, like SQL INSERT) **or numbers**, taken as
//!   the column's time unit verbatim: `"ts": 1704067200000` is an epoch
//!   in milliseconds for a millisecond column — there is no unit
//!   conversion, and `"ts": 1704067200` means 1970-01-20 in that column.
//!   A quoted number is parsed as a date-time string and so fails.
//! - Keys outside the column list are ignored, so a misspelled key
//!   (`"hots"`) yields NULL in its column instead of an error, and
//!   records may carry more keys than a column subset selects.
//!   (arrow-json's strict mode would change the first half of that but
//!   also reject the second, so it stays off.)
//! - Absent keys become NULL; a NULL in a non-nullable column (such as
//!   the time index) is rejected with a message naming the column.
//! - Column types arrow-json cannot decode (JSON, Binary) are rejected
//!   up front.
//!
//! The format is table-bound like `csv` and takes no options; the
//! file-based json format's layout options are writer-side only, and
//! arrow-json's reader parses date-time strings natively (RFC 3339,
//! with the schema's timezone for offset-free strings). `json` is an
//! alias for `jsonl`, matching the file-based format's name; it still
//! means newline-delimited objects — a top-level JSON array `[{...}]`
//! is rejected with a specific hint.

use std::sync::Arc;

use api::helper::vectors_to_rows;
use arrow::json::reader::{Decoder as JsonDecoder, ReaderBuilder};
use arrow::record_batch::RecordBatch;
use arrow_schema::{ArrowError, DataType as ArrowDataType, Field, Schema as ArrowSchema};
use datatypes::prelude::ConcreteDataType;
use datatypes::schema::{ColumnSchema, Schema as GreptimeSchema};
use datatypes::vectors::Helper;
use pgwire::error::{PgWireError, PgWireResult};

use crate::postgres::copy_in::codec::{
    CopyFormat, ParsedRow, RecordParser, TableBoundCodec, check_arrow_column_type,
};
use crate::postgres::copy_in::options::CopyOptionSet;
use crate::postgres::copy_in::{COPY_IN_FLUSH_ROWS, bad_copy_data, invalid_text_repr, unsupported};

/// The validated `jsonl` format configuration.
#[derive(Debug, Clone, Default)]
pub(super) struct JsonlCodec;

/// Builds the codec from the parsed options.
pub(super) fn build_jsonl_codec(options: CopyOptionSet) -> PgWireResult<CopyFormat> {
    options.reject_leftovers("jsonl")?;
    Ok(CopyFormat::Jsonl(JsonlCodec))
}

impl TableBoundCodec for JsonlCodec {
    fn name(&self) -> &'static str {
        "jsonl"
    }

    fn check_column_type(&self, data_type: &ConcreteDataType) -> PgWireResult<()> {
        check_arrow_column_type(data_type, "jsonl")
    }

    fn create_parser(
        &self,
        columns: &[ColumnSchema],
        timezone: &str,
    ) -> PgWireResult<Box<dyn RecordParser + Send>> {
        // Decode schema: the greptime schema's arrow projection, with
        // Timestamp fields carrying the SESSION timezone so offset-free
        // strings parse like SQL INSERT does instead of as UTC (the same
        // substitution the csv codec makes; explicit offsets in the
        // input always win). Every field is made nullable: a missing or
        // null value in a non-nullable column then surfaces as a NULL
        // that `flush_batch` reports by column name, instead of
        // arrow-json's generic "unmasked nulls in non-nullable
        // StructArray child" error.
        let arrow_schema = GreptimeSchema::try_new(columns.to_vec())
            .map_err(|e| unsupported(format!("invalid columns for format jsonl: {e}")))?
            .arrow_schema()
            .clone();
        let timezone = timezone.to_string();
        let fields = arrow_schema
            .fields()
            .iter()
            .zip(columns)
            .map(|(field, column)| {
                let data_type = match &column.data_type {
                    ConcreteDataType::Timestamp(_) => match field.data_type() {
                        ArrowDataType::Timestamp(unit, None) => {
                            ArrowDataType::Timestamp(*unit, Some(Arc::from(timezone.as_str())))
                        }
                        _ => field.data_type().clone(),
                    },
                    _ => field.data_type().clone(),
                };
                Field::new(field.name(), data_type, true)
            })
            .collect::<Vec<_>>();
        let builder = ReaderBuilder::new(Arc::new(ArrowSchema::new(fields)))
            .with_batch_size(COPY_IN_FLUSH_ROWS);
        Ok(Box::new(JsonlRecordParser {
            decoder: builder.build_decoder().map_err(decode_error)?,
            required: columns
                .iter()
                .enumerate()
                .filter(|(_, column)| !column.is_nullable())
                .map(|(index, column)| (index, column.name.clone()))
                .collect(),
            started: false,
        }))
    }
}

/// Incremental parser backed by arrow-json's decoder. Unlike
/// arrow-csv, a record finalizes as soon as its closing `}` is parsed,
/// and a single `flush` drains and clears the buffered tape — so there
/// is no EOF signal to send at `CopyDone`.
struct JsonlRecordParser {
    decoder: JsonDecoder,
    /// The non-nullable target columns (position and name), checked per
    /// decoded batch because the decode schema marks every field
    /// nullable (see [`TableBoundCodec::create_parser`]).
    required: Vec<(usize, String)>,
    /// Whether the first non-whitespace byte of the stream has been
    /// seen; only that byte can be a top-level array's `[`.
    started: bool,
}

const TOP_LEVEL_ARRAY_HINT: &str = "format jsonl expects newline-delimited JSON objects, one \
     per record; a top-level JSON array is not supported, write one object per line instead";

/// arrow-json's whitespace set, which `Decoder::decode` skips freely.
fn is_json_whitespace(byte: u8) -> bool {
    matches!(byte, b' ' | b'\t' | b'\n' | b'\r')
}

impl RecordParser for JsonlRecordParser {
    fn decode(&mut self, data: &[u8]) -> PgWireResult<usize> {
        if !self.started {
            // Report the most likely misuse of the `json` alias — a
            // top-level array — specifically; arrow-json would only say
            // `expected { got [...]`.
            if let Some(first) = data.iter().copied().find(|&byte| !is_json_whitespace(byte)) {
                self.started = true;
                if first == b'[' {
                    return Err(bad_copy_data(TOP_LEVEL_ARRAY_HINT));
                }
            }
        }
        self.decoder.decode(data).map_err(decode_error)
    }

    fn is_full(&self) -> bool {
        self.decoder.len() == COPY_IN_FLUSH_ROWS && !self.decoder.has_partial_record()
    }

    fn flush(&mut self) -> PgWireResult<Vec<ParsedRow>> {
        match self.decoder.flush().map_err(decode_error)? {
            Some(batch) => self.flush_batch(&batch),
            None => Ok(Vec::new()),
        }
    }
}

impl JsonlRecordParser {
    /// Converts one decoded arrow batch into insertable rows with the
    /// shared [`vectors_to_rows`] helper, after checking the
    /// non-nullable columns.
    fn flush_batch(&self, batch: &RecordBatch) -> PgWireResult<Vec<ParsedRow>> {
        for (index, name) in &self.required {
            if batch.column(*index).null_count() > 0 {
                return Err(bad_copy_data(format!(
                    "null value in non-nullable column \"{name}\"; every jsonl record must set it"
                )));
            }
        }
        let vectors = Helper::try_into_vectors(batch.columns())
            .map_err(|e| bad_copy_data(format!("failed to convert decoded JSON batch: {e}")))?;
        Ok(vectors_to_rows(vectors.iter(), batch.num_rows()))
    }
}

/// Maps arrow-json errors to PostgreSQL's COPY error classes, the
/// way `csv.rs` does. A field value that does not decode against its
/// column type is an invalid text representation (22P02): arrow-json
/// wraps those as `JsonError("whilst decoding field '<name>': …")` (see
/// its `struct_array.rs`), since it has no separate `ParseError` for
/// them. JSON syntax errors (truncated records, unexpected tokens) and
/// anything else is bad copy data (22P04) — never an internal `XX000`.
fn decode_error(error: ArrowError) -> PgWireError {
    const FIELD_PREFIX: &str = "whilst decoding field '";
    match &error {
        ArrowError::ParseError(message) | ArrowError::CastError(message) => {
            invalid_text_repr(format!("failed to parse JSON data: {message}"))
        }
        ArrowError::JsonError(message) if message.starts_with(FIELD_PREFIX) => {
            invalid_text_repr(format!("failed to parse JSON data: {message}"))
        }
        _ => bad_copy_data(format!("failed to parse JSON data: {error}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::postgres::copy_in::codec::CopyFormat;
    use crate::postgres::copy_in::options::{CopyOptionSet, CopyOptionValue};
    use api::v1::value::ValueData;
    use datatypes::schema::ColumnSchema;

    fn columns() -> Vec<ColumnSchema> {
        vec![
            ColumnSchema::new("host", ConcreteDataType::string_datatype(), true),
            ColumnSchema::new("val", ConcreteDataType::float64_datatype(), true),
            ColumnSchema::new(
                "ts",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            ),
        ]
    }

    fn codec() -> JsonlCodec {
        match build_jsonl_codec(CopyOptionSet::default()).unwrap() {
            CopyFormat::Jsonl(codec) => codec,
            other => panic!("unexpected format {other:?}"),
        }
    }

    fn new_parser(columns: &[ColumnSchema]) -> Box<dyn RecordParser + Send> {
        codec().create_parser(columns, "UTC").unwrap()
    }

    fn feed_all(parser: &mut Box<dyn RecordParser + Send>, chunks: &[&[u8]]) -> Vec<ParsedRow> {
        let mut rows = Vec::new();
        for chunk in chunks {
            rows.extend(parser.feed(chunk).unwrap());
        }
        rows.extend(parser.finish().unwrap());
        rows
    }

    fn string_at(row: &ParsedRow, col: usize) -> Option<String> {
        row.values[col].value_data.as_ref().map(|v| match v {
            ValueData::StringValue(s) => s.clone(),
            other => panic!("expected string, got {other:?}"),
        })
    }

    fn f64_at(row: &ParsedRow, col: usize) -> Option<f64> {
        match row.values[col].value_data.as_ref() {
            None => None,
            Some(ValueData::F64Value(v)) => Some(*v),
            other => panic!("expected f64, got {other:?}"),
        }
    }

    fn ts_millis_at(row: &ParsedRow, col: usize) -> Option<i64> {
        match row.values[col].value_data.as_ref() {
            None => None,
            Some(ValueData::TimestampMillisecondValue(v)) => Some(*v),
            other => panic!("expected timestamp ms, got {other:?}"),
        }
    }

    /// The SQLSTATE a copy error is sent to the client with.
    fn error_code(error: &PgWireError) -> String {
        error.code().to_string()
    }

    /// Feeds `data` then finishes, returning the first error. arrow-json
    /// reports JSON syntax errors from `feed` but converts values at
    /// the flush in `finish`, so value-level failures surface there.
    fn error_feeding(parser: &mut Box<dyn RecordParser + Send>, data: &[u8]) -> PgWireError {
        match parser.feed(data).and_then(|_| parser.finish()) {
            Ok(_) => panic!("expected an error for {:?}", String::from_utf8_lossy(data)),
            Err(error) => error,
        }
    }

    #[test]
    fn test_jsonl_records_across_chunk_boundaries() {
        let payload: Vec<u8> = br#"{"host":"a","val":1.5,"ts":"2024-01-01 00:00:00"}
{"host":"b","val":2.5,"ts":"2024-01-01 00:00:01"}"#
            .to_vec();
        for chunk in [1usize, 2, 3, 7, 64] {
            let mut parser = new_parser(&columns());
            let rows = feed_all(&mut parser, &payload.chunks(chunk).collect::<Vec<_>>());
            assert_eq!(rows.len(), 2, "chunk={chunk}");
            assert_eq!(string_at(&rows[0], 0), Some("a".to_string()));
            assert!(matches!(
                rows[1].values[0].value_data,
                Some(ValueData::StringValue(ref s)) if s == "b"
            ));
        }
    }

    #[test]
    fn test_jsonl_absent_key_is_null() {
        let mut parser = new_parser(&columns());
        let rows = feed_all(
            &mut parser,
            &[b"{\"val\":3.5,\"ts\":\"2024-01-01 00:00:00\"}\n"],
        );
        assert_eq!(rows.len(), 1);
        assert!(rows[0].values[0].value_data.is_none());
    }

    #[test]
    fn test_jsonl_trailing_record_without_newline() {
        let mut parser = new_parser(&columns());
        let rows = feed_all(
            &mut parser,
            &[
                b"{\"host\":\"a\",\"val\":1.5,\"ts\":\"2024-01-01 00:00:00\"}\n",
                b"{\"host\":\"b\",\"val\":2.5,\"ts\":\"2024-01-01 00:00:01\"}",
            ],
        );
        assert_eq!(rows.len(), 2);
        assert_eq!(f64_at(&rows[1], 1), Some(2.5));
    }

    #[test]
    fn test_jsonl_numeric_columns_take_native_and_quoted_numbers() {
        let mut parser = new_parser(&columns());
        let rows = feed_all(
            &mut parser,
            &[
                b"{\"val\":2,\"ts\":\"2024-01-01 00:00:00\"}\n",
                b"{\"val\":\"1.5\",\"ts\":\"2024-01-01 00:00:01\"}\n",
            ],
        );
        assert_eq!(rows.len(), 2);
        assert_eq!(f64_at(&rows[0], 1), Some(2.0));
        assert_eq!(f64_at(&rows[1], 1), Some(1.5));

        // Booleans, by contrast, must be JSON booleans.
        let mut parser = new_parser(&[ColumnSchema::new(
            "b",
            ConcreteDataType::boolean_datatype(),
            true,
        )]);
        let error = error_feeding(&mut parser, b"{\"b\":\"true\"}\n");
        assert_eq!(error_code(&error), "22P02", "{error}");
        assert!(error.to_string().contains("expected boolean"), "{error}");
    }

    #[test]
    fn test_jsonl_numeric_timestamp_is_taken_in_the_column_unit() {
        let mut parser = new_parser(&columns());
        let rows = feed_all(
            &mut parser,
            &[
                b"{\"val\":1,\"ts\":1704067200000}\n",
                // A number is not re-scaled: 1704067200 is an epoch in
                // the column's unit, milliseconds here.
                b"{\"val\":2,\"ts\":1704067200}\n",
                // Explicit offsets in strings win over the session zone.
                b"{\"val\":3,\"ts\":\"2024-01-01T00:00:00+08:00\"}\n",
            ],
        );
        assert_eq!(rows.len(), 3);
        assert_eq!(ts_millis_at(&rows[0], 2), Some(1704067200000));
        assert_eq!(ts_millis_at(&rows[1], 2), Some(1704067200));
        assert_eq!(ts_millis_at(&rows[2], 2), Some(1704038400000));

        // A quoted number is parsed as a date-time string, and fails.
        let mut parser = new_parser(&columns());
        let error = error_feeding(&mut parser, b"{\"ts\":\"1704067200000\"}\n");
        assert_eq!(error_code(&error), "22P02", "{error}");
    }

    #[test]
    fn test_jsonl_unknown_keys_are_ignored() {
        let mut parser = new_parser(&columns());
        // A misspelled key does not error: its column stays NULL.
        let rows = feed_all(
            &mut parser,
            &[b"{\"hots\":\"a\",\"val\":1.5,\"ts\":\"2024-01-01 00:00:00\"}\n"],
        );
        assert_eq!(rows.len(), 1);
        assert!(rows[0].values[0].value_data.is_none());
        assert_eq!(f64_at(&rows[0], 1), Some(1.5));
    }

    #[test]
    fn test_jsonl_column_subset_and_reorder_tolerate_extra_keys() {
        // COPY t (ts, val) FROM STDIN: a reordered subset of the table's
        // columns, keyed by name; records still carry the other keys.
        let subset = vec![columns()[2].clone(), columns()[1].clone()];
        let mut parser = new_parser(&subset);
        let rows = feed_all(
            &mut parser,
            &[b"{\"host\":\"a\",\"val\":1.5,\"ts\":\"2024-01-01 00:00:00\",\"extra\":true}\n"],
        );
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].values.len(), 2);
        assert_eq!(ts_millis_at(&rows[0], 0), Some(1704067200000));
        assert_eq!(f64_at(&rows[0], 1), Some(1.5));
    }

    #[test]
    fn test_jsonl_null_in_non_nullable_column_is_named() {
        for record in [
            // Missing key.
            &b"{\"val\":1.5}\n"[..],
            // Explicit null.
            &b"{\"val\":1.5,\"ts\":null}\n"[..],
        ] {
            let mut parser = new_parser(&columns());
            let error = error_feeding(&mut parser, record);
            assert_eq!(error_code(&error), "22P04", "{record:?}: {error}");
            let message = error.to_string();
            assert!(message.contains("\"ts\""), "{message}");
            assert!(message.contains("non-nullable"), "{message}");
            assert!(!message.contains("StructArray"), "{message}");
        }
    }

    #[test]
    fn test_jsonl_top_level_array_is_rejected_with_hint() {
        let mut parser = new_parser(&columns());
        let error = parser
            .feed(b"[{\"val\":1.5,\"ts\":\"2024-01-01 00:00:00\"}]\n")
            .unwrap_err();
        assert_eq!(error_code(&error), "22P04", "{error}");
        assert!(error.to_string().contains("one object per line"), "{error}");

        // Leading whitespace, even across chunks, does not hide the `[`.
        let mut parser = new_parser(&columns());
        parser.feed(b"\n").unwrap();
        parser.feed(b" \t").unwrap();
        let error = parser.feed(b" [{\"val\":1}").unwrap_err();
        assert!(
            error.to_string().contains("top-level JSON array"),
            "{error}"
        );

        // A `[` later in the stream is a value, not an array of records:
        // it fails as an ordinary syntax/type error, without the hint.
        let mut parser = new_parser(&[ColumnSchema::new(
            "host",
            ConcreteDataType::string_datatype(),
            true,
        )]);
        let error = error_feeding(&mut parser, b"{\"host\":\"a\"}\n[1]\n");
        assert!(
            !error.to_string().contains("top-level JSON array"),
            "{error}"
        );
    }

    #[test]
    fn test_jsonl_syntax_errors_are_bad_copy_data() {
        // A syntax error fails the feed itself.
        let mut parser = new_parser(&columns());
        let error = parser.feed(b"hello\n").unwrap_err();
        assert_eq!(error_code(&error), "22P04", "{error}");

        // Value-level failures are invalid text representation.
        let mut parser = new_parser(&columns());
        let error = error_feeding(
            &mut parser,
            br#"{"host":"a","val":"not a number","ts":"2024-01-01 00:00:00"}
"#,
        );
        assert_eq!(error_code(&error), "22P02", "{error}");
        assert!(
            error.to_string().contains("failed to parse JSON data"),
            "{error}"
        );
    }

    #[test]
    fn test_jsonl_truncated_tail_fails_at_finish() {
        let mut parser = new_parser(&columns());
        parser
            .feed(b"{\"host\":\"a\",\"val\":1.5,\"ts\":\"2024-01-01 00:00:00\"}\n")
            .unwrap();
        parser.feed(b"{\"host\":\"b\",\"val\":2.5").unwrap();
        let error = parser.finish().unwrap_err();
        assert_eq!(error_code(&error), "22P04", "{error}");
        assert!(error.to_string().contains("Truncated record"), "{error}");
    }

    #[test]
    fn test_jsonl_multi_batch_sweep_flushes_mid_stream() {
        // More than two full batches, fed in chunks that can split records,
        // so batches must be detected and flushed before the stream ends.
        let total = 2 * COPY_IN_FLUSH_ROWS + 137;
        let mut payload = Vec::new();
        for i in 0..total {
            payload.extend_from_slice(format!("{{\"val\":{i},\"ts\":{i}}}\n").as_bytes());
        }
        let columns = columns();
        let mut parser = new_parser(&columns);
        let mut rows = Vec::new();
        let mut feeds_with_rows = 0;
        for chunk in payload.chunks(4093) {
            let batch = parser.feed(chunk).unwrap();
            if !batch.is_empty() {
                feeds_with_rows += 1;
            }
            rows.extend(batch);
        }
        let tail = parser.finish().unwrap();
        rows.extend(tail);

        assert_eq!(rows.len(), total);
        assert!(
            feeds_with_rows >= 2,
            "rows only surfaced at finish: the mid-stream flush path was not exercised"
        );
        for (i, row) in rows.iter().enumerate() {
            assert_eq!(ts_millis_at(row, 2), Some(i as i64), "row {i}");
            assert_eq!(f64_at(row, 1), Some(i as f64), "row {i}");
        }
    }

    #[test]
    fn test_jsonl_decode_error_classes() {
        let cases = [
            (ArrowError::ParseError("x".into()), "22P02"),
            (ArrowError::CastError("x".into()), "22P02"),
            (
                ArrowError::JsonError(
                    "whilst decoding field 'val': failed to parse \"a\" as Float64".into(),
                ),
                "22P02",
            ),
            (
                ArrowError::JsonError("Truncated record whilst reading string".into()),
                "22P04",
            ),
            (ArrowError::JsonError("expected { got [".into()), "22P04"),
            (ArrowError::NotYetImplemented("x".into()), "22P04"),
            (ArrowError::DivideByZero, "22P04"),
        ];
        for (error, expected) in cases {
            let mapped = decode_error(error);
            assert_eq!(error_code(&mapped), expected, "{mapped}");
            // Never an internal error.
            assert!(!mapped.to_string().contains("XX000"), "{mapped}");
        }
    }

    #[test]
    fn test_jsonl_rejects_options_and_bad_columns() {
        let mut set = CopyOptionSet::default();
        set.insert("header".into(), CopyOptionValue::Flag).unwrap();
        assert!(build_jsonl_codec(set).is_err());

        let codec = codec();
        assert_eq!(codec.name(), "jsonl");
        assert!(
            codec
                .check_column_type(&ConcreteDataType::json_datatype())
                .is_err()
        );
        assert!(
            codec
                .check_column_type(&ConcreteDataType::binary_datatype())
                .is_err()
        );
        assert!(
            codec
                .check_column_type(&ConcreteDataType::timestamp_millisecond_datatype())
                .is_ok()
        );
    }
}
