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

//! The `csv` format — currently the only implemented COPY FROM STDIN
//! format.
//!
//! Parsing and typed conversion both reuse arrow-csv's incremental
//! [`Decoder`](arrow::csv::reader::Decoder) — the same engine backing
//! GreptimeDB's file-based `COPY ... FROM 'file.csv'` (see
//! `common-datasource`'s `csv` file format) — so both COPY routes accept
//! identical data. The decoder splits records (quoting, escaped quotes,
//! custom delimiters, `\n`/`\r\n` separators, optional headers) and
//! converts fields against the targeted columns' schema in one pass.
//!
//! Consequences of sharing the engine: JSON and Binary columns are
//! rejected, and timestamps must be date/time strings (offset-free ones
//! parse in the session timezone). An unquoted empty field is NULL; a
//! quoted empty string is an empty string, restored by a shadow
//! quote-mask scan since the decoder nulls both.

use std::sync::Arc;

use api::helper::vectors_to_rows;
use arrow::csv::ReaderBuilder;
use arrow::csv::reader::Decoder as CsvDecoder;
use arrow_schema::{ArrowError, DataType as ArrowDataType, Field, Schema as ArrowSchema};
use datatypes::data_type::DataType;
use datatypes::prelude::ConcreteDataType;
use datatypes::schema::{ColumnSchema, Schema as GreptimeSchema};
use datatypes::vectors::{BooleanVector, Helper, StringVector, Vector, VectorRef};
use pgwire::error::{PgWireError, PgWireResult};

use crate::postgres::copy_in::codec::{CopyInCodec, ParsedRow, RecordParser};
use crate::postgres::copy_in::options::CopyOptionSet;
use crate::postgres::copy_in::{COPY_IN_FLUSH_ROWS, bad_copy_data, invalid_text_repr, unsupported};

/// The validated `csv` format configuration.
#[derive(Debug)]
pub(super) struct CsvCodec {
    delimiter: u8,
    quote: u8,
    escape: u8,
    header: bool,
}

/// Builds the CSV codec from the parsed options.
pub(super) fn build_csv_codec(mut options: CopyOptionSet) -> PgWireResult<Arc<dyn CopyInCodec>> {
    let delimiter = options.take_single_byte("DELIMITER")?.unwrap_or(b',');
    let quote = options.take_single_byte("QUOTE")?.unwrap_or(b'"');
    // In PostgreSQL the default escape character for CSV is the quote
    // character itself (doubled quotes inside quoted fields).
    let escape = options.take_single_byte("ESCAPE")?.unwrap_or(quote);
    // An empty string is the default NULL marker (an empty field is NULL);
    // a custom NULL string is not supported because arrow-csv's null
    // matching does not distinguish quoted from unquoted fields.
    if let Some(null) = options.take_string("NULL")?
        && !null.is_empty()
    {
        return Err(unsupported(
            "custom NULL string is not supported by format csv; an empty field is NULL",
        ));
    }
    let header = options.take_flag("HEADER")?.unwrap_or(false);
    options.reject_leftovers("csv")?;
    if delimiter == quote {
        return Err(bad_copy_data("DELIMITER must not be equal to QUOTE"));
    }
    Ok(Arc::new(CsvCodec {
        delimiter,
        quote,
        escape,
        header,
    }))
}

impl CopyInCodec for CsvCodec {
    fn name(&self) -> &'static str {
        "csv"
    }

    /// The column types arrow-csv can decode against.
    fn check_column_type(&self, data_type: &ConcreteDataType) -> PgWireResult<()> {
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
                | ConcreteDataType::Date(_)
                | ConcreteDataType::Timestamp(_)
                | ConcreteDataType::Decimal128(_)
        );
        if !supported {
            return Err(unsupported(format!(
                "column type \"{}\" is not supported by format csv",
                data_type.name()
            )));
        }
        Ok(())
    }

    fn create_parser(
        &self,
        columns: &[ColumnSchema],
        timezone: &str,
    ) -> PgWireResult<Box<dyn RecordParser + Send>> {
        // Decode schema: the greptime schema's arrow projection, except
        // Boolean columns decode as Utf8 — arrow-csv only accepts
        // `true`/`false`, while PostgreSQL COPY IN accepts its full set
        // of boolean spellings (`t`, `f`, `yes`, `on`, ...), which COPY
        // OUT emits — and Timestamp fields carry the SESSION timezone so
        // offset-free strings parse like SQL INSERT does instead of as
        // UTC. The substitutions are applied in `flush_batch` and by the
        // decoder's tz-aware parsing respectively; explicit offsets in
        // the input always win.
        let boolean_columns: Vec<usize> = columns
            .iter()
            .enumerate()
            .filter(|(_, c)| matches!(c.data_type, ConcreteDataType::Boolean(_)))
            .map(|(i, _)| i)
            .collect();
        let arrow_schema = GreptimeSchema::try_new(columns.to_vec())
            .map_err(|e| unsupported(format!("invalid columns for format csv: {e}")))?
            .arrow_schema()
            .clone();
        let timezone = timezone.to_string();
        let fields = arrow_schema
            .fields()
            .iter()
            .zip(columns)
            .map(|(field, column)| match &column.data_type {
                ConcreteDataType::Boolean(_) => {
                    Field::new(field.name(), ArrowDataType::Utf8, field.is_nullable())
                }
                ConcreteDataType::Timestamp(_) => match field.data_type() {
                    ArrowDataType::Timestamp(unit, None) => Field::new(
                        field.name(),
                        ArrowDataType::Timestamp(*unit, Some(Arc::from(timezone.as_str()))),
                        field.is_nullable(),
                    ),
                    _ => field.as_ref().clone(),
                },
                // String columns decode as NULLABLE: the decoder turns a
                // quoted empty string into NULL, and a NOT NULL field
                // would make `decoder.flush()` reject the batch before
                // `flush_batch` can restore it to an empty string. The
                // target's NOT NULL constraint is enforced there instead.
                ConcreteDataType::String(_) => Field::new(field.name(), ArrowDataType::Utf8, true),
                _ => field.as_ref().clone(),
            })
            .collect::<Vec<_>>();
        let mut builder = ReaderBuilder::new(Arc::new(ArrowSchema::new(fields)))
            .with_header(self.header)
            .with_delimiter(self.delimiter)
            .with_quote(self.quote)
            .with_batch_size(COPY_IN_FLUSH_ROWS);
        // An escape character equal to the quote is the doubled-quote
        // default; csv-core handles that through quoting alone.
        if self.escape != self.quote {
            builder = builder.with_escape(self.escape);
        }
        let string_columns = columns
            .iter()
            .enumerate()
            .filter(|(_, c)| matches!(c.data_type, ConcreteDataType::String(_)))
            .map(|(i, c)| (i, c.clone()))
            .collect::<Vec<_>>();
        Ok(Box::new(CsvRecordParser {
            decoder: builder.build_decoder(),
            boolean_columns,
            string_columns,
            masks: std::collections::VecDeque::new(),
            scanner: QuoteMaskScanner::new(self.delimiter, self.quote, self.escape, self.header),
        }))
    }
}

/// Incremental parser backed by arrow-csv's decoder.
struct CsvRecordParser {
    decoder: CsvDecoder,
    /// Indices of Boolean columns, decoded as Utf8 and converted by
    /// [`flush_batch`] to accept PostgreSQL's boolean spellings.
    boolean_columns: Vec<usize>,
    /// Indices of String columns: the decoder nulls every empty field, so
    /// a quoted empty string (`""` — an empty STRING in PostgreSQL) is
    /// indistinguishable from an unquoted empty field (NULL) after the
    /// decode. A shadow scan of the same bytes records per field whether
    /// it was quoted; [`flush_batch`] then restores quoted empty strings.
    /// The payload carries the TARGET nullability of the column so the
    /// NOT NULL constraint lost in the (always-nullable) decode schema
    /// can be enforced after restoration.
    string_columns: Vec<(usize, ColumnSchema)>,
    /// Shadow scan of the raw bytes: one quoted-mask per completed record,
    /// aligned with the decoder's emitted rows.
    masks: std::collections::VecDeque<Vec<bool>>,
    scanner: QuoteMaskScanner,
}

impl RecordParser for CsvRecordParser {
    fn feed(&mut self, data: &[u8]) -> PgWireResult<Vec<ParsedRow>> {
        // The shadow scan sees the same bytes as the decoder, so its
        // per-record masks line up with the decoder's emitted rows.
        self.masks.extend(self.scanner.feed(data));
        let mut buf = data;
        let mut rows = Vec::new();
        while !buf.is_empty() {
            let consumed = self.decoder.decode(buf).map_err(decode_error)?;
            if consumed == 0 {
                while let Some(batch) = self.decoder.flush().map_err(decode_error)? {
                    rows.extend(self.flush_batch(&batch)?);
                }
            } else {
                buf = &buf[consumed..];
            }
        }
        Ok(rows)
    }

    fn finish(&mut self) -> PgWireResult<Vec<ParsedRow>> {
        // An empty decode signals EOF, finalizing a trailing record that
        // lacks its record separator.
        self.decoder.decode(&[]).map_err(decode_error)?;
        self.masks.extend(self.scanner.finish());
        let mut rows = Vec::new();
        while let Some(batch) = self.decoder.flush().map_err(decode_error)? {
            rows.extend(self.flush_batch(&batch)?);
        }
        Ok(rows)
    }
}

impl CsvRecordParser {
    fn flush_batch(
        &mut self,
        batch: &arrow::record_batch::RecordBatch,
    ) -> PgWireResult<Vec<ParsedRow>> {
        let mut vectors = Helper::try_into_vectors(batch.columns())
            .map_err(|e| bad_copy_data(format!("failed to convert decoded CSV batch: {e}")))?;
        for &index in &self.boolean_columns {
            let Some(vector) = vectors[index].as_any().downcast_ref::<StringVector>() else {
                return Err(bad_copy_data(
                    "expected boolean column to decode as a string vector",
                ));
            };
            let mut values = Vec::with_capacity(batch.num_rows());
            for row_index in 0..batch.num_rows() {
                values.push(match vector.get(row_index) {
                    datatypes::value::Value::Null => None,
                    datatypes::value::Value::String(s) => Some(parse_pg_bool(s.as_utf8())?),
                    other => {
                        return Err(bad_copy_data(format!(
                            "expected string values in a boolean column, got {other:?}"
                        )));
                    }
                });
            }
            vectors[index] = Arc::new(BooleanVector::from(values));
        }
        // Restore quoted empty strings: after the decode they are NULL
        // exactly like unquoted empty fields, which stay NULL. Then
        // enforce the target NOT NULL constraint, which the (always
        // nullable) decode schema cannot.
        for row_index in 0..batch.num_rows() {
            let Some(mask) = self.masks.pop_front() else {
                return Err(bad_copy_data(
                    "internal error: quote masks out of sync with decoded rows",
                ));
            };
            for &(index, ref column) in &self.string_columns {
                if vectors[index].get(row_index).is_null() {
                    if mask.get(index).copied().unwrap_or(false) {
                        vectors[index] = set_string_at(vectors[index].clone(), row_index, "");
                    } else if !column.is_nullable() {
                        return Err(bad_copy_data(format!(
                            "null value in column \"{}\" violates not-null constraint",
                            column.name
                        )));
                    }
                }
            }
        }
        Ok(vectors_to_rows(vectors.iter(), batch.num_rows()))
    }
}

/// Replaces a null string slot with an empty string, returning a new
/// vector (immutable vectors cannot be updated in place).
fn set_string_at(vector: VectorRef, row_index: usize, value: &str) -> VectorRef {
    let string_vector = vector
        .as_any()
        .downcast_ref::<StringVector>()
        .expect("string column decodes as StringVector");
    let mut values = Vec::with_capacity(string_vector.len());
    for i in 0..string_vector.len() {
        values.push(match string_vector.get(i) {
            datatypes::value::Value::String(s) => Some(s.as_utf8().to_string()),
            _ if i == row_index => Some(value.to_string()),
            _ => None,
        });
    }
    Arc::new(StringVector::from(values))
}

/// Incremental scan of raw CSV bytes recording, per completed record,
/// whether each field was quoted. Mirrors the decoder's own splitting
/// rules — delimiter, quoting, escapes, LF/CRLF/CR record ends, blank
/// lines skipped, chunk-boundary carry — so the masks align with its
/// output; field VALUES are not materialized.
struct QuoteMaskScanner {
    delimiter: u8,
    quote: u8,
    escape: u8,
    state: MaskState,
    /// Whether the current field was quoted (indexed by field position).
    fields: Vec<bool>,
    /// A byte deferred until the next chunk (see the decoder-side rules).
    carry: Option<u8>,
    skip_first: bool,
    header_seen: bool,
}

#[derive(PartialEq, Eq)]
enum MaskState {
    FieldStart,
    InUnquoted,
    InQuoted,
    QuoteInQuoted,
}

impl QuoteMaskScanner {
    fn new(delimiter: u8, quote: u8, escape: u8, has_header: bool) -> Self {
        Self {
            delimiter,
            quote,
            escape,
            state: MaskState::FieldStart,
            fields: Vec::new(),
            carry: None,
            skip_first: has_header,
            header_seen: false,
        }
    }

    fn feed(&mut self, data: &[u8]) -> Vec<Vec<bool>> {
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
                MaskState::FieldStart => {
                    if b == self.quote {
                        self.state = MaskState::InQuoted;
                        self.fields.push(true);
                        idx += 1;
                    } else if b == self.delimiter {
                        self.fields.push(false);
                        idx += 1;
                    } else if is_record_end(b) {
                        if let Some(record) = self.end_record() {
                            records.push(record);
                        }
                        idx += record_end_len(b, next);
                    } else if is_incomplete_record_end(b, next) {
                        self.carry = Some(b);
                        return records;
                    } else {
                        self.fields.push(false);
                        self.state = MaskState::InUnquoted;
                        idx += 1;
                    }
                }
                MaskState::InUnquoted => {
                    if b == self.delimiter {
                        self.state = MaskState::FieldStart;
                        idx += 1;
                    } else if is_record_end(b) {
                        if let Some(record) = self.end_record() {
                            records.push(record);
                        }
                        idx += record_end_len(b, next);
                    } else if is_incomplete_record_end(b, next) {
                        self.carry = Some(b);
                        return records;
                    } else {
                        idx += 1;
                    }
                }
                MaskState::InQuoted => {
                    if self.escape != self.quote && b == self.escape {
                        if let Some(n) = next {
                            let _ = n;
                            idx += 2;
                        } else {
                            self.carry = Some(b);
                            return records;
                        }
                    } else if b == self.quote {
                        self.state = MaskState::QuoteInQuoted;
                        idx += 1;
                    } else {
                        idx += 1;
                    }
                }
                MaskState::QuoteInQuoted => {
                    if b == self.quote && self.escape == self.quote {
                        // Doubled quote (quote doubling is the escape):
                        // consume only the second quote and stay inside the
                        // quoted field. Skipping further bytes would also
                        // swallow a following delimiter or closing quote
                        // and desynchronize the mask from the decoder.
                        self.state = MaskState::InQuoted;
                        idx += 1;
                    } else if b == self.delimiter {
                        self.state = MaskState::FieldStart;
                        idx += 1;
                    } else if is_record_end(b) {
                        if let Some(record) = self.end_record() {
                            records.push(record);
                        }
                        idx += record_end_len(b, next);
                    } else if is_incomplete_record_end(b, next) {
                        self.carry = Some(b);
                        return records;
                    } else {
                        // Stray data after a closing quote: keep scanning
                        // the field; the decoder is the authority on
                        // whether this is an error.
                        idx += 1;
                    }
                }
            }
        }
        records
    }

    fn finish(&mut self) -> Vec<Vec<bool>> {
        if let Some(b) = self.carry.take()
            && b == b'\r'
        {
            // A lone `\r` at end of stream terminates the record.
            return self.end_record().into_iter().collect();
        }
        if self.state != MaskState::FieldStart || !self.fields.is_empty() {
            return self.end_record().into_iter().collect();
        }
        Vec::new()
    }

    fn end_record(&mut self) -> Option<Vec<bool>> {
        self.state = MaskState::FieldStart;
        let record = std::mem::take(&mut self.fields);
        if record.is_empty() {
            // A completely blank line produces no decoded row (csv-core
            // skips it): no mask, and the header expectation is not
            // consumed either.
            None
        } else if self.skip_first && !self.header_seen {
            // The header record produces no data row; drop its mask.
            self.header_seen = true;
            None
        } else {
            Some(record)
        }
    }
}

/// The decoder accepts LF, CRLF, and CR alone as record terminators.
fn is_record_end(b: u8) -> bool {
    b == b'\n' || b == b'\r'
}

/// A `\r` at the end of a chunk may still turn into CRLF once the next
/// chunk arrives; defer it via `carry` instead of guessing.
fn is_incomplete_record_end(b: u8, next: Option<u8>) -> bool {
    b == b'\r' && next.is_none()
}

fn record_end_len(b: u8, next: Option<u8>) -> usize {
    if b == b'\r' && next == Some(b'\n') {
        2
    } else {
        1
    }
}

/// Parses PostgreSQL's boolean spellings: `true`/`false`, `t`/`f`,
/// `yes`/`no`, `on`/`off` and `1`/`0`, case-insensitive with surrounding
/// whitespace allowed (see the BOOLEAN type's input grammar). COPY OUT
/// emits `t`/`f`, so accepting them keeps both COPY directions
/// round-trippable.
fn parse_pg_bool(input: &str) -> PgWireResult<bool> {
    match input.trim().to_ascii_lowercase().as_str() {
        "true" | "t" | "yes" | "y" | "on" | "1" => Ok(true),
        "false" | "f" | "no" | "n" | "off" | "0" => Ok(false),
        _ => Err(invalid_text_repr(format!(
            "invalid input syntax for type boolean: \"{input}\""
        ))),
    }
}

fn decode_error(error: ArrowError) -> PgWireError {
    match &error {
        ArrowError::CsvError(message) => {
            bad_copy_data(format!("failed to parse CSV data: {message}"))
        }
        ArrowError::ParseError(message) | ArrowError::CastError(message) => {
            invalid_text_repr(format!("failed to parse CSV data: {message}"))
        }
        _ => bad_copy_data(format!("failed to parse CSV data: {error}")),
    }
}

#[cfg(test)]
mod tests {
    use datatypes::schema::ColumnSchema;

    use super::*;
    use crate::postgres::copy_in::options::CopyOptionValue;

    fn test_columns() -> Vec<ColumnSchema> {
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

    fn parser() -> Box<dyn RecordParser + Send> {
        build_csv_codec(CopyOptionSet::default())
            .unwrap()
            .create_parser(&test_columns(), "UTC")
            .unwrap()
    }

    fn string_at(row: &ParsedRow, index: usize) -> Option<String> {
        row.values[index].value_data.as_ref().map(|v| match v {
            api::v1::value::ValueData::StringValue(s) => s.clone(),
            other => panic!("expected string, got {other:?}"),
        })
    }

    fn rows_with(mut set: CopyOptionSet) -> Box<dyn RecordParser + Send> {
        set.take_format().unwrap();
        build_csv_codec(set)
            .unwrap()
            .create_parser(&test_columns(), "UTC")
            .unwrap()
    }

    /// Feeds chunks and then the EOF signal, returning all rows: with the
    /// strict decoder protocol, feeds only emit full
    /// `COPY_IN_FLUSH_ROWS` batches; a trailing partial batch surfaces at
    /// `finish`.
    fn feed_all(parser: &mut Box<dyn RecordParser + Send>, chunks: &[&[u8]]) -> Vec<ParsedRow> {
        let mut rows = Vec::new();
        for chunk in chunks {
            rows.extend(parser.feed(chunk).unwrap());
        }
        rows.extend(parser.finish().unwrap());
        rows
    }

    #[test]
    fn test_build_defaults() {
        let codec = build_csv_codec(CopyOptionSet::default()).unwrap();
        assert_eq!(codec.name(), "csv");
    }

    #[test]
    fn test_build_rejects_bad_options() {
        let mut set = CopyOptionSet::default();
        set.insert("delimiter".into(), CopyOptionValue::String("\"".into()))
            .unwrap();
        let err = build_csv_codec(set).unwrap_err();
        assert!(
            err.to_string()
                .contains("DELIMITER must not be equal to QUOTE")
        );

        let mut set = CopyOptionSet::default();
        set.insert("null".into(), CopyOptionValue::String("NULL".into()))
            .unwrap();
        assert!(
            build_csv_codec(set)
                .unwrap_err()
                .to_string()
                .contains("custom NULL string")
        );

        let mut set = CopyOptionSet::default();
        set.take_format().unwrap();
        set.insert("precision".into(), CopyOptionValue::Ident("ns".into()))
            .unwrap();
        assert!(
            build_csv_codec(set)
                .unwrap_err()
                .to_string()
                .contains("not supported by format csv")
        );
    }

    #[test]
    fn test_check_column_type() {
        let codec = build_csv_codec(CopyOptionSet::default()).unwrap();
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
                .check_column_type(&ConcreteDataType::string_datatype())
                .is_ok()
        );
    }

    #[test]
    fn test_csv_parse_records() {
        let mut parser = parser();
        let rows = feed_all(
            &mut parser,
            &[b"host1,1.5,2023-11-14 22:13:20\n\
                ,2.5,2023-11-14 22:13:21\n\
                \"quoted, comma\",3.5,2023-11-14 22:13:22\n"],
        );
        assert_eq!(rows.len(), 3);
        assert_eq!(string_at(&rows[0], 0), Some("host1".into()));
        // An empty field is NULL.
        assert_eq!(string_at(&rows[1], 0), None);
        assert_eq!(string_at(&rows[2], 0), Some("quoted, comma".into()));
    }

    #[test]
    fn test_csv_escaped_quotes() {
        let mut parser = parser();
        let rows = feed_all(
            &mut parser,
            &[b"\"say \"\"hi\"\"\",1.0,2023-11-14 22:13:20\n"],
        );
        assert_eq!(string_at(&rows[0], 0), Some("say \"hi\"".into()));
    }

    #[test]
    fn test_csv_custom_escape() {
        let mut set = CopyOptionSet::default();
        set.insert("escape".into(), CopyOptionValue::String("\\".into()))
            .unwrap();
        let mut parser = rows_with(set);
        let rows = feed_all(&mut parser, &[b"\"a\\\"b\",1.0,2023-11-14 22:13:20\n"]);
        assert_eq!(string_at(&rows[0], 0), Some("a\"b".into()));
    }

    #[test]
    fn test_csv_quoted_newline_and_chunk_boundaries() {
        let mut parser = parser();
        // Split inside a quoted field containing a newline, and a CRLF
        // record separator split across chunks. No batch is full, so all
        // feeds return nothing and both rows surface at the EOF signal.
        assert!(parser.feed(b"\"multi").unwrap().is_empty());
        assert!(
            parser
                .feed(b"line\nfield\",1.0,2023-11-14 22:13:20")
                .unwrap()
                .is_empty()
        );
        assert!(
            parser
                .feed(b"\r\nhost2,2.0,2023-11-14 22:13:21\n")
                .unwrap()
                .is_empty()
        );
        let rows = parser.finish().unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(string_at(&rows[0], 0), Some("multiline\nfield".into()));
        assert_eq!(string_at(&rows[1], 0), Some("host2".into()));
    }

    #[test]
    fn test_csv_header() {
        let mut set = CopyOptionSet::default();
        set.insert("header".into(), CopyOptionValue::Flag).unwrap();
        let mut parser = rows_with(set);
        let rows = feed_all(
            &mut parser,
            &[b"host,val,ts\nhost1,1.0,2023-11-14 22:13:20\n"],
        );
        assert_eq!(rows.len(), 1);
        assert_eq!(string_at(&rows[0], 0), Some("host1".into()));
    }

    #[test]
    fn test_csv_trailing_record_without_newline() {
        let mut parser = parser();
        let fed = parser
            .feed(b"host1,1.0,2023-11-14 22:13:20\nhost2,2.0,2023-11-14 22:13:21")
            .unwrap();
        // While the trailing record is pending, flush cannot emit the
        // completed one either; both surface at the EOF signal.
        assert!(fed.is_empty());
        let rows = parser.finish().unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(string_at(&rows[0], 0), Some("host1".into()));
        assert_eq!(string_at(&rows[1], 0), Some("host2".into()));
    }

    #[test]
    fn test_csv_finish_at_record_boundary() {
        let mut parser = parser();
        // The complete record stays buffered (no full batch); the feed
        // emits nothing and `finish` surfaces it.
        assert!(
            parser
                .feed(b"host1,1.0,2023-11-14 22:13:20\n")
                .unwrap()
                .is_empty()
        );
        let rows = parser.finish().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(string_at(&rows[0], 0), Some("host1".into()));
    }

    /// Regression test for chunk-boundary corruption: CopyData chunking is
    /// arbitrary (records may split mid-field), and an earlier feed loop
    /// flushed partial batches at record-boundary chunk ends, which
    /// arrow-csv does not permit mid-stream — fields then parse
    /// ("00001h,ost-001u,..."). Feed the same payload at many chunk sizes
    /// and verify EVERY cell.
    #[test]
    fn test_csv_chunked_feeds_are_alignment_safe() {
        // (int32 id, timestamp) rows of near-equal length.
        let sweep_columns = vec![
            ColumnSchema::new("i", ConcreteDataType::int32_datatype(), true),
            ColumnSchema::new(
                "ts",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            ),
        ];
        // A payload wider than one decoder batch so batch-full flushes
        // interleave with chunk boundaries too.
        let total = 2 * COPY_IN_FLUSH_ROWS + 137;
        let payload: Vec<u8> = (0..total)
            .map(|i| {
                format!(
                    "{:06},2024-01-01 00:00:{:02}.{:03}\n",
                    i,
                    (i / 1000) % 60,
                    i % 1000
                )
            })
            .collect::<String>()
            .into_bytes();

        let check = |chunk: usize| {
            let mut parser = build_csv_codec(CopyOptionSet::default())
                .unwrap()
                .create_parser(&sweep_columns, "UTC")
                .unwrap();
            let mut ids: Vec<i32> = Vec::new();
            for c in payload.chunks(chunk) {
                for row in parser.feed(c).unwrap() {
                    ids.push(match row.values[0].value_data.as_ref().unwrap() {
                        api::v1::value::ValueData::I32Value(v) => *v,
                        other => panic!("expected i32, got {other:?}"),
                    });
                }
            }
            for row in parser.finish().unwrap() {
                ids.push(match row.values[0].value_data.as_ref().unwrap() {
                    api::v1::value::ValueData::I32Value(v) => *v,
                    other => panic!("expected i32, got {other:?}"),
                });
            }
            assert_eq!(ids.len(), total, "chunk={chunk}");
            for (i, v) in ids.iter().enumerate() {
                assert_eq!(*v, i as i32, "chunk={chunk} row {i} corrupted");
            }
        };

        // Small chunk sizes hit every possible boundary alignment,
        // including multiple decode calls per record.
        for chunk in [1usize, 2, 3, 7, 8, 13, 31, 64, 97, 128, 1024] {
            check(chunk);
        }
        // Client-natural sizes with a shifted start to vary where
        // boundaries land relative to records.
        for chunk in [4096usize, 8192, 65531] {
            check(chunk);
            let mut shifted = payload.clone();
            shifted.drain(..37);
            let mut parser = build_csv_codec(CopyOptionSet::default())
                .unwrap()
                .create_parser(&sweep_columns, "UTC")
                .unwrap();
            let mut count = 0usize;
            for c in shifted.chunks(chunk) {
                count += parser.feed(c).unwrap().len();
            }
            count += parser.finish().unwrap().len();
            assert_eq!(count, total - 1, "chunk={chunk} shifted");
        }
    }

    #[test]
    fn test_csv_boolean_spellings() {
        // PostgreSQL COPY OUT emits `t`/`f`; COPY IN accepts the full
        // boolean input grammar.
        let columns = vec![
            ColumnSchema::new("flag", ConcreteDataType::boolean_datatype(), true),
            ColumnSchema::new(
                "ts",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            ),
        ];
        let mut parser = build_csv_codec(CopyOptionSet::default())
            .unwrap()
            .create_parser(&columns, "UTC")
            .unwrap();
        let rows = feed_all(
            &mut parser,
            &[b"t,2024-01-01 00:00:00\n\
                 f,2024-01-01 00:00:01\n\
                 TRUE,2024-01-01 00:00:02\n\
                 no,2024-01-01 00:00:03\n\
                 on,2024-01-01 00:00:04\n\
                 0,2024-01-01 00:00:05\n\
                 ,2024-01-01 00:00:06\n"],
        );
        let expected = [
            Some(true),
            Some(false),
            Some(true),
            Some(false),
            Some(true),
            Some(false),
            None,
        ];
        for (row, want) in rows.iter().zip(expected) {
            assert_eq!(
                row.values[0].value_data,
                want.map(api::v1::value::ValueData::BoolValue),
                "{:?}",
                row.values[0].value_data
            );
        }
    }

    #[test]
    fn test_csv_boolean_rejects_garbage() {
        let columns = vec![
            ColumnSchema::new("flag", ConcreteDataType::boolean_datatype(), true),
            ColumnSchema::new(
                "ts",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            ),
        ];
        let mut parser = build_csv_codec(CopyOptionSet::default())
            .unwrap()
            .create_parser(&columns, "UTC")
            .unwrap();
        parser.feed(b"maybe,2024-01-01 00:00:00\n").unwrap();
        let err = match parser.finish() {
            Ok(_) => panic!("expected parse error"),
            Err(e) => e,
        };
        assert!(
            err.to_string()
                .contains("invalid input syntax for type boolean"),
            "{err}"
        );
    }

    #[test]
    fn test_csv_timestamp_session_timezone() {
        // Offset-free timestamp strings must parse in the session
        // timezone (parity with SQL INSERT); explicit offsets win.
        let columns = vec![
            ColumnSchema::new("host", ConcreteDataType::string_datatype(), true),
            ColumnSchema::new(
                "ts",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            ),
        ];
        let mut parser = build_csv_codec(CopyOptionSet::default())
            .unwrap()
            .create_parser(&columns, "Asia/Shanghai")
            .unwrap();
        let rows = feed_all(
            &mut parser,
            &[b"host1,2024-01-01 00:00:00\n\
                host2,2024-01-01T00:00:00+08:00\n\
                host3,2024-01-01T00:00:00Z\n"],
        );
        let millis = |row: &ParsedRow| match row.values[1].value_data.as_ref().unwrap() {
            api::v1::value::ValueData::TimestampMillisecondValue(v) => *v,
            other => panic!("expected ts, got {other:?}"),
        };
        // 2024-01-01T00:00:00+08:00 == 2023-12-31T16:00:00Z
        assert_eq!(millis(&rows[0]), 1704038400000);
        // Explicit +08:00 offset: same instant as the naive parse.
        assert_eq!(millis(&rows[1]), 1704038400000);
        // Explicit Z: parsed as UTC regardless of the session zone.
        assert_eq!(millis(&rows[2]), 1704067200000);
    }

    #[test]
    fn test_csv_blank_lines_keep_masks_aligned() {
        // csv-core skips blank lines; the scanner must not emit a mask
        // for them (before, between or after records — and before a
        // header), or every later row consumes the wrong mask.
        let columns = vec![
            ColumnSchema::new("note", ConcreteDataType::string_datatype(), true),
            ColumnSchema::new(
                "ts",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            ),
        ];
        let payload: Vec<u8> = b"\n\"\",2024-01-01 00:00:00\n\n,2024-01-01 00:00:01\n".to_vec();
        for chunk in [1usize, 2, 3, 5, 7, 11, 64] {
            let mut parser = build_csv_codec(CopyOptionSet::default())
                .unwrap()
                .create_parser(&columns, "UTC")
                .unwrap();
            let mut rows = Vec::new();
            for c in payload.chunks(chunk) {
                rows.extend(parser.feed(c).unwrap());
            }
            rows.extend(parser.finish().unwrap());
            assert_eq!(rows.len(), 2, "chunk={chunk}");
            assert!(
                matches!(
                    rows[0].values[0].value_data,
                    Some(api::v1::value::ValueData::StringValue(ref s)) if s.is_empty()
                ),
                "chunk={chunk}: row 0 quoted empty must be an empty string, got {:?}",
                rows[0].values[0].value_data
            );
            assert!(
                rows[1].values[0].value_data.is_none(),
                "chunk={chunk}: row 1 unquoted empty must be NULL, got {:?}",
                rows[1].values[0].value_data
            );
        }

        // A blank line before the header must not shift the header's
        // position either.
        let mut set = CopyOptionSet::default();
        set.insert("header".into(), CopyOptionValue::Flag).unwrap();
        let mut parser = build_csv_codec(set)
            .unwrap()
            .create_parser(&columns, "UTC")
            .unwrap();
        let rows = feed_all(&mut parser, &[b"\nnote,ts\n\"\",2024-01-01 00:00:00\n"]);
        assert_eq!(rows.len(), 1);
        assert!(matches!(
            rows[0].values[0].value_data,
            Some(api::v1::value::ValueData::StringValue(ref s)) if s.is_empty()
        ));

        // NOT NULL behavior with a leading blank line: the quoted empty
        // still restores to an empty string instead of tripping the
        // constraint through the blank line's (wrong) mask.
        let not_null_columns = vec![
            ColumnSchema::new("note", ConcreteDataType::string_datatype(), false),
            ColumnSchema::new(
                "ts",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            ),
        ];
        let mut parser = build_csv_codec(CopyOptionSet::default())
            .unwrap()
            .create_parser(&not_null_columns, "UTC")
            .unwrap();
        let rows = feed_all(&mut parser, &[b"\n\"\",2024-01-01 00:00:00\n"]);
        assert_eq!(rows.len(), 1);
        assert!(matches!(
            rows[0].values[0].value_data,
            Some(api::v1::value::ValueData::StringValue(ref s)) if s.is_empty()
        ));
    }

    #[test]
    fn test_csv_cr_only_record_separators() {
        // The decoder accepts CR alone as a record terminator, just like
        // LF and CRLF; the scanner must too, or it emits fewer masks
        // than the decoder emits rows.
        let columns = vec![
            ColumnSchema::new("note", ConcreteDataType::string_datatype(), true),
            ColumnSchema::new(
                "ts",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            ),
        ];
        // CR-only, then CRLF, then LF — mixed terminators in one stream.
        let payload: Vec<u8> =
            b"\"\",2024-01-01 00:00:00\r\"\",2024-01-01 00:00:01\r\n\"\",2024-01-01 00:00:02\n"
                .to_vec();
        for chunk in [1usize, 2, 3, 5, 7, 11, 64] {
            let mut parser = build_csv_codec(CopyOptionSet::default())
                .unwrap()
                .create_parser(&columns, "UTC")
                .unwrap();
            let mut rows = Vec::new();
            for c in payload.chunks(chunk) {
                rows.extend(parser.feed(c).unwrap());
            }
            rows.extend(parser.finish().unwrap());
            assert_eq!(rows.len(), 3, "chunk={chunk}");
            for (i, row) in rows.iter().enumerate() {
                assert!(
                    matches!(
                        row.values[0].value_data,
                        Some(api::v1::value::ValueData::StringValue(ref s)) if s.is_empty()
                    ),
                    "chunk={chunk}: row {i} quoted empty must be an empty string, got {:?}",
                    row.values[0].value_data
                );
            }
        }

        // A CR-only terminator at the very end of the stream (no LF
        // following) must also complete the trailing record.
        let mut parser = build_csv_codec(CopyOptionSet::default())
            .unwrap()
            .create_parser(&columns, "UTC")
            .unwrap();
        let rows = feed_all(&mut parser, &[b"x,2024-01-01 00:00:00\r"]);
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn test_csv_doubled_quotes_keep_masks_aligned() {
        // A doubled quote inside a quoted field must consume only the
        // escaped quote; the field after it (here a quoted empty string)
        // must still get its own mask regardless of chunk boundaries.
        let columns = vec![
            ColumnSchema::new("note", ConcreteDataType::string_datatype(), true),
            ColumnSchema::new("other", ConcreteDataType::string_datatype(), true),
            ColumnSchema::new(
                "ts",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            ),
        ];
        let payload: Vec<u8> = b"\"a\"\"b\",\"\",2024-01-01 00:00:00\n".to_vec();
        for chunk in [1usize, 2, 3, 5, 7, 11, 64] {
            let mut parser = build_csv_codec(CopyOptionSet::default())
                .unwrap()
                .create_parser(&columns, "UTC")
                .unwrap();
            let mut rows = Vec::new();
            for c in payload.chunks(chunk) {
                rows.extend(parser.feed(c).unwrap());
            }
            rows.extend(parser.finish().unwrap());
            assert_eq!(rows.len(), 1, "chunk={chunk}");
            let string_at = |col: usize| {
                rows[0].values[col].value_data.as_ref().map(|v| match v {
                    api::v1::value::ValueData::StringValue(s) => s.clone(),
                    other => panic!("expected string, got {other:?}"),
                })
            };
            assert_eq!(string_at(0), Some("a\"b".to_string()), "chunk={chunk}");
            assert_eq!(string_at(1), Some("".to_string()), "chunk={chunk}");
        }
    }

    #[test]
    fn test_csv_quoted_empty_in_not_null_column() {
        // A quoted empty string is a valid empty STRING for a NOT NULL
        // column; only an unquoted empty field (NULL) must be rejected.
        let columns = vec![
            ColumnSchema::new("note", ConcreteDataType::string_datatype(), false),
            ColumnSchema::new(
                "ts",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            ),
        ];

        let mut parser = build_csv_codec(CopyOptionSet::default())
            .unwrap()
            .create_parser(&columns, "UTC")
            .unwrap();
        let rows = feed_all(&mut parser, &[b"\"\",2024-01-01 00:00:00\n"]);
        assert_eq!(rows.len(), 1);
        assert!(matches!(
            rows[0].values[0].value_data,
            Some(api::v1::value::ValueData::StringValue(ref s)) if s.is_empty()
        ));

        let mut parser = build_csv_codec(CopyOptionSet::default())
            .unwrap()
            .create_parser(&columns, "UTC")
            .unwrap();
        // With the strict decoder protocol the NULL surfaces when the
        // EOF signal flushes the trailing batch.
        parser.feed(b",2024-01-01 00:00:00\n").unwrap();
        let error = parser.finish().unwrap_err();
        assert!(
            error.to_string().contains("violates not-null constraint"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn test_csv_quoted_empty_string_vs_null() {
        // PostgreSQL: an unquoted empty field is NULL, a quoted empty
        // string is an empty string. The decoder nulls both; the shadow
        // quote-mask scan restores the distinction for string columns.
        let columns = vec![
            ColumnSchema::new("note", ConcreteDataType::string_datatype(), true),
            ColumnSchema::new(
                "ts",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            ),
        ];
        let mut parser = build_csv_codec(CopyOptionSet::default())
            .unwrap()
            .create_parser(&columns, "UTC")
            .unwrap();
        let rows = feed_all(
            &mut parser,
            &[b"plain,2024-01-01 00:00:00\n\
                \"\",2024-01-01 00:00:01\n\
                ,2024-01-01 00:00:02\n\
                \" \",2024-01-01 00:00:03\n"],
        );
        let note = |row: &ParsedRow| {
            row.values[0].value_data.as_ref().map(|v| match v {
                api::v1::value::ValueData::StringValue(s) => s.clone(),
                other => panic!("expected string, got {other:?}"),
            })
        };
        assert_eq!(rows.len(), 4);
        assert_eq!(note(&rows[0]), Some("plain".to_string()));
        // Quoted empty string.
        assert_eq!(note(&rows[1]), Some("".to_string()));
        // Unquoted empty field: NULL.
        assert_eq!(note(&rows[2]), None);
        // Quoted single space is a non-empty string.
        assert_eq!(note(&rows[3]), Some(" ".to_string()));
    }

    #[test]
    fn test_csv_quoted_empty_across_chunk_boundaries() {
        // The mask scanner must track quoting across arbitrary chunk
        // splits, including splits inside a quoted empty field and a
        // CRLF record end.
        let columns = vec![
            ColumnSchema::new("note", ConcreteDataType::string_datatype(), true),
            ColumnSchema::new(
                "ts",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            ),
        ];
        let payload: Vec<u8> =
            b"\"\",2024-01-01 00:00:00\r\n,2024-01-01 00:00:01\n\"\",2024-01-01 00:00:02\n"
                .to_vec();
        for chunk in [1usize, 2, 3, 5, 7, 11, 64] {
            let mut parser = build_csv_codec(CopyOptionSet::default())
                .unwrap()
                .create_parser(&columns, "UTC")
                .unwrap();
            let mut rows = Vec::new();
            for c in payload.chunks(chunk) {
                rows.extend(parser.feed(c).unwrap());
            }
            rows.extend(parser.finish().unwrap());
            // Payload rows: quoted empty, unquoted empty (NULL), quoted
            // empty — the middle NULL must survive and the quoted ones
            // must decode to empty strings.
            assert_eq!(rows.len(), 3, "chunk={chunk}");
            assert!(
                matches!(
                    rows[0].values[0].value_data,
                    Some(api::v1::value::ValueData::StringValue(ref s)) if s.is_empty()
                ),
                "chunk={chunk}: row 0 quoted empty must be an empty string, got {:?}",
                rows[0].values[0].value_data
            );
            assert!(
                rows[1].values[0].value_data.is_none(),
                "chunk={chunk}: row 1 unquoted empty must be NULL, got {:?}",
                rows[1].values[0].value_data
            );
            assert!(
                matches!(
                    rows[2].values[0].value_data,
                    Some(api::v1::value::ValueData::StringValue(ref s)) if s.is_empty()
                ),
                "chunk={chunk}: row 2 quoted empty must be an empty string, got {:?}",
                rows[2].values[0].value_data
            );
        }
    }

    #[test]
    fn test_csv_errors() {
        let mut p = parser();
        let err = match p.feed(b"host1,1.0\n") {
            Ok(_) => panic!("expected parse error"),
            Err(e) => e,
        };
        assert!(
            err.to_string().contains("incorrect number of fields"),
            "{err}"
        );

        // Value conversion runs at flush time, so the bad double
        // surfaces at the EOF signal, not at feed time.
        let mut p = parser();
        p.feed(b"host1,not_a_double,2023-11-14 22:13:20\n").unwrap();
        let err = match p.finish() {
            Ok(_) => panic!("expected parse error"),
            Err(e) => e,
        };
        assert!(err.to_string().contains("failed to parse"), "{err}");
    }
}
