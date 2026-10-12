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
//! rejected, timestamps must be date/time strings, and an empty field —
//! quoted or not — is NULL.

use std::sync::Arc;

use api::helper::vectors_to_rows;
use arrow::csv::ReaderBuilder;
use arrow::csv::reader::Decoder as CsvDecoder;
use arrow_schema::ArrowError;
use datatypes::data_type::DataType;
use datatypes::prelude::ConcreteDataType;
use datatypes::schema::{ColumnSchema, Schema as GreptimeSchema};
use datatypes::vectors::Helper;
use pgwire::error::{PgWireError, PgWireResult};

use super::codec::{CopyInCodec, ParsedRow, RecordParser};
use super::options::CopyOptionSet;
use super::{COPY_IN_FLUSH_ROWS, bad_copy_data, invalid_text_repr, unsupported};

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
    ) -> PgWireResult<Box<dyn RecordParser + Send>> {
        // Reuse the greptime schema's arrow projection for decoding.
        let arrow_schema = GreptimeSchema::try_new(columns.to_vec())
            .map_err(|e| unsupported(format!("invalid columns for format csv: {e}")))?
            .arrow_schema()
            .clone();
        let mut builder = ReaderBuilder::new(arrow_schema)
            .with_header(self.header)
            .with_delimiter(self.delimiter)
            .with_quote(self.quote)
            .with_batch_size(COPY_IN_FLUSH_ROWS);
        // An escape character equal to the quote is the doubled-quote
        // default; csv-core handles that through quoting alone.
        if self.escape != self.quote {
            builder = builder.with_escape(self.escape);
        }
        Ok(Box::new(CsvRecordParser {
            decoder: builder.build_decoder(),
        }))
    }
}

/// Incremental parser backed by arrow-csv's decoder.
struct CsvRecordParser {
    decoder: CsvDecoder,
}

impl RecordParser for CsvRecordParser {
    fn feed(&mut self, data: &[u8]) -> PgWireResult<Vec<ParsedRow>> {
        let mut buf = data;
        let mut rows = Vec::new();
        loop {
            // Decode until the input is consumed or a full batch is
            // buffered. A record may end mid-buffer; its bytes stay in the
            // decoder until more input (or the EOF signal) completes it.
            while !buf.is_empty() {
                let consumed = self.decoder.decode(buf).map_err(decode_error)?;
                if consumed == 0 {
                    break;
                }
                buf = &buf[consumed..];
            }
            // Flush complete records. `flush` fails without side effects
            // when a record is still incomplete (the input ended
            // mid-record); its bytes are kept for the next chunk or the
            // EOF signal in `finish`.
            match self.decoder.flush() {
                Ok(Some(batch)) => rows.extend(flush_batch(&batch)?),
                Ok(None) => return Ok(rows),
                Err(error) if is_mid_record(&error) => return Ok(rows),
                Err(error) => return Err(decode_error(error)),
            }
            if buf.is_empty() {
                return Ok(rows);
            }
            // decode() returned 0 with input remaining: a full batch was
            // flushed, continue with the rest of the input.
        }
    }

    fn finish(&mut self) -> PgWireResult<Vec<ParsedRow>> {
        // An empty decode signals EOF, finalizing a trailing record that
        // lacks its record separator.
        self.decoder.decode(&[]).map_err(decode_error)?;
        let mut rows = Vec::new();
        while let Some(batch) = self.decoder.flush().map_err(decode_error)? {
            rows.extend(flush_batch(&batch)?);
        }
        Ok(rows)
    }
}

/// Converts one decoded arrow batch into insertable rows with the shared
/// [`vectors_to_rows`] helper.
fn flush_batch(batch: &arrow::record_batch::RecordBatch) -> PgWireResult<Vec<ParsedRow>> {
    let vectors = Helper::try_into_vectors(batch.columns())
        .map_err(|e| bad_copy_data(format!("failed to convert decoded CSV batch: {e}")))?;
    Ok(vectors_to_rows(vectors.iter(), batch.num_rows()))
}

/// Whether the error is arrow-csv reporting a record still in progress,
/// which `flush` rejects (without side effects) until more input or the
/// EOF signal completes the record.
fn is_mid_record(error: &ArrowError) -> bool {
    matches!(error, ArrowError::CsvError(m) if m.contains("part way through record"))
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
            .create_parser(&test_columns())
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
            .create_parser(&test_columns())
            .unwrap()
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
        let rows = parser
            .feed(
                b"host1,1.5,2023-11-14 22:13:20\n\
                  ,2.5,2023-11-14 22:13:21\n\
                  \"quoted, comma\",3.5,2023-11-14 22:13:22\n",
            )
            .unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(string_at(&rows[0], 0), Some("host1".into()));
        // An empty field is NULL.
        assert_eq!(string_at(&rows[1], 0), None);
        assert_eq!(string_at(&rows[2], 0), Some("quoted, comma".into()));
    }

    #[test]
    fn test_csv_escaped_quotes() {
        let mut parser = parser();
        let rows = parser
            .feed(b"\"say \"\"hi\"\"\",1.0,2023-11-14 22:13:20\n")
            .unwrap();
        assert_eq!(string_at(&rows[0], 0), Some("say \"hi\"".into()));
    }

    #[test]
    fn test_csv_custom_escape() {
        let mut set = CopyOptionSet::default();
        set.insert("escape".into(), CopyOptionValue::String("\\".into()))
            .unwrap();
        let mut parser = rows_with(set);
        let rows = parser
            .feed(b"\"a\\\"b\",1.0,2023-11-14 22:13:20\n")
            .unwrap();
        assert_eq!(string_at(&rows[0], 0), Some("a\"b".into()));
    }

    #[test]
    fn test_csv_quoted_newline_and_chunk_boundaries() {
        let mut parser = parser();
        // Split inside a quoted field containing a newline, and a CRLF
        // record separator split across chunks.
        assert!(parser.feed(b"\"multi").unwrap().is_empty());
        assert!(
            parser
                .feed(b"line\nfield\",1.0,2023-11-14 22:13:20")
                .unwrap()
                .is_empty()
        );
        let rows = parser.feed(b"\r\nhost2,2.0,2023-11-14 22:13:21\n").unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(string_at(&rows[0], 0), Some("multiline\nfield".into()));
        assert_eq!(string_at(&rows[1], 0), Some("host2".into()));
    }

    #[test]
    fn test_csv_header() {
        let mut set = CopyOptionSet::default();
        set.insert("header".into(), CopyOptionValue::Flag).unwrap();
        let mut parser = rows_with(set);
        let rows = parser
            .feed(b"host,val,ts\nhost1,1.0,2023-11-14 22:13:20\n")
            .unwrap();
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
        parser.feed(b"host1,1.0,2023-11-14 22:13:20\n").unwrap();
        assert!(parser.finish().unwrap().is_empty());
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

        let mut p = parser();
        let err = match p.feed(b"host1,not_a_double,2023-11-14 22:13:20\n") {
            Ok(_) => panic!("expected parse error"),
            Err(e) => e,
        };
        assert!(err.to_string().contains("failed to parse"), "{err}");
    }
}
