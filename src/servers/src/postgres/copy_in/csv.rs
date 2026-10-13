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
        while !buf.is_empty() {
            let consumed = self.decoder.decode(buf).map_err(decode_error)?;
            if consumed == 0 {
                while let Some(batch) = self.decoder.flush().map_err(decode_error)? {
                    rows.extend(flush_batch(&batch)?);
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
    /// arrow-csv does not permit mid-stream — fields then mis-parse
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
                .create_parser(&sweep_columns)
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
                .create_parser(&sweep_columns)
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
