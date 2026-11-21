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
//! [`Decoder`](arrow::json::reader::Decoder) — the same engine backing
//! GreptimeDB's file-based `COPY ... FROM 'file.json'` (see
//! `common-datasource`'s `json` file format) — so both COPY routes
//! accept identical data. The decoder splits records (objects may span
//! chunk boundaries) and converts fields against the targeted columns'
//! schema in one pass; a record is one JSON object whose keys address
//! the columns **by name** (the COPY column list fixes the target
//! order, as with csv).
//!
//! Consequences of sharing the engine: JSON native types (numbers,
//! booleans, strings) must match the column types; absent keys become
//! NULL; timestamps are RFC 3339 / date-time strings (offset-free ones
//! parse in the session timezone, like SQL INSERT); and columns whose
//! type arrow-json cannot decode (JSON, Binary) are rejected.
//!
//! The format is table-bound like `csv` and takes no options; the
//! file-based json format's layout options are writer-side only, and
//! arrow-json's reader parses date-time strings natively (RFC 3339,
//! with the schema's timezone for offset-free strings).

use std::sync::Arc;

use api::helper::vectors_to_rows;
use arrow::json::reader::{Decoder as JsonDecoder, ReaderBuilder};
use arrow_schema::{ArrowError, DataType as ArrowDataType, Field, Schema as ArrowSchema};
use datatypes::data_type::DataType;
use datatypes::prelude::ConcreteDataType;
use datatypes::schema::{ColumnSchema, Schema as GreptimeSchema};
use datatypes::vectors::Helper;
use pgwire::error::{PgWireError, PgWireResult};

use crate::postgres::copy_in::codec::{CopyFormat, ParsedRow, RecordParser};
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

impl JsonlCodec {
    /// The column types arrow-json can decode against.
    pub(crate) fn check_column_type(&self, data_type: &ConcreteDataType) -> PgWireResult<()> {
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
                "column type \"{}\" is not supported by format jsonl",
                data_type.name()
            )));
        }
        Ok(())
    }

    pub(crate) fn create_parser(
        &self,
        columns: &[ColumnSchema],
        timezone: &str,
    ) -> PgWireResult<Box<dyn RecordParser + Send>> {
        // Decode schema: the greptime schema's arrow projection with
        // Timestamp fields carrying the SESSION timezone so
        // offset-free strings parse like SQL INSERT does instead of as
        // UTC (the same substitution the csv codec makes); explicit
        // offsets in the input always win.
        let arrow_schema = GreptimeSchema::try_new(columns.to_vec())
            .map_err(|e| unsupported(format!("invalid columns for format jsonl: {e}")))?
            .arrow_schema()
            .clone();
        let timezone = timezone.to_string();
        let fields = arrow_schema
            .fields()
            .iter()
            .zip(columns)
            .map(|(field, column)| match &column.data_type {
                ConcreteDataType::Timestamp(_) => match field.data_type() {
                    ArrowDataType::Timestamp(unit, None) => Field::new(
                        field.name(),
                        ArrowDataType::Timestamp(*unit, Some(Arc::from(timezone.as_str()))),
                        field.is_nullable(),
                    ),
                    _ => field.as_ref().clone(),
                },
                _ => field.as_ref().clone(),
            })
            .collect::<Vec<_>>();
        let builder = ReaderBuilder::new(Arc::new(ArrowSchema::new(fields)))
            .with_batch_size(COPY_IN_FLUSH_ROWS);
        Ok(Box::new(JsonlRecordParser {
            decoder: builder.build_decoder().map_err(decode_error)?,
        }))
    }
}

/// Incremental parser backed by arrow-json's decoder, whose streaming
/// contract mirrors arrow-csv's (`decode` returns consumed bytes; a
/// short read means a full batch is ready).
struct JsonlRecordParser {
    decoder: JsonDecoder,
}

impl RecordParser for JsonlRecordParser {
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
        // An empty decode signals EOF, finalizing a trailing record
        // that lacks its newline.
        self.decoder.decode(&[]).map_err(decode_error)?;
        let mut rows = Vec::new();
        while let Some(batch) = self.decoder.flush().map_err(decode_error)? {
            rows.extend(flush_batch(&batch)?);
        }
        Ok(rows)
    }
}

/// Converts one decoded arrow batch into insertable rows with the
/// shared [`vectors_to_rows`] helper.
fn flush_batch(batch: &arrow::record_batch::RecordBatch) -> PgWireResult<Vec<ParsedRow>> {
    let vectors = Helper::try_into_vectors(batch.columns())
        .map_err(|e| bad_copy_data(format!("failed to convert decoded JSON batch: {e}")))?;
    Ok(vectors_to_rows(vectors.iter(), batch.num_rows()))
}

fn decode_error(error: ArrowError) -> PgWireError {
    match &error {
        ArrowError::ParseError(reason) => invalid_text_repr(reason.clone()),
        ArrowError::JsonError(reason) => invalid_text_repr(reason.clone()),
        _ => PgWireError::ApiError(Box::new(error)),
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

    fn parser() -> Box<dyn RecordParser + Send> {
        match build_jsonl_codec(CopyOptionSet::default()).unwrap() {
            CopyFormat::Jsonl(codec) => codec.create_parser(&columns(), "UTC").unwrap(),
            other => panic!("unexpected format {other:?}"),
        }
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

    #[test]
    fn test_jsonl_records_across_chunk_boundaries() {
        let payload: Vec<u8> = br#"{"host":"a","val":1.5,"ts":"2024-01-01 00:00:00"}
{"host":"b","val":2.5,"ts":"2024-01-01 00:00:01"}"#
            .to_vec();
        for chunk in [1usize, 2, 3, 7, 64] {
            let mut parser = parser();
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
        let mut parser = parser();
        let rows = feed_all(
            &mut parser,
            &[b"{\"val\":3.5,\"ts\":\"2024-01-01 00:00:00\"}
"],
        );
        assert_eq!(rows.len(), 1);
        assert!(rows[0].values[0].value_data.is_none());
    }

    #[test]
    fn test_jsonl_type_mismatch_fails() {
        let mut parser = parser();
        // The mismatch surfaces when the record is decoded at flush.
        parser
            .feed(
                br#"{"host":"a","val":"not a number","ts":"2024-01-01 00:00:00"}
"#
                .as_ref(),
            )
            .unwrap();
        let error = parser.finish().unwrap_err();
        assert!(!error.to_string().is_empty());
    }

    #[test]
    fn test_jsonl_rejects_options_and_bad_columns() {
        let mut set = CopyOptionSet::default();
        set.insert("header".into(), CopyOptionValue::Flag).unwrap();
        assert!(build_jsonl_codec(set).is_err());

        let codec = JsonlCodec;
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
    }
}
