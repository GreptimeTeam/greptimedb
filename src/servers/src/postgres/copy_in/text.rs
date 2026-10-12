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

//! PostgreSQL's default `text` format: one record per line, fields
//! separated by an unescaped delimiter (tab by default), special
//! characters escaped with backslashes and `\N` marking NULL.
//!
//! This is the smallest example of the field-splitting building-block
//! style: it splits records into raw [`Field`]s and converts them with
//! the shared text-representation helpers — the same pieces are
//! available to any future format that carries values as text.

use std::sync::Arc;

use pgwire::error::PgWireResult;

use super::bad_copy_data;
use super::codec::{CopyInCodec, Field, ParsedRow, RecordParser, convert_text_field};
use super::options::CopyOptionSet;

/// The validated `text` format configuration.
#[derive(Debug)]
pub(super) struct TextCodec {
    delimiter: u8,
    null: Vec<u8>,
}

/// Builds the text codec from the parsed options.
pub(super) fn build_text_codec(mut options: CopyOptionSet) -> PgWireResult<Arc<dyn CopyInCodec>> {
    let delimiter = options.take_single_byte("DELIMITER")?.unwrap_or(b'\t');
    let null = options
        .take_string("NULL")?
        .unwrap_or_else(|| "\\N".to_string())
        .into_bytes();
    options.reject_leftovers("text")?;
    Ok(Arc::new(TextCodec { delimiter, null }))
}

impl CopyInCodec for TextCodec {
    fn name(&self) -> &'static str {
        "text"
    }

    fn create_parser(
        &self,
        columns: &[datatypes::schema::ColumnSchema],
    ) -> PgWireResult<Box<dyn RecordParser + Send>> {
        Ok(Box::new(TextRecordParser {
            delimiter: self.delimiter,
            null: self.null.clone(),
            columns: columns.to_vec(),
            pending: Vec::new(),
        }))
    }
}

/// Parser for the text format.
struct TextRecordParser {
    delimiter: u8,
    null: Vec<u8>,
    /// Targeted columns, in COPY field order.
    columns: Vec<datatypes::schema::ColumnSchema>,
    pending: Vec<u8>,
}

impl RecordParser for TextRecordParser {
    fn feed(&mut self, data: &[u8]) -> PgWireResult<Vec<ParsedRow>> {
        self.pending.extend_from_slice(data);
        let mut rows = Vec::new();
        while let Some(pos) = self.pending.iter().position(|&b| b == b'\n') {
            let line: Vec<u8> = self.pending.drain(..=pos).collect();
            // The drained line always ends with the `\n` separator.
            let line = &line[..line.len() - 1];
            rows.push(self.parse_text_line(line)?);
        }
        Ok(rows)
    }

    fn finish(&mut self) -> PgWireResult<Vec<ParsedRow>> {
        if self.pending.is_empty() {
            return Ok(Vec::new());
        }
        let line = std::mem::take(&mut self.pending);
        Ok(vec![self.parse_text_line(&line)?])
    }
}

impl TextRecordParser {
    fn parse_text_line(&self, line: &[u8]) -> PgWireResult<ParsedRow> {
        let fields = split_fields(line, self.delimiter, &self.null)?;
        if fields.len() < self.columns.len() {
            let missing = &self.columns[fields.len()];
            return Err(bad_copy_data(format!(
                "missing data for column \"{}\"",
                missing.name
            )));
        } else if fields.len() > self.columns.len() {
            return Err(bad_copy_data("extra data after last expected column"));
        }
        Ok(ParsedRow {
            values: fields
                .into_iter()
                .zip(&self.columns)
                .map(|(field, column)| convert_text_field(column, field))
                .collect::<PgWireResult<Vec<_>>>()?,
        })
    }
}

/// Splits a text-format line into null-checked fields.
fn split_fields(line: &[u8], delimiter: u8, null: &[u8]) -> PgWireResult<Vec<Field>> {
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

#[cfg(test)]
mod tests {
    use datatypes::prelude::ConcreteDataType;
    use datatypes::schema::ColumnSchema;

    use super::*;
    use crate::postgres::copy_in::options::CopyOptionValue;

    /// Columns: (int32, string, string).
    fn test_columns() -> Vec<ColumnSchema> {
        vec![
            ColumnSchema::new("a", ConcreteDataType::int32_datatype(), true),
            ColumnSchema::new("b", ConcreteDataType::string_datatype(), true),
            ColumnSchema::new("c", ConcreteDataType::string_datatype(), true),
        ]
    }

    fn parser(columns: &[ColumnSchema]) -> Box<dyn RecordParser + Send> {
        build_text_codec(CopyOptionSet::default())
            .unwrap()
            .create_parser(columns)
            .unwrap()
    }

    fn string_of(row: &ParsedRow, index: usize) -> Option<String> {
        row.values[index].value_data.as_ref().map(|v| match v {
            api::v1::value::ValueData::StringValue(s) => s.clone(),
            other => panic!("expected string, got {other:?}"),
        })
    }

    #[test]
    fn test_build_defaults() {
        let codec = build_text_codec(CopyOptionSet::default()).unwrap();
        assert_eq!(codec.name(), "text");
        assert!(format!("{codec:?}").contains("TextCodec"));
    }

    #[test]
    fn test_build_custom_options() {
        let mut set = CopyOptionSet::default();
        set.insert("null".into(), CopyOptionValue::String("nil".into()))
            .unwrap();
        set.insert("delimiter".into(), CopyOptionValue::String("|".into()))
            .unwrap();
        let columns = vec![
            ColumnSchema::new("a", ConcreteDataType::string_datatype(), true),
            ColumnSchema::new("b", ConcreteDataType::string_datatype(), true),
        ];
        let mut parser = build_text_codec(set)
            .unwrap()
            .create_parser(&columns)
            .unwrap();
        // Custom options flow into the parser: `nil` is NULL and `|` the
        // delimiter.
        let rows = parser.feed(b"a|nil\n").unwrap();
        assert_eq!(string_of(&rows[0], 0), Some("a".into()));
        assert_eq!(string_of(&rows[0], 1), None);
    }

    #[test]
    fn test_build_rejects_unsupported_options() {
        let mut set = CopyOptionSet::default();
        set.insert("quote".into(), CopyOptionValue::String("\"".into()))
            .unwrap();
        let err = build_text_codec(set).unwrap_err();
        assert!(err.to_string().contains("not supported by format text"));
    }

    #[test]
    fn test_text_parse_records() {
        let mut parser = parser(&test_columns());
        let rows = parser.feed(b"1\thello\t\\N\n2\two\\trld\t3\n").unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(
            rows[0].values[0].value_data,
            Some(api::v1::value::ValueData::I32Value(1))
        );
        assert_eq!(string_of(&rows[0], 1), Some("hello".into()));
        assert_eq!(string_of(&rows[1], 1), Some("wo\trld".into()));
    }

    #[test]
    fn test_text_null_marker() {
        let mut parser = parser(&test_columns());
        let rows = parser.feed(b"1\t\\N\t\\N\n").unwrap();
        assert_eq!(string_of(&rows[0], 1), None);
        assert_eq!(string_of(&rows[0], 2), None);
    }

    #[test]
    fn test_text_parse_chunk_boundaries() {
        let columns = vec![
            ColumnSchema::new("a", ConcreteDataType::string_datatype(), true),
            ColumnSchema::new("b", ConcreteDataType::string_datatype(), true),
        ];
        let mut parser = parser(&columns);
        assert!(parser.feed(b"ab").unwrap().is_empty());
        assert!(parser.feed(b"c\td").unwrap().is_empty());
        let rows = parser.feed(b"e\n").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(string_of(&rows[0], 0), Some("abc".into()));
        assert_eq!(string_of(&rows[0], 1), Some("de".into()));
    }

    #[test]
    fn test_text_trailing_record_without_newline() {
        let columns = vec![
            ColumnSchema::new("a", ConcreteDataType::string_datatype(), true),
            ColumnSchema::new("b", ConcreteDataType::string_datatype(), true),
        ];
        let mut parser = parser(&columns);
        parser.feed(b"x\ty").unwrap();
        let rows = parser.finish().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(string_of(&rows[0], 0), Some("x".into()));
        assert_eq!(string_of(&rows[0], 1), Some("y".into()));
    }

    #[test]
    fn test_text_octal_and_errors() {
        let columns = vec![ColumnSchema::new(
            "a",
            ConcreteDataType::string_datatype(),
            true,
        )];
        let mut p = parser(&columns);
        let rows = p.feed(b"\\101\\102\n").unwrap();
        assert_eq!(string_of(&rows[0], 0), Some("AB".into()));

        let mut p = parser(&columns);
        let err = match p.feed(b"a\\z\n") {
            Ok(_) => panic!("expected parse error"),
            Err(e) => e,
        };
        assert!(err.to_string().contains("invalid escape sequence"));

        let mut p = parser(&columns);
        p.feed(b"trailing\\").unwrap();
        let err = match p.finish() {
            Ok(_) => panic!("expected parse error"),
            Err(e) => e,
        };
        assert!(err.to_string().contains("end-of-line backslash"));
    }

    #[test]
    fn test_text_field_count_mismatch() {
        let mut p = parser(&test_columns());
        let err = match p.feed(b"1\thello\n") {
            Ok(_) => panic!("expected parse error"),
            Err(e) => e,
        };
        assert!(err.to_string().contains("missing data for column \"c\""));

        let mut p = parser(&test_columns());
        let err = match p.feed(b"1\thello\tx\textra\n") {
            Ok(_) => panic!("expected parse error"),
            Err(e) => e,
        };
        assert!(err.to_string().contains("extra data"));
    }
}
