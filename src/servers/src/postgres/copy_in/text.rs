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

//! The default `text` format: one record per line, fields separated by an
//! unescaped delimiter (tab by default), special characters escaped with
//! backslashes and `\N` marking NULL. This is PostgreSQL's plain text
//! COPY format.

use std::sync::Arc;

use pgwire::error::PgWireResult;

use super::codec::{CopyInCodec, Field, Record, RecordParser};
use super::options::CopyOptionSet;
use super::{bad_copy_data, unsupported};

/// The validated `text` format configuration.
#[derive(Debug)]
pub(super) struct TextCodec {
    delimiter: u8,
    null: Vec<u8>,
}

/// Builds the text codec from the parsed options.
pub(super) fn build_text_codec(options: CopyOptionSet) -> PgWireResult<Arc<dyn CopyInCodec>> {
    if options.quote.is_some() || options.escape.is_some() {
        return Err(unsupported("QUOTE/ESCAPE are only allowed in CSV format"));
    }
    Ok(Arc::new(TextCodec {
        delimiter: options.delimiter.unwrap_or(b'\t'),
        null: options.null.unwrap_or_else(|| b"\\N".to_vec()),
    }))
}

impl CopyInCodec for TextCodec {
    fn name(&self) -> &'static str {
        "text"
    }

    fn create_parser(&self) -> Box<dyn RecordParser + Send> {
        Box::new(TextRecordParser {
            delimiter: self.delimiter,
            null: self.null.clone(),
            pending: Vec::new(),
        })
    }
}

/// Parser for the text format.
struct TextRecordParser {
    delimiter: u8,
    null: Vec<u8>,
    pending: Vec<u8>,
}

impl RecordParser for TextRecordParser {
    fn feed(&mut self, data: &[u8]) -> PgWireResult<Vec<Record>> {
        self.pending.extend_from_slice(data);
        let mut records = Vec::new();
        while let Some(pos) = self.pending.iter().position(|&b| b == b'\n') {
            let line: Vec<u8> = self.pending.drain(..=pos).collect();
            // The drained line always ends with the `\n` separator.
            let line = &line[..line.len() - 1];
            records.push(parse_text_line(line, self.delimiter, &self.null)?);
        }
        Ok(records)
    }

    fn finish(&mut self) -> PgWireResult<Vec<Record>> {
        if self.pending.is_empty() {
            return Ok(Vec::new());
        }
        let line = std::mem::take(&mut self.pending);
        Ok(vec![parse_text_line(&line, self.delimiter, &self.null)?])
    }
}

fn parse_text_line(line: &[u8], delimiter: u8, null: &[u8]) -> PgWireResult<Record> {
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
    use super::*;

    fn default_options() -> CopyOptionSet {
        CopyOptionSet::default()
    }

    #[test]
    fn test_build_defaults() {
        let codec = build_text_codec(default_options()).unwrap();
        assert_eq!(codec.name(), "text");
        assert!(format!("{codec:?}").contains("TextCodec"));
    }

    #[test]
    fn test_build_custom_options() {
        let options = CopyOptionSet {
            null: Some(b"nil".to_vec()),
            delimiter: Some(b'|'),
            ..Default::default()
        };
        let codec = build_text_codec(options).unwrap();
        let mut parser = codec.create_parser();
        // Custom options flow into the parser: `nil` is NULL and `|` the
        // delimiter.
        let records = parser.feed(b"a|nil\n").unwrap();
        assert!(matches!(&records[0][0], Field::Value(v) if v == b"a"));
        assert!(matches!(&records[0][1], Field::Null));
    }

    #[test]
    fn test_build_rejects_csv_only_options() {
        let options = CopyOptionSet {
            quote: Some(b'"'),
            ..Default::default()
        };
        let err = build_text_codec(options).unwrap_err();
        assert!(err.to_string().contains("only allowed in CSV"));
    }

    fn text_parser() -> TextRecordParser {
        TextRecordParser {
            delimiter: b'\t',
            null: b"\\N".to_vec(),
            pending: Vec::new(),
        }
    }

    #[test]
    fn test_text_parse_records() {
        let mut parser = text_parser();
        let records = parser.feed(b"1\thello\t\\N\n2\two\\trld\t3\n").unwrap();
        assert_eq!(records.len(), 2);
        assert!(matches!(&records[0][0], Field::Value(v) if v == b"1"));
        assert!(matches!(&records[0][2], Field::Null));
        assert!(matches!(&records[1][1], Field::Value(v) if v == b"wo\trld"));
    }

    #[test]
    fn test_text_parse_chunk_boundaries() {
        let mut parser = text_parser();
        assert!(parser.feed(b"ab").unwrap().is_empty());
        assert!(parser.feed(b"c\td").unwrap().is_empty());
        let records = parser.feed(b"e\n").unwrap();
        assert_eq!(records.len(), 1);
        assert!(matches!(&records[0][0], Field::Value(v) if v == b"abc"));
        assert!(matches!(&records[0][1], Field::Value(v) if v == b"de"));
    }

    #[test]
    fn test_text_trailing_record_without_newline() {
        let mut parser = text_parser();
        parser.feed(b"x\ty").unwrap();
        let records = parser.finish().unwrap();
        assert_eq!(records.len(), 1);
        assert!(matches!(&records[0][0], Field::Value(v) if v == b"x"));
        assert!(matches!(&records[0][1], Field::Value(v) if v == b"y"));
    }

    #[test]
    fn test_text_octal_and_errors() {
        let mut parser = text_parser();
        let records = parser.feed(b"\\101\\102\n").unwrap();
        assert!(matches!(&records[0][0], Field::Value(v) if v == b"AB"));

        let mut parser = text_parser();
        let err = match parser.feed(b"a\\z\n") {
            Ok(_) => panic!("expected parse error"),
            Err(e) => e,
        };
        assert!(err.to_string().contains("invalid escape sequence"));

        let mut parser = text_parser();
        parser.feed(b"trailing\\").unwrap();
        let err = match parser.finish() {
            Ok(_) => panic!("expected parse error"),
            Err(e) => e,
        };
        assert!(err.to_string().contains("end-of-line backslash"));
    }
}
