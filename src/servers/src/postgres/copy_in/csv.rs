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

//! The `csv` format, per RFC 4180 with PostgreSQL's COPY extensions:
//! configurable delimiter, quote and escape characters, `\r\n` or `\n`
//! record separators, an optional header row, and NULL represented by an
//! unquoted field matching the `NULL` string (empty by default).

use std::sync::Arc;

use pgwire::error::PgWireResult;

use super::bad_copy_data;
use super::codec::{CopyInCodec, Field, Record, RecordParser};
use super::options::CopyOptionSet;

/// The validated `csv` format configuration.
#[derive(Debug)]
pub(super) struct CsvCodec {
    delimiter: u8,
    quote: u8,
    escape: u8,
    null: Vec<u8>,
    header: bool,
}

/// Builds the CSV codec from the parsed options.
pub(super) fn build_csv_codec(options: CopyOptionSet) -> PgWireResult<Arc<dyn CopyInCodec>> {
    let delimiter = options.delimiter.unwrap_or(b',');
    let quote = options.quote.unwrap_or(b'"');
    if delimiter == quote {
        return Err(bad_copy_data("DELIMITER must not be equal to QUOTE"));
    }
    Ok(Arc::new(CsvCodec {
        delimiter,
        quote,
        // In PostgreSQL the default escape character for CSV is the quote
        // character itself (doubled quotes inside quoted fields).
        escape: options.escape.unwrap_or(quote),
        null: options.null.unwrap_or_default(),
        header: options.header,
    }))
}

impl CopyInCodec for CsvCodec {
    fn name(&self) -> &'static str {
        "csv"
    }

    fn create_parser(&self) -> Box<dyn RecordParser + Send> {
        Box::new(CsvRecordParser::new(
            self.delimiter,
            self.quote,
            self.escape,
            &self.null,
            self.header,
        ))
    }
}

/// Incremental parser for the CSV format, keeping state across `CopyData`
/// chunk boundaries (quoted fields may contain newlines, and `\r\n` pairs
/// may split across chunks).
struct CsvRecordParser {
    delimiter: u8,
    quote: u8,
    escape: u8,
    null: Vec<u8>,
    state: CsvState,
    /// Completed fields of the current record.
    fields: Vec<Field>,
    /// Field currently being parsed.
    current: Vec<u8>,
    current_quoted: bool,
    /// A byte deferred until the next chunk because its meaning depends on
    /// its successor (`\r` of a possible `\r\n`, or an escape character).
    carry: Option<u8>,
    header: bool,
}

impl CsvRecordParser {
    fn new(delimiter: u8, quote: u8, escape: u8, null: &[u8], header: bool) -> Self {
        Self {
            delimiter,
            quote,
            escape,
            null: null.to_vec(),
            state: CsvState::FieldStart,
            fields: Vec::new(),
            current: Vec::new(),
            current_quoted: false,
            carry: None,
            header,
        }
    }

    /// Treats `b` as a data byte in the current state; used for a carried
    /// `\r` at end-of-stream that turns out to be literal data.
    fn push_data(&mut self, b: u8) {
        if self.state == CsvState::FieldStart {
            self.state = CsvState::InUnquoted;
        }
        self.current.push(b);
    }
}

#[derive(PartialEq, Eq)]
enum CsvState {
    FieldStart,
    InUnquoted,
    InQuoted,
    QuoteInQuoted,
}

impl RecordParser for CsvRecordParser {
    fn skips_header(&self) -> bool {
        self.header
    }

    fn feed(&mut self, data: &[u8]) -> PgWireResult<Vec<Record>> {
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
                CsvState::FieldStart => {
                    if b == self.quote {
                        self.state = CsvState::InQuoted;
                        self.current_quoted = true;
                        idx += 1;
                    } else if b == self.delimiter {
                        self.end_field();
                        idx += 1;
                    } else if is_record_end(b, next) {
                        self.state = CsvState::FieldStart;
                        records.push(self.end_record());
                        idx += record_end_len(b);
                    } else if is_incomplete_record_end(b, next) {
                        self.carry = Some(b);
                        break;
                    } else {
                        self.current.push(b);
                        self.state = CsvState::InUnquoted;
                        idx += 1;
                    }
                }
                CsvState::InUnquoted => {
                    if b == self.delimiter {
                        self.end_field();
                        self.state = CsvState::FieldStart;
                        idx += 1;
                    } else if is_record_end(b, next) {
                        self.state = CsvState::FieldStart;
                        records.push(self.end_record());
                        idx += record_end_len(b);
                    } else if is_incomplete_record_end(b, next) {
                        self.carry = Some(b);
                        break;
                    } else {
                        self.current.push(b);
                        idx += 1;
                    }
                }
                CsvState::InQuoted => {
                    if self.escape != self.quote && b == self.escape {
                        // The escape character escapes its successor.
                        match next {
                            Some(n) => {
                                self.current.push(n);
                                idx += 2;
                            }
                            None => {
                                self.carry = Some(b);
                                break;
                            }
                        }
                    } else if b == self.quote {
                        self.state = CsvState::QuoteInQuoted;
                        idx += 1;
                    } else {
                        self.current.push(b);
                        idx += 1;
                    }
                }
                CsvState::QuoteInQuoted => {
                    if b == self.quote && self.escape == self.quote {
                        self.current.push(self.quote);
                        self.state = CsvState::InQuoted;
                        idx += 1;
                    } else if b == self.delimiter {
                        self.end_field();
                        self.state = CsvState::FieldStart;
                        idx += 1;
                    } else if is_record_end(b, next) {
                        self.state = CsvState::FieldStart;
                        records.push(self.end_record());
                        idx += record_end_len(b);
                    } else if is_incomplete_record_end(b, next) {
                        self.carry = Some(b);
                        break;
                    } else {
                        return Err(bad_copy_data(
                            "found unexpected data after a closing quote in CSV field",
                        ));
                    }
                }
            }
        }
        Ok(records)
    }

    fn finish(&mut self) -> PgWireResult<Vec<Record>> {
        if let Some(b) = self.carry.take() {
            // The deferred byte has no successor: a `\r` or escape character
            // at end of stream is literal data.
            match self.state {
                CsvState::QuoteInQuoted => {
                    return Err(bad_copy_data(
                        "found unexpected data after a closing quote in CSV field",
                    ));
                }
                CsvState::InQuoted | CsvState::InUnquoted | CsvState::FieldStart => {
                    self.push_data(b);
                }
            }
        }
        if matches!(self.state, CsvState::InQuoted) {
            return Err(bad_copy_data("unterminated CSV quoted field"));
        }
        // A field is pending unless the stream ended exactly at a record
        // boundary. A trailing delimiter also implies one (empty) field.
        let has_pending_field = !self.fields.is_empty()
            || !self.current.is_empty()
            || self.current_quoted
            || matches!(self.state, CsvState::InUnquoted | CsvState::QuoteInQuoted);
        if has_pending_field {
            self.end_field();
        }
        if !self.fields.is_empty() {
            return Ok(vec![std::mem::take(&mut self.fields)]);
        }
        Ok(Vec::new())
    }
}

/// `b` starts a record terminator (`\n` or the `\r` of a `\r\n` pair).
fn is_record_end(b: u8, next: Option<u8>) -> bool {
    b == b'\n' || (b == b'\r' && next == Some(b'\n'))
}

/// `b` may start a record terminator but its successor is unknown.
fn is_incomplete_record_end(b: u8, next: Option<u8>) -> bool {
    b == b'\r' && next.is_none()
}

fn record_end_len(b: u8) -> usize {
    if b == b'\r' { 2 } else { 1 }
}

impl CsvRecordParser {
    fn end_field(&mut self) {
        let value = std::mem::take(&mut self.current);
        let quoted = self.current_quoted;
        self.current_quoted = false;
        if !quoted && value == self.null {
            self.fields.push(Field::Null);
        } else {
            self.fields.push(Field::Value(value));
        }
    }

    fn end_record(&mut self) -> Record {
        self.end_field();
        std::mem::take(&mut self.fields)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_defaults() {
        let codec = build_csv_codec(CopyOptionSet::default()).unwrap();
        assert_eq!(codec.name(), "csv");
    }

    #[test]
    fn test_build_rejects_delimiter_equal_quote() {
        let options = CopyOptionSet {
            delimiter: Some(b'"'),
            ..Default::default()
        };
        let err = build_csv_codec(options).unwrap_err();
        assert!(
            err.to_string()
                .contains("DELIMITER must not be equal to QUOTE")
        );
    }

    fn csv_parser() -> CsvRecordParser {
        CsvRecordParser::new(b',', b'"', b'"', b"", false)
    }

    #[test]
    fn test_csv_parse_records() {
        let mut parser = csv_parser();
        let records = parser
            .feed(b"a,b,\"multi\nline\",plain\"quote\n1,,x\n")
            .unwrap();
        assert_eq!(records.len(), 2);
        assert!(matches!(&records[0][0], Field::Value(v) if v == b"a"));
        assert!(matches!(&records[0][2], Field::Value(v) if v == b"multi\nline"));
        // Unquoted empty field is NULL; an unquoted quote is plain data.
        assert!(matches!(&records[1][1], Field::Null));
        assert!(matches!(&records[0][3], Field::Value(v) if v == b"plain\"quote"));
    }

    #[test]
    fn test_csv_escaped_quotes() {
        let mut parser = csv_parser();
        let records = parser.feed(b"\"say \"\"hi\"\"\"\n").unwrap();
        assert!(matches!(&records[0][0], Field::Value(v) if v == b"say \"hi\""));
    }

    #[test]
    fn test_csv_chunk_boundaries() {
        let mut parser = csv_parser();
        // Quoted field containing a newline, then a CRLF record separator
        // split across chunks.
        assert!(parser.feed(b"\"ab").unwrap().is_empty());
        assert!(parser.feed(b"c\nd\",x").unwrap().is_empty());
        let records = parser.feed(b"\r\nnext\r\n").unwrap();
        assert_eq!(records.len(), 2);
        assert!(matches!(&records[0][0], Field::Value(v) if v == b"abc\nd"));
        assert!(matches!(&records[1][0], Field::Value(v) if v == b"next"));
    }

    #[test]
    fn test_csv_lone_cr_is_data() {
        let mut parser = csv_parser();
        let records = parser.feed(b"a\rb\nc\r").unwrap();
        assert_eq!(records.len(), 1);
        assert!(matches!(&records[0][0], Field::Value(v) if v == b"a\rb"));
        // Trailing lone `\r` becomes data of a final unterminated record.
        let records = parser.finish().unwrap();
        assert_eq!(records.len(), 1);
        assert!(matches!(&records[0][0], Field::Value(v) if v == b"c\r"));
    }

    #[test]
    fn test_csv_unterminated_quote() {
        let mut parser = csv_parser();
        parser.feed(b"\"abc").unwrap();
        let err = match parser.finish() {
            Ok(_) => panic!("expected parse error"),
            Err(e) => e,
        };
        assert!(err.to_string().contains("unterminated"));
    }

    #[test]
    fn test_csv_custom_null() {
        let mut parser = CsvRecordParser::new(b',', b'"', b'"', b"NULL", false);
        let records = parser.feed(b"NULL,\"NULL\"\n").unwrap();
        assert!(matches!(&records[0][0], Field::Null));
        assert!(matches!(&records[0][1], Field::Value(v) if v == b"NULL"));
    }

    #[test]
    fn test_csv_custom_escape() {
        let mut parser = CsvRecordParser::new(b',', b'"', b'\\', b"", false);
        let records = parser.feed(b"\"a\\\"b\",x\n").unwrap();
        assert!(matches!(&records[0][0], Field::Value(v) if v == b"a\"b"));
        assert!(matches!(&records[0][1], Field::Value(v) if v == b"x"));
    }

    #[test]
    fn test_csv_stray_data_after_quote() {
        let mut parser = csv_parser();
        let err = match parser.feed(b"\"ab\"c\n") {
            Ok(_) => panic!("expected parse error"),
            Err(e) => e,
        };
        assert!(err.to_string().contains("unexpected data"));
    }

    #[test]
    fn test_csv_finish_at_record_boundary() {
        let mut parser = csv_parser();
        parser.feed(b"a,b\n").unwrap();
        assert!(parser.finish().unwrap().is_empty());
    }

    #[test]
    fn test_csv_trailing_delimiter() {
        let mut parser = csv_parser();
        parser.feed(b"a,").unwrap();
        let records = parser.finish().unwrap();
        assert_eq!(records.len(), 1);
        assert!(matches!(&records[0][0], Field::Value(v) if v == b"a"));
        assert!(matches!(&records[0][1], Field::Null));
    }
}
