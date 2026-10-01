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

//! Textual parser for `COPY ... FROM STDIN` statements.
//!
//! COPY FROM STDIN is parsed here instead of through sqlparser because the
//! `WITH (...)` option list is open: formats may accept options beyond
//! sqlparser's COPY grammar (for example a line-protocol format taking
//! `precision`), and unknown options must reach the format codec rather
//! than fail statement parsing.
//!
//! The parser commits to error reporting only after `FROM STDIN` has been
//! seen; anything else (like `COPY ... TO STDOUT`) is reported as
//! [`CopyInScan::NotCopyIn`] so the caller falls back to the regular SQL
//! path.

use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::tokenizer::{Token, Tokenizer};
use pgwire::error::PgWireResult;

use super::options::{CopyOptionSet, CopyOptionValue};
use super::{bad_copy_data, copy_in_must_run_alone_error};

/// A scanned `COPY tbl [(cols)] FROM STDIN [WITH (...)]` statement.
#[derive(Debug)]
pub(super) struct RawCopyFromStdin {
    /// Table name parts, as written (`t`, `schema.t`,
    /// `catalog.schema.t`); quoted names keep their case.
    pub(super) table: Vec<String>,
    /// Requested column list; empty means all columns in table order.
    pub(super) columns: Vec<String>,
    /// The open option list, handed to the resolved format codec.
    pub(super) options: CopyOptionSet,
}

pub(super) enum CopyInScan {
    /// Not a `COPY ... FROM STDIN` statement.
    NotCopyIn,
    /// A complete copy-in statement with only trailing separators left.
    CopyIn(RawCopyFromStdin),
}

/// Scans a SQL string for a leading `COPY ... FROM STDIN` statement.
pub(super) fn scan_copy_from_stdin(sql: &str) -> PgWireResult<CopyInScan> {
    // Fast path: the statement must start with the COPY keyword, possibly
    // behind comments; only then is a full tokenization worth its cost.
    let trimmed = sql.trim_start();
    let starts_like_copy = trimmed
        .as_bytes()
        .get(..4)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case(b"copy"));
    let starts_with_comment = trimmed.starts_with("--") || trimmed.starts_with("/*");
    if !starts_like_copy && !starts_with_comment {
        return Ok(CopyInScan::NotCopyIn);
    }

    let tokens = match Tokenizer::new(&GenericDialect {}, sql).tokenize() {
        Ok(tokens) => tokens,
        // Let the regular SQL path produce the parse error.
        Err(_) => return Ok(CopyInScan::NotCopyIn),
    };
    // The tokenizer keeps whitespace; the scanner works on tokens only.
    let tokens: Vec<Token> = tokens
        .into_iter()
        .filter(|token| !matches!(token, Token::Whitespace(_)))
        .collect();
    let mut cursor = Tokens {
        tokens: &tokens,
        pos: 0,
    };

    if !cursor.take_keyword("copy") {
        return Ok(CopyInScan::NotCopyIn);
    }

    let table = match parse_object_name(&mut cursor) {
        Some(table) => table,
        None => return Ok(CopyInScan::NotCopyIn),
    };
    let columns = match parse_column_list(&mut cursor) {
        Some(columns) => columns,
        None => return Ok(CopyInScan::NotCopyIn),
    };

    if !cursor.take_keyword("from") || !cursor.take_keyword("stdin") {
        // A COPY with another direction or source; not ours to handle.
        return Ok(CopyInScan::NotCopyIn);
    }

    let mut options = CopyOptionSet::default();
    // PostgreSQL allows the WITH keyword to be omitted.
    let has_options = if cursor.take_keyword("with") {
        if !matches!(cursor.peek(), Some(Token::LParen)) {
            return Err(bad_copy_data("expected '(' after WITH in COPY FROM STDIN"));
        }
        true
    } else {
        matches!(cursor.peek(), Some(Token::LParen))
    };
    if has_options {
        // Consume the opening '('.
        cursor.next();
        parse_option_list(&mut cursor, &mut options)?;
    }

    // Only statement separators may follow.
    while let Some(token) = cursor.next() {
        if !matches!(token, Token::SemiColon) {
            return Err(copy_in_must_run_alone_error());
        }
    }

    Ok(CopyInScan::CopyIn(RawCopyFromStdin {
        table,
        columns,
        options,
    }))
}

struct Tokens<'a> {
    tokens: &'a [Token],
    pos: usize,
}

impl<'a> Tokens<'a> {
    fn next(&mut self) -> Option<&'a Token> {
        let token = self.tokens.get(self.pos)?;
        self.pos += 1;
        Some(token)
    }

    fn peek(&mut self) -> Option<&'a Token> {
        self.tokens.get(self.pos)
    }

    /// Consumes the next token if it is the unquoted keyword `keyword`.
    fn take_keyword(&mut self, keyword: &str) -> bool {
        match self.peek() {
            Some(Token::Word(w))
                if w.quote_style.is_none() && w.value.eq_ignore_ascii_case(keyword) =>
            {
                self.pos += 1;
                true
            }
            _ => false,
        }
    }
}

fn expect_ident<'a>(cursor: &mut Tokens<'a>, _what: &str) -> Option<String> {
    match cursor.next() {
        Some(Token::Word(w)) => Some(w.value.clone()),
        _ => None,
    }
}

fn parse_object_name(cursor: &mut Tokens<'_>) -> Option<Vec<String>> {
    let mut parts = vec![expect_ident(cursor, "a table name")?];
    while matches!(cursor.peek(), Some(Token::Period)) {
        cursor.next();
        parts.push(expect_ident(cursor, "a name after '.'")?);
    }
    Some(parts)
}

/// Parses an optional `(column, ...)` list. `None` means the statement is
/// not a copy-in shape (either no list, or a malformed list better left to
/// the regular parser to report).
fn parse_column_list(cursor: &mut Tokens<'_>) -> Option<Vec<String>> {
    if !matches!(cursor.peek(), Some(Token::LParen)) {
        return Some(Vec::new());
    }
    cursor.next();
    let mut columns = vec![expect_ident(cursor, "a column name")?];
    loop {
        match cursor.next() {
            Some(Token::Comma) => columns.push(expect_ident(cursor, "a column name")?),
            Some(Token::RParen) => break,
            _ => return None,
        }
    }
    Some(columns)
}

/// Parses `NAME [value] (, NAME [value])*` until the closing `)`.
///
/// Option values are open-ended: a string literal, a number, an identifier
/// or a boolean word. Whether an option makes sense is decided by the
/// format codec, not here.
fn parse_option_list(cursor: &mut Tokens<'_>, options: &mut CopyOptionSet) -> PgWireResult<()> {
    loop {
        let key = match cursor.next() {
            Some(Token::Word(w)) if w.quote_style.is_none() => w.value.to_lowercase(),
            _ => return Err(bad_copy_data("expected an option name after '(' or ','")),
        };
        let value = match cursor.peek() {
            Some(Token::Comma) | Some(Token::RParen) => CopyOptionValue::Flag,
            Some(Token::SingleQuotedString(s)) => {
                let value = CopyOptionValue::String(s.clone());
                cursor.next();
                value
            }
            Some(Token::Number(n, _)) => {
                let value = CopyOptionValue::Number(n.clone());
                cursor.next();
                value
            }
            Some(Token::Word(w)) => {
                let value = if w.quote_style.is_none() {
                    match w.value.to_ascii_lowercase().as_str() {
                        "true" => CopyOptionValue::Boolean(true),
                        "false" => CopyOptionValue::Boolean(false),
                        _ => CopyOptionValue::Ident(w.value.clone()),
                    }
                } else {
                    CopyOptionValue::Ident(w.value.clone())
                };
                cursor.next();
                value
            }
            _ => return Err(bad_copy_data("expected an option value or ',' or ')'")),
        };
        options.insert(key, value)?;
        match cursor.next() {
            Some(Token::Comma) => continue,
            Some(Token::RParen) => break,
            _ => {
                return Err(bad_copy_data(
                    "expected ',' or ')' in COPY FROM STDIN options",
                ));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scan(sql: &str) -> PgWireResult<Option<RawCopyFromStdin>> {
        Ok(match scan_copy_from_stdin(sql)? {
            CopyInScan::CopyIn(raw) => Some(raw),
            CopyInScan::NotCopyIn => None,
        })
    }

    #[test]
    fn test_scan_basic() {
        let raw = scan("COPY t FROM STDIN").unwrap().unwrap();
        assert_eq!(raw.table, vec!["t"]);
        assert!(raw.columns.is_empty());
    }

    #[test]
    fn test_scan_qualified_table_and_columns() {
        let raw = scan("COPY db.public.Metrics (ts, \"Host\") FROM STDIN")
            .unwrap()
            .unwrap();
        assert_eq!(raw.table, vec!["db", "public", "Metrics"]);
        assert_eq!(raw.columns, vec!["ts", "Host"]);
    }

    #[test]
    fn test_scan_not_copy_in() {
        assert!(scan("COPY (SELECT 1) TO STDOUT").unwrap().is_none());
        assert!(
            scan("COPY t TO '/tmp/x.csv' WITH (FORMAT csv)")
                .unwrap()
                .is_none()
        );
        assert!(scan("SELECT 1").unwrap().is_none());
        // GreptimeDB's own file-based COPY FROM.
        assert!(scan("COPY t FROM '/tmp/x.csv'").unwrap().is_none());
    }

    #[test]
    fn test_scan_leading_comment() {
        let raw = scan("-- a comment\nCOPY t FROM STDIN").unwrap().unwrap();
        assert_eq!(raw.table, vec!["t"]);
    }

    #[test]
    fn test_scan_case_insensitive() {
        let raw = scan("copy T from stdin").unwrap().unwrap();
        assert_eq!(raw.table, vec!["T"]);
    }

    #[test]
    fn test_scan_options_kinds() {
        let raw = scan(
            "COPY t FROM STDIN WITH (FORMAT csv, DELIMITER ';', HEADER, \
             skip_bad_records true, precision 3, mode strict)",
        )
        .unwrap()
        .unwrap();
        let mut options = raw.options;
        assert_eq!(options.take_format().unwrap().as_deref(), Some("csv"));
        assert_eq!(options.take_single_byte("DELIMITER").unwrap(), Some(b';'));
        assert_eq!(options.take_flag("HEADER").unwrap(), Some(true));
        assert_eq!(
            options.take_value("skip_bad_records"),
            Some(CopyOptionValue::Boolean(true))
        );
        assert_eq!(
            options.take_value("precision"),
            Some(CopyOptionValue::Number("3".into()))
        );
        assert_eq!(
            options.take_value("mode"),
            Some(CopyOptionValue::Ident("strict".into()))
        );
    }

    #[test]
    fn test_scan_semicolon_inside_option_value() {
        // A quoted semicolon is not a statement separator.
        let mut raw = scan("COPY t FROM STDIN WITH (NULL ';')").unwrap().unwrap();
        assert_eq!(
            raw.options.take_string("NULL").unwrap().as_deref(),
            Some(";")
        );
    }

    #[test]
    fn test_scan_trailing_statements_rejected() {
        let err = scan("COPY t FROM STDIN; SELECT 1").unwrap_err();
        assert!(err.to_string().contains("alone"));

        // Trailing separators alone are fine.
        assert!(scan("COPY t FROM STDIN;").unwrap().is_some());
        assert!(scan("COPY t FROM STDIN ; ;").unwrap().is_some());
    }

    #[test]
    fn test_scan_options_without_with_keyword() {
        let raw = scan("COPY t FROM STDIN (FORMAT csv, HEADER)")
            .unwrap()
            .unwrap();
        let mut options = raw.options;
        assert_eq!(options.take_format().unwrap().as_deref(), Some("csv"));
        assert_eq!(options.take_flag("HEADER").unwrap(), Some(true));
        assert!(options.reject_leftovers("csv").is_ok());
    }

    #[test]
    fn test_scan_malformed_after_stdin() {
        let err = scan("COPY t FROM STDIN WITH").unwrap_err();
        assert!(err.to_string().contains("'('"), "{err}");
        let err = scan("COPY t FROM STDIN WITH (").unwrap_err();
        assert!(err.to_string().contains("option"), "{err}");
        // A malformed column list never commits to the copy-in shape and
        // falls through to the regular parser.
        assert!(scan("COPY t (a,").unwrap().is_none());
    }

    #[test]
    fn test_scan_redundant_option() {
        let err = scan("COPY t FROM STDIN WITH (FORMAT csv, FORMAT text)").unwrap_err();
        assert!(err.to_string().contains("redundant FORMAT"));
    }
}
