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

use snafu::{ensure, ResultExt};
use sqlparser::keywords::Keyword;
use sqlparser::tokenizer::Token;

use crate::error::{self, InvalidSqlSnafu, InvalidTableNameSnafu, Result, UnexpectedTokenSnafu};
use crate::parser::ParserContext;
use crate::statements::statement::Statement;
use crate::statements::truncate::TruncateTable;

/// `TRUNCATE [TABLE] table_name;`
impl ParserContext<'_> {
    pub(crate) fn parse_truncate(&mut self) -> Result<Statement> {
        let _ = self.parser.next_token();
        let _ = self.parser.parse_keyword(Keyword::TABLE);

        let raw_table_ident =
            self.parse_object_name()
                .with_context(|_| error::UnexpectedSnafu {
                    expected: "a table name",
                    actual: self.peek_token_as_string(),
                })?;
        let table_ident = Self::canonicalize_object_name(raw_table_ident);

        ensure!(
            !table_ident.0.is_empty(),
            InvalidTableNameSnafu {
                name: table_ident.to_string()
            }
        );

        let have_range = self.parser.parse_keywords(&[Keyword::FILE, Keyword::RANGE]);

        // if no range is specified, we just truncate the table
        if !have_range {
            return Ok(Statement::TruncateTable(TruncateTable::new(table_ident)));
        }

        // parse a list of time ranges consist of (Timestamp, Timestamp),?
        let mut time_ranges = vec![];
        loop {
            let _ = self
                .parser
                .expect_token(&sqlparser::tokenizer::Token::LParen)
                .with_context(|_| error::UnexpectedSnafu {
                    expected: "a left parenthesis",
                    actual: self.peek_token_as_string(),
                })?;

            // parse to values here, no need to valid in parser
            let start = self
                .parser
                .parse_value()
                .map(|x| x.value)
                .with_context(|e| error::UnexpectedSnafu {
                    expected: "a timestamp value",
                    actual: e.to_string(),
                })?;

            let _ = self
                .parser
                .expect_token(&sqlparser::tokenizer::Token::Comma)
                .with_context(|_| error::UnexpectedSnafu {
                    expected: "a comma",
                    actual: self.peek_token_as_string(),
                })?;

            let end = self
                .parser
                .parse_value()
                .map(|x| x.value)
                .with_context(|_| error::UnexpectedSnafu {
                    expected: "a timestamp",
                    actual: self.peek_token_as_string(),
                })?;

            let _ = self
                .parser
                .expect_token(&sqlparser::tokenizer::Token::RParen)
                .with_context(|_| error::UnexpectedSnafu {
                    expected: "a right parenthesis",
                    actual: self.peek_token_as_string(),
                })?;

            time_ranges.push((start, end));

            let peek = self.parser.peek_token().token;

            match peek {
                sqlparser::tokenizer::Token::EOF | Token::SemiColon => {
                    if time_ranges.is_empty() {
                        return Err(InvalidSqlSnafu {
                            msg: "TRUNCATE TABLE RANGE must have at least one range".to_string(),
                        }
                        .build());
                    }
                    break;
                }
                Token::Comma => {
                    self.parser.next_token(); // Consume the comma
                    let next_peek = self.parser.peek_token().token; // Peek the token after the comma
                    if matches!(next_peek, Token::EOF | Token::SemiColon) {
                        break; // Trailing comma, end of statement
                    }
                    // Otherwise, continue to parse next range
                    continue;
                }
                _ => UnexpectedTokenSnafu {
                    expected: "a comma or end of statement",
                    actual: self.peek_token_as_string(),
                }
                .fail()?,
            }
        }

        Ok(Statement::TruncateTable(TruncateTable::new_with_ranges(
            table_ident,
            time_ranges,
        )))
    }
}

#[cfg(test)]
mod tests {
    use sqlparser::ast::{Ident, ObjectName};

    use super::*;
    use crate::dialect::GreptimeDbDialect;
    use crate::parser::ParseOptions;

    #[test]
    pub fn test_parse_truncate_with_ranges() {
        let sql = r#"TRUNCATE foo FILE RANGE (0, 20)"#;
        let mut stmts =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();
        assert_eq!(
            stmts.pop().unwrap(),
            Statement::TruncateTable(TruncateTable::new_with_ranges(
                ObjectName::from(vec![Ident::new("foo")]),
                vec![(
                    sqlparser::ast::Value::Number("0".to_string(), false),
                    sqlparser::ast::Value::Number("20".to_string(), false)
                )]
            ))
        );

        let sql = r#"TRUNCATE TABLE foo FILE RANGE ("2000-01-01 00:00:00+00:00", "2000-01-01 00:00:00+00:00"), (2,33)"#;
        let mut stmts =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();
        assert_eq!(
            stmts.pop().unwrap(),
            Statement::TruncateTable(TruncateTable::new_with_ranges(
                ObjectName::from(vec![Ident::new("foo")]),
                vec![
                    (
                        sqlparser::ast::Value::DoubleQuotedString(
                            "2000-01-01 00:00:00+00:00".to_string()
                        ),
                        sqlparser::ast::Value::DoubleQuotedString(
                            "2000-01-01 00:00:00+00:00".to_string()
                        )
                    ),
                    (
                        sqlparser::ast::Value::Number("2".to_string(), false),
                        sqlparser::ast::Value::Number("33".to_string(), false)
                    )
                ]
            ))
        );

        let sql = "TRUNCATE TABLE my_schema.foo FILE RANGE (1, 2), (3, 4),";
        let mut stmts =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();
        assert_eq!(
            stmts.pop().unwrap(),
            Statement::TruncateTable(TruncateTable::new_with_ranges(
                ObjectName::from(vec![Ident::new("my_schema"), Ident::new("foo")]),
                vec![
                    (
                        sqlparser::ast::Value::Number("1".to_string(), false),
                        sqlparser::ast::Value::Number("2".to_string(), false)
                    ),
                    (
                        sqlparser::ast::Value::Number("3".to_string(), false),
                        sqlparser::ast::Value::Number("4".to_string(), false)
                    )
                ]
            ))
        );

        let sql = "TRUNCATE my_schema.foo FILE RANGE (1,2),";
        let mut stmts =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();
        assert_eq!(
            stmts.pop().unwrap(),
            Statement::TruncateTable(TruncateTable::new_with_ranges(
                ObjectName::from(vec![Ident::new("my_schema"), Ident::new("foo")]),
                vec![(
                    sqlparser::ast::Value::Number("1".to_string(), false),
                    sqlparser::ast::Value::Number("2".to_string(), false)
                )]
            ))
        );

        let sql = "TRUNCATE TABLE my_catalog.my_schema.foo FILE RANGE (1,2);";
        let mut stmts =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();
        assert_eq!(
            stmts.pop().unwrap(),
            Statement::TruncateTable(TruncateTable::new_with_ranges(
                ObjectName::from(vec![
                    Ident::new("my_catalog"),
                    Ident::new("my_schema"),
                    Ident::new("foo")
                ]),
                vec![(
                    sqlparser::ast::Value::Number("1".to_string(), false),
                    sqlparser::ast::Value::Number("2".to_string(), false)
                )]
            ))
        );

        let sql = "TRUNCATE drop FILE RANGE (1,2)";
        let mut stmts =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();
        assert_eq!(
            stmts.pop().unwrap(),
            Statement::TruncateTable(TruncateTable::new_with_ranges(
                ObjectName::from(vec![Ident::new("drop")]),
                vec![(
                    sqlparser::ast::Value::Number("1".to_string(), false),
                    sqlparser::ast::Value::Number("2".to_string(), false)
                )]
            ))
        );

        let sql = "TRUNCATE `drop`";
        let mut stmts =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();
        assert_eq!(
            stmts.pop().unwrap(),
            Statement::TruncateTable(TruncateTable::new(ObjectName::from(vec![
                Ident::with_quote('`', "drop"),
            ])))
        );

        let sql = "TRUNCATE \"drop\" FILE RANGE (\"1\", \"2\")";
        let mut stmts =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();
        assert_eq!(
            stmts.pop().unwrap(),
            Statement::TruncateTable(TruncateTable::new_with_ranges(
                ObjectName::from(vec![Ident::with_quote('"', "drop")]),
                vec![(
                    sqlparser::ast::Value::DoubleQuotedString("1".to_string()),
                    sqlparser::ast::Value::DoubleQuotedString("2".to_string())
                )]
            ))
        );
    }

    #[test]
    pub fn test_parse_truncate() {
        let sql = "TRUNCATE foo";
        let mut stmts =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();
        assert_eq!(
            stmts.pop().unwrap(),
            Statement::TruncateTable(TruncateTable::new(ObjectName::from(vec![Ident::new(
                "foo"
            )])))
        );

        let sql = "TRUNCATE TABLE foo";
        let mut stmts =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();
        assert_eq!(
            stmts.pop().unwrap(),
            Statement::TruncateTable(TruncateTable::new(ObjectName::from(vec![Ident::new(
                "foo"
            )])))
        );

        let sql = "TRUNCATE TABLE my_schema.foo";
        let mut stmts =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();
        assert_eq!(
            stmts.pop().unwrap(),
            Statement::TruncateTable(TruncateTable::new(ObjectName::from(vec![
                Ident::new("my_schema"),
                Ident::new("foo")
            ])))
        );

        let sql = "TRUNCATE my_schema.foo";
        let mut stmts =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();
        assert_eq!(
            stmts.pop().unwrap(),
            Statement::TruncateTable(TruncateTable::new(ObjectName::from(vec![
                Ident::new("my_schema"),
                Ident::new("foo")
            ])))
        );

        let sql = "TRUNCATE TABLE my_catalog.my_schema.foo";
        let mut stmts =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();
        assert_eq!(
            stmts.pop().unwrap(),
            Statement::TruncateTable(TruncateTable::new(ObjectName::from(vec![
                Ident::new("my_catalog"),
                Ident::new("my_schema"),
                Ident::new("foo")
            ])))
        );

        let sql = "TRUNCATE drop";
        let mut stmts =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();
        assert_eq!(
            stmts.pop().unwrap(),
            Statement::TruncateTable(TruncateTable::new(ObjectName::from(vec![Ident::new(
                "drop"
            )])))
        );

        let sql = "TRUNCATE `drop`";
        let mut stmts =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();
        assert_eq!(
            stmts.pop().unwrap(),
            Statement::TruncateTable(TruncateTable::new(ObjectName::from(vec![
                Ident::with_quote('`', "drop"),
            ])))
        );

        let sql = "TRUNCATE \"drop\"";
        let mut stmts =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();
        assert_eq!(
            stmts.pop().unwrap(),
            Statement::TruncateTable(TruncateTable::new(ObjectName::from(vec![
                Ident::with_quote('"', "drop"),
            ])))
        );
    }

    #[test]
    pub fn test_parse_invalid_truncate() {
        let sql = "TRUNCATE SCHEMA foo";
        let result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default());
        assert!(result.is_err(), "result is: {result:?}");

        let sql = "TRUNCATE";
        let result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default());
        assert!(result.is_err(), "result is: {result:?}");

        let sql = "TRUNCATE TABLE foo RANGE";
        let result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default());
        assert!(
            result.is_err()
                && format!("{result:?}").contains("SQL statement is not supported, keyword: RANGE"),
            "result is: {result:?}"
        );

        let sql = "TRUNCATE TABLE foo FILE";
        let result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default());
        assert!(
            result.is_err()
                && format!("{result:?}").contains("SQL statement is not supported, keyword: FILE"),
            "result is: {result:?}"
        );

        let sql = "TRUNCATE TABLE foo FILE RANGE";
        let result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default());
        assert!(
            result.is_err() && format!("{result:?}").contains("expected: 'a left parenthesis'"),
            "result is: {result:?}"
        );

        let sql = "TRUNCATE TABLE foo FILE RANGE (";
        let result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default());
        assert!(
            result.is_err() && format!("{result:?}").contains("expected: 'a timestamp value'"),
            "result is: {result:?}"
        );

        let sql = "TRUNCATE TABLE foo FILE RANGE ()";
        let result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default());
        assert!(
            result.is_err() && format!("{result:?}").contains("expected: 'a timestamp value'"),
            "result is: {result:?}"
        );

        let sql = "TRUNCATE TABLE foo FILE RANGE (1 2) (3 4)";
        let result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default());
        assert!(
            result.is_err() && format!("{result:?}").contains("expected: 'a comma'"),
            "result is: {result:?}"
        );

        let sql = "TRUNCATE TABLE foo FILE RANGE (,),(3,4)";
        let result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default());
        assert!(
            result.is_err() && format!("{result:?}").contains("Expected: a value, found: ,"),
            "result is: {result:?}"
        );

        let sql = "TRUNCATE TABLE foo FILE RANGE (1,2) (3,4)";
        let result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default());
        assert!(
            result.is_err()
                && format!("{result:?}")
                    .contains("expected: 'a comma or end of statement', found: ("),
            "result is: {result:?}"
        );

        let sql = "TRUNCATE TABLE foo FILE RANGE (1,2),,,,,,,,,(3,4)";
        let result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default());
        assert!(
            result.is_err()
                && format!("{result:?}").contains("expected: 'a left parenthesis', found: ,"),
            "result is: {result:?}"
        );
    }
}
