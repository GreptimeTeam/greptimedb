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

use common_time::Timestamp;
use datatypes::prelude::ConcreteDataType;
use snafu::{ensure, ResultExt};
use sqlparser::keywords::Keyword;

use crate::error::{self, InvalidTableNameSnafu, Result, SqlCommonSnafu};
use crate::parser::ParserContext;
use crate::statements::statement::Statement;
use crate::statements::truncate::TruncateTable;

/// `TRUNCATE [TABLE] table_name;`
impl ParserContext<'_> {
    pub(crate) fn parse_timestamp(&mut self) -> Result<Timestamp> {
        let val = self
            .parser
            .parse_value()
            .with_context(|_| error::UnexpectedSnafu {
                expected: "a timestamp",
                actual: self.peek_token_as_string(),
            })
            .and_then(|v| {
                common_sql::convert::sql_value_to_value(
                    "dummy_column",
                    &ConcreteDataType::timestamp_millisecond_datatype(),
                    &v,
                    None,
                    None,
                    false,
                )
                .context(SqlCommonSnafu)
            })?;
        if let datatypes::value::Value::Timestamp(t) = val {
            Ok(t)
        } else {
            error::InvalidSqlSnafu {
                msg: "Expected a timestamp value".to_string(),
            }
            .fail()
        }
    }
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

        let _ = self.parser.parse_keyword(Keyword::RANGE);
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

            // TODO(discord9): parse timestamp here
            let start = self.parse_timestamp()?;

            let _ = self
                .parser
                .expect_token(&sqlparser::tokenizer::Token::Comma)
                .with_context(|_| error::UnexpectedSnafu {
                    expected: "a comma",
                    actual: self.peek_token_as_string(),
                })?;

            let end = self.parse_timestamp()?;

            let _ = self
                .parser
                .expect_token(&sqlparser::tokenizer::Token::RParen)
                .with_context(|_| error::UnexpectedSnafu {
                    expected: "a right parenthesis",
                    actual: self.peek_token_as_string(),
                })?;

            time_ranges.push((start, end));

            let is_end = self
                .parser
                .expect_token(&sqlparser::tokenizer::Token::Comma)
                .is_err();
            if is_end {
                break;
            }
        }

        Ok(Statement::TruncateTable(TruncateTable::new(table_ident)))
    }
}

#[cfg(test)]
mod tests {
    use sqlparser::ast::{Ident, ObjectName};

    use super::*;
    use crate::dialect::GreptimeDbDialect;
    use crate::parser::ParseOptions;

    #[test]
    pub fn test_parse_truncate() {
        let sql = "TRUNCATE foo";
        let mut stmts =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();
        assert_eq!(
            stmts.pop().unwrap(),
            Statement::TruncateTable(TruncateTable::new(ObjectName(vec![Ident::new("foo")])))
        );

        let sql = "TRUNCATE TABLE foo";
        let mut stmts =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();
        assert_eq!(
            stmts.pop().unwrap(),
            Statement::TruncateTable(TruncateTable::new(ObjectName(vec![Ident::new("foo")])))
        );

        let sql = "TRUNCATE TABLE my_schema.foo";
        let mut stmts =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();
        assert_eq!(
            stmts.pop().unwrap(),
            Statement::TruncateTable(TruncateTable::new(ObjectName(vec![
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
            Statement::TruncateTable(TruncateTable::new(ObjectName(vec![
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
            Statement::TruncateTable(TruncateTable::new(ObjectName(vec![
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
            Statement::TruncateTable(TruncateTable::new(ObjectName(vec![Ident::new("drop")])))
        );

        let sql = "TRUNCATE `drop`";
        let mut stmts =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();
        assert_eq!(
            stmts.pop().unwrap(),
            Statement::TruncateTable(TruncateTable::new(ObjectName(vec![Ident::with_quote(
                '`', "drop"
            ),])))
        );

        let sql = "TRUNCATE \"drop\"";
        let mut stmts =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();
        assert_eq!(
            stmts.pop().unwrap(),
            Statement::TruncateTable(TruncateTable::new(ObjectName(vec![Ident::with_quote(
                '"', "drop"
            ),])))
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
    }
}
