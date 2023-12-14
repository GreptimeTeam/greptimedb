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

use snafu::ResultExt;
use sqlparser::ast::Ident;
use sqlparser::dialect::Dialect;
use sqlparser::keywords::Keyword;
use sqlparser::parser::{Parser, ParserError, ParserOptions};
use sqlparser::tokenizer::{Token, TokenWithLocation};

use crate::ast::{Expr, ObjectName};
use crate::error::{self, Result, SyntaxSnafu};
use crate::parsers::tql_parser;
use crate::statements::statement::Statement;
use crate::statements::transform_statements;

/// GrepTime SQL parser context, a simple wrapper for Datafusion SQL parser.
pub struct ParserContext<'a> {
    pub(crate) parser: Parser<'a>,
    pub(crate) sql: &'a str,
}

impl<'a> ParserContext<'a> {
    /// Parses SQL with given dialect
    pub fn create_with_dialect(sql: &'a str, dialect: &dyn Dialect) -> Result<Vec<Statement>> {
        let mut stmts: Vec<Statement> = Vec::new();

        let parser = Parser::new(dialect)
            .with_options(ParserOptions::new().with_trailing_commas(true))
            .try_with_sql(sql)
            .context(SyntaxSnafu)?;
        let mut parser_ctx = ParserContext { sql, parser };

        let mut expecting_statement_delimiter = false;
        loop {
            // ignore empty statements (between successive statement delimiters)
            while parser_ctx.parser.consume_token(&Token::SemiColon) {
                expecting_statement_delimiter = false;
            }

            if parser_ctx.parser.peek_token() == Token::EOF {
                break;
            }
            if expecting_statement_delimiter {
                return parser_ctx.unsupported(parser_ctx.peek_token_as_string());
            }

            let statement = parser_ctx.parse_statement()?;
            stmts.push(statement);
            expecting_statement_delimiter = true;
        }

        transform_statements(&mut stmts)?;

        Ok(stmts)
    }

    pub fn parse_function(sql: &'a str, dialect: &dyn Dialect) -> Result<Expr> {
        let mut parser = Parser::new(dialect)
            .with_options(ParserOptions::new().with_trailing_commas(true))
            .try_with_sql(sql)
            .context(SyntaxSnafu)?;

        let function_name = parser.parse_identifier().context(SyntaxSnafu)?;
        parser
            .parse_function(ObjectName(vec![function_name]))
            .context(SyntaxSnafu)
    }

    /// Parses parser context to a set of statements.
    pub fn parse_statement(&mut self) -> Result<Statement> {
        match self.parser.peek_token().token {
            Token::Word(w) => {
                match w.keyword {
                    Keyword::CREATE => {
                        let _ = self.parser.next_token();
                        self.parse_create()
                    }

                    Keyword::EXPLAIN => {
                        let _ = self.parser.next_token();
                        self.parse_explain()
                    }

                    Keyword::SHOW => {
                        let _ = self.parser.next_token();
                        self.parse_show()
                    }

                    Keyword::DELETE => self.parse_delete(),

                    Keyword::DESCRIBE | Keyword::DESC => {
                        let _ = self.parser.next_token();
                        self.parse_describe()
                    }

                    Keyword::INSERT => self.parse_insert(),

                    Keyword::SELECT | Keyword::WITH | Keyword::VALUES => self.parse_query(),

                    Keyword::ALTER => self.parse_alter(),

                    Keyword::DROP => self.parse_drop(),

                    Keyword::COPY => self.parse_copy(),

                    Keyword::TRUNCATE => self.parse_truncate(),

                    Keyword::NoKeyword
                        if w.value.to_uppercase() == tql_parser::TQL && w.quote_style.is_none() =>
                    {
                        self.parse_tql()
                    }

                    // todo(hl) support more statements.
                    _ => self.unsupported(self.peek_token_as_string()),
                }
            }
            Token::LParen => self.parse_query(),
            unexpected => self.unsupported(unexpected.to_string()),
        }
    }

    /// Raises an "unsupported statement" error.
    pub fn unsupported<T>(&self, keyword: String) -> Result<T> {
        error::UnsupportedSnafu {
            sql: self.sql,
            keyword,
        }
        .fail()
    }

    // Report unexpected token
    pub(crate) fn expected<T>(&self, expected: &str, found: TokenWithLocation) -> Result<T> {
        Err(ParserError::ParserError(format!(
            "Expected {expected}, found: {found}",
        )))
        .context(SyntaxSnafu)
    }

    pub fn matches_keyword(&mut self, expected: Keyword) -> bool {
        match self.parser.peek_token().token {
            Token::Word(w) => w.keyword == expected,
            _ => false,
        }
    }

    pub fn consume_token(&mut self, expected: &str) -> bool {
        if self.peek_token_as_string().to_uppercase() == *expected.to_uppercase() {
            let _ = self.parser.next_token();
            true
        } else {
            false
        }
    }

    #[inline]
    pub(crate) fn peek_token_as_string(&self) -> String {
        self.parser.peek_token().to_string()
    }

    /// Canonicalize the identifier to lowercase if it's not quoted.
    pub fn canonicalize_identifier(ident: Ident) -> Ident {
        if ident.quote_style.is_some() {
            ident
        } else {
            Ident {
                value: ident.value.to_lowercase(),
                quote_style: None,
            }
        }
    }

    /// Like [canonicalize_identifier] but for [ObjectName].
    pub fn canonicalize_object_name(object_name: ObjectName) -> ObjectName {
        ObjectName(
            object_name
                .0
                .into_iter()
                .map(Self::canonicalize_identifier)
                .collect(),
        )
    }
}

#[cfg(test)]
mod tests {

    use datatypes::prelude::ConcreteDataType;

    use super::*;
    use crate::dialect::GreptimeDbDialect;
    use crate::statements::create::CreateTable;
    use crate::statements::sql_data_type_to_concrete_data_type;

    fn test_timestamp_precision(sql: &str, expected_type: ConcreteDataType) {
        match ParserContext::create_with_dialect(sql, &GreptimeDbDialect {})
            .unwrap()
            .pop()
            .unwrap()
        {
            Statement::CreateTable(CreateTable { columns, .. }) => {
                let ts_col = columns.first().unwrap();
                assert_eq!(
                    expected_type,
                    sql_data_type_to_concrete_data_type(&ts_col.data_type).unwrap()
                );
            }
            _ => unreachable!(),
        }
    }

    #[test]
    pub fn test_create_table_with_precision() {
        test_timestamp_precision(
            "create table demo (ts timestamp time index, cnt int);",
            ConcreteDataType::timestamp_millisecond_datatype(),
        );
        test_timestamp_precision(
            "create table demo (ts timestamp(0) time index, cnt int);",
            ConcreteDataType::timestamp_second_datatype(),
        );
        test_timestamp_precision(
            "create table demo (ts timestamp(3) time index, cnt int);",
            ConcreteDataType::timestamp_millisecond_datatype(),
        );
        test_timestamp_precision(
            "create table demo (ts timestamp(6) time index, cnt int);",
            ConcreteDataType::timestamp_microsecond_datatype(),
        );
        test_timestamp_precision(
            "create table demo (ts timestamp(9) time index, cnt int);",
            ConcreteDataType::timestamp_nanosecond_datatype(),
        );
    }

    #[test]
    #[should_panic]
    pub fn test_create_table_with_invalid_precision() {
        test_timestamp_precision(
            "create table demo (ts timestamp(1) time index, cnt int);",
            ConcreteDataType::timestamp_millisecond_datatype(),
        );
    }
}
