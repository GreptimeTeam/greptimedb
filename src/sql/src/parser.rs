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
use sqlparser::ast::{Ident, Query};
use sqlparser::dialect::Dialect;
use sqlparser::keywords::Keyword;
use sqlparser::parser::{Parser, ParserError, ParserOptions};
use sqlparser::tokenizer::{Token, TokenWithLocation};

use crate::ast::{Expr, ObjectName};
use crate::error::{self, Result, SyntaxSnafu};
use crate::parsers::tql_parser;
use crate::statements::statement::Statement;
use crate::statements::transform_statements;

pub const FLOW: &str = "FLOW";

/// SQL Parser options.
#[derive(Clone, Debug, Default)]
pub struct ParseOptions {}

/// GrepTime SQL parser context, a simple wrapper for Datafusion SQL parser.
pub struct ParserContext<'a> {
    pub(crate) parser: Parser<'a>,
    pub(crate) sql: &'a str,
}

impl ParserContext<'_> {
    /// Construct a new ParserContext.
    pub fn new<'a>(dialect: &'a dyn Dialect, sql: &'a str) -> Result<ParserContext<'a>> {
        let parser = Parser::new(dialect)
            .with_options(ParserOptions::new().with_trailing_commas(true))
            .try_with_sql(sql)
            .context(SyntaxSnafu)?;

        Ok(ParserContext { parser, sql })
    }

    /// Parses parser context to Query.
    pub fn parser_query(&mut self) -> Result<Box<Query>> {
        self.parser.parse_query().context(SyntaxSnafu)
    }

    /// Parses SQL with given dialect
    pub fn create_with_dialect(
        sql: &str,
        dialect: &dyn Dialect,
        _opts: ParseOptions,
    ) -> Result<Vec<Statement>> {
        let mut stmts: Vec<Statement> = Vec::new();

        let mut parser_ctx = ParserContext::new(dialect, sql)?;

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

    pub fn parse_table_name(sql: &str, dialect: &dyn Dialect) -> Result<ObjectName> {
        let parser = Parser::new(dialect)
            .with_options(ParserOptions::new().with_trailing_commas(true))
            .try_with_sql(sql)
            .context(SyntaxSnafu)?;
        ParserContext { parser, sql }.intern_parse_table_name()
    }

    pub(crate) fn intern_parse_table_name(&mut self) -> Result<ObjectName> {
        let raw_table_name =
            self.parser
                .parse_object_name(false)
                .context(error::UnexpectedSnafu {
                    expected: "a table name",
                    actual: self.parser.peek_token().to_string(),
                })?;
        Ok(Self::canonicalize_object_name(raw_table_name))
    }

    pub fn parse_function(sql: &str, dialect: &dyn Dialect) -> Result<Expr> {
        let mut parser = Parser::new(dialect)
            .with_options(ParserOptions::new().with_trailing_commas(true))
            .try_with_sql(sql)
            .context(SyntaxSnafu)?;

        let function_name = parser.parse_identifier(false).context(SyntaxSnafu)?;
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

                    Keyword::REPLACE => self.parse_replace(),

                    Keyword::SELECT | Keyword::WITH | Keyword::VALUES => self.parse_query(),

                    Keyword::ALTER => self.parse_alter(),

                    Keyword::DROP => self.parse_drop(),

                    Keyword::COPY => self.parse_copy(),

                    Keyword::TRUNCATE => self.parse_truncate(),

                    Keyword::SET => self.parse_set_variables(),

                    Keyword::ADMIN => self.parse_admin_command(),

                    Keyword::NoKeyword
                        if w.quote_style.is_none() && w.value.to_uppercase() == tql_parser::TQL =>
                    {
                        self.parse_tql()
                    }

                    Keyword::DECLARE => self.parse_declare_cursor(),

                    Keyword::FETCH => self.parse_fetch_cursor(),

                    Keyword::CLOSE => self.parse_close_cursor(),

                    Keyword::USE => {
                        let _ = self.parser.next_token();

                        let database_name = self.parser.parse_identifier(false).context(
                            error::UnexpectedSnafu {
                                expected: "a database name",
                                actual: self.peek_token_as_string(),
                            },
                        )?;
                        Ok(Statement::Use(
                            Self::canonicalize_identifier(database_name).value,
                        ))
                    }

                    // todo(hl) support more statements.
                    _ => self.unsupported(self.peek_token_as_string()),
                }
            }
            Token::LParen => self.parse_query(),
            unexpected => self.unsupported(unexpected.to_string()),
        }
    }

    /// Parses MySQL style 'PREPARE stmt_name FROM stmt' into a (stmt_name, stmt) tuple.
    pub fn parse_mysql_prepare_stmt(sql: &str, dialect: &dyn Dialect) -> Result<(String, String)> {
        ParserContext::new(dialect, sql)?.parse_mysql_prepare()
    }

    /// Parses MySQL style 'EXECUTE stmt_name USING param_list' into a stmt_name string and a list of parameters.
    pub fn parse_mysql_execute_stmt(
        sql: &str,
        dialect: &dyn Dialect,
    ) -> Result<(String, Vec<Expr>)> {
        ParserContext::new(dialect, sql)?.parse_mysql_execute()
    }

    /// Parses MySQL style 'DEALLOCATE stmt_name' into a stmt_name string.
    pub fn parse_mysql_deallocate_stmt(sql: &str, dialect: &dyn Dialect) -> Result<String> {
        ParserContext::new(dialect, sql)?.parse_deallocate()
    }

    /// Raises an "unsupported statement" error.
    pub fn unsupported<T>(&self, keyword: String) -> Result<T> {
        error::UnsupportedSnafu { keyword }.fail()
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

    /// Simply a shortcut for sqlparser's same name method `parse_object_name`,
    /// but with constant argument "false".
    /// Because the argument is always "false" for us (it's introduced by BigQuery),
    /// we don't want to write it again and again.
    pub(crate) fn parse_object_name(&mut self) -> std::result::Result<ObjectName, ParserError> {
        self.parser.parse_object_name(false)
    }

    /// Simply a shortcut for sqlparser's same name method `parse_identifier`,
    /// but with constant argument "false".
    /// Because the argument is always "false" for us (it's introduced by BigQuery),
    /// we don't want to write it again and again.
    pub(crate) fn parse_identifier(parser: &mut Parser) -> std::result::Result<Ident, ParserError> {
        parser.parse_identifier(false)
    }
}

#[cfg(test)]
mod tests {

    use datatypes::prelude::ConcreteDataType;
    use sqlparser::dialect::MySqlDialect;

    use super::*;
    use crate::dialect::GreptimeDbDialect;
    use crate::statements::create::CreateTable;
    use crate::statements::sql_data_type_to_concrete_data_type;

    fn test_timestamp_precision(sql: &str, expected_type: ConcreteDataType) {
        match ParserContext::create_with_dialect(
            sql,
            &GreptimeDbDialect {},
            ParseOptions::default(),
        )
        .unwrap()
        .pop()
        .unwrap()
        {
            Statement::CreateTable(CreateTable { columns, .. }) => {
                let ts_col = columns.first().unwrap();
                assert_eq!(
                    expected_type,
                    sql_data_type_to_concrete_data_type(ts_col.data_type()).unwrap()
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

    #[test]
    pub fn test_parse_table_name() {
        let table_name = "a.b.c";

        let object_name =
            ParserContext::parse_table_name(table_name, &GreptimeDbDialect {}).unwrap();

        assert_eq!(object_name.0.len(), 3);
        assert_eq!(object_name.to_string(), table_name);

        let table_name = "a.b";

        let object_name =
            ParserContext::parse_table_name(table_name, &GreptimeDbDialect {}).unwrap();

        assert_eq!(object_name.0.len(), 2);
        assert_eq!(object_name.to_string(), table_name);

        let table_name = "Test.\"public-test\"";

        let object_name =
            ParserContext::parse_table_name(table_name, &GreptimeDbDialect {}).unwrap();

        assert_eq!(object_name.0.len(), 2);
        assert_eq!(object_name.to_string(), table_name.to_ascii_lowercase());

        let table_name = "HelloWorld";

        let object_name =
            ParserContext::parse_table_name(table_name, &GreptimeDbDialect {}).unwrap();

        assert_eq!(object_name.0.len(), 1);
        assert_eq!(object_name.to_string(), table_name.to_ascii_lowercase());
    }

    #[test]
    pub fn test_parse_mysql_prepare_stmt() {
        let sql = "PREPARE stmt1 FROM 'SELECT * FROM t1 WHERE id = ?';";
        let (stmt_name, stmt) =
            ParserContext::parse_mysql_prepare_stmt(sql, &MySqlDialect {}).unwrap();
        assert_eq!(stmt_name, "stmt1");
        assert_eq!(stmt, "SELECT * FROM t1 WHERE id = ?");

        let sql = "PREPARE stmt2 FROM \"SELECT * FROM t1 WHERE id = ?\"";
        let (stmt_name, stmt) =
            ParserContext::parse_mysql_prepare_stmt(sql, &MySqlDialect {}).unwrap();
        assert_eq!(stmt_name, "stmt2");
        assert_eq!(stmt, "SELECT * FROM t1 WHERE id = ?");
    }

    #[test]
    pub fn test_parse_mysql_execute_stmt() {
        let sql = "EXECUTE stmt1 USING 1, 'hello';";
        let (stmt_name, params) =
            ParserContext::parse_mysql_execute_stmt(sql, &GreptimeDbDialect {}).unwrap();
        assert_eq!(stmt_name, "stmt1");
        assert_eq!(params.len(), 2);
        assert_eq!(params[0].to_string(), "1");
        assert_eq!(params[1].to_string(), "'hello'");

        let sql = "EXECUTE stmt2;";
        let (stmt_name, params) =
            ParserContext::parse_mysql_execute_stmt(sql, &GreptimeDbDialect {}).unwrap();
        assert_eq!(stmt_name, "stmt2");
        assert_eq!(params.len(), 0);

        let sql = "EXECUTE stmt3 USING 231, 'hello', \"2003-03-1\", NULL, ;";
        let (stmt_name, params) =
            ParserContext::parse_mysql_execute_stmt(sql, &GreptimeDbDialect {}).unwrap();
        assert_eq!(stmt_name, "stmt3");
        assert_eq!(params.len(), 4);
        assert_eq!(params[0].to_string(), "231");
        assert_eq!(params[1].to_string(), "'hello'");
        assert_eq!(params[2].to_string(), "\"2003-03-1\"");
        assert_eq!(params[3].to_string(), "NULL");
    }

    #[test]
    pub fn test_parse_mysql_deallocate_stmt() {
        let sql = "DEALLOCATE stmt1;";
        let stmt_name = ParserContext::parse_mysql_deallocate_stmt(sql, &MySqlDialect {}).unwrap();
        assert_eq!(stmt_name, "stmt1");

        let sql = "DEALLOCATE stmt2";
        let stmt_name = ParserContext::parse_mysql_deallocate_stmt(sql, &MySqlDialect {}).unwrap();
        assert_eq!(stmt_name, "stmt2");
    }
}
