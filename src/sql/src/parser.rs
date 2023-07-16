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
use sqlparser::dialect::Dialect;
use sqlparser::keywords::Keyword;
use sqlparser::parser::{Parser, ParserError};
use sqlparser::tokenizer::{Token, TokenWithLocation};

use crate::ast::{Expr, ObjectName};
use crate::error::{self, InvalidTableNameSnafu, Result, SyntaxSnafu};
use crate::parsers::tql_parser;
use crate::statements::describe::DescribeTable;
use crate::statements::drop::DropTable;
use crate::statements::explain::Explain;
use crate::statements::show::{ShowDatabases, ShowKind};
use crate::statements::statement::Statement;

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
            .try_with_sql(sql)
            .context(SyntaxSnafu { sql })?;
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

        Ok(stmts)
    }

    pub fn parse_function(sql: &'a str, dialect: &dyn Dialect) -> Result<Expr> {
        let mut parser = Parser::new(dialect)
            .try_with_sql(sql)
            .context(SyntaxSnafu { sql })?;

        let function_name = parser.parse_identifier().context(SyntaxSnafu { sql })?;
        parser
            .parse_function(ObjectName(vec![function_name]))
            .context(SyntaxSnafu { sql })
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

                    Keyword::USE => {
                        let _ = self.parser.next_token();

                        let database_name =
                            self.parser
                                .parse_identifier()
                                .context(error::UnexpectedSnafu {
                                    sql: self.sql,
                                    expected: "a database name",
                                    actual: self.peek_token_as_string(),
                                })?;
                        Ok(Statement::Use(database_name.value))
                    }

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

    // /// Parses SHOW statements
    // /// todo(hl) support `show settings`/`show create`/`show users` etc.
    // fn parse_show(&mut self) -> Result<Statement> {
    //     if self.consume_token("DATABASES") || self.consume_token("SCHEMAS") {
    //         self.parse_show_databases()
    //     } else if self.matches_keyword(Keyword::TABLES) {
    //         let _ = self.parser.next_token();
    //         self.parse_show_tables()
    //     } else if self.consume_token("CREATE") {
    //         if self.consume_token("TABLE") {
    //             self.parse_show_create_table()
    //         } else {
    //             self.unsupported(self.peek_token_as_string())
    //         }
    //     } else {
    //         self.unsupported(self.peek_token_as_string())
    //     }
    // }

    // /// Parse SHOW CREATE TABLE statement
    // fn parse_show_create_table(&mut self) -> Result<Statement> {
    //     let table_name =
    //         self.parser
    //             .parse_object_name()
    //             .with_context(|_| error::UnexpectedSnafu {
    //                 sql: self.sql,
    //                 expected: "a table name",
    //                 actual: self.peek_token_as_string(),
    //             })?;
    //     ensure!(
    //         !table_name.0.is_empty(),
    //         InvalidTableNameSnafu {
    //             name: table_name.to_string(),
    //         }
    //     );
    //     Ok(Statement::ShowCreateTable(ShowCreateTable { table_name }))
    // }

    // fn parse_show_tables(&mut self) -> Result<Statement> {
    //     let database = match self.parser.peek_token().token {
    //         Token::EOF | Token::SemiColon => {
    //             return Ok(Statement::ShowTables(ShowTables {
    //                 kind: ShowKind::All,
    //                 database: None,
    //             }));
    //         }

    //         // SHOW TABLES [in | FROM] [DATABASE]
    //         Token::Word(w) => match w.keyword {
    //             Keyword::IN | Keyword::FROM => {
    //                 let _ = self.parser.next_token();
    //                 let db_name = self.parser.parse_object_name().with_context(|_| {
    //                     error::UnexpectedSnafu {
    //                         sql: self.sql,
    //                         expected: "a database name",
    //                         actual: self.peek_token_as_string(),
    //                     }
    //                 })?;

    //                 ensure!(
    //                     db_name.0.len() == 1,
    //                     InvalidDatabaseNameSnafu {
    //                         name: db_name.to_string(),
    //                     }
    //                 );

    //                 Some(db_name.to_string())
    //             }

    //             _ => None,
    //         },
    //         _ => None,
    //     };

    //     let kind = match self.parser.peek_token().token {
    //         Token::EOF | Token::SemiColon => ShowKind::All,
    //         // SHOW TABLES [WHERE | LIKE] [EXPR]
    //         Token::Word(w) => match w.keyword {
    //             Keyword::LIKE => {
    //                 let _ = self.parser.next_token();
    //                 ShowKind::Like(self.parser.parse_identifier().with_context(|_| {
    //                     error::UnexpectedSnafu {
    //                         sql: self.sql,
    //                         expected: "LIKE",
    //                         actual: self.peek_token_as_string(),
    //                     }
    //                 })?)
    //             }
    //             Keyword::WHERE => {
    //                 let _ = self.parser.next_token();
    //                 ShowKind::Where(self.parser.parse_expr().with_context(|_| {
    //                     error::UnexpectedSnafu {
    //                         sql: self.sql,
    //                         expected: "some valid expression",
    //                         actual: self.peek_token_as_string(),
    //                     }
    //                 })?)
    //             }
    //             _ => return self.unsupported(self.peek_token_as_string()),
    //         },
    //         _ => return self.unsupported(self.peek_token_as_string()),
    //     };

    //     Ok(Statement::ShowTables(ShowTables { kind, database }))
    // }

    /// Parses DESCRIBE statements
    fn parse_describe(&mut self) -> Result<Statement> {
        if self.matches_keyword(Keyword::TABLE) {
            let _ = self.parser.next_token();
        }
        self.parse_describe_table()
    }

    fn parse_describe_table(&mut self) -> Result<Statement> {
        let table_idents =
            self.parser
                .parse_object_name()
                .with_context(|_| error::UnexpectedSnafu {
                    sql: self.sql,
                    expected: "a table name",
                    actual: self.peek_token_as_string(),
                })?;
        ensure!(
            !table_idents.0.is_empty(),
            InvalidTableNameSnafu {
                name: table_idents.to_string(),
            }
        );
        Ok(Statement::DescribeTable(DescribeTable::new(table_idents)))
    }

    fn parse_explain(&mut self) -> Result<Statement> {
        let explain_statement =
            self.parser
                .parse_explain(false)
                .with_context(|_| error::UnexpectedSnafu {
                    sql: self.sql,
                    expected: "a query statement",
                    actual: self.peek_token_as_string(),
                })?;

        Ok(Statement::Explain(Explain::try_from(explain_statement)?))
    }

    fn parse_drop(&mut self) -> Result<Statement> {
        let _ = self.parser.next_token();
        if !self.matches_keyword(Keyword::TABLE) {
            return self.unsupported(self.peek_token_as_string());
        }
        let _ = self.parser.next_token();

        let table_ident =
            self.parser
                .parse_object_name()
                .with_context(|_| error::UnexpectedSnafu {
                    sql: self.sql,
                    expected: "a table name",
                    actual: self.peek_token_as_string(),
                })?;
        ensure!(
            !table_ident.0.is_empty(),
            InvalidTableNameSnafu {
                name: table_ident.to_string()
            }
        );

        Ok(Statement::DropTable(DropTable::new(table_ident)))
    }

    // Report unexpected token
    pub(crate) fn expected<T>(&self, expected: &str, found: TokenWithLocation) -> Result<T> {
        Err(ParserError::ParserError(format!(
            "Expected {expected}, found: {found}",
        )))
        .context(SyntaxSnafu { sql: self.sql })
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

    /// Parses `SHOW DATABASES` statement.
    pub fn parse_show_databases(&mut self) -> Result<Statement> {
        let tok = self.parser.next_token().token;
        match &tok {
            Token::EOF | Token::SemiColon => {
                Ok(Statement::ShowDatabases(ShowDatabases::new(ShowKind::All)))
            }
            Token::Word(w) => match w.keyword {
                Keyword::LIKE => Ok(Statement::ShowDatabases(ShowDatabases::new(
                    ShowKind::Like(self.parser.parse_identifier().with_context(|_| {
                        error::UnexpectedSnafu {
                            sql: self.sql,
                            expected: "LIKE",
                            actual: tok.to_string(),
                        }
                    })?),
                ))),
                Keyword::WHERE => Ok(Statement::ShowDatabases(ShowDatabases::new(
                    ShowKind::Where(self.parser.parse_expr().with_context(|_| {
                        error::UnexpectedSnafu {
                            sql: self.sql,
                            expected: "some valid expression",
                            actual: self.peek_token_as_string(),
                        }
                    })?),
                ))),
                _ => self.unsupported(self.peek_token_as_string()),
            },
            _ => self.unsupported(self.peek_token_as_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    // use std::assert_matches::assert_matches;

    use datatypes::prelude::ConcreteDataType;
    use sqlparser::ast::{
        Ident, ObjectName, Query as SpQuery, Statement as SpStatement, WildcardAdditionalOptions,
    };

    use super::*;
    use crate::dialect::GreptimeDbDialect;
    use crate::statements::create::CreateTable;
    use crate::statements::sql_data_type_to_concrete_data_type;

    // #[test]
    // pub fn test_show_database_all() {
    //     let sql = "SHOW DATABASES";
    //     let result = ParserContext::create_with_dialect(sql, &GreptimeDbDialect {});
    //     let stmts = result.unwrap();
    //     assert_eq!(1, stmts.len());

    //     assert_matches!(
    //         &stmts[0],
    //         Statement::ShowDatabases(ShowDatabases {
    //             kind: ShowKind::All
    //         })
    //     );
    // }

    // #[test]
    // pub fn test_show_database_like() {
    //     let sql = "SHOW DATABASES LIKE test_database";
    //     let result = ParserContext::create_with_dialect(sql, &GreptimeDbDialect {});
    //     let stmts = result.unwrap();
    //     assert_eq!(1, stmts.len());

    //     assert_matches!(
    //         &stmts[0],
    //         Statement::ShowDatabases(ShowDatabases {
    //             kind: ShowKind::Like(sqlparser::ast::Ident {
    //                 value: _,
    //                 quote_style: None,
    //             })
    //         })
    //     );
    // }

    // #[test]
    // pub fn test_show_database_where() {
    //     let sql = "SHOW DATABASES WHERE Database LIKE '%whatever1%' OR Database LIKE '%whatever2%'";
    //     let result = ParserContext::create_with_dialect(sql, &GreptimeDbDialect {});
    //     let stmts = result.unwrap();
    //     assert_eq!(1, stmts.len());

    //     assert_matches!(
    //         &stmts[0],
    //         Statement::ShowDatabases(ShowDatabases {
    //             kind: ShowKind::Where(sqlparser::ast::Expr::BinaryOp {
    //                 left: _,
    //                 right: _,
    //                 op: sqlparser::ast::BinaryOperator::Or,
    //             })
    //         })
    //     );
    // }

    // #[test]
    // pub fn test_show_tables_all() {
    //     let sql = "SHOW TABLES";
    //     let result = ParserContext::create_with_dialect(sql, &GreptimeDbDialect {});
    //     let stmts = result.unwrap();
    //     assert_eq!(1, stmts.len());

    //     assert_matches!(
    //         &stmts[0],
    //         Statement::ShowTables(ShowTables {
    //             kind: ShowKind::All,
    //             database: None,
    //         })
    //     );
    // }

    // #[test]
    // pub fn test_show_tables_like() {
    //     let sql = "SHOW TABLES LIKE test_table";
    //     let result = ParserContext::create_with_dialect(sql, &GreptimeDbDialect {});
    //     let stmts = result.unwrap();
    //     assert_eq!(1, stmts.len());

    //     assert_matches!(
    //         &stmts[0],
    //         Statement::ShowTables(ShowTables {
    //             kind: ShowKind::Like(sqlparser::ast::Ident {
    //                 value: _,
    //                 quote_style: None,
    //             }),
    //             database: None,
    //         })
    //     );

    //     let sql = "SHOW TABLES in test_db LIKE test_table";
    //     let result = ParserContext::create_with_dialect(sql, &GreptimeDbDialect {});
    //     let stmts = result.unwrap();
    //     assert_eq!(1, stmts.len());

    //     assert_matches!(
    //         &stmts[0],
    //         Statement::ShowTables(ShowTables {
    //             kind: ShowKind::Like(sqlparser::ast::Ident {
    //                 value: _,
    //                 quote_style: None,
    //             }),
    //             database: Some(_),
    //         })
    //     );
    // }

    // #[test]
    // pub fn test_show_tables_where() {
    //     let sql = "SHOW TABLES where name like test_table";
    //     let result = ParserContext::create_with_dialect(sql, &GreptimeDbDialect {});
    //     let stmts = result.unwrap();
    //     assert_eq!(1, stmts.len());

    //     assert_matches!(
    //         &stmts[0],
    //         Statement::ShowTables(ShowTables {
    //             kind: ShowKind::Where(sqlparser::ast::Expr::Like { .. }),
    //             database: None,
    //         })
    //     );

    //     let sql = "SHOW TABLES in test_db where name LIKE test_table";
    //     let result = ParserContext::create_with_dialect(sql, &GreptimeDbDialect {});
    //     let stmts = result.unwrap();
    //     assert_eq!(1, stmts.len());

    //     assert_matches!(
    //         &stmts[0],
    //         Statement::ShowTables(ShowTables {
    //             kind: ShowKind::Where(sqlparser::ast::Expr::Like { .. }),
    //             database: Some(_),
    //         })
    //     );
    // }

    #[test]
    pub fn test_explain() {
        let sql = "EXPLAIN select * from foo";
        let result = ParserContext::create_with_dialect(sql, &GreptimeDbDialect {});
        let stmts = result.unwrap();
        assert_eq!(1, stmts.len());

        let select = sqlparser::ast::Select {
            distinct: None,
            top: None,
            projection: vec![sqlparser::ast::SelectItem::Wildcard(
                WildcardAdditionalOptions::default(),
            )],
            into: None,
            from: vec![sqlparser::ast::TableWithJoins {
                relation: sqlparser::ast::TableFactor::Table {
                    name: sqlparser::ast::ObjectName(vec![sqlparser::ast::Ident::new("foo")]),
                    alias: None,
                    args: None,
                    with_hints: vec![],
                },
                joins: vec![],
            }],
            lateral_views: vec![],
            selection: None,
            group_by: vec![],
            cluster_by: vec![],
            distribute_by: vec![],
            sort_by: vec![],
            having: None,
            qualify: None,
            named_window: vec![],
        };

        let sp_statement = SpStatement::Query(Box::new(SpQuery {
            with: None,
            body: Box::new(sqlparser::ast::SetExpr::Select(Box::new(select))),
            order_by: vec![],
            limit: None,
            offset: None,
            fetch: None,
            locks: vec![],
        }));

        let explain = Explain::try_from(SpStatement::Explain {
            describe_alias: false,
            analyze: false,
            verbose: false,
            statement: Box::new(sp_statement),
            format: None,
        })
        .unwrap();

        assert_eq!(stmts[0], Statement::Explain(explain))
    }

    #[test]
    pub fn test_drop_table() {
        let sql = "DROP TABLE foo";
        let result = ParserContext::create_with_dialect(sql, &GreptimeDbDialect {});
        let mut stmts = result.unwrap();
        assert_eq!(
            stmts.pop().unwrap(),
            Statement::DropTable(DropTable::new(ObjectName(vec![Ident::new("foo")])))
        );

        let sql = "DROP TABLE my_schema.foo";
        let result = ParserContext::create_with_dialect(sql, &GreptimeDbDialect {});
        let mut stmts = result.unwrap();
        assert_eq!(
            stmts.pop().unwrap(),
            Statement::DropTable(DropTable::new(ObjectName(vec![
                Ident::new("my_schema"),
                Ident::new("foo")
            ])))
        );

        let sql = "DROP TABLE my_catalog.my_schema.foo";
        let result = ParserContext::create_with_dialect(sql, &GreptimeDbDialect {});
        let mut stmts = result.unwrap();
        assert_eq!(
            stmts.pop().unwrap(),
            Statement::DropTable(DropTable::new(ObjectName(vec![
                Ident::new("my_catalog"),
                Ident::new("my_schema"),
                Ident::new("foo")
            ])))
        )
    }

    fn test_timestamp_precision(sql: &str, expected_type: ConcreteDataType) {
        match ParserContext::create_with_dialect(sql, &GreptimeDbDialect {})
            .unwrap()
            .pop()
            .unwrap()
        {
            Statement::CreateTable(CreateTable { columns, .. }) => {
                let ts_col = columns.get(0).unwrap();
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

    #[test]
    fn test_parse_function() {
        let expr =
            ParserContext::parse_function("current_timestamp()", &GreptimeDbDialect {}).unwrap();
        assert!(matches!(expr, Expr::Function(_)));
    }

    fn assert_describe_table(sql: &str) {
        let stmt = ParserContext::create_with_dialect(sql, &GreptimeDbDialect {})
            .unwrap()
            .pop()
            .unwrap();
        assert!(matches!(stmt, Statement::DescribeTable(_)))
    }

    #[test]
    fn test_parse_describe_table() {
        assert_describe_table("desc table t;");
        assert_describe_table("describe table t;");
        assert_describe_table("desc t;");
        assert_describe_table("describe t;");
    }
}
