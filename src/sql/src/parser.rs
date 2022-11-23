// Copyright 2022 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
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
use sqlparser::tokenizer::{Token, Tokenizer};

use crate::error::{
    self, InvalidDatabaseNameSnafu, InvalidTableNameSnafu, Result, SyntaxSnafu, TokenizerSnafu,
};
use crate::statements::describe::DescribeTable;
use crate::statements::explain::Explain;
use crate::statements::show::{ShowCreateTable, ShowDatabases, ShowKind, ShowTables};
use crate::statements::statement::Statement;
use crate::statements::table_idents_to_full_name;

/// GrepTime SQL parser context, a simple wrapper for Datafusion SQL parser.
pub struct ParserContext<'a> {
    pub(crate) parser: Parser<'a>,
    pub(crate) sql: &'a str,
}

impl<'a> ParserContext<'a> {
    /// Parses SQL with given dialect
    pub fn create_with_dialect(sql: &'a str, dialect: &dyn Dialect) -> Result<Vec<Statement>> {
        let mut stmts: Vec<Statement> = Vec::new();
        let mut tokenizer = Tokenizer::new(dialect, sql);

        let tokens: Vec<Token> = tokenizer.tokenize().context(TokenizerSnafu { sql })?;

        let mut parser_ctx = ParserContext {
            sql,
            parser: Parser::new(tokens, dialect),
        };

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

    /// Parses parser context to a set of statements.
    pub fn parse_statement(&mut self) -> Result<Statement> {
        match self.parser.peek_token() {
            Token::Word(w) => {
                match w.keyword {
                    Keyword::CREATE => {
                        self.parser.next_token();
                        self.parse_create()
                    }

                    Keyword::EXPLAIN => {
                        self.parser.next_token();
                        self.parse_explain()
                    }

                    Keyword::SHOW => {
                        self.parser.next_token();
                        self.parse_show()
                    }

                    Keyword::DESCRIBE | Keyword::DESC => {
                        self.parser.next_token();
                        self.parse_describe()
                    }

                    Keyword::INSERT => self.parse_insert(),

                    Keyword::SELECT | Keyword::WITH | Keyword::VALUES => self.parse_query(),

                    Keyword::ALTER => self.parse_alter(),

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

    /// Parses SHOW statements
    /// todo(hl) support `show settings`/`show create`/`show users` etc.
    fn parse_show(&mut self) -> Result<Statement> {
        if self.consume_token("DATABASES") || self.consume_token("SCHEMAS") {
            self.parse_show_databases()
        } else if self.matches_keyword(Keyword::TABLES) {
            self.parser.next_token();
            self.parse_show_tables()
        } else if self.consume_token("CREATE") {
            if self.consume_token("TABLE") {
                self.parse_show_create_table()
            } else {
                self.unsupported(self.peek_token_as_string())
            }
        } else {
            self.unsupported(self.peek_token_as_string())
        }
    }

    /// Parse SHOW CREATE TABLE statement
    fn parse_show_create_table(&mut self) -> Result<Statement> {
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
        let (catalog_name, schema_name, table_name) = table_idents_to_full_name(&table_idents)?;

        Ok(Statement::ShowCreateTable(ShowCreateTable {
            schema_name,
            catalog_name,
            table_name,
        }))
    }

    fn parse_show_tables(&mut self) -> Result<Statement> {
        let database = match self.parser.peek_token() {
            Token::EOF | Token::SemiColon => {
                return Ok(Statement::ShowTables(ShowTables {
                    kind: ShowKind::All,
                    database: None,
                }));
            }

            // SHOW TABLES [in | FROM] [DATABASE]
            Token::Word(w) => match w.keyword {
                Keyword::IN | Keyword::FROM => {
                    self.parser.next_token();
                    let db_name = self.parser.parse_object_name().with_context(|_| {
                        error::UnexpectedSnafu {
                            sql: self.sql,
                            expected: "a database name",
                            actual: self.peek_token_as_string(),
                        }
                    })?;

                    ensure!(
                        db_name.0.len() == 1,
                        InvalidDatabaseNameSnafu {
                            name: db_name.to_string(),
                        }
                    );

                    Some(db_name.to_string())
                }

                _ => None,
            },
            _ => None,
        };

        let kind = match self.parser.peek_token() {
            Token::EOF | Token::SemiColon => ShowKind::All,
            // SHOW TABLES [WHERE | LIKE] [EXPR]
            Token::Word(w) => match w.keyword {
                Keyword::LIKE => {
                    self.parser.next_token();
                    ShowKind::Like(self.parser.parse_identifier().with_context(|_| {
                        error::UnexpectedSnafu {
                            sql: self.sql,
                            expected: "LIKE",
                            actual: self.peek_token_as_string(),
                        }
                    })?)
                }
                Keyword::WHERE => {
                    self.parser.next_token();
                    ShowKind::Where(self.parser.parse_expr().with_context(|_| {
                        error::UnexpectedSnafu {
                            sql: self.sql,
                            expected: "some valid expression",
                            actual: self.peek_token_as_string(),
                        }
                    })?)
                }
                _ => return self.unsupported(self.peek_token_as_string()),
            },
            _ => return self.unsupported(self.peek_token_as_string()),
        };

        Ok(Statement::ShowTables(ShowTables { kind, database }))
    }

    /// Parses DESCRIBE statements
    fn parse_describe(&mut self) -> Result<Statement> {
        if self.matches_keyword(Keyword::TABLE) {
            self.parser.next_token();
            self.parse_describe_table()
        } else {
            self.unsupported(self.peek_token_as_string())
        }
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
        let (catalog_name, schema_name, table_name) = table_idents_to_full_name(&table_idents)?;
        Ok(Statement::DescribeTable(DescribeTable {
            catalog_name,
            schema_name,
            table_name,
        }))
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

    // Report unexpected token
    pub(crate) fn expected<T>(&self, expected: &str, found: Token) -> Result<T> {
        Err(ParserError::ParserError(format!(
            "Expected {}, found: {}",
            expected, found
        )))
        .context(SyntaxSnafu { sql: self.sql })
    }

    pub fn matches_keyword(&mut self, expected: Keyword) -> bool {
        match self.parser.peek_token() {
            Token::Word(w) => w.keyword == expected,
            _ => false,
        }
    }

    pub fn consume_token(&mut self, expected: &str) -> bool {
        if self.peek_token_as_string().to_uppercase() == *expected.to_uppercase() {
            self.parser.next_token();
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
        let tok = self.parser.next_token();
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
    use std::assert_matches::assert_matches;

    use sqlparser::ast::{Query as SpQuery, Statement as SpStatement};
    use sqlparser::dialect::GenericDialect;

    use super::*;

    #[test]
    pub fn test_show_database_all() {
        let sql = "SHOW DATABASES";
        let result = ParserContext::create_with_dialect(sql, &GenericDialect {});
        let stmts = result.unwrap();
        assert_eq!(1, stmts.len());

        assert_matches!(
            &stmts[0],
            Statement::ShowDatabases(ShowDatabases {
                kind: ShowKind::All
            })
        );
    }

    #[test]
    pub fn test_show_database_like() {
        let sql = "SHOW DATABASES LIKE test_database";
        let result = ParserContext::create_with_dialect(sql, &GenericDialect {});
        let stmts = result.unwrap();
        assert_eq!(1, stmts.len());

        assert_matches!(
            &stmts[0],
            Statement::ShowDatabases(ShowDatabases {
                kind: ShowKind::Like(sqlparser::ast::Ident {
                    value: _,
                    quote_style: None,
                })
            })
        );
    }

    #[test]
    pub fn test_show_database_where() {
        let sql = "SHOW DATABASES WHERE Database LIKE '%whatever1%' OR Database LIKE '%whatever2%'";
        let result = ParserContext::create_with_dialect(sql, &GenericDialect {});
        let stmts = result.unwrap();
        assert_eq!(1, stmts.len());

        assert_matches!(
            &stmts[0],
            Statement::ShowDatabases(ShowDatabases {
                kind: ShowKind::Where(sqlparser::ast::Expr::BinaryOp {
                    left: _,
                    right: _,
                    op: sqlparser::ast::BinaryOperator::Or,
                })
            })
        );
    }

    #[test]
    pub fn test_show_tables_all() {
        let sql = "SHOW TABLES";
        let result = ParserContext::create_with_dialect(sql, &GenericDialect {});
        let stmts = result.unwrap();
        assert_eq!(1, stmts.len());

        assert_matches!(
            &stmts[0],
            Statement::ShowTables(ShowTables {
                kind: ShowKind::All,
                database: None,
            })
        );
    }

    #[test]
    pub fn test_show_tables_like() {
        let sql = "SHOW TABLES LIKE test_table";
        let result = ParserContext::create_with_dialect(sql, &GenericDialect {});
        let stmts = result.unwrap();
        assert_eq!(1, stmts.len());

        assert_matches!(
            &stmts[0],
            Statement::ShowTables(ShowTables {
                kind: ShowKind::Like(sqlparser::ast::Ident {
                    value: _,
                    quote_style: None,
                }),
                database: None,
            })
        );

        let sql = "SHOW TABLES in test_db LIKE test_table";
        let result = ParserContext::create_with_dialect(sql, &GenericDialect {});
        let stmts = result.unwrap();
        assert_eq!(1, stmts.len());

        assert_matches!(
            &stmts[0],
            Statement::ShowTables(ShowTables {
                kind: ShowKind::Like(sqlparser::ast::Ident {
                    value: _,
                    quote_style: None,
                }),
                database: Some(_),
            })
        );
    }

    #[test]
    pub fn test_show_tables_where() {
        let sql = "SHOW TABLES where name like test_table";
        let result = ParserContext::create_with_dialect(sql, &GenericDialect {});
        let stmts = result.unwrap();
        assert_eq!(1, stmts.len());

        assert_matches!(
            &stmts[0],
            Statement::ShowTables(ShowTables {
                kind: ShowKind::Where(sqlparser::ast::Expr::BinaryOp {
                    left: _,
                    right: _,
                    op: sqlparser::ast::BinaryOperator::Like,
                }),
                database: None,
            })
        );

        let sql = "SHOW TABLES in test_db where name LIKE test_table";
        let result = ParserContext::create_with_dialect(sql, &GenericDialect {});
        let stmts = result.unwrap();
        assert_eq!(1, stmts.len());

        assert_matches!(
            &stmts[0],
            Statement::ShowTables(ShowTables {
                kind: ShowKind::Where(sqlparser::ast::Expr::BinaryOp {
                    left: _,
                    right: _,
                    op: sqlparser::ast::BinaryOperator::Like,
                }),
                database: Some(_),
            })
        );
    }

    #[test]
    pub fn test_explain() {
        let sql = "EXPLAIN select * from foo";
        let result = ParserContext::create_with_dialect(sql, &GenericDialect {});
        let stmts = result.unwrap();
        assert_eq!(1, stmts.len());

        let select = sqlparser::ast::Select {
            distinct: false,
            top: None,
            projection: vec![sqlparser::ast::SelectItem::Wildcard],
            from: vec![sqlparser::ast::TableWithJoins {
                relation: sqlparser::ast::TableFactor::Table {
                    name: sqlparser::ast::ObjectName(vec![sqlparser::ast::Ident::new("foo")]),
                    alias: None,
                    args: vec![],
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
        };

        let sp_statement = SpStatement::Query(Box::new(SpQuery {
            with: None,
            body: sqlparser::ast::SetExpr::Select(Box::new(select)),
            order_by: vec![],
            limit: None,
            offset: None,
            fetch: None,
            lock: None,
        }));

        let explain = Explain::try_from(SpStatement::Explain {
            describe_alias: false,
            analyze: false,
            verbose: false,
            statement: Box::new(sp_statement),
        })
        .unwrap();

        assert_eq!(stmts[0], Statement::Explain(explain))
    }
}
