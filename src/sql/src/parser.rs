use snafu::{ensure, ResultExt};
use sqlparser::dialect::Dialect;
use sqlparser::keywords::Keyword;
use sqlparser::parser::Parser;
use sqlparser::parser::ParserError;
use sqlparser::tokenizer::{Token, Tokenizer};

use crate::error::{self, InvalidDatabaseNameSnafu, Result, SyntaxSnafu, TokenizerSnafu};
use crate::statements::show::{ShowCreateTable, ShowDatabases, ShowKind, ShowTables};
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
    /// todo(hl) support `show settings`/`show create`/`show users` ect.
    fn parse_show(&mut self) -> Result<Statement> {
        if self.consume_token("DATABASES") || self.consume_token("SCHEMAS") {
            self.parse_show_databases()
        } else if self.matches_keyword(Keyword::TABLES) {
            self.parser.next_token();
            self.parse_show_tables()
        } else if self.matches_keyword(Keyword::CREATE) {
            self.parser.next_token();
            if self.matches_keyword(Keyword::TABLE) {
                self.parser.next_token();
                self.parse_show_create_table()
            } else {
                self.unsupported(self.peek_token_as_string())
            }
        } else {
            self.unsupported(self.peek_token_as_string())
        }
    }

    /// Parser SHOW CREATE TABLE statement
    fn parse_show_create_table(&mut self) -> Result<Statement> {
        let tablename: Option<String> = match self.parser.peek_token() {
            Token::EOF | Token::SemiColon => {
                return Ok(Statement::ShowCreateTable(ShowCreateTable {
                    tablename: None,
                }));
            }
            _ => {
                let tabname =
                    self.parser
                        .parse_object_name()
                        .with_context(|_| error::UnexpectedSnafu {
                            sql: self.sql,
                            expected: "a table name",
                            actual: self.peek_token_as_string(),
                        })?;
                ensure!(
                    tabname.0.len() == 1,
                    InvalidDatabaseNameSnafu {
                        name: tabname.to_string(),
                    }
                );

                Some(tabname.to_string())
            }
        };
        Ok(Statement::ShowCreateTable(ShowCreateTable { tablename }))
    }

    fn parse_show_tables(&mut self) -> Result<Statement> {
        let database = match self.parser.peek_token() {
            Token::EOF | Token::SemiColon => {
                return Ok(Statement::ShowTables(ShowTables {
                    kind: ShowKind::All,
                    database: None,
                }));
            }

            // SHOW TABLES [in | FROM] [DATABSE]
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

    fn parse_explain(&mut self) -> Result<Statement> {
        todo!()
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
    fn peek_token_as_string(&self) -> String {
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
}
