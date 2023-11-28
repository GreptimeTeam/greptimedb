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

use crate::error::{self, InvalidDatabaseNameSnafu, InvalidTableNameSnafu, Result};
use crate::parser::ParserContext;
use crate::statements::show::{ShowCreateTable, ShowDatabases, ShowKind, ShowTables};
use crate::statements::statement::Statement;

/// SHOW statement parser implementation
impl<'a> ParserContext<'a> {
    /// Parses SHOW statements
    /// todo(hl) support `show settings`/`show create`/`show users` etc.
    pub(crate) fn parse_show(&mut self) -> Result<Statement> {
        if self.consume_token("DATABASES") || self.consume_token("SCHEMAS") {
            self.parse_show_databases()
        } else if self.matches_keyword(Keyword::TABLES) {
            let _ = self.parser.next_token();
            self.parse_show_tables(false)
        } else if self.consume_token("CREATE") {
            if self.consume_token("TABLE") {
                self.parse_show_create_table()
            } else {
                self.unsupported(self.peek_token_as_string())
            }
        } else if self.consume_token("FULL") {
            if self.consume_token("TABLES") {
                self.parse_show_tables(true)
            } else {
                self.unsupported(self.peek_token_as_string())
            }
        } else {
            self.unsupported(self.peek_token_as_string())
        }
    }

    /// Parse SHOW CREATE TABLE statement
    fn parse_show_create_table(&mut self) -> Result<Statement> {
        let raw_table_name =
            self.parser
                .parse_object_name()
                .with_context(|_| error::UnexpectedSnafu {
                    sql: self.sql,
                    expected: "a table name",
                    actual: self.peek_token_as_string(),
                })?;
        let table_name = Self::canonicalize_object_name(raw_table_name);
        ensure!(
            !table_name.0.is_empty(),
            InvalidTableNameSnafu {
                name: table_name.to_string(),
            }
        );
        Ok(Statement::ShowCreateTable(ShowCreateTable { table_name }))
    }

    fn parse_show_tables(&mut self, full: bool) -> Result<Statement> {
        let database = match self.parser.peek_token().token {
            Token::EOF | Token::SemiColon => {
                return Ok(Statement::ShowTables(ShowTables {
                    kind: ShowKind::All,
                    database: None,
                    full,
                }));
            }

            // SHOW TABLES [in | FROM] [DATABASE]
            Token::Word(w) => match w.keyword {
                Keyword::IN | Keyword::FROM => {
                    let _ = self.parser.next_token();
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

        let kind = match self.parser.peek_token().token {
            Token::EOF | Token::SemiColon => ShowKind::All,
            // SHOW TABLES [WHERE | LIKE] [EXPR]
            Token::Word(w) => match w.keyword {
                Keyword::LIKE => {
                    let _ = self.parser.next_token();
                    ShowKind::Like(self.parser.parse_identifier().with_context(|_| {
                        error::UnexpectedSnafu {
                            sql: self.sql,
                            expected: "LIKE",
                            actual: self.peek_token_as_string(),
                        }
                    })?)
                }
                Keyword::WHERE => {
                    let _ = self.parser.next_token();
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

        Ok(Statement::ShowTables(ShowTables {
            kind,
            database,
            full,
        }))
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
    use std::assert_matches::assert_matches;

    use super::*;
    use crate::dialect::GreptimeDbDialect;
    use crate::statements::show::ShowDatabases;

    #[test]
    pub fn test_show_database_all() {
        let sql = "SHOW DATABASES";
        let result = ParserContext::create_with_dialect(sql, &GreptimeDbDialect {});
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
        let result = ParserContext::create_with_dialect(sql, &GreptimeDbDialect {});
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
        let result = ParserContext::create_with_dialect(sql, &GreptimeDbDialect {});
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
        let result = ParserContext::create_with_dialect(sql, &GreptimeDbDialect {});
        let stmts = result.unwrap();
        assert_eq!(1, stmts.len());

        assert_matches!(
            &stmts[0],
            Statement::ShowTables(ShowTables {
                kind: ShowKind::All,
                database: None,
                full: false
            })
        );
    }

    #[test]
    pub fn test_show_tables_like() {
        let sql = "SHOW TABLES LIKE test_table";
        let result = ParserContext::create_with_dialect(sql, &GreptimeDbDialect {});
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
                full: false
            })
        );

        let sql = "SHOW TABLES in test_db LIKE test_table";
        let result = ParserContext::create_with_dialect(sql, &GreptimeDbDialect {});
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
                full: false
            })
        );
    }

    #[test]
    pub fn test_show_tables_where() {
        let sql = "SHOW TABLES where name like test_table";
        let result = ParserContext::create_with_dialect(sql, &GreptimeDbDialect {});
        let stmts = result.unwrap();
        assert_eq!(1, stmts.len());

        assert_matches!(
            &stmts[0],
            Statement::ShowTables(ShowTables {
                kind: ShowKind::Where(sqlparser::ast::Expr::Like { .. }),
                database: None,
                full: false
            })
        );

        let sql = "SHOW TABLES in test_db where name LIKE test_table";
        let result = ParserContext::create_with_dialect(sql, &GreptimeDbDialect {});
        let stmts = result.unwrap();
        assert_eq!(1, stmts.len());

        assert_matches!(
            &stmts[0],
            Statement::ShowTables(ShowTables {
                kind: ShowKind::Where(sqlparser::ast::Expr::Like { .. }),
                database: Some(_),
                full: false
            })
        );
    }

    #[test]
    pub fn test_show_full_tables() {
        let sql = "SHOW FULL TABLES";
        let stmts = ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}).unwrap();
        assert_eq!(1, stmts.len());
        assert_matches!(&stmts[0], Statement::ShowTables { .. });
        match &stmts[0] {
            Statement::ShowTables(show) => {
                assert!(show.full);
            }
            _ => {
                unreachable!();
            }
        }
    }

    #[test]
    pub fn test_show_full_tables_where() {
        let sql = "SHOW FULL TABLES IN test_db WHERE Tables LIKE test_table";
        let stmts = ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}).unwrap();
        assert_eq!(1, stmts.len());

        assert_matches!(
            &stmts[0],
            Statement::ShowTables(ShowTables {
                kind: ShowKind::Where(sqlparser::ast::Expr::Like { .. }),
                database: Some(_),
                full: true
            })
        );
    }

    #[test]
    pub fn test_show_full_tables_like() {
        let sql = "SHOW FULL TABLES LIKE test_table";
        let result = ParserContext::create_with_dialect(sql, &GreptimeDbDialect {});
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
                full: true
            })
        );

        let sql = "SHOW FULL TABLES in test_db LIKE test_table";
        let result = ParserContext::create_with_dialect(sql, &GreptimeDbDialect {});
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
                full: true
            })
        );
    }
}
