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

#[cfg(feature = "enterprise")]
pub mod trigger;

use snafu::{ensure, ResultExt};
use sqlparser::keywords::Keyword;
use sqlparser::tokenizer::Token;

use crate::ast::ObjectNamePartExt;
use crate::error::{
    self, InvalidDatabaseNameSnafu, InvalidFlowNameSnafu, InvalidTableNameSnafu, Result,
};
use crate::parser::ParserContext;
use crate::statements::show::{
    ShowColumns, ShowCreateDatabase, ShowCreateFlow, ShowCreateTable, ShowCreateTableVariant,
    ShowCreateView, ShowDatabases, ShowFlows, ShowIndex, ShowKind, ShowProcessList, ShowRegion,
    ShowSearchPath, ShowStatus, ShowTableStatus, ShowTables, ShowVariables, ShowViews,
};
use crate::statements::statement::Statement;

/// SHOW statement parser implementation
impl ParserContext<'_> {
    /// Parses SHOW statements
    /// todo(hl) support `show settings`/`show create`/`show users` etc.
    pub(crate) fn parse_show(&mut self) -> Result<Statement> {
        #[cfg(feature = "enterprise")]
        if self.consume_token("TRIGGERS") {
            return self.parse_show_triggers();
        }
        if self.consume_token("DATABASES") || self.consume_token("SCHEMAS") {
            self.parse_show_databases(false)
        } else if self.matches_keyword(Keyword::TABLES) {
            self.parser.next_token();
            self.parse_show_tables(false)
        } else if self.matches_keyword(Keyword::TABLE) {
            self.parser.next_token();
            if self.matches_keyword(Keyword::STATUS) {
                self.parser.next_token();
                self.parse_show_table_status()
            } else {
                self.unsupported(self.peek_token_as_string())
            }
        } else if self.consume_token("VIEWS") {
            self.parse_show_views()
        } else if self.consume_token("FLOWS") {
            self.parse_show_flows()
        } else if self.matches_keyword(Keyword::CHARSET) {
            self.parser.next_token();
            Ok(Statement::ShowCharset(self.parse_show_kind()?))
        } else if self.matches_keyword(Keyword::CHARACTER) {
            self.parser.next_token();

            if self.matches_keyword(Keyword::SET) {
                self.parser.next_token();
                Ok(Statement::ShowCharset(self.parse_show_kind()?))
            } else {
                self.unsupported(self.peek_token_as_string())
            }
        } else if self.matches_keyword(Keyword::COLLATION) {
            self.parser.next_token();
            Ok(Statement::ShowCollation(self.parse_show_kind()?))
        } else if self.matches_keyword(Keyword::COLUMNS) || self.matches_keyword(Keyword::FIELDS) {
            // SHOW {COLUMNS | FIELDS}
            self.parser.next_token();
            self.parse_show_columns(false)
        } else if self.consume_token("INDEX")
            || self.consume_token("INDEXES")
            || self.consume_token("KEYS")
        {
            // SHOW {INDEX | INDEXES | KEYS}
            self.parse_show_index()
        } else if self.consume_token("REGIONS") || self.consume_token("REGION") {
            // SHOW REGIONS
            self.parse_show_regions()
        } else if self.consume_token("CREATE") {
            if self.consume_token("DATABASE") || self.consume_token("SCHEMA") {
                self.parse_show_create_database()
            } else if self.consume_token("TABLE") {
                self.parse_show_create_table()
            } else if self.consume_token("FLOW") {
                self.parse_show_create_flow()
            } else if self.consume_token("VIEW") {
                self.parse_show_create_view()
            } else {
                self.unsupported(self.peek_token_as_string())
            }
        } else if self.consume_token("FULL") {
            if self.consume_token("TABLES") {
                self.parse_show_tables(true)
            } else if self.consume_token("COLUMNS") || self.consume_token("FIELDS") {
                // SHOW {COLUMNS | FIELDS}
                self.parse_show_columns(true)
            } else if self.consume_token("DATABASES") || self.consume_token("SCHEMAS") {
                self.parse_show_databases(true)
            } else if self.consume_token("PROCESSLIST") {
                self.parse_show_processlist(true)
            } else {
                self.unsupported(self.peek_token_as_string())
            }
        } else if self.consume_token("VARIABLES") {
            let variable = self
                .parse_object_name()
                .with_context(|_| error::UnexpectedSnafu {
                    expected: "a variable name",
                    actual: self.peek_token_as_string(),
                })?;
            Ok(Statement::ShowVariables(ShowVariables { variable }))
        } else if self.consume_token("STATUS") {
            Ok(Statement::ShowStatus(ShowStatus {}))
        } else if self.consume_token("SEARCH_PATH") {
            Ok(Statement::ShowSearchPath(ShowSearchPath {}))
        } else if self.consume_token("PROCESSLIST") {
            self.parse_show_processlist(false)
        } else {
            self.unsupported(self.peek_token_as_string())
        }
    }

    fn parse_show_create_database(&mut self) -> Result<Statement> {
        let raw_database_name =
            self.parse_object_name()
                .with_context(|_| error::UnexpectedSnafu {
                    expected: "a database name",
                    actual: self.peek_token_as_string(),
                })?;
        let database_name = Self::canonicalize_object_name(raw_database_name);
        ensure!(
            !database_name.0.is_empty(),
            InvalidDatabaseNameSnafu {
                name: database_name.to_string(),
            }
        );
        Ok(Statement::ShowCreateDatabase(ShowCreateDatabase {
            database_name,
        }))
    }

    /// Parse SHOW CREATE TABLE statement
    fn parse_show_create_table(&mut self) -> Result<Statement> {
        let raw_table_name = self
            .parse_object_name()
            .with_context(|_| error::UnexpectedSnafu {
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
        let mut variant = ShowCreateTableVariant::Original;
        if self.consume_token("FOR") {
            if self.consume_token("POSTGRES_FOREIGN_TABLE") {
                variant = ShowCreateTableVariant::PostgresForeignTable;
            } else {
                self.unsupported(self.peek_token_as_string())?;
            }
        }

        Ok(Statement::ShowCreateTable(ShowCreateTable {
            table_name,
            variant,
        }))
    }

    fn parse_show_create_flow(&mut self) -> Result<Statement> {
        let raw_flow_name = self
            .parse_object_name()
            .with_context(|_| error::UnexpectedSnafu {
                expected: "a flow name",
                actual: self.peek_token_as_string(),
            })?;
        let flow_name = Self::canonicalize_object_name(raw_flow_name);
        ensure!(
            !flow_name.0.is_empty(),
            InvalidFlowNameSnafu {
                name: flow_name.to_string(),
            }
        );
        Ok(Statement::ShowCreateFlow(ShowCreateFlow { flow_name }))
    }

    fn parse_show_create_view(&mut self) -> Result<Statement> {
        let raw_view_name = self
            .parse_object_name()
            .with_context(|_| error::UnexpectedSnafu {
                expected: "a view name",
                actual: self.peek_token_as_string(),
            })?;
        let view_name = Self::canonicalize_object_name(raw_view_name);
        ensure!(
            !view_name.0.is_empty(),
            InvalidTableNameSnafu {
                name: view_name.to_string(),
            }
        );
        Ok(Statement::ShowCreateView(ShowCreateView { view_name }))
    }

    fn parse_show_table_name(&mut self) -> Result<String> {
        self.parser.next_token();
        let table_name = self
            .parse_object_name()
            .with_context(|_| error::UnexpectedSnafu {
                expected: "a table name",
                actual: self.peek_token_as_string(),
            })?;

        ensure!(
            table_name.0.len() == 1,
            InvalidDatabaseNameSnafu {
                name: table_name.to_string(),
            }
        );

        // Safety: already checked above
        Ok(Self::canonicalize_object_name(table_name).0[0].to_string_unquoted())
    }

    fn parse_db_name(&mut self) -> Result<Option<String>> {
        self.parser.next_token();
        let db_name = self
            .parse_object_name()
            .with_context(|_| error::UnexpectedSnafu {
                expected: "a database name",
                actual: self.peek_token_as_string(),
            })?;

        ensure!(
            db_name.0.len() == 1,
            InvalidDatabaseNameSnafu {
                name: db_name.to_string(),
            }
        );

        // Safety: already checked above
        Ok(Some(
            Self::canonicalize_object_name(db_name).0[0].to_string_unquoted(),
        ))
    }

    fn parse_show_columns(&mut self, full: bool) -> Result<Statement> {
        let table = match self.parser.peek_token().token {
            // SHOW columns {in | FROM} TABLE
            Token::Word(w) if matches!(w.keyword, Keyword::IN | Keyword::FROM) => {
                self.parse_show_table_name()?
            }
            _ => {
                return error::UnexpectedTokenSnafu {
                    expected: "{FROM | IN} table",
                    actual: self.peek_token_as_string(),
                }
                .fail();
            }
        };

        let database = match self.parser.peek_token().token {
            Token::EOF | Token::SemiColon => {
                return Ok(Statement::ShowColumns(ShowColumns {
                    kind: ShowKind::All,
                    table,
                    database: None,
                    full,
                }));
            }

            // SHOW columns {In | FROM} TABLE {In | FROM} DATABASE
            Token::Word(w) => match w.keyword {
                Keyword::IN | Keyword::FROM => self.parse_db_name()?,

                _ => None,
            },
            _ => None,
        };

        let kind = self.parse_show_kind()?;

        Ok(Statement::ShowColumns(ShowColumns {
            kind,
            database,
            table,
            full,
        }))
    }

    fn parse_show_kind(&mut self) -> Result<ShowKind> {
        match self.parser.peek_token().token {
            Token::EOF | Token::SemiColon => Ok(ShowKind::All),
            Token::Word(w) => match w.keyword {
                Keyword::LIKE => {
                    self.parser.next_token();
                    Ok(ShowKind::Like(
                        self.parser.parse_identifier().with_context(|_| {
                            error::UnexpectedSnafu {
                                expected: "LIKE",
                                actual: self.peek_token_as_string(),
                            }
                        })?,
                    ))
                }
                Keyword::WHERE => {
                    self.parser.next_token();
                    Ok(ShowKind::Where(self.parser.parse_expr().with_context(
                        |_| error::UnexpectedSnafu {
                            expected: "some valid expression",
                            actual: self.peek_token_as_string(),
                        },
                    )?))
                }
                _ => self.unsupported(self.peek_token_as_string()),
            },
            _ => self.unsupported(self.peek_token_as_string()),
        }
    }

    fn parse_show_index(&mut self) -> Result<Statement> {
        let table = match self.parser.peek_token().token {
            // SHOW INDEX {in | FROM} TABLE
            Token::Word(w) if matches!(w.keyword, Keyword::IN | Keyword::FROM) => {
                self.parse_show_table_name()?
            }
            _ => {
                return error::UnexpectedTokenSnafu {
                    expected: "{FROM | IN} table",
                    actual: self.peek_token_as_string(),
                }
                .fail();
            }
        };

        let database = match self.parser.peek_token().token {
            Token::EOF | Token::SemiColon => {
                return Ok(Statement::ShowIndex(ShowIndex {
                    kind: ShowKind::All,
                    table,
                    database: None,
                }));
            }

            // SHOW INDEX {In | FROM} TABLE {In | FROM} DATABASE
            Token::Word(w) => match w.keyword {
                Keyword::IN | Keyword::FROM => self.parse_db_name()?,

                _ => None,
            },
            _ => None,
        };

        let kind = match self.parser.peek_token().token {
            Token::EOF | Token::SemiColon => ShowKind::All,
            // SHOW INDEX [WHERE] [EXPR]
            Token::Word(w) => match w.keyword {
                Keyword::WHERE => {
                    self.parser.next_token();
                    ShowKind::Where(self.parser.parse_expr().with_context(|_| {
                        error::UnexpectedSnafu {
                            expected: "some valid expression",
                            actual: self.peek_token_as_string(),
                        }
                    })?)
                }
                _ => return self.unsupported(self.peek_token_as_string()),
            },
            _ => return self.unsupported(self.peek_token_as_string()),
        };

        Ok(Statement::ShowIndex(ShowIndex {
            kind,
            database,
            table,
        }))
    }

    fn parse_show_regions(&mut self) -> Result<Statement> {
        let table = match self.parser.peek_token().token {
            // SHOW REGION {in | FROM} TABLE
            Token::Word(w) if matches!(w.keyword, Keyword::IN | Keyword::FROM) => {
                self.parse_show_table_name()?
            }
            _ => {
                return error::UnexpectedTokenSnafu {
                    expected: "{FROM | IN} table",
                    actual: self.peek_token_as_string(),
                }
                .fail();
            }
        };

        let database = match self.parser.peek_token().token {
            Token::EOF | Token::SemiColon => {
                return Ok(Statement::ShowRegion(ShowRegion {
                    kind: ShowKind::All,
                    table,
                    database: None,
                }));
            }

            // SHOW REGION {In | FROM} TABLE {In | FROM} DATABASE
            Token::Word(w) => match w.keyword {
                Keyword::IN | Keyword::FROM => self.parse_db_name()?,

                _ => None,
            },
            _ => None,
        };

        let kind = match self.parser.peek_token().token {
            Token::EOF | Token::SemiColon => ShowKind::All,
            // SHOW REGION [WHERE] [EXPR]
            Token::Word(w) => match w.keyword {
                Keyword::WHERE => {
                    self.parser.next_token();
                    ShowKind::Where(self.parser.parse_expr().with_context(|_| {
                        error::UnexpectedSnafu {
                            expected: "some valid expression",
                            actual: self.peek_token_as_string(),
                        }
                    })?)
                }
                _ => return self.unsupported(self.peek_token_as_string()),
            },
            _ => return self.unsupported(self.peek_token_as_string()),
        };

        Ok(Statement::ShowRegion(ShowRegion {
            kind,
            database,
            table,
        }))
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
                Keyword::IN | Keyword::FROM => self.parse_db_name()?,

                _ => None,
            },
            _ => None,
        };

        let kind = self.parse_show_kind()?;

        Ok(Statement::ShowTables(ShowTables {
            kind,
            database,
            full,
        }))
    }

    fn parse_show_table_status(&mut self) -> Result<Statement> {
        let database = match self.parser.peek_token().token {
            Token::EOF | Token::SemiColon => {
                return Ok(Statement::ShowTableStatus(ShowTableStatus {
                    kind: ShowKind::All,
                    database: None,
                }));
            }

            // SHOW TABLE STATUS [in | FROM] [DATABASE]
            Token::Word(w) => match w.keyword {
                Keyword::IN | Keyword::FROM => self.parse_db_name()?,

                _ => None,
            },
            _ => None,
        };

        let kind = self.parse_show_kind()?;

        Ok(Statement::ShowTableStatus(ShowTableStatus {
            kind,
            database,
        }))
    }

    /// Parses `SHOW DATABASES` statement.
    pub fn parse_show_databases(&mut self, full: bool) -> Result<Statement> {
        let tok = self.parser.next_token().token;
        match &tok {
            Token::EOF | Token::SemiColon => Ok(Statement::ShowDatabases(ShowDatabases::new(
                ShowKind::All,
                full,
            ))),
            Token::Word(w) => match w.keyword {
                Keyword::LIKE => Ok(Statement::ShowDatabases(ShowDatabases::new(
                    ShowKind::Like(self.parser.parse_identifier().with_context(|_| {
                        error::UnexpectedSnafu {
                            expected: "LIKE",
                            actual: tok.to_string(),
                        }
                    })?),
                    full,
                ))),
                Keyword::WHERE => Ok(Statement::ShowDatabases(ShowDatabases::new(
                    ShowKind::Where(self.parser.parse_expr().with_context(|_| {
                        error::UnexpectedSnafu {
                            expected: "some valid expression",
                            actual: self.peek_token_as_string(),
                        }
                    })?),
                    full,
                ))),
                _ => self.unsupported(self.peek_token_as_string()),
            },
            _ => self.unsupported(self.peek_token_as_string()),
        }
    }

    fn parse_show_views(&mut self) -> Result<Statement> {
        let database = match self.parser.peek_token().token {
            Token::EOF | Token::SemiColon => {
                return Ok(Statement::ShowViews(ShowViews {
                    kind: ShowKind::All,
                    database: None,
                }));
            }

            // SHOW VIEWS [in | FROM] [DATABASE]
            Token::Word(w) => match w.keyword {
                Keyword::IN | Keyword::FROM => self.parse_db_name()?,
                _ => None,
            },
            _ => None,
        };

        let kind = self.parse_show_kind()?;

        Ok(Statement::ShowViews(ShowViews { kind, database }))
    }

    fn parse_show_flows(&mut self) -> Result<Statement> {
        let database = match self.parser.peek_token().token {
            Token::EOF | Token::SemiColon => {
                return Ok(Statement::ShowFlows(ShowFlows {
                    kind: ShowKind::All,
                    database: None,
                }));
            }

            // SHOW FLOWS [in | FROM] [DATABASE]
            Token::Word(w) => match w.keyword {
                Keyword::IN | Keyword::FROM => self.parse_db_name()?,
                _ => None,
            },
            _ => None,
        };

        let kind = self.parse_show_kind()?;

        Ok(Statement::ShowFlows(ShowFlows { kind, database }))
    }

    fn parse_show_processlist(&mut self, full: bool) -> Result<Statement> {
        match self.parser.next_token().token {
            Token::EOF | Token::SemiColon => {
                Ok(Statement::ShowProcesslist(ShowProcessList { full }))
            }
            _ => self.unsupported(self.peek_token_as_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;

    use sqlparser::ast::{Ident, ObjectName};

    use super::*;
    use crate::dialect::GreptimeDbDialect;
    use crate::parser::ParseOptions;
    use crate::statements::show::ShowDatabases;

    #[test]
    pub fn test_show_database_all() {
        let sql = "SHOW DATABASES";
        let result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default());
        let stmts = result.unwrap();
        assert_eq!(1, stmts.len());

        assert_matches!(
            &stmts[0],
            Statement::ShowDatabases(ShowDatabases {
                kind: ShowKind::All,
                full: false,
            })
        );
    }

    #[test]
    pub fn test_show_full_databases() {
        let sql = "SHOW FULL DATABASES";
        let result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default());
        let stmts = result.unwrap();
        assert_eq!(1, stmts.len());

        assert_matches!(
            &stmts[0],
            Statement::ShowDatabases(ShowDatabases {
                kind: ShowKind::All,
                full: true,
            })
        );

        let sql = "SHOW FULL DATABASES LIKE 'test%'";
        let result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default());
        let stmts = result.unwrap();
        assert_eq!(1, stmts.len());

        assert_matches!(
            &stmts[0],
            Statement::ShowDatabases(ShowDatabases {
                kind: ShowKind::Like(_),
                full: true,
            })
        );
    }

    #[test]
    pub fn test_show_database_like() {
        let sql = "SHOW DATABASES LIKE test_database";
        let result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default());
        let stmts = result.unwrap();
        assert_eq!(1, stmts.len());

        assert_matches!(
            &stmts[0],
            Statement::ShowDatabases(ShowDatabases {
                kind: ShowKind::Like(sqlparser::ast::Ident {
                    value: _,
                    quote_style: None,
                    span: _,
                }),
                ..
            })
        );
    }

    #[test]
    pub fn test_show_database_where() {
        let sql = "SHOW DATABASES WHERE Database LIKE '%whatever1%' OR Database LIKE '%whatever2%'";
        let result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default());
        let stmts = result.unwrap();
        assert_eq!(1, stmts.len());

        assert_matches!(
            &stmts[0],
            Statement::ShowDatabases(ShowDatabases {
                kind: ShowKind::Where(sqlparser::ast::Expr::BinaryOp {
                    left: _,
                    right: _,
                    op: sqlparser::ast::BinaryOperator::Or,
                }),
                ..
            })
        );
    }

    #[test]
    pub fn test_show_tables_all() {
        let sql = "SHOW TABLES";
        let result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default());
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
        let result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default());
        let stmts = result.unwrap();
        assert_eq!(1, stmts.len());

        assert_matches!(
            &stmts[0],
            Statement::ShowTables(ShowTables {
                kind: ShowKind::Like(sqlparser::ast::Ident {
                    value: _,
                    quote_style: None,
                    span: _,
                }),
                database: None,
                full: false
            })
        );

        let sql = "SHOW TABLES in test_db LIKE test_table";
        let result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default());
        let stmts = result.unwrap();
        assert_eq!(1, stmts.len());

        assert_matches!(
            &stmts[0],
            Statement::ShowTables(ShowTables {
                kind: ShowKind::Like(sqlparser::ast::Ident {
                    value: _,
                    quote_style: None,
                    span: _,
                }),
                database: Some(_),
                full: false
            })
        );
    }

    #[test]
    pub fn test_show_tables_where() {
        let sql = "SHOW TABLES where name like test_table";
        let result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default());
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
        let result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default());
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
        let stmts =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();
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
        let stmts =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();
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
        let result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default());
        let stmts = result.unwrap();
        assert_eq!(1, stmts.len());

        assert_matches!(
            &stmts[0],
            Statement::ShowTables(ShowTables {
                kind: ShowKind::Like(sqlparser::ast::Ident {
                    value: _,
                    quote_style: None,
                    span: _,
                }),
                database: None,
                full: true
            })
        );

        let sql = "SHOW FULL TABLES in test_db LIKE test_table";
        let result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default());
        let stmts = result.unwrap();
        assert_eq!(1, stmts.len());

        assert_matches!(
            &stmts[0],
            Statement::ShowTables(ShowTables {
                kind: ShowKind::Like(sqlparser::ast::Ident {
                    value: _,
                    quote_style: None,
                    span: _,
                }),
                database: Some(_),
                full: true
            })
        );
    }

    #[test]
    pub fn test_show_variables() {
        let sql = "SHOW VARIABLES system_time_zone";
        let result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default());
        let stmts = result.unwrap();
        assert_eq!(1, stmts.len());
        assert_eq!(
            stmts[0],
            Statement::ShowVariables(ShowVariables {
                variable: ObjectName::from(vec![Ident::new("system_time_zone")]),
            })
        );
    }

    #[test]
    pub fn test_show_columns() {
        let sql = "SHOW COLUMNS";
        let result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default());
        let error = result.unwrap_err();
        assert_eq!("Unexpected token while parsing SQL statement, expected: '{FROM | IN} table', found: EOF", error.to_string());

        let sql = "SHOW COLUMNS from test";
        let result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default());
        let stmts = result.unwrap();
        assert_eq!(1, stmts.len());
        assert!(matches!(&stmts[0],
                         Statement::ShowColumns(ShowColumns {
                             table,
                             database,
                             full,
                             ..

                         }) if table == "test" && database.is_none() && !full));

        let sql = "SHOW FULL COLUMNS from test from public";
        let result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default());
        let stmts = result.unwrap();
        assert_eq!(1, stmts.len());
        assert!(matches!(&stmts[0],
                         Statement::ShowColumns(ShowColumns {
                             table,
                             database: Some(database),
                             full,
                             ..
                         }) if table == "test" && database == "public" && *full));

        let sql = "SHOW COLUMNS from test like 'disk%'";
        let result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default());
        let stmts = result.unwrap();
        assert_eq!(1, stmts.len());
        assert!(matches!(&stmts[0],
                         Statement::ShowColumns(ShowColumns {
                             table,
                             kind: ShowKind::Like(ident),
                             ..
                         }) if table == "test" && ident.to_string() == "'disk%'"));

        let sql = "SHOW COLUMNS from test where Field = 'disk'";
        let result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default());
        let stmts = result.unwrap();
        assert_eq!(1, stmts.len());
        assert!(matches!(&stmts[0],
                         Statement::ShowColumns(ShowColumns {
                             table,
                             kind: ShowKind::Where(expr),
                             ..
                         }) if table == "test" && expr.to_string() == "Field = 'disk'"));
    }

    #[test]
    pub fn test_show_index() {
        let sql = "SHOW INDEX";
        let result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default());
        let error = result.unwrap_err();
        assert_eq!("Unexpected token while parsing SQL statement, expected: '{FROM | IN} table', found: EOF", error.to_string());

        let sql = "SHOW INDEX from test";
        let result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default());
        let stmts = result.unwrap();
        assert_eq!(1, stmts.len());
        assert!(matches!(&stmts[0],
                         Statement::ShowIndex(ShowIndex {
                             table,
                             database,
                             ..

                         }) if table == "test" && database.is_none()));

        let sql = "SHOW INDEX from test from public";
        let result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default());
        let stmts = result.unwrap();
        assert_eq!(1, stmts.len());
        assert!(matches!(&stmts[0],
                         Statement::ShowIndex(ShowIndex {
                             table,
                             database: Some(database),
                             ..
                         }) if table == "test" && database == "public"));

        // SHOW INDEX deosn't support like
        let sql = "SHOW INDEX from test like 'disk%'";
        let result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default());
        let error = result.unwrap_err();
        assert_eq!(
            "SQL statement is not supported, keyword: like",
            error.to_string()
        );

        let sql = "SHOW INDEX from test where Field = 'disk'";
        let result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default());
        let stmts = result.unwrap();
        assert_eq!(1, stmts.len());
        assert!(matches!(&stmts[0],
                         Statement::ShowIndex(ShowIndex {
                             table,
                             kind: ShowKind::Where(expr),
                             ..
                         }) if table == "test" && expr.to_string() == "Field = 'disk'"));
    }

    #[test]
    fn test_show_region() {
        let sql = "SHOW REGION";
        let result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default());
        let error = result.unwrap_err();
        assert_eq!("Unexpected token while parsing SQL statement, expected: '{FROM | IN} table', found: EOF", error.to_string());

        let sql = "SHOW REGION from test";
        let result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default());
        let stmts = result.unwrap();
        assert_eq!(1, stmts.len());
        assert!(matches!(&stmts[0],
                         Statement::ShowRegion(ShowRegion {
                             table,
                             database,
                             ..

                         }) if table == "test" && database.is_none()));

        let sql = "SHOW REGION from test from public";
        let result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default());
        let stmts = result.unwrap();
        assert_eq!(1, stmts.len());
        assert!(matches!(&stmts[0],
                         Statement::ShowRegion(ShowRegion {
                             table,
                             database: Some(database),
                             ..
                         }) if table == "test" && database == "public"));

        // SHOW REGION deosn't support like
        let sql = "SHOW REGION from test like 'disk%'";
        let result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default());
        let error = result.unwrap_err();
        assert_eq!(
            "SQL statement is not supported, keyword: like",
            error.to_string()
        );

        let sql = "SHOW REGION from test where Field = 'disk'";
        let result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default());
        let stmts = result.unwrap();
        assert_eq!(1, stmts.len());
        assert!(matches!(&stmts[0],
                         Statement::ShowRegion(ShowRegion {
                             table,
                             kind: ShowKind::Where(expr),
                             ..
                         }) if table == "test" && expr.to_string() == "Field = 'disk'"));
    }

    #[test]
    fn parse_show_collation() {
        let sql = "SHOW COLLATION";
        let result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default());
        assert!(matches!(
            result.unwrap()[0],
            Statement::ShowCollation(ShowKind::All)
        ));

        let sql = "SHOW COLLATION WHERE Charset = 'latin1'";
        let result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default());
        assert!(matches!(
            result.unwrap()[0],
            Statement::ShowCollation(ShowKind::Where(_))
        ));

        let sql = "SHOW COLLATION LIKE 'latin1'";
        let result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default());
        assert!(matches!(
            result.unwrap()[0],
            Statement::ShowCollation(ShowKind::Like(_))
        ));
    }

    #[test]
    fn parse_show_charset() {
        let sql = "SHOW CHARSET";
        let result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default());
        assert!(matches!(
            result.unwrap()[0],
            Statement::ShowCharset(ShowKind::All)
        ));

        let sql = "SHOW CHARACTER SET";
        let result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default());
        assert!(matches!(
            result.unwrap()[0],
            Statement::ShowCharset(ShowKind::All)
        ));

        let sql = "SHOW CHARSET WHERE Charset = 'latin1'";
        let result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default());
        assert!(matches!(
            result.unwrap()[0],
            Statement::ShowCharset(ShowKind::Where(_))
        ));

        let sql = "SHOW CHARACTER SET LIKE 'latin1'";
        let result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default());
        assert!(matches!(
            result.unwrap()[0],
            Statement::ShowCharset(ShowKind::Like(_))
        ));
    }

    fn parse_show_table_status(sql: &str) -> ShowTableStatus {
        let result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default());
        let mut stmts = result.unwrap();
        assert_eq!(1, stmts.len());

        match stmts.remove(0) {
            Statement::ShowTableStatus(stmt) => stmt,
            _ => panic!("Failed to parse show table status"),
        }
    }

    #[test]
    pub fn test_show_table_status() {
        let sql = "SHOW TABLE STATUS";
        let stmt = parse_show_table_status(sql);
        assert!(stmt.database.is_none());
        assert_eq!(sql, stmt.to_string());

        let sql = "SHOW TABLE STATUS IN test";
        let stmt = parse_show_table_status(sql);
        assert_eq!("test", stmt.database.as_ref().unwrap());
        assert_eq!(sql, stmt.to_string());

        let sql = "SHOW TABLE STATUS LIKE '%monitor'";
        let stmt = parse_show_table_status(sql);
        assert!(stmt.database.is_none());
        assert!(matches!(stmt.kind, ShowKind::Like(_)));
        assert_eq!(sql, stmt.to_string());

        let sql = "SHOW TABLE STATUS IN test WHERE Name = 'monitor'";
        let stmt = parse_show_table_status(sql);
        assert_eq!("test", stmt.database.as_ref().unwrap());
        assert!(matches!(stmt.kind, ShowKind::Where(_)));
        assert_eq!(sql, stmt.to_string());
    }

    #[test]
    pub fn test_show_create_view() {
        let sql = "SHOW CREATE VIEW test";
        let result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default());
        let stmts = result.unwrap();
        assert_eq!(1, stmts.len());
        assert_eq!(
            stmts[0],
            Statement::ShowCreateView(ShowCreateView {
                view_name: ObjectName::from(vec![Ident::new("test")]),
            })
        );
        assert_eq!(sql, stmts[0].to_string());
    }

    #[test]
    pub fn test_show_views() {
        let sql = "SHOW VIEWS";
        let result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default());
        let stmts = result.unwrap();
        assert_eq!(1, stmts.len());
        assert_eq!(
            stmts[0],
            Statement::ShowViews(ShowViews {
                kind: ShowKind::All,
                database: None,
            })
        );
        assert_eq!(sql, stmts[0].to_string());
    }

    #[test]
    pub fn test_show_views_in_db() {
        let sql = "SHOW VIEWS IN d1";
        let result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default());
        let stmts = result.unwrap();
        assert_eq!(1, stmts.len());
        assert_eq!(
            stmts[0],
            Statement::ShowViews(ShowViews {
                kind: ShowKind::All,
                database: Some("d1".to_string()),
            })
        );
        assert_eq!(sql, stmts[0].to_string());
    }

    #[test]
    pub fn test_show_flows() {
        let sql = "SHOW FLOWS";
        let result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default());
        let stmts = result.unwrap();
        assert_eq!(1, stmts.len());
        assert_eq!(
            stmts[0],
            Statement::ShowFlows(ShowFlows {
                kind: ShowKind::All,
                database: None,
            })
        );
        assert_eq!(sql, stmts[0].to_string());
    }

    #[test]
    pub fn test_show_flows_in_db() {
        let sql = "SHOW FLOWS IN d1";
        let result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default());
        let stmts = result.unwrap();
        assert_eq!(1, stmts.len());
        assert_eq!(
            stmts[0],
            Statement::ShowFlows(ShowFlows {
                kind: ShowKind::All,
                database: Some("d1".to_string()),
            })
        );
        assert_eq!(sql, stmts[0].to_string());
    }

    #[test]
    pub fn test_show_processlist() {
        let sql = "SHOW PROCESSLIST";
        let result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default());
        let stmts = result.unwrap();
        assert_eq!(1, stmts.len());
        assert_eq!(
            stmts[0],
            Statement::ShowProcesslist(ShowProcessList { full: false })
        );
        assert_eq!(sql, stmts[0].to_string());

        let sql = "SHOW FULL PROCESSLIST";
        let result =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default());
        let stmts = result.unwrap();
        assert_eq!(1, stmts.len());
        assert_eq!(
            stmts[0],
            Statement::ShowProcesslist(ShowProcessList { full: true })
        );
        assert_eq!(sql, stmts[0].to_string());
    }
}
