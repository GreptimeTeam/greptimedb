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

use std::fmt;

use crate::ast::{Expr, Ident};

/// Show kind for SQL expressions like `SHOW DATABASE` or `SHOW TABLE`
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ShowKind {
    All,
    Like(Ident),
    Where(Expr),
}

impl fmt::Display for ShowKind {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ShowKind::All => write!(f, "ALL"),
            ShowKind::Like(ident) => write!(f, "LIKE {}", ident),
            ShowKind::Where(expr) => write!(f, "WHERE {}", expr),
        }
    }
}

/// SQL structure for `SHOW DATABASES`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShowDatabases {
    pub kind: ShowKind,
}

impl ShowDatabases {
    /// Creates a statement for `SHOW DATABASES`
    pub fn new(kind: ShowKind) -> Self {
        ShowDatabases { kind }
    }
}

/// SQL structure for `SHOW TABLES`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShowTables {
    pub kind: ShowKind,
    pub database: Option<String>,
}

/// SQL structure for `SHOW CREATE TABLE`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShowCreateTable {
    pub table_name: String,
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;

    use sqlparser::ast::UnaryOperator;
    use sqlparser::dialect::GenericDialect;

    use super::*;
    use crate::parser::ParserContext;
    use crate::statements::statement::Statement;

    #[test]
    fn test_kind_display() {
        assert_eq!("ALL", format!("{}", ShowKind::All));
        assert_eq!(
            "LIKE test",
            format!(
                "{}",
                ShowKind::Like(Ident {
                    value: "test".to_string(),
                    quote_style: None,
                })
            )
        );
        assert_eq!(
            "WHERE NOT a",
            format!(
                "{}",
                ShowKind::Where(Expr::UnaryOp {
                    op: UnaryOperator::Not,
                    expr: Box::new(Expr::Identifier(Ident {
                        value: "a".to_string(),
                        quote_style: None,
                    })),
                })
            )
        );
    }

    #[test]
    pub fn test_show_database() {
        let sql = "SHOW DATABASES";
        let stmts = ParserContext::create_with_dialect(sql, &GenericDialect {}).unwrap();
        assert_eq!(1, stmts.len());
        assert_matches!(&stmts[0], Statement::ShowDatabases { .. });
        match &stmts[0] {
            Statement::ShowDatabases(show) => {
                assert_eq!(ShowKind::All, show.kind);
            }
            _ => {
                unreachable!();
            }
        }
    }

    #[test]
    pub fn test_show_create_table() {
        let sql = "SHOW CREATE TABLE test";
        let stmts: Vec<Statement> =
            ParserContext::create_with_dialect(sql, &GenericDialect {}).unwrap();
        assert_eq!(1, stmts.len());
        assert_matches!(&stmts[0], Statement::ShowCreateTable { .. });
        match &stmts[0] {
            Statement::ShowCreateTable(show) => {
                let table_name = show.table_name.as_str();
                assert_eq!(table_name, "test");
            }
            _ => {
                unreachable!();
            }
        }
    }
    #[test]
    pub fn test_show_create_missing_table_name() {
        let sql = "SHOW CREATE TABLE";
        ParserContext::create_with_dialect(sql, &GenericDialect {}).unwrap_err();
    }
}
