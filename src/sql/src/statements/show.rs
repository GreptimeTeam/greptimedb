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

use std::fmt::{self, Display};

use sqlparser_derive::{Visit, VisitMut};

use crate::ast::{Expr, Ident, ObjectName};

/// Show kind for SQL expressions like `SHOW DATABASE` or `SHOW TABLE`
#[derive(Debug, Clone, PartialEq, Eq, Visit, VisitMut)]
pub enum ShowKind {
    All,
    Like(Ident),
    Where(Expr),
}

impl fmt::Display for ShowKind {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ShowKind::All => write!(f, "ALL"),
            ShowKind::Like(ident) => write!(f, "LIKE {ident}"),
            ShowKind::Where(expr) => write!(f, "WHERE {expr}"),
        }
    }
}

/// SQL structure for `SHOW DATABASES`.
#[derive(Debug, Clone, PartialEq, Eq, Visit, VisitMut)]
pub struct ShowDatabases {
    pub kind: ShowKind,
}

/// The SQL `SHOW COLUMNS` statement
#[derive(Debug, Clone, PartialEq, Eq, Visit, VisitMut)]
pub struct ShowColumns {
    pub kind: ShowKind,
    pub table: String,
    pub database: Option<String>,
    pub full: bool,
}

impl ShowColumns {
    pub fn kind(&self) -> &ShowKind {
        &self.kind
    }

    pub fn table(&self) -> &String {
        &self.table
    }

    #[inline]
    fn format_table(&self) -> String {
        format!("IN {}", self.table)
    }

    #[inline]
    fn format_database(&self) -> String {
        match &self.database {
            Some(database) => format!("IN {}", database),
            None => String::default(),
        }
    }

    #[inline]
    fn format_full(&self) -> &str {
        if self.full {
            "FULL"
        } else {
            ""
        }
    }
}

impl Display for ShowColumns {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let kind = self.kind();
        let table = self.format_table();
        let database = self.format_database();
        let full = self.format_full();
        write!(f, r#"SHOW {full} {table} {database} {kind}"#)
    }
}

/// The SQL `SHOW INDEX` statement
#[derive(Debug, Clone, PartialEq, Eq, Visit, VisitMut)]
pub struct ShowIndex {
    pub kind: ShowKind,
    pub table: String,
    pub database: Option<String>,
}

impl ShowIndex {
    pub fn kind(&self) -> &ShowKind {
        &self.kind
    }

    #[inline]
    fn format_table(&self) -> String {
        format!("IN {}", self.table)
    }

    #[inline]
    fn format_database(&self) -> String {
        match &self.database {
            Some(database) => format!("IN {}", database),
            None => String::default(),
        }
    }
}

impl Display for ShowIndex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let kind = self.kind();
        let table = self.format_table();
        let database = self.format_database();
        write!(f, r#"SHOW INDEX {table} {database} {kind}"#)
    }
}

impl ShowDatabases {
    /// Creates a statement for `SHOW DATABASES`
    pub fn new(kind: ShowKind) -> Self {
        ShowDatabases { kind }
    }

    pub fn kind(&self) -> &ShowKind {
        &self.kind
    }
}

impl Display for ShowDatabases {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let kind = self.kind();
        write!(f, r#"SHOW DATABASES {kind}"#)
    }
}

/// SQL structure for `SHOW TABLES`.
#[derive(Debug, Clone, PartialEq, Eq, Visit, VisitMut)]
pub struct ShowTables {
    pub kind: ShowKind,
    pub database: Option<String>,
    pub full: bool,
}

impl ShowTables {
    pub fn kind(&self) -> &ShowKind {
        &self.kind
    }

    #[inline]
    fn format_database(&self) -> String {
        match &self.database {
            None => String::default(),
            Some(database) => {
                format!("IN {}", database)
            }
        }
    }

    #[inline]
    fn format_full(&self) -> &str {
        if self.full {
            "FULL"
        } else {
            ""
        }
    }
}

impl Display for ShowTables {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let full = self.format_full();
        let database = self.format_database();
        let kind = self.kind();
        write!(f, r#"SHOW {full} TABLES {database} {kind}"#)
    }
}

/// SQL structure for `SHOW CREATE TABLE`.
#[derive(Debug, Clone, PartialEq, Eq, Visit, VisitMut)]
pub struct ShowCreateTable {
    pub table_name: ObjectName,
}

impl ShowCreateTable {
    fn table_name(&self) -> &ObjectName {
        &self.table_name
    }
}

impl Display for ShowCreateTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let table_name = self.table_name();
        write!(f, r#"SHOW CREATE {table_name}"#)
    }
}

/// SQL structure for `SHOW VARIABLES xxx`.
#[derive(Debug, Clone, PartialEq, Eq, Visit, VisitMut)]
pub struct ShowVariables {
    pub variable: ObjectName,
}

impl ShowVariables {
    fn variable(&self) -> &ObjectName {
        &self.variable
    }
}

impl Display for ShowVariables {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let variable = self.variable();
        write!(f, r#"SHOW VARIABLES {variable}"#)
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;

    use sqlparser::ast::UnaryOperator;

    use super::*;
    use crate::dialect::GreptimeDbDialect;
    use crate::parser::{ParseOptions, ParserContext};
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
        let stmts =
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();
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
            ParserContext::create_with_dialect(sql, &GreptimeDbDialect {}, ParseOptions::default())
                .unwrap();
        assert_eq!(1, stmts.len());
        assert_matches!(&stmts[0], Statement::ShowCreateTable { .. });
        match &stmts[0] {
            Statement::ShowCreateTable(show) => {
                let table_name = show.table_name.to_string();
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
        assert!(ParserContext::create_with_dialect(
            sql,
            &GreptimeDbDialect {},
            ParseOptions::default()
        )
        .is_err());
    }
}
