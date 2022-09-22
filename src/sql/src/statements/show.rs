use crate::ast::{Expr, Ident, ObjectName};

/// Show kind for SQL expressions like `SHOW DATABASE` or `SHOW TABLE`
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ShowKind {
    All,
    Like(Ident),
    Where(Expr),
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

/// SQL structure for `SHOW DATABASES`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShowTables {
    pub kind: ShowKind,
    pub database: Option<ObjectName>,
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;

    use sqlparser::dialect::GenericDialect;

    use super::*;
    use crate::parser::ParserContext;
    use crate::statements::statement::Statement;

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
}
