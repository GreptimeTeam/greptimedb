use sqlparser::ast::Expr;
use sqlparser::ast::Ident;

/// Show kind for SQL expressions like `SHOW DATABASE` or `SHOW TABLE`
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ShowKind {
    All,
    Like(Ident),
    Where(Expr),
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;

    use sqlparser::dialect::GenericDialect;

    use crate::parser::ParserContext;
    use crate::statements::show_kind::ShowKind::All;
    use crate::statements::statement::Statement;

    #[test]
    pub fn test_show_database() {
        let sql = "SHOW DATABASES";
        let stmts = ParserContext::create_with_dialect(sql, &GenericDialect {}).unwrap();
        assert_eq!(1, stmts.len());
        assert_matches!(&stmts[0], Statement::ShowDatabases { .. });
        match &stmts[0] {
            Statement::ShowDatabases(show) => {
                assert_eq!(All, show.kind);
            }
            _ => {
                unreachable!();
            }
        }
    }
}
