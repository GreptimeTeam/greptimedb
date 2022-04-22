use sqlparser::ast::Expr;
use sqlparser::ast::Ident;

/// Show kind for SQL expressions like `SHOW DATABASE` or `SHOW TABLE`
#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub enum ShowKind {
    All,
    Like(Ident),
    Where(Expr),
}
