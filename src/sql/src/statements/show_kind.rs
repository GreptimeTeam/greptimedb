use sqlparser::ast::Expr;
use sqlparser::ast::Ident;

/// Show kind for SQL expressions like `SHOW DATABASE` or `SHOW TABLE`
#[derive(Debug, Clone, PartialEq)]
pub enum ShowKind {
    All,
    Like(Ident),
    Where(Expr),
}
