use datafusion::logical_plan::Expr as DfExpr;

/// Central struct of query API.
/// Represent logical expressions such as `A + 1`, or `CAST(c1 AS int)`.
#[derive(Clone, PartialEq, Hash)]
pub struct Expr {
    df_expr: DfExpr,
}

impl Expr {
    pub fn df_expr(&self) -> &DfExpr {
        &self.df_expr
    }
}

impl From<DfExpr> for Expr {
    fn from(df_expr: DfExpr) -> Self {
        Self { df_expr }
    }
}
