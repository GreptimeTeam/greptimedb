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

pub use datafusion_expr::expr::Expr as DfExpr;

/// Central struct of query API.
/// Represent logical expressions such as `A + 1`, or `CAST(c1 AS int)`.
#[derive(Clone, PartialEq, Hash, Debug)]
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_df_expr() {
        let df_expr = DfExpr::Wildcard;

        let expr: Expr = df_expr.into();

        assert_eq!(DfExpr::Wildcard, *expr.df_expr());
    }
}
