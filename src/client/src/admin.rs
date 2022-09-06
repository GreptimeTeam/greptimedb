use api::v1::*;
use snafu::prelude::*;

use crate::database::PROTOCOL_VERSION;
use crate::error;
use crate::Client;
use crate::Result;

#[derive(Clone, Debug)]
pub struct Admin {
    name: String,
    client: Client,
}

impl Admin {
    pub fn new(name: impl Into<String>, client: Client) -> Self {
        Self {
            name: name.into(),
            client,
        }
    }

    pub async fn create(&self, expr: CreateExpr) -> Result<AdminResult> {
        let header = ExprHeader {
            version: PROTOCOL_VERSION,
        };
        let expr = AdminExpr {
            header: Some(header),
            expr: Some(admin_expr::Expr::Create(expr)),
        };
        // `remove(0)` is safe because of `do_request`'s invariants.
        Ok(self.do_request(vec![expr]).await?.remove(0))
    }

    /// Invariants: the lengths of input vec (`Vec<AdminExpr>`) and output vec (`Vec<AdminResult>`) are equal.
    async fn do_request(&self, exprs: Vec<AdminExpr>) -> Result<Vec<AdminResult>> {
        let expr_count = exprs.len();
        let req = AdminRequest {
            name: self.name.clone(),
            exprs,
        };

        let resp = self.client.admin(req).await?;

        let results = resp.results;
        ensure!(
            results.len() == expr_count,
            error::MissingResultSnafu {
                name: "admin_results",
                expected: expr_count,
                actual: results.len(),
            }
        );
        Ok(results)
    }
}
