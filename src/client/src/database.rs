use api::v1::object_expr;
use api::v1::*;
use snafu::{ensure, ResultExt};

use crate::error;
use crate::{Client, Result, error::DecodeSelectSnafu};

pub const PROTOCOL_VERSION: u32 = 1;

pub type Bytes = Vec<u8>;

#[derive(Clone, Debug)]
pub struct Database {
    name: String,
    client: Client,
}

impl Database {
    pub fn new(name: impl Into<String>, client: Client) -> Self {
        Self {
            name: name.into(),
            client,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub async fn insert(&self, table: impl Into<String>, values: Vec<Bytes>) -> Result<()> {
        let header = Header {
            version: PROTOCOL_VERSION,
        };
        let insert = InsertExpr {
            table_name: table.into(),
            values,
        };
        let expr = ObjectExpr {
            header: Some(header),
            expr: Some(object_expr::Expr::Insert(insert)),
        };

        self.object(expr).await?;

        Ok(())
    }

    pub async fn select(&self, select_expr: SelectExpr) -> Result<(Option<ResultHeader>, SelectResult)> {
        let header = Header {
            version: PROTOCOL_VERSION,
        };

        let expr = ObjectExpr {
            header: Some(header),
            expr: Some(object_expr::Expr::Select(select_expr)),
        };

        let obj_result = self.object(expr).await?;
        let header = obj_result.header;
        let result: SelectResult = obj_result.results.try_into().context(DecodeSelectSnafu)?;

        Ok((header, result))
    }

    // TODO(jiachun) update/delete

    async fn object(&self, expr: ObjectExpr) -> Result<ObjectResult> {
        let res = self.objects(vec![expr]).await?.pop().unwrap();
        Ok(res)
    }

    async fn objects(&self, exprs: Vec<ObjectExpr>) -> Result<Vec<ObjectResult>> {
        let expr_count = exprs.len();
        let req = DatabaseRequest {
            name: self.name.clone(),
            exprs,
        };

        let res = self.client.database(req).await?;
        let res = res.results;

        ensure!(
            res.len() == expr_count,
            error::MissingResultSnafu {
                name: "object_results",
                expected: expr_count,
                actual: res.len(),
            }
        );

        Ok(res)
    }
}
