use api::v1::*;
use snafu::ensure;

use crate::{Client, MissingResultSnafu, Result};

pub const PROTOCOL_VERSION: u32 = 1;

pub type Values = Vec<Vec<u8>>;

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

    pub async fn insert(&self, table: impl Into<String>, values: Values) -> Result<()> {
        let header = ExprHeader {
            version: PROTOCOL_VERSION,
        };
        let insert = InsertExpr {
            name: table.into(),
            values,
        };
        let expr = ObjectExpr {
            header: Some(header),
            insert: Some(insert),
            ..Default::default()
        };

        self.object(expr).await?;

        Ok(())
    }

    // TODO(jiachun) select/update/delete

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
            MissingResultSnafu {
                name: "object_results",
                expected: expr_count,
                actual: res.len(),
            }
        );

        Ok(res)
    }
}
