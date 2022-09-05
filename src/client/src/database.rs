use std::sync::Arc;

use api::v1::codec::SelectResult as GrpcSelectResult;
use api::v1::{
    object_expr, object_result, select_expr, DatabaseRequest, ExprHeader, InsertExpr,
    MutateResult as GrpcMutateResult, ObjectExpr, ObjectResult as GrpcObjectResult, PhysicalPlan,
    SelectExpr,
};
use common_error::status_code::StatusCode;
use common_grpc::AsExcutionPlan;
use common_grpc::DefaultAsPlanImpl;
use datafusion::physical_plan::ExecutionPlan;
use snafu::{ensure, OptionExt, ResultExt};

use crate::error;
use crate::{
    error::DatanodeSnafu, error::DecodeSelectSnafu, error::EncodePhysicalSnafu, Client, Result,
};

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
        let header = ExprHeader {
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

    pub async fn select(&self, expr: Select) -> Result<ObjectResult> {
        let select_expr = match expr {
            Select::Sql(sql) => SelectExpr {
                expr: Some(select_expr::Expr::Sql(sql)),
            },
        };
        self.do_select(select_expr).await
    }

    pub async fn physical_plan(
        &self,
        physical: Arc<dyn ExecutionPlan>,
        original_ql: Option<String>,
    ) -> Result<ObjectResult> {
        let plan = DefaultAsPlanImpl::try_from_physical_plan(physical.clone())
            .context(EncodePhysicalSnafu { physical })?
            .bytes;
        let original_ql = original_ql.unwrap_or_default();
        let select_expr = SelectExpr {
            expr: Some(select_expr::Expr::PhysicalPlan(PhysicalPlan {
                original_ql: original_ql.into_bytes(),
                plan,
            })),
        };
        self.do_select(select_expr).await
    }

    async fn do_select(&self, select_expr: SelectExpr) -> Result<ObjectResult> {
        let header = ExprHeader {
            version: PROTOCOL_VERSION,
        };

        let expr = ObjectExpr {
            header: Some(header),
            expr: Some(object_expr::Expr::Select(select_expr)),
        };

        let obj_result = self.object(expr).await?;
        obj_result.try_into()
    }

    // TODO(jiachun) update/delete

    async fn object(&self, expr: ObjectExpr) -> Result<GrpcObjectResult> {
        let res = self.objects(vec![expr]).await?.pop().unwrap();
        Ok(res)
    }

    async fn objects(&self, exprs: Vec<ObjectExpr>) -> Result<Vec<GrpcObjectResult>> {
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

#[derive(Debug)]
pub enum ObjectResult {
    Select(GrpcSelectResult),
    Mutate(GrpcMutateResult),
}

impl TryFrom<api::v1::ObjectResult> for ObjectResult {
    type Error = error::Error;

    fn try_from(object_result: api::v1::ObjectResult) -> std::result::Result<Self, Self::Error> {
        let header = object_result.header.context(error::MissingHeaderSnafu)?;
        if !StatusCode::is_success(header.code) {
            return DatanodeSnafu {
                code: header.code,
                msg: header.err_msg,
            }
            .fail();
        }

        let obj_result = object_result.result.context(error::MissingResultSnafu {
            name: "result".to_string(),
            expected: 1_usize,
            actual: 0_usize,
        })?;
        Ok(match obj_result {
            object_result::Result::Select(select) => {
                let result = (*select.raw_data).try_into().context(DecodeSelectSnafu)?;
                ObjectResult::Select(result)
            }
            object_result::Result::Mutate(mutate) => ObjectResult::Mutate(mutate),
        })
    }
}

pub enum Select {
    Sql(String),
}
