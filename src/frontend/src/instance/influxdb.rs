use api::v1::{insert_expr::Expr, InsertExpr};
use async_trait::async_trait;
use common_error::prelude::BoxedError;
use servers::influxdb::InfluxdbRequest;
use servers::{
    error::ExecuteQuerySnafu, influxdb::InsertBatches, query_handler::InfluxdbLineProtocolHandler,
};
use snafu::ResultExt;

use crate::error::RequestDatanodeSnafu;
use crate::error::Result;
use crate::instance::Instance;

#[async_trait]
impl InfluxdbLineProtocolHandler for Instance {
    async fn exec(&self, request: &InfluxdbRequest) -> servers::error::Result<()> {
        // TODO(fys): use batch insert
        self.do_insert(request.try_into()?)
            .await
            .map_err(BoxedError::new)
            .context(ExecuteQuerySnafu {
                query: &request.lines,
            })?;
        Ok(())
    }
}

impl Instance {
    async fn do_insert(&self, insert_batches: InsertBatches) -> Result<()> {
        for (table_name, batch) in insert_batches.data {
            let expr = Expr::Values(api::v1::insert_expr::Values {
                values: vec![batch.into()],
            });
            let _object_result = self
                .db
                .insert(InsertExpr {
                    table_name,
                    expr: Some(expr),
                })
                .await
                .context(RequestDatanodeSnafu)?;
        }
        Ok(())
    }
}
