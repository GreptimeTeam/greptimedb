use api::v1::InsertExpr;
use async_trait::async_trait;
use common_error::prelude::BoxedError;
use servers::influxdb::InfluxdbRequest;
use servers::{error::ExecuteQuerySnafu, query_handler::InfluxdbLineProtocolHandler};
use snafu::ResultExt;

use crate::instance::Instance;

#[async_trait]
impl InfluxdbLineProtocolHandler for Instance {
    async fn exec(&self, request: &InfluxdbRequest) -> servers::error::Result<()> {
        let exprs: Vec<InsertExpr> = request.try_into()?;
        self.db
            .batch_insert(exprs)
            .await
            .map_err(BoxedError::new)
            .context(ExecuteQuerySnafu {
                query: &request.lines,
            })?;
        Ok(())
    }
}
