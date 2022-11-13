use api::v1::InsertExpr;
use async_trait::async_trait;
use common_error::prelude::BoxedError;
use servers::influxdb::InfluxdbRequest;
use servers::{error as server_error, query_handler::InfluxdbLineProtocolHandler};
use snafu::{OptionExt, ResultExt};
use table::requests::InsertRequest;

use crate::error;
use crate::error::Result;
use crate::frontend::Mode;
use crate::instance::Instance;

#[async_trait]
impl InfluxdbLineProtocolHandler for Instance {
    async fn exec(&self, request: &InfluxdbRequest) -> servers::error::Result<()> {
        match self.mode {
            Mode::Standalone => {
                let exprs: Vec<InsertExpr> = request.try_into()?;
                self.database()
                    .batch_insert(exprs)
                    .await
                    .map_err(BoxedError::new)
                    .context(server_error::ExecuteQuerySnafu {
                        query: &request.lines,
                    })?;
            }
            Mode::Distributed => {
                self.dist_insert(request.try_into()?)
                    .await
                    .map_err(BoxedError::new)
                    .context(server_error::ExecuteInsertSnafu {
                        msg: "execute insert failed",
                    })?;
            }
        }

        Ok(())
    }
}

impl Instance {
    pub(crate) async fn dist_insert(&self, inserts: Vec<InsertRequest>) -> Result<usize> {
        let mut joins = Vec::with_capacity(inserts.len());

        for insert in inserts {
            let self_clone = self.clone();
            let insert_batch = crate::table::insert::insert_request_to_insert_batch(&insert)?;
            self.create_or_alter_table_on_demand(
                &insert.catalog_name,
                &insert.schema_name,
                &insert.table_name,
                &[insert_batch],
            )
            .await?;
            // TODO(fys): need a separate runtime here
            let join = tokio::spawn(async move {
                let catalog = self_clone.get_catalog(&insert.catalog_name)?;
                let schema = Self::get_schema(catalog, &insert.schema_name)?;
                let table = schema
                    .table(&insert.table_name)
                    .context(error::CatalogSnafu)?
                    .context(error::TableNotFoundSnafu {
                        table_name: &insert.table_name,
                    })?;

                table.insert(insert).await.context(error::TableSnafu)
            });
            joins.push(join);
        }

        let mut affected = 0;

        for join in joins {
            affected += join.await.context(error::JoinTaskSnafu)??;
        }

        Ok(affected)
    }
}
