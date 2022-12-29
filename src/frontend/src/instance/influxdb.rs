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

use std::collections::HashMap;

use api::v1::{Column, InsertRequest as GrpcInsertRequest};
use async_trait::async_trait;
use common_catalog::consts::DEFAULT_CATALOG_NAME;
use common_error::prelude::BoxedError;
use common_grpc_expr::column_to_vector;
use servers::influxdb::InfluxdbRequest;
use servers::query_handler::InfluxdbLineProtocolHandler;
use servers::{error as server_error, Mode};
use snafu::{OptionExt, ResultExt};
use table::requests::InsertRequest;

use crate::error;
use crate::error::{InsertBatchToRequestSnafu, Result};
use crate::instance::Instance;

#[async_trait]
impl InfluxdbLineProtocolHandler for Instance {
    async fn exec(&self, request: &InfluxdbRequest) -> servers::error::Result<()> {
        match self.mode {
            Mode::Standalone => {
                self.handle_inserts(request.try_into()?)
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
    pub(crate) async fn dist_insert(&self, inserts: Vec<GrpcInsertRequest>) -> Result<usize> {
        let mut joins = Vec::with_capacity(inserts.len());
        let catalog_name = DEFAULT_CATALOG_NAME;

        for insert in inserts {
            let self_clone = self.clone();

            let schema_name = insert.schema_name.to_string();
            let table_name = insert.table_name.to_string();

            let columns = &insert.columns;
            let row_count = insert.row_count;

            self.create_or_alter_table_on_demand(catalog_name, &schema_name, &table_name, columns)
                .await?;

            let request = Self::columns_to_request(
                catalog_name,
                &schema_name,
                &table_name,
                columns,
                row_count,
            )?;

            // TODO(fys): need a separate runtime here
            let self_clone = self_clone.clone();
            let join = tokio::spawn(async move {
                let catalog = self_clone.get_catalog(catalog_name)?;
                let schema = Self::get_schema(catalog, &schema_name)?;
                let table = schema
                    .table(&table_name)
                    .context(error::CatalogSnafu)?
                    .context(error::TableNotFoundSnafu { table_name })?;

                table.insert(request).await.context(error::TableSnafu)
            });
            joins.push(join);
        }

        let mut affected = 0;

        for join in joins {
            affected += join.await.context(error::JoinTaskSnafu)??;
        }

        Ok(affected)
    }

    fn columns_to_request(
        catalog_name: &str,
        schema_name: &str,
        table_name: &str,
        columns: &[Column],
        row_count: u32,
    ) -> Result<InsertRequest> {
        let mut vectors = HashMap::with_capacity(columns.len());
        for col in columns {
            let vector = column_to_vector(col, row_count).context(InsertBatchToRequestSnafu)?;
            vectors.insert(col.column_name.clone(), vector);
        }
        Ok(InsertRequest {
            catalog_name: catalog_name.to_string(),
            schema_name: schema_name.to_string(),
            table_name: table_name.to_string(),
            columns_values: vectors,
        })
    }
}
