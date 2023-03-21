// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use catalog::SchemaProviderRef;
use common_query::Output;
use snafu::{OptionExt, ResultExt};
use table::requests::FlushTableRequest;

use crate::error::{self, CatalogSnafu, DatabaseNotFoundSnafu, Result};
use crate::sql::SqlHandler;

impl SqlHandler {
    pub(crate) async fn flush_table(&self, req: FlushTableRequest) -> Result<Output> {
        let schema = self
            .catalog_manager
            .schema(&req.catalog_name, &req.schema_name)
            .context(CatalogSnafu)?
            .context(DatabaseNotFoundSnafu {
                catalog: &req.catalog_name,
                schema: &req.schema_name,
            })?;

        if let Some(table) = &req.table_name {
            self.flush_table_inner(schema, table, req.region_number, req.wait)
                .await?;
        } else {
            let all_table_names = schema.table_names().context(CatalogSnafu)?;
            futures::future::join_all(all_table_names.iter().map(|table| {
                self.flush_table_inner(schema.clone(), table, req.region_number, req.wait)
            }))
            .await
            .into_iter()
            .collect::<Result<Vec<_>>>()?;
        }
        Ok(Output::AffectedRows(0))
    }

    async fn flush_table_inner(
        &self,
        schema: SchemaProviderRef,
        table_name: &str,
        region: Option<u32>,
        wait: Option<bool>,
    ) -> Result<()> {
        schema
            .table(table_name)
            .await
            .context(error::FindTableSnafu { table_name })?
            .context(error::TableNotFoundSnafu { table_name })?
            .flush(region, wait)
            .await
            .context(error::FlushTableSnafu { table_name })
    }
}
