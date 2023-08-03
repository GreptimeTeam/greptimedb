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

use api::v1::{RowInsertRequest, RowInsertRequests};
use catalog::CatalogManagerRef;
use common_query::Output;
use futures_util::future;
use session::context::QueryContextRef;
use snafu::{OptionExt, ResultExt};
use table::requests::InsertRequest;

use crate::error::{CatalogSnafu, InsertSnafu, JoinTaskSnafu, Result};

pub struct RowInserter {
    catalog_manager: CatalogManagerRef,
}

impl RowInserter {
    pub fn new(catalog_manager: CatalogManagerRef) -> Self {
        Self { catalog_manager }
    }

    pub async fn handle_inserts(
        &self,
        requests: RowInsertRequests,
        ctx: QueryContextRef,
    ) -> Result<Output> {
        let insert_tasks = requests.inserts.into_iter().map(|insert| {
            let catalog_manager = self.catalog_manager.clone();
            let catalog_name = ctx.current_catalog();
            let schema_name = ctx.current_schema();

            let insert_task = async move {
                let table_name = insert.table_name.clone();
                let table = catalog_manager
                    .table(&catalog_name, &schema_name, &table_name)
                    .await
                    .context(CatalogSnafu)?
                    .with_context(|| crate::error::TableNotFoundSnafu {
                        table_name: format!("{catalog_name}.{schema_name}.{table_name}"),
                    })?;
                let request =
                    Self::convert_to_table_insert_request(&catalog_name, &schema_name, insert)?;

                table.insert(request).await.with_context(|_| InsertSnafu {
                    table_name: format!("{catalog_name}.{schema_name}.{table_name}"),
                })
            };

            common_runtime::spawn_write(insert_task)
        });

        let results = future::try_join_all(insert_tasks)
            .await
            .context(JoinTaskSnafu)?;
        let affected_rows = results.into_iter().sum::<Result<usize>>()?;

        Ok(Output::AffectedRows(affected_rows))
    }

    pub fn convert_to_table_insert_request(
        _catalog_name: &str,
        _schema_name: &str,
        _request: RowInsertRequest,
    ) -> Result<InsertRequest> {
        todo!("jeremy")
    }
}
