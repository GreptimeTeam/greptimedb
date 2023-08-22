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

use std::collections::HashMap;
use std::sync::Arc;

use api::v1::RowInsertRequests;
use catalog::CatalogManager;
use common_meta::peer::Peer;
use snafu::{OptionExt, ResultExt};
use table::TableRef;

use crate::catalog::FrontendCatalogManager;
use crate::error::{CatalogSnafu, Result, TableNotFoundSnafu};

pub struct RowDistInserter {
    catalog_name: String,
    schema_name: String,
    catalog_manager: Arc<FrontendCatalogManager>,
}

impl RowDistInserter {
    pub fn new(
        catalog_name: String,
        schema_name: String,
        catalog_manager: Arc<FrontendCatalogManager>,
    ) -> Self {
        Self {
            catalog_name,
            schema_name,
            catalog_manager,
        }
    }

    async fn split(&self, requests: RowInsertRequests) -> Result<HashMap<Peer, RowInsertRequests>> {
        let partition_manager = self.catalog_manager.partition_manager();

        for req in requests.inserts {
            let table_name = &req.table_name;
            let table = self.get_table(table_name).await?;
        }

        todo!("Implement split_inserts_by_partition")
    }

    async fn get_table(&self, table_name: &str) -> Result<TableRef> {
        self.catalog_manager
            .table(&self.catalog_name, &self.schema_name, table_name)
            .await
            .context(CatalogSnafu)?
            .with_context(|| TableNotFoundSnafu {
                table_name: common_catalog::format_full_table_name(
                    &self.catalog_name,
                    &self.schema_name,
                    table_name,
                ),
            })
    }
}
