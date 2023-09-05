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

use api::v1::region::{
    InsertRequest as RegionInsertRequest, InsertRequests as RegionInsertRequests,
};
use api::v1::RowInsertRequests;
use catalog::CatalogManager;
use session::context::QueryContext;
use snafu::{OptionExt, ResultExt};
use store_api::storage::RegionId;
use table::TableRef;

use crate::error::{CatalogSnafu, Result, TableNotFoundSnafu};

pub struct RowToRegion<'a> {
    catalog_manager: &'a dyn CatalogManager,
    ctx: &'a QueryContext,
}

impl<'a> RowToRegion<'a> {
    pub fn new(catalog_manager: &'a dyn CatalogManager, ctx: &'a QueryContext) -> Self {
        Self {
            catalog_manager,
            ctx,
        }
    }

    pub async fn convert(&self, requests: RowInsertRequests) -> Result<RegionInsertRequests> {
        let mut region_request = Vec::with_capacity(requests.inserts.len());
        for request in requests.inserts {
            let table = self.get_table(&request.table_name).await?;

            let region_id = RegionId::new(table.table_info().table_id(), request.region_number);
            let insert_request = RegionInsertRequest {
                region_id: region_id.into(),
                rows: request.rows,
            };
            region_request.push(insert_request);
        }

        Ok(RegionInsertRequests {
            requests: region_request,
        })
    }

    async fn get_table(&self, table_name: &str) -> Result<TableRef> {
        let catalog_name = self.ctx.current_catalog();
        let schema_name = self.ctx.current_schema();
        self.catalog_manager
            .table(catalog_name, schema_name, table_name)
            .await
            .context(CatalogSnafu)?
            .with_context(|| TableNotFoundSnafu {
                table_name: format!("{}.{}.{}", catalog_name, schema_name, table_name),
            })
    }
}
