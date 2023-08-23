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
use client::Database;
use common_meta::peer::Peer;
use futures_util::future;
use metrics::counter;
use snafu::{OptionExt, ResultExt};
use table::metadata::TableId;

use crate::catalog::FrontendCatalogManager;
use crate::error::{
    CatalogSnafu, FindDatanodeSnafu, FindTableRouteSnafu, JoinTaskSnafu, RequestDatanodeSnafu,
    Result, SplitInsertSnafu, TableNotFoundSnafu,
};

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

    pub(crate) async fn insert(&self, requests: RowInsertRequests) -> Result<u32> {
        let requests = self.split(requests).await?;
        let results = future::try_join_all(requests.into_iter().map(|(peer, inserts)| {
            let datanode_clients = self.catalog_manager.datanode_clients();
            let catalog = self.catalog_name.clone();
            let schema = self.schema_name.clone();

            common_runtime::spawn_write(async move {
                let client = datanode_clients.get_client(&peer).await;
                let database = Database::new(&catalog, &schema, client);
                database
                    .row_insert(inserts)
                    .await
                    .context(RequestDatanodeSnafu)
            })
        }))
        .await
        .context(JoinTaskSnafu)?;

        let affected_rows = results.into_iter().sum::<Result<u32>>()?;
        counter!(crate::metrics::DIST_INGEST_ROW_COUNT, affected_rows as u64);
        Ok(affected_rows)
    }

    async fn split(&self, requests: RowInsertRequests) -> Result<HashMap<Peer, RowInsertRequests>> {
        let partition_manager = self.catalog_manager.partition_manager();
        let mut inserts = HashMap::new();

        for req in requests.inserts {
            let table_name = req.table_name.clone();
            let table_id = self.get_table_id(table_name.as_str()).await?;

            let req_splits = partition_manager
                .split_row_insert_request(table_id, req)
                .await
                .context(SplitInsertSnafu)?;
            let table_route = partition_manager
                .find_table_route(table_id)
                .await
                .context(FindTableRouteSnafu { table_name })?;

            for (region_number, insert) in req_splits {
                let peer =
                    table_route
                        .find_region_leader(region_number)
                        .context(FindDatanodeSnafu {
                            region: region_number,
                        })?;
                inserts
                    .entry(peer.clone())
                    .or_default()
                    .inserts
                    .push(insert);
            }
        }

        Ok(inserts)
    }

    async fn get_table_id(&self, table_name: &str) -> Result<TableId> {
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
            .map(|table| table.table_info().table_id())
    }
}
