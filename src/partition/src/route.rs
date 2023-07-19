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

use std::sync::Arc;
use std::time::Duration;

use common_meta::rpc::router::{RouteRequest, TableRoute};
use common_telemetry::timer;
use meta_client::client::MetaClient;
use moka::future::{Cache, CacheBuilder};
use snafu::{ensure, ResultExt};
use table::metadata::TableId;

use crate::error::{self, Result};
use crate::metrics;

type TableRouteCache = Cache<TableId, Arc<TableRoute>>;

pub struct TableRoutes {
    meta_client: Arc<MetaClient>,
    cache: TableRouteCache,
}

// TODO(hl): maybe periodically refresh table route cache?
impl TableRoutes {
    pub fn new(meta_client: Arc<MetaClient>) -> Self {
        Self {
            meta_client,
            cache: CacheBuilder::new(1024)
                .time_to_live(Duration::from_secs(30 * 60))
                .time_to_idle(Duration::from_secs(5 * 60))
                .build(),
        }
    }

    pub async fn get_route(&self, table_id: TableId) -> Result<Arc<TableRoute>> {
        let _timer = timer!(metrics::METRIC_TABLE_ROUTE_GET);

        self.cache
            .try_get_with_by_ref(&table_id, self.get_from_meta(table_id))
            .await
            .map_err(|e| {
                error::GetCacheSnafu {
                    err_msg: format!("{e:?}"),
                }
                .build()
            })
    }

    async fn get_from_meta(&self, table_id: TableId) -> Result<Arc<TableRoute>> {
        let _timer = timer!(metrics::METRIC_TABLE_ROUTE_GET_REMOTE);

        let mut resp = self
            .meta_client
            .route(RouteRequest {
                table_ids: vec![table_id],
            })
            .await
            .context(error::RequestMetaSnafu)?;
        ensure!(
            !resp.table_routes.is_empty(),
            error::FindTableRoutesSnafu { table_id }
        );
        let route = resp.table_routes.swap_remove(0);
        Ok(Arc::new(route))
    }

    pub async fn insert_table_route(&self, table_id: TableId, table_route: Arc<TableRoute>) {
        self.cache.insert(table_id, table_route).await
    }

    pub async fn invalidate_table_route(&self, table_id: TableId) {
        self.cache.invalidate(&table_id).await
    }

    pub fn cache(&self) -> &TableRouteCache {
        &self.cache
    }
}
