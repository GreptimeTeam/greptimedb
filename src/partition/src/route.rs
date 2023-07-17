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
use common_meta::table_name::TableName;
use common_telemetry::timer;
use meta_client::client::MetaClient;
use moka::future::{Cache, CacheBuilder};
use snafu::{ensure, ResultExt};

use crate::error::{self, Result};
use crate::metrics;

type TableRouteCache = Cache<TableName, Arc<TableRoute>>;

pub struct TableRoutes {
    meta_client: Arc<MetaClient>,
    // TODO(LFC): Use table id as cache key, then remove all the manually invoked cache invalidations.
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

    pub async fn get_route(&self, table_name: &TableName) -> Result<Arc<TableRoute>> {
        let _timer = timer!(metrics::METRIC_TABLE_ROUTE_GET);

        self.cache
            .try_get_with_by_ref(table_name, self.get_from_meta(table_name))
            .await
            .map_err(|e| {
                error::GetCacheSnafu {
                    err_msg: format!("{e:?}"),
                }
                .build()
            })
    }

    async fn get_from_meta(&self, table_name: &TableName) -> Result<Arc<TableRoute>> {
        let _timer = timer!(metrics::METRIC_TABLE_ROUTE_GET_REMOTE);

        let mut resp = self
            .meta_client
            .route(RouteRequest {
                table_names: vec![table_name.clone()],
                table_ids: vec![],
            })
            .await
            .context(error::RequestMetaSnafu)?;
        ensure!(
            !resp.table_routes.is_empty(),
            error::FindTableRoutesSnafu {
                table_name: table_name.to_string()
            }
        );
        let route = resp.table_routes.swap_remove(0);
        Ok(Arc::new(route))
    }

    pub async fn insert_table_route(&self, table_name: TableName, table_route: Arc<TableRoute>) {
        self.cache.insert(table_name, table_route).await
    }

    pub async fn invalidate_table_route(&self, table_name: &TableName) {
        self.cache.invalidate(table_name).await
    }

    pub fn cache(&self) -> &TableRouteCache {
        &self.cache
    }
}
