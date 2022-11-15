use std::sync::Arc;
use std::time::Duration;

use meta_client::client::MetaClient;
use meta_client::rpc::{RouteRequest, TableName, TableRoute};
use moka::future::{Cache, CacheBuilder};
use snafu::{ensure, ResultExt};

use crate::error::{self, Result};

pub(crate) struct TableRoutes {
    meta_client: Arc<MetaClient>,
    cache: Cache<TableName, Arc<TableRoute>>,
}

impl TableRoutes {
    pub(crate) fn new(meta_client: Arc<MetaClient>) -> Self {
        Self {
            meta_client,
            cache: CacheBuilder::new(1024)
                .time_to_live(Duration::from_secs(30 * 60))
                .time_to_idle(Duration::from_secs(5 * 60))
                .build(),
        }
    }

    pub(crate) async fn get_route(&self, table_name: &TableName) -> Result<Arc<TableRoute>> {
        self.cache
            .try_get_with_by_ref(table_name, self.get_from_meta(table_name))
            .await
            .map_err(|e| {
                error::GetCacheSnafu {
                    err_msg: format!("{:?}", e),
                }
                .build()
            })
    }

    async fn get_from_meta(&self, table_name: &TableName) -> Result<Arc<TableRoute>> {
        let mut resp = self
            .meta_client
            .route(RouteRequest {
                table_names: vec![table_name.clone()],
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

    #[cfg(test)]
    pub(crate) async fn insert_table_route(
        &self,
        table_name: TableName,
        table_route: Arc<TableRoute>,
    ) {
        self.cache.insert(table_name, table_route).await
    }
}
