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

use futures::future::BoxFuture;
use moka::future::Cache;
use snafu::OptionExt;
use store_api::storage::TableId;

use crate::cache::table::{TableRoute, TableRouteCacheRef};
use crate::cache::{CacheContainer, Initializer};
use crate::error;
use crate::error::Result;
use crate::instruction::CacheIdent;
use crate::key::table_route::{LogicalTableRouteValue, PhysicalTableRouteValue};

/// [CompositeTableRoute] stores all level routes of a table.
/// - stores [PhysicalTableRouteValue] for the physical table.
/// - stores [LogicalTableRouteValue], [PhysicalTableRouteValue] for the logical table.
#[derive(Clone)]
pub enum CompositeTableRoute {
    Physical(Arc<PhysicalTableRouteValue>),
    Logical(Arc<LogicalTableRouteValue>, Arc<PhysicalTableRouteValue>),
}

impl CompositeTableRoute {
    /// Returns true if it's physical table.
    pub fn is_physical(&self) -> bool {
        matches!(self, CompositeTableRoute::Physical(_))
    }

    /// Returns [PhysicalTableRouteValue] reference.
    pub fn as_physical_table_route_ref(&self) -> &Arc<PhysicalTableRouteValue> {
        match self {
            CompositeTableRoute::Physical(route) => route,
            CompositeTableRoute::Logical(_, route) => route,
        }
    }

    /// Returns [LogicalTableRouteValue] reference if it's [CompositeTableRoute::Logical]; Otherwise returns [None].
    pub fn as_logical_table_route_ref(&self) -> Option<&Arc<LogicalTableRouteValue>> {
        match self {
            CompositeTableRoute::Physical(_) => None,
            CompositeTableRoute::Logical(route, _) => Some(route),
        }
    }
}

/// [CompositeTableRouteCache] caches the [TableId] to [CompositeTableRoute] mapping.
pub type CompositeTableRouteCache = CacheContainer<TableId, Arc<CompositeTableRoute>, CacheIdent>;

pub type CompositeTableRouteCacheRef = Arc<CompositeTableRouteCache>;

/// Constructs a [CompositeTableRouteCache].
pub fn new_composite_table_route_cache(
    name: String,
    cache: Cache<TableId, Arc<CompositeTableRoute>>,
    table_route_cache: TableRouteCacheRef,
) -> CompositeTableRouteCache {
    let init = init_factory(table_route_cache);

    CacheContainer::new(name, cache, Box::new(invalidator), init, Box::new(filter))
}

fn init_factory(
    table_route_cache: TableRouteCacheRef,
) -> Initializer<TableId, Arc<CompositeTableRoute>> {
    Arc::new(move |table_id| {
        let table_route_cache = table_route_cache.clone();
        Box::pin(async move {
            let table_route_value = table_route_cache
                .get(*table_id)
                .await?
                .context(error::ValueNotExistSnafu)?;
            match table_route_value.as_ref() {
                TableRoute::Physical(physical_table_route) => Ok(Some(Arc::new(
                    CompositeTableRoute::Physical(physical_table_route.clone()),
                ))),
                TableRoute::Logical(logical_table_route) => {
                    let physical_table_id = logical_table_route.physical_table_id();
                    let physical_table_route = table_route_cache
                        .get(physical_table_id)
                        .await?
                        .context(error::ValueNotExistSnafu)?;

                    let physical_table_route = physical_table_route
                        .as_physical_table_route_ref()
                        .with_context(|| error::UnexpectedSnafu {
                            err_msg: format!(
                                "Expected the physical table route, but got logical table route, table: {table_id}"
                            ),
                        })?;

                    Ok(Some(Arc::new(CompositeTableRoute::Logical(
                        logical_table_route.clone(),
                        physical_table_route.clone(),
                    ))))
                }
            }
        })
    })
}

fn invalidator<'a>(
    cache: &'a Cache<TableId, Arc<CompositeTableRoute>>,
    ident: &'a CacheIdent,
) -> BoxFuture<'a, Result<()>> {
    Box::pin(async move {
        if let CacheIdent::TableId(table_id) = ident {
            cache.invalidate(table_id).await
        }
        Ok(())
    })
}

fn filter(ident: &CacheIdent) -> bool {
    matches!(ident, CacheIdent::TableId(_))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use moka::future::CacheBuilder;
    use store_api::storage::RegionId;

    use super::*;
    use crate::cache::new_table_route_cache;
    use crate::ddl::test_util::create_table::test_create_table_task;
    use crate::ddl::test_util::test_create_logical_table_task;
    use crate::key::table_route::TableRouteValue;
    use crate::key::TableMetadataManager;
    use crate::kv_backend::memory::MemoryKvBackend;
    use crate::peer::Peer;
    use crate::rpc::router::{Region, RegionRoute};

    #[tokio::test]
    async fn test_cache_with_physical_table_route() {
        let mem_kv = Arc::new(MemoryKvBackend::default());
        let table_metadata_manager = TableMetadataManager::new(mem_kv.clone());
        let cache = CacheBuilder::new(128).build();
        let table_route_cache = Arc::new(new_table_route_cache(
            "test".to_string(),
            cache,
            mem_kv.clone(),
        ));
        let cache = CacheBuilder::new(128).build();
        let cache =
            new_composite_table_route_cache("test".to_string(), cache, table_route_cache.clone());

        let result = cache.get(1024).await.unwrap();
        assert!(result.is_none());
        let task = test_create_table_task("my_table", 1024);
        let table_id = 10;
        let region_id = RegionId::new(table_id, 1);
        let peer = Peer::empty(1);
        let region_routes = vec![RegionRoute {
            region: Region::new_test(region_id),
            leader_peer: Some(peer.clone()),
            ..Default::default()
        }];
        table_metadata_manager
            .create_table_metadata(
                task.table_info.clone(),
                TableRouteValue::physical(region_routes.clone()),
                HashMap::new(),
            )
            .await
            .unwrap();
        let table_route = cache.get(1024).await.unwrap().unwrap();
        assert_eq!(
            (*table_route)
                .clone()
                .as_physical_table_route_ref()
                .region_routes,
            region_routes
        );

        assert!(table_route_cache.contains_key(&1024));
        assert!(cache.contains_key(&1024));
        cache
            .invalidate(&[CacheIdent::TableId(1024)])
            .await
            .unwrap();
        assert!(!cache.contains_key(&1024));
    }

    #[tokio::test]
    async fn test_cache_with_logical_table_route() {
        let mem_kv = Arc::new(MemoryKvBackend::default());
        let table_metadata_manager = TableMetadataManager::new(mem_kv.clone());
        let cache = CacheBuilder::new(128).build();
        let table_route_cache = Arc::new(new_table_route_cache(
            "test".to_string(),
            cache,
            mem_kv.clone(),
        ));
        let cache = CacheBuilder::new(128).build();
        let cache =
            new_composite_table_route_cache("test".to_string(), cache, table_route_cache.clone());

        let result = cache.get(1024).await.unwrap();
        assert!(result.is_none());
        // Prepares table routes
        let task = test_create_table_task("my_table", 1024);
        let table_id = 10;
        let region_id = RegionId::new(table_id, 1);
        let peer = Peer::empty(1);
        let region_routes = vec![RegionRoute {
            region: Region::new_test(region_id),
            leader_peer: Some(peer.clone()),
            ..Default::default()
        }];
        table_metadata_manager
            .create_table_metadata(
                task.table_info.clone(),
                TableRouteValue::physical(region_routes.clone()),
                HashMap::new(),
            )
            .await
            .unwrap();
        let mut task = test_create_logical_table_task("logical");
        task.table_info.ident.table_id = 1025;
        table_metadata_manager
            .create_logical_tables_metadata(vec![(
                task.table_info,
                TableRouteValue::logical(1024, vec![RegionId::new(1025, 0)]),
            )])
            .await
            .unwrap();

        // Gets logical table route
        let table_route = cache.get(1025).await.unwrap().unwrap();
        assert_eq!(
            table_route
                .as_logical_table_route_ref()
                .unwrap()
                .physical_table_id(),
            1024
        );
        assert_eq!(
            table_route.as_physical_table_route_ref().region_routes,
            region_routes
        );

        assert!(!cache.contains_key(&1024));
        // Gets physical table route
        let table_route = cache.get(1024).await.unwrap().unwrap();
        assert_eq!(
            table_route.as_physical_table_route_ref().region_routes,
            region_routes
        );
        assert!(table_route.is_physical());

        cache
            .invalidate(&[CacheIdent::TableId(1025)])
            .await
            .unwrap();
        assert!(!cache.contains_key(&1025));
    }
}
