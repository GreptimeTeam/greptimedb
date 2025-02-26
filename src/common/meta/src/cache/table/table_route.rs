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

use crate::cache::{CacheContainer, Initializer};
use crate::error;
use crate::error::Result;
use crate::instruction::CacheIdent;
use crate::key::table_route::{
    LogicalTableRouteValue, PhysicalTableRouteValue, TableRouteManager, TableRouteManagerRef,
    TableRouteValue,
};
use crate::kv_backend::KvBackendRef;

/// [TableRoute] stores `Arc` wrapped table route.
#[derive(Clone)]
pub enum TableRoute {
    Physical(Arc<PhysicalTableRouteValue>),
    Logical(Arc<LogicalTableRouteValue>),
}

impl TableRoute {
    /// Returns true if it's physical table.
    pub fn is_physical(&self) -> bool {
        matches!(self, TableRoute::Physical(_))
    }

    /// Returns [PhysicalTableRouteValue] reference if it's [TableRoute::Physical]; Otherwise it returns [None].
    pub fn as_physical_table_route_ref(&self) -> Option<&Arc<PhysicalTableRouteValue>> {
        match self {
            TableRoute::Physical(table_route) => Some(table_route),
            TableRoute::Logical(_) => None,
        }
    }
}

/// [TableRouteCache] caches the [TableId] to [TableRoute] mapping.
pub type TableRouteCache = CacheContainer<TableId, Arc<TableRoute>, CacheIdent>;

pub type TableRouteCacheRef = Arc<TableRouteCache>;

/// Constructs a [TableRouteCache].
pub fn new_table_route_cache(
    name: String,
    cache: Cache<TableId, Arc<TableRoute>>,
    kv_backend: KvBackendRef,
) -> TableRouteCache {
    let table_info_manager = Arc::new(TableRouteManager::new(kv_backend));
    let init = init_factory(table_info_manager);

    CacheContainer::new(name, cache, Box::new(invalidator), init, filter)
}

fn init_factory(
    table_route_manager: TableRouteManagerRef,
) -> Initializer<TableId, Arc<TableRoute>> {
    Arc::new(move |table_id| {
        let table_route_manager = table_route_manager.clone();
        Box::pin(async move {
            let table_route_value = table_route_manager
                .table_route_storage()
                .get(*table_id)
                .await?
                .context(error::ValueNotExistSnafu {})?;

            let table_route = match table_route_value {
                TableRouteValue::Physical(physical) => TableRoute::Physical(Arc::new(physical)),
                TableRouteValue::Logical(logical) => TableRoute::Logical(Arc::new(logical)),
            };

            Ok(Some(Arc::new(table_route)))
        })
    })
}

fn invalidator<'a>(
    cache: &'a Cache<TableId, Arc<TableRoute>>,
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
    use crate::ddl::test_util::create_table::test_create_table_task;
    use crate::key::table_route::TableRouteValue;
    use crate::key::TableMetadataManager;
    use crate::kv_backend::memory::MemoryKvBackend;
    use crate::peer::Peer;
    use crate::rpc::router::{Region, RegionRoute};

    #[tokio::test]
    async fn test_cache() {
        let mem_kv = Arc::new(MemoryKvBackend::default());
        let table_metadata_manager = TableMetadataManager::new(mem_kv.clone());
        let cache = CacheBuilder::new(128).build();
        let cache = new_table_route_cache("test".to_string(), cache, mem_kv.clone());

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
                .unwrap()
                .region_routes,
            region_routes
        );

        assert!(cache.contains_key(&1024));
        cache
            .invalidate(&[CacheIdent::TableId(1024)])
            .await
            .unwrap();
        assert!(!cache.contains_key(&1024));
    }
}
