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
use crate::key::table_route::{TableRouteManager, TableRouteManagerRef, TableRouteValue};
use crate::kv_backend::KvBackendRef;

/// [TableRouteCache] caches the [TableId] to [TableRouteValue] mapping.
pub type TableRouteCache = CacheContainer<TableId, Arc<TableRouteValue>, CacheIdent>;

pub type TableRouteCacheRef = Arc<TableRouteCache>;

/// Constructs a [TableRouteCache].
pub fn new_table_route_cache(
    name: String,
    cache: Cache<TableId, Arc<TableRouteValue>>,
    kv_backend: KvBackendRef,
) -> TableRouteCache {
    let table_info_manager = Arc::new(TableRouteManager::new(kv_backend));
    let init = init_factory(table_info_manager);

    CacheContainer::new(name, cache, Box::new(invalidator), init, Box::new(filter))
}

fn init_factory(
    table_route_manager: TableRouteManagerRef,
) -> Initializer<TableId, Arc<TableRouteValue>> {
    Arc::new(move |table_id| {
        let table_route_manager = table_route_manager.clone();
        Box::pin(async move {
            let table_route_value = table_route_manager
                .table_route_storage()
                .get(*table_id)
                .await?
                .context(error::ValueNotExistSnafu {})?;

            Ok(Some(Arc::new(table_route_value)))
        })
    })
}

fn invalidator<'a>(
    cache: &'a Cache<TableId, Arc<TableRouteValue>>,
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
