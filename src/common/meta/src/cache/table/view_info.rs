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
use crate::key::view_info::{ViewInfoManager, ViewInfoManagerRef, ViewInfoValue};
use crate::kv_backend::KvBackendRef;

/// [ViewInfoCache] caches the [TableId] to [ViewInfoValue] mapping.
pub type ViewInfoCache = CacheContainer<TableId, Arc<ViewInfoValue>, CacheIdent>;

pub type ViewInfoCacheRef = Arc<ViewInfoCache>;

/// Constructs a [ViewInfoCache].
pub fn new_view_info_cache(
    name: String,
    cache: Cache<TableId, Arc<ViewInfoValue>>,
    kv_backend: KvBackendRef,
) -> ViewInfoCache {
    let view_info_manager = Arc::new(ViewInfoManager::new(kv_backend));
    let init = init_factory(view_info_manager);

    CacheContainer::new(name, cache, Box::new(invalidator), init, Box::new(filter))
}

fn init_factory(view_info_manager: ViewInfoManagerRef) -> Initializer<TableId, Arc<ViewInfoValue>> {
    Arc::new(move |view_id| {
        let view_info_manager = view_info_manager.clone();
        Box::pin(async move {
            let view_info = view_info_manager
                .get(*view_id)
                .await?
                .context(error::ValueNotExistSnafu {})?
                .into_inner();

            Ok(Some(Arc::new(view_info)))
        })
    })
}

fn invalidator<'a>(
    cache: &'a Cache<TableId, Arc<ViewInfoValue>>,
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
    use std::sync::Arc;

    use moka::future::CacheBuilder;

    use super::*;
    use crate::ddl::tests::create_view::test_create_view_task;
    use crate::key::TableMetadataManager;
    use crate::kv_backend::memory::MemoryKvBackend;

    #[tokio::test]
    async fn test_view_info_cache() {
        let mem_kv = Arc::new(MemoryKvBackend::default());
        let table_metadata_manager = TableMetadataManager::new(mem_kv.clone());
        let cache = CacheBuilder::new(128).build();
        let cache = new_view_info_cache("test".to_string(), cache, mem_kv.clone());

        let result = cache.get(1024).await.unwrap();
        assert!(result.is_none());
        let mut task = test_create_view_task("my_view");
        task.view_info.ident.table_id = 1024;
        table_metadata_manager
            .create_view_metadata(task.view_info.clone(), &task.create_view.logical_plan)
            .await
            .unwrap();

        let view_info = cache.get(1024).await.unwrap().unwrap();
        assert_eq!((*view_info).view_info, task.create_view.logical_plan);

        assert!(cache.contains_key(&1024));
        cache
            .invalidate(&[CacheIdent::TableId(1024)])
            .await
            .unwrap();
        assert!(!cache.contains_key(&1024));
    }
}
