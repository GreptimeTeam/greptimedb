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
use snafu::{OptionExt, ResultExt};
use store_api::storage::TableId;
use table::metadata::TableInfo;

use crate::cache::{CacheContainer, Initializer};
use crate::error;
use crate::error::Result;
use crate::instruction::CacheIdent;
use crate::key::table_info::{TableInfoManager, TableInfoManagerRef};
use crate::kv_backend::KvBackendRef;

/// [TableInfoCache] caches the [TableId] to [TableInfo] mapping.
pub type TableInfoCache = CacheContainer<TableId, Arc<TableInfo>, CacheIdent>;

pub type TableInfoCacheRef = Arc<TableInfoCache>;

/// Constructs a [TableInfoCache].
pub fn new_table_info_cache(
    name: String,
    cache: Cache<TableId, Arc<TableInfo>>,
    kv_backend: KvBackendRef,
) -> TableInfoCache {
    let table_info_manager = Arc::new(TableInfoManager::new(kv_backend));
    let init = init_factory(table_info_manager);

    CacheContainer::new(name, cache, Box::new(invalidator), init, Box::new(filter))
}

fn init_factory(table_info_manager: TableInfoManagerRef) -> Initializer<TableId, Arc<TableInfo>> {
    Arc::new(move |table_id| {
        let table_info_manager = table_info_manager.clone();
        Box::pin(async move {
            let raw_table_info = table_info_manager
                .get(*table_id)
                .await?
                .context(error::ValueNotExistSnafu {})?
                .into_inner()
                .table_info;
            Ok(Some(Arc::new(
                TableInfo::try_from(raw_table_info).context(error::ConvertRawTableInfoSnafu)?,
            )))
        })
    })
}

fn invalidator<'a>(
    cache: &'a Cache<TableId, Arc<TableInfo>>,
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

    use super::*;
    use crate::ddl::test_util::create_table::test_create_table_task;
    use crate::key::table_route::TableRouteValue;
    use crate::key::TableMetadataManager;
    use crate::kv_backend::memory::MemoryKvBackend;

    #[tokio::test]
    async fn test_cache() {
        let mem_kv = Arc::new(MemoryKvBackend::default());
        let table_metadata_manager = TableMetadataManager::new(mem_kv.clone());
        let cache = CacheBuilder::new(128).build();
        let cache = new_table_info_cache("test".to_string(), cache, mem_kv.clone());

        let result = cache.get(1024).await.unwrap();
        assert!(result.is_none());
        let task = test_create_table_task("my_table", 1024);
        table_metadata_manager
            .create_table_metadata(
                task.table_info.clone(),
                TableRouteValue::physical(vec![]),
                HashMap::new(),
            )
            .await
            .unwrap();
        let table_info = cache.get(1024).await.unwrap().unwrap();
        assert_eq!(*table_info, TableInfo::try_from(task.table_info).unwrap());

        assert!(cache.contains_key(&1024));
        cache
            .invalidate(&[CacheIdent::TableId(1024)])
            .await
            .unwrap();
        assert!(!cache.contains_key(&1024));
    }
}
