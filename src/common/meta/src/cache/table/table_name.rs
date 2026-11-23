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

use moka::future::Cache;
use snafu::OptionExt;
use table::metadata::TableId;
use table::table_name::TableName;

use crate::cache::{CacheContainer, Initializer};
use crate::error;
use crate::instruction::CacheIdent;
use crate::key::table_name::{TableNameKey, TableNameManager, TableNameManagerRef};
use crate::kv_backend::KvBackendRef;

/// [TableNameCache] caches the [TableName] to [TableId] mapping.
pub type TableNameCache = CacheContainer<TableName, TableId, CacheIdent>;

pub type TableNameCacheRef = Arc<TableNameCache>;

/// Constructs a [TableNameCache].
pub fn new_table_name_cache(
    name: String,
    cache: Cache<(usize, TableName), TableId>,
    kv_backend: KvBackendRef,
) -> TableNameCache {
    let table_name_manager = Arc::new(TableNameManager::new(kv_backend));
    let init = init_factory(table_name_manager.clone());

    CacheContainer::new(name, cache, init, CacheIdent::table_name_key).with_batch_initializer(
        Arc::new(move |names| {
            let manager = table_name_manager.clone();
            Box::pin(async move {
                let keys = names
                    .iter()
                    .map(|name| {
                        TableNameKey::new(&name.catalog_name, &name.schema_name, &name.table_name)
                    })
                    .collect();
                let ids = manager.batch_get(keys).await?;
                Ok(names
                    .into_iter()
                    .zip(ids)
                    .filter_map(|(name, id)| id.map(|id| (name, id.table_id())))
                    .collect())
            })
        }),
    )
}

fn init_factory(table_name_manager: TableNameManagerRef) -> Initializer<TableName, TableId> {
    Arc::new(
        move |TableName {
                  catalog_name,
                  schema_name,
                  table_name,
              }| {
            let table_name_manager = table_name_manager.clone();
            Box::pin(async move {
                Ok(Some(
                    table_name_manager
                        .get(TableNameKey {
                            catalog: catalog_name,
                            schema: schema_name,
                            table: table_name,
                        })
                        .await?
                        .context(error::ValueNotExistSnafu {})?
                        .table_id(),
                ))
            })
        },
    )
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
    use moka::future::CacheBuilder;

    use super::*;
    use crate::kv_backend::memory::MemoryKvBackend;
    use crate::kv_backend::txn::TxnService;

    #[tokio::test]
    async fn test_cache_get() {
        let mem_kv = Arc::new(MemoryKvBackend::default());
        let cache = CacheBuilder::new(128).build();
        let cache = new_table_name_cache("test".to_string(), cache, mem_kv.clone());
        let result = cache
            .get_by_ref(&TableName {
                catalog_name: DEFAULT_CATALOG_NAME.to_string(),
                schema_name: DEFAULT_SCHEMA_NAME.to_string(),
                table_name: "my_table".to_string(),
            })
            .await
            .unwrap();
        assert!(result.is_none());
        // Puts a new value.
        let table_name_manager = TableNameManager::new(mem_kv.clone());
        let table_id = 1024;
        let txn = table_name_manager
            .build_create_txn(
                &TableNameKey {
                    catalog: DEFAULT_CATALOG_NAME,
                    schema: DEFAULT_SCHEMA_NAME,
                    table: "my_table",
                },
                table_id,
            )
            .unwrap();
        mem_kv.txn(txn).await.unwrap();
        let got = cache
            .get_by_ref(&TableName {
                catalog_name: DEFAULT_CATALOG_NAME.to_string(),
                schema_name: DEFAULT_SCHEMA_NAME.to_string(),
                table_name: "my_table".to_string(),
            })
            .await
            .unwrap()
            .unwrap();
        assert_eq!(got, table_id);
    }

    #[tokio::test]
    async fn test_lookup_after_same_name_recreation() {
        use crate::cache::new_table_info_cache;
        use crate::ddl::test_util::create_table::test_create_table_task;
        use crate::key::TableMetadataManager;
        use crate::key::table_route::TableRouteValue;

        let kv = Arc::new(MemoryKvBackend::default());
        let metadata = TableMetadataManager::new(kv.clone());
        let names = new_table_name_cache("names".into(), Cache::new(128), kv.clone());
        let infos = new_table_info_cache("infos".into(), Cache::new(128), kv);
        let route = TableRouteValue::physical(vec![]);
        let old = test_create_table_task("my_table", 1024).table_info;
        let new = test_create_table_task("my_table", 1025).table_info;
        let name = TableName {
            catalog_name: old.catalog_name.clone(),
            schema_name: old.schema_name.clone(),
            table_name: old.name.clone(),
        };
        metadata
            .create_table_metadata(old, route.clone(), Default::default())
            .await
            .unwrap();
        assert_eq!(names.get_by_ref(&name).await.unwrap(), Some(1024));
        assert!(infos.get(1024).await.unwrap().is_some());

        metadata
            .delete_table_metadata(1024, &name, &route, &Default::default(), None)
            .await
            .unwrap();
        metadata
            .create_table_metadata(new, route, Default::default())
            .await
            .unwrap();
        let tokens = [
            CacheIdent::TableName(name.clone()),
            CacheIdent::TableId(1024),
        ];
        names.invalidate(&tokens).await.unwrap();
        infos.invalidate(&tokens).await.unwrap();

        let ids = names.get_many(&[name]).await.unwrap();
        assert_eq!(ids, [Some(1025)]);
        let result = infos.get_many(&[1024, ids[0].unwrap()]).await.unwrap();
        assert!(result[0].is_none());
        assert_eq!(result[1].as_ref().unwrap().ident.table_id, 1025);
    }
}
