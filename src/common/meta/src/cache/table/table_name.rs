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
use table::metadata::TableId;
use table::table_name::TableName;

use crate::cache::{CacheContainer, Initializer};
use crate::error;
use crate::error::Result;
use crate::instruction::CacheIdent;
use crate::key::table_name::{TableNameKey, TableNameManager, TableNameManagerRef};
use crate::kv_backend::KvBackendRef;

/// [TableNameCache] caches the [TableName] to [TableId] mapping.
pub type TableNameCache = CacheContainer<TableName, TableId, CacheIdent>;

pub type TableNameCacheRef = Arc<TableNameCache>;

/// Constructs a [TableNameCache].
pub fn new_table_name_cache(
    name: String,
    cache: Cache<TableName, TableId>,
    kv_backend: KvBackendRef,
) -> TableNameCache {
    let table_name_manager = Arc::new(TableNameManager::new(kv_backend));
    let init = init_factory(table_name_manager);

    CacheContainer::new(name, cache, Box::new(invalidator), init, filter)
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

fn invalidator<'a>(
    cache: &'a Cache<TableName, TableId>,
    ident: &'a CacheIdent,
) -> BoxFuture<'a, Result<()>> {
    Box::pin(async move {
        if let CacheIdent::TableName(table_name) = ident {
            cache.invalidate(table_name).await
        }
        Ok(())
    })
}

fn filter(ident: &CacheIdent) -> bool {
    matches!(ident, CacheIdent::TableName(_))
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
    async fn test_invalidate_cache() {
        let mem_kv = Arc::new(MemoryKvBackend::default());
        let cache = CacheBuilder::new(128).build();
        let cache = new_table_name_cache("test".to_string(), cache, mem_kv.clone());
        // Puts a new value.
        let table_name_manager = TableNameManager::new(mem_kv.clone());
        let table_id = 1024;
        let table_name = TableName {
            catalog_name: DEFAULT_CATALOG_NAME.to_string(),
            schema_name: DEFAULT_SCHEMA_NAME.to_string(),
            table_name: "my_table".to_string(),
        };
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
        let got = cache.get_by_ref(&table_name).await.unwrap().unwrap();
        assert_eq!(got, table_id);

        assert!(cache.contains_key(&table_name));
        cache
            .invalidate(&[CacheIdent::TableName(table_name.clone())])
            .await
            .unwrap();
        assert!(!cache.contains_key(&table_name));
    }
}
