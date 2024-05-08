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

use std::collections::HashSet;
use std::sync::Arc;

use futures::future::BoxFuture;
use futures::TryStreamExt;
use moka::future::Cache;
use moka::ops::compute::Op;
use table::metadata::TableId;

use crate::cache::{CacheContainer, Initializer};
use crate::error::Result;
use crate::instruction::{CacheIdent, CreateFlow, DropFlow};
use crate::key::flow::{TableFlowManager, TableFlowManagerRef};
use crate::kv_backend::KvBackendRef;
use crate::FlownodeId;

type FlownodeSet = HashSet<FlownodeId>;

/// [TableFlownodeSetCache] caches the [TableId] to [FlownodeSet] mapping.
pub type TableFlownodeSetCache = CacheContainer<TableId, FlownodeSet, CacheIdent>;

/// Constructs a [TableFlownodeSetCache].
pub fn new_table_flownode_set_cache(
    cache: Cache<TableId, FlownodeSet>,
    kv_backend: KvBackendRef,
) -> TableFlownodeSetCache {
    let table_flow_manager = Arc::new(TableFlowManager::new(kv_backend));
    let init = init_factory(table_flow_manager);

    CacheContainer::new(cache, Box::new(invalidator), init, Box::new(filter))
}

fn init_factory(table_flow_manager: TableFlowManagerRef) -> Initializer<TableId, FlownodeSet> {
    Arc::new(move |&table_id| {
        let table_flow_manager = table_flow_manager.clone();
        Box::pin(async move {
            table_flow_manager
                .flows(table_id)
                .map_ok(|key| key.flownode_id())
                .try_collect::<HashSet<_>>()
                .await
                .map(Some)
        })
    })
}

async fn invalidate_create_flow(
    cache: &Cache<TableId, FlownodeSet>,
    CreateFlow {
        source_table_ids,
        flownode_ids,
    }: CreateFlow,
) {
    for table_id in source_table_ids {
        let entry = cache.entry(table_id);
        entry
            .and_compute_with(
                async |entry: Option<moka::Entry<u32, HashSet<u64>>>| match entry {
                    Some(entry) => {
                        let mut set = entry.into_value();
                        set.extend(flownode_ids.clone());

                        Op::Put(set)
                    }
                    None => Op::Put(HashSet::from_iter(flownode_ids.clone())),
                },
            )
            .await;
    }
}

async fn invalidate_drop_flow(
    cache: &Cache<TableId, FlownodeSet>,
    DropFlow {
        source_table_ids,
        flownode_ids,
    }: DropFlow,
) {
    for table_id in source_table_ids {
        let entry = cache.entry(table_id);
        entry
            .and_compute_with(
                async |entry: Option<moka::Entry<u32, HashSet<u64>>>| match entry {
                    Some(entry) => {
                        let mut set = entry.into_value();
                        for flownode_id in &flownode_ids {
                            set.remove(flownode_id);
                        }

                        Op::Put(set)
                    }
                    None => {
                        // Do nothing
                        Op::Nop
                    }
                },
            )
            .await;
    }
}

fn invalidator(
    cache: &'_ Cache<TableId, FlownodeSet>,
    caches: Vec<CacheIdent>,
) -> BoxFuture<'_, Result<()>> {
    Box::pin(async move {
        for ident in caches {
            match ident {
                CacheIdent::CreateFlow(create_flow) => {
                    invalidate_create_flow(cache, create_flow).await
                }
                CacheIdent::DropFlow(drop_flow) => invalidate_drop_flow(cache, drop_flow).await,
                _ => {}
            }
        }

        Ok(())
    })
}

fn filter(caches: Vec<CacheIdent>) -> Vec<CacheIdent> {
    caches
        .into_iter()
        .filter(|cache| matches!(cache, CacheIdent::CreateFlow(_) | CacheIdent::DropFlow(_)))
        .collect()
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::Arc;

    use moka::future::CacheBuilder;

    use crate::cache::flow::table_flownode::new_table_flownode_set_cache;
    use crate::instruction::{CacheIdent, CreateFlow, DropFlow};
    use crate::kv_backend::memory::MemoryKvBackend;

    #[tokio::test]
    async fn test_cache_empty_set() {
        let mem_kv = Arc::new(MemoryKvBackend::default());
        let cache = CacheBuilder::new(128).build();
        let cache = new_table_flownode_set_cache(cache, mem_kv);
        let set = cache.get(1024).await.unwrap().unwrap();
        assert!(set.is_empty());
    }

    #[tokio::test]
    async fn test_create_flow() {
        let mem_kv = Arc::new(MemoryKvBackend::default());
        let cache = CacheBuilder::new(128).build();
        let cache = new_table_flownode_set_cache(cache, mem_kv);
        let ident = vec![CacheIdent::CreateFlow(CreateFlow {
            source_table_ids: vec![1024, 1025],
            flownode_ids: vec![1, 2, 3, 4, 5],
        })];
        cache.invalidate(ident).await.unwrap();
        let set = cache.get(1024).await.unwrap().unwrap();
        assert_eq!(set.len(), 5);
        let set = cache.get(1025).await.unwrap().unwrap();
        assert_eq!(set.len(), 5);
    }

    #[tokio::test]
    async fn test_drop_flow() {
        let mem_kv = Arc::new(MemoryKvBackend::default());
        let cache = CacheBuilder::new(128).build();
        let cache = new_table_flownode_set_cache(cache, mem_kv);
        let ident = vec![
            CacheIdent::CreateFlow(CreateFlow {
                source_table_ids: vec![1024, 1025],
                flownode_ids: vec![1, 2, 3, 4, 5],
            }),
            CacheIdent::CreateFlow(CreateFlow {
                source_table_ids: vec![1024, 1025],
                flownode_ids: vec![11, 12],
            }),
        ];
        cache.invalidate(ident).await.unwrap();
        let set = cache.get(1024).await.unwrap().unwrap();
        assert_eq!(set.len(), 7);
        let set = cache.get(1025).await.unwrap().unwrap();
        assert_eq!(set.len(), 7);

        let ident = vec![CacheIdent::DropFlow(DropFlow {
            source_table_ids: vec![1024, 1025],
            flownode_ids: vec![1, 2, 3, 4, 5],
        })];
        cache.invalidate(ident).await.unwrap();
        let set = cache.get(1024).await.unwrap().unwrap();
        assert_eq!(set, HashSet::from([11, 12]));
        let set = cache.get(1025).await.unwrap().unwrap();
        assert_eq!(set, HashSet::from([11, 12]));
    }
}
