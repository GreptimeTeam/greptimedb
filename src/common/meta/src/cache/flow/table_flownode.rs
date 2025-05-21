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

use std::collections::HashMap;
use std::sync::Arc;

use futures::future::BoxFuture;
use moka::future::Cache;
use moka::ops::compute::Op;
use table::metadata::TableId;

use crate::cache::{CacheContainer, Initializer};
use crate::error::Result;
use crate::instruction::{CacheIdent, CreateFlow, DropFlow};
use crate::key::flow::{TableFlowManager, TableFlowManagerRef};
use crate::key::{FlowId, FlowPartitionId};
use crate::kv_backend::KvBackendRef;
use crate::peer::Peer;

/// Flow id&flow partition key
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FlowIdent {
    pub flow_id: FlowId,
    pub partition_id: FlowPartitionId,
}

impl FlowIdent {
    pub fn new(flow_id: FlowId, partition_id: FlowPartitionId) -> Self {
        Self {
            flow_id,
            partition_id,
        }
    }
}

/// cache for TableFlowManager, the table_id part is in the outer cache
/// include flownode_id, flow_id, partition_id mapping to Peer
type FlownodeFlowSet = Arc<HashMap<FlowIdent, Peer>>;

pub type TableFlownodeSetCacheRef = Arc<TableFlownodeSetCache>;

/// [TableFlownodeSetCache] caches the [TableId] to [FlownodeSet] mapping.
pub type TableFlownodeSetCache = CacheContainer<TableId, FlownodeFlowSet, CacheIdent>;

/// Constructs a [TableFlownodeSetCache].
pub fn new_table_flownode_set_cache(
    name: String,
    cache: Cache<TableId, FlownodeFlowSet>,
    kv_backend: KvBackendRef,
) -> TableFlownodeSetCache {
    let table_flow_manager = Arc::new(TableFlowManager::new(kv_backend));
    let init = init_factory(table_flow_manager);

    CacheContainer::new(name, cache, Box::new(invalidator), init, filter)
}

fn init_factory(table_flow_manager: TableFlowManagerRef) -> Initializer<TableId, FlownodeFlowSet> {
    Arc::new(move |&table_id| {
        let table_flow_manager = table_flow_manager.clone();
        Box::pin(async move {
            table_flow_manager
                .flows(table_id)
                .await
                .map(|flows| {
                    flows
                        .into_iter()
                        .map(|(key, value)| {
                            (
                                FlowIdent::new(key.flow_id(), key.partition_id()),
                                value.peer,
                            )
                        })
                        .collect::<HashMap<_, _>>()
                })
                // We must cache the `HashSet` even if it's empty,
                // to avoid future requests to the remote storage next time;
                // If the value is added to the remote storage,
                // we have a corresponding cache invalidation mechanism to invalidate `(Key, EmptyHashSet)`.
                .map(Arc::new)
                .map(Some)
        })
    })
}

async fn handle_create_flow(
    cache: &Cache<TableId, FlownodeFlowSet>,
    CreateFlow {
        flow_id,
        source_table_ids,
        partition_to_peer_mapping: flow_part2nodes,
    }: &CreateFlow,
) {
    for table_id in source_table_ids {
        let entry = cache.entry(*table_id);
        entry
            .and_compute_with(
                async |entry: Option<moka::Entry<u32, FlownodeFlowSet>>| match entry {
                    Some(entry) => {
                        let mut map = entry.into_value().as_ref().clone();
                        map.extend(
                            flow_part2nodes.iter().map(|(part, peer)| {
                                (FlowIdent::new(*flow_id, *part), peer.clone())
                            }),
                        );

                        Op::Put(Arc::new(map))
                    }
                    None => {
                        Op::Put(Arc::new(HashMap::from_iter(flow_part2nodes.iter().map(
                            |(part, peer)| (FlowIdent::new(*flow_id, *part), peer.clone()),
                        ))))
                    }
                },
            )
            .await;
    }
}

async fn handle_drop_flow(
    cache: &Cache<TableId, FlownodeFlowSet>,
    DropFlow {
        flow_id,
        source_table_ids,
        flow_part2node_id,
    }: &DropFlow,
) {
    for table_id in source_table_ids {
        let entry = cache.entry(*table_id);
        entry
            .and_compute_with(
                async |entry: Option<moka::Entry<u32, FlownodeFlowSet>>| match entry {
                    Some(entry) => {
                        let mut set = entry.into_value().as_ref().clone();
                        for (part, _node) in flow_part2node_id {
                            let key = FlowIdent::new(*flow_id, *part);
                            set.remove(&key);
                        }

                        Op::Put(Arc::new(set))
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

fn invalidator<'a>(
    cache: &'a Cache<TableId, FlownodeFlowSet>,
    ident: &'a CacheIdent,
) -> BoxFuture<'a, Result<()>> {
    Box::pin(async move {
        match ident {
            CacheIdent::CreateFlow(create_flow) => handle_create_flow(cache, create_flow).await,
            CacheIdent::DropFlow(drop_flow) => handle_drop_flow(cache, drop_flow).await,
            _ => {}
        }
        Ok(())
    })
}

fn filter(ident: &CacheIdent) -> bool {
    matches!(ident, CacheIdent::CreateFlow(_) | CacheIdent::DropFlow(_))
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap};
    use std::sync::Arc;

    use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
    use moka::future::CacheBuilder;
    use table::table_name::TableName;

    use crate::cache::flow::table_flownode::{new_table_flownode_set_cache, FlowIdent};
    use crate::instruction::{CacheIdent, CreateFlow, DropFlow};
    use crate::key::flow::flow_info::FlowInfoValue;
    use crate::key::flow::flow_route::FlowRouteValue;
    use crate::key::flow::FlowMetadataManager;
    use crate::kv_backend::memory::MemoryKvBackend;
    use crate::peer::Peer;

    #[tokio::test]
    async fn test_cache_empty_set() {
        let mem_kv = Arc::new(MemoryKvBackend::default());
        let cache = CacheBuilder::new(128).build();
        let cache = new_table_flownode_set_cache("test".to_string(), cache, mem_kv);
        let set = cache.get(1024).await.unwrap().unwrap();
        assert!(set.is_empty());
    }

    #[tokio::test]
    async fn test_get() {
        let mem_kv = Arc::new(MemoryKvBackend::default());
        let flownode_metadata_manager = FlowMetadataManager::new(mem_kv.clone());
        flownode_metadata_manager
            .create_flow_metadata(
                1024,
                FlowInfoValue {
                    source_table_ids: vec![1024, 1025],
                    sink_table_name: TableName {
                        catalog_name: DEFAULT_CATALOG_NAME.to_string(),
                        schema_name: DEFAULT_SCHEMA_NAME.to_string(),
                        table_name: "sink_table".to_string(),
                    },
                    flownode_ids: BTreeMap::from([(0, 1), (1, 2), (2, 3)]),
                    catalog_name: DEFAULT_CATALOG_NAME.to_string(),
                    query_context: None,
                    flow_name: "my_flow".to_string(),
                    raw_sql: "sql".to_string(),
                    expire_after: Some(300),
                    comment: "comment".to_string(),
                    options: Default::default(),
                    created_time: chrono::Utc::now(),
                    updated_time: chrono::Utc::now(),
                },
                (1..=3)
                    .map(|i| {
                        (
                            (i - 1) as u32,
                            FlowRouteValue {
                                peer: Peer::empty(i),
                            },
                        )
                    })
                    .collect::<Vec<_>>(),
            )
            .await
            .unwrap();
        let cache = CacheBuilder::new(128).build();
        let cache = new_table_flownode_set_cache("test".to_string(), cache, mem_kv);
        let set = cache.get(1024).await.unwrap().unwrap();
        assert_eq!(
            set.as_ref().clone(),
            HashMap::from_iter(
                (1..=3).map(|i| { (FlowIdent::new(1024, (i - 1) as u32), Peer::empty(i),) })
            )
        );
        let set = cache.get(1025).await.unwrap().unwrap();
        assert_eq!(
            set.as_ref().clone(),
            HashMap::from_iter(
                (1..=3).map(|i| { (FlowIdent::new(1024, (i - 1) as u32), Peer::empty(i),) })
            )
        );
        let result = cache.get(1026).await.unwrap().unwrap();
        assert_eq!(result.len(), 0);
    }

    #[tokio::test]
    async fn test_create_flow() {
        let mem_kv = Arc::new(MemoryKvBackend::default());
        let cache = CacheBuilder::new(128).build();
        let cache = new_table_flownode_set_cache("test".to_string(), cache, mem_kv);
        let ident = vec![CacheIdent::CreateFlow(CreateFlow {
            flow_id: 2001,
            source_table_ids: vec![1024, 1025],
            partition_to_peer_mapping: (1..=5).map(|i| (i as u32, Peer::empty(i + 1))).collect(),
        })];
        cache.invalidate(&ident).await.unwrap();
        let set = cache.get(1024).await.unwrap().unwrap();
        assert_eq!(set.len(), 5);
        let set = cache.get(1025).await.unwrap().unwrap();
        assert_eq!(set.len(), 5);
    }

    #[tokio::test]
    async fn test_replace_flow() {
        let mem_kv = Arc::new(MemoryKvBackend::default());
        let cache = CacheBuilder::new(128).build();
        let cache = new_table_flownode_set_cache("test".to_string(), cache, mem_kv);
        let ident = vec![CacheIdent::CreateFlow(CreateFlow {
            flow_id: 2001,
            source_table_ids: vec![1024, 1025],
            partition_to_peer_mapping: (1..=5).map(|i| (i as u32, Peer::empty(i + 1))).collect(),
        })];
        cache.invalidate(&ident).await.unwrap();
        let set = cache.get(1024).await.unwrap().unwrap();
        assert_eq!(set.len(), 5);
        let set = cache.get(1025).await.unwrap().unwrap();
        assert_eq!(set.len(), 5);

        let drop_then_create_flow = vec![
            CacheIdent::DropFlow(DropFlow {
                flow_id: 2001,
                source_table_ids: vec![1024, 1025],
                flow_part2node_id: (1..=5).map(|i| (i as u32, i + 1)).collect(),
            }),
            CacheIdent::CreateFlow(CreateFlow {
                flow_id: 2001,
                source_table_ids: vec![1026, 1027],
                partition_to_peer_mapping: (11..=15)
                    .map(|i| (i as u32, Peer::empty(i + 1)))
                    .collect(),
            }),
            CacheIdent::FlowId(2001),
        ];
        cache.invalidate(&drop_then_create_flow).await.unwrap();

        let set = cache.get(1024).await.unwrap().unwrap();
        assert!(set.is_empty());

        let expected = HashMap::from_iter(
            (11..=15).map(|i| (FlowIdent::new(2001, i as u32), Peer::empty(i + 1))),
        );
        let set = cache.get(1026).await.unwrap().unwrap();

        assert_eq!(set.as_ref().clone(), expected);

        let set = cache.get(1027).await.unwrap().unwrap();

        assert_eq!(set.as_ref().clone(), expected);
    }

    #[tokio::test]
    async fn test_drop_flow() {
        let mem_kv = Arc::new(MemoryKvBackend::default());
        let cache = CacheBuilder::new(128).build();
        let cache = new_table_flownode_set_cache("test".to_string(), cache, mem_kv);
        let ident = vec![
            CacheIdent::CreateFlow(CreateFlow {
                flow_id: 2001,
                source_table_ids: vec![1024, 1025],
                partition_to_peer_mapping: (1..=5)
                    .map(|i| (i as u32, Peer::empty(i + 1)))
                    .collect(),
            }),
            CacheIdent::CreateFlow(CreateFlow {
                flow_id: 2002,
                source_table_ids: vec![1024, 1025],
                partition_to_peer_mapping: (11..=12)
                    .map(|i| (i as u32, Peer::empty(i + 1)))
                    .collect(),
            }),
            // same flownode that hold multiple flows
            CacheIdent::CreateFlow(CreateFlow {
                flow_id: 2003,
                source_table_ids: vec![1024, 1025],
                partition_to_peer_mapping: (1..=5)
                    .map(|i| (i as u32, Peer::empty(i + 1)))
                    .collect(),
            }),
        ];
        cache.invalidate(&ident).await.unwrap();
        let set = cache.get(1024).await.unwrap().unwrap();
        assert_eq!(set.len(), 12);
        let set = cache.get(1025).await.unwrap().unwrap();
        assert_eq!(set.len(), 12);

        let ident = vec![CacheIdent::DropFlow(DropFlow {
            flow_id: 2001,
            source_table_ids: vec![1024, 1025],
            flow_part2node_id: (1..=5).map(|i| (i as u32, i + 1)).collect(),
        })];
        cache.invalidate(&ident).await.unwrap();
        let set = cache.get(1024).await.unwrap().unwrap();
        assert_eq!(
            set.as_ref().clone(),
            HashMap::from_iter(
                (11..=12)
                    .map(|i| (FlowIdent::new(2002, i as u32), Peer::empty(i + 1)))
                    .chain((1..=5).map(|i| (FlowIdent::new(2003, i as u32), Peer::empty(i + 1))))
            )
        );
        let set = cache.get(1025).await.unwrap().unwrap();
        assert_eq!(
            set.as_ref().clone(),
            HashMap::from_iter(
                (11..=12)
                    .map(|i| (FlowIdent::new(2002, i as u32), Peer::empty(i + 1)))
                    .chain((1..=5).map(|i| (FlowIdent::new(2003, i as u32), Peer::empty(i + 1))))
            )
        );
    }
}
