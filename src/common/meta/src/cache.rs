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

mod container;
mod flow;
mod registry;
mod table;

use std::collections::HashMap;
use std::hash::Hash;
use std::sync::Arc;

pub use container::{BatchInitializer, CacheContainer, Initializer, Invalidator, TokenFilter};
pub use flow::{
    FlowInfoCache, FlowInfoCacheRef, TableFlownodeSetCache, TableFlownodeSetCacheRef,
    load_shared_flow_infos, new_flow_info_cache, new_table_flownode_set_cache,
};
pub use registry::{
    CacheRegistry, CacheRegistryBuilder, CacheRegistryRef, LayeredCacheRegistry,
    LayeredCacheRegistryBuilder, LayeredCacheRegistryRef,
};
pub use table::{
    SchemaCache, SchemaCacheRef, TableInfoCache, TableInfoCacheRef, TableNameCache,
    TableNameCacheRef, TableRoute, TableRouteCache, TableRouteCacheRef, TableSchemaCache,
    TableSchemaCacheRef, ViewInfoCache, ViewInfoCacheRef, new_schema_cache, new_table_info_cache,
    new_table_name_cache, new_table_route_cache, new_table_schema_cache, new_view_info_cache,
};

fn share_batch_values<K: Eq + Hash, V>(values: HashMap<K, V>) -> HashMap<K, Arc<V>> {
    values
        .into_iter()
        .map(|(key, value)| (key, Arc::new(value)))
        .collect()
}

#[cfg(test)]
pub(crate) mod test_util {
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use moka::future::Cache;

    use crate::cache::CacheContainer;
    use crate::instruction::CacheIdent;
    use crate::kv_backend::KvBackendRef;
    use crate::kv_backend::test_util::MockKvBackendBuilder;
    use crate::rpc::KeyValue;
    use crate::rpc::store::BatchGetResponse;

    /// Exercises a real cache constructor with a backend that permits only the cold batch.
    pub(crate) async fn assert_batch_cache_reads<V: Send + Sync + 'static>(
        new_cache: impl FnOnce(
            String,
            Cache<(usize, u32), Arc<V>>,
            KvBackendRef,
        ) -> CacheContainer<u32, Arc<V>, CacheIdent>,
        fixture: impl Fn(u32) -> (Vec<u8>, Vec<u8>),
    ) {
        let records: BTreeMap<_, _> = [10, 11].into_iter().map(fixture).collect();
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = calls.clone();
        let backend = MockKvBackendBuilder::default()
            .max_txn_ops(128)
            .range_fn(Arc::new(|_| panic!("unexpected metadata point read")))
            .batch_get_fn(Arc::new(move |req| {
                let mut keys = req.keys;
                keys.sort();
                assert!(keys.iter().eq(records.keys()));
                observed.fetch_add(1, Ordering::Relaxed);
                Ok(BatchGetResponse {
                    kvs: keys
                        .into_iter()
                        .map(|key| KeyValue {
                            value: records[&key].clone(),
                            key,
                        })
                        .collect(),
                })
            }))
            .build()
            .unwrap();
        let cache = new_cache("test".into(), Cache::new(8), Arc::new(backend));
        for _ in 0..2 {
            let values = cache.get_many(&[11, 10, 11]).await.unwrap();
            assert_eq!(values.len(), 3);
            assert!(values.iter().all(Option::is_some));
            assert_eq!(calls.load(Ordering::Relaxed), 1);
        }
    }
}
