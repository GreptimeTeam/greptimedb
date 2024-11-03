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

use std::any::Any;
use std::collections::HashSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};

use common_meta::error::{Error, Result};
use common_meta::key::CACHE_KEY_PREFIXES;
use common_meta::kv_backend::memory::MemoryKvBackend;
use common_meta::kv_backend::txn::{Txn, TxnOp, TxnRequest, TxnResponse};
use common_meta::kv_backend::{
    KvBackend, KvBackendRef, ResettableKvBackend, ResettableKvBackendRef, TxnService,
};
use common_meta::range_stream::{PaginationStream, DEFAULT_PAGE_SIZE};
use common_meta::rpc::store::{
    BatchDeleteRequest, BatchDeleteResponse, BatchGetRequest, BatchGetResponse, BatchPutRequest,
    BatchPutResponse, CompareAndPutRequest, CompareAndPutResponse, DeleteRangeRequest,
    DeleteRangeResponse, PutRequest, PutResponse, RangeRequest, RangeResponse,
};
use common_meta::rpc::KeyValue;
use futures::TryStreamExt;

use crate::metrics;
use crate::state::State;

pub type CheckLeaderRef = Arc<dyn CheckLeader>;

pub trait CheckLeader: Sync + Send {
    fn check(&self) -> bool;
}

struct AlwaysLeader;

impl CheckLeader for AlwaysLeader {
    fn check(&self) -> bool {
        true
    }
}

impl CheckLeader for RwLock<State> {
    fn check(&self) -> bool {
        self.read().unwrap().enable_leader_cache()
    }
}

/// A cache dedicated to a Leader node, in order to cache some metadata.
///
/// To use this cache, the following constraints must be followed:
///   1. The leader node can create this metadata.
///   2. The follower node can create this metadata. The leader node can lazily retrieve
///      the corresponding data through the caching loading mechanism.
///   3. Only the leader node can update this metadata, as the cache cannot detect
///      modifications made to the data on the follower node.
///   4. Only the leader node can delete this metadata for the same reason mentioned above.
pub struct LeaderCachedKvBackend {
    check_leader: CheckLeaderRef,
    store: KvBackendRef,
    cache: ResettableKvBackendRef,
    version: AtomicUsize,
    name: String,
}

impl LeaderCachedKvBackend {
    pub fn new(check_leader: CheckLeaderRef, store: KvBackendRef) -> Self {
        let name = format!("LeaderCached({})", store.name());
        Self {
            check_leader,
            store,
            cache: Arc::new(MemoryKvBackend::new()),
            version: AtomicUsize::new(0),
            name,
        }
    }

    /// With a leader checker which always returns true when checking,
    /// mainly used in test scenarios.
    pub fn with_always_leader(store: KvBackendRef) -> Self {
        Self::new(Arc::new(AlwaysLeader), store)
    }

    /// The caller MUST ensure during the loading, there are no mutation requests reaching the `LeaderCachedKvStore`.
    pub async fn load(&self) -> Result<()> {
        for prefix in &CACHE_KEY_PREFIXES[..] {
            let _timer =
                metrics::METRIC_META_LEADER_CACHED_KV_LOAD_ELAPSED.with_label_values(&[prefix]);

            // TODO(weny): Refactors PaginationStream's output to unary output.
            let stream = PaginationStream::new(
                self.store.clone(),
                RangeRequest::new().with_prefix(prefix.as_bytes()),
                DEFAULT_PAGE_SIZE,
                Arc::new(Ok),
            )
            .into_stream();

            let kvs = stream.try_collect::<Vec<_>>().await?;

            self.cache
                .batch_put(BatchPutRequest {
                    kvs,
                    prev_kv: false,
                })
                .await?;
        }

        Ok(())
    }

    #[inline]
    fn is_leader(&self) -> bool {
        self.check_leader.check()
    }

    #[inline]
    async fn invalid_key(&self, key: Vec<u8>) -> Result<()> {
        let _ = self.cache.delete(&key, false).await?;
        Ok(())
    }

    #[inline]
    async fn invalid_keys(&self, keys: Vec<Vec<u8>>) -> Result<()> {
        let txn = Txn::new().and_then(keys.into_iter().map(TxnOp::Delete).collect::<Vec<_>>());
        let _ = self.cache.txn(txn).await?;
        Ok(())
    }

    #[inline]
    fn get_version(&self) -> usize {
        self.version.load(Ordering::Relaxed)
    }

    #[inline]
    fn create_new_version(&self) -> usize {
        self.version.fetch_add(1, Ordering::Relaxed) + 1
    }

    #[inline]
    fn validate_version(&self, version: usize) -> bool {
        version == self.version.load(Ordering::Relaxed)
    }
}

#[async_trait::async_trait]
impl KvBackend for LeaderCachedKvBackend {
    fn name(&self) -> &str {
        &self.name
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn range(&self, req: RangeRequest) -> Result<RangeResponse> {
        if !self.is_leader() {
            return self.store.range(req).await;
        }

        // We can only cache for exact key queries (i.e. get requests)
        // because we cannot confirm if a range response is complete.
        if !req.range_end.is_empty() {
            return self.store.range(req).await;
        }

        let res = self.cache.range(req.clone()).await?;
        if !res.kvs.is_empty() {
            return Ok(res);
        }

        let ver = self.get_version();

        let res = self
            .store
            .range(RangeRequest {
                // ignores `keys_only`
                keys_only: false,
                ..req.clone()
            })
            .await?;
        if !res.kvs.is_empty() {
            let KeyValue { key, value } = res.kvs[0].clone();
            let put_req = PutRequest {
                key: key.clone(),
                value,
                ..Default::default()
            };
            let _ = self.cache.put(put_req).await?;

            if !self.validate_version(ver) {
                self.invalid_key(key).await?;
            }
        }

        return Ok(res);
    }

    async fn put(&self, req: PutRequest) -> Result<PutResponse> {
        if !self.is_leader() {
            return self.store.put(req).await;
        }

        let ver = self.create_new_version();

        let res = self.store.put(req.clone()).await?;
        let _ = self.cache.put(req.clone()).await?;

        if !self.validate_version(ver) {
            self.invalid_key(req.key).await?;
        }

        Ok(res)
    }

    async fn batch_put(&self, req: BatchPutRequest) -> Result<BatchPutResponse> {
        if !self.is_leader() {
            return self.store.batch_put(req).await;
        }

        let ver = self.create_new_version();

        let res = self.store.batch_put(req.clone()).await?;
        let _ = self.cache.batch_put(req.clone()).await?;

        if !self.validate_version(ver) {
            let keys = req.kvs.into_iter().map(|kv| kv.key).collect::<Vec<_>>();
            self.invalid_keys(keys).await?;
        }

        Ok(res)
    }

    async fn batch_get(&self, req: BatchGetRequest) -> Result<BatchGetResponse> {
        if !self.is_leader() {
            return self.store.batch_get(req).await;
        }

        let cached_res = self.cache.batch_get(req.clone()).await?;
        // The cache hit all keys
        if cached_res.kvs.len() == req.keys.len() {
            return Ok(cached_res);
        }

        let hit_keys = cached_res
            .kvs
            .iter()
            .map(|kv| kv.key.clone())
            .collect::<HashSet<_>>();

        metrics::METRIC_META_KV_CACHE_HIT
            .with_label_values(&["batch_get"])
            .inc_by(hit_keys.len() as u64);

        let missed_keys = req
            .keys
            .iter()
            .filter(|key| !hit_keys.contains(*key))
            .cloned()
            .collect::<Vec<_>>();
        metrics::METRIC_META_KV_CACHE_MISS
            .with_label_values(&["batch_get"])
            .inc_by(missed_keys.len() as u64);

        let remote_req = BatchGetRequest { keys: missed_keys };

        let ver = self.get_version();

        let remote_res = self.store.batch_get(remote_req).await?;
        let put_req = BatchPutRequest {
            kvs: remote_res.kvs.clone().into_iter().map(Into::into).collect(),
            ..Default::default()
        };
        let _ = self.cache.batch_put(put_req).await?;

        if !self.validate_version(ver) {
            let keys = remote_res
                .kvs
                .iter()
                .map(|kv| kv.key().to_vec())
                .collect::<Vec<_>>();
            self.invalid_keys(keys).await?;
        }

        let mut merged_res = cached_res;
        merged_res.kvs.extend(remote_res.kvs);
        Ok(merged_res)
    }

    async fn compare_and_put(&self, req: CompareAndPutRequest) -> Result<CompareAndPutResponse> {
        if !self.is_leader() {
            return self.store.compare_and_put(req).await;
        }

        let _ = self.create_new_version();

        let key = req.key.clone();
        let res = self.store.compare_and_put(req).await?;
        // Delete key in the cache.
        //
        // Cache can not deal with the CAS operation, because it does
        // not contain full data, so we need to delete the key.
        self.invalid_key(key).await?;
        Ok(res)
    }

    async fn delete_range(&self, req: DeleteRangeRequest) -> Result<DeleteRangeResponse> {
        if !self.is_leader() {
            return self.store.delete_range(req).await;
        }

        let _ = self.create_new_version();

        let res = self.store.delete_range(req.clone()).await?;
        let _ = self.cache.delete_range(req).await?;
        Ok(res)
    }

    async fn batch_delete(&self, req: BatchDeleteRequest) -> Result<BatchDeleteResponse> {
        if !self.is_leader() {
            return self.store.batch_delete(req).await;
        }

        let _ = self.create_new_version();

        let res = self.store.batch_delete(req.clone()).await?;
        let _ = self.cache.batch_delete(req).await?;
        Ok(res)
    }
}

#[async_trait::async_trait]
impl TxnService for LeaderCachedKvBackend {
    type Error = Error;

    async fn txn(&self, txn: Txn) -> Result<TxnResponse> {
        if !self.is_leader() {
            return self.store.txn(txn).await;
        }

        let _ = self.create_new_version();

        let res = self.store.txn(txn.clone()).await?;
        let TxnRequest {
            success, failure, ..
        } = txn.into();
        let mut all = success;
        all.extend(failure);
        // Delete all keys in the cache.
        //
        // Cache can not deal with the txn operation, because it does
        // not contain full data, so we need to delete both keys.
        let mut keys = Vec::with_capacity(all.len());
        for txn_op in all {
            match txn_op {
                TxnOp::Put(key, _) => {
                    keys.push(key);
                }
                TxnOp::Delete(key) => {
                    keys.push(key);
                }
                TxnOp::Get(_) => {}
            }
        }
        self.invalid_keys(keys).await?;

        Ok(res)
    }

    fn max_txn_ops(&self) -> usize {
        self.store.max_txn_ops()
    }
}

impl ResettableKvBackend for LeaderCachedKvBackend {
    fn reset(&self) {
        self.cache.reset()
    }
}

#[cfg(test)]
mod tests {
    use common_meta::rpc::KeyValue;

    use super::*;

    fn create_leader_cached_kv_backend() -> LeaderCachedKvBackend {
        let store = Arc::new(MemoryKvBackend::new());
        LeaderCachedKvBackend::with_always_leader(store)
    }

    #[tokio::test]
    async fn test_get_put_delete() {
        let cached_store = create_leader_cached_kv_backend();
        let inner_store = cached_store.store.clone();
        let inner_cache = cached_store.cache.clone();

        let key = "test_key".to_owned().into_bytes();
        let value = "value".to_owned().into_bytes();

        let put_req = PutRequest {
            key: key.clone(),
            value: value.clone(),
            ..Default::default()
        };
        let _ = inner_store.put(put_req).await.unwrap();

        let cached_value = inner_cache.get(&key).await.unwrap();
        assert!(cached_value.is_none());

        let cached_value = cached_store.get(&key).await.unwrap().unwrap();
        assert_eq!(cached_value.value(), value);

        let cached_value = inner_cache.get(&key).await.unwrap().unwrap();
        assert_eq!(cached_value.value(), value);

        let res = cached_store.delete(&key, true).await.unwrap().unwrap();
        assert_eq!(res.value(), value);

        let cached_value = inner_cache.get(&key).await.unwrap();
        assert!(cached_value.is_none());
    }

    #[tokio::test]
    async fn test_batch_get_put_delete() {
        let cached_store = create_leader_cached_kv_backend();
        let inner_store = cached_store.store.clone();
        let inner_cache = cached_store.cache.clone();

        let kvs = (1..3)
            .map(|i| {
                let key = format!("test_key_{}", i).into_bytes();
                let value = format!("value_{}", i).into_bytes();
                KeyValue { key, value }
            })
            .collect::<Vec<_>>();

        let batch_put_req = BatchPutRequest {
            kvs: kvs.clone(),
            ..Default::default()
        };

        let _ = inner_store.batch_put(batch_put_req).await.unwrap();

        let keys = (1..5)
            .map(|i| format!("test_key_{}", i).into_bytes())
            .collect::<Vec<_>>();

        let batch_get_req = BatchGetRequest { keys };

        let cached_values = inner_cache.batch_get(batch_get_req.clone()).await.unwrap();
        assert!(cached_values.kvs.is_empty());

        let cached_values = cached_store.batch_get(batch_get_req.clone()).await.unwrap();
        assert_eq!(cached_values.kvs.len(), 2);

        let cached_values = inner_cache.batch_get(batch_get_req.clone()).await.unwrap();
        assert_eq!(cached_values.kvs.len(), 2);

        cached_store.reset();

        let cached_values = inner_cache.batch_get(batch_get_req).await.unwrap();
        assert!(cached_values.kvs.is_empty());
    }

    #[tokio::test]
    async fn test_txn() {
        let cached_store = create_leader_cached_kv_backend();
        let inner_cache = cached_store.cache.clone();

        let kvs = (1..5)
            .map(|i| {
                let key = format!("test_key_{}", i).into_bytes();
                let value = format!("value_{}", i).into_bytes();
                KeyValue { key, value }
            })
            .collect::<Vec<_>>();

        let batch_put_req = BatchPutRequest {
            kvs: kvs.clone(),
            ..Default::default()
        };
        let _ = cached_store.batch_put(batch_put_req).await.unwrap();

        let keys = (1..5)
            .map(|i| format!("test_key_{}", i).into_bytes())
            .collect::<Vec<_>>();
        let batch_get_req = BatchGetRequest { keys };
        let cached_values = inner_cache.batch_get(batch_get_req.clone()).await.unwrap();
        assert_eq!(cached_values.kvs.len(), 4);

        let put_ops = (1..5)
            .map(|i| {
                let key = format!("test_key_{}", i).into_bytes();
                let value = format!("value_{}", i).into_bytes();
                TxnOp::Put(key, value)
            })
            .collect::<Vec<_>>();
        let txn = Txn::new().and_then(put_ops);
        let _ = cached_store.txn(txn).await.unwrap();

        let cached_values = inner_cache.batch_get(batch_get_req).await.unwrap();
        assert!(cached_values.kvs.is_empty());
    }
}
