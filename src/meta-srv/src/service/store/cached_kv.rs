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

use api::v1::meta::{
    BatchDeleteRequest, BatchDeleteResponse, BatchGetRequest, BatchGetResponse, BatchPutRequest,
    BatchPutResponse, CompareAndPutRequest, CompareAndPutResponse, DeleteRangeRequest,
    DeleteRangeResponse, MoveValueRequest, MoveValueResponse, PutRequest, PutResponse,
    RangeRequest, RangeResponse,
};

use crate::error::Result;
use crate::service::store::ext::KvStoreExt;
use crate::service::store::kv::{KvStore, KvStoreRef, ResettableKvStore, ResettableKvStoreRef};
use crate::service::store::memory::MemStore;
use crate::service::store::txn::{Txn, TxnOp, TxnRequest, TxnResponse, TxnService};

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

/// A cache dedicated to a Leader node, in order to cache some metadata.
///
/// To use this cache, the following constraints must be followed:
///   1. The leader node can create this metadata.
///   2. The follower node can create this metadata. The leader node can lazily retrieve
///     the corresponding data through the caching loading mechanism.
///   3. Only the leader node can update this metadata, as the cache cannot detect
///     modifications made to the data on the follower node.
///   4. Only the leader node can delete this metadata for the same reason mentioned above.
pub struct LeaderCachedKvStore {
    check_leader: CheckLeaderRef,
    store: KvStoreRef,
    cache: ResettableKvStoreRef,
}

impl LeaderCachedKvStore {
    pub fn new(check_leader: CheckLeaderRef, store: KvStoreRef) -> Self {
        Self {
            check_leader,
            store,
            cache: Arc::new(MemStore::new()),
        }
    }

    /// With a leader checker which always returns true when checking,
    /// mainly used in test scenarios.
    pub fn with_always_leader(store: KvStoreRef) -> Self {
        Self::new(Arc::new(AlwaysLeader), store)
    }

    #[inline]
    fn is_leader(&self) -> bool {
        self.check_leader.check()
    }
}

#[async_trait::async_trait]
impl KvStore for LeaderCachedKvStore {
    async fn range(&self, req: RangeRequest) -> Result<RangeResponse> {
        if !self.is_leader() {
            return self.store.range(req).await;
        }

        // We can only cache for exact key queries (i.e. get requests)
        // because we cannot confirm if a range response is complete.
        if req.range_end.is_empty() {
            let res = self.cache.range(req.clone()).await?;
            if !res.kvs.is_empty() {
                return Ok(res);
            }

            let res = self.store.range(req.clone()).await?;
            if !res.kvs.is_empty() {
                let kv = res.kvs[0].clone();
                let put_req = PutRequest {
                    key: kv.key,
                    value: kv.value,
                    ..Default::default()
                };
                let _ = self.cache.put(put_req).await?;
            }
            return Ok(res);
        }

        self.store.range(req).await
    }

    async fn put(&self, req: PutRequest) -> Result<PutResponse> {
        if !self.is_leader() {
            return self.store.put(req).await;
        }

        let res = self.store.put(req.clone()).await?;
        let _ = self.cache.put(req).await?;
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
        let missed_keys = req
            .keys
            .iter()
            .filter(|key| !hit_keys.contains(*key))
            .cloned()
            .collect::<Vec<_>>();
        let remote_req = BatchGetRequest {
            keys: missed_keys,
            ..Default::default()
        };
        let remote_res = self.store.batch_get(remote_req).await?;

        let put_req = BatchPutRequest {
            kvs: remote_res.kvs.clone(),
            ..Default::default()
        };
        let _ = self.cache.batch_put(put_req).await?;

        let mut merged_res = cached_res;
        merged_res.kvs.extend(remote_res.kvs);
        Ok(merged_res)
    }

    async fn batch_put(&self, req: BatchPutRequest) -> Result<BatchPutResponse> {
        if !self.is_leader() {
            return self.store.batch_put(req).await;
        }

        let res = self.store.batch_put(req.clone()).await?;
        let _ = self.cache.batch_put(req).await?;
        Ok(res)
    }

    async fn batch_delete(&self, req: BatchDeleteRequest) -> Result<BatchDeleteResponse> {
        if !self.is_leader() {
            return self.store.batch_delete(req).await;
        }

        let res = self.store.batch_delete(req.clone()).await?;
        let _ = self.cache.batch_delete(req).await?;
        Ok(res)
    }

    async fn compare_and_put(&self, req: CompareAndPutRequest) -> Result<CompareAndPutResponse> {
        if !self.is_leader() {
            return self.store.compare_and_put(req).await;
        }

        let res = self.store.compare_and_put(req.clone()).await?;
        // Delete key in the cache.
        //
        // Cache can not deal with the cas operation, because it does
        // not contain full data, so we need to delete the key.
        let _ = self.cache.delete(req.key, false).await?;
        Ok(res)
    }

    async fn delete_range(&self, req: DeleteRangeRequest) -> Result<DeleteRangeResponse> {
        if !self.is_leader() {
            return self.store.delete_range(req).await;
        }

        let res = self.store.delete_range(req.clone()).await?;
        let _ = self.cache.delete_range(req).await?;
        Ok(res)
    }

    async fn move_value(&self, req: MoveValueRequest) -> Result<MoveValueResponse> {
        if !self.is_leader() {
            return self.store.move_value(req).await;
        }

        let res = self.store.move_value(req.clone()).await?;
        let MoveValueRequest {
            from_key, to_key, ..
        } = req;
        // Delete all keys in the cache.
        //
        // Cache can not deal with the move operation, because it does
        // not contain full data, so we need to delete both keys.
        let batch_delete_req = BatchDeleteRequest {
            keys: vec![from_key, to_key],
            ..Default::default()
        };
        let _ = self.cache.batch_delete(batch_delete_req).await?;
        Ok(res)
    }
}

#[async_trait::async_trait]
impl TxnService for LeaderCachedKvStore {
    async fn txn(&self, txn: Txn) -> Result<TxnResponse> {
        if !self.is_leader() {
            return self.store.txn(txn).await;
        }

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
        for txn_op in all {
            match txn_op {
                TxnOp::Put(key, _) => {
                    let _ = self.cache.delete(key, false).await?;
                }
                TxnOp::Delete(key) => {
                    let _ = self.cache.delete(key, false).await?;
                }
                TxnOp::Get(_) => {}
            }
        }
        Ok(res)
    }
}

impl ResettableKvStore for LeaderCachedKvStore {
    fn reset(&self) {
        self.cache.reset()
    }
}

#[cfg(test)]
mod tests {
    use api::v1::meta::KeyValue;

    use super::*;
    use crate::service::store::memory::MemStore;

    fn create_leader_cached_kv_store() -> LeaderCachedKvStore {
        let store = Arc::new(MemStore::new());
        LeaderCachedKvStore::with_always_leader(store)
    }

    #[tokio::test]
    async fn test_get_put_delete() {
        let cached_store = create_leader_cached_kv_store();
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

        let cached_value = inner_cache.get(key.clone()).await.unwrap();
        assert!(cached_value.is_none());

        let cached_value = cached_store.get(key.clone()).await.unwrap().unwrap();
        assert_eq!(cached_value.value, value);

        let cached_value = inner_cache.get(key.clone()).await.unwrap().unwrap();
        assert_eq!(cached_value.value, value);

        let res = cached_store
            .delete(key.clone(), true)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(res.value, value);

        let cached_value = inner_cache.get(key.clone()).await.unwrap();
        assert!(cached_value.is_none());
    }

    #[tokio::test]
    async fn test_batch_get_put_delete() {
        let cached_store = create_leader_cached_kv_store();
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

        let batch_get_req = BatchGetRequest {
            keys,
            ..Default::default()
        };

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
        let cached_store = create_leader_cached_kv_store();
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
        let batch_get_req = BatchGetRequest {
            keys,
            ..Default::default()
        };
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
