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
use common_meta::range_stream::{DEFAULT_PAGE_SIZE, PaginationStream};
use common_meta::rpc::KeyValue;
use common_meta::rpc::store::{
    BatchDeleteRequest, BatchDeleteResponse, BatchGetRequest, BatchGetResponse, BatchPutRequest,
    BatchPutResponse, CompareAndPutRequest, CompareAndPutResponse, DeleteRangeRequest,
    DeleteRangeResponse, PutRequest, PutResponse, RangeRequest, RangeResponse,
};
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
    /// Incremented before and after each store mutation, also while the cache is
    /// disabled. The first increment detects overlapping writers; the second
    /// invalidates cache fills and loads that read the old value while the
    /// store mutation was in flight.
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

    /// Loads the cached prefixes. Writes may run meanwhile: a prefix whose load overlapped a
    /// write is dropped from the cache again, and read through later.
    pub async fn load(&self) -> Result<()> {
        for prefix in &CACHE_KEY_PREFIXES[..] {
            let _timer =
                metrics::METRIC_META_LEADER_CACHED_KV_LOAD_ELAPSED.with_label_values(&[prefix]);
            let ver = self.get_version();

            // TODO(weny): Refactors PaginationStream's output to unary output.
            let stream = PaginationStream::new(
                self.store.clone(),
                RangeRequest::new().with_prefix(prefix.as_bytes()),
                DEFAULT_PAGE_SIZE,
                Ok,
            )
            .into_stream();

            let kvs = stream.try_collect::<Vec<_>>().await?;
            let keys = kvs.iter().map(|kv| kv.key.clone()).collect::<Vec<_>>();

            self.cache
                .batch_put(BatchPutRequest {
                    kvs,
                    prev_kv: false,
                })
                .await?;
            // A write while loading may have changed what was read.
            if !self.validate_version(ver) {
                self.invalid_keys(keys).await?;
            }
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
        let ver = self.create_new_version();

        let res = self.store.put(req.clone()).await?;
        self.create_new_version();
        let _ = self.cache.put(req.clone()).await?;

        // Only our own completion increment may follow the starting version.
        if !self.validate_version(ver + 1) {
            self.invalid_key(req.key).await?;
        }

        Ok(res)
    }

    async fn batch_put(&self, req: BatchPutRequest) -> Result<BatchPutResponse> {
        let ver = self.create_new_version();

        let res = self.store.batch_put(req.clone()).await?;
        self.create_new_version();
        let _ = self.cache.batch_put(req.clone()).await?;

        // Only our own completion increment may follow the starting version.
        if !self.validate_version(ver + 1) {
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
        let hit_keys = cached_res
            .kvs
            .iter()
            .map(|kv| kv.key.clone())
            .collect::<HashSet<_>>();

        metrics::METRIC_META_KV_CACHE_HIT
            .with_label_values(&["batch_get"])
            .inc_by(hit_keys.len() as u64);

        // The cache hit all keys
        if cached_res.kvs.len() == req.keys.len() {
            return Ok(cached_res);
        }

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
            kvs: remote_res.kvs.clone().into_iter().collect(),
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
        let _ = self.create_new_version();

        let key = req.key.clone();
        let res = self.store.compare_and_put(req).await?;
        self.create_new_version();
        // Delete key in the cache.
        //
        // Cache can not deal with the CAS operation, because it does
        // not contain full data, so we need to delete the key.
        self.invalid_key(key).await?;
        Ok(res)
    }

    async fn delete_range(&self, req: DeleteRangeRequest) -> Result<DeleteRangeResponse> {
        let _ = self.create_new_version();

        let res = self.store.delete_range(req.clone()).await?;
        self.create_new_version();
        let _ = self.cache.delete_range(req).await?;
        Ok(res)
    }

    async fn batch_delete(&self, req: BatchDeleteRequest) -> Result<BatchDeleteResponse> {
        let _ = self.create_new_version();

        let res = self.store.batch_delete(req.clone()).await?;
        self.create_new_version();
        let _ = self.cache.batch_delete(req).await?;
        Ok(res)
    }
}

#[async_trait::async_trait]
impl TxnService for LeaderCachedKvBackend {
    type Error = Error;

    async fn txn(&self, txn: Txn) -> Result<TxnResponse> {
        let _ = self.create_new_version();

        let res = self.store.txn(txn.clone()).await?;
        self.create_new_version();
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
        self.create_new_version();
        self.cache.reset()
    }

    fn as_kv_backend_ref(self: Arc<Self>) -> KvBackendRef<Self::Error> {
        self
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use common_meta::kv_backend::txn::{Compare, CompareOp};
    use common_meta::rpc::KeyValue;
    use futures::poll;
    use tokio::sync::oneshot;

    use super::*;

    /// Pauses one backend call at a known point, without relying on task scheduling.
    #[derive(Default)]
    struct Pause {
        receiver: Mutex<Option<oneshot::Receiver<()>>>,
    }

    impl Pause {
        fn arm(&self) -> oneshot::Sender<()> {
            let (sender, receiver) = oneshot::channel();
            assert!(self.receiver.lock().unwrap().replace(receiver).is_none());
            sender
        }

        async fn wait(&self) {
            let receiver = self.receiver.lock().unwrap().take();
            if let Some(receiver) = receiver {
                receiver.await.unwrap();
            }
        }
    }

    #[derive(Default)]
    struct ControlledBackend {
        inner: MemoryKvBackend<Error>,
        before_write: Pause,
        after_write: Pause,
        after_read: Pause,
    }

    #[async_trait::async_trait]
    impl TxnService for ControlledBackend {
        type Error = Error;

        async fn txn(&self, txn: Txn) -> Result<TxnResponse> {
            self.before_write.wait().await;
            let res = self.inner.txn(txn).await;
            self.after_write.wait().await;
            res
        }

        fn max_txn_ops(&self) -> usize {
            self.inner.max_txn_ops()
        }
    }

    #[async_trait::async_trait]
    impl KvBackend for ControlledBackend {
        fn name(&self) -> &str {
            "ControlledBackend"
        }

        fn as_any(&self) -> &dyn Any {
            self
        }

        async fn range(&self, req: RangeRequest) -> Result<RangeResponse> {
            let res = self.inner.range(req).await;
            self.after_read.wait().await;
            res
        }

        async fn batch_get(&self, req: BatchGetRequest) -> Result<BatchGetResponse> {
            let res = self.inner.batch_get(req).await;
            self.after_read.wait().await;
            res
        }

        async fn put(&self, req: PutRequest) -> Result<PutResponse> {
            self.before_write.wait().await;
            let res = self.inner.put(req).await;
            self.after_write.wait().await;
            res
        }

        async fn batch_put(&self, req: BatchPutRequest) -> Result<BatchPutResponse> {
            self.before_write.wait().await;
            let res = self.inner.batch_put(req).await;
            self.after_write.wait().await;
            res
        }

        async fn delete_range(&self, req: DeleteRangeRequest) -> Result<DeleteRangeResponse> {
            self.before_write.wait().await;
            let res = self.inner.delete_range(req).await;
            self.after_write.wait().await;
            res
        }

        async fn batch_delete(&self, req: BatchDeleteRequest) -> Result<BatchDeleteResponse> {
            self.before_write.wait().await;
            let res = self.inner.batch_delete(req).await;
            self.after_write.wait().await;
            res
        }
    }

    #[derive(Clone, Copy, Debug)]
    enum Write {
        Txn,
        TxnFailure,
        Put,
        BatchPut,
        CompareAndPut,
        DeleteRange,
        BatchDelete,
    }

    const WRITES: [Write; 7] = [
        Write::Txn,
        Write::TxnFailure,
        Write::Put,
        Write::BatchPut,
        Write::CompareAndPut,
        Write::DeleteRange,
        Write::BatchDelete,
    ];
    /// Under the first prefix the leader cache loads.
    const KEY: &[u8] = b"__table_name/1";

    impl Write {
        async fn execute(self, store: &LeaderCachedKvBackend, value: &[u8]) {
            match self {
                Self::Txn => {
                    store
                        .txn(Txn::new().and_then(vec![TxnOp::Put(KEY.to_vec(), value.to_vec())]))
                        .await
                        .unwrap();
                }
                Self::TxnFailure => {
                    let res = store
                        .txn(
                            Txn::new()
                                .when(vec![Compare::with_value_not_exists(
                                    KEY.to_vec(),
                                    CompareOp::Equal,
                                )])
                                .or_else(vec![TxnOp::Put(KEY.to_vec(), value.to_vec())]),
                        )
                        .await
                        .unwrap();
                    assert!(!res.succeeded);
                }
                Self::Put => {
                    store
                        .put(PutRequest::new().with_key(KEY).with_value(value))
                        .await
                        .unwrap();
                }
                Self::BatchPut => {
                    store
                        .batch_put(BatchPutRequest {
                            kvs: vec![KeyValue {
                                key: KEY.to_vec(),
                                value: value.to_vec(),
                            }],
                            ..Default::default()
                        })
                        .await
                        .unwrap();
                }
                Self::CompareAndPut => {
                    let res = store
                        .compare_and_put(
                            CompareAndPutRequest::new()
                                .with_key(KEY)
                                .with_expect(b"old")
                                .with_value(value),
                        )
                        .await
                        .unwrap();
                    assert!(res.success);
                }
                Self::DeleteRange => {
                    store
                        .delete_range(DeleteRangeRequest::new().with_key(KEY))
                        .await
                        .unwrap();
                }
                Self::BatchDelete => {
                    store
                        .batch_delete(BatchDeleteRequest {
                            keys: vec![KEY.to_vec()],
                            prev_kv: false,
                        })
                        .await
                        .unwrap();
                }
            }
        }
    }

    async fn read(store: &LeaderCachedKvBackend, batch: bool) -> Option<KeyValue> {
        if batch {
            store
                .batch_get(BatchGetRequest {
                    keys: vec![KEY.to_vec()],
                })
                .await
                .unwrap()
                .kvs
                .pop()
        } else {
            store.get(KEY).await.unwrap()
        }
    }

    /// A cache miss reads the old value after a writer starts, then returns after
    /// the writer has committed and updated/invalidated the cache (#9548).
    #[tokio::test]
    async fn test_cache_miss_overlaps_write() {
        for write in WRITES {
            for batch in [true, false] {
                let backing = Arc::new(ControlledBackend::default());
                backing
                    .inner
                    .put(PutRequest::new().with_key(KEY).with_value(b"old"))
                    .await
                    .unwrap();
                let store = LeaderCachedKvBackend::with_always_leader(backing.clone());

                let commit = backing.before_write.arm();
                let writer = write.execute(&store, b"new");
                tokio::pin!(writer);
                assert!(poll!(writer.as_mut()).is_pending());

                let return_read = backing.after_read.arm();
                let reader = read(&store, batch);
                tokio::pin!(reader);
                assert!(poll!(reader.as_mut()).is_pending());

                commit.send(()).unwrap();
                writer.await;
                return_read.send(()).unwrap();
                assert_eq!(reader.await.unwrap().value(), b"old");

                let durable = backing.inner.get(KEY).await.unwrap();
                assert_eq!(
                    read(&store, batch).await,
                    durable,
                    "{write:?}, batch={batch}"
                );
            }
        }
    }

    /// Start order, commit order and response order can all differ. An older
    /// put response must not overwrite the cache after a newer mutation.
    #[tokio::test]
    async fn test_overlapping_writes() {
        for first in [Write::Put, Write::BatchPut] {
            for second in WRITES {
                for reverse_commit in [false, true] {
                    let backing = Arc::new(ControlledBackend::default());
                    backing
                        .inner
                        .put(PutRequest::new().with_key(KEY).with_value(b"old"))
                        .await
                        .unwrap();
                    let store = LeaderCachedKvBackend::with_always_leader(backing.clone());

                    let resume = if reverse_commit {
                        backing.before_write.arm()
                    } else {
                        backing.after_write.arm()
                    };
                    // Keep the initial value so the second CAS succeeds.
                    let writer = first.execute(&store, b"old");
                    tokio::pin!(writer);
                    assert!(poll!(writer.as_mut()).is_pending());
                    second.execute(&store, b"new").await;
                    resume.send(()).unwrap();
                    writer.await;

                    let durable = backing.inner.get(KEY).await.unwrap();
                    assert_eq!(
                        read(&store, true).await,
                        durable,
                        "{first:?}, {second:?}, reverse_commit={reverse_commit}"
                    );
                }
            }
        }
    }

    /// The later-started put commits first but returns last, after its stored
    /// value has already been replaced by the first writer.
    #[tokio::test]
    async fn test_write_response_reordering() {
        for first in WRITES {
            for second in [Write::Put, Write::BatchPut] {
                let backing = Arc::new(ControlledBackend::default());
                backing
                    .inner
                    .put(PutRequest::new().with_key(KEY).with_value(b"old"))
                    .await
                    .unwrap();
                let store = LeaderCachedKvBackend::with_always_leader(backing.clone());

                let commit_first = backing.before_write.arm();
                let first_writer = first.execute(&store, b"new");
                tokio::pin!(first_writer);
                assert!(poll!(first_writer.as_mut()).is_pending());

                let return_second = backing.after_write.arm();
                let second_writer = second.execute(&store, b"old");
                tokio::pin!(second_writer);
                assert!(poll!(second_writer.as_mut()).is_pending());

                commit_first.send(()).unwrap();
                first_writer.await;
                return_second.send(()).unwrap();
                second_writer.await;

                let durable = backing.inner.get(KEY).await.unwrap();
                assert_eq!(read(&store, true).await, durable, "{first:?}, {second:?}");
            }
        }
    }

    struct EnableCache(std::sync::atomic::AtomicBool);

    impl CheckLeader for EnableCache {
        fn check(&self) -> bool {
            self.0.load(Ordering::Relaxed)
        }
    }

    /// A write made while the cache is disabled overlaps the load: the load reads the old value
    /// either before the write commits and installs it afterwards, or while the write is in
    /// flight and completes, with the cache enabled, before the write commits.
    #[tokio::test]
    async fn test_load_overlaps_write() {
        for write in WRITES {
            for write_first in [false, true] {
                let backing = Arc::new(ControlledBackend::default());
                backing
                    .inner
                    .put(PutRequest::new().with_key(KEY).with_value(b"old"))
                    .await
                    .unwrap();
                let enabled = Arc::new(EnableCache(Default::default()));
                let store = LeaderCachedKvBackend::new(enabled.clone(), backing.clone());

                if write_first {
                    let commit = backing.before_write.arm();
                    let writer = write.execute(&store, b"new");
                    tokio::pin!(writer);
                    assert!(poll!(writer.as_mut()).is_pending());
                    store.load().await.unwrap();
                    enabled.0.store(true, Ordering::Relaxed);
                    commit.send(()).unwrap();
                    writer.await;
                } else {
                    let return_read = backing.after_read.arm();
                    let loader = store.load();
                    tokio::pin!(loader);
                    assert!(poll!(loader.as_mut()).is_pending());
                    write.execute(&store, b"new").await;
                    return_read.send(()).unwrap();
                    loader.await.unwrap();
                    enabled.0.store(true, Ordering::Relaxed);
                }

                let durable = backing.inner.get(KEY).await.unwrap();
                assert_eq!(
                    read(&store, false).await,
                    durable,
                    "{write:?}, write_first={write_first}"
                );
            }
        }
    }

    #[tokio::test]
    async fn test_cache_miss_overlaps_reset() {
        for batch in [true, false] {
            let backing = Arc::new(ControlledBackend::default());
            backing
                .inner
                .put(PutRequest::new().with_key(KEY).with_value(b"old"))
                .await
                .unwrap();
            let store = LeaderCachedKvBackend::with_always_leader(backing.clone());

            let return_read = backing.after_read.arm();
            let reader = read(&store, batch);
            tokio::pin!(reader);
            assert!(poll!(reader.as_mut()).is_pending());
            store.reset();
            return_read.send(()).unwrap();
            reader.await;
            assert!(store.cache.get(KEY).await.unwrap().is_none());
        }
    }

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
