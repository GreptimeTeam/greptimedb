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
use std::fmt::Debug;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use common_error::ext::BoxedError;
use common_meta::cache_invalidator::KvCacheInvalidator;
use common_meta::error::Error::CacheNotGet;
use common_meta::error::{CacheNotGetSnafu, Error, ExternalSnafu, GetKvCacheSnafu, Result};
use common_meta::kv_backend::{KvBackend, KvBackendRef, TxnService};
use common_meta::rpc::store::{
    BatchDeleteRequest, BatchDeleteResponse, BatchGetRequest, BatchGetResponse, BatchPutRequest,
    BatchPutResponse, CompareAndPutRequest, CompareAndPutResponse, DeleteRangeRequest,
    DeleteRangeResponse, PutRequest, PutResponse, RangeRequest, RangeResponse,
};
use common_meta::rpc::KeyValue;
use common_telemetry::debug;
use meta_client::client::MetaClient;
use moka::future::{Cache, CacheBuilder};
use snafu::{OptionExt, ResultExt};

use crate::metrics::{
    METRIC_CATALOG_KV_BATCH_GET, METRIC_CATALOG_KV_GET, METRIC_CATALOG_KV_REMOTE_GET,
};

const DEFAULT_CACHE_MAX_CAPACITY: u64 = 10000;
const DEFAULT_CACHE_TTL: Duration = Duration::from_secs(10 * 60);
const DEFAULT_CACHE_TTI: Duration = Duration::from_secs(5 * 60);

pub struct CachedKvBackendBuilder {
    cache_max_capacity: Option<u64>,
    cache_ttl: Option<Duration>,
    cache_tti: Option<Duration>,
    inner: KvBackendRef,
}

impl CachedKvBackendBuilder {
    pub fn new(inner: KvBackendRef) -> Self {
        Self {
            cache_max_capacity: None,
            cache_ttl: None,
            cache_tti: None,
            inner,
        }
    }

    pub fn cache_max_capacity(mut self, cache_max_capacity: u64) -> Self {
        self.cache_max_capacity.replace(cache_max_capacity);
        self
    }

    pub fn cache_ttl(mut self, cache_ttl: Duration) -> Self {
        self.cache_ttl.replace(cache_ttl);
        self
    }

    pub fn cache_tti(mut self, cache_tti: Duration) -> Self {
        self.cache_tti.replace(cache_tti);
        self
    }

    pub fn build(self) -> CachedKvBackend {
        let cache_max_capacity = self
            .cache_max_capacity
            .unwrap_or(DEFAULT_CACHE_MAX_CAPACITY);
        let cache_ttl = self.cache_ttl.unwrap_or(DEFAULT_CACHE_TTL);
        let cache_tti = self.cache_tti.unwrap_or(DEFAULT_CACHE_TTI);

        let cache = CacheBuilder::new(cache_max_capacity)
            .time_to_live(cache_ttl)
            .time_to_idle(cache_tti)
            .build();
        let kv_backend = self.inner;
        let name = format!("CachedKvBackend({})", kv_backend.name());
        let version = AtomicUsize::new(0);

        CachedKvBackend {
            kv_backend,
            cache,
            name,
            version,
        }
    }
}

pub type CacheBackend = Cache<Vec<u8>, KeyValue>;

/// A wrapper of `MetaKvBackend` with cache support.
///
/// CachedMetaKvBackend is mainly used to read metadata information from Metasrv, and provides
/// cache for get and batch_get. One way to trigger cache invalidation of CachedMetaKvBackend:
/// when metadata information changes, Metasrv will broadcast a metadata invalidation request.
///
/// Therefore, it is recommended to use CachedMetaKvBackend to only read metadata related
/// information. Note: If you read other information, you may read expired data, which depends on
/// TTL and TTI for cache.
pub struct CachedKvBackend {
    kv_backend: KvBackendRef,
    cache: CacheBackend,
    name: String,
    version: AtomicUsize,
}

impl TxnService for CachedKvBackend {
    type Error = Error;
}

#[async_trait::async_trait]
impl KvBackend for CachedKvBackend {
    fn name(&self) -> &str {
        &self.name
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn range(&self, req: RangeRequest) -> Result<RangeResponse> {
        self.kv_backend.range(req).await
    }

    async fn put(&self, req: PutRequest) -> Result<PutResponse> {
        let key = &req.key.clone();

        let ret = self.kv_backend.put(req).await;

        if ret.is_ok() {
            self.invalidate_key(key).await;
        }

        ret
    }

    async fn batch_put(&self, req: BatchPutRequest) -> Result<BatchPutResponse> {
        let keys = req
            .kvs
            .iter()
            .map(|kv| kv.key().to_vec())
            .collect::<Vec<_>>();

        let resp = self.kv_backend.batch_put(req).await;

        if resp.is_ok() {
            for key in keys {
                self.invalidate_key(&key).await;
            }
        }

        resp
    }

    async fn batch_get(&self, req: BatchGetRequest) -> Result<BatchGetResponse> {
        let _timer = METRIC_CATALOG_KV_BATCH_GET.start_timer();

        let mut kvs = Vec::with_capacity(req.keys.len());
        let mut miss_keys = Vec::with_capacity(req.keys.len());

        for key in req.keys {
            if let Some(val) = self.cache.get(&key).await {
                kvs.push(val);
            } else {
                miss_keys.push(key);
            }
        }

        let batch_get_req = BatchGetRequest::new().with_keys(miss_keys.clone());

        let pre_version = self.version();

        let unhit_kvs = self.kv_backend.batch_get(batch_get_req).await?.kvs;

        for kv in unhit_kvs.iter() {
            self.cache.insert(kv.key().to_vec(), kv.clone()).await;
        }

        if !self.validate_version(pre_version) {
            for key in miss_keys.iter() {
                self.cache.invalidate(key).await;
            }
        }

        kvs.extend(unhit_kvs);

        Ok(BatchGetResponse { kvs })
    }

    async fn compare_and_put(&self, req: CompareAndPutRequest) -> Result<CompareAndPutResponse> {
        let key = &req.key.clone();

        let ret = self.kv_backend.compare_and_put(req).await;

        if ret.is_ok() {
            self.invalidate_key(key).await;
        }

        ret
    }

    async fn delete_range(&self, mut req: DeleteRangeRequest) -> Result<DeleteRangeResponse> {
        let prev_kv = req.prev_kv;

        req.prev_kv = true;
        let resp = self.kv_backend.delete_range(req).await;
        match resp {
            Ok(mut resp) => {
                for prev_kv in resp.prev_kvs.iter() {
                    self.invalidate_key(prev_kv.key()).await;
                }

                if !prev_kv {
                    resp.prev_kvs = vec![];
                }
                Ok(resp)
            }
            Err(e) => Err(e),
        }
    }

    async fn batch_delete(&self, mut req: BatchDeleteRequest) -> Result<BatchDeleteResponse> {
        let prev_kv = req.prev_kv;

        req.prev_kv = true;
        let resp = self.kv_backend.batch_delete(req).await;
        match resp {
            Ok(mut resp) => {
                for prev_kv in resp.prev_kvs.iter() {
                    self.invalidate_key(prev_kv.key()).await;
                }

                if !prev_kv {
                    resp.prev_kvs = vec![];
                }
                Ok(resp)
            }
            Err(e) => Err(e),
        }
    }

    async fn get(&self, key: &[u8]) -> Result<Option<KeyValue>> {
        let _timer = METRIC_CATALOG_KV_GET.start_timer();

        let pre_version = Arc::new(Mutex::new(None));

        let init = async {
            let version_clone = pre_version.clone();
            let _timer = METRIC_CATALOG_KV_REMOTE_GET.start_timer();

            version_clone.lock().unwrap().replace(self.version());

            self.kv_backend.get(key).await.map(|val| {
                val.with_context(|| CacheNotGetSnafu {
                    key: String::from_utf8_lossy(key),
                })
            })?
        };

        // currently moka doesn't have `optionally_try_get_with_by_ref`
        // TODO(fys): change to moka method when available
        // https://github.com/moka-rs/moka/issues/254
        let ret = match self.cache.try_get_with_by_ref(key, init).await {
            Ok(val) => Ok(Some(val)),
            Err(e) => match e.as_ref() {
                CacheNotGet { .. } => Ok(None),
                _ => Err(e),
            },
        }
        .map_err(|e| {
            GetKvCacheSnafu {
                err_msg: e.to_string(),
            }
            .build()
        });

        // "cache.invalidate_key" and "cache.try_get_with_by_ref" are not mutually exclusive. So we need
        // to use the version mechanism to prevent expired data from being put into the cache.
        if pre_version
            .lock()
            .unwrap()
            .as_ref()
            .map_or(false, |v| !self.validate_version(*v))
        {
            self.cache.invalidate(key).await;
        }

        ret
    }
}

#[async_trait::async_trait]
impl KvCacheInvalidator for CachedKvBackend {
    async fn invalidate_key(&self, key: &[u8]) {
        self.create_new_version();
        self.cache.invalidate(key).await;
        debug!("invalidated cache key: {}", String::from_utf8_lossy(key));
    }
}

impl CachedKvBackend {
    // only for test
    #[cfg(test)]
    fn wrap(kv_backend: KvBackendRef) -> Self {
        let cache = CacheBuilder::new(DEFAULT_CACHE_MAX_CAPACITY)
            .time_to_live(DEFAULT_CACHE_TTL)
            .time_to_idle(DEFAULT_CACHE_TTI)
            .build();

        let name = format!("CachedKvBackend({})", kv_backend.name());
        Self {
            kv_backend,
            cache,
            name,
            version: AtomicUsize::new(0),
        }
    }

    pub fn cache(&self) -> &CacheBackend {
        &self.cache
    }

    fn version(&self) -> usize {
        self.version.load(Ordering::Relaxed)
    }

    fn validate_version(&self, pre_version: usize) -> bool {
        self.version() == pre_version
    }

    fn create_new_version(&self) -> usize {
        self.version.fetch_add(1, Ordering::Relaxed) + 1
    }
}

#[derive(Debug)]
pub struct MetaKvBackend {
    pub client: Arc<MetaClient>,
}

impl MetaKvBackend {
    /// Constructs a [MetaKvBackend].
    pub fn new(client: Arc<MetaClient>) -> MetaKvBackend {
        MetaKvBackend { client }
    }
}

impl TxnService for MetaKvBackend {
    type Error = Error;
}

/// Implement `KvBackend` trait for `MetaKvBackend` instead of opendal's `Accessor` since
/// `MetaClient`'s range method can return both keys and values, which can reduce IO overhead
/// comparing to `Accessor`'s list and get method.
#[async_trait::async_trait]
impl KvBackend for MetaKvBackend {
    fn name(&self) -> &str {
        "MetaKvBackend"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn range(&self, req: RangeRequest) -> Result<RangeResponse> {
        self.client
            .range(req)
            .await
            .map_err(BoxedError::new)
            .context(ExternalSnafu)
    }

    async fn put(&self, req: PutRequest) -> Result<PutResponse> {
        self.client
            .put(req)
            .await
            .map_err(BoxedError::new)
            .context(ExternalSnafu)
    }

    async fn batch_put(&self, req: BatchPutRequest) -> Result<BatchPutResponse> {
        self.client
            .batch_put(req)
            .await
            .map_err(BoxedError::new)
            .context(ExternalSnafu)
    }

    async fn batch_get(&self, req: BatchGetRequest) -> Result<BatchGetResponse> {
        self.client
            .batch_get(req)
            .await
            .map_err(BoxedError::new)
            .context(ExternalSnafu)
    }

    async fn compare_and_put(
        &self,
        request: CompareAndPutRequest,
    ) -> Result<CompareAndPutResponse> {
        self.client
            .compare_and_put(request)
            .await
            .map_err(BoxedError::new)
            .context(ExternalSnafu)
    }

    async fn delete_range(&self, req: DeleteRangeRequest) -> Result<DeleteRangeResponse> {
        self.client
            .delete_range(req)
            .await
            .map_err(BoxedError::new)
            .context(ExternalSnafu)
    }

    async fn batch_delete(&self, req: BatchDeleteRequest) -> Result<BatchDeleteResponse> {
        self.client
            .batch_delete(req)
            .await
            .map_err(BoxedError::new)
            .context(ExternalSnafu)
    }

    async fn get(&self, key: &[u8]) -> Result<Option<KeyValue>> {
        let mut response = self
            .client
            .range(RangeRequest::new().with_key(key))
            .await
            .map_err(BoxedError::new)
            .context(ExternalSnafu)?;
        Ok(response.take_kvs().get_mut(0).map(|kv| KeyValue {
            key: kv.take_key(),
            value: kv.take_value(),
        }))
    }
}

#[cfg(test)]
mod tests {
    use std::any::Any;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;

    use async_trait::async_trait;
    use common_meta::kv_backend::{KvBackend, TxnService};
    use common_meta::rpc::store::{
        BatchDeleteRequest, BatchDeleteResponse, BatchGetRequest, BatchGetResponse,
        BatchPutRequest, BatchPutResponse, DeleteRangeRequest, DeleteRangeResponse, PutRequest,
        PutResponse, RangeRequest, RangeResponse,
    };
    use common_meta::rpc::KeyValue;
    use dashmap::DashMap;

    use super::CachedKvBackend;

    #[derive(Default)]
    pub struct SimpleKvBackend {
        inner_map: DashMap<Vec<u8>, Vec<u8>>,
        get_execute_times: Arc<AtomicU32>,
    }

    impl TxnService for SimpleKvBackend {
        type Error = common_meta::error::Error;
    }

    #[async_trait]
    impl KvBackend for SimpleKvBackend {
        fn name(&self) -> &str {
            "SimpleKvBackend"
        }

        fn as_any(&self) -> &dyn Any {
            self
        }

        async fn batch_get(&self, req: BatchGetRequest) -> Result<BatchGetResponse, Self::Error> {
            let mut kvs = Vec::with_capacity(req.keys.len());
            for key in req.keys.iter() {
                if let Some(kv) = self.get(key).await? {
                    kvs.push(kv);
                }
            }
            Ok(BatchGetResponse { kvs })
        }

        async fn put(&self, req: PutRequest) -> Result<PutResponse, Self::Error> {
            self.inner_map.insert(req.key, req.value);
            // always return None as prev_kv, since we don't use it in this test.
            Ok(PutResponse { prev_kv: None })
        }

        async fn get(&self, key: &[u8]) -> Result<Option<KeyValue>, Self::Error> {
            self.get_execute_times
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(self.inner_map.get(key).map(|v| KeyValue {
                key: key.to_vec(),
                value: v.value().clone(),
            }))
        }

        async fn range(&self, _req: RangeRequest) -> Result<RangeResponse, Self::Error> {
            unimplemented!()
        }

        async fn batch_put(&self, _req: BatchPutRequest) -> Result<BatchPutResponse, Self::Error> {
            unimplemented!()
        }

        async fn delete_range(
            &self,
            _req: DeleteRangeRequest,
        ) -> Result<DeleteRangeResponse, Self::Error> {
            unimplemented!()
        }

        async fn batch_delete(
            &self,
            _req: BatchDeleteRequest,
        ) -> Result<BatchDeleteResponse, Self::Error> {
            unimplemented!()
        }
    }

    #[tokio::test]
    async fn test_cached_kv_backend() {
        let simple_kv = Arc::new(SimpleKvBackend::default());
        let get_execute_times = simple_kv.get_execute_times.clone();
        let cached_kv = CachedKvBackend::wrap(simple_kv);

        add_some_vals(&cached_kv).await;

        let batch_get_req = BatchGetRequest {
            keys: vec![b"k1".to_vec(), b"k2".to_vec()],
        };

        assert_eq!(get_execute_times.load(Ordering::SeqCst), 0);

        for _ in 0..10 {
            let _batch_get_resp = cached_kv.batch_get(batch_get_req.clone()).await.unwrap();

            assert_eq!(get_execute_times.load(Ordering::SeqCst), 2);
        }

        let batch_get_req = BatchGetRequest {
            keys: vec![b"k1".to_vec(), b"k2".to_vec(), b"k3".to_vec()],
        };

        let _batch_get_resp = cached_kv.batch_get(batch_get_req.clone()).await.unwrap();

        assert_eq!(get_execute_times.load(Ordering::SeqCst), 3);

        for _ in 0..10 {
            let _batch_get_resp = cached_kv.batch_get(batch_get_req.clone()).await.unwrap();

            assert_eq!(get_execute_times.load(Ordering::SeqCst), 3);
        }
    }

    async fn add_some_vals(kv_backend: &impl KvBackend) {
        kv_backend
            .put(PutRequest {
                key: b"k1".to_vec(),
                value: b"v1".to_vec(),
                prev_kv: false,
            })
            .await
            .unwrap();

        kv_backend
            .put(PutRequest {
                key: b"k2".to_vec(),
                value: b"v2".to_vec(),
                prev_kv: false,
            })
            .await
            .unwrap();

        kv_backend
            .put(PutRequest {
                key: b"k3".to_vec(),
                value: b"v3".to_vec(),
                prev_kv: false,
            })
            .await
            .unwrap();
    }
}
