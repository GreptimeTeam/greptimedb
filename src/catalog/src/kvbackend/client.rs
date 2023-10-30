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
use std::sync::Arc;
use std::time::Duration;

use common_error::ext::BoxedError;
use common_meta::cache_invalidator::KvCacheInvalidator;
use common_meta::error::Error::{CacheNotGet, GetKvCache};
use common_meta::error::{CacheNotGetSnafu, Error, ExternalSnafu, Result};
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

use crate::metrics::{METRIC_CATALOG_KV_GET, METRIC_CATALOG_KV_REMOTE_GET};

const CACHE_MAX_CAPACITY: u64 = 10000;
const CACHE_TTL_SECOND: u64 = 10 * 60;
const CACHE_TTI_SECOND: u64 = 5 * 60;

pub type CacheBackendRef = Arc<Cache<Vec<u8>, KeyValue>>;

pub struct CachedMetaKvBackend {
    kv_backend: KvBackendRef,
    cache: CacheBackendRef,
    name: String,
}

impl TxnService for CachedMetaKvBackend {
    type Error = Error;
}

#[async_trait::async_trait]
impl KvBackend for CachedMetaKvBackend {
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
        self.kv_backend.batch_get(req).await
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

        let init = async {
            let _timer = METRIC_CATALOG_KV_REMOTE_GET.start_timer();
            self.kv_backend.get(key).await.map(|val| {
                val.with_context(|| CacheNotGetSnafu {
                    key: String::from_utf8_lossy(key),
                })
            })?
        };

        // currently moka doesn't have `optionally_try_get_with_by_ref`
        // TODO(fys): change to moka method when available
        // https://github.com/moka-rs/moka/issues/254
        match self.cache.try_get_with_by_ref(key, init).await {
            Ok(val) => Ok(Some(val)),
            Err(e) => match e.as_ref() {
                CacheNotGet { .. } => Ok(None),
                _ => Err(e),
            },
        }
        .map_err(|e| GetKvCache {
            err_msg: e.to_string(),
        })
    }
}

#[async_trait::async_trait]
impl KvCacheInvalidator for CachedMetaKvBackend {
    async fn invalidate_key(&self, key: &[u8]) {
        self.cache.invalidate(key).await;
        debug!("invalidated cache key: {}", String::from_utf8_lossy(key));
    }
}

impl CachedMetaKvBackend {
    pub fn new(client: Arc<MetaClient>) -> Self {
        let kv_backend = Arc::new(MetaKvBackend { client });
        Self::wrap(kv_backend)
    }

    pub fn wrap(kv_backend: KvBackendRef) -> Self {
        let cache = Arc::new(
            CacheBuilder::new(CACHE_MAX_CAPACITY)
                .time_to_live(Duration::from_secs(CACHE_TTL_SECOND))
                .time_to_idle(Duration::from_secs(CACHE_TTI_SECOND))
                .build(),
        );

        let name = format!("CachedKvBackend({})", kv_backend.name());
        Self {
            kv_backend,
            cache,
            name,
        }
    }

    pub fn cache(&self) -> &CacheBackendRef {
        &self.cache
    }
}

#[derive(Debug)]
pub struct MetaKvBackend {
    pub client: Arc<MetaClient>,
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

    async fn range(&self, req: RangeRequest) -> Result<RangeResponse> {
        self.client
            .range(req)
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

    async fn batch_put(&self, req: BatchPutRequest) -> Result<BatchPutResponse> {
        self.client
            .batch_put(req)
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

    fn as_any(&self) -> &dyn Any {
        self
    }
}
