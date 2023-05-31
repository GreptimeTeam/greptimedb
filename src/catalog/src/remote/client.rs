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

use async_stream::stream;
use common_meta::rpc::store::{CompareAndPutRequest, DeleteRangeRequest, PutRequest, RangeRequest};
use common_telemetry::{info, timer};
use meta_client::client::MetaClient;
use moka::future::{Cache, CacheBuilder};
use snafu::ResultExt;

use super::KvCacheInvalidator;
use crate::error::{Error, GenericSnafu, MetaSrvSnafu, Result};
use crate::metrics::{METRIC_CATALOG_KV_GET, METRIC_CATALOG_KV_REMOTE_GET};
use crate::remote::{Kv, KvBackend, KvBackendRef, ValueIter};

const CACHE_MAX_CAPACITY: u64 = 10000;
const CACHE_TTL_SECOND: u64 = 10 * 60;
const CACHE_TTI_SECOND: u64 = 5 * 60;

pub type CacheBackendRef = Arc<Cache<Vec<u8>, Option<Kv>>>;
pub struct CachedMetaKvBackend {
    kv_backend: KvBackendRef,
    cache: CacheBackendRef,
}

#[async_trait::async_trait]
impl KvBackend for CachedMetaKvBackend {
    fn range<'a, 'b>(&'a self, key: &[u8]) -> ValueIter<'b, Error>
    where
        'a: 'b,
    {
        self.kv_backend.range(key)
    }

    async fn get(&self, key: &[u8]) -> Result<Option<Kv>> {
        let _timer = timer!(METRIC_CATALOG_KV_GET);

        let init = async {
            let _timer = timer!(METRIC_CATALOG_KV_REMOTE_GET);

            self.kv_backend.get(key).await
        };

        let schema_provider = self.cache.try_get_with_by_ref(key, init).await;
        schema_provider.map_err(|e| GenericSnafu { msg: e.to_string() }.build())
    }

    async fn set(&self, key: &[u8], val: &[u8]) -> Result<()> {
        let ret = self.kv_backend.set(key, val).await;

        if ret.is_ok() {
            self.invalidate_key(key).await;
        }

        ret
    }

    async fn delete(&self, key: &[u8]) -> Result<()> {
        let ret = self.kv_backend.delete_range(key, &[]).await;

        if ret.is_ok() {
            self.invalidate_key(key).await;
        }

        ret
    }

    async fn delete_range(&self, _key: &[u8], _end: &[u8]) -> Result<()> {
        // TODO(fys): implement it
        unimplemented!()
    }

    async fn compare_and_set(
        &self,
        key: &[u8],
        expect: &[u8],
        val: &[u8],
    ) -> Result<std::result::Result<(), Option<Vec<u8>>>> {
        let ret = self.kv_backend.compare_and_set(key, expect, val).await;

        if ret.is_ok() {
            self.invalidate_key(key).await;
        }

        ret
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[async_trait::async_trait]
impl KvCacheInvalidator for CachedMetaKvBackend {
    async fn invalidate_key(&self, key: &[u8]) {
        self.cache.invalidate(key).await
    }
}

impl CachedMetaKvBackend {
    pub fn new(client: Arc<MetaClient>) -> Self {
        let cache = Arc::new(
            CacheBuilder::new(CACHE_MAX_CAPACITY)
                .time_to_live(Duration::from_secs(CACHE_TTL_SECOND))
                .time_to_idle(Duration::from_secs(CACHE_TTI_SECOND))
                .build(),
        );
        let kv_backend = Arc::new(MetaKvBackend { client });

        Self { kv_backend, cache }
    }

    pub fn wrap(kv_backend: KvBackendRef) -> Self {
        let cache = Arc::new(
            CacheBuilder::new(CACHE_MAX_CAPACITY)
                .time_to_live(Duration::from_secs(CACHE_TTL_SECOND))
                .time_to_idle(Duration::from_secs(CACHE_TTI_SECOND))
                .build(),
        );

        Self { kv_backend, cache }
    }

    pub fn cache(&self) -> &CacheBackendRef {
        &self.cache
    }
}

#[derive(Debug)]
pub struct MetaKvBackend {
    pub client: Arc<MetaClient>,
}

/// Implement `KvBackend` trait for `MetaKvBackend` instead of opendal's `Accessor` since
/// `MetaClient`'s range method can return both keys and values, which can reduce IO overhead
/// comparing to `Accessor`'s list and get method.
#[async_trait::async_trait]
impl KvBackend for MetaKvBackend {
    fn range<'a, 'b>(&'a self, key: &[u8]) -> ValueIter<'b, Error>
    where
        'a: 'b,
    {
        let key = key.to_vec();
        Box::pin(stream!({
            let mut resp = self
                .client
                .range(RangeRequest::new().with_prefix(key))
                .await
                .context(MetaSrvSnafu)?;
            let kvs = resp.take_kvs();
            for mut kv in kvs.into_iter() {
                yield Ok(Kv(kv.take_key(), kv.take_value()))
            }
        }))
    }

    async fn get(&self, key: &[u8]) -> Result<Option<Kv>> {
        let mut response = self
            .client
            .range(RangeRequest::new().with_key(key))
            .await
            .context(MetaSrvSnafu)?;
        Ok(response
            .take_kvs()
            .get_mut(0)
            .map(|kv| Kv(kv.take_key(), kv.take_value())))
    }

    async fn set(&self, key: &[u8], val: &[u8]) -> Result<()> {
        let req = PutRequest::new()
            .with_key(key.to_vec())
            .with_value(val.to_vec());
        let _ = self.client.put(req).await.context(MetaSrvSnafu)?;
        Ok(())
    }

    async fn delete_range(&self, key: &[u8], end: &[u8]) -> Result<()> {
        let req = DeleteRangeRequest::new().with_range(key.to_vec(), end.to_vec());
        let resp = self.client.delete_range(req).await.context(MetaSrvSnafu)?;
        info!(
            "Delete range, key: {}, end: {}, deleted: {}",
            String::from_utf8_lossy(key),
            String::from_utf8_lossy(end),
            resp.deleted()
        );

        Ok(())
    }

    async fn compare_and_set(
        &self,
        key: &[u8],
        expect: &[u8],
        val: &[u8],
    ) -> Result<std::result::Result<(), Option<Vec<u8>>>> {
        let request = CompareAndPutRequest::new()
            .with_key(key.to_vec())
            .with_expect(expect.to_vec())
            .with_value(val.to_vec());
        let mut response = self
            .client
            .compare_and_put(request)
            .await
            .context(MetaSrvSnafu)?;
        if response.is_success() {
            Ok(Ok(()))
        } else {
            Ok(Err(response.take_prev_kv().map(|v| v.value().to_vec())))
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
