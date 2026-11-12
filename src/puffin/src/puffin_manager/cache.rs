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

use std::future::Future;
use std::sync::Arc;

use common_cache::Cache;
use common_cache::metrics::CacheMetrics;
use common_cache::metrics::CacheSizeMetrics;
use common_telemetry::tracing::{self, Instrument};
use snafu::ResultExt;

use crate::error::{CacheGetSnafu, Result};
use crate::file_metadata::FileMetadata;
use crate::puffin_manager::file_key::PuffinFileKey;

/// Metrics for index metadata.
const PUFFIN_METADATA_TYPE: &str = "puffin_metadata";

pub type PuffinMetadataCacheRef = Arc<PuffinMetadataCache>;

/// A cache for storing the metadata of the index files.
pub struct PuffinMetadataCache {
    cache: Cache<PuffinFileKey<String>, Arc<FileMetadata>>,
}

fn puffin_metadata_weight(k: &PuffinFileKey<String>, v: &Arc<FileMetadata>) -> u32 {
    u32::try_from(k.estimated_size() + k.handle().capacity() + v.memory_usage()).unwrap_or(u32::MAX)
}

impl PuffinMetadataCache {
    pub fn new(capacity: u64, cache_bytes: &CacheSizeMetrics) -> Self {
        common_telemetry::debug!("Building PuffinMetadataCache with capacity: {capacity}");
        Self {
            cache: Cache::new(
                "puffin_metadata",
                capacity,
                |k: &PuffinFileKey<String>, v: &Arc<FileMetadata>| puffin_metadata_weight(k, v),
                |_, _, _| {},
            )
            .with_size_metric(cache_bytes.bind(&[PUFFIN_METADATA_TYPE])),
        }
    }

    /// Attaches native loader metrics without changing existing cache byte accounting.
    pub fn with_load_metrics(mut self, metrics: Arc<CacheMetrics>) -> Self {
        self.cache = self.cache.with_metrics(metrics);
        self
    }

    /// Gets the metadata from the cache.
    pub async fn get_metadata(&self, file_id: &PuffinFileKey<String>) -> Option<Arc<FileMetadata>> {
        self.cache.get(file_id).await
    }

    /// Shares one metadata load among concurrent readers of the same file.
    #[tracing::instrument(name = "puffin_metadata_lookup", skip_all)]
    pub async fn try_get_with<F>(
        &self,
        file_id: &PuffinFileKey<String>,
        load: F,
    ) -> Result<Arc<FileMetadata>>
    where
        F: Future<Output = Result<Arc<FileMetadata>>>,
    {
        self.cache
            .try_get_with(file_id, async {
                let metadata = load
                    .instrument(tracing::info_span!("puffin_metadata_load"))
                    .await?;
                Ok(metadata)
            })
            .await
            .context(CacheGetSnafu)
    }

    /// Puts the metadata into the cache.
    pub async fn put_metadata(&self, file_id: PuffinFileKey<String>, metadata: Arc<FileMetadata>) {
        self.cache.insert(file_id, metadata).await;
    }

    /// Invalidates the versioned handle across storage scopes when the caller has no manager.
    /// Pending loaders are not fenced; physical removal runs asynchronously.
    pub fn invalidate_file(&self, file_id: &str) -> std::io::Result<()> {
        let file_id = file_id.to_string();
        self.cache
            .invalidate_if(move |key, _| key.handle() == &file_id)
    }

    /// Removes the metadata of the given file from the cache, if present.
    pub async fn remove(&self, file_id: &PuffinFileKey<String>) {
        self.cache.invalidate(file_id).await;
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use prometheus::Opts;
    use prometheus::core::Collector;

    use super::*;
    use crate::error::InvalidPuffinFooterSnafu;

    fn key() -> PuffinFileKey<String> {
        PuffinFileKey::new(
            Arc::new(crate::puffin_manager::file_key::PuffinStorageScope::new(
                "file".into(),
                String::new(),
                "/root/".into(),
            )),
            "file".into(),
            "file".into(),
        )
    }

    fn new_cache() -> (PuffinMetadataCache, CacheSizeMetrics) {
        let metrics =
            CacheSizeMetrics::new(Opts::new("test_metadata_bytes", "test"), &["type"]).unwrap();
        let capacity = u64::from(puffin_metadata_weight(&key(), &metadata())) * 4;
        (PuffinMetadataCache::new(capacity, &metrics), metrics)
    }

    fn sample_bytes(metrics: &CacheSizeMetrics) -> i64 {
        metrics.collect()[0].get_metric()[0]
            .get_gauge()
            .as_ref()
            .unwrap()
            .value() as i64
    }

    fn metadata() -> Arc<FileMetadata> {
        Arc::new(FileMetadata {
            blobs: Vec::new(),
            properties: Default::default(),
        })
    }

    // Hold both requests at the same cold-load boundary. Only coalescing differs.
    #[tokio::test]
    async fn compare_separate_lookup_with_coalesced_load() {
        let key = key();
        for _ in 0..3 {
            for coalesce in [false, true] {
                let (cache, _) = new_cache();
                let calls = AtomicUsize::new(0);
                let gate = tokio::sync::Notify::new();
                let request = || async {
                    let load = async {
                        calls.fetch_add(1, Ordering::Relaxed);
                        gate.notified().await;
                        Ok(metadata())
                    };
                    if coalesce {
                        cache.try_get_with(&key, load).await
                    } else if let Some(value) = cache.get_metadata(&key).await {
                        Ok(value)
                    } else {
                        let value = load.await?;
                        cache.put_metadata(key.clone(), value.clone()).await;
                        Ok(value)
                    }
                };
                let mut first = Box::pin(request());
                let mut second = Box::pin(request());
                assert!(futures::poll!(&mut first).is_pending());
                assert!(futures::poll!(&mut second).is_pending());
                assert_eq!(calls.load(Ordering::Relaxed), if coalesce { 1 } else { 2 });
                gate.notify_waiters();
                let (first, second) = tokio::join!(first, second);
                assert_eq!(first.unwrap().as_ref(), second.unwrap().as_ref());
            }
        }
    }

    #[tokio::test]
    async fn concurrent_loads_share_result_and_accounting() {
        let key = key();
        let (cache, gauge) = new_cache();
        let calls = AtomicUsize::new(0);
        let gate = tokio::sync::Notify::new();
        let load = || async {
            calls.fetch_add(1, Ordering::Relaxed);
            gate.notified().await;
            Ok(metadata())
        };
        let mut first = Box::pin(cache.try_get_with(&key, load()));
        let mut second = Box::pin(cache.try_get_with(&key, load()));
        assert!(futures::poll!(&mut first).is_pending());
        assert!(futures::poll!(&mut second).is_pending());
        assert_eq!(calls.load(Ordering::Relaxed), 1);
        gate.notify_waiters();
        let (first, second) = tokio::join!(first, second);
        let first = first.unwrap();
        assert!(Arc::ptr_eq(&first, &second.unwrap()));
        cache.cache.run_pending_tasks().await;
        assert_eq!(
            sample_bytes(&gauge),
            i64::from(puffin_metadata_weight(&key, &first))
        );
        let warm = cache
            .try_get_with(&key, async { panic!("warm read loaded") })
            .await
            .unwrap();
        assert!(Arc::ptr_eq(&first, &warm));
        cache.remove(&key).await;
        cache.cache.run_pending_tasks().await;
        assert!(cache.get_metadata(&key).await.is_none());
        assert_eq!(sample_bytes(&gauge), 0);
    }

    #[tokio::test]
    async fn shared_failure_can_retry() {
        let key = key();
        let (cache, gauge) = new_cache();
        let calls = AtomicUsize::new(0);
        let gate = tokio::sync::Notify::new();
        let load = || async {
            calls.fetch_add(1, Ordering::Relaxed);
            gate.notified().await;
            InvalidPuffinFooterSnafu.fail()
        };
        let mut first = Box::pin(cache.try_get_with(&key, load()));
        let mut second = Box::pin(cache.try_get_with(&key, load()));
        assert!(futures::poll!(&mut first).is_pending());
        assert!(futures::poll!(&mut second).is_pending());
        gate.notify_waiters();
        let (first, second) = tokio::join!(first, second);
        assert!(first.is_err());
        assert!(second.is_err());
        assert_eq!(calls.load(Ordering::Relaxed), 1);
        assert_eq!(sample_bytes(&gauge), 0);
        assert!(
            cache
                .try_get_with(&key, async { Ok(metadata()) })
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn cancelled_loader_allows_waiter_to_load() {
        let key = key();
        let (cache, _) = new_cache();
        let mut first = Box::pin(cache.try_get_with(&key, std::future::pending()));
        assert!(futures::poll!(&mut first).is_pending());
        let mut second = Box::pin(cache.try_get_with(&key, async { Ok(metadata()) }));
        assert!(futures::poll!(&mut second).is_pending());
        drop(first);
        tokio::time::timeout(std::time::Duration::from_secs(5), second)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn same_handle_in_different_stores_does_not_share_footer() {
        let (cache, _) = new_cache();
        let first = key();
        let second = PuffinFileKey::new(
            Arc::new(crate::puffin_manager::file_key::PuffinStorageScope::new(
                "file".into(),
                String::new(),
                "/other-root/".into(),
            )),
            "file".into(),
            "file".into(),
        );
        let first_metadata = metadata();
        cache
            .put_metadata(first.clone(), first_metadata.clone())
            .await;
        assert!(cache.get_metadata(&second).await.is_none());
        let second_metadata = cache
            .try_get_with(&second, async {
                Ok(Arc::new(FileMetadata {
                    blobs: Vec::new(),
                    properties: [("store".to_string(), "second".to_string())].into(),
                }))
            })
            .await
            .unwrap();
        assert_ne!(first_metadata, second_metadata);
        cache.remove(&first).await;
        assert!(cache.get_metadata(&first).await.is_none());
        assert_eq!(Some(second_metadata), cache.get_metadata(&second).await);
        cache.invalidate_file("file").unwrap();
        cache.cache.run_pending_tasks().await;
        assert!(cache.get_metadata(&second).await.is_none());
    }
}
