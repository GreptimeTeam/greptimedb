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
use std::sync::atomic::{AtomicU32, Ordering};

use prometheus::{IntGauge, IntGaugeVec};
use snafu::ResultExt;

use crate::error::{CacheGetSnafu, Result};
use crate::file_metadata::FileMetadata;

/// Metrics for index metadata.
const PUFFIN_METADATA_TYPE: &str = "puffin_metadata";

pub type PuffinMetadataCacheRef = Arc<PuffinMetadataCache>;

/// A cache for storing the metadata of the index files.
pub struct PuffinMetadataCache {
    cache: moka::future::Cache<String, Arc<CachedMetadata>>,
    cache_bytes: IntGaugeVec,
}

fn puffin_metadata_weight(k: &str, v: &Arc<FileMetadata>) -> u32 {
    (k.len() + v.memory_usage()) as u32
}

impl PuffinMetadataCache {
    pub fn new(capacity: u64, cache_bytes: &IntGaugeVec) -> Self {
        common_telemetry::debug!("Building PuffinMetadataCache with capacity: {capacity}");
        Self {
            cache: moka::future::CacheBuilder::new(capacity)
                .name("puffin_metadata")
                .weigher(|k: &String, v: &Arc<CachedMetadata>| {
                    puffin_metadata_weight(k, &v.metadata)
                })
                .eviction_listener(|_k, v, _cause| v.release_accounting())
                .build(),
            cache_bytes: cache_bytes.clone(),
        }
    }

    /// Gets the metadata from the cache.
    pub async fn get_metadata(&self, file_id: &str) -> Option<Arc<FileMetadata>> {
        self.cache.get(file_id).await.map(|v| v.metadata.clone())
    }

    /// Shares one metadata load among concurrent readers of the same file.
    pub async fn get_or_load<F>(&self, file_id: &str, load: F) -> Result<Arc<FileMetadata>>
    where
        F: Future<Output = Result<Arc<FileMetadata>>>,
    {
        self.cache
            .try_get_with_by_ref(file_id, async {
                let metadata = load.await?;
                Ok(CachedMetadata::new(file_id, metadata, &self.cache_bytes))
            })
            .await
            .context(CacheGetSnafu)
            .map(|value| value.metadata.clone())
    }

    /// Puts the metadata into the cache.
    pub async fn put_metadata(&self, file_id: String, metadata: Arc<FileMetadata>) {
        let value = CachedMetadata::new(&file_id, metadata, &self.cache_bytes);
        self.cache.insert(file_id, value).await;
    }

    /// Removes the metadata of the given file from the cache, if present.
    pub async fn remove(&self, file_id: &str) {
        self.cache.invalidate(file_id).await;
    }
}

/// Releases accounting on eviction, or if insertion is cancelled before admission.
struct CachedMetadata {
    metadata: Arc<FileMetadata>,
    accounted_bytes: AtomicU32,
    cache_bytes: IntGauge,
}

impl CachedMetadata {
    fn new(file_id: &str, metadata: Arc<FileMetadata>, metrics: &IntGaugeVec) -> Arc<Self> {
        let size = puffin_metadata_weight(file_id, &metadata);
        let cache_bytes = metrics.with_label_values(&[PUFFIN_METADATA_TYPE]);
        cache_bytes.add(size.into());
        Arc::new(Self {
            metadata,
            accounted_bytes: AtomicU32::new(size),
            cache_bytes,
        })
    }

    fn release_accounting(&self) {
        let size = self.accounted_bytes.swap(0, Ordering::Relaxed);
        self.cache_bytes.sub(size.into());
    }
}

impl Drop for CachedMetadata {
    fn drop(&mut self) {
        self.release_accounting();
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;

    use prometheus::Opts;

    use super::*;
    use crate::error::InvalidPuffinFooterSnafu;

    fn new_cache() -> (PuffinMetadataCache, IntGauge) {
        let metrics =
            IntGaugeVec::new(Opts::new("test_metadata_bytes", "test"), &["type"]).unwrap();
        let gauge = metrics.with_label_values(&[PUFFIN_METADATA_TYPE]);
        (PuffinMetadataCache::new(1024 * 1024, &metrics), gauge)
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
                        cache.get_or_load("file", load).await
                    } else if let Some(value) = cache.get_metadata("file").await {
                        Ok(value)
                    } else {
                        let value = load.await?;
                        cache.put_metadata("file".to_string(), value.clone()).await;
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
        let (cache, gauge) = new_cache();
        let calls = AtomicUsize::new(0);
        let gate = tokio::sync::Notify::new();
        let load = || async {
            calls.fetch_add(1, Ordering::Relaxed);
            gate.notified().await;
            Ok(metadata())
        };
        let mut first = Box::pin(cache.get_or_load("file", load()));
        let mut second = Box::pin(cache.get_or_load("file", load()));
        assert!(futures::poll!(&mut first).is_pending());
        assert!(futures::poll!(&mut second).is_pending());
        assert_eq!(calls.load(Ordering::Relaxed), 1);
        gate.notify_waiters();
        let (first, second) = tokio::join!(first, second);
        let first = first.unwrap();
        assert!(Arc::ptr_eq(&first, &second.unwrap()));
        assert_eq!(
            gauge.get(),
            i64::from(puffin_metadata_weight("file", &first))
        );
        let warm = cache
            .get_or_load("file", async { panic!("warm read loaded") })
            .await
            .unwrap();
        assert!(Arc::ptr_eq(&first, &warm));
        cache.remove("file").await;
        cache.cache.run_pending_tasks().await;
        assert!(cache.get_metadata("file").await.is_none());
        assert_eq!(gauge.get(), 0);
    }

    #[tokio::test]
    async fn shared_failure_can_retry() {
        let (cache, gauge) = new_cache();
        let calls = AtomicUsize::new(0);
        let gate = tokio::sync::Notify::new();
        let load = || async {
            calls.fetch_add(1, Ordering::Relaxed);
            gate.notified().await;
            InvalidPuffinFooterSnafu.fail()
        };
        let mut first = Box::pin(cache.get_or_load("file", load()));
        let mut second = Box::pin(cache.get_or_load("file", load()));
        assert!(futures::poll!(&mut first).is_pending());
        assert!(futures::poll!(&mut second).is_pending());
        gate.notify_waiters();
        let (first, second) = tokio::join!(first, second);
        assert!(first.is_err());
        assert!(second.is_err());
        assert_eq!(calls.load(Ordering::Relaxed), 1);
        assert_eq!(gauge.get(), 0);
        assert!(
            cache
                .get_or_load("file", async { Ok(metadata()) })
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn cancelled_loader_allows_waiter_to_load() {
        let (cache, _) = new_cache();
        let mut first = Box::pin(cache.get_or_load("file", std::future::pending()));
        assert!(futures::poll!(&mut first).is_pending());
        let mut second = Box::pin(cache.get_or_load("file", async { Ok(metadata()) }));
        assert!(futures::poll!(&mut second).is_pending());
        drop(first);
        tokio::time::timeout(std::time::Duration::from_secs(5), second)
            .await
            .unwrap()
            .unwrap();
    }
}
