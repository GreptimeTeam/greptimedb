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

//! A cache for manifest files.

use std::sync::Arc;
use std::time::{Duration, Instant};

use common_base::readable_size::ReadableSize;
use common_telemetry::{info, warn};
use futures::{FutureExt, TryStreamExt};
use moka::future::Cache;
use moka::notification::RemovalCause;
use moka::policy::EvictionPolicy;
use object_store::ObjectStore;
use object_store::util::join_path;
use snafu::ResultExt;

use crate::error::{OpenDalSnafu, Result};
use crate::metrics::CACHE_BYTES;

/// Subdirectory of cached manifest files.
///
/// This must contain three layers, corresponding to [`build_prometheus_metrics_layer`](object_store::layers::build_prometheus_metrics_layer).
const MANIFEST_DIR: &str = "cache/object/manifest/";

/// Metric label for manifest files.
const MANIFEST_TYPE: &str = "manifest";

/// A manifest cache manages manifest files on local store and evicts files based
/// on size.
#[derive(Debug)]
pub(crate) struct ManifestCache {
    /// Local store to cache files.
    local_store: ObjectStore,
    /// Index to track cached manifest files.
    index: Cache<String, IndexValue>,
}

pub(crate) type ManifestCacheRef = Arc<ManifestCache>;

impl ManifestCache {
    /// Creates a new manifest cache.
    pub(crate) fn new(
        local_store: ObjectStore,
        capacity: ReadableSize,
        ttl: Option<Duration>,
    ) -> ManifestCache {
        let total_capacity = capacity.as_bytes();

        info!(
            "Initializing manifest cache with capacity: {}",
            ReadableSize(total_capacity)
        );

        let index = Self::build_cache(local_store.clone(), total_capacity, ttl);

        ManifestCache { local_store, index }
    }

    /// Builds the cache.
    fn build_cache(
        local_store: ObjectStore,
        capacity: u64,
        ttl: Option<Duration>,
    ) -> Cache<String, IndexValue> {
        let cache_store = local_store;
        let mut builder = Cache::builder()
            .eviction_policy(EvictionPolicy::lru())
            .weigher(|_key: &String, value: &IndexValue| -> u32 {
                // We only measure space on local store.
                value.file_size
            })
            .max_capacity(capacity)
            .async_eviction_listener(move |key: Arc<String>, value: IndexValue, cause| {
                let store = cache_store.clone();
                // Stores files under MANIFEST_DIR.
                let file_path = join_path(MANIFEST_DIR, &key);
                async move {
                    if let RemovalCause::Replaced = cause {
                        // The cache is replaced by another file. This is unexpected, we don't remove the same
                        // file but updates the metrics as the file is already replaced by users.
                        CACHE_BYTES
                            .with_label_values(&[MANIFEST_TYPE])
                            .sub(value.file_size.into());
                        warn!("Replace existing cache {} unexpectedly", file_path);
                        return;
                    }

                    match store.delete(&file_path).await {
                        Ok(()) => {
                            CACHE_BYTES
                                .with_label_values(&[MANIFEST_TYPE])
                                .sub(value.file_size.into());
                        }
                        Err(e) => {
                            warn!(e; "Failed to delete cached manifest file {}", file_path);
                        }
                    }
                }
                .boxed()
            });
        if let Some(ttl) = ttl {
            builder = builder.time_to_idle(ttl);
        }
        builder.build()
    }

    /// Puts a file into the cache index.
    ///
    /// The caller should ensure the file is in the correct path.
    pub(crate) async fn put(&self, key: String, value: IndexValue) {
        CACHE_BYTES
            .with_label_values(&[MANIFEST_TYPE])
            .add(value.file_size.into());
        self.index.insert(key, value).await;

        // Since files can be large items, we run the pending tasks immediately.
        self.index.run_pending_tasks().await;
    }

    /// Gets the index value for the key.
    pub(crate) async fn get(&self, key: &str) -> Option<IndexValue> {
        self.index.get(key).await
    }

    /// Removes a file from the cache explicitly.
    /// It always tries to remove the file from the local store because we may not have the file
    /// in the memory index if upload failed.
    pub(crate) async fn remove(&self, key: &str) {
        let file_path = self.cache_file_path(key);
        self.index.remove(key).await;
        // Always delete the file from the local store.
        if let Err(e) = self.local_store.delete(&file_path).await {
            warn!(e; "Failed to delete a cached manifest file {}", file_path);
        }
    }

    async fn recover_inner(&self) -> Result<()> {
        let now = Instant::now();
        let mut lister = self
            .local_store
            .lister_with(MANIFEST_DIR)
            .await
            .context(OpenDalSnafu)?;
        // Use i64 for total_size to reduce the risk of overflow.
        let (mut total_size, mut total_keys) = (0i64, 0);
        while let Some(entry) = lister.try_next().await.context(OpenDalSnafu)? {
            let meta = entry.metadata();
            if !meta.is_file() {
                continue;
            }

            let meta = self
                .local_store
                .stat(entry.path())
                .await
                .context(OpenDalSnafu)?;
            let file_size = meta.content_length() as u32;
            // Use the file name as the key
            let key = entry.name().to_string();
            self.index.insert(key, IndexValue { file_size }).await;
            let size = i64::from(file_size);
            total_size += size;
            total_keys += 1;
        }
        // The metrics is a signed int gauge so we can update it finally.
        CACHE_BYTES
            .with_label_values(&[MANIFEST_TYPE])
            .add(total_size);

        // Run all pending tasks of the moka cache so that the cache size is updated
        // and the eviction policy is applied.
        self.index.run_pending_tasks().await;

        let weight = self.index.weighted_size();
        let count = self.index.entry_count();
        info!(
            "Recovered manifest cache, num_keys: {}, num_bytes: {}, count: {}, weight: {}, cost: {:?}",
            total_keys,
            total_size,
            count,
            weight,
            now.elapsed()
        );
        Ok(())
    }

    /// Recovers the index from local store.
    pub(crate) async fn recover(self: &Arc<Self>, sync: bool) {
        // FIXME(yingwen): Adds a task to clean empty directories.
        let moved_self = self.clone();
        let handle = tokio::spawn(async move {
            if let Err(err) = moved_self.recover_inner().await {
                common_telemetry::error!(err; "Failed to recover manifest cache.")
            }
        });

        if sync {
            let _ = handle.await;
        }
    }

    /// Returns the cache file path for the key.
    pub(crate) fn cache_file_path(&self, key: &str) -> String {
        join_path(MANIFEST_DIR, key)
    }

    /// Returns the local store of the manifest cache.
    pub(crate) fn local_store(&self) -> ObjectStore {
        self.local_store.clone()
    }

    /// Checks if the key is in the manifest cache.
    pub(crate) fn contains_key(&self, key: &str) -> bool {
        self.index.contains_key(key)
    }

    /// Gets a manifest file from cache.
    /// Returns the file data if found in cache, None otherwise.
    pub(crate) async fn get_file(&self, key: &str) -> Option<Vec<u8>> {
        // Check if file is in cache index
        self.get(key).await?;

        // Read from local cache store
        let cache_file_path = self.cache_file_path(key);
        match self.local_store.read(&cache_file_path).await {
            Ok(data) => Some(data.to_vec()),
            Err(e) => {
                warn!(e; "Failed to read cached manifest file {}", cache_file_path);
                None
            }
        }
    }

    /// Puts a manifest file into cache.
    pub(crate) async fn put_file(&self, key: String, data: Vec<u8>) {
        let cache_file_path = self.cache_file_path(&key);

        // Write to local cache store
        if let Err(e) = self.local_store.write(&cache_file_path, data.clone()).await {
            warn!(e; "Failed to write manifest to cache {}", cache_file_path);
            return;
        }

        // Add to cache index
        let file_size = data.len() as u32;
        self.put(key, IndexValue { file_size }).await;
    }
}

/// An entity that describes the file in the manifest cache.
///
/// It should only keep minimal information needed by the cache.
#[derive(Debug, Clone)]
pub(crate) struct IndexValue {
    /// Size of the file in bytes.
    pub(crate) file_size: u32,
}

#[cfg(test)]
mod tests {
    use common_test_util::temp_dir::create_temp_dir;
    use object_store::services::Fs;

    use super::*;

    fn new_fs_store(path: &str) -> ObjectStore {
        let builder = Fs::default().root(path);
        ObjectStore::new(builder).unwrap().finish()
    }

    #[tokio::test]
    async fn test_manifest_cache_basic() {
        let dir = create_temp_dir("");
        let local_store = new_fs_store(dir.path().to_str().unwrap());

        let cache = ManifestCache::new(local_store.clone(), ReadableSize::mb(10), None);
        let key = "region_1/manifest/00000000000000000007.json";
        let file_path = cache.cache_file_path(key);

        // Get an empty file.
        assert!(cache.get(key).await.is_none());

        // Write a file.
        local_store
            .write(&file_path, b"manifest content".as_slice())
            .await
            .unwrap();
        // Add to the cache.
        cache
            .put(key.to_string(), IndexValue { file_size: 16 })
            .await;

        // Get the cached value.
        let value = cache.get(key).await.unwrap();
        assert_eq!(16, value.file_size);

        // Get weighted size.
        cache.index.run_pending_tasks().await;
        assert_eq!(16, cache.index.weighted_size());

        // Remove the file.
        cache.remove(key).await;
        assert!(cache.get(key).await.is_none());

        // Ensure all pending tasks of the moka cache is done before assertion.
        cache.index.run_pending_tasks().await;

        // The file also not exists.
        assert!(!local_store.exists(&file_path).await.unwrap());
        assert_eq!(0, cache.index.weighted_size());
    }

    #[tokio::test]
    async fn test_manifest_cache_recover() {
        let dir = create_temp_dir("");
        let local_store = new_fs_store(dir.path().to_str().unwrap());
        let cache = ManifestCache::new(local_store.clone(), ReadableSize::mb(10), None);

        // Write some manifest files with different paths
        let keys = vec![
            "region_1/manifest/00000000000000000001.json",
            "region_1/manifest/00000000000000000002.json",
            "region_1/manifest/00000000000000000001.checkpoint",
            "region_2/manifest/00000000000000000001.json",
        ];

        let mut total_size = 0;
        for (i, key) in keys.iter().enumerate() {
            let file_path = cache.cache_file_path(key);
            let content = format!("manifest-{}", i).into_bytes();
            local_store
                .write(&file_path, content.clone())
                .await
                .unwrap();

            // Add to the cache.
            cache
                .put(
                    key.to_string(),
                    IndexValue {
                        file_size: content.len() as u32,
                    },
                )
                .await;
            total_size += content.len();
        }

        // Recover the cache.
        let cache = Arc::new(ManifestCache::new(
            local_store.clone(),
            ReadableSize::mb(10),
            None,
        ));
        // No entry before recovery.
        assert!(cache.get(keys[0]).await.is_none());
        cache.recover(true).await;

        // Check size.
        cache.index.run_pending_tasks().await;
        let total_cached = cache.index.weighted_size() as usize;
        assert_eq!(total_size, total_cached);

        // Verify all files
        for (i, key) in keys.iter().enumerate() {
            let value = cache.get(key).await.unwrap();
            assert_eq!(format!("manifest-{}", i).len() as u32, value.file_size);
        }
    }

    #[test]
    fn test_cache_file_path() {
        let dir = create_temp_dir("");
        let local_store = new_fs_store(dir.path().to_str().unwrap());
        let cache = ManifestCache::new(local_store, ReadableSize::mb(10), None);

        assert_eq!(
            "cache/object/manifest/region_1/manifest/00000000000000000007.json",
            cache.cache_file_path("region_1/manifest/00000000000000000007.json")
        );
        assert_eq!(
            "cache/object/manifest/region_1/manifest/00000000000000000007.checkpoint",
            cache.cache_file_path("region_1/manifest/00000000000000000007.checkpoint")
        );
    }
}
