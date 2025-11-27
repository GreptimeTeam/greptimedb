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

use std::path::PathBuf;
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
use crate::metrics::{CACHE_BYTES, CACHE_HIT, CACHE_MISS};

/// Subdirectory of cached manifest files.
///
/// This must contain three layers, corresponding to [`build_prometheus_metrics_layer`](object_store::layers::build_prometheus_metrics_layer).
const MANIFEST_DIR: &str = "cache/object/manifest/";

/// Metric label for manifest files.
const MANIFEST_TYPE: &str = "manifest";

/// A manifest cache manages manifest files on local store and evicts files based
/// on size.
#[derive(Debug, Clone)]
pub struct ManifestCache {
    /// Local store to cache files.
    local_store: ObjectStore,
    /// Index to track cached manifest files.
    index: Cache<String, IndexValue>,
}

impl ManifestCache {
    /// Creates a new manifest cache and recovers the index from local store.
    pub async fn new(
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

        let cache = ManifestCache { local_store, index };

        // Recover the cache index from local store asynchronously
        cache.recover(false).await;

        cache
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

    /// Removes multiple files from the cache in batch.
    /// This is more efficient than calling `remove` multiple times.
    pub(crate) async fn remove_batch(&self, keys: &[String]) {
        if keys.is_empty() {
            return;
        }

        // Remove from index
        for key in keys {
            self.index.remove(key).await;
        }

        // Collect file paths to delete
        let file_paths: Vec<String> = keys.iter().map(|key| self.cache_file_path(key)).collect();

        // Delete files from local store in batch
        if let Err(e) = self.local_store.delete_iter(file_paths).await {
            warn!(e; "Failed to delete cached manifest files in batch");
        }
    }

    async fn recover_inner(&self) -> Result<()> {
        let now = Instant::now();
        let mut lister = self
            .local_store
            .lister_with(MANIFEST_DIR)
            .recursive(true)
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
    pub(crate) async fn recover(&self, sync: bool) {
        let moved_self = self.clone();
        let handle = tokio::spawn(async move {
            // Clean empty directories before recovering
            moved_self.clean_empty_dirs().await;

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

    /// Gets a manifest file from cache.
    /// Returns the file data if found in cache, None otherwise.
    pub(crate) async fn get_file(&self, key: &str) -> Option<Vec<u8>> {
        common_telemetry::info!("Manifest cache get key: {key}");

        // Check if file is in cache index
        if self.get(key).await.is_none() {
            CACHE_MISS.with_label_values(&[MANIFEST_TYPE]).inc();
            return None;
        }

        // Read from local cache store
        let cache_file_path = self.cache_file_path(key);
        match self.local_store.read(&cache_file_path).await {
            Ok(data) => {
                CACHE_HIT.with_label_values(&[MANIFEST_TYPE]).inc();
                Some(data.to_vec())
            }
            Err(e) => {
                warn!(e; "Failed to read cached manifest file {}", cache_file_path);
                CACHE_MISS.with_label_values(&[MANIFEST_TYPE]).inc();
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

    /// Removes empty directories recursively under the manifest cache directory.
    pub(crate) async fn clean_empty_dirs(&self) {
        let root = self.local_store.info().root();
        let manifest_dir = PathBuf::from(root).join(MANIFEST_DIR);
        let manifest_dir_clone = manifest_dir.clone();

        let result =
            tokio::task::spawn_blocking(move || Self::clean_empty_dirs_sync(&manifest_dir_clone))
                .await;

        match result {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                warn!(e; "Failed to clean empty directories under {}", manifest_dir.display());
            }
            Err(e) => {
                warn!(e; "Failed to spawn blocking task for cleaning empty directories");
            }
        }
    }

    /// Removes all manifest files under the given directory from cache and cleans up empty directories.
    ///
    /// This method:
    /// 1. Lists all files under the specified directory
    /// 2. Removes each file from the cache (both index and local store) in batch
    /// 3. Cleans up empty directories under the given directory (including the directory itself)
    pub(crate) async fn clean_manifests(&self, dir: &str) {
        // List all files under the directory
        let cache_dir = join_path(MANIFEST_DIR, dir);
        let mut lister = match self
            .local_store
            .lister_with(&cache_dir)
            .recursive(true)
            .await
        {
            Ok(lister) => lister,
            Err(e) => {
                warn!(e; "Failed to list manifest files under {}", cache_dir);
                return;
            }
        };

        // Collect all file keys to remove
        let mut keys_to_remove = Vec::new();
        loop {
            match lister.try_next().await {
                Ok(Some(entry)) => {
                    let meta = entry.metadata();
                    if meta.is_file() {
                        // The entry name is relative to MANIFEST_DIR, which is what we use as key
                        keys_to_remove.push(entry.name().to_string());
                    }
                }
                Ok(None) => break,
                Err(e) => {
                    warn!(e; "Failed to read entry while listing {}", cache_dir);
                    break;
                }
            }
        }

        // Remove all files from cache in batch
        self.remove_batch(&keys_to_remove).await;

        // Clean up empty directories under the given dir
        let root = self.local_store.info().root();
        let dir_path = PathBuf::from(root).join(&cache_dir);
        let dir_path_clone = dir_path.clone();

        let result =
            tokio::task::spawn_blocking(move || Self::clean_empty_dirs_sync(&dir_path_clone)).await;

        match result {
            Ok(Ok(())) => {
                info!("Cleaned manifest cache for directory: {}", dir);
            }
            Ok(Err(e)) => {
                warn!(e; "Failed to clean empty directories under {}", dir_path.display());
            }
            Err(e) => {
                warn!(e; "Failed to spawn blocking task for cleaning empty directories");
            }
        }
    }

    /// Synchronously removes empty directories recursively.
    fn clean_empty_dirs_sync(dir: &PathBuf) -> std::io::Result<()> {
        Self::remove_empty_dirs_recursive_sync(dir)?;
        Ok(())
    }

    /// Recursively removes empty directories using synchronous filesystem APIs.
    fn remove_empty_dirs_recursive_sync(dir: &PathBuf) -> std::io::Result<bool> {
        let entries = std::fs::read_dir(dir)?;
        let mut is_empty = true;

        for entry in entries {
            let entry = entry?;
            let path = entry.path();
            let metadata = std::fs::metadata(&path)?;

            if metadata.is_dir() {
                // Recursively check subdirectories
                let subdir_empty = Self::remove_empty_dirs_recursive_sync(&path)?;
                if subdir_empty {
                    // Remove the empty subdirectory
                    if let Err(e) = std::fs::remove_dir(&path) {
                        warn!(e; "Failed to remove empty directory {}", path.display());
                        is_empty = false;
                    } else {
                        info!("Removed empty directory {}", path.display());
                    }
                } else {
                    is_empty = false;
                }
            } else {
                // Found a file, directory is not empty
                is_empty = false;
            }
        }

        Ok(is_empty)
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

        let cache = ManifestCache::new(local_store.clone(), ReadableSize::mb(10), None).await;
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
        let cache = ManifestCache::new(local_store.clone(), ReadableSize::mb(10), None).await;

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

        // Create a new cache instance which will automatically recover from local store
        let cache = ManifestCache::new(local_store.clone(), ReadableSize::mb(10), None).await;

        // Wait for recovery to complete synchronously
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

    #[tokio::test]
    async fn test_cache_file_path() {
        let dir = create_temp_dir("");
        let local_store = new_fs_store(dir.path().to_str().unwrap());
        let cache = ManifestCache::new(local_store, ReadableSize::mb(10), None).await;

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
