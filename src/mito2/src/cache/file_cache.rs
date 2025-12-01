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

//! A cache for files.

use std::fmt;
use std::ops::Range;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use common_base::readable_size::ReadableSize;
use common_telemetry::{debug, error, info, warn};
use futures::{AsyncWriteExt, FutureExt, TryStreamExt};
use moka::future::Cache;
use moka::notification::RemovalCause;
use moka::policy::EvictionPolicy;
use object_store::util::join_path;
use object_store::{ErrorKind, ObjectStore, Reader};
use parquet::file::metadata::ParquetMetaData;
use snafu::ResultExt;
use store_api::storage::{FileId, RegionId};
use tokio::sync::mpsc::UnboundedReceiver;

use crate::access_layer::TempFileCleaner;
use crate::cache::{FILE_TYPE, INDEX_TYPE};
use crate::error::{self, OpenDalSnafu, Result};
use crate::metrics::{
    CACHE_BYTES, CACHE_HIT, CACHE_MISS, WRITE_CACHE_DOWNLOAD_BYTES_TOTAL,
    WRITE_CACHE_DOWNLOAD_ELAPSED,
};
use crate::region::opener::RegionLoadCacheTask;
use crate::sst::parquet::helper::fetch_byte_ranges;
use crate::sst::parquet::metadata::MetadataLoader;

/// Subdirectory of cached files for write.
///
/// This must contain three layers, corresponding to [`build_prometheus_metrics_layer`](object_store::layers::build_prometheus_metrics_layer).
const FILE_DIR: &str = "cache/object/write/";

/// Default percentage for index (puffin) cache (20% of total capacity).
pub(crate) const DEFAULT_INDEX_CACHE_PERCENT: u8 = 20;

/// Minimum capacity for each cache (512MB).
const MIN_CACHE_CAPACITY: u64 = 512 * 1024 * 1024;

/// Inner struct for FileCache that can be used in spawned tasks.
#[derive(Debug)]
struct FileCacheInner {
    /// Local store to cache files.
    local_store: ObjectStore,
    /// Index to track cached Parquet files.
    parquet_index: Cache<IndexKey, IndexValue>,
    /// Index to track cached Puffin files.
    puffin_index: Cache<IndexKey, IndexValue>,
}

impl FileCacheInner {
    /// Returns the appropriate memory index for the given file type.
    fn memory_index(&self, file_type: FileType) -> &Cache<IndexKey, IndexValue> {
        match file_type {
            FileType::Parquet => &self.parquet_index,
            FileType::Puffin { .. } => &self.puffin_index,
        }
    }

    /// Returns the cache file path for the key.
    fn cache_file_path(&self, key: IndexKey) -> String {
        cache_file_path(FILE_DIR, key)
    }

    /// Puts a file into the cache index.
    ///
    /// The `WriteCache` should ensure the file is in the correct path.
    async fn put(&self, key: IndexKey, value: IndexValue) {
        CACHE_BYTES
            .with_label_values(&[key.file_type.metric_label()])
            .add(value.file_size.into());
        let index = self.memory_index(key.file_type);
        index.insert(key, value).await;

        // Since files are large items, we run the pending tasks immediately.
        index.run_pending_tasks().await;
    }

    /// Recovers the index from local store.
    async fn recover(&self) -> Result<()> {
        let now = Instant::now();
        let mut lister = self
            .local_store
            .lister_with(FILE_DIR)
            .await
            .context(OpenDalSnafu)?;
        // Use i64 for total_size to reduce the risk of overflow.
        // It is possible that the total size of the cache is larger than i32::MAX.
        let (mut total_size, mut total_keys) = (0i64, 0);
        let (mut parquet_size, mut puffin_size) = (0i64, 0i64);
        while let Some(entry) = lister.try_next().await.context(OpenDalSnafu)? {
            let meta = entry.metadata();
            if !meta.is_file() {
                continue;
            }
            let Some(key) = parse_index_key(entry.name()) else {
                continue;
            };

            let meta = self
                .local_store
                .stat(entry.path())
                .await
                .context(OpenDalSnafu)?;
            let file_size = meta.content_length() as u32;
            let index = self.memory_index(key.file_type);
            index.insert(key, IndexValue { file_size }).await;
            let size = i64::from(file_size);
            total_size += size;
            total_keys += 1;

            // Track sizes separately for each file type
            match key.file_type {
                FileType::Parquet => parquet_size += size,
                FileType::Puffin { .. } => puffin_size += size,
            }
        }
        // The metrics is a signed int gauge so we can updates it finally.
        CACHE_BYTES
            .with_label_values(&[FILE_TYPE])
            .add(parquet_size);
        CACHE_BYTES
            .with_label_values(&[INDEX_TYPE])
            .add(puffin_size);

        // Run all pending tasks of the moka cache so that the cache size is updated
        // and the eviction policy is applied.
        self.parquet_index.run_pending_tasks().await;
        self.puffin_index.run_pending_tasks().await;

        let parquet_weight = self.parquet_index.weighted_size();
        let parquet_count = self.parquet_index.entry_count();
        let puffin_weight = self.puffin_index.weighted_size();
        let puffin_count = self.puffin_index.entry_count();
        info!(
            "Recovered file cache, num_keys: {}, num_bytes: {}, parquet(count: {}, weight: {}), puffin(count: {}, weight: {}), cost: {:?}",
            total_keys,
            total_size,
            parquet_count,
            parquet_weight,
            puffin_count,
            puffin_weight,
            now.elapsed()
        );
        Ok(())
    }

    /// Downloads a file without cleaning up on error.
    async fn download_without_cleaning(
        &self,
        index_key: IndexKey,
        remote_path: &str,
        remote_store: &ObjectStore,
        file_size: u64,
    ) -> Result<()> {
        const DOWNLOAD_READER_CONCURRENCY: usize = 8;
        const DOWNLOAD_READER_CHUNK_SIZE: ReadableSize = ReadableSize::mb(8);

        let file_type = index_key.file_type;
        let timer = WRITE_CACHE_DOWNLOAD_ELAPSED
            .with_label_values(&[match file_type {
                FileType::Parquet => "download_parquet",
                FileType::Puffin { .. } => "download_puffin",
            }])
            .start_timer();

        let reader = remote_store
            .reader_with(remote_path)
            .concurrent(DOWNLOAD_READER_CONCURRENCY)
            .chunk(DOWNLOAD_READER_CHUNK_SIZE.as_bytes() as usize)
            .await
            .context(error::OpenDalSnafu)?
            .into_futures_async_read(0..file_size)
            .await
            .context(error::OpenDalSnafu)?;

        let cache_path = self.cache_file_path(index_key);
        let mut writer = self
            .local_store
            .writer(&cache_path)
            .await
            .context(error::OpenDalSnafu)?
            .into_futures_async_write();

        let region_id = index_key.region_id;
        let file_id = index_key.file_id;
        let bytes_written =
            futures::io::copy(reader, &mut writer)
                .await
                .context(error::DownloadSnafu {
                    region_id,
                    file_id,
                    file_type,
                })?;
        writer.close().await.context(error::DownloadSnafu {
            region_id,
            file_id,
            file_type,
        })?;

        WRITE_CACHE_DOWNLOAD_BYTES_TOTAL.inc_by(bytes_written);

        let elapsed = timer.stop_and_record();
        debug!(
            "Successfully download file '{}' to local '{}', file size: {}, region: {}, cost: {:?}s",
            remote_path, cache_path, bytes_written, region_id, elapsed,
        );

        let index_value = IndexValue {
            file_size: bytes_written as _,
        };
        self.put(index_key, index_value).await;
        Ok(())
    }

    /// Downloads a file from remote store to local cache.
    async fn download(
        &self,
        index_key: IndexKey,
        remote_path: &str,
        remote_store: &ObjectStore,
        file_size: u64,
    ) -> Result<()> {
        if let Err(e) = self
            .download_without_cleaning(index_key, remote_path, remote_store, file_size)
            .await
        {
            let filename = index_key.to_string();
            TempFileCleaner::clean_atomic_dir_files(&self.local_store, &[&filename]).await;

            return Err(e);
        }

        Ok(())
    }
}

/// A file cache manages files on local store and evict files based
/// on size.
#[derive(Debug, Clone)]
pub(crate) struct FileCache {
    /// Inner cache state shared with background worker.
    inner: Arc<FileCacheInner>,
    /// Capacity of the puffin (index) cache in bytes.
    puffin_capacity: u64,
}

pub(crate) type FileCacheRef = Arc<FileCache>;

impl FileCache {
    /// Creates a new file cache.
    pub(crate) fn new(
        local_store: ObjectStore,
        capacity: ReadableSize,
        ttl: Option<Duration>,
        index_cache_percent: Option<u8>,
    ) -> FileCache {
        // Validate and use the provided percent or default
        let index_percent = index_cache_percent
            .filter(|&percent| percent > 0 && percent < 100)
            .unwrap_or(DEFAULT_INDEX_CACHE_PERCENT);
        let total_capacity = capacity.as_bytes();

        // Convert percent to ratio and calculate capacity for each cache
        let index_ratio = index_percent as f64 / 100.0;
        let puffin_capacity = (total_capacity as f64 * index_ratio) as u64;
        let parquet_capacity = total_capacity - puffin_capacity;

        // Ensure both capacities are at least 512MB
        let puffin_capacity = puffin_capacity.max(MIN_CACHE_CAPACITY);
        let parquet_capacity = parquet_capacity.max(MIN_CACHE_CAPACITY);

        info!(
            "Initializing file cache with index_percent: {}%, total_capacity: {}, parquet_capacity: {}, puffin_capacity: {}",
            index_percent,
            ReadableSize(total_capacity),
            ReadableSize(parquet_capacity),
            ReadableSize(puffin_capacity)
        );

        let parquet_index = Self::build_cache(local_store.clone(), parquet_capacity, ttl, "file");
        let puffin_index = Self::build_cache(local_store.clone(), puffin_capacity, ttl, "index");

        // Create inner cache shared with background worker
        let inner = Arc::new(FileCacheInner {
            local_store,
            parquet_index,
            puffin_index,
        });

        FileCache {
            inner,
            puffin_capacity,
        }
    }

    /// Builds a cache for a specific file type.
    fn build_cache(
        local_store: ObjectStore,
        capacity: u64,
        ttl: Option<Duration>,
        label: &'static str,
    ) -> Cache<IndexKey, IndexValue> {
        let cache_store = local_store;
        let mut builder = Cache::builder()
            .eviction_policy(EvictionPolicy::lru())
            .weigher(|_key, value: &IndexValue| -> u32 {
                // We only measure space on local store.
                value.file_size
            })
            .max_capacity(capacity)
            .async_eviction_listener(move |key, value, cause| {
                let store = cache_store.clone();
                // Stores files under FILE_DIR.
                let file_path = cache_file_path(FILE_DIR, *key);
                async move {
                    if let RemovalCause::Replaced = cause {
                        // The cache is replaced by another file. This is unexpected, we don't remove the same
                        // file but updates the metrics as the file is already replaced by users.
                        CACHE_BYTES.with_label_values(&[label]).sub(value.file_size.into());
                        // TODO(yingwen): Don't log warn later.
                        warn!("Replace existing cache {} for region {} unexpectedly", file_path, key.region_id);
                        return;
                    }

                    match store.delete(&file_path).await {
                        Ok(()) => {
                            CACHE_BYTES.with_label_values(&[label]).sub(value.file_size.into());
                        }
                        Err(e) => {
                            warn!(e; "Failed to delete cached file {} for region {}", file_path, key.region_id);
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
    /// The `WriteCache` should ensure the file is in the correct path.
    pub(crate) async fn put(&self, key: IndexKey, value: IndexValue) {
        self.inner.put(key, value).await
    }

    pub(crate) async fn get(&self, key: IndexKey) -> Option<IndexValue> {
        self.inner.memory_index(key.file_type).get(&key).await
    }

    /// Reads a file from the cache.
    #[allow(unused)]
    pub(crate) async fn reader(&self, key: IndexKey) -> Option<Reader> {
        // We must use `get()` to update the estimator of the cache.
        // See https://docs.rs/moka/latest/moka/future/struct.Cache.html#method.contains_key
        let index = self.inner.memory_index(key.file_type);
        if index.get(&key).await.is_none() {
            CACHE_MISS
                .with_label_values(&[key.file_type.metric_label()])
                .inc();
            return None;
        }

        let file_path = self.inner.cache_file_path(key);
        match self.get_reader(&file_path).await {
            Ok(Some(reader)) => {
                CACHE_HIT
                    .with_label_values(&[key.file_type.metric_label()])
                    .inc();
                return Some(reader);
            }
            Err(e) => {
                if e.kind() != ErrorKind::NotFound {
                    warn!(e; "Failed to get file for key {:?}", key);
                }
            }
            Ok(None) => {}
        }

        // We removes the file from the index.
        index.remove(&key).await;
        CACHE_MISS
            .with_label_values(&[key.file_type.metric_label()])
            .inc();
        None
    }

    /// Reads ranges from the cache.
    pub(crate) async fn read_ranges(
        &self,
        key: IndexKey,
        ranges: &[Range<u64>],
    ) -> Option<Vec<Bytes>> {
        let index = self.inner.memory_index(key.file_type);
        if index.get(&key).await.is_none() {
            CACHE_MISS
                .with_label_values(&[key.file_type.metric_label()])
                .inc();
            return None;
        }

        let file_path = self.inner.cache_file_path(key);
        // In most cases, it will use blocking read,
        // because FileCache is normally based on local file system, which supports blocking read.
        let bytes_result =
            fetch_byte_ranges(&file_path, self.inner.local_store.clone(), ranges).await;
        match bytes_result {
            Ok(bytes) => {
                CACHE_HIT
                    .with_label_values(&[key.file_type.metric_label()])
                    .inc();
                Some(bytes)
            }
            Err(e) => {
                if e.kind() != ErrorKind::NotFound {
                    warn!(e; "Failed to get file for key {:?}", key);
                }

                // We removes the file from the index.
                index.remove(&key).await;
                CACHE_MISS
                    .with_label_values(&[key.file_type.metric_label()])
                    .inc();
                None
            }
        }
    }

    /// Removes a file from the cache explicitly.
    /// It always tries to remove the file from the local store because we may not have the file
    /// in the memory index if upload is failed.
    pub(crate) async fn remove(&self, key: IndexKey) {
        let file_path = self.inner.cache_file_path(key);
        self.inner.memory_index(key.file_type).remove(&key).await;
        // Always delete the file from the local store.
        if let Err(e) = self.inner.local_store.delete(&file_path).await {
            warn!(e; "Failed to delete a cached file {}", file_path);
        }
    }

    /// Recovers the index from local store.
    ///
    /// If `task_receiver` is provided, spawns a background task after recovery
    /// to process `RegionLoadCacheTask` messages for loading files into the cache.
    pub(crate) async fn recover(
        &self,
        sync: bool,
        task_receiver: Option<UnboundedReceiver<RegionLoadCacheTask>>,
    ) {
        let moved_self = self.clone();
        let handle = tokio::spawn(async move {
            if let Err(err) = moved_self.inner.recover().await {
                error!(err; "Failed to recover file cache.")
            }

            // Spawns background task to process region load cache tasks after recovery.
            // So it won't block the recovery when `sync` is true.
            if let Some(mut receiver) = task_receiver {
                info!("Spawning background task for processing region load cache tasks");
                tokio::spawn(async move {
                    while let Some(task) = receiver.recv().await {
                        task.fill_cache(&moved_self).await;
                    }
                    info!("Background task for processing region load cache tasks stopped");
                });
            }
        });

        if sync {
            let _ = handle.await;
        }
    }

    /// Returns the cache file path for the key.
    pub(crate) fn cache_file_path(&self, key: IndexKey) -> String {
        self.inner.cache_file_path(key)
    }

    /// Returns the local store of the file cache.
    pub(crate) fn local_store(&self) -> ObjectStore {
        self.inner.local_store.clone()
    }

    /// Get the parquet metadata in file cache.
    /// If the file is not in the cache or fail to load metadata, return None.
    pub(crate) async fn get_parquet_meta_data(&self, key: IndexKey) -> Option<ParquetMetaData> {
        // Check if file cache contains the key
        if let Some(index_value) = self.inner.parquet_index.get(&key).await {
            // Load metadata from file cache
            let local_store = self.local_store();
            let file_path = self.inner.cache_file_path(key);
            let file_size = index_value.file_size as u64;
            let metadata_loader = MetadataLoader::new(local_store, &file_path, file_size);

            match metadata_loader.load().await {
                Ok(metadata) => {
                    CACHE_HIT
                        .with_label_values(&[key.file_type.metric_label()])
                        .inc();
                    Some(metadata)
                }
                Err(e) => {
                    if !e.is_object_not_found() {
                        warn!(
                            e; "Failed to get parquet metadata for key {:?}",
                            key
                        );
                    }
                    // We removes the file from the index.
                    self.inner.parquet_index.remove(&key).await;
                    CACHE_MISS
                        .with_label_values(&[key.file_type.metric_label()])
                        .inc();
                    None
                }
            }
        } else {
            CACHE_MISS
                .with_label_values(&[key.file_type.metric_label()])
                .inc();
            None
        }
    }

    async fn get_reader(&self, file_path: &str) -> object_store::Result<Option<Reader>> {
        if self.inner.local_store.exists(file_path).await? {
            Ok(Some(self.inner.local_store.reader(file_path).await?))
        } else {
            Ok(None)
        }
    }

    /// Checks if the key is in the file cache.
    pub(crate) fn contains_key(&self, key: &IndexKey) -> bool {
        self.inner.memory_index(key.file_type).contains_key(key)
    }

    /// Returns the capacity of the puffin (index) cache in bytes.
    pub(crate) fn puffin_cache_capacity(&self) -> u64 {
        self.puffin_capacity
    }

    /// Returns the current weighted size (used bytes) of the puffin (index) cache.
    pub(crate) fn puffin_cache_size(&self) -> u64 {
        self.inner.puffin_index.weighted_size()
    }

    /// Downloads a file in `remote_path` from the remote object store to the local cache
    /// (specified by `index_key`).
    pub(crate) async fn download(
        &self,
        index_key: IndexKey,
        remote_path: &str,
        remote_store: &ObjectStore,
        file_size: u64,
    ) -> Result<()> {
        self.inner
            .download(index_key, remote_path, remote_store, file_size)
            .await
    }
}

/// Key of file cache index.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct IndexKey {
    pub region_id: RegionId,
    pub file_id: FileId,
    pub file_type: FileType,
}

impl IndexKey {
    /// Creates a new index key.
    pub fn new(region_id: RegionId, file_id: FileId, file_type: FileType) -> IndexKey {
        IndexKey {
            region_id,
            file_id,
            file_type,
        }
    }
}

impl fmt::Display for IndexKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}.{}.{}",
            self.region_id.as_u64(),
            self.file_id,
            self.file_type
        )
    }
}

/// Type of the file.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FileType {
    /// Parquet file.
    Parquet,
    /// Puffin file.
    Puffin(u64),
}

impl std::fmt::Display for FileType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FileType::Parquet => write!(f, "parquet"),
            FileType::Puffin(version) => write!(f, "{}.puffin", version),
        }
    }
}

impl FileType {
    /// Parses the file type from string.
    fn parse(s: &str) -> Option<FileType> {
        match s {
            "parquet" => Some(FileType::Parquet),
            "puffin" => Some(FileType::Puffin(0)),
            _ => {
                // if post-fix with .puffin, try to parse the version
                if let Some(version_str) = s.strip_suffix(".puffin") {
                    let version = version_str.parse::<u64>().ok()?;
                    Some(FileType::Puffin(version))
                } else {
                    None
                }
            }
        }
    }

    /// Returns the metric label for this file type.
    fn metric_label(&self) -> &'static str {
        match self {
            FileType::Parquet => FILE_TYPE,
            FileType::Puffin(_) => INDEX_TYPE,
        }
    }
}

/// An entity that describes the file in the file cache.
///
/// It should only keep minimal information needed by the cache.
#[derive(Debug, Clone)]
pub(crate) struct IndexValue {
    /// Size of the file in bytes.
    pub(crate) file_size: u32,
}

/// Generates the path to the cached file.
///
/// The file name format is `{region_id}.{file_id}.{file_type}`
fn cache_file_path(cache_file_dir: &str, key: IndexKey) -> String {
    join_path(cache_file_dir, &key.to_string())
}

/// Parse index key from the file name.
fn parse_index_key(name: &str) -> Option<IndexKey> {
    let mut split = name.splitn(3, '.');
    let region_id = split.next().and_then(|s| {
        let id = s.parse::<u64>().ok()?;
        Some(RegionId::from_u64(id))
    })?;
    let file_id = split.next().and_then(|s| FileId::parse_str(s).ok())?;
    let file_type = split.next().and_then(FileType::parse)?;

    Some(IndexKey::new(region_id, file_id, file_type))
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
    async fn test_file_cache_ttl() {
        let dir = create_temp_dir("");
        let local_store = new_fs_store(dir.path().to_str().unwrap());

        let cache = FileCache::new(
            local_store.clone(),
            ReadableSize::mb(10),
            Some(Duration::from_millis(10)),
            None,
        );
        let region_id = RegionId::new(2000, 0);
        let file_id = FileId::random();
        let key = IndexKey::new(region_id, file_id, FileType::Parquet);
        let file_path = cache.cache_file_path(key);

        // Get an empty file.
        assert!(cache.reader(key).await.is_none());

        // Write a file.
        local_store
            .write(&file_path, b"hello".as_slice())
            .await
            .unwrap();

        // Add to the cache.
        cache
            .put(
                IndexKey::new(region_id, file_id, FileType::Parquet),
                IndexValue { file_size: 5 },
            )
            .await;

        let exist = cache.reader(key).await;
        assert!(exist.is_some());
        tokio::time::sleep(Duration::from_millis(15)).await;
        cache.inner.parquet_index.run_pending_tasks().await;
        let non = cache.reader(key).await;
        assert!(non.is_none());
    }

    #[tokio::test]
    async fn test_file_cache_basic() {
        let dir = create_temp_dir("");
        let local_store = new_fs_store(dir.path().to_str().unwrap());

        let cache = FileCache::new(local_store.clone(), ReadableSize::mb(10), None, None);
        let region_id = RegionId::new(2000, 0);
        let file_id = FileId::random();
        let key = IndexKey::new(region_id, file_id, FileType::Parquet);
        let file_path = cache.cache_file_path(key);

        // Get an empty file.
        assert!(cache.reader(key).await.is_none());

        // Write a file.
        local_store
            .write(&file_path, b"hello".as_slice())
            .await
            .unwrap();
        // Add to the cache.
        cache
            .put(
                IndexKey::new(region_id, file_id, FileType::Parquet),
                IndexValue { file_size: 5 },
            )
            .await;

        // Read file content.
        let reader = cache.reader(key).await.unwrap();
        let buf = reader.read(..).await.unwrap().to_vec();
        assert_eq!("hello", String::from_utf8(buf).unwrap());

        // Get weighted size.
        cache.inner.parquet_index.run_pending_tasks().await;
        assert_eq!(5, cache.inner.parquet_index.weighted_size());

        // Remove the file.
        cache.remove(key).await;
        assert!(cache.reader(key).await.is_none());

        // Ensure all pending tasks of the moka cache is done before assertion.
        cache.inner.parquet_index.run_pending_tasks().await;

        // The file also not exists.
        assert!(!local_store.exists(&file_path).await.unwrap());
        assert_eq!(0, cache.inner.parquet_index.weighted_size());
    }

    #[tokio::test]
    async fn test_file_cache_file_removed() {
        let dir = create_temp_dir("");
        let local_store = new_fs_store(dir.path().to_str().unwrap());

        let cache = FileCache::new(local_store.clone(), ReadableSize::mb(10), None, None);
        let region_id = RegionId::new(2000, 0);
        let file_id = FileId::random();
        let key = IndexKey::new(region_id, file_id, FileType::Parquet);
        let file_path = cache.cache_file_path(key);

        // Write a file.
        local_store
            .write(&file_path, b"hello".as_slice())
            .await
            .unwrap();
        // Add to the cache.
        cache
            .put(
                IndexKey::new(region_id, file_id, FileType::Parquet),
                IndexValue { file_size: 5 },
            )
            .await;

        // Remove the file but keep the index.
        local_store.delete(&file_path).await.unwrap();

        // Reader is none.
        assert!(cache.reader(key).await.is_none());
        // Key is removed.
        assert!(!cache.inner.parquet_index.contains_key(&key));
    }

    #[tokio::test]
    async fn test_file_cache_recover() {
        let dir = create_temp_dir("");
        let local_store = new_fs_store(dir.path().to_str().unwrap());
        let cache = FileCache::new(local_store.clone(), ReadableSize::mb(10), None, None);

        let region_id = RegionId::new(2000, 0);
        let file_type = FileType::Parquet;
        // Write N files.
        let file_ids: Vec<_> = (0..10).map(|_| FileId::random()).collect();
        let mut total_size = 0;
        for (i, file_id) in file_ids.iter().enumerate() {
            let key = IndexKey::new(region_id, *file_id, file_type);
            let file_path = cache.cache_file_path(key);
            let bytes = i.to_string().into_bytes();
            local_store.write(&file_path, bytes.clone()).await.unwrap();

            // Add to the cache.
            cache
                .put(
                    IndexKey::new(region_id, *file_id, file_type),
                    IndexValue {
                        file_size: bytes.len() as u32,
                    },
                )
                .await;
            total_size += bytes.len();
        }

        // Recover the cache.
        let cache = FileCache::new(local_store.clone(), ReadableSize::mb(10), None, None);
        // No entry before recovery.
        assert!(
            cache
                .reader(IndexKey::new(region_id, file_ids[0], file_type))
                .await
                .is_none()
        );
        cache.recover(true, None).await;

        // Check size.
        cache.inner.parquet_index.run_pending_tasks().await;
        assert_eq!(
            total_size,
            cache.inner.parquet_index.weighted_size() as usize
        );

        for (i, file_id) in file_ids.iter().enumerate() {
            let key = IndexKey::new(region_id, *file_id, file_type);
            let reader = cache.reader(key).await.unwrap();
            let buf = reader.read(..).await.unwrap().to_vec();
            assert_eq!(i.to_string(), String::from_utf8(buf).unwrap());
        }
    }

    #[tokio::test]
    async fn test_file_cache_read_ranges() {
        let dir = create_temp_dir("");
        let local_store = new_fs_store(dir.path().to_str().unwrap());
        let file_cache = FileCache::new(local_store.clone(), ReadableSize::mb(10), None, None);
        let region_id = RegionId::new(2000, 0);
        let file_id = FileId::random();
        let key = IndexKey::new(region_id, file_id, FileType::Parquet);
        let file_path = file_cache.cache_file_path(key);
        // Write a file.
        let data = b"hello greptime database";
        local_store
            .write(&file_path, data.as_slice())
            .await
            .unwrap();
        // Add to the cache.
        file_cache.put(key, IndexValue { file_size: 5 }).await;
        // Ranges
        let ranges = vec![0..5, 6..10, 15..19, 0..data.len() as u64];
        let bytes = file_cache.read_ranges(key, &ranges).await.unwrap();

        assert_eq!(4, bytes.len());
        assert_eq!(b"hello", bytes[0].as_ref());
        assert_eq!(b"grep", bytes[1].as_ref());
        assert_eq!(b"data", bytes[2].as_ref());
        assert_eq!(data, bytes[3].as_ref());
    }

    #[test]
    fn test_cache_file_path() {
        let file_id = FileId::parse_str("3368731b-a556-42b8-a5df-9c31ce155095").unwrap();
        assert_eq!(
            "test_dir/5299989643269.3368731b-a556-42b8-a5df-9c31ce155095.parquet",
            cache_file_path(
                "test_dir",
                IndexKey::new(RegionId::new(1234, 5), file_id, FileType::Parquet)
            )
        );
        assert_eq!(
            "test_dir/5299989643269.3368731b-a556-42b8-a5df-9c31ce155095.parquet",
            cache_file_path(
                "test_dir/",
                IndexKey::new(RegionId::new(1234, 5), file_id, FileType::Parquet)
            )
        );
    }

    #[test]
    fn test_parse_file_name() {
        let file_id = FileId::parse_str("3368731b-a556-42b8-a5df-9c31ce155095").unwrap();
        let region_id = RegionId::new(1234, 5);
        assert_eq!(
            IndexKey::new(region_id, file_id, FileType::Parquet),
            parse_index_key("5299989643269.3368731b-a556-42b8-a5df-9c31ce155095.parquet").unwrap()
        );
        assert_eq!(
            IndexKey::new(region_id, file_id, FileType::Puffin(0)),
            parse_index_key("5299989643269.3368731b-a556-42b8-a5df-9c31ce155095.puffin").unwrap()
        );
        assert_eq!(
            IndexKey::new(region_id, file_id, FileType::Puffin(42)),
            parse_index_key("5299989643269.3368731b-a556-42b8-a5df-9c31ce155095.42.puffin")
                .unwrap()
        );
        assert!(parse_index_key("").is_none());
        assert!(parse_index_key(".").is_none());
        assert!(parse_index_key("5299989643269").is_none());
        assert!(parse_index_key("5299989643269.").is_none());
        assert!(parse_index_key(".5299989643269").is_none());
        assert!(parse_index_key("5299989643269.").is_none());
        assert!(parse_index_key("5299989643269.3368731b-a556-42b8-a5df").is_none());
        assert!(parse_index_key("5299989643269.3368731b-a556-42b8-a5df-9c31ce155095").is_none());
        assert!(
            parse_index_key("5299989643269.3368731b-a556-42b8-a5df-9c31ce155095.parque").is_none()
        );
        assert!(
            parse_index_key("5299989643269.3368731b-a556-42b8-a5df-9c31ce155095.parquet.puffin")
                .is_none()
        );
    }
}
