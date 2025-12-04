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

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use async_walkdir::{Filtering, WalkDir};
use base64::Engine;
use base64::prelude::BASE64_URL_SAFE;
use common_base::range_read::FileReader;
use common_runtime::runtime::RuntimeTrait;
use common_telemetry::{info, warn};
use futures::{FutureExt, StreamExt};
use moka::future::Cache;
use moka::policy::EvictionPolicy;
use sha2::{Digest, Sha256};
use snafu::ResultExt;
use tokio::fs;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio_util::compat::TokioAsyncWriteCompatExt;

use crate::error::{
    CacheGetSnafu, CreateSnafu, MetadataSnafu, OpenSnafu, ReadSnafu, RemoveSnafu, RenameSnafu,
    Result, WalkDirSnafu,
};
use crate::puffin_manager::stager::{
    BoxWriter, DirWriterProvider, InitBlobFn, InitDirFn, Stager, StagerNotifier,
};
use crate::puffin_manager::{BlobGuard, DirGuard, DirMetrics};

const DELETE_QUEUE_SIZE: usize = 10240;
const TMP_EXTENSION: &str = "tmp";
const DELETED_EXTENSION: &str = "deleted";
const RECYCLE_BIN_TTL: Duration = Duration::from_secs(60);

/// `BoundedStager` is a `Stager` that uses `moka` to manage staging area.
pub struct BoundedStager<H> {
    /// The base directory of the staging area.
    base_dir: PathBuf,

    /// The cache maintaining the cache key to the size of the file or directory.
    cache: Cache<String, CacheValue>,

    /// The recycle bin for the deleted files and directories.
    recycle_bin: Cache<String, CacheValue>,

    /// The delete queue for the cleanup task.
    ///
    /// The lifetime of a guard is:
    ///   1. initially inserted into the cache
    ///   2. moved to the recycle bin when evicted
    ///      2.1 moved back to the cache when accessed
    ///      2.2 deleted from the recycle bin after a certain period
    ///   3. sent the delete task to the delete queue on drop
    ///   4. background routine removes the file or directory
    delete_queue: Sender<DeleteTask>,

    /// Notifier for the stager.
    notifier: Option<Arc<dyn StagerNotifier>>,

    _phantom: std::marker::PhantomData<H>,
}

impl<H: 'static> BoundedStager<H> {
    pub async fn new(
        base_dir: PathBuf,
        capacity: u64,
        notifier: Option<Arc<dyn StagerNotifier>>,
        cache_ttl: Option<Duration>,
    ) -> Result<Self> {
        tokio::fs::create_dir_all(&base_dir)
            .await
            .context(CreateSnafu)?;

        let recycle_bin = Cache::builder().time_to_idle(RECYCLE_BIN_TTL).build();
        let recycle_bin_cloned = recycle_bin.clone();
        let notifier_cloned = notifier.clone();

        let mut cache_builder = Cache::builder()
            .max_capacity(capacity)
            .weigher(|_: &String, v: &CacheValue| v.weight())
            .eviction_policy(EvictionPolicy::lru())
            .support_invalidation_closures()
            .async_eviction_listener(move |k, v, _| {
                let recycle_bin = recycle_bin_cloned.clone();
                if let Some(notifier) = notifier_cloned.as_ref() {
                    notifier.on_cache_evict(v.size());
                    notifier.on_recycle_insert(v.size());
                }
                async move {
                    recycle_bin.insert(k.as_str().to_string(), v).await;
                }
                .boxed()
            });
        if let Some(ttl) = cache_ttl
            && !ttl.is_zero()
        {
            cache_builder = cache_builder.time_to_live(ttl);
        }
        let cache = cache_builder.build();

        let (delete_queue, rx) = tokio::sync::mpsc::channel(DELETE_QUEUE_SIZE);
        let notifier_cloned = notifier.clone();
        common_runtime::global_runtime().spawn(Self::delete_routine(
            rx,
            recycle_bin.clone(),
            notifier_cloned,
        ));
        let stager = Self {
            cache,
            base_dir,
            delete_queue,
            recycle_bin,
            notifier,
            _phantom: std::marker::PhantomData,
        };

        stager.recover().await?;

        Ok(stager)
    }
}

#[async_trait]
impl<H: ToString + Clone + Send + Sync> Stager for BoundedStager<H> {
    type Blob = Arc<FsBlobGuard>;
    type Dir = Arc<FsDirGuard>;
    type FileHandle = H;

    async fn get_blob<'a>(
        &self,
        handle: &Self::FileHandle,
        key: &str,
        init_fn: Box<dyn InitBlobFn + Send + Sync + 'a>,
    ) -> Result<Self::Blob> {
        let handle_str = handle.to_string();
        let cache_key = Self::encode_cache_key(&handle_str, key);

        let mut miss = false;
        let v = self
            .cache
            .try_get_with_by_ref(&cache_key, async {
                if let Some(v) = self.recycle_bin.remove(&cache_key).await {
                    if let Some(notifier) = self.notifier.as_ref() {
                        let size = v.size();
                        notifier.on_cache_insert(size);
                        notifier.on_recycle_clear(size);
                    }
                    return Ok(v);
                }

                miss = true;
                let timer = Instant::now();
                let file_name = format!("{}.{}", cache_key, uuid::Uuid::new_v4());
                let path = self.base_dir.join(&file_name);

                let size = Self::write_blob(&path, init_fn).await?;
                if let Some(notifier) = self.notifier.as_ref() {
                    notifier.on_cache_insert(size);
                    notifier.on_load_blob(timer.elapsed());
                }
                let guard = Arc::new(FsBlobGuard {
                    handle: handle_str,
                    path,
                    delete_queue: self.delete_queue.clone(),
                    size,
                });
                Ok(CacheValue::File(guard))
            })
            .await
            .context(CacheGetSnafu)?;

        if let Some(notifier) = self.notifier.as_ref() {
            if miss {
                notifier.on_cache_miss(v.size());
            } else {
                notifier.on_cache_hit(v.size());
            }
        }
        match v {
            CacheValue::File(guard) => Ok(guard),
            _ => unreachable!(),
        }
    }

    async fn get_dir<'a>(
        &self,
        handle: &Self::FileHandle,
        key: &str,
        init_fn: Box<dyn InitDirFn + Send + Sync + 'a>,
    ) -> Result<(Self::Dir, DirMetrics)> {
        let handle_str = handle.to_string();

        let cache_key = Self::encode_cache_key(&handle_str, key);

        let mut miss = false;
        let v = self
            .cache
            .try_get_with_by_ref(&cache_key, async {
                if let Some(v) = self.recycle_bin.remove(&cache_key).await {
                    if let Some(notifier) = self.notifier.as_ref() {
                        let size = v.size();
                        notifier.on_cache_insert(size);
                        notifier.on_recycle_clear(size);
                    }
                    return Ok(v);
                }

                miss = true;
                let timer = Instant::now();
                let dir_name = format!("{}.{}", cache_key, uuid::Uuid::new_v4());
                let path = self.base_dir.join(&dir_name);

                let size = Self::write_dir(&path, init_fn).await?;
                if let Some(notifier) = self.notifier.as_ref() {
                    notifier.on_cache_insert(size);
                    notifier.on_load_dir(timer.elapsed());
                }
                let guard = Arc::new(FsDirGuard {
                    handle: handle_str,
                    path,
                    size,
                    delete_queue: self.delete_queue.clone(),
                });
                Ok(CacheValue::Dir(guard))
            })
            .await
            .context(CacheGetSnafu)?;

        let dir_size = v.size();
        if let Some(notifier) = self.notifier.as_ref() {
            if miss {
                notifier.on_cache_miss(dir_size);
            } else {
                notifier.on_cache_hit(dir_size);
            }
        }

        let metrics = DirMetrics {
            cache_hit: !miss,
            dir_size,
        };

        match v {
            CacheValue::Dir(guard) => Ok((guard, metrics)),
            _ => unreachable!(),
        }
    }

    async fn put_dir(
        &self,
        handle: &Self::FileHandle,
        key: &str,
        dir_path: PathBuf,
        size: u64,
    ) -> Result<()> {
        let handle_str = handle.to_string();
        let cache_key = Self::encode_cache_key(&handle_str, key);

        self.cache
            .try_get_with(cache_key.clone(), async move {
                if let Some(v) = self.recycle_bin.remove(&cache_key).await {
                    if let Some(notifier) = self.notifier.as_ref() {
                        let size = v.size();
                        notifier.on_cache_insert(size);
                        notifier.on_recycle_clear(size);
                    }
                    return Ok(v);
                }

                let dir_name = format!("{}.{}", cache_key, uuid::Uuid::new_v4());
                let path = self.base_dir.join(&dir_name);

                fs::rename(&dir_path, &path).await.context(RenameSnafu)?;
                if let Some(notifier) = self.notifier.as_ref() {
                    notifier.on_cache_insert(size);
                }
                let guard = Arc::new(FsDirGuard {
                    handle: handle_str,
                    path,
                    size,
                    delete_queue: self.delete_queue.clone(),
                });
                Ok(CacheValue::Dir(guard))
            })
            .await
            .map(|_| ())
            .context(CacheGetSnafu)?;

        // Dir is usually large.
        // Runs pending tasks of the cache and recycle bin to free up space
        // more quickly.
        self.cache.run_pending_tasks().await;
        self.recycle_bin.run_pending_tasks().await;

        Ok(())
    }

    async fn purge(&self, handle: &Self::FileHandle) -> Result<()> {
        let handle_str = handle.to_string();
        self.cache
            .invalidate_entries_if(move |_k, v| v.handle() == handle_str)
            .unwrap(); // SAFETY: `support_invalidation_closures` is enabled
        self.cache.run_pending_tasks().await;
        Ok(())
    }
}

impl<H> BoundedStager<H> {
    fn encode_cache_key(puffin_file_name: &str, key: &str) -> String {
        let mut hasher = Sha256::new();
        hasher.update(puffin_file_name);
        hasher.update(key);
        hasher.update(puffin_file_name);
        let hash = hasher.finalize();

        BASE64_URL_SAFE.encode(hash)
    }

    async fn write_blob(
        target_path: &PathBuf,
        init_fn: Box<dyn InitBlobFn + Send + Sync + '_>,
    ) -> Result<u64> {
        // To guarantee the atomicity of writing the file, we need to write
        // the file to a temporary file first...
        let tmp_path = target_path.with_extension(TMP_EXTENSION);
        let writer = Box::new(
            fs::File::create(&tmp_path)
                .await
                .context(CreateSnafu)?
                .compat_write(),
        );
        let size = init_fn(writer).await?;

        // ...then rename the temporary file to the target path
        fs::rename(tmp_path, target_path)
            .await
            .context(RenameSnafu)?;
        Ok(size)
    }

    async fn write_dir(
        target_path: &PathBuf,
        init_fn: Box<dyn InitDirFn + Send + Sync + '_>,
    ) -> Result<u64> {
        // To guarantee the atomicity of writing the directory, we need to write
        // the directory to a temporary directory first...
        let tmp_base = target_path.with_extension(TMP_EXTENSION);
        let writer_provider = Box::new(MokaDirWriterProvider(tmp_base.clone()));
        let size = init_fn(writer_provider).await?;

        // ...then rename the temporary directory to the target path
        fs::rename(&tmp_base, target_path)
            .await
            .context(RenameSnafu)?;
        Ok(size)
    }

    /// Recovers the staging area by iterating through the staging directory.
    ///
    /// Note: It can't recover the mapping between puffin files and keys, so TTL
    ///       is configured to purge the dangling files and directories.
    async fn recover(&self) -> Result<()> {
        let timer = std::time::Instant::now();
        info!("Recovering the staging area, base_dir: {:?}", self.base_dir);

        let mut read_dir = fs::read_dir(&self.base_dir).await.context(ReadSnafu)?;

        let mut elems = HashMap::new();
        while let Some(entry) = read_dir.next_entry().await.context(ReadSnafu)? {
            let path = entry.path();

            if path.extension() == Some(TMP_EXTENSION.as_ref())
                || path.extension() == Some(DELETED_EXTENSION.as_ref())
            {
                // Remove temporary or deleted files and directories
                if entry.metadata().await.context(MetadataSnafu)?.is_dir() {
                    fs::remove_dir_all(path).await.context(RemoveSnafu)?;
                } else {
                    fs::remove_file(path).await.context(RemoveSnafu)?;
                }
            } else {
                // Insert the guard of the file or directory to the cache
                let meta = entry.metadata().await.context(MetadataSnafu)?;
                let file_path = path.file_name().unwrap().to_string_lossy().into_owned();

                // <key>.<uuid>
                let key = match file_path.split('.').next() {
                    Some(key) => key.to_string(),
                    None => {
                        warn!(
                            "Invalid staging file name: {}, expected format: <key>.<uuid>",
                            file_path
                        );
                        continue;
                    }
                };

                if meta.is_dir() {
                    let size = Self::get_dir_size(&path).await?;
                    let v = CacheValue::Dir(Arc::new(FsDirGuard {
                        path,
                        size,
                        delete_queue: self.delete_queue.clone(),

                        // placeholder
                        handle: String::new(),
                    }));
                    // A duplicate dir will be moved to the delete queue.
                    let _dup_dir = elems.insert(key, v);
                } else {
                    let size = meta.len();
                    let v = CacheValue::File(Arc::new(FsBlobGuard {
                        path,
                        size,
                        delete_queue: self.delete_queue.clone(),

                        // placeholder
                        handle: String::new(),
                    }));
                    // A duplicate file will be moved to the delete queue.
                    let _dup_file = elems.insert(key, v);
                }
            }
        }

        let mut size = 0;
        let num_elems = elems.len();
        for (key, value) in elems {
            size += value.size();
            self.cache.insert(key, value).await;
        }
        if let Some(notifier) = self.notifier.as_ref() {
            notifier.on_cache_insert(size);
        }

        self.cache.run_pending_tasks().await;

        info!(
            "Recovered the staging area, num_entries: {}, num_bytes: {}, cost: {:?}",
            num_elems,
            size,
            timer.elapsed()
        );
        Ok(())
    }

    /// Walks through the directory and calculate the total size of all files in the directory.
    async fn get_dir_size(path: &PathBuf) -> Result<u64> {
        let mut size = 0;
        let mut wd = WalkDir::new(path).filter(|entry| async move {
            match entry.file_type().await {
                Ok(ft) if ft.is_dir() => Filtering::Ignore,
                _ => Filtering::Continue,
            }
        });

        while let Some(entry) = wd.next().await {
            let entry = entry.context(WalkDirSnafu)?;
            size += entry.metadata().await.context(MetadataSnafu)?.len();
        }

        Ok(size)
    }

    async fn delete_routine(
        mut receiver: Receiver<DeleteTask>,
        recycle_bin: Cache<String, CacheValue>,
        notifier: Option<Arc<dyn StagerNotifier>>,
    ) {
        loop {
            match tokio::time::timeout(RECYCLE_BIN_TTL, receiver.recv()).await {
                Ok(Some(task)) => match task {
                    DeleteTask::File(path, size) => {
                        if let Err(err) = fs::remove_file(&path).await {
                            if err.kind() == std::io::ErrorKind::NotFound {
                                continue;
                            }

                            warn!(err; "Failed to remove the file.");
                        }

                        if let Some(notifier) = notifier.as_ref() {
                            notifier.on_recycle_clear(size);
                        }
                    }

                    DeleteTask::Dir(path, size) => {
                        let deleted_path = path.with_extension(DELETED_EXTENSION);
                        if let Err(err) = fs::rename(&path, &deleted_path).await {
                            if err.kind() == std::io::ErrorKind::NotFound {
                                continue;
                            }

                            // Remove the deleted directory if the rename fails and retry
                            let _ = fs::remove_dir_all(&deleted_path).await;
                            if let Err(err) = fs::rename(&path, &deleted_path).await {
                                warn!(err; "Failed to rename the dangling directory to deleted path.");
                                continue;
                            }
                        }
                        if let Err(err) = fs::remove_dir_all(&deleted_path).await {
                            warn!(err; "Failed to remove the dangling directory.");
                        }
                        if let Some(notifier) = notifier.as_ref() {
                            notifier.on_recycle_clear(size);
                        }
                    }
                    DeleteTask::Terminate => {
                        break;
                    }
                },
                Ok(None) => break,
                Err(_) => {
                    // Purge recycle bin periodically to reclaim the space quickly.
                    recycle_bin.run_pending_tasks().await;
                }
            }
        }

        info!("The delete routine for the bounded stager is terminated.");
    }
}

impl<H> Drop for BoundedStager<H> {
    fn drop(&mut self) {
        let _ = self.delete_queue.try_send(DeleteTask::Terminate);
    }
}

#[derive(Debug, Clone)]
enum CacheValue {
    File(Arc<FsBlobGuard>),
    Dir(Arc<FsDirGuard>),
}

impl CacheValue {
    fn size(&self) -> u64 {
        match self {
            CacheValue::File(guard) => guard.size,
            CacheValue::Dir(guard) => guard.size,
        }
    }

    fn weight(&self) -> u32 {
        self.size().try_into().unwrap_or(u32::MAX)
    }

    fn handle(&self) -> &str {
        match self {
            CacheValue::File(guard) => &guard.handle,
            CacheValue::Dir(guard) => &guard.handle,
        }
    }
}

enum DeleteTask {
    File(PathBuf, u64),
    Dir(PathBuf, u64),
    Terminate,
}

/// `FsBlobGuard` is a `BlobGuard` for accessing the blob and
/// automatically deleting the file on drop.
#[derive(Debug)]
pub struct FsBlobGuard {
    handle: String,
    path: PathBuf,
    size: u64,
    delete_queue: Sender<DeleteTask>,
}

#[async_trait]
impl BlobGuard for FsBlobGuard {
    type Reader = FileReader;

    async fn reader(&self) -> Result<Self::Reader> {
        FileReader::new(&self.path).await.context(OpenSnafu)
    }
}

impl Drop for FsBlobGuard {
    fn drop(&mut self) {
        if let Err(err) = self
            .delete_queue
            .try_send(DeleteTask::File(self.path.clone(), self.size))
        {
            if matches!(err, TrySendError::Closed(_)) {
                return;
            }
            warn!(err; "Failed to send the delete task for the file.");
        }
    }
}

/// `FsDirGuard` is a `DirGuard` for accessing the directory and
/// automatically deleting the directory on drop.
#[derive(Debug)]
pub struct FsDirGuard {
    handle: String,
    path: PathBuf,
    size: u64,
    delete_queue: Sender<DeleteTask>,
}

impl DirGuard for FsDirGuard {
    fn path(&self) -> &PathBuf {
        &self.path
    }
}

impl Drop for FsDirGuard {
    fn drop(&mut self) {
        if let Err(err) = self
            .delete_queue
            .try_send(DeleteTask::Dir(self.path.clone(), self.size))
        {
            if matches!(err, TrySendError::Closed(_)) {
                return;
            }
            warn!(err; "Failed to send the delete task for the directory.");
        }
    }
}

/// `MokaDirWriterProvider` implements `DirWriterProvider` for initializing a directory.
struct MokaDirWriterProvider(PathBuf);

#[async_trait]
impl DirWriterProvider for MokaDirWriterProvider {
    async fn writer(&self, rel_path: &str) -> Result<BoxWriter> {
        let full_path = if cfg!(windows) {
            self.0.join(rel_path.replace('/', "\\"))
        } else {
            self.0.join(rel_path)
        };
        if let Some(parent) = full_path.parent() {
            fs::create_dir_all(parent).await.context(CreateSnafu)?;
        }
        Ok(Box::new(
            fs::File::create(full_path)
                .await
                .context(CreateSnafu)?
                .compat_write(),
        ) as BoxWriter)
    }
}

#[cfg(test)]
impl<H> BoundedStager<H> {
    pub async fn must_get_file(&self, puffin_file_name: &str, key: &str) -> fs::File {
        let cache_key = Self::encode_cache_key(puffin_file_name, key);
        let value = self.cache.get(&cache_key).await.unwrap();
        let path = match &value {
            CacheValue::File(guard) => &guard.path,
            _ => panic!("Expected a file, but got a directory."),
        };
        fs::File::open(path).await.unwrap()
    }

    pub async fn must_get_dir(&self, puffin_file_name: &str, key: &str) -> PathBuf {
        let cache_key = Self::encode_cache_key(puffin_file_name, key);
        let value = self.cache.get(&cache_key).await.unwrap();
        let path = match &value {
            CacheValue::Dir(guard) => &guard.path,
            _ => panic!("Expected a directory, but got a file."),
        };
        path.clone()
    }

    pub fn in_cache(&self, puffin_file_name: &str, key: &str) -> bool {
        let cache_key = Self::encode_cache_key(puffin_file_name, key);
        self.cache.contains_key(&cache_key)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicU64;

    use common_base::range_read::RangeReader;
    use common_test_util::temp_dir::create_temp_dir;
    use futures::AsyncWriteExt;
    use tokio::io::AsyncReadExt as _;

    use super::*;
    use crate::error::BlobNotFoundSnafu;
    use crate::puffin_manager::stager::Stager;

    struct MockNotifier {
        cache_insert_size: AtomicU64,
        cache_evict_size: AtomicU64,
        cache_hit_count: AtomicU64,
        cache_hit_size: AtomicU64,
        cache_miss_count: AtomicU64,
        cache_miss_size: AtomicU64,
        recycle_insert_size: AtomicU64,
        recycle_clear_size: AtomicU64,
    }

    #[derive(Debug, PartialEq, Eq)]
    struct Stats {
        cache_insert_size: u64,
        cache_evict_size: u64,
        cache_hit_count: u64,
        cache_hit_size: u64,
        cache_miss_count: u64,
        cache_miss_size: u64,
        recycle_insert_size: u64,
        recycle_clear_size: u64,
    }

    impl MockNotifier {
        fn build() -> Arc<MockNotifier> {
            Arc::new(Self {
                cache_insert_size: AtomicU64::new(0),
                cache_evict_size: AtomicU64::new(0),
                cache_hit_count: AtomicU64::new(0),
                cache_hit_size: AtomicU64::new(0),
                cache_miss_count: AtomicU64::new(0),
                cache_miss_size: AtomicU64::new(0),
                recycle_insert_size: AtomicU64::new(0),
                recycle_clear_size: AtomicU64::new(0),
            })
        }

        fn stats(&self) -> Stats {
            Stats {
                cache_insert_size: self
                    .cache_insert_size
                    .load(std::sync::atomic::Ordering::Relaxed),
                cache_evict_size: self
                    .cache_evict_size
                    .load(std::sync::atomic::Ordering::Relaxed),
                cache_hit_count: self
                    .cache_hit_count
                    .load(std::sync::atomic::Ordering::Relaxed),
                cache_hit_size: self
                    .cache_hit_size
                    .load(std::sync::atomic::Ordering::Relaxed),
                cache_miss_count: self
                    .cache_miss_count
                    .load(std::sync::atomic::Ordering::Relaxed),
                cache_miss_size: self
                    .cache_miss_size
                    .load(std::sync::atomic::Ordering::Relaxed),
                recycle_insert_size: self
                    .recycle_insert_size
                    .load(std::sync::atomic::Ordering::Relaxed),
                recycle_clear_size: self
                    .recycle_clear_size
                    .load(std::sync::atomic::Ordering::Relaxed),
            }
        }
    }

    impl StagerNotifier for MockNotifier {
        fn on_cache_insert(&self, size: u64) {
            self.cache_insert_size
                .fetch_add(size, std::sync::atomic::Ordering::Relaxed);
        }

        fn on_cache_evict(&self, size: u64) {
            self.cache_evict_size
                .fetch_add(size, std::sync::atomic::Ordering::Relaxed);
        }

        fn on_cache_hit(&self, size: u64) {
            self.cache_hit_count
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            self.cache_hit_size
                .fetch_add(size, std::sync::atomic::Ordering::Relaxed);
        }

        fn on_cache_miss(&self, size: u64) {
            self.cache_miss_count
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            self.cache_miss_size
                .fetch_add(size, std::sync::atomic::Ordering::Relaxed);
        }

        fn on_recycle_insert(&self, size: u64) {
            self.recycle_insert_size
                .fetch_add(size, std::sync::atomic::Ordering::Relaxed);
        }

        fn on_recycle_clear(&self, size: u64) {
            self.recycle_clear_size
                .fetch_add(size, std::sync::atomic::Ordering::Relaxed);
        }

        fn on_load_blob(&self, _duration: Duration) {}

        fn on_load_dir(&self, _duration: Duration) {}
    }

    #[tokio::test]
    async fn test_get_blob() {
        let tempdir = create_temp_dir("test_get_blob_");
        let notifier = MockNotifier::build();
        let stager = BoundedStager::new(
            tempdir.path().to_path_buf(),
            u64::MAX,
            Some(notifier.clone()),
            None,
        )
        .await
        .unwrap();

        let puffin_file_name = "test_get_blob".to_string();
        let key = "key";
        let reader = stager
            .get_blob(
                &puffin_file_name,
                key,
                Box::new(|mut writer| {
                    Box::pin(async move {
                        writer.write_all(b"hello world").await.unwrap();
                        Ok(11)
                    })
                }),
            )
            .await
            .unwrap()
            .reader()
            .await
            .unwrap();

        let m = reader.metadata().await.unwrap();
        let buf = reader.read(0..m.content_length).await.unwrap();
        assert_eq!(&*buf, b"hello world");

        let mut file = stager.must_get_file(&puffin_file_name, key).await;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf).await.unwrap();
        assert_eq!(buf, b"hello world");

        let stats = notifier.stats();
        assert_eq!(
            stats,
            Stats {
                cache_insert_size: 11,
                cache_evict_size: 0,
                cache_hit_count: 0,
                cache_hit_size: 0,
                cache_miss_count: 1,
                cache_miss_size: 11,
                recycle_insert_size: 0,
                recycle_clear_size: 0,
            }
        );
    }

    #[tokio::test]
    async fn test_get_dir() {
        let tempdir = create_temp_dir("test_get_dir_");
        let notifier = MockNotifier::build();
        let stager = BoundedStager::new(
            tempdir.path().to_path_buf(),
            u64::MAX,
            Some(notifier.clone()),
            None,
        )
        .await
        .unwrap();

        let files_in_dir = [
            ("file_a", "Hello, world!".as_bytes()),
            ("file_b", "Hello, Rust!".as_bytes()),
            ("file_c", "你好，世界！".as_bytes()),
            ("subdir/file_d", "Hello, Puffin!".as_bytes()),
            ("subdir/subsubdir/file_e", "¡Hola mundo!".as_bytes()),
        ];

        let puffin_file_name = "test_get_dir".to_string();
        let key = "key";
        let (dir_path, metrics) = stager
            .get_dir(
                &puffin_file_name,
                key,
                Box::new(|writer_provider| {
                    Box::pin(async move {
                        let mut size = 0;
                        for (rel_path, content) in &files_in_dir {
                            size += content.len();
                            let mut writer = writer_provider.writer(rel_path).await.unwrap();
                            writer.write_all(content).await.unwrap();
                        }
                        Ok(size as _)
                    })
                }),
            )
            .await
            .unwrap();

        assert!(!metrics.cache_hit);
        assert!(metrics.dir_size > 0);

        for (rel_path, content) in &files_in_dir {
            let file_path = dir_path.path().join(rel_path);
            let mut file = tokio::fs::File::open(&file_path).await.unwrap();
            let mut buf = Vec::new();
            file.read_to_end(&mut buf).await.unwrap();
            assert_eq!(buf, *content);
        }

        let dir_path = stager.must_get_dir(&puffin_file_name, key).await;
        for (rel_path, content) in &files_in_dir {
            let file_path = dir_path.join(rel_path);
            let mut file = tokio::fs::File::open(&file_path).await.unwrap();
            let mut buf = Vec::new();
            file.read_to_end(&mut buf).await.unwrap();
            assert_eq!(buf, *content);
        }

        let stats = notifier.stats();
        assert_eq!(
            stats,
            Stats {
                cache_insert_size: 70,
                cache_evict_size: 0,
                cache_hit_count: 0,
                cache_hit_size: 0,
                cache_miss_count: 1,
                cache_miss_size: 70,
                recycle_insert_size: 0,
                recycle_clear_size: 0
            }
        );
    }

    #[tokio::test]
    async fn test_recover() {
        let tempdir = create_temp_dir("test_recover_");
        let notifier = MockNotifier::build();
        let stager = BoundedStager::new(
            tempdir.path().to_path_buf(),
            u64::MAX,
            Some(notifier.clone()),
            None,
        )
        .await
        .unwrap();

        // initialize stager
        let puffin_file_name = "test_recover".to_string();
        let blob_key = "blob_key";
        let guard = stager
            .get_blob(
                &puffin_file_name,
                blob_key,
                Box::new(|mut writer| {
                    Box::pin(async move {
                        writer.write_all(b"hello world").await.unwrap();
                        Ok(11)
                    })
                }),
            )
            .await
            .unwrap();
        drop(guard);

        let files_in_dir = [
            ("file_a", "Hello, world!".as_bytes()),
            ("file_b", "Hello, Rust!".as_bytes()),
            ("file_c", "你好，世界！".as_bytes()),
            ("subdir/file_d", "Hello, Puffin!".as_bytes()),
            ("subdir/subsubdir/file_e", "¡Hola mundo!".as_bytes()),
        ];

        let dir_key = "dir_key";
        let (guard, _metrics) = stager
            .get_dir(
                &puffin_file_name,
                dir_key,
                Box::new(|writer_provider| {
                    Box::pin(async move {
                        let mut size = 0;
                        for (rel_path, content) in &files_in_dir {
                            size += content.len();
                            let mut writer = writer_provider.writer(rel_path).await.unwrap();
                            writer.write_all(content).await.unwrap();
                        }
                        Ok(size as _)
                    })
                }),
            )
            .await
            .unwrap();
        drop(guard);

        // recover stager
        drop(stager);
        let stager = BoundedStager::new(tempdir.path().to_path_buf(), u64::MAX, None, None)
            .await
            .unwrap();

        let reader = stager
            .get_blob(
                &puffin_file_name,
                blob_key,
                Box::new(|_| Box::pin(async { Ok(0) })),
            )
            .await
            .unwrap()
            .reader()
            .await
            .unwrap();

        let m = reader.metadata().await.unwrap();
        let buf = reader.read(0..m.content_length).await.unwrap();
        assert_eq!(&*buf, b"hello world");

        let (dir_path, metrics) = stager
            .get_dir(
                &puffin_file_name,
                dir_key,
                Box::new(|_| Box::pin(async { Ok(0) })),
            )
            .await
            .unwrap();

        assert!(metrics.cache_hit);
        assert!(metrics.dir_size > 0);
        for (rel_path, content) in &files_in_dir {
            let file_path = dir_path.path().join(rel_path);
            let mut file = tokio::fs::File::open(&file_path).await.unwrap();
            let mut buf = Vec::new();
            file.read_to_end(&mut buf).await.unwrap();
            assert_eq!(buf, *content);
        }

        let stats = notifier.stats();
        assert_eq!(
            stats,
            Stats {
                cache_insert_size: 81,
                cache_evict_size: 0,
                cache_hit_count: 0,
                cache_hit_size: 0,
                cache_miss_count: 2,
                cache_miss_size: 81,
                recycle_insert_size: 0,
                recycle_clear_size: 0
            }
        );
    }

    #[tokio::test]
    async fn test_eviction() {
        let tempdir = create_temp_dir("test_eviction_");
        let notifier = MockNotifier::build();
        let stager = BoundedStager::new(
            tempdir.path().to_path_buf(),
            1, /* extremely small size */
            Some(notifier.clone()),
            None,
        )
        .await
        .unwrap();

        let puffin_file_name = "test_eviction".to_string();
        let blob_key = "blob_key";

        // First time to get the blob
        let reader = stager
            .get_blob(
                &puffin_file_name,
                blob_key,
                Box::new(|mut writer| {
                    Box::pin(async move {
                        writer.write_all(b"Hello world").await.unwrap();
                        Ok(11)
                    })
                }),
            )
            .await
            .unwrap()
            .reader()
            .await
            .unwrap();

        // The blob should be evicted
        stager.cache.run_pending_tasks().await;
        assert!(!stager.in_cache(&puffin_file_name, blob_key));

        let stats = notifier.stats();
        assert_eq!(
            stats,
            Stats {
                cache_insert_size: 11,
                cache_evict_size: 11,
                cache_hit_count: 0,
                cache_hit_size: 0,
                cache_miss_count: 1,
                cache_miss_size: 11,
                recycle_insert_size: 11,
                recycle_clear_size: 0
            }
        );

        let m = reader.metadata().await.unwrap();
        let buf = reader.read(0..m.content_length).await.unwrap();
        assert_eq!(&*buf, b"Hello world");

        // Second time to get the blob, get from recycle bin
        let reader = stager
            .get_blob(
                &puffin_file_name,
                blob_key,
                Box::new(|_| async { Ok(0) }.boxed()),
            )
            .await
            .unwrap()
            .reader()
            .await
            .unwrap();

        // The blob should be evicted
        stager.cache.run_pending_tasks().await;
        assert!(!stager.in_cache(&puffin_file_name, blob_key));

        let stats = notifier.stats();
        assert_eq!(
            stats,
            Stats {
                cache_insert_size: 22,
                cache_evict_size: 22,
                cache_hit_count: 1,
                cache_hit_size: 11,
                cache_miss_count: 1,
                cache_miss_size: 11,
                recycle_insert_size: 22,
                recycle_clear_size: 11
            }
        );

        let m = reader.metadata().await.unwrap();
        let buf = reader.read(0..m.content_length).await.unwrap();
        assert_eq!(&*buf, b"Hello world");

        let dir_key = "dir_key";
        let files_in_dir = [
            ("file_a", "Hello, world!".as_bytes()),
            ("file_b", "Hello, Rust!".as_bytes()),
            ("file_c", "你好，世界！".as_bytes()),
            ("subdir/file_d", "Hello, Puffin!".as_bytes()),
            ("subdir/subsubdir/file_e", "¡Hola mundo!".as_bytes()),
        ];

        // First time to get the directory
        let (guard_0, _metrics) = stager
            .get_dir(
                &puffin_file_name,
                dir_key,
                Box::new(|writer_provider| {
                    Box::pin(async move {
                        let mut size = 0;
                        for (rel_path, content) in &files_in_dir {
                            let mut writer = writer_provider.writer(rel_path).await.unwrap();
                            writer.write_all(content).await.unwrap();
                            size += content.len() as u64;
                        }
                        Ok(size)
                    })
                }),
            )
            .await
            .unwrap();

        for (rel_path, content) in &files_in_dir {
            let file_path = guard_0.path().join(rel_path);
            let mut file = tokio::fs::File::open(&file_path).await.unwrap();
            let mut buf = Vec::new();
            file.read_to_end(&mut buf).await.unwrap();
            assert_eq!(buf, *content);
        }

        // The directory should be evicted
        stager.cache.run_pending_tasks().await;
        assert!(!stager.in_cache(&puffin_file_name, dir_key));

        let stats = notifier.stats();
        assert_eq!(
            stats,
            Stats {
                cache_insert_size: 92,
                cache_evict_size: 92,
                cache_hit_count: 1,
                cache_hit_size: 11,
                cache_miss_count: 2,
                cache_miss_size: 81,
                recycle_insert_size: 92,
                recycle_clear_size: 11
            }
        );

        // Second time to get the directory
        let (guard_1, _metrics) = stager
            .get_dir(
                &puffin_file_name,
                dir_key,
                Box::new(|_| async { Ok(0) }.boxed()),
            )
            .await
            .unwrap();

        for (rel_path, content) in &files_in_dir {
            let file_path = guard_1.path().join(rel_path);
            let mut file = tokio::fs::File::open(&file_path).await.unwrap();
            let mut buf = Vec::new();
            file.read_to_end(&mut buf).await.unwrap();
            assert_eq!(buf, *content);
        }

        // Still hold the guard
        stager.cache.run_pending_tasks().await;
        assert!(!stager.in_cache(&puffin_file_name, dir_key));

        let stats = notifier.stats();
        assert_eq!(
            stats,
            Stats {
                cache_insert_size: 162,
                cache_evict_size: 162,
                cache_hit_count: 2,
                cache_hit_size: 81,
                cache_miss_count: 2,
                cache_miss_size: 81,
                recycle_insert_size: 162,
                recycle_clear_size: 81
            }
        );

        // Third time to get the directory and all guards are dropped
        drop(guard_0);
        drop(guard_1);
        let (guard_2, _metrics) = stager
            .get_dir(
                &puffin_file_name,
                dir_key,
                Box::new(|_| Box::pin(async move { Ok(0) })),
            )
            .await
            .unwrap();

        // Still hold the guard, so the directory should not be removed even if it's evicted
        stager.cache.run_pending_tasks().await;
        assert!(!stager.in_cache(&puffin_file_name, blob_key));

        for (rel_path, content) in &files_in_dir {
            let file_path = guard_2.path().join(rel_path);
            let mut file = tokio::fs::File::open(&file_path).await.unwrap();
            let mut buf = Vec::new();
            file.read_to_end(&mut buf).await.unwrap();
            assert_eq!(buf, *content);
        }

        let stats = notifier.stats();
        assert_eq!(
            stats,
            Stats {
                cache_insert_size: 232,
                cache_evict_size: 232,
                cache_hit_count: 3,
                cache_hit_size: 151,
                cache_miss_count: 2,
                cache_miss_size: 81,
                recycle_insert_size: 232,
                recycle_clear_size: 151
            }
        );
    }

    #[tokio::test]
    async fn test_get_blob_concurrency_on_fail() {
        let tempdir = create_temp_dir("test_get_blob_concurrency_on_fail_");
        let stager = BoundedStager::new(tempdir.path().to_path_buf(), u64::MAX, None, None)
            .await
            .unwrap();

        let puffin_file_name = "test_get_blob_concurrency_on_fail".to_string();
        let key = "key";

        let stager = Arc::new(stager);
        let handles = (0..10)
            .map(|_| {
                let stager = stager.clone();
                let puffin_file_name = puffin_file_name.clone();
                let task = async move {
                    let failed_init = Box::new(|_| {
                        async {
                            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                            BlobNotFoundSnafu { blob: "whatever" }.fail()
                        }
                        .boxed()
                    });
                    stager.get_blob(&puffin_file_name, key, failed_init).await
                };

                tokio::spawn(task)
            })
            .collect::<Vec<_>>();

        for handle in handles {
            let r = handle.await.unwrap();
            assert!(r.is_err());
        }

        assert!(!stager.in_cache(&puffin_file_name, key));
    }

    #[tokio::test]
    async fn test_get_dir_concurrency_on_fail() {
        let tempdir = create_temp_dir("test_get_dir_concurrency_on_fail_");
        let stager = BoundedStager::new(tempdir.path().to_path_buf(), u64::MAX, None, None)
            .await
            .unwrap();

        let puffin_file_name = "test_get_dir_concurrency_on_fail".to_string();
        let key = "key";

        let stager = Arc::new(stager);
        let handles = (0..10)
            .map(|_| {
                let stager = stager.clone();
                let puffin_file_name = puffin_file_name.clone();
                let task = async move {
                    let failed_init = Box::new(|_| {
                        async {
                            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                            BlobNotFoundSnafu { blob: "whatever" }.fail()
                        }
                        .boxed()
                    });
                    stager.get_dir(&puffin_file_name, key, failed_init).await
                };

                tokio::spawn(task)
            })
            .collect::<Vec<_>>();

        for handle in handles {
            let r = handle.await.unwrap();
            assert!(r.is_err());
        }

        assert!(!stager.in_cache(&puffin_file_name, key));
    }

    #[tokio::test]
    async fn test_purge() {
        let tempdir = create_temp_dir("test_purge_");
        let notifier = MockNotifier::build();
        let stager = BoundedStager::new(
            tempdir.path().to_path_buf(),
            u64::MAX,
            Some(notifier.clone()),
            None,
        )
        .await
        .unwrap();

        // initialize stager
        let puffin_file_name = "test_purge".to_string();
        let blob_key = "blob_key";
        let guard = stager
            .get_blob(
                &puffin_file_name,
                blob_key,
                Box::new(|mut writer| {
                    Box::pin(async move {
                        writer.write_all(b"hello world").await.unwrap();
                        Ok(11)
                    })
                }),
            )
            .await
            .unwrap();
        drop(guard);

        let files_in_dir = [
            ("file_a", "Hello, world!".as_bytes()),
            ("file_b", "Hello, Rust!".as_bytes()),
            ("file_c", "你好，世界！".as_bytes()),
            ("subdir/file_d", "Hello, Puffin!".as_bytes()),
            ("subdir/subsubdir/file_e", "¡Hola mundo!".as_bytes()),
        ];

        let dir_key = "dir_key";
        let (guard, _metrics) = stager
            .get_dir(
                &puffin_file_name,
                dir_key,
                Box::new(|writer_provider| {
                    Box::pin(async move {
                        let mut size = 0;
                        for (rel_path, content) in &files_in_dir {
                            size += content.len();
                            let mut writer = writer_provider.writer(rel_path).await.unwrap();
                            writer.write_all(content).await.unwrap();
                        }
                        Ok(size as _)
                    })
                }),
            )
            .await
            .unwrap();
        drop(guard);

        // purge the stager
        stager.purge(&puffin_file_name).await.unwrap();

        let stats = notifier.stats();
        assert_eq!(
            stats,
            Stats {
                cache_insert_size: 81,
                cache_evict_size: 81,
                cache_hit_count: 0,
                cache_hit_size: 0,
                cache_miss_count: 2,
                cache_miss_size: 81,
                recycle_insert_size: 81,
                recycle_clear_size: 0
            }
        );
    }
}
