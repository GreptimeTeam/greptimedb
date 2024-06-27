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
use std::io::ErrorKind;
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use async_walkdir::{Filtering, WalkDir};
use base64::prelude::BASE64_URL_SAFE;
use base64::Engine;
use common_telemetry::{debug, warn};
use futures::lock::Mutex;
use futures::{FutureExt, StreamExt};
use moka::future::Cache;
use sha2::{Digest, Sha256};
use snafu::ResultExt;
use tokio::fs;
use tokio_util::compat::{Compat, TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};

use crate::error::{
    CacheGetSnafu, CreateSnafu, MetadataSnafu, OpenSnafu, ReadSnafu, RemoveSnafu, RenameSnafu,
    Result, RetriesExhaustedSnafu, WalkDirSnafu,
};
use crate::puffin_manager::cache_manager::{
    BoxWriter, CacheManager, DirWriterProvider, InitBlobFn, InitDirFn,
};
use crate::puffin_manager::DirGuard;

const MAX_RETRIES: usize = 3;

const TMP_EXTENSION: &str = "tmp";
const DELETED_EXTENSION: &str = "deleted";

/// `MokaCacheManager` is a `CacheManager` that uses `moka` to manage cache.
pub struct MokaCacheManager {
    /// The base directory of the cache.
    base_dir: PathBuf,

    /// The cache maintaining the cache key to the size of the file or directory.
    cache: Cache<String, CacheValue>,

    /// The unreleased directories that are waiting to be released. The value is the count of
    /// references to the directory.
    unreleased_dirs: Arc<Mutex<HashMap<String, usize>>>,
}

impl MokaCacheManager {
    #[allow(unused)]
    pub async fn new(base_dir: PathBuf, max_size: u64) -> Result<Self> {
        let cache_root_clone = base_dir.clone();

        let unreleased_dirs = Arc::new(Mutex::new(HashMap::new()));
        let unreleased_dirs_cloned = unreleased_dirs.clone();

        let cache = Cache::builder()
            .max_capacity(max_size)
            .weigher(|_: &String, v: &CacheValue| v.weight())
            .async_eviction_listener(move |key, v, _| {
                let unreleased_dirs = unreleased_dirs_cloned.clone();
                let cache_root_clone = cache_root_clone.clone();
                async move {
                    let path = cache_root_clone.join(key.as_str());
                    match v {
                        CacheValue::File(_) => {
                            debug!("Removing file due to eviction. Path: {path:?}");
                            if let Err(err) = fs::remove_file(&path).await {
                                warn!(err; "Failed to remove evicted file.")
                            }
                        }
                        CacheValue::Dir(_) => {
                            let deleted_path = path.with_extension(DELETED_EXTENSION);
                            {
                                let unreleased_dirs = unreleased_dirs.lock().await;
                                if unreleased_dirs.contains_key(key.as_ref()) {
                                    // Won't remove the directory if it's still in use.
                                    debug!("Skipping eviction of directory due to unreleased reference. Path: {path:?}");
                                    return;
                                }
                                if let Err(err) = fs::rename(&path, &deleted_path).await {
                                    warn!(err; "Failed to rename evicted file to deleted path.")
                                }
                            } // End of `unreleased_dirs`
                            debug!("Removing directory due to eviction. Path: {path:?}");
                            if let Err(err) = fs::remove_dir_all(&deleted_path).await {
                                warn!(err; "Failed to remove evicted directory.")
                            }
                        }
                    }
                }
                .boxed()
            })
            .build();

        let manager = Self {
            cache,
            base_dir,
            unreleased_dirs,
        };

        manager.recover().await?;

        Ok(manager)
    }
}

#[async_trait]
impl CacheManager for MokaCacheManager {
    type Reader = Compat<fs::File>;
    type Dir = RcDirGuard;

    async fn get_blob<'a>(
        &self,
        puffin_file_name: &str,
        key: &str,
        init_fn: Box<dyn InitBlobFn + Send + Sync + 'a>,
    ) -> Result<Self::Reader> {
        let cache_key = Self::encode_cache_key(puffin_file_name, key);
        let file_path = self.base_dir.join(&cache_key);

        for _ in 0..(MAX_RETRIES + 1) {
            self.cache
                .try_get_with(cache_key.clone(), async {
                    let size = Self::write_blob(&file_path, &init_fn).await?;
                    Ok(CacheValue::File(size))
                })
                .await
                .context(CacheGetSnafu)?;

            let file = fs::File::open(&file_path).await;
            match file {
                Ok(file) => return Ok(file.compat()),
                Err(err) if err.kind() == ErrorKind::NotFound => {
                    // A snap eviction happened. Retry. :(
                    continue;
                }
                Err(err) => return Err(err).context(OpenSnafu),
            }
        }

        RetriesExhaustedSnafu {
            retires: MAX_RETRIES,
        }
        .fail()
    }

    async fn get_dir<'a>(
        &self,
        puffin_file_name: &str,
        key: &str,
        init_fn: Box<dyn InitDirFn + Send + Sync + 'a>,
    ) -> Result<RcDirGuard> {
        let cache_key = Self::encode_cache_key(puffin_file_name, key);
        let dir_path = self.base_dir.join(&cache_key);

        let dir_exists = {
            let mut unreleased_dirs = self.unreleased_dirs.lock().await;

            // Prevent the directory from being removed by a snap eviction
            let rc = unreleased_dirs
                .entry(cache_key.clone())
                .and_modify(|count| *count += 1)
                .or_insert(1);

            *rc > 1
        }; // End of `unreleased_dirs`

        let res = self
            .cache
            .try_get_with(cache_key.clone(), async {
                let size = if dir_exists {
                    Self::get_dir_size(&dir_path).await?
                } else {
                    Self::write_dir(&dir_path, init_fn).await?
                };
                Ok(CacheValue::Dir(size))
            })
            .await
            .context(CacheGetSnafu);

        if res.is_err() {
            let mut unreleased_dirs = self.unreleased_dirs.lock().await;
            if let Some(count) = unreleased_dirs.get_mut(&cache_key) {
                if *count > 1 {
                    *count -= 1;
                } else {
                    unreleased_dirs.remove(&cache_key);
                }
            }
        } // End of `unreleased_dirs`

        res.map(move |_| RcDirGuard {
            path: dir_path,
            key: cache_key,
            unreleased_dirs: self.unreleased_dirs.clone(),
            cache: self.cache.clone(),
        })
    }

    async fn put_dir(
        &self,
        puffin_file_name: &str,
        key: &str,
        dir_path: PathBuf,
        size: u64,
    ) -> Result<()> {
        let cache_key = Self::encode_cache_key(puffin_file_name, key);
        let target_path = self.base_dir.join(&cache_key);

        self.cache
            .try_get_with(cache_key, async move {
                fs::rename(&dir_path, &target_path)
                    .await
                    .context(RenameSnafu)?;
                Ok(CacheValue::Dir(size))
            })
            .await
            .map(|_| ())
            .context(CacheGetSnafu)
    }
}

impl MokaCacheManager {
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
        init_fn: &(dyn InitBlobFn + Send + Sync + '_),
    ) -> Result<u64> {
        // To guarantee the atomicity of writing the file, we need to write
        // the directory to a temporary file first...
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
        let tmp_root = target_path.with_extension(TMP_EXTENSION);

        let writer_provider = Box::new(MokaDirWriterProvider(tmp_root.clone()));
        let size = init_fn(writer_provider).await?;

        // ...then rename the temporary directory to the target path
        fs::rename(&tmp_root, target_path)
            .await
            .context(RenameSnafu)?;
        Ok(size)
    }

    /// Recovers the cache by iterating through the cache directory.
    async fn recover(&self) -> Result<()> {
        let mut read_dir = fs::read_dir(&self.base_dir).await.context(ReadSnafu)?;
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
                // Insert the size of the file or directory to the cache
                let meta = entry.metadata().await.context(MetadataSnafu)?;
                let key = path.file_name().unwrap().to_string_lossy().into_owned();
                if meta.is_dir() {
                    let size = Self::get_dir_size(&path).await?;
                    self.cache.insert(key, CacheValue::Dir(size)).await;
                } else {
                    self.cache.insert(key, CacheValue::File(meta.len())).await;
                }
            }
        }
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
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum CacheValue {
    File(u64),
    Dir(u64),
}

impl CacheValue {
    fn size(&self) -> u64 {
        match self {
            CacheValue::File(size) => *size,
            CacheValue::Dir(size) => *size,
        }
    }

    fn weight(&self) -> u32 {
        self.size().try_into().unwrap_or(u32::MAX)
    }
}

/// `MokaDirWriterProvider` implements `DirWriterProvider` for initializing a directory.
struct MokaDirWriterProvider(PathBuf);

#[async_trait]
impl DirWriterProvider for MokaDirWriterProvider {
    async fn writer(&self, rel_path: &str) -> Result<BoxWriter> {
        let full_path = self.0.join(rel_path);
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

/// `RcDirGuard` is to manage the directory guard with reference counting.
pub struct RcDirGuard {
    path: PathBuf,
    key: String,
    unreleased_dirs: Arc<Mutex<HashMap<String, usize>>>,
    cache: Cache<String, CacheValue>,
}

impl DirGuard for RcDirGuard {
    fn path(&self) -> &PathBuf {
        &self.path
    }
}

impl Drop for RcDirGuard {
    fn drop(&mut self) {
        let unreleased_dirs = self.unreleased_dirs.clone();
        let key = self.key.clone();
        let path = self.path.clone();
        let cache = self.cache.clone();
        common_runtime::bg_runtime().spawn(async move {
            let deleted_path = path.with_extension(DELETED_EXTENSION);

            {
                let mut unreleased_dirs = unreleased_dirs.lock().await;
                if let Some(count) = unreleased_dirs.get_mut(&key) {
                    *count -= 1;
                    if *count > 0 {
                        // The directory is still in use.
                        return;
                    }

                    unreleased_dirs.remove(&key);
                    if cache.contains_key(&key) {
                        // The directory is still in the cache.
                        return;
                    }
                }

                debug!("Removing directory due to releasing. Path: {path:?}");
                if let Err(err) = fs::rename(&path, &deleted_path).await {
                    warn!(err; "Failed to rename evicted file to deleted path.")
                }
            } // End of `unreleased_dirs`

            if let Err(err) = fs::remove_dir_all(&deleted_path).await {
                warn!(err; "Failed to remove evicted directory.")
            }
        });
    }
}

#[cfg(test)]
impl MokaCacheManager {
    pub async fn must_get_file(&self, puffin_file_name: &str, key: &str) -> fs::File {
        let cache_key = Self::encode_cache_key(puffin_file_name, key);
        let file_path = self.base_dir.join(&cache_key);

        self.cache.get(&cache_key).await.unwrap();

        fs::File::open(&file_path).await.unwrap()
    }

    pub async fn must_get_dir(&self, puffin_file_name: &str, key: &str) -> PathBuf {
        let cache_key = Self::encode_cache_key(puffin_file_name, key);
        let dir_path = self.base_dir.join(&cache_key);

        self.cache.get(&cache_key).await.unwrap();

        dir_path
    }

    pub async fn dir_or_file_exists(&self, puffin_file_name: &str, key: &str) -> bool {
        let cache_key = Self::encode_cache_key(puffin_file_name, key);
        fs::try_exists(self.base_dir.join(&cache_key))
            .await
            .unwrap()
    }

    pub fn in_cache(&self, puffin_file_name: &str, key: &str) -> bool {
        let cache_key = Self::encode_cache_key(puffin_file_name, key);
        self.cache.contains_key(&cache_key)
    }
}

#[cfg(test)]
mod tests {
    use common_test_util::temp_dir::create_temp_dir;
    use futures::{AsyncReadExt, AsyncWriteExt};
    use tokio::io::AsyncReadExt as _;

    use super::*;
    use crate::error::BlobNotFoundSnafu;
    use crate::puffin_manager::cache_manager::CacheManager;

    #[tokio::test]
    async fn test_get_blob() {
        let tempdir = create_temp_dir("test_get_blob_");
        let manager = MokaCacheManager::new(tempdir.path().to_path_buf(), u64::MAX)
            .await
            .unwrap();

        let puffin_file_name = "test_get_blob";
        let key = "key";
        let mut reader = manager
            .get_blob(
                puffin_file_name,
                key,
                Box::new(|mut writer| {
                    Box::pin(async move {
                        writer.write_all(b"hello world").await.unwrap();
                        Ok(11)
                    })
                }),
            )
            .await
            .unwrap();

        let mut buf = Vec::new();
        reader.read_to_end(&mut buf).await.unwrap();
        assert_eq!(buf, b"hello world");

        let mut file = manager.must_get_file(puffin_file_name, key).await;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf).await.unwrap();
        assert_eq!(buf, b"hello world");
    }

    #[tokio::test]
    async fn test_get_dir() {
        let tempdir = create_temp_dir("test_get_dir_");
        let manager = MokaCacheManager::new(tempdir.path().to_path_buf(), u64::MAX)
            .await
            .unwrap();

        let files_in_dir = [
            ("file_a", "Hello, world!".as_bytes()),
            ("file_b", "Hello, Rust!".as_bytes()),
            ("file_c", "你好，世界！".as_bytes()),
            ("subdir/file_d", "Hello, Puffin!".as_bytes()),
            ("subdir/subsubdir/file_e", "¡Hola mundo!".as_bytes()),
        ];

        let puffin_file_name = "test_get_dir";
        let key = "key";
        let dir_path = manager
            .get_dir(
                puffin_file_name,
                key,
                Box::new(|writer_provider| {
                    Box::pin(async move {
                        for (rel_path, content) in &files_in_dir {
                            let mut writer = writer_provider.writer(rel_path).await.unwrap();
                            writer.write_all(content).await.unwrap();
                        }
                        Ok(0)
                    })
                }),
            )
            .await
            .unwrap();

        for (rel_path, content) in &files_in_dir {
            let file_path = dir_path.path().join(rel_path);
            let mut file = tokio::fs::File::open(&file_path).await.unwrap();
            let mut buf = Vec::new();
            file.read_to_end(&mut buf).await.unwrap();
            assert_eq!(buf, *content);
        }

        let dir_path = manager.must_get_dir(puffin_file_name, key).await;
        for (rel_path, content) in &files_in_dir {
            let file_path = dir_path.join(rel_path);
            let mut file = tokio::fs::File::open(&file_path).await.unwrap();
            let mut buf = Vec::new();
            file.read_to_end(&mut buf).await.unwrap();
            assert_eq!(buf, *content);
        }
    }

    #[tokio::test]
    async fn test_recover() {
        let tempdir = create_temp_dir("test_recover_");
        let manager = MokaCacheManager::new(tempdir.path().to_path_buf(), u64::MAX)
            .await
            .unwrap();

        // initialize cache
        let puffin_file_name = "test_recover";
        let blob_key = "blob_key";
        let _ = manager
            .get_blob(
                puffin_file_name,
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

        let files_in_dir = [
            ("file_a", "Hello, world!".as_bytes()),
            ("file_b", "Hello, Rust!".as_bytes()),
            ("file_c", "你好，世界！".as_bytes()),
            ("subdir/file_d", "Hello, Puffin!".as_bytes()),
            ("subdir/subsubdir/file_e", "¡Hola mundo!".as_bytes()),
        ];

        let dir_key = "dir_key";
        let _ = manager
            .get_dir(
                puffin_file_name,
                dir_key,
                Box::new(|writer_provider| {
                    Box::pin(async move {
                        for (rel_path, content) in &files_in_dir {
                            let mut writer = writer_provider.writer(rel_path).await.unwrap();
                            writer.write_all(content).await.unwrap();
                        }
                        Ok(0)
                    })
                }),
            )
            .await
            .unwrap();

        // recover cache
        drop(manager);
        let manager = MokaCacheManager::new(tempdir.path().to_path_buf(), u64::MAX)
            .await
            .unwrap();

        let mut reader = manager
            .get_blob(
                puffin_file_name,
                blob_key,
                Box::new(|_| Box::pin(async { Ok(0) })),
            )
            .await
            .unwrap();
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf).await.unwrap();
        assert_eq!(buf, b"hello world");

        let dir_path = manager
            .get_dir(
                puffin_file_name,
                dir_key,
                Box::new(|_| Box::pin(async { Ok(0) })),
            )
            .await
            .unwrap();
        for (rel_path, content) in &files_in_dir {
            let file_path = dir_path.path().join(rel_path);
            let mut file = tokio::fs::File::open(&file_path).await.unwrap();
            let mut buf = Vec::new();
            file.read_to_end(&mut buf).await.unwrap();
            assert_eq!(buf, *content);
        }
    }

    #[tokio::test]
    async fn test_eviction() {
        common_telemetry::init_default_ut_logging();

        let tempdir = create_temp_dir("test_eviction_");
        let manager = MokaCacheManager::new(
            tempdir.path().to_path_buf(),
            1, /* extremely small size */
        )
        .await
        .unwrap();

        let puffin_file_name = "test_eviction";
        let blob_key = "blob_key";

        // First time to get the blob
        let mut reader = manager
            .get_blob(
                puffin_file_name,
                blob_key,
                Box::new(|mut writer| {
                    Box::pin(async move {
                        writer.write_all(b"Hello world").await.unwrap();
                        Ok(11)
                    })
                }),
            )
            .await
            .unwrap();

        // The blob should be evicted
        manager.cache.run_pending_tasks().await;
        assert!(!manager.in_cache(puffin_file_name, blob_key));
        assert!(!manager.dir_or_file_exists(puffin_file_name, blob_key).await);

        let mut buf = Vec::new();
        reader.read_to_end(&mut buf).await.unwrap();
        assert_eq!(buf, b"Hello world");

        // Second time to get the blob
        let mut reader = manager
            .get_blob(
                puffin_file_name,
                blob_key,
                Box::new(|mut writer| {
                    Box::pin(async move {
                        writer.write_all(b"Hello world").await.unwrap();
                        Ok(11)
                    })
                }),
            )
            .await
            .unwrap();

        // The blob should be evicted
        manager.cache.run_pending_tasks().await;
        assert!(!manager.in_cache(puffin_file_name, blob_key));
        assert!(!manager.dir_or_file_exists(puffin_file_name, blob_key).await);

        let mut buf = Vec::new();
        reader.read_to_end(&mut buf).await.unwrap();
        assert_eq!(buf, b"Hello world");

        let dir_key = "dir_key";
        let files_in_dir = [
            ("file_a", "Hello, world!".as_bytes()),
            ("file_b", "Hello, Rust!".as_bytes()),
            ("file_c", "你好，世界！".as_bytes()),
            ("subdir/file_d", "Hello, Puffin!".as_bytes()),
            ("subdir/subsubdir/file_e", "¡Hola mundo!".as_bytes()),
        ];

        // First time to get the directory
        let dir_path = manager
            .get_dir(
                puffin_file_name,
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
            let file_path = dir_path.path().join(rel_path);
            let mut file = tokio::fs::File::open(&file_path).await.unwrap();
            let mut buf = Vec::new();
            file.read_to_end(&mut buf).await.unwrap();
            assert_eq!(buf, *content);
        }

        // Drop the guard to release the directory
        drop(dir_path);
        // The directory should be evicted, and the directory should be removed
        manager.cache.run_pending_tasks().await;
        assert!(!manager.in_cache(puffin_file_name, dir_key));
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
        assert!(!manager.dir_or_file_exists(puffin_file_name, dir_key).await);

        // Second time to get the directory
        let dir_path = manager
            .get_dir(
                puffin_file_name,
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
            let file_path = dir_path.path().join(rel_path);
            let mut file = tokio::fs::File::open(&file_path).await.unwrap();
            let mut buf = Vec::new();
            file.read_to_end(&mut buf).await.unwrap();
            assert_eq!(buf, *content);
        }

        // Still hold the guard, so the directory should not be removed even if it's evicted
        manager.cache.run_pending_tasks().await;
        assert!(!manager.in_cache(puffin_file_name, dir_key));
        assert!(manager.dir_or_file_exists(puffin_file_name, dir_key).await);

        // Third time to get the directory. The returned directory should be the same as
        // the previous one because the manager will get it from `unreleased_dirs`.
        let dir_path_from_trush = manager
            .get_dir(
                puffin_file_name,
                dir_key,
                Box::new(|_| Box::pin(async move { Ok(0) })),
            )
            .await
            .unwrap();

        // Still hold the guard, so the directory should not be removed even if it's evicted
        manager.cache.run_pending_tasks().await;
        assert!(!manager.in_cache(puffin_file_name, blob_key));
        assert!(manager.dir_or_file_exists(puffin_file_name, dir_key).await);

        for (rel_path, content) in &files_in_dir {
            let file_path = dir_path_from_trush.path().join(rel_path);
            let mut file = tokio::fs::File::open(&file_path).await.unwrap();
            let mut buf = Vec::new();
            file.read_to_end(&mut buf).await.unwrap();
            assert_eq!(buf, *content);
        }

        drop(dir_path_from_trush);
        assert!(manager.dir_or_file_exists(puffin_file_name, dir_key).await);

        // Drop the directory
        drop(dir_path);
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
        assert!(!manager.dir_or_file_exists(puffin_file_name, dir_key).await);
    }

    #[tokio::test]
    async fn test_get_blob_concurrency_on_fail() {
        let tempdir = create_temp_dir("test_get_blob_concurrency_on_fail_");
        let manager = MokaCacheManager::new(tempdir.path().to_path_buf(), u64::MAX)
            .await
            .unwrap();

        let puffin_file_name = "test_get_blob_concurrency_on_fail";
        let key = "key";

        let manager = Arc::new(manager);
        let handles = (0..10)
            .map(|_| {
                let manager = manager.clone();
                let task = async move {
                    let failed_init = Box::new(|_| {
                        async {
                            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                            BlobNotFoundSnafu { blob: "whatever" }.fail()
                        }
                        .boxed()
                    });
                    manager.get_blob(puffin_file_name, key, failed_init).await
                };

                tokio::spawn(task)
            })
            .collect::<Vec<_>>();

        for handle in handles {
            let r = handle.await.unwrap();
            assert!(r.is_err());
        }

        assert!(!manager.in_cache(puffin_file_name, key));
        assert!(!manager.dir_or_file_exists(puffin_file_name, key).await);
    }

    #[tokio::test]
    async fn test_get_dir_concurrency_on_fail() {
        let tempdir = create_temp_dir("test_get_dir_concurrency_on_fail_");
        let manager = MokaCacheManager::new(tempdir.path().to_path_buf(), u64::MAX)
            .await
            .unwrap();

        let puffin_file_name = "test_get_dir_concurrency_on_fail";
        let key = "key";

        let manager = Arc::new(manager);
        let handles = (0..10)
            .map(|_| {
                let manager = manager.clone();
                let task = async move {
                    let failed_init = Box::new(|_| {
                        async {
                            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                            BlobNotFoundSnafu { blob: "whatever" }.fail()
                        }
                        .boxed()
                    });
                    manager.get_dir(puffin_file_name, key, failed_init).await
                };

                tokio::spawn(task)
            })
            .collect::<Vec<_>>();

        for handle in handles {
            let r = handle.await.unwrap();
            assert!(r.is_err());
        }

        assert!(!manager.in_cache(puffin_file_name, key));
        assert!(!manager.dir_or_file_exists(puffin_file_name, key).await);
    }
}
