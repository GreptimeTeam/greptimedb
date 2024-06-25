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

use std::path::PathBuf;

use async_trait::async_trait;
use async_walkdir::{Filtering, WalkDir};
use base64::prelude::BASE64_URL_SAFE;
use base64::Engine;
use common_telemetry::warn;
use futures::{FutureExt, StreamExt};
use moka::future::Cache;
use sha2::{Digest, Sha256};
use snafu::ResultExt;
use tokio::fs;
use tokio_util::compat::{Compat, TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};

use crate::error::{
    CreateSnafu, MetadataSnafu, OpenSnafu, ReadSnafu, RemoveSnafu, RenameSnafu, Result,
    WalkDirSnafu,
};
use crate::puffin_manager::cache_manager::{
    BoxWriter, CacheManager, DirWriterProvider, InitBlobFn, InitDirFn,
};

const TMP_EXTENSION: &str = "tmp";
const DELETED_EXTENSION: &str = "deleted";

/// `MokaCacheManager` is a `CacheManager` that uses `moka` to manage cache.
pub struct MokaCacheManager {
    /// The root directory of the cache.
    root: PathBuf,

    /// The cache maintaining the cache key to the size of the file or directory.
    cache: Cache<String, u64>,
}

impl MokaCacheManager {
    #[allow(unused)]
    pub async fn new(root: PathBuf, max_size: u64) -> Result<Self> {
        let cache_root_clone = root.clone();

        let cache = Cache::builder()
            .max_capacity(max_size)
            .weigher(|_: &String, size: &u64| *size as u32)
            .async_eviction_listener(move |key, _, _| {
                let cloned_root = cache_root_clone.clone();
                async move {
                    // To guarantee the atomicity of the eviction, we need to rename the file or
                    // directory to a deleted path first...
                    let path = cloned_root.join(key.as_str());
                    let deleted_path = path.with_extension(DELETED_EXTENSION);
                    if let Err(err) = fs::rename(&path, &deleted_path).await {
                        warn!(err; "Failed to rename evicted file to deleted path.")
                    }

                    // ...then remove the file or directory
                    match fs::metadata(&deleted_path).await {
                        Ok(metadata) => {
                            if metadata.is_dir() {
                                if let Err(err) = fs::remove_dir_all(&deleted_path).await {
                                    warn!(err; "Failed to remove evicted directory.")
                                }
                            } else if let Err(err) = fs::remove_file(&deleted_path).await {
                                warn!(err; "Failed to remove evicted file.")
                            }
                        }
                        Err(err) => warn!(err; "Failed to get metadata of evicted file."),
                    }
                }
                .boxed()
            })
            .build();

        let manager = Self { cache, root };

        manager.recover().await?;

        Ok(manager)
    }
}

#[async_trait]
impl CacheManager for MokaCacheManager {
    type Reader = Compat<fs::File>;

    async fn get_blob<'a>(
        &self,
        puffin_file_name: &str,
        key: &str,
        init_fn: Box<dyn InitBlobFn + Send + 'a>,
    ) -> Result<Self::Reader> {
        let cache_key = Self::encode_cache_key(puffin_file_name, key);
        let file_path = self.root.join(&cache_key);

        if self.cache.get(&cache_key).await.is_some() {
            Ok(fs::File::open(file_path).await.context(OpenSnafu)?.compat())
        } else {
            let size = self.write_blob(&file_path, init_fn).await?;
            self.cache.insert(cache_key, size).await;
            Ok(fs::File::open(file_path).await.context(OpenSnafu)?.compat())
        }
    }

    async fn get_dir<'a>(
        &self,
        puffin_file_name: &str,
        key: &str,
        init_fn: Box<dyn InitDirFn + Send + 'a>,
    ) -> Result<PathBuf> {
        let cache_key = Self::encode_cache_key(puffin_file_name, key);
        let dir_path = self.root.join(&cache_key);

        if self.cache.get(&cache_key).await.is_some() {
            Ok(dir_path)
        } else {
            let size = self.write_dir(&dir_path, init_fn).await?;
            self.cache.insert(cache_key, size).await;
            Ok(dir_path)
        }
    }

    async fn put_dir(
        &self,
        puffin_file_name: &str,
        key: &str,
        dir_path: PathBuf,
        size: u64,
    ) -> Result<()> {
        let cache_key = Self::encode_cache_key(puffin_file_name, key);
        let target_path = self.root.join(&cache_key);

        fs::rename(&dir_path, &target_path)
            .await
            .context(RenameSnafu)?;
        self.cache.insert(cache_key, size).await;

        Ok(())
    }
}

impl MokaCacheManager {
    fn encode_cache_key(puffin_file_name: &str, key: &str) -> String {
        let key1 = Sha256::digest(puffin_file_name);
        let key2 = Sha256::digest(key);

        let mut hasher = Sha256::new();
        hasher.update(key1);
        hasher.update(key2);
        let hash = hasher.finalize();

        BASE64_URL_SAFE.encode(hash)
    }

    async fn write_blob(
        &self,
        target_path: &PathBuf,
        init_fn: Box<dyn InitBlobFn + Send + '_>,
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
        &self,
        target_path: &PathBuf,
        init_fn: Box<dyn InitDirFn + Send + '_>,
    ) -> Result<u64> {
        // To guarantee the atomicity of writing the directory, we need to write
        // the directory to a temporary directory first...
        let tmp_root = target_path.with_extension(TMP_EXTENSION);

        let writer_provider = Box::new(MokaDirWriterProvider {
            root: tmp_root.clone(),
        });
        let size = init_fn(writer_provider).await?;

        // ...then rename the temporary directory to the target path
        fs::rename(&tmp_root, target_path)
            .await
            .context(RenameSnafu)?;
        Ok(size)
    }

    /// Recover the cache by iterating through the cache directory.
    async fn recover(&self) -> Result<()> {
        let mut read_dir = fs::read_dir(&self.root).await.context(ReadSnafu)?;
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
                    self.cache.insert(key, size).await;
                } else {
                    self.cache.insert(key, meta.len()).await;
                }
            }
        }
        Ok(())
    }

    /// Walk through the directory and calculate the total size of all files in the directory.
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

pub struct MokaDirWriterProvider {
    root: PathBuf,
}

#[async_trait]
impl DirWriterProvider for MokaDirWriterProvider {
    async fn writer(&self, rel_path: &str) -> Result<BoxWriter> {
        let full_path = self.root.join(rel_path);
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
impl MokaCacheManager {
    pub async fn must_get_file(&self, puffin_file_name: &str, key: &str) -> fs::File {
        let cache_key = Self::encode_cache_key(puffin_file_name, key);
        let file_path = self.root.join(&cache_key);

        self.cache.get(&cache_key).await.unwrap();

        fs::File::open(&file_path).await.unwrap()
    }

    pub async fn must_get_dir(&self, puffin_file_name: &str, key: &str) -> PathBuf {
        let cache_key = Self::encode_cache_key(puffin_file_name, key);
        let dir_path = self.root.join(&cache_key);

        self.cache.get(&cache_key).await.unwrap();

        dir_path
    }
}

#[cfg(test)]
mod tests {
    use common_test_util::temp_dir::create_temp_dir;
    use futures::{AsyncReadExt, AsyncWriteExt};
    use tokio::io::AsyncReadExt as _;

    use super::*;
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
                            let mut writer = writer_provider.writer(*rel_path).await.unwrap();
                            writer.write_all(content).await.unwrap();
                        }
                        Ok(0)
                    })
                }),
            )
            .await
            .unwrap();

        for (rel_path, content) in &files_in_dir {
            let file_path = dir_path.join(rel_path);
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
                            let mut writer = writer_provider.writer(*rel_path).await.unwrap();
                            writer.write_all(content).await.unwrap();
                        }
                        Ok(0)
                    })
                }),
            )
            .await
            .unwrap();

        // reover cache
        drop(manager);
        let manager = MokaCacheManager::new(tempdir.path().to_path_buf(), u64::MAX)
            .await
            .unwrap();

        let mut reader = manager
            .get_blob(
                puffin_file_name,
                "blob_key",
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
            let file_path = dir_path.join(rel_path);
            let mut file = tokio::fs::File::open(&file_path).await.unwrap();
            let mut buf = Vec::new();
            file.read_to_end(&mut buf).await.unwrap();
            assert_eq!(buf, *content);
        }
    }
}
