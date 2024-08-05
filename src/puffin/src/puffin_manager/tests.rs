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
use std::sync::Arc;

use async_trait::async_trait;
use common_test_util::temp_dir::{create_temp_dir, TempDir};
use futures::AsyncReadExt as _;
use tokio::fs::File;
use tokio::io::AsyncReadExt as _;
use tokio_util::compat::{Compat, TokioAsyncReadCompatExt};

use crate::blob_metadata::CompressionCodec;
use crate::error::Result;
use crate::puffin_manager::file_accessor::PuffinFileAccessor;
use crate::puffin_manager::fs_puffin_manager::FsPuffinManager;
use crate::puffin_manager::stager::BoundedStager;
use crate::puffin_manager::{
    BlobGuard, DirGuard, PuffinManager, PuffinReader, PuffinWriter, PutOptions,
};

async fn new_bounded_stager(prefix: &str, capacity: u64) -> (TempDir, Arc<BoundedStager>) {
    let staging_dir = create_temp_dir(prefix);
    let path = staging_dir.path().to_path_buf();
    (
        staging_dir,
        Arc::new(BoundedStager::new(path, capacity).await.unwrap()),
    )
}

#[tokio::test]
async fn test_put_get_file() {
    let capicities = [1, 16, u64::MAX];
    let compression_codecs = [None, Some(CompressionCodec::Zstd)];

    for capacity in capicities {
        for compression_codec in compression_codecs {
            let (_staging_dir, stager) = new_bounded_stager("test_put_get_file_", capacity).await;
            let file_accessor = Arc::new(MockFileAccessor::new("test_put_get_file_"));

            let puffin_manager = FsPuffinManager::new(stager.clone(), file_accessor.clone());

            let puffin_file_name = "puffin_file";
            let mut writer = puffin_manager.writer(puffin_file_name).await.unwrap();

            let key = "blob_a";
            let raw_data = "Hello, world!".as_bytes();
            put_blob(key, raw_data, compression_codec, &mut writer).await;

            writer.finish().await.unwrap();

            let reader = puffin_manager.reader(puffin_file_name).await.unwrap();
            check_blob(
                puffin_file_name,
                key,
                raw_data,
                &stager,
                &reader,
                compression_codec.is_some(),
            )
            .await;

            // renew cache manager
            let (_staging_dir, stager) = new_bounded_stager("test_put_get_file_", capacity).await;
            let puffin_manager = FsPuffinManager::new(stager.clone(), file_accessor);

            let reader = puffin_manager.reader(puffin_file_name).await.unwrap();
            check_blob(
                puffin_file_name,
                key,
                raw_data,
                &stager,
                &reader,
                compression_codec.is_some(),
            )
            .await;
        }
    }
}

#[tokio::test]
async fn test_put_get_files() {
    let capicities = [1, 16, u64::MAX];

    for capacity in capicities {
        let compression_codecs = [None, Some(CompressionCodec::Zstd)];

        for compression_codec in compression_codecs {
            let (_staging_dir, stager) = new_bounded_stager("test_put_get_files_", capacity).await;
            let file_accessor = Arc::new(MockFileAccessor::new("test_put_get_files_"));

            let puffin_manager = FsPuffinManager::new(stager.clone(), file_accessor.clone());

            let puffin_file_name = "puffin_file";
            let mut writer = puffin_manager.writer(puffin_file_name).await.unwrap();

            let blobs = [
                ("blob_a", "Hello, world!".as_bytes()),
                ("blob_b", "Hello, Rust!".as_bytes()),
                ("blob_c", "你好，世界！".as_bytes()),
            ]
            .into_iter()
            .collect::<HashMap<_, _>>();

            for (key, raw_data) in &blobs {
                put_blob(key, raw_data, compression_codec, &mut writer).await;
            }

            writer.finish().await.unwrap();

            let reader = puffin_manager.reader(puffin_file_name).await.unwrap();
            for (key, raw_data) in &blobs {
                check_blob(
                    puffin_file_name,
                    key,
                    raw_data,
                    &stager,
                    &reader,
                    compression_codec.is_some(),
                )
                .await;
            }

            // renew cache manager
            let (_staging_dir, stager) = new_bounded_stager("test_put_get_files_", capacity).await;
            let puffin_manager = FsPuffinManager::new(stager.clone(), file_accessor);
            let reader = puffin_manager.reader(puffin_file_name).await.unwrap();
            for (key, raw_data) in &blobs {
                check_blob(
                    puffin_file_name,
                    key,
                    raw_data,
                    &stager,
                    &reader,
                    compression_codec.is_some(),
                )
                .await;
            }
        }
    }
}

#[tokio::test]
async fn test_put_get_dir() {
    let capicities = [1, 64, u64::MAX];

    let compression_codecs = [None, Some(CompressionCodec::Zstd)];

    for capacity in capicities {
        for compression_codec in compression_codecs {
            let (_staging_dir, stager) = new_bounded_stager("test_put_get_dir_", capacity).await;
            let file_accessor = Arc::new(MockFileAccessor::new("test_put_get_dir_"));

            let puffin_manager = FsPuffinManager::new(stager.clone(), file_accessor.clone());

            let puffin_file_name = "puffin_file";
            let mut writer = puffin_manager.writer(puffin_file_name).await.unwrap();

            let key = "dir_a";

            let files_in_dir = vec![
                ("file_a", "Hello, world!".as_bytes()),
                ("file_b", "Hello, Rust!".as_bytes()),
                ("file_c", "你好，世界！".as_bytes()),
                ("subdir/file_d", "Hello, Puffin!".as_bytes()),
                ("subdir/subsubdir/file_e", "¡Hola mundo!".as_bytes()),
            ];

            put_dir(key, &files_in_dir, compression_codec, &mut writer).await;

            writer.finish().await.unwrap();

            let reader = puffin_manager.reader(puffin_file_name).await.unwrap();
            check_dir(puffin_file_name, key, &files_in_dir, &stager, &reader).await;

            // renew cache manager
            let (_staging_dir, stager) = new_bounded_stager("test_put_get_dir_", capacity).await;
            let puffin_manager = FsPuffinManager::new(stager.clone(), file_accessor);

            let reader = puffin_manager.reader(puffin_file_name).await.unwrap();
            check_dir(puffin_file_name, key, &files_in_dir, &stager, &reader).await;
        }
    }
}

#[tokio::test]
async fn test_put_get_mix_file_dir() {
    let capicities = [1, 64, u64::MAX];
    let compression_codecs = [None, Some(CompressionCodec::Zstd)];

    for capacity in capicities {
        for compression_codec in compression_codecs {
            let (_staging_dir, stager) =
                new_bounded_stager("test_put_get_mix_file_dir_", capacity).await;
            let file_accessor = Arc::new(MockFileAccessor::new("test_put_get_mix_file_dir_"));

            let puffin_manager = FsPuffinManager::new(stager.clone(), file_accessor.clone());

            let puffin_file_name = "puffin_file";
            let mut writer = puffin_manager.writer(puffin_file_name).await.unwrap();

            let blobs = [
                ("blob_a", "Hello, world!".as_bytes()),
                ("blob_b", "Hello, Rust!".as_bytes()),
                ("blob_c", "你好，世界！".as_bytes()),
            ]
            .into_iter()
            .collect::<HashMap<_, _>>();

            let dir_key = "dir_a";
            let files_in_dir = [
                ("file_a", "Hello, world!".as_bytes()),
                ("file_b", "Hello, Rust!".as_bytes()),
                ("file_c", "你好，世界！".as_bytes()),
                ("subdir/file_d", "Hello, Puffin!".as_bytes()),
                ("subdir/subsubdir/file_e", "¡Hola mundo!".as_bytes()),
            ];

            for (key, raw_data) in &blobs {
                put_blob(key, raw_data, compression_codec, &mut writer).await;
            }
            put_dir(dir_key, &files_in_dir, compression_codec, &mut writer).await;

            writer.finish().await.unwrap();

            let reader = puffin_manager.reader(puffin_file_name).await.unwrap();
            for (key, raw_data) in &blobs {
                check_blob(
                    puffin_file_name,
                    key,
                    raw_data,
                    &stager,
                    &reader,
                    compression_codec.is_some(),
                )
                .await;
            }
            check_dir(puffin_file_name, dir_key, &files_in_dir, &stager, &reader).await;

            // renew cache manager
            let (_staging_dir, stager) =
                new_bounded_stager("test_put_get_mix_file_dir_", capacity).await;
            let puffin_manager = FsPuffinManager::new(stager.clone(), file_accessor);

            let reader = puffin_manager.reader(puffin_file_name).await.unwrap();
            for (key, raw_data) in &blobs {
                check_blob(
                    puffin_file_name,
                    key,
                    raw_data,
                    &stager,
                    &reader,
                    compression_codec.is_some(),
                )
                .await;
            }
            check_dir(puffin_file_name, dir_key, &files_in_dir, &stager, &reader).await;
        }
    }
}

async fn put_blob(
    key: &str,
    raw_data: &[u8],
    compression_codec: Option<CompressionCodec>,
    puffin_writer: &mut impl PuffinWriter,
) {
    puffin_writer
        .put_blob(
            key,
            raw_data,
            PutOptions {
                compression: compression_codec,
            },
        )
        .await
        .unwrap();
}

async fn check_blob(
    puffin_file_name: &str,
    key: &str,
    raw_data: &[u8],
    stager: &BoundedStager,
    puffin_reader: &impl PuffinReader,
    compressed: bool,
) {
    let blob = puffin_reader.blob(key).await.unwrap();
    let mut reader = blob.reader().await.unwrap();
    let mut buf = Vec::new();
    reader.read_to_end(&mut buf).await.unwrap();
    assert_eq!(buf, raw_data);

    if !compressed {
        // If the blob is not compressed, it won't be exist in the stager.
        return;
    }

    let mut staged_file = stager.must_get_file(puffin_file_name, key).await;
    let mut buf = Vec::new();
    staged_file.read_to_end(&mut buf).await.unwrap();
    assert_eq!(buf, raw_data);
}

async fn put_dir(
    key: &str,
    files_in_dir: &[(&str, &[u8])],
    compression_codec: Option<CompressionCodec>,
    puffin_writer: &mut impl PuffinWriter,
) {
    let dir = create_temp_dir("dir_in_puffin_");
    for (file_key, raw_data) in files_in_dir.iter() {
        let file_path = if cfg!(windows) {
            dir.path().join(file_key.replace('/', "\\"))
        } else {
            dir.path().join(file_key)
        };
        std::fs::create_dir_all(file_path.parent().unwrap()).unwrap();
        std::fs::write(&file_path, raw_data).unwrap();
    }

    puffin_writer
        .put_dir(
            key,
            dir.path().to_path_buf(),
            PutOptions {
                compression: compression_codec,
            },
        )
        .await
        .unwrap();
}

async fn check_dir(
    puffin_file_name: &str,
    key: &str,
    files_in_dir: &[(&str, &[u8])],
    stager: &BoundedStager,
    puffin_reader: &impl PuffinReader,
) {
    let res_dir = puffin_reader.dir(key).await.unwrap();
    for (file_name, raw_data) in files_in_dir {
        let file_path = if cfg!(windows) {
            res_dir.path().join(file_name.replace('/', "\\"))
        } else {
            res_dir.path().join(file_name)
        };
        let buf = std::fs::read(file_path).unwrap();
        assert_eq!(buf, *raw_data);
    }

    let staged_dir = stager.must_get_dir(puffin_file_name, key).await;
    for (file_name, raw_data) in files_in_dir {
        let file_path = if cfg!(windows) {
            staged_dir.as_path().join(file_name.replace('/', "\\"))
        } else {
            staged_dir.as_path().join(file_name)
        };
        let buf = std::fs::read(file_path).unwrap();
        assert_eq!(buf, *raw_data);
    }
}

pub struct MockFileAccessor {
    tempdir: TempDir,
}

impl MockFileAccessor {
    pub fn new(prefix: &str) -> Self {
        let tempdir = create_temp_dir(prefix);
        Self { tempdir }
    }
}

#[async_trait]
impl PuffinFileAccessor for MockFileAccessor {
    type Reader = Compat<File>;
    type Writer = Compat<File>;

    async fn reader(&self, puffin_file_name: &str) -> Result<Self::Reader> {
        let f = tokio::fs::File::open(self.tempdir.path().join(puffin_file_name))
            .await
            .unwrap();
        Ok(f.compat())
    }

    async fn writer(&self, puffin_file_name: &str) -> Result<Self::Writer> {
        let p = self.tempdir.path().join(puffin_file_name);
        if let Some(p) = p.parent() {
            if !tokio::fs::try_exists(p).await.unwrap() {
                tokio::fs::create_dir_all(p).await.unwrap();
            }
        }
        let f = tokio::fs::File::create(p).await.unwrap();
        Ok(f.compat())
    }
}
