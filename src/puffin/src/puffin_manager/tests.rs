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

use common_base::range_read::RangeReader;
use common_test_util::temp_dir::{TempDir, create_temp_dir};
use tokio::io::AsyncReadExt as _;

use crate::blob_metadata::CompressionCodec;
use crate::puffin_manager::file_accessor::MockFileAccessor;
use crate::puffin_manager::fs_puffin_manager::FsPuffinManager;
use crate::puffin_manager::stager::BoundedStager;
use crate::puffin_manager::{PuffinManager, PuffinReader, PuffinWriter, PutOptions};

async fn new_bounded_stager(prefix: &str, capacity: u64) -> (TempDir, Arc<BoundedStager<String>>) {
    let staging_dir = create_temp_dir(prefix);
    let path = staging_dir.path().to_path_buf();
    (
        staging_dir,
        Arc::new(
            BoundedStager::new(path, capacity, None, None)
                .await
                .unwrap(),
        ),
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

            let puffin_file_name = "puffin_file".to_string();
            let mut writer = puffin_manager.writer(&puffin_file_name).await.unwrap();

            let key = "blob_a";
            let raw_data = "Hello, world!".as_bytes();
            put_blob(key, raw_data, compression_codec, &mut writer).await;

            writer.finish().await.unwrap();

            let reader = puffin_manager.reader(&puffin_file_name).await.unwrap();
            check_blob(
                &puffin_file_name,
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

            let reader = puffin_manager.reader(&puffin_file_name).await.unwrap();
            check_blob(
                &puffin_file_name,
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
    let compression_codecs = [None, Some(CompressionCodec::Zstd)];

    for capacity in capicities {
        for compression_codec in compression_codecs {
            let (_staging_dir, stager) = new_bounded_stager("test_put_get_files_", capacity).await;
            let file_accessor = Arc::new(MockFileAccessor::new("test_put_get_files_"));

            let puffin_manager = FsPuffinManager::new(stager.clone(), file_accessor.clone());

            let puffin_file_name = "puffin_file".to_string();
            let mut writer = puffin_manager.writer(&puffin_file_name).await.unwrap();

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

            let reader = puffin_manager.reader(&puffin_file_name).await.unwrap();
            for (key, raw_data) in &blobs {
                check_blob(
                    &puffin_file_name,
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
            let reader = puffin_manager.reader(&puffin_file_name).await.unwrap();
            for (key, raw_data) in &blobs {
                check_blob(
                    &puffin_file_name,
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

            let puffin_file_name = "puffin_file".to_string();
            let mut writer = puffin_manager.writer(&puffin_file_name).await.unwrap();

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

            let reader = puffin_manager.reader(&puffin_file_name).await.unwrap();
            check_dir(&puffin_file_name, key, &files_in_dir, &stager, &reader).await;

            // renew cache manager
            let (_staging_dir, stager) = new_bounded_stager("test_put_get_dir_", capacity).await;
            let puffin_manager = FsPuffinManager::new(stager.clone(), file_accessor);

            let reader = puffin_manager.reader(&puffin_file_name).await.unwrap();
            check_dir(&puffin_file_name, key, &files_in_dir, &stager, &reader).await;
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

            let puffin_file_name = "puffin_file".to_string();
            let mut writer = puffin_manager.writer(&puffin_file_name).await.unwrap();

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

            let reader = puffin_manager.reader(&puffin_file_name).await.unwrap();
            for (key, raw_data) in &blobs {
                check_blob(
                    &puffin_file_name,
                    key,
                    raw_data,
                    &stager,
                    &reader,
                    compression_codec.is_some(),
                )
                .await;
            }
            check_dir(&puffin_file_name, dir_key, &files_in_dir, &stager, &reader).await;

            // renew cache manager
            let (_staging_dir, stager) =
                new_bounded_stager("test_put_get_mix_file_dir_", capacity).await;
            let puffin_manager = FsPuffinManager::new(stager.clone(), file_accessor);

            let reader = puffin_manager.reader(&puffin_file_name).await.unwrap();
            for (key, raw_data) in &blobs {
                check_blob(
                    &puffin_file_name,
                    key,
                    raw_data,
                    &stager,
                    &reader,
                    compression_codec.is_some(),
                )
                .await;
            }
            check_dir(&puffin_file_name, dir_key, &files_in_dir, &stager, &reader).await;
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
            HashMap::from_iter([("test_key".to_string(), "test_value".to_string())]),
        )
        .await
        .unwrap();
}

async fn check_blob(
    puffin_file_name: &str,
    key: &str,
    raw_data: &[u8],
    stager: &BoundedStager<String>,
    puffin_reader: &impl PuffinReader,
    compressed: bool,
) {
    let blob = puffin_reader.blob(key).await.unwrap();

    let blob_metadata = blob.metadata();
    assert_eq!(
        blob_metadata.properties,
        HashMap::from_iter([("test_key".to_string(), "test_value".to_string())])
    );

    let reader = blob.reader().await.unwrap();
    let meta = reader.metadata().await.unwrap();
    let bs = reader.read(0..meta.content_length).await.unwrap();
    assert_eq!(&*bs, raw_data);

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
            HashMap::from_iter([("test_key".to_string(), "test_value".to_string())]),
        )
        .await
        .unwrap();
}

async fn check_dir(
    puffin_file_name: &str,
    key: &str,
    files_in_dir: &[(&str, &[u8])],
    stager: &BoundedStager<String>,
    puffin_reader: &impl PuffinReader,
) {
    let (res_dir, _metrics) = puffin_reader.dir(key).await.unwrap();
    let metadata = res_dir.metadata();
    assert_eq!(
        metadata.properties,
        HashMap::from_iter([("test_key".to_string(), "test_value".to_string())])
    );
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

#[tokio::test]
async fn test_metadata_cache_shared_by_independent_readers() {
    use common_cache::metrics::{CacheMetrics, OperationMetrics};
    use prometheus::{Histogram, HistogramOpts, IntCounter, IntGauge, IntGaugeVec, Opts};

    use crate::puffin_manager::cache::PuffinMetadataCache;

    let operation = |name: &str| OperationMetrics {
        started: IntCounter::new(format!("{name}_started"), "test").unwrap(),
        succeeded: IntCounter::new(format!("{name}_succeeded"), "test").unwrap(),
        failed: IntCounter::new(format!("{name}_failed"), "test").unwrap(),
        cancelled: IntCounter::new(format!("{name}_cancelled"), "test").unwrap(),
        active: IntGauge::new(format!("{name}_active"), "test").unwrap(),
        elapsed: Histogram::with_opts(HistogramOpts::new(format!("{name}_seconds"), "test"))
            .unwrap(),
    };
    let metrics = Arc::new(CacheMetrics {
        requests: operation("requests"),
        loads: operation("loads"),
    });
    let bytes = IntGaugeVec::new(Opts::new("metadata_bytes", "test"), &["type"]).unwrap();
    let cache =
        Arc::new(PuffinMetadataCache::new(1024 * 1024, &bytes).with_load_metrics(metrics.clone()));
    let (_dir, stager) = new_bounded_stager("shared_metadata", 1024).await;
    let accessor = Arc::new(MockFileAccessor::new("shared_metadata"));
    let manager =
        FsPuffinManager::new(stager, accessor).with_puffin_metadata_cache(Some(cache.clone()));
    let file = "file".to_string();
    let mut writer = manager.writer(&file).await.unwrap();
    put_blob("blob", b"payload", None, &mut writer).await;
    writer.finish().await.unwrap();

    let requests = (0..8).map(|_| async {
        manager
            .reader(&file)
            .await
            .unwrap()
            .metadata()
            .await
            .unwrap()
    });
    let values = futures::future::join_all(requests).await;
    assert!(values.iter().all(|value| Arc::ptr_eq(value, &values[0])));
    assert_eq!(8, metrics.requests.succeeded.get());
    assert_eq!(1, metrics.loads.succeeded.get());
    assert_eq!(0, metrics.loads.active.get());
    assert_eq!(1, values[0].blobs.len());
    assert_eq!("blob", values[0].blobs[0].blob_type);

    cache.remove(&file).await;
    let reloaded = manager
        .reader(&file)
        .await
        .unwrap()
        .metadata()
        .await
        .unwrap();
    assert_eq!(values[0].as_ref(), reloaded.as_ref());
    assert!(!Arc::ptr_eq(&values[0], &reloaded));
    assert_eq!(2, metrics.loads.succeeded.get());
}

// After warming the metadata cache, reject footer reads while allowing blob reads.
struct FooterGuardAccessor {
    inner: MockFileAccessor,
    blob_end: Arc<std::sync::atomic::AtomicU64>,
}

struct FooterGuardReader {
    inner: common_base::range_read::FileReader,
    blob_end: Arc<std::sync::atomic::AtomicU64>,
}

#[async_trait::async_trait]
impl crate::puffin_manager::file_accessor::PuffinFileAccessor for FooterGuardAccessor {
    type Reader = FooterGuardReader;
    type Writer =
        <MockFileAccessor as crate::puffin_manager::file_accessor::PuffinFileAccessor>::Writer;
    type FileHandle = String;

    async fn reader(&self, handle: &String) -> crate::error::Result<Self::Reader> {
        Ok(FooterGuardReader {
            inner: self.inner.reader(handle).await?,
            blob_end: self.blob_end.clone(),
        })
    }

    async fn writer(&self, handle: &String) -> crate::error::Result<Self::Writer> {
        self.inner.writer(handle).await
    }
}

#[async_trait::async_trait]
impl RangeReader for FooterGuardReader {
    async fn metadata(&self) -> std::io::Result<common_base::range_read::Metadata> {
        self.inner.metadata().await
    }

    async fn read(&self, range: std::ops::Range<u64>) -> std::io::Result<bytes::Bytes> {
        if range.end > self.blob_end.load(std::sync::atomic::Ordering::Relaxed) {
            return Err(std::io::Error::other(
                "unexpected footer read after cache warmup",
            ));
        }
        self.inner.read(range).await
    }
}

impl common_base::range_read::SizeAwareRangeReader for FooterGuardReader {
    fn with_file_size_hint(&mut self, size: u64) {
        self.inner.with_file_size_hint(size);
    }
}

#[tokio::test]
async fn test_cached_metadata_used_when_staging_directory() {
    use std::sync::atomic::{AtomicU64, Ordering};

    use prometheus::{IntGaugeVec, Opts};

    use crate::puffin_manager::cache::PuffinMetadataCache;

    let bytes = IntGaugeVec::new(Opts::new("directory_metadata_bytes", "test"), &["type"]).unwrap();
    let cache = Arc::new(PuffinMetadataCache::new(1024 * 1024, &bytes));
    let (_dir, stager) = new_bounded_stager("cached_directory", 1024 * 1024).await;
    let blob_end = Arc::new(AtomicU64::new(u64::MAX));
    let accessor = Arc::new(FooterGuardAccessor {
        inner: MockFileAccessor::new("cached_directory"),
        blob_end: blob_end.clone(),
    });
    let manager =
        FsPuffinManager::new(stager.clone(), accessor).with_puffin_metadata_cache(Some(cache));
    let file = "directory".to_string();
    let mut writer = manager.writer(&file).await.unwrap();
    let files: &[(&str, &[u8])] = &[("a.txt", b"hello"), ("b.txt", b"world")];
    put_dir("dir", files, None, &mut writer).await;
    writer.finish().await.unwrap();
    let metadata = manager
        .reader(&file)
        .await
        .unwrap()
        .metadata()
        .await
        .unwrap();
    blob_end.store(
        metadata
            .blobs
            .iter()
            .map(|blob| u64::try_from(blob.offset + blob.length).unwrap())
            .max()
            .unwrap(),
        Ordering::Relaxed,
    );
    let reader = manager.reader(&file).await.unwrap();
    check_dir(&file, "dir", files, &stager, &reader).await;
}
