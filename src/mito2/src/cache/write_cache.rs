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

//! A write-through cache for remote object stores.

use std::sync::Arc;
use std::time::{Duration, Instant};

use common_base::readable_size::ReadableSize;
use common_telemetry::{debug, info};
use futures::AsyncWriteExt;
use object_store::ObjectStore;
use snafu::ResultExt;
use store_api::storage::RegionId;
use tokio::sync::mpsc::{UnboundedSender, unbounded_channel};

use crate::access_layer::{
    FilePathProvider, Metrics, RegionFilePathFactory, SstInfoArray, SstWriteRequest,
    TempFileCleaner, WriteCachePathProvider, WriteType, new_fs_cache_store,
};
use crate::cache::file_cache::{FileCache, FileCacheRef, FileType, IndexKey, IndexValue};
use crate::cache::manifest_cache::ManifestCache;
use crate::error::{self, Result};
use crate::metrics::UPLOAD_BYTES_TOTAL;
use crate::region::opener::RegionLoadCacheTask;
use crate::sst::file::RegionFileId;
use crate::sst::index::IndexerBuilderImpl;
use crate::sst::index::intermediate::IntermediateManager;
use crate::sst::index::puffin_manager::{PuffinManagerFactory, SstPuffinManager};
use crate::sst::parquet::writer::ParquetWriter;
use crate::sst::parquet::{SstInfo, WriteOptions};
use crate::sst::{DEFAULT_WRITE_BUFFER_SIZE, DEFAULT_WRITE_CONCURRENCY};

/// A cache for uploading files to remote object stores.
///
/// It keeps files in local disk and then sends files to object stores.
pub struct WriteCache {
    /// Local file cache.
    file_cache: FileCacheRef,
    /// Puffin manager factory for index.
    puffin_manager_factory: PuffinManagerFactory,
    /// Intermediate manager for index.
    intermediate_manager: IntermediateManager,
    /// Sender for region load cache tasks.
    task_sender: UnboundedSender<RegionLoadCacheTask>,
    /// Optional cache for manifest files.
    manifest_cache: Option<ManifestCache>,
}

pub type WriteCacheRef = Arc<WriteCache>;

impl WriteCache {
    /// Create the cache with a `local_store` to cache files and a
    /// `object_store_manager` for all object stores.
    pub async fn new(
        local_store: ObjectStore,
        cache_capacity: ReadableSize,
        ttl: Option<Duration>,
        index_cache_percent: Option<u8>,
        puffin_manager_factory: PuffinManagerFactory,
        intermediate_manager: IntermediateManager,
        manifest_cache: Option<ManifestCache>,
    ) -> Result<Self> {
        let (task_sender, task_receiver) = unbounded_channel();

        let file_cache = Arc::new(FileCache::new(
            local_store,
            cache_capacity,
            ttl,
            index_cache_percent,
        ));
        file_cache.recover(false, Some(task_receiver)).await;

        Ok(Self {
            file_cache,
            puffin_manager_factory,
            intermediate_manager,
            task_sender,
            manifest_cache,
        })
    }

    /// Creates a write cache based on local fs.
    pub async fn new_fs(
        cache_dir: &str,
        cache_capacity: ReadableSize,
        ttl: Option<Duration>,
        index_cache_percent: Option<u8>,
        puffin_manager_factory: PuffinManagerFactory,
        intermediate_manager: IntermediateManager,
        manifest_cache_capacity: ReadableSize,
        manifest_cache_ttl: Option<Duration>,
    ) -> Result<Self> {
        info!("Init write cache on {cache_dir}, capacity: {cache_capacity}");

        let local_store = new_fs_cache_store(cache_dir).await?;

        // Create manifest cache if capacity is non-zero
        let manifest_cache = if manifest_cache_capacity.as_bytes() > 0 {
            Some(
                ManifestCache::new(
                    local_store.clone(),
                    manifest_cache_capacity,
                    manifest_cache_ttl,
                )
                .await,
            )
        } else {
            None
        };

        Self::new(
            local_store,
            cache_capacity,
            ttl,
            index_cache_percent,
            puffin_manager_factory,
            intermediate_manager,
            manifest_cache,
        )
        .await
    }

    /// Returns the file cache of the write cache.
    pub(crate) fn file_cache(&self) -> FileCacheRef {
        self.file_cache.clone()
    }

    /// Returns the manifest cache if available.
    pub(crate) fn manifest_cache(&self) -> Option<ManifestCache> {
        self.manifest_cache.clone()
    }

    /// Build the puffin manager
    pub(crate) fn build_puffin_manager(&self) -> SstPuffinManager {
        let store = self.file_cache.local_store();
        let path_provider = WriteCachePathProvider::new(self.file_cache.clone());
        self.puffin_manager_factory.build(store, path_provider)
    }

    /// Put encoded SST data to the cache and upload to the remote object store.
    pub(crate) async fn put_and_upload_sst(
        &self,
        data: &bytes::Bytes,
        region_id: RegionId,
        sst_info: &SstInfo,
        upload_request: SstUploadRequest,
    ) -> Result<Metrics> {
        let file_id = sst_info.file_id;
        let mut metrics = Metrics::new(WriteType::Flush);

        // Create index key for the SST file
        let parquet_key = IndexKey::new(region_id, file_id, FileType::Parquet);

        // Write to cache first
        let cache_start = Instant::now();
        let cache_path = self.file_cache.cache_file_path(parquet_key);
        let store = self.file_cache.local_store();
        let cleaner = TempFileCleaner::new(region_id, store.clone());
        let write_res = store
            .write(&cache_path, data.clone())
            .await
            .context(crate::error::OpenDalSnafu);
        if let Err(e) = write_res {
            cleaner.clean_by_file_id(file_id).await;
            return Err(e);
        }

        metrics.write_batch = cache_start.elapsed();

        // Upload to remote store
        let upload_start = Instant::now();
        let region_file_id = RegionFileId::new(region_id, file_id);
        let remote_path = upload_request
            .dest_path_provider
            .build_sst_file_path(region_file_id);

        if let Err(e) = self
            .upload(parquet_key, &remote_path, &upload_request.remote_store)
            .await
        {
            // Clean up cache on failure
            self.remove(parquet_key).await;
            return Err(e);
        }

        metrics.upload_parquet = upload_start.elapsed();
        Ok(metrics)
    }

    /// Returns the intermediate manager of the write cache.
    pub(crate) fn intermediate_manager(&self) -> &IntermediateManager {
        &self.intermediate_manager
    }

    /// Writes SST to the cache and then uploads it to the remote object store.
    pub(crate) async fn write_and_upload_sst(
        &self,
        write_request: SstWriteRequest,
        upload_request: SstUploadRequest,
        write_opts: &WriteOptions,
        metrics: &mut Metrics,
    ) -> Result<SstInfoArray> {
        let region_id = write_request.metadata.region_id;

        let store = self.file_cache.local_store();
        let path_provider = WriteCachePathProvider::new(self.file_cache.clone());
        let indexer = IndexerBuilderImpl {
            build_type: write_request.op_type.into(),
            metadata: write_request.metadata.clone(),
            row_group_size: write_opts.row_group_size,
            puffin_manager: self
                .puffin_manager_factory
                .build(store.clone(), path_provider.clone()),
            intermediate_manager: self.intermediate_manager.clone(),
            index_options: write_request.index_options,
            inverted_index_config: write_request.inverted_index_config,
            fulltext_index_config: write_request.fulltext_index_config,
            bloom_filter_index_config: write_request.bloom_filter_index_config,
        };

        let cleaner = TempFileCleaner::new(region_id, store.clone());
        // Write to FileCache.
        let mut writer = ParquetWriter::new_with_object_store(
            store.clone(),
            write_request.metadata,
            write_request.index_config,
            indexer,
            path_provider.clone(),
            metrics,
        )
        .await
        .with_file_cleaner(cleaner);

        let sst_info = match write_request.source {
            either::Left(source) => {
                writer
                    .write_all(source, write_request.max_sequence, write_opts)
                    .await?
            }
            either::Right(flat_source) => writer.write_all_flat(flat_source, write_opts).await?,
        };

        // Upload sst file to remote object store.
        if sst_info.is_empty() {
            return Ok(sst_info);
        }

        let mut upload_tracker = UploadTracker::new(region_id);
        let mut err = None;
        let remote_store = &upload_request.remote_store;
        for sst in &sst_info {
            let parquet_key = IndexKey::new(region_id, sst.file_id, FileType::Parquet);
            let parquet_path = upload_request
                .dest_path_provider
                .build_sst_file_path(RegionFileId::new(region_id, sst.file_id));
            let start = Instant::now();
            if let Err(e) = self.upload(parquet_key, &parquet_path, remote_store).await {
                err = Some(e);
                break;
            }
            metrics.upload_parquet += start.elapsed();
            upload_tracker.push_uploaded_file(parquet_path);

            if sst.index_metadata.file_size > 0 {
                let puffin_key = IndexKey::new(region_id, sst.file_id, FileType::Puffin);
                let puffin_path = upload_request
                    .dest_path_provider
                    .build_index_file_path(RegionFileId::new(region_id, sst.file_id));
                let start = Instant::now();
                if let Err(e) = self.upload(puffin_key, &puffin_path, remote_store).await {
                    err = Some(e);
                    break;
                }
                metrics.upload_puffin += start.elapsed();
                upload_tracker.push_uploaded_file(puffin_path);
            }
        }

        if let Some(err) = err {
            // Cleans files on failure.
            upload_tracker
                .clean(&sst_info, &self.file_cache, remote_store)
                .await;
            return Err(err);
        }

        Ok(sst_info)
    }

    /// Removes a file from the cache by `index_key`.
    pub(crate) async fn remove(&self, index_key: IndexKey) {
        self.file_cache.remove(index_key).await
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
        self.file_cache
            .download(index_key, remote_path, remote_store, file_size)
            .await
    }

    /// Uploads a Parquet file or a Puffin file to the remote object store.
    pub(crate) async fn upload(
        &self,
        index_key: IndexKey,
        upload_path: &str,
        remote_store: &ObjectStore,
    ) -> Result<()> {
        let region_id = index_key.region_id;
        let file_id = index_key.file_id;
        let file_type = index_key.file_type;
        let cache_path = self.file_cache.cache_file_path(index_key);

        let start = Instant::now();
        let cached_value = self
            .file_cache
            .local_store()
            .stat(&cache_path)
            .await
            .context(error::OpenDalSnafu)?;
        let reader = self
            .file_cache
            .local_store()
            .reader(&cache_path)
            .await
            .context(error::OpenDalSnafu)?
            .into_futures_async_read(0..cached_value.content_length())
            .await
            .context(error::OpenDalSnafu)?;

        let mut writer = remote_store
            .writer_with(upload_path)
            .chunk(DEFAULT_WRITE_BUFFER_SIZE.as_bytes() as usize)
            .concurrent(DEFAULT_WRITE_CONCURRENCY)
            .await
            .context(error::OpenDalSnafu)?
            .into_futures_async_write();

        let bytes_written =
            futures::io::copy(reader, &mut writer)
                .await
                .context(error::UploadSnafu {
                    region_id,
                    file_id,
                    file_type,
                })?;

        // Must close to upload all data.
        writer.close().await.context(error::UploadSnafu {
            region_id,
            file_id,
            file_type,
        })?;

        UPLOAD_BYTES_TOTAL.inc_by(bytes_written);

        debug!(
            "Successfully upload file to remote, region: {}, file: {}, upload_path: {}, cost: {:?}",
            region_id,
            file_id,
            upload_path,
            start.elapsed(),
        );

        let index_value = IndexValue {
            file_size: bytes_written as _,
        };
        // Register to file cache
        self.file_cache.put(index_key, index_value).await;

        Ok(())
    }

    /// Sends a region load cache task to the background processing queue.
    ///
    /// If the receiver has been dropped, the error is ignored.
    pub(crate) fn load_region_cache(&self, task: RegionLoadCacheTask) {
        let _ = self.task_sender.send(task);
    }
}

/// Request to write and upload a SST.
pub struct SstUploadRequest {
    /// Destination path provider of which SST files in write cache should be uploaded to.
    pub dest_path_provider: RegionFilePathFactory,
    /// Remote object store to upload.
    pub remote_store: ObjectStore,
}

/// A structs to track files to upload and clean them if upload failed.
pub(crate) struct UploadTracker {
    /// Id of the region to track.
    region_id: RegionId,
    /// Paths of files uploaded successfully.
    files_uploaded: Vec<String>,
}

impl UploadTracker {
    /// Creates a new instance of `UploadTracker` for a given region.
    pub(crate) fn new(region_id: RegionId) -> Self {
        Self {
            region_id,
            files_uploaded: Vec::new(),
        }
    }

    /// Add a file path to the list of uploaded files.
    pub(crate) fn push_uploaded_file(&mut self, path: String) {
        self.files_uploaded.push(path);
    }

    /// Cleans uploaded files and files in the file cache at best effort.
    pub(crate) async fn clean(
        &self,
        sst_info: &SstInfoArray,
        file_cache: &FileCacheRef,
        remote_store: &ObjectStore,
    ) {
        common_telemetry::info!(
            "Start cleaning files on upload failure, region: {}, num_ssts: {}",
            self.region_id,
            sst_info.len()
        );

        // Cleans files in the file cache first.
        for sst in sst_info {
            let parquet_key = IndexKey::new(self.region_id, sst.file_id, FileType::Parquet);
            file_cache.remove(parquet_key).await;

            if sst.index_metadata.file_size > 0 {
                let puffin_key = IndexKey::new(self.region_id, sst.file_id, FileType::Puffin);
                file_cache.remove(puffin_key).await;
            }
        }

        // Cleans uploaded files.
        for file_path in &self.files_uploaded {
            if let Err(e) = remote_store.delete(file_path).await {
                common_telemetry::error!(e; "Failed to delete file {}", file_path);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use common_test_util::temp_dir::create_temp_dir;
    use object_store::ATOMIC_WRITE_DIR;
    use store_api::region_request::PathType;

    use super::*;
    use crate::access_layer::OperationType;
    use crate::cache::test_util::new_fs_store;
    use crate::cache::{CacheManager, CacheStrategy};
    use crate::error::InvalidBatchSnafu;
    use crate::read::Source;
    use crate::region::options::IndexOptions;
    use crate::sst::parquet::reader::ParquetReaderBuilder;
    use crate::test_util::TestEnv;
    use crate::test_util::sst_util::{
        assert_parquet_metadata_eq, new_batch_by_range, new_source, sst_file_handle_with_file_id,
        sst_region_metadata,
    };

    #[tokio::test]
    async fn test_write_and_upload_sst() {
        // TODO(QuenKar): maybe find a way to create some object server for testing,
        // and now just use local file system to mock.
        let mut env = TestEnv::new().await;
        let mock_store = env.init_object_store_manager();
        let path_provider = RegionFilePathFactory::new("test".to_string(), PathType::Bare);

        let local_dir = create_temp_dir("");
        let local_store = new_fs_store(local_dir.path().to_str().unwrap());

        let write_cache = env
            .create_write_cache(local_store.clone(), ReadableSize::mb(10))
            .await;

        // Create Source
        let metadata = Arc::new(sst_region_metadata());
        let region_id = metadata.region_id;
        let source = new_source(&[
            new_batch_by_range(&["a", "d"], 0, 60),
            new_batch_by_range(&["b", "f"], 0, 40),
            new_batch_by_range(&["b", "h"], 100, 200),
        ]);

        let write_request = SstWriteRequest {
            op_type: OperationType::Flush,
            metadata,
            source: either::Left(source),
            storage: None,
            max_sequence: None,
            cache_manager: Default::default(),
            index_options: IndexOptions::default(),
            index_config: Default::default(),
            inverted_index_config: Default::default(),
            fulltext_index_config: Default::default(),
            bloom_filter_index_config: Default::default(),
        };

        let upload_request = SstUploadRequest {
            dest_path_provider: path_provider.clone(),
            remote_store: mock_store.clone(),
        };

        let write_opts = WriteOptions {
            row_group_size: 512,
            ..Default::default()
        };

        // Write to cache and upload sst to mock remote store
        let mut metrics = Metrics::new(WriteType::Flush);
        let mut sst_infos = write_cache
            .write_and_upload_sst(write_request, upload_request, &write_opts, &mut metrics)
            .await
            .unwrap();
        let sst_info = sst_infos.remove(0);

        let file_id = sst_info.file_id;
        let sst_upload_path =
            path_provider.build_sst_file_path(RegionFileId::new(region_id, file_id));
        let index_upload_path =
            path_provider.build_index_file_path(RegionFileId::new(region_id, file_id));

        // Check write cache contains the key
        let key = IndexKey::new(region_id, file_id, FileType::Parquet);
        assert!(write_cache.file_cache.contains_key(&key));

        // Check file data
        let remote_data = mock_store.read(&sst_upload_path).await.unwrap();
        let cache_data = local_store
            .read(&write_cache.file_cache.cache_file_path(key))
            .await
            .unwrap();
        assert_eq!(remote_data.to_vec(), cache_data.to_vec());

        // Check write cache contains the index key
        let index_key = IndexKey::new(region_id, file_id, FileType::Puffin);
        assert!(write_cache.file_cache.contains_key(&index_key));

        let remote_index_data = mock_store.read(&index_upload_path).await.unwrap();
        let cache_index_data = local_store
            .read(&write_cache.file_cache.cache_file_path(index_key))
            .await
            .unwrap();
        assert_eq!(remote_index_data.to_vec(), cache_index_data.to_vec());

        // Removes the file from the cache.
        let sst_index_key = IndexKey::new(region_id, file_id, FileType::Parquet);
        write_cache.remove(sst_index_key).await;
        assert!(!write_cache.file_cache.contains_key(&sst_index_key));
        write_cache.remove(index_key).await;
        assert!(!write_cache.file_cache.contains_key(&index_key));
    }

    #[tokio::test]
    async fn test_read_metadata_from_write_cache() {
        common_telemetry::init_default_ut_logging();
        let mut env = TestEnv::new().await;
        let data_home = env.data_home().display().to_string();
        let mock_store = env.init_object_store_manager();

        let local_dir = create_temp_dir("");
        let local_path = local_dir.path().to_str().unwrap();
        let local_store = new_fs_store(local_path);

        // Create a cache manager using only write cache
        let write_cache = env
            .create_write_cache(local_store.clone(), ReadableSize::mb(10))
            .await;
        let cache_manager = Arc::new(
            CacheManager::builder()
                .write_cache(Some(write_cache.clone()))
                .build(),
        );

        // Create source
        let metadata = Arc::new(sst_region_metadata());

        let source = new_source(&[
            new_batch_by_range(&["a", "d"], 0, 60),
            new_batch_by_range(&["b", "f"], 0, 40),
            new_batch_by_range(&["b", "h"], 100, 200),
        ]);

        // Write to local cache and upload sst to mock remote store
        let write_request = SstWriteRequest {
            op_type: OperationType::Flush,
            metadata,
            source: either::Left(source),
            storage: None,
            max_sequence: None,
            cache_manager: cache_manager.clone(),
            index_options: IndexOptions::default(),
            index_config: Default::default(),
            inverted_index_config: Default::default(),
            fulltext_index_config: Default::default(),
            bloom_filter_index_config: Default::default(),
        };
        let write_opts = WriteOptions {
            row_group_size: 512,
            ..Default::default()
        };
        let upload_request = SstUploadRequest {
            dest_path_provider: RegionFilePathFactory::new(data_home.clone(), PathType::Bare),
            remote_store: mock_store.clone(),
        };

        let mut metrics = Metrics::new(WriteType::Flush);
        let mut sst_infos = write_cache
            .write_and_upload_sst(write_request, upload_request, &write_opts, &mut metrics)
            .await
            .unwrap();
        let sst_info = sst_infos.remove(0);
        let write_parquet_metadata = sst_info.file_metadata.unwrap();

        // Read metadata from write cache
        let handle = sst_file_handle_with_file_id(sst_info.file_id, 0, 1000);
        let builder = ParquetReaderBuilder::new(
            data_home,
            PathType::Bare,
            handle.clone(),
            mock_store.clone(),
        )
        .cache(CacheStrategy::EnableAll(cache_manager.clone()));
        let reader = builder.build().await.unwrap();

        // Check parquet metadata
        assert_parquet_metadata_eq(write_parquet_metadata, reader.parquet_metadata());
    }

    #[tokio::test]
    async fn test_write_cache_clean_tmp_files() {
        common_telemetry::init_default_ut_logging();
        let mut env = TestEnv::new().await;
        let data_home = env.data_home().display().to_string();
        let mock_store = env.init_object_store_manager();

        let write_cache_dir = create_temp_dir("");
        let write_cache_path = write_cache_dir.path().to_str().unwrap();
        let write_cache = env
            .create_write_cache_from_path(write_cache_path, ReadableSize::mb(10))
            .await;

        // Create a cache manager using only write cache
        let cache_manager = Arc::new(
            CacheManager::builder()
                .write_cache(Some(write_cache.clone()))
                .build(),
        );

        // Create source
        let metadata = Arc::new(sst_region_metadata());

        // Creates a source that can return an error to abort the writer.
        let source = Source::Iter(Box::new(
            [
                Ok(new_batch_by_range(&["a", "d"], 0, 60)),
                InvalidBatchSnafu {
                    reason: "Abort the writer",
                }
                .fail(),
            ]
            .into_iter(),
        ));

        // Write to local cache and upload sst to mock remote store
        let write_request = SstWriteRequest {
            op_type: OperationType::Flush,
            metadata,
            source: either::Left(source),
            storage: None,
            max_sequence: None,
            cache_manager: cache_manager.clone(),
            index_options: IndexOptions::default(),
            index_config: Default::default(),
            inverted_index_config: Default::default(),
            fulltext_index_config: Default::default(),
            bloom_filter_index_config: Default::default(),
        };
        let write_opts = WriteOptions {
            row_group_size: 512,
            ..Default::default()
        };
        let upload_request = SstUploadRequest {
            dest_path_provider: RegionFilePathFactory::new(data_home.clone(), PathType::Bare),
            remote_store: mock_store.clone(),
        };

        let mut metrics = Metrics::new(WriteType::Flush);
        write_cache
            .write_and_upload_sst(write_request, upload_request, &write_opts, &mut metrics)
            .await
            .unwrap_err();
        let atomic_write_dir = write_cache_dir.path().join(ATOMIC_WRITE_DIR);
        let mut entries = tokio::fs::read_dir(&atomic_write_dir).await.unwrap();
        let mut has_files = false;
        while let Some(entry) = entries.next_entry().await.unwrap() {
            if entry.file_type().await.unwrap().is_dir() {
                continue;
            }
            has_files = true;
            common_telemetry::warn!(
                "Found remaining temporary file in atomic dir: {}",
                entry.path().display()
            );
        }

        assert!(!has_files);
    }
}
