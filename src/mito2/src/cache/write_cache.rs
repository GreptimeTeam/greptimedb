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

use api::v1::region;
use common_base::readable_size::ReadableSize;
use common_telemetry::{debug, info};
use object_store::manager::ObjectStoreManagerRef;
use object_store::ObjectStore;
use snafu::ResultExt;
use store_api::metadata::RegionMetadataRef;
use store_api::storage::RegionId;

use crate::access_layer::new_fs_object_store;
use crate::cache::file_cache::{FileCache, FileCacheRef, FileType, IndexKey, IndexValue};
use crate::error::{self, Result};
use crate::metrics::{FLUSH_ELAPSED, UPLOAD_BYTES_TOTAL};
use crate::read::Source;
use crate::sst::file::FileId;
use crate::sst::parquet::writer::ParquetWriter;
use crate::sst::parquet::{SstInfo, WriteOptions};
use crate::sst::DEFAULT_WRITE_BUFFER_SIZE;

/// A cache for uploading files to remote object stores.
///
/// It keeps files in local disk and then sends files to object stores.
pub struct WriteCache {
    /// Local file cache.
    file_cache: FileCacheRef,
    /// Object store manager.
    object_store_manager: ObjectStoreManagerRef,
}

pub type WriteCacheRef = Arc<WriteCache>;

impl WriteCache {
    /// Create the cache with a `local_store` to cache files and a
    /// `object_store_manager` for all object stores.
    pub async fn new(
        local_store: ObjectStore,
        object_store_manager: ObjectStoreManagerRef,
        cache_capacity: ReadableSize,
    ) -> Result<Self> {
        let file_cache = FileCache::new(local_store, cache_capacity);
        file_cache.recover().await?;

        Ok(Self {
            file_cache: Arc::new(file_cache),
            object_store_manager,
        })
    }

    /// Creates a write cache based on local fs.
    pub async fn new_fs(
        cache_dir: &str,
        object_store_manager: ObjectStoreManagerRef,
        cache_capacity: ReadableSize,
    ) -> Result<Self> {
        info!("Init write cache on {cache_dir}, capacity: {cache_capacity}");

        let local_store = new_fs_object_store(cache_dir).await?;
        Self::new(local_store, object_store_manager, cache_capacity).await
    }

    /// Returns the file cache of the write cache.
    pub(crate) fn file_cache(&self) -> FileCacheRef {
        self.file_cache.clone()
    }

    /// Writes SST to the cache and then uploads it to the remote object store.
    pub async fn write_and_upload_sst(
        &self,
        request: SstUploadRequest,
        write_opts: &WriteOptions,
    ) -> Result<Option<SstInfo>> {
        let timer = FLUSH_ELAPSED
            .with_label_values(&["write_sst"])
            .start_timer();

        let region_id = request.metadata.region_id;
        let file_id = request.file_id;
        let parquet_key = IndexKey::new(region_id, file_id, FileType::Parquet);

        // Write to FileCache.
        let mut writer = ParquetWriter::new(
            self.file_cache.cache_file_path(parquet_key),
            request.metadata,
            self.file_cache.local_store(),
        );

        let sst_info = writer.write_all(request.source, write_opts).await?;

        timer.stop_and_record();

        // Upload sst file to remote object store.
        let Some(sst_info) = sst_info else {
            // No data need to upload.
            return Ok(None);
        };

        let parquet_path = &request.upload_path;
        let remote_store = &request.remote_store;
        self.upload(parquet_key, parquet_path, remote_store).await?;

        if sst_info.inverted_index_available {
            let puffin_key = IndexKey::new(region_id, file_id, FileType::Puffin);
            let puffin_path = &request.index_upload_path;
            self.upload(puffin_key, puffin_path, remote_store).await?;
        }

        Ok(Some(sst_info))
    }

    /// Uploads a Parquet file or a Puffin file to the remote object store.
    async fn upload(
        &self,
        index_key: IndexKey,
        upload_path: &str,
        remote_store: &ObjectStore,
    ) -> Result<()> {
        let region_id = index_key.region_id;
        let file_id = index_key.file_id;
        let file_type = index_key.file_type;
        let cache_path = self.file_cache.cache_file_path(index_key);

        let timer = FLUSH_ELAPSED
            .with_label_values(&[match file_type {
                FileType::Parquet => "upload_parquet",
                FileType::Puffin => "upload_puffin",
            }])
            .start_timer();

        let reader = self
            .file_cache
            .local_store()
            .reader(&cache_path)
            .await
            .context(error::OpenDalSnafu)?;

        let mut writer = remote_store
            .writer_with(upload_path)
            .buffer(DEFAULT_WRITE_BUFFER_SIZE.as_bytes() as usize)
            .await
            .context(error::OpenDalSnafu)?;

        let bytes_written =
            futures::io::copy(reader, &mut writer)
                .await
                .context(error::UploadSnafu {
                    region_id,
                    file_id,
                    file_type,
                })?;

        // Must close to upload all data.
        writer.close().await.context(error::OpenDalSnafu)?;

        UPLOAD_BYTES_TOTAL.inc_by(bytes_written);

        debug!(
            "Successfully upload file to remote, region: {}, file: {}, upload_path: {}, cost: {:?}s",
            region_id,
            file_id,
            upload_path,
            timer.stop_and_record()
        );

        let index_value = IndexValue {
            file_size: bytes_written as _,
        };
        // Register to file cache
        self.file_cache.put(index_key, index_value).await;

        Ok(())
    }
}

/// Request to write and upload a SST.
pub struct SstUploadRequest {
    pub file_id: FileId,
    pub metadata: RegionMetadataRef,
    pub source: Source,
    pub storage: Option<String>,
    /// Path to upload the file.
    pub upload_path: String,
    /// Path to upload the index file.
    pub index_upload_path: String,
    /// Remote object store to upload.
    pub remote_store: ObjectStore,
}

#[cfg(test)]
mod tests {
    use api::v1::OpType;
    use common_base::readable_size::ReadableSize;
    use common_test_util::temp_dir::create_temp_dir;
    use object_store::manager::ObjectStoreManager;
    use object_store::services::Fs;
    use object_store::ObjectStore;
    use store_api::storage::RegionId;

    use super::*;
    use crate::cache::file_cache::{self, FileCache};
    use crate::cache::test_util::new_fs_store;
    use crate::sst::file::FileId;
    use crate::sst::location::{index_file_path, sst_file_path};
    use crate::test_util::sst_util::{
        new_batch_by_range, new_source, sst_file_handle, sst_region_metadata,
    };
    use crate::test_util::{build_rows, new_batch_builder, CreateRequestBuilder, TestEnv};

    #[tokio::test]
    async fn test_write_and_upload_sst() {
        // TODO(QuenKar): maybe find a way to create some object server for testing,
        // and now just use local file system to mock.
        let mut env = TestEnv::new();
        let mock_store = env.init_object_store_manager();
        let file_id = FileId::random();
        let upload_path = sst_file_path("test", file_id);
        let index_upload_path = index_file_path("test", file_id);

        // Create WriteCache
        let local_dir = create_temp_dir("");
        let local_store = new_fs_store(local_dir.path().to_str().unwrap());
        let object_store_manager = env.get_object_store_manager().unwrap();
        let write_cache = WriteCache::new(
            local_store.clone(),
            object_store_manager,
            ReadableSize::mb(10),
        )
        .await
        .unwrap();

        // Create Source
        let metadata = Arc::new(sst_region_metadata());
        let region_id = metadata.region_id;
        let source = new_source(&[
            new_batch_by_range(&["a", "d"], 0, 60),
            new_batch_by_range(&["b", "f"], 0, 40),
            new_batch_by_range(&["b", "h"], 100, 200),
        ]);

        let request = SstUploadRequest {
            file_id,
            metadata,
            source,
            storage: None,
            upload_path: upload_path.clone(),
            index_upload_path,
            remote_store: mock_store.clone(),
        };

        let write_opts = WriteOptions {
            row_group_size: 512,
            ..Default::default()
        };

        // Write to cache and upload sst to mock remote store
        let sst_info = write_cache
            .write_and_upload_sst(request, &write_opts)
            .await
            .unwrap()
            .unwrap();

        // Check write cache contains the key
        let key = IndexKey::new(region_id, file_id, FileType::Parquet);
        assert!(write_cache.file_cache.contains_key(&key));

        // Check file data
        let remote_data = mock_store.read(&upload_path).await.unwrap();
        let cache_data = local_store
            .read(&write_cache.file_cache.cache_file_path(key))
            .await
            .unwrap();
        assert_eq!(remote_data, cache_data);
    }
}
