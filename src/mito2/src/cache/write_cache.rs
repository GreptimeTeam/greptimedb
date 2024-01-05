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
use common_telemetry::info;
use object_store::manager::ObjectStoreManagerRef;
use object_store::ObjectStore;
use snafu::ResultExt;
use store_api::metadata::RegionMetadataRef;

use crate::access_layer::new_fs_object_store;
use crate::cache::file_cache::{FileCache, FileCacheRef, IndexValue};
use crate::error::{self, Result};
use crate::metrics::{UPLOAD_BYTES_TOTAL, WRITE_AND_UPLOAD_ELAPSED_TOTAL};
use crate::read::Source;
use crate::sst::file::FileId;
use crate::sst::parquet::writer::ParquetWriter;
use crate::sst::parquet::{SstInfo, WriteOptions, DEFAULT_WRITE_BUFFER_SIZE};

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

    /// Writes SST to the cache and then uploads it to the remote object store.
    pub async fn write_and_upload_sst(
        &self,
        request: SstUploadRequest,
        write_opts: &WriteOptions,
    ) -> Result<Option<SstInfo>> {
        let _timer = WRITE_AND_UPLOAD_ELAPSED_TOTAL.start_timer();

        let region_id = request.metadata.region_id;
        let file_id = request.file_id;

        let cache_path = self.file_cache.cache_file_path((region_id, file_id));
        // Write to FileCache.
        let mut writer = ParquetWriter::new(
            cache_path.clone(),
            request.metadata,
            self.file_cache.local_store(),
        );

        let sst_info = writer.write_all(request.source, write_opts).await?;

        // Upload sst file to remote object store.
        let upload_path = request.upload_path.clone();
        let reader = self
            .file_cache
            .local_store()
            .reader(&cache_path)
            .await
            .context(error::OpenDalSnafu)?;

        let mut writer = request
            .remote_store
            .writer_with(&upload_path)
            .buffer(DEFAULT_WRITE_BUFFER_SIZE.as_bytes() as usize)
            .await
            .context(error::OpenDalSnafu)?;

        let n = futures::io::copy(reader, &mut writer)
            .await
            .context(error::UploadSstSnafu { region_id, file_id })?;

        UPLOAD_BYTES_TOTAL.inc_by(n);

        // Must close to upload all data.
        writer.close().await.context(error::OpenDalSnafu)?;

        info!(
            "Upload file to remote, file: {}, upload_path: {}",
            file_id, upload_path
        );

        // Register to file cache
        if let Some(sst_info) = &sst_info {
            let file_size = sst_info.file_size as u32;
            self.file_cache
                .put((region_id, file_id), IndexValue::new(file_size))
                .await;
        }

        Ok(sst_info)
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
    /// Remote object store to upload.
    pub remote_store: ObjectStore,
}
