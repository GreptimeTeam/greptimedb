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

use common_base::readable_size::ReadableSize;
use common_telemetry::info;
use object_store::manager::ObjectStoreManagerRef;
use object_store::ObjectStore;
use store_api::metadata::RegionMetadataRef;

use crate::access_layer::new_fs_object_store;
use crate::cache::file_cache::{FileCache, FileCacheRef};
use crate::error::Result;
use crate::read::Source;
use crate::sst::file::FileId;
use crate::sst::parquet::writer::ParquetWriter;
use crate::sst::parquet::{SstInfo, WriteOptions};

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
        // TODO(yingwen): Write to the local store and then upload.
        // Now we write to the remote and ignore local cache.
        let mut writer = ParquetWriter::new(
            request.region_dir,
            request.file_id,
            request.metadata,
            request.remote_store,
        );
        writer.write_all(request.source, write_opts).await
    }
}

/// Request to write and upload a SST.
pub struct SstUploadRequest {
    pub file_id: FileId,
    pub metadata: RegionMetadataRef,
    pub source: Source,
    pub storage: Option<String>,
    pub region_dir: String,
    /// Remote object store to upload.
    pub remote_store: ObjectStore,
}
