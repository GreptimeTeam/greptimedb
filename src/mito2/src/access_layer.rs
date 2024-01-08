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

use std::sync::Arc;

use object_store::ObjectStore;
use snafu::ResultExt;
use store_api::metadata::RegionMetadataRef;

use crate::cache::write_cache::SstUploadRequest;
use crate::cache::CacheManagerRef;
use crate::error::{DeleteSstSnafu, Result};
use crate::read::Source;
use crate::sst::file::{FileHandle, FileId};
use crate::sst::location;
use crate::sst::parquet::reader::ParquetReaderBuilder;
use crate::sst::parquet::writer::ParquetWriter;
use crate::sst::parquet::{SstInfo, WriteOptions};

pub type AccessLayerRef = Arc<AccessLayer>;

/// A layer to access SST files under the same directory.
pub struct AccessLayer {
    region_dir: String,
    /// Target object store.
    object_store: ObjectStore,
}

impl std::fmt::Debug for AccessLayer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AccessLayer")
            .field("region_dir", &self.region_dir)
            .finish()
    }
}

impl AccessLayer {
    /// Returns a new [AccessLayer] for specific `region_dir`.
    pub fn new(region_dir: impl Into<String>, object_store: ObjectStore) -> AccessLayer {
        AccessLayer {
            region_dir: region_dir.into(),
            object_store,
        }
    }

    /// Returns the directory of the region.
    pub fn region_dir(&self) -> &str {
        &self.region_dir
    }

    /// Returns the object store of the layer.
    pub fn object_store(&self) -> &ObjectStore {
        &self.object_store
    }

    /// Deletes a SST file with given file id.
    pub(crate) async fn delete_sst(&self, file_id: FileId) -> Result<()> {
        let path = location::sst_file_path(&self.region_dir, file_id);
        self.object_store
            .delete(&path)
            .await
            .context(DeleteSstSnafu { file_id })
    }

    /// Returns a reader builder for specific `file`.
    pub(crate) fn read_sst(&self, file: FileHandle) -> ParquetReaderBuilder {
        ParquetReaderBuilder::new(self.region_dir.clone(), file, self.object_store.clone())
    }

    /// Writes a SST with specific `file_id` and `metadata` to the layer.
    ///
    /// Returns the info of the SST. If no data written, returns None.
    pub(crate) async fn write_sst(
        &self,
        request: SstWriteRequest,
        write_opts: &WriteOptions,
    ) -> Result<Option<SstInfo>> {
        let path = location::sst_file_path(&self.region_dir, request.file_id);

        if let Some(write_cache) = request.cache_manager.write_cache() {
            // Write to the write cache.
            return write_cache
                .write_and_upload_sst(
                    SstUploadRequest {
                        file_id: request.file_id,
                        metadata: request.metadata,
                        source: request.source,
                        storage: request.storage,
                        upload_path: path,
                        remote_store: self.object_store.clone(),
                    },
                    write_opts,
                )
                .await;
        }

        // Write cache is disabled.
        let mut writer = ParquetWriter::new(path, request.metadata, self.object_store.clone());
        writer.write_all(request.source, write_opts).await
    }
}

/// Contents to build a SST.
pub(crate) struct SstWriteRequest {
    pub(crate) file_id: FileId,
    pub(crate) metadata: RegionMetadataRef,
    pub(crate) source: Source,
    pub(crate) cache_manager: CacheManagerRef,
    pub(crate) storage: Option<String>,
}
