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

use object_store::{util, ObjectStore};
use snafu::ResultExt;
use store_api::metadata::RegionMetadataRef;

use crate::cache::write_cache::UploadPartWriter;
use crate::cache::CacheManagerRef;
use crate::error::{DeleteSstSnafu, Result};
use crate::sst::file::{FileHandle, FileId};
use crate::sst::parquet::reader::ParquetReaderBuilder;

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
        let path = sst_file_path(&self.region_dir, file_id);
        self.object_store
            .delete(&path)
            .await
            .context(DeleteSstSnafu { file_id })
    }

    /// Returns a reader builder for specific `file`.
    pub(crate) fn read_sst(&self, file: FileHandle) -> ParquetReaderBuilder {
        ParquetReaderBuilder::new(self.region_dir.clone(), file, self.object_store.clone())
    }

    // Returns a [UploadPartWriter] to upload.
    pub(crate) fn upload_part_writer(
        &self,
        metadata: RegionMetadataRef,
        _cache_manager: &CacheManagerRef,
    ) -> UploadPartWriter {
        // TODO(yingwen): Use local store in the cache manager once the cache is ready.
        UploadPartWriter::new(self.object_store.clone(), metadata)
            .with_region_dir(self.region_dir.clone())
    }
}

/// Returns the `file_path` for the `file_id` in the object store.
pub(crate) fn sst_file_path(region_dir: &str, file_id: FileId) -> String {
    util::join_path(region_dir, &file_id.as_parquet())
}
