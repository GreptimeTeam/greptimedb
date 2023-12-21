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

//! Cache files sent to remote object stores.

use std::sync::Arc;

use object_store::manager::ObjectStoreManagerRef;
use object_store::ObjectStore;
use store_api::metadata::RegionMetadataRef;
use store_api::storage::{RegionId, SequenceNumber};
use tokio::sync::mpsc::Sender;

use crate::error::Result;
use crate::read::Source;
use crate::request::WorkerRequest;
use crate::sst::file::{FileId, FileMeta};
use crate::sst::parquet::WriteOptions;
use crate::wal::EntryId;

// TODO(yingwen): Works with CacheManager.
/// A cache for uploading files to remote object stores.
///
/// It keeps files in local disk and sends files to object store in background.
pub struct UploadCache {
    /// Local object storage to store files to upload.
    local_store: ObjectStore,
    /// Object store manager.
    object_store_manager: ObjectStoreManagerRef,
}

pub type UploadCacheRef = Arc<UploadCache>;

impl UploadCache {
    // TODO(yingwen): Maybe pass cache path instead of local store.
    /// Create the upload cache with a `local_store` to cache files and a
    /// `object_store_manager` for all object stores.
    pub fn new(
        local_store: ObjectStore,
        object_store_manager: ObjectStoreManagerRef,
    ) -> UploadCache {
        // TODO(yingwen): Cache capacity.
        UploadCache {
            local_store,
            object_store_manager,
        }
    }

    fn upload(&self, upload: Upload) -> Result<()> {
        // Add the upload metadata to the manifest.
        unimplemented!()
    }
}

/// A remote write request to upload files.
struct Upload {
    /// Parts to upload.
    parts: Vec<UploadPart>,
}

/// Metadata of SSTs to upload together.
struct UploadPart {
    /// Region id.
    region_id: RegionId,
    /// Meta of files created.
    file_metas: Vec<FileMeta>,
    /// Target storage of SSTs.
    storage: Option<String>,
    /// Sender to send notify.
    request_sender: Option<Sender<WorkerRequest>>,
}

// TODO(yingwen): Cache manager to cache parquet meta.
/// Builder to build a upload part.
struct UploadPartBuilder {
    /// Metadata of the region.
    metadata: RegionMetadataRef,
    /// Meta of files created.
    file_metas: Vec<FileMeta>,
    /// Local object store to cache SSTs.
    local_store: ObjectStore,
    /// Target storage of SSTs.
    storage: Option<String>,
    /// Sender to send notify.
    request_sender: Sender<WorkerRequest>,
    /// The last entry id has been flushed.
    flushed_entry_id: Option<EntryId>,
    /// The last sequence has been flushed.
    flushed_sequence: Option<SequenceNumber>,
}

impl UploadPartBuilder {
    /// Write sst to the part.
    async fn write_sst(
        &mut self,
        file_id: FileId,
        source: Source,
        write_opts: &WriteOptions,
    ) -> Result<()> {
        // TODO(yingwen): Creates a writer from source and write it.
        // How to get flushed data.
        todo!()
    }

    /// Builds a part.
    fn build(self) -> UploadPart {
        unimplemented!()
    }
}
