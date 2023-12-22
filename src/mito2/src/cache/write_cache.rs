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

use crate::access_layer::sst_file_path;
use crate::error::Result;
use crate::read::Source;
use crate::request::WorkerRequest;
use crate::sst::file::{FileId, FileMeta, Level};
use crate::sst::parquet::writer::ParquetWriter;
use crate::sst::parquet::WriteOptions;
use crate::wal::EntryId;

/// A cache for uploading files to remote object stores.
///
/// It keeps files in local disk and sends files to object store in background.
pub(crate) struct WriteCache {
    /// Local object storage to store files to upload.
    local_store: ObjectStore,
    /// Object store manager.
    object_store_manager: ObjectStoreManagerRef,
}

pub(crate) type WriteCacheRef = Arc<WriteCache>;

impl WriteCache {
    // TODO(yingwen): Maybe pass cache path instead of local store.
    /// Create the cache with a `local_store` to cache files and a
    /// `object_store_manager` for all object stores.
    pub(crate) fn new(
        local_store: ObjectStore,
        object_store_manager: ObjectStoreManagerRef,
    ) -> Self {
        // TODO(yingwen): Cache capacity.
        Self {
            local_store,
            object_store_manager,
        }
    }

    /// Adds files to the cache.
    pub(crate) fn upload(&self, upload: Upload) -> Result<()> {
        // Add the upload metadata to the manifest.
        unimplemented!()
    }
}

/// A remote write request to upload files.
pub(crate) struct Upload {
    /// Parts to upload.
    pub(crate) parts: Vec<UploadPart>,
}

/// Metadata of SSTs to upload together.
pub(crate) struct UploadPart {
    /// Region id.
    region_id: RegionId,
    /// Directory of the region data.
    region_dir: String,
    /// Meta of files created.
    pub(crate) file_metas: Vec<FileMeta>,
    /// Target storage of SSTs.
    storage: Option<String>,
    /// Sender to send notify.
    request_sender: Option<Sender<WorkerRequest>>,
    /// The last entry id has been flushed.
    flushed_entry_id: Option<EntryId>,
    /// The last sequence has been flushed.
    flushed_sequence: Option<SequenceNumber>,
}

/// Writer to build a upload part.
pub(crate) struct UploadPartWriter {
    /// Local object store to cache SSTs.
    local_store: ObjectStore,
    /// Metadata of the region.
    metadata: RegionMetadataRef,
    /// Directory of the region.
    region_dir: String,
    /// Meta of files created.
    file_metas: Vec<FileMeta>,
    /// Target storage of SSTs.
    storage: Option<String>,
    /// Sender to send notify.
    request_sender: Option<Sender<WorkerRequest>>,
    /// The last entry id has been flushed.
    flushed_entry_id: Option<EntryId>,
    /// The last sequence has been flushed.
    flushed_sequence: Option<SequenceNumber>,
}

impl UploadPartWriter {
    /// Creates a new writer.
    pub(crate) fn new(local_store: ObjectStore, metadata: RegionMetadataRef) -> Self {
        Self {
            local_store,
            metadata,
            region_dir: String::new(),
            file_metas: Vec::new(),
            storage: None,
            request_sender: None,
            flushed_entry_id: None,
            flushed_sequence: None,
        }
    }

    /// Sets region directory for the part.
    #[must_use]
    pub(crate) fn with_region_dir(mut self, region_dir: String) -> Self {
        self.region_dir = region_dir;
        self
    }

    /// Sets target storage for the part.
    #[must_use]
    pub(crate) fn with_storage(mut self, storage: Option<String>) -> Self {
        self.storage = storage;
        self
    }

    /// Sets request sender for the part.
    #[must_use]
    pub(crate) fn with_request_sender(mut self, sender: Option<Sender<WorkerRequest>>) -> Self {
        self.request_sender = sender;
        self
    }

    /// Sets flushed entry id.
    #[must_use]
    pub(crate) fn with_flushed_entry_id(mut self, entry_id: Option<EntryId>) -> Self {
        self.flushed_entry_id = entry_id;
        self
    }

    /// Sets flushed sequence.
    #[must_use]
    pub(crate) fn with_flushed_sequence(mut self, sequence: Option<SequenceNumber>) -> Self {
        self.flushed_sequence = sequence;
        self
    }

    /// Reserve capacity for `additional` files.
    pub(crate) fn reserve_capacity(&mut self, addtional: usize) {
        self.file_metas.reserve(addtional);
    }

    /// Builds a new parquet writer to write to this part.
    pub(crate) fn new_sst_writer(&self, file_id: FileId) -> ParquetWriter {
        let path = sst_file_path(&self.region_dir, file_id);
        ParquetWriter::new(path, self.metadata.clone(), self.local_store.clone())
    }

    /// Adds a SST to this part.
    pub(crate) fn add_sst(&mut self, file_meta: FileMeta) {
        self.file_metas.push(file_meta);
    }

    /// Adds multiple SSTs to this part.
    pub(crate) fn extend_ssts(&mut self, iter: impl IntoIterator<Item = FileMeta>) {
        self.file_metas.extend(iter)
    }

    /// Returns [FileMeta] of written files.
    pub(crate) fn written_file_metas(&self) -> &[FileMeta] {
        &self.file_metas
    }

    /// Finishes the writer and builds a part.
    pub(crate) fn finish(self) -> UploadPart {
        UploadPart {
            region_id: self.metadata.region_id,
            region_dir: self.region_dir,
            file_metas: self.file_metas,
            storage: self.storage,
            request_sender: self.request_sender,
            flushed_entry_id: self.flushed_entry_id,
            flushed_sequence: self.flushed_sequence,
        }
    }
}
