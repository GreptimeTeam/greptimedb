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

//! Immutable index snapshots and reference-counted file handles.

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};

use common_time::Timestamp;
use store_api::storage::{FileId, RegionId};

use crate::series_index::bucket::IndexBucket;
use crate::series_index::catalog::{IndexFileMetadata, SeriesIndexEntry};
use crate::series_index::purger::{IndexFilePurger, IndexFileType, PurgeRequest};
use crate::sst::file::RegionFileId;

/// Physical deletion waits until every snapshot releases its handle.
#[derive(Debug, Clone)]
pub(crate) struct IndexFileHandle(Arc<IndexFileHandleInner>);

#[derive(Debug)]
struct IndexFileHandleInner {
    request: PurgeRequest,
    metadata: IndexFileMetadata,
    deleted: AtomicBool,
    purger: IndexFilePurger,
}

impl IndexFileHandle {
    pub(crate) fn new(
        region_id: RegionId,
        file_id: FileId,
        kind: IndexFileType,
        metadata: IndexFileMetadata,
        purger: IndexFilePurger,
    ) -> Self {
        Self(Arc::new(IndexFileHandleInner {
            request: PurgeRequest {
                file_id: RegionFileId::new(region_id, file_id),
                kind,
            },
            metadata,
            deleted: AtomicBool::new(false),
            purger,
        }))
    }

    pub(crate) fn metadata(&self) -> IndexFileMetadata {
        self.0.metadata
    }

    pub(crate) fn mark_deleted(&self) {
        self.0.purger.retire(self.0.request);
        self.0.deleted.store(true, Ordering::Release);
    }
}

impl Drop for IndexFileHandleInner {
    fn drop(&mut self) {
        if self.deleted.load(Ordering::Acquire) {
            self.purger.purge(self.request);
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct SeriesIndexFileHandle {
    file: IndexFileHandle,
    entry: Arc<SeriesIndexEntry>,
}

impl SeriesIndexFileHandle {
    #[cfg(test)]
    pub(crate) fn new(
        region_id: RegionId,
        entry: SeriesIndexEntry,
        purger: IndexFilePurger,
    ) -> Self {
        let metadata = IndexFileMetadata {
            file_size: 0,
            min_timestamp: entry.bucket_start,
        };
        Self::with_metadata(region_id, entry, metadata, purger)
    }

    pub(crate) fn with_metadata(
        region_id: RegionId,
        entry: SeriesIndexEntry,
        metadata: IndexFileMetadata,
        purger: IndexFilePurger,
    ) -> Self {
        Self {
            file: IndexFileHandle::new(
                region_id,
                entry.index_uuid,
                IndexFileType::Series,
                metadata,
                purger,
            ),
            entry: Arc::new(entry),
        }
    }

    pub(crate) fn file_id(&self) -> RegionFileId {
        self.file.0.request.file_id
    }

    pub(crate) fn file_handle(&self) -> &IndexFileHandle {
        &self.file
    }

    pub(crate) fn entry(&self) -> &SeriesIndexEntry {
        &self.entry
    }

    pub(crate) fn metadata(&self) -> IndexFileMetadata {
        self.file.metadata()
    }

    pub(crate) fn mark_deleted(&self) {
        self.file.mark_deleted();
    }
}

#[derive(Debug, Default)]
pub(crate) struct SeriesIndexVersion {
    pub(crate) range_indexes: HashMap<FileId, IndexFileHandle>,
    pub(crate) series_indexes: HashMap<FileId, SeriesIndexFileHandle>,
    pub(crate) index_buckets: BTreeMap<Timestamp, IndexBucket>,
}

impl SeriesIndexVersion {
    pub(crate) fn new(
        range_indexes: HashMap<FileId, IndexFileHandle>,
        series_indexes: HashMap<FileId, SeriesIndexFileHandle>,
    ) -> Self {
        let mut index_buckets = BTreeMap::new();
        for handle in series_indexes.values() {
            IndexBucket::from_entry(handle.entry()).insert_into(&mut index_buckets);
        }
        Self {
            range_indexes,
            series_indexes,
            index_buckets,
        }
    }

    /// Published bytes; retained old snapshots are not charged.
    pub(crate) fn disk_usage(&self) -> u64 {
        self.range_indexes
            .values()
            .map(|h| h.metadata().file_size)
            .sum::<u64>()
            + self
                .series_indexes
                .values()
                .map(|h| h.metadata().file_size)
                .sum::<u64>()
    }

    fn mark_all_deleted(&self) {
        self.range_indexes
            .values()
            .for_each(IndexFileHandle::mark_deleted);
        self.series_indexes
            .values()
            .for_each(SeriesIndexFileHandle::mark_deleted);
    }
}

/// Shared by maintenance and the region, including while the region is closed.
#[derive(Debug, Default)]
pub(crate) struct SeriesIndexVersionControl {
    current: RwLock<Arc<SeriesIndexVersion>>,
}

impl SeriesIndexVersionControl {
    pub(crate) fn current(&self) -> Arc<SeriesIndexVersion> {
        self.current.read().unwrap().clone()
    }

    pub(crate) fn publish(&self, next: Arc<SeriesIndexVersion>) -> Arc<SeriesIndexVersion> {
        std::mem::replace(&mut *self.current.write().unwrap(), next)
    }

    pub(crate) fn mark_dropped(&self) {
        self.publish(Arc::new(SeriesIndexVersion::default()))
            .mark_all_deleted();
    }
}
