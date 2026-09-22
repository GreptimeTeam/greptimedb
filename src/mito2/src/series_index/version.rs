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

//! Immutable index snapshots and aggregate series-file handles.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt::{self, Debug, Formatter};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};

use common_time::Timestamp;
use store_api::storage::{FileId, RegionId};

use crate::series_index::bucket::IndexBucket;
use crate::series_index::catalog::{IndexFileMetadata, SeriesIndexEntry};
use crate::series_index::purger::{IndexFilePurger, PurgeRequest};
use crate::sst::file::RegionFileId;

/// A reference-counted series-index file with deferred deletion semantics.
#[derive(Clone)]
pub(crate) struct SeriesIndexFileHandle {
    inner: Arc<SeriesIndexFileHandleInner>,
}

impl Debug for SeriesIndexFileHandle {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("SeriesIndexFileHandle")
            .field("file_id", &self.inner.file_id)
            .field("deleted", &self.inner.deleted.load(Ordering::Relaxed))
            .finish()
    }
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
            inner: Arc::new(SeriesIndexFileHandleInner {
                file_id: RegionFileId::new(region_id, entry.index_uuid),
                _disk_pin: purger.budget().and_then(|budget| {
                    budget.pin(&crate::series_index::catalog::series_index_path(
                        region_id,
                        entry.index_uuid,
                    ))
                }),
                entry,
                metadata,
                deleted: AtomicBool::new(false),
                purger,
            }),
        }
    }

    /// Returns the region and file identity used for storage and deletion.
    pub(crate) fn file_id(&self) -> RegionFileId {
        self.inner.file_id
    }

    pub(crate) fn entry(&self) -> &SeriesIndexEntry {
        &self.inner.entry
    }

    pub(crate) fn metadata(&self) -> IndexFileMetadata {
        self.inner.metadata
    }

    pub(crate) fn mark_deleted(&self) {
        self.inner.deleted.store(true, Ordering::Release);
        if let Some(budget) = self.inner.purger.budget() {
            budget.retire(&crate::series_index::catalog::series_index_path(
                self.inner.file_id.region_id(),
                self.inner.file_id.file_id(),
            ));
        }
    }
}

struct SeriesIndexFileHandleInner {
    file_id: RegionFileId,
    entry: SeriesIndexEntry,
    metadata: IndexFileMetadata,
    _disk_pin: Option<Arc<()>>,
    deleted: AtomicBool,
    purger: IndexFilePurger,
}

impl Drop for SeriesIndexFileHandleInner {
    fn drop(&mut self) {
        if self.deleted.load(Ordering::Acquire) {
            self.purger.purge(PurgeRequest {
                file_id: self.file_id,
            });
        }
    }
}

/// Immutable series-index snapshot for one region.
#[derive(Debug, Default)]
pub(crate) struct SeriesIndexVersion {
    /// Range indexes for visible SSTs; reconciliation removes IDs absent from its SST snapshot.
    /// Physical deletion is independently handled by the SST file purger.
    pub(crate) range_indexes: HashSet<FileId>,
    pub(crate) range_metadata: HashMap<FileId, IndexFileMetadata>,
    pub(crate) series_indexes: HashMap<FileId, SeriesIndexFileHandle>,
    pub(crate) index_buckets: BTreeMap<Timestamp, IndexBucket>,
    /// Keeps budgeted files alive even when SST garbage collection requests their deletion.
    pub(crate) disk_pins: Vec<Arc<()>>,
}

impl SeriesIndexVersion {
    /// Restores bucket lookup from immutable index coverage stored in the catalog.
    pub(crate) fn new(
        range_indexes: HashSet<FileId>,
        series_indexes: HashMap<FileId, SeriesIndexFileHandle>,
    ) -> Self {
        let mut index_buckets = BTreeMap::new();
        for handle in series_indexes.values() {
            IndexBucket::from_entry(handle.entry()).insert_into(&mut index_buckets);
        }
        Self {
            range_indexes,
            range_metadata: HashMap::new(),
            series_indexes,
            index_buckets,
            disk_pins: Vec::new(),
        }
    }

    pub(crate) fn with_range_metadata(
        mut self,
        mut metadata: HashMap<FileId, IndexFileMetadata>,
    ) -> Self {
        metadata.retain(|id, _| self.range_indexes.contains(id));
        self.range_metadata = metadata;
        self
    }

    /// Installed bytes for this region's snapshot; retained old snapshots are not charged.
    pub(crate) fn disk_usage(&self) -> u64 {
        self.range_metadata
            .values()
            .map(|meta| meta.file_size)
            .sum::<u64>()
            + self
                .series_indexes
                .values()
                .map(|handle| handle.metadata().file_size)
                .sum::<u64>()
    }

    pub(crate) fn with_disk_pins(
        mut self,
        region_id: RegionId,
        budget: Option<&Arc<crate::series_index::disk_budget::SeriesIndexDiskBudget>>,
    ) -> Self {
        if let Some(budget) = budget {
            self.range_indexes.retain(|id| {
                if let Some(pin) = budget.pin(&crate::series_index::catalog::range_index_path(
                    region_id, *id,
                )) {
                    self.disk_pins.push(pin);
                    true
                } else {
                    false
                }
            });
            self.series_indexes.retain(|id, _| {
                if let Some(pin) = budget.pin(&crate::series_index::catalog::series_index_path(
                    region_id, *id,
                )) {
                    self.disk_pins.push(pin);
                    true
                } else {
                    false
                }
            });
            self.range_metadata
                .retain(|id, _| self.range_indexes.contains(id));
            self.index_buckets.clear();
            for handle in self.series_indexes.values() {
                IndexBucket::from_entry(handle.entry()).insert_into(&mut self.index_buckets);
            }
        }
        self
    }

    fn mark_all_deleted(&self) {
        self.series_indexes
            .values()
            .for_each(SeriesIndexFileHandle::mark_deleted);
    }
}

/// Copy-on-write series-index snapshots owned by a region.
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
