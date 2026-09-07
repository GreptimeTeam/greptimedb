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

use std::collections::{HashMap, HashSet};
use std::fmt::{self, Debug, Formatter};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};

use store_api::storage::{FileId, RegionId};

use super::catalog::SeriesIndexEntry;
use super::purger::{IndexFilePurger, PurgeRequest};
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
    pub(crate) fn new(
        region_id: RegionId,
        entry: SeriesIndexEntry,
        purger: IndexFilePurger,
    ) -> Self {
        Self {
            inner: Arc::new(SeriesIndexFileHandleInner {
                file_id: RegionFileId::new(region_id, entry.index_uuid),
                entry,
                deleted: AtomicBool::new(false),
                purger,
            }),
        }
    }

    pub(crate) fn entry(&self) -> &SeriesIndexEntry {
        &self.inner.entry
    }

    pub(crate) fn mark_deleted(&self) {
        self.inner.deleted.store(true, Ordering::Release);
    }
}

struct SeriesIndexFileHandleInner {
    file_id: RegionFileId,
    entry: SeriesIndexEntry,
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
    pub(crate) range_indexes: HashSet<FileId>,
    pub(crate) series_indexes: HashMap<FileId, SeriesIndexFileHandle>,
}

impl SeriesIndexVersion {
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
