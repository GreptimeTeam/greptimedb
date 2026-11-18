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

use std::collections::{BTreeMap, HashMap};
use std::fmt::{self, Debug, Formatter};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};

use common_time::Timestamp;
use store_api::storage::{FileId, RegionId};

use crate::series_index::bucket::IndexBucket;
use crate::series_index::catalog::{RangeIndexEntry, SeriesIndexEntry};
use crate::series_index::purger::{IndexFilePurger, PurgeRequest};
use crate::sst::file::RegionFileId;

/// Legacy codec coverage descriptor derived from immutable narthex metadata.
/// Narthex admission and scan selection use the region-qualified `sources` instead.
pub(crate) fn narthex_series_entry(
    file: &crate::sst::file::FileMeta,
) -> crate::error::Result<SeriesIndexEntry> {
    use crate::series_index::artifact::{ArtifactFormat, ArtifactMetadata, ArtifactObject};
    let crate::sst::file::FileFormat::NarthexSeriesIndex(index) = &file.format else {
        return Err(crate::series_index::columnar::invalid(
            "expected narthex series metadata",
        ));
    };
    Ok(SeriesIndexEntry {
        artifact: Some(ArtifactMetadata {
            format: ArtifactFormat::ColumnarSeries,
            generation: file.file_id,
            objects: index
                .parts
                .iter()
                .chain(std::iter::once(&index.manifest))
                .map(|object| ArtifactObject {
                    path: object.path.clone(),
                    file_size: object.file_size,
                })
                .collect(),
            file_size: file.file_size,
        }),
        file_size: file.file_size,
        index_uuid: file.file_id,
        bucket_start: file.time_range.0,
        bucket_end: file.time_range.1,
        source_file_ids: index
            .sources
            .iter()
            .map(|source| source.file_id())
            .collect(),
        min_file_sequence: 0,
        max_file_sequence: 0,
        compaction_window_secs: 0,
        window_sequences: Default::default(),
    })
}

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
                purger: Some(purger),
                narthex_file: None,
            }),
        }
    }

    pub(crate) fn from_narthex(file: crate::sst::file::FileHandle) -> crate::error::Result<Self> {
        let entry = narthex_series_entry(file.meta_ref())?;
        Ok(Self {
            inner: Arc::new(SeriesIndexFileHandleInner {
                file_id: file.file_id(),
                entry,
                deleted: AtomicBool::new(false),
                purger: None,
                narthex_file: Some(file),
            }),
        })
    }

    pub(crate) fn manifest_path(&self) -> String {
        if let Some(file) = &self.inner.narthex_file
            && let crate::sst::file::FileFormat::NarthexSeriesIndex(index) = &file.meta_ref().format
        {
            return index.manifest.path.clone();
        }
        format!(
            "{}/series/{}/manifest.json",
            self.file_id().region_id().as_u64(),
            self.inner.entry.index_uuid
        )
    }

    /// Returns the region and file identity used for storage and deletion.
    pub(crate) fn file_id(&self) -> RegionFileId {
        self.inner.file_id
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
    purger: Option<IndexFilePurger>,
    narthex_file: Option<crate::sst::file::FileHandle>,
}

impl Drop for SeriesIndexFileHandleInner {
    fn drop(&mut self) {
        if let Some(purger) = &self.purger
            && self.deleted.load(Ordering::Acquire)
        {
            if let Some(artifact) = &self.entry.artifact {
                purger.purge_objects(
                    self.file_id,
                    artifact
                        .objects
                        .iter()
                        .map(|object| object.path.clone())
                        .collect(),
                );
            } else {
                purger.purge(PurgeRequest {
                    file_id: self.file_id,
                    objects: None,
                });
            }
        }
    }
}

/// Immutable series-index snapshot for one region.
#[derive(Debug, Default)]
pub(crate) struct SeriesIndexVersion {
    /// Range indexes for visible SSTs; reconciliation removes IDs absent from its SST snapshot.
    /// Physical deletion is independently handled by the SST file purger.
    pub(crate) samples: HashMap<FileId, crate::series_index::artifact::SamplesFileHandle>,
    pub(crate) range_indexes: HashMap<FileId, RangeIndexEntry>,
    pub(crate) series_indexes: HashMap<FileId, SeriesIndexFileHandle>,
    pub(crate) index_buckets: BTreeMap<Timestamp, IndexBucket>,
}

impl SeriesIndexVersion {
    /// Restores bucket lookup from immutable index coverage stored in the catalog.
    pub(crate) fn new(
        range_indexes: HashMap<FileId, RangeIndexEntry>,
        series_indexes: HashMap<FileId, SeriesIndexFileHandle>,
    ) -> Self {
        let mut index_buckets = BTreeMap::new();
        for handle in series_indexes.values() {
            IndexBucket::from_entry(handle.entry()).insert_into(&mut index_buckets);
        }
        Self {
            samples: Default::default(),
            range_indexes,
            series_indexes,
            index_buckets,
        }
    }

    /// Approximate installed usage; old snapshots and unpublished outputs are excluded.
    pub(crate) fn disk_usage(&self) -> u64 {
        self.samples
            .values()
            .map(|handle| handle.entry().artifact.file_size)
            .sum::<u64>()
            + self
                .range_indexes
                .values()
                .map(|entry| entry.file_size)
                .sum::<u64>()
            + self
                .series_indexes
                .values()
                .map(|handle| handle.entry().file_size)
                .sum::<u64>()
    }

    fn mark_all_deleted(&self) {
        self.samples
            .values()
            .for_each(|handle| handle.mark_deleted());
        self.series_indexes
            .values()
            .for_each(SeriesIndexFileHandle::mark_deleted);
    }
}

/// Copy-on-write series-index snapshots owned by a region.
#[derive(Debug, Default)]
pub(crate) struct SeriesIndexVersionControl {
    current: RwLock<Arc<SeriesIndexVersion>>,
    pub(crate) formats: RwLock<Arc<crate::series_index::format::SeriesFormats>>,
}

impl SeriesIndexVersionControl {
    pub(crate) fn with_format(self, format: crate::config::SeriesIndexFormat) -> Self {
        *self.formats.write().unwrap() = Arc::new(
            crate::series_index::format::SeriesFormats::from_config(format),
        );
        self
    }

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
