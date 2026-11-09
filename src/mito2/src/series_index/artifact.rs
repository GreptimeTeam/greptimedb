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

//! Immutable, potentially multi-object format outputs and reader-owned lifetimes.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use serde::{Deserialize, Serialize};
use store_api::storage::FileId;

use crate::series_index::purger::IndexFilePurger;
use crate::sst::file::{FileHandle, FileTimeRange, RegionFileId};

/// Persisted artifact identifiers. Concrete codecs are added with their implementations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum ArtifactFormat {
    #[serde(rename = "parquet")]
    Parquet,
    #[serde(rename = "columnar-series")]
    ColumnarSeries,
    #[serde(rename = "columnar-samples")]
    ColumnarSamples,
    #[cfg(test)]
    #[serde(rename = "fake-samples")]
    FakeSamples,
    #[cfg(test)]
    #[serde(rename = "fake-index")]
    FakeIndex,
}

/// One immutable object produced by a backend build.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ArtifactObject {
    pub(crate) path: String,
    pub(crate) file_size: u64,
}

/// Identifies the exact bytes produced by one backend build.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ArtifactMetadata {
    pub(crate) format: ArtifactFormat,
    pub(crate) generation: FileId,
    /// Every object owned by this build, including metadata and auxiliary files.
    pub(crate) objects: Vec<ArtifactObject>,
    pub(crate) file_size: u64,
}

/// Complete source coverage for a bounded time interval at build time.
/// New admissions must advance the sequence watermark. Unknown sequences and
/// groups omitting any intersecting SST cannot establish this contract.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct SamplesCoverage {
    pub(crate) time_range: FileTimeRange,
    pub(crate) max_sequence: u64,
    pub(crate) windows: Vec<i64>,
}

impl SamplesCoverage {
    pub(crate) fn new(
        files: &[FileHandle],
        snapshot: &[FileHandle],
        windows: Vec<i64>,
    ) -> Option<Self> {
        let first = files.first()?;
        let mut time_range = first.time_range();
        let mut max_sequence = 0;
        for file in files {
            max_sequence = max_sequence.max(file.meta_ref().sequence?.get());
            time_range.0 = time_range.0.min(file.time_range().0);
            time_range.1 = time_range.1.max(file.time_range().1);
        }
        if snapshot.iter().any(|file| {
            intersects(time_range, file.time_range())
                && !files
                    .iter()
                    .any(|source| source.file_id() == file.file_id())
        }) {
            return None;
        }
        Some(Self {
            time_range,
            max_sequence,
            windows,
        })
    }

    /// Requires unchanged bounds and watermark for the complete intersecting set.
    /// Bounds alone are not evidence that an arbitrary source subset was complete.
    fn replacement_sources(&self, files: &[FileHandle]) -> Option<HashSet<FileId>> {
        let mut sources = HashSet::new();
        let mut bounds: Option<FileTimeRange> = None;
        let mut watermark = 0;
        for file in files
            .iter()
            .filter(|file| intersects(self.time_range, file.time_range()))
        {
            let range = file.time_range();
            if range.0 < self.time_range.0 || range.1 > self.time_range.1 {
                return None;
            }
            watermark = watermark.max(file.meta_ref().sequence?.get());
            bounds = Some(match bounds {
                Some((start, end)) => (start.min(range.0), end.max(range.1)),
                None => range,
            });
            sources.insert(file.file_id().file_id());
        }
        (bounds == Some(self.time_range) && watermark == self.max_sequence).then_some(sources)
    }
}

fn intersects(left: FileTimeRange, right: FileTimeRange) -> bool {
    left.0 <= right.1 && right.0 <= left.1
}

/// One samples artifact derived from immutable Parquet SSTs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct SamplesEntry {
    pub(crate) file_id: FileId,
    pub(crate) artifact: ArtifactMetadata,
    /// Empty for legacy per-SST artifacts. Otherwise the complete source set for a UTC day.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) source_file_ids: Vec<FileId>,
    /// UTC day number since the Unix epoch; absent for legacy per-SST artifacts.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) window: Option<i64>,
    /// Complete nonempty UTC-day coverage, stored on one anchor entry per source group.
    #[serde(default, skip_serializing_if = "std::collections::HashMap::is_empty")]
    pub(crate) source_windows: std::collections::HashMap<FileId, Vec<i64>>,
    /// Optional complete-interval proof for reuse after compaction replaces SSTs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) coverage: Option<SamplesCoverage>,
}

impl SamplesEntry {
    #[cfg(test)]
    pub(crate) fn covers(&self, id: FileId) -> bool {
        if self.window.is_some() {
            self.source_file_ids.contains(&id)
        } else {
            self.file_id == id
        }
    }

    pub(crate) fn sources_visible(&self, visible: &std::collections::HashSet<FileId>) -> bool {
        if self.window.is_some() {
            !self.source_file_ids.is_empty()
                && self.source_file_ids.iter().all(|id| visible.contains(id))
        } else {
            visible.contains(&self.file_id)
        }
    }
}

/// Snapshot-local source assignments. Artifact handles and persisted identities
/// remain unchanged so existing readers continue to govern artifact retirement.
pub(crate) struct SamplesSourceAssignments {
    pub(crate) sources: HashMap<FileId, HashSet<FileId>>,
    replacements: HashSet<FileId>,
}

impl SamplesSourceAssignments {
    pub(crate) fn new(
        samples: &HashMap<FileId, SamplesFileHandle>,
        files: &[FileHandle],
        visible: &HashSet<FileId>,
        allow_replacements: bool,
    ) -> Self {
        let mut assignments = Self {
            sources: HashMap::new(),
            replacements: HashSet::new(),
        };
        for (id, handle) in samples {
            let entry = handle.entry();
            if entry.sources_visible(visible) {
                let sources = if entry.window.is_some() {
                    entry.source_file_ids.iter().copied().collect()
                } else {
                    HashSet::from([entry.file_id])
                };
                assignments.sources.insert(*id, sources);
            } else if allow_replacements
                && entry.window.is_some()
                && let Some(sources) = entry
                    .coverage
                    .as_ref()
                    .and_then(|coverage| coverage.replacement_sources(files))
            {
                assignments.sources.insert(*id, sources);
                assignments.replacements.insert(*id);
            }
        }
        assignments
    }

    pub(crate) fn covers(&self, artifact: FileId, source: FileId) -> bool {
        self.sources
            .get(&artifact)
            .is_some_and(|sources| sources.contains(&source))
    }

    pub(crate) fn covers_file(
        &self,
        samples: &HashMap<FileId, SamplesFileHandle>,
        file: &FileHandle,
    ) -> bool {
        let id = file.file_id().file_id();
        samples.iter().any(|(artifact, handle)| {
            if !self.covers(*artifact, id) {
                return false;
            }
            let entry = handle.entry();
            if entry.window.is_none() {
                return true;
            }
            let days = if self.replacements.contains(artifact) {
                entry.coverage.as_ref().map(|coverage| &coverage.windows)
            } else {
                entry.source_windows.get(&id)
            };
            days.is_some_and(|days| {
                !days.is_empty()
                    && days.iter().all(|day| {
                        samples.iter().any(|(other, h)| {
                            h.entry().window == Some(*day)
                                && self.covers(*other, id)
                                && h.entry().source_file_ids == entry.source_file_ids
                                && h.entry().coverage == entry.coverage
                        })
                    })
            })
        })
    }
}

#[derive(Debug, Clone)]
pub(crate) struct SamplesFileHandle(Arc<SamplesFileInner>);

#[derive(Debug)]
struct SamplesFileInner {
    file_id: RegionFileId,
    entry: SamplesEntry,
    retired: AtomicBool,
    purger: IndexFilePurger,
}

impl SamplesFileHandle {
    pub(crate) fn new(file_id: RegionFileId, entry: SamplesEntry, purger: IndexFilePurger) -> Self {
        Self(Arc::new(SamplesFileInner {
            file_id,
            entry,
            retired: AtomicBool::new(false),
            purger,
        }))
    }

    pub(crate) fn file_id(&self) -> RegionFileId {
        self.0.file_id
    }

    pub(crate) fn entry(&self) -> &SamplesEntry {
        &self.0.entry
    }

    pub(crate) fn mark_deleted(&self) {
        self.0.retired.store(true, Ordering::Release);
    }
}

impl Drop for SamplesFileInner {
    fn drop(&mut self) {
        if self.retired.load(Ordering::Acquire) {
            self.purger.purge_objects(
                self.file_id,
                self.entry
                    .artifact
                    .objects
                    .iter()
                    .map(|object| object.path.clone())
                    .collect(),
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn replacement_requires_complete_time_and_sequence_coverage() {
        use crate::sst::file::{FileHandle, FileMeta};
        use crate::sst::file_purger::NoopFilePurger;
        use std::num::NonZeroU64;
        let file = |start, end, sequence| {
            FileHandle::new(
                FileMeta {
                    file_id: FileId::random(),
                    time_range: (
                        common_time::Timestamp::new_millisecond(start),
                        common_time::Timestamp::new_millisecond(end),
                    ),
                    sequence: NonZeroU64::new(sequence),
                    ..Default::default()
                },
                Arc::new(NoopFilePurger),
            )
        };
        let original = vec![file(0, 100, 10), file(50, 200, 20)];
        let coverage = SamplesCoverage::new(&original, &original, vec![0]).unwrap();
        let replacement = file(0, 200, 20);
        assert_eq!(
            coverage.replacement_sources(std::slice::from_ref(&replacement)),
            Some(HashSet::from([replacement.file_id().file_id()]))
        );
        for invalid in [
            file(0, 200, 21),
            file(0, 200, 19),
            file(0, 200, 0),
            file(-1, 200, 20),
            file(0, 201, 20),
            file(1, 200, 20),
            file(0, 199, 20),
        ] {
            assert!(coverage.replacement_sources(&[invalid]).is_none());
        }
        assert!(SamplesCoverage::new(&original[..1], &original, vec![0]).is_none());
        assert!(SamplesCoverage::new(&[file(0, 200, 0)], &[], vec![0]).is_none());
    }

    #[test]
    fn frozen_sample_catalog_coverage() {
        let legacy: SamplesEntry = serde_json::from_str(include_str!(
            "../../../../tests/compatibility/wire/mito_columnar_samples/legacy-catalog-entry.json"
        ))
        .unwrap();
        assert!(legacy.coverage.is_none());
        let covered: SamplesEntry = serde_json::from_str(include_str!(
            "../../../../tests/compatibility/wire/mito_columnar_samples/covered-daily-catalog-entry.json"
        )).unwrap();
        assert_eq!(covered.coverage.as_ref().unwrap().max_sequence, 20);
        assert_eq!(covered.coverage.as_ref().unwrap().windows, vec![0, 3]);
        assert_eq!(
            covered,
            serde_json::from_str(&serde_json::to_string(&covered).unwrap()).unwrap()
        );
        assert!(legacy.window.is_none());
        assert!(legacy.source_file_ids.is_empty());
        assert!(legacy.covers(legacy.file_id));
        let daily: SamplesEntry = serde_json::from_str(include_str!(
            "../../../../tests/compatibility/wire/mito_columnar_samples/daily-catalog-entry.json"
        ))
        .unwrap();
        assert!(daily.source_windows.is_empty());
        let sparse: SamplesEntry = serde_json::from_str(include_str!(
            "../../../../tests/compatibility/wire/mito_columnar_samples/sparse-daily-catalog-entry.json"
        )).unwrap();
        assert_eq!(sparse.source_windows[&legacy.file_id], vec![0, 3]);
        assert_eq!(
            sparse,
            serde_json::from_str(&serde_json::to_string(&sparse).unwrap()).unwrap()
        );
        let mut visible: HashSet<_> = daily.source_file_ids.iter().copied().collect();
        assert!(daily.sources_visible(&visible));
        assert!(daily.covers(legacy.file_id));
        assert!(!daily.covers(daily.file_id));
        visible.remove(&legacy.file_id);
        assert!(!daily.sources_visible(&visible));
        assert_eq!(
            daily,
            serde_json::from_str(&serde_json::to_string(&daily).unwrap()).unwrap()
        );
        assert!(
            !serde_json::to_string(&legacy)
                .unwrap()
                .contains("source_file_ids")
        );
    }
}
