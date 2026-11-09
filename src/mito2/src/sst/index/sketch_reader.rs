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

//! Injected statistics reads over a pinned selection of SST files.

use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use common_error::ext::BoxedError;
use common_time::Timestamp;
use store_api::metadata::RegionMetadataRef;
use store_api::sketch::{SketchRequest, SketchResponse, SketchTimeRange};

use crate::sst::file::FileHandle;
use crate::sst::index::puffin_manager::SstPuffinManager;
use crate::sst::version::SstVersionRef;

/// Lazy candidates from one immutable SST snapshot. Construction does not enumerate files.
/// Retaining this view pins the snapshot, including files outside the requested range.
pub struct SketchReadFiles {
    snapshot: SstVersionRef,
    time_range: SketchTimeRange,
}

impl SketchReadFiles {
    pub(crate) fn new(snapshot: SstVersionRef, time_range: SketchTimeRange) -> Self {
        Self {
            snapshot,
            time_range,
        }
    }

    /// Visits intersecting files without cloning handles or allocating a selection.
    /// Iteration scans snapshot metadata; it performs no file I/O and has no order guarantee.
    pub fn iter(&self) -> impl Iterator<Item = &FileHandle> {
        let start = Timestamp::new_millisecond(self.time_range.start_ms);
        let end = Timestamp::new_millisecond(self.time_range.end_ms);
        self.snapshot
            .levels()
            .iter()
            .flat_map(|level| level.files())
            .filter(move |file| {
                file.meta_ref().time_range.0 < end && file.meta_ref().time_range.1 >= start
            })
    }
}

/// Borrowed inputs from one snapshot, enumerated only when the handler consumes them.
pub struct SketchReadContext<'a> {
    /// Schema captured with the selected file snapshot.
    pub metadata: &'a RegionMetadataRef,
    /// Lazy time-filtered candidates; consumers validate requests before iterating.
    pub files: &'a SketchReadFiles,
    /// Existing Puffin I/O and metadata cache facilities for these files.
    pub puffin_manager: &'a SstPuffinManager,
}

/// Node-scoped implementation owns caches; the engine owns file selection and lifetimes.
#[async_trait]
pub trait SketchReadHandler: Send + Sync + Debug {
    /// Reads coverage directories without decoding algorithm payloads. Missing files are gaps, not empty data.
    async fn stat(
        &self,
        context: &SketchReadContext<'_>,
        request: &SketchRequest,
    ) -> Result<SketchResponse, BoxedError>;

    /// Reads and merges complete buckets, preserving coverage gaps alongside estimates.
    /// Invalid or corrupt payloads return errors rather than being silently excluded.
    async fn query(
        &self,
        context: &SketchReadContext<'_>,
        request: &SketchRequest,
    ) -> Result<SketchResponse, BoxedError>;
}

/// Plugin registration type shared by engine bootstrap and the read entry point.
pub type SketchReadHandlerRef = Arc<dyn SketchReadHandler>;

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use common_time::Timestamp;
    use store_api::sketch::SketchTimeRange;
    use store_api::storage::FileId;

    use super::SketchReadFiles;
    use crate::sst::file::FileMeta;
    use crate::sst::file_purger::FilePurger;
    use crate::sst::version::SstVersion;
    use crate::test_util::memtable_util::metadata_for_test;
    use crate::test_util::new_noop_file_purger;

    fn file(start: Timestamp, end: Timestamp) -> FileMeta {
        FileMeta {
            file_id: FileId::random(),
            time_range: (start, end),
            ..Default::default()
        }
    }

    #[test]
    fn test_sketch_files_time_boundaries() {
        let millis = Timestamp::new_millisecond;
        let cases = [
            (millis(-1), millis(999), false),
            (millis(999), millis(1000), true),
            (millis(1000), millis(1000), true),
            (millis(1999), millis(2000), true),
            (millis(2000), millis(2001), false),
            (Timestamp::new_second(1), Timestamp::new_second(1), true),
            (
                Timestamp::new_microsecond(1_999_999),
                Timestamp::new_microsecond(2_000_000),
                true,
            ),
            (
                Timestamp::new_nanosecond(2_000_000_000),
                Timestamp::new_nanosecond(2_000_000_001),
                false,
            ),
        ];
        let mut version = SstVersion::new(metadata_for_test());
        let mut expected = Vec::new();
        version.add_files(
            new_noop_file_purger(),
            cases
                .into_iter()
                .enumerate()
                .map(|(index, (start, end, included))| {
                    let mut meta = file(start, end);
                    meta.level = (index % 2) as u8;
                    if included {
                        expected.push(meta.file_id);
                    }
                    meta
                }),
        );
        let selection = SketchReadFiles::new(
            Arc::new(version),
            SketchTimeRange {
                start_ms: 1000,
                end_ms: 2000,
            },
        );
        let mut actual: Vec<_> = selection
            .iter()
            .map(|handle| handle.file_id().file_id())
            .collect();
        actual.sort_unstable_by(|a, b| a.as_bytes().cmp(b.as_bytes()));
        expected.sort_unstable_by(|a, b| a.as_bytes().cmp(b.as_bytes()));
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_sketch_files_borrowed_selection_matches_eager_filter() {
        let mut version = SstVersion::new(metadata_for_test());
        version.add_files(
            new_noop_file_purger(),
            (0..16_384).map(|index| {
                let start = Timestamp::new_millisecond(index - 8192);
                let end = Timestamp::new_millisecond(index - 8192 + 100);
                let mut meta = file(start, end);
                meta.level = (index % 2) as u8;
                meta
            }),
        );
        let snapshot = Arc::new(version);
        for (start_ms, end_ms) in [(-10_000, -9_000), (-100, 100), (0, 20_000)] {
            let range = SketchTimeRange { start_ms, end_ms };
            let expected: Vec<_> = snapshot
                .levels()
                .iter()
                .flat_map(|level| level.files())
                .filter(|handle| {
                    let (start, end) = handle.meta_ref().time_range;
                    start < Timestamp::new_millisecond(end_ms)
                        && end >= Timestamp::new_millisecond(start_ms)
                })
                .collect();
            let selection = SketchReadFiles::new(snapshot.clone(), range);
            let first: Vec<_> = selection.iter().take(1).collect();
            assert_eq!(first.len(), expected.len().min(1));
            if let Some(first) = first.first() {
                assert!(std::ptr::eq(*first, expected[0]));
            }
            assert_eq!(
                selection
                    .iter()
                    .map(|handle| handle.file_id())
                    .collect::<Vec<_>>(),
                expected
                    .iter()
                    .map(|handle| handle.file_id())
                    .collect::<Vec<_>>(),
            );
        }
    }

    #[derive(Debug, Default)]
    struct RecordingPurger(Mutex<Vec<FileId>>);

    impl FilePurger for RecordingPurger {
        fn purge_file(&self, meta: FileMeta) {
            self.0.lock().unwrap().push(meta.file_id);
        }
    }

    #[test]
    fn test_sketch_files_pin_retired_snapshot_until_drop() {
        let old = file(
            Timestamp::new_millisecond(10),
            Timestamp::new_millisecond(20),
        );
        let outside = file(
            Timestamp::new_millisecond(100),
            Timestamp::new_millisecond(200),
        );
        let replacement = file(
            Timestamp::new_millisecond(15),
            Timestamp::new_millisecond(25),
        );
        let purger = Arc::new(RecordingPurger::default());
        let mut version = SstVersion::new(metadata_for_test());
        version.add_files(purger.clone(), [old.clone(), outside.clone()].into_iter());
        let mut latest = Arc::new(version);
        let range = SketchTimeRange {
            start_ms: 0,
            end_ms: 30,
        };
        let selected = SketchReadFiles::new(latest.clone(), range);

        Arc::make_mut(&mut latest).remove_files([old.clone(), outside.clone()].into_iter());
        Arc::make_mut(&mut latest).add_files(purger.clone(), [replacement.clone()].into_iter());

        assert!(purger.0.lock().unwrap().is_empty());
        let old_handles: Vec<_> = selected.iter().collect();
        assert_eq!(old_handles.len(), 1);
        assert_eq!(old_handles[0].file_id().file_id(), old.file_id);
        assert!(old_handles[0].is_deleted());
        let current = SketchReadFiles::new(latest, range);
        assert_eq!(
            current
                .iter()
                .map(|handle| handle.file_id().file_id())
                .collect::<Vec<_>>(),
            vec![replacement.file_id],
        );
        drop(old_handles);
        drop(selected);

        // The lazy view pins the complete snapshot, including the out-of-range file.
        let mut purged = purger.0.lock().unwrap().clone();
        let mut expected = vec![old.file_id, outside.file_id];
        purged.sort_unstable_by(|a, b| a.as_bytes().cmp(b.as_bytes()));
        expected.sort_unstable_by(|a, b| a.as_bytes().cmp(b.as_bytes()));
        assert_eq!(purged, expected);
    }
}
