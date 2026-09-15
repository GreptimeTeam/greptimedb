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

use std::collections::HashMap;
use std::ops::Range;

use common_time::Timestamp;
use store_api::metadata::RegionMetadata;

use crate::compaction::run::primary_key_ranges_overlap;
use crate::sst::file::{FileHandle, RegionFileId};

/// Snapshot-local overlap candidates, ordered by start time. Each subtree stores
/// its maximum active end. Removing visited files prunes dense internal overlaps.
/// PK-disjoint files with overlapping times can still cost O(N) per query, so a
/// complete closure has O(N²) worst-case time and O(N) auxiliary space.
pub(super) struct FileOverlapIndex<'a> {
    metadata: &'a RegionMetadata,
    files: Vec<FileHandle>,
    by_start: Vec<usize>,
    positions: HashMap<RegionFileId, usize>,
    max_ends: Vec<Option<Timestamp>>,
    leaf_base: usize,
}

struct OverlapQuery<'a> {
    input: &'a FileHandle,
    min_end: Timestamp,
    candidates_end: usize,
}

impl<'a> FileOverlapIndex<'a> {
    pub(super) fn new(files: Vec<FileHandle>, metadata: &'a RegionMetadata) -> Self {
        let mut by_start: Vec<_> = (0..files.len()).collect();
        by_start.sort_unstable_by_key(|&i| (files[i].time_range().0, i));
        // A single empty leaf also handles an empty snapshot.
        let leaf_base = files.len().next_power_of_two();
        let mut max_ends = vec![None; 2 * leaf_base];
        let mut positions = HashMap::with_capacity(files.len());
        for (offset, &i) in by_start.iter().enumerate() {
            max_ends[leaf_base + offset] = Some(files[i].time_range().1);
            positions.insert(files[i].file_id(), leaf_base + offset);
        }
        for node in (1..leaf_base).rev() {
            max_ends[node] = max_ends[2 * node].max(max_ends[2 * node + 1]);
        }
        Self {
            metadata,
            files,
            by_start,
            positions,
            max_ends,
            leaf_base,
        }
    }

    pub(super) fn remove(&mut self, file_id: RegionFileId) {
        let Some(mut node) = self.positions.remove(&file_id) else {
            return;
        };
        self.max_ends[node] = None;
        while node > 1 {
            node /= 2;
            self.max_ends[node] = self.max_ends[2 * node].max(self.max_ends[2 * node + 1]);
        }
    }

    /// Removes dependencies so expansion never enumerates visited files.
    /// Preserve snapshot order rather than exposing the index's time ordering
    /// to the merge reader (including its handling of equal-sequence rows).
    pub(super) fn drain_overlaps(&mut self, input: &FileHandle) -> Vec<FileHandle> {
        let mut matches = Vec::new();
        while let Some(i) = self.find_overlap(input) {
            self.remove(self.files[i].file_id());
            matches.push(i);
        }
        matches.sort_unstable();
        matches.into_iter().map(|i| self.files[i].clone()).collect()
    }

    fn find_overlap(&self, input: &FileHandle) -> Option<usize> {
        let (start, end) = input.time_range();
        let query = OverlapQuery {
            input,
            min_end: start,
            candidates_end: self
                .by_start
                .partition_point(|&i| self.files[i].time_range().0 <= end),
        };
        self.find_in_subtree(1, 0..self.leaf_base, &query)
    }

    fn find_in_subtree(
        &self,
        node: usize,
        leaves: Range<usize>,
        query: &OverlapQuery<'_>,
    ) -> Option<usize> {
        if leaves.start >= query.candidates_end
            || self.max_ends[node].is_none_or(|end| end < query.min_end)
        {
            return None;
        }
        if leaves.len() == 1 {
            let i = self.by_start[leaves.start];
            return files_may_overlap(query.input, &self.files[i], self.metadata).then_some(i);
        }
        let mid = leaves.start + leaves.len() / 2;
        self.find_in_subtree(2 * node, leaves.start..mid, query)
            .or_else(|| self.find_in_subtree(2 * node + 1, mid..leaves.end, query))
    }
}

/// SST bounds are inclusive. Missing statistics, foreign encodings and schema
/// evolution must not exclude a possible logical-key dependency.
fn files_may_overlap(lhs: &FileHandle, rhs: &FileHandle, metadata: &RegionMetadata) -> bool {
    let (lhs_start, lhs_end) = lhs.time_range();
    let (rhs_start, rhs_end) = rhs.time_range();
    if lhs_start.max(rhs_start) > lhs_end.min(rhs_end) {
        return false;
    }

    // FileMeta does not record the PK schema/encoding. Adding a tag can change
    // encoded keys without changing the logical keys of old rows.
    if metadata.schema_version != 0
        || lhs.region_id() != metadata.region_id
        || rhs.region_id() != metadata.region_id
    {
        return true;
    }
    match (lhs.primary_key_range(), rhs.primary_key_range()) {
        (Some(lhs), Some(rhs)) if lhs.0 <= lhs.1 && rhs.0 <= rhs.1 => {
            primary_key_ranges_overlap(&lhs, &rhs)
        }
        _ => true,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use bytes::Bytes;
    use rand::{Rng, SeedableRng};
    use store_api::storage::FileId;

    use super::*;
    use crate::sst::file::FileMeta;
    use crate::test_util::memtable_util::metadata_for_test;
    use crate::test_util::new_noop_file_purger;

    fn file(start: i64, end: i64, pk: Option<(&str, &str)>) -> FileHandle {
        FileHandle::new_with_primary_key_range(
            FileMeta {
                file_id: FileId::random(),
                time_range: (
                    Timestamp::new_millisecond(start),
                    Timestamp::new_millisecond(end),
                ),
                ..Default::default()
            },
            new_noop_file_purger(),
            pk.map(|(start, end)| {
                (
                    Bytes::copy_from_slice(start.as_bytes()),
                    Bytes::copy_from_slice(end.as_bytes()),
                )
            }),
        )
    }

    #[rstest::rstest]
    #[case(10, Some(("b", "c")), 0, false, true)]
    #[case(11, Some(("b", "c")), 0, false, false)]
    #[case(10, Some(("c", "d")), 0, false, false)]
    #[case(10, None, 0, false, true)]
    #[case(10, Some(("z", "a")), 0, false, true)]
    #[case(10, Some(("c", "d")), 1, false, true)]
    #[case(10, Some(("c", "d")), 0, true, true)]
    #[case(11, None, 1, true, false)]
    fn test_conservative_overlap(
        #[case] start: i64,
        #[case] pk: Option<(&str, &str)>,
        #[case] schema_version: u64,
        #[case] foreign: bool,
        #[case] expected: bool,
    ) {
        let mut metadata = (*metadata_for_test()).clone();
        metadata.region_id = 0.into();
        metadata.schema_version = schema_version;
        let lhs = file(0, 10, Some(("a", "b")));
        let mut rhs = file(start, start + 10, pk);
        if foreign {
            let mut meta = rhs.meta_ref().clone();
            meta.region_id = 1.into();
            rhs = FileHandle::new_with_primary_key_range(
                meta,
                new_noop_file_purger(),
                rhs.primary_key_range(),
            );
        }
        assert_eq!(expected, files_may_overlap(&lhs, &rhs, &metadata));
        assert_eq!(expected, files_may_overlap(&rhs, &lhs, &metadata));
    }

    #[test]
    fn test_index_removal_distinguishes_region_owners() {
        let metadata = metadata_for_test();
        let lhs = file(0, 10, None);
        let mut meta = lhs.meta_ref().clone();
        meta.region_id = 1.into();
        let rhs = FileHandle::new(meta, new_noop_file_purger());
        let mut index = FileOverlapIndex::new(vec![lhs.clone(), rhs.clone()], &metadata);
        index.remove(lhs.file_id());
        let overlaps = index.drain_overlaps(&lhs);
        assert_eq!(1, overlaps.len());
        assert_eq!(rhs.file_id(), overlaps[0].file_id());
        assert!(index.drain_overlaps(&lhs).is_empty());
    }

    #[test]
    fn test_index_matches_linear_scan_after_removals() {
        let mut metadata = (*metadata_for_test()).clone();
        metadata.region_id = 0.into();
        let mut rng = rand::rngs::StdRng::seed_from_u64(9146);
        for count in [0, 1, 7, 32, 127] {
            let files: Vec<_> = (0..count)
                .map(|_| {
                    let start = rng.random_range(-50..50);
                    let end = start + rng.random_range(0..40);
                    let pk = match rng.random_range(0..4) {
                        0 => None,
                        1 => Some(("a", "b")),
                        2 => Some(("b", "c")),
                        _ => Some(("z", "z")),
                    };
                    file(start, end, pk)
                })
                .collect();
            let mut index = FileOverlapIndex::new(files.clone(), &metadata);
            let mut removed = HashSet::new();
            for f in files.iter().step_by(3) {
                index.remove(f.file_id());
                removed.insert(f.file_id());
            }
            for _ in 0..32 {
                let start = rng.random_range(-60..60);
                let query = file(start, start + rng.random_range(0..25), Some(("b", "c")));
                let expected: Vec<_> = files
                    .iter()
                    .filter(|f| {
                        !removed.contains(&f.file_id()) && files_may_overlap(&query, f, &metadata)
                    })
                    .map(FileHandle::file_id)
                    .collect();
                let actual: Vec<_> = index
                    .drain_overlaps(&query)
                    .iter()
                    .map(FileHandle::file_id)
                    .collect();
                assert_eq!(expected, actual, "snapshot size {count}");
                removed.extend(expected);
            }
        }
    }
}
