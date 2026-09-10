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

//! Event-time bucket planning and source coverage.

use std::collections::BTreeMap;
use std::time::Duration;

use common_time::{TimeToLive, Timestamp};
use smallvec::{SmallVec, smallvec};
use store_api::storage::FileId;

use crate::series_index::catalog::SeriesIndexEntry;
use crate::sst::file::FileHandle;

const SERIES_INDEX_TRIGGER_FILES: usize = 4;
/// Bound expansion of a single SST range when compaction windows become smaller.
/// Merged buckets may contain more entries.
const MAX_FILE_WINDOW_SEQUENCES: usize = 32;

/// Index files sharing a non-overlapping, half-open time bucket.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct IndexBucket {
    pub(crate) start: Timestamp,
    pub(crate) end: Timestamp,
    pub(crate) index_ids: SmallVec<[FileId; 2]>,
    /// Zero means that merged indexes use incompatible window widths.
    pub(crate) compaction_window_secs: i64,
    /// Indexed SST sequence maxima per compaction window. Merging takes maxima only
    /// for matching windows. See [`SeriesIndexEntry::window_sequences`] for the layout.
    pub(crate) window_sequences: BTreeMap<i64, u64>,
}

impl IndexBucket {
    pub(crate) fn new(start: Timestamp, end: Timestamp) -> Self {
        Self {
            start,
            end,
            index_ids: SmallVec::new(),
            compaction_window_secs: 0,
            window_sequences: BTreeMap::new(),
        }
    }

    pub(crate) fn from_entry(entry: &SeriesIndexEntry) -> Self {
        Self {
            start: entry.bucket_start,
            end: entry.bucket_end,
            index_ids: smallvec![entry.index_uuid],
            compaction_window_secs: entry.compaction_window_secs,
            window_sequences: entry.window_sequences.clone(),
        }
    }

    fn merge(&mut self, mut other: Self) {
        self.start = self.start.min(other.start);
        self.end = self.end.max(other.end);
        if self.index_ids.is_empty() {
            self.compaction_window_secs = other.compaction_window_secs;
            self.window_sequences = std::mem::take(&mut other.window_sequences);
        } else if !other.index_ids.is_empty() {
            if self.compaction_window_secs == other.compaction_window_secs {
                merge_window_sequences(&mut self.window_sequences, other.window_sequences);
            } else {
                // Do not compare maps with different window boundaries, including
                // a previous incompatible merge, against current SST coverage.
                self.compaction_window_secs = 0;
                self.window_sequences.clear();
            }
        }
        self.index_ids.append(&mut other.index_ids);
    }

    /// Inserts a bucket, consuming overlapping entries and expanding their interval.
    /// Each map key equals the stored bucket start; adjacent intervals remain separate.
    pub(crate) fn insert_into(mut self, buckets: &mut BTreeMap<Timestamp, Self>) {
        if let Some((&previous_start, previous)) = buckets.range(..self.start).next_back()
            && previous.end > self.start
        {
            self.start = previous_start;
        }
        // Expanding the end may expose more overlaps, so look up the next entry again.
        while let Some((&next_start, _)) = buckets.range(self.start..self.end).next() {
            if let Some(other) = buckets.remove(&next_start) {
                self.merge(other);
            }
        }
        buckets.insert(self.start, self);
    }
}

/// SSTs grouped into a half-open time interval for an aggregate series-index build.
///
/// Reconciliation may expand the interval through existing index coverage. A changed
/// bucket is rebuilt from all its SSTs. Unknown source sequences prevent builds,
/// since their coverage cannot be determined safely.
#[derive(Debug, Clone)]
pub(crate) struct SeriesBucket {
    pub(crate) start: Timestamp,
    pub(crate) end: Timestamp,
    pub(crate) files: SmallVec<[FileHandle; 2]>,
    pub(crate) has_unknown_sequence: bool,
    /// Maximum known source sequence, or zero when all sequences are unknown.
    pub(crate) max_file_sequence: u64,
    pub(crate) compaction_window_secs: i64,
    /// Current SST sequence maxima per window, compared with indexed coverage.
    /// Unknown sequences contribute zero and prevent building an index.
    /// Empty when a source SST exceeds the per-file limit; reuse cannot be established.
    pub(crate) window_sequences: BTreeMap<i64, u64>,
}

/// Next bucket coverage and the work required to publish it, computed without I/O.
pub(crate) struct SeriesIndexPlan {
    pub(crate) index_buckets: BTreeMap<Timestamp, IndexBucket>,
    pub(crate) builds: Vec<(SeriesBucket, SeriesIndexEntry)>,
    pub(crate) expired_index_ids: Vec<FileId>,
    /// Retire only after replacement builds and catalog publication succeed.
    pub(crate) superseded_index_ids: Vec<FileId>,
    pub(crate) computed_buckets: usize,
    pub(crate) skipped_buckets: usize,
}

/// Plans whole-bucket replacements when per-window coverage changes. The returned bucket
/// map is publishable only after every planned build and the catalog writes succeed.
pub(crate) fn plan_series_indexes(
    buckets: Vec<SeriesBucket>,
    mut index_buckets: BTreeMap<Timestamp, IndexBucket>,
    ttl: Option<TimeToLive>,
    now_ms: i64,
) -> SeriesIndexPlan {
    // Reconciliation changes geometry, not established coverage. In particular,
    // a deferred bridge must not change the indexed snapshot.
    let buckets = reconcile_series_buckets(buckets, &mut index_buckets.clone());
    let computed_buckets = buckets.len();
    let expired = |end| {
        ttl.is_some_and(|ttl| {
            ttl.is_expired(&end, &Timestamp::new_millisecond(now_ms))
                .unwrap_or(false)
        })
    };
    let mut expired_index_ids = Vec::new();
    index_buckets.retain(|_, bucket| {
        if expired(bucket.end) {
            expired_index_ids.extend_from_slice(&bucket.index_ids);
            false
        } else {
            true
        }
    });
    let mut builds = Vec::new();
    let mut superseded_index_ids = Vec::new();
    for bucket in buckets {
        if expired(bucket.end) {
            continue;
        }
        if bucket.has_unknown_sequence {
            continue;
        }
        if index_buckets.get(&bucket.start).is_some_and(|indexed| {
            indexed.end == bucket.end
                && indexed.compaction_window_secs == bucket.compaction_window_secs
                // Missing coverage cannot establish reuse, even when both maps are empty.
                && !bucket.window_sequences.is_empty()
                && indexed.window_sequences == bucket.window_sequences
        }) {
            continue;
        }
        if let Some(entry) = bucket.to_series_entry() {
            index_buckets.retain(|_, indexed| {
                if indexed.start < bucket.end && bucket.start < indexed.end {
                    superseded_index_ids.extend_from_slice(&indexed.index_ids);
                    false
                } else {
                    true
                }
            });
            IndexBucket::from_entry(&entry).insert_into(&mut index_buckets);
            builds.push((bucket, entry));
        }
    }
    index_buckets.retain(|_, bucket| !bucket.index_ids.is_empty());
    SeriesIndexPlan {
        index_buckets,
        skipped_buckets: computed_buckets - builds.len(),
        builds,
        expired_index_ids,
        superseded_index_ids,
        computed_buckets,
    }
}

pub(crate) fn rounded_bucket_width(
    requested: Duration,
    compaction_window: Duration,
) -> Option<i64> {
    let window_secs = i64::try_from(compaction_window.as_secs()).ok()?.max(1);
    let requested_secs = i64::try_from(requested.as_secs())
        .unwrap_or(i64::MAX)
        .max(1);
    let multiples = requested_secs / window_secs + i64::from(requested_secs % window_secs != 0);
    multiples.checked_mul(window_secs)
}

/// Groups SSTs across levels into sorted, disjoint time buckets without planning builds.
///
/// Both widths must be positive, and `width_secs` must be a multiple of the compaction
/// window width. Inclusive SST ranges are rounded outward to aligned,
/// half-open intervals in seconds. Overlapping intervals merge; adjacent ones stay
/// separate. Each merged bucket tracks its maximum sequence and any unknown sequence.
pub(crate) fn group_files_into_series_buckets(
    files: &[FileHandle],
    width_secs: i64,
    compaction_window_secs: i64,
) -> Vec<SeriesBucket> {
    let mut spans = files
        .iter()
        .map(|file| {
            let start = file.time_range().0.split().0;
            let end = file.time_range().1.split().0;
            let sequence = file
                .meta_ref()
                .sequence
                .map_or(0, |sequence| sequence.get());
            let first_window = start.div_euclid(compaction_window_secs);
            let last_window = end.div_euclid(compaction_window_secs);
            let window_count = i128::from(last_window) - i128::from(first_window) + 1;
            let window_sequences = if window_count > MAX_FILE_WINDOW_SEQUENCES as i128 {
                BTreeMap::new()
            } else {
                (first_window..=last_window)
                    .map(|window| (window.saturating_mul(compaction_window_secs), sequence))
                    .collect()
            };
            SeriesBucket {
                start: Timestamp::new_second(
                    start.div_euclid(width_secs).saturating_mul(width_secs),
                ),
                end: Timestamp::new_second(
                    end.div_euclid(width_secs)
                        .saturating_add(1)
                        .saturating_mul(width_secs),
                ),
                files: smallvec![file.clone()],
                has_unknown_sequence: file.meta_ref().sequence.is_none(),
                max_file_sequence: sequence,
                compaction_window_secs,
                window_sequences,
            }
        })
        .collect::<Vec<_>>();
    spans.sort_unstable_by(|a, b| a.start.cmp(&b.start).then_with(|| b.end.cmp(&a.end)));
    group_series_buckets(spans)
}

/// Expands SST buckets through existing index coverage before grouping build inputs.
fn reconcile_series_buckets(
    mut buckets: Vec<SeriesBucket>,
    index_buckets: &mut BTreeMap<Timestamp, IndexBucket>,
) -> Vec<SeriesBucket> {
    for bucket in &buckets {
        IndexBucket::new(bucket.start, bucket.end).insert_into(index_buckets);
    }
    for bucket in &mut buckets {
        if let Some((&start, index_bucket)) = index_buckets.range(..=bucket.start).next_back() {
            bucket.start = start;
            bucket.end = index_bucket.end;
        }
    }
    // Expanding sorted, disjoint spans preserves their order, but may join several of them.
    group_series_buckets(buckets)
}

fn group_series_buckets(spans: Vec<SeriesBucket>) -> Vec<SeriesBucket> {
    let mut buckets: Vec<SeriesBucket> = Vec::new();
    for mut span in spans {
        if let Some(last) = buckets.last_mut()
            && span.start < last.end
        {
            last.end = last.end.max(span.end);
            last.files.append(&mut span.files);
            last.has_unknown_sequence |= span.has_unknown_sequence;
            last.max_file_sequence = last.max_file_sequence.max(span.max_file_sequence);
            merge_window_sequences(&mut last.window_sequences, span.window_sequences);
        } else {
            buckets.push(span);
        }
    }
    buckets
}

fn merge_window_sequences(target: &mut BTreeMap<i64, u64>, source: BTreeMap<i64, u64>) {
    // An empty map denotes omitted coverage, including after merging oversized spans.
    if target.is_empty() || source.is_empty() {
        target.clear();
        return;
    }
    for (window, sequence) in source {
        target
            .entry(window)
            .and_modify(|max| *max = (*max).max(sequence))
            .or_insert(sequence);
    }
}

impl SeriesBucket {
    /// Creates entry metadata with a fresh UUID and sorted source file IDs.
    /// Returns `None` for unknown sequences or too few files.
    fn to_series_entry(&self) -> Option<SeriesIndexEntry> {
        if self.has_unknown_sequence || self.files.len() < SERIES_INDEX_TRIGGER_FILES {
            return None;
        }
        let mut source_file_ids = self
            .files
            .iter()
            .map(|file| file.file_id().file_id())
            .collect::<Vec<_>>();
        source_file_ids.sort_unstable_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
        let min_file_sequence = self
            .files
            .iter()
            .filter_map(|file| file.meta_ref().sequence.map(|sequence| sequence.get()))
            .min()?;
        Some(SeriesIndexEntry {
            index_uuid: FileId::random(),
            bucket_start: self.start,
            bucket_end: self.end,
            source_file_ids,
            min_file_sequence,
            max_file_sequence: self.max_file_sequence,
            compaction_window_secs: self.compaction_window_secs,
            window_sequences: self.window_sequences.clone(),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::num::NonZeroU64;

    use common_time::timestamp::TimeUnit;

    use super::*;
    use crate::sst::file::FileMeta;
    use crate::test_util::new_noop_file_purger;

    fn file(sequence: Option<u64>, level: u8, start: Timestamp, end: Timestamp) -> FileHandle {
        FileHandle::new(
            FileMeta {
                file_id: FileId::random(),
                sequence: sequence.and_then(NonZeroU64::new),
                level,
                time_range: (start, end),
                ..Default::default()
            },
            new_noop_file_purger(),
        )
    }

    #[test]
    fn test_second_resolution_buckets_merge_spans_across_levels() {
        let width = rounded_bucket_width(Duration::from_secs(11), Duration::from_secs(10)).unwrap();
        let files = [
            file(
                Some(1),
                0,
                Timestamp::new_millisecond(-1),
                Timestamp::new_millisecond(19999),
            ),
            file(
                Some(2),
                1,
                Timestamp::new_microsecond(19000000),
                Timestamp::new_microsecond(39000000),
            ),
            file(
                Some(3),
                2,
                Timestamp::new_nanosecond(39000000000),
                Timestamp::new_nanosecond(40000000000),
            ),
            file(
                Some(4),
                1,
                Timestamp::new_second(60),
                Timestamp::new_second(61),
            ),
        ];
        let buckets = group_files_into_series_buckets(&files, width, 10);
        let spans = buckets
            .iter()
            .map(|b| (b.start, b.end, b.files.len()))
            .collect::<Vec<_>>();
        assert_eq!(
            vec![
                (Timestamp::new_second(-20), Timestamp::new_second(60), 3),
                (Timestamp::new_second(60), Timestamp::new_second(80), 1),
            ],
            spans
        );
        assert_eq!(3, buckets[0].max_file_sequence);
        assert_eq!(4, buckets[1].max_file_sequence);
        assert!(buckets[0].to_series_entry().is_none());
        assert!(buckets[1].to_series_entry().is_none());
        let mut files = files.to_vec();
        files.push(file(
            None,
            0,
            Timestamp::new_second(0),
            Timestamp::new_second(1),
        ));
        assert!(
            group_files_into_series_buckets(&files, width, 10)[0]
                .to_series_entry()
                .is_none()
        );
    }

    #[test]
    fn test_seconds_do_not_require_millisecond_conversion() {
        let start = Timestamp::new_second(i64::MAX / 1000 + 100);
        assert!(start.convert_to(TimeUnit::Millisecond).is_none());
        let buckets = group_files_into_series_buckets(&[file(Some(1), 0, start, start)], 1, 1);
        assert_eq!(start, buckets[0].start);
        assert_eq!(Timestamp::new_second(start.value() + 1), buckets[0].end);

        let width = rounded_bucket_width(Duration::ZERO, Duration::from_millis(100)).unwrap();
        let buckets = group_files_into_series_buckets(
            &[file(
                Some(1),
                0,
                Timestamp::new_nanosecond(-1),
                Timestamp::new_microsecond(1),
            )],
            width,
            1,
        );
        assert_eq!(
            (Timestamp::new_second(-1), Timestamp::new_second(1)),
            (buckets[0].start, buckets[0].end)
        );
    }

    #[test]
    fn test_reconcile_bridge_expands_through_index_and_sst_buckets() {
        let ts = Timestamp::new_second;
        let mut indexes = BTreeMap::new();
        let ids = [FileId::random(), FileId::random(), FileId::random()];
        for (start, end, max_sequence, id) in [
            (0, 20, 10, ids[0]),
            (30, 60, 30, ids[1]),
            (70, 100, 20, ids[2]),
        ] {
            IndexBucket {
                start: ts(start),
                end: ts(end),
                index_ids: smallvec![id],
                compaction_window_secs: 10,
                window_sequences: (start..end)
                    .step_by(10)
                    .map(|w| (w, max_sequence))
                    .collect(),
            }
            .insert_into(&mut indexes);
        }
        let files = [
            file(Some(15), 1, ts(10), ts(30)),
            file(Some(31), 0, ts(50), ts(70)),
            file(Some(32), 0, ts(90), ts(95)),
            file(Some(32), 1, ts(90), ts(95)),
            file(Some(33), 0, ts(90), ts(95)),
            file(Some(34), 0, ts(100), ts(105)),
        ];
        let planned = group_files_into_series_buckets(&files, 10, 10);
        assert_eq!(4, planned.len());
        let plan = plan_series_indexes(planned, indexes, None, 0);
        assert_eq!((2, 1), (plan.computed_buckets, plan.skipped_buckets));
        let [(bucket, entry)] = plan.builds.as_slice() else {
            panic!("expected one replacement build");
        };
        assert_eq!((ts(0), ts(100)), (bucket.start, bucket.end));
        // Include sequence 15 and both files at 32, but exclude the adjacent bucket.
        let expected = files[..5]
            .iter()
            .map(|file| file.file_id().file_id())
            .collect::<HashSet<_>>();
        assert_eq!(
            expected,
            bucket
                .files
                .iter()
                .map(|file| file.file_id().file_id())
                .collect()
        );
        assert_eq!(expected, entry.source_file_ids.iter().copied().collect());
        assert_eq!((15, 33), (entry.min_file_sequence, entry.max_file_sequence));
        assert!(plan.expired_index_ids.is_empty());
        assert_eq!(1, plan.index_buckets.len());
        let merged = &plan.index_buckets[&ts(0)];
        assert_eq!((ts(0), ts(100)), (merged.start, merged.end));
        assert_eq!(33, bucket.max_file_sequence);
        assert_eq!(bucket.window_sequences, merged.window_sequences);
        assert_eq!(ids.as_slice(), plan.superseded_index_ids.as_slice());
        assert_eq!([entry.index_uuid].as_slice(), merged.index_ids.as_slice());
    }

    #[test]
    fn test_plan_reuses_replaced_sources_and_rebuilds_changed_bucket() {
        let ts = Timestamp::new_second;
        let make_file = |sequence| file(Some(sequence), 0, ts(1), ts(2));
        let original = (1..=4).map(make_file).collect::<Vec<_>>();
        let initial = plan_series_indexes(
            group_files_into_series_buckets(&original, 10, 10),
            BTreeMap::new(),
            None,
            0,
        );
        let first_id = initial.builds[0].1.index_uuid;

        // New file IDs with already indexed sequences do not invalidate the index.
        let mut files = (1..=4).map(make_file).collect::<Vec<_>>();
        let replaced = plan_series_indexes(
            group_files_into_series_buckets(&files, 10, 10),
            initial.index_buckets.clone(),
            None,
            0,
        );
        assert!(replaced.builds.is_empty());
        assert!(replaced.expired_index_ids.is_empty());
        assert_eq!(initial.index_buckets, replaced.index_buckets);

        files.push(make_file(5));
        let ready = plan_series_indexes(
            group_files_into_series_buckets(&files, 10, 10),
            replaced.index_buckets,
            None,
            0,
        );
        let [(bucket, entry)] = ready.builds.as_slice() else {
            panic!("a changed window must rebuild the whole bucket");
        };
        let expected = files[..]
            .iter()
            .map(|file| file.file_id().file_id())
            .collect::<HashSet<_>>();
        assert_eq!(
            expected,
            bucket
                .files
                .iter()
                .map(|file| file.file_id().file_id())
                .collect()
        );
        assert_eq!(expected, entry.source_file_ids.iter().copied().collect());
        assert_eq!((1, 5), (entry.min_file_sequence, entry.max_file_sequence));
        assert_eq!(
            [entry.index_uuid].as_slice(),
            ready.index_buckets[&ts(0)].index_ids.as_slice()
        );
        assert_eq!(vec![first_id], ready.superseded_index_ids);
        let repeated = plan_series_indexes(
            group_files_into_series_buckets(&files, 10, 10),
            ready.index_buckets.clone(),
            None,
            0,
        );
        assert!(repeated.builds.is_empty());
        assert_eq!(ready.index_buckets, repeated.index_buckets);

        // TTL retires the replacement; the previous index is already superseded.
        let expired = plan_series_indexes(
            Vec::new(),
            ready.index_buckets,
            Some(TimeToLive::Duration(Duration::from_secs(10))),
            21_000,
        );
        assert!(expired.builds.is_empty());
        assert!(expired.index_buckets.is_empty());
        assert_eq!(
            [entry.index_uuid].as_slice(),
            expired.expired_index_ids.as_slice()
        );
    }

    #[test]
    fn test_window_coverage_includes_every_intersected_window() {
        let ts = Timestamp::new_millisecond;
        let files = [
            file(Some(10), 0, ts(-1), ts(19_999)),
            file(Some(30), 1, ts(10_000), ts(20_000)),
        ];
        let buckets = group_files_into_series_buckets(&files, 100, 10);
        assert_eq!(1, buckets.len());
        assert_eq!(
            BTreeMap::from([(-10, 10), (0, 10), (10, 30), (20, 30)]),
            buckets[0].window_sequences
        );
    }

    #[test]
    fn test_lower_window_changes_with_unchanged_bucket_maximum() {
        let ts = Timestamp::new_second;
        let mut files = [
            file(Some(1), 0, ts(1), ts(2)),
            file(Some(10), 0, ts(1), ts(2)),
            file(Some(20), 0, ts(11), ts(12)),
            file(Some(30), 0, ts(11), ts(12)),
        ]
        .to_vec();
        let initial = plan_series_indexes(
            group_files_into_series_buckets(&files, 100, 10),
            BTreeMap::new(),
            None,
            0,
        );
        files.extend((11..=13).map(|seq| file(Some(seq), 0, ts(1), ts(2))));
        let plan = plan_series_indexes(
            group_files_into_series_buckets(&files, 100, 10),
            initial.index_buckets,
            None,
            0,
        );
        let [(bucket, entry)] = plan.builds.as_slice() else {
            panic!("a changed lower watermark must rebuild the bucket");
        };
        assert_eq!(files.len(), bucket.files.len());
        assert_eq!(30, entry.max_file_sequence);
        assert_eq!(BTreeMap::from([(0, 13), (10, 30)]), entry.window_sequences);
    }

    #[test]
    fn test_deferred_bridge_preserves_established_coverage() {
        let ts = Timestamp::new_second;
        let mut indexes = BTreeMap::new();
        for (start, seq) in [(0, 10), (20, 30)] {
            IndexBucket {
                start: ts(start),
                end: ts(start + 10),
                index_ids: smallvec![FileId::random()],
                compaction_window_secs: 10,
                window_sequences: BTreeMap::from([(start, seq)]),
            }
            .insert_into(&mut indexes);
        }
        let mut files = vec![
            file(Some(13), 0, ts(1), ts(21)),
            file(Some(30), 0, ts(21), ts(22)),
        ];
        let deferred = plan_series_indexes(
            group_files_into_series_buckets(&files, 10, 10),
            indexes.clone(),
            None,
            0,
        );
        assert!(deferred.builds.is_empty());
        assert!(deferred.superseded_index_ids.is_empty());
        assert_eq!(indexes, deferred.index_buckets);
        files.extend((11..=12).map(|seq| file(Some(seq), 0, ts(1), ts(2))));
        let plan = plan_series_indexes(
            group_files_into_series_buckets(&files, 10, 10),
            deferred.index_buckets,
            None,
            0,
        );
        assert_eq!(1, plan.builds.len());
        assert_eq!(4, plan.builds[0].0.files.len());
        assert_eq!(2, plan.superseded_index_ids.len());
        assert_eq!(
            BTreeMap::from([(0, 13), (10, 13), (20, 30)]),
            plan.builds[0].1.window_sequences
        );

        files.push(file(None, 0, ts(1), ts(2)));
        let unknown = plan_series_indexes(
            group_files_into_series_buckets(&files, 10, 10),
            indexes.clone(),
            None,
            0,
        );
        assert!(unknown.builds.is_empty());
        assert_eq!(indexes, unknown.index_buckets);
    }

    #[rstest::rstest]
    #[case::removed_window(100, 10, false)]
    #[case::added_window(100, 10, true)]
    #[case::window_width(100, 20, false)]
    #[case::bucket_width(200, 10, false)]
    fn test_coverage_shape_changes_rebuild(
        #[case] bucket_width: i64,
        #[case] window_width: i64,
        #[case] add_window: bool,
    ) {
        let ts = Timestamp::new_second;
        let mut files = (27..=30)
            .map(|seq| file(Some(seq), 0, ts(11), ts(12)))
            .collect::<Vec<_>>();
        let mut original = files.clone();
        original.push(file(Some(10), 0, ts(1), ts(2)));
        let initial = plan_series_indexes(
            group_files_into_series_buckets(&original, 100, 10),
            BTreeMap::new(),
            None,
            0,
        );
        if add_window {
            files = original;
            files.push(file(Some(15), 0, ts(21), ts(22)));
        } else if bucket_width != 100 || window_width != 10 {
            files = original;
        }
        let plan = plan_series_indexes(
            group_files_into_series_buckets(&files, bucket_width, window_width),
            initial.index_buckets,
            None,
            0,
        );
        assert_eq!(1, plan.builds.len());
        assert_eq!(1, plan.superseded_index_ids.len());
        let repeated = plan_series_indexes(
            group_files_into_series_buckets(&files, bucket_width, window_width),
            plan.index_buckets,
            None,
            0,
        );
        assert!(repeated.builds.is_empty());
    }

    #[test]
    fn test_index_map_merge_is_order_independent() {
        let ts = Timestamp::new_second;
        let make_index = |start, end, width, windows: &[(i64, u64)]| IndexBucket {
            start: ts(start),
            end: ts(end),
            index_ids: smallvec![FileId::random()],
            compaction_window_secs: width,
            window_sequences: windows.iter().copied().collect(),
        };
        let indexes = [
            make_index(0, 20, 10, &[(0, 10), (10, 20)]),
            make_index(10, 30, 10, &[(10, 30), (20, 15)]),
            make_index(20, 40, 10, &[(20, 25), (30, 5)]),
        ];
        for order in [[0, 1, 2], [2, 1, 0], [1, 0, 2], [0, 2, 1]] {
            let mut map = BTreeMap::new();
            for i in order {
                indexes[i].clone().insert_into(&mut map);
            }
            assert_eq!(1, map.len());
            assert_eq!(10, map[&ts(0)].compaction_window_secs);
            assert_eq!(
                BTreeMap::from([(0, 10), (10, 30), (20, 25), (30, 5)]),
                map[&ts(0)].window_sequences
            );
            make_index(10, 30, 20, &[(0, 30)]).insert_into(&mut map);
            indexes[0].clone().insert_into(&mut map);
            assert_eq!(0, map[&ts(0)].compaction_window_secs);
            assert!(map[&ts(0)].window_sequences.is_empty());
        }
    }
}
