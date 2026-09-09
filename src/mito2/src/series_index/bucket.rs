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

/// Index files sharing a non-overlapping, half-open time bucket.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct IndexBucket {
    pub(crate) start: Timestamp,
    pub(crate) end: Timestamp,
    pub(crate) index_ids: SmallVec<[FileId; 2]>,
    pub(crate) max_file_sequence: u64,
}

impl IndexBucket {
    pub(crate) fn new(start: Timestamp, end: Timestamp) -> Self {
        Self {
            start,
            end,
            index_ids: SmallVec::new(),
            max_file_sequence: 0,
        }
    }

    fn merge(&mut self, mut other: Self) {
        self.start = self.start.min(other.start);
        self.end = self.end.max(other.end);
        self.index_ids.append(&mut other.index_ids);
        self.max_file_sequence = self.max_file_sequence.max(other.max_file_sequence);
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
/// Reconciliation may expand the interval through existing index coverage and retain
/// only files above its sequence watermark. Unknown source sequences prevent builds
/// even after filtering, since their coverage cannot be determined safely.
#[derive(Debug, Clone)]
pub(crate) struct SeriesBucket {
    pub(crate) start: Timestamp,
    pub(crate) end: Timestamp,
    pub(crate) files: SmallVec<[FileHandle; 2]>,
    pub(crate) has_unknown_sequence: bool,
    /// Maximum known source sequence, or zero when all sequences are unknown.
    pub(crate) max_file_sequence: u64,
}

/// Next bucket coverage and the work required to publish it, computed without I/O.
pub(crate) struct SeriesIndexPlan {
    pub(crate) index_buckets: BTreeMap<Timestamp, IndexBucket>,
    pub(crate) builds: Vec<(SeriesBucket, SeriesIndexEntry)>,
    pub(crate) expired_index_ids: Vec<FileId>,
    pub(crate) computed_buckets: usize,
    pub(crate) skipped_buckets: usize,
}

/// Plans complete sequence suffixes after merging time coverage. The returned bucket
/// map is publishable only after every planned build and the catalog writes succeed.
pub(crate) fn plan_series_indexes(
    buckets: Vec<SeriesBucket>,
    mut index_buckets: BTreeMap<Timestamp, IndexBucket>,
    ttl: Option<TimeToLive>,
    now_ms: i64,
) -> SeriesIndexPlan {
    let buckets = reconcile_series_buckets(buckets, &mut index_buckets);
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
    for mut bucket in buckets {
        if expired(bucket.end) {
            continue;
        }
        let index_bucket = index_buckets
            .entry(bucket.start)
            .or_insert_with(|| IndexBucket::new(bucket.start, bucket.end));
        if bucket.has_unknown_sequence || bucket.max_file_sequence <= index_bucket.max_file_sequence
        {
            continue;
        }
        // Select all SSTs above the watermark, including every file sharing a sequence.
        // File IDs may change under compaction without advancing indexed coverage.
        // The maximum remains valid because it exceeds the watermark and is retained.
        bucket.files.retain(|file| {
            file.meta_ref()
                .sequence
                .is_some_and(|sequence| sequence.get() > index_bucket.max_file_sequence)
        });
        if let Some(entry) = bucket.to_series_entry() {
            index_bucket.index_ids.push(entry.index_uuid);
            index_bucket.max_file_sequence = entry.max_file_sequence;
            builds.push((bucket, entry));
        }
    }
    index_buckets.retain(|_, bucket| !bucket.index_ids.is_empty());
    SeriesIndexPlan {
        index_buckets,
        skipped_buckets: computed_buckets - builds.len(),
        builds,
        expired_index_ids,
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
/// `width_secs` must be positive. Inclusive SST ranges are rounded outward to aligned,
/// half-open intervals in seconds. Overlapping intervals merge; adjacent ones stay
/// separate. Each merged bucket tracks its maximum sequence and any unknown sequence.
pub(crate) fn group_files_into_series_buckets(
    files: &[FileHandle],
    width_secs: i64,
) -> Vec<SeriesBucket> {
    let mut spans = files
        .iter()
        .map(|file| {
            let start = file.time_range().0.split().0;
            let end = file.time_range().1.split().0;
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
                max_file_sequence: file
                    .meta_ref()
                    .sequence
                    .map_or(0, |sequence| sequence.get()),
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
        } else {
            buckets.push(span);
        }
    }
    buckets
}

impl SeriesBucket {
    /// Creates entry metadata with a fresh UUID and sorted source file IDs.
    /// Returns `None` if a source sequence is unknown or too few files remain to build.
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
        let buckets = group_files_into_series_buckets(&files, width);
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
            group_files_into_series_buckets(&files, width)[0]
                .to_series_entry()
                .is_none()
        );
    }

    #[test]
    fn test_seconds_do_not_require_millisecond_conversion() {
        let start = Timestamp::new_second(i64::MAX / 1000 + 100);
        assert!(start.convert_to(TimeUnit::Millisecond).is_none());
        let buckets = group_files_into_series_buckets(&[file(Some(1), 0, start, start)], 1);
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
                max_file_sequence: max_sequence,
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
        let planned = group_files_into_series_buckets(&files, 10);
        assert_eq!(4, planned.len());
        let plan = plan_series_indexes(planned, indexes, None, 0);
        assert_eq!((2, 1), (plan.computed_buckets, plan.skipped_buckets));
        let [(bucket, entry)] = plan.builds.as_slice() else {
            panic!("expected one incremental build");
        };
        assert_eq!((ts(0), ts(100)), (bucket.start, bucket.end));
        // Ignore the gap at sequence 15, include both files at 32, and exclude the
        // adjacent time bucket even though its sequence exceeds the merged watermark.
        let expected = files[1..5]
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
        assert_eq!((31, 33), (entry.min_file_sequence, entry.max_file_sequence));
        assert!(plan.expired_index_ids.is_empty());
        assert_eq!(1, plan.index_buckets.len());
        let merged = &plan.index_buckets[&ts(0)];
        assert_eq!((ts(0), ts(100)), (merged.start, merged.end));
        assert_eq!(33, bucket.max_file_sequence);
        assert_eq!(33, merged.max_file_sequence);
        assert_eq!(
            [ids[0], ids[1], ids[2], entry.index_uuid].as_slice(),
            merged.index_ids.as_slice()
        );
    }

    #[test]
    fn test_plan_reuses_replaced_sources_and_builds_complete_sequence_suffix() {
        let ts = Timestamp::new_second;
        let make_file = |sequence| file(Some(sequence), 0, ts(1), ts(2));
        let original = (1..=4).map(make_file).collect::<Vec<_>>();
        let initial = plan_series_indexes(
            group_files_into_series_buckets(&original, 10),
            BTreeMap::new(),
            None,
            0,
        );
        let first_id = initial.builds[0].1.index_uuid;

        // New file IDs with already indexed sequences do not invalidate the index.
        let mut files = (1..=4).map(make_file).collect::<Vec<_>>();
        let replaced = plan_series_indexes(
            group_files_into_series_buckets(&files, 10),
            initial.index_buckets.clone(),
            None,
            0,
        );
        assert!(replaced.builds.is_empty());
        assert!(replaced.expired_index_ids.is_empty());
        assert_eq!(initial.index_buckets, replaced.index_buckets);

        files.extend((5..=7).map(make_file));
        let deferred = plan_series_indexes(
            group_files_into_series_buckets(&files, 10),
            replaced.index_buckets,
            None,
            0,
        );
        assert!(deferred.builds.is_empty());
        assert_eq!(initial.index_buckets, deferred.index_buckets);

        files.push(make_file(8));
        let ready = plan_series_indexes(
            group_files_into_series_buckets(&files, 10),
            deferred.index_buckets,
            None,
            0,
        );
        let [(bucket, entry)] = ready.builds.as_slice() else {
            panic!("the fourth new SST must trigger one build");
        };
        let expected = files[4..]
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
        assert_eq!((5, 8), (entry.min_file_sequence, entry.max_file_sequence));
        assert_eq!(
            [first_id, entry.index_uuid].as_slice(),
            ready.index_buckets[&ts(0)].index_ids.as_slice()
        );
        let repeated = plan_series_indexes(
            group_files_into_series_buckets(&files, 10),
            ready.index_buckets.clone(),
            None,
            0,
        );
        assert!(repeated.builds.is_empty());
        assert_eq!(ready.index_buckets, repeated.index_buckets);

        // Retiring the whole bucket must retire both historical and incremental indexes.
        let expired = plan_series_indexes(
            Vec::new(),
            ready.index_buckets,
            Some(TimeToLive::Duration(Duration::from_secs(10))),
            21_000,
        );
        assert!(expired.builds.is_empty());
        assert!(expired.index_buckets.is_empty());
        assert_eq!(
            [first_id, entry.index_uuid].as_slice(),
            expired.expired_index_ids.as_slice()
        );
    }
}
