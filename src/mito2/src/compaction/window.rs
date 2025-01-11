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

use std::collections::BTreeMap;
use std::fmt::Debug;

use common_telemetry::info;
use common_time::range::TimestampRange;
use common_time::timestamp::TimeUnit;
use common_time::timestamp_millis::BucketAligned;
use common_time::Timestamp;
use store_api::storage::RegionId;

use crate::compaction::buckets::infer_time_bucket;
use crate::compaction::compactor::{CompactionRegion, CompactionVersion};
use crate::compaction::picker::{Picker, PickerOutput};
use crate::compaction::{get_expired_ssts, CompactionOutput};
use crate::sst::file::FileHandle;

/// Compaction picker that splits the time range of all involved files to windows, and merges
/// the data segments intersects with those windows of files together so that the output files
/// never overlaps.
#[derive(Debug)]
pub struct WindowedCompactionPicker {
    compaction_time_window_seconds: Option<i64>,
}

impl WindowedCompactionPicker {
    pub fn new(window_seconds: Option<i64>) -> Self {
        Self {
            compaction_time_window_seconds: window_seconds,
        }
    }

    // Computes compaction time window. First we respect user specified parameter, then
    // use persisted window. If persist window is not present, we check the time window
    // provided while creating table. If all of those are absent, we infer the window
    // from files in level0.
    fn calculate_time_window(
        &self,
        region_id: RegionId,
        current_version: &CompactionVersion,
    ) -> i64 {
        self.compaction_time_window_seconds
            .or(current_version
                .compaction_time_window
                .map(|t| t.as_secs() as i64))
            .unwrap_or_else(|| {
                let levels = current_version.ssts.levels();
                let inferred = infer_time_bucket(levels[0].files());
                info!(
                    "Compaction window for region {} is not present, inferring from files: {:?}",
                    region_id, inferred
                );
                inferred
            })
    }

    fn pick_inner(
        &self,
        region_id: RegionId,
        current_version: &CompactionVersion,
        current_time: Timestamp,
    ) -> (Vec<CompactionOutput>, Vec<FileHandle>, i64) {
        let time_window = self.calculate_time_window(region_id, current_version);
        info!(
            "Compaction window for region: {} is {} seconds",
            region_id, time_window
        );

        let expired_ssts = get_expired_ssts(
            current_version.ssts.levels(),
            current_version.options.ttl,
            current_time,
        );
        if !expired_ssts.is_empty() {
            info!("Expired SSTs in region {}: {:?}", region_id, expired_ssts);
            // here we mark expired SSTs as compacting to avoid them being picked.
            expired_ssts.iter().for_each(|f| f.set_compacting(true));
        }

        let windows = assign_files_to_time_windows(
            time_window,
            current_version
                .ssts
                .levels()
                .iter()
                .flat_map(|level| level.files.values()),
        );

        (build_output(windows), expired_ssts, time_window)
    }
}

impl Picker for WindowedCompactionPicker {
    fn pick(&self, compaction_region: &CompactionRegion) -> Option<PickerOutput> {
        let (outputs, expired_ssts, time_window) = self.pick_inner(
            compaction_region.current_version.metadata.region_id,
            &compaction_region.current_version,
            Timestamp::current_millis(),
        );

        Some(PickerOutput {
            outputs,
            expired_ssts,
            time_window_size: time_window,
            max_file_size: None, // todo (hl): we may need to support `max_file_size` parameter in manual compaction.
        })
    }
}

fn build_output(windows: BTreeMap<i64, (i64, Vec<FileHandle>)>) -> Vec<CompactionOutput> {
    let mut outputs = Vec::with_capacity(windows.len());
    for (lower_bound, (upper_bound, files)) in windows {
        // safety: the upper bound must > lower bound.
        let output_time_range = Some(
            TimestampRange::new(
                Timestamp::new_second(lower_bound),
                Timestamp::new_second(upper_bound),
            )
            .unwrap(),
        );

        let output = CompactionOutput {
            output_level: 1,
            inputs: files,
            filter_deleted: false,
            output_time_range,
        };
        outputs.push(output);
    }

    outputs
}

/// Assigns files to time windows. If file does not contain a time range in metadata, it will be
/// assigned to a special bucket `i64::MAX` (normally no timestamp can be aligned to this bucket)
/// so that all files without timestamp can be compacted together.
fn assign_files_to_time_windows<'a>(
    bucket_sec: i64,
    files: impl Iterator<Item = &'a FileHandle>,
) -> BTreeMap<i64, (i64, Vec<FileHandle>)> {
    let mut buckets = BTreeMap::new();

    for file in files {
        if file.compacting() {
            continue;
        }
        let (start, end) = file.time_range();
        let bounds = file_time_bucket_span(
            // safety: converting whatever timestamp to seconds will not overflow.
            start.convert_to(TimeUnit::Second).unwrap().value(),
            end.convert_to(TimeUnit::Second).unwrap().value(),
            bucket_sec,
        );
        for (lower_bound, upper_bound) in bounds {
            let (_, files) = buckets
                .entry(lower_bound)
                .or_insert_with(|| (upper_bound, Vec::new()));
            files.push(file.clone());
        }
    }
    buckets
}

/// Calculates timestamp span between start and end timestamp.
fn file_time_bucket_span(start_sec: i64, end_sec: i64, bucket_sec: i64) -> Vec<(i64, i64)> {
    assert!(start_sec <= end_sec);

    // if timestamp is between `[i64::MIN, i64::MIN.align_by_bucket(bucket)]`, which cannot
    // be aligned to a valid i64 bound, simply return `i64::MIN` rather than just underflow.
    let mut start_aligned = start_sec.align_by_bucket(bucket_sec).unwrap_or(i64::MIN);
    let end_aligned = end_sec
        .align_by_bucket(bucket_sec)
        .unwrap_or(start_aligned + (end_sec - start_sec));

    let mut res = Vec::with_capacity(((end_aligned - start_aligned) / bucket_sec + 1) as usize);
    while start_aligned <= end_aligned {
        let window_size = if start_aligned % bucket_sec == 0 {
            bucket_sec
        } else {
            (start_aligned % bucket_sec).abs()
        };
        let upper_bound = start_aligned.checked_add(window_size).unwrap_or(i64::MAX);
        res.push((start_aligned, upper_bound));
        start_aligned = upper_bound;
    }
    res
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use common_time::range::TimestampRange;
    use common_time::Timestamp;
    use store_api::storage::RegionId;

    use crate::compaction::compactor::CompactionVersion;
    use crate::compaction::window::{file_time_bucket_span, WindowedCompactionPicker};
    use crate::region::options::RegionOptions;
    use crate::sst::file::{FileId, FileMeta, Level};
    use crate::sst::version::SstVersion;
    use crate::test_util::memtable_util::metadata_for_test;
    use crate::test_util::NoopFilePurger;

    fn build_version(
        files: &[(FileId, i64, i64, Level)],
        ttl: Option<Duration>,
    ) -> CompactionVersion {
        let metadata = metadata_for_test();
        let file_purger_ref = Arc::new(NoopFilePurger);

        let mut ssts = SstVersion::new();

        ssts.add_files(
            file_purger_ref,
            files.iter().map(|(file_id, start, end, level)| FileMeta {
                file_id: *file_id,
                time_range: (
                    Timestamp::new_millisecond(*start),
                    Timestamp::new_millisecond(*end),
                ),
                level: *level,
                ..Default::default()
            }),
        );

        CompactionVersion {
            metadata,
            ssts: Arc::new(ssts),
            options: RegionOptions {
                ttl: ttl.map(|t| t.into()),
                compaction: Default::default(),
                storage: None,
                append_mode: false,
                wal_options: Default::default(),
                index_options: Default::default(),
                memtable: None,
                merge_mode: None,
            },
            compaction_time_window: None,
        }
    }

    #[test]
    fn test_pick_expired() {
        let picker = WindowedCompactionPicker::new(None);
        let files = vec![(FileId::random(), 0, 10, 0)];

        let version = build_version(&files, Some(Duration::from_millis(1)));
        let (outputs, expired_ssts, _window) = picker.pick_inner(
            RegionId::new(0, 0),
            &version,
            Timestamp::new_millisecond(12),
        );
        assert!(outputs.is_empty());
        assert_eq!(1, expired_ssts.len());
    }

    const HOUR: i64 = 60 * 60 * 1000;

    #[test]
    fn test_infer_window() {
        let picker = WindowedCompactionPicker::new(None);

        let files = vec![
            (FileId::random(), 0, HOUR, 0),
            (FileId::random(), HOUR, HOUR * 2 - 1, 0),
        ];

        let version = build_version(&files, Some(Duration::from_millis(3 * HOUR as u64)));

        let (outputs, expired_ssts, window_seconds) = picker.pick_inner(
            RegionId::new(0, 0),
            &version,
            Timestamp::new_millisecond(HOUR * 2),
        );
        assert!(expired_ssts.is_empty());
        assert_eq!(2 * HOUR / 1000, window_seconds);
        assert_eq!(1, outputs.len());
        assert_eq!(2, outputs[0].inputs.len());
    }

    #[test]
    fn test_assign_files_to_windows() {
        let picker = WindowedCompactionPicker::new(Some(HOUR / 1000));
        let files = vec![
            (FileId::random(), 0, 2 * HOUR - 1, 0),
            (FileId::random(), HOUR, HOUR * 3 - 1, 0),
        ];
        let version = build_version(&files, Some(Duration::from_millis(3 * HOUR as u64)));
        let (outputs, expired_ssts, window_seconds) = picker.pick_inner(
            RegionId::new(0, 0),
            &version,
            Timestamp::new_millisecond(HOUR * 3),
        );

        assert!(expired_ssts.is_empty());
        assert_eq!(HOUR / 1000, window_seconds);
        assert_eq!(3, outputs.len());

        assert_eq!(1, outputs[0].inputs.len());
        assert_eq!(files[0].0, outputs[0].inputs[0].file_id());
        assert_eq!(
            TimestampRange::new(
                Timestamp::new_millisecond(0),
                Timestamp::new_millisecond(HOUR)
            ),
            outputs[0].output_time_range
        );

        assert_eq!(2, outputs[1].inputs.len());
        assert_eq!(
            TimestampRange::new(
                Timestamp::new_millisecond(HOUR),
                Timestamp::new_millisecond(2 * HOUR)
            ),
            outputs[1].output_time_range
        );

        assert_eq!(1, outputs[2].inputs.len());
        assert_eq!(files[1].0, outputs[2].inputs[0].file_id());
        assert_eq!(
            TimestampRange::new(
                Timestamp::new_millisecond(2 * HOUR),
                Timestamp::new_millisecond(3 * HOUR)
            ),
            outputs[2].output_time_range
        );
    }

    #[test]
    fn test_assign_compacting_files_to_windows() {
        let picker = WindowedCompactionPicker::new(Some(HOUR / 1000));
        let files = vec![
            (FileId::random(), 0, 2 * HOUR - 1, 0),
            (FileId::random(), HOUR, HOUR * 3 - 1, 0),
        ];
        let version = build_version(&files, Some(Duration::from_millis(3 * HOUR as u64)));
        version.ssts.levels()[0]
            .files()
            .for_each(|f| f.set_compacting(true));
        let (outputs, expired_ssts, window_seconds) = picker.pick_inner(
            RegionId::new(0, 0),
            &version,
            Timestamp::new_millisecond(HOUR * 3),
        );

        assert!(expired_ssts.is_empty());
        assert_eq!(HOUR / 1000, window_seconds);
        assert!(outputs.is_empty());
    }

    #[test]
    fn test_file_time_bucket_span() {
        assert_eq!(
            vec![(i64::MIN, i64::MIN + 8),],
            file_time_bucket_span(i64::MIN, i64::MIN + 1, 10)
        );

        assert_eq!(
            vec![(i64::MIN, i64::MIN + 8), (i64::MIN + 8, i64::MIN + 18)],
            file_time_bucket_span(i64::MIN, i64::MIN + 8, 10)
        );

        assert_eq!(
            vec![
                (i64::MIN, i64::MIN + 8),
                (i64::MIN + 8, i64::MIN + 18),
                (i64::MIN + 18, i64::MIN + 28)
            ],
            file_time_bucket_span(i64::MIN, i64::MIN + 20, 10)
        );

        assert_eq!(
            vec![(-10, 0), (0, 10), (10, 20)],
            file_time_bucket_span(-1, 11, 10)
        );

        assert_eq!(
            vec![(-3, 0), (0, 3), (3, 6)],
            file_time_bucket_span(-1, 3, 3)
        );

        assert_eq!(vec![(0, 10)], file_time_bucket_span(0, 9, 10));

        assert_eq!(
            vec![(i64::MAX - (i64::MAX % 10), i64::MAX)],
            file_time_bucket_span(i64::MAX - 1, i64::MAX, 10)
        );
    }
}
