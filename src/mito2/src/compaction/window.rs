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

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::fmt::Debug;

use common_telemetry::info;
use common_time::Timestamp;
use common_time::range::TimestampRange;
use common_time::timestamp::TimeUnit;
use common_time::timestamp_millis::BucketAligned;
use snafu::ResultExt;
use store_api::storage::RegionId;

use crate::compaction::CompactionOutput;
use crate::compaction::buckets::infer_time_bucket;
use crate::compaction::compactor::{CompactionRegion, CompactionVersion};
use crate::compaction::picker::{Picker, PickerOutput, get_expired_ssts};
use crate::error::{JoinSnafu, Result};
use crate::sst::file::FileHandle;

/// Compaction picker that splits the time range of all involved files to windows, and merges
/// the data segments intersects with those windows of files together so that the output files
/// never overlaps.
#[derive(Clone, Debug)]
pub struct WindowedCompactionPicker {
    compaction_time_window_seconds: Option<i64>,
    time_range: Option<TimestampRange>,
}

impl WindowedCompactionPicker {
    pub fn new(window_seconds: Option<i64>) -> Self {
        Self {
            compaction_time_window_seconds: window_seconds,
            time_range: None,
        }
    }

    /// Sets the time range used to select compaction windows.
    pub(crate) fn with_time_range(mut self, time_range: Option<TimestampRange>) -> Self {
        self.time_range = time_range;
        self
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
        }
        let expired_file_ids = expired_ssts
            .iter()
            .map(|file| file.file_id())
            .collect::<HashSet<_>>();

        let windows = assign_files_to_time_windows(
            time_window,
            current_version
                .ssts
                .levels()
                .iter()
                .flat_map(|level| level.files.values())
                .filter(|file| !expired_file_ids.contains(&file.file_id())),
        );
        let windows = filter_time_windows(windows, self.time_range);

        (build_output(windows), expired_ssts, time_window)
    }
}

#[async_trait::async_trait]
impl Picker for WindowedCompactionPicker {
    async fn pick(&self, compaction_region: &CompactionRegion) -> Result<Option<PickerOutput>> {
        let picker = self.clone();
        let region_id = compaction_region.current_version.metadata.region_id;
        let current_version = compaction_region.current_version.clone();
        let (outputs, expired_ssts, time_window) =
            common_runtime::spawn_blocking_compact(move || {
                picker.pick_inner(region_id, &current_version, Timestamp::current_millis())
            })
            .await
            .context(JoinSnafu)?;

        Ok(Some(PickerOutput {
            outputs,
            expired_ssts,
            time_window_size: time_window,
            max_file_size: None, // todo (hl): we may need to support `max_file_size` parameter in manual compaction.
        }))
    }
}

/// Keeps windows that overlap the requested range and their transitive dependencies.
///
/// [`assign_files_to_time_windows`] adds an SST to every time window that the SST covers. If a
/// selected window contains such a cross-window SST, compaction will remove that input SST after
/// rewriting it. Keeping only the directly selected window would therefore omit the SST's rows in
/// the other windows. We must include every window covered by the SST, then repeat the process for
/// other cross-window SSTs in those windows, until the complete dependency closure is selected.
fn filter_time_windows(
    mut windows: BTreeMap<i64, (i64, Vec<FileHandle>)>,
    time_range: Option<TimestampRange>,
) -> BTreeMap<i64, (i64, Vec<FileHandle>)> {
    let Some(time_range) = time_range else {
        return windows;
    };

    let mut selected_windows = windows
        .iter()
        .filter_map(|(lower_bound, (upper_bound, _))| {
            let window_start = Timestamp::new_second(*lower_bound);
            let window_end = Timestamp::new_second(*upper_bound);
            let starts_before_range_end = time_range
                .end()
                .is_none_or(|range_end| window_start < range_end);
            let ends_after_range_start = time_range
                .start()
                .is_none_or(|range_start| range_start < window_end);
            (starts_before_range_end && ends_after_range_start).then_some(*lower_bound)
        })
        .collect::<HashSet<_>>();

    let mut file_windows = HashMap::new();
    for (lower_bound, (_, files)) in &windows {
        for file in files {
            file_windows
                .entry(file.file_id())
                .or_insert_with(Vec::new)
                .push(*lower_bound);
        }
    }

    let mut pending_windows = selected_windows.iter().copied().collect::<VecDeque<_>>();
    let mut visited_files = HashSet::new();
    while let Some(lower_bound) = pending_windows.pop_front() {
        let (_, files) = &windows[&lower_bound];
        for file in files {
            if !visited_files.insert(file.file_id()) {
                continue;
            }
            for dependent_window in &file_windows[&file.file_id()] {
                if selected_windows.insert(*dependent_window) {
                    pending_windows.push_back(*dependent_window);
                }
            }
        }
    }

    windows.retain(|lower_bound, _| selected_windows.contains(lower_bound));
    windows
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

    use common_time::Timestamp;
    use common_time::range::TimestampRange;
    use store_api::storage::{FileId, RegionId};

    use crate::compaction::compactor::CompactionVersion;
    use crate::compaction::window::{WindowedCompactionPicker, file_time_bucket_span};
    use crate::region::options::RegionOptions;
    use crate::sst::file::{FileMeta, Level};
    use crate::sst::file_purger::NoopFilePurger;
    use crate::sst::version::SstVersion;
    use crate::test_util::memtable_util::metadata_for_test;

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
                auto_flush_interval: None,
                compaction: Default::default(),
                compaction_override: false,
                storage: None,
                append_mode: false,
                skip_wal: false,
                wal_options: Default::default(),
                index_options: Default::default(),
                memtable: None,
                merge_mode: None,
                sst_format: None,
                max_row_group_row_count: None,
                primary_key_encoding: None,
                write_buffer_size: None,
                preserve_row_sequence: false,
            },
            compaction_time_window: None,
        }
    }

    #[test]
    fn test_pick_expired_ssts_without_marking_compacting() {
        let picker = WindowedCompactionPicker::new(None);
        let files = vec![(FileId::random(), 0, 10, 0)];
        let version = build_version(&files, Some(Duration::from_millis(1)));
        let (outputs, expired_ssts, _) = picker.pick_inner(
            RegionId::new(0, 0),
            &version,
            Timestamp::new_millisecond(12),
        );

        assert!(outputs.is_empty());
        assert_eq!(1, expired_ssts.len());
        assert!(expired_ssts.iter().all(|file| !file.compacting()));
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
        assert_eq!(files[0].0, outputs[0].inputs[0].file_id().file_id());
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
        assert_eq!(files[1].0, outputs[2].inputs[0].file_id().file_id());
        assert_eq!(
            TimestampRange::new(
                Timestamp::new_millisecond(2 * HOUR),
                Timestamp::new_millisecond(3 * HOUR)
            ),
            outputs[2].output_time_range
        );
    }

    #[test]
    fn test_pick_time_range_expands_for_cross_window_files() {
        let time_range = TimestampRange::new(
            Timestamp::new_millisecond(HOUR / 2),
            Timestamp::new_millisecond(HOUR * 3 / 4),
        )
        .unwrap();
        let picker =
            WindowedCompactionPicker::new(Some(HOUR / 1000)).with_time_range(Some(time_range));
        let files = vec![
            (FileId::random(), 0, 2 * HOUR - 1, 0),
            (FileId::random(), HOUR, HOUR * 3 - 1, 0),
            (FileId::random(), 4 * HOUR, 5 * HOUR - 1, 0),
        ];
        let version = build_version(&files, None);

        let (outputs, _, _) = picker.pick_inner(
            RegionId::new(0, 0),
            &version,
            Timestamp::new_millisecond(6 * HOUR),
        );

        assert_eq!(3, outputs.len());
        assert_eq!(
            Some(TimestampRange::new(
                Timestamp::new_millisecond(0),
                Timestamp::new_millisecond(HOUR),
            )),
            outputs.first().map(|output| output.output_time_range)
        );
        assert_eq!(
            Some(TimestampRange::new(
                Timestamp::new_millisecond(2 * HOUR),
                Timestamp::new_millisecond(3 * HOUR),
            )),
            outputs.last().map(|output| output.output_time_range)
        );
    }

    #[test]
    fn test_pick_time_range_expands_long_dependency_chain() {
        const CHAIN_LEN: i64 = 128;

        let time_range = TimestampRange::new(
            Timestamp::new_millisecond(0),
            Timestamp::new_millisecond(HOUR / 2),
        )
        .unwrap();
        let picker =
            WindowedCompactionPicker::new(Some(HOUR / 1000)).with_time_range(Some(time_range));
        let files = (0..CHAIN_LEN)
            .map(|window| (FileId::random(), window * HOUR, (window + 2) * HOUR - 1, 0))
            .collect::<Vec<_>>();
        let version = build_version(&files, None);

        let (outputs, _, _) = picker.pick_inner(
            RegionId::new(0, 0),
            &version,
            Timestamp::new_millisecond((CHAIN_LEN + 2) * HOUR),
        );

        assert_eq!(CHAIN_LEN as usize + 1, outputs.len());
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
