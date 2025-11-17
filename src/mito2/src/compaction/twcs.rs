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

use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, HashMap};
use std::fmt::Debug;
use std::num::NonZeroU64;

use common_base::readable_size::ReadableSize;
use common_telemetry::{debug, info};
use common_time::Timestamp;
use common_time::timestamp::TimeUnit;
use common_time::timestamp_millis::BucketAligned;
use store_api::storage::RegionId;

use crate::compaction::buckets::infer_time_bucket;
use crate::compaction::compactor::CompactionRegion;
use crate::compaction::picker::{Picker, PickerOutput};
use crate::compaction::run::{
    FileGroup, Item, Ranged, find_sorted_runs, merge_seq_files, reduce_runs,
};
use crate::compaction::{CompactionOutput, get_expired_ssts};
use crate::sst::file::{FileHandle, Level, overlaps};
use crate::sst::version::LevelMeta;

const LEVEL_COMPACTED: Level = 1;

/// Default value for max compaction input file num.
const DEFAULT_MAX_INPUT_FILE_NUM: usize = 32;

/// `TwcsPicker` picks files of which the max timestamp are in the same time window as compaction
/// candidates.
#[derive(Debug)]
pub struct TwcsPicker {
    /// Minimum file num to trigger a compaction.
    pub trigger_file_num: usize,
    /// Compaction time window in seconds.
    pub time_window_seconds: Option<i64>,
    /// Max allowed compaction output file size.
    pub max_output_file_size: Option<u64>,
    /// Whether the target region is in append mode.
    pub append_mode: bool,
    /// Max background compaction tasks.
    pub max_background_tasks: Option<usize>,
}

impl TwcsPicker {
    /// Builds compaction output from files.
    fn build_output(
        &self,
        region_id: RegionId,
        time_windows: &mut BTreeMap<i64, Window>,
        active_window: Option<i64>,
    ) -> Vec<CompactionOutput> {
        let mut output = vec![];
        for (window, files) in time_windows {
            if files.files.is_empty() {
                continue;
            }
            let mut files_to_merge: Vec<_> = files.files().cloned().collect();

            // Filter out large files in append mode - they won't benefit from compaction
            if self.append_mode
                && let Some(max_size) = self.max_output_file_size
            {
                let (kept_files, ignored_files) = files_to_merge
                    .into_iter()
                    .partition(|fg| fg.size() <= max_size as usize);
                files_to_merge = kept_files;
                info!(
                    "Skipped {} large files in append mode for region {}, window {}, max_size: {}",
                    ignored_files.len(),
                    region_id,
                    window,
                    max_size
                );
            }

            let sorted_runs = find_sorted_runs(&mut files_to_merge);
            let found_runs = sorted_runs.len();
            // We only remove deletion markers if we found less than 2 runs and not in append mode.
            // because after compaction there will be no overlapping files.
            let filter_deleted = !files.overlapping && found_runs <= 2 && !self.append_mode;
            if found_runs == 0 {
                continue;
            }

            let mut inputs = if found_runs > 1 {
                reduce_runs(sorted_runs)
            } else {
                let run = sorted_runs.last().unwrap();
                if run.items().len() < self.trigger_file_num {
                    continue;
                }
                // no overlapping files, try merge small files
                merge_seq_files(run.items(), self.max_output_file_size)
            };

            // Limits the number of input files.
            let total_input_files: usize = inputs.iter().map(|fg| fg.num_files()).sum();
            if total_input_files > DEFAULT_MAX_INPUT_FILE_NUM {
                // Sorts file groups by size first.
                inputs.sort_unstable_by_key(|fg| fg.size());
                let mut num_picked_files = 0;
                inputs = inputs
                    .into_iter()
                    .take_while(|fg| {
                        let current_group_file_num = fg.num_files();
                        if current_group_file_num + num_picked_files <= DEFAULT_MAX_INPUT_FILE_NUM {
                            num_picked_files += current_group_file_num;
                            true
                        } else {
                            false
                        }
                    })
                    .collect::<Vec<_>>();
                info!(
                    "Compaction for region {} enforces max input file num limit: {}, current total: {}, input: {:?}",
                    region_id, DEFAULT_MAX_INPUT_FILE_NUM, total_input_files, inputs
                );
            }

            if inputs.len() > 1 {
                // If we have more than one group to compact.
                log_pick_result(
                    region_id,
                    *window,
                    active_window,
                    found_runs,
                    files.files.len(),
                    self.max_output_file_size,
                    filter_deleted,
                    &inputs,
                );
                output.push(CompactionOutput {
                    output_level: LEVEL_COMPACTED, // always compact to l1
                    inputs: inputs.into_iter().flat_map(|fg| fg.into_files()).collect(),
                    filter_deleted,
                    output_time_range: None, // we do not enforce output time range in twcs compactions.
                });

                if let Some(max_background_tasks) = self.max_background_tasks
                    && output.len() >= max_background_tasks
                {
                    debug!(
                        "Region ({:?}) compaction task size larger than max background tasks({}), remaining tasks discarded",
                        region_id, max_background_tasks
                    );
                    break;
                }
            }
        }
        output
    }
}

#[allow(clippy::too_many_arguments)]
fn log_pick_result(
    region_id: RegionId,
    window: i64,
    active_window: Option<i64>,
    found_runs: usize,
    file_num: usize,
    max_output_file_size: Option<u64>,
    filter_deleted: bool,
    inputs: &[FileGroup],
) {
    let input_file_str: Vec<String> = inputs
        .iter()
        .map(|f| {
            let range = f.range();
            let start = range.0.to_iso8601_string();
            let end = range.1.to_iso8601_string();
            let num_rows = f.num_rows();
            format!(
                "FileGroup{{id: {:?}, range: ({}, {}), size: {}, num rows: {} }}",
                f.file_ids(),
                start,
                end,
                ReadableSize(f.size() as u64),
                num_rows
            )
        })
        .collect();
    let window_str = Timestamp::new_second(window).to_iso8601_string();
    let active_window_str = active_window.map(|s| Timestamp::new_second(s).to_iso8601_string());
    let max_output_file_size = max_output_file_size.map(|size| ReadableSize(size).to_string());
    info!(
        "Region ({:?}) compaction pick result: current window: {}, active window: {:?}, \
            found runs: {}, file num: {}, max output file size: {:?}, filter deleted: {}, \
            input files: {:?}",
        region_id,
        window_str,
        active_window_str,
        found_runs,
        file_num,
        max_output_file_size,
        filter_deleted,
        input_file_str
    );
}

impl Picker for TwcsPicker {
    fn pick(&self, compaction_region: &CompactionRegion) -> Option<PickerOutput> {
        let region_id = compaction_region.region_id;
        let levels = compaction_region.current_version.ssts.levels();

        let expired_ssts =
            get_expired_ssts(levels, compaction_region.ttl, Timestamp::current_millis());
        if !expired_ssts.is_empty() {
            info!("Expired SSTs in region {}: {:?}", region_id, expired_ssts);
            // here we mark expired SSTs as compacting to avoid them being picked.
            expired_ssts.iter().for_each(|f| f.set_compacting(true));
        }

        let compaction_time_window = compaction_region
            .current_version
            .compaction_time_window
            .map(|window| window.as_secs() as i64);
        let time_window_size = compaction_time_window
            .or(self.time_window_seconds)
            .unwrap_or_else(|| {
                let inferred = infer_time_bucket(levels[0].files());
                info!(
                    "Compaction window for region {} is not present, inferring from files: {:?}",
                    region_id, inferred
                );
                inferred
            });

        // Find active window from files in level 0.
        let active_window = find_latest_window_in_seconds(levels[0].files(), time_window_size);
        // Assign files to windows
        let mut windows =
            assign_to_windows(levels.iter().flat_map(LevelMeta::files), time_window_size);
        let outputs = self.build_output(region_id, &mut windows, active_window);

        if outputs.is_empty() && expired_ssts.is_empty() {
            return None;
        }

        let max_file_size = self.max_output_file_size.map(|v| v as usize);
        Some(PickerOutput {
            outputs,
            expired_ssts,
            time_window_size,
            max_file_size,
        })
    }
}

struct Window {
    start: Timestamp,
    end: Timestamp,
    // Mapping from file sequence to file groups. Files with the same sequence is considered
    // created from the same compaction task.
    files: HashMap<Option<NonZeroU64>, FileGroup>,
    time_window: i64,
    overlapping: bool,
}

impl Window {
    /// Creates a new [Window] with given file.
    fn new_with_file(file: FileHandle) -> Self {
        let (start, end) = file.time_range();
        let files = HashMap::from([(file.meta_ref().sequence, FileGroup::new_with_file(file))]);
        Self {
            start,
            end,
            files,
            time_window: 0,
            overlapping: false,
        }
    }

    /// Returns the time range of all files in current window (inclusive).
    fn range(&self) -> (Timestamp, Timestamp) {
        (self.start, self.end)
    }

    /// Adds a new file to window and updates time range.
    fn add_file(&mut self, file: FileHandle) {
        let (start, end) = file.time_range();
        self.start = self.start.min(start);
        self.end = self.end.max(end);

        match self.files.entry(file.meta_ref().sequence) {
            Entry::Occupied(mut o) => {
                o.get_mut().add_file(file);
            }
            Entry::Vacant(v) => {
                v.insert(FileGroup::new_with_file(file));
            }
        }
    }

    fn files(&self) -> impl Iterator<Item = &FileGroup> {
        self.files.values()
    }
}

/// Assigns files to windows with predefined window size (in seconds) by their max timestamps.
fn assign_to_windows<'a>(
    files: impl Iterator<Item = &'a FileHandle>,
    time_window_size: i64,
) -> BTreeMap<i64, Window> {
    let mut windows: HashMap<i64, Window> = HashMap::new();
    // Iterates all files and assign to time windows according to max timestamp
    for f in files {
        if f.compacting() {
            continue;
        }
        let (_, end) = f.time_range();
        let time_window = end
            .convert_to(TimeUnit::Second)
            .unwrap()
            .value()
            .align_to_ceil_by_bucket(time_window_size)
            .unwrap_or(i64::MIN);

        match windows.entry(time_window) {
            Entry::Occupied(mut e) => {
                e.get_mut().add_file(f.clone());
            }
            Entry::Vacant(e) => {
                let mut window = Window::new_with_file(f.clone());
                window.time_window = time_window;
                e.insert(window);
            }
        }
    }
    if windows.is_empty() {
        return BTreeMap::new();
    }

    let mut windows = windows.into_values().collect::<Vec<_>>();
    windows.sort_unstable_by(|l, r| l.start.cmp(&r.start).then(l.end.cmp(&r.end).reverse()));

    let mut current_range: (Timestamp, Timestamp) = windows[0].range(); // windows cannot be empty.

    for idx in 1..windows.len() {
        let next_range = windows[idx].range();
        if overlaps(&current_range, &next_range) {
            windows[idx - 1].overlapping = true;
            windows[idx].overlapping = true;
        }
        current_range = (
            current_range.0.min(next_range.0),
            current_range.1.max(next_range.1),
        );
    }

    windows.into_iter().map(|w| (w.time_window, w)).collect()
}

/// Finds the latest active writing window among all files.
/// Returns `None` when there are no files or all files are corrupted.
fn find_latest_window_in_seconds<'a>(
    files: impl Iterator<Item = &'a FileHandle>,
    time_window_size: i64,
) -> Option<i64> {
    let mut latest_timestamp = None;
    for f in files {
        let (_, end) = f.time_range();
        if let Some(latest) = latest_timestamp {
            if end > latest {
                latest_timestamp = Some(end);
            }
        } else {
            latest_timestamp = Some(end);
        }
    }
    latest_timestamp
        .and_then(|ts| ts.convert_to_ceil(TimeUnit::Second))
        .and_then(|ts| ts.value().align_to_ceil_by_bucket(time_window_size))
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use store_api::storage::FileId;

    use super::*;
    use crate::compaction::test_util::{
        new_file_handle, new_file_handle_with_sequence, new_file_handle_with_size_and_sequence,
    };
    use crate::sst::file::Level;

    #[test]
    fn test_get_latest_window_in_seconds() {
        assert_eq!(
            Some(1),
            find_latest_window_in_seconds([new_file_handle(FileId::random(), 0, 999, 0)].iter(), 1)
        );
        assert_eq!(
            Some(1),
            find_latest_window_in_seconds(
                [new_file_handle(FileId::random(), 0, 1000, 0)].iter(),
                1
            )
        );

        assert_eq!(
            Some(-9223372036854000),
            find_latest_window_in_seconds(
                [new_file_handle(FileId::random(), i64::MIN, i64::MIN + 1, 0)].iter(),
                3600,
            )
        );

        assert_eq!(
            (i64::MAX / 10000000 + 1) * 10000,
            find_latest_window_in_seconds(
                [new_file_handle(FileId::random(), i64::MIN, i64::MAX, 0)].iter(),
                10000,
            )
            .unwrap()
        );

        assert_eq!(
            Some((i64::MAX / 3600000 + 1) * 3600),
            find_latest_window_in_seconds(
                [
                    new_file_handle(FileId::random(), i64::MIN, i64::MAX, 0),
                    new_file_handle(FileId::random(), 0, 1000, 0)
                ]
                .iter(),
                3600
            )
        );
    }

    #[test]
    fn test_assign_to_windows() {
        let windows = assign_to_windows(
            [
                new_file_handle(FileId::random(), 0, 999, 0),
                new_file_handle(FileId::random(), 0, 999, 0),
                new_file_handle(FileId::random(), 0, 999, 0),
                new_file_handle(FileId::random(), 0, 999, 0),
                new_file_handle(FileId::random(), 0, 999, 0),
            ]
            .iter(),
            3,
        );
        let fgs = &windows.get(&0).unwrap().files;
        assert_eq!(1, fgs.len());
        assert_eq!(fgs.values().map(|f| f.files().len()).sum::<usize>(), 5);

        let files = [FileId::random(); 3];
        let windows = assign_to_windows(
            [
                new_file_handle(files[0], -2000, -3, 0),
                new_file_handle(files[1], 0, 2999, 0),
                new_file_handle(files[2], 50, 10001, 0),
            ]
            .iter(),
            3,
        );
        assert_eq!(
            files[0],
            windows.get(&0).unwrap().files().next().unwrap().files()[0]
                .file_id()
                .file_id()
        );
        assert_eq!(
            files[1],
            windows.get(&3).unwrap().files().next().unwrap().files()[0]
                .file_id()
                .file_id()
        );
        assert_eq!(
            files[2],
            windows.get(&12).unwrap().files().next().unwrap().files()[0]
                .file_id()
                .file_id()
        );
    }

    #[test]
    fn test_assign_file_groups_to_windows() {
        let files = [
            FileId::random(),
            FileId::random(),
            FileId::random(),
            FileId::random(),
        ];
        let windows = assign_to_windows(
            [
                new_file_handle_with_sequence(files[0], 0, 999, 0, 1),
                new_file_handle_with_sequence(files[1], 0, 999, 0, 1),
                new_file_handle_with_sequence(files[2], 0, 999, 0, 2),
                new_file_handle_with_sequence(files[3], 0, 999, 0, 2),
            ]
            .iter(),
            3,
        );
        assert_eq!(windows.len(), 1);
        let fgs = &windows.get(&0).unwrap().files;
        assert_eq!(2, fgs.len());
        assert_eq!(
            fgs.get(&NonZeroU64::new(1))
                .unwrap()
                .files()
                .iter()
                .map(|f| f.file_id().file_id())
                .collect::<HashSet<_>>(),
            [files[0], files[1]].into_iter().collect()
        );
        assert_eq!(
            fgs.get(&NonZeroU64::new(2))
                .unwrap()
                .files()
                .iter()
                .map(|f| f.file_id().file_id())
                .collect::<HashSet<_>>(),
            [files[2], files[3]].into_iter().collect()
        );
    }

    #[test]
    fn test_assign_compacting_to_windows() {
        let files = [
            new_file_handle(FileId::random(), 0, 999, 0),
            new_file_handle(FileId::random(), 0, 999, 0),
            new_file_handle(FileId::random(), 0, 999, 0),
            new_file_handle(FileId::random(), 0, 999, 0),
            new_file_handle(FileId::random(), 0, 999, 0),
        ];
        files[0].set_compacting(true);
        files[2].set_compacting(true);
        let mut windows = assign_to_windows(files.iter(), 3);
        let window0 = windows.remove(&0).unwrap();
        assert_eq!(1, window0.files.len());
        let candidates = window0
            .files
            .into_values()
            .flat_map(|fg| fg.into_files())
            .map(|f| f.file_id().file_id())
            .collect::<HashSet<_>>();
        assert_eq!(candidates.len(), 3);
        assert_eq!(
            candidates,
            [
                files[1].file_id().file_id(),
                files[3].file_id().file_id(),
                files[4].file_id().file_id()
            ]
            .into_iter()
            .collect::<HashSet<_>>()
        );
    }

    /// (Window value, overlapping, files' time ranges in window)
    type ExpectedWindowSpec = (i64, bool, Vec<(i64, i64)>);

    fn check_assign_to_windows_with_overlapping(
        file_time_ranges: &[(i64, i64)],
        time_window: i64,
        expected_files: &[ExpectedWindowSpec],
    ) {
        let files: Vec<_> = (0..file_time_ranges.len())
            .map(|_| FileId::random())
            .collect();

        let file_handles = files
            .iter()
            .zip(file_time_ranges.iter())
            .map(|(file_id, range)| new_file_handle(*file_id, range.0, range.1, 0))
            .collect::<Vec<_>>();

        let windows = assign_to_windows(file_handles.iter(), time_window);

        for (expected_window, overlapping, window_files) in expected_files {
            let actual_window = windows.get(expected_window).unwrap();
            assert_eq!(*overlapping, actual_window.overlapping);
            let mut file_ranges = actual_window
                .files
                .iter()
                .flat_map(|(_, f)| {
                    f.files().iter().map(|f| {
                        let (s, e) = f.time_range();
                        (s.value(), e.value())
                    })
                })
                .collect::<Vec<_>>();
            file_ranges.sort_unstable_by(|l, r| l.0.cmp(&r.0).then(l.1.cmp(&r.1)));
            assert_eq!(window_files, &file_ranges);
        }
    }

    #[test]
    fn test_assign_to_windows_with_overlapping() {
        check_assign_to_windows_with_overlapping(
            &[(0, 999), (1000, 1999), (2000, 2999)],
            2,
            &[
                (0, false, vec![(0, 999)]),
                (2, false, vec![(1000, 1999), (2000, 2999)]),
            ],
        );

        check_assign_to_windows_with_overlapping(
            &[(0, 1), (0, 999), (100, 2999)],
            2,
            &[
                (0, true, vec![(0, 1), (0, 999)]),
                (2, true, vec![(100, 2999)]),
            ],
        );

        check_assign_to_windows_with_overlapping(
            &[(0, 999), (1000, 1999), (2000, 2999), (3000, 3999)],
            2,
            &[
                (0, false, vec![(0, 999)]),
                (2, false, vec![(1000, 1999), (2000, 2999)]),
                (4, false, vec![(3000, 3999)]),
            ],
        );

        check_assign_to_windows_with_overlapping(
            &[
                (0, 999),
                (1000, 1999),
                (2000, 2999),
                (3000, 3999),
                (0, 3999),
            ],
            2,
            &[
                (0, true, vec![(0, 999)]),
                (2, true, vec![(1000, 1999), (2000, 2999)]),
                (4, true, vec![(0, 3999), (3000, 3999)]),
            ],
        );

        check_assign_to_windows_with_overlapping(
            &[
                (0, 999),
                (1000, 1999),
                (2000, 2999),
                (3000, 3999),
                (1999, 3999),
            ],
            2,
            &[
                (0, false, vec![(0, 999)]),
                (2, true, vec![(1000, 1999), (2000, 2999)]),
                (4, true, vec![(1999, 3999), (3000, 3999)]),
            ],
        );

        check_assign_to_windows_with_overlapping(
            &[
                (0, 999),     // window 0
                (1000, 1999), // window 2
                (2000, 2999), // window 2
                (3000, 3999), // window 4
                (2999, 3999), // window 4
            ],
            2,
            &[
                // window 2 overlaps with window 4
                (0, false, vec![(0, 999)]),
                (2, true, vec![(1000, 1999), (2000, 2999)]),
                (4, true, vec![(2999, 3999), (3000, 3999)]),
            ],
        );

        check_assign_to_windows_with_overlapping(
            &[
                (0, 999),     // window 0
                (1000, 1999), // window 2
                (2000, 2999), // window 2
                (3000, 3999), // window 4
                (0, 1000),    // // window 2
            ],
            2,
            &[
                // only window 0 overlaps with window 2.
                (0, true, vec![(0, 999)]),
                (2, true, vec![(0, 1000), (1000, 1999), (2000, 2999)]),
                (4, false, vec![(3000, 3999)]),
            ],
        );
    }

    struct CompactionPickerTestCase {
        window_size: i64,
        input_files: Vec<FileHandle>,
        expected_outputs: Vec<ExpectedOutput>,
    }

    impl CompactionPickerTestCase {
        fn check(&self) {
            let file_id_to_idx = self
                .input_files
                .iter()
                .enumerate()
                .map(|(idx, file)| (file.file_id(), idx))
                .collect::<HashMap<_, _>>();
            let mut windows = assign_to_windows(self.input_files.iter(), self.window_size);
            let active_window =
                find_latest_window_in_seconds(self.input_files.iter(), self.window_size);
            let output = TwcsPicker {
                trigger_file_num: 4,
                time_window_seconds: None,
                max_output_file_size: None,
                append_mode: false,
                max_background_tasks: None,
            }
            .build_output(RegionId::from_u64(0), &mut windows, active_window);

            let output = output
                .iter()
                .map(|o| {
                    let input_file_ids = o
                        .inputs
                        .iter()
                        .map(|f| file_id_to_idx.get(&f.file_id()).copied().unwrap())
                        .collect::<HashSet<_>>();
                    (input_file_ids, o.output_level)
                })
                .collect::<Vec<_>>();

            let expected = self
                .expected_outputs
                .iter()
                .map(|o| {
                    let input_file_ids = o.input_files.iter().copied().collect::<HashSet<_>>();
                    (input_file_ids, o.output_level)
                })
                .collect::<Vec<_>>();
            assert_eq!(expected, output);
        }
    }

    struct ExpectedOutput {
        input_files: Vec<usize>,
        output_level: Level,
    }

    #[test]
    fn test_build_twcs_output() {
        let file_ids = (0..4).map(|_| FileId::random()).collect::<Vec<_>>();

        // Case 1: 2 runs found in each time window.
        CompactionPickerTestCase {
            window_size: 3,
            input_files: [
                new_file_handle_with_sequence(file_ids[0], -2000, -3, 0, 1),
                new_file_handle_with_sequence(file_ids[1], -3000, -100, 0, 2),
                new_file_handle_with_sequence(file_ids[2], 0, 2999, 0, 3), //active windows
                new_file_handle_with_sequence(file_ids[3], 50, 2998, 0, 4), //active windows
            ]
            .to_vec(),
            expected_outputs: vec![
                ExpectedOutput {
                    input_files: vec![0, 1],
                    output_level: 1,
                },
                ExpectedOutput {
                    input_files: vec![2, 3],
                    output_level: 1,
                },
            ],
        }
        .check();

        // Case 2:
        //    -2000........-3
        // -3000.....-100
        //                    0..............2999
        //                      50..........2998
        //                     11.........2990
        let file_ids = (0..6).map(|_| FileId::random()).collect::<Vec<_>>();
        CompactionPickerTestCase {
            window_size: 3,
            input_files: [
                new_file_handle_with_sequence(file_ids[0], -2000, -3, 0, 1),
                new_file_handle_with_sequence(file_ids[1], -3000, -100, 0, 2),
                new_file_handle_with_sequence(file_ids[2], 0, 2999, 0, 3),
                new_file_handle_with_sequence(file_ids[3], 50, 2998, 0, 4),
                new_file_handle_with_sequence(file_ids[4], 11, 2990, 0, 5),
            ]
            .to_vec(),
            expected_outputs: vec![
                ExpectedOutput {
                    input_files: vec![0, 1],
                    output_level: 1,
                },
                ExpectedOutput {
                    input_files: vec![2, 4],
                    output_level: 1,
                },
            ],
        }
        .check();

        // Case 3:
        // A compaction may split output into several files that have overlapping time ranges and same sequence,
        // we should treat these files as one FileGroup.
        let file_ids = (0..6).map(|_| FileId::random()).collect::<Vec<_>>();
        CompactionPickerTestCase {
            window_size: 3,
            input_files: [
                new_file_handle_with_sequence(file_ids[0], 0, 2999, 1, 1),
                new_file_handle_with_sequence(file_ids[1], 0, 2998, 1, 1),
                new_file_handle_with_sequence(file_ids[2], 3000, 5999, 1, 2),
                new_file_handle_with_sequence(file_ids[3], 3000, 5000, 1, 2),
                new_file_handle_with_sequence(file_ids[4], 11, 2990, 0, 3),
            ]
            .to_vec(),
            expected_outputs: vec![ExpectedOutput {
                input_files: vec![0, 1, 4],
                output_level: 1,
            }],
        }
        .check();
    }

    #[test]
    fn test_append_mode_filter_large_files() {
        let file_ids = (0..4).map(|_| FileId::random()).collect::<Vec<_>>();
        let max_output_file_size = 1000u64;

        // Create files with different sizes
        let small_file_1 = new_file_handle_with_size_and_sequence(file_ids[0], 0, 999, 0, 1, 500);
        let large_file_1 = new_file_handle_with_size_and_sequence(file_ids[1], 0, 999, 0, 2, 1500);
        let small_file_2 = new_file_handle_with_size_and_sequence(file_ids[2], 0, 999, 0, 3, 800);
        let large_file_2 = new_file_handle_with_size_and_sequence(file_ids[3], 0, 999, 0, 4, 2000);

        // Create file groups (each file is in its own group due to different sequences)
        let mut files_to_merge = vec![
            FileGroup::new_with_file(small_file_1),
            FileGroup::new_with_file(large_file_1),
            FileGroup::new_with_file(small_file_2),
            FileGroup::new_with_file(large_file_2),
        ];

        // Test filtering logic directly
        let original_count = files_to_merge.len();

        // Apply append mode filtering
        files_to_merge.retain(|fg| fg.size() <= max_output_file_size as usize);

        // Should have filtered out 2 large files, leaving 2 small files
        assert_eq!(files_to_merge.len(), 2);
        assert_eq!(original_count, 4);

        // Verify the remaining files are the small ones
        for fg in &files_to_merge {
            assert!(
                fg.size() <= max_output_file_size as usize,
                "File size {} should be <= {}",
                fg.size(),
                max_output_file_size
            );
        }
    }

    #[test]
    fn test_build_output_multiple_windows_with_zero_runs() {
        let file_ids = (0..6).map(|_| FileId::random()).collect::<Vec<_>>();

        let files = [
            // Window 0: Contains 3 files but not forming any runs (not enough files in sequence to reach trigger_file_num)
            new_file_handle_with_sequence(file_ids[0], 0, 999, 0, 1),
            new_file_handle_with_sequence(file_ids[1], 0, 999, 0, 2),
            new_file_handle_with_sequence(file_ids[2], 0, 999, 0, 3),
            // Window 3: Contains files that will form 2 runs
            new_file_handle_with_sequence(file_ids[3], 3000, 3999, 0, 4),
            new_file_handle_with_sequence(file_ids[4], 3000, 3999, 0, 5),
            new_file_handle_with_sequence(file_ids[5], 3000, 3999, 0, 6),
        ];

        let mut windows = assign_to_windows(files.iter(), 3);

        // Create picker with trigger_file_num of 4 so single files won't form runs in first window
        let picker = TwcsPicker {
            trigger_file_num: 4, // High enough to prevent runs in first window
            time_window_seconds: Some(3),
            max_output_file_size: None,
            append_mode: false,
            max_background_tasks: None,
        };

        let active_window = find_latest_window_in_seconds(files.iter(), 3);
        let output = picker.build_output(RegionId::from_u64(123), &mut windows, active_window);

        assert!(
            !output.is_empty(),
            "Should have output from windows with runs, even when one window has 0 runs"
        );

        let all_output_files: Vec<_> = output
            .iter()
            .flat_map(|o| o.inputs.iter())
            .map(|f| f.file_id().file_id())
            .collect();

        assert!(
            all_output_files.contains(&file_ids[3])
                || all_output_files.contains(&file_ids[4])
                || all_output_files.contains(&file_ids[5]),
            "Output should contain files from the window with runs"
        );
    }

    #[test]
    fn test_build_output_single_window_zero_runs() {
        let file_ids = (0..2).map(|_| FileId::random()).collect::<Vec<_>>();

        let large_file_1 = new_file_handle_with_size_and_sequence(file_ids[0], 0, 999, 0, 1, 2000); // 2000 bytes
        let large_file_2 = new_file_handle_with_size_and_sequence(file_ids[1], 0, 999, 0, 2, 2500); // 2500 bytes

        let files = [large_file_1, large_file_2];

        let mut windows = assign_to_windows(files.iter(), 3);

        let picker = TwcsPicker {
            trigger_file_num: 2,
            time_window_seconds: Some(3),
            max_output_file_size: Some(1000),
            append_mode: true,
            max_background_tasks: None,
        };

        let active_window = find_latest_window_in_seconds(files.iter(), 3);
        let output = picker.build_output(RegionId::from_u64(456), &mut windows, active_window);

        // Should return empty output (no compaction needed)
        assert!(
            output.is_empty(),
            "Should return empty output when no runs are found after filtering"
        );
    }

    #[test]
    fn test_max_background_tasks_truncation() {
        let file_ids = (0..10).map(|_| FileId::random()).collect::<Vec<_>>();
        let max_background_tasks = 3;

        // Create files across multiple windows that will generate multiple compaction outputs
        let files = [
            // Window 0: 4 files that will form a run
            new_file_handle_with_sequence(file_ids[0], 0, 999, 0, 1),
            new_file_handle_with_sequence(file_ids[1], 0, 999, 0, 2),
            new_file_handle_with_sequence(file_ids[2], 0, 999, 0, 3),
            new_file_handle_with_sequence(file_ids[3], 0, 999, 0, 4),
            // Window 3: 4 files that will form another run
            new_file_handle_with_sequence(file_ids[4], 3000, 3999, 0, 5),
            new_file_handle_with_sequence(file_ids[5], 3000, 3999, 0, 6),
            new_file_handle_with_sequence(file_ids[6], 3000, 3999, 0, 7),
            new_file_handle_with_sequence(file_ids[7], 3000, 3999, 0, 8),
            // Window 6: 4 files that will form another run
            new_file_handle_with_sequence(file_ids[8], 6000, 6999, 0, 9),
            new_file_handle_with_sequence(file_ids[9], 6000, 6999, 0, 10),
        ];

        let mut windows = assign_to_windows(files.iter(), 3);

        let picker = TwcsPicker {
            trigger_file_num: 4,
            time_window_seconds: Some(3),
            max_output_file_size: None,
            append_mode: false,
            max_background_tasks: Some(max_background_tasks),
        };

        let active_window = find_latest_window_in_seconds(files.iter(), 3);
        let output = picker.build_output(RegionId::from_u64(123), &mut windows, active_window);

        // Should have at most max_background_tasks outputs
        assert!(
            output.len() <= max_background_tasks,
            "Output should be truncated to max_background_tasks: expected <= {}, got {}",
            max_background_tasks,
            output.len()
        );

        // Without max_background_tasks, should have more outputs
        let picker_no_limit = TwcsPicker {
            trigger_file_num: 4,
            time_window_seconds: Some(3),
            max_output_file_size: None,
            append_mode: false,
            max_background_tasks: None,
        };

        let mut windows_no_limit = assign_to_windows(files.iter(), 3);
        let output_no_limit = picker_no_limit.build_output(
            RegionId::from_u64(123),
            &mut windows_no_limit,
            active_window,
        );

        // Without limit, should have more outputs (if there are enough windows)
        if output_no_limit.len() > max_background_tasks {
            assert!(
                output_no_limit.len() > output.len(),
                "Without limit should have more outputs than with limit"
            );
        }
    }

    #[test]
    fn test_max_background_tasks_no_truncation_when_under_limit() {
        let file_ids = (0..4).map(|_| FileId::random()).collect::<Vec<_>>();
        let max_background_tasks = 10; // Larger than expected outputs

        // Create files in one window that will generate one compaction output
        let files = [
            new_file_handle_with_sequence(file_ids[0], 0, 999, 0, 1),
            new_file_handle_with_sequence(file_ids[1], 0, 999, 0, 2),
            new_file_handle_with_sequence(file_ids[2], 0, 999, 0, 3),
            new_file_handle_with_sequence(file_ids[3], 0, 999, 0, 4),
        ];

        let mut windows = assign_to_windows(files.iter(), 3);

        let picker = TwcsPicker {
            trigger_file_num: 4,
            time_window_seconds: Some(3),
            max_output_file_size: None,
            append_mode: false,
            max_background_tasks: Some(max_background_tasks),
        };

        let active_window = find_latest_window_in_seconds(files.iter(), 3);
        let output = picker.build_output(RegionId::from_u64(123), &mut windows, active_window);

        // Should have all outputs since we're under the limit
        assert!(
            output.len() <= max_background_tasks,
            "Output should be within limit"
        );
        // Should have at least one output
        assert!(!output.is_empty(), "Should have at least one output");
    }

    #[test]
    fn test_pick_multiple_runs() {
        common_telemetry::init_default_ut_logging();

        let num_files = 8;
        let file_ids = (0..num_files).map(|_| FileId::random()).collect::<Vec<_>>();

        // Create files with different sequences so they form multiple runs
        let files: Vec<_> = file_ids
            .iter()
            .enumerate()
            .map(|(idx, file_id)| {
                new_file_handle_with_size_and_sequence(
                    *file_id,
                    0,
                    999,
                    0,
                    (idx + 1) as u64,
                    1024 * 1024,
                )
            })
            .collect();

        let mut windows = assign_to_windows(files.iter(), 3);

        let picker = TwcsPicker {
            trigger_file_num: 4,
            time_window_seconds: Some(3),
            max_output_file_size: None,
            append_mode: false,
            max_background_tasks: None,
        };

        let active_window = find_latest_window_in_seconds(files.iter(), 3);
        let output = picker.build_output(RegionId::from_u64(123), &mut windows, active_window);

        assert_eq!(1, output.len());
        assert_eq!(output[0].inputs.len(), 2);
    }

    #[test]
    fn test_limit_max_input_files() {
        common_telemetry::init_default_ut_logging();

        let num_files = 50;
        let file_ids = (0..num_files).map(|_| FileId::random()).collect::<Vec<_>>();

        // Create files with different sequences so they form 2 runs
        let files: Vec<_> = file_ids
            .iter()
            .enumerate()
            .map(|(idx, file_id)| {
                new_file_handle_with_size_and_sequence(
                    *file_id,
                    (idx / 2 * 10) as i64,
                    (idx / 2 * 10 + 5) as i64,
                    0,
                    (idx + 1) as u64,
                    1024 * 1024,
                )
            })
            .collect();

        let mut windows = assign_to_windows(files.iter(), 3);

        let picker = TwcsPicker {
            trigger_file_num: 4,
            time_window_seconds: Some(3),
            max_output_file_size: None,
            append_mode: false,
            max_background_tasks: None,
        };

        let active_window = find_latest_window_in_seconds(files.iter(), 3);
        let output = picker.build_output(RegionId::from_u64(123), &mut windows, active_window);

        assert_eq!(1, output.len());
        assert_eq!(output[0].inputs.len(), 32);
    }

    // TODO(hl): TTL tester that checks if get_expired_ssts function works as expected.
}
