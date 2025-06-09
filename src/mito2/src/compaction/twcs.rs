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

use common_base::readable_size::ReadableSize;
use common_telemetry::info;
use common_time::timestamp::TimeUnit;
use common_time::timestamp_millis::BucketAligned;
use common_time::Timestamp;
use store_api::storage::RegionId;

use crate::compaction::buckets::infer_time_bucket;
use crate::compaction::compactor::CompactionRegion;
use crate::compaction::picker::{Picker, PickerOutput};
use crate::compaction::run::{find_sorted_runs, merge_seq_files, reduce_runs};
use crate::compaction::{get_expired_ssts, CompactionOutput};
use crate::sst::file::{overlaps, FileHandle, Level};
use crate::sst::version::LevelMeta;

const LEVEL_COMPACTED: Level = 1;

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
            let sorted_runs = find_sorted_runs(&mut files.files);
            let found_runs = sorted_runs.len();
            // We only remove deletion markers if we found less than 2 runs and not in append mode.
            // because after compaction there will be no overlapping files.
            let filter_deleted = !files.overlapping && found_runs <= 2 && !self.append_mode;

            let inputs = if found_runs > 1 {
                reduce_runs(sorted_runs)
            } else {
                let run = sorted_runs.last().unwrap();
                if run.items().len() < self.trigger_file_num {
                    continue;
                }
                // no overlapping files, try merge small files
                merge_seq_files(run.items(), self.max_output_file_size)
            };

            if !inputs.is_empty() {
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
                    inputs,
                    filter_deleted,
                    output_time_range: None, // we do not enforce output time range in twcs compactions.
                });
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
    inputs: &[FileHandle],
) {
    let input_file_str: Vec<String> = inputs
        .iter()
        .map(|f| {
            let range = f.time_range();
            let start = range.0.to_iso8601_string();
            let end = range.1.to_iso8601_string();
            let num_rows = f.num_rows();
            format!(
                "SST{{id: {}, range: ({}, {}), size: {}, num rows: {} }}",
                f.file_id(),
                start,
                end,
                ReadableSize(f.size()),
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
    files: Vec<FileHandle>,
    time_window: i64,
    overlapping: bool,
}

impl Window {
    /// Creates a new [Window] with given file.
    fn new_with_file(file: FileHandle) -> Self {
        let (start, end) = file.time_range();
        Self {
            start,
            end,
            files: vec![file],
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
        self.files.push(file);
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

    use super::*;
    use crate::compaction::test_util::new_file_handle;
    use crate::sst::file::{FileId, Level};

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
        assert_eq!(5, windows.get(&0).unwrap().files.len());

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
            windows.get(&0).unwrap().files.first().unwrap().file_id()
        );
        assert_eq!(
            files[1],
            windows.get(&3).unwrap().files.first().unwrap().file_id()
        );
        assert_eq!(
            files[2],
            windows.get(&12).unwrap().files.first().unwrap().file_id()
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
        let windows = assign_to_windows(files.iter(), 3);
        assert_eq!(3, windows.get(&0).unwrap().files.len());
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
                .map(|f| {
                    let (s, e) = f.time_range();
                    (s.value(), e.value())
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
                new_file_handle(file_ids[0], -2000, -3, 0),
                new_file_handle(file_ids[1], -3000, -100, 0),
                new_file_handle(file_ids[2], 0, 2999, 0), //active windows
                new_file_handle(file_ids[3], 50, 2998, 0), //active windows
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
                new_file_handle(file_ids[0], -2000, -3, 0),
                new_file_handle(file_ids[1], -3000, -100, 0),
                new_file_handle(file_ids[2], 0, 2999, 0),
                new_file_handle(file_ids[3], 50, 2998, 0),
                new_file_handle(file_ids[4], 11, 2990, 0),
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
    }

    // TODO(hl): TTL tester that checks if get_expired_ssts function works as expected.
}
