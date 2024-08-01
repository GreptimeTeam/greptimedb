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

use common_telemetry::{debug, info};
use common_time::timestamp::TimeUnit;
use common_time::timestamp_millis::BucketAligned;
use common_time::Timestamp;

use crate::compaction::buckets::infer_time_bucket;
use crate::compaction::compactor::CompactionRegion;
use crate::compaction::picker::{Picker, PickerOutput};
use crate::compaction::run::{find_sorted_runs, reduce_runs, Item};
use crate::compaction::{get_expired_ssts, CompactionOutput};
use crate::sst::file::{overlaps, FileHandle, FileId, Level};
use crate::sst::version::LevelMeta;

const LEVEL_COMPACTED: Level = 1;

/// `TwcsPicker` picks files of which the max timestamp are in the same time window as compaction
/// candidates.
#[derive(Debug)]
pub struct TwcsPicker {
    max_active_window_runs: usize,
    max_active_window_files: usize,
    max_inactive_window_runs: usize,
    max_inactive_window_files: usize,
    time_window_seconds: Option<i64>,
}

impl TwcsPicker {
    pub fn new(
        max_active_window_runs: usize,
        max_active_window_files: usize,
        max_inactive_window_runs: usize,
        max_inactive_window_files: usize,
        time_window_seconds: Option<i64>,
    ) -> Self {
        Self {
            max_inactive_window_runs,
            max_active_window_runs,
            time_window_seconds,
            max_active_window_files,
            max_inactive_window_files,
        }
    }

    /// Builds compaction output from files.
    /// For active writing window, we allow for at most `max_active_window_runs` files to alleviate
    /// fragmentation. For other windows, we allow at most 1 file at each window.
    fn build_output(
        &self,
        time_windows: &mut BTreeMap<i64, Window>,
        active_window: Option<i64>,
    ) -> Vec<CompactionOutput> {
        let mut output = vec![];
        for (window, files) in time_windows {
            let sorted_runs = find_sorted_runs(&mut files.files);

            let (max_runs, max_files) = if let Some(active_window) = active_window
                && *window == active_window
            {
                (self.max_active_window_runs, self.max_active_window_files)
            } else {
                (
                    self.max_inactive_window_runs,
                    self.max_inactive_window_files,
                )
            };

            // we only remove deletion markers once no file in current window overlaps with any other window.
            let found_runs = sorted_runs.len();
            let filter_deleted = !files.overlapping && (found_runs == 1 || max_runs == 1);

            if found_runs > max_runs {
                let files_to_compact = reduce_runs(sorted_runs, max_runs);
                info!("Building compaction output, active window: {:?}, current window: {}, max runs: {}, found runs: {}, output size: {}, remove deletion markers: {}", active_window, *window,max_runs, found_runs, files_to_compact.len(), filter_deleted);
                for inputs in files_to_compact {
                    output.push(CompactionOutput {
                        output_file_id: FileId::random(),
                        output_level: LEVEL_COMPACTED, // always compact to l1
                        inputs,
                        filter_deleted,
                        output_time_range: None, // we do not enforce output time range in twcs compactions.
                    });
                }
            } else if files.files.len() > max_files {
                debug!(
                    "Enforcing max file num in window: {}, active: {:?}, max: {}, current: {}",
                    *window,
                    active_window,
                    max_files,
                    files.files.len()
                );
                // Files in window exceeds file num limit
                let to_merge = enforce_file_num(&files.files, max_files);
                output.push(CompactionOutput {
                    output_file_id: FileId::random(),
                    output_level: LEVEL_COMPACTED, // always compact to l1
                    inputs: to_merge,
                    filter_deleted,
                    output_time_range: None,
                });
            } else {
                debug!("Skip building compaction output, active window: {:?}, current window: {}, max runs: {}, found runs: {}, ", active_window, *window, max_runs, found_runs);
            }
        }
        output
    }
}

/// Merges consecutive files so that file num does not exceed `max_file_num`, and chooses
/// the solution with minimum overhead according to files sizes to be merged.
/// `enforce_file_num` only merges consecutive files so that it won't create overlapping outputs.
/// `runs` must be sorted according to time ranges.
fn enforce_file_num<T: Item>(files: &[T], max_file_num: usize) -> Vec<T> {
    debug_assert!(files.len() > max_file_num);
    let to_merge = files.len() - max_file_num + 1;
    let mut min_penalty = usize::MAX;
    let mut min_idx = 0;

    for idx in 0..=(files.len() - to_merge) {
        let current_penalty: usize = files
            .iter()
            .skip(idx)
            .take(to_merge)
            .map(|f| f.size())
            .sum();
        if current_penalty < min_penalty {
            min_penalty = current_penalty;
            min_idx = idx;
        }
    }
    files.iter().skip(min_idx).take(to_merge).cloned().collect()
}

impl Picker for TwcsPicker {
    fn pick(&self, compaction_region: &CompactionRegion) -> Option<PickerOutput> {
        let region_id = compaction_region.region_id;
        let levels = compaction_region.current_version.ssts.levels();
        let ttl = compaction_region.current_version.options.ttl;
        let expired_ssts = get_expired_ssts(levels, ttl, Timestamp::current_millis());
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
        let outputs = self.build_output(&mut windows, active_window);

        if outputs.is_empty() && expired_ssts.is_empty() {
            return None;
        }

        Some(PickerOutput {
            outputs,
            expired_ssts,
            time_window_size,
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
        if let Some(latest) = latest_timestamp
            && end > latest
        {
            latest_timestamp = Some(end);
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
    use crate::compaction::test_util::{new_file_handle, new_file_handles};
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
            let mut windows = assign_to_windows(self.input_files.iter(), self.window_size);
            let active_window =
                find_latest_window_in_seconds(self.input_files.iter(), self.window_size);
            let output = TwcsPicker::new(4, usize::MAX, 1, usize::MAX, None)
                .build_output(&mut windows, active_window);

            let output = output
                .iter()
                .map(|o| {
                    let input_file_ids =
                        o.inputs.iter().map(|f| f.file_id()).collect::<HashSet<_>>();
                    (input_file_ids, o.output_level)
                })
                .collect::<Vec<_>>();

            let expected = self
                .expected_outputs
                .iter()
                .map(|o| {
                    let input_file_ids = o
                        .input_files
                        .iter()
                        .map(|idx| self.input_files[*idx].file_id())
                        .collect::<HashSet<_>>();
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

    fn check_enforce_file_num(
        input_files: &[(i64, i64, u64)],
        max_file_num: usize,
        files_to_merge: &[(i64, i64)],
    ) {
        let mut files = new_file_handles(input_files);
        // ensure sorted
        find_sorted_runs(&mut files);
        let mut to_merge = enforce_file_num(&files, max_file_num);
        to_merge.sort_unstable_by_key(|f| f.time_range().0);
        assert_eq!(
            files_to_merge.to_vec(),
            to_merge
                .iter()
                .map(|f| {
                    let (start, end) = f.time_range();
                    (start.value(), end.value())
                })
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_enforce_file_num() {
        check_enforce_file_num(
            &[(0, 300, 2), (100, 200, 1), (200, 400, 1)],
            2,
            &[(100, 200), (200, 400)],
        );

        check_enforce_file_num(
            &[(0, 300, 200), (100, 200, 100), (200, 400, 100)],
            1,
            &[(0, 300), (100, 200), (200, 400)],
        );
    }

    #[test]
    fn test_build_twcs_output() {
        let file_ids = (0..4).map(|_| FileId::random()).collect::<Vec<_>>();

        CompactionPickerTestCase {
            window_size: 3,
            input_files: [
                new_file_handle(file_ids[0], -2000, -3, 0),
                new_file_handle(file_ids[1], -3000, -100, 0),
                new_file_handle(file_ids[2], 0, 2999, 0), //active windows
                new_file_handle(file_ids[3], 50, 2998, 0), //active windows
            ]
            .to_vec(),
            expected_outputs: vec![ExpectedOutput {
                input_files: vec![0, 1],
                output_level: 1,
            }],
        }
        .check();

        let file_ids = (0..6).map(|_| FileId::random()).collect::<Vec<_>>();
        CompactionPickerTestCase {
            window_size: 3,
            input_files: [
                new_file_handle(file_ids[0], -2000, -3, 0),
                new_file_handle(file_ids[1], -3000, -100, 0),
                new_file_handle(file_ids[2], 0, 2999, 0),
                new_file_handle(file_ids[3], 50, 2998, 0),
                new_file_handle(file_ids[4], 11, 2990, 0),
                new_file_handle(file_ids[5], 50, 4998, 0),
            ]
            .to_vec(),
            expected_outputs: vec![
                ExpectedOutput {
                    input_files: vec![0, 1],
                    output_level: 1,
                },
                ExpectedOutput {
                    input_files: vec![2, 3, 4],
                    output_level: 1,
                },
            ],
        }
        .check();
    }

    // TODO(hl): TTL tester that checks if get_expired_ssts function works as expected.
}
