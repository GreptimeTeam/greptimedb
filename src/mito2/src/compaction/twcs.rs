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
use std::fmt::{Debug, Formatter};

use common_telemetry::{debug, info};
use common_time::timestamp::TimeUnit;
use common_time::timestamp_millis::BucketAligned;
use common_time::Timestamp;

use crate::compaction::buckets::infer_time_bucket;
use crate::compaction::picker::{CompactionTask, Picker};
use crate::compaction::task::CompactionTaskImpl;
use crate::compaction::{get_expired_ssts, CompactionOutput, CompactionRequest};
use crate::sst::file::{FileHandle, FileId};
use crate::sst::version::LevelMeta;

/// `TwcsPicker` picks files of which the max timestamp are in the same time window as compaction
/// candidates.
pub struct TwcsPicker {
    max_active_window_files: usize,
    max_inactive_window_files: usize,
    time_window_seconds: Option<i64>,
}

impl Debug for TwcsPicker {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TwcsPicker")
            .field("max_active_window_files", &self.max_active_window_files)
            .field("max_inactive_window_files", &self.max_inactive_window_files)
            .finish()
    }
}

impl TwcsPicker {
    pub fn new(
        max_active_window_files: usize,
        max_inactive_window_files: usize,
        time_window_seconds: Option<i64>,
    ) -> Self {
        Self {
            max_inactive_window_files,
            max_active_window_files,
            time_window_seconds,
        }
    }

    /// Builds compaction output from files.
    /// For active writing window, we allow for at most `max_active_window_files` files to alleviate
    /// fragmentation. For other windows, we allow at most 1 file at each window.
    fn build_output(
        &self,
        time_windows: &BTreeMap<i64, Window>,
        active_window: Option<i64>,
    ) -> Vec<CompactionOutput> {
        let mut output = vec![];
        for (window, files) in time_windows {
            let files_in_window = &files.files;
            // we only remove deletion markers once no file in current window overlaps with any other window.
            let filter_deleted = !files.overlapping;

            if let Some(active_window) = active_window
                && *window == active_window
            {
                if files_in_window.len() > self.max_active_window_files {
                    output.push(CompactionOutput {
                        output_file_id: FileId::random(),
                        output_level: 1, // we only have two levels and always compact to l1
                        inputs: files_in_window.clone(),
                        filter_deleted,
                        output_time_range: None, // we do not enforce output time range in twcs compactions.
                    });
                } else {
                    debug!("Active window not present or no enough files in active window {:?}, window: {}", active_window, *window);
                }
            } else {
                // not active writing window
                if files_in_window.len() > self.max_inactive_window_files {
                    output.push(CompactionOutput {
                        output_file_id: FileId::random(),
                        output_level: 1,
                        inputs: files_in_window.clone(),
                        filter_deleted,
                        output_time_range: None,
                    });
                } else {
                    debug!(
                        "No enough files, current: {}, max_inactive_window_files: {}",
                        files_in_window.len(),
                        self.max_inactive_window_files
                    )
                }
            }
        }
        output
    }
}

impl Picker for TwcsPicker {
    fn pick(&self, req: CompactionRequest) -> Option<Box<dyn CompactionTask>> {
        let CompactionRequest {
            engine_config,
            current_version,
            access_layer,
            request_sender,
            waiters,
            file_purger,
            start_time,
            cache_manager,
            manifest_ctx,
            version_control,
            listener,
            ..
        } = req;

        let region_metadata = current_version.metadata.clone();
        let region_id = region_metadata.region_id;

        let levels = current_version.ssts.levels();
        let ttl = current_version.options.ttl;
        let expired_ssts = get_expired_ssts(levels, ttl, Timestamp::current_millis());
        if !expired_ssts.is_empty() {
            info!("Expired SSTs in region {}: {:?}", region_id, expired_ssts);
            // here we mark expired SSTs as compacting to avoid them being picked.
            expired_ssts.iter().for_each(|f| f.set_compacting(true));
        }

        let compaction_time_window = current_version
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
        let windows = assign_to_windows(levels.iter().flat_map(LevelMeta::files), time_window_size);
        let outputs = self.build_output(&windows, active_window);

        if outputs.is_empty() && expired_ssts.is_empty() {
            // Nothing to compact, we are done. Notifies all waiters as we consume the compaction request.
            for waiter in waiters {
                waiter.send(Ok(0));
            }
            return None;
        }
        let task = CompactionTaskImpl {
            engine_config,
            region_id,
            metadata: region_metadata,
            sst_layer: access_layer,
            outputs,
            expired_ssts,
            compaction_time_window: Some(time_window_size),
            request_sender,
            waiters,
            file_purger,
            start_time,
            cache_manager,
            storage: current_version.options.storage.clone(),
            index_options: current_version.options.index_options.clone(),
            append_mode: current_version.options.append_mode,
            manifest_ctx,
            version_control,
            listener,
        };
        Some(Box::new(task))
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

/// Checks if two inclusive timestamp ranges overlap with each other.
fn overlaps(l: &(Timestamp, Timestamp), r: &(Timestamp, Timestamp)) -> bool {
    let (l, r) = if l.0 <= r.0 { (l, r) } else { (r, l) };
    let (_, l_end) = l;
    let (r_start, _) = r;

    r_start <= l_end
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
    use crate::compaction::test_util::new_file_handle;
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
            let windows = assign_to_windows(self.input_files.iter(), self.window_size);
            let active_window =
                find_latest_window_in_seconds(self.input_files.iter(), self.window_size);
            let output = TwcsPicker::new(4, 1, None).build_output(&windows, active_window);

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
