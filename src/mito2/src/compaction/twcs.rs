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
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt::Debug;
use std::num::NonZeroU64;
use std::sync::Arc;

use common_base::readable_size::ReadableSize;
use common_telemetry::{debug, info};
use common_time::Timestamp;
use common_time::range::TimestampRange;
use common_time::timestamp::TimeUnit;
use common_time::timestamp_millis::BucketAligned;
use snafu::ResultExt;
use store_api::storage::RegionId;

use crate::compaction::CompactionOutput;
use crate::compaction::buckets::infer_time_bucket;
use crate::compaction::compactor::CompactionRegion;
use crate::compaction::picker::{Picker, PickerOutput, get_expired_ssts};
use crate::compaction::run::{
    FileGroup, Item, Ranged, SortedRun, find_sorted_runs, find_sorted_runs_by_time_range,
    merge_primary_key_ranges, primary_key_ranges_overlap,
};
use crate::error::{JoinSnafu, Result};
use crate::sst::file::{FileHandle, Level, overlaps};
use crate::sst::version::LevelMeta;

const LEVEL_COMPACTED: Level = 1;

/// Maximum number of logical FileGroups in one compaction input.
const MAX_INPUT_FILE_GROUPS: usize = 32;

/// `TwcsPicker` picks files of which the max timestamp are in the same time window as compaction
/// candidates.
#[derive(Clone, Debug)]
pub struct TwcsPicker {
    /// Minimum eligible FileGroup num to trigger a compaction.
    pub trigger_file_num: usize,
    /// Compaction time window in seconds.
    pub time_window_seconds: Option<i64>,
    /// Max allowed compaction output file size.
    pub max_output_file_size: Option<u64>,
    /// Whether the target region is in append mode.
    pub append_mode: bool,
    /// Max background compaction tasks.
    pub max_background_tasks: Option<usize>,
    /// Optional time range that constrains candidate compaction windows.
    pub(crate) time_range: Option<TimestampRange>,
}

impl TwcsPicker {
    async fn build_output_with_time_range(
        &self,
        region_id: RegionId,
        time_windows: BTreeMap<i64, Window>,
        active_window: Option<i64>,
        time_window_size: Option<i64>,
    ) -> Result<Vec<CompactionOutput>> {
        let mut output = vec![];
        let windows = time_windows
            .values()
            .rev()
            .filter(|window| {
                !window.files.is_empty()
                    && self.time_range.as_ref().is_none_or(|time_range| {
                        time_window_size.is_none_or(|time_window_size| {
                            time_window_intersects_range(
                                window.time_window,
                                time_window_size,
                                time_range,
                            )
                        })
                    })
            })
            .map(|window| window.time_window)
            .collect::<Vec<_>>();
        let time_windows = Arc::new(time_windows);
        let chunk_size = self.max_background_tasks.unwrap_or(windows.len()).max(1);
        'chunks: for chunk in windows.chunks(chunk_size) {
            let mut handles = Vec::with_capacity(chunk.len());
            for window in chunk {
                let picker = self.clone();
                let time_windows = time_windows.clone();
                let window = *window;
                handles.push(common_runtime::spawn_blocking_compact(move || {
                    time_windows.get(&window).map(|window| {
                        picker.find_inputs(region_id, active_window, window, &time_windows)
                    })
                }));
                tokio::task::yield_now().await;
            }
            for result in futures::future::join_all(handles).await {
                let Some((inputs, filter_deleted)) = result.context(JoinSnafu)? else {
                    continue;
                };
                if inputs.is_empty() {
                    continue;
                }

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
                    break 'chunks;
                }
            }
        }
        Ok(output)
    }

    fn find_inputs(
        &self,
        region_id: RegionId,
        active_window: Option<i64>,
        files: &Window,
        windows: &BTreeMap<i64, Window>,
    ) -> (Vec<FileGroup>, bool) {
        let window = &files.time_window;
        let mut files_to_merge: Vec<_> = files.files().cloned().collect();

        // Filter out large files in append mode - they won't benefit from compaction
        if self.append_mode
            && let Some(max_size) = self.max_output_file_size
        {
            let (kept_files, ignored_files) = files_to_merge
                .into_iter()
                .partition(|fg| fg.size() <= max_size as usize);
            files_to_merge = kept_files;
            if !ignored_files.is_empty() {
                info!(
                    "Skipped {} large files in append mode for region {}, window {}, max_size: {}",
                    ignored_files.len(),
                    region_id,
                    window,
                    max_size
                );
            }
        }

        let sorted_runs = if files_to_merge.len() < 1024 {
            find_sorted_runs(&mut files_to_merge)
        } else {
            find_sorted_runs_by_time_range(&mut files_to_merge)
        };
        let found_runs = sorted_runs.len();
        let inputs = pick_count_first(sorted_runs, self.trigger_file_num);
        let mut filter_deleted = !self.append_mode
            && !window_has_overlap(files, windows)
            && !selected_overlaps_unselected(&inputs, files);

        // The inputs are only a subset of the window: `reduce_runs` and `merge_seq_files` both
        // narrow the selection down and the file num limit above narrows it further. A deletion
        // marker may only be dropped when every file that can hold a row it masks is compacted
        // along with it, so re-check the final selection against what stays in the window.
        if filter_deleted && overlaps_files_left_behind(&inputs, &files_to_merge) {
            filter_deleted = false;
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
        }
        (inputs, filter_deleted)
    }
}

/// Returns whether any file group of `window_files` that is not part of `inputs` overlaps
/// `inputs`. Such a group keeps rows a deletion marker in `inputs` masks, so the compaction
/// must not filter deleted rows out.
///
/// Overlapping is inclusive on both boundaries here: two files that only share a boundary
/// timestamp can still hold the same row.
fn overlaps_files_left_behind(inputs: &[FileGroup], window_files: &[FileGroup]) -> bool {
    let picked: HashSet<_> = inputs.iter().flat_map(|fg| fg.file_ids()).collect();
    window_files
        .iter()
        .filter(|fg| !fg.file_ids().iter().any(|id| picked.contains(id)))
        .any(|fg| inputs.iter().any(|input| input.overlap_inclusive(fg)))
}

#[derive(Debug)]
struct OrderedFileGroup<'a> {
    group: &'a FileGroup,
    run_id: usize,
    position_in_run: usize,
}

#[derive(Debug)]
struct CandidateScore {
    num_file_groups: usize,
    overlap_participants: usize,
    size: usize,
}

impl CandidateScore {
    fn is_better_than(&self, other: &Self) -> bool {
        self.num_file_groups
            .cmp(&other.num_file_groups)
            .then_with(|| self.overlap_participants.cmp(&other.overlap_participants))
            .then_with(|| other.size.cmp(&self.size))
            .is_gt()
    }
}

fn pick_count_first(
    sorted_runs: Vec<SortedRun<FileGroup>>,
    trigger_file_num: usize,
) -> Vec<FileGroup> {
    let mut groups = sorted_runs
        .iter()
        .enumerate()
        .flat_map(|(run_id, run)| {
            run.items()
                .iter()
                .enumerate()
                .map(move |(position_in_run, group)| OrderedFileGroup {
                    group,
                    run_id,
                    position_in_run,
                })
        })
        .collect::<Vec<_>>();
    groups.sort_unstable_by(|lhs, rhs| {
        let (lhs_start, lhs_end) = lhs.group.range();
        let (rhs_start, rhs_end) = rhs.group.range();
        lhs_start
            .cmp(&rhs_start)
            .then_with(|| rhs_end.cmp(&lhs_end))
            .then_with(|| lhs.run_id.cmp(&rhs.run_id))
            .then_with(|| lhs.position_in_run.cmp(&rhs.position_in_run))
    });
    // A multi-SST FileGroup is a compaction output that already exceeded the output size target
    // and was split. Recompacting it cannot reduce physical files, so automatic TWCS treats it as
    // stable. It remains in the original Window for tombstone overlap checks.
    groups.retain(|group| group.group.num_files() == 1);

    let mut best = None;
    for left in 0..groups.len() {
        let mut total_size = 0;
        let mut largest_group_size = 0;
        let mut overlap_participants = 0;
        let right_bound = (left + MAX_INPUT_FILE_GROUPS).min(groups.len());
        let mut participants: Vec<bool> = Vec::with_capacity(right_bound - left);
        for right in left..right_bound {
            let right_group = &groups[right];
            let group_size = right_group.group.size();
            total_size += group_size;
            largest_group_size = largest_group_size.max(group_size);

            let mut right_participates = false;
            for (offset, other) in groups[left..right].iter().enumerate() {
                if right_group.run_id != other.run_id
                    && right_group.group.overlap_inclusive(other.group)
                {
                    if !participants[offset] {
                        participants[offset] = true;
                        overlap_participants += 1;
                    }
                    right_participates = true;
                }
            }
            participants.push(right_participates);
            if right_participates {
                overlap_participants += 1;
            }

            let num_file_groups = right + 1 - left;
            if num_file_groups < trigger_file_num || num_file_groups < 2 {
                continue;
            }
            // A historical group is only rewritten after peers contribute at least the same
            // amount of data, so every rewrite moves it into a larger size tier.
            if largest_group_size > total_size - largest_group_size {
                continue;
            }

            let score = CandidateScore {
                num_file_groups,
                overlap_participants,
                size: total_size,
            };
            if best
                .as_ref()
                .is_none_or(|(best_score, _, _)| score.is_better_than(best_score))
            {
                best = Some((score, left, right));
            }
        }
    }

    let Some((_, left, right)) = best else {
        return vec![];
    };
    groups[left..=right]
        .iter()
        .map(|group| group.group.clone())
        .collect()
}

fn selected_overlaps_unselected(selected: &[FileGroup], window: &Window) -> bool {
    let selected_file_ids = selected
        .iter()
        .flat_map(FileGroup::file_ids)
        .collect::<HashSet<_>>();
    window
        .files()
        .filter(|group| {
            !group
                .file_ids()
                .iter()
                .all(|file_id| selected_file_ids.contains(file_id))
        })
        .any(|unselected| {
            selected
                .iter()
                .any(|selected| selected.overlap_inclusive(unselected))
        })
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

#[async_trait::async_trait]
impl Picker for TwcsPicker {
    async fn pick(&self, compaction_region: &CompactionRegion) -> Result<Option<PickerOutput>> {
        let region_id = compaction_region.region_id;
        let picker = self.clone();
        let compaction_region = compaction_region.clone();
        let (expired_ssts, time_window_size, active_window, windows) =
            common_runtime::spawn_blocking_compact(move || {
                let levels = compaction_region.current_version.ssts.levels();
                let expired_ssts = get_expired_ssts(
                    levels,
                    compaction_region.ttl,
                    Timestamp::current_millis(),
                );
                if !expired_ssts.is_empty() {
                    info!("Expired SSTs in region {}: {:?}", region_id, expired_ssts);
                }
                let expired_file_ids = expired_ssts
                    .iter()
                    .map(|file| file.file_id())
                    .collect::<HashSet<_>>();

                let compaction_time_window = compaction_region
                    .current_version
                    .compaction_time_window
                    .map(|window| window.as_secs() as i64);
                let time_window_size = compaction_time_window
                    .or(picker.time_window_seconds)
                    .unwrap_or_else(|| {
                        let inferred = infer_time_bucket(levels[0].files());
                        info!(
                            "Compaction window for region {} is not present, inferring from files: {:?}",
                            region_id, inferred
                        );
                        inferred
                    });

                let active_window =
                    find_latest_window_in_seconds(levels[0].files(), time_window_size);
                let windows = assign_to_windows(
                    levels
                        .iter()
                        .flat_map(LevelMeta::files)
                        .filter(|file| !expired_file_ids.contains(&file.file_id())),
                    time_window_size,
                );

                (expired_ssts, time_window_size, active_window, windows)
            })
            .await
            .context(JoinSnafu)?;

        let outputs = self
            .build_output_with_time_range(region_id, windows, active_window, Some(time_window_size))
            .await?;

        if outputs.is_empty() && expired_ssts.is_empty() {
            return Ok(None);
        }

        let max_file_size = self.max_output_file_size.map(|v| v as usize);
        Ok(Some(PickerOutput {
            outputs,
            expired_ssts,
            time_window_size,
            max_file_size,
        }))
    }
}

#[derive(Clone)]
struct Window {
    start: Timestamp,
    end: Timestamp,
    // Mapping from file sequence to file groups. Files with the same sequence is considered
    // created from the same compaction task.
    files: HashMap<Option<NonZeroU64>, FileGroup>,
    time_window: i64,
    primary_key_range: Option<(bytes::Bytes, bytes::Bytes)>,
}

impl Window {
    /// Creates a new [Window] with given file.
    fn new_with_file(file: FileHandle) -> Self {
        let (start, end) = file.time_range();
        let primary_key_range = file.primary_key_range();
        let files = HashMap::from([(file.meta_ref().sequence, FileGroup::new_with_file(file))]);
        Self {
            start,
            end,
            files,
            time_window: 0,
            primary_key_range,
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
        self.primary_key_range =
            merge_primary_key_ranges(self.primary_key_range.take(), file.primary_key_range());

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
    windows.into_iter().collect()
}

fn time_window_intersects_range(
    window_end: i64,
    time_window_size: i64,
    time_range: &TimestampRange,
) -> bool {
    let first_window = match time_range.start() {
        None => i64::MIN,
        Some(start) => {
            let Some(first_window) = start
                .convert_to(TimeUnit::Second)
                .and_then(|timestamp| timestamp.value().align_to_ceil_by_bucket(time_window_size))
            else {
                return false;
            };
            first_window
        }
    };
    let last_window = match time_range.end() {
        None => i64::MAX,
        Some(end) => {
            let Some(last_window) = end
                .convert_to_ceil(TimeUnit::Second)
                .and_then(|timestamp| timestamp.value().checked_sub(1))
                .and_then(|timestamp| timestamp.align_to_ceil_by_bucket(time_window_size))
            else {
                return false;
            };
            last_window
        }
    };
    (first_window..=last_window).contains(&window_end)
}

fn window_has_overlap(this: &Window, windows: &BTreeMap<i64, Window>) -> bool {
    windows
        .values()
        .filter(|that| this.time_window != that.time_window)
        .any(|that| {
            overlaps(&this.range(), &that.range()) && {
                match (&this.primary_key_range, &that.primary_key_range) {
                    (Some(l), Some(r)) => primary_key_ranges_overlap(l, r),
                    _ => true,
                }
            }
        })
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
    use std::num::NonZeroU64;
    use std::sync::Arc;
    use std::time::Duration;

    use bytes::Bytes;
    use common_base::Plugins;
    use common_time::range::TimestampRange;
    use store_api::storage::FileId;

    use super::*;
    use crate::cache::CacheManager;
    use crate::compaction::compactor::CompactionVersion;
    use crate::compaction::test_util::{
        new_file_handle, new_file_handle_with_sequence, new_file_handle_with_size_and_sequence,
        new_file_handle_with_size_sequence_and_primary_key_range,
    };
    use crate::config::MitoConfig;
    use crate::region::options::RegionOptions;
    use crate::sst::file::{FileMeta, Level};
    use crate::sst::version::SstVersion;
    use crate::test_util::memtable_util::metadata_for_test;
    use crate::test_util::scheduler_util::SchedulerEnv;

    async fn compaction_region_with_expired_sst() -> CompactionRegion {
        let env = SchedulerEnv::new().await;
        let metadata = metadata_for_test();
        let manifest_ctx = env.mock_manifest_context(metadata.clone()).await;
        let mut ssts = SstVersion::new();
        ssts.add_files(
            Arc::new(crate::sst::file_purger::NoopFilePurger),
            (1..=4).map(|sequence| FileMeta {
                file_id: FileId::random(),
                time_range: (
                    Timestamp::new_millisecond(0),
                    Timestamp::new_millisecond(10),
                ),
                level: 0,
                sequence: NonZeroU64::new(sequence),
                ..Default::default()
            }),
        );

        CompactionRegion {
            region_id: metadata.region_id,
            region_options: RegionOptions::default(),
            engine_config: Arc::new(MitoConfig::default()),
            region_metadata: metadata.clone(),
            cache_manager: Arc::new(CacheManager::default()),
            access_layer: env.access_layer,
            manifest_ctx,
            current_version: CompactionVersion {
                metadata,
                options: RegionOptions::default(),
                ssts: Arc::new(ssts),
                compaction_time_window: None,
            },
            file_purger: None,
            ttl: Some(Duration::from_millis(1).into()),
            max_parallelism: 1,
            plugins: Plugins::new(),
        }
    }

    #[tokio::test]
    async fn test_pick_expired_ssts_without_marking_compacting() {
        let picker = TwcsPicker {
            trigger_file_num: 4,
            time_window_seconds: Some(3),
            max_output_file_size: None,
            append_mode: false,
            max_background_tasks: None,
            time_range: None,
        };
        let compaction_region = compaction_region_with_expired_sst().await;

        let output = picker.pick(&compaction_region).await.unwrap().unwrap();

        assert!(output.outputs.is_empty());
        assert!(!output.expired_ssts.is_empty());
        assert!(output.expired_ssts.iter().all(|file| !file.compacting()));
    }

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

    fn pk_range(min: &'static [u8], max: &'static [u8]) -> Option<(Bytes, Bytes)> {
        Some((Bytes::from_static(min), Bytes::from_static(max)))
    }

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
            let actual_overlapping = window_has_overlap(actual_window, &windows);
            assert_eq!(*overlapping, actual_overlapping);
            let mut file_ranges = actual_window
                .files
                .values()
                .flat_map(|f| {
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

    #[test]
    fn test_assign_to_windows_not_overlapping_when_pk_disjoint() {
        let files = [
            new_file_handle_with_size_sequence_and_primary_key_range(
                FileId::random(),
                0,
                1000,
                0,
                1,
                10,
                pk_range(b"a", b"f"),
            ),
            new_file_handle_with_size_sequence_and_primary_key_range(
                FileId::random(),
                500,
                1999,
                0,
                2,
                10,
                pk_range(b"x", b"z"),
            ),
        ];

        let windows = assign_to_windows(files.iter(), 2);

        let overlapping = window_has_overlap(windows.get(&2).unwrap(), &windows);
        assert!(!overlapping);
    }

    #[test]
    fn test_assign_to_windows_pk_unknown_in_earlier_window_does_not_poison_later_windows() {
        let files = [
            new_file_handle(FileId::random(), 0, 1999, 0),
            new_file_handle_with_size_sequence_and_primary_key_range(
                FileId::random(),
                2000,
                3999,
                0,
                1,
                10,
                pk_range(b"a", b"f"),
            ),
            new_file_handle_with_size_sequence_and_primary_key_range(
                FileId::random(),
                3000,
                4999,
                0,
                2,
                10,
                pk_range(b"x", b"z"),
            ),
        ];

        let windows = assign_to_windows(files.iter(), 2);

        let overlapping = window_has_overlap(windows.get(&4).unwrap(), &windows);
        assert!(!overlapping);
    }

    struct CompactionPickerTestCase {
        window_size: i64,
        input_files: Vec<FileHandle>,
        expected_outputs: Vec<ExpectedOutput>,
    }

    impl CompactionPickerTestCase {
        async fn check(&self) {
            let file_id_to_idx = self
                .input_files
                .iter()
                .enumerate()
                .map(|(idx, file)| (file.file_id(), idx))
                .collect::<HashMap<_, _>>();
            let windows = assign_to_windows(self.input_files.iter(), self.window_size);
            let active_window =
                find_latest_window_in_seconds(self.input_files.iter(), self.window_size);
            let output = TwcsPicker {
                trigger_file_num: 2,
                time_window_seconds: None,
                max_output_file_size: None,
                append_mode: false,
                max_background_tasks: None,
                time_range: None,
            }
            .build_output_with_time_range(RegionId::from_u64(0), windows, active_window, None)
            .await
            .unwrap();

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

    #[tokio::test]
    async fn test_build_twcs_output() {
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
                    input_files: vec![2, 3],
                    output_level: 1,
                },
                ExpectedOutput {
                    input_files: vec![0, 1],
                    output_level: 1,
                },
            ],
        }
        .check()
        .await;

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
                    input_files: vec![2, 3, 4],
                    output_level: 1,
                },
                ExpectedOutput {
                    input_files: vec![0, 1],
                    output_level: 1,
                },
            ],
        }
        .check()
        .await;

        // Case 3:
        // A compaction may split output into several files that have overlapping time ranges and
        // the same sequence. We treat these files as one stable FileGroup and exclude it from
        // automatic TWCS candidates.
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
            expected_outputs: vec![],
        }
        .check()
        .await;
    }

    #[tokio::test]
    async fn test_build_output_skips_pk_disjoint_files() {
        let files = [
            new_file_handle_with_size_sequence_and_primary_key_range(
                FileId::random(),
                0,
                2999,
                0,
                1,
                10,
                pk_range(b"a", b"f"),
            ),
            new_file_handle_with_size_sequence_and_primary_key_range(
                FileId::random(),
                50,
                2998,
                0,
                2,
                10,
                pk_range(b"x", b"z"),
            ),
        ];
        let windows = assign_to_windows(files.iter(), 3);
        let active_window = find_latest_window_in_seconds(files.iter(), 3);
        let output = TwcsPicker {
            trigger_file_num: 4,
            time_window_seconds: None,
            max_output_file_size: None,
            append_mode: false,
            max_background_tasks: None,
            time_range: None,
        }
        .build_output_with_time_range(RegionId::from_u64(0), windows, active_window, None)
        .await
        .unwrap();

        assert!(output.is_empty());
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

    #[tokio::test]
    async fn test_build_output_multiple_windows_with_zero_runs() {
        let file_ids = (0..7).map(|_| FileId::random()).collect::<Vec<_>>();

        let files = [
            // Window 0: Contains 3 files but not forming any runs (not enough files in sequence to reach trigger_file_num)
            new_file_handle_with_sequence(file_ids[0], 0, 999, 0, 1),
            new_file_handle_with_sequence(file_ids[1], 0, 999, 0, 2),
            new_file_handle_with_sequence(file_ids[2], 0, 999, 0, 3),
            // Window 3: Contains files that will form 2 runs
            new_file_handle_with_sequence(file_ids[3], 3000, 3999, 0, 4),
            new_file_handle_with_sequence(file_ids[4], 3000, 3999, 0, 5),
            new_file_handle_with_sequence(file_ids[5], 3000, 3999, 0, 6),
            new_file_handle_with_sequence(file_ids[6], 3000, 3999, 0, 7),
        ];

        let windows = assign_to_windows(files.iter(), 3);

        // Create picker with trigger_file_num of 4 so single files won't form runs in first window
        let picker = TwcsPicker {
            trigger_file_num: 4, // High enough to prevent runs in first window
            time_window_seconds: Some(3),
            max_output_file_size: None,
            append_mode: false,
            max_background_tasks: None,
            time_range: None,
        };

        let active_window = find_latest_window_in_seconds(files.iter(), 3);
        let output = picker
            .build_output_with_time_range(RegionId::from_u64(123), windows, active_window, None)
            .await
            .unwrap();

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

    #[tokio::test]
    async fn test_build_output_single_window_zero_runs() {
        let file_ids = (0..2).map(|_| FileId::random()).collect::<Vec<_>>();

        let large_file_1 = new_file_handle_with_size_and_sequence(file_ids[0], 0, 999, 0, 1, 2000); // 2000 bytes
        let large_file_2 = new_file_handle_with_size_and_sequence(file_ids[1], 0, 999, 0, 2, 2500); // 2500 bytes

        let files = [large_file_1, large_file_2];

        let windows = assign_to_windows(files.iter(), 3);

        let picker = TwcsPicker {
            trigger_file_num: 2,
            time_window_seconds: Some(3),
            max_output_file_size: Some(1000),
            append_mode: true,
            max_background_tasks: None,
            time_range: None,
        };

        let active_window = find_latest_window_in_seconds(files.iter(), 3);
        let output = picker
            .build_output_with_time_range(RegionId::from_u64(456), windows, active_window, None)
            .await
            .unwrap();

        // Should return empty output (no compaction needed)
        assert!(
            output.is_empty(),
            "Should return empty output when no runs are found after filtering"
        );
    }

    #[tokio::test]
    async fn test_max_background_tasks_truncation() {
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

        let windows = assign_to_windows(files.iter(), 3);

        let picker = TwcsPicker {
            trigger_file_num: 4,
            time_window_seconds: Some(3),
            max_output_file_size: None,
            append_mode: false,
            max_background_tasks: Some(max_background_tasks),
            time_range: None,
        };

        let active_window = find_latest_window_in_seconds(files.iter(), 3);
        let output = picker
            .build_output_with_time_range(RegionId::from_u64(123), windows, active_window, None)
            .await
            .unwrap();

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
            time_range: None,
        };

        let windows_no_limit = assign_to_windows(files.iter(), 3);
        let output_no_limit = picker_no_limit
            .build_output_with_time_range(
                RegionId::from_u64(123),
                windows_no_limit,
                active_window,
                None,
            )
            .await
            .unwrap();

        // Without limit, should have more outputs (if there are enough windows)
        if output_no_limit.len() > max_background_tasks {
            assert!(
                output_no_limit.len() > output.len(),
                "Without limit should have more outputs than with limit"
            );
        }
    }

    #[tokio::test]
    async fn test_max_background_tasks_no_truncation_when_under_limit() {
        let file_ids = (0..4).map(|_| FileId::random()).collect::<Vec<_>>();
        let max_background_tasks = 10; // Larger than expected outputs

        // Create files in one window that will generate one compaction output
        let files = [
            new_file_handle_with_sequence(file_ids[0], 0, 999, 0, 1),
            new_file_handle_with_sequence(file_ids[1], 0, 999, 0, 2),
            new_file_handle_with_sequence(file_ids[2], 0, 999, 0, 3),
            new_file_handle_with_sequence(file_ids[3], 0, 999, 0, 4),
        ];

        let windows = assign_to_windows(files.iter(), 3);

        let picker = TwcsPicker {
            trigger_file_num: 4,
            time_window_seconds: Some(3),
            max_output_file_size: None,
            append_mode: false,
            max_background_tasks: Some(max_background_tasks),
            time_range: None,
        };

        let active_window = find_latest_window_in_seconds(files.iter(), 3);
        let output = picker
            .build_output_with_time_range(RegionId::from_u64(123), windows, active_window, None)
            .await
            .unwrap();

        // Should have all outputs since we're under the limit
        assert!(
            output.len() <= max_background_tasks,
            "Output should be within limit"
        );
        // Should have at least one output
        assert!(!output.is_empty(), "Should have at least one output");
    }

    #[tokio::test]
    async fn test_pick_multiple_runs() {
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

        let windows = assign_to_windows(files.iter(), 3);

        let picker = TwcsPicker {
            trigger_file_num: 4,
            time_window_seconds: Some(3),
            max_output_file_size: None,
            append_mode: false,
            max_background_tasks: None,
            time_range: None,
        };

        let active_window = find_latest_window_in_seconds(files.iter(), 3);
        let output = picker
            .build_output_with_time_range(RegionId::from_u64(123), windows, active_window, None)
            .await
            .unwrap();

        assert_eq!(1, output.len());
        assert_eq!(output[0].inputs.len(), num_files);
    }

    #[tokio::test]
    async fn test_limit_max_input_files() {
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

        let windows = assign_to_windows(files.iter(), 3);

        let picker = TwcsPicker {
            trigger_file_num: 4,
            time_window_seconds: Some(3),
            max_output_file_size: None,
            append_mode: false,
            max_background_tasks: None,
            time_range: None,
        };

        let active_window = find_latest_window_in_seconds(files.iter(), 3);
        let output = picker
            .build_output_with_time_range(RegionId::from_u64(123), windows, active_window, None)
            .await
            .unwrap();

        assert_eq!(1, output.len());
        assert_eq!(output[0].inputs.len(), 32);
    }

    #[tokio::test]
    async fn test_limit_max_input_files_keeps_deletion_markers() {
        common_telemetry::init_default_ut_logging();

        // One large file group spanning the whole window plus 32 small ones nested inside
        // it. That is exactly 2 runs, so the window on its own allows filtering deletions.
        let mut files = vec![new_file_handle_with_size_and_sequence(
            FileId::random(),
            0,
            3_000_000,
            0,
            1,
            1024 * 1024 * 1024,
        )];
        files.extend((0..32).map(|idx: i64| {
            new_file_handle_with_size_and_sequence(
                FileId::random(),
                (idx + 1) * 10_000,
                (idx + 1) * 10_000 + 1_000,
                0,
                (idx + 2) as u64,
                1024,
            )
        }));

        let windows = assign_to_windows(files.iter(), 3600);

        let picker = TwcsPicker {
            trigger_file_num: 4,
            time_window_seconds: Some(3600),
            max_output_file_size: None,
            append_mode: false,
            max_background_tasks: None,
            time_range: None,
        };

        let active_window = find_latest_window_in_seconds(files.iter(), 3600);
        let output = picker
            .build_output_with_time_range(RegionId::from_u64(123), windows, active_window, None)
            .await
            .unwrap();

        assert_eq!(1, output.len());
        // The input file num limit picks the smallest groups first, so the large group is
        // left behind while the small groups that overlap it are compacted.
        assert_eq!(32, output[0].inputs.len());
        assert!(
            !output[0].filter_deleted,
            "deletion markers must be kept once the file num limit drops files they may mask"
        );
    }

    #[tokio::test]
    async fn test_limit_max_input_files_still_filters_without_overlap() {
        common_telemetry::init_default_ut_logging();

        // 40 file groups with disjoint time ranges, i.e. a single run. Nothing the file num
        // limit leaves behind can hold a row masked by a deletion marker we compact.
        let files: Vec<_> = (0..40i64)
            .map(|idx| {
                new_file_handle_with_size_and_sequence(
                    FileId::random(),
                    (idx + 1) * 10_000,
                    (idx + 1) * 10_000 + 1_000,
                    0,
                    (idx + 1) as u64,
                    1024,
                )
            })
            .collect();

        let windows = assign_to_windows(files.iter(), 3600);

        let picker = TwcsPicker {
            trigger_file_num: 4,
            time_window_seconds: Some(3600),
            max_output_file_size: Some(1024 * 1024 * 1024),
            append_mode: false,
            max_background_tasks: None,
            time_range: None,
        };

        let active_window = find_latest_window_in_seconds(files.iter(), 3600);
        let output = picker
            .build_output_with_time_range(RegionId::from_u64(123), windows, active_window, None)
            .await
            .unwrap();

        assert_eq!(1, output.len());
        assert_eq!(32, output[0].inputs.len());
        assert!(output[0].filter_deleted);
    }

    #[tokio::test]
    async fn test_newer_windows_have_priority() {
        let older_file_ids = [FileId::random(), FileId::random()];
        let newer_file_ids = [FileId::random(), FileId::random()];
        let files = [
            new_file_handle_with_sequence(older_file_ids[0], 1_000, 1_999, 0, 1),
            new_file_handle_with_sequence(older_file_ids[1], 1_000, 1_999, 0, 2),
            new_file_handle_with_sequence(newer_file_ids[0], 7_000, 7_999, 0, 3),
            new_file_handle_with_sequence(newer_file_ids[1], 7_000, 7_999, 0, 4),
        ];
        let windows = assign_to_windows(files.iter(), 3);
        let picker = TwcsPicker {
            trigger_file_num: 2,
            time_window_seconds: Some(3),
            max_output_file_size: None,
            append_mode: false,
            max_background_tasks: Some(1),
            time_range: None,
        };

        let output = picker
            .build_output_with_time_range(RegionId::from_u64(123), windows, Some(9), None)
            .await
            .unwrap();

        assert_eq!(1, output.len());
        assert_eq!(
            newer_file_ids.into_iter().collect::<HashSet<_>>(),
            output[0]
                .inputs
                .iter()
                .map(|file| file.file_id().file_id())
                .collect::<HashSet<_>>()
        );
    }

    #[test]
    fn test_filter_time_windows_by_time_range() {
        let time_range = TimestampRange::new(
            Timestamp::new_millisecond(1_200),
            Timestamp::new_millisecond(1_800),
        )
        .unwrap();

        assert!(time_window_intersects_range(3, 3, &time_range));
        assert!(!time_window_intersects_range(9, 3, &time_range));

        let boundary_range =
            TimestampRange::new(Timestamp::new_second(0), Timestamp::new_second(3)).unwrap();
        assert!(time_window_intersects_range(0, 3, &boundary_range));
        assert!(time_window_intersects_range(3, 3, &boundary_range));
        assert!(!time_window_intersects_range(6, 3, &boundary_range));

        let overflowing_range = TimestampRange::new(
            Timestamp::new_second(i64::MAX - 1),
            Timestamp::new_second(i64::MAX),
        )
        .unwrap();
        assert!(!time_window_intersects_range(0, 4, &overflowing_range));
    }

    #[tokio::test]
    async fn test_time_range_filter_precedes_background_task_limit() {
        let early_file_ids = [FileId::random(), FileId::random()];
        let selected_file_ids = [FileId::random(), FileId::random()];
        let files = [
            new_file_handle_with_sequence(early_file_ids[0], 1_000, 1_999, 0, 1),
            new_file_handle_with_sequence(early_file_ids[1], 1_000, 1_999, 0, 2),
            new_file_handle_with_sequence(selected_file_ids[0], 7_000, 7_999, 0, 3),
            new_file_handle_with_sequence(selected_file_ids[1], 7_000, 7_999, 0, 4),
        ];
        let windows = assign_to_windows(files.iter(), 3);
        let picker = TwcsPicker {
            trigger_file_num: 2,
            time_window_seconds: Some(3),
            max_output_file_size: None,
            append_mode: false,
            max_background_tasks: Some(1),
            time_range: TimestampRange::new(
                Timestamp::new_millisecond(7_200),
                Timestamp::new_millisecond(7_800),
            ),
        };

        let output = picker
            .build_output_with_time_range(RegionId::from_u64(123), windows, Some(9), Some(3))
            .await
            .unwrap();

        assert_eq!(1, output.len());
        assert_eq!(
            selected_file_ids.into_iter().collect::<HashSet<_>>(),
            output[0]
                .inputs
                .iter()
                .map(|file| file.file_id().file_id())
                .collect::<HashSet<_>>()
        );
    }

    #[tokio::test]
    async fn test_count_first_prefers_more_files_over_smaller_overlap() {
        let files = [
            new_file_handle_with_size_and_sequence(FileId::random(), 0, 10, 0, 1, 10),
            new_file_handle_with_size_and_sequence(FileId::random(), 20, 30, 0, 2, 10),
            new_file_handle_with_size_and_sequence(FileId::random(), 40, 50, 0, 3, 10),
            new_file_handle_with_size_and_sequence(FileId::random(), 5, 15, 0, 4, 10),
        ];
        let windows = assign_to_windows(files.iter(), 100);
        let picker = TwcsPicker {
            trigger_file_num: 2,
            time_window_seconds: Some(100),
            max_output_file_size: None,
            append_mode: false,
            max_background_tasks: None,
            time_range: None,
        };

        let output = picker
            .build_output_with_time_range(RegionId::from_u64(1), windows, Some(0), None)
            .await
            .unwrap();

        assert_eq!(1, output.len());
        assert_eq!(4, output[0].inputs.len());
    }

    #[tokio::test]
    async fn test_count_first_trigger_counts_file_groups() {
        let files = [
            new_file_handle_with_size_and_sequence(FileId::random(), 0, 10, 0, 1, 10),
            new_file_handle_with_size_and_sequence(FileId::random(), 0, 10, 0, 1, 10),
            new_file_handle_with_size_and_sequence(FileId::random(), 20, 30, 0, 2, 10),
        ];
        let windows = assign_to_windows(files.iter(), 100);
        let picker = TwcsPicker {
            trigger_file_num: 3,
            time_window_seconds: Some(100),
            max_output_file_size: None,
            append_mode: false,
            max_background_tasks: None,
            time_range: None,
        };

        let output = picker
            .build_output_with_time_range(RegionId::from_u64(1), windows, Some(0), None)
            .await
            .unwrap();

        assert!(output.is_empty());
    }

    #[tokio::test]
    async fn test_count_first_does_not_compact_overlap_below_trigger() {
        let files = [
            new_file_handle_with_size_and_sequence(FileId::random(), 0, 20, 0, 1, 10),
            new_file_handle_with_size_and_sequence(FileId::random(), 10, 30, 0, 2, 10),
        ];
        let windows = assign_to_windows(files.iter(), 100);
        let picker = TwcsPicker {
            trigger_file_num: 3,
            time_window_seconds: Some(100),
            max_output_file_size: None,
            append_mode: false,
            max_background_tasks: None,
            time_range: None,
        };

        let output = picker
            .build_output_with_time_range(RegionId::from_u64(1), windows, Some(0), None)
            .await
            .unwrap();

        assert!(output.is_empty());
    }

    #[tokio::test]
    async fn test_filter_deleted_is_false_when_selected_files_overlap_unselected_file() {
        let mut files = (0..32)
            .map(|idx| {
                new_file_handle_with_size_and_sequence(
                    FileId::random(),
                    idx * 10,
                    idx * 10 + 9,
                    0,
                    idx as u64 + 1,
                    10,
                )
            })
            .collect::<Vec<_>>();
        files.push(new_file_handle_with_size_and_sequence(
            FileId::random(),
            0,
            320,
            0,
            33,
            10,
        ));
        let windows = assign_to_windows(files.iter(), 1000);
        let picker = TwcsPicker {
            trigger_file_num: 2,
            time_window_seconds: Some(1000),
            max_output_file_size: None,
            append_mode: false,
            max_background_tasks: None,
            time_range: None,
        };

        let output = picker
            .build_output_with_time_range(RegionId::from_u64(1), windows, Some(0), None)
            .await
            .unwrap();

        assert_eq!(1, output.len());
        assert_eq!(MAX_INPUT_FILE_GROUPS, output[0].inputs.len());
        assert!(!output[0].filter_deleted);
    }

    #[tokio::test]
    async fn test_filter_deleted_is_false_when_selected_overlaps_ignored_file_group() {
        let files = [
            new_file_handle_with_size_and_sequence(FileId::random(), 0, 100, 0, 1, 10),
            new_file_handle_with_size_and_sequence(FileId::random(), 0, 100, 0, 1, 10),
            new_file_handle_with_size_and_sequence(FileId::random(), 0, 10, 0, 2, 10),
            new_file_handle_with_size_and_sequence(FileId::random(), 20, 30, 0, 3, 10),
            new_file_handle_with_size_and_sequence(FileId::random(), 40, 50, 0, 4, 10),
            new_file_handle_with_size_and_sequence(FileId::random(), 60, 70, 0, 5, 10),
        ];
        let windows = assign_to_windows(files.iter(), 100);
        let picker = TwcsPicker {
            trigger_file_num: 4,
            time_window_seconds: Some(100),
            max_output_file_size: None,
            append_mode: false,
            max_background_tasks: None,
            time_range: None,
        };

        let output = picker
            .build_output_with_time_range(RegionId::from_u64(1), windows, Some(0), None)
            .await
            .unwrap();

        assert_eq!(1, output.len());
        assert_eq!(4, output[0].inputs.len());
        assert!(!output[0].filter_deleted);
    }

    fn new_file_group(
        start: i64,
        end: i64,
        sequence: u64,
        num_files: usize,
        file_size: u64,
    ) -> FileGroup {
        let mut group = FileGroup::new_with_file(new_file_handle_with_size_and_sequence(
            FileId::random(),
            start,
            end,
            0,
            sequence,
            file_size,
        ));
        for _ in 1..num_files {
            group.add_file(new_file_handle_with_size_and_sequence(
                FileId::random(),
                start,
                end,
                0,
                sequence,
                file_size,
            ));
        }
        group
    }

    fn picked_ranges(groups: &[FileGroup]) -> Vec<(i64, i64)> {
        groups
            .iter()
            .map(|group| {
                let (start, end) = group.range();
                (start.value(), end.value())
            })
            .collect()
    }

    #[test]
    fn test_count_first_counts_file_groups_not_physical_ssts() {
        let picked = pick_count_first(
            vec![SortedRun::from(vec![
                new_file_group(0, 9, 1, 2, 10),
                new_file_group(20, 29, 2, 1, 10),
                new_file_group(40, 49, 3, 1, 10),
            ])],
            2,
        );

        assert_eq!(vec![(20, 29), (40, 49)], picked_ranges(&picked));
    }

    #[test]
    fn test_count_first_ignores_multi_sst_groups_between_singletons() {
        let picked = pick_count_first(
            vec![SortedRun::from(vec![
                new_file_group(0, 9, 1, 1, 10),
                new_file_group(10, 19, 2, 2, 10),
                new_file_group(20, 29, 3, 1, 10),
            ])],
            2,
        );

        assert_eq!(vec![(0, 9), (20, 29)], picked_ranges(&picked));
    }

    #[test]
    fn test_count_first_returns_empty_for_only_multi_sst_groups() {
        let picked = pick_count_first(
            vec![SortedRun::from(vec![
                new_file_group(0, 9, 1, 2, 10),
                new_file_group(20, 29, 2, 2, 10),
            ])],
            2,
        );

        assert!(picked.is_empty());
    }

    #[test]
    fn test_count_first_rejects_dominant_historical_group() {
        let picked = pick_count_first(
            vec![SortedRun::from(vec![
                new_file_group(0, 9, 1, 1, 400),
                new_file_group(20, 29, 2, 1, 100),
                new_file_group(40, 49, 3, 1, 100),
                new_file_group(60, 69, 4, 1, 100),
            ])],
            4,
        );

        assert!(picked.is_empty());
    }

    #[test]
    fn test_count_first_accepts_balanced_historical_group() {
        let picked = pick_count_first(
            vec![SortedRun::from(vec![
                new_file_group(0, 9, 1, 1, 400),
                new_file_group(20, 29, 2, 1, 100),
                new_file_group(40, 49, 3, 1, 100),
                new_file_group(60, 69, 4, 1, 100),
                new_file_group(80, 89, 5, 1, 100),
            ])],
            4,
        );

        assert_eq!(
            vec![(0, 9), (20, 29), (40, 49), (60, 69), (80, 89)],
            picked_ranges(&picked)
        );
    }

    #[test]
    fn test_count_first_finds_smaller_balanced_interval_when_larger_one_is_unbalanced() {
        let picked = pick_count_first(
            vec![SortedRun::from(vec![
                new_file_group(0, 9, 1, 1, 1000),
                new_file_group(20, 29, 2, 1, 100),
                new_file_group(40, 49, 3, 1, 100),
                new_file_group(60, 69, 4, 1, 100),
                new_file_group(80, 89, 5, 1, 100),
            ])],
            4,
        );

        assert_eq!(
            vec![(20, 29), (40, 49), (60, 69), (80, 89)],
            picked_ranges(&picked)
        );
    }

    #[test]
    fn test_count_first_prefers_overlap_participants_when_file_group_counts_match() {
        let first_run = (0..MAX_INPUT_FILE_GROUPS)
            .map(|idx| {
                let start = idx as i64 * 20;
                let end = if idx + 1 == MAX_INPUT_FILE_GROUPS {
                    700
                } else {
                    start + 9
                };
                new_file_group(start, end, idx as u64 + 1, 1, 10)
            })
            .collect::<Vec<_>>();
        let overlapping = new_file_group(690, 710, 100, 1, 10);

        let picked = pick_count_first(
            vec![
                SortedRun::from(first_run),
                SortedRun::from(vec![overlapping]),
            ],
            2,
        );
        let ranges = picked_ranges(&picked);

        assert_eq!(MAX_INPUT_FILE_GROUPS, ranges.len());
        assert!(!ranges.contains(&(0, 9)));
        assert!(ranges.contains(&(690, 710)));
    }

    #[test]
    fn test_count_first_prefers_smaller_bytes_when_file_group_counts_match() {
        let groups = (0..=MAX_INPUT_FILE_GROUPS)
            .map(|idx| {
                let start = idx as i64 * 20;
                let size = if idx == 0 {
                    100
                } else if idx == MAX_INPUT_FILE_GROUPS {
                    1
                } else {
                    10
                };
                new_file_group(start, start + 9, idx as u64 + 1, 1, size)
            })
            .collect::<Vec<_>>();

        let picked = pick_count_first(vec![SortedRun::from(groups)], 2);
        let ranges = picked_ranges(&picked);

        assert_eq!(MAX_INPUT_FILE_GROUPS, ranges.len());
        assert_eq!(Some(&(20, 29)), ranges.first());
        assert_eq!(Some(&(640, 649)), ranges.last());
    }

    #[test]
    fn test_count_first_prefers_earlier_time_on_exact_tie() {
        let groups = (0..=MAX_INPUT_FILE_GROUPS)
            .map(|idx| {
                let start = idx as i64 * 20;
                new_file_group(start, start + 9, idx as u64 + 1, 1, 10)
            })
            .collect::<Vec<_>>();

        let picked = pick_count_first(vec![SortedRun::from(groups)], 2);
        let ranges = picked_ranges(&picked);

        assert_eq!(MAX_INPUT_FILE_GROUPS, ranges.len());
        assert_eq!(Some(&(0, 9)), ranges.first());
        assert_eq!(Some(&(620, 629)), ranges.last());
    }

    #[test]
    fn test_count_first_keeps_each_interleaved_run_contiguous() {
        let picked = pick_count_first(
            vec![
                SortedRun::from(vec![
                    new_file_group(0, 9, 1, 1, 1),
                    new_file_group(20, 29, 2, 1, 1),
                    new_file_group(40, 49, 3, 1, 1),
                ]),
                SortedRun::from(vec![
                    new_file_group(10, 19, 4, 1, 1),
                    new_file_group(30, 39, 5, 1, 1),
                ]),
            ],
            2,
        );

        assert_eq!(
            vec![(0, 9), (10, 19), (20, 29), (30, 39), (40, 49)],
            picked_ranges(&picked)
        );
    }

    // TODO(hl): TTL tester that checks if get_expired_ssts function works as expected.
}
