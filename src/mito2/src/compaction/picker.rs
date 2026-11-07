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

use std::fmt::Debug;
use std::sync::Arc;

use api::v1::region::compact_request;
use common_time::range::TimestampRange;
use common_time::{TimeToLive, Timestamp};
use serde::{Deserialize, Serialize};

use crate::compaction::compactor::CompactionRegion;
use crate::compaction::last_non_null::LastNonNullPicker;
use crate::compaction::twcs::TwcsPicker;
use crate::compaction::window::WindowedCompactionPicker;
use crate::compaction::{CompactionOutput, SerializedCompactionOutput};
use crate::error::Result;
use crate::region::options::{CompactionOptions, MergeMode, RegionOptions};
use crate::sst::file::{FileHandle, FileMeta, LEVEL_SHARED, overlaps};
use crate::sst::file_purger::FilePurger;
use crate::sst::version::LevelMeta;

/// Picker picks input SST files for compaction.
/// Different compaction strategy may implement different pickers.
#[async_trait::async_trait]
pub trait Picker: Debug + Send + Sync + 'static {
    /// Picks input SST files for compaction.
    async fn pick(&self, compaction_region: &CompactionRegion) -> Result<Option<PickerOutput>>;
}

/// PickerOutput is the output of a [`Picker`].
/// It contains the outputs of the compaction and the expired SST files.
#[derive(Default, Clone, Debug)]
pub struct PickerOutput {
    pub outputs: Vec<CompactionOutput>,
    pub expired_ssts: Vec<FileHandle>,
    pub time_window_size: i64,
    /// Max single output file size in bytes.
    pub max_file_size: Option<usize>,
}

/// SerializedPickerOutput is a serialized version of PickerOutput by replacing [CompactionOutput] and [FileHandle] with [SerializedCompactionOutput] and [FileMeta].
#[derive(Default, Clone, Debug, Serialize, Deserialize)]
pub struct SerializedPickerOutput {
    pub outputs: Vec<SerializedCompactionOutput>,
    pub expired_ssts: Vec<FileMeta>,
    pub time_window_size: i64,
    pub max_file_size: Option<usize>,
}

impl From<&PickerOutput> for SerializedPickerOutput {
    fn from(input: &PickerOutput) -> Self {
        let outputs = input
            .outputs
            .iter()
            .map(|output| SerializedCompactionOutput {
                output_level: output.output_level,
                inputs: output.inputs.iter().map(|s| s.meta_ref().clone()).collect(),
                filter_deleted: output.filter_deleted,
                output_time_range: output.output_time_range,
            })
            .collect();
        let expired_ssts = input
            .expired_ssts
            .iter()
            .map(|s| s.meta_ref().clone())
            .collect();
        Self {
            outputs,
            expired_ssts,
            time_window_size: input.time_window_size,
            max_file_size: input.max_file_size,
        }
    }
}

impl PickerOutput {
    /// Converts a [SerializedPickerOutput] to a [PickerOutput]. File statistics
    /// retain their original encoding; comparisons supply their own schema context.
    pub fn from_serialized(
        input: SerializedPickerOutput,
        file_purger: Arc<dyn FilePurger>,
    ) -> Self {
        let outputs = input
            .outputs
            .into_iter()
            .map(|output| CompactionOutput {
                output_level: output.output_level,
                inputs: output
                    .inputs
                    .into_iter()
                    .map(|file_meta| FileHandle::new(file_meta, file_purger.clone()))
                    .collect(),
                filter_deleted: output.filter_deleted,
                output_time_range: output.output_time_range,
            })
            .collect();

        let expired_ssts = input
            .expired_ssts
            .into_iter()
            .map(|file_meta| FileHandle::new(file_meta, file_purger.clone()))
            .collect();

        Self {
            outputs,
            expired_ssts,
            time_window_size: input.time_window_size,
            max_file_size: input.max_file_size,
        }
    }
}

/// Creates a picker for the request and the region's compaction and merge modes.
pub fn new_picker(
    compact_request_options: &compact_request::Options,
    region_options: &RegionOptions,
    max_background_tasks: Option<usize>,
    time_range: Option<TimestampRange>,
    manual: bool,
) -> Arc<dyn Picker> {
    if let compact_request::Options::StrictWindow(window) = compact_request_options {
        let window = if window.window_seconds == 0 {
            None
        } else {
            Some(window.window_seconds)
        };
        // Strict-window outputs already include every overlapping file segment
        // within each disjoint, half-open output range, including LastNonNull.
        Arc::new(WindowedCompactionPicker::new(window).with_time_range(time_range)) as Arc<_>
    } else {
        let picker = new_twcs_picker(region_options, max_background_tasks, time_range, manual);
        if region_options.merge_mode() == MergeMode::LastNonNull {
            // LastNonNull correctness spans windows and levels, so wrap the seed
            // picker instead of trusting TWCS's independent windows.
            Arc::new(LastNonNullPicker::new(picker))
        } else {
            Arc::new(picker)
        }
    }
}

/// Builds the stateless TWCS strategy used for discovery and legacy picks.
pub(crate) fn new_twcs_picker(
    region_options: &RegionOptions,
    max_background_tasks: Option<usize>,
    time_range: Option<TimestampRange>,
    manual: bool,
) -> TwcsPicker {
    match &region_options.compaction {
        CompactionOptions::Twcs(twcs_opts) => TwcsPicker {
            trigger_file_num: twcs_opts.active_window_trigger_file_num,
            active_window_l1_merge_trigger: twcs_opts.active_window_l1_merge_trigger,
            inactive_window_trigger_file_num: twcs_opts.inactive_window_trigger_file_num,
            inactive_window_l1_merge_trigger: twcs_opts.inactive_window_l1_merge_trigger,
            time_window_seconds: twcs_opts.time_window_seconds(),
            max_output_file_size: twcs_opts.max_output_file_size.map(|r| r.as_bytes()),
            append_mode: region_options.append_mode,
            merge_mode: region_options.merge_mode(),
            max_background_tasks,
            time_range,
            all_windows_inactive: manual,
            metrics: Default::default(),
        },
    }
}

/// Finds all expired SSTs across levels.
pub(super) fn get_expired_ssts(
    levels: &[LevelMeta],
    ttl: Option<TimeToLive>,
    now: Timestamp,
) -> Vec<FileHandle> {
    let Some(ttl) = ttl else {
        return vec![];
    };

    levels
        .iter()
        .flat_map(|l| l.get_expired_files(&now, &ttl).into_iter())
        // Expiring a tombstone while an overlapping shared source remains
        // query-visible can resurrect its old value. The table path owns that
        // dependency; retain the ordinary SST until the shared source is gone.
        .filter(|expired| {
            !levels
                .iter()
                .filter(|level| level.level == LEVEL_SHARED)
                .flat_map(LevelMeta::files)
                .any(|shared| overlaps(&expired.time_range(), &shared.time_range()))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use store_api::storage::FileId;

    use super::*;
    use crate::compaction::test_util::new_file_handle;
    use crate::test_util::new_noop_file_purger;

    #[tokio::test]
    async fn shared_inputs_are_not_picked_but_still_block_last_non_null_folding() {
        use crate::compaction::test_util::compaction_region_with_ssts;

        for strict_window in [false, true] {
            for merge_mode in [MergeMode::LastRow, MergeMode::LastNonNull] {
                let mut files: Vec<_> = (0..4)
                    .map(|_| {
                        new_file_handle(FileId::random(), 0, 1000, 0)
                            .meta_ref()
                            .clone()
                    })
                    .collect();
                files.push(
                    new_file_handle(FileId::random(), 0, 1000, LEVEL_SHARED)
                        .meta_ref()
                        .clone(),
                );
                let mut region =
                    compaction_region_with_ssts(files, std::time::Duration::from_secs(3600)).await;
                region.ttl = None;
                region.region_options.merge_mode = Some(merge_mode);
                region.current_version.options = region.region_options.clone();
                let options = if strict_window {
                    compact_request::Options::StrictWindow(Default::default())
                } else {
                    compact_request::Options::Regular(Default::default())
                };
                let picked = new_picker(&options, &region.region_options, None, None, true)
                    .pick(&region)
                    .await
                    .unwrap();
                let outputs = picked.map(|p| p.outputs).unwrap_or_default();
                if merge_mode == MergeMode::LastNonNull {
                    assert!(outputs.is_empty(), "cannot fold across a shared dependency");
                } else {
                    assert!(!outputs.is_empty(), "ordinary files remain eligible");
                    assert!(outputs.iter().all(|output| !output.filter_deleted));
                    assert!(
                        outputs
                            .iter()
                            .flat_map(|output| &output.inputs)
                            .all(|file| file.level() != LEVEL_SHARED)
                    );
                }
            }
        }
    }

    #[test]
    fn ttl_retains_shared_sources_and_overlapping_tombstone_files() {
        let mut levels = [
            LevelMeta::new(0),
            LevelMeta::new(1),
            LevelMeta::new(LEVEL_SHARED),
        ];
        let shared = new_file_handle(FileId::random(), 0, 1000, LEVEL_SHARED);
        let overlapping = new_file_handle(FileId::random(), 500, 1500, 1);
        let independent = new_file_handle(FileId::random(), 2000, 3000, 0);
        for file in [&shared, &overlapping, &independent] {
            levels[file.level() as usize]
                .files
                .insert(file.meta_ref().file_id, file.clone());
        }
        let expired = get_expired_ssts(
            &levels,
            Some(std::time::Duration::from_secs(1).into()),
            Timestamp::new_second(100),
        );
        assert_eq!(
            vec![independent.file_id()],
            expired.iter().map(FileHandle::file_id).collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_picker_output_serialization() {
        let inputs_file_handle = vec![
            new_file_handle(FileId::random(), 0, 999, 0),
            new_file_handle(FileId::random(), 0, 999, 0),
            new_file_handle(FileId::random(), 0, 999, 0),
        ];
        let expired_ssts_file_handle = vec![
            new_file_handle(FileId::random(), 0, 999, 0),
            new_file_handle(FileId::random(), 0, 999, 0),
        ];

        let picker_output = PickerOutput {
            outputs: vec![
                CompactionOutput {
                    output_level: 0,
                    inputs: inputs_file_handle.clone(),
                    filter_deleted: false,
                    output_time_range: None,
                },
                CompactionOutput {
                    output_level: 0,
                    inputs: inputs_file_handle.clone(),
                    filter_deleted: false,
                    output_time_range: None,
                },
            ],
            expired_ssts: expired_ssts_file_handle.clone(),
            time_window_size: 1000,
            max_file_size: None,
        };

        let picker_output_str =
            serde_json::to_string(&SerializedPickerOutput::from(&picker_output)).unwrap();
        let serialized_picker_output: SerializedPickerOutput =
            serde_json::from_str(&picker_output_str).unwrap();
        let picker_output_from_serialized =
            PickerOutput::from_serialized(serialized_picker_output, new_noop_file_purger());

        picker_output
            .expired_ssts
            .iter()
            .zip(picker_output_from_serialized.expired_ssts.iter())
            .for_each(|(expected, actual)| {
                assert_eq!(expected.meta_ref(), actual.meta_ref());
            });

        picker_output
            .outputs
            .iter()
            .zip(picker_output_from_serialized.outputs.iter())
            .for_each(|(expected, actual)| {
                assert_eq!(expected.output_level, actual.output_level);
                expected
                    .inputs
                    .iter()
                    .zip(actual.inputs.iter())
                    .for_each(|(expected, actual)| {
                        assert_eq!(expected.meta_ref(), actual.meta_ref());
                    });
                assert_eq!(expected.filter_deleted, actual.filter_deleted);
                assert_eq!(expected.output_time_range, actual.output_time_range);
            });
    }
}
