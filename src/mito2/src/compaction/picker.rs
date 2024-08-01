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
use serde::{Deserialize, Serialize};

use crate::compaction::compactor::CompactionRegion;
use crate::compaction::twcs::TwcsPicker;
use crate::compaction::window::WindowedCompactionPicker;
use crate::compaction::{CompactionOutput, SerializedCompactionOutput};
use crate::region::options::CompactionOptions;
use crate::sst::file::{FileHandle, FileMeta};
use crate::sst::file_purger::FilePurger;

#[async_trait::async_trait]
pub(crate) trait CompactionTask: Debug + Send + Sync + 'static {
    async fn run(&mut self);
}

/// Picker picks input SST files for compaction.
/// Different compaction strategy may implement different pickers.
pub trait Picker: Debug + Send + Sync + 'static {
    /// Picks input SST files for compaction.
    fn pick(&self, compaction_region: &CompactionRegion) -> Option<PickerOutput>;
}

/// PickerOutput is the output of a [`Picker`].
/// It contains the outputs of the compaction and the expired SST files.
#[derive(Default, Clone, Debug)]
pub struct PickerOutput {
    pub outputs: Vec<CompactionOutput>,
    pub expired_ssts: Vec<FileHandle>,
    pub time_window_size: i64,
}

/// SerializedPickerOutput is a serialized version of PickerOutput by replacing [CompactionOutput] and [FileHandle] with [SerializedCompactionOutput] and [FileMeta].
#[derive(Default, Clone, Debug, Serialize, Deserialize)]
pub struct SerializedPickerOutput {
    pub outputs: Vec<SerializedCompactionOutput>,
    pub expired_ssts: Vec<FileMeta>,
    pub time_window_size: i64,
}

impl From<&PickerOutput> for SerializedPickerOutput {
    fn from(input: &PickerOutput) -> Self {
        let outputs = input
            .outputs
            .iter()
            .map(|output| SerializedCompactionOutput {
                output_file_id: output.output_file_id,
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
        }
    }
}

impl PickerOutput {
    /// Converts a [SerializedPickerOutput] to a [PickerOutput].
    pub fn from_serialized(
        input: SerializedPickerOutput,
        file_purger: Arc<dyn FilePurger>,
    ) -> Self {
        let outputs = input
            .outputs
            .into_iter()
            .map(|output| CompactionOutput {
                output_file_id: output.output_file_id,
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
        }
    }
}

/// Create a new picker based on the compaction request options and compaction options.
pub fn new_picker(
    compact_request_options: compact_request::Options,
    compaction_options: &CompactionOptions,
) -> Arc<dyn Picker> {
    if let compact_request::Options::StrictWindow(window) = &compact_request_options {
        let window = if window.window_seconds == 0 {
            None
        } else {
            Some(window.window_seconds)
        };
        Arc::new(WindowedCompactionPicker::new(window)) as Arc<_>
    } else {
        match compaction_options {
            CompactionOptions::Twcs(twcs_opts) => Arc::new(TwcsPicker::new(
                twcs_opts.max_active_window_runs,
                twcs_opts.max_active_window_files,
                twcs_opts.max_inactive_window_runs,
                twcs_opts.max_inactive_window_files,
                twcs_opts.time_window_seconds(),
            )) as Arc<_>,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compaction::test_util::new_file_handle;
    use crate::sst::file::FileId;
    use crate::test_util::new_noop_file_purger;

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
                    output_file_id: FileId::random(),
                    output_level: 0,
                    inputs: inputs_file_handle.clone(),
                    filter_deleted: false,
                    output_time_range: None,
                },
                CompactionOutput {
                    output_file_id: FileId::random(),
                    output_level: 0,
                    inputs: inputs_file_handle.clone(),
                    filter_deleted: false,
                    output_time_range: None,
                },
            ],
            expired_ssts: expired_ssts_file_handle.clone(),
            time_window_size: 1000,
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
                assert_eq!(expected.output_file_id, actual.output_file_id);
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
