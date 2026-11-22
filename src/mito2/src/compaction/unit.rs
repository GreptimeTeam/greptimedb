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

use std::collections::{BTreeMap, HashMap, HashSet};

use crate::batch_size::estimate_batch_size;
use crate::compaction::CompactionOutput;
use crate::compaction::picker::PickerOutput;
use crate::read::scan_util::compute_parallel_channel_size;
use crate::sst::file::{FileHandle, FileMeta};
use crate::sst::parquet::DEFAULT_READ_BATCH_SIZE;

/// A complete input dependency group, merged and published atomically.
///
/// Logical outputs retain their original input sets and time bounds. In particular,
/// splitting physical SSTs must never split a strict-window input dependency group.
#[derive(Debug, Clone)]
pub(crate) struct CompactionUnit {
    /// Distinct input SSTs reserved and removed together.
    pub(crate) inputs: Vec<FileHandle>,
    /// Original logical outputs; empty for expiration-only maintenance.
    pub(crate) outputs: Vec<CompactionOutput>,
    /// Compaction window size in seconds.
    pub(crate) time_window_size: i64,
    /// Maximum physical output SST size in bytes.
    pub(crate) max_file_size: Option<usize>,
}

impl CompactionUnit {
    /// Groups outputs by the transitive closure of shared input FileIds.
    /// Returns `None` for an inconsistent plan that both expires and merges a file.
    /// Expiration maintenance runs before merge units, matching the legacy pre-merge removal.
    /// Merge units follow the legacy compactor's tail-first execution order; a shared-input
    /// group inherits the position of its highest-priority output.
    pub(crate) fn from_picker(output: PickerOutput) -> Option<Vec<Self>> {
        let PickerOutput {
            outputs,
            expired_ssts,
            time_window_size,
            max_file_size,
        } = output;
        let mut parents: Vec<_> = (0..outputs.len()).collect();
        let mut owners = HashMap::new();
        for (index, output) in outputs.iter().enumerate() {
            for file in &output.inputs {
                if let Some(&previous) = owners.get(&file.file_id()) {
                    let a = root(&mut parents, previous);
                    let b = root(&mut parents, index);
                    // Higher output indices are consumed first by the legacy compactor.
                    parents[a.min(b)] = a.max(b);
                } else {
                    owners.insert(file.file_id(), index);
                }
            }
        }
        if expired_ssts
            .iter()
            .any(|f| owners.contains_key(&f.file_id()))
        {
            return None;
        }

        let mut groups: BTreeMap<usize, Vec<CompactionOutput>> = BTreeMap::new();
        for (index, output) in outputs.into_iter().enumerate() {
            groups
                .entry(root(&mut parents, index))
                .or_default()
                .push(output);
        }
        let mut units = Vec::with_capacity(groups.len() + 1);
        if !expired_ssts.is_empty() {
            let mut seen = HashSet::new();
            units.push(Self {
                inputs: expired_ssts
                    .into_iter()
                    .filter(|f| seen.insert(f.file_id()))
                    .collect(),
                outputs: Vec::new(),
                time_window_size,
                max_file_size,
            });
        }
        for outputs in groups.into_values().rev() {
            let mut seen = HashSet::new();
            let inputs: Vec<_> = outputs
                .iter()
                .flat_map(|o| &o.inputs)
                .filter(|f| seen.insert(f.file_id()))
                .cloned()
                .collect();
            if inputs.is_empty() {
                continue;
            }
            units.push(Self {
                inputs,
                outputs,
                time_window_size,
                max_file_size,
            });
        }
        Some(units)
    }

    /// Estimates merge work from distinct input bytes. Expiration-only units do
    /// not read SSTs; admission assigns them its minimum positive service charge.
    pub(crate) fn estimated_input_bytes(&self) -> u64 {
        if self.outputs.is_empty() {
            return 0;
        }
        self.inputs
            .iter()
            .fold(0u64, |total, file| total.saturating_add(file.size()))
    }

    /// Estimates merge memory once per input, even when several outputs read it.
    pub(crate) fn estimated_memory_bytes(&self) -> u64 {
        if self.outputs.is_empty() {
            return 0;
        }
        self.inputs.iter().fold(0u64, |total, file| {
            total.saturating_add(input_memory_bytes(file.meta_ref()))
        })
    }
}

/// Estimates the memory compaction needs to read `meta`: its largest row group, or the batches
/// its reader holds ahead of the merge when those are larger.
///
/// Compaction reads each input in its own task. The task buffers a channel of batches and one
/// more batch it is sending, and the merge holds one batch of every input. Inputs with small row
/// groups can buffer more than one row group, up to the whole file.
fn input_memory_bytes(meta: &FileMeta) -> u64 {
    let row_group_bytes = meta.max_row_group_uncompressed_size;
    if meta.num_rows == 0 || meta.num_row_groups == 0 {
        return row_group_bytes;
    }
    // File metadata keeps no per-row-group row counts, so the row width is approximated from
    // the largest row group and the average row count.
    let rows_per_row_group = (meta.num_rows / meta.num_row_groups).max(1);
    let row_width = row_group_bytes.div_ceil(rows_per_row_group);
    // Compaction picks one batch size for all inputs from the widest row, so batches of this
    // input are at most this many rows.
    let batch_rows = estimate_batch_size([(rows_per_row_group, row_group_bytes)]) as u64;
    let read_ahead_batches = compute_parallel_channel_size(DEFAULT_READ_BATCH_SIZE) as u64 + 2;
    let read_ahead_bytes = read_ahead_batches
        .saturating_mul(batch_rows)
        .saturating_mul(row_width)
        .min(row_group_bytes.saturating_mul(meta.num_row_groups));
    row_group_bytes.max(read_ahead_bytes)
}

/// Finds a dependency group's representative while shortening the parent chain.
fn root(parents: &mut [usize], mut index: usize) -> usize {
    while parents[index] != index {
        parents[index] = parents[parents[index]];
        index = parents[index];
    }
    index
}

#[cfg(test)]
mod tests {
    use store_api::storage::FileId;

    use super::*;
    use crate::compaction::test_util::{new_file_handle, new_file_handle_with_size_and_sequence};

    /// Builds an L1 output that preserves deletion markers for grouping tests.
    fn output(inputs: Vec<FileHandle>) -> CompactionOutput {
        CompactionOutput {
            inputs,
            output_level: 1,
            filter_deleted: false,
            output_time_range: None,
        }
    }

    #[test]
    fn test_compaction_unit_transitive_inputs() {
        let files: Vec<_> = (0..3)
            .map(|i| {
                new_file_handle_with_size_and_sequence(FileId::random(), 0, 100, 0, 0, (i + 1) * 10)
            })
            .collect();
        let units = CompactionUnit::from_picker(PickerOutput {
            outputs: vec![
                output(vec![files[0].clone()]),
                output(vec![files[2].clone()]),
                output(vec![files[1].clone()]),
                output(vec![files[0].clone(), files[1].clone()]),
            ],
            time_window_size: 100,
            max_file_size: Some(1024),
            ..Default::default()
        })
        .unwrap();
        assert_eq!(2, units.len());
        assert_eq!(3, units[0].outputs.len());
        assert_eq!(2, units[0].inputs.len());
        assert_eq!(files[2].file_id(), units[1].inputs[0].file_id());
        assert_eq!(30, units[0].estimated_input_bytes());
        assert_eq!(30, units[1].estimated_input_bytes());
        assert_eq!(Some(1024), units[0].max_file_size);
        assert_eq!(100, units[1].time_window_size);
    }

    #[test]
    fn test_compaction_unit_group_inherits_highest_output_priority() {
        let files: Vec<_> = (0..3)
            .map(|_| new_file_handle(FileId::random(), 0, 100, 0))
            .collect();
        let units = CompactionUnit::from_picker(PickerOutput {
            outputs: [0, 1, 1, 2, 0]
                .into_iter()
                .map(|index| output(vec![files[index].clone()]))
                .collect(),
            ..Default::default()
        })
        .unwrap();
        assert_eq!(
            vec![files[0].file_id(), files[2].file_id(), files[1].file_id()],
            units
                .iter()
                .map(|unit| unit.inputs[0].file_id())
                .collect::<Vec<_>>()
        );
        assert_eq!(
            vec![2, 1, 2],
            units
                .iter()
                .map(|unit| unit.outputs.len())
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_compaction_unit_expiration_precedes_merge_units() {
        let files: Vec<_> = (0..3)
            .map(|_| new_file_handle(FileId::random(), 0, 100, 0))
            .collect();
        let units = CompactionUnit::from_picker(PickerOutput {
            outputs: vec![
                output(vec![files[0].clone()]),
                output(vec![files[1].clone()]),
            ],
            expired_ssts: vec![files[2].clone()],
            ..Default::default()
        })
        .unwrap();

        assert_eq!(3, units.len());
        assert!(units[0].outputs.is_empty());
        assert_eq!(files[2].file_id(), units[0].inputs[0].file_id());
        assert_eq!(files[1].file_id(), units[1].inputs[0].file_id());
        assert_eq!(files[0].file_id(), units[2].inputs[0].file_id());
    }

    #[test]
    fn test_input_memory_covers_read_ahead_of_small_row_groups() {
        const MIB: u64 = 1024 * 1024;
        let meta = |row_group_bytes: u64, rows_per_row_group: u64, num_row_groups: u64| FileMeta {
            max_row_group_uncompressed_size: row_group_bytes,
            num_rows: rows_per_row_group * num_row_groups,
            num_row_groups,
            ..Default::default()
        };

        // 8192-row batches of 512-byte rows are 4 MiB, far below a 50 MiB row group.
        assert_eq!(
            50 * MIB,
            input_memory_bytes(&meta(50 * MIB, 100 * 1024, 10))
        );
        // 64 MiB batches of 1 MiB rows: the reader holds 4 batches, not one 4 MiB row group.
        assert_eq!(256 * MIB, input_memory_bytes(&meta(4 * MIB, 4, 1000)));
        // Read-ahead never exceeds the whole file.
        assert_eq!(40 * MIB, input_memory_bytes(&meta(4 * MIB, 4, 10)));
        // Without row counts, only the row group is known.
        let unknown_rows = FileMeta {
            max_row_group_uncompressed_size: 4 * MIB,
            ..Default::default()
        };
        assert_eq!(4 * MIB, input_memory_bytes(&unknown_rows));
    }

    #[test]
    fn test_compaction_unit_expiration_inputs() {
        let file = new_file_handle(FileId::random(), 0, 100, 0);
        assert!(
            CompactionUnit::from_picker(PickerOutput {
                outputs: vec![output(vec![file.clone()])],
                expired_ssts: vec![file.clone()],
                ..Default::default()
            })
            .is_none()
        );
        let units = CompactionUnit::from_picker(PickerOutput {
            expired_ssts: vec![file.clone(), file],
            ..Default::default()
        })
        .unwrap();
        assert_eq!(1, units.len());
        assert_eq!(1, units[0].inputs.len());
        assert!(units[0].outputs.is_empty());
        assert_eq!(0, units[0].estimated_memory_bytes());
        assert!(
            CompactionUnit::from_picker(PickerOutput::default())
                .unwrap()
                .is_empty()
        );
    }
}
