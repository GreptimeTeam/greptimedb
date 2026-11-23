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
use crate::compaction::reader::MERGED_BATCH_CHANNEL_SIZE;
use crate::read::seq_scan::compaction_input_channel_size;
use crate::sst::file::{FileHandle, FileMeta};

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

    /// Estimates the memory budget of merging this unit. Outputs are merged one after another,
    /// so the unit needs the budget of its largest output.
    pub(crate) fn estimated_memory_bytes(&self, read_parallelism: usize) -> u64 {
        self.outputs
            .iter()
            .map(|output| output_memory_bytes(&output.inputs, read_parallelism))
            .max()
            .unwrap_or(0)
    }
}

/// Row group size of a file, approximated from its file metadata.
struct RowGroupShape {
    /// Uncompressed bytes of the largest row group.
    bytes: u64,
    /// Average rows of a row group; the metadata keeps no per-row-group row counts.
    rows: u64,
    num_row_groups: u64,
}

impl RowGroupShape {
    fn of(meta: &FileMeta) -> Option<Self> {
        if meta.max_row_group_uncompressed_size == 0
            || meta.num_rows == 0
            || meta.num_row_groups == 0
        {
            return None;
        }
        Some(Self {
            bytes: meta.max_row_group_uncompressed_size,
            rows: (meta.num_rows / meta.num_row_groups).max(1),
            num_row_groups: meta.num_row_groups,
        })
    }

    fn row_width(&self) -> u64 {
        self.bytes.div_ceil(self.rows)
    }
}

/// Estimates the peak memory of merging `inputs` into one output, following the read path:
///
/// - Every input reader holds its current row group.
/// - With a read parallelism above 1, every input is read in its own task and also holds
///   batches ahead of the merge: a full channel, one batch being sent and one in the merge. A batch never spans row groups, and the batches of an
///   input never exceed the whole file.
/// - Inputs read serially hold the batch the merge is consuming.
/// - The merge retains consumed input batches until it emits an output batch.
/// - Merged batches pass a channel to the writer, with one more batch on each side of it.
///
/// The writer's row group buffer and index builders are not covered.
fn output_memory_bytes(inputs: &[FileHandle], read_parallelism: usize) -> u64 {
    let shapes: Vec<_> = inputs
        .iter()
        .filter_map(|file| RowGroupShape::of(file.meta_ref()))
        .collect();
    let row_group_bytes = inputs.iter().fold(0u64, |total, file| {
        total.saturating_add(file.meta_ref().max_row_group_uncompressed_size)
    });
    if shapes.is_empty() {
        return row_group_bytes;
    }
    // Compaction reads every input of an output with one batch size, picked from the widest row.
    let batch_rows =
        estimate_batch_size(shapes.iter().map(|shape| (shape.rows, shape.bytes))) as u64;
    let merges = inputs.len() > 1;
    let total_rows = shapes.iter().fold(0u64, |total, shape| {
        total.saturating_add(shape.rows.saturating_mul(shape.num_row_groups))
    });
    let total_bytes = shapes.iter().fold(0u64, |total, shape| {
        total.saturating_add(shape.bytes.saturating_mul(shape.num_row_groups))
    });
    // The merge emits batches of `batch_rows`, but never more rows than the inputs have; a
    // single input passes its batches through, and those never span row groups.
    let output_batch_bytes = shapes
        .iter()
        .map(|shape| {
            let rows = if merges {
                batch_rows.min(total_rows)
            } else {
                batch_rows.min(shape.rows)
            };
            rows.saturating_mul(shape.row_width())
        })
        .max()
        .unwrap_or(0);

    // Matches the condition under which `SeqScan` spawns a task per input.
    let batches_per_input = if read_parallelism > 1 && merges {
        compaction_input_channel_size() as u64 + 2
    } else {
        1
    };
    let input_batch_bytes = shapes.iter().fold(0u64, |total, shape| {
        let batch_bytes = batch_rows.min(shape.rows).saturating_mul(shape.row_width());
        let file_bytes = shape.bytes.saturating_mul(shape.num_row_groups);
        total.saturating_add(
            batches_per_input
                .saturating_mul(batch_bytes)
                .min(file_bytes),
        )
    });
    let merge_bytes = if merges { output_batch_bytes } else { 0 };
    let output_bytes = (MERGED_BATCH_CHANNEL_SIZE as u64 + 2).saturating_mul(output_batch_bytes);

    // Batches past the readers hold distinct rows, so together they never exceed the inputs.
    let downstream_bytes = merge_bytes.saturating_add(output_bytes).min(total_bytes);

    row_group_bytes
        .saturating_add(input_batch_bytes)
        .saturating_add(downstream_bytes)
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
    use crate::test_util::new_noop_file_purger;

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
    fn test_memory_follows_the_merge_of_each_output() {
        const MIB: u64 = 1024 * 1024;
        let file = |row_group_bytes: u64, rows_per_row_group: u64, num_row_groups: u64| {
            FileHandle::new(
                FileMeta {
                    file_id: FileId::random(),
                    max_row_group_uncompressed_size: row_group_bytes,
                    num_rows: rows_per_row_group * num_row_groups,
                    num_row_groups,
                    ..Default::default()
                },
                new_noop_file_purger(),
            )
        };
        let unit = |outputs: Vec<CompactionOutput>| {
            let mut units = CompactionUnit::from_picker(PickerOutput {
                outputs,
                ..Default::default()
            })
            .unwrap();
            assert_eq!(1, units.len());
            units.pop().unwrap()
        };
        // Rows of 512 bytes are read in 8192-row batches of 4 MiB. Each input holds a 50 MiB row
        // group and the batch the merge consumes; the merge retains one 4 MiB output batch and 4
        // more sit between the merge and the writer.
        let wide_groups = || file(50 * MIB, 100 * 1024, 10);
        let (a, b, c) = (wide_groups(), wide_groups(), wide_groups());
        let serial = 2 * 50 * MIB + 2 * 4 * MIB + 4 * MIB + 16 * MIB;
        assert_eq!(
            serial,
            unit(vec![output(vec![a.clone(), b.clone()])]).estimated_memory_bytes(1)
        );
        // Read in parallel, each input also holds the batches read ahead of the merge.
        let parallel = 2 * 50 * MIB + 2 * 16 * MIB + 4 * MIB + 16 * MIB;
        assert_eq!(
            parallel,
            unit(vec![output(vec![a.clone(), b.clone()])]).estimated_memory_bytes(4)
        );
        // Outputs sharing an input run one after another, so the unit needs one output's budget.
        assert_eq!(
            parallel,
            unit(vec![output(vec![a.clone(), b]), output(vec![a, c])]).estimated_memory_bytes(4)
        );

        // Rows of 1 MiB make 64-row batches, but an input batch stops at its 4-row row group.
        // The merge output still holds 64 rows.
        let narrow_groups = || file(4 * MIB, 4, 1000);
        let (d, e) = (narrow_groups(), narrow_groups());
        assert_eq!(
            2 * 4 * MIB + 2 * 16 * MIB + 64 * MIB + 4 * 64 * MIB,
            unit(vec![output(vec![d.clone(), e])]).estimated_memory_bytes(4)
        );
        // A single input is read without read-ahead or merge, so its batches stay row groups.
        assert_eq!(
            4 * MIB + 4 * MIB + 4 * 4 * MIB,
            unit(vec![output(vec![d])]).estimated_memory_bytes(4)
        );

        // Two one-row files of 8 KiB: a batch holds both rows at most, and the batches past
        // the readers never hold more than the two rows.
        let (f, g) = (file(8 * 1024, 1, 1), file(8 * 1024, 1, 1));
        assert_eq!(
            2 * 8 * 1024 + 2 * 8 * 1024 + 2 * 8 * 1024,
            unit(vec![output(vec![f, g])]).estimated_memory_bytes(4)
        );
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
        assert_eq!(0, units[0].estimated_memory_bytes(1));
        assert!(
            CompactionUnit::from_picker(PickerOutput::default())
                .unwrap()
                .is_empty()
        );
    }
}
