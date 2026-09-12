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

use crate::compaction::CompactionOutput;
use crate::compaction::picker::PickerOutput;
use crate::sst::file::FileHandle;

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
                    parents[a.max(b)] = a.min(b);
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
        for outputs in groups.into_values() {
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
        Some(units)
    }

    /// Estimates merge memory once per input, even when several outputs read it.
    pub(crate) fn estimated_memory_bytes(&self) -> u64 {
        if self.outputs.is_empty() {
            return 0;
        }
        self.inputs.iter().fold(0u64, |total, file| {
            total.saturating_add(file.meta_ref().max_row_group_uncompressed_size)
        })
    }
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
    use crate::compaction::test_util::new_file_handle;

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
            .map(|_| new_file_handle(FileId::random(), 0, 100, 0))
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
        assert_eq!(Some(1024), units[0].max_file_size);
        assert_eq!(100, units[1].time_window_size);
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
