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

use std::collections::{HashMap, HashSet};

use snafu::ResultExt;
use store_api::metadata::RegionMetadataRef;
use store_api::storage::SequenceNumber;

use crate::compaction::CompactionOutput;
use crate::compaction::compactor::CompactionRegion;
use crate::compaction::overlap::FileOverlapIndex;
use crate::compaction::picker::{Picker, PickerOutput};
use crate::compaction::twcs::TwcsPicker;
use crate::error::{JoinSnafu, Result};
use crate::sst::file::{FileHandle, RegionFileId};

/// Picker for [`MergeMode::LastNonNull`](crate::region::options::MergeMode).
///
/// LastNonNull fills null fields of a newer row from older non-null values when
/// rows with the same key and timestamp are merged. That fill is only correct if
/// every SST that can contribute to the same logical row is merged together:
/// once a value is promoted into an output, an intermediate version left behind
/// in a separately compacted SST is invisible, so it can no longer override the
/// promoted value on a later read.
///
/// TWCS alone cannot guarantee this: it schedules independent windows and seeds,
/// while an L1 or cross-window SST can overlap several of them. This picker
/// therefore delegates seed selection to [TwcsPicker] and then closes each seed
/// over the whole snapshot: it absorbs every overlapping file and every seed
/// connected through those overlaps, so a merge stream is never split across
/// outputs, and defers a closure whose inputs are already compacting.
#[derive(Debug)]
pub(super) struct LastNonNullPicker {
    base_picker: TwcsPicker,
}

impl LastNonNullPicker {
    pub(super) fn new(base_picker: TwcsPicker) -> Self {
        Self { base_picker }
    }
}

#[async_trait::async_trait]
impl Picker for LastNonNullPicker {
    async fn pick(&self, region: &CompactionRegion) -> Result<Option<PickerOutput>> {
        let max_outputs = self.base_picker.max_background_tasks;

        // Find all possible compaction seeds
        let Some(mut all_seeds) = self
            .base_picker
            .pick_with_output_limit(region, None)
            .await?
        else {
            return Ok(None);
        };
        let version = region.current_version.clone();
        common_runtime::spawn_blocking_compact(move || {
            let expired: HashSet<_> = all_seeds
                .expired_ssts
                .iter()
                .map(FileHandle::file_id)
                .collect();
            // Include busy files and all levels/windows. Request ranges and
            // TWCS size/file-count limits only constrain seeds, not correctness.
            let all_files = version
                .ssts
                .levels()
                .iter()
                .flat_map(|level| level.files())
                .filter(|file| !expired.contains(&file.file_id()))
                .cloned()
                .collect();
            all_seeds.outputs =
                build_closed_outputs(all_seeds.outputs, all_files, &version.metadata);
            all_seeds.outputs.retain(|output| {
                inputs_precede_memtables(&output.inputs, version.memtable_min_sequence)
            });
            if let Some(limit) = max_outputs {
                // Only eligible closures consume the budget. Keep the high-priority
                // tail because the compactor pops from the end of the output list.
                let excess = all_seeds.outputs.len().saturating_sub(limit);
                all_seeds.outputs.drain(..excess);
            }
            (!all_seeds.outputs.is_empty() || !all_seeds.expired_ssts.is_empty())
                .then_some(all_seeds)
        })
        .await
        .context(JoinSnafu)
    }
}

/// Imported SSTs can be newer than pending writes. A merge must not attach an
/// old field to a sequence above an intermediate version left in a memtable.
pub(super) fn inputs_precede_memtables(
    inputs: &[FileHandle],
    memtable_min_sequence: Option<SequenceNumber>,
) -> bool {
    memtable_min_sequence.is_none_or(|min_sequence| {
        inputs.iter().all(|file| {
            file.meta_ref()
                .sequence
                .is_some_and(|max_sequence| max_sequence.get() < min_sequence)
        })
    })
}

/// Each seed and snapshot file is consumed at most once. Absorbing a seed also
/// absorbs its disconnected members: a merge stream must never be split across
/// independently scheduled outputs.
fn build_closed_outputs(
    seeds: Vec<CompactionOutput>,
    files: Vec<FileHandle>,
    metadata: &RegionMetadataRef,
) -> Vec<CompactionOutput> {
    // Maps every seed input to its seed, so a closure that reaches that input
    // can absorb the rest of the seed instead of letting it schedule separately.
    let seed_by_file: HashMap<_, _> = seeds
        .iter()
        .enumerate()
        .flat_map(|(i, seed)| seed.inputs.iter().map(move |file| (file.file_id(), i)))
        .collect();
    // `None` marks a seed already absorbed by an earlier closure.
    let mut seeds: Vec<_> = seeds.into_iter().map(Some).collect();
    let mut remaining = FileOverlapIndex::new(files, metadata);
    let mut outputs = Vec::new();
    // The compactor pops from the end; retain TWCS's seed priority.
    for seed_index in (0..seeds.len()).rev() {
        let Some(output) = seeds[seed_index].take() else {
            continue;
        };
        let output = expand_closure(output, &mut seeds, &seed_by_file, &mut remaining);
        // Defer the entire closure rather than truncate it around busy files.
        if !output.inputs.iter().any(FileHandle::compacting) {
            outputs.push(output);
        }
    }
    outputs.reverse();
    outputs
}

/// Expands one seed into its full merge closure: absorb every connected seed
/// and every overlapping snapshot file, transitively. Growing `output.inputs`
/// while iterating is what makes the expansion transitive.
fn expand_closure(
    mut output: CompactionOutput,
    seeds: &mut [Option<CompactionOutput>],
    seed_by_file: &HashMap<RegionFileId, usize>,
    remaining_files: &mut FileOverlapIndex<'_>,
) -> CompactionOutput {
    let mut selected = HashSet::new();
    let inputs = std::mem::take(&mut output.inputs);
    extend_inputs(&mut output, inputs, &mut selected, remaining_files);

    let mut cursor = 0;
    while cursor < output.inputs.len() {
        let input = output.inputs[cursor].clone();
        if let Some(&seed_index) = seed_by_file.get(&input.file_id())
            && let Some(seed) = seeds[seed_index].take()
        {
            // AND the tombstone policies so the merged stream stays conservative.
            output.filter_deleted &= seed.filter_deleted;
            extend_inputs(&mut output, seed.inputs, &mut selected, remaining_files);
        }
        let dependencies = remaining_files.drain_overlaps(&input);
        extend_inputs(&mut output, dependencies, &mut selected, remaining_files);
        cursor += 1;
    }
    output
}

/// Appends never-seen inputs and removes them from the overlap index so the
/// same file is neither expanded nor scheduled twice.
fn extend_inputs(
    output: &mut CompactionOutput,
    inputs: Vec<FileHandle>,
    selected: &mut HashSet<RegionFileId>,
    remaining: &mut FileOverlapIndex<'_>,
) {
    for input in inputs {
        if selected.insert(input.file_id()) {
            remaining.remove(input.file_id());
            output.inputs.push(input);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use api::v1::region::compact_request;
    use common_time::Timestamp;
    use common_time::range::TimestampRange;
    use store_api::storage::FileId;

    use super::*;
    use crate::compaction::picker::new_picker;
    use crate::compaction::test_util::{
        compaction_region_with_ssts, new_file_handle, new_file_handle_with_size_and_sequence,
    };
    use crate::region::options::{CompactionOptions, MergeMode};
    use crate::sst::file::{FileMeta, RegionFileId};
    use crate::test_util::memtable_util::metadata_for_test;

    fn seed(inputs: Vec<FileHandle>) -> CompactionOutput {
        CompactionOutput {
            output_level: 1,
            inputs,
            filter_deleted: false,
            output_time_range: None,
        }
    }

    fn file_ids(files: &[FileHandle]) -> HashSet<RegionFileId> {
        files.iter().map(FileHandle::file_id).collect()
    }

    #[rstest::rstest]
    #[case::memtable_barrier(2, Some(20), false, Some(1), vec![vec![1]])]
    #[case::busy_dependency(2, None, true, Some(1), vec![vec![1]])]
    #[case::highest_priority(2, None, false, Some(1), vec![vec![2]])]
    #[case::priority_order(2, None, false, Some(2), vec![vec![2], vec![1]])]
    #[case::unlimited(2, None, false, None, vec![vec![2], vec![1], vec![0]])]
    #[case::fewer_eligible_outputs(2, Some(20), false, Some(3), vec![vec![1], vec![0]])]
    #[case::absorbed_seeds(1, None, false, Some(2), vec![vec![2, 1], vec![0]])]
    #[case::busy_bridge_defers_both_seeds(1, None, true, Some(2), vec![vec![0]])]
    #[case::memtable_barrier_defers_both_seeds(1, Some(20), false, Some(2), vec![vec![0]])]
    #[tokio::test]
    async fn test_output_limit_counts_eligible_closures(
        #[case] dependency_start_window: usize,
        #[case] memtable_min: Option<u64>,
        #[case] busy: bool,
        #[case] limit: Option<usize>,
        #[case] expected_windows: Vec<Vec<usize>>,
    ) {
        // Three independent, eligible L0 windows. A large L1 dependency is not
        // a seed, but can reject the newest closure or connect two seeds.
        // Rejecting that bridge must defer both connected seeds, leaving only
        // the independent oldest window eligible rather than a partial merge.
        let windows: Vec<Vec<_>> = (0..3)
            .map(|window| {
                (0..4)
                    .map(|i| FileMeta {
                        file_id: FileId::random(),
                        time_range: (
                            Timestamp::new_second(window * 3600),
                            Timestamp::new_second(window * 3600 + 10),
                        ),
                        level: 0,
                        file_size: 100,
                        sequence: std::num::NonZeroU64::new((window * 4 + i + 1) as u64),
                        ..Default::default()
                    })
                    .collect()
            })
            .collect();
        let dependency = FileMeta {
            file_id: FileId::random(),
            time_range: (
                Timestamp::new_second(dependency_start_window as i64 * 3600),
                Timestamp::new_second(7210),
            ),
            level: 1,
            file_size: 1_000_000,
            sequence: std::num::NonZeroU64::new(20),
            ..Default::default()
        };
        let dependency_id = dependency.file_id();
        let files = windows.iter().flatten().cloned().chain([dependency]);
        let mut region = compaction_region_with_ssts(files, Duration::from_secs(60)).await;
        region.ttl = None;
        region.region_options.merge_mode = Some(MergeMode::LastNonNull);
        region.current_version.memtable_min_sequence = memtable_min;
        let CompactionOptions::Twcs(opts) = &mut region.region_options.compaction;
        opts.time_window = Some(Duration::from_secs(3600));
        opts.active_window_trigger_file_num = 4;
        opts.inactive_window_trigger_file_num = 4;
        region.current_version.ssts.levels()[1]
            .files()
            .next()
            .unwrap()
            .set_compacting(busy);
        let request = compact_request::Options::Regular(Default::default());
        let picker = new_picker(&request, &region.region_options, limit, None);

        let picked = picker.pick(&region).await.unwrap().unwrap();
        assert_eq!(expected_windows.len(), picked.outputs.len());
        assert!(picked.expired_ssts.is_empty());
        // The execution pops outputs from the end, not the front.
        for (output, window_indices) in picked.outputs.iter().rev().zip(expected_windows) {
            let mut expected: HashSet<_> = window_indices
                .iter()
                .flat_map(|&i| windows[i].iter().map(FileMeta::file_id))
                .collect();
            if window_indices.contains(&2) {
                expected.insert(dependency_id);
            }
            assert_eq!(expected, file_ids(&output.inputs));
        }

        // The LastRow picker must retain its original seed cap and priority.
        region.region_options.merge_mode = Some(MergeMode::LastRow);
        let picker = new_picker(&request, &region.region_options, limit, None);
        let picked = picker.pick(&region).await.unwrap().unwrap();
        assert_eq!(limit.unwrap_or(3).min(3), picked.outputs.len());
        for (output, window) in picked.outputs.iter().rev().zip(windows.iter().rev()) {
            let expected: HashSet<_> = window.iter().map(FileMeta::file_id).collect();
            assert_eq!(expected, file_ids(&output.inputs));
        }
    }

    // Cover absent/unknown boundaries and strict inequality without arithmetic
    // on sequence numbers, including the maximum representable sequence.
    #[rstest::rstest]
    #[case(0, None, true)]
    #[case(0, Some(2), false)]
    #[case(1, Some(0), false)]
    #[case(1, Some(2), true)]
    #[case(2, Some(2), false)]
    #[case(3, Some(2), false)]
    #[case(u64::MAX - 1, Some(u64::MAX), true)]
    #[case(u64::MAX, Some(u64::MAX), false)]
    fn test_inputs_must_precede_pending_memtables(
        #[case] file_sequence: u64,
        #[case] memtable_min: Option<u64>,
        #[case] expected: bool,
    ) {
        let file =
            new_file_handle_with_size_and_sequence(FileId::random(), 0, 10, 0, file_sequence, 100);
        assert_eq!(expected, inputs_precede_memtables(&[file], memtable_min));
    }

    #[test]
    fn test_transitive_closure_exceeds_seed_limit_across_levels_and_windows() {
        let metadata = metadata_for_test();
        // Inclusive endpoints connect neighbors across hours and both levels.
        // A one-pass expansion or applying the TWCS seed cap loses the tail.
        let files: Vec<_> = (0..32)
            .map(|i| {
                new_file_handle(
                    FileId::random(),
                    i * 3_600_000,
                    (i + 1) * 3_600_000,
                    (i % 2) as u8,
                )
            })
            .collect();
        let outputs =
            build_closed_outputs(vec![seed(vec![files[0].clone()])], files.clone(), &metadata);
        assert_eq!(1, outputs.len());
        assert_eq!(files.len(), outputs[0].inputs.len());
        assert_eq!(file_ids(&files), file_ids(&outputs[0].inputs));
    }

    #[rstest::rstest]
    #[case(false)]
    #[case(true)]
    fn test_absorbed_seed_closes_disconnected_members_and_defers_busy_group(#[case] busy: bool) {
        let metadata = metadata_for_test();
        let files: Vec<_> = [(0, 10), (10, 20), (100, 110), (110, 120), (200, 210)]
            .into_iter()
            .map(|(start, end)| new_file_handle(FileId::random(), start, end, 0))
            .collect();
        files[3].set_compacting(busy);
        let mut priority_seed = seed(vec![files[0].clone()]);
        priority_seed.filter_deleted = true;
        let seeds = vec![
            seed(vec![files[4].clone()]),
            seed(vec![files[1].clone(), files[2].clone()]),
            priority_seed,
        ];

        let mut outputs = build_closed_outputs(seeds, files.clone(), &metadata);
        assert_eq!(if busy { 1 } else { 2 }, outputs.len());
        if !busy {
            let first = outputs.pop().unwrap();
            assert_eq!(4, first.inputs.len());
            assert_eq!(file_ids(&files[..4]), file_ids(&first.inputs));
            // Preserve the absorbed seed's conservative tombstone policy.
            assert!(!first.filter_deleted);
        }
        assert_eq!(
            vec![files[4].file_id()],
            outputs[0]
                .inputs
                .iter()
                .map(FileHandle::file_id)
                .collect::<Vec<_>>()
        );
    }

    #[rstest::rstest]
    #[case("chain", 2048)]
    #[case("dense_with_external", 1024)]
    #[case("time_dense_pk_disjoint", 1)]
    fn test_large_snapshot_closure(#[case] shape: &str, #[case] expected_count: usize) {
        use crate::compaction::test_util::{
            new_file_handle_with_size_sequence_and_primary_key_range, pk_range,
            primary_key_metadata_for_test,
        };

        let metadata = primary_key_metadata_for_test();
        let files = (0..2048)
            .map(|i| {
                let (start, end, pk) = match shape {
                    "chain" => (i, i + 1, None),
                    "dense_with_external" if i < 1024 => (0, 1, None),
                    "dense_with_external" => (i, i, None),
                    "time_dense_pk_disjoint" => {
                        let key = i.to_string();
                        (0, 1, pk_range(key.as_bytes(), key.as_bytes()))
                    }
                    _ => unreachable!(),
                };
                new_file_handle_with_size_sequence_and_primary_key_range(
                    FileId::random(),
                    start,
                    end,
                    0,
                    i as u64 + 1,
                    100,
                    pk,
                )
            })
            .collect::<Vec<_>>();
        let outputs =
            build_closed_outputs(vec![seed(vec![files[0].clone()])], files.clone(), &metadata);
        assert_eq!(1, outputs.len());
        assert_eq!(expected_count, outputs[0].inputs.len());
        assert_eq!(
            file_ids(&files[..expected_count]),
            file_ids(&outputs[0].inputs)
        );
    }

    #[rstest::rstest]
    #[case(MergeMode::LastRow, false, None, 4)]
    #[case(MergeMode::LastRow, true, None, 4)]
    #[case(MergeMode::LastNonNull, false, None, 5)]
    #[case(MergeMode::LastNonNull, true, None, 0)]
    #[case(MergeMode::LastRow, false, Some(5), 4)]
    #[case(MergeMode::LastNonNull, false, Some(5), 0)]
    #[case(MergeMode::LastNonNull, false, Some(6), 5)]
    #[tokio::test]
    async fn test_picker_dependencies_outside_request_window(
        #[case] merge_mode: MergeMode,
        #[case] busy: bool,
        #[case] memtable_min: Option<u64>,
        #[case] expected_count: usize,
    ) {
        let files = (0..5).map(|i| FileMeta {
            file_id: FileId::random(),
            time_range: (
                Timestamp::new_second(0),
                Timestamp::new_second(if i == 4 { 3601 } else { 10 }),
            ),
            level: if i == 4 { 1 } else { 0 },
            file_size: if i == 4 { 1_000_000 } else { 100 },
            sequence: std::num::NonZeroU64::new(i + 1),
            ..Default::default()
        });
        let mut region = compaction_region_with_ssts(files, Duration::from_secs(60)).await;
        region.ttl = None;
        region.region_options.merge_mode = Some(merge_mode);
        region.current_version.memtable_min_sequence = memtable_min;
        let CompactionOptions::Twcs(opts) = &mut region.region_options.compaction;
        opts.time_window = Some(Duration::from_secs(3600));
        let dependency = region.current_version.ssts.levels()[1]
            .files()
            .next()
            .unwrap();
        dependency.set_compacting(busy);
        let picker = new_picker(
            &compact_request::Options::Regular(Default::default()),
            &region.region_options,
            Some(1),
            TimestampRange::new(Timestamp::new_second(0), Timestamp::new_second(3600)),
        );

        let picked = picker.pick(&region).await.unwrap();
        if expected_count == 0 {
            assert!(picked.is_none());
        } else {
            let picked = picked.unwrap();
            assert_eq!(1, picked.outputs.len());
            assert_eq!(expected_count, picked.outputs[0].inputs.len());
            assert_eq!(
                merge_mode == MergeMode::LastNonNull,
                picked.outputs[0]
                    .inputs
                    .iter()
                    .any(|file| file.file_id() == dependency.file_id())
            );
        }
    }

    #[rstest::rstest]
    #[case(MergeMode::LastNonNull, None, 3)]
    #[case(MergeMode::LastNonNull, Some(2), 0)]
    #[case(MergeMode::LastRow, Some(2), 3)]
    #[tokio::test]
    async fn test_strict_window_keeps_all_versions_in_disjoint_output_slices(
        #[case] merge_mode: MergeMode,
        #[case] memtable_min: Option<u64>,
        #[case] expected_outputs: usize,
    ) {
        let files: Vec<_> = [(0, 10), (10, 10), (10, 20)]
            .into_iter()
            .enumerate()
            .map(|(i, (start, end))| {
                new_file_handle_with_size_and_sequence(
                    FileId::random(),
                    start * 1000,
                    end * 1000,
                    0,
                    i as u64 + 1,
                    100,
                )
                .meta_ref()
                .clone()
            })
            .collect();
        let mut region = compaction_region_with_ssts(files, Duration::from_secs(60)).await;
        region.region_options.merge_mode = Some(merge_mode);
        region.current_version.options.merge_mode = Some(merge_mode);
        region.current_version.memtable_min_sequence = memtable_min;
        let picker = new_picker(
            &compact_request::Options::StrictWindow(api::v1::region::StrictWindow {
                window_seconds: 10,
            }),
            &region.region_options,
            Some(1),
            TimestampRange::new(Timestamp::new_second(10), Timestamp::new_second(20)),
        );

        let picked = picker.pick(&region).await.unwrap().unwrap();
        // Cross-window inputs also need their outer slices rewritten, despite
        // requesting only [10, 20). All three versions at t=10 share one stream.
        // With pending sequence 2, [0, 10) alone is safe, but must not be
        // rewritten without the unsafe windows sharing its input SST.
        assert_eq!(expected_outputs, picked.outputs.len());
        for (i, output) in picked.outputs.iter().enumerate() {
            assert_eq!(
                TimestampRange::new(
                    Timestamp::new_second(i as i64 * 10),
                    Timestamp::new_second((i as i64 + 1) * 10),
                ),
                output.output_time_range
            );
            assert_eq!(if i == 1 { 3 } else { 1 }, output.inputs.len());
            assert!(!output.filter_deleted);
        }
    }

    #[tokio::test]
    async fn test_busy_closure_keeps_expired_files_without_rewriting_them() {
        let now = Timestamp::current_millis().value();
        let files = (0..6).map(|i| FileMeta {
            file_id: FileId::random(),
            time_range: (
                Timestamp::new_millisecond(if i == 5 { 0 } else { now - 10_000 }),
                Timestamp::new_millisecond(if i == 5 { 10 } else { now }),
            ),
            level: if i == 4 { 1 } else { 0 },
            file_size: if i == 4 { 1_000_000 } else { 100 },
            sequence: std::num::NonZeroU64::new(i + 1),
            ..Default::default()
        });
        let mut region = compaction_region_with_ssts(files, Duration::from_secs(3600)).await;
        region.region_options.merge_mode = Some(MergeMode::LastNonNull);
        let dependency = region.current_version.ssts.levels()[1]
            .files()
            .next()
            .unwrap();
        dependency.set_compacting(true);
        let picker = new_picker(
            &compact_request::Options::Regular(Default::default()),
            &region.region_options,
            Some(1),
            None,
        );
        let picked = picker.pick(&region).await.unwrap().unwrap();
        assert!(picked.outputs.is_empty());
        assert_eq!(1, picked.expired_ssts.len());
        assert_eq!(
            Timestamp::new_millisecond(10),
            picked.expired_ssts[0].time_range().1
        );
        assert!(!picked.expired_ssts[0].compacting());

        dependency.set_compacting(false);
        let picked = picker.pick(&region).await.unwrap().unwrap();
        assert_eq!(1, picked.outputs.len());
        assert_eq!(5, picked.outputs[0].inputs.len());
        assert_eq!(1, picked.expired_ssts.len());
        assert!(!file_ids(&picked.outputs[0].inputs).contains(&picked.expired_ssts[0].file_id()));
    }
}
