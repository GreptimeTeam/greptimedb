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

//! Unit-based compaction picks for one TWCS window.
//!
//! Both picks group files into *units* and select a contiguous interval of units
//! in a fixed order, so a pick never splits files that must be rewritten
//! together.
//!
//! - [`pick_active_l1`] reduces the L1 file count of the active window.
//! - [`pick_closed_merge`] only merges *closed* intervals in an inactive window:
//!   no other file of the window intrudes on the bounding box of the inputs.
//!   Compaction outputs stay inside that box, so they cannot overlap a file that
//!   the inputs did not overlap.
//!
//! # Termination
//!
//! Let `P` be the number of conflicting file pairs ([`Extent::conflicts`]) in an
//! inactive window and `N` its file count. A closed merge removes every pair
//! among its inputs and adds none: its outputs are one sorted run, whose files
//! only share boundary keys, inside a box no other file intrudes on. Every
//! inactive pick either resolves a pair (`P` drops) or removes a file (`P` does
//! not grow and `N` drops), so `(P, N)` strictly decreases in lexicographic
//! order until the window receives new writes. Active-window picks are not
//! closed and may add conflicts, so each of them must remove a file: `N`
//! strictly decreases.

use std::cmp::Reverse;
use std::ops::Range;

use bytes::Bytes;
use common_time::Timestamp;

use crate::sst::file::FileHandle;
use crate::sst::primary_key::PrimaryKeyRangeMapper;

/// An L1 file reduction may rewrite at most this many output files' worth of
/// bytes per SST it removes.
const MAX_REWRITE_FILES_PER_REDUCED_FILE: u64 = 4;

/// Output size limits and input limits shared by both picks.
#[derive(Debug, Clone, Copy)]
pub(crate) struct PickLimits {
    /// Split threshold of compaction outputs; `None` writes a single output.
    pub(crate) max_output_file_size: Option<u64>,
    /// Maximum number of input files of one compaction.
    pub(crate) max_input_files: usize,
}

impl PickLimits {
    /// Upper bound of the outputs of compacting `size` bytes: the writer only
    /// starts a new file after the current one reaches the threshold.
    fn predicted_outputs(&self, size: u64) -> usize {
        match self.max_output_file_size {
            Some(max) if max > 0 => size.div_ceil(max).max(1) as usize,
            _ => 1,
        }
    }

    /// Whether rewriting `size` bytes to remove `reduction` files stays within
    /// the rewrite budget. An unset output size imposes no bound.
    fn within_reduction_cost(&self, size: u64, reduction: usize) -> bool {
        match self.max_output_file_size {
            Some(max) if max > 0 => {
                size <= (reduction as u64)
                    .saturating_mul(MAX_REWRITE_FILES_PER_REDUCED_FILE)
                    .saturating_mul(max)
            }
            _ => true,
        }
    }

    /// Bytes a dominant sorted run may exceed its peers by in an overlap merge.
    fn run_tolerance(&self) -> u64 {
        self.max_output_file_size.unwrap_or(0)
    }
}

/// A selected compaction input.
#[derive(Debug)]
pub(crate) struct UnitPick {
    pub(crate) inputs: Vec<FileHandle>,
    /// Whether the pick is predicted to reduce the window's file count. Otherwise
    /// it only resolves overlaps.
    pub(crate) reduces_files: bool,
    /// Number of units the candidate files were grouped into.
    pub(crate) units: usize,
}

/// Picks L1 files of the active window whose compaction removes files.
///
/// Units are groups of mutually conflicting files, so a merge does not leave a
/// file behind while rewriting its overlapping peers. A group larger than the
/// input limit is split back into single files. Intervals follow
/// [`sort_units`]; the pick removes the most files, then rewrites the fewest
/// bytes.
pub(crate) fn pick_active_l1(
    l1_files: &[FileHandle],
    mapper: &PrimaryKeyRangeMapper,
    options: PickLimits,
) -> Option<UnitPick> {
    let extents = extents_of(l1_files, mapper);
    let mut units = conflict_units(l1_files, &extents);
    split_oversized_units(&mut units, l1_files, &extents, options.max_input_files);
    sort_units(&mut units);

    let mut best: Option<(usize, u64, Range<usize>)> = None;
    for_each_interval(&units, options.max_input_files, |range, stats| {
        let reduction = stats
            .num_files
            .saturating_sub(options.predicted_outputs(stats.size));
        if stats.num_files < 2
            || reduction == 0
            || !options.within_reduction_cost(stats.size, reduction)
            || stats.largest_file_size > stats.size - stats.largest_file_size
        {
            return;
        }
        let better = best.as_ref().is_none_or(|(best_reduction, best_size, _)| {
            (reduction, Reverse(stats.size)) > (*best_reduction, Reverse(*best_size))
        });
        if better {
            best = Some((reduction, stats.size, range));
        }
    });

    best.map(|(_, _, range)| UnitPick {
        inputs: collect_inputs(&units[range], l1_files),
        reduces_files: true,
        units: units.len(),
    })
}

/// Thresholds of the closed merge pick.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ClosedMergeOptions {
    pub(crate) limits: PickLimits,
    /// Whether merges that only remove files are allowed. Overlap resolution is
    /// always allowed.
    pub(crate) allow_file_reduction: bool,
}

/// Picks a closed interval of units among all files of an inactive window.
///
/// Units are the closures of conflicting files: no file outside a unit intrudes
/// on its bounding box. Intervals follow [`sort_units`]. An interval is
/// eligible if it
///
/// - removes files within the rewrite budget, or
/// - resolves conflicts without rewriting a dominant sorted run, and adds at
///   most one tail file.
///
/// The pick maximizes resolved conflicts plus removed files per rewritten byte.
pub(crate) fn pick_closed_merge(
    files: &[FileHandle],
    mapper: &PrimaryKeyRangeMapper,
    options: ClosedMergeOptions,
) -> Option<UnitPick> {
    let limits = options.limits;
    let extents = extents_of(files, mapper);
    let mut units = conflict_units(files, &extents);
    close_units(&mut units, &extents);
    sort_units(&mut units);

    let mut best: Option<(u64, u64, Range<usize>, bool)> = None;
    for_each_interval(&units, limits.max_input_files, |range, stats| {
        if stats.num_files < 2 {
            return;
        }
        let outputs = limits.predicted_outputs(stats.size);
        if outputs > stats.num_files + 1 {
            return;
        }
        let reduction = stats.num_files.saturating_sub(outputs);
        let reduces_files = reduction > 0
            && (options.allow_file_reduction || stats.conflicts > 0)
            && limits.within_reduction_cost(stats.size, reduction);
        if !reduces_files && stats.conflicts == 0 {
            return;
        }
        let benefit = (reduction + stats.conflicts) as u64;
        let better = best.as_ref().is_none_or(|(best_benefit, best_size, _, _)| {
            is_more_efficient(benefit, stats.size, *best_benefit, *best_size)
        });
        if !better {
            return;
        }
        let units = &units[range.clone()];
        let unit_files = || units.iter().flat_map(|unit| unit.files.iter().copied());
        // Overlap merges must not rewrite a dominant run for a small peer. A
        // file-reducing merge is already bounded by its rewrite budget.
        if !reduces_files
            && !has_balanced_runs(unit_files(), files, &extents, limits.run_tolerance())
        {
            return;
        }
        if units.len() > 1 && !is_closed(units, &extents) {
            return;
        }
        best = Some((benefit, stats.size, range, reduces_files));
    });

    best.map(|(_, _, range, reduces_files)| UnitPick {
        inputs: collect_inputs(&units[range], files),
        reduces_files,
        units: units.len(),
    })
}

/// Orders units by minimum primary key, then by time, so adjacent units are
/// key-adjacent. Compaction outputs and time-sliced flushes split a window by
/// key ranges, so merging neighbors in this order keeps outputs from covering
/// the key ranges of files left behind.
fn sort_units(units: &mut [Unit]) {
    units.sort_by(|lhs, rhs| {
        (lhs.first_key(), lhs.extent.start, Reverse(lhs.extent.end))
            .cmp(&(rhs.first_key(), rhs.extent.start, Reverse(rhs.extent.end)))
            .then_with(|| lhs.files[0].cmp(&rhs.files[0]))
    });
}

/// Whether `benefit` per `size` bytes beats the best so far; ties prefer fewer
/// bytes, then the earlier interval.
fn is_more_efficient(benefit: u64, size: u64, best_benefit: u64, best_size: u64) -> bool {
    let lhs = u128::from(benefit) * u128::from(best_size.max(1));
    let rhs = u128::from(best_benefit) * u128::from(size.max(1));
    lhs > rhs || (lhs == rhs && size < best_size)
}

/// Time and primary key bounds of a file or a group of files.
#[derive(Debug, Clone)]
pub(crate) struct Extent {
    start: Timestamp,
    end: Timestamp,
    /// Inclusive primary key bounds. `None` is unknown and matches any key.
    primary_key: Option<(Bytes, Bytes)>,
}

impl Extent {
    pub(crate) fn of_file(file: &FileHandle, mapper: &PrimaryKeyRangeMapper) -> Self {
        let (start, end) = file.time_range();
        Self {
            start,
            end,
            primary_key: file.primary_key_range(mapper),
        }
    }

    fn union(&self, other: &Self) -> Self {
        Self {
            start: self.start.min(other.start),
            end: self.end.max(other.end),
            primary_key: match (&self.primary_key, &other.primary_key) {
                (Some((lhs_min, lhs_max)), Some((rhs_min, rhs_max))) => Some((
                    lhs_min.clone().min(rhs_min.clone()),
                    lhs_max.clone().max(rhs_max.clone()),
                )),
                _ => None,
            },
        }
    }

    /// Time ranges overlap; touching boundaries do not.
    fn time_overlaps(&self, other: &Self) -> bool {
        self.start.max(other.start) < self.end.min(other.end)
    }

    /// Whether two files overlap for the picker.
    ///
    /// Key ranges that share only a boundary key do not conflict unless both
    /// hold that single key. A compaction output split by size shares boundary
    /// keys with its neighbors, and counting them would make every sorted run
    /// look overlapping.
    pub(crate) fn conflicts(&self, other: &Self) -> bool {
        self.time_overlaps(other)
            && match (&self.primary_key, &other.primary_key) {
                (Some(lhs), Some(rhs)) => {
                    let low = lhs.0.clone().max(rhs.0.clone());
                    let high = lhs.1.clone().min(rhs.1.clone());
                    low < high || (low == high && is_single_key(lhs) && is_single_key(rhs))
                }
                _ => true,
            }
    }

    /// Whether `self` may conflict with a file inside `area`, which is the
    /// bounding box of a group of files.
    fn intrudes(&self, area: &Self) -> bool {
        self.time_overlaps(area)
            && match (&self.primary_key, &area.primary_key) {
                (Some(lhs), Some(rhs)) => {
                    let low = lhs.0.clone().max(rhs.0.clone());
                    let high = lhs.1.clone().min(rhs.1.clone());
                    low < high || (low == high && is_single_key(lhs))
                }
                _ => true,
            }
    }
}

fn is_single_key(range: &(Bytes, Bytes)) -> bool {
    range.0 == range.1
}

fn extents_of(files: &[FileHandle], mapper: &PrimaryKeyRangeMapper) -> Vec<Extent> {
    files
        .iter()
        .map(|file| Extent::of_file(file, mapper))
        .collect()
}

/// Files that are compacted together.
#[derive(Debug)]
struct Unit {
    /// Ascending indices into the candidate files.
    files: Vec<usize>,
    extent: Extent,
    size: u64,
    largest_file_size: u64,
    /// Number of conflicting file pairs inside the unit.
    conflicts: usize,
}

impl Unit {
    fn new(
        members: Vec<usize>,
        files: &[FileHandle],
        extents: &[Extent],
        conflicts: usize,
    ) -> Self {
        let extent = members[1..]
            .iter()
            .fold(extents[members[0]].clone(), |extent, &i| {
                extent.union(&extents[i])
            });
        let sizes = members.iter().map(|&i| files[i].size());
        Self {
            size: sizes.clone().sum(),
            largest_file_size: sizes.max().unwrap_or(0),
            files: members,
            extent,
            conflicts,
        }
    }

    /// The minimum primary key, with unknown keys ordered first.
    fn first_key(&self) -> Option<&Bytes> {
        self.extent.primary_key.as_ref().map(|(min, _)| min)
    }
}

/// Groups files into connected components of [`Extent::conflicts`], each with
/// its number of conflicting pairs. A sweep in time order compares each file
/// only with files whose time ranges are still open: O(N log N + C) for C
/// compared pairs.
fn conflict_units(files: &[FileHandle], extents: &[Extent]) -> Vec<Unit> {
    let mut by_start: Vec<usize> = (0..extents.len()).collect();
    by_start.sort_by_key(|&i| extents[i].start);
    let mut sets = DisjointSet::new(extents.len());
    let mut conflicts = vec![0; extents.len()];
    let mut open: Vec<usize> = Vec::new();
    for &i in &by_start {
        open.retain(|&j| extents[j].end > extents[i].start);
        for &j in &open {
            if extents[i].conflicts(&extents[j]) {
                sets.union(i, j);
                conflicts[i] += 1;
            }
        }
        open.push(i);
    }
    sets.groups()
        .into_iter()
        .map(|members| {
            let pairs = members.iter().map(|&i| conflicts[i]).sum();
            Unit::new(members, files, extents, pairs)
        })
        .collect()
}

/// Replaces units with more files than one compaction accepts by single files.
fn split_oversized_units(
    units: &mut Vec<Unit>,
    files: &[FileHandle],
    extents: &[Extent],
    max_input_files: usize,
) {
    let (oversized, mut kept): (Vec<_>, Vec<_>) = std::mem::take(units)
        .into_iter()
        .partition(|unit| unit.files.len() > max_input_files);
    kept.extend(
        oversized
            .into_iter()
            .flat_map(|unit| unit.files)
            .map(|i| Unit::new(vec![i], files, extents, 0)),
    );
    *units = kept;
}

/// Merges units until no file intrudes on a unit it does not belong to.
///
/// Each merge re-checks only the merged unit, so the closure takes O(U · N)
/// intrusion checks for U initial units and N files.
fn close_units(units: &mut Vec<Unit>, extents: &[Extent]) {
    let mut owner = vec![0; extents.len()];
    for (u, unit) in units.iter().enumerate() {
        for &i in &unit.files {
            owner[i] = u;
        }
    }
    let mut alive = vec![true; units.len()];
    let mut pending: Vec<usize> = (0..units.len()).collect();
    while let Some(u) = pending.pop() {
        if !alive[u] {
            continue;
        }
        let intruders = (0..extents.len())
            .filter(|&i| owner[i] != u && extents[i].intrudes(&units[u].extent))
            .map(|i| owner[i])
            .collect::<std::collections::BTreeSet<_>>();
        if intruders.is_empty() {
            continue;
        }
        for v in intruders {
            alive[v] = false;
            let absorbed = std::mem::take(&mut units[v].files);
            for &i in &absorbed {
                owner[i] = u;
            }
            let (extent, size, largest, conflicts) = {
                let other = &units[v];
                (
                    other.extent.clone(),
                    other.size,
                    other.largest_file_size,
                    other.conflicts,
                )
            };
            let unit = &mut units[u];
            unit.files.extend(absorbed);
            unit.extent = unit.extent.union(&extent);
            unit.size += size;
            unit.largest_file_size = unit.largest_file_size.max(largest);
            unit.conflicts += conflicts;
        }
        pending.push(u);
    }
    let mut closed = Vec::with_capacity(units.len());
    for (u, mut unit) in std::mem::take(units).into_iter().enumerate() {
        if alive[u] {
            unit.files.sort_unstable();
            closed.push(unit);
        }
    }
    *units = closed;
}

/// Totals of a contiguous interval of units.
#[derive(Debug, Default)]
struct IntervalStats {
    num_files: usize,
    size: u64,
    largest_file_size: u64,
    conflicts: usize,
}

/// Visits every contiguous interval of units with at most `max_input_files`
/// files: O(U · K) intervals for an input limit K.
fn for_each_interval(
    units: &[Unit],
    max_input_files: usize,
    mut visit: impl FnMut(Range<usize>, &IntervalStats),
) {
    for left in 0..units.len() {
        let mut stats = IntervalStats::default();
        for (right, unit) in units.iter().enumerate().skip(left) {
            stats.num_files += unit.files.len();
            if stats.num_files > max_input_files {
                break;
            }
            stats.size += unit.size;
            stats.largest_file_size = stats.largest_file_size.max(unit.largest_file_size);
            stats.conflicts += unit.conflicts;
            visit(left..right + 1, &stats);
        }
    }
}

/// Whether no file outside `units` intrudes on their bounding box.
fn is_closed(units: &[Unit], extents: &[Extent]) -> bool {
    let area = units[1..]
        .iter()
        .fold(units[0].extent.clone(), |area, unit| {
            area.union(&unit.extent)
        });
    let mut selected = vec![false; extents.len()];
    for &i in units.iter().flat_map(|unit| unit.files.iter()) {
        selected[i] = true;
    }
    !extents
        .iter()
        .zip(&selected)
        .any(|(extent, selected)| !selected && extent.intrudes(&area))
}

/// Whether the largest sorted run among `files` holds at most `tolerance` bytes
/// more than all other runs together. Runs are built greedily in time order,
/// which takes O(K²) conflict checks for K files.
fn has_balanced_runs(
    members: impl Iterator<Item = usize>,
    files: &[FileHandle],
    extents: &[Extent],
    tolerance: u64,
) -> bool {
    let mut members: Vec<usize> = members.collect();
    members.sort_by(|&lhs, &rhs| {
        (extents[lhs].start, Reverse(extents[lhs].end), lhs).cmp(&(
            extents[rhs].start,
            Reverse(extents[rhs].end),
            rhs,
        ))
    });
    let mut runs: Vec<(Vec<usize>, u64)> = Vec::new();
    for i in members {
        let size = files[i].size();
        match runs.iter_mut().find(|(run, _)| {
            run.iter()
                .all(|&other| !extents[i].conflicts(&extents[other]))
        }) {
            Some((run, run_size)) => {
                run.push(i);
                *run_size += size;
            }
            None => runs.push((vec![i], size)),
        }
    }
    let total: u64 = runs.iter().map(|(_, size)| size).sum();
    let largest = runs.iter().map(|(_, size)| *size).max().unwrap_or(0);
    largest <= (total - largest).saturating_add(tolerance)
}

fn collect_inputs(units: &[Unit], files: &[FileHandle]) -> Vec<FileHandle> {
    let mut indices: Vec<usize> = units
        .iter()
        .flat_map(|unit| unit.files.iter().copied())
        .collect();
    indices.sort_unstable();
    indices.into_iter().map(|i| files[i].clone()).collect()
}

/// Union-find over file indices; each root is the smallest index of its set.
struct DisjointSet {
    parent: Vec<usize>,
}

impl DisjointSet {
    fn new(len: usize) -> Self {
        Self {
            parent: (0..len).collect(),
        }
    }

    fn find(&mut self, mut i: usize) -> usize {
        while self.parent[i] != i {
            self.parent[i] = self.parent[self.parent[i]];
            i = self.parent[i];
        }
        i
    }

    fn union(&mut self, lhs: usize, rhs: usize) {
        let (lhs, rhs) = (self.find(lhs), self.find(rhs));
        let (root, child) = (lhs.min(rhs), lhs.max(rhs));
        self.parent[child] = root;
    }

    /// Sets in ascending order of their smallest index, each sorted.
    fn groups(&mut self) -> Vec<Vec<usize>> {
        let mut groups: Vec<Vec<usize>> = Vec::new();
        let mut slot = vec![usize::MAX; self.parent.len()];
        for i in 0..self.parent.len() {
            let root = self.find(i);
            if slot[root] == usize::MAX {
                slot[root] = groups.len();
                groups.push(Vec::new());
            }
            groups[slot[root]].push(i);
        }
        groups
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use store_api::storage::FileId;

    use super::*;
    use crate::compaction::test_util::{
        new_file_handle_with_size_sequence_and_primary_key_range, pk_range,
        primary_key_mapper_for_test,
    };

    const MAX_OUTPUT: u64 = 16;

    fn file(start: i64, end: i64, min: &[u8], max: &[u8], size: u64) -> FileHandle {
        new_file_handle_with_size_sequence_and_primary_key_range(
            FileId::random(),
            start,
            end,
            1,
            1,
            size,
            pk_range(min, max),
        )
    }

    fn limits(max_output_file_size: u64) -> PickLimits {
        PickLimits {
            max_output_file_size: Some(max_output_file_size),
            max_input_files: 16,
        }
    }

    fn closed_merge(files: &[FileHandle], max_output_file_size: u64) -> Option<UnitPick> {
        pick_closed_merge(
            files,
            &primary_key_mapper_for_test(),
            ClosedMergeOptions {
                limits: limits(max_output_file_size),
                allow_file_reduction: true,
            },
        )
    }

    fn ids(files: &[FileHandle]) -> HashSet<FileId> {
        files.iter().map(|file| file.file_id().file_id()).collect()
    }

    fn extent(start: i64, end: i64, min: &[u8], max: &[u8]) -> Extent {
        Extent::of_file(
            &file(start, end, min, max, 1),
            &primary_key_mapper_for_test(),
        )
    }

    #[test]
    fn test_conflicts_ignore_shared_boundary_keys() {
        // Neighbors of a sorted run split by size.
        assert!(!extent(0, 10, b"a", b"m").conflicts(&extent(0, 10, b"m", b"z")));
        assert!(!extent(0, 10, b"m", b"m").conflicts(&extent(0, 10, b"m", b"z")));
        // A single key shared by two single-key files is a real overlap.
        assert!(extent(0, 10, b"m", b"m").conflicts(&extent(5, 15, b"m", b"m")));
        assert!(extent(0, 10, b"a", b"n").conflicts(&extent(0, 10, b"m", b"z")));
        // Touching time ranges do not overlap.
        assert!(!extent(0, 10, b"a", b"z").conflicts(&extent(10, 20, b"a", b"z")));
    }

    #[test]
    fn test_intrusion_covers_every_conflict_inside_an_area() {
        let area = extent(0, 10, b"m", b"z");
        // A file touching the area's boundary key may conflict with an output
        // holding only that key, if it holds only that key itself.
        assert!(extent(0, 10, b"m", b"m").intrudes(&area));
        assert!(!extent(0, 10, b"a", b"m").intrudes(&area));
        assert!(extent(0, 10, b"a", b"n").intrudes(&area));
        assert!(!extent(10, 20, b"a", b"z").intrudes(&area));
    }

    #[test]
    fn test_active_l1_packs_pk_disjoint_files() {
        let files = vec![
            file(0, 1000, b"a", b"a", 100),
            file(0, 1000, b"z", b"z", 100),
        ];
        let pick = pick_active_l1(&files, &primary_key_mapper_for_test(), limits(512)).unwrap();
        assert_eq!(ids(&files), ids(&pick.inputs));
    }

    #[test]
    fn test_active_l1_bounds_rewrite_cost() {
        let overlapping = |sizes: &[u64]| {
            sizes
                .iter()
                .enumerate()
                .map(|(idx, &size)| file(idx as i64, 100, b"a", b"z", size))
                .collect::<Vec<_>>()
        };
        let mapper = primary_key_mapper_for_test();
        assert!(pick_active_l1(&overlapping(&[300, 300, 300]), &mapper, limits(512)).is_some());
        // Removing one file would rewrite 4600 bytes, beyond 4 * 512.
        let costly = overlapping(&[500, 500, 500, 500, 500, 500, 500, 500, 500, 100]);
        assert!(pick_active_l1(&costly, &mapper, limits(512)).is_none());
    }

    #[test]
    fn test_closed_merge_does_not_enclose_other_files() {
        let a = file(0, 10, b"a", b"c", 10);
        let b = file(0, 10, b"x", b"z", 10);
        let c = file(0, 10, b"m", b"m", 1);
        let files = vec![a.clone(), b, c.clone()];
        let pick = closed_merge(&files, MAX_OUTPUT).unwrap();
        // Merging A and B alone would produce a file covering C.
        assert_eq!(ids(&[a, c]), ids(&pick.inputs));
        assert!(pick.reduces_files);
    }

    #[test]
    fn test_closed_merge_skips_components_over_input_limit() {
        // A file spanning all keys overlaps more files than one compaction accepts.
        let mut files = (0..20u8)
            .map(|idx| file(0, 1000, &[b'a', idx], &[b'a', idx], MAX_OUTPUT))
            .collect::<Vec<_>>();
        files.push(file(100, 900, &[b'a', 0], &[b'a', 19], MAX_OUTPUT));
        assert!(closed_merge(&files, MAX_OUTPUT).is_none());
    }

    #[test]
    fn test_closed_merge_rejects_dominant_runs() {
        // Each of these components has a 3 GiB run and a 512 MiB peer.
        let size = 512 * 1024 * 1024;
        let mut files = (0..6u8)
            .map(|idx| file(idx.into(), 10_000, &[b'a' + idx], &[b'a' + idx], size))
            .collect::<Vec<_>>();
        files.push(file(6, 10_000, b"a", b"f", size));
        files.extend((0..6u8).map(|idx| {
            file(
                8 + i64::from(idx),
                10_000,
                &[b'm' + idx],
                &[b'm' + idx],
                size,
            )
        }));
        files.push(file(7, 10_000, b"m", b"r", size));
        // A separate balanced pair is still resolved.
        let pair = [
            file(14, 10_000, b"x", b"x", size),
            file(15, 10_000, b"x", b"x", size),
        ];
        files.extend(pair.clone());

        let pick = closed_merge(&files, size).unwrap();
        assert_eq!(ids(&pair), ids(&pick.inputs));
        assert!(!pick.reduces_files);
    }

    #[test]
    fn test_closed_merge_ignores_touching_time_ranges() {
        let first = file(0, 10, b"a", b"z", 1024);
        let pair = [file(10, 20, b"a", b"z", 512), file(11, 19, b"a", b"z", 512)];
        let mut files = vec![first];
        files.extend(pair.clone());
        let pick = closed_merge(&files, 512).unwrap();
        assert_eq!(ids(&pair), ids(&pick.inputs));
    }

    /// SplitMix64, so the simulation needs no random number dependency.
    struct Rng(u64);

    impl Rng {
        fn next(&mut self, bound: u64) -> u64 {
            self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
            let mut z = self.0;
            z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
            (z ^ (z >> 31)) % bound
        }
    }

    /// A simulated file with its key bounds as numbers.
    #[derive(Clone)]
    struct SimFile {
        handle: FileHandle,
        min: u64,
        max: u64,
    }

    fn sim_file(start: i64, end: i64, min: u64, max: u64, size: u64) -> SimFile {
        let (min_key, max_key) = (format!("{min:04}"), format!("{max:04}"));
        SimFile {
            handle: file(start, end, min_key.as_bytes(), max_key.as_bytes(), size),
            min,
            max,
        }
    }

    /// Writes the outputs of merging `inputs` like the compactor: one stream in
    /// (key, time) order, split by size into consecutive files. The data is
    /// spread evenly over a grid of keys and time slots.
    fn merge(inputs: &[SimFile], rng: &mut Rng) -> Vec<SimFile> {
        const SLOTS: u64 = 8;
        let min = inputs.iter().map(|file| file.min).min().unwrap();
        let max = inputs.iter().map(|file| file.max).max().unwrap();
        let start = inputs
            .iter()
            .map(|file| file.handle.time_range().0.value())
            .min()
            .unwrap();
        let end = inputs
            .iter()
            .map(|file| file.handle.time_range().1.value())
            .max()
            .unwrap();
        let slot_time = |slot: u64| start + (end - start) * slot as i64 / SLOTS as i64;
        let size: u64 = inputs.iter().map(|file| file.handle.size()).sum();
        // Outputs may be fewer than predicted, never more.
        let predicted = size.div_ceil(MAX_OUTPUT).max(1);
        let outputs = predicted - rng.next(2).min(predicted - 1);
        let cells = (max - min + 1) * SLOTS;
        (0..outputs)
            .map(|i| {
                let (first, last) = (cells * i / outputs, cells * (i + 1) / outputs - 1);
                let (first_key, last_key) = (min + first / SLOTS, min + last / SLOTS);
                let (time_start, time_end) = if first_key == last_key {
                    (slot_time(first % SLOTS), slot_time(last % SLOTS + 1))
                } else {
                    (start, end)
                };
                let output_size = size * (i + 1) / outputs - size * i / outputs;
                sim_file(time_start, time_end, first_key, last_key, output_size)
            })
            .collect()
    }

    fn conflict_pairs(files: &[FileHandle]) -> usize {
        let extents = extents_of(files, &primary_key_mapper_for_test());
        (0..extents.len())
            .map(|i| {
                (i + 1..extents.len())
                    .filter(|&j| extents[i].conflicts(&extents[j]))
                    .count()
            })
            .sum()
    }

    fn random_window(rng: &mut Rng) -> Vec<SimFile> {
        (0..2 + rng.next(30))
            .map(|_| {
                let start = rng.next(3_000) as i64;
                let end = start + 1 + rng.next(600) as i64;
                let min = rng.next(200);
                let span = if rng.next(4) == 0 { 200 } else { 20 };
                let max = min + rng.next(span);
                sim_file(start, end, min, max, 1 + rng.next(2 * MAX_OUTPUT))
            })
            .collect()
    }

    /// Applies `pick` until it picks nothing, asserting that `measure` strictly
    /// decreases with every merge. Returns the number of merges.
    fn run_to_fixpoint(
        rng: &mut Rng,
        pick: impl Fn(&[FileHandle]) -> Option<UnitPick>,
        measure: impl Fn(&[FileHandle]) -> (usize, usize),
    ) -> usize {
        let mut total_steps = 0;
        for window in 0..300 {
            let mut files = random_window(rng);
            let handles =
                |files: &[SimFile]| files.iter().map(|f| f.handle.clone()).collect::<Vec<_>>();
            let mut current = measure(&handles(&files));
            let mut steps = 0;
            while let Some(pick) = pick(&handles(&files)) {
                assert!(pick.inputs.len() >= 2);
                let picked = ids(&pick.inputs);
                let (inputs, kept): (Vec<_>, Vec<_>) = files
                    .into_iter()
                    .partition(|file| picked.contains(&file.handle.file_id().file_id()));
                files = kept;
                files.extend(merge(&inputs, rng));
                let next = measure(&handles(&files));
                assert!(next < current, "window {window}: {current:?} -> {next:?}");
                current = next;
                steps += 1;
            }
            total_steps += steps;
        }
        total_steps
    }

    #[test]
    fn test_closed_merges_terminate() {
        let steps = run_to_fixpoint(
            &mut Rng(7),
            |files| closed_merge(files, MAX_OUTPUT),
            |files| (conflict_pairs(files), files.len()),
        );
        assert!(steps > 300);
    }

    #[test]
    fn test_active_l1_picks_terminate() {
        let mapper = primary_key_mapper_for_test();
        let steps = run_to_fixpoint(
            &mut Rng(11),
            |files| pick_active_l1(files, &mapper, limits(MAX_OUTPUT)),
            |files| (files.len(), 0),
        );
        assert!(steps > 200);
    }
}
