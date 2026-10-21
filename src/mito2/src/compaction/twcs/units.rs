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
//! - [`pick_closed_merge`] only merges intervals of an inactive window whose
//!   outputs cannot add conflicts. Compaction outputs stay inside the bounding
//!   box of the inputs, so they can only overlap files intruding on that box.
//!
//! When a window's split-unit conflict graph is too dense to materialize,
//! [`pick_closed_merge`] falls back to a bounded file reduction and admits it
//! only when the same conflict bound holds.
//!
//! # Termination
//!
//! Let `P` be the number of conflicting file pairs ([`Extent::conflicts`]) in an
//! inactive window and `N` its file count. A merge removes every pair among its
//! inputs. Its outputs are one sorted run, whose files only share boundary keys,
//! so they only conflict with the `I` files intruding on the inputs' box, at
//! most `outputs · I` pairs. An inactive pick only merges inputs that had at
//! least as many pairs with other files, so `P` does not grow; without
//! intruders the inputs are closed and add no pairs. Every inactive pick either
//! resolves a pair (`P` drops) or removes a file (`P` does not grow and `N`
//! drops), so `(P, N)` strictly decreases in lexicographic order until the
//! window receives new writes. Active-window picks may add conflicts, so each
//! of them must remove a file: `N` strictly decreases.
//!
//! The argument relies on how the compactor writes outputs:
//!
//! - Outputs hold a subset of the input rows, so each output's time and key
//!   bounds lie inside the inputs' bounding box.
//! - Outputs are one stream in (key, time) order split by size, so two outputs
//!   share at most a boundary key, and two outputs of a single key have
//!   touching time ranges. Neither is an [`Extent::conflicts`] pair.
//! - A split only starts a new file once the current one reaches the size
//!   threshold, and outputs are not larger than inputs in total, so
//!   [`PickLimits::predicted_outputs`] bounds the output count.
//!
//! `test_closed_merges_terminate` and `test_active_l1_picks_terminate` replay
//! random windows with outputs written under these rules.

use std::cmp::Reverse;
use std::ops::Range;

use bytes::Bytes;
use common_time::Timestamp;

use crate::sst::file::FileHandle;
use crate::sst::primary_key::PrimaryKeyRangeMapper;

/// An L1 file reduction may rewrite at most this many output files' worth of
/// bytes per SST it removes.
const MAX_REWRITE_FILES_PER_REDUCED_FILE: u64 = 4;

/// Maximum number of index nodes visited while building conflict structures for
/// a pick. Above this, the picker falls back to a bounded file reduction instead
/// of materializing an unbounded conflict graph.
const CONFLICT_GRAPH_BUDGET: usize = 1 << 20;

/// Output size limits and input limits shared by both picks.
#[derive(Debug, Clone, Copy)]
pub(crate) struct PickLimits {
    /// Split threshold of compaction outputs; `None` writes a single output.
    pub(crate) max_output_file_size: Option<u64>,
    /// Maximum number of input files of one compaction.
    pub(crate) max_input_files: usize,
}

impl PickLimits {
    /// Upper bound of the outputs of compacting an interval: the writer only
    /// starts a new file after the current one reaches the threshold, and never
    /// splits a series, so files of a single primary key compact into one file.
    fn predicted_outputs(&self, stats: &IntervalStats) -> usize {
        match self.max_output_file_size {
            Some(max) if max > 0 && stats.single_key.is_none() => {
                stats.size.div_ceil(max).max(1) as usize
            }
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
    /// Whether this pick came from the bounded dense-window fallback.
    pub(crate) dense_fallback: bool,
}

/// Picks L1 files of the active window whose compaction removes files.
///
/// Units are groups of conflicting files, so a merge does not leave a file
/// behind while rewriting its overlapping peers. A group larger than the input
/// limit is split back into single files. If no interval of groups qualifies,
/// for example because a large file dominates its group, the groups are split
/// into single files, so the smaller files can still be merged. Intervals
/// follow [`sort_units`]; the pick removes the most files, then rewrites the
/// fewest bytes.
pub(crate) fn pick_active_l1(
    l1_files: &[FileHandle],
    mapper: &PrimaryKeyRangeMapper,
    options: PickLimits,
) -> Option<UnitPick> {
    pick_active_l1_with_budget(l1_files, mapper, options, CONFLICT_GRAPH_BUDGET)
}

/// [`pick_active_l1`] with an explicit work budget, so tests can exercise the
/// single-file fallback without a window-sized conflict graph.
fn pick_active_l1_with_budget(
    l1_files: &[FileHandle],
    mapper: &PrimaryKeyRangeMapper,
    options: PickLimits,
    work_budget: usize,
) -> Option<UnitPick> {
    let extents = extents_of(l1_files, mapper);
    let index = ExtentIndex::new(&extents);
    let mut units = match conflict_units(l1_files, &extents, &index, work_budget) {
        Some(units) => units,
        None => single_file_units(l1_files, &extents),
    };
    split_oversized_units(&mut units, l1_files, &extents, options.max_input_files);
    if let Some(pick) = pick_reducing_interval(&mut units, l1_files, options) {
        return Some(pick);
    }
    if units.len() == l1_files.len() {
        return None;
    }
    split_oversized_units(&mut units, l1_files, &extents, 1);
    pick_reducing_interval(&mut units, l1_files, options)
}

/// Picks the interval of `units` that removes the most files within the
/// rewrite budget, then rewrites the fewest bytes.
fn pick_reducing_interval(
    units: &mut [Unit],
    files: &[FileHandle],
    options: PickLimits,
) -> Option<UnitPick> {
    sort_units(units);
    let range = pick_reducing_interval_where(units, options, |_| true)?;
    Some(UnitPick {
        inputs: collect_inputs(&units[range], files),
        reduces_files: true,
        units: units.len(),
        dense_fallback: false,
    })
}

/// Selects a reducing interval of sorted units, with an additional filter so
/// the dense fallback can retry without duplicating the reduction guards.
fn pick_reducing_interval_where(
    units: &[Unit],
    options: PickLimits,
    is_eligible: impl Fn(&IntervalStats) -> bool,
) -> Option<Range<usize>> {
    let mut best: Option<(usize, u64, Range<usize>)> = None;
    // File reductions ignore conflicts, so split files are not counted.
    for_each_interval(units, &[], options.max_input_files, |range, stats| {
        let reduction = stats
            .num_files
            .saturating_sub(options.predicted_outputs(stats));
        if stats.num_files < 2
            || reduction == 0
            || !options.within_reduction_cost(stats.size, reduction)
            || stats.largest_file_size > stats.size - stats.largest_file_size
            || !is_eligible(stats)
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

    best.map(|(_, _, range)| range)
}

/// Thresholds of the closed merge pick.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ClosedMergeOptions {
    pub(crate) limits: PickLimits,
    /// Whether merges that only remove files are allowed.
    pub(crate) allow_file_reduction: bool,
    /// Whether merges may rewrite conflicting files. Otherwise only
    /// conflict-free intervals are merged.
    pub(crate) allow_conflicts: bool,
    /// Whether overlap merges may rewrite a dominant sorted run for a small
    /// peer. Otherwise such a merge would be repeated whenever late data adds
    /// another small peer.
    pub(crate) allow_unbalanced: bool,
}

/// Picks an interval of units among all files of an inactive window.
///
/// Units are the closures of conflicting files: no file outside a unit intrudes
/// on its bounding box. A closure larger than the input limit is split back into
/// single files, so files around a late file spanning a large sorted run can
/// still be merged. Intervals follow [`sort_units`]. An interval is eligible if
/// its outputs add no more conflicts than its files have with other files (see
/// [`adds_no_conflicts`]), and it
///
/// - removes files within the rewrite budget, or
/// - resolves conflicts without rewriting a dominant sorted run (unless
///   [`ClosedMergeOptions::allow_unbalanced`]), and adds at most one tail file.
///
/// The pick maximizes resolved conflicts plus removed files per rewritten byte.
/// A window whose split-unit conflict graph is too dense to materialize falls
/// back to a bounded file reduction; see [`pick_dense_closed_merge`].
pub(crate) fn pick_closed_merge(
    files: &[FileHandle],
    mapper: &PrimaryKeyRangeMapper,
    options: ClosedMergeOptions,
) -> Option<UnitPick> {
    pick_closed_merge_with_budget(files, mapper, options, CONFLICT_GRAPH_BUDGET)
}

/// [`pick_closed_merge`] with an explicit work budget, so tests can exercise
/// the dense fallback without building a window-sized conflict graph.
fn pick_closed_merge_with_budget(
    files: &[FileHandle],
    mapper: &PrimaryKeyRangeMapper,
    options: ClosedMergeOptions,
    work_budget: usize,
) -> Option<UnitPick> {
    let limits = options.limits;
    let extents = extents_of(files, mapper);
    let index = ExtentIndex::new(&extents);
    let mut dense = false;
    let mut units = match conflict_units(files, &extents, &index, work_budget) {
        Some(mut units) => {
            close_units(&mut units, &extents, &index);
            split_oversized_units(&mut units, files, &extents, limits.max_input_files);
            units
        }
        None => {
            dense = true;
            single_file_units(files, &extents)
        }
    };
    sort_units(&mut units);
    if dense {
        return pick_dense_closed_merge(&units, files, &extents, &index, options);
    }
    let Some(pairs) = split_conflicts(&units, &extents, &index, work_budget) else {
        return pick_dense_closed_merge(&units, files, &extents, &index, options);
    };

    let mut best: Option<(u64, u64, Range<usize>, bool)> = None;
    for_each_interval(&units, &pairs, limits.max_input_files, |range, stats| {
        if stats.num_files < 2 {
            return;
        }
        let outputs = limits.predicted_outputs(stats);
        if outputs > stats.num_files + 1 {
            return;
        }
        let reduction = stats.num_files.saturating_sub(outputs);
        let reduces_files = reduction > 0
            && (options.allow_file_reduction || stats.conflicts > 0)
            && limits.within_reduction_cost(stats.size, reduction);
        if (stats.conflicts > 0 && !options.allow_conflicts)
            || (!reduces_files && stats.conflicts == 0)
        {
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
        // Overlap merges must not rewrite a dominant run for a small peer. A
        // file-reducing merge is already bounded by its rewrite budget.
        if !reduces_files
            && !options.allow_unbalanced
            && !has_balanced_components(units, files, &extents, limits.run_tolerance())
        {
            return;
        }
        if units.len() > 1 && !adds_no_conflicts(units, &extents, &index, outputs) {
            return;
        }
        best = Some((benefit, stats.size, range, reduces_files));
    });

    best.map(|(_, _, range, reduces_files)| UnitPick {
        inputs: collect_inputs(&units[range], files),
        reduces_files,
        units: units.len(),
        dense_fallback: false,
    })
}

/// Picks a bounded file reduction when the split-unit conflict graph is too
/// dense to materialize.
///
/// The candidate comes from [`pick_reducing_interval_where`], which ignores
/// conflicts. If rejected, it is searched once more for a one-output interval.
/// Both candidates must pass [`adds_no_conflicts`], so the `(P, N)` termination
/// argument still holds. Searching only within the original candidate adds at
/// most O(K²) interval visits for K input files and one extra conflict check,
/// rather than checking external conflicts for every interval in the window.
/// This bounded retry can still miss other safely reducible intervals.
fn pick_dense_closed_merge(
    units: &[Unit],
    files: &[FileHandle],
    extents: &[Extent],
    index: &ExtentIndex,
    options: ClosedMergeOptions,
) -> Option<UnitPick> {
    let limits = options.limits;
    let range = pick_reducing_interval_where(units, limits, |_| true)?;
    let mut selected = &units[range];
    if !is_admissible_dense_interval(selected, extents, index, options) {
        let range = pick_reducing_interval_where(selected, limits, |stats| {
            limits.predicted_outputs(stats) == 1
        })?;
        selected = &selected[range];
        if !is_admissible_dense_interval(selected, extents, index, options) {
            return None;
        }
    }
    Some(UnitPick {
        inputs: collect_inputs(selected, files),
        reduces_files: true,
        units: units.len(),
        dense_fallback: true,
    })
}

/// File reduction and rewrite limits are checked by the interval picker;
/// each dense candidate must also respect the closed-window conflict policy.
fn is_admissible_dense_interval(
    units: &[Unit],
    extents: &[Extent],
    index: &ExtentIndex,
    options: ClosedMergeOptions,
) -> bool {
    let stats = interval_stats_exact(units, extents);
    (options.allow_file_reduction || stats.conflicts > 0)
        && (options.allow_conflicts || stats.conflicts == 0)
        && adds_no_conflicts(
            units,
            extents,
            index,
            options.limits.predicted_outputs(&stats),
        )
}

/// Totals of an interval without materializing [`split_conflicts`].
///
/// After [`close_units`], files of different units never conflict, so only
/// split units can conflict across units, and each split unit holds one file.
/// The dense fallback may instead treat every file as a split unit, and then
/// the same pairwise loop covers every cross-unit pair. Counting them takes
/// `O(K²)` for an interval of at most `K` files, instead of building a
/// window-wide adjacency.
fn interval_stats_exact<'a>(units: &'a [Unit], extents: &[Extent]) -> IntervalStats<'a> {
    let mut stats = IntervalStats::default();
    let mut first = true;
    for unit in units {
        stats.num_files += unit.files.len();
        stats.size += unit.size;
        stats.largest_file_size = stats.largest_file_size.max(unit.largest_file_size);
        stats.conflicts += unit.conflicts;
        stats.single_key = if first {
            unit.single_key()
        } else {
            stats
                .single_key
                .filter(|&key| unit.single_key() == Some(key))
        };
        first = false;
    }

    let split_files: Vec<usize> = units
        .iter()
        .filter(|unit| unit.split)
        .map(|unit| unit.files[0])
        .collect();
    for (offset, &lhs) in split_files.iter().enumerate() {
        for &rhs in &split_files[offset + 1..] {
            if extents[lhs].conflicts(&extents[rhs]) {
                stats.conflicts += 1;
            }
        }
    }
    stats
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
    /// Key ranges that share only a key at the boundary of both do not conflict
    /// unless both hold that single key. A compaction output split by size
    /// shares boundary keys with its neighbors, and counting them would make
    /// every sorted run look overlapping. A key inside the other range is a real
    /// overlap.
    pub(crate) fn conflicts(&self, other: &Self) -> bool {
        self.time_overlaps(other)
            && match (&self.primary_key, &other.primary_key) {
                (Some(lhs), Some(rhs)) => {
                    let (low, high) = ((&lhs.0).max(&rhs.0), (&lhs.1).min(&rhs.1));
                    low < high
                        || (low == high
                            && ((is_single_key(lhs) && is_single_key(rhs))
                                || is_inner_key(low, lhs)
                                || is_inner_key(low, rhs)))
                }
                _ => true,
            }
    }

    /// Whether `self` may conflict with a file inside `area`, which is the
    /// bounding box of a group of files.
    ///
    /// Sharing a single key with the area is enough if `self` holds only that
    /// key, or holds it inside its range and the area is that single key.
    fn intrudes(&self, area: &Self) -> bool {
        self.time_overlaps(area)
            && match (&self.primary_key, &area.primary_key) {
                (Some(lhs), Some(rhs)) => {
                    let (low, high) = ((&lhs.0).max(&rhs.0), (&lhs.1).min(&rhs.1));
                    low < high || (low == high && (is_single_key(lhs) || is_inner_key(low, lhs)))
                }
                _ => true,
            }
    }
}

fn is_single_key(range: &(Bytes, Bytes)) -> bool {
    range.0 == range.1
}

/// Whether `key` lies strictly inside `range`, not at its bounds.
fn is_inner_key(key: &Bytes, range: &(Bytes, Bytes)) -> bool {
    range.0 < *key && *key < range.1
}

/// Upper primary key bound, ordered after every key when unknown.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum UpperKey<'a> {
    Key(&'a Bytes),
    Unbounded,
}

/// Key and time bounds of the files in a subtree of [`ExtentIndex`].
#[derive(Debug, Clone, Copy)]
struct SubtreeBounds<'a> {
    max_upper_key: UpperKey<'a>,
    min_start: Timestamp,
    max_end: Timestamp,
}

impl<'a> SubtreeBounds<'a> {
    fn of(extent: &'a Extent) -> Self {
        Self {
            max_upper_key: match &extent.primary_key {
                Some((_, max)) => UpperKey::Key(max),
                None => UpperKey::Unbounded,
            },
            min_start: extent.start,
            max_end: extent.end,
        }
    }

    fn merge(self, other: Self) -> Self {
        Self {
            max_upper_key: self.max_upper_key.max(other.max_upper_key),
            min_start: self.min_start.min(other.min_start),
            max_end: self.max_end.max(other.max_end),
        }
    }
}

/// Files of a window ordered by their lower primary key, in a segment tree
/// holding the bounds of each subtree.
///
/// A query visits the files whose key and time ranges may overlap an area and
/// skips subtrees whose keys end before the area or whose times miss it. The
/// files of a sorted run split by key then only meet their key neighbors,
/// instead of every file sharing their time range.
struct ExtentIndex<'a> {
    extents: &'a [Extent],
    /// Indices into `extents`, sorted by lower key with unknown keys first.
    /// Sorted offset `i` is leaf `leaf_base + i`.
    by_lower_key: Vec<usize>,
    /// Array-backed tree rooted at index 1; padding leaves are `None`.
    bounds: Vec<Option<SubtreeBounds<'a>>>,
    /// First leaf index, a power of two.
    leaf_base: usize,
}

impl<'a> ExtentIndex<'a> {
    fn new(extents: &'a [Extent]) -> Self {
        let lower_key = |i: usize| extents[i].primary_key.as_ref().map(|(min, _)| min);
        let mut by_lower_key: Vec<usize> = (0..extents.len()).collect();
        by_lower_key.sort_by(|&lhs, &rhs| {
            (lower_key(lhs), extents[lhs].start).cmp(&(lower_key(rhs), extents[rhs].start))
        });
        let leaf_base = extents.len().next_power_of_two();
        let mut bounds = vec![None; 2 * leaf_base];
        for (offset, &i) in by_lower_key.iter().enumerate() {
            bounds[leaf_base + offset] = Some(SubtreeBounds::of(&extents[i]));
        }
        for node in (1..leaf_base).rev() {
            bounds[node] = match (bounds[2 * node], bounds[2 * node + 1]) {
                (Some(lhs), Some(rhs)) => Some(lhs.merge(rhs)),
                (lhs, rhs) => lhs.or(rhs),
            };
        }
        Self {
            extents,
            by_lower_key,
            bounds,
            leaf_base,
        }
    }

    /// Visits the files whose inclusive key range meets `area`'s and whose time
    /// range overlaps it, a superset of the files that conflict with or intrude
    /// on `area`. Takes O(log N) per visited file.
    fn for_each_near(&self, area: &Extent, visit: impl FnMut(usize)) {
        let mut budget = usize::MAX;
        let _ = self.for_each_near_with_budget(area, &mut budget, visit);
    }

    /// [`for_each_near`] with a budget on visited tree nodes.
    ///
    /// Every node reached by the traversal consumes one unit, including nodes
    /// pruned before any file callback. Returns `false` as soon as the budget is
    /// exhausted, so a caller can stop a traversal whose pruning is poor even
    /// when it finds few files.
    fn for_each_near_with_budget(
        &self,
        area: &Extent,
        budget: &mut usize,
        mut visit: impl FnMut(usize),
    ) -> bool {
        let leaves_end = match &area.primary_key {
            Some((_, max)) => self.by_lower_key.partition_point(|&i| {
                self.extents[i]
                    .primary_key
                    .as_ref()
                    .is_none_or(|(min, _)| min <= max)
            }),
            None => self.by_lower_key.len(),
        };
        self.visit_subtree(1, 0..self.leaf_base, leaves_end, area, budget, &mut visit)
    }

    fn visit_subtree(
        &self,
        node: usize,
        leaves: Range<usize>,
        leaves_end: usize,
        area: &Extent,
        budget: &mut usize,
        visit: &mut impl FnMut(usize),
    ) -> bool {
        if *budget == 0 {
            return false;
        }
        *budget -= 1;
        if leaves.start >= leaves_end {
            return true;
        }
        let Some(bounds) = self.bounds[node] else {
            return true;
        };
        if bounds.min_start >= area.end
            || bounds.max_end <= area.start
            || area
                .primary_key
                .as_ref()
                .is_some_and(|(min, _)| bounds.max_upper_key < UpperKey::Key(min))
        {
            return true;
        }
        if leaves.len() == 1 {
            let i = self.by_lower_key[leaves.start];
            if self.extents[i].time_overlaps(area) {
                visit(i);
            }
            return true;
        }
        let mid = leaves.start + leaves.len() / 2;
        if !self.visit_subtree(2 * node, leaves.start..mid, leaves_end, area, budget, visit) {
            return false;
        }
        self.visit_subtree(
            2 * node + 1,
            mid..leaves.end,
            leaves_end,
            area,
            budget,
            visit,
        )
    }
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
    /// A single file split from a unit over the input limit. It may conflict
    /// with other split files, see [`split_conflicts`].
    split: bool,
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
            split: false,
        }
    }

    /// The minimum primary key, with unknown keys ordered first.
    fn first_key(&self) -> Option<&Bytes> {
        self.extent.primary_key.as_ref().map(|(min, _)| min)
    }

    /// The primary key of a unit whose files all hold that single key.
    fn single_key(&self) -> Option<&Bytes> {
        self.extent
            .primary_key
            .as_ref()
            .filter(|range| is_single_key(range))
            .map(|(min, _)| min)
    }
}

/// Groups files into connected components of [`Extent::conflicts`], each with
/// its number of conflicting pairs. Each file only checks the files the index
/// finds near it: O((N + M) log N) for M pairs of files whose key and time
/// ranges meet.
///
/// Returns `None` once the index traversal has visited more than `work_budget`
/// nodes, so a dense window can fall back before building a quadratic graph.
fn conflict_units(
    files: &[FileHandle],
    extents: &[Extent],
    index: &ExtentIndex,
    work_budget: usize,
) -> Option<Vec<Unit>> {
    let mut sets = DisjointSet::new(extents.len());
    let mut conflicts = vec![0; extents.len()];
    let mut budget = work_budget;
    for i in 0..extents.len() {
        let completed = index.for_each_near_with_budget(&extents[i], &mut budget, |j| {
            // Counts each pair once, from its smaller index.
            if j > i && extents[i].conflicts(&extents[j]) {
                sets.union(i, j);
                conflicts[i] += 1;
            }
        });
        if !completed {
            return None;
        }
    }
    Some(
        sets.groups()
            .into_iter()
            .map(|members| {
                let pairs = members.iter().map(|&i| conflicts[i]).sum();
                Unit::new(members, files, extents, pairs)
            })
            .collect(),
    )
}

/// Treats every file as its own split unit, for the dense fallback that cannot
/// afford to build conflict components.
fn single_file_units(files: &[FileHandle], extents: &[Extent]) -> Vec<Unit> {
    (0..files.len())
        .map(|i| Unit {
            split: true,
            ..Unit::new(vec![i], files, extents, 0)
        })
        .collect()
}

/// Replaces units with more files than one compaction accepts by single files,
/// which are no longer closed.
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
            .map(|i| Unit {
                split: true,
                ..Unit::new(vec![i], files, extents, 0)
            }),
    );
    *units = kept;
}

/// Merges units until no file intrudes on a unit it does not belong to.
///
/// Each check only visits the files the index finds near the unit, and each
/// merge re-checks only the merged unit.
fn close_units(units: &mut Vec<Unit>, extents: &[Extent], index: &ExtentIndex) {
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
        let area = &units[u].extent;
        let mut intruders = std::collections::BTreeSet::new();
        index.for_each_near(area, |i| {
            if owner[i] != u && extents[i].intrudes(area) {
                intruders.insert(owner[i]);
            }
        });
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
struct IntervalStats<'a> {
    num_files: usize,
    size: u64,
    largest_file_size: u64,
    conflicts: usize,
    /// The primary key of an interval whose files all hold that single key.
    single_key: Option<&'a Bytes>,
}

/// Positions of the split units each unit conflicts with, sorted. Files of
/// different units never conflict, so only split files have any.
///
/// Returns `None` once the index traversal has visited more than `work_budget`
/// nodes, so a dense window cannot materialize an unbounded adjacency.
fn split_conflicts(
    units: &[Unit],
    extents: &[Extent],
    index: &ExtentIndex,
    work_budget: usize,
) -> Option<Vec<Vec<usize>>> {
    let mut position = vec![None; extents.len()];
    for (p, unit) in units.iter().enumerate() {
        if unit.split {
            position[unit.files[0]] = Some(p);
        }
    }
    let mut budget = work_budget;
    let mut result = Vec::with_capacity(units.len());
    for unit in units {
        if !unit.split {
            result.push(Vec::new());
            continue;
        }
        let i = unit.files[0];
        let mut neighbors = Vec::new();
        let completed = index.for_each_near_with_budget(&extents[i], &mut budget, |j| {
            if let Some(p) = position[j]
                && j != i
                && extents[i].conflicts(&extents[j])
            {
                neighbors.push(p);
            }
        });
        if !completed {
            return None;
        }
        neighbors.sort_unstable();
        result.push(neighbors);
    }
    Some(result)
}

/// Visits every contiguous interval of units with at most `max_input_files`
/// files: O(U · K) intervals for an input limit K. Each interval counts the
/// conflicts of its split files from `split_conflicts` (see
/// [`split_conflicts`]) with a binary search; an empty `split_conflicts`
/// skips them.
fn for_each_interval<'a>(
    units: &'a [Unit],
    split_conflicts: &[Vec<usize>],
    max_input_files: usize,
    mut visit: impl FnMut(Range<usize>, &IntervalStats<'a>),
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
            stats.single_key = if right == left {
                unit.single_key()
            } else {
                stats
                    .single_key
                    .filter(|&key| unit.single_key() == Some(key))
            };
            if let Some(neighbors) = split_conflicts.get(right) {
                // Conflicts with the split files already in the interval.
                stats.conflicts += neighbors.partition_point(|&p| p < right)
                    - neighbors.partition_point(|&p| p < left);
            }
            visit(left..right + 1, &stats);
        }
    }
}

/// Whether merging `units` into `outputs` files adds no more conflicts than
/// their files have with other files. Outputs stay inside the bounding box of
/// the inputs, so each of them may conflict with every file intruding on that
/// box. An interval without intruders is closed and adds no conflicts.
fn adds_no_conflicts(
    units: &[Unit],
    extents: &[Extent],
    index: &ExtentIndex,
    outputs: usize,
) -> bool {
    let area = units[1..]
        .iter()
        .fold(units[0].extent.clone(), |area, unit| {
            area.union(&unit.extent)
        });
    let selected: std::collections::HashSet<usize> = units
        .iter()
        .flat_map(|unit| unit.files.iter().copied())
        .collect();
    let mut intruders = Vec::new();
    index.for_each_near(&area, |i| {
        if !selected.contains(&i) && extents[i].intrudes(&area) {
            intruders.push(i);
        }
    });
    if intruders.is_empty() {
        return true;
    }
    let existing: usize = intruders
        .iter()
        .map(|&other| {
            units
                .iter()
                .flat_map(|unit| unit.files.iter())
                .filter(|&&i| extents[i].conflicts(&extents[other]))
                .count()
        })
        .sum();
    outputs.saturating_mul(intruders.len()) <= existing
}

/// Whether every group of conflicting files among `units` is balanced, see
/// [`has_balanced_runs`]. Runs of separate groups never overlap, so pairing a
/// dominant run with the peer of another group would hide it. Takes O(K²)
/// conflict checks for K files.
fn has_balanced_components(
    units: &[Unit],
    files: &[FileHandle],
    extents: &[Extent],
    tolerance: u64,
) -> bool {
    let members: Vec<usize> = units
        .iter()
        .flat_map(|unit| unit.files.iter().copied())
        .collect();
    let mut sets = DisjointSet::new(members.len());
    for (i, &lhs) in members.iter().enumerate() {
        for (j, &rhs) in members.iter().enumerate().skip(i + 1) {
            if extents[lhs].conflicts(&extents[rhs]) {
                sets.union(i, j);
            }
        }
    }
    sets.groups()
        .into_iter()
        .filter(|group| group.len() > 1)
        .all(|group| {
            has_balanced_runs(
                group.into_iter().map(|i| members[i]),
                files,
                extents,
                tolerance,
            )
        })
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

    /// Files that all overlap each other in time and primary key.
    fn dense_files(count: usize) -> Vec<FileHandle> {
        (0..count)
            .map(|idx| file(0, 100, b"a", b"z", 1 + (idx % 3) as u64))
            .collect()
    }

    /// A chain of files whose key ranges overlap their immediate neighbors.
    fn chain_files(count: usize, size: u64) -> Vec<FileHandle> {
        (0..count)
            .map(|idx| {
                let min = format!("{idx:04}");
                let max = format!("{:04}", idx + 2);
                file(0, 100, min.as_bytes(), max.as_bytes(), size)
            })
            .collect()
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
            closed_merge_options(max_output_file_size),
        )
    }

    fn closed_merge_with_budget(
        files: &[FileHandle],
        max_output_file_size: u64,
        work_budget: usize,
    ) -> Option<UnitPick> {
        pick_closed_merge_with_budget(
            files,
            &primary_key_mapper_for_test(),
            closed_merge_options(max_output_file_size),
            work_budget,
        )
    }

    fn closed_merge_options(max_output_file_size: u64) -> ClosedMergeOptions {
        ClosedMergeOptions {
            limits: limits(max_output_file_size),
            allow_file_reduction: true,
            allow_conflicts: true,
            allow_unbalanced: false,
        }
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
        // So is a single key inside another range.
        assert!(extent(0, 10, b"m", b"m").conflicts(&extent(0, 10, b"a", b"z")));
        assert!(extent(0, 10, b"a", b"z").conflicts(&extent(0, 10, b"m", b"m")));
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
        // A range may conflict with a single-key file inside a single-key area.
        let point = extent(0, 10, b"m", b"m");
        assert!(extent(0, 10, b"a", b"z").intrudes(&point));
        assert!(!extent(0, 10, b"a", b"m").intrudes(&point));
    }

    #[test]
    fn test_intrusion_covers_conflicts_of_random_files() {
        let mut rng = Rng(23);
        let random_extent = |rng: &mut Rng| {
            let start = rng.next(10) as i64;
            let end = start + 1 + rng.next(10) as i64;
            let min = rng.next(6) as u8;
            let max = min + rng.next(3) as u8;
            extent(start, end, &[b'0' + min], &[b'0' + max])
        };
        for _ in 0..2_000 {
            let group: Vec<_> = (0..1 + rng.next(3))
                .map(|_| random_extent(&mut rng))
                .collect();
            let area = group[1..]
                .iter()
                .fold(group[0].clone(), |area, extent| area.union(extent));
            let other = random_extent(&mut rng);
            if group.iter().any(|extent| other.conflicts(extent)) {
                assert!(other.intrudes(&area), "{other:?} conflicts inside {area:?}");
            }
        }
    }

    #[test]
    fn test_extent_index_budget_counts_nodes() {
        // The root's time bounds straddle the query, but neither leaf overlaps
        // it. A callback-only budget would never fire; the node budget must.
        let files = vec![file(0, 10, b"a", b"z", 1), file(90, 100, b"a", b"z", 1)];
        let extents = extents_of(&files, &primary_key_mapper_for_test());
        let index = ExtentIndex::new(&extents);
        let query = extent(50, 51, b"a", b"z");
        let mut matched = 0;

        let mut budget = 2;
        assert!(!index.for_each_near_with_budget(&query, &mut budget, |_| matched += 1));
        assert_eq!(0, budget);
        assert_eq!(0, matched);

        let mut budget = 3;
        assert!(index.for_each_near_with_budget(&query, &mut budget, |_| matched += 1));
        assert_eq!(0, budget);
        assert_eq!(0, matched);
    }

    #[test]
    fn test_extent_index_matches_linear_scan() {
        let mut rng = Rng(29);
        let mapper = primary_key_mapper_for_test();
        for count in [0, 1, 2, 7, 64, 200] {
            let extents: Vec<_> = (0..count)
                .map(|_| {
                    let start = rng.next(100) as i64;
                    let end = start + rng.next(30) as i64;
                    let min = rng.next(50) as u8;
                    let max = min + rng.next(10) as u8;
                    let range = if rng.next(10) > 0 {
                        pk_range(&[b'0' + min], &[b'0' + max])
                    } else {
                        None
                    };
                    let file = new_file_handle_with_size_sequence_and_primary_key_range(
                        FileId::random(),
                        start,
                        end,
                        1,
                        1,
                        1,
                        range,
                    );
                    Extent::of_file(&file, &mapper)
                })
                .collect();
            let index = ExtentIndex::new(&extents);
            for area in &extents {
                let mut near = Vec::new();
                index.for_each_near(area, |i| near.push(i));
                near.sort_unstable();
                let expected: Vec<_> = (0..extents.len())
                    .filter(|&i| {
                        let other = &extents[i];
                        let keys_meet = match (&other.primary_key, &area.primary_key) {
                            (Some(lhs), Some(rhs)) => lhs.0 <= rhs.1 && rhs.0 <= lhs.1,
                            _ => true,
                        };
                        keys_meet && other.time_overlaps(area)
                    })
                    .collect();
                assert_eq!(expected, near, "{area:?}");
                for (i, other) in extents.iter().enumerate() {
                    if other.conflicts(area) || other.intrudes(area) {
                        assert!(near.binary_search(&i).is_ok(), "{other:?} near {area:?}");
                    }
                }
            }
        }
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
    fn test_active_l1_merges_small_files_beside_dominant_file() {
        // All files overlap. Merging the group would rewrite the large file for
        // its small peers, so only the small files are merged.
        let mut files = vec![file(0, 1000, b"a", b"z", 1024)];
        files.extend((1..16).map(|start| file(start, 1000, b"a", b"z", 1)));
        let pick = pick_active_l1(&files, &primary_key_mapper_for_test(), limits(512)).unwrap();
        assert_eq!(ids(&files[1..]), ids(&pick.inputs));
    }

    #[test]
    fn test_merges_of_a_single_key_write_one_file() {
        // The writer never splits a series, so the overlapping files of one key
        // compact into a single file however large they are.
        let files = vec![file(0, 100, b"m", b"m", 32), file(50, 150, b"m", b"m", 32)];
        let mapper = primary_key_mapper_for_test();
        let pick = closed_merge(&files, MAX_OUTPUT).unwrap();
        assert_eq!(ids(&files), ids(&pick.inputs));
        assert!(pick.reduces_files);
        let pick = pick_active_l1(&files, &mapper, limits(MAX_OUTPUT)).unwrap();
        assert_eq!(ids(&files), ids(&pick.inputs));
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

    /// A sorted run of `sizes.len()` key-sliced files, and files spanning all
    /// of its keys in the given time ranges, which overlap the whole run.
    fn run_with_late_files(
        sizes: &[u64],
        late: &[(i64, i64)],
    ) -> (Vec<FileHandle>, Vec<FileHandle>) {
        let run = sizes
            .iter()
            .enumerate()
            .map(|(idx, &size)| {
                let idx = 2 * idx as u8;
                file(0, 1000, &[b'a', idx], &[b'a', idx + 2], size)
            })
            .collect::<Vec<_>>();
        let max_key = 2 * sizes.len() as u8;
        let late = late
            .iter()
            .map(|&(start, end)| file(start, end, &[b'a', 0], &[b'a', max_key], 1))
            .collect();
        (run, late)
    }

    #[test]
    fn test_closed_merge_splits_components_over_input_limit() {
        // The late file joins all 20 run files into one component, over the
        // input limit. Small run files are still merged.
        let (run, late) = run_with_late_files(&[1; 20], &[(100, 900)]);
        let files = [run, late].concat();
        let pick = closed_merge(&files, MAX_OUTPUT).unwrap();
        assert!(pick.reduces_files);
        assert!(pick.inputs.len() <= 16);
    }

    #[test]
    fn test_closed_merge_leaves_late_file_over_large_run() {
        // Resolving the overlap would rewrite the whole run, beyond the input
        // limit, and merging part of it would only spread the overlap.
        let (run, late) = run_with_late_files(&[MAX_OUTPUT; 20], &[(100, 900)]);
        let files = [run, late].concat();
        assert!(closed_merge(&files, MAX_OUTPUT).is_none());
    }

    #[test]
    fn test_closed_merge_compacts_late_files_over_large_run() {
        let (run, late) =
            run_with_late_files(&[MAX_OUTPUT; 20], &[(100, 200), (300, 400), (500, 600)]);
        let files = [run, late.clone()].concat();
        let pick = closed_merge(&files, MAX_OUTPUT).unwrap();
        assert_eq!(ids(&late), ids(&pick.inputs));
        assert!(pick.reduces_files);
    }

    #[test]
    fn test_closed_merge_rejects_dominant_runs() {
        // Each of these components has a 3 GiB run and a 512 MiB peer.
        let size = 512 * 1024 * 1024;
        // The peer spans the keys of its run, written before or after it.
        let mut files = (0..6u8)
            .map(|idx| file(idx.into(), 10_000, &[b'b' + idx], &[b'b' + idx], size))
            .collect::<Vec<_>>();
        files.push(file(6, 10_000, b"a", b"h", size));
        files.extend((0..6u8).map(|idx| {
            file(
                8 + i64::from(idx),
                10_000,
                &[b'n' + idx],
                &[b'n' + idx],
                size,
            )
        }));
        files.push(file(7, 10_000, b"m", b"t", size));
        // A separate balanced pair is still resolved.
        let pair = [
            file(14, 10_000, b"x", b"y", size),
            file(15, 10_000, b"x", b"y", size),
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

    #[test]
    fn test_conflict_units_respects_work_budget() {
        let files = dense_files(20);
        let mapper = primary_key_mapper_for_test();
        let extents = extents_of(&files, &mapper);
        let index = ExtentIndex::new(&extents);

        assert!(conflict_units(&files, &extents, &index, 0).is_none());
        assert!(conflict_units(&files, &extents, &index, usize::MAX).is_some());
    }

    #[test]
    fn test_split_conflicts_respects_work_budget() {
        let files = dense_files(20);
        let mapper = primary_key_mapper_for_test();
        let extents = extents_of(&files, &mapper);
        let index = ExtentIndex::new(&extents);
        let mut units = conflict_units(&files, &extents, &index, usize::MAX).unwrap();
        close_units(&mut units, &extents, &index);
        split_oversized_units(&mut units, &files, &extents, 16);
        sort_units(&mut units);

        assert_eq!(20, units.iter().filter(|unit| unit.split).count());
        assert!(split_conflicts(&units, &extents, &index, 0).is_none());
        assert!(split_conflicts(&units, &extents, &index, usize::MAX).is_some());
    }

    #[test]
    fn test_interval_stats_exact_matches_for_each_interval() {
        let mapper = primary_key_mapper_for_test();
        // One closed unit with internal conflicts, and one oversized component
        // split into single-file units with cross-unit conflicts.
        let mut files = (0..10)
            .map(|_| file(0, 100, b"a", b"c", 1))
            .collect::<Vec<_>>();
        files.extend((0..20).map(|_| file(0, 100, b"x", b"z", 1)));
        let extents = extents_of(&files, &mapper);
        let index = ExtentIndex::new(&extents);
        let mut units = conflict_units(&files, &extents, &index, usize::MAX).unwrap();
        close_units(&mut units, &extents, &index);
        split_oversized_units(&mut units, &files, &extents, 16);
        sort_units(&mut units);

        let pairs = split_conflicts(&units, &extents, &index, usize::MAX).unwrap();
        for_each_interval(&units, &pairs, 16, |range, stats| {
            let exact = interval_stats_exact(&units[range], &extents);
            assert_eq!(stats.num_files, exact.num_files);
            assert_eq!(stats.size, exact.size);
            assert_eq!(stats.largest_file_size, exact.largest_file_size);
            assert_eq!(stats.conflicts, exact.conflicts);
            assert_eq!(stats.single_key, exact.single_key);
        });
    }

    #[test]
    fn test_active_l1_falls_back_on_dense_conflict_graph() {
        let files = dense_files(300);
        let pick = pick_active_l1_with_budget(
            &files,
            &primary_key_mapper_for_test(),
            limits(MAX_OUTPUT),
            1 << 10,
        )
        .unwrap();
        assert!(!pick.dense_fallback);
        assert!(pick.reduces_files);
        assert!((2..=16).contains(&pick.inputs.len()));
    }

    #[test]
    fn test_dense_closed_merge_reduces_files() {
        let files = dense_files(300);
        let pick = closed_merge_with_budget(&files, MAX_OUTPUT, 1 << 10).unwrap();
        assert!(pick.dense_fallback);
        assert!(pick.reduces_files);
        assert!((2..=16).contains(&pick.inputs.len()));
    }

    #[test]
    fn test_dense_closed_merge_resolves_conflicts_without_file_reduction_setting() {
        let files = dense_files(300);
        let mapper = primary_key_mapper_for_test();
        let options = ClosedMergeOptions {
            allow_file_reduction: false,
            ..closed_merge_options(MAX_OUTPUT)
        };
        let pick = pick_closed_merge_with_budget(&files, &mapper, options, 1 << 10).unwrap();
        assert!(pick.dense_fallback);
        assert!(pick.reduces_files);
    }

    #[test]
    fn test_dense_closed_merge_defers_when_conflicts_are_not_allowed() {
        let files = dense_files(300);
        let mapper = primary_key_mapper_for_test();
        let options = ClosedMergeOptions {
            allow_conflicts: false,
            ..closed_merge_options(MAX_OUTPUT)
        };
        assert!(pick_closed_merge_with_budget(&files, &mapper, options, 1 << 10).is_none());
    }

    #[test]
    fn test_dense_closed_merge_retries_one_output_interval() {
        // PR #9741: the first 16 files predict two outputs but have only one
        // conflict with the rest of the chain. The first eight predict one
        // output, so rejecting the larger interval must not prevent progress.
        let files = chain_files(300, 2);
        let mapper = primary_key_mapper_for_test();
        let extents = extents_of(&files, &mapper);
        let index = ExtentIndex::new(&extents);
        let mut units = conflict_units(&files, &extents, &index, usize::MAX).unwrap();
        close_units(&mut units, &extents, &index);
        split_oversized_units(&mut units, &files, &extents, 16);
        sort_units(&mut units);

        assert!(!adds_no_conflicts(&units[..16], &extents, &index, 2));
        assert!(adds_no_conflicts(&units[..8], &extents, &index, 1));
        // Cover both single-file units from an exhausted grouping budget and
        // split units from an oversized closure.
        for pick in [
            closed_merge_with_budget(&files, MAX_OUTPUT, 0).unwrap(),
            pick_dense_closed_merge(
                &units,
                &files,
                &extents,
                &index,
                closed_merge_options(MAX_OUTPUT),
            )
            .unwrap(),
        ] {
            assert_eq!(ids(&files[..8]), ids(&pick.inputs));
            assert!(pick.dense_fallback);
            assert!(pick.reduces_files);
            assert_eq!(300, pick.units);
        }
    }

    #[test]
    fn test_dense_closed_merge_rejects_unsafe_one_output_retry() {
        // Intruders lie in the time gaps between inputs, so even a one-output
        // merge can introduce conflicts that no input previously had.
        let mut files: Vec<_> = (0..4)
            .map(|i| file(30 * i, 30 * i + 10, b"a", b"z", 6))
            .collect();
        files.extend((0..3).map(|i| file(30 * i + 15, 30 * i + 25, b"b", b"y", 1_000)));

        assert!(closed_merge_with_budget(&files, MAX_OUTPUT, 0).is_none());
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
        // Outputs may be fewer than predicted, never more. A single key is
        // never split.
        let predicted = if min == max {
            1
        } else {
            size.div_ceil(MAX_OUTPUT).max(1)
        };
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

    /// A sorted run of more files than one compaction accepts, and a few files
    /// spanning many of its keys, which join them into oversized components.
    fn oversized_window(rng: &mut Rng) -> Vec<SimFile> {
        let run_files = 17 + rng.next(40);
        let mut files: Vec<_> = (0..run_files)
            .map(|idx| {
                let start = rng.next(100) as i64;
                let end = 2_900 + rng.next(100) as i64;
                sim_file(
                    start,
                    end,
                    5 * idx,
                    5 * (idx + 1),
                    1 + rng.next(2 * MAX_OUTPUT),
                )
            })
            .collect();
        files.extend((0..1 + rng.next(6)).map(|_| {
            let start = rng.next(2_800) as i64;
            let end = start + 1 + rng.next(200) as i64;
            let min = rng.next(5 * run_files / 2);
            let max = min + 5 * run_files / 2 + rng.next(5 * run_files / 2);
            sim_file(start, end, min, max, 1 + rng.next(MAX_OUTPUT))
        }));
        files
    }

    #[test]
    fn test_dense_closed_merge_terminates() {
        let mut rng = Rng(23);
        let mut files: Vec<_> = (0..300)
            .map(|_| sim_file(0, 100, 0, 8, 1 + rng.next(2 * MAX_OUTPUT)))
            .collect();
        let handles = |files: &[SimFile]| {
            files
                .iter()
                .map(|file| file.handle.clone())
                .collect::<Vec<_>>()
        };
        let mut current = (conflict_pairs(&handles(&files)), files.len());
        let mut steps = 0;
        let mut dense_steps = 0;
        while let Some(pick) = closed_merge_with_budget(&handles(&files), MAX_OUTPUT, 1 << 10) {
            assert!(pick.inputs.len() >= 2);
            if pick.dense_fallback {
                dense_steps += 1;
            }
            let picked = ids(&pick.inputs);
            let (inputs, kept): (Vec<_>, Vec<_>) = files
                .into_iter()
                .partition(|file| picked.contains(&file.handle.file_id().file_id()));
            files = kept;
            files.extend(merge(&inputs, &mut rng));
            let next = (conflict_pairs(&handles(&files)), files.len());
            assert!(next < current, "step {steps}: {current:?} -> {next:?}");
            current = next;
            steps += 1;
            assert!(steps < 1_000);
        }
        assert!(dense_steps > 0);
        assert!(files.len() < 300);
    }

    /// Applies `pick` until it picks nothing, asserting that `measure` strictly
    /// decreases with every merge. Returns the number of merges.
    fn run_to_fixpoint(
        rng: &mut Rng,
        pick: impl Fn(&[FileHandle]) -> Option<UnitPick>,
        measure: impl Fn(&[FileHandle]) -> (usize, usize),
    ) -> usize {
        run_windows_to_fixpoint(rng, random_window, pick, measure)
    }

    fn run_windows_to_fixpoint(
        rng: &mut Rng,
        new_window: impl Fn(&mut Rng) -> Vec<SimFile>,
        pick: impl Fn(&[FileHandle]) -> Option<UnitPick>,
        measure: impl Fn(&[FileHandle]) -> (usize, usize),
    ) -> usize {
        let mut total_steps = 0;
        for window in 0..300 {
            let mut files = new_window(rng);
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
    fn test_closed_merges_of_oversized_components_terminate() {
        let steps = run_windows_to_fixpoint(
            &mut Rng(19),
            oversized_window,
            |files| closed_merge(files, MAX_OUTPUT),
            |files| (conflict_pairs(files), files.len()),
        );
        assert!(steps > 300);
    }

    #[test]
    fn test_closed_merges_of_quiet_and_settling_windows_terminate() {
        let mapper = primary_key_mapper_for_test();
        // Quiet windows take merges a default window refuses; settling windows
        // skip the conflicting ones.
        for (seed, options, more_merges) in [
            (
                13,
                ClosedMergeOptions {
                    allow_unbalanced: true,
                    ..closed_merge_options(MAX_OUTPUT)
                },
                true,
            ),
            (
                17,
                ClosedMergeOptions {
                    allow_conflicts: false,
                    ..closed_merge_options(MAX_OUTPUT)
                },
                false,
            ),
        ] {
            let steps = run_to_fixpoint(
                &mut Rng(seed),
                |files| pick_closed_merge(files, &mapper, options),
                |files| (conflict_pairs(files), files.len()),
            );
            let base = run_to_fixpoint(
                &mut Rng(seed),
                |files| closed_merge(files, MAX_OUTPUT),
                |files| (conflict_pairs(files), files.len()),
            );
            assert_eq!(more_merges, steps > base, "seed {seed}: {steps} vs {base}");
        }
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
