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

//! Splitting a source's series ordinals over the partitions that read them.
//!
//! A scan hands each DataFusion partition a slice of one source's stable ordinals, and that
//! partition then reads and evaluates exactly those series. Splitting the ordinals evenly by count
//! is only balanced when every series costs the same; a source whose series differ in length puts
//! the long ones on one partition, which then finishes last while the others sit idle. The split
//! here therefore takes a per-ordinal weight from the prepared source, which reports the row count
//! its series indexes record for each of its ordinals, and divides the series into consecutive runs
//! of near-equal total weight. Those weights are obtained once per source and shared by every
//! partition, rather than rebuilt by each partition that divides them.
//!
//! The runs are not chosen one at a time by taking the one nearest a share: a run that stops at the
//! nearest total leaves its neighbours a split that can be worse than the count split it was meant
//! to improve on. Instead the smallest maximum partition weight that any contiguous split can have is
//! found by binary search, and the series are then packed under it. That buys one thing only: the
//! minimum maximum *estimated* weight, under this additive weight model. The count split is one of
//! the contiguous splits the search minimises over, so for a given cost vector the packing is never
//! worse than the count split as measured by that same weight; it says nothing about elapsed time,
//! because the model itself can be wrong.
//!
//! The weight is the row count a series index records for each series, plus what carrying a series
//! costs on its own before any of its samples are counted: a partition's time is not proportional to
//! rows alone, and weighing by rows only evens the samples while loading the leftover partitions with
//! the series count. One series costs about what [`SERIES_OVERHEAD_ROWS`] rows do, so a source of
//! series much shorter than that overhead tends toward count balance -- the overhead dominates, so
//! the partitions end up with nearly equal series counts -- while a series long enough to dominate
//! that overhead is weighed by its rows. The count formula itself is what genuinely uniform costs
//! get, and a source that cannot report differing weights -- every series weighing the same -- falls
//! back to it as well.
//!
//! The whole split is computed once, as [`partition_boundaries`], and every partition then reads its
//! own range out of it in constant time. Deciding the split costs `O(n log total)` for `n` weights --
//! a binary search over the possible maximum load, one packing pass per step -- and no partition
//! repeats it: P partitions would otherwise scan the source's weights P times just to learn which
//! slice is theirs.
//!
//! A series is never split across partitions: each partition reads a contiguous ordinal range, which
//! is what `read_series` takes, and one series' chunks therefore stay in one partition, as the series
//! continuity and closing-chunk contracts require. A partition can consequently end up with no
//! series at all, because the maximum load is held at its minimum and handing the same work round
//! differently is then a tie the packing does not break: an empty partition is a tie-breaking
//! consequence of minimising the maximum, not evidence that the work did not need that partition.

use std::ops::Range;

/// What one series costs before any of its samples are counted, in rows of sample work.
///
/// A partition's time is not proportional to the rows it reads: every series it is handed costs a
/// fixed amount on its own -- its chunk is built, keyed and merged separately -- and only then is the
/// row count paid for. Measured over the prepared path on one release build, on one machine, with the
/// fixture and the A/B order alternated, one series cost 0.2310 ms and a million rows 72.27 ms, so one
/// series costs about what 3196 rows do (0.2310 / 72.27 * 10^6) -- a fit of about 3.2 thousand rows
/// per series. A least-squares fit over five configurations reproduced every measurement to within a
/// percent.
///
/// The shipped value is 4096, deliberately above that fit as a safety margin, because the error is
/// asymmetric: if the real per-series cost exceeds the assumed one, short series are under-weighted
/// and the split can end up worse than the count split -- a shape with many short series is where
/// that bites -- whereas an assumed cost above the real one only leans the split back toward the
/// count split and gives up part of the win. 4096 stays within 1% of the count split's maximum load
/// for an assumed overhead anywhere up to about 10 thousand rows, which the fit of 3.2 thousand sits
/// comfortably inside.
///
/// That margin is a calibration for a measured workload and build, not an invariant: a different CPU,
/// build or codec mix can shift the ratio, and nothing here guarantees elapsed time across
/// environments.
///
/// Weighing by rows alone therefore trades one imbalance for another: it evens the samples and, on a
/// source of many short series, loads the leftover partitions with the series count instead. On a
/// 2056-series source (8 series of 3600 rows and 2048 of 31) the row-only split gives
/// `[6, 530, 760, 760]` series against the count split's `514` each and runs 1.36x slower. Weighing
/// rows plus the fitted overhead instead gave `[507, 516, 516, 517]` on that fixture -- the shape of
/// the count split rather than of the row-only split -- and ran at the count split's speed; the split
/// here gives `[509, 516, 516, 515]` on it. On a source where the rows really do dominate (8 series of
/// 200000 rows and 56 short ones) they still decide: the split packs `[3, 3, 51, 7]` series whose
/// heaviest partition weighs 612288 against the count split's 1665784, the count split putting all
/// eight long series on one partition.
///
/// The figure is a property of the scan implementation, not of a data set, so it is a constant here
/// rather than something measured per query -- calibrated once, against one workload and one build. A
/// source whose series carry far fewer samples than this therefore tends toward the count split's
/// balance, the assignment it had before any weight existed; only genuinely uniform costs get that
/// assignment exactly.
const SERIES_OVERHEAD_ROWS: usize = 4_096;

/// Returns the boundary of every partition, in ordinal order, ending at the series count.
///
/// `costs` holds one weight per ordinal, in ordinal order, as the source reports them, and the
/// result holds `partitions + 1` non-decreasing indices: partition `p` owns
/// `boundaries[p]..boundaries[p+1]`. A caller builds this once per source and every partition reads
/// its range out of the shared result with [`partition_range`], so a scan pays for the split one time
/// rather than once per partition.
///
/// A source with no series gets all boundaries at zero, and a partition count of zero gets no
/// boundary at all: nothing to divide and nothing to give out. When every series weighs the same,
/// nothing but the count distinguishes one series from another, so the count split decides and the
/// assignment is bit for bit what it was before any weight existed. Fewer series than partitions
/// gives each series a partition of its own and the rest an empty range: a series cannot be divided,
/// so the alternative would be reading one twice.
///
/// Otherwise a series weighs its row count plus [`SERIES_OVERHEAD_ROWS`], and the split is the
/// contiguous split with the smallest achievable maximum weight: the search in [`weighted_boundaries`]
/// finds the smallest maximum that still packs the series into `partitions` runs and then packs under
/// it. The guarantee is exactly that, and only that: the minimum maximum *estimated* weight for this
/// additive weight model. The count split is one of the contiguous splits the search minimises over,
/// so for a given cost vector the packing is never worse than the count split **as measured by that
/// same weight** -- but the model itself can be wrong, so that carries no general promise about
/// measured time. A partition can still end up reading nothing, which is a tie the packing breaks
/// rather than a judgement about the work.
pub(crate) fn partition_boundaries(costs: &[usize], partitions: usize) -> Vec<usize> {
    if partitions == 0 {
        return Vec::new();
    }
    let series = costs.len();
    let uniform = costs
        .first()
        .is_none_or(|first| costs.iter().all(|cost| cost == first));
    if uniform {
        // Uniform weight: the count is the whole of what is known, so keep the count split.
        return (0..=partitions)
            .map(|partition| count_range(series, partition, partitions).start)
            .collect();
    }
    if series < partitions {
        // Fewer series than partitions: one series each, in order, and the rest read nothing.
        return (0..=partitions)
            .map(|partition| partition.min(series))
            .collect();
    }
    weighted_boundaries(costs, partitions, SERIES_OVERHEAD_ROWS)
}

/// Returns the ordinal range the partition `partition` owns, in constant time.
///
/// `boundaries` is the result of [`partition_boundaries`] for the source being read, built once and
/// shared by every partition of the scan. A partition past the end of the split, or a split that
/// covers no ordinal at all, reads an empty range.
pub(crate) fn partition_range(boundaries: &[usize], partition: usize) -> Range<usize> {
    match (boundaries.get(partition), boundaries.get(partition + 1)) {
        (Some(start), Some(end)) => *start..*end,
        // No such partition, or the last one: nothing is assigned to it.
        _ => 0..0,
    }
}

/// Returns the boundaries of the contiguous split with the smallest achievable maximum load.
///
/// `overhead` is what every series weighs on top of its own cost: production passes
/// [`SERIES_OVERHEAD_ROWS`], and the tests pin the packing itself by passing zero. Under a maximum
/// load `L` -- a capacity -- packing the series in ordinal order and cutting only where the next one
/// would overflow takes the fewest runs any split of capacity `L` can take, so the smallest `L` that
/// needs at most `partitions` runs is the smallest maximum load achievable, and packing again under
/// that `L` realises it. A single series always fits in a run of its own, so the search starts at the
/// heaviest series and ends at the total.
///
/// When the lightest packing needs fewer runs than there are partitions, the remaining partitions are
/// given the final index: a partition with nothing to read, rather than a series read twice.
fn weighted_boundaries(costs: &[usize], partitions: usize, overhead: usize) -> Vec<usize> {
    debug_assert!(partitions > 0, "a split needs a partition to fill");
    let series = costs.len();
    if series == 0 {
        return vec![0; partitions + 1];
    }
    // u128 throughout: a source of many long series runs past what a u32 sum holds, and the search
    // halves sums rather than any single cost.
    let weight = |cost: usize| cost.saturating_add(overhead) as u128;

    // The fewest runs capacity `capacity` allows. Greedy is optimal here: a series is only moved to
    // the next run when it cannot join the current one, and any split that broke earlier puts no
    // fewer series in front of this point.
    let runs_at = |capacity: u128| {
        let mut runs = 1;
        let mut load = weight(costs[0]);
        for cost in &costs[1..] {
            let next = weight(*cost);
            if load.saturating_add(next) > capacity {
                runs += 1;
                load = next;
            } else {
                load = load.saturating_add(next);
            }
        }
        runs
    };

    let heaviest = costs.iter().copied().map(weight).max().unwrap_or(0);
    let total: u128 = costs.iter().copied().map(weight).sum();
    let (mut low, mut high) = (heaviest, total);
    while low < high {
        let capacity = low + (high - low) / 2;
        if runs_at(capacity) <= partitions {
            high = capacity;
        } else {
            low = capacity + 1;
        }
    }
    let capacity = low;

    // Pack under the load the search settled on. It needs at most `partitions` runs, so the guard on
    // the run count never forces a series into a run that overflows.
    let mut boundaries = Vec::with_capacity(partitions + 1);
    boundaries.push(0);
    let mut load = 0u128;
    let mut in_run = false;
    for (ordinal, cost) in costs.iter().enumerate() {
        let next = weight(*cost);
        if in_run && boundaries.len() < partitions && load.saturating_add(next) > capacity {
            boundaries.push(ordinal);
            load = next;
        } else {
            load = load.saturating_add(next);
            in_run = true;
        }
    }
    boundaries.push(series);
    // Fewer runs than partitions: the rest read nothing, so that a caller always gets one boundary
    // per partition plus the end. The packing opens at most `partitions` runs, so this only pads.
    boundaries.resize(partitions + 1, series);
    boundaries
}

/// The equal-count split, which is what a uniformly weighted source gets.
fn count_range(series: usize, partition: usize, partitions: usize) -> Range<usize> {
    let series = series as u128;
    let partitions = partitions as u128;
    let start = (series * partition as u128 / partitions) as usize;
    let end = (series * (partition + 1) as u128 / partitions) as usize;
    start..end
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The weight a series carries in production: its cost plus what a series costs by itself.
    fn weight(cost: usize) -> usize {
        cost.saturating_add(SERIES_OVERHEAD_ROWS)
    }

    /// The ranges a vector of boundaries describes.
    fn ranges_of(boundaries: &[usize]) -> Vec<Range<usize>> {
        boundaries.windows(2).map(|pair| pair[0]..pair[1]).collect()
    }

    /// The pure packing, without the per-series overhead.
    ///
    /// The overhead would flatten the shapes a test of the search wants to keep sharp: with it, a
    /// series of a few hundred rows costs about as much as one of one row. Passing zero leaves the
    /// costs as the weights they are being packed by.
    fn packing(costs: &[usize], partitions: usize) -> Vec<Range<usize>> {
        ranges_of(&weighted_boundaries(costs, partitions, 0))
    }

    /// The ranges the production split gives a source, as the scan reads them out of one split.
    fn ranges(costs: &[usize], partitions: usize) -> Vec<Range<usize>> {
        ranges_of(&partition_boundaries(costs, partitions))
    }

    /// Each partition's range and the rows it carries.
    fn split(costs: &[usize], partitions: usize) -> Vec<(Range<usize>, usize)> {
        let ranges = ranges(costs, partitions);
        loads(costs, &ranges, 0)
            .into_iter()
            .zip(ranges)
            .map(|(load, range)| (range, load))
            .collect()
    }

    /// Each partition's range and the weight it carries in production: rows plus the overhead.
    fn weighted_split(costs: &[usize], partitions: usize) -> Vec<(Range<usize>, usize)> {
        let ranges = ranges(costs, partitions);
        loads(costs, &ranges, SERIES_OVERHEAD_ROWS)
            .into_iter()
            .zip(ranges)
            .map(|(load, range)| (range, load))
            .collect()
    }

    /// The count split's ranges, which the weighted split must never be worse than.
    fn count_split(costs: &[usize], partitions: usize) -> Vec<Range<usize>> {
        (0..partitions)
            .map(|partition| count_range(costs.len(), partition, partitions))
            .collect()
    }

    /// What each partition carries, in the unit the split weighs: rows plus `overhead` per series.
    fn loads(costs: &[usize], ranges: &[Range<usize>], overhead: usize) -> Vec<usize> {
        ranges
            .iter()
            .map(|range| {
                costs[range.clone()]
                    .iter()
                    .map(|cost| cost.saturating_add(overhead))
                    .sum()
            })
            .collect()
    }

    /// The heaviest partition of a list of loads.
    fn max_of(loads: &[usize]) -> usize {
        loads.iter().copied().max().unwrap_or(0)
    }

    /// The heaviest partition's rows.
    fn max_load(costs: &[usize], ranges: &[Range<usize>]) -> usize {
        max_of(&loads(costs, ranges, 0))
    }

    /// The heaviest partition's weight, in production units: rows plus the per-series overhead.
    fn max_weight(costs: &[usize], partitions: usize) -> usize {
        max_of(&loads(
            costs,
            &ranges(costs, partitions),
            SERIES_OVERHEAD_ROWS,
        ))
    }

    /// The heaviest partition of the count split, in the same units as [`max_weight`].
    fn count_max_weight(costs: &[usize], partitions: usize) -> usize {
        max_of(&loads(
            costs,
            &count_split(costs, partitions),
            SERIES_OVERHEAD_ROWS,
        ))
    }

    /// A deterministic generator, so a failure is reproducible without a random crate or a seed file.
    struct Lcg(u64);

    impl Lcg {
        /// Numerical Recipes' multiplier and increment: cheap, and mixed well enough to spread small
        /// costs over the whole range.
        fn next_below(&mut self, bound: usize) -> usize {
            self.0 = self
                .0
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            ((self.0 >> 33) % bound as u64) as usize
        }
    }

    #[test]
    fn equal_costs_split_by_count() {
        let costs = vec![10; 8];
        assert_eq!(
            split(&costs, 4),
            vec![(0..2, 20), (2..4, 20), (4..6, 20), (6..8, 20),]
        );
        assert_eq!(
            count_split(&costs, 4),
            ranges(&costs, 4),
            "a source of equal series must keep the count split"
        );
    }

    /// The case the split exists for: a few long series among many short ones.
    ///
    /// Splitting these by count gives the first partition every long series, so it reads most of the
    /// samples while the others finish early; packed by weight the long ones land one per partition
    /// and every partition carries a quarter of the rows. Pinned on the pure packing, because under
    /// the per-series overhead a 1000-row series is short enough that keeping four of them together
    /// is the cheaper split -- [`SERIES_OVERHEAD_ROWS`] is exactly what makes those two answers
    /// differ, and this test is about the packing.
    #[test]
    fn long_series_are_spread_over_partitions() {
        const PARTITIONS: usize = 4;
        let mut costs = vec![1000; PARTITIONS];
        costs.extend(std::iter::repeat_n(10, 40));
        let per_partition = packing(&costs, PARTITIONS);
        let long_per_partition = per_partition
            .iter()
            .map(|range| {
                range
                    .clone()
                    .filter(|ordinal| *ordinal < PARTITIONS)
                    .count()
            })
            .collect::<Vec<_>>();
        assert_eq!(
            vec![1, 1, 1, 1],
            long_per_partition,
            "the long series are not spread one per partition: {per_partition:?}"
        );
        let total: usize = costs.iter().sum();
        let per_partition_loads = loads(&costs, &per_partition, 0);
        assert_eq!(
            total,
            per_partition_loads.iter().sum::<usize>(),
            "the split lost or duplicated cost"
        );
        for (range, load) in per_partition.iter().zip(&per_partition_loads) {
            assert!(
                *load * 2 <= total,
                "partition {range:?} carries {load} of {total}: {per_partition_loads:?}"
            );
        }
        // The count split puts every long series on the first partition and is just as unbalanced
        // as the rows allow.
        assert!(
            max_load(&costs, &count_split(&costs, PARTITIONS)) > max_of(&per_partition_loads),
            "the count split was expected to be worse than the packing"
        );
    }

    /// A long tail of short series behind a few long ones, which is what a real source looks like.
    ///
    /// The costs are in ordinal order and the long series come first, the arrangement a count split
    /// punishes hardest when the rows dominate. At 1000 rows one series still costs less than the
    /// per-series overhead, so the split stays within a couple of percent of the count split -- and
    /// never heavier than it in the weight the split minimises, which is the model-relative property
    /// that lets the count split be replaced at all. The shapes where the rows do dominate are
    /// [`series_that_dwarf_the_per_series_overhead_are_split_by_their_rows`].
    #[test]
    fn few_long_series_among_many_short_ones_are_balanced() {
        const PARTITIONS: usize = 4;
        const LONG_SERIES: usize = 8;
        const SHORT_SERIES: usize = 2048;
        const LONG_ROWS: usize = 1000;
        const SHORT_ROWS: usize = 10;

        let mut costs = vec![LONG_ROWS; LONG_SERIES];
        costs.extend(std::iter::repeat_n(SHORT_ROWS, SHORT_SERIES));
        let per_partition = weighted_split(&costs, PARTITIONS);
        let total: usize = per_partition.iter().map(|(_, weight)| weight).sum();
        let average = total as f64 / PARTITIONS as f64;
        let expected: usize = costs.iter().copied().map(weight).sum();
        assert_eq!(
            expected, total,
            "the split lost or duplicated the weight it divides"
        );

        // (a) The heaviest partition stays close to the average weight.
        let heaviest = max_weight(&costs, PARTITIONS);
        assert!(
            heaviest as f64 / average <= 1.05,
            "the heaviest partition carries {heaviest} against an average of {average}: \
             {per_partition:?}"
        );

        // (b) It is no worse than the count split, the assignment it replaces.
        let count_heaviest = count_max_weight(&costs, PARTITIONS);
        assert!(
            heaviest <= count_heaviest,
            "the weighted split is worse than the count split: {heaviest} against {count_heaviest}"
        );

        // (c) The ranges still cover every ordinal exactly once, in order.
        let mut next = 0;
        for (range, _) in &per_partition {
            assert_eq!(range.start, next, "gap or overlap in {per_partition:?}");
            next = range.end;
        }
        assert_eq!(
            costs.len(),
            next,
            "the last range ends early in {per_partition:?}"
        );
    }

    /// Balance holds even when one series dwarfs the rest.
    ///
    /// One long series cannot be divided, so whichever partition owns it carries that cost; the split
    /// must not pile the remaining series onto the same partition as well. What it may do is leave a
    /// partition empty, because a series is never cut across partitions and the maximum load is held
    /// at its minimum: the empty partition is a tie the packing does not break, not a claim that the
    /// work did not need it.
    #[test]
    fn one_dominant_series_does_not_drag_the_rest_with_it() {
        let costs = [1000, 1, 1, 1, 1, 1, 1, 1];
        let per_partition = split(&costs, 4);
        let total: usize = per_partition.iter().map(|(_, cost)| cost).sum();
        assert_eq!(total, 1007, "the split lost or duplicated cost");
        // The long series lands on one partition; the short ones are spread over the others rather
        // than following it.
        let with_long = per_partition
            .iter()
            .filter(|(range, _)| range.contains(&0))
            .count();
        assert_eq!(with_long, 1, "the long series is in more than one range");
        let short_holders = per_partition
            .iter()
            .filter(|(range, cost)| *cost > 0 && !range.contains(&0))
            .count();
        assert!(
            short_holders >= 2,
            "the short series did not spread: {per_partition:?}"
        );
    }

    /// Every ordinal is covered exactly once, whatever the costs and partition count are.
    #[test]
    fn ranges_are_contiguous_and_cover_every_ordinal() {
        for costs in [
            vec![1, 2, 3, 4, 5, 6, 7],
            vec![100, 1, 1, 1, 100, 1],
            vec![5],
            vec![0, 0, 0, 7, 0, 0],
            vec![1000, 1, 1, 1],
        ] {
            for partitions in 1..=7 {
                let boundaries = partition_boundaries(&costs, partitions);
                assert_eq!(
                    partitions + 1,
                    boundaries.len(),
                    "one boundary per partition, plus the end, for {costs:?}/{partitions}"
                );
                assert_eq!(
                    0, boundaries[0],
                    "the split must start at the first ordinal"
                );
                assert_eq!(
                    costs.len(),
                    *boundaries.last().unwrap(),
                    "the last range ends early in {costs:?}/{partitions}"
                );
                assert!(
                    boundaries.windows(2).all(|pair| pair[0] <= pair[1]),
                    "the boundaries are not increasing in {costs:?}/{partitions}: {boundaries:?}"
                );
                let mut next = 0;
                for range in ranges(&costs, partitions) {
                    assert_eq!(
                        range.start, next,
                        "gap or overlap in {costs:?}/{partitions}"
                    );
                    next = range.end;
                }
                assert_eq!(next, costs.len(), "the last range ends early in {costs:?}");
            }
        }
    }

    /// Fewer series than partitions reads each series once and leaves the rest empty.
    ///
    /// There is nothing to give the extra partitions, so an empty range is the honest answer rather
    /// than an error or a partition reading another's series twice.
    #[test]
    fn extra_partitions_read_nothing() {
        let costs = vec![7, 8];
        assert_eq!(ranges(&costs, 4), vec![0..1, 1..2, 2..2, 2..2]);
        assert_eq!(ranges(&[], 4), vec![0..0, 0..0, 0..0, 0..0]);
        assert_eq!(ranges(&costs, 2), vec![0..1, 1..2]);
        assert_eq!(ranges(&costs, 0), Vec::<Range<usize>>::new());
        // A partition past the end of the split, or a split with no partition, reads nothing.
        let boundaries = partition_boundaries(&costs, 2);
        assert_eq!(partition_range(&boundaries, 2), 0..0);
        assert_eq!(partition_range(&boundaries, 7), 0..0);
        assert_eq!(partition_range(&[], 0), 0..0);
    }

    /// A source with no cost information is split by count, exactly as before.
    #[test]
    fn unknown_costs_fall_back_to_the_count_split() {
        let costs = vec![0; 9];
        assert_eq!(
            ranges(&costs, 3),
            vec![0..3, 3..6, 6..9],
            "a source without cost information must keep the count split"
        );
    }

    /// A uniformly weighted source keeps exactly the assignment the count split gave it.
    ///
    /// This is the contract the scanner relies on: a source that cannot report differing weights must
    /// not have a series moved between partitions by the weighting. Fewer series than partitions is
    /// the case where a front-filling split would move one, and the count split is what keeps two
    /// scans of the same region on the same partitions.
    #[test]
    fn uniform_costs_reproduce_the_count_split() {
        for series in 0..12 {
            for partitions in 1..=6 {
                let costs = vec![1; series];
                for (partition, range) in ranges(&costs, partitions).into_iter().enumerate() {
                    assert_eq!(
                        count_range(series, partition, partitions),
                        range,
                        "uniform weight of {series} series over {partitions} partitions moved series"
                    );
                }
            }
        }
    }

    /// A share that is not a whole number must not cost the split its smallest maximum.
    ///
    /// These costs total 13 over 4 partitions, so no split can have a maximum below 4, and
    /// `[2, 2 | 2 | 4 | 1, 2]` reaches exactly that. This replaces the test of the nearest-share
    /// rounding the old greedy needed: no run is stopped at a share any more, so what is left to pin
    /// is that the smallest achievable maximum is the one the search finds, dropped remainder and all.
    #[test]
    fn a_fractional_share_reaches_the_smallest_maximum() {
        let costs = [2, 2, 2, 4, 1, 2];
        let packed = packing(&costs, 4);
        assert_eq!(
            vec![0..2, 2..3, 3..4, 4..6],
            packed,
            "the split does not reach the smallest maximum: {packed:?}"
        );
        // 13 rows over 4 partitions cannot go below 4, and the split reaches it.
        assert_eq!(4, max_load(&costs, &packed));
        // The count split reaches the same maximum here, and must not be beaten either.
        assert!(max_load(&costs, &packed) <= max_load(&costs, &count_split(&costs, 4)));
        // The production weight (overhead included) keeps the same packing here.
        assert_eq!(packed, ranges(&costs, 4));
    }

    /// The same relative weights must split the same way whatever unit they are reported in.
    ///
    /// Every packing decision compares a sum against a capacity, so scaling all the costs by one
    /// factor scales capacity and sums together and cannot change which series belong together.
    /// Pinned on the pure packing: the per-series overhead is added, not multiplied, so it does not
    /// scale with the costs, and a scaled source may rightly split differently once it is weighed as
    /// rows plus that fixed overhead.
    #[test]
    fn the_split_does_not_depend_on_the_unit_of_the_costs() {
        for costs in [vec![2, 2, 2, 4, 1, 2], vec![4, 16, 16, 7, 8]] {
            let expected = packing(&costs, 4);
            for scale in [10, 1000, 100_000] {
                let scaled = costs.iter().map(|cost| cost * scale).collect::<Vec<_>>();
                assert_eq!(
                    expected,
                    packing(&scaled, 4),
                    "scaling {costs:?} by {scale} moved the split"
                );
            }
        }
    }

    /// The first shape the old nearest-share split made worse than the count split.
    ///
    /// `[4, 16, 16, 7, 8]` over 4 partitions: taking each run nearest its share loaded
    /// `[20, 16, 7, 8]`, a maximum of 20 where the count split needs 16. The smallest maximum a
    /// contiguous split can have here is 16 -- only `[7, 8]` can share a run -- so the split packs the
    /// tail together and is no worse than the count split. Weighed without the per-series overhead, so
    /// that this pins the search rather than the constant.
    #[test]
    fn the_first_reviewers_counterexample_is_no_worse_than_the_count_split() {
        let costs = [4, 16, 16, 7, 8];
        let packed = packing(&costs, 4);
        let load = max_load(&costs, &packed);
        let by_count = max_load(&costs, &count_split(&costs, 4));
        assert!(
            load <= 16,
            "the split carries {load} where 16 is enough: {packed:?}"
        );
        assert!(
            load <= by_count,
            "the split carries {load}, worse than the count split's {by_count}: {packed:?}"
        );
    }

    /// The second shape the old nearest-share split made worse than the count split.
    ///
    /// `[36, ..., 1000]` over 8 partitions: the old split put 994 and 997 in the same run for 1991,
    /// while the best contiguous split stays at 1236. The count split, taking two series per
    /// partition, carries 1997. Weighed without the per-series overhead, so that this pins the search
    /// rather than the constant.
    #[test]
    fn the_second_reviewers_counterexample_is_no_worse_than_the_count_split() {
        let costs = [
            36, 38, 57, 59, 212, 253, 275, 306, 433, 754, 793, 917, 940, 994, 997, 1000,
        ];
        let packed = packing(&costs, 8);
        let load = max_load(&costs, &packed);
        let by_count = max_load(&costs, &count_split(&costs, 8));
        assert!(
            load <= 1236,
            "the split carries {load} where 1236 is enough: {packed:?}"
        );
        assert!(
            load <= by_count,
            "the split carries {load}, worse than the count split's {by_count}: {packed:?}"
        );
    }

    /// No shape at all may come out worse than the count split.
    ///
    /// This is the property that lets the count split be replaced: the count split is one of the
    /// contiguous splits the search minimises over, so the weighted split's heaviest partition can
    /// never be heavier -- in rows without the overhead, and in weights with it. The shapes are
    /// enumerated and then generated from a fixed seed, so a failure can be reproduced.
    #[test]
    fn the_split_is_never_worse_than_the_count_split() {
        let mut shapes = vec![
            // The two shapes the old nearest-share split lost on.
            vec![4, 16, 16, 7, 8],
            vec![
                36, 38, 57, 59, 212, 253, 275, 306, 433, 754, 793, 917, 940, 994, 997, 1000,
            ],
        ];
        // Series counts on both sides of the partition counts, and spreads wide enough that the
        // count split can starve a partition.
        let mut rng = Lcg(0x5eed_1234_9abc_def0);
        for series in [1usize, 2, 3, 5, 7, 8, 13, 16, 17, 31, 64] {
            for spread in [1usize, 2, 7, 100, 5000] {
                shapes.push((0..series).map(|_| 1 + rng.next_below(spread)).collect());
            }
        }
        for costs in &shapes {
            for partitions in 1..=8 {
                let packed = packing(costs, partitions);
                let by_count = count_split(costs, partitions);
                let packed_load = max_load(costs, &packed);
                let count_load = max_load(costs, &by_count);
                assert!(
                    packed_load <= count_load,
                    "{costs:?} over {partitions} partitions: {packed_load} {packed:?} against the \
                     count split's {count_load} {by_count:?}"
                );
                // The same holds in the weights production minimises, overhead included.
                let weighted = max_of(&loads(
                    costs,
                    &ranges(costs, partitions),
                    SERIES_OVERHEAD_ROWS,
                ));
                let count_weight = max_of(&loads(costs, &by_count, SERIES_OVERHEAD_ROWS));
                assert!(
                    weighted <= count_weight,
                    "{costs:?} over {partitions} partitions: weight {weighted} against the count \
                     split's {count_weight}"
                );
            }
        }
    }

    /// Many short series behind a few longer ones must not load the leftover partitions with series.
    ///
    /// This is the case the row count alone gets wrong, measured on the prepared path: eight series
    /// of 3600 rows beside 2048 of 31. Evened by rows alone the first partition takes the long ones
    /// and stops after 6 series, leaving 760 to each of the rest against the count split's 514, and
    /// the scan ran 1.36x slower than the count split. A series costs about what
    /// `SERIES_OVERHEAD_ROWS` rows do, so at these lengths the overhead is what the split evens out,
    /// and it lands near the count split's own assignment instead of 760 series per partition.
    #[test]
    fn many_short_series_behind_a_few_long_ones_stay_near_the_count_split() {
        const PARTITIONS: usize = 4;
        let mut costs = vec![3600; 8];
        costs.extend(std::iter::repeat_n(31, 2048));
        let per_partition = ranges(&costs, PARTITIONS);
        let counts = per_partition
            .iter()
            .map(|range| range.len())
            .collect::<Vec<_>>();
        let count_counts = count_split(&costs, PARTITIONS)
            .iter()
            .map(|range| range.len())
            .collect::<Vec<_>>();
        for (partition, count) in counts.iter().enumerate() {
            let even = count_counts[partition];
            assert!(
                count.abs_diff(even) <= even / 10,
                "partition {partition} holds {count} series against the count split's {even}: \
                 {counts:?}"
            );
        }
        // And the tail is not loaded with them, which is what the row-only split did.
        assert!(
            counts[PARTITIONS - 1] < 3 * count_counts[PARTITIONS - 1] / 2,
            "the last partition was handed {counts:?} against the count split's {count_counts:?}"
        );
    }

    /// A series long enough to dwarf its own overhead is still split by its rows.
    ///
    /// The constant must not turn the split back into the count split for the shape the weighting
    /// exists for: eight series of 200000 rows beside 56 short ones. Evened by count the first
    /// partition takes every long series; evened by weight the long ones are spread over three of the
    /// four partitions, and the heaviest partition carries 612288 against the count split's 1665784.
    /// The split is min-max optimal in the weight being minimised, but which packing of that weight
    /// reaches the optimum is a tie the greedy pass breaks: it fills the first runs to the capacity
    /// and leaves the tail one light, rather than evening the long series out two apiece.
    #[test]
    fn series_that_dwarf_the_per_series_overhead_are_split_by_their_rows() {
        const PARTITIONS: usize = 4;
        const LONG_SERIES: usize = 8;
        let mut costs = vec![200_000; LONG_SERIES];
        costs.extend(std::iter::repeat_n(31, 56));
        let per_partition = ranges(&costs, PARTITIONS);
        let long_per_partition = per_partition
            .iter()
            .map(|range| {
                range
                    .clone()
                    .filter(|ordinal| *ordinal < LONG_SERIES)
                    .count()
            })
            .collect::<Vec<_>>();
        assert_eq!(
            vec![3, 3, 2, 0],
            long_per_partition,
            "the long series are not spread over the partitions: {per_partition:?}"
        );
        // No partition carries more than half the weight, where the count split loads the first one
        // with every long series: the weights, not the counts, are what is even.
        let total: usize = costs.iter().copied().map(weight).sum();
        let per_partition_weights = loads(&costs, &per_partition, SERIES_OVERHEAD_ROWS);
        assert_eq!(
            total,
            per_partition_weights.iter().sum::<usize>(),
            "the split lost or duplicated the weight it divides"
        );
        for (range, carried) in per_partition.iter().zip(&per_partition_weights) {
            assert!(
                *carried * 2 <= total,
                "partition {range:?} carries {carried} of {total}: {per_partition_weights:?}"
            );
        }
    }

    /// The split is decided once, and reading a partition out of it is constant time.
    ///
    /// This is what keeps a scan's split cost independent of its partition count: the boundaries are
    /// built once from the weights, and no partition scans the source's costs to find its slice.
    #[test]
    fn every_partition_reads_its_range_out_of_the_shared_boundaries() {
        let costs = [1000, 1, 1, 1, 1, 1, 1, 1];
        let boundaries = partition_boundaries(&costs, 4);
        assert_eq!(
            5,
            boundaries.len(),
            "one boundary per partition, plus the end"
        );
        assert_eq!(0, boundaries[0]);
        assert_eq!(costs.len(), *boundaries.last().unwrap());
        assert!(
            boundaries.windows(2).all(|pair| pair[0] <= pair[1]),
            "the boundaries are not increasing: {boundaries:?}"
        );
        for (partition, expected) in split(&costs, 4).into_iter().enumerate() {
            assert_eq!(expected.0, partition_range(&boundaries, partition));
        }
    }
}
