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
//! here therefore takes a per-ordinal weight from the source's
//! [`series_cost`](store_api::region_engine::PreparedSeriesSource::series_cost) and gives each
//! partition series of roughly equal total weight. Those weights are obtained once per source and
//! shared by every partition, rather than rebuilt by each partition that divides them.
//!
//! The weight is the row count a series index records for each series, so a partition's share is a
//! share of the samples rather than of the series. A source that cannot report differing weights --
//! every series weighing the same -- falls back to the count split, which is exactly what it got
//! before any weight existed.
//!
//! The whole split is computed once, as [`partition_boundaries`], and every partition then reads its
//! own range out of it in constant time. Deciding the split costs one pass over the weights and the
//! number of partitions, and no partition repeats it: P partitions would otherwise scan the source's
//! weights P times just to learn which slice is theirs.
//!
//! A series is never split across partitions: each partition reads a contiguous ordinal range, which
//! is what `read_series` takes, and one series' chunks therefore stay in one partition, as the series
//! continuity and closing-chunk contracts require. A partition can consequently end up with no
//! series at all when the weights put everything on its neighbours, and that is the correct outcome
//! rather than a defect: it means the work genuinely did not need that partition.

use std::ops::Range;

/// Returns the boundary of every partition, in ordinal order, ending at the series count.
///
/// `costs` holds one weight per ordinal, in ordinal order, as the source reports them, and the
/// result holds `partitions + 1` increasing indices: partition `p` owns
/// `boundaries[p]..boundaries[p+1]`. A caller builds this once per source and every partition reads
/// its range out of the shared result with [`partition_range`], so a scan pays for the split one time
/// rather than once per partition.
///
/// A source with no series gets all boundaries at zero, and a partition count of zero gets no
/// boundary at all: nothing to divide and nothing to give out. When every series weighs the same,
/// nothing but the count distinguishes one series from another, so the count split decides and the
/// assignment is bit for bit what it was before any weight existed.
///
/// Only a source that reports differing weights takes the split below, where partitions are filled
/// in ordinal order, each taking the run of series whose total cost is nearest to its share of what
/// is left. The distance is compared against the exact share rather than against a rounded one, so a
/// share that is not a whole number does not make a run stop short of the nearer total, and the same
/// relative weights split the same way whatever unit they are reported in.
///
/// The share is recomputed from the remaining cost, so a series that dwarfs the rest takes
/// a partition of its own and the series after it are divided over the partitions still to fill,
/// instead of following the long one onto the same partition.
///
/// Every ordinal belongs to exactly one range and the ranges are contiguous, so no series is cut and
/// none is read twice. With fewer series than partitions some partitions read nothing: a series
/// cannot be divided, so the alternative would be reading one twice.
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
    weighted_boundaries(costs, partitions)
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

/// Returns the boundaries of a weighted split, where partitions take the series nearest their share.
fn weighted_boundaries(costs: &[usize], partitions: usize) -> Vec<usize> {
    let series = costs.len();
    let mut bounds = Vec::with_capacity(partitions + 1);
    bounds.push(0);
    let mut idx = 0usize;
    let mut remaining: u128 = costs.iter().map(|cost| *cost as u128).sum();
    for filled in 1..partitions {
        let partitions_left = partitions - filled + 1;
        // One series stays for every partition after this one, so none of them is skipped.
        let max_take = series - idx - (partitions_left - 1);
        let left = partitions_left as u128;
        let mut take = 1;
        let mut cost = costs[idx] as u128;
        while take < max_take {
            let next = costs[idx + take] as u128;
            // Another series is taken only while it brings this partition closer to its share, so
            // the run ends at the nearest total instead of overshooting by a whole series. Both
            // distances are scaled by `partitions_left`, which leaves them in proportion to the
            // distances from the exact share `remaining / partitions_left` while keeping them
            // integers: rounding the share down first would flatter the run that stops short of it
            // and stop this partition early, pushing the weight it skipped onto its neighbours.
            if (cost + next).saturating_mul(left).abs_diff(remaining)
                >= cost.saturating_mul(left).abs_diff(remaining)
            {
                break;
            }
            cost += next;
            take += 1;
        }
        idx += take;
        remaining = remaining.saturating_sub(cost);
        bounds.push(idx);
    }
    bounds.push(series);
    bounds
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

    /// Returns every partition's range, as the scan reads them out of one shared split.
    fn ranges(costs: &[usize], partitions: usize) -> Vec<Range<usize>> {
        let boundaries = partition_boundaries(costs, partitions);
        (0..partitions)
            .map(|partition| partition_range(&boundaries, partition))
            .collect()
    }

    /// Returns each partition's range and the cost it carries.
    fn split(costs: &[usize], partitions: usize) -> Vec<(Range<usize>, usize)> {
        ranges(costs, partitions)
            .into_iter()
            .map(|range| {
                let cost = costs[range.clone()].iter().sum();
                (range, cost)
            })
            .collect()
    }

    #[test]
    fn equal_costs_split_by_count() {
        let costs = vec![10; 8];
        assert_eq!(
            split(&costs, 4),
            vec![(0..2, 20), (2..4, 20), (4..6, 20), (6..8, 20),]
        );
    }

    /// The case the split exists for: a few long series among many short ones.
    ///
    /// Splitting these by count gives the first partition every long series, so it reads most of the
    /// samples while the others finish early. By cost each partition carries about a quarter.
    #[test]
    fn long_series_are_spread_over_partitions() {
        let mut costs = vec![1000; 4];
        costs.extend(std::iter::repeat_n(10, 40));
        let per_partition = split(&costs, 4);
        let total: usize = per_partition.iter().map(|(_, cost)| cost).sum();
        assert_eq!(total, 4400, "the split lost or duplicated cost");
        for (range, cost) in &per_partition {
            assert!(
                *cost * 2 <= total,
                "partition {range:?} carries {cost} of {total}: {per_partition:?}"
            );
        }
    }

    /// A long tail of short series behind a few long ones, which is what a real source looks like.
    ///
    /// The costs are in ordinal order and the long series come first, the arrangement a count split
    /// punishes hardest: it puts every long series on the first partition and leaves that partition
    /// with well over half again the average while the others read short series only. Splitting by
    /// cost keeps every partition within a few percent of the average.
    #[test]
    fn few_long_series_among_many_short_ones_are_balanced() {
        const PARTITIONS: usize = 4;
        const LONG_SERIES: usize = 8;
        const SHORT_SERIES: usize = 2048;
        const LONG_ROWS: usize = 1000;
        const SHORT_ROWS: usize = 10;

        let mut costs = vec![LONG_ROWS; LONG_SERIES];
        costs.extend(std::iter::repeat_n(SHORT_ROWS, SHORT_SERIES));
        let total = LONG_SERIES * LONG_ROWS + SHORT_SERIES * SHORT_ROWS;
        let average = total as f64 / PARTITIONS as f64;

        let per_partition = split(&costs, PARTITIONS);
        assert_eq!(
            total,
            per_partition.iter().map(|(_, cost)| cost).sum::<usize>(),
            "the split lost or duplicated cost"
        );
        // (a) The heaviest partition stays close to the average instead of taking every long series.
        let heaviest = per_partition.iter().map(|(_, cost)| *cost).max().unwrap();
        assert!(
            heaviest as f64 / average <= 1.5,
            "the heaviest partition carries {heaviest} against an average of {average}: {per_partition:?}"
        );

        // (b) The count split on the same input is unbalanced, which is what the weighting fixes.
        let by_count: Vec<(Range<usize>, usize)> = (0..PARTITIONS)
            .map(|partition| {
                let range = count_range(costs.len(), partition, PARTITIONS);
                let cost = costs[range.clone()].iter().sum();
                (range, cost)
            })
            .collect();
        let count_heaviest = by_count.iter().map(|(_, cost)| *cost).max().unwrap();
        assert!(
            count_heaviest as f64 / average > 1.5,
            "the count split was expected to be unbalanced but carries {count_heaviest} against an \
             average of {average}: {by_count:?}"
        );
        assert!(
            heaviest < count_heaviest,
            "the weighted split is no better than the count split: {per_partition:?} vs {by_count:?}"
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
    /// partition empty, because a series is never cut across partitions and the work may genuinely
    /// not need it.
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
                let mut next = 0;
                for range in ranges(&costs, partitions) {
                    assert_eq!(
                        range.start, next,
                        "gap or overlap in {costs:?}/{partitions}"
                    );
                    assert!(range.end >= range.start);
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

    /// A share that is not a whole number must not stop a run short of the nearer total.
    ///
    /// These costs total 13 over 4 partitions, so the exact share is 3.25. Taking the first two
    /// series costs 4, a distance of 0.75; taking only the first costs 2, a distance of 1.25. The
    /// second is nearer, so the run has to include it. Rounded down to 3 first, both distances
    /// become 1 and the run stops early, which leaves 7 to the last partition where 4 is the most
    /// any partition needs.
    #[test]
    fn a_fractional_share_does_not_stop_the_run_early() {
        let costs = [2, 2, 2, 4, 1, 2];
        let per_partition = split(&costs, 4);
        assert_eq!(
            per_partition,
            vec![(0..2, 4), (2..3, 2), (3..4, 4), (4..6, 3)],
            "the split does not follow the exact share"
        );
        assert_eq!(
            4,
            per_partition.iter().map(|(_, cost)| *cost).max().unwrap(),
            "the heaviest partition carries more than the exact share of 3.25 allows"
        );
    }

    /// The same relative weights must split the same way whatever unit they are reported in.
    ///
    /// The share of a partition is a ratio, so scaling every cost by the same factor cannot change
    /// which series belong together. It does when the share is rounded before the comparison:
    /// truncation error shrinks relative to the costs as they scale up, and the boundaries move with
    /// it even though nothing about the source did.
    #[test]
    fn the_split_does_not_depend_on_the_unit_of_the_costs() {
        let costs = [2, 2, 2, 4, 1, 2];
        let expected = ranges(&costs, 4);
        for scale in [10, 1000, 100_000] {
            let scaled = costs.map(|cost| cost * scale);
            assert_eq!(
                expected,
                ranges(&scaled, 4),
                "scaling the costs by {scale} moved the split"
            );
        }
    }

    /// The split is decided once, and reading a partition out of it is constant time.
    ///
    /// This is what keeps a scan's split cost independent of its partition count: the boundaries are
    /// built in one pass over the weights, and no partition scans the source's costs again.
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
