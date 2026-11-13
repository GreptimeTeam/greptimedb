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

//! Finds sources that may share a row in (time, primary key) space.

use std::cmp::{max, min};

/// The inclusive time and primary key bounds of one source.
///
/// A missing key range is unknown and conservatively spans every key.
#[derive(Debug, Clone)]
pub struct Extent<T, K> {
    /// Inclusive `(start, end)`; `start <= end`.
    pub time: (T, T),
    /// Inclusive `(min, max)`; `min <= max`.
    pub key: Option<(K, K)>,
}

/// Returns whether each extent intersects at least one other extent. Bounds
/// are inclusive in both dimensions, so touching endpoints count as overlap.
///
/// Sorts the extents by start time once and sweeps them twice over one
/// array-backed segment tree on the compressed key coordinates. The forward
/// sweep finds each extent's conflict with an earlier-starting extent: an
/// extent that starts no later overlaps if, somewhere in its key range, an
/// earlier extent ends at or after its start. The reverse sweep finds the
/// conflict with a later-starting extent in the same way. Expired extents need
/// no removal, since their bounds fail the comparison. Each sweep queries
/// before it updates, so the cost is O(N log N), even when many extents
/// overlap in time but have disjoint keys.
pub fn find_overlaps<T: Ord, K: Ord>(extents: &[Extent<T, K>]) -> Vec<bool> {
    // Compress key endpoints into 1..=len. 0 and len + 1 stand for the open
    // ends of an unknown key range.
    let mut keys: Vec<&K> = Vec::with_capacity(2 * extents.len());
    keys.extend(
        extents
            .iter()
            .filter_map(|extent| extent.key.as_ref())
            .flat_map(|(min, max)| [min, max]),
    );
    keys.sort_unstable();
    keys.dedup();
    let position = |key: &K| keys.partition_point(|k| *k < key) + 1;
    let unbounded = keys.len() + 1;
    let coordinates: Vec<_> = extents
        .iter()
        .map(|extent| {
            debug_assert!(extent.time.0 <= extent.time.1);
            match &extent.key {
                Some((min, max)) => {
                    debug_assert!(min <= max);
                    (position(min), position(max))
                }
                None => (0, unbounded),
            }
        })
        .collect();

    // Equal starts in index order; an unstable sort needs no buffer.
    let mut order: Vec<usize> = (0..extents.len()).collect();
    order.sort_unstable_by(|&lhs, &rhs| {
        extents[lhs]
            .time
            .0
            .cmp(&extents[rhs].time.0)
            .then(lhs.cmp(&rhs))
    });

    let mut overlaps = vec![false; extents.len()];
    let mut tree = BoundTree::new(unbounded + 1);
    // Latest end time over every key of the extents started so far.
    for &index in &order {
        let (low, high) = coordinates[index];
        let (start, end) = (&extents[index].time.0, &extents[index].time.1);
        overlaps[index] |= tree
            .query::<Latest>(low, high)
            .is_some_and(|last| last >= start);
        tree.update::<Latest>(low, high, end);
    }
    // Earliest start time over every key of the extents starting later.
    tree.clear();
    for &index in order.iter().rev() {
        let (low, high) = coordinates[index];
        let (start, end) = (&extents[index].time.0, &extents[index].time.1);
        overlaps[index] |= tree
            .query::<Earliest>(low, high)
            .is_some_and(|first| first <= end);
        tree.update::<Earliest>(low, high, start);
    }
    overlaps
}

/// The quadratic reference implementation of [`find_overlaps`].
#[cfg(any(test, feature = "test", feature = "testing"))]
pub fn find_overlaps_pairwise<T: Ord, K: Ord>(extents: &[Extent<T, K>]) -> Vec<bool> {
    let mut overlaps = vec![false; extents.len()];
    for (index, lhs) in extents.iter().enumerate() {
        for (offset, rhs) in extents[index + 1..].iter().enumerate() {
            let keys_overlap = match (&lhs.key, &rhs.key) {
                (Some(lhs), Some(rhs)) => (&lhs.0).max(&rhs.0) <= (&lhs.1).min(&rhs.1),
                _ => true,
            };
            if keys_overlap && (&lhs.time.0).max(&rhs.time.0) <= (&lhs.time.1).min(&rhs.time.1) {
                overlaps[index] = true;
                overlaps[index + 1 + offset] = true;
            }
        }
    }
    overlaps
}

/// The best bound per key coordinate, for range updates and range queries
/// that take the larger (or smaller) of two bounds.
///
/// A bound applied to a whole node is kept on that node and never pushed down,
/// since an update and a query only ever combine bounds with the same order.
struct BoundTree<'a, T> {
    /// The best bound applied to every key of each node.
    tags: Vec<Option<&'a T>>,
    /// The best bound applied to any key of each node, including its tag.
    best: Vec<Option<&'a T>>,
    /// Number of leaves, a power of two.
    width: usize,
}

/// The order in which a [`BoundTree`] keeps the better of two bounds. A type
/// parameter rather than a function pointer, so each sweep is monomorphized.
trait Pick {
    fn pick<'a, T: Ord>(lhs: &'a T, rhs: &'a T) -> &'a T;

    fn combine<'a, T: Ord>(lhs: Option<&'a T>, rhs: Option<&'a T>) -> Option<&'a T> {
        match (lhs, rhs) {
            (Some(lhs), Some(rhs)) => Some(Self::pick(lhs, rhs)),
            (lhs, rhs) => lhs.or(rhs),
        }
    }
}

/// Keeps the larger bound.
struct Latest;

impl Pick for Latest {
    fn pick<'a, T: Ord>(lhs: &'a T, rhs: &'a T) -> &'a T {
        max(lhs, rhs)
    }
}

/// Keeps the smaller bound.
struct Earliest;

impl Pick for Earliest {
    fn pick<'a, T: Ord>(lhs: &'a T, rhs: &'a T) -> &'a T {
        min(lhs, rhs)
    }
}

impl<'a, T: Ord> BoundTree<'a, T> {
    fn new(len: usize) -> Self {
        let width = len.next_power_of_two();
        Self {
            tags: vec![None; 2 * width],
            best: vec![None; 2 * width],
            width,
        }
    }

    fn clear(&mut self) {
        self.tags.fill(None);
        self.best.fill(None);
    }

    /// Applies `bound` to every key in `[low, high]`.
    fn update<P: Pick>(&mut self, low: usize, high: usize, bound: &'a T) {
        self.update_in::<P>(1, 0, self.width - 1, low, high, bound);
    }

    fn update_in<P: Pick>(
        &mut self,
        node: usize,
        node_low: usize,
        node_high: usize,
        low: usize,
        high: usize,
        bound: &'a T,
    ) {
        if high < node_low || node_high < low {
            return;
        }
        self.best[node] = P::combine(self.best[node], Some(bound));
        if low <= node_low && node_high <= high {
            self.tags[node] = P::combine(self.tags[node], Some(bound));
            return;
        }
        let mid = node_low + (node_high - node_low) / 2;
        self.update_in::<P>(2 * node, node_low, mid, low, high, bound);
        self.update_in::<P>(2 * node + 1, mid + 1, node_high, low, high, bound);
    }

    /// Returns the best bound applied to any key in `[low, high]`.
    fn query<P: Pick>(&self, low: usize, high: usize) -> Option<&'a T> {
        self.query_in::<P>(1, 0, self.width - 1, low, high)
    }

    fn query_in<P: Pick>(
        &self,
        node: usize,
        node_low: usize,
        node_high: usize,
        low: usize,
        high: usize,
    ) -> Option<&'a T> {
        if high < node_low || node_high < low {
            return None;
        }
        if low <= node_low && node_high <= high {
            return self.best[node];
        }
        let mid = node_low + (node_high - node_low) / 2;
        let children = P::combine(
            self.query_in::<P>(2 * node, node_low, mid, low, high),
            self.query_in::<P>(2 * node + 1, mid + 1, node_high, low, high),
        );
        // The tag covers the part of the node inside the query range.
        P::combine(self.tags[node], children)
    }
}

#[cfg(test)]
mod tests {
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};

    use super::*;

    fn extent(time: (i64, i64), key: Option<(u32, u32)>) -> Extent<i64, u32> {
        Extent { time, key }
    }

    #[test]
    fn test_find_overlaps_boundaries() {
        for (extents, expected) in [
            (vec![], vec![]),
            (vec![extent((0, 10), Some((0, 0)))], vec![false]),
            // Disjoint keys keep time-overlapping extents independent.
            (
                vec![extent((0, 10), Some((0, 4))), extent((5, 15), Some((5, 9)))],
                vec![false, false],
            ),
            // Touching time and key endpoints are inclusive.
            (
                vec![
                    extent((0, 10), Some((0, 4))),
                    extent((10, 20), Some((4, 9))),
                ],
                vec![true, true],
            ),
            (
                vec![
                    extent((0, 10), Some((0, 4))),
                    extent((11, 20), Some((0, 4))),
                ],
                vec![false, false],
            ),
            // An unknown key range overlaps every key.
            (
                vec![extent((0, 10), None), extent((5, 6), Some((7, 7)))],
                vec![true, true],
            ),
            (
                vec![extent((0, 10), None), extent((11, 12), None)],
                vec![false, false],
            ),
            // A contained extent marks the earlier one as well.
            (
                vec![
                    extent((0, 100), Some((0, 9))),
                    extent((40, 50), Some((3, 3))),
                ],
                vec![true, true],
            ),
            // Overlap is not transitive.
            (
                vec![
                    extent((0, 10), Some((0, 1))),
                    extent((5, 15), Some((1, 2))),
                    extent((12, 20), Some((0, 0))),
                ],
                vec![true, true, false],
            ),
            // Identical extents.
            (
                vec![extent((3, 3), Some((2, 2))), extent((3, 3), Some((2, 2)))],
                vec![true, true],
            ),
        ] {
            assert_eq!(find_overlaps(&extents), expected, "{extents:?}");
            assert_eq!(find_overlaps_pairwise(&extents), expected, "{extents:?}");
        }
    }

    #[test]
    fn test_find_overlaps_matches_pairwise() {
        let mut rng = StdRng::seed_from_u64(0x0c0_ffee);
        for _ in 0..2000 {
            let len = rng.random_range(0..40);
            // Small domains force ties, containment and touching endpoints.
            let time_domain = rng.random_range(1..60);
            let key_domain = rng.random_range(1..30);
            let extents: Vec<_> = (0..len)
                .map(|_| {
                    let start = rng.random_range(0..time_domain);
                    let end = start + rng.random_range(0..time_domain / 4 + 1);
                    let key = (!rng.random_bool(0.1)).then(|| {
                        let min = rng.random_range(0..key_domain);
                        (min, min + rng.random_range(0..key_domain / 4 + 1))
                    });
                    extent((start, end), key)
                })
                .collect();
            assert_eq!(
                find_overlaps(&extents),
                find_overlaps_pairwise(&extents),
                "{extents:?}"
            );
        }
    }

    #[test]
    fn test_find_overlaps_time_overlapping_disjoint_keys() {
        // The pairwise worst case: every extent overlaps in time, none in key.
        let extents: Vec<_> = (0..10_000)
            .map(|i| extent((0, 100), Some((i, i))))
            .collect();
        assert!(find_overlaps(&extents).iter().all(|overlap| !overlap));
    }
}
