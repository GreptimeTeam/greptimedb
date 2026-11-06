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

use std::collections::BTreeSet;

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
/// Sweeps the time axis once. Active extents live in a segment tree over the
/// compressed key coordinates, built once per call. Every extent is inserted
/// and removed once, and every extent is reported as overlapping at most once,
/// so the total cost is O(N log N) even when many extents overlap in time but
/// have disjoint keys.
pub fn find_overlaps<T: Ord, K: Ord>(extents: &[Extent<T, K>]) -> Vec<bool> {
    // Compress key endpoints into 1..=len. 0 and len + 1 stand for the open
    // ends of an unknown key range.
    let mut keys: Vec<&K> = extents
        .iter()
        .filter_map(|extent| extent.key.as_ref())
        .flat_map(|(min, max)| [min, max])
        .collect();
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

    // At equal times, starts sort before ends, so touching extents are both
    // active when the later one starts.
    let mut events: Vec<_> = extents
        .iter()
        .enumerate()
        .flat_map(|(index, extent)| {
            [
                (&extent.time.0, false, index),
                (&extent.time.1, true, index),
            ]
        })
        .collect();
    events.sort_unstable_by(|lhs, rhs| lhs.0.cmp(rhs.0).then(lhs.1.cmp(&rhs.1)));

    let mut overlaps = vec![false; extents.len()];
    // All active extents, to detect an overlap for an extent being inserted.
    let mut active = KeyTree::new(unbounded + 1);
    // Active extents not yet marked, so earlier extents are marked once each.
    let mut unmarked = KeyTree::new(unbounded + 1);
    for (_, is_end, index) in events {
        let (low, high) = coordinates[index];
        if is_end {
            active.remove(low, high, index);
            if !overlaps[index] {
                unmarked.remove(low, high, index);
            }
            continue;
        }
        if active.find(low, high).is_some() {
            overlaps[index] = true;
            while let Some(other) = unmarked.find(low, high) {
                overlaps[other] = true;
                let (other_low, other_high) = coordinates[other];
                unmarked.remove(other_low, other_high, other);
            }
        }
        active.insert(low, high, index);
        if !overlaps[index] {
            unmarked.insert(low, high, index);
        }
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

/// Inclusive key intervals `[low, high]` keyed by `low`. Each node stores the
/// largest `high` in its subtree, so a search can skip subtrees whose
/// intervals all end before the queried range starts.
struct KeyTree {
    /// `(high, id)` of the intervals starting at each coordinate.
    leaves: Vec<BTreeSet<(usize, usize)>>,
    /// Array-backed tree rooted at 1. `None` marks an empty subtree.
    max_high: Vec<Option<usize>>,
    /// Index of the first leaf node, a power of two.
    base: usize,
}

impl KeyTree {
    fn new(len: usize) -> Self {
        let base = len.next_power_of_two();
        Self {
            leaves: vec![BTreeSet::new(); len],
            max_high: vec![None; 2 * base],
            base,
        }
    }

    fn insert(&mut self, low: usize, high: usize, id: usize) {
        self.leaves[low].insert((high, id));
        self.update(low);
    }

    fn remove(&mut self, low: usize, high: usize, id: usize) {
        let removed = self.leaves[low].remove(&(high, id));
        debug_assert!(removed);
        self.update(low);
    }

    fn update(&mut self, low: usize) {
        let mut node = self.base + low;
        self.max_high[node] = self.leaves[low].last().map(|(high, _)| *high);
        while node > 1 {
            node /= 2;
            self.max_high[node] = self.max_high[2 * node].max(self.max_high[2 * node + 1]);
        }
    }

    /// Returns any interval intersecting `[low, high]`, that is, one starting
    /// at or before `high` and ending at or after `low`.
    fn find(&self, low: usize, high: usize) -> Option<usize> {
        self.find_in(1, 0, self.base - 1, high, low)
    }

    fn find_in(
        &self,
        node: usize,
        node_start: usize,
        node_end: usize,
        max_low: usize,
        min_high: usize,
    ) -> Option<usize> {
        if node_start > max_low || self.max_high[node].is_none_or(|high| high < min_high) {
            return None;
        }
        if node >= self.base {
            return self.leaves[node - self.base].last().map(|(_, id)| *id);
        }
        let mid = node_start + (node_end - node_start) / 2;
        self.find_in(2 * node, node_start, mid, max_low, min_high)
            .or_else(|| self.find_in(2 * node + 1, mid + 1, node_end, max_low, min_high))
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
