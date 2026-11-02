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

//! Compares the metadata aggregate overlap check with its earlier dual-tree
//! sweep and the pairwise oracle. Each case prints the allocations per call
//! before it is timed.

use std::alloc::{GlobalAlloc, Layout, System};
use std::collections::BTreeSet;
use std::hint::black_box;
use std::sync::atomic::{AtomicUsize, Ordering};

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use mito2::read::scan_region::metadata_aggr_overlap::{
    Extent, find_overlaps, find_overlaps_pairwise,
};

/// Counts heap allocations, including reallocations.
struct CountingAlloc;

static ALLOCATIONS: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static GLOBAL: CountingAlloc = CountingAlloc;

type Extents = Vec<Extent<i64, u64>>;

/// Every extent overlaps every other in time, but keys are disjoint: the
/// worst case for pairwise checking.
fn disjoint_keys(len: usize) -> Extents {
    (0..len)
        .map(|i| Extent {
            time: (0, 100),
            key: Some((i as u64 * 2, i as u64 * 2 + 1)),
        })
        .collect()
}

/// Every extent overlaps every other in both time and key.
fn dense_overlap(len: usize) -> Extents {
    (0..len)
        .map(|i| Extent {
            time: (i as i64, i as i64 + len as i64),
            key: Some((i as u64, i as u64 + len as u64)),
        })
        .collect()
}

/// Daily compaction outputs: each day splits the key space into a few files,
/// and adjacent days touch at their boundaries.
fn daily_outputs(len: usize) -> Extents {
    const FILES_PER_DAY: usize = 4;
    (0..len)
        .map(|i| {
            let day = (i / FILES_PER_DAY) as i64;
            let part = (i % FILES_PER_DAY) as u64;
            Extent {
                time: (day * 86_400, (day + 1) * 86_400),
                key: Some((part * 1000, part * 1000 + 999)),
            }
        })
        .collect()
}

/// Repeated identical bounds, with every eighth key range unknown, on
/// disjoint hourly windows.
fn duplicate_unknown(len: usize) -> Extents {
    (0..len)
        .map(|i| {
            let hour = (i / 4) as i64;
            Extent {
                time: (hour * 3_600, hour * 3_600 + 3_599),
                key: (i % 8 != 7).then_some((0, 999)),
            }
        })
        .collect()
}

fn allocations<R>(f: impl FnOnce() -> R) -> usize {
    let before = ALLOCATIONS.load(Ordering::Relaxed);
    black_box(f());
    ALLOCATIONS.load(Ordering::Relaxed) - before
}

#[allow(clippy::print_stdout)]
fn bench_overlaps(c: &mut Criterion) {
    for (name, generate) in [
        ("disjoint_keys", disjoint_keys as fn(usize) -> Extents),
        ("dense_overlap", dense_overlap),
        ("daily_outputs", daily_outputs),
        ("duplicate_unknown", duplicate_unknown),
    ] {
        let mut group = c.benchmark_group(format!("metadata_aggr_overlap/{name}"));
        for len in [2, 8, 32, 128, 512, 2048] {
            let extents = generate(len);
            let expected = find_overlaps_pairwise(&extents);
            assert_eq!(find_overlaps(&extents), expected);
            assert_eq!(dual_tree::find_overlaps(&extents), expected);
            println!(
                "allocations {name}/{len}: sweep={} dual_tree={} pairwise={}",
                allocations(|| find_overlaps(&extents)),
                allocations(|| dual_tree::find_overlaps(&extents)),
                allocations(|| find_overlaps_pairwise(&extents)),
            );
            group.bench_with_input(BenchmarkId::new("sweep", len), &extents, |b, extents| {
                b.iter(|| find_overlaps(black_box(extents)))
            });
            group.bench_with_input(
                BenchmarkId::new("dual_tree", len),
                &extents,
                |b, extents| b.iter(|| dual_tree::find_overlaps(black_box(extents))),
            );
            group.bench_with_input(BenchmarkId::new("pairwise", len), &extents, |b, extents| {
                b.iter(|| find_overlaps_pairwise(black_box(extents)))
            });
        }
        group.finish();
    }
}

/// The previous sweep: one event pass with `active` and `unmarked` interval
/// trees whose leaves are `BTreeSet`s. Kept only as a baseline.
mod dual_tree {
    use super::*;

    pub fn find_overlaps<T: Ord, K: Ord>(extents: &[Extent<T, K>]) -> Vec<bool> {
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
            .map(|extent| match &extent.key {
                Some((min, max)) => (position(min), position(max)),
                None => (0, unbounded),
            })
            .collect();
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
        let mut active = KeyTree::new(unbounded + 1);
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

    struct KeyTree {
        leaves: Vec<BTreeSet<(usize, usize)>>,
        max_high: Vec<Option<usize>>,
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
            self.leaves[low].remove(&(high, id));
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
}

criterion_group!(benches, bench_overlaps);
criterion_main!(benches);
