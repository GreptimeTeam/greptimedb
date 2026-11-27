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

//! Compares the metadata aggregate overlap check with the pairwise oracle.
//! Each case prints the allocations per call before it is timed.

use std::alloc::{GlobalAlloc, Layout, System};
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
            println!(
                "allocations {name}/{len}: sweep={} pairwise={}",
                allocations(|| find_overlaps(&extents)),
                allocations(|| find_overlaps_pairwise(&extents)),
            );
            group.bench_with_input(BenchmarkId::new("sweep", len), &extents, |b, extents| {
                b.iter(|| find_overlaps(black_box(extents)))
            });
            group.bench_with_input(BenchmarkId::new("pairwise", len), &extents, |b, extents| {
                b.iter(|| find_overlaps_pairwise(black_box(extents)))
            });
        }
        group.finish();
    }
}

criterion_group!(benches, bench_overlaps);
criterion_main!(benches);
