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

//! Benchmarks for the page-range cache index lock (LC7).
//!
//! Hammers `put_page_ranges`/`get_page_ranges` from `THREADS` concurrent
//! threads and reports throughput (ops/sec) for two scenarios:
//! - `distinct_row_groups` (the contention scenario: different row groups'
//!   page fetches should not contend on the index lock)
//! - `same_row_group` (control: every thread hits the same row-group key, so
//!   they always contend)
//!
//! Run with:
//! ```sh
//! cargo bench -p mito2 --features test --bench bench_page_range_cache
//! ```

use std::hint::black_box;
use std::ops::Range;
use std::sync::{Arc, Barrier};
use std::thread;

use bytes::Bytes;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use mito2::cache::CacheManager;
use store_api::storage::FileId;

const THREADS: usize = 8;
const FRAGMENT_SIZE: usize = 64;
/// Number of put+get pairs each thread performs per iteration. Each pair counts
/// as 2 ops, so `Throughput::Elements` below reports ops/sec directly.
const OPS_PER_THREAD: u64 = 20_000;

/// Runs `threads` concurrent workers against the page-range cache.
///
/// With `distinct_groups`, every thread uses its own row-group index; otherwise
/// all threads share row-group index 0. Returns the total number of ops
/// (put + get pairs × 2) performed across all threads.
fn run_round(cache: &Arc<CacheManager>, threads: usize, distinct_groups: bool) -> u64 {
    let file_id = FileId::random();
    let barrier = Arc::new(Barrier::new(threads));
    let handles: Vec<_> = (0..threads)
        .map(|t| {
            let cache = Arc::clone(cache);
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                // Distinct row groups per thread in the contention scenario.
                let row_group_idx = if distinct_groups { t * 17 + 1 } else { 0 };
                let range: Range<u64> = 0..FRAGMENT_SIZE as u64;
                let page = Bytes::from(vec![7u8; FRAGMENT_SIZE]);
                barrier.wait();
                let mut ops = 0u64;
                for _ in 0..OPS_PER_THREAD {
                    cache.put_page_ranges(
                        file_id,
                        row_group_idx,
                        std::slice::from_ref(&range),
                        std::slice::from_ref(&page),
                    );
                    let lookup =
                        cache.get_page_ranges(file_id, row_group_idx, std::slice::from_ref(&range));
                    black_box(lookup);
                    ops += 2;
                }
                ops
            })
        })
        .collect();
    handles.into_iter().map(|h| h.join().unwrap()).sum()
}

fn page_range_cache_lock_bench(c: &mut Criterion) {
    let cache = Arc::new(CacheManager::builder().page_cache_size(1 << 30).build());

    let mut group = c.benchmark_group("page_range_cache_lock");
    group.sample_size(10);
    // Each iteration performs 2 ops (put + get) per thread.
    group.throughput(Throughput::Elements(2 * OPS_PER_THREAD * THREADS as u64));

    for (name, distinct_groups) in [("distinct_row_groups", true), ("same_row_group", false)] {
        group.bench_function(BenchmarkId::from_parameter(name), |b| {
            b.iter(|| black_box(run_round(&cache, THREADS, distinct_groups)));
        });
    }

    group.finish();
}

criterion_group!(benches, page_range_cache_lock_bench);
criterion_main!(benches);
