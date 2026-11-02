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

use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use mito2::read::scan_region::metadata_count_overlap::{
    Extent, find_overlaps, find_overlaps_pairwise,
};

/// Every extent overlaps every other in time, but keys are disjoint: the
/// worst case for pairwise checking.
fn disjoint_keys(len: usize) -> Vec<Extent<i64, u64>> {
    (0..len)
        .map(|i| Extent {
            time: (0, 100),
            key: Some((i as u64 * 2, i as u64 * 2 + 1)),
        })
        .collect()
}

/// Daily compaction outputs: each day splits the key space into a few files,
/// and adjacent days touch at their boundaries.
fn daily_outputs(len: usize) -> Vec<Extent<i64, u64>> {
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

fn bench_overlaps(c: &mut Criterion) {
    for (name, generate) in [
        ("disjoint_keys", disjoint_keys as fn(usize) -> _),
        ("daily_outputs", daily_outputs),
    ] {
        let mut group = c.benchmark_group(format!("metadata_count_overlap/{name}"));
        for len in [8, 32, 128, 512, 2048] {
            let extents = generate(len);
            assert_eq!(find_overlaps(&extents), find_overlaps_pairwise(&extents));
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
