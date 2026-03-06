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

use criterion::{Criterion, criterion_group, criterion_main};
use mito2::compaction::run::{
    Item, Ranged, SortedRun, find_overlapping_items, find_sorted_runs, merge_seq_files, reduce_runs,
};

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct MockFile {
    start: i64,
    end: i64,
    size: usize,
}

impl Ranged for MockFile {
    type BoundType = i64;

    fn range(&self) -> (Self::BoundType, Self::BoundType) {
        (self.start, self.end)
    }
}

impl Item for MockFile {
    fn size(&self) -> usize {
        self.size
    }
}

fn generate_test_files(n: usize) -> Vec<MockFile> {
    let mut files = Vec::with_capacity(n);
    for _ in 0..n {
        // Create slightly overlapping ranges to force multiple sorted runs
        files.push(MockFile {
            start: 0,
            end: 10,
            size: 10,
        });
    }
    files
}

fn bench_find_sorted_runs(c: &mut Criterion) {
    let mut group = c.benchmark_group("find_sorted_runs");

    for size in [10, 100, 1000].iter() {
        group.bench_function(format!("size_{}", size), |b| {
            let mut files = generate_test_files(*size);
            b.iter(|| {
                find_sorted_runs(black_box(&mut files));
            });
        });
    }
    group.finish();
}

fn bench_reduce_runs(c: &mut Criterion) {
    let mut group = c.benchmark_group("reduce_runs");

    for size in [10, 100, 1000].iter() {
        group.bench_function(format!("size_{}", size), |b| {
            let mut files = generate_test_files(*size);
            let runs = find_sorted_runs(&mut files);
            b.iter(|| {
                reduce_runs(black_box(runs.clone()));
            });
        });
    }
    group.finish();
}

fn bench_find_overlapping_items(c: &mut Criterion) {
    let mut group = c.benchmark_group("find_overlapping_items");

    for size in [10, 100, 1000].iter() {
        group.bench_function(format!("size_{}", size), |b| {
            // Create two sets of files with some overlapping ranges
            let mut files1 = Vec::with_capacity(*size);
            let mut files2 = Vec::with_capacity(*size);

            for i in 0..*size {
                files1.push(MockFile {
                    start: i as i64,
                    end: (i + 5) as i64,
                    size: 10,
                });

                files2.push(MockFile {
                    start: (i + 3) as i64,
                    end: (i + 8) as i64,
                    size: 10,
                });
            }

            let mut r1 = SortedRun::from(files1);
            let mut r2 = SortedRun::from(files2);
            b.iter(|| {
                let mut result = vec![];
                find_overlapping_items(black_box(&mut r1), black_box(&mut r2), &mut result);
            });
        });
    }
    group.finish();
}

fn bench_merge_seq_files(c: &mut Criterion) {
    let mut group = c.benchmark_group("merge_seq_files");

    for size in [10, 100, 1000].iter() {
        group.bench_function(format!("size_{}", size), |b| {
            // Create a set of files with varying sizes
            let mut files = Vec::with_capacity(*size);

            for i in 0..*size {
                // Create files with different sizes to test the scoring algorithm
                let file_size = if i % 3 == 0 {
                    5
                } else if i % 3 == 1 {
                    10
                } else {
                    15
                };

                files.push(MockFile {
                    start: i as i64,
                    end: (i + 1) as i64,
                    size: file_size,
                });
            }

            b.iter(|| {
                merge_seq_files(black_box(&files), black_box(Some(50)));
            });
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_find_sorted_runs,
    bench_reduce_runs,
    bench_find_overlapping_items,
    bench_merge_seq_files
);
criterion_main!(benches);
