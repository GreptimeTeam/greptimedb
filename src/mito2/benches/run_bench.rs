use criterion::{black_box, criterion_group, criterion_main, Criterion};
use mito2::compaction::run::{find_overlapping_items, find_sorted_runs, reduce_runs, Item, Ranged};

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

    for size in [10, 100, 1000, 10000].iter() {
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

    for size in [10, 50, 100].iter() {
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

            b.iter(|| {
                find_overlapping_items(black_box(&mut files1), black_box(&mut files2));
            });
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_find_sorted_runs,
    bench_reduce_runs,
    bench_find_overlapping_items
);
criterion_main!(benches);
