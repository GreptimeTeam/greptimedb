use criterion::{criterion_group, criterion_main, Criterion};
use mito2::memtable::merge_tree::{MergeTreeConfig, MergeTreeMemtable};
use mito2::memtable::Memtable;
use mito2::test_util::memtable_util;

fn bench_merge_tree_memtable(c: &mut Criterion) {
    let metadata = memtable_util::metadata_with_primary_key(vec![1, 0], true);
    let timestamps = (0..100).collect::<Vec<_>>();

    let memtable = MergeTreeMemtable::new(1, metadata.clone(), None, &MergeTreeConfig::default());

    let _ = c.bench_function("MergeTreeMemtable", |b| {
        let kvs =
            memtable_util::build_key_values(&metadata, "hello".to_string(), 42, &timestamps, 1);
        b.iter(|| {
            memtable.write(&kvs).unwrap();
        });
    });
}

criterion_group!(benches, bench_merge_tree_memtable);
criterion_main!(benches);
