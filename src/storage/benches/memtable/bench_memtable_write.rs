use criterion::{criterion_group, criterion_main, Criterion, Throughput};

use crate::memtable::generate_kvs;
use crate::memtable::util::bench_context::BenchContext;

pub fn bench_memtable_write(c: &mut Criterion) {
    // the length of string in value is 20
    let kvs = generate_kvs(10, 1000, 20);
    let mut group = c.benchmark_group("memtable_write");
    group.throughput(Throughput::Elements(10 * 1000));
    group.bench_function("write", |b| {
        let ctx = BenchContext::new();
        b.iter(|| kvs.iter().for_each(|kv| ctx.write(kv)))
    });
    group.finish();
}

criterion_group!(benches, bench_memtable_write);
criterion_main!(benches);
