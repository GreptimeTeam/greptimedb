use criterion::{criterion_group, criterion_main, Criterion, Throughput};

use crate::memtable::{generate_kvs, util::bench_context::BenchContext};

fn bench_memtable_read(c: &mut Criterion) {
    // the length of string in value is 20
    let kvs = generate_kvs(10, 10000, 20);
    let ctx = BenchContext::new();
    kvs.iter().for_each(|kv| ctx.write(kv));
    let mut group = c.benchmark_group("memtable_read");
    group.throughput(Throughput::Elements(10 * 10000));
    group.bench_function("read", |b| b.iter(|| ctx.read(100)));
    group.finish();
}

criterion_group!(benches, bench_memtable_read);
criterion_main!(benches);
