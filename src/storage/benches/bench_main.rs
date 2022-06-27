use criterion::criterion_main;

mod memtable;

criterion_main! {
    memtable::bench_memtable_read::benches,
    memtable::bench_memtable_write::benches,
    memtable::bench_memtable_read_write_ratio::benches,
}
