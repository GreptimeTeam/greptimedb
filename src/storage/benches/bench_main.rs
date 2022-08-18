use criterion::criterion_main;

mod memtable;
mod wal;

criterion_main! {
    memtable::bench_memtable_read::benches,
    memtable::bench_memtable_write::benches,
    memtable::bench_memtable_read_write_ratio::benches,
    wal::bench_wal::benches,
    wal::bench_decode::benches,
    wal::bench_encode::benches,
}
