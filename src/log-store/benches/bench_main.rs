use criterion::criterion_main;

mod logstore_write;

criterion_main! {
    logstore_write::benches
}
