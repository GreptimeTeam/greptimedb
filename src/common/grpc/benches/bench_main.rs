use criterion::criterion_main;

mod channel_manager;

criterion_main! {
    channel_manager::benches
}
