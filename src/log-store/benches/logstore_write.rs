use criterion::{criterion_group, criterion_main, Criterion};
use log_store::fs::config::LogConfig;
use log_store::fs::log::LocalFileLogStore;
use log_store::fs::namespace::LocalNamespace;
use rand::distributions::Alphanumeric;
use rand::Rng;
use store_api::logstore::LogStore;

fn generate_data(size: usize) -> Vec<u8> {
    let s: String = rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(size)
        .map(char::from)
        .collect();
    s.into_bytes()
}

async fn append(logstore: &LocalFileLogStore, data: &[u8]) {
    let entry = logstore.entry(data, 1, LocalNamespace::new(42));
    logstore.append(entry).await.unwrap();
}

fn test_benchmark(c: &mut Criterion) {
    common_telemetry::init_default_ut_logging();
    let rt = tokio::runtime::Runtime::new().unwrap();
    let config = LogConfig {
        append_buffer_size: 1024,
        max_log_file_size: 1024 * 1024 * 128,
        log_file_dir: "/Users/lei/wal-bench".to_string(),
        ..Default::default()
    };
    let data = generate_data(1024);
    let logstore = rt.block_on(LocalFileLogStore::open(&config)).unwrap();
    c.bench_function("log-write", |b| {
        b.to_async(&rt).iter(|| append(&logstore, &data))
    });
}

criterion_group!(benches, test_benchmark);
criterion_main!(benches);
