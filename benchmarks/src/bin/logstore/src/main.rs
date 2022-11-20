use std::sync::Arc;

use common_telemetry::timer;
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

#[tokio::main]
async fn main() {
    common_telemetry::init_default_ut_logging();
    common_telemetry::init_default_metrics_recorder();
    let config = LogConfig {
        append_buffer_size: 1024,
        max_log_file_size: 1024 * 1024 * 128,
        log_file_dir: "/Users/lei/wal-bench".to_string(),
    };
    let logstore = Arc::new(LocalFileLogStore::open(&config).await.unwrap());
    let data = Arc::new(generate_data(1024));

    let concurrency = 8;
    let iterations = 1000;

    let mut handles = Vec::with_capacity(concurrency);

    for _ in 0..concurrency {
        let logstore = logstore.clone();
        let data = data.clone();
        let handle = tokio::spawn(async move {
            for _ in 0..iterations {
                let entry = logstore.entry(&data as &[u8], 1, LocalNamespace::new(42));
                {
                    let _timer = timer!("append-timer");
                    logstore.append(entry).await.unwrap();
                }
            }
        });
        handles.push(handle);
    }

    for h in handles {
        h.await.unwrap();
    }

    let handler = common_telemetry::metric::try_handle();
    println!("handler.unwrap().render(): {}", handler.unwrap().render());
}
