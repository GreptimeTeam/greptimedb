use std::sync::Arc;

use tempdir::TempDir;

use crate::fs::{config::LogConfig, log::LocalFileLogStore};

/// Create a tmp directory for write log, used for test.
pub async fn create_log_file_store(dir: &str) -> Arc<LocalFileLogStore> {
    let dir = TempDir::new(dir.into()).unwrap();
    let cfg = LogConfig {
        append_buffer_size: 128,
        max_log_file_size: 128,
        log_file_dir: dir.path().to_str().unwrap().to_string(),
    };

    Arc::new(LocalFileLogStore::open(&cfg).await.unwrap())
}
