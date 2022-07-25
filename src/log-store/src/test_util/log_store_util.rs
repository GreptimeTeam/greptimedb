use tempdir::TempDir;

use crate::fs::{config::LogConfig, log::LocalFileLogStore};

/// Create a tmp directory for write log, used for test.
// TODO: Add a test feature
pub async fn create_tmp_local_file_log_store(dir: &str) -> (LocalFileLogStore, TempDir) {
    let dir = TempDir::new(dir).unwrap();
    let cfg = LogConfig {
        append_buffer_size: 128,
        max_log_file_size: 128,
        log_file_dir: dir.path().to_str().unwrap().to_string(),
    };

    (LocalFileLogStore::open(&cfg).await.unwrap(), dir)
}
