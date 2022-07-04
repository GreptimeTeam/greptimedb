use std::sync::Arc;

use tempdir::TempDir;

use crate::fs::{config::LogConfig, log::LocalFileLogStore};

pub async fn create_tmp_log_store() -> Arc<LocalFileLogStore> {
    let dir = TempDir::new("greptimedb_test").unwrap();
    let config = LogConfig {
        append_buffer_size: 128,
        max_log_file_size: 128,
        log_file_dir: dir.path().to_str().unwrap().to_string(),
    };

    Arc::new(LocalFileLogStore::open(&config).await.unwrap())
}
