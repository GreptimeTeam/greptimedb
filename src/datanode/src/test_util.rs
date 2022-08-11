use tempdir::TempDir;

use crate::datanode::{DatanodeOptions, FileStoreConfig, ObjectStoreConfig};

/// Create a tmp dir(will be deleted once it goes out of scope.) and a default `DatanodeOptions`,
/// Only for test.
///
/// TODO: Add a test feature
pub fn create_tmp_dir_and_datanode_opts() -> (DatanodeOptions, TempDir, TempDir) {
    let wal_tmp_dir = TempDir::new("/tmp/greptimedb_test_wal").unwrap();
    let data_tmp_dir = TempDir::new("/tmp/greptimedb_test_data").unwrap();
    let opts = DatanodeOptions {
        wal_dir: wal_tmp_dir.path().to_str().unwrap().to_string(),
        store_config: ObjectStoreConfig::File(FileStoreConfig {
            store_dir: data_tmp_dir.path().to_str().unwrap().to_string(),
        }),
        ..Default::default()
    };

    (opts, wal_tmp_dir, data_tmp_dir)
}
