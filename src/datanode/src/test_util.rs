use tempdir::TempDir;

use crate::datanode::DatanodeOptions;

/// Create a tmp dir(will be deleted once it goes out of scope.) and a default `DatanodeOptions`,
/// Only for test.
///
/// TODO: Add a test feature
pub struct TestGuard {
    _wal_tmp_dir: TempDir,
    _data_tmp_dir: TempDir,
}

pub fn create_tmp_dir_and_datanode_opts() -> (DatanodeOptions, TestGuard) {
    let wal_tmp_dir = TempDir::new("/tmp/greptimedb_test_wal").unwrap();
    let data_tmp_dir = TempDir::new("/tmp/greptimedb_test_data").unwrap();
    let opts = DatanodeOptions {
        wal_dir: tmp_dir.path().to_str().unwrap().to_string(),
        ..Default::default()
    };

    (
        opts,
        TestGuard {
            _wal_tmp_dir: wal_tmp_dir,
            _data_tmp_dir: data_tmp_dir,
        },
    )
}
