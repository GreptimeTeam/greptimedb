use tempdir::TempDir;

use crate::datanode::DatanodeOptions;

pub fn create_tmp_dir_and_datanode_opts() -> (DatanodeOptions, TempDir) {
    let tmp_dir = TempDir::new("/tmp/greptimedb_test").unwrap();
    let opts = DatanodeOptions {
        wal_dir: tmp_dir.path().to_str().unwrap().to_string(),
        ..Default::default()
    };

    (opts, tmp_dir)
}
