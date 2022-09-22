use std::sync::Arc;

use log_store::fs::{config::LogConfig, log::LocalFileLogStore};
use object_store::{backend::fs::*, ObjectStore};

use crate::background::JobPoolImpl;
use crate::engine;
use crate::flush::{FlushSchedulerImpl, SizeBasedStrategy};
use crate::manifest::region::RegionManifest;
use crate::memtable::DefaultMemtableBuilder;
use crate::region::StoreConfig;
use crate::sst::FsAccessLayer;

fn log_store_dir(store_dir: &str) -> String {
    format!("{}/logstore", store_dir)
}

/// Create a new StoreConfig for test.
pub async fn new_store_config(
    region_name: &str,
    store_dir: &str,
) -> StoreConfig<LocalFileLogStore> {
    let parent_dir = "";
    let sst_dir = engine::region_sst_dir(parent_dir, region_name);
    let manifest_dir = engine::region_manifest_dir(parent_dir, region_name);

    let mut builder = Builder::default();
    let accessor = builder.root(store_dir).build().unwrap();
    let object_store = ObjectStore::new(accessor);
    let sst_layer = Arc::new(FsAccessLayer::new(&sst_dir, object_store.clone()));
    let manifest = RegionManifest::new(&manifest_dir, object_store);
    let job_pool = Arc::new(JobPoolImpl {});
    let flush_scheduler = Arc::new(FlushSchedulerImpl::new(job_pool));
    let log_config = LogConfig {
        log_file_dir: log_store_dir(store_dir),
        ..Default::default()
    };
    let log_store = Arc::new(LocalFileLogStore::open(&log_config).await.unwrap());

    StoreConfig {
        log_store,
        sst_layer,
        manifest,
        memtable_builder: Arc::new(DefaultMemtableBuilder {}),
        flush_scheduler,
        flush_strategy: Arc::new(SizeBasedStrategy::default()),
    }
}
