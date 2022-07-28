use std::sync::Arc;

use log_store::fs::noop::NoopLogStore;
use object_store::{backend::fs::Backend, ObjectStore};
use store_api::manifest::Manifest;

use crate::background::JobPoolImpl;
use crate::engine;
use crate::flush::{FlushSchedulerImpl, SizeBasedStrategy};
use crate::manifest::region::RegionManifest;
use crate::memtable::DefaultMemtableBuilder;
use crate::region::StoreConfig;
use crate::sst::FsAccessLayer;

/// Create a new StoreConfig for test.
pub async fn new_store_config(store_dir: &str, region_name: &str) -> StoreConfig<NoopLogStore> {
    let sst_dir = engine::region_sst_dir(region_name);
    let manifest_dir = engine::region_manifest_dir(region_name);

    let accessor = Backend::build().root(store_dir).finish().await.unwrap();
    let object_store = ObjectStore::new(accessor);
    let sst_layer = Arc::new(FsAccessLayer::new(&sst_dir, object_store.clone()));
    let manifest = RegionManifest::new(&manifest_dir, object_store);
    let job_pool = Arc::new(JobPoolImpl {});
    let flush_scheduler = Arc::new(FlushSchedulerImpl::new(job_pool));

    StoreConfig {
        log_store: Arc::new(NoopLogStore::default()),
        sst_layer,
        manifest,
        memtable_builder: Arc::new(DefaultMemtableBuilder {}),
        flush_scheduler,
        flush_strategy: Arc::new(SizeBasedStrategy::default()),
    }
}
