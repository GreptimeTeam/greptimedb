// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;

use log_store::raft_engine::log_store::RaftEngineLogStore;
use log_store::LogConfig;
use object_store::services::Fs;
use object_store::ObjectStore;
use store_api::manifest::Manifest;

use crate::compaction::noop::NoopCompactionScheduler;
use crate::engine;
use crate::file_purger::noop::NoopFilePurgeHandler;
use crate::flush::{FlushScheduler, SizeBasedStrategy};
use crate::manifest::region::RegionManifest;
use crate::memtable::DefaultMemtableBuilder;
use crate::region::StoreConfig;
use crate::scheduler::{LocalScheduler, SchedulerConfig};
use crate::sst::FsAccessLayer;

fn log_store_dir(store_dir: &str) -> String {
    format!("{store_dir}/logstore")
}

/// Create a new StoreConfig for test.
pub async fn new_store_config(
    region_name: &str,
    store_dir: &str,
) -> StoreConfig<RaftEngineLogStore> {
    let mut builder = Fs::default();
    builder.root(store_dir);
    let object_store = ObjectStore::new(builder).unwrap().finish();

    new_store_config_with_object_store(region_name, store_dir, object_store).await
}

/// Create a new StoreConfig with given object store.
pub async fn new_store_config_with_object_store(
    region_name: &str,
    store_dir: &str,
    object_store: ObjectStore,
) -> StoreConfig<RaftEngineLogStore> {
    let parent_dir = "";
    let sst_dir = engine::region_sst_dir(parent_dir, region_name);
    let manifest_dir = engine::region_manifest_dir(parent_dir, region_name);

    let sst_layer = Arc::new(FsAccessLayer::new(&sst_dir, object_store.clone()));
    let manifest = RegionManifest::with_checkpointer(&manifest_dir, object_store, None, None);
    manifest.start().await.unwrap();
    let log_config = LogConfig {
        log_file_dir: log_store_dir(store_dir),
        ..Default::default()
    };
    let log_store = Arc::new(RaftEngineLogStore::try_new(log_config).await.unwrap());
    let compaction_scheduler = Arc::new(NoopCompactionScheduler::default());
    let flush_scheduler = Arc::new(FlushScheduler::new(
        SchedulerConfig::default(),
        compaction_scheduler.clone(),
    ));
    let file_purger = Arc::new(LocalScheduler::new(
        SchedulerConfig::default(),
        NoopFilePurgeHandler,
    ));
    StoreConfig {
        log_store,
        sst_layer,
        manifest,
        memtable_builder: Arc::new(DefaultMemtableBuilder::default()),
        flush_scheduler,
        flush_strategy: Arc::new(SizeBasedStrategy::default()),
        compaction_scheduler,
        engine_config: Default::default(),
        file_purger,
        ttl: None,
        compaction_time_window: None,
    }
}
