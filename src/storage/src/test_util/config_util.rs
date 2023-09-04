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

use common_config::WalConfig;
use common_datasource::compression::CompressionType;
use log_store::raft_engine::log_store::RaftEngineLogStore;
use object_store::services::Fs;
use object_store::ObjectStore;
use store_api::manifest::Manifest;
use store_api::storage::{CompactionStrategy, TwcsOptions};

use crate::compaction::CompactionHandler;
use crate::config::{EngineConfig, DEFAULT_REGION_WRITE_BUFFER_SIZE};
use crate::engine::{self, RegionMap};
use crate::file_purger::noop::NoopFilePurgeHandler;
use crate::flush::{FlushScheduler, PickerConfig, SizeBasedStrategy};
use crate::manifest::region::RegionManifest;
use crate::memtable::DefaultMemtableBuilder;
use crate::region::StoreConfig;
use crate::scheduler::{LocalScheduler, SchedulerConfig};
use crate::sst::FsAccessLayer;

fn log_store_dir(store_dir: &str) -> String {
    format!("{store_dir}/wal")
}

/// Create a new StoreConfig for test.
pub async fn new_store_config(
    region_name: &str,
    store_dir: &str,
    engine_config: EngineConfig,
) -> StoreConfig<RaftEngineLogStore> {
    let mut builder = Fs::default();
    let _ = builder.root(store_dir);
    let object_store = ObjectStore::new(builder).unwrap().finish();

    new_store_config_with_object_store(region_name, store_dir, object_store, engine_config)
        .await
        .0
}

/// Create a new StoreConfig and region map for test.
pub async fn new_store_config_and_region_map(
    region_name: &str,
    store_dir: &str,
    engine_config: EngineConfig,
) -> (
    StoreConfig<RaftEngineLogStore>,
    Arc<RegionMap<RaftEngineLogStore>>,
) {
    let mut builder = Fs::default();
    let _ = builder.root(store_dir);
    let object_store = ObjectStore::new(builder).unwrap().finish();

    new_store_config_with_object_store(region_name, store_dir, object_store, engine_config).await
}

/// Create a new StoreConfig with given object store.
pub async fn new_store_config_with_object_store(
    region_name: &str,
    store_dir: &str,
    object_store: ObjectStore,
    engine_config: EngineConfig,
) -> (
    StoreConfig<RaftEngineLogStore>,
    Arc<RegionMap<RaftEngineLogStore>>,
) {
    let parent_dir = "";
    let sst_dir = engine::region_sst_dir(parent_dir, region_name);
    let manifest_dir = engine::region_manifest_dir(parent_dir, region_name);

    let sst_layer = Arc::new(FsAccessLayer::new(&sst_dir, object_store.clone()));
    let manifest = RegionManifest::with_checkpointer(
        &manifest_dir,
        object_store,
        CompressionType::Uncompressed,
        None,
        None,
    );
    manifest.start().await.unwrap();
    let log_store = Arc::new(
        RaftEngineLogStore::try_new(log_store_dir(store_dir), WalConfig::default())
            .await
            .unwrap(),
    );

    let compaction_scheduler = Arc::new(LocalScheduler::new(
        SchedulerConfig::default(),
        CompactionHandler::default(),
    ));
    // We use an empty region map so actually the background worker of the picker is disabled.
    let regions = Arc::new(RegionMap::new());
    let flush_scheduler = Arc::new(
        FlushScheduler::new(
            SchedulerConfig::default(),
            compaction_scheduler.clone(),
            regions.clone(),
            PickerConfig::default(),
        )
        .unwrap(),
    );
    let file_purger = Arc::new(LocalScheduler::new(
        SchedulerConfig::default(),
        NoopFilePurgeHandler,
    ));
    (
        StoreConfig {
            log_store,
            sst_layer,
            manifest,
            memtable_builder: Arc::new(DefaultMemtableBuilder::default()),
            flush_scheduler,
            flush_strategy: Arc::new(SizeBasedStrategy::default()),
            compaction_scheduler,
            engine_config: Arc::new(engine_config),
            file_purger,
            ttl: None,
            write_buffer_size: DEFAULT_REGION_WRITE_BUFFER_SIZE.as_bytes() as usize,
            compaction_strategy: CompactionStrategy::Twcs(TwcsOptions::default()),
        },
        regions,
    )
}
