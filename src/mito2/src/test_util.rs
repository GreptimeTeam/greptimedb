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

//! Utilities for testing.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use common_datasource::compression::CompressionType;
use common_test_util::temp_dir::{create_temp_dir, TempDir};
use log_store::raft_engine::log_store::RaftEngineLogStore;
use log_store::test_util::log_store_util;
use object_store::services::Fs;
use object_store::util::join_dir;
use object_store::ObjectStore;

use crate::config::MitoConfig;
use crate::engine::MitoEngine;
use crate::error::Result;
use crate::manifest::manager::RegionManifestManager;
use crate::manifest::options::RegionManifestOptions;
use crate::memtable::{Memtable, MemtableBuilder, MemtableId, MemtableRef};
use crate::metadata::{RegionMetadata, RegionMetadataRef};
use crate::worker::WorkerGroup;

/// Env to test mito engine.
pub struct TestEnv {
    /// Path to store data.
    data_home: TempDir,
}

impl TestEnv {
    /// Returns a new env with specific `prefix` for test.
    pub fn new(prefix: &str) -> TestEnv {
        TestEnv {
            data_home: create_temp_dir(prefix),
        }
    }

    /// Creates a new engine with specific config under this env.
    pub async fn create_engine(&self, config: MitoConfig) -> MitoEngine {
        let (log_store, object_store) = self.create_log_and_object_store().await;

        MitoEngine::new(config, Arc::new(log_store), object_store)
    }

    /// Creates a new [WorkerGroup] with specific config under this env.
    pub(crate) async fn create_worker_group(&self, config: MitoConfig) -> WorkerGroup {
        let (log_store, object_store) = self.create_log_and_object_store().await;

        WorkerGroup::start(config, Arc::new(log_store), object_store)
    }

    async fn create_log_and_object_store(&self) -> (RaftEngineLogStore, ObjectStore) {
        let data_home = self.data_home.path().to_str().unwrap();
        let wal_path = join_dir(data_home, "wal");
        let data_path = join_dir(data_home, "data");

        let log_store = log_store_util::create_tmp_local_file_log_store(&wal_path).await;
        let mut builder = Fs::default();
        builder.root(&data_path);
        let object_store = ObjectStore::new(builder).unwrap().finish();

        (log_store, object_store)
    }

    pub async fn create_manifest_manager(
        &self,
        compress_type: CompressionType,
        checkpoint_interval: Option<u64>,
        initial_metadata: Option<RegionMetadata>,
    ) -> Result<RegionManifestManager> {
        let data_home = self.data_home.path().to_str().unwrap();
        let manifest_dir = join_dir(data_home, "manifest");

        let mut builder = Fs::default();
        let _ = builder.root(&manifest_dir);
        let object_store = ObjectStore::new(builder).unwrap().finish();

        let manifest_opts = RegionManifestOptions {
            manifest_dir,
            object_store,
            compress_type,
            checkpoint_interval,
            initial_metadata,
        };

        RegionManifestManager::new(manifest_opts).await
    }
}

/// Memtable that only for testing metadata.
#[derive(Debug, Default)]
pub struct MetaOnlyMemtable {
    /// Id of this memtable.
    id: MemtableId,
}

impl MetaOnlyMemtable {
    /// Returns a new memtable with specific `id`.
    pub fn new(id: MemtableId) -> MetaOnlyMemtable {
        MetaOnlyMemtable { id }
    }
}

impl Memtable for MetaOnlyMemtable {
    fn id(&self) -> MemtableId {
        self.id
    }
}

#[derive(Debug, Default)]
pub struct MetaOnlyBuilder {
    /// Next memtable id.
    next_id: AtomicU32,
}

impl MemtableBuilder for MetaOnlyBuilder {
    fn build(&self, _metadata: &RegionMetadataRef) -> MemtableRef {
        Arc::new(MetaOnlyMemtable::new(
            self.next_id.fetch_add(1, Ordering::Relaxed),
        ))
    }
}
