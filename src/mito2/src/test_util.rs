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

use std::sync::Arc;

use common_test_util::temp_dir::{create_temp_dir, TempDir};
use log_store::raft_engine::log_store::RaftEngineLogStore;
use log_store::test_util::log_store_util;
use object_store::services::Fs;
use object_store::util::normalize_dir;
use object_store::ObjectStore;

use crate::config::MitoConfig;
use crate::engine::MitoEngine;
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
    pub(crate) async fn create_worker_group(&self, config: &MitoConfig) -> WorkerGroup {
        let (log_store, object_store) = self.create_log_and_object_store().await;

        WorkerGroup::start(config, Arc::new(log_store), object_store)
    }

    async fn create_log_and_object_store(&self) -> (RaftEngineLogStore, ObjectStore) {
        let data_home = self.data_home.path().to_str().unwrap();
        let data_home = normalize_dir(data_home);
        let wal_path = format!("{}wal", data_home);
        let data_path = format!("{}data", data_home);

        let log_store = log_store_util::create_tmp_local_file_log_store(&wal_path).await;
        let mut builder = Fs::default();
        builder.root(&data_path);
        let object_store = ObjectStore::new(builder).unwrap().finish();

        (log_store, object_store)
    }
}
