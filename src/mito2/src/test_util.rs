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

use common_datasource::compression::CompressionType;
use common_test_util::temp_dir::{create_temp_dir, TempDir};
use datatypes::prelude::ConcreteDataType;
use datatypes::schema::ColumnSchema;
use log_store::raft_engine::log_store::RaftEngineLogStore;
use log_store::test_util::log_store_util;
use object_store::services::Fs;
use object_store::util::join_dir;
use object_store::ObjectStore;
use store_api::storage::RegionId;

use crate::config::MitoConfig;
use crate::engine::MitoEngine;
use crate::error::Result;
use crate::manifest::manager::RegionManifestManager;
use crate::manifest::options::RegionManifestOptions;
use crate::metadata::{ColumnMetadata, RegionMetadata, SemanticType};
use crate::worker::request::{CreateRequest, RegionOptions};
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

/// Builder to mock a [CreateRequest].
pub struct CreateRequestBuilder {
    region_id: RegionId,
    region_dir: String,
    tag_num: usize,
    field_num: usize,
    create_if_not_exists: bool,
}

impl Default for CreateRequestBuilder {
    fn default() -> Self {
        CreateRequestBuilder {
            region_id: RegionId::default(),
            region_dir: "test".to_string(),
            tag_num: 1,
            field_num: 1,
            create_if_not_exists: false,
        }
    }
}

impl CreateRequestBuilder {
    pub fn new(region_id: RegionId) -> CreateRequestBuilder {
        CreateRequestBuilder {
            region_id,
            ..Default::default()
        }
    }

    pub fn region_dir(mut self, value: &str) -> Self {
        self.region_dir = value.to_string();
        self
    }

    pub fn tag_num(mut self, value: usize) -> Self {
        self.tag_num = value;
        self
    }

    pub fn field_num(mut self, value: usize) -> Self {
        self.tag_num = value;
        self
    }

    pub fn create_if_not_exists(mut self, value: bool) -> Self {
        self.create_if_not_exists = value;
        self
    }

    pub fn build(&self) -> CreateRequest {
        let mut column_id = 0;
        let mut column_metadatas = Vec::with_capacity(self.tag_num + self.field_num + 1);
        let mut primary_key = Vec::with_capacity(self.tag_num);
        for i in 0..self.tag_num {
            column_metadatas.push(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    format!("tag_{i}"),
                    ConcreteDataType::string_datatype(),
                    true,
                ),
                semantic_type: SemanticType::Tag,
                column_id: column_id,
            });
            primary_key.push(column_id);
            column_id += 1;
        }
        for i in 0..self.field_num {
            column_metadatas.push(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    format!("field_{i}"),
                    ConcreteDataType::float64_datatype(),
                    true,
                ),
                semantic_type: SemanticType::Field,
                column_id: column_id,
            });
            column_id += 1;
        }
        column_metadatas.push(ColumnMetadata {
            column_schema: ColumnSchema::new(
                "ts",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            ),
            semantic_type: SemanticType::Timestamp,
            column_id: column_id,
        });

        CreateRequest {
            region_id: self.region_id,
            region_dir: self.region_dir.clone(),
            column_metadatas,
            primary_key,
            create_if_not_exists: self.create_if_not_exists,
            options: RegionOptions::default(),
        }
    }
}
