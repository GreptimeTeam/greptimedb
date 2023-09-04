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

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use api::greptime_proto::v1;
use api::v1::value::ValueData;
use api::v1::{OpType, SemanticType};
use common_datasource::compression::CompressionType;
use common_test_util::temp_dir::{create_temp_dir, TempDir};
use datatypes::arrow::array::{TimestampMillisecondArray, UInt64Array, UInt8Array};
use datatypes::prelude::ConcreteDataType;
use datatypes::schema::ColumnSchema;
use log_store::raft_engine::log_store::RaftEngineLogStore;
use log_store::test_util::log_store_util;
use object_store::services::Fs;
use object_store::ObjectStore;
use store_api::metadata::{ColumnMetadata, RegionMetadataRef};
use store_api::region_request::RegionCreateRequest;

use crate::config::MitoConfig;
use crate::engine::listener::EventListenerRef;
use crate::engine::MitoEngine;
use crate::error::Result;
use crate::flush::{WriteBufferManager, WriteBufferManagerRef};
use crate::manifest::manager::{RegionManifestManager, RegionManifestOptions};
use crate::read::{Batch, BatchBuilder, BatchReader};
use crate::worker::WorkerGroup;

/// Env to test mito engine.
pub struct TestEnv {
    /// Path to store data.
    data_home: TempDir,
    logstore: Option<Arc<RaftEngineLogStore>>,
    object_store: Option<ObjectStore>,
}

impl Default for TestEnv {
    fn default() -> Self {
        TestEnv::new()
    }
}

impl TestEnv {
    /// Returns a new env with empty prefix for test.
    pub fn new() -> TestEnv {
        TestEnv {
            data_home: create_temp_dir(""),
            logstore: None,
            object_store: None,
        }
    }

    /// Returns a new env with specific `prefix` for test.
    pub fn with_prefix(prefix: &str) -> TestEnv {
        TestEnv {
            data_home: create_temp_dir(prefix),
            logstore: None,
            object_store: None,
        }
    }

    pub fn get_logstore(&self) -> Option<Arc<RaftEngineLogStore>> {
        self.logstore.clone()
    }

    pub fn get_object_store(&self) -> Option<ObjectStore> {
        self.object_store.clone()
    }

    /// Creates a new engine with specific config under this env.
    pub async fn create_engine(&mut self, config: MitoConfig) -> MitoEngine {
        let (log_store, object_store) = self.create_log_and_object_store().await;

        let logstore = Arc::new(log_store);
        self.logstore = Some(logstore.clone());
        self.object_store = Some(object_store.clone());
        MitoEngine::new(config, logstore, object_store)
    }

    /// Creates a new engine with specific config and manager/listener under this env.
    pub async fn create_engine_with(
        &mut self,
        config: MitoConfig,
        manager: WriteBufferManagerRef,
        listener: Option<EventListenerRef>,
    ) -> MitoEngine {
        let (log_store, object_store) = self.create_log_and_object_store().await;

        let logstore = Arc::new(log_store);
        self.logstore = Some(logstore.clone());
        self.object_store = Some(object_store.clone());
        MitoEngine::new_for_test(config, logstore, object_store, manager, listener)
    }

    /// Reopen the engine.
    pub async fn reopen_engine(&mut self, engine: MitoEngine, config: MitoConfig) -> MitoEngine {
        engine.stop().await.unwrap();

        MitoEngine::new(
            config,
            self.logstore.clone().unwrap(),
            self.object_store.clone().unwrap(),
        )
    }

    /// Reopen the engine.
    pub async fn reopen_engine_with(
        &self,
        engine: MitoEngine,
        config: MitoConfig,
        manager: WriteBufferManagerRef,
        listener: Option<EventListenerRef>,
    ) -> MitoEngine {
        engine.stop().await.unwrap();

        MitoEngine::new_for_test(
            config,
            self.logstore.clone().unwrap(),
            self.object_store.clone().unwrap(),
            manager,
            listener,
        )
    }

    /// Creates a new [WorkerGroup] with specific config under this env.
    pub(crate) async fn create_worker_group(&self, config: MitoConfig) -> WorkerGroup {
        let (log_store, object_store) = self.create_log_and_object_store().await;

        WorkerGroup::start(config, Arc::new(log_store), object_store)
    }

    async fn create_log_and_object_store(&self) -> (RaftEngineLogStore, ObjectStore) {
        let data_home = self.data_home.path();
        let wal_path = data_home.join("wal");
        let data_path = data_home.join("data").as_path().display().to_string();

        let log_store = log_store_util::create_tmp_local_file_log_store(&wal_path).await;
        let mut builder = Fs::default();
        builder.root(&data_path);
        let object_store = ObjectStore::new(builder).unwrap().finish();

        (log_store, object_store)
    }

    /// If `initial_metadata` is `Some`, creates a new manifest. If `initial_metadata`
    /// is `None`, opens an existing manifest and returns `None` if no such manifest.
    pub async fn create_manifest_manager(
        &self,
        compress_type: CompressionType,
        checkpoint_distance: u64,
        initial_metadata: Option<RegionMetadataRef>,
    ) -> Result<Option<RegionManifestManager>> {
        let data_home = self.data_home.path();
        let manifest_dir = data_home.join("manifest").as_path().display().to_string();

        let mut builder = Fs::default();
        builder.root(&manifest_dir);
        let object_store = ObjectStore::new(builder).unwrap().finish();

        // The "manifest_dir" here should be the relative path from the `object_store`'s root.
        // Otherwise the OpenDal's list operation would fail with "StripPrefixError". This is
        // because the `object_store`'s root path is "canonicalize"d; and under the Windows,
        // canonicalize a path will prepend "\\?\" to it. This behavior will cause the original
        // happen-to-be-working list on an absolute path failed on Windows.
        let manifest_dir = "/".to_string();

        let manifest_opts = RegionManifestOptions {
            manifest_dir,
            object_store,
            compress_type,
            checkpoint_distance,
        };

        if let Some(metadata) = initial_metadata {
            RegionManifestManager::new(metadata, manifest_opts)
                .await
                .map(Some)
        } else {
            RegionManifestManager::open(manifest_opts).await
        }
    }
}

/// Builder to mock a [RegionCreateRequest].
pub struct CreateRequestBuilder {
    region_dir: String,
    tag_num: usize,
    field_num: usize,
    create_if_not_exists: bool,
}

impl Default for CreateRequestBuilder {
    fn default() -> Self {
        CreateRequestBuilder {
            region_dir: "test".to_string(),
            tag_num: 1,
            field_num: 1,
            create_if_not_exists: false,
        }
    }
}

impl CreateRequestBuilder {
    pub fn new() -> CreateRequestBuilder {
        CreateRequestBuilder::default()
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

    pub fn build(&self) -> RegionCreateRequest {
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
                column_id,
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
                column_id,
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
            column_id,
        });

        RegionCreateRequest {
            // We use empty engine name as we already locates the engine.
            engine: String::new(),
            column_metadatas,
            primary_key,
            create_if_not_exists: self.create_if_not_exists,
            options: HashMap::default(),
            region_dir: self.region_dir.clone(),
        }
    }
}

// TODO(yingwen): Support conversion in greptime-proto.
/// Creates value for i64.
#[cfg(test)]
pub(crate) fn i64_value(data: i64) -> v1::Value {
    v1::Value {
        value_data: Some(ValueData::I64Value(data)),
    }
}

/// Creates value for timestamp millis.
#[cfg(test)]
pub(crate) fn ts_ms_value(data: i64) -> v1::Value {
    v1::Value {
        value_data: Some(ValueData::TsMillisecondValue(data)),
    }
}

/// A reader for test that pop [Batch] from a vector.
pub struct VecBatchReader {
    batches: Vec<Batch>,
}

impl VecBatchReader {
    pub fn new(batches: &[Batch]) -> VecBatchReader {
        let batches = batches.iter().rev().cloned().collect();

        VecBatchReader { batches }
    }
}

#[async_trait::async_trait]
impl BatchReader for VecBatchReader {
    async fn next_batch(&mut self) -> Result<Option<Batch>> {
        Ok(self.batches.pop())
    }
}

impl Iterator for VecBatchReader {
    type Item = Result<Batch>;

    fn next(&mut self) -> Option<Result<Batch>> {
        self.batches.pop().map(Ok)
    }
}

pub fn new_batch_builder(
    primary_key: &[u8],
    timestamps: &[i64],
    sequences: &[u64],
    op_types: &[OpType],
    field: &[u64],
) -> BatchBuilder {
    let mut builder = BatchBuilder::new(primary_key.to_vec());
    builder
        .timestamps_array(Arc::new(TimestampMillisecondArray::from_iter_values(
            timestamps.iter().copied(),
        )))
        .unwrap()
        .sequences_array(Arc::new(UInt64Array::from_iter_values(
            sequences.iter().copied(),
        )))
        .unwrap()
        .op_types_array(Arc::new(UInt8Array::from_iter_values(
            op_types.iter().map(|v| *v as u8),
        )))
        .unwrap()
        .push_field_array(
            1,
            Arc::new(UInt64Array::from_iter_values(field.iter().copied())),
        )
        .unwrap();
    builder
}

pub fn new_batch(
    primary_key: &[u8],
    timestamps: &[i64],
    sequences: &[u64],
    op_types: &[OpType],
    field: &[u64],
) -> Batch {
    new_batch_builder(primary_key, timestamps, sequences, op_types, field)
        .build()
        .unwrap()
}

/// A mock [WriteBufferManager] that supports controlling whether to flush/stall.
#[derive(Debug, Default)]
pub struct MockWriteBufferManager {
    should_flush: AtomicBool,
    should_stall: AtomicBool,
    memory_used: AtomicUsize,
    memory_active: AtomicUsize,
}

impl MockWriteBufferManager {
    /// Set whether to flush the engine.
    pub fn set_should_flush(&self, value: bool) {
        self.should_flush.store(value, Ordering::Relaxed);
    }

    /// Set whether to stall the engine.
    pub fn set_should_stall(&self, value: bool) {
        self.should_stall.store(value, Ordering::Relaxed);
    }

    /// Returns memory usage of mutable memtables.
    pub fn mutable_usage(&self) -> usize {
        self.memory_active.load(Ordering::Relaxed)
    }
}

impl WriteBufferManager for MockWriteBufferManager {
    fn should_flush_engine(&self) -> bool {
        self.should_flush.load(Ordering::Relaxed)
    }

    fn should_stall(&self) -> bool {
        self.should_stall.load(Ordering::Relaxed)
    }

    fn reserve_mem(&self, mem: usize) {
        self.memory_used.fetch_add(mem, Ordering::Relaxed);
        self.memory_active.fetch_add(mem, Ordering::Relaxed);
    }

    fn schedule_free_mem(&self, mem: usize) {
        self.memory_active.fetch_sub(mem, Ordering::Relaxed);
    }

    fn free_mem(&self, mem: usize) {
        self.memory_used.fetch_sub(mem, Ordering::Relaxed);
    }

    fn memory_usage(&self) -> usize {
        self.memory_used.load(Ordering::Relaxed)
    }
}
