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

pub mod memtable_util;
pub mod meta_util;
pub mod scheduler_util;
pub mod sst_util;
pub mod version_util;

use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use api::greptime_proto::v1;
use api::helper::ColumnDataTypeWrapper;
use api::v1::value::ValueData;
use api::v1::{OpType, Row, Rows, SemanticType};
use common_datasource::compression::CompressionType;
use common_test_util::temp_dir::{create_temp_dir, TempDir};
use datatypes::arrow::array::{TimestampMillisecondArray, UInt64Array, UInt8Array};
use datatypes::prelude::ConcreteDataType;
use datatypes::schema::ColumnSchema;
use log_store::raft_engine::log_store::RaftEngineLogStore;
use log_store::test_util::log_store_util;
use object_store::manager::{ObjectStoreManager, ObjectStoreManagerRef};
use object_store::services::Fs;
use object_store::ObjectStore;
use store_api::metadata::{ColumnMetadata, RegionMetadataRef};
use store_api::region_engine::RegionEngine;
use store_api::region_request::{
    RegionCloseRequest, RegionCreateRequest, RegionDeleteRequest, RegionFlushRequest,
    RegionOpenRequest, RegionPutRequest, RegionRequest,
};
use store_api::storage::{ColumnId, RegionId};

use crate::config::MitoConfig;
use crate::engine::listener::EventListenerRef;
use crate::engine::{MitoEngine, MITO_ENGINE_NAME};
use crate::error::Result;
use crate::flush::{WriteBufferManager, WriteBufferManagerRef};
use crate::manifest::manager::{RegionManifestManager, RegionManifestOptions};
use crate::read::{Batch, BatchBuilder, BatchReader};
use crate::sst::file_purger::{FilePurger, FilePurgerRef, PurgeRequest};
use crate::worker::WorkerGroup;

#[derive(Debug)]
pub(crate) struct NoopFilePurger;

impl FilePurger for NoopFilePurger {
    fn send_request(&self, _request: PurgeRequest) {}
}

pub(crate) fn new_noop_file_purger() -> FilePurgerRef {
    Arc::new(NoopFilePurger {})
}

/// Env to test mito engine.
pub struct TestEnv {
    /// Path to store data.
    data_home: TempDir,
    logstore: Option<Arc<RaftEngineLogStore>>,
    object_store_manager: Option<ObjectStoreManagerRef>,
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
            object_store_manager: None,
        }
    }

    /// Returns a new env with specific `prefix` for test.
    pub fn with_prefix(prefix: &str) -> TestEnv {
        TestEnv {
            data_home: create_temp_dir(prefix),
            logstore: None,
            object_store_manager: None,
        }
    }

    /// Returns a new env with specific `data_home` for test.
    pub fn with_data_home(data_home: TempDir) -> TestEnv {
        TestEnv {
            data_home,
            logstore: None,
            object_store_manager: None,
        }
    }

    pub fn get_logstore(&self) -> Option<Arc<RaftEngineLogStore>> {
        self.logstore.clone()
    }

    pub fn get_object_store(&self) -> Option<ObjectStore> {
        self.object_store_manager
            .as_ref()
            .map(|manager| manager.default_object_store().clone())
    }

    pub fn data_home(&self) -> &Path {
        self.data_home.path()
    }

    pub fn get_object_store_manager(&self) -> Option<Arc<ObjectStoreManager>> {
        self.object_store_manager.clone()
    }

    /// Creates a new engine with specific config under this env.
    pub async fn create_engine(&mut self, config: MitoConfig) -> MitoEngine {
        let (log_store, object_store_manager) = self.create_log_and_object_store_manager().await;

        let logstore = Arc::new(log_store);
        let object_store_manager = Arc::new(object_store_manager);
        self.logstore = Some(logstore.clone());
        self.object_store_manager = Some(object_store_manager.clone());
        MitoEngine::new(config, logstore, object_store_manager)
    }

    /// Creates a new engine with specific config and existing logstore and object store manager.
    pub async fn create_follower_engine(&mut self, config: MitoConfig) -> MitoEngine {
        let logstore = self.logstore.as_ref().unwrap().clone();
        let object_store_manager = self.object_store_manager.as_ref().unwrap().clone();

        MitoEngine::new(config, logstore, object_store_manager)
    }

    /// Creates a new engine with specific config and manager/listener under this env.
    pub async fn create_engine_with(
        &mut self,
        config: MitoConfig,
        manager: Option<WriteBufferManagerRef>,
        listener: Option<EventListenerRef>,
    ) -> MitoEngine {
        let (log_store, object_store_manager) = self.create_log_and_object_store_manager().await;

        let logstore = Arc::new(log_store);
        let object_store_manager = Arc::new(object_store_manager);
        self.logstore = Some(logstore.clone());
        self.object_store_manager = Some(object_store_manager.clone());
        MitoEngine::new_for_test(config, logstore, object_store_manager, manager, listener)
    }

    pub async fn create_engine_with_multiple_object_stores(
        &mut self,
        config: MitoConfig,
        manager: Option<WriteBufferManagerRef>,
        listener: Option<EventListenerRef>,
        custom_storage_names: &[&str],
    ) -> MitoEngine {
        let (logstore, mut object_store_manager) = self.create_log_and_object_store_manager().await;
        for storage_name in custom_storage_names {
            let data_path = self
                .data_home
                .path()
                .join("data")
                .join(storage_name)
                .as_path()
                .display()
                .to_string();
            let mut builder = Fs::default();
            builder.root(&data_path);
            let object_store = ObjectStore::new(builder).unwrap().finish();
            object_store_manager.add(storage_name, object_store);
        }
        let logstore = Arc::new(logstore);
        let object_store_manager = Arc::new(object_store_manager);
        self.logstore = Some(logstore.clone());
        self.object_store_manager = Some(object_store_manager.clone());
        MitoEngine::new_for_test(config, logstore, object_store_manager, manager, listener)
    }

    /// Reopen the engine.
    pub async fn reopen_engine(&mut self, engine: MitoEngine, config: MitoConfig) -> MitoEngine {
        engine.stop().await.unwrap();

        MitoEngine::new(
            config,
            self.logstore.clone().unwrap(),
            self.object_store_manager.clone().unwrap(),
        )
    }

    /// Open the engine.
    pub async fn open_engine(&mut self, config: MitoConfig) -> MitoEngine {
        MitoEngine::new(
            config,
            self.logstore.clone().unwrap(),
            self.object_store_manager.clone().unwrap(),
        )
    }

    /// Only initializes the object store manager, returns the default object store.
    pub fn init_object_store_manager(&mut self) -> ObjectStore {
        self.object_store_manager = Some(Arc::new(self.create_object_store_manager()));
        self.get_object_store().unwrap()
    }

    /// Creates a new [WorkerGroup] with specific config under this env.
    pub(crate) async fn create_worker_group(&self, config: MitoConfig) -> WorkerGroup {
        let (log_store, object_store_manager) = self.create_log_and_object_store_manager().await;

        WorkerGroup::start(
            Arc::new(config),
            Arc::new(log_store),
            Arc::new(object_store_manager),
        )
    }

    /// Returns the log store and object store manager.
    async fn create_log_and_object_store_manager(
        &self,
    ) -> (RaftEngineLogStore, ObjectStoreManager) {
        let data_home = self.data_home.path();
        let wal_path = data_home.join("wal");
        let log_store = log_store_util::create_tmp_local_file_log_store(&wal_path).await;

        let object_store_manager = self.create_object_store_manager();
        (log_store, object_store_manager)
    }

    fn create_object_store_manager(&self) -> ObjectStoreManager {
        let data_home = self.data_home.path();
        let data_path = data_home.join("data").as_path().display().to_string();
        let mut builder = Fs::default();
        builder.root(&data_path);
        let object_store = ObjectStore::new(builder).unwrap().finish();
        ObjectStoreManager::new("default", object_store)
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
///
/// It builds schema like `[tag_0, tag_1, ..., field_0, field_1, ..., ts]`.
pub struct CreateRequestBuilder {
    region_dir: String,
    tag_num: usize,
    field_num: usize,
    options: HashMap<String, String>,
    primary_key: Option<Vec<ColumnId>>,
    all_not_null: bool,
    engine: String,
}

impl Default for CreateRequestBuilder {
    fn default() -> Self {
        CreateRequestBuilder {
            region_dir: "test".to_string(),
            tag_num: 1,
            field_num: 1,
            options: HashMap::new(),
            primary_key: None,
            all_not_null: false,
            engine: MITO_ENGINE_NAME.to_string(),
        }
    }
}

impl CreateRequestBuilder {
    #[must_use]
    pub fn new() -> CreateRequestBuilder {
        CreateRequestBuilder::default()
    }

    #[must_use]
    pub fn region_dir(mut self, value: &str) -> Self {
        self.region_dir = value.to_string();
        self
    }

    #[must_use]
    pub fn tag_num(mut self, value: usize) -> Self {
        self.tag_num = value;
        self
    }

    #[must_use]
    pub fn field_num(mut self, value: usize) -> Self {
        self.field_num = value;
        self
    }

    #[must_use]
    pub fn primary_key(mut self, primary_key: Vec<ColumnId>) -> Self {
        self.primary_key = Some(primary_key);
        self
    }

    #[must_use]
    pub fn insert_option(mut self, key: &str, value: &str) -> Self {
        self.options.insert(key.to_string(), value.to_string());
        self
    }

    #[must_use]
    pub fn all_not_null(mut self, value: bool) -> Self {
        self.all_not_null = value;
        self
    }

    pub fn build(&self) -> RegionCreateRequest {
        let mut column_id = 0;
        let mut column_metadatas = Vec::with_capacity(self.tag_num + self.field_num + 1);
        let mut primary_key = Vec::with_capacity(self.tag_num);
        let nullable = !self.all_not_null;
        for i in 0..self.tag_num {
            column_metadatas.push(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    format!("tag_{i}"),
                    ConcreteDataType::string_datatype(),
                    nullable,
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
                    nullable,
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
                // Time index is always not null.
                false,
            ),
            semantic_type: SemanticType::Timestamp,
            column_id,
        });

        RegionCreateRequest {
            engine: self.engine.to_string(),
            column_metadatas,
            primary_key: self.primary_key.clone().unwrap_or(primary_key),
            options: self.options.clone(),
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
        value_data: Some(ValueData::TimestampMillisecondValue(data)),
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
    field_column_id: ColumnId,
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
            field_column_id,
            Arc::new(UInt64Array::from_iter_values(field.iter().copied())),
        )
        .unwrap();
    builder
}

/// Returns a new [Batch] whose field has column id 1.
pub fn new_batch(
    primary_key: &[u8],
    timestamps: &[i64],
    sequences: &[u64],
    op_types: &[OpType],
    field: &[u64],
) -> Batch {
    new_batch_builder(primary_key, timestamps, sequences, op_types, 1, field)
        .build()
        .unwrap()
}

/// Ensure the reader returns batch as `expect`.
pub async fn check_reader_result<R: BatchReader>(reader: &mut R, expect: &[Batch]) {
    let mut result = Vec::new();
    while let Some(batch) = reader.next_batch().await.unwrap() {
        result.push(batch);
    }

    assert_eq!(expect, result);
    // Next call to `next_batch()` still returns None.
    assert!(reader.next_batch().await.unwrap().is_none());
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

pub(crate) fn column_metadata_to_column_schema(metadata: &ColumnMetadata) -> api::v1::ColumnSchema {
    let (datatype, datatype_extension) =
        ColumnDataTypeWrapper::try_from(metadata.column_schema.data_type.clone())
            .unwrap()
            .to_parts();
    api::v1::ColumnSchema {
        column_name: metadata.column_schema.name.clone(),
        datatype: datatype as i32,
        semantic_type: metadata.semantic_type as i32,
        datatype_extension,
    }
}

/// Build rows with schema (string, f64, ts_millis).
pub fn build_rows(start: usize, end: usize) -> Vec<Row> {
    (start..end)
        .map(|i| api::v1::Row {
            values: vec![
                api::v1::Value {
                    value_data: Some(ValueData::StringValue(i.to_string())),
                },
                api::v1::Value {
                    value_data: Some(ValueData::F64Value(i as f64)),
                },
                api::v1::Value {
                    value_data: Some(ValueData::TimestampMillisecondValue(i as i64 * 1000)),
                },
            ],
        })
        .collect()
}

/// Get column schemas for rows.
pub fn rows_schema(request: &RegionCreateRequest) -> Vec<api::v1::ColumnSchema> {
    request
        .column_metadatas
        .iter()
        .map(column_metadata_to_column_schema)
        .collect::<Vec<_>>()
}

/// Get column schemas for delete requests.
pub fn delete_rows_schema(request: &RegionCreateRequest) -> Vec<api::v1::ColumnSchema> {
    request
        .column_metadatas
        .iter()
        .filter(|col| col.semantic_type != SemanticType::Field)
        .map(column_metadata_to_column_schema)
        .collect::<Vec<_>>()
}

/// Put rows into the engine.
pub async fn put_rows(engine: &MitoEngine, region_id: RegionId, rows: Rows) {
    let num_rows = rows.rows.len();
    let rows_inserted = engine
        .handle_request(region_id, RegionRequest::Put(RegionPutRequest { rows }))
        .await
        .unwrap();
    assert_eq!(num_rows, rows_inserted);
}

/// Build rows to put for specific `key`.
pub fn build_rows_for_key(key: &str, start: usize, end: usize, value_start: usize) -> Vec<Row> {
    (start..end)
        .enumerate()
        .map(|(idx, ts)| api::v1::Row {
            values: vec![
                api::v1::Value {
                    value_data: Some(ValueData::StringValue(key.to_string())),
                },
                api::v1::Value {
                    value_data: Some(ValueData::F64Value((value_start + idx) as f64)),
                },
                api::v1::Value {
                    value_data: Some(ValueData::TimestampMillisecondValue(ts as i64 * 1000)),
                },
            ],
        })
        .collect()
}

/// Build rows to delete for specific `key`.
pub fn build_delete_rows_for_key(key: &str, start: usize, end: usize) -> Vec<Row> {
    (start..end)
        .map(|ts| api::v1::Row {
            values: vec![
                api::v1::Value {
                    value_data: Some(ValueData::StringValue(key.to_string())),
                },
                api::v1::Value {
                    value_data: Some(ValueData::TimestampMillisecondValue(ts as i64 * 1000)),
                },
            ],
        })
        .collect()
}

/// Delete rows from the engine.
pub async fn delete_rows(engine: &MitoEngine, region_id: RegionId, rows: Rows) {
    let num_rows = rows.rows.len();
    let rows_inserted = engine
        .handle_request(
            region_id,
            RegionRequest::Delete(RegionDeleteRequest { rows }),
        )
        .await
        .unwrap();
    assert_eq!(num_rows, rows_inserted);
}

/// Flush a region manually.
pub async fn flush_region(engine: &MitoEngine, region_id: RegionId, row_group_size: Option<usize>) {
    let rows = engine
        .handle_request(
            region_id,
            RegionRequest::Flush(RegionFlushRequest { row_group_size }),
        )
        .await
        .unwrap();
    assert_eq!(0, rows);
}

/// Reopen a region.
pub async fn reopen_region(
    engine: &MitoEngine,
    region_id: RegionId,
    region_dir: String,
    writable: bool,
) {
    // Close the region.
    engine
        .handle_request(region_id, RegionRequest::Close(RegionCloseRequest {}))
        .await
        .unwrap();

    // Open the region again.
    engine
        .handle_request(
            region_id,
            RegionRequest::Open(RegionOpenRequest {
                engine: String::new(),
                region_dir,
                options: HashMap::default(),
                skip_wal_replay: false,
            }),
        )
        .await
        .unwrap();

    if writable {
        engine.set_writable(region_id, true).unwrap();
    }
}
