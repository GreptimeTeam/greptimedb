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

//! Flush tests for mito engine.

use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
use std::time::Duration;

use api::v1::Rows;
use async_trait::async_trait;
use common_base::Plugins;
use common_recordbatch::RecordBatches;
use common_time::util::current_time_millis;
use common_wal::options::WAL_OPTIONS_KEY;
use rstest::rstest;
use rstest_reuse::{self, apply};
use store_api::ManifestVersion;
use store_api::metadata::RegionMetadataRef;
use store_api::region_engine::RegionEngine;
use store_api::region_request::{RegionFlushRequest, RegionRequest};
use store_api::storage::{RegionId, ScanRequest};
use tokio::sync::Notify;

use crate::config::MitoConfig;
use crate::engine::listener::{FlushListener, StallListener};
use crate::engine::region_hook::{RegionHook, RegionHookRef, SstFileInfo};
use crate::manifest::action::RegionMetaActionList;
use crate::test_util::{
    CreateRequestBuilder, LogStoreFactory, MockWriteBufferManager, TestEnv, build_rows,
    build_rows_for_key, flush_region, kafka_log_store_factory, multiple_log_store_factories,
    prepare_test_for_kafka_log_store, put_rows, raft_engine_log_store_factory, reopen_region,
    rows_schema, single_kafka_log_store_factory,
};
use crate::time_provider::TimeProvider;
use crate::worker::MAX_INITIAL_CHECK_DELAY_SECS;

#[tokio::test]
async fn test_manual_flush() {
    test_manual_flush_with_format(false).await;
    test_manual_flush_with_format(true).await;
}

async fn test_manual_flush_with_format(flat_format: bool) {
    let mut env = TestEnv::new().await;
    let engine = env
        .create_engine(MitoConfig {
            default_flat_format: flat_format,
            ..Default::default()
        })
        .await;

    let region_id = RegionId::new(1, 1);
    env.get_schema_metadata_manager()
        .register_region_table_info(
            region_id.table_id(),
            "test_table",
            "test_catalog",
            "test_schema",
            None,
            env.get_kv_backend(),
        )
        .await;

    let request = CreateRequestBuilder::new().build();

    let column_schemas = rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    let rows = Rows {
        schema: column_schemas,
        rows: build_rows(0, 3),
    };
    put_rows(&engine, region_id, rows).await;

    flush_region(&engine, region_id, None).await;

    let request = ScanRequest::default();
    let scanner = engine.scanner(region_id, request).await.unwrap();
    assert_eq!(0, scanner.num_memtables());
    assert_eq!(1, scanner.num_files());
    let stream = scanner.scan().await.unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    let expected = "\
+-------+---------+---------------------+
| tag_0 | field_0 | ts                  |
+-------+---------+---------------------+
| 0     | 0.0     | 1970-01-01T00:00:00 |
| 1     | 1.0     | 1970-01-01T00:00:01 |
| 2     | 2.0     | 1970-01-01T00:00:02 |
+-------+---------+---------------------+";
    assert_eq!(expected, batches.pretty_print().unwrap());
}

#[tokio::test]
async fn test_flush_engine() {
    test_flush_engine_with_format(false).await;
    test_flush_engine_with_format(true).await;
}

async fn test_flush_engine_with_format(flat_format: bool) {
    let mut env = TestEnv::new().await;
    let write_buffer_manager = Arc::new(MockWriteBufferManager::default());
    let listener = Arc::new(FlushListener::default());
    let engine = env
        .create_engine_with(
            MitoConfig {
                default_flat_format: flat_format,
                ..Default::default()
            },
            Some(write_buffer_manager.clone()),
            Some(listener.clone()),
            None,
        )
        .await;

    let region_id = RegionId::new(1, 1);
    env.get_schema_metadata_manager()
        .register_region_table_info(
            region_id.table_id(),
            "test_table",
            "test_catalog",
            "test_schema",
            None,
            env.get_kv_backend(),
        )
        .await;

    let request = CreateRequestBuilder::new().build();

    let column_schemas = rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    // Prepares rows for flush.
    let rows = Rows {
        schema: column_schemas.clone(),
        rows: build_rows_for_key("a", 0, 2, 0),
    };
    put_rows(&engine, region_id, rows).await;

    write_buffer_manager.set_should_flush(true);

    // Writes to the mutable memtable and triggers flush.
    let rows = Rows {
        schema: column_schemas.clone(),
        rows: build_rows_for_key("b", 0, 2, 0),
    };
    put_rows(&engine, region_id, rows).await;

    // Wait until flush is finished.
    listener.wait().await;

    let request = ScanRequest::default();
    let scanner = engine.scanner(region_id, request).await.unwrap();
    assert_eq!(1, scanner.num_memtables());
    assert_eq!(1, scanner.num_files());
    let stream = scanner.scan().await.unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    let expected = "\
+-------+---------+---------------------+
| tag_0 | field_0 | ts                  |
+-------+---------+---------------------+
| a     | 0.0     | 1970-01-01T00:00:00 |
| a     | 1.0     | 1970-01-01T00:00:01 |
| b     | 0.0     | 1970-01-01T00:00:00 |
| b     | 1.0     | 1970-01-01T00:00:01 |
+-------+---------+---------------------+";
    assert_eq!(expected, batches.pretty_print().unwrap());
}

#[tokio::test]
async fn test_write_stall() {
    test_write_stall_with_format(false).await;
    test_write_stall_with_format(true).await;
}

async fn test_write_stall_with_format(flat_format: bool) {
    let mut env = TestEnv::new().await;
    let write_buffer_manager = Arc::new(MockWriteBufferManager::default());
    let listener = Arc::new(StallListener::default());
    let engine = env
        .create_engine_with(
            MitoConfig {
                default_flat_format: flat_format,
                ..Default::default()
            },
            Some(write_buffer_manager.clone()),
            Some(listener.clone()),
            None,
        )
        .await;

    let region_id = RegionId::new(1, 1);
    env.get_schema_metadata_manager()
        .register_region_table_info(
            region_id.table_id(),
            "test_table",
            "test_catalog",
            "test_schema",
            None,
            env.get_kv_backend(),
        )
        .await;
    let request = CreateRequestBuilder::new().build();

    let column_schemas = rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    // Prepares rows for flush.
    let rows = Rows {
        schema: column_schemas.clone(),
        rows: build_rows_for_key("a", 0, 2, 0),
    };
    put_rows(&engine, region_id, rows).await;

    // Stalls the engine.
    write_buffer_manager.set_should_stall(true);

    let engine_cloned = engine.clone();
    // Spawns a task to flush the engine on stall.
    tokio::spawn(async move {
        listener.wait().await;

        flush_region(&engine_cloned, region_id, None).await;
    });

    // Triggers write stall.
    let rows = Rows {
        schema: column_schemas.clone(),
        rows: build_rows_for_key("b", 0, 2, 0),
    };
    put_rows(&engine, region_id, rows).await;

    let request = ScanRequest::default();
    let scanner = engine.scanner(region_id, request).await.unwrap();
    assert_eq!(1, scanner.num_memtables());
    assert_eq!(1, scanner.num_files());
    let stream = scanner.scan().await.unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    let expected = "\
+-------+---------+---------------------+
| tag_0 | field_0 | ts                  |
+-------+---------+---------------------+
| a     | 0.0     | 1970-01-01T00:00:00 |
| a     | 1.0     | 1970-01-01T00:00:01 |
| b     | 0.0     | 1970-01-01T00:00:00 |
| b     | 1.0     | 1970-01-01T00:00:01 |
+-------+---------+---------------------+";
    assert_eq!(expected, batches.pretty_print().unwrap());
}

#[tokio::test]
async fn test_flush_empty() {
    test_flush_empty_with_format(false).await;
    test_flush_empty_with_format(true).await;
}

async fn test_flush_empty_with_format(flat_format: bool) {
    let mut env = TestEnv::new().await;
    let write_buffer_manager = Arc::new(MockWriteBufferManager::default());
    let engine = env
        .create_engine_with(
            MitoConfig {
                default_flat_format: flat_format,
                ..Default::default()
            },
            Some(write_buffer_manager.clone()),
            None,
            None,
        )
        .await;

    let region_id = RegionId::new(1, 1);
    env.get_schema_metadata_manager()
        .register_region_table_info(
            region_id.table_id(),
            "test_table",
            "test_catalog",
            "test_schema",
            None,
            env.get_kv_backend(),
        )
        .await;
    let request = CreateRequestBuilder::new().build();

    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    flush_region(&engine, region_id, None).await;

    let request = ScanRequest::default();
    let scanner = engine.scanner(region_id, request).await.unwrap();
    assert_eq!(0, scanner.num_memtables());
    assert_eq!(0, scanner.num_files());
    let stream = scanner.scan().await.unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    let expected = "\
++
++";
    assert_eq!(expected, batches.pretty_print().unwrap());
}

#[apply(multiple_log_store_factories)]
async fn test_flush_reopen_region(factory: Option<LogStoreFactory>) {
    use std::collections::HashMap;

    use common_wal::options::{KafkaWalOptions, WalOptions};

    common_telemetry::init_default_ut_logging();
    let Some(factory) = factory else {
        return;
    };

    let mut env = TestEnv::new().await.with_log_store_factory(factory.clone());
    let engine = env.create_engine(MitoConfig::default()).await;
    let region_id = RegionId::new(1, 1);
    env.get_schema_metadata_manager()
        .register_region_table_info(
            region_id.table_id(),
            "test_table",
            "test_catalog",
            "test_schema",
            None,
            env.get_kv_backend(),
        )
        .await;

    let topic = prepare_test_for_kafka_log_store(&factory).await;
    let request = CreateRequestBuilder::new()
        .kafka_topic(topic.clone())
        .build();
    let table_dir = request.table_dir.clone();

    let column_schemas = rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    let rows = Rows {
        schema: column_schemas.clone(),
        rows: build_rows_for_key("a", 0, 3, 0),
    };
    put_rows(&engine, region_id, rows).await;

    flush_region(&engine, region_id, None).await;
    let check_region = || {
        let region = engine.get_region(region_id).unwrap();
        let version_data = region.version_control.current();
        assert_eq!(1, version_data.last_entry_id);
        assert_eq!(3, version_data.committed_sequence);
        assert_eq!(1, version_data.version.flushed_entry_id);
        assert_eq!(3, version_data.version.flushed_sequence);
    };
    check_region();

    let mut options = HashMap::new();
    if let Some(topic) = &topic {
        options.insert(
            WAL_OPTIONS_KEY.to_string(),
            serde_json::to_string(&WalOptions::Kafka(KafkaWalOptions {
                topic: topic.clone(),
            }))
            .unwrap(),
        );
    };
    reopen_region(&engine, region_id, table_dir, true, options).await;
    check_region();

    // Puts again.
    let rows = Rows {
        schema: column_schemas.clone(),
        rows: build_rows_for_key("a", 0, 2, 10),
    };
    put_rows(&engine, region_id, rows).await;
    let region = engine.get_region(region_id).unwrap();
    let version_data = region.version_control.current();
    assert_eq!(2, version_data.last_entry_id);
    assert_eq!(5, version_data.committed_sequence);
}

#[derive(Debug)]
pub(crate) struct MockTimeProvider {
    now: AtomicI64,
    elapsed: AtomicI64,
}

impl TimeProvider for MockTimeProvider {
    fn current_time_millis(&self) -> i64 {
        self.now.load(Ordering::Relaxed)
    }

    fn elapsed_since(&self, _current_millis: i64) -> i64 {
        self.elapsed.load(Ordering::Relaxed)
    }

    fn wait_duration(&self, _duration: Duration) -> Duration {
        Duration::from_millis(20)
    }
}

impl MockTimeProvider {
    pub(crate) fn new(now: i64) -> Self {
        Self {
            now: AtomicI64::new(now),
            elapsed: AtomicI64::new(0),
        }
    }

    pub(crate) fn set_now(&self, now: i64) {
        self.now.store(now, Ordering::Relaxed);
    }

    fn set_elapsed(&self, elapsed: i64) {
        self.elapsed.store(elapsed, Ordering::Relaxed);
    }
}

#[tokio::test]
async fn test_auto_flush_engine() {
    test_auto_flush_engine_with_format(false).await;
    test_auto_flush_engine_with_format(true).await;
}

async fn test_auto_flush_engine_with_format(flat_format: bool) {
    let mut env = TestEnv::new().await;
    let write_buffer_manager = Arc::new(MockWriteBufferManager::default());
    let listener = Arc::new(FlushListener::default());
    let now = current_time_millis();
    let time_provider = Arc::new(MockTimeProvider::new(now));
    let engine = env
        .create_engine_with_time(
            MitoConfig {
                auto_flush_interval: Duration::from_secs(60 * 5),
                default_flat_format: flat_format,
                ..Default::default()
            },
            Some(write_buffer_manager.clone()),
            Some(listener.clone()),
            time_provider.clone(),
        )
        .await;
    let region_id = RegionId::new(1, 1);
    env.get_schema_metadata_manager()
        .register_region_table_info(
            region_id.table_id(),
            "test_table",
            "test_catalog",
            "test_schema",
            None,
            env.get_kv_backend(),
        )
        .await;

    let request = CreateRequestBuilder::new().build();

    let column_schemas = rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    // Prepares rows for flush.
    let rows = Rows {
        schema: column_schemas.clone(),
        rows: build_rows_for_key("a", 0, 2, 0),
    };
    put_rows(&engine, region_id, rows).await;

    // Sets current time to now + auto_flush_interval * 2.
    time_provider.set_now(now + (60 * 5 * 2) * 1000);
    // Sets elapsed time to MAX_INITIAL_CHECK_DELAY_SECS + 1.
    time_provider.set_elapsed((MAX_INITIAL_CHECK_DELAY_SECS as i64 + 1) * 1000);

    // Wait until flush is finished.
    tokio::time::timeout(Duration::from_secs(3), listener.wait())
        .await
        .unwrap();

    let request = ScanRequest::default();
    let scanner = engine.scanner(region_id, request).await.unwrap();
    assert_eq!(0, scanner.num_memtables());
    assert_eq!(1, scanner.num_files());
    let stream = scanner.scan().await.unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    let expected = "\
+-------+---------+---------------------+
| tag_0 | field_0 | ts                  |
+-------+---------+---------------------+
| a     | 0.0     | 1970-01-01T00:00:00 |
| a     | 1.0     | 1970-01-01T00:00:01 |
+-------+---------+---------------------+";
    assert_eq!(expected, batches.pretty_print().unwrap());
}

#[tokio::test]
async fn test_flush_workers() {
    test_flush_workers_with_format(false).await;
    test_flush_workers_with_format(true).await;
}

async fn test_flush_workers_with_format(flat_format: bool) {
    common_telemetry::init_default_ut_logging();
    let mut env = TestEnv::new().await;
    let write_buffer_manager = Arc::new(MockWriteBufferManager::default());
    let listener = Arc::new(FlushListener::default());
    let engine = env
        .create_engine_with(
            MitoConfig {
                num_workers: 2,
                default_flat_format: flat_format,
                ..Default::default()
            },
            Some(write_buffer_manager.clone()),
            Some(listener.clone()),
            None,
        )
        .await;

    let region_id0 = RegionId::new(1, 0);
    let region_id1 = RegionId::new(1, 1);
    env.get_schema_metadata_manager()
        .register_region_table_info(
            region_id0.table_id(),
            "test_table",
            "test_catalog",
            "test_schema",
            None,
            env.get_kv_backend(),
        )
        .await;

    let request = CreateRequestBuilder::new().table_dir("r0").build();
    let column_schemas = rows_schema(&request);
    engine
        .handle_request(region_id0, RegionRequest::Create(request))
        .await
        .unwrap();
    let request = CreateRequestBuilder::new().table_dir("r1").build();
    engine
        .handle_request(region_id1, RegionRequest::Create(request.clone()))
        .await
        .unwrap();

    // Prepares rows for flush.
    let rows = Rows {
        schema: column_schemas.clone(),
        rows: build_rows_for_key("a", 0, 2, 0),
    };
    put_rows(&engine, region_id0, rows.clone()).await;
    put_rows(&engine, region_id1, rows).await;

    write_buffer_manager.set_should_flush(true);

    // Writes to the mutable memtable and triggers flush for region 0.
    let rows = Rows {
        schema: column_schemas.clone(),
        rows: build_rows_for_key("b", 0, 2, 0),
    };
    put_rows(&engine, region_id0, rows).await;

    // Waits until flush is finished.
    while listener.success_count() < 3 {
        listener.wait().await;
    }

    // Scans region 1.
    let request = ScanRequest::default();
    let scanner = engine.scanner(region_id1, request).await.unwrap();
    assert_eq!(0, scanner.num_memtables());
    assert_eq!(1, scanner.num_files());
    let stream = scanner.scan().await.unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    let expected = "\
+-------+---------+---------------------+
| tag_0 | field_0 | ts                  |
+-------+---------+---------------------+
| a     | 0.0     | 1970-01-01T00:00:00 |
| a     | 1.0     | 1970-01-01T00:00:01 |
+-------+---------+---------------------+";
    assert_eq!(expected, batches.pretty_print().unwrap());
}

#[apply(single_kafka_log_store_factory)]
async fn test_update_topic_latest_entry_id(factory: Option<LogStoreFactory>) {
    common_telemetry::init_default_ut_logging();
    let Some(factory) = factory else {
        return;
    };
    let write_buffer_manager = Arc::new(MockWriteBufferManager::default());
    let listener = Arc::new(FlushListener::default());

    let mut env = TestEnv::new().await.with_log_store_factory(factory.clone());
    let engine = env
        .create_engine_with(
            MitoConfig::default(),
            Some(write_buffer_manager.clone()),
            Some(listener.clone()),
            None,
        )
        .await;
    let region_id = RegionId::new(1, 1);
    env.get_schema_metadata_manager()
        .register_region_table_info(
            region_id.table_id(),
            "test_table",
            "test_catalog",
            "test_schema",
            None,
            env.get_kv_backend(),
        )
        .await;

    let topic = prepare_test_for_kafka_log_store(&factory).await;
    let request = CreateRequestBuilder::new()
        .kafka_topic(topic.clone())
        .build();
    let column_schemas = rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request.clone()))
        .await
        .unwrap();

    let region = engine.get_region(region_id).unwrap();
    assert_eq!(region.topic_latest_entry_id.load(Ordering::Relaxed), 0);

    let rows = Rows {
        schema: column_schemas.clone(),
        rows: build_rows_for_key("a", 0, 2, 0),
    };
    put_rows(&engine, region_id, rows.clone()).await;

    let request = RegionFlushRequest::default();
    engine
        .handle_request(region_id, RegionRequest::Flush(request.clone()))
        .await
        .unwrap();
    // Wait until flush is finished.
    listener.wait().await;
    assert_eq!(region.topic_latest_entry_id.load(Ordering::Relaxed), 0);

    engine
        .handle_request(region_id, RegionRequest::Flush(request.clone()))
        .await
        .unwrap();
    assert_eq!(region.topic_latest_entry_id.load(Ordering::Relaxed), 1);
}

#[derive(Debug)]
struct MockRegionHook {
    sst_written_count: AtomicUsize,
    manifest_updated_count: AtomicUsize,
    notify: Notify,
}

impl MockRegionHook {
    fn new() -> Self {
        Self {
            sst_written_count: AtomicUsize::new(0),
            manifest_updated_count: AtomicUsize::new(0),
            notify: Notify::new(),
        }
    }

    async fn wait_for_manifest_update(&self) {
        self.notify.notified().await;
    }
}

#[async_trait]
impl RegionHook for MockRegionHook {
    async fn on_sst_files_written(
        &self,
        region_id: RegionId,
        _region_metadata: &RegionMetadataRef,
        files: &[SstFileInfo<'_>],
    ) {
        self.sst_written_count
            .fetch_add(files.len(), Ordering::Relaxed);
        common_telemetry::info!(
            "MockRegionHook::on_sst_files_written: region={}, files={}",
            region_id,
            files.len(),
        );
        for (i, file) in files.iter().enumerate() {
            common_telemetry::info!(
                "  file[{}]: file_id={}, num_rows={}, num_series={}, file_size={}",
                i,
                file.sst_info_ref.file_id,
                file.sst_info_ref.num_rows,
                file.sst_info_ref.num_series,
                file.sst_info_ref.file_size,
            );
        }
    }

    async fn on_manifest_updated(
        &self,
        region_id: RegionId,
        action_list: &RegionMetaActionList,
        manifest_version: ManifestVersion,
    ) {
        self.manifest_updated_count.fetch_add(1, Ordering::Relaxed);
        // Count files added across all Edit actions.
        let files_added: usize = action_list
            .actions
            .iter()
            .map(|action| match action {
                crate::manifest::action::RegionMetaAction::Edit(edit) => edit.files_to_add.len(),
                _ => 0,
            })
            .sum();
        common_telemetry::info!(
            "MockRegionHook::on_manifest_updated: region={}, manifest_version={}, files_added={}",
            region_id,
            manifest_version,
            files_added,
        );
        self.notify.notify_one();
    }
}

#[tokio::test]
async fn test_region_hook() {
    common_telemetry::init_default_ut_logging();
    let mut env = TestEnv::new().await;

    let hook = Arc::new(MockRegionHook::new());
    let plugins = Plugins::new();
    plugins.insert(hook.clone() as RegionHookRef);

    let engine = env
        .create_engine_with_plugins(MitoConfig::default(), plugins)
        .await;

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new().build();
    let column_schemas = rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    let rows = Rows {
        schema: column_schemas.clone(),
        rows: build_rows_for_key("a", 0, 10, 0),
    };
    put_rows(&engine, region_id, rows).await;

    let rows2 = Rows {
        schema: column_schemas.clone(),
        rows: build_rows_for_key("b", 0, 10, 0),
    };
    put_rows(&engine, region_id, rows2).await;

    flush_region(&engine, region_id, None).await;

    hook.wait_for_manifest_update().await;

    let sst_count = hook.sst_written_count.load(Ordering::Relaxed);
    let manifest_count = hook.manifest_updated_count.load(Ordering::Relaxed);

    assert!(
        sst_count > 0,
        "Expected at least 1 SST file, got {sst_count}"
    );
    assert_eq!(
        manifest_count, 1,
        "Expected exactly 1 manifest update, got {manifest_count}"
    );

    common_telemetry::info!(
        "test_region_hook passed: sst_count={}, manifest_count={}",
        sst_count,
        manifest_count,
    );
}
