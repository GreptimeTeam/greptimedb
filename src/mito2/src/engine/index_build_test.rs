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

//! Index build tests for mito engine.
//!
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use api::v1::region::{StrictWindow, compact_request};
use api::v1::{Rows, SemanticType};
use async_trait::async_trait;
use common_base::readable_size::ReadableSize;
use common_recordbatch::RecordBatches;
use datatypes::arrow::array::AsArray;
use datatypes::arrow::datatypes::TimestampMillisecondType;
use datatypes::prelude::ConcreteDataType;
use datatypes::schema::{ColumnSchema, SkippingIndexOptions, SkippingIndexType};
use store_api::metadata::ColumnMetadata;
use store_api::region_engine::RegionEngine;
use store_api::region_request::{
    AddColumn, AlterKind, RegionAlterRequest, RegionBuildIndexRequest, RegionCloseRequest,
    RegionCompactRequest, RegionRequest, SetIndexOption,
};
use store_api::storage::{RegionId, ScanRequest};
use tokio::sync::{Notify, Semaphore};

use crate::cache::file_cache::{FileType, IndexKey};
use crate::config::{IndexBuildMode, MitoConfig, Mode};
use crate::engine::MitoEngine;
use crate::engine::compaction_test::put_and_flush;
use crate::engine::listener::{EventListener, GateIndexBuildListener, IndexBuildListener};
use crate::manifest::action::RegionEdit;
use crate::read::scan_region::Scanner;
use crate::sst::file::{FileMeta, RegionFileId, RegionIndexId};
use crate::sst::location;
use crate::test_util::{
    CreateRequestBuilder, TestEnv, build_rows, flush_region, put_rows, reopen_region, rows_schema,
};

fn async_build_mode_config(is_create_on_flush: bool) -> MitoConfig {
    let mut config = MitoConfig::default();
    config.index.build_mode = IndexBuildMode::Async;
    if !is_create_on_flush {
        config.inverted_index.create_on_flush = Mode::Disable;
        config.fulltext_index.create_on_flush = Mode::Disable;
        config.bloom_filter_index.create_on_flush = Mode::Disable;
    }
    config
}

/// Get the number of generated index files for existed sst files in the scanner.
async fn num_of_index_files(engine: &MitoEngine, scanner: &Scanner, region_id: RegionId) -> usize {
    let region = engine.get_region(region_id).unwrap();
    let access_layer = region.access_layer.clone();
    // When there is no file, return 0 directly.
    // Because we can't know region file ids here.
    if scanner.file_ids().is_empty() {
        return 0;
    }
    let mut index_files_count: usize = 0;
    for region_index_id in scanner.index_ids() {
        let index_path = location::index_file_path(
            access_layer.table_dir(),
            region_index_id,
            access_layer.path_type(),
        );
        if access_layer
            .object_store()
            .exists(&index_path)
            .await
            .unwrap()
        {
            index_files_count += 1;
        }
    }
    index_files_count
}

fn assert_listener_counts(
    listener: &IndexBuildListener,
    expected_begin_count: usize,
    expected_success_count: usize,
) {
    assert_eq!(listener.begin_count(), expected_begin_count);
    assert_eq!(listener.finish_count(), expected_success_count);
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum IndexPublicationPhase {
    BeforeManifestCommit,
    AfterManifestCommit,
}

struct IndexPublicationGate {
    phase: IndexPublicationPhase,
    entered: AtomicUsize,
    entered_notify: Notify,
    permits: Semaphore,
    finish_count: AtomicUsize,
    abort_count: AtomicUsize,
    stopped_notify: Notify,
}

impl IndexPublicationGate {
    fn new(phase: IndexPublicationPhase) -> Self {
        Self {
            phase,
            entered: AtomicUsize::new(0),
            entered_notify: Notify::new(),
            permits: Semaphore::new(0),
            finish_count: AtomicUsize::new(0),
            abort_count: AtomicUsize::new(0),
            stopped_notify: Notify::new(),
        }
    }

    async fn enter(&self) {
        self.entered.fetch_add(1, Ordering::Relaxed);
        self.entered_notify.notify_one();
        self.permits
            .acquire()
            .await
            .expect("index publication gate should remain open")
            .forget();
    }

    async fn wait_entered(&self, count: usize) {
        while self.entered.load(Ordering::Relaxed) < count {
            self.entered_notify.notified().await;
        }
    }

    fn release(&self, count: usize) {
        self.permits.add_permits(count);
    }

    async fn wait_stopped(&self, count: usize) {
        while self.finish_count.load(Ordering::Relaxed) + self.abort_count.load(Ordering::Relaxed)
            < count
        {
            self.stopped_notify.notified().await;
        }
    }

    async fn wait_finished(&self, count: usize) {
        while self.finish_count.load(Ordering::Relaxed) < count {
            self.stopped_notify.notified().await;
        }
    }
}

#[async_trait]
impl EventListener for IndexPublicationGate {
    async fn on_index_build_before_manifest_commit(&self, _region_file_id: RegionFileId) {
        if self.phase == IndexPublicationPhase::BeforeManifestCommit {
            self.enter().await;
        }
    }

    async fn on_index_build_manifest_committed(&self, _region_file_id: RegionFileId) {
        if self.phase == IndexPublicationPhase::AfterManifestCommit {
            self.enter().await;
        }
    }

    async fn on_index_build_finish(&self, _region_file_id: RegionFileId) {
        self.finish_count.fetch_add(1, Ordering::Relaxed);
        self.stopped_notify.notify_one();
    }

    async fn on_index_build_abort(&self, _region_file_id: RegionFileId) {
        self.abort_count.fetch_add(1, Ordering::Relaxed);
        self.stopped_notify.notify_one();
    }
}

async fn scan_timestamps(engine: &MitoEngine, region_id: RegionId) -> Vec<i64> {
    let scanner = engine
        .scanner(region_id, ScanRequest::default())
        .await
        .unwrap();
    let batches = RecordBatches::try_collect(scanner.scan().await.unwrap())
        .await
        .unwrap();
    let mut timestamps = Vec::new();
    for batch in batches {
        let column = batch
            .column_by_name("ts")
            .unwrap()
            .as_primitive::<TimestampMillisecondType>();
        timestamps.extend((0..column.len()).map(|index| column.value(index)));
    }
    timestamps
}

async fn current_file_metas(engine: &MitoEngine, region_id: RegionId) -> Vec<FileMeta> {
    engine
        .get_region(region_id)
        .unwrap()
        .version()
        .ssts
        .levels()
        .iter()
        .flat_map(|level| level.files.values())
        .map(|file| file.meta_ref().clone())
        .collect()
}

async fn assert_compacted_state(
    engine: &MitoEngine,
    region_id: RegionId,
    source_files: &[FileMeta],
) {
    let region = engine.get_region(region_id).unwrap();
    let manifest = region.manifest_ctx.manifest().await;
    assert_eq!(manifest.files.len(), 1);
    for source in source_files {
        assert!(!manifest.files.contains_key(&source.file_id));
    }

    let current_files = current_file_metas(engine, region_id).await;
    assert_eq!(current_files.len(), 1);
    assert_eq!(
        manifest.files.keys().copied().collect::<HashSet<_>>(),
        current_files
            .iter()
            .map(|file| file.file_id)
            .collect::<HashSet<_>>()
    );
}

async fn assert_stale_artifact_caches_removed(engine: &MitoEngine, source_files: &[FileMeta]) {
    let cache_manager = engine.cache_manager();
    let write_cache = cache_manager
        .write_cache()
        .expect("race tests enable write cache");
    for source in source_files {
        let index_version = source
            .index_version()
            .map_or(0, |index_version| index_version + 1);
        assert!(
            !write_cache.file_cache().contains_key(&IndexKey::new(
                source.region_id,
                source.file_id,
                FileType::Puffin(index_version),
            )),
            "stale index artifact remains in write cache"
        );
    }
}

async fn assert_index_artifacts_exist(
    engine: &MitoEngine,
    region_id: RegionId,
    source_files: &[FileMeta],
) {
    let region = engine.get_region(region_id).unwrap();
    for source in source_files {
        let index_version = source
            .index_version()
            .map_or(0, |index_version| index_version + 1);
        let index_id = RegionIndexId::new(
            RegionFileId::new(source.region_id, source.file_id),
            index_version,
        );
        let path = location::index_file_path(
            region.access_layer.table_dir(),
            index_id,
            region.access_layer.path_type(),
        );
        assert!(
            region
                .access_layer
                .object_store()
                .exists(&path)
                .await
                .unwrap(),
            "stale index artifact was deleted outside FilePurger/GC: {path}"
        );
    }
}

async fn run_index_publication_compaction_race(phase: IndexPublicationPhase, gc_enabled: bool) {
    let prefix = format!("test_index_publication_{phase:?}_{gc_enabled}_");
    let mut env = TestEnv::with_prefix(&prefix).await;
    let mut config = async_build_mode_config(false);
    config.max_background_index_builds = 4;
    config.gc.enable = gc_enabled;
    config = config.enable_write_cache(
        env.data_home().join("write_cache").display().to_string(),
        ReadableSize::mb(32),
        None,
    );
    let gate = Arc::new(IndexPublicationGate::new(phase));
    let engine = Arc::new(
        env.create_engine_with(config, None, Some(gate.clone()), None)
            .await,
    );

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

    let request = CreateRequestBuilder::new()
        .insert_option("compaction.type", "twcs")
        .insert_option("compaction.twcs.trigger_file_num", "100")
        .build_with_index();
    let table_dir = request.table_dir.clone();
    let column_schemas = rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();
    let purger = format!("{:?}", engine.get_region(region_id).unwrap().file_purger);
    if gc_enabled {
        assert!(purger.contains("ObjectStoreFilePurger"), "{purger}");
    } else {
        assert!(purger.contains("LocalFilePurger"), "{purger}");
    }

    put_and_flush(&engine, region_id, &column_schemas, 0..10).await;
    put_and_flush(&engine, region_id, &column_schemas, 10..20).await;
    reopen_region(&engine, region_id, table_dir.clone(), true, HashMap::new()).await;
    let old_files = current_file_metas(&engine, region_id).await;
    assert_eq!(old_files.len(), 2);

    let engine_for_build = engine.clone();
    let build = tokio::spawn(async move {
        engine_for_build
            .handle_request(
                region_id,
                RegionRequest::BuildIndex(RegionBuildIndexRequest {}),
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(10), gate.wait_entered(old_files.len()))
        .await
        .expect("index builds did not reach the publication gate");

    engine
        .handle_request(
            region_id,
            RegionRequest::Compact(RegionCompactRequest {
                options: compact_request::Options::StrictWindow(StrictWindow {
                    window_seconds: 60,
                }),
                parallelism: None,
                time_range: None,
            }),
        )
        .await
        .unwrap();
    assert_compacted_state(&engine, region_id, &old_files).await;

    gate.release(old_files.len());
    tokio::time::timeout(Duration::from_secs(10), build)
        .await
        .expect("build index request did not finish")
        .expect("build index task panicked")
        .unwrap();
    tokio::time::timeout(Duration::from_secs(10), gate.wait_stopped(old_files.len()))
        .await
        .expect("stale index builds were not consumed");
    assert_eq!(gate.finish_count.load(Ordering::Relaxed), 0);
    assert_eq!(gate.abort_count.load(Ordering::Relaxed), old_files.len());

    assert_compacted_state(&engine, region_id, &old_files).await;
    assert_stale_artifact_caches_removed(&engine, &old_files).await;
    if gc_enabled {
        // ObjectStoreFilePurger leaves remote deletion to reference-aware GC.
        assert_index_artifacts_exist(&engine, region_id, &old_files).await;
    }
    let before_reopen = scan_timestamps(&engine, region_id).await;
    assert_eq!(before_reopen.len(), 20);
    assert_eq!(
        before_reopen.iter().copied().collect::<HashSet<_>>().len(),
        20,
        "scan contains duplicate rows before reopen"
    );

    reopen_region(&engine, region_id, table_dir, true, HashMap::new()).await;
    assert_compacted_state(&engine, region_id, &old_files).await;
    let after_reopen = scan_timestamps(&engine, region_id).await;
    assert_eq!(after_reopen, before_reopen);
}

#[tokio::test]
async fn test_compaction_wins_before_index_manifest_commit() {
    for gc_enabled in [false, true] {
        run_index_publication_compaction_race(
            IndexPublicationPhase::BeforeManifestCommit,
            gc_enabled,
        )
        .await;
    }
}

#[tokio::test]
async fn test_compaction_wins_before_index_worker_apply() {
    for gc_enabled in [false, true] {
        run_index_publication_compaction_race(
            IndexPublicationPhase::AfterManifestCommit,
            gc_enabled,
        )
        .await;
    }
}

#[tokio::test]
async fn test_index_build_uses_physical_file_region_and_logical_manifest_region() {
    let mut env = TestEnv::with_prefix("test_cross_region_index_build_").await;
    let listener = Arc::new(IndexBuildListener::default());
    let config = async_build_mode_config(false).enable_write_cache(
        env.data_home().join("write_cache").display().to_string(),
        ReadableSize::mb(32),
        None,
    );
    let engine = Arc::new(
        env.create_engine_with(config, None, Some(listener.clone()), None)
            .await,
    );

    let source_region_id = RegionId::new(1, 1);
    let target_region_id = RegionId::new(1, 2);
    env.get_schema_metadata_manager()
        .register_region_table_info(
            source_region_id.table_id(),
            "test_table",
            "test_catalog",
            "test_schema",
            None,
            env.get_kv_backend(),
        )
        .await;

    let builder = CreateRequestBuilder::new();
    let source_request = builder.build_with_index();
    let column_schemas = rows_schema(&source_request);
    engine
        .handle_request(source_region_id, RegionRequest::Create(source_request))
        .await
        .unwrap();
    engine
        .handle_request(
            target_region_id,
            RegionRequest::Create(builder.build_with_index()),
        )
        .await
        .unwrap();

    put_and_flush(&engine, source_region_id, &column_schemas, 0..10).await;
    let source_files = current_file_metas(&engine, source_region_id).await;
    assert_eq!(source_files.len(), 1);
    assert_eq!(source_files[0].region_id, source_region_id);

    engine
        .edit_region(
            target_region_id,
            RegionEdit {
                files_to_add: source_files.clone(),
                files_to_remove: Vec::new(),
                timestamp_ms: None,
                compaction_time_window: None,
                flushed_entry_id: None,
                flushed_sequence: None,
                committed_sequence: None,
            },
        )
        .await
        .unwrap();
    engine
        .handle_request(
            target_region_id,
            RegionRequest::BuildIndex(RegionBuildIndexRequest {}),
        )
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(10), listener.wait_finish(1))
        .await
        .expect("cross-region index build did not apply to the logical region");

    let target_files = current_file_metas(&engine, target_region_id).await;
    assert_eq!(target_files.len(), 1);
    assert_eq!(target_files[0].region_id, source_region_id);
    assert!(target_files[0].exists_index());
    let scanner = engine
        .scanner(target_region_id, ScanRequest::default())
        .await
        .unwrap();
    assert_eq!(
        num_of_index_files(&engine, &scanner, target_region_id).await,
        1
    );
    assert_eq!(
        engine
            .get_region(target_region_id)
            .unwrap()
            .manifest_ctx
            .manifest()
            .await
            .files
            .get(&target_files[0].file_id),
        Some(&target_files[0])
    );
    assert!(!current_file_metas(&engine, source_region_id).await[0].exists_index());
}

#[tokio::test]
async fn test_index_build_type_flush() {
    let mut env = TestEnv::with_prefix("test_index_build_type_flush_").await;
    let listener = Arc::new(IndexBuildListener::default());
    let engine = env
        .create_engine_with(
            async_build_mode_config(true),
            None,
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

    let request = CreateRequestBuilder::new().build_with_index();

    let column_schemas = rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    let rows = Rows {
        schema: column_schemas.clone(),
        rows: build_rows(0, 2),
    };
    put_rows(&engine, region_id, rows).await;

    // Before first flush is finished, index file and data file should not exist.
    let scanner = engine
        .scanner(region_id, ScanRequest::default())
        .await
        .unwrap();
    assert_eq!(scanner.num_memtables(), 1);
    assert_eq!(scanner.num_files(), 0);
    assert_eq!(num_of_index_files(&engine, &scanner, region_id).await, 0);

    flush_region(&engine, region_id, None).await;

    // When first flush is just finished, index file should not exist.
    let scanner = engine
        .scanner(region_id, ScanRequest::default())
        .await
        .unwrap();
    assert_eq!(scanner.num_memtables(), 0);
    assert_eq!(scanner.num_files(), 1);
    assert_eq!(num_of_index_files(&engine, &scanner, region_id).await, 0);

    let rows = Rows {
        schema: column_schemas.clone(),
        rows: build_rows(2, 4),
    };
    put_rows(&engine, region_id, rows).await;

    flush_region(&engine, region_id, None).await;

    // After 2 index build task are finished, 2 index files should exist.
    listener.wait_finish(2).await;
    let scanner = engine
        .scanner(region_id, ScanRequest::default())
        .await
        .unwrap();
    assert_eq!(num_of_index_files(&engine, &scanner, region_id).await, 2);
}

#[tokio::test]
async fn test_index_build_type_compact() {
    common_telemetry::init_default_ut_logging();

    let mut env = TestEnv::with_prefix("test_index_build_type_compact_").await;
    let listener = Arc::new(IndexBuildListener::default());
    let engine = env
        .create_engine_with(
            async_build_mode_config(true),
            None,
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

    let request = CreateRequestBuilder::new()
        .insert_option("compaction.type", "twcs")
        .insert_option("compaction.twcs.trigger_file_num", "4")
        .build_with_index();

    let column_schemas = rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    put_and_flush(&engine, region_id, &column_schemas, 10..20).await;
    put_and_flush(&engine, region_id, &column_schemas, 20..30).await;
    put_and_flush(&engine, region_id, &column_schemas, 35..45).await;

    common_telemetry::info!("After flush 3 files");

    // all index build tasks begin means flush tasks are all finished.
    listener.wait_begin(3).await;
    // Before compaction is triggered, files should be 4.
    let scanner = engine
        .scanner(region_id, ScanRequest::default())
        .await
        .unwrap();
    assert_eq!(scanner.num_files(), 3);
    assert!(num_of_index_files(&engine, &scanner, region_id).await <= 3);

    common_telemetry::info!("Checked 3 files, start compact");

    put_and_flush(&engine, region_id, &column_schemas, 45..50).await;

    listener.wait_begin(5).await; // 4 flush + 1 compaction begin

    // Wait a while to make sure index build tasks are finished.
    listener.wait_stop(5).await; // 4 flush + 1 compaction = some abort + some finish

    common_telemetry::info!("All stopped");

    let scanner = engine
        .scanner(region_id, ScanRequest::default())
        .await
        .unwrap();
    assert_eq!(scanner.num_files(), 1);
    // Index files should be built.
    assert_eq!(num_of_index_files(&engine, &scanner, region_id).await, 1);
}

#[tokio::test]
async fn test_index_build_type_schema_change() {
    let mut env = TestEnv::with_prefix("test_index_build_type_schema_change_").await;
    let listener = Arc::new(IndexBuildListener::default());
    let engine = env
        .create_engine_with(
            async_build_mode_config(true),
            None,
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

    // Create a region without index.
    let request = CreateRequestBuilder::new().build();
    let table_dir = request.table_dir.clone();
    let column_schemas = rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    // Flush and make sure there is no index file.
    put_and_flush(&engine, region_id, &column_schemas, 10..20).await;
    reopen_region(&engine, region_id, table_dir, true, HashMap::new()).await;

    let scanner = engine
        .scanner(region_id, ScanRequest::default())
        .await
        .unwrap();
    assert_eq!(scanner.num_files(), 1);
    assert_eq!(num_of_index_files(&engine, &scanner, region_id).await, 0);

    // Set Index and make sure index file is built without flush or compaction.
    let set_index_request = RegionAlterRequest {
        kind: AlterKind::SetIndexes {
            options: vec![SetIndexOption::Inverted {
                column_name: "tag_0".to_string(),
            }],
        },
    };
    engine
        .handle_request(region_id, RegionRequest::Alter(set_index_request))
        .await
        .unwrap();
    listener.wait_finish(1).await;
    let scanner = engine
        .scanner(region_id, ScanRequest::default())
        .await
        .unwrap();
    assert_eq!(scanner.num_files(), 1);
    assert_eq!(num_of_index_files(&engine, &scanner, region_id).await, 1);
}

/// Tests that a schema change (ALTER SetIndexes) triggers index rebuild
/// for all pre-existing SST files, not just one.  Covers the scenario
/// where multiple SSTs were flushed before the index was defined:
/// 1. Create region without index, flush 3 files.
/// 2. Reopen while flush-triggered no-index builds may still be stopping.
/// 3. Verify 3 SST files and 0 index files.
/// 4. ALTER SetIndexes — triggers rebuild of all 3 inconsistent SSTs.
/// 5. Wait for 3 finishes, then verify 3 SST files + 3 index files.
#[tokio::test]
async fn test_index_build_type_schema_change_multiple_files() {
    let mut env = TestEnv::with_prefix("test_index_build_type_schema_change_multiple_files_").await;
    let listener = Arc::new(IndexBuildListener::default());
    let engine = env
        .create_engine_with(
            async_build_mode_config(true),
            None,
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

    // Create a region without index.
    let request = CreateRequestBuilder::new().build();
    let table_dir = request.table_dir.clone();
    let column_schemas = rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    // Flush 3 SST files without any index defined.
    put_and_flush(&engine, region_id, &column_schemas, 10..20).await;
    put_and_flush(&engine, region_id, &column_schemas, 20..30).await;
    put_and_flush(&engine, region_id, &column_schemas, 30..40).await;

    // Async flush still schedules index builds for flushed SSTs. Since this
    // region has no index metadata yet, those builds are no-ops; if they already
    // stopped, reopening is harmless. Otherwise the subsequent ALTER rebuilds
    // wait behind their active leases.
    reopen_region(&engine, region_id, table_dir, true, HashMap::new()).await;

    let scanner = engine
        .scanner(region_id, ScanRequest::default())
        .await
        .unwrap();
    assert_eq!(scanner.num_files(), 3);
    assert_eq!(num_of_index_files(&engine, &scanner, region_id).await, 0);

    // Set Index via ALTER — triggers schema-change rebuild of all 3 SSTs.
    let set_index_request = RegionAlterRequest {
        kind: AlterKind::SetIndexes {
            options: vec![SetIndexOption::Inverted {
                column_name: "tag_0".to_string(),
            }],
        },
    };
    engine
        .handle_request(region_id, RegionRequest::Alter(set_index_request))
        .await
        .unwrap();

    // Wait for all 3 schema-change rebuilds to finish.
    tokio::time::timeout(std::time::Duration::from_secs(5), listener.wait_finish(3))
        .await
        .unwrap();
    assert_eq!(listener.finish_count(), 3);

    // Verify all 3 SST files now have corresponding index files.
    let scanner = engine
        .scanner(region_id, ScanRequest::default())
        .await
        .unwrap();
    assert_eq!(scanner.num_files(), 3);
    assert_eq!(num_of_index_files(&engine, &scanner, region_id).await, 3);
}

#[tokio::test]
async fn test_consecutive_schema_changes_publish_latest_index_generation() {
    let mut env = TestEnv::with_prefix("test_consecutive_schema_change_index_builds_").await;
    let mut config = async_build_mode_config(false);
    config.max_background_index_builds = 2;
    let gate = Arc::new(IndexPublicationGate::new(
        IndexPublicationPhase::BeforeManifestCommit,
    ));
    let engine = Arc::new(
        env.create_engine_with(config, None, Some(gate.clone()), None)
            .await,
    );

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
    let table_dir = request.table_dir.clone();
    let column_schemas = rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();
    put_and_flush(&engine, region_id, &column_schemas, 0..20).await;

    let set_index = |option| {
        RegionRequest::Alter(RegionAlterRequest {
            kind: AlterKind::SetIndexes {
                options: vec![option],
            },
        })
    };

    engine
        .handle_request(
            region_id,
            set_index(SetIndexOption::Inverted {
                column_name: "tag_0".to_string(),
            }),
        )
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(10), gate.wait_entered(1))
        .await
        .expect("first schema generation did not reach manifest publication");

    engine
        .handle_request(
            region_id,
            set_index(SetIndexOption::Inverted {
                column_name: "field_0".to_string(),
            }),
        )
        .await
        .unwrap();
    engine
        .handle_request(
            region_id,
            set_index(SetIndexOption::Skipping {
                column_name: "field_0".to_string(),
                options: SkippingIndexOptions::new_unchecked(
                    1024,
                    0.01,
                    SkippingIndexType::BloomFilter,
                ),
            }),
        )
        .await
        .unwrap();

    tokio::time::timeout(Duration::from_secs(10), gate.wait_stopped(1))
        .await
        .expect("superseded schema-change build was not coalesced");
    assert_eq!(
        gate.entered.load(Ordering::Relaxed),
        1,
        "the same SST was built concurrently"
    );
    gate.release(1);

    tokio::time::timeout(Duration::from_secs(10), gate.wait_entered(2))
        .await
        .expect("latest schema generation was not scheduled");
    gate.release(1);
    tokio::time::timeout(Duration::from_secs(10), gate.wait_finished(1))
        .await
        .expect("latest schema generation was not published");

    assert_eq!(gate.finish_count.load(Ordering::Relaxed), 1);

    let region = engine.get_region(region_id).unwrap();
    let version = region.version();
    let files = current_file_metas(&engine, region_id).await;
    assert_eq!(files.len(), 1);
    assert!(
        files[0].is_index_consistent_with_region(&version.metadata.column_metadatas),
        "the published index must match the latest schema generation, file: {:?}, metadata: {:?}",
        files[0],
        version.metadata
    );
    assert_eq!(
        region.manifest_ctx.manifest().await.metadata.schema_version,
        version.metadata.schema_version
    );
    let scanner = engine
        .scanner(region_id, ScanRequest::default())
        .await
        .unwrap();
    assert_eq!(num_of_index_files(&engine, &scanner, region_id).await, 1);

    reopen_region(&engine, region_id, table_dir, true, HashMap::new()).await;
    let reopened = engine.get_region(region_id).unwrap();
    let reopened_files = current_file_metas(&engine, region_id).await;
    assert_eq!(reopened_files.len(), 1);
    assert!(
        reopened_files[0]
            .is_index_consistent_with_region(&reopened.version().metadata.column_metadatas)
    );
}

#[tokio::test]
async fn test_unrelated_schema_change_retries_stale_flush_index_build() {
    let mut env =
        TestEnv::with_prefix("test_unrelated_schema_change_retries_stale_index_build_").await;
    let gate = Arc::new(IndexPublicationGate::new(
        IndexPublicationPhase::BeforeManifestCommit,
    ));
    let engine = Arc::new(
        env.create_engine_with(
            async_build_mode_config(true),
            None,
            Some(gate.clone()),
            None,
        )
        .await,
    );

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

    let request = CreateRequestBuilder::new().build_with_index();
    let column_schemas = rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();
    put_and_flush(&engine, region_id, &column_schemas, 0..20).await;
    tokio::time::timeout(Duration::from_secs(10), gate.wait_entered(1))
        .await
        .expect("flush index build did not reach manifest publication");

    engine
        .handle_request(
            region_id,
            RegionRequest::Alter(RegionAlterRequest {
                kind: AlterKind::AddColumns {
                    columns: vec![AddColumn {
                        column_metadata: ColumnMetadata {
                            column_schema: ColumnSchema::new(
                                "field_1",
                                ConcreteDataType::float64_datatype(),
                                true,
                            ),
                            semantic_type: SemanticType::Field,
                            column_id: 3,
                        },
                        location: None,
                    }],
                },
            }),
        )
        .await
        .unwrap();

    gate.release(1);
    tokio::time::timeout(Duration::from_secs(10), gate.wait_entered(2))
        .await
        .expect("stale flush index build was not retried");
    gate.release(1);
    tokio::time::timeout(Duration::from_secs(10), gate.wait_stopped(2))
        .await
        .expect("retried index build did not stop");

    assert_eq!(gate.abort_count.load(Ordering::Relaxed), 1);
    assert_eq!(gate.finish_count.load(Ordering::Relaxed), 1);

    let region = engine.get_region(region_id).unwrap();
    let files = current_file_metas(&engine, region_id).await;
    assert_eq!(files.len(), 1);
    assert!(files[0].is_index_consistent_with_region(&region.version().metadata.column_metadatas));
    let scanner = engine
        .scanner(region_id, ScanRequest::default())
        .await
        .unwrap();
    assert_eq!(num_of_index_files(&engine, &scanner, region_id).await, 1);
}

#[tokio::test]
async fn test_schema_change_uses_manifest_file_generation() {
    let mut env = TestEnv::with_prefix("test_schema_change_uses_manifest_file_generation_").await;
    let mut config = async_build_mode_config(false);
    config.max_background_index_builds = 2;
    let gate = Arc::new(IndexPublicationGate::new(
        IndexPublicationPhase::AfterManifestCommit,
    ));
    let engine = Arc::new(
        env.create_engine_with(config, None, Some(gate.clone()), None)
            .await,
    );

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
    put_and_flush(&engine, region_id, &column_schemas, 0..20).await;

    engine
        .handle_request(
            region_id,
            RegionRequest::Alter(RegionAlterRequest {
                kind: AlterKind::SetIndexes {
                    options: vec![SetIndexOption::Inverted {
                        column_name: "tag_0".to_string(),
                    }],
                },
            }),
        )
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(10), gate.wait_entered(1))
        .await
        .expect("first index generation did not commit");

    // The first publication is committed to the manifest but its worker
    // notification is blocked, so version control still has the previous file
    // metadata when the next schema generation is created.
    engine
        .handle_request(
            region_id,
            RegionRequest::Alter(RegionAlterRequest {
                kind: AlterKind::SetIndexes {
                    options: vec![SetIndexOption::Inverted {
                        column_name: "field_0".to_string(),
                    }],
                },
            }),
        )
        .await
        .unwrap();

    gate.release(1);
    tokio::time::timeout(Duration::from_secs(10), gate.wait_entered(2))
        .await
        .expect("latest schema generation did not commit");
    gate.release(1);
    tokio::time::timeout(Duration::from_secs(10), gate.wait_stopped(2))
        .await
        .expect("index builds did not stop");

    let region = engine.get_region(region_id).unwrap();
    let version = region.version();
    let files = current_file_metas(&engine, region_id).await;
    assert_eq!(files.len(), 1);
    assert!(
        files[0].is_index_consistent_with_region(&version.metadata.column_metadatas),
        "the latest index definition was not published"
    );
}

#[tokio::test]
async fn test_index_build_type_manual_basic() {
    let mut env = TestEnv::with_prefix("test_index_build_type_manual_").await;
    let listener = Arc::new(IndexBuildListener::default());
    let engine = env
        .create_engine_with(
            async_build_mode_config(false), // Disable index file creation on flush.
            None,
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

    // Create a region with index.
    let request = CreateRequestBuilder::new().build_with_index();
    let table_dir = request.table_dir.clone();
    let column_schemas = rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    // Flush and make sure there is no index file (because create_on_flush is disabled).
    put_and_flush(&engine, region_id, &column_schemas, 10..20).await;
    reopen_region(&engine, region_id, table_dir.clone(), true, HashMap::new()).await;
    let scanner = engine
        .scanner(region_id, ScanRequest::default())
        .await
        .unwrap();
    // Index build task is triggered on flush, but not finished.
    assert_listener_counts(&listener, 1, 0);
    assert_eq!(scanner.num_files(), 1);
    assert_eq!(num_of_index_files(&engine, &scanner, region_id).await, 0);

    // Trigger manual index build task and make sure index file is built without flush or compaction.
    let request = RegionRequest::BuildIndex(RegionBuildIndexRequest {});
    engine.handle_request(region_id, request).await.unwrap();
    listener.wait_finish(1).await;
    let scanner = engine
        .scanner(region_id, ScanRequest::default())
        .await
        .unwrap();
    assert_listener_counts(&listener, 2, 1);
    assert_eq!(scanner.num_files(), 1);
    assert_eq!(num_of_index_files(&engine, &scanner, region_id).await, 1);

    // Test idempotency: Second manual index build request on the same file.
    let request = RegionRequest::BuildIndex(RegionBuildIndexRequest {});
    engine.handle_request(region_id, request).await.unwrap();
    reopen_region(&engine, region_id, table_dir.clone(), true, HashMap::new()).await;
    let scanner = engine
        .scanner(region_id, ScanRequest::default())
        .await
        .unwrap();
    // Should still be 2 begin and 1 finish - no new task should be created for already indexed file.
    assert_listener_counts(&listener, 2, 1);
    assert_eq!(scanner.num_files(), 1);
    assert_eq!(num_of_index_files(&engine, &scanner, region_id).await, 1);

    // Test idempotency again: Third manual index build request to further verify.
    let request = RegionRequest::BuildIndex(RegionBuildIndexRequest {});
    engine.handle_request(region_id, request).await.unwrap();
    reopen_region(&engine, region_id, table_dir.clone(), true, HashMap::new()).await;
    let scanner = engine
        .scanner(region_id, ScanRequest::default())
        .await
        .unwrap();
    assert_listener_counts(&listener, 2, 1);
    assert_eq!(scanner.num_files(), 1);
    assert_eq!(num_of_index_files(&engine, &scanner, region_id).await, 1);
}

#[tokio::test]
async fn test_index_build_type_manual_consistency() {
    let mut env = TestEnv::with_prefix("test_index_build_type_manual_consistency_").await;
    let listener = Arc::new(IndexBuildListener::default());
    let engine = env
        .create_engine_with(
            async_build_mode_config(true),
            None,
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

    // Create a region with index.
    let create_request = CreateRequestBuilder::new().build_with_index();
    let table_dir = create_request.table_dir.clone();
    let column_schemas = rows_schema(&create_request);
    engine
        .handle_request(region_id, RegionRequest::Create(create_request.clone()))
        .await
        .unwrap();
    assert_listener_counts(&listener, 0, 0);

    // Flush and make sure index file exists.
    put_and_flush(&engine, region_id, &column_schemas, 10..20).await;
    listener.wait_finish(1).await;
    let scanner = engine
        .scanner(region_id, ScanRequest::default())
        .await
        .unwrap();
    assert_listener_counts(&listener, 1, 1);
    assert_eq!(scanner.num_files(), 1);
    assert_eq!(num_of_index_files(&engine, &scanner, region_id).await, 1);

    // Check index build task for consistent file will be skipped.
    let request = RegionRequest::BuildIndex(RegionBuildIndexRequest {});
    engine.handle_request(region_id, request).await.unwrap();
    // Reopen the region to ensure the task wasn't skipped due to insufficient time.
    reopen_region(&engine, region_id, table_dir.clone(), true, HashMap::new()).await;
    let scanner = engine
        .scanner(region_id, ScanRequest::default())
        .await
        .unwrap();
    // Because the file is consistent, no new index build task is triggered.
    assert_listener_counts(&listener, 1, 1);
    assert_eq!(scanner.num_files(), 1);
    assert_eq!(num_of_index_files(&engine, &scanner, region_id).await, 1);

    let mut altered_metadata = create_request.column_metadatas.clone();
    // Set index for field_0.
    altered_metadata[1].column_schema.set_inverted_index(true);
    let sync_columns_request = RegionAlterRequest {
        kind: AlterKind::SyncColumns {
            column_metadatas: altered_metadata,
        },
    };
    // Use SyncColumns to avoid triggering SchemaChange index build.
    engine
        .handle_request(region_id, RegionRequest::Alter(sync_columns_request))
        .await
        .unwrap();
    reopen_region(&engine, region_id, table_dir, true, HashMap::new()).await;
    // SyncColumns won't trigger index build.
    assert_listener_counts(&listener, 1, 1);

    let request = RegionRequest::BuildIndex(RegionBuildIndexRequest {});
    engine.handle_request(region_id, request).await.unwrap();
    listener.wait_finish(2).await; // previous 1 + new 1
    // Because the file is inconsistent, new index build task is triggered.
    assert_listener_counts(&listener, 2, 2);
}

#[tokio::test]
async fn test_gate_index_build_listener_smoke() {
    use store_api::storage::{FileId, RegionId};

    use crate::engine::listener::{EventListener, GateIndexBuildListener};
    use crate::sst::file::RegionFileId;

    let gate = Arc::new(GateIndexBuildListener::default());

    // Initial counts are zero.
    assert_eq!(gate.begin_count(), 0);
    assert_eq!(gate.finish_count(), 0);
    assert_eq!(gate.abort_count(), 0);

    // Spawn a task that will block in on_index_build_begin.
    let gate_clone = gate.clone();
    let handle = tokio::spawn(async move {
        gate_clone
            .on_index_build_begin(RegionFileId::new(RegionId::new(1, 1), FileId::random()))
            .await;
    });

    // Wait for begin to arrive.
    tokio::time::timeout(std::time::Duration::from_secs(5), gate.wait_begin(1))
        .await
        .unwrap();
    assert_eq!(gate.begin_count(), 1);
    assert_eq!(gate.finish_count(), 0);
    assert_eq!(gate.abort_count(), 0);

    // Release the blocked begin.
    gate.release_begin();

    // The spawned task should now complete.
    tokio::time::timeout(std::time::Duration::from_secs(5), handle)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn test_index_build_type_manual_duplicate_in_flight() {
    let mut env = TestEnv::with_prefix("test_index_build_type_manual_duplicate_in_flight_").await;
    let gate = Arc::new(GateIndexBuildListener::default());
    let engine = Arc::new(
        env.create_engine_with(
            async_build_mode_config(false), // Disable index file creation on flush.
            None,
            Some(gate.clone()),
            None,
        )
        .await,
    );

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

    // Create a region with index metadata.
    let request = CreateRequestBuilder::new().build_with_index();
    let table_dir = request.table_dir.clone();
    let column_schemas = rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    // Flush: the flush-triggered index build begins but is blocked by the gate.
    put_and_flush(&engine, region_id, &column_schemas, 10..20).await;

    // Wait for the flush-triggered build to begin (blocked by gate).
    tokio::time::timeout(std::time::Duration::from_secs(5), gate.wait_begin(1))
        .await
        .unwrap();
    assert_eq!(gate.begin_count(), 1);
    assert_eq!(gate.finish_count(), 0);
    assert_eq!(gate.abort_count(), 0);

    // Release the gate so the flush-triggered build can complete.
    // With create_on_flush=false the build produces no index file (file_size=0),
    // so on_index_build_finish is NOT called.
    gate.release_begin();

    // Reopen the region. If the flush-triggered no-op is still stopping, the
    // manual build below waits behind its active lease.
    reopen_region(&engine, region_id, table_dir.clone(), true, HashMap::new()).await;

    // Verify no index file exists after flush (create_on_flush=false).
    let scanner = engine
        .scanner(region_id, ScanRequest::default())
        .await
        .unwrap();
    assert_eq!(scanner.num_files(), 1);
    assert_eq!(num_of_index_files(&engine, &scanner, region_id).await, 0);

    // Spawn the first manual BuildIndex in background. It will schedule the task,
    // the file enters building_files, and the begin is blocked by the gate.
    // handle_request blocks until the background collector sends the response,
    // so we must spawn it in a separate task.
    let engine_clone = engine.clone();
    let first_handle = tokio::spawn(async move {
        let request = RegionRequest::BuildIndex(RegionBuildIndexRequest {});
        engine_clone.handle_request(region_id, request).await
    });

    // Wait for the first manual build to begin (blocked by gate).
    tokio::time::timeout(std::time::Duration::from_secs(5), gate.wait_begin(2))
        .await
        .unwrap(); // begin 1 = flush, begin 2 = first manual
    assert_eq!(gate.begin_count(), 2);
    assert_eq!(gate.finish_count(), 0);
    assert_eq!(gate.abort_count(), 0);

    // Issue the second manual BuildIndex for the same region/file.
    // Since the file is already in building_files (from the first manual build),
    // schedule_build detects the duplicate and calls on_index_build_abort.
    let request = RegionRequest::BuildIndex(RegionBuildIndexRequest {});
    engine.handle_request(region_id, request).await.unwrap();

    // The second request should have been aborted as duplicate.
    assert_eq!(gate.abort_count(), 1, "duplicate request should be aborted");
    assert_eq!(gate.begin_count(), 2, "no new begin for duplicate");
    assert_eq!(gate.finish_count(), 0, "first build hasn't finished yet");

    // Release the gate to let the first manual build proceed.
    gate.release_begin();
    tokio::time::timeout(std::time::Duration::from_secs(5), gate.wait_finish(1))
        .await
        .unwrap(); // first manual build completes

    // Final counts: only one successful build, one aborted duplicate.
    assert_eq!(gate.begin_count(), 2); // flush + first manual
    assert_eq!(gate.finish_count(), 1); // first manual only
    assert_eq!(gate.abort_count(), 1); // second manual duplicate abort

    // Await the first build's handle_request to ensure it completed cleanly.
    tokio::time::timeout(std::time::Duration::from_secs(5), first_handle)
        .await
        .unwrap()
        .unwrap()
        .unwrap();

    // Verify exactly one SST and one index file.
    let scanner = engine
        .scanner(region_id, ScanRequest::default())
        .await
        .unwrap();
    assert_eq!(scanner.num_files(), 1);
    assert_eq!(num_of_index_files(&engine, &scanner, region_id).await, 1);
}

#[tokio::test]
async fn test_reopen_waits_for_active_index_build_of_previous_incarnation() {
    let mut env =
        TestEnv::with_prefix("test_reopen_waits_for_active_index_build_of_previous_incarnation_")
            .await;
    let gate = Arc::new(GateIndexBuildListener::default());
    let engine = Arc::new(
        env.create_engine_with(
            async_build_mode_config(false),
            None,
            Some(gate.clone()),
            None,
        )
        .await,
    );

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

    let request = CreateRequestBuilder::new().build_with_index();
    let table_dir = request.table_dir.clone();
    let column_schemas = rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();
    put_and_flush(&engine, region_id, &column_schemas, 0..20).await;

    tokio::time::timeout(Duration::from_secs(10), gate.wait_begin(1))
        .await
        .expect("flush index build did not start");
    gate.release_begin();
    reopen_region(&engine, region_id, table_dir.clone(), true, HashMap::new()).await;

    let engine_for_old_build = engine.clone();
    let old_build = tokio::spawn(async move {
        engine_for_old_build
            .handle_request(
                region_id,
                RegionRequest::BuildIndex(RegionBuildIndexRequest {}),
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(10), gate.wait_begin(2))
        .await
        .expect("old index build did not reach the begin gate");

    // Closing the region must not wait for the blocked index build.
    tokio::time::timeout(
        Duration::from_secs(10),
        engine.handle_request(
            region_id,
            RegionRequest::Close(RegionCloseRequest::default()),
        ),
    )
    .await
    .expect("close waited for the active index build")
    .unwrap();
    reopen_region(&engine, region_id, table_dir.clone(), true, HashMap::new()).await;

    let received_before_rebuild = gate.recv_count();
    let aborts_before_rebuild = gate.abort_count();
    let engine_for_new_build = engine.clone();
    let new_build = tokio::spawn(async move {
        engine_for_new_build
            .handle_request(
                region_id,
                RegionRequest::BuildIndex(RegionBuildIndexRequest {}),
            )
            .await
    });
    tokio::time::timeout(
        Duration::from_secs(10),
        gate.wait_recv(received_before_rebuild + 1),
    )
    .await
    .expect("worker did not receive the reopened index build request");

    // The reopened task is queued: it is neither started nor rejected as a
    // duplicate while the old incarnation still owns the SST build lease.
    assert_eq!(gate.begin_count(), 2);
    assert_eq!(gate.abort_count(), aborts_before_rebuild);

    gate.release_begin();
    tokio::time::timeout(Duration::from_secs(10), old_build)
        .await
        .expect("old index build request did not finish")
        .expect("old index build task panicked")
        .unwrap();
    tokio::time::timeout(Duration::from_secs(10), gate.wait_begin(3))
        .await
        .expect("reopened index build did not start after the old build stopped");

    gate.release_begin();
    tokio::time::timeout(Duration::from_secs(10), gate.wait_finish(1))
        .await
        .expect("reopened index build did not finish");
    tokio::time::timeout(Duration::from_secs(10), new_build)
        .await
        .expect("reopened index build request did not finish")
        .expect("reopened index build task panicked")
        .unwrap();

    assert_eq!(gate.begin_count(), 3);
    assert_eq!(gate.finish_count(), 1);
    assert_eq!(gate.abort_count(), aborts_before_rebuild + 1);
    let scanner = engine
        .scanner(region_id, ScanRequest::default())
        .await
        .unwrap();
    assert_eq!(scanner.num_files(), 1);
    assert_eq!(num_of_index_files(&engine, &scanner, region_id).await, 1);

    let before_reopen = scan_timestamps(&engine, region_id).await;
    assert_eq!(before_reopen.len(), 20);
    assert_eq!(
        before_reopen.iter().copied().collect::<HashSet<_>>().len(),
        20,
        "scan contains duplicate rows before reopen"
    );

    reopen_region(&engine, region_id, table_dir, true, HashMap::new()).await;
    assert_eq!(scan_timestamps(&engine, region_id).await, before_reopen);
}

/// Tests the race between an in-flight index build and a compaction that
/// removes the source SST.  The test blocks all flush-triggered index builds
/// via [`GateIndexBuildListener`] so that at least one old SST build is
/// still at `on_index_build_begin` when compaction completes.  After the
/// gate is released, the stale builds find the SSTs gone and abort, while the
/// compaction-triggered build for the new SST succeeds.
///
/// Deterministic orchestration (no sleeps):
/// 1. Flush 3 files — gate blocks all 3 index builds.
/// 2. Flush the 4th file (TWCS trigger_file_num=4) — compaction starts.
/// 3. Wait for 5 begins (4 flush + 1 compaction) — at this point compaction
///    is finished and the old SSTs are removed from the version.
/// 4. Release all 5 blocked begins.
/// 5. Wait for all 5 to stop.
/// 6. Assert: at least one abort, final state = 1 SST + 1 index file.
#[tokio::test]
async fn test_index_build_type_compact_abort_race() {
    common_telemetry::init_default_ut_logging();

    // We must raise max_background_index_builds because the gate blocks all
    // flush-triggered builds at `on_index_build_begin`, causing them to be
    // in "building_files" indefinitely.  The default limit (cpu/8, often ~2-4)
    // would prevent the compaction-triggered build from being scheduled.
    // Setting a generous limit ensures all 5 builds can be scheduled.
    let mut config = async_build_mode_config(true);
    config.max_background_index_builds = 8;

    let mut env = TestEnv::with_prefix("test_index_build_type_compact_abort_race_").await;
    let gate = Arc::new(GateIndexBuildListener::default());
    let engine = env
        .create_engine_with(config, None, Some(gate.clone()), None)
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

    let request = CreateRequestBuilder::new()
        .insert_option("compaction.type", "twcs")
        .insert_option("compaction.twcs.trigger_file_num", "4")
        .build_with_index();

    let column_schemas = rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    // Flush 3 files — all 3 index builds blocked at begin by the gate.
    put_and_flush(&engine, region_id, &column_schemas, 10..20).await;
    put_and_flush(&engine, region_id, &column_schemas, 20..30).await;
    put_and_flush(&engine, region_id, &column_schemas, 35..45).await;

    common_telemetry::info!("After flush 3 files, waiting for begins");

    tokio::time::timeout(std::time::Duration::from_secs(5), gate.wait_begin(3))
        .await
        .unwrap();
    assert_eq!(gate.begin_count(), 3);
    assert_eq!(gate.finish_count(), 0);
    assert_eq!(gate.abort_count(), 0);

    // Flush 4th file — triggers compaction on the TWCS picker.
    put_and_flush(&engine, region_id, &column_schemas, 45..50).await;

    common_telemetry::info!("After flush 4th file, waiting for compaction begin");

    // Wait for 5 begins: 4 flush-triggered + 1 compaction-triggered.
    // The 5th begin indicates compaction has finished and the compacted SST's
    // index build is now blocked at begin. All old SST files have been
    // removed from the version at this point.
    tokio::time::timeout(std::time::Duration::from_secs(5), gate.wait_begin(5))
        .await
        .unwrap();

    common_telemetry::info!("All 5 builds blocked, releasing gates");

    // Release all blocked begins — the old SST builds will see their SSTs
    // are gone and abort; the compaction SST build will succeed.
    for _ in 0..5 {
        gate.release_begin();
    }

    // Wait for all builds to complete (finish or abort).
    tokio::time::timeout(std::time::Duration::from_secs(5), gate.wait_stop(5))
        .await
        .unwrap();

    common_telemetry::info!("All builds stopped, checking results");

    // Verify the compaction race caused the old SST index builds to abort. In
    // this blocked-then-compact scenario, all 4 flush-triggered builds abort
    // (their SST files were removed by compaction) and only the compacted SST
    // build finishes.
    assert_eq!(gate.begin_count(), 5);
    assert_eq!(gate.finish_count(), 1);
    assert_eq!(gate.abort_count(), 4);

    // Final state: all files compacted into 1 SST with 1 index file.
    let scanner = engine
        .scanner(region_id, ScanRequest::default())
        .await
        .unwrap();
    assert_eq!(scanner.num_files(), 1);
    assert_eq!(num_of_index_files(&engine, &scanner, region_id).await, 1);
}
