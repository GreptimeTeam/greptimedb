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

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use api::v1::Rows;
use async_trait::async_trait;
use common_base::Plugins;
use common_recordbatch::RecordBatches;
use common_telemetry::init_default_ut_logging;
use common_time::Timestamp;
use futures::TryStreamExt;
use object_store::layers::mock::{
    Error as MockError, ErrorKind, MockLayerBuilder, OpDelete, Result as ObjectStoreResult, oio,
};
use object_store::{Entry, ObjectStore, services};
use store_api::metadata::RegionMetadataRef;
use store_api::region_engine::RegionEngine as _;
use store_api::region_request::{
    PathType, RegionCompactRequest, RegionFlushRequest, RegionRequest,
};
use store_api::storage::{FileId, FileRef, FileRefsManifest, RegionId, ScanRequest};
use tokio::sync::Semaphore;

use crate::cache::file_cache::FileType;
use crate::config::MitoConfig;
use crate::engine::MitoEngine;
use crate::engine::compaction_test::{delete_and_flush, put_and_flush};
use crate::engine::region_hook::{RegionHook, RegionHookRef, SstFileInfo};
use crate::gc::{
    GcConfig, LocalGcWorker, ManifestLiveSet, TestFinalizationBarrier, TmpRefLiveSet,
    should_delete_file,
};
use crate::manifest::action::{RegionManifest, RemovedFile};
use crate::region::MitoRegionRef;
use crate::sst::file::{FileMeta, IndexType, RegionFileId};
use crate::sst::location;
use crate::test_util::{
    CreateRequestBuilder, TestEnv, build_rows, flush_region, put_rows, rows_schema,
};

const RACE_TIMEOUT: Duration = Duration::from_secs(10);

async fn wait_for_permit(semaphore: &Semaphore, step: &str) {
    tokio::time::timeout(RACE_TIMEOUT, semaphore.acquire())
        .await
        .unwrap_or_else(|_| panic!("timed out waiting for {step}"))
        .expect("test semaphore must remain open")
        .forget();
}

#[derive(Debug)]
struct PauseAfterSstWritten {
    reached: Arc<Semaphore>,
    release: Arc<Semaphore>,
    file_id: Mutex<Option<FileId>>,
    paused: AtomicBool,
}

impl PauseAfterSstWritten {
    fn new() -> Self {
        Self {
            reached: Arc::new(Semaphore::new(0)),
            release: Arc::new(Semaphore::new(0)),
            file_id: Mutex::new(None),
            paused: AtomicBool::new(false),
        }
    }

    async fn wait_until_written(&self) -> FileId {
        wait_for_permit(&self.reached, "SST-written hook").await;
        self.file_id
            .lock()
            .unwrap()
            .expect("SST-written hook must record a file id")
    }

    fn release(&self) {
        self.release.add_permits(1);
    }
}

#[async_trait]
impl RegionHook for PauseAfterSstWritten {
    async fn on_sst_files_written(
        &self,
        _region_id: RegionId,
        _region_metadata: &RegionMetadataRef,
        files: &[SstFileInfo<'_>],
    ) {
        if self.paused.swap(true, Ordering::SeqCst) {
            return;
        }
        assert_eq!(1, files.len(), "the race tests write one Parquet SST");
        *self.file_id.lock().unwrap() = Some(files[0].file_meta.file_id);
        self.reached.add_permits(1);
        wait_for_permit(&self.release, "release of SST-written hook").await;
    }
}

struct FailCloseAfterParquetDelete {
    inner: oio::Deleter,
    deleted_parquet: bool,
    close_attempted: Arc<Semaphore>,
}

impl oio::Delete for FailCloseAfterParquetDelete {
    async fn delete(&mut self, path: &str, args: OpDelete) -> ObjectStoreResult<()> {
        if path.ends_with(".parquet") {
            // Model a batch delete whose close fails before the target request is committed.
            self.deleted_parquet = true;
            Ok(())
        } else {
            self.inner.delete(path, args).await
        }
    }

    async fn close(&mut self) -> ObjectStoreResult<()> {
        if self.deleted_parquet {
            self.close_attempted.add_permits(1);
            return Err(MockError::new(
                ErrorKind::Unexpected,
                "injected target Parquet batch-delete close failure",
            ));
        }
        self.inner.close().await
    }
}

struct SignalAfterParquetDelete {
    inner: oio::Deleter,
    deleted_parquet: bool,
    completed: Arc<Semaphore>,
}

impl oio::Delete for SignalAfterParquetDelete {
    async fn delete(&mut self, path: &str, args: OpDelete) -> ObjectStoreResult<()> {
        self.deleted_parquet |= path.ends_with(".parquet");
        self.inner.delete(path, args).await
    }

    async fn close(&mut self) -> ObjectStoreResult<()> {
        let result = self.inner.close().await;
        if result.is_ok() && self.deleted_parquet {
            self.completed.add_permits(1);
        }
        result
    }
}

fn gc_config_with_zero_unknown_ttl() -> MitoConfig {
    MitoConfig {
        gc: GcConfig {
            enable: true,
            lingering_time: None,
            unknown_file_lingering_time: Duration::ZERO,
            ..Default::default()
        },
        ..Default::default()
    }
}

fn file_refs_manifest(region_id: RegionId, manifest: &RegionManifest) -> FileRefsManifest {
    FileRefsManifest {
        file_refs: Default::default(),
        manifest_version: [(region_id, manifest.manifest_version)].into(),
        cross_region_refs: HashMap::new(),
    }
}

fn version_contains_file(region: &MitoRegionRef, file_id: FileId) -> bool {
    region
        .version_control
        .current()
        .version
        .ssts
        .levels()
        .iter()
        .flat_map(|level| level.files())
        .any(|file| file.file_id().file_id() == file_id)
}

async fn assert_rows_readable(engine: &MitoEngine, region_id: RegionId) {
    let stream = engine
        .scan_to_stream(region_id, ScanRequest::default())
        .await
        .unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    assert_eq!(
        3,
        batches.iter().map(|batch| batch.num_rows()).sum::<usize>()
    );
}

fn parquet_path(table_dir: &str, region_id: RegionId, file_id: FileId) -> String {
    location::sst_file_path(
        table_dir,
        RegionFileId::new(region_id, file_id),
        PathType::Bare,
    )
}

async fn entry_at_path(store: &ObjectStore, path: &str) -> Entry {
    let parent = std::path::Path::new(path)
        .parent()
        .and_then(|parent| (!parent.as_os_str().is_empty()).then_some(parent));
    let prefix = parent
        .map(|parent| format!("{}/", parent.to_str().unwrap()))
        .unwrap_or_default();
    let expected_name = std::path::Path::new(path)
        .file_name()
        .unwrap()
        .to_str()
        .unwrap();
    let lister = store.lister_with(&prefix).await.unwrap();
    lister
        .try_collect::<Vec<_>>()
        .await
        .unwrap()
        .into_iter()
        .find(|entry| entry.name() == expected_name)
        .unwrap_or_else(|| panic!("entry '{path}' not found when listing '{prefix}'"))
}

async fn wait_until_gc_cutoff_is_after_file_mtime(store: &ObjectStore, path: &str) {
    let entry = entry_at_path(store, path).await;
    let mtime_ms = entry
        .metadata()
        .last_modified()
        .expect("FS object store must provide the target SST last-modified time")
        .into_inner()
        .as_millisecond();

    tokio::time::timeout(RACE_TIMEOUT, async {
        loop {
            // `should_delete_file` compares these exact millisecond values with strict `<`.
            if mtime_ms < chrono::Utc::now().timestamp_millis() {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "GC cutoff did not become strictly later than target SST mtime {mtime_ms} within {RACE_TIMEOUT:?}"
        )
    });
}

fn assert_target_is_candidate(candidates: &[RemovedFile], file_id: FileId) {
    assert!(
        candidates
            .iter()
            .any(|candidate| candidate.file_id() == file_id),
        "GC candidates do not contain target SST {file_id}: {candidates:?}"
    );
}

fn indexed_file_meta(region_id: RegionId, file_id: FileId, index_version: u64) -> FileMeta {
    let mut meta = FileMeta {
        region_id,
        file_id,
        index_version,
        ..Default::default()
    };
    meta.available_indexes.push(IndexType::InvertedIndex);
    meta
}

#[tokio::test]
async fn test_manifest_live_set_protects_normal_and_staging_index_versions() {
    let mut env = TestEnv::new().await;
    let engine = env.create_engine(MitoConfig::default()).await;
    let region_id = RegionId::new(1, 1);
    engine
        .handle_request(
            region_id,
            RegionRequest::Create(CreateRequestBuilder::new().build()),
        )
        .await
        .unwrap();
    let region = engine.get_region(region_id).unwrap();
    let file_id = FileId::random();
    let mut normal = (*region.manifest_ctx.manifest().await).clone();
    normal
        .files
        .insert(file_id, indexed_file_meta(region_id, file_id, 1));
    let mut staging = normal.clone();
    staging
        .files
        .insert(file_id, indexed_file_meta(region_id, file_id, 2));
    let live_set = ManifestLiveSet::from_manifests(&normal, Some(&staging));

    assert!(live_set.contains_file(file_id, FileType::Puffin(1)));
    assert!(live_set.contains_file(file_id, FileType::Puffin(2)));
    assert!(!live_set.contains_file(file_id, FileType::Puffin(3)));
    assert!(live_set.contains_file(file_id, FileType::Parquet));
}

#[test]
fn test_tmp_ref_live_set_protects_parquet_and_matching_puffin_version() {
    let file_id = FileId::random();
    let no_index_live_set = TmpRefLiveSet::from(&HashSet::from([(file_id, None)]));
    assert!(no_index_live_set.contains_file(file_id, FileType::Parquet));
    assert!(!no_index_live_set.contains_file(file_id, FileType::Puffin(1)));

    let indexed_live_set = TmpRefLiveSet::from(&HashSet::from([(file_id, Some(1))]));

    assert!(indexed_live_set.contains_file(file_id, FileType::Parquet));
    assert!(indexed_live_set.contains_file(file_id, FileType::Puffin(1)));
    assert!(!indexed_live_set.contains_file(file_id, FileType::Puffin(2)));
}

#[tokio::test]
async fn test_fast_gc_tmp_ref_filtering_protects_parquet_and_matching_puffin() {
    let mut env = TestEnv::new().await;
    let engine = env
        .create_engine(MitoConfig {
            gc: GcConfig {
                lingering_time: None,
                ..Default::default()
            },
            ..Default::default()
        })
        .await;
    let region_id = RegionId::new(1, 1);
    engine
        .handle_request(
            region_id,
            RegionRequest::Create(CreateRequestBuilder::new().build()),
        )
        .await
        .unwrap();
    let region = engine.get_region(region_id).unwrap();
    let manifest = region.manifest_ctx.manifest().await;
    let worker = create_gc_worker(
        &engine,
        BTreeMap::from([(region_id, Some(region))]),
        &file_refs_manifest(region_id, &manifest),
        false,
    )
    .await;
    let file_id = FileId::random();
    let removed_files = HashSet::from([
        RemovedFile::File(file_id, None),
        RemovedFile::Index(file_id, 1),
        RemovedFile::Index(file_id, 2),
    ]);
    let deletable_files = worker
        .list_to_be_deleted_files(
            region_id,
            false,
            &ManifestLiveSet::default(),
            &TmpRefLiveSet::from(&HashSet::from([(file_id, Some(1))])),
            BTreeMap::from([(Timestamp::new_millisecond(0), removed_files)]),
            vec![],
        )
        .unwrap();

    assert_eq!(deletable_files, vec![RemovedFile::Index(file_id, 2)]);
}

async fn create_gc_worker(
    mito_engine: &MitoEngine,
    regions: BTreeMap<RegionId, Option<MitoRegionRef>>,
    file_ref_manifest: &FileRefsManifest,
    full_file_listing: bool,
) -> LocalGcWorker {
    let access_layer = regions
        .first_key_value()
        .as_ref()
        .unwrap()
        .1
        .as_ref()
        .unwrap()
        .access_layer
        .clone();
    let cache_manager = mito_engine.cache_manager();

    LocalGcWorker::try_new(
        access_layer,
        Some(cache_manager),
        regions,
        mito_engine.mito_config().gc.clone(),
        file_ref_manifest.clone(),
        &mito_engine.gc_limiter(),
        full_file_listing,
    )
    .await
    .unwrap()
}

/// Test insert/flush then truncate can allow gc worker to delete files
#[tokio::test]
async fn test_gc_worker_basic_truncate() {
    init_default_ut_logging();

    let mut env = TestEnv::new().await;
    env.log_store = Some(env.create_log_store().await);
    // use in memory object store for gc test, so it will use `ObjectStoreFilePurger`
    env.object_store_manager = Some(Arc::new(env.create_in_memory_object_store_manager()));

    let engine = env
        .new_mito_engine(MitoConfig {
            gc: GcConfig {
                enable: true,
                // for faster delete file
                lingering_time: None,
                ..Default::default()
            },
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
        .handle_request(region_id, RegionRequest::Create(request.clone()))
        .await
        .unwrap();

    let rows = Rows {
        schema: column_schemas.clone(),
        rows: build_rows(0, 3),
    };
    put_rows(&engine, region_id, rows).await;

    flush_region(&engine, region_id, None).await;

    let region = engine.get_region(region_id).unwrap();
    let manifest = region.manifest_ctx.manifest().await;

    let to_be_deleted_file_id = *manifest.files.iter().next().unwrap().0;

    assert_eq!(manifest.files.len(), 1);

    engine
        .handle_request(
            region.region_id,
            RegionRequest::Truncate(store_api::region_request::RegionTruncateRequest::All),
        )
        .await
        .unwrap();

    let manifest = region.manifest_ctx.manifest().await;
    assert!(
        manifest.removed_files.removed_files[0]
            .files
            .contains(&RemovedFile::File(to_be_deleted_file_id, None))
            && manifest.removed_files.removed_files[0].files.len() == 1
            && manifest.files.is_empty(),
        "Manifest after truncate: {:?}",
        manifest
    );
    let version = manifest.manifest_version;

    let regions = BTreeMap::from([(region_id, Some(region.clone()))]);
    let file_ref_manifest = FileRefsManifest {
        file_refs: Default::default(),
        manifest_version: [(region_id, version)].into(),
        cross_region_refs: HashMap::new(),
    };
    let gc_worker = create_gc_worker(&engine, regions, &file_ref_manifest, true).await;
    let report = gc_worker.run().await.unwrap();
    assert_eq!(
        report.deleted_files.get(&region_id).unwrap(),
        &vec![to_be_deleted_file_id],
    );
    assert!(report.need_retry_regions.is_empty());

    let manifest = region.manifest_ctx.manifest().await;
    assert!(manifest.removed_files.removed_files.is_empty() && manifest.files.is_empty());
}

#[tokio::test]
async fn test_gc_worker_manifest_version_mismatch_needs_retry() {
    let mut env = TestEnv::new().await;
    let engine = env.create_engine(MitoConfig::default()).await;

    let region_id = RegionId::new(1, 1);
    engine
        .handle_request(
            region_id,
            RegionRequest::Create(CreateRequestBuilder::new().build()),
        )
        .await
        .unwrap();
    let region = engine.get_region(region_id).unwrap();
    let manifest = region.manifest_ctx.manifest().await;
    let file_ref_manifest = FileRefsManifest {
        file_refs: Default::default(),
        manifest_version: [(region_id, manifest.manifest_version + 1)].into(),
        cross_region_refs: HashMap::new(),
    };

    let report = create_gc_worker(
        &engine,
        BTreeMap::from([(region_id, Some(region))]),
        &file_ref_manifest,
        true,
    )
    .await
    .run()
    .await
    .unwrap();

    assert_eq!(report.need_retry_regions, HashSet::from([region_id]));
    assert!(!report.processed_regions.contains(&region_id));
    assert!(!report.deleted_files.contains_key(&region_id));
    assert!(!report.deleted_indexes.contains_key(&region_id));
}

/// Truncate with file refs should not delete files
#[tokio::test]
async fn test_gc_worker_truncate_with_ref() {
    init_default_ut_logging();

    let mut env = TestEnv::new().await;
    env.log_store = Some(env.create_log_store().await);
    // use in memory object store for gc test, so it will use `ObjectStoreFilePurger`
    env.object_store_manager = Some(Arc::new(env.create_in_memory_object_store_manager()));

    let engine = env
        .new_mito_engine(MitoConfig {
            gc: GcConfig {
                enable: true,
                // for faster delete file
                lingering_time: None,
                ..Default::default()
            },
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
        .handle_request(region_id, RegionRequest::Create(request.clone()))
        .await
        .unwrap();

    let rows = Rows {
        schema: column_schemas.clone(),
        rows: build_rows(0, 3),
    };
    put_rows(&engine, region_id, rows).await;

    flush_region(&engine, region_id, None).await;

    let region = engine.get_region(region_id).unwrap();
    let manifest = region.manifest_ctx.manifest().await;

    assert_eq!(manifest.files.len(), 1);

    let to_be_deleted_file_id = *manifest.files.iter().next().unwrap().0;

    engine
        .handle_request(
            region.region_id,
            RegionRequest::Truncate(store_api::region_request::RegionTruncateRequest::All),
        )
        .await
        .unwrap();

    let manifest = region.manifest_ctx.manifest().await;
    assert!(
        manifest.removed_files.removed_files[0]
            .files
            .contains(&RemovedFile::File(to_be_deleted_file_id, None))
            && manifest.removed_files.removed_files[0].files.len() == 1
            && manifest.files.is_empty(),
        "Manifest after truncate: {:?}",
        manifest
    );
    let version = manifest.manifest_version;

    let regions = BTreeMap::from([(region_id, Some(region.clone()))]);
    let file_ref_manifest = FileRefsManifest {
        file_refs: [(
            region_id,
            HashSet::from([FileRef::new(region_id, to_be_deleted_file_id, None)]),
        )]
        .into(),
        manifest_version: [(region_id, version)].into(),
        cross_region_refs: HashMap::new(),
    };
    let gc_worker = create_gc_worker(&engine, regions, &file_ref_manifest, true).await;
    let report = gc_worker.run().await.unwrap();
    assert!(report.deleted_files.get(&region_id).unwrap().is_empty());
    assert!(report.need_retry_regions.is_empty());

    let manifest = region.manifest_ctx.manifest().await;
    assert!(
        manifest.removed_files.removed_files[0].files.len() == 1 && manifest.files.is_empty(),
        "Manifest: {:?}",
        manifest
    );
}

/// Test insert/flush then compact can allow gc worker to delete files
#[tokio::test]
async fn test_gc_worker_basic_compact() {
    init_default_ut_logging();

    let mut env = TestEnv::new().await;
    env.log_store = Some(env.create_log_store().await);
    // use in memory object store for gc test, so it will use `ObjectStoreFilePurger`
    env.object_store_manager = Some(Arc::new(env.create_in_memory_object_store_manager()));

    let engine = env
        .new_mito_engine(MitoConfig {
            gc: GcConfig {
                enable: true,
                // for faster delete file
                lingering_time: None,
                ..Default::default()
            },
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
        .handle_request(region_id, RegionRequest::Create(request.clone()))
        .await
        .unwrap();

    put_and_flush(&engine, region_id, &column_schemas, 0..10).await;
    put_and_flush(&engine, region_id, &column_schemas, 10..20).await;
    put_and_flush(&engine, region_id, &column_schemas, 20..30).await;
    delete_and_flush(&engine, region_id, &column_schemas, 15..30).await;
    put_and_flush(&engine, region_id, &column_schemas, 15..25).await;

    let result = engine
        .handle_request(
            region_id,
            RegionRequest::Compact(RegionCompactRequest::default()),
        )
        .await
        .unwrap();
    assert_eq!(result.affected_rows, 0);

    let region = engine.get_region(region_id).unwrap();
    let manifest = region.manifest_ctx.manifest().await;
    assert_eq!(manifest.removed_files.removed_files[0].files.len(), 3);

    let version = manifest.manifest_version;

    let regions = BTreeMap::from([(region_id, Some(region.clone()))]);
    let file_ref_manifest = FileRefsManifest {
        file_refs: Default::default(),
        manifest_version: [(region_id, version)].into(),
        cross_region_refs: HashMap::new(),
    };

    let gc_worker = create_gc_worker(&engine, regions, &file_ref_manifest, true).await;
    let report = gc_worker.run().await.unwrap();

    assert_eq!(report.deleted_files.get(&region_id).unwrap().len(), 3,);
    assert!(report.need_retry_regions.is_empty());
}

/// Compact with file refs should not delete files
#[tokio::test]
async fn test_gc_worker_compact_with_ref() {
    init_default_ut_logging();

    let mut env = TestEnv::new().await;
    env.log_store = Some(env.create_log_store().await);
    // use in memory object store for gc test, so it will use `ObjectStoreFilePurger`
    env.object_store_manager = Some(Arc::new(env.create_in_memory_object_store_manager()));

    let engine = env
        .new_mito_engine(MitoConfig {
            gc: GcConfig {
                enable: true,
                // for faster delete file
                lingering_time: None,
                ..Default::default()
            },
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
        .handle_request(region_id, RegionRequest::Create(request.clone()))
        .await
        .unwrap();

    put_and_flush(&engine, region_id, &column_schemas, 0..10).await;
    put_and_flush(&engine, region_id, &column_schemas, 10..20).await;
    put_and_flush(&engine, region_id, &column_schemas, 20..30).await;
    delete_and_flush(&engine, region_id, &column_schemas, 15..30).await;
    put_and_flush(&engine, region_id, &column_schemas, 15..25).await;

    let result = engine
        .handle_request(
            region_id,
            RegionRequest::Compact(RegionCompactRequest::default()),
        )
        .await
        .unwrap();
    assert_eq!(result.affected_rows, 0);

    let region = engine.get_region(region_id).unwrap();
    let manifest = region.manifest_ctx.manifest().await;
    assert_eq!(manifest.removed_files.removed_files[0].files.len(), 3);

    let version = manifest.manifest_version;

    let regions = BTreeMap::from([(region_id, Some(region.clone()))]);
    let file_ref_manifest = FileRefsManifest {
        file_refs: HashMap::from([(
            region_id,
            manifest.removed_files.removed_files[0]
                .files
                .iter()
                .map(|removed_file| match removed_file {
                    RemovedFile::File(file_id, v) => FileRef::new(region_id, *file_id, *v),
                    RemovedFile::Index(file_id, v) => FileRef::new(region_id, *file_id, Some(*v)),
                })
                .collect(),
        )]),
        manifest_version: [(region_id, version)].into(),
        cross_region_refs: HashMap::new(),
    };

    let gc_worker = create_gc_worker(&engine, regions, &file_ref_manifest, true).await;
    let report = gc_worker.run().await.unwrap();

    assert_eq!(report.deleted_files.get(&region_id).unwrap().len(), 0);
    assert!(report.need_retry_regions.is_empty());
}

#[tokio::test]
async fn test_full_gc_does_not_delete_sst_published_before_finalization() {
    init_default_ut_logging();

    let mock_layer = MockLayerBuilder::default().build().unwrap();
    let mut env = TestEnv::new().await.with_mock_layer(mock_layer);
    let hook = Arc::new(PauseAfterSstWritten::new());
    let plugins = Plugins::new();
    plugins.insert(hook.clone() as RegionHookRef);
    let engine = env
        .create_engine_with_plugins(gc_config_with_zero_unknown_ttl(), plugins)
        .await;

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new().build();
    let table_dir = request.table_dir.clone();
    let column_schemas = rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();
    put_rows(
        &engine,
        region_id,
        Rows {
            schema: column_schemas,
            rows: build_rows(0, 3),
        },
    )
    .await;

    let region = engine.get_region(region_id).unwrap();
    let manifest_before_flush = region.manifest_ctx.manifest().await;
    let flush_engine = engine.clone();
    let flush = tokio::spawn(async move {
        flush_engine
            .handle_request(
                region_id,
                RegionRequest::Flush(RegionFlushRequest::default()),
            )
            .await
    });
    let file_id = hook.wait_until_written().await;
    let path = parquet_path(&table_dir, region_id, file_id);
    wait_until_gc_cutoff_is_after_file_mtime(region.access_layer.object_store(), &path).await;

    let barrier = TestFinalizationBarrier::new();
    let gc_worker = create_gc_worker(
        &engine,
        BTreeMap::from([(region_id, Some(region.clone()))]),
        &file_refs_manifest(region_id, &manifest_before_flush),
        true,
    )
    .await
    .with_test_finalization_barrier(barrier.clone());
    let gc = tokio::spawn(async move { gc_worker.run().await });
    let candidates = barrier.wait_until_reached().await;
    assert_target_is_candidate(&candidates, file_id);

    hook.release();
    flush.await.unwrap().unwrap();

    let manifest = region.manifest_ctx.manifest().await;
    assert!(manifest.files.contains_key(&file_id));
    assert!(version_contains_file(&region, file_id));

    barrier.release();
    let report = gc.await.unwrap().unwrap();
    assert!(report.need_retry_regions.contains(&region_id));
    assert!(
        !report
            .deleted_files
            .get(&region_id)
            .is_some_and(|files| files.contains(&file_id)),
        "GC must not report a published SST as deleted"
    );

    let object = region.access_layer.object_store().read(&path).await;
    assert!(object.is_ok(), "published SST must remain readable: {path}");
    assert_rows_readable(&engine, region_id).await;
}

#[tokio::test]
async fn test_full_gc_delete_before_publication_fails_closed_and_flush_can_retry() {
    init_default_ut_logging();

    let delete_completed = Arc::new(Semaphore::new(0));
    let completed = delete_completed.clone();
    let mock_layer = MockLayerBuilder::default()
        .deleter_factory(Arc::new(move |inner| {
            Box::new(SignalAfterParquetDelete {
                inner,
                deleted_parquet: false,
                completed: completed.clone(),
            })
        }))
        .build()
        .unwrap();
    let mut env = TestEnv::new().await.with_mock_layer(mock_layer);
    let hook = Arc::new(PauseAfterSstWritten::new());
    let plugins = Plugins::new();
    plugins.insert(hook.clone() as RegionHookRef);
    let engine = env
        .create_engine_with_plugins(gc_config_with_zero_unknown_ttl(), plugins)
        .await;

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new().build();
    let table_dir = request.table_dir.clone();
    let column_schemas = rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();
    put_rows(
        &engine,
        region_id,
        Rows {
            schema: column_schemas,
            rows: build_rows(0, 3),
        },
    )
    .await;

    let region = engine.get_region(region_id).unwrap();
    let manifest_before_flush = region.manifest_ctx.manifest().await;
    let flush_engine = engine.clone();
    let flush = tokio::spawn(async move {
        flush_engine
            .handle_request(
                region_id,
                RegionRequest::Flush(RegionFlushRequest::default()),
            )
            .await
    });
    let file_id = hook.wait_until_written().await;
    let path = parquet_path(&table_dir, region_id, file_id);
    wait_until_gc_cutoff_is_after_file_mtime(region.access_layer.object_store(), &path).await;

    let barrier = TestFinalizationBarrier::new();
    let gc_worker = create_gc_worker(
        &engine,
        BTreeMap::from([(region_id, Some(region.clone()))]),
        &file_refs_manifest(region_id, &manifest_before_flush),
        true,
    )
    .await
    .with_test_finalization_barrier(barrier.clone());
    let gc = tokio::spawn(async move { gc_worker.run().await });
    let candidates = barrier.wait_until_reached().await;
    assert_target_is_candidate(&candidates, file_id);
    barrier.release();
    wait_for_permit(&delete_completed, "target Parquet delete completion").await;
    assert!(
        region
            .access_layer
            .object_store()
            .read(&path)
            .await
            .is_err(),
        "target SST must be absent before releasing its publisher"
    );

    hook.release();
    let flush_result = flush.await.unwrap();
    let report = gc.await.unwrap().unwrap();
    assert!(
        report
            .deleted_files
            .get(&region_id)
            .is_some_and(|files| files.contains(&file_id)),
        "GC report must contain the physically deleted target SST"
    );
    assert!(
        flush_result.is_err(),
        "a flush must not publish an SST that GC already deleted"
    );

    let manifest = region.manifest_ctx.manifest().await;
    assert!(!manifest.files.contains_key(&file_id));
    assert!(!version_contains_file(&region, file_id));
    assert_rows_readable(&engine, region_id).await;

    engine
        .handle_request(
            region_id,
            RegionRequest::Flush(RegionFlushRequest::default()),
        )
        .await
        .unwrap();
    assert_rows_readable(&engine, region_id).await;
}

#[tokio::test]
async fn test_full_gc_delete_error_does_not_block_publisher_recovery() {
    init_default_ut_logging();

    let close_attempted = Arc::new(Semaphore::new(0));
    let attempted = close_attempted.clone();
    let mock_layer = MockLayerBuilder::default()
        .deleter_factory(Arc::new(move |inner| {
            Box::new(FailCloseAfterParquetDelete {
                inner,
                deleted_parquet: false,
                close_attempted: attempted.clone(),
            })
        }))
        .build()
        .unwrap();
    let mut env = TestEnv::new().await.with_mock_layer(mock_layer);
    let hook = Arc::new(PauseAfterSstWritten::new());
    let plugins = Plugins::new();
    plugins.insert(hook.clone() as RegionHookRef);
    let engine = env
        .create_engine_with_plugins(gc_config_with_zero_unknown_ttl(), plugins)
        .await;

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new().build();
    let table_dir = request.table_dir.clone();
    let column_schemas = rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();
    put_rows(
        &engine,
        region_id,
        Rows {
            schema: column_schemas,
            rows: build_rows(0, 3),
        },
    )
    .await;

    let region = engine.get_region(region_id).unwrap();
    let manifest_before_flush = region.manifest_ctx.manifest().await;
    let flush_engine = engine.clone();
    let flush = tokio::spawn(async move {
        flush_engine
            .handle_request(
                region_id,
                RegionRequest::Flush(RegionFlushRequest::default()),
            )
            .await
    });
    let file_id = hook.wait_until_written().await;
    let path = parquet_path(&table_dir, region_id, file_id);
    wait_until_gc_cutoff_is_after_file_mtime(region.access_layer.object_store(), &path).await;

    let barrier = TestFinalizationBarrier::new();
    let gc_worker = create_gc_worker(
        &engine,
        BTreeMap::from([(region_id, Some(region.clone()))]),
        &file_refs_manifest(region_id, &manifest_before_flush),
        true,
    )
    .await
    .with_test_finalization_barrier(barrier.clone());
    let gc = tokio::spawn(async move { gc_worker.run().await });
    let candidates = barrier.wait_until_reached().await;
    assert_target_is_candidate(&candidates, file_id);
    barrier.release();
    wait_for_permit(&close_attempted, "injected Parquet delete close failure").await;
    let gc_result = tokio::time::timeout(RACE_TIMEOUT, gc)
        .await
        .expect("timed out waiting for GC after injected delete failure")
        .unwrap();
    assert!(
        gc_result.is_err(),
        "GC must surface the injected delete failure"
    );

    hook.release();
    let flush_result = tokio::time::timeout(RACE_TIMEOUT, flush)
        .await
        .expect("timed out waiting for publisher after GC delete failure")
        .unwrap();
    assert!(
        flush_result.is_ok(),
        "publisher must continue after the failed GC delete is resolved"
    );
    assert!(
        region
            .manifest_ctx
            .manifest()
            .await
            .files
            .contains_key(&file_id)
    );
    assert!(version_contains_file(&region, file_id));
    assert!(
        region.access_layer.object_store().read(&path).await.is_ok(),
        "failed batch delete must leave the target SST readable"
    );
}

// --- Tests for unknown_file_lingering_time TTL logic ---

/// Helper to write a dummy parquet file to an in-memory object store and
/// retrieve its Entry via listing.
async fn write_and_list_entry(store: &ObjectStore, path: &str) -> Entry {
    store
        .write(path, b"dummy_parquet_content".as_slice())
        .await
        .unwrap();
    // List the parent directory to get the entry.
    let parent = std::path::Path::new(path).parent().and_then(|p| {
        if p.as_os_str().is_empty() {
            None
        } else {
            Some(p)
        }
    });
    let prefix = match parent {
        Some(p) => format!("{}/", p.to_str().unwrap()),
        None => String::new(), // root
    };
    let lister = store.lister_with(&prefix).await.unwrap();
    let entries: Vec<Entry> = lister.try_collect().await.unwrap();
    let expected_name = std::path::Path::new(path)
        .file_name()
        .unwrap()
        .to_str()
        .unwrap();
    match entries.iter().find(|e| e.name() == expected_name) {
        Some(e) => e.clone(),
        None => {
            panic!(
                "entry '{}' not found when listing prefix '{}'; entries: {:?}",
                expected_name,
                prefix,
                entries.iter().map(|e| e.name()).collect::<Vec<_>>()
            )
        }
    }
}

/// Test: active/open region unknown file within TTL (last_modified newer than
/// threshold) should NOT be deleted.
/// NOTE: Memory backend leaves `last_modified` as `None`, so this test also
/// covers the "missing last_modified → keep" conservative behavior.
#[tokio::test]
async fn test_unknown_file_within_ttl_not_deleted() {
    let builder = services::Memory::default();
    let store = ObjectStore::new(builder).unwrap().finish();

    let entry = write_and_list_entry(&store, "test/1.parquet").await;

    // unknown_file_may_linger_until set to epoch (very old) → file is "too young"
    // since last_modified is ~now.
    let threshold = chrono::DateTime::from_timestamp(0, 0).unwrap();

    let should_delete = should_delete_file(
        false, // not in manifest
        false, // not in tmp_ref
        false, // not in may_linger
        false, // not eligible for delete
        false, // active region (not dropped)
        &entry, threshold,
    );
    assert!(
        !should_delete,
        "Active-region unknown file within TTL should NOT be deleted"
    );
}

/// Test: active/open region unknown file exceeding TTL (last_modified older
/// than threshold) should be deleted.
#[tokio::test]
async fn test_unknown_file_exceeded_ttl_deleted() {
    // Use Fs backend so that last_modified is properly set on file metadata.
    let tmp_dir = common_test_util::temp_dir::create_temp_dir("gc_unknown_ttl");
    let root = tmp_dir.path().to_string_lossy().to_string();
    let builder = services::Fs::default().root(&root);
    let store = ObjectStore::new(builder).unwrap().finish();

    let entry = write_and_list_entry(&store, "2.parquet").await;

    // threshold set far into the future → any file with last_modified before now
    // is guaranteed to be older than the threshold.
    let threshold = chrono::Utc::now() + chrono::Duration::days(1);

    let should_delete = should_delete_file(
        false, // not in manifest
        false, // not in tmp_ref
        false, // not in may_linger
        false, // not eligible for delete
        false, // active region (not dropped)
        &entry, threshold,
    );
    assert!(
        should_delete,
        "Active-region unknown file exceeding TTL should be deleted"
    );
}

/// Test: dropped region unknown file should always be deleted regardless of TTL.
#[tokio::test]
async fn test_unknown_file_dropped_region_deleted() {
    let builder = services::Memory::default();
    let store = ObjectStore::new(builder).unwrap().finish();

    let entry = write_and_list_entry(&store, "test/3.parquet").await;

    // Even with threshold far in the past (epoch), dropped region deletes immediately.
    let threshold = chrono::DateTime::from_timestamp(0, 0).unwrap();

    let should_delete = should_delete_file(
        false, // not in manifest
        false, // not in tmp_ref
        false, // not in may_linger
        false, // not eligible for delete
        true,  // region dropped
        &entry, threshold,
    );
    assert!(
        should_delete,
        "Dropped region unknown file should be deleted immediately"
    );
}

/// Test: file in manifest should NOT be deleted even if unknown.
#[tokio::test]
async fn test_file_in_manifest_not_deleted() {
    let builder = services::Memory::default();
    let store = ObjectStore::new(builder).unwrap().finish();

    let entry = write_and_list_entry(&store, "test/4.parquet").await;
    let threshold = chrono::Utc::now() + chrono::Duration::days(1);

    let should_delete = should_delete_file(
        true,  // in manifest
        false, // not in tmp_ref
        false, false, false, // active region
        &entry, threshold,
    );
    assert!(!should_delete, "File in manifest should NOT be deleted");
}

/// Test: file in tmp_ref should NOT be deleted even if unknown.
#[tokio::test]
async fn test_file_in_tmp_ref_not_deleted() {
    let builder = services::Memory::default();
    let store = ObjectStore::new(builder).unwrap().finish();

    let entry = write_and_list_entry(&store, "test/5.parquet").await;
    let threshold = chrono::Utc::now() + chrono::Duration::days(1);

    let should_delete = should_delete_file(
        false, true, // in tmp_ref
        false, false, false, // active region
        &entry, threshold,
    );
    assert!(!should_delete, "File in tmp_ref should NOT be deleted");
}

/// Test: known removed file that is still lingering should NOT be deleted.
/// (is_linger=true but is_eligible_for_delete=false)
#[tokio::test]
async fn test_known_file_still_lingering_not_deleted() {
    let builder = services::Memory::default();
    let store = ObjectStore::new(builder).unwrap().finish();

    let entry = write_and_list_entry(&store, "test/6.parquet").await;
    let threshold = chrono::Utc::now() + chrono::Duration::days(1);

    let should_delete = should_delete_file(
        false, false, true,  // is_linger
        false, // not yet eligible for delete
        false, &entry, threshold,
    );
    assert!(
        !should_delete,
        "Known file still in lingering period should NOT be deleted"
    );
}

/// Test: known removed file eligible for delete should be deleted.
/// (is_linger=true and is_eligible_for_delete=true)
#[tokio::test]
async fn test_known_file_eligible_for_delete_deleted() {
    let builder = services::Memory::default();
    let store = ObjectStore::new(builder).unwrap().finish();

    let entry = write_and_list_entry(&store, "test/7.parquet").await;
    let threshold = chrono::DateTime::from_timestamp(0, 0).unwrap();

    let should_delete = should_delete_file(
        false, false, true, // is_linger
        true, // eligible for delete
        false, &entry, threshold,
    );
    assert!(
        should_delete,
        "Known file eligible for delete should be deleted"
    );
}

// --- Tests for boundary conditions and explicit "keep" semantics ---

/// Test: when `last_modified` equals the cutoff exactly, the file should be
/// kept (strict `<` comparison, not `<=`).
#[tokio::test]
async fn test_unknown_file_at_cutoff_not_deleted() {
    // Use Fs backend for real last_modified
    let tmp_dir = common_test_util::temp_dir::create_temp_dir("gc_at_cutoff");
    let root = tmp_dir.path().to_string_lossy().to_string();
    let builder = services::Fs::default().root(&root);
    let store = ObjectStore::new(builder).unwrap().finish();

    let entry = write_and_list_entry(&store, "cutoff.parquet").await;

    // Set threshold to the actual last_modified → should NOT be deleted (strict <)
    let actual_mtime_millis = entry
        .metadata()
        .last_modified()
        .map(|ts| ts.into_inner().as_millisecond())
        .expect("Fs backend must provide last_modified");
    // Construct a chrono::DateTime at exactly the same millisecond
    let threshold = chrono::DateTime::from_timestamp_millis(actual_mtime_millis).unwrap();

    let should_delete = should_delete_file(
        false, // not in manifest
        false, // not in tmp_ref
        false, // not in may_linger
        false, // not eligible for delete
        false, // active region
        &entry, threshold,
    );
    assert!(
        !should_delete,
        "File at exact cutoff (last_modified == threshold) should NOT be deleted (strict < comparison)"
    );
}

/// Test: active unknown file with missing `last_modified` (e.g. object store
/// that does not provide the timestamp) should be conservatively kept.
#[tokio::test]
async fn test_missing_last_modified_unknown_kept() {
    // Memory backend does NOT set last_modified
    let builder = services::Memory::default();
    let store = ObjectStore::new(builder).unwrap().finish();

    let entry = write_and_list_entry(&store, "test/missing_mtime.parquet").await;
    // Verify that last_modified is indeed None
    assert!(
        entry.metadata().last_modified().is_none(),
        "Memory backend must not provide last_modified for this test"
    );

    // threshold in the far past → should still NOT delete
    let threshold = chrono::DateTime::from_timestamp(0, 0).unwrap();

    let should_delete = should_delete_file(
        false, false, false, false, // active unknown
        false, // active region
        &entry, threshold,
    );
    assert!(
        !should_delete,
        "Missing last_modified should keep the file (conservative behavior)"
    );
}

/// Test: file in manifest should NOT be deleted even when its object
/// `last_modified` is very old (far below the TTL cutoff).
#[tokio::test]
async fn test_file_in_manifest_old_mtime_kept() {
    let tmp_dir = common_test_util::temp_dir::create_temp_dir("gc_manifest_old");
    let root = tmp_dir.path().to_string_lossy().to_string();
    let builder = services::Fs::default().root(&root);
    let store = ObjectStore::new(builder).unwrap().finish();

    let entry = write_and_list_entry(&store, "in_manifest.parquet").await;
    // threshold far in the future → mtime is definitely old, but manifest protects
    let threshold = chrono::Utc::now() + chrono::Duration::days(1);

    let should_delete = should_delete_file(
        true,  // in manifest
        false, // not in tmp_ref
        false, false, false, // not linger/eligible, active
        &entry, threshold,
    );
    assert!(
        !should_delete,
        "File in manifest should NOT be deleted even with old last-modified time"
    );
}

/// Test: file in tmp_ref should NOT be deleted even when its object
/// `last_modified` is very old (far below the TTL cutoff).
#[tokio::test]
async fn test_file_in_tmp_ref_old_mtime_kept() {
    let tmp_dir = common_test_util::temp_dir::create_temp_dir("gc_tmpref_old");
    let root = tmp_dir.path().to_string_lossy().to_string();
    let builder = services::Fs::default().root(&root);
    let store = ObjectStore::new(builder).unwrap().finish();

    let entry = write_and_list_entry(&store, "in_tmp_ref.parquet").await;
    // threshold far in the future → mtime is definitely old, but tmp_ref protects
    let threshold = chrono::Utc::now() + chrono::Duration::days(1);

    let should_delete = should_delete_file(
        false, true, // in tmp_ref
        false, false, false, // not linger/eligible, active
        &entry, threshold,
    );
    assert!(
        !should_delete,
        "File in tmp_ref should NOT be deleted even with old last-modified time"
    );
}
