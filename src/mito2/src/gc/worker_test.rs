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
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use api::v1::Rows;
use async_trait::async_trait;
use bytes::Bytes;
use common_base::Plugins;
use common_telemetry::init_default_ut_logging;
use futures::TryStreamExt;
use object_store::{Entry, ObjectStore};
use store_api::metadata::RegionMetadataRef;
use store_api::region_engine::RegionEngine as _;
use store_api::region_request::{RegionCompactRequest, RegionRequest};
use store_api::storage::{FileId, FileRef, FileRefsManifest, RegionId};

use crate::access_layer::AccessLayerRef;
use crate::config::MitoConfig;
use crate::engine::MitoEngine;
use crate::engine::compaction_test::{delete_and_flush, put_and_flush};
use crate::engine::region_hook::{RegionGcInfo, RegionHook, RegionHookRef};
use crate::error::UnexpectedSnafu;
use crate::gc::{GcConfig, LocalGcWorker};
use crate::manifest::action::RemovedFile;
use crate::metrics::GC_SKIPPED_UNPARSABLE_FILES;
use crate::region::MitoRegionRef;
use crate::test_util::{
    CreateRequestBuilder, TestEnv, build_rows, flush_region, put_rows, rows_schema,
};
use crate::worker::DROPPING_MARKER_FILE;

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
        mito_engine.region_hook(),
    )
    .await
    .unwrap()
}

#[tokio::test]
async fn test_gc_worker_ignores_dropping_marker_file() {
    init_default_ut_logging();

    let mut env = TestEnv::new().await;
    env.log_store = Some(env.create_log_store().await);
    env.object_store_manager = Some(Arc::new(env.create_in_memory_object_store_manager()));

    let engine = env
        .new_mito_engine(MitoConfig {
            gc: GcConfig {
                enable: true,
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
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    let region = engine.get_region(region_id).unwrap();
    let manifest = region.manifest_ctx.manifest().await;
    let marker_path = object_store::util::join_path(
        &region.access_layer.build_region_dir(region_id),
        DROPPING_MARKER_FILE,
    );
    region
        .access_layer
        .object_store()
        .write(&marker_path, Bytes::new())
        .await
        .unwrap();

    let before = GC_SKIPPED_UNPARSABLE_FILES.get();
    let regions = BTreeMap::from([(region_id, Some(region.clone()))]);
    let file_ref_manifest = FileRefsManifest {
        file_refs: Default::default(),
        manifest_version: [(region_id, manifest.manifest_version)].into(),
        cross_region_refs: HashMap::new(),
    };

    let gc_worker = create_gc_worker(&engine, regions, &file_ref_manifest, true).await;
    let report = gc_worker.run().await.unwrap();

    assert!(
        report
            .deleted_files
            .get(&region_id)
            .is_none_or(|files| files.is_empty())
    );
    assert!(report.need_retry_regions.is_empty());
    assert_eq!(GC_SKIPPED_UNPARSABLE_FILES.get(), before);
}

#[tokio::test]
async fn test_fast_gc_respects_parquet_tmp_ref_regardless_of_index_version() {
    let mut env = TestEnv::new().await;
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
    engine
        .handle_request(
            region_id,
            RegionRequest::Create(CreateRequestBuilder::new().build()),
        )
        .await
        .unwrap();

    let region = engine.get_region(region_id).unwrap();
    let gc_worker = create_gc_worker(
        &engine,
        BTreeMap::from([(region_id, Some(region))]),
        &FileRefsManifest::default(),
        false,
    )
    .await;
    let file_id = FileId::random();
    let tmp_refs = HashSet::from([FileRef::new(region_id, file_id, Some(1))]);
    let in_tmp_ref = tmp_refs
        .iter()
        .map(|file_ref| (file_ref.file_id, file_ref.index_version))
        .collect();
    let deletable = gc_worker
        .list_to_be_deleted_files(
            region_id,
            false,
            &HashMap::new(),
            &in_tmp_ref,
            BTreeMap::from([(
                common_time::Timestamp::new_millisecond(0),
                HashSet::from([
                    RemovedFile::File(file_id, Some(2)),
                    RemovedFile::Index(file_id, 1),
                    RemovedFile::Index(file_id, 3),
                ]),
            )]),
            vec![],
        )
        .await
        .unwrap();

    assert_eq!(deletable, vec![RemovedFile::Index(file_id, 3)]);
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
    let removed_file_num = manifest.removed_files.removed_files[0].files.len();
    assert!(removed_file_num > 0);

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
        report.deleted_files.get(&region_id).unwrap().len(),
        removed_file_num
    );
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
    assert!(!manifest.removed_files.removed_files[0].files.is_empty());

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

// --- Tests for unknown-file retention policy ---

/// Helper to write a file to an object store and retrieve its Entry via listing.
async fn write_and_list_entry(store: &ObjectStore, path: &str) -> Entry {
    store
        .write(path, b"dummy_file_content".as_slice())
        .await
        .unwrap();
    let parent = std::path::Path::new(path).parent().and_then(|p| {
        if p.as_os_str().is_empty() {
            None
        } else {
            Some(p)
        }
    });
    let prefix = match parent {
        Some(p) => format!("{}/", p.to_str().unwrap()),
        None => String::new(),
    };
    let entries: Vec<Entry> = store
        .lister_with(&prefix)
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let expected_name = std::path::Path::new(path)
        .file_name()
        .unwrap()
        .to_str()
        .unwrap();
    entries
        .into_iter()
        .find(|entry| entry.name() == expected_name)
        .unwrap_or_else(|| {
            panic!("entry '{expected_name}' not found when listing prefix '{prefix}'")
        })
}

async fn create_full_listing_gc_worker() -> (LocalGcWorker, ObjectStore) {
    let mut env = TestEnv::new().await;
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
    engine
        .handle_request(
            region_id,
            RegionRequest::Create(CreateRequestBuilder::new().build()),
        )
        .await
        .unwrap();

    let region = engine.get_region(region_id).unwrap();
    let store = region.access_layer.object_store().clone();
    let manifest_version = region.manifest_ctx.manifest().await.manifest_version;
    let file_ref_manifest = FileRefsManifest {
        manifest_version: [(region_id, manifest_version)].into(),
        ..Default::default()
    };
    let worker = create_gc_worker(
        &engine,
        BTreeMap::from([(region_id, Some(region))]),
        &file_ref_manifest,
        true,
    )
    .await;
    (worker, store)
}

#[tokio::test]
async fn test_active_unknown_parquet_and_puffin_are_retained() {
    let (worker, store) = create_full_listing_gc_worker().await;
    let region_id = RegionId::new(1, 1);
    let parquet_id = FileId::random();
    let puffin_id = FileId::random();
    let parquet_path = object_store::util::join_path(
        &worker.access_layer.build_region_dir(region_id),
        &format!("{parquet_id}.parquet"),
    );
    let puffin_path = object_store::util::join_path(
        &object_store::util::join_dir(&worker.access_layer.build_region_dir(region_id), "index"),
        &format!("{puffin_id}.1.puffin"),
    );
    write_and_list_entry(&store, &parquet_path).await;
    write_and_list_entry(&store, &puffin_path).await;

    let report = worker.run().await.unwrap();

    assert!(
        report
            .deleted_files
            .get(&region_id)
            .is_none_or(|files| !files.contains(&parquet_id))
    );
    assert!(
        report
            .deleted_indexes
            .get(&region_id)
            .is_none_or(|files| !files.contains(&(puffin_id, 1)))
    );
    assert!(report.need_retry_regions.is_empty());
    assert!(store.exists(&parquet_path).await.unwrap());
    assert!(store.exists(&puffin_path).await.unwrap());
}

#[tokio::test]
async fn test_dropped_unknown_parquet_and_puffin_are_deleted() {
    let (worker, store) = create_full_listing_gc_worker().await;
    let region_id = RegionId::new(1, 1);
    let parquet_id = FileId::random();
    let puffin_id = FileId::random();
    let parquet_path = object_store::util::join_path(
        &worker.access_layer.build_region_dir(region_id),
        &format!("{parquet_id}.parquet"),
    );
    let puffin_path = object_store::util::join_path(
        &object_store::util::join_dir(&worker.access_layer.build_region_dir(region_id), "index"),
        &format!("{puffin_id}.1.puffin"),
    );
    let entries = vec![
        write_and_list_entry(&store, &parquet_path).await,
        write_and_list_entry(&store, &puffin_path).await,
    ];

    let deletable = worker
        .list_to_be_deleted_files(
            region_id,
            true,
            &HashMap::new(),
            &HashSet::new(),
            BTreeMap::new(),
            entries,
        )
        .await
        .unwrap();

    assert_eq!(
        deletable,
        vec![
            RemovedFile::File(parquet_id, None),
            RemovedFile::Index(puffin_id, 1),
        ]
    );
    assert!(store.exists(&parquet_path).await.unwrap());
    assert!(store.exists(&puffin_path).await.unwrap());

    worker.delete_files(region_id, &deletable).await.unwrap();

    assert!(!store.exists(&parquet_path).await.unwrap());
    assert!(!store.exists(&puffin_path).await.unwrap());
}

/// A region hook that records `on_region_gc` calls, for testing the GC hook.
/// `fail_remaining` simulates a transient extension-cleanup failure: while it
/// is greater than zero, `on_region_gc` returns `Err` (after decrementing), so
/// the GC retry path can be exercised.
#[derive(Debug, Default)]
struct GcCountingHook {
    gc_calls: AtomicUsize,
    last_is_region_dropped: AtomicBool,
    fail_remaining: AtomicUsize,
}

#[async_trait]
impl RegionHook for GcCountingHook {
    async fn on_region_gc(
        &self,
        _region_id: RegionId,
        _region_metadata: Option<&RegionMetadataRef>,
        _access_layer: &AccessLayerRef,
        info: &RegionGcInfo<'_>,
    ) -> Result<(), crate::error::Error> {
        self.gc_calls.fetch_add(1, Ordering::Relaxed);
        self.last_is_region_dropped
            .store(info.is_region_dropped, Ordering::Relaxed);
        // Atomically decrement-and-return the previous value; fail while > 0.
        let prev = self.fail_remaining.fetch_sub(1, Ordering::Relaxed);
        if prev > 0 {
            return UnexpectedSnafu {
                reason: "simulated extension cleanup failure".to_string(),
            }
            .fail();
        }
        Ok(())
    }
}

/// `on_region_gc` is **skipped** for a live region in **fast mode** (no full
/// listing) when the GC pass deleted no files — the common case for a periodic
/// pass — to avoid unnecessary hook overhead/I/O. A freshly-flushed region has
/// no deletable files. (A full-listing pass instead always fires the hook; see
/// `test_on_region_gc_fires_on_full_listing_without_deleted_files`.)
#[tokio::test]
async fn test_on_region_gc_skipped_when_no_files_deleted() {
    init_default_ut_logging();

    let mut env = TestEnv::new().await;

    let hook = Arc::new(GcCountingHook::default());
    let plugins = Plugins::new();
    plugins.insert(hook.clone() as RegionHookRef);

    let engine = env
        .create_engine_with_plugins(
            MitoConfig {
                gc: GcConfig {
                    enable: true,
                    lingering_time: None,
                    ..Default::default()
                },
                ..Default::default()
            },
            plugins,
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
        .handle_request(region_id, RegionRequest::Create(request.clone()))
        .await
        .unwrap();
    put_rows(
        &engine,
        region_id,
        Rows {
            schema: rows_schema(&request),
            rows: build_rows(0, 3),
        },
    )
    .await;
    flush_region(&engine, region_id, None).await;

    let region = engine.get_region(region_id).unwrap();
    let version = region.manifest_ctx.manifest().await.manifest_version;
    let regions = BTreeMap::from([(region_id, Some(region.clone()))]);
    let file_ref_manifest = FileRefsManifest {
        file_refs: Default::default(),
        manifest_version: [(region_id, version)].into(),
        cross_region_refs: HashMap::new(),
    };

    let gc_worker = create_gc_worker(&engine, regions, &file_ref_manifest, false).await;
    gc_worker.run().await.unwrap();

    assert_eq!(
        hook.gc_calls.load(Ordering::Relaxed),
        0,
        "on_region_gc must be skipped when a live region's fast-mode GC pass deleted no files"
    );
}

/// `on_region_gc` **fires** (with `is_region_dropped = false`) for a live region
/// when the GC pass actually deleted files. Truncating makes the flushed SST
/// deletable, so the pass reclaims it.
#[tokio::test]
async fn test_on_region_gc_fires_when_live_region_has_deleted_files() {
    init_default_ut_logging();

    let mut env = TestEnv::new().await;

    let hook = Arc::new(GcCountingHook::default());
    let plugins = Plugins::new();
    plugins.insert(hook.clone() as RegionHookRef);

    let engine = env
        .create_engine_with_plugins(
            MitoConfig {
                gc: GcConfig {
                    enable: true,
                    lingering_time: None,
                    ..Default::default()
                },
                ..Default::default()
            },
            plugins,
        )
        .await;

    let region_id = RegionId::new(1, 2);
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
        .handle_request(region_id, RegionRequest::Create(request.clone()))
        .await
        .unwrap();
    put_rows(
        &engine,
        region_id,
        Rows {
            schema: rows_schema(&request),
            rows: build_rows(0, 3),
        },
    )
    .await;
    flush_region(&engine, region_id, None).await;

    // Truncate so the flushed SST becomes a removed file the next GC pass reclaims
    // (`lingering_time` is None -> immediately deletable).
    engine
        .handle_request(
            region_id,
            RegionRequest::Truncate(store_api::region_request::RegionTruncateRequest::All),
        )
        .await
        .unwrap();

    let region = engine.get_region(region_id).unwrap();
    let version = region.manifest_ctx.manifest().await.manifest_version;
    let regions = BTreeMap::from([(region_id, Some(region.clone()))]);
    let file_ref_manifest = FileRefsManifest {
        file_refs: Default::default(),
        manifest_version: [(region_id, version)].into(),
        cross_region_refs: HashMap::new(),
    };

    let gc_worker = create_gc_worker(&engine, regions, &file_ref_manifest, true).await;
    let report = gc_worker.run().await.unwrap();
    assert!(
        report
            .deleted_files
            .get(&region_id)
            .map(|v| !v.is_empty())
            .unwrap_or(false),
        "precondition: the GC pass should have deleted the truncated SST"
    );

    assert!(
        hook.gc_calls.load(Ordering::Relaxed) >= 1,
        "on_region_gc should fire when a live region's GC pass deleted files"
    );
    assert!(
        !hook.last_is_region_dropped.load(Ordering::Relaxed),
        "a live region's GC pass must report is_region_dropped=false"
    );
}

/// `on_region_gc` fires with `is_region_dropped = true` on the global-GC
/// reclamation path for a dropped/absent region — e.g. a repartitioned-away
/// source region whose mito2 files global GC reclaims. Mirrors
/// [`test_on_region_gc_fires_on_gc_pass`] but drives GC with the region absent
/// (`region: None`), the state `do_region_gc` reports as dropped. This is the
/// path extensions rely on to clean up a deallocated region's sidecar files.
#[tokio::test]
async fn test_on_region_gc_fires_for_dropped_region() {
    init_default_ut_logging();

    let mut env = TestEnv::new().await;

    let hook = Arc::new(GcCountingHook::default());
    let plugins = Plugins::new();
    plugins.insert(hook.clone() as RegionHookRef);

    let engine = env
        .create_engine_with_plugins(
            MitoConfig {
                gc: GcConfig {
                    enable: true,
                    lingering_time: None,
                    ..Default::default()
                },
                ..Default::default()
            },
            plugins,
        )
        .await;

    let region_id = RegionId::new(2, 1);
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
        .handle_request(region_id, RegionRequest::Create(request.clone()))
        .await
        .unwrap();
    put_rows(
        &engine,
        region_id,
        Rows {
            schema: rows_schema(&request),
            rows: build_rows(0, 3),
        },
    )
    .await;
    flush_region(&engine, region_id, None).await;

    // Grab the access layer + manifest version while the region is live, then
    // treat the region as dropped (absent) for GC — the global reclamation path
    // where `do_region_gc` sees `region: None` and sets `is_region_dropped`.
    let region = engine.get_region(region_id).unwrap();
    let access_layer = region.access_layer.clone();
    let version = region.manifest_ctx.manifest().await.manifest_version;
    let regions = BTreeMap::from([(region_id, None)]); // dropped/absent
    let file_ref_manifest = FileRefsManifest {
        file_refs: Default::default(),
        manifest_version: [(region_id, version)].into(),
        cross_region_refs: HashMap::new(),
    };

    let gc_worker = LocalGcWorker::try_new(
        access_layer,
        Some(engine.cache_manager()),
        regions,
        engine.mito_config().gc.clone(),
        file_ref_manifest,
        &engine.gc_limiter(),
        true, // full_file_listing is required when the region is absent
        engine.region_hook(),
    )
    .await
    .unwrap();
    gc_worker.run().await.unwrap();

    assert!(
        hook.gc_calls.load(Ordering::Relaxed) >= 1,
        "on_region_gc should fire after a GC pass for a dropped region"
    );
    assert!(
        hook.last_is_region_dropped.load(Ordering::Relaxed),
        "a dropped region's GC pass must report is_region_dropped=true"
    );
}

/// `on_region_gc` **fires** on a full-listing pass even when mito itself deleted
/// no files, so an extension can reconcile sidecar-only orphans (failed writes,
/// leftovers from a previous cleanup). Contrast with the fast-mode skip in
/// `test_on_region_gc_skipped_when_no_files_deleted`.
#[tokio::test]
async fn test_on_region_gc_fires_on_full_listing_without_deleted_files() {
    init_default_ut_logging();

    let mut env = TestEnv::new().await;

    let hook = Arc::new(GcCountingHook::default());
    let plugins = Plugins::new();
    plugins.insert(hook.clone() as RegionHookRef);

    let engine = env
        .create_engine_with_plugins(
            MitoConfig {
                gc: GcConfig {
                    enable: true,
                    lingering_time: None,
                    ..Default::default()
                },
                ..Default::default()
            },
            plugins,
        )
        .await;

    let region_id = RegionId::new(1, 3);
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
        .handle_request(region_id, RegionRequest::Create(request.clone()))
        .await
        .unwrap();
    put_rows(
        &engine,
        region_id,
        Rows {
            schema: rows_schema(&request),
            rows: build_rows(0, 3),
        },
    )
    .await;
    flush_region(&engine, region_id, None).await;

    // No truncate: there are no removed files to reclaim this pass.
    let region = engine.get_region(region_id).unwrap();
    let version = region.manifest_ctx.manifest().await.manifest_version;
    let regions = BTreeMap::from([(region_id, Some(region.clone()))]);
    let file_ref_manifest = FileRefsManifest {
        file_refs: Default::default(),
        manifest_version: [(region_id, version)].into(),
        cross_region_refs: HashMap::new(),
    };

    let gc_worker = create_gc_worker(&engine, regions, &file_ref_manifest, true).await;
    gc_worker.run().await.unwrap();

    assert!(
        hook.gc_calls.load(Ordering::Relaxed) >= 1,
        "on_region_gc must fire on a full-listing pass even when no files were deleted"
    );
    assert!(
        !hook.last_is_region_dropped.load(Ordering::Relaxed),
        "a live region's GC pass must report is_region_dropped=false"
    );
}

/// A failing `on_region_gc` must keep the region un-acknowledged: it lands in
/// `need_retry_regions` and is excluded from `processed_regions` / `deleted_files`,
/// so metasrv does not treat it as fully cleaned and the next GC pass replays
/// the callback.
#[tokio::test]
async fn test_on_region_gc_failure_routes_region_to_retry() {
    init_default_ut_logging();

    let mut env = TestEnv::new().await;

    // Fail the first (and only, in this test) extension cleanup.
    let hook = Arc::new(GcCountingHook {
        fail_remaining: AtomicUsize::new(1),
        ..Default::default()
    });
    let plugins = Plugins::new();
    plugins.insert(hook.clone() as RegionHookRef);

    let engine = env
        .create_engine_with_plugins(
            MitoConfig {
                gc: GcConfig {
                    enable: true,
                    lingering_time: None,
                    ..Default::default()
                },
                ..Default::default()
            },
            plugins,
        )
        .await;

    let region_id = RegionId::new(1, 4);
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
        .handle_request(region_id, RegionRequest::Create(request.clone()))
        .await
        .unwrap();
    put_rows(
        &engine,
        region_id,
        Rows {
            schema: rows_schema(&request),
            rows: build_rows(0, 3),
        },
    )
    .await;
    flush_region(&engine, region_id, None).await;
    // Truncate so the flushed SST becomes deletable.
    engine
        .handle_request(
            region_id,
            RegionRequest::Truncate(store_api::region_request::RegionTruncateRequest::All),
        )
        .await
        .unwrap();

    let region = engine.get_region(region_id).unwrap();
    let version = region.manifest_ctx.manifest().await.manifest_version;
    let regions = BTreeMap::from([(region_id, Some(region.clone()))]);
    let file_ref_manifest = FileRefsManifest {
        file_refs: Default::default(),
        manifest_version: [(region_id, version)].into(),
        cross_region_refs: HashMap::new(),
    };

    let gc_worker = create_gc_worker(&engine, regions, &file_ref_manifest, true).await;
    let report = gc_worker.run().await.unwrap();

    assert!(
        hook.gc_calls.load(Ordering::Relaxed) >= 1,
        "on_region_gc should fire when the GC pass deleted files"
    );
    assert!(
        report.need_retry_regions.contains(&region_id),
        "a failed extension cleanup must keep the region in need_retry_regions"
    );
    assert!(
        !report.processed_regions.contains(&region_id),
        "a failed extension cleanup must not acknowledge the region as processed"
    );
    assert!(
        !report.deleted_files.contains_key(&region_id),
        "a failed extension cleanup must not report deleted_files for the region"
    );
}
