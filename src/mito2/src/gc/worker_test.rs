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

use api::v1::Rows;
use common_telemetry::init_default_ut_logging;
use common_time::Timestamp;
use futures::TryStreamExt;
use object_store::{Entry, ObjectStore, services};
use store_api::region_engine::RegionEngine as _;
use store_api::region_request::{RegionCompactRequest, RegionRequest};
use store_api::storage::{FileId, FileRef, FileRefsManifest, IndexVersion, RegionId};

use crate::config::MitoConfig;
use crate::engine::MitoEngine;
use crate::engine::compaction_test::{delete_and_flush, put_and_flush};
use crate::gc::{
    GcConfig, LocalGcWorker, filter_deletable_files, list_to_be_deleted_files_impl,
    should_delete_file,
};
use crate::manifest::action::RemovedFile;
use crate::region::MitoRegionRef;
use crate::test_util::{
    CreateRequestBuilder, TestEnv, build_rows, flush_region, put_rows, rows_schema,
};

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

// --- Integration-style tests exercising filter_deletable_files / list_to_be_deleted_files_impl ---

/// Helper: write a dummy parquet file with a standard UUID-based FileId path
/// to the given object store, and return its entry via listing.
async fn write_entry_with_file_id(store: &ObjectStore, file_id: FileId, subdir: &str) -> Entry {
    let path = format!("{}/{}.parquet", subdir, file_id);
    write_and_list_entry(store, &path).await
}

/// Test: full listing with active region — unknown file with TTL NOT exceeded
/// → `filter_deletable_files` should NOT return it for deletion.
#[tokio::test]
async fn test_full_listing_active_unknown_within_ttl() {
    let builder = services::Memory::default();
    let store = ObjectStore::new(builder).unwrap().finish();
    let file_id = FileId::random();

    let entry = write_entry_with_file_id(&store, file_id, "test").await;

    // threshold at epoch → file's last_modified (None for Memory) → not deleted
    let threshold = chrono::DateTime::from_timestamp(0, 0).unwrap();

    let in_manifest: HashMap<FileId, Option<IndexVersion>> = Default::default();
    let in_tmp_ref: HashSet<(FileId, Option<IndexVersion>)> = Default::default();
    let may_linger: HashSet<&RemovedFile> = Default::default();
    let eligible: HashSet<&RemovedFile> = Default::default();

    let result = filter_deletable_files(
        false, // active region
        vec![entry],
        &in_manifest,
        &in_tmp_ref,
        &may_linger,
        &eligible,
        threshold,
    );

    assert!(
        result.is_empty(),
        "Active unknown file within TTL should not be returned for deletion"
    );
}

/// Test: full listing with active region — unknown file with TTL exceeded
/// → `filter_deletable_files` should return `RemovedFile::File(file_id, None)`.
#[tokio::test]
async fn test_full_listing_active_unknown_exceeded_ttl() {
    // Use Fs backend for real last_modified
    let tmp_dir = common_test_util::temp_dir::create_temp_dir("gc_list_ttl2");
    let root = tmp_dir.path().to_string_lossy().to_string();
    let builder = services::Fs::default().root(&root);
    let store = ObjectStore::new(builder).unwrap().finish();
    let file_id = FileId::random();

    let entry = write_entry_with_file_id(&store, file_id, "").await;

    // threshold far in future → definitely exceeded
    let threshold = chrono::Utc::now() + chrono::Duration::days(1);

    let in_manifest: HashMap<FileId, Option<IndexVersion>> = Default::default();
    let in_tmp_ref: HashSet<(FileId, Option<IndexVersion>)> = Default::default();
    let may_linger: HashSet<&RemovedFile> = Default::default();
    let eligible: HashSet<&RemovedFile> = Default::default();

    let result = filter_deletable_files(
        false, // active region
        vec![entry],
        &in_manifest,
        &in_tmp_ref,
        &may_linger,
        &eligible,
        threshold,
    );

    assert_eq!(result.len(), 1);
    assert_eq!(
        result[0],
        RemovedFile::File(file_id, None),
        "Active unknown file exceeding TTL should be returned as RemovedFile::File"
    );
}

/// Test: fast mode (`full_file_listing=false`) does NOT process unknown files.
/// Even with an unknown entry, fast mode only looks at `eligible_for_removal`.
#[tokio::test]
async fn test_fast_mode_does_not_process_unknown() {
    let builder = services::Memory::default();
    let store = ObjectStore::new(builder).unwrap().finish();
    let file_id = FileId::random();
    let entry = write_entry_with_file_id(&store, file_id, "test").await;

    let opt = GcConfig {
        enable: true,
        lingering_time: None, // no lingering: known files eligible immediately
        unknown_file_lingering_time: std::time::Duration::ZERO, // unknown would be eligible
        ..Default::default()
    };

    let in_manifest: HashMap<FileId, Option<IndexVersion>> = Default::default();
    let in_tmp_ref: HashSet<(FileId, Option<IndexVersion>)> = Default::default();
    let recently_removed: BTreeMap<Timestamp, HashSet<RemovedFile>> = Default::default();

    // fast mode — entries are present but should be ignored
    let result = list_to_be_deleted_files_impl(
        &opt,
        false, // full_file_listing = false → fast mode
        RegionId::new(1, 1),
        false, // active region
        &in_manifest,
        &in_tmp_ref,
        recently_removed,
        vec![entry],
    )
    .await
    .unwrap();

    assert!(
        result.is_empty(),
        "Fast mode should not process unknown files; got {:?}",
        result
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
