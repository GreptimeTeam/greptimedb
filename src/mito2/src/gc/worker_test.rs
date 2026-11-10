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
use common_time::Timestamp;
use futures::TryStreamExt;
use object_store::{Entry, ObjectStore, services};
use store_api::metadata::RegionMetadataRef;
use store_api::region_engine::{RegionEngine as _, RegionRole};
use store_api::region_request::{RegionCompactRequest, RegionRequest};
use store_api::storage::{FileId, FileRef, FileRefsManifest, IndexVersion, RegionId};

use crate::access_layer::AccessLayerRef;
use crate::config::MitoConfig;
use crate::engine::MitoEngine;
use crate::engine::compaction_test::{delete_and_flush, put_and_flush};
use crate::engine::region_hook::{RegionGcInfo, RegionHook, RegionHookRef};
use crate::error::UnexpectedSnafu;
use crate::gc::{GcConfig, LocalGcWorker, should_delete_file};
use crate::manifest::action::{RegionEdit, RegionMetaAction, RegionMetaActionList, RemovedFile};
use crate::metrics::GC_SKIPPED_UNPARSABLE_FILES;
use crate::region::{MitoRegionRef, RegionLeaderState};
use crate::sst::file::{ColumnIndexMetadata, FileHandle, FileMeta, IndexType, RegionIndexId};
use crate::sst::location;
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

struct IndexGcTestEnv {
    engine: MitoEngine,
    region: MitoRegionRef,
    worker: LocalGcWorker,
    meta: FileMeta,
    entries: Vec<Entry>,
    removed_files: BTreeMap<Timestamp, HashSet<RemovedFile>>,
    _env: TestEnv,
}

impl IndexGcTestEnv {
    async fn new(full_file_listing: bool) -> Self {
        let mut env = TestEnv::new().await;
        let engine = env
            .create_engine(MitoConfig {
                gc: GcConfig {
                    enable: true,
                    lingering_time: Some(std::time::Duration::from_secs(60 * 60)),
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
        let meta = FileMeta {
            region_id,
            file_id: FileId::random(),
            available_indexes: smallvec::smallvec![IndexType::InvertedIndex],
            ..Default::default()
        };
        let layer = &region.access_layer;
        let paths = [
            location::sst_file_path(layer.table_dir(), meta.file_id(), layer.path_type()),
            location::index_file_path(layer.table_dir(), meta.index_id(), layer.path_type()),
            location::index_file_path(
                layer.table_dir(),
                RegionIndexId::new(meta.file_id(), 1),
                layer.path_type(),
            ),
        ];
        let mut entries = Vec::new();
        for path in paths {
            entries.push(write_and_list_entry(layer.object_store(), &path).await);
        }
        let removed_files = BTreeMap::from([(
            Timestamp::new_millisecond(0),
            HashSet::from([
                RemovedFile::File(meta.file_id, Some(1)),
                RemovedFile::Index(meta.file_id, 0),
                RemovedFile::Index(meta.file_id, 1),
            ]),
        )]);
        let worker = create_gc_worker(
            &engine,
            BTreeMap::from([(region_id, Some(region.clone()))]),
            &FileRefsManifest::default(),
            full_file_listing,
        )
        .await;
        Self {
            engine,
            region,
            worker,
            meta,
            entries,
            removed_files,
            _env: env,
        }
    }
}

#[rstest::rstest]
#[tokio::test]
async fn test_gc_keeps_sst_across_index_versions(
    #[values(false, true)] full_file_listing: bool,
    #[values(false, true)] old_reader_last: bool,
) {
    use crate::sst::version::SstVersion;

    let env = IndexGcTestEnv::new(full_file_listing).await;
    let meta = &env.meta;
    let region_id = meta.region_id;
    let mut version = SstVersion::new(crate::test_util::memtable_util::metadata_for_test());
    version.add_files(env.region.file_purger.clone(), [meta.clone()].into_iter());
    let old = version.levels()[0].files[&meta.file_id].clone();
    let updated = FileMeta {
        index_version: 1,
        ..meta.clone()
    };
    version.add_files(env.region.file_purger.clone(), [updated].into_iter());
    let new = version.levels()[0].files[&meta.file_id].clone();
    version.mark_all_deleted();
    drop(version);

    let (first, last) = if old_reader_last {
        (new, old)
    } else {
        (old, new)
    };
    let released_version = first.index_id().version;
    let live_version = last.index_id().version;
    let mut handles = [Some(first), Some(last)];
    for released in 0..=2 {
        if released > 0 {
            drop(handles[released - 1].take());
        }
        let refs = env
            .engine
            .get_snapshot_of_file_refs([region_id], HashMap::new())
            .await
            .unwrap();
        let refs = refs.file_refs.get(&region_id).cloned().unwrap_or_default();
        let expected_versions = match released {
            0 => vec![0, 1],
            1 => vec![live_version],
            _ => vec![],
        };
        assert_eq!(
            refs,
            expected_versions
                .into_iter()
                .map(|v| FileRef::new(region_id, meta.file_id, Some(v)))
                .collect()
        );
        let tmp_refs = refs
            .into_iter()
            .map(|f| (f.file_id, f.index_version))
            .collect();
        let (deleted, _deferred_unknown) = env
            .worker
            .list_to_be_deleted_files(
                region_id,
                &HashMap::new(),
                &tmp_refs,
                env.removed_files.clone(),
                env.entries.clone(),
            )
            .await
            .unwrap();
        let deleted: HashSet<_> = deleted.into_iter().collect();
        let expected = match released {
            0 => HashSet::new(),
            1 => HashSet::from([RemovedFile::Index(meta.file_id, released_version)]),
            _ => HashSet::from([
                RemovedFile::File(meta.file_id, if full_file_listing { None } else { Some(1) }),
                RemovedFile::Index(meta.file_id, 0),
                RemovedFile::Index(meta.file_id, 1),
            ]),
        };
        assert_eq!(deleted, expected);
    }
}

#[rstest::rstest]
#[case::manifest(true, Some(3))]
#[case::no_index_reader(false, None)]
#[case::old_index_reader(false, Some(0))]
#[case::other_index_reader(false, Some(3))]
#[tokio::test]
async fn test_gc_reference_protects_parquet_and_only_its_index(
    #[values(false, true)] full_file_listing: bool,
    #[case] in_manifest: bool,
    #[case] index_version: Option<IndexVersion>,
) {
    let env = IndexGcTestEnv::new(full_file_listing).await;
    let file_id = env.meta.file_id;
    let (manifest_refs, tmp_refs) = if in_manifest {
        (HashMap::from([(file_id, index_version)]), HashSet::new())
    } else {
        (HashMap::new(), HashSet::from([(file_id, index_version)]))
    };
    let (deleted, _deferred_unknown) = env
        .worker
        .list_to_be_deleted_files(
            env.meta.region_id,
            &manifest_refs,
            &tmp_refs,
            env.removed_files.clone(),
            env.entries.clone(),
        )
        .await
        .unwrap();
    let expected: HashSet<_> = [0, 1]
        .into_iter()
        .filter(|version| Some(*version) != index_version)
        .map(|version| RemovedFile::Index(file_id, version))
        .collect();
    assert_eq!(deleted.into_iter().collect::<HashSet<_>>(), expected);
}

#[rstest::rstest]
#[tokio::test]
async fn test_gc_reclaims_obsolete_index_after_readers_and_linger_release(
    #[values(false, true)] full_file_listing: bool,
    #[values(false, true)] linger_expired: bool,
    #[values(false, true)] column_only: bool,
) {
    let mut env = IndexGcTestEnv::new(full_file_listing).await;
    if column_only {
        env.meta.available_indexes.clear();
        env.meta.indexes = vec![ColumnIndexMetadata {
            column_id: 0,
            created_indexes: smallvec::smallvec![IndexType::HllIndex],
            ..Default::default()
        }];
    }
    let region_id = env.meta.region_id;
    env.engine
        .set_region_role(region_id, RegionRole::Leader)
        .unwrap();
    let old_reader = FileHandle::new(env.meta.clone(), env.region.file_purger.clone());
    let current_meta = FileMeta {
        index_version: 1,
        ..env.meta.clone()
    };
    let current_reader = old_reader.with_updated_index(current_meta.clone());
    let removed_at = if linger_expired {
        0
    } else {
        chrono::Utc::now().timestamp_millis()
    };
    for meta in [env.meta.clone(), current_meta] {
        env.region
            .manifest_ctx
            .update_manifest(
                RegionLeaderState::Writable,
                RegionMetaActionList::with_action(RegionMetaAction::Edit(RegionEdit {
                    files_to_add: vec![meta],
                    files_to_remove: vec![],
                    timestamp_ms: Some(removed_at),
                    compaction_time_window: None,
                    flushed_entry_id: None,
                    flushed_sequence: None,
                    committed_sequence: None,
                    wal_recovery: Default::default(),
                })),
                false,
            )
            .await
            .unwrap();
    }

    let layer = &env.region.access_layer;
    let parquet = location::sst_file_path(layer.table_dir(), env.meta.file_id(), layer.path_type());
    let old_index =
        location::index_file_path(layer.table_dir(), env.meta.index_id(), layer.path_type());
    let current_index = location::index_file_path(
        layer.table_dir(),
        current_reader.index_id(),
        layer.path_type(),
    );
    let mut old_reader = Some(old_reader);
    for released in [false, true] {
        if released {
            drop(old_reader.take());
        }
        let refs = env
            .engine
            .get_snapshot_of_file_refs([region_id], HashMap::new())
            .await
            .unwrap();
        let worker = create_gc_worker(
            &env.engine,
            BTreeMap::from([(region_id, Some(env.region.clone()))]),
            &refs,
            full_file_listing,
        )
        .await;
        let report = worker.run().await.unwrap();
        let reclaimed = released && linger_expired;
        assert!(report.deleted_files[&region_id].is_empty());
        assert_eq!(
            report.deleted_indexes[&region_id],
            if reclaimed {
                vec![(env.meta.file_id, 0)]
            } else {
                vec![]
            }
        );
        assert!(layer.object_store().exists(&parquet).await.unwrap());
        assert!(layer.object_store().exists(&current_index).await.unwrap());
        assert_eq!(
            layer.object_store().exists(&old_index).await.unwrap(),
            !reclaimed
        );
    }
    drop(current_reader);
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
    // A manual compaction may take several rounds, each recording its removals.
    let removed_file_num: usize = manifest
        .removed_files
        .removed_files
        .iter()
        .map(|removed| removed.files.len())
        .sum();
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
            manifest
                .removed_files
                .removed_files
                .iter()
                .flat_map(|removed| removed.files.iter())
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
    let parent = std::path::Path::new(path)
        .parent()
        .filter(|p| !p.as_os_str().is_empty());
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

/// Unknown file with a missing `last_modified`: keep it (conservative).
#[tokio::test]
async fn test_unknown_file_within_ttl_not_deleted() {
    let store = ObjectStore::new(services::Memory::default()).unwrap();
    let entry = write_and_list_entry(&store, "test/1.parquet").await;
    assert!(entry.metadata().last_modified().is_none());

    let threshold = chrono::DateTime::from_timestamp(0, 0).unwrap();
    assert!(!should_delete_file(
        false, false, false, false, &entry, threshold
    ));
}

/// Unknown file older than the lingering window: reclaim it.
#[tokio::test]
async fn test_unknown_file_exceeded_ttl_deleted() {
    let tmp_dir = common_test_util::temp_dir::create_temp_dir("gc_unknown_ttl");
    let root = tmp_dir.path().to_string_lossy().to_string();
    let store = ObjectStore::new(services::Fs::default().root(&root)).unwrap();
    let entry = write_and_list_entry(&store, "2.parquet").await;

    let threshold = chrono::Utc::now() + chrono::Duration::days(1);
    assert!(should_delete_file(
        false, false, false, false, &entry, threshold
    ));
}

/// Without a region manifest (dropped, or not hosted here) an unknown file must
/// still pass `unknown_file_lingering_time` instead of being deleted immediately,
/// because a young object may still belong to an unpublished table-compaction
/// attempt.
#[tokio::test]
async fn test_unknown_file_without_region_manifest_lingers() {
    let tmp_dir = common_test_util::temp_dir::create_temp_dir("gc_no_manifest");
    let root = tmp_dir.path().to_string_lossy().to_string();
    let store = ObjectStore::new(services::Fs::default().root(&root)).unwrap();
    let entry = write_and_list_entry(&store, "no_manifest.parquet").await;

    let young_cutoff = chrono::DateTime::from_timestamp(0, 0).unwrap();
    assert!(
        !should_delete_file(false, false, false, false, &entry, young_cutoff),
        "a young unknown file must be kept even without a region manifest"
    );

    let old_cutoff = chrono::Utc::now() + chrono::Duration::days(1);
    assert!(
        should_delete_file(false, false, false, false, &entry, old_cutoff),
        "an unknown file older than the lingering window is reclaimed"
    );
}

/// A live or referenced file is never deleted, even when its object is old.
#[tokio::test]
async fn test_referenced_file_kept_with_old_mtime() {
    let tmp_dir = common_test_util::temp_dir::create_temp_dir("gc_referenced_old");
    let root = tmp_dir.path().to_string_lossy().to_string();
    let store = ObjectStore::new(services::Fs::default().root(&root)).unwrap();
    let entry = write_and_list_entry(&store, "referenced.parquet").await;
    let threshold = chrono::Utc::now() + chrono::Duration::days(1);

    assert!(
        !should_delete_file(true, false, false, false, &entry, threshold),
        "a file in the manifest must be kept"
    );
    assert!(
        !should_delete_file(false, true, false, false, &entry, threshold),
        "a file referenced by a temporary reader must be kept"
    );
}

/// A known removed file is only deleted once its lingering period elapsed.
#[tokio::test]
async fn test_known_removed_file_respects_linger() {
    let store = ObjectStore::new(services::Memory::default()).unwrap();
    let entry = write_and_list_entry(&store, "test/6.parquet").await;
    let threshold = chrono::Utc::now() + chrono::Duration::days(1);

    assert!(
        !should_delete_file(false, false, true, false, &entry, threshold),
        "a removed file that is still lingering must be kept"
    );
    assert!(
        should_delete_file(false, false, true, true, &entry, threshold),
        "a removed file past its lingering period is reclaimed"
    );
}

/// Files at or after the cutoff are kept (`strict <` comparison).
#[tokio::test]
async fn test_unknown_file_at_cutoff_kept() {
    let tmp_dir = common_test_util::temp_dir::create_temp_dir("gc_cutoff");
    let root = tmp_dir.path().to_string_lossy().to_string();
    let store = ObjectStore::new(services::Fs::default().root(&root)).unwrap();
    let entry = write_and_list_entry(&store, "cutoff.parquet").await;
    let last_modified = entry.metadata().last_modified().unwrap();
    let threshold =
        chrono::DateTime::from_timestamp_millis(last_modified.into_inner().as_millisecond())
            .unwrap();

    assert!(
        !should_delete_file(false, false, false, false, &entry, threshold),
        "a file at the exact cutoff is kept"
    );
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

#[derive(Debug, Clone, Copy)]
enum ProtectionOutcome {
    Protected,
    Unavailable,
    PublishedDuringLookup,
    Unprotected,
}

#[derive(Debug)]
struct BlockingProtectionHook {
    outcome: ProtectionOutcome,
    entered: tokio::sync::Notify,
    resume: tokio::sync::Notify,
}

#[async_trait]
impl RegionHook for BlockingProtectionHook {
    async fn protected_sst_files(
        &self,
        _access: &crate::access_layer::AccessLayer,
        files: &[crate::sst::file::RegionFileId],
    ) -> crate::error::Result<HashSet<crate::sst::file::RegionFileId>> {
        self.entered.notify_one();
        self.resume.notified().await;
        match self.outcome {
            ProtectionOutcome::Protected => Ok(files.iter().copied().collect()),
            ProtectionOutcome::Unavailable => UnexpectedSnafu {
                reason: "protection service unavailable",
            }
            .fail(),
            _ => Ok(HashSet::new()),
        }
    }
}

#[rstest::rstest]
#[case::protected(ProtectionOutcome::Protected)]
#[case::unavailable(ProtectionOutcome::Unavailable)]
#[case::published(ProtectionOutcome::PublishedDuringLookup)]
#[case::unprotected(ProtectionOutcome::Unprotected)]
#[tokio::test]
async fn test_gc_protection_and_manifest_recheck(#[case] outcome: ProtectionOutcome) {
    let mut env = TestEnv::new().await;
    let hook = Arc::new(BlockingProtectionHook {
        outcome,
        entered: Default::default(),
        resume: Default::default(),
    });
    let plugins = Plugins::new();
    plugins.insert(hook.clone() as RegionHookRef);
    let engine = env
        .create_engine_with_plugins(
            MitoConfig {
                schedule_compaction_after_edit: false,
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
    engine
        .handle_request(
            region_id,
            RegionRequest::Create(CreateRequestBuilder::new().build()),
        )
        .await
        .unwrap();
    let region = engine.get_region(region_id).unwrap();
    let file = crate::sst::file::FileMeta {
        region_id,
        file_id: store_api::storage::FileId::random(),
        ..Default::default()
    };
    let path = crate::sst::location::sst_file_path(
        region.access_layer.table_dir(),
        file.file_id(),
        region.access_layer.path_type(),
    );
    region
        .access_layer
        .object_store()
        .write(&path, Bytes::from_static(b"unreferenced SST"))
        .await
        .unwrap();
    engine
        .edit_region(
            region_id,
            crate::manifest::action::RegionEdit {
                files_to_add: vec![file.clone()],
                ..Default::default()
            },
        )
        .await
        .unwrap();
    engine
        .edit_region(
            region_id,
            crate::manifest::action::RegionEdit {
                files_to_remove: vec![file.clone()],
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let refs = FileRefsManifest {
        manifest_version: [(region_id, region.manifest_ctx.manifest_version().await)].into(),
        file_refs: Default::default(),
        cross_region_refs: Default::default(),
    };
    let worker = create_gc_worker(
        &engine,
        [(region_id, Some(region.clone()))].into(),
        &refs,
        false,
    )
    .await;
    let temporary_refs = HashSet::new();
    let (gc, ()) = tokio::join!(
        worker.do_region_gc(region_id, Some(region.clone()), &temporary_refs),
        async {
            hook.entered.notified().await;
            if matches!(outcome, ProtectionOutcome::PublishedDuringLookup) {
                engine
                    .edit_region(
                        region_id,
                        crate::manifest::action::RegionEdit {
                            files_to_add: vec![file],
                            ..Default::default()
                        },
                    )
                    .await
                    .unwrap();
            }
            hook.resume.notify_one();
        }
    );
    if matches!(outcome, ProtectionOutcome::Unavailable) {
        assert!(gc.is_err());
    } else {
        let gc = gc.unwrap();
        assert_eq!(
            usize::from(matches!(outcome, ProtectionOutcome::Unprotected)),
            gc.removed_files.len()
        );
    }
    assert_eq!(
        !matches!(outcome, ProtectionOutcome::Unprotected),
        region
            .access_layer
            .object_store()
            .exists(&path)
            .await
            .unwrap()
    );
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

/// Protects every candidate it is asked about; used to exercise deferrals on
/// the dropped-region reclamation path.
#[derive(Debug)]
struct ProtectAllHook;

#[async_trait]
impl RegionHook for ProtectAllHook {
    async fn protected_sst_files(
        &self,
        _access: &crate::access_layer::AccessLayer,
        files: &[crate::sst::file::RegionFileId],
    ) -> crate::error::Result<HashSet<crate::sst::file::RegionFileId>> {
        Ok(files.iter().copied().collect())
    }
}

/// Shared setup: create a region with one flushed SST, then hand its access
/// layer to a dropped-region GC worker (region absent) built from `config` and
/// `hook`. Returns the worker and the region's SST/index object paths.
async fn dropped_region_gc_env(
    config: GcConfig,
    hook: Option<RegionHookRef>,
) -> (TestEnv, MitoEngine, LocalGcWorker, RegionId, Vec<String>) {
    let mut env = TestEnv::new().await;
    let engine = env
        .create_engine(MitoConfig {
            schedule_compaction_after_edit: false,
            gc: config,
            ..Default::default()
        })
        .await;
    let region_id = RegionId::new(3, 1);
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
    let access_layer = region.access_layer.clone();
    let manifest = region.manifest_ctx.manifest().await;
    let version = manifest.manifest_version;
    let store = access_layer.object_store();
    let mut paths = Vec::new();
    for meta in manifest.files.values() {
        for path in [
            location::sst_file_path(
                access_layer.table_dir(),
                crate::sst::file::RegionFileId::new(meta.region_id, meta.file_id),
                access_layer.path_type(),
            ),
            location::index_file_path(
                access_layer.table_dir(),
                meta.index_id(),
                access_layer.path_type(),
            ),
        ] {
            if store.exists(&path).await.unwrap() {
                paths.push(path);
            }
        }
    }
    assert!(!paths.is_empty(), "the flushed SST objects must be listed");
    let file_ref_manifest = FileRefsManifest {
        file_refs: Default::default(),
        manifest_version: [(region_id, version)].into(),
        cross_region_refs: HashMap::new(),
    };
    let worker = LocalGcWorker::try_new(
        access_layer,
        Some(engine.cache_manager()),
        BTreeMap::from([(region_id, None)]),
        engine.mito_config().gc.clone(),
        file_ref_manifest,
        &engine.gc_limiter(),
        true,
        hook,
    )
    .await
    .unwrap();
    // Keep the env (and its data-home TempDir) alive for the caller: dropping
    // it removes the on-disk objects that the dropped-region pass must list.
    (env, engine, worker, region_id, paths)
}

/// A dropped region's young unknown objects are deferred by
/// `unknown_file_lingering_time`; the pass must report `needs_retry` so the
/// meta tombstone survives and a later pass can reclaim them.
#[tokio::test]
async fn test_dropped_region_young_unknown_files_need_retry() {
    init_default_ut_logging();
    let (_env, _engine, worker, region_id, paths) = dropped_region_gc_env(
        GcConfig {
            enable: true,
            ..Default::default()
        },
        None,
    )
    .await;

    let outcome = worker
        .do_region_gc(region_id, None, &HashSet::new())
        .await
        .unwrap();

    assert!(outcome.removed_files.is_empty());
    assert!(
        outcome.needs_retry,
        "deferred young unknown objects must keep the dropped-region tombstone"
    );
    let store = worker.access_layer.object_store();
    for path in &paths {
        assert!(store.exists(path).await.unwrap(), "{path} must survive");
    }
}

/// Hook-protected candidates on a dropped region must also keep the tombstone;
/// once nothing defers the pass any longer the report clears so the procedure
/// can drop the tombstone.
#[tokio::test]
async fn test_dropped_region_hook_deferral_needs_retry_until_reclaimed() {
    init_default_ut_logging();
    let config = GcConfig {
        enable: true,
        lingering_time: None,
        unknown_file_lingering_time: std::time::Duration::ZERO,
        ..Default::default()
    };
    let (_env, _engine, worker, region_id, paths) =
        dropped_region_gc_env(config.clone(), Some(Arc::new(ProtectAllHook))).await;

    let store = worker.access_layer.object_store();
    let protected = worker
        .do_region_gc(region_id, None, &HashSet::new())
        .await
        .unwrap();
    assert!(protected.removed_files.is_empty());
    assert!(
        protected.needs_retry,
        "hook-deferred candidates must keep the dropped-region tombstone"
    );
    for path in &paths {
        assert!(store.exists(path).await.unwrap(), "{path} must survive");
    }

    // Nothing defers any more: objects are old enough and no hook retains them,
    // so this pass reclaims them and clears the retry flag.
    let (_env, _engine, worker, region_id, paths) = dropped_region_gc_env(config, None).await;
    let store = worker.access_layer.object_store();
    let reclaimed = worker
        .do_region_gc(region_id, None, &HashSet::new())
        .await
        .unwrap();
    assert!(
        !reclaimed.needs_retry,
        "nothing defers any more, so the tombstone can be dropped"
    );
    assert!(!reclaimed.removed_files.is_empty());
    for path in &paths {
        assert!(!store.exists(path).await.unwrap(), "{path} must be gone");
    }
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
