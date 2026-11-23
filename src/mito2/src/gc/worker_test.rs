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

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use api::v1::Rows;
use async_trait::async_trait;
use bytes::Bytes;
use common_base::Plugins;
use common_datasource::compression::CompressionType;
use common_telemetry::init_default_ut_logging;
use common_time::Timestamp;
use futures::TryStreamExt;
use object_store::layers::mock::{
    Error as MockError, ErrorKind, MockLayer, MockLayerBuilder, OpDelete, Result as MockResult, oio,
};
use object_store::{Entry, ObjectStore, services};
use store_api::metadata::RegionMetadataRef;
use store_api::region_engine::{RegionEngine as _, RegionRole};
use store_api::region_request::{RegionCompactRequest, RegionRequest};
use store_api::storage::{FileId, FileRef, FileRefsManifest, GcReport, IndexVersion, RegionId};

use crate::access_layer::{AccessLayer, AccessLayerRef};
use crate::cache::file_cache::FileType;
use crate::config::MitoConfig;
use crate::engine::MitoEngine;
use crate::engine::compaction_test::{delete_and_flush, put_and_flush};
use crate::engine::pending_tail_test::{set_pending_tails, tail};
use crate::engine::region_hook::{RegionGcInfo, RegionHook, RegionHookRef};
use crate::error::UnexpectedSnafu;
use crate::gc::{GcConfig, LocalGcWorker, should_delete_file};
use crate::manifest::action::{
    RegionEdit, RegionManifest, RegionMetaAction, RegionMetaActionList, RemovedFile,
    RemovedFilesRecord,
};
use crate::manifest::storage::{delta_file, gen_path, manifest_dir};
use crate::metrics::{GC_ERRORS_TOTAL, GC_SKIPPED_UNPARSABLE_FILES};
use crate::region::{MitoRegionRef, RegionLeaderState};
use crate::sst::file::{
    ColumnIndexMetadata, FileFormat, FileHandle, FileMeta, FileMetaRef, IndexType, NarthexPartMeta,
    NarthexSamplesMeta, RegionFileId, RegionIndexId,
};
use crate::sst::location;
use crate::test_util::{
    CreateRequestBuilder, TestEnv, build_rows, flush_region, put_rows, rows_schema,
};
use crate::worker::DROPPING_MARKER_FILE;

/// Builds a manifest that references `files` as SST files and `series_indexes` as
/// series index files, each entry reporting the given index version.
fn manifest_with_files(
    files: impl IntoIterator<Item = (FileId, Option<IndexVersion>)>,
    series_indexes: impl IntoIterator<Item = (FileId, Option<IndexVersion>)>,
) -> RegionManifest {
    RegionManifest {
        metadata: crate::test_util::memtable_util::metadata_for_test(),
        files: file_metas(files),
        series_indexes: file_metas(series_indexes),
        removed_files: RemovedFilesRecord::default(),
        flushed_entry_id: 0,
        flushed_sequence: 0,
        committed_sequence: None,
        manifest_version: 0,
        truncated_entry_id: None,
        compaction_time_window: None,
        sst_format: Default::default(),
        append_mode: None,
        pending_tails: Vec::new(),
        handoff_floors: Default::default(),
    }
}

/// Maps `(file_id, index_version)` pairs to manifest entries reporting that version.
fn file_metas(
    files: impl IntoIterator<Item = (FileId, Option<IndexVersion>)>,
) -> HashMap<FileId, FileMetaRef> {
    files
        .into_iter()
        .map(|(file_id, index_version)| {
            let meta = match index_version {
                Some(index_version) => FileMeta {
                    file_id,
                    available_indexes: smallvec::smallvec![IndexType::InvertedIndex],
                    index_version,
                    ..Default::default()
                },
                None => FileMeta {
                    file_id,
                    ..Default::default()
                },
            };
            (file_id, meta.into())
        })
        .collect()
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
                None,
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
    let manifest = in_manifest.then(|| manifest_with_files([(file_id, index_version)], []));
    let tmp_refs = if in_manifest {
        HashSet::new()
    } else {
        HashSet::from([(file_id, index_version)])
    };
    let (deleted, _deferred_unknown) = env
        .worker
        .list_to_be_deleted_files(
            env.meta.region_id,
            manifest.as_ref(),
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
                    ..Default::default()
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
                    RemovedFile::Narthex(file_id) => FileRef::new(region_id, *file_id, None),
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

struct FailingLister;

impl oio::List for FailingLister {
    async fn next(&mut self) -> MockResult<Option<oio::Entry>> {
        Err(MockError::new(ErrorKind::Unexpected, "mock list failure"))
    }
}

#[rstest::rstest]
#[case::pending_tails("pending_tails")]
#[case::newer_manifest("newer_manifest")]
#[case::manifest_list_failure("manifest_list_failure")]
#[case::manifest_mismatch("manifest_mismatch")]
#[tokio::test]
async fn test_full_listing_gc_skips_region(#[case] cause: &str) {
    let fail_manifest_lists = Arc::new(AtomicBool::new(false));
    let fail = fail_manifest_lists.clone();
    let mock_layer = MockLayerBuilder::default()
        .lister_factory(Arc::new(move |path, _, inner| {
            if fail.load(Ordering::Relaxed) && path.contains("/manifest/") {
                Box::new(FailingLister)
            } else {
                inner
            }
        }))
        .build()
        .unwrap();
    let mut env = TestEnv::new().await.with_mock_layer(mock_layer);
    let hook = Arc::new(GcCountingHook::default());
    let plugins = Plugins::new();
    plugins.insert(hook.clone() as RegionHookRef);
    let config = MitoConfig {
        gc: GcConfig {
            enable: true,
            ..Default::default()
        },
        ..Default::default()
    };
    let engine = env.create_engine_with_plugins(config, plugins).await;
    let region_id = RegionId::new(1, 1);
    engine
        .handle_request(
            region_id,
            RegionRequest::Create(CreateRequestBuilder::new().build()),
        )
        .await
        .unwrap();
    let region = engine.get_region(region_id).unwrap();
    let run_gc = async |full_file_listing, mismatch| {
        let mut file_ref_manifest = FileRefsManifest {
            file_refs: Default::default(),
            manifest_version: [(region_id, region.manifest_ctx.manifest_version().await)].into(),
            cross_region_refs: HashMap::new(),
        };
        if mismatch {
            file_ref_manifest.manifest_version.remove(&region_id);
        }
        let regions = BTreeMap::from([(region_id, Some(region.clone()))]);
        let gc_worker =
            create_gc_worker(&engine, regions, &file_ref_manifest, full_file_listing).await;
        gc_worker.run().await.unwrap()
    };
    let report = run_gc(true, false).await;
    assert!(report.processed_regions.contains(&region_id));
    let fast_report = run_gc(false, false).await;
    assert!(
        fast_report.processed_regions.contains(&region_id),
        "a successful fast GC must still acknowledge the region as processed"
    );
    assert_eq!(1, hook.gc_calls.load(Ordering::Relaxed));

    match cause {
        "pending_tails" => set_pending_tails(&engine, region_id, vec![tail(1, 0, 1)]).await,
        "newer_manifest" => {
            let store = region.manifest_ctx.manifest_manager.read().await.store();
            let version = region.manifest_ctx.manifest_version().await;
            let path = gen_path(
                store.manifest_dir(),
                &delta_file(version + 1),
                CompressionType::Uncompressed,
            );
            let object_store = region.access_layer.object_store();
            object_store.write(&path, b"".as_slice()).await.unwrap();
        }
        "manifest_list_failure" => fail_manifest_lists.store(true, Ordering::Relaxed),
        "manifest_mismatch" => {}
        _ => unreachable!(),
    }
    let error_label = match cause {
        "pending_tails" => "pending_tails",
        "newer_manifest" => "newer_manifest",
        "manifest_list_failure" => "list_failed",
        "manifest_mismatch" => "manifest_mismatch",
        _ => unreachable!(),
    };
    let errors = GC_ERRORS_TOTAL.with_label_values(&[error_label]).get();
    let report = run_gc(true, cause == "manifest_mismatch").await;

    assert_eq!(1, hook.gc_calls.load(Ordering::Relaxed));
    assert!(
        !report.processed_regions.contains(&region_id),
        "an incomplete GC must not acknowledge the region as processed"
    );
    assert!(
        !report.need_retry_regions.contains(&region_id),
        "entry checks must not send a region through repartition retry"
    );
    assert!(
        GC_ERRORS_TOTAL.with_label_values(&[error_label]).get() > errors,
        "the matching error counter must increment"
    );
}

#[tokio::test]
async fn test_narthex_artifact_gc_inventory_and_references() {
    let env = IndexGcTestEnv::new(true).await;
    let region_id = env.meta.region_id;
    let layer = &env.region.access_layer;
    let id = FileId::random();
    let region_dir = layer.build_region_dir(region_id);
    let prefix = location::narthex_file_dir(&region_dir, id);
    let paths = [
        format!("{prefix}manifest.json"),
        format!("{prefix}part-0"),
        format!("{prefix}part-1"),
    ];
    for path in &paths {
        layer
            .object_store()
            .write(path, "narthex".as_bytes())
            .await
            .unwrap();
    }
    let entries = env
        .worker
        .list_from_object_store(region_id, 3)
        .await
        .unwrap();
    let entries: Vec<_> = entries
        .into_iter()
        .filter(|entry| crate::sst::location::parse_narthex_file_id(entry.path()) == Some(id))
        .collect();
    assert_eq!(entries.len(), 3);
    let removed = RemovedFile::Narthex(id);
    let threshold = chrono::Utc::now() + chrono::Duration::days(1);
    let manifest = manifest_with_files([(id, None)], []);
    for (manifest, readers) in [
        (Some(&manifest), HashSet::new()),
        (None, HashSet::from([(id, None)])),
    ] {
        let (delete, _) = env.worker.filter_deletable_files(
            entries.clone(),
            manifest,
            &readers,
            &HashSet::new(),
            &HashSet::from([&removed]),
            threshold,
        );
        assert!(delete.is_empty());
    }
    let (delete, _) = env.worker.filter_deletable_files(
        entries,
        None,
        &HashSet::new(),
        &HashSet::new(),
        &HashSet::from([&removed]),
        threshold,
    );
    assert_eq!(delete, vec![removed]);
    env.worker.delete_files(region_id, &delete).await.unwrap();
    assert!(!layer.object_store().exists(&prefix).await.unwrap());
    for path in paths {
        assert!(!layer.object_store().exists(&path).await.unwrap());
    }
}

#[tokio::test]
async fn test_narthex_artifact_gc_requires_age_for_every_object() {
    let env = IndexGcTestEnv::new(true).await;
    let layer = &env.region.access_layer;
    let id = FileId::random();
    let prefix =
        crate::sst::location::narthex_file_dir(&layer.build_region_dir(env.meta.region_id), id);
    let old = write_and_list_entry(layer.object_store(), &format!("{prefix}part-0")).await;
    // A missing timestamp must withhold the entire artifact even if another
    // part has a known, sufficiently old timestamp.
    let memory = ObjectStore::new(services::Memory::default()).unwrap();
    let unknown = write_and_list_entry(&memory, &format!("{prefix}manifest.json")).await;
    let threshold = chrono::Utc::now() + chrono::Duration::days(1);
    assert!(should_delete_file(
        false, false, false, false, &old, threshold
    ));
    let (delete, deferred) = env.worker.filter_deletable_files(
        vec![old, unknown],
        None,
        &HashSet::new(),
        &HashSet::new(),
        &HashSet::new(),
        threshold,
    );
    assert!(delete.is_empty());
    assert!(deferred);
}

// ===========================================================================
// Physical-delete oracle
//
// `GcReport` only says what a pass *believes* it deleted, and a
// `RemovedFile::File(id, v)` expands into the Parquet object plus every index
// object `0..=v` at the real delete site. These tests therefore observe the
// real delete site: `recording_layer` wraps the object store's deleter and
// records every path and listing a pass hands to storage, changing nothing that
// the production filters decide. Each recorded path is then checked against an
// expectation model written from the safety contract (in manifest / in a
// temporary reader reference / past the lingering period / not hook protected /
// old enough to be unknown), never from `should_delete_file` itself.
//
// The check is two-sided: nothing outside the expected set may be deleted, and
// everything in it must really be gone (a pass that never deletes would
// otherwise satisfy every safety assertion). Deletes of absent objects are
// no-ops that the `0..=v` index fan-out produces legitimately, so they are
// compared separately, and the model refuses to guess an age or a linger state
// it cannot prove from the pass window.
// ===========================================================================

/// A data object GC can physically delete.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
enum GcObject {
    /// Parquet SST object of one file id.
    Sst(FileId),
    /// Puffin index object of one file id at one metadata version.
    Index(FileId, IndexVersion),
    /// One object of the narthex artifact directory of a file id.
    Narthex(FileId, NarthexObject),
}

/// One object inside a narthex artifact directory `<region>/<file id>/`.
///
/// A narthex artifact is one immutable file of *parts*, so it is never deleted
/// object by object: the removal of the whole directory is one unit at the
/// delete site (`AccessLayer::delete_narthex_artifact`), while the safety
/// contract has to be judged part by part. The oracle therefore tracks the
/// parts as objects (inventory, age, `deleted_ever`) and the artifact as the
/// unit the pass really hands to the store.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
enum NarthexObject {
    /// The artifact's own attribute file, `manifest.json`.
    Manifest,
    /// One immutable part; parts are numbered from zero, `part-<n>.samples`.
    Part(u32),
}

impl NarthexObject {
    fn name(self) -> String {
        match self {
            NarthexObject::Manifest => "manifest.json".to_string(),
            NarthexObject::Part(part) => format!("part-{part}.samples"),
        }
    }
}

impl GcObject {
    fn file_id(self) -> FileId {
        match self {
            GcObject::Sst(id) | GcObject::Index(id, _) | GcObject::Narthex(id, _) => id,
        }
    }

    fn describe(self) -> String {
        match self {
            GcObject::Sst(id) => format!("sst({id})"),
            GcObject::Index(id, version) => format!("index({id}, v{version})"),
            GcObject::Narthex(id, object) => format!("narthex({id}, {})", object.name()),
        }
    }
}

fn normalize_object_path(path: &str) -> String {
    path.trim_start_matches('/').to_string()
}

/// Path of `object` in the region directory, as the object store reports it.
fn object_path(object: GcObject, region_id: RegionId, layer: &AccessLayer) -> String {
    let path = match object {
        GcObject::Sst(id) => location::sst_file_path(
            layer.table_dir(),
            RegionFileId::new(region_id, id),
            layer.path_type(),
        ),
        GcObject::Index(id, version) => location::index_file_path(
            layer.table_dir(),
            RegionIndexId::new(RegionFileId::new(region_id, id), version),
            layer.path_type(),
        ),
        GcObject::Narthex(id, object) => format!(
            "{}{}",
            location::narthex_file_dir(&layer.build_region_dir(region_id), id),
            object.name()
        ),
    };
    normalize_object_path(&path)
}

/// The artifact directory a delete path names, if that delete covered the whole
/// artifact (`delete_with(dir).recursive(true)`) instead of a single object.
///
/// The recursive removal is one delete call naming the directory, so a pass that
/// reclaims an artifact never names its parts. Part paths are not directories:
/// their last component is `manifest.json` or `part-<n>.samples`.
fn narthex_artifact_dir(path: &str) -> Option<FileId> {
    let trimmed = path.trim_end_matches('/');
    if !location::is_narthex_file_dir(trimmed) {
        return None;
    }
    trimmed.rsplit('/').next()?.parse().ok()
}

/// Classifies a path below the region directory.
fn object_of_path(path: &str) -> Option<GcObject> {
    if let Ok((file_id, file_type)) = location::parse_file_id_type_from_path(path) {
        return Some(match file_type {
            FileType::Parquet => GcObject::Sst(file_id),
            FileType::Puffin(version) => GcObject::Index(file_id, version),
        });
    }
    let file_id = location::parse_narthex_file_id(path)?;
    let name = path.trim_end_matches('/').rsplit('/').next()?;
    let object = match name {
        "manifest.json" => NarthexObject::Manifest,
        _ => {
            let item = name.strip_prefix("part-")?;
            NarthexObject::Part(item.split('.').next()?.parse().ok()?)
        }
    };
    Some(GcObject::Narthex(file_id, object))
}

/// One `delete` call forwarded to the underlying store.
#[derive(Clone, Debug)]
struct DeleteRecord {
    /// Oracle round the call happened in, or [`NO_ROUND`] outside a pass.
    round: usize,
    /// Deleter instance (`deleter()` call, i.e. deletion batch) the call belongs to.
    batch: usize,
    path: String,
    /// `Some` when the underlying delete reported an error.
    error: Option<String>,
}

/// One `lister()` the store handed out, with its prefix.
#[derive(Clone, Debug)]
struct ListRecord {
    round: usize,
    path: String,
}

/// Marks work that happens outside a GC pass.
const NO_ROUND: usize = usize::MAX;

/// Side-channel observation of what GC does to the object store.
#[derive(Debug, Default)]
struct GcDeleteLog {
    records: Mutex<Vec<DeleteRecord>>,
    lists: Mutex<Vec<ListRecord>>,
    batches: AtomicUsize,
    next_round: AtomicUsize,
    current_round: AtomicUsize,
}

impl GcDeleteLog {
    fn new() -> Arc<GcDeleteLog> {
        Arc::new(GcDeleteLog {
            current_round: AtomicUsize::new(NO_ROUND),
            ..Default::default()
        })
    }

    /// Opens a pass window; every delete and list until [`Self::end_round`] is
    /// attributed to it.
    fn begin_round(&self) -> usize {
        let round = self.next_round.fetch_add(1, Ordering::SeqCst);
        self.current_round.store(round, Ordering::SeqCst);
        round
    }

    fn end_round(&self) {
        self.current_round.store(NO_ROUND, Ordering::SeqCst);
    }

    fn batch(&self) -> usize {
        self.batches.fetch_add(1, Ordering::SeqCst)
    }

    fn deletes(&self) -> Vec<DeleteRecord> {
        self.records.lock().unwrap().clone()
    }

    fn lists(&self) -> Vec<ListRecord> {
        self.lists.lock().unwrap().clone()
    }
}

/// Forwards every delete to the wrapped deleter and records the attempt first,
/// so a failing delete is observed too.
struct RecordingDeleter {
    inner: oio::Deleter,
    batch: usize,
    log: Arc<GcDeleteLog>,
}

impl oio::Delete for RecordingDeleter {
    async fn delete(&mut self, path: &str, args: OpDelete) -> MockResult<()> {
        let result = self.inner.delete(path, args).await;
        self.log.records.lock().unwrap().push(DeleteRecord {
            round: self.log.current_round.load(Ordering::SeqCst),
            batch: self.batch,
            path: normalize_object_path(path),
            error: result.as_ref().err().map(|err| err.to_string()),
        });
        result
    }

    async fn close(&mut self) -> MockResult<()> {
        self.inner.close().await
    }
}

/// Faults the oracle can arm on [`recording_layer`].
#[derive(Debug, Default)]
struct LayerFaults {
    /// Fails every list of the manifest directory, so `has_update()` errors.
    fail_manifest_lists: AtomicBool,
}

/// Object store layer that records deletes and listings and forwards everything.
fn recording_layer(log: Arc<GcDeleteLog>, faults: Arc<LayerFaults>) -> MockLayer {
    let list_log = log.clone();
    MockLayerBuilder::default()
        .deleter_factory(Arc::new(move |inner| {
            Box::new(RecordingDeleter {
                inner,
                batch: log.batch(),
                log: log.clone(),
            }) as oio::Deleter
        }))
        .lister_factory(Arc::new(move |path, _args, inner| {
            list_log.lists.lock().unwrap().push(ListRecord {
                round: list_log.current_round.load(Ordering::SeqCst),
                path: normalize_object_path(path),
            });
            if faults.fail_manifest_lists.load(Ordering::SeqCst) && path.contains("/manifest/") {
                Box::new(FailingLister)
            } else {
                inner
            }
        }))
        .build()
        .unwrap()
}

/// Region hook used by the oracle. It reports a fixed protection set (so the
/// model knows what "hook protected" means in each pass) and can block inside
/// the protection lookup, which lets a publication race the pass.
#[derive(Debug, Default)]
struct OracleHook {
    protected: RwLock<HashSet<FileId>>,
    /// When set, the next protection lookup blocks until `resume`.
    block_next: AtomicBool,
    entered: tokio::sync::Notify,
    resume: tokio::sync::Notify,
}

impl OracleHook {
    fn protect(&self, ids: impl IntoIterator<Item = FileId>) {
        *self.protected.write().unwrap() = ids.into_iter().collect();
    }

    fn protected_ids(&self) -> HashSet<FileId> {
        self.protected.read().unwrap().clone()
    }

    fn block_next_lookup(&self) {
        self.block_next.store(true, Ordering::SeqCst);
    }
}

#[async_trait]
impl RegionHook for OracleHook {
    async fn protected_sst_files(
        &self,
        _access: &AccessLayer,
        files: &[RegionFileId],
    ) -> crate::error::Result<HashSet<RegionFileId>> {
        if self.block_next.swap(false, Ordering::SeqCst) {
            self.entered.notify_one();
            self.resume.notified().await;
        }
        let protected = self.protected.read().unwrap();
        Ok(files
            .iter()
            .copied()
            .filter(|file| protected.contains(&file.file_id()))
            .collect())
    }
}

/// "Long enough that nothing the test just wrote can be old": every record and
/// every object written by the test is young under this setting.
const YOUNG_LINGER: Option<Duration> = Some(Duration::from_secs(3600));
/// `lingering_time = None`: no known record waits.
const NO_KNOWN_WAIT: Option<Duration> = None;
/// Every object written by the test is younger than this.
const YOUNG_UNKNOWN_LINGER: Duration = Duration::from_secs(3600);
/// No grace for unknown objects; the oracle waits until each object is strictly
/// older than the pass start, so "old enough" is a fact rather than a guess.
const MATURE_UNKNOWN_LINGER: Duration = Duration::ZERO;

/// Knobs of one GC pass.
#[derive(Clone, Copy, Debug)]
struct RoundKnobs {
    full_listing: bool,
    linger: Option<Duration>,
    unknown_linger: Duration,
}

impl RoundKnobs {
    const fn fast(linger: Option<Duration>, unknown_linger: Duration) -> Self {
        Self {
            full_listing: false,
            linger,
            unknown_linger,
        }
    }

    const fn full(linger: Option<Duration>, unknown_linger: Duration) -> Self {
        Self {
            full_listing: true,
            linger,
            unknown_linger,
        }
    }

    fn gc_config(self) -> GcConfig {
        GcConfig {
            enable: true,
            lingering_time: self.linger,
            unknown_file_lingering_time: self.unknown_linger,
            ..Default::default()
        }
    }
}

/// Wiring of one pass beyond the GC knobs.
#[derive(Clone, Copy, Debug, Default)]
struct PassWiring {
    /// Hand the worker no manifest version for the region: the entry check must
    /// skip the region.
    omit_manifest_version: bool,
}

impl PassWiring {
    const fn mismatched_version() -> Self {
        Self {
            omit_manifest_version: true,
        }
    }
}

/// What one pass must do.
#[derive(Clone, Copy, Debug)]
struct PassExpectation {
    /// The pass runs to completion and deletes exactly the oracle's candidates.
    complete: bool,
    /// The region is routed to a retry.
    needs_retry: bool,
    /// The pass must really delete something. Only valid together with
    /// `complete`: it is the positive control that keeps a completed pass from
    /// passing the exactness check with an empty expectation by doing nothing.
    must_delete: bool,
}

impl PassExpectation {
    const fn complete() -> Self {
        Self {
            complete: true,
            needs_retry: false,
            must_delete: false,
        }
    }

    /// A pass that runs to completion and is required to physically delete the
    /// objects the oracle expects.
    const fn complete_deleting() -> Self {
        Self {
            complete: true,
            needs_retry: false,
            must_delete: true,
        }
    }

    const fn skipped() -> Self {
        Self {
            complete: false,
            needs_retry: false,
            must_delete: false,
        }
    }

    const fn retracted() -> Self {
        Self {
            complete: false,
            needs_retry: true,
            must_delete: false,
        }
    }
}

/// What one pass did to the region's data objects, as observed at the delete
/// site and in the `GcReport` it returned.
#[derive(Clone, Debug, Default)]
struct PassFacts {
    /// Existing objects the oracle expected the pass to delete.
    expected: BTreeSet<GcObject>,
    /// Objects the pass really deleted (all of them existed).
    actual: BTreeSet<GcObject>,
    /// Objects the pass asked storage to delete although no such object existed.
    /// These are legitimate no-ops of the `0..=v` index fan-out: never a safety
    /// violation, and never a deletion either.
    phantom: BTreeSet<GcObject>,
    /// `GcReport::deleted_files` for the region.
    reported_files: BTreeSet<FileId>,
    /// `GcReport::deleted_indexes` for the region. Lossy by design, see
    /// `test_gc_physical_delete_oracle_index_fanout`.
    reported_indexes: BTreeSet<(FileId, IndexVersion)>,
    /// Narthex artifact directories the oracle expected the pass to remove.
    expected_narthex: BTreeSet<FileId>,
    /// Narthex artifact directories the pass really handed to a recursive
    /// delete. This is the unit the store sees, so it is checked on its own.
    deleted_narthex: BTreeSet<FileId>,
}

/// The oracle's verdict for one object in one pass.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Verdict {
    /// The pass may physically delete the object.
    Delete,
    /// The pass must keep the object, for the quoted reason.
    Keep(&'static str),
}

/// Every protection surface one pass may consult, observed right before it.
struct PassInput {
    knobs: RoundKnobs,
    /// Wall clock captured before the pass.
    started_at_ms: i64,
    /// Wall clock captured after the pass.
    finished_at_ms: i64,
    /// Manifest SST id -> index version its metadata records.
    manifest_index: BTreeMap<FileId, Option<IndexVersion>>,
    /// `removed_files` records as `(removed_at, members)`.
    removed: Vec<(i64, HashSet<RemovedFile>)>,
    /// Temporary reader references handed to the pass.
    tmp_refs: HashSet<(FileId, Option<IndexVersion>)>,
    /// File ids the hook reported as protected.
    hook_protected: HashSet<FileId>,
    /// Objects present in the region directory, with their modification time.
    listed: BTreeMap<GcObject, Option<i64>>,
}

impl PassInput {
    /// Whether a record at `removed_at` has certainly passed the lingering
    /// period, or certainly not. A record that could go either way would make
    /// the expectation a guess, so the oracle refuses to run instead.
    fn record_expired(&self, removed_at: i64) -> bool {
        let Some(linger) = self.knobs.linger else {
            // `lingering_time = None` waits for no known record.
            return true;
        };
        let linger = linger.as_millis() as i64;
        let expired = removed_at < self.started_at_ms - linger;
        let young = removed_at >= self.finished_at_ms - linger;
        assert!(
            expired || young,
            "ambiguous linger: record removed_at={removed_at} linger={linger}ms, pass window [{}, {}]",
            self.started_at_ms,
            self.finished_at_ms
        );
        expired
    }

    /// Whether an unknown object is certainly older than the unknown-file
    /// cutoff. Same rule as [`Self::record_expired`]: no guessing.
    fn object_expired(&self, mtime: Option<i64>) -> bool {
        let linger = self.knobs.unknown_linger.as_millis() as i64;
        // A missing modification time can never be proven old.
        let Some(mtime) = mtime else {
            return false;
        };
        let expired = mtime < self.started_at_ms - linger;
        let young = mtime >= self.finished_at_ms - linger;
        assert!(
            expired || young,
            "ambiguous object age: mtime={mtime} linger={linger}ms, pass window [{}, {}]",
            self.started_at_ms,
            self.finished_at_ms
        );
        expired
    }

    /// Whether any record covers the object, and whether one of those records
    /// has passed its lingering period. Parquet objects are tracked per file id
    /// (an outdated index record also makes the id tracked), Puffin objects per
    /// exact `(file id, version)`; this mirrors the `is_linger`/`is_eligible`
    /// lookups of `filter_deletable_files` and of the fast-mode `in_use` check.
    /// A narthex part is tracked per file id as well: the manifest records the
    /// artifact, never one part.
    fn known_and_expired(&self, object: GcObject) -> (bool, bool) {
        let mut known = false;
        let mut expired = false;
        for (removed_at, files) in &self.removed {
            let hit = match object {
                GcObject::Sst(id) | GcObject::Narthex(id, _) => {
                    files.iter().any(|file| file.file_id() == id)
                }
                GcObject::Index(id, version) => files.contains(&RemovedFile::Index(id, version)),
            };
            if hit {
                known = true;
                expired |= self.record_expired(*removed_at);
            }
        }
        (known, expired)
    }

    /// The safety contract: may this pass delete `object`?
    fn verdict(&self, object: GcObject) -> Verdict {
        if let Some(recorded) = self.manifest_index.get(&object.file_id()) {
            let protected = match object {
                // A narthex artifact is in the manifest as a whole, so any entry
                // for the id protects every part of it.
                GcObject::Sst(_) | GcObject::Narthex(_, _) => true,
                GcObject::Index(_, version) => *recorded == Some(version),
            };
            if protected {
                return Verdict::Keep("in manifest");
            }
        }
        let referenced = match object {
            GcObject::Sst(id) | GcObject::Narthex(id, _) => {
                self.tmp_refs.iter().any(|(tmp_id, _)| *tmp_id == id)
            }
            GcObject::Index(id, version) => self.tmp_refs.contains(&(id, Some(version))),
        };
        if referenced {
            return Verdict::Keep("temporary reader reference");
        }
        if self.hook_protected.contains(&object.file_id()) {
            return Verdict::Keep("hook protection");
        }
        let (known, expired) = self.known_and_expired(object);
        if known {
            return if expired {
                Verdict::Delete
            } else {
                Verdict::Keep("still lingering")
            };
        }
        if !self.knobs.full_listing {
            return Verdict::Keep("a fast pass only deletes tracked files");
        }
        let mtime = *self.listed.get(&object).unwrap_or_else(|| {
            panic!(
                "object {} is not listed, the oracle cannot judge its age",
                object.describe()
            )
        });
        if self.object_expired(mtime) {
            Verdict::Delete
        } else {
            Verdict::Keep("young unknown object")
        }
    }

    /// The artifact-level verdict for the narthex artifact of `id`.
    ///
    /// Unlike an SST, an artifact is judged per part: `filter_deletable_files`
    /// ANDs the per-object decision of every listed part into one eligibility
    /// flag, so a single part that is in use, in the manifest, still lingering,
    /// hook protected, or younger than the unknown-file grace keeps the whole
    /// artifact. Deleting a subset of the parts is not an option: the removal
    /// site drops the artifact directory.
    fn narthex_verdict(&self, id: FileId) -> Verdict {
        let parts: Vec<GcObject> = self
            .listed
            .keys()
            .copied()
            .filter(|object| matches!(object, GcObject::Narthex(part_id, _) if *part_id == id))
            .collect();
        if parts.is_empty() {
            // No object of the artifact is listed, so there is nothing to judge
            // part by part: only the id-level protections apply.
            if self.manifest_index.contains_key(&id) {
                return Verdict::Keep("in manifest");
            }
            if self.tmp_refs.iter().any(|(tmp_id, _)| *tmp_id == id) {
                return Verdict::Keep("temporary reader reference");
            }
            if self.hook_protected.contains(&id) {
                return Verdict::Keep("hook protection");
            }
            return Verdict::Delete;
        }
        for part in parts {
            if let Verdict::Keep(reason) = self.verdict(part) {
                return Verdict::Keep(reason);
            }
        }
        Verdict::Delete
    }

    /// The `RemovedFile`s the pass is expected to hand to the physical deleter.
    fn candidates(&self) -> Vec<RemovedFile> {
        let mut candidates = Vec::new();
        if self.knobs.full_listing {
            let mut narthex_ids = BTreeSet::new();
            for object in self.listed.keys() {
                if let GcObject::Narthex(id, _) = *object {
                    narthex_ids.insert(id);
                    continue;
                }
                if self.verdict(*object) == Verdict::Delete {
                    candidates.push(match *object {
                        GcObject::Sst(id) => RemovedFile::File(id, None),
                        GcObject::Index(id, version) => RemovedFile::Index(id, version),
                        GcObject::Narthex(_, _) => unreachable!("handled above"),
                    });
                }
            }
            // One `RemovedFile::Narthex` per artifact, not per part.
            for id in narthex_ids {
                if self.narthex_verdict(id) == Verdict::Delete {
                    candidates.push(RemovedFile::Narthex(id));
                }
            }
        } else {
            for (removed_at, files) in &self.removed {
                if !self.record_expired(*removed_at) {
                    continue;
                }
                for file in files {
                    let deletable = match file {
                        RemovedFile::Narthex(id) => self.narthex_verdict(*id) == Verdict::Delete,
                        RemovedFile::File(id, _) => {
                            self.verdict(GcObject::Sst(*id)) == Verdict::Delete
                        }
                        RemovedFile::Index(id, version) => {
                            self.verdict(GcObject::Index(*id, *version)) == Verdict::Delete
                        }
                    };
                    if deletable {
                        candidates.push(file.clone());
                    }
                }
            }
        }
        candidates
    }
}

/// Model of the physical deletion expansion: deleting an SST also deletes every
/// index object up to the version recorded with the removal (`None` = version 0).
/// A narthex artifact is the opposite case: the delete site removes the whole
/// directory at once, so the expectation is every part object it holds, not the
/// directory path the store sees.
fn expand_candidates(
    candidates: &[RemovedFile],
    listed: &BTreeMap<GcObject, Option<i64>>,
    region_id: RegionId,
    layer: &AccessLayer,
) -> BTreeSet<String> {
    let mut paths = BTreeSet::new();
    for candidate in candidates {
        match candidate {
            RemovedFile::Narthex(id) => {
                paths.extend(
                    listed
                        .keys()
                        .filter(|object| {
                            matches!(object, GcObject::Narthex(part_id, _) if part_id == id)
                        })
                        .map(|object| object_path(*object, region_id, layer)),
                );
            }
            RemovedFile::File(id, version) => {
                paths.insert(object_path(GcObject::Sst(*id), region_id, layer));
                for index_version in 0..=version.unwrap_or(0) {
                    paths.insert(object_path(
                        GcObject::Index(*id, index_version),
                        region_id,
                        layer,
                    ));
                }
            }
            RemovedFile::Index(id, version) => {
                paths.insert(object_path(
                    GcObject::Index(*id, *version),
                    region_id,
                    layer,
                ));
            }
        }
    }
    paths
}

/// The artifact directories (not their parts) the candidates name.
fn narthex_artifacts(candidates: &[RemovedFile]) -> BTreeSet<FileId> {
    candidates
        .iter()
        .filter_map(|candidate| match candidate {
            RemovedFile::Narthex(id) => Some(*id),
            RemovedFile::File(_, _) | RemovedFile::Index(_, _) => None,
        })
        .collect()
}

/// Ids a raw delete path asks the store to remove: an SST object, one index
/// object (which `GcReport` never reports as a file), or an artifact directory.
fn attempted_file_id(path: &str) -> Option<FileId> {
    if let Some(id) = narthex_artifact_dir(path) {
        return Some(id);
    }
    match location::parse_file_id_type_from_path(path).ok()? {
        (file_id, FileType::Parquet) => Some(file_id),
        (_, FileType::Puffin(_)) => None,
    }
}

/// Index objects a raw delete path asks the store to remove.
fn attempted_index(path: &str) -> Option<(FileId, IndexVersion)> {
    match location::parse_file_id_type_from_path(path).ok()? {
        (file_id, FileType::Puffin(version)) => Some((file_id, version)),
        (_, FileType::Parquet) => None,
    }
}

/// Model of what a pass reports through `GcReport`, derived from the candidates
/// rather than from the report itself.
fn report_view(candidates: &[RemovedFile]) -> (BTreeSet<FileId>, BTreeSet<(FileId, IndexVersion)>) {
    let mut files = BTreeSet::new();
    let mut indexes = BTreeSet::new();
    for candidate in candidates {
        if let RemovedFile::Narthex(id) | RemovedFile::File(id, _) = candidate {
            files.insert(*id);
        }
        if let Some(version) = candidate.index_version() {
            indexes.insert((candidate.file_id(), version));
        }
    }
    (files, indexes)
}

/// One scenario's region, its model inventory and the recorder of what GC
/// physically did to it.
struct GcOracle {
    _env: TestEnv,
    engine: MitoEngine,
    region: MitoRegionRef,
    region_id: RegionId,
    hook: Arc<OracleHook>,
    faults: Arc<LayerFaults>,
    log: Arc<GcDeleteLog>,
    /// Objects the oracle believes exist in the region directory.
    inventory: BTreeSet<GcObject>,
    /// Temporary reader references the next pass is handed.
    tmp_refs: HashSet<(FileId, Option<IndexVersion>)>,
    /// Objects deleted by any pass so far.
    deleted_ever: BTreeSet<GcObject>,
    /// Facts of the pass that ran last, for tests that pin one pass closely.
    last_pass: Option<PassFacts>,
    /// Branch labels folded from every pass so far. The seeded scenario's
    /// coverage table is reconciled with this, so a seed that drifts onto another
    /// branch fails instead of quietly covering the same one again.
    branches: ScenarioBranches,
    /// Scenario name and seed, so a failure is replayable.
    scenario: String,
    seed: u64,
    /// Operations applied so far, for failure messages.
    history: Vec<String>,
}

impl GcOracle {
    async fn new(scenario: &str, seed: u64) -> Self {
        let log = GcDeleteLog::new();
        let faults = Arc::new(LayerFaults::default());
        let hook = Arc::new(OracleHook::default());
        let mut env = TestEnv::new()
            .await
            .with_mock_layer(recording_layer(log.clone(), faults.clone()));
        let engine = env
            .create_engine_with_plugins(
                MitoConfig {
                    // No background compaction: this oracle only observes GC.
                    schedule_compaction_after_edit: false,
                    // `enable` keeps `removed_files` records in the manifest.
                    gc: GcConfig {
                        enable: true,
                        ..Default::default()
                    },
                    ..Default::default()
                },
                Plugins::new(),
            )
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
        engine
            .set_region_role(region_id, RegionRole::Leader)
            .unwrap();
        let oracle = GcOracle {
            _env: env,
            engine,
            region,
            region_id,
            hook,
            faults,
            log,
            inventory: BTreeSet::new(),
            tmp_refs: HashSet::new(),
            deleted_ever: BTreeSet::new(),
            last_pass: None,
            branches: ScenarioBranches::default(),
            scenario: scenario.to_string(),
            seed,
            history: Vec::new(),
        };
        oracle.verify_step("after create").await;
        oracle
    }

    fn access_layer(&self) -> &AccessLayer {
        &self.region.access_layer
    }

    fn context(&self) -> String {
        format!(
            "[{} seed=0x{:X}] operations: {}",
            self.scenario,
            self.seed,
            self.history.join(" -> ")
        )
    }

    /// Prefix of this region's data directory.
    fn data_prefix(&self) -> String {
        normalize_object_path(&self.access_layer().build_region_dir(self.region_id))
    }

    /// Whether the path is one of the region's data objects. The region's
    /// manifest directory (`<region>/manifest/`) is bookkeeping and is excluded,
    /// but an artifact directory is data, including its own `manifest.json`: only
    /// the manifest *directory* is filtered out, not every path that happens to
    /// contain `manifest`.
    fn is_data_path(&self, path: &str) -> bool {
        let path = normalize_object_path(path);
        let region_dir = self.data_prefix().trim_end_matches('/').to_string();
        let manifest_dir = normalize_object_path(&manifest_dir(&format!("{region_dir}/")));
        if path.starts_with(&manifest_dir) {
            return false;
        }
        path == region_dir || path.starts_with(&format!("{region_dir}/"))
    }

    /// Objects present in the region directory, with their modification time.
    async fn list_objects(&self) -> BTreeMap<GcObject, Option<i64>> {
        let entries: Vec<Entry> = self
            .access_layer()
            .object_store()
            .lister_with(&self.data_prefix())
            .recursive(true)
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        let mut objects = BTreeMap::new();
        for entry in entries {
            // The listing also yields directories and the region manifest.
            if entry.metadata().is_dir() || !self.is_data_path(entry.path()) {
                continue;
            }
            let Some(object) = object_of_path(entry.path()) else {
                continue;
            };
            objects.insert(
                object,
                entry
                    .metadata()
                    .last_modified()
                    .map(|ts| ts.into_inner().as_millisecond()),
            );
        }
        objects
    }

    fn inventory(&self) -> &BTreeSet<GcObject> {
        &self.inventory
    }

    fn deleted_ever(&self) -> &BTreeSet<GcObject> {
        &self.deleted_ever
    }

    /// What the pass that ran last did, as observed at the delete site.
    fn last_pass(&self) -> &PassFacts {
        self.last_pass
            .as_ref()
            .expect("no pass ran before its facts were inspected")
    }

    /// The branch labels the passes so far have taken, folded from what each pass
    /// really did rather than from what the test asked for.
    fn branches(&self) -> ScenarioBranches {
        self.branches
    }

    /// Checks the whole inventory after every operation: the object set the
    /// oracle believes in must be exactly the object set storage holds, and no
    /// pass may have deleted anything outside a GC pass.
    async fn verify_step(&self, context: &str) {
        let outside: Vec<String> = self
            .log
            .deletes()
            .into_iter()
            .filter(|record| record.round == NO_ROUND && self.is_data_path(&record.path))
            .map(|record| record.path)
            .collect();
        assert!(
            outside.is_empty(),
            "{}: data objects were deleted outside a GC pass {context}: {outside:?}",
            self.context()
        );
        let listed: BTreeSet<GcObject> = self.list_objects().await.into_keys().collect();
        assert_eq!(
            self.inventory,
            listed,
            "{}: model inventory differs from storage {context}",
            self.context()
        );
    }

    async fn write_object(&mut self, object: GcObject) {
        let path = object_path(object, self.region_id, self.access_layer());
        self.access_layer()
            .object_store()
            .write(&path, b"oracle object".as_slice())
            .await
            .unwrap();
        self.history.push(format!("write {}", object.describe()));
        assert!(
            self.inventory.insert(object),
            "{}: duplicate write of {}",
            self.context(),
            object.describe()
        );
        self.verify_step("after write").await;
    }

    fn file_meta(&self, id: FileId, index_version: Option<IndexVersion>) -> FileMeta {
        let mut meta = FileMeta {
            region_id: self.region_id,
            file_id: id,
            ..Default::default()
        };
        if let Some(version) = index_version {
            meta.available_indexes = smallvec::smallvec![IndexType::InvertedIndex];
            meta.index_version = version;
        }
        meta
    }

    async fn publish_edit(&mut self, edit: RegionEdit, label: String) {
        self.engine.edit_region(self.region_id, edit).await.unwrap();
        self.history.push(label);
        self.verify_step("after manifest edit").await;
    }

    /// Adds `id` to the manifest with an optional index version (which also
    /// exercises index-version replacement when the file was already there).
    async fn publish_add(&mut self, id: FileId, index_version: Option<IndexVersion>) {
        let meta = self.file_meta(id, index_version);
        self.publish_edit(
            RegionEdit {
                files_to_add: vec![meta],
                ..Default::default()
            },
            format!("publish-add({id}, {index_version:?})"),
        )
        .await;
    }

    /// Removes `id` from the manifest, recording an expel time. `None` uses the
    /// wall clock (a young record); `Some(0)` is long past any linger period.
    async fn publish_remove(
        &mut self,
        id: FileId,
        index_version: Option<IndexVersion>,
        removed_at: Option<i64>,
    ) {
        let meta = self.file_meta(id, index_version);
        self.publish_edit(
            RegionEdit {
                files_to_remove: vec![meta],
                timestamp_ms: removed_at,
                ..Default::default()
            },
            format!("publish-remove({id}, {index_version:?}, at={removed_at:?})"),
        )
        .await;
    }

    /// Writes the whole artifact directory of `id`: `manifest.json` plus `parts`
    /// parts (`part-0.samples`, ...). The parts are written in order, so the
    /// caller can make one part younger than the rest.
    async fn write_narthex_artifact(&mut self, id: FileId, parts: u32) {
        for object in std::iter::once(GcObject::Narthex(id, NarthexObject::Manifest))
            .chain((0..parts).map(|part| GcObject::Narthex(id, NarthexObject::Part(part))))
        {
            self.write_object(object).await;
        }
    }

    /// Manifest metadata of a narthex artifact with `parts` parts. A non-Parquet
    /// entry is what makes [`RegionManifestBuilder`] record a
    /// `RemovedFile::Narthex` when the artifact is removed.
    fn narthex_meta(&self, id: FileId, parts: u32) -> FileMeta {
        FileMeta {
            region_id: self.region_id,
            file_id: id,
            format: FileFormat::NarthexSamples(Box::new(NarthexSamplesMeta {
                use_file_sequence: false,
                manifest_size: b"narthex".len() as u64,
                parts: (0..parts)
                    .map(|_| NarthexPartMeta {
                        file_size: b"oracle object".len() as u64,
                    })
                    .collect(),
            })),
            ..Default::default()
        }
    }

    /// Publishes a narthex artifact, so the manifest lists the id (and therefore
    /// protects every part of it).
    async fn publish_add_narthex(&mut self, id: FileId, parts: u32) {
        let meta = self.narthex_meta(id, parts);
        self.publish_edit(
            RegionEdit {
                files_to_add: vec![meta],
                ..Default::default()
            },
            format!("publish-add-narthex({id}, {parts} parts)"),
        )
        .await;
    }

    /// Removes a narthex artifact. Because the manifest entry is not Parquet,
    /// the removal is recorded as `RemovedFile::Narthex`, i.e. as the whole
    /// artifact and never as an SST object of the same id.
    async fn publish_remove_narthex(&mut self, id: FileId, removed_at: Option<i64>) {
        let meta = self.narthex_meta(id, 1);
        self.publish_edit(
            RegionEdit {
                files_to_remove: vec![meta],
                timestamp_ms: removed_at,
                ..Default::default()
            },
            format!("publish-remove-narthex({id}, at={removed_at:?})"),
        )
        .await;
    }

    fn set_tmp_refs(&mut self, refs: impl IntoIterator<Item = (FileId, Option<IndexVersion>)>) {
        self.tmp_refs = refs.into_iter().collect();
        let mut refs = self.tmp_refs.iter().copied().collect::<Vec<_>>();
        refs.sort_unstable();
        self.history.push(format!("tmp-refs {refs:?}"));
    }

    fn set_hook_protected(&mut self, ids: impl IntoIterator<Item = FileId>) {
        let ids = ids.into_iter().collect::<BTreeSet<_>>();
        self.hook.protect(ids.iter().copied());
        self.history.push(format!("hook-protects {ids:?}"));
    }

    async fn arm_pending_tails(&mut self) {
        set_pending_tails(&self.engine, self.region_id, vec![tail(1, 0, 1)]).await;
        self.history.push("arm pending tails".to_string());
    }

    async fn clear_pending_tails(&mut self) {
        set_pending_tails(&self.engine, self.region_id, vec![]).await;
        self.history.push("clear pending tails".to_string());
    }

    /// Writes a delta of a version the worker does not know about, which the
    /// entry check must see as a newer manifest.
    async fn arm_newer_manifest(&mut self) -> String {
        let store = {
            let manager = self.region.manifest_ctx.manifest_manager.read().await;
            manager.store()
        };
        let version = self.region.manifest_ctx.manifest_version().await;
        let path = gen_path(
            store.manifest_dir(),
            &delta_file(version + 1),
            CompressionType::Uncompressed,
        );
        self.access_layer()
            .object_store()
            .write(&path, b"".as_slice())
            .await
            .unwrap();
        self.history
            .push(format!("arm newer manifest v{}", version + 1));
        normalize_object_path(&path)
    }

    async fn clear_newer_manifest(&mut self, path: &str) {
        self.access_layer()
            .object_store()
            .delete(path)
            .await
            .unwrap();
        self.history.push("clear newer manifest".to_string());
    }

    fn arm_manifest_list_failure(&mut self) {
        self.faults
            .fail_manifest_lists
            .store(true, Ordering::SeqCst);
        self.history.push("arm manifest list failure".to_string());
    }

    fn clear_manifest_list_failure(&mut self) {
        self.faults
            .fail_manifest_lists
            .store(false, Ordering::SeqCst);
        self.history.push("clear manifest list failure".to_string());
    }

    /// Waits until every object in the region is strictly older than the wall
    /// clock, so a pass with no unknown-file grace period provably sees them as
    /// mature instead of relying on timing luck.
    async fn wait_until_mature(&self) {
        loop {
            let newest = self
                .list_objects()
                .await
                .values()
                .filter_map(|mtime| *mtime)
                .max();
            match newest {
                Some(newest) if newest < chrono::Utc::now().timestamp_millis() => return,
                Some(_) => tokio::time::sleep(Duration::from_millis(1)).await,
                None => return,
            }
        }
    }

    async fn build_worker(
        &self,
        knobs: RoundKnobs,
        wiring: PassWiring,
        manifest_version: u64,
    ) -> LocalGcWorker {
        let mut file_ref_manifest = FileRefsManifest {
            file_refs: HashMap::new(),
            manifest_version: HashMap::new(),
            cross_region_refs: HashMap::new(),
        };
        if !self.tmp_refs.is_empty() {
            file_ref_manifest.file_refs.insert(
                self.region_id,
                self.tmp_refs
                    .iter()
                    .map(|(id, version)| FileRef::new(self.region_id, *id, *version))
                    .collect(),
            );
        }
        if !wiring.omit_manifest_version {
            file_ref_manifest
                .manifest_version
                .insert(self.region_id, manifest_version);
        }
        LocalGcWorker::try_new(
            self.region.access_layer.clone(),
            Some(self.engine.cache_manager()),
            BTreeMap::from([(self.region_id, Some(self.region.clone()))]),
            knobs.gc_config(),
            file_ref_manifest,
            &self.engine.gc_limiter(),
            knobs.full_listing,
            Some(self.hook.clone() as RegionHookRef),
        )
        .await
        .unwrap()
    }

    /// Runs one pass and checks it against the oracle at the real delete site.
    async fn run_pass(
        &mut self,
        knobs: RoundKnobs,
        wiring: PassWiring,
        expectation: PassExpectation,
        what: &str,
    ) -> GcReport {
        if knobs.unknown_linger == MATURE_UNKNOWN_LINGER {
            self.wait_until_mature().await;
        }
        // Observe every input the pass will consult, before it runs.
        let manifest = self.region.manifest_ctx.manifest().await;
        let manifest_index: BTreeMap<FileId, Option<IndexVersion>> = manifest
            .files
            .iter()
            .map(|(id, meta)| (*id, meta.index_version()))
            .collect();
        let removed: Vec<(i64, HashSet<RemovedFile>)> = manifest
            .removed_files
            .removed_files
            .iter()
            .map(|record| (record.removed_at, record.files.clone()))
            .collect();
        let manifest_version = manifest.manifest_version;
        let hook_protected = self.hook.protected_ids();
        let tmp_refs = self.tmp_refs.clone();
        let listed = self.list_objects().await;

        let started_at_ms = chrono::Utc::now().timestamp_millis();
        let round = self.log.begin_round();
        let result = {
            let worker = self.build_worker(knobs, wiring, manifest_version).await;
            worker.run().await
        };
        let finished_at_ms = chrono::Utc::now().timestamp_millis();
        self.log.end_round();
        let report =
            result.unwrap_or_else(|err| panic!("{}: pass `{what}` failed: {err}", self.context()));
        let input = PassInput {
            knobs,
            started_at_ms,
            finished_at_ms,
            manifest_index,
            removed,
            tmp_refs,
            hook_protected,
            listed,
        };
        let candidates = input.candidates();
        let raw_expected = expand_candidates(
            &candidates,
            &input.listed,
            self.region_id,
            self.access_layer(),
        );
        let expected_narthex = narthex_artifacts(&candidates);
        // The artifact directories the listing holds, so a recursive delete of an
        // artifact can be expanded into the part objects it removed.
        let mut narthex_parts: BTreeMap<FileId, Vec<String>> = BTreeMap::new();
        for object in input.listed.keys() {
            if let GcObject::Narthex(id, _) = *object {
                narthex_parts.entry(id).or_default().push(object_path(
                    *object,
                    self.region_id,
                    self.access_layer(),
                ));
            }
        }
        // `RemovedFile::File(id, v)` fans out into every index version `0..=v`,
        // so a pass may hand the store a path for an object that never existed
        // (a delete of an absent key is a no-op). The oracle compares only
        // objects that really exist and checks the no-op attempts separately.
        let existing_paths: BTreeSet<String> = input
            .listed
            .keys()
            .map(|object| object_path(*object, self.region_id, self.access_layer()))
            .collect();
        let expected_paths: BTreeSet<String> = if expectation.complete {
            raw_expected
                .intersection(&existing_paths)
                .cloned()
                .collect()
        } else {
            BTreeSet::new()
        };
        let records = self.log.deletes();
        let in_round = |record: &DeleteRecord| record.round == round;
        let mut deleted_paths: BTreeSet<String> = BTreeSet::new();
        let mut attempted_paths: BTreeSet<String> = BTreeSet::new();
        let mut deleted_narthex: BTreeSet<FileId> = BTreeSet::new();
        for record in records
            .iter()
            .filter(|record| in_round(record) && self.is_data_path(&record.path))
        {
            // Every path the pass handed to the store, before it is mapped to the
            // objects it covers. The over-report check below compares the report
            // against this set, because that is what "tried to delete" means: an
            // artifact directory delete attempts the artifact, not one part.
            attempted_paths.insert(record.path.clone());
            match narthex_artifact_dir(&record.path) {
                // The artifact removal is one recursive delete naming the
                // directory: it covers every part object the directory held.
                Some(id) => {
                    deleted_narthex.insert(id);
                    deleted_paths.extend(narthex_parts.get(&id).cloned().unwrap_or_default());
                }
                None => {
                    deleted_paths.insert(record.path.clone());
                }
            }
        }
        let actual_paths: BTreeSet<String> = deleted_paths
            .intersection(&existing_paths)
            .cloned()
            .collect();
        let phantom_deletes: BTreeSet<String> =
            deleted_paths.difference(&existing_paths).cloned().collect();
        let failed_deletes: Vec<(String, Option<String>)> = records
            .iter()
            .filter(|record| in_round(record) && self.is_data_path(&record.path))
            .filter(|record| record.error.is_some())
            .map(|record| (record.path.clone(), record.error.clone()))
            .collect();
        let listed_round: Vec<String> = self
            .log
            .lists()
            .into_iter()
            .filter(|list| list.round == round && self.is_data_path(&list.path))
            .map(|list| list.path)
            .collect();

        // S1: everything that was physically deleted in this pass must be free of
        // every protection at the moment of the pass.
        let mut unsafe_deletes = Vec::new();
        for path in &actual_paths {
            if expected_paths.contains(path) {
                // The model itself approved this object; the two-sided set check
                // below still holds it to exactly the expected deletion.
                continue;
            }
            let object = object_of_path(path).unwrap_or_else(|| {
                panic!(
                    "{}: pass `{what}` deleted an object the oracle cannot identify: {path}",
                    self.context()
                )
            });
            if let Verdict::Keep(reason) = input.verdict(object) {
                unsafe_deletes.push(format!(
                    "{} ({path}) was physically deleted but must be kept: {reason}",
                    object.describe()
                ));
            }
        }
        assert!(
            unsafe_deletes.is_empty(),
            "{}: pass `{what}` violated the delete safety contract:\n  {}\n",
            self.context(),
            unsafe_deletes.join("\n  ")
        );
        assert!(
            failed_deletes.is_empty(),
            "{}: pass `{what}` failed to delete data objects: {failed_deletes:?}",
            self.context(),
        );
        // A pass hands the store at most three deleters for the region's data
        // objects: the SSTs of the removed files, the index versions of their
        // `0..=v` fan-out, and the explicitly recorded index removals. A batch
        // per object would grow this count with the inventory. Each narthex
        // artifact is removed by its own recursive directory delete, so it adds
        // exactly one batch per artifact.
        let batches: BTreeSet<usize> = records
            .iter()
            .filter(|record| in_round(record) && self.is_data_path(&record.path))
            .map(|record| record.batch)
            .collect();
        let allowed_batches = 3 + deleted_narthex.len();
        assert!(
            batches.len() <= allowed_batches,
            "{}: pass `{what}` used {} delete batches ({batches:?}) for the region's data objects, at most {allowed_batches} are expected",
            self.context(),
            batches.len()
        );
        // Artifact directories are the unit the store sees, so they are compared
        // on their own: a pass may neither remove an artifact the oracle keeps
        // (even when only one of its parts is still protected) nor leave one the
        // oracle expects to be gone.
        assert_eq!(
            deleted_narthex,
            expected_narthex,
            "{}: pass `{what}` narthex artifact delete set mismatch\n  removed but not expected: {:?}\n  expected but not removed: {:?}\n",
            self.context(),
            deleted_narthex
                .difference(&expected_narthex)
                .collect::<Vec<_>>(),
            expected_narthex
                .difference(&deleted_narthex)
                .collect::<Vec<_>>(),
        );
        // Two-sided check: no object may be deleted unexpectedly, and every
        // candidate the oracle expects must really be gone. Without the second
        // half a "never delete anything" implementation would pass every safety
        // assertion above.
        assert_eq!(
            actual_paths,
            expected_paths,
            "{}: pass `{what}` physical delete set mismatch\n  deleted but not expected: {:?}\n  expected but not deleted: {:?}\n",
            self.context(),
            actual_paths.difference(&expected_paths).collect::<Vec<_>>(),
            expected_paths.difference(&actual_paths).collect::<Vec<_>>(),
        );
        // Deletes of absent objects are no-ops, but a pass that withholds work
        // must not issue any of them either.
        assert!(
            expectation.complete || phantom_deletes.is_empty(),
            "{}: pass `{what}` issued deletes for absent objects although it must withhold work: {phantom_deletes:?}",
            self.context(),
        );
        // A complete pass may only issue a no-op delete for an object that its
        // own `0..=v` expansion names: deleting an absent key outside the
        // expectation is as unmodelled as deleting a real one.
        let absent_expected: BTreeSet<String> =
            raw_expected.difference(&existing_paths).cloned().collect();
        if expectation.complete {
            assert!(
                phantom_deletes.is_subset(&absent_expected),
                "{}: pass `{what}` deleted objects that neither exist nor appear in the expected `0..=v` expansion: {:?}",
                self.context(),
                phantom_deletes
                    .difference(&absent_expected)
                    .collect::<Vec<_>>(),
            );
        }
        // Positive control for the exactness check above: a completed pass that
        // is required to delete must have something to delete and must really
        // delete it, otherwise an expectation that came out empty would let a
        // pass that does nothing at all through.
        if expectation.must_delete {
            assert!(
                expectation.complete && !expectation.needs_retry,
                "{}: pass `{what}` is marked must-delete but does not run to completion",
                self.context(),
            );
            assert!(
                !expected_paths.is_empty(),
                "{}: pass `{what}` is marked must-delete but the oracle expects no existing object to be deleted, so the exactness check would be vacuous",
                self.context(),
            );
            assert!(
                !actual_paths.is_empty(),
                "{}: pass `{what}` is marked must-delete but physically deleted nothing",
                self.context(),
            );
        }
        // The report must describe the same deletion, not a different one.
        let (expected_files, expected_indexes) = if expectation.complete {
            report_view(&candidates)
        } else {
            (BTreeSet::new(), BTreeSet::new())
        };
        let reported_files: BTreeSet<FileId> = report
            .deleted_files
            .get(&self.region_id)
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .collect();
        let reported_indexes: BTreeSet<(FileId, IndexVersion)> = report
            .deleted_indexes
            .get(&self.region_id)
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .collect();
        assert_eq!(
            reported_files,
            expected_files,
            "{}: pass `{what}` GcReport::deleted_files does not match the physical deletion",
            self.context()
        );
        assert_eq!(
            reported_indexes,
            expected_indexes,
            "{}: pass `{what}` GcReport::deleted_indexes does not match the physical deletion",
            self.context()
        );
        // Reverse direction of the report check, against the delete site instead
        // of the model: whatever a pass reports must be something it really tried
        // to delete in this very pass. A report entry for an object the pass never
        // handed to the store is an over-report, and would make a consumer (for
        // example the dropping procedure counting reclaimed objects) believe work
        // happened that did not.
        //
        // The relation is `report ⊆ physical attempts`, not equality, because
        // `GcReport` is allowed to describe less than the delete site does:
        // * `RemovedFile::File(id, None)` (listing mode) reports the file id and
        //   physically also tries index version 0;
        // * `RemovedFile::File(id, Some(v))` reports `(id, v)` while the delete
        //   site fans out over `0..=v`, so it reports one of several attempted
        //   versions and never the whole fan-out;
        // * `RemovedFile::Narthex(id)` reports the file id although the delete
        //   site removes an artifact directory and never an SST object of that
        //   id. That is why a narthex directory delete counts as an attempt for
        //   the id, and an index-only delete does not.
        let attempted_files: BTreeSet<FileId> = attempted_paths
            .iter()
            .filter_map(|path| attempted_file_id(path))
            .collect();
        let attempted_indexes: BTreeSet<(FileId, IndexVersion)> = attempted_paths
            .iter()
            .filter_map(|path| attempted_index(path))
            .collect();
        assert!(
            reported_files.is_subset(&attempted_files),
            "{}: pass `{what}` reported files it never tried to delete\n  reported but not attempted: {:?}\n  attempted: {attempted_files:?}",
            self.context(),
            reported_files
                .difference(&attempted_files)
                .collect::<Vec<_>>(),
        );
        assert!(
            reported_indexes.is_subset(&attempted_indexes),
            "{}: pass `{what}` reported index versions it never tried to delete\n  reported but not attempted: {:?}\n  attempted: {attempted_indexes:?}",
            self.context(),
            reported_indexes
                .difference(&attempted_indexes)
                .collect::<Vec<_>>(),
        );
        // Entry checks and the post-lookup recheck must never touch the data
        // directory: no listing, no delete.
        if expectation.complete {
            assert_eq!(
                !listed_round.is_empty(),
                knobs.full_listing,
                "{}: pass `{what}` listed the region directory in the wrong mode: {listed_round:?}",
                self.context()
            );
        } else {
            assert!(
                listed_round.is_empty(),
                "{}: pass `{what}` listed the region directory although it must skip the region: {listed_round:?}",
                self.context()
            );
            assert!(
                !candidates.is_empty(),
                "{}: pass `{what}` was expected to withhold work, but the oracle sees no candidate at all",
                self.context(),
            );
        }
        assert_eq!(
            report.need_retry_regions.contains(&self.region_id),
            expectation.needs_retry,
            "{}: pass `{what}` needs_retry mismatch: {:?}",
            self.context(),
            report.need_retry_regions
        );
        assert_eq!(
            report.processed_regions.contains(&self.region_id),
            expectation.complete && !expectation.needs_retry,
            "{}: pass `{what}` processed_regions mismatch: {:?}",
            self.context(),
            report.processed_regions
        );

        self.last_pass = Some(PassFacts {
            expected: expected_paths
                .iter()
                .filter_map(|path| object_of_path(path))
                .collect(),
            actual: actual_paths
                .iter()
                .filter_map(|path| object_of_path(path))
                .collect(),
            phantom: phantom_deletes
                .iter()
                .filter_map(|path| object_of_path(path))
                .collect(),
            reported_files: reported_files.clone(),
            reported_indexes: reported_indexes.clone(),
            expected_narthex,
            deleted_narthex,
        });
        for path in &actual_paths {
            if let Some(object) = object_of_path(path) {
                self.deleted_ever.insert(object);
                self.inventory.remove(&object);
            }
        }
        // Branch labels, folded from what this pass really did: the report it
        // returned, the objects it handed to the store, and the manifest snapshot
        // it ran against. Nothing here reads the test's intention, so the seeded
        // scenario's coverage table can be reconciled with it.
        let superseded_index = actual_paths
            .iter()
            .filter_map(|path| object_of_path(path))
            .any(|object| match object {
                GcObject::Index(id, version) => input
                    .manifest_index
                    .get(&id)
                    .and_then(|recorded| *recorded)
                    .is_some_and(|recorded| recorded > version),
                GcObject::Sst(_) | GcObject::Narthex(_, _) => false,
            });
        let skipped = !report.processed_regions.contains(&self.region_id)
            && !report.need_retry_regions.contains(&self.region_id);
        // A batch the oracle had candidates for, withdrawn wholesale because the
        // region changed during the protection lookup.
        let retracted = report.need_retry_regions.contains(&self.region_id)
            && actual_paths.is_empty()
            && !candidates.is_empty();
        self.branches = ScenarioBranches {
            deletes: self.branches.deletes || !actual_paths.is_empty(),
            entry_skip: self.branches.entry_skip || skipped,
            retraction: self.branches.retraction || retracted,
            index_replacement: self.branches.index_replacement || superseded_index,
        };
        self.history.push(format!(
            "pass({what}: {}, linger={:?}, unknown={:?})",
            if knobs.full_listing { "full" } else { "fast" },
            knobs.linger,
            knobs.unknown_linger
        ));
        self.verify_step(&format!("after pass {what}")).await;
        report
    }
}

/// Deterministic generator so a failing scenario can be replayed from its seed
/// alone (SplitMix64, no extra dependency).
struct SeedRng(u64);

impl SeedRng {
    fn new(seed: u64) -> Self {
        SeedRng(seed ^ 0x9E37_79B9_7F4A_7C15)
    }

    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn below(&mut self, bound: u64) -> u64 {
        self.next() % bound
    }

    fn chance(&mut self, percent: u64) -> bool {
        self.below(100) < percent
    }

    /// Deterministic file id, so a replayed seed lists the same objects.
    fn file_id(&mut self) -> FileId {
        let mut bytes = [0u8; 16];
        bytes[..8].copy_from_slice(&self.next().to_le_bytes());
        bytes[8..].copy_from_slice(&self.next().to_le_bytes());
        FileId::from_bytes(bytes)
    }
}

/// The four trajectories the oracle must contain, on one inventory: a fast pass
/// that keeps a mature orphan and a stable full pass that reclaims it, a file
/// that is only referenced by an old index version, a publication that races the
/// protection lookup, and the entry checks (see
/// `test_gc_physical_delete_oracle_entry_skip`).
#[tokio::test]
async fn test_gc_physical_delete_oracle_trajectories() {
    let mut oracle = GcOracle::new("trajectories", 0xA11CE).await;
    let orphan = FileId::from_bytes([0x11; 16]);
    let referenced = FileId::from_bytes([0x22; 16]);
    let retired = FileId::from_bytes([0x33; 16]);
    let hooked = FileId::from_bytes([0x44; 16]);
    let racing = FileId::from_bytes([0x55; 16]);

    for object in [
        GcObject::Sst(orphan),
        GcObject::Index(orphan, 0),
        GcObject::Sst(referenced),
        GcObject::Index(referenced, 0),
        GcObject::Index(referenced, 2),
        GcObject::Sst(retired),
        GcObject::Index(retired, 0),
        GcObject::Sst(hooked),
    ] {
        oracle.write_object(object).await;
    }
    // Nothing references `orphan`: it is an unknown object that only a full
    // listing can find.
    // `referenced` is out of the manifest; only a reader that still holds index
    // version 0 keeps it. Index version 2 of the same file id is referenced by
    // nobody, so the exact-version rule lets the oracle reclaim it.
    oracle.set_tmp_refs([(referenced, Some(0))]);
    oracle.publish_add(retired, Some(0)).await;
    oracle.publish_remove(retired, Some(0), Some(0)).await;
    oracle.publish_add(hooked, None).await;
    oracle.publish_remove(hooked, None, Some(0)).await;
    oracle.set_hook_protected([hooked]);

    // A fast pass reclaims the expired removal but must not touch the unknown
    // orphan, the referenced SST, or the hook-protected file.
    oracle
        .run_pass(
            RoundKnobs::fast(NO_KNOWN_WAIT, YOUNG_UNKNOWN_LINGER),
            PassWiring::default(),
            PassExpectation::complete_deleting(),
            "fast/expired-record",
        )
        .await;
    assert_eq!(
        *oracle.deleted_ever(),
        BTreeSet::from([GcObject::Sst(retired), GcObject::Index(retired, 0)]),
        "{}: a fast pass must delete exactly the expired removal",
        oracle.context()
    );
    assert!(
        oracle.inventory().contains(&GcObject::Sst(orphan)),
        "{}: trajectory 1: a fast pass kept the unknown orphan for the wrong reason",
        oracle.context()
    );

    // A full listing that sees the orphan but has no unknown-file grace elapsed
    // must still keep it.
    oracle
        .run_pass(
            RoundKnobs::full(YOUNG_LINGER, YOUNG_UNKNOWN_LINGER),
            PassWiring::default(),
            PassExpectation::complete(),
            "full/young-unknown",
        )
        .await;
    assert_eq!(
        *oracle.deleted_ever(),
        BTreeSet::from([GcObject::Sst(retired), GcObject::Index(retired, 0)]),
        "{}: a full pass must keep young unknown objects",
        oracle.context()
    );

    // Trajectory 1, positive control: the same stable full pass, now with the
    // unknown-file grace elapsed, really reclaims the mature orphan.
    oracle
        .run_pass(
            RoundKnobs::full(NO_KNOWN_WAIT, MATURE_UNKNOWN_LINGER),
            PassWiring::default(),
            PassExpectation::complete_deleting(),
            "full/mature-orphan",
        )
        .await;
    assert!(
        !oracle.inventory().contains(&GcObject::Sst(orphan)),
        "{}: trajectory 1: the mature orphan must disappear",
        oracle.context()
    );
    // Trajectory 2, negative control: a reader's index version still protects the
    // SST, although index version 2 of the same id is unreferenced.
    assert!(
        oracle.inventory().contains(&GcObject::Sst(referenced)),
        "{}: trajectory 2: an SST referenced by an old index version must survive a full pass",
        oracle.context()
    );
    assert!(
        oracle.inventory().contains(&GcObject::Index(referenced, 0)),
        "{}: trajectory 2: the referenced index version must survive",
        oracle.context()
    );
    assert!(
        !oracle.deleted_ever().contains(&GcObject::Sst(referenced)),
        "{}: trajectory 2: the referenced SST was deleted",
        oracle.context()
    );
    assert!(
        oracle.inventory().contains(&GcObject::Sst(hooked)),
        "{}: a hook-protected file must survive a full pass",
        oracle.context()
    );

    // Releasing the reference makes the same object reclaimable: the reference,
    // not luck, was what kept it.
    oracle.set_tmp_refs([]);
    oracle
        .run_pass(
            RoundKnobs::full(NO_KNOWN_WAIT, MATURE_UNKNOWN_LINGER),
            PassWiring::default(),
            PassExpectation::complete_deleting(),
            "full/released-ref",
        )
        .await;
    assert!(
        !oracle.inventory().contains(&GcObject::Sst(referenced)),
        "{}: the released reference must let the SST go",
        oracle.context()
    );

    // Trajectory 3: a publication that lands while the protection lookup runs
    // withdraws the whole batch.
    oracle.write_object(GcObject::Sst(racing)).await;
    oracle.publish_add(racing, None).await;
    oracle.publish_remove(racing, None, Some(0)).await;
    let engine = oracle.engine.clone();
    let hook = oracle.hook.clone();
    let republish = oracle.file_meta(racing, None);
    let region_id = oracle.region_id;
    hook.block_next_lookup();
    let (report, ()) = tokio::join!(
        oracle.run_pass(
            RoundKnobs::fast(NO_KNOWN_WAIT, YOUNG_UNKNOWN_LINGER),
            PassWiring::default(),
            PassExpectation::retracted(),
            "raced-publication",
        ),
        async move {
            hook.entered.notified().await;
            engine
                .edit_region(
                    region_id,
                    RegionEdit {
                        files_to_add: vec![republish],
                        ..Default::default()
                    },
                )
                .await
                .unwrap();
            hook.resume.notify_one();
        }
    );
    assert!(
        report.need_retry_regions.contains(&region_id),
        "{}: trajectory 3: a raced publication must route the region to a retry",
        oracle.context()
    );
    assert!(
        !report.deleted_files.contains_key(&region_id),
        "{}: trajectory 3: a retracted batch must not report deletions",
        oracle.context()
    );
    assert!(
        oracle.inventory().contains(&GcObject::Sst(racing)),
        "{}: trajectory 3: the retracted batch must leave the object in place",
        oracle.context()
    );

    // Releasing the hook makes the hook-protected file reclaimable.
    oracle.set_hook_protected([]);
    oracle
        .run_pass(
            RoundKnobs::fast(NO_KNOWN_WAIT, YOUNG_UNKNOWN_LINGER),
            PassWiring::default(),
            PassExpectation::complete_deleting(),
            "fast/released-hook",
        )
        .await;
    // `racing` is back in the manifest, so it is the only object left.
    assert_eq!(
        *oracle.inventory(),
        BTreeSet::from([GcObject::Sst(racing)]),
        "{}: every unreachable object must be gone",
        oracle.context()
    );
}

/// Trajectory 4: every entry-level skip must leave the data directory untouched.
/// Each case proves the skip has teeth by running the same inventory again after
/// the cause is cleared.
#[rstest::rstest]
#[case::pending_tails("pending_tails")]
#[case::newer_manifest("newer_manifest")]
#[case::manifest_version_mismatch("manifest_version_mismatch")]
#[case::manifest_probe_failed("manifest_probe_failed")]
#[tokio::test]
async fn test_gc_physical_delete_oracle_entry_skip(#[case] cause: &str) {
    let mut oracle = GcOracle::new(cause, 0x5349_5000).await;
    let orphan = FileId::from_bytes([0x77; 16]);
    let retired = FileId::from_bytes([0x88; 16]);
    for object in [
        GcObject::Sst(orphan),
        GcObject::Index(orphan, 0),
        GcObject::Sst(retired),
        GcObject::Index(retired, 0),
    ] {
        oracle.write_object(object).await;
    }
    oracle.publish_add(retired, None).await;
    oracle.publish_remove(retired, None, Some(0)).await;

    // A mature orphan and an expired removal: a pass that proceeds reclaims both.
    let knobs = RoundKnobs::full(NO_KNOWN_WAIT, MATURE_UNKNOWN_LINGER);
    let mut wiring = PassWiring::default();
    let mut newer_manifest = None;
    match cause {
        "pending_tails" => {
            oracle.arm_pending_tails().await;
            assert!(
                !oracle
                    .region
                    .manifest_ctx
                    .manifest()
                    .await
                    .pending_tails
                    .is_empty(),
                "{}: pending tails vanished before the pass",
                oracle.context()
            );
        }
        "newer_manifest" => newer_manifest = Some(oracle.arm_newer_manifest().await),
        "manifest_version_mismatch" => wiring = PassWiring::mismatched_version(),
        "manifest_probe_failed" => oracle.arm_manifest_list_failure(),
        _ => unreachable!("unknown cause {cause}"),
    }
    oracle
        .run_pass(knobs, wiring, PassExpectation::skipped(), "skipped")
        .await;
    assert!(
        oracle.deleted_ever().is_empty(),
        "{}: trajectory 4: a skipped region must not delete anything",
        oracle.context()
    );

    // Clear the cause and run the very same pass: the preserved inventory is
    // reclaimed, so the skipped pass above was skipped for the armed reason.
    match cause {
        "pending_tails" => oracle.clear_pending_tails().await,
        "newer_manifest" => {
            let path = newer_manifest.unwrap();
            oracle.clear_newer_manifest(&path).await
        }
        "manifest_probe_failed" => oracle.clear_manifest_list_failure(),
        _ => {}
    }
    oracle
        .run_pass(
            knobs,
            PassWiring::default(),
            PassExpectation::complete_deleting(),
            "resumed",
        )
        .await;
    assert_eq!(
        *oracle.deleted_ever(),
        BTreeSet::from([
            GcObject::Sst(orphan),
            GcObject::Index(orphan, 0),
            GcObject::Sst(retired),
            GcObject::Index(retired, 0),
        ]),
        "{}: the resumed pass must reclaim the preserved inventory",
        oracle.context()
    );
}

/// `RemovedFile::File(id, Some(v))` is not "remove one index object": the
/// deletion site expands it into the SST plus every index version `0..=v`. That
/// fan-out is the reason this oracle exists, so it is pinned here at the real
/// delete site, including the two things a report-based test cannot see:
///
/// * index versions inside `0..=v` that never existed are deleted as no-ops -
///   neither a safety violation nor a deletion;
/// * `GcReport::deleted_indexes` is lossy: it records only the `(id, v)` of the
///   removal record, not the versions `0..=v` that the fan-out really removed,
///   and it names `v` even when that object never existed. Losing this
///   information is known and accepted behaviour, so every judgement below is
///   made on the physical deletion set; the report is only checked for the
///   facts it must still contain.
#[tokio::test]
async fn test_gc_physical_delete_oracle_index_fanout() {
    let mut oracle = GcOracle::new("index-fanout", 0xFA_1407).await;
    // Every index version `0..=v` exists.
    let whole = FileId::from_bytes([0x99; 16]);
    let whole_version: IndexVersion = 2;
    // Index versions 1 and 3 never existed, so the fan-out deletes absent keys.
    let partial = FileId::from_bytes([0xAA; 16]);
    let partial_version: IndexVersion = 3;

    for object in [
        GcObject::Sst(whole),
        GcObject::Index(whole, 0),
        GcObject::Index(whole, 1),
        GcObject::Index(whole, 2),
        GcObject::Sst(partial),
        GcObject::Index(partial, 0),
        GcObject::Index(partial, 2),
    ] {
        oracle.write_object(object).await;
    }
    oracle.publish_add(whole, Some(whole_version)).await;
    oracle
        .publish_remove(whole, Some(whole_version), Some(0))
        .await;
    oracle.publish_add(partial, Some(partial_version)).await;
    oracle
        .publish_remove(partial, Some(partial_version), Some(0))
        .await;

    oracle
        .run_pass(
            RoundKnobs::fast(NO_KNOWN_WAIT, YOUNG_UNKNOWN_LINGER),
            PassWiring::default(),
            PassExpectation::complete_deleting(),
            "fast/index-fanout",
        )
        .await;

    // Physical facts, not the report: both SSTs and, for the file whose versions
    // all exist, every index version `0..=v` are gone; for the other file the
    // versions that existed inside `0..=v` are gone too.
    assert_eq!(
        *oracle.deleted_ever(),
        BTreeSet::from([
            GcObject::Sst(whole),
            GcObject::Index(whole, 0),
            GcObject::Index(whole, 1),
            GcObject::Index(whole, 2),
            GcObject::Sst(partial),
            GcObject::Index(partial, 0),
            GcObject::Index(partial, 2),
        ]),
        "{}: `File(id, Some(v))` must delete the SST and every existing index version 0..=v",
        oracle.context()
    );
    assert!(
        oracle.inventory().is_empty(),
        "{}: the fan-out must leave nothing behind: {:?}",
        oracle.context(),
        oracle.inventory()
    );

    let facts = oracle.last_pass();
    // Positive control for the exactness check: the expectation is non-empty, so
    // "deleted exactly what was expected" cannot be satisfied by doing nothing.
    assert!(
        !facts.expected.is_empty(),
        "{}: the fan-out pass must really expect deletions",
        oracle.context()
    );
    // Deleting the keys `0..=v` names but that never existed is a legitimate
    // no-op: it is not counted as a deletion (the physical set above excludes
    // them) and it is not a violation (the pass is not reported as unsafe).
    assert_eq!(
        facts.phantom,
        BTreeSet::from([GcObject::Index(partial, 1), GcObject::Index(partial, 3),]),
        "{}: the absent keys of the 0..=v fan-out must be attempted and must stay out of the deletion set",
        oracle.context()
    );
    assert!(
        facts.actual.is_disjoint(&facts.phantom),
        "{}: an absent key can never count as a deleted object",
        oracle.context()
    );

    // Known lossiness: `(whole, 0)` and `(whole, 1)` were physically removed but
    // are not reported, while `(partial, 3)` is reported although no such object
    // ever existed. This is accepted `GcReport` behaviour, which is exactly why
    // the safety checks above use the physical set.
    assert_eq!(
        facts.reported_indexes,
        BTreeSet::from([(whole, whole_version), (partial, partial_version)]),
        "{}: the report is lossy by design and must be read as a projection of the physical set",
        oracle.context()
    );
    assert_eq!(
        facts.reported_files,
        BTreeSet::from([whole, partial]),
        "{}: every file whose SST the fan-out removed must be reported",
        oracle.context()
    );
    // The one direction the report must still satisfy: every physically deleted
    // object is named by a reported entry, either exactly or through a reported
    // version that covers it. A report that omits a deletion is a lie even when
    // the details are lossy.
    for object in &facts.actual {
        let covered = match *object {
            GcObject::Sst(id) => facts.reported_files.contains(&id),
            GcObject::Index(id, version) => {
                facts
                    .reported_indexes
                    .iter()
                    .any(|(reported_id, reported_version)| {
                        *reported_id == id && *reported_version >= version
                    })
            }
            // An artifact removal is reported by its file id, never per part.
            GcObject::Narthex(id, _) => facts.reported_files.contains(&id),
        };
        assert!(
            covered,
            "{}: physically deleted {} is not covered by the report {:?}",
            oracle.context(),
            object.describe(),
            facts.reported_indexes
        );
    }
}

/// The branch labels of a seeded scenario.
///
/// Every label is a behaviour of the GC implementation read at the delete site
/// or from the returned `GcReport` (see `GcOracle::run_pass`), never an intention
/// of the test, so the coverage table below describes what really ran.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct ScenarioBranches {
    /// `d`: some pass physically deleted a data object, so `deleted_ever` ends up
    /// non-empty.
    deletes: bool,
    /// `s`: some pass was withheld by an entry safety check - it neither processed
    /// the region nor routed it to a retry.
    entry_skip: bool,
    /// `r`: some publication during the protection lookup withdrew the whole
    /// batch - the region was routed to a retry and nothing was deleted although
    /// the oracle saw candidates.
    retraction: bool,
    /// `i`: some superseded index version was reclaimed while the manifest
    /// recorded a newer version of the same file id, that is `index_version`
    /// really changed under a reader that may still hold the old version.
    index_replacement: bool,
}

impl ScenarioBranches {
    const fn new(
        deletes: bool,
        entry_skip: bool,
        retraction: bool,
        index_replacement: bool,
    ) -> Self {
        Self {
            deletes,
            entry_skip,
            retraction,
            index_replacement,
        }
    }

    /// `d/s/r/i` as four `0`/`1` digits, for the coverage table and failures.
    fn key(self) -> String {
        format!(
            "{}{}{}{}",
            self.deletes as u8,
            self.entry_skip as u8,
            self.retraction as u8,
            self.index_replacement as u8
        )
    }
}

/// The entry safety check that withholds a pass before it lists the region.
#[derive(Clone, Copy, Debug)]
enum EntrySkip {
    /// An uncommitted tail SST is not in the snapshot, so a full listing would
    /// take it for an orphan.
    PendingTails,
    /// The manifest version handed to the worker does not match the region's.
    ManifestVersion,
}

impl EntrySkip {
    const fn name(self) -> &'static str {
        match self {
            EntrySkip::PendingTails => "pending-tails",
            EntrySkip::ManifestVersion => "manifest-version",
        }
    }
}

/// What one seed plans to cover, plus the operation choices that put the seed on
/// that row of the coverage table.
///
/// `expected` is spelled out by hand per seed instead of being derived from the
/// choices: the collapse guard is only worth anything if the plan and the
/// observed behaviour can disagree, which a table derived from the same fields
/// could never catch.
#[derive(Clone, Copy, Debug)]
struct ScenarioPlan {
    /// Entry-check cause of a withholding pass; `None` plans no skip at all.
    entry_skip: Option<EntrySkip>,
    /// Publish a file while the protection lookup blocks, to withdraw the batch.
    retraction: bool,
    /// Index version `live` is republished at, and the version the reader holds.
    /// `0` plans no replacement at all: `live` is only ever published at version
    /// 0, so no index version of a live file is ever superseded.
    index_version: IndexVersion,
    /// Run the passes that run to completion, so eligible objects are reclaimed.
    completing_passes: bool,
    /// The [`ScenarioBranches`] row this seed must reach.
    expected: ScenarioBranches,
}

/// The three seeded cases. A new case needs a row in the coverage table and a
/// plan in [`scenario_plan`];
/// `test_gc_physical_delete_oracle_scenario_plans_are_distinct` then checks that
/// the table did not collapse two seeds onto one row.
const SEED_A: u64 = 0x5EED_0001;
const SEED_B: u64 = 0x5EED_0002;
const SEED_C: u64 = 0x5EED_0003;

/// The plan of one seed: the seed selects a row of the coverage table, so what a
/// case covers is a decision written here instead of a draw that happens to land
/// somewhere.
fn scenario_plan(seed: u64) -> ScenarioPlan {
    match seed {
        // The withheld pass is armed with uncommitted tails; every other pass is
        // a stable full pass, and `live` is republished at index version 1, so a
        // superseded index version is reclaimed. Nothing races the hook.
        SEED_A => ScenarioPlan {
            entry_skip: Some(EntrySkip::PendingTails),
            retraction: false,
            index_version: 1,
            completing_passes: true,
            expected: ScenarioBranches::new(true, true, false, true),
        },
        // The withheld pass is stopped by a manifest-version mismatch, and the
        // only other pass is the raced one: the batch it gathered is withdrawn
        // and no pass ever runs to completion, so nothing is deleted. Every file
        // is published at index version 0 (or not at all), so no version is
        // superseded.
        SEED_B => ScenarioPlan {
            entry_skip: Some(EntrySkip::ManifestVersion),
            retraction: true,
            index_version: 0,
            completing_passes: false,
            expected: ScenarioBranches::new(false, true, true, false),
        },
        // No entry skip: the raced pass is the first pass of the scenario, and
        // the full passes after it reclaim everything eligible. Versions never
        // change, so no superseded index version is reclaimed.
        SEED_C => ScenarioPlan {
            entry_skip: None,
            retraction: true,
            index_version: 0,
            completing_passes: true,
            expected: ScenarioBranches::new(true, false, true, false),
        },
        _ => panic!(
            "seed {seed:#X} has no scenario plan: the coverage table in this file is the source of truth"
        ),
    }
}

/// Seeded, deterministic operation sequences over one inventory.
///
/// The seed does not pick parameters at random: it selects a row of the branch
/// coverage table below and the operations are generated from that row. A
/// failure is replayable from `seed` plus the printed operation prefix, and the
/// file ids still come from the seed, so the cases list different objects.
///
/// The four branch labels (see [`ScenarioBranches`]) are:
///
/// * `d` - a pass physically deleted a data object (`deleted_ever` non-empty);
/// * `s` - a pass was withheld by an entry safety check, i.e. one of the four
///   entry causes, so it neither completed nor asked for a retry;
/// * `r` - a publication during the protection lookup withdrew the whole batch;
/// * `i` - a superseded index version was reclaimed while the manifest recorded a
///   newer one, that is `Index(live, 0)` went away and `Index(live, 1)` stayed.
///
/// | seed         | d | s | r | i | what the seed runs |
/// | ------------ | - | - | - | - | ------------------ |
/// | `0x5EED_0001` | 1 | 1 | 0 | 1 | a pending-tails skip, then three full passes that reclaim an expired removal, a mature orphan, a young-then-aged removal, a hook-released file and the superseded `Index(live, 0)` |
/// | `0x5EED_0002` | 0 | 1 | 1 | 0 | a manifest-version skip, then a publication during the protection lookup: the batch is withdrawn and no pass runs to completion, so nothing is deleted |
/// | `0x5EED_0003` | 1 | 0 | 1 | 0 | no skip at all: the raced pass withdraws the first batch, then three full passes reclaim everything eligible |
///
/// The three rows differ pairwise and every label is exercised in both states, so
/// the cases cover the four dimensions instead of one combination three times.
/// The end of the test reconciles the row a seed planned with the row its passes
/// really took, so a change that makes a skip or a retraction stop happening
/// fails here instead of quietly shrinking coverage.
#[rstest::rstest]
#[case::seed_a(SEED_A)]
#[case::seed_b(SEED_B)]
#[case::seed_c(SEED_C)]
#[tokio::test]
async fn test_gc_physical_delete_oracle_scenario(#[case] seed: u64) {
    let plan = scenario_plan(seed);
    let mut oracle = GcOracle::new("scenario", seed).await;
    let mut rng = SeedRng::new(seed);

    let live = rng.file_id();
    let reader = rng.file_id();
    let old_removed = rng.file_id();
    let young_removed = rng.file_id();
    let guarded = rng.file_id();
    let orphan = rng.file_id();
    let racing = rng.file_id();
    let version = plan.index_version;

    // Inventory: only the ids are seed-dependent, every file's fate follows from
    // the plan and from the manifest records written below.
    for id in [live, reader, old_removed, young_removed, guarded] {
        oracle.write_object(GcObject::Sst(id)).await;
        oracle.write_object(GcObject::Index(id, 0)).await;
    }
    if version != 0 {
        oracle.write_object(GcObject::Index(live, version)).await;
        oracle.write_object(GcObject::Index(reader, version)).await;
    }
    // Never published: only a full listing sees it, and only once it is older
    // than the unknown-file grace.
    oracle.write_object(GcObject::Sst(orphan)).await;

    // `live` is published at version 0 and republished at `version`, which
    // records `Index(live, 0)` as a superseded index version (label `i`).
    oracle.publish_add(live, Some(0)).await;
    if version != 0 {
        oracle.publish_add(live, Some(version)).await;
    }
    // `reader` left the manifest while a reader still holds `version`: the SST
    // and that exact version stay, other versions of it need not.
    oracle.publish_add(reader, None).await;
    oracle.publish_remove(reader, None, Some(0)).await;
    oracle.set_tmp_refs([(reader, Some(version))]);
    // An expired removal, a removal whose record is still young, one only the
    // hook keeps alive, and one object nobody references.
    oracle.publish_add(old_removed, Some(0)).await;
    oracle.publish_remove(old_removed, Some(0), Some(0)).await;
    oracle.publish_add(young_removed, None).await;
    oracle.publish_remove(young_removed, None, None).await;
    oracle.publish_add(guarded, None).await;
    oracle.publish_remove(guarded, None, Some(0)).await;
    oracle.set_hook_protected([guarded]);

    // Label `s`: one pass withheld at an entry safety check. Both causes stop the
    // pass before the region is listed, although the expired removal and the
    // mature orphan are already candidates (`PassExpectation::skipped` checks
    // that the oracle sees work the pass refuses to do).
    if let Some(cause) = plan.entry_skip {
        match cause {
            EntrySkip::PendingTails => oracle.arm_pending_tails().await,
            EntrySkip::ManifestVersion => {}
        }
        let wiring = match cause {
            EntrySkip::PendingTails => PassWiring::default(),
            EntrySkip::ManifestVersion => PassWiring::mismatched_version(),
        };
        oracle
            .run_pass(
                RoundKnobs::full(NO_KNOWN_WAIT, MATURE_UNKNOWN_LINGER),
                wiring,
                PassExpectation::skipped(),
                &format!("skip/{}", cause.name()),
            )
            .await;
        if matches!(cause, EntrySkip::PendingTails) {
            oracle.clear_pending_tails().await;
        }
    }

    // Label `r`: a publication lands while the protection lookup runs, so the
    // whole batch must be withdrawn. `racing` carries an expired removal, which is
    // what gives that pass a non-empty batch to withdraw; the re-publication puts
    // it back into the manifest, so no later pass may reclaim it.
    if plan.retraction {
        oracle.write_object(GcObject::Sst(racing)).await;
        oracle.publish_add(racing, None).await;
        oracle.publish_remove(racing, None, Some(0)).await;

        let engine = oracle.engine.clone();
        let hook = oracle.hook.clone();
        let region_id = oracle.region_id;
        let republish = oracle.file_meta(racing, None);
        hook.block_next_lookup();
        let (_, ()) = tokio::join!(
            oracle.run_pass(
                RoundKnobs::fast(NO_KNOWN_WAIT, YOUNG_UNKNOWN_LINGER),
                PassWiring::default(),
                PassExpectation::retracted(),
                "raced-publication",
            ),
            async move {
                hook.entered.notified().await;
                engine
                    .edit_region(
                        region_id,
                        RegionEdit {
                            files_to_add: vec![republish],
                            ..Default::default()
                        },
                    )
                    .await
                    .unwrap();
                hook.resume.notify_one();
            }
        );
    }

    // Label `d`, plus the reclamations the tests above pin one at a time: the age
    // rules, the reader reference and the hook.
    if plan.completing_passes {
        // A full pass whose linger is over reclaims the expired removal and the
        // mature orphan, and keeps the young removal and the hook-protected file.
        oracle
            .run_pass(
                RoundKnobs::full(YOUNG_LINGER, MATURE_UNKNOWN_LINGER),
                PassWiring::default(),
                PassExpectation::complete_deleting(),
                "full/expired-removal",
            )
            .await;
        assert!(
            oracle.deleted_ever().contains(&GcObject::Sst(orphan)),
            "{}: the mature orphan was never reclaimed",
            oracle.context()
        );
        assert!(
            !oracle
                .deleted_ever()
                .contains(&GcObject::Sst(young_removed)),
            "{}: a still lingering removal must keep its file",
            oracle.context()
        );

        // Age the young removal without waiting: republishing it with an expel
        // time far in the past makes it eligible.
        oracle.publish_remove(young_removed, None, Some(0)).await;
        oracle
            .run_pass(
                RoundKnobs::full(NO_KNOWN_WAIT, MATURE_UNKNOWN_LINGER),
                PassWiring::default(),
                PassExpectation::complete_deleting(),
                "full/aged-removal",
            )
            .await;
        assert!(
            oracle
                .deleted_ever()
                .contains(&GcObject::Sst(young_removed)),
            "{}: the aged removal must let its file go",
            oracle.context()
        );

        // Release the hook: the file it alone kept alive is reclaimable now.
        oracle.set_hook_protected([]);
        oracle
            .run_pass(
                RoundKnobs::full(NO_KNOWN_WAIT, MATURE_UNKNOWN_LINGER),
                PassWiring::default(),
                PassExpectation::complete_deleting(),
                "full/hook-released",
            )
            .await;
        assert!(
            oracle.deleted_ever().contains(&GcObject::Sst(guarded)),
            "{}: the released hook must let its file go",
            oracle.context()
        );
    }

    // Collapse guard: reconcile the row this seed's row of the coverage table
    // promises with the row its passes really took. Without it a seed could drift
    // onto another row, or onto the same row as its neighbours, and the suite
    // would silently cover one combination three times.
    let observed = oracle.branches();
    assert_eq!(
        observed,
        plan.expected,
        "{}: branch collapse: the coverage table plans d/s/r/i = {} ({:?}) but the passes took d/s/r/i = {} ({:?}); if a change moved this seed onto another row, update the table and the plan together instead of weakening this check",
        oracle.context(),
        plan.expected.key(),
        plan.expected,
        observed.key(),
        observed,
    );

    // No branch may reclaim a live file, nor the SST and the exact index version
    // a reader still holds.
    for object in [
        GcObject::Sst(live),
        GcObject::Index(live, version),
        GcObject::Sst(reader),
        GcObject::Index(reader, version),
    ] {
        assert!(
            !oracle.deleted_ever().contains(&object),
            "{}: {} must never be deleted",
            oracle.context(),
            object.describe()
        );
        assert!(
            oracle.inventory().contains(&object),
            "{}: {} must still exist",
            oracle.context(),
            object.describe()
        );
    }
    // A withdrawn batch leaves the file it raced with in place.
    if plan.retraction {
        assert!(
            oracle.inventory().contains(&GcObject::Sst(racing))
                && !oracle.deleted_ever().contains(&GcObject::Sst(racing)),
            "{}: the retracted batch must leave the file it raced with in place",
            oracle.context()
        );
    }
    // Label `d`, stated on `deleted_ever` itself.
    if !plan.expected.deletes {
        assert!(
            oracle.deleted_ever().is_empty(),
            "{}: this seed plans no physical deletion, but reclaimed {:?}",
            oracle.context(),
            oracle.deleted_ever()
        );
    }
    // Every seed that plans deletions reclaims each object the model considers
    // unreachable.
    if plan.expected.deletes {
        for object in [
            GcObject::Sst(old_removed),
            GcObject::Sst(young_removed),
            GcObject::Sst(guarded),
        ] {
            assert!(
                oracle.deleted_ever().contains(&object),
                "{}: {} must be reclaimed",
                oracle.context(),
                object.describe()
            );
        }
    }
    // Label `i`, stated on the final inventory instead of on a single pass: a
    // superseded index version went away while a newer version of the same file
    // id stayed.
    let superseded_gone = oracle.deleted_ever().iter().any(|deleted| match *deleted {
        GcObject::Index(id, deleted_version) => oracle.inventory().iter().any(|left| {
            matches!(
                left,
                GcObject::Index(left_id, left_version)
                    if *left_id == id && *left_version > deleted_version
            )
        }),
        GcObject::Sst(_) | GcObject::Narthex(_, _) => false,
    });
    assert_eq!(
        superseded_gone,
        plan.expected.index_replacement,
        "{}: label `i` must be {} on this seed, deleted_ever = {:?}, inventory = {:?}",
        oracle.context(),
        plan.expected.index_replacement,
        oracle.deleted_ever(),
        oracle.inventory(),
    );
}

/// The coverage table is the only reason the three seeded cases exist: three
/// seeds that all take one branch combination prove no more than one seed, so the
/// table is checked without running any GC. Every row must differ from every
/// other row and every label must appear in both states.
#[test]
fn test_gc_physical_delete_oracle_scenario_plans_are_distinct() {
    let plans = [SEED_A, SEED_B, SEED_C].map(|seed| (seed, scenario_plan(seed)));
    for (index, (seed, plan)) in plans.iter().enumerate() {
        for (other_seed, other) in &plans[index + 1..] {
            assert_ne!(
                plan.expected,
                other.expected,
                "seeds {seed:#X} and {other_seed:#X} plan the same branch row d/s/r/i = {}: one of them is redundant coverage, change its plan and its row in the table",
                plan.expected.key(),
            );
        }
    }
    for (label, picked) in [
        ("deletes", plans.map(|(_, plan)| plan.expected.deletes)),
        (
            "entry_skip",
            plans.map(|(_, plan)| plan.expected.entry_skip),
        ),
        (
            "retraction",
            plans.map(|(_, plan)| plan.expected.retraction),
        ),
        (
            "index_replacement",
            plans.map(|(_, plan)| plan.expected.index_replacement),
        ),
    ] {
        assert!(
            picked.iter().any(|on| *on) && picked.iter().any(|on| !*on),
            "the coverage table must exercise both states of `{label}`, it only plans {picked:?}",
        );
    }
}

/// Narthex artifacts at the real delete site.
///
/// A narthex artifact is one file made of parts under `<region>/<file id>/`,
/// and it is reclaimed as a whole: `delete_narthex_artifact` hands the object
/// store one recursive directory delete. The safety contract is therefore
/// judged per part but executed per artifact, which is why the oracle models
/// both levels: the parts are inventory objects with their own age, and the
/// artifact directory is the unit a pass may or may not remove.
///
/// Three properties are covered here, none of which a report-based test can
/// see:
/// * every part is past the grace period and the artifact is not in the
///   manifest - the whole directory is reclaimed, including its own
///   `manifest.json`;
/// * one part is younger than the grace period - the whole artifact survives,
///   although its other parts are as old as the ones of a reclaimed artifact in
///   the very same pass;
/// * one artifact is listed in the manifest - no age makes it reclaimable.
///
/// A second pass then removes an artifact through the manifest record
/// (`RemovedFile::Narthex`), and leaves one alone while its record still
/// lingers. `deleted_ever` and the inventory are checked for the parts, so a
/// pass that removes the directory without accounting for its objects fails.
#[tokio::test]
async fn test_gc_physical_delete_oracle_narthex() {
    // Long enough to separate "written before the pass" from "written during
    // it", short enough to keep the test fast. The oracle refuses any object
    // whose age is not proven, so an unlucky machine reports an ambiguity
    // instead of a wrong verdict.
    const GRACE: Duration = Duration::from_millis(500);
    const AGING: Duration = Duration::from_millis(600);

    let mut oracle = GcOracle::new("narthex", 0x4E41_2748).await;
    // Every part is older than `GRACE`: the artifact is reclaimed as a whole.
    let mature = FileId::from_bytes([0x0A; 16]);
    // One part is written after the aging step, so the artifact must survive
    // even though its other parts are exactly as old as `mature`'s.
    let young_part = FileId::from_bytes([0x0B; 16]);
    // In the manifest: protected regardless of age.
    let published = FileId::from_bytes([0x0C; 16]);
    // Removed through the manifest, which records `RemovedFile::Narthex`.
    let recorded = FileId::from_bytes([0x0D; 16]);
    // Removed through the manifest, but its record still lingers.
    let lingering = FileId::from_bytes([0x0E; 16]);

    oracle.write_narthex_artifact(mature, 2).await;
    oracle.write_narthex_artifact(published, 2).await;
    oracle.publish_add_narthex(published, 2).await;
    oracle.write_narthex_artifact(young_part, 1).await;
    tokio::time::sleep(AGING).await;
    // The fresh part lands after every other object, so only it is young.
    oracle
        .write_object(GcObject::Narthex(young_part, NarthexObject::Part(1)))
        .await;

    // A full listing sees every part. `mature` and `young_part` are not in the
    // manifest, so only their age decides; the artifact of `young_part` is kept
    // whole because one of its parts is younger than the grace period.
    oracle
        .run_pass(
            RoundKnobs::full(NO_KNOWN_WAIT, GRACE),
            PassWiring::default(),
            PassExpectation::complete_deleting(),
            "full/aged-artifacts",
        )
        .await;

    let facts = oracle.last_pass();
    // Physical deletion set, not the report: every object of the mature artifact
    // is gone, and no object of the artifact with a young part is.
    assert_eq!(
        facts.deleted_narthex,
        BTreeSet::from([mature]),
        "{}: only the fully aged, unpublished artifact may be removed",
        oracle.context()
    );
    assert_eq!(
        facts.actual,
        BTreeSet::from([
            GcObject::Narthex(mature, NarthexObject::Manifest),
            GcObject::Narthex(mature, NarthexObject::Part(0)),
            GcObject::Narthex(mature, NarthexObject::Part(1)),
        ]),
        "{}: the artifact removal must remove every part it holds",
        oracle.context()
    );
    // The one-part-young artifact is the key safety property: not even its old
    // parts are reclaimed, because the removal drops the whole directory.
    for object in [
        GcObject::Narthex(young_part, NarthexObject::Manifest),
        GcObject::Narthex(young_part, NarthexObject::Part(0)),
        GcObject::Narthex(young_part, NarthexObject::Part(1)),
    ] {
        assert!(
            oracle.inventory().contains(&object),
            "{}: {} must survive while one part of its artifact is young",
            oracle.context(),
            object.describe()
        );
    }
    for object in [
        GcObject::Narthex(published, NarthexObject::Manifest),
        GcObject::Narthex(published, NarthexObject::Part(0)),
    ] {
        assert!(
            oracle.inventory().contains(&object),
            "{}: {} is listed in the manifest and must survive any age",
            oracle.context(),
            object.describe()
        );
    }
    // Inventory accounting really covers the parts, not just the directory: the
    // model's view of storage and the storage itself are compared after every
    // step, and the removed parts are recorded as deleted.
    for part in [NarthexObject::Manifest, NarthexObject::Part(0)] {
        assert!(
            oracle
                .deleted_ever()
                .contains(&GcObject::Narthex(mature, part)),
            "{}: {} was physically removed and must be accounted as deleted",
            oracle.context(),
            GcObject::Narthex(mature, part).describe()
        );
    }

    // The manifest route: `RegionManifestBuilder` records a non-Parquet removal
    // as `RemovedFile::Narthex`, so a fast pass must reclaim that artifact
    // directory, and must leave the one whose record still lingers.
    oracle.write_narthex_artifact(recorded, 2).await;
    oracle.publish_add_narthex(recorded, 2).await;
    oracle.publish_remove_narthex(recorded, Some(0)).await;
    oracle.write_narthex_artifact(lingering, 1).await;
    oracle.publish_add_narthex(lingering, 1).await;
    oracle.publish_remove_narthex(lingering, None).await;

    oracle
        .run_pass(
            RoundKnobs::fast(Some(GRACE), YOUNG_UNKNOWN_LINGER),
            PassWiring::default(),
            PassExpectation::complete_deleting(),
            "fast/recorded-artifact",
        )
        .await;

    let facts = oracle.last_pass();
    assert_eq!(
        facts.expected_narthex,
        BTreeSet::from([recorded]),
        "{}: only the recorded, expired artifact is a candidate",
        oracle.context()
    );
    assert_eq!(
        facts.deleted_narthex,
        BTreeSet::from([recorded]),
        "{}: a fast pass must remove the recorded artifact directory",
        oracle.context()
    );
    assert_eq!(
        facts.reported_files,
        BTreeSet::from([recorded]),
        "{}: an artifact removal is reported by its file id",
        oracle.context()
    );
    for part in [
        NarthexObject::Manifest,
        NarthexObject::Part(0),
        NarthexObject::Part(1),
    ] {
        let object = GcObject::Narthex(recorded, part);
        assert!(
            oracle.deleted_ever().contains(&object),
            "{}: {} must be accounted as deleted",
            oracle.context(),
            object.describe()
        );
        assert!(
            !oracle.inventory().contains(&object),
            "{}: {} must be gone from the model inventory",
            oracle.context(),
            object.describe()
        );
    }
    for part in [NarthexObject::Manifest, NarthexObject::Part(0)] {
        let object = GcObject::Narthex(lingering, part);
        assert!(
            oracle.inventory().contains(&object),
            "{}: {} belongs to a still lingering removal and must survive",
            oracle.context(),
            object.describe()
        );
        assert!(
            !oracle.deleted_ever().contains(&object),
            "{}: {} was deleted although its record still lingers",
            oracle.context(),
            object.describe()
        );
    }
    // Nothing else was touched: the artifacts of the first pass are all still
    // there, and the directory of the reclaimed one is really gone.
    for id in [young_part, published, lingering] {
        assert!(
            oracle
                .inventory()
                .iter()
                .any(|object| object.file_id() == id && matches!(object, GcObject::Narthex(_, _))),
            "{}: the artifact {id} must still hold objects",
            oracle.context()
        );
    }
    assert!(
        !oracle
            .inventory()
            .iter()
            .any(|object| object.file_id() == recorded),
        "{}: the reclaimed artifact {recorded} must hold no object any more",
        oracle.context()
    );
}
