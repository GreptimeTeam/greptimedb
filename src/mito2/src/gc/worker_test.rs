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
use store_api::region_engine::RegionEngine as _;
use store_api::region_request::{RegionCompactRequest, RegionRequest};
use store_api::storage::{FileRef, FileRefsManifest, RegionId};

use crate::config::MitoConfig;
use crate::engine::MitoEngine;
use crate::engine::compaction_test::{delete_and_flush, put_and_flush};
use crate::gc::{GcConfig, LocalGcWorker};
use crate::manifest::action::RemovedFile;
use crate::region::MitoRegionRef;
use crate::test_util::{
    CreateRequestBuilder, TestEnv, build_rows, flush_region, put_rows, rows_schema,
};

async fn create_gc_worker(
    mito_engine: &MitoEngine,
    regions: BTreeMap<RegionId, MitoRegionRef>,
    file_ref_manifest: &FileRefsManifest,
    full_file_listing: bool,
) -> LocalGcWorker {
    let access_layer = regions.first_key_value().unwrap().1.access_layer.clone();
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

    let regions = BTreeMap::from([(region_id, region.clone())]);
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

    let regions = BTreeMap::from([(region_id, region.clone())]);
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

    let regions = BTreeMap::from([(region_id, region.clone())]);
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

    let regions = BTreeMap::from([(region_id, region.clone())]);
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
