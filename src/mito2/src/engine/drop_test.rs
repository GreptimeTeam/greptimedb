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

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use api::v1::Rows;
use common_base::Plugins;
use common_error::ext::ErrorExt;
use common_error::status_code::StatusCode;
use common_meta::key::SchemaMetadataManager;
use common_meta::kv_backend::KvBackendRef;
use object_store::util::join_path;
use store_api::region_engine::{RegionEngine, RegionRole};
use store_api::region_request::{
    RegionDropRequest, RegionFlushReason, RegionFlushRequest, RegionRequest,
};
use store_api::storage::RegionId;

use crate::config::MitoConfig;
use crate::engine::MitoEngine;
use crate::engine::flush_test::MockRegionHook;
use crate::engine::listener::{DropListener, FlushCancellationListener};
use crate::engine::region_hook::RegionHookRef;
use crate::test_util::{
    CreateRequestBuilder, MockWriteBufferManager, TestEnv, build_rows_for_key, flush_region,
    put_rows, rows_schema,
};
use crate::worker::DROPPING_MARKER_FILE;

#[tokio::test]
async fn test_drop_cancels_running_flush() {
    common_telemetry::init_default_ut_logging();
    let mut env = TestEnv::with_prefix("drop-during-flush").await;
    let listener = Arc::new(FlushCancellationListener::default());
    let engine = env
        .create_engine_with(
            MitoConfig::default(),
            Some(Arc::new(MockWriteBufferManager::default())),
            Some(listener.clone()),
            None,
        )
        .await;
    let region_id = RegionId::new(100, 1);
    let request = CreateRequestBuilder::new().build();
    let rows = rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();
    put_rows(
        &engine,
        region_id,
        Rows {
            schema: rows,
            rows: build_rows_for_key("a", 0, 2, 0),
        },
    )
    .await;

    let flush_engine = engine.clone();
    let flush_task = tokio::spawn(async move {
        flush_engine
            .handle_request(
                region_id,
                RegionRequest::Flush(RegionFlushRequest::default()),
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), listener.wait_flush_started())
        .await
        .expect("flush did not reach the cancellation gate");

    tokio::time::timeout(
        Duration::from_secs(5),
        engine.handle_request(
            region_id,
            RegionRequest::Drop(RegionDropRequest {
                fast_path: false,
                force: false,
                partial_drop: false,
            }),
        ),
    )
    .await
    .expect("drop did not finish after cancelling flush")
    .unwrap();

    let flush_err = tokio::time::timeout(Duration::from_secs(5), flush_task)
        .await
        .expect("cancelled flush did not finish")
        .expect("flush task panicked")
        .unwrap_err();
    assert_eq!(flush_err.status_code(), StatusCode::Cancelled);
    assert!(engine.get_region(region_id).is_none());
}

#[tokio::test]
async fn test_drop_rejects_downgrading_region_without_cancelling_flush() {
    common_telemetry::init_default_ut_logging();
    let mut env = TestEnv::with_prefix("drop-during-downgrade-flush").await;
    let listener = Arc::new(FlushCancellationListener::default());
    let engine = env
        .create_engine_with(
            MitoConfig::default(),
            Some(Arc::new(MockWriteBufferManager::default())),
            Some(listener.clone()),
            None,
        )
        .await;
    let region_id = RegionId::new(101, 1);
    let request = CreateRequestBuilder::new().build();
    let rows = rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();
    put_rows(
        &engine,
        region_id,
        Rows {
            schema: rows,
            rows: build_rows_for_key("a", 0, 2, 0),
        },
    )
    .await;
    engine
        .set_region_role(region_id, RegionRole::DowngradingLeader)
        .unwrap();

    let flush_engine = engine.clone();
    let flush_task = tokio::spawn(async move {
        flush_engine
            .handle_request(
                region_id,
                RegionRequest::Flush(RegionFlushRequest {
                    reason: Some(RegionFlushReason::Downgrading),
                    ..Default::default()
                }),
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), listener.wait_flush_started())
        .await
        .expect("downgrade flush did not reach the cancellation gate");

    let drop_err = tokio::time::timeout(
        Duration::from_secs(5),
        engine.handle_request(
            region_id,
            RegionRequest::Drop(RegionDropRequest {
                fast_path: false,
                force: false,
                partial_drop: false,
            }),
        ),
    )
    .await
    .expect("drop was not rejected while the downgrade flush was blocked")
    .unwrap_err();
    assert_eq!(drop_err.status_code(), StatusCode::RegionNotReady);
    assert!(engine.get_region(region_id).is_some());
    assert_eq!(engine.role(region_id), Some(RegionRole::DowngradingLeader));

    listener.resume_flush();
    let flush_result = tokio::time::timeout(Duration::from_secs(5), flush_task)
        .await
        .expect("downgrade flush did not finish")
        .expect("downgrade flush task panicked")
        .expect("downgrade flush was cancelled");
    assert_eq!(flush_result.affected_rows, 0);
}

#[tokio::test]
async fn test_engine_drop_region() {
    test_engine_drop_region_with_format(false).await;
    test_engine_drop_region_with_format(true).await;
}

async fn test_engine_drop_region_with_format(flat_format: bool) {
    common_telemetry::init_default_ut_logging();

    let mut env = TestEnv::with_prefix("drop").await;
    let listener = Arc::new(DropListener::new(Duration::from_millis(100)));
    let engine = env
        .create_engine_with(
            MitoConfig {
                default_flat_format: flat_format,
                ..Default::default()
            },
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

    // It's okay to drop a region doesn't exist.
    engine
        .handle_request(
            region_id,
            RegionRequest::Drop(RegionDropRequest {
                fast_path: false,
                force: false,
                partial_drop: false,
            }),
        )
        .await
        .unwrap_err();

    let request = CreateRequestBuilder::new().build();
    let column_schemas = rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    let region = engine.get_region(region_id).unwrap();
    let region_dir = region.access_layer.build_region_dir(region_id);
    // no dropping marker file
    assert!(
        !env.get_object_store()
            .unwrap()
            .exists(&join_path(&region_dir, DROPPING_MARKER_FILE))
            .await
            .unwrap()
    );

    let rows = Rows {
        schema: column_schemas.clone(),
        rows: build_rows_for_key("a", 0, 2, 0),
    };
    put_rows(&engine, region_id, rows).await;
    flush_region(&engine, region_id, None).await;

    // drop the created region.
    engine
        .handle_request(
            region_id,
            RegionRequest::Drop(RegionDropRequest {
                fast_path: false,
                force: false,
                partial_drop: false,
            }),
        )
        .await
        .unwrap();
    assert!(!engine.is_region_exists(region_id));

    // Wait for drop task.
    listener.wait().await;

    let object_store = env.get_object_store().unwrap();
    assert!(!object_store.exists(&region_dir).await.unwrap());
}

#[tokio::test]
async fn test_engine_drop_region_for_custom_store() {
    test_engine_drop_region_for_custom_store_with_format(false).await;
    test_engine_drop_region_for_custom_store_with_format(true).await;
}

async fn test_engine_drop_region_for_custom_store_with_format(flat_format: bool) {
    common_telemetry::init_default_ut_logging();
    async fn setup(
        engine: &MitoEngine,
        schema_metadata_manager: &SchemaMetadataManager,
        kv_backend: &KvBackendRef,
        region_id: RegionId,
        storage_name: &str,
    ) {
        let request = CreateRequestBuilder::new()
            .insert_option("storage", storage_name)
            .table_dir(storage_name)
            .build();
        let column_schema = rows_schema(&request);
        engine
            .handle_request(region_id, RegionRequest::Create(request))
            .await
            .unwrap();

        let table_id = format!("test_table_{}", region_id.table_id());
        schema_metadata_manager
            .register_region_table_info(
                region_id.table_id(),
                &table_id,
                "test_catalog",
                "test_schema",
                None,
                kv_backend.clone(),
            )
            .await;

        let rows = Rows {
            schema: column_schema.clone(),
            rows: build_rows_for_key("a", 0, 2, 0),
        };
        put_rows(engine, region_id, rows).await;
        flush_region(engine, region_id, None).await;
    }
    let mut env = TestEnv::with_prefix("drop").await;
    let listener = Arc::new(DropListener::new(Duration::from_millis(100)));
    let engine = env
        .create_engine_with_multiple_object_stores(
            MitoConfig {
                default_flat_format: flat_format,
                ..Default::default()
            },
            None,
            Some(listener.clone()),
            &["Gcs"],
        )
        .await;
    let schema_metadata_manager = env.get_schema_metadata_manager();
    let object_store_manager = env.get_object_store_manager().unwrap();
    let kv_backend = env.get_kv_backend();

    let global_region_id = RegionId::new(1, 1);
    setup(
        &engine,
        &schema_metadata_manager,
        &kv_backend,
        global_region_id,
        "default",
    )
    .await;
    let custom_region_id = RegionId::new(2, 1);
    setup(
        &engine,
        &schema_metadata_manager,
        &kv_backend,
        custom_region_id,
        "Gcs",
    )
    .await;

    let global_region = engine.get_region(global_region_id).unwrap();
    let global_region_dir = global_region
        .access_layer
        .build_region_dir(global_region_id);

    let custom_region = engine.get_region(custom_region_id).unwrap();
    let custom_region_dir = custom_region
        .access_layer
        .build_region_dir(custom_region_id);

    common_telemetry::info!(
        "global_region_dir: {global_region_dir}, custom_region_dir: {custom_region_dir}"
    );

    let entries = object_store_manager
        .find("Gcs")
        .unwrap()
        .list(&custom_region_dir)
        .await
        .unwrap();
    common_telemetry::info!("Before drop, Gcs entries: {:?}", entries);
    let entries = object_store_manager
        .find("default")
        .unwrap()
        .list(&global_region_dir)
        .await
        .unwrap();
    common_telemetry::info!("Before drop,default entries: {:?}", entries);

    // Both these regions should exist before dropping the custom region.
    assert!(
        object_store_manager
            .find("Gcs")
            .unwrap()
            .exists(&custom_region_dir)
            .await
            .unwrap()
    );
    assert!(
        object_store_manager
            .find("default")
            .unwrap()
            .exists(&global_region_dir)
            .await
            .unwrap()
    );

    // Drop the custom region.
    engine
        .handle_request(
            custom_region_id,
            RegionRequest::Drop(RegionDropRequest {
                fast_path: false,
                force: false,
                partial_drop: false,
            }),
        )
        .await
        .unwrap();
    assert!(!engine.is_region_exists(custom_region_id));

    // Wait for drop task.
    listener.wait().await;

    let entries = object_store_manager
        .find("Gcs")
        .unwrap()
        .list(&custom_region_dir)
        .await
        .unwrap();
    common_telemetry::info!("After drop, Gcs entries: {:?}", entries);
    let entries = object_store_manager
        .find("default")
        .unwrap()
        .list(&global_region_dir)
        .await
        .unwrap();
    common_telemetry::info!("After drop,default entries: {:?}", entries);

    assert!(
        !object_store_manager
            .find("Gcs")
            .unwrap()
            .exists(&custom_region_dir)
            .await
            .unwrap()
    );
    assert!(
        object_store_manager
            .find("default")
            .unwrap()
            .exists(&global_region_dir)
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn test_region_hook_on_drop() {
    common_telemetry::init_default_ut_logging();
    let mut env = TestEnv::with_prefix("drop_hook").await;

    let hook = Arc::new(MockRegionHook::new());
    let plugins = Plugins::new();
    plugins.insert(hook.clone() as RegionHookRef);

    let engine = env
        .create_engine_with_plugins(MitoConfig::default(), plugins)
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

    // Put some rows so the region has active data (kept in memtable; we
    // intentionally do not flush so the drop GC worker finds no parquet files
    // and removes the directory immediately, avoiding the multi-minute GC wait).
    let rows = Rows {
        schema: column_schemas,
        rows: build_rows_for_key("a", 0, 2, 0),
    };
    put_rows(&engine, region_id, rows).await;

    // Sanity: nothing fired yet besides the open from create.
    assert_eq!(hook.opened_count.load(Ordering::Relaxed), 1);
    assert_eq!(hook.dropped_count.load(Ordering::Relaxed), 0);
    assert_eq!(hook.closed_count.load(Ordering::Relaxed), 0);

    // full drop (partial_drop = false) so the GC worker physically deletes the
    // directory and fires on_region_files_removed.
    engine
        .handle_request(
            region_id,
            RegionRequest::Drop(RegionDropRequest {
                fast_path: false,
                force: false,
                partial_drop: false,
            }),
        )
        .await
        .unwrap();

    // on_region_dropped fires inline while handling the drop request.
    hook.wait_for_dropped().await;
    assert_eq!(hook.dropped_count.load(Ordering::Relaxed), 1);
    // Drop must not be confused with close.
    assert_eq!(hook.closed_count.load(Ordering::Relaxed), 0);

    // on_region_files_removed fires from the background GC worker once the
    // directory is physically deleted.
    hook.wait_for_files_removed().await;
    assert_eq!(hook.files_removed_count.load(Ordering::Relaxed), 1);
}
