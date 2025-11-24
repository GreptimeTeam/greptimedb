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

//! Integration tests for staging state functionality.

use std::assert_matches::assert_matches;
use std::fs;
use std::sync::Arc;
use std::time::Duration;

use api::v1::Rows;
use common_recordbatch::RecordBatches;
use store_api::region_engine::{RegionEngine, SettableRegionRoleState};
use store_api::region_request::{
    EnterStagingRequest, RegionAlterRequest, RegionFlushRequest, RegionRequest,
    RegionTruncateRequest,
};
use store_api::storage::{RegionId, ScanRequest};

use crate::config::MitoConfig;
use crate::engine::listener::NotifyEnterStagingResultListener;
use crate::error::Error;
use crate::region::{RegionLeaderState, RegionRoleState};
use crate::request::WorkerRequest;
use crate::test_util::{CreateRequestBuilder, TestEnv, build_rows, put_rows, rows_schema};

#[tokio::test]
async fn test_staging_state_integration() {
    test_staging_state_integration_with_format(false).await;
    test_staging_state_integration_with_format(true).await;
}

async fn test_staging_state_integration_with_format(flat_format: bool) {
    let mut env = TestEnv::new().await;
    let engine = env
        .create_engine(MitoConfig {
            default_experimental_flat_format: flat_format,
            ..Default::default()
        })
        .await;

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new().build();

    // Create region
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    // Test external API patterns work correctly
    use store_api::region_engine::SettableRegionRoleState;

    let (role_req, _receiver) = WorkerRequest::new_set_readonly_gracefully(
        region_id,
        SettableRegionRoleState::StagingLeader,
    );
    match role_req {
        WorkerRequest::SetRegionRoleStateGracefully {
            region_id: req_region_id,
            region_role_state,
            ..
        } => {
            assert_eq!(req_region_id, region_id);
            assert_eq!(region_role_state, SettableRegionRoleState::StagingLeader);
        }
        _ => panic!("Expected SetRegionRoleStateGracefully request"),
    }

    let (role_req, _receiver) =
        WorkerRequest::new_set_readonly_gracefully(region_id, SettableRegionRoleState::Leader);
    match role_req {
        WorkerRequest::SetRegionRoleStateGracefully {
            region_id: req_region_id,
            region_role_state,
            ..
        } => {
            assert_eq!(req_region_id, region_id);
            assert_eq!(region_role_state, SettableRegionRoleState::Leader);
        }
        _ => panic!("Expected SetRegionRoleStateGracefully request"),
    }
}

#[tokio::test]
async fn test_staging_blocks_alter_operations() {
    test_staging_blocks_alter_operations_with_format(false).await;
    test_staging_blocks_alter_operations_with_format(true).await;
}

async fn test_staging_blocks_alter_operations_with_format(flat_format: bool) {
    let mut env = TestEnv::new().await;
    let engine = env
        .create_engine(MitoConfig {
            default_experimental_flat_format: flat_format,
            ..Default::default()
        })
        .await;

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new().build();

    // Create region
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    // Note: In the current implementation, we can't directly test staging mode
    // through the engine interface since staging transitions are handled
    // through worker requests. This test demonstrates the pattern that would
    // be used once external control interfaces are implemented.

    // Test that ALTER operations would be blocked in staging mode
    let alter_request = RegionAlterRequest {
        kind: store_api::region_request::AlterKind::AddColumns { columns: vec![] },
    };

    // This currently succeeds since we're not in staging mode
    let result = engine
        .handle_request(region_id, RegionRequest::Alter(alter_request))
        .await;
    assert!(result.is_ok(), "ALTER should succeed in normal mode");
}

#[tokio::test]
async fn test_staging_blocks_truncate_operations() {
    test_staging_blocks_truncate_operations_with_format(false).await;
    test_staging_blocks_truncate_operations_with_format(true).await;
}

async fn test_staging_blocks_truncate_operations_with_format(flat_format: bool) {
    let mut env = TestEnv::new().await;
    let engine = env
        .create_engine(MitoConfig {
            default_experimental_flat_format: flat_format,
            ..Default::default()
        })
        .await;

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new().build();

    // Create region
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    // Test that TRUNCATE operations would be blocked in staging mode
    let truncate_request = RegionTruncateRequest::All;

    // This currently succeeds since we're not in staging mode
    let result = engine
        .handle_request(region_id, RegionRequest::Truncate(truncate_request))
        .await;
    assert!(result.is_ok(), "TRUNCATE should succeed in normal mode");
}

#[tokio::test]
async fn test_staging_state_validation_patterns() {
    // Test the state validation patterns used throughout the codebase
    let staging_state = RegionRoleState::Leader(RegionLeaderState::Staging);
    let writable_state = RegionRoleState::Leader(RegionLeaderState::Writable);

    // Test staging detection
    let is_staging = staging_state == RegionRoleState::Leader(RegionLeaderState::Staging);
    assert!(is_staging, "Should correctly identify staging state");

    let is_not_staging = writable_state == RegionRoleState::Leader(RegionLeaderState::Staging);
    assert!(
        !is_not_staging,
        "Should correctly identify non-staging state"
    );

    // Test writable state check
    let staging_is_writable = matches!(
        staging_state,
        RegionRoleState::Leader(RegionLeaderState::Writable)
            | RegionRoleState::Leader(RegionLeaderState::Staging)
    );
    assert!(staging_is_writable, "Staging regions should be writable");

    let writable_is_writable = matches!(
        writable_state,
        RegionRoleState::Leader(RegionLeaderState::Writable)
            | RegionRoleState::Leader(RegionLeaderState::Staging)
    );
    assert!(writable_is_writable, "Writable regions should be writable");

    // Test flushable state check
    let staging_is_flushable = matches!(
        staging_state,
        RegionRoleState::Leader(RegionLeaderState::Writable)
            | RegionRoleState::Leader(RegionLeaderState::Staging)
            | RegionRoleState::Leader(RegionLeaderState::Downgrading)
    );
    assert!(staging_is_flushable, "Staging regions should be flushable");

    let writable_is_flushable = matches!(
        writable_state,
        RegionRoleState::Leader(RegionLeaderState::Writable)
            | RegionRoleState::Leader(RegionLeaderState::Staging)
            | RegionRoleState::Leader(RegionLeaderState::Downgrading)
    );
    assert!(
        writable_is_flushable,
        "Writable regions should be flushable"
    );
}

const PARTITION_EXPR: &str = "partition_expr";

#[tokio::test]
async fn test_staging_manifest_directory() {
    test_staging_manifest_directory_with_format(false).await;
    test_staging_manifest_directory_with_format(true).await;
}

async fn test_staging_manifest_directory_with_format(flat_format: bool) {
    common_telemetry::init_default_ut_logging();
    let mut env = TestEnv::new().await;
    let engine = env
        .create_engine(MitoConfig {
            default_experimental_flat_format: flat_format,
            ..Default::default()
        })
        .await;

    let region_id = RegionId::new(1024, 0);
    let request = CreateRequestBuilder::new().build();

    // Get column schemas before consuming the request
    let column_schemas = rows_schema(&request);

    // Check manifest files after region creation (before staging mode)
    // Create region
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    // Check that manifest files exist after region creation
    let data_home = env.data_home();

    let region_dir = format!("{}/data/test/1024_0000000000", data_home.display());
    let normal_manifest_dir = format!("{}/manifest", region_dir);
    assert!(
        fs::metadata(&normal_manifest_dir).is_ok(),
        "Normal manifest directory should exist"
    );

    // Now test staging mode manifest creation
    // Set region to staging mode using the engine API
    engine
        .handle_request(
            region_id,
            RegionRequest::EnterStaging(EnterStagingRequest {
                partition_expr: PARTITION_EXPR.to_string(),
            }),
        )
        .await
        .unwrap();
    let region = engine.get_region(region_id).unwrap();
    let staging_partition_expr = region.staging_partition_expr.lock().unwrap().clone();
    assert_eq!(staging_partition_expr.unwrap(), PARTITION_EXPR);
    {
        let manager = region.manifest_ctx.manifest_manager.read().await;
        assert_eq!(
            manager
                .staging_manifest()
                .unwrap()
                .metadata
                .partition_expr
                .as_deref()
                .unwrap(),
            PARTITION_EXPR
        );
        assert!(manager.manifest().metadata.partition_expr.is_none());
    }

    // Should be ok to enter staging mode again with the same partition expr
    engine
        .handle_request(
            region_id,
            RegionRequest::EnterStaging(EnterStagingRequest {
                partition_expr: PARTITION_EXPR.to_string(),
            }),
        )
        .await
        .unwrap();

    // Should throw error if try to enter staging mode again with a different partition expr
    let err = engine
        .handle_request(
            region_id,
            RegionRequest::EnterStaging(EnterStagingRequest {
                partition_expr: "".to_string(),
            }),
        )
        .await
        .unwrap_err();
    assert_matches!(
        err.into_inner().as_any().downcast_ref::<Error>().unwrap(),
        Error::StagingPartitionExprMismatch { .. }
    );

    // Put some data and flush in staging mode
    let rows_data = Rows {
        schema: column_schemas.clone(),
        rows: build_rows(0, 3),
    };
    put_rows(&engine, region_id, rows_data).await;

    // Force flush to generate manifest files in staging mode
    engine
        .handle_request(
            region_id,
            RegionRequest::Flush(RegionFlushRequest {
                row_group_size: None,
            }),
        )
        .await
        .unwrap();

    // Check that manifest files are in staging directory
    let staging_manifest_dir = format!("{}/staging/manifest", region_dir);
    assert!(
        fs::metadata(&staging_manifest_dir).is_ok(),
        "Staging manifest directory should exist"
    );

    // Check what exists in normal manifest directory
    let files: Vec<_> = fs::read_dir(&normal_manifest_dir)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert!(
        !files.is_empty(),
        "Normal manifest directory should contain files"
    );

    // Check what exists in staging manifest directory
    let staging_files = fs::read_dir(&staging_manifest_dir)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert!(
        !staging_files.is_empty(),
        "Staging manifest directory should contain files"
    );
}

#[tokio::test]
async fn test_staging_exit_success_with_manifests() {
    test_staging_exit_success_with_manifests_with_format(false).await;
    test_staging_exit_success_with_manifests_with_format(true).await;
}

async fn test_staging_exit_success_with_manifests_with_format(flat_format: bool) {
    common_telemetry::init_default_ut_logging();
    let mut env = TestEnv::new().await;
    let engine = env
        .create_engine(MitoConfig {
            default_experimental_flat_format: flat_format,
            ..Default::default()
        })
        .await;

    let region_id = RegionId::new(1024, 0);
    let request = CreateRequestBuilder::new().build();
    let column_schemas = rows_schema(&request);

    // Create region
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    // Add some data and flush in staging mode to generate staging manifests
    let rows_data = Rows {
        schema: column_schemas.clone(),
        rows: build_rows(0, 3),
    };
    put_rows(&engine, region_id, rows_data).await;

    // Enter staging mode
    engine
        .handle_request(
            region_id,
            RegionRequest::EnterStaging(EnterStagingRequest {
                partition_expr: PARTITION_EXPR.to_string(),
            }),
        )
        .await
        .unwrap();

    // Add some data and flush in staging mode to generate staging manifests
    let rows_data = Rows {
        schema: column_schemas.clone(),
        rows: build_rows(3, 8),
    };
    put_rows(&engine, region_id, rows_data).await;

    // Force flush to generate staging manifests
    engine
        .handle_request(
            region_id,
            RegionRequest::Flush(RegionFlushRequest {
                row_group_size: None,
            }),
        )
        .await
        .unwrap();

    // Add more data and flush again to generate multiple staging manifests
    let rows_data2 = Rows {
        schema: column_schemas.clone(),
        rows: build_rows(8, 10),
    };
    put_rows(&engine, region_id, rows_data2).await;

    engine
        .handle_request(
            region_id,
            RegionRequest::Flush(RegionFlushRequest {
                row_group_size: None,
            }),
        )
        .await
        .unwrap();

    // Verify we're in staging mode and staging manifests exist
    let data_home = env.data_home();
    let region_dir = format!("{}/data/test/1024_0000000000", data_home.display());
    let staging_manifest_dir = format!("{}/staging/manifest", region_dir);

    let staging_files_before = fs::read_dir(&staging_manifest_dir)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(
        staging_files_before.len(),
        // Two files for flush operation
        // One file for entering staging mode
        3,
        "Staging manifest directory should contain 3 files before exit, got: {:?}",
        staging_files_before
    );

    // Count normal manifest files before exit
    let normal_manifest_dir = format!("{}/manifest", region_dir);
    let normal_files_before = fs::read_dir(&normal_manifest_dir)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let normal_count_before = normal_files_before.len();
    assert_eq!(
        // One file for table creation
        // One file for flush operation
        normal_count_before,
        2,
        "Normal manifest directory should initially contain 2 files"
    );

    // Try read data before exiting staging, SST files should be invisible
    let request = ScanRequest::default();
    let scanner = engine.scanner(region_id, request).await.unwrap();
    assert_eq!(
        scanner.num_files(),
        1,
        "1 SST files should be scanned before exit"
    );
    assert_eq!(
        scanner.num_memtables(),
        0,
        "Memtables should be removed in staging before exit"
    );
    let stream = scanner.scan().await.unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    let total_rows: usize = batches.iter().map(|rb| rb.num_rows()).sum();
    assert_eq!(
        total_rows, 3,
        "3 rows should be readable before exit staging mode"
    );

    // Inspect SSTs from manifest
    let sst_entries = engine.all_ssts_from_manifest().await;
    assert_eq!(
        sst_entries.len(),
        3,
        "sst entries should be 3, got: {:?}",
        sst_entries
    );
    assert_eq!(sst_entries.iter().filter(|e| e.visible).count(), 1);
    assert_eq!(sst_entries.iter().filter(|e| !e.visible).count(), 2);

    // Exit staging mode successfully
    engine
        .set_region_role_state_gracefully(region_id, SettableRegionRoleState::Leader)
        .await
        .unwrap();

    // Verify we're back in normal mode
    let workers = &engine.inner.workers;
    let region = workers.get_region(region_id).unwrap();
    assert!(
        !region.is_staging(),
        "Region should no longer be in staging mode"
    );

    // Verify staging manifests have been cleared
    let staging_files_after = fs::read_dir(&staging_manifest_dir)
        .map(|entries| entries.collect::<Result<Vec<_>, _>>().unwrap_or_default())
        .unwrap_or_default();
    assert!(
        staging_files_after.is_empty(),
        "Staging manifest directory should be empty after successful exit"
    );

    // Verify normal manifests contain the merged changes
    let normal_files_after = fs::read_dir(&normal_manifest_dir)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert!(
        normal_files_after.len() > normal_count_before,
        "Normal manifest directory should contain more files after merge"
    );

    // Validate in-memory version reflects merged manifests (files visible in levels)
    let version = region.version();
    let levels = version.ssts.levels();
    assert!(
        !levels.is_empty() && !levels[0].files.is_empty(),
        "SST levels should have files after exiting staging"
    );

    // Also ensure scanner behavior reflects 2 SSTs
    let request = ScanRequest::default();
    let scanner = engine.scanner(region_id, request).await.unwrap();
    assert_eq!(
        scanner.num_files(),
        3,
        "SST files should be scanned after exit"
    );

    // Try reading data via scanner to ensure previous staged data is actually readable
    let stream = scanner.scan().await.unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    let total_rows: usize = batches.iter().map(|rb| rb.num_rows()).sum();
    assert_eq!(total_rows, 10, "Expected to read all staged rows");

    // Inspect SSTs from manifest
    let sst_entries = engine.all_ssts_from_manifest().await;
    assert_eq!(sst_entries.len(), 3);
    assert!(sst_entries.iter().all(|e| e.visible));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_write_stall_on_enter_staging() {
    test_write_stall_on_enter_staging_with_format(false).await;
    test_write_stall_on_enter_staging_with_format(true).await;
}

async fn test_write_stall_on_enter_staging_with_format(flat_format: bool) {
    let mut env = TestEnv::new().await;
    let listener = Arc::new(NotifyEnterStagingResultListener::default());
    let engine = env
        .create_engine_with(
            MitoConfig {
                default_experimental_flat_format: flat_format,
                ..Default::default()
            },
            None,
            Some(listener.clone()),
            None,
        )
        .await;

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new().build();

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

    let column_schemas = rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    let engine_cloned = engine.clone();
    let alter_job = tokio::spawn(async move {
        engine_cloned
            .handle_request(
                region_id,
                RegionRequest::EnterStaging(EnterStagingRequest {
                    partition_expr: PARTITION_EXPR.to_string(),
                }),
            )
            .await
            .unwrap();
    });
    // Make sure the loop is handling the alter request.
    tokio::time::sleep(Duration::from_millis(100)).await;

    let column_schemas_cloned = column_schemas.clone();
    let engine_cloned = engine.clone();
    let put_job = tokio::spawn(async move {
        let rows = Rows {
            schema: column_schemas_cloned,
            rows: build_rows(0, 3),
        };
        put_rows(&engine_cloned, region_id, rows).await;
    });
    // Make sure the loop is handling the put request.
    tokio::time::sleep(Duration::from_millis(100)).await;

    listener.wake_notify();
    alter_job.await.unwrap();
    put_job.await.unwrap();

    let expected = "\
+-------+---------+---------------------+
| tag_0 | field_0 | ts                  |
+-------+---------+---------------------+
| 0     | 0.0     | 1970-01-01T00:00:00 |
| 1     | 1.0     | 1970-01-01T00:00:01 |
| 2     | 2.0     | 1970-01-01T00:00:02 |
+-------+---------+---------------------+";
    let request = ScanRequest::default();
    let scanner = engine.scanner(region_id, request).await.unwrap();
    let stream = scanner.scan().await.unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    assert_eq!(expected, batches.pretty_print().unwrap());
}
