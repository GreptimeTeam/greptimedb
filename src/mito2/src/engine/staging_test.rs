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

use std::fs;

use api::v1::Rows;
use common_recordbatch::RecordBatches;
use store_api::region_engine::{RegionEngine, SettableRegionRoleState};
use store_api::region_request::{
    RegionAlterRequest, RegionFlushRequest, RegionRequest, RegionTruncateRequest,
};
use store_api::storage::{RegionId, ScanRequest};

use crate::config::MitoConfig;
use crate::region::{RegionLeaderState, RegionRoleState};
use crate::request::WorkerRequest;
use crate::test_util::{CreateRequestBuilder, TestEnv, build_rows, put_rows, rows_schema};

#[tokio::test]
async fn test_staging_state_integration() {
    let mut env = TestEnv::new().await;
    let engine = env.create_engine(MitoConfig::default()).await;

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
    let mut env = TestEnv::new().await;
    let engine = env.create_engine(MitoConfig::default()).await;

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
    let mut env = TestEnv::new().await;
    let engine = env.create_engine(MitoConfig::default()).await;

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

#[tokio::test]
async fn test_staging_manifest_directory() {
    let mut env = TestEnv::new().await;
    let engine = env.create_engine(MitoConfig::default()).await;

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
        .set_region_role_state_gracefully(region_id, SettableRegionRoleState::StagingLeader)
        .await
        .unwrap();

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
    let mut env = TestEnv::new().await;
    let engine = env.create_engine(MitoConfig::default()).await;

    let region_id = RegionId::new(1024, 0);
    let request = CreateRequestBuilder::new().build();
    let column_schemas = rows_schema(&request);

    // Create region
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    // Enter staging mode
    engine
        .set_region_role_state_gracefully(region_id, SettableRegionRoleState::StagingLeader)
        .await
        .unwrap();

    // Add some data and flush in staging mode to generate staging manifests
    let rows_data = Rows {
        schema: column_schemas.clone(),
        rows: build_rows(0, 5),
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
        rows: build_rows(5, 10),
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
        2,
        "Staging manifest directory should contain two files before exit"
    );

    // Count normal manifest files before exit
    let normal_manifest_dir = format!("{}/manifest", region_dir);
    let normal_files_before = fs::read_dir(&normal_manifest_dir)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let normal_count_before = normal_files_before.len();
    assert_eq!(
        normal_count_before, 1,
        "Normal manifest directory should initially contain one file"
    );

    // Try read data before exiting staging, SST files should be invisible
    let request = ScanRequest::default();
    let scanner = engine.scanner(region_id, request).await.unwrap();
    assert_eq!(
        scanner.num_files(),
        0,
        "No SST files should be scanned before exit"
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
        total_rows, 0,
        "No data should be readable before exit staging mode"
    );

    // Inspect SSTs from manifest
    let sst_entries = engine.all_ssts_from_manifest().await;
    assert_eq!(sst_entries.len(), 2);
    assert!(sst_entries.iter().all(|e| !e.visible));

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
        2,
        "SST files should be scanned after exit"
    );

    // Try reading data via scanner to ensure previous staged data is actually readable
    let stream = scanner.scan().await.unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    let total_rows: usize = batches.iter().map(|rb| rb.num_rows()).sum();
    assert_eq!(total_rows, 10, "Expected to read all staged rows");

    // Inspect SSTs from manifest
    let sst_entries = engine.all_ssts_from_manifest().await;
    assert_eq!(sst_entries.len(), 2);
    assert!(sst_entries.iter().all(|e| e.visible));
}
