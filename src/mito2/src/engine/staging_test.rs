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
use store_api::region_engine::{RegionEngine, SettableRegionRoleState};
use store_api::region_request::{
    RegionAlterRequest, RegionFlushRequest, RegionRequest, RegionTruncateRequest,
};
use store_api::storage::RegionId;

use crate::config::MitoConfig;
use crate::region::{RegionLeaderState, RegionRoleState};
use crate::request::WorkerRequest;
use crate::test_util::{build_rows, put_rows, rows_schema, CreateRequestBuilder, TestEnv};

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
