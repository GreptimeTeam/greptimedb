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

use api::v1::Rows;
use common_error::ext::ErrorExt;
use common_error::status_code::StatusCode;
use store_api::region_engine::{
    RegionEngine, RegionRole, SetRegionRoleStateResponse, SetRegionRoleStateSuccess,
    SettableRegionRoleState,
};
use store_api::region_request::{RegionPutRequest, RegionRequest};
use store_api::storage::RegionId;

use crate::config::MitoConfig;
use crate::test_util::{CreateRequestBuilder, TestEnv, build_rows, put_rows, rows_schema};

/// Helper function to assert a successful response with expected entry id
fn assert_success_response(response: &SetRegionRoleStateResponse, expected_entry_id: u64) {
    match response {
        SetRegionRoleStateResponse::Success(SetRegionRoleStateSuccess::Mito { last_entry_id }) => {
            assert_eq!(*last_entry_id, expected_entry_id);
        }
        _ => panic!("Expected success response, got: {:?}", response),
    }
}

/// Helper function to assert a NotFound response
fn assert_not_found_response(response: &SetRegionRoleStateResponse) {
    match response {
        SetRegionRoleStateResponse::NotFound => {
            // Expected - do nothing
        }
        _ => panic!("Expected NotFound response, got: {:?}", response),
    }
}

/// Helper function to assert an InvalidTransition response
fn assert_invalid_transition_response(response: &SetRegionRoleStateResponse) {
    match response {
        SetRegionRoleStateResponse::InvalidTransition(_) => {
            // Expected - do nothing
        }
        _ => panic!("Expected InvalidTransition response, got: {:?}", response),
    }
}

#[tokio::test]
async fn test_set_role_state_gracefully() {
    test_set_role_state_gracefully_with_format(false).await;
    test_set_role_state_gracefully_with_format(true).await;
}

async fn test_set_role_state_gracefully_with_format(flat_format: bool) {
    let settable_role_states = [
        SettableRegionRoleState::Follower,
        SettableRegionRoleState::DowngradingLeader,
    ];
    for settable_role_state in settable_role_states {
        let mut env = TestEnv::new().await;
        let engine = env
            .create_engine(MitoConfig {
                default_experimental_flat_format: flat_format,
                ..Default::default()
            })
            .await;

        let region_id = RegionId::new(1, 1);
        let request = CreateRequestBuilder::new().build();

        let column_schemas = rows_schema(&request);
        engine
            .handle_request(region_id, RegionRequest::Create(request))
            .await
            .unwrap();

        let result = engine
            .set_region_role_state_gracefully(region_id, settable_role_state)
            .await
            .unwrap();
        assert_success_response(&result, 0);

        // set Follower again.
        let result = engine
            .set_region_role_state_gracefully(region_id, settable_role_state)
            .await
            .unwrap();
        assert_success_response(&result, 0);

        let rows = Rows {
            schema: column_schemas,
            rows: build_rows(0, 3),
        };

        let error = engine
            .handle_request(
                region_id,
                RegionRequest::Put(RegionPutRequest {
                    rows: rows.clone(),
                    hint: None,
                    partition_expr_version: None,
                }),
            )
            .await
            .unwrap_err();

        assert_eq!(error.status_code(), StatusCode::RegionNotReady);

        engine
            .set_region_role(region_id, RegionRole::Leader)
            .unwrap();

        put_rows(&engine, region_id, rows).await;

        let result = engine
            .set_region_role_state_gracefully(region_id, settable_role_state)
            .await
            .unwrap();

        assert_success_response(&result, 1);
    }
}

#[tokio::test]
async fn test_set_role_state_gracefully_not_exist() {
    test_set_role_state_gracefully_not_exist_with_format(false).await;
    test_set_role_state_gracefully_not_exist_with_format(true).await;
}

async fn test_set_role_state_gracefully_not_exist_with_format(flat_format: bool) {
    let mut env = TestEnv::new().await;
    let engine = env
        .create_engine(MitoConfig {
            default_experimental_flat_format: flat_format,
            ..Default::default()
        })
        .await;

    let non_exist_region_id = RegionId::new(1, 1);

    // For fast-path.
    let result = engine
        .set_region_role_state_gracefully(non_exist_region_id, SettableRegionRoleState::Follower)
        .await
        .unwrap();
    assert_not_found_response(&result);
}

#[tokio::test]
async fn test_write_downgrading_region() {
    test_write_downgrading_region_with_format(false).await;
    test_write_downgrading_region_with_format(true).await;
}

async fn test_write_downgrading_region_with_format(flat_format: bool) {
    let mut env = TestEnv::with_prefix("write-to-downgrading-region").await;
    let engine = env
        .create_engine(MitoConfig {
            default_experimental_flat_format: flat_format,
            ..Default::default()
        })
        .await;

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new().build();

    let column_schemas = rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    let rows = Rows {
        schema: column_schemas.clone(),
        rows: build_rows(0, 42),
    };
    put_rows(&engine, region_id, rows).await;

    let result = engine
        .set_region_role_state_gracefully(region_id, SettableRegionRoleState::DowngradingLeader)
        .await
        .unwrap();
    assert_success_response(&result, 1);

    let rows = Rows {
        schema: column_schemas,
        rows: build_rows(0, 42),
    };
    let err = engine
        .handle_request(
            region_id,
            RegionRequest::Put(RegionPutRequest {
                rows: rows.clone(),
                hint: None,
                partition_expr_version: None,
            }),
        )
        .await
        .unwrap_err();
    assert_eq!(err.status_code(), StatusCode::RegionNotReady)
}

#[tokio::test]
async fn test_unified_state_transitions() {
    test_unified_state_transitions_with_format(false).await;
    test_unified_state_transitions_with_format(true).await;
}

async fn test_unified_state_transitions_with_format(flat_format: bool) {
    let mut env = TestEnv::new().await;
    let engine = env
        .create_engine(MitoConfig {
            default_experimental_flat_format: flat_format,
            ..Default::default()
        })
        .await;

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new().build();

    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    // Test all transitions from normal leader state

    // Leader -> StagingLeader -> Leader
    let result = engine
        .set_region_role_state_gracefully(region_id, SettableRegionRoleState::StagingLeader)
        .await
        .unwrap();
    assert_success_response(&result, 0);

    let result = engine
        .set_region_role_state_gracefully(region_id, SettableRegionRoleState::Leader)
        .await
        .unwrap();
    assert_success_response(&result, 0);

    // Leader -> StagingLeader -> Follower (exit staging via demotion)
    engine
        .set_region_role_state_gracefully(region_id, SettableRegionRoleState::StagingLeader)
        .await
        .unwrap();

    let result = engine
        .set_region_role_state_gracefully(region_id, SettableRegionRoleState::Follower)
        .await
        .unwrap();
    assert_success_response(&result, 0);

    // Note: Direct Follower -> Leader promotion is no longer allowed
    // Use existing set_region_role method for follower -> leader promotion
    engine
        .set_region_role(region_id, RegionRole::Leader)
        .unwrap();

    // Leader -> StagingLeader -> DowngradingLeader (exit staging via downgrade)
    engine
        .set_region_role_state_gracefully(region_id, SettableRegionRoleState::StagingLeader)
        .await
        .unwrap();

    let result = engine
        .set_region_role_state_gracefully(region_id, SettableRegionRoleState::DowngradingLeader)
        .await
        .unwrap();
    assert_success_response(&result, 0);

    // Note: Direct DowngradingLeader -> Leader is no longer allowed
    // Use existing set_region_role method for downgrading -> leader promotion
    engine
        .set_region_role(region_id, RegionRole::Leader)
        .unwrap();

    // Test idempotent operations (no-op cases)

    // Leader -> Leader (should be no-op)
    let result = engine
        .set_region_role_state_gracefully(region_id, SettableRegionRoleState::Leader)
        .await
        .unwrap();
    assert_success_response(&result, 0);

    // StagingLeader -> StagingLeader (should be no-op)
    engine
        .set_region_role_state_gracefully(region_id, SettableRegionRoleState::StagingLeader)
        .await
        .unwrap();

    let result = engine
        .set_region_role_state_gracefully(region_id, SettableRegionRoleState::StagingLeader)
        .await
        .unwrap();
    assert_success_response(&result, 0);

    // Back to follower for final test
    engine
        .set_region_role_state_gracefully(region_id, SettableRegionRoleState::Follower)
        .await
        .unwrap();

    // Follower -> Follower (should be no-op)
    let result = engine
        .set_region_role_state_gracefully(region_id, SettableRegionRoleState::Follower)
        .await
        .unwrap();
    assert_success_response(&result, 0);
}

#[tokio::test]
async fn test_restricted_state_transitions() {
    test_restricted_state_transitions_with_format(false).await;
    test_restricted_state_transitions_with_format(true).await;
}

async fn test_restricted_state_transitions_with_format(flat_format: bool) {
    let mut env = TestEnv::new().await;
    let engine = env
        .create_engine(MitoConfig {
            default_experimental_flat_format: flat_format,
            ..Default::default()
        })
        .await;

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new().build();

    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    // Test that Leader transition from follower is rejected with InvalidTransition
    engine
        .set_region_role(region_id, RegionRole::Follower)
        .unwrap();

    let result = engine
        .set_region_role_state_gracefully(region_id, SettableRegionRoleState::Leader)
        .await
        .unwrap();
    assert_invalid_transition_response(&result);

    // Test that Leader transition from downgrading is rejected with InvalidTransition
    engine
        .set_region_role(region_id, RegionRole::Leader)
        .unwrap();
    engine
        .set_region_role(region_id, RegionRole::DowngradingLeader)
        .unwrap();

    let result = engine
        .set_region_role_state_gracefully(region_id, SettableRegionRoleState::Leader)
        .await
        .unwrap();
    assert_invalid_transition_response(&result);

    // Test that StagingLeader transition from follower is rejected with InvalidTransition
    engine
        .set_region_role(region_id, RegionRole::Follower)
        .unwrap();

    let result = engine
        .set_region_role_state_gracefully(region_id, SettableRegionRoleState::StagingLeader)
        .await
        .unwrap();
    assert_invalid_transition_response(&result);

    // Test that StagingLeader transition from downgrading is rejected with InvalidTransition
    engine
        .set_region_role(region_id, RegionRole::Leader)
        .unwrap();
    engine
        .set_region_role(region_id, RegionRole::DowngradingLeader)
        .unwrap();

    let result = engine
        .set_region_role_state_gracefully(region_id, SettableRegionRoleState::StagingLeader)
        .await
        .unwrap();
    assert_invalid_transition_response(&result);

    // Test that valid staging workflow still works
    engine
        .set_region_role(region_id, RegionRole::Leader)
        .unwrap();

    // Writable Leader -> StagingLeader should work
    let result = engine
        .set_region_role_state_gracefully(region_id, SettableRegionRoleState::StagingLeader)
        .await
        .unwrap();
    assert_success_response(&result, 0);

    // Staging -> Leader should work
    let result = engine
        .set_region_role_state_gracefully(region_id, SettableRegionRoleState::Leader)
        .await
        .unwrap();
    assert_success_response(&result, 0);
}
