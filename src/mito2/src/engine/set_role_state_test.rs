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

use std::time::Duration;

use api::v1::Rows;
use common_error::ext::ErrorExt;
use common_error::status_code::StatusCode;
use store_api::region_engine::{
    RegionEngine, RegionRole, SetRegionRoleStateResponse, SetRegionRoleStateSuccess,
    SettableRegionRoleState,
};
use store_api::region_request::{
    EnterStagingRequest, RegionFlushRequest, RegionPutRequest, RegionRequest,
    StagingPartitionDirective,
};
use store_api::storage::RegionId;

use crate::config::MitoConfig;
use crate::region::{RegionLeaderState, RegionRoleState};
use crate::test_util::{
    CheckpointTaskBlocker, CreateRequestBuilder, TestEnv, build_rows, put_rows, rows_schema,
};

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
                default_flat_format: flat_format,
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
            default_flat_format: flat_format,
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
            default_flat_format: flat_format,
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

#[tokio::test(flavor = "multi_thread")]
async fn test_downgrading_waits_for_checkpoint_and_stops_new_checkpoints() {
    let (blocker, mock_layer) = CheckpointTaskBlocker::block_cleanup();
    let mut env = TestEnv::new().await.with_mock_layer(mock_layer);
    let engine = env
        .create_engine(MitoConfig {
            manifest_checkpoint_distance: 1,
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

    put_rows(
        &engine,
        region_id,
        Rows {
            schema: column_schemas.clone(),
            rows: build_rows(0, 1),
        },
    )
    .await;
    engine
        .handle_request(
            region_id,
            RegionRequest::Flush(RegionFlushRequest::default()),
        )
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), blocker.wait_until_blocked())
        .await
        .expect("checkpoint cleanup did not start");

    // Leave one memtable for the final flush after entering Downgrading.
    put_rows(
        &engine,
        region_id,
        Rows {
            schema: column_schemas.clone(),
            rows: build_rows(1, 2),
        },
    )
    .await;

    let region = engine.get_region(region_id).unwrap();
    let wait_started = region
        .manifest_ctx
        .manifest_manager
        .read()
        .await
        .checkpointer()
        .pending_checkpoint_wait_started();
    // Register before starting the transition so the notification cannot be lost.
    let wait_started = wait_started.notified();
    let cloned_engine = engine.clone();
    let downgrade = tokio::spawn(async move {
        cloned_engine
            .set_region_role_state_gracefully(region_id, SettableRegionRoleState::DowngradingLeader)
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), wait_started)
        .await
        .expect("downgrade did not start waiting for the checkpoint");
    assert_eq!(
        RegionRoleState::Leader(RegionLeaderState::Downgrading),
        region.state()
    );
    assert!(
        !downgrade.is_finished(),
        "downgrade returned before checkpoint cleanup finished"
    );

    // Cancel the first transition while it is waiting. The checkpoint handle
    // must remain owned by the manager so an idempotent retry can join it.
    downgrade.abort();
    assert!(downgrade.await.unwrap_err().is_cancelled());
    let retry_wait_started = region
        .manifest_ctx
        .manifest_manager
        .read()
        .await
        .checkpointer()
        .pending_checkpoint_wait_started();
    let retry_wait_started = retry_wait_started.notified();
    let cloned_engine = engine.clone();
    let retry_downgrade = tokio::spawn(async move {
        cloned_engine
            .set_region_role_state_gracefully(region_id, SettableRegionRoleState::DowngradingLeader)
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), retry_wait_started)
        .await
        .expect("idempotent downgrade retry did not start waiting for the checkpoint");
    assert!(
        !retry_downgrade.is_finished(),
        "idempotent downgrade retry did not wait for checkpoint cleanup"
    );

    blocker.release();
    assert_success_response(&retry_downgrade.await.unwrap().unwrap(), 2);

    // Final flush publishes a normal delta, but Downgrading must not start a
    // checkpoint after the barrier.
    blocker.arm_next_close();
    engine
        .handle_request(
            region_id,
            RegionRequest::Flush(RegionFlushRequest::default()),
        )
        .await
        .unwrap();
    assert!(
        !region
            .manifest_ctx
            .manifest_manager
            .read()
            .await
            .checkpointer()
            .is_doing_checkpoint(),
        "final flush started a checkpoint while Downgrading"
    );
    assert_eq!(2, region.manifest_ctx.manifest().await.manifest_version);
    assert_eq!(
        1,
        region
            .manifest_ctx
            .manifest_manager
            .read()
            .await
            .checkpointer()
            .last_checkpoint_version()
    );

    // Leaving Downgrading removes the scheduling restriction. The next normal
    // manifest update can checkpoint the accumulated deltas.
    engine
        .set_region_role(region_id, RegionRole::Leader)
        .unwrap();
    put_rows(
        &engine,
        region_id,
        Rows {
            schema: column_schemas,
            rows: build_rows(2, 3),
        },
    )
    .await;
    engine
        .handle_request(
            region_id,
            RegionRequest::Flush(RegionFlushRequest::default()),
        )
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), blocker.wait_until_blocked())
        .await
        .expect("checkpoint scheduling did not resume after leaving Downgrading");
    blocker.release();
    region
        .manifest_ctx
        .manifest_manager
        .write()
        .await
        .wait_for_pending_checkpoint()
        .await;
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
            default_flat_format: flat_format,
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
    assert_eq!(engine.role(region_id), Some(RegionRole::StagingLeader));

    let result = engine
        .set_region_role_state_gracefully(region_id, SettableRegionRoleState::Leader)
        .await
        .unwrap();
    assert_success_response(&result, 0);
    assert_eq!(engine.role(region_id), Some(RegionRole::Leader));

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
    assert_eq!(engine.role(region_id), Some(RegionRole::Follower));

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
    assert_eq!(engine.role(region_id), Some(RegionRole::DowngradingLeader));

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

#[tokio::test]
async fn test_direct_set_region_role_staging_leader_is_noop() {
    let mut env = TestEnv::new().await;
    let engine = env.create_engine(MitoConfig::default()).await;

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new().build();

    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    engine
        .set_region_role(region_id, RegionRole::StagingLeader)
        .unwrap();

    assert_eq!(engine.role(region_id), Some(RegionRole::Leader));

    engine
        .set_region_role(region_id, RegionRole::Follower)
        .unwrap();
    engine
        .set_region_role(region_id, RegionRole::StagingLeader)
        .unwrap();

    assert_eq!(engine.role(region_id), Some(RegionRole::Follower));
}

#[tokio::test]
async fn test_direct_set_region_role_exits_staging_state_only() {
    let mut env = TestEnv::new().await;
    let engine = env.create_engine(MitoConfig::default()).await;

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new().build();

    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    engine
        .handle_request(
            region_id,
            RegionRequest::EnterStaging(EnterStagingRequest {
                partition_directive: StagingPartitionDirective::RejectAllWrites,
            }),
        )
        .await
        .unwrap();
    assert_eq!(engine.role(region_id), Some(RegionRole::StagingLeader));
    assert!(
        engine
            .get_region(region_id)
            .unwrap()
            .manifest_ctx
            .staging_partition_info()
            .is_some()
    );

    engine
        .set_region_role(region_id, RegionRole::Leader)
        .unwrap();
    assert_eq!(engine.role(region_id), Some(RegionRole::Leader));
    assert!(
        engine
            .get_region(region_id)
            .unwrap()
            .manifest_ctx
            .staging_partition_info()
            .is_none()
    );
}

#[tokio::test]
async fn test_set_region_role_can_exit_staging_to_leader() {
    let mut env = TestEnv::new().await;
    let engine = env.create_engine(MitoConfig::default()).await;

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new().build();

    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    engine
        .set_region_role_state_gracefully(region_id, SettableRegionRoleState::StagingLeader)
        .await
        .unwrap();
    assert_eq!(engine.role(region_id), Some(RegionRole::StagingLeader));

    engine
        .set_region_role(region_id, RegionRole::Leader)
        .unwrap();

    assert_eq!(engine.role(region_id), Some(RegionRole::Leader));
    assert!(
        engine
            .get_region(region_id)
            .unwrap()
            .manifest_ctx
            .staging_partition_info()
            .is_none()
    );
}

#[tokio::test]
async fn test_set_region_role_leader_clears_staging_partition_info() {
    let mut env = TestEnv::new().await;
    let engine = env.create_engine(MitoConfig::default()).await;

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new().build();

    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    engine
        .handle_request(
            region_id,
            RegionRequest::EnterStaging(EnterStagingRequest {
                partition_directive: StagingPartitionDirective::RejectAllWrites,
            }),
        )
        .await
        .unwrap();

    let region = engine.get_region(region_id).unwrap();
    assert!(region.manifest_ctx.staging_partition_info().is_some());

    engine
        .set_region_role(region_id, RegionRole::Leader)
        .unwrap();

    let region = engine.get_region(region_id).unwrap();
    assert_eq!(engine.role(region_id), Some(RegionRole::Leader));
    assert!(region.manifest_ctx.staging_partition_info().is_none());
}

#[tokio::test]
async fn test_set_region_role_follower_clears_staging_partition_info() {
    let mut env = TestEnv::new().await;
    let engine = env.create_engine(MitoConfig::default()).await;

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new().build();

    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    engine
        .handle_request(
            region_id,
            RegionRequest::EnterStaging(EnterStagingRequest {
                partition_directive: StagingPartitionDirective::RejectAllWrites,
            }),
        )
        .await
        .unwrap();

    let region = engine.get_region(region_id).unwrap();
    assert!(region.manifest_ctx.staging_partition_info().is_some());

    engine
        .set_region_role(region_id, RegionRole::Follower)
        .unwrap();

    let region = engine.get_region(region_id).unwrap();
    assert_eq!(engine.role(region_id), Some(RegionRole::Follower));
    assert!(region.manifest_ctx.staging_partition_info().is_none());
}

#[tokio::test]
async fn test_set_region_role_downgrading_leader_clears_staging_partition_info() {
    let mut env = TestEnv::new().await;
    let engine = env.create_engine(MitoConfig::default()).await;

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new().build();

    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    engine
        .handle_request(
            region_id,
            RegionRequest::EnterStaging(EnterStagingRequest {
                partition_directive: StagingPartitionDirective::RejectAllWrites,
            }),
        )
        .await
        .unwrap();

    let region = engine.get_region(region_id).unwrap();
    assert!(region.manifest_ctx.staging_partition_info().is_some());

    engine
        .set_region_role(region_id, RegionRole::DowngradingLeader)
        .unwrap();

    let region = engine.get_region(region_id).unwrap();
    assert_eq!(engine.role(region_id), Some(RegionRole::DowngradingLeader));
    assert!(region.manifest_ctx.staging_partition_info().is_none());
}

#[tokio::test]
async fn test_can_reenter_staging_after_direct_exit_cleanup() {
    let mut env = TestEnv::new().await;
    let engine = env.create_engine(MitoConfig::default()).await;

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new().build();

    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    engine
        .handle_request(
            region_id,
            RegionRequest::EnterStaging(EnterStagingRequest {
                partition_directive: StagingPartitionDirective::RejectAllWrites,
            }),
        )
        .await
        .unwrap();
    engine
        .set_region_role(region_id, RegionRole::Follower)
        .unwrap();
    engine
        .set_region_role(region_id, RegionRole::Leader)
        .unwrap();

    engine
        .handle_request(
            region_id,
            RegionRequest::EnterStaging(EnterStagingRequest {
                partition_directive: StagingPartitionDirective::RejectAllWrites,
            }),
        )
        .await
        .unwrap();

    let region = engine.get_region(region_id).unwrap();
    assert_eq!(engine.role(region_id), Some(RegionRole::StagingLeader));
    assert!(region.manifest_ctx.staging_partition_info().is_some());
}

async fn test_restricted_state_transitions_with_format(flat_format: bool) {
    let mut env = TestEnv::new().await;
    let engine = env
        .create_engine(MitoConfig {
            default_flat_format: flat_format,
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
