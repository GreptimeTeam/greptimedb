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
use std::time::Duration;

use api::v1::{Rows, SemanticType};
use common_error::ext::{ErrorExt, RetryHint};
use common_error::status_code::StatusCode;
use common_recordbatch::RecordBatches;
use datatypes::prelude::ConcreteDataType;
use datatypes::schema::ColumnSchema;
use store_api::metadata::ColumnMetadata;
use store_api::region_engine::{
    RegionEngine, RegionRole, SetRegionRoleStateResponse, SetRegionRoleStateSuccess,
    SettableRegionRoleState,
};
use store_api::region_request::{
    AddColumn, AffectedRows, AlterKind, EnterStagingRequest, PathType, RegionAlterRequest,
    RegionCatchupRequest, RegionFlushRequest, RegionOpenRequest, RegionPutRequest, RegionRequest,
    StagingPartitionDirective,
};
use store_api::storage::{RegionId, ScanRequest};
use tokio::sync::oneshot;

use crate::config::MitoConfig;
use crate::engine::MitoEngine;
use crate::engine::basic_test::build_bulk_insert_request;
use crate::engine::listener::NotifyRegionChangeResultListener;
use crate::region::{MitoRegionRef, RegionLeaderState, RegionRoleState};
use crate::region_write_ctx::test_hooks::arm_bulk_install_barrier;
use crate::request::WorkerRequest;
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
                    skip_wal: false,
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
                skip_wal: false,
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

    blocker.release();
    assert_success_response(&downgrade.await.unwrap().unwrap(), 2);

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

#[tokio::test(flavor = "multi_thread")]
async fn test_direct_follower_waits_for_pending_checkpoint() {
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
            schema: column_schemas,
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

    let region = engine.get_region(region_id).unwrap();
    let wait_started = region
        .manifest_ctx
        .manifest_manager
        .read()
        .await
        .checkpointer()
        .pending_checkpoint_wait_started();
    // The no-flush downgrade path requests Follower directly. It must still
    // wait for checkpoint cleanup before replying to the caller.
    let wait_started = wait_started.notified();
    let cloned_engine = engine.clone();
    let set_follower = tokio::spawn(async move {
        cloned_engine
            .set_region_role_state_gracefully(region_id, SettableRegionRoleState::Follower)
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), wait_started)
        .await
        .expect("follower transition did not start waiting for the checkpoint");
    assert_eq!(RegionRoleState::Follower, region.state());
    assert!(
        !set_follower.is_finished(),
        "follower transition returned before checkpoint cleanup finished"
    );

    blocker.release();
    assert_success_response(&set_follower.await.unwrap().unwrap(), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_retried_downgrade_waits_after_first_request_is_cancelled() {
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
            schema: column_schemas,
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

    let region = engine.get_region(region_id).unwrap();
    let first_wait_started = region
        .manifest_ctx
        .manifest_manager
        .read()
        .await
        .checkpointer()
        .pending_checkpoint_wait_started();
    // Register before starting the transition so the notification cannot be lost.
    let first_wait_started = first_wait_started.notified();
    // Call the region transition directly so aborting this task cancels the
    // actual checkpoint waiter. The engine API submits the same transition to
    // a detached worker task, so aborting its caller only drops the reply receiver.
    let first_region = region.clone();
    let first_downgrade = tokio::spawn(async move {
        first_region
            .set_role_state_gracefully(SettableRegionRoleState::DowngradingLeader)
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), first_wait_started)
        .await
        .expect("first downgrade did not start waiting for the checkpoint");
    assert_eq!(
        RegionRoleState::Leader(RegionLeaderState::Downgrading),
        region.state()
    );
    assert!(
        !first_downgrade.is_finished(),
        "first downgrade did not wait for checkpoint cleanup"
    );

    // Cancelling the first caller must not remove the checkpoint handle from
    // the manifest manager. A retried migration request observes Downgrading
    // and must wait for the same checkpoint task before it can proceed.
    first_downgrade.abort();
    assert!(first_downgrade.await.unwrap_err().is_cancelled());
    assert_eq!(
        RegionRoleState::Leader(RegionLeaderState::Downgrading),
        region.state()
    );

    let second_wait_started = region
        .manifest_ctx
        .manifest_manager
        .read()
        .await
        .checkpointer()
        .pending_checkpoint_wait_started();
    // Register before spawning the retry so the notification cannot be lost.
    let second_wait_started = second_wait_started.notified();
    let retry_region = region.clone();
    let retry_downgrade = tokio::spawn(async move {
        retry_region
            .set_role_state_gracefully(SettableRegionRoleState::DowngradingLeader)
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), second_wait_started)
        .await
        .expect("retried downgrade did not start waiting for the checkpoint");
    assert!(
        !retry_downgrade.is_finished(),
        "retried downgrade did not wait for checkpoint cleanup"
    );

    blocker.release();
    retry_downgrade.await.unwrap().unwrap();
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

const WAIT: Duration = Duration::from_secs(10);

type WriteReceiver = oneshot::Receiver<crate::error::Result<AffectedRows>>;

/// Submits a graceful role change straight to the region's worker. Requests
/// submitted after this returns reach the worker after it.
async fn submit_role_change(
    engine: &MitoEngine,
    region_id: RegionId,
    state: SettableRegionRoleState,
) -> oneshot::Receiver<SetRegionRoleStateResponse> {
    let (request, receiver) = WorkerRequest::new_set_readonly_gracefully(region_id, state);
    engine
        .inner
        .workers
        .submit_to_worker(region_id, request)
        .await
        .unwrap();
    receiver
}

/// Submits a region request straight to the region's worker.
async fn submit_request(
    engine: &MitoEngine,
    region_id: RegionId,
    request: RegionRequest,
) -> WriteReceiver {
    let metadata = engine.inner.get_metadata(region_id).ok();
    let (request, receiver) =
        WorkerRequest::try_from_region_request(region_id, request, metadata).unwrap();
    engine
        .inner
        .workers
        .submit_to_worker(region_id, request)
        .await
        .unwrap();
    receiver
}

fn put_request(rows: Rows) -> RegionRequest {
    RegionRequest::Put(RegionPutRequest {
        skip_wal: false,
        rows,
        hint: None,
        partition_expr_version: None,
    })
}

fn reported_last_entry_id(response: &SetRegionRoleStateResponse) -> u64 {
    match response {
        SetRegionRoleStateResponse::Success(success) => success.last_entry_id().unwrap(),
        _ => panic!("Expected success response, got: {:?}", response),
    }
}

/// Lets the role change task waiting behind `manifest_guard` run and returns
/// the `last_entry_id` it reports.
async fn finish_role_change<G>(
    manifest_guard: G,
    role_change: oneshot::Receiver<SetRegionRoleStateResponse>,
) -> u64 {
    drop(manifest_guard);
    let response = tokio::time::timeout(WAIT, role_change)
        .await
        .expect("role change did not finish")
        .unwrap();
    reported_last_entry_id(&response)
}

/// Waits for the write's result if it is still in flight.
async fn write_result(
    early: Option<crate::error::Result<AffectedRows>>,
    write: WriteReceiver,
) -> crate::error::Result<AffectedRows> {
    match early {
        Some(result) => result,
        None => tokio::time::timeout(WAIT, write)
            .await
            .expect("write did not finish")
            .unwrap(),
    }
}

/// Checks the promise of `set_region_role_state_gracefully`: an acknowledged
/// write must be covered by the `last_entry_id` the call returned.
fn assert_write_covered(
    region: &MitoRegionRef,
    result: crate::error::Result<AffectedRows>,
    reported: u64,
) {
    let last_entry_id = region.version_control.current().last_entry_id;
    assert!(
        result.is_err() || last_entry_id <= reported,
        "a write was acknowledged ({result:?}) after the role change returned last_entry_id {reported}, \
         but the region's last_entry_id is now {last_entry_id}"
    );
}

/// The role change task waits for the manifest lock before it changes the
/// role, while the worker keeps handling requests. A bulk write handled after
/// the role change request must be rejected, or be covered by the returned
/// `last_entry_id` if it was accepted.
#[tokio::test(flavor = "multi_thread")]
async fn test_set_follower_gracefully_covers_inflight_bulk_write() {
    common_telemetry::init_default_ut_logging();
    let mut env = TestEnv::with_prefix("follower-inflight-bulk-write").await;
    let engine = env.create_engine(MitoConfig::default()).await;
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
            schema: column_schemas,
            rows: build_rows(0, 3),
        },
    )
    .await;

    let region = engine.get_region(region_id).unwrap();
    let manifest_guard = region.manifest_ctx.manifest_manager.read().await;
    let role_change =
        submit_role_change(&engine, region_id, SettableRegionRoleState::Follower).await;
    // An accepted write pauses after its WAL append, before installing its rows.
    let mut barrier = arm_bulk_install_barrier(region_id, region.version_control.clone());
    let mut write = submit_request(
        &engine,
        region_id,
        RegionRequest::BulkInserts(build_bulk_insert_request(region_id, 2)),
    )
    .await;
    let early = tokio::select! {
        _ = barrier.wait_until_reached() => None,
        result = &mut write => Some(result.unwrap()),
        _ = tokio::time::sleep(WAIT) => panic!("bulk write never reached the install barrier"),
    };
    let reported = finish_role_change(manifest_guard, role_change).await;
    assert_eq!(RegionRoleState::Follower, region.state());

    barrier.release();
    let result = write_result(early, write).await;
    assert_write_covered(&region, result, reported);
}

/// A lease renewal sets Downgrading outside the worker, so a write that passed
/// the state check before it can still be in flight. The datanode's downgrade
/// steps (DowngradingLeader, Flush, Follower) must still flush that write and
/// report its entry.
#[tokio::test(flavor = "multi_thread")]
async fn test_downgrade_flush_follower_covers_inflight_write() {
    common_telemetry::init_default_ut_logging();
    let mut env = TestEnv::with_prefix("downgrade-flush-follower").await;
    let engine = env.create_engine(MitoConfig::default()).await;
    let region_id = RegionId::new(1, 1);
    engine
        .handle_request(
            region_id,
            RegionRequest::Create(CreateRequestBuilder::new().build()),
        )
        .await
        .unwrap();

    let region = engine.get_region(region_id).unwrap();
    let mut barrier = arm_bulk_install_barrier(region_id, region.version_control.clone());
    let write = submit_request(
        &engine,
        region_id,
        RegionRequest::BulkInserts(build_bulk_insert_request(region_id, 2)),
    )
    .await;
    tokio::time::timeout(WAIT, barrier.wait_until_reached())
        .await
        .expect("bulk write never reached the install barrier");
    engine
        .set_region_role(region_id, RegionRole::DowngradingLeader)
        .unwrap();

    let downgrade = submit_role_change(
        &engine,
        region_id,
        SettableRegionRoleState::DowngradingLeader,
    )
    .await;
    barrier.release();
    assert_eq!(2, write.await.unwrap().unwrap());
    reported_last_entry_id(&downgrade.await.unwrap());
    engine
        .handle_request(
            region_id,
            RegionRequest::Flush(RegionFlushRequest::default()),
        )
        .await
        .unwrap();

    let response = engine
        .set_region_role_state_gracefully(region_id, SettableRegionRoleState::Follower)
        .await
        .unwrap();
    let reported = reported_last_entry_id(&response);
    let current = region.version_control.current();
    assert_eq!(current.last_entry_id, reported);
    assert_eq!(current.version.flushed_entry_id, reported);
    assert!(current.version.memtables.is_empty());
}

fn add_nullable_field1() -> RegionAlterRequest {
    RegionAlterRequest {
        kind: AlterKind::AddColumns {
            columns: vec![AddColumn {
                column_metadata: ColumnMetadata {
                    column_schema: ColumnSchema::new(
                        "field_1",
                        ConcreteDataType::float64_datatype(),
                        true,
                    ),
                    semantic_type: SemanticType::Field,
                    column_id: 3,
                },
                location: None,
            }],
        },
    }
}

async fn wait_for_state(region: &MitoRegionRef, state: RegionRoleState) {
    tokio::time::timeout(WAIT, async {
        while region.state() != state {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("region never reached {state:?}, now {:?}", region.state()));
}

async fn count_rows(engine: &MitoEngine, region_id: RegionId) -> usize {
    let stream = engine
        .scan_to_stream(region_id, ScanRequest::default())
        .await
        .unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    batches.iter().map(|batch| batch.num_rows()).sum()
}

/// Creates a region with rows `[0, 3)` in an SST and starts an ALTER whose
/// result the worker does not handle until `listener` is woken, so the region
/// stays in Altering. Returns the ALTER task.
async fn create_region_and_start_alter(
    env: &TestEnv,
    engine: &MitoEngine,
    region_id: RegionId,
    request: store_api::region_request::RegionCreateRequest,
) -> tokio::task::JoinHandle<()> {
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
    put_rows(
        engine,
        region_id,
        Rows {
            schema: column_schemas,
            rows: build_rows(0, 3),
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

    let cloned_engine = engine.clone();
    let alter = tokio::spawn(async move {
        cloned_engine
            .handle_request(region_id, RegionRequest::Alter(add_nullable_field1()))
            .await
            .unwrap();
    });
    let region = engine.get_region(region_id).unwrap();
    wait_for_state(
        &region,
        RegionRoleState::Leader(RegionLeaderState::Altering),
    )
    .await;
    alter
}

/// DowngradingLeader while an ALTER keeps the region in Altering must fail
/// with a retryable error, because the region becomes writable again once the
/// ALTER finishes. A retry after the ALTER fences writes.
#[tokio::test(flavor = "multi_thread")]
async fn test_downgrading_leader_during_alter_stops_writes() {
    common_telemetry::init_default_ut_logging();
    let mut env = TestEnv::with_prefix("downgrading-during-alter").await;
    let listener = Arc::new(NotifyRegionChangeResultListener::default());
    let engine = env
        .create_engine_with(MitoConfig::default(), None, Some(listener.clone()), None)
        .await;
    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new().build();
    let column_schemas = rows_schema(&request);
    let alter = create_region_and_start_alter(&env, &engine, region_id, request).await;
    let region = engine.get_region(region_id).unwrap();

    let response = engine
        .set_region_role_state_gracefully(region_id, SettableRegionRoleState::DowngradingLeader)
        .await
        .unwrap();
    let state_after_downgrade = region.state();
    listener.wake_notify();
    tokio::time::timeout(WAIT, alter).await.unwrap().unwrap();
    match &response {
        SetRegionRoleStateResponse::InvalidTransition(err) => {
            assert_eq!(RetryHint::Retryable, err.retry_hint(), "{err:?}")
        }
        _ => panic!(
            "DowngradingLeader returned {response:?} with the region in {state_after_downgrade:?}"
        ),
    }

    let response = engine
        .set_region_role_state_gracefully(region_id, SettableRegionRoleState::DowngradingLeader)
        .await
        .unwrap();
    reported_last_entry_id(&response);
    let err = engine
        .handle_request(
            region_id,
            put_request(Rows {
                schema: column_schemas,
                rows: build_rows(3, 5),
            }),
        )
        .await
        .unwrap_err();
    assert_eq!(StatusCode::RegionNotReady, err.status_code());
}

/// The datanode's downgrade steps (DowngradingLeader, Flush, Follower) with
/// the DowngradingLeader step overlapping an ALTER. The region accepts writes
/// until a downgrade takes effect. A migration candidate opens the region from
/// the object store with its own local WAL, so the final flush must carry
/// every write the old leader acknowledged.
#[tokio::test(flavor = "multi_thread")]
async fn test_downgrade_during_alter_keeps_acknowledged_writes_with_local_wal() {
    common_telemetry::init_default_ut_logging();
    let mut env = TestEnv::with_prefix("downgrade-during-alter-local-wal").await;
    let listener = Arc::new(NotifyRegionChangeResultListener::default());
    let engine = env
        .create_engine_with(MitoConfig::default(), None, Some(listener.clone()), None)
        .await;
    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new().build();
    let column_schemas = rows_schema(&request);
    let table_dir = request.table_dir.clone();
    let options = request.options.clone();
    let alter = create_region_and_start_alter(&env, &engine, region_id, request).await;

    // Mirrors `DowngradeRegionsHandler::handle_downgrade_region` in the
    // datanode. Metasrv retries the downgrade on a retryable error.
    let mut response = engine
        .set_region_role_state_gracefully(region_id, SettableRegionRoleState::DowngradingLeader)
        .await
        .unwrap();
    listener.wake_notify();
    tokio::time::timeout(WAIT, alter).await.unwrap().unwrap();
    put_rows(
        &engine,
        region_id,
        Rows {
            schema: column_schemas.clone(),
            rows: build_rows(3, 5),
        },
    )
    .await;
    let mut acknowledged_rows = 5;
    if let SetRegionRoleStateResponse::InvalidTransition(err) = &response {
        assert_eq!(RetryHint::Retryable, err.retry_hint(), "{err:?}");
        response = engine
            .set_region_role_state_gracefully(region_id, SettableRegionRoleState::DowngradingLeader)
            .await
            .unwrap();
    }
    reported_last_entry_id(&response);
    engine
        .handle_request(
            region_id,
            RegionRequest::Flush(RegionFlushRequest::default()),
        )
        .await
        .unwrap();
    if let Ok(output) = engine
        .handle_request(
            region_id,
            put_request(Rows {
                schema: column_schemas,
                rows: build_rows(5, 8),
            }),
        )
        .await
    {
        acknowledged_rows += output.affected_rows;
    }
    let response = engine
        .set_region_role_state_gracefully(region_id, SettableRegionRoleState::Follower)
        .await
        .unwrap();
    let reported = reported_last_entry_id(&response);

    // The candidate shares the object store but not the local WAL.
    let mut candidate_env = TestEnv::with_prefix("downgrade-during-alter-candidate").await;
    candidate_env.object_store_manager = env.get_object_store_manager();
    candidate_env.log_store = Some(candidate_env.create_log_store().await);
    let candidate = candidate_env.new_mito_engine(MitoConfig::default()).await;
    candidate
        .handle_request(
            region_id,
            RegionRequest::Open(RegionOpenRequest {
                engine: String::new(),
                table_dir,
                options,
                skip_wal_replay: true,
                path_type: PathType::Bare,
                checkpoint: None,
                requirements: Default::default(),
            }),
        )
        .await
        .unwrap();
    candidate
        .handle_request(
            region_id,
            RegionRequest::Catchup(RegionCatchupRequest {
                set_writable: true,
                entry_id: Some(reported),
                metadata_entry_id: None,
                location_id: None,
                checkpoint: None,
            }),
        )
        .await
        .unwrap();
    assert_eq!(
        acknowledged_rows,
        count_rows(&candidate, region_id).await,
        "the candidate misses rows the old leader acknowledged; Follower reported last_entry_id {reported}"
    );
}
