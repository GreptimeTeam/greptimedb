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

//! Tests that a region acknowledges writes only within its leader lease.

use std::sync::Arc;
use std::time::{Duration, Instant};

use api::region::RegionResponse;
use api::v1::{ColumnSchema, Rows};
use common_error::ext::{BoxedError, ErrorExt};
use common_recordbatch::RecordBatches;
use rstest::rstest;
use rstest_reuse::apply;
use store_api::region_engine::{
    RegionEngine, RegionLeaseGrant, RegionRole, SetRegionRoleStateResponse, SettableRegionRoleState,
};
use store_api::region_request::{RegionFlushRequest, RegionPutRequest, RegionRequest};
use store_api::storage::{RegionId, ScanRequest};
use tokio::task::JoinHandle;

use crate::config::MitoConfig;
use crate::engine::MitoEngine;
use crate::engine::listener::{NotifyRegionChangeResultListener, WriteWalListener};
use crate::engine::set_role_state_test::create_region_and_start_alter;
use crate::error::Error;
use crate::test_util::{
    CreateRequestBuilder, LogStoreFactory, TestEnv, build_rows, kafka_log_store_factory,
    multiple_log_store_factories, prepare_test_for_kafka_log_store, raft_engine_log_store_factory,
    rows_schema,
};

const LEASE: Duration = Duration::from_secs(10);

fn grant(role: RegionRole, deadline: Instant) -> RegionLeaseGrant {
    RegionLeaseGrant {
        role,
        heartbeat_sent_at: Instant::now(),
        deadline,
    }
}

fn spawn_put(
    engine: &MitoEngine,
    region_id: RegionId,
    rows: Rows,
) -> JoinHandle<Result<RegionResponse, BoxedError>> {
    let engine = engine.clone();
    tokio::spawn(async move {
        engine
            .handle_request(
                region_id,
                RegionRequest::Put(RegionPutRequest {
                    rows,
                    hint: None,
                    partition_expr_version: None,
                    skip_wal: false,
                }),
            )
            .await
    })
}

async fn count_rows(engine: &MitoEngine, region_id: RegionId) -> usize {
    let stream = engine
        .scan_to_stream(region_id, ScanRequest::default())
        .await
        .unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    batches.iter().map(|batch| batch.num_rows()).sum()
}

fn assert_retryable(err: BoxedError, expected: impl Fn(&Error) -> bool) {
    assert!(err.retry_hint().is_retryable(), "{err:?}");
    let err = err.into_inner();
    let err = err.as_any().downcast_ref::<Error>().unwrap();
    let err = match err {
        Error::WriteGroup { source } => source.as_ref(),
        err => err,
    };
    assert!(expected(err), "{err:?}");
}

#[apply(multiple_log_store_factories)]
async fn test_lease_expires_while_writing_wal(factory: Option<LogStoreFactory>) {
    common_telemetry::init_default_ut_logging();
    let Some(factory) = factory else {
        return;
    };
    let mut env = TestEnv::with_prefix("lease-expires-while-writing-wal")
        .await
        .with_log_store_factory(factory.clone());
    let listener = Arc::new(WriteWalListener::default());
    let engine = env
        .create_engine_with(MitoConfig::default(), None, Some(listener.clone()), None)
        .await;
    engine.require_write_lease();

    let region_id = RegionId::new(1, 1);
    let topic = prepare_test_for_kafka_log_store(&factory).await;
    let request = CreateRequestBuilder::new().kafka_topic(topic).build();
    let column_schemas = rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();
    engine
        .set_region_role_by_lease(region_id, grant(RegionRole::Leader, Instant::now() + LEASE))
        .unwrap();

    listener.pause_next();
    let rows = Rows {
        schema: column_schemas.clone(),
        rows: build_rows(0, 3),
    };
    let write = spawn_put(&engine, region_id, rows);
    listener.wait_appended().await;
    engine
        .set_region_role_by_lease(region_id, grant(RegionRole::Leader, Instant::now()))
        .unwrap();
    listener.resume();
    let err = write.await.unwrap().unwrap_err();
    assert_retryable(err, |err| matches!(err, Error::UnacknowledgedWrite { .. }));
    // The write is in the WAL, so it is applied rather than undone.
    assert_eq!(3, count_rows(&engine, region_id).await);

    // A new write fails before it reaches the WAL.
    let rows = Rows {
        schema: column_schemas,
        rows: build_rows(3, 6),
    };
    let err = spawn_put(&engine, region_id, rows)
        .await
        .unwrap()
        .unwrap_err();
    assert_retryable(err, |err| matches!(err, Error::RegionLeaseExpired { .. }));
    assert_eq!(3, count_rows(&engine, region_id).await);
}

#[tokio::test]
async fn test_downgrade_while_writing_wal() {
    check_downgrade_while_writing_wal(true).await;
}

#[tokio::test]
async fn test_downgrade_before_first_grant_while_writing_wal() {
    check_downgrade_while_writing_wal(false).await;
}

async fn check_downgrade_while_writing_wal(leader_granted: bool) {
    common_telemetry::init_default_ut_logging();
    let (_env, listener, engine, region_id, column_schemas) =
        create_leased_region("downgrade-while-writing-wal").await;
    if leader_granted {
        engine
            .set_region_role_by_lease(region_id, grant(RegionRole::Leader, Instant::now() + LEASE))
            .unwrap();
    } else {
        // The created region writes under the node lease until its first grant.
        engine.renew_node_lease(Instant::now(), LEASE);
    }

    listener.pause_next();
    let rows = Rows {
        schema: column_schemas,
        rows: build_rows(0, 3),
    };
    let write = spawn_put(&engine, region_id, rows);
    listener.wait_appended().await;
    // A planned downgrade does not fail a write that is already in the WAL.
    engine
        .set_region_role_by_lease(
            region_id,
            grant(RegionRole::DowngradingLeader, Instant::now() + LEASE),
        )
        .unwrap();
    listener.resume();
    assert_eq!(3, write.await.unwrap().unwrap().affected_rows);

    // The flush of the downgrade includes the write.
    engine
        .set_region_role_state_gracefully(region_id, SettableRegionRoleState::DowngradingLeader)
        .await
        .unwrap();
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
    let SetRegionRoleStateResponse::Success(success) = response else {
        panic!("Expected success response, got: {response:?}");
    };
    let version = engine.get_region(region_id).unwrap().version();
    assert_eq!(Some(1), success.last_entry_id());
    assert_eq!(1, version.flushed_entry_id);
    assert!(version.memtables.is_empty());
}

#[tokio::test]
async fn test_downgrading_lease_does_not_extend_leader_deadline() {
    common_telemetry::init_default_ut_logging();
    let (_env, listener, engine, region_id, column_schemas) =
        create_leased_region("downgrading-lease-deadline").await;
    let leader_deadline = Instant::now() + Duration::from_secs(1);
    engine
        .set_region_role_by_lease(region_id, grant(RegionRole::Leader, leader_deadline))
        .unwrap();

    listener.pause_next();
    let rows = Rows {
        schema: column_schemas,
        rows: build_rows(0, 3),
    };
    let write = spawn_put(&engine, region_id, rows);
    listener.wait_appended().await;
    // The metasrv keeps renewing the lease as DowngradingLeader after the route is marked as
    // downgrading, but the write is acknowledged only within the last Leader grant.
    engine
        .set_region_role_by_lease(
            region_id,
            grant(RegionRole::DowngradingLeader, Instant::now() + LEASE),
        )
        .unwrap();
    tokio::time::sleep(leader_deadline.saturating_duration_since(Instant::now())).await;
    listener.resume();
    let err = write.await.unwrap().unwrap_err();
    assert_retryable(err, |err| matches!(err, Error::UnacknowledgedWrite { .. }));
}

#[tokio::test]
async fn test_downgrading_lease_rejects_writes_after_alter() {
    common_telemetry::init_default_ut_logging();
    let mut env = TestEnv::with_prefix("downgrading-lease-alter").await;
    let listener = Arc::new(NotifyRegionChangeResultListener::default());
    let engine = env
        .create_engine_with(MitoConfig::default(), None, Some(listener.clone()), None)
        .await;
    engine.require_write_lease();
    engine.renew_node_lease(Instant::now(), LEASE);
    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new().build();
    let column_schemas = rows_schema(&request);
    let alter = create_region_and_start_alter(&env, &engine, region_id, request).await;
    engine
        .set_region_role_by_lease(region_id, grant(RegionRole::Leader, Instant::now() + LEASE))
        .unwrap();
    // The DowngradingLeader grant cannot downgrade the altering region, which becomes writable
    // again once the ALTER finishes.
    engine
        .set_region_role_by_lease(
            region_id,
            grant(RegionRole::DowngradingLeader, Instant::now() + LEASE),
        )
        .unwrap();
    listener.wake_notify();
    alter.await.unwrap();
    assert_eq!(Some(RegionRole::Leader), engine.role(region_id));

    let rows = Rows {
        schema: column_schemas,
        rows: build_rows(3, 5),
    };
    let err = spawn_put(&engine, region_id, rows)
        .await
        .unwrap()
        .unwrap_err();
    assert_retryable(err, |err| matches!(err, Error::RegionLeaseExpired { .. }));
}

/// Creates a region on an engine whose writes need a lease.
async fn create_leased_region(
    prefix: &str,
) -> (
    TestEnv,
    Arc<WriteWalListener>,
    MitoEngine,
    RegionId,
    Vec<ColumnSchema>,
) {
    let mut env = TestEnv::with_prefix(prefix).await;
    let listener = Arc::new(WriteWalListener::default());
    let engine = env
        .create_engine_with(MitoConfig::default(), None, Some(listener.clone()), None)
        .await;
    engine.require_write_lease();

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new().build();
    let column_schemas = rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();
    (env, listener, engine, region_id, column_schemas)
}
