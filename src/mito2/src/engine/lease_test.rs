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

//! Tests that a region acknowledges only writes whose WAL append completed within its leader
//! lease, that Leader grants for heartbeats sent before a downgrade do not make it writable
//! again, and what a region a catchup made writable may commit before a later lease confirms it.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use api::region::RegionResponse;
use api::v1::region::{StrictWindow, compact_request};
use api::v1::{ColumnSchema, Rows};
use common_base::readable_size::ReadableSize;
use common_error::ext::{BoxedError, ErrorExt};
use common_error::status_code::StatusCode;
use common_recordbatch::RecordBatches;
use rstest::rstest;
use rstest_reuse::apply;
use store_api::region_engine::{
    RegionEngine, RegionRole, SetRegionRoleStateResponse, SettableRegionRoleState,
};
use store_api::region_request::{
    EnterStagingRequest, RegionAlterRequest, RegionBuildIndexRequest, RegionCatchupRequest,
    RegionCloseRequest, RegionCompactRequest, RegionDropRequest, RegionFlushRequest,
    RegionPutRequest, RegionRequest, RegionTruncateRequest, StagingPartitionDirective,
};
use store_api::storage::{RegionId, ScanRequest};
use tokio::task::JoinHandle;

use crate::config::MitoConfig;
use crate::engine::MitoEngine;
use crate::engine::basic_test::build_bulk_insert_request;
use crate::engine::index_build_test::async_build_mode_config;
use crate::engine::listener::{
    CountingStallListener, EventListener, EventListenerRef, WriteWalListener,
};
use crate::engine::object_store_wal_test::wait_until;
use crate::engine::open_test::open_region_with_backfilled_partition_expr;
use crate::engine::pending_tail_test::add_field;
use crate::engine::staging_test::{
    enter_staging, first_file, float_range_expr, submit_staging_edit,
};
use crate::error::Error;
use crate::flush::FlushReason;
use crate::lease::GrantedLease;
use crate::manifest::action::RegionEdit;
use crate::metrics::{FLUSH_FAILURE_TOTAL, FLUSH_REQUESTS_TOTAL};
use crate::region::{RegionLeaderState, RegionRoleState};
use crate::request::{BackgroundNotify, BuildIndexRequest, WorkerRequest};
use crate::sst::file::IndexType;
use crate::sst::index::IndexBuildType;
use crate::test_util::{
    CreateRequestBuilder, LogStoreFactory, TestEnv, build_rows, flush_region,
    kafka_log_store_factory, multiple_log_store_factories, prepare_test_for_kafka_log_store,
    put_rows, raft_engine_log_store_factory, reopen_region, rows_schema,
};

const LEASE: Duration = Duration::from_secs(10);
const REGION_ID: RegionId = RegionId::new(1, 1);

/// Creates [REGION_ID] on an engine whose writes need a lease. The node has no lease yet, and
/// the clock of the lease checks stands still until a test moves it.
async fn create_region(
    env: &mut TestEnv,
    topic: Option<String>,
) -> (Arc<WriteWalListener>, MitoEngine, Vec<ColumnSchema>) {
    let listener = Arc::new(WriteWalListener::default());
    let (engine, schema) =
        create_region_with_config(env, topic, MitoConfig::default(), listener.clone()).await;
    (listener, engine, schema)
}

/// As [create_region], on an engine of `config` with `listener`.
async fn create_region_with_config(
    env: &mut TestEnv,
    topic: Option<String>,
    config: MitoConfig,
    listener: EventListenerRef,
) -> (MitoEngine, Vec<ColumnSchema>) {
    let engine = env
        .create_engine_with(config, None, Some(listener), None)
        .await;
    engine.require_write_lease();
    engine.freeze_lease_clock(Instant::now());
    let request = CreateRequestBuilder::new().kafka_topic(topic).build();
    let schema = rows_schema(&request);
    engine
        .handle_request(REGION_ID, RegionRequest::Create(request))
        .await
        .unwrap();
    (engine, schema)
}

fn grant(engine: &MitoEngine, role: RegionRole, deadline: Instant) {
    engine
        .set_region_role_with_lease(REGION_ID, role, Instant::now(), deadline, None)
        .unwrap();
}

/// Grants `role` with `candidate_lease_id` for a heartbeat sent at `sent_at`, and returns whether
/// the region applied the role.
fn grant_at(
    engine: &MitoEngine,
    role: RegionRole,
    sent_at: Instant,
    candidate_lease_id: Option<u64>,
) -> bool {
    engine
        .set_region_role_with_lease(
            REGION_ID,
            role,
            sent_at,
            Instant::now() + LEASE,
            candidate_lease_id,
        )
        .unwrap()
}

/// Returns the latest lease the region recorded.
fn granted_lease(engine: &MitoEngine) -> Option<GrantedLease> {
    engine.inner.workers.granted_leases().get(REGION_ID)
}

/// Returns a request that writes rows `start..start + 3`.
fn put_request(schema: &[ColumnSchema], start: usize) -> RegionRequest {
    let rows = Rows {
        schema: schema.to_vec(),
        rows: build_rows(start, start + 3),
    };
    RegionRequest::Put(RegionPutRequest {
        rows,
        hint: None,
        partition_expr_version: None,
        skip_wal: false,
    })
}

fn put(
    engine: &MitoEngine,
    region_id: RegionId,
    schema: &[ColumnSchema],
    start: usize,
) -> JoinHandle<Result<RegionResponse, BoxedError>> {
    let engine = engine.clone();
    let request = put_request(schema, start);
    tokio::spawn(async move { engine.handle_request(region_id, request).await })
}

/// Sends `request` to [REGION_ID] and waits until it is held after its WAL append.
async fn hold(
    listener: &WriteWalListener,
    engine: &MitoEngine,
    request: RegionRequest,
) -> JoinHandle<Result<RegionResponse, BoxedError>> {
    listener.pause_next();
    let engine = engine.clone();
    let mut write = tokio::spawn(async move { engine.handle_request(REGION_ID, request).await });
    tokio::select! {
        () = listener.wait_appended() => {}
        result = &mut write => panic!("the write finished before its WAL append: {result:?}"),
    }
    write
}

async fn count_rows(engine: &MitoEngine) -> usize {
    let stream = engine
        .scan_to_stream(REGION_ID, ScanRequest::default())
        .await
        .unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    batches.iter().map(|batch| batch.num_rows()).sum()
}

fn assert_retryable(result: Result<RegionResponse, BoxedError>, expected: fn(&Error) -> bool) {
    let err = result.unwrap_err();
    assert!(err.retry_hint().is_retryable(), "{err:?}");
    let err = err.into_inner();
    let err = match err.as_any().downcast_ref::<Error>().unwrap() {
        Error::WriteGroup { source } => source.as_ref(),
        err => err,
    };
    assert!(expected(err), "{err:?}");
}

fn lease_expired(err: &Error) -> bool {
    matches!(err, Error::RegionLeaseExpired { .. })
}

fn unacknowledged(err: &Error) -> bool {
    matches!(err, Error::UnacknowledgedWrite { .. })
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
    let topic = prepare_test_for_kafka_log_store(&factory).await;
    let (listener, engine, schema) = create_region(&mut env, topic).await;
    let deadline = Instant::now() + LEASE;
    grant(&engine, RegionRole::Leader, deadline);

    let write = hold(&listener, &engine, put_request(&schema, 0)).await;
    engine.freeze_lease_clock(deadline);
    listener.resume();
    assert_retryable(write.await.unwrap(), unacknowledged);
    // The write is in the WAL, so it is applied rather than undone.
    assert_eq!(3, count_rows(&engine).await);

    // A new write fails before it reaches the WAL.
    assert_retryable(
        put(&engine, REGION_ID, &schema, 3).await.unwrap(),
        lease_expired,
    );
    assert_eq!(3, count_rows(&engine).await);

    // The same holds for a bulk insert.
    let deadline = deadline + LEASE;
    grant(&engine, RegionRole::Leader, deadline);
    let bulk_insert = RegionRequest::BulkInserts(build_bulk_insert_request(REGION_ID, 3));
    let write = hold(&listener, &engine, bulk_insert).await;
    let committed_sequence = || {
        let region = engine.get_region(REGION_ID).unwrap();
        region.version_control.current().committed_sequence
    };
    let sequence = committed_sequence();
    engine.freeze_lease_clock(deadline);
    listener.resume();
    assert_retryable(write.await.unwrap(), unacknowledged);
    assert_eq!(6, count_rows(&engine).await);
    assert!(committed_sequence() > sequence);
}

#[tokio::test]
async fn test_lease_renewed_while_writing_wal() {
    common_telemetry::init_default_ut_logging();
    let mut env = TestEnv::with_prefix("lease-renewed-while-writing-wal").await;
    let (listener, engine, schema) = create_region(&mut env, None).await;
    let deadline = Instant::now() + LEASE;
    grant(&engine, RegionRole::Leader, deadline);

    let write = hold(&listener, &engine, put_request(&schema, 0)).await;
    // The grant keeps the region writable rather than staging, and still extends the deadline.
    grant(&engine, RegionRole::StagingLeader, deadline + LEASE);
    engine.freeze_lease_clock(deadline);
    listener.resume();
    assert_eq!(3, write.await.unwrap().unwrap().affected_rows);
}

#[rstest]
#[case::within_leader_lease(false)]
#[case::after_leader_lease(true)]
#[tokio::test]
async fn test_downgrade_while_writing_wal(#[case] lease_expires: bool) {
    common_telemetry::init_default_ut_logging();
    let mut env = TestEnv::with_prefix("downgrade-while-writing-wal").await;
    let (listener, engine, schema) = create_region(&mut env, None).await;
    let deadline = Instant::now() + LEASE;
    grant(&engine, RegionRole::Leader, deadline);

    let write = hold(&listener, &engine, put_request(&schema, 0)).await;
    // The metasrv renews the lease as DowngradingLeader once the route marks the region as
    // downgrading. A write already in the WAL is acknowledged, but only within the Leader
    // deadline, which such grants do not extend.
    grant(&engine, RegionRole::DowngradingLeader, deadline + LEASE);
    if lease_expires {
        engine.freeze_lease_clock(deadline);
    }
    listener.resume();
    let result = write.await.unwrap();
    if lease_expires {
        assert_retryable(result, unacknowledged);
    } else {
        assert_eq!(3, result.unwrap().affected_rows);
    }
}

#[tokio::test]
async fn test_created_region_writes_under_node_lease() {
    common_telemetry::init_default_ut_logging();
    let mut env = TestEnv::with_prefix("created-region-node-lease").await;
    let (_listener, engine, schema) = create_region(&mut env, None).await;
    let bulk_insert = |region_id| {
        engine.handle_request(
            region_id,
            RegionRequest::BulkInserts(build_bulk_insert_request(region_id, 3)),
        )
    };
    // Created before the node has a lease, the region has no deadline.
    assert_retryable(
        put(&engine, REGION_ID, &schema, 0).await.unwrap(),
        lease_expired,
    );
    assert_retryable(bulk_insert(REGION_ID).await, lease_expired);

    // Renewing the node lease does not extend the deadline of a region created before.
    engine.renew_node_lease(Instant::now() + LEASE);
    assert!(put(&engine, REGION_ID, &schema, 0).await.unwrap().is_err());

    // A region created under the node lease takes writes before its first Leader grant.
    let region_id = RegionId::new(1, 2);
    engine
        .handle_request(
            region_id,
            RegionRequest::Create(CreateRequestBuilder::new().build()),
        )
        .await
        .unwrap();
    put(&engine, region_id, &schema, 0).await.unwrap().unwrap();
    bulk_insert(region_id).await.unwrap();
}

#[tokio::test]
async fn test_upgraded_region_writes_before_leader_grant() {
    common_telemetry::init_default_ut_logging();
    let mut env = TestEnv::with_prefix("upgraded-region-node-lease").await;
    let (_listener, engine, schema) = create_region(&mut env, None).await;
    // A migration candidate is a follower until the upgrade.
    grant(&engine, RegionRole::Follower, Instant::now() + LEASE);
    engine.renew_node_lease(Instant::now() + LEASE);

    let request = RegionCatchupRequest {
        set_writable: true,
        ..Default::default()
    };
    engine
        .handle_request(REGION_ID, RegionRequest::Catchup(request))
        .await
        .unwrap();
    put(&engine, REGION_ID, &schema, 0).await.unwrap().unwrap();
}

#[rstest]
#[case::downgrading(SettableRegionRoleState::DowngradingLeader)]
#[case::follower(SettableRegionRoleState::Follower)]
#[tokio::test]
async fn test_stale_leader_grant_after_downgrade(#[case] downgraded_to: SettableRegionRoleState) {
    common_telemetry::init_default_ut_logging();
    let mut env = TestEnv::with_prefix("stale-leader-grant").await;
    let (_listener, engine, _schema) = create_region(&mut env, None).await;
    let sent_before = Instant::now() - Duration::from_millis(1);
    engine
        .set_region_role_state_gracefully(REGION_ID, downgraded_to)
        .await
        .unwrap();
    let downgraded = engine.get_region(REGION_ID).unwrap().state();
    // Another downgrade raises the gate again, though the role does not change.
    let sent_between = Instant::now();
    engine
        .set_region_role_state_gracefully(REGION_ID, downgraded_to)
        .await
        .unwrap();

    // Grants for heartbeats sent before either downgrade arrive late.
    assert!(!grant_at(&engine, RegionRole::Leader, sent_before, None));
    assert!(!grant_at(&engine, RegionRole::Leader, sent_between, None));
    assert_eq!(downgraded, engine.get_region(REGION_ID).unwrap().state());

    assert!(grant_at(&engine, RegionRole::Leader, Instant::now(), None));
    assert!(engine.get_region(REGION_ID).unwrap().is_writable());
}

#[rstest]
#[case::catchup(false)]
#[case::close_and_open(true)]
#[tokio::test]
async fn test_reopened_region_ignores_earlier_grants(#[case] close: bool) {
    common_telemetry::init_default_ut_logging();
    let mut env = TestEnv::with_prefix("reopened-region-grant").await;
    let (_listener, engine, schema) = create_region(&mut env, None).await;
    grant(&engine, RegionRole::Leader, Instant::now() + LEASE);
    // The memtable is not empty, so the catchup below reopens the region.
    put(&engine, REGION_ID, &schema, 0).await.unwrap().unwrap();
    let sent_before = Instant::now() - Duration::from_millis(1);
    let region = engine.get_region(REGION_ID).unwrap();
    if close {
        let table_dir = CreateRequestBuilder::new().build().table_dir;
        reopen_region(&engine, REGION_ID, table_dir, false, HashMap::new()).await;
    } else {
        engine
            .set_region_role_state_gracefully(REGION_ID, SettableRegionRoleState::Follower)
            .await
            .unwrap();
        let request = RegionRequest::Catchup(RegionCatchupRequest::default());
        engine.handle_request(REGION_ID, request).await.unwrap();
    }
    assert!(!Arc::ptr_eq(
        &region,
        &engine.get_region(REGION_ID).unwrap()
    ));

    assert!(!grant_at(&engine, RegionRole::Leader, sent_before, None));
    assert!(!engine.get_region(REGION_ID).unwrap().is_writable());

    grant(&engine, RegionRole::Leader, Instant::now() + LEASE);
    assert!(engine.get_region(REGION_ID).unwrap().is_writable());
}

#[tokio::test]
async fn test_leader_grant_exits_staging() {
    common_telemetry::init_default_ut_logging();
    let mut env = TestEnv::with_prefix("staging-leader-grant").await;
    let (_listener, engine, _schema) = create_region(&mut env, None).await;
    let request = EnterStagingRequest {
        partition_directive: StagingPartitionDirective::RejectAllWrites,
    };
    engine
        .handle_request(REGION_ID, RegionRequest::EnterStaging(request))
        .await
        .unwrap();
    let state = || engine.get_region(REGION_ID).unwrap().state();
    assert_eq!(RegionRoleState::Leader(RegionLeaderState::Staging), state());

    grant(&engine, RegionRole::Leader, Instant::now() + LEASE);
    assert_eq!(
        RegionRoleState::Leader(RegionLeaderState::Writable),
        state()
    );
}

#[tokio::test]
async fn test_granted_lease_keeps_latest_heartbeat_across_catchup_reopen() {
    common_telemetry::init_default_ut_logging();
    let mut env = TestEnv::with_prefix("granted-lease-reopen").await;
    let (_listener, engine, schema) = create_region(&mut env, None).await;
    grant(&engine, RegionRole::Leader, Instant::now() + LEASE);
    // The memtable is not empty, so the catchup below reopens the region.
    put(&engine, REGION_ID, &schema, 0).await.unwrap().unwrap();
    // After the first grant and before the downgrade.
    let sent_before = Instant::now();
    engine
        .set_region_role_state_gracefully(REGION_ID, SettableRegionRoleState::Follower)
        .await
        .unwrap();

    // A Leader lease the region ignores still counts as the latest one received.
    assert!(!grant_at(&engine, RegionRole::Leader, sent_before, Some(9)));
    let latest = GrantedLease {
        candidate_lease_id: Some(9),
        sent_at: sent_before,
    };
    assert_eq!(granted_lease(&engine), Some(latest));
    // A lease of an earlier or the same heartbeat that arrives late does not replace it.
    let earlier = sent_before - Duration::from_millis(1);
    grant_at(&engine, RegionRole::Follower, earlier, Some(1));
    assert_eq!(granted_lease(&engine), Some(latest));
    grant_at(&engine, RegionRole::Follower, sent_before, None);
    assert_eq!(granted_lease(&engine), Some(latest));

    let region = engine.get_region(REGION_ID).unwrap();
    let request = RegionRequest::Catchup(RegionCatchupRequest::default());
    engine.handle_request(REGION_ID, request).await.unwrap();
    assert!(!Arc::ptr_eq(
        &region,
        &engine.get_region(REGION_ID).unwrap()
    ));
    assert_eq!(granted_lease(&engine), Some(latest));

    let later = Instant::now();
    grant_at(&engine, RegionRole::Follower, later, None);
    assert_eq!(
        granted_lease(&engine),
        Some(GrantedLease {
            candidate_lease_id: None,
            sent_at: later,
        })
    );
}

#[rstest]
#[case::close(RegionRequest::Close(RegionCloseRequest::default()))]
#[case::close_after_flush(RegionRequest::Close(RegionCloseRequest { flush_on_close: true }))]
#[case::drop(RegionRequest::Drop(RegionDropRequest {
    fast_path: false,
    force: false,
    partial_drop: false,
}))]
#[tokio::test]
async fn test_granted_lease_removed_with_region(#[case] request: RegionRequest) {
    common_telemetry::init_default_ut_logging();
    let mut env = TestEnv::with_prefix("granted-lease-removed").await;
    let (_listener, engine, schema) = create_region(&mut env, None).await;
    grant(&engine, RegionRole::Leader, Instant::now() + LEASE);
    // The memtable is not empty, so a close with `flush_on_close` closes after the flush.
    put(&engine, REGION_ID, &schema, 0).await.unwrap().unwrap();
    assert!(granted_lease(&engine).is_some());

    engine.handle_request(REGION_ID, request).await.unwrap();

    assert!(!engine.is_region_exists(REGION_ID));
    assert_eq!(granted_lease(&engine), None);
    // A lease that arrives after the removal is not recorded.
    let _ = engine.set_region_role_with_lease(
        REGION_ID,
        RegionRole::Follower,
        Instant::now(),
        Instant::now() + LEASE,
        Some(3),
    );
    assert_eq!(granted_lease(&engine), None);
}

#[tokio::test]
async fn test_granted_lease_removed_on_engine_stop() {
    common_telemetry::init_default_ut_logging();
    let mut env = TestEnv::with_prefix("granted-lease-stop").await;
    let (_listener, engine, _schema) = create_region(&mut env, None).await;
    grant_at(&engine, RegionRole::Follower, Instant::now(), Some(7));
    assert!(granted_lease(&engine).is_some());

    engine.stop().await.unwrap();

    assert_eq!(granted_lease(&engine), None);
}

/// Demotes `region_id` on an engine whose writes need a lease and makes it writable again by a
/// catchup, after which it awaits its Leader lease.
pub(crate) async fn promote_to_await_leader_lease(engine: &MitoEngine, region_id: RegionId) {
    engine
        .set_region_role_state_gracefully(region_id, SettableRegionRoleState::Follower)
        .await
        .unwrap();
    // The lease the catchup seeds never runs out.
    engine.freeze_lease_clock(Instant::now());
    engine.renew_node_lease(Instant::now() + LEASE);
    catchup_region(engine, region_id).await.unwrap();
    let region = engine.get_region(region_id).unwrap();
    assert!(region.is_writable());
    assert!(region.manifest_ctx.awaits_leader_lease());
}

async fn catchup(engine: &MitoEngine) -> Result<RegionResponse, BoxedError> {
    catchup_region(engine, REGION_ID).await
}

async fn catchup_region(
    engine: &MitoEngine,
    region_id: RegionId,
) -> Result<RegionResponse, BoxedError> {
    let request = RegionCatchupRequest {
        set_writable: true,
        ..Default::default()
    };
    engine
        .handle_request(region_id, RegionRequest::Catchup(request))
        .await
}

fn add_field_request() -> RegionRequest {
    RegionRequest::Alter(RegionAlterRequest {
        kind: add_field(None),
    })
}

async fn flush(engine: &MitoEngine) -> Result<RegionResponse, BoxedError> {
    engine
        .handle_request(
            REGION_ID,
            RegionRequest::Flush(RegionFlushRequest::default()),
        )
        .await
}

async fn manifest_files(engine: &MitoEngine) -> Vec<u64> {
    let manifest = engine
        .get_region(REGION_ID)
        .unwrap()
        .manifest_ctx
        .manifest()
        .await;
    manifest.files.values().map(|file| file.num_rows).collect()
}

fn lease_not_received(err: &Error) -> bool {
    matches!(err, Error::RegionLeaseNotReceived { .. })
}

#[tokio::test]
async fn test_catchup_without_write_lease_does_not_await_leader_lease() {
    common_telemetry::init_default_ut_logging();
    let mut env = TestEnv::with_prefix("catchup-without-write-lease").await;
    let engine = env.create_engine(MitoConfig::default()).await;
    let request = CreateRequestBuilder::new().build();
    let schema = rows_schema(&request);
    engine
        .handle_request(REGION_ID, RegionRequest::Create(request))
        .await
        .unwrap();
    engine
        .set_region_role_state_gracefully(REGION_ID, SettableRegionRoleState::Follower)
        .await
        .unwrap();
    catchup(&engine).await.unwrap();

    let region = engine.get_region(REGION_ID).unwrap();
    assert!(region.is_writable());
    assert!(!region.manifest_ctx.awaits_leader_lease());
    put(&engine, REGION_ID, &schema, 0).await.unwrap().unwrap();
    flush(&engine).await.unwrap();
    assert_eq!(manifest_files(&engine).await, vec![3]);
}

#[tokio::test]
async fn test_region_awaiting_leader_lease_flushes_after_it() {
    common_telemetry::init_default_ut_logging();
    let mut env = TestEnv::with_prefix("catchup-flush-after-lease").await;
    let (_listener, engine, schema) = create_region(&mut env, None).await;
    let sent_before_catchup = Instant::now();
    promote_to_await_leader_lease(&engine, REGION_ID).await;
    put(&engine, REGION_ID, &schema, 0).await.unwrap().unwrap();

    let err = flush(&engine).await.unwrap_err();
    assert_eq!(err.status_code(), StatusCode::RegionBusy, "{err:?}");
    assert!(manifest_files(&engine).await.is_empty());

    // A lease of a heartbeat sent before the catchup does not end the wait.
    grant_at(&engine, RegionRole::Leader, sent_before_catchup, None);
    assert!(flush(&engine).await.is_err());
    assert!(manifest_files(&engine).await.is_empty());

    grant_at(&engine, RegionRole::Leader, Instant::now(), None);
    flush(&engine).await.unwrap();
    assert_eq!(manifest_files(&engine).await, vec![3]);
}

/// A change the region commits through its manifest is refused while it awaits its Leader
/// lease, and the region stays writable.
#[rstest]
#[case::alter(true)]
#[case::truncate(false)]
#[tokio::test]
async fn test_region_awaiting_leader_lease_commits_no_change(#[case] alter: bool) {
    common_telemetry::init_default_ut_logging();
    let mut env = TestEnv::with_prefix("catchup-change-before-lease").await;
    let (_listener, engine, _schema) = create_region(&mut env, None).await;
    promote_to_await_leader_lease(&engine, REGION_ID).await;
    let region = engine.get_region(REGION_ID).unwrap();
    let version = region.manifest_ctx.manifest_version().await;
    let change = || match alter {
        true => add_field_request(),
        false => RegionRequest::Truncate(RegionTruncateRequest::All),
    };

    assert_retryable(
        engine.handle_request(REGION_ID, change()).await,
        lease_not_received,
    );
    assert_eq!(region.manifest_ctx.manifest_version().await, version);
    assert_eq!(
        region.state(),
        RegionRoleState::Leader(RegionLeaderState::Writable)
    );

    grant_at(&engine, RegionRole::Leader, Instant::now(), None);
    engine.handle_request(REGION_ID, change()).await.unwrap();
    assert_eq!(region.manifest_ctx.manifest_version().await, version + 1);
}

#[tokio::test]
async fn test_staging_leader_lease_ends_wait_for_leader_lease() {
    common_telemetry::init_default_ut_logging();
    let mut env = TestEnv::with_prefix("staging-lease-ends-wait").await;
    let (_listener, engine, schema) = create_region(&mut env, None).await;
    promote_to_await_leader_lease(&engine, REGION_ID).await;
    put(&engine, REGION_ID, &schema, 0).await.unwrap().unwrap();

    grant_at(&engine, RegionRole::StagingLeader, Instant::now(), None);

    let region = engine.get_region(REGION_ID).unwrap();
    assert!(!region.manifest_ctx.awaits_leader_lease());
    // The lease does not change the role of a writable leader.
    assert_eq!(
        region.state(),
        RegionRoleState::Leader(RegionLeaderState::Writable)
    );
    flush(&engine).await.unwrap();
    assert_eq!(manifest_files(&engine).await, vec![3]);
}

/// ALTER and EnterStaging flush the rows first, which a region that awaits its Leader lease
/// cannot commit, so they start no flush.
#[rstest]
#[case::alter(FlushReason::Alter)]
#[case::enter_staging(FlushReason::EnterStaging)]
#[tokio::test]
async fn test_ddl_flush_refused_awaiting_leader_lease(#[case] reason: FlushReason) {
    common_telemetry::init_default_ut_logging();
    let mut env = TestEnv::with_prefix("ddl-flush-awaiting-lease").await;
    let (_listener, engine, schema) = create_region(&mut env, None).await;
    promote_to_await_leader_lease(&engine, REGION_ID).await;
    put(&engine, REGION_ID, &schema, 0).await.unwrap().unwrap();
    let request = || match reason {
        FlushReason::Alter => add_field_request(),
        _ => RegionRequest::EnterStaging(EnterStagingRequest {
            partition_directive: StagingPartitionDirective::RejectAllWrites,
        }),
    };
    let reason_label: &'static str = (&reason).into();
    let flushes = || {
        FLUSH_REQUESTS_TOTAL
            .with_label_values(&[reason_label])
            .get()
    };
    let before = flushes();

    assert_retryable(
        engine.handle_request(REGION_ID, request()).await,
        lease_not_received,
    );
    assert_eq!(flushes(), before);
    assert!(manifest_files(&engine).await.is_empty());

    grant_at(&engine, RegionRole::Leader, Instant::now(), None);
    engine.handle_request(REGION_ID, request()).await.unwrap();
    assert_eq!(flushes(), before + 1);
    assert_eq!(manifest_files(&engine).await, vec![3]);
}

/// A close of a region that awaits its Leader lease, such as the close of a shutdown, keeps rows
/// that are not in the WAL: it flushes them as a downgrade does. Rows in the WAL need no flush.
#[rstest]
#[case::wal_rows(false, false)]
#[case::flush_on_close(false, true)]
#[case::skip_wal_rows(true, false)]
#[tokio::test]
async fn test_close_of_region_awaiting_leader_lease_keeps_rows(
    #[case] skip_wal: bool,
    #[case] flush_on_close: bool,
) {
    common_telemetry::init_default_ut_logging();
    let mut env = TestEnv::with_prefix("close-awaiting-lease").await;
    let (_listener, engine, schema) = create_region(&mut env, None).await;
    promote_to_await_leader_lease(&engine, REGION_ID).await;
    let mut request = put_request(&schema, 0);
    if let RegionRequest::Put(put) = &mut request {
        put.skip_wal = skip_wal;
    }
    engine.handle_request(REGION_ID, request).await.unwrap();

    let close = RegionRequest::Close(RegionCloseRequest { flush_on_close });
    engine.handle_request(REGION_ID, close).await.unwrap();
    assert!(engine.get_region(REGION_ID).is_none());

    let engine = env.reopen_engine(engine, MitoConfig::default()).await;
    let table_dir = CreateRequestBuilder::new().build().table_dir;
    reopen_region(&engine, REGION_ID, table_dir, false, HashMap::new()).await;
    let flushed = skip_wal || flush_on_close;
    assert_eq!(manifest_files(&engine).await.len(), usize::from(flushed));
    assert_eq!(count_rows(&engine).await, 3);
}

#[tokio::test]
async fn test_staging_of_region_awaiting_leader_lease_ends_after_lease() {
    common_telemetry::init_default_ut_logging();
    let mut env = TestEnv::with_prefix("end-staging-awaiting-lease").await;
    let (_listener, engine, schema) = create_region(&mut env, None).await;
    grant(&engine, RegionRole::Leader, Instant::now() + LEASE);
    put(&engine, REGION_ID, &schema, 0).await.unwrap().unwrap();
    flush(&engine).await.unwrap();
    promote_to_await_leader_lease(&engine, REGION_ID).await;
    // Staged changes stay in memory, which a region that awaits its Leader lease may do.
    enter_staging(&engine, REGION_ID).await;
    let region = engine.get_region(REGION_ID).unwrap();
    submit_staging_edit(
        &engine,
        REGION_ID,
        first_file(&region).await,
        float_range_expr("field_0", 0., 100.).as_json_str().unwrap(),
    )
    .await
    .unwrap();
    let version = region.manifest_ctx.manifest_version().await;
    let end_staging =
        || engine.set_region_role_state_gracefully(REGION_ID, SettableRegionRoleState::Leader);

    let SetRegionRoleStateResponse::InvalidTransition(err) = end_staging().await.unwrap() else {
        panic!("the end of staging must fail before the Leader lease");
    };
    let err = err.into_inner();
    assert!(
        lease_not_received(err.as_any().downcast_ref::<Error>().unwrap()),
        "{err:?}"
    );
    assert!(region.is_staging());
    assert_eq!(region.manifest_ctx.manifest_version().await, version);

    grant_at(&engine, RegionRole::StagingLeader, Instant::now(), None);
    assert!(matches!(
        end_staging().await.unwrap(),
        SetRegionRoleStateResponse::Success(_)
    ));
    assert!(!region.is_staging());
    assert!(region.manifest_ctx.manifest_version().await > version);
}

#[tokio::test]
async fn test_downgrade_flushes_region_awaiting_leader_lease() {
    common_telemetry::init_default_ut_logging();
    let mut env = TestEnv::with_prefix("downgrade-awaiting-lease").await;
    let (_listener, engine, schema) = create_region(&mut env, None).await;
    promote_to_await_leader_lease(&engine, REGION_ID).await;
    // Rows that are not in the WAL survive the migration only through the downgrade's flush.
    let mut request = put_request(&schema, 0);
    if let RegionRequest::Put(put) = &mut request {
        put.skip_wal = true;
    }
    engine.handle_request(REGION_ID, request).await.unwrap();

    engine
        .set_region_role_state_gracefully(REGION_ID, SettableRegionRoleState::DowngradingLeader)
        .await
        .unwrap();
    assert!(
        engine
            .get_region(REGION_ID)
            .unwrap()
            .manifest_ctx
            .awaits_leader_lease()
    );
    flush(&engine).await.unwrap();

    assert_eq!(manifest_files(&engine).await, vec![3]);
}

#[rstest]
#[case::awaiting_leader_lease(false)]
#[case::leader_lease_received(true)]
#[tokio::test]
async fn test_catchup_catches_up_again_only_region_awaiting_leader_lease(
    #[case] leader_lease_received: bool,
) {
    common_telemetry::init_default_ut_logging();
    let mut env = TestEnv::with_prefix("catchup-writable-region").await;
    let (_listener, engine, schema) = create_region(&mut env, None).await;
    promote_to_await_leader_lease(&engine, REGION_ID).await;
    if leader_lease_received {
        grant_at(&engine, RegionRole::Leader, Instant::now(), None);
    }
    // The memtable is not empty, so a catchup reopens the region.
    put(&engine, REGION_ID, &schema, 0).await.unwrap().unwrap();
    let region = engine.get_region(REGION_ID).unwrap();

    catchup(&engine).await.unwrap();

    let caught_up = engine.get_region(REGION_ID).unwrap();
    // A region that the Leader lease confirmed is left as it is; another one is demoted and
    // catches up again.
    assert_eq!(Arc::ptr_eq(&region, &caught_up), leader_lease_received);
    assert_eq!(region.is_writable(), leader_lease_received);
    // The local WAL replays nothing, so a catchup again drops the unflushed rows.
    assert_eq!(
        count_rows(&engine).await,
        if leader_lease_received { 3 } else { 0 }
    );
    assert!(caught_up.is_writable());
    assert_eq!(
        caught_up.manifest_ctx.awaits_leader_lease(),
        !leader_lease_received
    );
}

#[tokio::test]
async fn test_leader_lease_sent_before_catchup_again_does_not_promote_kept_region() {
    common_telemetry::init_default_ut_logging();
    let mut env = TestEnv::with_prefix("catchup-again-earlier-lease").await;
    let (_listener, engine, _schema) = create_region(&mut env, None).await;
    promote_to_await_leader_lease(&engine, REGION_ID).await;
    let region = engine.get_region(REGION_ID).unwrap();
    let sent_before_catchup = Instant::now();

    // Without `set_writable`, the catchup leaves the region the follower it demoted it to.
    engine
        .handle_request(
            REGION_ID,
            RegionRequest::Catchup(RegionCatchupRequest::default()),
        )
        .await
        .unwrap();

    // The memtable is empty and the manifest unchanged, so the catchup keeps the instance.
    assert!(Arc::ptr_eq(&region, &engine.get_region(REGION_ID).unwrap()));
    assert!(!region.is_writable());
    assert!(!grant_at(
        &engine,
        RegionRole::Leader,
        sent_before_catchup,
        None
    ));
    assert!(!region.is_writable());
    // A lease sent after the catchup promotes it.
    assert!(grant_at(&engine, RegionRole::Leader, Instant::now(), None));
    assert!(region.is_writable());
}

/// Opens a region whose opener filled in the partition expression its manifest lacks and makes
/// it writable by a catchup, after which it awaits its Leader lease. Returns the engine and the
/// expression.
async fn catchup_region_with_backfill(
    env: &mut TestEnv,
    listener: Option<EventListenerRef>,
) -> (MitoEngine, String) {
    let (engine, expr_json, _) =
        open_region_with_backfilled_partition_expr(env, REGION_ID, listener).await;
    engine.require_write_lease();
    engine.freeze_lease_clock(Instant::now());
    engine.renew_node_lease(Instant::now() + LEASE);
    catchup(&engine).await.unwrap();
    // The catchup cannot commit it before the Leader lease.
    assert_eq!(persisted_partition_expr(&engine).await, None);
    (engine, expr_json)
}

async fn persisted_partition_expr(engine: &MitoEngine) -> Option<String> {
    let region = engine.get_region(REGION_ID).unwrap();
    region
        .manifest_ctx
        .manifest()
        .await
        .metadata
        .partition_expr
        .clone()
}

async fn wait_persisted_partition_expr(engine: &MitoEngine) -> String {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Some(expr) = persisted_partition_expr(engine).await {
                return expr;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap()
}

#[rstest]
#[case::leader(RegionRole::Leader)]
#[case::staging_leader(RegionRole::StagingLeader)]
#[tokio::test]
async fn test_leader_lease_after_catchup_persists_backfilled_partition_expr(
    #[case] role: RegionRole,
) {
    common_telemetry::init_default_ut_logging();
    let mut env = TestEnv::with_prefix("lease-persists-backfill").await;
    let (engine, expr_json) = catchup_region_with_backfill(&mut env, None).await;

    grant_at(&engine, role, Instant::now(), None);

    assert_eq!(wait_persisted_partition_expr(&engine).await, expr_json);
}

/// Counts the ends of a region's wait for its Leader lease, once the worker handled them, and
/// the persisting of backfilled metadata the worker spawned.
#[derive(Default)]
struct LeaseWaitEndedListener {
    wait_ends: AtomicUsize,
    backfill_persists: AtomicUsize,
}

#[async_trait::async_trait]
impl EventListener for LeaseWaitEndedListener {
    fn on_leader_lease_wait_ended(&self, _region_id: RegionId) {
        // Releases the persist count recorded before it, which the test acquires with `wait_ends`.
        self.wait_ends.fetch_add(1, Ordering::Release);
    }

    fn on_persist_backfilled_metadata_begin(&self, _region_id: RegionId) {
        self.backfill_persists.fetch_add(1, Ordering::Relaxed);
    }
}

impl LeaseWaitEndedListener {
    async fn wait_for_end(&self) {
        wait_until(|| self.wait_ends.load(Ordering::Acquire) != 0).await;
    }
}

/// A Follower or DowngradingLeader lease during the wait leaves the region waiting. The next
/// Leader lease promotes it and ends the wait, after which the worker persists the backfilled
/// metadata once.
#[rstest]
#[case::follower(RegionRole::Follower)]
#[case::downgrading_leader(RegionRole::DowngradingLeader)]
#[tokio::test]
async fn test_leader_lease_after_demotion_ends_wait(#[case] role: RegionRole) {
    common_telemetry::init_default_ut_logging();
    let mut env = TestEnv::with_prefix("lease-after-demotion").await;
    let listener = Arc::new(LeaseWaitEndedListener::default());
    let (engine, expr_json) = catchup_region_with_backfill(&mut env, Some(listener.clone())).await;
    let region = engine.get_region(REGION_ID).unwrap();

    grant_at(&engine, role, Instant::now(), None);
    assert!(!region.is_writable());
    assert!(region.manifest_ctx.awaits_leader_lease());

    grant_at(&engine, RegionRole::Leader, Instant::now(), None);
    assert!(region.is_writable());
    listener.wait_for_end().await;
    assert_eq!(listener.backfill_persists.load(Ordering::Relaxed), 1);
    assert_eq!(wait_persisted_partition_expr(&engine).await, expr_json);
}

/// A lease that finds the region in the middle of a change persists no backfilled metadata.
#[tokio::test]
async fn test_leader_lease_during_change_persists_no_backfill() {
    common_telemetry::init_default_ut_logging();
    let mut env = TestEnv::with_prefix("lease-during-change-backfill").await;
    let listener = Arc::new(LeaseWaitEndedListener::default());
    let (engine, _) = catchup_region_with_backfill(&mut env, Some(listener.clone())).await;
    let region = engine.get_region(REGION_ID).unwrap();
    region.set_editing(RegionLeaderState::Writable).unwrap();

    grant_at(&engine, RegionRole::Leader, Instant::now(), None);
    listener.wait_for_end().await;

    // The worker reports the persisting as it spawns it, and the end of the wait after.
    assert_eq!(listener.backfill_persists.load(Ordering::Relaxed), 0);
    region.switch_state_to_writable(RegionLeaderState::Editing);
    assert_eq!(persisted_partition_expr(&engine).await, None);
}

/// The replayed and new rows of a region that awaits its Leader lease stay in its memtable until
/// the lease ends the wait. A write the engine's full buffer stalls completes after the lease
/// with no other write; a write the region's own buffer would stall is rejected, since only the
/// lease could release it.
#[rstest]
#[case::engine_full(FlushReason::EngineFull)]
#[case::region_full(FlushReason::RegionFull)]
#[tokio::test]
async fn test_full_write_buffer_waits_for_leader_lease_to_flush(#[case] reason: FlushReason) {
    common_telemetry::init_default_ut_logging();
    let mut env = TestEnv::with_prefix("full-buffer-awaiting-lease").await;
    let engine_full = reason == FlushReason::EngineFull;
    let config = if engine_full {
        // Any row fills it.
        MitoConfig {
            global_write_buffer_size: ReadableSize(1),
            global_write_buffer_reject_size: ReadableSize::mb(64),
            ..Default::default()
        }
    } else {
        // One put of three rows (167 bytes) fills it.
        MitoConfig {
            default_region_write_buffer_size: ReadableSize(100),
            ..Default::default()
        }
    };
    let listener = Arc::new(CountingStallListener::default());
    let (engine, schema) =
        create_region_with_config(&mut env, None, config, listener.clone()).await;
    promote_to_await_leader_lease(&engine, REGION_ID).await;
    put(&engine, REGION_ID, &schema, 0).await.unwrap().unwrap();
    let flush_failures = FLUSH_FAILURE_TOTAL.get();
    let reason_label: &'static str = (&reason).into();
    let reason_flushes = || {
        FLUSH_REQUESTS_TOTAL
            .with_label_values(&[reason_label])
            .get()
    };
    let flushes = reason_flushes();

    // The buffer is full and the region starts no flush to free it.
    let second = put(&engine, REGION_ID, &schema, 3);
    let stalled = if engine_full {
        tokio::time::timeout(Duration::from_secs(10), listener.wait_for_count(1))
            .await
            .unwrap();
        assert!(!second.is_finished());
        Some(second)
    } else {
        let err = tokio::time::timeout(Duration::from_secs(10), second)
            .await
            .unwrap()
            .unwrap()
            .unwrap_err();
        assert_eq!(err.status_code(), StatusCode::StorageUnavailable, "{err:?}");
        None
    };
    assert_eq!(reason_flushes(), flushes);
    assert!(manifest_files(&engine).await.is_empty());

    // The lease starts the flush. Its completion releases a stalled write, and frees the
    // region's buffer for the write it rejected.
    grant_at(&engine, RegionRole::Leader, Instant::now(), None);
    if let Some(stalled) = stalled {
        tokio::time::timeout(Duration::from_secs(10), stalled)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    } else {
        tokio::time::timeout(Duration::from_secs(10), async {
            while manifest_files(&engine).await.is_empty() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        put(&engine, REGION_ID, &schema, 3).await.unwrap().unwrap();
    }
    // The released write may fill the buffer and start another flush.
    assert!(reason_flushes() > flushes);
    assert!(!manifest_files(&engine).await.is_empty());
    assert_eq!(FLUSH_FAILURE_TOTAL.get(), flush_failures);
}

#[tokio::test]
async fn test_compaction_skipped_awaiting_leader_lease() {
    common_telemetry::init_default_ut_logging();
    let mut env = TestEnv::with_prefix("compaction-awaiting-lease").await;
    let (_listener, engine, schema) = create_region(&mut env, None).await;
    grant(&engine, RegionRole::Leader, Instant::now() + LEASE);
    for start in [0, 3] {
        put(&engine, REGION_ID, &schema, start)
            .await
            .unwrap()
            .unwrap();
        flush(&engine).await.unwrap();
    }
    promote_to_await_leader_lease(&engine, REGION_ID).await;
    let compact = || {
        RegionRequest::Compact(RegionCompactRequest {
            options: compact_request::Options::StrictWindow(StrictWindow { window_seconds: 0 }),
            ..Default::default()
        })
    };

    engine.handle_request(REGION_ID, compact()).await.unwrap();
    assert_eq!(manifest_files(&engine).await.len(), 2);

    grant_at(&engine, RegionRole::Leader, Instant::now(), None);
    engine.handle_request(REGION_ID, compact()).await.unwrap();
    assert_eq!(manifest_files(&engine).await, vec![6]);
}

/// An index build retry that the wait refuses is built when the lease ends the wait, also for a
/// file that the lease-end pass leaves since its writer built an extension index, and also when a
/// catchup that keeps the instance held back an earlier lease.
#[rstest]
#[case::lease(false)]
#[case::lease_held_back_by_catchup(true)]
#[tokio::test]
async fn test_index_build_retry_refused_awaiting_leader_lease_runs_after_it(
    #[case] held_back_by_catchup: bool,
) {
    common_telemetry::init_default_ut_logging();
    let mut env = TestEnv::with_prefix("index-retry-awaiting-lease").await;
    let engine = env
        .create_engine_with(async_build_mode_config(false), None, None, None)
        .await;
    engine.require_write_lease();
    engine.freeze_lease_clock(Instant::now());
    let request = CreateRequestBuilder::new().build_with_index();
    let schema = rows_schema(&request);
    engine
        .handle_request(REGION_ID, RegionRequest::Create(request))
        .await
        .unwrap();
    grant(&engine, RegionRole::Leader, Instant::now() + LEASE);
    let rows = Rows {
        schema,
        rows: build_rows(0, 3),
    };
    put_rows(&engine, REGION_ID, rows).await;
    flush_region(&engine, REGION_ID, None).await;
    let region = engine.get_region(REGION_ID).unwrap();
    let mut files = region.all_manifest_files().await.0;
    files[0].available_indexes.push(IndexType::HllIndex);
    let edit = RegionEdit {
        files_to_add: files.clone(),
        ..Default::default()
    };
    engine.edit_region(REGION_ID, edit).await.unwrap();
    promote_to_await_leader_lease(&engine, REGION_ID).await;

    // What a publication that found the schema changed sends.
    let notify = BackgroundNotify::IndexBuildRetry(BuildIndexRequest {
        region_id: REGION_ID,
        build_type: IndexBuildType::Manual,
        file_metas: files,
        result_senders: Vec::new(),
    });
    let request = WorkerRequest::Background {
        region_id: REGION_ID,
        notify,
    };
    let workers = &engine.inner.workers;
    workers.submit_to_worker(REGION_ID, request).await.unwrap();
    // The worker handled the retry before this request.
    let build = RegionRequest::BuildIndex(RegionBuildIndexRequest::default());
    assert_retryable(
        engine.handle_request(REGION_ID, build).await,
        lease_not_received,
    );
    let has_inverted_index = || {
        let version = engine.get_region(REGION_ID).unwrap().version();
        version.ssts.levels()[0].files.values().any(|file| {
            file.meta_ref()
                .available_indexes
                .contains(&IndexType::InvertedIndex)
        })
    };
    assert!(!has_inverted_index());

    if held_back_by_catchup {
        // A catchup that keeps the instance holds the lease back, which ends no wait, so the
        // kept retry stays for the wait the catchup's promotion starts.
        let region = engine.get_region(REGION_ID).unwrap();
        assert_eq!(region.manifest_ctx.begin_catchup(REGION_ID), Some(true));
        assert!(region.is_follower());
        assert!(!grant_at(&engine, RegionRole::Leader, Instant::now(), None));
        assert!(region.manifest_ctx.awaits_leader_lease());
        assert!(region.manifest_ctx.promote_after_catchup(REGION_ID));
    }
    grant_at(&engine, RegionRole::Leader, Instant::now(), None);
    wait_until(has_inverted_index).await;
}
