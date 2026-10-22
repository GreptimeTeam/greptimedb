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
//! lease.

use std::sync::Arc;
use std::time::{Duration, Instant};

use api::region::RegionResponse;
use api::v1::{ColumnSchema, Rows};
use common_error::ext::{BoxedError, ErrorExt};
use common_recordbatch::RecordBatches;
use rstest::rstest;
use rstest_reuse::apply;
use store_api::region_engine::{RegionEngine, RegionRole};
use store_api::region_request::{RegionCatchupRequest, RegionPutRequest, RegionRequest};
use store_api::storage::{RegionId, ScanRequest};
use tokio::task::JoinHandle;

use crate::config::MitoConfig;
use crate::engine::MitoEngine;
use crate::engine::basic_test::build_bulk_insert_request;
use crate::engine::listener::WriteWalListener;
use crate::error::Error;
use crate::test_util::{
    CreateRequestBuilder, LogStoreFactory, TestEnv, build_rows, kafka_log_store_factory,
    multiple_log_store_factories, prepare_test_for_kafka_log_store, raft_engine_log_store_factory,
    rows_schema,
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
    let engine = env
        .create_engine_with(MitoConfig::default(), None, Some(listener.clone()), None)
        .await;
    engine.require_write_lease();
    engine.freeze_lease_clock(Instant::now());
    let request = CreateRequestBuilder::new().kafka_topic(topic).build();
    let schema = rows_schema(&request);
    engine
        .handle_request(REGION_ID, RegionRequest::Create(request))
        .await
        .unwrap();
    (listener, engine, schema)
}

fn grant(engine: &MitoEngine, role: RegionRole, deadline: Instant) {
    engine
        .set_region_role_with_lease(REGION_ID, role, deadline)
        .unwrap();
}

/// Writes rows `start..start + 3`.
fn put(
    engine: &MitoEngine,
    region_id: RegionId,
    schema: &[ColumnSchema],
    start: usize,
) -> JoinHandle<Result<RegionResponse, BoxedError>> {
    let engine = engine.clone();
    let rows = Rows {
        schema: schema.to_vec(),
        rows: build_rows(start, start + 3),
    };
    let request = RegionRequest::Put(RegionPutRequest {
        rows,
        hint: None,
        partition_expr_version: None,
        skip_wal: false,
    });
    tokio::spawn(async move { engine.handle_request(region_id, request).await })
}

/// Writes rows `0..3` and waits until the write is held after its WAL append.
async fn put_and_hold(
    listener: &WriteWalListener,
    engine: &MitoEngine,
    schema: &[ColumnSchema],
) -> JoinHandle<Result<RegionResponse, BoxedError>> {
    listener.pause_next();
    let mut write = put(engine, REGION_ID, schema, 0);
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

    let write = put_and_hold(&listener, &engine, &schema).await;
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
}

#[tokio::test]
async fn test_lease_renewed_while_writing_wal() {
    common_telemetry::init_default_ut_logging();
    let mut env = TestEnv::with_prefix("lease-renewed-while-writing-wal").await;
    let (listener, engine, schema) = create_region(&mut env, None).await;
    let deadline = Instant::now() + LEASE;
    grant(&engine, RegionRole::Leader, deadline);

    let write = put_and_hold(&listener, &engine, &schema).await;
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

    let write = put_and_hold(&listener, &engine, &schema).await;
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
