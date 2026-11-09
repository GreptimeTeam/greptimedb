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

//! Tests for regions whose WAL options select the object store WAL.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use api::v1::region::{StrictWindow, compact_request};
use api::v1::{ColumnSchema, Rows};
use common_datasource::compression::CompressionType;
use common_error::ext::ErrorExt;
use common_error::status_code::StatusCode;
use common_recordbatch::RecordBatches;
use common_wal::config::object_store::{ObjectStoreWalConfig, STANDALONE_GENERATION};
use common_wal::options::{ObjectStoreWalOptions, WAL_OPTIONS_KEY, WalOptions};
use futures::TryStreamExt;
use log_store::ObjectStoreLogStore;
use object_store::ObjectStore;
use rstest::rstest;
use store_api::logstore::LogStore;
use store_api::logstore::provider::Provider;
use store_api::region_engine::{RegionEngine, RegionRole};
use store_api::region_request::{
    PathType, RegionCleanUpRequest, RegionCloseRequest, RegionCompactRequest, RegionDropRequest,
    RegionFlushRequest, RegionOpenRequest, RegionRequest, RegionTruncateRequest,
};
use store_api::storage::{RegionId, ScanRequest};

use crate::config::MitoConfig;
use crate::engine::MitoEngine;
use crate::manifest::storage::{delta_file, gen_path, manifest_dir};
use crate::sst::location::region_dir_from_table_dir;
use crate::test_util::{
    CheckpointTaskBlocker, CreateRequestBuilder, TestEnv, build_rows, flush_region, nop_action,
    put_rows, reopen_region, rows_schema,
};
use crate::wal::EntryId;

fn wal_config() -> ObjectStoreWalConfig {
    ObjectStoreWalConfig {
        prefix: "cluster-a/wal".to_string(),
        ..Default::default()
    }
}

/// The node prefix a standalone datanode derives from the configured root,
/// which the regions persist in their WAL options.
fn node_prefix() -> String {
    wal_config().node_prefix(0, STANDALONE_GENERATION)
}

fn wal_options() -> HashMap<String, String> {
    let options = WalOptions::ObjectStore(ObjectStoreWalOptions::new(node_prefix()));
    HashMap::from([(
        WAL_OPTIONS_KEY.to_string(),
        serde_json::to_string(&options).unwrap(),
    )])
}

fn object_store_provider(region_id: RegionId) -> Provider {
    Provider::object_store_provider(region_id, node_prefix())
}

fn open_request(table_dir: String) -> RegionOpenRequest {
    RegionOpenRequest {
        engine: String::new(),
        table_dir,
        options: wal_options(),
        skip_wal_replay: false,
        path_type: PathType::Bare,
        checkpoint: None,
        requirements: Default::default(),
    }
}

pub(crate) async fn new_engine(env: &mut TestEnv) -> (MitoEngine, Arc<ObjectStoreLogStore>) {
    env.create_engine_with_object_store_wal(MitoConfig::default(), &wal_config())
        .await
}

/// Creates a region with object store WAL options and returns its table dir and row schema.
pub(crate) async fn create_region(
    engine: &MitoEngine,
    region_id: RegionId,
) -> (String, Vec<api::v1::ColumnSchema>) {
    let request = CreateRequestBuilder::new()
        .insert_option(WAL_OPTIONS_KEY, &wal_options()[WAL_OPTIONS_KEY])
        .build();
    let table_dir = request.table_dir.clone();
    let schema = rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();
    (table_dir, schema)
}

/// Creates a region and writes rows `[start, end)` to it in one WAL entry.
async fn create_region_with_rows(
    engine: &MitoEngine,
    region_id: RegionId,
    start: usize,
    end: usize,
) -> String {
    let (table_dir, schema) = create_region(engine, region_id).await;
    put_rows(
        engine,
        region_id,
        Rows {
            schema,
            rows: build_rows(start, end),
        },
    )
    .await;
    table_dir
}

async fn close_region(engine: &MitoEngine, region_id: RegionId) {
    engine
        .handle_request(
            region_id,
            RegionRequest::Close(RegionCloseRequest::default()),
        )
        .await
        .unwrap();
}

async fn count_rows(engine: &MitoEngine, region_id: RegionId) -> usize {
    let stream = engine
        .scan_to_stream(region_id, ScanRequest::default())
        .await
        .unwrap();
    RecordBatches::try_collect(stream)
        .await
        .unwrap()
        .iter()
        .map(|batch| batch.num_rows())
        .sum()
}

/// Returns the ids of the entries the store still hands out for `region_id`.
async fn stored_entry_ids(log_store: &ObjectStoreLogStore, region_id: RegionId) -> Vec<EntryId> {
    log_store
        .read(&object_store_provider(region_id), 0)
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap()
        .into_iter()
        .flatten()
        .map(|entry| entry.entry_id())
        .collect()
}

#[tokio::test]
async fn test_create_and_reopen_region_with_object_store_wal() {
    let mut env = TestEnv::with_prefix("object-store-wal-reopen").await;
    let (engine, log_store) = new_engine(&mut env).await;
    let region_id = RegionId::new(1, 1);
    let provider = object_store_provider(region_id);

    let table_dir = create_region_with_rows(&engine, region_id, 0, 3).await;
    assert_eq!(provider, engine.get_region(region_id).unwrap().provider);
    let last_entry_id = log_store.latest_entry_id(&provider).unwrap();
    assert_eq!(
        vec![last_entry_id],
        stored_entry_ids(&log_store, region_id).await
    );

    reopen_region(&engine, region_id, table_dir, true, wal_options()).await;
    let region = engine.get_region(region_id).unwrap();
    assert_eq!(provider, region.provider);
    assert_eq!(
        last_entry_id,
        region.version_control.current().last_entry_id
    );
    assert_eq!(3, count_rows(&engine, region_id).await);
}

#[tokio::test]
async fn test_topic_latest_entry_id_follows_replay() {
    let mut env = TestEnv::with_prefix("object-store-wal-latest-entry-id").await;
    let (engine, log_store) = new_engine(&mut env).await;
    let region_id = RegionId::new(1, 1);
    let provider = object_store_provider(region_id);

    let (table_dir, schema) = create_region(&engine, region_id).await;
    let region = engine.get_region(region_id).unwrap();
    assert_eq!(0, region.version_control.current().version.flushed_entry_id);
    assert_eq!(0, region.topic_latest_entry_id.load(Ordering::Relaxed));

    put_rows(
        &engine,
        region_id,
        Rows {
            schema: schema.clone(),
            rows: build_rows(0, 3),
        },
    )
    .await;
    flush_region(&engine, region_id, None).await;
    let flushed_entry_id = region.version_control.current().version.flushed_entry_id;
    assert_eq!(
        flushed_entry_id,
        log_store.latest_entry_id(&provider).unwrap()
    );
    put_rows(
        &engine,
        region_id,
        Rows {
            schema,
            rows: build_rows(3, 5),
        },
    )
    .await;
    let latest_entry_id = log_store.latest_entry_id(&provider).unwrap();
    assert!(latest_entry_id > flushed_entry_id);

    // Replay refills the memtable with the unflushed entry, so the topic latest
    // entry id stays at the replay start.
    reopen_region(&engine, region_id, table_dir.clone(), true, wal_options()).await;
    let region = engine.get_region(region_id).unwrap();
    assert_eq!(
        flushed_entry_id,
        region.version_control.current().version.flushed_entry_id
    );
    assert_eq!(
        latest_entry_id,
        region.version_control.current().last_entry_id
    );
    assert_eq!(
        flushed_entry_id,
        region.topic_latest_entry_id.load(Ordering::Relaxed)
    );
    assert_eq!(5, count_rows(&engine, region_id).await);

    // Nothing is replayed after a flush, so the topic latest entry id comes from the store.
    flush_region(&engine, region_id, None).await;
    reopen_region(&engine, region_id, table_dir, true, wal_options()).await;
    let region = engine.get_region(region_id).unwrap();
    assert_eq!(
        latest_entry_id,
        region.version_control.current().version.flushed_entry_id
    );
    assert_eq!(
        latest_entry_id,
        region.topic_latest_entry_id.load(Ordering::Relaxed)
    );
    assert_eq!(5, count_rows(&engine, region_id).await);
}

#[tokio::test]
async fn test_open_region_replays_only_its_own_entries() {
    let mut env = TestEnv::with_prefix("object-store-wal-isolation").await;
    let (engine, log_store) = new_engine(&mut env).await;
    let region_a = RegionId::new(1, 1);
    let region_b = RegionId::new(1, 2);

    let table_dir_a = create_region_with_rows(&engine, region_a, 0, 3).await;
    let table_dir_b = create_region_with_rows(&engine, region_b, 0, 5).await;
    close_region(&engine, region_a).await;
    close_region(&engine, region_b).await;
    // The store holds the entries of both regions under the same prefix.
    assert_eq!(1, stored_entry_ids(&log_store, region_a).await.len());
    assert_eq!(1, stored_entry_ids(&log_store, region_b).await.len());

    engine
        .handle_request(region_a, RegionRequest::Open(open_request(table_dir_a)))
        .await
        .unwrap();
    assert_eq!(3, count_rows(&engine, region_a).await);
    assert!(!engine.is_region_exists(region_b));

    engine
        .handle_request(region_b, RegionRequest::Open(open_request(table_dir_b)))
        .await
        .unwrap();
    assert_eq!(5, count_rows(&engine, region_b).await);
    assert_eq!(3, count_rows(&engine, region_a).await);
}

#[tokio::test]
async fn test_batch_open_regions_with_object_store_wal() {
    let mut env = TestEnv::with_prefix("object-store-wal-batch-open").await;
    let (engine, _) = new_engine(&mut env).await;

    let mut regions = Vec::new();
    for i in 1..=3 {
        let region_id = RegionId::new(1, i);
        let num_rows = i as usize + 1;
        let table_dir = create_region_with_rows(&engine, region_id, 0, num_rows).await;
        close_region(&engine, region_id).await;
        regions.push((region_id, table_dir, num_rows));
    }

    let requests = regions
        .iter()
        .map(|(region_id, table_dir, _)| (*region_id, open_request(table_dir.clone())))
        .collect();
    let responses = engine
        .handle_batch_open_requests(4, requests)
        .await
        .unwrap();
    assert_eq!(regions.len(), responses.len());
    for (_, response) in responses {
        response.unwrap();
    }

    for (region_id, _, num_rows) in regions {
        assert_eq!(
            object_store_provider(region_id),
            engine.get_region(region_id).unwrap().provider
        );
        assert_eq!(num_rows, count_rows(&engine, region_id).await);
    }
}

#[tokio::test]
async fn test_drop_and_offline_cleanup_with_object_store_wal() {
    let mut env = TestEnv::with_prefix("object-store-wal-cleanup").await;
    let (engine, log_store) = new_engine(&mut env).await;
    let region_a = RegionId::new(1, 1);
    let region_b = RegionId::new(1, 2);

    let _ = create_region_with_rows(&engine, region_a, 0, 3).await;
    let table_dir_b = create_region_with_rows(&engine, region_b, 0, 3).await;

    engine
        .handle_request(
            region_a,
            RegionRequest::Drop(RegionDropRequest {
                fast_path: false,
                force: false,
                partial_drop: false,
            }),
        )
        .await
        .unwrap();
    // The drop obsoletes the entries of region A only.
    assert!(stored_entry_ids(&log_store, region_a).await.is_empty());
    assert_eq!(1, stored_entry_ids(&log_store, region_b).await.len());

    close_region(&engine, region_b).await;
    engine
        .handle_request(
            region_b,
            RegionRequest::CleanUp(RegionCleanUpRequest {
                engine: String::new(),
                table_dir: table_dir_b,
                path_type: PathType::Bare,
                options: wal_options(),
            }),
        )
        .await
        .unwrap();
    assert!(stored_entry_ids(&log_store, region_b).await.is_empty());
}

#[tokio::test]
async fn test_object_store_wal_options_reject_raft_engine_log_store() {
    let mut env = TestEnv::with_prefix("object-store-wal-raft-engine").await;
    let engine = env.create_engine(MitoConfig::default()).await;

    let request = CreateRequestBuilder::new()
        .insert_option(WAL_OPTIONS_KEY, &wal_options()[WAL_OPTIONS_KEY])
        .build();
    let err = engine
        .handle_request(RegionId::new(1, 1), RegionRequest::Create(request))
        .await
        .unwrap_err();
    assert_eq!(StatusCode::InvalidArguments, err.status_code());
}

/// Returns a config that starts a checkpoint on every commit.
fn checkpoint_every_commit() -> MitoConfig {
    MitoConfig {
        manifest_checkpoint_distance: 1,
        ..Default::default()
    }
}

/// Creates a region whose writes require a lease. Returns the lease deadline, which the frozen
/// clock of the lease checks has not reached.
async fn create_leased_region(
    env: &mut TestEnv,
    region_id: RegionId,
) -> (
    MitoEngine,
    Arc<ObjectStoreLogStore>,
    String,
    Vec<ColumnSchema>,
    Instant,
) {
    let (engine, log_store) = env
        .create_engine_with_object_store_wal(checkpoint_every_commit(), &wal_config())
        .await;
    engine.require_write_lease();
    engine.freeze_lease_clock(Instant::now());
    let (table_dir, schema) = create_region(&engine, region_id).await;
    let deadline = Instant::now() + Duration::from_secs(10);
    engine
        .set_region_role_with_lease(region_id, RegionRole::Leader, Instant::now(), deadline)
        .unwrap();
    (engine, log_store, table_dir, schema, deadline)
}

async fn count_ssts(store: &ObjectStore, region_dir: &str) -> usize {
    let entries = store.list(region_dir).await.unwrap();
    entries
        .iter()
        .filter(|e| e.name().ends_with(".parquet"))
        .count()
}

#[rstest]
#[case::flush(RegionRequest::Flush(RegionFlushRequest::default()), 1)]
#[case::compaction(
    RegionRequest::Compact(RegionCompactRequest {
        options: compact_request::Options::StrictWindow(StrictWindow { window_seconds: 3600 }),
        ..Default::default()
    }),
    1
)]
#[case::truncate(RegionRequest::Truncate(RegionTruncateRequest::All), 0)]
#[tokio::test]
async fn test_stale_commit_on_deleted_delta_name_conflicts(
    #[case] commit: RegionRequest,
    #[case] new_ssts: usize,
) {
    common_telemetry::init_default_ut_logging();
    // The creation is version 0 and the flushes below are versions 1 and 2.
    const VERSION: u64 = 3;
    let (blocker, layer) = CheckpointTaskBlocker::block_delta_write(VERSION);
    let mut env = TestEnv::with_prefix("stale-manifest-commit")
        .await
        .with_mock_layer(layer);
    let region_id = RegionId::new(1, 1);
    let (old_leader, log_store, table_dir, schema, deadline) =
        create_leased_region(&mut env, region_id).await;
    let rows = |start| Rows {
        schema: schema.clone(),
        rows: build_rows(start, start + 3),
    };
    for start in [0, 3] {
        put_rows(&old_leader, region_id, rows(start)).await;
        flush_region(&old_leader, region_id, None).await;
    }
    put_rows(&old_leader, region_id, rows(6)).await;

    let region = old_leader.get_region(region_id).unwrap();
    let (manifest, checkpoint) = {
        let mut manager = region.manifest_ctx.manifest_manager.write().await;
        manager.wait_for_pending_checkpoint().await;
        (
            manager.manifest(),
            manager.checkpointer().last_checkpoint_version(),
        )
    };
    let store = env.get_object_store().unwrap();
    let region_dir = region_dir_from_table_dir(&table_dir, region_id, PathType::Bare);
    let ssts = count_ssts(&store, &region_dir).await;

    let engine = old_leader.clone();
    let commit = tokio::spawn(async move { engine.handle_request(region_id, commit).await });
    blocker.wait_until_blocked().await;
    let version = region.version_control.current().version;
    let entry_ids = stored_entry_ids(&log_store, region_id).await;

    let new_holder = env
        .create_engine_with_log_store(checkpoint_every_commit(), log_store.clone())
        .await;
    new_holder.require_write_lease();
    let open = RegionRequest::Open(open_request(table_dir.clone()));
    new_holder.handle_request(region_id, open).await.unwrap();
    let delta = gen_path(
        &manifest_dir(&region_dir),
        &delta_file(VERSION),
        CompressionType::Gzip,
    );
    {
        let held = new_holder.get_region(region_id).unwrap();
        let mut manager = held.manifest_ctx.manifest_manager.write().await;
        assert_eq!(VERSION - 1, manager.last_version());
        while manager.last_version() < VERSION || store.exists(&delta).await.unwrap() {
            manager
                .update(nop_action(), false, true, || false)
                .await
                .unwrap();
            manager.wait_for_pending_checkpoint().await;
        }
    }

    old_leader.freeze_lease_clock(deadline);
    blocker.release();
    let error = commit.await.unwrap().unwrap_err();
    assert!(error.retry_hint().is_retryable(), "{error:?}");
    assert!(format!("{error:?}").contains("conflicts"), "{error:?}");

    let manager = region.manifest_ctx.manifest_manager.read().await;
    assert_eq!(VERSION - 1, manager.last_version());
    assert!(Arc::ptr_eq(&manifest, &manager.manifest()));
    assert_eq!(checkpoint, manager.checkpointer().last_checkpoint_version());
    assert!(!manager.checkpointer().is_doing_checkpoint());
    drop(manager);
    assert!(Arc::ptr_eq(
        &version,
        &region.version_control.current().version
    ));
    assert_eq!(ssts + new_ssts, count_ssts(&store, &region_dir).await);
    assert!(store.exists(&delta).await.unwrap());
    assert_eq!(entry_ids, stored_entry_ids(&log_store, region_id).await);

    reopen_region(&new_holder, region_id, table_dir, false, wal_options()).await;
    assert_eq!(9, count_rows(&new_holder, region_id).await);
}

#[tokio::test]
async fn test_commit_crossing_deadline_applies_without_checkpoint() {
    common_telemetry::init_default_ut_logging();
    let (blocker, layer) = CheckpointTaskBlocker::block_delta_write(1);
    let mut env = TestEnv::with_prefix("commit-crossing-deadline")
        .await
        .with_mock_layer(layer);
    let region_id = RegionId::new(1, 1);
    let (engine, log_store, table_dir, schema, deadline) =
        create_leased_region(&mut env, region_id).await;
    let rows = Rows {
        schema,
        rows: build_rows(0, 3),
    };
    put_rows(&engine, region_id, rows).await;

    let flush = {
        let engine = engine.clone();
        tokio::spawn(async move {
            let request = RegionRequest::Flush(RegionFlushRequest::default());
            engine.handle_request(region_id, request).await
        })
    };
    blocker.wait_until_blocked().await;
    engine.freeze_lease_clock(deadline);
    blocker.release();
    flush.await.unwrap().unwrap();

    let region = engine.get_region(region_id).unwrap();
    let manager = region.manifest_ctx.manifest_manager.read().await;
    assert_eq!(1, manager.last_version());
    assert_eq!(1, region.version().ssts.levels()[0].files.len());
    assert_eq!(0, manager.checkpointer().last_checkpoint_version());
    assert!(!manager.checkpointer().is_doing_checkpoint());
    assert!(stored_entry_ids(&log_store, region_id).await.is_empty());
    drop(manager);

    reopen_region(&engine, region_id, table_dir, false, wal_options()).await;
    assert_eq!(3, count_rows(&engine, region_id).await);
}
