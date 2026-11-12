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
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use api::v1::region::{StrictWindow, compact_request};
use api::v1::{ColumnSchema, Rows};
use async_trait::async_trait;
use common_base::Plugins;
use common_base::readable_size::ReadableSize;
use common_datasource::compression::CompressionType;
use common_error::ext::ErrorExt;
use common_error::status_code::StatusCode;
use common_recordbatch::RecordBatches;
use common_time::util::current_time_millis;
use common_wal::config::object_store::{AckMode, ObjectStoreWalConfig, STANDALONE_GENERATION};
use common_wal::options::{ObjectStoreWalOptions, WAL_OPTIONS_KEY, WalOptions};
use datatypes::value::Value;
use futures::TryStreamExt;
use log_store::ObjectStoreLogStore;
use object_store::ObjectStore;
use object_store::layers::mock::MockLayerBuilder;
use object_store::services::Memory;
use partition::expr::col;
use rstest::rstest;
use store_api::ManifestVersion;
use store_api::logstore::LogStore;
use store_api::logstore::provider::Provider;
use store_api::region_engine::{
    RegionEngine, RegionRole, SetRegionRoleStateResponse, SettableRegionRoleState,
};
use store_api::region_request::{
    EnterStagingRequest, PathType, RegionCatchupRequest, RegionCleanUpRequest, RegionCloseRequest,
    RegionCompactRequest, RegionDropRequest, RegionFlushRequest, RegionOpenRequest, RegionRequest,
    RegionTruncateRequest, StagingPartitionDirective,
};
use store_api::storage::{FileId, RegionId, ScanRequest};
use tokio::sync::{Notify, oneshot};

use crate::config::MitoConfig;
use crate::engine::MitoEngine;
use crate::engine::listener::{EventListener, FlushCancellationListener};
use crate::engine::region_hook::{RegionHook, RegionHookRef};
use crate::manifest::action::{RegionEdit, RegionMetaAction, RegionMetaActionList};
use crate::manifest::storage::{delta_file, gen_path, manifest_dir};
use crate::region::MitoRegionRef;
use crate::request::{BackgroundNotify, RegionEditRequest, WorkerRequest};
use crate::sst::file::FileMeta;
use crate::sst::location::region_dir_from_table_dir;
use crate::test_util::{
    CheckpointTaskBlocker, CreateRequestBuilder, TestEnv, build_rows, flush_region, nop_action,
    put_rows, reopen_region, rows_schema,
};
use crate::time_provider::StdTimeProvider;
use crate::time_provider::mock::MockTimeProvider;
use crate::wal::EntryId;
use crate::worker::CHECK_REGION_INTERVAL;

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
fn checkpoint_every_commit_config() -> MitoConfig {
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
        .create_engine_with_object_store_wal(checkpoint_every_commit_config(), &wal_config())
        .await;
    let (table_dir, schema, deadline) = create_leased_region_on(&engine, region_id).await;
    (engine, log_store, table_dir, schema, deadline)
}

/// Creates a region whose writes require a lease on `engine`. Returns its table dir, row schema
/// and lease deadline, which the frozen clock of the lease checks has not reached.
async fn create_leased_region_on(
    engine: &MitoEngine,
    region_id: RegionId,
) -> (String, Vec<ColumnSchema>, Instant) {
    engine.require_write_lease();
    engine.freeze_lease_clock(Instant::now());
    let (table_dir, schema) = create_region(engine, region_id).await;
    let deadline = Instant::now() + Duration::from_secs(10);
    engine
        .set_region_role_with_lease(region_id, RegionRole::Leader, Instant::now(), deadline)
        .unwrap();
    (table_dir, schema, deadline)
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
        .create_engine_with_log_store(checkpoint_every_commit_config(), log_store.clone())
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
                .update_with_verification(nop_action(), false, true, || false)
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
    // The conflict fenced the old leader, which reloads the region from the shared WAL.
    wait_until(|| is_replaced(&old_leader, &region)).await;

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

/// Returns a WAL config whose batches are sealed only by
/// [`ObjectStoreLogStore::seal_open_batch`] and whose writes are acknowledged once enqueued.
fn held_wal_config() -> ObjectStoreWalConfig {
    ObjectStoreWalConfig {
        flush_interval: Duration::from_secs(3600),
        max_batch_bytes: ReadableSize(u64::MAX),
        ack_mode: AckMode::Enqueued,
        ..wal_config()
    }
}

async fn new_log_store() -> Arc<ObjectStoreLogStore> {
    ObjectStoreLogStore::try_new(
        ObjectStore::new(Memory::default()).unwrap(),
        &wal_config(),
        0,
        STANDALONE_GENERATION,
    )
    .await
    .unwrap()
}

/// Holds the swap of a reloaded region until released.
#[derive(Default)]
struct ReloadSwapBlocker {
    entered: Notify,
    release: Notify,
}

#[async_trait]
impl EventListener for ReloadSwapBlocker {
    async fn on_region_reload_swap(&self, _region_id: RegionId) {
        self.entered.notify_one();
        self.release.notified().await;
    }
}

/// Holds a catchup before it reopens the region until released.
#[derive(Default)]
struct CatchupReopenBlocker {
    entered: Notify,
    release: Notify,
}

#[async_trait]
impl EventListener for CatchupReopenBlocker {
    async fn on_catchup_reopen_begin(&self, _region_id: RegionId) {
        self.entered.notify_one();
        self.release.notified().await;
    }
}

async fn put_range(
    engine: &MitoEngine,
    region_id: RegionId,
    schema: &[ColumnSchema],
    start: usize,
    end: usize,
) {
    let rows = Rows {
        schema: schema.to_vec(),
        rows: build_rows(start, end),
    };
    put_rows(engine, region_id, rows).await;
}

async fn wait_until(condition: impl Fn() -> bool) {
    tokio::time::timeout(Duration::from_secs(10), async {
        while !condition() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
}

/// Returns whether the engine registers another instance of the region than `old`.
fn is_replaced(engine: &MitoEngine, old: &MitoRegionRef) -> bool {
    engine
        .get_region(old.region_id)
        .is_some_and(|region| !Arc::ptr_eq(&region, old))
}

async fn next_manifest_version(region: &MitoRegionRef) -> u64 {
    region
        .manifest_ctx
        .manifest_manager
        .read()
        .await
        .last_version()
        + 1
}

/// Stores `bytes` as manifest version `version` of the region, as another writer would.
async fn publish_delta(store: &ObjectStore, region_dir: &str, version: u64, bytes: Vec<u8>) {
    let path = gen_path(
        &manifest_dir(region_dir),
        &delta_file(version),
        CompressionType::Gzip,
    );
    store.write(&path, bytes).await.unwrap();
}

async fn encode_delta(action_list: RegionMetaActionList) -> Vec<u8> {
    CompressionType::Gzip
        .encode(&action_list.encode().unwrap()[..])
        .await
        .unwrap()
}

/// Enters staging with a new partition expression, which saves a staging manifest.
fn enter_staging_request() -> RegionRequest {
    let partition_expr = col("field_0")
        .gt_eq(Value::Float64(0.0.into()))
        .and(col("field_0").lt(Value::Float64(100.0.into())))
        .as_json_str()
        .unwrap();
    RegionRequest::EnterStaging(EnterStagingRequest {
        partition_directive: StagingPartitionDirective::UpdatePartitionExpr(partition_expr),
    })
}

/// Fences the region as a version conflict does, without notifying its worker.
async fn fence(region: &MitoRegionRef) {
    let mut manager = region.manifest_ctx.manifest_manager.write().await;
    region
        .manifest_ctx
        .fence_for_reload(&mut manager, region.region_id)
        .await;
}

/// Notifies the region's worker as a commit outside the worker that fenced it does.
async fn notify_fenced(engine: &MitoEngine, region_id: RegionId) {
    let notify = BackgroundNotify::RegionFenced;
    let request = WorkerRequest::Background { region_id, notify };
    engine
        .inner
        .workers
        .submit_to_worker(region_id, request)
        .await
        .unwrap();
}

/// Publishes the next manifest version as another writer, so that the next flush of the region
/// conflicts. Returns the instance the flush fenced.
async fn flush_into_conflict(
    engine: &MitoEngine,
    env: &TestEnv,
    table_dir: &str,
    region_id: RegionId,
) -> MitoRegionRef {
    let region = engine.get_region(region_id).unwrap();
    let region_dir = region_dir_from_table_dir(table_dir, region_id, PathType::Bare);
    let version = next_manifest_version(&region).await;
    let store = env.get_object_store().unwrap();
    publish_delta(
        &store,
        &region_dir,
        version,
        encode_delta(nop_action()).await,
    )
    .await;
    let flush = RegionRequest::Flush(RegionFlushRequest::default());
    let error = engine.handle_request(region_id, flush).await.unwrap_err();
    assert!(format!("{error:?}").contains("conflicts"), "{error:?}");
    region
}

#[rstest]
#[case::flush(RegionRequest::Flush(RegionFlushRequest::default()), 0)]
#[case::compaction(
    RegionRequest::Compact(RegionCompactRequest {
        options: compact_request::Options::StrictWindow(StrictWindow { window_seconds: 3600 }),
        ..Default::default()
    }),
    2
)]
#[tokio::test]
async fn test_commit_after_deadline_sends_nothing(
    #[case] commit: RegionRequest,
    #[case] flushes: usize,
) {
    let mut env = TestEnv::with_prefix("commit-after-deadline").await;
    let region_id = RegionId::new(1, 1);
    let (engine, _log_store, table_dir, schema, deadline) =
        create_leased_region(&mut env, region_id).await;
    for start in (0..flushes).map(|i| i * 3) {
        put_range(&engine, region_id, &schema, start, start + 3).await;
        flush_region(&engine, region_id, None).await;
    }
    put_range(&engine, region_id, &schema, 6, 9).await;
    let region = engine.get_region(region_id).unwrap();
    let version = next_manifest_version(&region).await - 1;
    let store = env.get_object_store().unwrap();
    let region_dir = region_dir_from_table_dir(&table_dir, region_id, PathType::Bare);
    let ssts = count_ssts(&store, &region_dir).await;

    engine.freeze_lease_clock(deadline);
    let error = engine.handle_request(region_id, commit).await.unwrap_err();

    assert!(
        format!("{error:?}").contains("does not hold a valid leader lease"),
        "{error:?}"
    );
    assert_eq!(version + 1, next_manifest_version(&region).await);
    let delta = gen_path(
        &manifest_dir(&region_dir),
        &delta_file(version + 1),
        CompressionType::Gzip,
    );
    assert!(!store.exists(&delta).await.unwrap());
    assert_eq!(ssts, count_ssts(&store, &region_dir).await);
    assert!(!region.manifest_ctx.is_reload_pending());
    assert!(region.is_writable());
}

#[tokio::test]
async fn test_staging_save_after_deadline_sends_nothing() {
    let mut env = TestEnv::with_prefix("staging-after-deadline").await;
    let region_id = RegionId::new(1, 1);
    let (engine, _log_store, _, _, deadline) = create_leased_region(&mut env, region_id).await;
    let region = engine.get_region(region_id).unwrap();
    let version = next_manifest_version(&region).await;

    // The memtable is empty, so entering staging saves its manifest without a flush.
    engine.freeze_lease_clock(deadline);
    let error = engine
        .handle_request(region_id, enter_staging_request())
        .await
        .unwrap_err();

    assert!(
        format!("{error:?}").contains("does not hold a valid leader lease"),
        "{error:?}"
    );
    assert_eq!(version, next_manifest_version(&region).await);
    let store = region.manifest_ctx.manifest_manager.read().await.store();
    assert!(store.fetch_staging_manifests().await.unwrap().is_empty());
    assert!(!region.manifest_ctx.is_reload_pending());
    assert!(region.is_writable());
}

#[tokio::test]
async fn test_default_mode_commit_ignores_leader_deadline() {
    let mut env = TestEnv::with_prefix("default-mode-deadline").await;
    let (engine, _log_store) = new_engine(&mut env).await;
    let region_id = RegionId::new(1, 1);
    let (_, schema) = create_region(&engine, region_id).await;
    put_range(&engine, region_id, &schema, 0, 3).await;

    // The region has no Leader deadline, which counts as passed.
    flush_region(&engine, region_id, None).await;

    let region = engine.get_region(region_id).unwrap();
    assert_eq!(1, region.version().ssts.levels()[0].files.len());
}

#[tokio::test]
async fn test_lease_on_fenced_region_only_extends_deadline() {
    let mut env = TestEnv::with_prefix("lease-on-fenced-region").await;
    let region_id = RegionId::new(1, 1);
    let (engine, _log_store, _, _, deadline) = create_leased_region(&mut env, region_id).await;
    let region = engine.get_region(region_id).unwrap();
    fence(&region).await;
    let fenced_deadline = region.manifest_ctx.leader_deadline.get();

    let later = deadline + Duration::from_secs(10);
    let promoted = engine
        .set_region_role_with_lease(region_id, RegionRole::Leader, Instant::now(), later)
        .unwrap();

    assert!(!promoted);
    assert!(region.manifest_ctx.leader_deadline.get() > fenced_deadline);
    assert!(region.is_follower());
}

#[rstest]
#[case::downgrading_leader(SettableRegionRoleState::DowngradingLeader)]
#[case::follower(SettableRegionRoleState::Follower)]
#[tokio::test]
async fn test_fenced_region_refuses_downgrade(#[case] state: SettableRegionRoleState) {
    let mut env = TestEnv::with_prefix("downgrade-fenced-region").await;
    let region_id = RegionId::new(1, 1);
    let (engine, _log_store, _, _, _) = create_leased_region(&mut env, region_id).await;
    fence(&engine.get_region(region_id).unwrap()).await;

    let response = engine
        .set_region_role_state_gracefully(region_id, state)
        .await
        .unwrap();

    let SetRegionRoleStateResponse::InvalidTransition(error) = response else {
        panic!("a fenced region reported a downgrade: {response:?}");
    };
    assert!(error.retry_hint().is_retryable(), "{error:?}");
}

#[tokio::test]
async fn test_fenced_region_refuses_upgrade() {
    let mut env = TestEnv::with_prefix("upgrade-fenced-region").await;
    let region_id = RegionId::new(1, 1);
    let (engine, _log_store, _, _, _) = create_leased_region(&mut env, region_id).await;
    let region = engine.get_region(region_id).unwrap();
    fence(&region).await;

    let catchup = RegionRequest::Catchup(RegionCatchupRequest {
        set_writable: true,
        ..Default::default()
    });
    let error = engine.handle_request(region_id, catchup).await.unwrap_err();

    assert!(error.retry_hint().is_retryable(), "{error:?}");
    assert!(Arc::ptr_eq(&region, &engine.get_region(region_id).unwrap()));
    assert!(region.is_follower());
}

#[tokio::test]
async fn test_fence_during_catchup_refuses_reopen() {
    let mut env = TestEnv::with_prefix("fence-during-catchup").await;
    let listener = Arc::new(CatchupReopenBlocker::default());
    let engine = env
        .create_engine_with_log_store_and_time(
            MitoConfig::default(),
            new_log_store().await,
            Arc::new(StdTimeProvider),
            Some(listener.clone()),
        )
        .await;
    let region_id = RegionId::new(1, 1);
    let (_, schema, _) = create_leased_region_on(&engine, region_id).await;
    // Rows left in the memtable make the catchup reopen the region.
    put_range(&engine, region_id, &schema, 0, 3).await;
    engine
        .set_region_role(region_id, RegionRole::Follower)
        .unwrap();
    let region = engine.get_region(region_id).unwrap();
    let catchup = {
        let engine = engine.clone();
        let request = RegionRequest::Catchup(RegionCatchupRequest {
            set_writable: true,
            ..Default::default()
        });
        tokio::spawn(async move { engine.handle_request(region_id, request).await })
    };

    // The catchup passed its entry check; a commit fences the region before the reopen.
    listener.entered.notified().await;
    fence(&region).await;
    listener.release.notify_one();

    let error = catchup.await.unwrap().unwrap_err();
    assert!(error.retry_hint().is_retryable(), "{error:?}");
    assert!(Arc::ptr_eq(&region, &engine.get_region(region_id).unwrap()));
}

#[tokio::test]
async fn test_conflict_reloads_region() {
    let mut env = TestEnv::with_prefix("conflict-reloads-region").await;
    let region_id = RegionId::new(1, 1);
    let (engine, _log_store, table_dir, schema, _) =
        create_leased_region(&mut env, region_id).await;
    put_range(&engine, region_id, &schema, 0, 3).await;
    flush_region(&engine, region_id, None).await;
    put_range(&engine, region_id, &schema, 3, 6).await;

    let old = flush_into_conflict(&engine, &env, &table_dir, region_id).await;

    assert!(old.manifest_ctx.manifest_manager.read().await.is_stopped());
    assert!(old.manifest_ctx.is_reload_pending());
    assert!(old.is_follower());
    wait_until(|| is_replaced(&engine, &old)).await;
    let region = engine.get_region(region_id).unwrap();
    assert!(region.is_follower());
    assert!(!region.manifest_ctx.is_reload_pending());
    assert_eq!(
        old.version_control.current().last_sequence,
        region.version_control.current().last_sequence
    );
    assert_eq!(6, count_rows(&engine, region_id).await);
    assert!(!engine.is_region_catching_up(region_id));
}

#[tokio::test]
async fn test_reload_waits_for_held_upload() {
    let mut env = TestEnv::with_prefix("reload-waits-for-upload").await;
    let (engine, log_store) = env
        .create_engine_with_object_store_wal(MitoConfig::default(), &held_wal_config())
        .await;
    let region_id = RegionId::new(1, 1);
    let (_, schema, _) = create_leased_region_on(&engine, region_id).await;
    put_range(&engine, region_id, &schema, 0, 3).await;
    let old = engine.get_region(region_id).unwrap();
    fence(&old).await;

    notify_fenced(&engine, region_id).await;
    let waits = log_store.wait_for_durability_waits(1);
    tokio::time::timeout(Duration::from_secs(10), waits)
        .await
        .unwrap()
        .unwrap();
    assert!(!is_replaced(&engine, &old));
    log_store.seal_open_batch().await.unwrap();

    wait_until(|| is_replaced(&engine, &old)).await;
    assert_eq!(3, count_rows(&engine, region_id).await);
}

#[tokio::test]
async fn test_reload_waits_for_running_flush() {
    let mut env = TestEnv::with_prefix("reload-waits-for-flush").await;
    let listener = Arc::new(FlushCancellationListener::default());
    let engine = env
        .create_engine_with_log_store_and_time(
            MitoConfig::default(),
            new_log_store().await,
            Arc::new(StdTimeProvider),
            Some(listener.clone()),
        )
        .await;
    let region_id = RegionId::new(1, 1);
    let (_, schema, _) = create_leased_region_on(&engine, region_id).await;
    put_range(&engine, region_id, &schema, 0, 3).await;
    let flush = tokio::spawn({
        let engine = engine.clone();
        let request = RegionRequest::Flush(RegionFlushRequest::default());
        async move { engine.handle_request(region_id, request).await }
    });
    listener.wait_flush_started().await;
    let old = engine.get_region(region_id).unwrap();
    fence(&old).await;

    notify_fenced(&engine, region_id).await;
    // The worker handles a background notification before the requests it receives with it.
    let catchup = RegionRequest::Catchup(RegionCatchupRequest::default());
    engine.handle_request(region_id, catchup).await.unwrap_err();
    assert!(!engine.is_region_catching_up(region_id));
    assert!(!is_replaced(&engine, &old));

    listener.resume_flush();
    flush.await.unwrap().unwrap_err();
    wait_until(|| is_replaced(&engine, &old)).await;
    assert_eq!(3, count_rows(&engine, region_id).await);
}

/// Holds the next manifest hook call after it is armed until released.
#[derive(Debug, Default)]
struct BlockingManifestHook {
    armed: AtomicBool,
    entered: Notify,
    release: Notify,
}

#[async_trait]
impl RegionHook for BlockingManifestHook {
    async fn on_manifest_updated(
        &self,
        _region_id: RegionId,
        _action_list: &RegionMetaActionList,
        _manifest_version: ManifestVersion,
    ) {
        if self.armed.swap(false, Ordering::Relaxed) {
            self.entered.notify_one();
            self.release.notified().await;
        }
    }
}

fn edit_files(files_to_add: Vec<FileMeta>, files_to_remove: Vec<FileMeta>) -> RegionEdit {
    RegionEdit {
        files_to_add,
        files_to_remove,
        ..Default::default()
    }
}

#[tokio::test]
async fn test_edit_conflict_reloads_region() {
    let mut env = TestEnv::with_prefix("edit-conflict").await;
    let region_id = RegionId::new(1, 1);
    let (engine, _log_store, table_dir, schema, _) =
        create_leased_region(&mut env, region_id).await;
    put_range(&engine, region_id, &schema, 0, 3).await;
    flush_region(&engine, region_id, None).await;
    put_range(&engine, region_id, &schema, 3, 6).await;
    let old = engine.get_region(region_id).unwrap();
    let flushed = old
        .manifest_ctx
        .manifest()
        .await
        .files
        .values()
        .next()
        .unwrap()
        .clone();
    let added = FileMeta {
        file_id: FileId::random(),
        ..flushed.as_ref().clone()
    };
    let region_dir = region_dir_from_table_dir(&table_dir, region_id, PathType::Bare);
    let version = next_manifest_version(&old).await;
    let store = env.get_object_store().unwrap();
    publish_delta(
        &store,
        &region_dir,
        version,
        encode_delta(nop_action()).await,
    )
    .await;

    let edit = edit_files(vec![added], vec![]);
    engine.edit_region(region_id, edit).await.unwrap_err();

    wait_until(|| is_replaced(&engine, &old)).await;
    assert_eq!(6, count_rows(&engine, region_id).await);
}

#[tokio::test]
async fn test_reload_ignores_edit_of_replaced_instance() {
    let mut env = TestEnv::with_prefix("reload-stale-edit").await;
    let hook = Arc::new(BlockingManifestHook::default());
    let plugins = Plugins::new();
    plugins.insert::<RegionHookRef>(hook.clone());
    let engine = env
        .create_engine_with_log_store_and_plugins(
            MitoConfig::default(),
            new_log_store().await,
            plugins,
        )
        .await;
    let region_id = RegionId::new(1, 1);
    let (table_dir, schema, _) = create_leased_region_on(&engine, region_id).await;
    put_range(&engine, region_id, &schema, 0, 3).await;
    flush_region(&engine, region_id, None).await;
    let old = engine.get_region(region_id).unwrap();
    let flushed = old
        .manifest_ctx
        .manifest()
        .await
        .files
        .values()
        .next()
        .unwrap()
        .clone();
    let file = |file_id| FileMeta {
        file_id,
        ..flushed.as_ref().clone()
    };
    let added = file(FileId::random());

    hook.armed.store(true, Ordering::Relaxed);
    let committed = {
        let engine = engine.clone();
        let edit = edit_files(vec![added.clone()], vec![]);
        tokio::spawn(async move { engine.edit_region(region_id, edit).await })
    };
    hook.entered.notified().await;
    let (sender, queued) = oneshot::channel();
    let edit = edit_files(vec![file(FileId::random())], vec![]);
    let request = WorkerRequest::EditRegion(RegionEditRequest::new(region_id, edit, true, sender));
    engine
        .inner
        .workers
        .submit_to_worker(region_id, request)
        .await
        .unwrap();
    // The worker queues the edit before it handles the flush request sent after it.
    let flush = RegionRequest::Flush(RegionFlushRequest::default());
    engine.handle_request(region_id, flush).await.unwrap_err();
    let region_dir = region_dir_from_table_dir(&table_dir, region_id, PathType::Bare);
    let version = next_manifest_version(&old).await;
    let removal = RegionMetaActionList::with_action(RegionMetaAction::Edit(edit_files(
        vec![],
        vec![added.clone()],
    )));
    let store = env.get_object_store().unwrap();
    publish_delta(&store, &region_dir, version, encode_delta(removal).await).await;
    fence(&old).await;
    notify_fenced(&engine, region_id).await;
    wait_until(|| is_replaced(&engine, &old)).await;
    // The swap rejects it as a close does, not as an edit sent to a Follower.
    let error = queued.await.unwrap().unwrap_err();
    assert_eq!(StatusCode::RegionNotFound, error.status_code());

    hook.release.notify_one();
    committed.await.unwrap().unwrap_err();
    let region = engine.get_region(region_id).unwrap();
    let ssts = region.version().ssts.clone();
    assert!(
        !ssts
            .levels()
            .iter()
            .any(|level| level.files.contains_key(&added.file_id))
    );
}

#[tokio::test]
async fn test_reload_ignores_leases_sent_before_swap() {
    let mut env = TestEnv::with_prefix("reload-lease-fence").await;
    let listener = Arc::new(ReloadSwapBlocker::default());
    let engine = env
        .create_engine_with_log_store_and_time(
            MitoConfig::default(),
            new_log_store().await,
            Arc::new(StdTimeProvider),
            Some(listener.clone()),
        )
        .await;
    let region_id = RegionId::new(1, 1);
    let (_, schema, deadline) = create_leased_region_on(&engine, region_id).await;
    put_range(&engine, region_id, &schema, 0, 3).await;
    let old = engine.get_region(region_id).unwrap();
    fence(&old).await;
    notify_fenced(&engine, region_id).await;

    // The reloaded instance exists and waits for the swap.
    listener.entered.notified().await;
    let sent_before_swap = Instant::now();
    listener.release.notify_one();
    wait_until(|| is_replaced(&engine, &old)).await;

    let lease = |sent_at| {
        engine.set_region_role_with_lease(region_id, RegionRole::Leader, sent_at, deadline)
    };
    assert!(!lease(sent_before_swap).unwrap());
    assert!(engine.get_region(region_id).unwrap().is_follower());
    assert!(lease(Instant::now()).unwrap());
    assert!(engine.get_region(region_id).unwrap().is_writable());
}

#[rstest]
#[case::close(false)]
#[case::reopen(true)]
#[tokio::test]
async fn test_region_change_during_reload_drops_reloaded_instance(#[case] reopen: bool) {
    let mut env = TestEnv::with_prefix("region-change-during-reload").await;
    let (engine, log_store) = env
        .create_engine_with_object_store_wal(MitoConfig::default(), &held_wal_config())
        .await;
    let region_id = RegionId::new(1, 1);
    let (table_dir, schema, _) = create_leased_region_on(&engine, region_id).await;
    put_range(&engine, region_id, &schema, 0, 3).await;
    fence(&engine.get_region(region_id).unwrap()).await;
    notify_fenced(&engine, region_id).await;
    wait_until(|| engine.is_region_catching_up(region_id)).await;

    close_region(&engine, region_id).await;
    let reopened = if reopen {
        let open = RegionRequest::Open(open_request(table_dir));
        engine.handle_request(region_id, open).await.unwrap();
        engine.get_region(region_id)
    } else {
        None
    };
    log_store.seal_open_batch().await.unwrap();
    wait_until(|| !engine.is_region_catching_up(region_id)).await;

    let registered = engine.get_region(region_id);
    match (reopened, registered) {
        (None, None) => {}
        (Some(reopened), Some(registered)) => assert!(Arc::ptr_eq(&reopened, &registered)),
        (reopened, registered) => panic!("reopened {reopened:?}, registered {registered:?}"),
    }
}

#[tokio::test]
async fn test_failed_reload_waits_for_backstop() {
    let region_id = RegionId::new(1, 1);
    // The region is created at version 0, so the conflicting flush commits version 1.
    let delta_name = format!("{}.gz", delta_file(1));
    let delta_reads = Arc::new(AtomicUsize::new(0));
    let layer = MockLayerBuilder::default()
        .reader_factory(Arc::new({
            let delta_reads = delta_reads.clone();
            move |path, _args, inner| {
                if path.ends_with(&delta_name) {
                    delta_reads.fetch_add(1, Ordering::Relaxed);
                }
                inner
            }
        }))
        .build()
        .unwrap();
    let mut env = TestEnv::with_prefix("failed-reload")
        .await
        .with_mock_layer(layer);
    let time_provider = Arc::new(MockTimeProvider::new(current_time_millis()));
    let engine = env
        .create_engine_with_log_store_and_time(
            MitoConfig::default(),
            new_log_store().await,
            time_provider.clone(),
            None,
        )
        .await;
    let (table_dir, schema, _) = create_leased_region_on(&engine, region_id).await;
    put_range(&engine, region_id, &schema, 0, 3).await;
    let old = engine.get_region(region_id).unwrap();
    let store = env.get_object_store().unwrap();
    let region_dir = region_dir_from_table_dir(&table_dir, region_id, PathType::Bare);
    publish_delta(&store, &region_dir, 1, b"not a delta".to_vec()).await;
    let flush = RegionRequest::Flush(RegionFlushRequest::default());
    engine.handle_request(region_id, flush).await.unwrap_err();

    // The commit read the delta back, and the reload failed to decode it.
    wait_until(|| {
        delta_reads.load(Ordering::Relaxed) >= 2 && !engine.is_region_catching_up(region_id)
    })
    .await;
    let reads = delta_reads.load(Ordering::Relaxed);
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(reads, delta_reads.load(Ordering::Relaxed));
    assert!(!is_replaced(&engine, &old));
    assert!(old.manifest_ctx.is_reload_pending());

    publish_delta(&store, &region_dir, 1, encode_delta(nop_action()).await).await;
    let interval = CHECK_REGION_INTERVAL.as_millis() as i64;
    time_provider.set_elapsed(interval);
    wait_until(|| is_replaced(&engine, &old)).await;
    assert_eq!(3, count_rows(&engine, region_id).await);
}

#[tokio::test]
async fn test_end_of_staging_conflict_reloads_region() {
    let mut env = TestEnv::with_prefix("staging-conflict").await;
    let region_id = RegionId::new(1, 1);
    let (engine, _log_store, table_dir, schema, _) =
        create_leased_region(&mut env, region_id).await;
    put_range(&engine, region_id, &schema, 0, 3).await;
    flush_region(&engine, region_id, None).await;
    let old = engine.get_region(region_id).unwrap();
    let file = old
        .manifest_ctx
        .manifest()
        .await
        .files
        .values()
        .next()
        .unwrap()
        .clone();
    engine
        .handle_request(region_id, enter_staging_request())
        .await
        .unwrap();
    engine
        .edit_region(region_id, edit_files(vec![file.as_ref().clone()], vec![]))
        .await
        .unwrap();
    let region_dir = region_dir_from_table_dir(&table_dir, region_id, PathType::Bare);
    let version = next_manifest_version(&old).await;
    let store = env.get_object_store().unwrap();
    publish_delta(
        &store,
        &region_dir,
        version,
        encode_delta(nop_action()).await,
    )
    .await;

    let response = engine
        .set_region_role_state_gracefully(region_id, SettableRegionRoleState::Leader)
        .await
        .unwrap();

    assert!(response.is_invalid_transition(), "{response:?}");
    wait_until(|| is_replaced(&engine, &old)).await;
}
