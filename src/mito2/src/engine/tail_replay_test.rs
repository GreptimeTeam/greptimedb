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

//! Tests for the background replay of pending WAL tails.

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use api::region::RegionResponse;
use api::v1::{Mutation, OpType, Rows, WalEntry};
use async_trait::async_trait;
use common_base::Plugins;
use common_base::readable_size::ReadableSize;
use common_error::ext::{BoxedError, ErrorExt};
use common_error::status_code::StatusCode;
use common_recordbatch::RecordBatches;
use common_wal::config::object_store::FIXED_DIRECTORY_GENERATION;
use common_wal::options::WAL_OPTIONS_KEY;
use datatypes::arrow::array::Array;
use log_store::ObjectStoreLogStore;
use log_store::object_store_wal::{NodeWalPrefix, ObjectStoreWalReaders};
use object_store::ObjectStore;
use object_store::layers::mock::{
    Buffer, Entry as ListEntry, Error as MockError, ErrorKind, List, Metadata, MockLayer,
    MockLayerBuilder, Result as MockResult, Write, Writer,
};
use object_store::services::Memory;
use rstest::rstest;
use store_api::logstore::LogStore;
use store_api::logstore::provider::Provider;
use store_api::region_engine::{
    RegionEngine, RegionManifestInfo, RegionRole, SettableRegionRoleState,
};
use store_api::region_request::{
    EnterStagingRequest, PathType, RegionAlterRequest, RegionBuildIndexRequest,
    RegionCatchupRequest, RegionCloseRequest, RegionDiscardPendingTailsRequest, RegionDropRequest,
    RegionFlushRequest, RegionOpenRequest, RegionRequest, RegionTruncateRequest,
    StagingPartitionDirective,
};
use store_api::storage::{RegionId, ScanRequest};

use crate::config::MitoConfig;
use crate::engine::MitoEngine;
use crate::engine::flush_test::MockRegionHook;
use crate::engine::index_build_test::async_build_mode_config;
use crate::engine::listener::EventListener;
use crate::engine::object_store_wal_test::{
    count_rows, count_ssts, create_leased_region_on, create_region, encode_delta, is_replaced,
    next_manifest_version, publish_delta, put_range, stored_entry_ids, wait_until, wal_config,
    wal_options,
};
use crate::engine::pending_tail_test::{
    add_field, commit_edit, pending_tails_edit, set_pending_tails,
};
use crate::engine::region_hook::RegionHookRef;
use crate::error::Error;
use crate::manifest::action::{
    PendingTail, RegionEdit, RegionMetaAction, RegionMetaActionList, UptoKind,
};
use crate::manifest::storage::delta_file;
use crate::region::version::VersionRef;
use crate::region::{MitoRegion, RegionLeaderState, RegionRoleState};
use crate::request::{BackgroundNotify, WorkerRequest};
use crate::sst::file::RegionFileId;
use crate::sst::location::region_dir_from_table_dir;
use crate::test_util::{
    CreateRequestBuilder, DeltaFaults, Gate, TestEnv, build_rows, build_rows_for_key, flush_region,
    nop_action, put_rows, reopen_region, rows_schema,
};
use crate::time_provider::StdTimeProvider;
use crate::wal::{EntryId, Wal};

/// The node the engine's object store WAL runs as.
const NODE_ID: u64 = 0;
/// Another node sharing the WAL storage.
const REMOTE_NODE_ID: u64 = 1;

/// Records the tail replays of an engine, and holds their commits while asked to.
#[derive(Default)]
struct TailReplayListener {
    commits: AtomicUsize,
    /// Commit attempts numbered from it on wait until it changes; 0 holds none.
    held_from: AtomicUsize,
    /// Whether a committed replay waits before telling the worker.
    hold_commit_end: AtomicBool,
    commit_ends: AtomicUsize,
    discard_releases: AtomicUsize,
    failures: Mutex<Vec<(StatusCode, String)>>,
    ended: AtomicUsize,
    lease_checks: AtomicUsize,
    fence_begins: AtomicUsize,
    /// Index builds the worker learned stopped.
    index_builds_stopped: AtomicUsize,
    compactions_scheduled: AtomicUsize,
    leader_lease_wait_ends: AtomicUsize,
    became_writable: AtomicUsize,
    park_deferred_work: Gate,
    park_backfill: Gate,
}

#[async_trait]
impl EventListener for TailReplayListener {
    async fn on_tail_commit_begin(&self, _region_id: RegionId) {
        let attempt = self.commits.fetch_add(1, Ordering::Relaxed) + 1;
        while (1..=attempt).contains(&self.held_from.load(Ordering::Relaxed)) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    async fn on_tail_commit_end(&self, _region_id: RegionId) {
        self.commit_ends.fetch_add(1, Ordering::Relaxed);
        while self.hold_commit_end.load(Ordering::Relaxed) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    fn on_discarded_tails_wal_released(&self, _region_id: RegionId) {
        self.discard_releases.fetch_add(1, Ordering::Relaxed);
    }

    fn on_tail_lease_check(&self, _region_id: RegionId) {
        self.lease_checks.fetch_add(1, Ordering::Relaxed);
    }

    fn on_leader_lease_wait_ended(&self, _region_id: RegionId) {
        // Releases the index build counts recorded before it.
        self.leader_lease_wait_ends.fetch_add(1, Ordering::Release);
    }

    fn on_region_became_writable(&self, _region_id: RegionId) {
        self.became_writable.fetch_add(1, Ordering::Release);
    }

    async fn on_deferred_lease_work_begin(&self, _region_id: RegionId) {
        self.park_deferred_work.pass().await;
    }

    async fn on_persist_backfilled_metadata_lock_begin(&self, _region_id: RegionId) {
        self.park_backfill.pass().await;
    }

    fn on_tail_fence_begin(&self, _region_id: RegionId) {
        self.fence_begins.fetch_add(1, Ordering::Relaxed);
    }

    fn on_tail_replay_failed(&self, _region_id: RegionId, error: &Error) {
        let failure = (error.status_code(), format!("{error:?}"));
        self.failures.lock().unwrap().push(failure);
    }

    fn on_tail_replay_ended(&self, _region_id: RegionId) {
        // A test that sees the end also sees the counts of the callbacks before it.
        self.ended.fetch_add(1, Ordering::Release);
    }

    async fn on_index_build_stopped(&self, _region_file_id: RegionFileId) {
        self.index_builds_stopped.fetch_add(1, Ordering::Relaxed);
    }

    fn on_compaction_scheduled(&self, _region_id: RegionId) {
        self.compactions_scheduled.fetch_add(1, Ordering::Relaxed);
    }
}

impl TailReplayListener {
    fn hold_from(&self, attempt: usize) {
        self.held_from.store(attempt, Ordering::Relaxed);
    }

    fn hold_commit_end(&self, hold: bool) {
        self.hold_commit_end.store(hold, Ordering::Relaxed);
    }

    fn discard_releases(&self) -> usize {
        self.discard_releases.load(Ordering::Relaxed)
    }

    fn commit_ends(&self) -> usize {
        self.commit_ends.load(Ordering::Relaxed)
    }

    fn commits(&self) -> usize {
        self.commits.load(Ordering::Relaxed)
    }

    fn ended(&self) -> usize {
        self.ended.load(Ordering::Acquire)
    }

    fn failed(&self) -> bool {
        !self.failures.lock().unwrap().is_empty()
    }

    fn failed_with(&self, code: StatusCode) -> bool {
        let failures = self.failures.lock().unwrap();
        failures.iter().any(|(failed, _)| *failed == code)
    }

    /// Returns whether a failure's debug output names `cause`.
    fn failed_naming(&self, cause: &str) -> bool {
        let failures = self.failures.lock().unwrap();
        failures.iter().any(|(_, error)| error.contains(cause))
    }

    fn lease_checks(&self) -> usize {
        self.lease_checks.load(Ordering::Relaxed)
    }

    fn index_builds_stopped(&self) -> usize {
        self.index_builds_stopped.load(Ordering::Relaxed)
    }

    fn fence_begins(&self) -> usize {
        self.fence_begins.load(Ordering::Relaxed)
    }

    fn compactions_scheduled(&self) -> usize {
        self.compactions_scheduled.load(Ordering::Relaxed)
    }
}

struct TestRegion {
    env: TestEnv,
    /// The storage of the WAL of every node.
    wal_store: ObjectStore,
    log_store: Arc<ObjectStoreLogStore>,
    engine: MitoEngine,
    listener: Arc<TailReplayListener>,
    region_id: RegionId,
    table_dir: String,
    /// The WAL entries holding rows `[0, 3)` and `[3, 6)`.
    entries: [EntryId; 2],
}

impl TestRegion {
    /// Creates a region on the object store WAL of node [NODE_ID], with leases when
    /// `leased`, and writes rows `[0, 3)` and `[3, 6)` in two WAL entries.
    async fn new(prefix: &str, leased: bool, mock_layer: Option<MockLayer>) -> Self {
        Self::build(prefix, leased, mock_layer, None, MitoConfig::default()).await
    }

    /// As [Self::new], with `wal_layer` over the WAL storage of every node.
    async fn with_wal_layer(
        prefix: &str,
        leased: bool,
        mock_layer: Option<MockLayer>,
        wal_layer: Option<MockLayer>,
    ) -> Self {
        Self::build(prefix, leased, mock_layer, wal_layer, MitoConfig::default()).await
    }

    /// As [Self::new] without leases, with an engine of `config`.
    async fn with_config(prefix: &str, mock_layer: Option<MockLayer>, config: MitoConfig) -> Self {
        Self::build(prefix, false, mock_layer, None, config).await
    }

    async fn build(
        prefix: &str,
        leased: bool,
        mock_layer: Option<MockLayer>,
        wal_layer: Option<MockLayer>,
        config: MitoConfig,
    ) -> Self {
        let mut env = TestEnv::with_prefix(prefix).await.with_node_id(NODE_ID);
        if let Some(mock_layer) = mock_layer {
            env = env.with_mock_layer(mock_layer);
        }
        let mut wal_store = ObjectStore::new(Memory::default()).unwrap();
        if let Some(wal_layer) = wal_layer {
            wal_store = wal_store.layer(wal_layer);
        }
        let log_store = ObjectStoreLogStore::try_new(
            wal_store.clone(),
            &wal_config(),
            NODE_ID,
            FIXED_DIRECTORY_GENERATION,
        )
        .await
        .unwrap();
        let listener = Arc::new(TailReplayListener::default());
        let engine = env
            .create_engine_with_log_store_and_time(
                config,
                log_store.clone(),
                Arc::new(StdTimeProvider),
                Some(listener.clone()),
            )
            .await;
        let region_id = RegionId::new(1, 1);
        let (table_dir, schema) = if leased {
            let (table_dir, schema, _) = create_leased_region_on(&engine, region_id).await;
            (table_dir, schema)
        } else {
            create_region(&engine, region_id).await
        };
        let mut entries = [0; 2];
        for (i, entry) in entries.iter_mut().enumerate() {
            put_range(&engine, region_id, &schema, i * 3, i * 3 + 3).await;
            *entry = engine
                .get_region(region_id)
                .unwrap()
                .version_control
                .current()
                .last_entry_id;
        }
        Self {
            env,
            wal_store,
            log_store,
            engine,
            listener,
            region_id,
            table_dir,
            entries,
        }
    }

    /// Records `tails` and reopens the region as a Follower, which replays none of them.
    async fn reopen_with_tails(&self, tails: Vec<PendingTail>) {
        self.reopen_with_edit(pending_tails_edit(tails)).await;
    }

    /// Commits `edit`, which records tails, and reopens the region as a Follower.
    async fn reopen_with_edit(&self, edit: RegionEdit) {
        commit_edit(&self.engine, self.region_id, edit).await;
        self.reopen().await;
    }

    /// Reopens the region as a Follower.
    async fn reopen(&self) {
        reopen_region(
            &self.engine,
            self.region_id,
            self.table_dir.clone(),
            false,
            wal_options(),
        )
        .await;
    }

    /// Writes rows `[6, 9)` to the WAL of [REMOTE_NODE_ID] and reopens the region with a tail of
    /// this node over rows `[0, 6)` and a tail of [REMOTE_NODE_ID] over the new rows. Returns the
    /// two tails.
    async fn reopen_with_own_and_remote_tails(&self) -> (PendingTail, PendingTail) {
        let remote_upto = self.write_remote_rows(self.region_id, 6, 9).await;
        let own = own_tail(0, self.entries[1], UptoKind::Reported);
        let remote = remote_tail(0, remote_upto, UptoKind::Reported);
        self.reopen_with_tails(vec![own.clone(), remote.clone()])
            .await;
        (own, remote)
    }

    fn promote(&self) {
        self.engine
            .set_region_role(self.region_id, RegionRole::Leader)
            .unwrap();
    }

    fn promote_with_lease(&self) {
        let now = Instant::now();
        self.engine
            .set_region_role_with_lease(
                self.region_id,
                RegionRole::Leader,
                now,
                now + Duration::from_secs(10),
                None,
            )
            .unwrap();
    }

    /// Asks the worker to replay the tails, as a role change that makes the region writable does.
    async fn start_replay(&self) {
        let request = WorkerRequest::Background {
            region_id: self.region_id,
            notify: BackgroundNotify::RegionBecameWritable,
        };
        self.engine
            .inner
            .workers
            .submit_to_worker(self.region_id, request)
            .await
            .unwrap();
    }

    /// Reopens the follower region as a new instance, through a catchup that finds a manifest
    /// version another writer published.
    async fn replace_follower(&self) {
        let mito_region = self.engine.get_region(self.region_id).unwrap();
        let store = self.env.get_object_store().unwrap();
        let version = next_manifest_version(&mito_region).await;
        publish_delta(
            &store,
            &self.region_dir(),
            version,
            encode_delta(nop_action()).await,
        )
        .await;
        let catchup = RegionRequest::Catchup(RegionCatchupRequest::default());
        self.engine
            .handle_request(self.region_id, catchup)
            .await
            .unwrap();
        assert!(is_replaced(&self.engine, &mito_region));
    }

    async fn catchup(&self) {
        let catchup = RegionRequest::Catchup(RegionCatchupRequest {
            set_writable: true,
            ..Default::default()
        });
        self.engine
            .handle_request(self.region_id, catchup)
            .await
            .unwrap();
    }

    /// Writes rows `[start, end)` of `region_id` to the WAL of [REMOTE_NODE_ID] in one entry
    /// with the sequence 100, and returns the entry id.
    async fn write_remote_rows(&self, region_id: RegionId, start: usize, end: usize) -> EntryId {
        let store = ObjectStoreLogStore::try_new(
            self.wal_store.clone(),
            &wal_config(),
            REMOTE_NODE_ID,
            FIXED_DIRECTORY_GENERATION,
        )
        .await
        .unwrap();
        let provider = Provider::object_store_provider(region_id, remote_prefix());
        let request = CreateRequestBuilder::new().build();
        let entry = WalEntry {
            mutations: vec![Mutation {
                op_type: OpType::Put as i32,
                sequence: 100,
                rows: Some(Rows {
                    schema: rows_schema(&request),
                    rows: build_rows(start, end),
                }),
                write_hint: None,
            }],
            bulk_entries: vec![],
        };
        let mut writer = Wal::new(store.clone()).writer();
        writer.add_entry(region_id, 0, &entry, &provider).unwrap();
        let response = writer.write_to_wal().await.unwrap();
        store.stop().await.unwrap();
        response.last_entry_ids[&region_id]
    }

    /// Reopens the region with a tail in the WAL of [REMOTE_NODE_ID], deletes that WAL and
    /// promotes the region, so its replay keeps failing.
    async fn start_failing_remote_replay(&self) {
        let last = self.write_remote_rows(self.region_id, 6, 9).await;
        self.reopen_with_tails(vec![remote_tail(0, last, UptoKind::Reported)])
            .await;
        self.delete_remote_objects().await;
        self.catchup().await;
    }

    /// Deletes every object of the WAL of [REMOTE_NODE_ID], so reading it fails.
    async fn delete_remote_objects(&self) {
        let objects = self
            .wal_store
            .list(&format!("{}/objects/", remote_prefix()))
            .await
            .unwrap();
        for object in objects {
            self.wal_store.delete(object.path()).await.unwrap();
        }
    }

    /// Returns the bound a takeover would record for the WAL of [REMOTE_NODE_ID].
    async fn remote_listed_bound(&self) -> EntryId {
        let readers = ObjectStoreWalReaders::new(self.wal_store.clone());
        let prefix = NodeWalPrefix::try_new(&remote_prefix()).unwrap();
        readers
            .reader(&prefix)
            .unwrap()
            .list_upper_bound(self.region_id)
            .await
            .unwrap()
    }

    /// Writes 99 to the row of key "6", which [Self::write_remote_rows] also writes.
    async fn overwrite_remote_row(&self) {
        let rows = Rows {
            schema: rows_schema(&CreateRequestBuilder::new().build()),
            rows: build_rows_for_key("6", 6, 7, 99),
        };
        put_rows(&self.engine, self.region_id, rows).await;
    }

    /// Returns whether a scan sees the value [Self::overwrite_remote_row] wrote.
    async fn sees_overwrite(&self) -> bool {
        let request = ScanRequest::default();
        let stream = self.engine.scan_to_stream(self.region_id, request).await;
        let batches = RecordBatches::try_collect(stream.unwrap()).await.unwrap();
        batches.pretty_print().unwrap().contains("99.0")
    }

    async fn discard_pending_tails(&self, node_id: u64) -> Result<usize, BoxedError> {
        let request =
            RegionRequest::DiscardPendingTails(RegionDiscardPendingTailsRequest { node_id });
        let response = self.engine.handle_request(self.region_id, request).await?;
        Ok(response.affected_rows)
    }

    /// Flushes this node's rows, records a listed tail of [REMOTE_NODE_ID] whose bound lies
    /// above every entry of both nodes, loses that node's WAL and makes the region writable, so
    /// the replay keeps failing. Returns the bound.
    async fn start_lost_listed_tail(&self) -> EntryId {
        // Rows of the region stay readable after a reopen from a flushed entry id at the bound.
        flush_region(&self.engine, self.region_id, None).await;
        self.write_remote_rows(self.region_id, 6, 9).await;
        // A listed bound may name no entry. It lies many objects above every entry, so later
        // writes of this node reach it only once its sequence floor is raised past it.
        let upto = self.remote_listed_bound().await.max(self.entries[1]) + (1 << 30);
        self.reopen_with_tails(vec![remote_tail(0, upto, UptoKind::Listed)])
            .await;
        self.delete_remote_objects().await;
        self.catchup().await;
        wait_until(|| self.listener.failed()).await;
        upto
    }

    /// Stops the engine and its log store as a crash of the process would, then opens the
    /// region on a new engine over a new log store on the same storage.
    async fn crash_and_open(self) -> Result<RegionResponse, BoxedError> {
        let TestRegion {
            mut env,
            wal_store,
            log_store,
            engine,
            region_id,
            table_dir,
            ..
        } = self;
        log_store.crash().await;
        engine.stop().await.unwrap();
        drop(engine);
        let released = Arc::downgrade(&log_store);
        drop(log_store);
        wait_until(|| released.strong_count() == 0).await;
        let log_store = ObjectStoreLogStore::try_new(
            wal_store,
            &wal_config(),
            NODE_ID,
            FIXED_DIRECTORY_GENERATION,
        )
        .await
        .unwrap();
        let engine = env
            .create_engine_with_log_store_and_time(
                MitoConfig::default(),
                log_store,
                Arc::new(StdTimeProvider),
                None,
            )
            .await;
        let request = RegionRequest::Open(RegionOpenRequest {
            engine: String::new(),
            table_dir,
            options: wal_options(),
            skip_wal_replay: false,
            path_type: PathType::Bare,
            checkpoint: None,
            requirements: Default::default(),
        });
        engine.handle_request(region_id, request).await
    }

    fn version(&self) -> VersionRef {
        self.engine.get_region(self.region_id).unwrap().version()
    }

    fn has_pending_tails(&self) -> bool {
        self.version().has_pending_tails()
    }

    async fn wait_until_replayed(&self) {
        wait_until(|| !self.has_pending_tails()).await;
    }

    fn region_dir(&self) -> String {
        region_dir_from_table_dir(&self.table_dir, self.region_id, PathType::Bare)
    }

    async fn stored_ssts(&self) -> usize {
        count_ssts(&self.env.get_object_store().unwrap(), &self.region_dir()).await
    }

    async fn stored_sst_names(&self) -> Vec<String> {
        let store = self.env.get_object_store().unwrap();
        let entries = store.list(&self.region_dir()).await.unwrap();
        entries
            .iter()
            .map(|entry| entry.name().to_string())
            .filter(|name| name.ends_with(".parquet"))
            .collect()
    }
}

fn own_tail(after: EntryId, upto: EntryId, upto_kind: UptoKind) -> PendingTail {
    PendingTail {
        node_id: NODE_ID,
        after,
        upto,
        upto_kind,
    }
}

fn remote_tail(after: EntryId, upto: EntryId, upto_kind: UptoKind) -> PendingTail {
    PendingTail {
        node_id: REMOTE_NODE_ID,
        ..own_tail(after, upto, upto_kind)
    }
}

fn remote_prefix() -> String {
    wal_config().node_prefix(REMOTE_NODE_ID, FIXED_DIRECTORY_GENERATION)
}

#[tokio::test]
async fn test_replays_own_tail_after_reopen() {
    let region = TestRegion::new("tail-replay-own", false, None).await;
    region
        .reopen_with_tails(vec![own_tail(0, region.entries[1], UptoKind::Reported)])
        .await;
    let version = region.version();
    assert_eq!(0, version.memtables.num_rows());

    region.promote();
    region.wait_until_replayed().await;

    let version = region.version();
    assert_eq!(1, version.ssts.levels()[0].files.len());
    assert_eq!(region.entries[1], version.flushed_entry_id);
    assert_eq!(6, count_rows(&region.engine, region.region_id).await);
    // The commit released the replayed WAL.
    assert!(
        stored_entry_ids(&region.log_store, region.region_id)
            .await
            .is_empty()
    );
}

#[tokio::test]
async fn test_tail_commit_advances_flushed_sequence() {
    let region = TestRegion::new("tail-replay-sequence", false, None).await;
    region
        .reopen_with_tails(vec![own_tail(0, region.entries[1], UptoKind::Reported)])
        .await;

    region.promote();
    region.wait_until_replayed().await;

    let version = region.version();
    assert_eq!(6, version.flushed_sequence);
}

#[tokio::test]
async fn test_each_tail_has_its_own_ssts() {
    let region = TestRegion::new("tail-replay-two-tails", false, None).await;
    let [first, second] = region.entries;
    region
        .reopen_with_tails(vec![
            own_tail(0, first, UptoKind::Reported),
            own_tail(first, second, UptoKind::Reported),
        ])
        .await;

    region.promote();
    region.wait_until_replayed().await;

    let version = region.version();
    let files = &version.ssts.levels()[0].files;
    assert_eq!(2, files.len());
    let mut num_rows = files
        .values()
        .map(|file| file.meta_ref().num_rows)
        .collect::<Vec<_>>();
    num_rows.sort();
    assert_eq!(vec![3, 3], num_rows);
}

#[tokio::test]
async fn test_own_listed_tail_is_cleared_without_lease() {
    let region = TestRegion::new("tail-replay-own-listed", false, None).await;
    let upto = region.entries[1] + 1000;
    region
        .reopen_with_tails(vec![own_tail(0, upto, UptoKind::Listed)])
        .await;
    let mito_region = region.engine.get_region(region.region_id).unwrap();
    assert!(
        mito_region
            .manifest_ctx
            .promote_after_catchup(region.region_id)
    );

    region.start_replay().await;
    region.wait_until_replayed().await;

    let version = region.version();
    assert_eq!(region.entries[1], version.flushed_entry_id);
    assert_eq!(6, count_rows(&region.engine, region.region_id).await);
}

#[tokio::test]
async fn test_reported_tail_must_reach_its_upto() {
    let region = TestRegion::new("tail-replay-missing-upto", false, None).await;
    let upto = region.entries[1] + 1;
    region
        .reopen_with_tails(vec![own_tail(0, upto, UptoKind::Reported)])
        .await;

    region.promote();
    wait_until(|| region.listener.failed_with(StatusCode::StorageUnavailable)).await;

    assert!(region.has_pending_tails());
}

#[tokio::test]
async fn test_remote_tail_fails_on_log_store_reading_no_other_node() {
    let mut env = TestEnv::with_prefix("tail-replay-raft-engine")
        .await
        .with_node_id(NODE_ID);
    let listener = Arc::new(TailReplayListener::default());
    let engine = env
        .create_engine_with(MitoConfig::default(), None, Some(listener.clone()), None)
        .await;
    let region_id = RegionId::new(1, 1);
    let request = RegionRequest::Create(CreateRequestBuilder::new().build());
    engine.handle_request(region_id, request).await.unwrap();
    let tails = vec![remote_tail(0, 1, UptoKind::Listed)];
    set_pending_tails(&engine, region_id, tails.clone()).await;

    // The Raft engine log store cannot read another node's WAL.
    let notify = BackgroundNotify::RegionBecameWritable;
    let request = WorkerRequest::Background { region_id, notify };
    let workers = &engine.inner.workers;
    workers.submit_to_worker(region_id, request).await.unwrap();
    wait_until(|| listener.failed_with(StatusCode::Unsupported)).await;

    let region = engine.get_region(region_id).unwrap();
    assert_eq!(tails, region.version().pending_tails);
}

#[rstest]
#[case::reported(UptoKind::Reported)]
#[case::listed(UptoKind::Listed)]
#[tokio::test]
async fn test_tail_read_stops_at_its_upto(#[case] upto_kind: UptoKind) {
    let region = TestRegion::new("tail-replay-stop-at-upto", false, None).await;
    // The object store WAL keeps an entry's position in the low 20 bits, so the listed bound
    // is the last id of the first entry's object and names no entry.
    let upto = match upto_kind {
        UptoKind::Reported => region.entries[0],
        UptoKind::Listed => region.entries[0] | ((1 << 20) - 1),
    };
    region
        .reopen_with_tails(vec![own_tail(0, upto, upto_kind)])
        .await;
    // Makes the entry after the tail unreadable.
    let prefix = wal_config().node_prefix(NODE_ID, FIXED_DIRECTORY_GENERATION);
    let objects = region
        .wal_store
        .list(&format!("{prefix}/objects/"))
        .await
        .unwrap();
    let last = objects.iter().map(|entry| entry.path()).max().unwrap();
    region.wal_store.delete(last).await.unwrap();

    region.promote();
    region.wait_until_replayed().await;

    assert_eq!(region.entries[0], region.version().flushed_entry_id);
}

/// With write leases the replayed tail is published while the region awaits its Leader lease,
/// and the compaction its commit could not start starts when the lease ends the wait.
#[rstest]
#[case::without_write_lease(false)]
#[case::awaiting_leader_lease(true)]
#[tokio::test]
async fn test_catchup_replays_remote_reported_tail(#[case] leased: bool) {
    let region = TestRegion::new("tail-replay-remote-reported", leased, None).await;
    let last = region.write_remote_rows(region.region_id, 6, 9).await;
    region
        .reopen_with_tails(vec![remote_tail(0, last, UptoKind::Reported)])
        .await;
    // The catchup lets the region write until the node's deadline.
    if leased {
        region
            .engine
            .renew_node_lease(Instant::now() + Duration::from_secs(10));
    }

    region.catchup().await;
    region.wait_until_replayed().await;

    let mito_region = region.engine.get_region(region.region_id).unwrap();
    assert_eq!(leased, mito_region.manifest_ctx.awaits_leader_lease());
    assert_eq!(last, region.version().flushed_entry_id);
    assert_eq!(9, count_rows(&region.engine, region.region_id).await);
    if leased {
        // The worker reports the replay's end after it handled the commit and its compaction.
        wait_until(|| region.listener.ended() == 1).await;
        assert_eq!(0, region.listener.compactions_scheduled());
        region.promote_with_lease();
    }
    wait_until(|| region.listener.compactions_scheduled() == 1).await;
}

/// A tail replay that commits while the region awaits its Leader lease starts no checkpoint,
/// which could clean up versions the previous leader may still need.
#[tokio::test]
async fn test_tail_commit_awaiting_leader_lease_starts_no_checkpoint() {
    let config = MitoConfig {
        manifest_checkpoint_distance: 1,
        ..Default::default()
    };
    let region = TestRegion::build("tail-replay-no-checkpoint", true, None, None, config).await;
    let last = region.write_remote_rows(region.region_id, 6, 9).await;
    region
        .reopen_with_tails(vec![remote_tail(0, last, UptoKind::Reported)])
        .await;
    region
        .engine
        .renew_node_lease(Instant::now() + Duration::from_secs(10));
    let mito_region = region.engine.get_region(region.region_id).unwrap();
    let manager = mito_region.manifest_ctx.manifest_manager.read().await;
    let (version, checkpoint) = (
        manager.last_version(),
        manager.checkpointer().last_checkpoint_version(),
    );
    drop(manager);

    region.catchup().await;
    region.wait_until_replayed().await;

    let mito_region = region.engine.get_region(region.region_id).unwrap();
    assert!(mito_region.manifest_ctx.awaits_leader_lease());
    let manager = mito_region.manifest_ctx.manifest_manager.read().await;
    assert!(manager.last_version() > version);
    assert_eq!(checkpoint, manager.checkpointer().last_checkpoint_version());
    assert!(!manager.checkpointer().is_doing_checkpoint());
}

#[tokio::test]
async fn test_missing_reported_object_fails_until_restored() {
    let region = TestRegion::new("tail-replay-missing-object", false, None).await;
    let last = region.write_remote_rows(region.region_id, 6, 9).await;
    region
        .reopen_with_tails(vec![remote_tail(0, last, UptoKind::Reported)])
        .await;
    let objects = region
        .wal_store
        .list(&format!("{}/objects/", remote_prefix()))
        .await
        .unwrap();
    let path = objects
        .iter()
        .map(|entry| entry.path())
        .max()
        .unwrap()
        .to_string();
    let bytes = region.wal_store.read(&path).await.unwrap();
    region.wal_store.delete(&path).await.unwrap();

    region.catchup().await;
    wait_until(|| {
        region
            .listener
            .failed_naming("does not hold the history of region")
    })
    .await;
    assert!(region.has_pending_tails());
    region.wal_store.write(&path, bytes).await.unwrap();
    region.wait_until_replayed().await;

    assert_eq!(9, count_rows(&region.engine, region.region_id).await);
}

#[tokio::test]
async fn test_failed_round_deletes_chunks_of_earlier_tails() {
    let region = TestRegion::new("tail-replay-failed-round", false, None).await;
    let last = region.write_remote_rows(region.region_id, 6, 9).await;
    region
        .reopen_with_tails(vec![
            own_tail(0, region.entries[1], UptoKind::Reported),
            remote_tail(0, last, UptoKind::Reported),
        ])
        .await;
    let objects = region
        .wal_store
        .list(&format!("{}/objects/", remote_prefix()))
        .await
        .unwrap();
    let path = objects[objects.len() - 1].path().to_string();
    let bytes = region.wal_store.read(&path).await.unwrap();
    region.wal_store.delete(&path).await.unwrap();

    region.catchup().await;
    // The round wrote the own tail's chunk before the remote read failed.
    wait_until(|| region.listener.failed()).await;
    assert_eq!(0, region.stored_ssts().await);
    region.wal_store.write(&path, bytes).await.unwrap();
    region.wait_until_replayed().await;

    assert_eq!(2, region.stored_ssts().await);
    assert_eq!(9, count_rows(&region.engine, region.region_id).await);
}

#[tokio::test]
async fn test_remote_listed_tail_shrinks_until_leader_lease() {
    let region = TestRegion::new("tail-replay-remote-listed", false, None).await;
    let last = region.write_remote_rows(region.region_id, 6, 9).await;
    let upto = region.remote_listed_bound().await;
    region
        .reopen_with_tails(vec![remote_tail(0, upto, UptoKind::Listed)])
        .await;

    region.catchup().await;
    let shrunk = vec![remote_tail(last, upto, UptoKind::Listed)];
    wait_until(|| region.version().pending_tails == shrunk).await;
    assert_eq!(last, region.version().flushed_entry_id);
    region.promote_with_lease();
    region.wait_until_replayed().await;

    assert_eq!(last, region.version().flushed_entry_id);
    assert_eq!(9, count_rows(&region.engine, region.region_id).await);
}

#[tokio::test]
async fn test_unread_remote_listed_tail_waits_for_leader_lease() {
    let region = TestRegion::new("tail-replay-remote-idle", false, None).await;
    region.write_remote_rows(RegionId::new(1, 2), 0, 1).await;
    let upto = region.remote_listed_bound().await;
    region
        .reopen_with_tails(vec![remote_tail(0, upto, UptoKind::Listed)])
        .await;
    let version = next_manifest_version(&region.engine.get_region(region.region_id).unwrap()).await;

    region.catchup().await;
    // The catchup reopens the region.
    let mito_region = region.engine.get_region(region.region_id).unwrap();
    // A second check means the empty read is done and the replay keeps waiting.
    wait_until(|| region.listener.lease_checks() >= 2).await;
    assert!(region.has_pending_tails());
    assert_eq!(version, next_manifest_version(&mito_region).await);
    // The replay waits for the lease instead of ending and starting again.
    assert_eq!(0, region.listener.ended());
    // Without write leases a StagingLeader lease is not the Leader lease the replay waits for.
    let now = Instant::now();
    region
        .engine
        .set_region_role_with_lease(
            region.region_id,
            RegionRole::StagingLeader,
            now,
            now + Duration::from_secs(10),
            None,
        )
        .unwrap();
    let checks = region.listener.lease_checks();
    wait_until(|| region.listener.lease_checks() >= checks + 2).await;
    assert!(region.has_pending_tails());
    assert_eq!(version, next_manifest_version(&mito_region).await);
    region.promote_with_lease();
    region.wait_until_replayed().await;

    assert_eq!(version + 1, next_manifest_version(&mito_region).await);
    assert_eq!(0, region.version().flushed_entry_id);
}

#[tokio::test]
async fn test_writes_after_replay_win_over_replayed_rows() {
    let region = TestRegion::new("tail-replay-later-writes", false, None).await;
    let last = region.write_remote_rows(region.region_id, 6, 9).await;
    region
        .reopen_with_tails(vec![remote_tail(0, last, UptoKind::Reported)])
        .await;
    region.catchup().await;
    region.wait_until_replayed().await;

    region.overwrite_remote_row().await;

    assert!(region.sees_overwrite().await);
}

#[tokio::test]
async fn test_writes_during_replay_win_over_replayed_rows() {
    let region = TestRegion::new("tail-replay-earlier-writes", false, None).await;
    let last = region.write_remote_rows(region.region_id, 6, 9).await;
    // Whatever records tails raises the committed sequence to theirs (100 to 102) and leaves
    // this node no unflushed entry at or below theirs: later writes win and stay in the WAL.
    flush_region(&region.engine, region.region_id, None).await;
    let edit = RegionEdit {
        committed_sequence: Some(102),
        ..pending_tails_edit(vec![remote_tail(0, last, UptoKind::Reported)])
    };
    region.reopen_with_edit(edit).await;
    region.listener.hold_from(1);
    region.catchup().await;
    wait_until(|| region.listener.commits() == 1).await;

    region.overwrite_remote_row().await;
    region.listener.hold_from(0);
    region.wait_until_replayed().await;

    assert!(region.sees_overwrite().await);
    region.reopen_with_tails(vec![]).await;
    assert_eq!(9, count_rows(&region.engine, region.region_id).await);
    assert!(region.sees_overwrite().await);
}

#[tokio::test]
async fn test_create_reopening_region_replays_tails() {
    let region = TestRegion::new("tail-replay-create", false, None).await;
    let tail = own_tail(0, region.entries[1], UptoKind::Reported);
    set_pending_tails(&region.engine, region.region_id, vec![tail]).await;
    let close = RegionRequest::Close(RegionCloseRequest::default());
    region
        .engine
        .handle_request(region.region_id, close)
        .await
        .unwrap();

    let request = CreateRequestBuilder::new()
        .insert_option(WAL_OPTIONS_KEY, &wal_options()[WAL_OPTIONS_KEY])
        .build();
    region
        .engine
        .handle_request(region.region_id, RegionRequest::Create(request))
        .await
        .unwrap();
    region.wait_until_replayed().await;

    assert_eq!(6, count_rows(&region.engine, region.region_id).await);
}

#[tokio::test]
async fn test_direct_promotion_counts_as_leader_lease() {
    let region = TestRegion::new("tail-replay-direct-promotion", false, None).await;
    let last = region.write_remote_rows(region.region_id, 6, 9).await;
    let upto = region.remote_listed_bound().await;
    region
        .reopen_with_tails(vec![remote_tail(0, upto, UptoKind::Listed)])
        .await;
    region.catchup().await;
    let shrunk = vec![remote_tail(last, upto, UptoKind::Listed)];
    wait_until(|| region.version().pending_tails == shrunk).await;
    // Setting a leader to Leader promotes nothing, so it is no lease.
    region.promote();
    let mito_region = region.engine.get_region(region.region_id).unwrap();
    assert!(!mito_region.manifest_ctx.leader_lease_received());

    region
        .engine
        .set_region_role(region.region_id, RegionRole::Follower)
        .unwrap();
    region.promote();
    region.wait_until_replayed().await;
}

#[tokio::test]
async fn test_leased_first_round_clears_remote_listed_tail() {
    let region = TestRegion::new("tail-replay-leased-listed", false, None).await;
    region.write_remote_rows(region.region_id, 6, 9).await;
    let upto = region.remote_listed_bound().await;
    region
        .reopen_with_tails(vec![remote_tail(0, upto, UptoKind::Listed)])
        .await;
    let mito_region = region.engine.get_region(region.region_id).unwrap();
    let version = next_manifest_version(&mito_region).await;

    // No catchup promoted this instance, so it counts as leased and no commit keeps part of
    // the tail.
    region.promote();
    region.wait_until_replayed().await;

    assert_eq!(version + 1, next_manifest_version(&mito_region).await);
    assert_eq!(9, count_rows(&region.engine, region.region_id).await);
}

#[tokio::test]
async fn test_replay_fills_columns_added_after_the_tail() {
    let region = TestRegion::new("tail-replay-alter", false, None).await;
    region
        .reopen_with_tails(vec![own_tail(0, region.entries[1], UptoKind::Reported)])
        .await;
    // Writable without asking the worker to replay, so the ALTER runs first.
    let mito_region = region.engine.get_region(region.region_id).unwrap();
    mito_region.set_role(RegionRole::Leader);
    let add_column = RegionRequest::Alter(RegionAlterRequest {
        kind: add_field(None),
    });
    region
        .engine
        .handle_request(region.region_id, add_column)
        .await
        .unwrap();

    region.start_replay().await;
    region.wait_until_replayed().await;

    let stream = region
        .engine
        .scan_to_stream(region.region_id, ScanRequest::default())
        .await
        .unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    let nulls = batches
        .iter()
        .map(|batch| batch.column_by_name("field_1").unwrap().null_count())
        .sum::<usize>();
    assert_eq!(6, nulls);
}

#[tokio::test]
async fn test_truncate_after_tail_commit_keeps_flushed_entry_id() {
    let region = TestRegion::new("tail-replay-truncate", false, None).await;
    region
        .reopen_with_tails(vec![own_tail(0, region.entries[1], UptoKind::Reported)])
        .await;
    region.promote();
    region.wait_until_replayed().await;

    region
        .engine
        .handle_request(
            region.region_id,
            RegionRequest::Truncate(RegionTruncateRequest::All),
        )
        .await
        .unwrap();

    let mito_region = region.engine.get_region(region.region_id).unwrap();
    let manifest = mito_region.manifest_ctx.manifest().await;
    assert_eq!(region.entries[1], manifest.flushed_entry_id);
}

#[tokio::test]
async fn test_tail_commit_conflict_reloads_region() {
    let region = TestRegion::new("tail-replay-conflict", true, None).await;
    region
        .reopen_with_tails(vec![own_tail(0, region.entries[1], UptoKind::Reported)])
        .await;
    let old = region.engine.get_region(region.region_id).unwrap();
    publish_delta(
        &region.env.get_object_store().unwrap(),
        &region.region_dir(),
        next_manifest_version(&old).await,
        encode_delta(nop_action()).await,
    )
    .await;

    region.promote_with_lease();
    wait_until(|| is_replaced(&region.engine, &old)).await;

    assert_eq!(1, region.listener.ended());
    assert!(region.has_pending_tails());
    assert_eq!(1, region.stored_ssts().await);
}

/// Lets the first write of a manifest delta land, then fails it as if its response was lost.
fn lose_delta_response(version: Arc<AtomicU64>) -> MockLayer {
    let armed = Arc::new(AtomicBool::new(true));
    MockLayerBuilder::default()
        .writer_factory(Arc::new(move |path, _args, inner| {
            let target = delta_file(version.load(Ordering::Relaxed));
            if path.contains(&target) && armed.swap(false, Ordering::Relaxed) {
                Box::new(LostResponseWriter { inner })
            } else {
                inner
            }
        }))
        .build()
        .unwrap()
}

struct LostResponseWriter {
    inner: Writer,
}

impl Write for LostResponseWriter {
    async fn write(&mut self, bs: Buffer) -> MockResult<()> {
        self.inner.write(bs).await
    }

    async fn close(&mut self) -> MockResult<Metadata> {
        self.inner.close().await?;
        Err(MockError::new(ErrorKind::Unexpected, "response lost"))
    }

    async fn abort(&mut self) -> MockResult<()> {
        self.inner.abort().await
    }
}

#[tokio::test]
async fn test_tail_commit_with_unknown_result_is_retried() {
    let version = Arc::new(AtomicU64::new(u64::MAX));
    let region = TestRegion::new(
        "tail-replay-unknown-commit",
        true,
        Some(lose_delta_response(version.clone())),
    )
    .await;
    region
        .reopen_with_tails(vec![own_tail(0, region.entries[1], UptoKind::Reported)])
        .await;
    let mito_region = region.engine.get_region(region.region_id).unwrap();
    version.store(next_manifest_version(&mito_region).await, Ordering::Relaxed);

    region.promote_with_lease();
    region.wait_until_replayed().await;

    assert_eq!(2, region.listener.commits());
    assert_eq!(1, region.stored_ssts().await);
    assert_eq!(6, count_rows(&region.engine, region.region_id).await);
}

#[tokio::test]
async fn test_uncertain_commit_is_retried_after_expired_lease() {
    let version = Arc::new(AtomicU64::new(u64::MAX));
    let region = TestRegion::new(
        "tail-replay-uncertain-expired",
        true,
        Some(lose_delta_response(version.clone())),
    )
    .await;
    region
        .reopen_with_tails(vec![own_tail(0, region.entries[1], UptoKind::Reported)])
        .await;
    let mito_region = region.engine.get_region(region.region_id).unwrap();
    version.store(next_manifest_version(&mito_region).await, Ordering::Relaxed);
    region.listener.hold_from(2);
    region.promote_with_lease();
    // The first attempt lands but reports an error.
    wait_until(|| region.listener.commits() == 2).await;
    let expired = Instant::now() + Duration::from_secs(60);
    region.engine.freeze_lease_clock(expired);
    region.listener.hold_from(3);

    wait_until(|| region.listener.commits() == 3).await;
    region.engine.freeze_lease_clock(Instant::now());
    region.listener.hold_from(0);
    region.wait_until_replayed().await;

    assert!(!region.listener.failed());
    assert_eq!(1, region.stored_ssts().await);
    assert!(Arc::ptr_eq(
        &mito_region,
        &region.engine.get_region(region.region_id).unwrap()
    ));
}

#[tokio::test]
async fn test_closed_replay_keeps_chunks_of_uncertain_commit() {
    let version = Arc::new(AtomicU64::new(u64::MAX));
    let region = TestRegion::new(
        "tail-replay-uncertain-closed",
        true,
        Some(lose_delta_response(version.clone())),
    )
    .await;
    region
        .reopen_with_tails(vec![own_tail(0, region.entries[1], UptoKind::Reported)])
        .await;
    let mito_region = region.engine.get_region(region.region_id).unwrap();
    version.store(next_manifest_version(&mito_region).await, Ordering::Relaxed);
    region.listener.hold_from(2);
    region.promote_with_lease();
    // The first attempt lands but reports an error, and the region closes before a retry.
    wait_until(|| region.listener.commits() == 2).await;
    let close = RegionRequest::Close(RegionCloseRequest::default());
    region
        .engine
        .handle_request(region.region_id, close)
        .await
        .unwrap();
    region.listener.hold_from(0);
    wait_until(|| region.listener.ended() == 1).await;

    assert_eq!(1, region.stored_ssts().await);
    region.reopen().await;
    assert!(!region.has_pending_tails());
    assert_eq!(6, count_rows(&region.engine, region.region_id).await);
}

#[tokio::test]
async fn test_tail_commit_after_deadline_deletes_chunks() {
    let region = TestRegion::new("tail-replay-deadline", true, None).await;
    region
        .reopen_with_tails(vec![own_tail(0, region.entries[1], UptoKind::Reported)])
        .await;
    region.listener.hold_from(1);
    region.promote_with_lease();
    wait_until(|| region.listener.commits() == 1).await;
    assert_eq!(1, region.stored_ssts().await);

    region
        .engine
        .freeze_lease_clock(Instant::now() + Duration::from_secs(60));
    region.listener.hold_from(0);
    wait_until(|| region.listener.failed_with(StatusCode::RegionNotReady)).await;
    region
        .engine
        .set_region_role(region.region_id, RegionRole::Follower)
        .unwrap();
    wait_until(|| region.listener.ended() == 1).await;

    assert!(region.has_pending_tails());
    assert_eq!(0, region.stored_ssts().await);
}

#[derive(Debug)]
enum Stop {
    Downgrade,
    Close,
    Drop,
}

#[rstest]
#[case::downgrade(Stop::Downgrade)]
#[case::close(Stop::Close)]
#[case::drop(Stop::Drop)]
#[tokio::test]
async fn test_stopping_region_cancels_replay(#[case] stop: Stop) {
    let region = TestRegion::new("tail-replay-cancel", false, None).await;
    region
        .reopen_with_tails(vec![own_tail(0, region.entries[1], UptoKind::Reported)])
        .await;
    region.listener.hold_from(1);
    region.promote();
    wait_until(|| region.listener.commits() == 1).await;

    match stop {
        Stop::Downgrade => {
            region
                .engine
                .set_region_role_state_gracefully(
                    region.region_id,
                    SettableRegionRoleState::Follower,
                )
                .await
                .unwrap();
        }
        Stop::Close => {
            region
                .engine
                .handle_request(
                    region.region_id,
                    RegionRequest::Close(RegionCloseRequest::default()),
                )
                .await
                .unwrap();
        }
        Stop::Drop => {
            region
                .engine
                .handle_request(
                    region.region_id,
                    RegionRequest::Drop(RegionDropRequest {
                        fast_path: false,
                        force: false,
                        partial_drop: false,
                    }),
                )
                .await
                .unwrap();
        }
    }
    region.listener.hold_from(0);
    wait_until(|| region.listener.ended() == 1).await;

    assert_eq!(1, region.listener.commits());
    assert_eq!(0, region.stored_ssts().await);
}

#[tokio::test]
async fn test_cancelled_replay_does_not_publish_after_promotion() {
    let region = TestRegion::new("tail-replay-cancel-promote", false, None).await;
    region
        .reopen_with_tails(vec![own_tail(0, region.entries[1], UptoKind::Reported)])
        .await;
    region.listener.hold_from(1);
    region.promote();
    wait_until(|| region.listener.commits() == 1).await;

    region
        .engine
        .set_region_role_state_gracefully(region.region_id, SettableRegionRoleState::Follower)
        .await
        .unwrap();
    region.promote();
    region.listener.hold_from(0);
    region.wait_until_replayed().await;

    assert_eq!(2, region.listener.commits());
}

/// A catchup of a writable region that awaits its Leader lease demotes it and cancels the tail
/// replay the first catchup started; the replay of the instance the catchup again replaced it
/// with commits instead.
#[tokio::test]
async fn test_catchup_again_cancels_replay_of_demoted_region() {
    let region = TestRegion::new("tail-replay-catchup-again", true, None).await;
    let last = region.write_remote_rows(region.region_id, 6, 9).await;
    // The catchups replay nothing into the memtable, which stays empty.
    flush_region(&region.engine, region.region_id, None).await;
    region
        .reopen_with_tails(vec![remote_tail(0, last, UptoKind::Reported)])
        .await;
    region
        .engine
        .renew_node_lease(Instant::now() + Duration::from_secs(10));
    region.listener.hold_from(1);
    region.catchup().await;
    wait_until(|| region.listener.commits() == 1).await;
    let first = region.engine.get_region(region.region_id).unwrap();
    assert!(first.manifest_ctx.awaits_leader_lease());

    region.catchup().await;

    let caught_up = region.engine.get_region(region.region_id).unwrap();
    assert!(!Arc::ptr_eq(&first, &caught_up));
    region.listener.hold_from(0);
    region.wait_until_replayed().await;
    // The cancelled replay committed nothing, so the replay that the catchup again started did.
    assert_eq!(2, region.listener.commits());
}

#[rstest]
#[case::demoted(true)]
#[case::tails_changed(false)]
#[tokio::test]
async fn test_tail_commit_checks_role_and_tails_under_lock(#[case] demote: bool) {
    let region = TestRegion::new("tail-replay-locked-check", false, None).await;
    region
        .reopen_with_tails(vec![own_tail(0, region.entries[1], UptoKind::Reported)])
        .await;
    region.listener.hold_from(1);
    region.promote();
    wait_until(|| region.listener.commits() == 1).await;

    if demote {
        // The replay waits for the manifest lock while the region is demoted: a read held
        // here queues its write, which then fails a further read.
        let mito_region = region.engine.get_region(region.region_id).unwrap();
        let manager = &mito_region.manifest_ctx.manifest_manager;
        let reading = manager.read().await;
        region.listener.hold_from(0);
        wait_until(|| manager.try_read().is_err()).await;
        region
            .engine
            .set_region_role(region.region_id, RegionRole::Follower)
            .unwrap();
        drop(reading);
    } else {
        set_pending_tails(&region.engine, region.region_id, vec![]).await;
        region.listener.hold_from(0);
    }
    wait_until(|| region.listener.ended() == 1).await;

    assert_eq!(1, region.listener.commits());
    assert_eq!(demote, region.has_pending_tails());
    assert_eq!(0, region.stored_ssts().await);
}

/// A commit notice that arrives after a sync rebuilt the version from a later manifest does
/// not bring back the tails that manifest cleared.
#[tokio::test]
async fn test_late_tail_commit_notice_keeps_synced_tails() {
    let region = TestRegion::new("tail-replay-late-notice", false, None).await;
    let replayed = vec![remote_tail(0, 10, UptoKind::Listed)];
    let shrunk = vec![remote_tail(5, 10, UptoKind::Listed)];
    // A sync expects a follower with empty memtables.
    flush_region(&region.engine, region.region_id, None).await;
    region.reopen_with_tails(shrunk.clone()).await;
    // A new leader clears the tails, and this node syncs its manifest.
    let mito_region = region.engine.get_region(region.region_id).unwrap();
    let version = next_manifest_version(&mito_region).await;
    let clear =
        RegionMetaActionList::with_action(RegionMetaAction::Edit(pending_tails_edit(vec![])));
    let store = region.env.get_object_store().unwrap();
    publish_delta(
        &store,
        &region.region_dir(),
        version,
        encode_delta(clear).await,
    )
    .await;
    let manifest_info = RegionManifestInfo::mito(version, 0, 0);
    region
        .engine
        .sync_region(region.region_id, manifest_info.into())
        .await
        .unwrap();
    assert!(!region.has_pending_tails());

    // The notice of the replay that left `shrunk` arrives after the sync.
    let notify = BackgroundNotify::TailReplayCommitted {
        region: mito_region,
        edit: pending_tails_edit(shrunk),
        tails: replayed,
    };
    let request = WorkerRequest::Background {
        region_id: region.region_id,
        notify,
    };
    let workers = &region.engine.inner.workers;
    workers
        .submit_to_worker(region.region_id, request)
        .await
        .unwrap();
    // The worker handles the notice before this request, which keeps the version.
    region
        .engine
        .set_region_role_state_gracefully(region.region_id, SettableRegionRoleState::Follower)
        .await
        .unwrap();

    assert!(!region.has_pending_tails());
}

/// Parks every listing of a path `is_target` accepts once `stall` is set, and counts them.
fn stall_listings(
    is_target: fn(&str) -> bool,
    stall: Arc<AtomicBool>,
    stalled: Arc<AtomicUsize>,
) -> MockLayer {
    MockLayerBuilder::default()
        .lister_factory(Arc::new(move |path, _args, inner| {
            if is_target(path) && stall.load(Ordering::Relaxed) {
                stalled.fetch_add(1, Ordering::Relaxed);
                Box::new(StalledLister)
            } else {
                inner
            }
        }))
        .build()
        .unwrap()
}

/// Whether `path` is in the WAL of [REMOTE_NODE_ID].
fn is_remote_wal(path: &str) -> bool {
    path.starts_with(&remote_prefix())
}

/// Whether `path` is in a manifest directory.
fn is_manifest(path: &str) -> bool {
    path.contains("/manifest/")
}

struct StalledLister;

impl List for StalledLister {
    async fn next(&mut self) -> MockResult<Option<ListEntry>> {
        std::future::pending().await
    }
}

#[rstest]
#[case::close(false)]
#[case::engine_stop(true)]
#[tokio::test]
async fn test_stalled_remote_read_ends_when_region_stops(#[case] stop_engine: bool) {
    let stall = Arc::new(AtomicBool::new(false));
    let stalled = Arc::new(AtomicUsize::new(0));
    let layer = stall_listings(is_remote_wal, stall.clone(), stalled.clone());
    let region =
        TestRegion::with_wal_layer("tail-replay-stalled-read", false, None, Some(layer)).await;
    let last = region.write_remote_rows(region.region_id, 6, 9).await;
    region
        .reopen_with_tails(vec![remote_tail(0, last, UptoKind::Reported)])
        .await;
    stall.store(true, Ordering::Relaxed);
    region.catchup().await;
    wait_until(|| stalled.load(Ordering::Relaxed) > 0).await;
    let replaying = Arc::downgrade(&region.engine.get_region(region.region_id).unwrap());

    if stop_engine {
        region.engine.stop().await.unwrap();
    } else {
        let close = RegionRequest::Close(RegionCloseRequest::default());
        region
            .engine
            .handle_request(region.region_id, close)
            .await
            .unwrap();
    }

    wait_until(|| replaying.strong_count() == 0).await;
}

#[tokio::test]
async fn test_replacing_region_cancels_its_replay() {
    let stall = Arc::new(AtomicBool::new(false));
    let stalled = Arc::new(AtomicUsize::new(0));
    let layer = stall_listings(is_remote_wal, stall.clone(), stalled.clone());
    let region = TestRegion::with_wal_layer("tail-replay-replaced", false, None, Some(layer)).await;
    let last = region.write_remote_rows(region.region_id, 6, 9).await;
    region
        .reopen_with_tails(vec![remote_tail(0, last, UptoKind::Reported)])
        .await;
    stall.store(true, Ordering::Relaxed);
    region.promote();
    wait_until(|| stalled.load(Ordering::Relaxed) > 0).await;
    let replaying = Arc::downgrade(&region.engine.get_region(region.region_id).unwrap());

    // Demotes without telling the worker, so only the replacement cancels the replay.
    region
        .engine
        .get_region(region.region_id)
        .unwrap()
        .set_role(RegionRole::Follower);
    region.replace_follower().await;

    wait_until(|| replaying.strong_count() == 0).await;
}

#[tokio::test]
async fn test_commit_notice_of_replaced_instance_changes_nothing() {
    let region = TestRegion::new("tail-replay-replaced-notice", false, None).await;
    let tails = vec![own_tail(0, region.entries[1], UptoKind::Reported)];
    region.reopen_with_tails(tails.clone()).await;
    let old = region.engine.get_region(region.region_id).unwrap();
    region.replace_follower().await;

    let notify = BackgroundNotify::TailReplayCommitted {
        region: old.clone(),
        edit: pending_tails_edit(vec![]),
        tails: tails.clone(),
    };
    let request = WorkerRequest::Background {
        region_id: region.region_id,
        notify,
    };
    let workers = &region.engine.inner.workers;
    workers
        .submit_to_worker(region.region_id, request)
        .await
        .unwrap();
    // The worker handles the notice before this request, which keeps the version.
    region
        .engine
        .set_region_role_state_gracefully(region.region_id, SettableRegionRoleState::Follower)
        .await
        .unwrap();

    assert_eq!(tails, old.version().pending_tails);
    assert_eq!(tails, region.version().pending_tails);
}

/// Moves a writable leader to `busy`, Altering or Editing.
fn set_busy(region: &MitoRegion, busy: RegionLeaderState) {
    match busy {
        RegionLeaderState::Altering => region.set_altering(),
        _ => region.set_editing(RegionLeaderState::Writable),
    }
    .unwrap();
}

#[rstest]
#[case::altering(RegionLeaderState::Altering)]
#[case::editing(RegionLeaderState::Editing)]
#[tokio::test]
async fn test_replay_backs_off_while_busy(#[case] busy: RegionLeaderState) {
    let region = TestRegion::new("tail-replay-busy", false, None).await;
    region
        .reopen_with_tails(vec![own_tail(0, region.entries[1], UptoKind::Reported)])
        .await;
    region.listener.hold_from(1);
    region.promote();
    wait_until(|| region.listener.commits() == 1).await;
    let chunks = region.stored_sst_names().await;
    let mito_region = region.engine.get_region(region.region_id).unwrap();
    set_busy(&mito_region, busy);

    region.listener.hold_from(0);
    wait_until(|| region.listener.commits() == 2).await;
    assert!(region.has_pending_tails());
    mito_region.switch_state_to_writable(busy);
    region.wait_until_replayed().await;

    assert_eq!(6, count_rows(&region.engine, region.region_id).await);
    assert!(!region.listener.failed());
    // The retries commit the chunks written before the region became busy.
    assert_eq!(chunks, region.stored_sst_names().await);
}

#[tokio::test]
async fn test_replay_round_keeps_no_flushed_memtables() {
    let region = TestRegion::new("tail-replay-memtables", false, None).await;
    region
        .reopen_with_tails(vec![own_tail(0, region.entries[1], UptoKind::Reported)])
        .await;
    region.listener.hold_from(1);
    region.promote();
    wait_until(|| region.listener.commits() == 1).await;
    let memtables = Arc::downgrade(&region.version().memtables);

    region.overwrite_remote_row().await;
    flush_region(&region.engine, region.region_id, None).await;
    wait_until(|| memtables.strong_count() == 0).await;
    region.listener.hold_from(0);
    region.wait_until_replayed().await;
}

#[rstest]
#[case::altering(RegionLeaderState::Altering)]
#[case::editing(RegionLeaderState::Editing)]
#[tokio::test]
async fn test_promotion_replays_tails_of_region_busy_meanwhile(#[case] busy: RegionLeaderState) {
    let region = TestRegion::new("tail-replay-promote-busy", false, None).await;
    region
        .reopen_with_tails(vec![own_tail(0, region.entries[1], UptoKind::Reported)])
        .await;
    // The worker starts an ALTER or an edit between the promotion and its check of the role.
    let mito_region = region.engine.get_region(region.region_id).unwrap();
    mito_region.set_role(RegionRole::Leader);
    set_busy(&mito_region, busy);
    region
        .engine
        .inner
        .notify_if_became_writable(&mito_region, Some(RegionRoleState::Follower));

    wait_until(|| region.listener.commits() > 0).await;
    mito_region.switch_state_to_writable(busy);
    region.wait_until_replayed().await;
}

#[tokio::test]
async fn test_triggers_during_replay_start_no_second_one() {
    let region = TestRegion::new("tail-replay-repeated-trigger", false, None).await;
    region
        .reopen_with_tails(vec![own_tail(0, region.entries[1], UptoKind::Reported)])
        .await;
    region.listener.hold_from(1);
    region.promote();
    wait_until(|| region.listener.commits() == 1).await;
    // Every replay task holds the region.
    let mito_region = region.engine.get_region(region.region_id).unwrap();
    let holders = Arc::strong_count(&mito_region);

    region.start_replay().await;
    region.start_replay().await;
    // The worker handles both triggers before this request, which leaves the region writable.
    region
        .engine
        .set_region_role_state_gracefully(region.region_id, SettableRegionRoleState::Leader)
        .await
        .unwrap();
    // The graceful role change holds the region until its task ends.
    wait_until(|| Arc::strong_count(&mito_region) == holders).await;

    region.listener.hold_from(0);
    region.wait_until_replayed().await;
    assert_eq!(1, region.listener.commits());
}

#[tokio::test]
async fn test_replay_chunks_fire_sst_written_hook() {
    let mut env = TestEnv::with_prefix("tail-replay-hook")
        .await
        .with_node_id(NODE_ID);
    let wal_store = ObjectStore::new(Memory::default()).unwrap();
    let log_store = ObjectStoreLogStore::try_new(
        wal_store,
        &wal_config(),
        NODE_ID,
        FIXED_DIRECTORY_GENERATION,
    )
    .await
    .unwrap();
    let hook = Arc::new(MockRegionHook::new());
    let plugins = Plugins::new();
    plugins.insert(hook.clone() as RegionHookRef);
    let engine = env
        .create_engine_with_log_store_and_plugins(MitoConfig::default(), log_store, plugins)
        .await;
    let region_id = RegionId::new(1, 1);
    let (table_dir, schema) = create_region(&engine, region_id).await;
    put_range(&engine, region_id, &schema, 0, 3).await;
    let region = engine.get_region(region_id).unwrap();
    let last = region.version_control.current().last_entry_id;
    let tails = vec![own_tail(0, last, UptoKind::Reported)];
    commit_edit(&engine, region_id, pending_tails_edit(tails)).await;
    reopen_region(&engine, region_id, table_dir, false, wal_options()).await;

    engine
        .set_region_role(region_id, RegionRole::Leader)
        .unwrap();
    let region = engine.get_region(region_id).unwrap();
    wait_until(|| !region.version().has_pending_tails()).await;

    // Nothing else wrote an SST: the region never flushed.
    assert_eq!(1, hook.sst_written_count.load(Ordering::Relaxed));
}

#[tokio::test]
async fn test_replay_writes_chunk_when_memory_budget_is_exhausted() {
    // The replay budget is 8 KiB: a 256 B floor and a 4 KiB pool, which a few hundred rows
    // exceed. The engine flushes at 32 KiB, above what the region itself writes.
    let config = MitoConfig {
        global_write_buffer_size: ReadableSize::kb(64),
        ..Default::default()
    };
    let region = TestRegion::with_config("tail-replay-chunk-budget", None, config).await;
    let first = region.write_remote_rows(region.region_id, 6, 300).await;
    let last = region.write_remote_rows(region.region_id, 300, 600).await;
    assert!(first < last);
    region
        .reopen_with_tails(vec![remote_tail(0, last, UptoKind::Reported)])
        .await;
    assert_eq!(0, region.stored_ssts().await);

    region.catchup().await;
    region.wait_until_replayed().await;

    // Each entry of the tail went to its own chunk.
    assert_eq!(2, region.version().ssts.levels()[0].files.len());
    assert_eq!(600, count_rows(&region.engine, region.region_id).await);
}

#[tokio::test]
async fn test_failed_replay_reloads_region_from_newer_manifest() {
    let region = TestRegion::new("tail-replay-failed-reload", false, None).await;
    region.start_failing_remote_replay().await;
    wait_until(|| region.listener.failed()).await;

    // Another writer clears the tails, which nothing can replay any more.
    let old = region.engine.get_region(region.region_id).unwrap();
    let action_list =
        RegionMetaActionList::with_action(RegionMetaAction::Edit(pending_tails_edit(vec![])));
    publish_delta(
        &region.env.get_object_store().unwrap(),
        &region.region_dir(),
        next_manifest_version(&old).await,
        encode_delta(action_list).await,
    )
    .await;
    wait_until(|| is_replaced(&region.engine, &old)).await;

    assert!(!region.has_pending_tails());
    assert_eq!(1, region.listener.ended());
    // The region's own rows survive the fence and the reload.
    assert_eq!(6, count_rows(&region.engine, region.region_id).await);
}

/// With write leases the replay commits while the region awaits its Leader lease, and the lease
/// that ends the wait builds the index the replay could not publish. If the region is staging
/// then, the end of staging builds it.
#[rstest]
#[case::create_on_flush(true, false, false)]
#[case::no_create_on_flush(false, false, false)]
#[case::awaiting_leader_lease(true, true, false)]
#[case::leader_lease_in_staging(true, true, true)]
#[tokio::test]
async fn test_replayed_ssts_build_index_as_flushed(
    #[case] create_on_flush: bool,
    #[case] leased: bool,
    #[case] staging: bool,
) {
    let region = TestRegion::build(
        "tail-replay-index",
        leased,
        None,
        None,
        async_build_mode_config(create_on_flush),
    )
    .await;
    if leased {
        region
            .engine
            .renew_node_lease(Instant::now() + Duration::from_secs(10));
    }
    let region_id = RegionId::new(1, 2);
    let request = CreateRequestBuilder::new()
        .insert_option(WAL_OPTIONS_KEY, &wal_options()[WAL_OPTIONS_KEY])
        .build_with_index();
    let table_dir = request.table_dir.clone();
    region
        .engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();
    let last = region.write_remote_rows(region_id, 0, 3).await;
    let tails = vec![remote_tail(0, last, UptoKind::Reported)];
    commit_edit(&region.engine, region_id, pending_tails_edit(tails)).await;
    reopen_region(&region.engine, region_id, table_dir, false, wal_options()).await;

    let catchup = RegionRequest::Catchup(RegionCatchupRequest {
        set_writable: true,
        ..Default::default()
    });
    region
        .engine
        .handle_request(region_id, catchup)
        .await
        .unwrap();
    let replayed = region.engine.get_region(region_id).unwrap();
    wait_until(|| !replayed.version().has_pending_tails()).await;
    if leased {
        assert!(replayed.manifest_ctx.awaits_leader_lease());
        // The worker handled the replay's commit before this request, and refused its build.
        let err = region
            .engine
            .handle_request(
                region_id,
                RegionRequest::BuildIndex(RegionBuildIndexRequest::default()),
            )
            .await
            .unwrap_err();
        assert!(
            matches!(
                err.as_any().downcast_ref::<Error>(),
                Some(Error::RegionLeaseNotReceived { .. })
            ),
            "{err:?}"
        );
        assert_eq!(region.listener.index_builds_stopped(), 0);
        let role = if staging {
            let request = RegionRequest::EnterStaging(EnterStagingRequest {
                partition_directive: StagingPartitionDirective::RejectAllWrites,
            });
            region
                .engine
                .handle_request(region_id, request)
                .await
                .unwrap();
            RegionRole::StagingLeader
        } else {
            RegionRole::Leader
        };
        let now = Instant::now();
        region
            .engine
            .set_region_role_with_lease(region_id, role, now, now + Duration::from_secs(10), None)
            .unwrap();
        if staging {
            // The end of the wait finds the region staging and builds nothing.
            wait_until(|| {
                region
                    .listener
                    .leader_lease_wait_ends
                    .load(Ordering::Acquire)
                    == 1
            })
            .await;
            assert_eq!(region.listener.index_builds_stopped(), 0);
            region
                .engine
                .set_region_role_state_gracefully(region_id, SettableRegionRoleState::Leader)
                .await
                .unwrap();
        }
    }

    wait_until(|| region.listener.index_builds_stopped() == 1).await;
    let version = replayed.version();
    let files = &version.ssts.levels()[0].files;
    let has_index = files.values().any(|file| file.meta_ref().exists_index());
    assert_eq!(create_on_flush, has_index);
}

/// A role change outside the worker that stops the pass building the index the replay could not
/// publish, and is undone before the save the work ends with, leaves the pass due.
#[tokio::test]
async fn test_missing_index_pass_stopped_by_role_change_stays_due() {
    let region = TestRegion::build(
        "tail-replay-index-role-change",
        true,
        None,
        None,
        async_build_mode_config(true),
    )
    .await;
    region
        .engine
        .renew_node_lease(Instant::now() + Duration::from_secs(10));
    let region_id = RegionId::new(1, 2);
    let request = CreateRequestBuilder::new()
        .insert_option(WAL_OPTIONS_KEY, &wal_options()[WAL_OPTIONS_KEY])
        .build_with_index();
    let table_dir = request.table_dir.clone();
    region
        .engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();
    let last = region.write_remote_rows(region_id, 0, 3).await;
    let tails = vec![remote_tail(0, last, UptoKind::Reported)];
    commit_edit(&region.engine, region_id, pending_tails_edit(tails)).await;
    reopen_region(&region.engine, region_id, table_dir, false, wal_options()).await;
    let catchup = RegionRequest::Catchup(RegionCatchupRequest {
        set_writable: true,
        ..Default::default()
    });
    region
        .engine
        .handle_request(region_id, catchup)
        .await
        .unwrap();
    let replayed = region.engine.get_region(region_id).unwrap();
    wait_until(|| !replayed.version().has_pending_tails()).await;
    assert!(replayed.manifest_ctx.awaits_leader_lease());
    let listener = &region.listener;

    // The lease starts the work, and a demotion lands before the pass.
    listener.park_deferred_work.arm();
    listener.park_backfill.arm();
    let now = Instant::now();
    region
        .engine
        .set_region_role_with_lease(
            region_id,
            RegionRole::Leader,
            now,
            now + Duration::from_secs(10),
            None,
        )
        .unwrap();
    listener.park_deferred_work.wait_entered().await;
    region
        .engine
        .set_region_role(region_id, RegionRole::Follower)
        .unwrap();
    listener.park_deferred_work.release();
    // The worker reports the end of the wait once it ran what it could.
    wait_until(|| listener.leader_lease_wait_ends.load(Ordering::Acquire) == 1).await;
    // A promotion before any save checks the region.
    let notified = listener.became_writable.load(Ordering::Acquire);
    region
        .engine
        .set_region_role(region_id, RegionRole::Leader)
        .unwrap();
    wait_until(|| listener.became_writable.load(Ordering::Acquire) > notified).await;
    listener.park_backfill.release();

    wait_until(|| listener.index_builds_stopped() == 1).await;
    let version = replayed.version();
    let files = &version.ssts.levels()[0].files;
    assert!(files.values().any(|file| file.meta_ref().exists_index()));
}

#[tokio::test]
async fn test_stalled_manifest_check_ends_when_region_closes() {
    let stall = Arc::new(AtomicBool::new(false));
    let stalled = Arc::new(AtomicUsize::new(0));
    let layer = stall_listings(is_manifest, stall.clone(), stalled.clone());
    let region = TestRegion::new("tail-replay-stalled-check", false, Some(layer)).await;
    region.start_failing_remote_replay().await;
    // A failed round checks the manifest for an update, which stalls.
    stall.store(true, Ordering::Relaxed);
    wait_until(|| stalled.load(Ordering::Relaxed) > 0).await;
    let mito_region = region.engine.get_region(region.region_id).unwrap();
    // The stalled listing holds no manifest lock.
    let write = tokio::time::timeout(
        Duration::from_secs(5),
        mito_region.manifest_ctx.manifest_manager.write(),
    )
    .await
    .unwrap();
    drop(write);
    let replaying = Arc::downgrade(&mito_region);
    drop(mito_region);

    let close = RegionRequest::Close(RegionCloseRequest::default());
    tokio::time::timeout(
        Duration::from_secs(10),
        region.engine.handle_request(region.region_id, close),
    )
    .await
    .unwrap()
    .unwrap();

    wait_until(|| replaying.strong_count() == 0).await;
}

/// Parks the first SST write once `stall` is set, and counts the parked writes.
fn stall_first_sst_write(stall: Arc<AtomicBool>, stalled: Arc<AtomicUsize>) -> MockLayer {
    MockLayerBuilder::default()
        .writer_factory(Arc::new(move |path, _args, inner| {
            if path.ends_with(".parquet") && stall.swap(false, Ordering::Relaxed) {
                stalled.fetch_add(1, Ordering::Relaxed);
                Box::new(StalledWriter)
            } else {
                inner
            }
        }))
        .build()
        .unwrap()
}

struct StalledWriter;

impl Write for StalledWriter {
    async fn write(&mut self, _bs: Buffer) -> MockResult<()> {
        std::future::pending().await
    }

    async fn close(&mut self) -> MockResult<Metadata> {
        std::future::pending().await
    }

    async fn abort(&mut self) -> MockResult<()> {
        Ok(())
    }
}

#[tokio::test]
async fn test_flush_runs_while_replay_writes_are_saturated() {
    let stall = Arc::new(AtomicBool::new(false));
    let stalled = Arc::new(AtomicUsize::new(0));
    let layer = stall_first_sst_write(stall.clone(), stalled.clone());
    // One replay SST write takes every replay write permit.
    let config = MitoConfig {
        max_background_flushes: 1,
        ..Default::default()
    };
    let region = TestRegion::with_config("tail-replay-saturated", Some(layer), config).await;
    let last = region.write_remote_rows(region.region_id, 6, 9).await;
    region
        .reopen_with_tails(vec![remote_tail(0, last, UptoKind::Reported)])
        .await;
    stall.store(true, Ordering::Relaxed);
    region.catchup().await;
    wait_until(|| stalled.load(Ordering::Relaxed) == 1).await;

    // The memtable holds rows `[0, 6)`, which the reopen replayed from this node's WAL.
    tokio::time::timeout(
        Duration::from_secs(10),
        flush_region(&region.engine, region.region_id, None),
    )
    .await
    .unwrap();

    assert_eq!(1, region.stored_ssts().await);
    assert!(region.has_pending_tails());
}

#[tokio::test]
async fn test_replay_waiting_to_fence_ends_when_demoted() {
    let region = TestRegion::new("tail-replay-fence-wait", false, None).await;
    region.start_failing_remote_replay().await;
    wait_until(|| region.listener.failed()).await;

    // The next failed round finds the newer manifest and waits for the write lock behind
    // this read guard, unless the replay is cancelled.
    let mito_region = region.engine.get_region(region.region_id).unwrap();
    let version = next_manifest_version(&mito_region).await;
    let guard = mito_region.manifest_ctx.manifest_manager.read().await;
    publish_delta(
        &region.env.get_object_store().unwrap(),
        &region.region_dir(),
        version,
        encode_delta(nop_action()).await,
    )
    .await;
    wait_until(|| region.listener.fence_begins() == 1).await;
    // The worker cancels the replay before the downgrade waits for the manifest lock.
    let engine = region.engine.clone();
    let region_id = region.region_id;
    let downgrade = tokio::spawn(async move {
        engine
            .set_region_role_state_gracefully(region_id, SettableRegionRoleState::Follower)
            .await
    });

    wait_until(|| region.listener.ended() == 1).await;
    drop(guard);
    downgrade.await.unwrap().unwrap();

    assert!(mito_region.is_follower());
}

#[rstest]
#[case::fences(false, false)]
#[case::version_changed(true, false)]
#[case::not_writable(false, true)]
#[tokio::test]
async fn test_fence_for_reload_rechecks_version_and_role(
    #[case] version_changed: bool,
    #[case] demoted: bool,
) {
    let region = TestRegion::new("tail-replay-fence-recheck", false, None).await;
    if demoted {
        region
            .engine
            .set_region_role(region.region_id, RegionRole::Follower)
            .unwrap();
    }
    let mito_region = region.engine.get_region(region.region_id).unwrap();
    let manifest_ctx = &mito_region.manifest_ctx;
    let mut manager = manifest_ctx.manifest_manager.write().await;
    let installed = manager.manifest().manifest_version + u64::from(version_changed);

    let fenced = manifest_ctx
        .fence_for_reload_if_unchanged(&mut manager, region.region_id, installed)
        .await;

    let expected = !version_changed && !demoted;
    assert_eq!(expected, fenced);
    assert_eq!(expected, manifest_ctx.is_reload_pending());
}

#[rstest]
#[case::set_role(false)]
#[case::set_role_with_lease(true)]
#[tokio::test]
async fn test_direct_demotion_ends_stalled_replay(#[case] with_lease: bool) {
    let stall = Arc::new(AtomicBool::new(false));
    let stalled = Arc::new(AtomicUsize::new(0));
    let layer = stall_listings(is_remote_wal, stall.clone(), stalled.clone());
    let region =
        TestRegion::with_wal_layer("tail-replay-direct-demotion", false, None, Some(layer)).await;
    let last = region.write_remote_rows(region.region_id, 6, 9).await;
    region
        .reopen_with_tails(vec![remote_tail(0, last, UptoKind::Reported)])
        .await;
    stall.store(true, Ordering::Relaxed);
    region.catchup().await;
    wait_until(|| stalled.load(Ordering::Relaxed) > 0).await;

    if with_lease {
        let now = Instant::now();
        region
            .engine
            .set_region_role_with_lease(
                region.region_id,
                RegionRole::Follower,
                now,
                now + Duration::from_secs(10),
                None,
            )
            .unwrap();
    } else {
        region
            .engine
            .set_region_role(region.region_id, RegionRole::Follower)
            .unwrap();
    }
    wait_until(|| region.listener.ended() == 1).await;

    // A later promotion replays the tails again.
    stall.store(false, Ordering::Relaxed);
    region.promote();
    region.wait_until_replayed().await;
    assert_eq!(9, count_rows(&region.engine, region.region_id).await);
}

#[tokio::test]
async fn test_late_demotion_notice_of_replaced_instance_keeps_replay() {
    let region = TestRegion::new("tail-replay-stale-notice", false, None).await;
    region
        .reopen_with_tails(vec![own_tail(0, region.entries[1], UptoKind::Reported)])
        .await;
    let old = region.engine.get_region(region.region_id).unwrap();
    region.replace_follower().await;
    region.listener.hold_from(1);
    region.promote();
    wait_until(|| region.listener.commits() == 1).await;

    // A demotion notice of the replaced instance arrives late.
    let request = WorkerRequest::Background {
        region_id: region.region_id,
        notify: BackgroundNotify::RegionCannotReplay { region: old },
    };
    region
        .engine
        .inner
        .workers
        .submit_to_worker(region.region_id, request)
        .await
        .unwrap();
    // The worker handles the notice before this write.
    region.overwrite_remote_row().await;
    region.listener.hold_from(0);
    region.wait_until_replayed().await;

    assert_eq!(1, region.listener.commits());
}

#[tokio::test]
async fn test_failed_replay_reloads_region_after_failed_commit() {
    let faults = DeltaFaults::default();
    let region = TestRegion::new(
        "tail-replay-failed-commit-reload",
        false,
        Some(faults.layer()),
    )
    .await;
    region.start_failing_remote_replay().await;
    wait_until(|| region.listener.failed()).await;

    // A failed flush commit takes the next version number without installing it.
    let old = region.engine.get_region(region.region_id).unwrap();
    let next = next_manifest_version(&old).await;
    faults.fail_next_write();
    let flush = RegionRequest::Flush(RegionFlushRequest::default());
    assert!(
        region
            .engine
            .handle_request(region.region_id, flush)
            .await
            .is_err()
    );
    assert_eq!(
        next,
        old.manifest_ctx
            .manifest_manager
            .read()
            .await
            .last_version()
    );

    // Another writer then publishes that version, clearing the tails.
    let action_list =
        RegionMetaActionList::with_action(RegionMetaAction::Edit(pending_tails_edit(vec![])));
    publish_delta(
        &region.env.get_object_store().unwrap(),
        &region.region_dir(),
        next,
        encode_delta(action_list).await,
    )
    .await;
    wait_until(|| is_replaced(&region.engine, &old)).await;

    assert!(!region.has_pending_tails());
}

#[tokio::test]
async fn test_discard_keeps_other_tails_and_their_wal() {
    let region = TestRegion::new("tail-discard-remote", false, None).await;
    let (own, remote) = region.reopen_with_own_and_remote_tails().await;
    region.listener.hold_from(1);
    region.catchup().await;
    // The replay read both tails and waits to commit them.
    wait_until(|| region.listener.commits() == 1).await;
    assert_eq!(
        region.entries.to_vec(),
        stored_entry_ids(&region.log_store, region.region_id).await
    );

    assert_eq!(
        1,
        region.discard_pending_tails(REMOTE_NODE_ID).await.unwrap()
    );

    let version = region.version();
    assert_eq!(vec![own], version.pending_tails);
    assert_eq!(remote.upto, version.flushed_entry_id);
    wait_until(|| region.listener.discard_releases() == 1).await;
    // The release kept the entries of the own tail, which the raised flushed entry id covers.
    assert_eq!(
        region.entries.to_vec(),
        stored_entry_ids(&region.log_store, region.region_id).await
    );
    assert_eq!(
        0,
        region.discard_pending_tails(REMOTE_NODE_ID).await.unwrap()
    );
    region.listener.hold_from(0);
    region.wait_until_replayed().await;
    assert_eq!(6, count_rows(&region.engine, region.region_id).await);
}

#[tokio::test]
async fn test_discard_after_unapplied_replay_commit_is_busy() {
    let region = TestRegion::new("tail-discard-unapplied", false, None).await;
    let remote_upto = region.write_remote_rows(region.region_id, 6, 9).await;
    region
        .reopen_with_tails(vec![remote_tail(0, remote_upto, UptoKind::Reported)])
        .await;
    region.listener.hold_commit_end(true);
    region.catchup().await;
    // The replay committed and has not told the worker yet.
    wait_until(|| region.listener.commit_ends() == 1).await;

    let error = region
        .discard_pending_tails(REMOTE_NODE_ID)
        .await
        .unwrap_err();
    assert_eq!(StatusCode::RegionBusy, error.status_code());

    region.listener.hold_commit_end(false);
    region.wait_until_replayed().await;
    assert_eq!(1, region.stored_ssts().await);
    assert_eq!(9, count_rows(&region.engine, region.region_id).await);
}

#[rstest]
#[case::follower(None)]
#[case::altering(Some(RegionLeaderState::Altering))]
#[case::editing(Some(RegionLeaderState::Editing))]
#[tokio::test]
async fn test_discard_on_unwritable_region_changes_nothing(
    #[case] busy: Option<RegionLeaderState>,
) {
    let region = TestRegion::new("tail-discard-unwritable", false, None).await;
    let tails = vec![own_tail(0, region.entries[1], UptoKind::Reported)];
    region.reopen_with_tails(tails.clone()).await;
    let mito_region = region.engine.get_region(region.region_id).unwrap();
    if let Some(busy) = busy {
        region.listener.hold_from(1);
        region.promote();
        wait_until(|| region.listener.commits() == 1).await;
        set_busy(&mito_region, busy);
    }
    let version = next_manifest_version(&mito_region).await;

    let error = region.discard_pending_tails(NODE_ID).await.unwrap_err();

    assert_eq!(StatusCode::RegionNotReady, error.status_code());
    assert_eq!(tails, region.version().pending_tails);
    assert_eq!(version, next_manifest_version(&mito_region).await);
}

#[tokio::test]
async fn test_discard_own_tails_releases_their_wal() {
    let region = TestRegion::new("tail-discard-own", false, None).await;
    let remote_upto = region.write_remote_rows(region.region_id, 6, 9).await;
    let remote = remote_tail(0, remote_upto, UptoKind::Reported);
    region
        .reopen_with_tails(vec![
            own_tail(0, region.entries[0], UptoKind::Reported),
            own_tail(region.entries[0], region.entries[1], UptoKind::Reported),
            remote.clone(),
        ])
        .await;
    region.listener.hold_from(1);
    region.promote();
    wait_until(|| region.listener.commits() == 1).await;

    assert_eq!(2, region.discard_pending_tails(NODE_ID).await.unwrap());

    let version = region.version();
    assert_eq!(vec![remote], version.pending_tails);
    assert_eq!(region.entries[1], version.flushed_entry_id);
    wait_until(|| region.listener.discard_releases() == 1).await;
    assert!(
        stored_entry_ids(&region.log_store, region.region_id)
            .await
            .is_empty()
    );
    region.listener.hold_from(0);
    region.wait_until_replayed().await;
    assert_eq!(3, count_rows(&region.engine, region.region_id).await);
}

/// A discard of pending tails is refused while the region awaits its Leader lease, and the same
/// discard succeeds after the lease.
#[tokio::test]
async fn test_discard_awaiting_leader_lease_succeeds_after_it() {
    let region = TestRegion::new("tail-discard-awaiting-lease", true, None).await;
    let tails = vec![
        own_tail(0, region.entries[0], UptoKind::Reported),
        own_tail(region.entries[0], region.entries[1], UptoKind::Reported),
    ];
    region.reopen_with_tails(tails.clone()).await;
    region
        .engine
        .renew_node_lease(Instant::now() + Duration::from_secs(10));
    region.listener.hold_from(1);
    region.catchup().await;
    wait_until(|| region.listener.commits() == 1).await;
    let mito_region = region.engine.get_region(region.region_id).unwrap();
    assert!(mito_region.manifest_ctx.awaits_leader_lease());
    let flushed_entry_id = region.version().flushed_entry_id;

    let error = region.discard_pending_tails(NODE_ID).await.unwrap_err();

    assert!(
        matches!(
            error.as_any().downcast_ref::<Error>(),
            Some(Error::RegionLeaseNotReceived { .. })
        ),
        "{error:?}"
    );
    assert_eq!(StatusCode::RegionNotReady, error.status_code());
    assert_eq!(tails, region.version().pending_tails);
    assert_eq!(flushed_entry_id, region.version().flushed_entry_id);

    region.promote_with_lease();
    assert_eq!(2, region.discard_pending_tails(NODE_ID).await.unwrap());
    let version = region.version();
    assert!(version.pending_tails.is_empty());
    assert_eq!(region.entries[1], version.flushed_entry_id);
    region.listener.hold_from(0);
}

#[tokio::test]
async fn test_discard_listed_tail_raises_flushed_entry_id_to_its_bound() {
    let region = TestRegion::new("tail-discard-listed", false, None).await;
    let upto = region.start_lost_listed_tail().await;

    assert_eq!(
        1,
        region.discard_pending_tails(REMOTE_NODE_ID).await.unwrap()
    );

    let current = region
        .engine
        .get_region(region.region_id)
        .unwrap()
        .version_control
        .current();
    assert_eq!(upto, current.version.flushed_entry_id);
    assert_eq!(upto, current.last_entry_id);
    region.reopen().await;
    assert!(!region.has_pending_tails());
    assert_eq!(6, count_rows(&region.engine, region.region_id).await);
}

/// The caller retries a discard whose commit landed but reported an error. Under conditional
/// publication a retry with other content conflicts with the landed edit and fences the
/// region, which reloads from it.
#[tokio::test]
async fn test_discard_retry_after_unknown_result_reloads_region() {
    let version = Arc::new(AtomicU64::new(u64::MAX));
    let region = TestRegion::new(
        "tail-discard-uncertain",
        true,
        Some(lose_delta_response(version.clone())),
    )
    .await;
    let (own, _) = region.reopen_with_own_and_remote_tails().await;
    region.listener.hold_from(1);
    region.promote_with_lease();
    wait_until(|| region.listener.commits() == 1).await;
    let old = region.engine.get_region(region.region_id).unwrap();
    version.store(next_manifest_version(&old).await, Ordering::Relaxed);

    // The discard lands but its commit reports an error.
    region
        .discard_pending_tails(REMOTE_NODE_ID)
        .await
        .unwrap_err();
    assert!(region.has_pending_tails());
    // A discard of the other node's tails commits other content at the same version.
    let error = region.discard_pending_tails(NODE_ID).await.unwrap_err();

    assert_eq!(StatusCode::StorageUnavailable, error.status_code());
    wait_until(|| is_replaced(&region.engine, &old)).await;
    assert_eq!(vec![own], region.version().pending_tails);
    region.promote_with_lease();
    assert_eq!(
        0,
        region.discard_pending_tails(REMOTE_NODE_ID).await.unwrap()
    );
}

/// Without conditional publication, a replay's commit that landed but reported an error is
/// uncertain until the replay commits again; a discard meanwhile would commit on top of it and
/// record the replayed tails again.
#[tokio::test]
async fn test_discard_over_uncertain_replay_commit_is_busy() {
    let version = Arc::new(AtomicU64::new(u64::MAX));
    let region = TestRegion::new(
        "tail-discard-uncertain-replay",
        false,
        Some(lose_delta_response(version.clone())),
    )
    .await;
    region.reopen_with_own_and_remote_tails().await;
    let mito_region = region.engine.get_region(region.region_id).unwrap();
    version.store(next_manifest_version(&mito_region).await, Ordering::Relaxed);
    region.listener.hold_from(2);
    region.catchup().await;
    // The replay's commit lands but reports an error, and its retry waits.
    wait_until(|| region.listener.commits() == 2).await;

    let error = region
        .discard_pending_tails(REMOTE_NODE_ID)
        .await
        .unwrap_err();

    assert_eq!(StatusCode::RegionBusy, error.status_code());
    // A flush that commits meanwhile leaves the replay's update uncertain.
    let schema = rows_schema(&CreateRequestBuilder::new().build());
    put_range(&region.engine, region.region_id, &schema, 9, 12).await;
    flush_region(&region.engine, region.region_id, None).await;
    let error = region
        .discard_pending_tails(REMOTE_NODE_ID)
        .await
        .unwrap_err();
    assert_eq!(StatusCode::RegionBusy, error.status_code());
    region.listener.hold_from(0);
    region.wait_until_replayed().await;
    region.reopen().await;
    assert!(!region.has_pending_tails());
    assert_eq!(12, count_rows(&region.engine, region.region_id).await);
}

/// A replay that commits after a discard whose result is uncertain replays every tail, so no
/// row is lost and no tail is left.
#[tokio::test]
async fn test_replay_commit_over_uncertain_discard_replays_every_tail() {
    let version = Arc::new(AtomicU64::new(u64::MAX));
    let region = TestRegion::new(
        "tail-replay-uncertain-discard",
        false,
        Some(lose_delta_response(version.clone())),
    )
    .await;
    region.reopen_with_own_and_remote_tails().await;
    region.listener.hold_from(1);
    region.catchup().await;
    wait_until(|| region.listener.commits() == 1).await;
    let mito_region = region.engine.get_region(region.region_id).unwrap();
    version.store(next_manifest_version(&mito_region).await, Ordering::Relaxed);

    // The discard lands but reports an error.
    region
        .discard_pending_tails(REMOTE_NODE_ID)
        .await
        .unwrap_err();
    region.listener.hold_from(0);
    region.wait_until_replayed().await;
    region.reopen().await;

    assert!(!region.has_pending_tails());
    assert_eq!(9, count_rows(&region.engine, region.region_id).await);
}

/// A discard waiting for the manifest lock while the region is demoted answers the region
/// state error, not busy.
#[tokio::test]
async fn test_discard_demoted_before_its_commit_reports_region_state() {
    let region = TestRegion::new("tail-discard-demoted", false, None).await;
    let tails = vec![own_tail(0, region.entries[1], UptoKind::Reported)];
    region.reopen_with_tails(tails.clone()).await;
    region.listener.hold_from(1);
    region.promote();
    wait_until(|| region.listener.commits() == 1).await;
    let mito_region = region.engine.get_region(region.region_id).unwrap();
    let version = next_manifest_version(&mito_region).await;
    let manager = &mito_region.manifest_ctx.manifest_manager;
    let reading = manager.read().await;

    let (discarded, ()) = tokio::join!(region.discard_pending_tails(NODE_ID), async {
        // The discard's commit waits for the lock while the region is demoted.
        wait_until(|| manager.try_read().is_err()).await;
        region
            .engine
            .set_region_role(region.region_id, RegionRole::Follower)
            .unwrap();
        drop(reading);
    });

    let error = discarded.unwrap_err();
    assert_eq!(StatusCode::RegionNotReady, error.status_code());
    assert_eq!(tails, region.version().pending_tails);
    assert_eq!(version, next_manifest_version(&mito_region).await);
}

/// Lets the first write of the manifest delta `version` land as if its response was lost, and
/// fails the write of the next delta before it stores anything.
fn lose_delta_response_then_reject(version: Arc<AtomicU64>) -> MockLayer {
    let armed = Arc::new(AtomicBool::new(true));
    MockLayerBuilder::default()
        .writer_factory(Arc::new(move |path, _args, inner| {
            let target = version.load(Ordering::Relaxed);
            if target == u64::MAX {
                return inner;
            }
            let (lost, rejected) = (delta_file(target), delta_file(target + 1));
            if path.contains(&lost) && armed.swap(false, Ordering::Relaxed) {
                Box::new(LostResponseWriter { inner })
            } else if path.contains(&rejected) {
                Box::new(RejectingWriter)
            } else {
                inner
            }
        }))
        .build()
        .unwrap()
}

struct RejectingWriter;

impl Write for RejectingWriter {
    async fn write(&mut self, _bs: Buffer) -> MockResult<()> {
        Err(MockError::new(ErrorKind::ConfigInvalid, "rejected"))
    }

    async fn close(&mut self) -> MockResult<Metadata> {
        Err(MockError::new(ErrorKind::ConfigInvalid, "rejected"))
    }

    async fn abort(&mut self) -> MockResult<()> {
        Ok(())
    }
}

/// A replay retry that fails without storing anything keeps the earlier attempt's uncertain
/// result, so a discard stays busy.
#[tokio::test]
async fn test_discard_stays_busy_after_a_rejected_replay_retry() {
    let version = Arc::new(AtomicU64::new(u64::MAX));
    let region = TestRegion::new(
        "tail-discard-rejected-retry",
        false,
        Some(lose_delta_response_then_reject(version.clone())),
    )
    .await;
    region.reopen_with_own_and_remote_tails().await;
    let mito_region = region.engine.get_region(region.region_id).unwrap();
    version.store(next_manifest_version(&mito_region).await, Ordering::Relaxed);
    region.listener.hold_from(3);
    region.catchup().await;
    // The first commit lands but reports an error, the second is rejected, the third waits.
    wait_until(|| region.listener.commits() == 3).await;

    let error = region
        .discard_pending_tails(REMOTE_NODE_ID)
        .await
        .unwrap_err();

    assert_eq!(StatusCode::RegionBusy, error.status_code());
    region.listener.hold_from(0);
    region.wait_until_replayed().await;
    region.reopen().await;
    assert!(!region.has_pending_tails());
    assert_eq!(9, count_rows(&region.engine, region.region_id).await);
}

/// The uncertain result of a cancelled replay keeps discards busy until a later replay of the
/// same instance publishes.
#[tokio::test]
async fn test_discard_stays_busy_after_an_uncertain_replay_ends() {
    let version = Arc::new(AtomicU64::new(u64::MAX));
    let region = TestRegion::new(
        "tail-discard-ended-uncertain",
        false,
        Some(lose_delta_response(version.clone())),
    )
    .await;
    region.reopen_with_own_and_remote_tails().await;
    let mito_region = region.engine.get_region(region.region_id).unwrap();
    version.store(next_manifest_version(&mito_region).await, Ordering::Relaxed);
    region.listener.hold_from(2);
    region.catchup().await;
    wait_until(|| region.listener.commits() == 2).await;
    // A demotion ends the replay while its update is uncertain.
    region
        .engine
        .set_region_role(region.region_id, RegionRole::Follower)
        .unwrap();
    region.listener.hold_from(3);
    wait_until(|| region.listener.ended() == 1).await;
    region.promote();
    wait_until(|| region.listener.commits() == 3).await;

    let error = region
        .discard_pending_tails(REMOTE_NODE_ID)
        .await
        .unwrap_err();

    assert_eq!(StatusCode::RegionBusy, error.status_code());
    region.listener.hold_from(0);
    region.wait_until_replayed().await;
    assert_eq!(
        0,
        region.discard_pending_tails(REMOTE_NODE_ID).await.unwrap()
    );
}

/// A replay's commit that succeeds after one with an uncertain result clears the uncertainty,
/// so a discard of the tail the replay keeps succeeds.
#[tokio::test]
async fn test_discard_succeeds_after_an_uncertain_replay_commit_is_retried() {
    let version = Arc::new(AtomicU64::new(u64::MAX));
    let region = TestRegion::new(
        "tail-discard-recommitted",
        false,
        Some(lose_delta_response(version.clone())),
    )
    .await;
    let remote_upto = region.write_remote_rows(region.region_id, 6, 9).await;
    // Another node's listed tail stays from its last entry read until the region is leased.
    let remote = remote_tail(0, remote_upto + 1, UptoKind::Listed);
    region
        .reopen_with_tails(vec![
            own_tail(0, region.entries[1], UptoKind::Reported),
            remote.clone(),
        ])
        .await;
    let mito_region = region.engine.get_region(region.region_id).unwrap();
    version.store(next_manifest_version(&mito_region).await, Ordering::Relaxed);
    region.catchup().await;
    // The first commit lands but reports an error, and its retry commits.
    let kept = PendingTail {
        after: remote_upto,
        ..remote
    };
    wait_until(|| region.version().pending_tails == vec![kept.clone()]).await;
    assert_eq!(2, region.listener.commits());

    assert_eq!(
        1,
        region.discard_pending_tails(REMOTE_NODE_ID).await.unwrap()
    );
    assert!(!region.has_pending_tails());
}

/// With conditional publication, a discard over a replay's update that was saved but reported
/// an error conflicts with it, and the region reloads from storage without the tails.
#[tokio::test]
async fn test_conditional_discard_over_uncertain_replay_reloads_region() {
    let version = Arc::new(AtomicU64::new(u64::MAX));
    let region = TestRegion::new(
        "tail-discard-conditional-uncertain",
        true,
        Some(lose_delta_response(version.clone())),
    )
    .await;
    region.reopen_with_own_and_remote_tails().await;
    let old = region.engine.get_region(region.region_id).unwrap();
    version.store(next_manifest_version(&old).await, Ordering::Relaxed);
    region.listener.hold_from(2);
    region.promote_with_lease();
    wait_until(|| region.listener.commits() == 2).await;

    let error = region
        .discard_pending_tails(REMOTE_NODE_ID)
        .await
        .unwrap_err();

    assert_eq!(StatusCode::StorageUnavailable, error.status_code());
    wait_until(|| is_replaced(&region.engine, &old)).await;
    assert!(!region.has_pending_tails());
    region.listener.hold_from(0);
    assert_eq!(9, count_rows(&region.engine, region.region_id).await);
}

/// A discard raises the flushed entry id to a listed tail's bound. After a crash, the
/// object-store WAL, which by default does not accept missing history, checks history, so the
/// region opens only if this node's WAL durably holds history through the bound, as whoever
/// records the tail ensures.
#[rstest]
#[case::history_past_bound(true)]
#[case::history_below_bound(false)]
#[tokio::test]
async fn test_discard_reopens_after_crash_with_history_past_bound(
    #[case] history_past_bound: bool,
) {
    let region = TestRegion::new("tail-discard-crash", false, None).await;
    let upto = region.start_lost_listed_tail().await;
    if history_past_bound {
        // Moves this node's next entry id past the bound and makes an entry there durable.
        let provider = region
            .engine
            .get_region(region.region_id)
            .unwrap()
            .provider
            .clone();
        region
            .log_store
            .obsolete(&provider, region.region_id, upto)
            .await
            .unwrap();
        let schema = rows_schema(&CreateRequestBuilder::new().build());
        put_range(&region.engine, region.region_id, &schema, 9, 10).await;
    }
    assert_eq!(
        1,
        region.discard_pending_tails(REMOTE_NODE_ID).await.unwrap()
    );

    let opened = region.crash_and_open().await;

    if history_past_bound {
        opened.unwrap();
    } else {
        let error = format!("{:?}", opened.unwrap_err());
        assert!(error.contains("does not hold the history"), "{error}");
    }
}

#[tokio::test]
async fn test_region_statistic_reports_pending_tails() {
    let region = TestRegion::new("tail-replay-statistic", false, None).await;
    region
        .reopen_with_tails(vec![own_tail(0, region.entries[1], UptoKind::Reported)])
        .await;
    let statistic = region.engine.region_statistic(region.region_id).unwrap();
    assert!(statistic.has_pending_tails);

    region.promote();
    region.wait_until_replayed().await;

    let statistic = region.engine.region_statistic(region.region_id).unwrap();
    assert!(!statistic.has_pending_tails);
}
