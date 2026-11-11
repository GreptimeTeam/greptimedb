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

use api::v1::{Mutation, OpType, Rows, WalEntry};
use async_trait::async_trait;
use common_error::ext::ErrorExt;
use common_error::status_code::StatusCode;
use common_recordbatch::RecordBatches;
use common_wal::config::object_store::STANDALONE_GENERATION;
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
use store_api::region_engine::{RegionEngine, RegionRole, SettableRegionRoleState};
use store_api::region_request::{
    PathType, RegionAlterRequest, RegionCatchupRequest, RegionCloseRequest, RegionDropRequest,
    RegionRequest, RegionTruncateRequest,
};
use store_api::storage::{RegionId, ScanRequest};

use crate::config::MitoConfig;
use crate::engine::MitoEngine;
use crate::engine::listener::EventListener;
use crate::engine::object_store_wal_test::{
    count_rows, count_ssts, create_leased_region_on, create_region, encode_delta, is_replaced,
    next_manifest_version, publish_delta, put_range, wait_until, wal_config, wal_options,
};
use crate::engine::pending_tail_test::{
    add_field, commit_edit, pending_tails_edit, set_pending_tails,
};
use crate::error::Error;
use crate::manifest::action::{PendingTail, RegionEdit, UptoKind};
use crate::manifest::storage::delta_file;
use crate::region::RegionLeaderState;
use crate::region::version::VersionRef;
use crate::request::{BackgroundNotify, WorkerRequest};
use crate::sst::location::region_dir_from_table_dir;
use crate::test_util::{
    CreateRequestBuilder, TestEnv, build_rows, build_rows_for_key, nop_action, put_rows,
    reopen_region, rows_schema,
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
    failures: Mutex<Vec<StatusCode>>,
    ended: AtomicUsize,
}

#[async_trait]
impl EventListener for TailReplayListener {
    async fn on_tail_commit_begin(&self, _region_id: RegionId) {
        let attempt = self.commits.fetch_add(1, Ordering::Relaxed) + 1;
        while (1..=attempt).contains(&self.held_from.load(Ordering::Relaxed)) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    fn on_tail_replay_failed(&self, _region_id: RegionId, error: &Error) {
        self.failures.lock().unwrap().push(error.status_code());
    }

    fn on_tail_replay_ended(&self, _region_id: RegionId) {
        self.ended.fetch_add(1, Ordering::Relaxed);
    }
}

impl TailReplayListener {
    fn hold_from(&self, attempt: usize) {
        self.held_from.store(attempt, Ordering::Relaxed);
    }

    fn commits(&self) -> usize {
        self.commits.load(Ordering::Relaxed)
    }

    fn ended(&self) -> usize {
        self.ended.load(Ordering::Relaxed)
    }

    fn failed(&self) -> bool {
        !self.failures.lock().unwrap().is_empty()
    }

    fn failed_with(&self, code: StatusCode) -> bool {
        self.failures.lock().unwrap().contains(&code)
    }
}

struct TestRegion {
    env: TestEnv,
    /// The storage of the WAL of every node.
    wal_store: ObjectStore,
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
        Self::with_wal_layer(prefix, leased, mock_layer, None).await
    }

    /// As [Self::new], with `wal_layer` over the WAL storage of every node.
    async fn with_wal_layer(
        prefix: &str,
        leased: bool,
        mock_layer: Option<MockLayer>,
        wal_layer: Option<MockLayer>,
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
            STANDALONE_GENERATION,
        )
        .await
        .unwrap();
        let listener = Arc::new(TailReplayListener::default());
        let engine = env
            .create_engine_with_log_store_and_time(
                MitoConfig::default(),
                log_store,
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
        reopen_region(
            &self.engine,
            self.region_id,
            self.table_dir.clone(),
            false,
            wal_options(),
        )
        .await;
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
            )
            .unwrap();
    }

    /// Asks the worker to replay the tails, as a role change that makes the region writable does.
    async fn start_replay(&self) {
        let request = WorkerRequest::Background {
            region_id: self.region_id,
            notify: BackgroundNotify::RegionWritable,
        };
        self.engine
            .inner
            .workers
            .submit_to_worker(self.region_id, request)
            .await
            .unwrap();
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
            STANDALONE_GENERATION,
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
    wal_config().node_prefix(REMOTE_NODE_ID, STANDALONE_GENERATION)
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
    let prefix = wal_config().node_prefix(NODE_ID, STANDALONE_GENERATION);
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

#[tokio::test]
async fn test_catchup_replays_remote_reported_tail() {
    let region = TestRegion::new("tail-replay-remote-reported", false, None).await;
    let last = region.write_remote_rows(region.region_id, 6, 9).await;
    region
        .reopen_with_tails(vec![remote_tail(0, last, UptoKind::Reported)])
        .await;

    region.catchup().await;
    region.wait_until_replayed().await;

    assert_eq!(last, region.version().flushed_entry_id);
    assert_eq!(9, count_rows(&region.engine, region.region_id).await);
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
    wait_until(|| region.listener.failed()).await;
    assert!(region.has_pending_tails());
    region.wal_store.write(&path, bytes).await.unwrap();
    region.wait_until_replayed().await;

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
    tokio::time::sleep(Duration::from_secs(1)).await;
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
    // Whatever records tails raises the committed sequence to theirs, here 100 to 102, so
    // writes accepted during the replay get larger sequences.
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

    region
        .engine
        .set_region_role(region.region_id, RegionRole::Follower)
        .unwrap();
    region.promote();
    region.wait_until_replayed().await;
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

/// Parks every listing of the WAL of [REMOTE_NODE_ID] once `stall` is set, and counts them.
fn stall_remote_listings(stall: Arc<AtomicBool>, stalled: Arc<AtomicUsize>) -> MockLayer {
    MockLayerBuilder::default()
        .lister_factory(Arc::new(move |path, _args, inner| {
            if path.starts_with(&remote_prefix()) && stall.load(Ordering::Relaxed) {
                stalled.fetch_add(1, Ordering::Relaxed);
                Box::new(StalledLister)
            } else {
                inner
            }
        }))
        .build()
        .unwrap()
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
    let layer = stall_remote_listings(stall.clone(), stalled.clone());
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
async fn test_replay_backs_off_while_altering() {
    let region = TestRegion::new("tail-replay-altering", false, None).await;
    region
        .reopen_with_tails(vec![own_tail(0, region.entries[1], UptoKind::Reported)])
        .await;
    region.listener.hold_from(1);
    region.promote();
    wait_until(|| region.listener.commits() == 1).await;
    let mito_region = region.engine.get_region(region.region_id).unwrap();
    mito_region.set_altering().unwrap();

    region.listener.hold_from(0);
    wait_until(|| region.listener.commits() == 2).await;
    assert!(region.has_pending_tails());
    mito_region.switch_state_to_writable(RegionLeaderState::Altering);
    region.wait_until_replayed().await;

    assert_eq!(6, count_rows(&region.engine, region.region_id).await);
}
