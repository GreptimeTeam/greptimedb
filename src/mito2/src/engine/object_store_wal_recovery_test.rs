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

//! Recovery tests for regions on a real [`ObjectStoreLogStore`] over an
//! in-memory object store. The store seals objects only through its testing
//! hooks, so every test controls which entries are durable.
//!
//! Every open of the store writes a start object above every present object,
//! so the first data object of a store opened on an empty prefix has
//! sequence 1.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use api::v1::Rows;
use common_base::readable_size::ReadableSize;
use common_error::ext::{BoxedError, ErrorExt, RetryHint};
use common_error::status_code::StatusCode;
use common_recordbatch::RecordBatches;
use common_wal::config::object_store::{
    AckMode, CorruptedSegmentAction, ObjectStoreWalConfig, STANDALONE_GENERATION,
};
use common_wal::options::{ObjectStoreWalOptions, WAL_OPTIONS_KEY, WalOptions};
use log_store::ObjectStoreLogStore;
use log_store::metrics::{
    METRIC_OBJECT_STORE_WAL_FAILING, METRIC_OBJECT_STORE_WAL_MISSING_HISTORY_ACCEPTED_TOTAL,
};
use log_store::object_store_wal::{WalHole, entry_id};
use object_store::ObjectStore;
use object_store::layers::mock::{self, MockLayerBuilder, oio};
use object_store::services::Memory;
use object_store::util::with_retry_layers;
use rstest::rstest;
use store_api::logstore::LogStore;
use store_api::logstore::provider::Provider;
use store_api::mito_engine_options::SKIP_WAL_KEY;
use store_api::region_engine::{RegionEngine, RegionRole};
use store_api::region_request::{
    PathType, RegionCatchupRequest, RegionCloseRequest, RegionDropRequest, RegionFlushRequest,
    RegionOpenRequest, RegionPutRequest, RegionRequest, RegionTruncateRequest,
};
use store_api::storage::{RegionId, ScanRequest};

use crate::config::MitoConfig;
use crate::engine::MitoEngine;
use crate::flush::STOP_DURABILITY_GRACE;
use crate::region::MitoRegionRef;
use crate::test_util::{
    CreateRequestBuilder, TestEnv, build_rows, build_rows_for_key, flush_region, put_rows,
    rows_schema,
};

/// The root the store derives its node prefix from.
const ROOT: &str = "cluster-a/wal";
/// The node prefix of datanode 0 in the standalone generation under [`ROOT`].
const PREFIX: &str = "cluster-a/wal/datanodes/0/epochs/0";
/// The engine runs two workers and these regions map to different ones, so
/// their writes can be admitted into the same open batch.
const REGION_A: RegionId = RegionId::new(1, 1);
const REGION_B: RegionId = RegionId::new(1, 2);
const WAIT: Duration = Duration::from_secs(30);

fn memory_store() -> ObjectStore {
    ObjectStore::new(Memory::default()).unwrap()
}

/// The configuration of a store with `ack_mode` that never seals a batch on
/// its own.
fn store_config(ack_mode: AckMode) -> ObjectStoreWalConfig {
    ObjectStoreWalConfig {
        storage_provider: String::new(),
        prefix: ROOT.to_string(),
        flush_interval: Duration::from_secs(3600),
        max_batch_bytes: ReadableSize(u64::MAX),
        ack_mode,
        ..Default::default()
    }
}

/// Opens a store under [`PREFIX`] with `ack_mode` that never seals a batch
/// on its own.
async fn open_store(object_store: &ObjectStore, ack_mode: AckMode) -> Arc<ObjectStoreLogStore> {
    ObjectStoreLogStore::try_new(
        object_store.clone(),
        &store_config(ack_mode),
        0,
        STANDALONE_GENERATION,
    )
    .await
    .unwrap()
}

async fn new_engine(env: &mut TestEnv, store: Arc<ObjectStoreLogStore>) -> MitoEngine {
    let config = MitoConfig {
        num_workers: 2,
        ..Default::default()
    };
    env.create_engine_with_log_store(config, store).await
}

fn wal_options() -> HashMap<String, String> {
    let options = WalOptions::ObjectStore(ObjectStoreWalOptions::new(PREFIX.to_string()));
    HashMap::from([(
        WAL_OPTIONS_KEY.to_string(),
        serde_json::to_string(&options).unwrap(),
    )])
}

fn provider(region_id: RegionId) -> Provider {
    Provider::object_store_provider(region_id, PREFIX.to_string())
}

/// Creates a region on the object store WAL with `extra_options` and returns
/// its table dir and row schema.
async fn create_region(
    engine: &MitoEngine,
    region_id: RegionId,
    extra_options: &[(&str, &str)],
) -> (String, Vec<api::v1::ColumnSchema>) {
    let mut builder =
        CreateRequestBuilder::new().insert_option(WAL_OPTIONS_KEY, &wal_options()[WAL_OPTIONS_KEY]);
    for (key, value) in extra_options {
        builder = builder.insert_option(key, value);
    }
    let request = builder.build();
    let table_dir = request.table_dir.clone();
    let schema = rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();
    (table_dir, schema)
}

/// Opens a region whose options select the object store WAL under
/// [`PREFIX`] plus `extra_options`, and makes it writable.
async fn open_region(
    engine: &MitoEngine,
    region_id: RegionId,
    table_dir: &str,
    extra_options: &[(&str, &str)],
) -> std::result::Result<(), BoxedError> {
    let mut options = wal_options();
    options.extend(
        extra_options
            .iter()
            .map(|(key, value)| (key.to_string(), value.to_string())),
    );
    engine
        .handle_request(
            region_id,
            RegionRequest::Open(RegionOpenRequest {
                engine: String::new(),
                table_dir: table_dir.to_string(),
                options,
                skip_wal_replay: false,
                path_type: PathType::Bare,
                checkpoint: None,
                requirements: Default::default(),
            }),
        )
        .await?;
    engine.set_region_role(region_id, RegionRole::Leader)
}

fn rows(schema: &[api::v1::ColumnSchema], start: usize, end: usize) -> Rows {
    Rows {
        schema: schema.to_vec(),
        rows: build_rows(start, end),
    }
}

/// Returns every row of the region as a table, so two scans can be compared
/// row by row.
async fn scan_rows(engine: &MitoEngine, region_id: RegionId) -> String {
    let stream = engine
        .scan_to_stream(region_id, ScanRequest::default())
        .await
        .unwrap();
    RecordBatches::try_collect(stream)
        .await
        .unwrap()
        .pretty_print()
        .unwrap()
}

fn region(engine: &MitoEngine, region_id: RegionId) -> MitoRegionRef {
    engine.get_region(region_id).unwrap()
}

/// The entry ids a region tracks in memory and in its manifest, and the
/// rows its memtables hold.
#[derive(Debug, PartialEq, Eq)]
struct RegionRecoveryState {
    flushed_entry_id: u64,
    last_entry_id: u64,
    topic_latest_entry_id: u64,
    manifest_flushed_entry_id: u64,
    memtable_rows: u64,
}

async fn region_recovery_state(engine: &MitoEngine, region_id: RegionId) -> RegionRecoveryState {
    let region = region(engine, region_id);
    let current = region.version_control.current();
    let manifest = region.manifest_ctx.manifest().await;
    RegionRecoveryState {
        flushed_entry_id: current.version.flushed_entry_id,
        last_entry_id: current.last_entry_id,
        topic_latest_entry_id: region.topic_latest_entry_id.load(Ordering::Relaxed),
        manifest_flushed_entry_id: manifest.flushed_entry_id,
        memtable_rows: current.version.memtables.num_rows(),
    }
}

const EMPTY_RECOVERY_STATE: RegionRecoveryState = RegionRecoveryState {
    flushed_entry_id: 0,
    last_entry_id: 0,
    topic_latest_entry_id: 0,
    manifest_flushed_entry_id: 0,
    memtable_rows: 0,
};

/// Writes rows through the engine and seals them into one WAL object.
///
/// A put in the `durable` mode blocks until its entries are durable, so the
/// writes run in the background until the store has admitted all of them and
/// the batch is sealed by hand.
struct SealedWriter {
    store: Arc<ObjectStoreLogStore>,
    admitted: usize,
}

impl SealedWriter {
    fn new(store: &Arc<ObjectStoreLogStore>) -> Self {
        Self {
            store: store.clone(),
            admitted: 0,
        }
    }

    async fn put_and_seal(&mut self, engine: &MitoEngine, writes: Vec<(RegionId, Rows)>) {
        let handles = writes
            .into_iter()
            .map(|(region_id, rows)| {
                let engine = engine.clone();
                tokio::spawn(async move { put_rows(&engine, region_id, rows).await })
            })
            .collect::<Vec<_>>();
        self.admitted += handles.len();
        tokio::time::timeout(WAIT, self.store.wait_for_admitted_appends(self.admitted))
            .await
            .expect("writes must reach the store")
            .unwrap();
        self.store.seal_open_batch().await.unwrap();
        for handle in handles {
            handle.await.unwrap();
        }
    }
}

fn latest(store: &ObjectStoreLogStore, region_id: RegionId) -> u64 {
    store.latest_entry_id(&provider(region_id)).unwrap()
}

/// Returns the sequences of the WAL objects under the prefix, in order.
async fn wal_object_seqs(object_store: &ObjectStore) -> Vec<u64> {
    let mut seqs = object_store
        .list(&format!("{PREFIX}/objects/"))
        .await
        .unwrap()
        .into_iter()
        .filter(|entry| entry.metadata().is_file())
        .map(|entry| {
            let path = entry.path();
            path.rsplit('/')
                .next()
                .and_then(|name| name.strip_suffix(".wal"))
                .and_then(|seq| seq.parse().ok())
                .unwrap_or_else(|| panic!("unexpected WAL object key {path}"))
        })
        .collect::<Vec<u64>>();
    seqs.sort_unstable();
    seqs
}

#[rstest]
#[case(AckMode::Durable)]
#[case(AckMode::Enqueued)]
#[tokio::test]
async fn test_reopen_after_partial_flush_replays_only_unflushed_regions(#[case] ack_mode: AckMode) {
    let mut env = TestEnv::with_prefix("object-store-wal-partial-flush").await;
    let object_store = memory_store();
    let store = open_store(&object_store, ack_mode).await;
    let engine = new_engine(&mut env, store.clone()).await;
    let (table_dir_a, schema_a) = create_region(&engine, REGION_A, &[]).await;
    let (table_dir_b, schema_b) = create_region(&engine, REGION_B, &[]).await;

    // Objects 1 and 2 each hold one entry of both regions: the ids of a
    // region are its position under the sequence of its object.
    let mut writer = SealedWriter::new(&store);
    writer
        .put_and_seal(
            &engine,
            vec![
                (REGION_A, rows(&schema_a, 0, 2)),
                (REGION_B, rows(&schema_b, 0, 3)),
            ],
        )
        .await;
    writer
        .put_and_seal(
            &engine,
            vec![
                (REGION_B, rows(&schema_b, 3, 5)),
                (REGION_A, rows(&schema_a, 2, 4)),
            ],
        )
        .await;
    assert_eq!(entry_id(2, 1), latest(&store, REGION_A));
    assert_eq!(entry_id(2, 1), latest(&store, REGION_B));

    // Flushing empties the memtables, so the topic latest entry id follows
    // the store.
    flush_region(&engine, REGION_A, None).await;
    let flushed_a = RegionRecoveryState {
        flushed_entry_id: entry_id(2, 1),
        last_entry_id: entry_id(2, 1),
        topic_latest_entry_id: entry_id(2, 1),
        manifest_flushed_entry_id: entry_id(2, 1),
        memtable_rows: 0,
    };
    assert_eq!(flushed_a, region_recovery_state(&engine, REGION_A).await);
    let rows_a = scan_rows(&engine, REGION_A).await;
    let rows_b = scan_rows(&engine, REGION_B).await;

    engine.stop().await.unwrap();
    drop(engine);
    drop(writer);
    drop(store);

    let store = open_store(&object_store, ack_mode).await;
    let engine = new_engine(&mut env, store.clone()).await;
    open_region(&engine, REGION_A, &table_dir_a, &[])
        .await
        .unwrap();
    open_region(&engine, REGION_B, &table_dir_b, &[])
        .await
        .unwrap();

    // Region A replays nothing: it flushed both entries, so the topic latest
    // entry id comes from the store. Region B replays both of its entries.
    assert_eq!(flushed_a, region_recovery_state(&engine, REGION_A).await);
    assert_eq!(
        RegionRecoveryState {
            last_entry_id: entry_id(2, 1),
            memtable_rows: 5,
            ..EMPTY_RECOVERY_STATE
        },
        region_recovery_state(&engine, REGION_B).await
    );
    assert_eq!(entry_id(2, 1), latest(&store, REGION_A));
    assert_eq!(entry_id(2, 1), latest(&store, REGION_B));
    assert_eq!(rows_a, scan_rows(&engine, REGION_A).await);
    assert_eq!(rows_b, scan_rows(&engine, REGION_B).await);
    assert_eq!(4, engine.get_region_statistic(REGION_A).unwrap().num_rows);
    assert_eq!(5, engine.get_region_statistic(REGION_B).unwrap().num_rows);
}

/// Waits until no WAL object delete is in flight.
async fn wait_for_collection(store: &ObjectStoreLogStore) {
    tokio::time::timeout(WAIT, store.wait_for_garbage_collection())
        .await
        .expect("collection must complete");
}

#[rstest]
#[case(AckMode::Durable)]
#[case(AckMode::Enqueued)]
#[tokio::test]
async fn test_flush_collects_objects_below_the_watermark(#[case] ack_mode: AckMode) {
    let mut env = TestEnv::with_prefix("object-store-wal-collection").await;
    let object_store = memory_store();
    let store = open_store(&object_store, ack_mode).await;
    let engine = new_engine(&mut env, store.clone()).await;
    let (table_dir_a, schema_a) = create_region(&engine, REGION_A, &[]).await;
    let (table_dir_b, schema_b) = create_region(&engine, REGION_B, &[]).await;

    // Object 1 holds entries of both regions, object 2 of region A and
    // object 3 of region B. Object 1 carries the boundary past the start
    // object, which is collected.
    let mut writer = SealedWriter::new(&store);
    writer
        .put_and_seal(
            &engine,
            vec![
                (REGION_A, rows(&schema_a, 0, 2)),
                (REGION_B, rows(&schema_b, 0, 3)),
            ],
        )
        .await;
    writer
        .put_and_seal(&engine, vec![(REGION_A, rows(&schema_a, 2, 4))])
        .await;
    writer
        .put_and_seal(&engine, vec![(REGION_B, rows(&schema_b, 3, 5))])
        .await;
    wait_for_collection(&store).await;
    assert_eq!(vec![1, 2, 3], wal_object_seqs(&object_store).await);

    // Region B has no watermark and still needs object 1, so flushing
    // region A collects nothing.
    flush_region(&engine, REGION_A, None).await;
    writer
        .put_and_seal(&engine, vec![(REGION_A, rows(&schema_a, 4, 6))])
        .await;
    wait_for_collection(&store).await;
    assert_eq!(vec![1, 2, 3, 4], wal_object_seqs(&object_store).await);

    // Once region B is flushed, the first object a region needs is object 4,
    // the unflushed entry of region A; object 5 carries that boundary.
    flush_region(&engine, REGION_B, None).await;
    writer
        .put_and_seal(&engine, vec![(REGION_B, rows(&schema_b, 5, 7))])
        .await;
    wait_for_collection(&store).await;
    assert_eq!(vec![4, 5], wal_object_seqs(&object_store).await);
    let rows_a = scan_rows(&engine, REGION_A).await;
    let rows_b = scan_rows(&engine, REGION_B).await;
    engine.stop().await.unwrap();
    drop(engine);
    drop(writer);
    drop(store);

    // Recovery stops at the boundary and each region replays its entry above
    // its flushed id.
    let store = open_store(&object_store, ack_mode).await;
    let engine = new_engine(&mut env, store.clone()).await;
    open_region(&engine, REGION_A, &table_dir_a, &[])
        .await
        .unwrap();
    open_region(&engine, REGION_B, &table_dir_b, &[])
        .await
        .unwrap();
    for (region_id, flushed_entry_id, last_entry_id) in [
        (REGION_A, entry_id(2, 1), entry_id(4, 1)),
        (REGION_B, entry_id(3, 1), entry_id(5, 1)),
    ] {
        let state = region_recovery_state(&engine, region_id).await;
        assert_eq!(flushed_entry_id, state.flushed_entry_id);
        assert_eq!(last_entry_id, state.last_entry_id);
        assert_eq!(2, state.memtable_rows);
    }
    assert_eq!(rows_a, scan_rows(&engine, REGION_A).await);
    assert_eq!(rows_b, scan_rows(&engine, REGION_B).await);
    assert_eq!(6, engine.get_region_statistic(REGION_A).unwrap().num_rows);
    assert_eq!(7, engine.get_region_statistic(REGION_B).unwrap().num_rows);
    wait_for_collection(&store).await;
    assert_eq!(vec![4, 5, 6], wal_object_seqs(&object_store).await);
}

#[rstest]
#[case(AckMode::Durable)]
#[case(AckMode::Enqueued)]
#[tokio::test]
async fn test_reopen_after_crash_replays_durable_entries_once(#[case] ack_mode: AckMode) {
    let mut env = TestEnv::with_prefix("object-store-wal-crash").await;
    let object_store = memory_store();
    let store = open_store(&object_store, ack_mode).await;
    let engine = new_engine(&mut env, store.clone()).await;
    let (table_dir, schema) = create_region(&engine, REGION_A, &[]).await;

    // The entry is durable as an object but the manifest never learns of it.
    let mut writer = SealedWriter::new(&store);
    writer
        .put_and_seal(&engine, vec![(REGION_A, rows(&schema, 0, 3))])
        .await;
    assert_eq!(entry_id(1, 1), latest(&store, REGION_A));
    let rows_before = scan_rows(&engine, REGION_A).await;

    drop(writer);
    crash(engine, store).await;

    let store = open_store(&object_store, ack_mode).await;
    let engine = new_engine(&mut env, store.clone()).await;
    open_region(&engine, REGION_A, &table_dir, &[])
        .await
        .unwrap();

    assert_eq!(
        RegionRecoveryState {
            last_entry_id: entry_id(1, 1),
            memtable_rows: 3,
            ..EMPTY_RECOVERY_STATE
        },
        region_recovery_state(&engine, REGION_A).await
    );
    assert_eq!(entry_id(1, 1), latest(&store, REGION_A));
    assert_eq!(rows_before, scan_rows(&engine, REGION_A).await);
    assert_eq!(3, engine.get_region_statistic(REGION_A).unwrap().num_rows);
}

#[rstest]
#[case(CorruptedSegmentAction::Skip)]
#[case(CorruptedSegmentAction::Fail)]
#[tokio::test]
async fn test_reopen_with_a_corrupted_segment(
    #[case] on_corrupted_segment: CorruptedSegmentAction,
) {
    let mut env = TestEnv::with_prefix("object-store-wal-corrupted-segment").await;
    let object_store = memory_store();
    let store = open_store(&object_store, AckMode::Durable).await;
    let engine = new_engine(&mut env, store.clone()).await;
    let (table_dir_a, schema_a) = create_region(&engine, REGION_A, &[]).await;
    let (table_dir_b, schema_b) = create_region(&engine, REGION_B, &[]).await;

    // Objects 1 and 2 each hold one entry of both regions; nothing is flushed.
    let mut writer = SealedWriter::new(&store);
    writer
        .put_and_seal(
            &engine,
            vec![
                (REGION_A, rows(&schema_a, 0, 2)),
                (REGION_B, rows(&schema_b, 0, 3)),
            ],
        )
        .await;
    writer
        .put_and_seal(
            &engine,
            vec![
                (REGION_A, rows(&schema_a, 2, 4)),
                (REGION_B, rows(&schema_b, 3, 5)),
            ],
        )
        .await;
    let rows_b = scan_rows(&engine, REGION_B).await;
    engine.stop().await.unwrap();

    // The segment of region A in object 1 is damaged.
    let (path, segment) = store
        .segment_location(&provider(REGION_A), 1)
        .unwrap()
        .unwrap();
    let mut bytes = object_store.read(&path).await.unwrap().to_vec();
    bytes[segment.end as usize - 1] ^= 1;
    object_store.write(&path, bytes).await.unwrap();
    drop(engine);
    drop(writer);
    drop(store);

    let config = ObjectStoreWalConfig {
        on_corrupted_segment,
        ..store_config(AckMode::Durable)
    };
    let store =
        ObjectStoreLogStore::try_new(object_store.clone(), &config, 0, STANDALONE_GENERATION)
            .await
            .unwrap();
    let engine = new_engine(&mut env, store.clone()).await;

    // Region B replays both of its entries whatever the damage does to A.
    open_region(&engine, REGION_B, &table_dir_b, &[])
        .await
        .unwrap();
    assert_eq!(rows_b, scan_rows(&engine, REGION_B).await);
    assert_eq!(5, engine.get_region_statistic(REGION_B).unwrap().num_rows);
    assert!(store.wal_holes(&provider(REGION_B)).unwrap().is_empty());

    let opened = open_region(&engine, REGION_A, &table_dir_a, &[]).await;
    match on_corrupted_segment {
        // Region A replays the entry of object 2 alone and records the
        // segment of object 1 as a hole.
        CorruptedSegmentAction::Skip => {
            opened.unwrap();
            assert_eq!(
                RegionRecoveryState {
                    last_entry_id: entry_id(2, 1),
                    memtable_rows: 2,
                    ..EMPTY_RECOVERY_STATE
                },
                region_recovery_state(&engine, REGION_A).await
            );
            assert_eq!(2, engine.get_region_statistic(REGION_A).unwrap().num_rows);
            assert_eq!(
                vec![WalHole {
                    path,
                    object_seq: 1,
                    min_entry_id: entry_id(1, 1),
                    max_entry_id: entry_id(1, 1),
                }],
                store.wal_holes(&provider(REGION_A)).unwrap()
            );
        }
        // The read fails, so the region does not open.
        CorruptedSegmentAction::Fail => {
            let message = opened.unwrap_err().output_msg();
            assert!(
                message.contains("checksum mismatch"),
                "unexpected error: {message}"
            );
            assert!(!engine.is_region_exists(REGION_A));
            assert!(store.wal_holes(&provider(REGION_A)).unwrap().is_empty());
        }
    }
}

#[tokio::test]
async fn test_open_region_rejects_mismatched_wal_prefix() {
    let mut env = TestEnv::with_prefix("object-store-wal-prefix-mismatch").await;
    let object_store = memory_store();
    let store = open_store(&object_store, AckMode::Durable).await;
    let engine = new_engine(&mut env, store.clone()).await;
    let (table_dir, schema) = create_region(&engine, REGION_A, &[]).await;
    let mut writer = SealedWriter::new(&store);
    writer
        .put_and_seal(&engine, vec![(REGION_A, rows(&schema, 0, 3))])
        .await;

    engine.stop().await.unwrap();
    drop(engine);
    drop(writer);
    drop(store);

    // The process now runs its store under the next generation while the
    // region still persists the prefix it was created with.
    let config = store_config(AckMode::Durable);
    let other_prefix = config.node_prefix(0, STANDALONE_GENERATION + 1);
    let store =
        ObjectStoreLogStore::try_new(object_store.clone(), &config, 0, STANDALONE_GENERATION + 1)
            .await
            .unwrap();
    let engine = new_engine(&mut env, store).await;
    let err = open_region(&engine, REGION_A, &table_dir, &[])
        .await
        .unwrap_err();
    assert_eq!(StatusCode::InvalidArguments, err.status_code());
    let message = err.output_msg();
    assert!(
        message.contains(PREFIX) && message.contains(&other_prefix),
        "unexpected error: {message}"
    );
    assert!(!engine.is_region_exists(REGION_A));
}

/// Opens a store under [`PREFIX`] like [`open_store`] that lets a region
/// open although the prefix does not hold its WAL history.
async fn open_store_accepting_missing_history(
    object_store: &ObjectStore,
) -> Arc<ObjectStoreLogStore> {
    let config = ObjectStoreWalConfig {
        accept_missing_history: true,
        ..store_config(AckMode::Durable)
    };
    ObjectStoreLogStore::try_new(object_store.clone(), &config, 0, STANDALONE_GENERATION)
        .await
        .unwrap()
}

/// Copies every WAL object under [`PREFIX`] from `from` to the same key in
/// `to`, which is how the WAL moves to another location.
async fn copy_wal_objects(from: &ObjectStore, to: &ObjectStore) {
    for object_seq in wal_object_seqs(from).await {
        let path = format!("{PREFIX}/objects/{object_seq:020}.wal");
        let bytes = from.read(&path).await.unwrap().to_vec();
        to.write(&path, bytes).await.unwrap();
    }
}

/// The request that opens a region under [`PREFIX`] and replays its WAL.
fn open_request(table_dir: &str) -> RegionOpenRequest {
    RegionOpenRequest {
        engine: String::new(),
        table_dir: table_dir.to_string(),
        options: wal_options(),
        skip_wal_replay: false,
        path_type: PathType::Bare,
        checkpoint: None,
        requirements: Default::default(),
    }
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

/// Asserts that an open of `region_id` failed because the WAL does not hold
/// its history for `reason`, with the ways forward named.
fn assert_missing_history(err: &BoxedError, region_id: RegionId, reason: &str) {
    assert_eq!(StatusCode::InvalidArguments, err.status_code());
    let message = err.output_msg();
    for expected in [
        &region_id.to_string(),
        PREFIX,
        "the default storage (memory",
        reason,
        "copy every object under the prefix",
        "`[wal].accept_missing_history = true`",
    ] {
        assert!(message.contains(expected), "unexpected error: {message}");
    }
}

#[tokio::test]
async fn test_open_on_an_empty_prefix_requires_accepting_the_missing_history() {
    let mut env = TestEnv::with_prefix("object-store-wal-empty-prefix").await;
    let store = open_store(&memory_store(), AckMode::Durable).await;
    let engine = new_engine(&mut env, store.clone()).await;
    // Region A never writes; region B skips the WAL and flushes its rows.
    let (table_dir_a, schema_a) = create_region(&engine, REGION_A, &[]).await;
    let (table_dir_b, schema_b) = create_region(&engine, REGION_B, &[(SKIP_WAL_KEY, "true")]).await;
    put_rows(&engine, REGION_B, rows(&schema_b, 0, 3)).await;
    flush_region(&engine, REGION_B, None).await;
    let rows_b = scan_rows(&engine, REGION_B).await;

    engine.stop().await.unwrap();
    drop(engine);
    drop(store);

    // A fresh object store holds no object under the prefix. Region B keeps
    // no history in the WAL and opens from its SST alone. Nothing tells
    // region A, which never wrote, from a region whose unflushed entries
    // were left behind, so it does not open, and the refused open deletes
    // the start object it wrote: a second start, as an orchestrator that
    // restarts a failed process makes, finds the prefix empty again and
    // refuses region A the same way.
    let new_location = memory_store();
    for _ in 0..2 {
        let store = open_store(&new_location, AckMode::Durable).await;
        let engine = new_engine(&mut env, store.clone()).await;
        open_region(&engine, REGION_B, &table_dir_b, &[(SKIP_WAL_KEY, "true")])
            .await
            .unwrap();
        assert_eq!(rows_b, scan_rows(&engine, REGION_B).await);
        let err = open_region(&engine, REGION_A, &table_dir_a, &[])
            .await
            .unwrap_err();
        assert_missing_history(
            &err,
            REGION_A,
            "the prefix held no object when the WAL opened",
        );
        assert!(!engine.is_region_exists(REGION_A));
        assert!(wal_object_seqs(&new_location).await.is_empty());
        engine.stop().await.unwrap();
        drop(engine);
        drop(store);
    }

    // Accepting the missing history opens region A without it, counted
    // once; region B is not checked.
    let object_store = memory_store();
    let store = open_store_accepting_missing_history(&object_store).await;
    let engine = new_engine(&mut env, store.clone()).await;
    let accepted = METRIC_OBJECT_STORE_WAL_MISSING_HISTORY_ACCEPTED_TOTAL.get();
    open_region(&engine, REGION_A, &table_dir_a, &[])
        .await
        .unwrap();
    open_region(&engine, REGION_B, &table_dir_b, &[(SKIP_WAL_KEY, "true")])
        .await
        .unwrap();
    assert_eq!(
        accepted + 1,
        METRIC_OBJECT_STORE_WAL_MISSING_HISTORY_ACCEPTED_TOTAL.get()
    );

    assert_eq!(0, latest(&store, REGION_A));
    assert_eq!(0, latest(&store, REGION_B));
    assert_eq!(
        EMPTY_RECOVERY_STATE,
        region_recovery_state(&engine, REGION_A).await
    );
    // Region B keeps the object store provider but wrote nothing to it; its
    // rows come back from the SST alone.
    assert_eq!(provider(REGION_B), region(&engine, REGION_B).provider);
    assert_eq!(
        0,
        region_recovery_state(&engine, REGION_B).await.memtable_rows
    );
    assert_eq!(rows_b, scan_rows(&engine, REGION_B).await);

    // The region is usable and its first entry lands in the first object
    // after the start object, which carries the boundary past the start
    // object.
    let mut writer = SealedWriter::new(&store);
    writer
        .put_and_seal(&engine, vec![(REGION_A, rows(&schema_a, 0, 2))])
        .await;
    wait_for_collection(&store).await;
    assert_eq!(vec![1], wal_object_seqs(&object_store).await);
    assert_eq!(entry_id(1, 1), latest(&store, REGION_A));
    assert_eq!(
        entry_id(1, 1),
        region_recovery_state(&engine, REGION_A).await.last_entry_id
    );
    assert_eq!(2, engine.get_region_statistic(REGION_A).unwrap().num_rows);
}

#[tokio::test]
async fn test_create_after_a_withdrawal_leaves_no_manifest() {
    let mut env = TestEnv::with_prefix("object-store-wal-create-after-withdrawal").await;
    let store = open_store(&memory_store(), AckMode::Durable).await;
    let engine = new_engine(&mut env, store.clone()).await;
    let (table_dir_a, _) = create_region(&engine, REGION_A, &[]).await;
    engine.stop().await.unwrap();
    drop(engine);
    drop(store);

    // On a new location the refused open of region A gives up the start
    // object, and a create of region B after it fails before it writes a
    // manifest.
    let new_location = memory_store();
    let store = open_store(&new_location, AckMode::Durable).await;
    let engine = new_engine(&mut env, store.clone()).await;
    open_region(&engine, REGION_A, &table_dir_a, &[])
        .await
        .unwrap_err();
    let request = CreateRequestBuilder::new()
        .insert_option(WAL_OPTIONS_KEY, &wal_options()[WAL_OPTIONS_KEY])
        .build();
    let err = engine
        .handle_request(REGION_B, RegionRequest::Create(request.clone()))
        .await
        .unwrap_err();
    assert!(
        format!("{err:?}").contains("gave up its start object"),
        "unexpected error: {err:?}"
    );
    assert!(!engine.is_region_exists(REGION_B));
    engine.stop().await.unwrap();
    drop(engine);
    drop(store);

    // The next start creates region B, which a manifest left behind would
    // have turned into an existing region without its history.
    let store = open_store(&new_location, AckMode::Durable).await;
    let engine = new_engine(&mut env, store.clone()).await;
    engine
        .handle_request(REGION_B, RegionRequest::Create(request))
        .await
        .unwrap();
    assert!(engine.is_region_exists(REGION_B));
    engine.stop().await.unwrap();
}

#[tokio::test]
async fn test_reopen_a_region_created_on_an_empty_prefix_without_writes() {
    let mut env = TestEnv::with_prefix("object-store-wal-created-region").await;
    let store = open_store(&memory_store(), AckMode::Durable).await;
    let engine = new_engine(&mut env, store.clone()).await;
    let (table_dir, _) = create_region(&engine, REGION_A, &[]).await;

    // The region's history starts in this store, so it opens again on it,
    // directly and through a catch-up, although it holds no entry.
    close_region(&engine, REGION_A).await;
    open_region(&engine, REGION_A, &table_dir, &[])
        .await
        .unwrap();
    close_region(&engine, REGION_A).await;
    engine
        .handle_request(
            REGION_A,
            RegionRequest::Open(RegionOpenRequest {
                skip_wal_replay: true,
                ..open_request(&table_dir)
            }),
        )
        .await
        .unwrap();
    engine
        .handle_request(
            REGION_A,
            RegionRequest::Catchup(RegionCatchupRequest {
                set_writable: true,
                ..Default::default()
            }),
        )
        .await
        .unwrap();
    assert!(region(&engine, REGION_A).is_writable());
    assert_eq!(
        EMPTY_RECOVERY_STATE,
        region_recovery_state(&engine, REGION_A).await
    );
}

#[tokio::test]
async fn test_moving_the_wal_keeps_the_history_only_with_its_objects() {
    let mut env = TestEnv::with_prefix("object-store-wal-move").await;
    let old_location = memory_store();
    let store = open_store(&old_location, AckMode::Durable).await;
    let engine = new_engine(&mut env, store.clone()).await;
    let (table_dir, schema) = create_region(&engine, REGION_A, &[]).await;
    // Object 1 holds the region's rows, which are never flushed.
    let mut writer = SealedWriter::new(&store);
    writer
        .put_and_seal(&engine, vec![(REGION_A, rows(&schema, 0, 10))])
        .await;
    let rows_before = scan_rows(&engine, REGION_A).await;
    engine.stop().await.unwrap();
    drop(engine);
    drop(writer);
    drop(store);

    // On a location without the objects the region opens neither directly
    // nor through a catch-up after an open that skipped the replay.
    let store = open_store(&memory_store(), AckMode::Durable).await;
    let engine = new_engine(&mut env, store.clone()).await;
    let reason = "the prefix held no object when the WAL opened";
    let err = open_region(&engine, REGION_A, &table_dir, &[])
        .await
        .unwrap_err();
    assert_missing_history(&err, REGION_A, reason);
    engine
        .handle_request(
            REGION_A,
            RegionRequest::Open(RegionOpenRequest {
                skip_wal_replay: true,
                ..open_request(&table_dir)
            }),
        )
        .await
        .unwrap();
    let err = engine
        .handle_request(
            REGION_A,
            RegionRequest::Catchup(RegionCatchupRequest {
                set_writable: true,
                ..Default::default()
            }),
        )
        .await
        .unwrap_err();
    assert_missing_history(&err, REGION_A, reason);
    assert!(!region(&engine, REGION_A).is_writable());
    // A repeated create of the existing region fails the same way rather
    // than creating the region again over its manifest.
    close_region(&engine, REGION_A).await;
    let request = CreateRequestBuilder::new()
        .insert_option(WAL_OPTIONS_KEY, &wal_options()[WAL_OPTIONS_KEY])
        .build();
    assert_eq!(table_dir, request.table_dir);
    let err = engine
        .handle_request(REGION_A, RegionRequest::Create(request))
        .await
        .unwrap_err();
    assert_missing_history(&err, REGION_A, reason);
    assert!(!engine.is_region_exists(REGION_A));
    engine.stop().await.unwrap();
    drop(engine);
    drop(store);

    // Moving the WAL means copying every object: the region replays all of
    // its rows on the new location.
    let new_location = memory_store();
    copy_wal_objects(&old_location, &new_location).await;
    let store = open_store(&new_location, AckMode::Durable).await;
    let engine = new_engine(&mut env, store.clone()).await;
    open_region(&engine, REGION_A, &table_dir, &[])
        .await
        .unwrap();
    assert_eq!(rows_before, scan_rows(&engine, REGION_A).await);
    assert_eq!(10, engine.get_region_statistic(REGION_A).unwrap().num_rows);
}

#[tokio::test]
async fn test_open_region_fails_when_its_watermark_is_above_the_wal_objects() {
    let mut env = TestEnv::with_prefix("object-store-wal-watermark-above").await;
    let store = open_store(&memory_store(), AckMode::Durable).await;
    let engine = new_engine(&mut env, store.clone()).await;
    let (table_dir_a, schema_a) = create_region(&engine, REGION_A, &[]).await;
    let (table_dir_b, schema_b) = create_region(&engine, REGION_B, &[]).await;
    // Region A flushes its entry of object 2, region B is truncated after its
    // entry of object 3.
    let mut writer = SealedWriter::new(&store);
    writer
        .put_and_seal(&engine, vec![(REGION_A, rows(&schema_a, 0, 2))])
        .await;
    writer
        .put_and_seal(&engine, vec![(REGION_A, rows(&schema_a, 2, 4))])
        .await;
    flush_region(&engine, REGION_A, None).await;
    writer
        .put_and_seal(&engine, vec![(REGION_B, rows(&schema_b, 0, 2))])
        .await;
    engine
        .handle_request(
            REGION_B,
            RegionRequest::Truncate(RegionTruncateRequest::All),
        )
        .await
        .unwrap();
    let manifest_a = region(&engine, REGION_A).manifest_ctx.manifest().await;
    assert_eq!(entry_id(2, 1), manifest_a.flushed_entry_id);
    let manifest_b = region(&engine, REGION_B).manifest_ctx.manifest().await;
    assert_eq!(Some(entry_id(3, 1)), manifest_b.truncated_entry_id);
    engine.stop().await.unwrap();
    drop(engine);
    drop(writer);
    drop(store);

    // The other location holds the start object 0 and object 1 of region C.
    let other_location = memory_store();
    let store = open_store(&other_location, AckMode::Durable).await;
    let engine = new_engine(&mut env, store.clone()).await;
    let region_c = RegionId::new(1, 3);
    let (table_dir_c, schema_c) = create_region(&engine, region_c, &[]).await;
    let mut writer = SealedWriter::new(&store);
    writer
        .put_and_seal(&engine, vec![(region_c, rows(&schema_c, 0, 3))])
        .await;
    engine.stop().await.unwrap();
    drop(engine);
    drop(writer);
    drop(store);

    // Its prefix held entries only in objects below 2, so neither watermark
    // can name one of its objects, while region C, whose history it holds,
    // opens. Every start adds a start object, which holds no entry, so a
    // second start after the refused one refuses the regions as well.
    for _ in 0..2 {
        let store = open_store(&other_location, AckMode::Durable).await;
        let engine = new_engine(&mut env, store.clone()).await;
        for (region_id, table_dir, watermark) in [
            (REGION_A, &table_dir_a, entry_id(2, 1)),
            (REGION_B, &table_dir_b, entry_id(3, 1)),
        ] {
            let err = open_region(&engine, region_id, table_dir, &[])
                .await
                .unwrap_err();
            let reason = format!(
                "its watermark, entry {watermark}, names object {}, but the prefix held entries only in objects below 2 when the WAL opened",
                watermark >> 20
            );
            assert_missing_history(&err, region_id, &reason);
            assert!(!engine.is_region_exists(region_id));
        }
        open_region(&engine, region_c, &table_dir_c, &[])
            .await
            .unwrap();
        assert_eq!(3, engine.get_region_statistic(region_c).unwrap().num_rows);
        engine.stop().await.unwrap();
        drop(engine);
        drop(store);
    }
}

#[tokio::test]
async fn test_reopen_after_collection_left_only_the_newest_object() {
    let mut env = TestEnv::with_prefix("object-store-wal-newest-object").await;
    let object_store = memory_store();
    let store = open_store(&object_store, AckMode::Durable).await;
    let engine = new_engine(&mut env, store.clone()).await;
    let (table_dir_a, schema_a) = create_region(&engine, REGION_A, &[]).await;
    let (table_dir_b, schema_b) = create_region(&engine, REGION_B, &[]).await;
    // Region A flushes object 1; object 2 of region B carries the boundary
    // past it, so once region B flushes too only object 2 is left.
    let mut writer = SealedWriter::new(&store);
    writer
        .put_and_seal(&engine, vec![(REGION_A, rows(&schema_a, 0, 2))])
        .await;
    flush_region(&engine, REGION_A, None).await;
    writer
        .put_and_seal(&engine, vec![(REGION_B, rows(&schema_b, 0, 3))])
        .await;
    flush_region(&engine, REGION_B, None).await;
    wait_for_collection(&store).await;
    assert_eq!(vec![2], wal_object_seqs(&object_store).await);
    engine.stop().await.unwrap();
    drop(engine);
    drop(writer);
    drop(store);

    // The object region A flushed is gone, but the newest object stays and
    // keeps the next sequence above it, so both regions open.
    let store = open_store(&object_store, AckMode::Durable).await;
    let engine = new_engine(&mut env, store.clone()).await;
    open_region(&engine, REGION_A, &table_dir_a, &[])
        .await
        .unwrap();
    open_region(&engine, REGION_B, &table_dir_b, &[])
        .await
        .unwrap();
    assert_eq!(
        entry_id(1, 1),
        region_recovery_state(&engine, REGION_A)
            .await
            .flushed_entry_id
    );
    assert_eq!(2, engine.get_region_statistic(REGION_A).unwrap().num_rows);
    assert_eq!(3, engine.get_region_statistic(REGION_B).unwrap().num_rows);
}

#[rstest]
#[case(AckMode::Durable)]
#[case(AckMode::Enqueued)]
#[tokio::test]
async fn test_entry_ids_continue_across_two_restarts(#[case] ack_mode: AckMode) {
    let mut env = TestEnv::with_prefix("object-store-wal-two-restarts").await;
    let object_store = memory_store();
    let store = open_store(&object_store, ack_mode).await;
    let engine = new_engine(&mut env, store.clone()).await;
    let (table_dir, schema) = create_region(&engine, REGION_A, &[]).await;

    // The entry of object 1 is flushed, the entry of object 2 is not.
    let mut writer = SealedWriter::new(&store);
    writer
        .put_and_seal(&engine, vec![(REGION_A, rows(&schema, 0, 2))])
        .await;
    flush_region(&engine, REGION_A, None).await;
    writer
        .put_and_seal(&engine, vec![(REGION_A, rows(&schema, 2, 4))])
        .await;
    assert_eq!(entry_id(2, 1), latest(&store, REGION_A));
    engine.stop().await.unwrap();
    drop(engine);
    drop(writer);
    drop(store);

    // The first restart writes start object 3 and replays the entry of
    // object 2 only.
    let store = open_store(&object_store, ack_mode).await;
    let engine = new_engine(&mut env, store.clone()).await;
    open_region(&engine, REGION_A, &table_dir, &[])
        .await
        .unwrap();
    assert_eq!(
        RegionRecoveryState {
            flushed_entry_id: entry_id(1, 1),
            last_entry_id: entry_id(2, 1),
            topic_latest_entry_id: entry_id(1, 1),
            manifest_flushed_entry_id: entry_id(1, 1),
            memtable_rows: 2,
        },
        region_recovery_state(&engine, REGION_A).await
    );
    assert_eq!(4, engine.get_region_statistic(REGION_A).unwrap().num_rows);

    // The sequence continues at object 4, whose entry is flushed; the entry
    // of object 5 is not.
    let mut writer = SealedWriter::new(&store);
    writer
        .put_and_seal(&engine, vec![(REGION_A, rows(&schema, 4, 6))])
        .await;
    assert_eq!(entry_id(4, 1), latest(&store, REGION_A));
    assert_eq!(
        entry_id(4, 1),
        region_recovery_state(&engine, REGION_A).await.last_entry_id
    );
    flush_region(&engine, REGION_A, None).await;
    writer
        .put_and_seal(&engine, vec![(REGION_A, rows(&schema, 6, 8))])
        .await;
    assert_eq!(entry_id(5, 1), latest(&store, REGION_A));
    let rows_before = scan_rows(&engine, REGION_A).await;
    engine.stop().await.unwrap();
    drop(engine);
    drop(writer);
    drop(store);

    // The second restart replays the entry of object 5 only.
    let store = open_store(&object_store, ack_mode).await;
    let engine = new_engine(&mut env, store.clone()).await;
    open_region(&engine, REGION_A, &table_dir, &[])
        .await
        .unwrap();
    assert_eq!(
        RegionRecoveryState {
            flushed_entry_id: entry_id(4, 1),
            last_entry_id: entry_id(5, 1),
            topic_latest_entry_id: entry_id(4, 1),
            manifest_flushed_entry_id: entry_id(4, 1),
            memtable_rows: 2,
        },
        region_recovery_state(&engine, REGION_A).await
    );
    assert_eq!(entry_id(5, 1), latest(&store, REGION_A));
    assert_eq!(rows_before, scan_rows(&engine, REGION_A).await);
    assert_eq!(8, engine.get_region_statistic(REGION_A).unwrap().num_rows);
    assert_eq!(
        8,
        region(&engine, REGION_A)
            .version_control
            .committed_sequence()
    );
}

/// Sends a put without asserting its result.
async fn try_put(
    engine: &MitoEngine,
    region_id: RegionId,
    rows: Rows,
) -> std::result::Result<(), BoxedError> {
    engine
        .handle_request(
            region_id,
            RegionRequest::Put(RegionPutRequest {
                skip_wal: false,
                rows,
                hint: None,
                partition_expr_version: None,
            }),
        )
        .await
        .map(|_| ())
}

#[tokio::test]
async fn test_reopen_after_a_failed_create_shows_only_the_acknowledged_write() {
    let mut env = TestEnv::with_prefix("object-store-wal-failed-create").await;
    let object_store = memory_store();
    let store = open_store(&object_store, AckMode::Durable).await;
    let engine = new_engine(&mut env, store.clone()).await;
    let (table_dir, schema) = create_region(&engine, REGION_A, &[]).await;
    let key_rows = |value| Rows {
        schema: schema.clone(),
        rows: build_rows_for_key("k", 0, 1, value),
    };

    // Object 1 is stored, but its create is reported as failed, so the write
    // fails.
    let failed = {
        let engine = engine.clone();
        let rows = key_rows(1);
        tokio::spawn(async move { try_put(&engine, REGION_A, rows).await })
    };
    tokio::time::timeout(WAIT, store.wait_for_admitted_appends(1))
        .await
        .unwrap()
        .unwrap();
    store.fail_next_create_after_write();
    store.seal_open_batch().await.unwrap_err();
    assert!(failed.await.unwrap().is_err());
    assert_eq!(vec![0, 1], wal_object_seqs(&object_store).await);
    assert_eq!(0, latest(&store, REGION_A));

    // The same key and timestamp is written again under the same row
    // sequence, and acknowledged from object 2.
    let mut writer = SealedWriter {
        store: store.clone(),
        admitted: 1,
    };
    writer
        .put_and_seal(&engine, vec![(REGION_A, key_rows(2))])
        .await;
    // Object 2 carries the boundary past the start object, which is
    // collected.
    wait_for_collection(&store).await;
    assert_eq!(vec![1, 2], wal_object_seqs(&object_store).await);
    assert_eq!(entry_id(2, 1), latest(&store, REGION_A));
    let region_a = region(&engine, REGION_A);
    assert_eq!(1, region_a.version_control.committed_sequence());
    let expected = "\
+-------+---------+---------------------+
| tag_0 | field_0 | ts                  |
+-------+---------+---------------------+
| k     | 2.0     | 1970-01-01T00:00:00 |
+-------+---------+---------------------+";
    assert_eq!(expected, scan_rows(&engine, REGION_A).await);

    engine.stop().await.unwrap();
    drop(engine);
    drop(writer);
    drop(store);

    // Object 1 is off the chain, so only the acknowledged write replays.
    let store = open_store(&object_store, AckMode::Durable).await;
    let engine = new_engine(&mut env, store.clone()).await;
    open_region(&engine, REGION_A, &table_dir, &[])
        .await
        .unwrap();
    assert_eq!(
        RegionRecoveryState {
            last_entry_id: entry_id(2, 1),
            memtable_rows: 1,
            ..EMPTY_RECOVERY_STATE
        },
        region_recovery_state(&engine, REGION_A).await
    );
    assert_eq!(expected, scan_rows(&engine, REGION_A).await);
}

/// Requests a flush of the region in the background and returns its handle;
/// the result is up to the caller.
fn spawn_flush(
    engine: &MitoEngine,
    region_id: RegionId,
) -> tokio::task::JoinHandle<std::result::Result<(), BoxedError>> {
    let engine = engine.clone();
    tokio::spawn(async move {
        engine
            .handle_request(
                region_id,
                RegionRequest::Flush(RegionFlushRequest {
                    row_group_size: None,
                    reason: None,
                }),
            )
            .await
            .map(|_| ())
    })
}

/// Seals the open batch in the background; with creates held, the seal
/// returns only once they are released.
fn spawn_seal(
    store: &Arc<ObjectStoreLogStore>,
) -> tokio::task::JoinHandle<log_store::error::Result<()>> {
    let store = store.clone();
    tokio::spawn(async move { store.seal_open_batch().await })
}

/// Waits until a testing hook of the store observes the state it waits for.
async fn wait_for(hook: impl Future<Output = log_store::error::Result<()>>) {
    tokio::time::timeout(WAIT, hook)
        .await
        .expect("the store must reach the state")
        .unwrap();
}

/// Tears the engine and its store down as a crash of the process would: the
/// store writes nothing more and fails every caller still waiting for it,
/// then the engine stops. Returns once nothing holds the store any more, so a
/// store reopened on the prefix never runs beside the old one.
async fn crash(engine: MitoEngine, store: Arc<ObjectStoreLogStore>) {
    store.crash().await;
    engine.stop().await.unwrap();
    drop(engine);
    let released = Arc::downgrade(&store);
    drop(store);
    tokio::time::timeout(WAIT, async {
        while released.strong_count() > 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("nothing may hold the crashed store");
}

#[tokio::test]
async fn test_flush_waits_until_the_wal_is_durable() {
    let mut env = TestEnv::with_prefix("object-store-wal-flush-barrier").await;
    let object_store = memory_store();
    let store = open_store(&object_store, AckMode::Enqueued).await;
    let engine = new_engine(&mut env, store.clone()).await;
    let (_, schema) = create_region(&engine, REGION_A, &[]).await;

    // The write is acknowledged on admission; its object is not created
    // while creates are held.
    store.hold_creates();
    put_rows(&engine, REGION_A, rows(&schema, 0, 3)).await;
    assert_eq!(
        RegionRecoveryState {
            last_entry_id: entry_id(1, 1),
            memtable_rows: 3,
            ..EMPTY_RECOVERY_STATE
        },
        region_recovery_state(&engine, REGION_A).await
    );
    assert_eq!(0, latest(&store, REGION_A));

    // The flush writes its SST, then waits for the entry to be durable
    // before it records the entry as flushed in the manifest.
    let flush = spawn_flush(&engine, REGION_A);
    let seal = spawn_seal(&store);
    wait_for(store.wait_for_parked_creates(1)).await;
    wait_for(store.wait_for_durability_waits(1)).await;
    assert!(!flush.is_finished());
    assert!(!seal.is_finished());
    assert_eq!(
        0,
        region_recovery_state(&engine, REGION_A)
            .await
            .manifest_flushed_entry_id
    );
    assert_eq!(vec![0], wal_object_seqs(&object_store).await);

    store.release_creates();
    tokio::time::timeout(WAIT, seal)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    tokio::time::timeout(WAIT, flush)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(entry_id(1, 1), latest(&store, REGION_A));
    assert_eq!(
        RegionRecoveryState {
            flushed_entry_id: entry_id(1, 1),
            last_entry_id: entry_id(1, 1),
            topic_latest_entry_id: entry_id(1, 1),
            manifest_flushed_entry_id: entry_id(1, 1),
            memtable_rows: 0,
        },
        region_recovery_state(&engine, REGION_A).await
    );
    assert_eq!(3, engine.get_region_statistic(REGION_A).unwrap().num_rows);
}

#[tokio::test]
async fn test_enqueued_crash_before_the_object_exists_replays_durable_entries() {
    let mut env = TestEnv::with_prefix("object-store-wal-enqueued-crash").await;
    let object_store = memory_store();
    let store = open_store(&object_store, AckMode::Enqueued).await;
    let engine = new_engine(&mut env, store.clone()).await;
    let (table_dir, schema) = create_region(&engine, REGION_A, &[]).await;

    // The entry is acknowledged and a flush is attempted, but its object is
    // never created: the flush waits and the manifest keeps watermark 0.
    store.hold_creates();
    put_rows(&engine, REGION_A, rows(&schema, 0, 3)).await;
    assert_eq!(
        entry_id(1, 1),
        region_recovery_state(&engine, REGION_A).await.last_entry_id
    );
    let flush = spawn_flush(&engine, REGION_A);
    let seal = spawn_seal(&store);
    wait_for(store.wait_for_parked_creates(1)).await;
    wait_for(store.wait_for_durability_waits(1)).await;
    assert!(!flush.is_finished());
    assert_eq!(
        0,
        region_recovery_state(&engine, REGION_A)
            .await
            .manifest_flushed_entry_id
    );
    assert_eq!(vec![0], wal_object_seqs(&object_store).await);

    // The process dies with the create still held: the waiting flush and
    // the seal fail, and the object is never written.
    store.crash().await;
    assert!(flush.await.unwrap().is_err());
    assert!(seal.await.unwrap().is_err());
    crash(engine, store).await;
    assert_eq!(vec![0], wal_object_seqs(&object_store).await);

    // Nothing is durable, so nothing replays. The restart writes start
    // object 1 and the region's next entry lands in object 2; the lost entry
    // was inside the unpersisted backlog.
    let store = open_store(&object_store, AckMode::Enqueued).await;
    let engine = new_engine(&mut env, store.clone()).await;
    open_region(&engine, REGION_A, &table_dir, &[])
        .await
        .unwrap();
    assert_eq!(
        EMPTY_RECOVERY_STATE,
        region_recovery_state(&engine, REGION_A).await
    );
    assert_eq!(0, latest(&store, REGION_A));
    let mut writer = SealedWriter::new(&store);
    writer
        .put_and_seal(&engine, vec![(REGION_A, rows(&schema, 3, 5))])
        .await;
    assert_eq!(entry_id(2, 1), latest(&store, REGION_A));
    assert_eq!(
        entry_id(2, 1),
        region_recovery_state(&engine, REGION_A).await.last_entry_id
    );
    // Opening the region recorded its watermark, so object 2 carries the
    // boundary past both start objects.
    wait_for_collection(&store).await;
    assert_eq!(vec![2], wal_object_seqs(&object_store).await);
    let rows_before = scan_rows(&engine, REGION_A).await;
    assert_eq!(2, engine.get_region_statistic(REGION_A).unwrap().num_rows);
    drop(writer);
    crash(engine, store).await;

    // The second restart replays the entry that became durable and skips
    // nothing: the manifest never named an entry that was not durable.
    let store = open_store(&object_store, AckMode::Enqueued).await;
    let engine = new_engine(&mut env, store.clone()).await;
    open_region(&engine, REGION_A, &table_dir, &[])
        .await
        .unwrap();
    assert_eq!(
        RegionRecoveryState {
            last_entry_id: entry_id(2, 1),
            memtable_rows: 2,
            ..EMPTY_RECOVERY_STATE
        },
        region_recovery_state(&engine, REGION_A).await
    );
    assert_eq!(entry_id(2, 1), latest(&store, REGION_A));
    assert_eq!(rows_before, scan_rows(&engine, REGION_A).await);
    assert_eq!(2, engine.get_region_statistic(REGION_A).unwrap().num_rows);
}

#[tokio::test]
async fn test_drop_cancels_a_flush_waiting_for_wal_durability() {
    let mut env = TestEnv::with_prefix("object-store-wal-flush-barrier-drop").await;
    let object_store = memory_store();
    let store = open_store(&object_store, AckMode::Enqueued).await;
    let engine = new_engine(&mut env, store.clone()).await;
    let (_, schema) = create_region(&engine, REGION_A, &[]).await;

    // The flush reaches the durability barrier and waits for a create that
    // is held.
    store.hold_creates();
    put_rows(&engine, REGION_A, rows(&schema, 0, 3)).await;
    let flush = spawn_flush(&engine, REGION_A);
    let seal = spawn_seal(&store);
    wait_for(store.wait_for_parked_creates(1)).await;
    wait_for(store.wait_for_durability_waits(1)).await;
    assert!(!flush.is_finished());

    // The drop cancels the flush instead of waiting behind the upload. Its
    // release of the region then waits for its own object, which is created
    // after the upload.
    let dropped = spawn_drop(&engine, REGION_A);
    assert!(
        tokio::time::timeout(WAIT, flush)
            .await
            .unwrap()
            .unwrap()
            .is_err()
    );
    wait_for(store.wait_for_parked_creates(2)).await;
    assert!(!dropped.is_finished());
    assert_eq!(vec![0], wal_object_seqs(&object_store).await);

    store.release_creates();
    tokio::time::timeout(WAIT, dropped)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(!engine.is_region_exists(REGION_A));
    tokio::time::timeout(WAIT, seal)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}

fn spawn_drop(
    engine: &MitoEngine,
    region_id: RegionId,
) -> tokio::task::JoinHandle<std::result::Result<(), BoxedError>> {
    let engine = engine.clone();
    tokio::spawn(async move {
        engine
            .handle_request(
                region_id,
                RegionRequest::Drop(RegionDropRequest {
                    fast_path: false,
                    force: false,
                    partial_drop: false,
                }),
            )
            .await
            .map(|_| ())
    })
}

#[rstest]
#[case::graceful(false, false)]
#[case::abrupt(true, false)]
#[case::failed_release(false, true)]
#[tokio::test]
async fn test_restart_after_a_drop_collects_the_objects_of_the_dropped_region(
    #[case] abrupt: bool,
    #[case] release_fails: bool,
) {
    let mut env = TestEnv::with_prefix("object-store-wal-drop-restart").await;
    let object_store = memory_store();
    let store = open_store(&object_store, AckMode::Durable).await;
    let engine = new_engine(&mut env, store.clone()).await;
    let (_, schema_a) = create_region(&engine, REGION_A, &[]).await;
    let (table_dir_b, schema_b) = create_region(&engine, REGION_B, &[]).await;

    // Object 1 holds entries of both regions and object 2 of region A. The
    // drop releases region A in object 3, and region B, which is not
    // flushed, holds the boundary at object 1.
    let mut writer = SealedWriter::new(&store);
    writer
        .put_and_seal(
            &engine,
            vec![
                (REGION_A, rows(&schema_a, 0, 2)),
                (REGION_B, rows(&schema_b, 0, 3)),
            ],
        )
        .await;
    writer
        .put_and_seal(&engine, vec![(REGION_A, rows(&schema_a, 2, 4))])
        .await;
    if release_fails {
        store.fail_creates();
    }
    let dropped = tokio::time::timeout(WAIT, spawn_drop(&engine, REGION_A))
        .await
        .unwrap()
        .unwrap();
    // A drop whose release fails succeeds all the same, and the stop creates
    // the record in object 4.
    dropped.unwrap();
    assert!(!engine.is_region_exists(REGION_A));
    store.allow_creates();
    wait_for_collection(&store).await;
    let released_in = if release_fails { 4 } else { 3 };
    drop(writer);
    if abrupt {
        crash(engine, store).await;
    } else {
        engine.stop().await.unwrap();
        drop(engine);
        store.stop().await.unwrap();
        drop(store);
    }
    assert_eq!(
        vec![1, 2, released_in],
        wal_object_seqs(&object_store).await
    );

    // Nothing is written before the restart, and region A is never opened
    // again.
    let store = open_store(&object_store, AckMode::Durable).await;
    let engine = new_engine(&mut env, store.clone()).await;
    open_region(&engine, REGION_B, &table_dir_b, &[])
        .await
        .unwrap();

    // Once region B is flushed no object holds a live segment, and the next
    // object carries the boundary past all of them.
    flush_region(&engine, REGION_B, None).await;
    let mut writer = SealedWriter::new(&store);
    writer
        .put_and_seal(&engine, vec![(REGION_B, rows(&schema_b, 3, 5))])
        .await;
    wait_for_collection(&store).await;
    assert_eq!(vec![released_in + 2], wal_object_seqs(&object_store).await);
    assert_eq!(5, engine.get_region_statistic(REGION_B).unwrap().num_rows);
}

#[rstest]
#[case(RegionTruncateRequest::All)]
#[case(RegionTruncateRequest::Unflushed)]
#[tokio::test]
async fn test_enqueued_truncate_waits_until_the_wal_is_durable(
    #[case] request: RegionTruncateRequest,
) {
    let mut env = TestEnv::with_prefix("object-store-wal-truncate-barrier").await;
    let object_store = memory_store();
    let store = open_store(&object_store, AckMode::Enqueued).await;
    let engine = new_engine(&mut env, store.clone()).await;
    let (table_dir, schema) = create_region(&engine, REGION_A, &[]).await;

    // The entry is acknowledged and a truncate is attempted, but its object
    // is never created: the truncate waits and the manifest keeps frontier 0.
    store.hold_creates();
    put_rows(&engine, REGION_A, rows(&schema, 0, 3)).await;
    assert_eq!(
        entry_id(1, 1),
        region_recovery_state(&engine, REGION_A).await.last_entry_id
    );
    let truncate = {
        let engine = engine.clone();
        tokio::spawn(async move {
            engine
                .handle_request(REGION_A, RegionRequest::Truncate(request))
                .await
                .map(|_| ())
        })
    };
    let seal = spawn_seal(&store);
    wait_for(store.wait_for_parked_creates(1)).await;
    wait_for(store.wait_for_durability_waits(1)).await;
    assert!(!truncate.is_finished());
    let manifest = region(&engine, REGION_A).manifest_ctx.manifest().await;
    assert_eq!(0, manifest.flushed_entry_id);
    assert_eq!(None, manifest.truncated_entry_id);
    assert_eq!(vec![0], wal_object_seqs(&object_store).await);

    // The process dies with the create still held: the waiting truncate and
    // the seal fail.
    store.crash().await;
    assert!(truncate.await.unwrap().is_err());
    assert!(seal.await.unwrap().is_err());
    crash(engine, store).await;

    // Nothing is durable and the manifest names no entry, so the region
    // starts over after start object 1, and its next entry becomes durable
    // in object 2.
    let store = open_store(&object_store, AckMode::Enqueued).await;
    let engine = new_engine(&mut env, store.clone()).await;
    open_region(&engine, REGION_A, &table_dir, &[])
        .await
        .unwrap();
    assert_eq!(
        EMPTY_RECOVERY_STATE,
        region_recovery_state(&engine, REGION_A).await
    );
    let mut writer = SealedWriter::new(&store);
    writer
        .put_and_seal(&engine, vec![(REGION_A, rows(&schema, 3, 5))])
        .await;
    assert_eq!(entry_id(2, 1), latest(&store, REGION_A));
    let rows_before = scan_rows(&engine, REGION_A).await;
    drop(writer);
    crash(engine, store).await;

    // The second restart replays the durable entry instead of skipping it.
    let store = open_store(&object_store, AckMode::Enqueued).await;
    let engine = new_engine(&mut env, store.clone()).await;
    open_region(&engine, REGION_A, &table_dir, &[])
        .await
        .unwrap();
    assert_eq!(
        RegionRecoveryState {
            last_entry_id: entry_id(2, 1),
            memtable_rows: 2,
            ..EMPTY_RECOVERY_STATE
        },
        region_recovery_state(&engine, REGION_A).await
    );
    assert_eq!(rows_before, scan_rows(&engine, REGION_A).await);
    assert_eq!(2, engine.get_region_statistic(REGION_A).unwrap().num_rows);
}

#[rstest]
#[case(RegionTruncateRequest::All)]
#[case(RegionTruncateRequest::Unflushed)]
#[tokio::test]
async fn test_enqueued_truncate_completes_once_the_wal_is_durable(
    #[case] request: RegionTruncateRequest,
) {
    let mut env = TestEnv::with_prefix("object-store-wal-truncate-durable").await;
    let object_store = memory_store();
    let store = open_store(&object_store, AckMode::Enqueued).await;
    let engine = new_engine(&mut env, store.clone()).await;
    let (table_dir, schema) = create_region(&engine, REGION_A, &[]).await;
    let truncated_entry_id =
        matches!(request, RegionTruncateRequest::All).then_some(entry_id(1, 1));

    // The truncate waits for the acknowledged entry, then records it once
    // its object is created.
    store.hold_creates();
    put_rows(&engine, REGION_A, rows(&schema, 0, 3)).await;
    let truncate = {
        let engine = engine.clone();
        tokio::spawn(async move {
            engine
                .handle_request(REGION_A, RegionRequest::Truncate(request))
                .await
                .map(|_| ())
        })
    };
    let seal = spawn_seal(&store);
    wait_for(store.wait_for_parked_creates(1)).await;
    wait_for(store.wait_for_durability_waits(1)).await;
    assert!(!truncate.is_finished());
    store.release_creates();
    tokio::time::timeout(WAIT, seal)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    tokio::time::timeout(WAIT, truncate)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let manifest = region(&engine, REGION_A).manifest_ctx.manifest().await;
    assert_eq!(entry_id(1, 1), manifest.flushed_entry_id);
    assert_eq!(truncated_entry_id, manifest.truncated_entry_id);
    assert_eq!(0, engine.get_region_statistic(REGION_A).unwrap().num_rows);
    engine.stop().await.unwrap();
    drop(engine);
    drop(store);

    // The restart replays nothing: the discarded rows stay discarded.
    let store = open_store(&object_store, AckMode::Enqueued).await;
    let engine = new_engine(&mut env, store.clone()).await;
    open_region(&engine, REGION_A, &table_dir, &[])
        .await
        .unwrap();
    assert_eq!(
        RegionRecoveryState {
            flushed_entry_id: entry_id(1, 1),
            last_entry_id: entry_id(1, 1),
            topic_latest_entry_id: entry_id(1, 1),
            manifest_flushed_entry_id: entry_id(1, 1),
            memtable_rows: 0,
        },
        region_recovery_state(&engine, REGION_A).await
    );
    assert_eq!(0, engine.get_region_statistic(REGION_A).unwrap().num_rows);
}

#[tokio::test]
async fn test_flush_does_not_publish_a_frontier_for_a_lost_enqueued_backlog() {
    let mut env = TestEnv::with_prefix("object-store-wal-lost-backlog").await;
    let object_store = memory_store();
    let store = open_store(&object_store, AckMode::Enqueued).await;
    let engine = new_engine(&mut env, store.clone()).await;
    let (_, schema) = create_region(&engine, REGION_A, &[]).await;

    // The entry is acknowledged, then its create fails while stop has begun
    // but the stop command is not handled yet: the backlog is lost.
    store.hold_creates();
    store.fail_creates();
    put_rows(&engine, REGION_A, rows(&schema, 0, 3)).await;
    let seal = spawn_seal(&store);
    wait_for(store.wait_for_parked_creates(1)).await;
    store.begin_stop();
    store.release_creates();
    assert!(
        tokio::time::timeout(WAIT, seal)
            .await
            .unwrap()
            .unwrap()
            .is_err()
    );
    assert_eq!(vec![0], wal_object_seqs(&object_store).await);

    // A flush in that window fails at the barrier instead of recording the
    // lost entry as flushed.
    let flush = spawn_flush(&engine, REGION_A);
    assert!(
        tokio::time::timeout(WAIT, flush)
            .await
            .unwrap()
            .unwrap()
            .is_err()
    );
    assert_eq!(
        0,
        region_recovery_state(&engine, REGION_A)
            .await
            .manifest_flushed_entry_id
    );
    assert!(store.stop().await.is_err());
}

/// An in-memory object store behind the retry layer object storage has in
/// production, whose writes fail with denied access while `denied` is set.
fn denying_store(denied: Arc<AtomicBool>) -> ObjectStore {
    let mock = MockLayerBuilder::default()
        .writer_factory(Arc::new(move |_, _, inner| {
            if denied.load(Ordering::SeqCst) {
                Box::new(DeniedWriter)
            } else {
                inner
            }
        }))
        .build()
        .unwrap();
    with_retry_layers(memory_store().layer(mock))
}

struct DeniedWriter;

impl oio::Write for DeniedWriter {
    async fn write(&mut self, _buffer: mock::Buffer) -> mock::Result<()> {
        Ok(())
    }

    async fn close(&mut self) -> mock::Result<mock::Metadata> {
        Err(mock::Error::new(
            mock::ErrorKind::PermissionDenied,
            "access denied",
        ))
    }

    async fn abort(&mut self) -> mock::Result<()> {
        Ok(())
    }
}

#[tokio::test]
async fn test_enqueued_writes_fail_fast_while_the_object_store_refuses_the_backlog() {
    let mut env = TestEnv::with_prefix("object-store-wal-refused-backlog").await;
    let denied = Arc::new(AtomicBool::new(false));
    let store = open_store(&denying_store(denied.clone()), AckMode::Enqueued).await;
    let engine = new_engine(&mut env, store.clone()).await;
    let (_, schema) = create_region(&engine, REGION_A, &[]).await;

    // The write is acknowledged, then the object store denies the create of
    // its object, which the store keeps.
    denied.store(true, Ordering::SeqCst);
    put_rows(&engine, REGION_A, rows(&schema, 0, 3)).await;
    let seal = spawn_seal(&store);
    tokio::time::timeout(WAIT, async {
        while METRIC_OBJECT_STORE_WAL_FAILING.get() == 0 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("the store must fail");

    // A later write fails at once with a retryable error that names the
    // refusal, and takes no entry id.
    let error = try_put(&engine, REGION_A, rows(&schema, 3, 5))
        .await
        .unwrap_err();
    assert_eq!(StatusCode::StorageUnavailable, error.status_code());
    assert_eq!(RetryHint::Retryable, error.retry_hint());
    assert!(
        format!("{error:?}").contains("PermissionDenied"),
        "{error:?}"
    );
    assert_eq!(
        entry_id(1, 1),
        region_recovery_state(&engine, REGION_A).await.last_entry_id
    );

    // A flush waits for the acknowledged entry instead of failing.
    let flush = spawn_flush(&engine, REGION_A);
    wait_for(store.wait_for_durability_waits(1)).await;
    assert!(!flush.is_finished());

    // Once the object store accepts writes again, the backlog is uploaded,
    // the flush records the entry and writes are accepted again.
    denied.store(false, Ordering::SeqCst);
    tokio::time::timeout(WAIT, seal)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    tokio::time::timeout(WAIT, flush)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    put_rows(&engine, REGION_A, rows(&schema, 3, 5)).await;
    assert_eq!(
        RegionRecoveryState {
            flushed_entry_id: entry_id(1, 1),
            last_entry_id: entry_id(2, 1),
            topic_latest_entry_id: entry_id(1, 1),
            manifest_flushed_entry_id: entry_id(1, 1),
            memtable_rows: 2,
        },
        region_recovery_state(&engine, REGION_A).await
    );
}

/// How the WAL's object store fails a create while a flush waits for it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WalOutage {
    /// Creates are denied, an error that requires intervention, so the store
    /// is failing.
    Refused,
    /// Creates fail with a transient error and are repeated.
    Unreachable,
    /// A create never answers.
    Stalled,
}

impl WalOutage {
    /// Starts the outage on `store`, whose object store denies writes while
    /// `denied` is set.
    fn start(self, store: &ObjectStoreLogStore, denied: &AtomicBool) {
        match self {
            WalOutage::Refused => denied.store(true, Ordering::SeqCst),
            WalOutage::Unreachable => store.fail_creates(),
            WalOutage::Stalled => store.hold_creates(),
        }
    }

    /// Waits until a create of the sealed backlog met the outage: the store
    /// is failing, handled a failed create, or has a create parked.
    async fn wait_until_met(self, store: &ObjectStoreLogStore) {
        match self {
            WalOutage::Refused => wait_for(store.wait_until_failing()).await,
            WalOutage::Unreachable => wait_for(store.wait_for_failed_creates(1)).await,
            WalOutage::Stalled => wait_for(store.wait_for_parked_creates(1)).await,
        }
    }

    /// Whether the log store's stop uploads the backlog: a stalled create
    /// completes once it is released, while the others keep failing and the
    /// stop drops the backlog.
    fn backlog_survives_stop(self) -> bool {
        self == WalOutage::Stalled
    }

    /// Prepares the log store's stop, which waits for the creates in flight:
    /// a stalled create is released. Afterwards the object store accepts
    /// writes again, for the restart.
    async fn stop_store(self, store: &ObjectStoreLogStore, denied: &AtomicBool) -> bool {
        if self == WalOutage::Stalled {
            store.release_creates();
        }
        let stopped = tokio::time::timeout(WAIT, store.stop())
            .await
            .expect("the log store must stop");
        denied.store(false, Ordering::SeqCst);
        stopped.is_ok()
    }
}

async fn sst_files(env: &TestEnv, table_dir: &str) -> Vec<String> {
    env.get_object_store()
        .unwrap()
        .list_with(table_dir)
        .recursive(true)
        .await
        .unwrap()
        .into_iter()
        .map(|entry| entry.path().to_string())
        .filter(|path| path.ends_with(".parquet"))
        .collect()
}

async fn put_rows_skipping_wal(engine: &MitoEngine, region_id: RegionId, rows: Rows) {
    engine
        .handle_request(
            region_id,
            RegionRequest::Put(RegionPutRequest {
                skip_wal: true,
                rows,
                hint: None,
                partition_expr_version: None,
            }),
        )
        .await
        .unwrap();
}

/// Restarts on the WAL objects that `object_store` holds, opens `region_id`
/// and returns the engine and the region's state after its replay.
async fn reopen(
    env: &mut TestEnv,
    object_store: &ObjectStore,
    region_id: RegionId,
    table_dir: &str,
) -> (MitoEngine, RegionRecoveryState) {
    let store = open_store(object_store, AckMode::Enqueued).await;
    let engine = new_engine(env, store).await;
    open_region(&engine, region_id, table_dir, &[])
        .await
        .unwrap();
    let state = region_recovery_state(&engine, region_id).await;
    (engine, state)
}

/// A flush that waits for the WAL to become durable no longer holds up the
/// stop of the engine, whether the WAL's object store refuses its creates,
/// fails them or never answers. The flush is cancelled: it records nothing
/// in the manifest and deletes its SST. After a restart the rows that were
/// durable before the stop replay once, and the backlog does as well when the
/// log store's stop uploaded it; otherwise that stop reports the loss.
#[rstest]
#[case(WalOutage::Refused)]
#[case(WalOutage::Unreachable)]
#[case(WalOutage::Stalled)]
#[tokio::test]
async fn test_engine_stop_cancels_a_flush_waiting_for_the_wal(#[case] outage: WalOutage) {
    common_telemetry::init_default_ut_logging();
    let mut env = TestEnv::with_prefix("object-store-wal-stop-flush").await;
    let denied = Arc::new(AtomicBool::new(false));
    let object_store = denying_store(denied.clone());
    let store = open_store(&object_store, AckMode::Enqueued).await;
    let engine = new_engine(&mut env, store.clone()).await;
    let (table_dir, schema) = create_region(&engine, REGION_A, &[]).await;

    // Object 1 holds the first write; the second waits in the backlog.
    put_rows(&engine, REGION_A, rows(&schema, 0, 3)).await;
    store.seal_open_batch().await.unwrap();
    outage.start(&store, &denied);
    put_rows(&engine, REGION_A, rows(&schema, 3, 5)).await;
    let seal = spawn_seal(&store);
    outage.wait_until_met(&store).await;
    let flush = spawn_flush(&engine, REGION_A);
    wait_for(store.wait_for_durability_waits(1)).await;
    assert!(!flush.is_finished());

    // The region server closes the region, which does not flush it, then
    // stops the engine, before the log store stops.
    close_region(&engine, REGION_A).await;
    tokio::time::timeout(WAIT, engine.stop())
        .await
        .expect("the engine must stop")
        .unwrap();
    let error = tokio::time::timeout(WAIT, flush)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert_eq!(StatusCode::Cancelled, error.status_code(), "{error:?}");
    assert_eq!(Vec::<String>::new(), sst_files(&env, &table_dir).await);

    let survives = outage.backlog_survives_stop();
    assert_eq!(survives, outage.stop_store(&store, &denied).await);
    assert_eq!(survives, seal.await.unwrap().is_ok());
    drop(engine);
    drop(store);

    let (engine, state) = reopen(&mut env, &object_store, REGION_A, &table_dir).await;
    let replayed = if survives { 5 } else { 3 };
    assert_eq!(0, state.manifest_flushed_entry_id);
    assert_eq!(replayed, state.memtable_rows);
    // Three lines of header and one of footer frame the rows.
    assert_eq!(
        replayed as usize + 4,
        scan_rows(&engine, REGION_A).await.lines().count()
    );
    assert_eq!(Vec::<String>::new(), sst_files(&env, &table_dir).await);
}

/// A flush queued behind one that waits for the WAL runs when the engine
/// stops, and gives up at its own wait instead of holding the stop up.
#[tokio::test]
async fn test_engine_stop_runs_a_queued_flush_without_waiting_for_the_wal() {
    common_telemetry::init_default_ut_logging();
    let mut env = TestEnv::with_prefix("object-store-wal-stop-queued-flush").await;
    let object_store = memory_store();
    let store = open_store(&object_store, AckMode::Enqueued).await;
    // One flush runs at a time, so the second waits in the queue.
    let config = MitoConfig {
        num_workers: 2,
        max_background_flushes: 1,
        ..Default::default()
    };
    let engine = env
        .create_engine_with_log_store(config, store.clone())
        .await;
    let (table_dir_a, schema_a) = create_region(&engine, REGION_A, &[]).await;
    let (table_dir_b, schema_b) = create_region(&engine, REGION_B, &[]).await;

    store.hold_creates();
    put_rows(&engine, REGION_A, rows(&schema_a, 0, 3)).await;
    put_rows(&engine, REGION_B, rows(&schema_b, 0, 2)).await;
    let seal = spawn_seal(&store);
    wait_for(store.wait_for_parked_creates(1)).await;
    let flush_a = spawn_flush(&engine, REGION_A);
    wait_for(store.wait_for_durability_waits(1)).await;
    // Scheduling the flush of B freezes its memtable.
    let flush_b = spawn_flush(&engine, REGION_B);
    let region_b = region(&engine, REGION_B);
    tokio::time::timeout(WAIT, async {
        while region_b
            .version_control
            .current()
            .version
            .memtables
            .immutables()
            .is_empty()
        {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("the flush of B must be scheduled");
    assert!(!flush_a.is_finished());
    assert!(!flush_b.is_finished());

    tokio::time::timeout(WAIT, engine.stop())
        .await
        .expect("the engine must stop")
        .unwrap();
    for flush in [flush_a, flush_b] {
        let error = flush.await.unwrap().unwrap_err();
        assert_eq!(StatusCode::Cancelled, error.status_code(), "{error:?}");
    }
    assert_eq!(Vec::<String>::new(), sst_files(&env, &table_dir_a).await);
    assert_eq!(Vec::<String>::new(), sst_files(&env, &table_dir_b).await);

    store.release_creates();
    tokio::time::timeout(WAIT, store.stop())
        .await
        .unwrap()
        .unwrap();
    seal.await.unwrap().unwrap();
    drop(engine);
    drop(region_b);
    drop(store);

    let (engine, state) = reopen(&mut env, &object_store, REGION_A, &table_dir_a).await;
    assert_eq!(0, state.manifest_flushed_entry_id);
    assert_eq!(3, state.memtable_rows);
    open_region(&engine, REGION_B, &table_dir_b, &[])
        .await
        .unwrap();
    let state = region_recovery_state(&engine, REGION_B).await;
    assert_eq!(0, state.manifest_flushed_entry_id);
    assert_eq!(2, state.memtable_rows);
}

/// A stopping region server tells the engine first, then closes every
/// region. Closing a region that holds writes that skipped the WAL flushes
/// it, and that flush gives up its wait for the WAL instead of holding the
/// close up: at once while the WAL fails to upload, and after the grace
/// period of the stop when a create never answers. The close fails and the
/// writes that skipped the WAL are lost.
#[rstest]
#[case(WalOutage::Refused)]
#[case(WalOutage::Unreachable)]
#[case(WalOutage::Stalled)]
#[tokio::test]
async fn test_close_of_a_stopping_engine_gives_up_on_the_wal(#[case] outage: WalOutage) {
    common_telemetry::init_default_ut_logging();
    let mut env = TestEnv::with_prefix("object-store-wal-stop-closing-flush").await;
    let denied = Arc::new(AtomicBool::new(false));
    let object_store = denying_store(denied.clone());
    let store = open_store(&object_store, AckMode::Enqueued).await;
    let engine = new_engine(&mut env, store.clone()).await;
    let (table_dir, schema) = create_region(&engine, REGION_A, &[]).await;

    outage.start(&store, &denied);
    put_rows(&engine, REGION_A, rows(&schema, 0, 3)).await;
    let seal = spawn_seal(&store);
    outage.wait_until_met(&store).await;
    put_rows_skipping_wal(&engine, REGION_A, rows(&schema, 3, 5)).await;

    let began = Instant::now();
    engine.begin_stop();
    let error = tokio::time::timeout(
        WAIT,
        engine.handle_request(
            REGION_A,
            RegionRequest::Close(RegionCloseRequest::default()),
        ),
    )
    .await
    .expect("the close must not wait for the WAL")
    .unwrap_err();
    assert_eq!(StatusCode::Cancelled, error.status_code(), "{error:?}");
    // A failing WAL is given up at once, a create that never answers after
    // the grace period.
    assert_eq!(
        outage == WalOutage::Stalled,
        began.elapsed() >= STOP_DURABILITY_GRACE,
        "{:?}",
        began.elapsed()
    );
    tokio::time::timeout(WAIT, engine.stop())
        .await
        .expect("the engine must stop")
        .unwrap();
    assert_eq!(Vec::<String>::new(), sst_files(&env, &table_dir).await);

    let survives = outage.backlog_survives_stop();
    assert_eq!(survives, outage.stop_store(&store, &denied).await);
    assert_eq!(survives, seal.await.unwrap().is_ok());
    drop(engine);
    drop(store);

    // Only the write that went through the WAL can come back.
    let (_engine, state) = reopen(&mut env, &object_store, REGION_A, &table_dir).await;
    assert_eq!(0, state.manifest_flushed_entry_id);
    assert_eq!(if survives { 3 } else { 0 }, state.memtable_rows);
}

/// Once the engine begins to stop, a flush whose wait for the WAL is already
/// satisfied still commits: closing a region flushes the writes that skipped
/// the WAL as before, both for a region whose WAL entries are durable and
/// for a region that skips the WAL.
#[rstest]
#[case::durable_entries(false)]
#[case::region_skips_wal(true)]
#[tokio::test]
async fn test_close_of_a_stopping_engine_flushes_a_region_whose_wal_is_durable(
    #[case] region_skips_wal: bool,
) {
    let mut env = TestEnv::with_prefix("object-store-wal-stop-durable-flush").await;
    let object_store = memory_store();
    let store = open_store(&object_store, AckMode::Enqueued).await;
    let engine = new_engine(&mut env, store.clone()).await;
    let options: &[(&str, &str)] = if region_skips_wal {
        &[(SKIP_WAL_KEY, "true")]
    } else {
        &[]
    };
    let (table_dir, schema) = create_region(&engine, REGION_A, options).await;

    put_rows(&engine, REGION_A, rows(&schema, 0, 3)).await;
    store.seal_open_batch().await.unwrap();
    put_rows_skipping_wal(&engine, REGION_A, rows(&schema, 3, 5)).await;
    let flushed_entry_id = if region_skips_wal { 0 } else { entry_id(1, 1) };

    engine.begin_stop();
    tokio::time::timeout(
        WAIT,
        engine.handle_request(
            REGION_A,
            RegionRequest::Close(RegionCloseRequest::default()),
        ),
    )
    .await
    .expect("the close must not wait")
    .unwrap();
    engine.stop().await.unwrap();
    assert_eq!(1, sst_files(&env, &table_dir).await.len());
    store.stop().await.unwrap();
    drop(engine);
    drop(store);

    let store = open_store(&object_store, AckMode::Enqueued).await;
    let engine = new_engine(&mut env, store).await;
    open_region(&engine, REGION_A, &table_dir, options)
        .await
        .unwrap();
    let state = region_recovery_state(&engine, REGION_A).await;
    assert_eq!(flushed_entry_id, state.manifest_flushed_entry_id);
    assert_eq!(0, state.memtable_rows);
    assert_eq!(5, engine.get_region_statistic(REGION_A).unwrap().num_rows);
}

/// With a healthy WAL, a stopping engine keeps every write. The WAL drains
/// once the engine begins to stop, so the write still in its open batch then
/// and the writes admitted afterwards become durable however long the seal
/// interval is, before any flush waits for them. A region holding writes that
/// skipped the WAL is then flushed when it closes, even when the close reaches
/// its wait for the WAL after the grace period of the stop, as a close behind
/// slow flushes would.
#[tokio::test]
async fn test_close_of_a_stopping_engine_keeps_writes_while_the_wal_is_healthy() {
    common_telemetry::init_default_ut_logging();
    let mut env = TestEnv::with_prefix("object-store-wal-stop-healthy-flush").await;
    let object_store = memory_store();
    let store = open_store(&object_store, AckMode::Enqueued).await;
    let engine = new_engine(&mut env, store.clone()).await;
    let (table_dir, schema) = create_region(&engine, REGION_A, &[]).await;

    put_rows(&engine, REGION_A, rows(&schema, 0, 3)).await;
    assert_eq!(0, latest(&store, REGION_A));

    let began = Instant::now();
    engine.begin_stop();
    // The store never seals a batch on its own.
    wait_for(store.wait_durable(&provider(REGION_A), entry_id(1, 1))).await;
    // The region takes writes until it closes; the next WAL write is sealed
    // right after its admission.
    put_rows(&engine, REGION_A, rows(&schema, 3, 5)).await;
    wait_for(store.wait_durable(&provider(REGION_A), entry_id(2, 1))).await;
    put_rows_skipping_wal(&engine, REGION_A, rows(&schema, 5, 7)).await;

    tokio::time::sleep_until((began + STOP_DURABILITY_GRACE).into()).await;
    tokio::time::timeout(
        WAIT,
        engine.handle_request(
            REGION_A,
            RegionRequest::Close(RegionCloseRequest::default()),
        ),
    )
    .await
    .expect("the close must not wait for the WAL")
    .unwrap();
    engine.stop().await.unwrap();
    assert_eq!(1, sst_files(&env, &table_dir).await.len());
    store.stop().await.unwrap();
    drop(engine);
    drop(store);

    let (engine, state) = reopen(&mut env, &object_store, REGION_A, &table_dir).await;
    assert_eq!(entry_id(2, 1), state.manifest_flushed_entry_id);
    assert_eq!(0, state.memtable_rows);
    assert_eq!(7, engine.get_region_statistic(REGION_A).unwrap().num_rows);
}

/// A flush of a stopping engine whose WAL already stopped, so that its wait
/// fails at once, fails as cancelled like a wait that gives up, and leaves no
/// SST behind; the WAL write was never uploaded, so no row comes back.
#[tokio::test]
async fn test_close_of_a_stopping_engine_cancels_a_flush_on_a_stopped_wal() {
    let mut env = TestEnv::with_prefix("object-store-wal-stop-stopped-wal").await;
    let object_store = memory_store();
    let store = open_store(&object_store, AckMode::Enqueued).await;
    let engine = new_engine(&mut env, store.clone()).await;
    let (table_dir, schema) = create_region(&engine, REGION_A, &[]).await;

    put_rows(&engine, REGION_A, rows(&schema, 0, 3)).await;
    put_rows_skipping_wal(&engine, REGION_A, rows(&schema, 3, 5)).await;
    store.crash().await;

    engine.begin_stop();
    let error = tokio::time::timeout(
        WAIT,
        engine.handle_request(
            REGION_A,
            RegionRequest::Close(RegionCloseRequest::default()),
        ),
    )
    .await
    .expect("the close must not wait for the WAL")
    .unwrap_err();
    assert_eq!(StatusCode::Cancelled, error.status_code(), "{error:?}");
    engine.stop().await.unwrap();
    assert_eq!(Vec::<String>::new(), sst_files(&env, &table_dir).await);
    drop(engine);
    drop(store);

    let (_engine, state) = reopen(&mut env, &object_store, REGION_A, &table_dir).await;
    assert_eq!(0, state.manifest_flushed_entry_id);
    assert_eq!(0, state.memtable_rows);
}
