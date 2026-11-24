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

//! Model-based (MBT) counterpart of the fixed-trace regression in
//! [`repartition_dup_row_test`](super::repartition_dup_row_test).
//!
//! # What is modeled
//!
//! A repartition (`RemapManifests`) hands the *same* SST `file_id` to several live
//! regions. Storage does not mark a file as co-owned, so one holder can materialize
//! that file into a brand-new `file_id` (region compaction) while its co-holder keeps
//! the original. Merging the holders back keeps both copies, because the remap dedups
//! by `file_id` and the copies carry different ids. The logical row is then visible
//! twice.
//!
//! The in-tree regression pins one hand-written trace of that sequence. This module
//! pins the *class*: it keeps an independent ledger of every committed write event and
//! checks, after every published step, that a plain region scan of the current live set
//! shows each committed event exactly once.
//!
//! # Event identity
//!
//! Every write event carries an immutable `event_id`, stored as a normal projectable
//! `Int64` field column inside the real SST (`k`, `payload`, `event_id`, `ts`). The
//! ledger records `(event_id, partition_key, ts, payload)` per event. `(PK, ts)` is not
//! an event identity: an append-only region legally holds two events with the same key
//! and timestamp, and the random traces generate exactly that case.
//!
//! # Oracle (and the two anti-oracles)
//!
//! After every published step the harness scans the regions of the published routing and
//! asserts
//!
//! 1. the multiset of visible `event_id`s equals the ledger's committed multiset, each
//!    event exactly once (a multiset, not a count and not a sum, so a lost event and a
//!    duplicated event are both visible, including when their payload is zero);
//! 2. every live region only shows events whose partition key is inside that region's
//!    published domain, so the union in (1) cannot be satisfied by misplacing rows.
//!
//! It deliberately does **not** assert that two live regions hold different `file_id`s
//! (a legal split hands the same object to both) and does not compare `file_id`s inside
//! one manifest (a broken merge that keeps two copies of one file passes such a check).
//!
//! # Layering
//!
//! Physical objects, per-holder region references and the published routing are three
//! separate views. Only immutable snapshots ([`SstRef`]) cross a layer boundary; no layer
//! ever receives a `FileMeta` that another layer mutates.
//!
//! # Waiting for publication
//!
//! Nothing here sleeps. `put_rows`, `Flush`, `Compact`, `set_region_role_state_gracefully`,
//! `remap_manifests`, `Create` and `ApplyStagingManifest` are all synchronous worker
//! requests: when they return, the change is applied to that region's manifest and
//! version. A split/merge step publishes every target region (create -> enter staging ->
//! apply staging manifest) before the routing switches and the oracle runs.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use api::v1::helper::row;
use api::v1::region::{StrictWindow, compact_request};
use api::v1::value::ValueData;
use api::v1::{Rows, SemanticType};
use common_recordbatch::RecordBatches;
use datatypes::arrow::array::AsArray;
use datatypes::arrow::datatypes::Int64Type;
use datatypes::prelude::ConcreteDataType;
use datatypes::schema::ColumnSchema;
use datatypes::value::Value;
use partition::expr::{PartitionExpr, col};
use store_api::metadata::ColumnMetadata;
use store_api::region_engine::{RegionEngine, RemapManifestsRequest, SettableRegionRoleState};
use store_api::region_request::{
    ApplyStagingManifestRequest, EnterStagingRequest, PathType, RegionCompactRequest,
    RegionCreateRequest, RegionFlushRequest, RegionRequest, StagingPartitionDirective,
};
use store_api::storage::{FileId, RegionId, ScanRequest};

use crate::config::MitoConfig;
use crate::engine::{MITO_ENGINE_NAME, MitoEngine};
use crate::gc::GcConfig;
use crate::test_util::{TestEnv, put_rows, rows_schema};

/// Number of distinct partition-key values (`k00..k09`).
const KEY_COUNT: usize = 10;
/// Wide enough to cover every timestamp the traces write.
const COMPACTION_WINDOW_SECONDS: i64 = 3600;
/// Length of one random trace.
const PLAN_LEN: usize = 12;
/// Fixed seeds; each seed is one independently reproducible trace.
const RANDOM_SEEDS: [u64; 3] = [0x5EED_0001, 0x5EED_0002, 0x5EED_0003];

/// Canonical name of the partition key value `idx`.
fn key_name(idx: usize) -> String {
    format!("k{idx:02}")
}

/// A half-open key domain `[lo, hi)` over key indices.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Domain {
    lo: usize,
    hi: usize,
}

impl Domain {
    /// The whole table domain.
    const FULL: Domain = Domain {
        lo: 0,
        hi: KEY_COUNT,
    };

    fn new(lo: usize, hi: usize) -> Domain {
        assert!(lo <= hi && hi <= KEY_COUNT, "invalid domain [{lo}, {hi})");
        Domain { lo, hi }
    }

    /// The partition expression `k >= key(lo) AND k < key(hi)`.
    ///
    /// `key(KEY_COUNT)` is `"k10"`, which sorts after every real key, so the full domain
    /// needs no unbounded upper bound.
    fn expr(&self) -> PartitionExpr {
        col("k")
            .gt_eq(Value::String(key_name(self.lo).into()))
            .and(col("k").lt(Value::String(key_name(self.hi).into())))
    }

    fn expr_json(&self) -> String {
        self.expr().as_json_str().unwrap()
    }

    fn contains_key(&self, key: usize) -> bool {
        self.lo <= key && key < self.hi
    }
}

/// One committed write event. The ledger's unit of truth.
#[derive(Debug, Clone, Copy, PartialEq)]
struct Event {
    event_id: i64,
    key: usize,
    ts: i64,
    payload: f64,
}

/// A row the trace generator planned to write; becomes an [`Event`] once committed.
#[derive(Debug, Clone, Copy)]
struct PlannedRow {
    key: usize,
    ts: i64,
    payload: f64,
}

/// Independent model of all committed events.
#[derive(Debug, Default)]
struct Ledger {
    next_event_id: i64,
    events: BTreeMap<i64, Event>,
}

impl Ledger {
    /// Reserves an immutable event id.
    ///
    /// The id is a column value inside the row, so it must be known before the write; the
    /// event is only *recorded* once that write has committed.
    fn reserve(&mut self) -> i64 {
        let event_id = self.next_event_id;
        self.next_event_id += 1;
        event_id
    }

    /// Records a committed event under an id obtained from [`Ledger::reserve`].
    fn commit(&mut self, event_id: i64, planned: PlannedRow) -> Event {
        let event = Event {
            event_id,
            key: planned.key,
            ts: planned.ts,
            payload: planned.payload,
        };
        assert!(
            self.events.insert(event_id, event).is_none(),
            "event ids must never be reused"
        );
        event
    }

    /// The expected visible multiset: every committed event exactly once.
    fn expected(&self) -> BTreeMap<i64, usize> {
        self.events.keys().map(|id| (*id, 1)).collect()
    }

    fn ids_in(&self, domain: Domain) -> BTreeSet<i64> {
        self.events
            .values()
            .filter(|event| domain.contains_key(event.key))
            .map(|event| event.event_id)
            .collect()
    }

    fn describe(&self, event_id: i64) -> String {
        match self.events.get(&event_id) {
            Some(event) => format!(
                "event_id {} (k={}, ts={}, payload={})",
                event.event_id,
                key_name(event.key),
                event.ts,
                event.payload
            ),
            None => format!("event_id {event_id} (not in ledger)"),
        }
    }
}

/// Layer 1: one physical SST object, as observed through the manifests.
///
/// An immutable snapshot: no layer below ever receives a live `FileMeta`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SstRef {
    file_id: FileId,
    /// The region that physically wrote the object (not necessarily its holder).
    owner: RegionId,
    level: u8,
    num_rows: u64,
}

/// Layer 2: one region's own manifest view of the objects it references.
#[derive(Debug, Default, Clone)]
struct HolderView {
    files: BTreeMap<FileId, SstRef>,
}

/// Layer 1 aggregate: which holders reference each physical object.
///
/// Derived by observing the per-holder views; an object referenced by more than one
/// holder is *not* an error by itself (a legal split produces exactly that).
#[derive(Debug, Default, Clone)]
struct PhysicalView {
    holders_of: BTreeMap<FileId, BTreeSet<RegionId>>,
}

/// One operation of a trace. Generated up front so a seed replays identically.
#[derive(Debug, Clone)]
enum Op {
    /// Append these rows to the live region that owns each row's partition key.
    Write { rows: Vec<PlannedRow> },
    /// Flush every live region that has unflushed rows.
    Flush,
    /// Split one live region at a key boundary.
    Split { choice: usize },
    /// Merge every live region into one whole-domain region.
    Merge,
    /// Region compaction on one live region with a wide strict window.
    Compact { choice: usize },
}

impl Op {
    fn describe(&self) -> String {
        match self {
            Op::Write { rows } => {
                let keys: Vec<String> = rows.iter().map(|r| key_name(r.key)).collect();
                format!("write {} rows (keys {keys:?})", rows.len())
            }
            Op::Flush => "flush".to_string(),
            Op::Split { choice } => format!("split (choice {choice})"),
            Op::Merge => "merge".to_string(),
            Op::Compact { choice } => format!("region compaction (choice {choice})"),
        }
    }
}

/// splitmix64: dependency free and platform independent, so a seed reproduces a trace.
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Rng {
        Rng(seed)
    }

    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn below(&mut self, bound: usize) -> usize {
        (self.next_u64() % bound as u64) as usize
    }
}

/// The real engine plus the three views and the ledger.
struct StorageModel {
    /// Dropped before `_env`, so the engine never outlives its data directory.
    engine: MitoEngine,
    request: RegionCreateRequest,
    schema: Vec<api::v1::ColumnSchema>,
    ledger: Ledger,
    next_region_seq: u32,

    /// Layer 3 (published routing): the regions that serve the table, with their domain.
    routing: BTreeMap<RegionId, Domain>,
    /// Layer 2 (per-holder references): every region ever created, live or retired.
    holders: BTreeMap<RegionId, HolderView>,
    /// Layer 1 (physical objects): object id -> holders, observed from layer 2.
    physical: PhysicalView,

    /// Every region the harness created, live or retired.
    tracked: BTreeSet<RegionId>,
    /// Live regions with unflushed rows in their memtable.
    dirty: BTreeSet<RegionId>,

    /// Keeps the temporary data directory alive for the whole model.
    _env: TestEnv,
}

impl StorageModel {
    async fn new(prefix: &str) -> StorageModel {
        let mut env = TestEnv::with_prefix(prefix).await;
        let engine = env
            .create_engine(MitoConfig {
                // No uncontrolled background compaction: every compaction below is requested.
                schedule_compaction_after_edit: false,
                // GC on (the production setting) with the default lingering time, so a
                // co-holder's purge unlinks but does not delete a shared object mid-test.
                gc: GcConfig {
                    enable: true,
                    ..Default::default()
                },
                // Plain (non-flat) Parquet SSTs.
                default_flat_format: false,
                ..Default::default()
            })
            .await;
        let request = create_request(Domain::FULL);
        let schema = rows_schema(&request);
        let mut model = StorageModel {
            engine,
            request,
            schema,
            ledger: Ledger::default(),
            next_region_seq: 1,
            routing: BTreeMap::new(),
            holders: BTreeMap::new(),
            physical: PhysicalView::default(),
            tracked: BTreeSet::new(),
            dirty: BTreeSet::new(),
            _env: env,
        };

        let region_id = model.alloc_region_id();
        model
            .engine
            .handle_request(region_id, RegionRequest::Create(model.request.clone()))
            .await
            .unwrap();
        model.tracked.insert(region_id);
        model.routing.insert(region_id, Domain::FULL);
        model.refresh_views().await;
        model
    }

    fn alloc_region_id(&mut self) -> RegionId {
        // Table id 1, monotonically increasing region sequence.
        let region_id = RegionId::new(1, self.next_region_seq);
        self.next_region_seq += 1;
        region_id
    }

    fn live_regions(&self) -> Vec<RegionId> {
        self.routing.keys().copied().collect()
    }

    fn domain_of(&self, region_id: RegionId) -> Domain {
        *self
            .routing
            .get(&region_id)
            .unwrap_or_else(|| panic!("region {region_id} is not published"))
    }

    fn holder_files(&self, region_id: RegionId) -> BTreeSet<FileId> {
        self.holders
            .get(&region_id)
            .map(|view| view.files.keys().copied().collect())
            .unwrap_or_default()
    }

    fn sst_ref(&self, region_id: RegionId, file_id: FileId) -> SstRef {
        *self.holders[&region_id]
            .files
            .get(&file_id)
            .unwrap_or_else(|| panic!("region {region_id} does not reference {file_id:?}"))
    }

    /// Refreshes layer 2 from the regions' persisted manifests, then rebuilds layer 1 from
    /// layer 2. Values are copied into [`SstRef`] snapshots, never shared.
    async fn refresh_views(&mut self) {
        let mut holders = BTreeMap::new();
        for region_id in self.tracked.clone() {
            let region = self
                .engine
                .get_region(region_id)
                .unwrap_or_else(|| panic!("region {region_id} disappeared"));
            let manifest = region.manifest_ctx.manifest().await;
            let mut view = HolderView::default();
            for (file_id, meta) in &manifest.files {
                view.files.insert(
                    *file_id,
                    SstRef {
                        file_id: *file_id,
                        owner: meta.region_id,
                        level: meta.level,
                        num_rows: meta.num_rows,
                    },
                );
            }
            holders.insert(region_id, view);
        }

        let mut physical = PhysicalView::default();
        for (region_id, view) in &holders {
            for file_id in view.files.keys() {
                physical
                    .holders_of
                    .entry(*file_id)
                    .or_default()
                    .insert(*region_id);
            }
        }

        self.holders = holders;
        self.physical = physical;
    }

    /// A plain region scan, as a multiset of visible `event_id`s.
    async fn scan_counts(&self, region_id: RegionId) -> BTreeMap<i64, usize> {
        let stream = self
            .engine
            .scan_to_stream(region_id, ScanRequest::default())
            .await
            .unwrap();
        let batches = RecordBatches::try_collect(stream).await.unwrap();
        let mut counts = BTreeMap::new();
        for batch in batches.iter() {
            let ids = batch
                .column_by_name("event_id")
                .expect("a plain scan must project the event_id column")
                .as_primitive::<Int64Type>();
            for i in 0..batch.num_rows() {
                *counts.entry(ids.value(i)).or_default() += 1;
            }
        }
        counts
    }

    /// The oracle. Returns a report when the invariant is violated, `None` otherwise.
    async fn check(&mut self, ctx: &str) -> Option<String> {
        self.refresh_views().await;

        let live = self.live_regions();
        let mut visible: BTreeMap<i64, usize> = BTreeMap::new();
        let mut per_region: Vec<(RegionId, BTreeMap<i64, usize>)> = Vec::new();
        let mut misplaced: Vec<String> = Vec::new();
        for region_id in &live {
            let domain = self.domain_of(*region_id);
            let counts = self.scan_counts(*region_id).await;
            let allowed = self.ledger.ids_in(domain);
            for event_id in counts.keys() {
                if !allowed.contains(event_id) {
                    misplaced.push(format!(
                        "region {} (domain [{}, {})) shows {} which the ledger does not place \
                         in that domain",
                        region_id,
                        key_name(domain.lo),
                        key_name(domain.hi),
                        self.ledger.describe(*event_id)
                    ));
                }
            }
            for (event_id, count) in &counts {
                *visible.entry(*event_id).or_default() += count;
            }
            per_region.push((*region_id, counts));
        }

        let expected = self.ledger.expected();
        let duplicated: Vec<String> = visible
            .iter()
            .filter(|(_, count)| **count > 1)
            .map(|(event_id, count)| format!("{} x{count}", self.ledger.describe(*event_id)))
            .collect();
        let missing: Vec<String> = expected
            .keys()
            .filter(|event_id| !visible.contains_key(*event_id))
            .map(|event_id| self.ledger.describe(*event_id))
            .collect();
        let unexpected: Vec<String> = visible
            .keys()
            .filter(|event_id| !expected.contains_key(*event_id))
            .map(|event_id| self.ledger.describe(*event_id))
            .collect();

        if duplicated.is_empty()
            && missing.is_empty()
            && unexpected.is_empty()
            && misplaced.is_empty()
        {
            return None;
        }

        let physical: Vec<String> = self
            .physical
            .holders_of
            .iter()
            .map(|(file_id, holders)| format!("{file_id:?} held by {holders:?}"))
            .collect();
        let references: Vec<String> = self
            .holders
            .iter()
            .map(|(region_id, view)| {
                let files: Vec<String> = view
                    .files
                    .values()
                    .map(|sst| {
                        format!(
                            "{:?}(owner={}, L{}, rows={})",
                            sst.file_id, sst.owner, sst.level, sst.num_rows
                        )
                    })
                    .collect();
                format!("region {region_id} -> [{files:?}]")
            })
            .collect();

        Some(format!(
            "VISIBILITY INVARIANT VIOLATED [{ctx}]\n\
             \x20 published routing: {live:?}\n\
             \x20 visible multiset over the published regions (event_id -> count): {visible:?}\n\
             \x20 ledger expectation (event_id -> count): {expected:?}\n\
             \x20 duplicated event ids: {duplicated:?}\n\
             \x20 missing event ids: {missing:?}\n\
             \x20 unexpected event ids: {unexpected:?}\n\
             \x20 misplaced rows: {misplaced:?}\n\
             \x20 per-region visible multisets: {per_region:?}\n\
             \x20 physical objects: {physical:?}\n\
             \x20 holder references: {references:?}"
        ))
    }

    async fn assert_visible(&mut self, ctx: &str) {
        if let Some(report) = self.check(ctx).await {
            panic!("{report}");
        }
    }

    /// Appends rows to the live region that owns each row's partition key.
    ///
    /// Publication: `put_rows` returns after the row is committed (WAL + memtable), and the
    /// ledger is updated only afterwards, so a crash between the two can only look like a
    /// missing commit, never a phantom one.
    async fn write(&mut self, rows: &[PlannedRow]) -> Vec<Event> {
        // The event id travels inside the SST, so it is reserved before the write and
        // only recorded in the ledger after the write committed.
        let mut by_region: BTreeMap<RegionId, Vec<(i64, PlannedRow, api::v1::Row)>> =
            BTreeMap::new();
        for planned in rows {
            let event_id = self.ledger.reserve();
            let region_id = self
                .routing
                .iter()
                .find(|(_, domain)| domain.contains_key(planned.key))
                .map(|(region_id, _)| *region_id)
                .unwrap_or_else(|| {
                    panic!(
                        "published routing {:?} does not cover key {}",
                        self.routing,
                        key_name(planned.key)
                    )
                });
            by_region.entry(region_id).or_default().push((
                event_id,
                *planned,
                event_row(event_id, *planned),
            ));
        }

        let mut committed = Vec::new();
        for (region_id, pending) in by_region {
            let values: Vec<api::v1::Row> = pending.iter().map(|(_, _, row)| row.clone()).collect();
            put_rows(
                &self.engine,
                region_id,
                Rows {
                    schema: self.schema.clone(),
                    rows: values,
                },
            )
            .await;
            for (event_id, planned, _) in pending {
                committed.push(self.ledger.commit(event_id, planned));
            }
            self.dirty.insert(region_id);
        }
        committed
    }

    /// Flushes the regions that hold unflushed rows.
    async fn flush_dirty(&mut self) -> usize {
        let dirty: Vec<RegionId> = self.dirty.iter().copied().collect();
        for region_id in &dirty {
            self.engine
                .handle_request(
                    *region_id,
                    RegionRequest::Flush(RegionFlushRequest::default()),
                )
                .await
                .unwrap();
            self.dirty.remove(region_id);
        }
        dirty.len()
    }

    async fn flush_region(&mut self, region_id: RegionId) {
        if self.dirty.remove(&region_id) {
            self.engine
                .handle_request(
                    region_id,
                    RegionRequest::Flush(RegionFlushRequest::default()),
                )
                .await
                .unwrap();
        }
    }

    /// Splits `source` at key boundary `cut`: `[lo, cut)` and `[cut, hi)`.
    ///
    /// Publication: the coordinator's staging manifests are applied to both new regions
    /// (each `ApplyStagingManifest` is a synchronous request that installs the layout and
    /// exits staging) before the routing switches to them.
    async fn split(&mut self, source: RegionId, cut: usize) -> (RegionId, RegionId) {
        let domain = self.domain_of(source);
        assert!(
            domain.lo < cut && cut < domain.hi,
            "cut {} must be strictly inside [{}, {})",
            key_name(cut),
            key_name(domain.lo),
            key_name(domain.hi)
        );
        // A split remaps persisted manifests, so the source's rows must be in SSTs.
        self.flush_dirty().await;

        let left_domain = Domain::new(domain.lo, cut);
        let right_domain = Domain::new(cut, domain.hi);
        let left = self.alloc_region_id();
        let right = self.alloc_region_id();

        self.engine
            .set_region_role_state_gracefully(source, SettableRegionRoleState::StagingLeader)
            .await
            .unwrap();
        let response = self
            .engine
            .remap_manifests(RemapManifestsRequest {
                region_id: source,
                input_regions: vec![source],
                region_mapping: [(source, vec![left, right])].into_iter().collect(),
                new_partition_exprs: [
                    (left, left_domain.expr_json()),
                    (right, right_domain.expr_json()),
                ]
                .into_iter()
                .collect(),
            })
            .await
            .unwrap();

        for (region_id, target_domain) in [(left, left_domain), (right, right_domain)] {
            self.create_and_enter_staging(region_id, target_domain)
                .await;
            self.apply_staging_manifest(region_id, target_domain, source, &response.manifest_paths)
                .await;
        }

        self.tracked.insert(left);
        self.tracked.insert(right);
        self.routing.remove(&source);
        self.routing.insert(left, left_domain);
        self.routing.insert(right, right_domain);
        self.refresh_views().await;
        (left, right)
    }

    /// Creates a region and puts it into staging state with `domain` as its partition expr.
    ///
    /// Both requests are synchronous worker requests, so the region is listable and in
    /// staging state when this returns.
    async fn create_and_enter_staging(&mut self, region_id: RegionId, domain: Domain) {
        self.engine
            .handle_request(region_id, RegionRequest::Create(self.request.clone()))
            .await
            .unwrap();
        self.engine
            .handle_request(
                region_id,
                RegionRequest::EnterStaging(EnterStagingRequest {
                    partition_directive: StagingPartitionDirective::UpdatePartitionExpr(
                        domain.expr_json(),
                    ),
                }),
            )
            .await
            .unwrap();
    }

    /// Applies the coordinator's staging manifest to a staged region, which exits staging.
    ///
    /// `central_region_id` is the staging region that owns the staging storage the
    /// coordinator wrote the manifest into (the split source, or the merged region itself).
    /// `ApplyStagingManifest` is a synchronous request: when it returns the region has
    /// installed the layout, so the published routing may switch to it.
    async fn apply_staging_manifest(
        &mut self,
        region_id: RegionId,
        domain: Domain,
        central_region_id: RegionId,
        manifest_paths: &HashMap<RegionId, String>,
    ) {
        let manifest_path = manifest_paths
            .get(&region_id)
            .unwrap_or_else(|| panic!("coordinator published no manifest for {region_id}"))
            .clone();
        self.engine
            .handle_request(
                region_id,
                RegionRequest::ApplyStagingManifest(ApplyStagingManifestRequest {
                    partition_expr: domain.expr_json(),
                    central_region_id,
                    manifest_path,
                }),
            )
            .await
            .unwrap();
    }

    /// Merges every published region into one new whole-domain region.
    ///
    /// Publication: the merged region is created, consumes the coordinator's staging
    /// manifest (a synchronous request) and only then becomes the sole published region.
    async fn merge_all(&mut self) -> RegionId {
        let inputs = self.live_regions();
        assert!(
            inputs.len() >= 2,
            "a merge needs at least two live regions, got {inputs:?}"
        );
        self.flush_dirty().await;

        let merged = self.alloc_region_id();
        // The merged region is the coordinator's staging owner, so it has to exist and be
        // in staging state before the remap runs (the source does that job for a split).
        self.create_and_enter_staging(merged, Domain::FULL).await;
        let response = self
            .engine
            .remap_manifests(RemapManifestsRequest {
                region_id: merged,
                input_regions: inputs.clone(),
                region_mapping: inputs
                    .iter()
                    .map(|region_id| (*region_id, vec![merged]))
                    .collect(),
                new_partition_exprs: [(merged, Domain::FULL.expr_json())].into_iter().collect(),
            })
            .await
            .unwrap();

        self.apply_staging_manifest(merged, Domain::FULL, merged, &response.manifest_paths)
            .await;

        self.tracked.insert(merged);
        self.routing = BTreeMap::from([(merged, Domain::FULL)]);
        self.refresh_views().await;
        merged
    }

    /// Region compaction on one published region.
    ///
    /// Publication: `Compact` is a synchronous worker request, and it flushes the target
    /// first so the compaction has SSTs to rewrite.
    async fn compact(&mut self, region_id: RegionId) {
        self.flush_region(region_id).await;
        self.engine
            .handle_request(
                region_id,
                RegionRequest::Compact(RegionCompactRequest {
                    options: compact_request::Options::StrictWindow(StrictWindow {
                        window_seconds: COMPACTION_WINDOW_SECONDS,
                    }),
                    parallelism: None,
                    time_range: None,
                }),
            )
            .await
            .unwrap();
        self.refresh_views().await;
    }

    /// The largest cut strictly between two adjacent distinct ledger keys inside `domain`.
    ///
    /// Both halves keep at least one committed event, so a split never produces an empty
    /// side and the following compaction/merge actually moves rows.
    fn split_cut(&self, domain: Domain, choice: usize) -> Option<usize> {
        let mut keys: Vec<usize> = self
            .ledger
            .events
            .values()
            .map(|event| event.key)
            .filter(|key| domain.contains_key(*key))
            .collect();
        keys.sort_unstable();
        keys.dedup();
        if keys.len() < 2 {
            return None;
        }
        let candidates: Vec<usize> = (0..keys.len() - 1).map(|i| keys[i] + 1).collect();
        Some(candidates[choice % candidates.len()])
    }

    /// Applies one op and returns a human readable description of what happened.
    async fn apply(&mut self, op: &Op) -> String {
        match op {
            Op::Write { rows } => {
                if rows.is_empty() {
                    return "write (no rows)".to_string();
                }
                let committed = self.write(rows).await;
                format!(
                    "write {} rows -> event ids {:?}",
                    committed.len(),
                    committed
                        .iter()
                        .map(|event| event.event_id)
                        .collect::<Vec<_>>()
                )
            }
            Op::Flush => {
                let flushed = self.flush_dirty().await;
                format!("flush ({flushed} region(s))")
            }
            Op::Split { choice } => {
                let candidates: Vec<RegionId> = self
                    .live_regions()
                    .into_iter()
                    .filter(|region_id| {
                        self.split_cut(self.domain_of(*region_id), *choice)
                            .is_some()
                    })
                    .collect();
                if candidates.is_empty() {
                    return "split (no live region can be split into two non-empty halves)"
                        .to_string();
                }
                let source = candidates[*choice % candidates.len()];
                let cut = self
                    .split_cut(self.domain_of(source), *choice)
                    .expect("candidate has a cut");
                let (left, right) = self.split(source, cut).await;
                format!(
                    "split region {source} at {} -> {left} [{}, {}) and {right} [{}, {})",
                    key_name(cut),
                    key_name(self.domain_of(left).lo),
                    key_name(self.domain_of(left).hi),
                    key_name(self.domain_of(right).lo),
                    key_name(self.domain_of(right).hi),
                )
            }
            Op::Merge => {
                let inputs = self.live_regions();
                if inputs.len() < 2 {
                    return "merge (fewer than two live regions)".to_string();
                }
                let merged = self.merge_all().await;
                format!("merge {inputs:?} -> {merged}")
            }
            Op::Compact { choice } => {
                let live = self.live_regions();
                if live.is_empty() {
                    return "region compaction (no live region)".to_string();
                }
                let target = live[*choice % live.len()];
                self.compact(target).await;
                let files: Vec<String> = self.holders[&target]
                    .files
                    .values()
                    .map(|sst| {
                        format!(
                            "{:?}(owner={}, L{}, rows={})",
                            sst.file_id, sst.owner, sst.level, sst.num_rows
                        )
                    })
                    .collect();
                format!("region compaction on {target} -> {files:?}")
            }
        }
    }
}

/// A key column `k` (string tag), payload `field` (`Float64`), `event_id` (`Int64`) and
/// `ts`; append mode with per-row sequences preserved, partitioned by `k`.
fn create_request(domain: Domain) -> RegionCreateRequest {
    let column_metadatas = vec![
        ColumnMetadata {
            column_schema: ColumnSchema::new(
                "k",
                ConcreteDataType::string_datatype(),
                // Not null: the split predicate is evaluated against this column.
                false,
            ),
            semantic_type: SemanticType::Tag,
            column_id: 0,
        },
        ColumnMetadata {
            column_schema: ColumnSchema::new("payload", ConcreteDataType::float64_datatype(), true),
            semantic_type: SemanticType::Field,
            column_id: 1,
        },
        ColumnMetadata {
            column_schema: ColumnSchema::new("event_id", ConcreteDataType::int64_datatype(), false),
            semantic_type: SemanticType::Field,
            column_id: 2,
        },
        ColumnMetadata {
            column_schema: ColumnSchema::new(
                "ts",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            ),
            semantic_type: SemanticType::Timestamp,
            column_id: 3,
        },
    ];
    let options = [("append_mode", "true"), ("preserve_row_sequence", "true")]
        .into_iter()
        .map(|(key, value)| (key.to_string(), value.to_string()))
        .collect();

    RegionCreateRequest {
        engine: MITO_ENGINE_NAME.to_string(),
        column_metadatas,
        primary_key: vec![0],
        options,
        table_dir: "test".to_string(),
        path_type: PathType::Bare,
        partition_expr_json: Some(domain.expr_json()),
        requirements: Default::default(),
    }
}

/// Encodes one planned row in the region's column order `k, payload, event_id, ts`.
fn event_row(event_id: i64, planned: PlannedRow) -> api::v1::Row {
    row(vec![
        ValueData::StringValue(key_name(planned.key)),
        ValueData::F64Value(planned.payload),
        // The event id is a normal projectable column inside the SST.
        ValueData::I64Value(event_id),
        ValueData::TimestampMillisecondValue(planned.ts),
    ])
}

/// One invariant violation found while replaying a trace.
struct Failure {
    seed: u64,
    step: usize,
    op: String,
    report: String,
}

impl Failure {
    fn describe(&self) -> String {
        format!(
            "=== model-based trace failure ===\n\
             seed: {seed} ({seed:#018x})\n\
             first violating step: {step}\n\
             operation: {op}\n\
             {report}",
            seed = self.seed,
            step = self.step,
            op = self.op,
            report = self.report,
        )
    }
}

fn random_rows(rng: &mut Rng, count: usize) -> Vec<PlannedRow> {
    (0..count)
        .map(|_| PlannedRow {
            key: rng.below(KEY_COUNT),
            // A small timestamp range so distinct events share `(k, ts)`; append tables
            // allow that, and `(PK, ts)` must not be mistaken for an event identity.
            ts: rng.below(4) as i64 * 1000,
            payload: rng.below(100) as f64 / 4.0,
        })
        .collect()
}

/// Rows over distinct partition keys, so the split that follows has a real domain cut.
fn distinct_rows(rng: &mut Rng, count: usize) -> Vec<PlannedRow> {
    let mut keys: Vec<usize> = (0..KEY_COUNT).collect();
    for i in 0..count.min(KEY_COUNT) {
        let j = i + rng.below(KEY_COUNT - i);
        keys.swap(i, j);
    }
    keys.into_iter()
        .take(count)
        .map(|key| PlannedRow {
            key,
            ts: rng.below(4) as i64 * 1000,
            payload: rng.below(100) as f64 / 4.0,
        })
        .collect()
}

/// A random op for the prologue: writes, flushes and splits only.
///
/// The prologue must leave the two halves of the split as co-holders of one SST until the
/// single-holder compaction has run, so these ops never merge (a merge would collapse the
/// holders first) and never compact (compacting both halves is safe and would mask the
/// class). The op after the prologue is free again.
fn prologue_op(rng: &mut Rng) -> Op {
    match rng.below(3) {
        0 => {
            let count = 1 + rng.below(2);
            Op::Write {
                rows: distinct_rows(rng, count),
            }
        }
        1 => Op::Flush,
        _ => Op::Split {
            choice: rng.below(4),
        },
    }
}

fn random_op(rng: &mut Rng) -> Op {
    match rng.below(5) {
        0 => {
            let count = 1 + rng.below(3);
            Op::Write {
                rows: random_rows(rng, count),
            }
        }
        1 => Op::Flush,
        2 => Op::Split {
            choice: rng.below(4),
        },
        3 => Op::Merge,
        _ => Op::Compact {
            choice: rng.below(4),
        },
    }
}

/// Builds a trace for `seed`.
///
/// The prologue guarantees the trace reaches the bug class (one flush, a split that makes
/// two holders share an SST, a single-holder region compaction, then a merge) instead of
/// hoping random ops line up; the ops in between and after are random, so distinct seeds
/// reach that class with different states.
fn generate_plan(seed: u64, len: usize) -> Vec<Op> {
    let mut rng = Rng::new(seed);
    let first_count = 2 + rng.below(2);
    let first_rows = distinct_rows(&mut rng, first_count);
    let mut ops = vec![
        Op::Write { rows: first_rows },
        Op::Flush,
        Op::Split {
            choice: rng.below(4),
        },
    ];
    let extra_before = rng.below(3);
    for _ in 0..extra_before {
        ops.push(prologue_op(&mut rng));
    }
    let compact_choice = rng.below(4);
    ops.push(Op::Compact {
        choice: compact_choice,
    });
    let extra_after = rng.below(2);
    for _ in 0..extra_after {
        ops.push(prologue_op(&mut rng));
    }
    ops.push(Op::Merge);
    while ops.len() < len {
        ops.push(random_op(&mut rng));
    }
    ops
}

/// Replays `ops` on a fresh engine and stops at the first step that violates the oracle.
async fn run_trace(prefix: &str, seed: u64, ops: &[Op]) -> Result<(), Failure> {
    let mut model = StorageModel::new(prefix).await;
    for (step, op) in ops.iter().enumerate() {
        let outcome = model.apply(op).await;
        let ctx = format!("step {step}: {} => {outcome}", op.describe());
        common_telemetry::info!("[{prefix}] seed {seed} {ctx}");
        if let Some(report) = model.check(&ctx).await {
            return Err(Failure {
                seed,
                step,
                op: op.describe(),
                report,
            });
        }
    }
    Ok(())
}

/// Shortest failing prefix: replay only `ops[..=first.step]` and confirm it reproduces.
async fn shrink(seed: u64, ops: &[Op], first: Failure) -> String {
    let prefix = &ops[..=first.step];
    let replay = run_trace(&format!("mbt-shrink-{seed}"), seed, prefix).await;
    let note = match replay {
        Err(failure) if failure.step == first.step => format!(
            "shortest failing prefix: ops[0..={}] ({} of {} ops) reproduces the same \
             violation at step {}",
            first.step,
            prefix.len(),
            ops.len(),
            first.step
        ),
        Err(failure) => format!(
            "replaying the prefix failed at step {} instead of {}",
            failure.step, first.step
        ),
        Ok(()) => "replaying the prefix passed, the failure is not reproducible".to_string(),
    };
    format!("{}\n{note}", first.describe())
}

/// Milestone 1: the fixed trace, expressed through the model.
///
/// Two events `A = (k01)` and `B = (k09)` are flushed into one SST `F`; `F` is split into
/// `P: k < k05` and `R: k05 <= k < k10` (both reference `F`); only `R` is compacted, which
/// materializes `F` under a new local file id while `P` still holds `F`; the two halves are
/// merged into one survivor whose domain is the whole table.
///
/// The ledger expectation is exactly `{A: 1, B: 1}` and it never relaxes: a fix that
/// refuses the compaction instead of rewriting it still has to make this assertion pass.
#[tokio::test]
async fn test_mbt_split_shared_sst_single_side_compaction_duplicates_after_merge() {
    common_telemetry::init_default_ut_logging();
    let mut model = StorageModel::new("mbt-dup-row-mvp").await;
    let source = model.live_regions()[0];

    // ---- Step 0: two events, one flush, one SST. ----
    let committed = model
        .write(&[
            PlannedRow {
                key: 1,
                ts: 0,
                payload: 1.5,
            },
            PlannedRow {
                key: 9,
                ts: 1000,
                payload: 9.5,
            },
        ])
        .await;
    let (a, b) = (committed[0].event_id, committed[1].event_id);
    model.flush_dirty().await;
    model.assert_visible("after flush").await;

    let source_files = model.holder_files(source);
    assert_eq!(
        1,
        source_files.len(),
        "the flush must produce exactly one SST, got {source_files:?}"
    );
    let shared = *source_files.iter().next().unwrap();

    // ---- Step 1: split into P: k < k05 and R: k05 <= k < k10. ----
    let (left, right) = model.split(source, 5).await;
    model.assert_visible("after split").await;
    assert_eq!(
        BTreeSet::from([shared]),
        model.holder_files(left),
        "the left half must reference the split SST"
    );
    assert_eq!(
        BTreeSet::from([shared]),
        model.holder_files(right),
        "the right half must reference the same file id: a legal split shares the object"
    );
    assert_eq!(
        BTreeMap::from([(a, 1)]),
        model.scan_counts(left).await,
        "the left half must show exactly event A"
    );
    assert_eq!(
        BTreeMap::from([(b, 1)]),
        model.scan_counts(right).await,
        "the right half must show exactly event B"
    );

    // ---- Step 2: region compaction on the right half only. ----
    model.compact(right).await;
    model
        .assert_visible("after compacting only the right half")
        .await;

    let right_files = model.holder_files(right);
    assert_eq!(
        1,
        right_files.len(),
        "the rewrite replaces the shared input, got {right_files:?}"
    );
    let copy = *right_files.iter().next().unwrap();
    assert_ne!(
        shared, copy,
        "the right half must replace the shared SST with its own file id"
    );
    assert_eq!(
        right,
        model.sst_ref(right, copy).owner,
        "the new file must be physically owned by the right half"
    );
    assert!(
        model.holder_files(left).contains(&shared),
        "the left half must still reference the original shared SST"
    );
    assert_eq!(
        BTreeMap::from([(a, 1)]),
        model.scan_counts(left).await,
        "the left half must still read event A from the original shared SST"
    );
    assert_eq!(
        BTreeMap::from([(b, 1)]),
        model.scan_counts(right).await,
        "the right half must read event B exactly once"
    );
    let holders_of_shared = model.physical.holders_of[&shared].clone();
    assert!(
        holders_of_shared.len() >= 2,
        "the shared object is still referenced by more than one holder: {holders_of_shared:?}"
    );

    // ---- Step 3: merge both halves back into one whole-domain survivor. ----
    let merged = model.merge_all().await;
    let violation = model
        .check(&format!("after merging into survivor {merged}"))
        .await;
    assert!(
        violation.is_none(),
        "every committed event must stay visible exactly once after the merge, but the merge \
         keeps both the shared SST and the single-side compaction's copy.\n{}",
        violation.unwrap_or_default()
    );
}

/// Milestone 2: fixed-seed random traces over the same table.
///
/// Ops: `write` (routed by partition key), `flush`, `split` (data-aware domain cut),
/// `merge` (all live regions into one) and `region compaction` on one live region. The
/// oracle runs after every published step. On failure the trace is shrunk to the shortest
/// failing prefix, which is reported with the seed, the step index, the operation and the
/// visible-vs-ledger diff.
#[tokio::test]
async fn test_mbt_random_traces() {
    common_telemetry::init_default_ut_logging();
    let mut all_ok = true;
    let mut reports = Vec::new();
    for seed in RANDOM_SEEDS {
        let ops = generate_plan(seed, PLAN_LEN);
        match run_trace(&format!("mbt-{seed}"), seed, &ops).await {
            Ok(()) => reports.push(format!(
                "seed {seed} ({seed:#018x}): no violation in {} steps",
                ops.len()
            )),
            Err(failure) => {
                all_ok = false;
                reports.push(shrink(seed, &ops, failure).await);
            }
        }
    }
    assert!(
        all_ok,
        "the storage model invariant was violated ({} of {} seeds):\n\n{}",
        reports
            .iter()
            .filter(|report| report.contains("VISIBILITY INVARIANT VIOLATED"))
            .count(),
        RANDOM_SEEDS.len(),
        reports.join("\n\n")
    );
}
