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

//! The chaos **driver** of the repartition (split/merge) line: fault injection, compaction
//! gates, the concurrency and recovery folds, and the scenario table.
//!
//! The model-based line pins the bug class "a repartition hands one SST `file_id` to several
//! live regions, and merging the holders back duplicates the logical row". Every trace in
//! that line silently assumes four things that production does not guarantee:
//!
//! 1. every object-store operation succeeds;
//! 2. every request runs to completion;
//! 3. no second request touches the region while the first one runs;
//! 4. the process keeps its memory between two steps.
//!
//! This module adds a chaos layer that breaks each assumption with a lever reachable
//! in-process, and keeps the oracle and every invariant category of the model-based test
//! exactly as they are.
//!
//! # The three layers of this line
//!
//! [`super::repartition_mbt_model`] is the specification (pure transitions and the
//! prediction), [`super::repartition_mbt_oracle`] is the judgement (the ledger, "exactly
//! once", "no row outside its owner's domain" and the five invariant categories), and this
//! module is the driver: requests, scans, publication waits, the fault levers and the
//! scenario table.
//!
//! # What this layer is not
//!
//! Not the specification and not the judgement: it owns no model state of its own and adds
//! no invariant — every lever below only perturbs *how* a step reaches the engine.
//!
//! # The oracle (unchanged)
//!
//! After every step the harness scans the regions of the published routing and asserts
//!
//! 1. `visibility`: the multiset of visible `event_id`s over the live regions equals the
//!    ledger's committed multiset, each event exactly once (no duplicate, no loss, and no
//!    region reading one event twice), and no event is visible in a region whose published
//!    domain does not own it;
//! 2. `manifest-consistency`: the persisted manifest file set equals the live version file
//!    set, and the published routing equals the regions that actually applied it;
//! 3. `model-authorization`: a region shows no row outside the domain scheme B lets it show
//!    (the specification advances on transitions only);
//! 4. `reference-bounds`: every reference a live holder shows stays inside that holder's
//!    domain (reported, never fatal — one *legal* split already fires it on this branch);
//! 5. `sequence-monotonicity`: the committed sequence of a region never moves backwards;
//! 6. `physical-liveness`: every object a live holder references still reads back complete;
//! 7. `perturbation`: the lever of the step actually fired. Fatal, because a perturbation
//!    that never reached the engine leaves the scenario passing while proving nothing.
//!
//! # Levers
//!
//! | lever | API | breaks |
//! | --- | --- | --- |
//! | SST upload fault | `MockLayer::writer_factory` on `*.parquet`, `ChaosFaults::arm` | the flush / compaction publication succeeds |
//! | SST read fault | `MockLayer::reader_factory` on `*.parquet` | the scan / compaction input read succeeds |
//! | staging-manifest fault | `MockLayer` on `.../staging/...` (write and read) | `remap_manifests` / `ApplyStagingManifest` succeeds |
//! | manifest delete fault | `MockLayer::deleter_factory` on manifest deltas and checkpoints | checkpoint cleanup succeeds |
//! | upload delay | `ChaosFaults::arm_delay` + `tokio::spawn` | publication is atomic from a reader's point of view |
//! | cancel a compaction | `EventListener::on_merge_ssts_finished` gate + `RegionRequest::CancelCompaction` | every request runs to completion |
//! | cancel too late | `EventListener::on_compaction_commit_begin` gate + the same request | a cancel can still be honoured after the commit |
//! | abort a split | `remap_manifests` without `ApplyStagingManifest`, `SetRegionRoleState::Leader` | the coordinator finishes what it starts |
//! | concurrency | `tokio::spawn` on a cloned `MitoEngine` | one request at a time |
//! | recovery fold | `RegionRequest::Close`/`Open`, `TestEnv::open_engine` | the process keeps its memory |
//!
//! The engine is created through `TestEnv::with_mock_layer(...)` and
//! `TestEnv::create_engine_with(config, None, Some(listener), None)`, with
//! `manifest_checkpoint_distance = 1` so the manifest cleanup that deletes really runs at
//! every publication, and with `schedule_compaction_after_edit = false` so no edit publishes
//! a compaction of its own. A flush *completion* still schedules one in the worker, so a
//! step boundary can hold a compaction in flight; the view reads wait for the engine's task
//! list to empty before they compare a region's manifest with its version, because a
//! publication writes the manifest first and installs the version after it.
//!
//! # What is NOT reachable from this crate
//!
//! - There is no hook inside `remap_manifests` itself: `EventListener` exposes
//!   `on_enter_staging_result_begin` and `on_notify_region_change_result_begin`, but nothing
//!   between the staging manifest write and its return, so a split is only ever cut *around*
//!   that call, not inside it. The scenario drives the two halves (`remap_manifests` and
//!   `ApplyStagingManifest`) by hand instead, which is the same observable cut.
//! - The object-store fault layer cannot fail a *manifest checkpoint write* on a specific
//!   version without also failing the delta write of that version, because both go through
//!   the same `*.json` / `*.checkpoint` writer path; the delete path is used instead.
//! - `MitoEngine` has no API to abort a flush from outside (`CancelCompaction` exists, a
//!   cancel-flush request does not), so a flush is only perturbed through the object store.
//!
//! # Determinism and replay
//!
//! Every scenario is a fixed list of [`ChaosOp`]s with its own seed. The seed drives the
//! rows, keys and cuts; the schedule is written out in the source, not sampled. A failure
//! prints the scenario name, the seed, the perturbed operation, the step, every invariant
//! category that fired and the faults that actually fired, and the shortest failing prefix is
//! replayed to prove the failure reproduces.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use api::v1::helper::row;
use api::v1::region::{StrictWindow, compact_request};
use api::v1::value::ValueData;
use api::v1::{Rows, SemanticType};
use common_error::ext::ErrorExt;
use common_recordbatch::RecordBatches;
use datatypes::arrow::array::AsArray;
use datatypes::arrow::datatypes::Int64Type;
use datatypes::prelude::ConcreteDataType;
use datatypes::schema::ColumnSchema;
use object_store::ObjectStore;
use store_api::metadata::ColumnMetadata;
use store_api::region_engine::{RegionEngine, RemapManifestsRequest, SettableRegionRoleState};
use store_api::region_request::{
    ApplyStagingManifestRequest, EnterStagingRequest, PathType, RegionCloseRequest,
    RegionCompactRequest, RegionCreateRequest, RegionFlushRequest, RegionRequest,
    StagingPartitionDirective,
};
use store_api::storage::{FileId, RegionId, ScanRequest};

use super::repartition_mbt_model::{
    Domain, ForeignRef, KEY_COUNT, MergeVerdict, RefModel, describe_keys, key_index, key_name,
};
use super::repartition_mbt_oracle::{
    AppliedState, Category, DomainExpr, Event, HolderView, Ledger, ManifestRef, Oracle,
    PhysicalView, PlannedRow, SstReader, SstRef, Violation, decode_domain, domain_of_json,
};
#[cfg(feature = "enterprise")]
use super::table_compaction::PublicationOutcome;
use crate::config::MitoConfig;
use crate::engine::{MITO_ENGINE_NAME, MitoEngine};
use crate::gc::GcConfig;
use crate::manifest::action::{RegionEdit, RegionManifest};
use crate::region::version::VersionControlData;
use crate::region::{RegionLeaderState, RegionRoleState};
use crate::sst::file::LEVEL_SHARED;
use crate::test_util::{TestEnv, put_rows, rows_schema};

/// Wide enough to cover every timestamp the traces write.
const COMPACTION_WINDOW_SECONDS: i64 = 3600;

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
    /// Publish an SST object the write path never produced into one live region's shared
    /// level, then observe its `sequence = None` admission.
    Import { choice: usize },
    /// Table-level (multi-region) compaction of one live region's admitted shared inputs.
    TableCompaction { choice: usize },
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
            Op::Import { choice } => format!("import (choice {choice})"),
            Op::TableCompaction { choice } => {
                format!("multi-region compaction (choice {choice})")
            }
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

/// The real engine plus the three observed views, the ledger and the specification.
struct StorageModel {
    /// Dropped before `_env`, so the engine never outlives its data directory.
    engine: MitoEngine,
    request: RegionCreateRequest,
    schema: Vec<api::v1::ColumnSchema>,
    /// The judgement: every committed event, its own mutable object.
    ledger: Ledger,
    /// The specification: what every published region serves and which files it references.
    /// Advanced by the transitions only, never by an observation.
    spec: RefModel<RegionId>,
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

    /// The object store the SSTs live in, so "readable" is a real read of that store.
    store: ObjectStore,
    /// The table directory and path type the engine resolves SST paths against.
    table_dir: String,
    path_type: PathType,
    /// Per-region applied state, refreshed every step.
    applied: BTreeMap<RegionId, AppliedState>,
    /// The judgement's cross-step bookkeeping (the oracle owns it, the driver feeds it).
    oracle: Oracle,
    /// The index of the step being applied, quoted by the physical liveness report.
    step: usize,

    /// The object-store faults every chaos step may arm.
    faults: ChaosFaults,
    /// The listener gates the engine was created with (compaction cancellation points).
    gates: Arc<ChaosListener>,
    /// The configuration the engine was created with, so a recovery fold can reopen it.
    config: MitoConfig,
    /// What each chaos step did, in order (quoted by the failure report).
    perturbations: Vec<String>,

    /// Keeps the temporary data directory alive for the whole model.
    env: TestEnv,
}

impl StorageModel {
    async fn new(prefix: &str) -> StorageModel {
        // The object store the engine is built on carries the fault schedule of this
        // scenario, so every arm/disarm below is visible to the engine.
        let faults = ChaosFaults::default();
        let gates = Arc::new(ChaosListener::default());
        let mut env = TestEnv::with_prefix(prefix)
            .await
            .with_mock_layer(faults.layer());
        let config = chaos_config();
        let listener: EventListenerRef = gates.clone();
        let engine = env
            .create_engine_with(config.clone(), None, Some(listener), None)
            .await;
        let request = create_request(Domain::FULL);
        let schema = rows_schema(&request);
        let region_id = RegionId::new(1, 1);
        engine
            .handle_request(region_id, RegionRequest::Create(request.clone()))
            .await
            .unwrap();
        // The first region tells us the table directory, the path type and the object store
        // every SST path below is resolved against.
        let (store, table_dir, path_type) = {
            let region = engine.get_region(region_id).unwrap();
            (
                region.access_layer.object_store().clone(),
                region.access_layer.table_dir().to_string(),
                region.access_layer.path_type(),
            )
        };
        let mut model = StorageModel {
            engine,
            request,
            schema,
            ledger: Ledger::default(),
            spec: RefModel::new(),
            next_region_seq: 2,
            routing: BTreeMap::new(),
            holders: BTreeMap::new(),
            physical: PhysicalView::default(),
            tracked: BTreeSet::new(),
            dirty: BTreeSet::new(),
            store,
            table_dir,
            path_type,
            applied: BTreeMap::new(),
            oracle: Oracle::default(),
            step: 0,
            faults: faults.clone(),
            gates: gates.clone(),
            config,
            perturbations: Vec::new(),
            env,
        };

        model.tracked.insert(region_id);
        model.routing.insert(region_id, Domain::FULL);
        model.spec.publish(region_id, Domain::FULL);
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

    /// Reads one region's persisted manifest, live version and role state, once.
    async fn read_region_views(
        &self,
        region_id: RegionId,
    ) -> (Arc<RegionManifest>, VersionControlData, RegionRoleState) {
        let region = self
            .engine
            .get_region(region_id)
            .unwrap_or_else(|| panic!("region {region_id} disappeared"));
        let manifest = region.manifest_ctx.manifest().await;
        let data = region.version_control.current();
        (manifest, data, region.state())
    }

    /// Reads one region's persisted manifest and live version as a *pair*, at a point where
    /// no publication is in flight.
    ///
    /// A publication writes its manifest action first and installs the version after it: a
    /// compaction commits the manifest inside the compaction task and the worker installs the
    /// version when it handles the result. Between the two halves the persisted manifest
    /// already names the compacted files while the live version still serves the inputs. That
    /// is the order the engine publishes in, not a disagreement, and neither half changes
    /// while it lasts — so an unchanged pair proves nothing about the engine being at rest,
    /// and `chaos-delete-faults` / `chaos-publication-race` caught exactly that window (a
    /// flush completion schedules a compaction, so a step boundary can hold a publication in
    /// flight).
    ///
    /// What does prove it is the engine's own accounting: a compaction stays in the task list
    /// from planning until the worker has installed its version (`handle_compaction_finished`
    /// applies the edit before it reports the execution finished), and every flush a step
    /// issues is awaited, so its own half-window is closed by the time the step returns. This
    /// therefore reads the pair only when the task list is empty on both sides of the read and
    /// nothing moved in between. A genuine divergence survives that; only the observation of a
    /// publication in progress is excluded.
    async fn stable_region_views(
        &self,
        region_id: RegionId,
    ) -> (Arc<RegionManifest>, VersionControlData, RegionRoleState) {
        for _ in 0..4 {
            let quiet_before = self.settle().await;
            let first = self.read_region_views(region_id).await;
            let quiet_after = self.settle().await;
            let second = self.read_region_views(region_id).await;
            let unchanged = first.0.manifest_version == second.0.manifest_version
                && Arc::ptr_eq(&first.1.version.ssts, &second.1.version.ssts);
            if quiet_before && quiet_after && unchanged {
                return first;
            }
            tokio::task::yield_now().await;
        }
        panic!(
            "region {region_id}: the engine did not come to rest around a read of its manifest \
             and its version, so the pair cannot be observed without racing a publication"
        )
    }

    /// Refreshes layer 2 (each holder's live version and persisted manifest) and each
    /// region's applied state, then rebuilds layer 1 from layer 2. Values are copied into
    /// [`SstRef`] snapshots, never shared.
    async fn refresh_views(&mut self) {
        let mut holders = BTreeMap::new();
        let mut applied = BTreeMap::new();
        for region_id in self.tracked.clone() {
            let (manifest, data, state) = self.stable_region_views(region_id).await;

            let mut view = HolderView::default();
            for level in data.version.ssts.levels() {
                for (file_id, handle) in &level.files {
                    let meta = handle.meta_ref();
                    view.files.insert(
                        *file_id,
                        SstRef {
                            file_id: *file_id,
                            owner: meta.region_id,
                            level: meta.level,
                            slot_level: level.level,
                            num_rows: meta.num_rows,
                            sequence: meta.sequence.map(|sequence| sequence.get()),
                            creator_domain: meta.partition_expr.as_ref().map(|expr| {
                                decode_domain(expr).unwrap_or_else(|| {
                                    panic!("harness cannot decode a file's creator domain")
                                })
                            }),
                        },
                    );
                }
            }
            for (file_id, meta) in &manifest.files {
                view.manifest.insert(
                    *file_id,
                    ManifestRef {
                        level: meta.level,
                        sequence: meta.sequence.map(|sequence| sequence.get()),
                        num_rows: meta.num_rows,
                    },
                );
            }

            holders.insert(region_id, view);
            applied.insert(
                region_id,
                AppliedState {
                    domain: data
                        .version
                        .metadata
                        .partition_expr
                        .as_deref()
                        .map(domain_of_json)
                        .unwrap_or(Domain::FULL),
                    committed: data.committed_sequence,
                    last_sequence: data.last_sequence,
                    manifest_committed: manifest.committed_sequence,
                    writable: state == RegionRoleState::Leader(RegionLeaderState::Writable),
                },
            );
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
        self.applied = applied;
    }

    /// The object store the SSTs live in, addressed the way the oracle's liveness check and
    /// the import path need it.
    fn reader(&self) -> SstReader<'_> {
        SstReader {
            store: &self.store,
            table_dir: &self.table_dir,
            path_type: self.path_type,
        }
    }

    /// The object-store path of one physical object, resolved through its creator region.
    fn sst_path(&self, creator: RegionId, file_id: FileId) -> String {
        self.reader().path(creator, file_id)
    }

    /// The live `FileMeta` of one reference, as the region's version holds it.
    fn file_meta(&self, region_id: RegionId, file_id: FileId) -> crate::sst::file::FileMeta {
        let region = self
            .engine
            .get_region(region_id)
            .unwrap_or_else(|| panic!("region {region_id} disappeared"));
        let version = region.version();
        version
            .ssts
            .levels()
            .iter()
            .find_map(|level| level.files.get(&file_id))
            .map(|handle| handle.meta_ref().clone())
            .unwrap_or_else(|| panic!("region {region_id} does not reference {file_id:?}"))
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

    /// A plain region scan, as `(event_id, partition key)` pairs.
    ///
    /// The model-side check needs the key each *visible* row carries — what the region
    /// actually shows — not what the ledger believes about that row. It therefore reads no
    /// ledger state at all.
    async fn scan_rows(&self, region_id: RegionId) -> Vec<(i64, usize)> {
        let stream = self
            .engine
            .scan_to_stream(region_id, ScanRequest::default())
            .await
            .unwrap();
        let batches = RecordBatches::try_collect(stream).await.unwrap();
        let mut rows = Vec::new();
        for batch in batches.iter() {
            let ids = batch
                .column_by_name("event_id")
                .expect("a plain scan must project the event_id column")
                .as_primitive::<Int64Type>();
            let keys = batch
                .column_by_name("k")
                .expect("a plain scan must project the partition key column")
                .as_string::<i32>();
            for i in 0..batch.num_rows() {
                let key = key_index(keys.value(i)).unwrap_or_else(|| {
                    panic!(
                        "the harness wrote an unknown partition key {:?}",
                        keys.value(i)
                    )
                });
                rows.push((ids.value(i), key));
            }
        }
        rows
    }

    /// A plain scan of every published region, as `event id -> copies`.
    ///
    /// The scans happen here, in the driver: the oracle judges them and never touches the
    /// engine.
    async fn scan_live(&self) -> BTreeMap<RegionId, BTreeMap<i64, usize>> {
        let mut scans = BTreeMap::new();
        for region_id in self.live_regions() {
            scans.insert(region_id, self.scan_counts(region_id).await);
        }
        scans
    }

    /// The ledger oracle, fed with a fresh scan of every published region.
    ///
    /// Returns a report when the invariant is violated, `None` otherwise.
    async fn check(&mut self, ctx: &str) -> Option<String> {
        self.refresh_views().await;
        let observed = self.scan_live().await;
        self.oracle.visibility(
            &self.ledger,
            &self.routing,
            &self.holders,
            &self.physical,
            &observed,
            ctx,
        )
    }

    /// Checks every invariant category and returns the violations in category order.
    ///
    /// This never relaxes the oracle above: [`StorageModel::check`] keeps its meaning, and
    /// the categories below are additional statements about the same step.
    async fn check_invariants(&mut self, ctx: &str) -> Vec<Violation> {
        self.refresh_views().await;

        let mut violations = Vec::new();
        self.oracle
            .manifest_consistency(&self.holders, ctx, &mut violations);
        self.oracle
            .routing_applied(&self.routing, &self.applied, ctx, &mut violations);
        // The model-side statement needs the specification, so it stays in the driver.
        self.check_model_authorization(ctx, &mut violations).await;
        self.oracle.reference_bounds(
            &self.routing,
            &self.holders,
            &self.physical,
            ctx,
            &mut violations,
        );
        self.oracle
            .sequence_monotonicity(&self.applied, ctx, &mut violations);
        let reader = SstReader {
            store: &self.store,
            table_dir: &self.table_dir,
            path_type: self.path_type,
        };
        self.oracle
            .physical_liveness(
                &self.live_regions(),
                &self.holders,
                &reader,
                self.step,
                ctx,
                &mut violations,
            )
            .await;
        violations
    }

    /// The model-side assertion, separate from the ledger oracle.
    ///
    /// After every published step each region may only show the rows scheme B lets it show:
    /// rows whose key lies in the region's own domain, plus the rows it
    /// wrote and has not flushed yet (in-domain by construction). B hands a reference over
    /// whole, so the domain is the only bound there is.
    ///
    /// This reads a fresh scan and the specification state and nothing else — never the
    /// ledger, whose oracle in [`StorageModel::check`] in turn never reads the
    /// specification.
    async fn check_model_authorization(&self, ctx: &str, violations: &mut Vec<Violation>) {
        for (region_id, domain) in self
            .routing
            .iter()
            .map(|(region, domain)| (*region, *domain))
        {
            let Some(state) = self.spec.region(region_id) else {
                violations.push(Violation {
                    category: Category::ModelAuthorization,
                    detail: format!(
                        "[{ctx}] region {region_id} is published but the specification has no \
                         state for it: a transition was missed"
                    ),
                });
                continue;
            };
            if state.domain != domain {
                violations.push(Violation {
                    category: Category::ModelAuthorization,
                    detail: format!(
                        "[{ctx}] the model published region {region_id} for [{}, {}) but the \
                         specification advanced it to [{}, {})",
                        key_name(domain.lo),
                        key_name(domain.hi),
                        key_name(state.domain.lo),
                        key_name(state.domain.hi)
                    ),
                });
                continue;
            }
            // Everything the diff needs is copied out first: the scan below is an await and
            // it must not overlap a borrow of the specification.
            let authorized = self.spec.authorized_keys(region_id);
            let state_desc = self.spec.describe(region_id);

            let mut rows_outside = 0usize;
            let mut keys_outside = BTreeSet::new();
            let mut examples = Vec::new();
            for (event_id, key) in self.scan_rows(region_id).await {
                if authorized.contains(&key) {
                    continue;
                }
                rows_outside += 1;
                keys_outside.insert(key);
                if examples.len() < 8 {
                    examples.push(format!("event_id {event_id} at {}", key_name(key)));
                }
            }
            if rows_outside == 0 {
                continue;
            }
            violations.push(Violation {
                category: Category::ModelAuthorization,
                detail: format!(
                    "[{ctx}] region {region_id} (domain [{}, {})) shows {rows_outside} row(s) \
                     outside the domain scheme B lets it show, keys {} (e.g. {examples:?}); the \
                     domain allows {}; specification state: {state_desc}",
                    key_name(domain.lo),
                    key_name(domain.hi),
                    describe_keys(&keys_outside),
                    describe_keys(&authorized)
                ),
            });
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
                // The row is in this region's memtable until a flush publishes it.
                self.spec.note_write(region_id, event_id, planned.key);
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
            // A flush turns this region's memtable into one new physical file.
            self.spec.flushed(*region_id);
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
            self.spec.flushed(region_id);
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

        // A real repartition retires the source: its rows live in the targets now, so the
        // engine drops the region and nothing later may compact it or merge into it.
        self.retire_split_source(source).await;
        self.tracked.insert(left);
        self.tracked.insert(right);
        self.routing.insert(left, left_domain);
        self.routing.insert(right, right_domain);
        self.spec
            .split(source, left, left_domain, right, right_domain);
        self.refresh_views().await;
        (left, right)
    }

    /// Retires the region a split published from, the way the coordinator removes it once the
    /// targets serve its rows: the engine drops it and every later step stops seeing it.
    async fn retire_split_source(&mut self, source: RegionId) {
        self.engine
            .handle_request(
                source,
                RegionRequest::Close(RegionCloseRequest {
                    flush_on_close: false,
                }),
            )
            .await
            .unwrap();
        assert!(
            !self.engine.is_region_exists(source),
            "the split source {source} is still open: a split retires its source"
        );
        self.tracked.remove(&source);
        self.routing.remove(&source);
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

    /// Merges every published region into one new whole-domain region, first making the merge
    /// legal under scheme B.
    ///
    /// B allows a merge only while every input holds nothing but the files it created itself.
    /// When the model expects a refusal the driver issues the coordinator's remap and asserts
    /// the engine refuses it, naming every offending `(region, file, creator)`; it then
    /// compacts every offending region and retries the merge the model now allows. An engine
    /// that publishes the survivor anyway is the divergence this suite exists to find.
    ///
    /// Publication: every input stops serving (a synchronous role-state request, exactly
    /// like the split source), then the merged region is created, consumes the coordinator's
    /// staging manifest (a synchronous request) and only then becomes the sole published
    /// region.
    async fn merge_all(&mut self) -> RegionId {
        let inputs = self.live_regions();
        assert!(
            inputs.len() >= 2,
            "a merge needs at least two live regions, got {inputs:?}"
        );
        // A merge consumes persisted manifests, so the row state has to reach the SSTs first.
        self.flush_dirty().await;
        let verdict = self.spec.merge_verdict(&inputs);
        if let MergeVerdict::Refused(offenders) = &verdict {
            self.spec.note_refused_merge(&verdict);
            let expected = verdict.describe();
            let named = describe_offenders(offenders);
            let err = match self.probe_merge_refusal(&inputs).await {
                Ok(probe) => panic!(
                    "SCHEME B DIVERGES FROM THE ENGINE: {expected}, but the engine accepted the \
                     remap of {} into staging region {probe}. Scheme B allows a merge only while \
                     every participant holds just the files it created itself, and these hold \
                     foreign ones: {named}",
                    describe_regions(&inputs)
                ),
                Err(err) => err,
            };
            // The refusal has to be the specific failure scheme B states, not just any error
            // the remap can return: an unrelated remap error would otherwise pass for one. The
            // trait's remap boxes the mito2 error, so it is downcast back to the variant.
            let refusal_variant = err.as_any().downcast_ref::<crate::error::Error>();
            if !matches!(
                refusal_variant,
                Some(crate::error::Error::ForeignFilePartitionExpr { .. })
            ) {
                panic!(
                    "SCHEME B DIVERGES FROM THE ENGINE: {expected} for inputs [{}], but the \
                     remap failed with an error that is not the refusal: {err:?}. Only \
                     Error::ForeignFilePartitionExpr is the refusal; these hold foreign files: \
                     {named}",
                    describe_regions(&inputs)
                );
            }
            let message = err.to_string();
            // The message has to name every offending region *and* every offending file id. The
            // model's file ids are its own (opaque, never an engine `FileId`), so the ids to look
            // for are read from the engine's persisted manifests, the way the remap reads them,
            // and the model's offender count has to agree with that set.
            let engine_offenders = self.engine_foreign_files(&inputs).await;
            assert_eq!(
                offenders.len(),
                engine_offenders.len(),
                "the model expects a refusal naming {} offending reference(s) ({named}), but the \
                 engine's persisted manifests hold {} file(s) another region created: \
                 {engine_offenders:?}",
                offenders.len(),
                engine_offenders.len()
            );
            for offender in offenders {
                assert!(
                    message.contains(&offender.region.to_string()),
                    "the refusal must name the offending region {}: {message}",
                    offender.region
                );
            }
            for (region_id, file_id) in &engine_offenders {
                assert!(
                    message.contains(&region_id.to_string()),
                    "the refusal must name the offending region {region_id} that holds \
                     {file_id}: {message}"
                );
                assert!(
                    message.contains(&file_id.to_string()),
                    "the refusal must name the offending file {file_id} held by region \
                     {region_id}: {message}"
                );
            }
            common_telemetry::info!(
                "[mbt-chaos] scheme B refused the merge of {inputs:?} ({expected}): {message}; \
                 compacting {:?} and retrying",
                verdict.offending_regions()
            );
            for region_id in verdict.offending_regions() {
                self.compact(region_id).await;
            }
            let retry = self.spec.merge_verdict(&inputs);
            assert!(
                retry.is_allowed(),
                "compacting {:?} must make the merge of {inputs:?} legal under scheme B, but {}",
                verdict.offending_regions(),
                retry.describe()
            );
        }
        // A retired region must not keep writing for a domain another region now serves.
        for input in &inputs {
            self.engine
                .set_region_role_state_gracefully(*input, SettableRegionRoleState::StagingLeader)
                .await
                .unwrap();
        }

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
        self.spec.merge(&inputs, merged);
        self.refresh_views().await;
        merged
    }

    /// Issues the coordinator's remap for `inputs` into a fresh staging survivor and reports
    /// whether the engine refused it, with the engine's own error so the caller can assert
    /// *which* failure it is (`RegionEngine::remap_manifests` boxes the mito2 error, so the
    /// caller downcasts it back to [`crate::error::Error`]).
    ///
    /// Only the remap runs: it reads every input's persisted manifest, so it is where an engine
    /// implementing scheme B would see the foreign file and refuse. Nothing is published and the
    /// inputs are not retired, so a refusal leaves the fixtures exactly as they were and the
    /// compactions that follow can still run.
    async fn probe_merge_refusal(
        &mut self,
        inputs: &[RegionId],
    ) -> Result<RegionId, common_error::ext::BoxedError> {
        let probe = self.alloc_region_id();
        self.create_and_enter_staging(probe, Domain::FULL).await;
        let remap = self
            .engine
            .remap_manifests(RemapManifestsRequest {
                region_id: probe,
                input_regions: inputs.to_vec(),
                region_mapping: inputs
                    .iter()
                    .map(|region_id| (*region_id, vec![probe]))
                    .collect(),
                new_partition_exprs: [(probe, Domain::FULL.expr_json())].into_iter().collect(),
            })
            .await;
        match remap {
            Err(err) => {
                // The probe region never joins the routing; close it so it does not keep a
                // staged identity alive behind the fixture's back.
                let _ = self
                    .engine
                    .handle_request(
                        probe,
                        RegionRequest::Close(RegionCloseRequest {
                            flush_on_close: false,
                        }),
                    )
                    .await;
                Err(err)
            }
            Ok(_) => Ok(probe),
        }
    }

    /// The `(region, file)` pairs a scheme B refusal has to name: every file an input's
    /// persisted manifest holds whose recorded creator is not that region.
    ///
    /// This is the rule the remap itself applies, read straight from the engine so the refusal
    /// can be checked against the engine's real `FileId`s — the model mints its own, opaque
    /// ones. A file that records no creator is foreign too: its provenance is unproven, and the
    /// remap refuses it exactly like a stamp another region wrote (#10646).
    async fn engine_foreign_files(&self, inputs: &[RegionId]) -> Vec<(RegionId, FileId)> {
        let mut foreign = Vec::new();
        for region_id in inputs {
            let region = self
                .engine
                .get_region(*region_id)
                .unwrap_or_else(|| panic!("region {region_id} disappeared"));
            let manifest = region.manifest_ctx.manifest().await;
            let holder_expr =
                crate::region::parse_partition_expr(manifest.metadata.partition_expr.as_deref())
                    .expect("the harness reads a region's persisted partition expr");
            for (file_id, meta) in &manifest.files {
                // Exact match, so an unstamped file counts as foreign: it may hold the rows of
                // both children of an earlier split.
                if meta.partition_expr.as_deref() != holder_expr.as_ref() {
                    foreign.push((*region_id, *file_id));
                }
            }
        }
        foreign
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
        self.spec.compact(region_id);
        self.refresh_views().await;
    }

    /// Publishes an SST object the write path never produced into `target`'s shared level.
    ///
    /// The object is a byte copy of one SST `target` flushed itself, re-registered under a
    /// brand-new `FileId` at [`LEVEL_SHARED`] with `sequence: None`, and the target's own L0
    /// reference is handed over in the same edit, so the visible row multiset does not move.
    /// Admission is the only place that can turn `sequence: None` into a stored admission
    /// sequence, so both outcomes are recorded: the fresh admission that assigns one, and
    /// the repeat admission of a still-visible import that keeps it.
    ///
    /// Publication: `MitoEngine::edit_region` is a synchronous worker request; when it
    /// returns, the reference is in the manifest and the version.
    async fn import_sst(&mut self, target: RegionId) -> String {
        self.flush_region(target).await;
        // The flush may have produced the SST this op republishes, so re-read the views.
        self.refresh_views().await;
        let original_id = self.holders.get(&target).and_then(|view| {
            view.files
                .iter()
                .find(|(_, sst)| sst.level == 0 && sst.owner == target)
                .map(|(file_id, _)| *file_id)
        });
        let Some(original_id) = original_id else {
            return format!(
                "import into {target} (no self-flushed L0 SST to republish, nothing imported)"
            );
        };

        // A brand-new physical object: the copy exists only because of this op.
        let original = self.file_meta(target, original_id);
        let copy_id = FileId::random();
        let source_path = self.sst_path(original.region_id, original.file_id);
        let target_path = self.sst_path(target, copy_id);
        if let Err(err) = self.store.copy(&source_path, &target_path).await {
            return format!(
                "import into {target} (copying {source_path} -> {target_path} failed: {err})"
            );
        }

        let mut published = original.clone();
        published.file_id = copy_id;
        published.region_id = target;
        published.level = LEVEL_SHARED;
        // The caller does not know an admission sequence: this is the `None` admission.
        published.sequence = None;
        let committed_before = self.committed_sequence(target);
        let admitted = self
            .engine
            .edit_region(
                target,
                RegionEdit {
                    files_to_add: vec![published],
                    files_to_remove: vec![original],
                    ..Default::default()
                },
            )
            .await;
        if let Err(err) = admitted {
            // The copy is orphaned (nothing references it) and is left to the sweeper: the
            // liveness check only speaks about objects a live holder still references.
            // The cause (not just the "failed to edit region" header) is what tells an
            // unsupported shared level from a real rejection here.
            return format!("import {copy_id:?} into {target}: rejected at admission ({err:?})");
        }
        // The copy carries exactly the rows the original reference carried out of this
        // region, so the region's authorized rows do not move: only one reference changes
        // its physical identity.
        self.spec.remint(target);
        self.refresh_views().await;
        let assigned = self
            .holders
            .get(&target)
            .and_then(|view| view.files.get(&copy_id))
            .map(|sst| sst.sequence);
        let committed_after = self.committed_sequence(target);
        let mut outcome = format!(
            "import {copy_id:?} into {target}: admitted with sequence None -> {:?} (committed \
             sequence {committed_before} -> {committed_after})",
            assigned.unwrap_or_default()
        );

        // Second observation: a repeat of a still-visible import is not a new admission.
        let visible = self.file_meta(target, copy_id);
        let repeat = self
            .engine
            .edit_region(
                target,
                RegionEdit {
                    files_to_add: vec![visible],
                    ..Default::default()
                },
            )
            .await;
        match repeat {
            Ok(()) => {
                self.refresh_views().await;
                let kept = self
                    .holders
                    .get(&target)
                    .and_then(|view| view.files.get(&copy_id))
                    .map(|sst| sst.sequence);
                outcome.push_str(&format!(
                    "; repeat import: no new admission, sequence kept {:?}, committed sequence \
                     {committed_after} -> {}",
                    kept.unwrap_or_default(),
                    self.committed_sequence(target)
                ));
            }
            Err(err) => outcome.push_str(&format!("; repeat import rejected ({err})")),
        }
        outcome
    }

    /// A live region whose own flush produced an L0 SST, so an import has an object to
    /// republish. Falls back to any live region (a flush inside the import may produce one).
    fn import_target(&self, choice: usize) -> Option<RegionId> {
        let own_flush: Vec<RegionId> = self
            .live_regions()
            .into_iter()
            .filter(|region_id| {
                self.holders.get(region_id).is_some_and(|view| {
                    view.files
                        .values()
                        .any(|sst| sst.level == 0 && sst.owner == *region_id)
                })
            })
            .collect();
        let candidates = if own_flush.is_empty() {
            self.live_regions()
        } else {
            own_flush
        };
        if candidates.is_empty() {
            return None;
        }
        Some(candidates[choice % candidates.len()])
    }

    /// The region's committed sequence, as the latest refresh observed it.
    fn committed_sequence(&self, region_id: RegionId) -> u64 {
        self.applied
            .get(&region_id)
            .map(|state| state.committed)
            .unwrap_or_default()
    }

    /// Table-level (multi-region) compaction of one published region.
    ///
    /// This is the region-side half of `compaction/multi_region.rs`: the coordinator's
    /// admitted shared inputs are rewritten into region-local objects and published through
    /// `engine/table_compaction.rs`. It is an *additional* path, never a replacement for the
    /// per-region compaction above.
    ///
    /// Publication: `publish_table_compaction` is a synchronous worker request that reports
    /// `Applied` only after the region's version and manifest hold the outputs.
    ///
    /// `engine/table_compaction.rs` is only compiled with the `enterprise` feature, so a
    /// default build reports the path as not compiled in instead of silently skipping it.
    async fn table_compact(&mut self, target: RegionId) -> String {
        #[cfg(feature = "enterprise")]
        {
            let snapshot = match self.engine.table_compaction_snapshot(target) {
                Ok(snapshot) => snapshot,
                Err(err) => {
                    return format!("multi-region compaction on {target}: no snapshot ({err})");
                }
            };
            if snapshot.inputs.is_empty() {
                return format!(
                    "multi-region compaction on {target}: no admitted shared input (an import \
                     must run first)"
                );
            }

            let inputs: Vec<FileId> = snapshot.inputs.iter().map(|input| input.file_id).collect();
            let applied_partition_expr = snapshot.metadata.partition_expr.clone();
            // A reference created by another region keeps that creator's partition expr, and a
            // byte copy cannot rewrite it: only a real rewrite (`compaction/multi_region.rs`)
            // produces a region-local output. Report it instead of publishing an invalid one.
            let foreign: Vec<String> = snapshot
                .inputs
                .iter()
                .filter(|input| input.region_id != target)
                .map(|input| format!("{:?} created by {}", input.file_id, input.region_id))
                .collect();
            if !foreign.is_empty() {
                return format!(
                    "multi-region compaction on {target}: cannot republish {foreign:?}; a shared \
                     reference created by another region keeps that creator's partition expr, so \
                     only a real multi-region rewrite can produce this region's output"
                );
            }
            let applied_expr =
                crate::region::parse_partition_expr(snapshot.metadata.partition_expr.as_deref())
                    .ok()
                    .flatten();
            // The publication validator compares each output against the region's current
            // metadata, so the inputs (which the outputs mirror) are the interesting diff if
            // it rejects one.
            let describe = |file: &crate::sst::file::FileMeta| {
                format!(
                    "{:?}(creator={}, L{}, seq={:?}, expr={:?}, preserve_row_sequence={})",
                    file.file_id,
                    file.region_id,
                    file.level,
                    file.sequence,
                    file.partition_expr
                        .as_ref()
                        .map(|expr| expr.as_json_str().unwrap_or_default()),
                    file.preserve_row_sequence
                )
            };
            let inputs_desc: Vec<String> = snapshot.inputs.iter().map(describe).collect();
            // A publication output inherits the maximum admission bound of its inputs (the
            // publication validator compares every output against that single bound), and it
            // is never a new import, so its sequence is not re-assigned here.
            let max_sequence = snapshot
                .inputs
                .iter()
                .map(|input| input.sequence)
                .collect::<Option<Vec<_>>>()
                .and_then(|sequences| sequences.into_iter().max());
            let mut outputs = Vec::with_capacity(snapshot.inputs.len());
            for input in &snapshot.inputs {
                let output_id = FileId::random();
                let source_path = self.sst_path(input.region_id, input.file_id);
                let output_path = self.sst_path(target, output_id);
                if let Err(err) = self.store.copy(&source_path, &output_path).await {
                    return format!(
                        "multi-region compaction on {target}: rewriting {input:?} failed ({err})"
                    );
                }
                let mut output = input.clone();
                output.file_id = output_id;
                output.region_id = target;
                output.level = 0;
                // A rewrite emits the target's current partition expr, and the publication
                // validator requires exactly that.
                output.partition_expr = applied_expr.clone().map(Into::into);
                output.sequence = max_sequence;
                outputs.push(output);
            }

            match self
                .engine
                .publish_table_compaction(target, snapshot, outputs)
                .await
            {
                Ok(PublicationOutcome::Applied) => {
                    self.refresh_views().await;
                    format!(
                        "multi-region compaction on {target}: published, shared inputs \
                         {inputs:?} rewritten into region-local L0 outputs"
                    )
                }
                Ok(PublicationOutcome::StaleInputs) => format!(
                    "multi-region compaction on {target}: inputs {inputs:?} went stale, nothing \
                     published"
                ),
                Err(err) => {
                    format!(
                        "multi-region compaction on {target}: publication rejected ({err:?}); \
                         inputs {inputs_desc:?}, applied partition expr {applied_partition_expr:?}"
                    )
                }
            }
        }
        #[cfg(not(feature = "enterprise"))]
        {
            format!(
                "multi-region compaction on {target}: MitoEngine::table_compaction_snapshot / \
                 publish_table_compaction are compiled only with the `enterprise` feature"
            )
        }
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
            Op::Import { choice } => {
                let Some(target) = self.import_target(*choice) else {
                    return "import (no live region)".to_string();
                };
                self.import_sst(target).await
            }
            Op::TableCompaction { choice } => {
                // Prefer a region whose admitted shared input it created itself: this harness
                // publishes byte copies, so only a self-created admission can be republished
                // as a region-local output (see `table_compact`).
                let self_created: Vec<RegionId> = self
                    .live_regions()
                    .into_iter()
                    .filter(|region_id| {
                        self.holders.get(region_id).is_some_and(|view| {
                            view.files
                                .values()
                                .any(|sst| sst.level == LEVEL_SHARED && sst.owner == *region_id)
                        })
                    })
                    .collect();
                if !self_created.is_empty() {
                    let target = self_created[*choice % self_created.len()];
                    return self.table_compact(target).await;
                }
                let Some(target) = self.import_target(*choice) else {
                    return "multi-region compaction (no live region)".to_string();
                };
                // The table-level path consumes admitted shared inputs, so admit one when
                // this target has none yet: that keeps the op reachable on every trace.
                let import = format!("{}; ", self.import_sst(target).await);
                format!("{import}{}", self.table_compact(target).await)
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

// ---------------------------------------------------------------------------
// The chaos layer
// ---------------------------------------------------------------------------
//
// Everything above is the model-based test line as it stands: the ledger oracle, the
// reference model, the five invariant categories and the `Op` vocabulary a trace drives.
// Those traces rest on three assumptions that are never stated and never tested:
//
// | assumption | lever in this section |
// | --- | --- |
// | every object-store operation succeeds | [`ChaosFaults`] fails `SstUpload`, `SstRead`, `StagingWrite`, `StagingRead` and `ManifestDelete` a fixed number of times, or delays them |
// | every request runs to completion | [`Gate`] + `RegionRequest::CancelCompaction` stop a region compaction after it read and before it published; a split is abandoned between `remap_manifests` and `ApplyStagingManifest`, or fails its manifest write |
// | one request at a time | `tokio::spawn` runs a compaction against a second compaction, a scan, a split publication or a merge publication |
// | the process keeps its memory | `RecoverRegions` / `RecoverEngine` close and reopen, so every check also covers the replayed state |
//
// Every lever is in-process: the faults live in the `MockLayer` the engine's object store is
// built with, the cancellation points are `EventListener` callbacks, the concurrency is
// `tokio` tasks on one engine, and the recovery folds are `Close`/`Open` requests. Nothing
// sleeps to reach a state; `settle` only waits for the compaction task list to drain.
//
// The oracle is not weakened: every chaos step is followed by the same
// [`StorageModel::check`] and [`StorageModel::check_invariants`] the traces use, once on the
// live engine and (for the perturbation steps) once more after the recovery fold. A
// scenario's failure names the scenario, the seed, the perturbation, the step and every
// invariant category that fired, and the shortest failing prefix is replayed to prove it.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use object_store::layers::mock::{
    Buffer, BytesRange, Error as MockError, ErrorKind, Metadata as MockMetadata, MockLayer,
    MockLayerBuilder, OpDelete, OpRead, OpWrite, Result as MockResult, RpRead, oio,
};
use store_api::compaction_task::CompactionTaskEntry;
use store_api::region_engine::RegionRole;
use store_api::region_request::{RegionCancelCompactionRequest, RegionOpenRequest};
use tokio::sync::Semaphore;

use crate::engine::listener::{EventListener, EventListenerRef};
use crate::manifest::storage::{is_checkpoint_file, is_delta_file};
use crate::test_util::reopen_region;

// ---------------------------------------------------------------------------
// L1: fail injection in the object store
// ---------------------------------------------------------------------------

/// One object-store operation a chaos step may fail or delay.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum FaultKind {
    /// Publishing a `*.parquet` SST: flush output, compaction output, import output.
    SstUpload,
    /// Reading a `*.parquet` SST: the page/object fetch of a scan, the input read of a
    /// compaction.
    SstRead,
    /// Writing the coordinator's staging manifest and blobs under `.../staging/...`, which is
    /// what `remap_manifests` publishes through.
    StagingWrite,
    /// Reading a staging manifest, which is what `ApplyStagingManifest` opens.
    StagingRead,
    /// Deleting a manifest delta or checkpoint at checkpoint cleanup: the only delete the
    /// engine issues in-process, and the one that keeps the manifest directory finite.
    ManifestDelete,
}

impl FaultKind {
    fn name(self) -> &'static str {
        match self {
            FaultKind::SstUpload => "sst-upload",
            FaultKind::SstRead => "sst-read",
            FaultKind::StagingWrite => "staging-write",
            FaultKind::StagingRead => "staging-read",
            FaultKind::ManifestDelete => "manifest-delete",
        }
    }

    /// What the fault breaks, for the per-scenario report.
    fn breaks(self) -> &'static str {
        match self {
            FaultKind::SstUpload => "a flush or compaction cannot publish its SST",
            FaultKind::SstRead => "a scan or a compaction cannot read its input SST",
            FaultKind::StagingWrite => "the coordinator cannot write the split/merge manifest",
            FaultKind::StagingRead => "a staged region cannot consume the coordinator's manifest",
            FaultKind::ManifestDelete => "manifest checkpoint cleanup cannot delete a stale file",
        }
    }
}

/// What a fault-injecting operation must do with the current call.
enum Injected {
    Pass,
    Wait(Duration),
    Fail,
}

#[derive(Default)]
struct FaultState {
    /// Remaining failures per kind.
    remaining: Mutex<BTreeMap<FaultKind, usize>>,
    /// One-shot delay per kind, consumed by the first call that reaches it.
    delay_ms: Mutex<BTreeMap<FaultKind, u64>>,
    /// Every fault that actually fired, in order.
    hits: Mutex<Vec<String>>,
}

/// The fault schedule of one scenario.
///
/// Cheap to clone: the object-store layer holds a clone and every arm/disarm from the
/// scenario is visible to it.
#[derive(Clone, Default)]
struct ChaosFaults(Arc<FaultState>);

impl ChaosFaults {
    /// Fails the next `times` operations of `kind`.
    fn arm(&self, kind: FaultKind, times: usize) {
        self.0.remaining.lock().unwrap().insert(kind, times);
    }

    /// Delays the next operation of `kind` by `ms` milliseconds.
    fn arm_delay(&self, kind: FaultKind, ms: u64) {
        self.0.delay_ms.lock().unwrap().insert(kind, ms);
    }

    fn disarm(&self, kind: FaultKind) {
        self.0.remaining.lock().unwrap().remove(&kind);
        self.0.delay_ms.lock().unwrap().remove(&kind);
    }

    /// Every fault that fired so far.
    fn hits(&self) -> Vec<String> {
        self.0.hits.lock().unwrap().clone()
    }

    fn last_hits(&self, since: usize) -> Vec<String> {
        let hits = self.0.hits.lock().unwrap();
        hits.iter().skip(since).cloned().collect()
    }

    fn hit_count(&self) -> usize {
        self.0.hits.lock().unwrap().len()
    }

    /// Decides what the current call must do and records the hit.
    fn inject(&self, kind: FaultKind, path: &str) -> Injected {
        let mut remaining = self.0.remaining.lock().unwrap();
        let fired = match remaining.get_mut(&kind) {
            Some(left) if *left > 0 => {
                *left -= 1;
                true
            }
            _ => false,
        };
        drop(remaining);
        if fired {
            self.0
                .hits
                .lock()
                .unwrap()
                .push(format!("{} on {path}", kind.name()));
            return Injected::Fail;
        }
        if let Some(ms) = self.0.delay_ms.lock().unwrap().remove(&kind) {
            self.0
                .hits
                .lock()
                .unwrap()
                .push(format!("{} delayed {ms}ms on {path}", kind.name()));
            return Injected::Wait(Duration::from_millis(ms));
        }
        Injected::Pass
    }

    /// The object-store layer that injects this schedule.
    fn layer(&self) -> MockLayer {
        let writer = self.clone();
        let reader = self.clone();
        let deleter = self.clone();
        MockLayerBuilder::default()
            .writer_factory(Arc::new(
                move |path: &str, _args: OpWrite, inner: oio::Writer| -> oio::Writer {
                    match write_fault_kind(path) {
                        Some(kind) => Box::new(ChaosWriter {
                            inner,
                            faults: writer.clone(),
                            kind,
                            path: path.to_string(),
                        }),
                        None => inner,
                    }
                },
            ))
            .reader_factory(Arc::new(
                move |path: &str, _args: OpRead, inner: oio::Reader| -> oio::Reader {
                    match read_fault_kind(path) {
                        Some(kind) => Box::new(ChaosReader {
                            inner,
                            faults: reader.clone(),
                            kind,
                            path: path.to_string(),
                        }),
                        None => inner,
                    }
                },
            ))
            .deleter_factory(Arc::new(move |inner: oio::Deleter| -> oio::Deleter {
                Box::new(ChaosDeleter {
                    inner,
                    faults: deleter.clone(),
                })
            }))
            .build()
            .unwrap()
    }
}

/// A `.parquet` object is an SST; `.../staging/...` is the coordinator's publication.
fn write_fault_kind(path: &str) -> Option<FaultKind> {
    if path.contains("/staging/") {
        Some(FaultKind::StagingWrite)
    } else if path.ends_with(".parquet") {
        Some(FaultKind::SstUpload)
    } else {
        None
    }
}

fn read_fault_kind(path: &str) -> Option<FaultKind> {
    if path.contains("/staging/") {
        Some(FaultKind::StagingRead)
    } else if path.ends_with(".parquet") {
        Some(FaultKind::SstRead)
    } else {
        None
    }
}

/// Whether a deleted path is a manifest delta or checkpoint.
fn is_manifest_file(path: &str) -> bool {
    let name = path.rsplit('/').next().unwrap_or(path);
    is_delta_file(name) || is_checkpoint_file(name)
}

fn chaos_error(kind: FaultKind) -> MockError {
    MockError::new(
        ErrorKind::Unexpected,
        format!("chaos: injected {} failure", kind.name()),
    )
}

/// Turns a decision into the action the wrapped operation must take.
async fn apply_injection(kind: FaultKind, injected: Injected) -> Option<MockError> {
    match injected {
        Injected::Pass => None,
        Injected::Wait(delay) => {
            tokio::time::sleep(delay).await;
            None
        }
        Injected::Fail => Some(chaos_error(kind)),
    }
}

struct ChaosWriter {
    inner: oio::Writer,
    faults: ChaosFaults,
    kind: FaultKind,
    path: String,
}

impl oio::Write for ChaosWriter {
    async fn write(&mut self, bytes: Buffer) -> MockResult<()> {
        let injected = self.faults.inject(self.kind, &self.path);
        if let Some(error) = apply_injection(self.kind, injected).await {
            return Err(error);
        }
        self.inner.write(bytes).await
    }

    async fn close(&mut self) -> MockResult<MockMetadata> {
        let injected = self.faults.inject(self.kind, &self.path);
        if let Some(error) = apply_injection(self.kind, injected).await {
            return Err(error);
        }
        self.inner.close().await
    }

    async fn abort(&mut self) -> MockResult<()> {
        self.inner.abort().await
    }
}

struct ChaosReader {
    inner: oio::Reader,
    faults: ChaosFaults,
    kind: FaultKind,
    path: String,
}

impl oio::Read for ChaosReader {
    async fn open(&self, range: BytesRange) -> MockResult<(RpRead, Box<dyn oio::ReadStreamDyn>)> {
        let injected = self.faults.inject(self.kind, &self.path);
        if let Some(error) = apply_injection(self.kind, injected).await {
            return Err(error);
        }
        self.inner.open(range).await
    }

    async fn read(&self, range: BytesRange) -> MockResult<(RpRead, Buffer)> {
        let injected = self.faults.inject(self.kind, &self.path);
        if let Some(error) = apply_injection(self.kind, injected).await {
            return Err(error);
        }
        self.inner.read(range).await
    }
}

struct ChaosDeleter {
    inner: oio::Deleter,
    faults: ChaosFaults,
}

impl oio::Delete for ChaosDeleter {
    async fn delete(&mut self, path: &str, args: OpDelete) -> MockResult<()> {
        if is_manifest_file(path) {
            let injected = self.faults.inject(FaultKind::ManifestDelete, path);
            if let Some(error) = apply_injection(FaultKind::ManifestDelete, injected).await {
                return Err(error);
            }
        }
        self.inner.delete(path, args).await
    }

    async fn close(&mut self) -> MockResult<()> {
        self.inner.close().await
    }
}

// ---------------------------------------------------------------------------
// L2: cancellation points
// ---------------------------------------------------------------------------

/// A one-shot blocking point on an engine callback.
///
/// `block` parks the engine task until the scenario calls `release`, and `wait_entered`
/// lets the scenario learn that the task is parked. Both use `Notify`'s stored permit, so a
/// scenario that arrives late still observes the parking instead of hanging.
struct Gate {
    armed: AtomicBool,
    entered: Semaphore,
    release: Semaphore,
}

impl Default for Gate {
    fn default() -> Self {
        Gate {
            armed: AtomicBool::new(false),
            entered: Semaphore::new(0),
            release: Semaphore::new(0),
        }
    }
}

impl Gate {
    /// Arms the gate for exactly one engine callback and drops every permit a previous step
    /// left behind, so an armed gate always parks.
    fn arm(&self) {
        while self.entered.try_acquire().is_ok() {}
        while self.release.try_acquire().is_ok() {}
        self.armed.store(true, Ordering::SeqCst);
    }

    async fn block(&self) {
        if !self.armed.swap(false, Ordering::SeqCst) {
            return;
        }
        self.entered.add_permits(1);
        let permit = self
            .release
            .acquire()
            .await
            .expect("the release semaphore is never closed");
        permit.forget();
    }

    async fn wait_entered(&self) {
        let permit = self
            .entered
            .acquire()
            .await
            .expect("the entered semaphore is never closed");
        permit.forget();
    }

    fn release(&self) {
        // Enough permits for every task this step may have parked.
        self.release.add_permits(8);
    }
}

/// The listener the chaos engine is created with: two parking points a compaction reaches
/// after it has read its inputs and before it has published its output.
#[derive(Default)]
struct ChaosListener {
    /// `on_merge_ssts_finished`: the output object is written, the region still serves the
    /// old version. Cancelling here is still possible.
    merge_finished: Gate,
    /// `on_compaction_commit_begin`: the commit is already non-cancellable.
    commit_begin: Gate,
}

#[async_trait]
impl EventListener for ChaosListener {
    async fn on_merge_ssts_finished(&self, _region_id: RegionId) {
        self.merge_finished.block().await;
    }

    async fn on_compaction_commit_begin(&self, _region_id: RegionId) {
        self.commit_begin.block().await;
    }
}

// ---------------------------------------------------------------------------
// L3: the model's engine is built with the levers attached
// ---------------------------------------------------------------------------

fn chaos_config() -> MitoConfig {
    MitoConfig {
        // An edit publishes no compaction of its own. A flush completion still schedules one
        // (`RegionWorkerLoop::schedule_compaction`), so a step boundary can hold a compaction
        // in flight; the view reads wait for the task list to empty before they judge a pair.
        schedule_compaction_after_edit: false,
        // GC on (the production setting) with the default lingering time, so a co-holder's
        // purge unlinks but does not delete a shared object mid-test.
        gc: GcConfig {
            enable: true,
            ..Default::default()
        },
        // Plain (non-flat) Parquet SSTs.
        default_flat_format: false,
        // One checkpoint per manifest delta: the checkpoint cleanup is the only delete the
        // engine issues in-process, and this makes it happen on every publication instead
        // of only every tenth one.
        manifest_checkpoint_distance: 1,
        ..Default::default()
    }
}

fn compact_request() -> RegionCompactRequest {
    RegionCompactRequest {
        options: compact_request::Options::StrictWindow(StrictWindow {
            window_seconds: COMPACTION_WINDOW_SECONDS,
        }),
        parallelism: None,
        time_range: None,
    }
}

// ---------------------------------------------------------------------------
// L4: chaos steps
// ---------------------------------------------------------------------------

/// Where a region compaction is cancelled.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CancelPoint {
    /// After the output SSTs are written, before the manifest is published: cancellable.
    AfterRead,
    /// Inside the commit, after the point of no return: the engine must refuse.
    InCommit,
}

impl CancelPoint {
    fn describe(self) -> &'static str {
        match self {
            CancelPoint::AfterRead => "cancel after read, before publish (cancellable window)",
            CancelPoint::InCommit => "cancel inside the commit (too late)",
        }
    }
}

/// Where a split publication is cut.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SplitAbort {
    /// The coordinator wrote the staging manifests and then vanished before creating a
    /// single target region.
    AfterRemap,
    /// The coordinator created and applied exactly one of the two targets, then vanished.
    AfterLeftApply,
    /// The coordinator's manifest write failed `times` times, so the split never produced a
    /// manifest the targets could consume.
    RemapWriteFault { times: usize },
}

impl SplitAbort {
    fn describe(self) -> String {
        match self {
            SplitAbort::AfterRemap => "abort split between remap_manifests and apply".to_string(),
            SplitAbort::AfterLeftApply => {
                "abort split after applying one of the two targets".to_string()
            }
            SplitAbort::RemapWriteFault { times } => {
                format!("remap_manifests staging write fails {times}x")
            }
        }
    }
}

/// One step of a chaos scenario: either a clean model step or one perturbation.
#[derive(Debug, Clone)]
enum ChaosOp {
    /// A step from the model's own vocabulary (write, flush, split, merge, compaction,
    /// import, multi-region compaction).
    Base(Op),
    /// Arm an SST upload fault, then flush until the fault budget is spent and the flush
    /// succeeds.
    UploadFault { times: usize },
    /// Arm an SST upload fault, then compact until the fault budget is spent and a
    /// compaction succeeds.
    CompactionFault { times: usize, choice: usize },
    /// Arm an SST read fault, then scan until the fault budget is spent.
    ReadFault { times: usize },
    /// Slow the SST upload down and scan while a flush is publishing it.
    UploadDelay { ms: u64 },
    /// Arm a manifest delete fault and drive the checkpoint cleanup that deletes.
    DeleteFault { times: usize },
    /// Cancel a region compaction at `point`.
    CancelCompaction { point: CancelPoint },
    /// Cut a split publication at `abort`, then hand the source back its domain.
    AbortSplit { abort: SplitAbort },
    /// Fail the read a staged target needs to consume the coordinator's staging manifest
    /// `times` times, then complete the split.
    StagingReadFault { times: usize },
    /// Two compactions of the same region at once.
    ConcurrentCompactions { choice: usize },
    /// A compaction and a scan of the same region at once.
    CompactionWithScan { choice: usize },
    /// A compaction racing a split publication.
    CompactionWithSplit { choice: usize },
    /// A compaction racing a merge publication.
    CompactionWithMerge,
    /// Recovery fold: close and reopen every live region.
    RecoverRegions,
    /// Recovery fold: stop the engine and open every live region on the reopened one.
    RecoverEngine,
}

impl ChaosOp {
    fn describe(&self) -> String {
        match self {
            ChaosOp::Base(op) => op.describe(),
            ChaosOp::UploadFault { times } => {
                format!(
                    "flush with {} failing {times}x",
                    FaultKind::SstUpload.name()
                )
            }
            ChaosOp::CompactionFault { times, choice } => format!(
                "region compaction (choice {choice}) with {} failing {times}x",
                FaultKind::SstUpload.name()
            ),
            ChaosOp::ReadFault { times } => {
                format!("scan with {} failing {times}x", FaultKind::SstRead.name())
            }
            ChaosOp::UploadDelay { ms } => {
                format!(
                    "flush with {} delayed {ms}ms, scanned while it runs",
                    FaultKind::SstUpload.name()
                )
            }
            ChaosOp::DeleteFault { times } => format!(
                "flush with {} failing {times}x",
                FaultKind::ManifestDelete.name()
            ),
            ChaosOp::CancelCompaction { point } => {
                format!("region compaction, {}", point.describe())
            }
            ChaosOp::AbortSplit { abort } => abort.describe(),
            ChaosOp::StagingReadFault { times } => format!(
                "split apply with {} failing {times}x",
                FaultKind::StagingRead.name()
            ),
            ChaosOp::ConcurrentCompactions { choice } => {
                format!("two compactions at once (choice {choice})")
            }
            ChaosOp::CompactionWithScan { choice } => {
                format!("compaction concurrent with a scan (choice {choice})")
            }
            ChaosOp::CompactionWithSplit { choice } => {
                format!("compaction concurrent with a split publication (choice {choice})")
            }
            ChaosOp::CompactionWithMerge => {
                "compaction concurrent with a merge publication".to_string()
            }
            ChaosOp::RecoverRegions => {
                "recovery fold: close and reopen every live region".to_string()
            }
            ChaosOp::RecoverEngine => "recovery fold: stop and reopen the engine".to_string(),
        }
    }

    /// The lever this step exercises, for the scenario table.
    fn lever(&self) -> &'static str {
        match self {
            ChaosOp::Base(_) => "none (clean step)",
            ChaosOp::UploadFault { .. } | ChaosOp::CompactionFault { .. } => {
                FaultKind::SstUpload.breaks()
            }
            ChaosOp::ReadFault { .. } => FaultKind::SstRead.breaks(),
            ChaosOp::UploadDelay { .. } => {
                "an upload is slow, so a scan runs against a half-published file"
            }
            ChaosOp::DeleteFault { .. } => FaultKind::ManifestDelete.breaks(),
            ChaosOp::CancelCompaction { .. } => "a compaction stops between reading and publishing",
            ChaosOp::AbortSplit { .. } => "a split stops between remap_manifests and apply",
            ChaosOp::StagingReadFault { .. } => FaultKind::StagingRead.breaks(),
            ChaosOp::ConcurrentCompactions { .. }
            | ChaosOp::CompactionWithScan { .. }
            | ChaosOp::CompactionWithSplit { .. }
            | ChaosOp::CompactionWithMerge => "two requests touch one region at once",
            ChaosOp::RecoverRegions | ChaosOp::RecoverEngine => {
                "the process forgets its memory and replays the disk"
            }
        }
    }
}

/// What one chaos step observed.
struct OpOutcome {
    /// What happened, quoted by the step context.
    description: String,
    /// The lever and its schedule, quoted by the failure report.
    perturbation: String,
    /// Violations found *before* the recovery fold, i.e. on the live engine.
    interim: Vec<(Category, String)>,
}

impl OpOutcome {
    fn clean(description: String, perturbation: String) -> OpOutcome {
        OpOutcome {
            description,
            perturbation,
            interim: Vec::new(),
        }
    }
}

/// Checks the oracle and every invariant category once.
async fn check_all(model: &mut StorageModel, ctx: &str) -> Vec<(Category, String)> {
    let mut violations = Vec::new();
    if let Some(report) = model.check(ctx).await {
        violations.push((Category::Visibility, report));
    }
    violations.extend(
        model
            .check_invariants(ctx)
            .await
            .into_iter()
            .map(|violation| (violation.category, violation.detail)),
    );
    violations
}

impl StorageModel {
    /// A scan that reports the object-store error instead of unwrapping it.
    async fn try_scan_counts(&self, region_id: RegionId) -> Result<BTreeMap<i64, usize>, String> {
        let stream = self
            .engine
            .scan_to_stream(region_id, ScanRequest::default())
            .await
            .map_err(|error| error.to_string())?;
        let batches = RecordBatches::try_collect(stream)
            .await
            .map_err(|error| error.to_string())?;
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
        Ok(counts)
    }

    /// The per-region expectation: every committed event its domain owns, exactly once.
    fn expected_in(&self, region_id: RegionId) -> BTreeMap<i64, usize> {
        self.ledger
            .ids_in(self.domain_of(region_id))
            .into_iter()
            .map(|event_id| (event_id, 1))
            .collect()
    }

    /// Waits until no compaction cycle is left in the task list.
    async fn settle(&self) -> bool {
        tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                match self.engine.inspect_compaction_tasks(&[]).await {
                    Ok(tasks) if tasks.is_empty() => break,
                    Ok(_) => tokio::task::yield_now().await,
                    Err(_) => break,
                }
            }
        })
        .await
        .is_ok()
    }

    /// Issues a region compaction request and reports whether it landed.
    async fn compact_request(&mut self, region_id: RegionId) -> Result<(), String> {
        self.engine
            .handle_request(region_id, RegionRequest::Compact(compact_request()))
            .await
            .map(|_| ())
            .map_err(|error| error.to_string())
    }

    /// Recovery fold on the live regions: close and reopen each one, so the check that
    /// follows reads the replayed state rather than the in-memory one.
    async fn recover_regions(&mut self) -> String {
        let live = self.live_regions();
        for region_id in &live {
            reopen_region(
                &self.engine,
                *region_id,
                self.table_dir.clone(),
                true,
                HashMap::new(),
            )
            .await;
        }
        format!("recovered {} region(s) in place", live.len())
    }

    /// Recovery fold on the process: stop the engine, open a new one and open every live
    /// region on it.
    async fn recover_engine(&mut self) -> String {
        // Every region the harness ever created is on disk, so every one of them is opened
        // again: the fold must not lose a region because it retired earlier.
        let regions: Vec<RegionId> = self.tracked.iter().copied().collect();
        let live: BTreeSet<RegionId> = self.routing.keys().copied().collect();
        self.engine
            .stop()
            .await
            .expect("the engine must stop for a recovery fold");
        self.engine = self.env.open_engine(self.config.clone()).await;
        for region_id in &regions {
            self.engine
                .handle_request(
                    *region_id,
                    RegionRequest::Open(RegionOpenRequest {
                        engine: MITO_ENGINE_NAME.to_string(),
                        table_dir: self.table_dir.clone(),
                        path_type: self.path_type,
                        options: HashMap::new(),
                        skip_wal_replay: false,
                        checkpoint: None,
                        requirements: Default::default(),
                    }),
                )
                .await
                .unwrap_or_else(|error| {
                    panic!("region {region_id} must open after the engine restart: {error}")
                });
            // A live region serves the table again; a retired one must not accept writes
            // for a domain another region already took over.
            let role = if live.contains(region_id) {
                RegionRole::Leader
            } else {
                RegionRole::Follower
            };
            self.engine.set_region_role(*region_id, role).unwrap();
        }
        format!(
            "restarted the engine and opened {} region(s) ({} live)",
            regions.len(),
            live.len()
        )
    }

    /// A live region whose domain the ledger can cut into two non-empty halves.
    fn split_target(&self, choice: usize) -> Option<(RegionId, usize)> {
        self.live_regions().iter().find_map(|region_id| {
            self.split_cut(self.domain_of(*region_id), choice)
                .map(|cut| (*region_id, cut))
        })
    }

    fn live_target(&self, choice: usize) -> RegionId {
        let live = self.live_regions();
        assert!(!live.is_empty(), "a chaos step needs a live region");
        live[choice % live.len()]
    }
}

/// Runs one chaos step.
async fn run_op(model: &mut StorageModel, op: &ChaosOp) -> OpOutcome {
    match op {
        ChaosOp::Base(op) => {
            let description = model.apply(op).await;
            OpOutcome::clean(description, format!("clean step: {}", op.describe()))
        }

        ChaosOp::UploadFault { times } => {
            let before = model.faults.hit_count();
            model.faults.arm(FaultKind::SstUpload, *times);
            let mut failures = Vec::new();
            // The budget is finite: after at most `times` failures a flush must land.
            for attempt in 0..(*times + 4) {
                if model.dirty.is_empty() {
                    break;
                }
                let dirty: Vec<RegionId> = model.dirty.iter().copied().collect();
                for region_id in dirty {
                    match model
                        .engine
                        .handle_request(
                            region_id,
                            RegionRequest::Flush(RegionFlushRequest::default()),
                        )
                        .await
                    {
                        Ok(_) => {
                            model.dirty.remove(&region_id);
                            model.spec.flushed(region_id);
                        }
                        Err(error) => failures.push(format!(
                            "attempt {attempt} flush of {region_id} failed: {error}"
                        )),
                    }
                }
            }
            model.faults.disarm(FaultKind::SstUpload);
            let flushed = model.flush_dirty().await;
            let hits = model.faults.last_hits(before);
            OpOutcome::clean(
                format!(
                    "with {}: {} failed flush(es), then {flushed} region(s) flushed; faults fired: {hits:?}",
                    FaultKind::SstUpload.name(),
                    failures.len()
                ),
                format!(
                    "{} x{times} on the flush path; a failed flush must leave the rows \
                     committed but unpublished, and the retry must publish them once",
                    FaultKind::SstUpload.name()
                ),
            )
        }

        ChaosOp::CompactionFault { times, choice } => {
            let target = model.live_target(*choice);
            model.flush_region(target).await;
            let before = model.faults.hit_count();
            model.faults.arm(FaultKind::SstUpload, *times);
            let mut outcomes = Vec::new();
            for _ in 0..(*times + 2) {
                match model.compact_request(target).await {
                    Ok(()) => {
                        model.spec.compact(target);
                        outcomes.push("compaction published".to_string());
                        break;
                    }
                    Err(error) => outcomes.push(format!("compaction failed: {error}")),
                }
            }
            model.faults.disarm(FaultKind::SstUpload);
            model.settle().await;
            let hits = model.faults.last_hits(before);
            OpOutcome::clean(
                format!("{target}: {outcomes:?}; faults fired: {hits:?}"),
                format!(
                    "{} x{times} on the compaction output; a failed compaction must not \
                     half-publish a version",
                    FaultKind::SstUpload.name()
                ),
            )
        }

        ChaosOp::ReadFault { times } => {
            // A read fault only fires on a *cold* object: the engine keeps the pages of a
            // file it already scanned, so a scan of an old file never touches the store.
            // This step publishes a fresh SST and reads that.
            let target = model.live_target(0);
            let key = model.domain_of(target).lo;
            let rows = vec![PlannedRow {
                key,
                ts: 0,
                payload: 1.0,
            }];
            model.write(&rows).await;
            model.flush_dirty().await;
            let before = model.faults.hit_count();
            model.faults.arm(FaultKind::SstRead, *times);
            let mut errors = Vec::new();
            let mut recovered = None;
            // The budget is finite: after at most `times` read failures a scan must land.
            for attempt in 0..(*times + 8) {
                match model.try_scan_counts(target).await {
                    Ok(counts) => {
                        recovered = Some(counts);
                        break;
                    }
                    Err(error) => errors.push(format!("scan {attempt} failed: {error}")),
                }
            }
            model.faults.disarm(FaultKind::SstRead);
            let counts = recovered.unwrap_or_else(|| {
                panic!(
                    "no scan of {target} succeeded after {} read faults: {errors:?}",
                    times
                )
            });
            let hits = model.faults.last_hits(before);
            assert!(
                !errors.is_empty(),
                "{} x{times} never fired on the scan path (hits: {hits:?})",
                FaultKind::SstRead.name()
            );
            OpOutcome::clean(
                format!(
                    "with {}: {} failed scan(s), then {} distinct event(s); faults fired: {hits:?}",
                    FaultKind::SstRead.name(),
                    errors.len(),
                    counts.len()
                ),
                format!(
                    "{} x{times} on the scan path; a scan that cannot read one object must \
                     fail as a whole, never return a partial row set",
                    FaultKind::SstRead.name()
                ),
            )
        }

        ChaosOp::UploadDelay { ms } => {
            let target = model
                .dirty
                .iter()
                .copied()
                .next()
                .unwrap_or_else(|| model.live_target(0));
            let before = model.faults.hit_count();
            // Make sure the flush has something to publish.
            if !model.dirty.contains(&target) {
                return OpOutcome::clean(
                    "flush delay (nothing to flush)".to_string(),
                    format!(
                        "{} delayed {ms}ms (no dirty region)",
                        FaultKind::SstUpload.name()
                    ),
                );
            }
            model.faults.arm_delay(FaultKind::SstUpload, *ms);
            let engine = model.engine.clone();
            let flush = tokio::spawn(async move {
                engine
                    .handle_request(target, RegionRequest::Flush(RegionFlushRequest::default()))
                    .await
            });
            let expected = model.expected_in(target);
            let mut interim = Vec::new();
            let mut scans = 0;
            while !flush.is_finished() && scans < 128 {
                scans += 1;
                match model.try_scan_counts(target).await {
                    Ok(counts) => {
                        if counts != expected {
                            interim.push((
                                Category::Visibility,
                                format!(
                                    "scan {scans} of {target} while a flush is publishing saw \
                                     {counts:?}, the ledger expects {expected:?}"
                                ),
                            ));
                            break;
                        }
                    }
                    Err(error) => interim.push((
                        Category::Visibility,
                        format!("scan {scans} of {target} during a flush failed: {error}"),
                    )),
                }
                tokio::task::yield_now().await;
            }
            let joined = flush.await.unwrap();
            model.faults.disarm(FaultKind::SstUpload);
            if joined.is_ok() {
                model.dirty.remove(&target);
                model.spec.flushed(target);
            }
            let hits = model.faults.last_hits(before);
            let mut outcome = OpOutcome::clean(
                format!(
                    "flush finished after {scans} scan(s) ran against it: {:?}; faults fired: {hits:?}",
                    joined
                        .as_ref()
                        .map(|_| "ok")
                        .map_err(|error| error.to_string())
                ),
                format!(
                    "{} delayed {ms}ms while a scan reads the same region; a slow publication \
                     must never expose a half-written file",
                    FaultKind::SstUpload.name()
                ),
            );
            outcome.interim = interim;
            outcome
        }

        ChaosOp::DeleteFault { times } => {
            let target = model.live_target(0);
            let before = model.faults.hit_count();
            model.faults.arm(FaultKind::ManifestDelete, *times);
            let mut outcomes = Vec::new();
            // Each flush writes a manifest delta; with `manifest_checkpoint_distance = 1` the
            // checkpoint that follows deletes the covered delta through the object store.
            for _ in 0..(*times + 2).min(4) {
                let rows = vec![PlannedRow {
                    key: 0,
                    ts: model.step as i64,
                    payload: 1.0,
                }];
                model.write(&rows).await;
                let flushed = model.flush_dirty().await;
                outcomes.push(format!("{flushed} flush(es) published"));
            }
            model.faults.disarm(FaultKind::ManifestDelete);
            // The retry: one more publication whose cleanup meets no fault.
            let rows = vec![PlannedRow {
                key: 0,
                ts: 1000,
                payload: 2.0,
            }];
            model.write(&rows).await;
            let flushed = model.flush_dirty().await;
            let hits = model.faults.last_hits(before);
            assert!(
                !hits.is_empty(),
                "{} x{times} never fired on the checkpoint cleanup (publications: {outcomes:?})",
                FaultKind::ManifestDelete.name()
            );
            OpOutcome::clean(
                format!("{target}: {outcomes:?}, then {flushed} retried; faults fired: {hits:?}"),
                format!(
                    "{} x{times} on the checkpoint cleanup; a delete that fails must not lose \
                     the file it kept or the delta it was covering",
                    FaultKind::ManifestDelete.name()
                ),
            )
        }

        ChaosOp::CancelCompaction { point } => {
            let target = model.live_target(0);
            // Two L0 files, so the compaction has an output to publish.
            model.flush_region(target).await;
            if model.dirty.contains(&target) {
                model.flush_region(target).await;
            }
            // The file set the cancellation must not move, read at rest *before* the gate is
            // armed: a parked compaction keeps the task list non-empty, so a view read while it
            // is parked would fail instead of observing.
            model.refresh_views().await;
            let before_files = model.holder_files(target);
            let gate = match point {
                CancelPoint::AfterRead => &model.gates.merge_finished,
                CancelPoint::InCommit => &model.gates.commit_begin,
            };
            gate.arm();
            let engine = model.engine.clone();
            let compact = tokio::spawn(async move {
                engine
                    .handle_request(target, RegionRequest::Compact(compact_request()))
                    .await
            });
            let entered = tokio::time::timeout(Duration::from_secs(30), gate.wait_entered()).await;
            if entered.is_err() {
                gate.release();
                let joined = compact.await.unwrap();
                // The gate was never reached, so the perturbation never ran and this step proved
                // nothing: a scenario that silently stops injecting has to be visible, so it
                // fails instead of reporting a clean step.
                let mut outcome = OpOutcome::clean(
                    format!("compaction on {target} never reached the gate: {joined:?}"),
                    format!(
                        "{} (unreachable: the engine never called the gate)",
                        point.describe()
                    ),
                );
                outcome.interim.push((
                    Category::Perturbation,
                    format!(
                        "the {} lever never fired: the compaction on {target} never reached the \
                         engine callback it cancels at, so the step observed a plain compaction \
                         and its report is not evidence about cancellation ({joined:?})",
                        point.describe()
                    ),
                ));
                return outcome;
            }
            // While the compaction is parked, ask the engine to cancel this exact cycle.
            let cancel = match model
                .engine
                .inspect_compaction_tasks(&[])
                .await
                .unwrap()
                .into_iter()
                .find(|task: &CompactionTaskEntry| task.region_id == target)
            {
                Some(task) => {
                    let task_id = task.task_id.to_string();
                    Some(
                        model
                            .engine
                            .handle_request(
                                target,
                                RegionRequest::CancelCompaction(RegionCancelCompactionRequest {
                                    task_id,
                                }),
                            )
                            .await
                            .map(|result| result.affected_rows)
                            .map_err(|error| error.to_string()),
                    )
                }
                None => None,
            };
            gate.release();
            let joined = compact.await.unwrap();
            let settled = model.settle().await;
            // Re-read at rest: the comparison below is between two observed file sets, not
            // between one observation and the model's memory of it.
            model.refresh_views().await;
            let after_files = model.holder_files(target);
            let mut outcome = OpOutcome::clean(
                format!(
                    "compaction on {target}: cancel {:?}, request {:?}, files {} -> {} after settle({settled})",
                    cancel,
                    joined
                        .as_ref()
                        .map(|_| "ok")
                        .map_err(|error| error.to_string()),
                    before_files.len(),
                    after_files.len()
                ),
                format!(
                    "{}: the request must either abort without publishing, or publish exactly \
                     once after refusing the cancel",
                    point.describe()
                ),
            );
            match point {
                CancelPoint::AfterRead => {
                    // A cancelled cycle must not publish: the live file set of the region after
                    // the cancellation is the set it had when the cancel was issued.
                    if joined.is_ok() {
                        outcome.interim.push((
                            Category::ManifestConsistency,
                            format!(
                                "the cancel of a parked compaction on {target} was accepted but \
                                 the request still reported success ({} -> {} files)",
                                before_files.len(),
                                after_files.len()
                            ),
                        ));
                    }
                    if before_files != after_files {
                        outcome.interim.push((
                            Category::ManifestConsistency,
                            format!(
                                "the cancel of a parked compaction on {target} moved the region's \
                                 file set: before {before_files:?}, after {after_files:?} after \
                                 settle({settled}); a cancelled cycle publishes nothing"
                            ),
                        ));
                    }
                }
                CancelPoint::InCommit => {
                    if joined.is_ok() {
                        model.spec.compact(target);
                    }
                }
            }
            outcome
        }

        ChaosOp::AbortSplit { abort } => abort_split(model, *abort).await,

        ChaosOp::StagingReadFault { times } => staging_read_fault(model, *times).await,

        ChaosOp::ConcurrentCompactions { choice } => {
            let target = model.live_target(*choice);
            model.flush_region(target).await;
            let first = model.engine.clone();
            let second = model.engine.clone();
            let a = tokio::spawn(async move {
                first
                    .handle_request(target, RegionRequest::Compact(compact_request()))
                    .await
            });
            let b = tokio::spawn(async move {
                second
                    .handle_request(target, RegionRequest::Compact(compact_request()))
                    .await
            });
            let (first, second) = tokio::join!(a, b);
            let outcomes = vec![
                first
                    .unwrap()
                    .map(|_| ())
                    .map_err(|error| error.to_string()),
                second
                    .unwrap()
                    .map(|_| ())
                    .map_err(|error| error.to_string()),
            ];
            let published = outcomes.iter().filter(|result| result.is_ok()).count();
            for _ in 0..published {
                model.spec.compact(target);
            }
            let settled = model.settle().await;
            OpOutcome::clean(
                format!("two compactions of {target}: {outcomes:?}, settle({settled})"),
                "two compactions of one region at once: they may serialise or one may be \
                 rejected, but the visible rows may not move"
                    .to_string(),
            )
        }

        ChaosOp::CompactionWithScan { choice } => {
            let target = model.live_target(*choice);
            model.flush_region(target).await;
            if model.dirty.contains(&target) {
                model.flush_region(target).await;
            }
            let expected = model.expected_in(target);
            let engine = model.engine.clone();
            let compact = tokio::spawn(async move {
                engine
                    .handle_request(target, RegionRequest::Compact(compact_request()))
                    .await
            });
            let mut interim = Vec::new();
            let mut scans = 0;
            while !compact.is_finished() && scans < 256 {
                scans += 1;
                match model.try_scan_counts(target).await {
                    Ok(counts) => {
                        if counts != expected {
                            interim.push((
                                Category::Visibility,
                                format!(
                                    "scan {scans} of {target} during its own compaction saw \
                                     {counts:?}, the ledger expects {expected:?}"
                                ),
                            ));
                            break;
                        }
                    }
                    Err(error) => interim.push((
                        Category::Visibility,
                        format!("scan {scans} of {target} during its compaction failed: {error}"),
                    )),
                }
                tokio::task::yield_now().await;
            }
            let joined = compact.await.unwrap();
            if joined.is_ok() {
                model.spec.compact(target);
            }
            let settled = model.settle().await;
            let mut outcome = OpOutcome::clean(
                format!(
                    "{scans} scan(s) ran against the compaction of {target}: request {:?}, \
                     settle({settled})",
                    joined
                        .as_ref()
                        .map(|_| "ok")
                        .map_err(|error| error.to_string())
                ),
                "a scan and a compaction of one region at once: every scan must see each \
                 committed event exactly once"
                    .to_string(),
            );
            outcome.interim = interim;
            outcome
        }

        ChaosOp::CompactionWithSplit { choice } => {
            abort_split_with_compaction(model, *choice).await
        }

        ChaosOp::CompactionWithMerge => abort_split_with_compaction(model, usize::MAX).await,

        ChaosOp::RecoverRegions => {
            let described = model.recover_regions().await;
            OpOutcome::clean(described.clone(), described)
        }

        ChaosOp::RecoverEngine => {
            let described = model.recover_engine().await;
            OpOutcome::clean(described.clone(), described)
        }
    }
}

/// Publishes a split or a merge the way the coordinator does, but runs a region compaction
/// of one input between the remap and the apply: the compaction reads the version the
/// coordinator copies and publishes after it.
async fn abort_split_with_compaction(model: &mut StorageModel, choice: usize) -> OpOutcome {
    let live = model.live_regions();
    let merge = choice == usize::MAX;
    if merge && live.len() < 2 {
        return OpOutcome::clean(
            format!(
                "publication race ({} live region(s), no merge shape)",
                live.len()
            ),
            "compaction concurrent with a merge publication (no shape)".to_string(),
        );
    }
    model.flush_dirty().await;
    let inputs = live;
    // What scheme B says about the merge this scenario races, decided before the engine is
    // driven through it so a refusal can be told apart from a publication.
    let pre_race = model.spec.merge_verdict(&inputs);
    // A split publication must cut a region whose domain really has two halves; a merge
    // compacts the first input.
    let target = if merge {
        inputs[0]
    } else {
        match model.split_target(choice) {
            Some((region_id, _)) => region_id,
            None => {
                return OpOutcome::clean(
                    "publication race (no live region can be cut)".to_string(),
                    "compaction concurrent with a split publication (no cut)".to_string(),
                );
            }
        }
    };
    // The publication retires its inputs before it reads their manifests: a merge retires
    // every live region, a split only the region whose domain it cuts.
    let demoted: Vec<RegionId> = if merge { inputs.clone() } else { vec![target] };
    let mut demotion_failures = Vec::new();
    for input in &demoted {
        if let Err(error) = model
            .engine
            .set_region_role_state_gracefully(*input, SettableRegionRoleState::StagingLeader)
            .await
        {
            demotion_failures.push(format!("{input}: {error}"));
        }
    }
    let engine = model.engine.clone();
    let compact = tokio::spawn(async move {
        engine
            .handle_request(target, RegionRequest::Compact(compact_request()))
            .await
    });

    let mut created: Vec<RegionId> = Vec::new();
    let publication: Result<Publication, String> = if merge {
        let merged = model.alloc_region_id();
        created.push(merged);
        model.create_and_enter_staging(merged, Domain::FULL).await;
        let response = model
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
            .await;
        match response {
            Err(error) => Err(format!("remap_manifests failed: {error}")),
            Ok(response) => model
                .apply_staging_manifest_result(
                    merged,
                    Domain::FULL,
                    merged,
                    &response.manifest_paths,
                )
                .await
                .map(|()| Publication::Merged(merged)),
        }
    } else {
        let domain = model.domain_of(target);
        match model.split_cut(domain, choice) {
            None => Err("no cut with two distinct keys".to_string()),
            Some(cut) => {
                let left_domain = Domain::new(domain.lo, cut);
                let right_domain = Domain::new(cut, domain.hi);
                let left = model.alloc_region_id();
                let right = model.alloc_region_id();
                created.push(left);
                created.push(right);
                let response = model
                    .engine
                    .remap_manifests(RemapManifestsRequest {
                        region_id: target,
                        input_regions: vec![target],
                        region_mapping: [(target, vec![left, right])].into_iter().collect(),
                        new_partition_exprs: [
                            (left, left_domain.expr_json()),
                            (right, right_domain.expr_json()),
                        ]
                        .into_iter()
                        .collect(),
                    })
                    .await;
                match response {
                    Err(error) => Err(format!("remap_manifests failed: {error}")),
                    Ok(response) => {
                        let mut result = Ok(Publication::Split {
                            left,
                            left_domain,
                            right,
                            right_domain,
                        });
                        for (region_id, target_domain) in
                            [(left, left_domain), (right, right_domain)]
                        {
                            model
                                .create_and_enter_staging(region_id, target_domain)
                                .await;
                            if let Err(error) = model
                                .apply_staging_manifest_result(
                                    region_id,
                                    target_domain,
                                    target,
                                    &response.manifest_paths,
                                )
                                .await
                            {
                                result = Err(format!("apply of {region_id} failed: {error}"));
                                break;
                            }
                        }
                        result
                    }
                }
            }
        }
    };

    // The compaction publishes while the publication holds the copied layout: the routing
    // switches only after the join.
    let compacted = compact.await.unwrap();
    let settled = model.settle().await;
    let compacted = compacted.map(|_| ()).map_err(|error| error.to_string());

    let mut outcome = OpOutcome::clean(
        format!(
            "publication of {inputs:?} raced a compaction of {target} (demotion failures \
             {demotion_failures:?}): publication {publication:?}, compaction {compacted:?}, \
             settle({settled})"
        ),
        "a compaction and a publication of the same region at once: whichever order the engine \
         picks, every committed event must stay visible exactly once"
            .to_string(),
    );
    match publication {
        Ok(Publication::Split {
            left,
            left_domain,
            right,
            right_domain,
        }) => {
            model.tracked.insert(left);
            model.tracked.insert(right);
            model.routing.remove(&target);
            model.routing.insert(left, left_domain);
            model.routing.insert(right, right_domain);
            model
                .spec
                .split(target, left, left_domain, right, right_domain);
            if compacted.is_ok() {
                // The compaction published into the region that now serves a half.
                model.spec.compact(target);
            }
        }
        Ok(Publication::Merged(merged)) => {
            model.tracked.insert(merged);
            model.routing = BTreeMap::from([(merged, Domain::FULL)]);
            // Scheme B refuses a merge whose participants hold a foreign file, so this raced
            // publication must not have published anything: it did.
            let verdict = model.spec.merge_verdict(&inputs);
            assert!(
                verdict.is_allowed(),
                "SCHEME B DIVERGES FROM THE ENGINE: {}. The raced merge publication of {} \
                 published survivor {merged} instead of being refused",
                verdict.describe(),
                describe_regions(&inputs)
            );
            model.spec.merge(&inputs, merged);
        }
        Err(error) => {
            // The publication never switched the routing: close what it created and hand the
            // inputs their own domain back.
            for region_id in &created {
                let _ = model
                    .engine
                    .handle_request(
                        *region_id,
                        RegionRequest::Close(RegionCloseRequest::default()),
                    )
                    .await;
                model.tracked.remove(region_id);
            }
            for input in &demoted {
                model
                    .engine
                    .set_region_role_state_gracefully(*input, SettableRegionRoleState::Leader)
                    .await
                    .unwrap();
            }
            outcome.description = format!("{} [publication aborted: {error}]", outcome.description);
            // Scheme B expects exactly this refusal while a participant still holds a foreign
            // file: compact the offending sides and retry the merge.
            if merge && !pre_race.is_allowed() {
                model.spec.note_refused_merge(&pre_race);
                let retried = model.merge_all().await;
                outcome.description = format!(
                    "{} [scheme B refused the raced merge publication, compacted {:?} and retried \
                     it into {retried}]",
                    outcome.description,
                    pre_race.offending_regions()
                );
            }
        }
    }
    outcome
}

/// Renders the regions of a merge input list for a refusal message.
fn describe_regions(inputs: &[RegionId]) -> String {
    inputs
        .iter()
        .map(|region_id| region_id.to_string())
        .collect::<Vec<_>>()
        .join(", ")
}

/// Renders the offending references a scheme B refusal has to name.
fn describe_offenders(offenders: &[ForeignRef<RegionId>]) -> String {
    offenders
        .iter()
        .map(|offender| offender.describe())
        .collect::<Vec<_>>()
        .join("; ")
}

/// What a publication left behind on the engine.
#[derive(Debug)]
enum Publication {
    Split {
        left: RegionId,
        left_domain: Domain,
        right: RegionId,
        right_domain: Domain,
    },
    Merged(RegionId),
}

impl StorageModel {
    /// [`StorageModel::apply_staging_manifest`] with the error kept instead of unwrapped.
    async fn apply_staging_manifest_result(
        &mut self,
        region_id: RegionId,
        domain: Domain,
        central_region_id: RegionId,
        manifest_paths: &HashMap<RegionId, String>,
    ) -> Result<(), String> {
        let manifest_path = manifest_paths
            .get(&region_id)
            .ok_or_else(|| format!("coordinator published no manifest for {region_id}"))?
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
            .map(|_| ())
            .map_err(|error| error.to_string())
    }
}

/// Cuts a split publication at `abort` and hands the source its domain back.
async fn abort_split(model: &mut StorageModel, abort: SplitAbort) -> OpOutcome {
    model.flush_dirty().await;
    let Some((source, cut)) = model.split_target(0) else {
        return OpOutcome::clean(
            "aborted split (no live region with two distinct keys)".to_string(),
            abort.describe(),
        );
    };
    let domain = model.domain_of(source);
    let left_domain = Domain::new(domain.lo, cut);
    let right_domain = Domain::new(cut, domain.hi);
    let left = model.alloc_region_id();
    let right = model.alloc_region_id();

    model
        .engine
        .set_region_role_state_gracefully(source, SettableRegionRoleState::StagingLeader)
        .await
        .unwrap();

    let before = model.faults.hit_count();
    if let SplitAbort::RemapWriteFault { times } = abort {
        model.faults.arm(FaultKind::StagingWrite, times);
    }
    let remap = model
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
        .await;
    model.faults.disarm(FaultKind::StagingWrite);
    let hits = model.faults.last_hits(before);

    let mut steps = Vec::new();
    match remap {
        Err(error) => steps.push(format!("remap_manifests failed: {error}")),
        Ok(response) => {
            steps.push("remap_manifests published the staging manifests".to_string());
            match abort {
                SplitAbort::AfterRemap => {
                    // The coordinator died before creating a single target.
                }
                SplitAbort::AfterLeftApply => {
                    model.create_and_enter_staging(left, left_domain).await;
                    let applied = model
                        .apply_staging_manifest_result(
                            left,
                            left_domain,
                            source,
                            &response.manifest_paths,
                        )
                        .await;
                    steps.push(format!("left target {left} apply: {applied:?}"));
                    model.create_and_enter_staging(right, right_domain).await;
                }
                SplitAbort::RemapWriteFault { .. } => {
                    steps.push("remap reported a manifest path despite the fault".to_string());
                }
            }
        }
    }

    // The abort: the coordinator gives up, closes what it created and hands the source its
    // own domain back so it keeps serving.
    for target in [left, right] {
        let closed = model
            .engine
            .handle_request(target, RegionRequest::Close(RegionCloseRequest::default()))
            .await;
        steps.push(format!("close {target}: {:?}", closed.map(|_| ())));
    }
    model
        .engine
        .set_region_role_state_gracefully(source, SettableRegionRoleState::Leader)
        .await
        .unwrap();
    steps.push(format!("{source} back to Leader"));

    OpOutcome::clean(
        format!("{}; faults fired: {hits:?}", steps.join(", ")),
        abort.describe(),
    )
}

/// Fails the read a staged target needs in order to consume the coordinator's manifest, then
/// completes the split.
///
/// `ApplyStagingManifest` opens the coordinator's staging manifest, so that read is the one the
/// fault hits: a target that cannot read it must not half-publish, and the coordinator's retry
/// has to land. The lever must fire — a scenario whose fault never reaches the engine proves
/// nothing, so a silent lever fails the step instead of passing it.
async fn staging_read_fault(model: &mut StorageModel, times: usize) -> OpOutcome {
    let perturbation = format!("{} fails {times}x", FaultKind::StagingRead.name());
    model.flush_dirty().await;
    let Some((source, cut)) = model.split_target(0) else {
        // Nothing to split means nothing to apply, so the lever cannot fire: the step would
        // pass while proving nothing.
        let mut outcome = OpOutcome::clean(
            "staging read fault (no live region with two distinct keys)".to_string(),
            perturbation,
        );
        outcome.interim.push((
            Category::Perturbation,
            "no live region with two distinct keys, so the split whose staging read this step \
             fails was never issued"
                .to_string(),
        ));
        return outcome;
    };
    let domain = model.domain_of(source);
    let left_domain = Domain::new(domain.lo, cut);
    let right_domain = Domain::new(cut, domain.hi);
    let left = model.alloc_region_id();
    let right = model.alloc_region_id();

    model
        .engine
        .set_region_role_state_gracefully(source, SettableRegionRoleState::StagingLeader)
        .await
        .unwrap();

    let before = model.faults.hit_count();
    model.faults.arm(FaultKind::StagingRead, times);
    let remap = model
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
        .await;
    let response = match remap {
        Ok(response) => response,
        Err(error) => {
            // Without a manifest there is nothing to apply: hand the source back its domain.
            model.faults.disarm(FaultKind::StagingRead);
            model
                .engine
                .set_region_role_state_gracefully(source, SettableRegionRoleState::Leader)
                .await
                .unwrap();
            return OpOutcome::clean(
                format!("{source}: remap_manifests failed: {error}"),
                perturbation,
            );
        }
    };

    // The first target is the one whose apply meets the fault; its retry must land.
    model.create_and_enter_staging(left, left_domain).await;
    let mut failures = Vec::new();
    let mut applied = false;
    for _ in 0..(times + 2) {
        match model
            .apply_staging_manifest_result(left, left_domain, source, &response.manifest_paths)
            .await
        {
            Ok(()) => {
                applied = true;
                break;
            }
            Err(error) => failures.push(error),
        }
    }
    model.faults.disarm(FaultKind::StagingRead);
    let hits = model.faults.last_hits(before);
    let mut outcome = OpOutcome::clean(
        format!(
            "{source} -> {left}, {right}: {} failed apply(ies) {failures:?}, then \
             applied={applied}; faults fired: {hits:?}",
            failures.len()
        ),
        perturbation,
    );
    if failures.is_empty() {
        // The lever is armed but the engine never read a staging manifest, so the step would
        // pass while proving nothing about a target that cannot consume the coordinator's
        // manifest: that is a hole in the coverage, not a clean step.
        outcome.interim.push((
            Category::Perturbation,
            format!(
                "{} x{times} never fired on the staging read path (hits: {hits:?}): \
                 ApplyStagingManifest on {left} never read the coordinator's manifest",
                FaultKind::StagingRead.name()
            ),
        ));
        return outcome;
    }
    if !applied {
        outcome.interim.push((
            Category::Perturbation,
            format!(
                "the retry of ApplyStagingManifest on {left} never landed after {} \
                 failure(s): {failures:?}",
                failures.len()
            ),
        ));
        return outcome;
    }

    model.create_and_enter_staging(right, right_domain).await;
    model
        .apply_staging_manifest(right, right_domain, source, &response.manifest_paths)
        .await;
    model.retire_split_source(source).await;
    model.tracked.insert(left);
    model.tracked.insert(right);
    model.routing.insert(left, left_domain);
    model.routing.insert(right, right_domain);
    model
        .spec
        .split(source, left, left_domain, right, right_domain);
    model.refresh_views().await;
    outcome
}

// ---------------------------------------------------------------------------
// L5: scenarios
// ---------------------------------------------------------------------------

/// One deterministic chaos scenario.
struct Scenario {
    name: &'static str,
    seed: u64,
    ops: Vec<ChaosOp>,
}

/// One failing step of a scenario.
struct Failure {
    scenario: String,
    seed: u64,
    step: usize,
    op: String,
    perturbation: String,
    categories: Vec<&'static str>,
    details: Vec<String>,
    faults: Vec<String>,
}

impl Failure {
    fn describe(&self) -> String {
        format!(
            "=== chaos scenario failure ===\n\
             scenario: {scenario}\n\
             seed: {seed} ({seed:#018x})  (replay: {scenario} with seed {seed:#018x})\n\
             first violating step: {step}\n\
             operation: {op}\n\
             perturbation: {perturbation}\n\
             invariant categories: {categories:?}\n\
             object-store faults that fired: {faults:?}\n\
             {details}",
            scenario = self.scenario,
            seed = self.seed,
            step = self.step,
            op = self.op,
            perturbation = self.perturbation,
            categories = self.categories,
            faults = self.faults,
            details = self.details.join("\n"),
        )
    }
}

/// Everything one scenario replay observed.
struct Report {
    name: &'static str,
    seed: u64,
    steps: usize,
    failure: Option<Failure>,
    observations: Vec<(usize, Category, String)>,
}

impl Report {
    fn verdict(&self) -> String {
        match &self.failure {
            None => format!(
                "PASS: {} step(s) of `{}` (seed {:#018x}) held every invariant",
                self.steps, self.name, self.seed
            ),
            Some(failure) => format!(
                "FAIL: {} at step {} of `{}` (seed {:#018x}), categories {:?}",
                failure.op, failure.step, self.name, self.seed, failure.categories
            ),
        }
    }

    fn observations_summary(&self) -> String {
        let mut seen: BTreeMap<Category, (usize, usize)> = BTreeMap::new();
        for (step, category, _) in &self.observations {
            seen.entry(*category)
                .and_modify(|entry| entry.1 += 1)
                .or_insert((*step, 1));
        }
        seen.into_iter()
            .map(|(category, (step, count))| {
                format!(
                    "[{}] first at step {step}, {count} step(s)",
                    category.name()
                )
            })
            .collect::<Vec<_>>()
            .join("; ")
    }
}

fn rows_for(seed: u64, count: usize) -> Vec<PlannedRow> {
    let mut rng = Rng::new(seed);
    distinct_rows(&mut rng, count)
}

fn mixed_rows(seed: u64, count: usize) -> Vec<PlannedRow> {
    let mut rng = Rng::new(seed ^ 0x9E37_79B9_7F4A_7C15);
    random_rows(&mut rng, count)
}

fn random_rows(rng: &mut Rng, count: usize) -> Vec<PlannedRow> {
    (0..count)
        .map(|_| PlannedRow {
            key: rng.below(KEY_COUNT),
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

fn scenarios() -> Vec<Scenario> {
    vec![
        // L1: the object store fails, then recovers.
        Scenario {
            name: "chaos-upload-faults",
            seed: 0xC4A0_0001,
            ops: vec![
                ChaosOp::Base(Op::Write {
                    rows: rows_for(0xC4A0_0001, 3),
                }),
                ChaosOp::UploadFault { times: 2 },
                ChaosOp::Base(Op::Write {
                    rows: mixed_rows(0xC4A0_0001, 2),
                }),
                ChaosOp::CompactionFault {
                    times: 1,
                    choice: 0,
                },
                ChaosOp::Base(Op::Import { choice: 0 }),
                ChaosOp::Base(Op::TableCompaction { choice: 0 }),
                ChaosOp::AbortSplit {
                    abort: SplitAbort::AfterRemap,
                },
            ],
        },
        Scenario {
            name: "chaos-read-faults",
            seed: 0xC4A0_0002,
            ops: vec![
                ChaosOp::Base(Op::Write {
                    rows: rows_for(0xC4A0_0002, 3),
                }),
                ChaosOp::Base(Op::Flush),
                ChaosOp::Base(Op::Write {
                    rows: mixed_rows(0xC4A0_0002, 2),
                }),
                ChaosOp::Base(Op::Flush),
                ChaosOp::ReadFault { times: 2 },
                ChaosOp::CompactionFault {
                    times: 1,
                    choice: 0,
                },
                ChaosOp::RecoverRegions,
            ],
        },
        Scenario {
            name: "chaos-delete-faults",
            seed: 0xC4A0_0003,
            ops: vec![
                ChaosOp::Base(Op::Write {
                    rows: rows_for(0xC4A0_0003, 2),
                }),
                ChaosOp::DeleteFault { times: 2 },
                ChaosOp::Base(Op::Flush),
                ChaosOp::RecoverEngine,
            ],
        },
        // L2: a compaction stops between reading and publishing.
        Scenario {
            name: "chaos-cancel-compaction-early",
            seed: 0xC4A0_0004,
            ops: vec![
                ChaosOp::Base(Op::Write {
                    rows: rows_for(0xC4A0_0004, 3),
                }),
                ChaosOp::Base(Op::Flush),
                ChaosOp::Base(Op::Write {
                    rows: mixed_rows(0xC4A0_0004, 2),
                }),
                ChaosOp::CancelCompaction {
                    point: CancelPoint::AfterRead,
                },
                ChaosOp::RecoverRegions,
                ChaosOp::Base(Op::Compact { choice: 0 }),
            ],
        },
        Scenario {
            name: "chaos-cancel-compaction-too-late",
            seed: 0xC4A0_0005,
            ops: vec![
                ChaosOp::Base(Op::Write {
                    rows: rows_for(0xC4A0_0005, 3),
                }),
                ChaosOp::Base(Op::Flush),
                ChaosOp::Base(Op::Write {
                    rows: mixed_rows(0xC4A0_0005, 2),
                }),
                ChaosOp::CancelCompaction {
                    point: CancelPoint::InCommit,
                },
                ChaosOp::RecoverEngine,
            ],
        },
        // L2: a split publication stops between the remap and the apply.
        Scenario {
            name: "chaos-abort-split",
            seed: 0xC4A0_0006,
            ops: vec![
                ChaosOp::Base(Op::Write {
                    rows: rows_for(0xC4A0_0006, 4),
                }),
                ChaosOp::Base(Op::Flush),
                ChaosOp::AbortSplit {
                    abort: SplitAbort::AfterRemap,
                },
                ChaosOp::Base(Op::Split { choice: 0 }),
                ChaosOp::AbortSplit {
                    abort: SplitAbort::AfterLeftApply,
                },
                ChaosOp::AbortSplit {
                    abort: SplitAbort::RemapWriteFault { times: 2 },
                },
                ChaosOp::StagingReadFault { times: 2 },
                ChaosOp::Base(Op::Split { choice: 1 }),
                ChaosOp::Base(Op::Merge),
                ChaosOp::RecoverRegions,
            ],
        },
        // L3: two requests touch one region at once.
        Scenario {
            name: "chaos-concurrency",
            seed: 0xC4A0_0007,
            ops: vec![
                ChaosOp::Base(Op::Write {
                    rows: rows_for(0xC4A0_0007, 3),
                }),
                ChaosOp::Base(Op::Flush),
                ChaosOp::Base(Op::Write {
                    rows: mixed_rows(0xC4A0_0007, 2),
                }),
                ChaosOp::ConcurrentCompactions { choice: 0 },
                ChaosOp::CompactionWithScan { choice: 0 },
                ChaosOp::Base(Op::Split { choice: 0 }),
                ChaosOp::CompactionWithSplit { choice: 0 },
                ChaosOp::Base(Op::Merge),
                ChaosOp::RecoverEngine,
            ],
        },
        // L3 + L4: a compaction racing a merge publication, and a slow upload.
        Scenario {
            name: "chaos-publication-race",
            seed: 0xC4A0_0008,
            ops: vec![
                ChaosOp::Base(Op::Write {
                    rows: rows_for(0xC4A0_0008, 4),
                }),
                ChaosOp::Base(Op::Flush),
                ChaosOp::Base(Op::Split { choice: 0 }),
                ChaosOp::Base(Op::Write {
                    rows: mixed_rows(0xC4A0_0008, 2),
                }),
                ChaosOp::CompactionWithMerge,
                ChaosOp::Base(Op::Write {
                    rows: rows_for(0xC4A0_0008, 2),
                }),
                ChaosOp::UploadDelay { ms: 20 },
                ChaosOp::RecoverRegions,
                ChaosOp::Base(Op::Merge),
            ],
        },
    ]
}

/// Replays a scenario on a fresh engine and checks every step.
async fn run_scenario(scenario: &Scenario, ops: &[ChaosOp]) -> Report {
    let prefix = format!("chaos-{}", scenario.name);
    let mut model = StorageModel::new(&prefix).await;
    let mut observations = Vec::new();
    for (step, op) in ops.iter().enumerate() {
        model.step = step;
        let outcome = run_op(&mut model, op).await;
        model.perturbations.push(outcome.perturbation.clone());
        let ctx = format!("step {step}: {} => {}", op.describe(), outcome.description);
        common_telemetry::info!("[{prefix}] seed {} {ctx}", scenario.seed);

        let mut violations = outcome.interim;
        violations.extend(check_all(&mut model, &format!("{ctx} [live]")).await);
        for (category, detail) in &violations {
            if !category.fatal() {
                observations.push((step, *category, detail.clone()));
            }
        }
        if violations.iter().any(|(category, _)| category.fatal()) {
            return Report {
                name: scenario.name,
                seed: scenario.seed,
                steps: step + 1,
                failure: Some(Failure {
                    scenario: scenario.name.to_string(),
                    seed: scenario.seed,
                    step,
                    op: op.describe(),
                    perturbation: outcome.perturbation,
                    categories: violations
                        .iter()
                        .filter(|(category, _)| category.fatal())
                        .map(|(category, _)| category.name())
                        .collect(),
                    details: {
                        let mut details: Vec<String> = violations
                            .iter()
                            .map(|(category, detail)| format!("[{}] {detail}", category.name()))
                            .collect();
                        details.push(format!(
                            "perturbation schedule so far: {:?}",
                            model.perturbations
                        ));
                        details
                    },
                    faults: model.faults.hits(),
                }),
                observations,
            };
        }
    }
    Report {
        name: scenario.name,
        seed: scenario.seed,
        steps: ops.len(),
        failure: None,
        observations,
    }
}

/// The smallest perturbation schedule that still fails: the shortest failing prefix.
async fn shrink(scenario: &Scenario, first: &Failure) -> String {
    let prefix = &scenario.ops[..=first.step];
    let replay = run_scenario(scenario, prefix).await;
    let note = match &replay.failure {
        Some(failure) if failure.step == first.step => format!(
            "replaying {} step(s) reproduces the failure at step {} (categories {:?})",
            prefix.len(),
            failure.step,
            failure.categories
        ),
        Some(failure) => format!(
            "replaying the prefix failed at step {} instead of {}",
            failure.step, first.step
        ),
        None => "replaying the prefix passed, the failure is not reproducible".to_string(),
    };
    format!("{}\n{note}", first.describe())
}

/// Runs one scenario by name, asserting that every invariant held.
async fn run_named(name: &'static str) -> Report {
    common_telemetry::init_default_ut_logging();
    let scenario = scenarios()
        .into_iter()
        .find(|scenario| scenario.name == name)
        .unwrap_or_else(|| panic!("no chaos scenario named {name}"));
    let levers: Vec<&'static str> = scenario
        .ops
        .iter()
        .map(|op| op.lever())
        .collect::<BTreeSet<&'static str>>()
        .into_iter()
        .collect();
    // The verdict of every scenario is data: it is collected here and attached to the
    // assertion that decides the scenario, so a failure shows it and a pass stays silent.
    let mut summary = format!("[{name}] levers: {levers:?}\n");
    let report = run_scenario(&scenario, &scenario.ops).await;
    summary.push_str(&format!("[{name}] {}\n", report.verdict()));
    let observations = report.observations_summary();
    if !observations.is_empty() {
        summary.push_str(&format!(
            "[{name}] non-fatal observations: {observations}\n"
        ));
    }
    if let Some(failure) = &report.failure {
        let shrunk = shrink(&scenario, failure).await;
        panic!("{summary}{shrunk}");
    }
    report
}

#[tokio::test]
async fn chaos_upload_faults() {
    run_named("chaos-upload-faults").await;
}

#[tokio::test]
async fn chaos_read_faults() {
    run_named("chaos-read-faults").await;
}

#[tokio::test]
async fn chaos_delete_faults() {
    run_named("chaos-delete-faults").await;
}

#[tokio::test]
async fn chaos_cancel_compaction_early() {
    run_named("chaos-cancel-compaction-early").await;
}

#[tokio::test]
async fn chaos_cancel_compaction_too_late() {
    run_named("chaos-cancel-compaction-too-late").await;
}

#[tokio::test]
async fn chaos_abort_split() {
    run_named("chaos-abort-split").await;
}

#[tokio::test]
async fn chaos_concurrency() {
    run_named("chaos-concurrency").await;
}

#[tokio::test]
async fn chaos_publication_race() {
    run_named("chaos-publication-race").await;
}
