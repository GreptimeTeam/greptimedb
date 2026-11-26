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

//! The model-based (MBT) **driver** of the repartition (split/merge) regression: it turns a
//! model operation into a real request, waits for publication, scans, compares the model's
//! prediction with the observation, and runs the generated traces (fixed trace, seeds,
//! shrink). Its fixed-trace counterpart is
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
//! The in-tree regression pins one hand-written trace of that sequence. This line pins the
//! *class*: it keeps an independent ledger of every committed write event and checks, after
//! every published step, that a plain region scan of the current live set shows each
//! committed event exactly once.
//!
//! The specification this driver runs against is **scheme B** (`repartition_mbt_model`): a
//! reference carries no per-reference row range, the region's own domain is the only filter a
//! scan applies, and a merge of regions that hold a file someone else created is **refused**
//! instead of narrowing. Where a trace merges such a state, the driver asserts the refusal,
//! compacts the offending side(s) and retries: that is the only merge B allows over a split.
//!
//! # The three layers of this line
//!
//! - [`super::repartition_mbt_model`] is the specification: the transitions, the
//!   prediction of this branch's visibility rule, and nothing else. Its state advances only
//!   through a transition, never from an observation.
//! - [`super::repartition_mbt_oracle`] is the judgement: the ledger, the "exactly once"
//!   multiset, "no row outside its owner's domain", manifest-versus-live-version
//!   consistency, reference bounds, sequence monotonicity, physical liveness and the
//!   violation report formatting. It reads no model state.
//! - this module is the driver: requests, scans, publication waits, trace generation and the
//!   reporting of a prediction that diverges from an observation.
//!
//! # What this layer is not
//!
//! Not the specification and not the judgement: it never decides what a scan *should* show,
//! and it never edits the model to match an observation.
//!
//! # Event identity
//!
//! Every write event carries an immutable `event_id`, stored as a normal projectable
//! `Int64` field column inside the real SST (`k`, `payload`, `event_id`, `ts`). `(PK, ts)`
//! is not an event identity: an append-only region legally holds two events with the same
//! key and timestamp, and the random traces generate exactly that case.
//!
//! # Operations
//!
//! Besides writes, flushes, splits, merges and per-region compaction, the traces also drive
//! the two paths that publish an SST the write path never produced for the target region:
//!
//! - `import`: a byte copy of one of the target's own flushed SSTs is re-registered under a
//!   new `FileId` at [`LEVEL_SHARED`] with `sequence: None`, so admission is the only place
//!   that can turn `None` into a stored admission sequence. Both observations are recorded:
//!   the fresh `None` admission and the repeat admission that keeps the existing sequence.
//! - table-level (multi-region) compaction: `table_compaction_snapshot` on that admitted
//!   shared input, rewritten into region-local outputs, published through
//!   `publish_table_compaction`. This is the region-side half of
//!   `compaction/multi_region.rs`; it is an *additional* op and never replaces the L0
//!   multi-holder case the traces exist for.
//!
//! # Waiting for publication
//!
//! Nothing here sleeps. `put_rows`, `Flush`, `Compact`, `set_region_role_state_gracefully`,
//! `remap_manifests`, `edit_region`, `publish_table_compaction`, `Create` and
//! `ApplyStagingManifest` are all synchronous worker requests: when they return, the change
//! is applied to that region's manifest and version. A split/merge step publishes every
//! target region (create -> enter staging -> apply staging manifest) before the routing
//! switches and the oracle runs.
//!
//! # Model-side assertion (separate from the ledger oracle)
//!
//! After every published step [`StorageModel::check_model_authorization`] asserts that a
//! region's visible rows are a subset of what scheme B lets it show: its own domain plus the
//! keys of its own unflushed rows. In B a reference hands over the whole file, so the domain is
//! the only bound there is; that bound is still an independent statement — the oracle counts
//! the ledger, this one bounds the model, and neither hands a mutable object to the other.
//!
//! It cannot move into the oracle: it is the one statement that needs the specification. The
//! oracle and it stay independent — the oracle keeps its exact meaning (each committed event
//! exactly once, nothing added, nothing misplaced); this check can pass while the oracle fails,
//! because the domain bound is not counting.
//!
//! # Cases the serial generator cannot reach
//!
//! `test_mbt_case_*` pins the states the random traces only reach by luck — a two-hop split,
//! copies across three generations, a repeated merge, an empty split target, two append-mode
//! events sharing `(key, ts)` — and `test_mbt_case_chaos_close_reopen` interleaves a region
//! close/reopen (and a merge retry) into the shared-file shape. A row whose partition key lies
//! outside the region it is written to is **not** one of them any more: scheme B states writes
//! as in-domain only, so the model has no rule for that state and the driver cannot predict it.
//!
//! # Traces
//!
//! Every seed's plan opens with the shape the model exists for, generated deliberately rather
//! than hoped for (`shared_file_shape`): collapse to one live region, write rows over several
//! distinct keys, flush that one file, split it so *both* halves keep the shared file, and
//! region-compact exactly one half. The block starts with a `Merge`, so the split always has
//! exactly two halves and the compaction always lands on one of them. The two widened paths
//! (`import`, multi-region compaction) then run against that two-holder state, and the plan's
//! merge meets scheme B's refusal (the un-compacted half still holds a foreign file), compacts
//! it and retries.
//! [`RefModel::shapes`] records which of the steps a trace actually drove, and every report
//! prints them; random ops after the prologue may reach the same shape on their own.
//!
//! # Seed space
//!
//! `MITO_MBT_SEED=<seed>` replays exactly one seed. `MITO_MBT_SEEDS=<list>` takes a comma
//! list whose items are a seed or an inclusive `start:end` range, decimal or `0x` hex (for
//! example `0x5EED0001:0x5EED0003,42`). Without either variable the three default seeds
//! below run.

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
use store_api::region_engine::{
    RegionEngine, RegionRole, RemapManifestsRequest, SettableRegionRoleState,
};
use store_api::region_request::{
    ApplyStagingManifestRequest, EnterStagingRequest, PathType, RegionCatchupRequest,
    RegionCloseRequest, RegionCompactRequest, RegionCreateRequest, RegionFlushRequest,
    RegionOpenRequest, RegionRequest, StagingPartitionDirective,
};
use store_api::storage::{FileId, RegionId, ScanRequest};

use super::repartition_mbt_model::{
    Domain, ForeignRef, KEY_COUNT, Level, MergeVerdict, RefModel, SHAPE_MERGE_RECOMBINES,
    SHAPE_ONE_SIDE_COMPACTION, SHAPE_SPLIT_SHARED, describe_keys, key_index, key_name,
};
use super::repartition_mbt_oracle::{
    AppliedState, Category, DomainExpr, Event, HolderView, Ledger, ManifestRef, Oracle,
    PhysicalView, PlannedRow, SstReader, SstRef, Violation, decode_domain, domain_of_json,
    format_violations, render_multiset,
};
#[cfg(feature = "enterprise")]
use super::table_compaction::PublicationOutcome;
use crate::config::MitoConfig;
use crate::engine::{MITO_ENGINE_NAME, MitoEngine};
use crate::gc::GcConfig;
use crate::manifest::action::RegionEdit;
use crate::region::{RegionLeaderState, RegionRoleState};
use crate::sst::file::LEVEL_SHARED;
use crate::test_util::{TestEnv, put_rows, rows_schema};

/// Wide enough to cover every timestamp the traces write.
const COMPACTION_WINDOW_SECONDS: i64 = 3600;
/// How often the driver re-reads a promoted region's role before failing loudly.
const WRITABLE_WAIT_ATTEMPTS: usize = 200;
/// Delay between two role reads while a reopened region is being promoted.
const WRITABLE_WAIT_INTERVAL_MS: u64 = 10;
/// Length of one random trace.
const PLAN_LEN: usize = 18;
/// Fixed seeds; each seed is one independently reproducible trace.
const DEFAULT_SEEDS: [u64; 3] = [0x5EED_0001, 0x5EED_0002, 0x5EED_0003];
/// Upper bound on the number of seeds one `start:end` range may expand to.
const MAX_RANGE_SEEDS: u64 = 64;

/// Renders the regions of a merge input list for a refusal message.
fn describe_regions(inputs: &[RegionId]) -> String {
    inputs
        .iter()
        .map(|region_id| region_id.to_string())
        .collect::<Vec<_>>()
        .join(", ")
}

/// Renders an offending reference's creator for the reports. A legacy file that records no
/// creator is a different state from one created by the whole table, so it is not rendered as
/// a domain.
fn describe_creator(creator: Option<Domain>) -> String {
    match creator {
        Some(domain) => format!(
            "creator domain [{}, {})",
            key_name(domain.lo),
            key_name(domain.hi)
        ),
        None => "no recorded creator (legacy)".to_string(),
    }
}

/// Renders the offending references a scheme B refusal has to name.
fn describe_offenders(offenders: &[ForeignRef<RegionId>]) -> String {
    offenders
        .iter()
        .map(|offender| offender.describe())
        .collect::<Vec<_>>()
        .join("; ")
}

/// One region's predicted-vs-observed visibility at one step of a case.
struct RegionStep {
    region: RegionId,
    domain: Domain,
    /// Scheme B's visibility rule evaluated on the specification
    /// ([`RefModel::predict_visible_rows`]).
    predicted: BTreeMap<i64, u64>,
    /// A fresh scan of the same region, as `event id -> copies`.
    observed: BTreeMap<i64, u64>,
    /// The keys scheme B lets this region show: its own domain (a reference hands over the
    /// whole file, so nothing narrower exists to state).
    authorized: BTreeSet<usize>,
    /// The engine's reference set, one line per file.
    references: Vec<String>,
}

impl RegionStep {
    fn agrees(&self) -> bool {
        self.predicted == self.observed
    }
}

/// One step of a case: what the model predicted, what a scan observed, and every judgement
/// that fired. A divergence is recorded here and printed; it is never turned into a silent
/// pass by adjusting the model.
struct CaseStep {
    case: String,
    step: String,
    regions: Vec<RegionStep>,
    /// Published regions that are closed at this step, so unobservable.
    closed: Vec<RegionId>,
    /// Every judgement that fired, rendered with its category.
    violations: Vec<String>,
}

impl CaseStep {
    fn mismatched(&self) -> Vec<&RegionStep> {
        self.regions
            .iter()
            .filter(|region| !region.agrees())
            .collect()
    }

    /// Whether the ledger oracle, the judge of "exactly once", reported something.
    fn oracle_fired(&self) -> bool {
        self.violations
            .iter()
            .any(|violation| violation.starts_with("[visibility]"))
    }

    fn render(&self) -> String {
        let mut lines = vec![format!("=== case `{}` / {} ===", self.case, self.step)];
        for region in &self.regions {
            lines.push(format!(
                "  region {} (domain [{}, {})): predicted {} | observed {} -> {}",
                region.region,
                key_name(region.domain.lo),
                key_name(region.domain.hi),
                render_multiset(&region.predicted),
                render_multiset(&region.observed),
                if region.agrees() {
                    "prediction agrees"
                } else {
                    "PREDICTION DIVERGES from the engine"
                }
            ));
            lines.push(format!(
                "    the specification authorizes keys {}; references: {:?}",
                describe_keys(&region.authorized),
                region.references
            ));
        }
        for region_id in &self.closed {
            lines.push(format!(
                "  region {region_id}: closed, so unobservable (a reopen re-reads its manifest)"
            ));
        }
        if self.violations.is_empty() {
            lines.push("  judgement: no violation".to_string());
        } else {
            lines.push("  judgement:".to_string());
            lines.extend(
                self.violations
                    .iter()
                    .map(|violation| format!("    {violation}")),
            );
        }
        lines.join("\n")
    }
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
    /// Regions that are currently closed: still part of the published routing, but the engine
    /// has no live version for them, so they are neither scannable nor observable until a
    /// reopen reloads their persisted manifest.
    closed: BTreeSet<RegionId>,

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
            closed: BTreeSet::new(),
            store,
            table_dir,
            path_type,
            applied: BTreeMap::new(),
            oracle: Oracle::default(),
            step: 0,
            _env: env,
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

    fn sst_ref(&self, region_id: RegionId, file_id: FileId) -> SstRef {
        *self.holders[&region_id]
            .files
            .get(&file_id)
            .unwrap_or_else(|| panic!("region {region_id} does not reference {file_id:?}"))
    }

    /// Refreshes layer 2 (each holder's live version and persisted manifest) and each
    /// region's applied state, then rebuilds layer 1 from layer 2. Values are copied into
    /// [`SstRef`] snapshots, never shared.
    async fn refresh_views(&mut self) {
        let mut holders = BTreeMap::new();
        let mut applied = BTreeMap::new();
        for region_id in self.tracked.clone() {
            if self.closed.contains(&region_id) {
                // A closed region has no live version: its references exist only in the
                // persisted manifest, which the reopen below re-reads. The last observed view
                // stays as the record of what the close happened on.
                continue;
            }
            let region = self
                .engine
                .get_region(region_id)
                .unwrap_or_else(|| panic!("region {region_id} disappeared"));
            let manifest = region.manifest_ctx.manifest().await;
            let data = region.version_control.current();

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
                    writable: region.state()
                        == RegionRoleState::Leader(RegionLeaderState::Writable),
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

    async fn assert_visible(&mut self, ctx: &str) {
        if let Some(report) = self.check(ctx).await {
            panic!("{report}");
        }
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
    /// After every published step each region may only show rows scheme B lets it show: rows
    /// whose key lies in the region's own domain, plus the rows it wrote and has not flushed
    /// yet (which are in-domain by construction). B hands a reference over whole, so the domain
    /// is the only bound there is — and a region that shows a row outside it still fails here.
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

    /// Asserts the model-side domain statement on its own.
    ///
    /// The oracle is not consulted: this pins that the visible rows of every published region
    /// lie inside the domain scheme B allows that region to show.
    async fn assert_model_authorization(&mut self, ctx: &str) {
        let mut violations = Vec::new();
        self.check_model_authorization(ctx, &mut violations).await;
        if !violations.is_empty() {
            panic!(
                "MODEL AUTHORIZATION VIOLATED\n{}\nthe specification state:\n{}",
                format_violations(&violations),
                self.spec.describe_all()
            );
        }
    }

    /// Asserts every invariant category except the non-fatal `reference-bounds` one.
    async fn assert_invariants(&mut self, ctx: &str) {
        let violations: Vec<Violation> = self
            .check_invariants(ctx)
            .await
            .into_iter()
            .filter(|violation| violation.category.fatal())
            .collect();
        if !violations.is_empty() {
            panic!(
                "STORAGE INVARIANT VIOLATED\n{}",
                format_violations(&violations)
            );
        }
    }

    /// Compares, at one step of a case, the model's *prediction* of this branch's engine rule
    /// with a fresh scan of every open published region, and collects both judgements.
    ///
    /// Nothing is asserted here. A divergence between prediction and observation, and every
    /// judgement that fires, is recorded and printed: a case reports it and continues instead
    /// of bending the model to an observation.
    async fn observe(&mut self, case: &str, step: &str) -> CaseStep {
        self.refresh_views().await;
        let live = self.live_regions();
        let closed: Vec<RegionId> = live
            .iter()
            .copied()
            .filter(|region_id| self.closed.contains(region_id))
            .collect();
        let mut regions = Vec::new();
        for region_id in live
            .into_iter()
            .filter(|region_id| !self.closed.contains(region_id))
        {
            let domain = self.domain_of(region_id);
            let predicted = self.spec.predict_counts(region_id);
            let observed: BTreeMap<i64, u64> = self
                .scan_counts(region_id)
                .await
                .into_iter()
                .map(|(event_id, copies)| (event_id, copies as u64))
                .collect();
            let authorized = self.spec.authorized_keys(region_id);
            let references = self.reference_lines(region_id);
            regions.push(RegionStep {
                region: region_id,
                domain,
                predicted,
                observed,
                authorized,
                references,
            });
        }

        let ctx = format!("{case} / {step}");
        let mut violations = Vec::new();
        if closed.is_empty() {
            if let Some(report) = self.check(&ctx).await {
                violations.push(format!("[visibility] {report}"));
            }
            violations.extend(
                self.check_invariants(&ctx)
                    .await
                    .into_iter()
                    .map(|violation| {
                        format!("[{}] {}", violation.category.name(), violation.detail)
                    }),
            );
        } else {
            violations.push(format!(
                "[visibility] not evaluated while {closed:?} {} closed: the ledger oracle needs \
                 an open region to scan",
                if closed.len() == 1 { "is" } else { "are" }
            ));
        }

        CaseStep {
            case: case.to_string(),
            step: step.to_string(),
            regions,
            closed,
            violations,
        }
    }

    /// The engine's reference set for `region_id`, one line per file: id, creator domain,
    /// level, rows and admission sequence.
    fn reference_lines(&self, region_id: RegionId) -> Vec<String> {
        self.holders
            .get(&region_id)
            .map(|view| {
                view.files
                    .values()
                    .map(|sst| {
                        format!(
                            "{:?}({}, owner={}, L{}, rows={}, sequence={:?})",
                            sst.file_id,
                            describe_creator(sst.creator_domain),
                            sst.owner,
                            sst.level,
                            sst.num_rows,
                            sst.sequence
                        )
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Records one step of a case: keeps the prediction, the observation and the judgement in
    /// `report`, and returns the whole step so the case can assert on it.
    ///
    /// The rendered step is not swallowed: the case attaches `report` to its deciding
    /// assertion, so a divergence shows every step that led to it.
    async fn record_step(&mut self, case: &str, step: &str, report: &mut Vec<String>) -> CaseStep {
        self.step += 1;
        let observed = self.observe(case, step).await;
        let text = observed.render();
        common_telemetry::info!("[mbt-case] {text}");
        report.push(text);
        observed
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
                validate_only: false,
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

    /// Merges every published region into one new whole-domain region.
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
        self.merge_settling(&inputs).await
    }

    /// Merges `inputs` into a fresh whole-domain survivor, first making the merge legal under
    /// scheme B.
    ///
    /// B allows a merge of **two or more distinct sources** only while every input holds nothing
    /// but the files it created itself: a single-source merge combines no file sets, so the model
    /// allows it and no probe runs. Before merging, the model is asked what it expects:
    ///
    /// 1. if it allows the merge, the merge runs and the specification takes the union of the
    ///    inputs' references;
    /// 2. if it expects a refusal, the driver issues the coordinator's remap and asserts the
    ///    engine refuses it, naming every offending `(region, file, creator)` — an engine that
    ///    merges anyway is exactly the divergence these suites exist to find;
    /// 3. it then compacts every offending region, so each participant holds a file it
    ///    materialized itself, and retries the merge (which the model then allows).
    async fn merge_settling(&mut self, inputs: &[RegionId]) -> RegionId {
        // A merge consumes persisted manifests, so the row state has to reach the SSTs first:
        // every row of every participant is then in a file, and the verdict speaks about files.
        self.flush_dirty().await;
        let verdict = self.spec.merge_verdict(inputs);
        if let MergeVerdict::Refused(offenders) = &verdict {
            self.spec.note_refused_merge(&verdict);
            let expected = verdict.describe();
            let named = describe_offenders(offenders);
            let refusal = self.probe_merge_refusal(inputs).await;
            let err = match refusal {
                Ok(merged) => panic!(
                    "SCHEME B DIVERGES FROM THE ENGINE: {expected}, but the engine published \
                     survivor {merged} for inputs [{}]. Scheme B allows a merge only while every \
                     participant holds just the files it created itself, and these hold foreign \
                     ones: {named}",
                    describe_regions(inputs)
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
                    describe_regions(inputs)
                );
            }
            let message = err.to_string();
            // The message has to name every offending region *and* every offending file id.
            // The model's file ids are its own (opaque, never an engine `FileId`), so the ids to
            // look for are read from the engine's persisted manifests, the way the remap reads
            // them, and the model's offender count has to agree with that set.
            let engine_offenders = self.engine_foreign_files(inputs).await;
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
                "[mbt-case] scheme B refused the merge of {inputs:?} ({expected}): {message}; \
                 compacting {:?} and retrying",
                verdict.offending_regions()
            );
            // Step 3: a compaction consumes every reference of the region and leaves one file
            // stamped with the region's own domain, so each participant becomes mergeable.
            for region_id in verdict.offending_regions() {
                self.compact(region_id).await;
            }
            let retry = self.spec.merge_verdict(inputs);
            assert!(
                retry.is_allowed(),
                "compacting {:?} must make the merge of {inputs:?} legal under scheme B, but {}",
                verdict.offending_regions(),
                retry.describe()
            );
        }
        let merged = self.publish_merge(inputs).await;
        self.spec.merge(inputs, merged);
        merged
    }

    /// Issues the coordinator's remap for `inputs` into a fresh staging survivor and reports
    /// whether the engine refused it, with the engine's own error so the caller can assert
    /// *which* failure it is (`RegionEngine::remap_manifests` boxes the mito2 error, so the
    /// caller downcasts it back to [`crate::error::Error`]).
    ///
    /// Only the remap runs: it is the step that reads every input's persisted manifest, so it is
    /// where an engine implementing scheme B would see the foreign file and refuse. Nothing is
    /// published and the inputs are not retired, so a refusal leaves the fixtures exactly as
    /// they were and the following compaction can still run. When the engine accepts the remap
    /// the state is already inconsistent with B and the caller stops with the finding.
    async fn probe_merge_refusal(
        &mut self,
        inputs: &[RegionId],
    ) -> Result<RegionId, common_error::ext::BoxedError> {
        let probe = self.alloc_region_id();
        self.create_and_enter_staging(probe, Domain::FULL).await;
        let remap = self
            .engine
            .remap_manifests(RemapManifestsRequest {
                validate_only: false,
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
                // The probe region never became part of the routing; close it so it does not
                // keep a staged identity alive behind the fixture's back.
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

    /// Runs the merge machinery for `inputs` into a fresh whole-domain survivor, publishes
    /// it as the routing and returns it. The caller states the specification transition,
    /// because a first merge and a retry derive it differently.
    ///
    /// Publication: every input stops serving (a synchronous role-state request, exactly
    /// like the split source), then the merged region is created, consumes the coordinator's
    /// staging manifest (a synchronous request) and only then becomes the sole published
    /// region.
    async fn publish_merge(&mut self, inputs: &[RegionId]) -> RegionId {
        self.flush_dirty().await;
        // A retired region must not keep writing for a domain another region now serves.
        for input in inputs {
            self.retire_region(*input).await;
        }

        let merged = self.alloc_region_id();
        // The merged region is the coordinator's staging owner, so it has to exist and be
        // in staging state before the remap runs (the source does that job for a split).
        self.create_and_enter_staging(merged, Domain::FULL).await;
        let response = self
            .engine
            .remap_manifests(RemapManifestsRequest {
                validate_only: false,
                region_id: merged,
                input_regions: inputs.to_vec(),
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

    /// Retries the merge of `inputs` after an interruption (a close/reopen of the survivor),
    /// re-running the very same remap into a fresh survivor, exactly as the coordinator does
    /// when the first attempt did not complete.
    ///
    /// The retry reports whether it republished the reference set the first attempt
    /// published: the observation compares the survivor's references before the close with
    /// the retried survivor's, which must be the same file ids.
    async fn retry_merge(&mut self, previous_survivor: RegionId, inputs: &[RegionId]) -> RegionId {
        // A retry re-runs the very same remap over the very same inputs, which the first
        // attempt already settled: scheme B must allow it without another compaction.
        let verdict = self.spec.merge_verdict(inputs);
        assert!(
            verdict.is_allowed(),
            "the retried merge of {inputs:?} must be legal under scheme B (the first attempt \
             settled those inputs), but {}",
            verdict.describe()
        );
        // The previous attempt's survivor is superseded: it stops accepting writes so no
        // region serves the whole domain twice.
        self.retire_region(previous_survivor).await;
        let retried = self.publish_merge(inputs).await;
        self.spec.retry_merge(previous_survivor, retried);
        retried
    }

    /// Stops `region_id` from serving its domain before a repartition moves that domain to
    /// another region.
    ///
    /// A writable leader enters staging, exactly like a split source. A region that a
    /// close/reopen brought back is a **follower** (`region/opener.rs` opens every region read
    /// only), and a follower cannot write in the first place, so there is nothing to demote:
    /// the engine refuses the staging transition from a follower, and the harness reports that
    /// state instead of pretending the region is still a leader.
    async fn retire_region(&mut self, region_id: RegionId) -> String {
        let role = self.engine.role(region_id);
        if role == Some(RegionRole::Follower) {
            return format!("region {region_id} is a follower: nothing to demote");
        }
        self.engine
            .set_region_role_state_gracefully(region_id, SettableRegionRoleState::StagingLeader)
            .await
            .unwrap();
        format!("region {region_id} entered staging (role before: {role:?})")
    }

    /// Closes `region_id` and records the references the close happened on.
    ///
    /// The region is flushed first, so its rows are already in SSTs and the close does not
    /// need an implicit flush that would change the file inventory behind the record.
    async fn close_region(&mut self, region_id: RegionId) -> String {
        self.flush_region(region_id).await;
        self.refresh_views().await;
        let before = self.describe_references(region_id);
        self.engine
            .handle_request(
                region_id,
                RegionRequest::Close(RegionCloseRequest {
                    flush_on_close: true,
                }),
            )
            .await
            .unwrap();
        assert!(
            !self.engine.is_region_exists(region_id),
            "the close must remove {region_id} from the engine"
        );
        self.closed.insert(region_id);
        let line =
            format!("close region {region_id} (flush_on_close); references before close: {before}");
        // Returned, not only recorded: the file inventory a close leaves behind is the evidence
        // of what the reopen has to reload, so the case keeps it in its report.
        common_telemetry::info!("[mbt-case] {line}");
        line
    }

    /// Reopens a closed region and re-reads the references its persisted manifest holds.
    ///
    /// `skip_wal_replay` is safe here only because [`StorageModel::close_region`] flushed the
    /// region: every row of it is in an SST, so the reopened region describes the same
    /// references without a WAL.
    async fn reopen_region(&mut self, region_id: RegionId) -> String {
        let before = self.describe_references(region_id);
        self.engine
            .handle_request(
                region_id,
                RegionRequest::Open(RegionOpenRequest {
                    engine: MITO_ENGINE_NAME.to_string(),
                    table_dir: self.request.table_dir.clone(),
                    path_type: self.path_type,
                    options: self.request.options.clone(),
                    skip_wal_replay: true,
                    checkpoint: None,
                    requirements: Default::default(),
                }),
            )
            .await
            .unwrap();
        self.closed.remove(&region_id);
        self.refresh_views().await;
        let line = format!(
            "reopen region {region_id} (role after reopen: {:?}): references before reopen: \
             {before}; after reopen: {}",
            self.engine.role(region_id),
            self.describe_references(region_id)
        );
        // Returned for the same reason: the role the engine assigns on open is what bounds what
        // a chaos case may do next, so it is reported instead of assumed.
        common_telemetry::info!("[mbt-case] {line}");
        line
    }

    /// The reference state the engine exposes for `region_id`: every file id with its creator
    /// domain, level, row count and admission sequence.
    ///
    /// Scheme B's reference is exactly this tuple `(file id, creator domain, level)`: there is
    /// no per-reference row range to observe or to report, because the holding region's domain
    /// is the only filter B states.
    fn describe_references(&self, region_id: RegionId) -> String {
        let Some(view) = self.holders.get(&region_id) else {
            return format!("region {region_id} (no observed references)");
        };
        if view.files.is_empty() {
            return format!("region {region_id} (no references)");
        }
        let files: Vec<String> = view
            .files
            .values()
            .map(|sst| {
                format!(
                    "{:?}({}, creator {}, L{}, rows={}, sequence={:?})",
                    sst.file_id,
                    describe_creator(sst.creator_domain),
                    sst.owner,
                    sst.level,
                    sst.num_rows,
                    sst.sequence
                )
            })
            .collect();
        format!(
            "region {region_id} (domain [{}, {})): {files:?}",
            key_name(self.domain_of(region_id).lo),
            key_name(self.domain_of(region_id).hi)
        )
    }

    /// Returns whether `region_id` accepts writes (and so a compaction).
    fn is_writable(&self, region_id: RegionId) -> bool {
        matches!(
            self.engine.role(region_id),
            Some(RegionRole::Leader | RegionRole::StagingLeader)
        )
    }

    /// Makes `region_id` writable before a compaction, the way the datanode makes a region it
    /// reopened writable.
    ///
    /// A region opened from storage is a read-only follower (`region/opener.rs`), and the engine
    /// refuses to compact it. The datanode's upgrade step is a catchup that replays the WAL tail
    /// and promotes the region (`RegionCatchupRequest::set_writable`); the promotion happens in
    /// the worker, so the role is waited for with a bound instead of being assumed. A region that
    /// stays read-only fails loudly here rather than turning into a compaction error.
    async fn ensure_writable(&mut self, region_id: RegionId) {
        if !self.is_writable(region_id)
            && let Err(err) = self
                .engine
                .handle_request(
                    region_id,
                    RegionRequest::Catchup(RegionCatchupRequest {
                        set_writable: true,
                        entry_id: None,
                        metadata_entry_id: None,
                        checkpoint: None,
                    }),
                )
                .await
        {
            // The wait below is the authority: it reports the role a region is stuck in.
            common_telemetry::warn!(
                "[mbt-case] catchup of {region_id} before its compaction returned {err:?}"
            );
        }
        for _ in 0..WRITABLE_WAIT_ATTEMPTS {
            if self.is_writable(region_id) {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(WRITABLE_WAIT_INTERVAL_MS)).await;
        }
        panic!(
            "region {region_id} did not become writable within {} attempts of {}ms (role {:?}); \
             the engine refuses to compact a region that is not a writable leader",
            WRITABLE_WAIT_ATTEMPTS,
            WRITABLE_WAIT_INTERVAL_MS,
            self.engine.role(region_id)
        );
    }

    /// Region compaction on one published region.
    ///
    /// Publication: `Compact` is a synchronous worker request, and it flushes the target
    /// first so the compaction has SSTs to rewrite.
    async fn compact(&mut self, region_id: RegionId) {
        self.ensure_writable(region_id).await;
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

    /// Publishes a **legacy** SST reference: a byte copy of one of `target`'s own flushed SSTs,
    /// re-registered under a brand-new `FileId` with **no recorded partition expression**, while
    /// the target's own L0 reference leaves in the same edit.
    ///
    /// That is the state a file written before the creating expression was persisted is in, which
    /// #10646 admits: the provenance cannot be established, so the remap treats the file as the
    /// region's own, while the read path still narrows it by the region's expression. The bytes
    /// and the admission sequence are the ones the flush produced, so the region keeps exactly
    /// the rows it had and only one reference's identity and stamp change.
    ///
    /// Publication: `MitoEngine::edit_region` is a synchronous worker request; when it returns,
    /// the reference is in the manifest and the version.
    async fn publish_creatorless_file(&mut self, target: RegionId) -> String {
        self.flush_region(target).await;
        self.refresh_views().await;
        let original_id = self.holders.get(&target).and_then(|view| {
            view.files
                .iter()
                .find(|(_, sst)| sst.level == 0 && sst.owner == target)
                .map(|(file_id, _)| *file_id)
        });
        let Some(original_id) = original_id else {
            return format!(
                "legacy file into {target} (no self-flushed L0 SST to re-register, nothing \
                 published)"
            );
        };

        let original = self.file_meta(target, original_id);
        let legacy_id = FileId::random();
        let source_path = self.sst_path(original.region_id, original.file_id);
        let target_path = self.sst_path(target, legacy_id);
        if let Err(err) = self.store.copy(&source_path, &target_path).await {
            return format!(
                "legacy file into {target} (copying {source_path} -> {target_path} failed: {err})"
            );
        }

        let mut published = original.clone();
        published.file_id = legacy_id;
        published.region_id = target;
        // The creator is simply not recorded on this object: that is the whole point.
        published.partition_expr = None;
        let edit = self
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
        if let Err(err) = edit {
            return format!(
                "legacy file {legacy_id:?} into {target}: rejected at the edit ({err:?})"
            );
        }
        // The replacement mints a new physical id whose stamp is unrecorded.
        let model_file = self.spec.forget_creator(target);
        self.refresh_views().await;
        let recorded = self
            .holders
            .get(&target)
            .and_then(|view| view.files.get(&legacy_id))
            .map(|sst| sst.creator_domain);
        format!(
            "legacy file into {target}: {original_id:?} re-registered as {legacy_id:?} with no \
             recorded creator (observed {recorded:?}, model ref {model_file:?})"
        )
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

/// One failing step of a trace, with every category that fired there.
struct Failure {
    seed: u64,
    step: usize,
    op: String,
    /// `(category, diff)` pairs, all categories violated at that step.
    violations: Vec<(Category, String)>,
    /// The shapes the model had already driven the engine through at that step.
    shapes: Vec<&'static str>,
}

impl Failure {
    fn describe(&self) -> String {
        let categories: Vec<&'static str> = self
            .violations
            .iter()
            .map(|(category, _)| category.name())
            .collect();
        let details: Vec<String> = self
            .violations
            .iter()
            .map(|(category, detail)| format!("  [{}] {detail}", category.name()))
            .collect();
        format!(
            "=== model-based trace failure ===\n\
             seed: {seed} ({seed:#018x})\n\
             first violating step: {step}\n\
             operation: {op}\n\
             invariant categories: {categories:?}\n\
             shapes driven so far: {shapes:?}\n\
             {details}",
            seed = self.seed,
            step = self.step,
            op = self.op,
            categories = categories,
            shapes = self.shapes,
            details = details.join("\n"),
        )
    }
}

/// Everything one trace replay observed.
struct TraceReport {
    /// The first step that violated a fatal category, if any.
    failure: Option<Failure>,
    /// Non-fatal observations (`reference-bounds`), which every legal split produces here.
    observations: Vec<(usize, Category, String)>,
    /// Which steps of the shared-file shape the model drove the engine through.
    shapes: Vec<&'static str>,
}

impl TraceReport {
    fn observations_summary(&self) -> String {
        let mut seen: BTreeMap<Category, (usize, usize, String)> = BTreeMap::new();
        for (step, category, detail) in &self.observations {
            seen.entry(*category)
                .and_modify(|entry| entry.1 += 1)
                .or_insert((*step, 1, detail.clone()));
        }
        seen.into_iter()
            .map(|(category, (step, count, detail))| {
                format!(
                    "[{}] first at step {step}, {count} step(s): {detail}",
                    category.name()
                )
            })
            .collect::<Vec<_>>()
            .join("\n  ")
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
/// The extras run after the one-side compaction, so the holders of the shared file are
/// already asymmetric; they never merge (a merge would collapse the halves before the plan's
/// own merge) and never compact (a second compaction could mask the class by rewriting the
/// remaining holder too). The op after the prologue is free again.
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
    match rng.below(7) {
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
        4 => Op::Compact {
            choice: rng.below(4),
        },
        5 => Op::Import {
            choice: rng.below(4),
        },
        _ => Op::TableCompaction {
            choice: rng.below(4),
        },
    }
}

/// The opening block of every plan: the shared-file shape, generated deliberately instead of
/// hoped for.
///
/// One live region flushes one file, a split hands that file to *both* halves, and region
/// compaction lands on exactly one half. The block starts with a `Merge` (a no-op on a
/// single region), so the split that follows always has exactly two halves and the
/// compaction always hits one of them; the plan's own merge closes the shape by merging both
/// halves.
///
/// Every step is a published step, so both the ledger oracle and the model-side domain
/// assertion run on the flush state, on the split state (one file, two holders, one shared
/// reference inherited unchanged) and on the one-side-compacted state. The plan's merge then
/// meets scheme B's refusal — the other half still holds the source's file — compacts it and
/// retries.
fn shared_file_shape(rng: &mut Rng) -> Vec<Op> {
    let count = 2 + rng.below(2);
    vec![
        Op::Merge,
        Op::Write {
            rows: distinct_rows(rng, count),
        },
        Op::Flush,
        Op::Split {
            choice: rng.below(4),
        },
        Op::Compact {
            choice: rng.below(2),
        },
    ]
}

/// Builds a trace for `seed`.
///
/// The plan opens with [`shared_file_shape`], so the trace reaches the bug class instead of
/// hoping random ops line up: one flush, a split that makes two holders share the file, a
/// single-holder region compaction on exactly one half, then the merge of both. The import
/// and the table-level compaction follow the single-holder compaction deterministically, so
/// every seed exercises the widened instrument against a two-holder state, while the ops in
/// between and after stay random: distinct seeds reach the class, and the two paths, with
/// different states.
fn generate_plan(seed: u64, len: usize) -> Vec<Op> {
    let mut rng = Rng::new(seed);
    let mut ops = shared_file_shape(&mut rng);
    ops.push(Op::Import {
        choice: rng.below(4),
    });
    ops.push(Op::TableCompaction {
        choice: rng.below(4),
    });
    let extra_after = rng.below(2);
    for _ in 0..extra_after {
        ops.push(prologue_op(&mut rng));
    }
    // The shape's last act: both halves — one holding the shared file, one holding the copy
    // its own compaction materialized — merge into one whole-domain region.
    ops.push(Op::Merge);
    while ops.len() < len {
        ops.push(random_op(&mut rng));
    }
    ops
}

/// Replays `ops` on a fresh engine.
///
/// Every step is checked against the oracle and against all invariant categories. The first
/// step that violates a fatal category is recorded as the trace's failure; non-fatal
/// observations are collected along the whole trace, so a category that fires on every legal
/// split never hides the categories that fire later.
async fn run_trace(prefix: &str, seed: u64, ops: &[Op]) -> TraceReport {
    let mut model = StorageModel::new(prefix).await;
    let mut observations = Vec::new();
    for (step, op) in ops.iter().enumerate() {
        model.step = step;
        let outcome = model.apply(op).await;
        let ctx = format!("step {step}: {} => {outcome}", op.describe());
        common_telemetry::info!("[{prefix}] seed {seed} {ctx}");

        let mut violations: Vec<(Category, String)> = Vec::new();
        if let Some(report) = model.check(&ctx).await {
            violations.push((Category::Visibility, report));
        }
        violations.extend(
            model
                .check_invariants(&ctx)
                .await
                .into_iter()
                .map(|violation| (violation.category, violation.detail)),
        );

        for (category, detail) in &violations {
            if !category.fatal() {
                observations.push((step, *category, detail.clone()));
            }
        }
        if violations.iter().any(|(category, _)| category.fatal()) {
            return TraceReport {
                failure: Some(Failure {
                    seed,
                    step,
                    op: op.describe(),
                    violations,
                    shapes: model.spec.shapes().iter().copied().collect(),
                }),
                observations,
                shapes: model.spec.shapes().iter().copied().collect(),
            };
        }
    }
    TraceReport {
        failure: None,
        observations,
        shapes: model.spec.shapes().iter().copied().collect(),
    }
}

/// Shortest failing prefix: replay only `ops[..=first.step]` and confirm it reproduces.
async fn shrink(seed: u64, ops: &[Op], first: Failure) -> String {
    let prefix = &ops[..=first.step];
    let replay = run_trace(&format!("mbt-shrink-{seed}"), seed, prefix).await;
    let note = match replay.failure {
        Some(failure) if failure.step == first.step => format!(
            "shortest failing prefix: ops[0..={}] ({} of {} ops) reproduces the same \
             violation at step {} in categories {:?}",
            first.step,
            prefix.len(),
            ops.len(),
            first.step,
            failure
                .violations
                .iter()
                .map(|(category, _)| category.name())
                .collect::<Vec<_>>()
        ),
        Some(failure) => format!(
            "replaying the prefix failed at step {} instead of {}",
            failure.step, first.step
        ),
        None => "replaying the prefix passed, the failure is not reproducible".to_string(),
    };
    format!("{}\n{note}", first.describe())
}

/// The seed space to replay, from the environment.
///
/// `MITO_MBT_SEED` replays exactly one seed and wins over `MITO_MBT_SEEDS`. `MITO_MBT_SEEDS`
/// takes a comma list whose items are a seed or an inclusive `start:end` range, decimal or
/// `0x` hex. Without either variable the default seeds run.
fn seeds_from_env() -> Vec<u64> {
    if let Ok(single) = std::env::var("MITO_MBT_SEED") {
        let seed =
            parse_seed(&single).unwrap_or_else(|| panic!("MITO_MBT_SEED={single:?} is not a seed"));
        return vec![seed];
    }
    match std::env::var("MITO_MBT_SEEDS") {
        Ok(spec) => parse_seed_spec(&spec),
        Err(_) => DEFAULT_SEEDS.to_vec(),
    }
}

fn parse_seed(text: &str) -> Option<u64> {
    let text = text.trim();
    match text.strip_prefix("0x").or_else(|| text.strip_prefix("0X")) {
        Some(hex) => u64::from_str_radix(hex, 16).ok(),
        None => text.parse().ok(),
    }
}

fn parse_seed_spec(spec: &str) -> Vec<u64> {
    let mut seeds = Vec::new();
    for item in spec.split(',') {
        let item = item.trim();
        if item.is_empty() {
            continue;
        }
        match item.split_once(':') {
            Some((start, end)) => {
                let start = parse_seed(start)
                    .unwrap_or_else(|| panic!("MITO_MBT_SEEDS: bad range start {start:?}"));
                let end = parse_seed(end)
                    .unwrap_or_else(|| panic!("MITO_MBT_SEEDS: bad range end {end:?}"));
                assert!(start <= end, "MITO_MBT_SEEDS: empty range {start}:{end}");
                assert!(
                    end - start < MAX_RANGE_SEEDS,
                    "MITO_MBT_SEEDS: range {start}:{end} is wider than {MAX_RANGE_SEEDS} seeds"
                );
                seeds.extend(start..=end);
            }
            None => seeds.push(
                parse_seed(item).unwrap_or_else(|| panic!("MITO_MBT_SEEDS: bad seed {item:?}")),
            ),
        }
    }
    assert!(!seeds.is_empty(), "MITO_MBT_SEEDS={spec:?} selects no seed");
    seeds
}

/// Milestone 1: the fixed trace, expressed through the model.
///
/// Two events `A = (k01)` and `B = (k09)` are flushed into one SST `F`; `F` is split into
/// `P: k < k05` and `R: k05 <= k < k10` (both reference `F`); only `R` is compacted, which
/// materializes `F` under a new local file id while `P` still holds `F`; then both halves are
/// merged into one survivor whose domain is the whole table.
///
/// The specification names the same trace in its own vocabulary, which is **scheme B**'s:
/// after the flush `f0` is stamped with the source's whole domain and registered in its L0;
/// after the split both halves hold the very same reference `f0`, unchanged, and each sees only
/// the rows of its own domain; after compacting `R`, `R` holds a new file `f1` stamped with
/// `R`'s domain (its rows outside that domain are masked) while `P` still holds `f0`; the merge
/// is then **refused**, because `P`'s only reference `f0` was created by the whole-domain
/// source, so `P` compacts as well and the retried merge publishes a survivor holding `f1` and
/// `f2`, one row each.
///
/// That is the shape scheme A has to mask per reference and scheme B cannot even merge: an
/// engine that publishes the survivor without the refusal diverges from B. The ledger
/// expectation is exactly `{A: 1, B: 1}` and it never relaxes.
#[tokio::test]
async fn test_mbt_split_shared_sst_single_side_compaction_refused_merge() {
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
    // Every category holds on a single-holder, single-domain step, so this pins that the
    // widened instrument is not vacuous and does not move the failure point below.
    model.assert_invariants("after flush").await;
    let mut observed: Vec<Violation> = Vec::new();

    let source_files = model.holder_files(source);
    assert_eq!(
        1,
        source_files.len(),
        "the flush must produce exactly one SST, got {source_files:?}"
    );
    let shared = *source_files.iter().next().unwrap();
    // The specification: one model file stamped with the source's whole domain and registered
    // in its L0; the region's domain is the only filter, and it is the whole table.
    let spec_shared = model.spec.region(source).expect("the source is published");
    assert_eq!(Domain::FULL, spec_shared.domain);
    let spec_ids = model.spec.ref_ids(source);
    let spec_file = *spec_ids
        .iter()
        .next()
        .expect("the flush minted one model file");
    assert_eq!(
        BTreeMap::from([(spec_file, Level::L0)]),
        spec_shared.refs,
        "the flush must mint one L0 model file stamped with the region's domain: {:?}",
        model.spec.describe(source)
    );
    assert_eq!(
        BTreeSet::from([source]),
        model.spec.holders_of(spec_file),
        "one model file, one holder"
    );

    // ---- Step 1: split into P: k < k05 and R: k05 <= k < k10. ----
    let (left, right) = model.split(source, 5).await;
    model.step = 1;
    model.assert_visible("after split").await;
    // Collected, not asserted: `reference-bounds` reports on every legal split here.
    observed.extend(model.check_invariants("after split").await);
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
    model.assert_model_authorization("after split").await;
    // The specification: both halves keep the *same* model reference, unchanged — scheme B
    // narrows nothing, so the domain is what makes each half see its own rows.
    let spec_left = model.spec.region(left).expect("the left half is published");
    let spec_right = model
        .spec
        .region(right)
        .expect("the right half is published");
    assert_eq!(
        BTreeMap::from([(spec_file, Level::L0)]),
        spec_left.refs,
        "the left half must inherit the reference unchanged"
    );
    assert_eq!(
        BTreeMap::from([(spec_file, Level::L0)]),
        spec_right.refs,
        "the right half must inherit the same reference unchanged"
    );
    assert_eq!(
        BTreeSet::from([left, right]),
        model.spec.holders_of(spec_file),
        "a split hands the same physical file to both halves"
    );
    assert!(
        model.spec.shapes().contains(SHAPE_SPLIT_SHARED),
        "the model must record the split-shared-file shape: {:?}",
        model.spec.shapes()
    );

    // ---- Step 2: region compaction on the right half only. ----
    model.compact(right).await;
    model.step = 2;
    model
        .assert_visible("after compacting only the right half")
        .await;
    observed.extend(
        model
            .check_invariants("after compacting only the right half")
            .await,
    );

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
    // The split retired the source, so the only holder left of the original object is the half
    // the compaction did not touch: the compacted half replaced its reference, and nothing else
    // may still be reading the shared object.
    let holders_of_shared = model.physical.holders_of[&shared].clone();
    assert_eq!(
        BTreeSet::from([left]),
        holders_of_shared,
        "the shared object must be referenced by exactly the half that did not compact it: \
         {holders_of_shared:?}"
    );
    model
        .assert_model_authorization("after compacting only the right half")
        .await;
    // The specification: the compacted half now holds a *new* model file it stamped itself,
    // and the consumed reference left that half only.
    let spec_left = model.spec.region(left).expect("the left half is published");
    let spec_right = model
        .spec
        .region(right)
        .expect("the right half is published");
    assert_eq!(
        BTreeMap::from([(spec_file, Level::L0)]),
        spec_left.refs,
        "the un-compacted half must keep the original reference, unchanged"
    );
    let spec_copy = spec_right
        .refs
        .keys()
        .find(|file| **file != spec_file)
        .copied()
        .unwrap_or_else(|| {
            panic!(
                "the compacted half must hold a new model file: {}",
                model.spec.describe(right)
            )
        });
    assert_eq!(
        BTreeMap::from([(spec_copy, Level::Compacted)]),
        spec_right.refs,
        "a compaction output is the only reference the region keeps, stamped by the region"
    );
    assert_eq!(
        BTreeSet::from([right]),
        model.spec.holders_of(spec_copy),
        "the compaction output has one holder"
    );
    assert_eq!(
        BTreeSet::from([left]),
        model.spec.holders_of(spec_file),
        "the consumed reference left the compacted half only"
    );
    assert!(
        model.spec.shapes().contains(SHAPE_ONE_SIDE_COMPACTION),
        "the model must record the one-side compaction shape: {:?}",
        model.spec.shapes()
    );
    // Scheme B: the left half still holds a file the whole-domain source created, so the
    // merge of the two halves must be refused until that half compacts too.
    let verdict = model.spec.merge_verdict(&[left, right]);
    assert!(
        !verdict.is_allowed(),
        "the un-compacted half holds a foreign file, so scheme B must refuse the merge: {}",
        verdict.describe()
    );
    assert_eq!(
        BTreeSet::from([left]),
        verdict.offending_regions(),
        "the offending side is the one that still holds the shared file"
    );

    // ---- Step 3: merge both halves back into one whole-domain survivor. ----
    // `merge_settling` drives the refusal, compacts the offending half and retries.
    let merged = model.merge_all().await;
    model.step = 3;
    let ctx = format!("after merging into survivor {merged}");
    let violation = model.check(&ctx).await;
    observed.extend(model.check_invariants(&ctx).await);
    common_telemetry::info!(
        "[mbt-dup-row-mvp] invariant observations over this fixed trace:\n{}",
        format_violations(&observed)
    );
    model.assert_model_authorization(&ctx).await;
    // The specification: the survivor holds the two files the halves materialized themselves,
    // each stamped with its half's domain. Neither file carries the other half's row, so the
    // survivor reads each event once — scheme B's merge never duplicates the shared row
    // because it never consumes a shared reference.
    let spec_merged = model
        .spec
        .region(merged)
        .expect("the survivor is published");
    let merged_ids = model.spec.ref_ids(merged);
    assert_eq!(
        2,
        merged_ids.len(),
        "one file per half: {:?}",
        model.spec.describe(merged)
    );
    assert!(
        merged_ids.contains(&spec_copy),
        "the survivor keeps the file the right half materialized: {}",
        model.spec.describe(merged)
    );
    let left_copy = *merged_ids
        .iter()
        .find(|file| **file != spec_copy)
        .expect("the left half's own file");
    assert_eq!(
        BTreeMap::from([(spec_copy, Level::Compacted), (left_copy, Level::Compacted),]),
        spec_merged.refs,
        "the merge must union the halves' own files: {:?}",
        model.spec.describe(merged)
    );
    let merged_refs = model.spec.describe(merged);
    assert!(
        merged_refs.contains("creator [k00, k05)") && merged_refs.contains("creator [k05, k10)"),
        "each survivor reference is stamped with the half that materialized it: {merged_refs}"
    );
    assert_eq!(
        (0..KEY_COUNT).collect::<BTreeSet<_>>(),
        model.spec.authorized_keys(merged),
        "the survivor serves the whole domain, which is the only bound scheme B states"
    );
    assert!(
        model.spec.shapes().contains(SHAPE_MERGE_RECOMBINES),
        "the model must record the recombining merge: {:?}",
        model.spec.shapes()
    );
    // The prediction of scheme B's rule next to the observation: the survivor reads each half's
    // own file through its whole domain, so each event is read exactly once.
    // Printed, never asserted here: the ledger oracle below is the judge of "exactly once".
    common_telemetry::info!(
        "[mbt-dup-row-mvp] predicted visibility of the survivor: {:?}",
        model.spec.predict_counts(merged)
    );
    assert!(
        violation.is_none(),
        "every committed event must stay visible exactly once after the merge, but the survivor \
         holds both halves' materialized files.\n{}\ninvariant observations over this fixed \
         trace:\n{}",
        violation.unwrap_or_default(),
        format_violations(&observed)
    );
}

/// Case 1: two hops of splits over a file a half materialized itself.
///
/// `A -> B1/B2` hands the whole-domain file to both halves; compacting `B2` materializes a file
/// *stamped with* `B2`'s domain. Scheme B does not let `B1` merge while it still holds the
/// source's file, so the merge meets the refusal, compacts `B1` and retries; the survivor then
/// holds both halves' own files. Splitting it at the same cut hands `C1` and `C2` the very same
/// two references, and each half again sees only the rows of its own domain — scheme B has no
/// per-reference range, so the domain is what makes `C2` show its two rows once.
#[tokio::test]
async fn test_mbt_case_two_hop_split() {
    common_telemetry::init_default_ut_logging();
    const CASE: &str = "two-hop split";
    let mut model = StorageModel::new("mbt-case-two-hop-split").await;
    let mut report = Vec::new();
    let mut agreed = true;

    let source = model.live_regions()[0];
    let written = model
        .write(&[
            PlannedRow {
                key: 1,
                ts: 0,
                payload: 1.0,
            },
            PlannedRow {
                key: 3,
                ts: 1000,
                payload: 3.0,
            },
            PlannedRow {
                key: 7,
                ts: 2000,
                payload: 7.0,
            },
            PlannedRow {
                key: 9,
                ts: 3000,
                payload: 9.0,
            },
        ])
        .await;
    let ids: Vec<i64> = written.iter().map(|event| event.event_id).collect();
    model.flush_dirty().await;
    let step = model
        .record_step(CASE, "after flushing the whole-domain file", &mut report)
        .await;
    agreed &= step.mismatched().is_empty();

    // First hop: `A -> B1/B2`.
    let (b1, b2) = model.split(source, 5).await;
    let step = model
        .record_step(CASE, "after the first split (A -> B1/B2)", &mut report)
        .await;
    agreed &= step.mismatched().is_empty();
    assert_eq!(
        BTreeMap::from([(ids[0], 1), (ids[1], 1)]),
        model.spec.predict_counts(b1),
        "the split inherits the file unchanged, so each half sees only the rows of its own \
         domain"
    );
    assert_eq!(
        BTreeMap::from([(ids[2], 1), (ids[3], 1)]),
        model.spec.predict_counts(b2)
    );

    model.compact(b2).await;
    let step = model
        .record_step(CASE, "after compacting the right half", &mut report)
        .await;
    agreed &= step.mismatched().is_empty();

    let merged = model.merge_all().await;
    let step = model
        .record_step(CASE, "after merging both halves", &mut report)
        .await;
    agreed &= step.mismatched().is_empty();

    // Second hop: split the survivor at the same cut. Both targets inherit both halves' own
    // files, unchanged.
    let (c1, c2) = model.split(merged, 5).await;
    let step = model
        .record_step(CASE, "after the second split (B -> C1/C2)", &mut report)
        .await;
    agreed &= step.mismatched().is_empty();
    assert_eq!(
        BTreeMap::from([(ids[0], 1), (ids[1], 1)]),
        model.spec.predict_counts(c1)
    );
    assert_eq!(
        BTreeMap::from([(ids[2], 1), (ids[3], 1)]),
        model.spec.predict_counts(c2),
        "both targets hold the same two references, and only the domain decides: C2 sees the \
         rows of [k05, k10), C1 the rows of [k00, k05)"
    );
    assert!(
        !step.oracle_fired(),
        "no row was lost or duplicated by the second split: each half reads its own rows once"
    );
    assert!(
        agreed,
        "the prediction must agree with the engine in every step of this case:\n\n{}",
        report.join("\n\n")
    );
}

/// Case 2: copies across three generations, `F0 -> F1 -> F2`.
///
/// The whole-domain flush publishes `F0`; compacting one half materializes `F1` (stamped with
/// that half's domain) holding exactly the rows that half saw; the merge of both halves is
/// refused while the other half still holds `F0`, so it compacts into `F1'` and the retry
/// publishes a survivor holding `F1` and `F1'`. Compacting the survivor materializes `F2`,
/// which holds one copy of each row the survivor read: scheme B never merges a shared
/// reference, so no row can be carried twice into `F2`.
#[tokio::test]
async fn test_mbt_case_multi_generation_copies() {
    common_telemetry::init_default_ut_logging();
    const CASE: &str = "multi-generation copies";
    let mut model = StorageModel::new("mbt-case-multi-generation").await;
    let mut report = Vec::new();
    let mut agreed = true;

    let source = model.live_regions()[0];
    let written = model
        .write(&[
            PlannedRow {
                key: 1,
                ts: 0,
                payload: 1.0,
            },
            PlannedRow {
                key: 7,
                ts: 1000,
                payload: 7.0,
            },
        ])
        .await;
    let ids: Vec<i64> = written.iter().map(|event| event.event_id).collect();
    model.flush_dirty().await;
    let step = model
        .record_step(
            CASE,
            "after flushing F0 (creator = whole domain)",
            &mut report,
        )
        .await;
    agreed &= step.mismatched().is_empty();

    // Generation 1: materialize on the left half only.
    let (left, right) = model.split(source, 5).await;
    model.compact(left).await;
    let step = model
        .record_step(CASE, "after compacting the left half (F1)", &mut report)
        .await;
    agreed &= step.mismatched().is_empty();

    // Generation 2: merge, then materialize the whole survivor.
    let survivor = model.merge_all().await;
    model.compact(survivor).await;
    let step = model
        .record_step(CASE, "after compacting the survivor (F2)", &mut report)
        .await;
    agreed &= step.mismatched().is_empty();
    assert_eq!(
        BTreeMap::from([(ids[0], 1), (ids[1], 1)]),
        model.spec.predict_counts(survivor),
        "F2 is stamped with the survivor's domain and holds exactly the rows it read: each half's \
         own file carries only that half's rows, so each event is read once"
    );

    // Generation 3: split the survivor, then merge both halves again.
    let (p, q) = model.split(survivor, 5).await;
    let step = model
        .record_step(CASE, "after splitting F2", &mut report)
        .await;
    agreed &= step.mismatched().is_empty();
    assert_eq!(BTreeMap::from([(ids[0], 1)]), model.spec.predict_counts(p));
    assert_eq!(BTreeMap::from([(ids[1], 1)]), model.spec.predict_counts(q));

    let rejoined = model.merge_all().await;
    let step = model
        .record_step(CASE, "after merging both halves again", &mut report)
        .await;
    agreed &= step.mismatched().is_empty();
    assert_eq!(
        BTreeMap::from([(ids[0], 1), (ids[1], 1)]),
        model.spec.predict_counts(rejoined),
        "both targets inherit F2's single reference, and the domain decides which rows each \
         sees; F2 holds one physical copy of each row"
    );
    assert!(
        !step.oracle_fired(),
        "no row was lost or duplicated by the three generations"
    );
    assert!(
        agreed,
        "the prediction must agree with the engine in every step of this case:\n\n{}",
        report.join("\n\n")
    );
    let _ = right;
}

/// Case 3: the same merge twice, and a merge followed immediately by a split at the same cut.
///
/// The first merge settles scheme B's refusal (the un-compacted half still holds the shared
/// file) by compacting that half, and publishes a survivor holding both halves' own files, each
/// event once. The retry re-runs the same remap over the same inputs, so it must republish the
/// same file ids. Splitting right after the merge at the cut the shape started from hands both
/// targets the same two references, and the domain decides what each of them sees.
#[tokio::test]
async fn test_mbt_case_repeated_merge() {
    common_telemetry::init_default_ut_logging();
    const CASE: &str = "repeated merge";
    let mut model = StorageModel::new("mbt-case-repeated-merge").await;
    let mut report = Vec::new();
    let mut agreed = true;

    let source = model.live_regions()[0];
    let written = model
        .write(&[
            PlannedRow {
                key: 1,
                ts: 0,
                payload: 1.0,
            },
            PlannedRow {
                key: 7,
                ts: 1000,
                payload: 7.0,
            },
        ])
        .await;
    let ids: Vec<i64> = written.iter().map(|event| event.event_id).collect();
    model.flush_dirty().await;
    let (left, right) = model.split(source, 5).await;
    model.compact(left).await;
    let step = model
        .record_step(
            CASE,
            "after the split and the one-side compaction",
            &mut report,
        )
        .await;
    agreed &= step.mismatched().is_empty();

    let first = model.merge_all().await;
    let step = model
        .record_step(CASE, "after the first merge", &mut report)
        .await;
    agreed &= step.mismatched().is_empty();
    assert_eq!(
        BTreeMap::from([(ids[0], 1), (ids[1], 1)]),
        model.spec.predict_counts(first)
    );

    // The same set merged a second time: a merge of a single live region is a no-op, so the
    // routing is unchanged and nothing is copied.
    assert_eq!(
        vec![first],
        model.live_regions(),
        "after a merge only the survivor serves the table, so a second merge has nothing to do"
    );
    // The same inputs merged a second time, as a coordinator retry would.
    let retried = model.retry_merge(first, &[left, right]).await;
    let step = model
        .record_step(
            CASE,
            "after retrying the merge of the same inputs",
            &mut report,
        )
        .await;
    agreed &= step.mismatched().is_empty();
    assert_eq!(
        BTreeMap::from([(ids[0], 1), (ids[1], 1)]),
        model.spec.predict_counts(retried),
        "a retry republishes the reference set the first attempt published, so no row is copied \
         a second time"
    );

    // Merge then immediately split on the same cut.
    let (again_left, again_right) = model.split(retried, 5).await;
    let step = model
        .record_step(
            CASE,
            "after splitting the survivor at the same cut",
            &mut report,
        )
        .await;
    agreed &= step.mismatched().is_empty();
    assert_eq!(
        BTreeMap::from([(ids[0], 1)]),
        model.spec.predict_counts(again_left),
        "both targets inherit the survivor's references unchanged, and only the domain decides \
         which rows each of them sees"
    );
    assert_eq!(
        BTreeMap::from([(ids[1], 1)]),
        model.spec.predict_counts(again_right)
    );
    assert!(
        !step.oracle_fired(),
        "no row was lost or duplicated by the merge, the retry or the split that followed"
    );
    assert!(
        agreed,
        "the prediction must agree with the engine in every step of this case:\n\n{}",
        report.join("\n\n")
    );
}

/// Case 4: a split target that receives no rows at all.
///
/// The engine hands the shared file to that half (repartition copies every file of the source
/// into every target) and scheme B inherits it whole, so the specification publishes the same
/// reference; the half's domain simply admits no row the file stores. The prediction is empty,
/// and it stays empty when the empty half compacts the file it cannot read — a compaction whose
/// output carries no row, which also makes that half mergeable again.
#[tokio::test]
async fn test_mbt_case_empty_domain() {
    common_telemetry::init_default_ut_logging();
    const CASE: &str = "empty split target";
    let mut model = StorageModel::new("mbt-case-empty-domain").await;
    let mut report = Vec::new();
    let mut agreed = true;

    let source = model.live_regions()[0];
    model
        .write(&[
            PlannedRow {
                key: 1,
                ts: 0,
                payload: 1.0,
            },
            PlannedRow {
                key: 2,
                ts: 1000,
                payload: 2.0,
            },
        ])
        .await;
    model.flush_dirty().await;
    let (left, empty) = model.split(source, 5).await;
    let step = model
        .record_step(
            CASE,
            "after the split: the right half receives no rows",
            &mut report,
        )
        .await;
    agreed &= step.mismatched().is_empty();
    assert!(model.spec.predict_counts(empty).is_empty());
    assert!(
        !model.holder_files(empty).is_empty(),
        "the engine hands the shared file to the half that receives no rows"
    );
    assert_eq!(
        1,
        model
            .spec
            .region(empty)
            .expect("the empty half is published")
            .refs
            .len(),
        "the specification inherits the reference the engine hands it, whole: {}",
        model.spec.describe(empty)
    );

    model.compact(empty).await;
    let step = model
        .record_step(CASE, "after compacting the empty half", &mut report)
        .await;
    agreed &= step.mismatched().is_empty();
    assert!(
        model.spec.predict_counts(empty).is_empty(),
        "a region that sees no row produces no row, whatever the compaction writes"
    );

    let merged = model.merge_all().await;
    let step = model
        .record_step(CASE, "after merging the empty half back", &mut report)
        .await;
    agreed &= step.mismatched().is_empty();
    assert_eq!(
        model
            .ledger
            .events
            .keys()
            .map(|event_id| (*event_id, 1u64))
            .collect::<BTreeMap<_, _>>(),
        model.spec.predict_counts(merged),
        "every committed event is visible exactly once after the empty half rejoins"
    );
    assert!(
        !step.oracle_fired(),
        "no row was lost or duplicated by the empty domain"
    );
    assert!(
        agreed,
        "the prediction must agree with the engine in every step of this case:\n\n{}",
        report.join("\n\n")
    );
    let _ = left;
}

/// Case 5: two append-mode events with the identical `(key, ts)` inside one file.
///
/// `(PK, ts)` is not an event identity in an append table, so the flush, the split, the
/// compaction and the merge must all keep both events. The case pins that no step dedups them
/// into one row: the event ids inside the SST are what the ledger counts.
#[tokio::test]
async fn test_mbt_case_same_key_and_timestamp() {
    common_telemetry::init_default_ut_logging();
    const CASE: &str = "two events sharing (key, ts)";
    let mut model = StorageModel::new("mbt-case-same-key-ts").await;
    let mut report = Vec::new();
    let mut agreed = true;

    let source = model.live_regions()[0];
    let row = PlannedRow {
        key: 3,
        ts: 0,
        payload: 1.0,
    };
    let first = model.write(&[row]).await;
    let second = model.write(&[row]).await;
    let (a, b) = (first[0].event_id, second[0].event_id);
    assert_ne!(
        a, b,
        "the two events are distinct rows of the same (key, ts)"
    );
    model.flush_dirty().await;
    let step = model
        .record_step(
            CASE,
            "after flushing two rows with the same (key, ts)",
            &mut report,
        )
        .await;
    agreed &= step.mismatched().is_empty();
    assert_eq!(
        BTreeMap::from([(a, 1), (b, 1)]),
        model.spec.predict_counts(source),
        "one file, two events, each visible once"
    );

    let (left, right) = model.split(source, 5).await;
    let step = model
        .record_step(CASE, "after the split", &mut report)
        .await;
    agreed &= step.mismatched().is_empty();
    assert_eq!(
        BTreeMap::from([(a, 1), (b, 1)]),
        model.spec.predict_counts(left)
    );
    assert!(model.spec.predict_counts(right).is_empty());

    model.compact(left).await;
    let step = model
        .record_step(
            CASE,
            "after compacting the half that holds both events",
            &mut report,
        )
        .await;
    agreed &= step.mismatched().is_empty();
    assert_eq!(
        BTreeMap::from([(a, 1), (b, 1)]),
        model.spec.predict_counts(left),
        "a compaction must not fold two events that share (key, ts) into one row"
    );

    let merged = model.merge_all().await;
    let step = model
        .record_step(CASE, "after merging both halves", &mut report)
        .await;
    agreed &= step.mismatched().is_empty();
    assert_eq!(
        BTreeMap::from([(a, 1), (b, 1)]),
        model.spec.predict_counts(merged),
        "after the refusal and the other half's compaction the survivor holds one file per half, \
         so each of the two events is visible once and never collapsed into one row"
    );
    assert!(
        agreed,
        "the prediction must agree with the engine in every step of this case:\n\n{}",
        report.join("\n\n")
    );
}

/// Case 6: a file whose creator was never recorded (the legacy state #10646 refuses on merge).
///
/// A region's own flushed SST is re-registered under a new `FileId` with
/// `partition_expr == None`, which is the state a file written before the creating expression
/// was persisted is in. The unrecorded creator proves nothing, so a merge refuses it: if both
/// children of an earlier split inherit the object and only one of them compacts, the merged
/// region would hold that child's rows under two file ids and an append-only scan would return
/// them twice.
///
/// The case drives exactly that: split, compact the left half, expect the refusal, compact the
/// right half, and only then let the merge through — with every event read once at every step.
#[tokio::test]
async fn test_mbt_case_legacy_file_without_creator() {
    common_telemetry::init_default_ut_logging();
    const CASE: &str = "legacy file without a recorded creator";
    let mut model = StorageModel::new("mbt-case-legacy-file").await;
    let mut report = Vec::new();
    let mut agreed = true;

    let source = model.live_regions()[0];
    let written = model
        .write(&[
            PlannedRow {
                key: 1,
                ts: 0,
                payload: 1.0,
            },
            PlannedRow {
                key: 7,
                ts: 1000,
                payload: 7.0,
            },
        ])
        .await;
    let ids: Vec<i64> = written.iter().map(|event| event.event_id).collect();
    model.flush_dirty().await;
    let step = model
        .record_step(
            CASE,
            "after flushing F0 (creator = whole domain)",
            &mut report,
        )
        .await;
    agreed &= step.mismatched().is_empty();

    // The state under test: the same file, re-registered with no recorded creator.
    let published = model.publish_creatorless_file(source).await;
    common_telemetry::info!("[mbt-case] {published}");
    let step = model
        .record_step(
            CASE,
            "after re-registering F0 with no recorded creator",
            &mut report,
        )
        .await;
    agreed &= step.mismatched().is_empty();
    assert_eq!(
        BTreeMap::from([(ids[0], 1), (ids[1], 1)]),
        model.spec.predict_counts(source),
        "a file with no recorded creator still shows exactly its own rows once"
    );
    let files = model.holder_files(source);
    assert_eq!(
        1,
        files.len(),
        "the edit replaced the file, it did not add one"
    );
    let file_id = *files.iter().next().expect("the legacy file");
    assert!(
        model.file_meta(source, file_id).partition_expr.is_none(),
        "the published reference has to record no creator at all"
    );

    // Both children inherit the creator-less object: that is what makes it dangerous.
    let (left, right) = model.split(source, 5).await;
    let step = model
        .record_step(CASE, "after the split of the legacy file", &mut report)
        .await;
    agreed &= step.mismatched().is_empty();
    assert_eq!(
        BTreeMap::from([(ids[0], 1)]),
        model.spec.predict_counts(left),
        "the left half reads only the rows its domain owns from the creator-less file"
    );
    assert_eq!(
        BTreeMap::from([(ids[1], 1)]),
        model.spec.predict_counts(right),
        "the right half reads only the rows its domain owns from the creator-less file"
    );
    assert_eq!(
        model.holder_files(left),
        model.holder_files(right),
        "a split hands both halves the same reference"
    );

    // One child materialises into a file it stamped itself; the other still holds the legacy
    // object, so the merge has to be refused — and it has to name the legacy object, not the
    // materialized one.
    model.compact(left).await;
    let step = model
        .record_step(
            CASE,
            "after compacting the left half into its own file",
            &mut report,
        )
        .await;
    agreed &= step.mismatched().is_empty();
    assert!(
        !model.holder_files(left).contains(&file_id),
        "the compaction consumed the creator-less reference from the left half"
    );
    let materialized = *model
        .holder_files(left)
        .iter()
        .next()
        .expect("the left half's compaction output");
    assert_ne!(
        materialized, file_id,
        "the output is a new file id, not the inherited object"
    );
    assert_eq!(
        BTreeMap::from([(ids[0], 1)]),
        model.spec.predict_counts(left),
        "the left half still reads exactly its own row"
    );
    assert_eq!(
        BTreeMap::from([(ids[1], 1)]),
        model.spec.predict_counts(right),
        "the right half still reads exactly its own row"
    );
    let verdict = model.spec.merge_verdict(&[left, right]);
    assert!(
        !verdict.is_allowed(),
        "a creator-less file must block the merge: {}",
        verdict.describe()
    );
    let refusal = model.probe_merge_refusal(&[left, right]).await;
    let err = match refusal {
        Ok(probe) => panic!(
            "SCHEME B DIVERGES FROM THE ENGINE: {} for inputs [{left}, {right}], but the remap \
             into staging region {probe} was accepted. A creator-less file can hold the rows of \
             both children of an earlier split",
            verdict.describe()
        ),
        Err(err) => err,
    };
    assert!(
        matches!(
            err.as_any().downcast_ref::<crate::error::Error>(),
            Some(crate::error::Error::ForeignFilePartitionExpr { .. })
        ),
        "the refusal must be the scheme B variant: {err:?}"
    );
    let message = err.to_string();
    assert!(
        message.contains(&right.to_string()),
        "the refusal must name the region that still holds the legacy object: {message}"
    );
    assert!(
        message.contains(&file_id.to_string()),
        "the refusal must name the creator-less file: {message}"
    );
    assert!(
        !message.contains(&materialized.to_string()),
        "the self-stamped output of the compacted half is not an offender: {message}"
    );
    let step = model
        .record_step(
            CASE,
            "after the refused merge of the legacy file",
            &mut report,
        )
        .await;
    agreed &= step.mismatched().is_empty();
    assert_eq!(
        BTreeMap::from([(ids[0], 1)]),
        model.spec.predict_counts(left),
        "the refusal changes nothing: the left half still reads its row once"
    );
    assert_eq!(
        BTreeMap::from([(ids[1], 1)]),
        model.spec.predict_counts(right),
        "the refusal changes nothing: the right half still reads its row once"
    );

    // The second child materialises too, so both participants now hold their own files.
    model.compact(right).await;
    let step = model
        .record_step(
            CASE,
            "after compacting the right half into its own file",
            &mut report,
        )
        .await;
    agreed &= step.mismatched().is_empty();
    assert!(
        model.spec.merge_verdict(&[left, right]).is_allowed(),
        "both halves hold files they stamped themselves, so the retry is allowed"
    );
    let survivor = model.merge_all().await;
    let step = model
        .record_step(
            CASE,
            "after merging both holders of the materialized files",
            &mut report,
        )
        .await;
    agreed &= step.mismatched().is_empty();
    assert_eq!(
        BTreeMap::from([(ids[0], 1), (ids[1], 1)]),
        model.spec.predict_counts(survivor),
        "the survivor holds one materialized file per half, so each event is read exactly once"
    );
    assert_eq!(
        BTreeSet::from([
            materialized,
            *model.holder_files(right).iter().next().unwrap()
        ]),
        model.holder_files(survivor),
        "the survivor holds exactly the two materialized files"
    );
    assert!(
        !model.holder_files(survivor).contains(&file_id),
        "the creator-less object left both halves when they compacted"
    );
    assert!(
        !step.oracle_fired(),
        "no row was lost, duplicated or misplaced while settling the creator-less file"
    );
    assert!(
        agreed,
        "the prediction must agree with the engine in every step of this case:\n\n{}",
        report.join("\n\n")
    );
}

/// Chaos: the region lifecycle the serial generator cannot interleave.
///
/// Three variants, each on its own engine and each recording what was closed and reopened, the
/// reference state before and after the reopen (file ids plus the range a reference exposes),
/// and the predicted-vs-observed visibility of every region at every step:
///
/// - **A**: split, compact one side, close and reopen the region that still holds the original
///   file, then merge and judge the ledger;
/// - **B**: a flush of new rows interleaves between the split and the compaction, and the
///   compacting half is closed and reopened before its compaction;
/// - **C**: the merge runs, its survivor is closed and reopened, and then the merge is retried
///   over the same inputs.
#[tokio::test]
async fn test_mbt_case_chaos_close_reopen() {
    common_telemetry::init_default_ut_logging();
    chaos_variant_a_original_file_holder().await;
    chaos_variant_b_interleaved_flush().await;
    chaos_variant_c_merge_retry().await;
}

/// Variant A: the region that still holds the original file is closed and reopened.
async fn chaos_variant_a_original_file_holder() {
    const CASE: &str = "chaos A: reopen the original-file holder before the merge";
    let mut model = StorageModel::new("mbt-chaos-a").await;
    let mut report = Vec::new();
    let mut agreed = true;

    let source = model.live_regions()[0];
    let written = model
        .write(&[
            PlannedRow {
                key: 1,
                ts: 0,
                payload: 1.0,
            },
            PlannedRow {
                key: 7,
                ts: 1000,
                payload: 7.0,
            },
        ])
        .await;
    let ids: Vec<i64> = written.iter().map(|event| event.event_id).collect();
    model.flush_dirty().await;
    let (left, right) = model.split(source, 5).await;
    model.compact(left).await;
    let step = model
        .record_step(CASE, "after the split and one-side compaction", &mut report)
        .await;
    agreed &= step.mismatched().is_empty();

    // Close and reopen the half that still holds the original file.
    let held = model.holder_files(right);
    report.push(format!("{CASE}: {}", model.close_region(right).await));
    let step = model
        .record_step(
            CASE,
            "while the original-file holder is closed",
            &mut report,
        )
        .await;
    agreed &= step.mismatched().is_empty();
    assert_eq!(
        vec![right],
        step.closed,
        "the closed region must be unobservable while it is closed"
    );
    report.push(format!("{CASE}: {}", model.reopen_region(right).await));
    assert_eq!(
        held,
        model.holder_files(right),
        "the reopen must reload the same file ids from the persisted manifest"
    );
    let step = model
        .record_step(
            CASE,
            "after reopening the original-file holder",
            &mut report,
        )
        .await;
    agreed &= step.mismatched().is_empty();
    assert_eq!(
        BTreeMap::from([(ids[1], 1)]),
        model.spec.predict_counts(right),
        "the reopened region sees the shared file through its own domain predicate"
    );

    let merged = model.merge_all().await;
    let step = model
        .record_step(CASE, "after merging the reopened holder back", &mut report)
        .await;
    agreed &= step.mismatched().is_empty();
    assert_eq!(
        BTreeMap::from([(ids[0], 1), (ids[1], 1)]),
        model.spec.predict_counts(merged),
        "the reopen changed neither the reference set nor the prediction"
    );
    assert!(
        !step.oracle_fired(),
        "the one-side compaction and the reopen lose and duplicate no row"
    );
    assert_chaos_agreed(agreed, &report);
}

/// Variant B: a flush of new rows interleaves between the split and the compaction.
async fn chaos_variant_b_interleaved_flush() {
    const CASE: &str = "chaos B: interleaved flush, close/reopen before the compaction";
    let mut model = StorageModel::new("mbt-chaos-b").await;
    let mut report = Vec::new();
    let mut agreed = true;

    let source = model.live_regions()[0];
    let written = model
        .write(&[
            PlannedRow {
                key: 1,
                ts: 0,
                payload: 1.0,
            },
            PlannedRow {
                key: 7,
                ts: 1000,
                payload: 7.0,
            },
        ])
        .await;
    let ids: Vec<i64> = written.iter().map(|event| event.event_id).collect();
    model.flush_dirty().await;
    let (left, right) = model.split(source, 5).await;
    let step = model
        .record_step(CASE, "after the split", &mut report)
        .await;
    agreed &= step.mismatched().is_empty();

    // A flush of new rows interleaves between the split and the compaction.
    let interleaved = model
        .write(&[
            PlannedRow {
                key: 2,
                ts: 2000,
                payload: 2.0,
            },
            PlannedRow {
                key: 8,
                ts: 3000,
                payload: 8.0,
            },
        ])
        .await;
    let late: Vec<i64> = interleaved.iter().map(|event| event.event_id).collect();
    model.flush_dirty().await;
    let step = model
        .record_step(CASE, "after the interleaved flush", &mut report)
        .await;
    agreed &= step.mismatched().is_empty();

    // The half that still holds the original file is closed and reopened before the
    // compaction of the other half. A reopened region is a follower, so the compaction lands
    // on the half that still accepts writes, exactly as a coordinator would route it.
    let held = model.holder_files(right);
    report.push(format!("{CASE}: {}", model.close_region(right).await));
    let step = model
        .record_step(
            CASE,
            "while the original-file holder is closed",
            &mut report,
        )
        .await;
    agreed &= step.mismatched().is_empty();
    report.push(format!("{CASE}: {}", model.reopen_region(right).await));
    assert_eq!(
        held,
        model.holder_files(right),
        "the reopen must reload the same file ids, including the interleaved flush"
    );
    let step = model
        .record_step(
            CASE,
            "after reopening the original-file holder",
            &mut report,
        )
        .await;
    agreed &= step.mismatched().is_empty();

    model.compact(left).await;
    let step = model
        .record_step(CASE, "after compacting the writable half", &mut report)
        .await;
    agreed &= step.mismatched().is_empty();
    assert_eq!(
        BTreeMap::from([(ids[0], 1), (late[0], 1)]),
        model.spec.predict_counts(left),
        "the compaction rewrites every row the compacting half sees, including the row of the \
         interleaved flush"
    );

    let merged = model.merge_all().await;
    let step = model
        .record_step(CASE, "after merging both halves", &mut report)
        .await;
    agreed &= step.mismatched().is_empty();
    assert_eq!(
        BTreeMap::from([(ids[0], 1), (late[0], 1), (ids[1], 1), (late[1], 1)]),
        model.spec.predict_counts(merged),
        "the survivor holds the right half's own files (the shared one and the interleaved \
         flush, both stamped by the source) plus the left half's compaction copy, each row once"
    );
    assert!(
        !step.oracle_fired(),
        "the interleaved flush and the one-side compaction lose and duplicate no row"
    );
    assert_chaos_agreed(agreed, &report);
}

/// Variant C: the merge survivor is closed and reopened, then the merge is retried.
async fn chaos_variant_c_merge_retry() {
    const CASE: &str = "chaos C: close/reopen the survivor and retry the merge";
    let mut model = StorageModel::new("mbt-chaos-c").await;
    let mut report = Vec::new();
    let mut agreed = true;

    let source = model.live_regions()[0];
    let written = model
        .write(&[
            PlannedRow {
                key: 1,
                ts: 0,
                payload: 1.0,
            },
            PlannedRow {
                key: 7,
                ts: 1000,
                payload: 7.0,
            },
        ])
        .await;
    let ids: Vec<i64> = written.iter().map(|event| event.event_id).collect();
    model.flush_dirty().await;
    let (left, right) = model.split(source, 5).await;
    model.compact(left).await;
    let first = model.merge_all().await;
    let step = model
        .record_step(CASE, "after the first merge", &mut report)
        .await;
    agreed &= step.mismatched().is_empty();

    let held = model.holder_files(first);
    report.push(format!("{CASE}: {}", model.close_region(first).await));
    let step = model
        .record_step(CASE, "while the survivor is closed", &mut report)
        .await;
    agreed &= step.mismatched().is_empty();
    report.push(format!("{CASE}: {}", model.reopen_region(first).await));
    assert_eq!(
        held,
        model.holder_files(first),
        "the reopen must reload the same file ids the merge published"
    );
    let step = model
        .record_step(CASE, "after reopening the survivor", &mut report)
        .await;
    agreed &= step.mismatched().is_empty();
    assert_eq!(
        BTreeMap::from([(ids[0], 1), (ids[1], 1)]),
        model.spec.predict_counts(first),
        "the reopened survivor sees what the first merge published"
    );

    // Retry the merge over the same inputs.
    let retried = model.retry_merge(first, &[left, right]).await;
    let step = model
        .record_step(
            CASE,
            "after retrying the merge over the same inputs",
            &mut report,
        )
        .await;
    agreed &= step.mismatched().is_empty();
    assert_eq!(
        held,
        model.holder_files(retried),
        "the retry republishes the same file ids, so it cannot add a third copy"
    );
    assert_eq!(
        BTreeMap::from([(ids[0], 1), (ids[1], 1)]),
        model.spec.predict_counts(retried),
        "the retried merge republishes exactly what the first attempt published"
    );
    assert_chaos_agreed(agreed, &report);
}

/// Closes a chaos case: every step's prediction must have agreed with the engine.
fn assert_chaos_agreed(agreed: bool, report: &[String]) {
    assert!(
        agreed,
        "the prediction must agree with the engine in every step of this chaos case:\n\n{}",
        report.join("\n\n")
    );
}

/// Milestone 2: fixed-seed random traces over the same table.
///
/// Ops: `write` (routed by partition key), `flush`, `split` (data-aware domain cut),
/// `merge` (all live regions into one), `region compaction`, `import` (publish an SST the
/// write path never produced) and table-level (multi-region) compaction. The oracle, the
/// model-side domain assertion and the four observed invariant categories run after
/// every published step. Every plan opens with [`shared_file_shape`], so each seed is driven
/// through a split whose halves both keep the shared file, a compaction of exactly one half
/// and the merge of both, which meets scheme B's refusal and is retried once the other half
/// compacted as well. On a fatal failure the trace is shrunk to the shortest failing
/// prefix, which is reported with the seed, the step index, the operation, the violated
/// categories, the diff and the shapes reached.
///
/// The seed space comes from the environment (`MITO_MBT_SEED`, `MITO_MBT_SEEDS`) and falls
/// back to the three default seeds.
#[tokio::test]
async fn test_mbt_random_traces() {
    common_telemetry::init_default_ut_logging();
    let seeds = seeds_from_env();
    let seed_count = seeds.len();
    let mut all_ok = true;
    let mut failing = 0;
    let mut reports = Vec::new();
    for seed in seeds {
        let ops = generate_plan(seed, PLAN_LEN);
        let report = run_trace(&format!("mbt-{seed}"), seed, &ops).await;
        let observations = if report.observations.is_empty() {
            format!("\n  shapes driven: {:?}", report.shapes)
        } else {
            format!(
                "\n  shapes driven: {:?}\n  non-fatal observations:\n  {}",
                report.shapes,
                report.observations_summary()
            )
        };
        match report.failure {
            None => reports.push(format!(
                "seed {seed} ({seed:#018x}): no fatal violation in {} steps{observations}",
                ops.len()
            )),
            Some(failure) => {
                all_ok = false;
                failing += 1;
                reports.push(format!(
                    "{}{observations}",
                    shrink(seed, &ops, failure).await
                ));
            }
        }
        // Every category is data, so the per-trace report is logged even when it passes.
        if let Some(report) = reports.last() {
            common_telemetry::info!("[mbt] {report}");
        }
    }
    assert!(
        all_ok,
        "the storage model invariants were violated ({failing} of {seed_count} seeds):\n\n{}",
        reports.join("\n\n")
    );
}
