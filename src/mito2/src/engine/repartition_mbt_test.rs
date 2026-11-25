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
//! # The specification state
//!
//! [`RefModel`] is the executable specification: an independent model of the table that is
//! mutated only by the transitions [`Op`] describes and never by an engine observation.
//!
//! - A *published region* has a domain (a range of the trace's own keys), a set of
//!   *references*, and the keys of the rows it wrote and has not flushed yet (a memtable is
//!   not a physical file). Every reference is an opaque physical-file id the model minted
//!   itself plus the set of keys whose rows that reference authorizes inside that file
//!   ([`KeyAuth`], `None` meaning the whole file).
//! - `write` adds its rows' keys to the destination region's memtable.
//! - `flush` turns a region's memtable into one new physical file authorized for exactly
//!   the keys it holds.
//! - `split(source, cut)` gives each target the source's references restricted to that
//!   target's domain; a restriction that authorizes no row is not published.
//! - `merge(inputs, survivor)` combines references per physical file and unions their
//!   authorized sets.
//! - `region compaction(region)` is legal while the region holds a file another holder
//!   keeps: the output is a *new* physical file carrying exactly the rows the region owns
//!   (`domain ∩ authorized`), and the consumed references leave *that region only*.
//! - `import` publishes a byte copy of one of the region's own files under a new id, so the
//!   region's authorized rows do not move; multi-region compaction is a byte copy of the
//!   region's admitted inputs, so it does not move them either.
//!
//! # The prediction (the engine rule of this branch)
//!
//! [`RefModel::predict_visible_rows`] is the one place that states what a region's scan is
//! predicted to show, and it mirrors the engine of **this** branch instead of an ideal one:
//!
//! - a referenced file contributes the rows whose partition key satisfies the region's domain,
//!   *except* when the region's partition expression equals the file's creator expression:
//!   `ScanRegion::should_skip_region_partition` (`src/mito2/src/read/scan_region.rs`) then
//!   drops the whole partition predicate, so every physical row of that file is visible to the
//!   region;
//! - the contribution is intersected with what each reference authorizes, which on this branch
//!   is the whole file: `FileMeta` carries no row range, so a reference authorizes every row of
//!   the object it points at;
//! - rows still in the memtable are always filtered by the domain, because the partition
//!   predicate belongs to the memtable scan too;
//! - `split` narrows a target to its domain, `merge` unions the inputs' references, and
//!   compaction writes a new file holding exactly the rows the compacting region sees, removing
//!   the consumed references from that region only.
//!
//! The prediction is the model's statement *about* the engine, so it is kept apart from the
//! judgement: neither the ledger oracle nor the invariant categories above read it. The cases
//! below print `predicted` next to `observed` per region and continue on a divergence instead of
//! adjusting the model to an observation.
//!
//! # Cases the serial generator cannot reach
//!
//! `test_mbt_case_*` pins the states the random traces only reach by luck — an out-of-domain row,
//! a two-hop split, copies across three generations, a repeated merge, an empty split target, two
//! append-mode events sharing `(key, ts)` — and `test_mbt_case_chaos_close_reopen` interleaves a
//! region close/reopen (and a merge retry) into the shared-file shape.
//!
//! # Model-side assertion (separate from the ledger oracle)
//!
//! After every published step [`StorageModel::check_model_authorization`] asserts that a
//! region's visible rows are a subset of the union of its own references' authorized sets
//! (plus the keys of its own unflushed rows). That is "selecting a file does not authorize
//! reading all of it", stated on the model side: a split that hands both halves the same
//! object still leaves each half authorized to read only its own rows.
//!
//! This statement and the ledger oracle are independent: the model derives from the
//! transitions above only, the oracle from the ledger only, and neither hands a mutable
//! object to the other. The oracle keeps its exact meaning (each committed event exactly
//! once, nothing added, nothing misplaced); the model-side check can pass while the oracle
//! fails, because authorization is not counting.
//!
//! # Layering
//!
//! Physical objects, per-holder region references and the published routing are three
//! separate views. Only immutable snapshots ([`SstRef`]) cross a layer boundary; no layer
//! ever receives a `FileMeta` that another layer mutates. The specification state is a
//! fourth view that reads none of the three: it is advanced by the transitions only.
//!
//! # Invariants beyond the oracle
//!
//! The oracle above is the visible-row statement of the bug. This module also checks five
//! invariant categories after *every* published step, each reported under its own name so a
//! trace can fail on one without hiding the others:
//!
//! | category | what it checks |
//! | --- | --- |
//! | `manifest-consistency` | a holder's persisted manifest file set equals its live version file set (id, level, admission sequence, row count), and the published routing equals the regions that actually applied it (writable, applied partition expr, demoted when retired) |
//! | `model-authorization` | the model-side statement above: a region shows no row outside the union of its own references' authorized sets |
//! | `reference-bounds` | every reference a live holder shows authorizes only rows inside that holder's domain |
//! | `sequence-monotonicity` | per region the committed sequence never decreases across steps, and the persisted committed sequence never moves backwards |
//! | `physical-liveness` | every object still referenced by a live holder still reads back as a complete Parquet object, with the object id, the holding set and the step that dropped it reported when it does not |
//!
//! `reference-bounds` is reported but never fatal: it can only speak about what the engine
//! exposes, and on this branch one *legal* split already makes both halves reference an SST
//! whose creator domain is the whole table, so the category fires on every split by
//! construction. [`RefModel`] states the precise version of the same property per holder
//! (which keys each reference authorizes) and that statement *is* fatal. See the
//! `FileMeta::owned_range` TODO on [`SstRef::creator_domain`] for the field that would let
//! the observed check be as precise as the model.
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
//! # Traces
//!
//! Every seed's plan opens with the shape the model previously could not express, generated
//! deliberately rather than hoped for (`shared_file_shape`): collapse to one live region,
//! write rows over several distinct keys, flush that one file, split it so *both* halves keep
//! the shared file, and region-compact exactly one half. The block starts with a `Merge`, so
//! the split always has exactly two halves and the compaction always lands on one of them.
//! The two widened paths (`import`, multi-region compaction) then run against that two-holder
//! state, and the plan's merge completes the shape by merging both halves.
//! [`RefModel::shapes`] records which of the three steps a trace actually drove, and every
//! report prints them; random ops after the prologue may reach the same shape on their own.
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
use common_recordbatch::RecordBatches;
use datatypes::arrow::array::AsArray;
use datatypes::arrow::datatypes::Int64Type;
use datatypes::prelude::ConcreteDataType;
use datatypes::schema::ColumnSchema;
use datatypes::value::Value;
use object_store::ObjectStore;
use partition::expr::{Operand, PartitionExpr, RestrictedOp, col};
use store_api::metadata::ColumnMetadata;
use store_api::region_engine::{
    RegionEngine, RegionRole, RemapManifestsRequest, SettableRegionRoleState,
};
use store_api::region_request::{
    ApplyStagingManifestRequest, EnterStagingRequest, PathType, RegionCloseRequest,
    RegionCompactRequest, RegionCreateRequest, RegionFlushRequest, RegionOpenRequest,
    RegionRequest, StagingPartitionDirective,
};
use store_api::storage::{FileId, RegionId, ScanRequest};

#[cfg(feature = "enterprise")]
use super::table_compaction::PublicationOutcome;
use crate::config::MitoConfig;
use crate::engine::{MITO_ENGINE_NAME, MitoEngine};
use crate::gc::GcConfig;
use crate::manifest::action::RegionEdit;
use crate::region::{RegionLeaderState, RegionRoleState};
use crate::sst::file::{LEVEL_SHARED, RegionFileId, SHARED_LEVEL_SLOT};
use crate::sst::location;
use crate::test_util::{TestEnv, put_rows, rows_schema};

/// Number of distinct partition-key values (`k00..k09`).
const KEY_COUNT: usize = 10;
/// Wide enough to cover every timestamp the traces write.
const COMPACTION_WINDOW_SECONDS: i64 = 3600;
/// Length of one random trace.
const PLAN_LEN: usize = 18;
/// Fixed seeds; each seed is one independently reproducible trace.
const DEFAULT_SEEDS: [u64; 3] = [0x5EED_0001, 0x5EED_0002, 0x5EED_0003];
/// Upper bound on the number of seeds one `start:end` range may expand to.
const MAX_RANGE_SEEDS: u64 = 64;

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

    /// Whether every key of `other` is also a key of `self`.
    fn contains_domain(&self, other: &Domain) -> bool {
        self.lo <= other.lo && other.hi <= self.hi
    }
}

/// The key index encoded by a partition key value; `"k10"` (the upper bound of the full
/// domain) is index [`KEY_COUNT`].
fn key_index(name: &str) -> Option<usize> {
    name.strip_prefix('k')?.parse().ok()
}

/// Accumulates the two half bounds of the harness' restricted partition grammar.
///
/// `None` means "unbounded on that side", which is what an unpartitioned region means.
#[derive(Debug, Default)]
struct DecodedBounds {
    lo: Option<usize>,
    hi: Option<usize>,
}

fn accumulate_bounds(expr: &PartitionExpr, bounds: &mut DecodedBounds) -> bool {
    match (&*expr.lhs, &expr.op, &*expr.rhs) {
        (Operand::Expr(left), RestrictedOp::And, Operand::Expr(right)) => {
            accumulate_bounds(left, bounds) && accumulate_bounds(right, bounds)
        }
        (Operand::Column(column), RestrictedOp::GtEq, Operand::Value(Value::String(value)))
            if column == "k" =>
        {
            bounds.lo = key_index(value.as_utf8());
            bounds.lo.is_some()
        }
        (Operand::Column(column), RestrictedOp::Lt, Operand::Value(Value::String(value)))
            if column == "k" =>
        {
            bounds.hi = key_index(value.as_utf8());
            bounds.hi.is_some()
        }
        _ => false,
    }
}

/// Decodes `k >= key(lo) AND k < key(hi)` back into a [`Domain`].
///
/// Returns `None` for every expression this harness does not generate, so a mismatch is
/// reported instead of silently passing a boundary check.
fn decode_domain(expr: &PartitionExpr) -> Option<Domain> {
    let mut bounds = DecodedBounds::default();
    if !accumulate_bounds(expr, &mut bounds) {
        return None;
    }
    Some(Domain::new(
        bounds.lo.unwrap_or(0),
        bounds.hi.unwrap_or(KEY_COUNT),
    ))
}

/// The domain expressed by a region's or a file's partition expression JSON.
///
/// A missing expression imposes nothing, and so does the explicit single-region designation
/// (the empty string), so both decode to [`Domain::FULL`].
fn domain_of_json(json: &str) -> Domain {
    if json.is_empty() {
        return Domain::FULL;
    }
    let expr = PartitionExpr::from_json_str(json)
        .unwrap_or_else(|err| panic!("harness cannot parse partition expr {json:?}: {err}"))
        .unwrap_or_else(|| panic!("harness got an empty partition bound from {json:?}"));
    decode_domain(&expr).unwrap_or_else(|| {
        panic!(
            "harness cannot decode partition expr {json:?}; it only generates \
             `k >= key(lo) AND k < key(hi)`"
        )
    })
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

/// The keys one reference authorizes inside its own physical file.
///
/// `None` is the universe: every row of that physical file. A reference is stated in the
/// trace's own vocabulary — an opaque model-minted file id plus this set — never in an
/// implementation type (`FileId`, `PartitionExpr`, an owned range).
type KeyAuth = Option<BTreeSet<usize>>;

/// Every key the traces can write, i.e. the universe of a `None` authorization.
fn key_universe() -> BTreeSet<usize> {
    (0..KEY_COUNT).collect()
}

/// The keys `auth` authorizes, with `None` expanded to the universe.
fn auth_keys(auth: &KeyAuth) -> BTreeSet<usize> {
    auth.clone().unwrap_or_else(key_universe)
}

/// `auth` restricted to the keys of `domain`.
///
/// The outer `None` means "not published": a reference left authorizing no row is dropped,
/// so a split can hand out strictly fewer references than it was given. The inner
/// [`KeyAuth`] keeps its own meaning (`None` = the whole file).
fn auth_restrict(auth: &KeyAuth, domain: Domain) -> Option<KeyAuth> {
    let keys: BTreeSet<usize> = auth_keys(auth)
        .into_iter()
        .filter(|key| domain.contains_key(*key))
        .collect();
    (!keys.is_empty()).then(|| Some(keys))
}

/// The union of two authorizations of the *same* physical file.
fn auth_union(left: &KeyAuth, right: &KeyAuth) -> KeyAuth {
    match (left, right) {
        (None, _) | (_, None) => None,
        (Some(left), Some(right)) => Some(left.union(right).copied().collect()),
    }
}

/// Renders a key set as the `k03, k07` form the reports use.
fn describe_keys(keys: &BTreeSet<usize>) -> String {
    if keys.is_empty() {
        return "{}".to_string();
    }
    format!(
        "{{{}}}",
        keys.iter()
            .map(|key| key_name(*key))
            .collect::<Vec<_>>()
            .join(", ")
    )
}

/// One event's physical presence inside a model file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FileRow {
    /// The partition key of that event's row.
    key: usize,
    /// How many physical copies of the row the file holds. A compaction of a region that
    /// already sees one event twice writes both rows, so a file can legally hold several
    /// copies of one event.
    copies: u64,
}

/// One physical file of the specification: the rows it physically stores and the domain of
/// the region that produced it.
#[derive(Debug, Clone)]
struct PhysicalFile {
    /// The domain of the region that created the file, i.e. `FileMeta::partition_expr` on
    /// this branch.
    creator: Domain,
    /// Event id -> the row's physical presence in this file.
    rows: BTreeMap<i64, FileRow>,
}

/// The three steps of the shape the model exists for, recorded when a transition reaches
/// one so every report can say what the engine was actually driven through.
const SHAPE_SPLIT_SHARED: &str = "split handed the shared file to both halves";
const SHAPE_ONE_SIDE_COMPACTION: &str = "region compaction while another holder kept the file";
const SHAPE_MERGE_RECOMBINES: &str = "merge of both holders after that compaction";

/// One published region of the specification.
#[derive(Debug, Clone)]
struct SpecRegion {
    /// The keys this region serves.
    domain: Domain,
    /// Model-minted physical-file id -> the keys this region may read from that file.
    refs: BTreeMap<u64, KeyAuth>,
    /// Keys of the rows this region wrote and has not flushed. A memtable is not a
    /// physical file, so it is tracked apart from `refs` and counts as authorized.
    memtable: BTreeSet<usize>,
}

impl SpecRegion {
    fn new(domain: Domain) -> SpecRegion {
        SpecRegion {
            domain,
            refs: BTreeMap::new(),
            memtable: BTreeSet::new(),
        }
    }
}

/// The specification: what every published region serves and what its references authorize.
///
/// Only the transitions on this type mutate it. Nothing here is derived from an engine
/// observation, and the ledger oracle never receives it, so model state and judgement share
/// no mutable object.
#[derive(Debug)]
struct RefModel {
    /// Mints the opaque physical-file ids of this model (never a `FileId`).
    next_file: u64,
    /// Every currently published region.
    regions: BTreeMap<RegionId, SpecRegion>,
    /// Model-minted file id -> the rows the file physically stores and its creator domain.
    /// The prediction below is evaluated on this graph, never on an engine observation.
    files: BTreeMap<u64, PhysicalFile>,
    /// Event ids of the rows a region wrote and has not flushed yet (its memtable), with the
    /// partition key of each. Parallel to [`SpecRegion::memtable`], which keeps only the keys
    /// the authorization statement needs.
    pending: BTreeMap<RegionId, BTreeMap<i64, usize>>,
    /// The *engine's* reference set of each published region. A repartition copies every file
    /// of a source into every target, so a target may reference a file none of whose rows fall
    /// inside its domain; [`SpecRegion::refs`] is the tighter *authorization* view and drops
    /// such a reference. The prediction is stated on this set, because this is what the reader
    /// is handed.
    engine_refs: BTreeMap<RegionId, BTreeSet<u64>>,
    /// The shapes the transitions have driven the engine through.
    shapes: BTreeSet<&'static str>,
}

impl RefModel {
    fn new() -> RefModel {
        RefModel {
            next_file: 0,
            regions: BTreeMap::new(),
            files: BTreeMap::new(),
            pending: BTreeMap::new(),
            engine_refs: BTreeMap::new(),
            shapes: BTreeSet::new(),
        }
    }

    /// What one region's scan sees, predicted by mirroring the engine of **this** branch.
    ///
    /// This is the single place the visibility rule lives, and it is deliberately stated
    /// against the engine as it behaves here, not against an ideal engine:
    ///
    /// - `ScanRegion::predicate_for_file` (`src/mito2/src/read/scan_region.rs`) hands a file
    ///   its predicate *without* the region partition expression exactly when
    ///   `should_skip_region_partition` holds, i.e. when the region's partition expression
    ///   equals the file's creator expression. Then every physical row of that file is visible
    ///   to the region, whatever the region's domain says;
    /// - otherwise the file contributes the rows whose key satisfies the region's domain;
    /// - the contribution is intersected with what the reference authorizes, which on this
    ///   branch is the whole file: `FileMeta` has no row range (`owned_range`), so a reference
    ///   authorizes every physical row of the object it points at. The model's own, tighter
    ///   per-key authorization ([`SpecRegion::refs`]) is the *specification* statement that
    ///   `check_model_authorization` judges; comparing it with this prediction is exactly the
    ///   per-reference divergence the cases print;
    /// - rows still in the memtable are always filtered by the domain;
    /// - the transitions mirror the rest: `split` narrows a target to its domain (its
    ///   references keep pointing at the same objects), `merge` unions the inputs' references,
    ///   and compaction writes a new file holding the rows the compacting region *sees* while
    ///   removing the consumed references from that region only.
    ///
    /// `copies` is the number of physical rows of that event the scan returns, so an event that
    /// two referenced files both carry is reported twice instead of being counted once.
    fn predict_visible_rows(&self, region: RegionId) -> BTreeMap<i64, FileRow> {
        let mut visible: BTreeMap<i64, FileRow> = BTreeMap::new();
        let Some(state) = self.regions.get(&region) else {
            return visible;
        };
        let domain = state.domain;
        // A reference on this branch carries no row range, so its authorization is the whole
        // file: `KeyAuth` `None`.
        let reference_auth: KeyAuth = None;
        let authorized = auth_keys(&reference_auth);
        for file in self.engine_refs.get(&region).into_iter().flatten() {
            let Some(physical) = self.files.get(file) else {
                continue;
            };
            // The mirror of `should_skip_region_partition`: equal expressions mean the reader
            // drops the region partition predicate for this file.
            let skips_partition = physical.creator == domain;
            for (event_id, row) in &physical.rows {
                if !authorized.contains(&row.key) {
                    continue;
                }
                if !skips_partition && !domain.contains_key(row.key) {
                    continue;
                }
                let entry = visible.entry(*event_id).or_insert(FileRow {
                    key: row.key,
                    copies: 0,
                });
                entry.copies += row.copies;
            }
        }
        for (event_id, key) in self.pending.get(&region).into_iter().flatten() {
            if !domain.contains_key(*key) {
                continue;
            }
            visible.insert(
                *event_id,
                FileRow {
                    key: *key,
                    copies: 1,
                },
            );
        }
        visible
    }

    /// The same prediction stated as the `event id -> copies` multiset a scan returns.
    fn predict_counts(&self, region: RegionId) -> BTreeMap<i64, u64> {
        self.predict_visible_rows(region)
            .into_iter()
            .map(|(event_id, row)| (event_id, row.copies))
            .collect()
    }

    /// A new opaque physical-file id.
    fn mint(&mut self) -> u64 {
        let file = self.next_file;
        self.next_file += 1;
        file
    }

    fn region(&self, region: RegionId) -> Option<&SpecRegion> {
        self.regions.get(&region)
    }

    /// The live regions that hold `file`.
    fn holders_of(&self, file: u64) -> BTreeSet<RegionId> {
        self.regions
            .iter()
            .filter(|(_, state)| state.refs.contains_key(&file))
            .map(|(region, _)| *region)
            .collect()
    }

    /// `Create` / publication: the region serves `domain` and holds nothing yet.
    fn publish(&mut self, region: RegionId, domain: Domain) {
        self.regions.insert(region, SpecRegion::new(domain));
        self.engine_refs.insert(region, BTreeSet::new());
    }

    /// `write`: the rows land in the destination region's own memtable first.
    ///
    /// The key is recorded even when it falls outside the region's domain: write admission
    /// checks the partition *version* the region serves, never the row's partition value, so
    /// the engine accepts such a row and the prediction has to place it by rule.
    fn note_write(&mut self, region: RegionId, event_id: i64, key: usize) {
        if let Some(state) = self.regions.get_mut(&region) {
            state.memtable.insert(key);
        }
        self.pending
            .entry(region)
            .or_default()
            .insert(event_id, key);
    }

    /// `flush`: the memtable becomes one new physical file, authorized for exactly the keys
    /// it holds and created by this region. A flush with nothing to write publishes nothing.
    fn flushed(&mut self, region: RegionId) {
        let rows = self.pending.remove(&region).unwrap_or_default();
        let Some(state) = self.regions.get_mut(&region) else {
            return;
        };
        let keys = std::mem::take(&mut state.memtable);
        let domain = state.domain;
        if keys.is_empty() {
            return;
        }
        let file = self.mint();
        self.files.insert(
            file,
            PhysicalFile {
                creator: domain,
                rows: rows
                    .into_iter()
                    .map(|(event_id, key)| (event_id, FileRow { key, copies: 1 }))
                    .collect(),
            },
        );
        self.regions
            .get_mut(&region)
            .expect("a flushed region is published")
            .refs
            .insert(file, Some(keys));
        self.engine_refs.entry(region).or_default().insert(file);
    }

    /// `split`: each target gets the source's references restricted to its own domain.
    ///
    /// An empty restriction is not part of the *authorization* view, so a half whose rows all
    /// fall on the other side of the cut is authorized to read nothing from the shared file.
    /// The engine still hands that file to both halves (repartition copies every file of the
    /// source into every target), so the engine view keeps it for both.
    fn split(
        &mut self,
        source: RegionId,
        left: RegionId,
        left_domain: Domain,
        right: RegionId,
        right_domain: Domain,
    ) {
        let Some(state) = self.regions.remove(&source) else {
            return;
        };
        assert!(
            state.memtable.is_empty(),
            "the model split region {source} with unflushed rows; a split must flush its \
             source first"
        );
        let engine_refs = self.engine_refs.remove(&source).unwrap_or_default();
        let mut left_refs: BTreeMap<u64, KeyAuth> = BTreeMap::new();
        let mut right_refs: BTreeMap<u64, KeyAuth> = BTreeMap::new();
        for (file, auth) in &state.refs {
            if let Some(restricted) = auth_restrict(auth, left_domain) {
                left_refs.insert(*file, restricted);
            }
            if let Some(restricted) = auth_restrict(auth, right_domain) {
                right_refs.insert(*file, restricted);
            }
        }
        if left_refs.keys().any(|file| right_refs.contains_key(file)) {
            self.shapes.insert(SHAPE_SPLIT_SHARED);
        }
        self.regions.insert(
            left,
            SpecRegion {
                domain: left_domain,
                refs: left_refs,
                memtable: BTreeSet::new(),
            },
        );
        self.regions.insert(
            right,
            SpecRegion {
                domain: right_domain,
                refs: right_refs,
                memtable: BTreeSet::new(),
            },
        );
        // The engine view: every file of the source, for both halves.
        self.engine_refs.insert(left, engine_refs.clone());
        self.engine_refs.insert(right, engine_refs);
    }

    /// `merge`: references combined per physical file, authorized sets unioned.
    fn merge(&mut self, inputs: &[RegionId], survivor: RegionId) {
        let compacted_one_side = self.shapes.contains(SHAPE_ONE_SIDE_COMPACTION);
        let mut refs: BTreeMap<u64, KeyAuth> = BTreeMap::new();
        let mut engine_refs: BTreeSet<u64> = BTreeSet::new();
        for input in inputs {
            engine_refs.extend(self.engine_refs.remove(input).unwrap_or_default());
            let Some(state) = self.regions.remove(input) else {
                continue;
            };
            assert!(
                state.memtable.is_empty(),
                "the model merged region {input} with unflushed rows; a merge must flush its \
                 inputs first"
            );
            for (file, auth) in state.refs {
                // A reference is never empty, so the only merge to compute is the union;
                // `None` (the whole file) absorbs everything else.
                let authorized = match refs.remove(&file) {
                    Some(existing) => auth_union(&existing, &auth),
                    None => auth,
                };
                refs.insert(file, authorized);
            }
        }
        if compacted_one_side && refs.len() > 1 {
            self.shapes.insert(SHAPE_MERGE_RECOMBINES);
        }
        self.regions.insert(
            survivor,
            SpecRegion {
                domain: Domain::FULL,
                refs,
                memtable: BTreeSet::new(),
            },
        );
        self.engine_refs.insert(survivor, engine_refs);
    }

    /// A **retried** merge: the same inputs are remapped again after the first attempt was
    /// interrupted by a close/reopen, into a fresh survivor.
    ///
    /// The inputs' persisted manifests did not move, so the retry republishes exactly the
    /// reference set the first attempt published: the same physical file ids, with the same
    /// rows. That is why the state of the previous survivor is what the retry's survivor
    /// starts from — no row is copied a second time, so a retry cannot add a third copy of an
    /// event the first merge already published twice.
    fn retry_merge(&mut self, previous_survivor: RegionId, survivor: RegionId) {
        let Some(state) = self.regions.remove(&previous_survivor) else {
            return;
        };
        let engine_refs = self
            .engine_refs
            .remove(&previous_survivor)
            .unwrap_or_default();
        self.regions.insert(survivor, state);
        self.engine_refs.insert(survivor, engine_refs);
    }

    /// `region compaction`: legal even while another holder keeps a file this region holds.
    ///
    /// The output is a *new* physical file holding exactly the rows the region **sees** — the
    /// mirror of this branch's read path, so a row outside the region's domain that a
    /// reference still exposes is copied into the output too. Its creator is the compacting
    /// region, which is what makes the region's next scan skip the partition predicate for it.
    /// The consumed references leave this region only, so every other holder keeps its own
    /// reference to the same files.
    fn compact(&mut self, region: RegionId) {
        let Some(state) = self.regions.get(&region) else {
            return;
        };
        if self
            .engine_refs
            .get(&region)
            .is_none_or(|refs| refs.is_empty())
        {
            return;
        }
        let domain = state.domain;
        let shared: Vec<u64> = state
            .refs
            .keys()
            .copied()
            .filter(|file| self.holders_of(*file).len() > 1)
            .collect();
        let mut owned = BTreeSet::new();
        for auth in state.refs.values() {
            owned.extend(auth_keys(auth));
        }
        owned.retain(|key| domain.contains_key(*key));
        if !shared.is_empty() {
            self.shapes.insert(SHAPE_ONE_SIDE_COMPACTION);
        }

        // The rows the engine rewrites are exactly the rows it reads, so the output file is
        // the prediction evaluated on the input references.
        let rows = self.predict_visible_rows(region);
        let file = self.mint();
        self.files.insert(
            file,
            PhysicalFile {
                creator: domain,
                rows,
            },
        );
        let state = self
            .regions
            .get_mut(&region)
            .expect("the compacted region is published");
        state.refs.clear();
        if !owned.is_empty() {
            state.refs.insert(file, Some(owned));
        }
        self.engine_refs.insert(region, BTreeSet::from([file]));
    }

    /// `import`: a byte copy of one of the region's own flushed files, published under a new
    /// physical id, while the original reference leaves the region in the same edit.
    ///
    /// The region keeps exactly the rows it had, so only the identity of one reference
    /// changes. Which reference the engine picked cannot change what any transition below
    /// derives, because every one of them reads only intersections and unions of the
    /// authorized sets.
    fn remint(&mut self, region: RegionId) {
        let Some((file, auth)) = self.regions.get(&region).and_then(|state| {
            state
                .refs
                .iter()
                .next()
                .map(|(file, auth)| (*file, auth.clone()))
        }) else {
            return;
        };
        let new_file = self.mint();
        if let Some(physical) = self.files.remove(&file) {
            self.files.insert(new_file, physical);
        }
        let state = self
            .regions
            .get_mut(&region)
            .expect("an imported region is published");
        state.refs.remove(&file);
        state.refs.insert(new_file, auth);
        if let Some(refs) = self.engine_refs.get_mut(&region) {
            refs.remove(&file);
            refs.insert(new_file);
        }
    }

    /// The keys `region` may read: the union of its references' authorizations plus the keys
    /// of its own unflushed rows.
    fn authorized_keys(&self, region: RegionId) -> BTreeSet<usize> {
        let mut keys = BTreeSet::new();
        let Some(state) = self.regions.get(&region) else {
            return keys;
        };
        for auth in state.refs.values() {
            keys.extend(auth_keys(auth));
        }
        keys.extend(state.memtable.iter().copied());
        keys
    }

    /// One region's specification state, for a diff.
    fn describe(&self, region: RegionId) -> String {
        match self.regions.get(&region) {
            None => format!("region {region}: the specification has no state for it"),
            Some(state) => {
                let refs: Vec<String> = state
                    .refs
                    .iter()
                    .map(|(file, auth)| format!("f{file} -> {}", describe_keys(&auth_keys(auth))))
                    .collect();
                format!(
                    "region {region} (domain [{}, {})): refs {refs:?}, unflushed keys {}",
                    key_name(state.domain.lo),
                    key_name(state.domain.hi),
                    describe_keys(&state.memtable)
                )
            }
        }
    }

    /// Every published region's specification state, for a diff.
    fn describe_all(&self) -> String {
        if self.regions.is_empty() {
            return "  (no published region)".to_string();
        }
        self.regions
            .keys()
            .map(|region| format!("  {}", self.describe(*region)))
            .collect::<Vec<_>>()
            .join("\n")
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
    /// The storage slot the reference sits in: `0`, `1` or [`SHARED_LEVEL_SLOT`].
    slot_level: u8,
    num_rows: u64,
    /// The reference's admission sequence (`FileMeta::sequence`).
    sequence: Option<u64>,
    /// The domain of the region that created the object (`FileMeta::partition_expr`).
    ///
    /// This is the range of rows the reference let a reader see for as long as a reference
    /// carried no row authorization of its own.
    ///
    /// TODO(repart-mbt): this branch has no `FileMeta::owned_range`, so the authorized range
    /// is reported as the creator domain. Once `FileMeta::owned_range` exists, the
    /// authorized range is `creator_domain ∩ owned_range`, and `owned_range` itself must be
    /// inside `creator_domain`.
    creator_domain: Domain,
}

/// Layer 2: one region's own manifest view of the objects it references.
#[derive(Debug, Default, Clone)]
struct HolderView {
    /// The live version's file set (`Version::ssts`).
    files: BTreeMap<FileId, SstRef>,
    /// The persisted manifest's file set (`RegionManifest::files`), for the
    /// manifest/version consistency check.
    manifest: BTreeMap<FileId, ManifestRef>,
}

/// The persisted manifest's view of one reference.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ManifestRef {
    level: u8,
    sequence: Option<u64>,
    num_rows: u64,
}

/// What one region actually applied, captured per step.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct AppliedState {
    /// The partition domain the region applied (`RegionMetadata::partition_expr`).
    domain: Domain,
    /// `VersionControlData::committed_sequence`.
    committed: u64,
    /// `VersionControlData::last_sequence`.
    last_sequence: u64,
    /// `RegionManifest::committed_sequence`, the persisted committed sequence.
    manifest_committed: Option<u64>,
    /// The region currently accepts writes, i.e. it is the region that serves its domain.
    writable: bool,
}

/// Layer 1 aggregate: which holders reference each physical object.
///
/// Derived by observing the per-holder views; an object referenced by more than one
/// holder is *not* an error by itself (a legal split produces exactly that).
#[derive(Debug, Default, Clone)]
struct PhysicalView {
    holders_of: BTreeMap<FileId, BTreeSet<RegionId>>,
}

/// What one region had already reached, so a later step can only move forward.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct SequenceHighWater {
    /// The highest `VersionControlData::committed_sequence` observed.
    committed: u64,
    /// The highest `VersionControlData::last_sequence` observed.
    last: u64,
    /// The highest `RegionManifest::committed_sequence` observed.
    persisted: u64,
}

/// One invariant category. Each is reported separately.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Category {
    /// The visible-row oracle (not an invariant below, reported for context).
    Visibility,
    ManifestConsistency,
    /// The model-side statement: a region shows no row outside the union of its references'
    /// authorized sets.
    ModelAuthorization,
    ReferenceBounds,
    SequenceMonotonicity,
    PhysicalLiveness,
}

impl Category {
    fn name(self) -> &'static str {
        match self {
            Category::Visibility => "visibility",
            Category::ManifestConsistency => "manifest-consistency",
            Category::ModelAuthorization => "model-authorization",
            Category::ReferenceBounds => "reference-bounds",
            Category::SequenceMonotonicity => "sequence-monotonicity",
            Category::PhysicalLiveness => "physical-liveness",
        }
    }

    /// Whether a violation of this category fails a trace.
    ///
    /// `reference-bounds` cannot: on this branch one legal split already makes both halves
    /// reference an object whose creator domain is the whole table, so the category fires on
    /// every split by construction. It is still checked and reported after every step.
    fn fatal(self) -> bool {
        !matches!(self, Category::ReferenceBounds)
    }
}

/// One violation, tagged with its category.
#[derive(Debug, Clone)]
struct Violation {
    category: Category,
    detail: String,
}

fn format_violations(violations: &[Violation]) -> String {
    violations
        .iter()
        .map(|violation| format!("  [{}] {}", violation.category.name(), violation.detail))
        .collect::<Vec<_>>()
        .join("\n")
}

/// Renders an `event id -> copies` multiset as `{e0, e3x2}`.
fn render_multiset(counts: &BTreeMap<i64, u64>) -> String {
    if counts.is_empty() {
        return "{}".to_string();
    }
    format!(
        "{{{}}}",
        counts
            .iter()
            .map(|(event_id, copies)| if *copies == 1 {
                format!("e{event_id}")
            } else {
                format!("e{event_id}x{copies}")
            })
            .collect::<Vec<_>>()
            .join(", ")
    )
}

/// One region's predicted-vs-observed visibility at one step of a case.
struct RegionStep {
    region: RegionId,
    domain: Domain,
    /// The engine rule of this branch evaluated on the specification
    /// ([`RefModel::predict_visible_rows`]).
    predicted: BTreeMap<i64, u64>,
    /// A fresh scan of the same region, as `event id -> copies`.
    observed: BTreeMap<i64, u64>,
    /// The keys the specification authorizes, which is tighter than the engine's "a reference
    /// authorizes the whole file".
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
    /// The specification: what every published region serves and what its references
    /// authorize. Advanced by the transitions only, never by an observation.
    spec: RefModel,
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
    /// The highest committed and last-allocated sequences observed per region (sequence
    /// monotonicity). The two are tracked apart: a region that inherits files without
    /// committing writes legitimately starts with `last_sequence` above `committed_sequence`.
    seq_high_water: BTreeMap<RegionId, SequenceHighWater>,
    /// The live-holder set of every object at the previous step.
    prev_objects: BTreeMap<(RegionId, FileId), BTreeSet<RegionId>>,
    /// The step at which an object stopped being referenced by a live holder, with the
    /// holder set before and after.
    dropped_at: BTreeMap<(RegionId, FileId), (usize, BTreeSet<RegionId>, BTreeSet<RegionId>)>,
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
            seq_high_water: BTreeMap::new(),
            prev_objects: BTreeMap::new(),
            dropped_at: BTreeMap::new(),
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
                            creator_domain: meta
                                .partition_expr
                                .as_ref()
                                .map(|expr| {
                                    decode_domain(expr).unwrap_or_else(|| {
                                        panic!("harness cannot decode a file's creator domain")
                                    })
                                })
                                .unwrap_or(Domain::FULL),
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

    /// The object-store path of one physical object, resolved through its creator region.
    fn sst_path(&self, creator: RegionId, file_id: FileId) -> String {
        location::sst_file_path(
            &self.table_dir,
            RegionFileId::new(creator, file_id),
            self.path_type,
        )
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

    /// Checks every invariant category and returns the violations in category order.
    ///
    /// This never relaxes the oracle above: [`StorageModel::check`] keeps its meaning, and
    /// the categories below are additional statements about the same step.
    async fn check_invariants(&mut self, ctx: &str) -> Vec<Violation> {
        self.refresh_views().await;

        let mut violations = Vec::new();
        self.check_manifest_consistency(ctx, &mut violations);
        self.check_routing_applied(ctx, &mut violations);
        self.check_model_authorization(ctx, &mut violations).await;
        self.check_reference_bounds(ctx, &mut violations);
        self.check_sequence_monotonicity(ctx, &mut violations);
        self.check_physical_liveness(ctx, &mut violations).await;
        violations
    }

    /// The model-side assertion, separate from the ledger oracle.
    ///
    /// After every published step each region may only show rows one of its own references
    /// authorizes, plus the rows it wrote and has not flushed yet. Selecting a file does not
    /// authorize reading all of it: a split hands both halves the same object but each half
    /// is authorized for its own rows only, and a region compaction of one half produces a
    /// new object whose authorization is `domain ∩ authorized`, not the whole file.
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
                     no reference authorizes, keys {} (e.g. {examples:?}); the references \
                     authorize {}; specification state: {state_desc}",
                    key_name(domain.lo),
                    key_name(domain.hi),
                    describe_keys(&keys_outside),
                    describe_keys(&authorized)
                ),
            });
        }
    }

    /// Asserts the model-side authorization statement on its own.
    ///
    /// The oracle is not consulted: this pins that the visible rows of every published
    /// region are inside the union of that region's references' authorized sets.
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
                            "{:?}(creator [{} ({}, {})], L{}, rows={}, sequence={:?})",
                            sst.file_id,
                            key_name(sst.creator_domain.lo),
                            key_name(sst.creator_domain.hi),
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

    /// Records one step of a case: prints the prediction next to the observation and the
    /// judgement, keeps the rendered text in `report`, and returns the whole step so the case
    /// can assert on it.
    async fn record_step(&mut self, case: &str, step: &str, report: &mut Vec<String>) -> CaseStep {
        self.step += 1;
        let observed = self.observe(case, step).await;
        let text = observed.render();
        // Printed, not swallowed: a divergence and every judgement are the product of a case.
        println!("{text}");
        common_telemetry::info!("[mbt-case] {text}");
        report.push(text);
        observed
    }

    /// Manifest consistency: a holder's persisted manifest file set and its live version
    /// file set must describe the same references.
    fn check_manifest_consistency(&self, ctx: &str, violations: &mut Vec<Violation>) {
        for (region_id, view) in &self.holders {
            // The level a reference declares must be the level slot the version keeps it in.
            let version: BTreeMap<FileId, (u8, Option<u64>, u64)> = view
                .files
                .iter()
                .map(|(file_id, sst)| {
                    let slot_level = if sst.slot_level as usize == SHARED_LEVEL_SLOT {
                        LEVEL_SHARED
                    } else {
                        sst.slot_level
                    };
                    (*file_id, (slot_level, sst.sequence, sst.num_rows))
                })
                .collect();
            let manifest: BTreeMap<FileId, (u8, Option<u64>, u64)> = view
                .manifest
                .iter()
                .map(|(file_id, reference)| {
                    (
                        *file_id,
                        (reference.level, reference.sequence, reference.num_rows),
                    )
                })
                .collect();
            if version == manifest {
                continue;
            }
            let only_version: Vec<FileId> = version
                .keys()
                .filter(|file_id| !manifest.contains_key(file_id))
                .copied()
                .collect();
            let only_manifest: Vec<FileId> = manifest
                .keys()
                .filter(|file_id| !version.contains_key(file_id))
                .copied()
                .collect();
            let differing: Vec<String> = version
                .iter()
                .filter_map(|(file_id, live)| {
                    manifest.get(file_id).and_then(|persisted| {
                        (live != persisted).then(|| {
                            format!(
                                "{file_id:?} version (L{}, sequence {:?}, rows {}) vs manifest \
                                 (L{}, sequence {:?}, rows {})",
                                live.0, live.1, live.2, persisted.0, persisted.1, persisted.2
                            )
                        })
                    })
                })
                .collect();
            violations.push(Violation {
                category: Category::ManifestConsistency,
                detail: format!(
                    "[{ctx}] region {region_id}: the live version and the persisted manifest \
                     disagree; only in version: {only_version:?}; only in manifest: \
                     {only_manifest:?}; differing: {differing:?}"
                ),
            });
        }
    }

    /// Manifest consistency, routing half: the published routing must equal the regions that
    /// actually applied it, and a retired region must no longer accept writes.
    fn check_routing_applied(&self, ctx: &str, violations: &mut Vec<Violation>) {
        for (region_id, domain) in &self.routing {
            let Some(state) = self.applied.get(region_id) else {
                violations.push(Violation {
                    category: Category::ManifestConsistency,
                    detail: format!(
                        "[{ctx}] published region {region_id} is not a region the harness \
                         created"
                    ),
                });
                continue;
            };
            if state.domain != *domain {
                violations.push(Violation {
                    category: Category::ManifestConsistency,
                    detail: format!(
                        "[{ctx}] region {region_id} is published as [{}, {}) but applied [{}, \
                         {})",
                        key_name(domain.lo),
                        key_name(domain.hi),
                        key_name(state.domain.lo),
                        key_name(state.domain.hi)
                    ),
                });
            }
            if !state.writable {
                violations.push(Violation {
                    category: Category::ManifestConsistency,
                    detail: format!(
                        "[{ctx}] region {region_id} is published for [{}, {}) but does not \
                         accept writes, so no region serves that domain",
                        key_name(domain.lo),
                        key_name(domain.hi)
                    ),
                });
            }
        }
        for (region_id, state) in &self.applied {
            if self.routing.contains_key(region_id) || !state.writable {
                continue;
            }
            violations.push(Violation {
                category: Category::ManifestConsistency,
                detail: format!(
                    "[{ctx}] region {region_id} is no longer published but still accepts \
                     writes for [{}, {})",
                    key_name(state.domain.lo),
                    key_name(state.domain.hi)
                ),
            });
        }
    }

    /// Reference bounds: a reference must not authorize rows outside its own region's
    /// domain.
    ///
    /// This is the *observed* approximation of the model-side statement
    /// ([`StorageModel::check_model_authorization`]): the engine exposes no per-reference row
    /// authorization, so all it can report is the creator's domain, which a repartition
    /// hands to every target region unchanged. See the `FileMeta::owned_range` TODO on
    /// [`SstRef::creator_domain`] for the field that would let this check speak about rows
    /// instead of domains.
    fn check_reference_bounds(&self, ctx: &str, violations: &mut Vec<Violation>) {
        for region_id in self.live_regions() {
            let holder_domain = self.domain_of(region_id);
            let Some(view) = self.holders.get(&region_id) else {
                continue;
            };
            for (file_id, sst) in &view.files {
                if holder_domain.contains_domain(&sst.creator_domain) {
                    continue;
                }
                let holders = self
                    .physical
                    .holders_of
                    .get(file_id)
                    .map(|holders| holders.iter().copied().collect::<Vec<_>>())
                    .unwrap_or_default();
                violations.push(Violation {
                    category: Category::ReferenceBounds,
                    detail: format!(
                        "[{ctx}] region {region_id} (domain [{}, {})) references {file_id:?} \
                         whose creator domain is [{}, {}) (creator {}, L{}, sequence {:?}); the \
                         reference authorizes rows outside the holder's domain; holders: \
                         {holders:?}",
                        key_name(holder_domain.lo),
                        key_name(holder_domain.hi),
                        key_name(sst.creator_domain.lo),
                        key_name(sst.creator_domain.hi),
                        sst.owner,
                        sst.level,
                        sst.sequence
                    ),
                });
            }
        }
    }

    /// Sequence monotonicity: per region the committed sequence never decreases across
    /// steps, the allocated and persisted sequences never decrease either, and the allocated
    /// sequence stays at or above the committed one.
    fn check_sequence_monotonicity(&mut self, ctx: &str, violations: &mut Vec<Violation>) {
        for (region_id, state) in &self.applied {
            let high = self
                .seq_high_water
                .get(region_id)
                .copied()
                .unwrap_or_default();
            if state.committed < high.committed {
                violations.push(Violation {
                    category: Category::SequenceMonotonicity,
                    detail: format!(
                        "[{ctx}] region {region_id}: committed sequence went back from {} to {}",
                        high.committed, state.committed
                    ),
                });
            }
            if state.last_sequence < high.last {
                violations.push(Violation {
                    category: Category::SequenceMonotonicity,
                    detail: format!(
                        "[{ctx}] region {region_id}: last allocated sequence went back from {} \
                         to {}",
                        high.last, state.last_sequence
                    ),
                });
            }
            if state.last_sequence < state.committed {
                violations.push(Violation {
                    category: Category::SequenceMonotonicity,
                    detail: format!(
                        "[{ctx}] region {region_id}: last allocated sequence {} is below the \
                         committed sequence {}",
                        state.last_sequence, state.committed
                    ),
                });
            }
            match state.manifest_committed {
                Some(persisted) if persisted < high.persisted => violations.push(Violation {
                    category: Category::SequenceMonotonicity,
                    detail: format!(
                        "[{ctx}] region {region_id}: the persisted committed sequence went back \
                         from {} to {persisted}",
                        high.persisted
                    ),
                }),
                _ => {}
            }
        }
        // Only a decrease from the highest sequence already observed is a violation, so the
        // high-water marks advance after the checks above.
        for (region_id, state) in &self.applied {
            let high = self.seq_high_water.entry(*region_id).or_default();
            high.committed = high.committed.max(state.committed);
            high.last = high.last.max(state.last_sequence);
            high.persisted = high
                .persisted
                .max(state.manifest_committed.unwrap_or_default());
        }
    }

    /// Physical liveness: every object still referenced by a live holder must stay readable
    /// for that holder.
    async fn check_physical_liveness(&mut self, ctx: &str, violations: &mut Vec<Violation>) {
        let mut objects: BTreeMap<(RegionId, FileId), BTreeSet<RegionId>> = BTreeMap::new();
        for region_id in self.live_regions() {
            let Some(view) = self.holders.get(&region_id) else {
                continue;
            };
            for (file_id, sst) in &view.files {
                objects
                    .entry((sst.owner, *file_id))
                    .or_default()
                    .insert(region_id);
            }
        }

        // Records the handover step first, so a failed read can name who dropped the object.
        for (object, holders) in &objects {
            if let Some(previous) = self.prev_objects.get(object)
                && previous != holders
                && !previous.is_subset(holders)
            {
                self.dropped_at
                    .insert(*object, (self.step, previous.clone(), holders.clone()));
            }
        }
        // An object that lost every live holder keeps the step that dropped it as well.
        for (object, previous) in &self.prev_objects {
            if objects.contains_key(object) {
                continue;
            }
            self.dropped_at
                .insert(*object, (self.step, previous.clone(), BTreeSet::new()));
        }

        for (object, holders) in &objects {
            let (creator, file_id) = *object;
            let path = self.sst_path(creator, file_id);
            let Err(reason) = self.read_object(&path).await else {
                continue;
            };
            let drop = match self.dropped_at.get(object) {
                Some((step, before, after)) => format!(
                    "dropped by {} at step {step} (holders before {before:?}, after {after:?})",
                    before
                        .difference(after)
                        .map(|region_id| region_id.to_string())
                        .collect::<Vec<_>>()
                        .join(", ")
                ),
                None => "never dropped by a live holder".to_string(),
            };
            violations.push(Violation {
                category: Category::PhysicalLiveness,
                detail: format!(
                    "[{ctx}] object {file_id:?} (creator {creator}) at {path} is not readable \
                     although it is still referenced by {holders:?}: {reason}; {drop}"
                ),
            });
        }

        self.prev_objects = objects;
    }

    /// Reads one physical object, i.e. what a holder of that reference needs to scan it.
    ///
    /// Reading the bytes (not just stat-ing them) is what makes this a liveness check: a
    /// deleted, truncated or foreign object fails here even though a stale metadata entry
    /// still lists it.
    async fn read_object(&self, path: &str) -> Result<u64, String> {
        let bytes = self
            .store
            .read(path)
            .await
            .map_err(|err| format!("object-store read failed: {err}"))?
            .to_vec();
        if bytes.len() < 8 || &bytes[..4] != b"PAR1" || &bytes[bytes.len() - 4..] != b"PAR1" {
            return Err(format!(
                "{} bytes that are not a complete Parquet object",
                bytes.len()
            ));
        }
        Ok(bytes.len() as u64)
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

    /// Appends rows to **`region_id`**, bypassing the routing.
    ///
    /// Write admission checks the partition *version* the region serves, not the row's
    /// partition value, so a row whose key lies outside the region's domain is admitted and
    /// lands in that region's memtable. The out-of-domain case uses this to hand the engine a
    /// row it has to place by rule instead of by construction.
    async fn write_to(&mut self, region_id: RegionId, rows: &[PlannedRow]) -> Vec<Event> {
        let pending: Vec<(i64, PlannedRow)> = rows
            .iter()
            .map(|planned| (self.ledger.reserve(), *planned))
            .collect();
        let values: Vec<api::v1::Row> = pending
            .iter()
            .map(|(event_id, planned)| event_row(*event_id, *planned))
            .collect();
        put_rows(
            &self.engine,
            region_id,
            Rows {
                schema: self.schema.clone(),
                rows: values,
            },
        )
        .await;
        let mut committed = Vec::new();
        for (event_id, planned) in pending {
            committed.push(self.ledger.commit(event_id, planned));
            self.spec.note_write(region_id, event_id, planned.key);
        }
        self.dirty.insert(region_id);
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

        self.tracked.insert(left);
        self.tracked.insert(right);
        self.routing.remove(&source);
        self.routing.insert(left, left_domain);
        self.routing.insert(right, right_domain);
        self.spec
            .split(source, left, left_domain, right, right_domain);
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
        let merged = self.publish_merge(&inputs).await;
        self.spec.merge(&inputs, merged);
        merged
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
        // Printed, not only recorded: the file inventory a close leaves behind is the evidence
        // of what the reopen has to reload, so it must be visible when the case passes.
        println!("{line}");
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
        // Printed for the same reason: the role the engine assigns on open is what bounds what
        // a chaos case may do next, so it is reported instead of assumed.
        println!("{line}");
        common_telemetry::info!("[mbt-case] {line}");
        line
    }

    /// The reference state the engine exposes for `region_id`: every file id with its creator
    /// domain, level, row count and admission sequence.
    ///
    /// The authorized range a reference exposes on this branch is its creator domain:
    /// `FileMeta::owned_range` does not exist, so there is no narrower range to report.
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
                    "{:?}(creator {}, authorized range = creator domain [{}, {}) on this branch, \
                     L{}, rows={}, sequence={:?})",
                    sst.file_id,
                    sst.owner,
                    key_name(sst.creator_domain.lo),
                    key_name(sst.creator_domain.hi),
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
/// Every step is a published step, so both the ledger oracle and the model-side
/// authorization assertion run on the flush state, on the split state (one file, two
/// holders, two disjoint authorizations) and on the one-side-compacted state.
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
                    shapes: model.spec.shapes.iter().copied().collect(),
                }),
                observations,
                shapes: model.spec.shapes.iter().copied().collect(),
            };
        }
    }
    TraceReport {
        failure: None,
        observations,
        shapes: model.spec.shapes.iter().copied().collect(),
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
/// materializes `F` under a new local file id while `P` still holds `F`; the two halves are
/// merged into one survivor whose domain is the whole table.
///
/// The specification names the same trace in its own vocabulary and is asserted at every
/// step: after the flush `f0` authorizes `{k01, k09}`; after the split both halves hold `f0`
/// but `P` authorizes only `{k01}` and `R` only `{k09}`; after compacting `R`, `R` holds a
/// new file `f1` authorized `{k09}` while `P` still holds `f0`; after the merge the survivor
/// holds `f0` authorized `{k01}` and `f1` authorized `{k09}`, and its rows stay inside that
/// union. Those per-region statements are what "selecting a file does not authorize reading
/// all of it" means, and they are why the ledger oracle below is the independent judge.
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
    // The specification: one model file authorized for exactly the two keys it holds.
    let spec_shared = model.spec.region(source).expect("the source is published");
    assert_eq!(Domain::FULL, spec_shared.domain);
    assert_eq!(
        vec![Some(BTreeSet::from([1, 9]))],
        spec_shared.refs.values().cloned().collect::<Vec<_>>(),
        "the flush must mint one model file authorized for its own keys: {:?}",
        model.spec.describe(source)
    );
    let spec_file = *spec_shared.refs.keys().next().unwrap();
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
    // The specification: both halves keep the *same* model file, but each is authorized for
    // its own rows only — the shape that used to be unrepresentable.
    let spec_left = model.spec.region(left).expect("the left half is published");
    let spec_right = model
        .spec
        .region(right)
        .expect("the right half is published");
    assert_eq!(
        BTreeMap::from([(spec_file, Some(BTreeSet::from([1])))]),
        spec_left.refs,
        "the left half must authorize only the rows of its own domain"
    );
    assert_eq!(
        BTreeMap::from([(spec_file, Some(BTreeSet::from([9])))]),
        spec_right.refs,
        "the right half must authorize only the rows of its own domain"
    );
    assert_eq!(
        BTreeSet::from([left, right]),
        model.spec.holders_of(spec_file),
        "a legal split hands the same physical file to both halves"
    );
    assert!(
        model.spec.shapes.contains(SHAPE_SPLIT_SHARED),
        "the model must record the split-shared-file shape: {:?}",
        model.spec.shapes
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
    let holders_of_shared = model.physical.holders_of[&shared].clone();
    assert!(
        holders_of_shared.len() >= 2,
        "the shared object is still referenced by more than one holder: {holders_of_shared:?}"
    );
    model
        .assert_model_authorization("after compacting only the right half")
        .await;
    // The specification: the compacted half now holds a *new* model file carrying exactly
    // the rows it owns, and the consumed reference left that half only.
    let spec_left = model.spec.region(left).expect("the left half is published");
    let spec_right = model
        .spec
        .region(right)
        .expect("the right half is published");
    assert_eq!(
        BTreeMap::from([(spec_file, Some(BTreeSet::from([1])))]),
        spec_left.refs,
        "the un-compacted half must keep the original file, still restricted to its rows"
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
        BTreeMap::from([(spec_copy, Some(BTreeSet::from([9])))]),
        spec_right.refs,
        "the compaction output authorizes the rows the region owns, not the whole file"
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
        model.spec.shapes.contains(SHAPE_ONE_SIDE_COMPACTION),
        "the model must record the one-side compaction shape: {:?}",
        model.spec.shapes
    );

    // ---- Step 3: merge both halves back into one whole-domain survivor. ----
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
    // The specification: the survivor holds the original file (authorized only for the rows
    // the remaining holder authorized, because the compacted half's reference left it) and
    // the compaction's output (authorized for the rows that half owns). The original object
    // physically carries *both* rows, so the survivor reads a row the reference is not
    // authorized for and the union covers it — authorization is not counting, which is why
    // the oracle below is the judge of "exactly once".
    let spec_merged = model
        .spec
        .region(merged)
        .expect("the survivor is published");
    assert_eq!(
        BTreeMap::from([
            (spec_file, Some(BTreeSet::from([1]))),
            (spec_copy, Some(BTreeSet::from([9]))),
        ]),
        spec_merged.refs,
        "the merge must combine the references per physical file: {:?}",
        model.spec.describe(merged)
    );
    assert_eq!(
        BTreeSet::from([1, 9]),
        model.spec.authorized_keys(merged),
        "together the references authorize every key the survivor serves"
    );
    assert!(
        model.spec.shapes.contains(SHAPE_MERGE_RECOMBINES),
        "the model must record the recombining merge: {:?}",
        model.spec.shapes
    );
    // The prediction of this branch's engine rule next to the observation: the survivor reads
    // the original file without its partition predicate (its creator expression is the whole
    // domain) *and* the compaction's copy, so the row the right half owned is predicted twice.
    // Printed, never asserted here: the ledger oracle below is the judge of "exactly once".
    common_telemetry::info!(
        "[mbt-dup-row-mvp] predicted visibility of the survivor: {:?}",
        model.spec.predict_counts(merged)
    );
    assert!(
        violation.is_none(),
        "every committed event must stay visible exactly once after the merge, but the merge \
         keeps both the shared SST and the single-side compaction's copy.\n{}\ninvariant \
         observations over this fixed trace:\n{}",
        violation.unwrap_or_default(),
        format_violations(&observed)
    );
}

/// Case 1: a row whose partition key lies outside the target region's domain.
///
/// Write admission checks the partition *version* a region serves, never the value of the row,
/// so `k07` is admitted into the memtable of a region whose domain is `[k00, k05)`. The
/// prediction then follows the mirror of this branch's read path: the flush's file is created
/// by that region, so the region's expression equals the file's creator expression and the scan
/// skips the partition predicate, showing the out-of-domain row. The split narrows both halves
/// to their own domains, so the row becomes invisible in every region (the ledger oracle
/// reports it missing), and the merge into a whole-domain survivor brings it back together with
/// a second copy of the row the one-side compaction already published.
#[tokio::test]
async fn test_mbt_case_out_of_domain_row() {
    common_telemetry::init_default_ut_logging();
    const CASE: &str = "out-of-domain row";
    let mut model = StorageModel::new("mbt-case-out-of-domain").await;
    let mut report = Vec::new();
    let mut agreed = true;

    let source = model.live_regions()[0];
    let (left, right) = model.split(source, 5).await;
    let step = model
        .record_step(CASE, "after the split", &mut report)
        .await;
    agreed &= step.mismatched().is_empty();

    let in_domain = model
        .write_to(
            left,
            &[
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
            ],
        )
        .await;
    // `k07` belongs to `right`, and is written into `left` anyway.
    let out_of_domain = model
        .write_to(
            left,
            &[PlannedRow {
                key: 7,
                ts: 2000,
                payload: 7.0,
            }],
        )
        .await;
    let out_id = out_of_domain[0].event_id;
    let in_ids: Vec<i64> = in_domain.iter().map(|event| event.event_id).collect();
    let step = model
        .record_step(CASE, "after writing the out-of-domain row", &mut report)
        .await;
    agreed &= step.mismatched().is_empty();
    assert_eq!(
        BTreeMap::new(),
        model
            .spec
            .predict_counts(left)
            .into_iter()
            .filter(|(event_id, _)| *event_id == out_id)
            .collect::<BTreeMap<_, _>>(),
        "an admitted row is in the memtable, and the memtable scan is filtered by the domain: \
         the out-of-domain row is predicted invisible before the flush: {}",
        model.spec.describe(left)
    );
    assert_eq!(
        BTreeMap::from([(in_ids[0], 1), (in_ids[1], 1)]),
        model
            .scan_counts(left)
            .await
            .into_iter()
            .filter(|(event_id, _)| in_ids.contains(event_id))
            .map(|(event_id, copies)| (event_id, copies as u64))
            .collect(),
        "the two in-domain rows must be visible from the memtable"
    );

    model.flush_region(left).await;
    let step = model
        .record_step(CASE, "after flushing the left half", &mut report)
        .await;
    agreed &= step.mismatched().is_empty();
    // The flush's file is created by `left`, so the region's expression equals the file's
    // creator expression and the reader drops the partition predicate: all three rows show.
    assert_eq!(
        BTreeMap::from([(in_ids[0], 1), (in_ids[1], 1), (out_id, 1)]),
        model.spec.predict_counts(left),
        "the file is created by the region itself, so every physical row of it is predicted \
         visible to that region: {}",
        model.spec.describe(left)
    );
    assert_eq!(
        model.spec.predict_counts(left),
        model
            .scan_counts(left)
            .await
            .into_iter()
            .map(|(event_id, copies)| (event_id, copies as u64))
            .collect::<BTreeMap<_, _>>(),
        "the prediction must agree with the flush's observation"
    );

    let (narrow, rest) = model.split(left, 2).await;
    let step = model
        .record_step(CASE, "after the split of the left half", &mut report)
        .await;
    agreed &= step.mismatched().is_empty();
    assert_eq!(
        BTreeMap::from([(in_ids[0], 1)]),
        model.spec.predict_counts(narrow),
        "the file's creator expression is [k00, k05) and the target's is [k00, k02), so the \
         partition predicate applies and only the in-domain row survives"
    );
    assert_eq!(
        BTreeMap::from([(in_ids[1], 1)]),
        model.spec.predict_counts(rest)
    );
    assert!(
        step.oracle_fired(),
        "the ledger oracle must report the out-of-domain row the split made invisible"
    );

    model.compact(narrow).await;
    let step = model
        .record_step(CASE, "after compacting the narrow half", &mut report)
        .await;
    agreed &= step.mismatched().is_empty();

    let merged = model.merge_all().await;
    let step = model
        .record_step(CASE, "after merging all halves", &mut report)
        .await;
    agreed &= step.mismatched().is_empty();
    assert_eq!(
        BTreeMap::from([(in_ids[0], 2), (in_ids[1], 1), (out_id, 1)]),
        model.spec.predict_counts(merged),
        "the whole-domain survivor applies no target domain, so the shared file (creator \
         [k00, k05)) shows all of its rows while the compaction's copy still holds one of them"
    );
    assert!(
        step.oracle_fired(),
        "the ledger oracle must report the row the one-side compaction duplicated"
    );
    assert!(
        agreed,
        "the prediction must agree with the engine in every step of this case:\n\n{}",
        report.join("\n\n")
    );
    let _ = (right, rest);
}

/// Case 2: two hops of splits, the second of which lands a target exactly on a file's creator
/// expression.
///
/// `A -> B1/B2` hands the whole-domain file to both halves; compacting `B2` materializes a file
/// *created by* `B2`, so a later split of the merge survivor at the same cut produces a target
/// whose domain equals that file's creator expression — the one case in which the reader skips
/// the partition predicate. That target then shows both the shared file's rows and the copy's
/// rows, which the ledger oracle reports as duplicates.
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
        "the shared file's creator expression is the whole domain, so the left half applies its \
         own partition predicate"
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

    // Second hop: split the survivor at the same cut. `C2` lands exactly on the creator
    // expression of the file the compaction materialized.
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
        BTreeMap::from([(ids[2], 2), (ids[3], 2)]),
        model.spec.predict_counts(c2),
        "C2's domain equals the creator expression of the compaction's file, so the reader \
         skips the partition predicate for it while the shared file still shows its rows too"
    );
    assert!(
        step.oracle_fired(),
        "the ledger oracle must report the duplicated rows of the second split"
    );
    assert!(
        agreed,
        "the prediction must agree with the engine in every step of this case:\n\n{}",
        report.join("\n\n")
    );
}

/// Case 3: copies across three generations, `F0 -> F1 -> F2`.
///
/// The whole-domain flush publishes `F0`; compacting one half materializes `F1` (created by
/// that half); merging both halves and compacting the survivor materializes `F2` (created by
/// the survivor), which physically holds *two* copies of the row both `F0` and `F1` carried.
/// Every scan of `F2` skips the partition predicate, because its creator expression is the
/// survivor's own.
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
        BTreeMap::from([(ids[0], 2), (ids[1], 1)]),
        model.spec.predict_counts(survivor),
        "F2 is created by the survivor, which holds both the shared file and the copy: the row \
         both of them carry is physically in F2 twice"
    );

    // Generation 3: split the survivor, then merge both halves again.
    let (p, q) = model.split(survivor, 5).await;
    let step = model
        .record_step(CASE, "after splitting F2", &mut report)
        .await;
    agreed &= step.mismatched().is_empty();
    assert_eq!(BTreeMap::from([(ids[0], 2)]), model.spec.predict_counts(p));
    assert_eq!(BTreeMap::from([(ids[1], 1)]), model.spec.predict_counts(q));

    let rejoined = model.merge_all().await;
    let step = model
        .record_step(CASE, "after merging both halves again", &mut report)
        .await;
    agreed &= step.mismatched().is_empty();
    assert_eq!(
        BTreeMap::from([(ids[0], 2), (ids[1], 1)]),
        model.spec.predict_counts(rejoined),
        "the whole-domain survivor reads F2 without a partition predicate, so both physical \
         copies of the shared row are visible"
    );
    assert!(
        step.oracle_fired(),
        "the ledger oracle must report the row the generations duplicated"
    );
    assert!(
        agreed,
        "the prediction must agree with the engine in every step of this case:\n\n{}",
        report.join("\n\n")
    );
    let _ = right;
}

/// Case 4: the same merge twice, and a merge followed immediately by a split at the same cut.
///
/// The first merge publishes the survivor; the retry re-runs the same remap over the same
/// inputs, so it must republish the same file ids and add no third copy. Splitting right after
/// the merge at the cut the shape started from gives a target whose domain equals the creator
/// expression of the compaction's file again.
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
        BTreeMap::from([(ids[0], 2), (ids[1], 1)]),
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
        BTreeMap::from([(ids[0], 2), (ids[1], 1)]),
        model.spec.predict_counts(retried),
        "a retry republishes the reference set the first attempt published, so it adds no third \
         copy of the row the first merge already duplicated"
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
        BTreeMap::from([(ids[0], 2)]),
        model.spec.predict_counts(again_left),
        "the compaction's file was created by the left half, so the left target skips the \
         partition predicate for it and shows both physical copies"
    );
    assert_eq!(
        BTreeMap::from([(ids[1], 1)]),
        model.spec.predict_counts(again_right)
    );
    assert!(
        step.oracle_fired(),
        "the ledger oracle must report the duplicated row"
    );
    assert!(
        agreed,
        "the prediction must agree with the engine in every step of this case:\n\n{}",
        report.join("\n\n")
    );
}

/// Case 5: a split target that receives no rows at all.
///
/// The engine still hands the shared file to that half (repartition copies every file of the
/// source into every target), so the observed invariant category reports the reference, while
/// the specification's authorization view drops it. The prediction is empty either way, and it
/// stays empty when the empty half compacts the file it cannot read.
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
    assert!(
        model
            .spec
            .region(empty)
            .expect("the empty half is published")
            .refs
            .is_empty(),
        "the specification authorizes no reference for it: {}",
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

/// Case 6: two append-mode events with the identical `(key, ts)` inside one file.
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
        BTreeMap::from([(a, 2), (b, 2)]),
        model.spec.predict_counts(merged),
        "the merge keeps the shared file and the compaction's copy, so each of the two events \
         is visible twice - but never collapsed into one row"
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
        BTreeMap::from([(ids[0], 2), (ids[1], 1)]),
        model.spec.predict_counts(merged),
        "the reopen changed neither the reference set nor the prediction"
    );
    assert!(
        step.oracle_fired(),
        "the ledger oracle must report the duplicated row of the one-side compaction"
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
        BTreeMap::from([(ids[0], 2), (late[0], 1), (ids[1], 1), (late[1], 1)]),
        model.spec.predict_counts(merged),
        "the survivor holds the shared file, the right half's interleaved flush and the \
         compaction's copy"
    );
    assert!(
        step.oracle_fired(),
        "the ledger oracle must report the duplicated row"
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
        BTreeMap::from([(ids[0], 2), (ids[1], 1)]),
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
        BTreeMap::from([(ids[0], 2), (ids[1], 1)]),
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
/// model-side authorization assertion and the four observed invariant categories run after
/// every published step. Every plan ends with [`shared_file_shape`], so each seed is driven
/// through a split whose halves both keep the shared file, a compaction of exactly one half
/// and the merge of both. On a fatal failure the trace is shrunk to the shortest failing
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
