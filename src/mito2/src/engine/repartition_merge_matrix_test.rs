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

//! Merge-focused combination matrix for the repartition (split/merge) path.
//!
//! # Why merge
//!
//! A repartition hands one SST `file_id` to several live regions. A merge is the only
//! operation that combines references from several regions back into one, so duplicates,
//! lost rows and range widening all surface there: it dedups by `file_id`, and a holder that
//! materialized the shared object into a new `file_id` (region compaction) survives the merge
//! as a second copy of the same rows.
//!
//! [`repartition_mbt_test`](super::repartition_mbt_test) pins the class with random traces.
//! This module pins a *fixed table* of merge shapes: every cell is enumerated up front, runs
//! on its own engine, and is reported by name, step, invariant category, diff and shrunk
//! prefix. A failing cell does not stop the table; all cells run and every failure is
//! collected.
//!
//! # Axes
//!
//! | axis | values |
//! | --- | --- |
//! | generations | 1, 2, 3 rounds of split -> merge |
//! | compaction placement | none, one side, both sides, before split, between merges, after the final merge |
//! | fan-out / fan-in | split into 2, 3, 4 targets; merge all at once, or merge a subset then the rest |
//! | merge target identity | fresh region id, the original (pre-split) region id, one of the surviving halves, an id that already holds the same file |
//! | domain relation (target vs file creator) | disjoint halves, one half empty of rows, nested superset, nested subset, equal |
//! | merge input order | A then B, B then A |
//! | row distribution | left half only, right half only, both, same key different timestamp, same key and timestamp (append mode) |
//! | interleaving | write+flush between split and compaction, and between compaction and merge |
//!
//! The table crosses the merge-relevant subset exhaustively instead of the full product: see
//! [`table`] for the enumerated cells and [`table_summary`] for their axis counts.
//!
//! # Oracle
//!
//! After every published step the harness scans the published routing and asserts
//!
//! 1. `visibility`: the multiset of visible `event_id`s over the live regions equals the
//!    ledger exactly once (no duplicate, no loss), and no event is visible in a region whose
//!    published domain does not own it;
//! 2. `manifest-consistency`: a region's persisted manifest file set equals its live version
//!    file set (id, level, admission sequence, row count) and the published routing equals the
//!    regions that actually applied it;
//! 3. `reference-bounds`: every reference a live holder shows stays inside that holder's domain.
//!    A reference carries no row range of its own on this branch, so the reference's range is its
//!    file's creator domain and one *legal* split already reports here (both halves reference an
//!    object the whole-domain source created). The category is therefore checked and reported
//!    after every step but is not fatal; see the `FileMeta::owned_range` TODO on
//!    [`SstRef::creator`] for the field that would narrow it;
//! 4. `sequence-monotonicity`: per region the committed sequence never decreases;
//! 5. `physical-liveness`: every object still referenced by a live holder still reads back as
//!    a complete Parquet object;
//! 6. `scenario-shape`: the cell realized the relation/identity it claims (a cell that silently
//!    degenerates is reported instead of passing).
//!
//! It deliberately does *not* use "two regions must not share a `file_id`" or single-manifest
//! `file_id` uniqueness as a criterion: a legal split hands one object to both halves, and a
//! broken merge that keeps two copies of one row passes both.
//!
//! # Range shape
//!
//! A reference's persisted range is the creator's `partition_expr` on this branch (there is no
//! per-reference `owned_range`), so a merge can union, narrow or widen nothing: the concern is
//! that the persisted expression stays bounded instead of nesting one `Or`/`And` level per
//! cycle. [`test_union_range_shape_bounded_growth`] measures the rendered expression depth over
//! five split -> merge cycles and checks that the persisted manifest still round-trips.

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
use store_api::region_engine::{RegionEngine, RemapManifestsRequest, SettableRegionRoleState};
use store_api::region_request::{
    ApplyStagingManifestRequest, EnterStagingRequest, PathType, RegionCompactRequest,
    RegionCreateRequest, RegionFlushRequest, RegionPutRequest, RegionRequest,
    StagingPartitionDirective,
};
use store_api::storage::{FileId, RegionId, ScanRequest};

use crate::config::MitoConfig;
use crate::engine::{MITO_ENGINE_NAME, MitoEngine};
use crate::gc::GcConfig;
use crate::manifest::action::RegionManifest;
use crate::region::{RegionLeaderState, RegionRoleState};
use crate::sst::file::{LEVEL_SHARED, RegionFileId, SHARED_LEVEL_SLOT};
use crate::sst::location;
use crate::test_util::{TestEnv, rows_schema};

/// Number of distinct partition-key values (`k00..k09`).
const KEY_COUNT: usize = 10;
/// Wide enough to cover every timestamp the cells write.
const COMPACTION_WINDOW_SECONDS: i64 = 3600;
/// How many violations of one cell the report prints before it summarizes the rest.
const MAX_REPORTED_FAILURES: usize = 3;
/// Number of split -> merge cycles of the range-shape test.
const GROWTH_CYCLES: usize = 5;
/// Depth bound of the persisted range after [`GROWTH_CYCLES`] cycles.
///
/// The reference keeps the creator's expression, so the expected depth is 1 in every cycle. The
/// bound allows one `And` (narrowing by a split target) and one `Or` (unioning two halves) per
/// cycle, which is what a per-reference range would add; exponential nesting would blow past it.
const MAX_RANGE_DEPTH: usize = 2 * GROWTH_CYCLES + 2;

/// Canonical name of the partition key value `idx`.
fn key_name(idx: usize) -> String {
    format!("k{idx:02}")
}

/// The key index encoded by a partition key value; `"k10"` (the upper bound of the full
/// domain) is index [`KEY_COUNT`].
fn key_index(name: &str) -> Option<usize> {
    name.strip_prefix('k')?.parse().ok()
}

/// A set of partition keys. Ranges are evaluated as key sets instead of being decoded back into
/// one `[lo, hi)` pair, because a merged reference authorizes a union of ranges.
type KeySet = BTreeSet<usize>;

fn all_keys() -> KeySet {
    (0..KEY_COUNT).collect()
}

/// Renders a key set as the interval list `{k00-k01, k05-k09}`.
fn keys_label(keys: &KeySet) -> String {
    let mut parts: Vec<String> = Vec::new();
    let mut run: Option<(usize, usize)> = None;
    for key in keys {
        run = match run {
            Some((lo, hi)) if hi + 1 == *key => Some((lo, *key)),
            Some((lo, hi)) => {
                parts.push(interval_label(lo, hi));
                Some((*key, *key))
            }
            None => Some((*key, *key)),
        };
    }
    if let Some((lo, hi)) = run {
        parts.push(interval_label(lo, hi));
    }
    format!("{{{}}}", parts.join(", "))
}

fn interval_label(lo: usize, hi: usize) -> String {
    if lo == hi {
        key_name(lo)
    } else {
        format!("{}-{}", key_name(lo), key_name(hi))
    }
}

/// A half-open key domain `[lo, hi)`: the shape every region of the matrix is created with.
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
    fn expr(&self) -> PartitionExpr {
        col("k")
            .gt_eq(Value::String(key_name(self.lo).into()))
            .and(col("k").lt(Value::String(key_name(self.hi).into())))
    }

    fn expr_json(&self) -> String {
        self.expr().as_json_str().unwrap()
    }

    fn keys(&self) -> KeySet {
        (self.lo..self.hi).collect()
    }

    fn contains_key(&self, key: usize) -> bool {
        self.lo <= key && key < self.hi
    }

    fn label(&self) -> String {
        format!("[{}, {})", key_name(self.lo), key_name(self.hi))
    }
}

/// One operand of the restricted partition grammar this matrix generates and evaluates.
enum Term {
    Column(String),
    Str(String),
    Keys(KeySet),
}

fn eval_operand(operand: &Operand) -> Option<Term> {
    match operand {
        Operand::Column(name) => Some(Term::Column(name.clone())),
        Operand::Value(Value::String(value)) => Some(Term::Str(value.as_utf8().to_string())),
        Operand::Expr(expr) => Some(Term::Keys(eval_keys(expr)?)),
        _ => None,
    }
}

/// Evaluates a partition expression over the harness' key space.
///
/// Returns `None` for every expression outside the generated grammar, so an unsupported shape is
/// reported instead of silently passing a boundary check.
fn eval_keys(expr: &PartitionExpr) -> Option<KeySet> {
    let lhs = eval_operand(&expr.lhs)?;
    let rhs = eval_operand(&expr.rhs)?;
    match &expr.op {
        RestrictedOp::And | RestrictedOp::Or => {
            let (Term::Keys(left), Term::Keys(right)) = (lhs, rhs) else {
                return None;
            };
            Some(if matches!(&expr.op, RestrictedOp::And) {
                left.intersection(&right).copied().collect()
            } else {
                left.union(&right).copied().collect()
            })
        }
        op => {
            let (name, value, reversed) = match (lhs, rhs) {
                (Term::Column(name), Term::Str(value)) => (name, value, false),
                (Term::Str(value), Term::Column(name)) => (name, value, true),
                _ => return None,
            };
            if name != "k" {
                return None;
            }
            let bound = key_index(&value)?;
            let op = match (reversed, op) {
                (false, op) => op,
                (true, RestrictedOp::Lt) => &RestrictedOp::Gt,
                (true, RestrictedOp::LtEq) => &RestrictedOp::GtEq,
                (true, RestrictedOp::Gt) => &RestrictedOp::Lt,
                (true, RestrictedOp::GtEq) => &RestrictedOp::LtEq,
                (true, op) => op,
            };
            let keys: KeySet = (0..KEY_COUNT)
                .filter(|key| match op {
                    RestrictedOp::GtEq => *key >= bound,
                    RestrictedOp::Gt => *key > bound,
                    RestrictedOp::Lt => *key < bound,
                    RestrictedOp::LtEq => *key <= bound,
                    RestrictedOp::Eq => *key == bound,
                    RestrictedOp::NotEq => *key != bound,
                    _ => false,
                })
                .collect();
            Some(keys)
        }
    }
}

/// The keys a reference authorizes: every physical row when the range is `None`.
fn range_keys(range: Option<&PartitionExpr>) -> KeySet {
    match range {
        None => all_keys(),
        Some(expr) => eval_keys(expr)
            .unwrap_or_else(|| panic!("harness cannot evaluate an authorized range as a key set")),
    }
}

/// Depth of the rendered expression tree.
fn expr_depth(expr: &PartitionExpr) -> usize {
    1 + operand_depth(&expr.lhs).max(operand_depth(&expr.rhs))
}

fn operand_depth(operand: &Operand) -> usize {
    match operand {
        Operand::Expr(expr) => expr_depth(expr),
        _ => 0,
    }
}

/// The relation between two key sets, as reported by the domain-relation axis.
fn relation_label(left: &KeySet, right: &KeySet) -> &'static str {
    if left == right {
        "equal"
    } else if left.is_superset(right) {
        "superset"
    } else if left.is_subset(right) {
        "subset"
    } else if left.is_disjoint(right) {
        "disjoint"
    } else {
        "overlapping"
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

/// A row the table plans to write; becomes an [`Event`] once committed.
#[derive(Debug, Clone, Copy)]
struct PlannedRow {
    key: usize,
    ts: i64,
    payload: f64,
}

fn planned(key: usize, ts: i64, payload: f64) -> PlannedRow {
    PlannedRow { key, ts, payload }
}

/// Independent model of all committed events.
#[derive(Debug, Default)]
struct Ledger {
    next_event_id: i64,
    events: BTreeMap<i64, Event>,
}

impl Ledger {
    fn reserve(&mut self) -> i64 {
        let event_id = self.next_event_id;
        self.next_event_id += 1;
        event_id
    }

    fn commit(&mut self, event_id: i64, planned: PlannedRow) {
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
    }

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
            None => format!("event_id {event_id} (not in the ledger)"),
        }
    }
}

/// One reference, as observed through a region's live version.
#[derive(Debug, Clone)]
struct SstRef {
    file_id: FileId,
    /// The region that physically wrote the object (not necessarily its holder).
    owner: RegionId,
    level: u8,
    slot_level: u8,
    num_rows: u64,
    sequence: Option<u64>,
    /// `FileMeta::partition_expr`: the domain of the region that created the object.
    ///
    /// This branch has no per-reference row range (`FileMeta::owned_range`), so this is also the
    /// range a reference authorizes: a reader bounds the object only by its holder's domain.
    creator: KeySet,
}

/// One region's persisted manifest view of a reference.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ManifestRef {
    level: u8,
    sequence: Option<u64>,
    num_rows: u64,
}

/// What one region actually applied, captured per step.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Applied {
    domain: Domain,
    committed: u64,
    last_sequence: u64,
    manifest_committed: Option<u64>,
    writable: bool,
}

/// The highest sequences observed per region, so a later step can only move forward.
#[derive(Debug, Clone, Copy, Default)]
struct HighWater {
    committed: u64,
    last: u64,
    persisted: u64,
}

/// One invariant category. Each is reported separately.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Category {
    Visibility,
    ManifestConsistency,
    ReferenceBounds,
    SequenceMonotonicity,
    PhysicalLiveness,
    ScenarioShape,
    Operation,
}

impl Category {
    fn name(self) -> &'static str {
        match self {
            Category::Visibility => "visibility",
            Category::ManifestConsistency => "manifest-consistency",
            Category::ReferenceBounds => "reference-bounds",
            Category::SequenceMonotonicity => "sequence-monotonicity",
            Category::PhysicalLiveness => "physical-liveness",
            Category::ScenarioShape => "scenario-shape",
            Category::Operation => "operation",
        }
    }

    /// `reference-bounds` is reported after every step but never fatal on this branch: a
    /// reference carries no row authorization of its own here, so one legal split already makes
    /// both halves reference an object whose creator domain is the whole table. See
    /// `SstRef::creator`.
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

/// One violation of one table cell.
#[derive(Debug, Clone)]
struct Failure {
    scenario: String,
    step: usize,
    step_description: String,
    category: Category,
    detail: String,
}

impl Failure {
    fn describe(&self) -> String {
        format!(
            "scenario {scenario}\n  step {step}: {desc}\n  [{category}] {detail}",
            scenario = self.scenario,
            step = self.step,
            desc = self.step_description,
            category = self.category.name(),
            detail = self.detail,
        )
    }
}

/// One reported-but-not-fatal observation of one table cell.
#[derive(Debug, Clone)]
struct Observation {
    scenario: String,
    step: usize,
    category: Category,
    detail: String,
}

// ---------------------------------------------------------------------------------------
// The axes of the matrix.
// ---------------------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Group {
    Generations,
    FanOut,
    Identity,
    Rows,
    Interleaving,
}

impl Group {
    fn name(self) -> &'static str {
        match self {
            Group::Generations => "generations-placement",
            Group::FanOut => "fan-out-fan-in",
            Group::Identity => "identity-order",
            Group::Rows => "rows-relation",
            Group::Interleaving => "interleaving",
        }
    }
}

/// Fan-out of one split: 2, 3 or 4 targets.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FanOut {
    Two,
    Three,
    Four,
}

impl FanOut {
    /// The domain cuts of the fan-out: one fewer cut than targets.
    fn cuts(self) -> Vec<usize> {
        match self {
            FanOut::Two => vec![5],
            FanOut::Three => vec![3, 7],
            FanOut::Four => vec![2, 5, 8],
        }
    }

    /// The target domains of the fan-out over the whole table.
    fn domains(self) -> Vec<Domain> {
        let mut bounds = vec![0];
        bounds.extend(self.cuts());
        bounds.push(KEY_COUNT);
        bounds
            .windows(2)
            .map(|window| Domain::new(window[0], window[1]))
            .collect()
    }

    fn token(self) -> &'static str {
        match self {
            FanOut::Two => "fo2",
            FanOut::Three => "fo3",
            FanOut::Four => "fo4",
        }
    }
}

/// Fan-in of one round: everything at once, or a subset then the rest.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FanIn {
    AllAtOnce,
    SubsetsThenRest,
}

impl FanIn {
    fn token(self) -> &'static str {
        match self {
            FanIn::AllAtOnce => "fiAll",
            FanIn::SubsetsThenRest => "fiSub",
        }
    }
}

/// Where a region compaction runs relative to the split and the merge.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Placement {
    None,
    OneSide,
    BothSides,
    BeforeSplit,
    BetweenMerges,
    AfterFinalMerge,
}

impl Placement {
    fn token(self) -> &'static str {
        match self {
            Placement::None => "plcNone",
            Placement::OneSide => "plcOne",
            Placement::BothSides => "plcBoth",
            Placement::BeforeSplit => "plcPre",
            Placement::BetweenMerges => "plcBetween",
            Placement::AfterFinalMerge => "plcAfter",
        }
    }
}

/// Which region id the merge target uses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Identity {
    /// A brand-new region id.
    Fresh,
    /// The region id the data lived in before the split, which still holds the objects.
    Original,
    /// One of the merge inputs, so the merge keeps a surviving half's identity.
    Half,
    /// The id that already holds exactly these objects: an idempotent second merge.
    SameFile,
}

impl Identity {
    fn token(self) -> &'static str {
        match self {
            Identity::Fresh => "idFresh",
            Identity::Original => "idOrig",
            Identity::Half => "idHalf",
            Identity::SameFile => "idSame",
        }
    }
}

/// The relation between the merge target's domain and the file creator's domain.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Relation {
    /// The creator's domain is split into two disjoint halves that share one object.
    DisjointHalves,
    /// Same split, but all rows land in one half.
    OneHalfEmpty,
    /// The target domain strictly contains a creator domain.
    NestedSuperset,
    /// The target domain is strictly inside the creator domain.
    NestedSubset,
    /// The target domain equals the creator domain (a same-domain remap).
    Equal,
}

impl Relation {
    fn token(self) -> &'static str {
        match self {
            Relation::DisjointHalves => "relDisjoint",
            Relation::OneHalfEmpty => "relHalfEmpty",
            Relation::NestedSuperset => "relSuperset",
            Relation::NestedSubset => "relSubset",
            Relation::Equal => "relEqual",
        }
    }
}

/// The order the merge consumes its inputs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Order {
    Forward,
    Reverse,
}

impl Order {
    fn token(self) -> &'static str {
        match self {
            Order::Forward => "ordAB",
            Order::Reverse => "ordBA",
        }
    }

    fn apply(&self, mut inputs: Vec<RegionId>) -> Vec<RegionId> {
        if *self == Order::Reverse {
            inputs.reverse();
        }
        inputs
    }
}

/// A write+flush interleaved between two steps of the round.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Interleaving {
    None,
    SplitToCompaction,
    CompactionToMerge,
    Both,
}

impl Interleaving {
    fn token(self) -> &'static str {
        match self {
            Interleaving::None => "ilNone",
            Interleaving::SplitToCompaction => "ilSplit",
            Interleaving::CompactionToMerge => "ilMerge",
            Interleaving::Both => "ilBoth",
        }
    }

    fn after_split(self) -> bool {
        matches!(self, Interleaving::SplitToCompaction | Interleaving::Both)
    }

    fn before_merge(self) -> bool {
        matches!(self, Interleaving::CompactionToMerge | Interleaving::Both)
    }
}

/// How the rows of a cell are distributed over the split targets.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RowShape {
    LeftOnly,
    RightOnly,
    Both,
    SameKeyDiffTs,
    SameKeySameTs,
}

impl RowShape {
    fn token(self) -> &'static str {
        match self {
            RowShape::LeftOnly => "rowsLeft",
            RowShape::RightOnly => "rowsRight",
            RowShape::Both => "rowsBoth",
            RowShape::SameKeyDiffTs => "rowsSameKeyDiffTs",
            RowShape::SameKeySameTs => "rowsSameKeySameTs",
        }
    }

    /// The initial rows, written before the first split.
    fn rows(self, domains: &[Domain]) -> Vec<PlannedRow> {
        let first = domains[0];
        let last = domains[domains.len() - 1];
        match self {
            RowShape::LeftOnly => vec![planned(first.lo, 0, 1.0), planned(first.lo + 1, 1000, 1.5)],
            RowShape::RightOnly => vec![planned(last.lo, 0, 2.0), planned(last.lo + 1, 1000, 2.5)],
            RowShape::Both => vec![planned(first.lo, 0, 3.0), planned(last.lo, 1000, 3.5)],
            RowShape::SameKeyDiffTs => {
                vec![planned(first.lo, 0, 4.0), planned(first.lo, 1000, 4.5)]
            }
            RowShape::SameKeySameTs => {
                vec![planned(first.lo, 1000, 5.0), planned(first.lo, 1000, 5.5)]
            }
        }
    }
}

/// One cell of the fixed table.
#[derive(Debug, Clone)]
struct Scenario {
    group: Group,
    generations: usize,
    fan_out: FanOut,
    fan_in: FanIn,
    placement: Placement,
    identity: Identity,
    relation: Relation,
    order: Order,
    rows: RowShape,
    interleaving: Interleaving,
}

impl Scenario {
    /// The cell name: every axis is encoded, so a failure names its own repro.
    fn name(&self) -> String {
        format!(
            "g{gen}-{fo}-{fi}-{plc}-{id}-{rel}-{ord}-{rows}-{il}",
            gen = self.generations,
            fo = self.fan_out.token(),
            fi = self.fan_in.token(),
            plc = self.placement.token(),
            id = self.identity.token(),
            rel = self.relation.token(),
            ord = self.order.token(),
            rows = self.rows.token(),
            il = self.interleaving.token(),
        )
    }

    /// The target domain of the merge: the source domain, or the one half that survives a
    /// nested-subset merge.
    fn final_domain(&self, source: Domain, domains: &[Domain]) -> Domain {
        match self.relation {
            Relation::NestedSubset => domains[0],
            _ => source,
        }
    }

    /// The rows a write+flush interleaves into the round.
    fn extra_rows(&self, domains: &[Domain]) -> Vec<PlannedRow> {
        let first = domains[0];
        let last = domains[domains.len() - 1];
        if self.relation == Relation::NestedSubset {
            // Only the surviving domain may hold rows: anything else would be a deliberately
            // losing merge, which is not a legal operation.
            return vec![planned(first.lo, 0, 6.0), planned(first.lo + 1, 0, 6.5)];
        }
        vec![planned(first.lo, 0, 6.0), planned(last.lo, 0, 6.5)]
    }
}

// ---------------------------------------------------------------------------------------
// The harness: real engine plus the ledger and the three views.
// ---------------------------------------------------------------------------------------

/// The engine plus the ledger, the published routing, the per-region references and the object
/// store the references resolve against.
struct Model {
    engine: MitoEngine,
    schema: Vec<api::v1::ColumnSchema>,
    store: ObjectStore,
    table_dir: String,
    path_type: PathType,
    ledger: Ledger,
    next_region_seq: u32,
    /// The published routing: the regions that serve the table, with their domain.
    routing: BTreeMap<RegionId, Domain>,
    /// Every region the harness created, live or retired: its live version file set.
    files: BTreeMap<RegionId, BTreeMap<FileId, SstRef>>,
    /// Every region the harness created: its persisted manifest file set.
    manifests: BTreeMap<RegionId, BTreeMap<FileId, ManifestRef>>,
    tracked: BTreeSet<RegionId>,
    dirty: BTreeSet<RegionId>,
    applied: BTreeMap<RegionId, Applied>,
    high_water: BTreeMap<RegionId, HighWater>,
    prev_objects: BTreeMap<(RegionId, FileId), BTreeSet<RegionId>>,
    dropped_at: BTreeMap<(RegionId, FileId), (usize, BTreeSet<RegionId>)>,

    scenario: Scenario,
    step: usize,
    limit: usize,
    log: Vec<String>,
    failures: Vec<Failure>,
    observations: Vec<Observation>,
    /// Set when an operation failed, so the cell stops instead of reporting noise.
    aborted: bool,
    /// Keeps the temporary data directory alive for the whole model.
    _env: TestEnv,
}

impl Model {
    async fn new(scenario: Scenario, limit: usize) -> Model {
        let mut env = TestEnv::with_prefix(&format!("mtx-{}", scenario.name())).await;
        let engine = env
            .create_engine(MitoConfig {
                // No uncontrolled background compaction: every compaction is requested.
                schedule_compaction_after_edit: false,
                // GC on (the production setting) with the default lingering time, so a
                // co-holder's purge unlinks but does not delete a shared object mid-cell.
                gc: GcConfig {
                    enable: true,
                    ..Default::default()
                },
                // Plain (non-flat) Parquet SSTs.
                default_flat_format: false,
                ..Default::default()
            })
            .await;
        let schema = rows_schema(&create_request(Domain::FULL));
        let root = RegionId::new(1, 1);
        engine
            .handle_request(root, RegionRequest::Create(create_request(Domain::FULL)))
            .await
            .expect("the harness must create its first region");
        let (store, table_dir, path_type) = {
            let region = engine.get_region(root).expect("the first region exists");
            (
                region.access_layer.object_store().clone(),
                region.access_layer.table_dir().to_string(),
                region.access_layer.path_type(),
            )
        };
        let mut model = Model {
            engine,
            schema,
            store,
            table_dir,
            path_type,
            ledger: Ledger::default(),
            next_region_seq: 2,
            routing: BTreeMap::from([(root, Domain::FULL)]),
            files: BTreeMap::new(),
            manifests: BTreeMap::new(),
            tracked: BTreeSet::from([root]),
            dirty: BTreeSet::new(),
            applied: BTreeMap::new(),
            high_water: BTreeMap::new(),
            prev_objects: BTreeMap::new(),
            dropped_at: BTreeMap::new(),
            scenario,
            step: 0,
            limit,
            log: Vec::new(),
            failures: Vec::new(),
            observations: Vec::new(),
            aborted: false,
            _env: env,
        };
        model.refresh().await;
        model
    }

    fn alloc_region_id(&mut self) -> RegionId {
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

    /// The file set of one region, as its live version holds it.
    fn file_set(&self, region_id: RegionId) -> BTreeSet<FileId> {
        self.files
            .get(&region_id)
            .map(|files| files.keys().copied().collect())
            .unwrap_or_default()
    }

    fn op_failure(&mut self, detail: String) {
        self.aborted = true;
        self.record(Category::Operation, detail);
    }

    fn record(&mut self, category: Category, detail: String) {
        let step_description = self.log.last().cloned().unwrap_or_default();
        if category.fatal() {
            self.failures.push(Failure {
                scenario: self.scenario.name(),
                step: self.step,
                step_description,
                category,
                detail,
            });
        } else {
            self.observations.push(Observation {
                scenario: self.scenario.name(),
                step: self.step,
                category,
                detail,
            });
        }
    }

    /// Starts one published step. Returns `false` when the replay budget is exhausted or a
    /// previous operation failed, which ends the cell.
    fn begin(&mut self, description: String) -> bool {
        if self.aborted || self.step >= self.limit {
            return false;
        }
        self.step += 1;
        self.log.push(description);
        true
    }

    /// Refreshes layer 2, then asserts every invariant category of the current step.
    async fn checkpoint(&mut self) {
        self.refresh().await;
        let mut violations = Vec::new();
        match self.check_visibility().await {
            Ok(mut found) => violations.append(&mut found),
            Err(err) => violations.push(Violation {
                category: Category::Visibility,
                detail: err,
            }),
        }
        self.check_manifest(&mut violations);
        self.check_routing(&mut violations);
        self.check_reference_bounds(&mut violations);
        self.check_sequences(&mut violations);
        self.check_physical_liveness(&mut violations).await;
        for violation in violations {
            self.record(violation.category, violation.detail);
        }
    }

    /// Reads each tracked region's live version, persisted manifest and applied state.
    async fn refresh(&mut self) {
        let mut files = BTreeMap::new();
        let mut manifests = BTreeMap::new();
        let mut applied = BTreeMap::new();
        for region_id in self.tracked.clone() {
            let region = self
                .engine
                .get_region(region_id)
                .unwrap_or_else(|| panic!("region {region_id} disappeared"));
            let manifest = region.manifest_ctx.manifest().await;
            let data = region.version_control.current();

            let mut version_files = BTreeMap::new();
            for level in data.version.ssts.levels() {
                for (file_id, handle) in &level.files {
                    let meta = handle.meta_ref();
                    version_files.insert(
                        *file_id,
                        SstRef {
                            file_id: *file_id,
                            owner: meta.region_id,
                            level: meta.level,
                            slot_level: level.level,
                            num_rows: meta.num_rows,
                            sequence: meta.sequence.map(|sequence| sequence.get()),
                            creator: range_keys(meta.partition_expr.as_deref()),
                        },
                    );
                }
            }
            let mut manifest_files = BTreeMap::new();
            for (file_id, meta) in &manifest.files {
                manifest_files.insert(
                    *file_id,
                    ManifestRef {
                        level: meta.level,
                        sequence: meta.sequence.map(|sequence| sequence.get()),
                        num_rows: meta.num_rows,
                    },
                );
            }

            files.insert(region_id, version_files);
            manifests.insert(region_id, manifest_files);
            applied.insert(
                region_id,
                Applied {
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
        self.files = files;
        self.manifests = manifests;
        self.applied = applied;
    }

    fn sst_path(&self, creator: RegionId, file_id: FileId) -> String {
        location::sst_file_path(
            &self.table_dir,
            RegionFileId::new(creator, file_id),
            self.path_type,
        )
    }

    /// A plain region scan, as a multiset of visible `event_id`s.
    async fn scan_counts(&self, region_id: RegionId) -> Result<BTreeMap<i64, usize>, String> {
        let stream = self
            .engine
            .scan_to_stream(region_id, ScanRequest::default())
            .await
            .map_err(|err| format!("scan of {region_id} failed: {err:?}"))?;
        let batches = RecordBatches::try_collect(stream)
            .await
            .map_err(|err| format!("collecting the scan of {region_id} failed: {err:?}"))?;
        let mut counts = BTreeMap::new();
        for batch in batches.iter() {
            let ids = batch
                .column_by_name("event_id")
                .ok_or_else(|| "a plain scan must project the event_id column".to_string())?
                .as_primitive::<Int64Type>();
            for i in 0..batch.num_rows() {
                *counts.entry(ids.value(i)).or_default() += 1;
            }
        }
        Ok(counts)
    }

    /// The oracle: the visible multiset over the published routing equals the ledger exactly
    /// once, and no region shows an event its published domain does not own.
    async fn check_visibility(&mut self) -> Result<Vec<Violation>, String> {
        let live = self.live_regions();
        let mut visible: BTreeMap<i64, usize> = BTreeMap::new();
        let mut per_region: Vec<(RegionId, BTreeMap<i64, usize>)> = Vec::new();
        let mut misplaced: Vec<String> = Vec::new();
        for region_id in &live {
            let domain = self.domain_of(*region_id);
            let counts = self.scan_counts(*region_id).await?;
            let allowed = self.ledger.ids_in(domain);
            for event_id in counts.keys() {
                if !allowed.contains(event_id) {
                    misplaced.push(format!(
                        "region {region_id} (domain {}) shows {} which the ledger does not place \
                         in that domain",
                        domain.label(),
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
            return Ok(Vec::new());
        }

        let references: Vec<String> = self
            .tracked
            .iter()
            .map(|region_id| {
                let files: Vec<String> = self
                    .files
                    .get(region_id)
                    .map(|files| {
                        files
                            .values()
                            .map(|sst| {
                                format!(
                                    "{:?}(creator={}, L{}, rows={}, authorized={})",
                                    sst.file_id,
                                    sst.owner,
                                    sst.level,
                                    sst.num_rows,
                                    keys_label(&sst.creator)
                                )
                            })
                            .collect()
                    })
                    .unwrap_or_default();
                format!("region {region_id} -> [{files:?}]")
            })
            .collect();

        Ok(vec![Violation {
            category: Category::Visibility,
            detail: format!(
                "the visible multiset does not equal the ledger; published routing {live:?}; \
                 visible {visible:?}; expected {expected:?}; duplicated {duplicated:?}; missing \
                 {missing:?}; unexpected {unexpected:?}; misplaced {misplaced:?}; per-region \
                 {per_region:?}; references {references:?}"
            ),
        }])
    }

    /// A holder's persisted manifest file set equals its live version file set.
    fn check_manifest(&self, violations: &mut Vec<Violation>) {
        for region_id in &self.tracked {
            let (Some(files), Some(manifest)) =
                (self.files.get(region_id), self.manifests.get(region_id))
            else {
                continue;
            };
            let version: BTreeMap<FileId, (u8, Option<u64>, u64)> = files
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
            let persisted: BTreeMap<FileId, (u8, Option<u64>, u64)> = manifest
                .iter()
                .map(|(file_id, reference)| {
                    (
                        *file_id,
                        (reference.level, reference.sequence, reference.num_rows),
                    )
                })
                .collect();
            if version == persisted {
                continue;
            }
            let only_version: Vec<FileId> = version
                .keys()
                .filter(|file_id| !persisted.contains_key(file_id))
                .copied()
                .collect();
            let only_manifest: Vec<FileId> = persisted
                .keys()
                .filter(|file_id| !version.contains_key(file_id))
                .copied()
                .collect();
            let differing: Vec<String> = version
                .iter()
                .filter_map(|(file_id, live)| {
                    persisted.get(file_id).and_then(|stored| {
                        (live != stored).then(|| {
                            format!(
                                "{file_id:?} version (L{}, sequence {:?}, rows {}) vs manifest \
                                 (L{}, sequence {:?}, rows {})",
                                live.0, live.1, live.2, stored.0, stored.1, stored.2
                            )
                        })
                    })
                })
                .collect();
            violations.push(Violation {
                category: Category::ManifestConsistency,
                detail: format!(
                    "region {region_id}: the live version and the persisted manifest disagree; \
                     only in version {only_version:?}; only in manifest {only_manifest:?}; \
                     differing {differing:?}"
                ),
            });
        }
    }

    /// The published routing equals the regions that actually applied it.
    fn check_routing(&self, violations: &mut Vec<Violation>) {
        for (region_id, domain) in &self.routing {
            let Some(state) = self.applied.get(region_id) else {
                violations.push(Violation {
                    category: Category::ManifestConsistency,
                    detail: format!("published region {region_id} is not a created region"),
                });
                continue;
            };
            if state.domain != *domain {
                violations.push(Violation {
                    category: Category::ManifestConsistency,
                    detail: format!(
                        "region {region_id} is published as {} but applied {}",
                        domain.label(),
                        state.domain.label()
                    ),
                });
            }
            if !state.writable {
                violations.push(Violation {
                    category: Category::ManifestConsistency,
                    detail: format!(
                        "region {region_id} is published for {} but does not accept writes, so no \
                         region serves that domain",
                        domain.label()
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
                    "region {region_id} is no longer published but still accepts writes for {}",
                    state.domain.label()
                ),
            });
        }
    }

    /// Every reference a live holder shows authorizes only rows inside that holder's domain.
    ///
    /// The reference's authorized range is its file's creator domain on this branch (see
    /// [`SstRef::creator`]), so a legal split already reports here: both halves reference an
    /// object the whole-domain source created. The category is checked and reported after every
    /// step, and is non-fatal for that reason.
    fn check_reference_bounds(&self, violations: &mut Vec<Violation>) {
        for region_id in self.live_regions() {
            let holder = self.domain_of(region_id).keys();
            let Some(files) = self.files.get(&region_id) else {
                continue;
            };
            for (file_id, sst) in files {
                if sst.creator.is_subset(&holder) {
                    continue;
                }
                violations.push(Violation {
                    category: Category::ReferenceBounds,
                    detail: format!(
                        "region {region_id} (domain {}) references {file_id:?} whose authorized \
                         range {} (creator domain, owner {}, L{}, sequence {:?}) leaves the \
                         holder's domain",
                        keys_label(&holder),
                        keys_label(&sst.creator),
                        sst.owner,
                        sst.level,
                        sst.sequence
                    ),
                });
            }
        }
    }

    /// Per region the committed sequence never decreases.
    fn check_sequences(&mut self, violations: &mut Vec<Violation>) {
        for (region_id, state) in &self.applied {
            let high = self.high_water.get(region_id).copied().unwrap_or_default();
            if state.committed < high.committed {
                violations.push(Violation {
                    category: Category::SequenceMonotonicity,
                    detail: format!(
                        "region {region_id}: committed sequence went back from {} to {}",
                        high.committed, state.committed
                    ),
                });
            }
            if state.last_sequence < high.last {
                violations.push(Violation {
                    category: Category::SequenceMonotonicity,
                    detail: format!(
                        "region {region_id}: last allocated sequence went back from {} to {}",
                        high.last, state.last_sequence
                    ),
                });
            }
            if state.last_sequence < state.committed {
                violations.push(Violation {
                    category: Category::SequenceMonotonicity,
                    detail: format!(
                        "region {region_id}: last allocated sequence {} is below the committed \
                         sequence {}",
                        state.last_sequence, state.committed
                    ),
                });
            }
            if let Some(persisted) = state.manifest_committed
                && persisted < high.persisted
            {
                violations.push(Violation {
                    category: Category::SequenceMonotonicity,
                    detail: format!(
                        "region {region_id}: the persisted committed sequence went back from {} to \
                         {}",
                        high.persisted, persisted
                    ),
                });
            }
            self.high_water.insert(
                *region_id,
                HighWater {
                    committed: high.committed.max(state.committed),
                    last: high.last.max(state.last_sequence),
                    persisted: high
                        .persisted
                        .max(state.manifest_committed.unwrap_or_default()),
                },
            );
        }
    }

    /// Every object still referenced by a live holder still reads back as a complete object.
    async fn check_physical_liveness(&mut self, violations: &mut Vec<Violation>) {
        let mut objects: BTreeMap<(RegionId, FileId), BTreeSet<RegionId>> = BTreeMap::new();
        for region_id in self.live_regions() {
            let Some(files) = self.files.get(&region_id) else {
                continue;
            };
            for sst in files.values() {
                objects
                    .entry((sst.owner, sst.file_id))
                    .or_default()
                    .insert(region_id);
            }
        }
        for (object, holders) in &objects {
            if let Some(previous) = self.prev_objects.get(object)
                && previous != holders
                && !previous.is_subset(holders)
            {
                self.dropped_at
                    .insert(*object, (self.step, previous.clone()));
            }
        }
        for (object, previous) in &self.prev_objects {
            if !objects.contains_key(object) {
                self.dropped_at
                    .insert(*object, (self.step, previous.clone()));
            }
        }
        for (object, holders) in &objects {
            let (creator, file_id) = *object;
            let path = self.sst_path(creator, file_id);
            let Err(reason) = self.read_object(&path).await else {
                continue;
            };
            let dropped = match self.dropped_at.get(object) {
                Some((step, before)) => format!(
                    "last released at step {step} by {:?}",
                    before
                        .difference(holders)
                        .map(|region_id| region_id.to_string())
                        .collect::<Vec<_>>()
                ),
                None => "never released by a live holder".to_string(),
            };
            violations.push(Violation {
                category: Category::PhysicalLiveness,
                detail: format!(
                    "object {file_id:?} (creator {creator}) at {path} is not readable although \
                     {holders:?} still reference it: {reason}; {dropped}"
                ),
            });
        }
        self.prev_objects = objects;
    }

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

    // -----------------------------------------------------------------------------------
    // Operations. Every one is a serial, successful request or reports a failure.
    // -----------------------------------------------------------------------------------

    /// Appends rows to the live region that owns each row's partition key, then commits them to
    /// the ledger.
    async fn write_rows(&mut self, rows: &[PlannedRow]) -> Result<(), String> {
        let mut by_region: BTreeMap<RegionId, Vec<(i64, PlannedRow, api::v1::Row)>> =
            BTreeMap::new();
        for row in rows {
            let region_id = self
                .routing
                .iter()
                .find(|(_, domain)| domain.contains_key(row.key))
                .map(|(region_id, _)| *region_id)
                .ok_or_else(|| {
                    format!(
                        "no published region covers key {}, routing {:?}",
                        key_name(row.key),
                        self.routing
                    )
                })?;
            let event_id = self.ledger.reserve();
            by_region.entry(region_id).or_default().push((
                event_id,
                *row,
                event_row(event_id, *row),
            ));
        }
        for (region_id, pending) in by_region {
            let values: Vec<api::v1::Row> = pending.iter().map(|(_, _, row)| row.clone()).collect();
            self.engine
                .handle_request(
                    region_id,
                    RegionRequest::Put(RegionPutRequest {
                        skip_wal: false,
                        rows: Rows {
                            schema: self.schema.clone(),
                            rows: values,
                        },
                        hint: None,
                        partition_expr_version: None,
                    }),
                )
                .await
                .map_err(|err| format!("put into {region_id} failed: {err:?}"))?;
            for (event_id, row, _) in pending {
                self.ledger.commit(event_id, row);
            }
            self.dirty.insert(region_id);
        }
        Ok(())
    }

    async fn write_and_flush(&mut self, rows: &[PlannedRow]) -> Result<(), String> {
        self.write_rows(rows).await?;
        self.flush_dirty().await
    }

    async fn flush_dirty(&mut self) -> Result<(), String> {
        let dirty: Vec<RegionId> = self.dirty.iter().copied().collect();
        for region_id in dirty {
            self.flush_region(region_id).await?;
        }
        Ok(())
    }

    async fn flush_region(&mut self, region_id: RegionId) -> Result<(), String> {
        if !self.dirty.remove(&region_id) {
            return Ok(());
        }
        self.engine
            .handle_request(
                region_id,
                RegionRequest::Flush(RegionFlushRequest::default()),
            )
            .await
            .map_err(|err| format!("flush of {region_id} failed: {err:?}"))?;
        Ok(())
    }

    /// Region compaction on one published region.
    async fn compact(&mut self, region_id: RegionId) -> Result<(), String> {
        self.flush_region(region_id).await?;
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
            .map_err(|err| format!("compaction of {region_id} failed: {err:?}"))?;
        Ok(())
    }

    async fn set_staging(&mut self, region_id: RegionId) -> Result<(), String> {
        let response = self
            .engine
            .set_region_role_state_gracefully(region_id, SettableRegionRoleState::StagingLeader)
            .await
            .map_err(|err| format!("staging {region_id} failed: {err:?}"))?;
        if response.is_invalid_transition() || response.is_not_found() {
            return Err(format!("staging {region_id} was rejected: {response:?}"));
        }
        Ok(())
    }

    async fn enter_staging(&mut self, region_id: RegionId, domain: Domain) -> Result<(), String> {
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
            .map_err(|err| format!("enter staging on {region_id} failed: {err:?}"))?;
        Ok(())
    }

    /// Creates a region and puts it into staging state with `domain` as its staging partition
    /// expression.
    async fn create_and_enter_staging(
        &mut self,
        region_id: RegionId,
        domain: Domain,
    ) -> Result<(), String> {
        self.engine
            .handle_request(region_id, RegionRequest::Create(create_request(domain)))
            .await
            .map_err(|err| format!("create of {region_id} failed: {err:?}"))?;
        self.tracked.insert(region_id);
        self.enter_staging(region_id, domain).await
    }

    async fn apply_staging_manifest(
        &mut self,
        region_id: RegionId,
        domain: Domain,
        central_region_id: RegionId,
        manifest_paths: &HashMap<RegionId, String>,
    ) -> Result<(), String> {
        let manifest_path = manifest_paths
            .get(&region_id)
            .ok_or_else(|| format!("the coordinator published no manifest for {region_id}"))?
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
            .map_err(|err| {
                format!("applying the staging manifest of {region_id} failed: {err:?}")
            })?;
        Ok(())
    }

    /// Splits `source` into one target per domain cut, in one remap.
    ///
    /// `pre_stage` enters staging on the source *before* the remap, which is what lets the same
    /// region id be the merge target later (identity `Original`). Publication is atomic for the
    /// harness: the targets are created, staged and applied before the routing switches, so no
    /// checkpoint observes the source while it is staged.
    async fn split_into(
        &mut self,
        source: RegionId,
        cuts: &[usize],
        pre_stage: Option<Domain>,
    ) -> Result<Vec<RegionId>, String> {
        let source_domain = self.domain_of(source);
        let mut bounds = vec![source_domain.lo];
        bounds.extend(cuts.iter().copied());
        bounds.push(source_domain.hi);
        for cut in cuts {
            if !(source_domain.lo < *cut && *cut < source_domain.hi) {
                return Err(format!(
                    "cut {} is not strictly inside the source domain {}",
                    key_name(*cut),
                    source_domain.label()
                ));
            }
        }
        let domains: Vec<Domain> = bounds
            .windows(2)
            .map(|window| Domain::new(window[0], window[1]))
            .collect();
        // A split remaps persisted manifests, so the source's rows must be in SSTs.
        self.flush_dirty().await?;
        if let Some(domain) = pre_stage {
            self.enter_staging(source, domain).await?;
        } else {
            self.set_staging(source).await?;
        }

        let targets: Vec<RegionId> = domains.iter().map(|_| self.alloc_region_id()).collect();
        let response = self
            .engine
            .remap_manifests(RemapManifestsRequest {
                region_id: source,
                input_regions: vec![source],
                region_mapping: [(source, targets.clone())].into_iter().collect(),
                new_partition_exprs: targets
                    .iter()
                    .zip(domains.iter())
                    .map(|(region_id, domain)| (*region_id, domain.expr_json()))
                    .collect(),
            })
            .await
            .map_err(|err| format!("remap of {source} failed: {err:?}"))?;

        for (region_id, domain) in targets.iter().zip(domains.iter()) {
            self.create_and_enter_staging(*region_id, *domain).await?;
            self.apply_staging_manifest(*region_id, *domain, source, &response.manifest_paths)
                .await?;
        }

        self.routing = targets
            .iter()
            .copied()
            .zip(domains.iter().copied())
            .collect();
        self.refresh().await;
        Ok(targets)
    }

    /// Merges `inputs` into one region whose domain is `domain`.
    ///
    /// `target` selects the merge target: a fresh region id, or an existing region which this
    /// same step brings into staging with `domain`. Publication is atomic for the harness: the
    /// inputs stop serving and the target installs the remapped manifest before the routing
    /// switches.
    async fn merge(
        &mut self,
        inputs: &[RegionId],
        target: Option<RegionId>,
        domain: Domain,
    ) -> Result<RegionId, String> {
        if inputs.is_empty() {
            return Err("a merge needs at least one input".to_string());
        }
        self.flush_dirty().await?;

        let target = match target {
            Some(target) => {
                if !self.tracked.contains(&target) {
                    return Err(format!("merge target {target} does not exist"));
                }
                // Bring the existing region into staging with the merged domain. A region the
                // split already left in staging keeps the staging expression it was given.
                if self
                    .applied
                    .get(&target)
                    .is_some_and(|state| state.writable)
                {
                    self.enter_staging(target, domain).await?;
                }
                target
            }
            None => {
                let target = self.alloc_region_id();
                self.create_and_enter_staging(target, domain).await?;
                target
            }
        };

        // A retired region must not keep writing for a domain another region now serves.
        for input in inputs {
            if *input != target {
                self.set_staging(*input).await?;
            }
        }

        let response = self
            .engine
            .remap_manifests(RemapManifestsRequest {
                region_id: target,
                input_regions: inputs.to_vec(),
                region_mapping: inputs
                    .iter()
                    .map(|region_id| (*region_id, vec![target]))
                    .collect(),
                new_partition_exprs: [(target, domain.expr_json())].into_iter().collect(),
            })
            .await
            .map_err(|err| format!("remap into {target} failed: {err:?}"))?;

        self.apply_staging_manifest(target, domain, target, &response.manifest_paths)
            .await?;

        // The merged region serves `domain`; a region of the routing the merge did not consume
        // keeps serving its own domain, so a subset merge leaves the rest of the table live.
        let mut routing: BTreeMap<RegionId, Domain> = BTreeMap::new();
        for (region_id, served) in &self.routing {
            if !inputs.contains(region_id) {
                routing.insert(*region_id, *served);
            }
        }
        routing.insert(target, domain);
        self.routing = routing;
        self.refresh().await;
        Ok(target)
    }
}

/// A key domain expressed by a region's partition expression JSON.
fn domain_of_json(json: &str) -> Domain {
    if json.is_empty() {
        return Domain::FULL;
    }
    let expr = PartitionExpr::from_json_str(json)
        .unwrap_or_else(|err| panic!("harness cannot parse partition expr {json:?}: {err}"))
        .unwrap_or_else(|| panic!("harness got an empty partition bound from {json:?}"));
    let keys = eval_keys(&expr)
        .unwrap_or_else(|| panic!("harness cannot evaluate partition expr {json:?} as keys"));
    match (keys.iter().next(), keys.iter().next_back()) {
        (Some(lo), Some(hi)) => Domain::new(*lo, (hi + 1).min(KEY_COUNT)),
        _ => Domain::new(0, 0),
    }
}

/// A key column `k` (string tag), payload `payload` (`Float64`), `event_id` (`Int64`) and `ts`;
/// append mode with per-row sequences preserved, partitioned by `k`.
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

// ---------------------------------------------------------------------------------------
// The table.
// ---------------------------------------------------------------------------------------

/// One cell with every axis written out.
#[allow(clippy::too_many_arguments)]
fn cell(
    group: Group,
    generations: usize,
    fan_out: FanOut,
    fan_in: FanIn,
    placement: Placement,
    identity: Identity,
    relation: Relation,
    order: Order,
    rows: RowShape,
    interleaving: Interleaving,
) -> Scenario {
    Scenario {
        group,
        generations,
        fan_out,
        fan_in,
        placement,
        identity,
        relation,
        order,
        rows,
        interleaving,
    }
}

/// The fixed table of merge shapes.
///
/// Each group crosses the axis it is named for and holds everything else at a baseline, so every
/// cell of the table is one assertion and the axes are covered exhaustively where they interact
/// with a merge.
fn table() -> Vec<Scenario> {
    let mut cells = Vec::new();

    // --- generations x compaction placement -------------------------------------------
    for generations in [1, 2, 3] {
        for placement in [
            Placement::None,
            Placement::OneSide,
            Placement::BothSides,
            Placement::BeforeSplit,
            Placement::BetweenMerges,
            Placement::AfterFinalMerge,
        ] {
            cells.push(cell(
                Group::Generations,
                generations,
                FanOut::Two,
                FanIn::AllAtOnce,
                placement,
                Identity::Fresh,
                Relation::DisjointHalves,
                Order::Forward,
                RowShape::Both,
                Interleaving::None,
            ));
        }
    }

    // --- fan-out x fan-in --------------------------------------------------------------
    for fan_out in [FanOut::Two, FanOut::Three, FanOut::Four] {
        for fan_in in [FanIn::AllAtOnce, FanIn::SubsetsThenRest] {
            cells.push(cell(
                Group::FanOut,
                1,
                fan_out,
                fan_in,
                Placement::None,
                Identity::Fresh,
                Relation::DisjointHalves,
                Order::Forward,
                RowShape::Both,
                Interleaving::None,
            ));
        }
        // A subset-then-rest fan-in over two generations nests the merges.
        cells.push(cell(
            Group::FanOut,
            2,
            fan_out,
            FanIn::SubsetsThenRest,
            Placement::None,
            Identity::Fresh,
            Relation::DisjointHalves,
            Order::Forward,
            RowShape::Both,
            Interleaving::None,
        ));
    }

    // --- merge target identity x input order ------------------------------------------
    for identity in [
        Identity::Fresh,
        Identity::Original,
        Identity::Half,
        Identity::SameFile,
    ] {
        for order in [Order::Forward, Order::Reverse] {
            cells.push(cell(
                Group::Identity,
                1,
                FanOut::Two,
                FanIn::AllAtOnce,
                Placement::None,
                identity,
                Relation::DisjointHalves,
                order,
                RowShape::Both,
                Interleaving::None,
            ));
        }
        // The identity crossed with a subset-then-rest fan-in.
        cells.push(cell(
            Group::Identity,
            1,
            FanOut::Three,
            FanIn::SubsetsThenRest,
            Placement::None,
            identity,
            Relation::DisjointHalves,
            Order::Forward,
            RowShape::Both,
            Interleaving::None,
        ));
    }

    // --- row distribution x domain relation -------------------------------------------
    for relation in [
        Relation::DisjointHalves,
        Relation::OneHalfEmpty,
        Relation::NestedSuperset,
        Relation::Equal,
    ] {
        // One half empty of rows is incompatible with a row shape that fills both halves.
        let row_shapes: &[RowShape] = if relation == Relation::OneHalfEmpty {
            &[
                RowShape::LeftOnly,
                RowShape::RightOnly,
                RowShape::SameKeyDiffTs,
                RowShape::SameKeySameTs,
            ]
        } else {
            &[
                RowShape::LeftOnly,
                RowShape::RightOnly,
                RowShape::Both,
                RowShape::SameKeyDiffTs,
                RowShape::SameKeySameTs,
            ]
        };
        for rows in row_shapes.iter().copied() {
            // A nested superset needs an object the *half* created, so the cell interleaves a
            // write+flush between the split and the compaction.
            let interleaving = if relation == Relation::NestedSuperset {
                Interleaving::SplitToCompaction
            } else {
                Interleaving::None
            };
            cells.push(cell(
                Group::Rows,
                1,
                FanOut::Two,
                FanIn::AllAtOnce,
                Placement::None,
                Identity::Fresh,
                relation,
                Order::Forward,
                rows,
                interleaving,
            ));
        }
    }
    // A nested-subset target only keeps the rows of the half it covers, so its row shapes are the
    // ones that live in the first target domain.
    for rows in [
        RowShape::LeftOnly,
        RowShape::SameKeyDiffTs,
        RowShape::SameKeySameTs,
    ] {
        cells.push(cell(
            Group::Rows,
            1,
            FanOut::Two,
            FanIn::AllAtOnce,
            Placement::None,
            Identity::Fresh,
            Relation::NestedSubset,
            Order::Forward,
            rows,
            Interleaving::None,
        ));
    }

    // --- interleaving x compaction placement ------------------------------------------
    for interleaving in [
        Interleaving::SplitToCompaction,
        Interleaving::CompactionToMerge,
        Interleaving::Both,
    ] {
        for placement in [Placement::None, Placement::OneSide, Placement::BothSides] {
            cells.push(cell(
                Group::Interleaving,
                2,
                FanOut::Two,
                FanIn::AllAtOnce,
                placement,
                Identity::Fresh,
                Relation::NestedSuperset,
                Order::Forward,
                RowShape::Both,
                interleaving,
            ));
        }
    }

    cells
}

/// The axis counts of [`table`], for the report.
fn table_summary(cells: &[Scenario]) -> String {
    let mut per_group: BTreeMap<&'static str, usize> = BTreeMap::new();
    let mut generations: BTreeMap<String, usize> = BTreeMap::new();
    let mut placements: BTreeMap<&'static str, usize> = BTreeMap::new();
    let mut fan_outs: BTreeMap<&'static str, usize> = BTreeMap::new();
    let mut fan_ins: BTreeMap<&'static str, usize> = BTreeMap::new();
    let mut identities: BTreeMap<&'static str, usize> = BTreeMap::new();
    let mut relations: BTreeMap<&'static str, usize> = BTreeMap::new();
    let mut orders: BTreeMap<&'static str, usize> = BTreeMap::new();
    let mut rows: BTreeMap<&'static str, usize> = BTreeMap::new();
    let mut interleavings: BTreeMap<&'static str, usize> = BTreeMap::new();
    for scenario in cells {
        *per_group.entry(scenario.group.name()).or_default() += 1;
        *generations
            .entry(format!("g{}", scenario.generations))
            .or_default() += 1;
        *placements.entry(scenario.placement.token()).or_default() += 1;
        *fan_outs.entry(scenario.fan_out.token()).or_default() += 1;
        *fan_ins.entry(scenario.fan_in.token()).or_default() += 1;
        *identities.entry(scenario.identity.token()).or_default() += 1;
        *relations.entry(scenario.relation.token()).or_default() += 1;
        *orders.entry(scenario.order.token()).or_default() += 1;
        *rows.entry(scenario.rows.token()).or_default() += 1;
        *interleavings
            .entry(scenario.interleaving.token())
            .or_default() += 1;
    }
    format!(
        "{} cells | groups {per_group:?} | generations {generations:?} | placement {placements:?} \
         | fan-out {fan_outs:?} | fan-in {fan_ins:?} | identity {identities:?} | relation \
         {relations:?} | order {orders:?} | rows {rows:?} | interleaving {interleavings:?}",
        cells.len()
    )
}

// ---------------------------------------------------------------------------------------
// Running one cell.
// ---------------------------------------------------------------------------------------

/// Applies one operation as a published step: it must succeed, and its post-state is checked
/// against every invariant category. Ends the cell on a failed operation.
macro_rules! apply {
    ($model:ident, $description:expr, $op:expr) => {{
        if !$model.begin($description) {
            return $model;
        }
        let outcome = $op;
        let value = match outcome {
            Ok(value) => value,
            Err(err) => {
                $model.op_failure(err);
                return $model;
            }
        };
        $model.checkpoint().await;
        if $model.aborted {
            return $model;
        }
        value
    }};
}

/// Replays one table cell. `limit` bounds the number of published steps, which is how a failing
/// cell is shrunk to its shortest reproducing prefix.
async fn run_scenario(scenario: &Scenario, limit: usize) -> Model {
    let mut model = Model::new(scenario.clone(), limit).await;
    let domains = scenario.fan_out.domains();
    let cuts = scenario.fan_out.cuts();

    let initial = scenario.rows.rows(&domains);
    apply!(
        model,
        format!(
            "setup: write {} rows ({}) into the whole-domain region and flush",
            initial.len(),
            scenario.rows.token()
        ),
        model.write_and_flush(&initial).await
    );

    for generation in 1..=scenario.generations {
        let live = model.live_regions();
        if live.len() != 1 {
            model.op_failure(format!(
                "generation {generation} must start from one live region, got {live:?}"
            ));
            return model;
        }
        let source = live[0];
        let source_domain = model.domain_of(source);
        let final_domain = scenario.final_domain(source_domain, &domains);

        if scenario.placement == Placement::BeforeSplit {
            apply!(
                model,
                format!("generation {generation}: compact {source} before the split"),
                model.compact(source).await
            );
        }

        // Identity `Original` needs the source staged under the domain it will serve after the
        // merge, so the same region id can consume the staging manifest.
        let pre_stage = (scenario.identity == Identity::Original).then_some(final_domain);
        let targets = apply!(
            model,
            format!(
                "generation {generation}: split {source} (domain {}) at {cuts:?} into {count} \
                 targets{staged}",
                source_domain.label(),
                count = domains.len(),
                staged = if pre_stage.is_some() {
                    ", staging the source under the merge target domain"
                } else {
                    ""
                }
            ),
            model.split_into(source, &cuts, pre_stage).await
        );
        model
            .check_split_shape(&targets, &domains, generation)
            .await;

        if scenario.interleaving.after_split() {
            let rows = scenario.extra_rows(&domains);
            apply!(
                model,
                format!(
                    "generation {generation}: interleaved write+flush of {} rows after the split",
                    rows.len()
                ),
                model.write_and_flush(&rows).await
            );
        }

        match scenario.placement {
            Placement::OneSide => {
                let target = targets[0];
                apply!(
                    model,
                    format!("generation {generation}: compact one side ({target})"),
                    model.compact(target).await
                );
            }
            Placement::BothSides => {
                for target in targets.clone() {
                    apply!(
                        model,
                        format!("generation {generation}: compact one side of both ({target})"),
                        model.compact(target).await
                    );
                }
            }
            _ => {}
        }

        if scenario.interleaving.before_merge() {
            let rows = scenario.extra_rows(&domains);
            apply!(
                model,
                format!(
                    "generation {generation}: interleaved write+flush of {} rows between the \
                     compaction and the merge",
                    rows.len()
                ),
                model.write_and_flush(&rows).await
            );
        }

        // ---- fan-in -------------------------------------------------------------------
        let ordered = scenario.order.apply(targets.clone());
        let half = match scenario.fan_in {
            FanIn::AllAtOnce => ordered.len(),
            FanIn::SubsetsThenRest => ordered.len().div_ceil(2).max(1),
        };
        let subset: Vec<RegionId> = ordered[..half].to_vec();
        let rest: Vec<RegionId> = ordered[half..].to_vec();
        let remaining: Vec<RegionId> = if rest.is_empty() {
            ordered
        } else {
            let subset_domain = Domain::new(domains[0].lo, domains[subset.len() - 1].hi);
            let merged_subset = apply!(
                model,
                format!(
                    "generation {generation}: merge the subset {subset:?} into a fresh region \
                     (domain {})",
                    subset_domain.label()
                ),
                model.merge(&subset, None, subset_domain).await
            );
            std::iter::once(merged_subset)
                .chain(rest.iter().copied())
                .collect()
        };

        let (target, plan_description) = match scenario.identity {
            Identity::Fresh | Identity::SameFile => (None, "a fresh region id".to_string()),
            Identity::Original => (
                Some(source),
                format!("the original region id {source} (the pre-split region)"),
            ),
            Identity::Half => (
                Some(remaining[0]),
                format!(
                    "the first merge input {} (a surviving half, order {})",
                    remaining[0],
                    scenario.order.token()
                ),
            ),
        };
        let merged = apply!(
            model,
            format!(
                "generation {generation}: merge {remaining:?} into {plan_description} (domain {}, \
                 order {})",
                final_domain.label(),
                scenario.order.token()
            ),
            model.merge(&remaining, target, final_domain).await
        );
        model.check_merge_identity(scenario, source, &remaining, merged);

        if generation == scenario.generations {
            model.check_merge_shape(scenario, merged);
            if scenario.identity == Identity::SameFile {
                let before = model.file_set(merged);
                apply!(
                    model,
                    format!(
                        "generation {generation}: idempotent second merge of {merged} into itself \
                         (an id that already holds these objects)"
                    ),
                    model.merge(&[merged], Some(merged), final_domain).await
                );
                model.check_same_file(merged, &before);
            }
        }

        let compact_after = match scenario.placement {
            Placement::BetweenMerges => generation < scenario.generations,
            Placement::AfterFinalMerge => generation == scenario.generations,
            _ => false,
        };
        if compact_after {
            apply!(
                model,
                format!(
                    "generation {generation}: compact the merged region {merged} ({})",
                    scenario.placement.token()
                ),
                model.compact(merged).await
            );
        }
    }

    model
}

impl Model {
    /// The structural claim of the cell's domain relation, measured on the split targets.
    async fn check_split_shape(
        &mut self,
        targets: &[RegionId],
        domains: &[Domain],
        generation: usize,
    ) {
        let mut complaints = Vec::new();
        // A split of a non-empty object shares that object: at least one file id must be held by
        // two targets, otherwise the cell never reaches the shape it claims.
        let mut shared: BTreeMap<FileId, Vec<RegionId>> = BTreeMap::new();
        for region_id in targets {
            for file_id in self.file_set(*region_id) {
                shared.entry(file_id).or_default().push(*region_id);
            }
        }
        if !shared.values().any(|holders| holders.len() > 1) {
            complaints.push(format!(
                "generation {generation}: the split of {targets:?} shared no object between two \
                 targets, so the cell never exercises co-held references: {shared:?}"
            ));
        }
        // One half empty of rows: exactly one target domain must hold every committed row.
        if self.scenario.relation == Relation::OneHalfEmpty {
            let keys: KeySet = self.ledger.events.values().map(|event| event.key).collect();
            let holding: Vec<String> = domains
                .iter()
                .filter(|domain| !keys.is_disjoint(&domain.keys()))
                .map(|domain| domain.label())
                .collect();
            if holding.len() != 1 {
                complaints.push(format!(
                    "generation {generation}: relation {} claims exactly one target half holds \
                     rows, but the ledger's keys {} are spread over {holding:?}",
                    self.scenario.relation.token(),
                    keys_label(&keys),
                ));
            }
        }
        for complaint in complaints {
            self.record(Category::ScenarioShape, complaint);
        }
    }

    /// The identity claim of the cell's merge target.
    fn check_merge_identity(
        &mut self,
        scenario: &Scenario,
        source: RegionId,
        inputs: &[RegionId],
        merged: RegionId,
    ) {
        let expected = match scenario.identity {
            Identity::Fresh | Identity::SameFile => None,
            Identity::Original => Some(source),
            Identity::Half => Some(inputs[0]),
        };
        if let Some(expected) = expected
            && merged != expected
        {
            self.record(
                Category::ScenarioShape,
                format!(
                    "identity {} must keep the region id {expected}, but the merge published \
                     {merged}",
                    scenario.identity.token()
                ),
            );
        }
        if expected.is_none() && (inputs.contains(&merged) || merged == source) {
            self.record(
                Category::ScenarioShape,
                format!(
                    "identity {} must publish a fresh region id, but the merge reused {merged}",
                    scenario.identity.token()
                ),
            );
        }
    }

    /// The structural claim of the cell's domain relation, measured on the merged region.
    fn check_merge_shape(&mut self, scenario: &Scenario, merged: RegionId) {
        let target = self.domain_of(merged).keys();
        let mut observed: BTreeMap<&'static str, usize> = BTreeMap::new();
        let mut references = Vec::new();
        for (file_id, sst) in self.files.get(&merged).cloned().unwrap_or_default() {
            *observed
                .entry(relation_label(&target, &sst.creator))
                .or_default() += 1;
            references.push(format!("{file_id:?}(creator {})", keys_label(&sst.creator)));
        }
        let wanted: &[&str] = match scenario.relation {
            Relation::NestedSuperset => &["superset"],
            Relation::NestedSubset => &["subset"],
            Relation::Equal => &["equal"],
            Relation::DisjointHalves | Relation::OneHalfEmpty => &["equal", "superset", "subset"],
        };
        if !wanted.iter().any(|label| observed.contains_key(label)) {
            self.record(
                Category::ScenarioShape,
                format!(
                    "the merged region {merged} (domain {}) realized none of the {} relation(s) \
                     between its domain and its references' creator domains: observed {observed:?} \
                     over {references:?}",
                    keys_label(&target),
                    wanted.join("/"),
                ),
            );
        }
    }

    /// The identity claim of the idempotent second merge: the same references, unchanged.
    fn check_same_file(&mut self, merged: RegionId, before: &BTreeSet<FileId>) {
        let after = self.file_set(merged);
        if &after != before {
            self.record(
                Category::ScenarioShape,
                format!(
                    "the idempotent merge of {merged} changed its references: before {before:?}, \
                     after {after:?}"
                ),
            );
        }
    }
}

// ---------------------------------------------------------------------------------------
// Group runners and the report.
// ---------------------------------------------------------------------------------------

/// Summarizes the non-fatal observations of one group.
fn observation_summary(observations: &[Observation]) -> String {
    let mut counts: BTreeMap<&'static str, (usize, String, String)> = BTreeMap::new();
    for observation in observations {
        counts
            .entry(observation.category.name())
            .and_modify(|entry| entry.0 += 1)
            .or_insert((
                1,
                observation.scenario.clone(),
                format!("step {}: {}", observation.step, observation.detail),
            ));
    }
    counts
        .into_iter()
        .map(|(category, (count, scenario, detail))| {
            format!("[{category}] {count} observation(s), first in {scenario}: {detail}")
        })
        .collect::<Vec<_>>()
        .join("\n  ")
}

/// Runs every cell of one group and reports pass/fail per cell.
async fn run_group(group: Group) {
    common_telemetry::init_default_ut_logging();
    let all = table();
    let cells: Vec<Scenario> = all
        .iter()
        .filter(|scenario| scenario.group == group)
        .cloned()
        .collect();
    assert!(
        !cells.is_empty(),
        "group {} has no cells in the table",
        group.name()
    );

    let mut passed = 0usize;
    let mut failed = 0usize;
    let mut category_counts: BTreeMap<&'static str, usize> = BTreeMap::new();
    let mut observations: Vec<Observation> = Vec::new();
    let mut reports: Vec<String> = Vec::new();
    let mut headlines: Vec<String> = Vec::new();
    for scenario in &cells {
        let name = scenario.name();
        let model = run_scenario(scenario, usize::MAX).await;
        observations.extend(model.observations.iter().cloned());
        if model.failures.is_empty() {
            passed += 1;
            reports.push(format!("PASS {name} ({} published steps)", model.step));
            continue;
        }
        failed += 1;
        for failure in &model.failures {
            *category_counts.entry(failure.category.name()).or_default() += 1;
        }
        let first = &model.failures[0];
        let first_step = first.step;
        headlines.push(format!(
            "{name}: [{}] at step {first_step} ({} violation(s) over {} steps)",
            first.category.name(),
            model.failures.len(),
            model.step
        ));
        // Shrink: replay the shortest prefix ending at the first violating step and confirm it
        // reproduces the same violation before reporting it.
        let replay = run_scenario(scenario, first_step).await;
        let reproduced = replay
            .failures
            .iter()
            .any(|failure| failure.step == first_step && failure.category == first.category);
        let mut report = String::from("FAIL\n");
        for failure in model.failures.iter().take(MAX_REPORTED_FAILURES) {
            report.push_str(&format!("  {}\n", failure.describe().replace('\n', "\n  ")));
        }
        if model.failures.len() > MAX_REPORTED_FAILURES {
            report.push_str(&format!(
                "  ... {} more violation(s) of this cell\n",
                model.failures.len() - MAX_REPORTED_FAILURES
            ));
        }
        report.push_str(&format!(
            "  shrunk prefix: steps 1..={first_step} of {total} (reproduces the same violation: \
             {reproduced})\n  repro: {name}\n",
            total = model.step
        ));
        reports.push(report);
    }

    let summary = format!(
        "group {}: {} cells, {passed} passed, {failed} failed\n  failing invariants: \
         {category_counts:?}\n  observations:\n  {}\n  axes: {}\n\n{}",
        group.name(),
        cells.len(),
        observation_summary(&observations),
        table_summary(&all),
        reports.join("\n\n")
    );
    common_telemetry::info!("[merge-matrix] {summary}");
    println!("[merge-matrix] {summary}");
    assert_eq!(
        0,
        failed,
        "the merge matrix reported {failed} of {} cells failing in group {} (full diffs are \
         printed above):\n  failing invariants: {category_counts:?}\n  {}\n  {headlines}",
        cells.len(),
        group.name(),
        table_summary(&all),
        headlines = headlines.join("\n  "),
    );
}

#[tokio::test]
async fn test_matrix_generations_and_placement() {
    run_group(Group::Generations).await;
}

#[tokio::test]
async fn test_matrix_fan_out_and_fan_in() {
    run_group(Group::FanOut).await;
}

#[tokio::test]
async fn test_matrix_identity_and_order() {
    run_group(Group::Identity).await;
}

#[tokio::test]
async fn test_matrix_rows_and_relation() {
    run_group(Group::Rows).await;
}

#[tokio::test]
async fn test_matrix_interleaving() {
    run_group(Group::Interleaving).await;
}

/// Every axis value of the table must be covered by at least one cell.
#[tokio::test]
async fn test_matrix_table_covers_every_axis() {
    let cells = table();
    let summary = table_summary(&cells);
    for generations in [1usize, 2, 3] {
        assert!(
            cells.iter().any(|c| c.generations == generations),
            "the table must cover {generations} generation(s): {summary}"
        );
    }
    for placement in [
        Placement::None,
        Placement::OneSide,
        Placement::BothSides,
        Placement::BeforeSplit,
        Placement::BetweenMerges,
        Placement::AfterFinalMerge,
    ] {
        assert!(
            cells.iter().any(|c| c.placement == placement),
            "the table must cover placement {placement:?}: {summary}"
        );
    }
    for fan_out in [FanOut::Two, FanOut::Three, FanOut::Four] {
        assert!(
            cells.iter().any(|c| c.fan_out == fan_out),
            "the table must cover fan-out {fan_out:?}: {summary}"
        );
    }
    for fan_in in [FanIn::AllAtOnce, FanIn::SubsetsThenRest] {
        assert!(
            cells.iter().any(|c| c.fan_in == fan_in),
            "the table must cover fan-in {fan_in:?}: {summary}"
        );
    }
    for identity in [
        Identity::Fresh,
        Identity::Original,
        Identity::Half,
        Identity::SameFile,
    ] {
        assert!(
            cells.iter().any(|c| c.identity == identity),
            "the table must cover identity {identity:?}: {summary}"
        );
    }
    for relation in [
        Relation::DisjointHalves,
        Relation::OneHalfEmpty,
        Relation::NestedSuperset,
        Relation::NestedSubset,
        Relation::Equal,
    ] {
        assert!(
            cells.iter().any(|c| c.relation == relation),
            "the table must cover relation {relation:?}: {summary}"
        );
    }
    for order in [Order::Forward, Order::Reverse] {
        assert!(
            cells.iter().any(|c| c.order == order),
            "the table must cover order {order:?}: {summary}"
        );
    }
    for rows in [
        RowShape::LeftOnly,
        RowShape::RightOnly,
        RowShape::Both,
        RowShape::SameKeyDiffTs,
        RowShape::SameKeySameTs,
    ] {
        assert!(
            cells.iter().any(|c| c.rows == rows),
            "the table must cover row shape {rows:?}: {summary}"
        );
    }
    for interleaving in [
        Interleaving::SplitToCompaction,
        Interleaving::CompactionToMerge,
        Interleaving::Both,
    ] {
        assert!(
            cells.iter().any(|c| c.interleaving == interleaving),
            "the table must cover interleaving {interleaving:?}: {summary}"
        );
    }
}

// ---------------------------------------------------------------------------------------
// Range shape: repeated split -> merge of one object.
// ---------------------------------------------------------------------------------------

/// What one split -> merge cycle of one object observed.
///
/// The measured expression is the persisted range the reference carries: the creator's
/// `partition_expr` on this branch (there is no per-reference `owned_range`), which is what a
/// reader falls back to when it bounds the object.
struct CycleObservation {
    cycle: usize,
    /// Range depth of the shared object in each split target.
    split_depth: Vec<usize>,
    /// Range depth of the shared object in the merged region.
    merged_depth: usize,
    /// Whether the persisted manifest still round-trips through encode/decode.
    round_trip: Result<(), String>,
    /// The range of the merged reference, as keys.
    merged_range: String,
}

/// Repeatedly splits and merges the same object, measuring the persisted range shape.
async fn growth() -> Vec<CycleObservation> {
    let scenario = cell(
        Group::Rows,
        1,
        FanOut::Two,
        FanIn::AllAtOnce,
        Placement::None,
        Identity::Fresh,
        Relation::DisjointHalves,
        Order::Forward,
        RowShape::Both,
        Interleaving::None,
    );
    let mut model = Model::new(scenario, usize::MAX).await;
    let rows = vec![planned(1, 0, 1.0), planned(9, 1000, 1.5)];
    if let Err(err) = model.write_and_flush(&rows).await {
        panic!("the range-shape test could not write its rows: {err}");
    }
    model.refresh().await;
    let mut observations = Vec::new();
    for cycle in 1..=GROWTH_CYCLES {
        let live = model.live_regions();
        assert_eq!(1, live.len(), "a cycle starts from one live region");
        let source = live[0];
        let shared = *model
            .file_set(source)
            .iter()
            .next()
            .expect("the flush produced one object");
        let targets = match model.split_into(source, &[5], None).await {
            Ok(targets) => targets,
            Err(err) => panic!("cycle {cycle} could not split: {err}"),
        };
        model.step += 1;
        model.refresh().await;
        let split_depth: Vec<usize> = targets
            .iter()
            .map(|region_id| range_depth(&model, *region_id, shared))
            .collect();
        let merged = match model.merge(&targets, None, Domain::FULL).await {
            Ok(merged) => merged,
            Err(err) => panic!("cycle {cycle} could not merge: {err}"),
        };
        model.step += 1;
        model.refresh().await;
        let merged_depth = range_depth(&model, merged, shared);
        let merged_range = model
            .files
            .get(&merged)
            .and_then(|files| files.get(&shared))
            .map(|sst| keys_label(&sst.creator))
            .unwrap_or_else(|| "absent".to_string());
        let round_trip = round_trip_manifest(&model, merged).await;
        observations.push(CycleObservation {
            cycle,
            split_depth,
            merged_depth,
            round_trip,
            merged_range,
        });
    }
    observations
}

/// Depth of one reference's rendered range, read from the live version.
fn range_depth(model: &Model, region_id: RegionId, file_id: FileId) -> usize {
    let region = model
        .engine
        .get_region(region_id)
        .unwrap_or_else(|| panic!("region {region_id} disappeared"));
    let version = region.version_control.current();
    for level in version.version.ssts.levels() {
        if let Some(handle) = level.files.get(&file_id) {
            return match handle.meta_ref().partition_expr.as_deref() {
                Some(expr) => expr_depth(expr),
                None => 0,
            };
        }
    }
    panic!("region {region_id} does not reference {file_id:?}");
}

/// The comparable part of a persisted file set: level, row count and range of every reference.
fn manifest_file_set(
    files: &HashMap<FileId, crate::sst::file::FileMetaRef>,
) -> BTreeMap<FileId, (u8, u64, KeySet)> {
    files
        .iter()
        .map(|(file_id, meta)| {
            (
                *file_id,
                (
                    meta.level,
                    meta.num_rows,
                    range_keys(meta.partition_expr.as_deref()),
                ),
            )
        })
        .collect()
}

/// Encodes the region's persisted manifest and decodes it again.
async fn round_trip_manifest(model: &Model, region_id: RegionId) -> Result<(), String> {
    let region = model
        .engine
        .get_region(region_id)
        .ok_or_else(|| format!("region {region_id} disappeared"))?;
    let manifest = region.manifest_ctx.manifest().await;
    let bytes = serde_json::to_vec(&*manifest)
        .map_err(|err| format!("encoding the manifest failed: {err}"))?;
    let decoded = RegionManifest::decode(&bytes)
        .map_err(|err| format!("decoding the manifest failed: {err}"))?;
    let expected = manifest_file_set(&manifest.files);
    let round_tripped = manifest_file_set(&decoded.files);
    if expected != round_tripped {
        return Err(format!(
            "the manifest does not round-trip: before {expected:?}, after {round_tripped:?}"
        ));
    }
    Ok(())
}

/// Repeated split -> merge of one object must keep the persisted range bounded, and the manifest
/// must still decode.
#[tokio::test]
async fn test_union_range_shape_bounded_growth() {
    common_telemetry::init_default_ut_logging();
    let observations = growth().await;

    let mut report = String::new();
    for observation in &observations {
        report.push_str(&format!(
            "  cycle {}: split depth {:?}, merged depth {}, merged range {}, manifest round-trip \
             {}\n",
            observation.cycle,
            observation.split_depth,
            observation.merged_depth,
            observation.merged_range,
            match &observation.round_trip {
                Ok(()) => "ok".to_string(),
                Err(err) => err.clone(),
            }
        ));
    }
    common_telemetry::info!("[merge-matrix] range shape over {GROWTH_CYCLES} cycles:\n{report}");
    println!("[merge-matrix] range shape over {GROWTH_CYCLES} cycles:\n{report}");

    for observation in &observations {
        assert!(
            observation.round_trip.is_ok(),
            "cycle {}: the persisted manifest must still decode: {}",
            observation.cycle,
            observation.round_trip.clone().err().unwrap_or_default()
        );
        assert!(
            observation.merged_depth <= MAX_RANGE_DEPTH,
            "cycle {}: the persisted range depth {} exceeds the bound {MAX_RANGE_DEPTH}, so the \
             nested `Or` chain grows without bound:\n{report}",
            observation.cycle,
            observation.merged_depth
        );
    }
    let last = observations.last().expect("at least one cycle ran");
    assert!(
        last.merged_depth <= MAX_RANGE_DEPTH,
        "after {GROWTH_CYCLES} cycles the persisted range depth is {}, above the bound \
         {MAX_RANGE_DEPTH}:\n{report}",
        last.merged_depth
    );
}
