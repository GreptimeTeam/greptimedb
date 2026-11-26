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

//! The judgement of the repartition (split/merge) tests: engine-aware, **model-free**.
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
//! After every published step the driver scans the regions of the published routing and this
//! module asserts
//!
//! 1. the multiset of visible `event_id`s equals the ledger's committed multiset, each
//!    event exactly once (a multiset, not a count and not a sum, so a lost event and a
//!    duplicated event are both visible, including when their payload is zero), **and** no
//!    single region reads one event twice: a double in one region cancelled by a miss in
//!    another nets out in the global multiset, so "exactly once" is also stated per region;
//! 2. every live region only shows events whose partition key is inside that region's
//!    published domain, so the union in (1) cannot be satisfied by misplacing rows.
//!
//! It deliberately does **not** assert that two live regions hold different `file_id`s
//! (a legal split hands the same object to both) and does not compare `file_id`s inside
//! one manifest (a broken merge that keeps two copies of one file passes such a check).
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
//! | `model-authorization` | the *drivers'* statement (this module only names the category): a region shows no row outside the union of its own references' authorized sets. It needs the specification and a scan, so it lives in the drivers |
//! | `reference-bounds` | every reference a live holder shows authorizes only rows inside that holder's domain |
//! | `sequence-monotonicity` | per region the committed sequence never decreases across steps, and the persisted committed sequence never moves backwards |
//! | `physical-liveness` | every object still referenced by a live holder still reads back as a complete Parquet object, with the object id, the holding set and the step that dropped it reported when it does not |
//!
//! `reference-bounds` is reported but never fatal: it can only speak about what the engine
//! exposes, and on this branch one *legal* split already makes both halves reference an SST
//! whose creator domain is the whole table, so the category fires on every split by
//! construction. The model states the precise version of the same property per holder (which
//! keys each reference authorizes) and that statement *is* fatal. See the
//! `FileMeta::owned_range` TODO on [`SstRef::creator_domain`] for the field that would let
//! the observed check be as precise as the model.
//!
//! # Layering
//!
//! Physical objects, per-holder region references and the published routing are three
//! separate views. Only immutable snapshots ([`SstRef`]) cross a layer boundary; no layer
//! ever receives a `FileMeta` that another layer mutates.
//!
//! # What this layer is not
//!
//! Not the specification and not the driver: it reads no model state and issues no request,
//! it only judges the observations a driver hands it (and, for liveness, reads the objects).
//!
//! # The observable approximations in this file
//!
//! [`decode_domain`] and [`domain_of_json`] decode the harness' own restricted partition
//! grammar back into a [`Domain`], and [`DomainExpr`] is the other direction, so a driver can
//! ask the engine for the expression a model domain means. Both are engine-facing: the model
//! itself states domains as plain key ranges.

use std::collections::{BTreeMap, BTreeSet};

use datatypes::value::Value;
use object_store::ObjectStore;
use partition::expr::{Operand, PartitionExpr, RestrictedOp, col};
use store_api::region_request::PathType;
use store_api::storage::{FileId, RegionId};

use super::repartition_mbt_model::{Domain, KEY_COUNT, key_index, key_name};
use crate::sst::file::{LEVEL_SHARED, RegionFileId, SHARED_LEVEL_SLOT};
use crate::sst::location;

/// Accumulates the two half bounds of the harness' restricted partition grammar.
///
/// `None` means "unbounded on that side", which is what an unpartitioned region means.
#[derive(Debug, Default)]
pub(crate) struct DecodedBounds {
    lo: Option<usize>,
    hi: Option<usize>,
}

pub(crate) fn accumulate_bounds(expr: &PartitionExpr, bounds: &mut DecodedBounds) -> bool {
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
pub(crate) fn decode_domain(expr: &PartitionExpr) -> Option<Domain> {
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
pub(crate) fn domain_of_json(json: &str) -> Domain {
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

/// The engine-facing translation of a [`Domain`].
///
/// A driver turns a model domain into the partition expression the engine is asked to apply
/// (region create, staging manifest) and [`domain_of_json`] turns an observed one back.
pub(crate) trait DomainExpr {
    /// The partition expression `k >= key(lo) AND k < key(hi)`.
    ///
    /// `key(KEY_COUNT)` is `"k10"`, which sorts after every real key, so the full domain
    /// needs no unbounded upper bound.
    fn expr(&self) -> PartitionExpr;

    fn expr_json(&self) -> String;
}

impl DomainExpr for Domain {
    fn expr(&self) -> PartitionExpr {
        col("k")
            .gt_eq(Value::String(key_name(self.lo).into()))
            .and(col("k").lt(Value::String(key_name(self.hi).into())))
    }

    fn expr_json(&self) -> String {
        self.expr().as_json_str().unwrap()
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct Event {
    pub(crate) event_id: i64,
    pub(crate) key: usize,
    pub(crate) ts: i64,
    pub(crate) payload: f64,
}

/// A row the trace generator planned to write; becomes an [`Event`] once committed.
#[derive(Debug, Clone, Copy)]
pub(crate) struct PlannedRow {
    pub(crate) key: usize,
    pub(crate) ts: i64,
    pub(crate) payload: f64,
}

/// Independent model of all committed events.
#[derive(Debug, Default)]
pub(crate) struct Ledger {
    next_event_id: i64,
    /// Every committed event, keyed by its immutable id.
    pub(crate) events: BTreeMap<i64, Event>,
}

impl Ledger {
    /// Reserves an immutable event id.
    ///
    /// The id is a column value inside the row, so it must be known before the write; the
    /// event is only *recorded* once that write has committed.
    pub(crate) fn reserve(&mut self) -> i64 {
        let event_id = self.next_event_id;
        self.next_event_id += 1;
        event_id
    }

    /// Records a committed event under an id obtained from [`Ledger::reserve`].
    pub(crate) fn commit(&mut self, event_id: i64, planned: PlannedRow) -> Event {
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
    pub(crate) fn expected(&self) -> BTreeMap<i64, usize> {
        self.events.keys().map(|id| (*id, 1)).collect()
    }

    pub(crate) fn ids_in(&self, domain: Domain) -> BTreeSet<i64> {
        self.events
            .values()
            .filter(|event| domain.contains_key(event.key))
            .map(|event| event.event_id)
            .collect()
    }

    pub(crate) fn describe(&self, event_id: i64) -> String {
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SstRef {
    pub(crate) file_id: FileId,
    /// The region that physically wrote the object (not necessarily its holder).
    pub(crate) owner: RegionId,
    pub(crate) level: u8,
    /// The storage slot the reference sits in: `0`, `1` or [`SHARED_LEVEL_SLOT`].
    pub(crate) slot_level: u8,
    pub(crate) num_rows: u64,
    /// The reference's admission sequence (`FileMeta::sequence`).
    pub(crate) sequence: Option<u64>,
    /// The domain of the region that created the object (`FileMeta::partition_expr`).
    ///
    /// This is the range of rows the reference let a reader see for as long as a reference
    /// carried no row authorization of its own.
    ///
    /// `None` is a **legacy** object: a file written before the creating expression was
    /// recorded, whose provenance therefore cannot be established. The remap treats such a
    /// file as the region's own and the read path narrows it by the region's expression
    /// (`should_skip_region_partition` only skips the region predicate when the file's
    /// expression *is* the region's), so it is not the same state as "created by the whole
    /// table" and must not be reported as one.
    ///
    /// TODO(repart-mbt): this branch has no `FileMeta::owned_range`, so the authorized range
    /// is reported as the creator domain. Once `FileMeta::owned_range` exists, the
    /// authorized range is `creator_domain ∩ owned_range`, and `owned_range` itself must be
    /// inside `creator_domain`.
    pub(crate) creator_domain: Option<Domain>,
}

/// Layer 2: one region's own manifest view of the objects it references.
#[derive(Debug, Default, Clone)]
pub(crate) struct HolderView {
    /// The live version's file set (`Version::ssts`).
    pub(crate) files: BTreeMap<FileId, SstRef>,
    /// The persisted manifest's file set (`RegionManifest::files`), for the
    /// manifest/version consistency check.
    pub(crate) manifest: BTreeMap<FileId, ManifestRef>,
}

/// The persisted manifest's view of one reference.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ManifestRef {
    pub(crate) level: u8,
    pub(crate) sequence: Option<u64>,
    pub(crate) num_rows: u64,
}

/// What one region actually applied, captured per step.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct AppliedState {
    /// The partition domain the region applied (`RegionMetadata::partition_expr`).
    pub(crate) domain: Domain,
    /// `VersionControlData::committed_sequence`.
    pub(crate) committed: u64,
    /// `VersionControlData::last_sequence`.
    pub(crate) last_sequence: u64,
    /// `RegionManifest::committed_sequence`, the persisted committed sequence.
    pub(crate) manifest_committed: Option<u64>,
    /// The region currently accepts writes, i.e. it is the region that serves its domain.
    pub(crate) writable: bool,
}

/// Layer 1 aggregate: which holders reference each physical object.
///
/// Derived by observing the per-holder views; an object referenced by more than one
/// holder is *not* an error by itself (a legal split produces exactly that).
#[derive(Debug, Default, Clone)]
pub(crate) struct PhysicalView {
    pub(crate) holders_of: BTreeMap<FileId, BTreeSet<RegionId>>,
}

/// What one region had already reached, so a later step can only move forward.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct SequenceHighWater {
    /// The highest `VersionControlData::committed_sequence` observed.
    pub(crate) committed: u64,
    /// The highest `VersionControlData::last_sequence` observed.
    pub(crate) last: u64,
    /// The highest `RegionManifest::committed_sequence` observed.
    pub(crate) persisted: u64,
}

/// One invariant category. Each is reported separately.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum Category {
    /// The visible-row oracle (not an invariant below, reported for context).
    Visibility,
    ManifestConsistency,
    /// The model-side statement: a region shows no row outside the union of its references'
    /// authorized sets.
    ModelAuthorization,
    ReferenceBounds,
    SequenceMonotonicity,
    PhysicalLiveness,
    /// The chaos lever itself: a perturbation that never reached the engine, so the step
    /// proved nothing about the state it was meant to create. Reported by the chaos driver
    /// only, and fatal, because a scenario that silently stops injecting is a hole in the
    /// coverage rather than a passing step.
    Perturbation,
}

impl Category {
    pub(crate) fn name(self) -> &'static str {
        match self {
            Category::Visibility => "visibility",
            Category::ManifestConsistency => "manifest-consistency",
            Category::ModelAuthorization => "model-authorization",
            Category::ReferenceBounds => "reference-bounds",
            Category::SequenceMonotonicity => "sequence-monotonicity",
            Category::PhysicalLiveness => "physical-liveness",
            Category::Perturbation => "perturbation",
        }
    }

    /// Whether a violation of this category fails a trace.
    ///
    /// `reference-bounds` cannot: on this branch one legal split already makes both halves
    /// reference an object whose creator domain is the whole table, so the category fires on
    /// every split by construction. It is still checked and reported after every step.
    pub(crate) fn fatal(self) -> bool {
        !matches!(self, Category::ReferenceBounds)
    }
}

/// One violation, tagged with its category.
#[derive(Debug, Clone)]
pub(crate) struct Violation {
    pub(crate) category: Category,
    pub(crate) detail: String,
}

pub(crate) fn format_violations(violations: &[Violation]) -> String {
    violations
        .iter()
        .map(|violation| format!("  [{}] {}", violation.category.name(), violation.detail))
        .collect::<Vec<_>>()
        .join("\n")
}

/// Renders an `event id -> copies` multiset as `{e0, e3x2}`.
pub(crate) fn render_multiset(counts: &BTreeMap<i64, u64>) -> String {
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

/// The cross-step bookkeeping of the judgement.
///
/// The observed views themselves (routing, holder views, applied state, physical objects)
/// stay with the driver that collected them: only what has to remember a previous step lives
/// here. Nothing in this type is derived from the specification.
#[derive(Debug, Default)]
pub(crate) struct Oracle {
    /// The highest committed and last-allocated sequences observed per region (sequence
    /// monotonicity). The two are tracked apart: a region that inherits files without
    /// committing writes legitimately starts with `last_sequence` above `committed_sequence`.
    seq_high_water: BTreeMap<RegionId, SequenceHighWater>,
    /// The live-holder set of every object at the previous step.
    prev_objects: BTreeMap<(RegionId, FileId), BTreeSet<RegionId>>,
    /// The step at which an object stopped being referenced by a live holder, with the
    /// holder set before and after.
    dropped_at: BTreeMap<(RegionId, FileId), (usize, BTreeSet<RegionId>, BTreeSet<RegionId>)>,
}

impl Oracle {
    /// The oracle. Returns a report when the invariant is violated, `None` otherwise.
    pub(crate) fn visibility(
        &self,
        ledger: &Ledger,
        routing: &BTreeMap<RegionId, Domain>,
        holders: &BTreeMap<RegionId, HolderView>,
        physical: &PhysicalView,
        observed: &BTreeMap<RegionId, BTreeMap<i64, usize>>,
        ctx: &str,
    ) -> Option<String> {
        let live: Vec<RegionId> = routing.keys().copied().collect();
        let mut visible: BTreeMap<i64, usize> = BTreeMap::new();
        let mut per_region: Vec<(RegionId, BTreeMap<i64, usize>)> = Vec::new();
        let mut misplaced: Vec<String> = Vec::new();
        // One region reading the same event twice. The global multiset cannot see it: one
        // event read twice in one region plus another event missing everywhere nets to the
        // same total, so "each event exactly once" has to be stated per region as well.
        let mut duplicated_in_region: Vec<String> = Vec::new();
        for (region_id, counts) in observed {
            let domain = *routing
                .get(region_id)
                .unwrap_or_else(|| panic!("region {region_id} is not published"));
            let allowed = ledger.ids_in(domain);
            for event_id in counts.keys() {
                if !allowed.contains(event_id) {
                    misplaced.push(format!(
                        "region {} (domain [{}, {})) shows {} which the ledger does not place \
                         in that domain",
                        region_id,
                        key_name(domain.lo),
                        key_name(domain.hi),
                        ledger.describe(*event_id)
                    ));
                }
            }
            for (event_id, count) in counts {
                if *count > 1 {
                    duplicated_in_region.push(format!(
                        "region {} (domain [{}, {})) reads {} x{count}",
                        region_id,
                        key_name(domain.lo),
                        key_name(domain.hi),
                        ledger.describe(*event_id)
                    ));
                }
                *visible.entry(*event_id).or_default() += count;
            }
            per_region.push((*region_id, counts.clone()));
        }

        let expected = ledger.expected();
        let duplicated: Vec<String> = visible
            .iter()
            .filter(|(_, count)| **count > 1)
            .map(|(event_id, count)| format!("{} x{count}", ledger.describe(*event_id)))
            .collect();
        let missing: Vec<String> = expected
            .keys()
            .filter(|event_id| !visible.contains_key(*event_id))
            .map(|event_id| ledger.describe(*event_id))
            .collect();
        let unexpected: Vec<String> = visible
            .keys()
            .filter(|event_id| !expected.contains_key(*event_id))
            .map(|event_id| ledger.describe(*event_id))
            .collect();

        if duplicated.is_empty()
            && missing.is_empty()
            && unexpected.is_empty()
            && misplaced.is_empty()
            && duplicated_in_region.is_empty()
        {
            return None;
        }

        let physical: Vec<String> = physical
            .holders_of
            .iter()
            .map(|(file_id, holders)| format!("{file_id:?} held by {holders:?}"))
            .collect();
        let references: Vec<String> = holders
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
             \x20 events one region reads twice: {duplicated_in_region:?}\n\
             \x20 missing event ids: {missing:?}\n\
             \x20 unexpected event ids: {unexpected:?}\n\
             \x20 misplaced rows: {misplaced:?}\n\
             \x20 per-region visible multisets: {per_region:?}\n\
             \x20 physical objects: {physical:?}\n\
             \x20 holder references: {references:?}"
        ))
    }

    /// Manifest consistency: a holder's persisted manifest file set and its live version
    /// file set must describe the same references.
    pub(crate) fn manifest_consistency(
        &self,
        holders: &BTreeMap<RegionId, HolderView>,
        ctx: &str,
        violations: &mut Vec<Violation>,
    ) {
        for (region_id, view) in holders {
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
    pub(crate) fn routing_applied(
        &self,
        routing: &BTreeMap<RegionId, Domain>,
        applied: &BTreeMap<RegionId, AppliedState>,
        ctx: &str,
        violations: &mut Vec<Violation>,
    ) {
        for (region_id, domain) in routing {
            let Some(state) = applied.get(region_id) else {
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
        for (region_id, state) in applied {
            if routing.contains_key(region_id) || !state.writable {
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
    pub(crate) fn reference_bounds(
        &self,
        routing: &BTreeMap<RegionId, Domain>,
        holders: &BTreeMap<RegionId, HolderView>,
        physical: &PhysicalView,
        ctx: &str,
        violations: &mut Vec<Violation>,
    ) {
        for region_id in routing.keys().copied() {
            let holder_domain = routing[&region_id];
            let Some(view) = holders.get(&region_id) else {
                continue;
            };
            for (file_id, sst) in &view.files {
                // A legacy object records no creator, so it states no bound: the region
                // predicate still narrows it, but nothing here says which keys it holds.
                let Some(creator_domain) = sst.creator_domain else {
                    continue;
                };
                if holder_domain.contains_domain(&creator_domain) {
                    continue;
                }
                let holders = physical
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
                        key_name(creator_domain.lo),
                        key_name(creator_domain.hi),
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
    pub(crate) fn sequence_monotonicity(
        &mut self,
        applied: &BTreeMap<RegionId, AppliedState>,
        ctx: &str,
        violations: &mut Vec<Violation>,
    ) {
        for (region_id, state) in applied {
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
        for (region_id, state) in applied {
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
    pub(crate) async fn physical_liveness(
        &mut self,
        live: &[RegionId],
        holders: &BTreeMap<RegionId, HolderView>,
        reader: &SstReader<'_>,
        step: usize,
        ctx: &str,
        violations: &mut Vec<Violation>,
    ) {
        let mut objects: BTreeMap<(RegionId, FileId), BTreeSet<RegionId>> = BTreeMap::new();
        for region_id in live.iter().copied() {
            let Some(view) = holders.get(&region_id) else {
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
                    .insert(*object, (step, previous.clone(), holders.clone()));
            }
        }
        // An object that lost every live holder keeps the step that dropped it as well.
        for (object, previous) in &self.prev_objects {
            if objects.contains_key(object) {
                continue;
            }
            self.dropped_at
                .insert(*object, (step, previous.clone(), BTreeSet::new()));
        }

        for (object, holders) in &objects {
            let (creator, file_id) = *object;
            let path = reader.path(creator, file_id);
            let Err(reason) = reader.read(&path).await else {
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
}

/// The object store the SSTs live in, addressed the way a holder of a reference needs it.
pub(crate) struct SstReader<'a> {
    pub(crate) store: &'a ObjectStore,
    pub(crate) table_dir: &'a str,
    pub(crate) path_type: PathType,
}

impl SstReader<'_> {
    /// The object-store path of one physical object, resolved through its creator region.
    pub(crate) fn path(&self, creator: RegionId, file_id: FileId) -> String {
        location::sst_file_path(
            self.table_dir,
            RegionFileId::new(creator, file_id),
            self.path_type,
        )
    }

    /// Reads one physical object, i.e. what a holder of that reference needs to scan it.
    ///
    /// Reading the bytes (not just stat-ing them) is what makes this a liveness check: a
    /// deleted, truncated or foreign object fails here even though a stale metadata entry
    /// still lists it.
    pub(crate) async fn read(&self, path: &str) -> Result<u64, String> {
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
}
