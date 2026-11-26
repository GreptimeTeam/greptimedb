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

//! The specification state of the repartition (split/merge) model-based test: **pure**.
//!
//! # Scheme B: a reference is a file, and the domain is the only gate
//!
//! [`RefModel`] is the executable specification of *scheme B*, an alternative to per-reference
//! row authorization (scheme A, `FileMeta::owned_range`). It is mutated only by the transitions
//! below and never by an engine observation.
//!
//! - A *published region* has a domain (a range of the trace's own keys), a set of
//!   *references*, and the keys of the rows it wrote and has not flushed yet (a memtable is
//!   not a physical file). A reference in scheme B is the tuple
//!   `(file id, creator domain, level)` — **there is no per-reference row range**. The creator
//!   domain is a property of the physical file (its *stamp*), the level is the level the
//!   reference is registered at, and no transition reads the level: it is carried because it
//!   is part of B's reference and the reports print it.
//! - `write` puts a row inside the region's own domain only. An out-of-domain write is not a
//!   reachable user state, so the model does not carry a rule for it: [`RefModel::note_write`]
//!   rejects it instead of inventing a placement.
//! - `flush` turns a region's memtable into one new physical file stamped with the region's
//!   domain, registered in that region's L0 ([`Level::L0`]).
//! - `split(region, [d1, d2])` requires `d1`, `d2` disjoint with `d1 ∪ d2 == region.domain`, and
//!   both targets inherit **every** reference unchanged: same file ids, same levels.
//! - `compact(region)`: for a reference whose stamp differs from the region's domain the rows
//!   outside that domain are masked; for a reference whose stamp equals the domain all of its
//!   rows are taken. The output is a *new* file stamped with the region's domain, and the
//!   consumed references leave **that region only**.
//! - `merge(inputs, survivor)` is allowed **only if** every input's every reference has
//!   `creator_domain == that input's domain`; otherwise the model expects a **refusal** naming
//!   every offending `(region, file, creator)`. On success the survivor holds the union of the
//!   inputs' references, deduplicated by file id.
//! - A **legacy** file records no creator at all (`FileMeta::partition_expr == None`, a file
//!   written before the expression was persisted). That proves nothing about provenance — it can
//!   hold the rows of both children of an earlier split — so a merge refuses it exactly like a
//!   file stamped by another region (#10646), while a scan still narrows it by the holder's
//!   domain, because the region predicate is only dropped when the file's expression *is* the
//!   region's. [`RefModel::forget_creator`] is the transition that reaches that state.
//! - Visibility: a reference contributes the rows whose key lies in the **region's domain**,
//!   one physical copy per row, plus the rows of the region's unflushed memtable.
//!
//! # Where B differs from scheme A
//!
//! A keeps a shared file readable by narrowing each reference; B keeps the reference whole and
//! refuses to merge two holders of one file until both have materialized their own rows. The
//! consequence is that a split no longer produces a state a merge can consume: the duplicate
//! that A has to mask per reference is unreachable in B, because the merge that would create it
//! is refused and, once both halves compacted, each row lives in exactly one file.
//!
//! # What this layer is not
//!
//! Not the judgement and not the driver: it never performs I/O, never talks to the engine and
//! knows no mito2 type (the region id is a generic [`RegionKey`], the file ids are its own).
//!
//! # Model-side assertion (separate from the ledger oracle)
//!
//! [`RefModel::authorized_keys`] is the statement the drivers assert: what a region may show is
//! bounded by its own domain, because in B a reference hands over the whole file and only the
//! region's domain decides which of its rows are visible. The statement is deliberately weaker
//! than scheme A's per-reference one — B has nothing narrower to state — and it is still an
//! independent check: the model derives it from the transitions above, the ledger oracle from
//! the ledger, and neither hands a mutable object to the other.

use std::collections::{BTreeMap, BTreeSet};

/// Number of distinct partition-key values (`k00..k09`).
pub(crate) const KEY_COUNT: usize = 10;

/// The region-id vocabulary the model keys its state by.
///
/// The model is deliberately generic over it: it only ever needs to tell its own regions
/// apart, so it states that much and nothing about the engine's region identity type.
/// Blanket-implemented, so a caller passes whatever id it has (an integer in the unit tests
/// below, the engine's own `RegionId` in the drivers).
pub(crate) trait RegionKey: Copy + Ord + std::fmt::Display {}

impl<T: Copy + Ord + std::fmt::Display> RegionKey for T {}

/// Canonical name of the partition key value `idx`.
pub(crate) fn key_name(idx: usize) -> String {
    format!("k{idx:02}")
}

/// A half-open key domain `[lo, hi)` over key indices.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Domain {
    pub(crate) lo: usize,
    pub(crate) hi: usize,
}

impl Domain {
    /// The whole table domain.
    pub(crate) const FULL: Domain = Domain {
        lo: 0,
        hi: KEY_COUNT,
    };

    pub(crate) fn new(lo: usize, hi: usize) -> Domain {
        assert!(lo <= hi && hi <= KEY_COUNT, "invalid domain [{lo}, {hi})");
        Domain { lo, hi }
    }

    pub(crate) fn contains_key(&self, key: usize) -> bool {
        self.lo <= key && key < self.hi
    }

    /// Whether every key of `other` is also a key of `self`.
    pub(crate) fn contains_domain(&self, other: &Domain) -> bool {
        self.lo <= other.lo && other.hi <= self.hi
    }

    /// The keys of this domain.
    pub(crate) fn keys(&self) -> BTreeSet<usize> {
        (self.lo..self.hi).collect()
    }

    /// The `[k00, k05)` form the reports use.
    pub(crate) fn label(&self) -> String {
        format!("[{}, {})", key_name(self.lo), key_name(self.hi))
    }
}

/// The key index encoded by a partition key value; `"k10"` (the upper bound of the full
/// domain) is index [`KEY_COUNT`].
pub(crate) fn key_index(name: &str) -> Option<usize> {
    name.strip_prefix('k')?.parse().ok()
}

/// Renders a key set as the `k03, k07` form the reports use.
pub(crate) fn describe_keys(keys: &BTreeSet<usize>) -> String {
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

/// The level a reference is registered at.
///
/// Part of scheme B's reference tuple and printed by the reports; no transition reads it, so
/// it can never hide a rule.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum Level {
    /// The region's own flush output.
    L0,
    /// The output of a region compaction on the holding region.
    Compacted,
    /// An imported object, re-registered at the shared level.
    Shared,
}

impl Level {
    pub(crate) fn name(self) -> &'static str {
        match self {
            Level::L0 => "L0",
            Level::Compacted => "compacted",
            Level::Shared => "shared",
        }
    }
}

/// One event's physical presence inside a model file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct FileRow {
    /// The partition key of that event's row.
    pub(crate) key: usize,
    /// How many physical copies of the row the file holds. A compaction of a region that
    /// already sees one event twice writes both rows, so a file can legally hold several
    /// copies of one event.
    pub(crate) copies: u64,
}

/// One physical file of the specification: the rows it physically stores and the domain of
/// the region that produced it (its *stamp*).
///
/// A file stamped `D` only ever holds rows inside `D`: a flush writes only in-domain rows, a
/// compaction masks what lies outside its own domain, and neither a split nor a merge creates
/// a file. That invariant is why "a reference whose stamp equals the domain takes all of its
/// rows" and "takes the rows inside the domain" coincide on every reachable state; the
/// transition states the first form because that is the rule B states.
#[derive(Debug, Clone)]
pub(crate) struct PhysicalFile {
    /// The domain of the region that created the file, or `None` for a **legacy** file: one
    /// written before the creating expression was persisted, so its provenance cannot be
    /// established at all.
    ///
    /// `None` is not "created by the whole table": nothing can be compared with a region's
    /// domain, so [`RefModel::merge_verdict`] calls such a reference foreign — a merge needs
    /// proof that the region created the file, and an unrecorded stamp is no proof. The read path
    /// still narrows it by the region's expression: the region predicate is only dropped when the
    /// file's expression *is* the region's, which an unrecorded stamp never is.
    pub(crate) creator: Option<Domain>,
    /// Event id -> the row's physical presence in this file.
    pub(crate) rows: BTreeMap<i64, FileRow>,
}

/// The steps of the shape the model exists for, recorded when a transition reaches one so
/// every report can say what the engine was actually driven through.
pub(crate) const SHAPE_SPLIT_SHARED: &str = "split handed the same file reference to both halves";
pub(crate) const SHAPE_ONE_SIDE_COMPACTION: &str =
    "region compaction while another holder kept the file";
pub(crate) const SHAPE_MERGE_REFUSED_FOREIGN: &str =
    "merge refused because a participant held a foreign file";
pub(crate) const SHAPE_MERGE_RECOMBINES: &str =
    "merge the model allowed recombined more than one file into the survivor";

/// One published region of the specification.
#[derive(Debug, Clone)]
pub(crate) struct SpecRegion {
    /// The keys this region serves.
    pub(crate) domain: Domain,
    /// Model-minted physical-file id -> the level this region registers that file at. There is
    /// no per-reference row range: in scheme B the region's domain is the only filter, and it
    /// is advanced only by the transitions below.
    pub(crate) refs: BTreeMap<u64, Level>,
    /// Keys of the rows this region wrote and has not flushed. A memtable is not a
    /// physical file, so it is tracked apart from `refs`.
    pub(crate) memtable: BTreeSet<usize>,
}

impl SpecRegion {
    pub(crate) fn new(domain: Domain) -> SpecRegion {
        SpecRegion {
            domain,
            refs: BTreeMap::new(),
            memtable: BTreeSet::new(),
        }
    }

    /// The file ids this region references, i.e. the ids of the `(file, creator, level)` tuples
    /// of this region.
    pub(crate) fn ref_ids(&self) -> BTreeSet<u64> {
        self.refs.keys().copied().collect()
    }
}

/// One reference that blocks a merge under scheme B: the region that holds it, the file, and
/// the domain the file is stamped with.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ForeignRef<R> {
    pub(crate) region: R,
    pub(crate) file: u64,
    /// The creator's domain, or [`None`] for a legacy file whose creator was never recorded. It
    /// is still an offender: a merge needs proof that *this* region created the file, and an
    /// unrecorded creator is no proof.
    pub(crate) creator: Option<Domain>,
}

impl<R: RegionKey> ForeignRef<R> {
    /// The `(region, file, creator)` tuple a refusal has to name.
    pub(crate) fn describe(&self) -> String {
        match self.creator {
            Some(creator) => format!(
                "region {} holds f{} created by {}",
                self.region,
                self.file,
                creator.label()
            ),
            None => format!(
                "region {} holds f{} with no recorded creator",
                self.region, self.file
            ),
        }
    }
}

/// What scheme B says about a merge of the given inputs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum MergeVerdict<R> {
    /// Every participant holds only the files it created itself.
    Allowed,
    /// At least one participant holds a file someone else created, so the merge must be
    /// refused, naming every offending `(region, file, creator)`.
    Refused(Vec<ForeignRef<R>>),
}

impl<R: RegionKey> MergeVerdict<R> {
    pub(crate) fn is_allowed(&self) -> bool {
        matches!(self, MergeVerdict::Allowed)
    }

    /// The offending references, in region order; empty when the merge is allowed.
    pub(crate) fn offenders(&self) -> &[ForeignRef<R>] {
        match self {
            MergeVerdict::Allowed => &[],
            MergeVerdict::Refused(offenders) => offenders,
        }
    }

    /// The regions that have to compact before the merge can be retried.
    pub(crate) fn offending_regions(&self) -> BTreeSet<R> {
        self.offenders()
            .iter()
            .map(|offender| offender.region)
            .collect()
    }

    pub(crate) fn describe(&self) -> String {
        match self {
            MergeVerdict::Allowed => "the model allows the merge".to_string(),
            MergeVerdict::Refused(offenders) => format!(
                "the model expects a refusal naming {}",
                offenders
                    .iter()
                    .map(|offender| offender.describe())
                    .collect::<Vec<_>>()
                    .join("; ")
            ),
        }
    }
}

/// The specification: what every published region serves and which files it references.
///
/// Only the transitions on this type mutate it. Nothing here is derived from an engine
/// observation, and the ledger oracle never receives it, so model state and judgement share
/// no mutable object.
#[derive(Debug)]
pub(crate) struct RefModel<R: RegionKey> {
    /// Mints the opaque physical-file ids of this model (never a `FileId`).
    next_file: u64,
    /// Every currently published region.
    regions: BTreeMap<R, SpecRegion>,
    /// Model-minted file id -> the rows the file physically stores and its creator domain.
    /// The prediction below is evaluated on this graph, never on an engine observation.
    files: BTreeMap<u64, PhysicalFile>,
    /// Event ids of the rows a region wrote and has not flushed yet (its memtable), with the
    /// partition key of each. Parallel to [`SpecRegion::memtable`], which keeps only the keys
    /// the visibility rule needs.
    pending: BTreeMap<R, BTreeMap<i64, usize>>,
    /// The shapes the transitions have driven the engine through.
    shapes: BTreeSet<&'static str>,
}

impl<R: RegionKey> RefModel<R> {
    pub(crate) fn new() -> RefModel<R> {
        RefModel {
            next_file: 0,
            regions: BTreeMap::new(),
            files: BTreeMap::new(),
            pending: BTreeMap::new(),
            shapes: BTreeSet::new(),
        }
    }

    /// What one region's scan sees under scheme B.
    ///
    /// A reference hands over the whole file; the **region's own domain** is what decides which
    /// of the file's rows are visible, with no exception: B has no creator-expression shortcut
    /// and no per-reference range. Rows still in the memtable are visible too, and they are
    /// inside the domain by construction, because [`RefModel::note_write`] rejects an
    /// out-of-domain write.
    ///
    /// `copies` is the number of physical rows of that event the scan returns, so an event that
    /// two referenced files both carry is reported twice instead of being counted once.
    pub(crate) fn predict_visible_rows(&self, region: R) -> BTreeMap<i64, FileRow> {
        let mut visible: BTreeMap<i64, FileRow> = BTreeMap::new();
        let Some(state) = self.regions.get(&region) else {
            return visible;
        };
        let domain = state.domain;
        for file in state.refs.keys() {
            let Some(physical) = self.files.get(file) else {
                continue;
            };
            for (event_id, row) in &physical.rows {
                if !domain.contains_key(row.key) {
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
    pub(crate) fn predict_counts(&self, region: R) -> BTreeMap<i64, u64> {
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

    pub(crate) fn region(&self, region: R) -> Option<&SpecRegion> {
        self.regions.get(&region)
    }

    /// The file ids `region` references.
    pub(crate) fn ref_ids(&self, region: R) -> BTreeSet<u64> {
        self.regions
            .get(&region)
            .map(SpecRegion::ref_ids)
            .unwrap_or_default()
    }

    /// The live regions that hold `file`.
    pub(crate) fn holders_of(&self, file: u64) -> BTreeSet<R> {
        self.regions
            .iter()
            .filter(|(_, state)| state.refs.contains_key(&file))
            .map(|(region, _)| *region)
            .collect()
    }

    /// `Create` / publication: the region serves `domain` and holds nothing yet.
    pub(crate) fn publish(&mut self, region: R, domain: Domain) {
        self.regions.insert(region, SpecRegion::new(domain));
    }

    /// `write`: the rows land in the destination region's own memtable first.
    ///
    /// Scheme B states the write as in-domain: a row whose key lies outside the region's domain
    /// is not a reachable user state (the routing sends it elsewhere), so the model refuses to
    /// place it instead of stating a rule for a state no user can produce.
    pub(crate) fn note_write(&mut self, region: R, event_id: i64, key: usize) {
        let Some(state) = self.regions.get_mut(&region) else {
            return;
        };
        assert!(
            state.domain.contains_key(key),
            "the model was asked to write key {} into region {region} (domain {}): scheme B \
             models in-domain writes only, an out-of-domain row is not a reachable user state",
            key_name(key),
            state.domain.label()
        );
        state.memtable.insert(key);
        self.pending
            .entry(region)
            .or_default()
            .insert(event_id, key);
    }

    /// `flush`: the memtable becomes one new physical file stamped with this region's domain
    /// and registered in the region's L0. A flush with nothing to write publishes nothing.
    pub(crate) fn flushed(&mut self, region: R) {
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
                creator: Some(domain),
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
            .insert(file, Level::L0);
    }

    /// `split`: both targets inherit **every** reference of the source, unchanged.
    ///
    /// The two target domains must be disjoint and together cover the source domain, so the
    /// split only re-partitions the *serving* of the keys; neither target's reference set
    /// changes, and the source stops being published. A reference whose rows all belong to the
    /// other half is still inherited: B has no way to narrow it, which is exactly why such a
    /// region has to compact before it may merge.
    pub(crate) fn split(
        &mut self,
        source: R,
        left: R,
        left_domain: Domain,
        right: R,
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
        let mut covered: BTreeSet<usize> = left_domain.keys();
        assert!(
            right_domain.keys().is_disjoint(&covered),
            "the model split region {source} into overlapping domains {} and {}",
            left_domain.label(),
            right_domain.label()
        );
        covered.extend(right_domain.keys());
        assert_eq!(
            state.domain.keys(),
            covered,
            "the model split region {source} into {} and {}, which do not cover the source \
             domain {}",
            left_domain.label(),
            right_domain.label(),
            state.domain.label()
        );
        if !state.refs.is_empty() {
            // The shapes a case exists for: one object, two holders, no narrowing.
            self.shapes.insert(SHAPE_SPLIT_SHARED);
        }
        let source_domain = state.domain;
        self.regions.insert(
            left,
            SpecRegion {
                domain: left_domain,
                refs: state.refs.clone(),
                memtable: BTreeSet::new(),
            },
        );
        self.regions.insert(
            right,
            SpecRegion {
                domain: right_domain,
                refs: state.refs,
                memtable: BTreeSet::new(),
            },
        );
        // A repartition retires the source: its rows live in the targets now, so it is no
        // longer a published region and holds no references. No live region may serve its
        // domain until a later step publishes one (a merge of the targets, for instance).
        assert!(
            !self.regions.contains_key(&source),
            "the model split region {source} but it is still live: a split retires its source"
        );
        assert!(
            self.regions
                .values()
                .all(|region| region.domain != source_domain),
            "after splitting region {source}, a live region still serves its domain {}: the \
             source has to be retired before the targets are published",
            source_domain.label()
        );
    }

    /// What scheme B says about merging `inputs`: allowed only while every participant holds
    /// nothing but the files it created itself.
    pub(crate) fn merge_verdict(&self, inputs: &[R]) -> MergeVerdict<R> {
        let mut offenders = Vec::new();
        for input in inputs {
            let Some(state) = self.regions.get(input) else {
                continue;
            };
            for file in state.refs.keys() {
                let Some(physical) = self.files.get(file) else {
                    continue;
                };
                // A region may merge only the files it created itself, so the recorded creator
                // has to be exactly the region's domain. A legacy file records none, which proves
                // nothing: it can hold the rows of both children of an earlier split, so it is an
                // offender like any other foreign stamp.
                if physical.creator != Some(state.domain) {
                    offenders.push(ForeignRef {
                        region: *input,
                        file: *file,
                        creator: physical.creator,
                    });
                }
            }
        }
        if offenders.is_empty() {
            MergeVerdict::Allowed
        } else {
            MergeVerdict::Refused(offenders)
        }
    }

    /// `merge`: the survivor serves the whole table and holds the union of the inputs'
    /// references, deduplicated by file id.
    ///
    /// Scheme B allows this only while every input holds only its own files
    /// ([`RefModel::merge_verdict`]); otherwise the transition panics, because the engine is
    /// expected to refuse the merge instead of publishing a survivor that would have to widen
    /// a reference to read a file someone else wrote. Inputs that disagree about a file's level
    /// cannot occur on a reachable state (a split copies the level unchanged); if they did, the
    /// first input's level is kept — no rule reads it.
    pub(crate) fn merge(&mut self, inputs: &[R], survivor: R) {
        let verdict = self.merge_verdict(inputs);
        assert!(
            verdict.is_allowed(),
            "the model merged [{}] although {}; scheme B requires the merge to be refused until \
             the offending regions compacted",
            inputs
                .iter()
                .map(|input| input.to_string())
                .collect::<Vec<_>>()
                .join(", "),
            verdict.describe()
        );
        let survivor_domain = Domain::FULL;
        let mut refs: BTreeMap<u64, Level> = BTreeMap::new();
        for input in inputs {
            let Some(state) = self.regions.remove(input) else {
                continue;
            };
            assert!(
                state.memtable.is_empty(),
                "the model merged region {input} with unflushed rows; a merge must flush its \
                 inputs first"
            );
            for (file, level) in state.refs {
                refs.entry(file).or_insert(level);
            }
        }
        if refs.len() > 1 {
            self.shapes.insert(SHAPE_MERGE_RECOMBINES);
        }
        self.regions.insert(
            survivor,
            SpecRegion {
                domain: survivor_domain,
                refs,
                memtable: BTreeSet::new(),
            },
        );
    }

    /// Records that the model expects a refusal naming `offenders`, so the report can say the
    /// trace reached the state scheme B cannot merge.
    pub(crate) fn note_refused_merge(&mut self, verdict: &MergeVerdict<R>) {
        if !verdict.is_allowed() {
            self.shapes.insert(SHAPE_MERGE_REFUSED_FOREIGN);
        }
    }

    /// A **retried** merge: the same inputs are remapped again after the first attempt was
    /// interrupted by a close/reopen, into a fresh survivor.
    ///
    /// The inputs' persisted manifests did not move, so the retry republishes exactly the
    /// reference set the first attempt published: the same files at the same levels, in the
    /// same state. That is why the state of the previous survivor is what the retry's survivor
    /// starts from — no row is copied a second time and no reference moves.
    pub(crate) fn retry_merge(&mut self, previous_survivor: R, survivor: R) {
        let Some(state) = self.regions.remove(&previous_survivor) else {
            return;
        };
        self.regions.insert(survivor, state);
    }

    /// `region compaction`: legal even while another holder keeps a file this region holds.
    ///
    /// The output is a *new* physical file stamped with the compacting region's domain: for a
    /// reference whose stamp differs from that domain the rows outside it are masked (the
    /// region cannot see them), for a reference whose stamp equals it all rows are taken. The
    /// consumed references leave this region only, so every other holder keeps its own
    /// reference to the same files, and the region ends up holding nothing but a file it
    /// created itself — which is what makes it mergeable again.
    pub(crate) fn compact(&mut self, region: R) {
        let Some(state) = self.regions.get(&region) else {
            return;
        };
        if state.refs.is_empty() {
            return;
        }
        let domain = state.domain;
        let shared = state
            .refs
            .keys()
            .any(|file| self.holders_of(*file).len() > 1);
        if shared {
            self.shapes.insert(SHAPE_ONE_SIDE_COMPACTION);
        }

        let mut rows: BTreeMap<i64, FileRow> = BTreeMap::new();
        for file in state.refs.keys() {
            let Some(physical) = self.files.get(file) else {
                continue;
            };
            // A file whose stamp is unknown is not the region's own either, so all of its rows
            // are filtered by the region's domain — which is what the read path does with it.
            let self_stamped = physical.creator == Some(domain);
            for (event_id, row) in &physical.rows {
                if !self_stamped && !domain.contains_key(row.key) {
                    continue;
                }
                let entry = rows.entry(*event_id).or_insert(FileRow {
                    key: row.key,
                    copies: 0,
                });
                entry.copies += row.copies;
            }
        }
        let file = self.mint();
        self.files.insert(
            file,
            PhysicalFile {
                creator: Some(domain),
                rows,
            },
        );
        let state = self
            .regions
            .get_mut(&region)
            .expect("the compacted region is published");
        state.refs.clear();
        state.refs.insert(file, Level::Compacted);
    }

    /// `import`: a byte copy of one of the region's own flushed files, published under a new
    /// physical id at the shared level, while the original reference leaves the region in the
    /// same edit.
    ///
    /// The copy carries the original's stamp and rows, so the region keeps exactly the rows it
    /// had and only the identity of one reference changes.
    pub(crate) fn remint(&mut self, region: R) {
        let Some(file) = self
            .regions
            .get(&region)
            .and_then(|state| state.refs.keys().next().copied())
        else {
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
        state.refs.insert(new_file, Level::Shared);
    }

    /// A **legacy** reference: the region re-registers its file under a new physical id with
    /// the creator left unrecorded — the state a file written before the expression was
    /// persisted is in, which the remap admits (#10646).
    ///
    /// The rows and the level stay; only the stamp is gone. A merge afterwards refuses the
    /// reference ([`RefModel::merge_verdict`]), because an unrecorded creator does not prove the
    /// region produced the file; a compaction filters all of its rows by the region's domain,
    /// exactly as the read path does, and stamps its output with that domain. The new physical id is returned so a case can
    /// name the reference it created.
    pub(crate) fn forget_creator(&mut self, region: R) -> Option<u64> {
        let file = self.regions.get(&region)?.refs.keys().next().copied()?;
        let new_file = self.mint();
        if let Some(mut physical) = self.files.remove(&file) {
            physical.creator = None;
            self.files.insert(new_file, physical);
        }
        let state = self
            .regions
            .get_mut(&region)
            .expect("a region holding a file is published");
        let level = state.refs.remove(&file)?;
        state.refs.insert(new_file, level);
        Some(new_file)
    }

    /// The keys `region` may show: its own domain, plus the in-domain keys of its memtable
    /// (which is inside the domain by construction, because a write is in-domain).
    ///
    /// This is B's model-side statement: a reference hands over the whole file, so nothing
    /// narrower than the region's domain bounds what the region shows.
    pub(crate) fn authorized_keys(&self, region: R) -> BTreeSet<usize> {
        let Some(state) = self.regions.get(&region) else {
            return BTreeSet::new();
        };
        let mut keys = state.domain.keys();
        keys.extend(
            state
                .memtable
                .iter()
                .copied()
                .filter(|key| state.domain.contains_key(*key)),
        );
        keys
    }

    /// The shapes the transitions have driven so far, for the reports.
    pub(crate) fn shapes(&self) -> &BTreeSet<&'static str> {
        &self.shapes
    }

    /// One region's specification state, for a diff: its domain, its reference tuples
    /// `(file, creator, level)` and the keys of its unflushed rows.
    pub(crate) fn describe(&self, region: R) -> String {
        match self.regions.get(&region) {
            None => format!("region {region}: the specification has no state for it"),
            Some(state) => {
                let refs: Vec<String> = state
                    .refs
                    .iter()
                    .map(|(file, level)| match self.files.get(file) {
                        Some(physical) => format!(
                            "f{file}(creator {}, {})",
                            match physical.creator {
                                Some(domain) => domain.label(),
                                None => "unrecorded (legacy file)".to_string(),
                            },
                            level.name()
                        ),
                        None => format!("f{file}(<unknown file>, {})", level.name()),
                    })
                    .collect();
                format!(
                    "region {region} {}: refs {:?}, unflushed keys {}",
                    state.domain.label(),
                    refs,
                    describe_keys(&state.memtable)
                )
            }
        }
    }

    /// Every published region's specification state, for a diff.
    pub(crate) fn describe_all(&self) -> String {
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

#[cfg(test)]
mod tests {
    use super::*;

    /// The model's region-id vocabulary in these tests: plain integers, never a mito2 type.
    type Rid = u64;

    /// A model with one whole-domain region published as region `1`.
    fn published() -> RefModel<Rid> {
        let mut spec = RefModel::<Rid>::new();
        spec.publish(1, Domain::FULL);
        spec
    }

    /// Two events (`0` at `k02`, `1` at `k08`) flushed into one whole-domain file.
    fn one_shared_file() -> RefModel<Rid> {
        let mut spec = published();
        spec.note_write(1, 0, 2);
        spec.note_write(1, 1, 8);
        spec.flushed(1);
        spec
    }

    /// B's merge path over the shared-file shape: refuse, compact both halves, retry.
    fn one_shared_file_both_compacted() -> RefModel<Rid> {
        let mut spec = one_shared_file();
        spec.split(1, 2, Domain::new(0, 5), 3, Domain::new(5, 10));
        assert!(!spec.merge_verdict(&[2, 3]).is_allowed());
        spec.compact(2);
        spec.compact(3);
        assert!(spec.merge_verdict(&[2, 3]).is_allowed());
        spec
    }

    #[test]
    fn write_then_flush_publishes_an_l0_file_stamped_with_the_region_domain() {
        let mut spec = published();
        spec.note_write(1, 0, 2);
        assert_eq!(
            BTreeMap::from([(0, FileRow { key: 2, copies: 1 })]),
            spec.predict_visible_rows(1)
        );
        spec.flushed(1);
        assert_eq!(BTreeMap::from([(0, 1)]), spec.predict_counts(1));
        let state = spec.region(1).expect("published");
        assert_eq!(BTreeMap::from([(0, Level::L0)]), state.refs);
        assert_eq!(Some(Domain::FULL), spec.files[&0].creator);
        // The whole domain, because the region's own domain is the only gate.
        assert_eq!(Domain::FULL.keys(), spec.authorized_keys(1));
        assert_eq!(BTreeSet::from([1]), spec.holders_of(0));
        // A flush with nothing to write publishes no file.
        spec.flushed(1);
        assert_eq!(BTreeSet::from([1]), spec.holders_of(0));
    }

    #[test]
    #[should_panic(expected = "not a reachable user state")]
    fn a_write_outside_the_region_domain_is_refused_by_the_model() {
        let mut spec = RefModel::<Rid>::new();
        spec.publish(1, Domain::new(0, 5));
        spec.note_write(1, 0, 7);
    }

    #[test]
    fn a_legacy_reference_blocks_the_merge_and_is_still_narrowed_by_the_domain() {
        let mut spec = one_shared_file();
        // A file written before its creator was recorded: the stamp is unknown, the rows stay.
        let legacy = spec
            .forget_creator(1)
            .expect("the region holds a flushed file");
        assert_eq!(None, spec.files[&legacy].creator);
        assert_eq!(
            BTreeMap::from([(0, 1), (1, 1)]),
            spec.predict_counts(1),
            "an unknown stamp still leaves the region's domain as the only gate"
        );
        spec.split(1, 2, Domain::new(0, 5), 3, Domain::new(5, 10));
        // An unrecorded creator proves nothing, so the merge is refused until both halves have
        // materialized their own rows.
        let verdict = spec.merge_verdict(&[2, 3]);
        assert_eq!(
            MergeVerdict::Refused(vec![
                ForeignRef {
                    region: 2,
                    file: legacy,
                    creator: None,
                },
                ForeignRef {
                    region: 3,
                    file: legacy,
                    creator: None,
                },
            ]),
            verdict,
            "the refusal has to name both holders of the creator-less file"
        );
        assert!(
            verdict
                .describe()
                .contains("region 2 holds f1 with no recorded creator"),
            "the refusal has to say the creator is unrecorded: {}",
            verdict.describe()
        );
        // Each half still reads only the rows inside its own domain.
        assert_eq!(BTreeMap::from([(0, 1)]), spec.predict_counts(2));
        assert_eq!(BTreeMap::from([(1, 1)]), spec.predict_counts(3));
        spec.compact(2);
        spec.compact(3);
        assert!(
            spec.merge_verdict(&[2, 3]).is_allowed(),
            "after both halves compacted, each holds a file it stamped itself"
        );
        spec.merge(&[2, 3], 4);
        assert_eq!(
            BTreeMap::from([(0, 1), (1, 1)]),
            spec.predict_counts(4),
            "the survivor holds one materialized file per half, so each row is read once"
        );

        // Compacting a legacy reference takes the rows its own domain holds and stamps the
        // output with that domain, the same way a foreign stamp is handled.
        spec.split(4, 5, Domain::new(0, 5), 6, Domain::new(5, 10));
        spec.compact(5);
        let output = *spec
            .ref_ids(5)
            .iter()
            .next()
            .expect("the compaction output");
        assert_eq!(Some(Domain::new(0, 5)), spec.files[&output].creator);
        assert_eq!(BTreeMap::from([(0, 1)]), spec.predict_counts(5));
    }

    #[test]
    fn a_reference_only_contributes_the_rows_inside_the_holder_domain() {
        let mut spec = one_shared_file();
        spec.split(1, 2, Domain::new(0, 5), 3, Domain::new(5, 10));
        // Both halves inherit the same file reference, unchanged: no per-reference range.
        assert_eq!(BTreeSet::from([2, 3]), spec.holders_of(0));
        assert_eq!(
            BTreeMap::from([(0, Level::L0)]),
            spec.region(2).unwrap().refs
        );
        assert_eq!(
            BTreeMap::from([(0, Level::L0)]),
            spec.region(3).unwrap().refs
        );
        // The domain decides what each half sees.
        assert_eq!(BTreeMap::from([(0, 1)]), spec.predict_counts(2));
        assert_eq!(BTreeMap::from([(1, 1)]), spec.predict_counts(3));
        assert_eq!(Domain::new(0, 5).keys(), spec.authorized_keys(2));
        assert_eq!(Domain::new(5, 10).keys(), spec.authorized_keys(3));
        assert!(spec.shapes().contains(SHAPE_SPLIT_SHARED));
        assert!(spec.region(1).is_none());
    }

    #[test]
    fn split_hands_every_reference_to_both_targets_unchanged() {
        let mut spec = one_shared_file();
        spec.split(1, 2, Domain::new(0, 5), 3, Domain::new(5, 10));
        assert_eq!(
            spec.region(2).unwrap().refs,
            spec.region(3).unwrap().refs,
            "a split is a re-partition of the serving, not of the references"
        );
        assert_eq!(BTreeSet::from([0]), spec.ref_ids(2));
        assert_eq!(BTreeSet::from([0]), spec.ref_ids(3));
    }

    #[test]
    #[should_panic(expected = "do not cover the source domain")]
    fn a_split_that_leaves_a_gap_is_refused() {
        let mut spec = one_shared_file();
        spec.split(1, 2, Domain::new(0, 3), 3, Domain::new(5, 10));
    }

    #[test]
    #[should_panic(expected = "overlapping domains")]
    fn a_split_into_overlapping_domains_is_refused() {
        let mut spec = one_shared_file();
        spec.split(1, 2, Domain::new(0, 6), 3, Domain::new(5, 10));
    }

    #[test]
    fn merge_is_refused_while_a_participant_holds_a_foreign_file() {
        let mut spec = one_shared_file();
        spec.split(1, 2, Domain::new(0, 5), 3, Domain::new(5, 10));
        spec.compact(3);
        let verdict = spec.merge_verdict(&[2, 3]);
        assert_eq!(
            MergeVerdict::Refused(vec![ForeignRef {
                region: 2,
                file: 0,
                creator: Some(Domain::FULL),
            }]),
            verdict,
            "the refusal has to name the (region, file, creator) that blocks the merge"
        );
        assert!(
            verdict
                .describe()
                .contains("region 2 holds f0 created by [k00, k10)")
        );
        assert_eq!(BTreeSet::from([2]), verdict.offending_regions());
        spec.note_refused_merge(&verdict);
        assert!(spec.shapes().contains(SHAPE_MERGE_REFUSED_FOREIGN));
        // Both halves still hold what they held: a refusal must not move the specification.
        assert_eq!(BTreeSet::from([0]), spec.ref_ids(2));
        assert_eq!(1, spec.ref_ids(3).len());
    }

    #[test]
    #[should_panic(expected = "requires the merge to be refused")]
    fn merging_a_foreign_file_holder_is_not_a_transition_of_the_model() {
        let mut spec = one_shared_file();
        spec.split(1, 2, Domain::new(0, 5), 3, Domain::new(5, 10));
        spec.merge(&[2, 3], 4);
    }

    #[test]
    fn merge_after_both_halves_compacted_keeps_each_row_once() {
        let mut spec = one_shared_file_both_compacted();
        spec.merge(&[2, 3], 4);
        // The survivor holds the union of both halves' own files, deduplicated by file id.
        assert_eq!(2, spec.ref_ids(4).len());
        assert_eq!(Domain::FULL.keys(), spec.authorized_keys(4));
        // Each row lives in exactly one file: scheme B does not duplicate the shared row, and
        // the duplicate-row shape of scheme A cannot even be merged.
        assert_eq!(BTreeMap::from([(0, 1), (1, 1)]), spec.predict_counts(4));
        assert!(spec.shapes().contains(SHAPE_MERGE_RECOMBINES));
    }

    #[test]
    fn a_compaction_masks_the_rows_a_foreign_file_carries_outside_the_domain() {
        let mut spec = one_shared_file();
        spec.split(1, 2, Domain::new(0, 5), 3, Domain::new(5, 10));
        spec.compact(2);
        let file = *spec.ref_ids(2).iter().next().expect("the copy");
        // The copy is stamped with the compacting half's domain and holds only its row.
        assert_eq!(Some(Domain::new(0, 5)), spec.files[&file].creator);
        assert_eq!(
            BTreeMap::from([(0, FileRow { key: 2, copies: 1 })]),
            spec.files[&file].rows
        );
        assert_eq!(
            BTreeMap::from([(file, Level::Compacted)]),
            spec.region(2).unwrap().refs
        );
        // The other half still holds the original object, whole.
        assert_eq!(BTreeSet::from([3]), spec.holders_of(0));
        assert!(spec.shapes().contains(SHAPE_ONE_SIDE_COMPACTION));
    }

    #[test]
    fn a_compaction_of_a_self_stamped_file_takes_all_of_its_rows() {
        let mut spec = published();
        spec.note_write(1, 0, 2);
        spec.note_write(1, 1, 8);
        spec.flushed(1);
        spec.compact(1);
        let file = *spec.ref_ids(1).iter().next().expect("the copy");
        assert_eq!(
            BTreeMap::from([
                (0, FileRow { key: 2, copies: 1 }),
                (1, FileRow { key: 8, copies: 1 }),
            ]),
            spec.files[&file].rows
        );
        assert_eq!(BTreeMap::from([(0, 1), (1, 1)]), spec.predict_counts(1));
    }

    #[test]
    fn two_regions_holding_only_their_own_files_merge() {
        let mut spec = RefModel::<Rid>::new();
        spec.publish(1, Domain::new(0, 5));
        spec.publish(2, Domain::new(5, 10));
        spec.note_write(1, 0, 1);
        spec.note_write(2, 1, 7);
        spec.flushed(1);
        spec.flushed(2);
        assert!(spec.merge_verdict(&[1, 2]).is_allowed());
        spec.merge(&[1, 2], 3);
        assert_eq!(2, spec.ref_ids(3).len());
        assert_eq!(BTreeMap::from([(0, 1), (1, 1)]), spec.predict_counts(3));
    }

    #[test]
    fn the_survivor_of_a_merge_holds_foreign_files_again() {
        let mut spec = one_shared_file_both_compacted();
        spec.merge(&[2, 3], 4);
        // The survivor serves the whole domain but holds the halves' files, so a merge that
        // consumes it is refused until it compacts.
        let verdict = spec.merge_verdict(&[4]);
        assert!(!verdict.is_allowed(), "{verdict:?}");
        spec.compact(4);
        assert!(spec.merge_verdict(&[4]).is_allowed());
    }

    #[test]
    fn import_keeps_the_rows_and_only_changes_the_file_identity() {
        let mut spec = published();
        spec.note_write(1, 0, 3);
        spec.flushed(1);
        let before = spec.predict_counts(1);
        let before_keys = spec.authorized_keys(1);
        spec.remint(1);
        assert_eq!(before, spec.predict_counts(1));
        assert_eq!(before_keys, spec.authorized_keys(1));
        assert_eq!(BTreeSet::from([1]), spec.holders_of(1));
        assert!(spec.holders_of(0).is_empty());
        // The copy is registered at the shared level, still the region's own stamp.
        assert_eq!(
            BTreeMap::from([(1, Level::Shared)]),
            spec.region(1).unwrap().refs
        );
        assert_eq!(Some(Domain::FULL), spec.files[&1].creator);
    }

    #[test]
    fn retry_merge_republishes_the_reference_set_under_the_new_survivor() {
        let mut spec = one_shared_file_both_compacted();
        spec.merge(&[2, 3], 4);
        let merged = spec.predict_counts(4);
        let merged_refs = spec.ref_ids(4);
        spec.retry_merge(4, 5);
        assert_eq!(merged, spec.predict_counts(5));
        assert_eq!(merged_refs, spec.ref_ids(5));
        assert!(spec.region(4).is_none());
    }

    /// A half whose share of the target domain holds no physical row: the reference is still
    /// inherited (a split narrows nothing), it just contributes no row.
    #[test]
    fn a_split_target_whose_rows_all_stay_right_holds_the_reference_but_sees_nothing() {
        let mut spec = RefModel::<Rid>::new();
        spec.publish(1, Domain::new(0, 10));
        spec.note_write(1, 0, 8);
        spec.flushed(1);
        spec.split(1, 2, Domain::new(0, 5), 3, Domain::new(5, 10));
        assert_eq!(BTreeSet::from([0]), spec.ref_ids(2));
        assert!(spec.predict_counts(2).is_empty());
        assert_eq!(BTreeMap::from([(0, 1)]), spec.predict_counts(3));
        // A region that sees no row compacts into an output file holding none, and it then
        // holds a file of its own.
        spec.compact(2);
        assert!(spec.predict_counts(2).is_empty());
        assert!(spec.merge_verdict(&[2]).is_allowed());
        assert_eq!(BTreeMap::from([(0, 1)]), spec.predict_counts(3));
    }
}
