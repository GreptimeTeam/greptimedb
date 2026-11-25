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
//! # The specification state
//!
//! [`RefModel`] is the executable specification: an independent model of the table that is
//! mutated only by the transitions below and never by an engine observation.
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
//! judgement: neither the ledger oracle nor the invariant categories read it. The cases print
//! `predicted` next to `observed` per region and continue on a divergence instead of adjusting
//! the model to an observation.
//!
//! # What this layer is not
//!
//! Not the judgement and not the driver: it never performs I/O, never talks to the engine and
//! knows no mito2 type (the region id is a generic [`RegionKey`], the file ids are its own).
//!
//! # Model-side assertion (separate from the ledger oracle)
//!
//! [`RefModel::authorized_keys`] is the statement the drivers assert: a region's visible rows
//! are a subset of the union of its own references' authorized sets (plus the keys of its own
//! unflushed rows). That is "selecting a file does not authorize reading all of it", stated on
//! the model side: a split that hands both halves the same object still leaves each half
//! authorized to read only its own rows.
//!
//! This statement and the ledger oracle are independent: the model derives from the
//! transitions above only, the oracle from the ledger only, and neither hands a mutable
//! object to the other. The oracle keeps its exact meaning (each committed event exactly
//! once, nothing added, nothing misplaced); the model-side check can pass while the oracle
//! fails, because authorization is not counting.

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
}

/// The key index encoded by a partition key value; `"k10"` (the upper bound of the full
/// domain) is index [`KEY_COUNT`].
pub(crate) fn key_index(name: &str) -> Option<usize> {
    name.strip_prefix('k')?.parse().ok()
}

/// The keys one reference authorizes inside its own physical file.
///
/// `None` is the universe: every row of that physical file. A reference is stated in the
/// trace's own vocabulary — an opaque model-minted file id plus this set — never in an
/// implementation type (`FileId`, `PartitionExpr`, an owned range).
pub(crate) type KeyAuth = Option<BTreeSet<usize>>;

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
/// the region that produced it.
#[derive(Debug, Clone)]
pub(crate) struct PhysicalFile {
    /// The domain of the region that created the file, i.e. `FileMeta::partition_expr` on
    /// this branch.
    pub(crate) creator: Domain,
    /// Event id -> the row's physical presence in this file.
    pub(crate) rows: BTreeMap<i64, FileRow>,
}

/// The three steps of the shape the model exists for, recorded when a transition reaches
/// one so every report can say what the engine was actually driven through.
pub(crate) const SHAPE_SPLIT_SHARED: &str = "split handed the shared file to both halves";
pub(crate) const SHAPE_ONE_SIDE_COMPACTION: &str =
    "region compaction while another holder kept the file";
pub(crate) const SHAPE_MERGE_RECOMBINES: &str = "merge of both holders after that compaction";

/// One published region of the specification.
#[derive(Debug, Clone)]
pub(crate) struct SpecRegion {
    /// The keys this region serves.
    pub(crate) domain: Domain,
    /// Model-minted physical-file id -> the keys this region may read from that file.
    pub(crate) refs: BTreeMap<u64, KeyAuth>,
    /// Keys of the rows this region wrote and has not flushed. A memtable is not a
    /// physical file, so it is tracked apart from `refs` and counts as authorized.
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
}

/// The specification: what every published region serves and what its references authorize.
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
    /// the authorization statement needs.
    pending: BTreeMap<R, BTreeMap<i64, usize>>,
    /// The *engine's* reference set of each published region. A repartition copies every file
    /// of a source into every target, so a target may reference a file none of whose rows fall
    /// inside its domain; [`SpecRegion::refs`] is the tighter *authorization* view and drops
    /// such a reference. The prediction is stated on this set, because this is what the reader
    /// is handed.
    engine_refs: BTreeMap<R, BTreeSet<u64>>,
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
    ///   the drivers judge; comparing it with this prediction is exactly the per-reference
    ///   divergence the cases print;
    /// - rows still in the memtable are always filtered by the domain;
    /// - the transitions mirror the rest: `split` narrows a target to its domain (its
    ///   references keep pointing at the same objects), `merge` unions the inputs' references,
    ///   and compaction writes a new file holding the rows the compacting region *sees* while
    ///   removing the consumed references from that region only.
    ///
    /// `copies` is the number of physical rows of that event the scan returns, so an event that
    /// two referenced files both carry is reported twice instead of being counted once.
    pub(crate) fn predict_visible_rows(&self, region: R) -> BTreeMap<i64, FileRow> {
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
        self.engine_refs.insert(region, BTreeSet::new());
    }

    /// `write`: the rows land in the destination region's own memtable first.
    ///
    /// The key is recorded even when it falls outside the region's domain: write admission
    /// checks the partition *version* the region serves, never the row's partition value, so
    /// the engine accepts such a row and the prediction has to place it by rule.
    pub(crate) fn note_write(&mut self, region: R, event_id: i64, key: usize) {
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
    pub(crate) fn merge(&mut self, inputs: &[R], survivor: R) {
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
    pub(crate) fn retry_merge(&mut self, previous_survivor: R, survivor: R) {
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
    pub(crate) fn compact(&mut self, region: R) {
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
    pub(crate) fn remint(&mut self, region: R) {
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
    pub(crate) fn authorized_keys(&self, region: R) -> BTreeSet<usize> {
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

    /// The shapes the transitions have driven so far, for the reports.
    pub(crate) fn shapes(&self) -> &BTreeSet<&'static str> {
        &self.shapes
    }

    /// One region's specification state, for a diff.
    pub(crate) fn describe(&self, region: R) -> String {
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

    #[test]
    fn write_then_flush_publishes_one_authorized_file() {
        let mut spec = published();
        spec.note_write(1, 0, 2);
        assert_eq!(
            BTreeMap::from([(0, FileRow { key: 2, copies: 1 })]),
            spec.predict_visible_rows(1)
        );
        spec.flushed(1);
        assert_eq!(BTreeMap::from([(0, 1)]), spec.predict_counts(1));
        assert_eq!(BTreeSet::from([2]), spec.authorized_keys(1));
        assert_eq!(BTreeSet::from([1]), spec.holders_of(0));
        // A flush with nothing to write publishes no file.
        spec.flushed(1);
        assert_eq!(BTreeSet::from([1]), spec.holders_of(0));
        assert!(spec.holders_of(1).is_empty());
    }

    #[test]
    fn prediction_filters_the_memtable_by_the_region_domain() {
        let mut spec = RefModel::<Rid>::new();
        spec.publish(1, Domain::new(0, 5));
        // Write admission checks the partition version, not the row's value, so such a row
        // is accepted; the memtable scan still filters it by the domain.
        spec.note_write(1, 0, 7);
        assert!(spec.predict_counts(1).is_empty());
    }

    #[test]
    fn prediction_skips_the_partition_predicate_for_a_file_the_region_created() {
        let mut spec = RefModel::<Rid>::new();
        spec.publish(1, Domain::new(0, 5));
        spec.note_write(1, 0, 7);
        spec.flushed(1);
        // The file's creator domain equals the region's, so the reader drops the region
        // partition predicate and the out-of-domain row is visible.
        assert_eq!(BTreeMap::from([(0, 1)]), spec.predict_counts(1));
    }

    #[test]
    fn a_reference_only_contributes_the_rows_inside_the_holder_domain() {
        let mut spec = one_shared_file();
        spec.split(1, 2, Domain::new(0, 5), 3, Domain::new(5, 10));
        // Both halves hold the same object, but each authorization covers its own rows only.
        assert_eq!(BTreeSet::from([2, 3]), spec.holders_of(0));
        assert_eq!(BTreeSet::from([2]), spec.authorized_keys(2));
        assert_eq!(BTreeSet::from([8]), spec.authorized_keys(3));
        assert_eq!(BTreeMap::from([(0, 1)]), spec.predict_counts(2));
        assert_eq!(BTreeMap::from([(1, 1)]), spec.predict_counts(3));
        assert!(spec.shapes().contains(SHAPE_SPLIT_SHARED));
        assert!(spec.region(1).is_none());
    }

    #[test]
    fn merge_after_one_side_compaction_duplicates_the_shared_row() {
        let mut spec = one_shared_file();
        spec.split(1, 2, Domain::new(0, 5), 3, Domain::new(5, 10));
        spec.compact(2);
        assert!(spec.shapes().contains(SHAPE_ONE_SIDE_COMPACTION));
        // The compacting half keeps its rows; the other half keeps the original object.
        assert_eq!(BTreeMap::from([(0, 1)]), spec.predict_counts(2));
        assert_eq!(BTreeSet::from([3]), spec.holders_of(0));
        assert_eq!(BTreeSet::from([2]), spec.authorized_keys(2));

        spec.merge(&[2, 3], 4);
        assert!(spec.shapes().contains(SHAPE_MERGE_RECOMBINES));
        assert_eq!(BTreeSet::from([2, 8]), spec.authorized_keys(4));
        // The survivor references both the original object and the copy, so the event the
        // compaction rewrote is read twice: the row the model exists for.
        assert_eq!(BTreeMap::from([(0, 2), (1, 1)]), spec.predict_counts(4));
    }

    #[test]
    fn import_keeps_the_rows_and_only_changes_the_file_identity() {
        let mut spec = published();
        spec.note_write(1, 0, 3);
        spec.flushed(1);
        let before = spec.predict_counts(1);
        spec.remint(1);
        assert_eq!(before, spec.predict_counts(1));
        assert_eq!(BTreeSet::from([3]), spec.authorized_keys(1));
        assert_eq!(BTreeSet::from([1]), spec.holders_of(1));
        assert!(spec.holders_of(0).is_empty());
    }

    #[test]
    fn retry_merge_republishes_the_reference_set_under_the_new_survivor() {
        let mut spec = one_shared_file();
        spec.split(1, 2, Domain::new(0, 5), 3, Domain::new(5, 10));
        spec.merge(&[2, 3], 4);
        let merged = spec.predict_counts(4);
        let merged_keys = spec.authorized_keys(4);
        spec.retry_merge(4, 5);
        assert_eq!(merged, spec.predict_counts(5));
        assert_eq!(merged_keys, spec.authorized_keys(5));
        assert!(spec.region(4).is_none());
    }

    #[test]
    fn an_empty_split_target_is_published_without_authorization() {
        let mut spec = RefModel::<Rid>::new();
        spec.publish(1, Domain::new(0, 10));
        spec.note_write(1, 0, 8);
        spec.flushed(1);
        spec.split(1, 2, Domain::new(0, 5), 3, Domain::new(5, 10));
        assert!(spec.authorized_keys(2).is_empty());
        assert!(spec.predict_counts(2).is_empty());
        assert!(spec.describe(2).contains("refs []"));
        // A region that holds no reference compacts into nothing.
        spec.compact(2);
        assert!(spec.predict_counts(2).is_empty());
        assert_eq!(BTreeMap::from([(0, 1)]), spec.predict_counts(3));
    }
}
