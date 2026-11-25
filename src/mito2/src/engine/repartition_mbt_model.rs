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
//!   itself plus the *authorization* of that reference: the set of keys of that physical file
//!   the reference may read ([`KeyAuth`], `None` meaning the whole file). A reference is never
//!   dropped by a transition; an authorization that admits no key is kept, because the engine
//!   keeps the reference too.
//! - `write` adds its rows' keys to the destination region's memtable.
//! - `flush` turns a region's memtable into one new physical file authorized for the whole
//!   file (`FileMeta::owned_range = None`), created by the flushing region.
//! - `split(source, cut)` republishes each of the source's references in both targets with
//!   `authorization ∩ source domain ∩ target domain`; the *source* term is elided exactly when
//!   the source's domain is the file's creator domain (the reader skipped it), the **target**
//!   term never is.
//! - `merge(inputs, survivor)` applies the same narrowing per input and unions the
//!   contributions of one physical file that arrive at the same target.
//! - `region compaction(region)` is legal while the region holds a file another holder
//!   keeps: the output is a *new* physical file carrying exactly the rows the region sees and
//!   authorized for the whole file, and the consumed references leave *that region only*.
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
//!   drops the whole partition predicate;
//! - the contribution is *always* intersected with what each reference authorizes: the reader
//!   conjoins a reference's `owned_range` into the file's partition filter even when it skipped
//!   the region predicate (`SstParquetReader::build_partition_filter`), so dropping the region
//!   predicate never widens a reference;
//! - rows still in the memtable are always filtered by the domain, because the partition
//!   predicate belongs to the memtable scan too;
//! - `split` and `merge` narrow a reference to `authorized ∩ source ∩ target` (`merge` unions
//!   the per-input contributions), and compaction writes a new file holding exactly the rows
//!   the compacting region sees, removing the consumed references from that region only.
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
//! are a subset of the keys its references authorize *in that region* plus the keys of its own
//! unflushed rows. That is "selecting a file does not authorize reading all of it", stated on
//! the model side: a split that hands both halves the same object still leaves each half
//! authorized to read only its own rows, and a reference whose authorization excludes a row
//! keeps it invisible even where the region predicate is skipped.
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

/// The keys of `domain`, i.e. what a region-domain term admits on its own.
fn domain_keys(domain: Domain) -> BTreeSet<usize> {
    (domain.lo..domain.hi).collect()
}

/// `auth` restricted to the keys of `domain`: `auth ∧ domain`, the restriction a region-domain
/// term applies to a reference.
fn auth_and_domain(auth: &KeyAuth, domain: Domain) -> KeyAuth {
    match auth {
        None => Some(domain_keys(domain)),
        Some(keys) => Some(
            keys.iter()
                .copied()
                .filter(|key| domain.contains_key(*key))
                .collect(),
        ),
    }
}

/// The authorization a remap (split/merge publication) derives for one reference
/// (`RemapManifest::narrowed_owned_range`, `src/mito2/src/remap_manifest.rs`):
/// `old authorization ∩ source region domain ∩ target region domain`.
///
/// `source` is [`None`] exactly when the source region's domain is the file's creator domain:
/// the reader skipped that term for the source, so the source saw every physical row and the
/// term must not be added back. The *target* term is never elided — it is what routes a row to
/// this target.
fn remapped_auth(auth: &KeyAuth, source: Option<Domain>, target: Domain) -> KeyAuth {
    let narrowed = match source {
        Some(source) => auth_and_domain(auth, source),
        None => auth.clone(),
    };
    auth_and_domain(&narrowed, target)
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
    /// Model-minted physical-file id -> the authorization of *this* region's reference to that
    /// file: the keys of the file this region may read, `None` meaning the whole file. It is
    /// `FileMeta::owned_range` on this branch and is advanced only by the transitions below.
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

    /// What one region's scan sees, predicted by mirroring the engine of **this** branch.
    ///
    /// This is the single place the visibility rule lives, and it is deliberately stated
    /// against the engine as it behaves here:
    ///
    /// - `ScanInput::predicate_for_file` (`src/mito2/src/read/scan_region.rs`) hands a file its
    ///   predicate *without* the region partition expression exactly when
    ///   `should_skip_region_partition` holds, i.e. when the region's partition expression
    ///   equals the file's creator expression, so the region-domain term may be skipped;
    /// - the reference's own authorization is applied in **both** cases:
    ///   `SstParquetReader::build_partition_filter` conjoins `FileMeta::owned_range` into the
    ///   file's partition filter even when the region predicate was skipped, so a reference
    ///   never widens because the region expression matched its creator;
    /// - rows still in the memtable are always filtered by the domain, because the memtable
    ///   carries the region predicate of its own;
    /// - the transitions advance the authorizations: `split` and `merge` narrow a reference to
    ///   `authorized ∩ source ∩ target` (`merge` unions the per-input contributions of one
    ///   file), and compaction writes a new file holding the rows the compacting region *sees*
    ///   while removing the consumed references from that region only.
    ///
    /// `copies` is the number of physical rows of that event the scan returns, so an event that
    /// two referenced files both carry is reported twice instead of being counted once.
    pub(crate) fn predict_visible_rows(&self, region: R) -> BTreeMap<i64, FileRow> {
        let mut visible: BTreeMap<i64, FileRow> = BTreeMap::new();
        let Some(state) = self.regions.get(&region) else {
            return visible;
        };
        let domain = state.domain;
        for (file, auth) in &state.refs {
            let Some(physical) = self.files.get(file) else {
                continue;
            };
            // The reference's authorization, always applied: `owned_range` filters rows of the
            // reference, not of the physical file.
            let authorized = auth_keys(auth);
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

    /// `flush`: the memtable becomes one new physical file, created by this region and
    /// authorized for the **whole** file — the engine writes `FileMeta::owned_range = None` for
    /// a flush output, so nothing but the region predicate can hide a row of it. A flush with
    /// nothing to write publishes nothing.
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
            .insert(file, None);
    }

    /// `split`: each target gets **every** reference of the source, re-authorized to
    /// `authorized ∩ source domain ∩ target domain` (the rule `narrowed_owned_range` applies).
    ///
    /// The source term is elided exactly when the source's domain is the file's creator domain:
    /// the source scan skipped that predicate, so the source saw every physical row and the term
    /// is true for it. The target term is never elided. A reference whose new authorization
    /// admits no key is kept, because the engine keeps publishing it; it simply contributes no
    /// row to either target.
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
        let mut left_refs: BTreeMap<u64, KeyAuth> = BTreeMap::new();
        let mut right_refs: BTreeMap<u64, KeyAuth> = BTreeMap::new();
        for (file, auth) in &state.refs {
            let source_term = self.source_term(*file, state.domain);
            let left_auth = remapped_auth(auth, source_term, left_domain);
            let right_auth = remapped_auth(auth, source_term, right_domain);
            // The shape fires when the same object is handed to both halves *and* both are
            // still allowed to read a row of it: that is the sharing the cases exist for.
            if !auth_keys(&left_auth).is_empty() && !auth_keys(&right_auth).is_empty() {
                self.shapes.insert(SHAPE_SPLIT_SHARED);
            }
            left_refs.insert(*file, left_auth);
            right_refs.insert(*file, right_auth);
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
    }

    /// The *source* term of a remap, i.e. what the source region's scan admitted of `file`:
    /// [`None`] when the source's domain is exactly the file's creator domain (the reader
    /// skipped the region predicate there), the source domain otherwise.
    fn source_term(&self, file: u64, source_domain: Domain) -> Option<Domain> {
        match self.files.get(&file) {
            Some(physical) if physical.creator == source_domain => None,
            _ => Some(source_domain),
        }
    }

    /// `merge`: the same remap rule applied to several sources at once — every input's
    /// references are re-authorized by that input's source term and the survivor's domain, and
    /// the contributions of **one** physical file that arrive at the survivor are *unioned*
    /// (`RemapManifest::union_owned_range`): the survivor may read what either input could read.
    pub(crate) fn merge(&mut self, inputs: &[R], survivor: R) {
        let compacted_one_side = self.shapes.contains(SHAPE_ONE_SIDE_COMPACTION);
        let survivor_domain = Domain::FULL;
        let mut refs: BTreeMap<u64, KeyAuth> = BTreeMap::new();
        for input in inputs {
            let Some(state) = self.regions.remove(input) else {
                continue;
            };
            assert!(
                state.memtable.is_empty(),
                "the model merged region {input} with unflushed rows; a merge must flush its \
                 inputs first"
            );
            for (file, auth) in state.refs {
                let source_term = self.source_term(file, state.domain);
                let contribution = remapped_auth(&auth, source_term, survivor_domain);
                let authorized = match refs.remove(&file) {
                    Some(existing) => auth_union(&existing, &contribution),
                    None => contribution,
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
                domain: survivor_domain,
                refs,
                memtable: BTreeSet::new(),
            },
        );
    }

    /// A **retried** merge: the same inputs are remapped again after the first attempt was
    /// interrupted by a close/reopen, into a fresh survivor.
    ///
    /// The inputs' persisted manifests did not move, so the retry republishes exactly the
    /// reference set the first attempt published: the same physical file ids, with the same
    /// authorizations. A remap is idempotent (`auth ∩ source ∩ target` applied twice is the
    /// same set), so that is why the state of the previous survivor is what the retry's
    /// survivor starts from — no row is copied a second time and no authorization moves.
    pub(crate) fn retry_merge(&mut self, previous_survivor: R, survivor: R) {
        let Some(state) = self.regions.remove(&previous_survivor) else {
            return;
        };
        self.regions.insert(survivor, state);
    }

    /// `region compaction`: legal even while another holder keeps a file this region holds.
    ///
    /// The output is a *new* physical file holding exactly the rows the region **sees** — the
    /// mirror of this branch's read path, so a row the region sees because its reference
    /// authorized it (or because the region predicate was skipped for it) is copied into the
    /// output too. Its creator is the compacting region, which is what makes the region's next
    /// scan skip the partition predicate for it, and it is authorized for the **whole** file:
    /// it physically holds only what the region saw, so nothing narrower is needed. The
    /// consumed references leave this region only, so every other holder keeps its own
    /// reference to the same files.
    pub(crate) fn compact(&mut self, region: R) {
        let Some(state) = self.regions.get(&region) else {
            return;
        };
        if state.refs.is_empty() {
            return;
        }
        let domain = state.domain;
        let shared: Vec<u64> = state
            .refs
            .keys()
            .copied()
            .filter(|file| self.holders_of(*file).len() > 1)
            .collect();
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
        state.refs.insert(file, None);
    }

    /// `import`: a byte copy of one of the region's own flushed files, published under a new
    /// physical id, while the original reference leaves the region in the same edit.
    ///
    /// The region keeps exactly the rows it had and the copy is authorized for exactly what
    /// the original reference authorized, so only the identity of one reference changes.
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
    }

    /// The keys `region` may read: for every reference, its authorization restricted to the
    /// keys the region's own scan still applies a domain predicate to, plus the keys of its own
    /// unflushed rows (the memtable is always filtered by the domain).
    ///
    /// The restriction matters because the region predicate is skipped exactly when the
    /// region's domain is the file's creator domain; everywhere else `authorized ∩ domain` is
    /// what a row must satisfy to be visible.
    pub(crate) fn authorized_keys(&self, region: R) -> BTreeSet<usize> {
        let mut keys = BTreeSet::new();
        let Some(state) = self.regions.get(&region) else {
            return keys;
        };
        for (file, auth) in &state.refs {
            let applies_domain = self
                .files
                .get(file)
                .is_none_or(|physical| physical.creator != state.domain);
            if applies_domain {
                keys.extend(auth_keys(&auth_and_domain(auth, state.domain)));
            } else {
                keys.extend(auth_keys(auth));
            }
        }
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

    /// One region's specification state, for a diff.
    pub(crate) fn describe(&self, region: R) -> String {
        match self.regions.get(&region) {
            None => format!("region {region}: the specification has no state for it"),
            Some(state) => {
                let refs: Vec<String> = state
                    .refs
                    .iter()
                    .map(|(file, auth)| match auth {
                        None => format!("f{file} -> whole file"),
                        Some(keys) => format!("f{file} -> {}", describe_keys(keys)),
                    })
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
        // A flush output is authorized for the whole file (rule 4 of the fixed engine), and
        // its creator domain is the flushing region's, so the region predicate is skipped for
        // it: the authorization statement is the whole universe, not the keys it holds.
        assert_eq!(key_universe(), spec.authorized_keys(1));
        assert!(spec.describe(1).contains("f0 -> whole file"));
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
        // Both halves hold the same object, but each authorization covers its own rows only:
        // the split intersected the target domain into it.
        assert_eq!(BTreeSet::from([2, 3]), spec.holders_of(0));
        assert_eq!(domain_keys(Domain::new(0, 5)), spec.authorized_keys(2));
        assert_eq!(domain_keys(Domain::new(5, 10)), spec.authorized_keys(3));
        assert_eq!(BTreeMap::from([(0, 1)]), spec.predict_counts(2));
        assert_eq!(BTreeMap::from([(1, 1)]), spec.predict_counts(3));
        assert!(spec.shapes().contains(SHAPE_SPLIT_SHARED));
        assert!(spec.region(1).is_none());
    }

    /// Rule 3: the reference authorization applies *even* when the region expression equals
    /// the file's creator expression, i.e. even where the region predicate is skipped.
    #[test]
    fn a_reference_authorization_applies_even_when_the_region_expression_equals_the_creator() {
        let mut spec = RefModel::<Rid>::new();
        spec.publish(1, Domain::new(0, 5));
        // Write admission checks the partition version, so the out-of-domain row is accepted.
        spec.note_write(1, 0, 7);
        spec.flushed(1);
        // The creator domain equals the region's: the reader drops the region predicate, and
        // the whole-file authorization lets the out-of-domain row through.
        assert_eq!(BTreeMap::from([(0, 1)]), spec.predict_counts(1));

        spec.split(1, 2, Domain::new(0, 5), 3, Domain::new(5, 10));
        // Region 2 still serves exactly the file's creator domain, so its scan again skips the
        // region predicate — but its reference now authorizes `[k00, k05)` only, and the row
        // at `k07` is outside it. Skipping the region predicate never widens a reference.
        assert_eq!(domain_keys(Domain::new(0, 5)), spec.authorized_keys(2));
        assert!(spec.predict_counts(2).is_empty());
        // Region 3's expression differs from the creator's, so it applies its own domain
        // predicate; `k07` is inside `[k05, k10)` and inside its authorization, so it sees the
        // row — the same physical row, visible through one reference and not the other.
        assert_eq!(BTreeMap::from([(0, 1)]), spec.predict_counts(3));
    }

    /// Rule 5: a merge unions the authorizations of the references it combines, so one
    /// physical file reaching the survivor from two holders is readable through both ranges.
    #[test]
    fn merge_unions_the_authorizations_of_the_same_physical_file() {
        let mut spec = one_shared_file();
        spec.split(1, 2, Domain::new(0, 5), 3, Domain::new(5, 10));
        spec.merge(&[2, 3], 4);
        // An intersection would authorize nothing and a single input's set would hide one of
        // the two rows; the union admits both.
        assert_eq!(key_universe(), spec.authorized_keys(4));
        assert_eq!(BTreeMap::from([(0, 1), (1, 1)]), spec.predict_counts(4));
        assert_eq!(1, spec.region(4).expect("published").refs.len());
    }

    #[test]
    fn merge_after_one_side_compaction_keeps_the_shared_row_once() {
        let mut spec = one_shared_file();
        spec.split(1, 2, Domain::new(0, 5), 3, Domain::new(5, 10));
        spec.compact(2);
        assert!(spec.shapes().contains(SHAPE_ONE_SIDE_COMPACTION));
        // The compacting half keeps only the row it saw; the other half keeps the object.
        assert_eq!(BTreeMap::from([(0, 1)]), spec.predict_counts(2));
        assert_eq!(BTreeSet::from([3]), spec.holders_of(0));
        assert_eq!(BTreeSet::from([2]), spec.holders_of(1));
        // The compaction output physically holds that row only, so the region may read the
        // whole of it; its creator is the region, so the region predicate is skipped too.
        assert_eq!(key_universe(), spec.authorized_keys(2));

        spec.merge(&[2, 3], 4);
        assert!(spec.shapes().contains(SHAPE_MERGE_RECOMBINES));
        // The survivor references the original object and the compaction copy. The original's
        // authorization is the right half, so the row the copy rewrote at `k02` is read only
        // through the copy: the fixed engine does not duplicate it.
        assert_eq!(BTreeMap::from([(0, 1), (1, 1)]), spec.predict_counts(4));
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
        // The copy carries the authorization of the reference it replaced.
        assert_eq!(before_keys, spec.authorized_keys(1));
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

    /// A half whose share of the target domain holds no physical row: the reference is still
    /// published (the engine keeps it), it just authorizes nothing that the file stores.
    #[test]
    fn a_split_target_whose_rows_all_stay_right_authorizes_nothing_it_stores() {
        let mut spec = RefModel::<Rid>::new();
        spec.publish(1, Domain::new(0, 10));
        spec.note_write(1, 0, 8);
        spec.flushed(1);
        spec.split(1, 2, Domain::new(0, 5), 3, Domain::new(5, 10));
        // The left half holds the reference, but the reference authorizes its own half of the
        // target domain and the file stores no row there.
        assert_eq!(domain_keys(Domain::new(0, 5)), spec.authorized_keys(2));
        assert!(spec.predict_counts(2).is_empty());
        assert!(spec.describe(2).contains("f0 -> "));
        assert_eq!(BTreeMap::from([(0, 1)]), spec.predict_counts(3));
        // A region that sees no row compacts into an output file holding none.
        spec.compact(2);
        assert!(spec.predict_counts(2).is_empty());
        assert_eq!(BTreeMap::from([(0, 1)]), spec.predict_counts(3));
    }
}
