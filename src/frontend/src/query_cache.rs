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

//! Frontend-local rollup cache core.
//!
//! The cache holds whole, materialized PromQL results for canonical spans of an
//! evaluation grid ([`TimeChunk`]), keyed by the request identity that makes a
//! result reusable ([`QueryIdentity`]) and never by the window that happened to
//! request it ([`QuerySpan`]). A dashboard sliding from `[10:00, 11:00)` to
//! `[10:05, 11:05)` therefore keeps every interior chunk.
//!
//! # Canonical chunks
//!
//! Chunk bounds are quantized to absolute time, not to the request: with a grid
//! phase `p = start_ms.rem_euclid(step_ms)` and a chunk of `c` milliseconds, chunk
//! `k` covers `[p + k*c, p + (k+1)*c)`, where `k` is the floor of
//! `(t - p) / c`. Planning therefore requires `c` to be a whole multiple of the
//! step ([`BypassReason::MisalignedChunk`] otherwise), so every bound is a grid
//! point of every request with the same step and phase, and two requests with
//! different phases can never produce the same bound.
//!
//! Only a chunk whose points are *all* requested and *all* older than the tail
//! cutoff is cacheable. Partial chunks at either end of a window, and every point
//! at or after the cutoff, are returned as [`DirectSpan`]s for the caller to
//! execute without the cache.
//!
//! # Tail
//!
//! The tail is measured from the wall clock: points at or after `now - tail` may
//! still be receiving data, so they are always direct. A historical query whose
//! end is older than the cutoff therefore caches its whole range.
//!
//! # Admission
//!
//! The cache serves a source table only after a background round has proved a
//! complete cursor domain for it. A request never probes: every lookup checks the
//! table's current readiness under its lock, and a cold table, a table no round has
//! proved yet, and a table whose last round failed all bypass instead of waiting.
//! A later successful round opens the table again. `last_success` is observation
//! only; it is not an admission deadline.
//!
//! # Validity
//!
//! An admitted entry is served while it is younger than
//! [`RollupCacheConfig::max_age`], counted from the instant the fill captured
//! *before* it read storage, so a slow fill is charged for the time it took. Nothing
//! else is checked on the lookup path: the sequence probe runs in the background,
//! never inside a request.
//!
//! # Invalidation
//!
//! A per-region sequence cursor says that *something* changed; it cannot say which
//! chunks that change reached. Each background round of a table asks
//! [`Instance::probe_sequence_range`] for the timestamp hull of everything after its
//! cursors, and [`RollupCache::record_refresh`] removes every entry of that table
//! whose *input dependency* overlaps the hull ([`DirtySpan`]). Every other chunk,
//! and every other table, stays resident. A round that proves nothing changed
//! removes nothing, and a round whose change has no boundable timestamp removes
//! every entry of the table.
//!
//! The input dependency is the span of source data the payload was materialized
//! from, recorded when the entry is stored. Deriving it from the evaluation window
//! is not enough: a chunk's output points can all come before a dirty range while
//! the history they folded — a lookback window, or the range of a `rate(x[5m])` —
//! still reaches into it. [`RollupCache::insert`] takes the dependency from the
//! caller, which is the only place that knows how far before the chunk the
//! evaluation read; overlap is half-open on both spans, so adjacent spans do not
//! invalidate each other.
//!
//! The source sequence decides what may be published, and the version only counts cache
//! changes. Two per-query values carry that:
//!
//! - [`QueryState::invalidated_watermark`] is the source sequence the last invalidation of
//!   the query was committed at, a complete per-region vector — never one number, which
//!   could hide a region that fell behind;
//! - [`Entry::read_through`] is the source sequence the read behind a payload reported,
//!   again per region.
//!
//! A fill publishes only when its own read reports exactly the regions the watermark covers —
//! the same key set, neither a subset nor a superset — and is at least as far as the
//! watermark in every one of them, equality allowed. A different set of regions is not a
//! comparison at all: a read that saw fewer regions cannot be shown to describe the same data,
//! and one that saw more cannot be ordered against the invalidation either. A query whose
//! watermark is still empty is the exception, because the table had no proved domain when the
//! fill registered: there is nothing to compare, and the read's own vector is the only bound
//! there is. Whatever is resident is the second bound: a read replaces it only when it reports
//! the same regions, is at least as far everywhere, and is strictly further somewhere — an
//! entry past its age bound is the one exception, and a read of the same sequence may refresh
//! it.
//!
//! A payload materialized before an invalidation can therefore never be published after it,
//! and a read that finished later but got less far cannot replace a newer payload. A read that
//! cannot report its sequence is not published at all. A fill that loses is still returned to
//! the request that read it.
//!
//! The watermark is written only by the rounds. A round that found a dirty hull drops the
//! entries the hull reaches and moves the watermark of every query of the table to the
//! probe's high watermark — even when it dropped nothing — and that is the whole of an
//! invalidation: the hull is the round's input and nothing more, no range is remembered,
//! and a lookup decides on the readiness of its table and the age of the entry alone.
//!
//! [`QueryState::version`] counts the changes to a query's cache: every published payload
//! and every committed invalidation advances it once. Nothing admits or refuses a fill by
//! it; it is what tells an observer how many times the cache changed.
//!
//! # Round proofs
//!
//! A round's vector is compared with the cursors the table committed before anything is
//! committed, whatever the round found — a quiet round included. The key sets have to be the
//! same and no region may go back: a region that went back is a vector of something the cache
//! cannot describe, so the table stops being served and neither the cursors, the watermarks nor
//! the version move.
//!
//! A route that gained or lost a region re-keys the cursors, which makes the round's vector and
//! everything committed under the old domain incomparable. The table then forgets its entries,
//! its queries and its cursors and starts again from the round's vector as its baseline, so a
//! chunk of the old route can never be served for the new one and a fill that was reading
//! across the change is refused when it tries to publish. The same happens when the table's
//! metadata version moved, because a schema change can change the value of rows already stored
//! without moving any sequence: a column dropped and added back with a default answers
//! differently for the same timestamps. The same happens again when the table's own id moved,
//! because a metric logical table rebuilt under the same name keeps its version and the physical
//! regions behind it while its logical identity is another one.
//!
//! Such a restart replaces the state the table is registered under instead of emptying it, so a
//! fill that was admitted before it holds a state of its own for good: another fill of the same
//! identity that registers after the restart creates its query in the replacement, and the
//! earlier one still cannot publish into it.
//!
//! A table's *first* round is its baseline, and it restarts the table like the rounds above do:
//! every query that registered while the table had no domain started with an empty watermark,
//! which bounds nothing, so everything it filled was read under no domain and no later round can
//! vouch for it. The replacement carries the baseline as its cursors, so a fill that is still
//! reading is refused when it tries to publish — it holds the state it was admitted under, which
//! is no longer the registered one — and the payloads the cold reads published are dropped. A
//! query that registers after the baseline inherits the table's cursors, so the table is never
//! cold for it.
//!
//! Nothing on either side waits for the other.
//!
//! # Memory
//!
//! The cache is bounded by bytes, not entries: each entry is charged its payload's
//! buffer memory plus a fixed per-entry overhead, and moka evicts by that weight.
//! A payload that is still being filled is not charged until it is inserted, since
//! the cache cannot see it before then.
//!
//! # Tables
//!
//! Every table a request is admitted for is registered as active, keyed by its
//! fully qualified name: a bare table name would match the same name in another
//! catalog or schema. A table no request has admitted for [`IDLE_TABLE_TIMEOUT`]
//! stops being refreshed and its entries are dropped, so a frontend that once saw
//! many tables does not probe them forever.
//!
//! # Concurrency
//!
//! A lookup is local: it reads at most one entry, never storage, and never waits for
//! an in-flight fill or for maintenance. There is no single-flight, so a miss while
//! another fill of the same chunk is running proceeds; the competing fills both reach
//! [`RollupCache::insert`], where the older publication loses. Everything here is
//! synchronous and no lock is held across an await, so a caller may await its storage
//! read without holding anything.
//!
//! A refresh round holds the registry lock across its scan and its cursor update, so a
//! request cannot admit the table, publish into it, or read its state in the middle of
//! one. A fill that was already running is dealt with by the publication order and by
//! the identity of the state it captured instead of by waiting for it.
//!
//! # Not covered
//!
//! Chunks cover *output* points. The evaluator still has to read the input history
//! every selected point folds: the first point of a `rate(x[5m])` chunk needs five
//! minutes of samples *before* the chunk start. Nothing here narrows that history.
//!
//! A rename or a reuse of a physical table, a truncate, a TTL or compaction removal,
//! and a DELETE mixed with a distant PUT can all erase or move input that the
//! timestamp hull of a sequence probe does not describe. None of them is detected: an
//! entry that survives such a write is served until something dirties its dependency
//! or it ages out.

#![allow(dead_code)] // The planning and identity helpers are exercised by the tests below.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;
use std::sync::Arc;
use std::sync::Mutex;
#[cfg(any(test, feature = "testing"))]
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime};

use common_recordbatch::RecordBatches;
use common_telemetry::debug;
use moka::sync::Cache;
use promql_parser::parser::EvalStmt;
use query::promql::label_values::signed_millis_since_epoch;
use query::promql::window::{EvalGrid, WindowError, is_windowable};

/// The placeholder a [`QueryIdentity`] prints instead of its query text.
const REDACTED_QUERY: &str = "<redacted>";

/// What every entry is charged beyond the bytes of its payload.
///
/// The key and the entry itself are heap-allocated too; without a fixed charge an
/// empty result would weigh nothing and the cache could hold unbounded keys.
const ENTRY_OVERHEAD_BYTES: u64 = 256;

/// How long a source table stays active after the last request admitted for it.
///
/// A table is only refreshed while requests keep reading it: one that no request
/// has admitted for this long stops being probed and its entries are dropped, so a
/// frontend that once saw many tables does not probe them forever. The next request
/// for it registers it again and bypasses until a background round proves it.
const IDLE_TABLE_TIMEOUT: Duration = Duration::from_secs(600);

/// How many times one invalidation rescans to order the fills that started while it
/// scanned.
///
/// A scan is a pass over the entries and a read of every publication number, so even a
/// busy table settles in a few of them. A query that is still moving after the last scan
/// keeps the payloads it published beyond it: they are the newest ones, and the next
/// round that reaches them, or [`RollupCacheConfig::max_age`], is what takes them out.
const INVALIDATE_SCANS: usize = 4;

/// The planner that evaluated a PromQL query.
///
/// Both routes evaluate the same expression on the same grid but build different
/// plans, so a result produced by one is not reused for the other.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum QueryRoute {
    /// The legacy `query::promql::planner` route.
    Legacy,
    /// The native `query::promql::native_compile` route.
    Native,
}

/// Every dimension two requests must agree on before one may reuse the other's
/// chunks.
///
/// This is the reusable half of a request: it carries no window, so two requests
/// that ask for overlapping windows of the same query produce the same identity
/// and can share every canonical chunk they have in common. A missing dimension
/// would let one request answer another's points, which is why the list is
/// exhaustive rather than minimal.
///
/// The query text and source table are redacted in [`fmt::Debug`], since a query
/// name and its selectors can carry tenant data; the alias and the read preference
/// are not query text and are printed.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct QueryIdentity {
    /// The tenant the query runs as.
    pub tenant: String,
    /// The catalog the query reads.
    pub catalog: String,
    /// The schema (database) the query reads.
    pub schema: String,
    /// The query text exactly as the caller wrote it.
    pub query: String,
    /// The admitted expression's named source table, without catalog or schema.
    pub source_table: String,
    /// The evaluation step, in milliseconds.
    ///
    /// It has to be the step the statement is evaluated with: a chunk keyed on one
    /// grid must never be served to another, so planning bypasses the cache when
    /// the two disagree ([`BypassReason::StepMismatch`]).
    pub step_ms: i64,
    /// The lookback delta planning applies, in milliseconds.
    pub lookback_ms: i64,
    /// The planner that evaluated the query.
    pub route: QueryRoute,
    /// The optional output alias of a PromQL statement
    /// (`QueryStatement::Promql(_, alias)`).
    ///
    /// The alias renames the result's columns without changing the expression, so
    /// two requests that differ only in it must not share entries.
    pub alias: Option<String>,
    /// The read preference the request ran under, as
    /// `QueryContext::read_preference()` spells it.
    ///
    /// It can change which replica serves the query, so it is part of the identity
    /// even while every session accepts the same preference. `None` means the
    /// caller has not threaded one through; the cache never substitutes a default.
    pub read_preference: Option<String>,
}

impl fmt::Debug for QueryIdentity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("QueryIdentity")
            .field("tenant", &self.tenant)
            .field("catalog", &self.catalog)
            .field("schema", &self.schema)
            .field("query", &REDACTED_QUERY)
            .field("source_table", &REDACTED_QUERY)
            .field("step_ms", &self.step_ms)
            .field("lookback_ms", &self.lookback_ms)
            .field("route", &self.route)
            .field("alias", &self.alias)
            .field("read_preference", &self.read_preference)
            .finish()
    }
}

/// The fully qualified name of a source table.
///
/// It is what the cache registers a table under and what invalidation matches on:
/// a bare table name would let a dirty `public.cpu` invalidate another schema's
/// `cpu`, so every comparison uses all three parts.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SourceTable {
    catalog: String,
    schema: String,
    table: String,
}

impl SourceTable {
    /// The table `catalog.schema.table`.
    pub fn new(
        catalog: impl Into<String>,
        schema: impl Into<String>,
        table: impl Into<String>,
    ) -> Self {
        Self {
            catalog: catalog.into(),
            schema: schema.into(),
            table: table.into(),
        }
    }

    /// The catalog the table lives in.
    pub fn catalog(&self) -> &str {
        &self.catalog
    }

    /// The schema the table lives in.
    pub fn schema(&self) -> &str {
        &self.schema
    }

    /// The table name, without its catalog or schema.
    pub fn table(&self) -> &str {
        &self.table
    }

    /// Whether `identity` reads this table.
    fn owns(&self, identity: &QueryIdentity) -> bool {
        identity.catalog == self.catalog
            && identity.schema == self.schema
            && identity.source_table == self.table
    }
}

impl From<&QueryIdentity> for SourceTable {
    fn from(identity: &QueryIdentity) -> Self {
        Self::new(
            identity.catalog.clone(),
            identity.schema.clone(),
            identity.source_table.clone(),
        )
    }
}

impl fmt::Display for SourceTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}.{}", self.catalog, self.schema, self.table)
    }
}

/// The coverage one request asked for, in milliseconds.
///
/// It is deliberately not part of any key: it says which points a plan must cover
/// exactly once, not which results are interchangeable. `end_ms` is the last
/// requested grid point, so the coverage is inclusive there and half-open in the
/// plans this module returns.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QuerySpan {
    start_ms: i64,
    end_ms: i64,
}

impl QuerySpan {
    /// The coverage `[start_ms, end_ms]`.
    pub fn new(start_ms: i64, end_ms: i64) -> Self {
        Self { start_ms, end_ms }
    }

    /// The coverage `stmt` evaluates.
    pub fn of(stmt: &EvalStmt) -> Result<Self, BypassReason> {
        let grid = EvalGrid::of(stmt).map_err(BypassReason::from)?;
        Ok(Self::new(grid.start_ms(), grid.last_ms()))
    }

    /// The first requested grid point.
    pub fn start_ms(&self) -> i64 {
        self.start_ms
    }

    /// The last requested grid point.
    pub fn end_ms(&self) -> i64 {
        self.end_ms
    }

    /// Whether `ms` is inside the coverage.
    pub fn contains_ms(&self, ms: i64) -> bool {
        self.start_ms <= ms && ms <= self.end_ms
    }
}

/// A half-open `[start, end)` span of output grid points, in milliseconds.
///
/// Both bounds lie on the evaluation grid of the statement the span was planned
/// from: `start` is a grid point, and `end` is either the next grid point past the
/// last point the span holds or the instant one step past the statement's last
/// point. Spans are never rounded to wall-clock buckets.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TimeChunk {
    start_ms: i64,
    end_ms: i64,
}

impl TimeChunk {
    /// A half-open `[start_ms, end_ms)` span.
    pub fn new(start_ms: i64, end_ms: i64) -> Self {
        Self { start_ms, end_ms }
    }

    /// The first millisecond of the span.
    pub fn start_ms(&self) -> i64 {
        self.start_ms
    }

    /// The millisecond just past the span.
    pub fn end_ms(&self) -> i64 {
        self.end_ms
    }

    /// Whether the span holds no millisecond at all.
    pub fn is_empty(&self) -> bool {
        self.start_ms >= self.end_ms
    }
}

/// Whether the read `current` may replace a payload the read `resident` produced.
///
/// The two have to report the same regions: a read that touched fewer cannot be shown to
/// describe the same data, so it replaces nothing. Then `current` has to be at least as far
/// everywhere and strictly further in at least one region — a read of the same sequence says
/// nothing new about the source. An entry past its age bound is the exception: refreshing it
/// is worth a read of the same sequence, because the payload is stale by age alone.
fn may_replace(
    current: &BTreeMap<u64, u64>,
    resident: &BTreeMap<u64, u64>,
    resident_expired: bool,
) -> bool {
    if current.len() != resident.len()
        || resident.keys().any(|region| !current.contains_key(region))
    {
        return false;
    }
    let mut further = false;
    for (region, previous) in resident {
        let now = current[region];
        if now < *previous {
            return false;
        }
        further |= now > *previous;
    }
    further || resident_expired
}

/// How one round's `high` vector relates to the cursors a table committed.
///
/// The two vectors describe regions, so they can only be compared when they name the same
/// ones. A route that gained or lost a region changes the keys, and a vector has to be
/// renamed rather than merged: a region the table no longer has would keep a bound of
/// another route, and a region it gained would hide behind the others' progress.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Domain {
    /// The table has committed no cursors yet, so the round is its baseline.
    Baseline,
    /// The round names the committed regions and none of them went back.
    Same,
    /// The round names another set of regions: the route moved.
    Rekeyed,
    /// The round names the committed regions but one of them went back, so it is not a
    /// later round of this table at all.
    Regressed,
}

/// The [`Domain`] a round's `high` vector sits in, seen from the committed `cursors`.
fn domain_of(committed: &BTreeMap<u64, u64>, high: &BTreeMap<u64, u64>) -> Domain {
    if committed.is_empty() {
        return Domain::Baseline;
    }
    // Equal lengths and a subset of the committed keys is the same key set.
    if committed.len() != high.len() || committed.keys().any(|region| !high.contains_key(region)) {
        return Domain::Rekeyed;
    }
    if committed
        .iter()
        .any(|(region, cursor)| high[region] < *cursor)
    {
        return Domain::Regressed;
    }
    Domain::Same
}

/// Whether `left` and `right` share at least one millisecond.
///
/// Both spans are half-open, so a span ending where the other starts does not
/// overlap it, and an empty span overlaps nothing.
fn overlaps(left: TimeChunk, right: TimeChunk) -> bool {
    !left.is_empty()
        && !right.is_empty()
        && left.start_ms() < right.end_ms()
        && right.start_ms() < left.end_ms()
}

/// Why a span must be executed without the cache.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DirectReason {
    /// A partial chunk at one end of the request, or the gaps around the cacheable
    /// chunks. Its points are cacheable in principle but this request does not
    /// cover a whole canonical chunk there.
    Edge,
    /// Points at or after `now - tail`, which may still be receiving data.
    Tail,
}

/// A span the caller must execute directly, and why.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DirectSpan {
    span: TimeChunk,
    reason: DirectReason,
}

impl DirectSpan {
    /// The span to execute without the cache.
    pub fn span(&self) -> TimeChunk {
        self.span
    }

    /// Why the span is not cacheable.
    pub fn reason(&self) -> DirectReason {
        self.reason
    }
}

/// The cache key of one materialized chunk.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TimeChunkKey {
    identity: QueryIdentity,
    chunk: TimeChunk,
}

impl TimeChunkKey {
    /// The key of the canonical `chunk` of `identity`.
    pub fn new(identity: QueryIdentity, chunk: TimeChunk) -> Self {
        Self { identity, chunk }
    }

    /// The request identity the chunk belongs to.
    pub fn identity(&self) -> &QueryIdentity {
        &self.identity
    }

    /// The canonical span the chunk covers.
    pub fn chunk(&self) -> TimeChunk {
        self.chunk
    }
}

/// Why a statement or a chunk must not be cached.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BypassReason {
    /// The expression is outside the windowable allowlist
    /// ([`query::promql::window::is_windowable`]).
    NotWindowable,
    /// The statement's span or its step cannot be planned into chunks.
    InvalidSpan,
    /// The grid arithmetic, or a chunk bound, cannot be represented.
    Overflow,
    /// The configured chunk is not a whole multiple of the query's step, so
    /// canonical chunk bounds would fall between the evaluation grid's points.
    MisalignedChunk,
    /// The identity's step is not the step the statement is evaluated with, so its
    /// chunks would be keyed as spans of another grid.
    StepMismatch,
    /// The chunk is empty, so nothing was ever materialized for it.
    EmptyChunk,
}

impl fmt::Display for BypassReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotWindowable => f.write_str("the expression is not windowable"),
            Self::InvalidSpan => f.write_str("the statement cannot be split into chunks"),
            Self::Overflow => f.write_str("a chunk bound overflows the time grid"),
            Self::MisalignedChunk => {
                f.write_str("the chunk is not a whole multiple of the query's step")
            }
            Self::StepMismatch => f.write_str("the identity's step is not the statement's step"),
            Self::EmptyChunk => f.write_str("the chunk is empty"),
        }
    }
}

impl std::error::Error for BypassReason {}

impl From<WindowError> for BypassReason {
    fn from(error: WindowError) -> Self {
        match error {
            WindowError::NotEligible => Self::NotWindowable,
            WindowError::InvalidSpan => Self::InvalidSpan,
            WindowError::Overflow => Self::Overflow,
        }
    }
}

/// The cacheable chunks and the direct spans of one statement.
///
/// [`ChunkSpans::chunks`] and [`ChunkSpans::direct`] partition the requested
/// coverage exactly once each, in increasing order of time: the direct spans are
/// disjoint, and together with the chunks they leave neither a gap nor an overlap.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChunkSpans {
    span: QuerySpan,
    chunks: Vec<TimeChunk>,
    direct: Vec<DirectSpan>,
    tail: Option<TimeChunk>,
}

impl ChunkSpans {
    /// The coverage every span below partitions.
    pub fn span(&self) -> QuerySpan {
        self.span
    }

    /// The complete canonical chunks, in time order.
    pub fn chunks(&self) -> &[TimeChunk] {
        &self.chunks
    }

    /// The spans to execute without the cache, in time order.
    pub fn direct(&self) -> &[DirectSpan] {
        &self.direct
    }

    /// The part of the coverage at or after `now - tail`.
    pub fn tail(&self) -> Option<TimeChunk> {
        self.tail
    }

    /// The coverage, chunks, direct spans, and tail.
    pub fn into_parts(
        self,
    ) -> (
        QuerySpan,
        Vec<TimeChunk>,
        Vec<DirectSpan>,
        Option<TimeChunk>,
    ) {
        (self.span, self.chunks, self.direct, self.tail)
    }
}

/// Plans the cacheable chunks and the direct spans of `stmt` as of `now`.
///
/// The rule, in one place:
///
/// - chunk bounds are canonical: with phase `p = start_ms.rem_euclid(step_ms)` and
///   chunk length `chunk`, they are `p + k * chunk`, which requires `chunk` to be
///   a whole multiple of the statement's step
///   ([`BypassReason::MisalignedChunk`] otherwise);
/// - a chunk is cacheable only when every one of its points is requested and every
///   one of them is older than `now - tail`;
/// - everything else — the partial chunks at either end and the whole tail — is
///   returned as [`DirectSpan`]s, because a partial chunk must never be keyed as a
///   full one.
///
/// Chunking selects output points only: the evaluator still has to read the input
/// history each point folds.
pub fn plan_chunks(
    stmt: &EvalStmt,
    tail: Duration,
    chunk: Duration,
    now: SystemTime,
) -> Result<ChunkSpans, BypassReason> {
    if !is_windowable(&stmt.expr) {
        return Err(BypassReason::NotWindowable);
    }
    let grid = EvalGrid::of(stmt)?;
    let chunk_in_steps = chunk_steps(chunk, grid.step_ms()).ok_or(BypassReason::MisalignedChunk)?;
    let chunk_ms = i64::try_from(chunk.as_millis()).map_err(|_| BypassReason::Overflow)?;
    let now_ms = signed_millis_since_epoch(now).map_err(|_| BypassReason::Overflow)?;
    let tail_ms = i64::try_from(tail.as_millis()).unwrap_or(i64::MAX);
    let cutoff_ms = now_ms.saturating_sub(tail_ms);

    let points = grid.points();
    let span = QuerySpan::new(grid.start_ms(), grid.last_ms());
    // The first requested point that must be read directly because of the tail.
    let tail_start = first_index_at_or_after(&grid, cutoff_ms)?;

    // The first canonical chunk bound at or after the coverage starts.
    let first_chunk = first_canonical_chunk(&grid, chunk_ms)?;
    // Chunks may only cover points before the tail cutoff.
    let cacheable_end = points.min(tail_start);

    let mut chunks = Vec::new();
    let mut next = first_chunk;
    while next.saturating_add(chunk_in_steps) <= cacheable_end {
        let start_ms = grid.point_ms(next)?;
        let end_ms = grid.point_ms(next + chunk_in_steps)?;
        chunks.push(TimeChunk::new(start_ms, end_ms));
        next += chunk_in_steps;
    }

    // What is left over: before the first chunk, and after the last one. With no
    // cacheable chunk both ends collapse into one region covering the whole
    // request.
    let head_end = if chunks.is_empty() { 0 } else { first_chunk };
    let tail_from = if chunks.is_empty() { 0 } else { next };
    let mut direct = Vec::new();
    push_direct_region(&grid, &mut direct, 0, head_end, tail_start)?;
    push_direct_region(&grid, &mut direct, tail_from, points, tail_start)?;
    let tail = direct
        .iter()
        .find(|span| span.reason == DirectReason::Tail)
        .map(|span| span.span);

    Ok(ChunkSpans {
        span,
        chunks,
        direct,
        tail,
    })
}

/// The configured chunk as a whole number of steps of `step_ms`.
///
/// `None` means canonical chunk bounds would fall between the grid's points, so no
/// span of that query could be cached at that chunk length.
fn chunk_steps(chunk: Duration, step_ms: i64) -> Option<u64> {
    let chunk_ms = i64::try_from(chunk.as_millis()).ok()?;
    if step_ms <= 0 || chunk_ms <= 0 || chunk_ms % step_ms != 0 {
        return None;
    }
    u64::try_from(chunk_ms / step_ms).ok()
}

/// `value / divisor` rounded towards positive infinity, for a positive `divisor`.
///
/// The signed `div_ceil` is not stable yet, and the bounds below may lie before the
/// epoch, where truncating division would round the wrong way.
fn div_ceil(value: i128, divisor: i128) -> i128 {
    let quotient = value.div_euclid(divisor);
    if value.rem_euclid(divisor) == 0 {
        quotient
    } else {
        quotient + 1
    }
}

/// The first grid index whose point is at or after `ms`, clamped to the grid.
fn first_index_at_or_after(grid: &EvalGrid, ms: i64) -> Result<u64, BypassReason> {
    let delta = i128::from(ms) - i128::from(grid.start_ms());
    let index = div_ceil(delta, i128::from(grid.step_ms()));
    let index = index.clamp(0, i128::from(grid.points()));
    u64::try_from(index).map_err(|_| BypassReason::Overflow)
}

/// The grid index of the first canonical chunk bound at or after the grid starts.
///
/// Canonical bounds are `phase + k * chunk`, with the phase read from the grid, so
/// this is the smallest such bound that is not before the grid's first point.
fn first_canonical_chunk(grid: &EvalGrid, chunk_ms: i64) -> Result<u64, BypassReason> {
    let phase = i128::from(grid.phase_ms());
    let chunk = i128::from(chunk_ms);
    let start = i128::from(grid.start_ms());
    let bound = div_ceil(start - phase, chunk) * chunk + phase;
    let index = (bound - start) / i128::from(grid.step_ms());
    u64::try_from(index).map_err(|_| BypassReason::Overflow)
}

/// Appends the direct spans that the grid indexes `[from, to)` split into at
/// `tail_start`.
fn push_direct_region(
    grid: &EvalGrid,
    direct: &mut Vec<DirectSpan>,
    from: u64,
    to: u64,
    tail_start: u64,
) -> Result<(), BypassReason> {
    let tail_from = from.max(tail_start).min(to);
    push_direct_span(grid, direct, from, tail_from, DirectReason::Edge)?;
    push_direct_span(grid, direct, tail_from, to, DirectReason::Tail)
}

/// Appends one direct span, merging it into the previous one when they are
/// contiguous and direct for the same reason.
fn push_direct_span(
    grid: &EvalGrid,
    direct: &mut Vec<DirectSpan>,
    from: u64,
    to: u64,
    reason: DirectReason,
) -> Result<(), BypassReason> {
    if from >= to {
        return Ok(());
    }
    let start_ms = grid.point_ms(from)?;
    let end_ms = grid.point_ms(to)?;
    if let Some(last) = direct.last_mut()
        && last.reason == reason
        && last.span.end_ms() == start_ms
    {
        last.span = TimeChunk::new(last.span.start_ms(), end_ms);
        return Ok(());
    }
    direct.push(DirectSpan {
        span: TimeChunk::new(start_ms, end_ms),
        reason,
    });
    Ok(())
}

/// Why a lookup misses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MissReason {
    /// No entry holds this chunk.
    NotFound,
    /// The entry was filled longer ago than the hard age bound allows.
    Expired {
        /// How long ago the fill captured its read.
        age: Duration,
    },
    /// No background round has proved the table since it was registered, so nothing
    /// of it may be served.
    TableNotProved,
}

/// The outcome of one [`RollupCache::lookup`].
#[derive(Debug, PartialEq)]
pub enum Lookup {
    /// The whole materialized chunk.
    Hit {
        /// The complete result of the chunk.
        payload: Arc<RecordBatches>,
        /// The warnings and infos the read behind `payload` recorded.
        annotations: Arc<CachedAnnotations>,
    },
    /// The chunk cannot be served; execute storage and fill it.
    Miss(MissReason),
    /// The chunk must not go through the cache at all.
    Bypass(BypassReason),
}

/// The outcome of one [`RollupCache::insert`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Insert {
    /// The payload became the entry of the chunk.
    Stored,
    /// The fill was not published: the table was invalidated after it was admitted, the
    /// region domain or the metadata version it read moved under it, or the state — or the
    /// query state — it registered was reclaimed and replaced, so the read behind the payload may
    /// answer with data nothing vouches for. A read whose region set is not the committed one is
    /// refused here too.
    Rejected,
    /// The chunk is empty, so nothing was written.
    Bypassed,
}

/// What one background probe round of a source table proved.
///
/// The probe reports a timestamp hull of the rows written after the round's
/// cursors, but a sequence can also advance without leaving a timestamp behind (a
/// DELETE whose row the ordinary scan no longer returns). The three answers keep
/// those cases apart, because "nothing changed" must not evict and "something
/// changed somewhere" must.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DirtySpan {
    /// Nothing changed since the last successful round: every entry stays.
    Nothing,
    /// Only rows inside this half-open timestamp hull changed.
    Hull(TimeChunk),
    /// Rows changed at timestamps the probe cannot bound, so every entry of the
    /// table goes.
    Everything,
}

/// What the cache remembers about one source table.
///
/// The background refresher is the only writer of the cursors, the readiness, and
/// the per-query invalidated watermarks; the request path only registers the table, reads
/// its readiness, and compares a read's sequence against that watermark when it publishes.
///
/// A round that cannot be applied to what is committed does not empty a state: it registers a
/// replacement (see [`RollupCache::record_refresh`]), so the state a request was admitted under
/// stays identifiable and becomes inert.
#[derive(Debug)]
pub(crate) struct TableState {
    table: SourceTable,
    inner: Mutex<TableStateInner>,
}

#[derive(Debug, Default)]
struct TableStateInner {
    /// The per-region sequence the background refresher has proved up to.
    ///
    /// Its keys are the table's committed region domain: every round's vector is compared
    /// with this one, and a query that registers while it is non-empty starts its own
    /// watermark here instead of starting cold.
    cursors: BTreeMap<u64, u64>,
    /// The metadata version of the source table these cursors were committed against.
    ///
    /// A schema change moves it without moving any sequence, and a schema change can change
    /// the value of rows that are already stored — a column dropped and added back under the
    /// same name and type answers its default for the same timestamps — so a round of another
    /// version empties the table exactly like a change it cannot bound.
    source_version: Option<u64>,
    /// The id of the source table these cursors were committed against, as the catalog
    /// resolved it.
    ///
    /// A metric logical table can be rebuilt under its own name: the name stays, the metadata
    /// version stays, the physical regions behind it stay, and only its logical id moves. What
    /// a round proved is therefore a proof about this id too, and a round of another one
    /// restarts the table exactly like a version change.
    source_table_id: Option<u32>,
    /// Whether a background round has proved this table since it was registered.
    ///
    /// A lookup is answered only for a ready table: an unproved table's entries may
    /// predate writes that nobody has looked at yet, so serving them would answer
    /// with data no probe has vouched for.
    ready: bool,
    /// Per query identity, what decides whether that query's fills may be stored.
    ///
    /// It is per query and not per table, so a dirty hull that reaches one query's
    /// dependencies cannot reject the fills of another query that reads the same
    /// table but different data.
    ///
    /// The map is pruned by every round: a query whose payloads are all gone and that no fill
    /// has registered for [`IDLE_TABLE_TIMEOUT`] is dropped, and the whole table is forgotten
    /// once it goes idle, so it is bounded by the identities read in the last
    /// [`IDLE_TABLE_TIMEOUT`].
    queries: HashMap<QueryIdentity, QueryState>,
    /// When the last round proved the table, which is what finds a failing
    /// refresher. It is observation only: it gates nothing.
    last_success: Option<Instant>,
    /// When a request last admitted this table, which is what keeps it active.
    last_used: Option<Instant>,
}

impl TableState {
    fn new(table: SourceTable) -> Self {
        Self {
            table,
            inner: Mutex::new(TableStateInner::default()),
        }
    }

    /// The table this state belongs to.
    fn table(&self) -> &SourceTable {
        &self.table
    }
}

/// The publication state of one query identity on one source table.
///
/// It is per query and not per table, so an invalidation of one query cannot reject the
/// fills of another query that reads the same table but different data.
#[derive(Debug)]
struct QueryState {
    /// The source sequence the last invalidation of this query was committed at, per
    /// region.
    ///
    /// It is the round's high watermark, a complete vector rather than one number: a
    /// region that fell behind must not be hidden by another region's progress. A fill
    /// publishes only when its read reported the same regions and is at least as far in
    /// every one of them, so a payload materialized before the invalidation cannot be
    /// published after it. It starts at the table's committed cursors — or empty while the
    /// table has no domain yet, in which case the table's first round drops the query instead of
    /// committing a watermark over it, because nothing bounds a read taken under no domain — and
    /// only a round moves it.
    invalidated_watermark: BTreeMap<u64, u64>,
    /// How many times this query's cache changed.
    ///
    /// It advances on every published payload and on every committed invalidation, and it
    /// admits nothing: it is the count of changes, not a token a fill has to hold.
    version: u64,
    /// When a fill of this query last registered, or when a round last found a payload of it
    /// resident.
    ///
    /// It is what reclaims a query nothing keeps any more: a state with no resident payload
    /// that no fill has registered for [`IDLE_TABLE_TIMEOUT`] is dropped by a round, so a table
    /// many identities read once does not keep a watermark per identity for as long as it is
    /// active.
    last_used: Instant,
    /// The marker of this state itself, which every fill of it captures.
    ///
    /// A reclaim drops the state and a later fill of the same identity registers another one, so
    /// a fill that is still reading has to be able to tell its own registration from the one that
    /// replaced it: it holds this marker and publishes only into the state it took it from (see
    /// [`RollupCache::insert`]). An `Arc<()>` is enough, because there is nothing in it but its
    /// address.
    lifecycle: Arc<()>,
}

impl QueryState {
    /// Whether a read that reported `read_through` may be published over this query's last
    /// invalidation.
    ///
    /// The read has to report exactly the regions the invalidation covers — the same key set,
    /// not a subset and not a superset — and be at least as far in every one of them: a read
    /// that opened exactly at the invalidated sequence saw everything that invalidation
    /// described, so equality is enough. A different set of regions cannot be compared at all,
    /// because a read that saw fewer regions proves less and one that saw more may have read
    /// regions this table never committed. An empty watermark is the cold case: the table had
    /// no proved domain when the fill registered, so the read's own vector is the only bound
    /// there is.
    fn is_past_watermark(&self, read_through: &BTreeMap<u64, u64>) -> bool {
        let watermark = &self.invalidated_watermark;
        if watermark.is_empty() {
            return !read_through.is_empty();
        }
        read_through.len() == watermark.len()
            && watermark.iter().all(|(region, sequence)| {
                read_through
                    .get(region)
                    .is_some_and(|read| read >= sequence)
            })
    }
}

/// What one request may do with one source table.
///
/// It is captured once per request, before the first read, so every chunk of the
/// request is answered against one table state even if the refresher finishes a
/// round while the request runs.
///
/// It is not a claim that the table is servable: readiness is re-read under the table
/// lock on every lookup, so a failing round stops the serving at the next chunk. Nor does it
/// outlive a restart of the table, the round that establishes its baseline included: the state it
/// captured is replaced then, and its lookups miss and its publications are refused for the rest
/// of the request.
pub struct Admission {
    state: Arc<TableState>,
}

impl Admission {
    /// The table the request reads.
    pub fn table(&self) -> &SourceTable {
        self.state.table()
    }
}

impl fmt::Debug for Admission {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Admission")
            .field("table", &self.state.table())
            .finish()
    }
}

/// What one fill declared before it read storage.
///
/// It is created before the read and registers the query, so a round that invalidates while
/// the fill runs moves the watermark the publication is judged against; it carries the marker of
/// the registration it made, so a state a round reclaimed and another fill of the same identity
/// registered again is not its own, and what may be published is decided by the sequence the read
/// reports.
#[derive(Debug)]
pub struct FillGuard {
    state: Arc<TableState>,
    identity: QueryIdentity,
    /// The [`QueryState::lifecycle`] of the registration this fill made.
    lifecycle: Arc<()>,
    chunk: TimeChunk,
    dependency: TimeChunk,
    captured_at: SystemTime,
}

/// Why a [`RollupCacheConfig`] is unusable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigError {
    /// The cache must be allowed some bytes.
    Bytes,
    /// A chunk must cover at least one millisecond.
    Chunk,
    /// The refresh period must leave the cache time to run a round at all.
    RefreshInterval,
    /// The age bound must accept an entry at all.
    MaxAge,
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Bytes => f.write_str("the cache byte budget must be positive"),
            Self::Chunk => f.write_str("the chunk duration must be positive"),
            Self::RefreshInterval => f.write_str("the refresh interval must be positive"),
            Self::MaxAge => f.write_str("the max age must be positive"),
        }
    }
}

impl std::error::Error for ConfigError {}

/// The bounds a [`RollupCache`] enforces.
///
/// `tail` is a [`Duration`], so it is non-negative by construction and needs no
/// check; `max_bytes`, `chunk`, `refresh_interval`, and `max_age` all have to be
/// positive.
///
/// `chunk` cannot be validated against the step here, because the step belongs to
/// the query, not the cache: [`RollupCacheConfig::chunk_steps`] reports whether a
/// given step suits it, and planning bypasses the cache when it does not.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RollupCacheConfig {
    max_bytes: u64,
    tail: Duration,
    chunk: Duration,
    refresh_interval: Duration,
    max_age: Duration,
}

impl RollupCacheConfig {
    /// The validated configuration of a cache.
    pub fn new(
        max_bytes: u64,
        tail: Duration,
        chunk: Duration,
        refresh_interval: Duration,
        max_age: Duration,
    ) -> Result<Self, ConfigError> {
        if max_bytes == 0 {
            return Err(ConfigError::Bytes);
        }
        if chunk.is_zero() {
            return Err(ConfigError::Chunk);
        }
        if refresh_interval.is_zero() {
            return Err(ConfigError::RefreshInterval);
        }
        if max_age.is_zero() {
            return Err(ConfigError::MaxAge);
        }
        Ok(Self {
            max_bytes,
            tail,
            chunk,
            refresh_interval,
            max_age,
        })
    }

    /// The bytes resident entries may occupy before eviction.
    pub fn max_bytes(&self) -> u64 {
        self.max_bytes
    }

    /// How close to the wall clock output points stop being cacheable.
    pub fn tail(&self) -> Duration {
        self.tail
    }

    /// How much of the grid one chunk covers.
    pub fn chunk(&self) -> Duration {
        self.chunk
    }

    /// How often the background refresher probes an active source table.
    ///
    /// It is the only thing this period controls: a request reads the table's
    /// readiness instead of waiting for, or measuring, a round.
    pub fn refresh_interval(&self) -> Duration {
        self.refresh_interval
    }

    /// How old an entry may be before it stops being served, counted from the
    /// instant its fill captured before reading storage.
    pub fn max_age(&self) -> Duration {
        self.max_age
    }

    /// The configured chunk as a whole number of steps of `step_ms`.
    ///
    /// `None` means the chunk is not a whole multiple of the step, so canonical
    /// chunk bounds would fall between the statement's grid points; planning
    /// rejects such a combination ([`BypassReason::MisalignedChunk`]) rather than
    /// caching spans the evaluator never produced.
    pub fn chunk_steps(&self, step_ms: i64) -> Option<u64> {
        chunk_steps(self.chunk, step_ms)
    }
}

/// A statement planned into the chunk keys a lookup uses, plus what to read
/// directly.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlannedChunks {
    span: QuerySpan,
    chunks: Vec<TimeChunkKey>,
    direct: Vec<DirectSpan>,
    tail: Option<TimeChunk>,
}

impl PlannedChunks {
    /// The coverage every span below partitions.
    pub fn span(&self) -> QuerySpan {
        self.span
    }

    /// The cacheable chunks of the statement, in time order.
    pub fn chunks(&self) -> &[TimeChunkKey] {
        &self.chunks
    }

    /// The spans to execute without the cache, in time order.
    pub fn direct(&self) -> &[DirectSpan] {
        &self.direct
    }

    /// The part of the coverage at or after `now - tail`.
    pub fn tail(&self) -> Option<TimeChunk> {
        self.tail
    }
}

/// The result of planning a statement against a cache.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChunkPlan {
    /// The statement must not use the cache at all.
    Bypass(BypassReason),
    /// The statement's cacheable chunks and the spans to read directly.
    Planned(PlannedChunks),
}

/// The PromQL annotations (warnings and infos) one chunk's read recorded.
///
/// A hit serves the rows of a read that already happened, so it cannot re-run the
/// code that observed what the read saw. Recording those observations beside the
/// rows lets a served chunk report the same warnings and infos a fresh read of it
/// reported, instead of dropping them because the evaluation was skipped.
///
/// Both lists are sorted and deduplicated, exactly as the collector a request
/// reports from keeps them, so replaying them cannot duplicate a message.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CachedAnnotations {
    warnings: Vec<String>,
    infos: Vec<String>,
}

impl CachedAnnotations {
    /// The annotations of one read, sorted and deduplicated.
    pub fn new(mut warnings: Vec<String>, mut infos: Vec<String>) -> Self {
        warnings.sort();
        warnings.dedup();
        infos.sort();
        infos.dedup();
        Self { warnings, infos }
    }

    /// The warnings the read recorded, in order.
    pub fn warnings(&self) -> &[String] {
        &self.warnings
    }

    /// The infos the read recorded, in order.
    pub fn infos(&self) -> &[String] {
        &self.infos
    }

    /// Whether the read recorded nothing.
    pub fn is_empty(&self) -> bool {
        self.warnings.is_empty() && self.infos.is_empty()
    }

    /// The heap bytes these messages hold, which is what an entry is charged for
    /// them.
    ///
    /// The text is the whole cost: the two lists and their pointers are covered by
    /// the per-entry overhead every entry pays.
    fn bytes(&self) -> u64 {
        self.warnings
            .iter()
            .chain(self.infos.iter())
            .map(|message| u64::try_from(message.len()).unwrap_or(u64::MAX))
            .sum()
    }
}

/// One cached chunk: the complete materialized result and how it was validated.
#[derive(Debug, Clone)]
struct Entry {
    payload: Arc<RecordBatches>,
    /// What the read that produced `payload` recorded, served with it.
    annotations: Arc<CachedAnnotations>,
    /// The span of source data the payload was materialized from.
    ///
    /// A dirty input range invalidates the entry when it overlaps this span, not
    /// the chunk's own output span: the history a chunk folded can reach before
    /// its first output point.
    dependency: TimeChunk,
    /// The bytes this entry is charged: its payload plus a fixed overhead.
    weight: u32,
    /// When the fill that produced the payload captured its read, which its age is
    /// counted from.
    filled_at: SystemTime,
    /// The source sequence the read behind the payload reached, per region.
    ///
    /// It is the payload's source watermark: what the read itself observed, kept with the
    /// payload as the record of how current it is. Nothing on the lookup path compares it —
    /// a round invalidates by dropping the entries its hull reaches — and a read that could
    /// not report it is never published.
    read_through: BTreeMap<u64, u64>,
}

/// A bounded, frontend-local cache of whole PromQL chunks.
///
/// The cache is read-through: [`RollupCache::lookup`] answers from memory or says
/// why it cannot, and the caller executes storage and calls [`RollupCache::insert`].
/// Entries are evicted by moka in admission order once the byte budget is spent,
/// which is bounded but not a strict recency ordering.
///
/// Two pieces of state decide what a request may do: the entries, and one
/// [`TableState`] per source table a request has been admitted for. The background
/// refresher is the only writer of a table's cursors, its readiness, and its query
/// watermarks when it invalidates; a request only registers its table, reads its
/// readiness, and compares the sequence its read reported against that watermark when it
/// publishes.
pub struct RollupCache {
    config: RollupCacheConfig,
    entries: Cache<TimeChunkKey, Entry>,
    /// The active source tables, keyed by fully qualified name.
    ///
    /// The map is the cache's record of what to refresh: a table is registered by
    /// the first request admitted for it, kept active while requests keep admitting
    /// it, and dropped once no request has for [`IDLE_TABLE_TIMEOUT`].
    tables: Mutex<HashMap<SourceTable, Arc<TableState>>>,
    /// How many lookups were answered from an existing entry.
    ///
    /// Only a test reads it ([`RollupCache::hit_count`]); it is compiled out of a
    /// production build and never takes part in a lookup decision.
    #[cfg(any(test, feature = "testing"))]
    hits: AtomicU64,
}

impl RollupCache {
    /// A cache bounded by `config`.
    pub fn new(config: RollupCacheConfig) -> Self {
        let entries = Cache::builder()
            .max_capacity(config.max_bytes())
            .weigher(|_key: &TimeChunkKey, entry: &Entry| entry.weight)
            .build();
        Self {
            config,
            entries,
            tables: Mutex::new(HashMap::new()),
            #[cfg(any(test, feature = "testing"))]
            hits: AtomicU64::new(0),
        }
    }

    /// The configuration this cache enforces.
    pub fn config(&self) -> &RollupCacheConfig {
        &self.config
    }

    /// How many entries the cache holds once pending maintenance has run.
    ///
    /// Tests state with it that a request filled exactly the chunks it read, or that
    /// it filled nothing because it never took the cached path. Production reads
    /// nothing of the sort, so it is not part of the cache's surface.
    #[cfg(any(test, feature = "testing"))]
    pub fn entry_count(&self) -> u64 {
        self.entries.run_pending_tasks();
        self.entries.entry_count()
    }

    /// How many lookups this cache has answered from an existing entry.
    ///
    /// Only a served entry advances it: a miss, a fill, and a bypass never do, so a
    /// test can tell a reused chunk from one that was executed again and stored
    /// again, which an unchanged [`Self::entry_count`] cannot show on its own.
    /// Production reads nothing of the sort, so it is not part of the cache's
    /// surface.
    #[cfg(any(test, feature = "testing"))]
    pub fn hit_count(&self) -> u64 {
        self.hits.load(Ordering::SeqCst)
    }

    /// How many query identities `table` still tracks.
    ///
    /// Tests state with it that a query nothing keeps is reclaimed and one with a resident
    /// payload is not; production reads nothing of the sort, so it is not part of the cache's
    /// surface.
    #[cfg(any(test, feature = "testing"))]
    pub fn query_count(&self, table: &SourceTable) -> usize {
        self.table_state(table)
            .map(|state| state.inner.lock().unwrap().queries.len())
            .unwrap_or(0)
    }

    /// Plans `stmt` of `identity` into chunk keys and direct spans as of `now`.
    ///
    /// The returned direct spans are the caller's to send to storage; their points
    /// are never cached.
    pub fn plan(&self, identity: QueryIdentity, stmt: &EvalStmt, now: SystemTime) -> ChunkPlan {
        // The identity claims the grid its chunks were cut from; a statement on a
        // different step would key the same span under the wrong grid.
        if EvalGrid::of(stmt).is_ok_and(|grid| grid.step_ms() != identity.step_ms) {
            return ChunkPlan::Bypass(BypassReason::StepMismatch);
        }
        match plan_chunks(stmt, self.config.tail(), self.config.chunk(), now) {
            Err(reason) => ChunkPlan::Bypass(reason),
            Ok(spans) => {
                let (span, chunks, direct, tail) = spans.into_parts();
                let chunks = chunks
                    .into_iter()
                    .map(|chunk| TimeChunkKey::new(identity.clone(), chunk))
                    .collect();
                ChunkPlan::Planned(PlannedChunks {
                    span,
                    chunks,
                    direct,
                    tail,
                })
            }
        }
    }

    /// Registers `table` as active for one request and captures the state its
    /// lookups and fills are validated against.
    ///
    /// Admission never fails and never waits for the background refresher: whether a
    /// table may be served is decided per lookup, and a cold or unproved table simply
    /// answers [`MissReason::TableNotProved`] until a round succeeds. Registering here
    /// is also what keeps the table on the refresher's list, and what a table is
    /// idle-evicted by.
    pub fn admit(&self, table: &SourceTable, now: Instant) -> Admission {
        let state = {
            let mut tables = self.tables.lock().unwrap();
            Arc::clone(
                tables
                    .entry(table.clone())
                    .or_insert_with(|| Arc::new(TableState::new(table.clone()))),
            )
        };
        // Registered under the table lock, so the refresh round that decides what to
        // probe sees this request before it decides the table is idle.
        state.inner.lock().unwrap().last_used = Some(now);
        Admission { state }
    }

    /// Tries to serve `key` from memory as of `now`.
    ///
    /// The table's readiness is read under its lock on every call, so a round that
    /// fails while the request runs stops the serving at the next chunk. This is local
    /// and infallible: it reads at most one entry, never storage, and never waits for
    /// an in-flight fill or for cache maintenance.
    pub fn lookup(&self, admission: &Admission, key: &TimeChunkKey, now: SystemTime) -> Lookup {
        if key.chunk().is_empty() {
            return Lookup::Bypass(BypassReason::EmptyChunk);
        }
        // The readiness and the entry are read under the same lock, so a restart cannot slip
        // between them: a request admitted under a state that is no longer registered sees that
        // state closed, and can never be served an entry the replacement filled. Nothing here
        // awaits, so the lock is held for a read of one entry and no longer.
        let (filled_at, payload, annotations) = {
            let inner = admission.state.inner.lock().unwrap();
            if !inner.ready {
                return Lookup::Miss(MissReason::TableNotProved);
            }
            let Some(entry) = self.entries.get(key) else {
                return Lookup::Miss(MissReason::NotFound);
            };
            (entry.filled_at, entry.payload, entry.annotations)
        };
        let age = now.duration_since(filled_at).unwrap_or_default();
        if age > self.config.max_age() {
            return Lookup::Miss(MissReason::Expired { age });
        }
        // The entry is served: nothing else in this cache counts as a hit.
        #[cfg(any(test, feature = "testing"))]
        self.hits.fetch_add(1, Ordering::SeqCst);
        Lookup::Hit {
            payload,
            annotations,
        }
    }

    /// Registers one fill of `key` as reading storage, before it reads.
    ///
    /// `dependency` is the span of source data the read touches, which a round compares
    /// with its dirty hull. `captured_at` is the instant the fill captured before it
    /// read, which the entry's age is counted from.
    ///
    /// The returned [`FillGuard`] is registered with the query, so a round that invalidates
    /// while the fill runs moves the watermark the publication is judged against, and it carries
    /// that registration's marker, so a state a round reclaimed is not the one it publishes into.
    ///
    /// A query that registers while the table already has a committed domain starts its
    /// watermark at the table's cursors rather than at nothing: the table is not cold, so a
    /// read of a new identity has no more right to publish an unproved sequence than one of
    /// an identity that was already reading it. A table that had no domain yet gets one with
    /// its first round, and that round restarts the table, so a fill that registered while it
    /// was cold is dropped with it instead of being judged against the baseline (see
    /// [`RollupCache::record_refresh`]).
    ///
    /// Registering also marks the query as used, which is what keeps a round from reclaiming
    /// the state of a query that is still being read.
    pub fn begin_fill(
        &self,
        admission: &Admission,
        key: &TimeChunkKey,
        dependency: TimeChunk,
        captured_at: SystemTime,
    ) -> FillGuard {
        let lifecycle = {
            let mut inner = admission.state.inner.lock().unwrap();
            let watermark = inner.cursors.clone();
            let now = Instant::now();
            // Registered before the read: the identity a round has to move the watermark of
            // exists while this fill runs, and registering is what keeps a state nothing has
            // filled for a while from being reclaimed under it.
            let query = inner
                .queries
                .entry(key.identity().clone())
                .or_insert_with(|| QueryState {
                    invalidated_watermark: watermark,
                    version: 0,
                    last_used: now,
                    lifecycle: Arc::new(()),
                });
            query.last_used = now;
            Arc::clone(&query.lifecycle)
        };
        FillGuard {
            state: Arc::clone(&admission.state),
            identity: key.identity().clone(),
            lifecycle,
            chunk: key.chunk(),
            dependency,
            captured_at,
        }
    }

    /// Stores `payload` as the whole result of the chunk `fill` was registered for.
    ///
    /// The payload is published only when its read reported exactly the regions the query's
    /// invalidated watermark covers and is at least as far in every one of them, and at least
    /// as far as whatever the chunk holds with at least one region strictly further. A payload
    /// a round overtook is therefore refused, and so is one of another region domain and one
    /// that would replace a newer payload; an entry past its age bound may still be refreshed
    /// by a read of the same sequence. The request that read a refused payload still gets it;
    /// the cache just does not keep it.
    ///
    /// A publication is a cache change, so it advances the query's version counter, which
    /// admits nothing and only records that the cache changed.
    ///
    /// The publication also requires the table to still be registered under the state the
    /// request was admitted under and that state to still hold the registration this fill made, so
    /// a state or a query that was reclaimed or restarted rejects the fill instead of publishing
    /// into its replacement — even when another fill of the same identity has already registered
    /// its query there. Every check and the store happen without releasing the table lock.
    ///
    /// The entry's input dependency is the span the fill registered: a dirty hull
    /// invalidates the entry when it reaches that span, not the chunk's own output span,
    /// because the history a chunk folded can reach before its first point.
    pub fn insert(
        &self,
        fill: &FillGuard,
        read_through: &BTreeMap<u64, u64>,
        payload: Arc<RecordBatches>,
        annotations: Arc<CachedAnnotations>,
    ) -> Insert {
        if fill.chunk.is_empty() {
            return Insert::Bypassed;
        }
        // A read that could not report the source sequence it reached cannot be ordered
        // against an invalidation, so it is not published at all.
        if read_through.is_empty() {
            return Insert::Rejected;
        }
        // Registry before table state, the order every writer here uses.
        let tables = self.tables.lock().unwrap();
        let current = tables.get(fill.state.table());
        if !current.is_some_and(|current| Arc::ptr_eq(current, &fill.state)) {
            return Insert::Rejected;
        }
        let mut inner = fill.state.inner.lock().unwrap();
        let Some(query) = inner.queries.get_mut(&fill.identity) else {
            return Insert::Rejected;
        };
        // The query has to be the registration this fill made. A reclaim drops the state it
        // registered, and a later fill of the same identity registers another one whose watermark
        // admits this read: publishing would store a payload of the reclaimed read into the
        // registration of the other one.
        if !Arc::ptr_eq(&query.lifecycle, &fill.lifecycle) {
            return Insert::Rejected;
        }
        if !query.is_past_watermark(read_through) {
            // The round that invalidated past this read has already committed.
            return Insert::Rejected;
        }
        let key = TimeChunkKey::new(fill.identity.clone(), fill.chunk);
        if let Some(resident) = self.entries.get(&key) {
            // The resident payload's age is measured at the instant this request captured,
            // which is the request's own clock and not the wall clock of the insertion.
            let age = fill
                .captured_at
                .duration_since(resident.filled_at)
                .unwrap_or_default();
            let expired = age > self.config.max_age();
            if !may_replace(read_through, &resident.read_through, expired) {
                return Insert::Rejected;
            }
        }
        // The publication is a cache change, so the version counts it.
        query.version = query.version.saturating_add(1);
        let entry = Entry {
            weight: Entry::weigh(&payload, &annotations),
            payload,
            annotations,
            dependency: fill.dependency,
            filled_at: fill.captured_at,
            read_through: read_through.clone(),
        };
        self.entries.insert(key, entry);
        Insert::Stored
    }

    /// The active source tables a background round must refresh, most recently used
    /// first.
    ///
    /// Tables no request has admitted for [`IDLE_TABLE_TIMEOUT`] are dropped here,
    /// together with the entries they left behind: a table nothing reads must not keep
    /// occupying the refresher's rounds or the byte budget. The next request for it
    /// registers a fresh state and is answered from storage until a round proves it.
    pub fn tables_to_refresh(&self, now: Instant) -> Vec<SourceTable> {
        let mut tables = self.tables.lock().unwrap();
        let mut active = Vec::with_capacity(tables.len());
        let mut idle = Vec::new();
        for (table, state) in tables.iter() {
            let last_used = state.inner.lock().unwrap().last_used;
            let is_idle = last_used
                .is_some_and(|used| now.saturating_duration_since(used) > IDLE_TABLE_TIMEOUT);
            if is_idle {
                idle.push(table.clone());
            } else {
                active.push((last_used, table.clone()));
            }
        }
        for table in idle {
            if let Some(state) = tables.remove(&table) {
                self.forget_table(&state);
            }
        }
        // Most recently used first, so a refresher that cannot keep up with every table
        // still keeps up with the ones requests are reading now.
        active.sort_by(|left, right| right.0.cmp(&left.0).then_with(|| left.1.cmp(&right.1)));
        active.into_iter().map(|(_, table)| table).collect()
    }

    /// The per-region cursors a background round of `table` starts from.
    pub fn cursors(&self, table: &SourceTable) -> BTreeMap<u64, u64> {
        let Some(state) = self.table_state(table) else {
            return BTreeMap::new();
        };
        state.inner.lock().unwrap().cursors.clone()
    }

    /// Records one successful background round of `table`.
    ///
    /// A round that proved nothing changed keeps every entry, every watermark, and advances
    /// nothing but the cursors and the readiness. A round that found a dirty hull, or a
    /// change it cannot bound, commits one invalidation — the entries whose dependency the
    /// change reaches are dropped and every query of the table moves its watermark to the
    /// round's high watermark — and only then advances the cursors and the readiness.
    /// Nothing here waits for a fill: a fill that is still reading is refused when it tries
    /// to publish.
    ///
    /// Every round, a quiet one included, is first compared with the cursors the table
    /// committed: its vector has to name the same regions and go back in none of them. A
    /// region that went back is a vector of something the cache cannot describe, so the round
    /// proves nothing, the table stops being served, and nothing at all is advanced.
    ///
    /// A table's first round starts from zero cursors, so its delta covers every row the
    /// table has ever had, whatever a request filled before the table was proved included — all
    /// of it read under no domain. That round is the table's baseline, and it restarts the table
    /// for the same reason the rounds below do: nothing it proves can vouch for a read that was
    /// taken under no domain.
    ///
    /// Three rounds cannot be applied to what is committed and instead restart the table from
    /// their own vector: one whose regions are of another domain — a route that gained or lost a
    /// region re-keys the cursors — one that read another metadata version, because a schema
    /// change can answer differently for rows that are already stored even when no sequence
    /// moved, and one that read another table id, because a metric logical table rebuilt under its
    /// own name keeps its version while its logical identity moves. Each replaces the state the
    /// table is registered under, so nothing read before it can be published or served after it.
    ///
    /// A committed round also reclaims the queries of the table that nothing keeps any more: see
    /// [`RollupCache::reclaim_queries`].
    pub fn record_refresh(
        &self,
        table: &SourceTable,
        cursors: BTreeMap<u64, u64>,
        source_version: u64,
        source_table_id: u32,
        dirty: DirtySpan,
        now: Instant,
    ) -> bool {
        // The registry before the table state, the order [`RollupCache::insert`] uses: a round
        // that restarts the table swaps the state it is registered under while holding both, so
        // neither a publication nor an admission can observe the pair half swapped.
        let mut tables = self.tables.lock().unwrap();
        let Some(state) = tables.get(table).cloned() else {
            return true;
        };
        let high = cursors.clone();
        let mut inner = state.inner.lock().unwrap();
        // A vector that names no region is not a domain: it cannot be committed over anything,
        // and it is not a domain change to restart from either.
        if high.is_empty() {
            inner.ready = false;
            return false;
        }
        let restart = match domain_of(&inner.cursors, &high) {
            Domain::Regressed => {
                // The round's vector cannot replace what is committed — a region went back — so
                // the table proves nothing: it stops being served, nothing is committed, and the
                // cursors stay where they were so the next round sees the same delta.
                inner.ready = false;
                return false;
            }
            Domain::Rekeyed => true,
            // The table's first round: the committed cursors are empty, so everything a request
            // filled before it was read under no domain at all. It restarts the table instead of
            // opening the cold one, because nothing it proves can vouch for a cold read.
            Domain::Baseline => true,
            // A version or a table id moves no sequence, so it would otherwise pass as a quiet
            // round.
            Domain::Same => {
                inner.source_version != Some(source_version)
                    || inner.source_table_id != Some(source_table_id)
            }
        };
        if restart {
            self.reset_table(
                &mut tables,
                table,
                &mut inner,
                cursors,
                source_version,
                source_table_id,
                now,
            );
            return true;
        }
        if !matches!(dirty, DirtySpan::Nothing) && !self.can_commit(&inner, &high) {
            // The round's vector cannot replace what is committed — it is empty, of another
            // domain, or behind a region — so the table proves nothing: it stops being served,
            // nothing is committed, and the cursors stay where they were so the next round
            // sees the same delta.
            inner.ready = false;
            return false;
        }
        match dirty {
            DirtySpan::Nothing => {}
            DirtySpan::Hull(span) => self.invalidate(table, &mut inner, Some(span), &high),
            DirtySpan::Everything => self.invalidate(table, &mut inner, None, &high),
        }
        inner.cursors = cursors;
        inner.ready = true;
        inner.source_version = Some(source_version);
        inner.source_table_id = Some(source_table_id);
        inner.last_success = Some(now);
        // Committed under the table lock, so a fill that registers after this round — or a query
        // that is still being read — is seen by the reclamation of this one.
        self.reclaim_queries(table, &mut inner, now);
        true
    }

    /// Replaces the state `table` is registered under, restarting it from the round's `cursors`.
    ///
    /// This is what a round that cannot be applied to the committed state does: the table's first
    /// round, one of another region domain, one of another metadata version, and one of another
    /// table id. The entries go because their payloads were materialized from data the baseline,
    /// the new domain, the new version or the new table may describe differently.
    ///
    /// The state itself is replaced rather than emptied, and the replacement carries the round's
    /// vector, version and id as its baseline. A fill that is reading right now holds the state it
    /// was admitted under: emptying that one in place would let it publish into the tables of a
    /// state that a later fill of the same identity has registered again, because the identity is
    /// the same. A replacement makes the publication fail on the identity of the state alone, and
    /// the queries of the old state are cleared and its readiness closed, so nothing can be
    /// published into it nor served from it either.
    ///
    /// The registry stays held while the entries are dropped, so no publication — of the old state
    /// or of the replacement — can slip between the swap and the removals.
    // The restart is the round: its vector, its version, its table id and its instant are exactly
    // the proof being committed, and bundling them into another type would only rename them.
    #[allow(clippy::too_many_arguments)]
    fn reset_table(
        &self,
        tables: &mut HashMap<SourceTable, Arc<TableState>>,
        table: &SourceTable,
        inner: &mut TableStateInner,
        cursors: BTreeMap<u64, u64>,
        source_version: u64,
        source_table_id: u32,
        now: Instant,
    ) {
        let last_used = inner.last_used;
        inner.ready = false;
        inner.queries.clear();
        let regions = cursors.len();
        tables.insert(
            table.clone(),
            Arc::new(TableState {
                table: table.clone(),
                inner: Mutex::new(TableStateInner {
                    cursors,
                    source_version: Some(source_version),
                    source_table_id: Some(source_table_id),
                    ready: true,
                    queries: HashMap::new(),
                    last_success: Some(now),
                    last_used,
                }),
            }),
        );
        let dropped = self.invalidate_table(table);
        debug!(
            "Rollup cache restarted table {table} (id {source_table_id}) on {regions} regions of version {source_version} and dropped {dropped} entries",
        );
    }

    /// Drops the query states of `table` that nothing keeps any more.
    ///
    /// A query state is a watermark, a version and a marker, registered by every fill and removed
    /// by none, so a table read by many identities would keep one per identity for as long as the
    /// table is active. A state is kept while a payload of it is resident, or while a fill of it
    /// registered within [`IDLE_TABLE_TIMEOUT`]; dropping it is safe, because a fill that is
    /// still reading then finds no query — or one that another fill of its identity registered —
    /// when it publishes and is refused ([`RollupCache::insert`]), which is a miss rather than a
    /// payload nothing vouches for.
    ///
    /// The scan of the entries only runs when a state is already idle, so a round of a table whose
    /// queries are all current pays a pass over the query states alone instead of the cache. An
    /// idle state that the scan does find resident is kept and moved a whole timeout forward, so
    /// the next rounds read the query states alone again instead of scanning the cache for the
    /// same answer every round.
    fn reclaim_queries(&self, table: &SourceTable, inner: &mut TableStateInner, now: Instant) {
        let idle = |query: &QueryState| {
            now.saturating_duration_since(query.last_used) > IDLE_TABLE_TIMEOUT
        };
        if !inner.queries.values().any(idle) {
            return;
        }
        let held: Vec<Arc<TimeChunkKey>> = self
            .entries
            .iter()
            .filter(|(key, _)| table.owns(key.identity()))
            .map(|(key, _)| key)
            .collect();
        let resident: HashSet<&QueryIdentity> = held.iter().map(|key| key.identity()).collect();
        let before = inner.queries.len();
        inner.queries.retain(|identity, query| {
            if !idle(query) {
                return true;
            }
            if resident.contains(identity) {
                // Kept, and checked again only after another timeout: the payload is what the
                // state is kept for, and the next rounds must not scan the cache for it again.
                query.last_used = now;
                return true;
            }
            false
        });
        debug!(
            "Rollup cache reclaimed {} idle queries of table {table}",
            before.saturating_sub(inner.queries.len()),
        );
    }

    /// Whether the round's `high` may be committed over every query's watermark of `table`.
    ///
    /// The new vector has to be complete, cover the same regions as the one it replaces, and
    /// go no further back in any of them. A round that cannot say that proves nothing, and
    /// the caller must not serve or advance on it. The vector is assigned, never merged: a
    /// merged bound would keep an older sequence for a region this round moved past.
    fn can_commit(&self, inner: &TableStateInner, high: &BTreeMap<u64, u64>) -> bool {
        if high.is_empty() {
            return false;
        }
        inner.queries.values().all(|query| {
            let previous = &query.invalidated_watermark;
            previous.is_empty()
                || (previous.len() == high.len()
                    && previous.iter().all(|(region, sequence)| {
                        high.get(region)
                            .is_some_and(|committed| committed >= sequence)
                    }))
        })
    }

    /// Records one failed background round of `table`.
    ///
    /// A failed round proves nothing, so the table stops being served until a later
    /// round succeeds. Its cursors stay where the last proved round left them, so the
    /// next round covers everything this one missed, and its entries stay resident:
    /// nothing says they are wrong, and a round that finds the change drops them.
    pub fn record_failure(&self, table: &SourceTable) {
        let Some(state) = self.table_state(table) else {
            return;
        };
        state.inner.lock().unwrap().ready = false;
    }

    /// The state `table` is registered under, if any request has admitted it.
    fn table_state(&self, table: &SourceTable) -> Option<Arc<TableState>> {
        self.tables.lock().unwrap().get(table).cloned()
    }

    /// Drops what an idle table left behind.
    ///
    /// The readiness closes, the query watermarks go with the state, and the entries are
    /// removed. A fill that was admitted under the evicted state is refused by
    /// [`RollupCache::insert`], because the registry no longer holds the state it captured
    /// and the fresh state the next request registers is a different one.
    fn forget_table(&self, state: &Arc<TableState>) {
        let mut inner = state.inner.lock().unwrap();
        inner.ready = false;
        inner.last_used = None;
        inner.queries.clear();
        self.invalidate_table(state.table());
    }

    /// Invalidates `table` for `hull` and drops the entries whose dependency it reaches.
    ///
    /// `hull` is the half-open span the round found dirty; `None` means the change cannot
    /// be bounded, so every entry of the table goes. Entries of other tables, and of this
    /// table whose dependency the change does not reach, stay resident.
    ///
    /// Every query of the table commits the invalidation — even one that had no entry, and
    /// even when the cache is empty — because a fill the cache cannot see may be reading
    /// right now: the watermark has to move past it so that its payload is refused at
    /// [`RollupCache::insert`]. The removals and the watermark move under one table lock,
    /// so no fill can publish in between and there is nothing to retry.
    fn invalidate(
        &self,
        table: &SourceTable,
        inner: &mut TableStateInner,
        hull: Option<TimeChunk>,
        high: &BTreeMap<u64, u64>,
    ) {
        let reaches = |dependency: TimeChunk| hull.is_none_or(|span| overlaps(dependency, span));
        // The keys are collected first because the scan borrows the cache that the
        // removals mutate.
        let mut dirty = Vec::new();
        for (key, entry) in self.entries.iter() {
            if table.owns(key.identity()) && reaches(entry.dependency) {
                dirty.push(key);
            }
        }
        let mut removed = 0;
        for key in dirty {
            if self.entries.remove(key.as_ref()).is_some() {
                removed += 1;
            }
        }
        // Committed after the removals: every query of the table moves its watermark to the
        // sequence this round proved, so a fill that is still reading — and whose read did
        // not get that far — is refused when it tries to publish. A query with nothing
        // resident moves too, and so does one whose entries were all spared: the fill the
        // watermark has to refuse is one the cache cannot see. The version counts the change.
        // The watermark is assigned rather than merged, because the round's vector is
        // complete and a region that fell behind must not keep an older bound.
        for query in inner.queries.values_mut() {
            query.invalidated_watermark = high.clone();
            query.version = query.version.saturating_add(1);
        }
        debug!(
            "Rollup cache dropped {} entries of table {} for {} and advanced {} query watermarks",
            removed,
            table,
            match hull {
                Some(span) => format!("[{}, {})", span.start_ms(), span.end_ms()),
                None => "an unbounded change".to_string(),
            },
            inner.queries.len(),
        );
    }

    /// Drops every entry of `table`, regardless of span, and returns how many were
    /// removed.
    fn invalidate_table(&self, table: &SourceTable) -> usize {
        let mut dirty = Vec::new();
        for (key, _) in self.entries.iter() {
            if table.owns(key.identity()) {
                dirty.push(key);
            }
        }
        dirty
            .into_iter()
            .filter(|key| self.entries.remove(key.as_ref()).is_some())
            .count()
    }
}

impl Entry {
    /// The bytes one entry is charged, clamped to what a cache weight can hold.
    ///
    /// The annotations count: a chunk whose read recorded a long message costs more
    /// to keep than the same rows without it, so eviction stays within the budget
    /// the configuration names.
    fn weigh(payload: &RecordBatches, annotations: &CachedAnnotations) -> u32 {
        let data: usize = payload
            .iter()
            .map(|batch| batch.buffer_memory_size())
            .sum::<usize>();
        let bytes = u64::try_from(data)
            .unwrap_or(u64::MAX)
            .saturating_add(annotations.bytes())
            .saturating_add(ENTRY_OVERHEAD_BYTES);
        u32::try_from(bytes).unwrap_or(u32::MAX)
    }
}

#[cfg(test)]
mod tests {
    use std::time::UNIX_EPOCH;

    use promql_parser::parser::parse;

    use super::*;

    /// One minute, the step every clock-aligned test uses.
    const STEP_MS: i64 = 60_000;
    /// Five minutes: a whole number of steps, so canonical bounds are grid points.
    const CHUNK_MS: i64 = 300_000;
    /// The five-minute chunk every clock-aligned test plans with.
    const CHUNK: Duration = Duration::from_millis(CHUNK_MS as u64);
    /// A five-minute-aligned instant, standing in for 10:00.
    const HEAD_MS: i64 = 36_000_000;
    /// A wall clock far enough past every test window that no point is in its tail.
    const LATER_MS: i64 = HEAD_MS + 24 * 3_600_000;

    fn at(ms: i64) -> SystemTime {
        UNIX_EPOCH + Duration::from_millis(ms.unsigned_abs())
    }

    /// A statement from `start_ms` to `end_ms` (inclusive), stepping `step_ms`.
    fn stmt_between(expr: &str, start_ms: i64, end_ms: i64, step_ms: i64) -> EvalStmt {
        EvalStmt {
            expr: parse(expr).expect("the expression parses"),
            start: at(start_ms),
            end: at(end_ms),
            interval: Duration::from_millis(step_ms.unsigned_abs()),
            lookback_delta: Duration::from_secs(300),
        }
    }

    /// A statement of `points` points from `start_ms`.
    fn stmt_from(expr: &str, start_ms: i64, points: u64, step_ms: i64) -> EvalStmt {
        let end_ms = start_ms + (points as i64 - 1) * step_ms;
        stmt_between(expr, start_ms, end_ms, step_ms)
    }

    fn identity() -> QueryIdentity {
        QueryIdentity {
            tenant: "greptime".to_string(),
            catalog: "greptime".to_string(),
            schema: "public".to_string(),
            query: "rate(http_requests_total[5m])".to_string(),
            source_table: "http_requests_total".to_string(),
            step_ms: STEP_MS,
            lookback_ms: 300_000,
            route: QueryRoute::Legacy,
            alias: None,
            read_preference: None,
        }
    }

    /// An identity of the same table that differs only in its query text, so its
    /// entries are separate from [`identity`]'s.
    fn other_identity() -> QueryIdentity {
        QueryIdentity {
            query: "sum(rate(http_requests_total[5m]))".to_string(),
            ..identity()
        }
    }

    fn table() -> SourceTable {
        SourceTable::from(&identity())
    }

    fn other_table() -> SourceTable {
        SourceTable::new("greptime", "public", "other")
    }

    fn chunk_key() -> TimeChunkKey {
        chunk_key_at(0)
    }

    /// The key of chunk `index` after [`HEAD_MS`], so `-1` ends where the first starts.
    fn chunk_key_at(index: i64) -> TimeChunkKey {
        let start = HEAD_MS + index * CHUNK_MS;
        TimeChunkKey::new(identity(), TimeChunk::new(start, start + CHUNK_MS))
    }

    fn payload() -> Arc<RecordBatches> {
        Arc::new(RecordBatches::empty())
    }

    fn config() -> RollupCacheConfig {
        RollupCacheConfig::new(
            1 << 20,
            Duration::ZERO,
            CHUNK,
            Duration::from_secs(1),
            Duration::from_secs(3_600),
        )
        .expect("the test configuration is valid")
    }

    fn cache() -> RollupCache {
        RollupCache::new(config())
    }

    /// The metadata version every test round reports, so a round only empties a table when
    /// the test says the version moved.
    const SOURCE_VERSION: u64 = 1;

    /// The table id every test round reports, so a round only restarts a table when the test
    /// says the table was rebuilt under its own name.
    const SOURCE_TABLE_ID: u32 = 1024;

    /// Admits `identity`'s table and opens it, as a background round that proved nothing
    /// changed would.
    ///
    /// The admission comes after the round: a table's first round is its baseline and replaces
    /// the state the table is registered under, so a request admitted before it holds a state
    /// that is no longer the registered one and neither serves nor fills.
    fn opened(cache: &RollupCache) -> Admission {
        cache.admit(&table(), Instant::now());
        cache.record_refresh(
            &table(),
            BTreeMap::from([(1, 11)]),
            SOURCE_VERSION,
            SOURCE_TABLE_ID,
            DirtySpan::Nothing,
            Instant::now(),
        );
        cache.admit(&table(), Instant::now())
    }

    /// The source sequence a test read reports, well past every test round's watermark.
    fn read_through() -> BTreeMap<u64, u64> {
        BTreeMap::from([(1, 100)])
    }

    /// One fill of `key` under `admission`, taken and published at `now`.
    fn fill(
        cache: &RollupCache,
        admission: &Admission,
        key: &TimeChunkKey,
        now: SystemTime,
    ) -> Insert {
        let guard = cache.begin_fill(admission, key, key.chunk(), now);
        cache.insert(&guard, &read_through(), payload(), Arc::default())
    }

    fn planned(
        cache: &RollupCache,
        stmt: &EvalStmt,
        now: SystemTime,
    ) -> (
        QuerySpan,
        Vec<TimeChunk>,
        Vec<DirectSpan>,
        Option<TimeChunk>,
    ) {
        planned_for(identity(), cache, stmt, now)
    }

    #[allow(clippy::type_complexity)]
    fn planned_for(
        identity: QueryIdentity,
        cache: &RollupCache,
        stmt: &EvalStmt,
        now: SystemTime,
    ) -> (
        QuerySpan,
        Vec<TimeChunk>,
        Vec<DirectSpan>,
        Option<TimeChunk>,
    ) {
        match cache.plan(identity, stmt, now) {
            ChunkPlan::Planned(planned) => (
                planned.span(),
                planned.chunks().iter().map(|key| key.chunk()).collect(),
                planned.direct().to_vec(),
                planned.tail(),
            ),
            ChunkPlan::Bypass(reason) => panic!("the statement should plan: {reason}"),
        }
    }

    /// Every span a plan returns, in time order, must partition the coverage exactly
    /// once.
    fn assert_partition(span: QuerySpan, chunks: &[TimeChunk], direct: &[DirectSpan]) {
        let mut spans: Vec<TimeChunk> = chunks
            .iter()
            .copied()
            .chain(direct.iter().map(|span| span.span()))
            .collect();
        spans.sort_by_key(|span| span.start_ms());
        assert_eq!(
            spans.first().map(|span| span.start_ms()),
            Some(span.start_ms())
        );
        let mut previous_end = span.start_ms();
        for chunk in &spans {
            assert_eq!(
                chunk.start_ms(),
                previous_end,
                "gap or overlap at {chunk:?}"
            );
            assert!(chunk.start_ms() < chunk.end_ms(), "empty span {chunk:?}");
            previous_end = chunk.end_ms();
        }
        assert!(
            previous_end > span.end_ms(),
            "the last span must reach past the last point of {span:?}"
        );
    }

    #[test]
    fn a_sliding_window_reuses_every_interior_chunk() {
        let cache = cache();
        // Ten points one minute apart cover two whole five-minute chunks.
        let first = stmt_from("rate(http_requests_total[5m])", HEAD_MS, 10, STEP_MS);
        let (_, chunks, direct, _) = planned(&cache, &first, at(LATER_MS));
        assert_eq!(
            chunks,
            vec![
                TimeChunk::new(HEAD_MS, HEAD_MS + CHUNK_MS),
                TimeChunk::new(HEAD_MS + CHUNK_MS, HEAD_MS + 2 * CHUNK_MS),
            ]
        );
        assert!(direct.is_empty());

        // Sliding the window by five minutes keeps both chunks and moves the coverage.
        let second = stmt_from(
            "rate(http_requests_total[5m])",
            HEAD_MS + CHUNK_MS,
            10,
            STEP_MS,
        );
        let (_, chunks, _, _) = planned(&cache, &second, at(LATER_MS));
        assert_eq!(
            chunks,
            vec![
                TimeChunk::new(HEAD_MS + CHUNK_MS, HEAD_MS + 2 * CHUNK_MS),
                TimeChunk::new(HEAD_MS + 2 * CHUNK_MS, HEAD_MS + 3 * CHUNK_MS),
            ]
        );
    }

    #[test]
    fn the_tail_is_measured_from_now_not_from_the_query_end() {
        let cache = cache();
        let stmt = stmt_from("rate(http_requests_total[5m])", HEAD_MS, 10, STEP_MS);
        // A wall clock inside the statement's own coverage puts the last points in the
        // tail even though the query asks for them.
        let now_ms = HEAD_MS + 6 * STEP_MS;
        let (span, chunks, direct, tail) = planned(&cache, &stmt, at(now_ms));
        assert_partition(span, &chunks, &direct);
        assert_eq!(chunks.len(), 1);
        assert!(tail.is_some_and(|tail| tail.end_ms() > now_ms));
    }

    #[test]
    fn a_different_step_changes_the_identity() {
        let cache = cache();
        let stmt = stmt_from("rate(http_requests_total[5m])", HEAD_MS, 10, 2 * STEP_MS);
        assert_eq!(
            cache.plan(identity(), &stmt, at(LATER_MS)),
            ChunkPlan::Bypass(BypassReason::StepMismatch)
        );
    }

    #[test]
    fn a_chunk_that_is_not_a_whole_number_of_steps_bypasses() {
        // A chunk of 90 seconds is not a whole number of one-minute steps.
        let short = RollupCacheConfig::new(
            1 << 20,
            Duration::ZERO,
            Duration::from_secs(90),
            Duration::from_secs(1),
            Duration::from_secs(3_600),
        )
        .unwrap();
        let cache = RollupCache::new(short);
        let stmt = stmt_from("rate(http_requests_total[5m])", HEAD_MS, 10, STEP_MS);
        assert_eq!(
            cache.plan(identity(), &stmt, at(LATER_MS)),
            ChunkPlan::Bypass(BypassReason::MisalignedChunk)
        );
    }

    #[test]
    fn a_non_windowable_expression_bypasses_the_cache() {
        let cache = cache();
        let stmt = stmt_from("http_requests_total + 1", HEAD_MS, 10, STEP_MS);
        assert_eq!(
            cache.plan(identity(), &stmt, at(LATER_MS)),
            ChunkPlan::Bypass(BypassReason::NotWindowable)
        );
    }

    #[test]
    fn an_identity_prints_its_context_but_never_its_query_text() {
        let printed = format!("{:?}", identity());
        assert!(printed.contains("greptime"));
        assert!(printed.contains("public"));
        assert!(!printed.contains("http_requests_total"));
        assert!(printed.contains(REDACTED_QUERY));
    }

    #[test]
    fn a_missed_chunk_is_filled_and_then_hit() {
        let cache = cache();
        let admission = opened(&cache);
        let key = chunk_key();
        assert_eq!(
            cache.lookup(&admission, &key, at(LATER_MS)),
            Lookup::Miss(MissReason::NotFound)
        );
        assert_eq!(fill(&cache, &admission, &key, at(LATER_MS)), Insert::Stored);
        assert!(matches!(
            cache.lookup(&admission, &key, at(LATER_MS)),
            Lookup::Hit { .. }
        ));
        assert_eq!(cache.hit_count(), 1);
        assert_eq!(cache.entry_count(), 1);
    }

    #[test]
    fn an_unproved_table_never_serves() {
        let cache = cache();
        // The request fills the chunk, but no round has proved the table yet.
        let admission = cache.admit(&table(), Instant::now());
        let key = chunk_key();
        assert_eq!(fill(&cache, &admission, &key, at(LATER_MS)), Insert::Stored);
        assert_eq!(
            cache.lookup(&admission, &key, at(LATER_MS)),
            Lookup::Miss(MissReason::TableNotProved)
        );
        // The table's first round is its baseline and replaces the state the table is
        // registered under, so the read taken before it goes and the request that made it is not
        // admitted any more.
        cache.record_refresh(
            &table(),
            BTreeMap::from([(1, 11)]),
            SOURCE_VERSION,
            SOURCE_TABLE_ID,
            DirtySpan::Nothing,
            Instant::now(),
        );
        assert_eq!(cache.entry_count(), 0, "the baseline drops the cold read");
        assert_eq!(
            cache.lookup(&admission, &key, at(LATER_MS)),
            Lookup::Miss(MissReason::TableNotProved),
            "the state the request was admitted under is not the registered one"
        );
        // A request admitted after the baseline is served what it fills.
        let admission = cache.admit(&table(), Instant::now());
        assert_eq!(fill(&cache, &admission, &key, at(LATER_MS)), Insert::Stored);
        assert!(matches!(
            cache.lookup(&admission, &key, at(LATER_MS)),
            Lookup::Hit { .. }
        ));
    }

    #[test]
    fn a_failed_round_closes_the_table() {
        let cache = cache();
        let admission = opened(&cache);
        let key = chunk_key();
        fill(&cache, &admission, &key, at(LATER_MS));
        assert!(matches!(
            cache.lookup(&admission, &key, at(LATER_MS)),
            Lookup::Hit { .. }
        ));
        cache.record_failure(&table());
        assert_eq!(
            cache.lookup(&admission, &key, at(LATER_MS)),
            Lookup::Miss(MissReason::TableNotProved)
        );
        // The entry stays: nothing said it was wrong, and the next round decides.
        assert_eq!(cache.entry_count(), 1);
    }

    #[test]
    fn an_entry_older_than_the_age_bound_never_serves() {
        let cache = cache();
        let admission = opened(&cache);
        let key = chunk_key();
        fill(&cache, &admission, &key, at(HEAD_MS));
        let expiry = HEAD_MS + i64::try_from(config().max_age().as_millis()).unwrap() + 1;
        assert_eq!(
            cache.lookup(&admission, &key, at(expiry)),
            Lookup::Miss(MissReason::Expired {
                age: Duration::from_millis(
                    u64::try_from(expiry - HEAD_MS).expect("the age is positive")
                )
            })
        );
    }

    #[test]
    fn a_fill_a_round_overtook_publishes_nothing() {
        let cache = cache();
        let admission = opened(&cache);
        let key = chunk_key();
        let slow = cache.begin_fill(&admission, &key, key.chunk(), at(LATER_MS));
        cache.record_refresh(
            &table(),
            BTreeMap::from([(1, 12)]),
            SOURCE_VERSION,
            SOURCE_TABLE_ID,
            DirtySpan::Hull(TimeChunk::new(HEAD_MS, HEAD_MS + 1)),
            Instant::now(),
        );
        // The read reached sequence 5 while the round committed up to 12, so its payload
        // predates what the round invalidated and is not kept.
        assert_eq!(
            cache.insert(&slow, &BTreeMap::from([(1, 5)]), payload(), Arc::default()),
            Insert::Rejected
        );
        assert_eq!(cache.entry_count(), 0);
        assert_eq!(
            cache.lookup(&admission, &key, at(LATER_MS)),
            Lookup::Miss(MissReason::NotFound)
        );

        // The fill that starts after the commit reads past the new watermark and publishes.
        assert_eq!(fill(&cache, &admission, &key, at(LATER_MS)), Insert::Stored);
        assert!(matches!(
            cache.lookup(&admission, &key, at(LATER_MS)),
            Lookup::Hit { .. }
        ));
    }

    #[test]
    fn a_second_fill_of_the_same_query_publishes_after_the_first() {
        let cache = cache();
        let admission = opened(&cache);
        let first = chunk_key_at(0);
        let second = chunk_key_at(1);
        // Fills do not move the watermark, so both chunks of one request publish.
        assert_eq!(
            fill(&cache, &admission, &first, at(LATER_MS)),
            Insert::Stored
        );
        assert_eq!(
            fill(&cache, &admission, &second, at(LATER_MS)),
            Insert::Stored
        );
        assert_eq!(cache.entry_count(), 2);
    }

    #[test]
    fn a_round_moves_the_watermark_even_with_an_empty_cache() {
        let cache = cache();
        let admission = opened(&cache);
        let key = chunk_key();
        // A fill of an empty cache is reading when the round commits.
        let slow = cache.begin_fill(&admission, &key, key.chunk(), at(LATER_MS));
        assert_eq!(cache.entry_count(), 0);
        cache.record_refresh(
            &table(),
            BTreeMap::from([(1, 12)]),
            SOURCE_VERSION,
            SOURCE_TABLE_ID,
            DirtySpan::Everything,
            Instant::now(),
        );
        // Nothing was dropped, but the watermark moved past this read, so it is refused.
        assert_eq!(cache.entry_count(), 0);
        assert_eq!(
            cache.insert(&slow, &BTreeMap::from([(1, 5)]), payload(), Arc::default()),
            Insert::Rejected
        );
        // A read that opened at the committed sequence saw everything the invalidation
        // described, so it publishes; a read of the same sequence as the resident payload
        // adds nothing, and one that got further replaces it.
        for (sequence, expected) in [
            (12, Insert::Stored),
            (12, Insert::Rejected),
            (13, Insert::Stored),
        ] {
            let next = cache.begin_fill(&admission, &key, key.chunk(), at(LATER_MS));
            assert_eq!(
                cache.insert(
                    &next,
                    &BTreeMap::from([(1, sequence)]),
                    payload(),
                    Arc::default()
                ),
                expected
            );
        }
    }

    #[test]
    fn a_read_that_is_not_newer_does_not_replace_a_payload() {
        let cache = cache();
        let admission = opened(&cache);
        let key = chunk_key();
        let newer = cache.begin_fill(&admission, &key, key.chunk(), at(LATER_MS));
        assert_eq!(
            cache.insert(
                &newer,
                &BTreeMap::from([(1, 20)]),
                payload(),
                Arc::default()
            ),
            Insert::Stored
        );
        // A read that finished later but got less far must not replace it.
        let older = cache.begin_fill(&admission, &key, key.chunk(), at(LATER_MS));
        assert_eq!(
            cache.insert(
                &older,
                &BTreeMap::from([(1, 10)]),
                payload(),
                Arc::default()
            ),
            Insert::Rejected
        );
        // A read of the same sequence says nothing new, so it does not replace the payload.
        let same = cache.begin_fill(&admission, &key, key.chunk(), at(LATER_MS));
        assert_eq!(
            cache.insert(&same, &BTreeMap::from([(1, 20)]), payload(), Arc::default()),
            Insert::Rejected
        );
        // One that got further does.
        let further = cache.begin_fill(&admission, &key, key.chunk(), at(LATER_MS));
        assert_eq!(
            cache.insert(
                &further,
                &BTreeMap::from([(1, 21)]),
                payload(),
                Arc::default()
            ),
            Insert::Stored
        );
    }

    #[test]
    fn an_expired_entry_is_refreshed_by_a_read_of_the_same_sequence() {
        let cache = cache();
        let admission = opened(&cache);
        let key = chunk_key();
        // The resident payload was read long before this request captured its instant, so it
        // is stale by age alone.
        let old = cache.begin_fill(&admission, &key, key.chunk(), at(HEAD_MS));
        assert_eq!(
            cache.insert(&old, &BTreeMap::from([(1, 20)]), payload(), Arc::default()),
            Insert::Stored
        );
        let refreshing = cache.begin_fill(&admission, &key, key.chunk(), at(LATER_MS));
        assert_eq!(
            cache.insert(
                &refreshing,
                &BTreeMap::from([(1, 20)]),
                payload(),
                Arc::default()
            ),
            Insert::Stored
        );
    }

    #[test]
    fn a_read_that_misses_a_region_the_resident_one_covered_does_not_replace_it() {
        let cache = cache();
        opened(&cache);
        // A second region joins the table's domain, so a read of both regions is what the
        // table's watermark covers.
        assert!(cache.record_refresh(
            &table(),
            BTreeMap::from([(1, 11), (2, 11)]),
            SOURCE_VERSION,
            SOURCE_TABLE_ID,
            DirtySpan::Nothing,
            Instant::now(),
        ));
        // A route that gained a region restarts the table, so the request that reads it is one
        // admitted after the round.
        let admission = cache.admit(&table(), Instant::now());
        let key = chunk_key();
        let both = cache.begin_fill(&admission, &key, key.chunk(), at(LATER_MS));
        assert_eq!(
            cache.insert(
                &both,
                &BTreeMap::from([(1, 20), (2, 20)]),
                payload(),
                Arc::default()
            ),
            Insert::Stored
        );
        // Region 2 is a region this read did not cover, so it cannot be shown to be at
        // least as current as the payload it would replace.
        let one = cache.begin_fill(&admission, &key, key.chunk(), at(LATER_MS));
        assert_eq!(
            cache.insert(&one, &BTreeMap::from([(1, 20)]), payload(), Arc::default()),
            Insert::Rejected
        );
    }

    #[test]
    fn a_read_without_a_source_sequence_is_never_published() {
        let cache = cache();
        let admission = opened(&cache);
        let key = chunk_key();
        // The read could not say how far it got, so nothing can order it against an
        // invalidation and it is not published at all.
        let guard = cache.begin_fill(&admission, &key, key.chunk(), at(LATER_MS));
        assert_eq!(
            cache.insert(&guard, &BTreeMap::new(), payload(), Arc::default()),
            Insert::Rejected
        );
        assert_eq!(cache.entry_count(), 0);
    }

    #[test]
    fn a_read_of_another_region_domain_is_not_published() {
        let cache = cache();
        let admission = opened(&cache);
        let key = chunk_key();
        // The table committed region 1 alone, so a read of another set of regions cannot be
        // ordered against the invalidation: neither a superset, which may have read regions
        // the table never committed, nor a subset, which proves less than the invalidation
        // covers.
        for read in [
            BTreeMap::from([(1, 100), (2, 100)]),
            BTreeMap::from([(2, 100)]),
        ] {
            let guard = cache.begin_fill(&admission, &key, key.chunk(), at(LATER_MS));
            assert_eq!(
                cache.insert(&guard, &read, payload(), Arc::default()),
                Insert::Rejected,
                "{read:?} is not the committed domain"
            );
        }
        assert_eq!(cache.entry_count(), 0);
        // A read of exactly the committed domain publishes.
        assert_eq!(fill(&cache, &admission, &key, at(LATER_MS)), Insert::Stored);
    }

    #[test]
    fn a_new_identity_cannot_publish_an_unproved_sequence() {
        let cache = cache();
        let admission = opened(&cache);
        let key = TimeChunkKey::new(other_identity(), chunk_key().chunk());
        // The identity is new, but the table it reads is not cold: its watermark starts where
        // the table committed, so neither another domain nor a read behind that sequence is
        // published.
        for (read, expected) in [
            (BTreeMap::from([(2, 100)]), Insert::Rejected),
            (BTreeMap::from([(1, 5)]), Insert::Rejected),
            (BTreeMap::from([(1, 11)]), Insert::Stored),
        ] {
            let guard = cache.begin_fill(&admission, &key, key.chunk(), at(LATER_MS));
            assert_eq!(
                cache.insert(&guard, &read, payload(), Arc::default()),
                expected,
                "{read:?}"
            );
        }
        // A table no round has established a domain for is the one place an unproved read is
        // published — there is nothing for it to be unproved against — which is what
        // [`an_unproved_table_never_serves`] states from the other side.
    }

    #[test]
    fn a_cold_table_keeps_the_unproved_reads_of_a_new_identity() {
        let cache = cache();
        let admission = cache.admit(&table(), Instant::now());
        let key = TimeChunkKey::new(other_identity(), chunk_key().chunk());
        // No round has established a domain yet, so the query starts with an empty watermark
        // and its read is the only bound there is.
        let guard = cache.begin_fill(&admission, &key, key.chunk(), at(LATER_MS));
        assert_eq!(
            cache.insert(&guard, &BTreeMap::from([(9, 1)]), payload(), Arc::default()),
            Insert::Stored
        );
    }

    #[test]
    fn a_hit_reports_the_annotations_of_the_read_it_serves() {
        let cache = cache();
        let admission = opened(&cache);
        let key = chunk_key();
        let annotations = Arc::new(CachedAnnotations::new(
            vec!["a warning".to_string()],
            vec!["an info".to_string()],
        ));
        let guard = cache.begin_fill(&admission, &key, key.chunk(), at(LATER_MS));
        assert_eq!(
            cache.insert(&guard, &read_through(), payload(), annotations.clone()),
            Insert::Stored
        );
        let Lookup::Hit {
            annotations: served,
            ..
        } = cache.lookup(&admission, &key, at(LATER_MS))
        else {
            panic!("the chunk should be resident");
        };
        assert_eq!(served, annotations);
    }

    #[test]
    fn the_byte_budget_bounds_the_entries() {
        // One entry costs the fixed overhead, so a budget of two of them holds two.
        let tiny = RollupCacheConfig::new(
            ENTRY_OVERHEAD_BYTES * 2,
            Duration::ZERO,
            CHUNK,
            Duration::from_secs(1),
            Duration::from_secs(3_600),
        )
        .unwrap();
        let cache = RollupCache::new(tiny);
        let admission = opened(&cache);
        for index in 0..6 {
            let key = chunk_key_at(index);
            fill(&cache, &admission, &key, at(LATER_MS));
        }
        assert_eq!(cache.entry_count(), 2);
    }

    #[test]
    fn an_empty_chunk_is_bypassed() {
        let cache = cache();
        let admission = opened(&cache);
        let key = TimeChunkKey::new(identity(), TimeChunk::new(HEAD_MS, HEAD_MS));
        assert_eq!(
            cache.lookup(&admission, &key, at(LATER_MS)),
            Lookup::Bypass(BypassReason::EmptyChunk)
        );
        let guard = cache.begin_fill(&admission, &key, key.chunk(), at(LATER_MS));
        assert_eq!(
            cache.insert(&guard, &read_through(), payload(), Arc::default()),
            Insert::Bypassed
        );
    }

    #[test]
    fn a_dirty_round_drops_the_chunks_whose_input_it_overlaps() {
        let cache = cache();
        let admission = opened(&cache);
        let first = chunk_key_at(0);
        let second = chunk_key_at(1);
        fill(&cache, &admission, &first, at(LATER_MS));
        fill(&cache, &admission, &second, at(LATER_MS));
        assert_eq!(cache.entry_count(), 2);

        // A row written one minute into the first chunk dirties only its dependency.
        cache.record_refresh(
            &table(),
            BTreeMap::from([(1, 12)]),
            SOURCE_VERSION,
            SOURCE_TABLE_ID,
            DirtySpan::Hull(TimeChunk::new(HEAD_MS + STEP_MS, HEAD_MS + STEP_MS + 1)),
            Instant::now(),
        );
        assert_eq!(cache.entry_count(), 1);
        assert_eq!(
            cache.lookup(&admission, &first, at(LATER_MS)),
            Lookup::Miss(MissReason::NotFound)
        );
        assert!(matches!(
            cache.lookup(&admission, &second, at(LATER_MS)),
            Lookup::Hit { .. }
        ));
    }

    #[test]
    fn a_table_round_spares_another_tables_entries() {
        let cache = cache();
        let admission = opened(&cache);
        let key = chunk_key();
        fill(&cache, &admission, &key, at(LATER_MS));

        // A whole-table change of another table reaches nothing here, even though that
        // table is registered and its round really runs.
        let _other = cache.admit(&other_table(), Instant::now());
        cache.record_refresh(
            &other_table(),
            BTreeMap::from([(1, 3)]),
            SOURCE_VERSION,
            SOURCE_TABLE_ID,
            DirtySpan::Everything,
            Instant::now(),
        );
        assert_eq!(cache.entry_count(), 1);
        assert!(matches!(
            cache.lookup(&admission, &key, at(LATER_MS)),
            Lookup::Hit { .. }
        ));
    }

    #[test]
    fn a_whole_table_change_drops_every_span_of_that_table() {
        let cache = cache();
        let admission = opened(&cache);
        fill(&cache, &admission, &chunk_key_at(0), at(LATER_MS));
        fill(&cache, &admission, &chunk_key_at(1), at(LATER_MS));
        cache.record_refresh(
            &table(),
            BTreeMap::from([(1, 12)]),
            SOURCE_VERSION,
            SOURCE_TABLE_ID,
            DirtySpan::Everything,
            Instant::now(),
        );
        assert_eq!(cache.entry_count(), 0);
    }

    #[test]
    fn a_quiet_round_keeps_every_entry_and_advances_nothing() {
        let cache = cache();
        let admission = opened(&cache);
        let key = chunk_key();
        fill(&cache, &admission, &key, at(LATER_MS));
        cache.record_refresh(
            &table(),
            BTreeMap::from([(1, 11)]),
            SOURCE_VERSION,
            SOURCE_TABLE_ID,
            DirtySpan::Nothing,
            Instant::now(),
        );
        assert_eq!(cache.entry_count(), 1);
        assert!(matches!(
            cache.lookup(&admission, &key, at(LATER_MS)),
            Lookup::Hit { .. }
        ));
        // A fill that started before the quiet round still publishes: nothing was
        // invalidated.
        let other = chunk_key_at(1);
        let slow = cache.begin_fill(&admission, &other, other.chunk(), at(LATER_MS));
        cache.record_refresh(
            &table(),
            BTreeMap::from([(1, 11)]),
            SOURCE_VERSION,
            SOURCE_TABLE_ID,
            DirtySpan::Nothing,
            Instant::now(),
        );
        assert_eq!(
            cache.insert(&slow, &read_through(), payload(), Arc::default()),
            Insert::Stored
        );
    }

    #[test]
    fn a_quiet_round_that_cannot_extend_the_cursors_proves_nothing() {
        let cache = cache();
        let admission = opened(&cache);
        let key = chunk_key();
        fill(&cache, &admission, &key, at(LATER_MS));

        // A quiet round is compared with what is committed like any other: a region that went
        // back is a vector of some other history, so the round proves nothing.
        assert!(!cache.record_refresh(
            &table(),
            BTreeMap::from([(1, 10)]),
            SOURCE_VERSION,
            SOURCE_TABLE_ID,
            DirtySpan::Nothing,
            Instant::now(),
        ));
        assert_eq!(
            cache.cursors(&table()),
            BTreeMap::from([(1, 11)]),
            "the cursor stays where the last proved round left it"
        );
        assert_eq!(
            cache.lookup(&admission, &key, at(LATER_MS)),
            Lookup::Miss(MissReason::TableNotProved),
            "a round that proves nothing closes the table"
        );
        // Nor does a round of no regions at all: an empty vector is not a domain.
        assert!(!cache.record_refresh(
            &table(),
            BTreeMap::new(),
            SOURCE_VERSION,
            SOURCE_TABLE_ID,
            DirtySpan::Nothing,
            Instant::now(),
        ));
        assert_eq!(cache.cursors(&table()), BTreeMap::from([(1, 11)]));
        assert_eq!(cache.entry_count(), 1, "nothing was dropped either");
    }

    #[test]
    fn a_round_of_another_region_domain_restarts_the_table() {
        let cache = cache();
        let admission = opened(&cache);
        let key = chunk_key();
        fill(&cache, &admission, &key, at(LATER_MS));
        // A fill that is still reading when the route moves.
        let slow = cache.begin_fill(&admission, &key, key.chunk(), at(LATER_MS));

        // The route gained a region. The round's vector and everything committed under the old
        // one are of different domains, so the table starts again instead of committing a mix
        // of the two.
        assert!(cache.record_refresh(
            &table(),
            BTreeMap::from([(1, 11), (2, 4)]),
            SOURCE_VERSION,
            SOURCE_TABLE_ID,
            DirtySpan::Nothing,
            Instant::now(),
        ));
        assert_eq!(cache.entry_count(), 0, "the entries of the old route go");
        assert_eq!(cache.cursors(&table()), BTreeMap::from([(1, 11), (2, 4)]));
        assert_eq!(
            cache.lookup(&admission, &key, at(LATER_MS)),
            Lookup::Miss(MissReason::TableNotProved),
            "the state the request was admitted under is not the registered one any more"
        );
        assert_eq!(
            cache.insert(&slow, &read_through(), payload(), Arc::default()),
            Insert::Rejected,
            "a fill of the old domain is not published into the new one"
        );

        // A read of the new domain publishes — from a request admitted after the restart, because
        // the state the earlier ones captured is not the registered one any more — and the route
        // losing a region drops it again.
        let admission = cache.admit(&table(), Instant::now());
        let guard = cache.begin_fill(&admission, &key, key.chunk(), at(LATER_MS));
        assert_eq!(
            cache.insert(
                &guard,
                &BTreeMap::from([(1, 100), (2, 100)]),
                payload(),
                Arc::default()
            ),
            Insert::Stored
        );
        assert!(cache.record_refresh(
            &table(),
            BTreeMap::from([(1, 12)]),
            SOURCE_VERSION,
            SOURCE_TABLE_ID,
            DirtySpan::Nothing,
            Instant::now(),
        ));
        assert_eq!(cache.entry_count(), 0, "a shrunken route drops them too");
        assert_eq!(cache.cursors(&table()), BTreeMap::from([(1, 12)]));
    }

    #[test]
    fn a_round_of_another_table_version_restarts_the_table() {
        let cache = cache();
        let admission = opened(&cache);
        let key = chunk_key();
        fill(&cache, &admission, &key, at(LATER_MS));
        let slow = cache.begin_fill(&admission, &key, key.chunk(), at(LATER_MS));

        // The schema moved while no sequence did. A column dropped and added back with a default
        // answers differently for rows that are already stored, so the same quiet vector is no
        // longer a proof.
        assert!(cache.record_refresh(
            &table(),
            BTreeMap::from([(1, 11)]),
            SOURCE_VERSION + 1,
            SOURCE_TABLE_ID,
            DirtySpan::Nothing,
            Instant::now(),
        ));
        assert_eq!(cache.entry_count(), 0, "a version change drops every entry");
        assert_eq!(
            cache.lookup(&admission, &key, at(LATER_MS)),
            Lookup::Miss(MissReason::TableNotProved),
            "the state the request was admitted under is not the registered one any more"
        );
        assert_eq!(
            cache.insert(&slow, &read_through(), payload(), Arc::default()),
            Insert::Rejected,
            "a fill that read the old schema is never published"
        );
        assert_eq!(
            cache.cursors(&table()),
            BTreeMap::from([(1, 11)]),
            "the round of the new version is the new baseline"
        );

        // The next round of that version is quiet, and the one after it keeps the entry. The
        // request that fills it is admitted after the restart, as every request of a restarted
        // table is.
        assert!(cache.record_refresh(
            &table(),
            BTreeMap::from([(1, 11)]),
            SOURCE_VERSION + 1,
            SOURCE_TABLE_ID,
            DirtySpan::Nothing,
            Instant::now(),
        ));
        let admission = cache.admit(&table(), Instant::now());
        assert_eq!(fill(&cache, &admission, &key, at(LATER_MS)), Insert::Stored);
        assert!(cache.record_refresh(
            &table(),
            BTreeMap::from([(1, 11)]),
            SOURCE_VERSION + 1,
            SOURCE_TABLE_ID,
            DirtySpan::Nothing,
            Instant::now(),
        ));
        assert_eq!(cache.entry_count(), 1, "the same version keeps it");
    }

    #[test]
    fn a_round_of_another_table_id_restarts_the_table() {
        let cache = cache();
        let admission = opened(&cache);
        let key = chunk_key();
        fill(&cache, &admission, &key, at(LATER_MS));

        // The table was rebuilt under its own name: its logical id moved while its metadata
        // version and its physical regions stayed the same, so nothing says the entry was read
        // from the table that carries the name now.
        assert!(cache.record_refresh(
            &table(),
            BTreeMap::from([(1, 11)]),
            SOURCE_VERSION,
            SOURCE_TABLE_ID + 1,
            DirtySpan::Nothing,
            Instant::now(),
        ));
        assert_eq!(cache.entry_count(), 0, "a new table id drops every entry");
        assert_eq!(
            cache.lookup(&admission, &key, at(LATER_MS)),
            Lookup::Miss(MissReason::TableNotProved),
            "the state the request was admitted under is not the registered one any more"
        );

        // The next round of that id is the baseline, and the one after it keeps what a fill of it
        // publishes.
        let admission = cache.admit(&table(), Instant::now());
        assert_eq!(fill(&cache, &admission, &key, at(LATER_MS)), Insert::Stored);
        assert!(cache.record_refresh(
            &table(),
            BTreeMap::from([(1, 11)]),
            SOURCE_VERSION,
            SOURCE_TABLE_ID + 1,
            DirtySpan::Nothing,
            Instant::now(),
        ));
        assert_eq!(cache.entry_count(), 1, "the same table id keeps it");
    }

    #[test]
    fn a_fill_of_a_restarted_table_cannot_publish_after_another_fill_registers() {
        let cache = cache();
        let admission = opened(&cache);
        let key = chunk_key();
        // A fill that started before the table restarts.
        let stale = cache.begin_fill(&admission, &key, key.chunk(), at(LATER_MS));

        // The schema moved, so the table restarts.
        assert!(cache.record_refresh(
            &table(),
            BTreeMap::from([(1, 11)]),
            SOURCE_VERSION + 1,
            SOURCE_TABLE_ID,
            DirtySpan::Nothing,
            Instant::now(),
        ));

        // The same identity registers a fill of its own on the replacement, and the earlier one
        // comes back first. The query it would find there is not its own: the state it captured
        // is the one that is no longer registered, so it is refused however far its read got.
        let admission = cache.admit(&table(), Instant::now());
        let fresh = cache.begin_fill(&admission, &key, key.chunk(), at(LATER_MS));
        assert_eq!(
            cache.insert(&stale, &read_through(), payload(), Arc::default()),
            Insert::Rejected,
            "a read of the old schema is refused even when its identity registered again"
        );
        assert_eq!(cache.entry_count(), 0);

        // The fill that registered on the replacement publishes.
        assert_eq!(
            cache.insert(&fresh, &read_through(), payload(), Arc::default()),
            Insert::Stored
        );
        assert_eq!(cache.entry_count(), 1);
    }

    #[test]
    fn a_first_round_drops_the_reads_a_cold_table_kept() {
        let cache = cache();
        // A request reads the table before any round has proved it, so its payload is published:
        // there is nothing for the read to be unproved against yet.
        let admission = cache.admit(&table(), Instant::now());
        let key = chunk_key();
        let cold = cache.begin_fill(&admission, &key, key.chunk(), at(LATER_MS));
        assert_eq!(
            cache.insert(&cold, &BTreeMap::from([(9, 1)]), payload(), Arc::default()),
            Insert::Stored
        );
        // A fill that is still reading when the baseline lands.
        let stopped = cache.begin_fill(&admission, &key, key.chunk(), at(LATER_MS));

        // The table's first round establishes its baseline, which replaces the state the table is
        // registered under: nothing that was read under no domain survives it, and no fill that
        // was registered under one can publish into the replacement.
        assert!(cache.record_refresh(
            &table(),
            BTreeMap::from([(1, 11)]),
            SOURCE_VERSION,
            SOURCE_TABLE_ID,
            DirtySpan::Nothing,
            Instant::now(),
        ));
        assert_eq!(
            cache.entry_count(),
            0,
            "no cold entry survives the baseline"
        );
        assert_eq!(
            cache.insert(&stopped, &read_through(), payload(), Arc::default()),
            Insert::Rejected,
            "a fill of the cold state is not published into the replacement, however far it got"
        );
        assert_eq!(cache.entry_count(), 0);
        assert_eq!(
            cache.lookup(&admission, &key, at(LATER_MS)),
            Lookup::Miss(MissReason::TableNotProved),
            "the baseline opens the table, and the state the cold read holds is not it"
        );

        // A request admitted after the baseline starts its query at the committed cursors, so a
        // read of another set of regions, or one behind the baseline, is refused.
        let admission = cache.admit(&table(), Instant::now());
        for read in [
            BTreeMap::from([(9, 1)]),
            BTreeMap::from([(1, 5)]),
            BTreeMap::from([(1, 100), (2, 100)]),
        ] {
            let guard = cache.begin_fill(&admission, &key, key.chunk(), at(LATER_MS));
            assert_eq!(
                cache.insert(&guard, &read, payload(), Arc::default()),
                Insert::Rejected,
                "{read:?} is not the baseline domain"
            );
        }
        // A read of the baseline domain publishes.
        assert_eq!(fill(&cache, &admission, &key, at(LATER_MS)), Insert::Stored);

        // An identity that registers after the baseline inherits the table's cursors instead of
        // starting cold, so it cannot publish a read of another set of regions either.
        let later = TimeChunkKey::new(other_identity(), key.chunk());
        let subset = cache.begin_fill(&admission, &later, later.chunk(), at(LATER_MS));
        assert_eq!(
            cache.insert(
                &subset,
                &BTreeMap::from([(9, 1)]),
                payload(),
                Arc::default()
            ),
            Insert::Rejected
        );
        assert_eq!(
            fill(&cache, &admission, &later, at(LATER_MS)),
            Insert::Stored
        );
    }

    #[test]
    fn a_reclaimed_state_rejects_a_fill() {
        let cache = cache();
        let admission = cache.admit(&table(), Instant::now());
        let key = chunk_key();
        let fill = cache.begin_fill(&admission, &key, key.chunk(), at(LATER_MS));

        // An idle table is forgotten and the next request registers a fresh state, which
        // the request that is still filling never captured.
        cache.tables.lock().unwrap().remove(&table());
        cache.admit(&table(), Instant::now());
        assert_eq!(
            cache.insert(&fill, &read_through(), payload(), Arc::default()),
            Insert::Rejected
        );
        assert_eq!(cache.entry_count(), 0);
    }

    #[test]
    fn a_query_state_nothing_keeps_is_reclaimed_by_a_round() {
        let cache = cache();
        let admission = opened(&cache);
        let resident = chunk_key();
        // One identity keeps a payload, one has a fill that has not come back yet, and many read
        // the table once and published nothing.
        assert_eq!(
            fill(&cache, &admission, &resident, at(LATER_MS)),
            Insert::Stored
        );
        let in_flight_key = TimeChunkKey::new(other_identity(), resident.chunk());
        let in_flight = cache.begin_fill(
            &admission,
            &in_flight_key,
            in_flight_key.chunk(),
            at(LATER_MS),
        );
        for index in 0..8 {
            let identity = QueryIdentity {
                query: format!("query_{index}"),
                ..identity()
            };
            let key = TimeChunkKey::new(identity, chunk_key().chunk());
            cache.begin_fill(&admission, &key, key.chunk(), at(LATER_MS));
        }
        assert_eq!(cache.query_count(&table()), 10);

        // A quiet round long after those fills reclaims the states nothing keeps.
        let idle = Instant::now() + IDLE_TABLE_TIMEOUT + Duration::from_secs(1);
        assert!(cache.record_refresh(
            &table(),
            BTreeMap::from([(1, 11)]),
            SOURCE_VERSION,
            SOURCE_TABLE_ID,
            DirtySpan::Nothing,
            idle,
        ));
        assert_eq!(
            cache.query_count(&table()),
            1,
            "only the identity with a resident payload is kept"
        );
        // The fill that was still reading when its state was reclaimed finds no query and is
        // refused: the request still gets its own rows, the cache keeps nothing.
        assert_eq!(
            cache.insert(&in_flight, &read_through(), payload(), Arc::default()),
            Insert::Rejected
        );
        // The identity that is still resident is served, and a reclaim of its query would have
        // made the next publication of it fail.
        assert!(matches!(
            cache.lookup(&admission, &resident, at(LATER_MS)),
            Lookup::Hit { .. }
        ));
        // A reclaimed identity is not lost: its next fill registers a state again and publishes.
        let guard = cache.begin_fill(
            &admission,
            &in_flight_key,
            in_flight_key.chunk(),
            at(LATER_MS),
        );
        assert_eq!(
            cache.insert(&guard, &read_through(), payload(), Arc::default()),
            Insert::Stored
        );
    }

    #[test]
    fn a_fill_of_a_reclaimed_query_cannot_publish_after_another_registered() {
        let cache = cache();
        let admission = opened(&cache);
        let key = chunk_key();
        // A fill registers its query and goes reading, publishing nothing.
        let stale = cache.begin_fill(&admission, &key, key.chunk(), at(LATER_MS));

        // A round long after it finds nothing resident, so the query state is reclaimed.
        let idle = Instant::now() + IDLE_TABLE_TIMEOUT + Duration::from_secs(1);
        assert!(cache.record_refresh(
            &table(),
            BTreeMap::from([(1, 11)]),
            SOURCE_VERSION,
            SOURCE_TABLE_ID,
            DirtySpan::Nothing,
            idle,
        ));
        assert_eq!(
            cache.query_count(&table()),
            0,
            "the idle query is reclaimed"
        );

        // The same identity registers again on the same table state, so the fill that comes back
        // first finds a query of its identity — but not its own registration.
        let fresh = cache.begin_fill(&admission, &key, key.chunk(), at(LATER_MS));
        assert_eq!(
            cache.insert(&stale, &read_through(), payload(), Arc::default()),
            Insert::Rejected,
            "a fill whose query was reclaimed never publishes into its replacement"
        );
        assert_eq!(cache.entry_count(), 0);

        // The registration that is still its own publishes.
        assert_eq!(
            cache.insert(&fresh, &read_through(), payload(), Arc::default()),
            Insert::Stored
        );
        assert_eq!(cache.entry_count(), 1);
    }

    #[test]
    fn a_reclaim_scan_defers_an_idle_query_that_still_has_a_payload() {
        let cache = cache();
        let admission = opened(&cache);
        let key = chunk_key();
        assert_eq!(fill(&cache, &admission, &key, at(LATER_MS)), Insert::Stored);

        // A round long after the fill finds the query idle, but its payload is resident, so it
        // keeps the state and moves its next check a whole timeout away.
        let idle = Instant::now() + IDLE_TABLE_TIMEOUT + Duration::from_secs(1);
        let round = |cursors: BTreeMap<u64, u64>, dirty: DirtySpan, now: Instant| {
            assert!(cache.record_refresh(
                &table(),
                cursors,
                SOURCE_VERSION,
                SOURCE_TABLE_ID,
                dirty,
                now,
            ));
        };
        round(BTreeMap::from([(1, 11)]), DirtySpan::Nothing, idle);
        assert_eq!(cache.query_count(&table()), 1);

        // The payload goes, but the state is not idle again yet: a round keeps it without
        // scanning the cache for it, which is the whole point of moving the check forward.
        round(
            BTreeMap::from([(1, 12)]),
            DirtySpan::Everything,
            idle + Duration::from_secs(1),
        );
        assert_eq!(cache.entry_count(), 0);
        assert_eq!(
            cache.query_count(&table()),
            1,
            "the next check is a timeout away, not the next round"
        );

        // Once that timeout passes, a round that finds nothing resident drops it.
        round(
            BTreeMap::from([(1, 13)]),
            DirtySpan::Nothing,
            idle + IDLE_TABLE_TIMEOUT + Duration::from_secs(2),
        );
        assert_eq!(cache.query_count(&table()), 0);
    }

    #[test]
    fn an_idle_table_stops_being_refreshed_and_loses_its_entries() {
        let cache = cache();
        let admission = opened(&cache);
        let key = chunk_key();
        fill(&cache, &admission, &key, at(LATER_MS));
        assert_eq!(cache.tables_to_refresh(Instant::now()).len(), 1);

        let later = Instant::now() + IDLE_TABLE_TIMEOUT + Duration::from_secs(1);
        assert!(cache.tables_to_refresh(later).is_empty());
        assert_eq!(cache.entry_count(), 0);
    }

    #[test]
    fn configuration_validation_rejects_disabled_bounds() {
        let positive = Duration::from_secs(1);
        assert_eq!(
            RollupCacheConfig::new(0, Duration::ZERO, CHUNK, positive, positive),
            Err(ConfigError::Bytes)
        );
        assert_eq!(
            RollupCacheConfig::new(1, Duration::ZERO, Duration::ZERO, positive, positive),
            Err(ConfigError::Chunk)
        );
        assert_eq!(
            RollupCacheConfig::new(1, Duration::ZERO, CHUNK, Duration::ZERO, positive),
            Err(ConfigError::RefreshInterval)
        );
        assert_eq!(
            RollupCacheConfig::new(1, Duration::ZERO, CHUNK, positive, Duration::ZERO),
            Err(ConfigError::MaxAge)
        );
        let valid = RollupCacheConfig::new(1, Duration::ZERO, CHUNK, positive, positive)
            .expect("a positive configuration is valid");
        assert_eq!(valid.refresh_interval(), positive);
        assert_eq!(valid.max_age(), positive);
        assert_eq!(valid.max_bytes(), 1);
        assert_eq!(valid.chunk(), CHUNK);
        assert_eq!(valid.tail(), Duration::ZERO);
    }

    #[test]
    fn a_table_registered_with_a_different_catalog_is_a_different_table() {
        let cache = cache();
        let admission = opened(&cache);
        let key = chunk_key();
        fill(&cache, &admission, &key, at(LATER_MS));

        // The same bare name in another schema owns another state and another set of
        // entries, so a change of it reaches nothing here.
        let elsewhere = SourceTable::new("greptime", "metrics", "http_requests_total");
        let _elsewhere = cache.admit(&elsewhere, Instant::now());
        cache.record_refresh(
            &elsewhere,
            BTreeMap::from([(1, 2)]),
            SOURCE_VERSION,
            SOURCE_TABLE_ID,
            DirtySpan::Everything,
            Instant::now(),
        );
        assert_eq!(cache.entry_count(), 1);
        assert_eq!(cache.cursors(&elsewhere), BTreeMap::from([(1, 2)]));
        assert_eq!(cache.cursors(&table()), BTreeMap::from([(1, 11)]));
        assert!(matches!(
            cache.lookup(&admission, &key, at(LATER_MS)),
            Lookup::Hit { .. }
        ));
    }

    #[test]
    fn an_identity_of_the_same_table_keeps_its_entries_when_another_is_invalidated() {
        let cache = cache();
        let admission = opened(&cache);
        let key = chunk_key();
        fill(&cache, &admission, &key, at(LATER_MS));
        // Another query of the same table, resident in the chunk before the first.
        let other_key = TimeChunkKey::new(other_identity(), chunk_key_at(-1).chunk());
        assert_eq!(
            fill(&cache, &admission, &other_key, at(LATER_MS)),
            Insert::Stored
        );

        // A change inside the first chunk reaches only that chunk's query.
        cache.record_refresh(
            &table(),
            BTreeMap::from([(1, 12)]),
            SOURCE_VERSION,
            SOURCE_TABLE_ID,
            DirtySpan::Hull(TimeChunk::new(HEAD_MS, HEAD_MS + 1)),
            Instant::now(),
        );
        assert_eq!(
            cache.lookup(&admission, &key, at(LATER_MS)),
            Lookup::Miss(MissReason::NotFound)
        );
        assert!(matches!(
            cache.lookup(&admission, &other_key, at(LATER_MS)),
            Lookup::Hit { .. }
        ));
    }
}
