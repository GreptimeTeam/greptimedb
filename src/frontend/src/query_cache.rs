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
//! # Validity
//!
//! An entry is served only while
//!
//! - with a [`CacheAuthority`]: the authority is ready, its reading is younger
//!   than [`RollupCacheConfig::max_staleness`], and the authority cursor equals the
//!   cursor the entry was filled at (`cursor equal -> hit`, `cursor changed ->
//!   miss`, unknown/unhealthy/stale -> miss);
//! - always: the entry is younger than [`RollupCacheConfig::max_age`], counted
//!   from the instant the fill captured *before* it read storage, so a slow fill
//!   is charged for the time it took.
//!
//! The authority is optional. Without one the cache is bounded only by
//! [`RollupCacheConfig::max_age`], which is what a deployment with no sequence
//! probe can serve; the cursor branch stays in place for the probe.
//!
//! # Invalidation
//!
//! A cursor says that *something* changed; it cannot say which chunks that change
//! reached. [`RollupCache::invalidate_range`] answers that for one identity: it
//! removes every entry of that identity whose *input dependency* overlaps a dirty
//! range, leaves every disjoint chunk and every other identity resident, and
//! returns how many it removed. [`RollupCache::invalidate_table_range`] instead
//! removes overlapping entries across all identities for a named source table.
//!
//! The input dependency is the span of source data the payload was materialized
//! from, recorded when the entry is stored. Deriving it from the evaluation
//! window is not enough: a chunk's output points can all come before a dirty
//! range while the history they folded — a lookback window, or the range of a
//! `rate(x[5m])` — still reaches into it. [`RollupCache::insert`] therefore records
//! the chunk itself as the dependency, and
//! [`RollupCache::insert_with_dependency`] records a wider span where the caller
//! knows one; overlap is half-open on both spans, so adjacent spans do not
//! invalidate each other.
//!
//! Invalidation is bounded by an epoch, not by coordination. A fill captures the
//! epoch in its [`FillCursor`] before it reads storage, and an insertion whose
//! epoch is older than the cache's is dropped, so a fill that started before an
//! invalidation cannot resurrect what it removed. Every invalidation advances the
//! epoch exactly once, whether or not it removed anything.
//!
//! # Memory
//!
//! The cache is bounded by bytes, not entries: each entry is charged its payload's
//! buffer memory plus a fixed per-entry overhead, and moka evicts by that weight.
//! A payload that is still being filled is not charged until it is inserted, since
//! the cache cannot see it before then.
//!
//! # Concurrency
//!
//! A lookup is local: it reads the authority status and at most one entry, never
//! storage, and never waits for an in-flight fill or for cache maintenance. There
//! is no single-flight, so a miss while another fill of the same chunk is running
//! proceeds; the competing fills both reach [`RollupCache::insert`], where the
//! older cursor loses and an equal cursor is replaced. Everything here is
//! synchronous and no lock is held across an await, so a caller may await its
//! storage read without holding anything.
//!
//! An invalidation takes no lock either: it advances the epoch, then scans the
//! entries and removes the ones it overlaps. A fill that was already running is
//! dealt with by the epoch rule instead of by waiting for it.
//!
//! # Not covered
//!
//! Chunks cover *output* points. The evaluator still has to read the input history
//! every selected point folds: the first point of a `rate(x[5m])` chunk needs five
//! minutes of samples *before* the chunk start. Nothing here narrows that history.
//!
//! Not implemented: the storage sequence probe behind a [`CacheAuthority`], its
//! call into invalidation, the query-path invalidation wiring, and any
//! configuration option.

#![allow(dead_code)] // Wired into the query path by a later milestone.

use std::fmt;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime};

use common_recordbatch::RecordBatches;
use moka::ops::compute::Op;
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

/// The cursor recorded and compared when no authority is configured.
///
/// It is a placeholder, never a claim: without an authority the cache never
/// compares cursors and bounds freshness by [`RollupCacheConfig::max_age`] alone.
const NO_AUTHORITY_CURSOR: u64 = 0;

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
    /// Whether the request asked for ordered output.
    pub ordered: bool,
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
            .field("ordered", &self.ordered)
            .field("alias", &self.alias)
            .field("read_preference", &self.read_preference)
            .finish()
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

/// Why a lookup misses, so a caller can tell a cold chunk from an unusable one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MissReason {
    /// No entry holds this chunk.
    NotFound,
    /// The entry was filled at a cursor the authority has since moved past.
    CursorChanged {
        /// The cursor the entry was filled at.
        cached: u64,
        /// The cursor the authority vouches for now.
        authority: u64,
    },
    /// The authority does not know a cursor, so no entry can be served.
    AuthorityUnknown,
    /// The authority reported itself unhealthy.
    AuthorityUnhealthy,
    /// The authority last confirmed its cursor longer ago than the staleness bound.
    AuthorityStale {
        /// How long ago the authority confirmed its cursor.
        age: Duration,
    },
    /// The entry was filled longer ago than the hard age bound allows.
    Expired {
        /// How long ago the fill captured its read.
        age: Duration,
    },
    /// A source rename, repartition, or incarnation changed after validation.
    SourceChanged,
}

/// The outcome of one [`RollupCache::lookup`].
#[derive(Debug, PartialEq)]
pub enum Lookup {
    /// The whole materialized chunk, filled at `cursor`.
    Hit {
        /// The complete result of the chunk.
        payload: Arc<RecordBatches>,
        /// The warnings and infos the read behind `payload` recorded.
        annotations: Arc<CachedAnnotations>,
        /// The cursor the payload was filled at.
        cursor: u64,
        /// When the payload was inserted.
        inserted_at: SystemTime,
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
    /// A fill at a later cursor already holds the chunk, so nothing was written.
    Rejected,
    /// The chunk is empty, so nothing was written.
    Bypassed,
}

/// The cursor, the invalidation epoch, and the instant, a fill was planned at.
///
/// All three are captured before the fill reads storage: the cursor is what the
/// payload is validated against, the epoch is what a concurrent invalidation
/// invalidates the fill itself with, and the instant is what its age is counted
/// from, so a slow fill is neither fresher nor younger than the read it actually
/// performed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FillCursor {
    cursor: u64,
    epoch: u64,
    /// The source-table generation validated for the query that owns this fill.
    source_epoch: u64,
    /// The source table incarnation validated for the query that owns this fill.
    source_table_id: Option<u32>,
    captured_at: SystemTime,
}

impl FillCursor {
    /// The cursor the fill reads storage at.
    pub fn cursor(&self) -> u64 {
        self.cursor
    }

    /// The invalidation epoch of the cache when the fill was planned.
    ///
    /// An insertion carries it back to [`RollupCache::insert`], which drops a fill
    /// that an invalidation has since overtaken.
    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    /// Pins the source-table generation that was validated for this fill.
    pub(crate) fn with_source_epoch(mut self, source_epoch: u64) -> Self {
        self.source_epoch = source_epoch;
        self
    }

    /// Pins the source table incarnation validated for this fill.
    pub(crate) fn with_source_table_id(mut self, source_table_id: Option<u32>) -> Self {
        self.source_table_id = source_table_id;
        self
    }

    /// When the fill was planned, before it read storage.
    pub fn captured_at(&self) -> SystemTime {
        self.captured_at
    }
}

/// The authority cursor a cache may serve, and how much it is trusted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthorityStatus {
    /// The authority vouches for `cursor`, last confirmed at `observed_at`.
    ///
    /// Entries filled at exactly `cursor` are servable; entries filled at any
    /// other cursor are not. `observed_at` is compared with the `now` of a lookup
    /// to decide whether the reading is too old to act on.
    Ready {
        /// The cursor the authority vouches for.
        cursor: u64,
        /// When the authority last confirmed `cursor`.
        observed_at: SystemTime,
    },
    /// The authority cannot name a cursor right now.
    Unknown,
    /// The authority reported itself unhealthy.
    Unhealthy,
}

/// A source of the cursor that decides which cache entries may be served.
///
/// Absent, unknown, unhealthy, or stale authority readings all miss, so a
/// deployment without a working authority simply never hits. A cache built without
/// an authority skips the cursor rule entirely and is bounded only by the hard age
/// bound, which is what a deployment without the storage sequence probe serves.
pub trait CacheAuthority: Send + Sync {
    /// The cursor the authority vouches for, and how fresh that reading is.
    fn status(&self) -> AuthorityStatus;
}

/// Why a [`RollupCacheConfig`] is unusable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigError {
    /// The cache must be allowed some bytes.
    Bytes,
    /// A chunk must cover at least one millisecond.
    Chunk,
    /// The staleness bound must accept an authority reading at all.
    MaxStaleness,
    /// The age bound must accept an entry at all.
    MaxAge,
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Bytes => f.write_str("the cache byte budget must be positive"),
            Self::Chunk => f.write_str("the chunk duration must be positive"),
            Self::MaxStaleness => f.write_str("the max staleness must be positive"),
            Self::MaxAge => f.write_str("the max age must be positive"),
        }
    }
}

impl std::error::Error for ConfigError {}

/// The bounds a [`RollupCache`] enforces.
///
/// `tail` is a [`Duration`], so it is non-negative by construction and needs no
/// check; `max_bytes`, `chunk`, `max_staleness`, and `max_age` all have to be
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
    max_staleness: Duration,
    max_age: Duration,
}

impl RollupCacheConfig {
    /// The validated configuration of a cache.
    pub fn new(
        max_bytes: u64,
        tail: Duration,
        chunk: Duration,
        max_staleness: Duration,
        max_age: Duration,
    ) -> Result<Self, ConfigError> {
        if max_bytes == 0 {
            return Err(ConfigError::Bytes);
        }
        if chunk.is_zero() {
            return Err(ConfigError::Chunk);
        }
        if max_staleness.is_zero() {
            return Err(ConfigError::MaxStaleness);
        }
        if max_age.is_zero() {
            return Err(ConfigError::MaxAge);
        }
        Ok(Self {
            max_bytes,
            tail,
            chunk,
            max_staleness,
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

    /// How old an authority reading may be before it stops being trusted.
    pub fn max_staleness(&self) -> Duration {
        self.max_staleness
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
    /// The cursor the payload was filled at, never the one seen at insertion.
    cursor: u64,
    /// The span of source data the payload was materialized from.
    ///
    /// A dirty input range invalidates the entry when it overlaps this span, not
    /// the chunk's own output span: the history a chunk folded can reach before
    /// its first output point.
    dependency: TimeChunk,
    /// The invalidation epoch the fill captured before it read storage.
    epoch: u64,
    /// The source table incarnation validated by the probe.
    source_table_id: Option<u32>,
    /// The bytes this entry is charged: its payload plus a fixed overhead.
    weight: u32,
    /// When the fill that produced the payload captured its read.
    validated_at: SystemTime,
    /// When the entry was inserted.
    inserted_at: SystemTime,
}

/// A bounded, frontend-local cache of whole PromQL chunks.
///
/// The cache is read-through: [`RollupCache::lookup`] answers from memory or says
/// why it cannot, and the caller executes storage and calls
/// [`RollupCache::insert`]. Entries are evicted by moka in admission order once the
/// byte budget is spent, which is bounded but not a strict recency ordering.
pub struct RollupCache {
    config: RollupCacheConfig,
    authority: Option<Arc<dyn CacheAuthority>>,
    entries: Cache<TimeChunkKey, Entry>,
    /// How many invalidations have run, which is what a fill is validated against.
    ///
    /// It is advanced by every invalidation method and never read as a clock: only
    /// the ordering between the epoch a fill captured and the current one matters.
    invalidation_epoch: AtomicU64,
    /// The source-table generation a query was validated against. Every rename,
    /// repartition, or incarnation transition advances it; a fill planned under an
    /// older generation is rejected even if its ordinary invalidation epoch is new.
    source_epoch: AtomicU64,
    /// Whether callers validate the source sequence before serving an entry.
    ///
    /// Off by default so an embedder that has no sequence authority (a test, or a
    /// deployment without the probe) keeps the age-bounded behavior. A deployment
    /// that can probe turns it on and invalidates the affected table before a
    /// lookup.
    source_probe: bool,
    /// Serializes lookup, publication, and invalidation.
    ///
    /// The epoch alone cannot fence publication: a fill may pass its epoch check
    /// and pause while an invalidation scans, then insert its pre-invalidation
    /// payload. Holding this lock across the epoch check and the store means an
    /// invalidation cannot complete between them, and a lookup cannot observe a
    /// payload that an already-completed invalidation meant to remove.
    invalidation_lock: Mutex<()>,
    /// How many lookups were answered from an existing entry.
    ///
    /// Only a test reads it ([`RollupCache::hit_count`]); it is compiled out of a
    /// production build and never takes part in a lookup decision.
    #[cfg(any(test, feature = "testing"))]
    hits: AtomicU64,
}

impl RollupCache {
    /// A cache bounded by `config` and, when given, validated by `authority`.
    ///
    /// Without an authority the cache never compares cursors and serves entries
    /// while they are younger than [`RollupCacheConfig::max_age`].
    pub fn new(config: RollupCacheConfig, authority: Option<Arc<dyn CacheAuthority>>) -> Self {
        let entries = Cache::builder()
            .max_capacity(config.max_bytes())
            .weigher(|_key: &TimeChunkKey, entry: &Entry| entry.weight)
            .build();
        Self {
            config,
            authority,
            entries,
            invalidation_epoch: AtomicU64::new(0),
            source_epoch: AtomicU64::new(0),
            source_probe: false,
            invalidation_lock: Mutex::new(()),
            #[cfg(any(test, feature = "testing"))]
            hits: AtomicU64::new(0),
        }
    }

    /// Requires callers to validate the source sequence before serving entries.
    ///
    /// This is a construction-time switch: a cache that never receives a source
    /// probe must not pretend to be sequence-consistent, and a cache that does must
    /// call [`Self::invalidate_table_range`] or [`Self::invalidate_table`] before
    /// advancing its own cursor.
    pub fn with_source_probe(mut self) -> Self {
        self.source_probe = true;
        self
    }

    /// Whether this cache expects its caller to validate the source sequence.
    pub fn source_probe_enabled(&self) -> bool {
        self.source_probe
    }

    /// The configuration this cache enforces.
    pub fn config(&self) -> &RollupCacheConfig {
        &self.config
    }

    /// How many invalidations this cache has run.
    ///
    /// A fill captures it in its [`FillCursor`] and an insertion compares it with
    /// the value current at insertion time; nothing else reads it.
    pub fn invalidation_epoch(&self) -> u64 {
        self.invalidation_epoch.load(Ordering::SeqCst)
    }

    /// The source-table generation currently validated.
    pub fn source_epoch(&self) -> u64 {
        self.source_epoch.load(Ordering::SeqCst)
    }

    /// Advances the source-table generation and returns the new value.
    ///
    /// A query captures the returned generation before executing; a fill carrying
    /// an older generation is dropped because its source identity or route may no
    /// longer be the one it read.
    pub fn advance_source_epoch(&self) -> u64 {
        let _guard = self.invalidation_lock.lock().unwrap();
        self.source_epoch.fetch_add(1, Ordering::SeqCst) + 1
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

    /// Tries to serve `key` from memory as of `now`.
    ///
    /// This is local and infallible: it reads the authority status and at most one
    /// entry, never storage, and never waits for an in-flight fill or for cache
    /// maintenance.
    pub fn lookup(&self, key: &TimeChunkKey, now: SystemTime) -> Lookup {
        self.lookup_fenced(key, now, self.source_epoch(), None)
    }

    /// [`Self::lookup`] with the source generation the caller validated.
    ///
    /// The check runs under the same lock as the hit decision: a rename that
    /// advanced the source epoch after the caller's pre-check cannot be followed by
    /// serving an entry keyed to the old identity.
    pub fn lookup_fenced(
        &self,
        key: &TimeChunkKey,
        now: SystemTime,
        source_epoch: u64,
        source_table_id: Option<u32>,
    ) -> Lookup {
        if key.chunk.is_empty() {
            return Lookup::Bypass(BypassReason::EmptyChunk);
        }
        let _guard = self.invalidation_lock.lock().unwrap();
        if self.source_epoch() != source_epoch {
            return Lookup::Miss(MissReason::SourceChanged);
        }
        let cursor = match self.authorized_cursor(now) {
            Ok(cursor) => cursor,
            Err(reason) => return Lookup::Miss(reason),
        };
        let Some(entry) = self.entries.get(key) else {
            return Lookup::Miss(MissReason::NotFound);
        };
        if entry.source_table_id != source_table_id {
            return Lookup::Miss(MissReason::SourceChanged);
        }
        if self.authority.is_some() && entry.cursor != cursor {
            return Lookup::Miss(MissReason::CursorChanged {
                cached: entry.cursor,
                authority: cursor,
            });
        }
        let age = now.duration_since(entry.validated_at).unwrap_or_default();
        if age > self.config.max_age() {
            return Lookup::Miss(MissReason::Expired { age });
        }
        // The entry is served: nothing else in this cache counts as a hit.
        #[cfg(any(test, feature = "testing"))]
        self.hits.fetch_add(1, Ordering::SeqCst);
        Lookup::Hit {
            payload: entry.payload,
            annotations: entry.annotations,
            cursor: entry.cursor,
            inserted_at: entry.inserted_at,
        }
    }

    /// The cursor, the epoch, and the instant, a fill of a chunk should be planned
    /// at.
    ///
    /// Callers must pass the returned [`FillCursor`] to [`RollupCache::insert`]
    /// instead of reading the authority, or the epoch, again once the fill
    /// completes.
    pub fn fill_cursor(&self, now: SystemTime) -> Result<FillCursor, MissReason> {
        let epoch = self.invalidation_epoch();
        self.authorized_cursor(now).map(|cursor| FillCursor {
            cursor,
            epoch,
            source_epoch: self.source_epoch(),
            source_table_id: None,
            captured_at: now,
        })
    }

    /// Stores `payload` as the whole result of `key`'s chunk, filled at `fill`.
    ///
    /// The insertion is conditional: a fill at a cursor older than the entry
    /// already in place is dropped, so a slow fill cannot overwrite a newer one, and
    /// a fill an invalidation has overtaken is dropped as well, so a slow fill
    /// cannot resurrect what that invalidation removed. A fill at the same cursor
    /// replaces the payload, since both read the same storage state. The entry
    /// records the cursor and the instant of `fill`, not whichever the authority
    /// reports now, and is charged the bytes of its payload so eviction stays within
    /// the configured budget.
    ///
    /// The entry's input dependency is the chunk itself: the payload covers exactly
    /// the points of its span and reads nothing else.
    /// [`RollupCache::insert_with_dependency`] records a wider span for a read that
    /// folded history outside it.
    pub fn insert(
        &self,
        key: TimeChunkKey,
        fill: FillCursor,
        payload: Arc<RecordBatches>,
    ) -> Insert {
        self.insert_annotated(key, fill, payload, Arc::default())
    }

    /// [`RollupCache::insert`] with the annotations the read recorded.
    ///
    /// The annotations are stored with the payload and served with it, so a hit
    /// reports what the read it replaces would have reported. A chunk filled
    /// without them is served without them.
    pub fn insert_annotated(
        &self,
        key: TimeChunkKey,
        fill: FillCursor,
        payload: Arc<RecordBatches>,
        annotations: Arc<CachedAnnotations>,
    ) -> Insert {
        let dependency = key.chunk();
        self.insert_with_dependency(key, fill, payload, annotations, dependency)
    }

    /// [`RollupCache::insert_annotated`] with the input span the payload was
    /// materialized from.
    ///
    /// `dependency` is the span of source data the read touched, which invalidation
    /// compares against a dirty range. It must contain the chunk's own span, and is
    /// wider than it whenever the evaluation folded history before the chunk start
    /// (a lookback window, or the range of `rate(x[5m])`); an entry whose dependency
    /// is only the chunk is invalidated only by a range that reaches its output points.
    ///
    /// The insertion is guarded by `fill`: a fill overtaken by an invalidation is
    /// dropped, even though its key and cursor are still current, because the read
    /// behind it may have returned exactly what that invalidation removed.
    pub fn insert_with_dependency(
        &self,
        key: TimeChunkKey,
        fill: FillCursor,
        payload: Arc<RecordBatches>,
        annotations: Arc<CachedAnnotations>,
        dependency: TimeChunk,
    ) -> Insert {
        if key.chunk.is_empty() {
            return Insert::Bypassed;
        }
        let _guard = self.invalidation_lock.lock().unwrap();
        // An invalidation ran after this fill captured its epoch, so the data the
        // fill read may be the data the invalidation removed. The fill loses.
        if self.invalidation_epoch() > fill.epoch() || self.source_epoch() != fill.source_epoch {
            return Insert::Rejected;
        }
        let candidate = Entry {
            weight: Entry::weigh(&payload, &annotations),
            payload,
            annotations,
            cursor: fill.cursor(),
            dependency,
            epoch: fill.epoch(),
            source_table_id: fill.source_table_id,
            validated_at: fill.captured_at(),
            inserted_at: SystemTime::now(),
        };
        let stored = Arc::clone(&candidate.payload);
        // `and_compute_with` serializes only the swap of this one key; nothing is
        // locked across the storage read that produced `payload`.
        let result = self
            .entries
            .entry(key.clone())
            .and_compute_with(|existing| match existing {
                Some(entry) if entry.value().cursor > candidate.cursor => Op::Nop,
                _ => Op::Put(candidate),
            });
        if let Some(entry) = result.into_entry()
            && entry.value().cursor > fill.cursor()
        {
            return Insert::Rejected;
        }
        // The epoch may have moved between the check above and the store, and the
        // invalidation that moved it may have scanned this chunk before the store
        // landed. Drop the entry rather than serve what that invalidation was
        // meant to remove; a newer fill of the same chunk keeps its own entry, as
        // only this payload is dropped.
        if self.invalidation_epoch() > fill.epoch() {
            self.remove_own_entry(&key, &stored, fill.epoch());
            return Insert::Rejected;
        }
        Insert::Stored
    }

    /// Removes `key` while it still holds `payload` filled at `epoch`.
    ///
    /// A concurrent fill may have replaced the entry since it was written, so the
    /// removal is conditional on the entry still being the one that lost the epoch
    /// race.
    fn remove_own_entry(&self, key: &TimeChunkKey, payload: &Arc<RecordBatches>, epoch: u64) {
        self.entries
            .entry(key.clone())
            .and_compute_with(|existing| match existing {
                Some(entry)
                    if entry.value().epoch == epoch
                        && Arc::ptr_eq(&entry.value().payload, payload) =>
                {
                    Op::Remove
                }
                _ => Op::Nop,
            });
    }

    /// Drops every entry of `identity` whose input dependency overlaps
    /// `input_span`, and returns how many entries were removed.
    ///
    /// `input_span` is a dirty half-open `[start, end)` range of *source* data, not
    /// of output points: an entry goes when any part of the history it was
    /// materialized from is in the range, so a chunk whose output points are all
    /// earlier can still be invalidated by a dependency that reaches into it.
    /// Overlap is half-open on both sides, so a range that ends where a dependency
    /// starts removes nothing. Entries of other identities, and of this identity
    /// whose dependency is disjoint, stay resident.
    ///
    /// The epoch advances once per call, whether or not anything was removed, so a
    /// fill that captured its epoch before the call cannot store afterwards. The
    /// method is synchronous and takes no lock, so it never waits for a fill and
    /// nothing it does is held across an await.
    ///
    /// The scan and the removals are not one atomic step: a fill that lands between
    /// them is removed too, even though it read storage after this call. That costs
    /// a later miss, never a wrong answer, and is why no fill has to be waited for.
    pub fn invalidate_range(&self, identity: &QueryIdentity, input_span: TimeChunk) -> usize {
        let _guard = self.invalidation_lock.lock().unwrap();
        // Advanced before the scan: a fill that captures the new epoch from now on
        // read storage after this point, and one that captured an older epoch is
        // refused by `insert_with_dependency`.
        self.invalidation_epoch.fetch_add(1, Ordering::SeqCst);

        // The keys are collected first because the scan borrows the cache that the
        // removals mutate.
        let mut dirty = Vec::new();
        for (key, entry) in self.entries.iter() {
            if key.identity() == identity && overlaps(entry.dependency, input_span) {
                dirty.push(key);
            }
        }
        dirty
            .into_iter()
            .filter(|key| self.entries.remove(key.as_ref()).is_some())
            .count()
    }

    /// Drops entries for `table` whose input dependencies overlap `input_span`,
    /// regardless of their query identity, and returns the number removed.
    ///
    /// The table is the exact named source table in [`QueryIdentity`]; matching it
    /// across catalogs and schemas is conservative. As in [`Self::invalidate_range`],
    /// both spans are half-open, the epoch advances once even if nothing is removed,
    /// and a concurrent fill that started before invalidation cannot be stored.
    /// The scan and removals are not atomic, so a newer fill may also be removed.
    pub fn invalidate_table_range(&self, table: &str, input_span: TimeChunk) -> usize {
        let _guard = self.invalidation_lock.lock().unwrap();
        self.invalidation_epoch.fetch_add(1, Ordering::SeqCst);

        let mut dirty = Vec::new();
        for (key, entry) in self.entries.iter() {
            if key.identity().source_table == table && overlaps(entry.dependency, input_span) {
                dirty.push(key);
            }
        }
        dirty
            .into_iter()
            .filter(|key| self.entries.remove(key.as_ref()).is_some())
            .count()
    }

    /// Drops every entry whose source table is `table`, regardless of span, and
    /// returns the number removed.
    ///
    /// This is the conservative invalidation for a source change whose timestamp
    /// range cannot be proved: a row removed by compaction may advance the source
    /// sequence without leaving a timestamp in the new range. Keeping an entry is
    /// never worth serving a point whose input was erased, so the caller evicts the
    /// whole table instead of guessing a span.
    pub fn invalidate_table(&self, table: &str) -> usize {
        let _guard = self.invalidation_lock.lock().unwrap();
        self.invalidation_epoch.fetch_add(1, Ordering::SeqCst);

        let mut dirty = Vec::new();
        for (key, _) in self.entries.iter() {
            if key.identity().source_table == table {
                dirty.push(key);
            }
        }
        dirty
            .into_iter()
            .filter(|key| self.entries.remove(key.as_ref()).is_some())
            .count()
    }

    /// The cursor a lookup at `now` may serve, or why it may serve none.
    fn authorized_cursor(&self, now: SystemTime) -> Result<u64, MissReason> {
        let Some(authority) = &self.authority else {
            // No probe is deployed: freshness is bounded by age alone.
            return Ok(NO_AUTHORITY_CURSOR);
        };
        match authority.status() {
            AuthorityStatus::Unknown => Err(MissReason::AuthorityUnknown),
            AuthorityStatus::Unhealthy => Err(MissReason::AuthorityUnhealthy),
            AuthorityStatus::Ready {
                cursor,
                observed_at,
            } => {
                let age = now.duration_since(observed_at).unwrap_or_default();
                if age > self.config.max_staleness() {
                    Err(MissReason::AuthorityStale { age })
                } else {
                    Ok(cursor)
                }
            }
        }
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
    use std::sync::{Arc, Barrier, Mutex};
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
            ordered: false,
            alias: None,
            read_preference: None,
        }
    }

    fn chunk_key() -> TimeChunkKey {
        chunk_key_at(0)
    }

    /// The key of the chunk `index` chunks after `HEAD_MS`, so `-1` is the chunk
    /// that ends where the first one starts.
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
            CHUNK,
            CHUNK,
            Duration::from_secs(60),
            Duration::from_secs(3_600),
        )
        .expect("the test configuration is valid")
    }

    fn config_with(max_bytes: u64, tail: Duration, chunk: Duration) -> RollupCacheConfig {
        RollupCacheConfig::new(
            max_bytes,
            tail,
            chunk,
            Duration::from_secs(60),
            Duration::from_secs(3_600),
        )
        .expect("the test configuration is valid")
    }

    /// A cache with no authority: freshness is bounded by age alone.
    fn cache() -> RollupCache {
        RollupCache::new(config(), None)
    }

    /// An authority whose reading a test sets directly.
    #[derive(Clone)]
    struct FakeAuthority {
        status: Arc<Mutex<AuthorityStatus>>,
    }

    impl Default for FakeAuthority {
        fn default() -> Self {
            Self {
                status: Arc::new(Mutex::new(AuthorityStatus::Unknown)),
            }
        }
    }

    impl FakeAuthority {
        fn ready(&self, cursor: u64, observed_at: SystemTime) {
            *self.status.lock().unwrap() = AuthorityStatus::Ready {
                cursor,
                observed_at,
            };
        }

        fn unknown(&self) {
            *self.status.lock().unwrap() = AuthorityStatus::Unknown;
        }

        fn unhealthy(&self) {
            *self.status.lock().unwrap() = AuthorityStatus::Unhealthy;
        }
    }

    impl CacheAuthority for FakeAuthority {
        fn status(&self) -> AuthorityStatus {
            *self.status.lock().unwrap()
        }
    }

    fn authority_cache(authority: &FakeAuthority) -> RollupCache {
        let authority: Arc<dyn CacheAuthority> = Arc::new(authority.clone());
        RollupCache::new(config(), Some(authority))
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

    /// Every span a plan returns, in time order, must partition the coverage
    /// exactly once.
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
            "the last span must reach past the last point of {:?}",
            span
        );
        for direct in direct {
            assert!(
                direct.span().start_ms() <= span.end_ms(),
                "a direct span escapes the coverage: {direct:?}"
            );
        }
    }

    #[test]
    fn a_sliding_window_reuses_every_interior_chunk() {
        let cache = cache();
        let now = at(LATER_MS);
        let first = stmt_between("rate(x[5m])", HEAD_MS, HEAD_MS + 3_600_000, STEP_MS);
        let second = stmt_between(
            "rate(x[5m])",
            HEAD_MS + CHUNK_MS,
            HEAD_MS + 3_600_000 + CHUNK_MS,
            STEP_MS,
        );

        let (_, first_chunks, _, _) = planned(&cache, &first, now);
        let (_, second_chunks, _, _) = planned(&cache, &second, now);

        // The interior chunks are the same absolute spans, so they are the same
        // keys even though the second request starts five minutes later.
        let shared: Vec<TimeChunk> = second_chunks
            .iter()
            .copied()
            .filter(|chunk| first_chunks.contains(chunk))
            .collect();
        assert_eq!(shared.len(), 11, "{shared:?}");
        let interior = shared[5];
        let fill = cache.fill_cursor(now).unwrap();
        let filled = payload();
        assert_eq!(
            cache.insert(
                TimeChunkKey::new(identity(), interior),
                fill,
                filled.clone()
            ),
            Insert::Stored
        );
        let Lookup::Hit { payload: hit, .. } =
            cache.lookup(&TimeChunkKey::new(identity(), interior), now)
        else {
            panic!("the interior chunk is a hit under the second window too");
        };
        assert!(Arc::ptr_eq(&hit, &filled));

        // The second window starts on a canonical bound, so only the point past
        // its last chunk is executed directly.
        let (span, second_all, direct, _) = planned(&cache, &second, now);
        assert_eq!(second_all.len(), 12);
        assert_eq!(direct.len(), 1, "{direct:?}");
        assert_eq!(direct[0].span().start_ms(), span.end_ms());
        assert_eq!(direct[0].span().end_ms(), span.end_ms() + STEP_MS);
    }

    #[test]
    fn the_tail_is_measured_from_now_not_from_the_query_end() {
        let cache = cache();
        // A historical window: its end is an hour before `now`, so nothing of it
        // can still be arriving and the whole range is cacheable.
        let historical = stmt_between("rate(x[5m])", HEAD_MS, HEAD_MS + 3_600_000, STEP_MS);
        let (span, chunks, direct, tail) = planned(&cache, &historical, at(LATER_MS));
        assert_eq!(tail, None);
        assert!(
            direct
                .iter()
                .all(|span| span.reason() == DirectReason::Edge)
        );
        assert_partition(span, &chunks, &direct);
        assert!(chunks.len() >= 11, "{chunks:?}");

        // A window ending at `now` keeps its last tail direct.
        let recent = stmt_between(
            "rate(x[5m])",
            HEAD_MS + 3_600_000,
            HEAD_MS + 3_600_000 + 3_600_000,
            STEP_MS,
        );
        let now = at(HEAD_MS + 3_600_000 + 3_600_000);
        let (span, chunks, direct, tail) = planned(&cache, &recent, now);
        let tail = tail.expect("the recent window has a tail");
        assert_eq!(tail.end_ms(), span.end_ms() + STEP_MS);
        assert_eq!(tail.start_ms(), HEAD_MS + 7_200_000 - CHUNK_MS);
        assert!(
            direct
                .iter()
                .any(|span| span.reason() == DirectReason::Tail)
        );
        assert_partition(span, &chunks, &direct);
        assert!(chunks.iter().all(|chunk| chunk.end_ms() <= tail.start_ms()));
    }

    #[test]
    fn edge_and_tail_spans_partition_an_odd_phase() {
        // A grid that starts off a step boundary, and off a chunk boundary, so the
        // request needs a leading and a trailing partial chunk.
        let step = 37_000;
        let chunk = 3 * step;
        let start = 1_001 + 2 * step;
        let stmt = stmt_from("rate(x[5m])", start, 11, step);
        let cache = RollupCache::new(
            config_with(1 << 20, Duration::ZERO, Duration::from_millis(chunk as u64)),
            None,
        );
        let odd_grid = QueryIdentity {
            step_ms: step,
            ..identity()
        };
        let (span, chunks, direct, tail) = planned_for(odd_grid, &cache, &stmt, at(LATER_MS));

        assert_eq!(tail, None);
        assert_eq!(chunks.len(), 3, "{chunks:?}");
        let filled = direct
            .iter()
            .filter(|s| s.reason() == DirectReason::Edge)
            .count();
        assert_eq!(filled, 2, "a leading and a trailing edge: {direct:?}");
        assert_partition(span, &chunks, &direct);

        // Every cacheable span is a whole configured chunk, and its bounds are
        // canonical bounds of this grid's phase.
        let phase = start.rem_euclid(step);
        for chunk_span in &chunks {
            assert_eq!(chunk_span.end_ms() - chunk_span.start_ms(), chunk);
            assert_eq!((chunk_span.start_ms() - phase) % chunk, 0);
        }
        for direct in &direct {
            assert!(
                direct.span().end_ms() - direct.span().start_ms() < chunk,
                "a partial chunk is never keyed as a full one: {direct:?}"
            );
        }
    }

    #[test]
    fn a_different_step_changes_the_identity() {
        let finer = QueryIdentity {
            step_ms: STEP_MS / 2,
            ..identity()
        };
        assert_ne!(finer, identity());
        assert_ne!(
            TimeChunkKey::new(finer, TimeChunk::new(0, 1)),
            TimeChunkKey::new(identity(), TimeChunk::new(0, 1))
        );
    }

    #[test]
    fn a_different_grid_phase_changes_the_chunk_bounds() {
        let cache = cache();
        let now = at(LATER_MS);
        let aligned = stmt_from("rate(x[5m])", HEAD_MS, 11, STEP_MS);
        let shifted = stmt_from("rate(x[5m])", HEAD_MS + 30_000, 11, STEP_MS);

        let (_, aligned_chunks, _, _) = planned(&cache, &aligned, now);
        let (_, shifted_chunks, _, _) = planned(&cache, &shifted, now);
        assert!(!aligned_chunks.is_empty() && !shifted_chunks.is_empty());
        for chunk in &aligned_chunks {
            assert!(!shifted_chunks.contains(chunk), "{chunk:?} is shared");
        }
    }

    #[test]
    fn the_query_span_is_not_part_of_the_identity() {
        let first = QuerySpan::of(&stmt_between("x", HEAD_MS, HEAD_MS + 600_000, STEP_MS)).unwrap();
        let second = QuerySpan::of(&stmt_between(
            "x",
            HEAD_MS + 60_000,
            HEAD_MS + 660_000,
            STEP_MS,
        ))
        .unwrap();
        assert_ne!(first, second);

        // ... while the identities of both requests are the same one.
        let cache = cache();
        let now = at(LATER_MS);
        let (first_span, _, _, _) = planned(
            &cache,
            &stmt_between("x", HEAD_MS, HEAD_MS + 600_000, STEP_MS),
            now,
        );
        let (second_span, _, _, _) = planned(
            &cache,
            &stmt_between("x", HEAD_MS + 60_000, HEAD_MS + 660_000, STEP_MS),
            now,
        );
        assert_ne!(first_span, second_span);
    }

    #[test]
    fn an_identity_on_another_grid_bypasses() {
        let cache = cache();
        let stmt = stmt_from("rate(x[5m])", HEAD_MS, 11, STEP_MS / 2);
        assert_eq!(
            cache.plan(identity(), &stmt, at(LATER_MS)),
            ChunkPlan::Bypass(BypassReason::StepMismatch)
        );
    }

    #[test]
    fn a_chunk_that_is_not_a_whole_number_of_steps_bypasses() {
        let odd_chunk = Duration::from_millis(100_000);
        let stmt = stmt_from("rate(x[5m])", HEAD_MS, 11, STEP_MS);

        assert_eq!(
            plan_chunks(&stmt, Duration::ZERO, odd_chunk, at(LATER_MS)),
            Err(BypassReason::MisalignedChunk)
        );
        assert_eq!(
            config_with(1 << 20, Duration::ZERO, odd_chunk).chunk_steps(STEP_MS),
            None
        );
        assert_eq!(config().chunk_steps(STEP_MS), Some(5));
        assert_eq!(config().chunk_steps(STEP_MS / 2), Some(10));
    }

    #[test]
    fn a_non_windowable_expression_bypasses_the_cache() {
        let cache = cache();
        let now = at(LATER_MS);
        // `sum` is admitted by the window allowlist, `avg` is not.
        for query in ["avg(rate(x[5m]))", "{host=\"a\"}", "rate(x[5m] offset 1m)"] {
            let stmt = stmt_from(query, HEAD_MS, 11, STEP_MS);
            assert_eq!(
                plan_chunks(&stmt, Duration::ZERO, CHUNK, now),
                Err(BypassReason::NotWindowable),
                "{query}"
            );
            assert_eq!(
                cache.plan(identity(), &stmt, now),
                ChunkPlan::Bypass(BypassReason::NotWindowable),
                "{query}"
            );
        }
    }

    #[test]
    fn an_invalid_span_bypasses_the_cache() {
        let now = at(LATER_MS);
        let stmt = stmt_from("x", HEAD_MS, 11, STEP_MS);
        let mut backwards = stmt.clone();
        backwards.end = backwards.start - Duration::from_millis(STEP_MS as u64);
        assert_eq!(
            plan_chunks(&backwards, Duration::ZERO, CHUNK, now),
            Err(BypassReason::InvalidSpan)
        );

        // A step carrying a sub-millisecond part lays out a grid the planners do
        // not evaluate on.
        let mut sub_millisecond = stmt;
        sub_millisecond.interval = Duration::from_micros(1_500);
        assert_eq!(
            plan_chunks(&sub_millisecond, Duration::ZERO, CHUNK, now),
            Err(BypassReason::InvalidSpan)
        );
    }

    #[test]
    fn identity_fields_are_isolated() {
        let base = identity();
        let chunk = TimeChunk::new(HEAD_MS, HEAD_MS + CHUNK_MS);
        let cases = [
            QueryIdentity {
                tenant: "other".to_string(),
                ..base.clone()
            },
            QueryIdentity {
                catalog: "other".to_string(),
                ..base.clone()
            },
            QueryIdentity {
                schema: "other".to_string(),
                ..base.clone()
            },
            QueryIdentity {
                query: "rate(http_requests_total[6m])".to_string(),
                ..base.clone()
            },
            QueryIdentity {
                source_table: "other".to_string(),
                ..base.clone()
            },
            QueryIdentity {
                step_ms: STEP_MS * 2,
                ..base.clone()
            },
            QueryIdentity {
                lookback_ms: 600_000,
                ..base.clone()
            },
            QueryIdentity {
                route: QueryRoute::Native,
                ..base.clone()
            },
            QueryIdentity {
                ordered: true,
                ..base.clone()
            },
            QueryIdentity {
                alias: Some("q".to_string()),
                ..base.clone()
            },
            QueryIdentity {
                read_preference: Some("LEADER".to_string()),
                ..base.clone()
            },
        ];

        for case in cases {
            assert_ne!(case, base);
            assert_ne!(
                TimeChunkKey::new(case, chunk),
                TimeChunkKey::new(base.clone(), chunk)
            );
        }

        assert_eq!(
            TimeChunkKey::new(base.clone(), chunk),
            TimeChunkKey::new(base, chunk)
        );
    }

    #[test]
    fn an_identity_prints_its_context_but_never_its_query_text() {
        let printed = format!("{:?}", identity());
        assert!(printed.contains(REDACTED_QUERY), "{printed}");
        assert!(!printed.contains("http_requests_total"), "{printed}");
        let distinct_table = QueryIdentity {
            source_table: "private_table".to_string(),
            ..identity()
        };
        let printed = format!("{distinct_table:?}");
        assert!(
            printed.contains("source_table: \"<redacted>\""),
            "{printed}"
        );
        assert!(!printed.contains("private_table"), "{printed}");

        let chunk = chunk_key();
        assert!(!format!("{chunk:?}").contains("http_requests_total"));

        // The alias and the read preference are not query text, so they print.
        let aliased = QueryIdentity {
            alias: Some("q".to_string()),
            read_preference: Some("LEADER".to_string()),
            ..identity()
        };
        let printed = format!("{aliased:?}");
        assert!(printed.contains("alias: Some(\"q\")"), "{printed}");
        assert!(
            printed.contains("read_preference: Some(\"LEADER\")"),
            "{printed}"
        );
        assert!(!printed.contains("http_requests_total"), "{printed}");
    }

    #[test]
    fn an_alias_change_separates_entries() {
        let cache = cache();
        let now = at(LATER_MS);
        let key = chunk_key();
        let fill = cache.fill_cursor(now).unwrap();
        assert_eq!(cache.insert(key.clone(), fill, payload()), Insert::Stored);
        assert!(matches!(cache.lookup(&key, now), Lookup::Hit { .. }));

        let aliased = TimeChunkKey::new(
            QueryIdentity {
                alias: Some("q".to_string()),
                ..identity()
            },
            key.chunk(),
        );
        assert!(matches!(
            cache.lookup(&aliased, now),
            Lookup::Miss(MissReason::NotFound)
        ));
    }

    #[test]
    fn a_read_preference_change_separates_entries() {
        let cache = cache();
        let now = at(LATER_MS);
        let key = chunk_key();
        let fill = cache.fill_cursor(now).unwrap();
        assert_eq!(cache.insert(key.clone(), fill, payload()), Insert::Stored);
        assert!(matches!(cache.lookup(&key, now), Lookup::Hit { .. }));

        let other = TimeChunkKey::new(
            QueryIdentity {
                read_preference: Some("LEADER".to_string()),
                ..identity()
            },
            key.chunk(),
        );
        assert!(matches!(
            cache.lookup(&other, now),
            Lookup::Miss(MissReason::NotFound)
        ));
    }

    #[test]
    fn a_missed_chunk_is_filled_and_then_hit() {
        let cache = cache();
        let now = at(LATER_MS);
        let key = chunk_key();

        assert!(matches!(
            cache.lookup(&key, now),
            Lookup::Miss(MissReason::NotFound)
        ));
        let fill = cache.fill_cursor(now).expect("no authority always fills");
        let filled = payload();
        assert_eq!(
            cache.insert(key.clone(), fill, filled.clone()),
            Insert::Stored
        );

        let Lookup::Hit {
            payload: hit,
            cursor,
            ..
        } = cache.lookup(&key, now)
        else {
            panic!("the filled chunk is a hit");
        };
        assert_eq!(cursor, NO_AUTHORITY_CURSOR);
        assert!(Arc::ptr_eq(&hit, &filled));
    }

    #[test]
    fn only_the_authority_cursor_is_served() {
        let authority = FakeAuthority::default();
        authority.ready(1, at(LATER_MS));
        let cache = authority_cache(&authority);
        let now = at(LATER_MS);
        let key = chunk_key();
        let fill = cache.fill_cursor(now).unwrap();
        cache.insert(key.clone(), fill, payload());

        // Equal cursor: a hit.
        authority.ready(1, now);
        assert!(matches!(cache.lookup(&key, now), Lookup::Hit { .. }));

        // Changed cursor: a miss, even though the entry is still resident.
        authority.ready(2, now);
        assert!(matches!(
            cache.lookup(&key, now),
            Lookup::Miss(MissReason::CursorChanged {
                cached: 1,
                authority: 2
            })
        ));

        // Unknown and unhealthy authorities never serve anything.
        authority.unknown();
        assert!(matches!(
            cache.lookup(&key, now),
            Lookup::Miss(MissReason::AuthorityUnknown)
        ));
        assert_eq!(
            cache.fill_cursor(now).unwrap_err(),
            MissReason::AuthorityUnknown
        );

        authority.unhealthy();
        assert!(matches!(
            cache.lookup(&key, now),
            Lookup::Miss(MissReason::AuthorityUnhealthy)
        ));
    }

    #[test]
    fn a_stale_authority_reading_never_serves() {
        let authority = FakeAuthority::default();
        authority.ready(1, at(LATER_MS));
        let cache = authority_cache(&authority);
        let now = at(LATER_MS);
        let key = chunk_key();
        let fill = cache.fill_cursor(now).unwrap();
        cache.insert(key.clone(), fill, payload());

        // Exactly at the bound the reading is still trusted...
        authority.ready(1, now - config().max_staleness());
        assert!(matches!(cache.lookup(&key, now), Lookup::Hit { .. }));

        // ...and one millisecond past it, nothing is.
        let observed_at = now - config().max_staleness() - Duration::from_millis(1);
        authority.ready(1, observed_at);
        assert_eq!(
            cache.lookup(&key, now),
            Lookup::Miss(MissReason::AuthorityStale {
                age: now.duration_since(observed_at).unwrap()
            })
        );
        assert!(cache.fill_cursor(now).is_err());
    }

    #[test]
    fn an_entry_older_than_the_age_bound_never_serves() {
        let cache = cache();
        let filled_at = at(LATER_MS);
        let key = chunk_key();
        let fill = cache.fill_cursor(filled_at).unwrap();
        cache.insert(key.clone(), fill, payload());

        // The age is counted from the fill's capture, not from the insertion.
        let bound = config().max_age();
        assert!(matches!(
            cache.lookup(&key, filled_at + bound),
            Lookup::Hit { .. }
        ));
        let now = filled_at + bound + Duration::from_millis(1);
        assert_eq!(
            cache.lookup(&key, now),
            Lookup::Miss(MissReason::Expired {
                age: bound + Duration::from_millis(1)
            })
        );
    }

    #[test]
    fn a_slow_fill_does_not_overwrite_a_newer_entry() {
        let authority = FakeAuthority::default();
        authority.ready(1, at(LATER_MS));
        let cache = authority_cache(&authority);
        let now = at(LATER_MS);
        let key = chunk_key();

        // A fill captured at cursor 1 is still running.
        let slow = cache.fill_cursor(now).unwrap();

        // The authority moves on, and a fill at the new cursor lands first.
        authority.ready(2, now);
        let fast = cache.fill_cursor(now).unwrap();
        let newer = payload();
        assert_eq!(
            cache.insert(key.clone(), fast, newer.clone()),
            Insert::Stored
        );

        // The slow fill's payload is older than what is stored, so it is dropped.
        assert_eq!(cache.insert(key.clone(), slow, payload()), Insert::Rejected);

        let Lookup::Hit {
            payload: hit,
            cursor,
            ..
        } = cache.lookup(&key, now)
        else {
            panic!("the newer entry is served");
        };
        assert_eq!(cursor, 2);
        assert!(Arc::ptr_eq(&hit, &newer));
    }

    #[test]
    fn the_fill_cursor_is_recorded_not_the_current_one() {
        let authority = FakeAuthority::default();
        authority.ready(1, at(LATER_MS));
        let cache = authority_cache(&authority);
        let now = at(LATER_MS);
        let key = chunk_key();

        let fill = cache.fill_cursor(now).unwrap();
        // The authority advances while the fill reads storage.
        authority.ready(9, now);
        assert_eq!(cache.insert(key.clone(), fill, payload()), Insert::Stored);

        assert!(matches!(
            cache.lookup(&key, now),
            Lookup::Miss(MissReason::CursorChanged {
                cached: 1,
                authority: 9
            })
        ));
    }

    #[test]
    fn a_hit_reports_the_annotations_of_the_read_it_serves() {
        let cache = cache();
        let now = at(LATER_MS);
        let key = chunk_key();
        let fill = cache.fill_cursor(now).unwrap();

        // A read that recorded nothing is served with nothing.
        assert_eq!(cache.insert(key.clone(), fill, payload()), Insert::Stored);
        let Lookup::Hit { annotations, .. } = cache.lookup(&key, now) else {
            panic!("the chunk is resident");
        };
        assert!(annotations.is_empty());

        // A read that recorded a warning and an info is served with them.
        let recorded = CachedAnnotations::new(
            vec!["b-warning".to_string(), "a-warning".to_string()],
            vec!["an-info".to_string()],
        );
        assert_eq!(
            cache.insert_annotated(key.clone(), fill, payload(), Arc::new(recorded.clone())),
            Insert::Stored
        );
        let Lookup::Hit { annotations, .. } = cache.lookup(&key, now) else {
            panic!("the chunk is resident");
        };
        assert_eq!(&recorded, annotations.as_ref());
        assert_eq!(&recorded.warnings(), &annotations.warnings());
        // The messages are ordered the way a request reports them.
        assert_eq!(
            vec!["a-warning".to_string(), "b-warning".to_string()],
            annotations.warnings()
        );
    }

    #[test]
    fn the_byte_budget_charges_the_annotations() {
        let now = at(LATER_MS);

        // The rows alone are charged the fixed overhead and nothing more.
        let plain = cache();
        let fill = plain.fill_cursor(now).unwrap();
        plain.insert(chunk_key(), fill, payload());
        plain.entries.run_pending_tasks();
        assert_eq!(ENTRY_OVERHEAD_BYTES, plain.entries.weighted_size());

        // A chunk whose read recorded messages is charged their bytes as well.
        let long = "w".repeat(4096);
        let annotated = cache();
        let fill = annotated.fill_cursor(now).unwrap();
        annotated.insert_annotated(
            chunk_key(),
            fill,
            payload(),
            Arc::new(CachedAnnotations::new(
                vec![long.clone()],
                vec!["i".to_string()],
            )),
        );
        annotated.entries.run_pending_tasks();
        assert_eq!(
            ENTRY_OVERHEAD_BYTES + long.len() as u64 + 1,
            annotated.entries.weighted_size()
        );
    }

    #[test]
    fn the_byte_budget_bounds_the_entries() {
        // Two entries fit: each empty payload costs the per-entry overhead.
        let cache = RollupCache::new(
            config_with(ENTRY_OVERHEAD_BYTES * 2, Duration::ZERO, CHUNK),
            None,
        );
        let now = at(LATER_MS);
        let fill = cache.fill_cursor(now).unwrap();

        for index in 0..4 {
            let key = TimeChunkKey::new(
                identity(),
                TimeChunk::new(HEAD_MS + index * CHUNK_MS, HEAD_MS + (index + 1) * CHUNK_MS),
            );
            cache.insert(key, fill, payload());
        }
        cache.entries.run_pending_tasks();

        // moka's admission policy picks the victim, so which chunks survive is not
        // asserted: the budget is.
        assert!(cache.entries.weighted_size() <= ENTRY_OVERHEAD_BYTES * 2);
        assert!(cache.entries.entry_count() <= 2);
    }

    #[test]
    fn a_miss_does_not_wait_for_an_in_flight_fill() {
        let cache = cache();
        let now = at(LATER_MS);
        let key = chunk_key();

        // One caller's fill is still running: it holds no lock and nothing else
        // can observe it.
        let in_flight = cache.fill_cursor(now).unwrap();

        // A second caller misses immediately rather than waiting for it, fills,
        // and inserts first.
        assert!(matches!(
            cache.lookup(&key, now),
            Lookup::Miss(MissReason::NotFound)
        ));
        assert_eq!(
            cache.insert(key.clone(), in_flight, payload()),
            Insert::Stored
        );
        assert!(matches!(cache.lookup(&key, now), Lookup::Hit { .. }));
    }

    #[test]
    fn concurrent_fills_do_not_block_each_other() {
        let cache = Arc::new(cache());
        let key = chunk_key();
        let start = Arc::new(Barrier::new(2));

        let fills = (0..2)
            .map(|_| {
                let cache = Arc::clone(&cache);
                let key = key.clone();
                let start = Arc::clone(&start);
                std::thread::spawn(move || {
                    let now = at(LATER_MS);
                    let fill = cache.fill_cursor(now).expect("no authority always fills");
                    // Both fills are in flight at once: neither has inserted.
                    start.wait();
                    assert!(matches!(
                        cache.lookup(&key, now),
                        Lookup::Miss(MissReason::NotFound) | Lookup::Hit { .. }
                    ));
                    cache.insert(key, fill, payload())
                })
            })
            .collect::<Vec<_>>();

        for fill in fills {
            assert_ne!(
                fill.join().expect("the fill thread panicked"),
                Insert::Bypassed
            );
        }
        assert!(matches!(
            cache.lookup(&key, at(LATER_MS)),
            Lookup::Hit { .. }
        ));
    }

    #[test]
    fn an_empty_chunk_is_bypassed() {
        let cache = cache();
        let key = TimeChunkKey::new(identity(), TimeChunk::new(HEAD_MS, HEAD_MS));
        let now = at(LATER_MS);

        assert_eq!(
            cache.lookup(&key, now),
            Lookup::Bypass(BypassReason::EmptyChunk)
        );
        let fill = cache.fill_cursor(now).unwrap();
        assert_eq!(cache.insert(key, fill, payload()), Insert::Bypassed);
    }

    #[test]
    fn a_dirty_range_removes_the_chunks_whose_input_it_overlaps() {
        let cache = cache();
        let now = at(LATER_MS);
        let fill = cache.fill_cursor(now).unwrap();
        for index in -1..=2 {
            assert_eq!(
                cache.insert(chunk_key_at(index), fill, payload()),
                Insert::Stored
            );
        }
        assert_eq!(cache.entry_count(), 4);

        // `[10:00, 10:10)` covers the first two chunks, and neither the chunk that
        // ends where it starts nor the chunk that starts where it ends is half-open
        // overlapping it.
        let dirty = TimeChunk::new(HEAD_MS, HEAD_MS + 2 * CHUNK_MS);
        assert_eq!(cache.invalidate_range(&identity(), dirty), 2);

        for index in [0, 1] {
            assert_eq!(
                cache.lookup(&chunk_key_at(index), now),
                Lookup::Miss(MissReason::NotFound),
                "chunk {index} overlaps the dirty range"
            );
        }
        for index in [-1, 2] {
            assert!(
                matches!(cache.lookup(&chunk_key_at(index), now), Lookup::Hit { .. }),
                "chunk {index} is disjoint from the dirty range"
            );
        }
        assert_eq!(cache.entry_count(), 2);
    }

    #[test]
    fn a_lookback_dependency_invalidates_a_chunk_whose_output_the_dirty_range_misses() {
        let cache = cache();
        let now = at(LATER_MS);
        let fill = cache.fill_cursor(now).unwrap();

        // Chunk `[10:00, 10:05)` was materialized from ten minutes of history, so
        // its input dependency reaches back to `09:50` even though its output starts
        // at `10:00`.
        let dependency = TimeChunk::new(HEAD_MS - 2 * CHUNK_MS, HEAD_MS + CHUNK_MS);
        assert_eq!(
            cache.insert_with_dependency(
                chunk_key_at(0),
                fill,
                payload(),
                Arc::default(),
                dependency,
            ),
            Insert::Stored
        );
        // Its neighbours record only their own chunk as their dependency.
        for index in [-1, 1] {
            assert_eq!(
                cache.insert(chunk_key_at(index), fill, payload()),
                Insert::Stored
            );
        }

        // The dirty range is `[09:51, 09:54)`: earlier than every cached output
        // point, but inside the history the middle chunk folded.
        let dirty = TimeChunk::new(HEAD_MS - 9 * 60_000, HEAD_MS - 6 * 60_000);
        assert_eq!(cache.invalidate_range(&identity(), dirty), 1);

        // The chunk whose window read the dirty range is gone, although its own
        // output starts six minutes after it...
        assert_eq!(
            cache.lookup(&chunk_key_at(0), now),
            Lookup::Miss(MissReason::NotFound)
        );
        // ...and the neighbours, whose dependency is only their own span and cannot
        // have read it, stay resident.
        for index in [-1, 1] {
            assert!(
                matches!(cache.lookup(&chunk_key_at(index), now), Lookup::Hit { .. }),
                "chunk {index} does not depend on the dirty range"
            );
        }
    }

    #[test]
    fn an_invalidation_leaves_the_same_chunk_of_another_identity_resident() {
        let cache = cache();
        let now = at(LATER_MS);
        let fill = cache.fill_cursor(now).unwrap();
        let mut other = identity();
        other.query = "sum(rate(http_requests_total[5m]))".to_string();
        let key = chunk_key();
        let other_key = TimeChunkKey::new(other.clone(), key.chunk());

        assert_eq!(cache.insert(key.clone(), fill, payload()), Insert::Stored);
        assert_eq!(
            cache.insert(other_key.clone(), fill, payload()),
            Insert::Stored
        );

        // The dirty range is exactly the chunk both identities hold.
        assert_eq!(cache.invalidate_range(&identity(), key.chunk()), 1);

        assert_eq!(cache.lookup(&key, now), Lookup::Miss(MissReason::NotFound));
        assert!(matches!(cache.lookup(&other_key, now), Lookup::Hit { .. }));

        // The other identity is invalidatable on its own.
        assert_eq!(cache.invalidate_range(&other, key.chunk()), 1);
        assert_eq!(
            cache.lookup(&other_key, now),
            Lookup::Miss(MissReason::NotFound)
        );
    }

    #[test]
    fn table_invalidation_removes_overlapping_dependencies_across_identities() {
        let cache = cache();
        let now = at(LATER_MS);
        let fill = cache.fill_cursor(now).unwrap();
        let table_a = QueryIdentity {
            query: "rate(table_a[5m])".to_string(),
            source_table: "table_a".to_string(),
            ..identity()
        };
        let another_a = QueryIdentity {
            query: "sum(rate(table_a[10m]))".to_string(),
            step_ms: STEP_MS * 2,
            ..table_a.clone()
        };
        let other_schema_a = QueryIdentity {
            schema: "another_schema".to_string(),
            ..table_a.clone()
        };
        let table_b = QueryIdentity {
            query: "rate(table_b[5m])".to_string(),
            source_table: "table_b".to_string(),
            ..identity()
        };
        let first = TimeChunkKey::new(table_a.clone(), TimeChunk::new(HEAD_MS, HEAD_MS + CHUNK_MS));
        let shifted = TimeChunkKey::new(
            another_a,
            TimeChunk::new(HEAD_MS + CHUNK_MS, HEAD_MS + 2 * CHUNK_MS),
        );
        let other_schema = TimeChunkKey::new(other_schema_a, first.chunk());
        let other_table = TimeChunkKey::new(table_b, first.chunk());
        let disjoint = TimeChunkKey::new(
            table_a,
            TimeChunk::new(HEAD_MS + 3 * CHUNK_MS, HEAD_MS + 4 * CHUNK_MS),
        );
        let dirty = TimeChunk::new(HEAD_MS - CHUNK_MS, HEAD_MS - CHUNK_MS + 1);

        // All but `disjoint` read the dirty history, which is before their output.
        for key in [&first, &shifted, &other_schema, &other_table] {
            let dependency = TimeChunk::new(dirty.start_ms(), key.chunk().end_ms());
            assert_eq!(
                cache.insert_with_dependency(
                    key.clone(),
                    fill,
                    payload(),
                    Arc::default(),
                    dependency,
                ),
                Insert::Stored
            );
        }
        assert_eq!(
            cache.insert(disjoint.clone(), fill, payload()),
            Insert::Stored
        );
        assert_eq!(cache.entry_count(), 5);

        assert_eq!(cache.invalidate_table_range("table_a", dirty), 3);
        assert_eq!(cache.invalidation_epoch(), 1);
        for key in [&first, &shifted, &other_schema] {
            assert_eq!(cache.lookup(key, now), Lookup::Miss(MissReason::NotFound));
        }
        for key in [&other_table, &disjoint] {
            assert!(matches!(cache.lookup(key, now), Lookup::Hit { .. }));
        }
    }

    #[test]
    fn whole_table_invalidation_removes_every_span_of_that_table() {
        let cache = cache();
        let now = at(LATER_MS);
        let fill = cache.fill_cursor(now).unwrap();
        let table_a = QueryIdentity {
            query: "rate(table_a[5m])".to_string(),
            source_table: "table_a".to_string(),
            ..identity()
        };
        let table_b = QueryIdentity {
            query: "rate(table_b[5m])".to_string(),
            source_table: "table_b".to_string(),
            ..identity()
        };
        let first = TimeChunkKey::new(table_a.clone(), TimeChunk::new(HEAD_MS, HEAD_MS + CHUNK_MS));
        let far = TimeChunkKey::new(
            table_a,
            TimeChunk::new(HEAD_MS + 10 * CHUNK_MS, HEAD_MS + 11 * CHUNK_MS),
        );
        let other = TimeChunkKey::new(table_b, first.chunk());
        for key in [&first, &far, &other] {
            assert_eq!(cache.insert(key.clone(), fill, payload()), Insert::Stored);
        }

        // A sequence advance without a timestamp hull may mean an erased row, so
        // every span of the table goes, not only the one a guessed range covers.
        assert_eq!(cache.invalidate_table("table_a"), 2);
        for key in [&first, &far] {
            assert_eq!(cache.lookup(key, now), Lookup::Miss(MissReason::NotFound));
        }
        assert!(matches!(cache.lookup(&other, now), Lookup::Hit { .. }));
        assert_eq!(cache.entry_count(), 1);
    }

    #[test]
    fn source_probe_validation_is_opt_in() {
        assert!(!cache().source_probe_enabled());
        assert!(
            RollupCache::new(config(), None)
                .with_source_probe()
                .source_probe_enabled()
        );
    }

    #[test]
    fn a_source_epoch_change_rejects_a_fill_planned_before_it() {
        let cache = cache();
        let now = at(LATER_MS);
        let fill = cache.fill_cursor(now).unwrap();
        assert_eq!(fill.source_epoch, cache.source_epoch());

        let next = cache.advance_source_epoch();
        assert_eq!(next, fill.source_epoch + 1);
        assert_eq!(cache.insert(chunk_key(), fill, payload()), Insert::Rejected);
    }

    #[test]
    fn table_invalidation_advances_epoch_even_when_nothing_is_removed() {
        let cache = cache();
        let now = at(LATER_MS);
        let key = chunk_key();
        let before = cache.fill_cursor(now).unwrap();
        assert_eq!(before.epoch(), 0);

        assert_eq!(cache.invalidate_table_range("missing", key.chunk()), 0);
        assert_eq!(cache.invalidation_epoch(), 1);
        // The epoch is global: even an outstanding fill for a different table
        // must be discarded after a table invalidation.
        assert_eq!(
            cache.insert(key.clone(), before, payload()),
            Insert::Rejected
        );

        let fresh = cache.fill_cursor(now).unwrap();
        assert_eq!(fresh.epoch(), 1);
        assert_eq!(cache.insert(key.clone(), fresh, payload()), Insert::Stored);
        let table = &key.identity().source_table;
        for span in [
            TimeChunk::new(HEAD_MS - 1, HEAD_MS),
            TimeChunk::new(HEAD_MS + CHUNK_MS, HEAD_MS + CHUNK_MS + 1),
            TimeChunk::new(HEAD_MS, HEAD_MS),
            TimeChunk::new(HEAD_MS + 1, HEAD_MS),
        ] {
            let epoch = cache.invalidation_epoch();
            assert_eq!(cache.invalidate_table_range(table, span), 0);
            assert_eq!(cache.invalidation_epoch(), epoch + 1);
            assert!(matches!(cache.lookup(&key, now), Lookup::Hit { .. }));
        }
        let epoch = cache.invalidation_epoch();
        assert_eq!(cache.invalidate_table_range(table, key.chunk()), 1);
        assert_eq!(cache.invalidation_epoch(), epoch + 1);
        assert_eq!(cache.lookup(&key, now), Lookup::Miss(MissReason::NotFound));
        assert_eq!(cache.invalidate_table_range(table, key.chunk()), 0);
        assert_eq!(cache.invalidation_epoch(), epoch + 2);
    }

    #[test]
    fn a_fill_that_started_before_an_invalidation_cannot_land() {
        let cache = cache();
        let now = at(LATER_MS);
        let key = chunk_key();

        // The fill captures its epoch, then reads storage.
        let outstanding = cache.fill_cursor(now).unwrap();

        // The probe invalidates while that read is still running. The scan takes no
        // lock and waits for nothing, so it completes against an empty cache.
        assert_eq!(cache.invalidate_range(&identity(), key.chunk()), 0);

        // The fill's read predates the invalidation, so its payload is dropped even
        // though its key and cursor are current.
        assert_eq!(
            cache.insert(key.clone(), outstanding, payload()),
            Insert::Rejected
        );
        assert_eq!(cache.lookup(&key, now), Lookup::Miss(MissReason::NotFound));

        // A fill that captured the epoch after the invalidation lands normally.
        let fresh = cache.fill_cursor(now).unwrap();
        assert_eq!(cache.insert(key.clone(), fresh, payload()), Insert::Stored);
        assert!(matches!(cache.lookup(&key, now), Lookup::Hit { .. }));
    }

    #[test]
    fn every_invalidation_advances_the_epoch_exactly_once() {
        let cache = cache();
        let now = at(LATER_MS);
        assert_eq!(cache.invalidation_epoch(), 0);

        let before = cache.fill_cursor(now).unwrap();
        assert_eq!(before.epoch(), 0);

        // Nothing is resident: a removal count of zero still advances the epoch.
        assert_eq!(cache.invalidate_range(&identity(), chunk_key().chunk()), 0);
        assert_eq!(cache.invalidation_epoch(), 1);
        assert_eq!(
            cache.insert(chunk_key(), before, payload()),
            Insert::Rejected
        );

        let after = cache.fill_cursor(now).unwrap();
        assert_eq!(after.epoch(), 1);
        assert_eq!(cache.insert(chunk_key(), after, payload()), Insert::Stored);

        // A dirty range that overlaps nothing removes nothing, advances the epoch
        // once, and leaves the entry resident.
        let disjoint = TimeChunk::new(HEAD_MS + 10 * CHUNK_MS, HEAD_MS + 11 * CHUNK_MS);
        assert_eq!(cache.invalidate_range(&identity(), disjoint), 0);
        assert_eq!(cache.invalidation_epoch(), 2);
        assert!(matches!(
            cache.lookup(&chunk_key(), now),
            Lookup::Hit { .. }
        ));

        // An invalidation of another identity removes nothing here either.
        let mut other = identity();
        other.alias = Some("other".to_string());
        let fill = cache.fill_cursor(now).unwrap();
        assert_eq!(fill.epoch(), 2);
        assert_eq!(cache.insert(chunk_key(), fill, payload()), Insert::Stored);
        assert_eq!(cache.invalidate_range(&other, chunk_key().chunk()), 0);
        assert_eq!(cache.invalidation_epoch(), 3);
        assert!(matches!(
            cache.lookup(&chunk_key(), now),
            Lookup::Hit { .. }
        ));
    }

    #[test]
    fn an_invalidation_rejects_a_fill_held_by_another_thread() {
        let cache = Arc::new(cache());
        let now = at(LATER_MS);
        let key = chunk_key();
        let captured = Arc::new(Barrier::new(2));
        let invalidated = Arc::new(Barrier::new(2));

        // A fill captures its epoch and is still reading storage when the probe
        // invalidates: it holds nothing the invalidation needs.
        let filler = {
            let cache = Arc::clone(&cache);
            let key = key.clone();
            let captured = Arc::clone(&captured);
            let invalidated = Arc::clone(&invalidated);
            std::thread::spawn(move || {
                let fill = cache.fill_cursor(now).unwrap();
                captured.wait();
                invalidated.wait();
                cache.insert(key, fill, payload())
            })
        };

        captured.wait();
        assert_eq!(cache.invalidate_range(&identity(), key.chunk()), 0);
        invalidated.wait();

        assert_eq!(
            filler.join().expect("the fill thread panicked"),
            Insert::Rejected
        );
        assert_eq!(cache.lookup(&key, now), Lookup::Miss(MissReason::NotFound));
    }

    #[test]
    fn configuration_validation_rejects_disabled_bounds() {
        let positive = Duration::from_secs(1);
        assert_eq!(
            RollupCacheConfig::new(0, Duration::ZERO, positive, positive, positive).unwrap_err(),
            ConfigError::Bytes
        );
        assert_eq!(
            RollupCacheConfig::new(1, positive, Duration::ZERO, positive, positive).unwrap_err(),
            ConfigError::Chunk
        );
        assert_eq!(
            RollupCacheConfig::new(1, positive, positive, Duration::ZERO, positive).unwrap_err(),
            ConfigError::MaxStaleness
        );
        assert_eq!(
            RollupCacheConfig::new(1, positive, positive, positive, Duration::ZERO).unwrap_err(),
            ConfigError::MaxAge
        );

        let valid = RollupCacheConfig::new(64, Duration::ZERO, positive, positive, positive)
            .expect("a zero tail is valid");
        assert_eq!(valid.max_bytes(), 64);
        assert_eq!(valid.tail(), Duration::ZERO);
        assert_eq!(valid.chunk(), positive);
        assert_eq!(valid.max_staleness(), positive);
        assert_eq!(valid.max_age(), positive);
    }
}
