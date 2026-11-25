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

//! Frontend-local, read-through cache of whole PromQL range-query chunks.
//!
//! The cache holds the materialized result of canonical spans of an evaluation
//! grid ([`TimeChunk`]), keyed by the request identity that makes a result
//! reusable ([`QueryIdentity`]) and never by the window that happened to request
//! it ([`QuerySpan`]). A dashboard sliding from `[10:00, 11:00)` to
//! `[10:05, 11:05)` therefore keeps every interior chunk.
//!
//! # Read-through
//!
//! One request does all of it: it plans its statement into chunks and direct
//! spans ([`RollupCache::plan`]), asks for each chunk ([`RollupCache::lookup`]),
//! executes the chunks that miss, and stores what it read
//! ([`RollupCache::insert`]). Nothing else runs for the cache: there is no
//! background round, no source probe, no per-table state, and no watermark. The
//! cache is a bounded map from canonical chunk keys to payloads, and each entry
//! carries the instant it was filled.
//!
//! # Freshness
//!
//! An entry is served while it is younger than [`RollupCacheConfig::ttl`], and it
//! stops being served at that age whether or not anything mentioned it in
//! between. That is the whole freshness rule, and it makes the cache a
//! *bounded-staleness* one: a write to a chunk's input reaches a served answer
//! within `ttl`, without the frontend having to observe that write at all. The
//! price is the mirror image of the same rule: a row written late — out of order,
//! or past the tail a request already read — can stay invisible for up to `ttl`.
//! Proving freshness per read instead would mean probing the source on the read
//! path, which this cache deliberately never does.
//!
//! # Canonical chunks
//!
//! Chunk bounds are quantized to absolute time, not to the request: with a grid
//! phase `p = start_ms.rem_euclid(step_ms)` and a chunk of `c` milliseconds, chunk
//! `k` covers `[p + k*c, p + (k+1)*c)`, where `k` is the floor of
//! `(t - p) / c`. Every bound is a grid point of every request with the same step
//! and phase, and two requests with different phases can never produce the same
//! bound.
//!
//! The configured `chunk` is a *target* rather than the length used, because it is
//! global while the step belongs to the query: planning aligns it down to a whole
//! number of the query's steps, and at least one step, so `c` is always a multiple
//! of the step and no step is refused. `c` is therefore step-dependent, but for one
//! query step it is one fixed length, so the canonical bounds of a sliding window do
//! not move.
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
//! end is older than the cutoff therefore caches its whole range, and a request
//! that asks for "the last hour" reads its tail directly on every execution.
//!
//! # Memory
//!
//! The cache is bounded by bytes, not entries: each entry is charged its payload's
//! buffer memory, the bytes of its annotations, and a fixed per-entry overhead, and
//! moka evicts by that weight once the budget is spent. Eviction is therefore
//! bounded but not a strict recency ordering, and a payload that is still being
//! read is not charged until it is inserted.
//!
//! # Concurrency
//!
//! A lookup is local: it reads at most one entry, never storage, and never waits.
//! There is no single-flight, so a miss while another request is filling the same
//! chunk proceeds; the later insertion simply replaces the earlier one. Everything
//! here is synchronous and no lock is taken, so a caller may await its storage read
//! without holding anything.

use std::fmt;
use std::sync::Arc;
#[cfg(any(test, feature = "testing"))]
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime};

use common_recordbatch::RecordBatches;
use moka::sync::Cache;
use promql_parser::parser::EvalStmt;
use query::promql::label_values::signed_millis_since_epoch;
use query::promql::window::{EvalGrid, WindowError, is_windowable};
use table::metadata::TableId;

/// The placeholder a [`QueryIdentity`] prints instead of its query text.
const REDACTED_QUERY: &str = "<redacted>";

/// What every entry is charged beyond the bytes of its payload.
///
/// The key and the entry itself are heap-allocated too; without a fixed charge an
/// empty result would weigh nothing and the cache could hold unbounded keys.
const ENTRY_OVERHEAD_BYTES: u64 = 256;

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
    /// The expression's named source table, without catalog or schema.
    pub source_table: String,
    /// The id of the table [`QueryIdentity::source_table`] resolved to.
    ///
    /// A name alone does not identify the data: a table dropped and created again
    /// under the same name is a different table with different rows, so a chunk
    /// keyed on the name would answer one table's points from another's rows. The
    /// id is also what an invalidation names ([`CacheIdent::TableId`]), so it is
    /// what [`RollupCache::clear_table`] selects entries by.
    ///
    /// [`CacheIdent::TableId`]: common_meta::instruction::CacheIdent::TableId
    pub table_id: TableId,
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
            .field("table_id", &self.table_id)
            .field("step_ms", &self.step_ms)
            .field("lookback_ms", &self.lookback_ms)
            .field("route", &self.route)
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

    /// The first requested grid point.
    ///
    /// Only the tests read the coverage back; a caller consumes the spans planned
    /// from it.
    #[cfg(test)]
    pub fn start_ms(&self) -> i64 {
        self.start_ms
    }

    /// The last requested grid point.
    ///
    /// Only the tests read the coverage back; a caller consumes the spans planned
    /// from it.
    #[cfg(test)]
    pub fn end_ms(&self) -> i64 {
        self.end_ms
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
    ///
    /// A caller executes the span either way, so only a test reads the reason back.
    #[cfg(test)]
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
/// The chunks and the direct spans partition the requested coverage exactly once
/// each, in increasing order of time: the direct spans are disjoint, and together
/// with the chunks they leave neither a gap nor an overlap.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChunkSpans {
    span: QuerySpan,
    chunks: Vec<TimeChunk>,
    direct: Vec<DirectSpan>,
    tail: Option<TimeChunk>,
}

impl ChunkSpans {
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
/// - chunk bounds are canonical: the configured `chunk` is a target that is
///   aligned down to a whole number of the statement's steps, at least one, so
///   with phase `p = start_ms.rem_euclid(step_ms)` and the aligned chunk length
///   `c` a bound is `p + k * c`;
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
    let chunk_in_steps = chunk_steps(chunk, grid.step_ms())?;
    // The aligned chunk, not the configured target: `chunk_in_steps` whole steps.
    let chunk_ms = i64::try_from(chunk_in_steps)
        .ok()
        .and_then(|steps| steps.checked_mul(grid.step_ms()))
        .ok_or(BypassReason::Overflow)?;
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

/// The configured target chunk as a whole number of steps of `step_ms`.
///
/// The target is global while the step belongs to the query, so it is rounded
/// *down* to a whole number of steps — never to zero — and the result is the
/// chunk planning uses. Every step therefore plans: no query bypasses merely
/// because the target is not a multiple of its step. For one step the result is
/// one fixed length, so the canonical bounds of a sliding window do not move.
///
/// Only an arithmetic that cannot be represented fails, with
/// [`BypassReason::Overflow`]; `step_ms` is positive by construction
/// ([`EvalGrid`] rejects anything else).
fn chunk_steps(chunk: Duration, step_ms: i64) -> Result<u64, BypassReason> {
    let chunk_ms = i64::try_from(chunk.as_millis()).map_err(|_| BypassReason::Overflow)?;
    let steps = (chunk_ms / step_ms).max(1);
    u64::try_from(steps).map_err(|_| BypassReason::Overflow)
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
    /// The entry is older than [`RollupCacheConfig::ttl`], so it may be stale.
    ///
    /// It is a miss on age, not a sign that anything was observed to change: an
    /// entry reaches it simply by having been filled `ttl` ago.
    Expired,
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
    /// The chunk is empty, so nothing was written.
    Bypassed,
}

/// Why a [`RollupCacheConfig`] is unusable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigError {
    /// The cache must be allowed some bytes.
    Bytes,
    /// A chunk must cover at least one millisecond.
    Chunk,
    /// An entry must live at least one millisecond.
    Ttl,
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Bytes => f.write_str("the cache byte budget must be positive"),
            Self::Chunk => f.write_str("the chunk duration must be positive"),
            Self::Ttl => f.write_str("the ttl must be positive"),
        }
    }
}

impl std::error::Error for ConfigError {}

/// The bounds a [`RollupCache`] enforces.
///
/// `tail` is a [`Duration`], so it is non-negative by construction and needs no
/// check; `max_bytes`, `chunk`, and `ttl` all have to be positive.
///
/// `chunk` cannot be resolved into a chunk length here, because the length is the
/// target aligned down to the query's steps and the step belongs to the query, not
/// the cache: planning aligns it, so the cache never bypasses a query merely because
/// the target is not a multiple of its step.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RollupCacheConfig {
    max_bytes: u64,
    tail: Duration,
    chunk: Duration,
    ttl: Duration,
}

impl RollupCacheConfig {
    /// The validated configuration of a cache.
    pub fn new(
        max_bytes: u64,
        tail: Duration,
        chunk: Duration,
        ttl: Duration,
    ) -> Result<Self, ConfigError> {
        if max_bytes == 0 {
            return Err(ConfigError::Bytes);
        }
        if chunk.is_zero() {
            return Err(ConfigError::Chunk);
        }
        if ttl.is_zero() {
            return Err(ConfigError::Ttl);
        }
        Ok(Self {
            max_bytes,
            tail,
            chunk,
            ttl,
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

    /// The target span of the grid one chunk covers.
    ///
    /// It is a target, not the length used: planning aligns it down to a whole
    /// number of the statement's steps, at least one, so a query at a coarser step
    /// than this target gets one step per chunk.
    pub fn chunk(&self) -> Duration {
        self.chunk
    }

    /// How long an entry is served, counted from the instant it was filled.
    ///
    /// It is the cache's whole freshness rule: no probe of the source decides that
    /// an entry is still current, so this age alone bounds how stale an answer may
    /// be.
    pub fn ttl(&self) -> Duration {
        self.ttl
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
    ///
    /// Only the tests read it back; a caller consumes the chunks and the direct
    /// spans a plan returns.
    #[cfg(test)]
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
    ///
    /// Only the tests read it back; a caller consumes the chunks and the direct
    /// spans a plan returns.
    #[cfg(test)]
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

/// One cached chunk: the complete materialized result and the read that made it.
#[derive(Debug, Clone)]
struct Entry {
    payload: Arc<RecordBatches>,
    /// What the read that produced `payload` recorded, served with it.
    annotations: Arc<CachedAnnotations>,
    /// The bytes this entry is charged: its payload, its annotations, and a fixed
    /// overhead.
    weight: u32,
    /// When the read behind the payload was inserted, which its age is counted
    /// from.
    ///
    /// moka drops an entry at [`RollupCacheConfig::ttl`] on its own clock; this
    /// field is what lets [`RollupCache::lookup`] make that same decision against
    /// the wall clock the caller passes, so an age miss is reported as
    /// [`MissReason::Expired`] rather than as an absence.
    filled_at: SystemTime,
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

/// A bounded, frontend-local, read-through cache of whole PromQL chunks.
///
/// The cache is read-through: [`RollupCache::lookup`] answers from memory or says
/// why it cannot, and the caller executes storage and calls [`RollupCache::insert`].
/// Entries leave for three reasons only — they reach
/// [`RollupCacheConfig::ttl`], moka evicts them once the byte budget is spent, or a
/// table invalidation drops the entries of one table ([`RollupCache::clear_table`]) —
/// so nothing on the request path has to observe the source at all. The last one
/// narrows what a served entry may be stale about: an entry older than `ttl` is
/// never served, and one that a received invalidation named is not served either.
pub struct RollupCache {
    config: RollupCacheConfig,
    entries: Cache<TimeChunkKey, Entry>,
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
            .time_to_live(config.ttl())
            .weigher(|_key: &TimeChunkKey, entry: &Entry| entry.weight)
            .build();
        Self {
            config,
            entries,
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
    /// The gauges report it after a fill and after a reset, so a request path never
    /// pays for it: a lookup reads at most one entry and never walks the map. Tests
    /// also state with it that a request filled exactly the chunks it read, or that it
    /// filled nothing because it never took the cached path.
    pub fn entry_count(&self) -> u64 {
        self.entries.run_pending_tasks();
        self.entries.entry_count()
    }

    /// The bytes the resident entries are charged, summed over their weights once
    /// pending maintenance has run.
    ///
    /// Like [`Self::entry_count`], it is read when the gauges are updated (after a
    /// fill and after a reset), and never on a lookup, which stays one map read.
    pub fn byte_count(&self) -> u64 {
        self.entries.run_pending_tasks();
        self.entries
            .iter()
            .map(|(_, entry)| u64::from(entry.weight))
            .sum()
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

    /// Empties the cache, dropping every entry of every request.
    ///
    /// This is the local reset used by the internal reset endpoint; it does not
    /// touch source data or other frontends.
    pub fn clear(&self) {
        self.entries.invalidate_all();
        self.entries.run_pending_tasks();
    }

    /// Drops every entry keyed on `table_id`, and nothing else.
    ///
    /// This is the local half of an invalidation: the frontend that receives a
    /// [`CacheIdent::TableId`] clears its own chunks of that table and no others,
    /// so a write to one table never costs the entries of its neighbours. The
    /// entries of the other tables are neither aged nor evicted, and an entry that
    /// is already gone is not an error — invalidation is only ever a hint to stop
    /// serving, never a fact a request may fail on.
    ///
    /// [`CacheIdent::TableId`]: common_meta::instruction::CacheIdent::TableId
    pub fn clear_table(&self, table_id: TableId) {
        // The cache has no per-table index, so the entries are walked once. An
        // invalidation is rare and bounded by the byte budget, while a lookup on the
        // request path never pays for this.
        let keys = self
            .entries
            .iter()
            .filter(|(key, _)| key.identity.table_id == table_id)
            .map(|(key, _)| TimeChunkKey::clone(key.as_ref()))
            .collect::<Vec<_>>();
        for key in keys {
            self.entries.invalidate(&key);
        }
        self.entries.run_pending_tasks();
    }

    /// Plans `stmt` of `identity` into chunk keys and direct spans as of `now`.
    ///
    /// The returned direct spans are the caller's to send to storage; their points
    /// are never cached.
    ///
    /// The step is the only thing the identity and the statement must agree on: the
    /// configured chunk is a target aligned down to the statement's steps, so a
    /// target that is not a multiple of this statement's step never bypasses here.
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
    /// This is local and infallible: it reads at most one entry, never storage, and
    /// never waits. An entry younger than [`RollupCacheConfig::ttl`] is served as it
    /// is, without asking the source whether it is still current — that question is
    /// what the age answers, and it is the only thing that answers it.
    pub fn lookup(&self, key: &TimeChunkKey, now: SystemTime) -> Lookup {
        if key.chunk().is_empty() {
            return Lookup::Bypass(BypassReason::EmptyChunk);
        }
        let Some(entry) = self.entries.get(key) else {
            return Lookup::Miss(MissReason::NotFound);
        };
        // A clock that stepped backwards leaves a non-negative age of zero: the entry
        // is younger than the clock thinks, not older.
        let age = now.duration_since(entry.filled_at).unwrap_or_default();
        if age > self.config.ttl() {
            return Lookup::Miss(MissReason::Expired);
        }
        // The entry is served: nothing else in this cache counts as a hit.
        #[cfg(any(test, feature = "testing"))]
        self.hits.fetch_add(1, Ordering::SeqCst);
        Lookup::Hit {
            payload: entry.payload,
            annotations: entry.annotations,
        }
    }

    /// Stores what a read of `key` produced.
    ///
    /// A non-empty chunk is stored unconditionally: the cache has no notion of a
    /// read being un-publishable, so the caller's insert is the whole decision. An
    /// empty chunk is not stored, since nothing was ever materialized for it
    /// ([`BypassReason::EmptyChunk`]); [`RollupCache::lookup`] refuses it for the
    /// same reason.
    pub fn insert(
        &self,
        key: &TimeChunkKey,
        payload: Arc<RecordBatches>,
        annotations: Arc<CachedAnnotations>,
    ) -> Insert {
        if key.chunk().is_empty() {
            return Insert::Bypassed;
        }
        let entry = Entry {
            weight: Entry::weigh(&payload, &annotations),
            payload,
            annotations,
            filled_at: SystemTime::now(),
        };
        self.entries.insert(key.clone(), entry);
        Insert::Stored
    }
}

#[cfg(test)]
mod tests {
    use std::time::UNIX_EPOCH;

    use promql_parser::parser::parse;

    use super::*;

    /// One minute, the step every clock-aligned test uses.
    const STEP_MS: i64 = 60_000;
    /// Five minutes: the chunk target the tests configure.
    const CHUNK_MS: i64 = 300_000;
    /// The five-minute chunk every clock-aligned test plans with.
    const CHUNK: Duration = Duration::from_millis(CHUNK_MS as u64);
    /// A five-minute-aligned instant, standing in for 10:00.
    const HEAD_MS: i64 = 36_000_000;
    /// A wall clock far enough past every test window that no point is in its tail.
    const LATER_MS: i64 = HEAD_MS + 24 * 3_600_000;
    /// The ttl every test configures.
    const TTL: Duration = Duration::from_secs(3_600);
    /// The id the table every test names resolves to.
    const TABLE_ID: TableId = 1024;
    /// The id a table created under a name another table was dropped from gets.
    const RECREATED_TABLE_ID: TableId = 2048;

    fn at(ms: i64) -> SystemTime {
        let duration = Duration::from_millis(ms.unsigned_abs());
        if ms >= 0 {
            UNIX_EPOCH + duration
        } else {
            UNIX_EPOCH - duration
        }
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
            table_id: TABLE_ID,
            step_ms: STEP_MS,
            lookback_ms: 300_000,
            route: QueryRoute::Legacy,
            alias: None,
            read_preference: None,
        }
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
        config_targeting(CHUNK)
    }

    /// The test configuration with `chunk` as its target chunk duration.
    fn config_targeting(chunk: Duration) -> RollupCacheConfig {
        RollupCacheConfig::new(1 << 20, Duration::ZERO, chunk, TTL)
            .expect("the test configuration is valid")
    }

    fn cache() -> RollupCache {
        RollupCache::new(config())
    }

    /// A cache whose target chunk is `chunk`, everything else as [`config`] has it.
    fn cache_targeting(chunk: Duration) -> RollupCache {
        RollupCache::new(config_targeting(chunk))
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
        assert!(
            direct
                .iter()
                .any(|span| span.reason() == DirectReason::Tail),
            "the points at or after the cutoff are direct: {direct:?}"
        );
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
    fn a_target_chunk_is_aligned_down_to_whole_steps() {
        // 90 seconds against a 60-second step is one step, not one and a half: the
        // query plans instead of bypassing, and every chunk is exactly one step long.
        let cache = cache_targeting(Duration::from_secs(90));
        let stmt = stmt_from("rate(http_requests_total[5m])", HEAD_MS, 10, STEP_MS);
        let (span, chunks, direct, _) = planned(&cache, &stmt, at(LATER_MS));
        assert_partition(span, &chunks, &direct);
        assert!(direct.is_empty());
        assert_eq!(chunks.len(), 10);
        assert!(
            chunks
                .iter()
                .all(|chunk| chunk.end_ms() - chunk.start_ms() == STEP_MS),
            "every chunk is one step: {chunks:?}"
        );
        assert_eq!(
            chunks.first(),
            Some(&TimeChunk::new(HEAD_MS, HEAD_MS + STEP_MS))
        );

        // A sliding window therefore keeps the same canonical bounds: one step later,
        // the interior chunks are the very same spans.
        let slid = stmt_from(
            "rate(http_requests_total[5m])",
            HEAD_MS + STEP_MS,
            10,
            STEP_MS,
        );
        let (_, slid_chunks, _, _) = planned(&cache, &slid, at(LATER_MS));
        assert_eq!(&chunks[1..], &slid_chunks[..chunks.len() - 1]);

        // A 5m target against a 2m step is two steps, so 4m chunks.
        let two_min = 2 * STEP_MS;
        let cache = cache_targeting(CHUNK);
        let stmt = stmt_from("rate(http_requests_total[5m])", HEAD_MS, 10, two_min);
        let (span, chunks, direct, _) = planned_for(
            QueryIdentity {
                step_ms: two_min,
                ..identity()
            },
            &cache,
            &stmt,
            at(LATER_MS),
        );
        assert_partition(span, &chunks, &direct);
        assert!(direct.is_empty());
        assert_eq!(
            chunks,
            vec![
                TimeChunk::new(HEAD_MS, HEAD_MS + 4 * 60_000),
                TimeChunk::new(HEAD_MS + 4 * 60_000, HEAD_MS + 8 * 60_000),
                TimeChunk::new(HEAD_MS + 8 * 60_000, HEAD_MS + 12 * 60_000),
                TimeChunk::new(HEAD_MS + 12 * 60_000, HEAD_MS + 16 * 60_000),
                TimeChunk::new(HEAD_MS + 16 * 60_000, HEAD_MS + 20 * 60_000),
            ]
        );
    }

    #[test]
    fn a_target_coarser_than_the_step_chunks_one_step() {
        // A 1m target against a 5m step cannot hold even one step, so planning keeps
        // the one-step floor rather than an empty chunk.
        let cache = cache_targeting(Duration::from_secs(60));
        let stmt = stmt_from("rate(http_requests_total[5m])", HEAD_MS, 4, CHUNK_MS);
        let (span, chunks, direct, _) = planned_for(
            QueryIdentity {
                step_ms: CHUNK_MS,
                ..identity()
            },
            &cache,
            &stmt,
            at(LATER_MS),
        );
        assert_partition(span, &chunks, &direct);
        assert_eq!(
            chunks,
            vec![
                TimeChunk::new(HEAD_MS, HEAD_MS + CHUNK_MS),
                TimeChunk::new(HEAD_MS + CHUNK_MS, HEAD_MS + 2 * CHUNK_MS),
                TimeChunk::new(HEAD_MS + 2 * CHUNK_MS, HEAD_MS + 3 * CHUNK_MS),
                TimeChunk::new(HEAD_MS + 3 * CHUNK_MS, HEAD_MS + 4 * CHUNK_MS),
            ]
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
        assert!(
            printed.contains(&TABLE_ID.to_string()),
            "the table id is what invalidation names, so it is printed: {printed}"
        );
    }

    #[test]
    fn a_chunk_is_never_served_to_another_table_of_the_same_name() {
        let cache = cache();
        let key = chunk_key_at(0);
        assert_eq!(
            cache.insert(&key, payload(), Arc::default()),
            Insert::Stored
        );

        // Dropping the table and creating it again under the same name leaves the
        // request otherwise identical and changes only the id the name resolves to.
        let recreated = TimeChunkKey::new(
            QueryIdentity {
                table_id: RECREATED_TABLE_ID,
                ..identity()
            },
            key.chunk(),
        );
        assert_eq!(
            cache.lookup(&recreated, SystemTime::now()),
            Lookup::Miss(MissReason::NotFound)
        );
        assert_eq!(
            cache.entry_count(),
            1,
            "the chunk of the table that still exists is untouched"
        );
    }

    #[test]
    fn clearing_a_table_removes_only_its_chunks() {
        let cache = cache();
        let first = chunk_key_at(0);
        let second = chunk_key_at(1);
        let other_table = TimeChunkKey::new(
            QueryIdentity {
                table_id: RECREATED_TABLE_ID,
                ..identity()
            },
            first.chunk(),
        );
        for key in [&first, &second, &other_table] {
            assert_eq!(cache.insert(key, payload(), Arc::default()), Insert::Stored);
        }
        assert_eq!(cache.entry_count(), 3);

        cache.clear_table(TABLE_ID);

        assert_eq!(cache.entry_count(), 1);
        for key in [&first, &second] {
            assert_eq!(
                cache.lookup(key, SystemTime::now()),
                Lookup::Miss(MissReason::NotFound)
            );
        }
        assert!(
            matches!(
                cache.lookup(&other_table, SystemTime::now()),
                Lookup::Hit { .. }
            ),
            "another table's chunk is served"
        );
        assert_eq!(cache.hit_count(), 1);
    }

    #[test]
    fn a_missed_chunk_is_filled_and_then_hit() {
        let cache = cache();
        let key = chunk_key_at(0);
        assert_eq!(
            cache.lookup(&key, SystemTime::now()),
            Lookup::Miss(MissReason::NotFound)
        );
        assert_eq!(
            cache.insert(&key, payload(), Arc::default()),
            Insert::Stored
        );
        assert!(matches!(
            cache.lookup(&key, SystemTime::now()),
            Lookup::Hit { .. }
        ));
        assert_eq!(cache.hit_count(), 1);
        assert_eq!(cache.entry_count(), 1);
    }

    #[test]
    fn a_hit_reports_the_annotations_of_the_read_it_serves() {
        let cache = cache();
        let key = chunk_key_at(0);
        let annotations = Arc::new(CachedAnnotations::new(
            vec!["a warning".to_string()],
            vec!["an info".to_string()],
        ));
        assert_eq!(
            cache.insert(&key, payload(), annotations.clone()),
            Insert::Stored
        );
        let Lookup::Hit {
            annotations: served,
            ..
        } = cache.lookup(&key, SystemTime::now())
        else {
            panic!("the chunk should be resident");
        };
        assert_eq!(served, annotations);
    }

    #[test]
    fn an_entry_older_than_the_ttl_never_serves() {
        let cache = cache();
        let key = chunk_key_at(0);
        // The entry is filled somewhere between these two readings of the clock, so a
        // lookup at the first one plus the ttl is never more than a ttl old and one at
        // the second one plus the ttl is always older.
        let before = SystemTime::now();
        assert_eq!(
            cache.insert(&key, payload(), Arc::default()),
            Insert::Stored
        );
        let after = SystemTime::now();
        let ttl = cache.config().ttl();

        // Exactly the ttl later the entry is still served: the bound is "older than",
        // not "at least".
        assert!(matches!(
            cache.lookup(&key, before + ttl),
            Lookup::Hit { .. }
        ));
        // One millisecond past it, the entry is stale even though nothing else
        // happened: the age alone decides.
        assert_eq!(
            cache.lookup(&key, after + ttl + Duration::from_millis(1)),
            Lookup::Miss(MissReason::Expired)
        );
        assert_eq!(
            cache.entry_count(),
            1,
            "an age miss is not an eviction: moka takes the entry out later"
        );
    }

    #[test]
    fn a_cleared_cache_holds_nothing() {
        let cache = cache();
        let key = chunk_key_at(0);
        assert_eq!(
            cache.insert(&key, payload(), Arc::default()),
            Insert::Stored
        );
        assert_eq!(cache.entry_count(), 1);
        cache.clear();
        assert_eq!(cache.entry_count(), 0);
        assert_eq!(
            cache.lookup(&key, SystemTime::now()),
            Lookup::Miss(MissReason::NotFound)
        );
    }

    #[test]
    fn the_byte_budget_bounds_the_entries() {
        // One entry costs the fixed overhead, so a budget of two of them holds two.
        let tiny = RollupCacheConfig::new(ENTRY_OVERHEAD_BYTES * 2, Duration::ZERO, CHUNK, TTL)
            .expect("the test configuration is valid");
        let cache = RollupCache::new(tiny);
        for index in 0..6 {
            let key = chunk_key_at(index);
            cache.insert(&key, payload(), Arc::default());
        }
        assert_eq!(cache.entry_count(), 2);
    }

    #[test]
    fn an_empty_chunk_is_bypassed() {
        let cache = cache();
        let key = TimeChunkKey::new(identity(), TimeChunk::new(HEAD_MS, HEAD_MS));
        assert_eq!(
            cache.lookup(&key, SystemTime::now()),
            Lookup::Bypass(BypassReason::EmptyChunk)
        );
        assert_eq!(
            cache.insert(&key, payload(), Arc::default()),
            Insert::Bypassed
        );
        assert_eq!(cache.entry_count(), 0);
    }

    #[test]
    fn an_entry_of_another_query_or_step_is_another_entry() {
        let cache = cache();
        let key = chunk_key_at(0);
        assert_eq!(
            cache.insert(&key, payload(), Arc::default()),
            Insert::Stored
        );

        // The same span of another query is a different key: a selector names the
        // series it reads, so two queries cannot answer each other's points.
        for other in [
            // Another label filter, so another series set.
            QueryIdentity {
                query: "rate(http_requests_total{code=\"500\"}[5m])".to_string(),
                ..identity()
            },
            // The same expression on another grid.
            QueryIdentity {
                step_ms: 2 * STEP_MS,
                ..identity()
            },
            // The same expression with another lookback.
            QueryIdentity {
                lookback_ms: 900_000,
                ..identity()
            },
            // The same expression renamed by an alias.
            QueryIdentity {
                alias: Some("alias".to_string()),
                ..identity()
            },
            // The same expression read by the other planner.
            QueryIdentity {
                route: QueryRoute::Native,
                ..identity()
            },
            // The same expression read under another read preference.
            QueryIdentity {
                read_preference: Some("read-from-follower".to_string()),
                ..identity()
            },
            // The same expression read by another tenant.
            QueryIdentity {
                tenant: "other".to_string(),
                ..identity()
            },
        ] {
            let other = TimeChunkKey::new(other, key.chunk());
            assert_eq!(
                cache.lookup(&other, SystemTime::now()),
                Lookup::Miss(MissReason::NotFound),
                "a chunk keyed by another identity must not be served"
            );
        }
        // ... while the identity that filled it is served.
        assert!(matches!(
            cache.lookup(&key, SystemTime::now()),
            Lookup::Hit { .. }
        ));
        assert_eq!(cache.entry_count(), 1);
    }

    #[test]
    fn an_entry_of_another_chunk_is_another_entry() {
        let cache = cache();
        assert_eq!(
            cache.insert(&chunk_key_at(0), payload(), Arc::default()),
            Insert::Stored
        );
        // The neighbouring chunk is a key of its own, so a hit cannot answer points
        // it never held.
        assert_eq!(
            cache.lookup(&chunk_key_at(1), SystemTime::now()),
            Lookup::Miss(MissReason::NotFound)
        );
        assert_eq!(
            cache.lookup(&chunk_key_at(-1), SystemTime::now()),
            Lookup::Miss(MissReason::NotFound)
        );
    }

    #[test]
    fn configuration_validation_rejects_disabled_bounds() {
        assert_eq!(
            RollupCacheConfig::new(0, Duration::ZERO, CHUNK, TTL),
            Err(ConfigError::Bytes)
        );
        assert_eq!(
            RollupCacheConfig::new(1 << 20, Duration::ZERO, Duration::ZERO, TTL),
            Err(ConfigError::Chunk)
        );
        assert_eq!(
            RollupCacheConfig::new(1 << 20, Duration::ZERO, CHUNK, Duration::ZERO),
            Err(ConfigError::Ttl)
        );
        // A zero tail is a valid configuration: it caches every point the query asks
        // for, which is what a test that runs at wall-clock time wants.
        assert!(RollupCacheConfig::new(1 << 20, Duration::ZERO, CHUNK, TTL).is_ok());
    }
}
