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
//! The cache holds whole, materialized PromQL results for historical spans of an
//! evaluation grid ([`TimeChunk`]) and keys them by the query that produced them
//! ([`QueryKey`]), so no chunk of one query is ever served to another. An entry
//! remembers the authority cursor its payload was filled at ([`FillCursor`]) and
//! is served only while the [`CacheAuthority`] still vouches for exactly that
//! cursor: `cursor equal -> hit`, `cursor changed -> miss`, and an authority that
//! is unknown, unhealthy, or stale always misses. A cursor that lags the storage
//! sequence is accepted, so a hit may be bounded-stale by design.
//!
//! Three properties are deliberate:
//!
//! - **The recent tail is never cached.** [`plan_chunks`] splits a statement into
//!   complete historical chunks on the original grid plus the trailing span that
//!   is still close enough to the query end that storage may be receiving data for
//!   it. Output points in the tail go straight to storage.
//! - **A lookup is local and cheap.** [`RollupCache::lookup`] reads the authority
//!   status, one cache entry, and nothing else: it never queries storage and never
//!   waits for cache maintenance. There is no single-flight, so a miss while
//!   another fill of the same chunk is in progress proceeds; the competing fills
//!   both reach [`RollupCache::insert`], where the older cursor loses.
//! - **No lock is held across an await.** Everything here is synchronous and the
//!   cache holds no lock across a fill, so a caller must not (and need not) hold a
//!   cache lock while it awaits the storage read that fills a chunk.
//!
//! Chunks cover *output* points. The evaluator still has to read the input history
//! every selected point folds: the first point of a `rate(x[5m])` chunk needs five
//! minutes of samples *before* the chunk start, and widening the chunk does not
//! widen that history.
//!
//! Not implemented here: the storage sequence probe that makes an authority
//! concrete, the promotion of this cache into the query path, and any
//! configuration option.

#![allow(dead_code)] // Wired into the query path by a later milestone.

use std::fmt;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use common_recordbatch::RecordBatches;
use moka::ops::compute::Op;
use moka::sync::Cache;
use promql_parser::parser::EvalStmt;
use query::promql::label_values::signed_millis_since_epoch;
use query::promql::window::{is_windowable, windowed_eval_stmt};

/// The placeholder a [`QueryKey`] prints instead of its query text.
const REDACTED_QUERY: &str = "<redacted>";

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

/// Every field two queries must agree on before one may reuse the other's chunks.
///
/// The key is compared verbatim: nothing is normalized, lowercased, or parsed, so
/// two textually different queries never share an entry even when they would plan
/// to the same result. A missing field would let one query answer another's
/// points, which is why the fields are exhaustive rather than minimal.
///
/// The query text is redacted in [`fmt::Debug`], since a query name and its
/// selectors can carry tenant data; the alias and the read preference are not
/// query text and are printed.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct QueryKey {
    /// The tenant the query runs as.
    pub tenant: String,
    /// The catalog the query reads.
    pub catalog: String,
    /// The schema (database) the query reads.
    pub schema: String,
    /// The query text exactly as the caller wrote it.
    pub query: String,
    /// The optional output alias of a PromQL statement
    /// (`QueryStatement::Promql(_, alias)`).
    ///
    /// The alias renames the result's columns without changing the expression, so
    /// two requests that differ only in it must not share entries.
    pub alias: Option<String>,
    /// The read preference the request ran under, as
    /// `QueryContext::read_preference()` spells it.
    ///
    /// It can change which replica serves the query, so it is part of the key even
    /// while every session accepts the same preference. `None` means the caller has
    /// not threaded one through; the cache never substitutes a default for it.
    pub read_preference: Option<String>,
    /// The first evaluation instant, in milliseconds since the epoch.
    pub start_ms: i64,
    /// The last evaluation instant (inclusive), in milliseconds since the epoch.
    pub end_ms: i64,
    /// The evaluation step, in milliseconds.
    pub step_ms: i64,
    /// The lookback delta planning applies, in milliseconds.
    pub lookback_ms: i64,
    /// The planner that evaluated the query.
    pub route: QueryRoute,
    /// Whether the request asked for ordered output.
    pub ordered: bool,
}

impl fmt::Debug for QueryKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("QueryKey")
            .field("tenant", &self.tenant)
            .field("catalog", &self.catalog)
            .field("schema", &self.schema)
            .field("query", &REDACTED_QUERY)
            .field("alias", &self.alias)
            .field("read_preference", &self.read_preference)
            .field("start_ms", &self.start_ms)
            .field("end_ms", &self.end_ms)
            .field("step_ms", &self.step_ms)
            .field("lookback_ms", &self.lookback_ms)
            .field("route", &self.route)
            .field("ordered", &self.ordered)
            .finish()
    }
}

/// A half-open `[start, end)` span of output grid points, in milliseconds.
///
/// Both bounds come from the evaluation grid of the statement the span was planned
/// from: `start` is a grid point, and `end` is either the next grid point past the
/// last point the span holds or the grid point one step past the statement's last
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

/// The cache key of one materialized chunk.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TimeChunkKey {
    query: QueryKey,
    chunk: TimeChunk,
}

impl TimeChunkKey {
    /// The key of `chunk` of the query identified by `query`.
    pub fn new(query: QueryKey, chunk: TimeChunk) -> Self {
        Self { query, chunk }
    }

    /// The query the chunk belongs to.
    pub fn query(&self) -> &QueryKey {
        &self.query
    }

    /// The span the chunk covers.
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
    /// The statement's span, its step, or the requested durations cannot be
    /// planned into chunks.
    InvalidSpan,
    /// The grid arithmetic, or a chunk bound, cannot be represented.
    Overflow,
    /// The chunk is empty, so nothing was ever materialized for it.
    EmptyChunk,
}

impl fmt::Display for BypassReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotWindowable => f.write_str("the expression is not windowable"),
            Self::InvalidSpan => f.write_str("the statement cannot be split into chunks"),
            Self::Overflow => f.write_str("a chunk bound overflows the time grid"),
            Self::EmptyChunk => f.write_str("the chunk is empty"),
        }
    }
}

impl std::error::Error for BypassReason {}

/// The cacheable and non-cacheable output spans of one statement.
///
/// Concatenated in grid order, [`ChunkSpans::chunks`] and [`ChunkSpans::tail`]
/// cover the statement's whole output grid exactly once. Neither is a claim about
/// the input ranges the expression folds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChunkSpans {
    chunks: Vec<TimeChunk>,
    tail: Option<TimeChunk>,
}

impl ChunkSpans {
    /// The complete historical chunks, in grid order.
    pub fn chunks(&self) -> &[TimeChunk] {
        &self.chunks
    }

    /// The trailing span that must not be cached, when the statement excludes one.
    pub fn tail(&self) -> Option<TimeChunk> {
        self.tail
    }

    /// The chunks and the tail span.
    pub fn into_parts(self) -> (Vec<TimeChunk>, Option<TimeChunk>) {
        (self.chunks, self.tail)
    }
}

/// Plans the cacheable chunks and the excluded tail of `stmt`.
///
/// Chunks are `chunk` long on the statement's own grid, anchored at its first
/// point: a chunk holds the grid points in `[head, head + chunk)`, which is at
/// least one point and never straddles a grid point of the next chunk. The tail is
/// the trailing span whose points are within `tail` of the statement's last point;
/// its points never enter the cache. `tail` of zero therefore caches the whole
/// statement.
///
/// The statement's grid, point count, and point timestamps come from
/// [`query::promql::window::windowed_eval_stmt`], so this does not lay out a grid
/// of its own: a statement planning would refuse is refused here too.
pub fn plan_chunks(
    stmt: &EvalStmt,
    tail: Duration,
    chunk: Duration,
) -> Result<ChunkSpans, BypassReason> {
    if !is_windowable(&stmt.expr) {
        return Err(BypassReason::NotWindowable);
    }
    let points = grid_len(stmt).ok_or(BypassReason::InvalidSpan)?;
    let step_ms = step_ms(stmt)?;
    let last_ms = grid_point_ms(stmt, points - 1).ok_or(BypassReason::InvalidSpan)?;
    let end_ms = last_ms.checked_add(step_ms).ok_or(BypassReason::Overflow)?;

    // The tail holds the points closer to the statement's end than `tail`.
    let tail_ms = i64::try_from(tail.as_millis()).unwrap_or(i64::MAX);
    let tail_start = first_point_after(stmt, points, last_ms.saturating_sub(tail_ms))?;

    let chunk_points = points_per_chunk(stmt, points, chunk)?;
    let mut chunks = Vec::new();
    let mut first = 0_u64;
    while first < tail_start {
        let last = first.saturating_add(chunk_points).min(tail_start);
        let start_ms = grid_point_ms(stmt, first).ok_or(BypassReason::InvalidSpan)?;
        // A chunk that stops inside the grid ends on the next grid point; the one
        // that runs to the end of the grid ends one step past its last point.
        let chunk_end_ms = if last < points {
            grid_point_ms(stmt, last).ok_or(BypassReason::InvalidSpan)?
        } else {
            end_ms
        };
        chunks.push(TimeChunk::new(start_ms, chunk_end_ms));
        first = last;
    }

    let tail = if tail_start < points {
        let start_ms = grid_point_ms(stmt, tail_start).ok_or(BypassReason::InvalidSpan)?;
        Some(TimeChunk::new(start_ms, end_ms))
    } else {
        None
    };

    Ok(ChunkSpans { chunks, tail })
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
}

/// The outcome of one [`RollupCache::lookup`].
#[derive(Debug, PartialEq)]
pub enum Lookup {
    /// The whole materialized chunk, filled at `cursor`.
    Hit {
        /// The complete result of the chunk.
        payload: Arc<RecordBatches>,
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

/// The cursor a fill was planned at, captured before the fill starts.
///
/// An entry records this cursor rather than the one the authority reports when the
/// fill completes: a cursor that moved while the fill ran does not make the payload
/// fresher than the state it actually read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FillCursor {
    cursor: u64,
}

impl FillCursor {
    /// The cursor the fill reads storage at.
    pub fn cursor(&self) -> u64 {
        self.cursor
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
/// deployment without a working authority simply never hits. The deterministic
/// fake in the tests is the only implementation so far; the storage sequence probe
/// that backs this in production is the next milestone.
pub trait CacheAuthority: Send + Sync {
    /// The cursor the authority vouches for, and how fresh that reading is.
    fn status(&self) -> AuthorityStatus;
}

/// Why a [`RollupCacheConfig`] is unusable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigError {
    /// The cache must hold at least one entry.
    Capacity,
    /// A chunk must cover at least one millisecond.
    Chunk,
    /// The staleness bound must accept an authority reading at all.
    MaxStaleness,
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Capacity => f.write_str("the cache capacity must be positive"),
            Self::Chunk => f.write_str("the chunk duration must be positive"),
            Self::MaxStaleness => f.write_str("the max staleness must be positive"),
        }
    }
}

impl std::error::Error for ConfigError {}

/// The bounds a [`RollupCache`] enforces.
///
/// `tail` is a [`Duration`], so it is non-negative by construction and needs no
/// check; `capacity`, `chunk`, and `max_staleness` all have to be positive.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RollupCacheConfig {
    capacity: u64,
    tail: Duration,
    chunk: Duration,
    max_staleness: Duration,
}

impl RollupCacheConfig {
    /// The validated configuration of a cache.
    pub fn new(
        capacity: u64,
        tail: Duration,
        chunk: Duration,
        max_staleness: Duration,
    ) -> Result<Self, ConfigError> {
        if capacity == 0 {
            return Err(ConfigError::Capacity);
        }
        if chunk.is_zero() {
            return Err(ConfigError::Chunk);
        }
        if max_staleness.is_zero() {
            return Err(ConfigError::MaxStaleness);
        }
        Ok(Self {
            capacity,
            tail,
            chunk,
            max_staleness,
        })
    }

    /// How many chunks the cache holds before evicting.
    pub fn capacity(&self) -> u64 {
        self.capacity
    }

    /// How close to the statement's end output points stop being cacheable.
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
}

/// A statement planned into the chunk keys a lookup uses.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlannedChunks {
    chunks: Vec<TimeChunkKey>,
    tail: Option<TimeChunk>,
}

impl PlannedChunks {
    /// The cacheable chunks of the statement, in grid order.
    pub fn chunks(&self) -> &[TimeChunkKey] {
        &self.chunks
    }

    /// The trailing span that must not be cached.
    pub fn tail(&self) -> Option<TimeChunk> {
        self.tail
    }
}

/// The result of planning a statement against a cache.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChunkPlan {
    /// The statement must not use the cache at all.
    Bypass(BypassReason),
    /// The statement's cacheable chunks and its excluded tail.
    Planned(PlannedChunks),
}

/// One cached chunk: the complete materialized result and how it was validated.
#[derive(Debug, Clone)]
struct Entry {
    payload: Arc<RecordBatches>,
    /// The cursor the payload was filled at, never the one seen at insertion.
    cursor: u64,
    /// When the entry was inserted.
    inserted_at: SystemTime,
}

/// A bounded, frontend-local cache of whole PromQL chunks.
///
/// The cache is read-through: [`RollupCache::lookup`] answers from memory or says
/// why it cannot, and the caller executes storage and calls
/// [`RollupCache::insert`]. Entries are evicted in moka's admission order once
/// `capacity` is reached, which is bounded but not a strict recency ordering.
pub struct RollupCache {
    config: RollupCacheConfig,
    authority: Arc<dyn CacheAuthority>,
    entries: Cache<TimeChunkKey, Entry>,
}

impl RollupCache {
    /// A cache bounded by `config` and validated by `authority`.
    pub fn new(config: RollupCacheConfig, authority: Arc<dyn CacheAuthority>) -> Self {
        let entries = Cache::builder().max_capacity(config.capacity()).build();
        Self {
            config,
            authority,
            entries,
        }
    }

    /// The configuration this cache enforces.
    pub fn config(&self) -> &RollupCacheConfig {
        &self.config
    }

    /// Plans `stmt` into the chunk keys of `query`, or reports why it cannot.
    ///
    /// The returned tail span is the caller's to send to storage; its points are
    /// never cached.
    pub fn plan(&self, query: QueryKey, stmt: &EvalStmt) -> ChunkPlan {
        match plan_chunks(stmt, self.config.tail(), self.config.chunk()) {
            Err(reason) => ChunkPlan::Bypass(reason),
            Ok(spans) => {
                let (chunks, tail) = spans.into_parts();
                let chunks = chunks
                    .into_iter()
                    .map(|chunk| TimeChunkKey::new(query.clone(), chunk))
                    .collect();
                ChunkPlan::Planned(PlannedChunks { chunks, tail })
            }
        }
    }

    /// Tries to serve `key` from memory as of `now`.
    ///
    /// This is local and infallible: it reads the authority status and at most one
    /// entry, never storage, and never waits for an in-flight fill or for cache
    /// maintenance.
    pub fn lookup(&self, key: &TimeChunkKey, now: SystemTime) -> Lookup {
        if key.chunk.is_empty() {
            return Lookup::Bypass(BypassReason::EmptyChunk);
        }
        let cursor = match self.authorized_cursor(now) {
            Ok(cursor) => cursor,
            Err(reason) => return Lookup::Miss(reason),
        };
        match self.entries.get(key) {
            None => Lookup::Miss(MissReason::NotFound),
            Some(entry) if entry.cursor == cursor => Lookup::Hit {
                payload: entry.payload,
                cursor,
                inserted_at: entry.inserted_at,
            },
            Some(entry) => Lookup::Miss(MissReason::CursorChanged {
                cached: entry.cursor,
                authority: cursor,
            }),
        }
    }

    /// The cursor a fill of `key` reads storage at, captured before the fill.
    ///
    /// Callers must pass the returned [`FillCursor`] to [`RollupCache::insert`]
    /// instead of reading the authority again once the fill completes.
    pub fn fill_cursor(&self, now: SystemTime) -> Result<FillCursor, MissReason> {
        self.authorized_cursor(now)
            .map(|cursor| FillCursor { cursor })
    }

    /// Stores `payload` as the whole result of `key`'s chunk, filled at `fill`.
    ///
    /// The insertion is conditional: a fill at a cursor older than the entry
    /// already in place is dropped, so a slow fill cannot overwrite a newer one.
    /// A fill at the same cursor replaces the payload, since both read the same
    /// storage state. The entry records the cursor of `fill`, not whichever cursor
    /// the authority reports now.
    pub fn insert(
        &self,
        key: TimeChunkKey,
        fill: FillCursor,
        payload: Arc<RecordBatches>,
    ) -> Insert {
        if key.chunk.is_empty() {
            return Insert::Bypassed;
        }
        let candidate = Entry {
            payload,
            cursor: fill.cursor(),
            inserted_at: SystemTime::now(),
        };
        // `and_compute_with` serializes only the swap of this one key; nothing is
        // locked across the storage read that produced `payload`.
        let result = self
            .entries
            .entry(key)
            .and_compute_with(|existing| match existing {
                Some(entry) if entry.value().cursor > candidate.cursor => Op::Nop,
                _ => Op::Put(candidate),
            });
        match result.into_entry() {
            Some(entry) if entry.value().cursor > fill.cursor() => Insert::Rejected,
            _ => Insert::Stored,
        }
    }

    /// The cursor a lookup at `now` may serve, or why it may serve none.
    fn authorized_cursor(&self, now: SystemTime) -> Result<u64, MissReason> {
        match self.authority.status() {
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

/// The number of points on `stmt`'s evaluation grid.
///
/// TODO(rollup-cache): `query::promql::window` should expose the grid it already
/// computes — its point count and point timestamps — instead of having callers
/// probe for them through single-point windows.
fn grid_len(stmt: &EvalStmt) -> Option<u64> {
    grid_point_ms(stmt, 0)?;
    // Exponential search for an index off the grid, then bisect the last index on
    // it. Both bounds are read from the window helper, not recomputed.
    let mut inside = 0_u64;
    let mut probe = 1_u64;
    while grid_point_ms(stmt, probe).is_some() {
        inside = probe;
        probe = probe.checked_mul(2)?;
    }
    let mut outside = probe;
    while outside - inside > 1 {
        let middle = inside + (outside - inside) / 2;
        if grid_point_ms(stmt, middle).is_some() {
            inside = middle;
        } else {
            outside = middle;
        }
    }
    inside.checked_add(1)
}

/// The timestamp of grid point `index`, in milliseconds since the epoch.
///
/// The window helper owns the grid arithmetic, so a grid point is read as the
/// single-point window `[index, index + 1)` rather than recomputed here.
fn grid_point_ms(stmt: &EvalStmt, index: u64) -> Option<i64> {
    let first = usize::try_from(index).ok()?;
    let last = first.checked_add(1)?;
    let window = windowed_eval_stmt(stmt, first, last).ok()?;
    signed_millis_since_epoch(window.start).ok()
}

/// The positive whole-millisecond step the grid is laid out with.
///
/// The window helper refuses a step that is not a positive whole number of
/// milliseconds, so a statement reaching the grid has one; this only reads it back
/// to name the millisecond one step past the grid's last point.
fn step_ms(stmt: &EvalStmt) -> Result<i64, BypassReason> {
    if !stmt.interval.subsec_nanos().is_multiple_of(1_000_000) {
        return Err(BypassReason::InvalidSpan);
    }
    let millis = i64::try_from(stmt.interval.as_millis()).map_err(|_| BypassReason::Overflow)?;
    if millis <= 0 {
        return Err(BypassReason::InvalidSpan);
    }
    Ok(millis)
}

/// The first grid index whose point is strictly later than `cutoff_ms`, or
/// `points` when no point is.
fn first_point_after(stmt: &EvalStmt, points: u64, cutoff_ms: i64) -> Result<u64, BypassReason> {
    // `points` is one past the last point, so it is later than any cutoff.
    let mut first = 0_u64;
    let mut past = points;
    while first < past {
        let middle = first + (past - first) / 2;
        let middle_ms = grid_point_ms(stmt, middle).ok_or(BypassReason::InvalidSpan)?;
        if middle_ms > cutoff_ms {
            past = middle;
        } else {
            first = middle + 1;
        }
    }
    Ok(first)
}

/// How many grid points one chunk holds: the points in `[head, head + chunk)`.
///
/// A chunk shorter than one step, and any step, still covers the point it starts
/// on; a chunk longer than the whole grid covers every point.
fn points_per_chunk(stmt: &EvalStmt, points: u64, chunk: Duration) -> Result<u64, BypassReason> {
    let head_ms = grid_point_ms(stmt, 0).ok_or(BypassReason::InvalidSpan)?;
    let chunk_ms = i64::try_from(chunk.as_millis()).map_err(|_| BypassReason::Overflow)?;
    let target_ms = head_ms
        .checked_add(chunk_ms)
        .ok_or(BypassReason::Overflow)?;
    // The first point not inside the chunk.
    let mut first = 0_u64;
    let mut past = points;
    while first < past {
        let middle = first + (past - first) / 2;
        let middle_ms = grid_point_ms(stmt, middle).ok_or(BypassReason::InvalidSpan)?;
        if middle_ms < target_ms {
            first = middle + 1;
        } else {
            past = middle;
        }
    }
    Ok(first.max(1))
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Barrier, Mutex};
    use std::time::UNIX_EPOCH;

    use promql_parser::parser::parse;

    use super::*;

    const STEP_MS: u64 = 37_000;
    /// The first point of the test grid: neither an epoch boundary nor a whole
    /// second, so any rounding would show as a shifted chunk.
    const HEAD_MS: u64 = 1_001;

    /// A statement whose grid starts at `start_ms` and steps `step_ms` over
    /// `points` points.
    fn stmt_between(expr: &str, start_ms: u64, points: u64, step_ms: u64) -> EvalStmt {
        let start = UNIX_EPOCH + Duration::from_millis(start_ms);
        let end = start + Duration::from_millis(step_ms * (points - 1));
        EvalStmt {
            expr: parse(expr).expect("the expression parses"),
            start,
            end,
            interval: Duration::from_millis(step_ms),
            lookback_delta: Duration::from_secs(300),
        }
    }

    fn query_key() -> QueryKey {
        QueryKey {
            tenant: "greptime".to_string(),
            catalog: "greptime".to_string(),
            schema: "public".to_string(),
            query: "rate(http_requests_total[5m])".to_string(),
            alias: None,
            read_preference: None,
            start_ms: 1_000,
            end_ms: 61_000,
            step_ms: 15_000,
            lookback_ms: 300_000,
            route: QueryRoute::Legacy,
            ordered: false,
        }
    }

    fn chunk_key() -> TimeChunkKey {
        TimeChunkKey::new(query_key(), TimeChunk::new(1_001, 38_001))
    }

    fn payload() -> Arc<RecordBatches> {
        Arc::new(RecordBatches::empty())
    }

    fn config(capacity: u64) -> RollupCacheConfig {
        RollupCacheConfig::new(
            capacity,
            Duration::from_secs(300),
            Duration::from_secs(120),
            Duration::from_secs(60),
        )
        .expect("the test configuration is valid")
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

    fn cache(capacity: u64, authority: &FakeAuthority) -> RollupCache {
        let authority: Arc<dyn CacheAuthority> = Arc::new(authority.clone());
        RollupCache::new(config(capacity), authority)
    }

    /// A grid of ten 37-second points from 1 001 ms, a tail of two points, and
    /// three-point chunks.
    fn planned() -> (RollupCache, FakeAuthority) {
        let authority = FakeAuthority::default();
        authority.ready(1, SystemTime::now());
        let cache = cache(8, &authority);
        (cache, authority)
    }

    #[test]
    fn tail_is_excluded_and_chunks_follow_the_original_grid() {
        let stmt = stmt_between("rate(x[5m])", HEAD_MS, 10, STEP_MS);
        let spans = plan_chunks(
            &stmt,
            Duration::from_millis(2 * STEP_MS),
            Duration::from_millis(3 * STEP_MS),
        )
        .expect("the statement is windowable");

        let at = |index: u64| HEAD_MS as i64 + index as i64 * STEP_MS as i64;
        assert_eq!(
            spans.chunks(),
            &[
                TimeChunk::new(at(0), at(3)),
                TimeChunk::new(at(3), at(6)),
                // The tail starts at point 8, so the last chunk is short rather
                // than padded into the tail.
                TimeChunk::new(at(6), at(8)),
            ]
        );
        assert_eq!(spans.tail(), Some(TimeChunk::new(at(8), at(10))));

        // Every bound is a grid point of the original statement: none of them is
        // rounded to a wall-clock bucket.
        for chunk in spans.chunks() {
            assert_eq!((chunk.start_ms() - at(0)) % STEP_MS as i64, 0);
            assert!(chunk.start_ms() < chunk.end_ms());
        }
    }

    #[test]
    fn a_zero_tail_caches_the_whole_statement() {
        let stmt = stmt_between("rate(x[5m])", HEAD_MS, 10, STEP_MS);
        let spans = plan_chunks(&stmt, Duration::ZERO, Duration::from_millis(3 * STEP_MS))
            .expect("the statement is windowable");

        assert_eq!(spans.tail(), None);
        let at = |index: u64| HEAD_MS as i64 + index as i64 * STEP_MS as i64;
        assert_eq!(
            spans.chunks(),
            &[
                TimeChunk::new(at(0), at(3)),
                TimeChunk::new(at(3), at(6)),
                TimeChunk::new(at(6), at(9)),
                // One point left over, still a complete chunk of its own.
                TimeChunk::new(at(9), at(10)),
            ]
        );
    }

    #[test]
    fn a_chunk_shorter_than_a_step_still_covers_its_point() {
        let stmt = stmt_between("x", HEAD_MS, 3, STEP_MS);
        let spans = plan_chunks(&stmt, Duration::ZERO, Duration::from_millis(1))
            .expect("the statement is windowable");

        assert_eq!(spans.chunks().len(), 3);
        assert_eq!(
            spans.chunks()[0].end_ms() - spans.chunks()[0].start_ms(),
            STEP_MS as i64
        );
    }

    #[test]
    fn a_full_tail_leaves_nothing_to_cache() {
        let stmt = stmt_between("x", HEAD_MS, 3, STEP_MS);
        let spans = plan_chunks(&stmt, Duration::from_secs(3600), Duration::from_secs(60))
            .expect("the statement is windowable");

        assert!(spans.chunks().is_empty());
        let at = |index: u64| HEAD_MS as i64 + index as i64 * STEP_MS as i64;
        assert_eq!(spans.tail(), Some(TimeChunk::new(at(0), at(3))));
    }

    #[test]
    fn a_non_windowable_expression_bypasses_the_cache() {
        let (cache, _authority) = planned();
        for query in ["sum(rate(x[5m]))", "{host=\"a\"}", "rate(x[5m] offset 1m)"] {
            let stmt = stmt_between(query, HEAD_MS, 10, STEP_MS);
            assert_eq!(
                plan_chunks(&stmt, Duration::ZERO, Duration::from_secs(60)),
                Err(BypassReason::NotWindowable),
                "{query}"
            );
            assert_eq!(
                cache.plan(query_key(), &stmt),
                ChunkPlan::Bypass(BypassReason::NotWindowable),
                "{query}"
            );
        }
    }

    #[test]
    fn an_invalid_span_bypasses_the_cache() {
        let stmt = stmt_between("x", HEAD_MS, 10, STEP_MS);
        let mut backwards = stmt.clone();
        backwards.end = backwards.start - Duration::from_millis(STEP_MS);
        assert_eq!(
            plan_chunks(&backwards, Duration::ZERO, Duration::from_secs(60)),
            Err(BypassReason::InvalidSpan)
        );

        // A step carrying a sub-millisecond part lays out a grid the planners do
        // not evaluate on.
        let mut sub_millisecond = stmt;
        sub_millisecond.interval = Duration::from_micros(1_500);
        assert_eq!(
            plan_chunks(&sub_millisecond, Duration::ZERO, Duration::from_secs(60)),
            Err(BypassReason::InvalidSpan)
        );
    }

    #[test]
    fn query_key_fields_are_isolated() {
        let base = query_key();
        let chunk = TimeChunk::new(1_001, 38_001);
        let cases = [
            QueryKey {
                tenant: "other".to_string(),
                ..base.clone()
            },
            QueryKey {
                catalog: "other".to_string(),
                ..base.clone()
            },
            QueryKey {
                schema: "other".to_string(),
                ..base.clone()
            },
            QueryKey {
                query: "rate(http_requests_total[6m])".to_string(),
                ..base.clone()
            },
            QueryKey {
                alias: Some("q".to_string()),
                ..base.clone()
            },
            QueryKey {
                read_preference: Some("LEADER".to_string()),
                ..base.clone()
            },
            QueryKey {
                start_ms: 2_000,
                ..base.clone()
            },
            QueryKey {
                end_ms: 62_000,
                ..base.clone()
            },
            QueryKey {
                step_ms: 30_000,
                ..base.clone()
            },
            QueryKey {
                lookback_ms: 600_000,
                ..base.clone()
            },
            QueryKey {
                route: QueryRoute::Native,
                ..base.clone()
            },
            QueryKey {
                ordered: true,
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

        // The chunks of the same query still share a key.
        assert_eq!(
            TimeChunkKey::new(base.clone(), chunk),
            TimeChunkKey::new(base.clone(), chunk)
        );
        assert_ne!(
            TimeChunkKey::new(base, chunk),
            TimeChunkKey::new(query_key(), TimeChunk::new(1_001, 75_001))
        );
    }

    #[test]
    fn a_query_key_prints_its_context_but_never_its_query_text() {
        let key = query_key();
        let printed = format!("{key:?}");
        assert!(printed.contains(REDACTED_QUERY), "{printed}");
        assert!(!printed.contains("http_requests_total"), "{printed}");

        let chunk = TimeChunkKey::new(key, TimeChunk::new(1_001, 38_001));
        assert!(!format!("{chunk:?}").contains("http_requests_total"));

        // The alias and the read preference are not query text, so they print.
        let aliased = QueryKey {
            alias: Some("q".to_string()),
            read_preference: Some("LEADER".to_string()),
            ..query_key()
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
    fn tenant_and_schema_isolation_reaches_the_entries() {
        let (cache, _authority) = planned();
        let now = SystemTime::now();
        let key = chunk_key();
        let fill = cache.fill_cursor(now).unwrap();
        assert_eq!(cache.insert(key.clone(), fill, payload()), Insert::Stored);
        assert!(matches!(cache.lookup(&key, now), Lookup::Hit { .. }));

        for other in [
            QueryKey {
                tenant: "other".to_string(),
                ..query_key()
            },
            QueryKey {
                schema: "other".to_string(),
                ..query_key()
            },
        ] {
            let other = TimeChunkKey::new(other, key.chunk());
            assert!(matches!(
                cache.lookup(&other, now),
                Lookup::Miss(MissReason::NotFound)
            ));
        }
    }

    #[test]
    fn an_alias_change_separates_entries() {
        let (cache, _authority) = planned();
        let now = SystemTime::now();
        let key = chunk_key();
        let fill = cache.fill_cursor(now).unwrap();
        assert_eq!(cache.insert(key.clone(), fill, payload()), Insert::Stored);
        assert!(matches!(cache.lookup(&key, now), Lookup::Hit { .. }));

        // The same expression under an output alias is a different result.
        let aliased = TimeChunkKey::new(
            QueryKey {
                alias: Some("q".to_string()),
                ..query_key()
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
        let (cache, _authority) = planned();
        let now = SystemTime::now();
        let key = chunk_key();
        let fill = cache.fill_cursor(now).unwrap();
        assert_eq!(cache.insert(key.clone(), fill, payload()), Insert::Stored);
        assert!(matches!(cache.lookup(&key, now), Lookup::Hit { .. }));

        // A different read preference may have been served by another replica.
        let other = TimeChunkKey::new(
            QueryKey {
                read_preference: Some("LEADER".to_string()),
                ..query_key()
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
        let (cache, _authority) = planned();
        let now = SystemTime::now();
        let key = chunk_key();

        assert!(matches!(
            cache.lookup(&key, now),
            Lookup::Miss(MissReason::NotFound)
        ));
        let fill = cache.fill_cursor(now).expect("the authority is ready");
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
        assert_eq!(cursor, 1);
        assert!(Arc::ptr_eq(&hit, &filled));
    }

    #[test]
    fn only_the_authority_cursor_is_served() {
        let (cache, authority) = planned();
        let now = SystemTime::now();
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

        // A cursor that moved back is just another change.
        authority.ready(0, now);
        assert!(matches!(
            cache.lookup(&key, now),
            Lookup::Miss(MissReason::CursorChanged {
                cached: 1,
                authority: 0
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
        let (cache, authority) = planned();
        let now = SystemTime::now();
        let key = chunk_key();
        let fill = cache.fill_cursor(now).unwrap();
        cache.insert(key.clone(), fill, payload());

        // Exactly at the bound the reading is still trusted...
        authority.ready(1, now - config(8).max_staleness());
        assert!(matches!(cache.lookup(&key, now), Lookup::Hit { .. }));

        // ...and one millisecond past it, nothing is.
        let observed_at = now - config(8).max_staleness() - Duration::from_millis(1);
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
    fn a_slow_fill_does_not_overwrite_a_newer_entry() {
        let (cache, authority) = planned();
        let now = SystemTime::now();
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
        let (cache, authority) = planned();
        let now = SystemTime::now();
        let key = chunk_key();

        let fill = cache.fill_cursor(now).unwrap();
        // The authority advances while the fill reads storage.
        authority.ready(9, now);
        assert_eq!(cache.insert(key.clone(), fill, payload()), Insert::Stored);

        // The entry is still the cursor of its fill, so it misses the new cursor
        // rather than claiming to be as fresh as it is not.
        assert!(matches!(
            cache.lookup(&key, now),
            Lookup::Miss(MissReason::CursorChanged {
                cached: 1,
                authority: 9
            })
        ));
    }

    #[test]
    fn capacity_bounds_the_entries() {
        let (cache, _authority) = planned_capacity(2);
        let now = SystemTime::now();
        let fill = cache.fill_cursor(now).unwrap();

        for start in [1_001, 38_001, 75_001] {
            let key = TimeChunkKey::new(query_key(), TimeChunk::new(start, start + STEP_MS as i64));
            cache.insert(key, fill, payload());
        }
        cache.entries.run_pending_tasks();

        // moka's admission policy picks the victim, so which chunks survive is not
        // asserted: the bound is.
        assert!(cache.entries.entry_count() <= 2);
        assert!(cache.entries.entry_count() > 0);
    }

    #[test]
    fn a_single_entry_cache_keeps_one_entry() {
        let (cache, _authority) = planned_capacity(1);
        let now = SystemTime::now();
        let fill = cache.fill_cursor(now).unwrap();

        for start in [1_001, 38_001] {
            let key = TimeChunkKey::new(query_key(), TimeChunk::new(start, start + STEP_MS as i64));
            cache.insert(key, fill, payload());
            cache.entries.run_pending_tasks();
            assert_eq!(cache.entries.entry_count(), 1);
        }
    }

    /// A cache of `capacity` entries whose authority is ready at cursor 1.
    fn planned_capacity(capacity: u64) -> (RollupCache, FakeAuthority) {
        let authority = FakeAuthority::default();
        authority.ready(1, SystemTime::now());
        let cache = cache(capacity, &authority);
        (cache, authority)
    }

    #[test]
    fn a_miss_does_not_wait_for_an_in_flight_fill() {
        let (cache, _authority) = planned();
        let now = SystemTime::now();
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
        let first = payload();
        assert_eq!(cache.insert(key.clone(), in_flight, first), Insert::Stored);

        // The in-flight fill still finishes; because it filled at the same cursor
        // it replaces the payload instead of being dropped. Nothing serialized the
        // two fills.
        assert!(matches!(cache.lookup(&key, now), Lookup::Hit { .. }));
    }

    #[test]
    fn concurrent_fills_do_not_block_each_other() {
        let (cache, _authority) = planned();
        let cache = Arc::new(cache);
        let key = chunk_key();
        let start = Arc::new(Barrier::new(2));

        let fills = (0..2)
            .map(|_| {
                let cache = Arc::clone(&cache);
                let key = key.clone();
                let start = Arc::clone(&start);
                std::thread::spawn(move || {
                    let now = SystemTime::now();
                    let fill = cache.fill_cursor(now).expect("the authority is ready");
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
            cache.lookup(&key, SystemTime::now()),
            Lookup::Hit { .. }
        ));
    }

    #[test]
    fn an_empty_chunk_is_bypassed() {
        let (cache, _authority) = planned();
        let key = TimeChunkKey::new(query_key(), TimeChunk::new(1_001, 1_001));
        let now = SystemTime::now();

        assert_eq!(
            cache.lookup(&key, now),
            Lookup::Bypass(BypassReason::EmptyChunk)
        );
        let fill = cache.fill_cursor(now).unwrap();
        assert_eq!(cache.insert(key, fill, payload()), Insert::Bypassed);
    }

    #[test]
    fn a_planned_statement_ends_before_its_tail() {
        let (cache, _authority) = planned();
        let stmt = stmt_between("rate(x[5m])", HEAD_MS, 10, STEP_MS);
        let ChunkPlan::Planned(planned) = cache.plan(query_key(), &stmt) else {
            panic!("the statement is windowable");
        };

        // The plan's chunks stop where the tail starts and every one of them is
        // keyed by the same query.
        let tail = planned.tail().expect("the statement excludes a tail");
        let last_chunk_end = planned
            .chunks()
            .iter()
            .map(|key| key.chunk().end_ms())
            .max()
            .expect("the statement has cacheable chunks");
        assert_eq!(last_chunk_end, tail.start_ms());
        for key in planned.chunks() {
            assert_eq!(key.query(), &query_key());
        }
    }

    #[test]
    fn configuration_validation_rejects_disabled_bounds() {
        let positive = Duration::from_secs(1);
        assert_eq!(
            RollupCacheConfig::new(0, Duration::ZERO, positive, positive).unwrap_err(),
            ConfigError::Capacity
        );
        assert_eq!(
            RollupCacheConfig::new(1, positive, Duration::ZERO, positive).unwrap_err(),
            ConfigError::Chunk
        );
        assert_eq!(
            RollupCacheConfig::new(1, positive, positive, Duration::ZERO).unwrap_err(),
            ConfigError::MaxStaleness
        );

        let valid = RollupCacheConfig::new(1, Duration::ZERO, positive, positive)
            .expect("a zero tail is valid");
        assert_eq!(valid.capacity(), 1);
        assert_eq!(valid.tail(), Duration::ZERO);
        assert_eq!(valid.chunk(), positive);
        assert_eq!(valid.max_staleness(), positive);
    }
}
