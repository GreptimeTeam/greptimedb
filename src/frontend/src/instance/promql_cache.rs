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

//! The cache-aware execution of one PromQL range query.
//!
//! A range query is one request over many output points, and the rollup cache of
//! [`crate::query_cache`] holds whole canonical chunks of that output. This module
//! turns one statement into the plan of [`RollupCache::plan`], serves every chunk
//! it can from memory, and executes only what the plan says must be read:
//!
//! - a **hit** reuses the materialized payload and never reaches the executor, so
//!   the chunk is neither planned nor executed a second time;
//! - a **miss** executes the chunk's own subwindow ([`windowed_eval_stmt`]), fully
//!   consumes it, and then inserts it;
//! - a **direct span** (the partial chunks at either end of the request and the
//!   whole tail) executes its subwindow and is never inserted, because a partial
//!   chunk must not be keyed as a full one.
//!
//! That is the whole protocol: a hit serves, a miss fills, and no request waits
//! for another one's fill. Whether a served chunk is still current is the entry's
//! age (`ttl` of [`crate::query_cache::RollupCacheConfig`]), not anything this
//! module measures.
//!
//! The spans partition the requested coverage exactly once each, in increasing
//! time, so concatenating their rows keeps the result's order and neither
//! duplicates nor loses a point. Every span of one statement shares its schema, so
//! a merged result that disagrees is reported rather than returned.
//!
//! # Annotations
//!
//! A PromQL evaluation can record warnings and infos (a range function folding
//! float samples together with native histograms is what records them). A chunk
//! served from memory is not evaluated, so it cannot record anything; its messages
//! are therefore stored beside its rows and served with them ([`CachedAnnotations`]).
//! The caller replays every message of the merged answer into the request's
//! collector, which deduplicates, so a request reports a message once whether the
//! chunk that recorded it was read now or earlier.
//!
//! A statement the cache refuses, one with nothing cacheable in its coverage, and
//! one whose spans disagree on their schema is returned as
//! [`RangeOutcome::Bypassed`], so the caller can run its original plan unchanged.
//!
//! Nothing here knows about permissions, interceptors, cancellation, or the
//! process list: the caller keeps all of them around this call, so they behave as
//! they do on the original path.
//!
//! [`windowed_eval_stmt`]: query::promql::window::windowed_eval_stmt

use std::sync::Arc;
use std::time::SystemTime;

use async_trait::async_trait;
use common_error::ext::BoxedError;
use common_recordbatch::{RecordBatch, RecordBatches};
use datatypes::schema::{Schema, SchemaRef};
use promql_parser::parser::EvalStmt;
use query::promql::window::{EvalGrid, windowed_eval_stmt};
use servers::error::{self as server_error, ExecuteQuerySnafu, UnexpectedResultSnafu};
use snafu::ResultExt;

use crate::query_cache::{
    CachedAnnotations, ChunkPlan, DirectSpan, Lookup, QueryIdentity, RollupCache, TimeChunk,
    TimeChunkKey,
};

/// Executes one PromQL evaluation subwindow.
///
/// A window is a whole statement of its own: the caller plans and executes it
/// through the same route selection, cancellation, and slow-query machinery a
/// statement built from the request would get, and hands back a fully consumed
/// result. Returning a stream would let a fill insert something it did not read to
/// the end, which is why the trait cannot express one.
#[async_trait]
pub(crate) trait WindowExecutor: Send + Sync {
    /// Plans and executes `stmt`, returning every row it produced and what the read
    /// recorded.
    async fn execute_window(&self, stmt: EvalStmt) -> server_error::Result<WindowOutput>;
}

/// What one evaluation window produced.
#[derive(Debug)]
pub(crate) struct WindowOutput {
    /// Every row the window read.
    pub rows: RecordBatches,
    /// The PromQL annotations the window's read recorded.
    pub annotations: CachedAnnotations,
}

/// What a cache-aware range execution produced.
#[derive(Debug)]
pub(crate) enum RangeOutcome {
    /// The merged answer of every span of the statement.
    Merged {
        /// The rows of every span, in time order.
        rows: RecordBatches,
        /// Everything the answering spans recorded, to report with the rows.
        ///
        /// The annotations of a served chunk come from the read that filled it, and
        /// those of an executed span from that execution, so a request reports what
        /// its answer was produced by whether the chunk was read now or earlier.
        annotations: CachedAnnotations,
    },
    /// The cache cannot serve this request; the caller must run its original plan.
    ///
    /// Nothing was executed: a bypass is decided by planning alone, before the
    /// first span reaches the executor.
    Bypassed,
}

/// Executes `stmt` as of wall-clock `now`, serving its chunks from `cache` and
/// executing only the spans the plan says must be read.
///
/// Wall-clock `now` is what places the tail — points at or after `now - tail` may
/// still be receiving data, so they are executed directly and never cached — and it
/// is also the instant every lookup measures an entry's age against.
pub(crate) async fn execute_range<E>(
    executor: &E,
    cache: &RollupCache,
    identity: QueryIdentity,
    stmt: &EvalStmt,
    now: SystemTime,
) -> server_error::Result<RangeOutcome>
where
    E: WindowExecutor + ?Sized,
{
    let planned = match cache.plan(identity, stmt, now) {
        ChunkPlan::Bypass(_) => return Ok(RangeOutcome::Bypassed),
        ChunkPlan::Planned(planned) => planned,
    };
    if planned.chunks().is_empty() {
        // Everything would be read directly: splitting it would change nothing.
        return Ok(RangeOutcome::Bypassed);
    }
    let grid = EvalGrid::of(stmt).map_err(|error| {
        UnexpectedResultSnafu {
            reason: format!("the statement planned into chunks has no evaluation grid: {error}"),
        }
        .build()
    })?;

    let mut merged = MergedRows::default();
    let mut warnings = Vec::new();
    let mut infos = Vec::new();
    for span in ascending_spans(planned.chunks(), planned.direct()) {
        let window = match span {
            Span::Chunk(key) => chunk_rows(executor, cache, key, stmt, &grid, now).await?,
            Span::Direct(direct) => execute_span(executor, stmt, &grid, direct.span()).await?,
        };
        warnings.extend(window.annotations.warnings().iter().cloned());
        infos.extend(window.annotations.infos().iter().cloned());
        if !merged.push(window.rows) {
            // The spans disagree on their schema, so they are not one result. Do not
            // report a merge no plan produced: let the caller run its original plan.
            return Ok(RangeOutcome::Bypassed);
        }
    }
    merged.finish(CachedAnnotations::new(warnings, infos))
}

/// The rows of one cacheable `key`, served from memory when it is resident and
/// filled otherwise.
async fn chunk_rows<E>(
    executor: &E,
    cache: &RollupCache,
    key: &TimeChunkKey,
    stmt: &EvalStmt,
    grid: &EvalGrid,
    now: SystemTime,
) -> server_error::Result<WindowOutput>
where
    E: WindowExecutor + ?Sized,
{
    if let Lookup::Hit {
        payload,
        annotations,
    } = cache.lookup(key, now)
    {
        // The rows and the messages of one read are served together, so a hit reports
        // what the read it replaces reported.
        return Ok(WindowOutput {
            rows: clone_rows(&payload)?,
            annotations: (*annotations).clone(),
        });
    }
    let window = execute_span(executor, stmt, grid, key.chunk()).await?;
    // Only a fully consumed chunk is inserted: `execute_span` returns rows it read to
    // the end, and a failure returned above without inserting anything. Its
    // annotations are stored beside them, and the same ones are returned, so the
    // request that filled the chunk reports them exactly as a later hit does.
    cache.insert(
        key,
        Arc::new(clone_rows(&window.rows)?),
        Arc::new(window.annotations.clone()),
    );
    Ok(window)
}

/// A copy of `rows`, which the cache hands out by shared reference.
///
/// The batches themselves are reference counted, so a copy shares their buffers; only
/// the list of batches is new, which is what keeps a cached payload immutable while a
/// result of its own is merged with the spans around it.
fn clone_rows(rows: &RecordBatches) -> server_error::Result<RecordBatches> {
    RecordBatches::try_new(rows.schema(), rows.iter().cloned().collect())
        .map_err(BoxedError::new)
        .context(ExecuteQuerySnafu)
}

/// Executes the subwindow covering `span`, which the plan cut from `stmt`'s grid.
async fn execute_span<E>(
    executor: &E,
    stmt: &EvalStmt,
    grid: &EvalGrid,
    span: TimeChunk,
) -> server_error::Result<WindowOutput>
where
    E: WindowExecutor + ?Sized,
{
    let first = grid_index(grid, span.start_ms())?;
    let last = grid_index(grid, span.end_ms())?;
    let window = windowed_eval_stmt(stmt, first, last).map_err(|error| {
        UnexpectedResultSnafu {
            reason: format!("the planned span {span:?} is not an evaluation window: {error}"),
        }
        .build()
    })?;
    executor.execute_window(window).await
}

/// The index of the grid point at `ms`.
///
/// Planning only cuts spans on grid points, so a bound that is not one is an
/// internal inconsistency rather than a caller error.
fn grid_index(grid: &EvalGrid, ms: i64) -> server_error::Result<usize> {
    let offset = i128::from(ms) - i128::from(grid.start_ms());
    let step = i128::from(grid.step_ms());
    if offset < 0 || offset % step != 0 {
        return Err(UnexpectedResultSnafu {
            reason: format!("{ms} is not a point of the evaluation grid"),
        }
        .build());
    }
    usize::try_from(offset / step).map_err(|error| {
        UnexpectedResultSnafu {
            reason: format!("the evaluation grid index of {ms} overflows: {error}"),
        }
        .build()
    })
}

/// One span of the statement, in the order the result must hold its rows.
#[derive(Debug, Clone, Copy)]
enum Span<'a> {
    /// A cacheable chunk: served from memory or filled.
    Chunk(&'a TimeChunkKey),
    /// A span that must be read and must not be cached.
    Direct(&'a DirectSpan),
}

/// The chunks and direct spans of a plan, interleaved in increasing time.
///
/// The two lists partition the coverage, and each is already in time order, so
/// this is the merge that makes the merged result's rows ascending.
fn ascending_spans<'a>(chunks: &'a [TimeChunkKey], direct: &'a [DirectSpan]) -> Vec<Span<'a>> {
    let mut spans = Vec::with_capacity(chunks.len() + direct.len());
    let (mut next_chunk, mut next_direct) = (0, 0);
    while next_chunk < chunks.len() || next_direct < direct.len() {
        let take_chunk = match (chunks.get(next_chunk), direct.get(next_direct)) {
            (Some(chunk), Some(span)) => chunk.chunk().start_ms() <= span.span().start_ms(),
            (Some(_), None) => true,
            (None, Some(_)) => false,
            (None, None) => break,
        };
        if take_chunk {
            spans.push(Span::Chunk(&chunks[next_chunk]));
            next_chunk += 1;
        } else {
            spans.push(Span::Direct(&direct[next_direct]));
            next_direct += 1;
        }
    }
    spans
}

/// The rows of every span of one statement, concatenated under one schema.
#[derive(Default)]
struct MergedRows {
    schema: Option<SchemaRef>,
    rows: Vec<RecordBatch>,
}

impl MergedRows {
    /// Appends one span's rows, reporting whether they joined the result.
    ///
    /// A span whose schema differs from the result's cannot be concatenated with the
    /// spans around it, so the caller bypasses the cache for the whole request
    /// instead of reporting an answer no plan produced.
    fn push(&mut self, rows: RecordBatches) -> bool {
        let schema = rows.schema();
        match &self.schema {
            Some(expected) if expected.arrow_schema() != schema.arrow_schema() => return false,
            Some(_) => {}
            None => self.schema = Some(schema),
        }
        self.rows.extend(rows.take());
        true
    }

    /// The merged result: the spans' rows in time order, or the schema alone when
    /// every span was empty, together with everything the spans recorded.
    fn finish(self, annotations: CachedAnnotations) -> server_error::Result<RangeOutcome> {
        let schema = self.schema.unwrap_or_else(|| Arc::new(Schema::new(vec![])));
        let rows = RecordBatches::try_new(schema, self.rows)
            .map_err(BoxedError::new)
            .context(ExecuteQuerySnafu)?;
        Ok(RangeOutcome::Merged { rows, annotations })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use std::time::{Duration, UNIX_EPOCH};

    use common_recordbatch::RecordBatch;
    use datatypes::prelude::ConcreteDataType;
    use datatypes::schema::{ColumnSchema, Schema};
    use datatypes::vectors::{Int64Vector, VectorRef};
    use promql_parser::parser::parse;

    use datatypes::arrow::array::Int64Array;

    use super::*;
    use crate::query_cache::{BypassReason, Insert, MissReason, QueryRoute, RollupCacheConfig};

    /// One minute, the step every test statement evaluates on.
    const STEP_MS: i64 = 60_000;
    /// Five minutes: the chunk target the tests configure.
    const CHUNK_MS: i64 = 300_000;
    /// A five-minute-aligned instant, standing in for 10:00.
    const HEAD_MS: i64 = 36_000_000;
    /// The ttl every test configures.
    const TTL: Duration = Duration::from_secs(3_600);
    /// The id the table every test reads resolves to.
    const CPU_TABLE_ID: u32 = 1024;

    fn at(ms: i64) -> SystemTime {
        UNIX_EPOCH + Duration::from_millis(ms.unsigned_abs())
    }

    /// The millisecond instant `time` names.
    fn millis(time: SystemTime) -> i64 {
        query::promql::label_values::signed_millis_since_epoch(time).unwrap()
    }

    /// A statement from `start_ms` over `points` grid points, stepping `step_ms`.
    fn range_stmt(expr: &str, start_ms: i64, points: u64, step_ms: i64) -> EvalStmt {
        EvalStmt {
            expr: parse(expr).expect("the expression parses"),
            start: at(start_ms),
            end: at(start_ms + (points as i64 - 1) * step_ms),
            interval: Duration::from_millis(step_ms.unsigned_abs()),
            lookback_delta: Duration::from_secs(300),
        }
    }

    fn identity(query: &str) -> QueryIdentity {
        QueryIdentity {
            tenant: "greptime".to_string(),
            catalog: "greptime".to_string(),
            schema: "public".to_string(),
            query: query.to_string(),
            // Every test query reads cpu; a test that wants another series set states
            // it in the query text.
            source_table: "cpu".to_string(),
            table_id: CPU_TABLE_ID,
            step_ms: STEP_MS,
            lookback_ms: 300_000,
            route: QueryRoute::Legacy,
            alias: None,
            read_preference: None,
        }
    }

    /// A cache whose tail is empty, so every point of a historical statement is
    /// cacheable, and whose chunk is five steps of a one-minute grid.
    fn test_cache() -> RollupCache {
        let config = RollupCacheConfig::new(
            1 << 20,
            Duration::ZERO,
            Duration::from_millis(CHUNK_MS as u64),
            TTL,
        )
        .expect("the test configuration is valid");
        RollupCache::new(config)
    }

    /// One row per grid point of `stmt`, holding the point's timestamp as an integer.
    fn rows_of(stmt: &EvalStmt) -> RecordBatches {
        let grid = EvalGrid::of(stmt).expect("a window is on a grid");
        let points = (0..grid.points())
            .map(|index| grid.point_ms(index).expect("the point fits"))
            .collect::<Vec<_>>();
        let schema = Arc::new(Schema::new(vec![ColumnSchema::new(
            "point",
            ConcreteDataType::int64_datatype(),
            false,
        )]));
        let columns: Vec<VectorRef> = vec![Arc::new(Int64Vector::from_vec(points))];
        RecordBatches::try_new(
            schema.clone(),
            vec![RecordBatch::new(schema, columns).unwrap()],
        )
        .unwrap()
    }

    /// Every point a result holds, in row order.
    fn points(result: &RecordBatches) -> Vec<i64> {
        let mut points = Vec::new();
        for batch in result.iter() {
            let column = batch
                .column_by_name("point")
                .expect("the result holds the point column")
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("the point column is an integer column");
            points.extend(column.values().iter().copied());
        }
        points
    }

    /// An executor that records the windows it was asked to plan and execute.
    #[derive(Default)]
    struct RecordingExecutor {
        windows: Mutex<Vec<(i64, i64)>>,
        /// The zero-based call at which the executor fails, when one is set.
        fail_at: Option<usize>,
        /// The warning every window the executor runs records.
        warns: Option<String>,
    }

    impl RecordingExecutor {
        fn failing_at(call: usize) -> Self {
            Self {
                fail_at: Some(call),
                ..Default::default()
            }
        }

        /// An executor whose every window records `warning`, as a range function
        /// over a mixed float/native-histogram window does.
        fn warning(warning: &str) -> Self {
            Self {
                warns: Some(warning.to_string()),
                ..Default::default()
            }
        }

        /// The windows executed so far, as `(start_ms, end_ms)` pairs.
        fn windows(&self) -> Vec<(i64, i64)> {
            self.windows.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl WindowExecutor for RecordingExecutor {
        async fn execute_window(&self, stmt: EvalStmt) -> server_error::Result<WindowOutput> {
            let call = {
                let mut windows = self.windows.lock().unwrap();
                windows.push((millis(stmt.start), millis(stmt.end)));
                windows.len() - 1
            };
            if self.fail_at == Some(call) {
                return Err(UnexpectedResultSnafu {
                    reason: format!("the executor was asked to fail on call {call}"),
                }
                .build());
            }
            let annotations = match &self.warns {
                Some(warning) => CachedAnnotations::new(vec![warning.clone()], vec![]),
                None => CachedAnnotations::default(),
            };
            Ok(WindowOutput {
                rows: rows_of(&stmt),
                annotations,
            })
        }
    }

    /// An executor that fails the test if it is asked to execute anything: a warm
    /// chunk must never reach it.
    struct ForbiddenExecutor;

    #[async_trait]
    impl WindowExecutor for ForbiddenExecutor {
        async fn execute_window(&self, stmt: EvalStmt) -> server_error::Result<WindowOutput> {
            panic!(
                "a warm chunk was planned and executed: [{}, {}]",
                millis(stmt.start),
                millis(stmt.end)
            );
        }
    }

    async fn run<E: WindowExecutor + ?Sized>(
        executor: &E,
        cache: &RollupCache,
        stmt: &EvalStmt,
        query: &str,
    ) -> server_error::Result<RangeOutcome> {
        // The wall clock is the real one and the tail is empty, so every point of a
        // historical statement is cacheable, and an entry is younger than the ttl
        // unless a test says otherwise.
        execute_range(executor, cache, identity(query), stmt, SystemTime::now()).await
    }

    fn merged(outcome: RangeOutcome) -> RecordBatches {
        match outcome {
            RangeOutcome::Merged { rows, .. } => rows,
            RangeOutcome::Bypassed => panic!("the statement was expected to be chunked"),
        }
    }

    fn recorded(outcome: &RangeOutcome) -> CachedAnnotations {
        match outcome {
            RangeOutcome::Merged { annotations, .. } => annotations.clone(),
            RangeOutcome::Bypassed => panic!("the statement was expected to be chunked"),
        }
    }

    #[tokio::test]
    async fn a_warm_chunk_is_served_without_planning_or_executing_it() {
        let cache = test_cache();
        // Ten points over two five-minute chunks: no partial chunk at either end, so
        // every point of the coverage is a chunk and a warm request must execute
        // nothing at all.
        let stmt = range_stmt("cpu", HEAD_MS, 10, STEP_MS);
        let cold = RecordingExecutor::default();

        let cold_rows = merged(run(&cold, &cache, &stmt, "cpu").await.unwrap());
        assert_eq!(
            vec![
                (HEAD_MS, HEAD_MS + CHUNK_MS - STEP_MS),
                (HEAD_MS + CHUNK_MS, HEAD_MS + 2 * CHUNK_MS - STEP_MS)
            ],
            cold.windows()
        );
        assert_eq!(2, cache.entry_count());

        // The second request reaches the cache before it reaches the executor: an
        // executor that panics when used shows the chunk was neither planned nor
        // executed, rather than that a lookup answered.
        let warm_rows = merged(run(&ForbiddenExecutor, &cache, &stmt, "cpu").await.unwrap());
        assert_eq!(points(&cold_rows), points(&warm_rows));
        assert_eq!(
            (0..10)
                .map(|index| HEAD_MS + index * STEP_MS)
                .collect::<Vec<_>>(),
            points(&warm_rows)
        );
        assert_eq!(2, cache.hit_count(), "both chunks were served from memory");
    }

    #[tokio::test]
    async fn a_miss_fills_the_chunk_it_read() {
        let cache = test_cache();
        let stmt = range_stmt("cpu", HEAD_MS, 10, STEP_MS);
        let executor = RecordingExecutor::default();

        assert_eq!(0, cache.entry_count());
        let rows = merged(run(&executor, &cache, &stmt, "cpu").await.unwrap());

        assert_eq!(
            (0..10)
                .map(|index| HEAD_MS + index * STEP_MS)
                .collect::<Vec<_>>(),
            points(&rows)
        );
        assert_eq!(2, cache.entry_count(), "both chunks were filled");
        assert_eq!(0, cache.hit_count(), "nothing was served");
    }

    #[tokio::test]
    async fn a_fill_that_reaches_the_ttl_is_executed_again() {
        // A ttl short enough to wait out, so the entry is dropped by age rather than by
        // anything having been observed to change.
        let config = RollupCacheConfig::new(
            1 << 20,
            Duration::ZERO,
            Duration::from_millis(CHUNK_MS as u64),
            Duration::from_millis(20),
        )
        .expect("the test configuration is valid");
        let cache = RollupCache::new(config);
        let stmt = range_stmt("cpu", HEAD_MS, 10, STEP_MS);
        let cold = RecordingExecutor::default();
        merged(run(&cold, &cache, &stmt, "cpu").await.unwrap());
        assert_eq!(2, cache.entry_count());
        assert!(matches!(
            run(&ForbiddenExecutor, &cache, &stmt, "cpu").await.unwrap(),
            RangeOutcome::Merged { .. }
        ));

        tokio::time::sleep(Duration::from_millis(40)).await;

        // Past the ttl nothing may be served, so both chunks are read and filled again.
        let warm = RecordingExecutor::default();
        let rows = merged(run(&warm, &cache, &stmt, "cpu").await.unwrap());
        assert_eq!(
            vec![
                (HEAD_MS, HEAD_MS + CHUNK_MS - STEP_MS),
                (HEAD_MS + CHUNK_MS, HEAD_MS + 2 * CHUNK_MS - STEP_MS),
            ],
            warm.windows(),
            "both chunks were executed again"
        );
        assert_eq!(
            (0..10)
                .map(|index| HEAD_MS + index * STEP_MS)
                .collect::<Vec<_>>(),
            points(&rows)
        );
        assert_eq!(
            2,
            cache.hit_count(),
            "both chunks were served before the ttl and neither after it"
        );
    }

    #[tokio::test]
    async fn a_hit_reuses_the_payload_of_the_chunk_it_serves() {
        let cache = test_cache();
        let stmt = range_stmt("cpu", HEAD_MS, 10, STEP_MS);
        let cold = RecordingExecutor::default();
        let cold_rows = merged(run(&cold, &cache, &stmt, "cpu").await.unwrap());

        // The same request one chunk later shares the second chunk only, which is
        // what a sliding dashboard does.
        let sliding = range_stmt("cpu", HEAD_MS + CHUNK_MS, 10, STEP_MS);
        let sliding_executor = RecordingExecutor::default();
        let slid = merged(
            run(&sliding_executor, &cache, &sliding, "cpu")
                .await
                .unwrap(),
        );

        assert_eq!(
            vec![(HEAD_MS + 2 * CHUNK_MS, HEAD_MS + 3 * CHUNK_MS - STEP_MS)],
            sliding_executor.windows(),
            "only the chunk that was not cached reaches the executor"
        );
        let expected = (0..10)
            .map(|index| HEAD_MS + index * STEP_MS)
            .collect::<Vec<_>>();
        assert_eq!(expected, points(&cold_rows));
        assert_eq!(
            expected
                .iter()
                .cloned()
                .skip(5)
                .chain((10..15).map(|index| HEAD_MS + index * STEP_MS))
                .collect::<Vec<_>>(),
            points(&slid)
        );
        assert_eq!(1, cache.hit_count(), "one chunk was served");
        assert_eq!(3, cache.entry_count(), "the third chunk was filled");
    }

    #[tokio::test]
    async fn a_direct_span_is_executed_but_not_inserted() {
        let cache = test_cache();
        // Twelve points: two whole chunks and a two-point edge that may not be
        // keyed as a chunk.
        let stmt = range_stmt("cpu", HEAD_MS, 12, STEP_MS);
        let executor = RecordingExecutor::default();

        let rows = merged(run(&executor, &cache, &stmt, "cpu").await.unwrap());

        assert_eq!(
            vec![
                (HEAD_MS, HEAD_MS + CHUNK_MS - STEP_MS),
                (HEAD_MS + CHUNK_MS, HEAD_MS + 2 * CHUNK_MS - STEP_MS),
                (HEAD_MS + 2 * CHUNK_MS, HEAD_MS + 2 * CHUNK_MS + STEP_MS),
            ],
            executor.windows()
        );
        assert_eq!(2, cache.entry_count(), "the edge span is not an entry");
        // The merged rows are in time order and hold every point once.
        assert_eq!(
            (0..12)
                .map(|index| HEAD_MS + index * STEP_MS)
                .collect::<Vec<_>>(),
            points(&rows)
        );
    }

    #[tokio::test]
    async fn the_tail_is_executed_directly_on_every_request() {
        let cache = test_cache();
        let stmt = range_stmt("cpu", HEAD_MS, 10, STEP_MS);
        // The wall clock sits on a chunk boundary inside the coverage, so the last
        // points are in the tail and may not be cached however often they are asked
        // for. Ten points at a five-minute chunk are two chunks; the tail starts where
        // the second one does, so only the first is cacheable.
        let now = at(HEAD_MS + CHUNK_MS);
        let expected = (0..10)
            .map(|index| HEAD_MS + index * STEP_MS)
            .collect::<Vec<_>>();
        let mut readings = Vec::new();
        for _ in 0..2 {
            let executor = RecordingExecutor::default();
            let rows = merged(
                execute_range(&executor, &cache, identity("cpu"), &stmt, now)
                    .await
                    .unwrap(),
            );
            assert_eq!(expected, points(&rows));
            readings.push(executor.windows());
        }
        assert_eq!(
            vec![
                vec![
                    (HEAD_MS, HEAD_MS + CHUNK_MS - STEP_MS),
                    (HEAD_MS + CHUNK_MS, HEAD_MS + 9 * STEP_MS),
                ],
                // The second request is served the chunk and reads only the tail.
                vec![(HEAD_MS + CHUNK_MS, HEAD_MS + 9 * STEP_MS)],
            ],
            readings,
            "the cacheable chunk is read once and the tail on every request"
        );
        // The first chunk was filled once and served the second time; the tail is
        // never an entry.
        assert_eq!(1, cache.entry_count());
        assert_eq!(1, cache.hit_count());
    }

    #[tokio::test]
    async fn a_failed_fill_publishes_no_rows_and_inserts_no_partial_chunk() {
        let cache = test_cache();
        let stmt = range_stmt("cpu", HEAD_MS, 10, STEP_MS);
        // The first span fails, so nothing was consumed and nothing may be offered
        // as a result or as an entry.
        let failing = RecordingExecutor::failing_at(0);
        assert!(run(&failing, &cache, &stmt, "cpu").await.is_err());
        assert_eq!(0, cache.entry_count());

        // The second span fails after the first was filled: the answer is an error
        // rather than the first chunk, and the chunk that was read to the end stays
        // as the complete entry it is.
        let cache = test_cache();
        let failing = RecordingExecutor::failing_at(1);
        assert!(run(&failing, &cache, &stmt, "cpu").await.is_err());
        assert_eq!(1, cache.entry_count());
    }

    #[tokio::test]
    async fn a_statement_outside_the_allowlist_bypasses_the_cache() {
        let cache = test_cache();
        // `avg` is not the one aggregate the window allowlist admits.
        let stmt = range_stmt("avg(cpu)", HEAD_MS, 10, STEP_MS);

        assert!(matches!(
            run(&ForbiddenExecutor, &cache, &stmt, "avg(cpu)")
                .await
                .unwrap(),
            RangeOutcome::Bypassed
        ));
        assert_eq!(0, cache.entry_count());
    }

    #[tokio::test]
    async fn a_coverage_without_a_cacheable_chunk_bypasses_the_cache() {
        let cache = test_cache();
        // Four points are shorter than one chunk, so every point is an edge span.
        let stmt = range_stmt("cpu", HEAD_MS, 4, STEP_MS);

        assert!(matches!(
            run(&ForbiddenExecutor, &cache, &stmt, "cpu").await.unwrap(),
            RangeOutcome::Bypassed
        ));
        assert_eq!(0, cache.entry_count());
    }

    #[tokio::test]
    async fn a_request_of_another_identity_is_not_served() {
        let cache = test_cache();
        let stmt = range_stmt("cpu", HEAD_MS, 10, STEP_MS);
        merged(
            run(&RecordingExecutor::default(), &cache, &stmt, "cpu")
                .await
                .unwrap(),
        );
        assert_eq!(2, cache.entry_count());

        // The same series set on another step keys its chunks as spans of another
        // grid, so the chunks of this one cannot answer it.
        let stepped = range_stmt("cpu", HEAD_MS, 10, 2 * STEP_MS);
        let mut stepped_identity = identity("cpu");
        stepped_identity.step_ms = 2 * STEP_MS;
        let executor = RecordingExecutor::default();
        merged(
            execute_range(
                &executor,
                &cache,
                stepped_identity,
                &stepped,
                SystemTime::now(),
            )
            .await
            .unwrap(),
        );
        assert!(!executor.windows().is_empty(), "the other grid was read");
        assert_eq!(0, cache.hit_count(), "nothing was served across grids");

        // A query that selects other series is another identity as well.
        let other = range_stmt("cpu{code=\"500\"}", HEAD_MS, 10, STEP_MS);
        let executor = RecordingExecutor::default();
        merged(
            run(&executor, &cache, &other, "cpu{code=\"500\"}")
                .await
                .unwrap(),
        );
        assert_eq!(2, executor.windows().len(), "the other series were read");
        assert_eq!(0, cache.hit_count());
    }

    #[tokio::test]
    async fn a_table_created_under_a_reused_name_is_never_served_the_old_chunks() {
        let cache = test_cache();
        let stmt = range_stmt("cpu", HEAD_MS, 10, STEP_MS);
        merged(
            run(&RecordingExecutor::default(), &cache, &stmt, "cpu")
                .await
                .unwrap(),
        );
        assert_eq!(2, cache.entry_count());

        // `DROP TABLE` followed by `CREATE TABLE` under the same name: every dimension
        // of the request is unchanged and only the id the name resolves to is new, so
        // the chunks of the table that was dropped must not answer the new one.
        let mut recreated = identity("cpu");
        recreated.table_id = CPU_TABLE_ID + 1;
        let executor = RecordingExecutor::default();
        merged(
            execute_range(
                &executor,
                &cache,
                recreated.clone(),
                &stmt,
                SystemTime::now(),
            )
            .await
            .unwrap(),
        );
        assert_eq!(2, executor.windows().len(), "the new table was read");
        assert_eq!(0, cache.hit_count(), "nothing was served across tables");
        assert_eq!(
            4,
            cache.entry_count(),
            "the chunks of the dropped table are resident but unreachable"
        );

        // The recreated table's own chunks answer its second request.
        merged(
            execute_range(
                &ForbiddenExecutor,
                &cache,
                recreated,
                &stmt,
                SystemTime::now(),
            )
            .await
            .unwrap(),
        );
        assert_eq!(2, cache.hit_count(), "the new table's chunks were served");
    }

    #[tokio::test]
    async fn a_hit_reports_the_annotations_of_the_read_it_serves() {
        let cache = test_cache();
        let stmt = range_stmt("cpu", HEAD_MS, 10, STEP_MS);
        let cold =
            RecordingExecutor::warning("encountered a mix of float and native histogram samples");

        // The fill records the warning; the request that filled reports it once.
        let filled = run(&cold, &cache, &stmt, "cpu").await.unwrap();
        assert_eq!(
            vec!["encountered a mix of float and native histogram samples".to_string()],
            recorded(&filled).warnings()
        );

        // The warm request executes nothing and still reports the warning of the
        // read it is served, exactly once.
        let warm = run(&ForbiddenExecutor, &cache, &stmt, "cpu").await.unwrap();
        assert_eq!(
            vec!["encountered a mix of float and native histogram samples".to_string()],
            recorded(&warm).warnings()
        );
        assert!(recorded(&warm).infos().is_empty());
        assert_eq!(points(&merged(filled)), points(&merged(warm)));
    }

    #[tokio::test]
    async fn a_direct_span_records_annotations_without_being_inserted() {
        let cache = test_cache();
        // Twelve points: two whole chunks and a two-point edge that may not be keyed
        // as a chunk.
        let stmt = range_stmt("cpu", HEAD_MS, 12, STEP_MS);
        let executor = RecordingExecutor::warning("the edge span warned");

        let cold = run(&executor, &cache, &stmt, "cpu").await.unwrap();
        assert_eq!(2, cache.entry_count(), "the edge span is not an entry");
        assert_eq!(
            vec!["the edge span warned".to_string()],
            recorded(&cold).warnings(),
            "the three spans recorded one message between them"
        );

        // The warm request reads only the edge span again, and still reports the
        // message once rather than once per span.
        let warm = run(
            &RecordingExecutor::warning("the edge span warned"),
            &cache,
            &stmt,
            "cpu",
        )
        .await
        .unwrap();
        assert_eq!(
            vec!["the edge span warned".to_string()],
            recorded(&warm).warnings()
        );
    }

    #[tokio::test]
    async fn a_chunk_without_annotations_stays_empty_across_hits() {
        let cache = test_cache();
        let stmt = range_stmt("cpu", HEAD_MS, 10, STEP_MS);
        let filled = run(&RecordingExecutor::default(), &cache, &stmt, "cpu")
            .await
            .unwrap();
        assert!(recorded(&filled).is_empty());

        let warm = run(&ForbiddenExecutor, &cache, &stmt, "cpu").await.unwrap();
        assert!(recorded(&warm).is_empty());
    }

    #[tokio::test]
    async fn an_empty_chunk_is_never_served() {
        let cache = test_cache();
        // A zero-length chunk stands for nothing that was ever materialized, so both
        // directions refuse it rather than keying an entry under an empty span.
        let key = TimeChunkKey::new(identity("cpu"), TimeChunk::new(HEAD_MS, HEAD_MS));
        assert_eq!(
            cache.lookup(&key, SystemTime::now()),
            Lookup::Bypass(BypassReason::EmptyChunk)
        );
        assert_eq!(
            cache.insert(
                &key,
                Arc::new(RecordBatches::empty()),
                Arc::new(CachedAnnotations::default())
            ),
            Insert::Bypassed
        );
        assert_eq!(cache.entry_count(), 0);
        // The miss of a chunk nobody filled says exactly that.
        assert_eq!(
            cache.lookup(
                &TimeChunkKey::new(identity("cpu"), TimeChunk::new(HEAD_MS, HEAD_MS + CHUNK_MS)),
                SystemTime::now()
            ),
            Lookup::Miss(MissReason::NotFound)
        );
    }
}
