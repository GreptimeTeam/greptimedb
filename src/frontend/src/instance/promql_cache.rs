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
//!   consumes it, and only then inserts it under the [`FillCursor`] captured
//!   *before* the read;
//! - a **direct span** (the partial chunks at either end of the request and the
//!   whole tail) executes its subwindow and is never inserted, because a partial
//!   chunk must not be keyed as a full one.
//!
//! The spans partition the requested coverage exactly once each, in increasing
//! time, so concatenating their rows keeps the result's order and neither
//! duplicates nor loses a point. Every span of one statement shares its schema, so
//! a merged result that disagrees is reported rather than returned.
//!
//! A statement the cache refuses, or one with nothing cacheable in its coverage,
//! is returned as [`RangeOutcome::Bypassed`] before the first span is executed, so
//! the caller can run its original plan unchanged.
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
use promql_parser::parser::{EvalStmt, Expr as PromExpr};
use query::promql::window::{EvalGrid, windowed_eval_stmt};
use servers::error::{self as server_error, ExecuteQuerySnafu, UnexpectedResultSnafu};
use snafu::ResultExt;

use crate::query_cache::{
    ChunkPlan, DirectSpan, FillCursor, Lookup, QueryIdentity, RollupCache, TimeChunk, TimeChunkKey,
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
    /// Plans and executes `stmt`, returning every row it produced.
    async fn execute_window(&self, stmt: EvalStmt) -> server_error::Result<RecordBatches>;
}

/// What a cache-aware range execution produced.
#[derive(Debug)]
pub(crate) enum RangeOutcome {
    /// The rows of every span of the statement, merged in time order.
    Merged(RecordBatches),
    /// The cache cannot serve this request; the caller must run its original plan.
    ///
    /// Nothing was executed: a bypass is decided by planning alone, before the
    /// first span reaches the executor.
    Bypassed,
}

/// Executes `stmt` as of wall-clock `now`, serving its chunks from `cache` and
/// executing only the spans the plan says must be read.
///
/// Wall-clock `now` is what places the tail: points at or after `now - tail` may
/// still be receiving data, so they are executed directly and never cached.
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
    for span in ascending_spans(planned.chunks(), planned.direct()) {
        let rows = match span {
            Span::Chunk(key) => chunk_rows(executor, cache, key, stmt, &grid, now).await?,
            Span::Direct(direct) => execute_span(executor, stmt, &grid, direct.span()).await?,
        };
        merged.push(rows)?;
    }
    merged.finish()
}

/// The rows of one cacheable `key`, served from memory when possible.
///
/// The fill cursor is captured before the read it describes, so the entry's age is
/// counted from the read that produced it and a fill that raced a newer one cannot
/// overwrite it.
async fn chunk_rows<E>(
    executor: &E,
    cache: &RollupCache,
    key: &TimeChunkKey,
    stmt: &EvalStmt,
    grid: &EvalGrid,
    now: SystemTime,
) -> server_error::Result<RecordBatches>
where
    E: WindowExecutor + ?Sized,
{
    let fill: FillCursor = match cache.lookup(key, now) {
        Lookup::Hit { payload, .. } => return clone_rows(&payload),
        Lookup::Bypass(_) | Lookup::Miss(_) => match cache.fill_cursor(now) {
            Ok(fill) => fill,
            // No fill may be validated right now (an unhealthy or stale authority):
            // read the chunk without keeping it rather than failing a query the
            // original path answers.
            Err(_) => return execute_span(executor, stmt, grid, key.chunk()).await,
        },
    };
    let rows = execute_span(executor, stmt, grid, key.chunk()).await?;
    // Only a fully consumed chunk is inserted: `execute_span` returns rows it read to
    // the end, and a failure returned above without inserting anything.
    cache.insert(key.clone(), fill, Arc::new(clone_rows(&rows)?));
    Ok(rows)
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
) -> server_error::Result<RecordBatches>
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

/// Whether evaluating `expr` can record a PromQL annotation (a warning or an info).
///
/// The window allowlist admits a named instant vector selector, `rate(<named range
/// selector>)`, and `sum` over an admitted expression, however each is
/// parenthesised. Only a range function folds a window, and a window folding float
/// samples together with native histograms is what records a "mixed" annotation
/// (`promql::range_eval::histogram`). A selector is evaluated sample by sample, and a
/// fold over selectors only, records nothing.
///
/// A chunk served from memory is never re-evaluated, so it cannot replay what its
/// original read recorded. Until annotations are stored beside the payload, a
/// statement that can record one keeps the original path rather than risking a
/// dropped warning.
pub(crate) fn may_record_annotations(expr: &PromExpr) -> bool {
    let mut expr = expr;
    loop {
        match expr {
            PromExpr::Paren(paren) => expr = &paren.expr,
            // A grouping modifier moves a value between groups; it does not change
            // which samples the folded window of one point holds.
            PromExpr::Aggregate(aggregate) if aggregate.param.is_none() => expr = &aggregate.expr,
            _ => break,
        }
    }
    !matches!(expr, PromExpr::VectorSelector(_))
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
    /// Appends one span's rows, refusing a span whose schema is not the result's.
    fn push(&mut self, rows: RecordBatches) -> server_error::Result<()> {
        let schema = rows.schema();
        match &self.schema {
            Some(expected) if expected.arrow_schema() != schema.arrow_schema() => {
                return Err(UnexpectedResultSnafu {
                    reason: format!(
                        "the spans of one range query disagree on their schema: {expected:?} and {schema:?}"
                    ),
                }
                .build());
            }
            Some(_) => {}
            None => self.schema = Some(schema),
        }
        self.rows.extend(rows.take());
        Ok(())
    }

    /// The merged result: the spans' rows in time order, or the schema alone when
    /// every span was empty.
    fn finish(self) -> server_error::Result<RangeOutcome> {
        let schema = self.schema.unwrap_or_else(|| Arc::new(Schema::new(vec![])));
        let merged = RecordBatches::try_new(schema, self.rows)
            .map_err(BoxedError::new)
            .context(ExecuteQuerySnafu)?;
        Ok(RangeOutcome::Merged(merged))
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
    use crate::query_cache::{QueryRoute, RollupCacheConfig};

    /// One minute, the step every test statement evaluates on.
    const STEP_MS: i64 = 60_000;
    /// Five minutes: a whole number of steps, so canonical bounds are grid points.
    const CHUNK_MS: i64 = 300_000;
    /// A five-minute-aligned instant, standing in for 10:00.
    const HEAD_MS: i64 = 36_000_000;
    /// A wall clock far enough past every test window that the tail holds nothing.
    const NOW_MS: i64 = HEAD_MS + 24 * 3_600_000;

    fn at(ms: i64) -> SystemTime {
        UNIX_EPOCH + Duration::from_millis(ms.unsigned_abs())
    }

    /// The millisecond instant `time` names.
    fn millis(time: SystemTime) -> i64 {
        i64::try_from(time.duration_since(UNIX_EPOCH).unwrap().as_millis()).unwrap()
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
            step_ms: STEP_MS,
            lookback_ms: 300_000,
            route: QueryRoute::Legacy,
            ordered: false,
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
            Duration::from_secs(60),
            Duration::from_secs(3_600),
        )
        .expect("the test configuration is valid");
        RollupCache::new(config, None)
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
    }

    impl RecordingExecutor {
        fn failing_at(call: usize) -> Self {
            Self {
                fail_at: Some(call),
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
        async fn execute_window(&self, stmt: EvalStmt) -> server_error::Result<RecordBatches> {
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
            Ok(rows_of(&stmt))
        }
    }

    /// An executor that fails the test if it is asked to execute anything: a warm
    /// chunk must never reach it.
    struct ForbiddenExecutor;

    #[async_trait]
    impl WindowExecutor for ForbiddenExecutor {
        async fn execute_window(&self, stmt: EvalStmt) -> server_error::Result<RecordBatches> {
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
        execute_range(executor, cache, identity(query), stmt, at(NOW_MS)).await
    }

    fn merged(outcome: RangeOutcome) -> RecordBatches {
        match outcome {
            RangeOutcome::Merged(rows) => rows,
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
    async fn a_step_that_does_not_divide_the_chunk_bypasses_the_cache() {
        let cache = test_cache();
        // A seven-minute step puts canonical chunk bounds between grid points.
        let stmt = range_stmt("cpu", HEAD_MS, 10, 7 * STEP_MS);

        assert!(matches!(
            run(&ForbiddenExecutor, &cache, &stmt, "cpu").await.unwrap(),
            RangeOutcome::Bypassed
        ));
        assert_eq!(0, cache.entry_count());
    }

    #[test]
    fn only_a_windowed_statement_can_record_annotations() {
        let selector = parse("cpu").unwrap();
        let parenthesised = parse("(cpu)").unwrap();
        let summed = parse("sum(cpu)").unwrap();
        let grouped = parse("sum by (host) (cpu)").unwrap();
        let rate = parse("rate(cpu[5m])").unwrap();
        let parenthesised_rate = parse("rate((cpu[5m]))").unwrap();
        let summed_rate = parse("sum(rate(cpu[5m]))").unwrap();

        assert!(!may_record_annotations(&selector));
        assert!(!may_record_annotations(&parenthesised));
        assert!(!may_record_annotations(&summed));
        assert!(!may_record_annotations(&grouped));
        assert!(may_record_annotations(&rate));
        assert!(may_record_annotations(&parenthesised_rate));
        assert!(may_record_annotations(&summed_rate));
    }
}
