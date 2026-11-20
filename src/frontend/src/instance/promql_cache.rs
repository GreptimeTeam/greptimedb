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
use promql_parser::parser::EvalStmt;
use query::promql::window::{EvalGrid, max_input_range_ms, windowed_eval_stmt};
use servers::error::{self as server_error, ExecuteQuerySnafu, UnexpectedResultSnafu};
use snafu::ResultExt;

use crate::query_cache::{
    CachedAnnotations, ChunkPlan, DirectSpan, FillCursor, Lookup, MissReason, QueryIdentity,
    RollupCache, TimeChunk, TimeChunkKey,
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

    /// The table ID the source name currently resolves to, when this executor can
    /// ask the catalog. A fill whose plan may have bound a different incarnation
    /// must not publish.
    async fn source_table_id(&self) -> Option<u32> {
        None
    }
}

/// What one evaluation window produced.
#[derive(Debug)]
pub(crate) struct WindowOutput {
    /// Every row the window read.
    pub rows: RecordBatches,
    /// The PromQL annotations the window's read recorded.
    pub annotations: CachedAnnotations,
    /// Table IDs bound by the executed plan.
    pub source_table_ids: Vec<u32>,
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
/// Wall-clock `now` is what places the tail: points at or after `now - tail` may
/// still be receiving data, so they are executed directly and never cached.
pub(crate) async fn execute_range<E>(
    executor: &E,
    cache: &RollupCache,
    identity: QueryIdentity,
    stmt: &EvalStmt,
    now: SystemTime,
    source_epoch: u64,
    source_table_id: Option<u32>,
) -> server_error::Result<RangeOutcome>
where
    E: WindowExecutor + ?Sized,
{
    // The entire request must bypass if its input dependency cannot be bounded:
    // deciding this before planning or executing any span avoids a partial fill.
    let (Ok(input_range_ms), Ok(lookback_ms)) = (
        max_input_range_ms(&stmt.expr),
        i64::try_from(stmt.lookback_delta.as_millis()),
    ) else {
        return Ok(RangeOutcome::Bypassed);
    };
    let dependency_history_ms = input_range_ms.max(lookback_ms);
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

    if cache.source_epoch() != source_epoch {
        return Ok(RangeOutcome::Bypassed);
    }
    let invalidation_epoch = cache.invalidation_epoch();
    let mut merged = MergedRows::default();
    let mut warnings = Vec::new();
    let mut infos = Vec::new();
    for span in ascending_spans(planned.chunks(), planned.direct()) {
        if let Some(expected) = source_table_id
            && executor.source_table_id().await != Some(expected)
        {
            // A table incarnation changed after validation. Do not mix old and
            // replacement rows; let the caller run its original plan.
            return Ok(RangeOutcome::Bypassed);
        }
        let Some(window) = (match span {
            Span::Chunk(key) => {
                chunk_rows(
                    executor,
                    cache,
                    key,
                    stmt,
                    &grid,
                    now,
                    dependency_history_ms,
                    source_epoch,
                    source_table_id,
                )
                .await?
            }
            Span::Direct(direct) => {
                let window = execute_span(executor, stmt, &grid, direct.span()).await?;
                if let Some(expected) = source_table_id
                    && (window.source_table_ids.is_empty()
                        || window
                            .source_table_ids
                            .iter()
                            .any(|table_id| *table_id != expected))
                {
                    // The direct window bound a different incarnation. Do not mix
                    // its rows with cached rows from the validated table.
                    return Ok(RangeOutcome::Bypassed);
                }
                Some(window)
            }
        }) else {
            return Ok(RangeOutcome::Bypassed);
        };
        warnings.extend(window.annotations.warnings().iter().cloned());
        infos.extend(window.annotations.infos().iter().cloned());
        merged.push(window.rows)?;
    }
    // A request must not mix rows read on both sides of an observed invalidation.
    // Discard the partial merge and let the caller run its original plan.
    if cache.invalidation_epoch() != invalidation_epoch || cache.source_epoch() != source_epoch {
        return Ok(RangeOutcome::Bypassed);
    }
    merged.finish(CachedAnnotations::new(warnings, infos))
}

/// The rows of one cacheable `key`, served from memory when possible.
///
/// The fill cursor is captured before the read it describes, so the entry's age is
/// counted from the read that produced it and a fill that raced a newer one cannot
/// overwrite it.
#[allow(clippy::too_many_arguments)]
async fn chunk_rows<E>(
    executor: &E,
    cache: &RollupCache,
    key: &TimeChunkKey,
    stmt: &EvalStmt,
    grid: &EvalGrid,
    now: SystemTime,
    dependency_history_ms: i64,
    source_epoch: u64,
    source_table_id: Option<u32>,
) -> server_error::Result<Option<WindowOutput>>
where
    E: WindowExecutor + ?Sized,
{
    // A transition after validation makes both the cached payload and any new
    // fill unvalidated. Bypass the entire request rather than mix rows from two
    // source incarnations.
    if cache.source_epoch() != source_epoch {
        return Ok(None);
    }
    if let Some(expected) = source_table_id
        && executor.source_table_id().await != Some(expected)
    {
        return Ok(None);
    }
    let fill: FillCursor = match cache.lookup_fenced(key, now, source_epoch, source_table_id) {
        Lookup::Hit {
            payload,
            annotations,
            ..
        } => {
            // The rows and the messages of one read are served together, so a hit
            // reports what the read it replaces reported.
            return Ok(Some(WindowOutput {
                rows: clone_rows(&payload)?,
                annotations: (*annotations).clone(),
                source_table_ids: Vec::new(),
            }));
        }
        Lookup::Miss(MissReason::SourceChanged) => return Ok(None),
        Lookup::Bypass(_) | Lookup::Miss(_) => match cache.fill_cursor(now) {
            Ok(fill) => fill
                .with_source_epoch(source_epoch)
                .with_source_table_id(source_table_id),
            // No fill may be validated right now (an unhealthy or stale authority):
            // read the chunk without keeping it rather than failing a query the
            // original path answers.
            Err(_) => {
                return execute_span(executor, stmt, grid, key.chunk())
                    .await
                    .map(Some);
            }
        },
    };
    let window = execute_span(executor, stmt, grid, key.chunk()).await?;
    if let Some(expected) = source_table_id
        && (window.source_table_ids.is_empty()
            || window
                .source_table_ids
                .iter()
                .any(|table_id| *table_id != expected))
    {
        // The executed plan bound a different incarnation from the one validated.
        // The rows answer the plan that ran but must not be published under the
        // validated name.
        return Ok(None);
    }
    // Only a fully consumed chunk is inserted: `execute_span` returns rows it read to
    // the end, and a failure returned above without inserting anything. Its
    // annotations are stored beside them, and the same ones are returned, so the
    // request that filled the chunk reports them exactly as a later hit does.
    let chunk = key.chunk();
    let dependency = TimeChunk::new(
        chunk.start_ms().saturating_sub(dependency_history_ms),
        chunk.end_ms(),
    );
    cache.insert_with_dependency(
        key.clone(),
        fill,
        Arc::new(clone_rows(&window.rows)?),
        Arc::new(window.annotations.clone()),
        dependency,
    );
    Ok(Some(window))
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
        let duration = Duration::from_millis(ms.unsigned_abs());
        if ms >= 0 {
            UNIX_EPOCH + duration
        } else {
            UNIX_EPOCH - duration
        }
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
            // All admitted test queries read cpu. Bypassed test queries never fill.
            source_table: "cpu".to_string(),
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
                source_table_ids: Vec::new(),
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
        execute_range(
            executor,
            cache,
            identity(query),
            stmt,
            at(NOW_MS),
            cache.source_epoch(),
            None,
        )
        .await
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
    }

    #[tokio::test]
    async fn filled_chunks_invalidate_on_their_full_input_history() {
        for (query, lookback_ms, history_ms) in [
            ("cpu", 300_000, 300_000),
            ("rate(cpu[10m])", 300_000, 600_000),
            ("sum(rate(cpu[10m]))", 300_000, 600_000),
            ("rate(cpu[10m])", 900_000, 900_000),
        ] {
            let cache = test_cache();
            let mut stmt = range_stmt(query, HEAD_MS, 5, STEP_MS);
            stmt.lookback_delta = Duration::from_millis(lookback_ms as u64);
            let mut identity = identity(query);
            identity.lookback_ms = lookback_ms;
            let key = TimeChunkKey::new(
                identity.clone(),
                TimeChunk::new(HEAD_MS, HEAD_MS + CHUNK_MS),
            );

            let rows = execute_range(
                &RecordingExecutor::default(),
                &cache,
                identity.clone(),
                &stmt,
                at(NOW_MS),
                cache.source_epoch(),
                None,
            )
            .await
            .unwrap();
            assert_eq!(points(&merged(rows)).len(), 5, "{query}");
            assert_eq!(cache.entry_count(), 1, "{query}");
            assert!(matches!(cache.lookup(&key, at(NOW_MS)), Lookup::Hit { .. }));

            // The dependency begins exactly `history_ms` before the output; a
            // disjoint range ending there leaves the entry resident.
            let start = HEAD_MS - history_ms;
            assert_eq!(
                cache.invalidate_table_range(
                    &identity.source_table,
                    TimeChunk::new(start - 1, start)
                ),
                0,
                "{query}"
            );
            assert!(matches!(cache.lookup(&key, at(NOW_MS)), Lookup::Hit { .. }));
            let end = HEAD_MS + CHUNK_MS;
            assert_eq!(
                cache.invalidate_table_range(&identity.source_table, TimeChunk::new(end, end + 1)),
                0,
                "{query}"
            );
            assert!(matches!(cache.lookup(&key, at(NOW_MS)), Lookup::Hit { .. }));
            // A single dirty millisecond inside that history evicts the chunk,
            // even though all its output points come later.
            assert_eq!(
                cache.invalidate_table_range(
                    &identity.source_table,
                    TimeChunk::new(start, start + 1)
                ),
                1,
                "{query}"
            );
            assert!(matches!(cache.lookup(&key, at(NOW_MS)), Lookup::Miss(_)));
        }
    }

    #[tokio::test]
    async fn dependency_start_saturates_at_the_minimum_timestamp() {
        let cache = test_cache();
        let query = "cpu";
        let mut stmt = range_stmt(query, -CHUNK_MS, 5, STEP_MS);
        stmt.lookback_delta = Duration::from_millis(i64::MAX as u64);
        let mut identity = identity(query);
        identity.lookback_ms = i64::MAX;
        let rows = execute_range(
            &RecordingExecutor::default(),
            &cache,
            identity.clone(),
            &stmt,
            at(NOW_MS),
            cache.source_epoch(),
            None,
        )
        .await
        .unwrap();
        assert_eq!(points(&merged(rows)).len(), 5);
        assert_eq!(cache.entry_count(), 1);
        assert_eq!(
            cache.invalidate_range(&identity, TimeChunk::new(i64::MIN, i64::MIN + 1)),
            1
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
    async fn unknown_or_unrepresentable_input_history_bypasses_before_any_span() {
        let cache = test_cache();
        // The leading partial chunk would execute first if this were admitted.
        let start = HEAD_MS + STEP_MS;
        let unknown = range_stmt("rate(cpu[10m]) + rate(other[20m])", start, 10, STEP_MS);
        assert!(matches!(
            run(&ForbiddenExecutor, &cache, &unknown, "multi")
                .await
                .unwrap(),
            RangeOutcome::Bypassed
        ));

        for range in [Duration::ZERO, Duration::from_millis(i64::MAX as u64 + 1)] {
            let mut invalid = range_stmt("rate(cpu[10m])", start, 10, STEP_MS);
            let promql_parser::parser::Expr::Call(call) = &mut invalid.expr else {
                panic!("rate is a call");
            };
            let promql_parser::parser::Expr::MatrixSelector(matrix) = &mut *call.args.args[0]
            else {
                panic!("rate folds a matrix selector");
            };
            matrix.range = range;
            assert!(matches!(
                run(&ForbiddenExecutor, &cache, &invalid, "rate(cpu[10m])")
                    .await
                    .unwrap(),
                RangeOutcome::Bypassed
            ));
        }

        let mut overflow_lookback = range_stmt("cpu", start, 10, STEP_MS);
        overflow_lookback.lookback_delta = Duration::from_millis(i64::MAX as u64 + 1);
        assert!(matches!(
            run(&ForbiddenExecutor, &cache, &overflow_lookback, "cpu")
                .await
                .unwrap(),
            RangeOutcome::Bypassed
        ));
        assert_eq!(cache.entry_count(), 0);
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
}
