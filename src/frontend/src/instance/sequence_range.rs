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

use std::collections::BTreeMap;
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

use common_error::ext::{BoxedError, PlainError};
use common_error::status_code::StatusCode;
use common_query::{Output, OutputData};
use common_recordbatch::RecordBatch;
use common_recordbatch::adapter::RecordBatchMetrics;
use common_runtime::{RepeatedTask, TaskFunction, global_runtime};
use common_telemetry::debug;
use common_time::{Timestamp, timestamp::TimeUnit};
use datafusion::common::ScalarValue;
use datatypes::value::Value;
use futures::TryStreamExt;
use query::metrics::terminal_recordbatch_metrics_from_plan;
use query::options::{
    FLOW_INCREMENTAL_AFTER_SEQS, FLOW_INCREMENTAL_MODE, FLOW_INCREMENTAL_MODE_SEQUENCE_RANGE,
    FLOW_RETURN_REGION_SEQ, FLOW_SINK_TABLE_ID, INTERNAL_SEQUENCE_RANGE_PROBE,
};
use query::parser::QueryStatement;
use session::context::{QueryContext, QueryContextRef};
use snafu::{IntoError, OptionExt, ResultExt};
use sqlparser::ast::Ident;
use store_api::storage::RegionId;
use table::table_name::TableName;

use super::{Instance, parse_stmt};
use crate::error::{
    CatalogSnafu, CollectRecordbatchSnafu, Error, ExternalSnafu, Result, TableNotFoundSnafu,
};
use crate::query_cache::{DirtySpan, RollupCache, SourceTable, TimeChunk};

/// How many regions one probe round reports on.
fn probe_regions(table_id: u32, route: &common_meta::rpc::router::RegionRoute) -> u64 {
    // A metric logical table owns the logical region IDs even though its route points
    // at a physical table, which is why the probe scans the queryable table.
    RegionId::new(table_id, route.region.id.region_number()).as_u64()
}

/// The background refresher of the rollup cache.
///
/// One instance runs repeated rounds, one round over every active source table. The
/// rounds of one table are therefore sequential by construction: a table is probed
/// by at most one round at a time, and a slow table delays the others rather than
/// overlapping them.
///
/// It holds only a weak reference to its [`Instance`], so a frontend that shuts down
/// without stopping the task still lets the instance go, and the task itself is
/// owned by the instance and cancelled when that handle is dropped or stopped.
struct RollupCacheRefreshTask {
    instance: Weak<Instance>,
}

#[async_trait::async_trait]
impl TaskFunction<Error> for RollupCacheRefreshTask {
    fn name(&self) -> &str {
        "rollup_cache_refresh"
    }

    async fn call(&mut self) -> Result<()> {
        let Some(instance) = self.instance.upgrade() else {
            return Ok(());
        };
        instance.refresh_rollup_cache_once().await;
        Ok(())
    }
}

impl Instance {
    /// Starts the background refresher of the rollup cache, if one is installed.
    ///
    /// The task belongs to the instance and is stopped by
    /// [`Instance::stop_rollup_cache_refresher`], which is what `Frontend::shutdown`
    /// calls, so it never outlives the frontend it serves. Calling it twice is a
    /// no-op.
    pub fn start_rollup_cache_refresher(self: &Arc<Self>) -> Result<()> {
        let Some(cache) = self.rollup_cache.as_ref() else {
            return Ok(());
        };
        let mut slot = self.rollup_cache_refresher.lock().unwrap();
        if slot.as_ref().is_some_and(|task| task.started()) {
            return Ok(());
        }
        let task = RepeatedTask::new(
            cache.config().refresh_interval(),
            Box::new(RollupCacheRefreshTask {
                instance: Arc::downgrade(self),
            }),
        )
        // The first round runs at once: a table becomes servable a refresh interval
        // after the first request for it, not after two.
        .with_initial_delay(Some(Duration::ZERO));
        task.start(global_runtime())
            .map_err(BoxedError::new)
            .context(ExternalSnafu)?;
        *slot = Some(Arc::new(task));
        Ok(())
    }

    /// Stops the background refresher and waits for the round it is running.
    ///
    /// A frontend without a rollup cache, and one whose refresher was never started,
    /// stop successfully without doing anything.
    pub async fn stop_rollup_cache_refresher(&self) -> Result<()> {
        let task = self.rollup_cache_refresher.lock().unwrap().take();
        if let Some(task) = task {
            task.stop()
                .await
                .map_err(BoxedError::new)
                .context(ExternalSnafu)?;
        }
        Ok(())
    }

    /// Runs one background round over every active source table of the rollup cache.
    ///
    /// Nothing here is on a request's path: a request reads the readiness the last
    /// round published, and a round that cannot prove a table closes that readiness
    /// instead of guessing.
    pub(crate) async fn refresh_rollup_cache_once(&self) {
        let Some(cache) = self.rollup_cache.as_ref() else {
            return;
        };
        let now = Instant::now();
        for table in cache.tables_to_refresh(now) {
            match self.refresh_source_table(cache, &table).await {
                // A round that cannot commit its vector leaves the cursors where they were:
                // the next round sees the same delta and retries.
                Some((cursors, dirty)) => {
                    if !cache.record_refresh(&table, cursors, dirty, Instant::now()) {
                        debug!("Rollup cache could not commit a round of {table}");
                    }
                }
                None => cache.record_failure(&table),
            }
        }
    }

    /// Probes one source table and answers its new cursors and what the probe found
    /// changed.
    ///
    /// [`None`] means the round proved nothing, and the caller must close the
    /// table's readiness. A round never advances a cursor without a proof: a removed
    /// row may advance a sequence without leaving a timestamp to invalidate by.
    async fn refresh_source_table(
        &self,
        cache: &RollupCache,
        table: &SourceTable,
    ) -> Option<(BTreeMap<u64, u64>, DirtySpan)> {
        let table_name = TableName::new(table.catalog(), table.schema(), table.table());
        // The probe is not a request: it runs as the frontend, under the catalog and
        // schema the table lives in, and reads nothing that depends on who asked.
        let query_ctx = Arc::new(QueryContext::with(table.catalog(), table.schema()));
        let resolved = self
            .catalog_manager
            .table(
                &table_name.catalog_name,
                &table_name.schema_name,
                &table_name.table_name,
                Some(&query_ctx),
            )
            .await
            .ok()??;
        let table_id = resolved.table_info().table_id();
        let (_physical_table_id, route) = self
            .partition_manager()
            .find_physical_table_route_with_id(table_id)
            .await
            .ok()?;
        let regions = route
            .region_routes
            .iter()
            .map(|route| probe_regions(table_id, route))
            .collect::<Vec<_>>();
        if regions.is_empty() {
            return None;
        }

        let cursors = cache.cursors(table);
        // A region the table did not have last round starts from zero, so its whole
        // history is part of this round's delta; a region it no longer has is
        // dropped, and the next round that has it again starts from zero.
        let lower = regions
            .iter()
            .map(|region| (*region, cursors.get(region).copied().unwrap_or(0)))
            .collect::<BTreeMap<_, _>>();
        let (high, min, max) = self
            .probe_sequence_range(&table_name, &lower, &query_ctx)
            .await
            .ok()?;
        if high.len() != regions.len()
            || high.keys().any(|region| !lower.contains_key(region))
            || high
                .iter()
                .any(|(region, watermark)| watermark < &lower[region])
        {
            return None;
        }
        let changed = high != lower;
        let dirty = match (min, max) {
            (Some(min), Some(max)) => {
                let min_ms = min.convert_to(TimeUnit::Millisecond)?;
                let max_ms = max.convert_to(TimeUnit::Millisecond)?;
                // Round outward: a sub-millisecond timestamp still lies inside the
                // span, and an extra millisecond only costs a later miss. A put that
                // overwrites a row at the same timestamp is caught by the same span;
                // an explicit DELETE whose timestamp the ordinary scan hides is the
                // one operation a timestamp hull cannot represent, and it is out of
                // scope.
                DirtySpan::Hull(TimeChunk::new(
                    min_ms.value().saturating_sub(1),
                    max_ms.value().saturating_add(1),
                ))
            }
            // A sequence advance without a timestamp can mean erased rows. Do not
            // guess a span: no entry of this table may survive. An unchanged empty
            // delta is a proven no-op and keeps the entries it did not touch.
            (None, None) if changed => DirtySpan::Everything,
            (None, None) => DirtySpan::Nothing,
            // A half hull is not a proof.
            _ => DirtySpan::Everything,
        };
        Some((high, dirty))
    }
}

impl Instance {
    /// Reads the timestamp hull for one table over each region's exclusive lower
    /// sequence and scan-open high watermark. An error means the caller must bypass or
    /// fully evict, never advance a cursor from a partial proof.
    ///
    /// This projection cannot prove the timestamps of erased rows. Callers must bypass
    /// or fully evict when rows may have been removed since `lower`.
    pub async fn probe_sequence_range(
        &self,
        table: &TableName,
        lower: &BTreeMap<u64, u64>,
        query_ctx: &QueryContextRef,
    ) -> Result<(BTreeMap<u64, u64>, Option<Timestamp>, Option<Timestamp>)> {
        if lower.is_empty() {
            return Err(unavailable("no lower region bounds"));
        }

        // Reusing snapshot maps can silently fence the query at an old high or mutate
        // the caller's snapshots. Construct an independent context while retaining the
        // request's identity, options and dialect.
        let options_ctx = query_ctx
            .query_option_snapshot()
            .map_err(|error| unavailable(error.to_string()))?;
        let mut wire_ctx = api::v1::QueryContext::from(&options_ctx);
        wire_ctx.snapshot_seqs = None;
        // A sink-table marker suppresses incremental semantics for that table.
        wire_ctx.extensions.remove(FLOW_SINK_TABLE_ID);
        let mut probe_ctx =
            QueryContext::try_from(wire_ctx).map_err(|error| unavailable(error.to_string()))?;
        probe_ctx.set_current_user(query_ctx.current_user());
        probe_ctx.set_extension(
            FLOW_INCREMENTAL_AFTER_SEQS,
            serde_json::to_string(lower).map_err(|error| unavailable(error.to_string()))?,
        );
        probe_ctx.set_extension(FLOW_INCREMENTAL_MODE, FLOW_INCREMENTAL_MODE_SEQUENCE_RANGE);
        probe_ctx.set_extension(FLOW_RETURN_REGION_SEQ, "true");
        probe_ctx.set_extension(INTERNAL_SEQUENCE_RANGE_PROBE, "true");
        let probe_ctx = Arc::new(probe_ctx);

        let resolved = self
            .catalog_manager
            .table(
                &table.catalog_name,
                &table.schema_name,
                &table.table_name,
                Some(&probe_ctx),
            )
            .await
            .context(CatalogSnafu)?
            .with_context(|| TableNotFoundSnafu {
                table_name: table.to_string(),
            })?;
        let info = resolved.table_info();
        let timestamp = info
            .meta
            .schema
            .timestamp_column()
            .ok_or_else(|| unavailable(format!("table {table} has no time-index column")))?;
        let quote = query_ctx.quote_style();
        // Aggregate in the engine: the probe only ever needs the hull of the
        // incremental delta, and pulling every changed timestamp back to the
        // frontend to fold it row by row turns a cheap probe into a wide scan.
        let quoted_timestamp = Ident::with_quote(quote, &timestamp.name);
        let sql = format!(
            "SELECT MIN({quoted_timestamp}), MAX({quoted_timestamp}) FROM {}.{}.{}",
            Ident::with_quote(quote, &table.catalog_name),
            Ident::with_quote(quote, &table.schema_name),
            Ident::with_quote(quote, &table.table_name),
        );
        let mut statements = parse_stmt(&sql, query_ctx.sql_dialect())?;
        if statements.len() != 1 {
            return Err(unavailable(
                "timestamp aggregate did not parse as one statement",
            ));
        }
        let stmt = QueryStatement::Sql(statements.remove(0));
        let plan = self
            .statement_executor
            .plan(&stmt, probe_ctx.clone())
            .await
            .map_err(BoxedError::new)
            .context(ExternalSnafu)?;
        let output = self
            .statement_executor
            .exec_plan(plan, probe_ctx)
            .await
            .map_err(BoxedError::new)
            .context(ExternalSnafu)?;
        collect_sequence_range(output, lower).await
    }
}

fn unavailable(reason: impl Into<String>) -> Error {
    ExternalSnafu.into_error(BoxedError::new(PlainError::new(
        format!(
            "Sequence-range timestamp probe unavailable: {}",
            reason.into()
        ),
        StatusCode::TableUnavailable,
    )))
}

/// Folds the aggregate's answer into the cursors and the timestamp hull it proved.
async fn collect_sequence_range(
    output: Output,
    lower: &BTreeMap<u64, u64>,
) -> Result<(BTreeMap<u64, u64>, Option<Timestamp>, Option<Timestamp>)> {
    let mut hull = None;
    let mut stream_metrics = None;
    match output.data {
        OutputData::AffectedRows(_) => {
            return Err(unavailable("timestamp aggregate returned affected rows"));
        }
        OutputData::RecordBatches(batches) => {
            for batch in batches.iter() {
                absorb_timestamp_hull(batch, &mut hull)?;
            }
        }
        OutputData::Stream(mut stream) => {
            while let Some(batch) = stream.try_next().await.context(CollectRecordbatchSnafu)? {
                absorb_timestamp_hull(&batch, &mut hull)?;
            }
            stream_metrics = stream.metrics();
        }
    }
    let (min_ts, max_ts) =
        hull.ok_or_else(|| unavailable("timestamp aggregate returned no rows"))?;
    // The stream's terminal metrics contain the same merged proof as the plan when
    // available. Prefer the plan's correctness-first merge; non-stream outputs only
    // expose this plan after execution.
    let metrics = output
        .meta
        .plan
        .and_then(terminal_recordbatch_metrics_from_plan)
        .or(stream_metrics)
        .ok_or_else(|| unavailable("timestamp aggregate returned no region metrics"))?;
    let high = validate_watermarks(lower, &metrics)?;
    Ok((high, min_ts, max_ts))
}

/// Folds the aggregate's one `(MIN, MAX)` row into the hull.
///
/// The aggregate answers exactly one row over its whole output, so a second row, a
/// second batch, or a batch that does not hold the two timestamp columns it was asked
/// for is a failed probe rather than an empty one.
fn absorb_timestamp_hull(
    batch: &RecordBatch,
    hull: &mut Option<(Option<Timestamp>, Option<Timestamp>)>,
) -> Result<()> {
    if batch.num_columns() != 2 {
        return Err(unavailable(
            "timestamp aggregate did not answer two columns",
        ));
    }
    if batch.num_rows() != 1 || hull.is_some() {
        return Err(unavailable(
            "timestamp aggregate answered more than one row",
        ));
    }
    let min = timestamp_or_null(&timestamp_of(batch, 0)?)?;
    let max = timestamp_or_null(&timestamp_of(batch, 1)?)?;
    match (&min, &max) {
        // A MIN above MAX cannot be a hull of any row set, so the pair proves nothing.
        (Some(min), Some(max)) if min > max => {
            return Err(unavailable("timestamp aggregate answered MIN above MAX"));
        }
        // A half hull is not a proof: it says a bound exists without saying which rows
        // lie inside it, so the caller must treat the change as unbounded.
        (Some(_), None) | (None, Some(_)) => {
            return Err(unavailable("timestamp aggregate answered half a hull"));
        }
        _ => {}
    }
    *hull = Some((min, max));
    Ok(())
}

/// The `MIN` or `MAX` value of the aggregate's single row, as a [`Value`].
///
/// A batch hands out arrow arrays, so the cell is read through the datatypes helper that
/// reads a value out of one.
fn timestamp_of(batch: &RecordBatch, index: usize) -> Result<Value> {
    let column = batch.column(index);
    let scalar = ScalarValue::try_from_array(column, 0)
        .map_err(|error| unavailable(format!("the hull cell is unreadable: {error}")))?;
    Value::try_from(scalar)
        .map_err(|error| unavailable(format!("the hull cell is not a value: {error}")))
}

/// The timestamp of one aggregate answer, or `None` for the `NULL` an empty delta folds
/// to.
fn timestamp_or_null(value: &Value) -> Result<Option<Timestamp>> {
    match value {
        Value::Null => Ok(None),
        Value::Timestamp(timestamp) => Ok(Some(*timestamp)),
        other => Err(unavailable(format!(
            "timestamp aggregate answered {other:?} instead of a timestamp"
        ))),
    }
}

/// The per-region watermarks a probe proved, or why they prove nothing.
///
/// The proof has to name every region the probe asked about and no other, and no region
/// may report a watermark below the one it started from: a probe that misses a
/// participating region, adds one, leaves one unproved, or observes a regression cannot
/// be used to advance any cursor.
fn validate_watermarks(
    lower: &BTreeMap<u64, u64>,
    metrics: &RecordBatchMetrics,
) -> Result<BTreeMap<u64, u64>> {
    let mut high = BTreeMap::new();
    for entry in &metrics.region_watermarks {
        let Some(watermark) = entry.watermark else {
            return Err(unavailable(format!(
                "region {} participated but was not proved",
                entry.region_id
            )));
        };
        let Some(start) = lower.get(&entry.region_id) else {
            return Err(unavailable(format!(
                "region {} was not asked about",
                entry.region_id
            )));
        };
        if watermark < *start {
            return Err(unavailable(format!(
                "region {} reported a sequence below the one it started from",
                entry.region_id
            )));
        }
        if high.insert(entry.region_id, watermark).is_some() {
            return Err(unavailable(format!(
                "region {} was reported twice",
                entry.region_id
            )));
        }
    }
    if high.len() != lower.len() {
        return Err(unavailable(
            "the probe did not report every participating region",
        ));
    }
    Ok(high)
}

#[cfg(test)]
mod tests {
    use common_error::ext::ErrorExt;
    use common_recordbatch::RecordBatches;
    use common_recordbatch::adapter::RegionWatermarkEntry;
    use datatypes::prelude::ConcreteDataType;
    use datatypes::schema::{ColumnSchema, Schema};
    use datatypes::vectors::{Int64Vector, NullVector, TimestampMillisecondVector, VectorRef};

    use super::*;

    /// A two-column aggregate answer holding `(MIN, MAX)`.
    fn hull(min: Option<i64>, max: Option<i64>) -> RecordBatch {
        let column = |value: Option<i64>| -> VectorRef {
            match value {
                Some(millis) => Arc::new(TimestampMillisecondVector::from_values([millis])),
                None => Arc::new(NullVector::new(1)),
            }
        };
        let columns = vec![column(min), column(max)];
        let schema = Arc::new(Schema::new(
            columns
                .iter()
                .enumerate()
                .map(|(index, column)| {
                    ColumnSchema::new(format!("c{index}"), column.data_type(), true)
                })
                .collect::<Vec<_>>(),
        ));
        RecordBatch::new(schema, columns).unwrap()
    }

    /// An aggregate answer whose columns are integers rather than timestamps.
    fn non_timestamp_hull() -> RecordBatch {
        let columns: Vec<VectorRef> = vec![
            Arc::new(Int64Vector::from_vec(vec![1000])),
            Arc::new(Int64Vector::from_vec(vec![2000])),
        ];
        let schema = Arc::new(Schema::new(vec![
            ColumnSchema::new("c0", ConcreteDataType::int64_datatype(), true),
            ColumnSchema::new("c1", ConcreteDataType::int64_datatype(), true),
        ]));
        RecordBatch::new(schema, columns).unwrap()
    }

    /// An aggregate answer of `columns` columns over `rows`, one row per inner slice.
    fn timestamp_batch(columns: usize, rows: &[Vec<Option<i64>>]) -> RecordBatch {
        let vectors: Vec<VectorRef> = (0..columns)
            .map(|index| {
                let values = rows
                    .iter()
                    .map(|row| row[index].expect("the test rows hold a timestamp"))
                    .collect::<Vec<_>>();
                Arc::new(TimestampMillisecondVector::from_values(values)) as VectorRef
            })
            .collect();
        let schema = Arc::new(Schema::new(
            (0..columns)
                .map(|index| {
                    ColumnSchema::new(
                        format!("c{index}"),
                        ConcreteDataType::timestamp_millisecond_datatype(),
                        true,
                    )
                })
                .collect::<Vec<_>>(),
        ));
        RecordBatch::new(schema, vectors).unwrap()
    }

    /// The per-region watermarks a probe reports.
    fn metrics(entries: &[(u64, Option<u64>)]) -> RecordBatchMetrics {
        RecordBatchMetrics {
            region_watermarks: entries
                .iter()
                .map(|(region_id, watermark)| RegionWatermarkEntry {
                    region_id: *region_id,
                    watermark: *watermark,
                })
                .collect(),
            ..Default::default()
        }
    }

    /// The hull `batches` fold into, or why they fold into nothing.
    fn fold(batches: &[RecordBatch]) -> Result<(Option<Timestamp>, Option<Timestamp>)> {
        let mut hull = None;
        for batch in batches {
            absorb_timestamp_hull(batch, &mut hull)?;
        }
        hull.ok_or_else(|| unavailable("timestamp aggregate returned no rows"))
    }

    #[test]
    fn sequence_range_folds_min_and_max_into_a_hull() {
        assert_eq!(
            fold(&[hull(Some(1000), Some(3000))]).unwrap(),
            (
                Some(Timestamp::new_millisecond(1000)),
                Some(Timestamp::new_millisecond(3000))
            )
        );
        // An empty delta aggregates to a single `(NULL, NULL)` row, which is a proven
        // no-op rather than an unproved hull.
        assert_eq!(fold(&[hull(None, None)]).unwrap(), (None, None));
    }

    #[test]
    fn sequence_range_rejects_half_null_inverted_and_non_timestamp_hulls() {
        for batch in [
            hull(Some(1000), None),
            hull(None, Some(3000)),
            // MIN above MAX cannot be a hull of any row set.
            hull(Some(3000), Some(1000)),
            non_timestamp_hull(),
        ] {
            assert!(
                fold(&[batch]).is_err(),
                "the batch should not fold into a hull"
            );
        }
    }

    #[test]
    fn sequence_range_rejects_wrong_arity_and_row_count() {
        for batches in [
            // One column.
            vec![timestamp_batch(1, &[vec![Some(1000)]])],
            // Three columns.
            vec![timestamp_batch(
                3,
                &[vec![Some(1000), Some(1000), Some(2000)]],
            )],
            // No row at all: the aggregate always emits one.
            vec![timestamp_batch(2, &[])],
            // Two rows in one batch.
            vec![timestamp_batch(
                2,
                &[vec![Some(1000), Some(1000)], vec![Some(2000), Some(2000)]],
            )],
            // One row split over two batches is still two rows.
            vec![hull(Some(1000), Some(1000)), hull(Some(2000), Some(2000))],
        ] {
            assert!(
                fold(&batches).is_err(),
                "the batches should not fold into a hull"
            );
        }
    }

    #[test]
    fn sequence_range_rejects_missing_unproved_regressing_and_extra_watermarks() {
        let lower = BTreeMap::from([(1, 5), (2, 10)]);
        for entries in [
            vec![],
            vec![(1, Some(5))],
            vec![(1, Some(5)), (2, None)],
            vec![(1, Some(4)), (2, Some(10))],
            vec![(1, Some(5)), (2, Some(10)), (3, Some(20))],
            vec![(1, Some(5)), (1, Some(6)), (2, Some(10))],
        ] {
            assert!(
                validate_watermarks(&lower, &metrics(&entries)).is_err(),
                "{entries:?}"
            );
        }
        // A probe that saw nothing new proves exactly the cursors it started from.
        assert_eq!(
            validate_watermarks(&lower, &metrics(&[(1, Some(5)), (2, Some(10))])).unwrap(),
            lower
        );
        assert_eq!(
            validate_watermarks(&lower, &metrics(&[(1, Some(6)), (2, Some(12))])).unwrap(),
            BTreeMap::from([(1, 6), (2, 12)])
        );
    }

    #[tokio::test]
    async fn sequence_range_rejects_missing_metrics_and_affected_rows() {
        let lower = BTreeMap::from([(1, 0)]);
        assert_eq!(
            collect_sequence_range(Output::new_with_affected_rows(1), &lower)
                .await
                .unwrap_err()
                .status_code(),
            StatusCode::TableUnavailable
        );
        // A well-formed hull without terminal region metrics proves nothing about which
        // regions participated.
        let batch = hull(Some(1000), Some(3000));
        let schema = batch.schema.clone();
        let batches = RecordBatches::try_new(schema, vec![batch]).unwrap();
        assert_eq!(
            collect_sequence_range(Output::new_with_record_batches(batches), &lower)
                .await
                .unwrap_err()
                .status_code(),
            StatusCode::TableUnavailable
        );
    }
}
