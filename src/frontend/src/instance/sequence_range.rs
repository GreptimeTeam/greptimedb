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
use std::sync::Arc;

use common_error::ext::{BoxedError, PlainError};
use common_error::status_code::StatusCode;
use common_query::{Output, OutputData};
use common_recordbatch::RecordBatch;
use common_recordbatch::adapter::RecordBatchMetrics;
use common_time::{Timestamp, timestamp::TimeUnit};
use datatypes::vectors::Helper;
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
use crate::query_cache::TimeChunk;

/// The sequence cursor frontend has already invalidated the rollup cache up to.
///
/// The route version is part of the state: a repartition changes which regions
/// participate, so an old per-region cursor is not comparable and the table is
/// evicted before a fresh scan.
#[derive(Debug, Clone, Default)]
pub(crate) struct SourceProbeState {
    route_version: u64,
    cursors: BTreeMap<u64, u64>,
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

impl Instance {
    /// Reads the timestamp hull for one table over each region's exclusive lower
    /// sequence and scan-open high watermark. An error means the caller must
    /// bypass or fully evict, never advance a cursor from a partial proof.
    ///
    /// This projection cannot prove the timestamps of erased rows. Callers must
    /// bypass or fully evict when rows may have been removed since `lower`.
    pub async fn probe_sequence_range(
        &self,
        table: &TableName,
        lower: &BTreeMap<u64, u64>,
        query_ctx: &QueryContextRef,
    ) -> Result<(BTreeMap<u64, u64>, Option<Timestamp>, Option<Timestamp>)> {
        if lower.is_empty() {
            return Err(unavailable("no lower region bounds"));
        }

        // Reusing snapshot maps can silently fence the query at an old high or
        // mutate the caller's snapshots. Construct an independent context while
        // retaining the request's identity, options and dialect.
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
        let sql = format!(
            "SELECT {} FROM {}.{}.{}",
            Ident::with_quote(quote, &timestamp.name),
            Ident::with_quote(quote, &table.catalog_name),
            Ident::with_quote(quote, &table.schema_name),
            Ident::with_quote(quote, &table.table_name),
        );
        let mut statements = parse_stmt(&sql, query_ctx.sql_dialect())?;
        if statements.len() != 1 {
            return Err(unavailable(
                "timestamp projection did not parse as one statement",
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

    /// Refreshes the rollup cache's view of one source table.
    ///
    /// The cache is invalidated *before* the per-region cursor advances. A probe
    /// that cannot prove every participating region, or that advances a cursor
    /// without a timestamp hull, evicts the whole table: a removed row may have
    /// advanced the sequence without leaving a timestamp to invalidate by. A put
    /// that replaces a row at the same timestamp is covered by the hull; an
    /// explicit DELETE whose timestamp the ordinary scan hides is not, which is
    /// why a DELETE-only advance evicts the whole table. `false` means the caller
    /// must bypass the cache, and no cursor has advanced.
    pub(crate) async fn refresh_cache_source(
        &self,
        table: &TableName,
        source_table: &str,
        query_ctx: &QueryContextRef,
    ) -> Result<bool> {
        let Some(cache) = self.rollup_cache.as_ref() else {
            return Ok(false);
        };
        let resolved = self
            .catalog_manager
            .table(
                &table.catalog_name,
                &table.schema_name,
                &table.table_name,
                Some(query_ctx),
            )
            .await
            .context(CatalogSnafu)?
            .with_context(|| TableNotFoundSnafu {
                table_name: table.to_string(),
            })?;
        let table_info = resolved.table_info();
        let table_id = table_info.table_id();
        let (_physical_table_id, route) = self
            .partition_manager()
            .find_physical_table_route_with_id(table_id)
            .await
            .map_err(BoxedError::new)
            .context(ExternalSnafu)?;
        let version = route.version();
        let regions = route
            .region_routes
            .iter()
            // The probe scans the queryable table: a metric logical table owns the
            // logical region IDs even though its route points at a physical table.
            .map(|route| RegionId::new(table_id, route.region.id.region_number()).as_u64())
            .collect::<Vec<_>>();
        if regions.is_empty() {
            return Ok(false);
        }

        let (lower, route_changed) = {
            let state = self.source_probe_cursors.lock().unwrap();
            // Cursors are per logical table: metric-engine logical tables share a
            // physical region sequence, and one logical table's scan must not
            // advance another's lower bound past rows it has not read.
            match state.get(&table_id) {
                Some(stored) if stored.route_version == version => (
                    regions
                        .iter()
                        .map(|region| (*region, stored.cursors.get(region).copied().unwrap_or(0)))
                        .collect::<BTreeMap<_, _>>(),
                    false,
                ),
                Some(_) => (regions.iter().map(|region| (*region, 0)).collect(), true),
                None => (regions.iter().map(|region| (*region, 0)).collect(), false),
            }
        };
        // A route change makes every old entry's input regions incomparable. Evict
        // before reading or advancing anything from the new route.
        if route_changed {
            cache.invalidate_table(source_table);
        }

        let (high, min, max) = match self.probe_sequence_range(table, &lower, query_ctx).await {
            Ok(probe) => probe,
            Err(_) => return Ok(false),
        };
        if high.len() != regions.len()
            || high.keys().any(|region| !lower.contains_key(region))
            || high
                .iter()
                .any(|(region, watermark)| watermark < &lower[region])
        {
            return Ok(false);
        }
        let changed = high != lower;
        match (min, max) {
            (Some(min), Some(max)) => {
                let Some(min_ms) = min.convert_to(TimeUnit::Millisecond) else {
                    return Ok(false);
                };
                let Some(max_ms) = max.convert_to(TimeUnit::Millisecond) else {
                    return Ok(false);
                };
                // Round outward: a sub-millisecond timestamp still lies inside the
                // span, and an extra millisecond only costs a later miss. A put that
                // overwrites a row at the same timestamp is caught by the same span;
                // an explicit DELETE whose timestamp the ordinary scan hides is the
                // one operation a timestamp hull cannot represent.
                let span = TimeChunk::new(
                    min_ms.value().saturating_sub(1),
                    max_ms.value().saturating_add(1),
                );
                cache.invalidate_table_range(source_table, span);
            }
            // A sequence advance without a timestamp can mean erased rows. Do not
            // guess a span: no entry of this table may survive.
            (None, None) if changed => {
                cache.invalidate_table(source_table);
            }
            // A half hull is not a proof.
            _ => {
                cache.invalidate_table(source_table);
            }
        }

        let mut state = self.source_probe_cursors.lock().unwrap();
        match state.get_mut(&table_id) {
            Some(stored) if stored.route_version > version => {
                // A newer repartition won the race; do not move its cursor back.
            }
            Some(stored) if stored.route_version == version => {
                for (region, watermark) in high {
                    stored
                        .cursors
                        .entry(region)
                        .and_modify(|cursor| *cursor = (*cursor).max(watermark))
                        .or_insert(watermark);
                }
            }
            _ => {
                state.insert(
                    table_id,
                    SourceProbeState {
                        route_version: version,
                        cursors: high,
                    },
                );
            }
        }
        Ok(true)
    }
}

async fn collect_sequence_range(
    output: Output,
    lower: &BTreeMap<u64, u64>,
) -> Result<(BTreeMap<u64, u64>, Option<Timestamp>, Option<Timestamp>)> {
    let mut min_ts = None;
    let mut max_ts = None;
    let mut stream_metrics = None;
    match output.data {
        OutputData::AffectedRows(_) => {
            return Err(unavailable("timestamp projection returned affected rows"));
        }
        OutputData::RecordBatches(batches) => {
            for batch in batches.iter() {
                update_timestamp_range(batch, &mut min_ts, &mut max_ts)?;
            }
        }
        OutputData::Stream(mut stream) => {
            while let Some(batch) = stream.try_next().await.context(CollectRecordbatchSnafu)? {
                update_timestamp_range(&batch, &mut min_ts, &mut max_ts)?;
            }
            stream_metrics = stream.metrics();
        }
    }
    // The stream's terminal metrics contain the same merged proof as the plan
    // when available. Prefer the plan's correctness-first merge; non-stream
    // outputs only expose this plan after execution.
    let metrics = output
        .meta
        .plan
        .and_then(terminal_recordbatch_metrics_from_plan)
        .or(stream_metrics)
        .ok_or_else(|| unavailable("missing terminal region metrics"))?;
    let high = validate_watermarks(lower, metrics)?;
    Ok((high, min_ts, max_ts))
}

fn update_timestamp_range(
    batch: &RecordBatch,
    min_ts: &mut Option<Timestamp>,
    max_ts: &mut Option<Timestamp>,
) -> Result<()> {
    if batch.num_columns() != 1 {
        return Err(unavailable(format!(
            "timestamp projection returned {} columns",
            batch.num_columns()
        )));
    }
    let values = Helper::try_into_vector(batch.column(0).clone())
        .map_err(BoxedError::new)
        .context(ExternalSnafu)?;
    for index in 0..values.len() {
        let timestamp = values.get(index).as_timestamp().ok_or_else(|| {
            unavailable("timestamp projection returned a null or non-timestamp value")
        })?;
        *min_ts = Some(min_ts.map_or(timestamp, |value| value.min(timestamp)));
        *max_ts = Some(max_ts.map_or(timestamp, |value| value.max(timestamp)));
    }
    Ok(())
}

fn validate_watermarks(
    lower: &BTreeMap<u64, u64>,
    metrics: RecordBatchMetrics,
) -> Result<BTreeMap<u64, u64>> {
    let mut high = BTreeMap::new();
    for entry in metrics.region_watermarks {
        let Some(low) = lower.get(&entry.region_id) else {
            return Err(unavailable(format!(
                "unexpected participating region {}",
                entry.region_id
            )));
        };
        let Some(watermark) = entry.watermark else {
            return Err(unavailable(format!(
                "unproved watermark for region {}",
                entry.region_id
            )));
        };
        if watermark < *low || high.insert(entry.region_id, watermark).is_some() {
            return Err(unavailable(format!(
                "regressing or duplicate watermark for region {}",
                entry.region_id
            )));
        }
    }
    if high.len() != lower.len() || high.is_empty() {
        return Err(unavailable(format!(
            "incomplete watermark proof: lower={lower:?}, high={high:?}"
        )));
    }
    Ok(high)
}

#[cfg(test)]
mod tests {
    use std::pin::Pin;
    use std::task::{Context, Poll};

    use common_error::ext::ErrorExt;
    use common_query::OutputMeta;
    use common_recordbatch::adapter::RegionWatermarkEntry;
    use common_recordbatch::{OrderOption, RecordBatchStream};
    use datatypes::prelude::ConcreteDataType;
    use datatypes::schema::{ColumnSchema, Schema, SchemaRef};
    use datatypes::vectors::{TimestampMillisecondVector, VectorRef};
    use futures::Stream;

    use super::*;

    fn batch(timestamps: Vec<i64>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            ColumnSchema::new(
                "observed_at",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            )
            .with_time_index(true),
        ]));
        RecordBatch::new(
            schema,
            vec![Arc::new(TimestampMillisecondVector::from_vec(timestamps)) as VectorRef],
        )
        .unwrap()
    }

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

    struct TerminalStream {
        batch: Option<RecordBatch>,
        schema: SchemaRef,
        metrics: RecordBatchMetrics,
        done: bool,
    }

    impl Stream for TerminalStream {
        type Item = common_recordbatch::error::Result<RecordBatch>;

        fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            match self.batch.take() {
                Some(batch) => Poll::Ready(Some(Ok(batch))),
                None => {
                    self.done = true;
                    Poll::Ready(None)
                }
            }
        }
    }

    impl RecordBatchStream for TerminalStream {
        fn name(&self) -> &str {
            "probe-test"
        }
        fn schema(&self) -> SchemaRef {
            self.schema.clone()
        }
        fn output_ordering(&self) -> Option<&[OrderOption]> {
            None
        }
        fn metrics(&self) -> Option<RecordBatchMetrics> {
            self.done.then(|| self.metrics.clone())
        }
    }

    fn output(timestamps: Vec<i64>, metrics: RecordBatchMetrics) -> Output {
        let batch = batch(timestamps);
        let schema = batch.schema.clone();
        Output::new(
            OutputData::Stream(Box::pin(TerminalStream {
                batch: Some(batch),
                schema,
                metrics,
                done: false,
            })),
            OutputMeta::default(),
        )
    }

    #[tokio::test]
    async fn sequence_range_collects_stream_and_terminal_watermarks() {
        let lower = BTreeMap::from([(1, 0), (2, 10)]);
        let result = collect_sequence_range(
            output(
                vec![3000, -1000, 2000],
                metrics(&[(1, Some(4)), (2, Some(10))]),
            ),
            &lower,
        )
        .await
        .unwrap();
        assert_eq!(
            result,
            (
                BTreeMap::from([(1, 4), (2, 10)]),
                Some(Timestamp::new_millisecond(-1000)),
                Some(Timestamp::new_millisecond(3000))
            )
        );
    }

    #[tokio::test]
    async fn sequence_range_empty_rows_keep_proved_high() {
        let lower = BTreeMap::from([(1, 7)]);
        let result = collect_sequence_range(output(vec![], metrics(&[(1, Some(7))])), &lower)
            .await
            .unwrap();
        assert_eq!(result, (lower, None, None));
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
            let err = validate_watermarks(&lower, metrics(&entries)).unwrap_err();
            assert_eq!(
                err.status_code(),
                StatusCode::TableUnavailable,
                "{entries:?}"
            );
        }
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
        let schema = batch(vec![]).schema.clone();
        let batches = common_recordbatch::RecordBatches::try_new(schema, vec![]).unwrap();
        assert_eq!(
            collect_sequence_range(Output::new_with_record_batches(batches), &lower)
                .await
                .unwrap_err()
                .status_code(),
            StatusCode::TableUnavailable
        );
    }
}
