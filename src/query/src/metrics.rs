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

use std::collections::{BTreeMap, BTreeSet};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use common_recordbatch::adapter::{
    ObservedPartitionExpr, RecordBatchMetrics, RegionWatermarkEntry, SubstitutedRange,
    SubstitutedRowRanges,
};
use common_recordbatch::{OrderOption, RecordBatch, RecordBatchStream, SendableRecordBatchStream};
use common_telemetry::warn;
use datafusion::physical_plan::ExecutionPlan;
use datatypes::schema::SchemaRef;
use futures::Stream;
use futures_util::ready;
use lazy_static::lazy_static;
use prometheus::*;
use session::context::QueryContextRef;

use table::table::scan::RegionScanExec;

use crate::dist_plan::MergeScanExec;
use crate::error::Result;
use crate::options::FlowQueryExtensions;

/// The key one terminal watermark statement is merged under: the region that
/// allocated the sequences plus the identity of the partition expression the
/// statement is about (`None` for the region's complete value domain).
///
/// Two statements about the same region but different expressions are statements
/// about different value domains: a round that read one slice of a region says
/// nothing about the other slices, so they must not be merged into one entry.
type WatermarkKey = (u64, Option<String>);

/// Intermediate merge state for one participating `(region, expression)` while
/// collecting terminal correctness watermarks across merge-scan sub-stages.
enum MergeState {
    /// At least one branch reported that this key cannot prove a safe
    /// checkpoint watermark for the current query round.
    Unproved,
    /// All seen branches agree the domain can advance safely to this sequence.
    Proved(u64),
    /// Different proved sequences were reported for the same key. The final
    /// result is degraded to `None`, and the collected values are logged.
    Conflict {
        /// Distinct proved watermark candidates reported for the key.
        watermarks: Vec<u64>,
    },
}

lazy_static! {
    /// Timer of different stages in query.
    pub static ref QUERY_STAGE_ELAPSED: HistogramVec = register_histogram_vec!(
        "greptime_query_stage_elapsed",
        "query engine time elapsed during each stage",
        &["stage"],
        vec![0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0, 60.0, 300.0]
    )
    .unwrap();
    pub static ref PARSE_SQL_ELAPSED: Histogram = QUERY_STAGE_ELAPSED
        .with_label_values(&["parse_sql"]);
    pub static ref PARSE_PROMQL_ELAPSED: Histogram = QUERY_STAGE_ELAPSED
        .with_label_values(&["parse_promql"]);
    pub static ref OPTIMIZE_LOGICAL_ELAPSED: Histogram = QUERY_STAGE_ELAPSED
        .with_label_values(&["optimize_logicalplan"]);
    pub static ref OPTIMIZE_PHYSICAL_ELAPSED: Histogram = QUERY_STAGE_ELAPSED
        .with_label_values(&["optimize_physicalplan"]);
    pub static ref CREATE_PHYSICAL_ELAPSED: Histogram = QUERY_STAGE_ELAPSED
        .with_label_values(&["create_physicalplan"]);
    pub static ref EXEC_PLAN_ELAPSED: Histogram = QUERY_STAGE_ELAPSED
        .with_label_values(&["execute_plan"]);
    pub static ref MERGE_SCAN_POLL_ELAPSED: Histogram = QUERY_STAGE_ELAPSED
        .with_label_values(&["merge_scan_poll"]);

    pub static ref MERGE_SCAN_REGIONS: Histogram = register_histogram!(
        "greptime_query_merge_scan_regions",
        "query merge scan regions"
    )
    .unwrap();
    pub static ref MERGE_SCAN_ERRORS_TOTAL: IntCounter = register_int_counter!(
        "greptime_query_merge_scan_errors_total",
        "query merge scan errors total"
    )
    .unwrap();
    pub static ref PUSH_DOWN_FALLBACK_ERRORS_TOTAL: IntCounter = register_int_counter!(
        "greptime_push_down_fallback_errors_total",
        "query push down fallback errors total"
    )
    .unwrap();

    pub static ref QUERY_MEMORY_POOL_USAGE_BYTES: IntGauge = register_int_gauge!(
        "greptime_query_memory_pool_usage_bytes",
        "current query memory pool usage in bytes"
    )
    .unwrap();

    pub static ref QUERY_MEMORY_POOL_REJECTED_TOTAL: IntCounter = register_int_counter!(
        "greptime_query_memory_pool_rejected_total",
        "total number of query memory allocations rejected"
    )
    .unwrap();

    /// Remote dynamic filter fanout RPC results, labeled with status.
    pub static ref REMOTE_DYN_FILTER_UPDATE_RPC_TOTAL: IntCounterVec = register_int_counter_vec!(
        "greptime_query_remote_dyn_filter_update_rpc_total",
        "remote dynamic filter fanout RPC results",
        &["status"]
    )
    .unwrap();

    /// Remote dynamic filter fanout payload bytes.
    pub static ref REMOTE_DYN_FILTER_PAYLOAD_BYTES: Histogram = register_histogram!(
        "greptime_query_remote_dyn_filter_payload_bytes",
        "remote dynamic filter fanout payload bytes",
        vec![
            128.0, 256.0, 512.0, 1024.0, 2048.0, 4096.0, 8192.0, 16384.0, 32768.0,
            65536.0, 131072.0, 262144.0, 524288.0,
        ]
    )
    .unwrap();

    /// Remote dynamic filter encode results, labeled with result.
    pub static ref REMOTE_DYN_FILTER_ENCODE_TOTAL: IntCounterVec = register_int_counter_vec!(
        "greptime_query_remote_dyn_filter_encode_total",
        "remote dynamic filter encode results",
        &["result"]
    )
    .unwrap();
}

/// A stream to call the callback once a RecordBatch stream is done.
pub struct OnDone<F> {
    stream: SendableRecordBatchStream,
    callback: Option<F>,
}

impl<F> OnDone<F> {
    /// Attaches a `callback` to invoke once the `stream` is terminated.
    pub fn new(stream: SendableRecordBatchStream, callback: F) -> Self {
        Self {
            stream,
            callback: Some(callback),
        }
    }
}

impl<F: FnOnce() + Unpin> RecordBatchStream for OnDone<F> {
    fn name(&self) -> &str {
        self.stream.name()
    }

    fn schema(&self) -> SchemaRef {
        self.stream.schema()
    }

    fn output_ordering(&self) -> Option<&[OrderOption]> {
        self.stream.output_ordering()
    }

    fn metrics(&self) -> Option<RecordBatchMetrics> {
        self.stream.metrics()
    }
}

impl<F: FnOnce() + Unpin> Stream for OnDone<F> {
    type Item = common_recordbatch::error::Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match ready!(Pin::new(&mut self.stream).poll_next(cx)) {
            Some(rb) => Poll::Ready(Some(rb)),
            None => {
                if let Some(callback) = self.callback.take() {
                    callback();
                }
                Poll::Ready(None)
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.stream.size_hint()
    }
}

pub struct RegionWatermarkMetricsStream {
    stream: SendableRecordBatchStream,
    plan: Arc<dyn ExecutionPlan>,
}

impl RegionWatermarkMetricsStream {
    pub fn new(stream: SendableRecordBatchStream, plan: Arc<dyn ExecutionPlan>) -> Self {
        Self { stream, plan }
    }
}

impl RecordBatchStream for RegionWatermarkMetricsStream {
    fn name(&self) -> &str {
        self.stream.name()
    }

    fn schema(&self) -> SchemaRef {
        self.stream.schema()
    }

    fn output_ordering(&self) -> Option<&[OrderOption]> {
        self.stream.output_ordering()
    }

    fn metrics(&self) -> Option<RecordBatchMetrics> {
        let mut metrics = self.stream.metrics()?;
        let region_watermarks = collect_region_watermarks(self.plan.clone());
        if !region_watermarks.is_empty() {
            metrics.region_watermarks = region_watermarks;
        }
        // A region scan reports the rows it substituted (adopted files whose sequences a
        // substitution produced) through its own stream metrics, which a merge-scan
        // sub-stage keeps per region. Both sources are joined here so the caller sees one
        // range set per region.
        let substituted_row_ranges = collect_substituted_row_ranges(self.plan.clone(), &metrics);
        metrics.substituted_row_ranges = substituted_row_ranges;
        // Which `(region, partition expression)` pairs the regions actually read travel with
        // the same result: the owner of a `(region, slice)` cursor splits and advances its
        // entries by exactly what this round observed.
        metrics.observed_slices = collect_observed_slices(self.plan.clone(), &metrics);
        // Which other regions' files the regions delivered rows from. A caller that must not
        // count a row above its creator's proved watermark cannot recover this from the
        // watermarks: they say what was proved, not what was read.
        metrics.foreign_file_row_creators =
            collect_foreign_file_row_creators(self.plan.clone(), &metrics);
        Some(metrics)
    }
}

impl Stream for RegionWatermarkMetricsStream {
    type Item = common_recordbatch::error::Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.stream).poll_next(cx)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.stream.size_hint()
    }
}

/// Returns whether terminal region watermark metrics should be collected for the query context.
pub fn should_collect_region_watermark_from_query_ctx(query_ctx: &QueryContextRef) -> Result<bool> {
    Ok(
        FlowQueryExtensions::parse_flow_extensions(&query_ctx.extensions())?
            .is_some_and(|extensions| extensions.should_collect_region_watermark()),
    )
}

/// Attaches terminal region watermark metrics to `stream` when collection is requested.
pub fn maybe_attach_region_watermark_metrics(
    stream: SendableRecordBatchStream,
    plan: Arc<dyn ExecutionPlan>,
    should_collect_region_watermark: bool,
) -> SendableRecordBatchStream {
    if should_collect_region_watermark {
        Box::pin(RegionWatermarkMetricsStream::new(stream, plan))
    } else {
        stream
    }
}

pub fn terminal_recordbatch_metrics_from_plan(
    plan: Arc<dyn ExecutionPlan>,
) -> Option<RecordBatchMetrics> {
    let region_watermarks = collect_region_watermarks(plan.clone());
    let own = RecordBatchMetrics::default();
    let substituted_row_ranges = collect_substituted_row_ranges(plan.clone(), &own);
    let observed_slices = collect_observed_slices(plan.clone(), &own);
    let foreign_file_row_creators = collect_foreign_file_row_creators(plan.clone(), &own);
    if region_watermarks.is_empty()
        && substituted_row_ranges.is_empty()
        && observed_slices.is_empty()
        && foreign_file_row_creators.is_none()
    {
        None
    } else {
        Some(RecordBatchMetrics {
            region_watermarks,
            substituted_row_ranges,
            observed_slices,
            foreign_file_row_creators,
            ..Default::default()
        })
    }
}

/// Collects terminal record-batch metrics from `plan` only when requested.
pub fn terminal_recordbatch_metrics_from_plan_if_requested(
    plan: Option<Arc<dyn ExecutionPlan>>,
    should_collect_region_watermark: bool,
) -> Option<RecordBatchMetrics> {
    if should_collect_region_watermark {
        plan.and_then(terminal_recordbatch_metrics_from_plan)
    } else {
        None
    }
}

/// Collects the proved terminal watermarks of `plan`, one entry per
/// `(region, partition expression)` the plan's scans stated.
fn collect_region_watermarks(plan: Arc<dyn ExecutionPlan>) -> Vec<RegionWatermarkEntry> {
    let mut merged = BTreeMap::<WatermarkKey, MergeState>::new();
    let mut stack = vec![plan];

    while let Some(plan) = stack.pop() {
        if let Some(merge_scan) = plan.downcast_ref::<MergeScanExec>()
            && !merge_scan.is_flow_sink_scan()
        {
            merge_merge_scan_region_watermarks(
                &mut merged,
                merge_scan
                    .regions()
                    .iter()
                    .map(|region_id| region_id.as_u64()),
                merge_scan.sub_stage_metrics(),
            );
        }
        stack.extend(plan.children().into_iter().cloned());
    }

    finalize_region_watermarks(merged)
}

/// Collects the time ranges of the rows the plan's region scans substituted.
///
/// `own` carries the ranges of the stream that already reported them (a local region
/// scan in standalone mode), and every merge-scan sub-stage contributes the ranges of
/// its own regions. Ranges are unioned per region, so a row substituted by two branches
/// is reported once.
fn collect_substituted_row_ranges(
    plan: Arc<dyn ExecutionPlan>,
    own: &RecordBatchMetrics,
) -> Vec<SubstitutedRowRanges> {
    let mut merged = BTreeMap::<u64, BTreeSet<(i64, i64)>>::new();
    let mut merge = |entries: Vec<SubstitutedRowRanges>| {
        for entry in entries {
            let ranges = merged.entry(entry.region_id).or_default();
            ranges.extend(
                entry
                    .ranges
                    .into_iter()
                    .map(|range| (range.start_millis, range.end_millis)),
            );
        }
    };
    merge(own.substituted_row_ranges.clone());

    let mut stack = vec![plan];
    while let Some(plan) = stack.pop() {
        if let Some(merge_scan) = plan.downcast_ref::<MergeScanExec>()
            && !merge_scan.is_flow_sink_scan()
        {
            for metrics in merge_scan.sub_stage_metrics() {
                merge(metrics.substituted_row_ranges);
            }
        }
        stack.extend(plan.children().into_iter().cloned());
    }

    merged
        .into_iter()
        .map(|(region_id, ranges)| SubstitutedRowRanges {
            region_id,
            ranges: ranges
                .into_iter()
                .map(|(start_millis, end_millis)| SubstitutedRange {
                    start_millis,
                    end_millis,
                })
                .collect(),
        })
        .collect()
}

/// Collects the `(region, partition expression)` pairs the plan's region scans read.
///
/// `own` carries the slices of the stream that already reported them (a local region scan
/// in standalone mode), and every merge-scan sub-stage contributes the slices of its own
/// regions. A slice reported by two branches is one slice, because it states what was read,
/// not how often.
fn collect_observed_slices(
    plan: Arc<dyn ExecutionPlan>,
    own: &RecordBatchMetrics,
) -> Vec<ObservedPartitionExpr> {
    let mut merged = BTreeSet::new();
    merged.extend(own.observed_slices.iter().cloned());

    let mut stack = vec![plan];
    while let Some(plan) = stack.pop() {
        if let Some(merge_scan) = plan.downcast_ref::<MergeScanExec>()
            && !merge_scan.is_flow_sink_scan()
        {
            for metrics in merge_scan.sub_stage_metrics() {
                merged.extend(metrics.observed_slices);
            }
        }
        stack.extend(plan.children().into_iter().cloned());
    }

    merged.into_iter().collect()
}

/// Collects the creator regions of the adopted files the plan's region scans delivered rows
/// from (`RecordBatchMetrics::foreign_file_row_creators`).
///
/// The answer is `Some` only when **every** region scan that could have read a file stated
/// what it delivered, and the region set is then the union of their answers. A scan that
/// reported nothing — a build that predates the field, a region whose metrics never arrived,
/// or a plan node that never ended its stream — latches the whole answer to `None`, because an
/// answer covering part of the read must not claim that nothing came from another region.
/// `Some(vec![])` is an affirmative "delivered none" and survives the merge as the empty set.
fn collect_foreign_file_row_creators(
    plan: Arc<dyn ExecutionPlan>,
    own: &RecordBatchMetrics,
) -> Option<Vec<u64>> {
    // `own` contributes when it states an answer, and is otherwise the absence of one this
    // function cannot interpret: the stream of a distributed plan reports plan-level metrics
    // and never a region's read, so its silence is not a scan that failed to report. A stream
    // that *is* a region scan has a merge scan in the plan only when the plan reads a table,
    // and then that region answers through the merge scan's sub-stage metrics.
    let mut merged = BTreeSet::new();
    let mut conclusive = false;
    let mut unknown = false;
    if let Some(creators) = own.foreign_file_row_creators.as_deref() {
        conclusive = true;
        merged.extend(creators.iter().copied());
    }

    let mut stack = vec![plan];
    while let Some(plan) = stack.pop() {
        // A region scan keeps the answer its partition streams stated when they ended, which
        // is the only source of it once the stream of a plan that ran in this process is
        // rebuilt from the plan's figures. Its silence is a scan that did not report, so it
        // latches the answer unknown.
        if let Some(region_scan) = plan.downcast_ref::<RegionScanExec>() {
            let creators = region_scan.foreign_file_row_creators();
            merge_foreign_file_row_creators(
                creators.as_deref(),
                &mut merged,
                &mut conclusive,
                &mut unknown,
            );
        }
        if let Some(merge_scan) = plan.downcast_ref::<MergeScanExec>()
            && !merge_scan.is_flow_sink_scan()
        {
            let sub_stage_metrics = merge_scan.sub_stage_metrics();
            // A region without sub-stage metrics never reported, so the read over it is
            // unknown even when every other region stated its answer.
            if sub_stage_metrics.len() != merge_scan.regions().len() {
                unknown = true;
            }
            for metrics in sub_stage_metrics {
                merge_foreign_file_row_creators(
                    metrics.foreign_file_row_creators.as_deref(),
                    &mut merged,
                    &mut conclusive,
                    &mut unknown,
                );
            }
        }
        stack.extend(plan.children().into_iter().cloned());
    }

    if unknown || !conclusive {
        None
    } else {
        Some(merged.into_iter().collect())
    }
}

/// Merges one scan's answer into the plan-wide one.
///
/// `Some` is an answer (it accumulates and makes the merged result conclusive); `None` is the
/// absence of one and latches the merged result to unknown.
fn merge_foreign_file_row_creators(
    creators: Option<&[u64]>,
    merged: &mut BTreeSet<u64>,
    conclusive: &mut bool,
    unknown: &mut bool,
) {
    match creators {
        Some(creators) => {
            *conclusive = true;
            merged.extend(creators.iter().copied());
        }
        None => *unknown = true,
    }
}

/// Merge a batch of watermark entries into the global merged state.
///
/// Entries are merged per `(region, partition expression)` ([`WatermarkKey`]): an
/// entry about one expression slice of a region is a statement about that slice only,
/// and letting it share a key with the region's whole-domain entry would either
/// advance a slice the round never read or forget that the region was opened.
///
/// # Merge strategy: correctness over maximum
///
/// Flow checkpoint advancement requires provable watermarks so that incremental
/// queries never miss rows. This merge uses correctness-first semantics:
///
/// | Current state  | New entry       | Result            | Rationale |
/// |---------------|-----------------|-------------------|-----------|
/// | Proved(old)   | Proved(same)    | Proved(old)       | Convergent proof, keep |
/// | Proved(old)   | Proved(diff)    | Conflict([old,diff]) | Ambiguous → degrade to unproved |
/// | Unproved      | _anything_      | Unproved          | Already unsafe, stays unsafe |
/// | Conflict{..}  | Proved(seq)     | Conflict[...seq]  | Record for diagnostics |
///
/// Using `max(old, new)` would be incorrect because it could advance a
/// checkpoint past rows that a competing MergeScan sub-stage has not yet
/// scanned, causing Flow to skip data.
fn merge_region_watermark_entries(
    merged: &mut BTreeMap<WatermarkKey, MergeState>,
    entries: impl IntoIterator<Item = RegionWatermarkEntry>,
) {
    for entry in entries {
        let RegionWatermarkEntry {
            region_id,
            partition_expr,
            watermark,
        } = entry;
        merged
            .entry((region_id, partition_expr))
            .and_modify(|existing| match watermark {
                None => match existing {
                    MergeState::Proved(_) => {
                        *existing = MergeState::Unproved;
                    }
                    MergeState::Unproved | MergeState::Conflict { .. } => {}
                },
                Some(seq) => match existing {
                    MergeState::Unproved => {}
                    MergeState::Proved(existing_seq) if *existing_seq == seq => {}
                    MergeState::Proved(existing_seq) => {
                        let old_seq = *existing_seq;
                        *existing = MergeState::Conflict {
                            watermarks: vec![old_seq, seq],
                        };
                    }
                    MergeState::Conflict { watermarks } => {
                        if !watermarks.contains(&seq) {
                            watermarks.push(seq);
                        }
                    }
                },
            })
            .or_insert(match watermark {
                Some(seq) => MergeState::Proved(seq),
                None => MergeState::Unproved,
            });
    }
}

fn merge_merge_scan_region_watermarks(
    merged: &mut BTreeMap<WatermarkKey, MergeState>,
    regions: impl IntoIterator<Item = u64>,
    sub_stage_metrics: impl IntoIterator<Item = RecordBatchMetrics>,
) {
    let regions = regions.into_iter().collect::<Vec<_>>();
    let mut proved_or_unproved_regions = BTreeSet::new();
    for metrics in sub_stage_metrics {
        proved_or_unproved_regions.extend(
            metrics
                .region_watermarks
                .iter()
                .map(|entry| entry.region_id),
        );
        merge_region_watermark_entries(merged, metrics.region_watermarks);
    }

    // Regions listed by a MergeScanExec participated even when no sub-stage can
    // prove a watermark. Merge missing per-scan region entries as explicit
    // `None` entries so an unproved participating branch vetoes any proof from
    // another branch for the same region. Participation is stated for the region
    // as a whole — no sub-stage said anything about one expression of it — so the
    // missing entry is the region-wide one.
    merge_region_watermark_entries(
        merged,
        regions
            .into_iter()
            .filter(|region_id| !proved_or_unproved_regions.contains(region_id))
            .map(|region_id| RegionWatermarkEntry {
                region_id,
                partition_expr: None,
                watermark: None,
            }),
    );
}

fn finalize_region_watermarks(
    merged: BTreeMap<WatermarkKey, MergeState>,
) -> Vec<RegionWatermarkEntry> {
    merged
        .into_iter()
        .map(|((region_id, partition_expr), state)| RegionWatermarkEntry {
            region_id,
            watermark: match state {
                MergeState::Unproved => None,
                MergeState::Proved(seq) => Some(seq),
                MergeState::Conflict { watermarks } => {
                    warn!(
                        "Conflicting proved watermarks for region {} expression {:?}: {:?}; degrading to unproved",
                        region_id, partition_expr, watermarks
                    );
                    None
                }
            },
            partition_expr,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use api::v1::region::{RemoteDynFilterUnregister, RemoteDynFilterUpdate};
    use async_trait::async_trait;
    use datafusion::arrow::datatypes::Schema as ArrowSchema;
    use datafusion::execution::SessionStateBuilder;
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion_expr::LogicalPlanBuilder;
    use session::ReadPreference;
    use session::context::QueryContextBuilder;
    use store_api::storage::RegionId;
    use table::table_name::TableName;

    use super::*;
    use crate::dist_plan::RemoteDynFilterProducerId;
    use crate::options::{FLOW_RETURN_REGION_SEQ, FLOW_SINK_TABLE_ID};
    use crate::region_query::RegionQueryHandler;

    struct NoopRegionQueryHandler;

    #[async_trait]
    impl RegionQueryHandler for NoopRegionQueryHandler {
        async fn select_target(
            &self,
            _read_preference: ReadPreference,
            _region_id: RegionId,
        ) -> Result<crate::region_query::RegionQueryTarget> {
            unreachable!("metrics tests should not execute remote queries")
        }

        async fn do_get(
            &self,
            _target: &crate::region_query::RegionQueryTarget,
            _request: common_query::request::QueryRequest,
        ) -> Result<SendableRecordBatchStream> {
            unreachable!("metrics tests should not execute remote queries")
        }

        async fn handle_remote_dyn_filter_update(
            &self,
            _target: &crate::region_query::RegionQueryTarget,
            _query_id: String,
            _update: RemoteDynFilterUpdate,
        ) -> Result<()> {
            unreachable!("metrics tests should not send remote dyn filter updates")
        }

        async fn handle_remote_dyn_filter_unregister(
            &self,
            _target: &crate::region_query::RegionQueryTarget,
            _query_id: String,
            _unregister: RemoteDynFilterUnregister,
        ) -> Result<()> {
            unreachable!("metrics tests should not send remote dyn filter unregisters")
        }
    }

    fn metrics_with_region_watermarks(entries: &[(u64, Option<u64>)]) -> RecordBatchMetrics {
        RecordBatchMetrics {
            region_watermarks: entries
                .iter()
                .map(|(region_id, watermark)| RegionWatermarkEntry {
                    region_id: *region_id,
                    partition_expr: None,
                    watermark: *watermark,
                })
                .collect(),
            ..Default::default()
        }
    }

    fn metrics_with_expression_watermarks(
        entries: &[(u64, Option<&str>, Option<u64>)],
    ) -> RecordBatchMetrics {
        RecordBatchMetrics {
            region_watermarks: entries
                .iter()
                .map(
                    |(region_id, partition_expr, watermark)| RegionWatermarkEntry {
                        region_id: *region_id,
                        partition_expr: partition_expr.map(|expr| expr.to_string()),
                        watermark: *watermark,
                    },
                )
                .collect(),
            ..Default::default()
        }
    }

    fn test_merge_scan_exec(table_id: u32, query_ctx: QueryContextRef) -> Arc<dyn ExecutionPlan> {
        let session_state = SessionStateBuilder::new().with_default_features().build();
        let plan = LogicalPlanBuilder::empty(false).build().unwrap();
        let schema = ArrowSchema::empty();

        Arc::new(
            MergeScanExec::new(
                &session_state,
                TableName::new("greptime", "public", "test"),
                vec![RegionId::new(table_id, 0)],
                plan,
                &schema,
                Arc::new(NoopRegionQueryHandler),
                query_ctx,
                1,
                Some(RemoteDynFilterProducerId::new(0)),
                false,
            )
            .unwrap(),
        )
    }

    fn flow_query_ctx_with_sink_table_id(sink_table_id: u32) -> QueryContextRef {
        Arc::new(
            QueryContextBuilder::default()
                .set_extension(FLOW_RETURN_REGION_SEQ.to_string(), "true".to_string())
                .set_extension(FLOW_SINK_TABLE_ID.to_string(), sink_table_id.to_string())
                .build(),
        )
    }

    #[test]
    fn terminal_metrics_returns_none_without_merge_scan() {
        let plan: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(Arc::new(ArrowSchema::empty())));
        assert!(terminal_recordbatch_metrics_from_plan(plan).is_none());
    }

    #[test]
    fn terminal_metrics_skip_flow_sink_merge_scan_regions() {
        let query_ctx = flow_query_ctx_with_sink_table_id(42);
        let plan = test_merge_scan_exec(42, query_ctx);

        assert!(terminal_recordbatch_metrics_from_plan(plan).is_none());
    }

    #[test]
    fn terminal_metrics_keep_source_merge_scan_regions_with_sink_extension() {
        let query_ctx = flow_query_ctx_with_sink_table_id(42);
        let plan = test_merge_scan_exec(43, query_ctx);

        assert_eq!(
            terminal_recordbatch_metrics_from_plan(plan)
                .unwrap()
                .region_watermarks,
            vec![RegionWatermarkEntry {
                region_id: RegionId::new(43, 0).as_u64(),
                partition_expr: None,
                watermark: None,
            }]
        );
    }

    /// The union of what the branches stated: a creator named by two branches is one entry,
    /// and an answer of "delivered none" survives as the empty set instead of disappearing.
    #[test]
    fn merge_foreign_file_row_creators_unions_the_branches() {
        let mut merged = BTreeSet::new();
        let mut conclusive = false;
        let mut unknown = false;

        merge_foreign_file_row_creators(Some(&[7, 9]), &mut merged, &mut conclusive, &mut unknown);
        merge_foreign_file_row_creators(Some(&[9]), &mut merged, &mut conclusive, &mut unknown);
        merge_foreign_file_row_creators(Some(&[]), &mut merged, &mut conclusive, &mut unknown);

        assert!(conclusive);
        assert!(!unknown);
        assert_eq!(
            BTreeSet::from([7, 9]),
            merged,
            "the delivered creators are the union of what the branches reported"
        );
    }

    /// A branch that states nothing latches the answer to unknown: the other branches' answers
    /// cover only part of the read, and "these are the creators" would claim the rest
    /// delivered nothing.
    #[test]
    fn merge_foreign_file_row_creators_keeps_a_silent_branch_unknown() {
        let mut merged = BTreeSet::new();
        let mut conclusive = false;
        let mut unknown = false;

        merge_foreign_file_row_creators(Some(&[]), &mut merged, &mut conclusive, &mut unknown);
        merge_foreign_file_row_creators(None, &mut merged, &mut conclusive, &mut unknown);

        assert!(conclusive);
        assert!(
            unknown,
            "one silent branch is enough to make the answer unknown"
        );
    }

    /// A plan whose stream states the answer reports it; one whose stream states nothing
    /// reports unknown rather than "none".
    #[test]
    fn collect_foreign_file_row_creators_reads_the_stream_that_reports_it() {
        let plan: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(Arc::new(ArrowSchema::empty())));

        let own = RecordBatchMetrics {
            foreign_file_row_creators: Some(vec![7]),
            ..Default::default()
        };
        assert_eq!(
            Some(vec![7]),
            collect_foreign_file_row_creators(plan.clone(), &own)
        );

        assert_eq!(
            None,
            collect_foreign_file_row_creators(plan, &RecordBatchMetrics::default())
        );
    }

    /// A merge scan lists its regions, so a region that never reported its metrics makes the
    /// whole answer unknown: the read over that region is uncovered, not empty.
    #[test]
    fn collect_foreign_file_row_creators_stays_unknown_when_a_region_never_reported() {
        let query_ctx = flow_query_ctx_with_sink_table_id(42);
        let plan = test_merge_scan_exec(43, query_ctx);
        let own = RecordBatchMetrics {
            foreign_file_row_creators: Some(vec![]),
            ..Default::default()
        };

        assert_eq!(
            None,
            collect_foreign_file_row_creators(plan, &own),
            "the merge scan's one region reported no metrics, so nothing about it is proved"
        );
    }

    /// A plan that runs in this process states its answer through the region scan that read the
    /// region, not through a stream that a rebuilt `RecordBatchMetrics` could still describe.
    #[tokio::test]
    async fn collect_foreign_file_row_creators_reads_the_local_region_scan() {
        use common_recordbatch::adapter::RecordBatchMetrics;
        use common_recordbatch::{RecordBatchStream, RecordBatches};
        use datafusion::prelude::SessionContext;
        use futures::TryStreamExt;
        use store_api::region_engine::SinglePartitionScanner;

        /// A partition stream that states what a Mito region's stream states when it ends.
        struct ReportedStream {
            inner: common_recordbatch::SendableRecordBatchStream,
            creators: Option<Vec<u64>>,
        }

        impl futures::Stream for ReportedStream {
            type Item = common_recordbatch::error::Result<common_recordbatch::RecordBatch>;

            fn poll_next(
                mut self: std::pin::Pin<&mut Self>,
                cx: &mut std::task::Context<'_>,
            ) -> std::task::Poll<Option<Self::Item>> {
                std::pin::Pin::new(&mut self.inner).poll_next(cx)
            }
        }

        impl RecordBatchStream for ReportedStream {
            fn schema(&self) -> datatypes::schema::SchemaRef {
                self.inner.schema()
            }

            fn output_ordering(&self) -> Option<&[common_recordbatch::OrderOption]> {
                self.inner.output_ordering()
            }

            fn metrics(&self) -> Option<RecordBatchMetrics> {
                Some(RecordBatchMetrics {
                    foreign_file_row_creators: self.creators.clone(),
                    ..Default::default()
                })
            }
        }

        fn region_scan(creators: Option<Vec<u64>>) -> Arc<dyn ExecutionPlan> {
            let mut builder = store_api::metadata::RegionMetadataBuilder::new(RegionId::new(1, 1));
            builder
                .push_column_metadata(store_api::metadata::ColumnMetadata {
                    column_schema: datatypes::schema::ColumnSchema::new(
                        "field",
                        datatypes::data_type::ConcreteDataType::float64_datatype(),
                        true,
                    ),
                    semantic_type: api::v1::SemanticType::Field,
                    column_id: 1,
                })
                .push_column_metadata(store_api::metadata::ColumnMetadata {
                    column_schema: datatypes::schema::ColumnSchema::new(
                        "ts",
                        datatypes::data_type::ConcreteDataType::timestamp_millisecond_datatype(),
                        false,
                    ),
                    semantic_type: api::v1::SemanticType::Timestamp,
                    column_id: 2,
                })
                .primary_key(vec![]);
            let metadata = Arc::new(builder.build().unwrap());
            let batches = RecordBatches::try_new(metadata.schema.clone(), vec![]).unwrap();
            let stream = ReportedStream {
                inner: batches.as_stream(),
                creators,
            };
            let scanner = Box::new(SinglePartitionScanner::new(
                Box::pin(stream),
                false,
                metadata,
                None,
            ));
            Arc::new(
                RegionScanExec::new(scanner, store_api::storage::ScanRequest::default(), None)
                    .unwrap(),
            )
        }

        async fn drain(plan: Arc<dyn ExecutionPlan>) {
            let ctx = SessionContext::new();
            let mut stream = plan.execute(0, ctx.task_ctx()).unwrap();
            while stream.try_next().await.unwrap().is_some() {}
        }

        // The scan answered what its stream stated.
        let plan = region_scan(Some(vec![7]));
        assert_eq!(
            None,
            collect_foreign_file_row_creators(plan.clone(), &RecordBatchMetrics::default()),
            "a scan whose stream has not ended states nothing"
        );
        drain(plan.clone()).await;
        assert_eq!(
            Some(vec![7]),
            collect_foreign_file_row_creators(plan.clone(), &RecordBatchMetrics::default()),
            "the plan states what its region scan delivered"
        );
        assert_eq!(
            Some(vec![7]),
            terminal_recordbatch_metrics_from_plan(plan)
                .expect("a conclusive creator answer is terminal metrics")
                .foreign_file_row_creators
        );

        // A scan whose stream stated nothing stays unknown: a read that did not report must
        // not be read as "delivered none".
        let plan = region_scan(None);
        drain(plan.clone()).await;
        assert_eq!(
            None,
            collect_foreign_file_row_creators(plan, &RecordBatchMetrics::default()),
            "a scan that states no answer leaves the plan's answer unknown"
        );
    }

    #[test]
    fn merge_merge_scan_region_watermarks_marks_missing_watermarks_unproved() {
        let mut merged = BTreeMap::new();

        merge_merge_scan_region_watermarks(&mut merged, [1, 2], std::iter::empty());

        assert_eq!(
            finalize_region_watermarks(merged),
            vec![
                RegionWatermarkEntry {
                    region_id: 1,
                    partition_expr: None,
                    watermark: None,
                },
                RegionWatermarkEntry {
                    region_id: 2,
                    partition_expr: None,
                    watermark: None,
                },
            ]
        );
    }

    #[test]
    fn merge_merge_scan_region_watermarks_keeps_matching_proved_values() {
        let mut merged = BTreeMap::new();

        merge_merge_scan_region_watermarks(
            &mut merged,
            [42],
            [
                metrics_with_region_watermarks(&[(42, Some(7))]),
                metrics_with_region_watermarks(&[(42, Some(7))]),
            ],
        );

        assert_eq!(
            finalize_region_watermarks(merged),
            vec![RegionWatermarkEntry {
                region_id: 42,
                partition_expr: None,
                watermark: Some(7),
            }]
        );
    }

    /// A statement about one partition expression of a region and a statement about the
    /// region's complete value domain are statements about different domains: merging
    /// them into one key would either advance a slice the round never read or forget
    /// that the whole region was opened.
    #[test]
    fn merge_merge_scan_region_watermarks_keys_by_region_and_expression() {
        let mut merged = BTreeMap::new();

        merge_merge_scan_region_watermarks(
            &mut merged,
            [42],
            [
                metrics_with_expression_watermarks(&[(42, None, Some(9))]),
                metrics_with_expression_watermarks(&[(42, Some("k0 < 10"), Some(5))]),
                metrics_with_expression_watermarks(&[(42, Some("k0 >= 10"), Some(7))]),
            ],
        );

        assert_eq!(
            finalize_region_watermarks(merged),
            vec![
                RegionWatermarkEntry {
                    region_id: 42,
                    partition_expr: None,
                    watermark: Some(9),
                },
                RegionWatermarkEntry {
                    region_id: 42,
                    partition_expr: Some("k0 < 10".to_string()),
                    watermark: Some(5),
                },
                RegionWatermarkEntry {
                    region_id: 42,
                    partition_expr: Some("k0 >= 10".to_string()),
                    watermark: Some(7),
                },
            ]
        );
    }

    /// The conflict rule still applies to one and the same expression.
    #[test]
    fn merge_merge_scan_region_watermarks_degrades_conflicting_expression_entries() {
        let mut merged = BTreeMap::new();

        merge_merge_scan_region_watermarks(
            &mut merged,
            [42],
            [
                metrics_with_expression_watermarks(&[(42, Some("k0 < 10"), Some(5))]),
                metrics_with_expression_watermarks(&[(42, Some("k0 < 10"), Some(6))]),
                metrics_with_expression_watermarks(&[(42, Some("k0 < 10"), None)]),
            ],
        );

        assert_eq!(
            finalize_region_watermarks(merged),
            vec![RegionWatermarkEntry {
                region_id: 42,
                partition_expr: Some("k0 < 10".to_string()),
                watermark: None,
            }]
        );
    }

    #[test]
    fn merge_merge_scan_region_watermarks_degrades_conflicting_proved_values() {
        let mut merged = BTreeMap::new();

        merge_merge_scan_region_watermarks(
            &mut merged,
            [7],
            [
                metrics_with_region_watermarks(&[(7, Some(11))]),
                metrics_with_region_watermarks(&[(7, Some(13))]),
            ],
        );

        assert_eq!(
            finalize_region_watermarks(merged),
            vec![RegionWatermarkEntry {
                region_id: 7,
                partition_expr: None,
                watermark: None,
            }]
        );
    }

    #[test]
    fn merge_merge_scan_region_watermarks_none_vetoes_proved_value() {
        let mut merged = BTreeMap::new();

        merge_merge_scan_region_watermarks(
            &mut merged,
            [9],
            [
                metrics_with_region_watermarks(&[(9, Some(21))]),
                metrics_with_region_watermarks(&[(9, None)]),
            ],
        );

        assert_eq!(
            finalize_region_watermarks(merged),
            vec![RegionWatermarkEntry {
                region_id: 9,
                partition_expr: None,
                watermark: None,
            }]
        );
    }

    #[test]
    fn merge_merge_scan_region_watermarks_none_vetoes_proved_value_regardless_of_order() {
        let mut merged = BTreeMap::new();

        merge_merge_scan_region_watermarks(
            &mut merged,
            [9],
            [
                metrics_with_region_watermarks(&[(9, None)]),
                metrics_with_region_watermarks(&[(9, Some(21))]),
            ],
        );

        assert_eq!(
            finalize_region_watermarks(merged),
            vec![RegionWatermarkEntry {
                region_id: 9,
                partition_expr: None,
                watermark: None,
            }]
        );
    }

    #[test]
    fn merge_merge_scan_region_watermarks_missing_branch_vetoes_proved_value() {
        let mut merged = BTreeMap::new();

        merge_merge_scan_region_watermarks(
            &mut merged,
            [9],
            [metrics_with_region_watermarks(&[(9, Some(21))])],
        );
        merge_merge_scan_region_watermarks(&mut merged, [9], std::iter::empty());

        assert_eq!(
            finalize_region_watermarks(merged),
            vec![RegionWatermarkEntry {
                region_id: 9,
                partition_expr: None,
                watermark: None,
            }]
        );
    }

    #[test]
    fn merge_merge_scan_region_watermarks_missing_branch_vetoes_proved_value_regardless_of_order() {
        let mut merged = BTreeMap::new();

        merge_merge_scan_region_watermarks(&mut merged, [9], std::iter::empty());
        merge_merge_scan_region_watermarks(
            &mut merged,
            [9],
            [metrics_with_region_watermarks(&[(9, Some(21))])],
        );

        assert_eq!(
            finalize_region_watermarks(merged),
            vec![RegionWatermarkEntry {
                region_id: 9,
                partition_expr: None,
                watermark: None,
            }]
        );
    }
}
