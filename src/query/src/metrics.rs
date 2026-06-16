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

use common_recordbatch::adapter::{RecordBatchMetrics, RegionWatermarkEntry};
use common_recordbatch::{OrderOption, RecordBatch, RecordBatchStream, SendableRecordBatchStream};
use common_telemetry::warn;
use datafusion::physical_plan::ExecutionPlan;
use datatypes::schema::SchemaRef;
use futures::Stream;
use futures_util::ready;
use lazy_static::lazy_static;
use prometheus::*;
use session::context::QueryContextRef;

use crate::dist_plan::MergeScanExec;
use crate::error::Result;
use crate::options::FlowQueryExtensions;

/// Intermediate merge state for one participating region while collecting
/// terminal correctness watermarks across merge-scan sub-stages.
enum MergeState {
    /// At least one branch reported that this region cannot prove a safe
    /// checkpoint watermark for the current query round.
    Unproved,
    /// All seen branches agree the region can advance safely to this sequence.
    Proved(u64),
    /// Different proved sequences were reported for the same region. The final
    /// result is degraded to `None`, and the collected values are logged.
    Conflict {
        /// Distinct proved watermark candidates reported for the region.
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
    let region_watermarks = collect_region_watermarks(plan);
    if region_watermarks.is_empty() {
        None
    } else {
        Some(RecordBatchMetrics {
            region_watermarks,
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

fn collect_region_watermarks(plan: Arc<dyn ExecutionPlan>) -> Vec<RegionWatermarkEntry> {
    let mut merged = BTreeMap::<u64, MergeState>::new();
    let mut stack = vec![plan];

    while let Some(plan) = stack.pop() {
        if let Some(merge_scan) = plan.as_any().downcast_ref::<MergeScanExec>()
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

/// Merge a batch of per-region watermark entries into the global merged state.
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
    merged: &mut BTreeMap<u64, MergeState>,
    entries: impl IntoIterator<Item = RegionWatermarkEntry>,
) {
    for entry in entries {
        merged
            .entry(entry.region_id)
            .and_modify(|existing| match entry.watermark {
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
            .or_insert(match entry.watermark {
                Some(seq) => MergeState::Proved(seq),
                None => MergeState::Unproved,
            });
    }
}

fn merge_merge_scan_region_watermarks(
    merged: &mut BTreeMap<u64, MergeState>,
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
    // another branch for the same region.
    merge_region_watermark_entries(
        merged,
        regions
            .into_iter()
            .filter(|region_id| !proved_or_unproved_regions.contains(region_id))
            .map(|region_id| RegionWatermarkEntry {
                region_id,
                watermark: None,
            }),
    );
}

fn finalize_region_watermarks(merged: BTreeMap<u64, MergeState>) -> Vec<RegionWatermarkEntry> {
    merged
        .into_iter()
        .map(|(region_id, state)| RegionWatermarkEntry {
            region_id,
            watermark: match state {
                MergeState::Unproved => None,
                MergeState::Proved(seq) => Some(seq),
                MergeState::Conflict { watermarks } => {
                    warn!(
                        "Conflicting proved watermarks for region {}: {:?}; degrading to unproved",
                        region_id, watermarks
                    );
                    None
                }
            },
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};
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
        async fn do_get(
            &self,
            _read_preference: ReadPreference,
            _request: common_query::request::QueryRequest,
        ) -> Result<SendableRecordBatchStream> {
            unreachable!("metrics tests should not execute remote queries")
        }

        async fn handle_remote_dyn_filter_update(
            &self,
            _region_id: RegionId,
            _query_id: String,
            _update: RemoteDynFilterUpdate,
        ) -> Result<()> {
            unreachable!("metrics tests should not send remote dyn filter updates")
        }

        async fn handle_remote_dyn_filter_unregister(
            &self,
            _region_id: RegionId,
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
                    watermark: *watermark,
                })
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
                BTreeMap::<String, BTreeSet<datafusion_common::Column>>::new(),
                Some(RemoteDynFilterProducerId::new(0)),
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
                watermark: None,
            }]
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
                    watermark: None,
                },
                RegionWatermarkEntry {
                    region_id: 2,
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
                watermark: Some(7),
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
                watermark: None,
            }]
        );
    }
}
