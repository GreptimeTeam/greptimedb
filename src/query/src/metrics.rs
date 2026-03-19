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

use crate::dist_plan::MergeScanExec;

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

fn collect_region_watermarks(plan: Arc<dyn ExecutionPlan>) -> Vec<RegionWatermarkEntry> {
    #[derive(Clone)]
    enum MergeState {
        Unproved,
        Proved(u64),
        Conflict {
            region_id: u64,
            watermarks: Vec<u64>,
        },
    }

    let mut merged = std::collections::BTreeMap::<u64, MergeState>::new();
    let mut stack = vec![plan];

    while let Some(plan) = stack.pop() {
        if let Some(merge_scan) = plan.as_any().downcast_ref::<MergeScanExec>() {
            for metrics in merge_scan.sub_stage_metrics() {
                for entry in metrics.region_watermarks {
                    merged
                        .entry(entry.region_id)
                        .and_modify(|existing| {
                            *existing = match (existing.clone(), entry.watermark) {
                                (
                                    MergeState::Conflict {
                                        region_id,
                                        mut watermarks,
                                    },
                                    Some(seq),
                                ) => {
                                    if !watermarks.contains(&seq) {
                                        watermarks.push(seq);
                                    }
                                    MergeState::Conflict {
                                        region_id,
                                        watermarks,
                                    }
                                }
                                (
                                    MergeState::Conflict {
                                        region_id,
                                        watermarks,
                                    },
                                    None,
                                ) => MergeState::Conflict {
                                    region_id,
                                    watermarks,
                                },
                                (MergeState::Unproved, None) => MergeState::Unproved,
                                (MergeState::Unproved, Some(seq)) => MergeState::Proved(seq),
                                (MergeState::Proved(existing_seq), None) => {
                                    MergeState::Proved(existing_seq)
                                }
                                (MergeState::Proved(existing_seq), Some(seq))
                                    if existing_seq == seq =>
                                {
                                    MergeState::Proved(existing_seq)
                                }
                                (MergeState::Proved(existing_seq), Some(seq)) => {
                                    MergeState::Conflict {
                                        region_id: entry.region_id,
                                        watermarks: vec![existing_seq, seq],
                                    }
                                }
                            }
                        })
                        .or_insert(match entry.watermark {
                            Some(seq) => MergeState::Proved(seq),
                            None => MergeState::Unproved,
                        });
                }
            }
        }

        stack.extend(plan.children().into_iter().cloned());
    }

    merged
        .into_iter()
        .map(|(region_id, state)| RegionWatermarkEntry {
            region_id,
            watermark: match state {
                MergeState::Unproved => None,
                MergeState::Proved(seq) => Some(seq),
                MergeState::Conflict {
                    region_id,
                    watermarks,
                } => {
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
