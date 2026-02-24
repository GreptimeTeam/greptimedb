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

use common_recordbatch::adapter::RecordBatchMetrics;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::metrics::MetricsSet;

pub(crate) const VECTOR_INDEX_REQUESTED_K: &str = "vector_index_requested_k";
pub(crate) const VECTOR_INDEX_RETURNED_K: &str = "vector_index_returned_k";
pub(crate) const VECTOR_INDEX_APPROXIMATE_RESULT: &str = "vector_index_approximate_result";

/// Recursively collects vector index requested/returned k from an `ExecutionPlan` tree.
///
/// Each node's `MetricsSet` is aggregated by name; the function sums across the entire tree.
pub(crate) fn collect_vector_index_k_from_plan(plan: &dyn ExecutionPlan) -> (usize, usize) {
    fn collect_from_metrics(metrics: Option<MetricsSet>) -> (usize, usize) {
        let Some(metrics) = metrics else {
            return (0, 0);
        };
        let aggregated = metrics
            .aggregate_by_name()
            .sorted_for_display()
            .timestamps_removed();
        let mut requested = 0usize;
        let mut returned = 0usize;
        for metric in aggregated.iter() {
            let name = metric.value().name();
            let value = metric.value().as_usize();
            match name {
                VECTOR_INDEX_REQUESTED_K => requested = requested.saturating_add(value),
                VECTOR_INDEX_RETURNED_K => returned = returned.saturating_add(value),
                _ => {}
            }
        }
        (requested, returned)
    }

    let mut requested = 0usize;
    let mut returned = 0usize;
    let (local_requested, local_returned) = collect_from_metrics(plan.metrics());
    requested = requested.saturating_add(local_requested);
    returned = returned.saturating_add(local_returned);
    for child in plan.children() {
        let (child_requested, child_returned) = collect_vector_index_k_from_plan(child.as_ref());
        requested = requested.saturating_add(child_requested);
        returned = returned.saturating_add(child_returned);
    }
    (requested, returned)
}

/// Collects vector index requested/returned k from `RecordBatchMetrics` (flat plan_metrics list).
///
/// Used in `MergeScanExec` where region metrics arrive as a flat list of `(String, usize)` pairs.
pub(crate) fn collect_vector_index_k_from_region_metrics(
    metrics: &RecordBatchMetrics,
) -> (usize, usize) {
    let mut requested = 0usize;
    let mut returned = 0usize;
    for plan_metric in &metrics.plan_metrics {
        for (name, value) in &plan_metric.metrics {
            match name.as_str() {
                VECTOR_INDEX_REQUESTED_K => requested = requested.saturating_add(*value),
                VECTOR_INDEX_RETURNED_K => returned = returned.saturating_add(*value),
                _ => {}
            }
        }
    }
    (requested, returned)
}
