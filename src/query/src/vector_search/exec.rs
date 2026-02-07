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

//! Physical execution plan for adaptive vector top-k search.

use std::any::Any;
use std::sync::Arc;

use arrow::array::{Array, UInt32Array};
use arrow::compute::{SortColumn, concat_batches, lexsort_to_indices, take};
use arrow::record_batch::RecordBatch;
use arrow_schema::SchemaRef;
use common_recordbatch::DfSendableRecordBatchStream;
use datafusion::execution::TaskContext;
use datafusion::execution::context::SessionState;
use datafusion::physical_plan::metrics::{
    ExecutionPlanMetricsSet, Gauge, MetricBuilder, MetricsSet,
};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
};
use datafusion_physical_expr::Partitioning;
use futures_util::StreamExt;

#[derive(Debug)]
pub struct AdaptiveVectorTopKExec {
    inner: Arc<dyn ExecutionPlan>,
    exprs: Vec<datafusion_physical_expr::PhysicalSortExpr>,
    logical_exprs: Vec<datafusion_expr::SortExpr>,
    logical_input: datafusion_expr::LogicalPlan,
    session_state: Arc<SessionState>,
    fetch: Option<usize>,
    skip: usize,
    rebuild_plan: bool,
    properties: PlanProperties,
    metrics: ExecutionPlanMetricsSet,
    /// Vector index requested k for the last executed round.
    vector_index_requested_k: Gauge,
    /// Vector index returned k for the last executed round.
    vector_index_returned_k: Gauge,
    /// Number of retry rounds (rounds - 1) for the last execution.
    vector_index_retry_rounds: Gauge,
    /// The last k used in the adaptive loop.
    vector_index_last_k: Gauge,
    /// Desired output rows (fetch + skip) for the last execution.
    vector_index_desired_rows: Gauge,
    /// Result length after sort/limit for the last execution.
    vector_index_result_len: Gauge,
    /// Kth distance in micros (distance * 1_000_000) for the last execution.
    vector_index_kth_distance_micros: Gauge,
}

impl AdaptiveVectorTopKExec {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        inner: Arc<dyn ExecutionPlan>,
        exprs: Vec<datafusion_physical_expr::PhysicalSortExpr>,
        logical_exprs: Vec<datafusion_expr::SortExpr>,
        logical_input: datafusion_expr::LogicalPlan,
        session_state: Arc<SessionState>,
        fetch: Option<usize>,
        skip: usize,
        rebuild_plan: bool,
    ) -> Self {
        let properties = PlanProperties::new(
            inner.equivalence_properties().clone(),
            Partitioning::UnknownPartitioning(1),
            inner.properties().emission_type,
            inner.properties().boundedness,
        );
        let metrics = ExecutionPlanMetricsSet::new();
        let vector_index_requested_k =
            MetricBuilder::new(&metrics).gauge("vector_index_requested_k", 1);
        let vector_index_returned_k =
            MetricBuilder::new(&metrics).gauge("vector_index_returned_k", 1);
        let vector_index_retry_rounds =
            MetricBuilder::new(&metrics).gauge("vector_index_retry_rounds", 1);
        let vector_index_last_k = MetricBuilder::new(&metrics).gauge("vector_index_last_k", 1);
        let vector_index_desired_rows =
            MetricBuilder::new(&metrics).gauge("vector_index_desired_rows", 1);
        let vector_index_result_len =
            MetricBuilder::new(&metrics).gauge("vector_index_result_len", 1);
        let vector_index_kth_distance_micros =
            MetricBuilder::new(&metrics).gauge("vector_index_kth_distance_micros", 1);
        Self {
            inner,
            exprs,
            logical_exprs,
            logical_input,
            session_state,
            fetch,
            skip,
            rebuild_plan,
            properties,
            metrics,
            vector_index_requested_k,
            vector_index_returned_k,
            vector_index_retry_rounds,
            vector_index_last_k,
            vector_index_desired_rows,
            vector_index_result_len,
            vector_index_kth_distance_micros,
        }
    }
}

impl DisplayAs for AdaptiveVectorTopKExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "AdaptiveVectorTopKExec")
    }
}

impl ExecutionPlan for AdaptiveVectorTopKExec {
    fn name(&self) -> &str {
        "AdaptiveVectorTopKExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.inner]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(datafusion_common::DataFusionError::Internal(
                "AdaptiveVectorTopKExec expects exactly one child".to_string(),
            ));
        }
        let inner = children.pop().ok_or_else(|| {
            datafusion_common::DataFusionError::Internal(
                "AdaptiveVectorTopKExec expects exactly one child".to_string(),
            )
        })?;
        Ok(Arc::new(Self::new(
            inner,
            self.exprs.clone(),
            self.logical_exprs.clone(),
            self.logical_input.clone(),
            self.session_state.clone(),
            self.fetch,
            self.skip,
            self.rebuild_plan,
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion_common::Result<DfSendableRecordBatchStream> {
        let schema = self.inner.schema();
        let stream_schema = schema.clone();
        let exprs = self.exprs.clone();
        let logical_exprs = self.logical_exprs.clone();
        let logical_input = self.logical_input.clone();
        let session_state = self.session_state.clone();
        let options = session_state
            .config()
            .get_extension::<crate::vector_search::options::AdaptiveVectorTopKOptions>();
        let fetch = self.fetch;
        let skip = self.skip;
        let rebuild_plan = self.rebuild_plan;
        let inner = self.inner.clone();
        let context = context.clone();
        let vector_index_requested_k = self.vector_index_requested_k.clone();
        let vector_index_returned_k = self.vector_index_returned_k.clone();
        let vector_index_retry_rounds = self.vector_index_retry_rounds.clone();
        let vector_index_last_k = self.vector_index_last_k.clone();
        let vector_index_desired_rows = self.vector_index_desired_rows.clone();
        let vector_index_result_len = self.vector_index_result_len.clone();
        let vector_index_kth_distance_micros = self.vector_index_kth_distance_micros.clone();

        let stream = async_stream::try_stream! {
            if partition != 0 {
                return;
            }
            let maybe_batch = if rebuild_plan {
                run_adaptive_topk(
                    &stream_schema,
                    &exprs,
                    &logical_exprs,
                    &logical_input,
                    &session_state,
                    &context,
                    fetch,
                    skip,
                    options.as_ref().map(|v| v.as_ref()),
                    &vector_index_requested_k,
                    &vector_index_returned_k,
                    &vector_index_retry_rounds,
                    &vector_index_last_k,
                    &vector_index_desired_rows,
                    &vector_index_result_len,
                    &vector_index_kth_distance_micros,
                )
                .await?
            } else {
                run_direct_topk(
                    &stream_schema,
                    &exprs,
                    &inner,
                    &context,
                    fetch,
                    skip,
                )
                .await?
            };

            if let Some(batch) = maybe_batch {
                yield batch;
            }
        };

        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

/// (batch, kth_distance, tie_hash, total_rows, result_len)
type SortLimitResult = (RecordBatch, Option<f64>, Option<u64>, usize, usize);

enum SortLimitOutcome {
    Empty,
    Skipped,
    Some(SortLimitResult),
}

async fn run_direct_topk(
    schema: &SchemaRef,
    exprs: &[datafusion_physical_expr::PhysicalSortExpr],
    inner: &Arc<dyn ExecutionPlan>,
    context: &Arc<TaskContext>,
    fetch: Option<usize>,
    skip: usize,
) -> datafusion_common::Result<Option<RecordBatch>> {
    let input_partition_count = inner.output_partitioning().partition_count();
    let mut batches = Vec::new();
    for input_partition in 0..input_partition_count {
        let mut input = inner.execute(input_partition, context.clone())?;
        while let Some(batch) = input.next().await {
            let batch = batch?;
            if batch.num_rows() > 0 {
                batches.push(batch);
            }
        }
    }

    let (batch, _, _, _, _) = match sort_and_limit(batches, schema, exprs, fetch, skip)? {
        SortLimitOutcome::Empty | SortLimitOutcome::Skipped => return Ok(None),
        SortLimitOutcome::Some(result) => result,
    };
    Ok(Some(batch))
}

#[allow(clippy::too_many_arguments)]
async fn run_adaptive_topk(
    schema: &SchemaRef,
    exprs: &[datafusion_physical_expr::PhysicalSortExpr],
    logical_exprs: &[datafusion_expr::SortExpr],
    logical_input: &datafusion_expr::LogicalPlan,
    session_state: &Arc<SessionState>,
    context: &Arc<TaskContext>,
    fetch: Option<usize>,
    skip: usize,
    options: Option<&crate::vector_search::options::AdaptiveVectorTopKOptions>,
    vector_index_requested_k: &Gauge,
    vector_index_returned_k: &Gauge,
    vector_index_retry_rounds: &Gauge,
    vector_index_last_k: &Gauge,
    vector_index_desired_rows: &Gauge,
    vector_index_result_len: &Gauge,
    vector_index_kth_distance_micros: &Gauge,
) -> datafusion_common::Result<Option<RecordBatch>> {
    let Some(desired) = fetch.map(|f| f.saturating_add(skip)) else {
        return Ok(None);
    };
    let mut k = desired;
    if k == 0 {
        return Ok(None);
    }
    let mut last_tie_hash = None;
    let mut last_kth = None;
    let mut last_total_rows = None;
    let mut round = 0usize;
    let max_rounds = options.map(|o| o.max_rounds).unwrap_or(8);
    let max_k = options.and_then(|o| o.max_k);
    let max_rows = options.map(|o| o.max_rows).unwrap_or(100_000);

    loop {
        round += 1;
        vector_index_retry_rounds.set(round.saturating_sub(1));
        if let Some(max_k) = max_k
            && k > max_k
        {
            k = max_k;
        }
        vector_index_last_k.set(k);
        vector_index_desired_rows.set(desired);
        let logical_plan = datafusion_expr::LogicalPlanBuilder::from(logical_input.clone())
            .sort(logical_exprs.to_vec())?
            .limit(0, Some(k))?
            .build()?;

        let plan = crate::optimizer::adaptive_vector_topk::with_adaptive_topk_disabled(
            session_state.create_physical_plan(&logical_plan),
        )
        .await?;

        let input_partition_count = plan.output_partitioning().partition_count();
        // Each adaptive round re-executes all partitions; this can trigger repeated RPCs
        // in distributed scans. Keep the loop simple and bounded by max_rows/max_rounds.
        let mut batches = Vec::new();
        let mut collected_rows = 0usize;
        let mut hit_max_rows = false;
        for input_partition in 0..input_partition_count {
            let mut input = plan.execute(input_partition, context.clone())?;
            while let Some(batch) = input.next().await {
                let batch = batch?;
                let rows = batch.num_rows();
                if rows > 0 {
                    batches.push(batch);
                    collected_rows = collected_rows.saturating_add(rows);
                    if collected_rows >= max_rows {
                        hit_max_rows = true;
                        break;
                    }
                }
            }
            if collected_rows >= max_rows {
                break;
            }
        }

        let (requested_k, returned_k) = collect_vector_index_k_metrics(plan.as_ref());
        vector_index_requested_k.set(requested_k);
        vector_index_returned_k.set(returned_k);
        let (batch, kth_distance, tie_hash, total_rows, result_len) =
            match sort_and_limit(batches, schema, exprs, fetch, skip)? {
                SortLimitOutcome::Empty | SortLimitOutcome::Skipped => {
                    if round >= max_rounds {
                        return Ok(None);
                    }
                    if let Some(max_k) = max_k
                        && k >= max_k
                    {
                        return Ok(None);
                    }
                    if requested_k > 0 && returned_k == requested_k {
                        k = k.saturating_mul(2);
                        continue;
                    }
                    return Ok(None);
                }
                SortLimitOutcome::Some(result) => result,
            };
        vector_index_result_len.set(result_len);
        let kth_micros = kth_distance
            .filter(|v| v.is_finite())
            .map(|v| (v * 1_000_000.0).round())
            .filter(|v| *v > 0.0)
            .map(|v| v as usize)
            .unwrap_or(0);
        vector_index_kth_distance_micros.set(kth_micros);

        let tie_stable = match (last_tie_hash, tie_hash, last_kth, kth_distance) {
            (Some(prev_hash), Some(curr_hash), Some(prev_kth), Some(curr_kth)) => {
                prev_hash == curr_hash && prev_kth == curr_kth
            }
            _ => false,
        };

        // `returned_k < requested_k` is a coarse aggregated metric and can be affected by
        // partial collection. Treat exhaustion as reliable only when this round did not hit
        // the row cap and the physical plan itself produced fewer than `k` rows.
        if should_finish_round(total_rows, desired, tie_stable, hit_max_rows, k) {
            return Ok(Some(batch));
        }

        // Stagnation stop: if the collected candidate count no longer increases after growing k,
        // another round is unlikely to produce new rows from storage/index pruning.
        // Returning here preserves current best-effort semantics and avoids useless retries.
        if total_rows < desired && matches!(last_total_rows, Some(prev) if prev == total_rows) {
            return Ok(Some(batch));
        }

        if round >= max_rounds {
            return Ok(Some(batch));
        }

        if let Some(max_k) = max_k
            && k >= max_k
        {
            return Ok(Some(batch));
        }

        last_tie_hash = tie_hash;
        last_kth = kth_distance;
        last_total_rows = Some(total_rows);
        k = k.saturating_mul(2);
    }
}

fn sort_and_limit(
    batches: Vec<RecordBatch>,
    schema: &SchemaRef,
    exprs: &[datafusion_physical_expr::PhysicalSortExpr],
    fetch: Option<usize>,
    skip: usize,
) -> datafusion_common::Result<SortLimitOutcome> {
    if batches.is_empty() {
        return Ok(SortLimitOutcome::Empty);
    }

    let batch = concat_batches(schema, &batches)?;
    let total_rows = batch.num_rows();
    if total_rows == 0 {
        return Ok(SortLimitOutcome::Empty);
    }

    let mut sort_columns = Vec::with_capacity(exprs.len());
    for expr in exprs {
        let array = expr.expr.evaluate(&batch)?.into_array(total_rows)?;
        sort_columns.push(SortColumn {
            values: array,
            options: Some(expr.options),
        });
    }
    let indices = if sort_columns.is_empty() {
        None
    } else {
        Some(lexsort_to_indices(&sort_columns, None)?)
    };

    let start = skip.min(total_rows);
    let length = match fetch {
        Some(fetch) => fetch.min(total_rows - start),
        None => total_rows - start,
    };

    if length == 0 {
        return Ok(SortLimitOutcome::Skipped);
    }

    let selected_indices = if let Some(ref indices) = indices {
        indices.slice(start, length)
    } else {
        let all_indices = UInt32Array::from_iter_values(0u32..total_rows as u32);
        Arc::new(all_indices).slice(start, length)
    };

    let kth_distance = if let Some(indices) = &indices {
        let kth_index = start.saturating_add(length).saturating_sub(1);
        extract_kth_distance(&sort_columns, indices, kth_index)
    } else {
        None
    };

    // Hash rows in the distance-tie group at kth boundary. Two consecutive rounds are treated as
    // tie-stable only when both kth distance and this hash stay unchanged.
    let tie_hash = if let (Some(indices), Some(kth_distance)) = (&indices, kth_distance) {
        Some(hash_tie_group(&sort_columns, indices, kth_distance)?)
    } else {
        None
    };

    let columns = batch
        .columns()
        .iter()
        .map(|col| take(col.as_ref(), &selected_indices, None))
        .collect::<Result<Vec<_>, _>>()?;

    Ok(SortLimitOutcome::Some((
        RecordBatch::try_new(schema.clone(), columns)?,
        kth_distance,
        tie_hash,
        total_rows,
        length,
    )))
}

fn collect_vector_index_k_metrics(plan: &dyn ExecutionPlan) -> (usize, usize) {
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
                "vector_index_requested_k" => requested = requested.saturating_add(value),
                "vector_index_returned_k" => returned = returned.saturating_add(value),
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
        let (child_requested, child_returned) = collect_vector_index_k_metrics(child.as_ref());
        requested = requested.saturating_add(child_requested);
        returned = returned.saturating_add(child_returned);
    }
    (requested, returned)
}

fn candidates_exhausted(hit_max_rows: bool, total_rows: usize, k: usize) -> bool {
    !hit_max_rows && total_rows < k
}

fn should_finish_round(
    total_rows: usize,
    desired: usize,
    tie_stable: bool,
    hit_max_rows: bool,
    k: usize,
) -> bool {
    total_rows >= desired && (tie_stable || candidates_exhausted(hit_max_rows, total_rows, k))
}

fn extract_kth_distance(
    sort_columns: &[SortColumn],
    indices: &UInt32Array,
    kth_index: usize,
) -> Option<f64> {
    let first = sort_columns.first()?;
    let value_index = indices.value(kth_index) as usize;
    match first.values.data_type() {
        arrow_schema::DataType::Float32 => first
            .values
            .as_any()
            .downcast_ref::<arrow::array::Float32Array>()
            .and_then(|arr| (!arr.is_null(value_index)).then(|| arr.value(value_index)))
            .map(f64::from),
        arrow_schema::DataType::Float64 => first
            .values
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()
            .and_then(|arr| (!arr.is_null(value_index)).then(|| arr.value(value_index))),
        _ => None,
    }
}

fn hash_tie_group(
    sort_columns: &[SortColumn],
    indices: &UInt32Array,
    kth_distance: f64,
) -> datafusion_common::Result<u64> {
    use std::hash::{Hash, Hasher};

    use arrow::row::{RowConverter, SortField};

    let first = sort_columns.first().ok_or_else(|| {
        datafusion_common::DataFusionError::Internal("Missing sort column".to_string())
    })?;

    // When only distance is sorted, the tie hash is stable but adds little value.
    // It becomes useful when secondary sort columns exist: if candidate expansion changes row
    // composition inside the equal-distance group, this hash changes even when kth distance does not.
    let mut tie_indices = Vec::new();
    for i in 0..indices.len() {
        let row_index = indices.value(i) as usize;
        let value = match first.values.data_type() {
            arrow_schema::DataType::Float32 => first
                .values
                .as_any()
                .downcast_ref::<arrow::array::Float32Array>()
                .and_then(|arr| (!arr.is_null(row_index)).then(|| arr.value(row_index)))
                .map(f64::from),
            arrow_schema::DataType::Float64 => first
                .values
                .as_any()
                .downcast_ref::<arrow::array::Float64Array>()
                .and_then(|arr| (!arr.is_null(row_index)).then(|| arr.value(row_index))),
            _ => None,
        };
        if let Some(value) = value
            && distance_equal(value, kth_distance)
        {
            tie_indices.push(row_index as u32);
        }
    }

    if tie_indices.is_empty() {
        return Ok(0);
    }

    let fields = sort_columns
        .iter()
        .map(|c| SortField::new(c.values.data_type().clone()))
        .collect::<Vec<_>>();
    let converter = RowConverter::new(fields)?;

    let arrays = sort_columns
        .iter()
        .map(|c| c.values.clone())
        .collect::<Vec<_>>();

    let rows = converter.convert_columns(&arrays)?;
    let mut hasher = ahash::AHasher::default();
    for idx in tie_indices {
        rows.row(idx as usize).as_ref().hash(&mut hasher);
    }
    Ok(hasher.finish())
}

fn distance_equal(lhs: f64, rhs: f64) -> bool {
    let scale = lhs.abs().max(rhs.abs()).max(1.0);
    let tol = (f32::EPSILON as f64) * scale;
    (lhs - rhs).abs() <= tol
}

#[cfg(test)]
mod tests {
    use std::any::Any;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::task::{Context, Poll};

    use arrow::array::{Float32Array, Int32Array};
    use arrow::compute::SortOptions;
    use arrow_schema::{DataType, Field, Schema, SchemaRef};
    use common_recordbatch::{DfRecordBatch, DfSendableRecordBatchStream};
    use datafusion::execution::TaskContext;
    use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
    use datafusion::physical_plan::metrics::{
        BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet,
    };
    use datafusion::physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, RecordBatchStream, collect,
    };
    use datafusion_common::DFSchema;
    use datafusion_physical_expr::expressions::Column;
    use datafusion_physical_expr::{EquivalenceProperties, Partitioning, PhysicalSortExpr};
    use futures::Stream;

    use super::AdaptiveVectorTopKExec;

    #[derive(Debug)]
    struct TestInputExec {
        partitions: Vec<Vec<DfRecordBatch>>,
        schema: SchemaRef,
        properties: PlanProperties,
        metrics: ExecutionPlanMetricsSet,
    }

    impl TestInputExec {
        fn new(partitions: Vec<Vec<DfRecordBatch>>, schema: SchemaRef) -> Self {
            let partitioning = Partitioning::UnknownPartitioning(partitions.len());
            let properties = PlanProperties::new(
                EquivalenceProperties::new(schema.clone()),
                partitioning,
                EmissionType::Incremental,
                Boundedness::Bounded,
            );
            Self {
                partitions,
                schema,
                properties,
                metrics: ExecutionPlanMetricsSet::new(),
            }
        }
    }

    impl DisplayAs for TestInputExec {
        fn fmt_as(&self, _t: DisplayFormatType, _f: &mut std::fmt::Formatter) -> std::fmt::Result {
            unimplemented!()
        }
    }

    impl ExecutionPlan for TestInputExec {
        fn name(&self) -> &str {
            "TestInputExec"
        }

        fn as_any(&self) -> &dyn Any {
            self
        }

        fn properties(&self) -> &PlanProperties {
            &self.properties
        }

        fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
            vec![]
        }

        fn with_new_children(
            self: Arc<Self>,
            _children: Vec<Arc<dyn ExecutionPlan>>,
        ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
            Ok(self)
        }

        fn execute(
            &self,
            partition: usize,
            _context: Arc<TaskContext>,
        ) -> datafusion_common::Result<DfSendableRecordBatchStream> {
            let batches = self.partitions.get(partition).cloned().unwrap_or_default();
            let stream = TestStream {
                schema: self.schema.clone(),
                batches,
                idx: 0,
                metrics: BaselineMetrics::new(&self.metrics, partition),
            };
            Ok(Box::pin(stream))
        }

        fn metrics(&self) -> Option<MetricsSet> {
            Some(self.metrics.clone_inner())
        }
    }

    struct TestStream {
        schema: SchemaRef,
        batches: Vec<DfRecordBatch>,
        idx: usize,
        metrics: BaselineMetrics,
    }

    impl Stream for TestStream {
        type Item = datafusion_common::Result<DfRecordBatch>;
        fn poll_next(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Option<datafusion_common::Result<DfRecordBatch>>> {
            if self.idx < self.batches.len() {
                let ret = self.batches[self.idx].clone();
                self.idx += 1;
                self.metrics.record_poll(Poll::Ready(Some(Ok(ret))))
            } else {
                Poll::Ready(None)
            }
        }
    }

    impl RecordBatchStream for TestStream {
        fn schema(&self) -> SchemaRef {
            self.schema.clone()
        }
    }

    fn build_batch(dist: Vec<f32>, id: Vec<i32>, schema: SchemaRef) -> DfRecordBatch {
        let dist = Arc::new(Float32Array::from(dist)) as _;
        let id = Arc::new(Int32Array::from(id)) as _;
        DfRecordBatch::try_new(schema, vec![dist, id]).unwrap()
    }

    fn empty_relation_plan(schema: SchemaRef) -> datafusion_expr::LogicalPlan {
        let df_schema = DFSchema::try_from(schema.as_ref().clone()).unwrap();
        datafusion_expr::LogicalPlan::EmptyRelation(datafusion_expr::EmptyRelation {
            produce_one_row: false,
            schema: Arc::new(df_schema),
        })
    }

    #[tokio::test]
    async fn test_adaptive_vector_topk_exec_global_sort_and_limit() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("dist", DataType::Float32, false),
            Field::new("id", DataType::Int32, false),
        ]));

        let p0 = vec![build_batch(vec![0.3, 0.1], vec![3, 1], schema.clone())];
        let p1 = vec![build_batch(vec![0.2, 0.1], vec![2, 0], schema.clone())];

        let input = Arc::new(TestInputExec::new(vec![p0, p1], schema.clone()));
        let exprs = vec![
            PhysicalSortExpr::new(
                Arc::new(Column::new_with_schema("dist", &schema).unwrap()),
                SortOptions {
                    descending: false,
                    nulls_first: true,
                },
            ),
            PhysicalSortExpr::new(
                Arc::new(Column::new_with_schema("id", &schema).unwrap()),
                SortOptions {
                    descending: false,
                    nulls_first: true,
                },
            ),
        ];

        let ctx = datafusion::execution::context::SessionContext::default();
        let exec = Arc::new(AdaptiveVectorTopKExec::new(
            input,
            exprs,
            Vec::new(),
            empty_relation_plan(schema.clone()),
            Arc::new(ctx.state()),
            Some(2),
            1,
            false,
        ));
        let batches = collect(exec, ctx.task_ctx()).await.unwrap();

        assert_eq!(batches.len(), 1);
        let batch = &batches[0];
        let dist = batch
            .column(0)
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap();
        let id = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();

        assert_eq!(dist.values(), &[0.1, 0.2]);
        assert_eq!(id.values(), &[1, 2]);
    }

    #[tokio::test]
    async fn test_adaptive_vector_topk_exec_empty_second_partition() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("dist", DataType::Float32, false),
            Field::new("id", DataType::Int32, false),
        ]));

        let p0 = vec![build_batch(vec![0.4], vec![4], schema.clone())];
        let p1 = vec![];

        let input = Arc::new(TestInputExec::new(vec![p0, p1], schema.clone()));
        let exprs = vec![PhysicalSortExpr::new(
            Arc::new(Column::new_with_schema("dist", &schema).unwrap()),
            SortOptions {
                descending: false,
                nulls_first: true,
            },
        )];

        let ctx = datafusion::execution::context::SessionContext::default();
        let exec = Arc::new(AdaptiveVectorTopKExec::new(
            input,
            exprs,
            Vec::new(),
            empty_relation_plan(schema.clone()),
            Arc::new(ctx.state()),
            Some(1),
            0,
            false,
        ));
        let batches = collect(exec, ctx.task_ctx()).await.unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);
    }

    #[tokio::test]
    async fn test_adaptive_vector_topk_exec_skip_beyond_rows() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("dist", DataType::Float32, false),
            Field::new("id", DataType::Int32, false),
        ]));

        let p0 = vec![build_batch(vec![0.4], vec![4], schema.clone())];
        let input = Arc::new(TestInputExec::new(vec![p0], schema.clone()));
        let exprs = vec![PhysicalSortExpr::new(
            Arc::new(Column::new_with_schema("dist", &schema).unwrap()),
            SortOptions {
                descending: false,
                nulls_first: true,
            },
        )];

        let ctx = datafusion::execution::context::SessionContext::default();
        let exec = Arc::new(AdaptiveVectorTopKExec::new(
            input,
            exprs,
            Vec::new(),
            empty_relation_plan(schema.clone()),
            Arc::new(ctx.state()),
            Some(1),
            5,
            false,
        ));
        let batches = collect(exec, ctx.task_ctx()).await.unwrap();
        assert!(batches.is_empty());
    }

    #[test]
    fn test_should_not_finish_round_when_row_cap_hit_and_tie_unstable() {
        // A capped round is not enough evidence of global exhaustion.
        assert!(!super::should_finish_round(
            100_000, 100_000, false, true, 200_000
        ));
    }

    #[test]
    fn test_should_finish_round_when_not_capped_and_candidates_exhausted() {
        assert!(super::should_finish_round(128, 64, false, false, 256));
    }
}
