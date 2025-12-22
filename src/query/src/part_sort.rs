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

//! Module for sorting input data within each [`PartitionRange`].
//!
//! This module defines the [`PartSortExec`] execution plan, which sorts each
//! partition ([`PartitionRange`]) independently based on the provided physical
//! sort expressions.

use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::array::ArrayRef;
use arrow::compute::{concat, concat_batches, take_record_batch};
use arrow_schema::SchemaRef;
use common_recordbatch::{DfRecordBatch, DfSendableRecordBatchStream};
use common_time::Timestamp;
use datafusion::common::arrow::compute::sort_to_indices;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion::execution::{RecordBatchStream, TaskContext};
use datafusion::physical_plan::execution_plan::CardinalityEffect;
use datafusion::physical_plan::filter_pushdown::{
    ChildFilterDescription, FilterDescription, FilterPushdownPhase,
};
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties, TopK,
    TopKDynamicFilters,
};
use datafusion_common::{DataFusionError, internal_err};
use datafusion_physical_expr::expressions::{DynamicFilterPhysicalExpr, lit};
use datafusion_physical_expr::{PhysicalExpr, PhysicalSortExpr};
use futures::{Stream, StreamExt};
use itertools::Itertools;
use parking_lot::RwLock;
use snafu::location;
use store_api::region_engine::PartitionRange;

use crate::error::Result;
use crate::window_sort::check_partition_range_monotonicity;
use crate::{array_iter_helper, downcast_ts_array};

/// Get the primary end of a `PartitionRange` based on sort direction.
///
/// - Descending: primary end is `end` (we process highest values first)
/// - Ascending: primary end is `start` (we process lowest values first)
fn get_primary_end(range: &PartitionRange, descending: bool) -> Timestamp {
    if descending { range.end } else { range.start }
}

/// Group consecutive ranges by their primary end value.
///
/// Returns a vector of (primary_end, start_idx_inclusive, end_idx_exclusive) tuples.
/// Ranges with the same primary end MUST be processed together because they may
/// overlap and contain values that belong to the same "top-k" result.
fn group_ranges_by_primary_end(
    ranges: &[PartitionRange],
    descending: bool,
) -> Vec<(Timestamp, usize, usize)> {
    if ranges.is_empty() {
        return vec![];
    }

    let mut groups = Vec::new();
    let mut group_start = 0;
    let mut current_primary_end = get_primary_end(&ranges[0], descending);

    for (idx, range) in ranges.iter().enumerate().skip(1) {
        let primary_end = get_primary_end(range, descending);
        if primary_end != current_primary_end {
            // End current group
            groups.push((current_primary_end, group_start, idx));
            // Start new group
            group_start = idx;
            current_primary_end = primary_end;
        }
    }
    // Push the last group
    groups.push((current_primary_end, group_start, ranges.len()));

    groups
}

/// Sort input within given PartitionRange
///
/// Input is assumed to be segmented by empty RecordBatch, which indicates a new `PartitionRange` is starting
///
/// and this operator will sort each partition independently within the partition.
#[derive(Debug, Clone)]
pub struct PartSortExec {
    /// Physical sort expressions(that is, sort by timestamp)
    expression: PhysicalSortExpr,
    limit: Option<usize>,
    input: Arc<dyn ExecutionPlan>,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
    partition_ranges: Vec<Vec<PartitionRange>>,
    properties: PlanProperties,
    /// Filter matching the state of the sort for dynamic filter pushdown.
    /// If `limit` is `Some`, this will also be set and a TopK operator may be used.
    /// If `limit` is `None`, this will be `None`.
    filter: Option<Arc<RwLock<TopKDynamicFilters>>>,
}

impl PartSortExec {
    pub fn try_new(
        expression: PhysicalSortExpr,
        limit: Option<usize>,
        partition_ranges: Vec<Vec<PartitionRange>>,
        input: Arc<dyn ExecutionPlan>,
    ) -> Result<Self> {
        check_partition_range_monotonicity(&partition_ranges, expression.options.descending)?;

        let metrics = ExecutionPlanMetricsSet::new();
        let properties = input.properties();
        let properties = PlanProperties::new(
            input.equivalence_properties().clone(),
            input.output_partitioning().clone(),
            properties.emission_type,
            properties.boundedness,
        );

        let filter = limit
            .is_some()
            .then(|| Self::create_filter(expression.expr.clone()));

        Ok(Self {
            expression,
            limit,
            input,
            metrics,
            partition_ranges,
            properties,
            filter,
        })
    }

    /// Add or reset `self.filter` to a new `TopKDynamicFilters`.
    fn create_filter(expr: Arc<dyn PhysicalExpr>) -> Arc<RwLock<TopKDynamicFilters>> {
        Arc::new(RwLock::new(TopKDynamicFilters::new(Arc::new(
            DynamicFilterPhysicalExpr::new(vec![expr], lit(true)),
        ))))
    }

    pub fn to_stream(
        &self,
        context: Arc<TaskContext>,
        partition: usize,
    ) -> datafusion_common::Result<DfSendableRecordBatchStream> {
        let input_stream: DfSendableRecordBatchStream =
            self.input.execute(partition, context.clone())?;

        if partition >= self.partition_ranges.len() {
            internal_err!(
                "Partition index out of range: {} >= {} at {}",
                partition,
                self.partition_ranges.len(),
                snafu::location!()
            )?;
        }

        let df_stream = Box::pin(PartSortStream::new(
            context,
            self,
            self.limit,
            input_stream,
            self.partition_ranges[partition].clone(),
            partition,
            self.filter.clone(),
        )?) as _;

        Ok(df_stream)
    }
}

impl DisplayAs for PartSortExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "PartSortExec: expr={} num_ranges={}",
            self.expression,
            self.partition_ranges.len(),
        )?;
        if let Some(limit) = self.limit {
            write!(f, " limit={}", limit)?;
        }
        Ok(())
    }
}

impl ExecutionPlan for PartSortExec {
    fn name(&self) -> &str {
        "PartSortExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        let new_input = if let Some(first) = children.first() {
            first
        } else {
            internal_err!("No children found")?
        };
        let new = Self::try_new(
            self.expression.clone(),
            self.limit,
            self.partition_ranges.clone(),
            new_input.clone(),
        )?;
        Ok(Arc::new(new))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion_common::Result<DfSendableRecordBatchStream> {
        self.to_stream(context, partition)
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    /// # Explain
    ///
    /// This plan needs to be executed on each partition independently,
    /// and is expected to run directly on storage engine's output
    /// distribution / partition.
    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        if self.limit.is_none() {
            CardinalityEffect::Equal
        } else {
            CardinalityEffect::LowerEqual
        }
    }

    fn gather_filters_for_pushdown(
        &self,
        phase: FilterPushdownPhase,
        parent_filters: Vec<Arc<dyn PhysicalExpr>>,
        _config: &datafusion::config::ConfigOptions,
    ) -> datafusion_common::Result<FilterDescription> {
        if !matches!(phase, FilterPushdownPhase::Post) {
            return FilterDescription::from_children(parent_filters, &self.children());
        }

        let mut child = ChildFilterDescription::from_child(&parent_filters, &self.input)?;

        if let Some(filter) = &self.filter {
            child = child.with_self_filter(filter.read().expr());
        }

        Ok(FilterDescription::new().with_child(child))
    }

    fn reset_state(self: Arc<Self>) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        // shared dynamic filter needs to be reset
        let new_filter = self
            .limit
            .is_some()
            .then(|| Self::create_filter(self.expression.expr.clone()));

        Ok(Arc::new(Self {
            expression: self.expression.clone(),
            limit: self.limit,
            input: self.input.clone(),
            metrics: self.metrics.clone(),
            partition_ranges: self.partition_ranges.clone(),
            properties: self.properties.clone(),
            filter: new_filter,
        }))
    }
}

enum PartSortBuffer {
    All(Vec<DfRecordBatch>),
    /// TopK buffer with row count.
    ///
    /// Given this heap only keeps k element, the capacity of this buffer
    /// is not accurate, and is only used for empty check.
    Top(TopK, usize),
}

impl PartSortBuffer {
    pub fn is_empty(&self) -> bool {
        match self {
            PartSortBuffer::All(v) => v.is_empty(),
            PartSortBuffer::Top(_, cnt) => *cnt == 0,
        }
    }
}

struct PartSortStream {
    /// Memory pool for this stream
    reservation: MemoryReservation,
    buffer: PartSortBuffer,
    expression: PhysicalSortExpr,
    limit: Option<usize>,
    input: DfSendableRecordBatchStream,
    input_complete: bool,
    schema: SchemaRef,
    partition_ranges: Vec<PartitionRange>,
    #[allow(dead_code)] // this is used under #[debug_assertions]
    partition: usize,
    cur_part_idx: usize,
    evaluating_batch: Option<DfRecordBatch>,
    metrics: BaselineMetrics,
    context: Arc<TaskContext>,
    root_metrics: ExecutionPlanMetricsSet,
    /// Groups of ranges by primary end: (primary_end, start_idx_inclusive, end_idx_exclusive).
    /// Ranges in the same group must be processed together before outputting results.
    range_groups: Vec<(Timestamp, usize, usize)>,
    /// Current group being processed (index into range_groups).
    cur_group_idx: usize,
}

impl PartSortStream {
    fn new(
        context: Arc<TaskContext>,
        sort: &PartSortExec,
        limit: Option<usize>,
        input: DfSendableRecordBatchStream,
        partition_ranges: Vec<PartitionRange>,
        partition: usize,
        filter: Option<Arc<RwLock<TopKDynamicFilters>>>,
    ) -> datafusion_common::Result<Self> {
        let buffer = if let Some(limit) = limit {
            let Some(filter) = filter else {
                return internal_err!(
                    "TopKDynamicFilters must be provided when limit is set at {}",
                    snafu::location!()
                );
            };

            PartSortBuffer::Top(
                TopK::try_new(
                    partition,
                    sort.schema().clone(),
                    vec![],
                    [sort.expression.clone()].into(),
                    limit,
                    context.session_config().batch_size(),
                    context.runtime_env(),
                    &sort.metrics,
                    filter,
                )?,
                0,
            )
        } else {
            PartSortBuffer::All(Vec::new())
        };

        // Compute range groups by primary end
        let descending = sort.expression.options.descending;
        let range_groups = group_ranges_by_primary_end(&partition_ranges, descending);

        Ok(Self {
            reservation: MemoryConsumer::new("PartSortStream".to_string())
                .register(&context.runtime_env().memory_pool),
            buffer,
            expression: sort.expression.clone(),
            limit,
            input,
            input_complete: false,
            schema: sort.input.schema(),
            partition_ranges,
            partition,
            cur_part_idx: 0,
            evaluating_batch: None,
            metrics: BaselineMetrics::new(&sort.metrics, partition),
            context,
            root_metrics: sort.metrics.clone(),
            range_groups,
            cur_group_idx: 0,
        })
    }
}

macro_rules! ts_to_timestamp {
    ($t:ty, $unit:expr, $arr:expr) => {{
        let arr = $arr
            .as_any()
            .downcast_ref::<arrow::array::PrimitiveArray<$t>>()
            .unwrap();

        arr.iter()
            .map(|v| v.map(|v| Timestamp::new(v, common_time::timestamp::TimeUnit::from(&$unit))))
            .collect_vec()
    }};
}

macro_rules! array_check_helper {
    ($t:ty, $unit:expr, $arr:expr, $cur_range:expr, $min_max_idx:expr) => {{
            if $cur_range.start.unit().as_arrow_time_unit() != $unit
            || $cur_range.end.unit().as_arrow_time_unit() != $unit
        {
            internal_err!(
                "PartitionRange unit mismatch, expect {:?}, found {:?}",
                $cur_range.start.unit(),
                $unit
            )?;
        }
        let arr = $arr
            .as_any()
            .downcast_ref::<arrow::array::PrimitiveArray<$t>>()
            .unwrap();

        let min = arr.value($min_max_idx.0);
        let max = arr.value($min_max_idx.1);
        let (min, max) = if min < max{
            (min, max)
        } else {
            (max, min)
        };
        let cur_min = $cur_range.start.value();
        let cur_max = $cur_range.end.value();
        // note that PartitionRange is left inclusive and right exclusive
        if !(min >= cur_min && max < cur_max) {
            internal_err!(
                "Sort column min/max value out of partition range: sort_column.min_max=[{:?}, {:?}] not in PartitionRange=[{:?}, {:?}]",
                min,
                max,
                cur_min,
                cur_max
            )?;
        }
    }};
}

impl PartSortStream {
    /// check whether the sort column's min/max value is within the current group's effective range.
    /// For group-based processing, data from multiple ranges with the same primary end
    /// is accumulated together, so we check against the union of all ranges in the group.
    fn check_in_range(
        &self,
        sort_column: &ArrayRef,
        min_max_idx: (usize, usize),
    ) -> datafusion_common::Result<()> {
        // Use the group's effective range instead of the current partition range
        let Some(cur_range) = self.get_current_group_effective_range() else {
            internal_err!(
                "No effective range for current group {} at {}",
                self.cur_group_idx,
                snafu::location!()
            )?
        };

        downcast_ts_array!(
            sort_column.data_type() => (array_check_helper, sort_column, cur_range, min_max_idx),
            _ => internal_err!(
                "Unsupported data type for sort column: {:?}",
                sort_column.data_type()
            )?,
        );

        Ok(())
    }

    /// Try find data whose value exceeds the current partition range.
    ///
    /// Returns `None` if no such data is found, and `Some(idx)` where idx points to
    /// the first data that exceeds the current partition range.
    fn try_find_next_range(
        &self,
        sort_column: &ArrayRef,
    ) -> datafusion_common::Result<Option<usize>> {
        if sort_column.is_empty() {
            return Ok(None);
        }

        // check if the current partition index is out of range
        if self.cur_part_idx >= self.partition_ranges.len() {
            internal_err!(
                "Partition index out of range: {} >= {} at {}",
                self.cur_part_idx,
                self.partition_ranges.len(),
                snafu::location!()
            )?;
        }
        let cur_range = self.partition_ranges[self.cur_part_idx];

        let sort_column_iter = downcast_ts_array!(
            sort_column.data_type() => (array_iter_helper, sort_column),
            _ => internal_err!(
                "Unsupported data type for sort column: {:?}",
                sort_column.data_type()
            )?,
        );

        for (idx, val) in sort_column_iter {
            // ignore vacant time index data
            if let Some(val) = val
                && (val >= cur_range.end.value() || val < cur_range.start.value())
            {
                return Ok(Some(idx));
            }
        }

        Ok(None)
    }

    fn push_buffer(&mut self, batch: DfRecordBatch) -> datafusion_common::Result<()> {
        rb_sanity_check(&batch)?;
        match &mut self.buffer {
            PartSortBuffer::All(v) => v.push(batch),
            PartSortBuffer::Top(top, cnt) => {
                *cnt += batch.num_rows();
                top.insert_batch(batch)?;
            }
        }

        Ok(())
    }

    /// A temporary solution for stop read earlier when current group do not overlap with any of those next group
    /// If not overlap, we can stop read further input as current top k is final
    fn can_stop_early(&mut self) -> datafusion_common::Result<bool> {
        let topk_cnt = match &self.buffer {
            PartSortBuffer::Top(_, cnt) => *cnt,
            _ => return Ok(false),
        };
        // not fulfill topk yet
        if Some(topk_cnt) < self.limit {
            return Ok(false);
        }
        // else check if last value in topk is not in next group range
        let topk_buffer = self.sort_top_buffer()?;
        assert_eq!(topk_buffer.num_rows(), self.limit.unwrap());
        assert!(topk_buffer.num_rows() >= 1);
        rb_sanity_check(&topk_buffer)?;
        let min_batch = topk_buffer.slice(topk_buffer.num_rows() - 1, 1);
        rb_sanity_check(&min_batch)?;
        let min_sort_column = self.expression.evaluate_to_sort_column(&min_batch)?.values;
        let last_val = downcast_ts_array!(
            min_sort_column.data_type() => (ts_to_timestamp, min_sort_column),
            _ => internal_err!(
                "Unsupported data type for sort column: {:?}",
                min_sort_column.data_type()
            )?,
        )[0];
        let Some(last_val) = last_val else {
            return Ok(false);
        };
        let next_group_primary_end = if self.cur_group_idx + 1 < self.range_groups.len() {
            self.range_groups[self.cur_group_idx + 1].0
        } else {
            // no next group
            return Ok(false);
        };
        let descending = self.expression.options.descending;
        let not_in_next_group_range = if descending {
            last_val >= next_group_primary_end
        } else {
            last_val < next_group_primary_end
        };

        // refill topk buffer count
        self.push_buffer(topk_buffer)?;

        Ok(not_in_next_group_range)
    }

    /// Check if the given partition index is within the current group.
    fn is_in_current_group(&self, part_idx: usize) -> bool {
        if self.cur_group_idx >= self.range_groups.len() {
            return false;
        }
        let (_, start, end) = self.range_groups[self.cur_group_idx];
        part_idx >= start && part_idx < end
    }

    /// Advance to the next group. Returns true if there is a next group.
    fn advance_to_next_group(&mut self) -> bool {
        self.cur_group_idx += 1;
        self.cur_group_idx < self.range_groups.len()
    }

    /// Get the effective range for the current group.
    /// For a group of ranges with the same primary end, the effective range is
    /// the union of all ranges in the group.
    fn get_current_group_effective_range(&self) -> Option<PartitionRange> {
        if self.cur_group_idx >= self.range_groups.len() {
            return None;
        }
        let (_, start_idx, end_idx) = self.range_groups[self.cur_group_idx];
        if start_idx >= end_idx || start_idx >= self.partition_ranges.len() {
            return None;
        }

        let ranges_in_group =
            &self.partition_ranges[start_idx..end_idx.min(self.partition_ranges.len())];
        if ranges_in_group.is_empty() {
            return None;
        }

        // Compute union of all ranges in the group
        let mut min_start = ranges_in_group[0].start;
        let mut max_end = ranges_in_group[0].end;
        for range in ranges_in_group.iter().skip(1) {
            if range.start < min_start {
                min_start = range.start;
            }
            if range.end > max_end {
                max_end = range.end;
            }
        }

        Some(PartitionRange {
            start: min_start,
            end: max_end,
            num_rows: 0,   // Not used for validation
            identifier: 0, // Not used for validation
        })
    }

    /// Sort and clear the buffer and return the sorted record batch
    ///
    /// this function will return a empty record batch if the buffer is empty
    fn sort_buffer(&mut self) -> datafusion_common::Result<DfRecordBatch> {
        match &mut self.buffer {
            PartSortBuffer::All(_) => self.sort_all_buffer(),
            PartSortBuffer::Top(_, _) => self.sort_top_buffer(),
        }
    }

    /// Internal method for sorting `All` buffer (without limit).
    fn sort_all_buffer(&mut self) -> datafusion_common::Result<DfRecordBatch> {
        let PartSortBuffer::All(buffer) =
            std::mem::replace(&mut self.buffer, PartSortBuffer::All(Vec::new()))
        else {
            unreachable!("buffer type is checked before and should be All variant")
        };

        if buffer.is_empty() {
            return Ok(DfRecordBatch::new_empty(self.schema.clone()));
        }
        let mut sort_columns = Vec::with_capacity(buffer.len());
        let mut opt = None;
        for batch in buffer.iter() {
            let sort_column = self.expression.evaluate_to_sort_column(batch)?;
            opt = opt.or(sort_column.options);
            sort_columns.push(sort_column.values);
        }

        let sort_column =
            concat(&sort_columns.iter().map(|a| a.as_ref()).collect_vec()).map_err(|e| {
                DataFusionError::ArrowError(
                    Box::new(e),
                    Some(format!("Fail to concat sort columns at {}", location!())),
                )
            })?;

        let indices = sort_to_indices(&sort_column, opt, self.limit).map_err(|e| {
            DataFusionError::ArrowError(
                Box::new(e),
                Some(format!("Fail to sort to indices at {}", location!())),
            )
        })?;
        if indices.is_empty() {
            return Ok(DfRecordBatch::new_empty(self.schema.clone()));
        }

        self.check_in_range(
            &sort_column,
            (
                indices.value(0) as usize,
                indices.value(indices.len() - 1) as usize,
            ),
        )
        .inspect_err(|_e| {
            #[cfg(debug_assertions)]
            common_telemetry::error!(
                "Fail to check sort column in range at {}, current_idx: {}, num_rows: {}, err: {}",
                self.partition,
                self.cur_part_idx,
                sort_column.len(),
                _e
            );
        })?;

        // reserve memory for the concat input and sorted output
        let total_mem: usize = buffer.iter().map(|r| r.get_array_memory_size()).sum();
        self.reservation.try_grow(total_mem * 2)?;

        let full_input = concat_batches(&self.schema, &buffer).map_err(|e| {
            DataFusionError::ArrowError(
                Box::new(e),
                Some(format!(
                    "Fail to concat input batches when sorting at {}",
                    location!()
                )),
            )
        })?;

        let sorted = take_record_batch(&full_input, &indices).map_err(|e| {
            DataFusionError::ArrowError(
                Box::new(e),
                Some(format!(
                    "Fail to take result record batch when sorting at {}",
                    location!()
                )),
            )
        })?;

        drop(full_input);
        // here remove both buffer and full_input memory
        self.reservation.shrink(2 * total_mem);
        Ok(sorted)
    }

    /// Internal method for sorting `Top` buffer (with limit).
    fn sort_top_buffer(&mut self) -> datafusion_common::Result<DfRecordBatch> {
        let filter = Arc::new(RwLock::new(TopKDynamicFilters::new(Arc::new(
            DynamicFilterPhysicalExpr::new(vec![], lit(true)),
        ))));
        let new_top_buffer = TopK::try_new(
            self.partition,
            self.schema().clone(),
            vec![],
            [self.expression.clone()].into(),
            self.limit.unwrap(),
            self.context.session_config().batch_size(),
            self.context.runtime_env(),
            &self.root_metrics,
            filter,
        )?;
        let PartSortBuffer::Top(top_k, _) =
            std::mem::replace(&mut self.buffer, PartSortBuffer::Top(new_top_buffer, 0))
        else {
            unreachable!("buffer type is checked before and should be Top variant")
        };

        let mut result_stream = top_k.emit()?;
        let mut placeholder_ctx = std::task::Context::from_waker(futures::task::noop_waker_ref());
        let mut results = vec![];
        // according to the current implementation of `TopK`, the result stream will always be ready
        loop {
            match result_stream.poll_next_unpin(&mut placeholder_ctx) {
                Poll::Ready(Some(batch)) => {
                    let batch = batch?;
                    results.push(batch);
                }
                Poll::Pending => {
                    #[cfg(debug_assertions)]
                    unreachable!("TopK result stream should always be ready")
                }
                Poll::Ready(None) => {
                    break;
                }
            }
        }

        let concat_batch = concat_batches(&self.schema, &results).map_err(|e| {
            DataFusionError::ArrowError(
                Box::new(e),
                Some(format!(
                    "Fail to concat top k result record batch when sorting at {}",
                    location!()
                )),
            )
        })?;

        Ok(concat_batch)
    }

    /// Sorts current buffer and returns `None` when there is nothing to emit.
    fn sorted_buffer_if_non_empty(&mut self) -> datafusion_common::Result<Option<DfRecordBatch>> {
        if self.buffer.is_empty() {
            return Ok(None);
        }

        let sorted = self.sort_buffer()?;
        if sorted.num_rows() == 0 {
            Ok(None)
        } else {
            Ok(Some(sorted))
        }
    }

    /// Try to split the input batch if it contains data that exceeds the current partition range.
    ///
    /// When the input batch contains data that exceeds the current partition range, this function
    /// will split the input batch into two parts, the first part is within the current partition
    /// range will be merged and sorted with previous buffer, and the second part will be registered
    /// to `evaluating_batch` for next polling.
    ///
    /// **Group-based processing**: Ranges with the same primary end are grouped together.
    /// We only sort and output when transitioning to a NEW group, not when moving between
    /// ranges within the same group.
    ///
    /// Returns `None` if the input batch is empty or fully within the current partition range
    /// (or we're still collecting data within the same group), and `Some(batch)` when we've
    /// completed a group and have sorted output. When operating in TopK (limit) mode, this
    /// function will not emit intermediate batches; it only prepares state for a single final
    /// output.
    fn split_batch(
        &mut self,
        batch: DfRecordBatch,
    ) -> datafusion_common::Result<Option<DfRecordBatch>> {
        if matches!(self.buffer, PartSortBuffer::Top(_, _)) {
            self.split_batch_topk(batch)?;
            return Ok(None);
        }

        self.split_batch_all(batch)
    }

    /// Specialized splitting logic for TopK (limit) mode.
    ///
    /// We only emit once when the TopK buffer is fulfilled or when input is fully consumed.
    /// When the buffer is fulfilled and we are about to enter a new group, we stop consuming
    /// further ranges.
    fn split_batch_topk(&mut self, batch: DfRecordBatch) -> datafusion_common::Result<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }

        let sort_column = self
            .expression
            .expr
            .evaluate(&batch)?
            .into_array(batch.num_rows())?;

        let next_range_idx = self.try_find_next_range(&sort_column)?;
        let Some(idx) = next_range_idx else {
            self.push_buffer(batch)?;
            // keep polling input for next batch
            return Ok(());
        };

        let this_range = batch.slice(0, idx);
        let remaining_range = batch.slice(idx, batch.num_rows() - idx);
        if this_range.num_rows() != 0 {
            self.push_buffer(this_range)?;
        }

        // Step to next proper PartitionRange
        self.cur_part_idx += 1;

        // If we've processed all partitions, mark completion.
        if self.cur_part_idx >= self.partition_ranges.len() {
            debug_assert!(remaining_range.num_rows() == 0);
            self.input_complete = true;
            return Ok(());
        }

        // Check if we're still in the same group
        let in_same_group = self.is_in_current_group(self.cur_part_idx);

        // When TopK is fulfilled and we are switching to a new group, stop consuming further ranges if possible.
        // read from topk heap and determine whether we can stop earlier.
        if !in_same_group && self.can_stop_early()? {
            self.input_complete = true;
            self.evaluating_batch = None;
            return Ok(());
        }

        // Transition to a new group if needed
        if !in_same_group {
            self.advance_to_next_group();
        }

        let next_sort_column = sort_column.slice(idx, batch.num_rows() - idx);
        if self.try_find_next_range(&next_sort_column)?.is_some() {
            // remaining batch still contains data that exceeds the current partition range
            // register the remaining batch for next polling
            self.evaluating_batch = Some(remaining_range);
        } else if remaining_range.num_rows() != 0 {
            // remaining batch is within the current partition range
            // push to the buffer and continue polling
            self.push_buffer(remaining_range)?;
        }

        Ok(())
    }

    fn split_batch_all(
        &mut self,
        batch: DfRecordBatch,
    ) -> datafusion_common::Result<Option<DfRecordBatch>> {
        if batch.num_rows() == 0 {
            return Ok(None);
        }

        let sort_column = self
            .expression
            .expr
            .evaluate(&batch)?
            .into_array(batch.num_rows())?;

        let next_range_idx = self.try_find_next_range(&sort_column)?;
        let Some(idx) = next_range_idx else {
            self.push_buffer(batch)?;
            // keep polling input for next batch
            return Ok(None);
        };

        let this_range = batch.slice(0, idx);
        let remaining_range = batch.slice(idx, batch.num_rows() - idx);
        if this_range.num_rows() != 0 {
            self.push_buffer(this_range)?;
        }

        // Step to next proper PartitionRange
        self.cur_part_idx += 1;

        // If we've processed all partitions, sort and output
        if self.cur_part_idx >= self.partition_ranges.len() {
            // assert there is no data beyond the last partition range (remaining is empty).
            debug_assert!(remaining_range.num_rows() == 0);

            // Sort and output the final group
            return self.sorted_buffer_if_non_empty();
        }

        // Check if we're still in the same group
        if self.is_in_current_group(self.cur_part_idx) {
            // Same group - don't sort yet, keep collecting
            let next_sort_column = sort_column.slice(idx, batch.num_rows() - idx);
            if self.try_find_next_range(&next_sort_column)?.is_some() {
                // remaining batch still contains data that exceeds the current partition range
                self.evaluating_batch = Some(remaining_range);
            } else {
                // remaining batch is within the current partition range
                if remaining_range.num_rows() != 0 {
                    self.push_buffer(remaining_range)?;
                }
            }
            // Return None to continue collecting within the same group
            return Ok(None);
        }

        // Transitioning to a new group - sort current group and output
        let sorted_batch = self.sorted_buffer_if_non_empty()?;
        self.advance_to_next_group();

        let next_sort_column = sort_column.slice(idx, batch.num_rows() - idx);
        if self.try_find_next_range(&next_sort_column)?.is_some() {
            // remaining batch still contains data that exceeds the current partition range
            // register the remaining batch for next polling
            self.evaluating_batch = Some(remaining_range);
        } else {
            // remaining batch is within the current partition range
            // push to the buffer and continue polling
            if remaining_range.num_rows() != 0 {
                self.push_buffer(remaining_range)?;
            }
        }

        Ok(sorted_batch)
    }

    pub fn poll_next_inner(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<datafusion_common::Result<DfRecordBatch>>> {
        loop {
            if self.input_complete {
                if let Some(sorted_batch) = self.sorted_buffer_if_non_empty()? {
                    return Poll::Ready(Some(Ok(sorted_batch)));
                }
                return Poll::Ready(None);
            }

            // if there is a remaining batch being evaluated from last run,
            // split on it instead of fetching new batch
            if let Some(evaluating_batch) = self.evaluating_batch.take()
                && evaluating_batch.num_rows() != 0
            {
                // Check if we've already processed all partitions
                if self.cur_part_idx >= self.partition_ranges.len() {
                    // All partitions processed, discard remaining data
                    if let Some(sorted_batch) = self.sorted_buffer_if_non_empty()? {
                        return Poll::Ready(Some(Ok(sorted_batch)));
                    }
                    return Poll::Ready(None);
                }

                if let Some(sorted_batch) = self.split_batch(evaluating_batch)? {
                    return Poll::Ready(Some(Ok(sorted_batch)));
                }
                continue;
            }

            // fetch next batch from input
            let res = self.input.as_mut().poll_next(cx);
            match res {
                Poll::Ready(Some(Ok(batch))) => {
                    if let Some(sorted_batch) = self.split_batch(batch)? {
                        return Poll::Ready(Some(Ok(sorted_batch)));
                    }
                }
                // input stream end, mark and continue
                Poll::Ready(None) => {
                    self.input_complete = true;
                }
                Poll::Ready(Some(Err(e))) => return Poll::Ready(Some(Err(e))),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

impl Stream for PartSortStream {
    type Item = datafusion_common::Result<DfRecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<datafusion_common::Result<DfRecordBatch>>> {
        let result = self.as_mut().poll_next_inner(cx);
        self.metrics.record_poll(result)
    }
}

impl RecordBatchStream for PartSortStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

fn rb_sanity_check(batch: &DfRecordBatch) -> datafusion_common::Result<()> {
    let row_cnt = batch.num_rows();
    for column in batch.columns() {
        if column.len() != row_cnt {
            internal_err!(
                "RecordBatch column length mismatch: expected {}, found {} at {} for column {:?}",
                row_cnt,
                column.len(),
                snafu::location!(),
                column
            )?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use arrow::array::{
        TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
        TimestampSecondArray,
    };
    use arrow_schema::{DataType, Field, Schema, SortOptions, TimeUnit};
    use common_telemetry::{info, init_default_ut_logging};
    use common_time::Timestamp;
    use datafusion_physical_expr::expressions::Column;
    use futures::StreamExt;
    use store_api::region_engine::PartitionRange;

    use super::*;
    use crate::test_util::{MockInputExec, new_ts_array};
    use crate::window_sort::WindowedSortExec;

    macro_rules! extract_ts_values_helper {
        ($t:ty, $unit:expr, $array:expr) => {
            $array
                .as_any()
                .downcast_ref::<arrow::array::PrimitiveArray<$t>>()
                .unwrap()
                .values()
                .iter()
                .map(|&v| v as i64)
                .collect::<Vec<_>>()
        };
    }

    /// Helper function to extract timestamp values from timestamp arrays as Vec<i64>
    fn extract_ts_values(array: &ArrayRef) -> Vec<i64> {
        downcast_ts_array!(
            array.data_type() => (extract_ts_values_helper, array),
            _ => panic!("Expected timestamp array, got {:?}", array.data_type()),
        )
    }

    /// TODO(discord9): move this to fuzz test folder
    #[tokio::test]
    async fn fuzzy_test_driver() {
        init_default_ut_logging();
        let mut rng = fastrand::Rng::new();

        for _ in 0..1000 {
            let seed = rng.u64(..);
            fuzzy_test(seed, true).await;
            fuzzy_test(seed, false).await;
        }
    }

    async fn fuzzy_test(seed: u64, chain_windowed_sort: bool) {
        let test_cnt = 1;
        // bound for total count of PartitionRange
        let part_cnt_bound = 100;
        // bound for timestamp range size and offset for each PartitionRange
        let range_size_bound = 100;
        let range_offset_bound = 100;
        // bound for batch count and size within each PartitionRange
        let batch_cnt_bound = 20;
        let batch_size_bound = 100;

        let mut rng = fastrand::Rng::new();
        rng.seed(seed);

        let mut test_cases = Vec::new();

        for case_id in 0..test_cnt {
            let mut bound_val: Option<i64> = None;
            let descending = rng.bool();
            let nulls_first = rng.bool();
            let opt = SortOptions {
                descending,
                nulls_first,
            };
            let limit = if rng.bool() {
                Some(rng.usize(1..batch_cnt_bound * batch_size_bound))
            } else {
                None
            };
            let unit = match rng.u8(0..3) {
                0 => TimeUnit::Second,
                1 => TimeUnit::Millisecond,
                2 => TimeUnit::Microsecond,
                _ => TimeUnit::Nanosecond,
            };

            let schema = Schema::new(vec![Field::new(
                "ts",
                DataType::Timestamp(unit, None),
                false,
            )]);
            let schema = Arc::new(schema);

            let mut input_ranged_data = vec![];
            let mut output_ranges = vec![];
            let mut output_data = vec![];
            // generate each input `PartitionRange`
            for part_id in 0..rng.usize(0..part_cnt_bound) {
                // generate each `PartitionRange`'s timestamp range
                let (start, end) = if descending {
                    // Use 1..=range_offset_bound to ensure strictly decreasing end values
                    let end = bound_val
                        .map(
                            |i| i
                            .checked_sub(rng.i64(1..=range_offset_bound))
                            .expect("Bad luck, fuzzy test generate data that will overflow, change seed and try again")
                        )
                        .unwrap_or_else(|| rng.i64(-100000000..100000000));
                    bound_val = Some(end);
                    let start = end - rng.i64(1..range_size_bound);
                    let start = Timestamp::new(start, unit.into());
                    let end = Timestamp::new(end, unit.into());
                    (start, end)
                } else {
                    // Use 1..=range_offset_bound to ensure strictly increasing start values
                    let start = bound_val
                        .map(|i| i + rng.i64(1..=range_offset_bound))
                        .unwrap_or_else(|| rng.i64(..));
                    bound_val = Some(start);
                    let end = start + rng.i64(1..range_size_bound);
                    let start = Timestamp::new(start, unit.into());
                    let end = Timestamp::new(end, unit.into());
                    (start, end)
                };
                assert!(start < end);

                let mut per_part_sort_data = vec![];
                let mut batches = vec![];
                for _batch_idx in 0..rng.usize(1..batch_cnt_bound) {
                    let cnt = rng.usize(0..batch_size_bound) + 1;
                    let iter = 0..rng.usize(0..cnt);
                    let mut data_gen = iter
                        .map(|_| rng.i64(start.value()..end.value()))
                        .collect_vec();
                    if data_gen.is_empty() {
                        // current batch is empty, skip
                        continue;
                    }
                    // mito always sort on ASC order
                    data_gen.sort();
                    per_part_sort_data.extend(data_gen.clone());
                    let arr = new_ts_array(unit, data_gen.clone());
                    let batch = DfRecordBatch::try_new(schema.clone(), vec![arr]).unwrap();
                    batches.push(batch);
                }

                let range = PartitionRange {
                    start,
                    end,
                    num_rows: batches.iter().map(|b| b.num_rows()).sum(),
                    identifier: part_id,
                };
                input_ranged_data.push((range, batches));

                output_ranges.push(range);
                if per_part_sort_data.is_empty() {
                    continue;
                }
                output_data.extend_from_slice(&per_part_sort_data);
            }

            // adjust output data with adjacent PartitionRanges
            let mut output_data_iter = output_data.iter().peekable();
            let mut output_data = vec![];
            for range in output_ranges.clone() {
                let mut cur_data = vec![];
                while let Some(val) = output_data_iter.peek() {
                    if **val < range.start.value() || **val >= range.end.value() {
                        break;
                    }
                    cur_data.push(*output_data_iter.next().unwrap());
                }

                if cur_data.is_empty() {
                    continue;
                }

                if descending {
                    cur_data.sort_by(|a, b| b.cmp(a));
                } else {
                    cur_data.sort();
                }
                output_data.push(cur_data);
            }

            let expected_output = if let Some(limit) = limit {
                let mut accumulated = Vec::new();
                for mut range_values in output_data {
                    accumulated.append(&mut range_values);
                }

                if accumulated.is_empty() {
                    None
                } else {
                    if descending {
                        accumulated.sort_by(|a, b| b.cmp(a));
                    } else {
                        accumulated.sort();
                    }
                    accumulated.truncate(limit.min(accumulated.len()));

                    Some(
                        DfRecordBatch::try_new(
                            schema.clone(),
                            vec![new_ts_array(unit, accumulated)],
                        )
                        .unwrap(),
                    )
                }
            } else if chain_windowed_sort {
                // if chain_windowed_sort is true, we expect a single output batch sorted across all ranges
                let mut sorted = output_data.into_iter().flatten().collect_vec();
                if descending {
                    sorted.sort_by(|a, b| b.cmp(a));
                } else {
                    sorted.sort();
                }

                Some(
                    DfRecordBatch::try_new(schema.clone(), vec![new_ts_array(unit, sorted)])
                        .unwrap(),
                )
            } else {
                let batches = output_data
                    .into_iter()
                    .map(|a| {
                        DfRecordBatch::try_new(schema.clone(), vec![new_ts_array(unit, a)]).unwrap()
                    })
                    .collect_vec();
                if batches.is_empty() {
                    None
                } else {
                    Some(concat_batches(&schema, &batches).unwrap())
                }
            };

            test_cases.push((
                case_id,
                unit,
                input_ranged_data,
                schema,
                opt,
                limit,
                expected_output,
            ));
        }

        for (case_id, _unit, input_ranged_data, schema, opt, limit, expected_output) in test_cases {
            // Wrap test execution to catch panics and print seed only on failure
            info!(
                "Fuzzy test with seed {}, chain_ws={}",
                seed, chain_windowed_sort
            );
            run_test(
                case_id,
                input_ranged_data,
                schema,
                opt,
                limit,
                expected_output,
                None,
                chain_windowed_sort,
            )
            .await;
        }
    }

    /// Comprehensive test case structure that encapsulates all test parameters
    #[derive(Debug, Clone)]
    struct TestCase {
        #[allow(dead_code)]
        name: &'static str,
        unit: TimeUnit,
        input_ranges: Vec<((i64, i64), Vec<Vec<i64>>)>, // (start, end) -> data batches
        descending: bool,
        nulls_first: bool,
        limit: Option<usize>,
        expected_output: Vec<Vec<i64>>,
        expected_polled_rows: Option<usize>,
        #[allow(dead_code)]
        nullable: bool,
    }

    /// Comprehensive test suite covering all PartSort functionality
    #[tokio::test]
    async fn comprehensive_part_sort_tests() {
        let test_cases = vec![
            // ===== Basic Cases =====
            // Test basic ascending sort with overlapping ranges
            TestCase {
                name: "basic_ascending_overlapping_ranges",
                unit: TimeUnit::Millisecond,
                input_ranges: vec![
                    ((0, 10), vec![vec![1, 2, 3], vec![4, 5, 6], vec![7, 8, 9]]),
                    ((5, 10), vec![vec![5, 6], vec![7, 8]]),
                ],
                descending: false,
                nulls_first: false,
                limit: None,
                expected_output: vec![vec![1, 2, 3, 4, 5, 5, 6, 6, 7, 7, 8, 8, 9]],
                expected_polled_rows: None,
                nullable: false,
            },
            // Test descending sort with overlapping ranges that have same primary end
            TestCase {
                name: "descending_overlapping_same_primary_end",
                unit: TimeUnit::Millisecond,
                input_ranges: vec![
                    ((5, 10), vec![vec![5, 6], vec![7, 8, 9]]),
                    ((0, 10), vec![vec![1, 2, 3], vec![4, 5, 6], vec![7, 8]]),
                ],
                descending: true,
                nulls_first: false,
                limit: None,
                expected_output: vec![vec![9, 8, 8, 7, 7, 6, 6, 5, 5, 4, 3, 2, 1]],
                expected_polled_rows: None,
                nullable: false,
            },
            TestCase {
                name: "empty_first_range",
                unit: TimeUnit::Millisecond,
                input_ranges: vec![
                    ((5, 10), vec![]),
                    ((0, 10), vec![vec![1, 2, 3], vec![4, 5, 6], vec![7, 8]]),
                ],
                descending: true,
                nulls_first: false,
                limit: None,
                expected_output: vec![vec![8, 7, 6, 5, 4, 3, 2, 1]],
                expected_polled_rows: None,
                nullable: false,
            },
            TestCase {
                name: "multiple_ranges_with_empty_middle",
                unit: TimeUnit::Millisecond,
                input_ranges: vec![
                    ((15, 20), vec![vec![17, 18, 19]]),
                    ((10, 15), vec![]),
                    ((5, 10), vec![]),
                    ((0, 10), vec![vec![1, 2, 3], vec![4, 5, 6], vec![7, 8]]),
                ],
                descending: true,
                nulls_first: false,
                limit: None,
                expected_output: vec![vec![19, 18, 17], vec![8, 7, 6, 5, 4, 3, 2, 1]],
                expected_polled_rows: None,
                nullable: false,
            },
            TestCase {
                name: "all_empty_ranges",
                unit: TimeUnit::Millisecond,
                input_ranges: vec![
                    ((15, 20), vec![]),
                    ((10, 15), vec![]),
                    ((5, 10), vec![]),
                    ((0, 10), vec![]),
                ],
                descending: true,
                nulls_first: false,
                limit: None,
                expected_output: vec![],
                expected_polled_rows: None,
                nullable: false,
            },
            TestCase {
                name: "data_spanning_multiple_ranges_with_limit",
                unit: TimeUnit::Millisecond,
                input_ranges: vec![
                    (
                        (15, 20),
                        vec![vec![15, 17, 19, 10, 11, 12, 5, 6, 7, 8, 9, 1, 2, 3, 4]],
                    ),
                    ((10, 15), vec![]),
                    ((5, 10), vec![]),
                    ((0, 10), vec![]),
                ],
                descending: true,
                nulls_first: false,
                limit: Some(2),
                expected_output: vec![vec![19, 17]],
                expected_polled_rows: None,
                nullable: false,
            },
            // ===== Limit Tests =====
            TestCase {
                name: "limit_multiple_batches_single_partition",
                unit: TimeUnit::Millisecond,
                input_ranges: vec![((0, 10), vec![vec![1, 2, 3], vec![4, 5, 6], vec![7, 8, 9]])],
                descending: true,
                nulls_first: false,
                limit: Some(3),
                expected_output: vec![vec![9, 8, 7]],
                expected_polled_rows: None,
                nullable: false,
            },
            TestCase {
                name: "limit_multiple_partitions",
                unit: TimeUnit::Millisecond,
                input_ranges: vec![
                    ((10, 20), vec![vec![10, 11, 12], vec![13, 14, 15]]),
                    ((0, 10), vec![vec![1, 2, 3], vec![4, 5]]),
                ],
                descending: true,
                nulls_first: false,
                limit: Some(2),
                expected_output: vec![vec![15, 14]],
                expected_polled_rows: None,
                nullable: false,
            },
            TestCase {
                name: "limit_ascending_sort",
                unit: TimeUnit::Millisecond,
                input_ranges: vec![((0, 10), vec![vec![7, 8, 9], vec![4, 5, 6], vec![1, 2, 3]])],
                descending: false,
                nulls_first: false,
                limit: Some(2),
                expected_output: vec![vec![1, 2]],
                expected_polled_rows: None,
                nullable: false,
            },
            // ===== Early Termination Tests =====
            // Test early termination with limit - should stop after processing first partition group
            // Input: Three partitions with ranges (20,30), (10,20), (0,10), each with 2 batches of 5 values
            // With limit=2 and descending sort, we only need top 2 values [29,28] from the first partition
            // expected_polled_rows=10: Only processes first partition (10 rows) before early termination
            TestCase {
                name: "early_termination_descending",
                unit: TimeUnit::Millisecond,
                input_ranges: vec![
                    (
                        (20, 30),
                        vec![vec![21, 22, 23, 24, 25], vec![26, 27, 28, 29, 30]],
                    ),
                    (
                        (10, 20),
                        vec![vec![11, 12, 13, 14, 15], vec![16, 17, 18, 19, 20]],
                    ),
                    ((0, 10), vec![vec![1, 2, 3, 4, 5], vec![6, 7, 8, 9, 10]]),
                ],
                descending: true,
                nulls_first: false,
                limit: Some(2),
                expected_output: vec![vec![29, 28]],
                expected_polled_rows: Some(10),
                nullable: false,
            },
            // ===== Primary End Grouping Tests =====
            TestCase {
                name: "primary_end_grouping_with_limit",
                unit: TimeUnit::Millisecond,
                input_ranges: vec![
                    ((70, 100), vec![vec![80, 90, 95]]),
                    ((50, 100), vec![vec![55, 65, 75, 85, 95]]),
                ],
                descending: true,
                nulls_first: false,
                limit: Some(4),
                expected_output: vec![vec![95, 95, 90, 85]],
                expected_polled_rows: None,
                nullable: false,
            },
            // ===== Three Ranges Keep Pulling Tests =====
            TestCase {
                name: "three_ranges_keep_pulling",
                unit: TimeUnit::Millisecond,
                input_ranges: vec![
                    ((70, 100), vec![vec![80, 90, 95]]),
                    ((50, 100), vec![vec![55, 75, 85]]),
                    ((40, 95), vec![vec![45, 65, 94]]),
                ],
                descending: true,
                nulls_first: false,
                limit: Some(4),
                expected_output: vec![vec![95, 94, 90, 85]],
                expected_polled_rows: None,
                nullable: false,
            },
            // ===== Threshold-Based Early Termination Tests =====
            // Test threshold-based early termination - when threshold >= next group's primary end
            // Input: Two partitions (70,100) with [94,95,96,97,98,99] and (50,90) with [85,86,87]
            // With limit=4, threshold becomes 96 (4th largest value), next_primary_end=90
            // Since 96 >= 90, we can stop early and skip the second partition
            // expected_polled_rows=9: Only processes first partition (6 rows) + partial second (3 rows)
            TestCase {
                name: "threshold_based_early_termination_desc",
                unit: TimeUnit::Millisecond,
                input_ranges: vec![
                    ((70, 100), vec![vec![94, 95, 96, 97, 98, 99]]),
                    ((50, 90), vec![vec![85, 86, 87]]),
                ],
                descending: true,
                nulls_first: false,
                limit: Some(4),
                expected_output: vec![vec![99, 98, 97, 96]],
                expected_polled_rows: Some(9),
                nullable: false,
            },
            // Test continuing when threshold falls within next group's range
            // Input: Two partitions (70,100) with [94,95,96,97,98,99] and (50,98) with [55,60,65]
            // With limit=4, threshold=96, next_primary_end=98
            // Since 96 < 98, we cannot stop early - next group might have values > 96
            // expected_polled_rows=9: Processes first partition (6) + second partition (3)
            TestCase {
                name: "continue_when_threshold_in_next_group_range",
                unit: TimeUnit::Millisecond,
                input_ranges: vec![
                    ((70, 100), vec![vec![94, 95, 96, 97, 98, 99]]),
                    ((50, 98), vec![vec![55, 60, 65]]),
                ],
                descending: true,
                nulls_first: false,
                limit: Some(4),
                expected_output: vec![vec![99, 98, 97, 96]],
                expected_polled_rows: Some(9),
                nullable: false,
            },
            // Test ascending threshold early termination
            // Input: Three partitions (10,50), (20,60), (60,70) with values [10-15], [25,30,35], [60,61]
            // With limit=4, threshold=13 (4th smallest value), next_primary_end=20
            // Since 13 < 20, we need to check next group, but 13 < 25 (smallest in next), we can stop
            // expected_polled_rows=11: Processes first partition (6) + second partition (3) + third (2)
            TestCase {
                name: "ascending_threshold_early_termination",
                unit: TimeUnit::Millisecond,
                input_ranges: vec![
                    ((10, 50), vec![vec![10, 11, 12, 13, 14, 15]]),
                    ((20, 60), vec![vec![25, 30, 35]]),
                    ((60, 70), vec![vec![60, 61]]),
                    ((80, 90), vec![vec![80, 81]]),
                ],
                descending: false,
                nulls_first: false,
                limit: Some(4),
                expected_output: vec![vec![10, 11, 12, 13]],
                expected_polled_rows: Some(11),
                nullable: false,
            },
            // Test complex ascending threshold early termination case
            // Input: Multiple partitions with overlapping ranges and various values
            // With limit=4, we need smallest 4 values. After processing (5,25) with [5,6,7,8],
            // threshold=8, and remaining partitions have min values > 8, so we can stop early
            // expected_polled_rows=11: Processes necessary partitions until threshold condition met
            TestCase {
                name: "ascending_threshold_early_termination_case_two",
                unit: TimeUnit::Millisecond,
                input_ranges: vec![
                    ((0, 20), vec![vec![9, 10, 11, 12]]),
                    ((4, 25), vec![vec![21]]),
                    ((5, 25), vec![vec![5, 6, 7, 8]]),
                    ((42, 52), vec![vec![42, 51]]),
                    ((48, 53), vec![vec![48, 51]]),
                ],
                descending: false,
                nulls_first: false,
                limit: Some(4),
                expected_output: vec![vec![5, 6, 7, 8]],
                expected_polled_rows: Some(11),
                nullable: false,
            },
            // ===== Null Handling Tests =====

            // ===== Single Group Tests =====
            // Test early stop within a single group - partitions share same primary end
            // Input: Two partitions both ending at 100, first has [94-99], second has [85-87]
            // With limit=4, we get [99,98,97,96] from first partition, threshold=96
            // Since both partitions have same primary end (100), we cannot stop early based on range
            // expected_polled_rows=9: Processes both partitions completely (6 + 3 rows)
            TestCase {
                name: "can_early_stop_single_group",
                unit: TimeUnit::Millisecond,
                input_ranges: vec![
                    ((70, 100), vec![vec![94, 95, 96, 97, 98, 99]]),
                    ((50, 100), vec![vec![85, 86, 87]]),
                ],
                descending: true,
                nulls_first: false,
                limit: Some(4),
                expected_output: vec![vec![99, 98, 97, 96]],
                expected_polled_rows: Some(9),
                nullable: false,
            },
            // ===== Exact Boundary Equality Tests =====
            // Test exact boundary equality for descending sort - threshold equals next primary end
            // Input: Two partitions (70,100) with [92,91,90,89] and (50,90) with [88,87,86]
            // With limit=3, threshold=90, next_primary_end=90
            // Since threshold == next_primary_end, we can stop early (90 >= 90)
            // expected_polled_rows=7: Processes first partition (4) + partial second (3 rows)
            TestCase {
                name: "exact_boundary_equality_desc",
                unit: TimeUnit::Millisecond,
                input_ranges: vec![
                    ((70, 100), vec![vec![92, 91, 90, 89]]),
                    ((50, 90), vec![vec![88, 87, 86]]),
                ],
                descending: true,
                nulls_first: false,
                limit: Some(3),
                expected_output: vec![vec![92, 91, 90]],
                expected_polled_rows: Some(7),
                nullable: false,
            },
            // Test exact boundary equality for ascending sort - threshold equals next primary start
            // Input: Two partitions (10,50) with [10,15,20,25] and (20,60) with [21,22,23]
            // With limit=3, threshold=20, next_primary_start=20
            // Since threshold == next_primary_start, we can stop early (20 <= 20)
            // expected_polled_rows=7: Processes first partition (4) + partial second (3 rows)
            TestCase {
                name: "exact_boundary_equality_asc",
                unit: TimeUnit::Millisecond,
                input_ranges: vec![
                    ((10, 50), vec![vec![10, 15, 20, 25]]),
                    ((20, 60), vec![vec![21, 22, 23]]),
                ],
                descending: false,
                nulls_first: false,
                limit: Some(3),
                expected_output: vec![vec![10, 15, 20]],
                expected_polled_rows: Some(7),
                nullable: false,
            },
            // ===== Empty Partition Tests =====
            // Test early stop with empty partitions at the start
            // Input: Four partitions, first two are empty, third has [74-77], fourth has [58-60]
            // With limit=2, we get [77,76] from third partition, threshold=76, next_primary_end=60
            // Since 76 >= 60, we can stop early and skip the fourth partition
            // expected_polled_rows=7: Processes empty partitions (0) + third partition (4) + partial fourth (3)
            TestCase {
                name: "early_stop_with_empty_partitions_at_start",
                unit: TimeUnit::Millisecond,
                input_ranges: vec![
                    ((70, 100), vec![vec![]]),
                    ((50, 100), vec![vec![]]),
                    ((30, 80), vec![vec![74, 75, 76, 77]]),
                    ((10, 60), vec![vec![58, 59, 60]]),
                ],
                descending: true,
                nulls_first: false,
                limit: Some(2),
                expected_output: vec![vec![77, 76]],
                expected_polled_rows: Some(7),
                nullable: false,
            },
            // Test early stop with empty partitions between data partitions
            // Input: Four partitions with data in first and last, middle two are empty
            // First partition has [96-99], last has [48-50], limit=2 gives [99,98]
            // Threshold=98, next_primary_end=50, since 98 >= 50, we can stop early
            // expected_polled_rows=7: Processes first partition (4) + empty partitions (0) + partial last (3)
            TestCase {
                name: "early_stop_with_empty_partitions_between_data",
                unit: TimeUnit::Millisecond,
                input_ranges: vec![
                    ((70, 100), vec![vec![96, 97, 98, 99]]),
                    ((50, 90), vec![vec![]]),
                    ((30, 70), vec![vec![]]),
                    ((10, 50), vec![vec![48, 49, 50]]),
                ],
                descending: true,
                nulls_first: false,
                limit: Some(2),
                expected_output: vec![vec![99, 98]],
                expected_polled_rows: Some(7),
                nullable: false,
            },
            // ===== Empty Ranges at Beginning Tests =====
            TestCase {
                name: "early_stop_empty_ranges_beginning_desc",
                unit: TimeUnit::Millisecond,
                input_ranges: vec![
                    ((70, 100), vec![vec![]]),
                    ((50, 100), vec![vec![]]),
                    ((60, 80), vec![vec![75, 76, 77, 78]]),
                    ((40, 70), vec![vec![65, 66, 67]]),
                ],
                descending: true,
                nulls_first: false,
                limit: Some(2),
                expected_output: vec![vec![78, 77]],
                expected_polled_rows: Some(7),
                nullable: false,
            },
            // ===== Empty Ranges Threshold Calculation Tests =====
            TestCase {
                name: "early_stop_empty_ranges_threshold_calculation",
                unit: TimeUnit::Millisecond,
                input_ranges: vec![
                    ((70, 100), vec![vec![94, 95, 96, 97, 98]]),
                    ((50, 100), vec![vec![]]),
                    ((60, 90), vec![vec![84, 85, 86, 87]]),
                    ((40, 90), vec![vec![]]),
                    ((30, 80), vec![vec![75, 76, 77]]),
                ],
                descending: true,
                nulls_first: false,
                limit: Some(3),
                expected_output: vec![vec![98, 97, 96]],
                expected_polled_rows: Some(12),
                nullable: false,
            },
            // ===== Consecutive Empty Ranges Tests =====
            TestCase {
                name: "consecutive_empty_ranges_early_stop_desc",
                unit: TimeUnit::Millisecond,
                input_ranges: vec![
                    ((90, 120), vec![vec![]]),
                    ((80, 110), vec![vec![]]),
                    ((70, 100), vec![vec![95, 96, 97, 98, 99]]),
                    ((60, 90), vec![vec![85, 86, 87, 88, 89]]),
                    ((40, 80), vec![vec![75, 76, 77]]),
                ],
                descending: true,
                nulls_first: false,
                limit: Some(4),
                expected_output: vec![vec![99, 98, 97, 96]],
                expected_polled_rows: Some(13),
                nullable: false,
            },
            // ===== Empty Ranges Exact Boundary Tests =====
            TestCase {
                name: "empty_ranges_exact_boundary_early_stop",
                unit: TimeUnit::Millisecond,
                input_ranges: vec![
                    ((80, 100), vec![vec![]]),
                    ((70, 100), vec![vec![96, 97, 98, 99]]),
                    ((60, 90), vec![vec![85, 86, 87, 88, 89]]),
                ],
                descending: true,
                nulls_first: false,
                limit: Some(3),
                expected_output: vec![vec![99, 98, 97]],
                expected_polled_rows: Some(9),
                nullable: false,
            },
            // ===== Empty Ranges Middle Processing Tests =====
            TestCase {
                name: "empty_ranges_middle_processing_early_stop",
                unit: TimeUnit::Millisecond,
                input_ranges: vec![
                    ((90, 100), vec![vec![94, 95, 96, 97, 98]]),
                    ((60, 90), vec![vec![]]),
                    ((50, 90), vec![vec![]]),
                    ((40, 80), vec![vec![75, 76, 77, 78, 79]]),
                ],
                descending: true,
                nulls_first: false,
                limit: Some(4),
                expected_output: vec![vec![98, 97, 96, 95]],
                expected_polled_rows: Some(10),
                nullable: false,
            },
            // ===== Two Non-Overlapping Ranges Tests =====
            // Test early stop with two non-overlapping ranges - limit reached in first
            // Input: Two partitions (85,100) with [90-94] and (65,80) with [70-74], no overlap
            // With limit=3, we get [94,93,92] from first partition, threshold=92
            // Since ranges don't overlap and we have enough values, we can stop early
            // expected_polled_rows=10: Processes first partition completely (5) + second partition (5)(which is needed to make sure first partition is completed)
            TestCase {
                name: "two_no_overlap_range_limit_reached_in_first",
                unit: TimeUnit::Millisecond,
                input_ranges: vec![
                    ((85, 100), vec![vec![90, 91, 92, 93, 94]]),
                    ((65, 80), vec![vec![70, 71, 72, 73, 74]]),
                ],
                descending: true,
                nulls_first: false,
                limit: Some(3),
                expected_output: vec![vec![94, 93, 92]],
                expected_polled_rows: Some(10),
                nullable: false,
            },
            TestCase {
                name: "two_no_overlap_range_need_both_groups",
                unit: TimeUnit::Millisecond,
                input_ranges: vec![
                    ((85, 100), vec![vec![90, 91, 92, 93, 94]]),
                    ((65, 80), vec![vec![70, 71, 72, 73, 74]]),
                ],
                descending: true,
                nulls_first: false,
                limit: Some(7),
                expected_output: vec![vec![94, 93, 92, 91, 90, 74, 73]],
                expected_polled_rows: Some(10),
                nullable: false,
            },
        ];

        for (case_id, test_case) in test_cases.into_iter().enumerate() {
            run_comprehensive_test(case_id, test_case).await;
        }
    }

    /// Test early stop behavior with null values in sort column.
    /// Verifies that nulls are handled correctly based on nulls_first option.
    #[tokio::test]
    async fn test_early_stop_with_nulls() {
        let unit = TimeUnit::Millisecond;
        let schema = Arc::new(Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(unit, None),
            true, // nullable
        )]));

        // Helper function to create nullable timestamp arrays
        let new_nullable_ts_array = |unit: TimeUnit, arr: Vec<Option<i64>>| -> ArrayRef {
            match unit {
                TimeUnit::Second => Arc::new(TimestampSecondArray::from(arr)) as ArrayRef,
                TimeUnit::Millisecond => Arc::new(TimestampMillisecondArray::from(arr)) as ArrayRef,
                TimeUnit::Microsecond => Arc::new(TimestampMicrosecondArray::from(arr)) as ArrayRef,
                TimeUnit::Nanosecond => Arc::new(TimestampNanosecondArray::from(arr)) as ArrayRef,
            }
        };

        // Test case 1: nulls_first=true, null values should appear first
        // Group 1 (end=100): [null, null, 99, 98, 97] -> with limit=3, top 3 are [null, null, 99]
        // Threshold is 99, next group end=90, since 99 >= 90, we should stop early
        let input_ranged_data = vec![
            (
                PartitionRange {
                    start: Timestamp::new(70, unit.into()),
                    end: Timestamp::new(100, unit.into()),
                    num_rows: 5,
                    identifier: 0,
                },
                vec![
                    DfRecordBatch::try_new(
                        schema.clone(),
                        vec![new_nullable_ts_array(
                            unit,
                            vec![Some(99), Some(98), None, Some(97), None],
                        )],
                    )
                    .unwrap(),
                ],
            ),
            (
                PartitionRange {
                    start: Timestamp::new(50, unit.into()),
                    end: Timestamp::new(90, unit.into()),
                    num_rows: 3,
                    identifier: 1,
                },
                vec![
                    DfRecordBatch::try_new(
                        schema.clone(),
                        vec![new_nullable_ts_array(
                            unit,
                            vec![Some(89), Some(88), Some(87)],
                        )],
                    )
                    .unwrap(),
                ],
            ),
        ];

        // With nulls_first=true, nulls sort before all values
        // For descending, order is: null, null, 99, 98, 97
        // With limit=3, we get: null, null, 99
        let expected_output = Some(
            DfRecordBatch::try_new(
                schema.clone(),
                vec![new_nullable_ts_array(unit, vec![None, None, Some(99)])],
            )
            .unwrap(),
        );

        run_test(
            3000,
            input_ranged_data,
            schema.clone(),
            SortOptions {
                descending: true,
                nulls_first: true,
            },
            Some(3),
            expected_output,
            Some(8), // Must read both batches to detect group boundary
            false,
        )
        .await;

        // Test case 2: nulls_last=true, null values should appear last
        // Group 1 (end=100): [99, 98, 97, null, null] -> with limit=3, top 3 are [99, 98, 97]
        // Threshold is 97, next group end=90, since 97 >= 90, we should stop early
        let input_ranged_data = vec![
            (
                PartitionRange {
                    start: Timestamp::new(70, unit.into()),
                    end: Timestamp::new(100, unit.into()),
                    num_rows: 5,
                    identifier: 0,
                },
                vec![
                    DfRecordBatch::try_new(
                        schema.clone(),
                        vec![new_nullable_ts_array(
                            unit,
                            vec![Some(99), Some(98), Some(97), None, None],
                        )],
                    )
                    .unwrap(),
                ],
            ),
            (
                PartitionRange {
                    start: Timestamp::new(50, unit.into()),
                    end: Timestamp::new(90, unit.into()),
                    num_rows: 3,
                    identifier: 1,
                },
                vec![
                    DfRecordBatch::try_new(
                        schema.clone(),
                        vec![new_nullable_ts_array(
                            unit,
                            vec![Some(89), Some(88), Some(87)],
                        )],
                    )
                    .unwrap(),
                ],
            ),
        ];

        // With nulls_last=false (equivalent to nulls_first=false), values sort before nulls
        // For descending, order is: 99, 98, 97, null, null
        // With limit=3, we get: 99, 98, 97
        let expected_output = Some(
            DfRecordBatch::try_new(
                schema.clone(),
                vec![new_nullable_ts_array(
                    unit,
                    vec![Some(99), Some(98), Some(97)],
                )],
            )
            .unwrap(),
        );

        run_test(
            3001,
            input_ranged_data,
            schema.clone(),
            SortOptions {
                descending: true,
                nulls_first: false,
            },
            Some(3),
            expected_output,
            Some(8), // Must read both batches to detect group boundary
            false,
        )
        .await;
    }

    /// Run a comprehensive test case
    async fn run_comprehensive_test(case_id: usize, test_case: TestCase) {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(test_case.unit, None),
            false,
        )]));

        let opt = SortOptions {
            descending: test_case.descending,
            nulls_first: test_case.nulls_first,
        };

        let input_ranged_data = test_case
            .input_ranges
            .into_iter()
            .enumerate()
            .map(|(identifier, (range, data))| {
                let part = PartitionRange {
                    start: Timestamp::new(range.0, test_case.unit.into()),
                    end: Timestamp::new(range.1, test_case.unit.into()),
                    num_rows: data.iter().map(|b| b.len()).sum(),
                    identifier,
                };

                let batches = data
                    .into_iter()
                    .map(|b| {
                        let arr = new_ts_array(test_case.unit, b);
                        DfRecordBatch::try_new(schema.clone(), vec![arr]).unwrap()
                    })
                    .collect_vec();
                (part, batches)
            })
            .collect_vec();

        let expected_output = if test_case.expected_output.is_empty() {
            None
        } else {
            let batches = test_case
                .expected_output
                .into_iter()
                .map(|a| {
                    let arr = new_ts_array(test_case.unit, a);
                    DfRecordBatch::try_new(schema.clone(), vec![arr]).unwrap()
                })
                .collect_vec();
            Some(concat_batches(&schema, &batches).unwrap())
        };

        run_test(
            case_id,
            input_ranged_data,
            schema.clone(),
            opt,
            test_case.limit,
            expected_output,
            test_case.expected_polled_rows,
            false,
        )
        .await;
    }

    #[allow(clippy::print_stdout)]
    #[allow(clippy::too_many_arguments)]
    async fn run_test(
        case_id: usize,
        input_ranged_data: Vec<(PartitionRange, Vec<DfRecordBatch>)>,
        schema: SchemaRef,
        opt: SortOptions,
        limit: Option<usize>,
        expected_output: Option<DfRecordBatch>,
        expected_polled_rows: Option<usize>,
        chain_windowed_sort: bool,
    ) {
        if let (Some(limit), Some(rb)) = (limit, &expected_output) {
            assert!(
                rb.num_rows() <= limit,
                "Expect row count in expected output({}) <= limit({})",
                rb.num_rows(),
                limit
            );
        }

        let mut data_partition = Vec::with_capacity(input_ranged_data.len());
        let mut ranges = Vec::with_capacity(input_ranged_data.len());
        for (part_range, batches) in input_ranged_data {
            data_partition.push(batches);
            ranges.push(part_range);
        }

        let mock_input = Arc::new(MockInputExec::new(data_partition, schema.clone()));

        let part_sort_exec = PartSortExec::try_new(
            PhysicalSortExpr {
                expr: Arc::new(Column::new("ts", 0)),
                options: opt,
            },
            limit,
            vec![ranges.clone()],
            mock_input.clone(),
        )
        .unwrap();

        let exec: Arc<dyn ExecutionPlan> = if chain_windowed_sort {
            Arc::new(
                WindowedSortExec::try_new(
                    PhysicalSortExpr {
                        expr: Arc::new(Column::new("ts", 0)),
                        options: opt,
                    },
                    limit,
                    vec![ranges],
                    Arc::new(part_sort_exec),
                )
                .unwrap(),
            )
        } else {
            Arc::new(part_sort_exec)
        };

        let exec_stream = exec.execute(0, Arc::new(TaskContext::default())).unwrap();

        let real_output = exec_stream.map(|r| r.unwrap()).collect::<Vec<_>>().await;
        if limit.is_some() && !chain_windowed_sort {
            assert!(
                real_output.len() <= 1,
                "case_{case_id} expects a single output batch when limit is set, got {}",
                real_output.len()
            );
        }

        let actual_output = if real_output.is_empty() {
            None
        } else {
            Some(concat_batches(&schema, &real_output).unwrap())
        };

        if let Some(expected_polled_rows) = expected_polled_rows {
            let input_pulled_rows = mock_input.metrics().unwrap().output_rows().unwrap();
            assert_eq!(input_pulled_rows, expected_polled_rows);
        }

        match (actual_output, expected_output) {
            (None, None) => {}
            (Some(actual), Some(expected)) => {
                if actual != expected {
                    // Extract timestamp values as Vec<i64> for comparison
                    let actual_ts_values = extract_ts_values(actual.column(0));
                    let expected_ts_values = extract_ts_values(expected.column(0));

                    panic!(
                        "case_{} failed (limit {limit:?}), opt: {:?},\nactual_ts_values: {:?}\nexpected_ts_values: {:?}",
                        case_id, opt, actual_ts_values, expected_ts_values,
                    );
                }
            }
            (None, Some(expected)) => {
                if expected.num_rows() != 0 {
                    panic!(
                        "case_{} failed (limit {limit:?}), opt: {:?},\nreal output is empty, expected {} rows",
                        case_id,
                        opt,
                        expected.num_rows()
                    )
                }
            }
            (Some(actual), None) => {
                if actual.num_rows() != 0 {
                    panic!(
                        "case_{} failed (limit {limit:?}), opt: {:?},\nreal output has {} rows, expected empty",
                        case_id,
                        opt,
                        actual.num_rows()
                    )
                }
            }
        }
    }

    /// Test that verifies the limit is correctly applied per partition when
    /// multiple batches are received for the same partition.
    #[tokio::test]
    async fn test_limit_with_multiple_batches_per_partition() {
        let unit = TimeUnit::Millisecond;
        let schema = Arc::new(Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(unit, None),
            false,
        )]));

        // Test case: Multiple batches in a single partition with limit=3
        // Input: 3 batches with [1,2,3], [4,5,6], [7,8,9] all in partition (0,10)
        // Expected: Only top 3 values [9,8,7] for descending sort
        let input_ranged_data = vec![(
            PartitionRange {
                start: Timestamp::new(0, unit.into()),
                end: Timestamp::new(10, unit.into()),
                num_rows: 9,
                identifier: 0,
            },
            vec![
                DfRecordBatch::try_new(schema.clone(), vec![new_ts_array(unit, vec![1, 2, 3])])
                    .unwrap(),
                DfRecordBatch::try_new(schema.clone(), vec![new_ts_array(unit, vec![4, 5, 6])])
                    .unwrap(),
                DfRecordBatch::try_new(schema.clone(), vec![new_ts_array(unit, vec![7, 8, 9])])
                    .unwrap(),
            ],
        )];

        let expected_output = Some(
            DfRecordBatch::try_new(schema.clone(), vec![new_ts_array(unit, vec![9, 8, 7])])
                .unwrap(),
        );

        run_test(
            1000,
            input_ranged_data,
            schema.clone(),
            SortOptions {
                descending: true,
                ..Default::default()
            },
            Some(3),
            expected_output,
            None,
            false,
        )
        .await;

        // Test case: Multiple batches across multiple partitions with limit=2
        // Partition 0: batches [10,11,12], [13,14,15] -> top 2 descending = [15,14]
        // Partition 1: batches [1,2,3], [4,5] -> top 2 descending = [5,4]
        let input_ranged_data = vec![
            (
                PartitionRange {
                    start: Timestamp::new(10, unit.into()),
                    end: Timestamp::new(20, unit.into()),
                    num_rows: 6,
                    identifier: 0,
                },
                vec![
                    DfRecordBatch::try_new(
                        schema.clone(),
                        vec![new_ts_array(unit, vec![10, 11, 12])],
                    )
                    .unwrap(),
                    DfRecordBatch::try_new(
                        schema.clone(),
                        vec![new_ts_array(unit, vec![13, 14, 15])],
                    )
                    .unwrap(),
                ],
            ),
            (
                PartitionRange {
                    start: Timestamp::new(0, unit.into()),
                    end: Timestamp::new(10, unit.into()),
                    num_rows: 5,
                    identifier: 1,
                },
                vec![
                    DfRecordBatch::try_new(schema.clone(), vec![new_ts_array(unit, vec![1, 2, 3])])
                        .unwrap(),
                    DfRecordBatch::try_new(schema.clone(), vec![new_ts_array(unit, vec![4, 5])])
                        .unwrap(),
                ],
            ),
        ];

        let expected_output = Some(
            DfRecordBatch::try_new(schema.clone(), vec![new_ts_array(unit, vec![15, 14])]).unwrap(),
        );

        run_test(
            1001,
            input_ranged_data,
            schema.clone(),
            SortOptions {
                descending: true,
                ..Default::default()
            },
            Some(2),
            expected_output,
            None,
            false,
        )
        .await;

        // Test case: Ascending sort with limit
        // Partition: batches [7,8,9], [4,5,6], [1,2,3] -> top 2 ascending = [1,2]
        let input_ranged_data = vec![(
            PartitionRange {
                start: Timestamp::new(0, unit.into()),
                end: Timestamp::new(10, unit.into()),
                num_rows: 9,
                identifier: 0,
            },
            vec![
                DfRecordBatch::try_new(schema.clone(), vec![new_ts_array(unit, vec![7, 8, 9])])
                    .unwrap(),
                DfRecordBatch::try_new(schema.clone(), vec![new_ts_array(unit, vec![4, 5, 6])])
                    .unwrap(),
                DfRecordBatch::try_new(schema.clone(), vec![new_ts_array(unit, vec![1, 2, 3])])
                    .unwrap(),
            ],
        )];

        let expected_output = Some(
            DfRecordBatch::try_new(schema.clone(), vec![new_ts_array(unit, vec![1, 2])]).unwrap(),
        );

        run_test(
            1002,
            input_ranged_data,
            schema.clone(),
            SortOptions {
                descending: false,
                ..Default::default()
            },
            Some(2),
            expected_output,
            None,
            false,
        )
        .await;
    }

    /// Test that verifies early termination behavior.
    /// Once we've produced limit * num_partitions rows, we should stop
    /// pulling from input stream.
    #[tokio::test]
    async fn test_early_termination() {
        let unit = TimeUnit::Millisecond;
        let schema = Arc::new(Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(unit, None),
            false,
        )]));

        // Create 3 partitions, each with more data than the limit
        // limit=2 per partition, so total expected output = 6 rows
        // After producing 6 rows, early termination should kick in
        // For descending sort, ranges must be ordered by (end DESC, start DESC)
        let input_ranged_data = vec![
            (
                PartitionRange {
                    start: Timestamp::new(20, unit.into()),
                    end: Timestamp::new(30, unit.into()),
                    num_rows: 10,
                    identifier: 2,
                },
                vec![
                    DfRecordBatch::try_new(
                        schema.clone(),
                        vec![new_ts_array(unit, vec![21, 22, 23, 24, 25])],
                    )
                    .unwrap(),
                    DfRecordBatch::try_new(
                        schema.clone(),
                        vec![new_ts_array(unit, vec![26, 27, 28, 29, 30])],
                    )
                    .unwrap(),
                ],
            ),
            (
                PartitionRange {
                    start: Timestamp::new(10, unit.into()),
                    end: Timestamp::new(20, unit.into()),
                    num_rows: 10,
                    identifier: 1,
                },
                vec![
                    DfRecordBatch::try_new(
                        schema.clone(),
                        vec![new_ts_array(unit, vec![11, 12, 13, 14, 15])],
                    )
                    .unwrap(),
                    DfRecordBatch::try_new(
                        schema.clone(),
                        vec![new_ts_array(unit, vec![16, 17, 18, 19, 20])],
                    )
                    .unwrap(),
                ],
            ),
            (
                PartitionRange {
                    start: Timestamp::new(0, unit.into()),
                    end: Timestamp::new(10, unit.into()),
                    num_rows: 10,
                    identifier: 0,
                },
                vec![
                    DfRecordBatch::try_new(
                        schema.clone(),
                        vec![new_ts_array(unit, vec![1, 2, 3, 4, 5])],
                    )
                    .unwrap(),
                    DfRecordBatch::try_new(
                        schema.clone(),
                        vec![new_ts_array(unit, vec![6, 7, 8, 9, 10])],
                    )
                    .unwrap(),
                ],
            ),
        ];

        // PartSort won't reorder `PartitionRange` (it assumes it's already ordered), so it will not read other partitions.
        // This case is just to verify that early termination works as expected.
        // First partition [20, 30) produces top 2 values: 29, 28
        let expected_output = Some(
            DfRecordBatch::try_new(schema.clone(), vec![new_ts_array(unit, vec![29, 28])]).unwrap(),
        );

        run_test(
            1003,
            input_ranged_data,
            schema.clone(),
            SortOptions {
                descending: true,
                ..Default::default()
            },
            Some(2),
            expected_output,
            Some(10),
            false,
        )
        .await;
    }

    /// Example:
    /// - Range [70, 100) has data [80, 90, 95]
    /// - Range [50, 100) has data [55, 65, 75, 85, 95]
    #[tokio::test]
    async fn test_primary_end_grouping_with_limit() {
        let unit = TimeUnit::Millisecond;
        let schema = Arc::new(Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(unit, None),
            false,
        )]));

        // Two ranges with the same end (100) - they should be grouped together
        // For descending, ranges are ordered by (end DESC, start DESC)
        // So [70, 100) comes before [50, 100) (70 > 50)
        let input_ranged_data = vec![
            (
                PartitionRange {
                    start: Timestamp::new(70, unit.into()),
                    end: Timestamp::new(100, unit.into()),
                    num_rows: 3,
                    identifier: 0,
                },
                vec![
                    DfRecordBatch::try_new(
                        schema.clone(),
                        vec![new_ts_array(unit, vec![80, 90, 95])],
                    )
                    .unwrap(),
                ],
            ),
            (
                PartitionRange {
                    start: Timestamp::new(50, unit.into()),
                    end: Timestamp::new(100, unit.into()),
                    num_rows: 5,
                    identifier: 1,
                },
                vec![
                    DfRecordBatch::try_new(
                        schema.clone(),
                        vec![new_ts_array(unit, vec![55, 65, 75, 85, 95])],
                    )
                    .unwrap(),
                ],
            ),
        ];

        // With limit=4, descending: top 4 values from combined data
        // Combined: [80, 90, 95, 55, 65, 75, 85, 95] -> sorted desc: [95, 95, 90, 85, 80, 75, 65, 55]
        // Top 4: [95, 95, 90, 85]
        let expected_output = Some(
            DfRecordBatch::try_new(
                schema.clone(),
                vec![new_ts_array(unit, vec![95, 95, 90, 85])],
            )
            .unwrap(),
        );

        run_test(
            2000,
            input_ranged_data,
            schema.clone(),
            SortOptions {
                descending: true,
                ..Default::default()
            },
            Some(4),
            expected_output,
            None,
            false,
        )
        .await;
    }

    /// Test early termination based on threshold comparison with next group.
    /// When the threshold (smallest value for descending) is >= next group's primary end,
    /// we can stop early because the next group cannot have better values.
    #[tokio::test]
    async fn test_threshold_based_early_termination() {
        let unit = TimeUnit::Millisecond;
        let schema = Arc::new(Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(unit, None),
            false,
        )]));

        // Group 1 (end=100) has 6 rows, TopK will keep top 4
        // Group 2 (end=90) has 3 rows - should NOT be processed because
        // threshold (96) >= next_primary_end (90)
        let input_ranged_data = vec![
            (
                PartitionRange {
                    start: Timestamp::new(70, unit.into()),
                    end: Timestamp::new(100, unit.into()),
                    num_rows: 6,
                    identifier: 0,
                },
                vec![
                    DfRecordBatch::try_new(
                        schema.clone(),
                        vec![new_ts_array(unit, vec![94, 95, 96, 97, 98, 99])],
                    )
                    .unwrap(),
                ],
            ),
            (
                PartitionRange {
                    start: Timestamp::new(50, unit.into()),
                    end: Timestamp::new(90, unit.into()),
                    num_rows: 3,
                    identifier: 1,
                },
                vec![
                    DfRecordBatch::try_new(
                        schema.clone(),
                        vec![new_ts_array(unit, vec![85, 86, 87])],
                    )
                    .unwrap(),
                ],
            ),
        ];

        // With limit=4, descending: top 4 from group 1 are [99, 98, 97, 96]
        // Threshold is 96, next group's primary_end is 90
        // Since 96 >= 90, we stop after group 1
        let expected_output = Some(
            DfRecordBatch::try_new(
                schema.clone(),
                vec![new_ts_array(unit, vec![99, 98, 97, 96])],
            )
            .unwrap(),
        );

        run_test(
            2002,
            input_ranged_data,
            schema.clone(),
            SortOptions {
                descending: true,
                ..Default::default()
            },
            Some(4),
            expected_output,
            Some(9), // Pull both batches since all rows fall within the first range
            false,
        )
        .await;
    }

    /// Test that we continue to next group when threshold is within next group's range.
    /// Even after fulfilling limit, if threshold < next_primary_end (descending),
    /// we would need to continue... but limit exhaustion stops us first.
    #[tokio::test]
    async fn test_continue_when_threshold_in_next_group_range() {
        let unit = TimeUnit::Millisecond;
        let schema = Arc::new(Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(unit, None),
            false,
        )]));

        // Group 1 (end=100) has 6 rows, TopK will keep top 4
        // Group 2 (end=98) has 3 rows - threshold (96) < 98, so next group
        // could theoretically have better values. But limit exhaustion stops us.
        // Note: Data values must not overlap between ranges to avoid ambiguity.
        let input_ranged_data = vec![
            (
                PartitionRange {
                    start: Timestamp::new(70, unit.into()),
                    end: Timestamp::new(100, unit.into()),
                    num_rows: 6,
                    identifier: 0,
                },
                vec![
                    DfRecordBatch::try_new(
                        schema.clone(),
                        vec![new_ts_array(unit, vec![94, 95, 96, 97, 98, 99])],
                    )
                    .unwrap(),
                ],
            ),
            (
                PartitionRange {
                    start: Timestamp::new(50, unit.into()),
                    end: Timestamp::new(98, unit.into()),
                    num_rows: 3,
                    identifier: 1,
                },
                vec![
                    // Values must be < 70 (outside group 1's range) to avoid ambiguity
                    DfRecordBatch::try_new(
                        schema.clone(),
                        vec![new_ts_array(unit, vec![55, 60, 65])],
                    )
                    .unwrap(),
                ],
            ),
        ];

        // With limit=4, we get [99, 98, 97, 96] from group 1
        // Threshold is 96, next group's primary_end is 98
        // 96 < 98, so threshold check says "could continue"
        // But limit is exhausted (0), so we stop anyway
        let expected_output = Some(
            DfRecordBatch::try_new(
                schema.clone(),
                vec![new_ts_array(unit, vec![99, 98, 97, 96])],
            )
            .unwrap(),
        );

        // Note: We pull 9 rows (both batches) because we need to read batch 2
        // to detect the group boundary, even though we stop after outputting group 1.
        run_test(
            2003,
            input_ranged_data,
            schema.clone(),
            SortOptions {
                descending: true,
                ..Default::default()
            },
            Some(4),
            expected_output,
            Some(9), // Pull both batches to detect boundary
            false,
        )
        .await;
    }

    /// Test ascending sort with threshold-based early termination.
    #[tokio::test]
    async fn test_ascending_threshold_early_termination() {
        let unit = TimeUnit::Millisecond;
        let schema = Arc::new(Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(unit, None),
            false,
        )]));

        // For ascending: primary_end is start, ranges sorted by (start ASC, end ASC)
        // Group 1 (start=10) has 6 rows
        // Group 2 (start=20) has 3 rows - should NOT be processed because
        // threshold (13) < next_primary_end (20)
        let input_ranged_data = vec![
            (
                PartitionRange {
                    start: Timestamp::new(10, unit.into()),
                    end: Timestamp::new(50, unit.into()),
                    num_rows: 6,
                    identifier: 0,
                },
                vec![
                    DfRecordBatch::try_new(
                        schema.clone(),
                        vec![new_ts_array(unit, vec![10, 11, 12, 13, 14, 15])],
                    )
                    .unwrap(),
                ],
            ),
            (
                PartitionRange {
                    start: Timestamp::new(20, unit.into()),
                    end: Timestamp::new(60, unit.into()),
                    num_rows: 3,
                    identifier: 1,
                },
                vec![
                    DfRecordBatch::try_new(
                        schema.clone(),
                        vec![new_ts_array(unit, vec![25, 30, 35])],
                    )
                    .unwrap(),
                ],
            ),
            // still read this batch to detect group boundary(?)
            (
                PartitionRange {
                    start: Timestamp::new(60, unit.into()),
                    end: Timestamp::new(70, unit.into()),
                    num_rows: 2,
                    identifier: 1,
                },
                vec![
                    DfRecordBatch::try_new(schema.clone(), vec![new_ts_array(unit, vec![60, 61])])
                        .unwrap(),
                ],
            ),
            // after boundary detected, this following one should not be read
            (
                PartitionRange {
                    start: Timestamp::new(61, unit.into()),
                    end: Timestamp::new(70, unit.into()),
                    num_rows: 2,
                    identifier: 1,
                },
                vec![
                    DfRecordBatch::try_new(schema.clone(), vec![new_ts_array(unit, vec![71, 72])])
                        .unwrap(),
                ],
            ),
        ];

        // With limit=4, ascending: top 4 (smallest) from group 1 are [10, 11, 12, 13]
        // Threshold is 13 (largest in top-k), next group's primary_end is 20
        // Since 13 < 20, we stop after group 1 (no value in group 2 can be < 13)
        let expected_output = Some(
            DfRecordBatch::try_new(
                schema.clone(),
                vec![new_ts_array(unit, vec![10, 11, 12, 13])],
            )
            .unwrap(),
        );

        run_test(
            2004,
            input_ranged_data,
            schema.clone(),
            SortOptions {
                descending: false,
                ..Default::default()
            },
            Some(4),
            expected_output,
            Some(11), // Pull first two batches to detect boundary
            false,
        )
        .await;
    }

    /// Test early stop behavior with null values in sort column.
    /// Verifies that nulls are handled correctly based on nulls_first option.
    /// This test covers the null handling scenarios that were previously in separate test functions.
    #[tokio::test]
    async fn test_null_handling_comprehensive() {
        let unit = TimeUnit::Millisecond;
        let schema = Arc::new(Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(unit, None),
            true, // nullable
        )]));

        // Helper function to create nullable timestamp array
        let new_nullable_ts_array = |unit: TimeUnit, arr: Vec<Option<i64>>| -> ArrayRef {
            match unit {
                TimeUnit::Second => Arc::new(TimestampSecondArray::from(arr)) as ArrayRef,
                TimeUnit::Millisecond => Arc::new(TimestampMillisecondArray::from(arr)) as ArrayRef,
                TimeUnit::Microsecond => Arc::new(TimestampMicrosecondArray::from(arr)) as ArrayRef,
                TimeUnit::Nanosecond => Arc::new(TimestampNanosecondArray::from(arr)) as ArrayRef,
            }
        };

        // Test case 1: nulls_first=true, null values should appear first
        // Group 1 (end=100): [null, null, 99, 98, 97] -> with limit=3, top 3 are [null, null, 99]
        // Threshold is 99, next group end=90, since 99 >= 90, we should stop early
        let input_ranged_data = vec![
            (
                PartitionRange {
                    start: Timestamp::new(70, unit.into()),
                    end: Timestamp::new(100, unit.into()),
                    num_rows: 5,
                    identifier: 0,
                },
                vec![
                    DfRecordBatch::try_new(
                        schema.clone(),
                        vec![new_nullable_ts_array(
                            unit,
                            vec![Some(99), Some(98), None, Some(97), None],
                        )],
                    )
                    .unwrap(),
                ],
            ),
            (
                PartitionRange {
                    start: Timestamp::new(50, unit.into()),
                    end: Timestamp::new(90, unit.into()),
                    num_rows: 3,
                    identifier: 1,
                },
                vec![
                    DfRecordBatch::try_new(
                        schema.clone(),
                        vec![new_nullable_ts_array(
                            unit,
                            vec![Some(89), Some(88), Some(87)],
                        )],
                    )
                    .unwrap(),
                ],
            ),
        ];

        // With nulls_first=true, nulls sort before all values
        // For descending, order is: null, null, 99, 98, 97
        // With limit=3, we get: null, null, 99
        let expected_output = Some(
            DfRecordBatch::try_new(
                schema.clone(),
                vec![new_nullable_ts_array(unit, vec![None, None, Some(99)])],
            )
            .unwrap(),
        );

        run_test(
            3000,
            input_ranged_data,
            schema.clone(),
            SortOptions {
                descending: true,
                nulls_first: true,
            },
            Some(3),
            expected_output,
            Some(8), // Must read both batches to detect group boundary
            false,
        )
        .await;

        // Test case 2: nulls_last=true, null values should appear last
        // Group 1 (end=100): [99, 98, 97, null, null] -> with limit=3, top 3 are [99, 98, 97]
        // Threshold is 97, next group end=90, since 97 >= 90, we should stop early
        let input_ranged_data = vec![
            (
                PartitionRange {
                    start: Timestamp::new(70, unit.into()),
                    end: Timestamp::new(100, unit.into()),
                    num_rows: 5,
                    identifier: 0,
                },
                vec![
                    DfRecordBatch::try_new(
                        schema.clone(),
                        vec![new_nullable_ts_array(
                            unit,
                            vec![Some(99), Some(98), Some(97), None, None],
                        )],
                    )
                    .unwrap(),
                ],
            ),
            (
                PartitionRange {
                    start: Timestamp::new(50, unit.into()),
                    end: Timestamp::new(90, unit.into()),
                    num_rows: 3,
                    identifier: 1,
                },
                vec![
                    DfRecordBatch::try_new(
                        schema.clone(),
                        vec![new_nullable_ts_array(
                            unit,
                            vec![Some(89), Some(88), Some(87)],
                        )],
                    )
                    .unwrap(),
                ],
            ),
        ];

        // With nulls_last=false (equivalent to nulls_first=false), values sort before nulls
        // For descending, order is: 99, 98, 97, null, null
        // With limit=3, we get: 99, 98, 97
        let expected_output = Some(
            DfRecordBatch::try_new(
                schema.clone(),
                vec![new_nullable_ts_array(
                    unit,
                    vec![Some(99), Some(98), Some(97)],
                )],
            )
            .unwrap(),
        );

        run_test(
            3001,
            input_ranged_data,
            schema.clone(),
            SortOptions {
                descending: true,
                nulls_first: false,
            },
            Some(3),
            expected_output,
            Some(8), // Must read both batches to detect group boundary
            false,
        )
        .await;
    }
}
