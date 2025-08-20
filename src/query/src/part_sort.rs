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
use datafusion::common::arrow::compute::sort_to_indices;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion::execution::{RecordBatchStream, TaskContext};
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties, TopK,
};
use datafusion_common::{internal_err, DataFusionError};
use datafusion_physical_expr::PhysicalSortExpr;
use futures::{Stream, StreamExt};
use itertools::Itertools;
use snafu::location;
use store_api::region_engine::PartitionRange;

use crate::{array_iter_helper, downcast_ts_array};

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
}

impl PartSortExec {
    pub fn new(
        expression: PhysicalSortExpr,
        limit: Option<usize>,
        partition_ranges: Vec<Vec<PartitionRange>>,
        input: Arc<dyn ExecutionPlan>,
    ) -> Self {
        let metrics = ExecutionPlanMetricsSet::new();
        let properties = input.properties();
        let properties = PlanProperties::new(
            input.equivalence_properties().clone(),
            input.output_partitioning().clone(),
            properties.emission_type,
            properties.boundedness,
        );

        Self {
            expression,
            limit,
            input,
            metrics,
            partition_ranges,
            properties,
        }
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
        Ok(Arc::new(Self::new(
            self.expression.clone(),
            self.limit,
            self.partition_ranges.clone(),
            new_input.clone(),
        )))
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
    produced: usize,
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
}

impl PartSortStream {
    fn new(
        context: Arc<TaskContext>,
        sort: &PartSortExec,
        limit: Option<usize>,
        input: DfSendableRecordBatchStream,
        partition_ranges: Vec<PartitionRange>,
        partition: usize,
    ) -> datafusion_common::Result<Self> {
        let buffer = if let Some(limit) = limit {
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
                    None,
                )?,
                0,
            )
        } else {
            PartSortBuffer::All(Vec::new())
        };

        Ok(Self {
            reservation: MemoryConsumer::new("PartSortStream".to_string())
                .register(&context.runtime_env().memory_pool),
            buffer,
            expression: sort.expression.clone(),
            limit,
            produced: 0,
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
        })
    }
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
    /// check whether the sort column's min/max value is within the partition range
    fn check_in_range(
        &self,
        sort_column: &ArrayRef,
        min_max_idx: (usize, usize),
    ) -> datafusion_common::Result<()> {
        if self.cur_part_idx >= self.partition_ranges.len() {
            internal_err!(
                "Partition index out of range: {} >= {} at {}",
                self.cur_part_idx,
                self.partition_ranges.len(),
                snafu::location!()
            )?;
        }
        let cur_range = self.partition_ranges[self.cur_part_idx];

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
            return Ok(Some(0));
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
            if let Some(val) = val {
                if val >= cur_range.end.value() || val < cur_range.start.value() {
                    return Ok(Some(idx));
                }
            }
        }

        Ok(None)
    }

    fn push_buffer(&mut self, batch: DfRecordBatch) -> datafusion_common::Result<()> {
        match &mut self.buffer {
            PartSortBuffer::All(v) => v.push(batch),
            PartSortBuffer::Top(top, cnt) => {
                *cnt += batch.num_rows();
                top.insert_batch(batch)?;
            }
        }

        Ok(())
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

        self.produced += sorted.num_rows();
        drop(full_input);
        // here remove both buffer and full_input memory
        self.reservation.shrink(2 * total_mem);
        Ok(sorted)
    }

    /// Internal method for sorting `Top` buffer (with limit).
    fn sort_top_buffer(&mut self) -> datafusion_common::Result<DfRecordBatch> {
        let new_top_buffer = TopK::try_new(
            self.partition,
            self.schema().clone(),
            vec![],
            [self.expression.clone()].into(),
            self.limit.unwrap(),
            self.context.session_config().batch_size(),
            self.context.runtime_env(),
            &self.root_metrics,
            None,
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

    /// Try to split the input batch if it contains data that exceeds the current partition range.
    ///
    /// When the input batch contains data that exceeds the current partition range, this function
    /// will split the input batch into two parts, the first part is within the current partition
    /// range will be merged and sorted with previous buffer, and the second part will be registered
    /// to `evaluating_batch` for next polling.
    ///
    /// Returns `None` if the input batch is empty or fully within the current partition range, and
    /// `Some(batch)` otherwise.
    fn split_batch(
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
        // mark end of current PartitionRange
        let sorted_batch = self.sort_buffer();
        // step to next proper PartitionRange
        self.cur_part_idx += 1;
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

        sorted_batch.map(|x| if x.num_rows() == 0 { None } else { Some(x) })
    }

    pub fn poll_next_inner(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<datafusion_common::Result<DfRecordBatch>>> {
        loop {
            // no more input, sort the buffer and return
            if self.input_complete {
                if self.buffer.is_empty() {
                    return Poll::Ready(None);
                } else {
                    return Poll::Ready(Some(self.sort_buffer()));
                }
            }

            // if there is a remaining batch being evaluated from last run,
            // split on it instead of fetching new batch
            if let Some(evaluating_batch) = self.evaluating_batch.take()
                && evaluating_batch.num_rows() != 0
            {
                if let Some(sorted_batch) = self.split_batch(evaluating_batch)? {
                    return Poll::Ready(Some(Ok(sorted_batch)));
                } else {
                    continue;
                }
            }

            // fetch next batch from input
            let res = self.input.as_mut().poll_next(cx);
            match res {
                Poll::Ready(Some(Ok(batch))) => {
                    if let Some(sorted_batch) = self.split_batch(batch)? {
                        return Poll::Ready(Some(Ok(sorted_batch)));
                    } else {
                        continue;
                    }
                }
                // input stream end, mark and continue
                Poll::Ready(None) => {
                    self.input_complete = true;
                    continue;
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

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use arrow::json::ArrayWriter;
    use arrow_schema::{DataType, Field, Schema, SortOptions, TimeUnit};
    use common_time::Timestamp;
    use datafusion_physical_expr::expressions::Column;
    use futures::StreamExt;
    use store_api::region_engine::PartitionRange;

    use super::*;
    use crate::test_util::{new_ts_array, MockInputExec};

    #[tokio::test]
    async fn fuzzy_test() {
        let test_cnt = 100;
        // bound for total count of PartitionRange
        let part_cnt_bound = 100;
        // bound for timestamp range size and offset for each PartitionRange
        let range_size_bound = 100;
        let range_offset_bound = 100;
        // bound for batch count and size within each PartitionRange
        let batch_cnt_bound = 20;
        let batch_size_bound = 100;

        let mut rng = fastrand::Rng::new();
        rng.seed(1337);

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
                Some(rng.usize(0..batch_cnt_bound * batch_size_bound))
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
                    let end = bound_val
                        .map(
                            |i| i
                            .checked_sub(rng.i64(0..range_offset_bound))
                            .expect("Bad luck, fuzzy test generate data that will overflow, change seed and try again")
                        )
                        .unwrap_or_else(|| rng.i64(-100000000..100000000));
                    bound_val = Some(end);
                    let start = end - rng.i64(1..range_size_bound);
                    let start = Timestamp::new(start, unit.into());
                    let end = Timestamp::new(end, unit.into());
                    (start, end)
                } else {
                    let start = bound_val
                        .map(|i| i + rng.i64(0..range_offset_bound))
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

            let expected_output = output_data
                .into_iter()
                .map(|a| {
                    DfRecordBatch::try_new(schema.clone(), vec![new_ts_array(unit, a)]).unwrap()
                })
                .map(|rb| {
                    // trim expected output with limit
                    if let Some(limit) = limit
                        && rb.num_rows() > limit
                    {
                        rb.slice(0, limit)
                    } else {
                        rb
                    }
                })
                .collect_vec();

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
            run_test(
                case_id,
                input_ranged_data,
                schema,
                opt,
                limit,
                expected_output,
            )
            .await;
        }
    }

    #[tokio::test]
    async fn simple_case() {
        let testcases = vec![
            (
                TimeUnit::Millisecond,
                vec![
                    ((0, 10), vec![vec![1, 2, 3], vec![4, 5, 6], vec![7, 8, 9]]),
                    ((5, 10), vec![vec![5, 6], vec![7, 8]]),
                ],
                false,
                None,
                vec![vec![1, 2, 3, 4, 5, 5, 6, 6, 7, 7, 8, 8, 9]],
            ),
            (
                TimeUnit::Millisecond,
                vec![
                    ((5, 10), vec![vec![5, 6], vec![7, 8, 9]]),
                    ((0, 10), vec![vec![1, 2, 3], vec![4, 5, 6], vec![7, 8]]),
                ],
                true,
                None,
                vec![vec![9, 8, 7, 6, 5], vec![8, 7, 6, 5, 4, 3, 2, 1]],
            ),
            (
                TimeUnit::Millisecond,
                vec![
                    ((5, 10), vec![]),
                    ((0, 10), vec![vec![1, 2, 3], vec![4, 5, 6], vec![7, 8]]),
                ],
                true,
                None,
                vec![vec![8, 7, 6, 5, 4, 3, 2, 1]],
            ),
            (
                TimeUnit::Millisecond,
                vec![
                    ((15, 20), vec![vec![17, 18, 19]]),
                    ((10, 15), vec![]),
                    ((5, 10), vec![]),
                    ((0, 10), vec![vec![1, 2, 3], vec![4, 5, 6], vec![7, 8]]),
                ],
                true,
                None,
                vec![vec![19, 18, 17], vec![8, 7, 6, 5, 4, 3, 2, 1]],
            ),
            (
                TimeUnit::Millisecond,
                vec![
                    ((15, 20), vec![]),
                    ((10, 15), vec![]),
                    ((5, 10), vec![]),
                    ((0, 10), vec![]),
                ],
                true,
                None,
                vec![],
            ),
            (
                TimeUnit::Millisecond,
                vec![
                    (
                        (15, 20),
                        vec![vec![15, 17, 19, 10, 11, 12, 5, 6, 7, 8, 9, 1, 2, 3, 4]],
                    ),
                    ((10, 15), vec![]),
                    ((5, 10), vec![]),
                    ((0, 10), vec![]),
                ],
                true,
                None,
                vec![
                    vec![19, 17, 15],
                    vec![12, 11, 10],
                    vec![9, 8, 7, 6, 5],
                    vec![4, 3, 2, 1],
                ],
            ),
            (
                TimeUnit::Millisecond,
                vec![
                    (
                        (15, 20),
                        vec![vec![15, 17, 19, 10, 11, 12, 5, 6, 7, 8, 9, 1, 2, 3, 4]],
                    ),
                    ((10, 15), vec![]),
                    ((5, 10), vec![]),
                    ((0, 10), vec![]),
                ],
                true,
                Some(2),
                vec![vec![19, 17], vec![12, 11], vec![9, 8], vec![4, 3]],
            ),
        ];

        for (identifier, (unit, input_ranged_data, descending, limit, expected_output)) in
            testcases.into_iter().enumerate()
        {
            let schema = Schema::new(vec![Field::new(
                "ts",
                DataType::Timestamp(unit, None),
                false,
            )]);
            let schema = Arc::new(schema);
            let opt = SortOptions {
                descending,
                ..Default::default()
            };

            let input_ranged_data = input_ranged_data
                .into_iter()
                .map(|(range, data)| {
                    let part = PartitionRange {
                        start: Timestamp::new(range.0, unit.into()),
                        end: Timestamp::new(range.1, unit.into()),
                        num_rows: data.iter().map(|b| b.len()).sum(),
                        identifier,
                    };

                    let batches = data
                        .into_iter()
                        .map(|b| {
                            let arr = new_ts_array(unit, b);
                            DfRecordBatch::try_new(schema.clone(), vec![arr]).unwrap()
                        })
                        .collect_vec();
                    (part, batches)
                })
                .collect_vec();

            let expected_output = expected_output
                .into_iter()
                .map(|a| {
                    DfRecordBatch::try_new(schema.clone(), vec![new_ts_array(unit, a)]).unwrap()
                })
                .collect_vec();

            run_test(
                identifier,
                input_ranged_data,
                schema.clone(),
                opt,
                limit,
                expected_output,
            )
            .await;
        }
    }

    #[allow(clippy::print_stdout)]
    async fn run_test(
        case_id: usize,
        input_ranged_data: Vec<(PartitionRange, Vec<DfRecordBatch>)>,
        schema: SchemaRef,
        opt: SortOptions,
        limit: Option<usize>,
        expected_output: Vec<DfRecordBatch>,
    ) {
        for rb in &expected_output {
            if let Some(limit) = limit {
                assert!(
                    rb.num_rows() <= limit,
                    "Expect row count in expected output's batch({}) <= limit({})",
                    rb.num_rows(),
                    limit
                );
            }
        }
        let (ranges, batches): (Vec<_>, Vec<_>) = input_ranged_data.clone().into_iter().unzip();

        let batches = batches
            .into_iter()
            .flat_map(|mut cols| {
                cols.push(DfRecordBatch::new_empty(schema.clone()));
                cols
            })
            .collect_vec();
        let mock_input = MockInputExec::new(batches, schema.clone());

        let exec = PartSortExec::new(
            PhysicalSortExpr {
                expr: Arc::new(Column::new("ts", 0)),
                options: opt,
            },
            limit,
            vec![ranges.clone()],
            Arc::new(mock_input),
        );

        let exec_stream = exec.execute(0, Arc::new(TaskContext::default())).unwrap();

        let real_output = exec_stream.map(|r| r.unwrap()).collect::<Vec<_>>().await;
        // a makeshift solution for compare large data
        if real_output != expected_output {
            let mut first_diff = 0;
            for (idx, (lhs, rhs)) in real_output.iter().zip(expected_output.iter()).enumerate() {
                if lhs != rhs {
                    first_diff = idx;
                    break;
                }
            }
            println!("first diff batch at {}", first_diff);
            println!(
                "ranges: {:?}",
                ranges
                    .into_iter()
                    .map(|r| (r.start.to_chrono_datetime(), r.end.to_chrono_datetime()))
                    .enumerate()
                    .collect::<Vec<_>>()
            );

            let mut full_msg = String::new();
            {
                let mut buf = Vec::with_capacity(10 * real_output.len());
                for batch in real_output.iter().skip(first_diff) {
                    let mut rb_json: Vec<u8> = Vec::new();
                    let mut writer = ArrayWriter::new(&mut rb_json);
                    writer.write(batch).unwrap();
                    writer.finish().unwrap();
                    buf.append(&mut rb_json);
                    buf.push(b',');
                }
                // TODO(discord9): better ways to print buf
                let buf = String::from_utf8_lossy(&buf);
                full_msg += &format!("\ncase_id:{case_id}, real_output \n{buf}\n");
            }
            {
                let mut buf = Vec::with_capacity(10 * real_output.len());
                for batch in expected_output.iter().skip(first_diff) {
                    let mut rb_json: Vec<u8> = Vec::new();
                    let mut writer = ArrayWriter::new(&mut rb_json);
                    writer.write(batch).unwrap();
                    writer.finish().unwrap();
                    buf.append(&mut rb_json);
                    buf.push(b',');
                }
                let buf = String::from_utf8_lossy(&buf);
                full_msg += &format!("case_id:{case_id}, expected_output \n{buf}");
            }
            panic!(
                "case_{} failed, opt: {:?},\n real output has {} batches, {} rows, expected has {} batches with {} rows\nfull msg: {}",
                case_id, opt,
                real_output.len(),
                real_output.iter().map(|x|x.num_rows()).sum::<usize>(),
                expected_output.len(),
                expected_output.iter().map(|x|x.num_rows()).sum::<usize>(), full_msg
            );
        }
    }
}
