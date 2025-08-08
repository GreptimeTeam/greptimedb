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

//! A physical plan for window sort(Which is sorting multiple sorted ranges according to input `PartitionRange`).
//!

use std::any::Any;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::pin::Pin;
use std::slice::from_ref;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::array::{Array, ArrayRef};
use arrow::compute::SortColumn;
use arrow_schema::{DataType, SchemaRef, SortOptions};
use common_error::ext::{BoxedError, PlainError};
use common_error::status_code::StatusCode;
use common_recordbatch::{DfRecordBatch, DfSendableRecordBatchStream};
use common_telemetry::error;
use common_time::Timestamp;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryPool};
use datafusion::execution::{RecordBatchStream, TaskContext};
use datafusion::physical_plan::memory::MemoryStream;
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::sorts::streaming_merge::StreamingMergeBuilder;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
};
use datafusion_common::utils::bisect;
use datafusion_common::{internal_err, DataFusionError};
use datafusion_physical_expr::PhysicalSortExpr;
use datatypes::value::Value;
use futures::Stream;
use itertools::Itertools;
use snafu::ResultExt;
use store_api::region_engine::PartitionRange;

use crate::error::{QueryExecutionSnafu, Result};

/// A complex stream sort execution plan which accepts a list of `PartitionRange` and
/// merge sort them whenever possible, and emit the sorted result as soon as possible.
/// This sorting plan only accept sort by ts and will not sort by other fields.
///
/// internally, it call [`StreamingMergeBuilder`] multiple times to merge multiple sorted "working ranges"
///
/// # Invariant Promise on Input Stream
/// 1. The input stream must be sorted by timestamp and
/// 2. in the order of `PartitionRange` in `ranges`
/// 3. Each `PartitionRange` is sorted within itself(ascending or descending) but no need to be sorted across ranges
/// 4. There can't be any RecordBatch that is cross multiple `PartitionRange` in the input stream
///
///  TODO(discord9): fix item 4, but since only use `PartSort` as input, this might not be a problem

#[derive(Debug, Clone)]
pub struct WindowedSortExec {
    /// Physical sort expressions(that is, sort by timestamp)
    expression: PhysicalSortExpr,
    /// Optional number of rows to fetch. Stops producing rows after this fetch
    fetch: Option<usize>,
    /// The input ranges indicate input stream will be composed of those ranges in given order.
    ///
    /// Each partition has one vector of `PartitionRange`.
    ranges: Vec<Vec<PartitionRange>>,
    /// All available working ranges and their corresponding working set
    ///
    /// working ranges promise once input stream get a value out of current range, future values will never
    /// be in this range. Each partition has one vector of ranges.
    all_avail_working_range: Vec<Vec<(TimeRange, BTreeSet<usize>)>>,
    input: Arc<dyn ExecutionPlan>,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
    properties: PlanProperties,
}

fn check_partition_range_monotonicity(
    ranges: &[Vec<PartitionRange>],
    descending: bool,
) -> Result<()> {
    let is_valid = ranges.iter().all(|r| {
        if descending {
            r.windows(2).all(|w| w[0].end >= w[1].end)
        } else {
            r.windows(2).all(|w| w[0].start <= w[1].start)
        }
    });

    if !is_valid {
        let msg = if descending {
            "Input `PartitionRange`s's upper bound is not monotonic non-increase"
        } else {
            "Input `PartitionRange`s's lower bound is not monotonic non-decrease"
        };
        let plain_error = PlainError::new(msg.to_string(), StatusCode::Unexpected);
        Err(BoxedError::new(plain_error)).context(QueryExecutionSnafu {})
    } else {
        Ok(())
    }
}

impl WindowedSortExec {
    pub fn try_new(
        expression: PhysicalSortExpr,
        fetch: Option<usize>,
        ranges: Vec<Vec<PartitionRange>>,
        input: Arc<dyn ExecutionPlan>,
    ) -> Result<Self> {
        check_partition_range_monotonicity(&ranges, expression.options.descending)?;

        let mut eq_properties = input.equivalence_properties().clone();
        eq_properties.reorder(vec![expression.clone()])?;

        let properties = input.properties();
        let properties = PlanProperties::new(
            eq_properties,
            input.output_partitioning().clone(),
            properties.emission_type,
            properties.boundedness,
        );

        let mut all_avail_working_range = Vec::with_capacity(ranges.len());
        for r in &ranges {
            let overlap_counts = split_overlapping_ranges(r);
            let working_ranges =
                compute_all_working_ranges(&overlap_counts, expression.options.descending);
            all_avail_working_range.push(working_ranges);
        }

        Ok(Self {
            expression,
            fetch,
            ranges,
            all_avail_working_range,
            input,
            metrics: ExecutionPlanMetricsSet::new(),
            properties,
        })
    }

    /// During receiving partial-sorted RecordBatch, we need to update the working set which is the
    /// `PartitionRange` we think those RecordBatch belongs to. And when we receive something outside
    /// of working set, we can merge results before whenever possible.
    pub fn to_stream(
        &self,
        context: Arc<TaskContext>,
        partition: usize,
    ) -> datafusion_common::Result<DfSendableRecordBatchStream> {
        let input_stream: DfSendableRecordBatchStream =
            self.input.execute(partition, context.clone())?;

        let df_stream = Box::pin(WindowedSortStream::new(
            context,
            self,
            input_stream,
            partition,
        )) as _;

        Ok(df_stream)
    }
}

impl DisplayAs for WindowedSortExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "WindowedSortExec: expr={} num_ranges={}",
            self.expression,
            self.ranges.len()
        )?;
        if let Some(fetch) = self.fetch {
            write!(f, " fetch={}", fetch)?;
        }
        Ok(())
    }
}

impl ExecutionPlan for WindowedSortExec {
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
            self.fetch,
            self.ranges.clone(),
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
        vec![false; self.ranges.len()]
    }

    fn name(&self) -> &str {
        "WindowedSortExec"
    }
}

/// The core logic of merging sort multiple sorted ranges
///
/// the flow of data is:
/// ```md
/// input --check if sorted--> in_progress --find sorted run--> sorted_input_runs --call merge sort--> merge_stream --> output
/// ```
pub struct WindowedSortStream {
    /// Memory pool for this stream
    memory_pool: Arc<dyn MemoryPool>,
    /// currently assembling RecordBatches, will be put to `sort_partition_rbs` when it's done
    in_progress: Vec<DfRecordBatch>,
    /// last `Timestamp` of the last input RecordBatch in `in_progress`, use to found partial sorted run's boundary
    last_value: Option<Timestamp>,
    /// Current working set of `PartitionRange` sorted RecordBatches
    sorted_input_runs: Vec<DfSendableRecordBatchStream>,
    /// Merge-sorted result streams, should be polled to end before start a new merge sort again
    merge_stream: VecDeque<DfSendableRecordBatchStream>,
    /// The number of times merge sort has been called
    merge_count: usize,
    /// Index into current `working_range` in `all_avail_working_range`
    working_idx: usize,
    /// input stream assumed reading in order of `PartitionRange`
    input: DfSendableRecordBatchStream,
    /// Whether this stream is terminated. For reasons like limit reached or input stream is done.
    is_terminated: bool,
    /// Output Schema, which is the same as input schema, since this is a sort plan
    schema: SchemaRef,
    /// Physical sort expressions(that is, sort by timestamp)
    expression: PhysicalSortExpr,
    /// Optional number of rows to fetch. Stops producing rows after this fetch
    fetch: Option<usize>,
    /// number of rows produced
    produced: usize,
    /// Resulting Stream(`merge_stream`)'s batch size, merely a suggestion
    batch_size: usize,
    /// All available working ranges and their corresponding working set
    ///
    /// working ranges promise once input stream get a value out of current range, future values will never be in this range
    all_avail_working_range: Vec<(TimeRange, BTreeSet<usize>)>,
    /// The input partition ranges
    #[allow(dead_code)] // this is used under #[debug_assertions]
    ranges: Vec<PartitionRange>,
    /// Execution metrics
    metrics: BaselineMetrics,
}

impl WindowedSortStream {
    pub fn new(
        context: Arc<TaskContext>,
        exec: &WindowedSortExec,
        input: DfSendableRecordBatchStream,
        partition: usize,
    ) -> Self {
        Self {
            memory_pool: context.runtime_env().memory_pool.clone(),
            in_progress: Vec::new(),
            last_value: None,
            sorted_input_runs: Vec::new(),
            merge_stream: VecDeque::new(),
            merge_count: 0,
            working_idx: 0,
            schema: input.schema(),
            input,
            is_terminated: false,
            expression: exec.expression.clone(),
            fetch: exec.fetch,
            produced: 0,
            batch_size: context.session_config().batch_size(),
            all_avail_working_range: exec.all_avail_working_range[partition].clone(),
            ranges: exec.ranges[partition].clone(),
            metrics: BaselineMetrics::new(&exec.metrics, partition),
        }
    }
}

impl WindowedSortStream {
    #[cfg(debug_assertions)]
    fn check_subset_ranges(&self, cur_range: &TimeRange) {
        let cur_is_subset_to = self
            .ranges
            .iter()
            .filter(|r| cur_range.is_subset(&TimeRange::from(*r)))
            .collect_vec();
        if cur_is_subset_to.is_empty() {
            error!("Current range is not a subset of any PartitionRange");
            // found in all ranges that are subset of current range
            let subset_ranges = self
                .ranges
                .iter()
                .filter(|r| TimeRange::from(*r).is_subset(cur_range))
                .collect_vec();
            let only_overlap = self
                .ranges
                .iter()
                .filter(|r| {
                    let r = TimeRange::from(*r);
                    r.is_overlapping(cur_range) && !r.is_subset(cur_range)
                })
                .collect_vec();
            error!(
                "Bad input, found {} ranges that are subset of current range, also found {} ranges that only overlap, subset ranges are: {:?}; overlap ranges are: {:?}",
                subset_ranges.len(),
                only_overlap.len(),
                subset_ranges,
                only_overlap
            );
        } else {
            let only_overlap = self
                .ranges
                .iter()
                .filter(|r| {
                    let r = TimeRange::from(*r);
                    r.is_overlapping(cur_range) && !cur_range.is_subset(&r)
                })
                .collect_vec();
            error!(
                "Found current range to be subset of {} ranges, also found {} ranges that only overlap, of subset ranges are:{:?}; overlap ranges are: {:?}",
                cur_is_subset_to.len(),
                only_overlap.len(),
                cur_is_subset_to,
                only_overlap
            );
        }
        let all_overlap_working_range = self
            .all_avail_working_range
            .iter()
            .filter(|(range, _)| range.is_overlapping(cur_range))
            .map(|(range, _)| range)
            .collect_vec();
        error!(
            "Found {} working ranges that overlap with current range: {:?}",
            all_overlap_working_range.len(),
            all_overlap_working_range
        );
    }

    /// Poll the next RecordBatch from the merge-sort's output stream
    fn poll_result_stream(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<datafusion_common::Result<DfRecordBatch>>> {
        while let Some(merge_stream) = &mut self.merge_stream.front_mut() {
            match merge_stream.as_mut().poll_next(cx) {
                Poll::Ready(Some(Ok(batch))) => {
                    let ret = if let Some(remaining) = self.remaining_fetch() {
                        if remaining == 0 {
                            self.is_terminated = true;
                            None
                        } else if remaining < batch.num_rows() {
                            self.produced += remaining;
                            Some(Ok(batch.slice(0, remaining)))
                        } else {
                            self.produced += batch.num_rows();
                            Some(Ok(batch))
                        }
                    } else {
                        self.produced += batch.num_rows();
                        Some(Ok(batch))
                    };
                    return Poll::Ready(ret);
                }
                Poll::Ready(Some(Err(e))) => {
                    return Poll::Ready(Some(Err(e)));
                }
                Poll::Ready(None) => {
                    // current merge stream is done, we can start polling the next one

                    self.merge_stream.pop_front();
                    continue;
                }
                Poll::Pending => {
                    return Poll::Pending;
                }
            }
        }
        // if no output stream is available
        Poll::Ready(None)
    }

    /// The core logic of merging sort multiple sorted ranges
    ///
    /// We try to maximize the number of sorted runs we can merge in one go, while emit the result as soon as possible.
    pub fn poll_next_inner(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<datafusion_common::Result<DfRecordBatch>>> {
        // first check and send out the merge result
        match self.poll_result_stream(cx) {
            Poll::Ready(None) => {
                if self.is_terminated {
                    return Poll::Ready(None);
                }
            }
            x => return x,
        };

        // consume input stream
        while !self.is_terminated {
            // then we get a new RecordBatch from input stream
            let SortedRunSet {
                runs_with_batch,
                sort_column,
            } = match self.input.as_mut().poll_next(cx) {
                Poll::Ready(Some(Ok(batch))) => split_batch_to_sorted_run(batch, &self.expression)?,
                Poll::Ready(Some(Err(e))) => {
                    return Poll::Ready(Some(Err(e)));
                }
                Poll::Ready(None) => {
                    // input stream is done, we need to merge sort the remaining working set
                    self.is_terminated = true;
                    self.build_sorted_stream()?;
                    self.start_new_merge_sort()?;
                    break;
                }
                Poll::Pending => return Poll::Pending,
            };

            // The core logic to eargerly merge sort the working set

            // compare with last_value to find boundary, then merge runs if needed

            // iterate over runs_with_batch to merge sort, might create zero or more stream to put to `sort_partition_rbs`
            let mut last_remaining = None;
            let mut run_iter = runs_with_batch.into_iter();
            loop {
                let Some((sorted_rb, run_info)) = last_remaining.take().or(run_iter.next()) else {
                    break;
                };
                if sorted_rb.num_rows() == 0 {
                    continue;
                }
                // determine if this batch is in current working range
                let Some(cur_range) = run_info.get_time_range() else {
                    internal_err!("Found NULL in time index column")?
                };
                let Some(working_range) = self.get_working_range() else {
                    internal_err!("No working range found")?
                };

                // ensure the current batch is in the working range
                if sort_column.options.unwrap_or_default().descending {
                    if cur_range.end > working_range.end {
                        error!("Invalid range: {:?} > {:?}", cur_range, working_range);
                        #[cfg(debug_assertions)]
                        self.check_subset_ranges(&cur_range);
                        internal_err!("Current batch have data on the right side of working range, something is very wrong")?;
                    }
                } else if cur_range.start < working_range.start {
                    error!("Invalid range: {:?} < {:?}", cur_range, working_range);
                    #[cfg(debug_assertions)]
                    self.check_subset_ranges(&cur_range);
                    internal_err!("Current batch have data on the left side of working range, something is very wrong")?;
                }

                if cur_range.is_subset(&working_range) {
                    // data still in range, can't merge sort yet
                    // see if can concat entire sorted rb, merge sort need to wait
                    self.try_concat_batch(sorted_rb.clone(), &run_info, sort_column.options)?;
                } else if let Some(intersection) = cur_range.intersection(&working_range) {
                    // slice rb by intersection and concat it then merge sort
                    let cur_sort_column = sort_column.values.slice(run_info.offset, run_info.len);
                    let (offset, len) = find_slice_from_range(
                        &SortColumn {
                            values: cur_sort_column.clone(),
                            options: sort_column.options,
                        },
                        &intersection,
                    )?;

                    if offset != 0 {
                        internal_err!("Current batch have data on the left side of working range, something is very wrong")?;
                    }

                    let sliced_rb = sorted_rb.slice(offset, len);

                    // try to concat the sliced input batch to the current `in_progress` run
                    self.try_concat_batch(sliced_rb, &run_info, sort_column.options)?;
                    // since no more sorted data in this range will come in now, build stream now
                    self.build_sorted_stream()?;

                    // since we are crossing working range, we need to merge sort the working set
                    self.start_new_merge_sort()?;

                    let (r_offset, r_len) = (offset + len, sorted_rb.num_rows() - offset - len);
                    if r_len != 0 {
                        // we have remaining data in this batch, put it back to input queue
                        let remaining_rb = sorted_rb.slice(r_offset, r_len);
                        let new_first_val = get_timestamp_from_idx(&cur_sort_column, r_offset)?;
                        let new_run_info = SucRun {
                            offset: run_info.offset + r_offset,
                            len: r_len,
                            first_val: new_first_val,
                            last_val: run_info.last_val,
                        };
                        last_remaining = Some((remaining_rb, new_run_info));
                    }
                    // deal with remaining batch cross working range problem
                    // i.e: this example require more slice, and we are currently at point A
                    // |---1---|       |---3---|
                    // |-------A--2------------|
                    //  put the remaining batch back to iter and deal it in next loop
                } else {
                    // no overlap, we can merge sort the working set

                    self.build_sorted_stream()?;
                    self.start_new_merge_sort()?;

                    // always put it back to input queue until some batch is in working range
                    last_remaining = Some((sorted_rb, run_info));
                }
            }

            // poll result stream again to see if we can emit more results
            match self.poll_result_stream(cx) {
                Poll::Ready(None) => {
                    if self.is_terminated {
                        return Poll::Ready(None);
                    }
                }
                x => return x,
            };
        }
        // emit the merge result after terminated(all input stream is done)
        self.poll_result_stream(cx)
    }

    fn push_batch(&mut self, batch: DfRecordBatch) {
        self.in_progress.push(batch);
    }

    /// Try to concat the input batch to the current `in_progress` run
    ///
    /// if the input batch is not sorted, build old run to stream and start a new run with new batch
    fn try_concat_batch(
        &mut self,
        batch: DfRecordBatch,
        run_info: &SucRun<Timestamp>,
        opt: Option<SortOptions>,
    ) -> datafusion_common::Result<()> {
        let is_ok_to_concat =
            cmp_with_opts(&self.last_value, &run_info.first_val, &opt) <= std::cmp::Ordering::Equal;

        if is_ok_to_concat {
            self.push_batch(batch);
            // next time we get input batch might still be ordered, so not build stream yet
        } else {
            // no more sorted data, build stream now
            self.build_sorted_stream()?;
            self.push_batch(batch);
        }
        self.last_value = run_info.last_val;
        Ok(())
    }

    /// Get the current working range
    fn get_working_range(&self) -> Option<TimeRange> {
        self.all_avail_working_range
            .get(self.working_idx)
            .map(|(range, _)| *range)
    }

    /// Set current working range to the next working range
    fn set_next_working_range(&mut self) {
        self.working_idx += 1;
    }

    /// make `in_progress` as a new `DfSendableRecordBatchStream` and put into `sorted_input_runs`
    fn build_sorted_stream(&mut self) -> datafusion_common::Result<()> {
        if self.in_progress.is_empty() {
            return Ok(());
        }
        let data = std::mem::take(&mut self.in_progress);

        let new_stream = MemoryStream::try_new(data, self.schema(), None)?;
        self.sorted_input_runs.push(Box::pin(new_stream));
        Ok(())
    }

    /// Start merging sort the current working set
    fn start_new_merge_sort(&mut self) -> datafusion_common::Result<()> {
        if !self.in_progress.is_empty() {
            return internal_err!("Starting a merge sort when in_progress is not empty")?;
        }

        self.set_next_working_range();

        let streams = std::mem::take(&mut self.sorted_input_runs);
        if streams.is_empty() {
            return Ok(());
        } else if streams.len() == 1 {
            self.merge_stream
                .push_back(streams.into_iter().next().unwrap());
            return Ok(());
        }

        let fetch = self.remaining_fetch();
        let reservation = MemoryConsumer::new(format!("WindowedSortStream[{}]", self.merge_count))
            .register(&self.memory_pool);
        self.merge_count += 1;

        let resulting_stream = StreamingMergeBuilder::new()
            .with_streams(streams)
            .with_schema(self.schema())
            .with_expressions(&[self.expression.clone()].into())
            .with_metrics(self.metrics.clone())
            .with_batch_size(self.batch_size)
            .with_fetch(fetch)
            .with_reservation(reservation)
            .build()?;
        self.merge_stream.push_back(resulting_stream);
        // this working range is done, move to next working range
        Ok(())
    }

    /// Remaining number of rows to fetch, if no fetch limit, return None
    /// if fetch limit is reached, return Some(0)
    fn remaining_fetch(&self) -> Option<usize> {
        let total_now = self.produced;
        self.fetch.map(|p| p.saturating_sub(total_now))
    }
}

impl Stream for WindowedSortStream {
    type Item = datafusion_common::Result<DfRecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<datafusion_common::Result<DfRecordBatch>>> {
        let result = self.as_mut().poll_next_inner(cx);
        self.metrics.record_poll(result)
    }
}

impl RecordBatchStream for WindowedSortStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

/// split batch to sorted runs
fn split_batch_to_sorted_run(
    batch: DfRecordBatch,
    expression: &PhysicalSortExpr,
) -> datafusion_common::Result<SortedRunSet<Timestamp>> {
    // split input rb to sorted runs
    let sort_column = expression.evaluate_to_sort_column(&batch)?;
    let sorted_runs_offset = get_sorted_runs(sort_column.clone())?;
    if let Some(run) = sorted_runs_offset.first()
        && sorted_runs_offset.len() == 1
    {
        if !(run.offset == 0 && run.len == batch.num_rows()) {
            internal_err!(
                "Invalid run offset and length: offset = {:?}, len = {:?}, num_rows = {:?}",
                run.offset,
                run.len,
                batch.num_rows()
            )?;
        }
        // input rb is already sorted, we can emit it directly
        Ok(SortedRunSet {
            runs_with_batch: vec![(batch, run.clone())],
            sort_column,
        })
    } else {
        // those slice should be zero copy, so supposedly no new reservation needed
        let mut ret = Vec::with_capacity(sorted_runs_offset.len());
        for run in sorted_runs_offset {
            if run.offset + run.len > batch.num_rows() {
                internal_err!(
                    "Invalid run offset and length: offset = {:?}, len = {:?}, num_rows = {:?}",
                    run.offset,
                    run.len,
                    batch.num_rows()
                )?;
            }
            let new_rb = batch.slice(run.offset, run.len);
            ret.push((new_rb, run));
        }
        Ok(SortedRunSet {
            runs_with_batch: ret,
            sort_column,
        })
    }
}

/// Downcast a temporal array to a specific type
///
/// usage similar to `downcast_primitive!` in `arrow-array` crate
#[macro_export]
macro_rules! downcast_ts_array {
    ($data_type:expr => ($m:path $(, $args:tt)*), $($p:pat => $fallback:expr $(,)*)*) =>
    {
        match $data_type {
            arrow_schema::DataType::Timestamp(arrow_schema::TimeUnit::Second, _) => {
                $m!(arrow::datatypes::TimestampSecondType, arrow_schema::TimeUnit::Second $(, $args)*)
            }
            arrow_schema::DataType::Timestamp(arrow_schema::TimeUnit::Millisecond, _) => {
                $m!(arrow::datatypes::TimestampMillisecondType, arrow_schema::TimeUnit::Millisecond $(, $args)*)
            }
            arrow_schema::DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, _) => {
                $m!(arrow::datatypes::TimestampMicrosecondType, arrow_schema::TimeUnit::Microsecond $(, $args)*)
            }
            arrow_schema::DataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, _) => {
                $m!(arrow::datatypes::TimestampNanosecondType, arrow_schema::TimeUnit::Nanosecond $(, $args)*)
            }
            $($p => $fallback,)*
        }
    };
}

/// Find the slice(where start <= data < end and sort by `sort_column.options`) from the given range
///
/// Return the offset and length of the slice
fn find_slice_from_range(
    sort_column: &SortColumn,
    range: &TimeRange,
) -> datafusion_common::Result<(usize, usize)> {
    let ty = sort_column.values.data_type();
    let time_unit = if let DataType::Timestamp(unit, _) = ty {
        unit
    } else {
        return Err(DataFusionError::Internal(format!(
            "Unsupported sort column type: {}",
            sort_column.values.data_type()
        )));
    };
    let array = &sort_column.values;
    let opt = &sort_column.options.unwrap_or_default();
    let descending = opt.descending;

    let typed_sorted_range = [range.start, range.end]
        .iter()
        .map(|t| {
            t.convert_to(time_unit.into())
                .ok_or_else(|| {
                    DataFusionError::Internal(format!(
                        "Failed to convert timestamp from {:?} to {:?}",
                        t.unit(),
                        time_unit
                    ))
                })
                .and_then(|typed_ts| {
                    let value = Value::Timestamp(typed_ts);
                    value
                        .try_to_scalar_value(&value.data_type())
                        .map_err(|e| DataFusionError::External(Box::new(e) as _))
                })
        })
        .try_collect::<_, Vec<_>, _>()?;

    let (min_val, max_val) = (typed_sorted_range[0].clone(), typed_sorted_range[1].clone());

    // get slice that in which all data that `min_val<=data<max_val`
    let (start, end) = if descending {
        // note that `data < max_val`
        // i,e, for max_val = 4, array = [5,3,2] should be start=1
        // max_val = 4, array = [5, 4, 3, 2] should be start= 2
        let start = bisect::<false>(from_ref(array), from_ref(&max_val), &[*opt])?;
        // min_val = 1, array = [3, 2, 1, 0], end = 3
        // min_val = 1, array = [3, 2, 0], end = 2
        let end = bisect::<false>(from_ref(array), from_ref(&min_val), &[*opt])?;
        (start, end)
    } else {
        // min_val = 1, array = [1, 2, 3], start = 0
        // min_val = 1, array = [0, 2, 3], start = 1
        let start = bisect::<true>(from_ref(array), from_ref(&min_val), &[*opt])?;
        // max_val = 3, array = [1, 3, 4], end = 1
        // max_val = 3, array = [1, 2, 4], end = 2
        let end = bisect::<true>(from_ref(array), from_ref(&max_val), &[*opt])?;
        (start, end)
    };

    Ok((start, end - start))
}

/// Get an iterator from a primitive array.
///
/// Used with `downcast_ts_array`. The returned iter is wrapped with `.enumerate()`.
#[macro_export]
macro_rules! array_iter_helper {
    ($t:ty, $unit:expr, $arr:expr) => {{
        let typed = $arr
            .as_any()
            .downcast_ref::<arrow::array::PrimitiveArray<$t>>()
            .unwrap();
        let iter = typed.iter().enumerate();
        Box::new(iter) as Box<dyn Iterator<Item = (usize, Option<i64>)>>
    }};
}

/// Compare with options, note None is considered as NULL here
///
/// default to null first
fn cmp_with_opts<T: Ord>(
    a: &Option<T>,
    b: &Option<T>,
    opt: &Option<SortOptions>,
) -> std::cmp::Ordering {
    let opt = opt.unwrap_or_default();

    if let (Some(a), Some(b)) = (a, b) {
        if opt.descending {
            b.cmp(a)
        } else {
            a.cmp(b)
        }
    } else if opt.nulls_first {
        // now we know at leatst one of them is None
        // in rust None < Some(_)
        a.cmp(b)
    } else {
        match (a, b) {
            (Some(a), Some(b)) => a.cmp(b),
            (Some(_), None) => std::cmp::Ordering::Less,
            (None, Some(_)) => std::cmp::Ordering::Greater,
            (None, None) => std::cmp::Ordering::Equal,
        }
    }
}

#[derive(Debug, Clone)]
struct SortedRunSet<N: Ord> {
    /// sorted runs with batch corresponding to them
    runs_with_batch: Vec<(DfRecordBatch, SucRun<N>)>,
    /// sorted column from eval sorting expr
    sort_column: SortColumn,
}

/// A struct to represent a successive run in the input iterator
#[derive(Debug, Clone, PartialEq)]
struct SucRun<N: Ord> {
    /// offset of the first element in the run
    offset: usize,
    /// length of the run
    len: usize,
    /// first non-null value in the run
    first_val: Option<N>,
    /// last non-null value in the run
    last_val: Option<N>,
}

impl SucRun<Timestamp> {
    /// Get the time range of the run, which is [min_val, max_val + 1)
    fn get_time_range(&self) -> Option<TimeRange> {
        let start = self.first_val.min(self.last_val);
        let end = self
            .first_val
            .max(self.last_val)
            .map(|i| Timestamp::new(i.value() + 1, i.unit()));
        start.zip(end).map(|(s, e)| TimeRange::new(s, e))
    }
}

/// find all successive runs in the input iterator
fn find_successive_runs<T: Iterator<Item = (usize, Option<N>)>, N: Ord + Copy>(
    iter: T,
    sort_opts: &Option<SortOptions>,
) -> Vec<SucRun<N>> {
    let mut runs = Vec::new();
    let mut last_value = None;
    let mut iter_len = None;

    let mut last_offset = 0;
    let mut first_val: Option<N> = None;
    let mut last_val: Option<N> = None;

    for (idx, t) in iter {
        if let Some(last_value) = &last_value {
            if cmp_with_opts(last_value, &t, sort_opts) == std::cmp::Ordering::Greater {
                // we found a boundary
                let len = idx - last_offset;
                let run = SucRun {
                    offset: last_offset,
                    len,
                    first_val,
                    last_val,
                };
                runs.push(run);
                first_val = None;
                last_val = None;

                last_offset = idx;
            }
        }
        last_value = Some(t);
        if let Some(t) = t {
            first_val = first_val.or(Some(t));
            last_val = Some(t).or(last_val);
        }
        iter_len = Some(idx);
    }
    let run = SucRun {
        offset: last_offset,
        len: iter_len.map(|l| l - last_offset + 1).unwrap_or(0),
        first_val,
        last_val,
    };
    runs.push(run);

    runs
}

/// return a list of non-overlapping (offset, length) which represent sorted runs, and
/// can be used to call [`DfRecordBatch::slice`] to get sorted runs
/// Returned runs will be as long as possible, and will not overlap with each other
fn get_sorted_runs(sort_column: SortColumn) -> datafusion_common::Result<Vec<SucRun<Timestamp>>> {
    let ty = sort_column.values.data_type();
    if let DataType::Timestamp(unit, _) = ty {
        let array = &sort_column.values;
        let iter = downcast_ts_array!(
            array.data_type() => (array_iter_helper, array),
            _ => internal_err!("Unsupported sort column type: {ty}")?
        );

        let raw = find_successive_runs(iter, &sort_column.options);
        let ts_runs = raw
            .into_iter()
            .map(|run| SucRun {
                offset: run.offset,
                len: run.len,
                first_val: run.first_val.map(|v| Timestamp::new(v, unit.into())),
                last_val: run.last_val.map(|v| Timestamp::new(v, unit.into())),
            })
            .collect_vec();
        Ok(ts_runs)
    } else {
        Err(DataFusionError::Internal(format!(
            "Unsupported sort column type: {ty}"
        )))
    }
}

/// Left(`start`) inclusive right(`end`) exclusive,
///
/// This is just tuple with extra methods
#[derive(Debug, Clone, Default, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct TimeRange {
    start: Timestamp,
    end: Timestamp,
}

impl From<&PartitionRange> for TimeRange {
    fn from(range: &PartitionRange) -> Self {
        Self::new(range.start, range.end)
    }
}

impl From<(Timestamp, Timestamp)> for TimeRange {
    fn from(range: (Timestamp, Timestamp)) -> Self {
        Self::new(range.0, range.1)
    }
}

impl From<&(Timestamp, Timestamp)> for TimeRange {
    fn from(range: &(Timestamp, Timestamp)) -> Self {
        Self::new(range.0, range.1)
    }
}

impl TimeRange {
    /// Create a new TimeRange, if start is greater than end, swap them
    fn new(start: Timestamp, end: Timestamp) -> Self {
        if start > end {
            Self {
                start: end,
                end: start,
            }
        } else {
            Self { start, end }
        }
    }

    fn is_subset(&self, other: &Self) -> bool {
        self.start >= other.start && self.end <= other.end
    }

    /// Check if two ranges are overlapping, exclusive(meaning if only boundary is overlapped then range is not overlapping)
    fn is_overlapping(&self, other: &Self) -> bool {
        !(self.start >= other.end || self.end <= other.start)
    }

    fn intersection(&self, other: &Self) -> Option<Self> {
        if self.is_overlapping(other) {
            Some(Self::new(
                self.start.max(other.start),
                self.end.min(other.end),
            ))
        } else {
            None
        }
    }

    fn difference(&self, other: &Self) -> Vec<Self> {
        if !self.is_overlapping(other) {
            vec![*self]
        } else {
            let mut ret = Vec::new();
            if self.start < other.start && self.end > other.end {
                ret.push(Self::new(self.start, other.start));
                ret.push(Self::new(other.end, self.end));
            } else if self.start < other.start {
                ret.push(Self::new(self.start, other.start));
            } else if self.end > other.end {
                ret.push(Self::new(other.end, self.end));
            }
            ret
        }
    }
}

/// split input range by `split_by` range to one, two or three parts.
fn split_range_by(
    input_range: &TimeRange,
    input_parts: &[usize],
    split_by: &TimeRange,
    split_idx: usize,
) -> Vec<Action> {
    let mut ret = Vec::new();
    if input_range.is_overlapping(split_by) {
        let input_parts = input_parts.to_vec();
        let new_parts = {
            let mut new_parts = input_parts.clone();
            new_parts.push(split_idx);
            new_parts
        };

        ret.push(Action::Pop(*input_range));
        if let Some(intersection) = input_range.intersection(split_by) {
            ret.push(Action::Push(intersection, new_parts.clone()));
        }
        for diff in input_range.difference(split_by) {
            ret.push(Action::Push(diff, input_parts.clone()));
        }
    }
    ret
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Action {
    Pop(TimeRange),
    Push(TimeRange, Vec<usize>),
}

/// Compute all working ranges and corresponding working sets from given `overlap_counts` computed from `split_overlapping_ranges`
///
/// working ranges promise once input stream get a value out of current range, future values will never be in this range
///
/// hence we can merge sort current working range once that happens
///
/// if `descending` is true, the working ranges will be in descending order
fn compute_all_working_ranges(
    overlap_counts: &BTreeMap<TimeRange, Vec<usize>>,
    descending: bool,
) -> Vec<(TimeRange, BTreeSet<usize>)> {
    let mut ret = Vec::new();
    let mut cur_range_set: Option<(TimeRange, BTreeSet<usize>)> = None;
    let overlap_iter: Box<dyn Iterator<Item = (&TimeRange, &Vec<usize>)>> = if descending {
        Box::new(overlap_counts.iter().rev()) as _
    } else {
        Box::new(overlap_counts.iter()) as _
    };
    for (range, set) in overlap_iter {
        match &mut cur_range_set {
            None => cur_range_set = Some((*range, BTreeSet::from_iter(set.iter().cloned()))),
            Some((working_range, working_set)) => {
                // if next overlap range have Partition that's is not last one in `working_set`(hence need
                // to be read before merge sorting), and `working_set` have >1 count
                // we have to expand current working range to cover it(and add it's `set` to `working_set`)
                // so that merge sort is possible
                let need_expand = {
                    let last_part = working_set.last();
                    let inter: BTreeSet<usize> = working_set
                        .intersection(&BTreeSet::from_iter(set.iter().cloned()))
                        .cloned()
                        .collect();
                    if let Some(one) = inter.first()
                        && inter.len() == 1
                        && Some(one) == last_part
                    {
                        // if only the last PartitionRange in current working set, we can just emit it so no need to expand working range
                        if set.iter().all(|p| Some(p) >= last_part) {
                            // if all PartitionRange in next overlap range is after the last one in current working set, we can just emit current working set
                            false
                        } else {
                            // elsewise, we need to expand working range to include next overlap range
                            true
                        }
                    } else if inter.is_empty() {
                        // if no common PartitionRange in current working set and next overlap range, we can just emit current working set
                        false
                    } else {
                        // have multiple intersection or intersection is not the last part, either way we need to expand working range to include next overlap range
                        true
                    }
                };

                if need_expand {
                    if descending {
                        working_range.start = range.start;
                    } else {
                        working_range.end = range.end;
                    }
                    working_set.extend(set.iter().cloned());
                } else {
                    ret.push((*working_range, std::mem::take(working_set)));
                    cur_range_set = Some((*range, BTreeSet::from_iter(set.iter().cloned())));
                }
            }
        }
    }

    if let Some(cur_range_set) = cur_range_set {
        ret.push(cur_range_set)
    }

    ret
}

/// return a map of non-overlapping ranges and their corresponding index
/// (not `PartitionRange.identifier` but position in array) in the input `PartitionRange`s that is in those ranges
fn split_overlapping_ranges(ranges: &[PartitionRange]) -> BTreeMap<TimeRange, Vec<usize>> {
    // invariant: the key ranges should not overlapping with each other by definition from `is_overlapping`
    let mut ret: BTreeMap<TimeRange, Vec<usize>> = BTreeMap::new();
    for (idx, range) in ranges.iter().enumerate() {
        let key: TimeRange = (range.start, range.end).into();
        let mut actions = Vec::new();
        let mut untouched = vec![key];
        // create a forward and backward iterator to find all overlapping ranges
        // given that tuple is sorted in lexicographical order and promise to not overlap,
        // since range is sorted that way, we can stop when we find a non-overlapping range
        let forward_iter = ret
            .range(key..)
            .take_while(|(range, _)| range.is_overlapping(&key));
        let backward_iter = ret
            .range(..key)
            .rev()
            .take_while(|(range, _)| range.is_overlapping(&key));

        for (range, parts) in forward_iter.chain(backward_iter) {
            untouched = untouched.iter().flat_map(|r| r.difference(range)).collect();
            let act = split_range_by(range, parts, &key, idx);
            actions.extend(act.into_iter());
        }

        for action in actions {
            match action {
                Action::Pop(range) => {
                    ret.remove(&range);
                }
                Action::Push(range, parts) => {
                    ret.insert(range, parts);
                }
            }
        }

        // insert untouched ranges
        for range in untouched {
            ret.insert(range, vec![idx]);
        }
    }
    ret
}

/// Get timestamp from array at offset
fn get_timestamp_from_idx(
    array: &ArrayRef,
    offset: usize,
) -> datafusion_common::Result<Option<Timestamp>> {
    let time_unit = if let DataType::Timestamp(unit, _) = array.data_type() {
        unit
    } else {
        return Err(DataFusionError::Internal(format!(
            "Unsupported sort column type: {}",
            array.data_type()
        )));
    };
    let ty = array.data_type();
    let array = array.slice(offset, 1);
    let mut iter = downcast_ts_array!(
        array.data_type() => (array_iter_helper, array),
        _ => internal_err!("Unsupported sort column type: {ty}")?
    );
    let (_idx, val) = iter.next().ok_or_else(|| {
        DataFusionError::Internal("Empty array in get_timestamp_from".to_string())
    })?;
    let val = if let Some(val) = val {
        val
    } else {
        return Ok(None);
    };
    let gt_timestamp = Timestamp::new(val, time_unit.into());
    Ok(Some(gt_timestamp))
}

#[cfg(test)]
mod test {
    use std::io::Write;
    use std::sync::Arc;

    use arrow::array::{ArrayRef, TimestampMillisecondArray};
    use arrow::compute::concat_batches;
    use arrow::json::ArrayWriter;
    use arrow_schema::{Field, Schema, TimeUnit};
    use futures::StreamExt;
    use pretty_assertions::assert_eq;
    use serde_json::json;

    use super::*;
    use crate::test_util::{new_ts_array, MockInputExec};

    #[test]
    fn test_overlapping() {
        let testcases = [
            (
                (Timestamp::new_second(1), Timestamp::new_millisecond(1001)),
                (Timestamp::new_second(0), Timestamp::new_millisecond(1)),
                false,
            ),
            (
                (Timestamp::new_second(1), Timestamp::new_millisecond(1001)),
                (Timestamp::new_second(0), Timestamp::new_millisecond(1001)),
                true,
            ),
            (
                (Timestamp::new_second(1), Timestamp::new_millisecond(1001)),
                (Timestamp::new_second(0), Timestamp::new_millisecond(1002)),
                true,
            ),
            (
                (Timestamp::new_second(1), Timestamp::new_millisecond(1001)),
                (
                    Timestamp::new_millisecond(1000),
                    Timestamp::new_millisecond(1002),
                ),
                true,
            ),
            (
                (Timestamp::new_second(1), Timestamp::new_millisecond(1001)),
                (
                    Timestamp::new_millisecond(1001),
                    Timestamp::new_millisecond(1002),
                ),
                false,
            ),
            (
                (Timestamp::new_second(1), Timestamp::new_millisecond(1001)),
                (
                    Timestamp::new_millisecond(1002),
                    Timestamp::new_millisecond(1003),
                ),
                false,
            ),
        ];

        for (range1, range2, expected) in testcases.iter() {
            assert_eq!(
                TimeRange::from(range1).is_overlapping(&range2.into()),
                *expected,
                "range1: {:?}, range2: {:?}",
                range1,
                range2
            );
        }
    }

    #[test]
    fn test_split() {
        let testcases = [
            // no split
            (
                (Timestamp::new_second(1), Timestamp::new_millisecond(1001)),
                vec![0],
                (Timestamp::new_second(0), Timestamp::new_millisecond(1)),
                1,
                vec![],
            ),
            // one part
            (
                (Timestamp::new_second(1), Timestamp::new_millisecond(1001)),
                vec![0],
                (Timestamp::new_second(0), Timestamp::new_millisecond(1001)),
                1,
                vec![
                    Action::Pop(
                        (Timestamp::new_second(1), Timestamp::new_millisecond(1001)).into(),
                    ),
                    Action::Push(
                        (Timestamp::new_second(1), Timestamp::new_millisecond(1001)).into(),
                        vec![0, 1],
                    ),
                ],
            ),
            (
                (Timestamp::new_second(1), Timestamp::new_millisecond(1001)),
                vec![0],
                (Timestamp::new_second(0), Timestamp::new_millisecond(1002)),
                1,
                vec![
                    Action::Pop(
                        (Timestamp::new_second(1), Timestamp::new_millisecond(1001)).into(),
                    ),
                    Action::Push(
                        (Timestamp::new_second(1), Timestamp::new_millisecond(1001)).into(),
                        vec![0, 1],
                    ),
                ],
            ),
            (
                (Timestamp::new_second(1), Timestamp::new_millisecond(1001)),
                vec![0],
                (
                    Timestamp::new_millisecond(1000),
                    Timestamp::new_millisecond(1002),
                ),
                1,
                vec![
                    Action::Pop(
                        (Timestamp::new_second(1), Timestamp::new_millisecond(1001)).into(),
                    ),
                    Action::Push(
                        (
                            Timestamp::new_millisecond(1000),
                            Timestamp::new_millisecond(1001),
                        )
                            .into(),
                        vec![0, 1],
                    ),
                ],
            ),
            // two part
            (
                (Timestamp::new_second(1), Timestamp::new_millisecond(1002)),
                vec![0],
                (
                    Timestamp::new_millisecond(1001),
                    Timestamp::new_millisecond(1002),
                ),
                1,
                vec![
                    Action::Pop(
                        (Timestamp::new_second(1), Timestamp::new_millisecond(1002)).into(),
                    ),
                    Action::Push(
                        (
                            Timestamp::new_millisecond(1001),
                            Timestamp::new_millisecond(1002),
                        )
                            .into(),
                        vec![0, 1],
                    ),
                    Action::Push(
                        (Timestamp::new_second(1), Timestamp::new_millisecond(1001)).into(),
                        vec![0],
                    ),
                ],
            ),
            // three part
            (
                (Timestamp::new_second(1), Timestamp::new_millisecond(1004)),
                vec![0],
                (
                    Timestamp::new_millisecond(1001),
                    Timestamp::new_millisecond(1002),
                ),
                1,
                vec![
                    Action::Pop(
                        (Timestamp::new_second(1), Timestamp::new_millisecond(1004)).into(),
                    ),
                    Action::Push(
                        (
                            Timestamp::new_millisecond(1001),
                            Timestamp::new_millisecond(1002),
                        )
                            .into(),
                        vec![0, 1],
                    ),
                    Action::Push(
                        (Timestamp::new_second(1), Timestamp::new_millisecond(1001)).into(),
                        vec![0],
                    ),
                    Action::Push(
                        (
                            Timestamp::new_millisecond(1002),
                            Timestamp::new_millisecond(1004),
                        )
                            .into(),
                        vec![0],
                    ),
                ],
            ),
        ];

        for (range, parts, split_by, split_idx, expected) in testcases.iter() {
            assert_eq!(
                split_range_by(&(*range).into(), parts, &split_by.into(), *split_idx),
                *expected,
                "range: {:?}, parts: {:?}, split_by: {:?}, split_idx: {}",
                range,
                parts,
                split_by,
                split_idx
            );
        }
    }

    #[test]
    fn test_compute_working_ranges_rev() {
        let testcases = vec![
            (
                BTreeMap::from([(
                    (Timestamp::new_second(1), Timestamp::new_second(2)),
                    vec![0],
                )]),
                vec![(
                    (Timestamp::new_second(1), Timestamp::new_second(2)),
                    BTreeSet::from([0]),
                )],
            ),
            (
                BTreeMap::from([(
                    (Timestamp::new_second(1), Timestamp::new_second(2)),
                    vec![0, 1],
                )]),
                vec![(
                    (Timestamp::new_second(1), Timestamp::new_second(2)),
                    BTreeSet::from([0, 1]),
                )],
            ),
            (
                BTreeMap::from([
                    (
                        (Timestamp::new_second(2), Timestamp::new_second(3)),
                        vec![0],
                    ),
                    (
                        (Timestamp::new_second(1), Timestamp::new_second(2)),
                        vec![0, 1],
                    ),
                ]),
                vec![
                    (
                        (Timestamp::new_second(2), Timestamp::new_second(3)),
                        BTreeSet::from([0]),
                    ),
                    (
                        (Timestamp::new_second(1), Timestamp::new_second(2)),
                        BTreeSet::from([0, 1]),
                    ),
                ],
            ),
            (
                BTreeMap::from([
                    (
                        (Timestamp::new_second(2), Timestamp::new_second(3)),
                        vec![0, 1],
                    ),
                    (
                        (Timestamp::new_second(1), Timestamp::new_second(2)),
                        vec![1],
                    ),
                ]),
                vec![
                    (
                        (Timestamp::new_second(2), Timestamp::new_second(3)),
                        BTreeSet::from([0, 1]),
                    ),
                    (
                        (Timestamp::new_second(1), Timestamp::new_second(2)),
                        BTreeSet::from([1]),
                    ),
                ],
            ),
            (
                BTreeMap::from([
                    (
                        (Timestamp::new_second(3), Timestamp::new_second(4)),
                        vec![0],
                    ),
                    (
                        (Timestamp::new_second(2), Timestamp::new_second(3)),
                        vec![0, 1],
                    ),
                    (
                        (Timestamp::new_second(1), Timestamp::new_second(2)),
                        vec![1],
                    ),
                ]),
                vec![
                    (
                        (Timestamp::new_second(3), Timestamp::new_second(4)),
                        BTreeSet::from([0]),
                    ),
                    (
                        (Timestamp::new_second(2), Timestamp::new_second(3)),
                        BTreeSet::from([0, 1]),
                    ),
                    (
                        (Timestamp::new_second(1), Timestamp::new_second(2)),
                        BTreeSet::from([1]),
                    ),
                ],
            ),
            (
                BTreeMap::from([
                    (
                        (Timestamp::new_second(3), Timestamp::new_second(4)),
                        vec![0, 2],
                    ),
                    (
                        (Timestamp::new_second(2), Timestamp::new_second(3)),
                        vec![0, 1, 2],
                    ),
                    (
                        (Timestamp::new_second(1), Timestamp::new_second(2)),
                        vec![1, 2],
                    ),
                ]),
                vec![(
                    (Timestamp::new_second(1), Timestamp::new_second(4)),
                    BTreeSet::from([0, 1, 2]),
                )],
            ),
            (
                BTreeMap::from([
                    (
                        (Timestamp::new_second(2), Timestamp::new_second(3)),
                        vec![0, 2],
                    ),
                    (
                        (Timestamp::new_second(1), Timestamp::new_second(2)),
                        vec![1, 2],
                    ),
                ]),
                vec![(
                    (Timestamp::new_second(1), Timestamp::new_second(3)),
                    BTreeSet::from([0, 1, 2]),
                )],
            ),
            (
                BTreeMap::from([
                    (
                        (Timestamp::new_second(3), Timestamp::new_second(4)),
                        vec![0, 1],
                    ),
                    (
                        (Timestamp::new_second(2), Timestamp::new_second(3)),
                        vec![0, 1, 2],
                    ),
                    (
                        (Timestamp::new_second(1), Timestamp::new_second(2)),
                        vec![1, 2],
                    ),
                ]),
                vec![(
                    (Timestamp::new_second(1), Timestamp::new_second(4)),
                    BTreeSet::from([0, 1, 2]),
                )],
            ),
            (
                BTreeMap::from([
                    (
                        (Timestamp::new_second(2), Timestamp::new_second(3)),
                        vec![0, 1],
                    ),
                    (
                        (Timestamp::new_second(1), Timestamp::new_second(2)),
                        vec![1, 2],
                    ),
                ]),
                vec![
                    (
                        (Timestamp::new_second(2), Timestamp::new_second(3)),
                        BTreeSet::from([0, 1]),
                    ),
                    (
                        (Timestamp::new_second(1), Timestamp::new_second(2)),
                        BTreeSet::from([1, 2]),
                    ),
                ],
            ),
            // non-overlapping
            (
                BTreeMap::from([
                    (
                        (Timestamp::new_second(2), Timestamp::new_second(3)),
                        vec![0],
                    ),
                    (
                        (Timestamp::new_second(1), Timestamp::new_second(2)),
                        vec![1, 2],
                    ),
                ]),
                vec![
                    (
                        (Timestamp::new_second(2), Timestamp::new_second(3)),
                        BTreeSet::from([0]),
                    ),
                    (
                        (Timestamp::new_second(1), Timestamp::new_second(2)),
                        BTreeSet::from([1, 2]),
                    ),
                ],
            ),
        ];

        for (input, expected) in testcases {
            let expected = expected
                .into_iter()
                .map(|(r, s)| (r.into(), s))
                .collect_vec();
            let input = input.into_iter().map(|(r, s)| (r.into(), s)).collect();
            assert_eq!(
                compute_all_working_ranges(&input, true),
                expected,
                "input: {:?}",
                input
            );
        }
    }

    #[test]
    fn test_compute_working_ranges() {
        let testcases = vec![
            (
                BTreeMap::from([(
                    (Timestamp::new_second(1), Timestamp::new_second(2)),
                    vec![0],
                )]),
                vec![(
                    (Timestamp::new_second(1), Timestamp::new_second(2)),
                    BTreeSet::from([0]),
                )],
            ),
            (
                BTreeMap::from([(
                    (Timestamp::new_second(1), Timestamp::new_second(2)),
                    vec![0, 1],
                )]),
                vec![(
                    (Timestamp::new_second(1), Timestamp::new_second(2)),
                    BTreeSet::from([0, 1]),
                )],
            ),
            (
                BTreeMap::from([
                    (
                        (Timestamp::new_second(1), Timestamp::new_second(2)),
                        vec![0, 1],
                    ),
                    (
                        (Timestamp::new_second(2), Timestamp::new_second(3)),
                        vec![1],
                    ),
                ]),
                vec![
                    (
                        (Timestamp::new_second(1), Timestamp::new_second(2)),
                        BTreeSet::from([0, 1]),
                    ),
                    (
                        (Timestamp::new_second(2), Timestamp::new_second(3)),
                        BTreeSet::from([1]),
                    ),
                ],
            ),
            (
                BTreeMap::from([
                    (
                        (Timestamp::new_second(1), Timestamp::new_second(2)),
                        vec![0],
                    ),
                    (
                        (Timestamp::new_second(2), Timestamp::new_second(3)),
                        vec![0, 1],
                    ),
                ]),
                vec![
                    (
                        (Timestamp::new_second(1), Timestamp::new_second(2)),
                        BTreeSet::from([0]),
                    ),
                    (
                        (Timestamp::new_second(2), Timestamp::new_second(3)),
                        BTreeSet::from([0, 1]),
                    ),
                ],
            ),
            // test if only one count in working set get it's own working range
            (
                BTreeMap::from([
                    (
                        (Timestamp::new_second(1), Timestamp::new_second(2)),
                        vec![0],
                    ),
                    (
                        (Timestamp::new_second(2), Timestamp::new_second(3)),
                        vec![0, 1],
                    ),
                    (
                        (Timestamp::new_second(3), Timestamp::new_second(4)),
                        vec![1],
                    ),
                ]),
                vec![
                    (
                        (Timestamp::new_second(1), Timestamp::new_second(2)),
                        BTreeSet::from([0]),
                    ),
                    (
                        (Timestamp::new_second(2), Timestamp::new_second(3)),
                        BTreeSet::from([0, 1]),
                    ),
                    (
                        (Timestamp::new_second(3), Timestamp::new_second(4)),
                        BTreeSet::from([1]),
                    ),
                ],
            ),
            (
                BTreeMap::from([
                    (
                        (Timestamp::new_second(1), Timestamp::new_second(2)),
                        vec![0, 2],
                    ),
                    (
                        (Timestamp::new_second(2), Timestamp::new_second(3)),
                        vec![0, 1, 2],
                    ),
                    (
                        (Timestamp::new_second(3), Timestamp::new_second(4)),
                        vec![1, 2],
                    ),
                ]),
                vec![(
                    (Timestamp::new_second(1), Timestamp::new_second(4)),
                    BTreeSet::from([0, 1, 2]),
                )],
            ),
            (
                BTreeMap::from([
                    (
                        (Timestamp::new_second(1), Timestamp::new_second(2)),
                        vec![0, 2],
                    ),
                    (
                        (Timestamp::new_second(2), Timestamp::new_second(3)),
                        vec![1, 2],
                    ),
                ]),
                vec![(
                    (Timestamp::new_second(1), Timestamp::new_second(3)),
                    BTreeSet::from([0, 1, 2]),
                )],
            ),
            (
                BTreeMap::from([
                    (
                        (Timestamp::new_second(1), Timestamp::new_second(2)),
                        vec![0, 1],
                    ),
                    (
                        (Timestamp::new_second(2), Timestamp::new_second(3)),
                        vec![0, 1, 2],
                    ),
                    (
                        (Timestamp::new_second(3), Timestamp::new_second(4)),
                        vec![1, 2],
                    ),
                ]),
                vec![(
                    (Timestamp::new_second(1), Timestamp::new_second(4)),
                    BTreeSet::from([0, 1, 2]),
                )],
            ),
            (
                BTreeMap::from([
                    (
                        (Timestamp::new_second(1), Timestamp::new_second(2)),
                        vec![0, 1],
                    ),
                    (
                        (Timestamp::new_second(2), Timestamp::new_second(3)),
                        vec![1, 2],
                    ),
                ]),
                vec![
                    (
                        (Timestamp::new_second(1), Timestamp::new_second(2)),
                        BTreeSet::from([0, 1]),
                    ),
                    (
                        (Timestamp::new_second(2), Timestamp::new_second(3)),
                        BTreeSet::from([1, 2]),
                    ),
                ],
            ),
            // non-overlapping
            (
                BTreeMap::from([
                    (
                        (Timestamp::new_second(1), Timestamp::new_second(2)),
                        vec![0, 1],
                    ),
                    (
                        (Timestamp::new_second(2), Timestamp::new_second(3)),
                        vec![2],
                    ),
                ]),
                vec![
                    (
                        (Timestamp::new_second(1), Timestamp::new_second(2)),
                        BTreeSet::from([0, 1]),
                    ),
                    (
                        (Timestamp::new_second(2), Timestamp::new_second(3)),
                        BTreeSet::from([2]),
                    ),
                ],
            ),
        ];

        for (input, expected) in testcases {
            let expected = expected
                .into_iter()
                .map(|(r, s)| (r.into(), s))
                .collect_vec();
            let input = input.into_iter().map(|(r, s)| (r.into(), s)).collect();
            assert_eq!(
                compute_all_working_ranges(&input, false),
                expected,
                "input: {:?}",
                input
            );
        }
    }

    #[test]
    fn test_split_overlap_range() {
        let testcases = vec![
            // simple one range
            (
                vec![PartitionRange {
                    start: Timestamp::new_second(1),
                    end: Timestamp::new_second(2),
                    num_rows: 2,
                    identifier: 0,
                }],
                BTreeMap::from_iter(
                    vec![(
                        (Timestamp::new_second(1), Timestamp::new_second(2)),
                        vec![0],
                    )]
                    .into_iter(),
                ),
            ),
            // two overlapping range
            (
                vec![
                    PartitionRange {
                        start: Timestamp::new_second(1),
                        end: Timestamp::new_second(2),
                        num_rows: 2,
                        identifier: 0,
                    },
                    PartitionRange {
                        start: Timestamp::new_second(1),
                        end: Timestamp::new_second(2),
                        num_rows: 2,
                        identifier: 1,
                    },
                ],
                BTreeMap::from_iter(
                    vec![(
                        (Timestamp::new_second(1), Timestamp::new_second(2)),
                        vec![0, 1],
                    )]
                    .into_iter(),
                ),
            ),
            (
                vec![
                    PartitionRange {
                        start: Timestamp::new_second(1),
                        end: Timestamp::new_second(3),
                        num_rows: 2,
                        identifier: 0,
                    },
                    PartitionRange {
                        start: Timestamp::new_second(2),
                        end: Timestamp::new_second(4),
                        num_rows: 2,
                        identifier: 1,
                    },
                ],
                BTreeMap::from_iter(
                    vec![
                        (
                            (Timestamp::new_second(1), Timestamp::new_second(2)),
                            vec![0],
                        ),
                        (
                            (Timestamp::new_second(2), Timestamp::new_second(3)),
                            vec![0, 1],
                        ),
                        (
                            (Timestamp::new_second(3), Timestamp::new_second(4)),
                            vec![1],
                        ),
                    ]
                    .into_iter(),
                ),
            ),
            // three or more overlapping range
            (
                vec![
                    PartitionRange {
                        start: Timestamp::new_second(1),
                        end: Timestamp::new_second(3),
                        num_rows: 2,
                        identifier: 0,
                    },
                    PartitionRange {
                        start: Timestamp::new_second(2),
                        end: Timestamp::new_second(4),
                        num_rows: 2,
                        identifier: 1,
                    },
                    PartitionRange {
                        start: Timestamp::new_second(1),
                        end: Timestamp::new_second(4),
                        num_rows: 2,
                        identifier: 2,
                    },
                ],
                BTreeMap::from_iter(
                    vec![
                        (
                            (Timestamp::new_second(1), Timestamp::new_second(2)),
                            vec![0, 2],
                        ),
                        (
                            (Timestamp::new_second(2), Timestamp::new_second(3)),
                            vec![0, 1, 2],
                        ),
                        (
                            (Timestamp::new_second(3), Timestamp::new_second(4)),
                            vec![1, 2],
                        ),
                    ]
                    .into_iter(),
                ),
            ),
            (
                vec![
                    PartitionRange {
                        start: Timestamp::new_second(1),
                        end: Timestamp::new_second(3),
                        num_rows: 2,
                        identifier: 0,
                    },
                    PartitionRange {
                        start: Timestamp::new_second(1),
                        end: Timestamp::new_second(4),
                        num_rows: 2,
                        identifier: 1,
                    },
                    PartitionRange {
                        start: Timestamp::new_second(2),
                        end: Timestamp::new_second(4),
                        num_rows: 2,
                        identifier: 2,
                    },
                ],
                BTreeMap::from_iter(
                    vec![
                        (
                            (Timestamp::new_second(1), Timestamp::new_second(2)),
                            vec![0, 1],
                        ),
                        (
                            (Timestamp::new_second(2), Timestamp::new_second(3)),
                            vec![0, 1, 2],
                        ),
                        (
                            (Timestamp::new_second(3), Timestamp::new_second(4)),
                            vec![1, 2],
                        ),
                    ]
                    .into_iter(),
                ),
            ),
        ];

        for (input, expected) in testcases {
            let expected = expected.into_iter().map(|(r, s)| (r.into(), s)).collect();
            assert_eq!(split_overlapping_ranges(&input), expected);
        }
    }

    impl From<(i32, i32, Option<i32>, Option<i32>)> for SucRun<i32> {
        fn from((offset, len, min_val, max_val): (i32, i32, Option<i32>, Option<i32>)) -> Self {
            Self {
                offset: offset as usize,
                len: len as usize,
                first_val: min_val,
                last_val: max_val,
            }
        }
    }

    #[test]
    fn test_find_successive_runs() {
        let testcases = vec![
            (
                vec![Some(1), Some(1), Some(2), Some(1), Some(3)],
                Some(SortOptions {
                    descending: false,
                    nulls_first: false,
                }),
                vec![(0, 3, Some(1), Some(2)), (3, 2, Some(1), Some(3))],
            ),
            (
                vec![Some(1), Some(2), Some(2), Some(1), Some(3)],
                Some(SortOptions {
                    descending: false,
                    nulls_first: false,
                }),
                vec![(0, 3, Some(1), Some(2)), (3, 2, Some(1), Some(3))],
            ),
            (
                vec![Some(1), Some(2), None, None, Some(1), Some(3)],
                Some(SortOptions {
                    descending: false,
                    nulls_first: false,
                }),
                vec![(0, 4, Some(1), Some(2)), (4, 2, Some(1), Some(3))],
            ),
            (
                vec![Some(1), Some(2), Some(1), Some(3)],
                Some(SortOptions {
                    descending: false,
                    nulls_first: false,
                }),
                vec![(0, 2, Some(1), Some(2)), (2, 2, Some(1), Some(3))],
            ),
            (
                vec![Some(1), Some(2), Some(1), Some(3)],
                Some(SortOptions {
                    descending: true,
                    nulls_first: false,
                }),
                vec![
                    (0, 1, Some(1), Some(1)),
                    (1, 2, Some(2), Some(1)),
                    (3, 1, Some(3), Some(3)),
                ],
            ),
            (
                vec![Some(1), Some(2), None, Some(3)],
                Some(SortOptions {
                    descending: false,
                    nulls_first: true,
                }),
                vec![(0, 2, Some(1), Some(2)), (2, 2, Some(3), Some(3))],
            ),
            (
                vec![Some(1), Some(2), None, Some(3)],
                Some(SortOptions {
                    descending: false,
                    nulls_first: false,
                }),
                vec![(0, 3, Some(1), Some(2)), (3, 1, Some(3), Some(3))],
            ),
            (
                vec![Some(2), Some(1), None, Some(3)],
                Some(SortOptions {
                    descending: true,
                    nulls_first: true,
                }),
                vec![(0, 2, Some(2), Some(1)), (2, 2, Some(3), Some(3))],
            ),
            (
                vec![],
                Some(SortOptions {
                    descending: false,
                    nulls_first: true,
                }),
                vec![(0, 0, None, None)],
            ),
            (
                vec![None, None, Some(2), Some(2), Some(1), Some(5), Some(4)],
                Some(SortOptions {
                    descending: true,
                    nulls_first: true,
                }),
                vec![(0, 5, Some(2), Some(1)), (5, 2, Some(5), Some(4))],
            ),
            (
                vec![None, None, Some(2), Some(2), Some(1), Some(5), Some(4)],
                Some(SortOptions {
                    descending: true,
                    nulls_first: false,
                }),
                vec![
                    (0, 2, None, None),
                    (2, 3, Some(2), Some(1)),
                    (5, 2, Some(5), Some(4)),
                ],
            ),
        ];
        for (input, sort_opts, expected) in testcases {
            let ret = find_successive_runs(input.clone().into_iter().enumerate(), &sort_opts);
            let expected = expected.into_iter().map(SucRun::<i32>::from).collect_vec();
            assert_eq!(
                ret, expected,
                "input: {:?}, opt: {:?},expected: {:?}",
                input, sort_opts, expected
            );
        }
    }

    #[test]
    fn test_cmp_with_opts() {
        let testcases = vec![
            (
                Some(1),
                Some(2),
                Some(SortOptions {
                    descending: false,
                    nulls_first: false,
                }),
                std::cmp::Ordering::Less,
            ),
            (
                Some(1),
                Some(2),
                Some(SortOptions {
                    descending: true,
                    nulls_first: false,
                }),
                std::cmp::Ordering::Greater,
            ),
            (
                Some(1),
                None,
                Some(SortOptions {
                    descending: false,
                    nulls_first: true,
                }),
                std::cmp::Ordering::Greater,
            ),
            (
                Some(1),
                None,
                Some(SortOptions {
                    descending: true,
                    nulls_first: true,
                }),
                std::cmp::Ordering::Greater,
            ),
            (
                Some(1),
                None,
                Some(SortOptions {
                    descending: true,
                    nulls_first: false,
                }),
                std::cmp::Ordering::Less,
            ),
            (
                Some(1),
                None,
                Some(SortOptions {
                    descending: false,
                    nulls_first: false,
                }),
                std::cmp::Ordering::Less,
            ),
            (
                None,
                None,
                Some(SortOptions {
                    descending: true,
                    nulls_first: true,
                }),
                std::cmp::Ordering::Equal,
            ),
            (
                None,
                None,
                Some(SortOptions {
                    descending: false,
                    nulls_first: true,
                }),
                std::cmp::Ordering::Equal,
            ),
            (
                None,
                None,
                Some(SortOptions {
                    descending: true,
                    nulls_first: false,
                }),
                std::cmp::Ordering::Equal,
            ),
            (
                None,
                None,
                Some(SortOptions {
                    descending: false,
                    nulls_first: false,
                }),
                std::cmp::Ordering::Equal,
            ),
        ];
        for (a, b, opts, expected) in testcases {
            assert_eq!(
                cmp_with_opts(&a, &b, &opts),
                expected,
                "a: {:?}, b: {:?}, opts: {:?}",
                a,
                b,
                opts
            );
        }
    }

    #[test]
    fn test_find_slice_from_range() {
        let test_cases = vec![
            // test for off by one case
            (
                Arc::new(TimestampMillisecondArray::from_iter_values([1, 2, 3, 4, 5])) as ArrayRef,
                false,
                TimeRange {
                    start: Timestamp::new_millisecond(2),
                    end: Timestamp::new_millisecond(4),
                },
                Ok((1, 2)),
            ),
            (
                Arc::new(TimestampMillisecondArray::from_iter_values([
                    -2, -1, 0, 1, 2, 3, 4, 5,
                ])) as ArrayRef,
                false,
                TimeRange {
                    start: Timestamp::new_millisecond(-1),
                    end: Timestamp::new_millisecond(4),
                },
                Ok((1, 5)),
            ),
            (
                Arc::new(TimestampMillisecondArray::from_iter_values([1, 3, 4, 6])) as ArrayRef,
                false,
                TimeRange {
                    start: Timestamp::new_millisecond(2),
                    end: Timestamp::new_millisecond(5),
                },
                Ok((1, 2)),
            ),
            (
                Arc::new(TimestampMillisecondArray::from_iter_values([1, 2, 3, 4, 6])) as ArrayRef,
                false,
                TimeRange {
                    start: Timestamp::new_millisecond(2),
                    end: Timestamp::new_millisecond(5),
                },
                Ok((1, 3)),
            ),
            (
                Arc::new(TimestampMillisecondArray::from_iter_values([1, 3, 4, 5, 6])) as ArrayRef,
                false,
                TimeRange {
                    start: Timestamp::new_millisecond(2),
                    end: Timestamp::new_millisecond(5),
                },
                Ok((1, 2)),
            ),
            (
                Arc::new(TimestampMillisecondArray::from_iter_values([1, 2, 3, 4, 5])) as ArrayRef,
                false,
                TimeRange {
                    start: Timestamp::new_millisecond(6),
                    end: Timestamp::new_millisecond(7),
                },
                Ok((5, 0)),
            ),
            // descending off by one cases
            (
                Arc::new(TimestampMillisecondArray::from_iter_values([5, 3, 2, 1])) as ArrayRef,
                true,
                TimeRange {
                    end: Timestamp::new_millisecond(4),
                    start: Timestamp::new_millisecond(1),
                },
                Ok((1, 3)),
            ),
            (
                Arc::new(TimestampMillisecondArray::from_iter_values([
                    5, 4, 3, 2, 1, 0,
                ])) as ArrayRef,
                true,
                TimeRange {
                    end: Timestamp::new_millisecond(4),
                    start: Timestamp::new_millisecond(1),
                },
                Ok((2, 3)),
            ),
            (
                Arc::new(TimestampMillisecondArray::from_iter_values([5, 3, 2, 0])) as ArrayRef,
                true,
                TimeRange {
                    end: Timestamp::new_millisecond(4),
                    start: Timestamp::new_millisecond(1),
                },
                Ok((1, 2)),
            ),
            (
                Arc::new(TimestampMillisecondArray::from_iter_values([5, 4, 3, 2, 0])) as ArrayRef,
                true,
                TimeRange {
                    end: Timestamp::new_millisecond(4),
                    start: Timestamp::new_millisecond(1),
                },
                Ok((2, 2)),
            ),
            (
                Arc::new(TimestampMillisecondArray::from_iter_values([5, 4, 3, 2, 1])) as ArrayRef,
                true,
                TimeRange {
                    end: Timestamp::new_millisecond(5),
                    start: Timestamp::new_millisecond(2),
                },
                Ok((1, 3)),
            ),
            (
                Arc::new(TimestampMillisecondArray::from_iter_values([5, 4, 3, 1])) as ArrayRef,
                true,
                TimeRange {
                    end: Timestamp::new_millisecond(5),
                    start: Timestamp::new_millisecond(2),
                },
                Ok((1, 2)),
            ),
            (
                Arc::new(TimestampMillisecondArray::from_iter_values([6, 4, 3, 2, 1])) as ArrayRef,
                true,
                TimeRange {
                    end: Timestamp::new_millisecond(5),
                    start: Timestamp::new_millisecond(2),
                },
                Ok((1, 3)),
            ),
            (
                Arc::new(TimestampMillisecondArray::from_iter_values([6, 4, 3, 1])) as ArrayRef,
                true,
                TimeRange {
                    end: Timestamp::new_millisecond(5),
                    start: Timestamp::new_millisecond(2),
                },
                Ok((1, 2)),
            ),
            (
                Arc::new(TimestampMillisecondArray::from_iter_values([
                    10, 9, 8, 7, 6,
                ])) as ArrayRef,
                true,
                TimeRange {
                    end: Timestamp::new_millisecond(5),
                    start: Timestamp::new_millisecond(2),
                },
                Ok((5, 0)),
            ),
            // test off by one case
            (
                Arc::new(TimestampMillisecondArray::from_iter_values([3, 2, 1, 0])) as ArrayRef,
                true,
                TimeRange {
                    end: Timestamp::new_millisecond(4),
                    start: Timestamp::new_millisecond(3),
                },
                Ok((0, 1)),
            ),
            (
                Arc::new(TimestampMillisecondArray::from_iter_values([5, 3, 2])) as ArrayRef,
                true,
                TimeRange {
                    end: Timestamp::new_millisecond(4),
                    start: Timestamp::new_millisecond(3),
                },
                Ok((1, 1)),
            ),
            (
                Arc::new(TimestampMillisecondArray::from_iter_values([5, 4, 3, 2])) as ArrayRef,
                true,
                TimeRange {
                    end: Timestamp::new_millisecond(4),
                    start: Timestamp::new_millisecond(3),
                },
                Ok((2, 1)),
            ),
        ];

        for (sort_vals, descending, range, expected) in test_cases {
            let sort_column = SortColumn {
                values: sort_vals,
                options: Some(SortOptions {
                    descending,
                    ..Default::default()
                }),
            };
            let ret = find_slice_from_range(&sort_column, &range);
            match (ret, expected) {
                (Ok(ret), Ok(expected)) => {
                    assert_eq!(
                        ret, expected,
                        "sort_vals: {:?}, range: {:?}",
                        sort_column, range
                    )
                }
                (Err(err), Err(expected)) => {
                    let expected: &str = expected;
                    assert!(
                        err.to_string().contains(expected),
                        "err: {:?}, expected: {:?}",
                        err,
                        expected
                    );
                }
                (r, e) => panic!("unexpected result: {:?}, expected: {:?}", r, e),
            }
        }
    }

    #[derive(Debug)]
    struct TestStream {
        expression: PhysicalSortExpr,
        fetch: Option<usize>,
        input: Vec<(PartitionRange, DfRecordBatch)>,
        output: Vec<DfRecordBatch>,
        schema: SchemaRef,
    }
    use datafusion::physical_plan::expressions::Column;
    impl TestStream {
        fn new(
            ts_col: Column,
            opt: SortOptions,
            fetch: Option<usize>,
            schema: impl Into<arrow_schema::Fields>,
            input: Vec<(PartitionRange, Vec<ArrayRef>)>,
            expected: Vec<Vec<ArrayRef>>,
        ) -> Self {
            let expression = PhysicalSortExpr {
                expr: Arc::new(ts_col),
                options: opt,
            };
            let schema = Schema::new(schema.into());
            let schema = Arc::new(schema);
            let input = input
                .into_iter()
                .map(|(k, v)| (k, DfRecordBatch::try_new(schema.clone(), v).unwrap()))
                .collect_vec();
            let output_batchs = expected
                .into_iter()
                .map(|v| DfRecordBatch::try_new(schema.clone(), v).unwrap())
                .collect_vec();
            Self {
                expression,
                fetch,
                input,
                output: output_batchs,
                schema,
            }
        }

        async fn run_test(&self) -> Vec<DfRecordBatch> {
            let (ranges, batches): (Vec<_>, Vec<_>) = self.input.clone().into_iter().unzip();

            let mock_input = MockInputExec::new(batches, self.schema.clone());

            let exec = WindowedSortExec::try_new(
                self.expression.clone(),
                self.fetch,
                vec![ranges],
                Arc::new(mock_input),
            )
            .unwrap();

            let exec_stream = exec.execute(0, Arc::new(TaskContext::default())).unwrap();

            let real_output = exec_stream.collect::<Vec<_>>().await;
            let real_output: Vec<_> = real_output.into_iter().try_collect().unwrap();
            real_output
        }
    }

    #[tokio::test]
    async fn test_window_sort_stream() {
        let test_cases = [
            TestStream::new(
                Column::new("ts", 0),
                SortOptions {
                    descending: false,
                    nulls_first: true,
                },
                None,
                vec![Field::new(
                    "ts",
                    DataType::Timestamp(TimeUnit::Millisecond, None),
                    false,
                )],
                vec![],
                vec![],
            ),
            TestStream::new(
                Column::new("ts", 0),
                SortOptions {
                    descending: false,
                    nulls_first: true,
                },
                None,
                vec![Field::new(
                    "ts",
                    DataType::Timestamp(TimeUnit::Millisecond, None),
                    false,
                )],
                vec![
                    // test one empty
                    (
                        PartitionRange {
                            start: Timestamp::new_millisecond(1),
                            end: Timestamp::new_millisecond(2),
                            num_rows: 1,
                            identifier: 0,
                        },
                        vec![Arc::new(TimestampMillisecondArray::from_iter_values([]))],
                    ),
                    (
                        PartitionRange {
                            start: Timestamp::new_millisecond(1),
                            end: Timestamp::new_millisecond(3),
                            num_rows: 1,
                            identifier: 0,
                        },
                        vec![Arc::new(TimestampMillisecondArray::from_iter_values([2]))],
                    ),
                ],
                vec![vec![Arc::new(TimestampMillisecondArray::from_iter_values(
                    [2],
                ))]],
            ),
            TestStream::new(
                Column::new("ts", 0),
                SortOptions {
                    descending: false,
                    nulls_first: true,
                },
                None,
                vec![Field::new(
                    "ts",
                    DataType::Timestamp(TimeUnit::Millisecond, None),
                    false,
                )],
                vec![
                    // test one empty
                    (
                        PartitionRange {
                            start: Timestamp::new_millisecond(1),
                            end: Timestamp::new_millisecond(2),
                            num_rows: 1,
                            identifier: 0,
                        },
                        vec![Arc::new(TimestampMillisecondArray::from_iter_values([]))],
                    ),
                    (
                        PartitionRange {
                            start: Timestamp::new_millisecond(1),
                            end: Timestamp::new_millisecond(3),
                            num_rows: 1,
                            identifier: 0,
                        },
                        vec![Arc::new(TimestampMillisecondArray::from_iter_values([]))],
                    ),
                ],
                vec![],
            ),
            TestStream::new(
                Column::new("ts", 0),
                SortOptions {
                    descending: false,
                    nulls_first: true,
                },
                None,
                vec![Field::new(
                    "ts",
                    DataType::Timestamp(TimeUnit::Millisecond, None),
                    false,
                )],
                vec![
                    // test indistinguishable case
                    // we can't know whether `2` belong to which range
                    (
                        PartitionRange {
                            start: Timestamp::new_millisecond(1),
                            end: Timestamp::new_millisecond(2),
                            num_rows: 1,
                            identifier: 0,
                        },
                        vec![Arc::new(TimestampMillisecondArray::from_iter_values([1]))],
                    ),
                    (
                        PartitionRange {
                            start: Timestamp::new_millisecond(1),
                            end: Timestamp::new_millisecond(3),
                            num_rows: 1,
                            identifier: 0,
                        },
                        vec![Arc::new(TimestampMillisecondArray::from_iter_values([2]))],
                    ),
                ],
                vec![
                    vec![Arc::new(TimestampMillisecondArray::from_iter_values([1]))],
                    vec![Arc::new(TimestampMillisecondArray::from_iter_values([2]))],
                ],
            ),
            TestStream::new(
                Column::new("ts", 0),
                SortOptions {
                    descending: false,
                    nulls_first: true,
                },
                None,
                vec![Field::new(
                    "ts",
                    DataType::Timestamp(TimeUnit::Millisecond, None),
                    false,
                )],
                vec![
                    // test direct emit
                    (
                        PartitionRange {
                            start: Timestamp::new_millisecond(1),
                            end: Timestamp::new_millisecond(3),
                            num_rows: 1,
                            identifier: 0,
                        },
                        vec![Arc::new(TimestampMillisecondArray::from_iter_values([
                            1, 2,
                        ]))],
                    ),
                    (
                        PartitionRange {
                            start: Timestamp::new_millisecond(1),
                            end: Timestamp::new_millisecond(4),
                            num_rows: 1,
                            identifier: 0,
                        },
                        vec![Arc::new(TimestampMillisecondArray::from_iter_values([
                            2, 3,
                        ]))],
                    ),
                ],
                vec![
                    vec![Arc::new(TimestampMillisecondArray::from_iter_values([
                        1, 2,
                    ]))],
                    // didn't trigger a merge sort/concat here so this is it
                    vec![Arc::new(TimestampMillisecondArray::from_iter_values([2]))],
                    vec![Arc::new(TimestampMillisecondArray::from_iter_values([3]))],
                ],
            ),
            TestStream::new(
                Column::new("ts", 0),
                SortOptions {
                    descending: false,
                    nulls_first: true,
                },
                None,
                vec![Field::new(
                    "ts",
                    DataType::Timestamp(TimeUnit::Millisecond, None),
                    false,
                )],
                vec![
                    // test more of cross working range batch intersection
                    (
                        PartitionRange {
                            start: Timestamp::new_millisecond(1),
                            end: Timestamp::new_millisecond(3),
                            num_rows: 1,
                            identifier: 0,
                        },
                        vec![Arc::new(TimestampMillisecondArray::from_iter_values([
                            1, 2,
                        ]))],
                    ),
                    (
                        PartitionRange {
                            start: Timestamp::new_millisecond(1),
                            end: Timestamp::new_millisecond(4),
                            num_rows: 1,
                            identifier: 1,
                        },
                        vec![Arc::new(TimestampMillisecondArray::from_iter_values([
                            1, 2, 3,
                        ]))],
                    ),
                ],
                vec![
                    vec![Arc::new(TimestampMillisecondArray::from_iter_values([
                        1, 1, 2, 2,
                    ]))],
                    vec![Arc::new(TimestampMillisecondArray::from_iter_values([3]))],
                ],
            ),
            TestStream::new(
                Column::new("ts", 0),
                SortOptions {
                    descending: false,
                    nulls_first: true,
                },
                None,
                vec![Field::new(
                    "ts",
                    DataType::Timestamp(TimeUnit::Millisecond, None),
                    false,
                )],
                vec![
                    // no overlap, empty intersection batch case
                    (
                        PartitionRange {
                            start: Timestamp::new_millisecond(1),
                            end: Timestamp::new_millisecond(3),
                            num_rows: 1,
                            identifier: 0,
                        },
                        vec![Arc::new(TimestampMillisecondArray::from_iter_values([
                            1, 2,
                        ]))],
                    ),
                    (
                        PartitionRange {
                            start: Timestamp::new_millisecond(1),
                            end: Timestamp::new_millisecond(4),
                            num_rows: 1,
                            identifier: 1,
                        },
                        vec![Arc::new(TimestampMillisecondArray::from_iter_values([
                            1, 2, 3,
                        ]))],
                    ),
                    (
                        PartitionRange {
                            start: Timestamp::new_millisecond(4),
                            end: Timestamp::new_millisecond(6),
                            num_rows: 1,
                            identifier: 1,
                        },
                        vec![Arc::new(TimestampMillisecondArray::from_iter_values([
                            4, 5,
                        ]))],
                    ),
                ],
                vec![
                    vec![Arc::new(TimestampMillisecondArray::from_iter_values([
                        1, 1, 2, 2,
                    ]))],
                    vec![Arc::new(TimestampMillisecondArray::from_iter_values([3]))],
                    vec![Arc::new(TimestampMillisecondArray::from_iter_values([
                        4, 5,
                    ]))],
                ],
            ),
            // test fetch
            TestStream::new(
                Column::new("ts", 0),
                SortOptions {
                    descending: false,
                    nulls_first: true,
                },
                Some(6),
                vec![Field::new(
                    "ts",
                    DataType::Timestamp(TimeUnit::Millisecond, None),
                    false,
                )],
                vec![
                    // no overlap, empty intersection batch case
                    (
                        PartitionRange {
                            start: Timestamp::new_millisecond(1),
                            end: Timestamp::new_millisecond(3),
                            num_rows: 1,
                            identifier: 0,
                        },
                        vec![Arc::new(TimestampMillisecondArray::from_iter_values([
                            1, 2,
                        ]))],
                    ),
                    (
                        PartitionRange {
                            start: Timestamp::new_millisecond(1),
                            end: Timestamp::new_millisecond(4),
                            num_rows: 1,
                            identifier: 1,
                        },
                        vec![Arc::new(TimestampMillisecondArray::from_iter_values([
                            1, 2, 3,
                        ]))],
                    ),
                    (
                        PartitionRange {
                            start: Timestamp::new_millisecond(3),
                            end: Timestamp::new_millisecond(6),
                            num_rows: 1,
                            identifier: 1,
                        },
                        vec![Arc::new(TimestampMillisecondArray::from_iter_values([
                            4, 5,
                        ]))],
                    ),
                ],
                vec![
                    vec![Arc::new(TimestampMillisecondArray::from_iter_values([
                        1, 1, 2, 2,
                    ]))],
                    vec![Arc::new(TimestampMillisecondArray::from_iter_values([3]))],
                    vec![Arc::new(TimestampMillisecondArray::from_iter_values([4]))],
                ],
            ),
            TestStream::new(
                Column::new("ts", 0),
                SortOptions {
                    descending: false,
                    nulls_first: true,
                },
                Some(3),
                vec![Field::new(
                    "ts",
                    DataType::Timestamp(TimeUnit::Millisecond, None),
                    false,
                )],
                vec![
                    // no overlap, empty intersection batch case
                    (
                        PartitionRange {
                            start: Timestamp::new_millisecond(1),
                            end: Timestamp::new_millisecond(3),
                            num_rows: 1,
                            identifier: 0,
                        },
                        vec![Arc::new(TimestampMillisecondArray::from_iter_values([
                            1, 2,
                        ]))],
                    ),
                    (
                        PartitionRange {
                            start: Timestamp::new_millisecond(1),
                            end: Timestamp::new_millisecond(4),
                            num_rows: 1,
                            identifier: 1,
                        },
                        vec![Arc::new(TimestampMillisecondArray::from_iter_values([
                            1, 2, 3,
                        ]))],
                    ),
                    (
                        PartitionRange {
                            start: Timestamp::new_millisecond(3),
                            end: Timestamp::new_millisecond(6),
                            num_rows: 1,
                            identifier: 1,
                        },
                        vec![Arc::new(TimestampMillisecondArray::from_iter_values([
                            4, 5,
                        ]))],
                    ),
                ],
                vec![vec![Arc::new(TimestampMillisecondArray::from_iter_values(
                    [1, 1, 2],
                ))]],
            ),
            // rev case
            TestStream::new(
                Column::new("ts", 0),
                SortOptions {
                    descending: true,
                    nulls_first: true,
                },
                None,
                vec![Field::new(
                    "ts",
                    DataType::Timestamp(TimeUnit::Millisecond, None),
                    false,
                )],
                vec![
                    // reverse order
                    (
                        PartitionRange {
                            start: Timestamp::new_millisecond(3),
                            end: Timestamp::new_millisecond(6),
                            num_rows: 1,
                            identifier: 1,
                        },
                        vec![Arc::new(TimestampMillisecondArray::from_iter_values([
                            5, 4,
                        ]))],
                    ),
                    (
                        PartitionRange {
                            start: Timestamp::new_millisecond(1),
                            end: Timestamp::new_millisecond(4),
                            num_rows: 1,
                            identifier: 1,
                        },
                        vec![Arc::new(TimestampMillisecondArray::from_iter_values([
                            3, 2, 1,
                        ]))],
                    ),
                    (
                        PartitionRange {
                            start: Timestamp::new_millisecond(1),
                            end: Timestamp::new_millisecond(3),
                            num_rows: 1,
                            identifier: 0,
                        },
                        vec![Arc::new(TimestampMillisecondArray::from_iter_values([
                            2, 1,
                        ]))],
                    ),
                ],
                vec![
                    vec![Arc::new(TimestampMillisecondArray::from_iter_values([
                        5, 4,
                    ]))],
                    vec![Arc::new(TimestampMillisecondArray::from_iter_values([3]))],
                    vec![Arc::new(TimestampMillisecondArray::from_iter_values([
                        2, 2, 1, 1,
                    ]))],
                ],
            ),
            TestStream::new(
                Column::new("ts", 0),
                SortOptions {
                    descending: false,
                    nulls_first: true,
                },
                None,
                vec![Field::new(
                    "ts",
                    DataType::Timestamp(TimeUnit::Millisecond, None),
                    false,
                )],
                vec![
                    // long have subset short run case
                    (
                        PartitionRange {
                            start: Timestamp::new_millisecond(1),
                            end: Timestamp::new_millisecond(10),
                            num_rows: 1,
                            identifier: 0,
                        },
                        vec![Arc::new(TimestampMillisecondArray::from_iter_values([
                            1, 5, 9,
                        ]))],
                    ),
                    (
                        PartitionRange {
                            start: Timestamp::new_millisecond(3),
                            end: Timestamp::new_millisecond(7),
                            num_rows: 1,
                            identifier: 1,
                        },
                        vec![Arc::new(TimestampMillisecondArray::from_iter_values([
                            3, 4, 5, 6,
                        ]))],
                    ),
                ],
                vec![
                    vec![Arc::new(TimestampMillisecondArray::from_iter_values([1]))],
                    vec![Arc::new(TimestampMillisecondArray::from_iter_values([
                        3, 4, 5, 5, 6, 9,
                    ]))],
                ],
            ),
            TestStream::new(
                Column::new("ts", 0),
                SortOptions {
                    descending: false,
                    nulls_first: true,
                },
                None,
                vec![Field::new(
                    "ts",
                    DataType::Timestamp(TimeUnit::Millisecond, None),
                    false,
                )],
                vec![
                    // complex overlap
                    (
                        PartitionRange {
                            start: Timestamp::new_millisecond(1),
                            end: Timestamp::new_millisecond(3),
                            num_rows: 1,
                            identifier: 0,
                        },
                        vec![Arc::new(TimestampMillisecondArray::from_iter_values([
                            1, 2,
                        ]))],
                    ),
                    (
                        PartitionRange {
                            start: Timestamp::new_millisecond(1),
                            end: Timestamp::new_millisecond(10),
                            num_rows: 1,
                            identifier: 1,
                        },
                        vec![Arc::new(TimestampMillisecondArray::from_iter_values([
                            1, 3, 4, 5, 6, 8,
                        ]))],
                    ),
                    (
                        PartitionRange {
                            start: Timestamp::new_millisecond(7),
                            end: Timestamp::new_millisecond(10),
                            num_rows: 1,
                            identifier: 1,
                        },
                        vec![Arc::new(TimestampMillisecondArray::from_iter_values([
                            7, 8, 9,
                        ]))],
                    ),
                ],
                vec![
                    vec![Arc::new(TimestampMillisecondArray::from_iter_values([
                        1, 1, 2,
                    ]))],
                    vec![Arc::new(TimestampMillisecondArray::from_iter_values([
                        3, 4, 5, 6,
                    ]))],
                    vec![Arc::new(TimestampMillisecondArray::from_iter_values([
                        7, 8, 8, 9,
                    ]))],
                ],
            ),
            TestStream::new(
                Column::new("ts", 0),
                SortOptions {
                    descending: false,
                    nulls_first: true,
                },
                None,
                vec![Field::new(
                    "ts",
                    DataType::Timestamp(TimeUnit::Millisecond, None),
                    false,
                )],
                vec![
                    // complex subset with having same datapoint
                    (
                        PartitionRange {
                            start: Timestamp::new_millisecond(1),
                            end: Timestamp::new_millisecond(11),
                            num_rows: 1,
                            identifier: 0,
                        },
                        vec![Arc::new(TimestampMillisecondArray::from_iter_values([
                            1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
                        ]))],
                    ),
                    (
                        PartitionRange {
                            start: Timestamp::new_millisecond(5),
                            end: Timestamp::new_millisecond(7),
                            num_rows: 1,
                            identifier: 1,
                        },
                        vec![Arc::new(TimestampMillisecondArray::from_iter_values([
                            5, 6,
                        ]))],
                    ),
                ],
                vec![
                    vec![Arc::new(TimestampMillisecondArray::from_iter_values([
                        1, 2, 3, 4,
                    ]))],
                    vec![Arc::new(TimestampMillisecondArray::from_iter_values([
                        5, 5, 6, 6, 7, 8, 9, 10,
                    ]))],
                ],
            ),
        ];

        let indexed_test_cases = test_cases.iter().enumerate().collect_vec();

        for (idx, testcase) in &indexed_test_cases {
            let output = testcase.run_test().await;
            assert_eq!(output, testcase.output, "case {idx} failed.");
        }
    }

    #[tokio::test]
    async fn fuzzy_ish_test_window_sort_stream() {
        let test_cnt = 100;
        let part_cnt_bound = 100;
        let range_size_bound = 100;
        let range_offset_bound = 100;
        let in_range_datapoint_cnt_bound = 100;
        let fetch_bound = 100;

        let mut rng = fastrand::Rng::new();
        let rng_seed = rng.u64(..);
        rng.seed(rng_seed);
        let mut bound_val = None;
        // construct testcases
        type CmpFn<T> = Box<dyn FnMut(&T, &T) -> std::cmp::Ordering>;
        let mut full_testcase_list = Vec::new();
        for _case_id in 0..test_cnt {
            let descending = rng.bool();
            fn ret_cmp_fn<T: Ord>(descending: bool) -> CmpFn<T> {
                if descending {
                    return Box::new(|a: &T, b: &T| b.cmp(a));
                }
                Box::new(|a: &T, b: &T| a.cmp(b))
            }
            let unit = match rng.u8(0..3) {
                0 => TimeUnit::Second,
                1 => TimeUnit::Millisecond,
                2 => TimeUnit::Microsecond,
                _ => TimeUnit::Nanosecond,
            };
            let fetch = if rng.bool() {
                Some(rng.usize(0..fetch_bound))
            } else {
                None
            };

            let mut input_ranged_data = vec![];
            let mut output_data: Vec<i64> = vec![];
            // generate input data
            for part_id in 0..rng.usize(0..part_cnt_bound) {
                let (start, end) = if descending {
                    let end = bound_val
                        .map(|i| i - rng.i64(0..range_offset_bound))
                        .unwrap_or_else(|| rng.i64(..));
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

                let iter = 0..rng.usize(0..in_range_datapoint_cnt_bound);
                let data_gen = iter
                    .map(|_| rng.i64(start.value()..end.value()))
                    .sorted_by(ret_cmp_fn(descending))
                    .collect_vec();
                output_data.extend(data_gen.clone());
                let arr = new_ts_array(unit, data_gen);
                let range = PartitionRange {
                    start,
                    end,
                    num_rows: arr.len(),
                    identifier: part_id,
                };
                input_ranged_data.push((range, vec![arr]));
            }

            output_data.sort_by(ret_cmp_fn(descending));
            if let Some(fetch) = fetch {
                output_data.truncate(fetch);
            }
            let output_arr = new_ts_array(unit, output_data);

            let test_stream = TestStream::new(
                Column::new("ts", 0),
                SortOptions {
                    descending,
                    nulls_first: true,
                },
                fetch,
                vec![Field::new("ts", DataType::Timestamp(unit, None), false)],
                input_ranged_data.clone(),
                vec![vec![output_arr]],
            );
            full_testcase_list.push(test_stream);
        }

        for (case_id, test_stream) in full_testcase_list.into_iter().enumerate() {
            let res = test_stream.run_test().await;
            let res_concat = concat_batches(&test_stream.schema, &res).unwrap();
            let expected = test_stream.output;
            let expected_concat = concat_batches(&test_stream.schema, &expected).unwrap();

            if res_concat != expected_concat {
                {
                    let mut f_input = std::io::stderr();
                    f_input.write_all(b"[").unwrap();
                    for (input_range, input_arr) in test_stream.input {
                        let range_json = json!({
                            "start": input_range.start.to_chrono_datetime().unwrap().to_string(),
                            "end": input_range.end.to_chrono_datetime().unwrap().to_string(),
                            "num_rows": input_range.num_rows,
                            "identifier": input_range.identifier,
                        });
                        let buf = Vec::new();
                        let mut input_writer = ArrayWriter::new(buf);
                        input_writer.write(&input_arr).unwrap();
                        input_writer.finish().unwrap();
                        let res_str =
                            String::from_utf8_lossy(&input_writer.into_inner()).to_string();
                        let whole_json =
                            format!(r#"{{"range": {}, "data": {}}},"#, range_json, res_str);
                        f_input.write_all(whole_json.as_bytes()).unwrap();
                    }
                    f_input.write_all(b"]").unwrap();
                }
                {
                    let mut f_res = std::io::stderr();
                    f_res.write_all(b"[").unwrap();
                    for batch in &res {
                        let mut res_writer = ArrayWriter::new(f_res);
                        res_writer.write(batch).unwrap();
                        res_writer.finish().unwrap();
                        f_res = res_writer.into_inner();
                        f_res.write_all(b",").unwrap();
                    }
                    f_res.write_all(b"]").unwrap();

                    let f_res_concat = std::io::stderr();
                    let mut res_writer = ArrayWriter::new(f_res_concat);
                    res_writer.write(&res_concat).unwrap();
                    res_writer.finish().unwrap();

                    let f_expected = std::io::stderr();
                    let mut expected_writer = ArrayWriter::new(f_expected);
                    expected_writer.write(&expected_concat).unwrap();
                    expected_writer.finish().unwrap();
                }
                panic!(
                    "case failed, case id: {0}, output and expected output to stderr",
                    case_id
                );
            }
            assert_eq!(
                res_concat, expected_concat,
                "case failed, case id: {}, rng seed: {}",
                case_id, rng_seed
            );
        }
    }
}
