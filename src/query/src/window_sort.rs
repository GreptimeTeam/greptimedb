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

// TODO(discord9): remove allow(unused) after implementation is done
#![allow(unused)]

use std::collections::{BTreeMap, BTreeSet};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::array::{
    Array, ArrayRef, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray,
};
use arrow::compute::SortColumn;
use arrow_schema::{DataType, SchemaRef, SortOptions, TimeUnit};
use async_stream::stream;
use common_error::ext::{BoxedError, PlainError};
use common_error::status_code::StatusCode;
use common_recordbatch::adapter::RecordBatchStreamAdapter;
use common_recordbatch::{DfRecordBatch, DfSendableRecordBatchStream, SendableRecordBatchStream};
use common_time::Timestamp;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryPool, MemoryReservation};
use datafusion::execution::{RecordBatchStream, TaskContext};
use datafusion::physical_plan::memory::MemoryStream;
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet};
use datafusion::physical_plan::sorts::streaming_merge::streaming_merge;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::{internal_err, DataFusionError, ScalarValue};
use datafusion_physical_expr::PhysicalSortExpr;
use futures::Stream;
use itertools::Itertools;
use snafu::ResultExt;
use store_api::region_engine::PartitionRange;

use crate::error::{QueryExecutionSnafu, Result};

/// A complex stream sort execution plan which accepts a list of `PartitionRange` and
/// merge sort them whenever possible, and emit the sorted result as soon as possible.
/// This sorting plan only accept sort by ts and will not sort by other fields.
///
/// internally, it call [`streaming_merge`] multiple times to merge multiple sorted ranges
#[derive(Debug, Clone)]
pub struct WindowedSortExec {
    /// Physical sort expressions(that is, sort by timestamp)
    expression: PhysicalSortExpr,
    /// Optional number of rows to fetch. Stops producing rows after this fetch
    fetch: Option<usize>,
    /// The input ranges indicate input stream will be composed of those ranges in given order
    ranges: Vec<PartitionRange>,
    /// Overlapping Timestamp Ranges'index given the input ranges
    ///
    /// note the key ranges here should not overlapping with each other
    overlap_counts: BTreeMap<(Timestamp, Timestamp), Vec<usize>>,
    /// record all existing timestamps in the input `ranges`
    all_exist_timestamps: BTreeSet<Timestamp>,
    input: Arc<dyn ExecutionPlan>,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
}

impl WindowedSortExec {
    pub fn try_new(
        expression: PhysicalSortExpr,
        fetch: Option<usize>,
        ranges: Vec<PartitionRange>,
        input: Arc<dyn ExecutionPlan>,
    ) -> Result<Self> {
        if !check_lower_bound_monotonicity(&ranges) {
            let plain_error = PlainError::new(
                "Input `PartitionRange`s's lower bound is not monotonic non-decrease".to_string(),
                StatusCode::Unexpected,
            );
            return Err(BoxedError::new(plain_error)).context(QueryExecutionSnafu {});
        }
        let all_exist_timestamps = find_all_exist_timestamps(&ranges);
        let overlap_counts = split_overlapping_ranges(&ranges);
        Ok(Self {
            expression,
            fetch,
            ranges,
            overlap_counts,
            all_exist_timestamps,
            input,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }

    /// During receiving partial-sorted RecordBatch, we need to update the working set which is the
    /// `PartitionRange` we think those RecordBatch belongs to. And when we receive something outside of working set, we can merge results before whenever possible.
    pub fn to_stream(
        &self,
        context: Arc<TaskContext>,
        partition: usize,
    ) -> datafusion_common::Result<DfSendableRecordBatchStream> {
        if 0 != partition {
            return Err(DataFusionError::Internal(format!(
                "WindowedSortExec invalid partition {partition}"
            )));
        }

        let input_stream: DfSendableRecordBatchStream =
            self.input.execute(partition, context.clone())?;

        let df_stream = Box::pin(WindowedSortStream::new(context, self, input_stream)) as _;

        Ok(df_stream)
    }
}

/// The core logic of merging sort multiple sorted ranges
pub struct WindowedSortStream {
    /// Memory pool for this stream
    memory_pool: Arc<dyn MemoryPool>,
    /// currently assembling RecordBatches, will be put to `sort_partition_rbs` when it's done
    in_progress: Vec<(DfRecordBatch, SucRun<Timestamp>)>,
    /// last `Timestamp` of the last input RecordBatch in `in_progress`, use to found partial sorted run's boundary
    last_value: Option<Timestamp>,
    /// Current working set of `PartitionRange` sorted RecordBatches
    sort_partition_rbs: Vec<DfSendableRecordBatchStream>,
    /// Merge-sorted result stream, should be polled to end before start a new merge sort again
    merge_stream: Option<DfSendableRecordBatchStream>,
    /// The number of times merge sort has been called
    merge_count: usize,
    /// Current working set of `PartitionRange`, update when need to merge sort more PartitionRange
    working_set: BTreeSet<usize>,
    /// Current working set of `PartitionRange`'s timestamp range
    ///
    /// expand upper bound(and add to working set if needed) by checking if next range in `overlap_counts` contain at least two index and only one range is not in `working_set`
    /// (Since one index is not enough to merge and can be emitted directly)
    ///
    /// Hence, `working_range` might not be a union of all ranges in working set
    working_range: Option<(Timestamp, Timestamp)>,
    /// input stream assumed reading in order of `PartitionRange`
    input: DfSendableRecordBatchStream,
    /// sOutput Schema, which is the same as input schema, since this is a sort plan
    schema: SchemaRef,
    /// Physical sort expressions(that is, sort by timestamp)
    expression: PhysicalSortExpr,
    /// Optional number of rows to fetch. Stops producing rows after this fetch
    fetch: Option<usize>,
    /// Resulting Stream(`merge_stream`)'s batch size
    batch_size: usize,
    /// The input ranges indicate input stream will be composed of those ranges in given order
    ranges: Vec<PartitionRange>,
    /// Overlapping Timestamp Ranges'index given the input ranges
    ///
    /// note the key ranges here should not overlapping with each other
    overlap_counts: BTreeMap<(Timestamp, Timestamp), Vec<usize>>,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
}

impl WindowedSortStream {
    pub fn new(
        context: Arc<TaskContext>,
        exec: &WindowedSortExec,
        input: DfSendableRecordBatchStream,
    ) -> Self {
        Self {
            memory_pool: context.runtime_env().memory_pool.clone(),
            in_progress: Vec::new(),
            last_value: None,
            sort_partition_rbs: Vec::new(),
            merge_stream: None,
            merge_count: 0,
            working_set: BTreeSet::new(),
            working_range: None,
            schema: input.schema(),
            input,
            expression: exec.expression.clone(),
            fetch: exec.fetch,
            batch_size: context.session_config().batch_size(),
            ranges: exec.ranges.clone(),
            overlap_counts: exec.overlap_counts.clone(),
            metrics: exec.metrics.clone(),
        }
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
        && run.offset == 0
        && run.len == batch.num_rows()
        && sorted_runs_offset.len() == 1
    {
        // input rb is already sorted, we can emit it directly
        Ok(SortedRunSet {
            runs_with_batch: vec![(batch, run.clone())],
            sort_column,
        })
    } else {
        // those slice should be zero copy, so supposely no new reservation needed
        let mut ret = Vec::new();
        for run in sorted_runs_offset {
            let new_rb = batch.slice(run.offset, run.len);
            ret.push((new_rb, run));
        }
        Ok(SortedRunSet {
            runs_with_batch: ret,
            sort_column,
        })
    }
}

impl WindowedSortStream {
    /// The core logic of merging sort multiple sorted ranges
    ///
    /// We try to maximize the number of sorted runs we can merge in one go, while emit the result as soon as possible.
    pub fn poll_next_inner(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<datafusion_common::Result<DfRecordBatch>>> {
        // first check and send out the merge result
        if let Some(merge_stream) = &mut self.merge_stream {
            match merge_stream.as_mut().poll_next(cx) {
                Poll::Ready(Some(Ok(batch))) => {
                    return Poll::Ready(Some(Ok(batch)));
                }
                Poll::Ready(Some(Err(e))) => {
                    return Poll::Ready(Some(Err(e)));
                }
                Poll::Ready(None) => {
                    self.merge_stream = None;
                }
                Poll::Pending => {
                    return Poll::Pending;
                }
            }
        }

        // then we get a new RecordBatch from input stream
        let new_input_rbs = match self.input.as_mut().poll_next(cx) {
            Poll::Ready(Some(Ok(batch))) => {
                Some(split_batch_to_sorted_run(batch, &self.expression)?)
            }
            Poll::Ready(Some(Err(e))) => {
                return Poll::Ready(Some(Err(e)));
            }
            Poll::Ready(None) => {
                // input stream is done, we need to merge sort the remaining working set
                None
            }
            Poll::Pending => return Poll::Pending,
        };

        // The core logic to eargerly merge sort the working set
        match new_input_rbs {
            Some(SortedRunSet {
                runs_with_batch,
                sort_column,
            }) => {
                // compare with last_value to find boundary, then merge runs if needed

                // iterate over runs_with_batch to merge sort, might create zero or more stream to put to `sort_partition_rbs`
                for (cur_rb, run_info) in &runs_with_batch {
                    // TODO: precompute all available working ranges and iter over them!
                    {
                        let mut cur_range = None;
                        let mut cur_set = BTreeSet::new();
                        for (range, set) in &self.overlap_counts {
                            if cur_range.is_none() {
                                cur_range = Some(*range);
                                cur_set.extend(set.clone().into_iter());
                            } else if let Some(cur_range) = &mut cur_range
                                && set.len() >= 2
                            {
                                // expand working range on those condition
                                cur_range.1 = range.1;
                                cur_set.extend(set.into_iter());
                            }
                        }
                    }
                    // determine if we can concat the current run to `in_progress`
                    // TODO: maintain `current_range` and `current_working_set` to determine if we can concat the current run
                    let is_ok_to_concat =
                        cmp_with_opts(&self.last_value, &run_info.first_val, &sort_column.options)
                            <= std::cmp::Ordering::Equal;
                    if is_ok_to_concat {
                        self.in_progress.push((cur_rb.clone(), run_info.clone()));
                    } else {
                        // we need to merge sort the current working set
                    }
                    self.last_value = run_info.last_val;
                }
                todo!("iterate over runs_with_batch to merge sort");
            }
            None => {
                todo!("Input complete, merge sort the rest of the working set");
            }
        }

        todo!()
    }

    /// make `in_progress` as a new `DfSendableRecordBatchStream` and put into `sort_partition_rbs`
    fn build_recordbatch(&mut self) -> datafusion_common::Result<()> {
        let done = std::mem::take(&mut self.in_progress);
        let data = done.into_iter().map(|(rb, _)| rb).collect_vec();
        let new_stream = MemoryStream::try_new(data, self.schema(), None)?;
        self.sort_partition_rbs.push(Box::pin(new_stream));
        Ok(())
    }

    /// make a new `DfSendableRecordBatchStream` from `in_progress` until a given Timestamp(sorted by `sort_opts`) and put them  and put into `sort_partition_rbs`
    fn build_recordbatch_until(
        &mut self,
        until: Timestamp,
        sort_opts: &Option<SortOptions>,
    ) -> datafusion_common::Result<()> {
        todo!("Binary search to find the boundary");
    }

    /// Start merging sort the current working set
    fn start_merge_sort(&mut self) -> datafusion_common::Result<()> {
        if self.merge_stream.is_some() {
            return internal_err!("Merge stream already exists");
        }
        let reservation = MemoryConsumer::new(format!("WindowedSortStream[{}]", self.merge_count))
            .register(&self.memory_pool);

        let streams = std::mem::take(&mut self.sort_partition_rbs);
        let resulting_stream = streaming_merge(
            streams,
            self.schema(),
            &[self.expression.clone()],
            BaselineMetrics::new(&self.metrics, 0),
            self.batch_size,
            self.fetch,
            reservation,
        );
        Ok(())
    }
}

impl Stream for WindowedSortStream {
    type Item = datafusion_common::Result<DfRecordBatch>;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<datafusion_common::Result<DfRecordBatch>>> {
        self.poll_next_inner(cx)
    }
}

impl RecordBatchStream for WindowedSortStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

macro_rules! cast_as_ts_array_iter {
    ($datatype:expr,$array:expr, $($pat:pat => $pty:ty),+) => {
        match $datatype{
            $(
                $pat => {
                    let arr = $array
                        .as_any()
                        .downcast_ref::<$pty>()
                        .unwrap();
                    let iter = arr.iter().enumerate();
                    Box::new(iter) as Box<dyn Iterator<Item = (usize, Option<i64>)>>
                }
            )+
        }
    };
}

/// Get timestamp from array at offset
fn get_timestamp_from(
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

    let array = array.slice(offset, 1);
    let mut iter = cast_as_ts_array_iter!(
        time_unit, array,
        TimeUnit::Second => TimestampSecondArray,
        TimeUnit::Millisecond => TimestampMillisecondArray,
        TimeUnit::Microsecond => TimestampMicrosecondArray,
        TimeUnit::Nanosecond => TimestampNanosecondArray
    );
    let (_idx, val) = iter.next().ok_or_else(|| {
        DataFusionError::Internal("Empty array in get_timestamp_from".to_string())
    })?;
    let val = if let Some(val) = val {
        val
    } else {
        return Ok(None);
    };
    let gt_timestamp = new_timestamp_from(time_unit, val);
    Ok(Some(gt_timestamp))
}

fn new_timestamp_from(time_unit: &TimeUnit, value: i64) -> Timestamp {
    match time_unit {
        TimeUnit::Second => Timestamp::new_second(value),
        TimeUnit::Millisecond => Timestamp::new_millisecond(value),
        TimeUnit::Microsecond => Timestamp::new_microsecond(value),
        TimeUnit::Nanosecond => Timestamp::new_nanosecond(value),
    }
}

/// Compare with options, note None is considered as NULL here
fn cmp_with_opts<T: Ord>(
    a: &Option<T>,
    b: &Option<T>,
    opt: &Option<SortOptions>,
) -> std::cmp::Ordering {
    if let Some(opt) = opt {
        if let (Some(a), Some(b)) = (a, b) {
            if opt.descending {
                b.cmp(a)
            } else {
                a.cmp(b)
            }
        } else {
            match (opt.nulls_first, a.is_none()) {
                (true, true) => std::cmp::Ordering::Less,
                (true, false) => std::cmp::Ordering::Greater,
                (false, true) => std::cmp::Ordering::Greater,
                (false, false) => std::cmp::Ordering::Less,
            }
        }
    } else {
        a.cmp(b)
    }
}

#[derive(Debug, Clone)]
struct SortedRunSet<N: Ord> {
    /// sorted runs with batch corrseponding to them
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

/// find all successive runs in the input iterator
fn find_successive_runs<T: Iterator<Item = (usize, Option<N>)>, N: Ord + Copy>(
    iter: T,
    sort_opts: &Option<SortOptions>,
) -> Vec<SucRun<N>> {
    let mut runs = Vec::new();
    let mut last_value = None;
    let mut iter_len = 0;

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
        iter_len = idx;
    }
    let run = SucRun {
        offset: last_offset,
        len: iter_len - last_offset + 1,
        first_val,
        last_val,
    };
    runs.push(run);

    runs
}

/// return a list of non-overlaping (offset, length) which represent sorted runs, and
/// can be used to call [`DfRecordBatch::slice`] to get sorted runs
/// Returned runs will be as long as possible, and will not overlap with each other
fn get_sorted_runs(sort_column: SortColumn) -> datafusion_common::Result<Vec<SucRun<Timestamp>>> {
    let ty = sort_column.values.data_type();
    if let DataType::Timestamp(unit, _) = ty {
        let iter = cast_as_ts_array_iter!(
            unit, sort_column.values,
            TimeUnit::Second => TimestampSecondArray,
            TimeUnit::Millisecond => TimestampMillisecondArray,
            TimeUnit::Microsecond => TimestampMicrosecondArray,
            TimeUnit::Nanosecond => TimestampNanosecondArray
        );

        let raw = find_successive_runs(iter, &sort_column.options);
        let ts_runs = raw
            .into_iter()
            .map(|run| SucRun {
                offset: run.offset,
                len: run.len,
                first_val: run.first_val.map(|v| new_timestamp_from(unit, v)),
                last_val: run.last_val.map(|v| new_timestamp_from(unit, v)),
            })
            .collect_vec();
        Ok(ts_runs)
    } else {
        Err(DataFusionError::Internal(format!(
            "Unsupported sort column type: {ty}"
        )))
    }
}

/// Check if two timestamp ranges are overlapping, if only end of range1 is equal to start of range2 or verse visa, they are not overlapping.
fn is_overlapping(a: &(Timestamp, Timestamp), b: &(Timestamp, Timestamp)) -> bool {
    debug_assert!(a.0 <= a.1 && b.0 <= b.1);
    !(a.0 >= b.1 || a.1 <= b.0)
}

fn range_intersection(
    a: &(Timestamp, Timestamp),
    b: &(Timestamp, Timestamp),
) -> Option<(Timestamp, Timestamp)> {
    debug_assert!(a.0 <= a.1 && b.0 <= b.1);
    if is_overlapping(a, b) {
        Some((a.0.max(b.0), a.1.min(b.1)))
    } else {
        None
    }
}

fn range_difference(
    a: &(Timestamp, Timestamp),
    b: &(Timestamp, Timestamp),
) -> Vec<(Timestamp, Timestamp)> {
    debug_assert!(a.0 <= a.1 && b.0 <= b.1);
    if !is_overlapping(a, b) {
        vec![*a]
    } else {
        let (a_start, a_end) = *a;
        let (b_start, b_end) = *b;
        if a_start < b_start && a_end > b_end {
            vec![(a_start, b_start), (b_end, a_end)]
        } else if a_start < b_start {
            vec![(a_start, b_start)]
        } else if a_end > b_end {
            vec![(b_end, a_end)]
        } else {
            vec![]
        }
    }
}

/// split input range by `split_by` range to one, two or three parts.
fn split_range_by(
    input_range: &(Timestamp, Timestamp),
    input_parts: &[usize],
    split_by: &(Timestamp, Timestamp),
    split_idx: usize,
) -> Vec<Action> {
    let mut ret = Vec::new();
    if is_overlapping(input_range, split_by) {
        let input_parts = input_parts.to_vec();
        let new_parts = {
            let mut new_parts = input_parts.clone();
            new_parts.push(split_idx);
            new_parts
        };

        ret.push(Action::Pop(*input_range));
        if let Some(intersection) = range_intersection(input_range, split_by) {
            ret.push(Action::Push(intersection, new_parts.clone()));
        }
        for diff in range_difference(input_range, split_by) {
            ret.push(Action::Push(diff, input_parts.clone()));
        }
    }
    ret
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Action {
    Pop((Timestamp, Timestamp)),
    Push((Timestamp, Timestamp), Vec<usize>),
}

/// Compute all working ranges and corrseponding working sets from given `overlap_counts` computed from `split_overlapping_ranges`
///
/// TODO: test this
pub fn compute_all_working_ranges(
    overlap_counts: &BTreeMap<(Timestamp, Timestamp), Vec<usize>>,
) -> Vec<((Timestamp, Timestamp), BTreeSet<usize>)> {
    let mut ret = Vec::new();
    let mut cur_range_set: Option<((Timestamp, Timestamp), BTreeSet<usize>)> = None;
    for (range, set) in overlap_counts.iter() {
        match &mut cur_range_set {
            None => cur_range_set = Some((*range, BTreeSet::from_iter(set.iter().cloned()))),
            Some((cur_range, cur_set)) => {
                // if next overlap range have Partition tha's is in `cur_set` but not the last one in `cur_set`
                // we have to expand current working range to cover it(and add it's `set` to `cur_set`)
                // so that merge sort is possible
                let last_part = cur_set.last();
                let need_expand = set
                    .iter()
                    .any(|new_part| cur_set.contains(new_part) && last_part != Some(new_part));

                if need_expand {
                    cur_range.1 = range.1;
                    cur_set.extend(set.iter().cloned());
                } else {
                    ret.push((*cur_range, std::mem::take(cur_set)));
                }
            }
        }
    }

    ret
}

/// return a map of non-overlapping ranges and their corresponding index
/// (not `PartitionRange.identifier` but position in array) in the input `PartitionRange`s that is in those ranges
pub fn split_overlapping_ranges(
    ranges: &[PartitionRange],
) -> BTreeMap<(Timestamp, Timestamp), Vec<usize>> {
    // invariant: the key ranges should not overlapping with each other by definition from `is_overlapping`
    let mut ret: BTreeMap<(Timestamp, Timestamp), Vec<usize>> = BTreeMap::new();
    for (idx, range) in ranges.iter().enumerate() {
        let key = (range.start, range.end);
        let mut actions = Vec::new();
        let mut untouched = vec![key];
        // create a forward and backward iterator to find all overlapping ranges
        // given that tuple is sorted in lexicographical order and promise to not overlap,
        // since range is sorted that way, we can stop when we find a non-overlapping range
        let forward_iter = ret
            .range(key..)
            .take_while(|(range, _)| is_overlapping(range, &key));
        let backward_iter = ret
            .range(..key)
            .rev()
            .take_while(|(range, _)| is_overlapping(range, &key));

        for (range, parts) in forward_iter.chain(backward_iter) {
            untouched = untouched
                .iter()
                .flat_map(|r| range_difference(r, range))
                .collect();
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

/// Find all exist timestamps from given ranges
pub fn find_all_exist_timestamps(ranges: &[PartitionRange]) -> BTreeSet<Timestamp> {
    ranges
        .iter()
        .flat_map(|p| [p.start, p.end].into_iter())
        .collect()
}

/// Check if the input ranges's lower bound is monotonic.
pub fn check_lower_bound_monotonicity(ranges: &[PartitionRange]) -> bool {
    if ranges.is_empty() {
        return true;
    }
    ranges.windows(2).all(|w| w[0].start <= w[1].start)
}

#[cfg(test)]
mod test {
    use chrono::format;
    use pretty_assertions::assert_eq;

    use super::*;
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
                is_overlapping(range1, range2),
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
                    Action::Pop((Timestamp::new_second(1), Timestamp::new_millisecond(1001))),
                    Action::Push(
                        (Timestamp::new_second(1), Timestamp::new_millisecond(1001)),
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
                    Action::Pop((Timestamp::new_second(1), Timestamp::new_millisecond(1001))),
                    Action::Push(
                        (Timestamp::new_second(1), Timestamp::new_millisecond(1001)),
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
                    Action::Pop((Timestamp::new_second(1), Timestamp::new_millisecond(1001))),
                    Action::Push(
                        (
                            Timestamp::new_millisecond(1000),
                            Timestamp::new_millisecond(1001),
                        ),
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
                    Action::Pop((Timestamp::new_second(1), Timestamp::new_millisecond(1002))),
                    Action::Push(
                        (
                            Timestamp::new_millisecond(1001),
                            Timestamp::new_millisecond(1002),
                        ),
                        vec![0, 1],
                    ),
                    Action::Push(
                        (Timestamp::new_second(1), Timestamp::new_millisecond(1001)),
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
                    Action::Pop((Timestamp::new_second(1), Timestamp::new_millisecond(1004))),
                    Action::Push(
                        (
                            Timestamp::new_millisecond(1001),
                            Timestamp::new_millisecond(1002),
                        ),
                        vec![0, 1],
                    ),
                    Action::Push(
                        (Timestamp::new_second(1), Timestamp::new_millisecond(1001)),
                        vec![0],
                    ),
                    Action::Push(
                        (
                            Timestamp::new_millisecond(1002),
                            Timestamp::new_millisecond(1004),
                        ),
                        vec![0],
                    ),
                ],
            ),
        ];

        for (range, parts, split_by, split_idx, expected) in testcases.iter() {
            assert_eq!(
                split_range_by(range, parts, split_by, *split_idx),
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
            assert_eq!(split_overlapping_ranges(&input), expected);
        }
    }

    #[test]
    fn test_find_successive_runs() {
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
        let testcases = vec![
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
                vec![Some(2), Some(1), None, Some(3)],
                Some(SortOptions {
                    descending: true,
                    nulls_first: true,
                }),
                vec![(0, 2, Some(2), Some(1)), (2, 2, Some(3), Some(3))],
            ),
        ];
        for (input, sort_opts, expected) in testcases {
            let ret = find_successive_runs(input.into_iter().enumerate(), &sort_opts);
            let expected = expected.into_iter().map(SucRun::<i32>::from).collect_vec();
            assert_eq!(ret, expected);
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
}
