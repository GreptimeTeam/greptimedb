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

use std::collections::{BTreeMap, BTreeSet};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::array::{
    Array, ArrayRef, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray,
};
use arrow::compute::SortColumn;
use arrow_schema::{DataType, SortOptions, TimeUnit};
use async_stream::stream;
use common_error::ext::{BoxedError, PlainError};
use common_error::status_code::StatusCode;
use common_recordbatch::{DfRecordBatch, DfSendableRecordBatchStream, SendableRecordBatchStream};
use common_time::Timestamp;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::sorts::streaming_merge::streaming_merge;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::{internal_err, DataFusionError, ScalarValue};
use datafusion_physical_expr::PhysicalSortExpr;
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
    /// The input ranges indicate input stream will be composed of those ranges in given order
    ranges: Vec<PartitionRange>,
    /// Overlapping Timestamp Ranges'index given the input ranges
    ///
    /// note the key ranges here should not overlapping with each other
    overlap_counts: BTreeMap<(Timestamp, Timestamp), Vec<usize>>,
    /// record all existing timestamps in the input `ranges`
    all_exist_timestamps: BTreeSet<Timestamp>,
    input: Arc<dyn ExecutionPlan>,
}

impl WindowedSortExec {
    pub fn try_new(
        expression: PhysicalSortExpr,
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
            ranges,
            overlap_counts,
            all_exist_timestamps,
            input,
        })
    }

    /// During receiving partial-sorted RecordBatch, we need to update the working set which is the
    /// `PartitionRange` we think those RecordBatch belongs to. And when we receive something outside of working set, we can merge results before whenever possible.
    pub fn to_stream(
        &self,
        context: Arc<TaskContext>,
        partition: usize,
    ) -> datafusion_common::Result<SendableRecordBatchStream> {
        if 0 != partition {
            return Err(DataFusionError::Internal(format!(
                "WindowedSortExec invalid partition {partition}"
            )));
        }

        let input_stream: DfSendableRecordBatchStream =
            self.input.execute(partition, context.clone())?;

        // we need to build batch using `BatchBuilder` thus need large memory reservation
        let reservation = MemoryConsumer::new(format!("WindowedSortExec[{partition}]"))
            .register(&context.runtime_env().memory_pool);

        todo!()
    }
}

pub struct WindowedSortStream {
    /// Accounts for memory used by buffered batches
    reservation: MemoryReservation,
    /// currently assembling RecordBatches, will be put to `sort_partition_rbs` when it's done
    in_progress: Vec<DfRecordBatch>,
    /// last value of the last RecordBatch in `in_progress`, use to found partial sorted run's boundary
    last_value: Option<ArrayRef>,
    /// Current working set of `PartitionRange` sorted RecordBatches
    sort_partition_rbs: Vec<DfSendableRecordBatchStream>,
    /// Merge-sorted result stream, should be polled to end before start a new merge sort again
    merge_stream: Option<DfSendableRecordBatchStream>,
    /// Current working set of `PartitionRange`, update when need to merge sort more PartitionRange
    current_working_set: BTreeSet<usize>,
    /// Current working set of `PartitionRange`'s timestamp range
    ///
    /// expand upper bound by checking if next range in `overlap_counts` contain at least two index and at least one of them are in `current_working_set`(Since one index is not enough to merge and can be emitted directly)
    current_range: Option<(Timestamp, Timestamp)>,
    /// input stream assumed reading in order of `PartitionRange`
    input: DfSendableRecordBatchStream,
    /// Physical sort expressions(that is, sort by timestamp)
    expression: PhysicalSortExpr,
    /// The input ranges indicate input stream will be composed of those ranges in given order
    ranges: Vec<PartitionRange>,
    /// Overlapping Timestamp Ranges'index given the input ranges
    ///
    /// note the key ranges here should not overlapping with each other
    overlap_counts: BTreeMap<(Timestamp, Timestamp), Vec<usize>>,
}

impl WindowedSortStream {
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
                // split input rb to sorted runs
                let sort_column = self.expression.evaluate_to_sort_column(&batch)?;
                let sorted_runs_offset = get_sorted_runs(sort_column.clone())?;
                if sorted_runs_offset.first() == Some(&(0, batch.num_rows())) {
                    // input rb is already sorted, we can emit it directly
                    Some((vec![batch], sort_column))
                } else {
                    // those slice should be zero copy, so supposely no new reservation needed
                    let mut ret = Vec::new();
                    for (offset, len) in sorted_runs_offset {
                        let new_rb = batch.slice(offset, len);
                        ret.push(new_rb);
                    }
                    Some((ret, sort_column))
                }
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

        match (&mut self.last_value, new_input_rbs) {
            (Some(last_value), Some((new_rb, sort_column))) => {
                self.last_value = Some(sort_column.values.slice(sort_column.values.len(), 1));
                todo!("compare with last_value to find boundary");
            }
            (None, Some((new_rb, sort_column))) => {
                self.last_value = Some(sort_column.values.slice(sort_column.values.len(), 1));
            }
            (_, None) => {
                todo!("Input complete, merge sort the rest of the working set");
            }
        }

        todo!()
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

fn cmp_two_val_in_arr(
    a: &ArrayRef,
    a_offset: usize,
    b: &ArrayRef,
    b_offset: usize,
    opts: Option<SortOptions>,
) -> datafusion_common::Result<std::cmp::Ordering> {
    let time_unit = if a.data_type() == b.data_type() {
        if let DataType::Timestamp(unit, _) = a.data_type() {
            unit
        } else {
            return Err(DataFusionError::Internal(format!(
                "Unsupported sort column type: {} and {}",
                a.data_type(),
                b.data_type()
            )));
        }
    } else {
        return Err(DataFusionError::Internal(format!(
            "Unsupported sort column type: {} and {}",
            a.data_type(),
            b.data_type()
        )));
    };

    let a = a.slice(a_offset, 1);
    let b = b.slice(b_offset, 1);
    let mut ret = Vec::new();
    for arr in [a, b] {
        let mut iter = cast_as_ts_array_iter!(
            time_unit, arr,
            TimeUnit::Second => TimestampSecondArray,
            TimeUnit::Millisecond => TimestampMillisecondArray,
            TimeUnit::Microsecond => TimestampMicrosecondArray,
            TimeUnit::Nanosecond => TimestampNanosecondArray
        );
        let (_idx, val) = iter.next().ok_or_else(|| {
            DataFusionError::Internal("Empty array in cmp_two_val_in_arr".to_string())
        })?;
        ret.push(val);
    }
    if ret.len() != 2 {
        return Err(DataFusionError::Internal(
            "cmp_two_val_in_arr should return two values".to_string(),
        ));
    }
    let res = cmp_with_opts(&ret[0], &ret[1], &opts);
    Ok(res)
}

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

/// find all successive runs in the input iterator
fn find_successive_runs<T: Iterator<Item = (usize, Option<N>)>, N: Ord>(
    iter: T,
    sort_opts: &Option<SortOptions>,
) -> Vec<(usize, usize)> {
    let mut runs = Vec::new();
    let mut last_value = None;
    let mut last_offset = 0;
    let mut len = 0;
    for (idx, t) in iter {
        if let Some(last_value) = &last_value {
            if cmp_with_opts(last_value, &t, sort_opts) == std::cmp::Ordering::Greater {
                // we found a boundary
                let len = idx - last_offset;
                runs.push((last_offset, len));
                last_offset = idx;
            }
        }
        last_value = Some(t);
        len = idx;
    }

    runs.push((last_offset, len - last_offset + 1));

    runs
}

/// return a list of non-overlaping (offset, length) which represent sorted runs, and
/// can be used to call [`DfRecordBatch::slice`] to get sorted runs
fn get_sorted_runs(sort_column: SortColumn) -> datafusion_common::Result<Vec<(usize, usize)>> {
    let ty = sort_column.values.data_type();
    if let DataType::Timestamp(unit, _) = ty {
        let iter = cast_as_ts_array_iter!(
            unit, sort_column.values,
            TimeUnit::Second => TimestampSecondArray,
            TimeUnit::Millisecond => TimestampMillisecondArray,
            TimeUnit::Microsecond => TimestampMicrosecondArray,
            TimeUnit::Nanosecond => TimestampNanosecondArray
        );
        Ok(find_successive_runs(iter, &sort_column.options))
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

/// return a map of non-overlapping ranges and their corresponding index(not PartitionRange.identifier) in the input ranges
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
        let testcases = vec![
            (
                vec![Some(1), Some(2), Some(1), Some(3)],
                Some(SortOptions {
                    descending: false,
                    nulls_first: false,
                }),
                vec![(0, 2), (2, 2)],
            ),
            (
                vec![Some(1), Some(2), Some(1), Some(3)],
                Some(SortOptions {
                    descending: true,
                    nulls_first: false,
                }),
                vec![(0, 1), (1, 2), (3, 1)],
            ),
            (
                vec![Some(1), Some(2), None, Some(3)],
                Some(SortOptions {
                    descending: false,
                    nulls_first: true,
                }),
                vec![(0, 2), (2, 2)],
            ),
            (
                vec![Some(2), Some(1), None, Some(3)],
                Some(SortOptions {
                    descending: true,
                    nulls_first: true,
                }),
                vec![(0, 2), (2, 2)],
            ),
        ];
        for (input, sort_opts, expected) in testcases {
            let ret = find_successive_runs(input.into_iter().enumerate(), &sort_opts);
            assert_eq!(ret, expected);
        }
    }

    #[test]
    fn test_cmp_with_opts() {
        let testcases = vec![(
            Some(1),
            Some(2),
            Some(SortOptions {
                descending: false,
                nulls_first: false,
            }),
            std::cmp::Ordering::Less,
        )];
    }
}
