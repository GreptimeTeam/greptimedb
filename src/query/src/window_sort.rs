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
use std::sync::Arc;

use common_error::ext::{BoxedError, PlainError};
use common_error::status_code::StatusCode;
use common_time::Timestamp;
use datafusion::physical_plan::ExecutionPlan;
use snafu::ResultExt;
use store_api::region_engine::PartitionRange;

use crate::error::{QueryExecutionSnafu, Result};

/// A complex stream sort execution plan which accepts a list of `PartitionRange` and
/// merge sort them whenever possible, and emit the sorted result as soon as possible.
#[derive(Debug, Clone)]
pub struct WindowedSortExec {
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
    pub fn try_new(ranges: Vec<PartitionRange>, input: Arc<dyn ExecutionPlan>) -> Result<Self> {
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
            ranges,
            overlap_counts,
            all_exist_timestamps,
            input,
        })
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

fn split_range_by(
    input_range: &(Timestamp, Timestamp),
    input_parts: &[usize],
    split_by: &(Timestamp, Timestamp),
    split_idx: usize,
) -> Vec<Action> {
    let mut ret = Vec::new();
    if is_overlapping(input_range, split_by) {
        let (i_start, i_end) = *input_range;
        let (s_start, s_end) = *split_by;
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

pub fn split_overlapping_ranges(
    ranges: &[PartitionRange],
) -> BTreeMap<(Timestamp, Timestamp), Vec<usize>> {
    let mut ret: BTreeMap<(Timestamp, Timestamp), Vec<usize>> = BTreeMap::new();
    for (idx, range) in ranges.iter().enumerate() {
        let key = (range.start, range.end);
        let next = ret.range(key..).next();
        let prev = ret.range(..=key).next_back();
        let mut actions = Vec::new();
        for (range, parts) in [next, prev].into_iter().flatten() {
            actions.extend(split_range_by(range, parts, &key, idx));
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
            (
                (Timestamp::new_second(1), Timestamp::new_millisecond(1001)),
                vec![0],
                (Timestamp::new_second(0), Timestamp::new_millisecond(1)),
                1,
                vec![],
            ),
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
}
