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

use std::sync::Arc;

use datafusion::arrow::array::{Float64Array, TimestampMillisecondArray};
use datafusion::common::DataFusionError;
use datafusion::physical_plan::ColumnarValue;
use datatypes::arrow::array::Array;

use crate::functions::extract_range_array;
use crate::functions::rolling::{Layout, Window, classify, transition, window_from_raw};
use crate::range_array::RangeArray;

pub(crate) fn calc(input: &[ColumnarValue], name: &str) -> Result<ColumnarValue, DataFusionError> {
    let timestamps = extract_range_array(&input[0])?;
    let values = extract_range_array(&input[1])?;
    if timestamps.len() != values.len() {
        return Err(DataFusionError::Execution(format!(
            "RangeArray have different lengths in PromQL function {name}: array1={}, array2={}",
            timestamps.len(),
            values.len()
        )));
    }

    let timestamp_backing = timestamps
        .values()
        .as_any()
        .downcast_ref::<TimestampMillisecondArray>()
        .unwrap();
    let value_backing = values
        .values()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    let prepared = prepare_windows(
        &timestamps,
        &values,
        timestamp_backing.len(),
        value_backing.len(),
        name,
    )?;

    let result = match prepared.layout {
        Layout::Sliding => evaluate_sliding(&prepared.value_windows),
        Layout::Arbitrary => evaluate_direct(&prepared.value_windows),
    };
    Ok(ColumnarValue::Array(Arc::new(Float64Array::from_iter(
        result,
    ))))
}

#[derive(Debug)]
struct PreparedWindows {
    layout: Layout,
    value_windows: Vec<Window>,
}

fn prepare_windows(
    timestamps: &RangeArray,
    values: &RangeArray,
    timestamp_backing_len: usize,
    value_backing_len: usize,
    name: &str,
) -> Result<PreparedWindows, DataFusionError> {
    let mut value_windows = Vec::with_capacity(values.len());
    let mut timestamp_ranges = timestamps.ranges();
    let mut value_ranges = values.ranges();
    for index in 0..values.len() {
        let (timestamp_offset, timestamp_len) =
            timestamp_ranges.next().flatten().ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "RangeArray's element {index} is unavailable in PromQL function {name}"
                ))
            })?;
        let timestamp_window = window_from_raw(
            timestamp_offset,
            timestamp_len,
            timestamp_backing_len,
            index,
            name,
        )?;
        let (value_offset, value_len) = value_ranges.next().flatten().ok_or_else(|| {
            DataFusionError::Execution(format!(
                "RangeArray's element {index} is unavailable in PromQL function {name}"
            ))
        })?;
        let value_window =
            window_from_raw(value_offset, value_len, value_backing_len, index, name)?;
        if timestamp_window.len() != value_window.len() {
            return Err(DataFusionError::Execution(format!(
                "RangeArray's element {index} have different lengths in PromQL function {name}: array1={}, array2={}",
                timestamp_window.len(),
                value_window.len()
            )));
        }
        value_windows.push(value_window);
    }

    Ok(PreparedWindows {
        layout: classify(&value_windows),
        value_windows,
    })
}

fn evaluate_sliding(windows: &[Window]) -> Vec<Option<f64>> {
    let Some((&first, rest)) = windows.split_first() else {
        return Vec::new();
    };

    let mut count = first.len();
    let mut result = Vec::with_capacity(windows.len());
    result.push(count_result(count));
    let mut previous = first;
    for &current in rest {
        let delta = transition(previous, current);
        count -= delta.removed.len();
        count += delta.added.len();
        result.push(count_result(count));
        previous = current;
    }
    result
}

fn evaluate_direct(windows: &[Window]) -> Vec<Option<f64>> {
    windows
        .iter()
        .map(|window| count_result(window.len()))
        .collect()
}

fn count_result(len: usize) -> Option<f64> {
    (len != 0).then_some(len as f64)
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use datafusion::arrow::array::{Float64Array, TimestampMillisecondArray};
    use datafusion::physical_plan::ColumnarValue;

    use super::*;

    fn range_arrays(
        timestamp_ranges: &[(u32, u32)],
        value_ranges: &[(u32, u32)],
    ) -> (RangeArray, RangeArray) {
        let timestamps = Arc::new(TimestampMillisecondArray::from_iter(
            (0..64).map(|value| Some(value as i64)),
        ));
        let values = Arc::new(Float64Array::from_iter((0..64).map(|value| value as f64)));
        (
            RangeArray::from_ranges(timestamps, timestamp_ranges.iter().copied()).unwrap(),
            RangeArray::from_ranges(values, value_ranges.iter().copied()).unwrap(),
        )
    }

    fn run(timestamp_ranges: &[(u32, u32)], value_ranges: &[(u32, u32)]) -> Float64Array {
        let (timestamps, values) = range_arrays(timestamp_ranges, value_ranges);
        let input = [
            ColumnarValue::Array(Arc::new(timestamps.into_dict())),
            ColumnarValue::Array(Arc::new(values.into_dict())),
        ];
        let result = calc(&input, "prom_count_over_time").unwrap();
        let ColumnarValue::Array(result) = result else {
            panic!("count_over_time must return an array");
        };
        result
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .clone()
    }

    fn oracle(value_ranges: &[(u32, u32)]) -> Vec<Option<f64>> {
        value_ranges
            .iter()
            .map(|&(_, len)| (len != 0).then_some(f64::from(len)))
            .collect()
    }

    fn assert_exact(actual: &Float64Array, expected: &[Option<f64>]) {
        assert_eq!(actual.len(), expected.len());
        for (index, expected) in expected.iter().enumerate() {
            match expected {
                Some(expected) => {
                    assert!(actual.is_valid(index));
                    assert_eq!(actual.value(index).to_bits(), expected.to_bits());
                }
                None => assert!(!actual.is_valid(index)),
            }
        }
    }

    #[test]
    fn applies_sliding_transitions_for_repeated_disjoint_and_empty_windows() {
        let ranges = &[(1, 3), (1, 3), (4, 2), (6, 0), (6, 2), (9, 2)];
        assert_exact(&run(ranges, ranges), &oracle(ranges));
    }

    #[test]
    fn falls_back_for_an_incompatible_suffix_after_a_compatible_prefix() {
        let ranges = &[(1, 3), (2, 3), (0, 2), (4, 1)];
        let (timestamps, values) = range_arrays(ranges, ranges);
        let prepared = prepare_windows(
            &timestamps,
            &values,
            timestamps.values().len(),
            values.values().len(),
            "prom_count_over_time",
        )
        .unwrap();
        assert_eq!(prepared.layout, Layout::Arbitrary);
        assert_exact(&run(ranges, ranges), &oracle(ranges));
    }

    #[test]
    fn prevalidates_all_rows_and_reports_the_first_mismatch() {
        let timestamp_ranges = &[(0, 2), (1, 2), (4, 3), (8, 2)];
        let value_ranges = &[(0, 2), (1, 2), (4, 2), (8, 1)];
        let (timestamps, values) = range_arrays(timestamp_ranges, value_ranges);
        let error = prepare_windows(
            &timestamps,
            &values,
            timestamps.values().len(),
            values.values().len(),
            "prom_count_over_time",
        )
        .unwrap_err();
        match error {
            DataFusionError::Execution(message) => assert_eq!(
                message,
                "RangeArray's element 2 have different lengths in PromQL function prom_count_over_time: array1=3, array2=2"
            ),
            other => panic!("expected execution error, got {other:?}"),
        }
    }

    struct Lcg(u64);

    impl Lcg {
        fn next(&mut self) -> u32 {
            self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1);
            (self.0 >> 32) as u32
        }
    }

    #[test]
    fn seeded_layout_differential_matches_the_raw_tuple_oracle() {
        let mut lcg = Lcg(0x5eed_c0de);
        let mut monotone = Vec::new();
        let mut left = 0u32;
        let mut right = 0u32;
        for _ in 0..48 {
            left = left.saturating_add(lcg.next() % 2);
            right = right.max(left).saturating_add(lcg.next() % 3);
            right = right.min(63);
            left = left.min(right);
            monotone.push((left, right - left));
        }
        assert_exact(&run(&monotone, &monotone), &oracle(&monotone));

        let arbitrary = (0..48)
            .map(|_| {
                let left = lcg.next() % 63;
                (left, lcg.next() % (64 - left))
            })
            .collect::<Vec<_>>();
        assert_exact(&run(&arbitrary, &arbitrary), &oracle(&arbitrary));
    }
}
