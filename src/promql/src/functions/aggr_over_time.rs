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

use std::collections::VecDeque;
use std::ops::Range;
use std::sync::Arc;

use common_macro::range_fn;
use datafusion::arrow::array::{Float64Array, TimestampMillisecondArray};
use datafusion::common::DataFusionError;
use datafusion::logical_expr::{ScalarUDF, Volatility};
use datafusion::physical_plan::ColumnarValue;
use datatypes::arrow::array::Array;
use datatypes::arrow::compute;
use datatypes::arrow::datatypes::{DataType, TimeUnit};

use crate::functions::{compensated_sum_inc, extract_array, extract_range_dict};
use crate::range_array::{RangeArray, unpack};

#[derive(Clone, Copy)]
enum PresenceEvaluator {
    Count,
    Last,
    Absent,
    Present,
}

fn count_over_time_evaluator(
    input: &[ColumnarValue],
    name: &str,
) -> Result<ColumnarValue, DataFusionError> {
    evaluate_presence(input, name, PresenceEvaluator::Count)
}

fn last_over_time_evaluator(
    input: &[ColumnarValue],
    name: &str,
) -> Result<ColumnarValue, DataFusionError> {
    evaluate_presence(input, name, PresenceEvaluator::Last)
}

fn absent_over_time_evaluator(
    input: &[ColumnarValue],
    name: &str,
) -> Result<ColumnarValue, DataFusionError> {
    evaluate_presence(input, name, PresenceEvaluator::Absent)
}

fn present_over_time_evaluator(
    input: &[ColumnarValue],
    name: &str,
) -> Result<ColumnarValue, DataFusionError> {
    evaluate_presence(input, name, PresenceEvaluator::Present)
}

fn evaluate_presence(
    input: &[ColumnarValue],
    name: &str,
    operation: PresenceEvaluator,
) -> Result<ColumnarValue, DataFusionError> {
    assert_eq!(input.len(), 2);

    let timestamp_ranges = RangeArray::try_new(extract_array(&input[0])?.to_data().into())?;
    let value_ranges = RangeArray::try_new(extract_array(&input[1])?.to_data().into())?;
    let len = timestamp_ranges.len();
    if len != value_ranges.len() {
        return Err(DataFusionError::Execution(format!(
            "RangeArray have different lengths in PromQL function {name}: array1={len}, array2={}",
            value_ranges.len()
        )));
    }

    if timestamp_ranges.is_empty() {
        return Ok(ColumnarValue::Array(Arc::new(Float64Array::from_iter(
            std::iter::empty::<Option<f64>>(),
        ))));
    }

    // The generic range-function wrapper downcasts both arrays for each window.
    // Do it once after retaining its zero-window behavior above.
    assert!(
        timestamp_ranges
            .values()
            .as_any()
            .is::<TimestampMillisecondArray>()
    );
    let values = value_ranges
        .values()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    // Windows overlap heavily, so locate every sample once and let each window binary-search
    // its own slice of the index instead of rescanning the slots it shares with its neighbours.
    let has_nulls = values.null_count() != 0;
    let valid_positions: Vec<usize> = if has_nulls && matches!(operation, PresenceEvaluator::Count)
    {
        (0..values.len())
            .filter(|&index| values.is_valid(index))
            .collect()
    } else {
        Vec::new()
    };
    let evaluator: fn(&Float64Array, &[usize], usize, usize) -> Option<f64> = if has_nulls {
        match operation {
            PresenceEvaluator::Count => |_, samples, offset, length| {
                let count = valid_slot_bounds(samples, offset, offset + length).len();
                (count != 0).then_some(count as f64)
            },
            PresenceEvaluator::Last => |values, _, offset, length| {
                (offset..offset + length)
                    .rev()
                    .find(|&index| values.is_valid(index))
                    .map(|index| values.value(index))
            },
            PresenceEvaluator::Absent => |values, _, offset, length| {
                (!(offset..offset + length).any(|index| values.is_valid(index))).then_some(1.0)
            },
            PresenceEvaluator::Present => |values, _, offset, length| {
                (offset..offset + length)
                    .any(|index| values.is_valid(index))
                    .then_some(1.0)
            },
        }
    } else {
        match operation {
            PresenceEvaluator::Count => |_, _, _, length| (length != 0).then_some(length as f64),
            PresenceEvaluator::Last => {
                |values, _, offset, length| (length != 0).then(|| values.value(offset + length - 1))
            }
            PresenceEvaluator::Absent => |_, _, _, length| (length == 0).then_some(1.0),
            PresenceEvaluator::Present => |_, _, _, length| (length != 0).then_some(1.0),
        }
    };

    let mut result = Vec::with_capacity(len);
    for index in 0..len {
        let (_, timestamp_length) = timestamp_ranges.get_offset_length(index).unwrap();
        let (value_offset, value_length) = value_ranges.get_offset_length(index).unwrap();
        if timestamp_length != value_length {
            return Err(DataFusionError::Execution(format!(
                "RangeArray's element {index} have different lengths in PromQL function {name}: array1={timestamp_length}, array2={value_length}"
            )));
        }
        result.push(evaluator(
            values,
            &valid_positions,
            value_offset,
            value_length,
        ));
    }

    Ok(ColumnarValue::Array(Arc::new(Float64Array::from_iter(
        result,
    ))))
}

/// The slice of `valid_positions` holding the samples inside `[offset, end)`: the positions are
/// ascending, so it is the half-open range between the first one at or past `offset` and the
/// first one at or past `end`. Empty when the window holds no sample.
fn valid_slot_bounds(valid_positions: &[usize], offset: usize, end: usize) -> Range<usize> {
    let lo = valid_positions.partition_point(|&index| index < offset);
    let hi = valid_positions.partition_point(|&index| index < end);
    lo..hi
}

/// The average value of all points in the specified interval.
#[range_fn(
    name = AvgOverTime,
    ret = Float64Array,
    display_name = prom_avg_over_time
)]
pub fn avg_over_time(_: &TimestampMillisecondArray, values: &Float64Array) -> Option<f64> {
    // `sum` already skips null slots and yields `None` for an all-null window, so only the
    // divisor needs to count samples instead of slots.
    let sample_count = values.len() - values.null_count();
    compute::sum(values).map(|result| result / sample_count as f64)
}

/// The minimum value of all points in the specified interval.
#[derive(Debug)]
pub struct MinOverTime {}

impl MinOverTime {
    pub const fn name() -> &'static str {
        "prom_min_over_time"
    }

    pub fn scalar_udf() -> ScalarUDF {
        datafusion_expr::create_udf(
            Self::name(),
            Self::input_type(),
            Self::return_type(),
            Volatility::Volatile,
            Arc::new(Self::calc) as _,
        )
    }

    fn input_type() -> Vec<DataType> {
        min_max_input_type()
    }

    fn return_type() -> DataType {
        Float64Array::new_null(0).data_type().clone()
    }

    fn calc(input: &[ColumnarValue]) -> Result<ColumnarValue, DataFusionError> {
        min_max_over_time_batch(input, true, Self::name())
    }
}

/// The maximum value of all points in the specified interval.
#[derive(Debug)]
pub struct MaxOverTime {}

impl MaxOverTime {
    pub const fn name() -> &'static str {
        "prom_max_over_time"
    }

    pub fn scalar_udf() -> ScalarUDF {
        datafusion_expr::create_udf(
            Self::name(),
            Self::input_type(),
            Self::return_type(),
            Volatility::Volatile,
            Arc::new(Self::calc) as _,
        )
    }

    fn input_type() -> Vec<DataType> {
        min_max_input_type()
    }

    fn return_type() -> DataType {
        Float64Array::new_null(0).data_type().clone()
    }

    fn calc(input: &[ColumnarValue]) -> Result<ColumnarValue, DataFusionError> {
        min_max_over_time_batch(input, false, Self::name())
    }
}

fn min_max_input_type() -> Vec<DataType> {
    vec![
        RangeArray::convert_data_type(DataType::Timestamp(TimeUnit::Millisecond, None)),
        RangeArray::convert_data_type(DataType::Float64),
    ]
}

/// Batches with fewer windows than this have too little repeated work to reclaim.
const MIN_SLIDING_WINDOWS: usize = 4;
/// Below this average window length the per-sample bookkeeping is comparable to the
/// scan it would replace.
const MIN_SLIDING_WINDOW_LENGTH: u64 = 32;
/// Reuse is taken only when a step advances at most this fraction of the window.
const MAX_SLIDING_STEP_FRACTION: u64 = 4;

/// Whether reusing candidates across windows is expected to beat rescanning each one.
///
/// Reuse pays off in proportion to how much consecutive windows overlap, and loses to
/// a plain scan on wide windows that barely overlap: maintaining the deque then costs
/// more than the rescan it replaces.
///
/// The shape is read from the whole batch rather than from its leading windows.
/// `RangeManipulate` holds the window duration and the evaluation step fixed, but the
/// sample counts still vary: a series that starts inside the query range gets a first
/// window covering roughly one step. Averages survive that; the first two windows do not.
///
/// A wrong answer costs time, not correctness — both evaluators return the same bits.
fn reuses_candidates(window_keys: &[i64]) -> bool {
    if window_keys.len() < MIN_SLIDING_WINDOWS {
        return false;
    }

    let windows = window_keys.len() as u64;
    let mut total_length = 0u64;
    let mut lowest_offset = u32::MAX;
    let mut highest_offset = 0u32;
    for &key in window_keys {
        let (offset, length) = unpack(key);
        // `RangeManipulate` emits a window covering no sample as `(0, 0)`, including
        // the trailing one a query gets when its last evaluation lands exactly one
        // window past the last sample. Its offset says nothing about the batch.
        if length == 0 {
            continue;
        }
        total_length += u64::from(length);
        lowest_offset = lowest_offset.min(offset);
        highest_offset = highest_offset.max(offset);
    }
    // What reuse skips re-reading: the distance the left bound travels over the batch.
    let total_advance = u64::from(highest_offset.saturating_sub(lowest_offset));

    // `total_length / windows >= MIN_SLIDING_WINDOW_LENGTH` and
    // `total_advance / (windows - 1) <= (total_length / windows) / MAX_SLIDING_STEP_FRACTION`,
    // cross-multiplied to keep the averages exact. Empty windows stay in the window
    // count, which only makes both conditions stricter.
    total_length >= windows * MIN_SLIDING_WINDOW_LENGTH
        && total_advance * MAX_SLIDING_STEP_FRACTION * windows <= total_length * (windows - 1)
}

fn is_better(value: f64, current: f64, is_min: bool) -> bool {
    if is_min {
        value < current
    } else {
        value > current
    }
}

fn min_max_over_time_batch(
    input: &[ColumnarValue],
    is_min: bool,
    func_name: &str,
) -> Result<ColumnarValue, DataFusionError> {
    if input.len() != 2 {
        return Err(DataFusionError::Execution(format!(
            "{func_name}: expected 2 inputs, found {}",
            input.len()
        )));
    }

    let timestamps = extract_range_dict(
        &input[0],
        func_name,
        "timestamp range vector",
        &DataType::Timestamp(TimeUnit::Millisecond, None),
    )?;
    let values = extract_range_dict(
        &input[1],
        func_name,
        "value range vector",
        &DataType::Float64,
    )?;

    let timestamp_keys = timestamps.keys().values();
    let value_keys = values.keys().values();
    if timestamp_keys.len() != value_keys.len() {
        return Err(DataFusionError::Execution(format!(
            "{func_name}: timestamp and value ranges should have the same number of windows, found {} and {}",
            timestamp_keys.len(),
            value_keys.len()
        )));
    }

    let values = values
        .values()
        .as_any()
        .downcast_ref::<Float64Array>()
        .ok_or_else(|| {
            DataFusionError::Execution(format!(
                "{func_name}: expect value range vector values of type Float64"
            ))
        })?;
    let mut sliding = reuses_candidates(value_keys).then(|| SlidingExtrema::new(is_min));
    let mut result = Vec::with_capacity(value_keys.len());

    for index in 0..value_keys.len() {
        let (_, timestamp_length) = unpack(timestamp_keys[index]);
        let (value_offset, value_length) = unpack(value_keys[index]);
        if timestamp_length != value_length {
            return Err(DataFusionError::Execution(format!(
                "{func_name}: timestamp and value ranges have different lengths at window {index}: {timestamp_length} and {value_length}"
            )));
        }

        let start = value_offset as usize;
        let end = start + value_length as usize;
        result.push(match sliding.as_mut() {
            Some(sliding) => sliding.evaluate(values, start, end),
            None => scan_extremum(values, start, end, is_min),
        });
    }

    Ok(ColumnarValue::Array(Arc::new(Float64Array::from_iter(
        result,
    ))))
}

/// Reuses extrema candidates across windows whose bounds do not retreat.
struct SlidingExtrema {
    is_min: bool,
    /// Indices of the samples that can still become the extremum, in arrival order.
    /// A strictly better sample evicts the ones queued before it, so the front is the
    /// extremum of the current window and tied values keep their arrival order — and
    /// with it the sign of tied zeros.
    candidates: VecDeque<usize>,
    /// Backs the all-NaN window, which keeps the last NaN of the window.
    latest_nan: Option<usize>,
    previous_window: Option<(usize, usize)>,
}

impl SlidingExtrema {
    fn new(is_min: bool) -> Self {
        Self {
            is_min,
            candidates: VecDeque::new(),
            latest_nan: None,
            previous_window: None,
        }
    }

    fn evaluate(&mut self, values: &Float64Array, start: usize, end: usize) -> Option<f64> {
        let append_start = match self.previous_window {
            Some((previous_start, previous_end))
                if start >= previous_start && end >= previous_end =>
            {
                while self.candidates.front().is_some_and(|&index| index < start) {
                    self.candidates.pop_front();
                }
                if self.latest_nan.is_some_and(|index| index < start) {
                    self.latest_nan = None;
                }
                // Samples between two disjoint windows belong to neither, and later
                // windows only move right, so skipping them keeps the state exact.
                previous_end.max(start)
            }
            // A retreating bound can bring back samples that are no longer tracked.
            Some(_) | None => {
                self.candidates.clear();
                self.latest_nan = None;
                start
            }
        };
        self.append(values, append_start, end);
        self.previous_window = Some((start, end));

        self.candidates
            .front()
            .or(self.latest_nan.as_ref())
            .map(|&index| values.value(index))
    }

    fn append(&mut self, values: &Float64Array, start: usize, end: usize) {
        for index in start..end {
            if values.is_null(index) {
                continue;
            }

            let value = values.value(index);
            if value.is_nan() {
                // Keep the latest payload while no non-NaN value is in the window.
                self.latest_nan = Some(index);
                continue;
            }

            while let Some(&tail_index) = self.candidates.back() {
                if !is_better(value, values.value(tail_index), self.is_min) {
                    break;
                }
                self.candidates.pop_back();
            }
            self.candidates.push_back(index);
        }
    }
}

/// Folds one window on its own, the way the per-window scan used to.
fn scan_extremum(values: &Float64Array, start: usize, end: usize, is_min: bool) -> Option<f64> {
    let mut extremum: Option<f64> = None;
    for index in start..end {
        if values.is_null(index) {
            continue;
        }

        let value = values.value(index);
        let replace = match extremum {
            None => true,
            // A NaN only holds the slot until any other sample arrives.
            Some(current) => current.is_nan() || is_better(value, current, is_min),
        };
        if replace {
            extremum = Some(value);
        }
    }
    extremum
}

#[cfg(test)]
fn min_over_time(_: &TimestampMillisecondArray, values: &Float64Array) -> Option<f64> {
    let mut valid_values = values.iter().flatten();
    let mut min = valid_values.next()?;
    for value in valid_values {
        if value < min || min.is_nan() {
            min = value;
        }
    }
    Some(min)
}

#[cfg(test)]
fn max_over_time(_: &TimestampMillisecondArray, values: &Float64Array) -> Option<f64> {
    let mut valid_values = values.iter().flatten();
    let mut max = valid_values.next()?;
    for value in valid_values {
        if value > max || max.is_nan() {
            max = value;
        }
    }
    Some(max)
}

/// The sum of all values in the specified interval.
#[range_fn(
    name = SumOverTime,
    ret = Float64Array,
    display_name = prom_sum_over_time
)]
pub fn sum_over_time(_: &TimestampMillisecondArray, values: &Float64Array) -> Option<f64> {
    compute::sum(values)
}

/// The count of all values in the specified interval.
#[range_fn(
    name = CountOverTime,
    ret = Float64Array,
    display_name = prom_count_over_time,
    evaluator = count_over_time_evaluator
)]
pub fn count_over_time(_: &TimestampMillisecondArray, values: &Float64Array) -> Option<f64> {
    let count = values.iter().flatten().count();
    (count != 0).then_some(count as f64)
}

/// The most recent point value in specified interval.
#[range_fn(
    name = LastOverTime,
    ret = Float64Array,
    display_name = prom_last_over_time,
    evaluator = last_over_time_evaluator
)]
pub fn last_over_time(_: &TimestampMillisecondArray, values: &Float64Array) -> Option<f64> {
    values.iter().flatten().last()
}

/// absent_over_time returns an empty vector if the range vector passed to it has any
/// elements (floats or native histograms) and a 1-element vector with the value 1 if
/// the range vector passed to it has no elements.
#[range_fn(
    name = AbsentOverTime,
    ret = Float64Array,
    display_name = prom_absent_over_time,
    evaluator = absent_over_time_evaluator
)]
pub fn absent_over_time(_: &TimestampMillisecondArray, values: &Float64Array) -> Option<f64> {
    values.iter().flatten().next().is_none().then_some(1.0)
}

/// the value 1 for any series in the specified interval.
#[range_fn(
    name = PresentOverTime,
    ret = Float64Array,
    display_name = prom_present_over_time,
    evaluator = present_over_time_evaluator
)]
pub fn present_over_time(_: &TimestampMillisecondArray, values: &Float64Array) -> Option<f64> {
    values.iter().flatten().next().is_some().then_some(1.0)
}

/// the population standard variance of the values in the specified interval.
/// DataFusion's implementation:
/// <https://github.com/apache/arrow-datafusion/blob/292eb954fc0bad3a1febc597233ba26cb60bda3e/datafusion/physical-expr/src/aggregate/variance.rs#L224-#L241>
#[range_fn(
    name = StdvarOverTime,
    ret = Float64Array,
    display_name = prom_stdvar_over_time
)]
pub fn stdvar_over_time(_: &TimestampMillisecondArray, values: &Float64Array) -> Option<f64> {
    let mut count = 0;
    let mut mean: f64 = 0.0;
    let mut result: f64 = 0.0;
    for value in values.iter().flatten() {
        let new_count = count + 1;
        let delta1 = value - mean;
        let new_mean = delta1 / new_count as f64 + mean;
        let delta2 = value - new_mean;
        let new_result = result + delta1 * delta2;

        count = new_count;
        mean = new_mean;
        result = new_result;
    }
    (count > 0).then(|| result / count as f64)
}

/// the population standard deviation of the values in the specified interval.
/// Prometheus's implementation: <https://github.com/prometheus/prometheus/blob/f55ab2217984770aa1eecd0f2d5f54580029b1c0/promql/functions.go#L556-L569>
#[range_fn(
    name = StddevOverTime,
    ret = Float64Array,
    display_name = prom_stddev_over_time
)]
pub fn stddev_over_time(_: &TimestampMillisecondArray, values: &Float64Array) -> Option<f64> {
    let mut count = 0.0;
    let mut mean = 0.0;
    let mut comp_mean = 0.0;
    let mut deviations_sum_sq = 0.0;
    let mut comp_deviations_sum_sq = 0.0;
    for current_value in values.iter().flatten() {
        count += 1.0;
        let delta = current_value - (mean + comp_mean);
        let (new_mean, new_comp_mean) = compensated_sum_inc(delta / count, mean, comp_mean);
        mean = new_mean;
        comp_mean = new_comp_mean;
        let (new_deviations_sum_sq, new_comp_deviations_sum_sq) = compensated_sum_inc(
            delta * (current_value - (mean + comp_mean)),
            deviations_sum_sq,
            comp_deviations_sum_sq,
        );
        deviations_sum_sq = new_deviations_sum_sq;
        comp_deviations_sum_sq = new_comp_deviations_sum_sq;
    }
    (count > 0.0).then(|| ((deviations_sum_sq + comp_deviations_sum_sq) / count).sqrt())
}

#[cfg(test)]
mod test {
    use datafusion::arrow::buffer::NullBuffer;

    use super::*;
    use crate::functions::test_util::{invoke_range_udf, simple_range_udf_runner};

    fn assert_option_bits(actual: &[Option<f64>], expected: &[Option<f64>]) {
        assert_eq!(actual.len(), expected.len());
        for (actual, expected) in actual.iter().zip(expected) {
            match (actual, expected) {
                (Some(actual), Some(expected)) => assert_eq!(actual.to_bits(), expected.to_bits()),
                (None, None) => {}
                (actual, expected) => panic!("expected {expected:?}, got {actual:?}"),
            }
        }
    }

    fn old_min_max_over_windows(
        values: &Float64Array,
        ranges: &[(u32, u32)],
        is_min: bool,
    ) -> Vec<Option<f64>> {
        ranges
            .iter()
            .map(|&(offset, length)| {
                let values = values.slice(offset as usize, length as usize);
                let values = values.as_any().downcast_ref::<Float64Array>().unwrap();
                let timestamps = TimestampMillisecondArray::new_null(values.len());
                if is_min {
                    min_over_time(&timestamps, values)
                } else {
                    max_over_time(&timestamps, values)
                }
            })
            .collect()
    }

    fn run_min_max_udf(
        udf: ScalarUDF,
        timestamps: RangeArray,
        values: RangeArray,
    ) -> Vec<Option<f64>> {
        let result = invoke_range_udf(udf, timestamps, values).unwrap();
        extract_array(&result)
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .iter()
            .collect()
    }

    fn slice_range_array(range: RangeArray, offset: usize, length: usize) -> RangeArray {
        RangeArray::try_new(range.into_dict().slice(offset, length).to_data().into()).unwrap()
    }

    fn sliced_float_values(values: Vec<Option<f64>>) -> Float64Array {
        let mut backing = vec![Some(1234.0)];
        backing.extend(values);
        let length = backing.len() - 1;
        let backing = Float64Array::from(backing);
        backing
            .slice(1, length)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .clone()
    }

    fn min_max_range_inputs(
        timestamps: &TimestampMillisecondArray,
        values: &Float64Array,
        timestamp_ranges: &[(u32, u32)],
        value_ranges: &[(u32, u32)],
    ) -> (RangeArray, RangeArray) {
        let timestamp_prefix = [(0, 1)];
        let value_prefix = [(0, 1)];
        let timestamps = RangeArray::from_ranges(
            Arc::new(timestamps.clone()),
            timestamp_prefix
                .into_iter()
                .chain(timestamp_ranges.iter().copied()),
        )
        .unwrap();
        let values = RangeArray::from_ranges(
            Arc::new(values.clone()),
            value_prefix.into_iter().chain(value_ranges.iter().copied()),
        )
        .unwrap();
        (
            slice_range_array(timestamps, 1, timestamp_ranges.len()),
            slice_range_array(values, 1, value_ranges.len()),
        )
    }

    fn window_keys(ranges: &[(u32, u32)]) -> Vec<i64> {
        let length = ranges
            .iter()
            .map(|&(offset, length)| (offset + length) as usize)
            .max()
            .unwrap_or_default();
        RangeArray::from_ranges(
            Arc::new(Float64Array::from(vec![0.0; length])),
            ranges.iter().copied(),
        )
        .unwrap()
        .into_dict()
        .keys()
        .values()
        .to_vec()
    }

    fn assert_min_max_udfs_match_oracle(
        timestamp_values: &TimestampMillisecondArray,
        all_values: &Float64Array,
        timestamp_ranges: &[(u32, u32)],
        value_ranges: &[(u32, u32)],
    ) {
        let expected_min = old_min_max_over_windows(all_values, value_ranges, true);
        let expected_max = old_min_max_over_windows(all_values, value_ranges, false);
        let (timestamps, values) =
            min_max_range_inputs(timestamp_values, all_values, timestamp_ranges, value_ranges);
        assert_option_bits(
            &run_min_max_udf(MinOverTime::scalar_udf(), timestamps, values),
            &expected_min,
        );
        let (timestamps, values) =
            min_max_range_inputs(timestamp_values, all_values, timestamp_ranges, value_ranges);
        assert_option_bits(
            &run_min_max_udf(MaxOverTime::scalar_udf(), timestamps, values),
            &expected_max,
        );
    }

    #[test]
    fn min_max_over_time_batch_preserves_bits_across_irregular_windows() {
        let first_nan = f64::from_bits(0x7ff8_0000_0000_00a1);
        let second_nan = f64::from_bits(0x7ff8_0000_0000_00b2);
        let third_nan = f64::from_bits(0x7ff8_0000_0000_00c3);
        let values = sliced_float_values(vec![
            None,
            Some(first_nan),
            Some(-0.0),
            Some(0.0),
            Some(2.0),
            Some(2.0),
            Some(f64::INFINITY),
            Some(f64::NEG_INFINITY),
            Some(second_nan),
            None,
            Some(3.0),
            Some(third_nan),
        ]);
        let timestamps = TimestampMillisecondArray::from_iter((0..16).map(Some));
        let value_ranges = [
            (0, 0),
            (0, 2),
            (0, 4),
            (1, 4),
            (4, 4),
            (4, 4),
            (8, 2),
            (5, 2),
            (3, 3),
            (11, 1),
        ];
        let timestamp_ranges = [
            (8, 0),
            (7, 2),
            (6, 4),
            (5, 4),
            (4, 4),
            (4, 4),
            (3, 2),
            (2, 2),
            (1, 3),
            (0, 1),
        ];

        assert_min_max_udfs_match_oracle(&timestamps, &values, &timestamp_ranges, &value_ranges);
    }

    #[test]
    fn min_max_over_time_batch_preserves_first_signed_zero_in_both_orders() {
        let timestamps = TimestampMillisecondArray::from(vec![0, 1, 2, 3]);
        for (values, expected) in [
            (Float64Array::from(vec![-0.0, 0.0]), -0.0),
            (Float64Array::from(vec![0.0, -0.0]), 0.0),
        ] {
            let (timestamp_ranges, value_ranges) =
                min_max_range_inputs(&timestamps, &values, &[(2, 2)], &[(0, 2)]);
            assert_option_bits(
                &run_min_max_udf(MinOverTime::scalar_udf(), timestamp_ranges, value_ranges),
                &[Some(expected)],
            );
            let (timestamp_ranges, value_ranges) =
                min_max_range_inputs(&timestamps, &values, &[(2, 2)], &[(0, 2)]);
            assert_option_bits(
                &run_min_max_udf(MaxOverTime::scalar_udf(), timestamp_ranges, value_ranges),
                &[Some(expected)],
            );
        }
    }

    #[test]
    fn min_max_over_time_batch_rebuilds_after_right_bound_retreat() {
        let timestamps = TimestampMillisecondArray::from(vec![0, 1, 2]);
        let ranges = [(0, 3), (0, 2)];
        assert_min_max_udfs_match_oracle(
            &timestamps,
            &Float64Array::from(vec![3.0, 2.0, 1.0]),
            &ranges,
            &ranges,
        );
        assert_min_max_udfs_match_oracle(
            &timestamps,
            &Float64Array::from(vec![1.0, 2.0, 3.0]),
            &ranges,
            &ranges,
        );
    }

    #[test]
    fn min_max_over_time_batch_returns_null_after_expiring_last_nan() {
        let nan = f64::from_bits(0x7ff8_0000_0000_00d4);
        let timestamps = TimestampMillisecondArray::from(vec![0, 1]);
        let values = Float64Array::from(vec![Some(nan), None]);
        let ranges = [(0, 1), (1, 1)];
        let (timestamp_ranges, value_ranges) =
            min_max_range_inputs(&timestamps, &values, &ranges, &ranges);
        assert_option_bits(
            &run_min_max_udf(MinOverTime::scalar_udf(), timestamp_ranges, value_ranges),
            &[Some(nan), None],
        );
        let (timestamp_ranges, value_ranges) =
            min_max_range_inputs(&timestamps, &values, &ranges, &ranges);
        assert_option_bits(
            &run_min_max_udf(MaxOverTime::scalar_udf(), timestamp_ranges, value_ranges),
            &[Some(nan), None],
        );
    }

    #[test]
    fn min_max_over_time_batch_matches_scalar_oracle_for_random_windows() {
        let values = sliced_float_values(
            (0..64)
                .map(|index| match index % 11 {
                    0 => None,
                    1 => Some(f64::from_bits(0x7ff8_0000_0000_0100 + index)),
                    2 => Some(-0.0),
                    3 => Some(0.0),
                    4 => Some(f64::INFINITY),
                    5 => Some(f64::NEG_INFINITY),
                    _ => Some((index as f64 * 17.0).sin()),
                })
                .collect(),
        );
        let timestamps = TimestampMillisecondArray::from_iter((0..128).map(Some));
        let mut seed = 0x5eed_u64;
        let mut value_ranges = Vec::new();
        let mut timestamp_ranges = Vec::new();
        for _ in 0..256 {
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
            let start = (seed as usize) % values.len();
            let length = ((seed >> 32) as usize) % (values.len() - start + 1);
            value_ranges.push((start as u32, length as u32));
            timestamp_ranges.push(((values.len() - start) as u32, length as u32));
        }

        assert_min_max_udfs_match_oracle(&timestamps, &values, &timestamp_ranges, &value_ranges);
    }

    #[test]
    fn min_max_over_time_batch_validates_inputs() {
        let error = MinOverTime::calc(&[]).unwrap_err();
        assert!(error.to_string().contains("expected 2 inputs, found 0"));

        let timestamps =
            RangeArray::from_ranges(Arc::new(TimestampMillisecondArray::from(vec![0])), [(0, 1)])
                .unwrap();
        let error = MaxOverTime::calc(&[
            ColumnarValue::Array(Arc::new(timestamps.into_dict())),
            ColumnarValue::Array(Arc::new(datatypes::arrow::array::Int64Array::from(vec![1]))),
        ])
        .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("expect value range vector as DictionaryArray<Int64>")
        );

        let null_keys = datatypes::arrow::array::Int64Array::from_iter([Some(0), None]);
        let null_key_dict = datatypes::arrow::array::DictionaryArray::<
            datatypes::arrow::datatypes::Int64Type,
        >::try_new(
            null_keys,
            Arc::new(TimestampMillisecondArray::from(vec![0, 1])),
        )
        .unwrap();
        let error = MinOverTime::calc(&[
            ColumnarValue::Array(Arc::new(null_key_dict)),
            ColumnarValue::Array(Arc::new(datatypes::arrow::array::Int64Array::from(vec![1]))),
        ])
        .unwrap_err();
        assert!(error.to_string().contains("Empty range is not expected"));
    }

    #[test]
    fn min_max_over_time_batch_rejects_mismatched_windows() {
        let timestamps = Arc::new(TimestampMillisecondArray::from(vec![0, 1, 2]));
        let values = Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0]));
        let timestamp_ranges = RangeArray::from_ranges(timestamps.clone(), [(0, 1)]).unwrap();
        let value_ranges = RangeArray::from_ranges(values.clone(), [(0, 1), (1, 1)]).unwrap();
        let error = invoke_range_udf(MinOverTime::scalar_udf(), timestamp_ranges, value_ranges)
            .unwrap_err();
        assert!(error.to_string().contains("same number of windows"));

        let timestamp_ranges = RangeArray::from_ranges(timestamps, [(0, 1)]).unwrap();
        let value_ranges = RangeArray::from_ranges(values, [(1, 2)]).unwrap();
        let error = invoke_range_udf(MaxOverTime::scalar_udf(), timestamp_ranges, value_ranges)
            .unwrap_err();
        assert!(error.to_string().contains("different lengths at window 0"));
    }

    #[test]
    fn sliding_evaluator_is_selected_only_for_overlapping_window_batches() {
        // Enough windows, long enough, advancing by at most a quarter of the window.
        assert!(reuses_candidates(&window_keys(&[
            (0, 32),
            (8, 32),
            (16, 32),
            (24, 32)
        ])));

        // One window too few.
        assert!(!reuses_candidates(&window_keys(&[
            (0, 32),
            (8, 32),
            (16, 32)
        ])));
        // One sample per window too short.
        assert!(!reuses_candidates(&window_keys(&[
            (0, 31),
            (7, 31),
            (14, 31),
            (21, 31)
        ])));
        // One sample per step too far apart.
        assert!(!reuses_candidates(&window_keys(&[
            (0, 32),
            (9, 32),
            (18, 32),
            (27, 32)
        ])));
    }

    #[test]
    fn sliding_evaluator_survives_an_unrepresentative_leading_window() {
        // `RangeManipulate` gives a series that starts inside the query range a first
        // window covering roughly one step, and emits a window covering no sample at
        // all as (0, 0). Reading either one as the batch shape hides the overlap that
        // the twenty windows behind it do have.
        let overlapping = (0..20u32).map(|index| (index * 8, 40));
        for leading in [(0, 1), (0, 0)] {
            let ranges = std::iter::once(leading)
                .chain(overlapping.clone())
                .collect::<Vec<_>>();
            assert!(reuses_candidates(&window_keys(&ranges)));
        }
    }

    #[test]
    fn empty_windows_do_not_shorten_the_measured_advance() {
        // A query whose last evaluation lands one window past the last sample ends on
        // a (0, 0) window. Its zero offset must not read as a batch that never moved,
        // which would put disjoint windows on the evaluator built for overlap.
        let disjoint = (0..4u32)
            .map(|index| (index * 240, 240))
            .collect::<Vec<_>>();
        assert!(!reuses_candidates(&window_keys(&disjoint)));
        let with_trailing_empty = [disjoint.as_slice(), &[(0, 0)]].concat();
        assert!(!reuses_candidates(&window_keys(&with_trailing_empty)));
    }

    #[test]
    fn min_max_over_time_batch_matches_oracle_on_overlapping_window_batches() {
        let values = sliced_float_values(
            (0..96)
                .map(|index| match index % 13 {
                    0 => None,
                    1 => Some(f64::from_bits(0x7ff8_0000_0000_0200 + index as u64)),
                    2 => Some(-0.0),
                    3 => Some(0.0),
                    4 => Some(f64::INFINITY),
                    5 => Some(f64::NEG_INFINITY),
                    6 | 7 => Some(5.0),
                    _ => Some(((index * 37) % 23) as f64),
                })
                .collect(),
        );
        let timestamps = TimestampMillisecondArray::from_iter((0..96).map(Some));
        // The first two windows open the sliding path; the rest then grow, repeat,
        // jump over a gap, collapse to empty, retreat, and rebuild from scratch.
        let ranges = [
            (0, 32),
            (4, 32),
            (8, 32),
            (12, 32),
            (16, 40),
            (20, 36),
            (20, 36),
            (64, 32),
            (64, 0),
            (60, 20),
            (0, 96),
        ];
        assert!(reuses_candidates(&window_keys(&ranges)));

        assert_min_max_udfs_match_oracle(&timestamps, &values, &ranges, &ranges);
    }

    #[test]
    fn both_extrema_paths_match_the_scalar_oracle_on_every_four_sample_window() {
        let alphabet = [
            None,
            Some(-0.0),
            Some(0.0),
            Some(-3.0),
            Some(2.0),
            Some(f64::INFINITY),
            Some(f64::NEG_INFINITY),
            Some(f64::from_bits(0x7ff8_0000_0000_0042)),
            Some(f64::from_bits(0xfff8_0000_0000_0066)),
        ];
        // Every window of a four-sample array, walked forwards and then backwards, so
        // the evaluator sees growing, shrinking, disjoint, empty and retreating bounds.
        let windows = (0..=4)
            .flat_map(|start| (0..=4 - start).map(move |length| (start, start + length)))
            .collect::<Vec<_>>();

        for encoded in 0..alphabet.len().pow(4) {
            let mut remaining = encoded;
            let values = Float64Array::from(
                (0..4)
                    .map(|_| {
                        let value = alphabet[remaining % alphabet.len()];
                        remaining /= alphabet.len();
                        value
                    })
                    .collect::<Vec<_>>(),
            );
            let mut sliding_min = SlidingExtrema::new(true);
            let mut sliding_max = SlidingExtrema::new(false);

            for &(start, end) in windows.iter().chain(windows.iter().rev()) {
                let window = values.slice(start, end - start);
                let timestamps = TimestampMillisecondArray::new_null(window.len());

                let expected = min_over_time(&timestamps, &window).map(f64::to_bits);
                assert_eq!(
                    sliding_min.evaluate(&values, start, end).map(f64::to_bits),
                    expected,
                    "sliding min, values {encoded}, window {start}..{end}"
                );
                assert_eq!(
                    scan_extremum(&values, start, end, true).map(f64::to_bits),
                    expected,
                    "scanned min, values {encoded}, window {start}..{end}"
                );

                let expected = max_over_time(&timestamps, &window).map(f64::to_bits);
                assert_eq!(
                    sliding_max.evaluate(&values, start, end).map(f64::to_bits),
                    expected,
                    "sliding max, values {encoded}, window {start}..{end}"
                );
                assert_eq!(
                    scan_extremum(&values, start, end, false).map(f64::to_bits),
                    expected,
                    "scanned max, values {encoded}, window {start}..{end}"
                );
            }
        }
    }

    fn assert_over_time_value(actual: Option<f64>, expected: Option<f64>) {
        match (actual, expected) {
            (Some(actual), Some(expected)) if expected.is_nan() => assert!(actual.is_nan()),
            (Some(actual), Some(expected)) => assert_eq!(actual, expected),
            (None, None) => {}
            (actual, expected) => panic!("expected {expected:?}, got {actual:?}"),
        }
    }

    fn assert_min_max(
        values: Vec<Option<f64>>,
        expected_min: Option<f64>,
        expected_max: Option<f64>,
    ) {
        let timestamps = TimestampMillisecondArray::from(vec![0; values.len()]);
        let values = Float64Array::from(values);

        assert_over_time_value(min_over_time(&timestamps, &values), expected_min);
        assert_over_time_value(max_over_time(&timestamps, &values), expected_max);
    }

    fn special_ranges() -> (RangeArray, RangeArray) {
        use datafusion::arrow::buffer::NullBuffer;

        let timestamps = Arc::new(TimestampMillisecondArray::from_iter_values(0..10)).slice(1, 8);
        let values = Arc::new(Float64Array::new(
            vec![
                99.0,
                -0.0,
                0.0,
                f64::INFINITY,
                f64::NEG_INFINITY,
                f64::from_bits(0x7ff8_0000_0000_0042),
                f64::from_bits(0x7ff8_0000_0000_0066),
                3.0,
                -7.0,
                42.0,
            ]
            .into(),
            Some(NullBuffer::from(vec![
                true, true, true, true, true, false, true, true, true, true,
            ])),
        ))
        .slice(1, 8);
        // Empty, singleton, overlapping, disjoint, repeated, and backward windows use
        // independent timestamp and value offsets.
        let timestamp_ranges = [
            (0, 0),
            (1, 1),
            (2, 1),
            (3, 1),
            (4, 1),
            (5, 1),
            (6, 1),
            (0, 3),
            (3, 2),
            (4, 2),
            (6, 2),
            (5, 1),
            (2, 1),
        ];
        let value_ranges = [
            (7, 0),
            (0, 1),
            (1, 1),
            (2, 1),
            (3, 1),
            (4, 1),
            (5, 1),
            (0, 3),
            (3, 2),
            (4, 2),
            (1, 2),
            (5, 1),
            (1, 1),
        ];

        let timestamp_ranges = RangeArray::from_ranges(
            Arc::new(timestamps),
            std::iter::once((0, 0)).chain(timestamp_ranges),
        )
        .unwrap()
        .into_dict()
        .slice(1, timestamp_ranges.len());
        let value_ranges = RangeArray::from_ranges(
            Arc::new(values),
            std::iter::once((0, 0)).chain(value_ranges),
        )
        .unwrap()
        .into_dict()
        .slice(1, value_ranges.len());

        (
            RangeArray::try_new(timestamp_ranges).unwrap(),
            RangeArray::try_new(value_ranges).unwrap(),
        )
    }

    fn assert_specialized_matches_oracle(
        udf: ScalarUDF,
        kernel: fn(&TimestampMillisecondArray, &Float64Array) -> Option<f64>,
    ) {
        use crate::functions::test_util::invoke_range_udf;

        let (timestamps, values) = special_ranges();
        let expected = (0..timestamps.len())
            .map(|index| {
                let timestamps = timestamps.get(index).unwrap();
                let values = values.get(index).unwrap();
                kernel(
                    timestamps
                        .as_any()
                        .downcast_ref::<TimestampMillisecondArray>()
                        .unwrap(),
                    values.as_any().downcast_ref::<Float64Array>().unwrap(),
                )
            })
            .collect::<Vec<_>>();
        let (timestamps, values) = special_ranges();
        let output = invoke_range_udf(udf, timestamps, values).unwrap();
        let output_array = extract_array(&output).unwrap();
        let output = output_array
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();

        assert_eq!(output.len(), expected.len());
        for (index, expected) in expected.into_iter().enumerate() {
            assert_eq!(output.is_null(index), expected.is_none());
            if let Some(expected) = expected {
                assert_eq!(output.value(index).to_bits(), expected.to_bits());
            }
        }
    }

    #[test]
    fn specialized_presence_range_udfs_match_slice_oracles() {
        assert_specialized_matches_oracle(CountOverTime::scalar_udf(), count_over_time);
        assert_specialized_matches_oracle(LastOverTime::scalar_udf(), last_over_time);
        assert_specialized_matches_oracle(AbsentOverTime::scalar_udf(), absent_over_time);
        assert_specialized_matches_oracle(PresentOverTime::scalar_udf(), present_over_time);
    }

    #[test]
    fn specialized_presence_range_udfs_ignore_null_samples() {
        use datafusion::arrow::buffer::NullBuffer;

        let make_ranges = || {
            let timestamps =
                Arc::new(TimestampMillisecondArray::from_iter_values(0..9)).slice(1, 7);
            let values = Arc::new(Float64Array::new(
                vec![99.0, 1.0, 0.0, 0.0, 4.0, f64::NAN, 0.0, -0.0, 42.0].into(),
                Some(NullBuffer::from(vec![
                    true, true, false, false, true, true, true, true, true,
                ])),
            ))
            .slice(1, 7);
            let ranges = [(0, 4), (0, 3), (1, 2), (4, 1), (5, 2), (7, 0)];

            (
                RangeArray::from_ranges(Arc::new(timestamps), ranges).unwrap(),
                RangeArray::from_ranges(Arc::new(values), ranges).unwrap(),
            )
        };
        // The first window is [1, NULL, NULL, 4]; the old physical-length evaluator
        // incorrectly returned 4 for count_over_time.
        let cases = [
            (
                CountOverTime::scalar_udf(),
                [Some(2.0), Some(1.0), None, Some(1.0), Some(2.0), None],
            ),
            (
                LastOverTime::scalar_udf(),
                [Some(4.0), Some(1.0), None, Some(f64::NAN), Some(-0.0), None],
            ),
            (
                AbsentOverTime::scalar_udf(),
                [None, None, Some(1.0), None, None, Some(1.0)],
            ),
            (
                PresentOverTime::scalar_udf(),
                [Some(1.0), Some(1.0), None, Some(1.0), Some(1.0), None],
            ),
        ];

        for (udf, expected) in cases {
            let (timestamps, values) = make_ranges();
            let output =
                crate::functions::test_util::invoke_range_udf(udf, timestamps, values).unwrap();
            let output_array = extract_array(&output).unwrap();
            let output = output_array
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();

            for (index, expected) in expected.into_iter().enumerate() {
                assert_eq!(output.is_valid(index), expected.is_some());
                if let Some(expected) = expected {
                    assert_eq!(output.value(index).to_bits(), expected.to_bits());
                }
            }
        }
    }

    /// Payload kept under the null slots of the layouts below, so an evaluator that read the
    /// padding instead of the samples would report a different value for `last_over_time`.
    const NULL_PAYLOAD: f64 = -1234.5;

    /// The backing layouts the presence matrix covers: no nulls, a single null, a fixed spread
    /// of scattered nulls, a run of missing samples, and valid slots holding the special values
    /// that must not be mistaken for missing samples.
    fn presence_layouts(len: usize) -> Vec<(&'static str, Vec<Option<f64>>)> {
        let base = |index: usize| Some((index % 7) as f64 + 1.0);

        let no_nulls = (0..len).map(base).collect::<Vec<_>>();

        let mut one_null = no_nulls.clone();
        one_null[5] = None;

        let mut scattered = no_nulls.clone();
        // Pairs, gaps, and both ends of the backing array.
        for index in [0usize, 3, 8, 9, 13, len - 1] {
            scattered[index] = None;
        }

        let mut missing_run = no_nulls.clone();
        missing_run[11..=15].fill(None);

        let mut special = no_nulls.clone();
        special[0] = Some(f64::NAN);
        special[2] = Some(f64::from_bits(0x7ff0_0000_0000_0002)); // stale marker
        special[5] = Some(-0.0);
        special[7] = Some(f64::INFINITY);
        special[11] = Some(f64::NEG_INFINITY);
        special[13] = None;
        special[23] = None;

        vec![
            ("no nulls", no_nulls),
            ("one null", one_null),
            ("scattered nulls", scattered),
            ("missing run", missing_run),
            ("special payloads", special),
        ]
    }

    /// The window shapes the presence matrix covers, as `(offset, length)` pairs over a 24-slot
    /// backing array.
    fn presence_window_shapes() -> Vec<(&'static str, Vec<(u32, u32)>)> {
        vec![
            ("overlapping", (0..=12).map(|i| (i, 12)).collect()),
            ("small", (0..21).map(|i| (i, 3)).collect()),
            ("disjoint", (0..6).map(|i| (i * 4, 4)).collect()),
            ("empty", vec![(0, 0), (12, 0), (23, 0)]),
            // Boundaries on both sides of a null run, and windows sitting inside one.
            (
                "null run edges",
                vec![(0, 1), (1, 1), (23, 1), (10, 5), (12, 4), (15, 2)],
            ),
        ]
    }

    /// Builds a backing array that keeps `NULL_PAYLOAD` under every null slot.
    fn nullable_backing_values(values: &[Option<f64>]) -> Float64Array {
        Float64Array::new(
            values
                .iter()
                .map(|value| value.unwrap_or(NULL_PAYLOAD))
                .collect::<Vec<_>>()
                .into(),
            Some(NullBuffer::from_iter(values.iter().map(Option::is_some))),
        )
    }

    /// Runs one presence range UDF over a backing array and a set of windows.
    fn run_presence_udf(
        udf: ScalarUDF,
        values: &Float64Array,
        ranges: &[(u32, u32)],
    ) -> Vec<Option<f64>> {
        let timestamps = Arc::new(TimestampMillisecondArray::from_iter_values(
            (0..values.len() as i64).map(|index| index * 1_000),
        ));
        let output = invoke_range_udf(
            udf,
            RangeArray::from_ranges(timestamps, ranges.iter().copied()).unwrap(),
            RangeArray::from_ranges(Arc::new(values.clone()), ranges.iter().copied()).unwrap(),
        )
        .unwrap();
        extract_array(&output)
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .iter()
            .collect()
    }

    #[test]
    fn presence_range_udfs_match_per_window_oracle_on_nullable_layouts() {
        // The scalar kernels independently validate the specialized UDFs.
        let len = 24;
        type PresenceCase = (
            fn() -> ScalarUDF,
            fn(&TimestampMillisecondArray, &Float64Array) -> Option<f64>,
        );
        let cases: [PresenceCase; 4] = [
            (CountOverTime::scalar_udf, count_over_time),
            (LastOverTime::scalar_udf, last_over_time),
            (AbsentOverTime::scalar_udf, absent_over_time),
            (PresentOverTime::scalar_udf, present_over_time),
        ];

        for (_, nulls) in presence_layouts(len) {
            for (_, ranges) in presence_window_shapes() {
                let values = nullable_backing_values(&nulls);
                for (build_udf, kernel) in cases {
                    let expected = ranges
                        .iter()
                        .map(|&(offset, length)| {
                            let values = values.slice(offset as usize, length as usize);
                            let values = values.as_any().downcast_ref::<Float64Array>().unwrap();
                            let timestamps = TimestampMillisecondArray::new_null(values.len());
                            kernel(&timestamps, values)
                        })
                        .collect::<Vec<_>>();
                    let actual = run_presence_udf(build_udf(), &values, &ranges);
                    assert_option_bits(&actual, &expected);
                }
            }
        }
    }

    #[test]
    fn presence_range_udfs_measure_samples_across_null_runs() {
        // Slots 2..=5 are missing, slots 0, 1 and 6 are single samples, and one window is made
        // only of nulls.
        let nulls = [Some(1.0), Some(3.0), None, None, None, None, Some(9.0)];
        let ranges = vec![
            (0, 7),
            (0, 2),
            (2, 4),
            (3, 4),
            (1, 1),
            (2, 1),
            (6, 1),
            (0, 0),
        ];
        let values = nullable_backing_values(&nulls);
        let cases = [
            (
                CountOverTime::scalar_udf(),
                vec![
                    Some(3.0),
                    Some(2.0),
                    None,
                    Some(1.0),
                    Some(1.0),
                    None,
                    Some(1.0),
                    None,
                ],
            ),
            (
                LastOverTime::scalar_udf(),
                vec![
                    Some(9.0),
                    Some(3.0),
                    None,
                    Some(9.0),
                    Some(3.0),
                    None,
                    Some(9.0),
                    None,
                ],
            ),
            (
                AbsentOverTime::scalar_udf(),
                vec![
                    None,
                    None,
                    Some(1.0),
                    None,
                    None,
                    Some(1.0),
                    None,
                    Some(1.0),
                ],
            ),
            (
                PresentOverTime::scalar_udf(),
                vec![
                    Some(1.0),
                    Some(1.0),
                    None,
                    Some(1.0),
                    Some(1.0),
                    None,
                    Some(1.0),
                    None,
                ],
            ),
        ];

        for (udf, expected) in cases {
            let actual = run_presence_udf(udf, &values, &ranges);
            assert_option_bits(&actual, &expected);
        }
    }

    #[test]
    fn specialized_presence_range_udfs_preserve_errors_and_metadata() {
        use datafusion::arrow::array::{DictionaryArray, Int64Array};
        use datafusion::arrow::datatypes::{Field, Int64Type};
        use datafusion_common::config::ConfigOptions;
        use datafusion_expr::ScalarFunctionArgs;

        use crate::functions::test_util::{assert_execution_error, invoke_range_udf};

        for (udf, name) in [
            (CountOverTime::scalar_udf(), "prom_count_over_time"),
            (LastOverTime::scalar_udf(), "prom_last_over_time"),
            (AbsentOverTime::scalar_udf(), "prom_absent_over_time"),
            (PresentOverTime::scalar_udf(), "prom_present_over_time"),
        ] {
            assert_eq!(udf.name(), name);
            assert_eq!(udf.signature().volatility, Volatility::Volatile);
        }

        let timestamps = Arc::new(TimestampMillisecondArray::from_iter_values(0..3));
        let values = Arc::new(Float64Array::from_iter_values([1.0, 2.0, 3.0]));
        let error = invoke_range_udf(
            CountOverTime::scalar_udf(),
            RangeArray::from_ranges(timestamps.clone(), [(0, 1), (1, 1)]).unwrap(),
            RangeArray::from_ranges(values.clone(), [(0, 1)]).unwrap(),
        )
        .unwrap_err();
        assert_execution_error(
            error,
            "RangeArray have different lengths in PromQL function prom_count_over_time: array1=2, array2=1",
        );

        let error = invoke_range_udf(
            CountOverTime::scalar_udf(),
            RangeArray::from_ranges(timestamps.clone(), [(0, 1), (1, 2)]).unwrap(),
            RangeArray::from_ranges(values.clone(), [(0, 1), (1, 1)]).unwrap(),
        )
        .unwrap_err();
        assert_execution_error(
            error,
            "RangeArray's element 1 have different lengths in PromQL function prom_count_over_time: array1=2, array2=1",
        );

        let invoke_dict = |timestamps: DictionaryArray<Int64Type>,
                           values: DictionaryArray<Int64Type>| {
            let args = vec![
                ColumnarValue::Array(Arc::new(timestamps)),
                ColumnarValue::Array(Arc::new(values)),
            ];
            CountOverTime::scalar_udf().invoke_with_args(ScalarFunctionArgs {
                arg_fields: args
                    .iter()
                    .enumerate()
                    .map(|(index, value)| {
                        Arc::new(Field::new(
                            format!("c{index}"),
                            value.data_type().clone(),
                            true,
                        ))
                    })
                    .collect(),
                args,
                number_rows: 1,
                return_field: Arc::new(Field::new("out", DataType::Float64, true)),
                config_options: Arc::new(ConfigOptions::default()),
            })
        };
        let empty = invoke_dict(
            DictionaryArray::new(
                Int64Array::from(Vec::<i64>::new()),
                Arc::new(Float64Array::from_iter_values([1.0])),
            ),
            DictionaryArray::new(
                Int64Array::from(Vec::<i64>::new()),
                Arc::new(TimestampMillisecondArray::from_iter_values([0])),
            ),
        )
        .unwrap();
        assert!(extract_array(&empty).unwrap().is_empty());

        let null_keys = DictionaryArray::new(
            Int64Array::from(vec![None]),
            Arc::new(TimestampMillisecondArray::from_iter_values([0])),
        );
        let values_range = RangeArray::from_ranges(values.clone(), [(0, 1)]).unwrap();
        assert_eq!(
            invoke_dict(null_keys, values_range.into_dict())
                .unwrap_err()
                .to_string(),
            "External error: Empty range is not expected"
        );

        let timestamps_range = RangeArray::from_ranges(timestamps, [(0, 1)]).unwrap();
        let invalid_values = unsafe { RangeArray::from_ranges_unchecked(values, [(2, 2)]) };
        assert_eq!(
            invoke_dict(timestamps_range.into_dict(), invalid_values.into_dict())
                .unwrap_err()
                .to_string(),
            "External error: Illegal range: offset 2, length 2, array len 3"
        );

        let output = invoke_range_udf(
            SumOverTime::scalar_udf(),
            RangeArray::from_ranges(
                Arc::new(TimestampMillisecondArray::from_iter_values([0, 1])),
                [(0, 2)],
            )
            .unwrap(),
            RangeArray::from_ranges(
                Arc::new(Float64Array::from_iter_values([1.0, 2.0])),
                [(0, 2)],
            )
            .unwrap(),
        )
        .unwrap();
        assert_eq!(
            extract_array(&output)
                .unwrap()
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(0),
            3.0
        );
    }

    #[test]
    fn min_max_over_time_ignore_ordinary_nan_when_finite_values_exist() {
        let ordinary_nan = f64::from_bits(0x7ff8_0000_0000_0000);

        assert_min_max(
            vec![Some(ordinary_nan), Some(3.0), Some(-2.0)],
            Some(-2.0),
            Some(3.0),
        );
        assert_min_max(
            vec![Some(3.0), Some(ordinary_nan), Some(-2.0)],
            Some(-2.0),
            Some(3.0),
        );
        assert_min_max(
            vec![Some(-2.0), Some(3.0), Some(ordinary_nan)],
            Some(-2.0),
            Some(3.0),
        );
        assert_min_max(
            vec![Some(ordinary_nan), Some(ordinary_nan)],
            Some(ordinary_nan),
            Some(ordinary_nan),
        );
        assert_min_max(
            vec![Some(3.0), Some(-2.0), Some(1.0)],
            Some(-2.0),
            Some(3.0),
        );
        assert_min_max(vec![], None, None);
        assert_min_max(vec![None, None], None, None);
    }

    // build timestamp range and value range arrays for test
    fn build_test_range_arrays() -> (RangeArray, RangeArray) {
        let ts_array = Arc::new(TimestampMillisecondArray::from_iter(
            [
                1000i64, 3000, 5000, 7000, 9000, 11000, 13000, 15000, 17000, 200000, 500000,
            ]
            .into_iter()
            .map(Some),
        ));
        let ranges = [
            (0, 2),
            (0, 5),
            (1, 1), // only 1 element
            (2, 0), // empty range
            (2, 0), // empty range
            (3, 3),
            (4, 3),
            (5, 3),
            (8, 1), // only 1 element
            (9, 0), // empty range
        ];

        let values_array = Arc::new(Float64Array::from_iter([
            12.345678, 87.654321, 31.415927, 27.182818, 70.710678, 41.421356, 57.735027, 69.314718,
            98.019802, 1.98019802, 61.803399,
        ]));

        let ts_range_array = RangeArray::from_ranges(ts_array, ranges).unwrap();
        let value_range_array = RangeArray::from_ranges(values_array, ranges).unwrap();

        (ts_range_array, value_range_array)
    }

    #[test]
    fn calculate_avg_over_time() {
        let (ts_array, value_array) = build_test_range_arrays();
        simple_range_udf_runner(
            AvgOverTime::scalar_udf(),
            ts_array,
            value_array,
            vec![],
            vec![
                Some(49.9999995),
                Some(45.8618844),
                Some(87.654321),
                None,
                None,
                Some(46.438284),
                Some(56.62235366666667),
                Some(56.15703366666667),
                Some(98.019802),
                None,
            ],
        );
    }

    #[test]
    fn calculate_min_over_time() {
        let (ts_array, value_array) = build_test_range_arrays();
        simple_range_udf_runner(
            MinOverTime::scalar_udf(),
            ts_array,
            value_array,
            vec![],
            vec![
                Some(12.345678),
                Some(12.345678),
                Some(87.654321),
                None,
                None,
                Some(27.182818),
                Some(41.421356),
                Some(41.421356),
                Some(98.019802),
                None,
            ],
        );
    }

    #[test]
    fn calculate_max_over_time() {
        let (ts_array, value_array) = build_test_range_arrays();
        simple_range_udf_runner(
            MaxOverTime::scalar_udf(),
            ts_array,
            value_array,
            vec![],
            vec![
                Some(87.654321),
                Some(87.654321),
                Some(87.654321),
                None,
                None,
                Some(70.710678),
                Some(70.710678),
                Some(69.314718),
                Some(98.019802),
                None,
            ],
        );
    }

    #[test]
    fn calculate_sum_over_time() {
        let (ts_array, value_array) = build_test_range_arrays();
        simple_range_udf_runner(
            SumOverTime::scalar_udf(),
            ts_array,
            value_array,
            vec![],
            vec![
                Some(99.999999),
                Some(229.309422),
                Some(87.654321),
                None,
                None,
                Some(139.314852),
                Some(169.867061),
                Some(168.471101),
                Some(98.019802),
                None,
            ],
        );
    }

    #[test]
    fn calculate_count_over_time() {
        let (ts_array, value_array) = build_test_range_arrays();
        simple_range_udf_runner(
            CountOverTime::scalar_udf(),
            ts_array,
            value_array,
            vec![],
            vec![
                Some(2.0),
                Some(5.0),
                Some(1.0),
                None,
                None,
                Some(3.0),
                Some(3.0),
                Some(3.0),
                Some(1.0),
                None,
            ],
        );
    }

    #[test]
    fn calculate_last_over_time() {
        let (ts_array, value_array) = build_test_range_arrays();
        simple_range_udf_runner(
            LastOverTime::scalar_udf(),
            ts_array,
            value_array,
            vec![],
            vec![
                Some(87.654321),
                Some(70.710678),
                Some(87.654321),
                None,
                None,
                Some(41.421356),
                Some(57.735027),
                Some(69.314718),
                Some(98.019802),
                None,
            ],
        );
    }

    #[test]
    fn calculate_absent_over_time() {
        let (ts_array, value_array) = build_test_range_arrays();
        simple_range_udf_runner(
            AbsentOverTime::scalar_udf(),
            ts_array,
            value_array,
            vec![],
            vec![
                None,
                None,
                None,
                Some(1.0),
                Some(1.0),
                None,
                None,
                None,
                None,
                Some(1.0),
            ],
        );
    }

    #[test]
    fn calculate_present_over_time() {
        let (ts_array, value_array) = build_test_range_arrays();
        simple_range_udf_runner(
            PresentOverTime::scalar_udf(),
            ts_array,
            value_array,
            vec![],
            vec![
                Some(1.0),
                Some(1.0),
                Some(1.0),
                None,
                None,
                Some(1.0),
                Some(1.0),
                Some(1.0),
                Some(1.0),
                None,
            ],
        );
    }

    #[test]
    fn calculate_stdvar_over_time() {
        let (ts_array, value_array) = build_test_range_arrays();
        simple_range_udf_runner(
            StdvarOverTime::scalar_udf(),
            ts_array,
            value_array,
            vec![],
            vec![
                Some(1417.8479276253622),
                Some(808.999919713209),
                Some(0.0),
                None,
                None,
                Some(328.3638826418587),
                Some(143.5964181766362),
                Some(130.91830542386285),
                Some(0.0),
                None,
            ],
        );

        // add more assertions
        let ts_array = Arc::new(TimestampMillisecondArray::from_iter(
            [1000i64, 3000, 5000, 7000, 9000, 11000, 13000, 15000]
                .into_iter()
                .map(Some),
        ));
        let values_array = Arc::new(Float64Array::from_iter([
            1.5990505637277868,
            1.5990505637277868,
            1.5990505637277868,
            0.0,
            8.0,
            8.0,
            2.0,
            3.0,
        ]));
        let ranges = [(0, 3), (3, 5)];
        simple_range_udf_runner(
            StdvarOverTime::scalar_udf(),
            RangeArray::from_ranges(ts_array, ranges).unwrap(),
            RangeArray::from_ranges(values_array, ranges).unwrap(),
            vec![],
            vec![Some(0.0), Some(10.559999999999999)],
        );
    }

    #[test]
    fn calculate_std_dev_over_time() {
        let (ts_array, value_array) = build_test_range_arrays();
        simple_range_udf_runner(
            StddevOverTime::scalar_udf(),
            ts_array,
            value_array,
            vec![],
            vec![
                Some(37.6543215),
                Some(28.442923895289123),
                Some(0.0),
                None,
                None,
                Some(18.12081352042062),
                Some(11.983172291869804),
                Some(11.441953741554055),
                Some(0.0),
                None,
            ],
        );

        // add more assertions
        let ts_array = Arc::new(TimestampMillisecondArray::from_iter(
            [1000i64, 3000, 5000, 7000, 9000, 11000, 13000, 15000]
                .into_iter()
                .map(Some),
        ));
        let values_array = Arc::new(Float64Array::from_iter([
            1.5990505637277868,
            1.5990505637277868,
            1.5990505637277868,
            0.0,
            8.0,
            8.0,
            2.0,
            3.0,
        ]));
        let ranges = [(0, 3), (3, 5)];
        simple_range_udf_runner(
            StddevOverTime::scalar_udf(),
            RangeArray::from_ranges(ts_array, ranges).unwrap(),
            RangeArray::from_ranges(values_array, ranges).unwrap(),
            vec![],
            vec![Some(0.0), Some(3.249615361854384)],
        );
    }

    /// Timestamps and value ranges shared by the null-sample assertions below.
    fn null_sample_range_arrays() -> (RangeArray, RangeArray) {
        let ts_array = Arc::new(TimestampMillisecondArray::from_iter_values([
            0i64, 1000, 2000, 3000,
        ]));
        // Samples are 2.0@0 and 8.0@3000; the null slots keep a payload that would skew every
        // aggregate if it were read.
        let values_array = Arc::new(Float64Array::new(
            vec![2.0, 1000.0, -1000.0, 8.0].into(),
            Some(NullBuffer::from_iter([true, false, false, true])),
        ));
        // The second window holds no sample at all.
        let ranges = [(0, 4), (1, 2)];

        (
            RangeArray::from_ranges(ts_array, ranges).unwrap(),
            RangeArray::from_ranges(values_array, ranges).unwrap(),
        )
    }

    #[test]
    fn avg_over_time_divides_by_sample_count() {
        let (ts_array, value_array) = null_sample_range_arrays();
        simple_range_udf_runner(
            AvgOverTime::scalar_udf(),
            ts_array,
            value_array,
            vec![],
            vec![Some(5.0), None],
        );
    }

    #[test]
    fn stdvar_and_stddev_over_time_skip_null_samples() {
        let (ts_array, value_array) = null_sample_range_arrays();
        simple_range_udf_runner(
            StdvarOverTime::scalar_udf(),
            ts_array,
            value_array,
            vec![],
            vec![Some(9.0), None],
        );

        let (ts_array, value_array) = null_sample_range_arrays();
        simple_range_udf_runner(
            StddevOverTime::scalar_udf(),
            ts_array,
            value_array,
            vec![],
            vec![Some(3.0), None],
        );
    }
}
