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

// This file also contains some code from prometheus project.

// Copyright 2015 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Implementations of `rate`, `increase` and `delta` functions in PromQL.

use std::fmt::Display;
use std::ops::Range;
use std::sync::Arc;

use datafusion::arrow::array::{Float64Array, Float64Builder, TimestampMillisecondArray};
use datafusion::arrow::datatypes::TimeUnit;
use datafusion::common::{DataFusionError, Result as DfResult};
use datafusion::logical_expr::{ScalarUDF, Volatility};
use datafusion::physical_plan::ColumnarValue;
use datafusion_expr::create_udf;
use datatypes::arrow::array::{Array, Int64Array};
use datatypes::arrow::datatypes::DataType;

use crate::functions::{extract_array, extract_range_dict};
use crate::range_array::{RangeArray, unpack};

pub type Delta = ExtrapolatedRate<false, false>;
pub type Rate = ExtrapolatedRate<true, true>;
pub type Increase = ExtrapolatedRate<true, false>;

/// Part of the `extrapolatedRate` in Promql,
/// from <https://github.com/prometheus/prometheus/blob/v0.40.1/promql/functions.go#L66>
#[derive(Debug)]
pub struct ExtrapolatedRate<const IS_COUNTER: bool, const IS_RATE: bool> {
    /// Range length in milliseconds.
    range_length: i64,
}

impl<const IS_COUNTER: bool, const IS_RATE: bool> ExtrapolatedRate<IS_COUNTER, IS_RATE> {
    /// Constructor. Other public usage should use [scalar_udf()](ExtrapolatedRate::scalar_udf()) instead.
    fn new(range_length: i64) -> Self {
        Self { range_length }
    }

    fn func_name() -> &'static str {
        match (IS_COUNTER, IS_RATE) {
            (true, true) => "prom_rate",
            (true, false) => "prom_increase",
            (false, false) => "prom_delta",
            (false, true) => {
                unreachable!("gauge rate is not supported by ExtrapolatedRate")
            }
        }
    }

    fn scalar_udf_with_name(name: &str) -> ScalarUDF {
        let input_types = vec![
            // timestamp range vector
            RangeArray::convert_data_type(DataType::Timestamp(TimeUnit::Millisecond, None)),
            // value range vector
            RangeArray::convert_data_type(DataType::Float64),
            // timestamp vector
            DataType::Timestamp(TimeUnit::Millisecond, None),
            // range length
            DataType::Int64,
        ];

        create_udf(
            name,
            input_types,
            DataType::Float64,
            Volatility::Volatile,
            Arc::new(move |input: &_| Self::create_function(input)?.calc(input)) as _,
        )
    }

    fn create_function(inputs: &[ColumnarValue]) -> DfResult<Self> {
        if inputs.len() != 4 {
            return Err(DataFusionError::Plan(
                "ExtrapolatedRate function should have 4 inputs".to_string(),
            ));
        }

        let range_length_array = extract_array(&inputs[3])?;
        let range_length_array = range_length_array
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "{}: expect Int64 as range length type, found {}",
                    Self::func_name(),
                    range_length_array.data_type()
                ))
            })?;
        if range_length_array.is_empty() || range_length_array.is_null(0) {
            return Err(DataFusionError::Execution(format!(
                "{}: range length must contain a non-null Int64 value",
                Self::func_name()
            )));
        }
        let range_length = range_length_array.value(0);

        Ok(Self::new(range_length))
    }

    /// Input parameters:
    /// * 0: timestamp range vector
    /// * 1: value range vector
    /// * 2: timestamp vector
    /// * 3: range length. Range duration in milliseconds
    fn calc(&self, input: &[ColumnarValue]) -> DfResult<ColumnarValue> {
        if input.len() != 4 {
            return Err(DataFusionError::Plan(
                "ExtrapolatedRate function should have 4 inputs".to_string(),
            ));
        }

        let ts_dict = extract_range_dict(
            &input[0],
            Self::func_name(),
            "timestamp range vector",
            &DataType::Timestamp(TimeUnit::Millisecond, None),
        )?;
        let value_dict = extract_range_dict(
            &input[1],
            Self::func_name(),
            "value range vector",
            &DataType::Float64,
        )?;
        let eval_ts_array = extract_eval_timestamps(&input[2], Self::func_name())?;

        let keys = ts_dict.keys().values();
        let num_windows = keys.len();
        if value_dict.keys().len() != num_windows {
            return Err(DataFusionError::Execution(format!(
                "{}: timestamp and value ranges should have the same number of windows, found {} and {}",
                Self::func_name(),
                num_windows,
                value_dict.keys().len()
            )));
        }
        if value_dict.keys().values() != keys {
            return Err(DataFusionError::Execution(format!(
                "{}: timestamp and value ranges should have the same window layout",
                Self::func_name()
            )));
        }
        if eval_ts_array.len() != num_windows {
            return Err(DataFusionError::Execution(format!(
                "{}: evaluation timestamp vector should have the same number of rows as range inputs, found {} and {}",
                Self::func_name(),
                eval_ts_array.len(),
                num_windows
            )));
        }

        let all_timestamps = ts_dict
            .values()
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .expect("validated by extract_range_dict")
            .values();
        let all_values = value_dict
            .values()
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("validated by extract_range_dict")
            .values();
        let eval_ts = eval_ts_array.values();

        let mut result_builder = Float64Builder::with_capacity(num_windows);
        let range_length = self.range_length;
        let range_length_secs = range_length as f64 / 1000.0;

        // Range windows normally overlap heavily, so scanning every one for resets costs far
        // more than a single pass over the values. Index the reset positions once that is the
        // cheaper side, and stop counting as soon as the requested pairs pass that budget,
        // which heavy overlap does within the first few windows. A short lookback with a long
        // step is the shape that never reaches it, and there the per-window scans do win.
        let mut reset_index = if IS_COUNTER {
            let budget = all_values.len().saturating_sub(1);
            let mut scanned_pairs = 0usize;
            keys.iter()
                .any(|&key| {
                    scanned_pairs =
                        scanned_pairs.saturating_add(unpack(key).1.saturating_sub(1) as usize);
                    scanned_pairs > budget
                })
                .then(|| CounterResetIndex::new(all_values))
        } else {
            None
        };

        for index in 0..num_windows {
            let (raw_offset, raw_length) = unpack(keys[index]);
            let offset = raw_offset as usize;
            let length = raw_length as usize;

            if length < 2 {
                result_builder.append_null();
                continue;
            }

            let end = offset + length;
            let first_value = all_values[offset];
            let last_value = all_values[end - 1];

            let mut result_value = last_value - first_value;
            if IS_COUNTER {
                result_value = match &mut reset_index {
                    Some(reset_index) => reset_index.add_resets(result_value, offset, end),
                    None => add_counter_resets(result_value, &all_values[offset..end]),
                };
            }

            let first_ts = all_timestamps[offset];
            let last_ts = all_timestamps[end - 1];
            let range_end = eval_ts[index];
            let range_start = range_end - range_length;
            let sampled_interval_ms = (last_ts - first_ts) as f64;
            let average_interval_ms = sampled_interval_ms / (length - 1) as f64;
            let mut duration_to_start_ms = (first_ts - range_start) as f64;
            let duration_to_end_ms = (range_end - last_ts) as f64;

            // Counters cannot be negative, so Prometheus allows the extrapolation window to snap
            // back to the inferred zero point instead of extending into negative values.
            if IS_COUNTER && result_value > 0.0 && first_value >= 0.0 {
                let duration_to_zero = sampled_interval_ms * (first_value / result_value);
                if duration_to_zero < duration_to_start_ms {
                    duration_to_start_ms = duration_to_zero;
                }
            }

            let extrapolation_threshold = average_interval_ms * 1.1;
            let mut extrapolated_interval_ms = sampled_interval_ms;

            // Mirror Prometheus extrapolation: extend to the real range boundary when a sample is
            // close enough, otherwise add half an average sampling interval on that side.
            if duration_to_start_ms < extrapolation_threshold {
                extrapolated_interval_ms += duration_to_start_ms;
            } else {
                extrapolated_interval_ms += average_interval_ms / 2.0;
            }
            if duration_to_end_ms < extrapolation_threshold {
                extrapolated_interval_ms += duration_to_end_ms;
            } else {
                extrapolated_interval_ms += average_interval_ms / 2.0;
            }

            let mut factor = extrapolated_interval_ms / sampled_interval_ms;

            if IS_RATE {
                factor /= range_length_secs;
            }

            result_builder.append_value(result_value * factor);
        }

        let result = ColumnarValue::Array(Arc::new(result_builder.finish()));
        Ok(result)
    }
}

/// Adds the value preceding every counter reset in `values` to `result`, in sample order.
///
/// Prometheus accumulates the resets into the running result rather than summing them on
/// their own, and the two are not interchangeable in f64: a reset large enough to swallow a
/// later one in an isolated sum still leaves it visible once the first difference is folded
/// in first.
fn add_counter_resets(result: f64, values: &[f64]) -> f64 {
    values
        .windows(2)
        .filter(|pair| pair[1] < pair[0])
        .fold(result, |result, pair| result + pair[0])
}

/// Positions of the counter resets in a value array, so that a window can accumulate the
/// resets it contains instead of scanning all of its samples.
struct CounterResetIndex<'a> {
    values: &'a [f64],
    /// Ascending indices `i` where `values[i] < values[i - 1]`.
    positions: Vec<usize>,
    /// Slice of `positions` covered by the last window.
    active: Range<usize>,
    /// That window, so the next one can tell whether it advanced.
    previous: Range<usize>,
    /// `positions[active.start]`: the reset a later `start` would drop. `usize::MAX` when the
    /// active slice reaches the end of `positions`.
    drops_at: usize,
    /// `positions[active.end]`: the reset a later `end` would gain, saturated the same way.
    gains_at: usize,
}

impl<'a> CounterResetIndex<'a> {
    fn new(values: &'a [f64]) -> Self {
        let positions: Vec<usize> = (1..values.len())
            .filter(|&i| values[i] < values[i - 1])
            .collect();
        let first = positions.first().copied().unwrap_or(usize::MAX);
        Self {
            values,
            positions,
            active: 0..0,
            previous: 0..0,
            drops_at: first,
            gains_at: first,
        }
    }

    /// Same additions [`add_counter_resets`] performs over `values[start..end]`, in the same
    /// order, reached through the index instead of by scanning the window.
    #[inline]
    fn add_resets(&mut self, result: f64, start: usize, end: usize) -> f64 {
        // The active slice only stays put if the window advanced without reaching either of
        // the resets that bound it.
        if start < self.previous.start
            || end < self.previous.end
            || start >= self.drops_at
            || end > self.gains_at
        {
            self.locate(start, end);
        }
        self.previous = start..end;

        if self.active.start == self.active.end {
            // A counter that has not reset inside this window, which is the normal case, would
            // otherwise pay a range bounds check and an empty iterator for nothing.
            return result;
        }

        let values = self.values;
        self.positions[self.active.start..self.active.end]
            .iter()
            .fold(result, |result, &i| result + values[i - 1])
    }

    fn locate(&mut self, start: usize, end: usize) {
        // Walk the bounds forward from the previous window and only search when they move
        // back. On a series that resets often the searches cost more than the additions they
        // locate, because they run deep and a window holds a handful of resets.
        let (left, right) = if start < self.previous.start || end < self.previous.end {
            (
                self.positions.partition_point(|&i| i <= start),
                self.positions.partition_point(|&i| i < end),
            )
        } else {
            let mut left = self.active.start;
            while left < self.positions.len() && self.positions[left] <= start {
                left += 1;
            }
            let mut right = self.active.end.max(left);
            while right < self.positions.len() && self.positions[right] < end {
                right += 1;
            }
            (left, right)
        };
        self.active = left..right;
        self.drops_at = self.positions.get(left).copied().unwrap_or(usize::MAX);
        self.gains_at = self.positions.get(right).copied().unwrap_or(usize::MAX);
    }
}

fn extract_eval_timestamps(
    columnar_value: &ColumnarValue,
    func_name: &str,
) -> DfResult<TimestampMillisecondArray> {
    let array = extract_array(columnar_value)?;
    let timestamps = array
        .as_any()
        .downcast_ref::<TimestampMillisecondArray>()
        .ok_or_else(|| {
            DataFusionError::Execution(format!(
                "{func_name}: expect evaluation timestamp vector as Timestamp(Millisecond), found {}",
                array.data_type()
            ))
        })?;
    Ok(timestamps.clone())
}

// delta
impl ExtrapolatedRate<false, false> {
    pub const fn name() -> &'static str {
        "prom_delta"
    }

    pub fn scalar_udf() -> ScalarUDF {
        Self::scalar_udf_with_name(Self::name())
    }
}

// rate
impl ExtrapolatedRate<true, true> {
    pub const fn name() -> &'static str {
        "prom_rate"
    }

    pub fn scalar_udf() -> ScalarUDF {
        Self::scalar_udf_with_name(Self::name())
    }
}

// increase
impl ExtrapolatedRate<true, false> {
    pub const fn name() -> &'static str {
        "prom_increase"
    }

    pub fn scalar_udf() -> ScalarUDF {
        Self::scalar_udf_with_name(Self::name())
    }
}

impl Display for ExtrapolatedRate<false, false> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("PromQL Delta Function")
    }
}

impl Display for ExtrapolatedRate<true, true> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("PromQL Rate Function")
    }
}

impl Display for ExtrapolatedRate<true, false> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("PromQL Increase Function")
    }
}

#[cfg(test)]
mod test {

    use datafusion::arrow::array::ArrayRef;
    use datafusion_common::ScalarValue;

    use super::*;

    /// Range length is fixed to 5
    fn extrapolated_rate_runner<const IS_COUNTER: bool, const IS_RATE: bool>(
        ts_range: RangeArray,
        value_range: RangeArray,
        timestamps: ArrayRef,
        expected: Vec<f64>,
    ) {
        let input = vec![
            ColumnarValue::Array(Arc::new(ts_range.into_dict())),
            ColumnarValue::Array(Arc::new(value_range.into_dict())),
            ColumnarValue::Array(timestamps),
            ColumnarValue::Array(Arc::new(Int64Array::from(vec![5]))),
        ];
        let output = extract_array(
            &ExtrapolatedRate::<IS_COUNTER, IS_RATE>::new(5)
                .calc(&input)
                .unwrap(),
        )
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap()
        .values()
        .to_vec();
        assert_eq!(output, expected);
    }

    fn sample_range_inputs() -> (ColumnarValue, ColumnarValue, ColumnarValue) {
        let ts_values = Arc::new(TimestampMillisecondArray::from_iter(
            [1, 2, 3].into_iter().map(Some),
        ));
        let value_values = Arc::new(Float64Array::from_iter([1.0, 2.0, 3.0]));
        let ranges = [(0, 2), (1, 2)];

        let ts_range = RangeArray::from_ranges(ts_values, ranges).unwrap();
        let value_range = RangeArray::from_ranges(value_values, ranges).unwrap();
        let eval_ts = Arc::new(TimestampMillisecondArray::from_iter(
            [2, 3].into_iter().map(Some),
        )) as _;

        (
            ColumnarValue::Array(Arc::new(ts_range.into_dict())),
            ColumnarValue::Array(Arc::new(value_range.into_dict())),
            ColumnarValue::Array(eval_ts),
        )
    }

    /// Evaluates `ranges` as one batch and asserts every window is bit-identical to evaluating
    /// that window on its own, which always takes the direct per-window reduction.
    fn assert_counter_windows_match_single(values: &[f64], ranges: &[(u32, u32)]) {
        let timestamps = Arc::new(TimestampMillisecondArray::from_iter_values(
            (0..values.len()).map(|i| i as i64 * 30_000 + (i % 5) as i64 * 1_000),
        ));
        let values = Arc::new(Float64Array::from(values.to_vec()));
        let evaluate = |ranges: &[(u32, u32)]| {
            let eval_ts = Arc::new(TimestampMillisecondArray::from_iter_values(
                ranges.iter().map(|&(offset, length)| {
                    timestamps.value((offset + length.saturating_sub(1)) as usize) + 5_000
                }),
            ));
            let input = [
                ColumnarValue::Array(Arc::new(
                    RangeArray::from_ranges(timestamps.clone(), ranges.iter().copied())
                        .unwrap()
                        .into_dict(),
                )),
                ColumnarValue::Array(Arc::new(
                    RangeArray::from_ranges(values.clone(), ranges.iter().copied())
                        .unwrap()
                        .into_dict(),
                )),
                ColumnarValue::Array(eval_ts),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(3_600_000))),
            ];
            extract_array(&Rate::new(3_600_000).calc(&input).unwrap())
                .unwrap()
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>()
        };

        for (range, batched) in ranges.iter().zip(evaluate(ranges)) {
            let single = evaluate(std::slice::from_ref(range))[0];
            match (batched, single) {
                (None, None) => {}
                (Some(batched), Some(single)) => assert!(
                    batched.to_bits() == single.to_bits() || (batched.is_nan() && single.is_nan()),
                    "range {range:?}: batched {batched} != single {single}"
                ),
                _ => panic!("range {range:?}: batched {batched:?} != single {single:?}"),
            }
        }
    }

    #[test]
    fn counter_resets_accumulate_into_the_running_result() {
        // Summed on their own, 1e16 and 1.0 round to 1e16, which then cancels against the
        // first sample and reports no increase at all. Folding each reset into `last - first`
        // as Prometheus does keeps the 1.0. Both paths detect the same two resets.
        let ts_array = Arc::new(TimestampMillisecondArray::from_iter(
            [1, 2, 3, 4].into_iter().map(Some),
        ));
        let values_array = Arc::new(Float64Array::from_iter([1e16, 1.0, 0.0, 1.0]));
        let ranges = [(0, 4)];
        let ts_range = RangeArray::from_ranges(ts_array, ranges).unwrap();
        let value_range = RangeArray::from_ranges(values_array, ranges).unwrap();
        let timestamps = Arc::new(TimestampMillisecondArray::from_iter([Some(4)])) as _;

        extrapolated_rate_runner::<true, false>(
            ts_range,
            value_range,
            timestamps,
            vec![1.1666666666666667],
        );
    }

    #[test]
    fn counter_correction_survives_huge_and_infinite_resets() {
        // Both series reset twice inside the first window and once inside the second, but the
        // first reset is large enough to swallow the second one when they are summed together.
        for values in [
            vec![1e16, 1.0, 0.0, 1.0, 2.0],
            vec![f64::INFINITY, 1.0, 0.0, 1.0, 2.0],
        ] {
            assert_counter_windows_match_single(&values, &[(0, 4), (1, 4)]);
        }
    }

    #[test]
    fn counter_correction_matches_single_window_on_irregular_layouts() {
        let mut values: Vec<f64> = (0..512).map(|i| (i % 37) as f64 * 0.25).collect();
        values[20] = f64::NAN;
        values[70] = f64::INFINITY;
        values[140] = f64::NEG_INFINITY;
        values[220] = 1e300;
        values[221] = 1e-200;

        let mut ranges: Vec<(u32, u32)> = (0..390).map(|i| (i, 120)).collect();
        // Empty, too-short, backward and disjoint windows all break a forward-only slide.
        ranges.extend([(400, 0), (400, 1), (2, 20), (450, 30), (0, 120)]);
        ranges.extend((0..390).rev().step_by(10).map(|i| (i, 120)));

        assert_counter_windows_match_single(&values, &ranges);
    }

    #[test]
    fn rate_rejects_wrong_input_arity() {
        let err = ExtrapolatedRate::<true, true>::new(5)
            .calc(&[])
            .unwrap_err();

        assert!(err.to_string().contains("should have 4 inputs"));
    }

    #[test]
    fn rate_rejects_non_int64_range_length() {
        let (ts_range, value_range, eval_ts) = sample_range_inputs();

        let err = ExtrapolatedRate::<true, true>::create_function(&[
            ts_range,
            value_range,
            eval_ts,
            ColumnarValue::Scalar(ScalarValue::Float64(Some(5.0))),
        ])
        .unwrap_err();

        assert!(err.to_string().contains("range length type"));
    }

    #[test]
    fn rate_rejects_empty_range_length() {
        let (ts_range, value_range, eval_ts) = sample_range_inputs();

        let err = ExtrapolatedRate::<true, true>::create_function(&[
            ts_range,
            value_range,
            eval_ts,
            ColumnarValue::Array(Arc::new(Int64Array::from(Vec::<i64>::new()))),
        ])
        .unwrap_err();

        assert!(err.to_string().contains("range length must contain"));
    }

    #[test]
    fn rate_rejects_null_range_length() {
        let (ts_range, value_range, eval_ts) = sample_range_inputs();

        let err = ExtrapolatedRate::<true, true>::create_function(&[
            ts_range,
            value_range,
            eval_ts,
            ColumnarValue::Array(Arc::new(Int64Array::from(vec![None]))),
        ])
        .unwrap_err();

        assert!(err.to_string().contains("range length must contain"));
    }

    #[test]
    fn increase_abnormal_input() {
        let ts_array = Arc::new(TimestampMillisecondArray::from_iter(
            [1, 2, 3, 4, 5, 6, 7, 8, 9].into_iter().map(Some),
        ));
        let values_array = Arc::new(Float64Array::from_iter([
            1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0,
        ]));
        let ranges = [(0, 2), (0, 5), (1, 1), (3, 3), (8, 1), (9, 0)];
        let ts_range = RangeArray::from_ranges(ts_array, ranges).unwrap();
        let value_range = RangeArray::from_ranges(values_array, ranges).unwrap();
        let timestamps = Arc::new(TimestampMillisecondArray::from_iter([
            Some(2),
            Some(5),
            Some(2),
            Some(6),
            Some(9),
            None,
        ])) as _;
        extrapolated_rate_runner::<true, false>(
            ts_range,
            value_range,
            timestamps,
            vec![2.0, 5.0, 0.0, 2.5, 0.0, 0.0],
        );
    }

    #[test]
    fn increase_normal_input() {
        let ts_array = Arc::new(TimestampMillisecondArray::from_iter(
            [1, 2, 3, 4, 5, 6, 7, 8, 9].into_iter().map(Some),
        ));
        let values_array = Arc::new(Float64Array::from_iter([
            1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0,
        ]));
        let ranges = [
            (0, 2),
            (1, 2),
            (2, 2),
            (3, 2),
            (4, 2),
            (5, 2),
            (6, 2),
            (7, 2),
        ];
        let ts_range = RangeArray::from_ranges(ts_array, ranges).unwrap();
        let value_range = RangeArray::from_ranges(values_array, ranges).unwrap();
        let timestamps = Arc::new(TimestampMillisecondArray::from_iter(
            [2, 3, 4, 5, 6, 7, 8, 9].into_iter().map(Some),
        )) as _;
        extrapolated_rate_runner::<true, false>(
            ts_range,
            value_range,
            timestamps,
            // `2.0` is because that `duration_to_zero` less than `extrapolation_threshold`
            vec![2.0, 1.5, 1.5, 1.5, 1.5, 1.5, 1.5, 1.5],
        );
    }

    #[test]
    fn increase_short_input() {
        let ts_array = Arc::new(TimestampMillisecondArray::from_iter(
            [1, 2, 3, 4, 5, 6, 7, 8, 9].into_iter().map(Some),
        ));
        let values_array = Arc::new(Float64Array::from_iter([
            1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0,
        ]));
        let ranges = [
            (0, 1),
            (1, 0),
            (2, 1),
            (3, 0),
            (4, 3),
            (5, 1),
            (6, 0),
            (7, 2),
        ];
        let ts_range = RangeArray::from_ranges(ts_array, ranges).unwrap();
        let value_range = RangeArray::from_ranges(values_array, ranges).unwrap();
        let timestamps = Arc::new(TimestampMillisecondArray::from_iter([
            Some(1),
            None,
            Some(3),
            None,
            Some(7),
            Some(6),
            None,
            Some(9),
        ])) as _;
        extrapolated_rate_runner::<true, false>(
            ts_range,
            value_range,
            timestamps,
            vec![0.0, 0.0, 0.0, 0.0, 2.5, 0.0, 0.0, 1.5],
        );
    }

    #[test]
    fn increase_counter_reset() {
        let ts_array = Arc::new(TimestampMillisecondArray::from_iter(
            [1, 2, 3, 4, 5, 6, 7, 8, 9].into_iter().map(Some),
        ));
        // this series should be treated like [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0]
        let values_array = Arc::new(Float64Array::from_iter([
            1.0, 2.0, 3.0, 4.0, 1.0, 2.0, 3.0, 4.0, 5.0,
        ]));
        let ranges = [
            (0, 2),
            (1, 2),
            (2, 2),
            (3, 2),
            (4, 2),
            (5, 2),
            (6, 2),
            (7, 2),
        ];
        let ts_range = RangeArray::from_ranges(ts_array, ranges).unwrap();
        let value_range = RangeArray::from_ranges(values_array, ranges).unwrap();
        let timestamps = Arc::new(TimestampMillisecondArray::from_iter(
            [2, 3, 4, 5, 6, 7, 8, 9].into_iter().map(Some),
        )) as _;
        extrapolated_rate_runner::<true, false>(
            ts_range,
            value_range,
            timestamps,
            // that two `2.0` is because `duration_to_start` are shrunk to
            // `duration_to_zero`, and causes `duration_to_zero` less than
            // `extrapolation_threshold`.
            vec![2.0, 1.5, 1.5, 1.5, 2.0, 1.5, 1.5, 1.5],
        );
    }

    #[test]
    fn increase_counter_reset_wide_windows() {
        let ts_array = Arc::new(TimestampMillisecondArray::from_iter(
            [1, 2, 3, 4, 5, 6, 7].into_iter().map(Some),
        ));
        let values_array = Arc::new(Float64Array::from_iter([1.0, 2.0, 3.0, 1.0, 2.0, 1.0, 2.0]));
        let ranges = [(0, 4), (1, 4), (2, 4), (3, 4)];
        let ts_range = RangeArray::from_ranges(ts_array, ranges).unwrap();
        let value_range = RangeArray::from_ranges(values_array, ranges).unwrap();
        let timestamps = Arc::new(TimestampMillisecondArray::from_iter(
            [4, 5, 6, 7].into_iter().map(Some),
        )) as _;
        extrapolated_rate_runner::<true, false>(
            ts_range,
            value_range,
            timestamps,
            vec![4.0, 3.5, 3.5, 4.0],
        );
    }

    #[test]
    fn rate_rejects_non_array_timestamp_ranges() {
        let value_values = Arc::new(Float64Array::from_iter([1.0, 2.0]));
        let value_range = RangeArray::from_ranges(value_values, [(0, 2)]).unwrap();
        let eval_ts = Arc::new(TimestampMillisecondArray::from_iter([Some(2)]));

        let err = ExtrapolatedRate::<true, true>::new(5)
            .calc(&[
                ColumnarValue::Scalar(ScalarValue::Int64(Some(0))),
                ColumnarValue::Array(Arc::new(value_range.into_dict())),
                ColumnarValue::Array(eval_ts),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(5))),
            ])
            .unwrap_err();

        assert!(err.to_string().contains("timestamp range vector"));
    }

    #[test]
    fn rate_rejects_non_timestamp_timestamp_range_values() {
        let ts_values = Arc::new(Int64Array::from_iter([1, 2]));
        let value_values = Arc::new(Float64Array::from_iter([1.0, 2.0]));
        let ts_range = RangeArray::from_ranges(ts_values, [(0, 2)]).unwrap();
        let value_range = RangeArray::from_ranges(value_values, [(0, 2)]).unwrap();
        let eval_ts = Arc::new(TimestampMillisecondArray::from_iter([Some(2)]));

        let err = ExtrapolatedRate::<true, true>::new(5)
            .calc(&[
                ColumnarValue::Array(Arc::new(ts_range.into_dict())),
                ColumnarValue::Array(Arc::new(value_range.into_dict())),
                ColumnarValue::Array(eval_ts),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(5))),
            ])
            .unwrap_err();

        assert!(err.to_string().contains("values of type Timestamp"));
    }

    #[test]
    fn rate_rejects_non_float_value_range_values() {
        let ts_values = Arc::new(TimestampMillisecondArray::from_iter(
            [1, 2].into_iter().map(Some),
        ));
        let value_values = Arc::new(Int64Array::from_iter([1, 2]));
        let ts_range = RangeArray::from_ranges(ts_values, [(0, 2)]).unwrap();
        let value_range = RangeArray::from_ranges(value_values, [(0, 2)]).unwrap();
        let eval_ts = Arc::new(TimestampMillisecondArray::from_iter([Some(2)]));

        let err = ExtrapolatedRate::<true, true>::new(5)
            .calc(&[
                ColumnarValue::Array(Arc::new(ts_range.into_dict())),
                ColumnarValue::Array(Arc::new(value_range.into_dict())),
                ColumnarValue::Array(eval_ts),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(5))),
            ])
            .unwrap_err();

        assert!(
            err.to_string()
                .contains("value range vector values of type Float64")
        );
    }

    #[test]
    fn rate_rejects_mismatched_range_counts() {
        let ts_values = Arc::new(TimestampMillisecondArray::from_iter(
            [1, 2, 3].into_iter().map(Some),
        ));
        let value_values = Arc::new(Float64Array::from_iter([1.0, 2.0, 3.0]));
        let ts_range = RangeArray::from_ranges(ts_values, [(0, 2), (1, 2)]).unwrap();
        let value_range = RangeArray::from_ranges(value_values, [(0, 2)]).unwrap();
        let eval_ts = Arc::new(TimestampMillisecondArray::from_iter(
            [2, 3].into_iter().map(Some),
        ));

        let err = ExtrapolatedRate::<true, true>::new(5)
            .calc(&[
                ColumnarValue::Array(Arc::new(ts_range.into_dict())),
                ColumnarValue::Array(Arc::new(value_range.into_dict())),
                ColumnarValue::Array(eval_ts),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(5))),
            ])
            .unwrap_err();

        assert!(err.to_string().contains("same number of windows"));
    }

    #[test]
    fn rate_rejects_mismatched_range_layouts() {
        let ts_values = Arc::new(TimestampMillisecondArray::from_iter(
            [1, 2, 3, 4].into_iter().map(Some),
        ));
        let value_values = Arc::new(Float64Array::from_iter([1.0, 2.0, 3.0, 4.0]));
        let ts_range = RangeArray::from_ranges(ts_values, [(0, 2), (1, 2)]).unwrap();
        let value_range = RangeArray::from_ranges(value_values, [(0, 2), (2, 2)]).unwrap();
        let eval_ts = Arc::new(TimestampMillisecondArray::from_iter(
            [2, 4].into_iter().map(Some),
        ));

        let err = ExtrapolatedRate::<true, true>::new(5)
            .calc(&[
                ColumnarValue::Array(Arc::new(ts_range.into_dict())),
                ColumnarValue::Array(Arc::new(value_range.into_dict())),
                ColumnarValue::Array(eval_ts),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(5))),
            ])
            .unwrap_err();

        assert!(err.to_string().contains("same window layout"));
    }

    #[test]
    fn rate_rejects_non_timestamp_eval_vector() {
        let (ts_range, value_range, _) = sample_range_inputs();

        let err = ExtrapolatedRate::<true, true>::new(5)
            .calc(&[
                ts_range,
                value_range,
                ColumnarValue::Array(Arc::new(Float64Array::from_iter([2.0, 3.0]))),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(5))),
            ])
            .unwrap_err();

        assert!(err.to_string().contains("evaluation timestamp vector"));
    }

    #[test]
    fn rate_rejects_mismatched_eval_timestamp_rows() {
        let (ts_range, value_range, _) = sample_range_inputs();

        let err = ExtrapolatedRate::<true, true>::new(5)
            .calc(&[
                ts_range,
                value_range,
                ColumnarValue::Array(Arc::new(TimestampMillisecondArray::from_iter([Some(2)]))),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(5))),
            ])
            .unwrap_err();

        assert!(err.to_string().contains("same number of rows"));
    }

    #[test]
    fn rate_counter_reset() {
        let ts_array = Arc::new(TimestampMillisecondArray::from_iter(
            [1, 2, 3, 4, 5, 6, 7, 8, 9].into_iter().map(Some),
        ));
        // this series should be treated like [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0]
        let values_array = Arc::new(Float64Array::from_iter([
            1.0, 2.0, 3.0, 4.0, 1.0, 2.0, 3.0, 4.0, 5.0,
        ]));
        let ranges = [
            (0, 2),
            (1, 2),
            (2, 2),
            (3, 2),
            (4, 2),
            (5, 2),
            (6, 2),
            (7, 2),
        ];
        let ts_range = RangeArray::from_ranges(ts_array, ranges).unwrap();
        let value_range = RangeArray::from_ranges(values_array, ranges).unwrap();
        let timestamps = Arc::new(TimestampMillisecondArray::from_iter(
            [2, 3, 4, 5, 6, 7, 8, 9].into_iter().map(Some),
        )) as _;
        extrapolated_rate_runner::<true, true>(
            ts_range,
            value_range,
            timestamps,
            vec![400.0, 300.0, 300.0, 300.0, 400.0, 300.0, 300.0, 300.0],
        );
    }

    #[test]
    fn rate_normal_input() {
        let ts_array = Arc::new(TimestampMillisecondArray::from_iter(
            [1, 2, 3, 4, 5, 6, 7, 8, 9].into_iter().map(Some),
        ));
        let values_array = Arc::new(Float64Array::from_iter([
            1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0,
        ]));
        let ranges = [
            (0, 2),
            (1, 2),
            (2, 2),
            (3, 2),
            (4, 2),
            (5, 2),
            (6, 2),
            (7, 2),
        ];
        let ts_range = RangeArray::from_ranges(ts_array, ranges).unwrap();
        let value_range = RangeArray::from_ranges(values_array, ranges).unwrap();
        let timestamps = Arc::new(TimestampMillisecondArray::from_iter(
            [2, 3, 4, 5, 6, 7, 8, 9].into_iter().map(Some),
        )) as _;
        extrapolated_rate_runner::<true, true>(
            ts_range,
            value_range,
            timestamps,
            vec![400.0, 300.0, 300.0, 300.0, 300.0, 300.0, 300.0, 300.0],
        );
    }

    #[test]
    fn delta_counter_reset() {
        let ts_array = Arc::new(TimestampMillisecondArray::from_iter(
            [1, 2, 3, 4, 5, 6, 7, 8, 9].into_iter().map(Some),
        ));
        // this series should be treated like [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0]
        let values_array = Arc::new(Float64Array::from_iter([
            1.0, 2.0, 3.0, 4.0, 1.0, 2.0, 3.0, 4.0, 5.0,
        ]));
        let ranges = [
            (0, 2),
            (1, 2),
            (2, 2),
            (3, 2),
            (4, 2),
            (5, 2),
            (6, 2),
            (7, 2),
        ];
        let ts_range = RangeArray::from_ranges(ts_array, ranges).unwrap();
        let value_range = RangeArray::from_ranges(values_array, ranges).unwrap();
        let timestamps = Arc::new(TimestampMillisecondArray::from_iter(
            [2, 3, 4, 5, 6, 7, 8, 9].into_iter().map(Some),
        )) as _;
        extrapolated_rate_runner::<false, false>(
            ts_range,
            value_range,
            timestamps,
            // delta doesn't handle counter reset, thus there is a negative value
            vec![1.5, 1.5, 1.5, -4.5, 1.5, 1.5, 1.5, 1.5],
        );
    }

    #[test]
    fn delta_normal_input() {
        let ts_array = Arc::new(TimestampMillisecondArray::from_iter(
            [1, 2, 3, 4, 5, 6, 7, 8, 9].into_iter().map(Some),
        ));
        let values_array = Arc::new(Float64Array::from_iter([
            1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0,
        ]));
        let ranges = [
            (0, 2),
            (1, 2),
            (2, 2),
            (3, 2),
            (4, 2),
            (5, 2),
            (6, 2),
            (7, 2),
        ];
        let ts_range = RangeArray::from_ranges(ts_array, ranges).unwrap();
        let value_range = RangeArray::from_ranges(values_array, ranges).unwrap();
        let timestamps = Arc::new(TimestampMillisecondArray::from_iter(
            [2, 3, 4, 5, 6, 7, 8, 9].into_iter().map(Some),
        )) as _;
        extrapolated_rate_runner::<false, false>(
            ts_range,
            value_range,
            timestamps,
            vec![1.5, 1.5, 1.5, 1.5, 1.5, 1.5, 1.5, 1.5],
        );
    }
}
