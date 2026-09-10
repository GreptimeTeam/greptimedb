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

/// The average value of all points in the specified interval.
#[range_fn(
    name = AvgOverTime,
    ret = Float64Array,
    display_name = prom_avg_over_time
)]
pub fn avg_over_time(_: &TimestampMillisecondArray, values: &Float64Array) -> Option<f64> {
    compute::sum(values).map(|result| result / values.len() as f64)
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
    let mut extrema = VecDeque::new();
    let mut latest_nan = None;
    let mut previous_window = None;
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
        let append_start = match previous_window {
            Some((previous_start, previous_end))
                if start >= previous_start && end >= previous_end =>
            {
                while extrema
                    .front()
                    .is_some_and(|&value_index| value_index < start)
                {
                    extrema.pop_front();
                }
                if latest_nan.is_some_and(|value_index| value_index < start) {
                    latest_nan = None;
                }
                previous_end.max(start)
            }
            Some(_) | None => {
                extrema.clear();
                latest_nan = None;
                start
            }
        };
        append_min_max_values(
            values,
            append_start,
            end,
            is_min,
            &mut extrema,
            &mut latest_nan,
        );

        result.push(
            extrema
                .front()
                .map(|&value_index| values.value(value_index))
                .or_else(|| latest_nan.map(|value_index| values.value(value_index))),
        );
        previous_window = Some((start, end));
    }

    Ok(ColumnarValue::Array(Arc::new(Float64Array::from_iter(
        result,
    ))))
}

fn append_min_max_values(
    values: &Float64Array,
    start: usize,
    end: usize,
    is_min: bool,
    extrema: &mut VecDeque<usize>,
    latest_nan: &mut Option<usize>,
) {
    for index in start..end {
        if values.is_null(index) {
            continue;
        }

        let value = values.value(index);
        if value.is_nan() {
            // Keep the latest payload while no non-NaN value is in the window.
            *latest_nan = Some(index);
            continue;
        }

        while let Some(&tail_index) = extrema.back() {
            let tail_value = values.value(tail_index);
            // Strict comparison retains the first equal value, including signed zero.
            if if is_min {
                value < tail_value
            } else {
                value > tail_value
            } {
                extrema.pop_back();
            } else {
                break;
            }
        }
        extrema.push_back(index);
    }
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
    display_name = prom_count_over_time
)]
pub fn count_over_time(_: &TimestampMillisecondArray, values: &Float64Array) -> Option<f64> {
    if values.is_empty() {
        None
    } else {
        Some(values.len() as f64)
    }
}

/// The most recent point value in specified interval.
#[range_fn(
    name = LastOverTime,
    ret = Float64Array,
    display_name = prom_last_over_time
)]
pub fn last_over_time(_: &TimestampMillisecondArray, values: &Float64Array) -> Option<f64> {
    values.values().last().copied()
}

/// absent_over_time returns an empty vector if the range vector passed to it has any
/// elements (floats or native histograms) and a 1-element vector with the value 1 if
/// the range vector passed to it has no elements.
#[range_fn(
    name = AbsentOverTime,
    ret = Float64Array,
    display_name = prom_absent_over_time
)]
pub fn absent_over_time(_: &TimestampMillisecondArray, values: &Float64Array) -> Option<f64> {
    if values.is_empty() { Some(1.0) } else { None }
}

/// the value 1 for any series in the specified interval.
#[range_fn(
    name = PresentOverTime,
    ret = Float64Array,
    display_name = prom_present_over_time
)]
pub fn present_over_time(_: &TimestampMillisecondArray, values: &Float64Array) -> Option<f64> {
    if values.is_empty() { None } else { Some(1.0) }
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
    if values.is_empty() {
        None
    } else {
        let mut count = 0;
        let mut mean: f64 = 0.0;
        let mut result: f64 = 0.0;
        for value in values {
            let value = value.unwrap();
            let new_count = count + 1;
            let delta1 = value - mean;
            let new_mean = delta1 / new_count as f64 + mean;
            let delta2 = value - new_mean;
            let new_result = result + delta1 * delta2;

            count += 1;
            mean = new_mean;
            result = new_result;
        }
        Some(result / count as f64)
    }
}

/// the population standard deviation of the values in the specified interval.
/// Prometheus's implementation: <https://github.com/prometheus/prometheus/blob/f55ab2217984770aa1eecd0f2d5f54580029b1c0/promql/functions.go#L556-L569>
#[range_fn(
    name = StddevOverTime,
    ret = Float64Array,
    display_name = prom_stddev_over_time
)]
pub fn stddev_over_time(_: &TimestampMillisecondArray, values: &Float64Array) -> Option<f64> {
    if values.is_empty() {
        None
    } else {
        let mut count = 0.0;
        let mut mean = 0.0;
        let mut comp_mean = 0.0;
        let mut deviations_sum_sq = 0.0;
        let mut comp_deviations_sum_sq = 0.0;
        for v in values {
            count += 1.0;
            let current_value = v.unwrap();
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
        Some(((deviations_sum_sq + comp_deviations_sum_sq) / count).sqrt())
    }
}

#[cfg(test)]
mod test {
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
}
