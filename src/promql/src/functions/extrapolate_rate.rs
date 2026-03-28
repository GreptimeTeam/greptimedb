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
use std::sync::Arc;

use datafusion::arrow::array::{
    DictionaryArray, Float64Array, Float64Builder, TimestampMillisecondArray,
};
use datafusion::arrow::datatypes::TimeUnit;
use datafusion::common::{DataFusionError, Result as DfResult};
use datafusion::logical_expr::{ScalarUDF, Volatility};
use datafusion::physical_plan::ColumnarValue;
use datafusion_expr::create_udf;
use datatypes::arrow::array::{Array, Int64Array};
use datatypes::arrow::datatypes::{DataType, Int64Type};

use crate::functions::extract_array;
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
            (false, true) => "prom_delta",
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
    /// * 3: range length. Range duration in millisecond. Not used here
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

        let mut counter_correction = 0.0;
        let mut prev_offset = usize::MAX;
        let mut prev_length = 0usize;

        for index in 0..num_windows {
            let (raw_offset, raw_length) = unpack(keys[index]);
            let offset = raw_offset as usize;
            let length = raw_length as usize;

            if length < 2 {
                result_builder.append_null();
                prev_offset = usize::MAX;
                continue;
            }

            let end = offset + length;
            let first_value = all_values[offset];
            let last_value = all_values[end - 1];

            let result_value = if IS_COUNTER {
                if prev_offset != usize::MAX && offset == prev_offset + 1 && length == prev_length {
                    if all_values[prev_offset + 1] < all_values[prev_offset] {
                        counter_correction -= all_values[prev_offset];
                    }
                    if all_values[end - 1] < all_values[end - 2] {
                        counter_correction += all_values[end - 2];
                    }
                } else {
                    counter_correction = 0.0;
                    for pair in all_values[offset..end].windows(2) {
                        if pair[1] < pair[0] {
                            counter_correction += pair[0];
                        }
                    }
                }
                last_value - first_value + counter_correction
            } else {
                last_value - first_value
            };

            prev_offset = offset;
            prev_length = length;

            let first_ts = all_timestamps[offset];
            let last_ts = all_timestamps[end - 1];
            let range_end = eval_ts[index];
            let range_start = range_end - range_length;
            let sampled_interval_ms = (last_ts - first_ts) as f64;
            let average_interval_ms = sampled_interval_ms / (length - 1) as f64;
            let mut duration_to_start_ms = (first_ts - range_start) as f64;
            let duration_to_end_ms = (range_end - last_ts) as f64;

            if IS_COUNTER && result_value > 0.0 && first_value >= 0.0 {
                let duration_to_zero = sampled_interval_ms * (first_value / result_value);
                if duration_to_zero < duration_to_start_ms {
                    duration_to_start_ms = duration_to_zero;
                }
            }

            let extrapolation_threshold = average_interval_ms * 1.1;
            let mut extrapolated_interval_ms = sampled_interval_ms;

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

fn extract_range_dict(
    columnar_value: &ColumnarValue,
    func_name: &str,
    arg_name: &str,
    expected_value_type: &DataType,
) -> DfResult<DictionaryArray<Int64Type>> {
    let array = extract_array(columnar_value)?;
    let dict = array
        .as_any()
        .downcast_ref::<DictionaryArray<Int64Type>>()
        .ok_or_else(|| {
            DataFusionError::Execution(format!(
                "{func_name}: expect {arg_name} as DictionaryArray<Int64>, found {}",
                array.data_type()
            ))
        })?
        .clone();

    if &dict.value_type() != expected_value_type {
        return Err(DataFusionError::Execution(format!(
            "{func_name}: expect {arg_name} values of type {expected_value_type}, found {}",
            dict.value_type()
        )));
    }

    RangeArray::try_new(dict.clone()).map_err(DataFusionError::from)?;
    Ok(dict)
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
