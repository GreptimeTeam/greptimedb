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

use datafusion::arrow::array::{Float64Array, TimestampMillisecondArray};
use datafusion::arrow::datatypes::TimeUnit;
use datafusion::common::DataFusionError;
use datafusion::logical_expr::{ScalarUDF, Signature, TypeSignature, Volatility};
use datafusion::physical_plan::ColumnarValue;
use datatypes::arrow::array::Array;
use datatypes::arrow::datatypes::DataType;

use crate::extension_plan::Millisecond;
use crate::functions::extract_array;
use crate::range_array::RangeArray;

pub type Delta = ExtrapolatedRate<false, false>;
pub type Rate = ExtrapolatedRate<true, true>;
pub type Increase = ExtrapolatedRate<true, false>;

/// Part of the `extrapolatedRate` in Promql,
/// from <https://github.com/prometheus/prometheus/blob/6bdecf377cea8e856509914f35234e948c4fcb80/promql/functions.go#L66>
#[derive(Debug)]
pub struct ExtrapolatedRate<const IS_COUNTER: bool, const IS_RATE: bool> {
    /// Range duration in millisecond
    range_length: i64,
}

impl<const IS_COUNTER: bool, const IS_RATE: bool> ExtrapolatedRate<IS_COUNTER, IS_RATE> {
    /// Constructor. Other public usage should use [scalar_udf()](ExtrapolatedRate::scalar_udf()) instead.
    fn new(range_length: i64) -> Self {
        Self { range_length }
    }

    fn input_type() -> Vec<DataType> {
        vec![
            // timestamp range vector
            RangeArray::convert_data_type(DataType::Timestamp(TimeUnit::Millisecond, None)),
            // value range vector
            RangeArray::convert_data_type(DataType::Float64),
            // timestamp vector
            DataType::Timestamp(TimeUnit::Millisecond, None),
        ]
    }

    fn return_type() -> DataType {
        DataType::Float64
    }

    fn calc(&self, input: &[ColumnarValue]) -> Result<ColumnarValue, DataFusionError> {
        assert_eq!(input.len(), 3);

        // construct matrix from input
        let ts_array = extract_array(&input[0])?;
        let ts_range = RangeArray::try_new(ts_array.to_data().into())?;
        let value_array = extract_array(&input[1])?;
        let value_range = RangeArray::try_new(value_array.to_data().into())?;
        let ts = extract_array(&input[2])?;
        let ts = ts
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap();

        // calculation
        let mut result_array = Vec::with_capacity(ts_range.len());
        for index in 0..ts_range.len() {
            let timestamps = ts_range.get(index).unwrap();
            let timestamps = timestamps
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .unwrap()
                .values();
            let end_ts = ts.value(index);
            let values = value_range.get(index).unwrap();
            let values = values
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .values();

            if values.len() < 2 {
                result_array.push(None);
                continue;
            }

            // refer to functions.go L83-L110
            let mut result_value = values.last().unwrap() - values.first().unwrap();
            if IS_COUNTER {
                for window in values.windows(2) {
                    let prev = window[0];
                    let curr = window[1];
                    if curr < prev {
                        result_value += prev
                    }
                }
            }

            let mut factor = Self::extrapolate_factor(
                timestamps,
                end_ts,
                self.range_length,
                *values.first().unwrap(),
                result_value,
            );

            if IS_RATE {
                // safety: range_length is checked to be non-zero in the planner.
                factor /= self.range_length as f64 / 1000.0;
            }

            result_array.push(Some(result_value * factor));
        }

        let result = ColumnarValue::Array(Arc::new(Float64Array::from_iter(result_array)));
        Ok(result)
    }

    fn extrapolate_factor(
        timestamps: &[Millisecond],
        range_end: Millisecond,
        range_length: Millisecond,
        // the following two parameters are for counters.
        // see functions.go L121 - L127
        first_value: f64,
        result_value: f64,
    ) -> f64 {
        // result_value
        // refer to functions.go extrapolatedRate fn
        // assume offset is processed (and it should be processed in normalize plan)
        let range_start = range_end - range_length;
        let mut duration_to_start = (timestamps.first().unwrap() - range_start) as f64 / 1000.0;
        let duration_to_end = (range_end - timestamps.last().unwrap()) as f64 / 1000.0;
        let sampled_interval =
            (timestamps.last().unwrap() - timestamps.first().unwrap()) as f64 / 1000.0;
        let average_duration_between_samples = sampled_interval / (timestamps.len() - 1) as f64;

        // functions.go L122 - L134. quote:
        // Counters cannot be negative. If we have any slope at
        // all (i.e. resultValue went up), we can extrapolate
        // the zero point of the counter. If the duration to the
        // zero point is shorter than the durationToStart, we
        // take the zero point as the start of the series,
        // thereby avoiding extrapolation to negative counter
        // values.
        if IS_COUNTER && result_value > 0.0 && first_value >= 0.0 {
            let duration_to_zero = sampled_interval * (first_value / result_value);
            if duration_to_zero < duration_to_start {
                duration_to_start = duration_to_zero;
            }
        }

        let extrapolation_threshold = average_duration_between_samples * 1.1;
        let mut extrapolate_to_interval = sampled_interval;

        if duration_to_start < extrapolation_threshold {
            extrapolate_to_interval += duration_to_start;
        } else {
            extrapolate_to_interval += average_duration_between_samples / 2.0;
        }
        if duration_to_end < extrapolation_threshold {
            extrapolate_to_interval += duration_to_end;
        } else {
            extrapolate_to_interval += average_duration_between_samples / 2.0;
        }

        extrapolate_to_interval / sampled_interval
    }
}

// delta
impl ExtrapolatedRate<false, false> {
    pub fn name() -> &'static str {
        "prom_delta"
    }

    pub fn scalar_udf(range_length: i64) -> ScalarUDF {
        ScalarUDF {
            name: Self::name().to_string(),
            signature: Signature::new(
                TypeSignature::Exact(Self::input_type()),
                Volatility::Immutable,
            ),
            return_type: Arc::new(|_| Ok(Arc::new(Self::return_type()))),
            fun: Arc::new(move |input| Self::new(range_length).calc(input)),
        }
    }
}

// rate
impl ExtrapolatedRate<true, true> {
    pub fn name() -> &'static str {
        "prom_rate"
    }

    pub fn scalar_udf(range_length: i64) -> ScalarUDF {
        ScalarUDF {
            name: Self::name().to_string(),
            signature: Signature::new(
                TypeSignature::Exact(Self::input_type()),
                Volatility::Immutable,
            ),
            return_type: Arc::new(|_| Ok(Arc::new(Self::return_type()))),
            fun: Arc::new(move |input| Self::new(range_length).calc(input)),
        }
    }
}

// increase
impl ExtrapolatedRate<true, false> {
    pub fn name() -> &'static str {
        "prom_increase"
    }

    pub fn scalar_udf(range_length: i64) -> ScalarUDF {
        ScalarUDF {
            name: Self::name().to_string(),
            signature: Signature::new(
                TypeSignature::Exact(Self::input_type()),
                Volatility::Immutable,
            ),
            return_type: Arc::new(|_| Ok(Arc::new(Self::return_type()))),
            fun: Arc::new(move |input| Self::new(range_length).calc(input)),
        }
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
            // that two `2.0` is because `duration_to_start` are shrunk to to
            // `duration_to_zero`, and causes `duration_to_zero` less than
            // `extrapolation_threshold`.
            vec![2.0, 1.5, 1.5, 1.5, 2.0, 1.5, 1.5, 1.5],
        );
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
