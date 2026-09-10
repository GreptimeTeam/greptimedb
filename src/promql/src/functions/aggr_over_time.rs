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

use common_macro::range_fn;
use datafusion::arrow::array::{Float64Array, TimestampMillisecondArray};
use datafusion::common::DataFusionError;
use datafusion::logical_expr::{ScalarUDF, Volatility};
use datafusion::physical_plan::ColumnarValue;
use datatypes::arrow::array::Array;
use datatypes::arrow::compute;
use datatypes::arrow::datatypes::DataType;

use crate::functions::{compensated_sum_inc, extract_array};
use crate::range_array::RangeArray;

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
    timestamp_ranges
        .values()
        .as_any()
        .downcast_ref::<TimestampMillisecondArray>()
        .unwrap();
    let values = value_ranges
        .values()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    let evaluator: fn(&Float64Array, usize, usize) -> Option<f64> = match operation {
        PresenceEvaluator::Count => |_, _, length| (length != 0).then_some(length as f64),
        PresenceEvaluator::Last => {
            |values, offset, length| (length != 0).then(|| values.value(offset + length - 1))
        }
        PresenceEvaluator::Absent => |_, _, length| (length == 0).then_some(1.0),
        PresenceEvaluator::Present => |_, _, length| (length != 0).then_some(1.0),
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
        result.push(evaluator(values, value_offset, value_length));
    }

    Ok(ColumnarValue::Array(Arc::new(Float64Array::from_iter(
        result,
    ))))
}

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
#[range_fn(
    name = MinOverTime,
    ret = Float64Array,
    display_name = prom_min_over_time
)]
pub fn min_over_time(_: &TimestampMillisecondArray, values: &Float64Array) -> Option<f64> {
    let mut valid_values = values.iter().flatten();
    let mut min = valid_values.next()?;
    for value in valid_values {
        if value < min || min.is_nan() {
            min = value;
        }
    }
    Some(min)
}

/// The maximum value of all points in the specified interval.
#[range_fn(
    name = MaxOverTime,
    ret = Float64Array,
    display_name = prom_max_over_time
)]
pub fn max_over_time(_: &TimestampMillisecondArray, values: &Float64Array) -> Option<f64> {
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
    display_name = prom_last_over_time,
    evaluator = last_over_time_evaluator
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
    display_name = prom_absent_over_time,
    evaluator = absent_over_time_evaluator
)]
pub fn absent_over_time(_: &TimestampMillisecondArray, values: &Float64Array) -> Option<f64> {
    if values.is_empty() { Some(1.0) } else { None }
}

/// the value 1 for any series in the specified interval.
#[range_fn(
    name = PresentOverTime,
    ret = Float64Array,
    display_name = prom_present_over_time,
    evaluator = present_over_time_evaluator
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
    use crate::functions::test_util::simple_range_udf_runner;

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
}
