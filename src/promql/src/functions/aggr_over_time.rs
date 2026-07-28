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
use datafusion_expr::create_udf;
use datatypes::arrow::array::Array;
use datatypes::arrow::compute;
use datatypes::arrow::datatypes::DataType;

use crate::functions::{compensated_sum_inc, extract_array};
use crate::range_array::RangeArray;

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
#[allow(dead_code)]
pub fn count_over_time(_: &TimestampMillisecondArray, values: &Float64Array) -> Option<f64> {
    if values.is_empty() {
        None
    } else {
        Some(values.len() as f64)
    }
}

#[derive(Debug)]
pub struct CountOverTime {}

impl CountOverTime {
    pub const fn name() -> &'static str {
        "prom_count_over_time"
    }

    pub fn scalar_udf() -> ScalarUDF {
        create_udf(
            Self::name(),
            Self::input_type(),
            Self::return_type(),
            Volatility::Volatile,
            Arc::new(Self::calc) as _,
        )
    }

    fn input_type() -> Vec<DataType> {
        vec![
            RangeArray::convert_data_type(
                TimestampMillisecondArray::new_null(0).data_type().clone(),
            ),
            RangeArray::convert_data_type(Float64Array::new_null(0).data_type().clone()),
        ]
    }

    fn return_type() -> DataType {
        Float64Array::new_null(0).data_type().clone()
    }

    fn calc(input: &[ColumnarValue]) -> Result<ColumnarValue, DataFusionError> {
        assert_eq!(input.len(), 2);
        crate::functions::rolling::count::calc(input, Self::name())
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
    use datafusion::arrow::buffer::NullBuffer;
    use datafusion_common::config::ConfigOptions;
    use datafusion_expr::ScalarFunctionArgs;
    use datatypes::arrow::datatypes::Field;

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

    fn count_over_time_oracle(ranges: &[(u32, u32)]) -> Vec<Option<f64>> {
        ranges
            .iter()
            .map(|&(_, length)| (length != 0).then_some(f64::from(length)))
            .collect()
    }

    fn invoke_count_over_time(
        udf: &ScalarUDF,
        timestamps: RangeArray,
        values: RangeArray,
    ) -> Result<Float64Array, DataFusionError> {
        let number_rows = timestamps.len();
        let args = vec![
            ColumnarValue::Array(Arc::new(timestamps.into_dict())),
            ColumnarValue::Array(Arc::new(values.into_dict())),
        ];
        let arg_fields = vec![
            Arc::new(Field::new("timestamps", args[0].data_type(), false)),
            Arc::new(Field::new("values", args[1].data_type(), false)),
        ];
        let result = udf.invoke_with_args(ScalarFunctionArgs {
            args,
            arg_fields,
            number_rows,
            return_field: Arc::new(Field::new("result", DataType::Float64, false)),
            config_options: Arc::new(ConfigOptions::default()),
        })?;
        let result = extract_array(&result)?;

        Ok(result
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("count_over_time must return a Float64Array")
            .clone())
    }

    fn assert_count_over_time_result(actual: &Float64Array, expected: &[Option<f64>]) {
        assert_eq!(actual.len(), expected.len());
        for (index, expected) in expected.iter().enumerate() {
            match expected {
                Some(expected) => {
                    assert!(actual.is_valid(index), "row {index} should be valid");
                    assert_eq!(
                        actual.value(index).to_bits(),
                        expected.to_bits(),
                        "row {index}"
                    );
                }
                None => assert!(!actual.is_valid(index), "row {index} should be null"),
            }
        }
    }

    fn timestamp_backing(len: usize) -> Arc<TimestampMillisecondArray> {
        Arc::new(TimestampMillisecondArray::from_iter(
            (0..len).map(|index| Some(index as i64)),
        ))
    }

    fn run_count_over_time_case(
        udf: &ScalarUDF,
        timestamps: Arc<TimestampMillisecondArray>,
        values: Arc<Float64Array>,
        timestamp_ranges: &[(u32, u32)],
        value_ranges: &[(u32, u32)],
    ) {
        let expected = count_over_time_oracle(value_ranges);
        let timestamps =
            RangeArray::from_ranges(timestamps, timestamp_ranges.iter().copied()).unwrap();
        let values = RangeArray::from_ranges(values, value_ranges.iter().copied()).unwrap();
        let actual = invoke_count_over_time(udf, timestamps, values).unwrap();

        assert_count_over_time_result(&actual, &expected);
    }

    fn assert_execution_error(error: DataFusionError, expected: &str) {
        match error {
            DataFusionError::Execution(message) => assert_eq!(message, expected),
            other => panic!("expected execution error, got {other:?}"),
        }
    }

    #[test]
    fn count_over_time_freezes_physical_slot_semantics() {
        const RANGES: &[(u32, u32)] = &[
            (0, 0), // empty
            (0, 1), // finite
            (1, 1), // NaN
            (2, 1), // positive infinity
            (3, 1), // negative infinity
            (3, 3), // mixed valid and null slots
            (4, 2), // all null slots
            (4, 1), // raw nonzero value under a null slot
        ];

        let values = Arc::new(Float64Array::new(
            vec![
                1.25,
                f64::from_bits(0x7ff8_0000_0000_0000),
                f64::INFINITY,
                f64::NEG_INFINITY,
                42.0,
                -7.0,
            ]
            .into(),
            Some(NullBuffer::from(vec![true, true, true, true, false, false])),
        ));
        assert!(!values.is_valid(4));
        assert_eq!(values.value(4).to_bits(), 42.0f64.to_bits());

        let udf = CountOverTime::scalar_udf();
        run_count_over_time_case(
            &udf,
            timestamp_backing(values.len()),
            values,
            RANGES,
            RANGES,
        );
    }

    #[test]
    fn count_over_time_freezes_dense_and_arbitrary_layouts() {
        const DENSE_SLIDING_RANGES: &[(u32, u32)] =
            &[(1, 2), (2, 2), (3, 2), (4, 2), (5, 2), (6, 2), (8, 0)];
        const ARBITRARY_RANGES: &[(u32, u32)] = &[
            (5, 2),
            (0, 3),
            (3, 0),
            (1, 4),
            (1, 4),
            (7, 1),
            (3, 2),
            (8, 0),
        ];

        let values = Arc::new(Float64Array::from_iter((0..8).map(|value| value as f64)));
        let udf = CountOverTime::scalar_udf();

        run_count_over_time_case(
            &udf,
            timestamp_backing(values.len()),
            values.clone(),
            DENSE_SLIDING_RANGES,
            DENSE_SLIDING_RANGES,
        );
        run_count_over_time_case(
            &udf,
            timestamp_backing(values.len()),
            values,
            ARBITRARY_RANGES,
            ARBITRARY_RANGES,
        );
    }

    #[test]
    fn count_over_time_ignores_timestamp_offsets_and_validity_after_shape_validation() {
        const TIMESTAMP_RANGES: &[(u32, u32)] = &[(2, 2), (4, 1), (7, 0), (5, 3)];
        const VALUE_RANGES: &[(u32, u32)] = &[(1, 2), (5, 1), (4, 0), (2, 3)];

        let timestamps = Arc::new(TimestampMillisecondArray::new(
            vec![100, 200, 300, 400, 500, 600, 700, 800].into(),
            Some(NullBuffer::from(vec![
                true, true, false, true, true, true, true, true,
            ])),
        ));
        assert!(!timestamps.is_valid(2));
        assert_eq!(timestamps.value(2), 300);

        let values = Arc::new(Float64Array::from_iter((0..8).map(|value| value as f64)));
        let udf = CountOverTime::scalar_udf();
        run_count_over_time_case(&udf, timestamps, values, TIMESTAMP_RANGES, VALUE_RANGES);
    }

    #[test]
    fn count_over_time_freezes_zero_window_outer_invocation() {
        const NO_RANGES: &[(u32, u32)] = &[];

        let values = Arc::new(Float64Array::from_iter([1.0, 2.0, 3.0]));
        let udf = CountOverTime::scalar_udf();
        run_count_over_time_case(
            &udf,
            timestamp_backing(values.len()),
            values,
            NO_RANGES,
            NO_RANGES,
        );
    }

    #[test]
    fn count_over_time_same_udf_resets_between_invocations() {
        const FIRST_RANGES: &[(u32, u32)] = &[(0, 3), (3, 0), (4, 2)];
        const SECOND_RANGES: &[(u32, u32)] = &[(1, 1), (4, 3), (0, 0), (2, 2)];

        let udf = CountOverTime::scalar_udf();
        let first_values = Arc::new(Float64Array::from_iter([
            10.0, 20.0, 30.0, 40.0, 50.0, 60.0,
        ]));
        run_count_over_time_case(
            &udf,
            timestamp_backing(first_values.len()),
            first_values,
            FIRST_RANGES,
            FIRST_RANGES,
        );

        let second_values = Arc::new(Float64Array::from_iter([
            f64::NAN,
            f64::NEG_INFINITY,
            3.0,
            4.0,
            f64::INFINITY,
            6.0,
            7.0,
        ]));
        run_count_over_time_case(
            &udf,
            timestamp_backing(second_values.len()),
            second_values,
            SECOND_RANGES,
            SECOND_RANGES,
        );
    }

    #[test]
    fn count_over_time_preserves_range_shape_errors() {
        let udf = CountOverTime::scalar_udf();
        let timestamps = RangeArray::from_ranges(timestamp_backing(2), [(0, 1), (1, 1)]).unwrap();
        let values =
            RangeArray::from_ranges(Arc::new(Float64Array::from_iter([1.0, 2.0])), [(0, 1)])
                .unwrap();
        let error = invoke_count_over_time(&udf, timestamps, values).unwrap_err();
        assert_execution_error(
            error,
            "RangeArray have different lengths in PromQL function prom_count_over_time: array1=2, array2=1",
        );

        let timestamps = RangeArray::from_ranges(timestamp_backing(2), [(0, 2)]).unwrap();
        let values =
            RangeArray::from_ranges(Arc::new(Float64Array::from_iter([1.0, 2.0])), [(0, 1)])
                .unwrap();
        let error = invoke_count_over_time(&udf, timestamps, values).unwrap_err();
        assert_execution_error(
            error,
            "RangeArray's element 0 have different lengths in PromQL function prom_count_over_time: array1=2, array2=1",
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
