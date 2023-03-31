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

use common_function_macro::range_fn;
use datafusion::arrow::array::{Float64Array, TimestampMillisecondArray};
use datafusion::arrow::datatypes::TimeUnit;
use datafusion::common::DataFusionError;
use datafusion::logical_expr::{ScalarUDF, Signature, TypeSignature, Volatility};
use datafusion::physical_plan::ColumnarValue;
use datatypes::arrow::array::Array;
use datatypes::arrow::compute;
use datatypes::arrow::datatypes::DataType;

use crate::functions::extract_array;
use crate::range_array::RangeArray;

/// The average value of all points in the specified interval.
#[range_fn(
    name = "AvgOverTime",
    ret = "Float64Array",
    display_name = "prom_avg_over_time"
)]
pub fn avg_over_time(_: &TimestampMillisecondArray, values: &Float64Array) -> Option<f64> {
    compute::sum(values).map(|result| result / values.len() as f64)
}

/// The minimum value of all points in the specified interval.
#[range_fn(
    name = "MinOverTime",
    ret = "Float64Array",
    display_name = "prom_min_over_time"
)]
pub fn min_over_time(_: &TimestampMillisecondArray, values: &Float64Array) -> Option<f64> {
    compute::min(values)
}

/// The maximum value of all points in the specified interval.
#[range_fn(
    name = "MaxOverTime",
    ret = "Float64Array",
    display_name = "prom_max_over_time"
)]
pub fn max_over_time(_: &TimestampMillisecondArray, values: &Float64Array) -> Option<f64> {
    compute::max(values)
}

/// The sum of all values in the specified interval.
#[range_fn(
    name = "SumOverTime",
    ret = "Float64Array",
    display_name = "prom_sum_over_time"
)]
pub fn sum_over_time(_: &TimestampMillisecondArray, values: &Float64Array) -> Option<f64> {
    compute::sum(values)
}

/// The count of all values in the specified interval.
#[range_fn(
    name = "CountOverTime",
    ret = "Float64Array",
    display_name = "prom_count_over_time"
)]
pub fn count_over_time(_: &TimestampMillisecondArray, values: &Float64Array) -> f64 {
    values.len() as f64
}

/// The most recent point value in specified interval.
#[range_fn(
    name = "LastOverTime",
    ret = "Float64Array",
    display_name = "prom_last_over_time"
)]
pub fn last_over_time(_: &TimestampMillisecondArray, values: &Float64Array) -> Option<f64> {
    values.values().last().copied()
}

/// absent_over_time returns an empty vector if the range vector passed to it has any
/// elements (floats or native histograms) and a 1-element vector with the value 1 if
/// the range vector passed to it has no elements.
#[range_fn(
    name = "AbsentOverTime",
    ret = "Float64Array",
    display_name = "prom_absent_over_time"
)]
pub fn absent_over_time(_: &TimestampMillisecondArray, values: &Float64Array) -> Option<f64> {
    if values.is_empty() {
        Some(1.0)
    } else {
        None
    }
}

/// the value 1 for any series in the specified interval.
#[range_fn(
    name = "PresentOverTime",
    ret = "Float64Array",
    display_name = "prom_present_over_time"
)]
pub fn present_over_time(_: &TimestampMillisecondArray, values: &Float64Array) -> Option<f64> {
    if values.is_empty() {
        None
    } else {
        Some(1.0)
    }
}

/// the population standard variance of the values in the specified interval.
#[range_fn(
    name = "StdvarOverTime",
    ret = "Float64Array",
    display_name = "prom_stdvar_over_time"
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

// TODO(ruihang): support quantile_over_time and stddev_over_time

#[cfg(test)]
mod test {
    use super::*;
    use crate::functions::test_util::simple_range_udf_runner;

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
            vec![
                Some(2.0),
                Some(5.0),
                Some(1.0),
                Some(0.0),
                Some(0.0),
                Some(3.0),
                Some(3.0),
                Some(3.0),
                Some(1.0),
                Some(0.0),
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
    }
}
