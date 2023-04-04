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

//! Implementation of [`deriv`](https://prometheus.io/docs/prometheus/latest/querying/functions/#deriv) in PromQL. Refer to the [original
//! implementation](https://github.com/prometheus/prometheus/blob/90b2f7a540b8a70d8d81372e6692dcbb67ccbaaa/promql/functions.go#L839-L856).

use std::sync::Arc;

use common_function_macro::range_fn;
use datafusion::arrow::array::{Float64Array, TimestampMillisecondArray};
use datafusion::arrow::datatypes::TimeUnit;
use datafusion::common::DataFusionError;
use datafusion::logical_expr::{ScalarUDF, Signature, TypeSignature, Volatility};
use datafusion::physical_plan::ColumnarValue;
use datatypes::arrow::array::Array;
use datatypes::arrow::datatypes::DataType;

use crate::functions::{compensated_sum_inc, extract_array};
use crate::range_array::RangeArray;

#[range_fn(name = "Deriv", ret = "Float64Array", display_name = "prom_drive")]
pub fn drive(times: &TimestampMillisecondArray, values: &Float64Array) -> Option<f64> {
    if values.len() < 2 {
        None
    } else {
        let intercept_time = times.value(0);
        let (slope, _) = linear_regression(times, values, intercept_time);
        slope
    }
}

fn linear_regression(
    times: &TimestampMillisecondArray,
    values: &Float64Array,
    intercept_time: i64,
) -> (Option<f64>, Option<f64>) {
    let mut count: f64 = 0.0;
    let mut sum_x: f64 = 0.0;
    let mut sum_y: f64 = 0.0;
    let mut sum_xy: f64 = 0.0;
    let mut sum_x2: f64 = 0.0;
    let mut comp_x: f64 = 0.0;
    let mut comp_y: f64 = 0.0;
    let mut comp_xy: f64 = 0.0;
    let mut comp_x2: f64 = 0.0;

    let mut const_y = true;
    let init_y: f64 = values.value(0);

    for (i, value) in values.iter().enumerate() {
        let time = times.value(i) as f64;
        let value = value.unwrap();
        if const_y && i > 0 && value != init_y {
            const_y = false;
        }
        count += 1.0;
        let x = time - intercept_time as f64 / 1e3;
        (sum_x, comp_x) = compensated_sum_inc(x, sum_x, comp_x);
        (sum_y, comp_y) = compensated_sum_inc(value, sum_y, comp_y);
        (sum_xy, comp_xy) = compensated_sum_inc(x * value, sum_xy, comp_xy);
        (sum_x2, comp_x2) = compensated_sum_inc(x * x, sum_x2, comp_x2);
    }

    if const_y {
        if init_y.is_finite() {
            return (None, None);
        }
        return (Some(0.0), Some(init_y));
    }

    sum_x += comp_x;
    sum_y += comp_y;
    sum_xy += comp_xy;
    sum_x2 += comp_x2;

    let cov_xy = sum_xy - sum_x * sum_y / count;
    let var_x = sum_x2 - sum_x * sum_x / count;

    let slope = cov_xy / var_x;
    let intercept = sum_y / count - slope * sum_x / count;

    (Some(slope), Some(intercept))
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::functions::test_util::simple_range_udf_runner;

    // build timestamp range and value range arrays for test
    fn build_test_range_arrays() -> (RangeArray, RangeArray) {
        let ts_array = Arc::new(TimestampMillisecondArray::from_iter(
            [
                0i64, 300, 600, 900, 1200, 1500, 1800, 2100, 2400, 2700, 3000,
            ]
            .into_iter()
            .map(Some),
        ));
        let ranges = [(0, 11)];

        let values_array = Arc::new(Float64Array::from_iter([
            0.0, 10.0, 20.0, 30.0, 40.0, 0.0, 10.0, 20.0, 30.0, 40.0, 50.0,
        ]));

        let ts_range_array = RangeArray::from_ranges(ts_array, ranges).unwrap();
        let value_range_array = RangeArray::from_ranges(values_array, ranges).unwrap();

        (ts_range_array, value_range_array)
    }

    #[test]
    fn calculate_deriv() {
        let (ts_array, value_array) = build_test_range_arrays();
        simple_range_udf_runner(
            Deriv::scalar_udf(),
            ts_array,
            value_array,
            vec![Some(0.010606060606060607)],
        );
    }
}
