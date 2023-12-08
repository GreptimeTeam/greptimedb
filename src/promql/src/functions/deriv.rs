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

use common_macro::range_fn;
use datafusion::arrow::array::{Float64Array, TimestampMillisecondArray};
use datafusion::arrow::datatypes::TimeUnit;
use datafusion::common::DataFusionError;
use datafusion::logical_expr::{ScalarUDF, Signature, TypeSignature, Volatility};
use datafusion::physical_plan::ColumnarValue;
use datatypes::arrow::array::Array;
use datatypes::arrow::datatypes::DataType;

use crate::functions::{extract_array, linear_regression};
use crate::range_array::RangeArray;

#[range_fn(name = "Deriv", ret = "Float64Array", display_name = "prom_deriv")]
pub fn deriv(times: &TimestampMillisecondArray, values: &Float64Array) -> Option<f64> {
    if values.len() < 2 {
        None
    } else {
        let intercept_time = times.value(0);
        let (slope, _) = linear_regression(times, values, intercept_time);
        slope
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

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
        let ranges = [(0, 11), (0, 1)];

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
            vec![Some(10.606060606060607), None],
        );
    }

    // From prometheus `promql/functions_test.go` case `TestDeriv`
    #[test]
    fn complicate_deriv() {
        let start = 1493712816939;
        let interval = 30 * 1000;
        let mut ts_data = vec![];
        for i in 0..15 {
            let jitter = 12 * i % 2;
            ts_data.push(Some(start + interval * i + jitter));
        }
        let val_data = vec![Some(1.0); 15];
        let ts_array = Arc::new(TimestampMillisecondArray::from_iter(ts_data));
        let val_array = Arc::new(Float64Array::from_iter(val_data));
        let range = [(0, 15)];
        let ts_range_array = RangeArray::from_ranges(ts_array, range).unwrap();
        let value_range_array = RangeArray::from_ranges(val_array, range).unwrap();

        simple_range_udf_runner(
            Deriv::scalar_udf(),
            ts_range_array,
            value_range_array,
            vec![Some(0.0)],
        );
    }
}
