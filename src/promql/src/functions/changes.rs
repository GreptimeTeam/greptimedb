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

//! Implementation of [`changes`](https://prometheus.io/docs/prometheus/latest/querying/functions/#changes) in PromQL. Refer to the [original
//! implementation](https://github.com/prometheus/prometheus/blob/main/promql/functions.go#L1023-L1040).

use std::sync::Arc;

use common_macro::range_fn;
use datafusion::arrow::array::{Float64Array, TimestampMillisecondArray};
use datafusion::arrow::datatypes::TimeUnit;
use datafusion::common::DataFusionError;
use datafusion::logical_expr::{ScalarUDF, Signature, TypeSignature, Volatility};
use datafusion::physical_plan::ColumnarValue;
use datatypes::arrow::array::Array;
use datatypes::arrow::datatypes::DataType;

use crate::functions::extract_array;
use crate::range_array::RangeArray;

/// used to count the number of value changes that occur within a specific time range
#[range_fn(name = "Changes", ret = "Float64Array", display_name = "prom_changes")]
pub fn changes(_: &TimestampMillisecondArray, values: &Float64Array) -> Option<f64> {
    if values.is_empty() {
        None
    } else {
        let (first, rest) = values.values().split_first().unwrap();
        let mut num_changes = 0;
        let mut prev_element = first;
        for cur_element in rest {
            if cur_element != prev_element && !(cur_element.is_nan() && prev_element.is_nan()) {
                num_changes += 1;
            }
            prev_element = cur_element;
        }
        Some(num_changes as f64)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::functions::test_util::simple_range_udf_runner;

    // build timestamp range and value range arrays for test
    fn build_test_range_arrays(
        timestamps: Vec<i64>,
        values: Vec<f64>,
        ranges: Vec<(u32, u32)>,
    ) -> (RangeArray, RangeArray) {
        let ts_array = Arc::new(TimestampMillisecondArray::from_iter(
            timestamps.into_iter().map(Some),
        ));
        let values_array = Arc::new(Float64Array::from_iter(values));

        let ts_range_array = RangeArray::from_ranges(ts_array, ranges.clone()).unwrap();
        let value_range_array = RangeArray::from_ranges(values_array, ranges).unwrap();

        (ts_range_array, value_range_array)
    }

    #[test]
    fn calculate_changes() {
        let timestamps = vec![
            1000i64, 3000, 5000, 7000, 9000, 11000, 13000, 15000, 17000, 200000, 500000,
        ];
        let ranges = vec![
            (0, 1),
            (0, 4),
            (0, 6),
            (0, 10),
            (0, 0), // empty range
        ];

        // assertion 1
        let values_1 = vec![1.0, 2.0, 3.0, 0.0, 1.0, 0.0, 0.0, 1.0, 2.0, 0.0];
        let (ts_array_1, value_array_1) =
            build_test_range_arrays(timestamps.clone(), values_1, ranges.clone());
        simple_range_udf_runner(
            Changes::scalar_udf(),
            ts_array_1,
            value_array_1,
            vec![Some(0.0), Some(3.0), Some(5.0), Some(8.0), None],
        );

        // assertion 2
        let values_2 = vec![1.0, 2.0, 3.0, 4.0, 5.0, 1.0, 2.0, 3.0, 4.0, 5.0];
        let (ts_array_2, value_array_2) =
            build_test_range_arrays(timestamps.clone(), values_2, ranges.clone());
        simple_range_udf_runner(
            Changes::scalar_udf(),
            ts_array_2,
            value_array_2,
            vec![Some(0.0), Some(3.0), Some(5.0), Some(9.0), None],
        );

        // assertion 3
        let values_3 = vec![0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 1.0, 1.0, 1.0, 1.0];
        let (ts_array_3, value_array_3) = build_test_range_arrays(timestamps, values_3, ranges);
        simple_range_udf_runner(
            Changes::scalar_udf(),
            ts_array_3,
            value_array_3,
            vec![Some(0.0), Some(0.0), Some(1.0), Some(1.0), None],
        );
    }
}
