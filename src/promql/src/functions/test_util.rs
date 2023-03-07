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

use datafusion::arrow::array::Float64Array;
use datafusion::logical_expr::ScalarUDF;
use datafusion::physical_plan::ColumnarValue;

use crate::functions::extract_array;
use crate::range_array::RangeArray;

/// Runner to run range UDFs that only requires ts range and value range.
pub fn simple_range_udf_runner(
    range_fn: ScalarUDF,
    input_ts: RangeArray,
    input_value: RangeArray,
    expected: Vec<Option<f64>>,
) {
    let input = vec![
        ColumnarValue::Array(Arc::new(input_ts.into_dict())),
        ColumnarValue::Array(Arc::new(input_value.into_dict())),
    ];
    let eval_result: Vec<Option<f64>> = extract_array(&(range_fn.fun)(&input).unwrap())
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap()
        .iter()
        .collect();
    assert_eq!(eval_result, expected)
}
