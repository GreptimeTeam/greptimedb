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
use datafusion_common::config::ConfigOptions;
use datafusion_common::ScalarValue;
use datafusion_expr::ScalarFunctionArgs;
use datatypes::arrow::datatypes::{DataType, Field};

use crate::functions::extract_array;
use crate::range_array::RangeArray;

/// Runner to run range UDFs that only requires ts range and value range.
pub fn simple_range_udf_runner(
    range_fn: ScalarUDF,
    input_ts: RangeArray,
    input_value: RangeArray,
    other_args: Vec<ScalarValue>,
    expected: Vec<Option<f64>>,
) {
    let num_rows = input_ts.len();
    let input = [
        ColumnarValue::Array(Arc::new(input_ts.into_dict())),
        ColumnarValue::Array(Arc::new(input_value.into_dict())),
    ]
    .into_iter()
    .chain(other_args.into_iter().map(ColumnarValue::Scalar))
    .collect::<Vec<_>>();
    let arg_fields = vec![
        Arc::new(Field::new("a", input[0].data_type(), false)),
        Arc::new(Field::new("b", input[1].data_type(), false)),
    ];
    let return_field = Arc::new(Field::new("c", DataType::Float64, false));
    let args = ScalarFunctionArgs {
        args: input,
        arg_fields,
        number_rows: num_rows,
        return_field,
        config_options: Arc::new(ConfigOptions::default()),
    };
    let value = range_fn.invoke_with_args(args).unwrap();
    let eval_result: Vec<Option<f64>> = extract_array(&value)
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap()
        .iter()
        .collect();
    assert_eq!(eval_result.len(), expected.len());
    assert!(eval_result
        .iter()
        .zip(expected.iter())
        .all(|(x, y)| match (*x, *y) {
            (Some(x), Some(y)) => (x - y).abs() < 0.0001,
            (None, None) => true,
            _ => false,
        }));
}
