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

use datafusion::error::DataFusionError;
use datafusion_common::ScalarValue;
use datafusion_expr::{create_udf, ColumnarValue, ScalarUDF, Volatility};
use datatypes::arrow::array::{AsArray, Float64Array, PrimitiveArray};
use datatypes::arrow::datatypes::{DataType, Float64Type};
use datatypes::arrow::error::ArrowError;

use crate::error;
use crate::functions::extract_array;

pub struct Round;

impl Round {
    pub const fn name() -> &'static str {
        "prom_round"
    }

    fn input_type() -> Vec<DataType> {
        vec![DataType::Float64, DataType::Float64]
    }

    pub fn return_type() -> DataType {
        DataType::Float64
    }

    pub fn scalar_udf() -> ScalarUDF {
        create_udf(
            Self::name(),
            Self::input_type(),
            Self::return_type(),
            Volatility::Volatile,
            Arc::new(Self::round) as _,
        )
    }

    fn round(input: &[ColumnarValue]) -> Result<ColumnarValue, DataFusionError> {
        error::ensure(
            input.len() == 2,
            DataFusionError::Plan("prom_round function should have 2 inputs".to_string()),
        )?;

        let value_array = extract_array(&input[0])?;
        let nearest_col = &input[1];

        match nearest_col {
            ColumnarValue::Scalar(nearest_scalar) => {
                let nearest = if let ScalarValue::Float64(Some(val)) = nearest_scalar {
                    *val
                } else {
                    let null_array = Float64Array::new_null(value_array.len());
                    return Ok(ColumnarValue::Array(Arc::new(null_array)));
                };
                let op = |a: f64| {
                    if nearest == 0.0 {
                        a.round()
                    } else {
                        (a / nearest).round() * nearest
                    }
                };
                let result: PrimitiveArray<Float64Type> =
                    value_array.as_primitive::<Float64Type>().unary(op);
                Ok(ColumnarValue::Array(Arc::new(result) as _))
            }
            ColumnarValue::Array(nearest_array) => {
                let value_array = value_array.as_primitive::<Float64Type>();
                let nearest_array = nearest_array.as_primitive::<Float64Type>();
                error::ensure(
                    value_array.len() == nearest_array.len(),
                    DataFusionError::Execution(format!(
                        "input arrays should have the same length, found {} and {}",
                        value_array.len(),
                        nearest_array.len()
                    )),
                )?;

                let result: PrimitiveArray<Float64Type> =
                    datatypes::arrow::compute::binary(value_array, nearest_array, |a, nearest| {
                        if nearest == 0.0 {
                            a.round()
                        } else {
                            (a / nearest).round() * nearest
                        }
                    })
                    .map_err(|err: ArrowError| DataFusionError::ArrowError(Box::new(err), None))?;

                Ok(ColumnarValue::Array(Arc::new(result) as _))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use datafusion_common::config::ConfigOptions;
    use datafusion_expr::ScalarFunctionArgs;
    use datatypes::arrow::array::Float64Array;
    use datatypes::arrow::datatypes::Field;

    use super::*;

    fn test_round_f64(value: Vec<f64>, nearest: f64, expected: Vec<f64>) {
        let round_udf = Round::scalar_udf();
        let input = vec![
            ColumnarValue::Array(Arc::new(Float64Array::from(value))),
            ColumnarValue::Scalar(ScalarValue::Float64(Some(nearest))),
        ];
        let arg_fields = vec![
            Arc::new(Field::new("a", input[0].data_type(), false)),
            Arc::new(Field::new("b", input[1].data_type(), false)),
        ];
        let return_field = Arc::new(Field::new("c", DataType::Float64, false));
        let args = ScalarFunctionArgs {
            args: input,
            arg_fields,
            number_rows: 1,
            return_field,
            config_options: Arc::new(ConfigOptions::default()),
        };
        let result = round_udf.invoke_with_args(args).unwrap();
        let result_array = extract_array(&result).unwrap();
        assert_eq!(result_array.len(), 1);
        assert_eq!(
            result_array.as_primitive::<Float64Type>().values(),
            &expected
        );
    }

    #[test]
    fn test_round() {
        test_round_f64(vec![123.456], 0.001, vec![123.456]);
        test_round_f64(vec![123.456], 0.01, vec![123.46000000000001]);
        test_round_f64(vec![123.456], 0.1, vec![123.5]);
        test_round_f64(vec![123.456], 0.0, vec![123.0]);
        test_round_f64(vec![123.456], 1.0, vec![123.0]);
        test_round_f64(vec![123.456], 10.0, vec![120.0]);
        test_round_f64(vec![123.456], 100.0, vec![100.0]);
        test_round_f64(vec![123.456], 105.0, vec![105.0]);
        test_round_f64(vec![123.456], 1000.0, vec![0.0]);
    }
}
