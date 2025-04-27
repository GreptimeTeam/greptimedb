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
use datatypes::arrow::array::AsArray;
use datatypes::arrow::datatypes::{DataType, Float64Type};
use datatypes::compute;

use crate::functions::extract_array;

pub struct Round {
    nearest: f64,
}

impl Round {
    fn new(nearest: f64) -> Self {
        Self { nearest }
    }

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
            Arc::new(move |input: &_| Self::create_function(input)?.calc(input)) as _,
        )
    }

    fn create_function(inputs: &[ColumnarValue]) -> Result<Self, DataFusionError> {
        if inputs.len() != 2 {
            return Err(DataFusionError::Plan(
                "Round function should have 2 inputs".to_string(),
            ));
        }
        let ColumnarValue::Scalar(ScalarValue::Float64(Some(nearest))) = inputs[1] else {
            return Err(DataFusionError::Plan(
                "Round function's second input should be a scalar float64".to_string(),
            ));
        };
        Ok(Self::new(nearest))
    }

    fn calc(&self, input: &[ColumnarValue]) -> Result<ColumnarValue, DataFusionError> {
        assert_eq!(input.len(), 2);

        let value_array = extract_array(&input[0])?;

        if self.nearest == 0.0 {
            let values = value_array.as_primitive::<Float64Type>();
            let result = compute::unary::<_, _, Float64Type>(values, |a| a.round());
            Ok(ColumnarValue::Array(Arc::new(result) as _))
        } else {
            let values = value_array.as_primitive::<Float64Type>();
            let nearest = self.nearest;
            let result =
                compute::unary::<_, _, Float64Type>(values, |a| ((a / nearest).round() * nearest));
            Ok(ColumnarValue::Array(Arc::new(result) as _))
        }
    }
}

#[cfg(test)]
mod tests {
    use datafusion_expr::ScalarFunctionArgs;
    use datatypes::arrow::array::Float64Array;

    use super::*;

    fn test_round_f64(value: Vec<f64>, nearest: f64, expected: Vec<f64>) {
        let round_udf = Round::scalar_udf();
        let input = vec![
            ColumnarValue::Array(Arc::new(Float64Array::from(value))),
            ColumnarValue::Scalar(ScalarValue::Float64(Some(nearest))),
        ];
        let args = ScalarFunctionArgs {
            args: input,
            number_rows: 1,
            return_type: &DataType::Float64,
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
