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

use std::fmt::Display;

use common_query::error::Result;
use datafusion::logical_expr::ColumnarValue;
use datafusion_common::{DataFusionError, ScalarValue};
use datafusion_expr::{ScalarFunctionArgs, Signature};
use datatypes::arrow::datatypes::DataType;

use crate::function::Function;
use crate::helper;
use crate::scalars::vector::VectorCalculator;
use crate::scalars::vector::impl_conv::as_veclit;

const NAME: &str = "vec_kth_elem";

/// Returns the k-th(0-based index) element of the vector.
///
/// # Example
///
/// ```sql
/// SELECT vec_kth_elem("[2, 4, 6]",1) as result;
///
/// +---------+
/// | result  |
/// +---------+
/// | 4 |
/// +---------+
///
/// ```
///

#[derive(Debug, Clone, Default)]
pub struct VectorKthElemFunction;

impl Function for VectorKthElemFunction {
    fn name(&self) -> &str {
        NAME
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float32)
    }

    fn signature(&self) -> Signature {
        helper::one_of_sigs2(
            vec![DataType::Utf8, DataType::Binary],
            vec![DataType::Int64],
        )
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion_common::Result<ColumnarValue> {
        let body = |v0: &ScalarValue, v1: &ScalarValue| -> datafusion_common::Result<ScalarValue> {
            let v0 = as_veclit(v0)?;

            let v1 = if let ScalarValue::Int64(Some(v1)) = v1
                && *v1 >= 0
            {
                *v1 as usize
            } else {
                return Err(DataFusionError::Execution(format!(
                    "2nd argument not a valid index: {}",
                    self.name()
                )));
            };

            let result = v0
                .map(|v0| {
                    if v1 >= v0.len() {
                        Err(DataFusionError::Execution(format!(
                            "index out of bound: {}",
                            self.name()
                        )))
                    } else {
                        Ok(v0[v1])
                    }
                })
                .transpose()?;
            Ok(ScalarValue::Float32(result))
        };

        let calculator = VectorCalculator {
            name: self.name(),
            func: body,
        };
        calculator.invoke_with_args(args)
    }
}

impl Display for VectorKthElemFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", NAME.to_ascii_uppercase())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::Field;
    use datafusion::arrow::array::{Array, ArrayRef, AsArray, Int64Array, StringViewArray};
    use datafusion::arrow::datatypes::Float32Type;
    use datafusion_common::config::ConfigOptions;

    use super::*;

    #[test]
    fn test_vec_kth_elem() {
        let func = VectorKthElemFunction;

        let input0: ArrayRef = Arc::new(StringViewArray::from(vec![
            Some("[1.0,2.0,3.0]".to_string()),
            Some("[4.0,5.0,6.0]".to_string()),
            Some("[7.0,8.0,9.0]".to_string()),
            None,
        ]));
        let input1: ArrayRef = Arc::new(Int64Array::from(vec![Some(0), Some(2), None, Some(1)]));

        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(input0), ColumnarValue::Array(input1)],
            arg_fields: vec![],
            number_rows: 4,
            return_field: Arc::new(Field::new("x", DataType::Float32, false)),
            config_options: Arc::new(ConfigOptions::new()),
        };
        let result = func
            .invoke_with_args(args)
            .and_then(|x| x.to_array(4))
            .unwrap();

        let result = result.as_primitive::<Float32Type>();
        assert_eq!(result.len(), 4);
        assert_eq!(result.value(0), 1.0);
        assert_eq!(result.value(1), 6.0);
        assert!(result.is_null(2));
        assert!(result.is_null(3));

        let input0: ArrayRef = Arc::new(StringViewArray::from(vec![Some(
            "[1.0,2.0,3.0]".to_string(),
        )]));
        let input1: ArrayRef = Arc::new(Int64Array::from(vec![Some(3)]));

        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(input0), ColumnarValue::Array(input1)],
            arg_fields: vec![],
            number_rows: 3,
            return_field: Arc::new(Field::new("x", DataType::Float32, false)),
            config_options: Arc::new(ConfigOptions::new()),
        };
        let e = func.invoke_with_args(args).unwrap_err();
        assert!(e.to_string().starts_with("External error: Invalid function args: Out of range: k must be in the range [0, 2], but got k = 3."));

        let input0: ArrayRef = Arc::new(StringViewArray::from(vec![Some(
            "[1.0,2.0,3.0]".to_string(),
        )]));
        let input1: ArrayRef = Arc::new(Int64Array::from(vec![Some(-1)]));

        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(input0), ColumnarValue::Array(input1)],
            arg_fields: vec![],
            number_rows: 3,
            return_field: Arc::new(Field::new("x", DataType::Float32, false)),
            config_options: Arc::new(ConfigOptions::new()),
        };
        let e = func.invoke_with_args(args).unwrap_err();
        assert!(e.to_string().starts_with("External error: Invalid function args: Invalid argument: k must be a non-negative integer, but got k = -1."));
    }
}
