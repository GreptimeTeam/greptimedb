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

use std::fmt;
use std::fmt::Display;

use datafusion_common::arrow::compute;
use datafusion_common::arrow::compute::kernels::numeric;
use datafusion_common::arrow::datatypes::DataType;
use datafusion_expr::type_coercion::aggregates::NUMERICS;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, Signature, Volatility};

use crate::function::{Function, extract_args};

const NAME: &str = "mod";

/// The function to find remainders
#[derive(Clone, Debug)]
pub(crate) struct ModuloFunction {
    signature: Signature,
}

impl Default for ModuloFunction {
    fn default() -> Self {
        Self {
            signature: Signature::uniform(2, NUMERICS.to_vec(), Volatility::Immutable),
        }
    }
}

impl Display for ModuloFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", NAME.to_ascii_uppercase())
    }
}

impl Function for ModuloFunction {
    fn name(&self) -> &str {
        NAME
    }

    fn return_type(&self, input_types: &[DataType]) -> datafusion_common::Result<DataType> {
        if input_types.iter().all(DataType::is_signed_integer) {
            Ok(DataType::Int64)
        } else if input_types.iter().all(DataType::is_unsigned_integer) {
            Ok(DataType::UInt64)
        } else {
            Ok(DataType::Float64)
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion_common::Result<ColumnarValue> {
        let [nums, divs] = extract_args(self.name(), &args)?;
        let array = numeric::rem(&nums, &divs)?;

        let result = match nums.data_type() {
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
                compute::cast(&array, &DataType::Int64)
            }
            DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
                compute::cast(&array, &DataType::UInt64)
            }
            DataType::Float32 | DataType::Float64 => compute::cast(&array, &DataType::Float64),
            _ => unreachable!("unexpected datatype: {:?}", nums.data_type()),
        }?;
        Ok(ColumnarValue::Array(result))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::Field;
    use datafusion_common::arrow::array::{
        AsArray, Float64Array, Int32Array, StringViewArray, UInt32Array,
    };
    use datafusion_common::arrow::datatypes::{Float64Type, Int64Type, UInt64Type};

    use super::*;
    #[test]
    fn test_mod_function_signed() {
        let function = ModuloFunction::default();
        assert_eq!("mod", function.name());
        assert_eq!(
            DataType::Int64,
            function.return_type(&[DataType::Int64]).unwrap()
        );
        assert_eq!(
            DataType::Int64,
            function.return_type(&[DataType::Int32]).unwrap()
        );

        let nums = vec![18, -17, 5, -6];
        let divs = vec![4, 8, -5, -5];

        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::new(Int32Array::from(nums.clone()))),
                ColumnarValue::Array(Arc::new(Int32Array::from(divs.clone()))),
            ],
            arg_fields: vec![],
            number_rows: 4,
            return_field: Arc::new(Field::new("x", DataType::Int64, false)),
            config_options: Arc::new(Default::default()),
        };
        let result = function.invoke_with_args(args).unwrap();
        let result = result.to_array(4).unwrap();
        let result = result.as_primitive::<Int64Type>();
        assert_eq!(result.len(), 4);
        for i in 0..4 {
            let p: i64 = (nums[i] % divs[i]) as i64;
            assert_eq!(result.value(i), p);
        }
    }

    #[test]
    fn test_mod_function_unsigned() {
        let function = ModuloFunction::default();
        assert_eq!("mod", function.name());
        assert_eq!(
            DataType::UInt64,
            function.return_type(&[DataType::UInt64]).unwrap()
        );
        assert_eq!(
            DataType::UInt64,
            function.return_type(&[DataType::UInt32]).unwrap()
        );

        let nums: Vec<u32> = vec![18, 17, 5, 6];
        let divs: Vec<u32> = vec![4, 8, 5, 5];

        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::new(UInt32Array::from(nums.clone()))),
                ColumnarValue::Array(Arc::new(UInt32Array::from(divs.clone()))),
            ],
            arg_fields: vec![],
            number_rows: 4,
            return_field: Arc::new(Field::new("x", DataType::UInt64, false)),
            config_options: Arc::new(Default::default()),
        };
        let result = function.invoke_with_args(args).unwrap();
        let result = result.to_array(4).unwrap();
        let result = result.as_primitive::<UInt64Type>();
        assert_eq!(result.len(), 4);
        for i in 0..4 {
            let p: u64 = (nums[i] % divs[i]) as u64;
            assert_eq!(result.value(i), p);
        }
    }

    #[test]
    fn test_mod_function_float() {
        let function = ModuloFunction::default();
        assert_eq!("mod", function.name());
        assert_eq!(
            DataType::Float64,
            function.return_type(&[DataType::Float64]).unwrap()
        );
        assert_eq!(
            DataType::Float64,
            function.return_type(&[DataType::Float32]).unwrap()
        );

        let nums = vec![18.0, 17.0, 5.0, 6.0];
        let divs = vec![4.0, 8.0, 5.0, 5.0];

        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::new(Float64Array::from(nums.clone()))),
                ColumnarValue::Array(Arc::new(Float64Array::from(divs.clone()))),
            ],
            arg_fields: vec![],
            number_rows: 4,
            return_field: Arc::new(Field::new("x", DataType::Float64, false)),
            config_options: Arc::new(Default::default()),
        };
        let result = function.invoke_with_args(args).unwrap();
        let result = result.to_array(4).unwrap();
        let result = result.as_primitive::<Float64Type>();
        assert_eq!(result.len(), 4);
        for i in 0..4 {
            let p: f64 = nums[i] % divs[i];
            assert_eq!(result.value(i), p);
        }
    }

    #[test]
    fn test_mod_function_errors() {
        let function = ModuloFunction::default();
        assert_eq!("mod", function.name());
        let nums = vec![27];
        let divs = vec![0];

        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::new(Int32Array::from(nums))),
                ColumnarValue::Array(Arc::new(Int32Array::from(divs))),
            ],
            arg_fields: vec![],
            number_rows: 1,
            return_field: Arc::new(Field::new("x", DataType::Int64, false)),
            config_options: Arc::new(Default::default()),
        };
        let result = function.invoke_with_args(args);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert_eq!(err_msg, "Arrow error: Divide by zero error");

        let nums = vec![27];

        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::new(Int32Array::from(nums)))],
            arg_fields: vec![],
            number_rows: 1,
            return_field: Arc::new(Field::new("x", DataType::Int64, false)),
            config_options: Arc::new(Default::default()),
        };
        let result = function.invoke_with_args(args);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert_eq!(
            err_msg,
            "Execution error: mod function requires 2 arguments, got 1"
        );

        let nums = vec!["27"];
        let divs = vec!["4"];
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::new(StringViewArray::from(nums))),
                ColumnarValue::Array(Arc::new(StringViewArray::from(divs))),
            ],
            arg_fields: vec![],
            number_rows: 1,
            return_field: Arc::new(Field::new("x", DataType::Int64, false)),
            config_options: Arc::new(Default::default()),
        };
        let result = function.invoke_with_args(args);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("Invalid arithmetic operation"));
    }
}
