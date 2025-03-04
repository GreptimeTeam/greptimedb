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

use common_query::error;
use common_query::error::{ArrowComputeSnafu, InvalidFuncArgsSnafu, Result};
use common_query::prelude::{Signature, Volatility};
use datatypes::arrow::compute;
use datatypes::arrow::compute::kernels::numeric;
use datatypes::arrow::datatypes::DataType as ArrowDataType;
use datatypes::prelude::ConcreteDataType;
use datatypes::vectors::{Helper, VectorRef};
use snafu::{ensure, ResultExt};

use crate::function::{Function, FunctionContext};

const NAME: &str = "mod";

/// The function to find remainders
#[derive(Clone, Debug, Default)]
pub struct ModuloFunction;

impl Display for ModuloFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", NAME.to_ascii_uppercase())
    }
}

impl Function for ModuloFunction {
    fn name(&self) -> &str {
        NAME
    }

    fn return_type(&self, input_types: &[ConcreteDataType]) -> Result<ConcreteDataType> {
        if input_types.iter().all(ConcreteDataType::is_signed) {
            Ok(ConcreteDataType::int64_datatype())
        } else if input_types.iter().all(ConcreteDataType::is_unsigned) {
            Ok(ConcreteDataType::uint64_datatype())
        } else {
            Ok(ConcreteDataType::float64_datatype())
        }
    }

    fn signature(&self) -> Signature {
        Signature::uniform(2, ConcreteDataType::numerics(), Volatility::Immutable)
    }

    fn eval(&self, _func_ctx: &FunctionContext, columns: &[VectorRef]) -> Result<VectorRef> {
        ensure!(
            columns.len() == 2,
            InvalidFuncArgsSnafu {
                err_msg: format!(
                    "The length of the args is not correct, expect exactly two, have: {}",
                    columns.len()
                ),
            }
        );
        let nums = &columns[0];
        let divs = &columns[1];
        let nums_arrow_array = &nums.to_arrow_array();
        let divs_arrow_array = &divs.to_arrow_array();
        let array = numeric::rem(nums_arrow_array, divs_arrow_array).context(ArrowComputeSnafu)?;

        let result = match nums.data_type() {
            ConcreteDataType::Int8(_)
            | ConcreteDataType::Int16(_)
            | ConcreteDataType::Int32(_)
            | ConcreteDataType::Int64(_) => compute::cast(&array, &ArrowDataType::Int64),
            ConcreteDataType::UInt8(_)
            | ConcreteDataType::UInt16(_)
            | ConcreteDataType::UInt32(_)
            | ConcreteDataType::UInt64(_) => compute::cast(&array, &ArrowDataType::UInt64),
            ConcreteDataType::Float32(_) | ConcreteDataType::Float64(_) => {
                compute::cast(&array, &ArrowDataType::Float64)
            }
            _ => unreachable!("unexpected datatype: {:?}", nums.data_type()),
        }
        .context(ArrowComputeSnafu)?;
        Helper::try_into_vector(&result).context(error::FromArrowArraySnafu)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_error::ext::ErrorExt;
    use datatypes::value::Value;
    use datatypes::vectors::{Float64Vector, Int32Vector, StringVector, UInt32Vector};

    use super::*;
    #[test]
    fn test_mod_function_signed() {
        let function = ModuloFunction;
        assert_eq!("mod", function.name());
        assert_eq!(
            ConcreteDataType::int64_datatype(),
            function
                .return_type(&[ConcreteDataType::int64_datatype()])
                .unwrap()
        );
        assert_eq!(
            ConcreteDataType::int64_datatype(),
            function
                .return_type(&[ConcreteDataType::int32_datatype()])
                .unwrap()
        );

        let nums = vec![18, -17, 5, -6];
        let divs = vec![4, 8, -5, -5];

        let args: Vec<VectorRef> = vec![
            Arc::new(Int32Vector::from_vec(nums.clone())),
            Arc::new(Int32Vector::from_vec(divs.clone())),
        ];
        let result = function.eval(&FunctionContext::default(), &args).unwrap();
        assert_eq!(result.len(), 4);
        for i in 0..4 {
            let p: i64 = (nums[i] % divs[i]) as i64;
            assert!(matches!(result.get(i), Value::Int64(v) if v == p));
        }
    }

    #[test]
    fn test_mod_function_unsigned() {
        let function = ModuloFunction;
        assert_eq!("mod", function.name());
        assert_eq!(
            ConcreteDataType::uint64_datatype(),
            function
                .return_type(&[ConcreteDataType::uint64_datatype()])
                .unwrap()
        );
        assert_eq!(
            ConcreteDataType::uint64_datatype(),
            function
                .return_type(&[ConcreteDataType::uint32_datatype()])
                .unwrap()
        );

        let nums: Vec<u32> = vec![18, 17, 5, 6];
        let divs: Vec<u32> = vec![4, 8, 5, 5];

        let args: Vec<VectorRef> = vec![
            Arc::new(UInt32Vector::from_vec(nums.clone())),
            Arc::new(UInt32Vector::from_vec(divs.clone())),
        ];
        let result = function.eval(&FunctionContext::default(), &args).unwrap();
        assert_eq!(result.len(), 4);
        for i in 0..4 {
            let p: u64 = (nums[i] % divs[i]) as u64;
            assert!(matches!(result.get(i), Value::UInt64(v) if v == p));
        }
    }

    #[test]
    fn test_mod_function_float() {
        let function = ModuloFunction;
        assert_eq!("mod", function.name());
        assert_eq!(
            ConcreteDataType::float64_datatype(),
            function
                .return_type(&[ConcreteDataType::float64_datatype()])
                .unwrap()
        );
        assert_eq!(
            ConcreteDataType::float64_datatype(),
            function
                .return_type(&[ConcreteDataType::float32_datatype()])
                .unwrap()
        );

        let nums = vec![18.0, 17.0, 5.0, 6.0];
        let divs = vec![4.0, 8.0, 5.0, 5.0];

        let args: Vec<VectorRef> = vec![
            Arc::new(Float64Vector::from_vec(nums.clone())),
            Arc::new(Float64Vector::from_vec(divs.clone())),
        ];
        let result = function.eval(&FunctionContext::default(), &args).unwrap();
        assert_eq!(result.len(), 4);
        for i in 0..4 {
            let p: f64 = nums[i] % divs[i];
            assert!(matches!(result.get(i), Value::Float64(v) if v == p));
        }
    }

    #[test]
    fn test_mod_function_errors() {
        let function = ModuloFunction;
        assert_eq!("mod", function.name());
        let nums = vec![27];
        let divs = vec![0];

        let args: Vec<VectorRef> = vec![
            Arc::new(Int32Vector::from_vec(nums.clone())),
            Arc::new(Int32Vector::from_vec(divs.clone())),
        ];
        let result = function.eval(&FunctionContext::default(), &args);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().output_msg();
        assert_eq!(
            err_msg,
            "Failed to perform compute operation on arrow arrays: Divide by zero error"
        );

        let nums = vec![27];

        let args: Vec<VectorRef> = vec![Arc::new(Int32Vector::from_vec(nums.clone()))];
        let result = function.eval(&FunctionContext::default(), &args);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().output_msg();
        assert!(
            err_msg.contains("The length of the args is not correct, expect exactly two, have: 1")
        );

        let nums = vec!["27"];
        let divs = vec!["4"];
        let args: Vec<VectorRef> = vec![
            Arc::new(StringVector::from(nums.clone())),
            Arc::new(StringVector::from(divs.clone())),
        ];
        let result = function.eval(&FunctionContext::default(), &args);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().output_msg();
        assert!(err_msg.contains("Invalid arithmetic operation"));
    }
}
