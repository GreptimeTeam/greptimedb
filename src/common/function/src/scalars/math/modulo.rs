// Copyright 2024 Greptime Team
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
use std::sync::Arc;

use common_query::error;
use common_query::error::{ArrowComputeSnafu, InvalidFuncArgsSnafu, Result};
use common_query::prelude::{Signature, Volatility};
use datatypes::arrow::compute;
use datatypes::arrow::compute::kernels::numeric;
use datatypes::arrow::datatypes::DataType as ArrowDataType;
use datatypes::prelude::{ConcreteDataType, Value};
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

    fn return_type(
        &self,
        _input_types: &[ConcreteDataType],
    ) -> common_query::error::Result<ConcreteDataType> {
        Ok(ConcreteDataType::int64_datatype())
    }

    fn signature(&self) -> Signature {
        Signature::uniform(
            2,
            vec![
                ConcreteDataType::int8_datatype(),
                ConcreteDataType::int16_datatype(),
                ConcreteDataType::int32_datatype(),
                ConcreteDataType::int64_datatype(),
                ConcreteDataType::float32_datatype(),
                ConcreteDataType::float64_datatype(),
            ],
            Volatility::Immutable,
        )
    }

    fn eval(&self, _func_ctx: FunctionContext, columns: &[VectorRef]) -> Result<VectorRef> {
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
        let result = compute::cast(&array, &ArrowDataType::Int64).context(ArrowComputeSnafu)?;
        Ok(Helper::try_into_vector(&result).context(error::FromArrowArraySnafu)?)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datatypes::vectors::Int32Vector;

    use super::*;
    #[test]
    fn test_mod_function() {
        let function = ModuloFunction;
        assert_eq!("mod", function.name());
        let nums = vec![18, 17, 5, 6];
        let divs = vec![4, 8, 5, 5];

        let args: Vec<VectorRef> = vec![
            Arc::new(Int32Vector::from_vec(nums.clone())),
            Arc::new(Int32Vector::from_vec(divs.clone())),
        ];
        let result = function.eval(FunctionContext::default(), &args).unwrap();
        assert_eq!(result.len(), 4);
        for i in 0..3 {
            let p: i64 = (nums[i] % divs[i]) as i64;
            assert!(matches!(result.get(i), Value::Int64(v) if v == p));
        }
    }
}
