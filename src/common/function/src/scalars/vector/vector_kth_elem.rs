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

use std::borrow::Cow;
use std::fmt::Display;

use common_query::error::{InvalidFuncArgsSnafu, Result};
use common_query::prelude::Signature;
use datatypes::prelude::ConcreteDataType;
use datatypes::scalars::ScalarVectorBuilder;
use datatypes::vectors::{Float32VectorBuilder, MutableVector, VectorRef};
use snafu::ensure;

use crate::function::{Function, FunctionContext};
use crate::helper;
use crate::scalars::vector::impl_conv::{as_veclit, as_veclit_if_const};

const NAME: &str = "vec_kth_elem";

/// Returns the k-th element of the vector.
///
/// # Example
///
/// ```sql
/// SELECT vec_kth_elem("[2, 4, 6]",2) as result;
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

    fn return_type(
        &self,
        _imput_types: &[ConcreteDataType],
    ) -> common_query::error::Result<ConcreteDataType> {
        Ok(ConcreteDataType::float32_datatype())
    }

    fn signature(&self) -> Signature {
        helper::one_of_sigs2(
            vec![
                ConcreteDataType::string_datatype(),
                ConcreteDataType::binary_datatype(),
            ],
            vec![ConcreteDataType::uint64_datatype()],
        )
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

        let arg0 = &columns[0];
        let arg1 = &columns[1];

        let len = arg0.len();
        let mut result = Float32VectorBuilder::with_capacity(len);
        if len == 0 {
            return Ok(result.to_vector());
        };

        let arg0_const = as_veclit_if_const(arg0)?;

        for i in 0..len {
            let arg0 = match arg0_const.as_ref() {
                Some(arg0) => Some(Cow::Borrowed(arg0.as_ref())),
                None => as_veclit(arg0.get_ref(i))?,
            };
            let Some(arg0) = arg0 else {
                result.push_null();
                continue;
            };

            let arg1 = arg1.get(i).as_f64_lossy();
            let Some(arg1) = arg1 else {
                result.push_null();
                continue;
            };

            let k = arg1 as usize;

            ensure!(
                k > 0 && k <= arg0.len(),
                InvalidFuncArgsSnafu {
                    err_msg: format!(
                        "Invalid function args: Invalid k: {}. k must be in the range [1, {}]",
                        k,
                        arg0.len()
                    ),
                }
            );

            let value = arg0[k - 1];

            result.push(Some(value));
        }
        Ok(result.to_vector())
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

    use common_query::error;
    use datatypes::vectors::{Int64Vector, StringVector};

    use super::*;

    #[test]
    fn test_vec_kth_elem() {
        let func = VectorKthElemFunction;

        let input0 = Arc::new(StringVector::from(vec![
            Some("[1.0,2.0,3.0]".to_string()),
            Some("[7.0,8.0,9.0]".to_string()),
            None,
        ]));
        let input1 = Arc::new(Int64Vector::from(vec![Some(2), None, Some(2)]));

        let result = func
            .eval(&FunctionContext::default(), &[input0, input1])
            .unwrap();

        let result = result.as_ref();
        assert_eq!(result.len(), 3);
        assert_eq!(result.get_ref(0).as_f32().unwrap(), Some(2.0));
        assert!(result.get_ref(1).is_null());
        assert!(result.get_ref(2).is_null());

        let input0 = Arc::new(StringVector::from(vec![Some("[1.0,2.0,3.0]".to_string())]));
        let input1 = Arc::new(Int64Vector::from(vec![Some(4)]));

        let err = func
            .eval(&FunctionContext::default(), &[input0, input1])
            .unwrap_err();
        match err {
            error::Error::InvalidFuncArgs { err_msg, .. } => {
                assert_eq!(
                    err_msg,
                    format!("Invalid function args: Invalid k: 4. k must be in the range [1, 3]")
                )
            }
            _ => unreachable!(),
        }

        let input0 = Arc::new(StringVector::from(vec![Some("[1.0,2.0,3.0]".to_string())]));
        let input1 = Arc::new(Int64Vector::from(vec![Some(0)]));

        let err = func
            .eval(&FunctionContext::default(), &[input0, input1])
            .unwrap_err();
        match err {
            error::Error::InvalidFuncArgs { err_msg, .. } => {
                assert_eq!(
                    err_msg,
                    format!("Invalid function args: Invalid k: 0. k must be in the range [1, 3]")
                )
            }
            _ => unreachable!(),
        }
    }
}
