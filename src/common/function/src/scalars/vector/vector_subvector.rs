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
use common_query::prelude::{Signature, TypeSignature};
use datafusion_expr::Volatility;
use datatypes::prelude::ConcreteDataType;
use datatypes::scalars::ScalarVectorBuilder;
use datatypes::vectors::{BinaryVectorBuilder, MutableVector, VectorRef};
use snafu::ensure;

use crate::function::{Function, FunctionContext};
use crate::scalars::vector::impl_conv::{as_veclit, as_veclit_if_const, veclit_to_binlit};

const NAME: &str = "vec_subvector";

/// Returns a subvector from start(included) to end(excluded) index.
///
/// # Example
///
/// ```sql
/// SELECT vec_to_string(vec_subvector("[1, 2, 3, 4, 5]", 1, 3)) as result;
///
/// +---------+
/// | result  |
/// +---------+
/// | [2, 3]  |
/// +---------+
///
/// ```
///

#[derive(Debug, Clone, Default)]
pub struct VectorSubvectorFunction;

impl Function for VectorSubvectorFunction {
    fn name(&self) -> &str {
        NAME
    }

    fn return_type(&self, _input_types: &[ConcreteDataType]) -> Result<ConcreteDataType> {
        Ok(ConcreteDataType::binary_datatype())
    }

    fn signature(&self) -> Signature {
        Signature::one_of(
            vec![
                TypeSignature::Exact(vec![
                    ConcreteDataType::string_datatype(),
                    ConcreteDataType::int64_datatype(),
                    ConcreteDataType::int64_datatype(),
                ]),
                TypeSignature::Exact(vec![
                    ConcreteDataType::binary_datatype(),
                    ConcreteDataType::int64_datatype(),
                    ConcreteDataType::int64_datatype(),
                ]),
            ],
            Volatility::Immutable,
        )
    }

    fn eval(&self, _func_ctx: &FunctionContext, columns: &[VectorRef]) -> Result<VectorRef> {
        ensure!(
            columns.len() == 3,
            InvalidFuncArgsSnafu {
                err_msg: format!(
                    "The length of the args is not correct, expect exactly three, have: {}",
                    columns.len()
                )
            }
        );

        let arg0 = &columns[0];
        let arg1 = &columns[1];
        let arg2 = &columns[2];

        ensure!(
            arg0.len() == arg1.len() && arg1.len() == arg2.len(),
            InvalidFuncArgsSnafu {
                err_msg: format!(
                    "The lengths of the vector are not aligned, args 0: {}, args 1: {}, args 2: {}",
                    arg0.len(),
                    arg1.len(),
                    arg2.len()
                )
            }
        );

        let len = arg0.len();
        let mut result = BinaryVectorBuilder::with_capacity(len);
        if len == 0 {
            return Ok(result.to_vector());
        }

        let arg0_const = as_veclit_if_const(arg0)?;

        for i in 0..len {
            let arg0 = match arg0_const.as_ref() {
                Some(arg0) => Some(Cow::Borrowed(arg0.as_ref())),
                None => as_veclit(arg0.get_ref(i))?,
            };
            let arg1 = arg1.get(i).as_i64();
            let arg2 = arg2.get(i).as_i64();
            let (Some(arg0), Some(arg1), Some(arg2)) = (arg0, arg1, arg2) else {
                result.push_null();
                continue;
            };

            ensure!(
                0 <= arg1 && arg1 <= arg2 && arg2 as usize <= arg0.len(),
                InvalidFuncArgsSnafu {
                    err_msg: format!(
                        "Invalid start and end indices: start={}, end={}, vec_len={}",
                        arg1,
                        arg2,
                        arg0.len()
                    )
                }
            );

            let subvector = &arg0[arg1 as usize..arg2 as usize];
            let binlit = veclit_to_binlit(subvector);
            result.push(Some(&binlit));
        }

        Ok(result.to_vector())
    }
}

impl Display for VectorSubvectorFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", NAME.to_ascii_uppercase())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_query::error::Error;
    use datatypes::vectors::{Int64Vector, StringVector};

    use super::*;
    use crate::function::FunctionContext;
    #[test]
    fn test_subvector() {
        let func = VectorSubvectorFunction;

        let input0 = Arc::new(StringVector::from(vec![
            Some("[1.0, 2.0, 3.0, 4.0, 5.0]".to_string()),
            Some("[6.0, 7.0, 8.0, 9.0, 10.0]".to_string()),
            None,
            Some("[11.0, 12.0, 13.0]".to_string()),
        ]));
        let input1 = Arc::new(Int64Vector::from(vec![Some(1), Some(0), Some(0), Some(1)]));
        let input2 = Arc::new(Int64Vector::from(vec![Some(3), Some(5), Some(2), Some(3)]));

        let result = func
            .eval(&FunctionContext::default(), &[input0, input1, input2])
            .unwrap();

        let result = result.as_ref();
        assert_eq!(result.len(), 4);
        assert_eq!(
            result.get_ref(0).as_binary().unwrap(),
            Some(veclit_to_binlit(&[2.0, 3.0]).as_slice())
        );
        assert_eq!(
            result.get_ref(1).as_binary().unwrap(),
            Some(veclit_to_binlit(&[6.0, 7.0, 8.0, 9.0, 10.0]).as_slice())
        );
        assert!(result.get_ref(2).is_null());
        assert_eq!(
            result.get_ref(3).as_binary().unwrap(),
            Some(veclit_to_binlit(&[12.0, 13.0]).as_slice())
        );
    }
    #[test]
    fn test_subvector_error() {
        let func = VectorSubvectorFunction;

        let input0 = Arc::new(StringVector::from(vec![
            Some("[1.0, 2.0, 3.0]".to_string()),
            Some("[4.0, 5.0, 6.0]".to_string()),
        ]));
        let input1 = Arc::new(Int64Vector::from(vec![Some(1), Some(2)]));
        let input2 = Arc::new(Int64Vector::from(vec![Some(3)]));

        let result = func.eval(&FunctionContext::default(), &[input0, input1, input2]);

        match result {
            Err(Error::InvalidFuncArgs { err_msg, .. }) => {
                assert_eq!(
                    err_msg,
                    "The lengths of the vector are not aligned, args 0: 2, args 1: 2, args 2: 1"
                )
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn test_subvector_invalid_indices() {
        let func = VectorSubvectorFunction;

        let input0 = Arc::new(StringVector::from(vec![
            Some("[1.0, 2.0, 3.0]".to_string()),
            Some("[4.0, 5.0, 6.0]".to_string()),
        ]));
        let input1 = Arc::new(Int64Vector::from(vec![Some(1), Some(3)]));
        let input2 = Arc::new(Int64Vector::from(vec![Some(3), Some(4)]));

        let result = func.eval(&FunctionContext::default(), &[input0, input1, input2]);

        match result {
            Err(Error::InvalidFuncArgs { err_msg, .. }) => {
                assert_eq!(
                    err_msg,
                    "Invalid start and end indices: start=3, end=4, vec_len=3"
                )
            }
            _ => unreachable!(),
        }
    }
}
