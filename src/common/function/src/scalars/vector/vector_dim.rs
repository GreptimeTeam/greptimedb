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

use common_query::error::InvalidFuncArgsSnafu;
use common_query::prelude::{Signature, TypeSignature, Volatility};
use datatypes::prelude::ConcreteDataType;
use datatypes::scalars::ScalarVectorBuilder;
use datatypes::vectors::{MutableVector, UInt64VectorBuilder, VectorRef};
use snafu::ensure;

use crate::function::{Function, FunctionContext};
use crate::scalars::vector::impl_conv::{as_veclit, as_veclit_if_const};

const NAME: &str = "vec_dim";

/// Returns the dimension of the vector.
///
/// # Example
///
/// ```sql
/// SELECT vec_dim('[7.0, 8.0, 9.0, 10.0]');
///
/// +---------------------------------------------------------------+
/// | vec_dim(Utf8("[7.0, 8.0, 9.0, 10.0]"))                        |
/// +---------------------------------------------------------------+
/// | 4                                                             |
/// +---------------------------------------------------------------+
///
#[derive(Debug, Clone, Default)]
pub struct VectorDimFunction;

impl Function for VectorDimFunction {
    fn name(&self) -> &str {
        NAME
    }

    fn return_type(
        &self,
        _input_types: &[ConcreteDataType],
    ) -> common_query::error::Result<ConcreteDataType> {
        Ok(ConcreteDataType::uint64_datatype())
    }

    fn signature(&self) -> Signature {
        Signature::one_of(
            vec![
                TypeSignature::Exact(vec![ConcreteDataType::string_datatype()]),
                TypeSignature::Exact(vec![ConcreteDataType::binary_datatype()]),
            ],
            Volatility::Immutable,
        )
    }

    fn eval(
        &self,
        _func_ctx: &FunctionContext,
        columns: &[VectorRef],
    ) -> common_query::error::Result<VectorRef> {
        ensure!(
            columns.len() == 1,
            InvalidFuncArgsSnafu {
                err_msg: format!(
                    "The length of the args is not correct, expect exactly one, have: {}",
                    columns.len()
                )
            }
        );
        let arg0 = &columns[0];

        let len = arg0.len();
        let mut result = UInt64VectorBuilder::with_capacity(len);
        if len == 0 {
            return Ok(result.to_vector());
        }

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
            result.push(Some(arg0.len() as u64));
        }

        Ok(result.to_vector())
    }
}

impl Display for VectorDimFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", NAME.to_ascii_uppercase())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_query::error::Error;
    use datatypes::vectors::StringVector;

    use super::*;

    #[test]
    fn test_vec_dim() {
        let func = VectorDimFunction;

        let input0 = Arc::new(StringVector::from(vec![
            Some("[0.0,2.0,3.0]".to_string()),
            Some("[1.0,2.0,3.0,4.0]".to_string()),
            None,
            Some("[5.0]".to_string()),
        ]));

        let result = func.eval(&FunctionContext::default(), &[input0]).unwrap();

        let result = result.as_ref();
        assert_eq!(result.len(), 4);
        assert_eq!(result.get_ref(0).as_u64().unwrap(), Some(3));
        assert_eq!(result.get_ref(1).as_u64().unwrap(), Some(4));
        assert!(result.get_ref(2).is_null());
        assert_eq!(result.get_ref(3).as_u64().unwrap(), Some(1));
    }

    #[test]
    fn test_dim_error() {
        let func = VectorDimFunction;

        let input0 = Arc::new(StringVector::from(vec![
            Some("[1.0,2.0,3.0]".to_string()),
            Some("[4.0,5.0,6.0]".to_string()),
            None,
            Some("[2.0,3.0,3.0]".to_string()),
        ]));
        let input1 = Arc::new(StringVector::from(vec![
            Some("[1.0,1.0,1.0]".to_string()),
            Some("[6.0,5.0,4.0]".to_string()),
            Some("[3.0,2.0,2.0]".to_string()),
        ]));

        let result = func.eval(&FunctionContext::default(), &[input0, input1]);

        match result {
            Err(Error::InvalidFuncArgs { err_msg, .. }) => {
                assert_eq!(
                    err_msg,
                    "The length of the args is not correct, expect exactly one, have: 2"
                )
            }
            _ => unreachable!(),
        }
    }
}
