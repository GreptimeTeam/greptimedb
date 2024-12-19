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
use datatypes::vectors::{BinaryVectorBuilder, MutableVector, VectorRef};
use nalgebra::DVectorView;
use snafu::ensure;

use crate::function::{Function, FunctionContext};
use crate::helper;
use crate::scalars::vector::impl_conv::{as_veclit, as_veclit_if_const, veclit_to_binlit};

const NAME: &str = "vec_scalar_mul";

/// Multiples a scalar to each element of a vector.
///
/// # Example
///
/// ```sql
/// SELECT vec_to_string(vec_scalar_mul(2, "[1, 2, 3]")) as result;
///
/// +---------+
/// | result  |
/// +---------+
/// | [2,4,6] |
/// +---------+
///
/// -- 1/scalar to simulate division
/// SELECT vec_to_string(vec_scalar_mul(0.5, "[2, 4, 6]")) as result;
///
/// +---------+
/// | result  |
/// +---------+
/// | [1,2,3] |
/// +---------+
/// ```
#[derive(Debug, Clone, Default)]
pub struct ScalarMulFunction;

impl Function for ScalarMulFunction {
    fn name(&self) -> &str {
        NAME
    }

    fn return_type(&self, _input_types: &[ConcreteDataType]) -> Result<ConcreteDataType> {
        Ok(ConcreteDataType::binary_datatype())
    }

    fn signature(&self) -> Signature {
        helper::one_of_sigs2(
            vec![ConcreteDataType::float64_datatype()],
            vec![
                ConcreteDataType::string_datatype(),
                ConcreteDataType::binary_datatype(),
            ],
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
        let arg0 = &columns[0];
        let arg1 = &columns[1];

        let len = arg0.len();
        let mut result = BinaryVectorBuilder::with_capacity(len);
        if len == 0 {
            return Ok(result.to_vector());
        }

        let arg1_const = as_veclit_if_const(arg1)?;

        for i in 0..len {
            let arg0 = arg0.get(i).as_f64_lossy();
            let Some(arg0) = arg0 else {
                result.push_null();
                continue;
            };

            let arg1 = match arg1_const.as_ref() {
                Some(arg1) => Some(Cow::Borrowed(arg1.as_ref())),
                None => as_veclit(arg1.get_ref(i))?,
            };
            let Some(arg1) = arg1 else {
                result.push_null();
                continue;
            };

            let vec = DVectorView::from_slice(&arg1, arg1.len());
            let vec_res = vec.scale(arg0 as _);

            let veclit = vec_res.as_slice();
            let binlit = veclit_to_binlit(veclit);
            result.push(Some(&binlit));
        }

        Ok(result.to_vector())
    }
}

impl Display for ScalarMulFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", NAME.to_ascii_uppercase())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datatypes::vectors::{Float32Vector, StringVector};

    use super::*;

    #[test]
    fn test_scalar_mul() {
        let func = ScalarMulFunction;

        let input0 = Arc::new(Float32Vector::from(vec![
            Some(2.0),
            Some(-0.5),
            None,
            Some(3.0),
        ]));
        let input1 = Arc::new(StringVector::from(vec![
            Some("[1.0,2.0,3.0]".to_string()),
            Some("[8.0,10.0,12.0]".to_string()),
            Some("[7.0,8.0,9.0]".to_string()),
            None,
        ]));

        let result = func
            .eval(FunctionContext::default(), &[input0, input1])
            .unwrap();

        let result = result.as_ref();
        assert_eq!(result.len(), 4);
        assert_eq!(
            result.get_ref(0).as_binary().unwrap(),
            Some(veclit_to_binlit(&[2.0, 4.0, 6.0]).as_slice())
        );
        assert_eq!(
            result.get_ref(1).as_binary().unwrap(),
            Some(veclit_to_binlit(&[-4.0, -5.0, -6.0]).as_slice())
        );
        assert!(result.get_ref(2).is_null());
        assert!(result.get_ref(3).is_null());
    }
}
