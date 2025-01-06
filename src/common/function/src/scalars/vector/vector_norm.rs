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
use common_query::prelude::{Signature, TypeSignature, Volatility};
use datatypes::prelude::ConcreteDataType;
use datatypes::scalars::ScalarVectorBuilder;
use datatypes::vectors::{BinaryVectorBuilder, MutableVector, VectorRef};
use nalgebra::DVectorView;
use snafu::ensure;

use crate::function::{Function, FunctionContext};
use crate::scalars::vector::impl_conv::{as_veclit, as_veclit_if_const, veclit_to_binlit};

const NAME: &str = "vec_norm";

/// Normalizes the vector to length 1, returns a vector.
/// This's equivalent to `VECTOR_SCALAR_MUL(1/SQRT(VECTOR_ELEM_SUM(VECTOR_MUL(v, v))), v)`.
///
/// # Example
///
/// ```sql
/// SELECT vec_to_string(vec_norm('[7.0, 8.0, 9.0]'));
///
/// +--------------------------------------------------+
/// | vec_to_string(vec_norm(Utf8("[7.0, 8.0, 9.0]"))) |
/// +--------------------------------------------------+
/// | [0.013888889,0.015873017,0.017857144]            |
/// +--------------------------------------------------+
///
/// ```
#[derive(Debug, Clone, Default)]
pub struct VectorNormFunction;

impl Function for VectorNormFunction {
    fn name(&self) -> &str {
        NAME
    }

    fn return_type(&self, _input_types: &[ConcreteDataType]) -> Result<ConcreteDataType> {
        Ok(ConcreteDataType::binary_datatype())
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
        _func_ctx: FunctionContext,
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
            let Some(arg0) = arg0 else {
                result.push_null();
                continue;
            };

            let vec0 = DVectorView::from_slice(&arg0, arg0.len());
            let vec1 = DVectorView::from_slice(&arg0, arg0.len());
            let vec2scalar = vec1.component_mul(&vec0);
            let scalar_var = vec2scalar.sum().sqrt();

            let vec = DVectorView::from_slice(&arg0, arg0.len());
            // Use unscale to avoid division by zero and keep more precision as possible
            let vec_res = vec.unscale(scalar_var);

            let veclit = vec_res.as_slice();
            let binlit = veclit_to_binlit(veclit);
            result.push(Some(&binlit));
        }

        Ok(result.to_vector())
    }
}

impl Display for VectorNormFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", NAME.to_ascii_uppercase())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datatypes::vectors::StringVector;

    use super::*;

    #[test]
    fn test_vec_norm() {
        let func = VectorNormFunction;

        let input0 = Arc::new(StringVector::from(vec![
            Some("[0.0,2.0,3.0]".to_string()),
            Some("[1.0,2.0,3.0]".to_string()),
            Some("[7.0,8.0,9.0]".to_string()),
            Some("[7.0,-8.0,9.0]".to_string()),
            None,
        ]));

        let result = func.eval(FunctionContext::default(), &[input0]).unwrap();

        let result = result.as_ref();
        assert_eq!(result.len(), 5);
        assert_eq!(
            result.get_ref(0).as_binary().unwrap(),
            Some(veclit_to_binlit(&[0.0, 0.5547002, 0.8320503]).as_slice())
        );
        assert_eq!(
            result.get_ref(1).as_binary().unwrap(),
            Some(veclit_to_binlit(&[0.26726124, 0.5345225, 0.8017837]).as_slice())
        );
        assert_eq!(
            result.get_ref(2).as_binary().unwrap(),
            Some(veclit_to_binlit(&[0.5025707, 0.5743665, 0.64616233]).as_slice())
        );
        assert_eq!(
            result.get_ref(3).as_binary().unwrap(),
            Some(veclit_to_binlit(&[0.5025707, -0.5743665, 0.64616233]).as_slice())
        );
        assert!(result.get_ref(4).is_null());
    }
}
