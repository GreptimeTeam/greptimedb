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

const NAME: &str = "vec_div";

/// Divides corresponding elements of two vectors.
///
/// # Example
///
/// ```sql
/// SELECT vec_to_string(vec_div("[2, 4, 6]", "[2, 2, 2]")) as result;
///
/// +---------+
/// | result  |
/// +---------+
/// | [1,2,3] |
/// +---------+
///
/// ```
#[derive(Debug, Clone, Default)]
pub struct VectorDivFunction;

impl Function for VectorDivFunction {
    fn name(&self) -> &str {
        NAME
    }

    fn return_type(&self, _input_types: &[ConcreteDataType]) -> Result<ConcreteDataType> {
        Ok(ConcreteDataType::binary_datatype())
    }

    fn signature(&self) -> Signature {
        helper::one_of_sigs2(
            vec![
                ConcreteDataType::string_datatype(),
                ConcreteDataType::binary_datatype(),
            ],
            vec![
                ConcreteDataType::string_datatype(),
                ConcreteDataType::binary_datatype(),
            ],
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
        let mut result = BinaryVectorBuilder::with_capacity(len);
        if len == 0 {
            return Ok(result.to_vector());
        }

        let arg0_const = as_veclit_if_const(arg0)?;
        let arg1_const = as_veclit_if_const(arg1)?;

        for i in 0..len {
            let arg0 = match arg0_const.as_ref() {
                Some(arg0) => Some(Cow::Borrowed(arg0.as_ref())),
                None => as_veclit(arg0.get_ref(i))?,
            };

            let arg1 = match arg1_const.as_ref() {
                Some(arg1) => Some(Cow::Borrowed(arg1.as_ref())),
                None => as_veclit(arg1.get_ref(i))?,
            };

            if let (Some(arg0), Some(arg1)) = (arg0, arg1) {
                ensure!(
                    arg0.len() == arg1.len(),
                    InvalidFuncArgsSnafu {
                        err_msg: format!(
                            "The length of the vectors must match for division, have: {} vs {}",
                            arg0.len(),
                            arg1.len()
                        ),
                    }
                );
                let vec0 = DVectorView::from_slice(&arg0, arg0.len());
                let vec1 = DVectorView::from_slice(&arg1, arg1.len());
                let vec_res = vec0.component_div(&vec1);

                let veclit = vec_res.as_slice();
                let binlit = veclit_to_binlit(veclit);
                result.push(Some(&binlit));
            } else {
                result.push_null();
            }
        }

        Ok(result.to_vector())
    }
}

impl Display for VectorDivFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", NAME.to_ascii_uppercase())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_query::error;
    use datatypes::vectors::StringVector;

    use super::*;

    #[test]
    fn test_vector_mul() {
        let func = VectorDivFunction;

        let vec0 = vec![1.0, 2.0, 3.0];
        let vec1 = vec![1.0, 1.0];
        let (len0, len1) = (vec0.len(), vec1.len());
        let input0 = Arc::new(StringVector::from(vec![Some(format!("{vec0:?}"))]));
        let input1 = Arc::new(StringVector::from(vec![Some(format!("{vec1:?}"))]));

        let err = func
            .eval(&FunctionContext::default(), &[input0, input1])
            .unwrap_err();

        match err {
            error::Error::InvalidFuncArgs { err_msg, .. } => {
                assert_eq!(
                    err_msg,
                    format!(
                        "The length of the vectors must match for division, have: {} vs {}",
                        len0, len1
                    )
                )
            }
            _ => unreachable!(),
        }

        let input0 = Arc::new(StringVector::from(vec![
            Some("[1.0,2.0,3.0]".to_string()),
            Some("[8.0,10.0,12.0]".to_string()),
            Some("[7.0,8.0,9.0]".to_string()),
            None,
        ]));

        let input1 = Arc::new(StringVector::from(vec![
            Some("[1.0,1.0,1.0]".to_string()),
            Some("[2.0,2.0,2.0]".to_string()),
            None,
            Some("[3.0,3.0,3.0]".to_string()),
        ]));

        let result = func
            .eval(&FunctionContext::default(), &[input0, input1])
            .unwrap();

        let result = result.as_ref();
        assert_eq!(result.len(), 4);
        assert_eq!(
            result.get_ref(0).as_binary().unwrap(),
            Some(veclit_to_binlit(&[1.0, 2.0, 3.0]).as_slice())
        );
        assert_eq!(
            result.get_ref(1).as_binary().unwrap(),
            Some(veclit_to_binlit(&[4.0, 5.0, 6.0]).as_slice())
        );
        assert!(result.get_ref(2).is_null());
        assert!(result.get_ref(3).is_null());

        let input0 = Arc::new(StringVector::from(vec![Some("[1.0,-2.0]".to_string())]));
        let input1 = Arc::new(StringVector::from(vec![Some("[0.0,0.0]".to_string())]));

        let result = func
            .eval(&FunctionContext::default(), &[input0, input1])
            .unwrap();

        let result = result.as_ref();
        assert_eq!(
            result.get_ref(0).as_binary().unwrap(),
            Some(veclit_to_binlit(&[f64::INFINITY as f32, f64::NEG_INFINITY as f32]).as_slice())
        );
    }
}
