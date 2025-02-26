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

mod cos;
mod dot;
mod l2sq;

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

macro_rules! define_distance_function {
    ($StructName:ident, $display_name:expr, $similarity_method:path) => {

        /// A function calculates the distance between two vectors.

        #[derive(Debug, Clone, Default)]
        pub struct $StructName;

        impl Function for $StructName {
            fn name(&self) -> &str {
                $display_name
            }

            fn return_type(&self, _input_types: &[ConcreteDataType]) -> Result<ConcreteDataType> {
                Ok(ConcreteDataType::float32_datatype())
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

                let size = arg0.len();
                let mut result = Float32VectorBuilder::with_capacity(size);
                if size == 0 {
                    return Ok(result.to_vector());
                }

                let arg0_const = as_veclit_if_const(arg0)?;
                let arg1_const = as_veclit_if_const(arg1)?;

                for i in 0..size {
                    let vec0 = match arg0_const.as_ref() {
                        Some(a) => Some(Cow::Borrowed(a.as_ref())),
                        None => as_veclit(arg0.get_ref(i))?,
                    };
                    let vec1 = match arg1_const.as_ref() {
                        Some(b) => Some(Cow::Borrowed(b.as_ref())),
                        None => as_veclit(arg1.get_ref(i))?,
                    };

                    if let (Some(vec0), Some(vec1)) = (vec0, vec1) {
                        ensure!(
                            vec0.len() == vec1.len(),
                            InvalidFuncArgsSnafu {
                                err_msg: format!(
                                    "The length of the vectors must match to calculate distance, have: {} vs {}",
                                    vec0.len(),
                                    vec1.len()
                                ),
                            }
                        );

                        // Checked if the length of the vectors match
                        let d = $similarity_method(vec0.as_ref(), vec1.as_ref());
                        result.push(Some(d));
                    } else {
                        result.push_null();
                    }
                }

                return Ok(result.to_vector());
            }
        }

        impl Display for $StructName {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{}", $display_name.to_ascii_uppercase())
            }
        }
    }
}

define_distance_function!(CosDistanceFunction, "vec_cos_distance", cos::cos);
define_distance_function!(L2SqDistanceFunction, "vec_l2sq_distance", l2sq::l2sq);
define_distance_function!(DotProductFunction, "vec_dot_product", dot::dot);

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datatypes::vectors::{BinaryVector, ConstantVector, StringVector};

    use super::*;

    #[test]
    fn test_distance_string_string() {
        let funcs = [
            Box::new(CosDistanceFunction {}) as Box<dyn Function>,
            Box::new(L2SqDistanceFunction {}) as Box<dyn Function>,
            Box::new(DotProductFunction {}) as Box<dyn Function>,
        ];

        for func in funcs {
            let vec1 = Arc::new(StringVector::from(vec![
                Some("[0.0, 1.0]"),
                Some("[1.0, 0.0]"),
                None,
                Some("[1.0, 0.0]"),
            ])) as VectorRef;
            let vec2 = Arc::new(StringVector::from(vec![
                Some("[0.0, 1.0]"),
                Some("[0.0, 1.0]"),
                Some("[0.0, 1.0]"),
                None,
            ])) as VectorRef;

            let result = func
                .eval(FunctionContext::default(), &[vec1.clone(), vec2.clone()])
                .unwrap();

            assert!(!result.get(0).is_null());
            assert!(!result.get(1).is_null());
            assert!(result.get(2).is_null());
            assert!(result.get(3).is_null());

            let result = func
                .eval(FunctionContext::default(), &[vec2, vec1])
                .unwrap();

            assert!(!result.get(0).is_null());
            assert!(!result.get(1).is_null());
            assert!(result.get(2).is_null());
            assert!(result.get(3).is_null());
        }
    }

    #[test]
    fn test_distance_binary_binary() {
        let funcs = [
            Box::new(CosDistanceFunction {}) as Box<dyn Function>,
            Box::new(L2SqDistanceFunction {}) as Box<dyn Function>,
            Box::new(DotProductFunction {}) as Box<dyn Function>,
        ];

        for func in funcs {
            let vec1 = Arc::new(BinaryVector::from(vec![
                Some(vec![0, 0, 0, 0, 0, 0, 128, 63]),
                Some(vec![0, 0, 128, 63, 0, 0, 0, 0]),
                None,
                Some(vec![0, 0, 128, 63, 0, 0, 0, 0]),
            ])) as VectorRef;
            let vec2 = Arc::new(BinaryVector::from(vec![
                // [0.0, 1.0]
                Some(vec![0, 0, 0, 0, 0, 0, 128, 63]),
                Some(vec![0, 0, 0, 0, 0, 0, 128, 63]),
                Some(vec![0, 0, 0, 0, 0, 0, 128, 63]),
                None,
            ])) as VectorRef;

            let result = func
                .eval(FunctionContext::default(), &[vec1.clone(), vec2.clone()])
                .unwrap();

            assert!(!result.get(0).is_null());
            assert!(!result.get(1).is_null());
            assert!(result.get(2).is_null());
            assert!(result.get(3).is_null());

            let result = func
                .eval(FunctionContext::default(), &[vec2, vec1])
                .unwrap();

            assert!(!result.get(0).is_null());
            assert!(!result.get(1).is_null());
            assert!(result.get(2).is_null());
            assert!(result.get(3).is_null());
        }
    }

    #[test]
    fn test_distance_string_binary() {
        let funcs = [
            Box::new(CosDistanceFunction {}) as Box<dyn Function>,
            Box::new(L2SqDistanceFunction {}) as Box<dyn Function>,
            Box::new(DotProductFunction {}) as Box<dyn Function>,
        ];

        for func in funcs {
            let vec1 = Arc::new(StringVector::from(vec![
                Some("[0.0, 1.0]"),
                Some("[1.0, 0.0]"),
                None,
                Some("[1.0, 0.0]"),
            ])) as VectorRef;
            let vec2 = Arc::new(BinaryVector::from(vec![
                // [0.0, 1.0]
                Some(vec![0, 0, 0, 0, 0, 0, 128, 63]),
                Some(vec![0, 0, 0, 0, 0, 0, 128, 63]),
                Some(vec![0, 0, 0, 0, 0, 0, 128, 63]),
                None,
            ])) as VectorRef;

            let result = func
                .eval(FunctionContext::default(), &[vec1.clone(), vec2.clone()])
                .unwrap();

            assert!(!result.get(0).is_null());
            assert!(!result.get(1).is_null());
            assert!(result.get(2).is_null());
            assert!(result.get(3).is_null());

            let result = func
                .eval(FunctionContext::default(), &[vec2, vec1])
                .unwrap();

            assert!(!result.get(0).is_null());
            assert!(!result.get(1).is_null());
            assert!(result.get(2).is_null());
            assert!(result.get(3).is_null());
        }
    }

    #[test]
    fn test_distance_const_string() {
        let funcs = [
            Box::new(CosDistanceFunction {}) as Box<dyn Function>,
            Box::new(L2SqDistanceFunction {}) as Box<dyn Function>,
            Box::new(DotProductFunction {}) as Box<dyn Function>,
        ];

        for func in funcs {
            let const_str = Arc::new(ConstantVector::new(
                Arc::new(StringVector::from(vec!["[0.0, 1.0]"])),
                4,
            ));

            let vec1 = Arc::new(StringVector::from(vec![
                Some("[0.0, 1.0]"),
                Some("[1.0, 0.0]"),
                None,
                Some("[1.0, 0.0]"),
            ])) as VectorRef;
            let vec2 = Arc::new(BinaryVector::from(vec![
                // [0.0, 1.0]
                Some(vec![0, 0, 0, 0, 0, 0, 128, 63]),
                Some(vec![0, 0, 0, 0, 0, 0, 128, 63]),
                Some(vec![0, 0, 0, 0, 0, 0, 128, 63]),
                None,
            ])) as VectorRef;

            let result = func
                .eval(
                    FunctionContext::default(),
                    &[const_str.clone(), vec1.clone()],
                )
                .unwrap();

            assert!(!result.get(0).is_null());
            assert!(!result.get(1).is_null());
            assert!(result.get(2).is_null());
            assert!(!result.get(3).is_null());

            let result = func
                .eval(
                    FunctionContext::default(),
                    &[vec1.clone(), const_str.clone()],
                )
                .unwrap();

            assert!(!result.get(0).is_null());
            assert!(!result.get(1).is_null());
            assert!(result.get(2).is_null());
            assert!(!result.get(3).is_null());

            let result = func
                .eval(
                    FunctionContext::default(),
                    &[const_str.clone(), vec2.clone()],
                )
                .unwrap();

            assert!(!result.get(0).is_null());
            assert!(!result.get(1).is_null());
            assert!(!result.get(2).is_null());
            assert!(result.get(3).is_null());

            let result = func
                .eval(
                    FunctionContext::default(),
                    &[vec2.clone(), const_str.clone()],
                )
                .unwrap();

            assert!(!result.get(0).is_null());
            assert!(!result.get(1).is_null());
            assert!(!result.get(2).is_null());
            assert!(result.get(3).is_null());
        }
    }

    #[test]
    fn test_invalid_vector_length() {
        let funcs = [
            Box::new(CosDistanceFunction {}) as Box<dyn Function>,
            Box::new(L2SqDistanceFunction {}) as Box<dyn Function>,
            Box::new(DotProductFunction {}) as Box<dyn Function>,
        ];

        for func in funcs {
            let vec1 = Arc::new(StringVector::from(vec!["[1.0]"])) as VectorRef;
            let vec2 = Arc::new(StringVector::from(vec!["[1.0, 1.0]"])) as VectorRef;
            let result = func.eval(FunctionContext::default(), &[vec1, vec2]);
            assert!(result.is_err());

            let vec1 = Arc::new(BinaryVector::from(vec![vec![0, 0, 128, 63]])) as VectorRef;
            let vec2 =
                Arc::new(BinaryVector::from(vec![vec![0, 0, 128, 63, 0, 0, 0, 64]])) as VectorRef;
            let result = func.eval(FunctionContext::default(), &[vec1, vec2]);
            assert!(result.is_err());
        }
    }
}
