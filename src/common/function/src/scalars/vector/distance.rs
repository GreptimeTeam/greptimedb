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
use std::sync::Arc;

use common_query::error::{InvalidFuncArgsSnafu, Result};
use common_query::prelude::Signature;
use datatypes::prelude::ConcreteDataType;
use datatypes::scalars::ScalarVectorBuilder;
use datatypes::value::ValueRef;
use datatypes::vectors::{Float64VectorBuilder, MutableVector, Vector, VectorRef};
use snafu::ensure;

use crate::function::{Function, FunctionContext};
use crate::helper;

macro_rules! define_distance_function {
    ($StructName:ident, $display_name:expr, $similarity_method:ident) => {

        /// A function calculates the distance between two vectors.

        #[derive(Debug, Clone, Default)]
        pub struct $StructName;

        impl Function for $StructName {
            fn name(&self) -> &str {
                $display_name
            }

            fn return_type(&self, _input_types: &[ConcreteDataType]) -> Result<ConcreteDataType> {
                Ok(ConcreteDataType::float64_datatype())
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
                let mut result = Float64VectorBuilder::with_capacity(size);
                if size == 0 {
                    return Ok(result.to_vector());
                }

                let arg0_const = parse_if_constant_string(arg0)?;
                let arg1_const = parse_if_constant_string(arg1)?;

                for i in 0..size {
                    let vec0 = match arg0_const.as_ref() {
                        Some(a) => Some(Cow::Borrowed(a.as_slice())),
                        None => as_vector(arg0.get_ref(i))?,
                    };
                    let vec1 = match arg1_const.as_ref() {
                        Some(b) => Some(Cow::Borrowed(b.as_slice())),
                        None => as_vector(arg1.get_ref(i))?,
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

                        let f = <f32 as simsimd::SpatialSimilarity>::$similarity_method;
                        // Safe: checked if the length of the vectors match
                        let d = f(vec0.as_ref(), vec1.as_ref()).unwrap();
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

define_distance_function!(CosDistanceFunction, "cos_distance", cos);
define_distance_function!(L2SqDistanceFunction, "l2sq_distance", l2sq);
define_distance_function!(DotProductFunction, "dot_product", dot);

/// Parse a vector value if the value is a constant string.
fn parse_if_constant_string(arg: &Arc<dyn Vector>) -> Result<Option<Vec<f32>>> {
    if !arg.is_const() {
        return Ok(None);
    }
    if arg.data_type() != ConcreteDataType::string_datatype() {
        return Ok(None);
    }
    arg.get_ref(0)
        .as_string()
        .unwrap() // Safe: checked if it is a string
        .map(parse_f32_vector_from_string)
        .transpose()
}

/// Convert a value to a vector value.
/// Supported data types are binary and string.
fn as_vector(arg: ValueRef<'_>) -> Result<Option<Cow<'_, [f32]>>> {
    match arg.data_type() {
        ConcreteDataType::Binary(_) => arg
            .as_binary()
            .unwrap() // Safe: checked if it is a binary
            .map(|bytes| Ok(Cow::Borrowed(binary_as_vector(bytes)?)))
            .transpose(),
        ConcreteDataType::String(_) => arg
            .as_string()
            .unwrap() // Safe: checked if it is a string
            .map(|s| Ok(Cow::Owned(parse_f32_vector_from_string(s)?)))
            .transpose(),
        ConcreteDataType::Null(_) => Ok(None),
        _ => InvalidFuncArgsSnafu {
            err_msg: format!("Unsupported data type: {:?}", arg.data_type()),
        }
        .fail(),
    }
}

/// Convert a u8 slice to a vector value.
fn binary_as_vector(bytes: &[u8]) -> Result<&[f32]> {
    if bytes.len() % 4 != 0 {
        return InvalidFuncArgsSnafu {
            err_msg: format!("Invalid binary length of vector: {}", bytes.len()),
        }
        .fail();
    }

    unsafe {
        let num_floats = bytes.len() / 4;
        let floats: &[f32] = std::slice::from_raw_parts(bytes.as_ptr() as *const f32, num_floats);
        Ok(floats)
    }
}

/// Parse a string to a vector value.
/// Valid inputs are strings like "[1.0, 2.0, 3.0]".
fn parse_f32_vector_from_string(s: &str) -> Result<Vec<f32>> {
    let trimmed = s.trim();
    if !trimmed.starts_with('[') || !trimmed.ends_with(']') {
        return InvalidFuncArgsSnafu {
            err_msg: format!(
                "Failed to parse {s} to Vector value: not properly enclosed in brackets"
            ),
        }
        .fail();
    }
    let content = trimmed[1..trimmed.len() - 1].trim();
    if content.is_empty() {
        return Ok(Vec::new());
    }

    content
        .split(',')
        .map(|s| s.trim().parse::<f32>())
        .collect::<std::result::Result<_, _>>()
        .map_err(|e| {
            InvalidFuncArgsSnafu {
                err_msg: format!("Failed to parse {s} to Vector value: {e}"),
            }
            .build()
        })
}

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

    #[test]
    fn test_parse_vector_from_string() {
        let result = parse_f32_vector_from_string("[1.0, 2.0, 3.0]").unwrap();
        assert_eq!(result, vec![1.0, 2.0, 3.0]);

        let result = parse_f32_vector_from_string("[]").unwrap();
        assert_eq!(result, Vec::<f32>::new());

        let result = parse_f32_vector_from_string("[1.0, a, 3.0]");
        assert!(result.is_err());
    }

    #[test]
    fn test_binary_as_vector() {
        let bytes = [0, 0, 128, 63];
        let result = binary_as_vector(&bytes).unwrap();
        assert_eq!(result, &[1.0]);

        let invalid_bytes = [0, 0, 128];
        let result = binary_as_vector(&invalid_bytes);
        assert!(result.is_err());
    }
}
