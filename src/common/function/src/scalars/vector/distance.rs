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

use common_query::error::Result;
use datafusion::logical_expr::ColumnarValue;
use datafusion_common::ScalarValue;
use datafusion_expr::{ScalarFunctionArgs, Signature};
use datatypes::arrow::datatypes::DataType;

use crate::function::Function;
use crate::helper;

macro_rules! define_distance_function {
    ($StructName:ident, $display_name:expr, $similarity_method:path) => {
        /// A function calculates the distance between two vectors.

        #[derive(Debug, Clone, Default)]
        pub struct $StructName;

        impl Function for $StructName {
            fn name(&self) -> &str {
                $display_name
            }

            fn return_type(&self, _: &[DataType]) -> Result<DataType> {
                Ok(DataType::Float32)
            }

            fn signature(&self) -> Signature {
                helper::one_of_sigs2(
                    vec![
                        DataType::Utf8,
                        DataType::Utf8View,
                        DataType::Binary,
                        DataType::BinaryView,
                    ],
                    vec![
                        DataType::Utf8,
                        DataType::Utf8View,
                        DataType::Binary,
                        DataType::BinaryView,
                    ],
                )
            }

            fn invoke_with_args(
                &self,
                args: ScalarFunctionArgs,
            ) -> datafusion_common::Result<ColumnarValue> {
                let body = |v0: &Option<Cow<[f32]>>,
                            v1: &Option<Cow<[f32]>>|
                 -> datafusion_common::Result<ScalarValue> {
                    let result = if let (Some(v0), Some(v1)) = (v0, v1) {
                        if v0.len() != v1.len() {
                            return Err(datafusion_common::DataFusionError::Execution(format!(
                                "vectors length not match: {}",
                                self.name()
                            )));
                        }

                        let d = $similarity_method(v0, v1);
                        Some(d)
                    } else {
                        None
                    };
                    Ok(ScalarValue::Float32(result))
                };

                let calculator = $crate::scalars::vector::VectorCalculator {
                    name: self.name(),
                    func: body,
                };
                calculator.invoke_with_vectors(args)
            }
        }

        impl Display for $StructName {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{}", $display_name.to_ascii_uppercase())
            }
        }
    };
}

define_distance_function!(CosDistanceFunction, "vec_cos_distance", cos::cos);
define_distance_function!(L2SqDistanceFunction, "vec_l2sq_distance", l2sq::l2sq);
define_distance_function!(DotProductFunction, "vec_dot_product", dot::dot);

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::Field;
    use datafusion::arrow::array::{Array, ArrayRef, AsArray, BinaryArray, StringViewArray};
    use datafusion::arrow::datatypes::Float32Type;
    use datafusion_common::config::ConfigOptions;

    use super::*;

    fn test_invoke(func: &dyn Function, args: &[ArrayRef]) -> datafusion_common::Result<ArrayRef> {
        let number_rows = args[0].len();
        let args = ScalarFunctionArgs {
            args: args
                .iter()
                .map(|x| ColumnarValue::Array(x.clone()))
                .collect::<Vec<_>>(),
            arg_fields: vec![],
            number_rows,
            return_field: Arc::new(Field::new("x", DataType::Float32, false)),
            config_options: Arc::new(ConfigOptions::new()),
        };
        func.invoke_with_args(args)
            .and_then(|x| x.to_array(number_rows))
    }

    #[test]
    fn test_distance_string_string() {
        let funcs = [
            Box::new(CosDistanceFunction {}) as Box<dyn Function>,
            Box::new(L2SqDistanceFunction {}) as Box<dyn Function>,
            Box::new(DotProductFunction {}) as Box<dyn Function>,
        ];

        for func in funcs {
            let vec1: ArrayRef = Arc::new(StringViewArray::from(vec![
                Some("[0.0, 1.0]"),
                Some("[1.0, 0.0]"),
                None,
                Some("[1.0, 0.0]"),
            ]));
            let vec2: ArrayRef = Arc::new(StringViewArray::from(vec![
                Some("[0.0, 1.0]"),
                Some("[0.0, 1.0]"),
                Some("[0.0, 1.0]"),
                None,
            ]));

            let result = test_invoke(func.as_ref(), &[vec1.clone(), vec2.clone()]).unwrap();
            let result = result.as_primitive::<Float32Type>();

            assert!(!result.is_null(0));
            assert!(!result.is_null(1));
            assert!(result.is_null(2));
            assert!(result.is_null(3));

            let result = test_invoke(func.as_ref(), &[vec2, vec1]).unwrap();
            let result = result.as_primitive::<Float32Type>();

            assert!(!result.is_null(0));
            assert!(!result.is_null(1));
            assert!(result.is_null(2));
            assert!(result.is_null(3));
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
            let vec1: ArrayRef = Arc::new(BinaryArray::from_iter(vec![
                Some(vec![0, 0, 0, 0, 0, 0, 128, 63]),
                Some(vec![0, 0, 128, 63, 0, 0, 0, 0]),
                None,
                Some(vec![0, 0, 128, 63, 0, 0, 0, 0]),
            ]));
            let vec2: ArrayRef = Arc::new(BinaryArray::from_iter(vec![
                // [0.0, 1.0]
                Some(vec![0, 0, 0, 0, 0, 0, 128, 63]),
                Some(vec![0, 0, 0, 0, 0, 0, 128, 63]),
                Some(vec![0, 0, 0, 0, 0, 0, 128, 63]),
                None,
            ]));

            let result = test_invoke(func.as_ref(), &[vec1.clone(), vec2.clone()]).unwrap();
            let result = result.as_primitive::<Float32Type>();

            assert!(!result.is_null(0));
            assert!(!result.is_null(1));
            assert!(result.is_null(2));
            assert!(result.is_null(3));

            let result = test_invoke(func.as_ref(), &[vec2, vec1]).unwrap();
            let result = result.as_primitive::<Float32Type>();

            assert!(!result.is_null(0));
            assert!(!result.is_null(1));
            assert!(result.is_null(2));
            assert!(result.is_null(3));
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
            let vec1: ArrayRef = Arc::new(StringViewArray::from(vec![
                Some("[0.0, 1.0]"),
                Some("[1.0, 0.0]"),
                None,
                Some("[1.0, 0.0]"),
            ]));
            let vec2: ArrayRef = Arc::new(BinaryArray::from_iter(vec![
                // [0.0, 1.0]
                Some(vec![0, 0, 0, 0, 0, 0, 128, 63]),
                Some(vec![0, 0, 0, 0, 0, 0, 128, 63]),
                Some(vec![0, 0, 0, 0, 0, 0, 128, 63]),
                None,
            ]));

            let result = test_invoke(func.as_ref(), &[vec1.clone(), vec2.clone()]).unwrap();
            let result = result.as_primitive::<Float32Type>();

            assert!(!result.is_null(0));
            assert!(!result.is_null(1));
            assert!(result.is_null(2));
            assert!(result.is_null(3));

            let result = test_invoke(func.as_ref(), &[vec2, vec1]).unwrap();
            let result = result.as_primitive::<Float32Type>();

            assert!(!result.is_null(0));
            assert!(!result.is_null(1));
            assert!(result.is_null(2));
            assert!(result.is_null(3));
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
            let vec1: ArrayRef = Arc::new(StringViewArray::from(vec!["[1.0]"]));
            let vec2: ArrayRef = Arc::new(StringViewArray::from(vec!["[1.0, 1.0]"]));
            let result = test_invoke(func.as_ref(), &[vec1, vec2]);
            assert!(result.is_err());

            let vec1: ArrayRef = Arc::new(BinaryArray::from_iter_values(vec![vec![0, 0, 128, 63]]));
            let vec2: ArrayRef = Arc::new(BinaryArray::from_iter_values(vec![vec![
                0, 0, 128, 63, 0, 0, 0, 64,
            ]]));
            let result = test_invoke(func.as_ref(), &[vec1, vec2]);
            assert!(result.is_err());
        }
    }
}
