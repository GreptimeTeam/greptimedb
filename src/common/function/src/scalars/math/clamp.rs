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

use std::fmt::{self, Display};
use std::sync::Arc;

use common_query::error::Result;
use datafusion::arrow::array::{Array, ArrayRef, AsArray, PrimitiveArray};
use datafusion::arrow::datatypes::DataType as ArrowDataType;
use datafusion::logical_expr::{ColumnarValue, Volatility};
use datafusion_common::{DataFusionError, ScalarValue, utils};
use datafusion_expr::type_coercion::aggregates::NUMERICS;
use datafusion_expr::{ScalarFunctionArgs, Signature};

use crate::function::Function;

#[derive(Clone, Debug, Default)]
pub struct ClampFunction;

const CLAMP_NAME: &str = "clamp";

impl Function for ClampFunction {
    fn name(&self) -> &str {
        CLAMP_NAME
    }

    fn return_type(&self, input_types: &[ArrowDataType]) -> Result<ArrowDataType> {
        // Type check is done by `signature`
        Ok(input_types[0].clone())
    }

    fn signature(&self) -> Signature {
        // input, min, max
        Signature::uniform(3, NUMERICS.to_vec(), Volatility::Immutable)
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion_common::Result<ColumnarValue> {
        let [col, min, max] = utils::take_function_args(self.name(), args.args)?;
        clamp_impl(col, min, max)
    }
}

impl Display for ClampFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", CLAMP_NAME.to_ascii_uppercase())
    }
}

fn clamp_impl(
    col: ColumnarValue,
    min: ColumnarValue,
    max: ColumnarValue,
) -> datafusion_common::Result<ColumnarValue> {
    if col.data_type() != min.data_type() || min.data_type() != max.data_type() {
        return Err(DataFusionError::Execution(format!(
            "argument data types mismatch: {}, {}, {}",
            col.data_type(),
            min.data_type(),
            max.data_type(),
        )));
    }

    macro_rules! with_match_numerics_types {
        ($data_type:expr, | $_:tt $T:ident | $body:tt) => {{
            macro_rules! __with_ty__ {
                ( $_ $T:ident ) => {
                    $body
                };
            }

            use datafusion::arrow::datatypes::{
                Float32Type, Float64Type, Int8Type, Int16Type, Int32Type, Int64Type, UInt8Type,
                UInt16Type, UInt32Type, UInt64Type,
            };

            match $data_type {
                ArrowDataType::Int8 => Ok(__with_ty__! { Int8Type }),
                ArrowDataType::Int16 => Ok(__with_ty__! { Int16Type }),
                ArrowDataType::Int32 => Ok(__with_ty__! { Int32Type }),
                ArrowDataType::Int64 => Ok(__with_ty__! { Int64Type }),
                ArrowDataType::UInt8 => Ok(__with_ty__! { UInt8Type }),
                ArrowDataType::UInt16 => Ok(__with_ty__! { UInt16Type }),
                ArrowDataType::UInt32 => Ok(__with_ty__! { UInt32Type }),
                ArrowDataType::UInt64 => Ok(__with_ty__! { UInt64Type }),
                ArrowDataType::Float32 => Ok(__with_ty__! { Float32Type }),
                ArrowDataType::Float64 => Ok(__with_ty__! { Float64Type }),
                _ => Err(DataFusionError::Execution(format!(
                    "unsupported numeric data type: '{}'",
                    $data_type
                ))),
            }
        }};
    }

    macro_rules! clamp {
        ($v: ident, $min: ident, $max: ident) => {
            if $v < $min {
                $min
            } else if $v > $max {
                $max
            } else {
                $v
            }
        };
    }

    match (col, min, max) {
        (ColumnarValue::Scalar(col), ColumnarValue::Scalar(min), ColumnarValue::Scalar(max)) => {
            if min > max {
                return Err(DataFusionError::Execution(format!(
                    "min '{}' > max '{}'",
                    min, max
                )));
            }
            Ok(ColumnarValue::Scalar(clamp!(col, min, max)))
        }

        (ColumnarValue::Array(col), ColumnarValue::Array(min), ColumnarValue::Array(max)) => {
            if col.len() != min.len() || col.len() != max.len() {
                return Err(DataFusionError::Internal(
                    "arguments not of same length".to_string(),
                ));
            }
            let result = with_match_numerics_types!(
                col.data_type(),
                |$S| {
                    let col = col.as_primitive::<$S>();
                    let min = min.as_primitive::<$S>();
                    let max = max.as_primitive::<$S>();
                    Arc::new(PrimitiveArray::<$S>::from(
                        (0..col.len())
                            .map(|i| {
                                let v = col.is_valid(i).then(|| col.value(i));
                                // Index safety: checked above, all have same length.
                                let min = min.is_valid(i).then(|| min.value(i));
                                let max = max.is_valid(i).then(|| max.value(i));
                                Ok(match (v, min, max) {
                                    (Some(v), Some(min), Some(max)) => {
                                        if min > max {
                                            return Err(DataFusionError::Execution(format!(
                                                "min '{}' > max '{}'",
                                                min, max
                                            )));
                                        }
                                        Some(clamp!(v, min, max))
                                    },
                                    _ => None,
                                })
                            })
                            .collect::<datafusion_common::Result<Vec<_>>>()?,
                        )
                    ) as ArrayRef
                }
            )?;
            Ok(ColumnarValue::Array(result))
        }

        (ColumnarValue::Array(col), ColumnarValue::Scalar(min), ColumnarValue::Scalar(max)) => {
            if min.is_null() || max.is_null() {
                return Err(DataFusionError::Execution(
                    "argument 'min' or 'max' is null".to_string(),
                ));
            }
            let min = min.to_array()?;
            let max = max.to_array()?;
            let result = with_match_numerics_types!(
                col.data_type(),
                |$S| {
                    let col = col.as_primitive::<$S>();
                    // Index safety: checked above, both are not nulls.
                    let min = min.as_primitive::<$S>().value(0);
                    let max = max.as_primitive::<$S>().value(0);
                    if min > max {
                        return Err(DataFusionError::Execution(format!(
                            "min '{}' > max '{}'",
                            min, max
                        )));
                    }
                    Arc::new(PrimitiveArray::<$S>::from(
                        (0..col.len())
                            .map(|x| {
                                col.is_valid(x).then(|| {
                                    let v = col.value(x);
                                    clamp!(v, min, max)
                                })
                            })
                            .collect::<Vec<_>>(),
                        )
                    ) as ArrayRef
                }
            )?;
            Ok(ColumnarValue::Array(result))
        }
        _ => Err(DataFusionError::Internal(
            "argument column types mismatch".to_string(),
        )),
    }
}

#[derive(Clone, Debug, Default)]
pub struct ClampMinFunction;

const CLAMP_MIN_NAME: &str = "clamp_min";

impl Function for ClampMinFunction {
    fn name(&self) -> &str {
        CLAMP_MIN_NAME
    }

    fn return_type(&self, input_types: &[ArrowDataType]) -> Result<ArrowDataType> {
        Ok(input_types[0].clone())
    }

    fn signature(&self) -> Signature {
        // input, min
        Signature::uniform(2, NUMERICS.to_vec(), Volatility::Immutable)
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion_common::Result<ColumnarValue> {
        let [col, min] = utils::take_function_args(self.name(), args.args)?;

        let Some(max) = ScalarValue::max(&min.data_type()) else {
            return Err(DataFusionError::Internal(format!(
                "cannot find a max value for numeric data type {}",
                min.data_type()
            )));
        };
        clamp_impl(col, min, ColumnarValue::Scalar(max))
    }
}

impl Display for ClampMinFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", CLAMP_MIN_NAME.to_ascii_uppercase())
    }
}

#[derive(Clone, Debug, Default)]
pub struct ClampMaxFunction;

const CLAMP_MAX_NAME: &str = "clamp_max";

impl Function for ClampMaxFunction {
    fn name(&self) -> &str {
        CLAMP_MAX_NAME
    }

    fn return_type(&self, input_types: &[ArrowDataType]) -> Result<ArrowDataType> {
        Ok(input_types[0].clone())
    }

    fn signature(&self) -> Signature {
        // input, max
        Signature::uniform(2, NUMERICS.to_vec(), Volatility::Immutable)
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion_common::Result<ColumnarValue> {
        let [col, max] = utils::take_function_args(self.name(), args.args)?;

        let Some(min) = ScalarValue::min(&max.data_type()) else {
            return Err(DataFusionError::Internal(format!(
                "cannot find a min value for numeric data type {}",
                max.data_type()
            )));
        };
        clamp_impl(col, ColumnarValue::Scalar(min), max)
    }
}

impl Display for ClampMaxFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", CLAMP_MAX_NAME.to_ascii_uppercase())
    }
}

#[cfg(test)]
mod test {

    use std::sync::Arc;

    use arrow_schema::Field;
    use datafusion_common::config::ConfigOptions;
    use datatypes::arrow::array::{ArrayRef, Float64Array, Int64Array, UInt64Array};
    use datatypes::arrow_array::StringArray;

    use super::*;

    macro_rules! impl_test_eval {
        ($func: ty) => {
            impl $func {
                fn test_eval(
                    &self,
                    args: Vec<ColumnarValue>,
                    number_rows: usize,
                ) -> datafusion_common::Result<ArrayRef> {
                    let input_type = args[0].data_type();
                    self.invoke_with_args(ScalarFunctionArgs {
                        args,
                        arg_fields: vec![],
                        number_rows,
                        return_field: Arc::new(Field::new("x", input_type, false)),
                        config_options: Arc::new(ConfigOptions::new()),
                    })
                    .and_then(|v| ColumnarValue::values_to_arrays(&[v]).map_err(Into::into))
                    .map(|mut a| a.remove(0))
                }
            }
        };
    }

    impl_test_eval!(ClampFunction);
    impl_test_eval!(ClampMinFunction);
    impl_test_eval!(ClampMaxFunction);

    #[test]
    fn clamp_i64() {
        let inputs = [
            (
                vec![Some(-3), Some(-2), Some(-1), Some(0), Some(1), Some(2)],
                -1i64,
                10i64,
                vec![Some(-1), Some(-1), Some(-1), Some(0), Some(1), Some(2)],
            ),
            (
                vec![Some(-3), Some(-2), Some(-1), Some(0), Some(1), Some(2)],
                0i64,
                0i64,
                vec![Some(0), Some(0), Some(0), Some(0), Some(0), Some(0)],
            ),
            (
                vec![Some(-3), None, Some(-1), None, None, Some(2)],
                -2i64,
                1i64,
                vec![Some(-2), None, Some(-1), None, None, Some(1)],
            ),
            (
                vec![None, None, None, None, None],
                0i64,
                1i64,
                vec![None, None, None, None, None],
            ),
        ];

        let func = ClampFunction;
        for (in_data, min, max, expected) in inputs {
            let number_rows = in_data.len();
            let args = vec![
                ColumnarValue::Array(Arc::new(Int64Array::from(in_data))),
                ColumnarValue::Scalar(min.into()),
                ColumnarValue::Scalar(max.into()),
            ];
            let result = func.test_eval(args, number_rows).unwrap();
            let expected: ArrayRef = Arc::new(Int64Array::from(expected));
            assert_eq!(expected.as_ref(), result.as_ref());
        }
    }

    #[test]
    fn clamp_u64() {
        let inputs = [
            (
                vec![Some(0), Some(1), Some(2), Some(3), Some(4), Some(5)],
                1u64,
                3u64,
                vec![Some(1), Some(1), Some(2), Some(3), Some(3), Some(3)],
            ),
            (
                vec![Some(0), Some(1), Some(2), Some(3), Some(4), Some(5)],
                0u64,
                0u64,
                vec![Some(0), Some(0), Some(0), Some(0), Some(0), Some(0)],
            ),
            (
                vec![Some(0), None, Some(2), None, None, Some(5)],
                1u64,
                3u64,
                vec![Some(1), None, Some(2), None, None, Some(3)],
            ),
            (
                vec![None, None, None, None, None],
                0u64,
                1u64,
                vec![None, None, None, None, None],
            ),
        ];

        let func = ClampFunction;
        for (in_data, min, max, expected) in inputs {
            let number_rows = in_data.len();
            let args = vec![
                ColumnarValue::Array(Arc::new(UInt64Array::from(in_data))),
                ColumnarValue::Scalar(min.into()),
                ColumnarValue::Scalar(max.into()),
            ];
            let result = func.test_eval(args, number_rows).unwrap();
            let expected: ArrayRef = Arc::new(UInt64Array::from(expected));
            assert_eq!(expected.as_ref(), result.as_ref());
        }
    }

    #[test]
    fn clamp_f64() {
        let inputs = [
            (
                vec![Some(-3.0), Some(-2.0), Some(-1.0), Some(0.0), Some(1.0)],
                -1.0,
                10.0,
                vec![Some(-1.0), Some(-1.0), Some(-1.0), Some(0.0), Some(1.0)],
            ),
            (
                vec![Some(-2.0), Some(-1.0), Some(0.0), Some(1.0)],
                0.0,
                0.0,
                vec![Some(0.0), Some(0.0), Some(0.0), Some(0.0)],
            ),
            (
                vec![Some(-3.0), None, Some(-1.0), None, None, Some(2.0)],
                -2.0,
                1.0,
                vec![Some(-2.0), None, Some(-1.0), None, None, Some(1.0)],
            ),
            (
                vec![None, None, None, None, None],
                0.0,
                1.0,
                vec![None, None, None, None, None],
            ),
        ];

        let func = ClampFunction;
        for (in_data, min, max, expected) in inputs {
            let number_rows = in_data.len();
            let args = vec![
                ColumnarValue::Array(Arc::new(Float64Array::from(in_data))),
                ColumnarValue::Scalar(min.into()),
                ColumnarValue::Scalar(max.into()),
            ];
            let result = func.test_eval(args, number_rows).unwrap();
            let expected: ArrayRef = Arc::new(Float64Array::from(expected));
            assert_eq!(expected.as_ref(), result.as_ref());
        }
    }

    #[test]
    fn clamp_invalid_min_max() {
        let input = vec![Some(-3.0), Some(-2.0), Some(-1.0), Some(0.0), Some(1.0)];
        let min = 10.0;
        let max = -1.0;

        let func = ClampFunction;
        let number_rows = input.len();
        let args = vec![
            ColumnarValue::Array(Arc::new(Float64Array::from(input))),
            ColumnarValue::Scalar(min.into()),
            ColumnarValue::Scalar(max.into()),
        ];
        let result = func.test_eval(args, number_rows);
        assert!(result.is_err());
    }

    #[test]
    fn clamp_type_not_match() {
        let input = vec![Some(-3.0), Some(-2.0), Some(-1.0), Some(0.0), Some(1.0)];
        let min = -1i64;
        let max = 10u64;

        let func = ClampFunction;
        let number_rows = input.len();
        let args = vec![
            ColumnarValue::Array(Arc::new(Float64Array::from(input))),
            ColumnarValue::Scalar(min.into()),
            ColumnarValue::Scalar(max.into()),
        ];
        let result = func.test_eval(args, number_rows);
        assert!(result.is_err());
    }

    #[test]
    fn clamp_min_is_not_scalar() {
        let input = vec![Some(-3.0), Some(-2.0), Some(-1.0), Some(0.0), Some(1.0)];
        let min = -10.0;
        let max = 1.0;

        let func = ClampFunction;
        let number_rows = input.len();
        let args = vec![
            ColumnarValue::Array(Arc::new(Float64Array::from(input))),
            ColumnarValue::Array(Arc::new(Float64Array::from(vec![min, max]))),
            ColumnarValue::Array(Arc::new(Float64Array::from(vec![max, min]))),
        ];
        let result = func.test_eval(args, number_rows);
        assert!(result.is_err());
    }

    #[test]
    fn clamp_no_max() {
        let input = vec![Some(-3.0), Some(-2.0), Some(-1.0), Some(0.0), Some(1.0)];
        let min = -10.0;

        let func = ClampFunction;
        let number_rows = input.len();
        let args = vec![
            ColumnarValue::Array(Arc::new(Float64Array::from(input))),
            ColumnarValue::Scalar(min.into()),
        ];
        let result = func.test_eval(args, number_rows);
        assert!(result.is_err());
    }

    #[test]
    fn clamp_on_string() {
        let input = vec![Some("foo"), Some("foo"), Some("foo"), Some("foo")];

        let func = ClampFunction;
        let number_rows = input.len();
        let args = vec![
            ColumnarValue::Array(Arc::new(StringArray::from(input))),
            ColumnarValue::Scalar("bar".into()),
            ColumnarValue::Scalar("baz".into()),
        ];
        let result = func.test_eval(args, number_rows);
        assert!(result.is_err());
    }

    #[test]
    fn clamp_min_i64() {
        let inputs = [
            (
                vec![Some(-3), Some(-2), Some(-1), Some(0), Some(1), Some(2)],
                -1i64,
                vec![Some(-1), Some(-1), Some(-1), Some(0), Some(1), Some(2)],
            ),
            (
                vec![Some(-3), None, Some(-1), None, None, Some(2)],
                -2i64,
                vec![Some(-2), None, Some(-1), None, None, Some(2)],
            ),
        ];

        let func = ClampMinFunction;
        for (in_data, min, expected) in inputs {
            let number_rows = in_data.len();
            let args = vec![
                ColumnarValue::Array(Arc::new(Int64Array::from(in_data))),
                ColumnarValue::Scalar(min.into()),
            ];
            let result = func.test_eval(args, number_rows).unwrap();
            let expected: ArrayRef = Arc::new(Int64Array::from(expected));
            assert_eq!(expected.as_ref(), result.as_ref());
        }
    }

    #[test]
    fn clamp_max_i64() {
        let inputs = [
            (
                vec![Some(-3), Some(-2), Some(-1), Some(0), Some(1), Some(2)],
                1i64,
                vec![Some(-3), Some(-2), Some(-1), Some(0), Some(1), Some(1)],
            ),
            (
                vec![Some(-3), None, Some(-1), None, None, Some(2)],
                0i64,
                vec![Some(-3), None, Some(-1), None, None, Some(0)],
            ),
        ];

        let func = ClampMaxFunction;
        for (in_data, max, expected) in inputs {
            let number_rows = in_data.len();
            let args = vec![
                ColumnarValue::Array(Arc::new(Int64Array::from(in_data))),
                ColumnarValue::Scalar(max.into()),
            ];
            let result = func.test_eval(args, number_rows).unwrap();
            let expected: ArrayRef = Arc::new(Int64Array::from(expected));
            assert_eq!(expected.as_ref(), result.as_ref());
        }
    }

    #[test]
    fn clamp_min_f64() {
        let inputs = [(
            vec![Some(-3.0), Some(-2.0), Some(-1.0), Some(0.0), Some(1.0)],
            -1.0,
            vec![Some(-1.0), Some(-1.0), Some(-1.0), Some(0.0), Some(1.0)],
        )];

        let func = ClampMinFunction;
        for (in_data, min, expected) in inputs {
            let number_rows = in_data.len();
            let args = vec![
                ColumnarValue::Array(Arc::new(Float64Array::from(in_data))),
                ColumnarValue::Scalar(min.into()),
            ];
            let result = func.test_eval(args, number_rows).unwrap();
            let expected: ArrayRef = Arc::new(Float64Array::from(expected));
            assert_eq!(expected.as_ref(), result.as_ref());
        }
    }

    #[test]
    fn clamp_max_f64() {
        let inputs = [(
            vec![Some(-3.0), Some(-2.0), Some(-1.0), Some(0.0), Some(1.0)],
            0.0,
            vec![Some(-3.0), Some(-2.0), Some(-1.0), Some(0.0), Some(0.0)],
        )];

        let func = ClampMaxFunction;
        for (in_data, max, expected) in inputs {
            let number_rows = in_data.len();
            let args = vec![
                ColumnarValue::Array(Arc::new(Float64Array::from(in_data))),
                ColumnarValue::Scalar(max.into()),
            ];
            let result = func.test_eval(args, number_rows).unwrap();
            let expected: ArrayRef = Arc::new(Float64Array::from(expected));
            assert_eq!(expected.as_ref(), result.as_ref());
        }
    }

    #[test]
    fn clamp_min_type_not_match() {
        let input = vec![Some(-3.0), Some(-2.0), Some(-1.0), Some(0.0), Some(1.0)];
        let min = -1i64;

        let func = ClampMinFunction;
        let number_rows = input.len();
        let args = vec![
            ColumnarValue::Array(Arc::new(Float64Array::from(input))),
            ColumnarValue::Scalar(min.into()),
        ];
        let result = func.test_eval(args, number_rows);
        assert!(result.is_err());
    }

    #[test]
    fn clamp_max_type_not_match() {
        let input = vec![Some(-3.0), Some(-2.0), Some(-1.0), Some(0.0), Some(1.0)];
        let max = 1i64;

        let func = ClampMaxFunction;
        let number_rows = input.len();
        let args = vec![
            ColumnarValue::Array(Arc::new(Float64Array::from(input))),
            ColumnarValue::Scalar(max.into()),
        ];
        let result = func.test_eval(args, number_rows);
        assert!(result.is_err());
    }
}
