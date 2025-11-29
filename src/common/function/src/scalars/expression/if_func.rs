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

use std::fmt;
use std::fmt::Display;

use datafusion::arrow::array::{Array, ArrayRef, AsArray, BooleanArray};
use datafusion::arrow::compute::kernels::zip::zip;
use datafusion::arrow::datatypes::DataType;
use datafusion_common::{DataFusionError, ScalarValue};
use datafusion_expr::type_coercion::binary::comparison_coercion;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, Signature, Volatility};

use crate::function::Function;

const NAME: &str = "if";

/// MySQL-compatible IF function: IF(condition, true_value, false_value)
///
/// Returns true_value if condition is TRUE (not NULL and not 0),
/// otherwise returns false_value.
///
/// MySQL truthy rules:
/// - NULL -> false
/// - 0 (numeric zero) -> false
/// - Any non-zero numeric -> true
/// - Boolean true/false -> use directly
#[derive(Clone, Debug)]
pub struct IfFunction {
    signature: Signature,
}

impl Default for IfFunction {
    fn default() -> Self {
        Self {
            signature: Signature::any(3, Volatility::Immutable),
        }
    }
}

impl Display for IfFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", NAME.to_ascii_uppercase())
    }
}

impl Function for IfFunction {
    fn name(&self) -> &str {
        NAME
    }

    fn return_type(&self, input_types: &[DataType]) -> datafusion_common::Result<DataType> {
        // Return the common type of true_value and false_value (args[1] and args[2])
        if input_types.len() < 3 {
            return Err(DataFusionError::Plan(format!(
                "{} requires 3 arguments, got {}",
                NAME,
                input_types.len()
            )));
        }
        let true_type = &input_types[1];
        let false_type = &input_types[2];

        // Use comparison_coercion to find common type
        comparison_coercion(true_type, false_type).ok_or_else(|| {
            DataFusionError::Plan(format!(
                "Cannot find common type for IF function between {:?} and {:?}",
                true_type, false_type
            ))
        })
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion_common::Result<ColumnarValue> {
        if args.args.len() != 3 {
            return Err(DataFusionError::Plan(format!(
                "{} requires exactly 3 arguments, got {}",
                NAME,
                args.args.len()
            )));
        }

        let condition = &args.args[0];
        let true_value = &args.args[1];
        let false_value = &args.args[2];

        // Convert condition to boolean array using MySQL truthy rules
        let bool_array = to_boolean_array(condition, args.number_rows)?;

        // Convert true and false values to arrays
        let true_array = true_value.to_array(args.number_rows)?;
        let false_array = false_value.to_array(args.number_rows)?;

        // Use zip to select values based on condition
        // zip expects &dyn Datum, and ArrayRef (Arc<dyn Array>) implements Datum
        let result = zip(&bool_array, &true_array, &false_array)?;
        Ok(ColumnarValue::Array(result))
    }
}

/// Convert a ColumnarValue to a BooleanArray using MySQL truthy rules:
/// - NULL -> false
/// - 0 (any numeric zero) -> false
/// - Non-zero numeric -> true
/// - Boolean -> use directly
fn to_boolean_array(
    value: &ColumnarValue,
    num_rows: usize,
) -> datafusion_common::Result<BooleanArray> {
    match value {
        ColumnarValue::Scalar(scalar) => {
            let bool_val = scalar_to_bool(scalar)?;
            Ok(BooleanArray::from(vec![bool_val; num_rows]))
        }
        ColumnarValue::Array(array) => array_to_bool(array.clone()),
    }
}

/// Convert a ScalarValue to boolean using MySQL truthy rules
fn scalar_to_bool(scalar: &ScalarValue) -> datafusion_common::Result<bool> {
    if scalar.is_null() {
        return Ok(false);
    }

    match scalar {
        ScalarValue::Boolean(Some(b)) => Ok(*b),
        ScalarValue::Int8(Some(v)) => Ok(*v != 0),
        ScalarValue::Int16(Some(v)) => Ok(*v != 0),
        ScalarValue::Int32(Some(v)) => Ok(*v != 0),
        ScalarValue::Int64(Some(v)) => Ok(*v != 0),
        ScalarValue::UInt8(Some(v)) => Ok(*v != 0),
        ScalarValue::UInt16(Some(v)) => Ok(*v != 0),
        ScalarValue::UInt32(Some(v)) => Ok(*v != 0),
        ScalarValue::UInt64(Some(v)) => Ok(*v != 0),
        ScalarValue::Float16(Some(v)) => Ok(v.to_f32() != 0.0),
        ScalarValue::Float32(Some(v)) => Ok(*v != 0.0),
        ScalarValue::Float64(Some(v)) => Ok(*v != 0.0),
        // For other types, if not null, treat as true
        _ => Ok(true),
    }
}

/// Convert an Array to BooleanArray using MySQL truthy rules
fn array_to_bool(array: ArrayRef) -> datafusion_common::Result<BooleanArray> {
    let len = array.len();
    let mut result = Vec::with_capacity(len);

    match array.data_type() {
        DataType::Boolean => {
            let bool_array = array.as_boolean();
            for i in 0..len {
                if array.is_null(i) {
                    result.push(false);
                } else {
                    result.push(bool_array.value(i));
                }
            }
        }
        DataType::Int8 => {
            let typed_array = array.as_primitive::<arrow::datatypes::Int8Type>();
            for i in 0..len {
                result.push(!array.is_null(i) && typed_array.value(i) != 0);
            }
        }
        DataType::Int16 => {
            let typed_array = array.as_primitive::<arrow::datatypes::Int16Type>();
            for i in 0..len {
                result.push(!array.is_null(i) && typed_array.value(i) != 0);
            }
        }
        DataType::Int32 => {
            let typed_array = array.as_primitive::<arrow::datatypes::Int32Type>();
            for i in 0..len {
                result.push(!array.is_null(i) && typed_array.value(i) != 0);
            }
        }
        DataType::Int64 => {
            let typed_array = array.as_primitive::<arrow::datatypes::Int64Type>();
            for i in 0..len {
                result.push(!array.is_null(i) && typed_array.value(i) != 0);
            }
        }
        DataType::UInt8 => {
            let typed_array = array.as_primitive::<arrow::datatypes::UInt8Type>();
            for i in 0..len {
                result.push(!array.is_null(i) && typed_array.value(i) != 0);
            }
        }
        DataType::UInt16 => {
            let typed_array = array.as_primitive::<arrow::datatypes::UInt16Type>();
            for i in 0..len {
                result.push(!array.is_null(i) && typed_array.value(i) != 0);
            }
        }
        DataType::UInt32 => {
            let typed_array = array.as_primitive::<arrow::datatypes::UInt32Type>();
            for i in 0..len {
                result.push(!array.is_null(i) && typed_array.value(i) != 0);
            }
        }
        DataType::UInt64 => {
            let typed_array = array.as_primitive::<arrow::datatypes::UInt64Type>();
            for i in 0..len {
                result.push(!array.is_null(i) && typed_array.value(i) != 0);
            }
        }
        DataType::Float32 => {
            let typed_array = array.as_primitive::<arrow::datatypes::Float32Type>();
            for i in 0..len {
                result.push(!array.is_null(i) && typed_array.value(i) != 0.0);
            }
        }
        DataType::Float64 => {
            let typed_array = array.as_primitive::<arrow::datatypes::Float64Type>();
            for i in 0..len {
                result.push(!array.is_null(i) && typed_array.value(i) != 0.0);
            }
        }
        // For other types, treat non-null as true
        _ => {
            for i in 0..len {
                result.push(!array.is_null(i));
            }
        }
    }

    Ok(BooleanArray::from(result))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::Field;
    use datafusion_common::arrow::array::{AsArray, Int32Array, StringArray};

    use super::*;

    #[test]
    fn test_if_function_basic() {
        let if_func = IfFunction::default();
        assert_eq!("if", if_func.name());

        // Test IF(true, 'yes', 'no') -> 'yes'
        let result = if_func
            .invoke_with_args(ScalarFunctionArgs {
                args: vec![
                    ColumnarValue::Scalar(ScalarValue::Boolean(Some(true))),
                    ColumnarValue::Scalar(ScalarValue::Utf8(Some("yes".to_string()))),
                    ColumnarValue::Scalar(ScalarValue::Utf8(Some("no".to_string()))),
                ],
                arg_fields: vec![],
                number_rows: 1,
                return_field: Arc::new(Field::new("", DataType::Utf8, true)),
                config_options: Arc::new(Default::default()),
            })
            .unwrap();

        if let ColumnarValue::Array(arr) = result {
            let str_arr = arr.as_string::<i32>();
            assert_eq!(str_arr.value(0), "yes");
        } else {
            panic!("Expected Array result");
        }
    }

    #[test]
    fn test_if_function_false() {
        let if_func = IfFunction::default();

        // Test IF(false, 'yes', 'no') -> 'no'
        let result = if_func
            .invoke_with_args(ScalarFunctionArgs {
                args: vec![
                    ColumnarValue::Scalar(ScalarValue::Boolean(Some(false))),
                    ColumnarValue::Scalar(ScalarValue::Utf8(Some("yes".to_string()))),
                    ColumnarValue::Scalar(ScalarValue::Utf8(Some("no".to_string()))),
                ],
                arg_fields: vec![],
                number_rows: 1,
                return_field: Arc::new(Field::new("", DataType::Utf8, true)),
                config_options: Arc::new(Default::default()),
            })
            .unwrap();

        if let ColumnarValue::Array(arr) = result {
            let str_arr = arr.as_string::<i32>();
            assert_eq!(str_arr.value(0), "no");
        } else {
            panic!("Expected Array result");
        }
    }

    #[test]
    fn test_if_function_null_is_false() {
        let if_func = IfFunction::default();

        // Test IF(NULL, 'yes', 'no') -> 'no' (NULL is treated as false)
        let result = if_func
            .invoke_with_args(ScalarFunctionArgs {
                args: vec![
                    ColumnarValue::Scalar(ScalarValue::Boolean(None)),
                    ColumnarValue::Scalar(ScalarValue::Utf8(Some("yes".to_string()))),
                    ColumnarValue::Scalar(ScalarValue::Utf8(Some("no".to_string()))),
                ],
                arg_fields: vec![],
                number_rows: 1,
                return_field: Arc::new(Field::new("", DataType::Utf8, true)),
                config_options: Arc::new(Default::default()),
            })
            .unwrap();

        if let ColumnarValue::Array(arr) = result {
            let str_arr = arr.as_string::<i32>();
            assert_eq!(str_arr.value(0), "no");
        } else {
            panic!("Expected Array result");
        }
    }

    #[test]
    fn test_if_function_numeric_truthy() {
        let if_func = IfFunction::default();

        // Test IF(1, 'yes', 'no') -> 'yes' (non-zero is true)
        let result = if_func
            .invoke_with_args(ScalarFunctionArgs {
                args: vec![
                    ColumnarValue::Scalar(ScalarValue::Int32(Some(1))),
                    ColumnarValue::Scalar(ScalarValue::Utf8(Some("yes".to_string()))),
                    ColumnarValue::Scalar(ScalarValue::Utf8(Some("no".to_string()))),
                ],
                arg_fields: vec![],
                number_rows: 1,
                return_field: Arc::new(Field::new("", DataType::Utf8, true)),
                config_options: Arc::new(Default::default()),
            })
            .unwrap();

        if let ColumnarValue::Array(arr) = result {
            let str_arr = arr.as_string::<i32>();
            assert_eq!(str_arr.value(0), "yes");
        } else {
            panic!("Expected Array result");
        }

        // Test IF(0, 'yes', 'no') -> 'no' (zero is false)
        let result = if_func
            .invoke_with_args(ScalarFunctionArgs {
                args: vec![
                    ColumnarValue::Scalar(ScalarValue::Int32(Some(0))),
                    ColumnarValue::Scalar(ScalarValue::Utf8(Some("yes".to_string()))),
                    ColumnarValue::Scalar(ScalarValue::Utf8(Some("no".to_string()))),
                ],
                arg_fields: vec![],
                number_rows: 1,
                return_field: Arc::new(Field::new("", DataType::Utf8, true)),
                config_options: Arc::new(Default::default()),
            })
            .unwrap();

        if let ColumnarValue::Array(arr) = result {
            let str_arr = arr.as_string::<i32>();
            assert_eq!(str_arr.value(0), "no");
        } else {
            panic!("Expected Array result");
        }
    }

    #[test]
    fn test_if_function_with_arrays() {
        let if_func = IfFunction::default();

        // Test with array condition
        let condition = Int32Array::from(vec![Some(1), Some(0), None, Some(5)]);
        let true_val = StringArray::from(vec!["yes", "yes", "yes", "yes"]);
        let false_val = StringArray::from(vec!["no", "no", "no", "no"]);

        let result = if_func
            .invoke_with_args(ScalarFunctionArgs {
                args: vec![
                    ColumnarValue::Array(Arc::new(condition)),
                    ColumnarValue::Array(Arc::new(true_val)),
                    ColumnarValue::Array(Arc::new(false_val)),
                ],
                arg_fields: vec![],
                number_rows: 4,
                return_field: Arc::new(Field::new("", DataType::Utf8, true)),
                config_options: Arc::new(Default::default()),
            })
            .unwrap();

        if let ColumnarValue::Array(arr) = result {
            let str_arr = arr.as_string::<i32>();
            assert_eq!(str_arr.value(0), "yes"); // 1 is true
            assert_eq!(str_arr.value(1), "no"); // 0 is false
            assert_eq!(str_arr.value(2), "no"); // NULL is false
            assert_eq!(str_arr.value(3), "yes"); // 5 is true
        } else {
            panic!("Expected Array result");
        }
    }
}
