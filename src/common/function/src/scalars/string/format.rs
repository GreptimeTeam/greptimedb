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

//! MySQL-compatible FORMAT function implementation.
//!
//! FORMAT(X, D) - Formats the number X with D decimal places using thousand separators.

use std::fmt;
use std::sync::Arc;

use datafusion_common::DataFusionError;
use datafusion_common::arrow::array::{Array, AsArray, LargeStringBuilder};
use datafusion_common::arrow::datatypes::DataType;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, Signature, TypeSignature, Volatility};

use crate::function::Function;
use crate::function_registry::FunctionRegistry;

const NAME: &str = "format";

/// MySQL-compatible FORMAT function.
///
/// Syntax: FORMAT(X, D)
/// Formats the number X to a format like '#,###,###.##', rounded to D decimal places.
/// D can be 0 to 30.
///
/// Note: This implementation uses the en_US locale (comma as thousand separator,
/// period as decimal separator).
#[derive(Debug)]
pub struct FormatFunction {
    signature: Signature,
}

impl FormatFunction {
    pub fn register(registry: &FunctionRegistry) {
        registry.register_scalar(FormatFunction::default());
    }
}

impl Default for FormatFunction {
    fn default() -> Self {
        let mut signatures = Vec::new();

        // Support various numeric types for X
        let numeric_types = [
            DataType::Float64,
            DataType::Float32,
            DataType::Int64,
            DataType::Int32,
            DataType::Int16,
            DataType::Int8,
            DataType::UInt64,
            DataType::UInt32,
            DataType::UInt16,
            DataType::UInt8,
        ];

        // D can be various integer types
        let int_types = [
            DataType::Int64,
            DataType::Int32,
            DataType::Int16,
            DataType::Int8,
            DataType::UInt64,
            DataType::UInt32,
            DataType::UInt16,
            DataType::UInt8,
        ];

        for x_type in &numeric_types {
            for d_type in &int_types {
                signatures.push(TypeSignature::Exact(vec![x_type.clone(), d_type.clone()]));
            }
        }

        Self {
            signature: Signature::one_of(signatures, Volatility::Immutable),
        }
    }
}

impl fmt::Display for FormatFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", NAME.to_ascii_uppercase())
    }
}

impl Function for FormatFunction {
    fn name(&self) -> &str {
        NAME
    }

    fn return_type(&self, _: &[DataType]) -> datafusion_common::Result<DataType> {
        Ok(DataType::LargeUtf8)
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion_common::Result<ColumnarValue> {
        if args.args.len() != 2 {
            return Err(DataFusionError::Execution(
                "FORMAT requires exactly 2 arguments: FORMAT(X, D)".to_string(),
            ));
        }

        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let len = arrays[0].len();

        let x_array = &arrays[0];
        let d_array = &arrays[1];

        let mut builder = LargeStringBuilder::with_capacity(len, len * 20);

        for i in 0..len {
            if x_array.is_null(i) || d_array.is_null(i) {
                builder.append_null();
                continue;
            }

            let decimal_places = get_decimal_places(d_array, i)?.clamp(0, 30) as usize;

            let formatted = match x_array.data_type() {
                DataType::Float64 | DataType::Float32 => {
                    format_number_float(get_float_value(x_array, i)?, decimal_places)
                }
                DataType::Int64
                | DataType::Int32
                | DataType::Int16
                | DataType::Int8
                | DataType::UInt64
                | DataType::UInt32
                | DataType::UInt16
                | DataType::UInt8 => format_number_integer(x_array, i, decimal_places)?,
                _ => {
                    return Err(DataFusionError::Execution(format!(
                        "FORMAT: unsupported type {:?}",
                        x_array.data_type()
                    )));
                }
            };
            builder.append_value(&formatted);
        }

        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

/// Get float value from various numeric types.
fn get_float_value(
    array: &datafusion_common::arrow::array::ArrayRef,
    index: usize,
) -> datafusion_common::Result<f64> {
    use datafusion_common::arrow::datatypes as arrow_types;

    match array.data_type() {
        DataType::Float64 => Ok(array
            .as_primitive::<arrow_types::Float64Type>()
            .value(index)),
        DataType::Float32 => Ok(array
            .as_primitive::<arrow_types::Float32Type>()
            .value(index) as f64),
        _ => Err(DataFusionError::Execution(format!(
            "FORMAT: unsupported type {:?}",
            array.data_type()
        ))),
    }
}

/// Get decimal places from various integer types.
///
/// MySQL clamps decimal places to `0..=30`. This function returns an `i64` so the caller can clamp.
fn get_decimal_places(
    array: &datafusion_common::arrow::array::ArrayRef,
    index: usize,
) -> datafusion_common::Result<i64> {
    use datafusion_common::arrow::datatypes as arrow_types;

    match array.data_type() {
        DataType::Int64 => Ok(array.as_primitive::<arrow_types::Int64Type>().value(index)),
        DataType::Int32 => Ok(array.as_primitive::<arrow_types::Int32Type>().value(index) as i64),
        DataType::Int16 => Ok(array.as_primitive::<arrow_types::Int16Type>().value(index) as i64),
        DataType::Int8 => Ok(array.as_primitive::<arrow_types::Int8Type>().value(index) as i64),
        DataType::UInt64 => {
            let v = array.as_primitive::<arrow_types::UInt64Type>().value(index);
            Ok(if v > i64::MAX as u64 {
                i64::MAX
            } else {
                v as i64
            })
        }
        DataType::UInt32 => Ok(array.as_primitive::<arrow_types::UInt32Type>().value(index) as i64),
        DataType::UInt16 => Ok(array.as_primitive::<arrow_types::UInt16Type>().value(index) as i64),
        DataType::UInt8 => Ok(array.as_primitive::<arrow_types::UInt8Type>().value(index) as i64),
        _ => Err(DataFusionError::Execution(format!(
            "FORMAT: unsupported type {:?}",
            array.data_type()
        ))),
    }
}

fn format_number_integer(
    array: &datafusion_common::arrow::array::ArrayRef,
    index: usize,
    decimal_places: usize,
) -> datafusion_common::Result<String> {
    use datafusion_common::arrow::datatypes as arrow_types;

    let (is_negative, abs_digits) = match array.data_type() {
        DataType::Int64 => {
            let v = array.as_primitive::<arrow_types::Int64Type>().value(index) as i128;
            (v.is_negative(), v.unsigned_abs().to_string())
        }
        DataType::Int32 => {
            let v = array.as_primitive::<arrow_types::Int32Type>().value(index) as i128;
            (v.is_negative(), v.unsigned_abs().to_string())
        }
        DataType::Int16 => {
            let v = array.as_primitive::<arrow_types::Int16Type>().value(index) as i128;
            (v.is_negative(), v.unsigned_abs().to_string())
        }
        DataType::Int8 => {
            let v = array.as_primitive::<arrow_types::Int8Type>().value(index) as i128;
            (v.is_negative(), v.unsigned_abs().to_string())
        }
        DataType::UInt64 => {
            let v = array.as_primitive::<arrow_types::UInt64Type>().value(index) as u128;
            (false, v.to_string())
        }
        DataType::UInt32 => {
            let v = array.as_primitive::<arrow_types::UInt32Type>().value(index) as u128;
            (false, v.to_string())
        }
        DataType::UInt16 => {
            let v = array.as_primitive::<arrow_types::UInt16Type>().value(index) as u128;
            (false, v.to_string())
        }
        DataType::UInt8 => {
            let v = array.as_primitive::<arrow_types::UInt8Type>().value(index) as u128;
            (false, v.to_string())
        }
        _ => {
            return Err(DataFusionError::Execution(format!(
                "FORMAT: unsupported type {:?}",
                array.data_type()
            )));
        }
    };

    let mut result = String::new();
    if is_negative {
        result.push('-');
    }
    result.push_str(&add_thousand_separators(&abs_digits));

    if decimal_places > 0 {
        result.push('.');
        result.push_str(&"0".repeat(decimal_places));
    }

    Ok(result)
}

/// Format a float with thousand separators and `decimal_places` digits after decimal point.
fn format_number_float(x: f64, decimal_places: usize) -> String {
    // Handle special cases
    if x.is_nan() {
        return "NaN".to_string();
    }
    if x.is_infinite() {
        return if x.is_sign_positive() {
            "Infinity".to_string()
        } else {
            "-Infinity".to_string()
        };
    }

    // Round to decimal_places
    let multiplier = 10f64.powi(decimal_places as i32);
    let rounded = (x * multiplier).round() / multiplier;

    // Split into integer and fractional parts
    let is_negative = rounded < 0.0;
    let abs_value = rounded.abs();

    // Format with the specified decimal places
    let formatted = if decimal_places == 0 {
        format!("{:.0}", abs_value)
    } else {
        format!("{:.prec$}", abs_value, prec = decimal_places)
    };

    // Split at decimal point
    let parts: Vec<&str> = formatted.split('.').collect();
    let int_part = parts[0];
    let dec_part = parts.get(1).copied();

    // Add thousand separators to integer part
    let int_with_sep = add_thousand_separators(int_part);

    // Build result
    let mut result = String::new();
    if is_negative {
        result.push('-');
    }
    result.push_str(&int_with_sep);
    if let Some(dec) = dec_part {
        result.push('.');
        result.push_str(dec);
    }

    result
}

/// Add thousand separators (commas) to an integer string.
fn add_thousand_separators(s: &str) -> String {
    let chars: Vec<char> = s.chars().collect();
    let len = chars.len();

    if len <= 3 {
        return s.to_string();
    }

    let mut result = String::with_capacity(len + len / 3);
    let first_group_len = len % 3;
    let first_group_len = if first_group_len == 0 {
        3
    } else {
        first_group_len
    };

    for (i, ch) in chars.iter().enumerate() {
        if i > 0 && i >= first_group_len && (i - first_group_len) % 3 == 0 {
            result.push(',');
        }
        result.push(*ch);
    }

    result
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion_common::arrow::array::{Float64Array, Int64Array};
    use datafusion_common::arrow::datatypes::Field;
    use datafusion_expr::ScalarFunctionArgs;

    use super::*;

    fn create_args(arrays: Vec<datafusion_common::arrow::array::ArrayRef>) -> ScalarFunctionArgs {
        let arg_fields: Vec<_> = arrays
            .iter()
            .enumerate()
            .map(|(i, arr)| {
                Arc::new(Field::new(
                    format!("arg_{}", i),
                    arr.data_type().clone(),
                    true,
                ))
            })
            .collect();

        ScalarFunctionArgs {
            args: arrays.iter().cloned().map(ColumnarValue::Array).collect(),
            arg_fields,
            return_field: Arc::new(Field::new("result", DataType::LargeUtf8, true)),
            number_rows: arrays[0].len(),
            config_options: Arc::new(datafusion_common::config::ConfigOptions::default()),
        }
    }

    #[test]
    fn test_format_basic() {
        let function = FormatFunction::default();

        let x = Arc::new(Float64Array::from(vec![1234567.891, 1234.5, 1234567.0]));
        let d = Arc::new(Int64Array::from(vec![2, 0, 3]));

        let args = create_args(vec![x, d]);
        let result = function.invoke_with_args(args).unwrap();

        if let ColumnarValue::Array(array) = result {
            let str_array = array.as_string::<i64>();
            assert_eq!(str_array.value(0), "1,234,567.89");
            assert_eq!(str_array.value(1), "1,235"); // rounded
            assert_eq!(str_array.value(2), "1,234,567.000");
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_format_negative() {
        let function = FormatFunction::default();

        let x = Arc::new(Float64Array::from(vec![-1234567.891]));
        let d = Arc::new(Int64Array::from(vec![2]));

        let args = create_args(vec![x, d]);
        let result = function.invoke_with_args(args).unwrap();

        if let ColumnarValue::Array(array) = result {
            let str_array = array.as_string::<i64>();
            assert_eq!(str_array.value(0), "-1,234,567.89");
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_format_small_numbers() {
        let function = FormatFunction::default();

        let x = Arc::new(Float64Array::from(vec![0.5, 12.345, 123.0]));
        let d = Arc::new(Int64Array::from(vec![2, 2, 0]));

        let args = create_args(vec![x, d]);
        let result = function.invoke_with_args(args).unwrap();

        if let ColumnarValue::Array(array) = result {
            let str_array = array.as_string::<i64>();
            assert_eq!(str_array.value(0), "0.50");
            assert_eq!(str_array.value(1), "12.35"); // rounded
            assert_eq!(str_array.value(2), "123");
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_format_with_nulls() {
        let function = FormatFunction::default();

        let x = Arc::new(Float64Array::from(vec![Some(1234.5), None]));
        let d = Arc::new(Int64Array::from(vec![2, 2]));

        let args = create_args(vec![x, d]);
        let result = function.invoke_with_args(args).unwrap();

        if let ColumnarValue::Array(array) = result {
            let str_array = array.as_string::<i64>();
            assert_eq!(str_array.value(0), "1,234.50");
            assert!(str_array.is_null(1));
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_add_thousand_separators() {
        assert_eq!(add_thousand_separators("1"), "1");
        assert_eq!(add_thousand_separators("12"), "12");
        assert_eq!(add_thousand_separators("123"), "123");
        assert_eq!(add_thousand_separators("1234"), "1,234");
        assert_eq!(add_thousand_separators("12345"), "12,345");
        assert_eq!(add_thousand_separators("123456"), "123,456");
        assert_eq!(add_thousand_separators("1234567"), "1,234,567");
        assert_eq!(add_thousand_separators("12345678"), "12,345,678");
        assert_eq!(add_thousand_separators("123456789"), "123,456,789");
    }

    #[test]
    fn test_format_large_int_no_float_precision_loss() {
        let function = FormatFunction::default();

        // 2^53 + 1 cannot be represented exactly as f64.
        let x = Arc::new(Int64Array::from(vec![9_007_199_254_740_993i64]));
        let d = Arc::new(Int64Array::from(vec![0]));

        let args = create_args(vec![x, d]);
        let result = function.invoke_with_args(args).unwrap();

        if let ColumnarValue::Array(array) = result {
            let str_array = array.as_string::<i64>();
            assert_eq!(str_array.value(0), "9,007,199,254,740,993");
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_format_decimal_places_u64_overflow_clamps() {
        use datafusion_common::arrow::array::UInt64Array;

        let function = FormatFunction::default();

        let x = Arc::new(Int64Array::from(vec![1]));
        let d = Arc::new(UInt64Array::from(vec![u64::MAX]));

        let args = create_args(vec![x, d]);
        let result = function.invoke_with_args(args).unwrap();

        if let ColumnarValue::Array(array) = result {
            let str_array = array.as_string::<i64>();
            assert_eq!(str_array.value(0), format!("1.{}", "0".repeat(30)));
        } else {
            panic!("Expected array result");
        }
    }
}
