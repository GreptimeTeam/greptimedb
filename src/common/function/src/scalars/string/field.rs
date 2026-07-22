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

//! MySQL-compatible FIELD function implementation.
//!
//! FIELD(str, str1, str2, str3, ...) - Returns the 1-based index of str in the list.
//! Returns 0 if str is not found or is NULL.

use std::fmt;
use std::sync::Arc;

use datafusion_common::DataFusionError;
use datafusion_common::arrow::array::{Array, ArrayRef, AsArray, Float64Builder, Int64Builder};
use datafusion_common::arrow::compute::{CastOptions, cast, cast_with_options};
use datafusion_common::arrow::datatypes::DataType;
use datafusion_common::types::NativeType;
use datafusion_expr::type_coercion::binary::binary_numeric_coercion;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, Signature, Volatility};

use crate::function::Function;
use crate::function_registry::FunctionRegistry;

const NAME: &str = "field";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FieldType {
    String,
    Integer,
    Decimal,
    Float,
    Other,
}

#[derive(Debug)]
enum ComparisonMode {
    String,
    Integer,
    Decimal(DataType),
    Double,
    LegacyString,
}

fn field_type(data_type: &DataType) -> FieldType {
    match NativeType::from(data_type) {
        NativeType::Null | NativeType::String => FieldType::String,
        native_type if native_type.is_integer() => FieldType::Integer,
        NativeType::Decimal(_, scale) if scale >= 0 => FieldType::Decimal,
        NativeType::Decimal(_, _) => FieldType::Other,
        native_type if native_type.is_float() => FieldType::Float,
        _ => FieldType::Other,
    }
}

fn decimal_type(data_type: &DataType) -> DataType {
    match data_type {
        DataType::Dictionary(_, value_type) => decimal_type(value_type),
        DataType::Decimal32(precision, scale)
        | DataType::Decimal64(precision, scale)
        | DataType::Decimal128(precision, scale)
        | DataType::Decimal256(precision, scale) => DataType::Decimal256(*precision, *scale),
        _ => data_type.clone(),
    }
}

fn comparison_mode(arrays: &[ArrayRef]) -> datafusion_common::Result<ComparisonMode> {
    let types: Vec<_> = arrays
        .iter()
        .map(|array| field_type(array.data_type()))
        .collect();

    if types.contains(&FieldType::Other) {
        return Ok(ComparisonMode::LegacyString);
    }
    if types
        .iter()
        .all(|field_type| *field_type == FieldType::String)
    {
        return Ok(ComparisonMode::String);
    }
    if types
        .iter()
        .all(|field_type| *field_type == FieldType::Integer)
    {
        return Ok(ComparisonMode::Integer);
    }
    if types
        .iter()
        .all(|field_type| matches!(field_type, FieldType::Integer | FieldType::Decimal))
        && types.contains(&FieldType::Decimal)
    {
        let mut data_types = arrays.iter().map(|array| decimal_type(array.data_type()));
        let mut common_type = data_types.next().ok_or_else(|| {
            DataFusionError::Execution("FIELD: expected at least one argument".to_string())
        })?;
        for data_type in data_types {
            common_type = binary_numeric_coercion(&common_type, &data_type).ok_or_else(|| {
                DataFusionError::Execution(
                    "FIELD: unable to derive a common Decimal256 type".to_string(),
                )
            })?;
        }
        let common_type = match common_type {
            DataType::Decimal256(_, _) => common_type,
            _ => {
                return Err(DataFusionError::Execution(
                    "FIELD: unable to derive a common Decimal256 type".to_string(),
                ));
            }
        };

        return Ok(ComparisonMode::Decimal(common_type));
    }

    Ok(ComparisonMode::Double)
}

fn cast_to_string_arrays(arrays: &[ArrayRef]) -> datafusion_common::Result<Vec<ArrayRef>> {
    arrays
        .iter()
        .enumerate()
        .map(|(index, array)| {
            cast(array.as_ref(), &DataType::LargeUtf8).map_err(|error| {
                DataFusionError::Execution(format!(
                    "FIELD: argument {index} string cast failed: {error}"
                ))
            })
        })
        .collect()
}

fn parse_mysql_double(value: &str) -> f64 {
    let value = value.trim_start_matches(|character: char| character.is_ascii_whitespace());
    let bytes = value.as_bytes();
    let mut index = 0;

    if matches!(bytes.first(), Some(b'+' | b'-')) {
        index += 1;
    }

    let mantissa_start = index;
    while matches!(bytes.get(index), Some(byte) if byte.is_ascii_digit()) {
        index += 1;
    }
    let mut has_digit = index > mantissa_start;

    if bytes.get(index) == Some(&b'.') {
        index += 1;
        let fractional_start = index;
        while matches!(bytes.get(index), Some(byte) if byte.is_ascii_digit()) {
            index += 1;
        }
        has_digit |= index > fractional_start;
    }

    if !has_digit {
        return 0.0;
    }

    let number_end = index;
    if matches!(bytes.get(index), Some(b'e' | b'E')) {
        let exponent_start = index;
        index += 1;
        if matches!(bytes.get(index), Some(b'+' | b'-')) {
            index += 1;
        }
        let exponent_digits_start = index;
        while matches!(bytes.get(index), Some(byte) if byte.is_ascii_digit()) {
            index += 1;
        }
        if exponent_digits_start == index {
            index = exponent_start;
        }
    }

    let signed_max = if bytes.first() == Some(&b'-') {
        -f64::MAX
    } else {
        f64::MAX
    };
    match value[..index.max(number_end)].parse::<f64>() {
        Ok(parsed) if parsed.is_infinite() => signed_max,
        Ok(parsed) => parsed,
        Err(_) => signed_max,
    }
}

fn double_arrays(arrays: &[ArrayRef]) -> datafusion_common::Result<Vec<ArrayRef>> {
    arrays
        .iter()
        .enumerate()
        .map(|(index, array)| {
            if field_type(array.data_type()) == FieldType::String {
                let strings = cast(array.as_ref(), &DataType::LargeUtf8).map_err(|error| {
                    DataFusionError::Execution(format!(
                        "FIELD: argument {index} string cast failed: {error}"
                    ))
                })?;
                let strings = strings.as_string::<i64>();
                let mut builder = Float64Builder::with_capacity(strings.len());
                for row in 0..strings.len() {
                    if strings.is_null(row) {
                        builder.append_null();
                    } else {
                        builder.append_value(parse_mysql_double(strings.value(row)));
                    }
                }
                Ok(Arc::new(builder.finish()) as ArrayRef)
            } else {
                cast(array.as_ref(), &DataType::Float64).map_err(|error| {
                    DataFusionError::Execution(format!(
                        "FIELD: argument {index} double cast failed: {error}"
                    ))
                })
            }
        })
        .collect()
}

fn values_equal(left: f64, right: f64) -> bool {
    left == right || (left.is_nan() && right.is_nan())
}

/// MySQL-compatible FIELD function.
///
/// Syntax: FIELD(str, str1, str2, str3, ...)
/// Returns the 1-based index of str in the argument list (str1, str2, str3, ...).
/// Returns 0 if str is not found or is NULL.
#[derive(Debug)]
pub struct FieldFunction {
    signature: Signature,
}

impl FieldFunction {
    pub fn register(registry: &FunctionRegistry) {
        registry.register_scalar(FieldFunction::default());
    }
}

impl Default for FieldFunction {
    fn default() -> Self {
        Self {
            // FIELD takes a variable number of arguments: (String, String, String, ...)
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

impl fmt::Display for FieldFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", NAME.to_ascii_uppercase())
    }
}

impl Function for FieldFunction {
    fn name(&self) -> &str {
        NAME
    }

    fn return_type(&self, _: &[DataType]) -> datafusion_common::Result<DataType> {
        Ok(DataType::Int64)
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion_common::Result<ColumnarValue> {
        if args.args.len() < 2 {
            return Err(DataFusionError::Execution(
                "FIELD requires at least 2 arguments: FIELD(str, str1, ...)".to_string(),
            ));
        }

        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let len = arrays[0].len();

        let result = match comparison_mode(&arrays)? {
            ComparisonMode::String | ComparisonMode::Integer | ComparisonMode::LegacyString => {
                let arrays = cast_to_string_arrays(&arrays)?;
                let needle = arrays[0].as_string::<i64>();
                let mut builder = Int64Builder::with_capacity(len);
                for row in 0..len {
                    if needle.is_null(row) {
                        builder.append_value(0);
                        continue;
                    }
                    let index = arrays[1..]
                        .iter()
                        .position(|array| {
                            let candidate = array.as_string::<i64>();
                            !candidate.is_null(row) && candidate.value(row) == needle.value(row)
                        })
                        .map_or(0, |index| index as i64 + 1);
                    builder.append_value(index);
                }
                builder.finish()
            }
            ComparisonMode::Decimal(data_type) => {
                let options = CastOptions {
                    safe: false,
                    ..Default::default()
                };
                let arrays: Vec<_> = arrays
                    .iter()
                    .enumerate()
                    .map(|(index, array)| {
                        cast_with_options(array.as_ref(), &data_type, &options).map_err(|error| {
                            DataFusionError::Execution(format!(
                                "FIELD: argument {index} decimal cast failed: {error}"
                            ))
                        })
                    })
                    .collect::<datafusion_common::Result<_>>()?;
                let needle =
                    arrays[0].as_primitive::<datafusion_common::arrow::datatypes::Decimal256Type>();
                let mut builder = Int64Builder::with_capacity(len);
                for row in 0..len {
                    if needle.is_null(row) {
                        builder.append_value(0);
                        continue;
                    }
                    let index = arrays[1..]
                        .iter()
                        .position(|array| {
                            let candidate = array.as_primitive::<
                                datafusion_common::arrow::datatypes::Decimal256Type,
                            >();
                            !candidate.is_null(row) && candidate.value(row) == needle.value(row)
                        })
                        .map_or(0, |index| index as i64 + 1);
                    builder.append_value(index);
                }
                builder.finish()
            }
            ComparisonMode::Double => {
                let arrays = double_arrays(&arrays)?;
                let needle =
                    arrays[0].as_primitive::<datafusion_common::arrow::datatypes::Float64Type>();
                let mut builder = Int64Builder::with_capacity(len);
                for row in 0..len {
                    if needle.is_null(row) {
                        builder.append_value(0);
                        continue;
                    }
                    let index = arrays[1..]
                        .iter()
                        .position(|array| {
                            let candidate = array
                                .as_primitive::<datafusion_common::arrow::datatypes::Float64Type>();
                            !candidate.is_null(row)
                                && values_equal(needle.value(row), candidate.value(row))
                        })
                        .map_or(0, |index| index as i64 + 1);
                    builder.append_value(index);
                }
                builder.finish()
            }
        };

        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion_common::arrow::array::{
        BooleanArray, Decimal32Array, Decimal128Array, Decimal256Array, DictionaryArray,
        Float64Array, Int8Array, Int64Array, NullArray, StringArray, UInt64Array,
    };
    use datafusion_common::arrow::datatypes::{Field, Int8Type, i256};
    use datafusion_expr::ScalarFunctionArgs;

    use super::*;

    fn create_args(arrays: Vec<ArrayRef>) -> ScalarFunctionArgs {
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
            return_field: Arc::new(Field::new("result", DataType::Int64, true)),
            number_rows: arrays[0].len(),
            config_options: Arc::new(datafusion_common::config::ConfigOptions::default()),
        }
    }

    fn assert_field_result(args: Vec<ArrayRef>, expected: i64) {
        let result = FieldFunction::default()
            .invoke_with_args(create_args(args))
            .unwrap();
        let ColumnarValue::Array(array) = result else {
            panic!("Expected array result");
        };
        let result = array.as_primitive::<datafusion_common::arrow::datatypes::Int64Type>();

        assert_eq!(result.value(0), expected);
    }

    fn decimal32(value: i32, precision: u8, scale: i8) -> ArrayRef {
        Arc::new(
            Decimal32Array::from(vec![value])
                .with_precision_and_scale(precision, scale)
                .unwrap(),
        )
    }

    fn decimal128(value: i128, precision: u8, scale: i8) -> ArrayRef {
        Arc::new(
            Decimal128Array::from(vec![value])
                .with_precision_and_scale(precision, scale)
                .unwrap(),
        )
    }

    fn decimal256(value: i128, precision: u8, scale: i8) -> ArrayRef {
        Arc::new(
            Decimal256Array::from(vec![i256::from_i128(value)])
                .with_precision_and_scale(precision, scale)
                .unwrap(),
        )
    }

    fn string_dictionary(value: &str) -> ArrayRef {
        Arc::new(DictionaryArray::<Int8Type>::new(
            Int8Array::from(vec![0]),
            Arc::new(StringArray::from(vec![value])),
        ))
    }

    fn integer_dictionary(value: i64) -> ArrayRef {
        Arc::new(DictionaryArray::<Int8Type>::new(
            Int8Array::from(vec![0]),
            Arc::new(Int64Array::from(vec![value])),
        ))
    }

    #[test]
    fn test_field_basic() {
        let function = FieldFunction::default();

        let search = Arc::new(StringArray::from(vec!["b", "d", "a"]));
        let s1 = Arc::new(StringArray::from(vec!["a", "a", "a"]));
        let s2 = Arc::new(StringArray::from(vec!["b", "b", "b"]));
        let s3 = Arc::new(StringArray::from(vec!["c", "c", "c"]));

        let args = create_args(vec![search, s1, s2, s3]);
        let result = function.invoke_with_args(args).unwrap();

        if let ColumnarValue::Array(array) = result {
            let int_array = array.as_primitive::<datafusion_common::arrow::datatypes::Int64Type>();
            assert_eq!(int_array.value(0), 2); // "b" is at index 2
            assert_eq!(int_array.value(1), 0); // "d" not found
            assert_eq!(int_array.value(2), 1); // "a" is at index 1
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_field_with_null_search() {
        let function = FieldFunction::default();

        let search = Arc::new(StringArray::from(vec![Some("a"), None]));
        let s1 = Arc::new(StringArray::from(vec!["a", "a"]));
        let s2 = Arc::new(StringArray::from(vec!["b", "b"]));

        let args = create_args(vec![search, s1, s2]);
        let result = function.invoke_with_args(args).unwrap();

        if let ColumnarValue::Array(array) = result {
            let int_array = array.as_primitive::<datafusion_common::arrow::datatypes::Int64Type>();
            assert_eq!(int_array.value(0), 1); // "a" found at index 1
            assert_eq!(int_array.value(1), 0); // NULL search returns 0
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_field_case_sensitive() {
        let function = FieldFunction::default();

        let search = Arc::new(StringArray::from(vec!["A", "a"]));
        let s1 = Arc::new(StringArray::from(vec!["a", "a"]));
        let s2 = Arc::new(StringArray::from(vec!["A", "A"]));

        let args = create_args(vec![search, s1, s2]);
        let result = function.invoke_with_args(args).unwrap();

        if let ColumnarValue::Array(array) = result {
            let int_array = array.as_primitive::<datafusion_common::arrow::datatypes::Int64Type>();
            assert_eq!(int_array.value(0), 2); // "A" matches at index 2
            assert_eq!(int_array.value(1), 1); // "a" matches at index 1
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_field_mysql_comparison_mode_matrix() {
        assert_field_result(
            vec![
                Arc::new(Int64Array::from(vec![2])),
                Arc::new(StringArray::from(vec!["02"])),
                Arc::new(Int64Array::from(vec![3])),
            ],
            1,
        );
        assert_field_result(
            vec![
                Arc::new(StringArray::from(vec!["2"])),
                Arc::new(StringArray::from(vec!["02"])),
                Arc::new(StringArray::from(vec!["3"])),
            ],
            0,
        );
        assert_field_result(
            vec![
                Arc::new(Int64Array::from(vec![2])),
                Arc::new(Int64Array::from(vec![2])),
                Arc::new(Int64Array::from(vec![3])),
            ],
            1,
        );
    }

    #[test]
    fn test_field_mysql_null_needle_returns_zero_and_null_candidate_is_skipped() {
        assert_field_result(
            vec![
                Arc::new(Int64Array::from(vec![None])),
                Arc::new(Int64Array::from(vec![2])),
            ],
            0,
        );
        assert_field_result(
            vec![
                Arc::new(Int64Array::from(vec![2])),
                Arc::new(Int64Array::from(vec![None])),
                Arc::new(Int64Array::from(vec![2])),
            ],
            2,
        );
    }

    #[test]
    fn test_field_mysql_mixed_numeric_string_arguments_use_double_comparison() {
        assert_field_result(
            vec![
                Arc::new(StringArray::from(vec!["02"])),
                Arc::new(Int64Array::from(vec![2])),
                Arc::new(Int64Array::from(vec![3])),
            ],
            1,
        );
    }

    #[test]
    fn test_field_mysql_non_numeric_string_uses_double_zero() {
        assert_field_result(
            vec![
                Arc::new(Int64Array::from(vec![0])),
                Arc::new(StringArray::from(vec!["not-a-number"])),
                Arc::new(Int64Array::from(vec![1])),
            ],
            1,
        );
    }

    #[test]
    fn test_field_mysql_mixed_unsigned_and_double_use_approximate_comparison() {
        assert_field_result(
            vec![
                Arc::new(UInt64Array::from(vec![u64::MAX])),
                Arc::new(Float64Array::from(vec![18_446_744_073_709_551_616.0])),
            ],
            1,
        );
    }

    #[test]
    fn test_field_mysql_mixed_strings_use_numeric_prefixes() {
        assert_field_result(
            vec![
                Arc::new(Int64Array::from(vec![12])),
                Arc::new(StringArray::from(vec!["12x"])),
            ],
            1,
        );
        assert_field_result(
            vec![
                Arc::new(Int64Array::from(vec![0])),
                Arc::new(StringArray::from(vec!["x12"])),
            ],
            1,
        );
    }

    #[test]
    fn test_field_mysql_integer_mode_preserves_exact_extrema() {
        let above_f64_precision = 9_007_199_254_740_993_i64;
        assert_field_result(
            vec![
                Arc::new(Int64Array::from(vec![above_f64_precision])),
                Arc::new(UInt64Array::from(vec![9_007_199_254_740_992])),
                Arc::new(UInt64Array::from(vec![above_f64_precision as u64])),
            ],
            2,
        );
        assert_field_result(
            vec![
                Arc::new(Int64Array::from(vec![i64::MIN])),
                Arc::new(Int64Array::from(vec![i64::MIN])),
            ],
            1,
        );
        assert_field_result(
            vec![
                Arc::new(Int64Array::from(vec![i64::MAX])),
                Arc::new(Int64Array::from(vec![i64::MAX])),
            ],
            1,
        );
        assert_field_result(
            vec![
                Arc::new(UInt64Array::from(vec![u64::MAX])),
                Arc::new(UInt64Array::from(vec![u64::MAX])),
            ],
            1,
        );
        assert_field_result(
            vec![
                Arc::new(Int64Array::from(vec![2])),
                Arc::new(UInt64Array::from(vec![2])),
            ],
            1,
        );
    }

    #[test]
    fn test_field_mysql_decimal_mode_is_exact() {
        assert_field_result(vec![decimal32(120, 3, 2), decimal32(12, 2, 1)], 1);
        assert_field_result(
            vec![decimal32(20, 2, 1), Arc::new(Int64Array::from(vec![2]))],
            1,
        );
        assert_field_result(
            vec![
                decimal128(9_007_199_254_740_993, 16, 0),
                Arc::new(Int64Array::from(vec![9_007_199_254_740_992])),
            ],
            0,
        );
    }

    #[test]
    fn test_field_mysql_decimal_fold_order_and_widening() {
        assert_field_result(
            vec![decimal32(20, 2, 1), Arc::new(Int64Array::from(vec![2]))],
            1,
        );
        assert_field_result(
            vec![Arc::new(Int64Array::from(vec![2])), decimal32(20, 2, 1)],
            1,
        );
        assert_field_result(
            vec![decimal128(1, 38, 0), Arc::new(UInt64Array::from(vec![1]))],
            1,
        );
    }

    #[test]
    fn test_field_mysql_decimal_capacity_overflow_is_an_error() {
        let result = FieldFunction::default().invoke_with_args(create_args(vec![
            decimal256(1, 76, 76),
            Arc::new(Int64Array::from(vec![1])),
        ]));

        assert!(result.is_err());
    }

    #[test]
    fn test_field_mysql_negative_scale_decimal_uses_legacy_string_mode() {
        assert_field_result(
            vec![
                decimal32(123, 3, -2),
                Arc::new(StringArray::from(vec!["12300"])),
            ],
            1,
        );
    }

    #[test]
    fn test_field_mysql_double_mode_int_and_decimal_precision_collapse() {
        assert_field_result(
            vec![
                Arc::new(Int64Array::from(vec![9_007_199_254_740_993])),
                Arc::new(Float64Array::from(vec![9_007_199_254_740_992.0])),
            ],
            1,
        );
        assert_field_result(
            vec![
                decimal128(9_007_199_254_740_993, 16, 0),
                Arc::new(Float64Array::from(vec![9_007_199_254_740_992.0])),
            ],
            1,
        );
    }

    #[test]
    fn test_field_mysql_bare_null_is_string_but_typed_null_is_numeric() {
        assert_field_result(
            vec![
                Arc::new(StringArray::from(vec!["02"])),
                Arc::new(NullArray::new(1)),
                Arc::new(StringArray::from(vec!["2"])),
            ],
            0,
        );
        assert_field_result(
            vec![
                Arc::new(StringArray::from(vec!["02"])),
                Arc::new(Int64Array::from(vec![None::<i64>])),
                Arc::new(Int64Array::from(vec![2])),
            ],
            2,
        );
    }

    #[test]
    fn test_field_mysql_double_special_values() {
        assert_field_result(
            vec![
                Arc::new(Float64Array::from(vec![f64::NAN])),
                Arc::new(Float64Array::from(vec![f64::NAN])),
            ],
            1,
        );
        assert_field_result(
            vec![
                Arc::new(Float64Array::from(vec![f64::INFINITY])),
                Arc::new(Float64Array::from(vec![f64::INFINITY])),
            ],
            1,
        );
        assert_field_result(
            vec![
                Arc::new(Float64Array::from(vec![f64::NEG_INFINITY])),
                Arc::new(Float64Array::from(vec![f64::NEG_INFINITY])),
            ],
            1,
        );
        assert_field_result(
            vec![
                Arc::new(Float64Array::from(vec![f64::INFINITY])),
                Arc::new(Float64Array::from(vec![f64::NEG_INFINITY])),
            ],
            0,
        );
        assert_field_result(
            vec![
                Arc::new(Float64Array::from(vec![-0.0])),
                Arc::new(Float64Array::from(vec![0.0])),
            ],
            1,
        );
    }

    #[test]
    fn test_field_mysql_dictionary_arguments_keep_logical_modes() {
        assert_field_result(
            vec![Arc::new(Int64Array::from(vec![2])), string_dictionary("02")],
            1,
        );
        assert_field_result(
            vec![Arc::new(Int64Array::from(vec![2])), integer_dictionary(2)],
            1,
        );
    }

    #[test]
    fn test_field_mysql_boolean_uses_legacy_string_mode() {
        assert_field_result(
            vec![
                Arc::new(BooleanArray::from(vec![true])),
                Arc::new(Int64Array::from(vec![1])),
            ],
            0,
        );
    }

    #[test]
    fn test_field_mysql_permissive_double_parser() {
        assert_eq!(parse_mysql_double("  +12.5x"), 12.5);
        assert_eq!(parse_mysql_double(".5x"), 0.5);
        assert_eq!(parse_mysql_double("1e"), 1.0);
        assert_eq!(parse_mysql_double("1e+"), 1.0);
        assert_eq!(parse_mysql_double("x12"), 0.0);
        assert_eq!(parse_mysql_double("0x10"), 0.0);
        assert_eq!(parse_mysql_double("NaN"), 0.0);
        assert_eq!(parse_mysql_double("inf"), 0.0);
        assert_eq!(parse_mysql_double("1e9999"), f64::MAX);
        assert_eq!(parse_mysql_double("-1e9999"), -f64::MAX);
        let underflow = parse_mysql_double("-1e-9999");
        assert_eq!(underflow, 0.0);
        assert!(underflow.is_sign_negative());
    }

    #[test]
    fn test_field_mysql_overflow_string_clamps_to_signed_max() {
        assert_field_result(
            vec![
                Arc::new(Float64Array::from(vec![f64::MAX])),
                Arc::new(StringArray::from(vec!["1e9999"])),
            ],
            1,
        );
        assert_field_result(
            vec![
                Arc::new(Float64Array::from(vec![-f64::MAX])),
                Arc::new(StringArray::from(vec!["-1e9999"])),
            ],
            1,
        );
    }
}
