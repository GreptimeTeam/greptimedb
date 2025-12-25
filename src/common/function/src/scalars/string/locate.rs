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

//! MySQL-compatible LOCATE function implementation.
//!
//! LOCATE(substr, str) - Returns the position of the first occurrence of substr in str (1-based).
//! LOCATE(substr, str, pos) - Returns the position of the first occurrence of substr in str,
//!                            starting from position pos.
//! Returns 0 if substr is not found.

use std::fmt;
use std::sync::Arc;

use datafusion_common::DataFusionError;
use datafusion_common::arrow::array::{Array, ArrayRef, AsArray, Int64Builder};
use datafusion_common::arrow::compute::cast;
use datafusion_common::arrow::datatypes::DataType;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, Signature, TypeSignature, Volatility};

use crate::function::Function;
use crate::function_registry::FunctionRegistry;

const NAME: &str = "locate";

/// MySQL-compatible LOCATE function.
///
/// Syntax:
/// - LOCATE(substr, str) - Returns 1-based position of substr in str, or 0 if not found.
/// - LOCATE(substr, str, pos) - Same, but starts searching from position pos.
#[derive(Debug)]
pub struct LocateFunction {
    signature: Signature,
}

impl LocateFunction {
    pub fn register(registry: &FunctionRegistry) {
        registry.register_scalar(LocateFunction::default());
    }
}

impl Default for LocateFunction {
    fn default() -> Self {
        // Support 2 or 3 arguments with various string types
        let mut signatures = Vec::new();
        let string_types = [DataType::Utf8, DataType::LargeUtf8, DataType::Utf8View];
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

        // 2-argument form: LOCATE(substr, str)
        for substr_type in &string_types {
            for str_type in &string_types {
                signatures.push(TypeSignature::Exact(vec![
                    substr_type.clone(),
                    str_type.clone(),
                ]));
            }
        }

        // 3-argument form: LOCATE(substr, str, pos)
        for substr_type in &string_types {
            for str_type in &string_types {
                for pos_type in &int_types {
                    signatures.push(TypeSignature::Exact(vec![
                        substr_type.clone(),
                        str_type.clone(),
                        pos_type.clone(),
                    ]));
                }
            }
        }

        Self {
            signature: Signature::one_of(signatures, Volatility::Immutable),
        }
    }
}

impl fmt::Display for LocateFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", NAME.to_ascii_uppercase())
    }
}

impl Function for LocateFunction {
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
        let arg_count = args.args.len();
        if !(2..=3).contains(&arg_count) {
            return Err(DataFusionError::Execution(
                "LOCATE requires 2 or 3 arguments: LOCATE(substr, str) or LOCATE(substr, str, pos)"
                    .to_string(),
            ));
        }

        let arrays = ColumnarValue::values_to_arrays(&args.args)?;

        // Cast string arguments to LargeUtf8 for uniform access
        let substr_array = cast_to_large_utf8(&arrays[0], "substr")?;
        let str_array = cast_to_large_utf8(&arrays[1], "str")?;

        let substr = substr_array.as_string::<i64>();
        let str_arr = str_array.as_string::<i64>();
        let len = substr.len();

        // Handle optional pos argument
        let pos_array: Option<ArrayRef> = if arg_count == 3 {
            Some(cast_to_int64(&arrays[2], "pos")?)
        } else {
            None
        };

        let mut builder = Int64Builder::with_capacity(len);

        for i in 0..len {
            if substr.is_null(i) || str_arr.is_null(i) {
                builder.append_null();
                continue;
            }

            let needle = substr.value(i);
            let haystack = str_arr.value(i);

            // Get starting position (1-based in MySQL, convert to 0-based)
            let start_pos = if let Some(ref pos_arr) = pos_array {
                if pos_arr.is_null(i) {
                    builder.append_null();
                    continue;
                }
                let pos = pos_arr
                    .as_primitive::<datafusion_common::arrow::datatypes::Int64Type>()
                    .value(i);
                if pos < 1 {
                    // MySQL returns 0 for pos < 1
                    builder.append_value(0);
                    continue;
                }
                (pos - 1) as usize
            } else {
                0
            };

            // Find position using character-based indexing (for Unicode support)
            let result = locate_substr(haystack, needle, start_pos);
            builder.append_value(result);
        }

        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

/// Cast array to LargeUtf8 for uniform string access.
fn cast_to_large_utf8(array: &ArrayRef, name: &str) -> datafusion_common::Result<ArrayRef> {
    cast(array.as_ref(), &DataType::LargeUtf8)
        .map_err(|e| DataFusionError::Execution(format!("LOCATE: {} cast failed: {}", name, e)))
}

fn cast_to_int64(array: &ArrayRef, name: &str) -> datafusion_common::Result<ArrayRef> {
    cast(array.as_ref(), &DataType::Int64)
        .map_err(|e| DataFusionError::Execution(format!("LOCATE: {} cast failed: {}", name, e)))
}

/// Find the 1-based position of needle in haystack, starting from start_pos (0-based character index).
/// Returns 0 if not found.
fn locate_substr(haystack: &str, needle: &str, start_pos: usize) -> i64 {
    // Handle empty needle - MySQL returns start_pos + 1
    if needle.is_empty() {
        let char_count = haystack.chars().count();
        return if start_pos <= char_count {
            (start_pos + 1) as i64
        } else {
            0
        };
    }

    // Convert start_pos (character index) to byte index
    let byte_start = haystack
        .char_indices()
        .nth(start_pos)
        .map(|(idx, _)| idx)
        .unwrap_or(haystack.len());

    if byte_start >= haystack.len() {
        return 0;
    }

    // Search in the substring
    let search_str = &haystack[byte_start..];
    if let Some(byte_pos) = search_str.find(needle) {
        // Convert byte position back to character position
        let char_pos = search_str[..byte_pos].chars().count();
        // Return 1-based position relative to original string
        (start_pos + char_pos + 1) as i64
    } else {
        0
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion_common::arrow::array::StringArray;
    use datafusion_common::arrow::datatypes::Field;
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

    #[test]
    fn test_locate_basic() {
        let function = LocateFunction::default();

        let substr = Arc::new(StringArray::from(vec!["world", "xyz", "hello"]));
        let str_arr = Arc::new(StringArray::from(vec![
            "hello world",
            "hello world",
            "hello world",
        ]));

        let args = create_args(vec![substr, str_arr]);
        let result = function.invoke_with_args(args).unwrap();

        if let ColumnarValue::Array(array) = result {
            let int_array = array.as_primitive::<datafusion_common::arrow::datatypes::Int64Type>();
            assert_eq!(int_array.value(0), 7); // "world" at position 7
            assert_eq!(int_array.value(1), 0); // "xyz" not found
            assert_eq!(int_array.value(2), 1); // "hello" at position 1
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_locate_with_position() {
        let function = LocateFunction::default();

        let substr = Arc::new(StringArray::from(vec!["o", "o", "o"]));
        let str_arr = Arc::new(StringArray::from(vec![
            "hello world",
            "hello world",
            "hello world",
        ]));
        let pos = Arc::new(datafusion_common::arrow::array::Int64Array::from(vec![
            1, 5, 8,
        ]));

        let args = create_args(vec![substr, str_arr, pos]);
        let result = function.invoke_with_args(args).unwrap();

        if let ColumnarValue::Array(array) = result {
            let int_array = array.as_primitive::<datafusion_common::arrow::datatypes::Int64Type>();
            assert_eq!(int_array.value(0), 5); // first 'o' at position 5
            assert_eq!(int_array.value(1), 5); // 'o' at position 5 (start from 5)
            assert_eq!(int_array.value(2), 8); // 'o' in "world" at position 8
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_locate_unicode() {
        let function = LocateFunction::default();

        let substr = Arc::new(StringArray::from(vec!["世", "界"]));
        let str_arr = Arc::new(StringArray::from(vec!["hello世界", "hello世界"]));

        let args = create_args(vec![substr, str_arr]);
        let result = function.invoke_with_args(args).unwrap();

        if let ColumnarValue::Array(array) = result {
            let int_array = array.as_primitive::<datafusion_common::arrow::datatypes::Int64Type>();
            assert_eq!(int_array.value(0), 6); // "世" at position 6
            assert_eq!(int_array.value(1), 7); // "界" at position 7
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_locate_empty_needle() {
        let function = LocateFunction::default();

        let substr = Arc::new(StringArray::from(vec!["", ""]));
        let str_arr = Arc::new(StringArray::from(vec!["hello", "hello"]));
        let pos = Arc::new(datafusion_common::arrow::array::Int64Array::from(vec![
            1, 3,
        ]));

        let args = create_args(vec![substr, str_arr, pos]);
        let result = function.invoke_with_args(args).unwrap();

        if let ColumnarValue::Array(array) = result {
            let int_array = array.as_primitive::<datafusion_common::arrow::datatypes::Int64Type>();
            assert_eq!(int_array.value(0), 1); // empty string at pos 1
            assert_eq!(int_array.value(1), 3); // empty string at pos 3
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_locate_with_nulls() {
        let function = LocateFunction::default();

        let substr = Arc::new(StringArray::from(vec![Some("o"), None]));
        let str_arr = Arc::new(StringArray::from(vec![Some("hello"), Some("hello")]));

        let args = create_args(vec![substr, str_arr]);
        let result = function.invoke_with_args(args).unwrap();

        if let ColumnarValue::Array(array) = result {
            let int_array = array.as_primitive::<datafusion_common::arrow::datatypes::Int64Type>();
            assert_eq!(int_array.value(0), 5);
            assert!(int_array.is_null(1));
        } else {
            panic!("Expected array result");
        }
    }
}
