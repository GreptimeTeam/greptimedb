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

//! MySQL-compatible INSERT function implementation.
//!
//! INSERT(str, pos, len, newstr) - Inserts newstr into str at position pos,
//! replacing len characters.

use std::fmt;
use std::sync::Arc;

use datafusion_common::DataFusionError;
use datafusion_common::arrow::array::{Array, ArrayRef, AsArray, LargeStringBuilder};
use datafusion_common::arrow::compute::cast;
use datafusion_common::arrow::datatypes::DataType;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, Signature, TypeSignature, Volatility};

use crate::function::Function;
use crate::function_registry::FunctionRegistry;

const NAME: &str = "insert";

/// MySQL-compatible INSERT function.
///
/// Syntax: INSERT(str, pos, len, newstr)
/// Returns str with the substring beginning at position pos and len characters long
/// replaced by newstr.
///
/// - pos is 1-based
/// - If pos is out of range, returns the original string
/// - If len is out of range, replaces from pos to end of string
#[derive(Debug)]
pub struct InsertFunction {
    signature: Signature,
}

impl InsertFunction {
    pub fn register(registry: &FunctionRegistry) {
        registry.register_scalar(InsertFunction::default());
    }
}

impl Default for InsertFunction {
    fn default() -> Self {
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

        for str_type in &string_types {
            for newstr_type in &string_types {
                for pos_type in &int_types {
                    for len_type in &int_types {
                        signatures.push(TypeSignature::Exact(vec![
                            str_type.clone(),
                            pos_type.clone(),
                            len_type.clone(),
                            newstr_type.clone(),
                        ]));
                    }
                }
            }
        }

        Self {
            signature: Signature::one_of(signatures, Volatility::Immutable),
        }
    }
}

impl fmt::Display for InsertFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", NAME.to_ascii_uppercase())
    }
}

impl Function for InsertFunction {
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
        if args.args.len() != 4 {
            return Err(DataFusionError::Execution(
                "INSERT requires exactly 4 arguments: INSERT(str, pos, len, newstr)".to_string(),
            ));
        }

        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let len = arrays[0].len();

        // Cast string arguments to LargeUtf8
        let str_array = cast_to_large_utf8(&arrays[0], "str")?;
        let newstr_array = cast_to_large_utf8(&arrays[3], "newstr")?;
        let pos_array = cast_to_int64(&arrays[1], "pos")?;
        let replace_len_array = cast_to_int64(&arrays[2], "len")?;

        let str_arr = str_array.as_string::<i64>();
        let pos_arr = pos_array.as_primitive::<datafusion_common::arrow::datatypes::Int64Type>();
        let len_arr =
            replace_len_array.as_primitive::<datafusion_common::arrow::datatypes::Int64Type>();
        let newstr_arr = newstr_array.as_string::<i64>();

        let mut builder = LargeStringBuilder::with_capacity(len, len * 32);

        for i in 0..len {
            // Check for NULLs
            if str_arr.is_null(i)
                || pos_array.is_null(i)
                || replace_len_array.is_null(i)
                || newstr_arr.is_null(i)
            {
                builder.append_null();
                continue;
            }

            let original = str_arr.value(i);
            let pos = pos_arr.value(i);
            let replace_len = len_arr.value(i);
            let new_str = newstr_arr.value(i);

            let result = insert_string(original, pos, replace_len, new_str);
            builder.append_value(&result);
        }

        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

/// Cast array to LargeUtf8 for uniform string access.
fn cast_to_large_utf8(array: &ArrayRef, name: &str) -> datafusion_common::Result<ArrayRef> {
    cast(array.as_ref(), &DataType::LargeUtf8)
        .map_err(|e| DataFusionError::Execution(format!("INSERT: {} cast failed: {}", name, e)))
}

fn cast_to_int64(array: &ArrayRef, name: &str) -> datafusion_common::Result<ArrayRef> {
    cast(array.as_ref(), &DataType::Int64)
        .map_err(|e| DataFusionError::Execution(format!("INSERT: {} cast failed: {}", name, e)))
}

/// Perform the INSERT string operation.
/// pos is 1-based. If pos < 1 or pos > len(str) + 1, returns original string.
fn insert_string(original: &str, pos: i64, replace_len: i64, new_str: &str) -> String {
    let char_count = original.chars().count();

    // MySQL behavior: if pos < 1 or pos > string length + 1, return original
    if pos < 1 || pos as usize > char_count + 1 {
        return original.to_string();
    }

    let start_idx = (pos - 1) as usize; // Convert to 0-based

    // Calculate end index for replacement
    let replace_len = if replace_len < 0 {
        0
    } else {
        replace_len as usize
    };
    let end_idx = (start_idx + replace_len).min(char_count);

    let start_byte = char_to_byte_idx(original, start_idx);
    let end_byte = char_to_byte_idx(original, end_idx);

    let mut result = String::with_capacity(original.len() + new_str.len());
    result.push_str(&original[..start_byte]);
    result.push_str(new_str);
    result.push_str(&original[end_byte..]);
    result
}

fn char_to_byte_idx(s: &str, char_idx: usize) -> usize {
    s.char_indices()
        .nth(char_idx)
        .map(|(idx, _)| idx)
        .unwrap_or(s.len())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion_common::arrow::array::{Int64Array, StringArray};
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
            return_field: Arc::new(Field::new("result", DataType::LargeUtf8, true)),
            number_rows: arrays[0].len(),
            config_options: Arc::new(datafusion_common::config::ConfigOptions::default()),
        }
    }

    #[test]
    fn test_insert_basic() {
        let function = InsertFunction::default();

        // INSERT('Quadratic', 3, 4, 'What') => 'QuWhattic'
        let str_arr = Arc::new(StringArray::from(vec!["Quadratic"]));
        let pos = Arc::new(Int64Array::from(vec![3]));
        let len = Arc::new(Int64Array::from(vec![4]));
        let newstr = Arc::new(StringArray::from(vec!["What"]));

        let args = create_args(vec![str_arr, pos, len, newstr]);
        let result = function.invoke_with_args(args).unwrap();

        if let ColumnarValue::Array(array) = result {
            let str_array = array.as_string::<i64>();
            assert_eq!(str_array.value(0), "QuWhattic");
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_insert_out_of_range_pos() {
        let function = InsertFunction::default();

        // INSERT('Quadratic', 0, 4, 'What') => 'Quadratic' (pos < 1)
        let str_arr = Arc::new(StringArray::from(vec!["Quadratic", "Quadratic"]));
        let pos = Arc::new(Int64Array::from(vec![0, 100]));
        let len = Arc::new(Int64Array::from(vec![4, 4]));
        let newstr = Arc::new(StringArray::from(vec!["What", "What"]));

        let args = create_args(vec![str_arr, pos, len, newstr]);
        let result = function.invoke_with_args(args).unwrap();

        if let ColumnarValue::Array(array) = result {
            let str_array = array.as_string::<i64>();
            assert_eq!(str_array.value(0), "Quadratic"); // pos < 1
            assert_eq!(str_array.value(1), "Quadratic"); // pos > length
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_insert_replace_to_end() {
        let function = InsertFunction::default();

        // INSERT('Quadratic', 3, 100, 'What') => 'QuWhat' (len exceeds remaining)
        let str_arr = Arc::new(StringArray::from(vec!["Quadratic"]));
        let pos = Arc::new(Int64Array::from(vec![3]));
        let len = Arc::new(Int64Array::from(vec![100]));
        let newstr = Arc::new(StringArray::from(vec!["What"]));

        let args = create_args(vec![str_arr, pos, len, newstr]);
        let result = function.invoke_with_args(args).unwrap();

        if let ColumnarValue::Array(array) = result {
            let str_array = array.as_string::<i64>();
            assert_eq!(str_array.value(0), "QuWhat");
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_insert_unicode() {
        let function = InsertFunction::default();

        // INSERT('hello世界', 6, 1, 'の') => 'helloの界'
        let str_arr = Arc::new(StringArray::from(vec!["hello世界"]));
        let pos = Arc::new(Int64Array::from(vec![6]));
        let len = Arc::new(Int64Array::from(vec![1]));
        let newstr = Arc::new(StringArray::from(vec!["の"]));

        let args = create_args(vec![str_arr, pos, len, newstr]);
        let result = function.invoke_with_args(args).unwrap();

        if let ColumnarValue::Array(array) = result {
            let str_array = array.as_string::<i64>();
            assert_eq!(str_array.value(0), "helloの界");
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_insert_with_nulls() {
        let function = InsertFunction::default();

        let str_arr = Arc::new(StringArray::from(vec![Some("hello"), None]));
        let pos = Arc::new(Int64Array::from(vec![1, 1]));
        let len = Arc::new(Int64Array::from(vec![1, 1]));
        let newstr = Arc::new(StringArray::from(vec!["X", "X"]));

        let args = create_args(vec![str_arr, pos, len, newstr]);
        let result = function.invoke_with_args(args).unwrap();

        if let ColumnarValue::Array(array) = result {
            let str_array = array.as_string::<i64>();
            assert_eq!(str_array.value(0), "Xello");
            assert!(str_array.is_null(1));
        } else {
            panic!("Expected array result");
        }
    }
}
