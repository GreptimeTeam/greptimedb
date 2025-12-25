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
use datafusion_common::arrow::array::{Array, ArrayRef, AsArray, Int64Builder};
use datafusion_common::arrow::compute::cast;
use datafusion_common::arrow::datatypes::DataType;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, Signature, Volatility};

use crate::function::Function;
use crate::function_registry::FunctionRegistry;

const NAME: &str = "field";

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

        // Cast all arguments to LargeUtf8
        let string_arrays: Vec<ArrayRef> = arrays
            .iter()
            .enumerate()
            .map(|(i, arr)| {
                cast(arr.as_ref(), &DataType::LargeUtf8).map_err(|e| {
                    DataFusionError::Execution(format!("FIELD: argument {} cast failed: {}", i, e))
                })
            })
            .collect::<datafusion_common::Result<Vec<_>>>()?;

        let search_str = string_arrays[0].as_string::<i64>();
        let mut builder = Int64Builder::with_capacity(len);

        for i in 0..len {
            // If search string is NULL, return 0
            if search_str.is_null(i) {
                builder.append_value(0);
                continue;
            }

            let needle = search_str.value(i);
            let mut found_idx = 0i64;

            // Search through the list (starting from index 1 in string_arrays)
            for (j, str_arr) in string_arrays[1..].iter().enumerate() {
                let str_array = str_arr.as_string::<i64>();
                if !str_array.is_null(i) && str_array.value(i) == needle {
                    found_idx = (j + 1) as i64; // 1-based index
                    break;
                }
            }

            builder.append_value(found_idx);
        }

        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
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
}
