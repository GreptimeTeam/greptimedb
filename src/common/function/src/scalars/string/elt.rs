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

//! MySQL-compatible ELT function implementation.
//!
//! ELT(N, str1, str2, str3, ...) - Returns the Nth string from the list.
//! Returns NULL if N < 1 or N > number of strings.

use std::fmt;
use std::sync::Arc;

use datafusion_common::DataFusionError;
use datafusion_common::arrow::array::{Array, ArrayRef, AsArray, LargeStringBuilder};
use datafusion_common::arrow::compute::cast;
use datafusion_common::arrow::datatypes::DataType;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, Signature, Volatility};

use crate::function::Function;
use crate::function_registry::FunctionRegistry;

const NAME: &str = "elt";

/// MySQL-compatible ELT function.
///
/// Syntax: ELT(N, str1, str2, str3, ...)
/// Returns the Nth string argument. N is 1-based.
/// Returns NULL if N is NULL, N < 1, or N > number of string arguments.
#[derive(Debug)]
pub struct EltFunction {
    signature: Signature,
}

impl EltFunction {
    pub fn register(registry: &FunctionRegistry) {
        registry.register_scalar(EltFunction::default());
    }
}

impl Default for EltFunction {
    fn default() -> Self {
        Self {
            // ELT takes a variable number of arguments: (Int64, String, String, ...)
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

impl fmt::Display for EltFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", NAME.to_ascii_uppercase())
    }
}

impl Function for EltFunction {
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
        if args.args.len() < 2 {
            return Err(DataFusionError::Execution(
                "ELT requires at least 2 arguments: ELT(N, str1, ...)".to_string(),
            ));
        }

        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let len = arrays[0].len();
        let num_strings = arrays.len() - 1;

        // First argument is the index (N) - try to cast to Int64
        let index_array = if arrays[0].data_type() == &DataType::Null {
            // All NULLs - return all NULLs
            let mut builder = LargeStringBuilder::with_capacity(len, 0);
            for _ in 0..len {
                builder.append_null();
            }
            return Ok(ColumnarValue::Array(Arc::new(builder.finish())));
        } else {
            cast(arrays[0].as_ref(), &DataType::Int64).map_err(|e| {
                DataFusionError::Execution(format!("ELT: index argument cast failed: {}", e))
            })?
        };

        // Cast string arguments to LargeUtf8
        let string_arrays: Vec<ArrayRef> = arrays[1..]
            .iter()
            .enumerate()
            .map(|(i, arr)| {
                cast(arr.as_ref(), &DataType::LargeUtf8).map_err(|e| {
                    DataFusionError::Execution(format!(
                        "ELT: string argument {} cast failed: {}",
                        i + 1,
                        e
                    ))
                })
            })
            .collect::<datafusion_common::Result<Vec<_>>>()?;

        let mut builder = LargeStringBuilder::with_capacity(len, len * 32);

        for i in 0..len {
            if index_array.is_null(i) {
                builder.append_null();
                continue;
            }

            let n = index_array
                .as_primitive::<datafusion_common::arrow::datatypes::Int64Type>()
                .value(i);

            // N is 1-based, check bounds
            if n < 1 || n as usize > num_strings {
                builder.append_null();
                continue;
            }

            let str_idx = (n - 1) as usize;
            let str_array = string_arrays[str_idx].as_string::<i64>();

            if str_array.is_null(i) {
                builder.append_null();
            } else {
                builder.append_value(str_array.value(i));
            }
        }

        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
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
    fn test_elt_basic() {
        let function = EltFunction::default();

        let n = Arc::new(Int64Array::from(vec![1, 2, 3]));
        let s1 = Arc::new(StringArray::from(vec!["a", "a", "a"]));
        let s2 = Arc::new(StringArray::from(vec!["b", "b", "b"]));
        let s3 = Arc::new(StringArray::from(vec!["c", "c", "c"]));

        let args = create_args(vec![n, s1, s2, s3]);
        let result = function.invoke_with_args(args).unwrap();

        if let ColumnarValue::Array(array) = result {
            let str_array = array.as_string::<i64>();
            assert_eq!(str_array.value(0), "a");
            assert_eq!(str_array.value(1), "b");
            assert_eq!(str_array.value(2), "c");
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_elt_out_of_bounds() {
        let function = EltFunction::default();

        let n = Arc::new(Int64Array::from(vec![0, 4, -1]));
        let s1 = Arc::new(StringArray::from(vec!["a", "a", "a"]));
        let s2 = Arc::new(StringArray::from(vec!["b", "b", "b"]));
        let s3 = Arc::new(StringArray::from(vec!["c", "c", "c"]));

        let args = create_args(vec![n, s1, s2, s3]);
        let result = function.invoke_with_args(args).unwrap();

        if let ColumnarValue::Array(array) = result {
            let str_array = array.as_string::<i64>();
            assert!(str_array.is_null(0)); // 0 is out of bounds
            assert!(str_array.is_null(1)); // 4 is out of bounds
            assert!(str_array.is_null(2)); // -1 is out of bounds
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_elt_with_nulls() {
        let function = EltFunction::default();

        // Row 0: n=1, select s1="a" -> "a"
        // Row 1: n=NULL -> NULL
        // Row 2: n=1, select s1=NULL -> NULL
        let n = Arc::new(Int64Array::from(vec![Some(1), None, Some(1)]));
        let s1 = Arc::new(StringArray::from(vec![Some("a"), Some("a"), None]));
        let s2 = Arc::new(StringArray::from(vec![Some("b"), Some("b"), Some("b")]));

        let args = create_args(vec![n, s1, s2]);
        let result = function.invoke_with_args(args).unwrap();

        if let ColumnarValue::Array(array) = result {
            let str_array = array.as_string::<i64>();
            assert_eq!(str_array.value(0), "a");
            assert!(str_array.is_null(1)); // N is NULL
            assert!(str_array.is_null(2)); // Selected string is NULL
        } else {
            panic!("Expected array result");
        }
    }
}
