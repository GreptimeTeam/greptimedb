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

//! Implementation of REGEXP_EXTRACT function
use std::fmt;
use std::sync::Arc;

use datafusion_common::DataFusionError;
use datafusion_common::arrow::array::{Array, AsArray, LargeStringBuilder};
use datafusion_common::arrow::compute::cast;
use datafusion_common::arrow::datatypes::DataType;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, Signature, TypeSignature, Volatility};
use regex::{Regex, RegexBuilder};

use crate::function::Function;
use crate::function_registry::FunctionRegistry;

const NAME: &str = "regexp_extract";

// Safety limits
const MAX_REGEX_SIZE: usize = 1024 * 1024; // compiled regex heap cap
const MAX_DFA_SIZE: usize = 2 * 1024 * 1024; // lazy DFA cap
const MAX_TOTAL_RESULT_SIZE: usize = 64 * 1024 * 1024; // total batch cap
const MAX_SINGLE_MATCH: usize = 1024 * 1024; // per-row cap
const MAX_PATTERN_LEN: usize = 10_000; // pattern text length cap

/// REGEXP_EXTRACT function implementation
/// Extracts the first substring matching the given regular expression pattern.
/// If no match is found, returns NULL.
///
#[derive(Debug)]
pub struct RegexpExtractFunction {
    signature: Signature,
}

impl RegexpExtractFunction {
    pub fn register(registry: &FunctionRegistry) {
        registry.register_scalar(RegexpExtractFunction::default());
    }
}

impl Default for RegexpExtractFunction {
    fn default() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![DataType::Utf8View, DataType::Utf8]),
                    TypeSignature::Exact(vec![DataType::Utf8View, DataType::Utf8View]),
                    TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8View]),
                    TypeSignature::Exact(vec![DataType::LargeUtf8, DataType::Utf8View]),
                    TypeSignature::Exact(vec![DataType::Utf8View, DataType::LargeUtf8]),
                    TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8]),
                    TypeSignature::Exact(vec![DataType::LargeUtf8, DataType::Utf8]),
                    TypeSignature::Exact(vec![DataType::Utf8, DataType::LargeUtf8]),
                    TypeSignature::Exact(vec![DataType::LargeUtf8, DataType::LargeUtf8]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl fmt::Display for RegexpExtractFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", NAME.to_ascii_uppercase())
    }
}

impl Function for RegexpExtractFunction {
    fn name(&self) -> &str {
        NAME
    }

    // Always return LargeUtf8 for simplicity and safety
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
                "REGEXP_EXTRACT requires exactly two arguments (text, pattern)".to_string(),
            ));
        }

        // Keep original ColumnarValue variants for scalar-pattern fast path
        let pattern_is_scalar = matches!(args.args[1], ColumnarValue::Scalar(_));

        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let text_array = &arrays[0];
        let pattern_array = &arrays[1];

        // Cast both to LargeUtf8 for uniform access (supports Utf8/Utf8View/Dictionary<String>)
        let text_large = cast(text_array.as_ref(), &DataType::LargeUtf8).map_err(|e| {
            DataFusionError::Execution(format!("REGEXP_EXTRACT: text cast failed: {e}"))
        })?;
        let pattern_large = cast(pattern_array.as_ref(), &DataType::LargeUtf8).map_err(|e| {
            DataFusionError::Execution(format!("REGEXP_EXTRACT: pattern cast failed: {e}"))
        })?;

        let text = text_large.as_string::<i64>();
        let pattern = pattern_large.as_string::<i64>();
        let len = text.len();

        // Pre-size result builder with conservative estimate
        let mut estimated_total = 0usize;
        for i in 0..len {
            if !text.is_null(i) {
                estimated_total = estimated_total.saturating_add(text.value_length(i) as usize);
                if estimated_total > MAX_TOTAL_RESULT_SIZE {
                    return Err(DataFusionError::ResourcesExhausted(format!(
                        "REGEXP_EXTRACT total output exceeds {} bytes",
                        MAX_TOTAL_RESULT_SIZE
                    )));
                }
            }
        }
        let mut builder = LargeStringBuilder::with_capacity(len, estimated_total);

        // Fast path: if pattern is scalar, compile once
        let compiled_scalar: Option<Regex> = if pattern_is_scalar && len > 0 && !pattern.is_null(0)
        {
            Some(compile_regex_checked(pattern.value(0))?)
        } else {
            None
        };

        for i in 0..len {
            if text.is_null(i) || pattern.is_null(i) {
                builder.append_null();
                continue;
            }

            let s = text.value(i);
            let pat = pattern.value(i);

            // Compile or reuse regex
            let re = if let Some(ref compiled) = compiled_scalar {
                compiled
            } else {
                // TODO: For performance-critical applications with repeating patterns,
                // consider adding a small LRU cache here
                &compile_regex_checked(pat)?
            };

            // First match only
            if let Some(m) = re.find(s) {
                let m_str = m.as_str();
                if m_str.len() > MAX_SINGLE_MATCH {
                    return Err(DataFusionError::Execution(
                        "REGEXP_EXTRACT match exceeds per-row limit (1MB)".to_string(),
                    ));
                }
                builder.append_value(m_str);
            } else {
                builder.append_null();
            }
        }

        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

// Compile a regex with safety checks
fn compile_regex_checked(pattern: &str) -> datafusion_common::Result<Regex> {
    if pattern.len() > MAX_PATTERN_LEN {
        return Err(DataFusionError::Execution(format!(
            "REGEXP_EXTRACT pattern too long (> {} chars)",
            MAX_PATTERN_LEN
        )));
    }
    RegexBuilder::new(pattern)
        .size_limit(MAX_REGEX_SIZE)
        .dfa_size_limit(MAX_DFA_SIZE)
        .build()
        .map_err(|e| {
            DataFusionError::Execution(format!("REGEXP_EXTRACT invalid pattern '{}': {e}", pattern))
        })
}

#[cfg(test)]
mod tests {
    use datafusion_common::arrow::array::StringArray;
    use datafusion_common::arrow::datatypes::Field;
    use datafusion_expr::ScalarFunctionArgs;

    use super::*;

    #[test]
    fn test_regexp_extract_function_basic() {
        let text_array = Arc::new(StringArray::from(vec!["version 1.2.3", "no match here"]));
        let pattern_array = Arc::new(StringArray::from(vec!["\\d+\\.\\d+\\.\\d+", "\\d+"]));

        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(text_array),
                ColumnarValue::Array(pattern_array),
            ],
            arg_fields: vec![
                Arc::new(Field::new("arg_0", DataType::Utf8, false)),
                Arc::new(Field::new("arg_1", DataType::Utf8, false)),
            ],
            return_field: Arc::new(Field::new("result", DataType::LargeUtf8, true)),
            number_rows: 2,
            config_options: Arc::new(datafusion_common::config::ConfigOptions::default()),
        };

        let function = RegexpExtractFunction::default();
        let result = function.invoke_with_args(args).unwrap();

        if let ColumnarValue::Array(array) = result {
            let string_array = array.as_string::<i64>();
            assert_eq!(string_array.value(0), "1.2.3");
            assert!(string_array.is_null(1)); // no match should return NULL
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_regexp_extract_phone_number() {
        let text_array = Arc::new(StringArray::from(vec!["Phone: 123-456-7890", "No phone"]));
        let pattern_array = Arc::new(StringArray::from(vec![
            "\\d{3}-\\d{3}-\\d{4}",
            "\\d{3}-\\d{3}-\\d{4}",
        ]));

        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(text_array),
                ColumnarValue::Array(pattern_array),
            ],
            arg_fields: vec![
                Arc::new(Field::new("arg_0", DataType::Utf8, false)),
                Arc::new(Field::new("arg_1", DataType::Utf8, false)),
            ],
            return_field: Arc::new(Field::new("result", DataType::LargeUtf8, true)),
            number_rows: 2,
            config_options: Arc::new(datafusion_common::config::ConfigOptions::default()),
        };

        let function = RegexpExtractFunction::default();
        let result = function.invoke_with_args(args).unwrap();

        if let ColumnarValue::Array(array) = result {
            let string_array = array.as_string::<i64>();
            assert_eq!(string_array.value(0), "123-456-7890");
            assert!(string_array.is_null(1)); // no match should return NULL
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_regexp_extract_email() {
        let text_array = Arc::new(StringArray::from(vec![
            "Email: user@domain.com",
            "Invalid email",
        ]));
        let pattern_array = Arc::new(StringArray::from(vec![
            "[a-zA-Z0-9]+@[a-zA-Z0-9]+\\.[a-zA-Z]+",
            "[a-zA-Z0-9]+@[a-zA-Z0-9]+\\.[a-zA-Z]+",
        ]));

        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(text_array),
                ColumnarValue::Array(pattern_array),
            ],
            arg_fields: vec![
                Arc::new(Field::new("arg_0", DataType::Utf8, false)),
                Arc::new(Field::new("arg_1", DataType::Utf8, false)),
            ],
            return_field: Arc::new(Field::new("result", DataType::LargeUtf8, true)),
            number_rows: 2,
            config_options: Arc::new(datafusion_common::config::ConfigOptions::default()),
        };

        let function = RegexpExtractFunction::default();
        let result = function.invoke_with_args(args).unwrap();

        if let ColumnarValue::Array(array) = result {
            let string_array = array.as_string::<i64>();
            assert_eq!(string_array.value(0), "user@domain.com");
            assert!(string_array.is_null(1)); // no match should return NULL
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_regexp_extract_with_nulls() {
        let text_array = Arc::new(StringArray::from(vec![Some("test 123"), None]));
        let pattern_array = Arc::new(StringArray::from(vec![Some("\\d+"), Some("\\d+")]));

        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(text_array),
                ColumnarValue::Array(pattern_array),
            ],
            arg_fields: vec![
                Arc::new(Field::new("arg_0", DataType::Utf8, true)),
                Arc::new(Field::new("arg_1", DataType::Utf8, false)),
            ],
            return_field: Arc::new(Field::new("result", DataType::LargeUtf8, true)),
            number_rows: 2,
            config_options: Arc::new(datafusion_common::config::ConfigOptions::default()),
        };

        let function = RegexpExtractFunction::default();
        let result = function.invoke_with_args(args).unwrap();

        if let ColumnarValue::Array(array) = result {
            let string_array = array.as_string::<i64>();
            assert_eq!(string_array.value(0), "123");
            assert!(string_array.is_null(1)); // NULL input should return NULL
        } else {
            panic!("Expected array result");
        }
    }
}
