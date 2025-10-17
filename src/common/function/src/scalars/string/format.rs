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

// Licensed under the Apache License, Version 2.0 (the "License");

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::Arc;

use common_time::{Date, Timestamp, Timezone};
use datafusion_common::arrow::array::{Array, ArrayRef, AsArray, LargeStringBuilder};
use datafusion_common::arrow::compute::cast;
use datafusion_common::arrow::datatypes::DataType;
use datafusion_common::{DataFusionError, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, Signature, Volatility};

use crate::function::{Function, find_function_context};
use crate::function_registry::FunctionRegistry;

const NAME: &str = "format";

/// SQL fomrmat function which supports both positional and named placeholders.
/// The first argument is the format template string, and the rest are values to be
/// substituted into the template.
///# Examples
/// ```sql
/// SELECT format('Hello {}, you are {} years old', 'Alice', 25);
/// -- Result: 'Hello Alice, you are 25 years old'
/// ```
/// ```sql
/// SELECT format('Hello {name}, you are {age} years old', 'name', 'Bob', 'age', 30);
/// -- Result: 'Hello Bob, you are 30 years old'
/// ```
///
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
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
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
        if args.args.is_empty() {
            return Err(DataFusionError::Execution(
                "FORMAT requires at least one argument (the template)".to_string(),
            ));
        }

        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let fmt_array = &arrays[0];

        let fmt_dt = fmt_array.data_type();
        let fmt_is_utf8 = matches!(fmt_dt, DataType::Utf8);
        let fmt_is_large = matches!(fmt_dt, DataType::LargeUtf8);

        let casted_fmt_array = match cast(fmt_array.as_ref(), &DataType::LargeUtf8) {
            Ok(a) => Some(Arc::new(a) as ArrayRef),
            Err(_) => None,
        };

        if !(fmt_is_utf8 || fmt_is_large) && casted_fmt_array.is_none() {
            return Err(DataFusionError::Execution(format!(
                "FORMAT: first argument must be a string‑compatible type, got {fmt_dt:?}"
            )));
        }

        let value_arrays: Vec<ArrayRef> = arrays[1..].to_vec();
        let len = fmt_array.len();
        let mut builder = LargeStringBuilder::with_capacity(len, len * 64);
        let func_ctx = find_function_context(&args)?;
        let tz = func_ctx.query_ctx.timezone();

        for row in 0..len {
            let is_null = if let Some(casted) = &casted_fmt_array {
                casted.as_string::<i64>().is_null(row)
            } else if fmt_is_large {
                fmt_array.as_string::<i64>().is_null(row)
            } else {
                fmt_array.as_string::<i32>().is_null(row)
            };

            if is_null {
                builder.append_null();
                continue;
            }

            let fmt_str: &str = if let Some(casted) = &casted_fmt_array {
                casted.as_string::<i64>().value(row)
            } else if fmt_is_large {
                fmt_array.as_string::<i64>().value(row)
            } else {
                fmt_array.as_string::<i32>().value(row)
            };

            let mut values = Vec::with_capacity(value_arrays.len());
            for arr in &value_arrays {
                values.push(ScalarValue::try_from_array(arr, row)?);
            }

            match format_string(fmt_str, &values, &tz) {
                Ok(out) => builder.append_value(out),
                Err(e) => return Err(e),
            }
        }

        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

/// Scans the template once and performs placeholder substitution
fn format_string(
    template: &str,
    values: &[ScalarValue],
    timezone: &Timezone,
) -> datafusion_common::Result<String> {
    let needed_names = collect_placeholder_names(template);
    // index in values
    let mut used_indices: HashSet<usize> = HashSet::new();
    // key: name, value: ScalarValue
    let mut named_map: HashMap<String, &ScalarValue> = HashMap::new();

    let mut i = 0;
    while i < values.len() {
        let name_opt = match &values[i] {
            ScalarValue::Utf8(s) => s.as_ref(),
            ScalarValue::LargeUtf8(s) => s.as_ref(),
            _ => None,
        };
        if let Some(name) = name_opt
            && needed_names.contains(name)
        {
            if i + 1 >= values.len() {
                return Err(DataFusionError::Execution(format!(
                    "FORMAT: named argument '{}' has no corresponding value",
                    name
                )));
            }
            named_map.insert(name.clone(), &values[i + 1]);
            used_indices.insert(i);
            used_indices.insert(i + 1);
            i += 2;
            continue;
        }

        i += 1;
    }

    // Collect positional arguments
    let mut positional: Vec<&ScalarValue> = Vec::new();
    for (idx, v) in values.iter().enumerate() {
        if !used_indices.contains(&idx) {
            positional.push(v);
        }
    }
    let mut pos_idx = 0;
    let mut out = String::with_capacity(template.len());
    let bytes = template.as_bytes();
    let mut p = 0;

    while p < bytes.len() {
        match bytes[p] {
            b'{' => {
                // Escaped "{{"
                if p + 1 < bytes.len() && bytes[p + 1] == b'{' {
                    out.push('{');
                    p += 2;
                    continue;
                }
                let start = p + 1;
                let mut q = start;
                while q < bytes.len() && bytes[q] != b'}' {
                    q += 1;
                }
                if q < bytes.len() {
                    let name = &template[start..q];
                    // It's a positional placeholder
                    if name.is_empty() {
                        if pos_idx < positional.len() {
                            out.push_str(&scalar_value_to_string(positional[pos_idx], timezone)?);
                            pos_idx += 1;
                        } else {
                            out.push_str("{}");
                        }
                    } else if let Some(v) = named_map.get(name) {
                        // It's a named placeholder
                        out.push_str(&scalar_value_to_string(v, timezone)?);
                    } else {
                        // Unknown placeholder, keep as is
                        out.push('{');
                        out.push_str(name);
                        out.push('}');
                    }
                    p = q + 1;
                } else {
                    // Unknown placeholder, keep as is
                    out.push('{');
                    p += 1;
                }
            }
            b'}' => {
                if p + 1 < bytes.len() && bytes[p + 1] == b'}' {
                    out.push('}');
                    p += 2;
                } else {
                    out.push('}');
                    p += 1;
                }
            }
            _ => {
                let ch = template[p..].chars().next().unwrap();
                out.push(ch);
                p += ch.len_utf8();
            }
        }
    }

    Ok(out)
}

/// Collect named placeholders like `{name}` (ignoring empty `{}`)
fn collect_placeholder_names(template: &str) -> HashSet<String> {
    let mut set = HashSet::new();
    let bytes = template.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'{' {
            // Escaped "{{"
            if i + 1 < bytes.len() && bytes[i + 1] == b'{' {
                i += 2;
                continue;
            }
            let start = i + 1;
            let mut j = start;
            while j < bytes.len() && bytes[j] != b'}' {
                j += 1;
            }
            if j < bytes.len() && j > start {
                let name = &template[start..j];
                if !name.is_empty() {
                    set.insert(name.to_string());
                }
                i = j + 1;
                continue;
            }
        } else if bytes[i] == b'}' && i + 1 < bytes.len() && bytes[i + 1] == b'}' {
            // Escaped "}}"
            i += 2;
            continue;
        }
        i += 1;
    }
    set
}

/// Converts a ScalarValue into a string representation
fn scalar_value_to_string(
    value: &ScalarValue,
    timezone: &Timezone,
) -> datafusion_common::Result<String> {
    use ScalarValue::*;
    let s = match value {
        Null => "".to_string(),
        Boolean(Some(v)) => v.to_string(),
        Int8(Some(v)) => v.to_string(),
        Int16(Some(v)) => v.to_string(),
        Int32(Some(v)) => v.to_string(),
        Int64(Some(v)) => v.to_string(),
        UInt8(Some(v)) => v.to_string(),
        UInt16(Some(v)) => v.to_string(),
        UInt32(Some(v)) => v.to_string(),
        UInt64(Some(v)) => v.to_string(),
        Float32(Some(v)) => v.to_string(),
        Float64(Some(v)) => v.to_string(),
        Utf8(Some(s)) => s.clone(),
        LargeUtf8(Some(s)) => s.clone(),
        Date32(Some(days)) => Date::from(*days)
            .as_formatted_string("%Y-%m-%d", Some(timezone))
            .map_err(|e| DataFusionError::Execution(format!("Date32 formatting error: {}", e)))?
            .unwrap_or_else(|| "".to_string()),
        Date64(Some(ms)) => {
            let timestamp = Timestamp::new_millisecond(*ms);
            timestamp.to_timezone_aware_string(Some(timezone))
        }
        TimestampSecond(Some(ts), _) => {
            let timestamp = Timestamp::new_second(*ts);
            timestamp.to_timezone_aware_string(Some(timezone))
        }
        TimestampMillisecond(Some(ts), _) => {
            let timestamp = Timestamp::new_millisecond(*ts);
            timestamp.to_timezone_aware_string(Some(timezone))
        }
        TimestampMicrosecond(Some(ts), _) => {
            let timestamp = Timestamp::new_microsecond(*ts);
            timestamp.to_timezone_aware_string(Some(timezone))
        }
        TimestampNanosecond(Some(ts), _) => {
            let timestamp = Timestamp::new_nanosecond(*ts);
            timestamp.to_timezone_aware_string(Some(timezone))
        }
        Decimal128(Some(v), _p, scale) => {
            if *scale == 0 {
                v.to_string()
            } else {
                let sign = if *v < 0 { "-" } else { "" };
                let mut abs = v.abs().to_string();
                let s = *scale as usize;
                if abs.len() <= s {
                    abs = format!("{}{}", "0".repeat(s + 1 - abs.len()), abs);
                }
                let (int_part, frac_part) = abs.split_at(abs.len() - s);
                format!("{sign}{int_part}.{frac_part}")
            }
        }
        other => {
            return Err(DataFusionError::Execution(format!(
                "FORMAT: unsupported value type: {other:?}"
            )));
        }
    };
    Ok(s)
}

#[cfg(test)]
mod tests {
    use datafusion::config::ConfigOptions;
    use datafusion_common::arrow::array::{Int32Array, StringArray};
    use datafusion_common::arrow::datatypes::Field;
    use datafusion_expr::ScalarFunctionArgs;

    use super::*;
    use crate::function::FunctionContext;

    #[test]
    fn test_format_function_positional() {
        let format_array = Arc::new(StringArray::from(vec!["Hello {}, you are {} years old"]));
        let name_array = Arc::new(StringArray::from(vec!["Alice"]));
        let age_array = Arc::new(Int32Array::from(vec![25]));
        let mut config_options = ConfigOptions::default();
        config_options.extensions.insert(FunctionContext::default());

        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(format_array),
                ColumnarValue::Array(name_array),
                ColumnarValue::Array(age_array),
            ],
            arg_fields: vec![
                Arc::new(Field::new("arg_0", DataType::Utf8, false)),
                Arc::new(Field::new("arg_1", DataType::Utf8, false)),
                Arc::new(Field::new("arg_2", DataType::Int32, false)),
            ],
            return_field: Arc::new(Field::new("result", DataType::LargeUtf8, true)),
            number_rows: 1,
            config_options: Arc::new(config_options),
        };

        let function = FormatFunction::default();
        let result = function.invoke_with_args(args).unwrap();

        if let ColumnarValue::Array(array) = result {
            let string_array = array.as_string::<i64>();
            assert_eq!(string_array.value(0), "Hello Alice, you are 25 years old");
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_format_function_named() {
        let format_array = Arc::new(StringArray::from(vec![
            "Hello {name}, you are {age} years old",
        ]));
        let name_key_array = Arc::new(StringArray::from(vec!["name"]));
        let name_value_array = Arc::new(StringArray::from(vec!["Bob"]));
        let age_key_array = Arc::new(StringArray::from(vec!["age"]));
        let age_value_array = Arc::new(Int32Array::from(vec![30]));
        let mut config_options = ConfigOptions::default();
        config_options.extensions.insert(FunctionContext::default());

        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(format_array),
                ColumnarValue::Array(name_key_array),
                ColumnarValue::Array(name_value_array),
                ColumnarValue::Array(age_key_array),
                ColumnarValue::Array(age_value_array),
            ],
            arg_fields: vec![
                Arc::new(Field::new("arg_0", DataType::Utf8, false)),
                Arc::new(Field::new("arg_1", DataType::Utf8, false)),
                Arc::new(Field::new("arg_2", DataType::Utf8, false)),
                Arc::new(Field::new("arg_3", DataType::Utf8, false)),
                Arc::new(Field::new("arg_4", DataType::Int32, false)),
            ],
            return_field: Arc::new(Field::new("result", DataType::LargeUtf8, true)),
            number_rows: 1,
            config_options: Arc::new(config_options),
        };

        let function = FormatFunction::default();
        let result = function.invoke_with_args(args).unwrap();

        if let ColumnarValue::Array(array) = result {
            let string_array = array.as_string::<i64>();
            assert_eq!(string_array.value(0), "Hello Bob, you are 30 years old");
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_format_function_mixed_and_escaped() {
        let format_array = Arc::new(StringArray::from(vec!["Path: {{root}}/{dir}/file-{}.txt"]));
        let dir_key = Arc::new(StringArray::from(vec!["dir"]));
        let dir_val = Arc::new(StringArray::from(vec!["etc"]));
        let seq = Arc::new(Int32Array::from(vec![42]));
        let mut config_options = ConfigOptions::default();
        config_options.extensions.insert(FunctionContext::default());

        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(format_array),
                ColumnarValue::Array(dir_key),
                ColumnarValue::Array(dir_val),
                ColumnarValue::Array(seq),
            ],
            arg_fields: vec![
                Arc::new(Field::new("arg_0", DataType::Utf8, false)),
                Arc::new(Field::new("arg_1", DataType::Utf8, false)),
                Arc::new(Field::new("arg_2", DataType::Utf8, false)),
                Arc::new(Field::new("arg_3", DataType::Int32, false)),
            ],
            return_field: Arc::new(Field::new("result", DataType::LargeUtf8, true)),
            number_rows: 1,
            config_options: Arc::new(config_options),
        };

        let function = FormatFunction::default();
        let result = function.invoke_with_args(args).unwrap();

        if let ColumnarValue::Array(array) = result {
            let string_array = array.as_string::<i64>();
            assert_eq!(string_array.value(0), "Path: {root}/etc/file-42.txt");
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_format_function_insufficient_args() {
        let format_array = Arc::new(StringArray::from(vec!["A:{} B:{}"]));
        let only_one = Arc::new(Int32Array::from(vec![7]));
        let mut config_options = ConfigOptions::default();
        config_options.extensions.insert(FunctionContext::default());

        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(format_array),
                ColumnarValue::Array(only_one),
            ],
            arg_fields: vec![
                Arc::new(Field::new("arg_0", DataType::Utf8, false)),
                Arc::new(Field::new("arg_1", DataType::Int32, false)),
            ],
            return_field: Arc::new(Field::new("result", DataType::LargeUtf8, true)),
            number_rows: 1,
            config_options: Arc::new(config_options),
        };

        let function = FormatFunction::default();
        let result = function.invoke_with_args(args).unwrap();

        if let ColumnarValue::Array(array) = result {
            let string_array = array.as_string::<i64>();
            assert_eq!(string_array.value(0), "A:7 B:{}"); // 第二个不足，保留 {}
        } else {
            panic!("Expected array result");
        }
    }
}
