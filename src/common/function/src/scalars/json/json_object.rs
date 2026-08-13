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

use datafusion_common::DataFusionError;
use datafusion_common::arrow::array::{Array, ArrayRef, AsArray, BinaryViewBuilder};
use datafusion_common::arrow::compute;
use datafusion_common::arrow::datatypes::{DataType, Float64Type, Int64Type, UInt64Type};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, Signature, Volatility};

use crate::function::Function;

const NAME: &str = "json_object";

/// Builds a `JSONB` object from interleaved `(key, value, key, value, ...)`
/// arguments, like MySQL's `JSON_OBJECT`. Values are written into the binary
/// directly, so they need no JSON text escaping. Keys must be non-NULL strings;
/// values may be strings, numbers, booleans, or NULL (rendered as JSON null).
/// A duplicate key keeps the last value.
#[derive(Clone, Debug)]
pub(crate) struct JsonObjectFunction {
    signature: Signature,
}

impl Default for JsonObjectFunction {
    fn default() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

/// A value column normalized to the canonical arrow type its JSON rendering
/// reads from.
enum ValueColumn {
    Null,
    Bool(ArrayRef),
    Int(ArrayRef),
    UInt(ArrayRef),
    Float(ArrayRef),
    String(ArrayRef),
}

impl ValueColumn {
    fn try_new(array: &ArrayRef) -> datafusion_common::Result<Self> {
        let normalized = match array.data_type() {
            DataType::Null => return Ok(ValueColumn::Null),
            DataType::Boolean => ValueColumn::Bool(array.clone()),
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
                ValueColumn::Int(compute::cast(array, &DataType::Int64)?)
            }
            DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
                ValueColumn::UInt(compute::cast(array, &DataType::UInt64)?)
            }
            DataType::Float16 | DataType::Float32 | DataType::Float64 => {
                ValueColumn::Float(compute::cast(array, &DataType::Float64)?)
            }
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
                ValueColumn::String(compute::cast(array, &DataType::Utf8View)?)
            }
            other => {
                return Err(DataFusionError::Execution(format!(
                    "{NAME} does not support values of type {other}; cast the value to a string"
                )));
            }
        };
        Ok(normalized)
    }

    fn value(&self, row: usize) -> jsonb::Value<'_> {
        let array = match self {
            ValueColumn::Null => return jsonb::Value::Null,
            ValueColumn::Bool(array)
            | ValueColumn::Int(array)
            | ValueColumn::UInt(array)
            | ValueColumn::Float(array)
            | ValueColumn::String(array) => array,
        };
        if !array.is_valid(row) {
            return jsonb::Value::Null;
        }
        match self {
            ValueColumn::Null => unreachable!(),
            ValueColumn::Bool(array) => array.as_boolean().value(row).into(),
            ValueColumn::Int(array) => array.as_primitive::<Int64Type>().value(row).into(),
            ValueColumn::UInt(array) => array.as_primitive::<UInt64Type>().value(row).into(),
            ValueColumn::Float(array) => array.as_primitive::<Float64Type>().value(row).into(),
            ValueColumn::String(array) => array.as_string_view().value(row).into(),
        }
    }
}

impl Function for JsonObjectFunction {
    fn name(&self) -> &str {
        NAME
    }

    fn return_type(&self, _: &[DataType]) -> datafusion_common::Result<DataType> {
        Ok(DataType::BinaryView)
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion_common::Result<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        if arrays.is_empty() || arrays.len() % 2 != 0 {
            return Err(DataFusionError::Execution(format!(
                "{NAME} expects (key, value) argument pairs, got {} arguments",
                arrays.len()
            )));
        }
        let pairs = arrays
            .chunks(2)
            .map(|pair| {
                let keys = compute::cast(&pair[0], &DataType::Utf8View).map_err(|_| {
                    DataFusionError::Execution(format!(
                        "{NAME} expects string keys, got {}",
                        pair[0].data_type()
                    ))
                })?;
                if keys.null_count() > 0 {
                    return Err(DataFusionError::Execution(format!(
                        "{NAME} does not allow NULL keys"
                    )));
                }
                Ok((keys, ValueColumn::try_new(&pair[1])?))
            })
            .collect::<datafusion_common::Result<Vec<_>>>()?;

        let rows = arrays[0].len();
        let mut builder = BinaryViewBuilder::with_capacity(rows);
        let mut buf = Vec::new();
        for row in 0..rows {
            let mut object = jsonb::Object::new();
            for (keys, values) in &pairs {
                object.insert(
                    keys.as_string_view().value(row).to_string(),
                    values.value(row),
                );
            }
            buf.clear();
            jsonb::Value::Object(object).write_to_vec(&mut buf);
            builder.append_value(&buf);
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

impl Display for JsonObjectFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "JSON_OBJECT")
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::Field;
    use datafusion_common::arrow::array::{
        Int64Array, NullArray, StringArray, TimestampMillisecondArray,
    };

    use super::*;

    fn invoke(args: Vec<ColumnarValue>, rows: usize) -> datafusion_common::Result<Vec<String>> {
        let function = JsonObjectFunction::default();
        let result = function.invoke_with_args(ScalarFunctionArgs {
            args,
            arg_fields: vec![],
            number_rows: rows,
            return_field: Arc::new(Field::new("x", DataType::BinaryView, true)),
            config_options: Arc::new(Default::default()),
        })?;
        let array = result.to_array(rows)?;
        let array = array.as_binary_view();
        Ok((0..array.len())
            .map(|i| jsonb::from_slice(array.value(i)).unwrap().to_string())
            .collect())
    }

    fn key(name: &str) -> ColumnarValue {
        ColumnarValue::Scalar(datafusion_common::ScalarValue::Utf8(Some(name.to_string())))
    }

    #[test]
    fn builds_objects_from_mixed_types_without_escaping() {
        let texts = invoke(
            vec![
                key("host"),
                ColumnarValue::Array(Arc::new(StringArray::from(vec![
                    Some("we\"ird\\\nhost"),
                    None,
                ]))),
                key("pid"),
                ColumnarValue::Array(Arc::new(Int64Array::from(vec![42, 7]))),
                key("up"),
                ColumnarValue::Scalar(datafusion_common::ScalarValue::Boolean(Some(true))),
                key("v"),
                ColumnarValue::Array(Arc::new(NullArray::new(2))),
            ],
            2,
        )
        .unwrap();
        // Keys come out sorted (JSONB objects are key-ordered); the raw control
        // character survives as a proper JSON escape; NULL values (typed or
        // Null-typed) are JSON null.
        assert_eq!(
            texts,
            vec![
                r#"{"host":"we\"ird\\\nhost","pid":42,"up":true,"v":null}"#,
                r#"{"host":null,"pid":7,"up":true,"v":null}"#,
            ]
        );
    }

    #[test]
    fn rejects_odd_arguments_null_keys_and_unsupported_values() {
        let err = invoke(vec![key("a")], 1).unwrap_err();
        assert!(err.to_string().contains("(key, value) argument pairs"));

        let err = invoke(
            vec![
                ColumnarValue::Array(Arc::new(StringArray::from(vec![None::<&str>]))),
                key("v"),
            ],
            1,
        )
        .unwrap_err();
        assert!(err.to_string().contains("NULL keys"));

        let err = invoke(
            vec![
                key("ts"),
                ColumnarValue::Array(Arc::new(TimestampMillisecondArray::from(vec![1_000]))),
            ],
            1,
        )
        .unwrap_err();
        assert!(err.to_string().contains("does not support values"));
    }
}
