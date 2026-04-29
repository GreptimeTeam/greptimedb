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

use std::sync::Arc;

use arrow::array::{ArrayRef, BinaryViewArray, new_null_array};
use arrow::compute;
use arrow::datatypes::Float64Type;
use arrow_schema::Field;
use datafusion_common::arrow::array::{
    Array, AsArray, BinaryViewBuilder, BooleanBuilder, Float64Builder, Int64Builder,
    StringViewBuilder,
};
use datafusion_common::arrow::datatypes::DataType;
use datafusion_common::{DataFusionError, Result, ScalarValue, exec_datafusion_err, exec_err};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, Signature, Volatility};
use datatypes::arrow_array::{int_array_value_at_index, string_array_value_at_index};
use datatypes::vectors::json::array::JsonArray;
use derive_more::Display;
use serde_json::Value;

use crate::function::{Function, extract_args};
use crate::helper;

fn get_json_by_path(json: &[u8], path: &str) -> Option<Vec<u8>> {
    let json_path = jsonb::jsonpath::parse_json_path(path.as_bytes());
    match json_path {
        Ok(json_path) => {
            let mut sub_jsonb = Vec::new();
            let mut sub_offsets = Vec::new();
            match jsonb::get_by_path(json, json_path, &mut sub_jsonb, &mut sub_offsets) {
                Ok(_) => Some(sub_jsonb),
                Err(_) => None,
            }
        }
        _ => None,
    }
}

enum JsonResultValue<'a> {
    Jsonb(Vec<u8>),
    #[expect(unused)]
    JsonStructByColumn(&'a ArrayRef, usize),
    JsonStructByValue(&'a Value),
}

trait JsonGetResultBuilder {
    fn append_value(&mut self, value: JsonResultValue<'_>) -> Result<()>;

    fn append_null(&mut self);

    fn build(&mut self) -> ArrayRef;
}

fn result_builder(
    len: usize,
    with_type: Option<&DataType>,
) -> Result<Box<dyn JsonGetResultBuilder>> {
    let builder = if let Some(t) = with_type {
        match t {
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
                Box::new(StringResultBuilder(StringViewBuilder::with_capacity(len)))
                    as Box<dyn JsonGetResultBuilder>
            }
            DataType::Int64 => Box::new(IntResultBuilder(Int64Builder::with_capacity(len))),
            DataType::Float64 => Box::new(FloatResultBuilder(Float64Builder::with_capacity(len))),
            DataType::Boolean => Box::new(BoolResultBuilder(BooleanBuilder::with_capacity(len))),
            t => {
                return exec_err!("json_get with unknown type {t}");
            }
        }
    } else {
        Box::new(StringResultBuilder(StringViewBuilder::with_capacity(len)))
    };
    Ok(builder)
}

// TODO: refactor this to StringLikeArrayBuilder from Arrow 57
struct StringResultBuilder(StringViewBuilder);

impl JsonGetResultBuilder for StringResultBuilder {
    fn append_value(&mut self, value: JsonResultValue<'_>) -> Result<()> {
        match value {
            JsonResultValue::Jsonb(value) => self.0.append_option(jsonb::to_str(&value).ok()),
            JsonResultValue::JsonStructByColumn(column, i) => {
                if let Some(v) = string_array_value_at_index(column, i) {
                    self.0.append_value(v);
                } else {
                    self.0
                        .append_value(arrow_cast::display::array_value_to_string(column, i)?);
                }
            }
            JsonResultValue::JsonStructByValue(value) => {
                if let Some(s) = value.as_str() {
                    self.0.append_value(s)
                } else {
                    self.0.append_value(value.to_string())
                }
            }
        }
        Ok(())
    }

    fn append_null(&mut self) {
        self.0.append_null();
    }

    fn build(&mut self) -> ArrayRef {
        Arc::new(self.0.finish())
    }
}

#[derive(Default, Display, Debug)]
#[display("{}", Self::NAME.to_ascii_uppercase())]
pub struct JsonGetString(JsonGetWithType);

impl JsonGetString {
    pub const NAME: &'static str = "json_get_string";
}

impl Function for JsonGetString {
    fn name(&self) -> &str {
        Self::NAME
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8View)
    }

    fn signature(&self) -> &Signature {
        &self.0.signature
    }

    fn invoke_with_args(&self, mut args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        args.args
            .push(ColumnarValue::Scalar(ScalarValue::Utf8View(None)));
        self.0.invoke_with_args(args)
    }
}

struct IntResultBuilder(Int64Builder);

impl JsonGetResultBuilder for IntResultBuilder {
    fn append_value(&mut self, value: JsonResultValue<'_>) -> Result<()> {
        match value {
            JsonResultValue::Jsonb(value) => self.0.append_option(jsonb::to_i64(&value).ok()),
            JsonResultValue::JsonStructByColumn(column, i) => {
                self.0.append_option(int_array_value_at_index(column, i))
            }
            JsonResultValue::JsonStructByValue(value) => self.0.append_option(value.as_i64()),
        }
        Ok(())
    }

    fn append_null(&mut self) {
        self.0.append_null();
    }

    fn build(&mut self) -> ArrayRef {
        Arc::new(self.0.finish())
    }
}

#[derive(Default, Display, Debug)]
#[display("{}", Self::NAME.to_ascii_uppercase())]
pub struct JsonGetInt(JsonGetWithType);

impl JsonGetInt {
    pub const NAME: &'static str = "json_get_int";
}

impl Function for JsonGetInt {
    fn name(&self) -> &str {
        Self::NAME
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }

    fn signature(&self) -> &Signature {
        &self.0.signature
    }

    fn invoke_with_args(&self, mut args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        args.args
            .push(ColumnarValue::Scalar(ScalarValue::Int64(None)));
        self.0.invoke_with_args(args)
    }
}

struct FloatResultBuilder(Float64Builder);

impl JsonGetResultBuilder for FloatResultBuilder {
    fn append_value(&mut self, value: JsonResultValue<'_>) -> Result<()> {
        match value {
            JsonResultValue::Jsonb(value) => self.0.append_option(jsonb::to_f64(&value).ok()),
            JsonResultValue::JsonStructByColumn(column, i) => {
                let result = if column.data_type() == &DataType::Float64 {
                    column
                        .as_primitive::<Float64Type>()
                        .is_valid(i)
                        .then(|| column.as_primitive::<Float64Type>().value(i))
                } else {
                    None
                };
                self.0.append_option(result);
            }
            JsonResultValue::JsonStructByValue(value) => self.0.append_option(value.as_f64()),
        }
        Ok(())
    }

    fn append_null(&mut self) {
        self.0.append_null();
    }

    fn build(&mut self) -> ArrayRef {
        Arc::new(self.0.finish())
    }
}

#[derive(Default, Display, Debug)]
#[display("{}", Self::NAME.to_ascii_uppercase())]
pub struct JsonGetFloat(JsonGetWithType);

impl JsonGetFloat {
    pub const NAME: &'static str = "json_get_float";
}

impl Function for JsonGetFloat {
    fn name(&self) -> &str {
        Self::NAME
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn signature(&self) -> &Signature {
        &self.0.signature
    }

    fn invoke_with_args(&self, mut args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        args.args
            .push(ColumnarValue::Scalar(ScalarValue::Float64(None)));
        self.0.invoke_with_args(args)
    }
}

struct BoolResultBuilder(BooleanBuilder);

impl JsonGetResultBuilder for BoolResultBuilder {
    fn append_value(&mut self, value: JsonResultValue<'_>) -> Result<()> {
        match value {
            JsonResultValue::Jsonb(value) => self.0.append_option(jsonb::to_bool(&value).ok()),
            JsonResultValue::JsonStructByColumn(column, i) => {
                let result = if column.data_type() == &DataType::Boolean {
                    column
                        .as_boolean()
                        .is_valid(i)
                        .then(|| column.as_boolean().value(i))
                } else {
                    None
                };
                self.0.append_option(result);
            }
            JsonResultValue::JsonStructByValue(value) => self.0.append_option(value.as_bool()),
        }
        Ok(())
    }

    fn append_null(&mut self) {
        self.0.append_null();
    }

    fn build(&mut self) -> ArrayRef {
        Arc::new(self.0.finish())
    }
}

#[derive(Default, Display, Debug)]
#[display("{}", Self::NAME.to_ascii_uppercase())]
pub struct JsonGetBool(JsonGetWithType);

impl JsonGetBool {
    pub const NAME: &'static str = "json_get_bool";
}

impl Function for JsonGetBool {
    fn name(&self) -> &str {
        Self::NAME
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn signature(&self) -> &Signature {
        &self.0.signature
    }

    fn invoke_with_args(&self, mut args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        args.args
            .push(ColumnarValue::Scalar(ScalarValue::Boolean(None)));
        self.0.invoke_with_args(args)
    }
}

fn jsonb_get(
    jsons: &BinaryViewArray,
    path: &str,
    builder: &mut dyn JsonGetResultBuilder,
) -> Result<()> {
    let size = jsons.len();
    for i in 0..size {
        let json = jsons.is_valid(i).then(|| jsons.value(i));
        let result = match json {
            Some(json) => get_json_by_path(json, path),
            _ => None,
        };
        if let Some(v) = result {
            builder.append_value(JsonResultValue::Jsonb(v))?;
        } else {
            builder.append_null();
        }
    }
    Ok(())
}

fn json_struct_get(array: &ArrayRef, path: &str, with_type: Option<&DataType>) -> Result<ArrayRef> {
    let path = path.trim_start_matches("$");

    // Fast path: if the JSON array fields can be directly indexed into by the `path`, simply get
    // the sub-array (`column_by_name`).
    let mut direct = true;
    let mut current = array;
    for segment in path.split(".").filter(|s| !s.is_empty()) {
        if matches!(current.data_type(), DataType::Binary) {
            direct = false;
            break;
        }

        let Some(json) = current.as_struct_opt() else {
            return exec_err!("unknown JSON array datatype: {}", current.data_type());
        };
        let Some(sub_json) = json.column_by_name(segment) else {
            return Ok(new_null_array(
                with_type.unwrap_or(&DataType::Utf8View),
                array.len(),
            ));
        };
        current = sub_json;
    }

    // Build the result array with optional value mapper.
    fn build_with<F>(
        input: &ArrayRef,
        with_type: Option<&DataType>,
        value_mapper: F,
    ) -> Result<ArrayRef>
    where
        for<'a> F: Fn(&'a Value) -> Option<&'a Value>,
    {
        let json_array = JsonArray::from(input);

        let mut builder = result_builder(input.len(), with_type)?;
        for i in 0..input.len() {
            if input.is_null(i) {
                builder.append_null();
                continue;
            }

            let value = json_array
                .try_get_value(i)
                .map_err(|e| exec_datafusion_err!("{e}"))?;
            let value = value_mapper(&value);

            if let Some(value) = value {
                builder.append_value(JsonResultValue::JsonStructByValue(value))?;
            } else {
                builder.append_null();
            }
        }
        Ok(builder.build())
    }

    if direct {
        let casted = if let Some(with_type) = with_type
            && current.data_type() != with_type
        {
            match (current.data_type(), with_type) {
                (DataType::Binary, _) => {
                    // Fall back to the slow path if the found JSON sub-array is serialized to bytes
                    // (because of JSON type conflicting)
                    build_with(current, Some(with_type), |v| Some(v))?
                }
                (DataType::List(_) | DataType::Struct(_), with_type) if with_type.is_string() => {
                    // Special handle for wanted array is string (Arrow cast is not working here if
                    // the datatype is list or struct), because it could be used in displaying the
                    // result.
                    build_with(current, Some(with_type), |v| Some(v))?
                }
                (_, with_type) if with_type.is_string() => {
                    // Same special handle for wanted array is string as above, except for simply
                    // casting by Arrow is more desirable.
                    arrow_cast::cast(current.as_ref(), with_type)?
                }
                _ => new_null_array(with_type, current.len()),
            }
        } else {
            current.clone()
        };
        return Ok(casted);
    }

    // Slow path: reconstruct the JSON array from serialized representation of conflicting JSON
    // values: `serde_json::Value`.
    let mut pointer = path.replace(".", "/");
    if !pointer.starts_with("/") {
        pointer = format!("/{}", pointer);
    }
    build_with(array, with_type, |value| value.pointer(&pointer))
}

/// This function is mostly called as `json_get(value, 'attr')::type` and rewritten by
/// `json_get_rewriter::JsonGetRewriter` to `json_get(value, 'attr', NULL::type)`. So we
/// use the third argument's type to determine the return type.
#[derive(Debug, Display)]
#[display("{}", Self::NAME.to_ascii_uppercase())]
pub(super) struct JsonGetWithType {
    signature: Signature,
}

impl JsonGetWithType {
    pub(crate) const NAME: &'static str = "json_get";
}

impl Default for JsonGetWithType {
    fn default() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

impl Function for JsonGetWithType {
    fn name(&self) -> &str {
        Self::NAME
    }

    fn return_type(&self, _input_types: &[DataType]) -> datafusion_common::Result<DataType> {
        Err(DataFusionError::Internal(
            "This method isn't meant to be called".to_string(),
        ))
    }

    fn return_field_from_args(
        &self,
        args: datafusion_expr::ReturnFieldArgs<'_>,
    ) -> datafusion_common::Result<Arc<Field>> {
        match args.scalar_arguments.get(2) {
            Some(Some(v)) => {
                let mut data_type = v.data_type();
                if matches!(data_type, DataType::Utf8 | DataType::LargeUtf8) {
                    data_type = DataType::Utf8View;
                }

                Ok(Arc::new(Field::new(self.name(), data_type, true)))
            }
            _ => Ok(Arc::new(Field::new(self.name(), DataType::Utf8View, true))),
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion_common::Result<ColumnarValue> {
        let args_len = args.args.len();
        if args_len != 2 && args_len != 3 {
            return exec_err!("json_get expects 2 or 3 arguments, got {args_len}");
        }

        let arg0 = args.args[0].to_array(args.number_rows)?;
        let len = arg0.len();

        let path = if let ColumnarValue::Scalar(path) = &args.args[1]
            && let Some(Some(path)) = path.try_as_str()
        {
            path
        } else {
            return exec_err!(
                r#"json_get expects a string literal "path" argument, got {}"#,
                args.args[1]
            );
        };

        let with_type = args.args.get(2).map(|x| x.data_type());
        let result = match arg0.data_type() {
            DataType::Binary | DataType::LargeBinary | DataType::BinaryView => {
                let arg0 = compute::cast(&arg0, &DataType::BinaryView)?;
                let jsons = arg0.as_binary_view();

                let mut builder = result_builder(len, with_type.as_ref())?;
                jsonb_get(jsons, path, builder.as_mut())?;
                builder.build()
            }
            DataType::Struct(_) => json_struct_get(&arg0, path, with_type.as_ref())?,
            _ => {
                return exec_err!("JSON_GET not supported argument type {}", arg0.data_type());
            }
        };

        Ok(ColumnarValue::Array(result))
    }
}

/// Get the object from JSON value by path.
#[derive(Display, Debug)]
#[display("{}", Self::NAME.to_ascii_uppercase())]
pub(super) struct JsonGetObject {
    signature: Signature,
}

impl JsonGetObject {
    const NAME: &'static str = "json_get_object";
}

impl Default for JsonGetObject {
    fn default() -> Self {
        Self {
            signature: helper::one_of_sigs2(
                vec![
                    DataType::Binary,
                    DataType::LargeBinary,
                    DataType::BinaryView,
                ],
                vec![DataType::UInt8, DataType::LargeUtf8, DataType::Utf8View],
            ),
        }
    }
}

impl Function for JsonGetObject {
    fn name(&self) -> &str {
        Self::NAME
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
        let [arg0, arg1] = extract_args(self.name(), &args)?;
        let arg0 = compute::cast(&arg0, &DataType::BinaryView)?;
        let jsons = arg0.as_binary_view();
        let arg1 = compute::cast(&arg1, &DataType::Utf8View)?;
        let paths = arg1.as_string_view();

        let len = jsons.len();
        let mut builder = BinaryViewBuilder::with_capacity(len);

        for i in 0..len {
            let json = jsons.is_valid(i).then(|| jsons.value(i));
            let path = paths.is_valid(i).then(|| paths.value(i));
            let result = if let (Some(json), Some(path)) = (json, path) {
                let result = jsonb::jsonpath::parse_json_path(path.as_bytes()).and_then(|path| {
                    let mut data = Vec::new();
                    let mut offset = Vec::new();
                    jsonb::get_by_path(json, path, &mut data, &mut offset)
                        .map(|()| jsonb::is_object(&data).then_some(data))
                });
                result.map_err(|e| DataFusionError::Execution(e.to_string()))?
            } else {
                None
            };
            builder.append_option(result);
        }

        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{BooleanArray, Int64Array, StructArray};
    use arrow_schema::{Field, Fields};
    use datafusion_common::ScalarValue;
    use datafusion_common::arrow::array::{BinaryArray, BinaryViewArray, StringArray};
    use datafusion_common::arrow::datatypes::{Float64Type, Int64Type};
    use datatypes::types::parse_string_to_jsonb;
    use serde_json::json;

    use super::*;

    /// Create a JSON object like this (as a one element struct array for testing):
    ///
    /// ```JSON
    /// {
    ///     "kind": "foo",
    ///     "payload": {
    ///         "code": 404,
    ///         "success": false,
    ///         "result": {
    ///             "error": "not found",
    ///             "time_cost": 1.234
    ///         }
    ///     }
    /// }
    /// ```
    fn test_json_struct() -> ArrayRef {
        let payload_fields = Fields::from(vec![
            Field::new("code", DataType::Int64, true),
            Field::new("success", DataType::Boolean, true),
            Field::new("result", DataType::Binary, true),
        ]);
        Arc::new(StructArray::new(
            vec![
                Field::new("kind", DataType::Utf8, true),
                Field::new("payload", DataType::Struct(payload_fields.clone()), true),
            ]
            .into(),
            vec![
                Arc::new(StringArray::from_iter([Some("foo")])) as ArrayRef,
                Arc::new(StructArray::new(
                    payload_fields,
                    vec![
                        Arc::new(Int64Array::from_iter([Some(404)])) as ArrayRef,
                        Arc::new(BooleanArray::from_iter([Some(false)])),
                        Arc::new(BinaryArray::from_iter([Some(
                            json!({
                                "error": "not found",
                                "time_cost": 1.234
                            })
                            .to_string()
                            .as_bytes(),
                        )])),
                    ],
                    None,
                )),
            ],
            None,
        ))
    }

    #[test]
    fn test_json_get_int() {
        let json_get_int = JsonGetInt::default();

        assert_eq!("json_get_int", json_get_int.name());
        assert_eq!(
            DataType::Int64,
            json_get_int
                .return_type(&[DataType::Binary, DataType::Utf8])
                .unwrap()
        );

        let json_strings = [
            r#"{"a": {"b": 2}, "b": 2, "c": 3}"#,
            r#"{"a": 4, "b": {"c": 6}, "c": 6}"#,
            r#"{"a": 7, "b": 8, "c": {"a": 7}}"#,
        ];
        let json_struct = test_json_struct();

        let path_expects = vec![
            ("$.a.b", Some(2)),
            ("$.a", Some(4)),
            ("$.c", None),
            ("$.kind", None),
            ("$.payload.code", Some(404)),
            ("$.payload.success", None),
            ("$.payload.result.time_cost", None),
            ("$.payload.not-exists", None),
            ("$.not-exists", None),
            ("$", None),
        ];

        let mut jsons = json_strings
            .iter()
            .map(|s| {
                let value = jsonb::parse_value(s.as_bytes()).unwrap();
                Arc::new(BinaryArray::from_iter_values([value.to_vec()])) as ArrayRef
            })
            .collect::<Vec<_>>();
        let json_struct_arrays =
            std::iter::repeat_n(json_struct, path_expects.len() - jsons.len()).collect::<Vec<_>>();
        jsons.extend(json_struct_arrays);

        for i in 0..jsons.len() {
            let json = &jsons[i];
            let (path, expect) = path_expects[i];

            let args = ScalarFunctionArgs {
                args: vec![
                    ColumnarValue::Array(json.clone()),
                    ColumnarValue::Scalar(path.into()),
                ],
                arg_fields: vec![],
                number_rows: 1,
                return_field: Arc::new(Field::new("x", DataType::Int64, false)),
                config_options: Arc::new(Default::default()),
            };
            let result = json_get_int
                .invoke_with_args(args)
                .and_then(|x| x.to_array(1))
                .unwrap();

            let result = result.as_primitive::<Int64Type>();
            assert_eq!(1, result.len());
            let actual = result.is_valid(0).then(|| result.value(0));
            assert_eq!(actual, expect);
        }
    }

    #[test]
    fn test_json_get_float() {
        let json_get_float = JsonGetFloat::default();

        assert_eq!("json_get_float", json_get_float.name());
        assert_eq!(
            DataType::Float64,
            json_get_float
                .return_type(&[DataType::Binary, DataType::Utf8])
                .unwrap()
        );

        let json_strings = [
            r#"{"a": {"b": 2.1}, "b": 2.2, "c": 3.3}"#,
            r#"{"a": 4.4, "b": {"c": 6.6}, "c": 6.6}"#,
            r#"{"a": 7.7, "b": 8.8, "c": {"a": 7.7}}"#,
        ];
        let json_struct = test_json_struct();

        let path_expects = vec![
            ("$.a.b", Some(2.1)),
            ("$.a", Some(4.4)),
            ("$.c", None),
            ("$.kind", None),
            ("$.payload.code", None),
            ("$.payload.success", None),
            ("$.payload.result.time_cost", Some(1.234)),
            ("$.payload.not-exists", None),
            ("$.not-exists", None),
            ("$", None),
        ];

        let mut jsons = json_strings
            .iter()
            .map(|s| {
                let value = jsonb::parse_value(s.as_bytes()).unwrap();
                Arc::new(BinaryArray::from_iter_values([value.to_vec()])) as ArrayRef
            })
            .collect::<Vec<_>>();
        let json_struct_arrays =
            std::iter::repeat_n(json_struct, path_expects.len() - jsons.len()).collect::<Vec<_>>();
        jsons.extend(json_struct_arrays);

        for i in 0..jsons.len() {
            let json = &jsons[i];
            let (path, expect) = path_expects[i];

            let args = ScalarFunctionArgs {
                args: vec![
                    ColumnarValue::Array(json.clone()),
                    ColumnarValue::Scalar(path.into()),
                ],
                arg_fields: vec![],
                number_rows: 1,
                return_field: Arc::new(Field::new("x", DataType::Float64, false)),
                config_options: Arc::new(Default::default()),
            };
            let result = json_get_float
                .invoke_with_args(args)
                .and_then(|x| x.to_array(1))
                .unwrap();

            let result = result.as_primitive::<Float64Type>();
            assert_eq!(1, result.len());
            let actual = result.is_valid(0).then(|| result.value(0));
            assert_eq!(actual, expect);
        }
    }

    #[test]
    fn test_json_get_bool() {
        let json_get_bool = JsonGetBool::default();

        assert_eq!("json_get_bool", json_get_bool.name());
        assert_eq!(
            DataType::Boolean,
            json_get_bool
                .return_type(&[DataType::Binary, DataType::Utf8])
                .unwrap()
        );

        let json_strings = [
            r#"{"a": {"b": true}, "b": false, "c": true}"#,
            r#"{"a": false, "b": {"c": true}, "c": false}"#,
            r#"{"a": true, "b": false, "c": {"a": true}}"#,
        ];
        let json_struct = test_json_struct();

        let path_expects = vec![
            ("$.a.b", Some(true)),
            ("$.a", Some(false)),
            ("$.c", None),
            ("$.kind", None),
            ("$.payload.code", None),
            ("$.payload.success", Some(false)),
            ("$.payload.result.time_cost", None),
            ("$.payload.not-exists", None),
            ("$.not-exists", None),
            ("$", None),
        ];

        let mut jsons = json_strings
            .iter()
            .map(|s| {
                let value = jsonb::parse_value(s.as_bytes()).unwrap();
                Arc::new(BinaryArray::from_iter_values([value.to_vec()])) as ArrayRef
            })
            .collect::<Vec<_>>();
        let json_struct_arrays =
            std::iter::repeat_n(json_struct, path_expects.len() - jsons.len()).collect::<Vec<_>>();
        jsons.extend(json_struct_arrays);

        for i in 0..jsons.len() {
            let json = &jsons[i];
            let (path, expect) = path_expects[i];

            let args = ScalarFunctionArgs {
                args: vec![
                    ColumnarValue::Array(json.clone()),
                    ColumnarValue::Scalar(path.into()),
                ],
                arg_fields: vec![],
                number_rows: 1,
                return_field: Arc::new(Field::new("x", DataType::Boolean, false)),
                config_options: Arc::new(Default::default()),
            };
            let result = json_get_bool
                .invoke_with_args(args)
                .and_then(|x| x.to_array(1))
                .unwrap();

            let result = result.as_boolean();
            assert_eq!(1, result.len());
            let actual = result.is_valid(0).then(|| result.value(0));
            assert_eq!(actual, expect);
        }
    }

    #[test]
    fn test_json_get_string() {
        let json_get_string = JsonGetString::default();

        assert_eq!("json_get_string", json_get_string.name());
        assert_eq!(
            DataType::Utf8View,
            json_get_string
                .return_type(&[DataType::Binary, DataType::Utf8])
                .unwrap()
        );

        let json_strings = [
            r#"{"a": {"b": "a"}, "b": "b", "c": "c"}"#,
            r#"{"a": "d", "b": {"c": "e"}, "c": "f"}"#,
            r#"{"a": "g", "b": "h", "c": {"a": "g"}}"#,
        ];
        let json_struct = test_json_struct();

        let paths = vec![
            "$.a.b",
            "$.a",
            "",
            "$.kind",
            "$.payload.code",
            "$.payload.result.time_cost",
            "$.payload",
            "$.payload.success",
            "$.payload.result",
            "$.payload.result.error",
            "$.payload.result.not-exists",
            "$.payload.not-exists",
            "$.not-exists",
            "$",
        ];
        let expects = [
            Some("a"),
            Some("d"),
            None,
            Some("foo"),
            Some("404"),
            Some("1.234"),
            Some(
                r#"{"code":404,"result":{"error":"not found","time_cost":1.234},"success":false}"#,
            ),
            Some("false"),
            Some(r#"{"error":"not found","time_cost":1.234}"#),
            Some("not found"),
            None,
            None,
            None,
            Some(
                r#"{"kind":"foo","payload":{"code":404,"result":{"error":"not found","time_cost":1.234},"success":false}}"#,
            ),
        ];

        let mut jsons = json_strings
            .iter()
            .map(|s| {
                let value = jsonb::parse_value(s.as_bytes()).unwrap();
                Arc::new(BinaryArray::from_iter_values([value.to_vec()])) as ArrayRef
            })
            .collect::<Vec<_>>();
        let json_struct_arrays =
            std::iter::repeat_n(json_struct, expects.len() - jsons.len()).collect::<Vec<_>>();
        jsons.extend(json_struct_arrays);

        for i in 0..jsons.len() {
            let json = &jsons[i];
            let path = paths[i];
            let expect = expects[i];

            let args = ScalarFunctionArgs {
                args: vec![
                    ColumnarValue::Array(json.clone()),
                    ColumnarValue::Scalar(path.into()),
                ],
                arg_fields: vec![],
                number_rows: 1,
                return_field: Arc::new(Field::new("x", DataType::Utf8View, false)),
                config_options: Arc::new(Default::default()),
            };
            let result = json_get_string
                .invoke_with_args(args)
                .and_then(|x| x.to_array(1))
                .unwrap();

            let result = result.as_string_view();
            assert_eq!(1, result.len());
            let actual = result.is_valid(0).then(|| result.value(0));
            assert_eq!(actual, expect);
        }
    }

    #[test]
    fn test_json_get_object() -> Result<()> {
        let udf = JsonGetObject::default();
        assert_eq!("json_get_object", udf.name());
        assert_eq!(
            DataType::BinaryView,
            udf.return_type(&[DataType::BinaryView, DataType::Utf8View])?
        );

        let json_value = parse_string_to_jsonb(r#"{"a": {"b": {"c": {"d": 1}}}}"#).unwrap();
        let paths = vec!["$", "$.a", "$.a.b", "$.a.b.c", "$.a.b.c.d", "$.e", "$.a.e"];
        let number_rows = paths.len();

        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Scalar(ScalarValue::Binary(Some(json_value))),
                ColumnarValue::Array(Arc::new(StringArray::from_iter_values(paths))),
            ],
            arg_fields: vec![],
            number_rows,
            return_field: Arc::new(Field::new("x", DataType::Binary, false)),
            config_options: Arc::new(Default::default()),
        };
        let result = udf
            .invoke_with_args(args)
            .and_then(|x| x.to_array(number_rows))?;
        let result = result.as_binary_view();

        let expected = &BinaryViewArray::from_iter(
            vec![
                Some(r#"{"a": {"b": {"c": {"d": 1}}}}"#),
                Some(r#"{"b": {"c": {"d": 1}}}"#),
                Some(r#"{"c": {"d": 1}}"#),
                Some(r#"{"d": 1}"#),
                None,
                None,
                None,
            ]
            .into_iter()
            .map(|x| x.and_then(|s| parse_string_to_jsonb(s).ok())),
        );
        assert_eq!(result, expected);
        Ok(())
    }

    #[test]
    fn test_json_get_with_type() {
        let json_get_with_type = JsonGetWithType::default();

        assert_eq!("json_get", json_get_with_type.name());

        let json_strings = [
            r#"{"a": {"b": "a"}, "b": "b", "c": "c"}"#,
            r#"{"a": "d", "b": {"c": "e"}, "c": "f"}"#,
            r#"{"a": "g", "b": "h", "c": {"a": "g"}}"#,
        ];
        let json_struct = test_json_struct();

        let paths = vec![
            "$.a.b",
            "$.a",
            "",
            "$.kind",
            "$.payload.code",
            "$.payload.result.time_cost",
            "$.payload",
            "$.payload.success",
            "$.payload.result",
            "$.payload.result.error",
            "$.payload.result.not-exists",
            "$.payload.not-exists",
            "$.not-exists",
            "$",
        ];
        let expects = [
            Some("a"),
            Some("d"),
            None,
            Some("foo"),
            Some("404"),
            Some("1.234"),
            Some(
                r#"{"code":404,"result":{"error":"not found","time_cost":1.234},"success":false}"#,
            ),
            Some("false"),
            Some(r#"{"error":"not found","time_cost":1.234}"#),
            Some("not found"),
            None,
            None,
            None,
            Some(
                r#"{"kind":"foo","payload":{"code":404,"result":{"error":"not found","time_cost":1.234},"success":false}}"#,
            ),
        ];

        let mut jsons = json_strings
            .iter()
            .map(|s| {
                let value = jsonb::parse_value(s.as_bytes()).unwrap();
                Arc::new(BinaryArray::from_iter_values([value.to_vec()])) as ArrayRef
            })
            .collect::<Vec<_>>();
        let json_struct_arrays =
            std::iter::repeat_n(json_struct, expects.len() - jsons.len()).collect::<Vec<_>>();
        jsons.extend(json_struct_arrays);

        for i in 0..jsons.len() {
            let json = &jsons[i];
            let path = paths[i];
            let expect = expects[i];

            let args = ScalarFunctionArgs {
                args: vec![
                    ColumnarValue::Array(json.clone()),
                    ColumnarValue::Scalar(path.into()),
                    ColumnarValue::Scalar(ScalarValue::Utf8View(None)),
                ],
                arg_fields: vec![],
                number_rows: 1,
                return_field: Arc::new(Field::new("x", DataType::Utf8View, false)),
                config_options: Arc::new(Default::default()),
            };
            let result = json_get_with_type
                .invoke_with_args(args)
                .and_then(|x| x.to_array(1))
                .unwrap();

            let result = result.as_string_view();
            assert_eq!(1, result.len());
            let actual = result.is_valid(0).then(|| result.value(0));
            assert_eq!(actual, expect);
        }

        let json_strings = [
            r#"{"a": {"b": 2}, "b": 2, "c": 3}"#,
            r#"{"a": 4, "b": {"c": 6}, "c": 6}"#,
            r#"{"a": 7, "b": 8, "c": {"a": 7}}"#,
        ];
        let paths = ["$.a.b", "$.a", "$.c", "$.payload.code"];
        let expects = [Some(2), Some(4), None, Some(404)];

        for (i, (path, expect)) in paths.iter().zip(expects.iter()).enumerate() {
            let json = if i < json_strings.len() {
                let value = jsonb::parse_value(json_strings[i].as_bytes()).unwrap();
                Arc::new(BinaryArray::from_iter_values([value.to_vec()])) as ArrayRef
            } else {
                test_json_struct()
            };

            let args = ScalarFunctionArgs {
                args: vec![
                    ColumnarValue::Array(json),
                    ColumnarValue::Scalar((*path).into()),
                    ColumnarValue::Scalar(ScalarValue::Int64(None)),
                ],
                arg_fields: vec![],
                number_rows: 1,
                return_field: Arc::new(Field::new("x", DataType::Int64, false)),
                config_options: Arc::new(Default::default()),
            };
            let result = json_get_with_type
                .invoke_with_args(args)
                .and_then(|x| x.to_array(1))
                .unwrap();

            let result = result.as_primitive::<Int64Type>();
            assert_eq!(1, result.len());
            let actual = result.is_valid(0).then(|| result.value(0));
            assert_eq!(actual, *expect);
        }

        let json_strings = [
            r#"{"a": {"b": 2.1}, "b": 2.2, "c": 3.3}"#,
            r#"{"a": 4.4, "b": {"c": 6.6}, "c": 6.6}"#,
            r#"{"a": 7.7, "b": 8.8, "c": {"a": 7.7}}"#,
        ];
        let paths = ["$.a.b", "$.a", "$.c", "$.payload.result.time_cost"];
        let expects = [Some(2.1), Some(4.4), None, Some(1.234)];

        for (i, (path, expect)) in paths.iter().zip(expects.iter()).enumerate() {
            let json = if i < json_strings.len() {
                let value = jsonb::parse_value(json_strings[i].as_bytes()).unwrap();
                Arc::new(BinaryArray::from_iter_values([value.to_vec()])) as ArrayRef
            } else {
                test_json_struct()
            };

            let args = ScalarFunctionArgs {
                args: vec![
                    ColumnarValue::Array(json),
                    ColumnarValue::Scalar((*path).into()),
                    ColumnarValue::Scalar(ScalarValue::Float64(None)),
                ],
                arg_fields: vec![],
                number_rows: 1,
                return_field: Arc::new(Field::new("x", DataType::Float64, false)),
                config_options: Arc::new(Default::default()),
            };
            let result = json_get_with_type
                .invoke_with_args(args)
                .and_then(|x| x.to_array(1))
                .unwrap();

            let result = result.as_primitive::<Float64Type>();
            assert_eq!(1, result.len());
            let actual = result.is_valid(0).then(|| result.value(0));
            assert_eq!(actual, *expect);
        }

        let json_strings = [
            r#"{"a": {"b": true}, "b": false, "c": true}"#,
            r#"{"a": false, "b": {"c": true}, "c": false}"#,
            r#"{"a": true, "b": false, "c": {"a": true}}"#,
        ];
        let paths = ["$.a.b", "$.a", "$.c", "$.payload.success"];
        let expects = [Some(true), Some(false), None, Some(false)];

        for (i, (path, expect)) in paths.iter().zip(expects.iter()).enumerate() {
            let json = if i < json_strings.len() {
                let value = jsonb::parse_value(json_strings[i].as_bytes()).unwrap();
                Arc::new(BinaryArray::from_iter_values([value.to_vec()])) as ArrayRef
            } else {
                test_json_struct()
            };

            let args = ScalarFunctionArgs {
                args: vec![
                    ColumnarValue::Array(json),
                    ColumnarValue::Scalar((*path).into()),
                    ColumnarValue::Scalar(ScalarValue::Boolean(None)),
                ],
                arg_fields: vec![],
                number_rows: 1,
                return_field: Arc::new(Field::new("x", DataType::Boolean, false)),
                config_options: Arc::new(Default::default()),
            };
            let result = json_get_with_type
                .invoke_with_args(args)
                .and_then(|x| x.to_array(1))
                .unwrap();

            let result = result.as_boolean();
            assert_eq!(1, result.len());
            let actual = result.is_valid(0).then(|| result.value(0));
            assert_eq!(actual, *expect);
        }
    }
}
