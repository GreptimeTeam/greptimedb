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
use std::str::FromStr;
use std::sync::Arc;

use arrow::array::{ArrayRef, BinaryViewArray, StringViewArray, StructArray};
use arrow::compute;
use arrow::datatypes::{Float64Type, Int64Type, UInt64Type};
use datafusion_common::arrow::array::{
    Array, AsArray, BinaryViewBuilder, BooleanBuilder, Float64Builder, Int64Builder,
    StringViewBuilder,
};
use datafusion_common::arrow::datatypes::DataType;
use datafusion_common::{DataFusionError, Result};
use datafusion_expr::type_coercion::aggregates::STRINGS;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, Signature, Volatility};
use datatypes::arrow_array::string_array_value_at_index;
use datatypes::json::JsonStructureSettings;
use jsonpath_rust::JsonPath;
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

/// Get the value from the JSONB by the given path and return it as specified type.
/// If the path does not exist or the value is not the type specified, return `NULL`.
macro_rules! json_get {
    // e.g. name = JsonGetInt, type = Int64, rust_type = i64, doc = "Get the value from the JSONB by the given path and return it as an integer."
    ($name:ident, $type:ident, $rust_type:ident, $doc:expr) => {
        paste::paste! {
            #[doc = $doc]
            #[derive(Clone, Debug)]
            pub struct $name {
                signature: Signature,
            }

            impl $name {
                pub const NAME: &'static str = stringify!([<$name:snake>]);
            }

            impl Default for $name {
                fn default() -> Self {
                    Self {
                        // TODO(LFC): Use a more clear type here instead of "Binary" for Json input, once we have a "Json" type.
                        signature: helper::one_of_sigs2(
                            vec![DataType::Binary, DataType::BinaryView],
                            vec![DataType::Utf8, DataType::Utf8View],
                        ),
                    }
                }
            }

            impl Function for $name {
                fn name(&self) -> &str {
                    Self::NAME
                }

                fn return_type(&self, _: &[DataType]) -> datafusion_common::Result<DataType> {
                    Ok(DataType::[<$type>])
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

                    let size = jsons.len();
                    let mut builder = [<$type Builder>]::with_capacity(size);

                    for i in 0..size {
                        let json = jsons.is_valid(i).then(|| jsons.value(i));
                        let path = paths.is_valid(i).then(|| paths.value(i));
                        let result = match (json, path) {
                            (Some(json), Some(path)) => {
                                get_json_by_path(json, path)
                                    .and_then(|json| { jsonb::[<to_ $rust_type>](&json).ok() })
                            }
                            _ => None,
                        };

                        builder.append_option(result);
                    }

                    Ok(ColumnarValue::Array(Arc::new(builder.finish())))
                }
            }

            impl Display for $name {
                fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
                    write!(f, "{}", Self::NAME.to_ascii_uppercase())
                }
            }
        }
    };
}

json_get!(
    JsonGetInt,
    Int64,
    i64,
    "Get the value from the JSONB by the given path and return it as an integer."
);

json_get!(
    JsonGetFloat,
    Float64,
    f64,
    "Get the value from the JSONB by the given path and return it as a float."
);

json_get!(
    JsonGetBool,
    Boolean,
    bool,
    "Get the value from the JSONB by the given path and return it as a boolean."
);

/// Get the value from the JSONB by the given path and return it as a string.
#[derive(Clone, Debug)]
pub struct JsonGetString {
    signature: Signature,
}

impl JsonGetString {
    pub const NAME: &'static str = "json_get_string";
}

impl Default for JsonGetString {
    fn default() -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
        }
    }
}

impl Function for JsonGetString {
    fn name(&self) -> &str {
        Self::NAME
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8View)
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [arg0, arg1] = extract_args(self.name(), &args)?;

        let arg1 = compute::cast(&arg1, &DataType::Utf8View)?;
        let paths = arg1.as_string_view();

        let result = match arg0.data_type() {
            DataType::Binary | DataType::LargeBinary | DataType::BinaryView => {
                let arg0 = compute::cast(&arg0, &DataType::BinaryView)?;
                let jsons = arg0.as_binary_view();
                jsonb_get_string(jsons, paths)?
            }
            DataType::Struct(_) => {
                let jsons = arg0.as_struct();
                json_struct_get_string(jsons, paths)?
            }
            _ => {
                return Err(DataFusionError::Execution(format!(
                    "{} not supported argument type {}",
                    Self::NAME,
                    arg0.data_type(),
                )));
            }
        };

        Ok(ColumnarValue::Array(result))
    }
}

fn jsonb_get_string(jsons: &BinaryViewArray, paths: &StringViewArray) -> Result<ArrayRef> {
    let size = jsons.len();
    let mut builder = StringViewBuilder::with_capacity(size);

    for i in 0..size {
        let json = jsons.is_valid(i).then(|| jsons.value(i));
        let path = paths.is_valid(i).then(|| paths.value(i));
        let result = match (json, path) {
            (Some(json), Some(path)) => {
                get_json_by_path(json, path).and_then(|json| jsonb::to_str(&json).ok())
            }
            _ => None,
        };
        builder.append_option(result);
    }

    Ok(Arc::new(builder.finish()))
}

fn json_struct_get_string(jsons: &StructArray, paths: &StringViewArray) -> Result<ArrayRef> {
    let size = jsons.len();
    let mut builder = StringViewBuilder::with_capacity(size);

    for i in 0..size {
        if jsons.is_null(i) || paths.is_null(i) {
            builder.append_null();
            continue;
        }
        let path = paths.value(i);

        // naively assume the JSON path is our kind of indexing to the field, by removing its "root"
        let field_path = path.trim().replace("$.", "");
        let column = jsons.column_by_name(&field_path);

        if let Some(column) = column {
            if let Some(v) = string_array_value_at_index(column, i) {
                builder.append_value(v);
            } else {
                builder.append_value(arrow_cast::display::array_value_to_string(column, i)?);
            }
        } else {
            let Some(raw) = jsons
                .column_by_name(JsonStructureSettings::RAW_FIELD)
                .and_then(|x| string_array_value_at_index(x, i))
            else {
                builder.append_null();
                continue;
            };

            let path: JsonPath<Value> = JsonPath::try_from(path).map_err(|e| {
                DataFusionError::Execution(format!("{path} is not a valid JSON path: {e}"))
            })?;
            // the wanted field is not retrievable from the JSON struct columns directly, we have
            // to combine everything (columns and the "_raw") into a complete JSON value to find it
            let value = json_struct_to_value(raw, jsons, i)?;

            match path.find(&value) {
                Value::Null => builder.append_null(),
                Value::Array(values) => match values.as_slice() {
                    [] => builder.append_null(),
                    [x] => {
                        if let Some(s) = x.as_str() {
                            builder.append_value(s)
                        } else {
                            builder.append_value(x.to_string())
                        }
                    }
                    x => builder.append_value(
                        x.iter()
                            .map(|v| v.to_string())
                            .collect::<Vec<_>>()
                            .join(", "),
                    ),
                },
                // Safety: guarded by the returns of `path.find` as documented
                _ => unreachable!(),
            }
        }
    }

    Ok(Arc::new(builder.finish()))
}

fn json_struct_to_value(raw: &str, jsons: &StructArray, i: usize) -> Result<Value> {
    let Ok(mut json) = Value::from_str(raw) else {
        return Err(DataFusionError::Internal(format!(
            "inner field '{}' is not a valid JSON string",
            JsonStructureSettings::RAW_FIELD
        )));
    };

    for (column_name, column) in jsons.column_names().into_iter().zip(jsons.columns()) {
        if column_name == JsonStructureSettings::RAW_FIELD {
            continue;
        }

        let (json_pointer, field) = if let Some((json_object, field)) = column_name.rsplit_once(".")
        {
            let json_pointer = format!("/{}", json_object.replace(".", "/"));
            (json_pointer, field)
        } else {
            ("".to_string(), column_name)
        };
        let Some(json_object) = json
            .pointer_mut(&json_pointer)
            .and_then(|x| x.as_object_mut())
        else {
            return Err(DataFusionError::Internal(format!(
                "value at JSON pointer '{}' is not an object",
                json_pointer
            )));
        };

        macro_rules! insert {
            ($column: ident, $i: ident, $json_object: ident, $field: ident) => {{
                if let Some(value) = $column
                    .is_valid($i)
                    .then(|| serde_json::Value::from($column.value($i)))
                {
                    $json_object.insert($field.to_string(), value);
                }
            }};
        }

        match column.data_type() {
            // boolean => Value::Bool
            DataType::Boolean => {
                let column = column.as_boolean();
                insert!(column, i, json_object, field);
            }
            // int => Value::Number
            DataType::Int64 => {
                let column = column.as_primitive::<Int64Type>();
                insert!(column, i, json_object, field);
            }
            DataType::UInt64 => {
                let column = column.as_primitive::<UInt64Type>();
                insert!(column, i, json_object, field);
            }
            DataType::Float64 => {
                let column = column.as_primitive::<Float64Type>();
                insert!(column, i, json_object, field);
            }
            // string => Value::String
            DataType::Utf8 => {
                let column = column.as_string::<i32>();
                insert!(column, i, json_object, field);
            }
            DataType::LargeUtf8 => {
                let column = column.as_string::<i64>();
                insert!(column, i, json_object, field);
            }
            DataType::Utf8View => {
                let column = column.as_string_view();
                insert!(column, i, json_object, field);
            }
            // other => Value::Array and Value::Object
            _ => {
                return Err(DataFusionError::NotImplemented(format!(
                    "{} is not yet supported to be executed with field {} of datatype {}",
                    JsonGetString::NAME,
                    column_name,
                    column.data_type()
                )));
            }
        }
    }
    Ok(json)
}

impl Display for JsonGetString {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", Self::NAME.to_ascii_uppercase())
    }
}

/// Get the object from JSON value by path.
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
                STRINGS.to_vec(),
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

impl Display for JsonGetObject {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", Self::NAME.to_ascii_uppercase())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Float64Array, Int64Array, StructArray};
    use arrow_schema::Field;
    use datafusion_common::ScalarValue;
    use datafusion_common::arrow::array::{BinaryArray, BinaryViewArray, StringArray};
    use datafusion_common::arrow::datatypes::{Float64Type, Int64Type};
    use datatypes::types::parse_string_to_jsonb;
    use serde_json::json;

    use super::*;

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
        let paths = vec!["$.a.b", "$.a", "$.c"];
        let results = [Some(2), Some(4), None];

        let jsonbs = json_strings
            .iter()
            .map(|s| {
                let value = jsonb::parse_value(s.as_bytes()).unwrap();
                value.to_vec()
            })
            .collect::<Vec<_>>();

        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::new(BinaryArray::from_iter_values(jsonbs))),
                ColumnarValue::Array(Arc::new(StringArray::from_iter_values(paths))),
            ],
            arg_fields: vec![],
            number_rows: 3,
            return_field: Arc::new(Field::new("x", DataType::Int64, false)),
            config_options: Arc::new(Default::default()),
        };
        let result = json_get_int
            .invoke_with_args(args)
            .and_then(|x| x.to_array(3))
            .unwrap();
        let vector = result.as_primitive::<Int64Type>();

        assert_eq!(3, vector.len());
        for (i, gt) in results.iter().enumerate() {
            let result = vector.is_valid(i).then(|| vector.value(i));
            assert_eq!(*gt, result);
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
        let paths = vec!["$.a.b", "$.a", "$.c"];
        let results = [Some(2.1), Some(4.4), None];

        let jsonbs = json_strings
            .iter()
            .map(|s| {
                let value = jsonb::parse_value(s.as_bytes()).unwrap();
                value.to_vec()
            })
            .collect::<Vec<_>>();

        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::new(BinaryArray::from_iter_values(jsonbs))),
                ColumnarValue::Array(Arc::new(StringArray::from_iter_values(paths))),
            ],
            arg_fields: vec![],
            number_rows: 3,
            return_field: Arc::new(Field::new("x", DataType::Float64, false)),
            config_options: Arc::new(Default::default()),
        };
        let result = json_get_float
            .invoke_with_args(args)
            .and_then(|x| x.to_array(3))
            .unwrap();
        let vector = result.as_primitive::<Float64Type>();

        assert_eq!(3, vector.len());
        for (i, gt) in results.iter().enumerate() {
            let result = vector.is_valid(i).then(|| vector.value(i));
            assert_eq!(*gt, result);
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
        let paths = vec!["$.a.b", "$.a", "$.c"];
        let results = [Some(true), Some(false), None];

        let jsonbs = json_strings
            .iter()
            .map(|s| {
                let value = jsonb::parse_value(s.as_bytes()).unwrap();
                value.to_vec()
            })
            .collect::<Vec<_>>();

        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::new(BinaryArray::from_iter_values(jsonbs))),
                ColumnarValue::Array(Arc::new(StringArray::from_iter_values(paths))),
            ],
            arg_fields: vec![],
            number_rows: 3,
            return_field: Arc::new(Field::new("x", DataType::Boolean, false)),
            config_options: Arc::new(Default::default()),
        };
        let result = json_get_bool
            .invoke_with_args(args)
            .and_then(|x| x.to_array(3))
            .unwrap();
        let vector = result.as_boolean();

        assert_eq!(3, vector.len());
        for (i, gt) in results.iter().enumerate() {
            let result = vector.is_valid(i).then(|| vector.value(i));
            assert_eq!(*gt, result);
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

        // complete JSON is:
        // {
        //     "kind": "foo",
        //     "payload": {
        //         "code": 404,
        //         "success": false,
        //         "result": {
        //             "error": "not found",
        //             "time_cost": 1.234
        //         }
        //     }
        // }
        let json_struct: ArrayRef = Arc::new(StructArray::new(
            vec![
                Field::new("kind", DataType::Utf8, true),
                Field::new("payload.code", DataType::Int64, true),
                Field::new("payload.result.time_cost", DataType::Float64, true),
                Field::new(JsonStructureSettings::RAW_FIELD, DataType::Utf8View, true),
            ]
            .into(),
            vec![
                Arc::new(StringArray::from_iter([Some("foo")])) as ArrayRef,
                Arc::new(Int64Array::from_iter([Some(404)])),
                Arc::new(Float64Array::from_iter([Some(1.234)])),
                Arc::new(StringViewArray::from_iter([Some(
                    json! ({
                        "payload": {
                            "success": false,
                            "result": {
                                "error": "not found"
                            }
                        }
                    })
                    .to_string(),
                )])),
            ],
            None,
        ));

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
}
