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

use arrow::compute;
use common_query::error::Result;
use datafusion_common::arrow::array::{
    Array, AsArray, BooleanBuilder, Float64Builder, Int64Builder, StringViewBuilder,
};
use datafusion_common::arrow::datatypes::DataType;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, Signature};

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
            #[derive(Clone, Debug, Default)]
            pub struct $name;

            impl Function for $name {
                fn name(&self) -> &str {
                    stringify!([<$name:snake>])
                }

                fn return_type(&self, _: &[DataType]) -> Result<DataType> {
                    Ok(DataType::[<$type>])
                }

                fn signature(&self) -> Signature {
                    // TODO(LFC): Use a more clear type here instead of "Binary" for Json input, once we have a "Json" type.
                    helper::one_of_sigs2(
                        vec![DataType::Binary, DataType::BinaryView],
                        vec![DataType::Utf8, DataType::Utf8View],
                    )
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
                    write!(f, "{}", stringify!([<$name:snake>]).to_ascii_uppercase())
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
#[derive(Clone, Debug, Default)]
pub struct JsonGetString;

impl Function for JsonGetString {
    fn name(&self) -> &str {
        "json_get_string"
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8View)
    }

    fn signature(&self) -> Signature {
        // TODO(LFC): Use a more clear type here instead of "Binary" for Json input, once we have a "Json" type.
        helper::one_of_sigs2(
            vec![DataType::Binary, DataType::BinaryView],
            vec![DataType::Utf8, DataType::Utf8View],
        )
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

        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

impl Display for JsonGetString {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", "json_get_string".to_ascii_uppercase())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::Field;
    use datafusion_common::arrow::array::{BinaryArray, StringArray};
    use datafusion_common::arrow::datatypes::{Float64Type, Int64Type};

    use super::*;

    #[test]
    fn test_json_get_int() {
        let json_get_int = JsonGetInt;

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
        let json_get_float = JsonGetFloat;

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
        let json_get_bool = JsonGetBool;

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
        let json_get_string = JsonGetString;

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
        let paths = vec!["$.a.b", "$.a", ""];
        let results = [Some("a"), Some("d"), None];

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
            return_field: Arc::new(Field::new("x", DataType::Utf8View, false)),
            config_options: Arc::new(Default::default()),
        };
        let result = json_get_string
            .invoke_with_args(args)
            .and_then(|x| x.to_array(3))
            .unwrap();
        let vector = result.as_string_view();

        assert_eq!(3, vector.len());
        for (i, gt) in results.iter().enumerate() {
            let result = vector.is_valid(i).then(|| vector.value(i));
            assert_eq!(*gt, result);
        }
    }
}
