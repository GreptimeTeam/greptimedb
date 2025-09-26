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
use datafusion_common::DataFusionError;
use datafusion_common::arrow::array::{Array, AsArray, BooleanBuilder};
use datafusion_common::arrow::datatypes::DataType;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, Signature};

use crate::function::{Function, extract_args};
use crate::helper;

/// Check if the given JSON data contains the given JSON path.
#[derive(Clone, Debug, Default)]
pub struct JsonPathExistsFunction;

const NAME: &str = "json_path_exists";

impl Function for JsonPathExistsFunction {
    fn name(&self) -> &str {
        NAME
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn signature(&self) -> Signature {
        // TODO(LFC): Use a more clear type here instead of "Binary" for Json input, once we have a "Json" type.
        helper::one_of_sigs2(
            vec![DataType::Binary, DataType::BinaryView, DataType::Null],
            vec![DataType::Utf8, DataType::Utf8View, DataType::Null],
        )
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion_common::Result<ColumnarValue> {
        let [jsons, paths] = extract_args(self.name(), &args)?;

        let size = jsons.len();
        let mut builder = BooleanBuilder::with_capacity(size);

        match (jsons.data_type(), paths.data_type()) {
            (DataType::Null, _) | (_, DataType::Null) => builder.append_nulls(size),
            _ => {
                let jsons = compute::cast(&jsons, &DataType::BinaryView)?;
                let jsons = jsons.as_binary_view();
                let paths = compute::cast(&paths, &DataType::Utf8View)?;
                let paths = paths.as_string_view();
                for i in 0..size {
                    let json = jsons.is_valid(i).then(|| jsons.value(i));
                    let path = paths.is_valid(i).then(|| paths.value(i));
                    let result = match (json, path) {
                        (Some(json), Some(path)) => {
                            // Get `JsonPath`.
                            let json_path = match jsonb::jsonpath::parse_json_path(path.as_bytes())
                            {
                                Ok(json_path) => json_path,
                                Err(e) => {
                                    return Err(DataFusionError::Execution(format!(
                                        "invalid json path '{path}': {e}"
                                    )));
                                }
                            };
                            jsonb::path_exists(json, json_path).ok()
                        }
                        _ => None,
                    };

                    builder.append_option(result);
                }
            }
        }

        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

impl Display for JsonPathExistsFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "JSON_PATH_EXISTS")
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::Field;
    use datafusion_common::arrow::array::{BinaryArray, NullArray, StringArray};

    use super::*;

    #[test]
    fn test_json_path_exists_function() {
        let json_path_exists = JsonPathExistsFunction;

        assert_eq!("json_path_exists", json_path_exists.name());
        assert_eq!(
            DataType::Boolean,
            json_path_exists.return_type(&[DataType::Binary]).unwrap()
        );

        let json_strings = [
            r#"{"a": {"b": 2}, "b": 2, "c": 3}"#,
            r#"{"a": 4, "b": {"c": 6}, "c": 6}"#,
            r#"{"a": 7, "b": 8, "c": {"a": 7}}"#,
            r#"{"a": 7, "b": 8, "c": {"a": 7}}"#,
            r#"[1, 2, 3]"#,
            r#"null"#,
            r#"{"a": 7, "b": 8, "c": {"a": 7}}"#,
            r#"null"#,
        ];
        let paths = vec![
            "$.a.b.c", "$.b", "$.c.a", ".d", "$[0]", "$.a", "null", "null",
        ];
        let expected = [false, true, true, false, true, false, false, false];

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
            number_rows: 8,
            return_field: Arc::new(Field::new("x", DataType::Boolean, false)),
            config_options: Arc::new(Default::default()),
        };
        let result = json_path_exists
            .invoke_with_args(args)
            .and_then(|x| x.to_array(8))
            .unwrap();
        let vector = result.as_boolean();

        // Test for non-nulls.
        assert_eq!(8, vector.len());
        for (i, real) in expected.iter().enumerate() {
            let val = vector.value(i);
            assert_eq!(val, *real);
        }

        // Test for path error.
        let json_bytes = jsonb::parse_value("{}".as_bytes()).unwrap().to_vec();
        let illegal_path = "$..a";

        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::new(BinaryArray::from_iter_values(vec![json_bytes]))),
                ColumnarValue::Array(Arc::new(StringArray::from_iter_values(vec![illegal_path]))),
            ],
            arg_fields: vec![],
            number_rows: 1,
            return_field: Arc::new(Field::new("x", DataType::Boolean, false)),
            config_options: Arc::new(Default::default()),
        };
        let err = json_path_exists.invoke_with_args(args);
        assert!(err.is_err());

        // Test for nulls.
        let json_bytes = jsonb::parse_value("{}".as_bytes()).unwrap().to_vec();
        let json = Arc::new(BinaryArray::from_iter_values(vec![json_bytes]));
        let null_json = Arc::new(NullArray::new(1));

        let path = Arc::new(StringArray::from_iter_values(vec!["$.a"]));
        let null_path = Arc::new(NullArray::new(1));

        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(null_json), ColumnarValue::Array(path)],
            arg_fields: vec![],
            number_rows: 1,
            return_field: Arc::new(Field::new("x", DataType::Boolean, false)),
            config_options: Arc::new(Default::default()),
        };
        let result = json_path_exists
            .invoke_with_args(args)
            .and_then(|x| x.to_array(1))
            .unwrap();
        let result1 = result.as_boolean();

        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(json), ColumnarValue::Array(null_path)],
            arg_fields: vec![],
            number_rows: 1,
            return_field: Arc::new(Field::new("x", DataType::Boolean, false)),
            config_options: Arc::new(Default::default()),
        };
        let result = json_path_exists
            .invoke_with_args(args)
            .and_then(|x| x.to_array(1))
            .unwrap();
        let result2 = result.as_boolean();

        assert_eq!(result1.len(), 1);
        assert!(result1.is_null(0));
        assert_eq!(result2.len(), 1);
        assert!(result2.is_null(0));
    }
}
