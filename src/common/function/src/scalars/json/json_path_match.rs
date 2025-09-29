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
use datafusion_common::arrow::array::{Array, AsArray, BooleanBuilder};
use datafusion_common::arrow::datatypes::DataType;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, Signature};

use crate::function::{Function, extract_args};
use crate::helper;

/// Check if the given JSON data match the given JSON path's predicate.
#[derive(Clone, Debug, Default)]
pub struct JsonPathMatchFunction;

const NAME: &str = "json_path_match";

impl Function for JsonPathMatchFunction {
    fn name(&self) -> &str {
        NAME
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
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
        let mut builder = BooleanBuilder::with_capacity(size);

        for i in 0..size {
            let json = jsons.is_valid(i).then(|| jsons.value(i));
            let path = paths.is_valid(i).then(|| paths.value(i));

            let result = match (json, path) {
                (Some(json), Some(path)) => {
                    if !jsonb::is_null(json) {
                        let json_path = jsonb::jsonpath::parse_json_path(path.as_bytes());
                        match json_path {
                            Ok(json_path) => jsonb::path_match(json, json_path).ok(),
                            Err(_) => None,
                        }
                    } else {
                        None
                    }
                }
                _ => None,
            };
            builder.append_option(result);
        }

        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

impl Display for JsonPathMatchFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "JSON_PATH_MATCH")
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::Field;
    use datafusion_common::arrow::array::{BinaryArray, StringArray};

    use super::*;

    #[test]
    fn test_json_path_match_function() {
        let json_path_match = JsonPathMatchFunction;

        assert_eq!("json_path_match", json_path_match.name());
        assert_eq!(
            DataType::Boolean,
            json_path_match.return_type(&[DataType::Binary]).unwrap()
        );

        let json_strings = [
            Some(r#"{"a": {"b": 2}, "b": 2, "c": 3}"#.to_string()),
            Some(r#"{"a": 1, "b": [1,2,3]}"#.to_string()),
            Some(r#"{"a": 1 ,"b": [1,2,3]}"#.to_string()),
            Some(r#"[1,2,3]"#.to_string()),
            Some(r#"{"a":1,"b":[1,2,3]}"#.to_string()),
            Some(r#"null"#.to_string()),
            Some(r#"null"#.to_string()),
        ];

        let paths = vec![
            Some("$.a.b == 2".to_string()),
            Some("$.b[1 to last] >= 2".to_string()),
            Some("$.c > 0".to_string()),
            Some("$[0 to last] > 0".to_string()),
            Some(r#"null"#.to_string()),
            Some("$.c > 0".to_string()),
            Some(r#"null"#.to_string()),
        ];

        let results = [
            Some(true),
            Some(true),
            Some(false),
            Some(true),
            None,
            None,
            None,
        ];

        let jsonbs = json_strings
            .into_iter()
            .map(|s| s.map(|json| jsonb::parse_value(json.as_bytes()).unwrap().to_vec()))
            .collect::<Vec<_>>();

        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::new(BinaryArray::from_iter(jsonbs))),
                ColumnarValue::Array(Arc::new(StringArray::from_iter(paths))),
            ],
            arg_fields: vec![],
            number_rows: 7,
            return_field: Arc::new(Field::new("x", DataType::Boolean, false)),
            config_options: Arc::new(Default::default()),
        };
        let result = json_path_match
            .invoke_with_args(args)
            .and_then(|x| x.to_array(7))
            .unwrap();
        let vector = result.as_boolean();

        assert_eq!(7, vector.len());
        for (actual, expected) in vector.iter().zip(results) {
            assert_eq!(actual, expected);
        }
    }
}
